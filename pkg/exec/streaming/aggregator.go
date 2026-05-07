package streaming

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// aggregator buffers per-(entity, bucket) deltas in memory and flushes them
// periodically as a single store.MergeUpdate per key. The point is to absorb
// hot-key write contention: a celebrity post receiving 50k likes/sec lands
// as ONE atomic-ADD per flush window per worker, instead of 50k.
//
// Correctness properties:
//
//  1. Dedup runs PER RECORD, before the record's contribution enters the
//     batch. Duplicates are Ack'd and dropped; first-seen records are
//     accumulated.
//  2. Acks are deferred. A record's Ack fires only after its batch has
//     successfully flushed to the store. If the flush dead-letters, every
//     record in the batch gets an Ack (so the source advances) AND a
//     deadLetter callback (so the user can DLQ the EventIDs).
//  3. Hierarchical rollups (KeyByMany) are supported — each emitted key
//     has its own batch entry. One record contributes to N batches but
//     dedup is claimed once.
//  4. Graceful shutdown drains every batch before returning. Records that
//     arrived before ctx cancellation are flushed; records after are not
//     accepted.
//
// Memory bound: at most maxBatchSize records per (entity, bucket). When a
// batch reaches maxBatchSize, an immediate flush of THAT batch is scheduled
// (synchronously inside the accept path). The aggregator does not bound
// the NUMBER of distinct keys in flight — a pipeline keying by user_id
// across millions of users will hold millions of batches concurrently.
// For high-cardinality pipelines, prefer a small window so each batch
// holds at most a few hundred records before being flushed by the timer.
type aggregator[T any, V any] struct {
	cfg     *runConfig
	monoid  monoid.Monoid[V]
	keysFn  func(T) []string
	valueFn func(T) V
	store   state.Store[V]
	cache   state.Cache[V]
	window  *windowed.Config
	name    string

	maxBatchSize int

	mu      sync.Mutex
	batches map[state.Key]*batch[V]
}

type batch[V any] struct {
	delta     V
	deltaSet  bool      // distinguishes "no contribution yet" from a legitimate zero value
	eventTime time.Time // latest contributor's timestamp (for windowing TTL)
	acks      []func() error
	eventIDs  []string
}

func newAggregator[T any, V any](
	cfg *runConfig,
	mon monoid.Monoid[V],
	keysFn func(T) []string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	name string,
	maxBatchSize int,
) *aggregator[T, V] {
	if maxBatchSize <= 0 {
		maxBatchSize = 1024
	}
	return &aggregator[T, V]{
		cfg:          cfg,
		monoid:       mon,
		keysFn:       keysFn,
		valueFn:      valueFn,
		store:        store,
		cache:        cache,
		window:       window,
		name:         name,
		maxBatchSize: maxBatchSize,
		batches:      map[state.Key]*batch[V]{},
	}
}

// accept claims dedup once for the record's EventID, then accumulates the
// record's delta into every emitted-key's batch. Returns true if the record
// was dropped as a duplicate (caller should still Ack to advance the source).
func (a *aggregator[T, V]) accept(ctx context.Context, rec source.Record[T]) (dup bool) {
	if a.cfg.Dedup != nil && rec.EventID != "" {
		first, err := a.cfg.Dedup.MarkSeen(ctx, rec.EventID)
		if err != nil {
			// Dedup backend failure: same fail-open policy as processor.MergeMany.
			a.cfg.Recorder.RecordError(a.name,
				fmt.Errorf("dedup MarkSeen %q: %w", rec.EventID, err))
		} else if !first {
			a.cfg.Recorder.RecordEvent(a.name + ":dedup_skip")
			return true
		}
	}

	keys := a.keysFn(rec.Value)
	if len(keys) == 0 {
		return false
	}
	delta := a.valueFn(rec.Value)
	eventTime := rec.EventTime
	if eventTime.IsZero() {
		eventTime = time.Now()
	}

	// Collect batches that hit max-size during this accept; flush them
	// outside the lock so the next accept doesn't block on the store.
	var fullBatches []state.Key

	a.mu.Lock()
	for i, entity := range keys {
		sk := state.Key{Entity: entity}
		if a.window != nil {
			sk.Bucket = a.window.BucketID(eventTime)
		}
		b, ok := a.batches[sk]
		if !ok {
			b = &batch[V]{eventTime: eventTime}
			a.batches[sk] = b
		}
		if b.deltaSet {
			b.delta = a.monoid.Combine(b.delta, delta)
		} else {
			// First contribution to this batch — Identity-relative combine
			// preserves the monoid contract (Combine(Identity, x) == x).
			b.delta = a.monoid.Combine(a.monoid.Identity(), delta)
			b.deltaSet = true
		}
		if eventTime.After(b.eventTime) {
			b.eventTime = eventTime
		}
		// Ack and EventID are recorded once per record (on the FIRST emitted
		// key); the flush of any batch unlocks all of them via their
		// per-record callback. We only need the per-record state once.
		if i == 0 {
			if rec.Ack != nil {
				b.acks = append(b.acks, rec.Ack)
			}
			b.eventIDs = append(b.eventIDs, rec.EventID)
		}
		if len(b.acks) >= a.maxBatchSize {
			fullBatches = append(fullBatches, sk)
		}
	}
	a.mu.Unlock()

	for _, sk := range fullBatches {
		a.flushOne(ctx, sk)
	}
	return false
}

// flushAll drains every batch currently held. Called on ticker fire and on
// shutdown. Each batch flushes serially — concurrent flushes against
// distinct DDB partitions could be parallelized, but at moderate flush
// frequency (1s) that's premature.
func (a *aggregator[T, V]) flushAll(ctx context.Context) {
	a.mu.Lock()
	keys := make([]state.Key, 0, len(a.batches))
	for k := range a.batches {
		keys = append(keys, k)
	}
	a.mu.Unlock()

	for _, sk := range keys {
		a.flushOne(ctx, sk)
	}
}

// flushOne removes the named batch from the live map and merges it into the
// store. On success, all deferred acks fire. On retry-exhaustion, the user's
// deadLetter callback receives every EventID in the batch and the acks fire
// anyway so the source advances past the poison batch.
func (a *aggregator[T, V]) flushOne(ctx context.Context, sk state.Key) {
	a.mu.Lock()
	b, ok := a.batches[sk]
	if ok {
		delete(a.batches, sk)
	}
	a.mu.Unlock()
	if !ok || !b.deltaSet {
		return
	}

	// Pass eventID="" so processor.MergeMany skips the dedup check (we
	// already deduped per-record at accept time). The retry/backoff /
	// metrics surface still applies.
	err := processor.MergeMany(ctx, &a.cfg.Config, a.name, "", b.eventTime,
		[]string{sk.Entity}, b.delta, a.store, a.cache, a.window)

	a.cfg.Recorder.RecordEvent(a.name + ":flush")

	if err != nil {
		// Retry exhausted: dead-letter every record in the batch.
		for _, id := range b.eventIDs {
			if a.cfg.deadLetter != nil {
				a.cfg.deadLetter(id, err)
			}
			a.cfg.Recorder.RecordEvent(a.name + ":dead_letter")
		}
	}

	// Ack regardless of flush outcome — same poison-record contract as the
	// non-batched path. Without ack, a poison batch would loop forever.
	for _, ack := range b.acks {
		if err := ack(); err != nil {
			a.cfg.Recorder.RecordError(a.name, fmt.Errorf("source Ack: %w", err))
		}
	}
}

// runFlushLoop is the periodic-flush goroutine. Returns on ctx cancellation
// after a final drain.
func (a *aggregator[T, V]) runFlushLoop(ctx context.Context, window time.Duration) {
	t := time.NewTicker(window)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// Final drain on shutdown — use a short detached context so a
			// canceled parent doesn't preempt the very flush that reconciles
			// outstanding batches with the store.
			drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			a.flushAll(drainCtx)
			cancel()
			return
		case <-t.C:
			a.flushAll(ctx)
		}
	}
}
