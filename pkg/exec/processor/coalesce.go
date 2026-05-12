package processor

// This file implements in-memory delta coalescing within a source batch.
//
// processor.Coalescer accumulates per-event (key, bucket) → delta contributions in a Go
// map and emits one store.MergeUpdate per unique key at flush time. For additive monoids
// against DynamoDB (10–20 ms p50 per UpdateItem), this collapses N events that touch the
// same key from N wire calls to 1 — a 1–2 orders-of-magnitude throughput win when a
// source batch contains repeated keys (the common case for hot rows in social counters,
// hierarchical rollups, and per-tenant aggregates).
//
// Scope. Coalescing is only safe for monoids whose Combine is commutative + cheap to
// apply in memory — KindSum, KindCount, and any KindCustom monoid that opts in via
// monoid.Additive (e.g. decayed-sum). For non-additive monoids (HLL, TopK, Bloom, opaque
// CAS-path BytesStore writers) the Coalescer transparently falls back to per-event
// processing through processor.MergeMany. The caller does not need to gate on this — see
// MergeBatch below.
//
// Failure semantics. Coalescing changes the granularity of failure reporting. A failed
// MergeUpdate at flush fails the flushed key, which corresponds to one-or-more original
// events. Callers that need finer-grained per-event failure reporting (e.g. Kinesis
// Lambda BatchItemFailures with the precise failing sequence number) get back the set
// of EventIDs that contributed to each failed key, but mapping that to a sequence-
// number list is the runtime driver's job. The Coalescer surfaces the EventIDs via the
// FlushError struct; drivers that don't need that granularity ignore it and treat the
// whole batch as failed.
//
// Dedup. The Coalescer applies the configured state.Deduper at AddMany time, the same
// as processor.MergeMany: a duplicate event is dropped before any delta is folded into
// the pending map. This preserves at-least-once semantics across Lambda redeliveries.
//
// Auto-flush triggers. AddMany flushes synchronously when either MaxKeys distinct
// (entity, bucket) pairs are accumulated or FlushTick has elapsed since the last flush
// (only applicable to long-running batches like a multi-hour bootstrap scan). Both
// checks are O(1). Drivers must also call Flush at the end of every source batch
// (Lambda invocation, Kafka PollFetches return, bootstrap source close) to drain
// residual deltas before acking the source.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Default coalescing knobs. Exposed so callers can reason about budgets without
// importing internal constants.
const (
	// DefaultMaxKeys caps the in-memory map size to bound memory under a hot-key
	// batch. ~10k keys × small int64 deltas is single-digit-MB scratch space.
	DefaultMaxKeys = 10_000

	// DefaultFlushTick triggers a periodic flush during long batches (e.g. a Mongo
	// snapshot scan that runs for minutes). Lambda batches typically complete well
	// inside this window so the tick rarely fires there.
	DefaultFlushTick = 1 * time.Second
)

// CoalesceConfig bundles the user-tunable coalescing knobs.
type CoalesceConfig struct {
	// Enabled toggles coalescing. When false, runtimes route every event through
	// processor.MergeMany directly. Default false (no behavior change).
	Enabled bool

	// MaxKeys caps the size of the in-memory coalescing map before an auto-flush.
	// Defaults to DefaultMaxKeys when unset (zero or negative).
	MaxKeys int

	// FlushTick triggers a periodic flush during long batches. Zero or negative
	// disables periodic flush; defaults to DefaultFlushTick when unset.
	FlushTick time.Duration
}

// CoalesceOption mutates a CoalesceConfig. Used by the pipeline DSL's WithCoalesce.
type CoalesceOption func(*CoalesceConfig)

// WithCoalesceMaxKeys sets the max-keys auto-flush trigger.
func WithCoalesceMaxKeys(n int) CoalesceOption {
	return func(c *CoalesceConfig) { c.MaxKeys = n }
}

// WithCoalesceFlushTick sets the time-based auto-flush trigger. Pass 0 to disable.
func WithCoalesceFlushTick(d time.Duration) CoalesceOption {
	return func(c *CoalesceConfig) { c.FlushTick = d }
}

// DefaultCoalesceConfig returns an enabled CoalesceConfig with the canonical defaults.
// Used by Pipeline.WithCoalesce when called without options.
func DefaultCoalesceConfig() CoalesceConfig {
	return CoalesceConfig{
		Enabled:   true,
		MaxKeys:   DefaultMaxKeys,
		FlushTick: DefaultFlushTick,
	}
}

// normalize fills in defaults for unset fields. Always returns a usable config.
func (c CoalesceConfig) normalize() CoalesceConfig {
	if c.MaxKeys <= 0 {
		c.MaxKeys = DefaultMaxKeys
	}
	if c.FlushTick == 0 {
		c.FlushTick = DefaultFlushTick
	}
	return c
}

// Coalescer accumulates (entity, bucket) → combined-delta contributions for a single
// source batch and emits one MergeUpdate per unique key at flush time. Not safe for
// concurrent use — callers should hold one Coalescer per worker goroutine. See package
// doc for the failure-semantics contract.
type Coalescer[V any] struct {
	cfg          *Config
	coalesceCfg  CoalesceConfig
	pipelineName string
	mon          monoid.Monoid[V]
	store        state.Store[V]
	cache        state.Cache[V]
	window       *windowed.Config

	// additive caches IsAdditiveMonoid(mon) — if false, the coalescer routes
	// every event through MergeMany directly (no buffering).
	additive bool

	// pending accumulates the per-batch state. The map's value carries the
	// running combined delta plus the EventIDs that contributed (for
	// fine-grained failure reporting back to the driver).
	pending   map[state.Key]*pendingEntry[V]
	lastFlush time.Time

	// now is the wall-clock source. Overridable for tests.
	now func() time.Time
}

// pendingEntry is the in-flight accumulator for one (entity, bucket). firstSeen is
// the event timestamp we replay to mergeKeyWithRetry at flush; it must fall inside
// the bucket we keyed by, which is guaranteed because all events sharing this map
// entry computed the same BucketID. TTL is re-derived from window.Retention by
// mergeOneAttempt rather than tracked here.
type pendingEntry[V any] struct {
	delta     V
	eventIDs  []string // contributing event IDs, in arrival order
	firstSeen time.Time
}

// NewCoalescer constructs a Coalescer bound to a Pipeline's storage + monoid. The cfg
// argument is the shared processor.Config (recorder, retry budget, dedup); coalesceCfg
// is the per-batch coalescing tuning.
//
// If the monoid is not additive (per monoid.IsAdditiveMonoid), the returned Coalescer
// short-circuits AddMany straight to MergeMany — no buffering, no flush savings.
// Callers can still use it as a uniform interface.
func NewCoalescer[V any](
	cfg *Config,
	coalesceCfg CoalesceConfig,
	pipelineName string,
	mon monoid.Monoid[V],
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) *Coalescer[V] {
	cc := coalesceCfg.normalize()
	c := &Coalescer[V]{
		cfg:          cfg,
		coalesceCfg:  cc,
		pipelineName: pipelineName,
		mon:          mon,
		store:        store,
		cache:        cache,
		window:       window,
		additive:     mon != nil && monoid.IsAdditiveMonoid(mon),
		pending:      make(map[state.Key]*pendingEntry[V], cc.MaxKeys),
		now:          time.Now,
	}
	c.lastFlush = c.now()
	return c
}

// IsAdditive reports whether the bound monoid was eligible for coalescing. Drivers can
// inspect this to surface a metric or log line on startup.
func (c *Coalescer[V]) IsAdditive() bool { return c.additive }

// PendingKeys returns the number of unique (entity, bucket) pairs currently buffered.
// Exposed for tests and operational metrics.
func (c *Coalescer[V]) PendingKeys() int { return len(c.pending) }

// AddMany folds an event's (keys, delta) into the pending map. Dedup is applied once
// up-front. If the monoid is not additive, AddMany delegates straight to MergeMany so
// every event hits the store exactly as before — no behavior change. Otherwise the
// delta is Combined into the per-key accumulator and a synchronous auto-flush may fire
// if MaxKeys or FlushTick triggers.
//
// Returns nil on success. A non-nil error is returned only when an auto-flush ran and
// some flushed key exhausted its retry budget — the error wraps the underlying
// FlushError.
func (c *Coalescer[V]) AddMany(
	ctx context.Context,
	eventID string,
	eventTime time.Time,
	keys []string,
	delta V,
) error {
	if len(keys) == 0 {
		return nil
	}

	if !c.additive {
		// Non-additive monoid: bypass the coalescing buffer entirely. We preserve
		// the existing MergeMany semantics (per-event retry, per-event dedup,
		// per-event metric).
		return MergeMany(ctx, c.cfg, c.pipelineName, eventID, eventTime,
			keys, delta, c.store, c.cache, c.window)
	}

	// Dedup is checked once per event before any key is buffered. This matches
	// MergeMany's contract: a duplicate event short-circuits the entire fanout.
	if c.cfg.Dedup != nil && eventID != "" {
		first, err := c.cfg.Dedup.MarkSeen(ctx, eventID)
		if err != nil {
			c.cfg.Recorder.RecordError(c.pipelineName,
				fmt.Errorf("dedup MarkSeen %q: %w", eventID, err))
			// Fail-open: same policy as MergeMany. Fall through to buffer.
		} else if !first {
			c.cfg.Recorder.RecordEvent(c.pipelineName + ":dedup_skip")
			return nil
		}
	}

	var bucket int64
	if c.window != nil {
		bucket = c.window.BucketID(eventTime)
	}

	for _, entity := range keys {
		k := state.Key{Entity: entity, Bucket: bucket}
		entry, ok := c.pending[k]
		if !ok {
			entry = &pendingEntry[V]{
				delta:     c.mon.Identity(),
				firstSeen: eventTime,
			}
			c.pending[k] = entry
		}
		entry.delta = c.mon.Combine(entry.delta, delta)
		if eventID != "" {
			entry.eventIDs = append(entry.eventIDs, eventID)
		}
	}

	// Auto-flush triggers.
	if len(c.pending) >= c.coalesceCfg.MaxKeys {
		c.cfg.Recorder.RecordEvent(c.pipelineName + ":coalesce_flush_maxkeys")
		return c.Flush(ctx)
	}
	if c.coalesceCfg.FlushTick > 0 && c.now().Sub(c.lastFlush) >= c.coalesceCfg.FlushTick {
		c.cfg.Recorder.RecordEvent(c.pipelineName + ":coalesce_flush_tick")
		return c.Flush(ctx)
	}
	return nil
}

// FlushError is returned (wrapped) when one or more keys fail to merge after retries.
// FailedKeys preserves per-key failure info; drivers that need per-event failure
// reporting (e.g. Kinesis BatchItemFailures) can fan out KeyFailure.ContributingIDs to
// the original record IDs they kept alongside the coalescer.
type FlushError struct {
	FailedKeys []KeyFailure
}

// KeyFailure is one entry in a FlushError.
type KeyFailure struct {
	Key             state.Key
	Err             error
	ContributingIDs []string
}

// Error implements error.
func (e *FlushError) Error() string {
	if len(e.FailedKeys) == 0 {
		return "coalesce flush: no failures"
	}
	if len(e.FailedKeys) == 1 {
		return fmt.Sprintf("coalesce flush: key %q failed: %v",
			e.FailedKeys[0].Key.Entity, e.FailedKeys[0].Err)
	}
	return fmt.Sprintf("coalesce flush: %d keys failed (first: %q: %v)",
		len(e.FailedKeys), e.FailedKeys[0].Key.Entity, e.FailedKeys[0].Err)
}

// Unwrap exposes the first per-key error for errors.Is/As walks.
func (e *FlushError) Unwrap() error {
	if len(e.FailedKeys) == 0 {
		return nil
	}
	return e.FailedKeys[0].Err
}

// Flush commits all pending (key, delta) pairs by issuing one MergeUpdate per unique
// key (plus the matching cache MergeUpdate). Each per-key write runs through the
// processor's retry/backoff loop. Successful keys remain successful even if a sibling
// key fails — there is no rollback, and the idempotent-merge contract handles a
// re-delivery.
//
// Returns nil when every key succeeded. Returns a *FlushError when one or more keys
// exhausted their retry budget; cancellation errors from the context propagate directly.
// Flush is safe to call when the pending map is empty (returns nil with no work).
func (c *Coalescer[V]) Flush(ctx context.Context) error {
	if len(c.pending) == 0 {
		c.lastFlush = c.now()
		return nil
	}

	var failures []KeyFailure
	for k, entry := range c.pending {
		// Re-use the existing per-key retry path for full parity with MergeMany.
		// fakeEventID is "" because dedup already ran at AddMany time.
		err := mergeKeyWithRetry(ctx, c.cfg, c.pipelineName, "",
			entry.firstSeen, k.Entity, entry.delta, c.store, c.cache, c.window)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Reset state and propagate. Pending entries are dropped; the
				// at-least-once redelivery will re-emit them.
				c.pending = make(map[state.Key]*pendingEntry[V], c.coalesceCfg.MaxKeys)
				c.lastFlush = c.now()
				return err
			}
			failures = append(failures, KeyFailure{
				Key:             k,
				Err:             err,
				ContributingIDs: entry.eventIDs,
			})
		}
	}

	// Reset state before returning — even on partial failure the caller must not
	// re-flush the same deltas (that would double-count successful keys).
	c.pending = make(map[state.Key]*pendingEntry[V], c.coalesceCfg.MaxKeys)
	c.lastFlush = c.now()

	if len(failures) > 0 {
		return &FlushError{FailedKeys: failures}
	}
	return nil
}

// MergeBatch is a convenience wrapper around NewCoalescer + a loop over events + Flush.
// It is the single-line entrypoint drivers can call when they hold an entire source
// batch in memory (Kinesis Lambda Records, bootstrap snapshot chunk, etc).
//
// The events slice is iterated in order; each event's keys are derived via keysFn, each
// delta via valueFn. After the loop, MergeBatch calls Flush. Returns nil on full
// success, or a wrapped *FlushError listing failed keys.
//
// MergeBatch is a zero-allocation passthrough for non-additive monoids (it just loops
// over MergeMany), so drivers can call it unconditionally regardless of the pipeline's
// monoid choice.
func MergeBatch[T any, V any](
	ctx context.Context,
	cfg *Config,
	coalesceCfg CoalesceConfig,
	pipelineName string,
	mon monoid.Monoid[V],
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	events []T,
	eventIDFn func(T) string,
	eventTimeFn func(T) time.Time,
	keysFn func(T) []string,
	valueFn func(T) V,
) error {
	c := NewCoalescer(cfg, coalesceCfg, pipelineName, mon, store, cache, window)
	for _, ev := range events {
		if err := c.AddMany(ctx, eventIDFn(ev), eventTimeFn(ev), keysFn(ev), valueFn(ev)); err != nil {
			// An auto-flush failed and reset the buffer; nothing to drain.
			return err
		}
	}
	return c.Flush(ctx)
}

// withClock overrides the wall-clock for tests. Unexported; tests use it via the
// package-internal helper in coalesce_test.go.
func (c *Coalescer[V]) withClock(now func() time.Time) {
	c.now = now
	c.lastFlush = now()
}
