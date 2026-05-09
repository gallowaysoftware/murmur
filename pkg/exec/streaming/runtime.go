// Package streaming runs a Murmur Pipeline in Live mode: it reads records from the
// pipeline's Source, applies the user's key/value extractors and monoid Combine, writes
// to the primary state store (and optionally a cache), and Acks the source record.
//
// Phase 1 is intentionally a single-goroutine sequential loop. Per-partition parallelism
// is a Phase 2 optimization gated on real benchmarks. franz-go batches fetches under the
// hood; for counter-class workloads at moderate rates, single-goroutine processing is
// not the bottleneck.
package streaming

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	processor.Config
	deadLetter       func(eventID string, err error)
	batchWindow      time.Duration
	maxBatchSize     int
	concurrency      int
	keyDebounce      time.Duration
	keyDebounceMax   int
	valueDebounce    time.Duration
	valueDebounceMax int

	// debouncer is materialized once in Run when keyDebounce > 0 and
	// reused across the per-record processing path. Concurrency-safe.
	debouncer *keyDebouncer

	// valueDebouncer is materialized once in Run when
	// valueDebounce > 0; same shape as debouncer but keyed by
	// (entityKey, fingerprintedValue).
	valueDebouncer *valueDebouncer
}

// debounceAllow returns false when a record's first-emitted key was
// already seen within the debounce window. Caller should Ack-and-skip
// when this returns false. Returns true (allow) when:
//   - the debouncer is unconfigured
//   - keys is empty (the underlying MergeMany no-ops on no keys anyway)
//   - the first key has not been seen within the window
func (c *runConfig) debounceAllow(keys []string) bool {
	if c.debouncer == nil {
		return true
	}
	if len(keys) == 0 {
		return true
	}
	return !c.debouncer.shouldDrop(keys[0])
}

// WithMetrics installs a metrics.Recorder on the runtime. Defaults to metrics.Noop{}.
func WithMetrics(r metrics.Recorder) RunOption {
	return func(c *runConfig) {
		if r != nil {
			c.Recorder = r
		}
	}
}

// WithMaxAttempts sets the per-record retry budget. Defaults to 3. A record that
// errors all the way through MaxAttempts is dead-lettered (see WithDeadLetter)
// and the runtime continues with the next record rather than crashing.
//
// Set to 1 to disable retries (any error → dead-letter immediately).
func WithMaxAttempts(n int) RunOption {
	return func(c *runConfig) {
		if n >= 1 {
			c.MaxAttempts = n
		}
	}
}

// WithRetryBackoff sets the per-attempt sleep schedule. Backoff doubles after
// each failure starting from base, capped at max, with full jitter. Defaults
// to 50 ms base / 5 s max.
func WithRetryBackoff(base, max time.Duration) RunOption {
	return func(c *runConfig) {
		if base > 0 {
			c.BackoffBase = base
		}
		if max > 0 {
			c.BackoffMax = max
		}
	}
}

// WithDeadLetter installs a callback invoked when a record fails every
// attempt. Use it to push the record's EventID (and the underlying error) to
// a DLQ table / topic / structured-log sink. Default is a no-op — the record
// is silently dropped and the runtime moves on. The runtime ALSO calls
// recorder.RecordError so DLQ counts surface in /api metrics regardless.
func WithDeadLetter(fn func(eventID string, err error)) RunOption {
	return func(c *runConfig) {
		if fn != nil {
			c.deadLetter = fn
		}
	}
}

// WithBatchWindow enables write aggregation: per-(entity, bucket) deltas
// are accumulated in memory for `window` time, then flushed as a SINGLE
// store.MergeUpdate per key. This is the only way to keep up with hot
// keys at scale — a celebrity post taking 50k like-events/sec lands as
// ONE atomic-ADD per flush window per worker instead of 50k.
//
// Tradeoffs:
//
//   - Latency: a record's contribution is invisible to readers for up to
//     `window` time after acceptance. Default unset (no batching) gives
//     immediate-merge semantics; common production values are 500ms–5s
//     depending on read-staleness tolerance.
//   - Durability under crash: records are Ack'd to the source AFTER the
//     batch flushes, so a worker crash loses at most `window`-worth of
//     in-flight records (the source replays them on restart, dedup
//     catches the redelivery).
//   - Memory: at most `maxBatch` records per (entity, bucket) before
//     forced flush. Default 1024 if unset. The number of concurrent keys
//     in flight is unbounded — for high-cardinality pipelines (per-user
//     keys), keep `window` short so each batch stays small.
//
// Pass window = 0 (or omit the option) to disable batching entirely;
// records flow through processor.MergeMany one at a time.
//
// Strongly recommended for any pipeline with a known hot-key distribution.
func WithBatchWindow(window time.Duration, maxBatch int) RunOption {
	return func(c *runConfig) {
		if window > 0 {
			c.batchWindow = window
		}
		if maxBatch > 0 {
			c.maxBatchSize = maxBatch
		}
	}
}

// WithConcurrency configures N worker goroutines that drain the source
// channel concurrently. Default 1 (the historical single-goroutine
// loop). When n > 1, records are routed by hash(first-emitted-key) to
// a worker, so SAME-key records always land on the same worker — that
// preserves per-key order and keeps the aggregator's per-(entity, bucket)
// lock contention tight.
//
// When to raise it:
//
//   - The single-goroutine ceiling (~5–10k events/s/worker against
//     DDB-local; documented in README) is your bottleneck.
//   - Fan-out via additional Kafka partitions isn't an option (e.g.,
//     downstream key cardinality demands a single partition).
//
// When NOT to raise it:
//
//   - Your store is CAS-heavy (BytesStore on a hot key). N workers
//     pre-routing by key removes cross-worker CAS contention but a
//     single worker on a hot CAS key is already the bottleneck.
//   - You're using `WithBatchWindow` aggressively. The aggregator's
//     per-key lock already serializes hot-key writes; concurrency
//     adds dispatch overhead without throughput gain on a single hot
//     key.
//
// The router routes by first-key hash. Hierarchical-rollup pipelines
// (KeyByMany) partition on the FIRST emitted key — so a record's
// "post:X" delta lands on the same worker as another "post:X" record,
// even if the records also fan out to "country:Y" / "global" keys
// that happen to hash differently. This keeps the per-key ordering
// guarantee meaningful where it matters most.
func WithConcurrency(n int) RunOption {
	return func(c *runConfig) {
		if n >= 1 {
			c.concurrency = n
		}
	}
}

// WithDedup installs a state.Deduper. Each record's EventID is claimed via
// MarkSeen before the merge runs; if MarkSeen reports the EventID was
// already claimed (a duplicate from a worker crash mid-write), the runtime
// skips the merge and Acks the record so the source advances.
//
// This makes at-least-once delivery idempotent at the monoid layer for any
// pipeline whose Combine is non-commutative or non-idempotent (e.g. Sum,
// HLL.Add). Pipelines whose Combine is already idempotent (Set, Min, Max)
// don't strictly need a Deduper but adding one is harmless.
//
// The Deduper itself is typically backed by a small DDB table with TTL —
// see pkg/state/dynamodb.NewDeduper.
func WithDedup(d state.Deduper) RunOption {
	return func(c *runConfig) {
		if d != nil {
			c.Dedup = d
		}
	}
}

// Run executes the pipeline in Live mode until ctx is canceled or the source returns.
// Returns nil on graceful shutdown.
//
// Per-record processing errors are retried up to MaxAttempts with exponential
// backoff, then dead-lettered (callback + RecordError) and skipped. The runtime
// itself only returns early if the source returns or ctx is canceled — a
// stuck downstream does not take the worker down with it.
//
// The pipeline must have been configured with From / Key / Value / Aggregate / StoreIn
// before Run is called; Build is invoked internally to validate.
func Run[T any, V any](ctx context.Context, p *pipeline.Pipeline[T, V], opts ...RunOption) error {
	if err := p.Build(); err != nil {
		return fmt.Errorf("streaming.Run: %w", err)
	}
	if p.Source() == nil {
		return fmt.Errorf("streaming.Run: %w", pipeline.ErrMissingSource)
	}

	cfg := runConfig{Config: processor.Defaults()}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.keyDebounce > 0 {
		cfg.debouncer = newKeyDebouncer(cfg.keyDebounce, cfg.keyDebounceMax)
	}
	if cfg.valueDebounce > 0 {
		cfg.valueDebouncer = newValueDebouncer(cfg.valueDebounce, cfg.valueDebounceMax)
	}

	name := p.Name()
	src := p.Source()
	keysFn := p.KeysFn() // multi-key aware; falls back to wrapping KeyFn in a 1-element slice
	valueFn := p.ValueFn()
	store := p.Store()
	cache := p.CacheStore()
	window := p.Window()

	// Buffered channel decouples the source's network polling from the processing loop.
	// 1024 records of slack absorbs short DDB latency spikes without backpressuring Kafka.
	records := make(chan source.Record[T], 1024)

	// Source pump goroutine.
	srcDone := make(chan error, 1)
	go func() {
		srcDone <- src.Read(ctx, records)
		close(records)
	}()

	// If write aggregation is enabled, spin up the aggregator + flush loop.
	// Records still pass through processWithRetry's retry-and-deadletter
	// shape, but the actual store write is batched per (entity, bucket).
	var agg *aggregator[T, V]
	var flushDone chan struct{}
	if cfg.batchWindow > 0 {
		agg = newAggregator(&cfg, p.Monoid(), keysFn, valueFn, store, cache, window, name, cfg.maxBatchSize)
		flushDone = make(chan struct{})
		go func() {
			defer close(flushDone)
			agg.runFlushLoop(ctx, cfg.batchWindow)
		}()
	}

	// Concurrency > 1: route records to N worker goroutines by
	// hash(first-key). Same-key records always go to the same worker,
	// preserving per-key order. concurrency = 1 retains the historical
	// single-goroutine loop.
	if cfg.concurrency > 1 {
		err := runConcurrent(ctx, name, records, srcDone, keysFn, valueFn, store, cache, window, agg, &cfg)
		if agg != nil {
			<-flushDone
		}
		return err
	}

	for {
		select {
		case <-ctx.Done():
			if agg != nil {
				<-flushDone // wait for final drain
			}
			return nil
		case err := <-srcDone:
			drainAndProcess(ctx, name, records, keysFn, valueFn, store, cache, window, agg, &cfg)
			if agg != nil {
				// Source closed naturally; force a final flush in addition
				// to whatever the flush loop was doing.
				agg.flushAll(ctx)
			}
			return err
		case rec, ok := <-records:
			if !ok {
				return <-srcDone
			}
			if agg != nil {
				if dup := agg.accept(ctx, rec); dup && rec.Ack != nil {
					_ = rec.Ack()
				}
			} else {
				processWithRetry(ctx, name, rec, keysFn, valueFn, store, cache, window, &cfg)
			}
		}
	}
}

// runConcurrent drains `records` across cfg.concurrency worker
// goroutines. A small router goroutine reads records, hashes the
// first-emitted-key, and dispatches to the matching worker channel.
// Same-key records always land on the same worker.
//
// Returns when the source channel closes or ctx cancels. Workers are
// drained on the way out so no record is dropped mid-flight.
func runConcurrent[T any, V any](
	ctx context.Context,
	name string,
	records <-chan source.Record[T],
	srcDone <-chan error,
	keysFn func(T) []string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	agg *aggregator[T, V],
	cfg *runConfig,
) error {
	n := cfg.concurrency
	workers := make([]chan source.Record[T], n)
	for i := range workers {
		// Per-worker buffer of 256 (1024/4 ~ chunked across N workers
		// in expectation; absorb short latency spikes without forcing
		// the router to block).
		workers[i] = make(chan source.Record[T], 256)
	}

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(ch <-chan source.Record[T]) {
			defer wg.Done()
			for rec := range ch {
				if agg != nil {
					if dup := agg.accept(ctx, rec); dup && rec.Ack != nil {
						_ = rec.Ack()
					}
				} else {
					processWithRetry(ctx, name, rec, keysFn, valueFn, store, cache, window, cfg)
				}
			}
		}(workers[i])
	}

	// Router: route every record to one worker by first-key hash.
	// Records with no emitted keys (KeysFn returns nil/empty — rare)
	// route to worker 0; processor.MergeMany handles the empty-keys
	// case as a no-op.
	routerDone := make(chan struct{})
	go func() {
		defer close(routerDone)
		defer func() {
			for _, ch := range workers {
				close(ch)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case rec, ok := <-records:
				if !ok {
					return
				}
				idx := routeKey(keysFn(rec.Value), n)
				select {
				case workers[idx] <- rec:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	<-routerDone
	wg.Wait()
	return <-srcDone
}

// routeKey maps the first-emitted key to a worker index. FNV-1a hash
// over the byte content; cheap and good-enough distribution. No keys
// → worker 0 (caller's processor.MergeMany no-ops on empty key list).
func routeKey(keys []string, n int) int {
	if len(keys) == 0 || n <= 1 {
		return 0
	}
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	for _, c := range []byte(keys[0]) {
		h ^= uint32(c)
		h *= prime
	}
	return int(h % uint32(n))
}

func drainAndProcess[T any, V any](
	ctx context.Context,
	name string,
	records <-chan source.Record[T],
	keysFn func(T) []string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	agg *aggregator[T, V],
	cfg *runConfig,
) {
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			return
		case r, ok := <-records:
			if !ok {
				return
			}
			if agg != nil {
				if dup := agg.accept(ctx, r); dup && r.Ack != nil {
					_ = r.Ack()
				}
			} else {
				processWithRetry(ctx, name, r, keysFn, valueFn, store, cache, window, cfg)
			}
		}
	}
}

// processWithRetry delegates per-record processing to processor.MergeMany
// (retries, dedup, metrics, multi-key fanout) and adds the streaming-specific
// Ack-and-skip poison-record handling on top. Returns no error so the caller's
// loop is not coupled to this record's outcome.
func processWithRetry[T any, V any](
	ctx context.Context,
	name string,
	rec source.Record[T],
	keysFn func(T) []string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	cfg *runConfig,
) {
	eventTime := rec.EventTime
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
	keys := keysFn(rec.Value)
	if !cfg.debounceAllow(keys) {
		// Duplicate-key record within the debounce window — drop the
		// processor work but still Ack so the source advances.
		cfg.Recorder.RecordEvent(name + ":debounce_skip")
		if rec.Ack != nil {
			_ = rec.Ack()
		}
		return
	}
	v := valueFn(rec.Value)
	if cfg.valueDebouncer != nil && len(keys) > 0 {
		// Same (entity, value) seen within the value-debounce window —
		// drop without processor work, but Ack so the source advances.
		if cfg.valueDebouncer.shouldDrop(keys[0], fingerprintValue(v)) {
			cfg.Recorder.RecordEvent(name + ":value_debounce_skip")
			if rec.Ack != nil {
				_ = rec.Ack()
			}
			return
		}
	}
	err := processor.MergeMany(ctx, &cfg.Config, name, rec.EventID, eventTime,
		keys, v, store, cache, window)
	if err != nil {
		// Either context cancellation (just return; the runtime is exiting
		// anyway) or retry exhaustion. processor.MergeOne already recorded
		// the dead_letter metric — we just need to invoke the user's
		// dead-letter callback and Ack past the poison record.
		if cfg.deadLetter != nil {
			cfg.deadLetter(rec.EventID, err)
		}
		if rec.Ack != nil {
			_ = rec.Ack()
		}
		return
	}

	// Successful merge (or dedup-skip). Ack so the source advances; surface
	// Ack errors to the recorder rather than as a runtime error.
	if rec.Ack != nil {
		if err := rec.Ack(); err != nil {
			cfg.Recorder.RecordError(name, fmt.Errorf("source Ack: %w", err))
		}
	}
}
