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
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	recorder    metrics.Recorder
	maxAttempts int
	backoffBase time.Duration
	backoffMax  time.Duration
	deadLetter  func(eventID string, err error)
	dedup       state.Deduper
}

// WithMetrics installs a metrics.Recorder on the runtime. Defaults to metrics.Noop{}.
func WithMetrics(r metrics.Recorder) RunOption {
	return func(c *runConfig) {
		if r != nil {
			c.recorder = r
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
			c.maxAttempts = n
		}
	}
}

// WithRetryBackoff sets the per-attempt sleep schedule. Backoff doubles after
// each failure starting from base, capped at max, with full jitter. Defaults
// to 50 ms base / 5 s max.
func WithRetryBackoff(base, max time.Duration) RunOption {
	return func(c *runConfig) {
		if base > 0 {
			c.backoffBase = base
		}
		if max > 0 {
			c.backoffMax = max
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

// WithDedup installs a state.Deduper. Each record's EventID is claimed via
// MarkSeen before processOne runs; if MarkSeen reports the EventID was
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
			c.dedup = d
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

	cfg := runConfig{
		recorder:    metrics.Noop{},
		maxAttempts: 3,
		backoffBase: 50 * time.Millisecond,
		backoffMax:  5 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}

	name := p.Name()
	src := p.Source()
	keyFn := p.KeyFn()
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

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-srcDone:
			drainAndProcess(ctx, name, records, keyFn, valueFn, store, cache, window, &cfg)
			return err
		case rec, ok := <-records:
			if !ok {
				return <-srcDone
			}
			processWithRetry(ctx, name, rec, keyFn, valueFn, store, cache, window, &cfg)
		}
	}
}

func drainAndProcess[T any, V any](
	ctx context.Context,
	name string,
	records <-chan source.Record[T],
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
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
			processWithRetry(ctx, name, r, keyFn, valueFn, store, cache, window, cfg)
		}
	}
}

// processWithRetry retries processOne on transient failure with exponential
// backoff, then dead-letters and skips. Returns no error so the caller's loop
// is not coupled to this record's outcome.
//
// If a Deduper is configured, the EventID is claimed before any processing;
// duplicates are Ack'd and recorded under name+":dedup_skip" so the metrics
// recorder surfaces dup rate alongside event rate.
func processWithRetry[T any, V any](
	ctx context.Context,
	name string,
	rec source.Record[T],
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	cfg *runConfig,
) {
	if cfg.dedup != nil && rec.EventID != "" {
		first, err := cfg.dedup.MarkSeen(ctx, rec.EventID)
		if err != nil {
			// Dedup backend is sick — surface as an error but DO NOT skip the
			// record (would silently double-count in steady state when the
			// dedup table comes back). Fall through to normal processing.
			cfg.recorder.RecordError(name, fmt.Errorf("dedup MarkSeen %q: %w", rec.EventID, err))
		} else if !first {
			// Duplicate. Ack so the source advances; record the skip and exit.
			cfg.recorder.RecordEvent(name + ":dedup_skip")
			if rec.Ack != nil {
				_ = rec.Ack()
			}
			return
		}
	}

	var lastErr error
	for attempt := 0; attempt < cfg.maxAttempts; attempt++ {
		if attempt > 0 {
			if err := backoffWait(ctx, cfg, attempt); err != nil {
				return // ctx canceled mid-backoff
			}
			cfg.recorder.RecordEvent(name + ":retry")
		}
		if err := processOne(ctx, name, rec, keyFn, valueFn, store, cache, window, cfg.recorder); err != nil {
			// Cancellation propagates immediately — don't burn retries on a dying ctx.
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			lastErr = err
			continue
		}
		return // success
	}

	// Exhausted retries — dead-letter and continue.
	wrapped := fmt.Errorf("pipeline %q event %q dead-lettered after %d attempts: %w",
		name, rec.EventID, cfg.maxAttempts, lastErr)
	cfg.recorder.RecordError(name, wrapped)
	cfg.recorder.RecordEvent(name + ":dead_letter")
	if cfg.deadLetter != nil {
		cfg.deadLetter(rec.EventID, lastErr)
	}
	// Ack so the source advances past this poison record. Without Ack the
	// next consumer-group rebalance would redeliver the same record forever.
	if rec.Ack != nil {
		_ = rec.Ack()
	}
}

func backoffWait(ctx context.Context, cfg *runConfig, attempt int) error {
	d := cfg.backoffBase << (attempt - 1)
	if d > cfg.backoffMax {
		d = cfg.backoffMax
	}
	if d > 0 {
		d += time.Duration(rand.Int64N(int64(d / 2))) // full jitter
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func processOne[T any, V any](
	ctx context.Context,
	name string,
	rec source.Record[T],
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
	rec_ metrics.Recorder,
) error {
	entity := keyFn(rec.Value)
	delta := valueFn(rec.Value)

	sk := state.Key{Entity: entity}
	var ttl time.Duration
	if window != nil {
		t := rec.EventTime
		if t.IsZero() {
			t = time.Now()
		}
		sk.Bucket = window.BucketID(t)
		ttl = window.Retention
	}

	storeStart := time.Now()
	if err := store.MergeUpdate(ctx, sk, delta, ttl); err != nil {
		return fmt.Errorf("store MergeUpdate: %w", err)
	}
	rec_.RecordLatency(name, "store_merge", time.Since(storeStart))

	if cache != nil {
		cacheStart := time.Now()
		if err := cache.MergeUpdate(ctx, sk, delta, ttl); err != nil {
			rec_.RecordError(name, fmt.Errorf("cache MergeUpdate: %w", err))
		}
		rec_.RecordLatency(name, "cache_merge", time.Since(cacheStart))
	}

	if rec.Ack != nil {
		if err := rec.Ack(); err != nil {
			return fmt.Errorf("source Ack: %w", err)
		}
	}
	rec_.RecordEvent(name)
	return nil
}
