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
	deadLetter func(eventID string, err error)
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

// processWithRetry delegates per-record processing to processor.MergeOne
// (retries, dedup, metrics) and adds the streaming-specific Ack-and-skip
// poison-record handling on top. Returns no error so the caller's loop is
// not coupled to this record's outcome.
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
	eventTime := rec.EventTime
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
	err := processor.MergeOne(ctx, &cfg.Config, name, rec.EventID, eventTime,
		rec.Value, keyFn, valueFn, store, cache, window)
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
