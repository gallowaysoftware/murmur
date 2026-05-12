// Package bootstrap drives a Murmur pipeline in Bootstrap mode: it reads from a
// SnapshotSource (Mongo collection scan, DynamoDB ParallelScan, etc.), applies the
// pipeline's key/value extractors and monoid Combine, and writes to the primary state
// store — populating initial state when the live event stream lacks full history.
//
// The pattern matches Debezium's "snapshot then stream":
//
//  1. Capture a HandoffToken from the snapshot source. This is the resume position the
//     live source should pick up from once bootstrap completes.
//  2. Scan the snapshot to completion, applying each record through the pipeline.
//  3. Return the captured token. The deployment system stores it and starts the live
//     runtime configured to begin from that point.
//
// At-least-once dedup at the state store handles re-emissions if bootstrap is retried.
// HandoffToken is opaque to the bootstrap runtime — the live source's NewSourceFromToken
// (or equivalent) interprets it.
package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	processor.Config
	// failOnError, when true, surfaces the first dead-lettered record's
	// error to the caller and aborts the bootstrap. Default false: dead-
	// lettered records are recorded via the metrics.Recorder and the
	// bootstrap continues — a single poison row in a Mongo collection
	// shouldn't fail the whole snapshot, especially for retry-driven
	// re-runs.
	failOnError bool

	// batchTick is the periodic flush interval for RecordBatch metrics. A
	// snapshot draining 40M documents emits one RecordBatch per tick with
	// the records-since-last-tick count and elapsed wall time, so the
	// "events/s by mode" dashboard updates without waiting for the whole
	// bootstrap to finish. Default 1 s.
	batchTick time.Duration
}

// WithMetrics installs a metrics.Recorder. Defaults to metrics.Noop{}.
func WithMetrics(r metrics.Recorder) RunOption {
	return func(c *runConfig) {
		if r != nil {
			c.Recorder = r
		}
	}
}

// WithMaxAttempts sets the per-record retry budget for transient store
// failures. Defaults to 3. A bootstrap that fails-fast on the first
// throttled DDB write is unfit for snapshotting a large collection — a
// 30-minute scan needs retries to absorb intermittent backpressure.
func WithMaxAttempts(n int) RunOption {
	return func(c *runConfig) {
		if n >= 1 {
			c.MaxAttempts = n
		}
	}
}

// WithRetryBackoff configures the per-attempt sleep schedule. Doubles
// after each failure starting from base, capped at max, with full jitter.
// Defaults to 50 ms / 5 s — same as the streaming runtime.
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

// WithDedup installs a state.Deduper for snapshot-side dedup. Useful when a
// bootstrap is re-run (operator retry, partial-failure recovery): without it
// every document is folded again on re-run; with it only documents not yet
// claimed are processed. The Deduper sees the same EventID derivation the
// SnapshotSource emits — for Mongo that's the document _id, so re-running
// against the same collection is idempotent.
func WithDedup(d state.Deduper) RunOption {
	return func(c *runConfig) {
		if d != nil {
			c.Dedup = d
		}
	}
}

// WithFailOnError configures whether the bootstrap aborts on the first
// dead-lettered record (after retries exhausted). Default false — the
// runtime records the error via metrics.Recorder and continues. Set true
// when the snapshot must complete fully or not at all (e.g., a one-shot
// migration where a missing entity is a correctness bug, not a tolerable
// blip). The streaming runtime never aborts on poison records; bootstrap
// is permissive by default for the same reason.
func WithFailOnError(v bool) RunOption {
	return func(c *runConfig) {
		c.failOnError = v
	}
}

// WithBatchTick overrides the RecordBatch flush interval. The default 1 s
// fits a multi-hour bootstrap where dashboard granularity matters more
// than emission cost. Pass d <= 0 to disable periodic batch flushes (a
// single batch is still emitted at completion).
func WithBatchTick(d time.Duration) RunOption {
	return func(c *runConfig) {
		c.batchTick = d
	}
}

// Run drives the bootstrap. Returns the captured HandoffToken on success; the deployment
// system persists it and hands it to the live runtime on transition.
func Run[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	src snapshot.Source[T],
	opts ...RunOption,
) (snapshot.HandoffToken, error) {
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("bootstrap.Run: %w", err)
	}
	if src == nil {
		return nil, fmt.Errorf("bootstrap.Run: snapshot source is nil")
	}
	cfg := runConfig{Config: processor.Defaults(), batchTick: time.Second}
	for _, o := range opts {
		o(&cfg)
	}

	token, err := src.CaptureHandoff(ctx)
	if err != nil {
		cfg.Recorder.RecordError(p.Name(), fmt.Errorf("capture handoff: %w", err))
		return nil, fmt.Errorf("capture handoff: %w", err)
	}

	name := p.Name()
	keysFn := p.KeysFn() // multi-key aware; falls back to wrapping KeyFn in a 1-element slice
	valueFn := p.ValueFn()
	store := p.Store()
	cache := p.CacheStore()
	window := p.Window()

	records := make(chan source.Record[T], 1024)
	scanErr := make(chan error, 1)
	go func() {
		scanErr <- src.Scan(ctx, records)
		close(records)
	}()

	// Batch metrics: count records and wall time since the last RecordBatch
	// emit. emitBatch is called every batchTick and once at completion so
	// dashboards can plot throughput by mode in near-real-time during a
	// multi-hour snapshot.
	var (
		batchCount int
		batchStart = time.Now()
	)
	emitBatch := func() {
		if batchCount == 0 {
			return
		}
		cfg.Recorder.RecordBatch(name, metrics.ModeBootstrap, batchCount, time.Since(batchStart))
		batchCount = 0
		batchStart = time.Now()
	}

	var tickC <-chan time.Time
	if cfg.batchTick > 0 {
		t := time.NewTicker(cfg.batchTick)
		defer t.Stop()
		tickC = t.C
	}

	drain := func(rec source.Record[T]) error {
		eventTime := rec.EventTime
		if eventTime.IsZero() {
			eventTime = time.Now()
		}
		err := processor.MergeMany(ctx, &cfg.Config, name, rec.EventID, eventTime,
			keysFn(rec.Value), valueFn(rec.Value), store, cache, window)
		if err != nil {
			// Either context cancellation or retry exhaustion. Either way,
			// the record is dead-lettered (processor.MergeMany already
			// recorded the metric). Decide between abort and continue
			// per the failOnError flag.
			if cfg.failOnError {
				return err
			}
			// Continue: the metrics surface carries the dead-letter event,
			// the operator sees the count, and the bootstrap finishes.
		}
		if rec.Ack != nil {
			if err := rec.Ack(); err != nil {
				cfg.Recorder.RecordError(name, fmt.Errorf("snapshot ack: %w", err))
			}
		}
		batchCount++
		return nil
	}

	for {
		select {
		case <-tickC:
			emitBatch()
		case rec, ok := <-records:
			if !ok {
				emitBatch()
				if err := <-scanErr; err != nil {
					cfg.Recorder.RecordError(name, fmt.Errorf("snapshot scan: %w", err))
					return nil, fmt.Errorf("snapshot scan: %w", err)
				}
				return token, nil
			}
			if err := drain(rec); err != nil {
				emitBatch()
				return nil, err
			}
		}
	}
}
