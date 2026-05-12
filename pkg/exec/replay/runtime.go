// Package replay drives a Murmur pipeline in Replay mode: it consumes historical events
// from a replay.Driver (typically S3-archived Firehose/Kafka-Connect output, or a Kafka
// offset range under tiered storage) and applies the same monoid Combine the live
// runtime would.
//
// The standard backfill pattern: configure the pipeline's StoreIn to point at a SHADOW
// state table (separate from the live one), run replay to completion, then atomically
// swap the live query layer's pointer to the shadow. The framework treats the shadow-
// table swap as a deployment concern; the runtime here just drives records through
// the pipeline.
//
// At-least-once dedup at the state-store level handles re-runs safely.
package replay

import (
	"fmt"
	"time"

	"context"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/replay"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	processor.Config
	failOnError bool

	// batchTick is the periodic flush interval for RecordBatch metrics. A
	// multi-day replay emits one RecordBatch per tick with the records-
	// since-last-tick count and elapsed wall time, so the "events/s by
	// mode" dashboard updates without waiting for the whole replay to
	// finish. Default 1 s.
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
// failures. Defaults to 3. Replay over a multi-day archive needs
// retries to absorb intermittent backpressure — without them, a single
// throttled DDB write fails the whole replay.
func WithMaxAttempts(n int) RunOption {
	return func(c *runConfig) {
		if n >= 1 {
			c.MaxAttempts = n
		}
	}
}

// WithRetryBackoff configures the per-attempt sleep schedule. Doubles
// after each failure starting from base, capped at max, with full jitter.
// Defaults to 50 ms / 5 s.
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

// WithDedup installs a state.Deduper. The replay driver typically emits
// stable per-event IDs (S3 archive line position, Kafka offset), so a
// re-run of the same archive folds idempotently when a Deduper is wired.
func WithDedup(d state.Deduper) RunOption {
	return func(c *runConfig) {
		if d != nil {
			c.Dedup = d
		}
	}
}

// WithFailOnError configures the replay to surface the first dead-lettered
// record's error to the caller and abort. Default false: dead-lettered
// records are recorded via the metrics.Recorder and the replay continues.
//
// For shadow-table backfills with atomic swap, an aborted replay leaves
// the shadow incomplete and the swap blocked — usually the right
// behavior, but expensive when the failure is one bad row in a 30-day
// archive. Pick per workload.
func WithFailOnError(v bool) RunOption {
	return func(c *runConfig) {
		c.failOnError = v
	}
}

// WithBatchTick overrides the RecordBatch flush interval. The default 1 s
// fits a multi-day replay where dashboard granularity matters more than
// emission cost. Pass d <= 0 to disable periodic batch flushes (a single
// batch is still emitted at completion).
func WithBatchTick(d time.Duration) RunOption {
	return func(c *runConfig) {
		c.batchTick = d
	}
}

// Run drives the replay to completion. Returns nil when the driver exhausts its source
// and all records have been processed; non-nil on a fatal error (or any dead-lettered
// record when WithFailOnError(true)).
func Run[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	drv replay.Driver[T],
	opts ...RunOption,
) error {
	if err := p.Build(); err != nil {
		return fmt.Errorf("replay.Run: %w", err)
	}
	if drv == nil {
		return fmt.Errorf("replay.Run: driver is nil")
	}
	cfg := runConfig{Config: processor.Defaults(), batchTick: time.Second}
	for _, o := range opts {
		o(&cfg)
	}

	name := p.Name()
	keysFn := p.KeysFn()
	valueFn := p.ValueFn()
	store := p.Store()
	cache := p.CacheStore()
	window := p.Window()

	records := make(chan source.Record[T], 1024)
	driverErr := make(chan error, 1)
	go func() {
		driverErr <- drv.Replay(ctx, records)
		close(records)
	}()

	// Batch metrics: count records and wall time since the last RecordBatch
	// emit. emitBatch is called every batchTick and once at completion so
	// dashboards can plot throughput by mode in near-real-time during a
	// multi-day replay.
	var (
		batchCount int
		batchStart = time.Now()
	)
	emitBatch := func() {
		if batchCount == 0 {
			return
		}
		cfg.Recorder.RecordBatch(name, metrics.ModeReplay, batchCount, time.Since(batchStart))
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
			if cfg.failOnError {
				return err
			}
			// Continue: the dead-letter event was recorded by processor.MergeMany.
		}
		if rec.Ack != nil {
			if err := rec.Ack(); err != nil {
				cfg.Recorder.RecordError(name, fmt.Errorf("replay ack: %w", err))
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
				if err := <-driverErr; err != nil {
					cfg.Recorder.RecordError(name, fmt.Errorf("replay driver: %w", err))
					return fmt.Errorf("replay driver: %w", err)
				}
				return nil
			}
			if err := drain(rec); err != nil {
				emitBatch()
				return err
			}
		}
	}
}
