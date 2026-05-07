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
	"context"
	"fmt"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/replay"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	recorder metrics.Recorder
}

// WithMetrics installs a metrics.Recorder. Defaults to metrics.Noop{}.
func WithMetrics(r metrics.Recorder) RunOption {
	return func(c *runConfig) {
		if r != nil {
			c.recorder = r
		}
	}
}

// Run drives the replay to completion. Returns nil when the driver exhausts its source
// and all records have been processed; non-nil on a fatal error.
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
	cfg := runConfig{recorder: metrics.Noop{}}
	for _, o := range opts {
		o(&cfg)
	}

	name := p.Name()
	keyFn := p.KeyFn()
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

	for rec := range records {
		if err := processOne(ctx, name, rec, keyFn, valueFn, store, cache, window, cfg.recorder); err != nil {
			cfg.recorder.RecordError(name, err)
			return err
		}
	}
	if err := <-driverErr; err != nil {
		cfg.recorder.RecordError(name, fmt.Errorf("replay driver: %w", err))
		return fmt.Errorf("replay driver: %w", err)
	}
	return nil
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
		return fmt.Errorf("pipeline %q entity %q bucket %d: store MergeUpdate: %w", name, sk.Entity, sk.Bucket, err)
	}
	rec_.RecordLatency(name, "store_merge", time.Since(storeStart))

	if cache != nil {
		cacheStart := time.Now()
		if err := cache.MergeUpdate(ctx, sk, delta, ttl); err != nil {
			rec_.RecordError(name, fmt.Errorf("cache MergeUpdate %q/%d: %w", sk.Entity, sk.Bucket, err))
		}
		rec_.RecordLatency(name, "cache_merge", time.Since(cacheStart))
	}
	if rec.Ack != nil {
		if err := rec.Ack(); err != nil {
			rec_.RecordError(name, fmt.Errorf("replay ack: %w", err))
		}
	}
	rec_.RecordEvent(name)
	return nil
}
