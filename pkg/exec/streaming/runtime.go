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

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	recorder metrics.Recorder
}

// WithMetrics installs a metrics.Recorder on the runtime. Defaults to metrics.Noop{}.
func WithMetrics(r metrics.Recorder) RunOption {
	return func(c *runConfig) {
		if r != nil {
			c.recorder = r
		}
	}
}

// Run executes the pipeline in Live mode until ctx is canceled or the source returns.
// Returns nil on graceful shutdown.
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

	cfg := runConfig{recorder: metrics.Noop{}}
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
			drainAndProcess(ctx, name, records, keyFn, valueFn, store, cache, window, cfg.recorder)
			return err
		case rec, ok := <-records:
			if !ok {
				return <-srcDone
			}
			if err := processOne(ctx, name, rec, keyFn, valueFn, store, cache, window, cfg.recorder); err != nil {
				cfg.recorder.RecordError(name, err)
				return err
			}
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
	rec metrics.Recorder,
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
			_ = processOne(ctx, name, r, keyFn, valueFn, store, cache, window, rec)
		}
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
