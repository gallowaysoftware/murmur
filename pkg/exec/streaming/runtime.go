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

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Run executes the pipeline in Live mode until ctx is canceled or the source returns.
// Returns nil on graceful shutdown.
//
// The pipeline must have been configured with From / Key / Value / Aggregate / StoreIn
// before Run is called; Build is invoked internally to validate.
func Run[T any, V any](ctx context.Context, p *pipeline.Pipeline[T, V]) error {
	if err := p.Build(); err != nil {
		return fmt.Errorf("streaming.Run: %w", err)
	}

	src := p.Source()
	keyFn := p.KeyFn()
	valueFn := p.ValueFn()
	mon := p.Monoid()
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
			drainAndProcess(ctx, records, keyFn, valueFn, mon, store, cache, window)
			return err
		case rec, ok := <-records:
			if !ok {
				return <-srcDone
			}
			if err := processOne(ctx, rec, keyFn, valueFn, mon, store, cache, window); err != nil {
				return err
			}
		}
	}
}

func drainAndProcess[T any, V any](
	ctx context.Context,
	records <-chan source.Record[T],
	keyFn func(T) string,
	valueFn func(T) V,
	mon monoid.Monoid[V],
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) {
	// Best-effort: process whatever's already in the buffer after the source has stopped,
	// up to a short deadline. Anything not Ack'd will be redelivered on next start
	// (at-least-once semantics).
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			return
		case rec, ok := <-records:
			if !ok {
				return
			}
			_ = processOne(ctx, rec, keyFn, valueFn, mon, store, cache, window)
		}
	}
}

func processOne[T any, V any](
	ctx context.Context,
	rec source.Record[T],
	keyFn func(T) string,
	valueFn func(T) V,
	mon monoid.Monoid[V],
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
) error {
	_ = mon // monoid identity/combine is consumed by the Store implementation per-Kind;
	// the runtime hands V deltas to the Store and lets it apply Combine natively.

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

	if err := store.MergeUpdate(ctx, sk, delta, ttl); err != nil {
		return fmt.Errorf("store MergeUpdate: %w", err)
	}

	// Cache write-through is best-effort. Cache is never source of truth; if it errors
	// we log (TODO: real logger) and continue. The pipeline's correctness invariant rests
	// entirely on the primary store having succeeded above.
	if cache != nil {
		_ = cache.MergeUpdate(ctx, sk, delta, ttl)
	}

	if rec.Ack != nil {
		if err := rec.Ack(); err != nil {
			return fmt.Errorf("source Ack: %w", err)
		}
	}
	return nil
}
