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

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/replay"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Run drives the replay to completion. Returns nil when the driver exhausts its source
// and all records have been processed; non-nil on a fatal error.
func Run[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	drv replay.Driver[T],
) error {
	if err := p.Build(); err != nil {
		return fmt.Errorf("replay.Run: %w", err)
	}
	if drv == nil {
		return fmt.Errorf("replay.Run: driver is nil")
	}

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
		if err := processOne(ctx, rec, keyFn, valueFn, store, cache, window); err != nil {
			return err
		}
	}
	if err := <-driverErr; err != nil {
		return fmt.Errorf("replay driver: %w", err)
	}
	return nil
}

func processOne[T any, V any](
	ctx context.Context,
	rec source.Record[T],
	keyFn func(T) string,
	valueFn func(T) V,
	store state.Store[V],
	cache state.Cache[V],
	window *windowed.Config,
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

	if err := store.MergeUpdate(ctx, sk, delta, ttl); err != nil {
		return fmt.Errorf("store MergeUpdate: %w", err)
	}
	if cache != nil {
		_ = cache.MergeUpdate(ctx, sk, delta, ttl)
	}
	if rec.Ack != nil {
		_ = rec.Ack()
	}
	return nil
}
