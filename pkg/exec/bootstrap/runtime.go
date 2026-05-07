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

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
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

// Run drives the bootstrap. Returns the captured HandoffToken on success; the deployment
// system persists it and hands it to the live runtime on transition.
func Run[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	src snapshot.SnapshotSource[T],
	opts ...RunOption,
) (snapshot.HandoffToken, error) {
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("bootstrap.Run: %w", err)
	}
	if src == nil {
		return nil, fmt.Errorf("bootstrap.Run: snapshot source is nil")
	}
	cfg := runConfig{recorder: metrics.Noop{}}
	for _, o := range opts {
		o(&cfg)
	}

	token, err := src.CaptureHandoff(ctx)
	if err != nil {
		cfg.recorder.RecordError(p.Name(), fmt.Errorf("capture handoff: %w", err))
		return nil, fmt.Errorf("capture handoff: %w", err)
	}

	name := p.Name()
	keyFn := p.KeyFn()
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

	for rec := range records {
		if err := processOne(ctx, name, rec, keyFn, valueFn, store, cache, window, cfg.recorder); err != nil {
			cfg.recorder.RecordError(name, err)
			return nil, err
		}
	}
	if err := <-scanErr; err != nil {
		cfg.recorder.RecordError(name, fmt.Errorf("snapshot scan: %w", err))
		return nil, fmt.Errorf("snapshot scan: %w", err)
	}

	return token, nil
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
			rec_.RecordError(name, fmt.Errorf("snapshot ack: %w", err))
		}
	}
	rec_.RecordEvent(name)
	return nil
}
