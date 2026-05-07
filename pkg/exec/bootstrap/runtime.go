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

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Run drives the bootstrap. Returns the captured HandoffToken on success; the deployment
// system persists it and hands it to the live runtime on transition.
func Run[T any, V any](
	ctx context.Context,
	p *pipeline.Pipeline[T, V],
	src snapshot.SnapshotSource[T],
) (snapshot.HandoffToken, error) {
	if err := p.Build(); err != nil {
		return nil, fmt.Errorf("bootstrap.Run: %w", err)
	}
	if src == nil {
		return nil, fmt.Errorf("bootstrap.Run: snapshot source is nil")
	}

	token, err := src.CaptureHandoff(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture handoff: %w", err)
	}

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
		if err := processOne(ctx, rec, keyFn, valueFn, store, cache, window); err != nil {
			return nil, err
		}
	}
	if err := <-scanErr; err != nil {
		return nil, fmt.Errorf("snapshot scan: %w", err)
	}

	return token, nil
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
