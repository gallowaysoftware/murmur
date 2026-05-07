// Package query provides the read-side merge logic Murmur uses to assemble
// sliding-window query results from per-bucket state.
//
// In Phase 1 these are in-process helpers callers invoke directly. In Phase 2 the
// auto-generated gRPC service in pkg/query/codegen will dispatch to these same
// functions, so the merge semantics live in one place.
//
// Lambda-mode merge (batch_view ⊕ realtime_delta) is a Phase 2 addition — currently
// the helpers read directly from the primary state.Store, matching kappa-mode
// semantics.
package query

import (
	"context"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Get returns the all-time aggregation value for entity. Used for non-windowed
// pipelines (Bucket = 0).
func Get[V any](
	ctx context.Context,
	store state.Store[V],
	entity string,
) (V, bool, error) {
	return store.Get(ctx, state.Key{Entity: entity})
}

// GetWindow returns the aggregation merged over the bucket range covering
// [now-duration, now] under the given Windowed config.
//
// Implementation: compute the inclusive bucket-ID range, BatchGetItem all buckets in one
// or more requests, then fold via the monoid Combine in stable order. Missing buckets
// are treated as the monoid Identity.
//
// Cost note: for fine-grained windows the bucket count can be large (e.g., a "last 30
// days" query on hourly buckets reads 720 items). For sketches this means 720 sketch
// merges in process. If query latency becomes a bottleneck, the Phase 2 plan calls for
// pre-rolled rollup buckets or a Valkey-cached pre-merged window.
func GetWindow[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	w windowed.Config,
	entity string,
	duration time.Duration,
	now time.Time,
) (V, error) {
	lo, hi := w.LastN(now, duration)
	return getRangeBuckets(ctx, store, m, entity, lo, hi)
}

// GetRange returns the aggregation merged over the bucket range covering [start, end]
// under the given Windowed config.
func GetRange[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	w windowed.Config,
	entity string,
	start, end time.Time,
) (V, error) {
	lo, hi := w.BucketRange(start, end)
	return getRangeBuckets(ctx, store, m, entity, lo, hi)
}

// GetWindowMany returns the windowed merge for each of `entities` in a single
// fanned-out fetch. Equivalent to calling GetWindow per entity, but uses ONE
// underlying store.GetMany over (N entities × M buckets) keys instead of N
// separate GetMany calls of M keys each.
//
// For ML rerank with windowed counter features over N=200 candidates with
// M=7 daily buckets, this is 14 BatchGetItem calls in parallel (~20ms p99)
// instead of 200 sequential GetWindow calls (~4 seconds).
//
// Returns one value per entity in input order. Missing entities (no bucket
// data within the range) return the monoid Identity rather than an error —
// callers branch on Identity, not on a separate "present" flag, since
// "merged-empty" is a legitimate windowed result.
func GetWindowMany[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	w windowed.Config,
	entities []string,
	duration time.Duration,
	now time.Time,
) ([]V, error) {
	lo, hi := w.LastN(now, duration)
	return getRangeBucketsMany(ctx, store, m, entities, lo, hi)
}

// GetRangeMany is the absolute-range counterpart to GetWindowMany.
func GetRangeMany[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	w windowed.Config,
	entities []string,
	start, end time.Time,
) ([]V, error) {
	lo, hi := w.BucketRange(start, end)
	return getRangeBucketsMany(ctx, store, m, entities, lo, hi)
}

func getRangeBucketsMany[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	entities []string,
	lo, hi int64,
) ([]V, error) {
	out := make([]V, len(entities))
	if hi < lo || len(entities) == 0 {
		for i := range out {
			out[i] = m.Identity()
		}
		return out, nil
	}
	bucketCount := int(hi - lo + 1)
	totalKeys := bucketCount * len(entities)

	// Layout: keys[i*bucketCount + j] holds entity i at bucket lo+j.
	// This layout lets us slice per-entity for the monoid merge without
	// needing a separate index map.
	keys := make([]state.Key, 0, totalKeys)
	for _, e := range entities {
		for b := lo; b <= hi; b++ {
			keys = append(keys, state.Key{Entity: e, Bucket: b})
		}
	}

	vals, _, err := store.GetMany(ctx, keys)
	if err != nil {
		// Identity-fill on error so callers don't see a partial slice.
		for i := range out {
			out[i] = m.Identity()
		}
		return out, err
	}

	for i := range entities {
		start := i * bucketCount
		end := start + bucketCount
		out[i] = windowed.MergeBuckets(m, vals[start:end])
	}
	return out, nil
}

func getRangeBuckets[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	entity string,
	lo, hi int64,
) (V, error) {
	if hi < lo {
		return m.Identity(), nil
	}
	count := int(hi - lo + 1)
	keys := make([]state.Key, 0, count)
	for b := lo; b <= hi; b++ {
		keys = append(keys, state.Key{Entity: entity, Bucket: b})
	}
	vals, _, err := store.GetMany(ctx, keys)
	if err != nil {
		return m.Identity(), err
	}
	return windowed.MergeBuckets(m, vals), nil
}
