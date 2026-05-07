package query

import (
	"context"
	"fmt"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// WarmupWindowed prefetches the (entity × bucket) keys covering the
// most-recent `duration` window for each entity into the cache, sourcing
// fresh values from the authoritative store via cache.Repopulate.
//
// Use case: cold-cache p99 inflation. After a Valkey restart or a fresh
// query-service deployment, the first batch of GetWindow / GetWindowMany
// queries falls through to DDB and pays full latency. A pre-flight
// WarmupWindowed at process start (or on a known traffic pattern) loads
// the hot keys before user requests arrive.
//
// Cost is `len(entities) × bucketCount` cache writes plus one batched
// store.GetMany over the same key set. For ML-rerank-style workloads
// where the candidate set is bounded (typically N=200 popular content
// items × M=7 daily buckets = 1400 keys), the warmup completes in a
// few hundred milliseconds against typical DDB capacity and is a
// straightforward step in the service-start path.
//
// Returns the count of successfully warmed entries (entities × buckets
// that the store had data for) so callers can log meaningful progress.
func WarmupWindowed[V any](
	ctx context.Context,
	cache state.Cache[V],
	store state.Store[V],
	w windowed.Config,
	entities []string,
	duration time.Duration,
	now time.Time,
) (int, error) {
	if cache == nil {
		return 0, fmt.Errorf("query.WarmupWindowed: cache is nil")
	}
	if store == nil {
		return 0, fmt.Errorf("query.WarmupWindowed: store is nil")
	}
	if len(entities) == 0 {
		return 0, nil
	}
	lo, hi := w.LastN(now, duration)
	if hi < lo {
		return 0, nil
	}
	bucketCount := int(hi - lo + 1)
	keys := make([]state.Key, 0, bucketCount*len(entities))
	for _, e := range entities {
		for b := lo; b <= hi; b++ {
			keys = append(keys, state.Key{Entity: e, Bucket: b})
		}
	}

	// Count present-in-store first so we can return a meaningful "warmed"
	// number — Repopulate doesn't currently report it. This costs one
	// extra store.GetMany; for the cold-start-warmup use case, that's
	// the same data Repopulate will fetch anyway, so the cost is amortized
	// across both calls.
	_, oks, err := store.GetMany(ctx, keys)
	if err != nil {
		return 0, fmt.Errorf("query.WarmupWindowed: probe store: %w", err)
	}
	warmed := 0
	for _, ok := range oks {
		if ok {
			warmed++
		}
	}

	if err := cache.Repopulate(ctx, store, keys); err != nil {
		return warmed, fmt.Errorf("query.WarmupWindowed: repopulate: %w", err)
	}
	return warmed, nil
}

// WarmupNonWindowed prefetches all-time entries for the given entities
// into the cache. Same shape as WarmupWindowed but for pipelines without
// a time-bucket dimension.
func WarmupNonWindowed[V any](
	ctx context.Context,
	cache state.Cache[V],
	store state.Store[V],
	entities []string,
) (int, error) {
	if cache == nil {
		return 0, fmt.Errorf("query.WarmupNonWindowed: cache is nil")
	}
	if store == nil {
		return 0, fmt.Errorf("query.WarmupNonWindowed: store is nil")
	}
	if len(entities) == 0 {
		return 0, nil
	}
	keys := make([]state.Key, len(entities))
	for i, e := range entities {
		keys[i] = state.Key{Entity: e}
	}
	_, oks, err := store.GetMany(ctx, keys)
	if err != nil {
		return 0, fmt.Errorf("query.WarmupNonWindowed: probe store: %w", err)
	}
	warmed := 0
	for _, ok := range oks {
		if ok {
			warmed++
		}
	}
	if err := cache.Repopulate(ctx, store, keys); err != nil {
		return warmed, fmt.Errorf("query.WarmupNonWindowed: repopulate: %w", err)
	}
	return warmed, nil
}
