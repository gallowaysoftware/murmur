// Trailing-window query helpers. Trailing windows are the canonical
// "last N days" / "last N hours" shape that the vast majority of
// counter consumers want: trailing-7d unique-viewers, trailing-30d
// likes, trailing-24h impressions. The helpers here are thin wrappers
// over GetWindow / GetWindowMany that document the intent at the
// callsite and pair with the matching builder sugar in pkg/murmur
// (`(*CounterBuilder).Trailing(durations...)`).
//
// Semantically, GetTrailing(d) ≡ GetWindow(d) — both ask for the
// merge of the most-recent buckets covering the last d. The split
// is purely about naming: callers reading "trailing 7d" at the
// callsite don't have to mentally translate "window 7d" into the
// same concept, and the trailing-builder records the requested
// durations on the pipeline so retention can be sized
// automatically.

package query

import (
	"context"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// GetTrailing returns the aggregation merged across the trailing window
// covering [now-duration, now] under the given Windowed config. A thin
// wrapper over GetWindow that documents the "trailing window" intent.
//
// Use this for "last 7d unique viewers", "trailing 30d likes",
// "last 24h impressions" — the dominant shape for counter queries.
func GetTrailing[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	w windowed.Config,
	entity string,
	duration time.Duration,
	now time.Time,
) (V, error) {
	return GetWindow(ctx, store, m, w, entity, duration, now)
}

// GetTrailingMany batches GetTrailing across many entities in a single
// fanned-out fetch. Same single-store-call contract as GetWindowMany.
func GetTrailingMany[V any](
	ctx context.Context,
	store state.Store[V],
	m monoid.Monoid[V],
	w windowed.Config,
	entities []string,
	duration time.Duration,
	now time.Time,
) ([]V, error) {
	return GetWindowMany(ctx, store, m, w, entities, duration, now)
}
