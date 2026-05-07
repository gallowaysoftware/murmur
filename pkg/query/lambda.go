package query

import (
	"context"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// LambdaQuery merges a precomputed batch view with a realtime delta at query time.
// This is Murmur's lambda-architecture mode: a periodic batch job writes the
// authoritative view; the streaming runtime writes only the delta accumulated since
// the last batch checkpoint; queries combine the two via the user's monoid.
//
// Two-store contract:
//
//   - view  : the precomputed result of the batch job over historical events.
//     Updated atomically by the batch worker (typically swap.SetActive after
//     writing into a fresh shadow table).
//   - delta : the streaming accumulator over events since the batch checkpoint.
//     Reset to identity when a new batch view is promoted.
//
// Merge semantics: at query time the result is `view.Get ⊕ delta.Get` for each key
// (or each bucket, in the windowed case). Both stores must use the same monoid; the
// merge is the monoid's Combine.
//
// At least-once dedup remains a per-event-ID concern at the streaming runtime — the
// lambda layer doesn't add duplicate-suppression semantics.
type LambdaQuery[V any] struct {
	View   state.Store[V]
	Delta  state.Store[V]
	Monoid monoid.Monoid[V]
}

// Get returns the lambda-merged value for entity (non-windowed pipelines).
func (q LambdaQuery[V]) Get(ctx context.Context, entity string) (V, bool, error) {
	k := state.Key{Entity: entity}
	vView, viewOK, err := q.View.Get(ctx, k)
	if err != nil {
		return q.Monoid.Identity(), false, err
	}
	vDelta, deltaOK, err := q.Delta.Get(ctx, k)
	if err != nil {
		return q.Monoid.Identity(), false, err
	}
	switch {
	case !viewOK && !deltaOK:
		return q.Monoid.Identity(), false, nil
	case !viewOK:
		return vDelta, true, nil
	case !deltaOK:
		return vView, true, nil
	default:
		return q.Monoid.Combine(vView, vDelta), true, nil
	}
}

// GetWindow returns the lambda-merged value for the rolling window of duration ending at now.
// Both stores are queried for the same bucket range; the per-bucket merge order is:
// view-bucket-N ⊕ delta-bucket-N for each N, then fold all merged buckets via Combine.
func (q LambdaQuery[V]) GetWindow(
	ctx context.Context,
	w windowed.Config,
	entity string,
	duration time.Duration,
	now time.Time,
) (V, error) {
	lo, hi := w.LastN(now, duration)
	return q.mergeBuckets(ctx, entity, lo, hi)
}

// GetRange returns the lambda-merged value over the absolute time range [start, end].
func (q LambdaQuery[V]) GetRange(
	ctx context.Context,
	w windowed.Config,
	entity string,
	start, end time.Time,
) (V, error) {
	lo, hi := w.BucketRange(start, end)
	return q.mergeBuckets(ctx, entity, lo, hi)
}

func (q LambdaQuery[V]) mergeBuckets(ctx context.Context, entity string, lo, hi int64) (V, error) {
	if hi < lo {
		return q.Monoid.Identity(), nil
	}
	count := int(hi - lo + 1)
	keys := make([]state.Key, 0, count)
	for b := lo; b <= hi; b++ {
		keys = append(keys, state.Key{Entity: entity, Bucket: b})
	}

	viewVals, viewOKs, err := q.View.GetMany(ctx, keys)
	if err != nil {
		return q.Monoid.Identity(), err
	}
	deltaVals, deltaOKs, err := q.Delta.GetMany(ctx, keys)
	if err != nil {
		return q.Monoid.Identity(), err
	}

	out := q.Monoid.Identity()
	for i := range keys {
		var bucket V
		switch {
		case !viewOKs[i] && !deltaOKs[i]:
			continue
		case !viewOKs[i]:
			bucket = deltaVals[i]
		case !deltaOKs[i]:
			bucket = viewVals[i]
		default:
			bucket = q.Monoid.Combine(viewVals[i], deltaVals[i])
		}
		out = q.Monoid.Combine(out, bucket)
	}
	return out, nil
}
