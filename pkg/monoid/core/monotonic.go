package core

import (
	"cmp"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Monotonic returns a monoid where Combine takes the larger of its inputs by
// V's natural ordering. The caller supplies Identity explicitly because
// `cmp.Ordered` has no portable minimum.
//
// # When to use this vs. Max
//
// `core.Max[V]()` is the right monoid for "the maximum observed value over
// the events seen so far" when each event contributes a single observation.
// Its value type is `Bounded[V]` (so Identity can mean "no observation
// yet"), which means callers lift each event via `NewBounded(v)`.
//
// `Monotonic[V]` is the right monoid for the SetCountIfGreater pattern:
// upstream services emit ABSOLUTE values (e.g. "user X has 47 likes") and
// the pipeline must accept-only-if-greater so that an out-of-order earlier
// emission ("user X has 32 likes") doesn't roll the value backwards. Pair
// it with `pkg/state/dynamodb.Int64MaxStore` (DDB conditional UpdateItem
// `attribute_not_exists OR #v < :v`) for the canonical end-to-end shape:
// in-process Combine collapses a batch via `max`, the store performs the
// final conditional write.
//
//	pipe := pipeline.NewPipeline[Event, int64]("likes_total").
//	    From(src).
//	    Key(func(e Event) string { return "user:" + e.UserID }).
//	    Value(func(e Event) int64 { return e.AbsoluteCount }).
//	    Aggregate(core.Monotonic[int64](math.MinInt64)).
//	    StoreIn(maxStore)
//
// # Why caller-supplied Identity
//
// For `cmp.Ordered`, there's no portable "minimum" value. For signed
// integers it's `math.MinInt*`; for unsigned, `0`; for `string`, `""`;
// for `time.Time`, the zero `Time{}`. Rather than hard-code one or
// require a new constraint, callers pass the appropriate min explicitly.
// In Int64MaxStore-backed pipelines Identity is largely vestigial — the
// DDB conditional handles the "no value yet" case via
// `attribute_not_exists` — so even an obviously-wrong Identity value
// would still produce correct stored state, but the in-process
// `Combine(Identity, x) = x` law fails if you pick wrong.
func Monotonic[V cmp.Ordered](identity V) monoid.Monoid[V] {
	return monotonic[V]{id: identity}
}

type monotonic[V cmp.Ordered] struct {
	id V
}

func (m monotonic[V]) Identity() V { return m.id }

func (m monotonic[V]) Combine(a, b V) V {
	if a < b {
		return b
	}
	return a
}

func (m monotonic[V]) Kind() monoid.Kind { return monoid.KindMax }
