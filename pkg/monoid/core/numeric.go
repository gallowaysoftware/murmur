// Package core provides the standard numeric and basic monoids: Sum, Count, Min, Max,
// First, Last, Set. These are structural — backend executors recognize their Kind and
// translate them to native operations.
package core

import (
	"cmp"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Numeric is the constraint for monoids over orderable numeric values.
type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Sum returns a monoid that adds values. Combine is +, Identity is the zero value.
//
// Note: this is a true monoid for V where the zero value is the additive identity
// — that holds for all `Numeric` types in Go.
func Sum[V Numeric]() monoid.Monoid[V] { return sum[V]{} }

type sum[V Numeric] struct{}

func (sum[V]) Identity() V       { var z V; return z }
func (sum[V]) Combine(a, b V) V  { return a + b }
func (sum[V]) Kind() monoid.Kind { return monoid.KindSum }

// Count returns a monoid that counts events. Each event contributes 1; values are summed.
// Use with Pipeline's Value(func(T) int64 { return 1 }) or have the source emit unit values.
func Count() monoid.Monoid[int64] { return count{} }

type count struct{}

func (count) Identity() int64          { return 0 }
func (count) Combine(a, b int64) int64 { return a + b }
func (count) Kind() monoid.Kind        { return monoid.KindCount }

// Bounded wraps a value V with a Set flag, used by Min/Max so that Identity can be
// represented as "no value yet" rather than the zero value of V (which would otherwise
// break the monoid identity law for any V where the zero is a legitimate input).
//
// Use NewBounded to lift a single observation; the Set flag is then true. The zero
// Bounded[V] is Identity for both Min and Max.
type Bounded[V any] struct {
	Value V
	Set   bool
}

// NewBounded lifts a single observation into a Bounded wrapper.
func NewBounded[V any](v V) Bounded[V] { return Bounded[V]{Value: v, Set: true} }

// Min returns a monoid that tracks the running minimum of V. Identity is the
// unset Bounded[V]; this satisfies the monoid identity law for all V.
//
// Lift per-event observations via NewBounded(v) (or core.Bounded[V]{Value: v, Set: true}).
func Min[V cmp.Ordered]() monoid.Monoid[Bounded[V]] { return min_[V]{} }

type min_[V cmp.Ordered] struct{}

func (min_[V]) Identity() Bounded[V] { return Bounded[V]{} }

func (min_[V]) Combine(a, b Bounded[V]) Bounded[V] {
	switch {
	case !a.Set:
		return b
	case !b.Set:
		return a
	case a.Value < b.Value:
		return a
	default:
		return b
	}
}

func (min_[V]) Kind() monoid.Kind { return monoid.KindMin }

// Max returns a monoid that tracks the running maximum of V. Identity is the
// unset Bounded[V]; this satisfies the monoid identity law for all V.
func Max[V cmp.Ordered]() monoid.Monoid[Bounded[V]] { return max_[V]{} }

type max_[V cmp.Ordered] struct{}

func (max_[V]) Identity() Bounded[V] { return Bounded[V]{} }

func (max_[V]) Combine(a, b Bounded[V]) Bounded[V] {
	switch {
	case !a.Set:
		return b
	case !b.Set:
		return a
	case a.Value > b.Value:
		return a
	default:
		return b
	}
}

func (max_[V]) Kind() monoid.Kind { return monoid.KindMax }
