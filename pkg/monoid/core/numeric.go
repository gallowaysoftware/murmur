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
func Sum[V Numeric]() monoid.Monoid[V] { return sum[V]{} }

type sum[V Numeric] struct{}

func (sum[V]) Identity() V       { var z V; return z }
func (sum[V]) Combine(a, b V) V  { return a + b }
func (sum[V]) Kind() monoid.Kind { return monoid.KindSum }

// Count returns a monoid that counts events. Each event contributes 1; values are summed.
// Use with Pipeline's Value(func(T) int64 { return 1 }) or have the source emit unit values.
func Count() monoid.Monoid[int64] { return count{} }

type count struct{}

func (count) Identity() int64       { return 0 }
func (count) Combine(a, b int64) int64 { return a + b }
func (count) Kind() monoid.Kind     { return monoid.KindCount }

// Min returns a monoid for tracking the running minimum.
// Identity is the zero value of V — meaning Min works correctly only when 0 is not a
// valid input or when callers are aware of the convention. For unbounded-Min use
// MinWithSentinel and supply an explicit "no value yet" sentinel.
func Min[V cmp.Ordered]() monoid.Monoid[V] { return min_[V]{} }

type min_[V cmp.Ordered] struct{}

func (min_[V]) Identity() V { var z V; return z }
func (min_[V]) Combine(a, b V) V {
	if a < b {
		return a
	}
	return b
}
func (min_[V]) Kind() monoid.Kind { return monoid.KindMin }

// Max returns a monoid for tracking the running maximum. Same identity caveat as Min.
func Max[V cmp.Ordered]() monoid.Monoid[V] { return max_[V]{} }

type max_[V cmp.Ordered] struct{}

func (max_[V]) Identity() V { var z V; return z }
func (max_[V]) Combine(a, b V) V {
	if a > b {
		return a
	}
	return b
}
func (max_[V]) Kind() monoid.Kind { return monoid.KindMax }
