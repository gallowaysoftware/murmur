package core

import (
	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Stamped wraps a value V with an event time. First and Last operate on Stamped[V].
type Stamped[V any] struct {
	Time  int64 // Unix nanoseconds
	Value V
	// Set indicates whether this Stamped carries a real value. The zero Stamped is the
	// identity for First/Last and must be distinguishable from a real event at t=0.
	Set bool
}

// First returns a monoid that keeps the earliest-timestamped value seen.
// On ties, the first-arrived value wins (left side of Combine).
func First[V any]() monoid.Monoid[Stamped[V]] { return first[V]{} }

type first[V any] struct{}

func (first[V]) Identity() Stamped[V] { return Stamped[V]{} }
func (first[V]) Combine(a, b Stamped[V]) Stamped[V] {
	switch {
	case !a.Set:
		return b
	case !b.Set:
		return a
	case a.Time <= b.Time:
		return a
	default:
		return b
	}
}
func (first[V]) Kind() monoid.Kind { return monoid.KindFirst }

// Last returns a monoid that keeps the latest-timestamped value seen.
// On ties, the most-recently-arrived value wins (right side of Combine).
func Last[V any]() monoid.Monoid[Stamped[V]] { return last[V]{} }

type last[V any] struct{}

func (last[V]) Identity() Stamped[V] { return Stamped[V]{} }
func (last[V]) Combine(a, b Stamped[V]) Stamped[V] {
	switch {
	case !a.Set:
		return b
	case !b.Set:
		return a
	case a.Time > b.Time:
		return a
	default:
		return b
	}
}
func (last[V]) Kind() monoid.Kind { return monoid.KindLast }
