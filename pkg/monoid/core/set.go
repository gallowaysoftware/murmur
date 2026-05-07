package core

import (
	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Set returns a monoid that unions sets of comparable elements. The set representation is
// a Go map[V]struct{}. For high-cardinality unique-element tracking prefer the HLL sketch
// monoid, which trades exactness for bounded memory.
func Set[V comparable]() monoid.Monoid[map[V]struct{}] { return setMonoid[V]{} }

type setMonoid[V comparable] struct{}

func (setMonoid[V]) Identity() map[V]struct{} { return nil }

func (setMonoid[V]) Combine(a, b map[V]struct{}) map[V]struct{} {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make(map[V]struct{}, len(a)+len(b))
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

func (setMonoid[V]) Kind() monoid.Kind { return monoid.KindSet }
