// Package compose provides higher-order monoids that combine simpler monoids:
//
//   - Map[K]V — merge two maps by Combining values per key
//   - Tuple{A,B}  — merge two paired values by Combining each component
//
// These let pipelines emit per-event multi-attribute aggregations without separate
// pipelines per attribute. Example: counting both views *and* unique users per page
// in one pipeline whose value type is Tuple[int64, []byte] (Sum on the left,
// HLL on the right).
package compose

import (
	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// MapMerge returns a monoid that merges map[K]V values by Combining matching keys via
// the inner monoid m, taking the union of the two key sets.
//
// Identity is a nil map; backends that materialize this on first write should treat
// nil and empty as equivalent. Combine is associative iff the inner monoid m is
// associative.
func MapMerge[K comparable, V any](m monoid.Monoid[V]) monoid.Monoid[map[K]V] {
	return mapMonoid[K, V]{inner: m}
}

type mapMonoid[K comparable, V any] struct{ inner monoid.Monoid[V] }

func (mapMonoid[K, V]) Identity() map[K]V { return nil }

func (m mapMonoid[K, V]) Combine(a, b map[K]V) map[K]V {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	out := make(map[K]V, len(a)+len(b))
	for k, va := range a {
		out[k] = va
	}
	for k, vb := range b {
		if va, ok := out[k]; ok {
			out[k] = m.inner.Combine(va, vb)
		} else {
			out[k] = vb
		}
	}
	return out
}

func (mapMonoid[K, V]) Kind() monoid.Kind { return monoid.KindMap }

// Tuple2 is a paired value with two components.
type Tuple2[A, B any] struct {
	A A
	B B
}

// TupleMonoid2 returns a monoid that merges Tuple2[A,B] componentwise via the inner
// monoids ma and mb. Useful for pipelines that aggregate multiple metrics in lockstep
// per key (e.g., view count + unique-visitor HLL).
func TupleMonoid2[A, B any](ma monoid.Monoid[A], mb monoid.Monoid[B]) monoid.Monoid[Tuple2[A, B]] {
	return tupleMonoid2[A, B]{a: ma, b: mb}
}

type tupleMonoid2[A, B any] struct {
	a monoid.Monoid[A]
	b monoid.Monoid[B]
}

func (m tupleMonoid2[A, B]) Identity() Tuple2[A, B] {
	return Tuple2[A, B]{A: m.a.Identity(), B: m.b.Identity()}
}

func (m tupleMonoid2[A, B]) Combine(x, y Tuple2[A, B]) Tuple2[A, B] {
	return Tuple2[A, B]{A: m.a.Combine(x.A, y.A), B: m.b.Combine(x.B, y.B)}
}

func (tupleMonoid2[A, B]) Kind() monoid.Kind { return monoid.KindTuple }
