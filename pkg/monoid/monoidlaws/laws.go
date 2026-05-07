// Package monoidlaws provides reusable test helpers that verify a monoid.Monoid[V]
// satisfies the algebraic laws (associativity and identity) required for safe use in
// Murmur pipelines.
//
// Implementing a custom monoid? Wire up a sample generator and call:
//
//	monoidlaws.TestMonoid(t, m, gen, monoidlaws.WithSamples(64))
//
// where `gen` is a func(int) V that returns the i-th sample. Optionally pass
// WithEqual to override the default equality check (useful for monoids whose
// internal representation has don't-care bits — encoded sketches, etc).
package monoidlaws

import (
	"reflect"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
)

// Option configures TestMonoid.
type Option[V any] func(*config[V])

type config[V any] struct {
	samples int
	equal   func(a, b V) bool
}

// WithSamples sets how many sample values are drawn from the generator. Default 32.
// Bigger values explore more triples for the associativity check; cubic in samples.
func WithSamples[V any](n int) Option[V] {
	return func(c *config[V]) {
		if n > 0 {
			c.samples = n
		}
	}
}

// WithEqual overrides the equality check. Default is reflect.DeepEqual. Use this
// for monoids whose Combine result is not bitwise-stable (e.g. floating point) —
// supply a tolerance-based comparator.
func WithEqual[V any](eq func(a, b V) bool) Option[V] {
	return func(c *config[V]) {
		c.equal = eq
	}
}

// TestMonoid asserts the three monoid laws hold for m given a sample generator:
//
//  1. Left identity:  Combine(Identity(), x) == x
//  2. Right identity: Combine(x, Identity()) == x
//  3. Associativity:  Combine(Combine(a, b), c) == Combine(a, Combine(b, c))
//
// gen(i) returns the i-th sample; the helper draws cfg.samples samples, then
// exercises identity at each one and associativity over a representative subset
// of triples (cubic blow-up is the reason samples defaults to 32).
//
// Failures are reported via t.Errorf with the offending input, so a single broken
// monoid produces a focused diagnostic rather than a wall of "FAIL".
func TestMonoid[V any](t *testing.T, m monoid.Monoid[V], gen func(i int) V, opts ...Option[V]) {
	t.Helper()
	cfg := config[V]{
		samples: 32,
		equal:   func(a, b V) bool { return reflect.DeepEqual(a, b) },
	}
	for _, o := range opts {
		o(&cfg)
	}

	xs := make([]V, cfg.samples)
	for i := range xs {
		xs[i] = gen(i)
	}

	id := m.Identity()

	t.Run("LeftIdentity", func(t *testing.T) {
		for i, x := range xs {
			got := m.Combine(id, x)
			if !cfg.equal(got, x) {
				t.Errorf("sample %d: Combine(Identity, x) != x; got %#v, want %#v", i, got, x)
			}
		}
	})

	t.Run("RightIdentity", func(t *testing.T) {
		for i, x := range xs {
			got := m.Combine(x, id)
			if !cfg.equal(got, x) {
				t.Errorf("sample %d: Combine(x, Identity) != x; got %#v, want %#v", i, got, x)
			}
		}
	})

	t.Run("Associativity", func(t *testing.T) {
		// Bound the triple count so cubic samples don't explode.
		// For samples <= 16: every triple. For larger: stride sampling.
		stride := 1
		if cfg.samples > 16 {
			stride = cfg.samples / 16
		}
		for i := 0; i < cfg.samples; i += stride {
			for j := 0; j < cfg.samples; j += stride {
				for k := 0; k < cfg.samples; k += stride {
					a, b, c := xs[i], xs[j], xs[k]
					left := m.Combine(m.Combine(a, b), c)
					right := m.Combine(a, m.Combine(b, c))
					if !cfg.equal(left, right) {
						t.Errorf(
							"associativity fails at (%d,%d,%d):\n  (a*b)*c = %#v\n  a*(b*c) = %#v",
							i, j, k, left, right,
						)
						return
					}
				}
			}
		}
	})
}
