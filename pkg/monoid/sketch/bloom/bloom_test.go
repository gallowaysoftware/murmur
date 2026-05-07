package bloom_test

import (
	"fmt"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
)

func TestBloom_AddAndContains(t *testing.T) {
	m := bloom.Bloom()
	state := m.Identity()
	for i := 0; i < 1000; i++ {
		state = m.Combine(state, bloom.Single([]byte(fmt.Sprintf("user-%d", i))))
	}
	// All inserted items must be present (no false negatives).
	for i := 0; i < 1000; i++ {
		if !bloom.Contains(state, []byte(fmt.Sprintf("user-%d", i))) {
			t.Fatalf("user-%d missing from filter (false negative)", i)
		}
	}
	// Non-inserted items should mostly miss; ~1% FPR allowed.
	const probe = 10_000
	var hits int
	for i := 0; i < probe; i++ {
		if bloom.Contains(state, []byte(fmt.Sprintf("never-inserted-%d", i))) {
			hits++
		}
	}
	rate := float64(hits) / float64(probe)
	if rate > 0.05 {
		t.Errorf("FPR %.2f too high; expected ~1%%, ceiling 5%%", rate*100)
	}
	t.Logf("Bloom FPR over %d non-inserted probes: %.2f%%", probe, rate*100)
}

func TestBloom_Associativity(t *testing.T) {
	m := bloom.Bloom()
	a := bloom.Single([]byte("alpha"))
	b := bloom.Single([]byte("beta"))
	c := bloom.Single([]byte("gamma"))

	left := m.Combine(m.Combine(a, b), c)
	right := m.Combine(a, m.Combine(b, c))
	// Bitwise-OR is commutative+associative, so the merged bytes must match exactly.
	if !bloom.Equal(left, right) {
		t.Fatalf("Bloom Combine not associative")
	}
	// Containment for all three must hold either way.
	for _, elem := range []string{"alpha", "beta", "gamma"} {
		if !bloom.Contains(left, []byte(elem)) {
			t.Fatalf("missing %q after associative merge", elem)
		}
	}
}

func TestBloom_Identity(t *testing.T) {
	m := bloom.Bloom()
	a := bloom.Single([]byte("only"))

	if got := m.Combine(m.Identity(), a); !bloom.Contains(got, []byte("only")) {
		t.Fatalf("Combine(Identity,a) lost element")
	}
	if got := m.Combine(a, m.Identity()); !bloom.Contains(got, []byte("only")) {
		t.Fatalf("Combine(a,Identity) lost element")
	}
}
