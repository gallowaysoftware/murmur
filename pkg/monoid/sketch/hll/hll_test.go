package hll_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
)

func TestHLL_Associativity(t *testing.T) {
	m := hll.HLL()
	a := hll.Single([]byte("alpha"))
	b := hll.Single([]byte("beta"))
	c := hll.Single([]byte("gamma"))

	left, _ := hll.Estimate(m.Combine(m.Combine(a, b), c))
	right, _ := hll.Estimate(m.Combine(a, m.Combine(b, c)))
	if left != right {
		t.Fatalf("HLL Combine not associative: (a*b)*c=%d, a*(b*c)=%d", left, right)
	}
}

func TestHLL_Identity(t *testing.T) {
	m := hll.HLL()
	a := hll.Single([]byte("only"))

	got, _ := hll.Estimate(m.Combine(m.Identity(), a))
	if got != 1 {
		t.Fatalf("Combine(Identity, a) estimate: got %d, want 1", got)
	}
	got, _ = hll.Estimate(m.Combine(a, m.Identity()))
	if got != 1 {
		t.Fatalf("Combine(a, Identity) estimate: got %d, want 1", got)
	}
}

func TestHLL_AccuracyAtScale(t *testing.T) {
	const N = 10000
	m := hll.HLL()
	state := m.Identity()

	for i := 0; i < N; i++ {
		state = m.Combine(state, hll.Single([]byte(fmt.Sprintf("user-%d", i))))
	}
	got, err := hll.Estimate(state)
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	// HLL++ at default precision 14 has ~1.6% standard error; allow 5% tolerance.
	tol := 0.05
	if math.Abs(float64(got)-N)/N > tol {
		t.Fatalf("estimate %d off by more than %g of N=%d", got, tol, N)
	}
	t.Logf("HLL estimated %d unique elements out of %d (error %.2f%%)", got, N, math.Abs(float64(got)-N)/N*100)
}

func TestHLL_Idempotency(t *testing.T) {
	// Inserting the same element multiple times shouldn't change the cardinality estimate
	// once it's stably "seen."
	m := hll.HLL()
	state := m.Identity()
	for i := 0; i < 100; i++ {
		state = m.Combine(state, hll.Single([]byte("repeat")))
	}
	got, _ := hll.Estimate(state)
	if got != 1 {
		t.Fatalf("repeated single element: got estimate %d, want 1", got)
	}
}
