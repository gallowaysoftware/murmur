package compose_test

import (
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
)

func TestDecayedSum_OneHalfLifeHalves(t *testing.T) {
	hl := 1 * time.Hour
	m := compose.DecayedSum(hl)

	t0 := time.Now()
	a := compose.DecayedAt(100, t0)
	b := compose.DecayedAt(0, t0.Add(hl)) // observe nothing at t0 + 1h

	got := m.Combine(a, b)
	// Decayed value of 100 after one half-life should be ~50.
	if math.Abs(got.Value-50.0) > 0.01 {
		t.Fatalf("after 1 half-life: got %.4f, want ~50.0", got.Value)
	}
}

func TestDecayedSum_RecentDominates(t *testing.T) {
	m := compose.DecayedSum(1 * time.Minute)
	t0 := time.Now()
	old := compose.DecayedAt(1000, t0.Add(-10*time.Minute))
	recent := compose.DecayedAt(1, t0)

	got := m.Combine(old, recent)
	// Old contribution decayed by 2^-10 ≈ 0.000977 → ~0.98 + 1 = ~1.98
	if got.Value > 5 || got.Value < 1 {
		t.Errorf("recent should dominate: got %.4f", got.Value)
	}
}

func TestDecayedSum_AssociativityApprox(t *testing.T) {
	m := compose.DecayedSum(30 * time.Minute)
	t0 := time.Now()
	a := compose.DecayedAt(5, t0)
	b := compose.DecayedAt(7, t0.Add(15*time.Minute))
	c := compose.DecayedAt(3, t0.Add(40*time.Minute))

	left := m.Combine(m.Combine(a, b), c)
	right := m.Combine(a, m.Combine(b, c))

	// Floating-point associativity isn't exact, but should be close.
	if math.Abs(left.Value-right.Value) > 1e-9 {
		t.Errorf("(a*b)*c = %.6f, a*(b*c) = %.6f", left.Value, right.Value)
	}
}

func TestDecayedSum_Identity(t *testing.T) {
	m := compose.DecayedSum(time.Hour)
	a := compose.DecayedAt(42, time.Now())
	if got := m.Combine(m.Identity(), a); got.Value != 42 {
		t.Fatalf("Combine(Identity,a): got %v, want 42", got.Value)
	}
	if got := m.Combine(a, m.Identity()); got.Value != 42 {
		t.Fatalf("Combine(a,Identity): got %v, want 42", got.Value)
	}
}

func TestEvaluateAt(t *testing.T) {
	t0 := time.Now()
	d := compose.DecayedAt(100, t0)
	// One half-life later, value should be ~50.
	got := compose.EvaluateAt(d, time.Hour, t0.Add(time.Hour))
	if math.Abs(got-50) > 0.01 {
		t.Fatalf("EvaluateAt 1 half-life: got %.4f, want ~50", got)
	}
}
