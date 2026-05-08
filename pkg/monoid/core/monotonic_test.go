package core

import (
	"math"
	"testing"
)

func TestMonotonic_TakesMax(t *testing.T) {
	m := Monotonic[int64](math.MinInt64)
	cases := []struct {
		a, b, want int64
	}{
		{10, 20, 20},
		{20, 10, 20},
		{42, 42, 42},
		{-5, -10, -5},
		{math.MinInt64, 100, 100},
		{100, math.MinInt64, 100},
	}
	for _, tc := range cases {
		if got := m.Combine(tc.a, tc.b); got != tc.want {
			t.Errorf("Combine(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestMonotonic_Associative(t *testing.T) {
	m := Monotonic[int64](math.MinInt64)
	a, b, c := int64(7), int64(13), int64(5)
	left := m.Combine(m.Combine(a, b), c)
	right := m.Combine(a, m.Combine(b, c))
	if left != right {
		t.Fatalf("not associative: (a∨b)∨c=%d, a∨(b∨c)=%d", left, right)
	}
}

func TestMonotonic_Commutative(t *testing.T) {
	m := Monotonic[int64](math.MinInt64)
	for _, pair := range [][2]int64{{1, 2}, {-100, 5}, {0, 0}, {math.MaxInt64, math.MinInt64}} {
		ab := m.Combine(pair[0], pair[1])
		ba := m.Combine(pair[1], pair[0])
		if ab != ba {
			t.Errorf("not commutative on (%d, %d): a∨b=%d, b∨a=%d", pair[0], pair[1], ab, ba)
		}
	}
}

func TestMonotonic_IdentityLaw(t *testing.T) {
	m := Monotonic[int64](math.MinInt64)
	// Combine(Identity, x) == Combine(x, Identity) == x for all int64 x.
	for _, x := range []int64{-1000, -1, 0, 1, math.MaxInt64} {
		if got := m.Combine(m.Identity(), x); got != x {
			t.Errorf("identity left: Combine(min, %d) = %d, want %d", x, got, x)
		}
		if got := m.Combine(x, m.Identity()); got != x {
			t.Errorf("identity right: Combine(%d, min) = %d, want %d", x, got, x)
		}
	}
}

func TestMonotonic_NonInt64Types(t *testing.T) {
	// String ordering: lexicographic max.
	ms := Monotonic[string]("")
	if got := ms.Combine("apple", "banana"); got != "banana" {
		t.Errorf("string Combine: got %q, want %q", got, "banana")
	}
	if got := ms.Combine(ms.Identity(), "x"); got != "x" {
		t.Errorf("string identity: got %q, want %q", got, "x")
	}

	// Float ordering.
	mf := Monotonic[float64](math.Inf(-1))
	if got := mf.Combine(1.5, 2.5); got != 2.5 {
		t.Errorf("float Combine: got %v, want 2.5", got)
	}
	if got := mf.Combine(mf.Identity(), 0.0); got != 0.0 {
		t.Errorf("float identity: got %v, want 0.0", got)
	}
}

func TestMonotonic_KindIsMax(t *testing.T) {
	m := Monotonic[int64](math.MinInt64)
	if got := string(m.Kind()); got != "max" {
		t.Errorf("Kind: got %q, want %q", got, "max")
	}
}
