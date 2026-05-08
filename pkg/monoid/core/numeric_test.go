package core

import (
	"testing"
)

func TestSumAssociativity(t *testing.T) {
	m := Sum[int64]()
	a, b, c := int64(3), int64(7), int64(11)
	left := m.Combine(m.Combine(a, b), c)
	right := m.Combine(a, m.Combine(b, c))
	if left != right {
		t.Fatalf("sum is not associative: (a+b)+c=%d, a+(b+c)=%d", left, right)
	}
	if got := m.Combine(m.Identity(), a); got != a {
		t.Fatalf("sum identity left: got %d want %d", got, a)
	}
	if got := m.Combine(a, m.Identity()); got != a {
		t.Fatalf("sum identity right: got %d want %d", got, a)
	}
}

func TestMinMax_Combine(t *testing.T) {
	mn := Min[int]()
	mx := Max[int]()
	b := NewBounded[int]

	got := mn.Combine(mn.Combine(b(5), b(3)), b(8))
	if !got.Set || got.Value != 3 {
		t.Fatalf("min combine: got %+v, want {3, true}", got)
	}
	got = mx.Combine(mx.Combine(b(5), b(3)), b(8))
	if !got.Set || got.Value != 8 {
		t.Fatalf("max combine: got %+v, want {8, true}", got)
	}
}

func TestMinMax_IdentityLawIncludingZero(t *testing.T) {
	// Regression: previously Min/Max returned 0 for Identity, breaking
	// `Combine(Identity, x) == x` whenever x was negative.
	mn := Min[int]()
	mx := Max[int]()
	cases := []int{-5, 0, 5, -100, 100}
	for _, v := range cases {
		x := NewBounded(v)
		if got := mn.Combine(mn.Identity(), x); got != x {
			t.Errorf("min Identity left at %d: got %+v, want %+v", v, got, x)
		}
		if got := mn.Combine(x, mn.Identity()); got != x {
			t.Errorf("min Identity right at %d: got %+v, want %+v", v, got, x)
		}
		if got := mx.Combine(mx.Identity(), x); got != x {
			t.Errorf("max Identity left at %d: got %+v, want %+v", v, got, x)
		}
		if got := mx.Combine(x, mx.Identity()); got != x {
			t.Errorf("max Identity right at %d: got %+v, want %+v", v, got, x)
		}
	}
}

func TestFirstLast(t *testing.T) {
	first := First[string]()
	last := Last[string]()

	a := Stamped[string]{Time: 100, Value: "alpha", Set: true}
	b := Stamped[string]{Time: 200, Value: "beta", Set: true}

	if got := first.Combine(a, b); got.Value != "alpha" {
		t.Fatalf("first(a,b): got %q want alpha", got.Value)
	}
	if got := first.Combine(b, a); got.Value != "alpha" {
		t.Fatalf("first(b,a): got %q want alpha", got.Value)
	}
	if got := last.Combine(a, b); got.Value != "beta" {
		t.Fatalf("last(a,b): got %q want beta", got.Value)
	}
	if got := last.Combine(b, a); got.Value != "beta" {
		t.Fatalf("last(b,a): got %q want beta", got.Value)
	}

	// Identity behavior: empty Stamped should be ignored.
	id := first.Identity()
	if got := first.Combine(id, a); got.Value != "alpha" {
		t.Fatalf("first(id,a): got %q want alpha", got.Value)
	}
	if got := last.Combine(a, id); got.Value != "alpha" {
		t.Fatalf("last(a,id): got %q want alpha", got.Value)
	}
}

func TestSetUnion(t *testing.T) {
	s := Set[string]()
	a := map[string]struct{}{"x": {}, "y": {}}
	b := map[string]struct{}{"y": {}, "z": {}}
	got := s.Combine(a, b)
	for _, k := range []string{"x", "y", "z"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("set union missing %q", k)
		}
	}
	if len(got) != 3 {
		t.Fatalf("set union size: got %d want 3", len(got))
	}
	// Identity = nil/empty.
	if got := s.Combine(nil, a); len(got) != 2 {
		t.Fatalf("set identity left: got len %d want 2", len(got))
	}
}
