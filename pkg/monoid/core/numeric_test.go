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

func TestMinMaxIdentity(t *testing.T) {
	mn := Min[int]()
	mx := Max[int]()
	if mn.Combine(mn.Combine(5, 3), 8) != 3 {
		t.Fatalf("min combine wrong")
	}
	if mx.Combine(mx.Combine(5, 3), 8) != 8 {
		t.Fatalf("max combine wrong")
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
