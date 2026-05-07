package compose_test

import (
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid/compose"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
)

func TestMapMerge_PerKeyCombine(t *testing.T) {
	m := compose.MapMerge[string](core.Sum[int64]())

	a := map[string]int64{"x": 1, "y": 2}
	b := map[string]int64{"y": 3, "z": 4}

	got := m.Combine(a, b)
	want := map[string]int64{"x": 1, "y": 5, "z": 4} // y key collides → 2+3
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got %d, want %d", k, got[k], v)
		}
	}
	if len(got) != 3 {
		t.Errorf("expected 3 keys, got %d", len(got))
	}
}

func TestMapMerge_Associativity(t *testing.T) {
	m := compose.MapMerge[string](core.Sum[int64]())
	a := map[string]int64{"x": 1}
	b := map[string]int64{"x": 2, "y": 5}
	c := map[string]int64{"y": 7, "z": 3}

	left := m.Combine(m.Combine(a, b), c)
	right := m.Combine(a, m.Combine(b, c))
	for k, v := range left {
		if right[k] != v {
			t.Errorf("associativity broken at %q: left=%d, right=%d", k, v, right[k])
		}
	}
	if len(left) != len(right) {
		t.Errorf("len: left=%d, right=%d", len(left), len(right))
	}
}

func TestMapMerge_Identity(t *testing.T) {
	m := compose.MapMerge[string](core.Sum[int64]())
	a := map[string]int64{"x": 7}
	if got := m.Combine(m.Identity(), a); got["x"] != 7 {
		t.Fatalf("Combine(Identity,a): got %v", got)
	}
	if got := m.Combine(a, m.Identity()); got["x"] != 7 {
		t.Fatalf("Combine(a,Identity): got %v", got)
	}
}

func TestTupleMonoid2_ComponentwiseCombine(t *testing.T) {
	m := compose.TupleMonoid2[int64, int64](core.Sum[int64](), core.Max[int64]())

	a := compose.Tuple2[int64, int64]{A: 5, B: 3}
	b := compose.Tuple2[int64, int64]{A: 7, B: 11}

	got := m.Combine(a, b)
	if got.A != 12 {
		t.Errorf(".A: got %d, want 12 (Sum)", got.A)
	}
	if got.B != 11 {
		t.Errorf(".B: got %d, want 11 (Max)", got.B)
	}
}

func TestTupleMonoid2_Identity(t *testing.T) {
	m := compose.TupleMonoid2[int64, int64](core.Sum[int64](), core.Sum[int64]())
	id := m.Identity()
	if id.A != 0 || id.B != 0 {
		t.Fatalf("identity should be (0,0), got %+v", id)
	}
}
