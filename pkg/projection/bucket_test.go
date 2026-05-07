package projection_test

import (
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/projection"
)

func TestLogBucket(t *testing.T) {
	cases := []struct {
		v    int64
		want int
	}{
		{-100, 0},
		{0, 0},
		{1, 0},
		{9, 0},
		{10, 1},
		{99, 1},
		{100, 2},
		{999, 2},
		{1000, 3},
		{9999, 3},
		{10_000, 4},
		{1_000_000, 6},
		{10_000_000, 7},
	}
	for _, c := range cases {
		if got := projection.LogBucket(c.v); got != c.want {
			t.Errorf("LogBucket(%d): got %d, want %d", c.v, got, c.want)
		}
	}
}

func TestLogBucket_Monotonic(t *testing.T) {
	prev := projection.LogBucket(0)
	for v := int64(0); v <= 10_000_000; v += 1000 {
		got := projection.LogBucket(v)
		if got < prev {
			t.Fatalf("LogBucket non-monotonic at v=%d: prev=%d got=%d", v, prev, got)
		}
		prev = got
	}
}

func TestLinearBucket(t *testing.T) {
	bf := projection.LinearBucket(100)
	cases := []struct {
		v    int64
		want int
	}{
		{0, 0},
		{99, 0},
		{100, 1},
		{199, 1},
		{200, 2},
		{1000, 10},
	}
	for _, c := range cases {
		if got := bf(c.v); got != c.want {
			t.Errorf("LinearBucket(100)(%d): got %d, want %d", c.v, got, c.want)
		}
	}
}

func TestLinearBucket_DegenerateBandSize(t *testing.T) {
	bf := projection.LinearBucket(0)
	if bf(42) != 42 {
		t.Errorf("LinearBucket(0) should fall back to bandSize=1: got %d, want 42", bf(42))
	}
}

func TestManualBucket(t *testing.T) {
	bf := projection.ManualBucket([]int64{10_000, 100_000, 1_000_000})
	cases := []struct {
		v    int64
		want int
	}{
		{0, 0},
		{9_999, 0},
		{10_000, 1},
		{50_000, 1},
		{100_000, 2},
		{500_000, 2},
		{1_000_000, 3},
		{5_000_000, 3},
	}
	for _, c := range cases {
		if got := bf(c.v); got != c.want {
			t.Errorf("ManualBucket(%d): got %d, want %d", c.v, got, c.want)
		}
	}
}

func TestManualBucket_UnsortedPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on unsorted breakpoints")
		}
	}()
	_ = projection.ManualBucket([]int64{100, 10, 1000})
}

func TestDetect_NoChange(t *testing.T) {
	tr := projection.Detect(projection.LogBucket, 100, 500) // both bucket 2
	if tr.Changed {
		t.Errorf("Detect(100, 500): expected no change (both bucket 2), got %+v", tr)
	}
}

func TestDetect_ChangeUp(t *testing.T) {
	tr := projection.Detect(projection.LogBucket, 999, 1000)
	if !tr.Changed || tr.OldBucket != 2 || tr.NewBucket != 3 {
		t.Errorf("Detect(999, 1000): got %+v, want changed 2→3", tr)
	}
}

func TestDetect_ChangeDown(t *testing.T) {
	tr := projection.Detect(projection.LogBucket, 1000, 999)
	if !tr.Changed || tr.OldBucket != 3 || tr.NewBucket != 2 {
		t.Errorf("Detect(1000, 999): got %+v, want changed 3→2", tr)
	}
}

func TestHysteresis_DampsOscillation(t *testing.T) {
	// With band=20% and inner=LogBucket, a value oscillating between
	// 950 and 1100 (around the bucket-2/3 boundary at 1000) should
	// stay in the bucket it started in, NOT flip on every observation.
	h := projection.HysteresisBucket{
		Inner: projection.LogBucket,
		Band:  0.20,
	}

	// Start at 500 (bucket 2), no prior observation.
	prev := h.Apply(-1, 500)
	if prev != 2 {
		t.Fatalf("initial bucket: got %d, want 2", prev)
	}

	// Oscillate near boundary.
	values := []int64{950, 1100, 950, 1100, 950, 1100}
	transitions := 0
	for _, v := range values {
		new := h.Apply(prev, v)
		if new != prev {
			transitions++
		}
		prev = new
	}

	// With 20% hysteresis, all values are within ±20% of 1000, so
	// none should trigger a transition.
	if transitions != 0 {
		t.Errorf("oscillation across boundary: %d transitions, want 0 (hysteresis should suppress)", transitions)
	}
}

func TestHysteresis_AllowsRealTransitions(t *testing.T) {
	// A value going from 500 to 5000 should transition (bucket 2 → 3)
	// even with hysteresis — 5000 is well outside the 20% band on 1000.
	h := projection.HysteresisBucket{
		Inner: projection.LogBucket,
		Band:  0.20,
	}

	prev := h.Apply(-1, 500) // bucket 2
	next := h.Apply(prev, 5000)
	if next != 3 {
		t.Errorf("Apply(2, 5000): got %d, want 3 (real transition past hysteresis)", next)
	}
}

func TestHysteresis_DisabledWithBandZero(t *testing.T) {
	h := projection.HysteresisBucket{Inner: projection.LogBucket, Band: 0}
	// Should match Inner exactly.
	if got := h.Apply(2, 1000); got != 3 {
		t.Errorf("Band=0 should be identity over Inner: got %d, want 3", got)
	}
	if got := h.Apply(3, 999); got != 2 {
		t.Errorf("Band=0 should be identity over Inner: got %d, want 2", got)
	}
}

func TestHysteresis_TransitionDown(t *testing.T) {
	// Starting at bucket 3 (e.g. value 5000), if the value drops to 950
	// — within 20% hysteresis of 1000 — we should stay at bucket 3.
	// If it drops further to 500, we should transition to bucket 2.
	h := projection.HysteresisBucket{Inner: projection.LogBucket, Band: 0.20}

	// Establish prev=3 from a clearly-bucket-3 value.
	prev := h.Apply(-1, 5000)
	if prev != 3 {
		t.Fatalf("init: got %d, want 3", prev)
	}
	// Drop to 950 — within hysteresis band, should stay 3.
	if got := h.Apply(prev, 950); got != 3 {
		t.Errorf("950 with prev=3, band=20%%: got %d, want 3 (sticky)", got)
	}
	// Drop to 500 — well below band, should transition to 2.
	if got := h.Apply(prev, 500); got != 2 {
		t.Errorf("500 with prev=3, band=20%%: got %d, want 2 (real drop)", got)
	}
}
