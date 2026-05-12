package murmur_test

import (
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/murmur"
)

func TestCounter_Trailing_SizesRetentionToMaxPlusSlack(t *testing.T) {
	b := murmur.Counter[ev]("post_likes").
		KeyBy(func(e ev) string { return e.K }).
		Trailing(24*time.Hour, 7*24*time.Hour, 30*24*time.Hour)

	p := b.Build()
	w := p.Window()
	if w == nil {
		t.Fatalf("expected windowed pipeline, got nil window")
	}
	if w.Granularity != 24*time.Hour {
		t.Errorf("granularity: got %v, want 24h", w.Granularity)
	}
	// Retention = max(durations) + 1 day slack = 31d.
	wantRetention := 31 * 24 * time.Hour
	if w.Retention != wantRetention {
		t.Errorf("retention: got %v, want %v", w.Retention, wantRetention)
	}

	got := b.TrailingWindows()
	want := []time.Duration{24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour}
	if len(got) != len(want) {
		t.Fatalf("trailing windows len: got %d, want %d", len(got), len(want))
	}
	for i, d := range want {
		if got[i] != d {
			t.Errorf("[%d]: got %v, want %v", i, got[i], d)
		}
	}
}

func TestCounter_Trailing_DropsZeroAndNegative(t *testing.T) {
	b := murmur.Counter[ev]("test").
		KeyBy(func(e ev) string { return e.K }).
		Trailing(0, -time.Hour, 7*24*time.Hour, 0)

	p := b.Build()
	w := p.Window()
	if w == nil {
		t.Fatalf("expected windowed pipeline")
	}
	if w.Retention != 8*24*time.Hour {
		t.Errorf("retention: got %v, want %v (7d max + 1d slack)", w.Retention, 8*24*time.Hour)
	}
	got := b.TrailingWindows()
	if len(got) != 1 || got[0] != 7*24*time.Hour {
		t.Errorf("trailing windows: got %v, want [7d]", got)
	}
}

func TestCounter_Trailing_NoDurationsIsNoOp(t *testing.T) {
	b := murmur.Counter[ev]("test").
		KeyBy(func(e ev) string { return e.K }).
		Daily(5 * 24 * time.Hour)

	// Call Trailing() with no positive durations — should preserve the
	// previously-set Daily window rather than wiping it.
	b2 := b.Trailing()
	p := b2.Build()
	w := p.Window()
	if w == nil {
		t.Fatalf("expected windowed pipeline (Daily) to be preserved")
	}
	if w.Retention != 5*24*time.Hour {
		t.Errorf("retention: got %v, want %v (preserved Daily)", w.Retention, 5*24*time.Hour)
	}
	if got := b2.TrailingWindows(); got != nil {
		t.Errorf("expected nil TrailingWindows, got %v", got)
	}
}

func TestCounter_Trailing_SupersedesPriorDaily(t *testing.T) {
	b := murmur.Counter[ev]("test").
		KeyBy(func(e ev) string { return e.K }).
		Daily(100 * 24 * time.Hour). // would otherwise win
		Trailing(7 * 24 * time.Hour) // 7d + 1d slack = 8d retention

	p := b.Build()
	w := p.Window()
	if w == nil {
		t.Fatalf("expected windowed pipeline")
	}
	if w.Retention != 8*24*time.Hour {
		t.Errorf("retention: got %v, want %v (Trailing supersedes Daily)", w.Retention, 8*24*time.Hour)
	}
}
