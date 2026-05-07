package murmur_test

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/murmur"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
)

func decodeFloat64(t *testing.T, b []byte) float64 {
	t.Helper()
	if len(b) < 8 {
		t.Fatalf("decodeFloat64: short input %d", len(b))
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(b[:8]))
}

type ev struct {
	K string
	U string
}

func TestCounter_BuildMissingFields(t *testing.T) {
	p := murmur.Counter[ev]("test").Build()
	// No KeyBy / StoreIn — Build should still produce a Pipeline, but
	// pipeline.Build (validation) should report what's missing in order.
	err := p.Build()
	if !errors.Is(err, pipeline.ErrMissingKeyFn) {
		t.Fatalf("expected ErrMissingKeyFn, got %v", err)
	}
}

func TestCounter_NotWindowed(t *testing.T) {
	p := murmur.Counter[ev]("test").
		KeyBy(func(e ev) string { return e.K }).
		Build()
	if p.Window() != nil {
		t.Errorf("expected nil window, got %+v", p.Window())
	}
}

func TestCounter_Daily(t *testing.T) {
	p := murmur.Counter[ev]("test").
		KeyBy(func(e ev) string { return e.K }).
		Daily(7 * 24 * time.Hour).
		Build()
	if p.Window() == nil {
		t.Fatalf("expected daily window")
	}
	if p.Window().Granularity != 24*time.Hour {
		t.Errorf("granularity: got %v, want 24h", p.Window().Granularity)
	}
}

func TestUniqueCount_Build(t *testing.T) {
	p := murmur.UniqueCount[ev]("test", func(e ev) []byte { return []byte(e.U) }).
		KeyBy(func(e ev) string { return e.K }).
		Build()
	if p.Name() != "test" {
		t.Errorf("name: got %q, want test", p.Name())
	}
	// HLL value type is []byte; sanity check the value extractor produces non-nil.
	got := p.ValueFn()(ev{K: "x", U: "alice"})
	if len(got) == 0 {
		t.Errorf("value extractor returned empty bytes")
	}
}

func TestTopN_Build(t *testing.T) {
	p := murmur.TopN[ev]("test", 5, func(e ev) string { return e.U }).
		KeyBy(func(e ev) string { return e.K }).
		Build()
	got := p.ValueFn()(ev{K: "x", U: "alice"})
	if len(got) == 0 {
		t.Errorf("topk Single bytes empty")
	}
}

func TestTrending_BuildAndValue(t *testing.T) {
	at := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	p := murmur.Trending[ev]("hot_posts", time.Hour).
		KeyBy(func(e ev) string { return e.K }).
		Hourly(7 * 24 * time.Hour).
		Clock(func() time.Time { return at }).
		Build()

	if p.Name() != "hot_posts" {
		t.Errorf("name: got %q, want hot_posts", p.Name())
	}
	if p.Window() == nil {
		t.Fatal("expected windowed pipeline")
	}
	// Value extractor should produce a 17-byte Decayed wire form with the
	// configured clock as the timestamp.
	bytes := p.ValueFn()(ev{K: "post-1", U: "u-1"})
	if len(bytes) != 17 {
		t.Fatalf("Decayed wire size: got %d, want 17", len(bytes))
	}
}

func TestTrending_AmountOverride(t *testing.T) {
	p := murmur.Trending[ev]("weighted_hot", time.Hour).
		KeyBy(func(e ev) string { return e.K }).
		// Verified-account boost: a like from "verified-user" weighs 5×.
		Amount(func(e ev) float64 {
			if e.U == "verified-user" {
				return 5.0
			}
			return 1.0
		}).
		Clock(func() time.Time { return time.Unix(0, 0) }). // deterministic
		Build()

	verifiedBytes := p.ValueFn()(ev{K: "post-1", U: "verified-user"})
	regularBytes := p.ValueFn()(ev{K: "post-1", U: "regular-user"})

	// Both serialize to 17 bytes; verify the encoded amounts.
	verified := decodeFloat64(t, verifiedBytes[0:8])
	regular := decodeFloat64(t, regularBytes[0:8])
	if verified != 5.0 {
		t.Errorf("verified amount: got %v, want 5.0", verified)
	}
	if regular != 1.0 {
		t.Errorf("regular amount: got %v, want 1.0", regular)
	}
}
