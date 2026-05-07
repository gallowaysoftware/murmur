package murmur_test

import (
	"errors"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/murmur"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
)

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
