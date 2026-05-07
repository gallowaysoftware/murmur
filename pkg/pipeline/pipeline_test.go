package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
)

type testEvent struct {
	PageID string
}

type fakeSource struct{}

func (fakeSource) Read(ctx context.Context, out chan<- source.Record[testEvent]) error {
	close(out)
	return nil
}
func (fakeSource) Name() string { return "fake" }
func (fakeSource) Close() error { return nil }

func TestBuild_RequiredFields(t *testing.T) {
	// Each missing field should be flagged in turn.
	tests := []struct {
		name    string
		mutate  func(*pipeline.Pipeline[testEvent, string, int64])
		wantErr error
	}{
		{
			name:    "missing source",
			mutate:  func(p *pipeline.Pipeline[testEvent, string, int64]) {},
			wantErr: pipeline.ErrMissingSource,
		},
		{
			name: "missing key fn",
			mutate: func(p *pipeline.Pipeline[testEvent, string, int64]) {
				p.From(fakeSource{})
			},
			wantErr: pipeline.ErrMissingKeyFn,
		},
		{
			name: "missing value fn",
			mutate: func(p *pipeline.Pipeline[testEvent, string, int64]) {
				p.From(fakeSource{}).Key(func(e testEvent) string { return e.PageID })
			},
			wantErr: pipeline.ErrMissingValueFn,
		},
		{
			name: "missing monoid",
			mutate: func(p *pipeline.Pipeline[testEvent, string, int64]) {
				p.From(fakeSource{}).
					Key(func(e testEvent) string { return e.PageID }).
					Value(func(testEvent) int64 { return 1 })
			},
			wantErr: pipeline.ErrMissingMonoid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := pipeline.NewPipeline[testEvent, string, int64]("test")
			tc.mutate(p)
			if err := p.Build(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestBuild_AllFieldsSet(t *testing.T) {
	p := pipeline.NewPipeline[testEvent, string, int64]("page_views").
		From(fakeSource{}).
		Key(func(e testEvent) string { return e.PageID }).
		Value(func(testEvent) int64 { return 1 }).
		Aggregate(core.Sum[int64]())
	// Note: state.Store/Cache require concrete implementations; we exercise the field
	// validation here by exposing StoreIn via interface conformance in Phase 2 once
	// we have a working stub store. For now ErrMissingStore is the expected outcome.
	if err := p.Build(); !errors.Is(err, pipeline.ErrMissingStore) {
		t.Fatalf("got %v, want ErrMissingStore", err)
	}
}
