package pipeline_test

import (
	"errors"
	"testing"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
)

type testEvent struct {
	PageID string
}

func TestBuild_RequiredFields(t *testing.T) {
	// Each missing field should be flagged in turn.
	tests := []struct {
		name    string
		mutate  func(*pipeline.Pipeline[testEvent, int64])
		wantErr error
	}{
		{
			name:    "missing key fn",
			mutate:  func(p *pipeline.Pipeline[testEvent, int64]) {},
			wantErr: pipeline.ErrMissingKeyFn,
		},
		{
			name: "missing value fn",
			mutate: func(p *pipeline.Pipeline[testEvent, int64]) {
				p.Key(func(e testEvent) string { return e.PageID })
			},
			wantErr: pipeline.ErrMissingValueFn,
		},
		{
			name: "missing monoid",
			mutate: func(p *pipeline.Pipeline[testEvent, int64]) {
				p.Key(func(e testEvent) string { return e.PageID }).
					Value(func(testEvent) int64 { return 1 })
			},
			wantErr: pipeline.ErrMissingMonoid,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := pipeline.NewPipeline[testEvent, int64]("test")
			tc.mutate(p)
			if err := p.Build(); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestBuild_AllRequiredFieldsSet(t *testing.T) {
	p := pipeline.NewPipeline[testEvent, int64]("page_views").
		Key(func(e testEvent) string { return e.PageID }).
		Value(func(testEvent) int64 { return 1 }).
		Aggregate(core.Sum[int64]())
	// State store still required even without a Source — Build covers everything but
	// the mode-specific source.
	if err := p.Build(); !errors.Is(err, pipeline.ErrMissingStore) {
		t.Fatalf("got %v, want ErrMissingStore", err)
	}
}
