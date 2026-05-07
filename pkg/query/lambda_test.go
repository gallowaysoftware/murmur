package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeStore is reused from window_test.go in the same package.

func TestLambda_GetMerges(t *testing.T) {
	view := fakeStore{state.Key{Entity: "page-A"}: 100} // batch result: 100
	delta := fakeStore{state.Key{Entity: "page-A"}: 7}  // streaming delta since last batch: +7

	q := query.LambdaQuery[int64]{View: view, Delta: delta, Monoid: core.Sum[int64]()}
	got, ok, err := q.Get(context.Background(), "page-A")
	if err != nil || !ok || got != 107 {
		t.Fatalf("Get: got (%d,%v,%v), want (107,true,nil)", got, ok, err)
	}
}

func TestLambda_GetMissingInView(t *testing.T) {
	view := fakeStore{}
	delta := fakeStore{state.Key{Entity: "fresh"}: 42}

	q := query.LambdaQuery[int64]{View: view, Delta: delta, Monoid: core.Sum[int64]()}
	got, ok, err := q.Get(context.Background(), "fresh")
	if err != nil || !ok || got != 42 {
		t.Fatalf("only-in-delta: got (%d,%v,%v), want (42,true,nil)", got, ok, err)
	}
}

func TestLambda_GetMissingInDelta(t *testing.T) {
	view := fakeStore{state.Key{Entity: "stable"}: 999}
	delta := fakeStore{}

	q := query.LambdaQuery[int64]{View: view, Delta: delta, Monoid: core.Sum[int64]()}
	got, ok, err := q.Get(context.Background(), "stable")
	if err != nil || !ok || got != 999 {
		t.Fatalf("only-in-view: got (%d,%v,%v), want (999,true,nil)", got, ok, err)
	}
}

func TestLambda_GetMissingInBoth(t *testing.T) {
	q := query.LambdaQuery[int64]{
		View:   fakeStore{},
		Delta:  fakeStore{},
		Monoid: core.Sum[int64](),
	}
	got, ok, err := q.Get(context.Background(), "ghost")
	if err != nil || ok || got != 0 {
		t.Fatalf("absent: got (%d,%v,%v), want (0,false,nil)", got, ok, err)
	}
}

func TestLambda_GetWindowMergesPerBucket(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	// view holds the batch result for days 0..6 ago: 10,9,8,7,6,5,4 (sum=49)
	// delta holds streaming-since-batch: only days 0,1 have late updates: +2, +3 (sum=5)
	// expected Last7Days = 49 + 5 = 54
	view := fakeStore{}
	delta := fakeStore{}
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		view[state.Key{Entity: "page-A", Bucket: bucket}] = int64(10 - i)
	}
	delta[state.Key{Entity: "page-A", Bucket: w.BucketID(now)}] = 2
	delta[state.Key{Entity: "page-A", Bucket: w.BucketID(now.Add(-24 * time.Hour))}] = 3

	q := query.LambdaQuery[int64]{View: view, Delta: delta, Monoid: core.Sum[int64]()}

	got, err := q.GetWindow(context.Background(), w, "page-A", 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	const want int64 = 49 + 5
	if got != want {
		t.Fatalf("Last7Days lambda merge: got %d, want %d", got, want)
	}
}
