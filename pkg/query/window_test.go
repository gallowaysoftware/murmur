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

// fakeStore is an in-memory state.Store[int64] used for query unit tests.
type fakeStore map[state.Key]int64

func (s fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	v, ok := s[k]
	return v, ok, nil
}
func (s fakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s[k]
	}
	return vs, oks, nil
}
func (s fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s[k] += d
	return nil
}
func (s fakeStore) Close() error { return nil }

func TestGetWindow_DailyBuckets(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	// Seed bucket values for "page-A": last 7 days = 1,2,3,4,5,6,7 (sum 28).
	store := fakeStore{}
	for i := 0; i < 7; i++ {
		bucketTime := now.Add(-time.Duration(i) * 24 * time.Hour)
		bucket := w.BucketID(bucketTime)
		store[state.Key{Entity: "page-A", Bucket: bucket}] = int64(i + 1)
	}

	// Last 7 days should sum to 1+2+...+7 = 28.
	got, err := query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	// Note: LastN computes lo = bucket(now - 7d), hi = bucket(now). That's 8 buckets
	// including today and 7 days back, which covers the seeded range fully.
	if got != 28 {
		t.Fatalf("Last7Days: got %d, want 28", got)
	}

	// Last 3 days should be 1+2+3 = 6 (today + yesterday + day-before-yesterday).
	got, err = query.GetWindow(context.Background(), store, core.Sum[int64](), w, "page-A", 3*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetWindow 3d: %v", err)
	}
	if got != 6 {
		t.Fatalf("Last3Days: got %d, want 6", got)
	}

	// Missing keys should be treated as Identity (0) — entity that doesn't exist returns 0.
	got, err = query.GetWindow(context.Background(), store, core.Sum[int64](), w, "missing", 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetWindow missing: %v", err)
	}
	if got != 0 {
		t.Fatalf("missing entity: got %d, want 0", got)
	}
}

func TestGetRange_BoundedRange(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	// 10 days of data, each day has count = day number.
	for i := 0; i < 10; i++ {
		bucketTime := now.Add(-time.Duration(i) * 24 * time.Hour)
		bucket := w.BucketID(bucketTime)
		store[state.Key{Entity: "page-A", Bucket: bucket}] = int64(i + 1)
	}

	// Range covering days 2-5 ago: 3 + 4 + 5 + 6 = 18.
	start := now.Add(-5 * 24 * time.Hour)
	end := now.Add(-2 * 24 * time.Hour)
	got, err := query.GetRange(context.Background(), store, core.Sum[int64](), w, "page-A", start, end)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if got != 18 {
		t.Fatalf("range: got %d, want 18 (sum of days 3,4,5,6)", got)
	}
}

func TestGet_NonWindowed(t *testing.T) {
	store := fakeStore{state.Key{Entity: "page-A"}: 42}

	v, ok, err := query.Get(context.Background(), store, "page-A")
	if err != nil || !ok || v != 42 {
		t.Fatalf("Get hit: got (%d,%v,%v), want (42,true,nil)", v, ok, err)
	}
	v, ok, err = query.Get(context.Background(), store, "missing")
	if err != nil || ok || v != 0 {
		t.Fatalf("Get miss: got (%d,%v,%v), want (0,false,nil)", v, ok, err)
	}
}
