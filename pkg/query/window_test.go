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

func TestGetWindowMany_BatchedAcrossEntities(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	// Three entities with distinct windowed sums:
	//   page-A: last 7d sum = 28
	//   page-B: last 7d sum = 14 (each day = 2)
	//   page-C: last 7d sum = 0  (no data)
	for i := 0; i < 7; i++ {
		bucketTime := now.Add(-time.Duration(i) * 24 * time.Hour)
		bucket := w.BucketID(bucketTime)
		store[state.Key{Entity: "page-A", Bucket: bucket}] = int64(i + 1)
		store[state.Key{Entity: "page-B", Bucket: bucket}] = 2
	}

	got, err := query.GetWindowMany(
		context.Background(), store, core.Sum[int64](), w,
		[]string{"page-A", "page-B", "page-C"},
		7*24*time.Hour, now,
	)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	want := []int64{28, 14, 0}
	if len(got) != len(want) {
		t.Fatalf("len: got %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("[%d]: got %d, want %d", i, got[i], w)
		}
	}
}

func TestGetWindowMany_PreservesOrder(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	for _, e := range []string{"a", "b", "c", "d"} {
		bucket := w.BucketID(now)
		store[state.Key{Entity: e, Bucket: bucket}] = int64(e[0])
	}

	// Request in a non-alphabetical order; result must match input order.
	want := []int64{int64('c'), int64('a'), int64('d'), int64('b')}
	got, err := query.GetWindowMany(
		context.Background(), store, core.Sum[int64](), w,
		[]string{"c", "a", "d", "b"},
		24*time.Hour, now,
	)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("[%d] (entity in slot): got %d, want %d", i, got[i], w)
		}
	}
}

func TestGetWindowMany_EmptyEntities(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	got, err := query.GetWindowMany(
		context.Background(), fakeStore{}, core.Sum[int64](), w,
		nil, 24*time.Hour, time.Now(),
	)
	if err != nil {
		t.Fatalf("GetWindowMany([]): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len: got %d, want 0", len(got))
	}
}

// countingStore wraps a fakeStore and tracks how many calls landed.
// Used to verify GetWindowMany makes ONE store.GetMany call rather than N.
type countingStore struct {
	inner   fakeStore
	getMany int
}

func (s *countingStore) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	return s.inner.Get(ctx, k)
}
func (s *countingStore) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	s.getMany++
	return s.inner.GetMany(ctx, ks)
}
func (s *countingStore) MergeUpdate(ctx context.Context, k state.Key, d int64, ttl time.Duration) error {
	return s.inner.MergeUpdate(ctx, k, d, ttl)
}
func (*countingStore) Close() error { return nil }

func TestGetWindowMany_OneStoreCall(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &countingStore{inner: fakeStore{}}
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		for _, e := range []string{"a", "b", "c", "d", "e"} {
			store.inner[state.Key{Entity: e, Bucket: bucket}] = 1
		}
	}

	_, err := query.GetWindowMany(context.Background(), store, core.Sum[int64](), w,
		[]string{"a", "b", "c", "d", "e"},
		7*24*time.Hour, now,
	)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	// 5 entities × 8 buckets = 40 keys, single store.GetMany call.
	if store.getMany != 1 {
		t.Errorf("store.GetMany calls: got %d, want 1 (the whole point of WindowMany)", store.getMany)
	}
}
