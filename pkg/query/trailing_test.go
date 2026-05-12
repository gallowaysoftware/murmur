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

// TestGetTrailing_7DayAcross8Buckets seeds 8 daily buckets but only the
// 7 most-recent should be merged by GetTrailing(7d). The 8th bucket
// (a day past the trailing-7d horizon) is present in the store but
// must not contribute to the result — this verifies the LastN window
// boundary on the trailing path.
func TestGetTrailing_7DayAcross8Buckets(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	// Seed 8 days of buckets: i=0..6 are inside the trailing-7d window
	// (today + previous 6 = 7 buckets), i=7 is just past.
	store := fakeStore{}
	var insideSum int64
	for i := 0; i < 8; i++ {
		bucketTime := now.Add(-time.Duration(i) * 24 * time.Hour)
		bucket := w.BucketID(bucketTime)
		v := int64(i + 1)
		store[state.Key{Entity: "page-A", Bucket: bucket}] = v
		if i < 7 {
			insideSum += v
		}
	}
	// Hand-summed: 1+2+...+7 = 28. The 8th bucket (value 8) is excluded.
	const wantHand = 28
	if insideSum != wantHand {
		t.Fatalf("test bug: insideSum=%d, want %d", insideSum, wantHand)
	}

	got, err := query.GetTrailing(context.Background(), store, core.Sum[int64](), w, "page-A", 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetTrailing: %v", err)
	}
	if got != insideSum {
		t.Fatalf("trailing-7d: got %d, want %d (8th bucket must be excluded)", got, insideSum)
	}
}

// TestGetTrailing_30DayRetentionBoundary checks the boundary case where
// the trailing window is sized just under the retention horizon. The
// trailing-30d query reads N buckets where N = ceil(30d/1d) = 30 buckets
// (today + previous 29). The 31st bucket of seeded data — the one the
// retention slack is for — must not contribute, even though it is
// still in the store.
func TestGetTrailing_30DayRetentionBoundary(t *testing.T) {
	// Daily windowed config; retention is 31d (matches what Trailing()
	// would size for a max-30d trailing window plus a 1d slack bucket).
	w := windowed.Daily(31 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	// Seed 31 daily buckets each with value 1.
	store := fakeStore{}
	for i := 0; i < 31; i++ {
		bucketTime := now.Add(-time.Duration(i) * 24 * time.Hour)
		bucket := w.BucketID(bucketTime)
		store[state.Key{Entity: "post-1", Bucket: bucket}] = 1
	}

	got, err := query.GetTrailing(context.Background(), store, core.Sum[int64](), w, "post-1", 30*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetTrailing: %v", err)
	}
	// Trailing-30d reads 30 daily buckets — today + the previous 29.
	// The 31st seeded bucket is the slack day; the store kept it so
	// queries near midnight don't miss data, but a trailing-30d query
	// at noon must not count it.
	if got != 30 {
		t.Fatalf("trailing-30d: got %d, want 30 (slack bucket excluded)", got)
	}
}

// TestGetTrailingMany_MatchesSingleEntityLoop asserts that the batch
// helper produces identical results to calling GetTrailing in a loop.
// This is the contract the "Many" variant promises: ONE store fetch
// vs N, same answers.
func TestGetTrailingMany_MatchesSingleEntityLoop(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	entities := []string{"a", "b", "c", "d"}
	// Seed differing per-day counts so each entity has a distinct sum.
	for i := 0; i < 7; i++ {
		bucketTime := now.Add(-time.Duration(i) * 24 * time.Hour)
		bucket := w.BucketID(bucketTime)
		for idx, e := range entities {
			store[state.Key{Entity: e, Bucket: bucket}] = int64(idx+1) * int64(i+1)
		}
	}

	// Reference: one-by-one loop.
	want := make([]int64, len(entities))
	for i, e := range entities {
		v, err := query.GetTrailing(context.Background(), store, core.Sum[int64](), w, e, 7*24*time.Hour, now)
		if err != nil {
			t.Fatalf("GetTrailing(%q): %v", e, err)
		}
		want[i] = v
	}

	// Batched version.
	got, err := query.GetTrailingMany(context.Background(), store, core.Sum[int64](), w, entities, 7*24*time.Hour, now)
	if err != nil {
		t.Fatalf("GetTrailingMany: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("len: got %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("[%d] entity %q: got %d, want %d", i, entities[i], got[i], w)
		}
	}
}

// TestGetTrailingMany_OneStoreCall confirms the batched path issues a
// single underlying store.GetMany across all (entity, bucket) pairs,
// matching the GetWindowMany contract this helper delegates to.
func TestGetTrailingMany_OneStoreCall(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := &countingStore{inner: fakeStore{}}
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		for _, e := range []string{"a", "b", "c"} {
			store.inner[state.Key{Entity: e, Bucket: bucket}] = 1
		}
	}

	_, err := query.GetTrailingMany(context.Background(), store, core.Sum[int64](), w,
		[]string{"a", "b", "c"},
		7*24*time.Hour, now,
	)
	if err != nil {
		t.Fatalf("GetTrailingMany: %v", err)
	}
	if store.getMany != 1 {
		t.Errorf("store.GetMany calls: got %d, want 1", store.getMany)
	}
}
