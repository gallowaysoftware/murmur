package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeCache is a minimal in-memory state.Cache[int64] for warmup tests.
// Repopulate copies entries from the source store; the rest of the
// Cache contract is the same as fakeStore.
type fakeCache struct {
	m map[state.Key]int64
}

func newFakeCache() *fakeCache { return &fakeCache{m: map[state.Key]int64{}} }

func (c *fakeCache) Get(_ context.Context, k state.Key) (int64, bool, error) {
	v, ok := c.m[k]
	return v, ok, nil
}
func (c *fakeCache) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = c.m[k]
	}
	return vs, oks, nil
}
func (c *fakeCache) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	c.m[k] += d
	return nil
}
func (c *fakeCache) Close() error { return nil }
func (c *fakeCache) Repopulate(ctx context.Context, src state.Store[int64], keys []state.Key) error {
	vals, oks, err := src.GetMany(ctx, keys)
	if err != nil {
		return err
	}
	for i, k := range keys {
		if oks[i] {
			c.m[k] = vals[i]
		}
	}
	return nil
}

func TestWarmupWindowed_LoadsHotKeys(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		store[state.Key{Entity: "page-A", Bucket: bucket}] = int64(i + 1)
		store[state.Key{Entity: "page-B", Bucket: bucket}] = int64(i + 100)
	}

	cache := newFakeCache()
	warmed, err := query.WarmupWindowed[int64](
		context.Background(), cache, store, w,
		[]string{"page-A", "page-B"},
		7*24*time.Hour, now,
	)
	if err != nil {
		t.Fatalf("WarmupWindowed: %v", err)
	}
	// 2 entities × 7 days each present in store → 14 warmed.
	if warmed != 14 {
		t.Errorf("warmed: got %d, want 14", warmed)
	}
	// Every (entity, bucket) the store had should now be in the cache.
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		if v, ok, _ := cache.Get(context.Background(), state.Key{Entity: "page-A", Bucket: bucket}); !ok || v != int64(i+1) {
			t.Errorf("page-A day-%d: cache (%d, %v), want (%d, true)", i, v, ok, i+1)
		}
	}
}

func TestWarmupWindowed_SkipsAbsentEntities(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	bucket := w.BucketID(now)
	store[state.Key{Entity: "present", Bucket: bucket}] = 42

	cache := newFakeCache()
	warmed, err := query.WarmupWindowed[int64](
		context.Background(), cache, store, w,
		[]string{"present", "absent"},
		24*time.Hour, now,
	)
	if err != nil {
		t.Fatalf("WarmupWindowed: %v", err)
	}
	// "present" has 1 bucket of data; "absent" has none → 1 warmed.
	if warmed != 1 {
		t.Errorf("warmed: got %d, want 1", warmed)
	}
	if _, ok, _ := cache.Get(context.Background(), state.Key{Entity: "absent", Bucket: bucket}); ok {
		t.Error("absent entity should not be in cache")
	}
}

func TestWarmupWindowed_EmptyInput(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	warmed, err := query.WarmupWindowed[int64](
		context.Background(), newFakeCache(), fakeStore{}, w,
		nil, 24*time.Hour, time.Now(),
	)
	if err != nil {
		t.Fatalf("WarmupWindowed([]): %v", err)
	}
	if warmed != 0 {
		t.Errorf("warmed: got %d, want 0", warmed)
	}
}

func TestWarmupWindowed_NilCacheReturnsError(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	_, err := query.WarmupWindowed[int64](
		context.Background(), nil, fakeStore{}, w,
		[]string{"x"}, 24*time.Hour, time.Now(),
	)
	if err == nil {
		t.Error("WarmupWindowed(cache=nil): expected error")
	}
}

func TestWarmupNonWindowed_LoadsAllTime(t *testing.T) {
	store := fakeStore{
		state.Key{Entity: "a"}: 1,
		state.Key{Entity: "b"}: 2,
		state.Key{Entity: "c"}: 3,
	}
	cache := newFakeCache()
	warmed, err := query.WarmupNonWindowed[int64](
		context.Background(), cache, store,
		[]string{"a", "b", "c", "missing"},
	)
	if err != nil {
		t.Fatalf("WarmupNonWindowed: %v", err)
	}
	if warmed != 3 {
		t.Errorf("warmed: got %d, want 3", warmed)
	}
	for _, e := range []string{"a", "b", "c"} {
		if _, ok, _ := cache.Get(context.Background(), state.Key{Entity: e}); !ok {
			t.Errorf("entity %s: missing from cache after warmup", e)
		}
	}
}
