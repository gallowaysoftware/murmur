package valkey_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/state"
	mvalkey "github.com/gallowaysoftware/murmur/pkg/state/valkey"
)

// fakeBytesStore mirrors fakeStore but for []byte. Used to exercise
// BytesCache.Repopulate without a real DDB BytesStore.
type fakeBytesStore struct{ m map[state.Key][]byte }

func (s *fakeBytesStore) Get(_ context.Context, k state.Key) ([]byte, bool, error) {
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fakeBytesStore) GetMany(_ context.Context, ks []state.Key) ([][]byte, []bool, error) {
	vs := make([][]byte, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}
func (s *fakeBytesStore) MergeUpdate(_ context.Context, k state.Key, d []byte, _ time.Duration) error {
	if s.m == nil {
		s.m = map[state.Key][]byte{}
	}
	s.m[k] = d
	return nil
}
func (s *fakeBytesStore) Close() error { return nil }

func newBytesCache(t *testing.T, prefix string) *mvalkey.BytesCache {
	t.Helper()
	c, err := mvalkey.NewBytesCache(mvalkey.BytesConfig{
		Address:   valkeyAddress(t),
		KeyPrefix: prefix,
		Monoid:    hll.HLL(),
	})
	if err != nil {
		t.Fatalf("new BytesCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestBytesCache_HLL_AddAndCount(t *testing.T) {
	c := newBytesCache(t, fmt.Sprintf("test_hll_%d", time.Now().UnixNano()))
	ctx := context.Background()
	key := state.Key{Entity: "page-A"}

	// Add 100 distinct elements via 100 single-element HLL sketches.
	for i := 0; i < 100; i++ {
		if err := c.MergeUpdate(ctx, key, hll.Single([]byte(fmt.Sprintf("user-%d", i))), 0); err != nil {
			t.Fatalf("MergeUpdate %d: %v", i, err)
		}
	}

	got, ok, err := c.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get: %v, ok=%v", err, ok)
	}
	count, err := hll.Estimate(got)
	if err != nil {
		t.Fatalf("hll.Estimate: %v", err)
	}
	// HLL has ~1-2% standard error; 100 distinct should land in [97, 103].
	if count < 95 || count > 105 {
		t.Errorf("HLL cardinality: got %d, want ~100 (within HLL error)", count)
	}
}

func TestBytesCache_TopK_TracksHotKeys(t *testing.T) {
	c, err := mvalkey.NewBytesCache(mvalkey.BytesConfig{
		Address:   valkeyAddress(t),
		KeyPrefix: fmt.Sprintf("test_topk_%d", time.Now().UnixNano()),
		Monoid:    topk.New(5),
	})
	if err != nil {
		t.Fatalf("new BytesCache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	key := state.Key{Entity: "global"}

	// 10 hits on "post-A", 5 on "post-B", 1 each on -C and -D.
	hits := map[string]int{"post-A": 10, "post-B": 5, "post-C": 1, "post-D": 1}
	for k, n := range hits {
		for i := 0; i < n; i++ {
			if err := c.MergeUpdate(ctx, key, topk.SingleN(5, k, 1), 0); err != nil {
				t.Fatalf("MergeUpdate: %v", err)
			}
		}
	}

	got, ok, err := c.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get: %v, ok=%v", err, ok)
	}
	items, err := topk.Items(got)
	if err != nil {
		t.Fatalf("topk.Items: %v", err)
	}
	if len(items) == 0 || items[0].Key != "post-A" {
		t.Errorf("top item: got %v, want post-A first", items)
	}
}

func TestBytesCache_TTL(t *testing.T) {
	c := newBytesCache(t, fmt.Sprintf("test_ttl_%d", time.Now().UnixNano()))
	ctx := context.Background()
	key := state.Key{Entity: "fleeting", Bucket: 100}

	if err := c.MergeUpdate(ctx, key, hll.Single([]byte("u-1")), 1*time.Second); err != nil {
		t.Fatalf("MergeUpdate: %v", err)
	}
	if _, ok, _ := c.Get(ctx, key); !ok {
		t.Fatalf("immediate Get: missing")
	}
	time.Sleep(1500 * time.Millisecond)
	if _, ok, _ := c.Get(ctx, key); ok {
		t.Errorf("post-TTL Get: still present, expected eviction")
	}
}

func TestBytesCache_Repopulate(t *testing.T) {
	c := newBytesCache(t, fmt.Sprintf("test_repop_%d", time.Now().UnixNano()))
	ctx := context.Background()

	// Source-of-truth holds an HLL sketch with ~50 distinct items.
	mon := hll.HLL()
	srcSketch := mon.Identity()
	for i := 0; i < 50; i++ {
		srcSketch = mon.Combine(srcSketch, hll.Single([]byte(fmt.Sprintf("u-%d", i))))
	}
	src := &fakeBytesStore{m: map[state.Key][]byte{
		{Entity: "page-A"}: srcSketch,
	}}

	if err := c.Repopulate(ctx, src, []state.Key{{Entity: "page-A"}, {Entity: "missing"}}); err != nil {
		t.Fatalf("Repopulate: %v", err)
	}
	got, ok, err := c.Get(ctx, state.Key{Entity: "page-A"})
	if err != nil || !ok {
		t.Fatalf("Get after repopulate: err=%v ok=%v", err, ok)
	}
	count, err := hll.Estimate(got)
	if err != nil {
		t.Fatalf("hll.Estimate: %v", err)
	}
	if count < 45 || count > 55 {
		t.Errorf("repopulated HLL cardinality: got %d, want ~50", count)
	}
	if _, ok, _ := c.Get(ctx, state.Key{Entity: "missing"}); ok {
		t.Errorf("missing should remain absent in cache")
	}
}

func TestBytesCache_GetMany(t *testing.T) {
	c := newBytesCache(t, fmt.Sprintf("test_many_%d", time.Now().UnixNano()))
	ctx := context.Background()

	_ = c.MergeUpdate(ctx, state.Key{Entity: "a"}, hll.Single([]byte("u-1")), 0)
	_ = c.MergeUpdate(ctx, state.Key{Entity: "c"}, hll.Single([]byte("u-9")), 0)

	keys := []state.Key{{Entity: "a"}, {Entity: "missing"}, {Entity: "c"}}
	vals, oks, err := c.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	wantOK := []bool{true, false, true}
	for i := range keys {
		if oks[i] != wantOK[i] {
			t.Errorf("[%d]: got ok=%v, want %v", i, oks[i], wantOK[i])
		}
		if oks[i] && len(vals[i]) == 0 {
			t.Errorf("[%d]: present but empty bytes", i)
		}
	}
}

func TestBytesCache_RequiresMonoid(t *testing.T) {
	if _, err := mvalkey.NewBytesCache(mvalkey.BytesConfig{
		Address:   "localhost:6379",
		KeyPrefix: "test",
	}); err == nil {
		t.Error("expected error when Monoid is nil")
	}
}
