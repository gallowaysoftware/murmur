package valkey_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/state"
	mvalkey "github.com/gallowaysoftware/murmur/pkg/state/valkey"
)

// fakeStore is a tiny in-memory Store[int64] used to exercise Repopulate without DDB.
type fakeStore struct{ m map[state.Key]int64 }

func (s *fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}
func (s *fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	if s.m == nil {
		s.m = map[state.Key]int64{}
	}
	s.m[k] += d
	return nil
}
func (s *fakeStore) Close() error { return nil }

func valkeyAddress(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("VALKEY_ADDRESS")
	if addr == "" {
		t.Skip("VALKEY_ADDRESS not set; run docker-compose up valkey and re-run")
	}
	return addr
}

func newCache(t *testing.T, prefix string) *mvalkey.Int64Cache {
	t.Helper()
	c, err := mvalkey.NewInt64Cache(mvalkey.Config{
		Address:   valkeyAddress(t),
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestInt64Cache_AtomicIncrAndGet(t *testing.T) {
	c := newCache(t, fmt.Sprintf("test_atomic_%d", time.Now().UnixNano()))
	ctx := context.Background()
	key := state.Key{Entity: "page-A"}

	// Initial Get: missing.
	if v, ok, err := c.Get(ctx, key); err != nil || ok || v != 0 {
		t.Fatalf("initial Get: got (%d,%v,%v), want (0,false,nil)", v, ok, err)
	}

	// MergeUpdate +5, +3, -2 → 6.
	for _, d := range []int64{5, 3, -2} {
		if err := c.MergeUpdate(ctx, key, d, 0); err != nil {
			t.Fatalf("MergeUpdate %d: %v", d, err)
		}
	}
	if v, ok, err := c.Get(ctx, key); err != nil || !ok || v != 6 {
		t.Fatalf("after sums Get: got (%d,%v,%v), want (6,true,nil)", v, ok, err)
	}
}

func TestInt64Cache_GetMany(t *testing.T) {
	c := newCache(t, fmt.Sprintf("test_many_%d", time.Now().UnixNano()))
	ctx := context.Background()

	for entity, count := range map[string]int64{"a": 1, "b": 2, "c": 3} {
		_ = c.MergeUpdate(ctx, state.Key{Entity: entity}, count, 0)
	}
	keys := []state.Key{
		{Entity: "a"},
		{Entity: "b"},
		{Entity: "missing"},
		{Entity: "c"},
	}
	vals, oks, err := c.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	wantVals := []int64{1, 2, 0, 3}
	wantOK := []bool{true, true, false, true}
	for i := range keys {
		if vals[i] != wantVals[i] || oks[i] != wantOK[i] {
			t.Errorf("[%d]: got (%d,%v), want (%d,%v)", i, vals[i], oks[i], wantVals[i], wantOK[i])
		}
	}
}

func TestInt64Cache_TTL(t *testing.T) {
	c := newCache(t, fmt.Sprintf("test_ttl_%d", time.Now().UnixNano()))
	ctx := context.Background()
	key := state.Key{Entity: "fleeting", Bucket: 100}

	if err := c.MergeUpdate(ctx, key, 7, 1*time.Second); err != nil {
		t.Fatalf("MergeUpdate: %v", err)
	}
	if v, ok, _ := c.Get(ctx, key); !ok || v != 7 {
		t.Fatalf("immediate Get: got (%d, %v), want (7, true)", v, ok)
	}
	// Wait past TTL.
	time.Sleep(1500 * time.Millisecond)
	if v, ok, _ := c.Get(ctx, key); ok || v != 0 {
		t.Errorf("post-TTL Get: got (%d, %v), want (0, false)", v, ok)
	}
}

func TestInt64Cache_Repopulate(t *testing.T) {
	c := newCache(t, fmt.Sprintf("test_repop_%d", time.Now().UnixNano()))
	ctx := context.Background()

	// Source-of-truth has data the cache doesn't.
	src := &fakeStore{m: map[state.Key]int64{
		{Entity: "a"}: 100,
		{Entity: "b"}: 200,
	}}

	if err := c.Repopulate(ctx, src, []state.Key{{Entity: "a"}, {Entity: "b"}, {Entity: "missing-in-src"}}); err != nil {
		t.Fatalf("Repopulate: %v", err)
	}
	if v, ok, _ := c.Get(ctx, state.Key{Entity: "a"}); !ok || v != 100 {
		t.Errorf("a after repopulate: got (%d,%v), want (100,true)", v, ok)
	}
	if v, ok, _ := c.Get(ctx, state.Key{Entity: "b"}); !ok || v != 200 {
		t.Errorf("b after repopulate: got (%d,%v), want (200,true)", v, ok)
	}
	if _, ok, _ := c.Get(ctx, state.Key{Entity: "missing-in-src"}); ok {
		t.Errorf("missing-in-src should remain absent in cache")
	}
}
