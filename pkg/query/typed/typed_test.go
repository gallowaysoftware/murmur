package typed_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/hll"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/topk"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/query/typed"
	"github.com/gallowaysoftware/murmur/pkg/state"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// fakeStore is the minimal int64 store used for the Sum tests.
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

// fakeBytesStore for the HLL/TopK tests.
type fakeBytesStore map[state.Key][]byte

func (s fakeBytesStore) Get(_ context.Context, k state.Key) ([]byte, bool, error) {
	v, ok := s[k]
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}
func (s fakeBytesStore) GetMany(context.Context, []state.Key) ([][]byte, []bool, error) {
	return nil, nil, nil
}
func (s fakeBytesStore) MergeUpdate(_ context.Context, k state.Key, d []byte, _ time.Duration) error {
	s[k] = d
	return nil
}
func (fakeBytesStore) Close() error { return nil }

// startServer is a small helper that wires a Murmur Connect QueryService
// against an httptest.Server for the typed-client tests.
func startServer[V any](t *testing.T, cfg grpc.Config[V]) (murmurv1connect.QueryServiceClient, func()) {
	t.Helper()
	srv := grpc.NewServer(cfg)
	mux := http.NewServeMux()
	mux.Handle(srv.Handler())
	httpSrv := httptest.NewServer(mux)
	client := murmurv1connect.NewQueryServiceClient(httpSrv.Client(), httpSrv.URL)
	return client, httpSrv.Close
}

func TestSumClient_Get(t *testing.T) {
	store := fakeStore{
		state.Key{Entity: "a"}: 42,
		state.Key{Entity: "b"}: 100,
	}
	rawClient, cleanup := startServer(t, grpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Encode: grpc.Int64LE(),
	})
	defer cleanup()

	c := typed.NewSumClient(rawClient)

	v, ok, err := c.Get(context.Background(), "a")
	if err != nil || !ok || v != 42 {
		t.Errorf("Get a: got (%d,%v,%v), want (42, true, nil)", v, ok, err)
	}
	v, ok, err = c.Get(context.Background(), "missing")
	if err != nil || ok || v != 0 {
		t.Errorf("Get missing: got (%d,%v,%v), want (0, false, nil)", v, ok, err)
	}
}

func TestSumClient_GetMany(t *testing.T) {
	store := fakeStore{
		state.Key{Entity: "a"}: 1,
		state.Key{Entity: "c"}: 3,
	}
	rawClient, cleanup := startServer(t, grpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Encode: grpc.Int64LE(),
	})
	defer cleanup()

	c := typed.NewSumClient(rawClient)
	vals, present, err := c.GetMany(context.Background(), []string{"a", "missing", "c"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if vals[0] != 1 || !present[0] {
		t.Errorf("[0] a: got (%d,%v), want (1,true)", vals[0], present[0])
	}
	if present[1] {
		t.Errorf("[1] missing: present, want absent")
	}
	if vals[2] != 3 || !present[2] {
		t.Errorf("[2] c: got (%d,%v), want (3,true)", vals[2], present[2])
	}
}

func TestSumClient_GetWindow(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeStore{}
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		store[state.Key{Entity: "page-A", Bucket: bucket}] = int64(i + 1)
	}
	rawClient, cleanup := startServer(t, grpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: grpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewSumClient(rawClient)
	got, err := c.GetWindow(context.Background(), "page-A", 7*24*time.Hour)
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	// 1+2+3+4+5+6+7 = 28
	if got != 28 {
		t.Errorf("Last7Days: got %d, want 28", got)
	}
}

func TestSumClient_GetWindowMany(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeStore{}
	for i := 0; i < 5; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		store[state.Key{Entity: "post-A", Bucket: bucket}] = int64(i + 1)
		store[state.Key{Entity: "post-B", Bucket: bucket}] = int64((i + 1) * 10)
	}
	rawClient, cleanup := startServer(t, grpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: grpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewSumClient(rawClient)
	got, err := c.GetWindowMany(context.Background(),
		[]string{"post-A", "post-B"}, 5*24*time.Hour)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	if got[0] != 15 {
		t.Errorf("post-A: got %d, want 15", got[0])
	}
	if got[1] != 150 {
		t.Errorf("post-B: got %d, want 150", got[1])
	}
}

func TestHLLClient_Get(t *testing.T) {
	mon := hll.HLL()
	store := fakeBytesStore{}
	// Populate with 50 distinct elements.
	sketch := mon.Identity()
	for i := 0; i < 50; i++ {
		sketch = mon.Combine(sketch, hll.Single([]byte{byte(i), byte(i / 256)}))
	}
	store[state.Key{Entity: "page-A"}] = sketch

	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Encode: grpc.BytesIdentity(),
	})
	defer cleanup()

	c := typed.NewHLLClient(rawClient)
	got, ok, err := c.Get(context.Background(), "page-A")
	if err != nil || !ok {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	// HLL has ~1-2% error; 50 distinct should land in [47, 53].
	if got.Cardinality < 45 || got.Cardinality > 55 {
		t.Errorf("cardinality: got %d, want ~50", got.Cardinality)
	}
	if got.ByteLen != len(sketch) {
		t.Errorf("byte_len: got %d, want %d", got.ByteLen, len(sketch))
	}
}

func TestTopKClient_Get(t *testing.T) {
	mon := topk.New(5)
	store := fakeBytesStore{}
	sketch := mon.Identity()
	hits := map[string]int{"post-A": 10, "post-B": 5, "post-C": 1}
	for k, n := range hits {
		for i := 0; i < n; i++ {
			sketch = mon.Combine(sketch, topk.SingleN(5, k, 1))
		}
	}
	store[state.Key{Entity: "global"}] = sketch

	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Encode: grpc.BytesIdentity(),
	})
	defer cleanup()

	c := typed.NewTopKClient(rawClient)
	items, ok, err := c.Get(context.Background(), "global")
	if err != nil || !ok {
		t.Fatalf("Get: err=%v ok=%v", err, ok)
	}
	if len(items) == 0 || items[0].Key != "post-A" {
		t.Errorf("top item: got %+v, want post-A first", items)
	}
}

func TestSumClient_FreshReadOption(t *testing.T) {
	// Smoke test: WithFreshRead is plumbed through the typed client to
	// the underlying request. The bypass-coalescing semantics are
	// verified at the gRPC layer; here we just confirm no error.
	store := fakeStore{state.Key{Entity: "a"}: 1}
	rawClient, cleanup := startServer(t, grpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Encode: grpc.Int64LE(),
	})
	defer cleanup()

	c := typed.NewSumClient(rawClient)
	v, ok, err := c.Get(context.Background(), "a", typed.WithFreshRead())
	if err != nil || !ok || v != 1 {
		t.Errorf("WithFreshRead: got (%d,%v,%v), want (1,true,nil)", v, ok, err)
	}
}

func TestDecodeInt64_ShortInput(t *testing.T) {
	if got := typed.DecodeInt64(nil); got != 0 {
		t.Errorf("nil input: got %d, want 0", got)
	}
	if got := typed.DecodeInt64([]byte{1, 2, 3}); got != 0 {
		t.Errorf("short input: got %d, want 0", got)
	}
}
