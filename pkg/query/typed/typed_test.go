package typed_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/monoid"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
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
func (s fakeBytesStore) GetMany(_ context.Context, ks []state.Key) ([][]byte, []bool, error) {
	vs := make([][]byte, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s[k]
	}
	return vs, oks, nil
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

func TestHLLClient_GetWindowMany(t *testing.T) {
	mon := hll.HLL()
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeBytesStore{}

	// Two entities × 5 daily buckets, each bucket holds a single
	// sketch with N distinct elements. post-A has 10 unique per
	// bucket → 50 across 5 days; post-B has 30 unique per bucket → 150.
	for i := 0; i < 5; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		sa := mon.Identity()
		for j := 0; j < 10; j++ {
			sa = mon.Combine(sa, hll.Single([]byte{byte(i*100 + j)}))
		}
		store[state.Key{Entity: "post-A", Bucket: bucket}] = sa

		sb := mon.Identity()
		for j := 0; j < 30; j++ {
			sb = mon.Combine(sb, hll.Single([]byte{byte(i*100 + j + 100)}))
		}
		store[state.Key{Entity: "post-B", Bucket: bucket}] = sb
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Window: &w, Encode: grpc.BytesIdentity(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewHLLClient(rawClient)
	got, err := c.GetWindowMany(context.Background(),
		[]string{"post-A", "post-B", "missing"}, 5*24*time.Hour)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(got): got %d, want 3", len(got))
	}
	// HLL ~1-2% error; 50 distinct → ±5; 150 distinct → ±10.
	if got[0].Cardinality < 45 || got[0].Cardinality > 55 {
		t.Errorf("post-A cardinality: got %d, want ~50", got[0].Cardinality)
	}
	if got[1].Cardinality < 140 || got[1].Cardinality > 160 {
		t.Errorf("post-B cardinality: got %d, want ~150", got[1].Cardinality)
	}
	if got[2].Cardinality != 0 {
		t.Errorf("missing entity: got %d, want 0", got[2].Cardinality)
	}
}

func TestTopKClient_GetWindowMany(t *testing.T) {
	mon := topk.New(5)
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeBytesStore{}

	// Per entity, one bucket with a known TopK distribution.
	bucket := w.BucketID(now.Add(-1 * time.Hour))
	for entity, hits := range map[string]map[string]int{
		"global-A": {"post-X": 10, "post-Y": 5, "post-Z": 2},
		"global-B": {"post-Q": 7, "post-R": 1},
	} {
		sketch := mon.Identity()
		for k, n := range hits {
			for i := 0; i < n; i++ {
				sketch = mon.Combine(sketch, topk.SingleN(5, k, 1))
			}
		}
		store[state.Key{Entity: entity, Bucket: bucket}] = sketch
	}

	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Window: &w, Encode: grpc.BytesIdentity(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewTopKClient(rawClient)
	got, err := c.GetWindowMany(context.Background(),
		[]string{"global-A", "global-B", "missing"}, 24*time.Hour)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(got): got %d, want 3", len(got))
	}
	if len(got[0]) == 0 || got[0][0].Key != "post-X" {
		t.Errorf("global-A top item: got %+v, want post-X first", got[0])
	}
	if len(got[1]) == 0 || got[1][0].Key != "post-Q" {
		t.Errorf("global-B top item: got %+v, want post-Q first", got[1])
	}
	// Missing entity yields the merged-empty-sketch's items list, which
	// is empty (no items stored, no items returned).
	if len(got[2]) != 0 {
		t.Errorf("missing entity: got %+v, want empty", got[2])
	}
}

// Test helpers for Bloom — small wrappers that hide the package-name
// shadowing between `bloom` (imported) and `bloom` the local helper.
func bloomMon() monoid.Monoid[[]byte] { return bloom.Bloom() }
func bloomSingle(b byte, salt string) []byte {
	return bloom.Single(append([]byte(salt), b))
}

func TestBloomClient_GetWindowMany(t *testing.T) {
	mon := bloomMon()
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeBytesStore{}

	bucket := w.BucketID(now.Add(-1 * time.Hour))
	// Build per-entity Bloom filters with known populated counts.
	for entity, n := range map[string]int{"campaign-A": 50, "campaign-B": 200} {
		filter := mon.Identity()
		for i := 0; i < n; i++ {
			filter = mon.Combine(filter, bloomSingle(byte(i), entity))
		}
		store[state.Key{Entity: entity, Bucket: bucket}] = filter
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Window: &w, Encode: grpc.BytesIdentity(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewBloomClient(rawClient)
	got, err := c.GetWindowMany(context.Background(),
		[]string{"campaign-A", "campaign-B", "missing"}, 24*time.Hour)
	if err != nil {
		t.Fatalf("GetWindowMany: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(got): got %d, want 3", len(got))
	}
	// CapacityBits and HashFunctions are filter-construction-stable.
	if got[0].CapacityBits == 0 {
		t.Errorf("campaign-A capacity_bits: got 0; want > 0")
	}
	if got[1].CapacityBits == 0 {
		t.Errorf("campaign-B capacity_bits: got 0; want > 0")
	}
	// Missing entity yields the empty Bloom's structural shape:
	// capacity_bits and hash_functions are stable filter-construction
	// constants (returned from the merged-empty sketch's metadata),
	// only ApproxSize should be 0 (no populated bits).
	if got[2].ApproxSize != 0 {
		t.Errorf("missing: got approx_size=%d, want 0", got[2].ApproxSize)
	}
}

func TestHLLClient_GetMany(t *testing.T) {
	mon := hll.HLL()
	store := fakeBytesStore{}
	for entity, n := range map[string]int{"page-A": 20, "page-C": 60} {
		sketch := mon.Identity()
		for i := 0; i < n; i++ {
			sketch = mon.Combine(sketch, hll.Single([]byte(entity+":"+string(rune(i)))))
		}
		store[state.Key{Entity: entity}] = sketch
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Encode: grpc.BytesIdentity(),
	})
	defer cleanup()

	c := typed.NewHLLClient(rawClient)
	got, present, err := c.GetMany(context.Background(), []string{"page-A", "missing", "page-C"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if !present[0] || got[0].Cardinality < 18 || got[0].Cardinality > 22 {
		t.Errorf("page-A: present=%v card=%d, want present=true card≈20", present[0], got[0].Cardinality)
	}
	if present[1] || got[1].Cardinality != 0 {
		t.Errorf("missing: present=%v card=%d, want present=false card=0", present[1], got[1].Cardinality)
	}
	if !present[2] || got[2].Cardinality < 55 || got[2].Cardinality > 65 {
		t.Errorf("page-C: present=%v card=%d, want present=true card≈60", present[2], got[2].Cardinality)
	}
}

func TestHLLClient_GetRange(t *testing.T) {
	mon := hll.HLL()
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeBytesStore{}

	// 10 daily buckets, each with 100 distinct elements; the per-bucket
	// elements partially overlap (shift of 50 across buckets) so the
	// merged cardinality across all 10 buckets is 100 + 9*50 = 550.
	encode := func(n int) []byte {
		return []byte{byte(n & 0xff), byte((n >> 8) & 0xff)}
	}
	for i := 0; i < 10; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		sketch := mon.Identity()
		for j := 0; j < 100; j++ {
			sketch = mon.Combine(sketch, hll.Single(encode(i*50+j)))
		}
		store[state.Key{Entity: "page-A", Bucket: bucket}] = sketch
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Window: &w, Encode: grpc.BytesIdentity(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewHLLClient(rawClient)
	// Range covers all 10 buckets.
	got, err := c.GetRange(context.Background(), "page-A",
		now.Add(-10*24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	// HLL ~1-2% error at 550 elements → ±15.
	if got.Cardinality < 500 || got.Cardinality > 600 {
		t.Errorf("GetRange cardinality: got %d, want ~550 ±50", got.Cardinality)
	}
}

func TestTopKClient_GetMany(t *testing.T) {
	mon := topk.New(5)
	store := fakeBytesStore{}
	for entity, hits := range map[string]map[string]int{
		"feed-A": {"post-X": 10, "post-Y": 3},
		"feed-B": {"post-Z": 7},
	} {
		sketch := mon.Identity()
		for k, n := range hits {
			for i := 0; i < n; i++ {
				sketch = mon.Combine(sketch, topk.SingleN(5, k, 1))
			}
		}
		store[state.Key{Entity: entity}] = sketch
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Encode: grpc.BytesIdentity(),
	})
	defer cleanup()

	c := typed.NewTopKClient(rawClient)
	got, present, err := c.GetMany(context.Background(), []string{"feed-A", "missing", "feed-B"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if !present[0] || len(got[0]) == 0 || got[0][0].Key != "post-X" {
		t.Errorf("feed-A: present=%v top=%+v, want present=true top=post-X", present[0], got[0])
	}
	if present[1] || got[1] != nil {
		t.Errorf("missing: present=%v got=%+v, want present=false got=nil", present[1], got[1])
	}
	if !present[2] || len(got[2]) == 0 || got[2][0].Key != "post-Z" {
		t.Errorf("feed-B: present=%v top=%+v, want present=true top=post-Z", present[2], got[2])
	}
}

func TestTopKClient_GetRange(t *testing.T) {
	mon := topk.New(5)
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeBytesStore{}

	// 3 daily buckets, post-X dominant across all of them.
	for i := 0; i < 3; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		sketch := mon.Identity()
		for j := 0; j < 10; j++ {
			sketch = mon.Combine(sketch, topk.SingleN(5, "post-X", 1))
		}
		for j := 0; j < 2; j++ {
			sketch = mon.Combine(sketch, topk.SingleN(5, "post-Y", 1))
		}
		store[state.Key{Entity: "feed-A", Bucket: bucket}] = sketch
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Window: &w, Encode: grpc.BytesIdentity(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewTopKClient(rawClient)
	got, err := c.GetRange(context.Background(), "feed-A",
		now.Add(-3*24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if len(got) == 0 || got[0].Key != "post-X" {
		t.Errorf("GetRange top: got %+v, want post-X first", got)
	}
}

func TestBloomClient_GetMany(t *testing.T) {
	mon := bloomMon()
	store := fakeBytesStore{}
	for entity, n := range map[string]int{"campaign-A": 30, "campaign-B": 100} {
		filter := mon.Identity()
		for i := 0; i < n; i++ {
			filter = mon.Combine(filter, bloomSingle(byte(i), entity))
		}
		store[state.Key{Entity: entity}] = filter
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Encode: grpc.BytesIdentity(),
	})
	defer cleanup()

	c := typed.NewBloomClient(rawClient)
	got, present, err := c.GetMany(context.Background(), []string{"campaign-A", "missing", "campaign-B"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if !present[0] || got[0].CapacityBits == 0 {
		t.Errorf("campaign-A: present=%v cap=%d, want present=true cap>0", present[0], got[0].CapacityBits)
	}
	if present[1] || got[1].ApproxSize != 0 {
		t.Errorf("missing: present=%v approx=%d, want present=false approx=0", present[1], got[1].ApproxSize)
	}
	if !present[2] || got[2].CapacityBits == 0 {
		t.Errorf("campaign-B: present=%v cap=%d, want present=true cap>0", present[2], got[2].CapacityBits)
	}
}

func TestBloomClient_GetRange(t *testing.T) {
	mon := bloomMon()
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)
	store := fakeBytesStore{}

	for i := 0; i < 3; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		filter := mon.Identity()
		for j := 0; j < 25; j++ {
			filter = mon.Combine(filter, bloomSingle(byte(i*100+j), "campaign-A"))
		}
		store[state.Key{Entity: "campaign-A", Bucket: bucket}] = filter
	}
	rawClient, cleanup := startServer(t, grpc.Config[[]byte]{
		Store: store, Monoid: mon, Window: &w, Encode: grpc.BytesIdentity(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	c := typed.NewBloomClient(rawClient)
	got, err := c.GetRange(context.Background(), "campaign-A",
		now.Add(-3*24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if got.CapacityBits == 0 {
		t.Errorf("GetRange: capacity_bits=0, want > 0 from merged filter")
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
