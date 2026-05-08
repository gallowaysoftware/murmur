package streaming_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// concurrentSource emits N records spread across `keys` distinct entity
// keys, with EventID numbering preserved so order verification is
// possible.
type concurrentSource struct {
	n    int
	keys []string
}

type concurrentEv struct {
	key string
	idx int
}

func (s *concurrentSource) Read(_ context.Context, out chan<- source.Record[concurrentEv]) error {
	for i := 0; i < s.n; i++ {
		k := s.keys[i%len(s.keys)]
		out <- source.Record[concurrentEv]{
			EventID: fmt.Sprintf("ev-%d", i),
			Value:   concurrentEv{key: k, idx: i},
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*concurrentSource) Name() string { return "concurrent-test" }
func (*concurrentSource) Close() error { return nil }

func TestConcurrency_PreservesPerKeyTotals(t *testing.T) {
	// 1000 events across 10 keys, processed by 4 worker goroutines.
	// Each key should accumulate exactly 100 (1000/10) regardless of
	// worker count — concurrency doesn't change correctness, just
	// throughput.
	const N = 1000
	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"}
	src := &concurrentSource{n: N, keys: keys}

	store := newFanoutStore()
	pipe := pipeline.NewPipeline[concurrentEv, int64]("conc-test").
		From(src).
		Key(func(e concurrentEv) string { return e.key }).
		Value(func(concurrentEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe, streaming.WithConcurrency(4)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, k := range keys {
		if got := store.m[state.Key{Entity: k}]; got != int64(N/len(keys)) {
			t.Errorf("key %s: got %d, want %d", k, got, N/len(keys))
		}
	}
}

// orderTrackingStore tracks the order of MergeUpdate calls per key, so
// we can verify that same-key records always serialize through the
// same worker (ordering preserved within a key, despite concurrent
// workers).
type orderTrackingStore struct {
	mu     atomic.Pointer[map[state.Key][]int64]
	orders chan keyEvent
}

type keyEvent struct {
	key state.Key
	val int64
}

func newOrderStore() *orderTrackingStore {
	o := &orderTrackingStore{
		orders: make(chan keyEvent, 10000),
	}
	m := make(map[state.Key][]int64)
	o.mu.Store(&m)
	return o
}

func (s *orderTrackingStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}
func (s *orderTrackingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *orderTrackingStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.orders <- keyEvent{key: k, val: d}
	return nil
}
func (*orderTrackingStore) Close() error { return nil }

func TestConcurrency_PerKeyOrderingPreserved(t *testing.T) {
	// Send a mono-increasing idx per key. With concurrency=4 and routing
	// by hash(key), same-key records always go to the same worker.
	// Since each worker is single-goroutine, MergeUpdates for one key
	// arrive in source order.
	const N = 200
	keys := []string{"k-A", "k-B", "k-C", "k-D"}

	store := newOrderStore()
	src := &orderedSource{n: N, keys: keys}
	pipe := pipeline.NewPipeline[idxEv, int64]("ordering").
		From(src).
		Key(func(e idxEv) string { return e.key }).
		Value(func(e idxEv) int64 { return e.idx }).
		Aggregate(core.Sum[int64]()). // Sum not order-preserving — we
		// observe the per-key MergeUpdate sequence directly via store.orders.
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe, streaming.WithConcurrency(4)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(store.orders)

	// Walk the recorded MergeUpdate sequence; for each key, the values
	// should be strictly increasing (matching source order).
	perKey := map[state.Key][]int64{}
	for ev := range store.orders {
		perKey[ev.key] = append(perKey[ev.key], ev.val)
	}
	for k, seq := range perKey {
		for i := 1; i < len(seq); i++ {
			if seq[i] <= seq[i-1] {
				t.Errorf("key %s: out-of-order at idx %d: prev=%d cur=%d", k.Entity, i, seq[i-1], seq[i])
				break
			}
		}
	}
	if len(perKey) != len(keys) {
		t.Errorf("got %d distinct keys, want %d", len(perKey), len(keys))
	}
}

type idxEv struct {
	key string
	idx int64
}

type orderedSource struct {
	n    int
	keys []string
}

func (s *orderedSource) Read(_ context.Context, out chan<- source.Record[idxEv]) error {
	for i := 0; i < s.n; i++ {
		k := s.keys[i%len(s.keys)]
		out <- source.Record[idxEv]{
			EventID: fmt.Sprintf("ev-%d", i),
			Value:   idxEv{key: k, idx: int64(i + 1)},
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*orderedSource) Name() string { return "ordered" }
func (*orderedSource) Close() error { return nil }

func TestConcurrency_DefaultIsOne(t *testing.T) {
	// Without WithConcurrency, the runtime is single-goroutine — same as
	// the historical behavior. Validates the default path stays
	// unchanged.
	const N = 100
	src := &concurrentSource{n: N, keys: []string{"k0", "k1"}}
	store := newFanoutStore()
	pipe := pipeline.NewPipeline[concurrentEv, int64]("default-conc").
		From(src).
		Key(func(e concurrentEv) string { return e.key }).
		Value(func(concurrentEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := streaming.Run(ctx, pipe); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, k := range []string{"k0", "k1"} {
		if got := store.m[state.Key{Entity: k}]; got != 50 {
			t.Errorf("default: key %s got %d, want 50", k, got)
		}
	}
}
