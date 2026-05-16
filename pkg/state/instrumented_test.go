package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeStore is the test double — minimal Store[int64] with controllable
// errors so we can verify the instrumented wrapper records both success
// and failure paths.
type fakeStore struct {
	m   map[state.Key]int64
	err error
}

func (s *fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	if s.err != nil {
		return 0, false, s.err
	}
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}
func (s *fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	if s.err != nil {
		return s.err
	}
	if s.m == nil {
		s.m = map[state.Key]int64{}
	}
	s.m[k] += d
	return nil
}
func (*fakeStore) Close() error { return nil }

func TestInstrumented_NilRecorderPassthrough(t *testing.T) {
	inner := &fakeStore{m: map[state.Key]int64{{Entity: "x"}: 42}}
	wrapped := state.NewInstrumented[int64](inner, nil, "test")
	// nil recorder → wrapper returns the inner store directly. Same
	// pointer identity isn't guaranteed by the API, but behavior must
	// match.
	v, ok, err := wrapped.Get(context.Background(), state.Key{Entity: "x"})
	if err != nil || !ok || v != 42 {
		t.Errorf("Get: got (%d, %v, %v), want (42, true, nil)", v, ok, err)
	}
}

func TestInstrumented_RecordsGetLatency(t *testing.T) {
	inner := &fakeStore{m: map[state.Key]int64{{Entity: "x"}: 42}}
	rec := metrics.NewInMemory()
	wrapped := state.NewInstrumented[int64](inner, rec, "page_views")

	for i := 0; i < 5; i++ {
		_, _, _ = wrapped.Get(context.Background(), state.Key{Entity: "x"})
	}
	snap := rec.SnapshotOne("page_views")
	if got := snap.Latencies["store_get"].N; got != 5 {
		t.Errorf("store_get samples: got %d, want 5", got)
	}
}

func TestInstrumented_RecordsGetManyLatency(t *testing.T) {
	inner := &fakeStore{m: map[state.Key]int64{{Entity: "x"}: 1}}
	rec := metrics.NewInMemory()
	wrapped := state.NewInstrumented[int64](inner, rec, "p")

	_, _, _ = wrapped.GetMany(context.Background(), []state.Key{{Entity: "x"}, {Entity: "y"}})
	if got := rec.SnapshotOne("p").Latencies["store_get_many"].N; got != 1 {
		t.Errorf("store_get_many samples: got %d, want 1", got)
	}
}

func TestInstrumented_RecordsMergeLatency(t *testing.T) {
	inner := &fakeStore{}
	rec := metrics.NewInMemory()
	wrapped := state.NewInstrumented[int64](inner, rec, "p")

	for i := 0; i < 3; i++ {
		_ = wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x"}, int64(i), 0)
	}
	if got := rec.SnapshotOne("p").Latencies["store_merge_update"].N; got != 3 {
		t.Errorf("store_merge_update samples: got %d, want 3", got)
	}
}

func TestInstrumented_RecordsErrors(t *testing.T) {
	rec := metrics.NewInMemory()
	wrapped := state.NewInstrumented[int64](&fakeStore{err: errors.New("ddb down")}, rec, "p")

	_, _, _ = wrapped.Get(context.Background(), state.Key{Entity: "x"})
	_, _, _ = wrapped.GetMany(context.Background(), []state.Key{{Entity: "x"}})
	_ = wrapped.MergeUpdate(context.Background(), state.Key{Entity: "x"}, 1, 0)

	snap := rec.SnapshotOne("p")
	if snap.Errors != 3 {
		t.Errorf("errors: got %d, want 3 (one per failed op)", snap.Errors)
	}
}

// fakeCache extends fakeStore with Repopulate.
type fakeCache struct {
	*fakeStore
	repopulated atomicSet
}

type atomicSet struct{ keys []state.Key }

func (c *fakeCache) Repopulate(_ context.Context, _ state.Store[int64], keys []state.Key) error {
	c.repopulated.keys = append(c.repopulated.keys, keys...)
	return nil
}

func TestInstrumentedCache_RecordsRepopulateLatency(t *testing.T) {
	rec := metrics.NewInMemory()
	cache := &fakeCache{fakeStore: &fakeStore{}}
	wrapped := state.NewInstrumentedCache[int64](cache, rec, "cache")

	if err := wrapped.Repopulate(context.Background(), nil, []state.Key{{Entity: "a"}}); err != nil {
		t.Fatalf("Repopulate: %v", err)
	}
	if got := rec.SnapshotOne("cache").Latencies["cache_repopulate"].N; got != 1 {
		t.Errorf("cache_repopulate samples: got %d, want 1", got)
	}
	if len(cache.repopulated.keys) != 1 {
		t.Errorf("inner Repopulate not called")
	}
}

func TestInstrumented_Close_PropagatesErrors(t *testing.T) {
	inner := &fakeStore{}
	rec := metrics.NewInMemory()
	wrapped := state.NewInstrumented[int64](inner, rec, "p")
	if err := wrapped.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
