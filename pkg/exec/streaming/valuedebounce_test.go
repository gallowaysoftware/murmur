package streaming_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// valueDebounceSource emits a fixed slice of (key, value) records and
// closes. Mirrors debounceSource but exposed under a different name so
// the two test suites stay independent.
type valueDebounceSource struct {
	events []debounceEv
}

func (s *valueDebounceSource) Read(_ context.Context, out chan<- source.Record[debounceEv]) error {
	for i, e := range s.events {
		out <- source.Record[debounceEv]{
			EventID: fmt.Sprintf("vd-ev-%d", i),
			Value:   e,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*valueDebounceSource) Name() string { return "value-debounce-test" }
func (*valueDebounceSource) Close() error { return nil }

func TestValueDebounce_DropsSameKeyValuePairs(t *testing.T) {
	// 6 events on the same key with the same value. Without debounce a
	// Sum pipeline would land 6. With value-debounce, only the first
	// contributes — the rest carry the same value and are dropped at
	// the source pump. (Sum isn't a production-safe target for
	// debouncing; we use it here as a test instrument because the
	// resulting count is easy to verify.)
	events := []debounceEv{
		{key: "k", v: 42},
		{key: "k", v: 42},
		{key: "k", v: 42},
		{key: "k", v: 42},
		{key: "k", v: 42},
		{key: "k", v: 42},
	}
	src := &valueDebounceSource{events: events}
	store := newFanoutStore()

	pipe := pipeline.NewPipeline[debounceEv, int64]("vd-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(e debounceEv) int64 { return e.v }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe,
		streaming.WithValueDebounce(time.Second, 100),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := store.m[state.Key{Entity: "k"}]; got != 42 {
		t.Errorf("after value-debounce: got %d, want 42 (5 same-value records dropped)", got)
	}
}

func TestValueDebounce_AllowsValueChangesOnSameKey(t *testing.T) {
	// Same key, value increases each event. value-debounce should NOT
	// drop these because the value changed each time. Sum monoid
	// accumulates: 1+2+3+4+5 = 15.
	events := []debounceEv{
		{key: "rising", v: 1},
		{key: "rising", v: 2},
		{key: "rising", v: 3},
		{key: "rising", v: 4},
		{key: "rising", v: 5},
	}
	src := &valueDebounceSource{events: events}
	store := newFanoutStore()

	pipe := pipeline.NewPipeline[debounceEv, int64]("vd-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(e debounceEv) int64 { return e.v }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe,
		streaming.WithValueDebounce(time.Second, 100),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := store.m[state.Key{Entity: "rising"}]; got != 15 {
		t.Errorf("rising values bypass debounce: got %d, want 15", got)
	}
}

func TestValueDebounce_AllowsSameValueOnDifferentKeys(t *testing.T) {
	// Different keys can carry the same value without interference —
	// debounce is per-(key, value) tuple, not per-value.
	events := []debounceEv{
		{key: "a", v: 99},
		{key: "b", v: 99},
		{key: "c", v: 99},
		{key: "d", v: 99},
		{key: "e", v: 99},
	}
	src := &valueDebounceSource{events: events}
	store := newFanoutStore()

	pipe := pipeline.NewPipeline[debounceEv, int64]("vd-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(e debounceEv) int64 { return e.v }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe,
		streaming.WithValueDebounce(time.Second, 100),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if got := store.m[state.Key{Entity: k}]; got != 99 {
			t.Errorf("distinct-key key=%q: got %d, want 99", k, got)
		}
	}
}

func TestValueDebounce_RecordsSkipMetric(t *testing.T) {
	// Verify the value_debounce_skip metric counts dropped records.
	events := []debounceEv{
		{key: "k", v: 7},
		{key: "k", v: 7}, // dropped (same value)
		{key: "k", v: 8}, // passes (new value)
		{key: "k", v: 8}, // dropped (same value re-seen)
		{key: "k", v: 8}, // dropped
	}
	src := &valueDebounceSource{events: events}
	store := newFanoutStore()
	rec := metrics.NewInMemory()

	pipe := pipeline.NewPipeline[debounceEv, int64]("vd-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(e debounceEv) int64 { return e.v }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe,
		streaming.WithValueDebounce(time.Second, 100),
		streaming.WithMetrics(rec),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 3 records dropped (the duplicate-value ones); 2 processed
	// (v=7 first, v=8 first). Sum = 7 + 8 = 15.
	if got := store.m[state.Key{Entity: "k"}]; got != 15 {
		t.Errorf("processed sum: got %d, want 15", got)
	}
	if got := rec.SnapshotOne("vd-test:value_debounce_skip").EventsProcessed; got != 3 {
		t.Errorf("value_debounce_skip events: got %d, want 3", got)
	}
	if got := rec.SnapshotOne("vd-test").EventsProcessed; got != 2 {
		t.Errorf("processed events: got %d, want 2", got)
	}
}

func TestValueDebounce_WorksWithBatchWindow(t *testing.T) {
	// Compose: value-debounce drops same-(key,value) repeats; batch
	// window collapses any remaining writes per key. The result for
	// 50 same-(key,value) records is one Sum store-write of value=10
	// (the first record passes, the rest are debounced).
	events := make([]debounceEv, 50)
	for i := range events {
		events[i] = debounceEv{key: "burst", v: 10}
	}
	src := &valueDebounceSource{events: events}
	store := newCountingStore()

	pipe := pipeline.NewPipeline[debounceEv, int64]("vd-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(e debounceEv) int64 { return e.v }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe,
		streaming.WithValueDebounce(5*time.Second, 100),
		streaming.WithBatchWindow(50*time.Millisecond, 10000),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := store.values[state.Key{Entity: "burst"}]; got != 10 {
		t.Errorf("after debounce+batch-window: got %d, want 10 (49 same-value records dropped)", got)
	}
	if got := store.totalCalls.Load(); got > 5 {
		t.Errorf("store calls: got %d, want ≤5 (debounce + batch should collapse 50→1)", got)
	}
}
