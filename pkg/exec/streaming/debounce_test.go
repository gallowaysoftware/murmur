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

// debounceEv is the input for debounce tests — minimal shape.
type debounceEv struct {
	key string
	v   int64
}

// debounceSource emits a fixed slice of records and closes.
type debounceSource struct {
	events []debounceEv
}

func (s *debounceSource) Read(_ context.Context, out chan<- source.Record[debounceEv]) error {
	for i, e := range s.events {
		out <- source.Record[debounceEv]{
			EventID: fmt.Sprintf("ev-%d", i),
			Value:   e,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*debounceSource) Name() string { return "debounce-test" }
func (*debounceSource) Close() error { return nil }

func TestKeyDebounce_DropsDuplicateKeysWithinWindow(t *testing.T) {
	// 6 events, all on the same key with delta=1. Without debounce a
	// Sum pipeline would land 6. With debounce, only the first
	// contributes — the other 5 are dropped at the source pump before
	// the processor sees them. Sum monoid would normally accumulate;
	// debouncing CHANGES that. (Documentation calls this out — Sum is
	// NOT a safe target for debouncing in production. Here we use it
	// as a test instrument because the semantics are easy to count.)
	events := []debounceEv{
		{key: "hot", v: 1},
		{key: "hot", v: 1},
		{key: "hot", v: 1},
		{key: "hot", v: 1},
		{key: "hot", v: 1},
		{key: "hot", v: 1},
	}
	src := &debounceSource{events: events}
	store := newFanoutStore()

	pipe := pipeline.NewPipeline[debounceEv, int64]("debounce-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(e debounceEv) int64 { return e.v }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe,
		streaming.WithKeyDebounce(time.Second, 100),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := store.m[state.Key{Entity: "hot"}]; got != 1 {
		t.Errorf("debounce-protected hot key: got %d, want 1 (5 records debounced)", got)
	}
}

func TestKeyDebounce_AllowsDistinctKeys(t *testing.T) {
	// Distinct keys should pass through normally — debounce only
	// suppresses repeats of the SAME key.
	events := []debounceEv{
		{key: "a", v: 1},
		{key: "b", v: 1},
		{key: "c", v: 1},
		{key: "d", v: 1},
		{key: "e", v: 1},
	}
	src := &debounceSource{events: events}
	store := newFanoutStore()
	pipe := pipeline.NewPipeline[debounceEv, int64]("distinct").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(debounceEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := streaming.Run(ctx, pipe,
		streaming.WithKeyDebounce(time.Hour, 100),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if got := store.m[state.Key{Entity: k}]; got != 1 {
			t.Errorf("key %q: got %d, want 1 (distinct key, not debounced)", k, got)
		}
	}
}

func TestKeyDebounce_AllowsRepeatsAfterWindow(t *testing.T) {
	// First emit, wait > window, second emit. Second should NOT be
	// debounced.
	const window = 50 * time.Millisecond
	events := []debounceEv{
		{key: "k", v: 1},
		{key: "k", v: 1}, // very-close — debounced
	}
	src := &debounceSource{events: events}
	store := newFanoutStore()
	pipe := pipeline.NewPipeline[debounceEv, int64]("after-window").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(debounceEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First Run: 2 records, second is debounced → store = 1.
	if err := streaming.Run(ctx, pipe, streaming.WithKeyDebounce(window, 100)); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := store.m[state.Key{Entity: "k"}]; got != 1 {
		t.Errorf("after first run with debounce: got %d, want 1 (one of the two debounced)", got)
	}

	// Wait past the window then re-run with a fresh source.
	time.Sleep(window + 20*time.Millisecond)
	src2 := &debounceSource{events: []debounceEv{{key: "k", v: 1}}}
	pipe2 := pipeline.NewPipeline[debounceEv, int64]("after-window-2").
		From(src2).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(debounceEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	if err := streaming.Run(ctx, pipe2); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	// New Run = new debouncer; record lands. Store now has 2.
	if got := store.m[state.Key{Entity: "k"}]; got != 2 {
		t.Errorf("after second run: got %d, want 2", got)
	}
}

func TestKeyDebounce_RecordsSkipMetric(t *testing.T) {
	events := []debounceEv{
		{key: "k", v: 1},
		{key: "k", v: 1},
		{key: "k", v: 1},
		{key: "k", v: 1},
		{key: "k", v: 1},
	}
	src := &debounceSource{events: events}
	store := newFanoutStore()
	rec := metrics.NewInMemory()
	pipe := pipeline.NewPipeline[debounceEv, int64]("metric-test").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(debounceEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := streaming.Run(ctx, pipe,
		streaming.WithMetrics(rec),
		streaming.WithKeyDebounce(time.Hour, 100),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 1 processed, 4 debounce_skip.
	if got := rec.SnapshotOne("metric-test:debounce_skip").EventsProcessed; got != 4 {
		t.Errorf("debounce_skip events: got %d, want 4", got)
	}
	if got := rec.SnapshotOne("metric-test").EventsProcessed; got != 1 {
		t.Errorf("processed events: got %d, want 1", got)
	}
}

func TestKeyDebounce_WorksWithBatchWindow(t *testing.T) {
	// Compose: debounce drops same-key repeats; batch window collapses
	// remaining records per key. With debounce window > batch window,
	// most repeats are dropped at the source pump rather than coalesced
	// at the aggregator — saving processor work AND aggregator-lock
	// contention.
	events := make([]debounceEv, 50)
	for i := range events {
		events[i] = debounceEv{key: "hot", v: 1} // all same key
	}
	src := &debounceSource{events: events}
	store := newFanoutStore()
	pipe := pipeline.NewPipeline[debounceEv, int64]("compose").
		From(src).
		Key(func(e debounceEv) string { return e.key }).
		Value(func(debounceEv) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := streaming.Run(ctx, pipe,
		streaming.WithKeyDebounce(time.Hour, 100),
		streaming.WithBatchWindow(50*time.Millisecond, 1000),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// One record landed (the first); the other 49 were debounced.
	// Batch window had nothing to do — only one record reached the
	// aggregator.
	if got := store.m[state.Key{Entity: "hot"}]; got != 1 {
		t.Errorf("debounce + batch: got %d, want 1 (49 debounced before reaching the batch)", got)
	}
}
