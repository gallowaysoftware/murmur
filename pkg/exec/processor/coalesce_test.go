package processor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// countingStore tracks both the running combined value AND how many times MergeUpdate
// was called. The latter is the throughput-win observable: with N events folded into
// K unique keys, naive processing makes N calls; coalesced processing makes K.
type countingStore struct {
	mu    sync.Mutex
	m     map[state.Key]int64
	calls int
}

func newCountingStore() *countingStore {
	return &countingStore{m: map[state.Key]int64{}}
}

func (s *countingStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}

func (s *countingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (s *countingStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.m[k] += d
	return nil
}

func (s *countingStore) Close() error { return nil }

// failingStore returns errFail on every MergeUpdate. Used to verify that flush
// surfaces failures with per-key contributing-EventID metadata.
type failingStore struct {
	calls int
}

var errFail = errors.New("store down")

func (s *failingStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}

func (s *failingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (s *failingStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	s.calls++
	return errFail
}

func (s *failingStore) Close() error { return nil }

func TestCoalescer_AdditiveCollapsesPerEvent(t *testing.T) {
	// 1000 events, 10 unique keys → 10 MergeUpdate calls, not 1000.
	store := newCountingStore()
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, nil)

	if !c.IsAdditive() {
		t.Fatal("expected additive monoid; got non-additive")
	}

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key := []string{intToKey(i % 10)}
		if err := c.AddMany(ctx, intToKey(i), time.Now(), key, int64(1)); err != nil {
			t.Fatalf("AddMany %d: %v", i, err)
		}
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := store.calls; got != 10 {
		t.Errorf("MergeUpdate calls: got %d, want 10 (one per unique key)", got)
	}
	for i := 0; i < 10; i++ {
		if got, want := store.m[state.Key{Entity: intToKey(i)}], int64(100); got != want {
			t.Errorf("key %q final value: got %d, want %d", intToKey(i), got, want)
		}
	}
}

func TestCoalescer_MatchesNaivePerEventTotals(t *testing.T) {
	// Sanity check: feed the same 500-event stream through naive MergeOne and
	// through the Coalescer; the final per-key totals must match exactly.
	naiveStore := newCountingStore()
	coalStore := newCountingStore()
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	ctx := context.Background()
	type ev struct {
		id  string
		key string
		v   int64
	}
	events := make([]ev, 500)
	for i := range events {
		events[i] = ev{id: intToKey(i), key: intToKey(i % 7), v: int64(i%5 + 1)}
	}

	// Naive path.
	for _, e := range events {
		err := processor.MergeMany(ctx, &cfg, "naive", e.id, time.Now(),
			[]string{e.key}, e.v, state.Store[int64](naiveStore), nil, nil)
		if err != nil {
			t.Fatalf("naive MergeMany: %v", err)
		}
	}

	// Coalesced path.
	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "coal",
		core.Sum[int64](), state.Store[int64](coalStore), nil, nil)
	for _, e := range events {
		if err := c.AddMany(ctx, e.id, time.Now(), []string{e.key}, e.v); err != nil {
			t.Fatalf("AddMany: %v", err)
		}
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Totals must agree.
	for i := 0; i < 7; i++ {
		k := state.Key{Entity: intToKey(i)}
		if naiveStore.m[k] != coalStore.m[k] {
			t.Errorf("key %q: naive=%d coalesced=%d", intToKey(i),
				naiveStore.m[k], coalStore.m[k])
		}
	}
	// Call-count savings: naive=500, coalesced=7.
	if coalStore.calls != 7 {
		t.Errorf("coalesced calls: got %d, want 7", coalStore.calls)
	}
	if naiveStore.calls != 500 {
		t.Errorf("naive calls: got %d, want 500", naiveStore.calls)
	}
}

func TestCoalescer_NonAdditiveFallsThrough(t *testing.T) {
	// A KindMin monoid is not additive — coalescer must route every event
	// straight to MergeMany so per-event semantics are preserved.
	store := newBoundedStore()
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Min[int64](), state.Store[core.Bounded[int64]](store), nil, nil)

	if c.IsAdditive() {
		t.Fatal("Min monoid should not be additive")
	}
	if c.PendingKeys() != 0 {
		t.Fatalf("pending keys before add: %d, want 0", c.PendingKeys())
	}

	ctx := context.Background()
	for i, v := range []int64{50, 40, 30, 20, 10} {
		err := c.AddMany(ctx, intToKey(i), time.Now(),
			[]string{"k"}, core.NewBounded(v))
		if err != nil {
			t.Fatalf("AddMany %d: %v", i, err)
		}
	}
	// Fall-through means MergeMany ran 5 times — no buffering.
	if store.calls != 5 {
		t.Errorf("MergeUpdate calls: got %d, want 5 (fall-through)", store.calls)
	}
	if c.PendingKeys() != 0 {
		t.Errorf("pending keys after AddMany: %d, want 0 (no buffering)", c.PendingKeys())
	}
	// Flush is a no-op for the fall-through case.
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if store.calls != 5 {
		t.Errorf("MergeUpdate calls after Flush: got %d, want 5", store.calls)
	}
	if got := store.m[state.Key{Entity: "k"}].Value; got != 10 {
		t.Errorf("min value: got %d, want 10", got)
	}
}

func TestCoalescer_FlushOnShutdown(t *testing.T) {
	// Source closes mid-batch: residual events must flush when the driver
	// invokes Flush. Verifies that AddMany alone never silently drops events.
	store := newCountingStore()
	cfg := processor.Defaults()

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, nil)

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		err := c.AddMany(ctx, intToKey(i), time.Now(), []string{"hot"}, int64(1))
		if err != nil {
			t.Fatalf("AddMany %d: %v", i, err)
		}
	}
	// Before Flush, nothing has hit the store.
	if store.calls != 0 {
		t.Errorf("MergeUpdate calls before Flush: got %d, want 0", store.calls)
	}
	if c.PendingKeys() != 1 {
		t.Errorf("pending keys: got %d, want 1", c.PendingKeys())
	}
	// Flush drains the buffer.
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if store.calls != 1 {
		t.Errorf("MergeUpdate calls after Flush: got %d, want 1", store.calls)
	}
	if got := store.m[state.Key{Entity: "hot"}]; got != 50 {
		t.Errorf("final value: got %d, want 50", got)
	}
	if c.PendingKeys() != 0 {
		t.Errorf("pending keys after Flush: got %d, want 0", c.PendingKeys())
	}
}

func TestCoalescer_MaxKeysAutoFlush(t *testing.T) {
	// Crossing MaxKeys triggers an inline flush; the buffer is drained.
	store := newCountingStore()
	cfg := processor.Defaults()
	coalesce := processor.CoalesceConfig{
		Enabled:   true,
		MaxKeys:   5,
		FlushTick: 0, // disable time-based flush; isolate MaxKeys behavior
	}
	rec := metrics.NewInMemory()
	cfg.Recorder = rec

	c := processor.NewCoalescer(&cfg, coalesce, "test",
		core.Count(), state.Store[int64](store), nil, nil)

	ctx := context.Background()
	// 5 unique keys → fits in the buffer; the 5th insert triggers the
	// flush (the auto-flush check is len >= MaxKeys *after* insertion).
	for i := 0; i < 5; i++ {
		if err := c.AddMany(ctx, intToKey(i), time.Now(),
			[]string{intToKey(i)}, int64(1)); err != nil {
			t.Fatalf("AddMany %d: %v", i, err)
		}
	}
	if store.calls != 5 {
		t.Errorf("MergeUpdate calls after 5 events with MaxKeys=5: got %d, want 5", store.calls)
	}
	if c.PendingKeys() != 0 {
		t.Errorf("pending after flush: %d, want 0", c.PendingKeys())
	}
	if rec.SnapshotOne("test:coalesce_flush_maxkeys").EventsProcessed == 0 {
		t.Error("expected coalesce_flush_maxkeys event")
	}
}

func TestCoalescer_TimeTickFlush(t *testing.T) {
	// During a long-running batch, the time tick must flush periodically so
	// retention SLAs hold even when MaxKeys isn't reached.
	store := newCountingStore()
	cfg := processor.Defaults()
	coalesce := processor.CoalesceConfig{
		Enabled:   true,
		MaxKeys:   1_000_000, // effectively unbounded
		FlushTick: 100 * time.Millisecond,
	}
	rec := metrics.NewInMemory()
	cfg.Recorder = rec

	c := processor.NewCoalescer(&cfg, coalesce, "test",
		core.Count(), state.Store[int64](store), nil, nil)

	// Drive a controllable clock.
	current := time.Now()
	processor.SetCoalescerClock(c, func() time.Time { return current })

	ctx := context.Background()
	// First event: clock hasn't advanced, no auto-flush.
	if err := c.AddMany(ctx, "e0", current, []string{"k"}, int64(1)); err != nil {
		t.Fatalf("AddMany 0: %v", err)
	}
	if store.calls != 0 {
		t.Errorf("calls after first event: got %d, want 0", store.calls)
	}

	// Advance the clock past FlushTick.
	current = current.Add(200 * time.Millisecond)
	// Second event: triggers tick-based flush.
	if err := c.AddMany(ctx, "e1", current, []string{"k"}, int64(1)); err != nil {
		t.Fatalf("AddMany 1: %v", err)
	}
	if store.calls != 1 {
		t.Errorf("calls after time-tick: got %d, want 1", store.calls)
	}
	if rec.SnapshotOne("test:coalesce_flush_tick").EventsProcessed == 0 {
		t.Error("expected coalesce_flush_tick event")
	}
	// The flushed total is the prior 2 events combined.
	if got := store.m[state.Key{Entity: "k"}]; got != 2 {
		t.Errorf("flushed value: got %d, want 2", got)
	}
}

func TestCoalescer_DedupSkipsBeforeBuffering(t *testing.T) {
	// A repeated EventID must short-circuit before its delta is added to the
	// pending map.
	store := newCountingStore()
	dedup := &memDeduper{}
	cfg := processor.Defaults()
	cfg.Dedup = dedup
	rec := metrics.NewInMemory()
	cfg.Recorder = rec

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, nil)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		err := c.AddMany(ctx, "dup", time.Now(), []string{"k"}, int64(1))
		if err != nil {
			t.Fatalf("AddMany %d: %v", i, err)
		}
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := store.m[state.Key{Entity: "k"}]; got != 1 {
		t.Errorf("dedup'd value: got %d, want 1", got)
	}
	if rec.SnapshotOne("test:dedup_skip").EventsProcessed != 4 {
		t.Errorf("dedup_skip count: got %d, want 4",
			rec.SnapshotOne("test:dedup_skip").EventsProcessed)
	}
}

func TestCoalescer_WindowedBucketing(t *testing.T) {
	// Two events with different bucket timestamps must accumulate into separate
	// keys even though their entity strings match — coalescing must respect
	// (entity, bucket) keying.
	store := newCountingStore()
	cfg := processor.Defaults()
	win := windowed.Hourly(24 * time.Hour)

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, &win)

	ctx := context.Background()
	t1 := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 13, 30, 0, 0, time.UTC)

	// 3 events into bucket(t1), 2 events into bucket(t2).
	for i := 0; i < 3; i++ {
		_ = c.AddMany(ctx, intToKey(i), t1, []string{"hot"}, int64(1))
	}
	for i := 3; i < 5; i++ {
		_ = c.AddMany(ctx, intToKey(i), t2, []string{"hot"}, int64(1))
	}
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// 2 unique (entity, bucket) keys → 2 MergeUpdate calls.
	if store.calls != 2 {
		t.Errorf("calls: got %d, want 2", store.calls)
	}
	b1 := win.BucketID(t1)
	b2 := win.BucketID(t2)
	if got := store.m[state.Key{Entity: "hot", Bucket: b1}]; got != 3 {
		t.Errorf("bucket(t1) value: got %d, want 3", got)
	}
	if got := store.m[state.Key{Entity: "hot", Bucket: b2}]; got != 2 {
		t.Errorf("bucket(t2) value: got %d, want 2", got)
	}
}

func TestCoalescer_FlushFailureSurfacesContributingEventIDs(t *testing.T) {
	// When a flushed key fails after retries, the FlushError carries the
	// EventIDs that contributed to it — drivers use this to do partial-batch
	// reporting back to the source.
	store := &failingStore{}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1
	cfg.BackoffBase = time.Microsecond
	cfg.BackoffMax = time.Microsecond

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, nil)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_ = c.AddMany(ctx, intToKey(i), time.Now(), []string{"k"}, int64(1))
	}

	err := c.Flush(ctx)
	if err == nil {
		t.Fatal("expected non-nil flush error")
	}
	var fe *processor.FlushError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FlushError, got %T: %v", err, err)
	}
	if len(fe.FailedKeys) != 1 {
		t.Fatalf("FailedKeys: got %d, want 1", len(fe.FailedKeys))
	}
	if got := fe.FailedKeys[0].Key.Entity; got != "k" {
		t.Errorf("failed key entity: got %q, want \"k\"", got)
	}
	if got := len(fe.FailedKeys[0].ContributingIDs); got != 3 {
		t.Errorf("ContributingIDs len: got %d, want 3", got)
	}
}

func TestCoalescer_ContextCancelDuringFlush(t *testing.T) {
	store := &cancellingStore{}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	_ = c.AddMany(ctx, "e0", time.Now(), []string{"k"}, int64(1))
	cancel()
	err := c.Flush(ctx)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	// State is reset so a retry after re-establishing context doesn't double-write.
	if c.PendingKeys() != 0 {
		t.Errorf("pending after cancel flush: %d, want 0", c.PendingKeys())
	}
}

func TestMergeBatch_AdditiveCollapses(t *testing.T) {
	store := newCountingStore()
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	type batchEvent struct {
		ID    string
		Key   string
		Value int64
		Time  time.Time
	}
	events := make([]batchEvent, 1000)
	for i := range events {
		events[i] = batchEvent{
			ID:    intToKey(i),
			Key:   intToKey(i % 10),
			Value: 1,
			Time:  time.Now(),
		}
	}
	err := processor.MergeBatch[batchEvent, int64](
		context.Background(), &cfg, processor.DefaultCoalesceConfig(),
		"test", core.Count(), state.Store[int64](store), nil, nil,
		events,
		func(e batchEvent) string { return e.ID },
		func(e batchEvent) time.Time { return e.Time },
		func(e batchEvent) []string { return []string{e.Key} },
		func(e batchEvent) int64 { return e.Value },
	)
	if err != nil {
		t.Fatalf("MergeBatch: %v", err)
	}
	if store.calls != 10 {
		t.Errorf("MergeUpdate calls: got %d, want 10", store.calls)
	}
}

// BenchmarkCoalescedHotKey_PerEvent vs BenchmarkCoalescedHotKey_Coalesced compare per-
// event MergeMany throughput against the Coalescer's batch-then-flush throughput on a
// hot-key workload (1000 events spread across 10 unique keys). Run with:
//
//	go test -bench=BenchmarkCoalescedHotKey -benchmem ./pkg/exec/processor/...
func BenchmarkCoalescedHotKey_PerEvent(b *testing.B) {
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1
	store := &slowStore{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = processor.MergeMany(context.Background(), &cfg, "bench",
			intToKey(i), time.Time{}, []string{intToKey(i % 10)}, int64(1),
			state.Store[int64](store), nil, nil)
	}
}

func BenchmarkCoalescedHotKey_Coalesced(b *testing.B) {
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1
	store := &slowStore{}

	const batchSize = 1000
	type batchEvent struct {
		ID    string
		Key   string
		Value int64
		Time  time.Time
	}
	events := make([]batchEvent, batchSize)
	for i := range events {
		events[i] = batchEvent{
			ID:    intToKey(i),
			Key:   intToKey(i % 10),
			Value: 1,
			Time:  time.Time{},
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i += batchSize {
		_ = processor.MergeBatch[batchEvent, int64](
			context.Background(), &cfg, processor.DefaultCoalesceConfig(),
			"bench", core.Count(), state.Store[int64](store), nil, nil,
			events,
			func(e batchEvent) string { return e.ID },
			func(e batchEvent) time.Time { return e.Time },
			func(e batchEvent) []string { return []string{e.Key} },
			func(e batchEvent) int64 { return e.Value },
		)
	}
}

// --- helpers ---

// slowStore simulates DDB-style per-call latency. Used in benchmarks to make
// the coalescing-win observable.
type slowStore struct {
	mu sync.Mutex
}

func (*slowStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}

func (*slowStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (s *slowStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	// Pure-CPU spinloop so the benchmark surfaces processor + serialization
	// overhead rather than allocation noise. Real DDB is 10ms+; here we just
	// want the relative shape between per-event and coalesced paths.
	s.mu.Lock()
	for i := 0; i < 50; i++ {
		_ = i
	}
	s.mu.Unlock()
	return nil
}

func (*slowStore) Close() error { return nil }

// cancellingStore returns the context error from MergeUpdate when ctx is done.
type cancellingStore struct{}

func (*cancellingStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}

func (*cancellingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (*cancellingStore) MergeUpdate(ctx context.Context, _ state.Key, _ int64, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (*cancellingStore) Close() error { return nil }

// boundedStore is a fake Store[core.Bounded[int64]] for Min/Max testing.
type boundedStore struct {
	mu    sync.Mutex
	m     map[state.Key]core.Bounded[int64]
	calls int
}

func newBoundedStore() *boundedStore {
	return &boundedStore{m: map[state.Key]core.Bounded[int64]{}}
}

func (s *boundedStore) Get(_ context.Context, k state.Key) (core.Bounded[int64], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}

func (s *boundedStore) GetMany(context.Context, []state.Key) ([]core.Bounded[int64], []bool, error) {
	return nil, nil, nil
}

func (s *boundedStore) MergeUpdate(_ context.Context, k state.Key, d core.Bounded[int64], _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	cur, ok := s.m[k]
	if !ok || !cur.Set {
		s.m[k] = d
		return nil
	}
	if d.Set && d.Value < cur.Value {
		s.m[k] = d
	}
	return nil
}

func (s *boundedStore) Close() error { return nil }

// intToKey is a small int→string helper that doesn't pull strconv.
func intToKey(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
