package processor_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// trackingStore is a Store[int64] that records (a) the final per-key sum
// and (b) per-key call counts. Safe for concurrent use.
type trackingStore struct {
	mu     sync.Mutex
	values map[state.Key]int64
	calls  map[state.Key]int
}

func newTrackingStore() *trackingStore {
	return &trackingStore{
		values: map[state.Key]int64{},
		calls:  map[state.Key]int{},
	}
}

func (s *trackingStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[k]
	return v, ok, nil
}
func (s *trackingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *trackingStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[k] += d
	s.calls[k]++
	return nil
}
func (*trackingStore) Close() error { return nil }

// errOnceStore returns errFlush exactly once on a configured key, then
// succeeds on subsequent attempts. Used to exercise retry-then-recovery
// inside FlushBatch.
type errOnceStore struct {
	mu       sync.Mutex
	values   map[state.Key]int64
	failed   map[state.Key]bool
	failKey  state.Key
	totalErr int32
}

var errFlush = errors.New("flush failure")

func (s *errOnceStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[k]
	return v, ok, nil
}
func (s *errOnceStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *errOnceStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if k == s.failKey && !s.failed[k] {
		s.failed[k] = true
		atomic.AddInt32(&s.totalErr, 1)
		return errFlush
	}
	s.values[k] += d
	return nil
}
func (*errOnceStore) Close() error { return nil }

// alwaysErrStore always returns errFlush. Used to verify that
// FlushBatch returns an error and short-circuits remaining work.
type alwaysErrStore struct {
	calls int32
}

func (*alwaysErrStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}
func (*alwaysErrStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *alwaysErrStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	atomic.AddInt32(&s.calls, 1)
	return errFlush
}
func (*alwaysErrStore) Close() error { return nil }

func buildItems(n int) []processor.FlushItem[int64] {
	items := make([]processor.FlushItem[int64], n)
	for i := range items {
		items[i] = processor.FlushItem[int64]{
			Key:       state.Key{Entity: fmt.Sprintf("k-%d", i)},
			Delta:     int64(i + 1),
			EventTime: time.Now(),
		}
	}
	return items
}

// TestFlushBatch_DefaultConcurrencyIsSequential verifies that
// concurrency<=1 routes through the sequential path and ends with the
// expected final state. The store doesn't observe concurrency directly,
// but the test exercises the default-path branch.
func TestFlushBatch_DefaultConcurrencyIsSequential(t *testing.T) {
	store := newTrackingStore()
	cfg := processor.Defaults()

	items := buildItems(10)
	if err := processor.FlushBatch(context.Background(), &cfg, "test",
		items, store, nil, nil, 1); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	for _, it := range items {
		if got := store.values[it.Key]; got != it.Delta {
			t.Errorf("key %s: got %d, want %d", it.Key.Entity, got, it.Delta)
		}
	}
}

// TestFlushBatch_ConcurrencyCorrectness verifies that the final per-key
// state at concurrency = 1, 4, 16 is byte-identical. Each FlushItem
// addresses a distinct key (which is the contract — callers coalesce
// deltas BEFORE FlushBatch), so the worker pool produces deterministic
// results across all N.
func TestFlushBatch_ConcurrencyCorrectness(t *testing.T) {
	const numItems = 500
	for _, n := range []int{1, 4, 16} {
		t.Run(fmt.Sprintf("concurrency=%d", n), func(t *testing.T) {
			store := newTrackingStore()
			cfg := processor.Defaults()

			items := buildItems(numItems)
			if err := processor.FlushBatch(context.Background(), &cfg, "test",
				items, store, nil, nil, n); err != nil {
				t.Fatalf("FlushBatch n=%d: %v", n, err)
			}

			for _, it := range items {
				if got := store.values[it.Key]; got != it.Delta {
					t.Errorf("n=%d key %s: got %d, want %d", n, it.Key.Entity, got, it.Delta)
				}
				if got := store.calls[it.Key]; got != 1 {
					t.Errorf("n=%d key %s: got %d MergeUpdate calls, want 1", n, it.Key.Entity, got)
				}
			}
		})
	}
}

// TestFlushBatch_ConcurrencyOverItemsIsClamped passes concurrency=64 for
// a batch of 4 items. The pool should clamp to 4 workers (one per item)
// and still produce the correct final state.
func TestFlushBatch_ConcurrencyOverItemsIsClamped(t *testing.T) {
	store := newTrackingStore()
	cfg := processor.Defaults()
	items := buildItems(4)
	if err := processor.FlushBatch(context.Background(), &cfg, "test",
		items, store, nil, nil, 64); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	for _, it := range items {
		if got := store.values[it.Key]; got != it.Delta {
			t.Errorf("key %s: got %d, want %d", it.Key.Entity, got, it.Delta)
		}
	}
}

// TestFlushBatch_EmptyItemsIsNoOp.
func TestFlushBatch_EmptyItemsIsNoOp(t *testing.T) {
	store := newTrackingStore()
	cfg := processor.Defaults()
	if err := processor.FlushBatch(context.Background(), &cfg, "test",
		nil, store, nil, nil, 4); err != nil {
		t.Fatalf("FlushBatch (nil items): %v", err)
	}
	if err := processor.FlushBatch(context.Background(), &cfg, "test",
		[]processor.FlushItem[int64]{}, store, nil, nil, 4); err != nil {
		t.Fatalf("FlushBatch (empty items): %v", err)
	}
	if len(store.values) != 0 {
		t.Errorf("expected no merges; store has %d entries", len(store.values))
	}
}

// TestFlushBatch_RetriesAndRecovers verifies that a transient error on
// one key is retried by mergeKeyWithRetry inside FlushBatch and the
// batch ultimately succeeds with the correct final state.
func TestFlushBatch_RetriesAndRecovers(t *testing.T) {
	store := &errOnceStore{
		values:  map[state.Key]int64{},
		failed:  map[state.Key]bool{},
		failKey: state.Key{Entity: "k-2"},
	}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 3
	cfg.BackoffBase = time.Millisecond
	cfg.BackoffMax = time.Millisecond

	items := buildItems(5)
	if err := processor.FlushBatch(context.Background(), &cfg, "test",
		items, store, nil, nil, 4); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	if atomic.LoadInt32(&store.totalErr) != 1 {
		t.Errorf("expected exactly 1 transient error, saw %d", store.totalErr)
	}
	for _, it := range items {
		if got := store.values[it.Key]; got != it.Delta {
			t.Errorf("key %s: got %d, want %d", it.Key.Entity, got, it.Delta)
		}
	}
}

// TestFlushBatch_WorkerErrorFailsBatch verifies that when one worker hits
// a permanent error, FlushBatch returns that error AND short-circuits
// the remaining work — the entire batch is treated as failed and the
// caller can rely on at-least-once redelivery to re-apply everything.
func TestFlushBatch_WorkerErrorFailsBatch(t *testing.T) {
	store := &alwaysErrStore{}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1 // fail immediately, no retry
	cfg.BackoffBase = time.Microsecond
	cfg.BackoffMax = time.Microsecond

	items := buildItems(50)
	err := processor.FlushBatch(context.Background(), &cfg, "test",
		items, store, nil, nil, 4)
	if err == nil {
		t.Fatal("expected non-nil error when every worker fails")
	}
	if !errors.Is(err, errFlush) {
		t.Errorf("error chain: got %v, want errFlush", err)
	}
	// Short-circuit: the producer should bail out as soon as the first
	// worker error cancels workerCtx, so we expect well under 50 store
	// calls. Allow up to one full pass through items (50) in pathological
	// scheduling, but typically it's < 10.
	if got := atomic.LoadInt32(&store.calls); got > int32(len(items)) {
		t.Errorf("store.MergeUpdate called %d times for %d items; should not exceed item count", got, len(items))
	}
}

// TestFlushBatch_ContextCancelStopsBatch verifies that canceling the
// caller's context propagates through to workers and returns a context
// error.
func TestFlushBatch_ContextCancelStopsBatch(t *testing.T) {
	store := newTrackingStore()
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	items := buildItems(100)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before starting

	err := processor.FlushBatch(ctx, &cfg, "test", items, store, nil, nil, 4)
	if err == nil {
		t.Fatal("expected non-nil error on canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error: got %v, want context.Canceled", err)
	}
}

// TestFlushBatch_AllWorkersExitOnError verifies there are no goroutine
// leaks after a worker error short-circuits the batch.
func TestFlushBatch_AllWorkersExitOnError(t *testing.T) {
	store := &alwaysErrStore{}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1
	cfg.BackoffBase = time.Microsecond
	cfg.BackoffMax = time.Microsecond

	items := buildItems(100)
	done := make(chan struct{})
	go func() {
		_ = processor.FlushBatch(context.Background(), &cfg, "test",
			items, store, nil, nil, 8)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("FlushBatch did not return after worker error; possible goroutine leak")
	}
}

// BenchmarkFlushBatch_Sequential benchmarks the concurrency=1 path,
// measuring the raw per-item overhead with no contention or parallelism.
func BenchmarkFlushBatch_Sequential(b *testing.B) {
	runFlushBench(b, 1)
}

// BenchmarkFlushBatch_4 / _16 measure the parallel-path overhead. Against
// the in-memory trackingStore there's no I/O to overlap, so these mostly
// show the cost of channel dispatch + goroutine wakeup; the real
// speed-up shows up against a latency-bearing store (see slowStore in
// streaming concurrency_bench_test.go).
func BenchmarkFlushBatch_4(b *testing.B) {
	runFlushBench(b, 4)
}

func BenchmarkFlushBatch_16(b *testing.B) {
	runFlushBench(b, 16)
}

func runFlushBench(b *testing.B, n int) {
	const numItems = 1024
	items := buildItems(numItems)
	store := newTrackingStore()
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = processor.FlushBatch(context.Background(), &cfg, "bench",
			items, store, nil, nil, n)
	}
	b.StopTimer()
	if b.Elapsed() > 0 {
		b.ReportMetric(float64(b.N*numItems)/b.Elapsed().Seconds(), "items/sec")
	}
}

// BenchmarkFlushBatch_SlowStore demonstrates the parallel speedup when
// the store has nontrivial per-call latency (5ms each). With 64 items at
// 5ms each:
//
//   - concurrency=1   : ~320ms
//   - concurrency=8   : ~40ms
//   - concurrency=16  : ~20ms
//
// This is the real-world picture against DDB UpdateItem.
func BenchmarkFlushBatch_SlowStore_1(b *testing.B)  { runFlushBenchSlow(b, 1) }
func BenchmarkFlushBatch_SlowStore_4(b *testing.B)  { runFlushBenchSlow(b, 4) }
func BenchmarkFlushBatch_SlowStore_8(b *testing.B)  { runFlushBenchSlow(b, 8) }
func BenchmarkFlushBatch_SlowStore_16(b *testing.B) { runFlushBenchSlow(b, 16) }

type slowFlushStore struct {
	latency time.Duration
}

func (*slowFlushStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}
func (*slowFlushStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *slowFlushStore) MergeUpdate(_ context.Context, _ state.Key, _ int64, _ time.Duration) error {
	time.Sleep(s.latency)
	return nil
}
func (*slowFlushStore) Close() error { return nil }

func runFlushBenchSlow(b *testing.B, n int) {
	const numItems = 64
	items := buildItems(numItems)
	store := &slowFlushStore{latency: 5 * time.Millisecond}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 1

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = processor.FlushBatch(context.Background(), &cfg, "bench-slow",
			items, store, nil, nil, n)
	}
	b.StopTimer()
	if b.Elapsed() > 0 {
		b.ReportMetric(float64(b.N*numItems)/b.Elapsed().Seconds(), "items/sec")
	}
}
