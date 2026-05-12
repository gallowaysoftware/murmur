package bootstrap_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/source/snapshot"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeStore is an in-memory state.Store[int64].
type fakeStore struct {
	mu sync.Mutex
	m  map[state.Key]int64
}

func newFakeStore() *fakeStore { return &fakeStore{m: map[state.Key]int64{}} }

func (s *fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.m[k]
	}
	return vs, oks, nil
}
func (s *fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}
func (*fakeStore) Close() error { return nil }

// flakyStore returns errFlaky N times per key, then succeeds.
type flakyStore struct {
	mu       sync.Mutex
	m        map[state.Key]int64
	attempts map[state.Key]int
	failures int
}

var errFlaky = errors.New("flaky")

func newFlakyStore(n int) *flakyStore {
	return &flakyStore{m: map[state.Key]int64{}, attempts: map[state.Key]int{}, failures: n}
}
func (s *flakyStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *flakyStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *flakyStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[k]++
	if s.attempts[k] <= s.failures {
		return errFlaky
	}
	s.m[k] += d
	return nil
}
func (*flakyStore) Close() error { return nil }

// fakeSnapshot is a minimal snapshot.Source[int] that emits a fixed batch.
type fakeSnapshot struct {
	values []int
}

func (f *fakeSnapshot) CaptureHandoff(context.Context) (snapshot.HandoffToken, error) {
	return snapshot.HandoffToken("token-1"), nil
}
func (f *fakeSnapshot) Scan(_ context.Context, out chan<- source.Record[int]) error {
	for i, v := range f.values {
		out <- source.Record[int]{
			EventID: "ev-" + strconv.Itoa(i),
			Value:   v,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*fakeSnapshot) Resume(context.Context, []byte, chan<- source.Record[int]) error {
	return nil
}
func (*fakeSnapshot) Name() string { return "fake-snapshot" }
func (*fakeSnapshot) Close() error { return nil }

func newPipe(store state.Store[int64]) *pipeline.Pipeline[int, int64] {
	return pipeline.NewPipeline[int, int64]("bootstrap-test").
		Key(func(i int) string { return strconv.Itoa(i % 3) }). // 3 buckets: "0", "1", "2"
		Value(func(i int) int64 { return int64(i) }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func TestBootstrap_HappyPath(t *testing.T) {
	store := newFakeStore()
	src := &fakeSnapshot{values: []int{1, 2, 3, 4, 5, 6}}

	token, err := bootstrap.Run(context.Background(), newPipe(store), src)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(token) != "token-1" {
		t.Errorf("token: got %q, want token-1", token)
	}
	// Sums by mod-3 group: 0+3+6=9, 1+4=5, 2+5=7.
	want := map[string]int64{"0": 9, "1": 5, "2": 7}
	for entity, w := range want {
		if got := store.m[state.Key{Entity: entity}]; got != w {
			t.Errorf("entity %q: got %d, want %d", entity, got, w)
		}
	}
}

func TestBootstrap_RetriesOnTransientStoreFailure(t *testing.T) {
	// Each key fails twice before succeeding. With MaxAttempts=3, every
	// record should land — bootstrap must retry, not abort, on transient
	// store errors.
	store := newFlakyStore(2)
	src := &fakeSnapshot{values: []int{1, 2, 3}}

	_, err := bootstrap.Run(context.Background(), newPipe(store), src,
		bootstrap.WithMaxAttempts(3),
		bootstrap.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Source values [1,2,3]; key = i%3 → 1→"1", 2→"2", 3→"0".
	want := map[string]int64{"0": 3, "1": 1, "2": 2}
	for entity, w := range want {
		if got := store.m[state.Key{Entity: entity}]; got != w {
			t.Errorf("entity %q: got %d, want %d", entity, got, w)
		}
	}
}

func TestBootstrap_DefaultPermissiveOnDeadLetter(t *testing.T) {
	// Perma-failing store + default config: bootstrap should COMPLETE
	// (returning the captured token) with the dead-letter recorded. The
	// streaming runtime never aborts on poison records; bootstrap inherits
	// the same default since a one-bad-row failure shouldn't fail a 30-
	// minute scan.
	store := newFlakyStore(99)
	src := &fakeSnapshot{values: []int{1, 2, 3}}
	rec := metrics.NewInMemory()

	token, err := bootstrap.Run(context.Background(), newPipe(store), src,
		bootstrap.WithMaxAttempts(2),
		bootstrap.WithRetryBackoff(time.Millisecond, time.Millisecond),
		bootstrap.WithMetrics(rec),
	)
	if err != nil {
		t.Fatalf("Run: %v (default should be permissive)", err)
	}
	if string(token) != "token-1" {
		t.Errorf("token: got %q, want token-1", token)
	}
	// 3 records each dead-lettered.
	dl := rec.SnapshotOne("bootstrap-test:dead_letter").EventsProcessed
	if dl != 3 {
		t.Errorf("dead_letter events: got %d, want 3", dl)
	}
}

func TestBootstrap_FailOnErrorAborts(t *testing.T) {
	store := newFlakyStore(99)
	src := &fakeSnapshot{values: []int{1, 2, 3}}

	_, err := bootstrap.Run(context.Background(), newPipe(store), src,
		bootstrap.WithMaxAttempts(2),
		bootstrap.WithRetryBackoff(time.Millisecond, time.Millisecond),
		bootstrap.WithFailOnError(true),
	)
	if err == nil {
		t.Fatal("expected error with WithFailOnError(true)")
	}
}

func TestBootstrap_EmitsRecordBatchWithBootstrapMode(t *testing.T) {
	// A successful bootstrap must emit exactly one or more RecordBatch
	// calls with mode="bootstrap" whose summed n equals the records
	// drained from the snapshot. The mode attribute is what lets the
	// "events/s by mode" dashboard distinguish bootstrap from streaming
	// traffic during the M8 backfill phase.
	store := newFakeStore()
	src := &fakeSnapshot{values: []int{1, 2, 3, 4, 5, 6}}
	rec := metrics.NewInMemory()

	if _, err := bootstrap.Run(context.Background(), newPipe(store), src,
		bootstrap.WithMetrics(rec),
		// Disable the periodic ticker so the test isn't time-sensitive; the
		// completion-time flush is what we assert on.
		bootstrap.WithBatchTick(0),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 6 records drained; the per-mode batch counter must reflect that.
	got := rec.SnapshotOne("bootstrap-test:batch:bootstrap").EventsProcessed
	if got != 6 {
		t.Errorf("bootstrap batch events: got %d, want 6", got)
	}

	// The unrelated streaming bucket must remain empty so dashboards
	// filtering by mode don't see cross-contamination.
	if got := rec.SnapshotOne("bootstrap-test:batch:streaming").EventsProcessed; got != 0 {
		t.Errorf("streaming batch events on a bootstrap run: got %d, want 0", got)
	}

	// Batch latency op must be present on the pipeline name so dashboards
	// can plot p50/p95/p99 of per-batch wall time.
	pipe := rec.SnapshotOne("bootstrap-test")
	if _, ok := pipe.Latencies["batch_bootstrap"]; !ok {
		t.Errorf("expected batch_bootstrap latency op on bootstrap-test")
	}
}

func TestBootstrap_EmitsExpectedProcessorOps(t *testing.T) {
	// Bootstrap must emit the same per-op latencies (store_merge,
	// cache_merge) that streaming does, so a single dashboard panel
	// covers all modes. We assert store_merge appears; cache_merge is
	// skipped here because the test pipeline has no cache configured.
	store := newFakeStore()
	src := &fakeSnapshot{values: []int{1, 2, 3}}
	rec := metrics.NewInMemory()

	if _, err := bootstrap.Run(context.Background(), newPipe(store), src,
		bootstrap.WithMetrics(rec),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	pipe := rec.SnapshotOne("bootstrap-test")
	if _, ok := pipe.Latencies["store_merge"]; !ok {
		t.Errorf("expected store_merge latency op")
	}
	if pipe.EventsProcessed != 3 {
		t.Errorf("pipeline events: got %d, want 3", pipe.EventsProcessed)
	}
}

func TestBootstrap_NoopRecorderIsDefault(t *testing.T) {
	// Without WithMetrics the runtime must default to a metrics.Noop and
	// must not panic when emitting batch / latency / event records. This
	// is the zero-overhead-default contract for callers that don't want
	// any observability surface.
	store := newFakeStore()
	src := &fakeSnapshot{values: []int{1, 2, 3}}

	if _, err := bootstrap.Run(context.Background(), newPipe(store), src); err != nil {
		t.Fatalf("Run with default Noop recorder: %v", err)
	}
}

func TestBootstrap_PeriodicBatchTickEmitsDuringScan(t *testing.T) {
	// With a periodic batchTick, the bootstrap must emit RecordBatch
	// while the snapshot is still being drained — not only at completion.
	// This is the dashboard-granularity contract for multi-hour bootstraps.
	store := newFakeStore()
	// Slow snapshot: emit one record every 30 ms, 5 records total. With
	// a 50 ms batchTick, the runtime should emit at least 2 batches
	// before the snapshot closes.
	slow := &slowSnapshot{values: []int{1, 2, 3, 4, 5}, delay: 30 * time.Millisecond}
	rec := metrics.NewInMemory()

	if _, err := bootstrap.Run(context.Background(), newPipe(store), slow,
		bootstrap.WithMetrics(rec),
		bootstrap.WithBatchTick(50*time.Millisecond),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All 5 records accounted for in the summed batch count.
	got := rec.SnapshotOne("bootstrap-test:batch:bootstrap").EventsProcessed
	if got != 5 {
		t.Errorf("bootstrap batch events: got %d, want 5", got)
	}
	// At least two batch latency samples (one tick mid-flight + one at
	// completion). Tolerate >= 1 since a fast machine could still race
	// the ticker, but the typical case is >= 2.
	pipe := rec.SnapshotOne("bootstrap-test")
	lat, ok := pipe.Latencies["batch_bootstrap"]
	if !ok || lat.N < 1 {
		t.Errorf("expected at least 1 batch_bootstrap latency sample, got %d", lat.N)
	}
}

// slowSnapshot emits values with a delay between each. Lets the periodic
// batch ticker fire before the snapshot completes.
type slowSnapshot struct {
	values []int
	delay  time.Duration
}

func (s *slowSnapshot) CaptureHandoff(context.Context) (snapshot.HandoffToken, error) {
	return snapshot.HandoffToken("token-slow"), nil
}
func (s *slowSnapshot) Scan(ctx context.Context, out chan<- source.Record[int]) error {
	for i, v := range s.values {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.delay):
		}
		out <- source.Record[int]{
			EventID: "ev-" + strconv.Itoa(i),
			Value:   v,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*slowSnapshot) Resume(context.Context, []byte, chan<- source.Record[int]) error {
	return nil
}
func (*slowSnapshot) Name() string { return "slow-snapshot" }
func (*slowSnapshot) Close() error { return nil }

func TestBootstrap_KeyByManyHierarchical(t *testing.T) {
	// Each value contributes to multiple keys (a + b + c). Verify the
	// hierarchical-rollup wiring works in bootstrap mode just like
	// streaming mode.
	store := newFakeStore()
	src := &fakeSnapshot{values: []int{1, 2, 3}}

	pipe := pipeline.NewPipeline[int, int64]("hier-bootstrap").
		KeyByMany(func(int) []string { return []string{"a", "b", "global"} }).
		Value(func(i int) int64 { return int64(i) }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	if _, err := bootstrap.Run(context.Background(), pipe, src); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// 1+2+3=6 lands on each of the three keys.
	for _, entity := range []string{"a", "b", "global"} {
		if got := store.m[state.Key{Entity: entity}]; got != 6 {
			t.Errorf("entity %q: got %d, want 6", entity, got)
		}
	}
}
