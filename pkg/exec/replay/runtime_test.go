package replay_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/replay"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	replaydrv "github.com/gallowaysoftware/murmur/pkg/replay"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// Compile-time interface check so a refactor of replaydrv.Driver surfaces
// here rather than in a runtime error.
var _ replaydrv.Driver[int] = (*fakeDriver)(nil)

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

// fakeDriver is a minimal replay.Driver[int] that emits a fixed slice of
// values and closes. Each record gets a distinct EventID.
type fakeDriver struct {
	values []int
}

func (d *fakeDriver) Replay(_ context.Context, out chan<- source.Record[int]) error {
	for i, v := range d.values {
		out <- source.Record[int]{
			EventID: "ev-" + strconv.Itoa(i),
			Value:   v,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*fakeDriver) Name() string { return "fake-driver" }
func (*fakeDriver) Close() error { return nil }

// slowDriver emits values with a delay between each so the periodic batch
// ticker fires before the driver completes.
type slowDriver struct {
	values []int
	delay  time.Duration
}

func (d *slowDriver) Replay(ctx context.Context, out chan<- source.Record[int]) error {
	for i, v := range d.values {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.delay):
		}
		out <- source.Record[int]{
			EventID: "ev-" + strconv.Itoa(i),
			Value:   v,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*slowDriver) Name() string { return "slow-driver" }
func (*slowDriver) Close() error { return nil }

func newPipe(store state.Store[int64]) *pipeline.Pipeline[int, int64] {
	return pipeline.NewPipeline[int, int64]("replay-test").
		Key(func(i int) string { return strconv.Itoa(i % 3) }).
		Value(func(i int) int64 { return int64(i) }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func TestReplay_HappyPath(t *testing.T) {
	store := newFakeStore()
	drv := &fakeDriver{values: []int{1, 2, 3, 4, 5, 6}}

	if err := replay.Run(context.Background(), newPipe(store), drv); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Sums by mod-3 group: 0+3+6=9, 1+4=5, 2+5=7.
	want := map[string]int64{"0": 9, "1": 5, "2": 7}
	for entity, w := range want {
		if got := store.m[state.Key{Entity: entity}]; got != w {
			t.Errorf("entity %q: got %d, want %d", entity, got, w)
		}
	}
}

func TestReplay_RetriesOnTransientStoreFailure(t *testing.T) {
	store := newFlakyStore(2)
	drv := &fakeDriver{values: []int{1, 2, 3}}

	if err := replay.Run(context.Background(), newPipe(store), drv,
		replay.WithMaxAttempts(3),
		replay.WithRetryBackoff(time.Millisecond, time.Millisecond),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := map[string]int64{"0": 3, "1": 1, "2": 2}
	for entity, w := range want {
		if got := store.m[state.Key{Entity: entity}]; got != w {
			t.Errorf("entity %q: got %d, want %d", entity, got, w)
		}
	}
}

func TestReplay_FailOnErrorAborts(t *testing.T) {
	store := newFlakyStore(99)
	drv := &fakeDriver{values: []int{1, 2, 3}}

	err := replay.Run(context.Background(), newPipe(store), drv,
		replay.WithMaxAttempts(2),
		replay.WithRetryBackoff(time.Millisecond, time.Millisecond),
		replay.WithFailOnError(true),
	)
	if err == nil {
		t.Fatal("expected error with WithFailOnError(true)")
	}
}

func TestReplay_DefaultPermissiveOnDeadLetter(t *testing.T) {
	// Perma-failing store + default config: replay should COMPLETE with the
	// dead-letter recorded. Same contract as bootstrap: a single bad row in
	// a 30-day archive shouldn't abort the whole replay.
	store := newFlakyStore(99)
	drv := &fakeDriver{values: []int{1, 2, 3}}
	rec := metrics.NewInMemory()

	if err := replay.Run(context.Background(), newPipe(store), drv,
		replay.WithMaxAttempts(2),
		replay.WithRetryBackoff(time.Millisecond, time.Millisecond),
		replay.WithMetrics(rec),
	); err != nil {
		t.Fatalf("Run: %v (default should be permissive)", err)
	}
	if dl := rec.SnapshotOne("replay-test:dead_letter").EventsProcessed; dl != 3 {
		t.Errorf("dead_letter events: got %d, want 3", dl)
	}
}

func TestReplay_EmitsRecordBatchWithReplayMode(t *testing.T) {
	store := newFakeStore()
	drv := &fakeDriver{values: []int{1, 2, 3, 4, 5, 6}}
	rec := metrics.NewInMemory()

	if err := replay.Run(context.Background(), newPipe(store), drv,
		replay.WithMetrics(rec),
		replay.WithBatchTick(0), // assert on the completion flush only
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := rec.SnapshotOne("replay-test:batch:replay").EventsProcessed
	if got != 6 {
		t.Errorf("replay batch events: got %d, want 6", got)
	}
	// Cross-mode buckets must remain empty.
	if got := rec.SnapshotOne("replay-test:batch:streaming").EventsProcessed; got != 0 {
		t.Errorf("streaming batch events on a replay run: got %d, want 0", got)
	}
	if got := rec.SnapshotOne("replay-test:batch:bootstrap").EventsProcessed; got != 0 {
		t.Errorf("bootstrap batch events on a replay run: got %d, want 0", got)
	}
	pipe := rec.SnapshotOne("replay-test")
	if _, ok := pipe.Latencies["batch_replay"]; !ok {
		t.Errorf("expected batch_replay latency op")
	}
}

func TestReplay_EmitsExpectedProcessorOps(t *testing.T) {
	store := newFakeStore()
	drv := &fakeDriver{values: []int{1, 2, 3}}
	rec := metrics.NewInMemory()

	if err := replay.Run(context.Background(), newPipe(store), drv,
		replay.WithMetrics(rec),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	pipe := rec.SnapshotOne("replay-test")
	if _, ok := pipe.Latencies["store_merge"]; !ok {
		t.Errorf("expected store_merge latency op")
	}
	if pipe.EventsProcessed != 3 {
		t.Errorf("pipeline events: got %d, want 3", pipe.EventsProcessed)
	}
}

func TestReplay_NoopRecorderIsDefault(t *testing.T) {
	// Without WithMetrics the runtime must default to a metrics.Noop and
	// must not panic when emitting batch / latency / event records.
	store := newFakeStore()
	drv := &fakeDriver{values: []int{1, 2, 3}}

	if err := replay.Run(context.Background(), newPipe(store), drv); err != nil {
		t.Fatalf("Run with default Noop recorder: %v", err)
	}
}

func TestReplay_PeriodicBatchTickEmitsDuringDrain(t *testing.T) {
	store := newFakeStore()
	drv := &slowDriver{values: []int{1, 2, 3, 4, 5}, delay: 30 * time.Millisecond}
	rec := metrics.NewInMemory()

	if err := replay.Run(context.Background(), newPipe(store), drv,
		replay.WithMetrics(rec),
		replay.WithBatchTick(50*time.Millisecond),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// All 5 records accounted for in the summed batch count.
	if got := rec.SnapshotOne("replay-test:batch:replay").EventsProcessed; got != 5 {
		t.Errorf("replay batch events: got %d, want 5", got)
	}
	pipe := rec.SnapshotOne("replay-test")
	lat, ok := pipe.Latencies["batch_replay"]
	if !ok || lat.N < 1 {
		t.Errorf("expected at least 1 batch_replay latency sample, got %d", lat.N)
	}
}
