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

// fakeClock is a hand-wound clock. Replay idempotency is bounded by the
// deduper's TTL, and a test that waited out a real one would never run.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ttlDeduper models dynamodb.Deduper: one claim per EventID, which DDB's
// native TTL drops once it expires, after which the same EventID is claimable
// again. That expiry is exactly what bounds a replay's idempotency.
type ttlDeduper struct {
	mu      sync.Mutex
	clock   *fakeClock
	ttl     time.Duration
	expires map[string]time.Time
}

func newTTLDeduper(clock *fakeClock, ttl time.Duration) *ttlDeduper {
	return &ttlDeduper{clock: clock, ttl: ttl, expires: map[string]time.Time{}}
}

func (d *ttlDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	if id == "" {
		return true, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.clock.Now()
	if exp, ok := d.expires[id]; ok && now.Before(exp) {
		return false, nil
	}
	d.expires[id] = now.Add(d.ttl)
	return true, nil
}

func (d *ttlDeduper) Release(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.expires, id)
	return nil
}

func (*ttlDeduper) Close() error { return nil }

// newCountingPipe sums one unit per record into a single entity, so a re-run
// that double-counts shows up as a doubled total rather than a per-key puzzle.
func newCountingPipe(store state.Store[int64]) *pipeline.Pipeline[int, int64] {
	return pipeline.NewPipeline[int, int64]("replay-dedup").
		Key(func(int) string { return "all" }).
		Value(func(int) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

// archive returns n records of replay input. fakeDriver assigns each record a
// positional EventID, which is what a real S3-archive or Kafka-offset driver
// does: stable across re-runs of the same archive, which is the whole basis
// for dedup catching a re-run.
func archive(n int) *fakeDriver {
	vals := make([]int, n)
	for i := range vals {
		vals[i] = i
	}
	return &fakeDriver{values: vals}
}

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

// TestReplay_RerunWithDedupIsIdempotent is the replay analogue of the
// bootstrap re-run test: an operator who re-runs the same archive (a retry
// after a partial failure, a second pass over a shadow table) must not double
// the totals. Sum is non-idempotent, so WithDedup is the only thing standing
// between a re-run and a corrupted backfill.
func TestReplay_RerunWithDedupIsIdempotent(t *testing.T) {
	const records = 100

	store := newFakeStore()
	clock := newFakeClock()
	dedup := newTTLDeduper(clock, time.Hour)
	rec := metrics.NewInMemory()

	for run := 1; run <= 2; run++ {
		if err := replay.Run(context.Background(), newCountingPipe(store), archive(records),
			replay.WithDedup(dedup),
			replay.WithMetrics(rec),
		); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}

	if got := store.m[state.Key{Entity: "all"}]; got != records {
		t.Errorf("total after two identical replays: got %d, want %d", got, records)
	}
	if got := rec.SnapshotOne("replay-dedup:dedup_skip").EventsProcessed; got != records {
		t.Errorf("dedup_skip events on the second replay: got %d, want %d", got, records)
	}
}

// TestReplay_RerunAfterClaimExpiryDoubleCounts pins the horizon on that
// idempotency: the Deduper's claims are TTL'd, and once they expire the same
// archive merges a second time. 200, not 100, is the intended contract — an
// operator re-running a backfill a day later with a 1h dedup TTL is not
// protected, and nothing in the runtime can tell that re-run from new data.
func TestReplay_RerunAfterClaimExpiryDoubleCounts(t *testing.T) {
	const (
		records = 100
		ttl     = time.Hour
	)

	store := newFakeStore()
	clock := newFakeClock()
	dedup := newTTLDeduper(clock, ttl)

	if err := replay.Run(context.Background(), newCountingPipe(store), archive(records),
		replay.WithDedup(dedup),
	); err != nil {
		t.Fatalf("first replay: %v", err)
	}
	if got := store.m[state.Key{Entity: "all"}]; got != records {
		t.Fatalf("after first replay: got %d, want %d", got, records)
	}

	// Past the TTL horizon: DDB has evicted every claim, so the identical
	// archive looks brand new.
	clock.Advance(ttl + time.Minute)

	if err := replay.Run(context.Background(), newCountingPipe(store), archive(records),
		replay.WithDedup(dedup),
	); err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if got := store.m[state.Key{Entity: "all"}]; got != 2*records {
		t.Errorf("re-run past the dedup TTL: got %d, want %d (claims expire; the merge repeats)",
			got, 2*records)
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
