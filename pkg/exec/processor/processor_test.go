package processor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// fakeStore is an in-memory state.Store[int64].
type fakeStore struct {
	mu sync.Mutex
	m  map[state.Key]int64
}

func (s *fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *fakeStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}
func (s *fakeStore) Close() error { return nil }

// flakyStore returns errFlaky N times per key, then succeeds.
type flakyStore struct {
	mu       sync.Mutex
	m        map[state.Key]int64
	attempts int
	failures int
}

var errFlaky = errors.New("flaky")

func (s *flakyStore) Get(context.Context, state.Key) (int64, bool, error) { return 0, false, nil }
func (s *flakyStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *flakyStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts <= s.failures {
		return errFlaky
	}
	if s.m == nil {
		s.m = map[state.Key]int64{}
	}
	s.m[k] += d
	return nil
}
func (*flakyStore) Close() error { return nil }

// memDeduper.
type memDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
	err  error
}

func (d *memDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	if d.err != nil {
		return false, d.err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = map[string]bool{}
	}
	if d.seen[id] {
		return false, nil
	}
	d.seen[id] = true
	return true, nil
}
func (*memDeduper) Close() error { return nil }

func key(s string) func(string) string { return func(string) string { return s } }
func one(string) int64                 { return 1 }

func TestMergeOne_HappyPath(t *testing.T) {
	store := &fakeStore{m: map[state.Key]int64{}}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec

	if err := processor.MergeOne(context.Background(), &cfg, "test", "ev1", time.Now(),
		"hello", key("a"), one, store, nil, nil); err != nil {
		t.Fatalf("MergeOne: %v", err)
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("merged value: got %d, want 1", got)
	}
	if rec.SnapshotOne("test").EventsProcessed != 1 {
		t.Errorf("event count: got %d, want 1", rec.SnapshotOne("test").EventsProcessed)
	}
}

func TestMergeOne_RetriesAndRecovers(t *testing.T) {
	store := &flakyStore{failures: 2}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 3
	cfg.BackoffBase = time.Millisecond
	cfg.BackoffMax = time.Millisecond

	if err := processor.MergeOne(context.Background(), &cfg, "test", "ev1", time.Now(),
		"hello", key("a"), one, store, nil, nil); err != nil {
		t.Fatalf("MergeOne: %v", err)
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("merged value: got %d, want 1", got)
	}
	if store.attempts != 3 {
		t.Errorf("attempts: got %d, want 3", store.attempts)
	}
}

func TestMergeOne_RetryExhaustionReturnsError(t *testing.T) {
	store := &flakyStore{failures: 99}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 2
	cfg.BackoffBase = time.Millisecond
	cfg.BackoffMax = time.Millisecond

	err := processor.MergeOne(context.Background(), &cfg, "test", "ev1", time.Now(),
		"hello", key("a"), one, store, nil, nil)
	if err == nil {
		t.Fatal("expected non-nil error after exhausted retries")
	}
	if !errors.Is(err, errFlaky) {
		t.Errorf("error chain: got %v, want errFlaky", err)
	}
}

func TestMergeOne_DedupSkip(t *testing.T) {
	store := &fakeStore{m: map[state.Key]int64{}}
	dedup := &memDeduper{}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup

	for i := 0; i < 5; i++ {
		if err := processor.MergeOne(context.Background(), &cfg, "test", "same-id", time.Now(),
			"hello", key("a"), one, store, nil, nil); err != nil {
			t.Fatalf("invocation %d: %v", i, err)
		}
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("merged value after 5 dedup-protected calls: got %d, want 1", got)
	}
	if rec.SnapshotOne("test:dedup_skip").EventsProcessed != 4 {
		t.Errorf("dedup_skip events: got %d, want 4",
			rec.SnapshotOne("test:dedup_skip").EventsProcessed)
	}
}

func TestMergeOne_DedupBackendFailure_DoesNotSkipMerge(t *testing.T) {
	// If the dedup backend errors, we MUST fall through to the merge so a
	// dedup-table outage doesn't silently drop events. This is the
	// fail-open policy.
	store := &fakeStore{m: map[state.Key]int64{}}
	dedup := &memDeduper{err: errors.New("dedup down")}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup

	if err := processor.MergeOne(context.Background(), &cfg, "test", "ev1", time.Now(),
		"hello", key("a"), one, store, nil, nil); err != nil {
		t.Fatalf("MergeOne: %v", err)
	}
	if got := store.m[state.Key{Entity: "a"}]; got != 1 {
		t.Errorf("merge did not run when dedup backend failed: got %d, want 1", got)
	}
	if rec.SnapshotOne("test").Errors == 0 {
		t.Error("expected dedup error to be recorded")
	}
}

func TestMergeOne_ContextCancelDuringBackoff(t *testing.T) {
	store := &flakyStore{failures: 99}
	cfg := processor.Defaults()
	cfg.MaxAttempts = 5
	cfg.BackoffBase = 100 * time.Millisecond
	cfg.BackoffMax = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := processor.MergeOne(ctx, &cfg, "test", "ev1", time.Now(),
		"hello", key("a"), one, store, nil, nil)
	if err == nil {
		t.Fatal("expected error on canceled context")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("MergeOne kept retrying after cancel: elapsed %v", elapsed)
	}
}

func TestDefaults(t *testing.T) {
	c := processor.Defaults()
	if c.Recorder == nil {
		t.Error("Defaults: Recorder is nil")
	}
	if c.MaxAttempts < 1 {
		t.Errorf("Defaults: MaxAttempts=%d, want >= 1", c.MaxAttempts)
	}
	if c.BackoffBase <= 0 || c.BackoffMax <= 0 {
		t.Errorf("Defaults: backoff = %v / %v, both must be positive", c.BackoffBase, c.BackoffMax)
	}
	if c.Dedup != nil {
		t.Error("Defaults: Dedup should be nil unless set explicitly")
	}
}
