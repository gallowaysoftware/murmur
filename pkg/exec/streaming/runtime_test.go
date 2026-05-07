package streaming_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// flakySource emits a fixed number of records and then closes its `out` channel.
// Each record has a distinct EventID so the dead-letter path can identify them.
type flakySource struct {
	n int
}

func (s *flakySource) Read(_ context.Context, out chan<- source.Record[int]) error {
	for i := 0; i < s.n; i++ {
		out <- source.Record[int]{
			EventID: fakeEventID(i),
			Value:   i,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*flakySource) Name() string { return "flaky" }
func (*flakySource) Close() error { return nil }

func fakeEventID(i int) string { return "e-" + string(rune('a'+i)) }

// flakyStore returns ErrFlaky on the first `failuresEach` calls per key, then
// succeeds. Lets us simulate transient throttle without a real DDB.
type flakyStore struct {
	mu           map[state.Key]int64
	attempts     map[state.Key]int
	failuresEach int
}

var errFlaky = errors.New("flaky store: transient error")

func newFlakyStore(failuresEach int) *flakyStore {
	return &flakyStore{
		mu:           map[state.Key]int64{},
		attempts:     map[state.Key]int{},
		failuresEach: failuresEach,
	}
}
func (s *flakyStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	v, ok := s.mu[k]
	return v, ok, nil
}
func (s *flakyStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s.mu[k]
	}
	return vs, oks, nil
}
func (s *flakyStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s.attempts[k]++
	if s.attempts[k] <= s.failuresEach {
		return errFlaky
	}
	s.mu[k] += d
	return nil
}
func (*flakyStore) Close() error { return nil }

func newPipe(src source.Source[int], store state.Store[int64]) *pipeline.Pipeline[int, int64] {
	return pipeline.NewPipeline[int, int64]("test").
		From(src).
		Key(func(i int) string { return fakeEventID(i) }).
		Value(func(int) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func TestRetry_RecoversBeforeMaxAttempts(t *testing.T) {
	// Every record fails twice, succeeds on the third attempt. With
	// MaxAttempts=3 every record should land successfully.
	store := newFlakyStore(2)
	src := &flakySource{n: 4}
	rec := metrics.NewInMemory()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, newPipe(src, store),
		streaming.WithMetrics(rec),
		streaming.WithMaxAttempts(3),
		streaming.WithRetryBackoff(1*time.Millisecond, 5*time.Millisecond),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i := 0; i < 4; i++ {
		k := state.Key{Entity: fakeEventID(i)}
		if got := store.mu[k]; got != 1 {
			t.Errorf("entity %s: got %d, want 1", k.Entity, got)
		}
	}
	snap := rec.SnapshotOne("test")
	if snap.EventsProcessed != 4 {
		t.Errorf("events processed: got %d, want 4", snap.EventsProcessed)
	}
	// Each successful record retried twice → 8 retries total.
	retrySnap := rec.SnapshotOne("test:retry")
	if retrySnap.EventsProcessed != 8 {
		t.Errorf("retry counter: got %d, want 8", retrySnap.EventsProcessed)
	}
	dlSnap := rec.SnapshotOne("test:dead_letter")
	if dlSnap.EventsProcessed != 0 {
		t.Errorf("dead-letter counter: got %d, want 0", dlSnap.EventsProcessed)
	}
}

func TestRetry_DeadLettersOnPermaFail(t *testing.T) {
	// Records fail forever; runtime should dead-letter and keep going for
	// every record rather than crashing.
	store := newFlakyStore(999)
	src := &flakySource{n: 3}
	rec := metrics.NewInMemory()

	var dlqHits atomic.Int64
	var dlqIDs []string
	dlq := func(eventID string, _ error) {
		dlqHits.Add(1)
		dlqIDs = append(dlqIDs, eventID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, newPipe(src, store),
		streaming.WithMetrics(rec),
		streaming.WithMaxAttempts(2),
		streaming.WithRetryBackoff(1*time.Millisecond, 2*time.Millisecond),
		streaming.WithDeadLetter(dlq),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if dlqHits.Load() != 3 {
		t.Errorf("dead-letter callback: got %d hits, want 3", dlqHits.Load())
	}
	want := []string{fakeEventID(0), fakeEventID(1), fakeEventID(2)}
	for i, w := range want {
		if i >= len(dlqIDs) || dlqIDs[i] != w {
			t.Errorf("dlq[%d]: got %q, want %q", i, dlqIDs[i], w)
		}
	}
	dlSnap := rec.SnapshotOne("test:dead_letter")
	if dlSnap.EventsProcessed != 3 {
		t.Errorf("dead-letter counter: got %d, want 3", dlSnap.EventsProcessed)
	}
	// No record should have made it through.
	for i := 0; i < 3; i++ {
		k := state.Key{Entity: fakeEventID(i)}
		if got := store.mu[k]; got != 0 {
			t.Errorf("entity %s: got %d, want 0 (everything dead-lettered)", k.Entity, got)
		}
	}
}

func TestRetry_RespectsContextCancel(t *testing.T) {
	// Even a perma-failing store shouldn't hang if ctx cancels mid-backoff.
	store := newFlakyStore(999)
	src := &flakySource{n: 1}
	rec := metrics.NewInMemory()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Big backoff so the cancel happens during sleep, not during processOne.
	start := time.Now()
	_ = streaming.Run(ctx, newPipe(src, store),
		streaming.WithMetrics(rec),
		streaming.WithMaxAttempts(20),
		streaming.WithRetryBackoff(1*time.Second, 10*time.Second),
	)
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Errorf("ctx-cancel didn't preempt backoff; elapsed=%v", elapsed)
	}
}
