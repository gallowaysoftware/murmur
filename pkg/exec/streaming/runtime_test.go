package streaming_test

import (
	"context"
	"errors"
	"sync"
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

// memDeduper is an in-memory implementation of state.Deduper for unit tests.
type memDeduper struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func newMemDeduper() *memDeduper { return &memDeduper{seen: map[string]struct{}{}} }
func (d *memDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, dup := d.seen[id]; dup {
		return false, nil
	}
	d.seen[id] = struct{}{}
	return true, nil
}
func (*memDeduper) Close() error { return nil }

// duplicatingSource emits each record twice — simulating a worker crash that
// causes the source to redeliver. With dedup configured the runtime should
// process each unique EventID exactly once.
type duplicatingSource struct{ n int }

func (s *duplicatingSource) Read(_ context.Context, out chan<- source.Record[int]) error {
	for round := 0; round < 2; round++ {
		for i := 0; i < s.n; i++ {
			out <- source.Record[int]{
				EventID: fakeEventID(i),
				Value:   i,
				Ack:     func() error { return nil },
			}
		}
	}
	return nil
}
func (*duplicatingSource) Name() string { return "dup" }
func (*duplicatingSource) Close() error { return nil }

func TestDedup_DuplicatesSkipped(t *testing.T) {
	store := newFlakyStore(0) // never fails
	src := &duplicatingSource{n: 3}
	rec := metrics.NewInMemory()
	dedup := newMemDeduper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, newPipe(src, store),
		streaming.WithMetrics(rec),
		streaming.WithDedup(dedup),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 6 records emitted (3 unique × 2 rounds). With dedup, exactly the 3
	// unique IDs should land in the store; each entity has count 1.
	for i := 0; i < 3; i++ {
		k := state.Key{Entity: fakeEventID(i)}
		if got := store.mu[k]; got != 1 {
			t.Errorf("entity %s after dedup: got %d, want 1", k.Entity, got)
		}
	}

	procSnap := rec.SnapshotOne("test")
	if procSnap.EventsProcessed != 3 {
		t.Errorf("processed events: got %d, want 3", procSnap.EventsProcessed)
	}
	dupSnap := rec.SnapshotOne("test:dedup_skip")
	if dupSnap.EventsProcessed != 3 {
		t.Errorf("dedup_skip counter: got %d, want 3", dupSnap.EventsProcessed)
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

// likeEvent carries the data a hierarchical-rollup test needs: the post,
// the country, and (implicitly) one "like" per event.
type likeEvent struct {
	postID  string
	country string
}

// likeSource emits a fixed batch of likeEvents and closes.
type likeSource struct {
	events []likeEvent
}

func (s *likeSource) Read(_ context.Context, out chan<- source.Record[likeEvent]) error {
	for i, e := range s.events {
		out <- source.Record[likeEvent]{
			EventID: fakeEventID(i),
			Value:   e,
			Ack:     func() error { return nil },
		}
	}
	return nil
}
func (*likeSource) Name() string { return "likes" }
func (*likeSource) Close() error { return nil }

func TestKeyByMany_HierarchicalRollups_FanOutIntoEveryLevel(t *testing.T) {
	// 5 likes on post-A: 3 from US, 1 from CA, 1 from UK.
	// 2 likes on post-B: 1 from US, 1 from UK.
	events := []likeEvent{
		{postID: "post-A", country: "US"},
		{postID: "post-A", country: "US"},
		{postID: "post-A", country: "US"},
		{postID: "post-A", country: "CA"},
		{postID: "post-A", country: "UK"},
		{postID: "post-B", country: "US"},
		{postID: "post-B", country: "UK"},
	}
	src := &likeSource{events: events}
	store := newFlakyStore(0) // fake store, no flakiness

	// 4-level hierarchy per like: post / post×country / country / global.
	pipe := pipeline.NewPipeline[likeEvent, int64]("likes").
		From(src).
		KeyByMany(func(e likeEvent) []string {
			return []string{
				"post:" + e.postID,
				"post:" + e.postID + "|country:" + e.country,
				"country:" + e.country,
				"global",
			}
		}).
		Value(func(likeEvent) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := streaming.Run(ctx, pipe); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := map[string]int64{
		"post:post-A":            5, // 5 likes on post-A
		"post:post-A|country:US": 3,
		"post:post-A|country:CA": 1,
		"post:post-A|country:UK": 1,
		"post:post-B":            2,
		"post:post-B|country:US": 1,
		"post:post-B|country:UK": 1,
		"country:US":             4,
		"country:CA":             1,
		"country:UK":             2,
		"global":                 7, // total likes
	}
	for entity, expected := range want {
		got, ok := store.mu[state.Key{Entity: entity}]
		if !ok {
			t.Errorf("entity %q: missing from store", entity)
			continue
		}
		if got != expected {
			t.Errorf("entity %q: got %d, want %d", entity, got, expected)
		}
	}
}
