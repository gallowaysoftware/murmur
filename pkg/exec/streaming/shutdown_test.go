package streaming_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// countingSource emits until its context is cancelled, counting Acks. Ack is
// the source's promise that a record will never be redelivered — so an Ack for
// a record that was never merged is a permanently lost event.
type countingSource struct {
	acks  atomic.Int64
	sent  atomic.Int64
	pause time.Duration
}

func (s *countingSource) Read(ctx context.Context, out chan<- source.Record[int]) error {
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		rec := source.Record[int]{
			EventID: "evt-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26)) + string(rune('a'+(i/676)%26)),
			Value:   i,
			Ack:     func() error { s.acks.Add(1); return nil },
		}
		select {
		case out <- rec:
			s.sent.Add(1)
		case <-ctx.Done():
			return nil
		}
		if s.pause > 0 {
			time.Sleep(s.pause)
		}
	}
}
func (*countingSource) Name() string { return "counting" }
func (*countingSource) Close() error { return nil }

// ctxRespectingStore merges only while ctx is live, exactly as a real AWS SDK
// call does — the SDK returns context.Canceled the moment the context is done.
type ctxRespectingStore struct {
	mu     sync.Mutex
	merges int64
	vals   map[state.Key]int64
}

func newCtxStore() *ctxRespectingStore {
	return &ctxRespectingStore{vals: map[state.Key]int64{}}
}
func (s *ctxRespectingStore) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vals[k]
	return v, ok, nil
}
func (s *ctxRespectingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *ctxRespectingStore) MergeUpdate(ctx context.Context, k state.Key, d int64, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vals[k] += d
	s.merges++
	return nil
}
func (s *ctxRespectingStore) Close() error { return nil }
func (s *ctxRespectingStore) mergeCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.merges
}

func shutdownPipe(src source.Source[int], store state.Store[int64]) *pipeline.Pipeline[int, int64] {
	return pipeline.NewPipeline[int, int64]("shutdown").
		From(src).
		Key(func(int) string { return "k" }).
		Value(func(int) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func TestShutdown_DoesNotAckRecordsItNeverMerged(t *testing.T) {
	// Ack is a promise the record will never be redelivered. Acking a record
	// that was not merged loses it permanently.
	//
	// Before the fix, cancellation fell through the "just return" comment into
	// deadLetter + Ack, so every SIGTERM — i.e. every ECS deploy, scale-in and
	// task replacement — silently dropped whatever was in flight, and Run still
	// returned nil so the worker logged a clean exit.
	for _, conc := range []int{1, 8} {
		conc := conc
		t.Run(map[int]string{1: "sequential", 8: "concurrent"}[conc], func(t *testing.T) {
			// Repeat: at concurrency 1 only a record or two is in flight at
			// the moment of cancellation, so a single run can miss the loss.
			const rounds = 25
			for round := 0; round < rounds; round++ {
				src := &countingSource{}
				store := newCtxStore()
				var dead atomic.Int64

				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					done <- streaming.Run(ctx, shutdownPipe(src, store),
						streaming.WithConcurrency(conc),
						streaming.WithMaxAttempts(1),
						streaming.WithMetrics(metrics.NewInMemory()),
						streaming.WithDeadLetter(func(string, error) { dead.Add(1) }),
					)
				}()

				time.Sleep(40 * time.Millisecond)
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("Run did not return after cancellation")
				}

				if acks, merges := src.acks.Load(), store.mergeCount(); acks > merges {
					t.Fatalf("round %d: acked %d records but merged only %d — %d event(s) lost permanently",
						round, acks, merges, acks-merges)
				}
				if n := dead.Load(); n > 0 {
					t.Fatalf("round %d: %d record(s) dead-lettered on shutdown; "+
						"a cancelled context is not a poison record", round, n)
				}
			}
		})
	}
}

// ctxSensitiveDeduper models a real DynamoDB Deduper: every call honours the
// context, so a Release attempted with an already-cancelled context fails.
type ctxSensitiveDeduper struct {
	mu       sync.Mutex
	seen     map[string]bool
	releases int
	relFails int
}

func newCtxDeduper() *ctxSensitiveDeduper {
	return &ctxSensitiveDeduper{seen: map[string]bool{}}
}
func (d *ctxSensitiveDeduper) MarkSeen(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[id] {
		return false, nil
	}
	d.seen[id] = true
	return true, nil
}
func (d *ctxSensitiveDeduper) Release(ctx context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		d.relFails++
		return err
	}
	d.releases++
	delete(d.seen, id)
	return nil
}
func (d *ctxSensitiveDeduper) Close() error { return nil }
func (d *ctxSensitiveDeduper) claimed(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen[id]
}
func (d *ctxSensitiveDeduper) stats() (rel, fail int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.releases, d.relFails
}

func TestShutdown_DedupClaimIsReleasedEvenWhenTheContextIsCancelled(t *testing.T) {
	// The dedup-release fix is worthless if it uses the dying context: the
	// most likely reason a merge fails is that ctx was just cancelled, and a
	// Release on that same ctx fails immediately, leaving the claim standing so
	// the redelivery is dedup_skipped and the event lost.
	//
	// Drives processor.MergeMany directly with an already-cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before we even start, as at SIGTERM

	dedup := newCtxDeduper()
	rec := metrics.NewInMemory()

	if err := runMergeWithCancelledCtx(ctx, dedup, rec); err == nil {
		t.Fatal("expected the merge to fail under a cancelled context")
	}

	if dedup.claimed("evt-1") {
		t.Error("dedup claim survived a cancelled-context merge failure: " +
			"the redelivery will be dedup_skipped and the event lost")
	}
	rel, fail := dedup.stats()
	if rel != 1 {
		t.Errorf("releases: got %d, want 1 (release must not use the cancelled ctx)", rel)
	}
	if fail != 0 {
		t.Errorf("release failed %d time(s) on the cancelled context", fail)
	}
}

// runMergeWithCancelledCtx drives processor.MergeMany directly so the test
// exercises the dedup-release path without racing a live runtime.
//
// The ctx passed to MergeMany is LIVE (so its MarkSeen wins the claim, exactly
// as it does just before SIGTERM lands); the store then fails with
// context.Canceled, modelling the context dying between claim and write. That
// is the precise ordering that made the original release-on-the-dying-context
// bug invisible.
func runMergeWithCancelledCtx(ctx context.Context, d *ctxSensitiveDeduper, rec metrics.Recorder) error {
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = d
	cfg.MaxAttempts = 1

	return processor.MergeMany(context.Background(), &cfg, "test", "evt-1", time.Now(),
		[]string{"k"}, int64(1), &cancelledStore{err: ctx.Err()}, nil, nil)
}

// cancelledStore always fails with the supplied error — used to simulate "the
// context died between the dedup claim and the store write".
type cancelledStore struct{ err error }

func (*cancelledStore) Get(context.Context, state.Key) (int64, bool, error) { return 0, false, nil }
func (*cancelledStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *cancelledStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	return s.err
}
func (*cancelledStore) Close() error { return nil }
