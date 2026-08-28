package streaming_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/streaming"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/pipeline"
	"github.com/gallowaysoftware/murmur/pkg/source"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

var errStoreDown = errors.New("store down")

// deadStore fails every write, the way a throttled or unreachable table does.
// Mutex-guarded because the aggregator's ticker drain and its end-of-source
// drain can both be in flight.
type deadStore struct {
	mu    sync.Mutex
	calls int
}

func (*deadStore) Get(context.Context, state.Key) (int64, bool, error) { return 0, false, nil }
func (*deadStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (s *deadStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return errStoreDown
}
func (*deadStore) Close() error { return nil }

func hotKeyPipeline(src source.Source[likeEvent], store state.Store[int64]) *pipeline.Pipeline[likeEvent, int64] {
	return pipeline.NewPipeline[likeEvent, int64]("likes").
		From(src).
		Key(func(e likeEvent) string { return e.postID }).
		Value(func(likeEvent) int64 { return 1 }).
		Aggregate(core.Sum[int64]()).
		StoreIn(store)
}

func TestBatchWindow_DeadLetteredFlushReleasesDedupClaims(t *testing.T) {
	// WithBatchWindow claims each record's EventID at accept time — a whole flush
	// window before the delta reaches the store. When the flush then dead-letters,
	// a claim left standing turns the DLQ replay the operator is being handed into
	// a run of dedup_skips: the counts are gone, with no error and no metric.
	const n = 200
	dedup := newMemDeduper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var dead int
	var deadMu sync.Mutex
	down := &deadStore{}
	err := streaming.Run(ctx, hotKeyPipeline(&hotKeySource{n: n, postID: "celeb-post"}, down),
		streaming.WithDedup(dedup),
		streaming.WithBatchWindow(50*time.Millisecond, 100000),
		streaming.WithMaxAttempts(1),
		streaming.WithRetryBackoff(time.Millisecond, time.Millisecond),
		streaming.WithDeadLetter(func(string, error) {
			deadMu.Lock()
			dead++
			deadMu.Unlock()
		}),
	)
	if err != nil {
		t.Fatalf("Run against a dead store: %v", err)
	}
	if down.calls == 0 {
		t.Fatal("the store was never written to; the batch never flushed")
	}
	deadMu.Lock()
	gotDead := dead
	deadMu.Unlock()
	if gotDead != n {
		t.Fatalf("dead-lettered records: got %d, want %d", gotDead, n)
	}

	if got := dedup.releaseCount(); got != n {
		t.Errorf("Release calls after a dead-lettered flush: got %d, want %d", got, n)
	}
	if got := dedup.claimedCount(); got != 0 {
		t.Errorf("claims still outstanding after a dead-lettered flush: got %d, want 0 "+
			"(a replay of those EventIDs would be skipped and the events lost)", got)
	}

	// Replaying the dead-lettered EventIDs through a healthy store must land all
	// 200 — that is the whole point of releasing.
	live := newCountingStore()
	if err := streaming.Run(ctx, hotKeyPipeline(&hotKeySource{n: n, postID: "celeb-post"}, live),
		streaming.WithDedup(dedup),
		streaming.WithBatchWindow(50*time.Millisecond, 100000),
	); err != nil {
		t.Fatalf("replay Run: %v", err)
	}
	if got := live.values[state.Key{Entity: "celeb-post"}]; got != n {
		t.Errorf("replayed total: got %d, want %d", got, n)
	}
}

func TestBatchWindow_UnwonClaimIsNeverReleased(t *testing.T) {
	// MarkSeen is fail-open: a dedup-backend error accepts the record anyway,
	// WITHOUT owning the claim. A dead-lettered flush has to release exactly the
	// claims this worker won and no others — releasing an unwon one deletes a row
	// another worker holds, letting a third delivery re-apply the event over the
	// winner's write. Ownership is therefore per-EventID, not per-batch.
	const n = 20
	unclaimable := map[string]bool{}
	for i := 1; i < n; i += 2 {
		unclaimable[fmt.Sprintf("ev-%d", i)] = true // matches hotKeySource's IDs
	}
	dedup := &partialClaimDeduper{unclaimable: unclaimable, seen: map[string]bool{}}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := streaming.Run(ctx, hotKeyPipeline(&hotKeySource{n: n, postID: "celeb-post"}, &deadStore{}),
		streaming.WithDedup(dedup),
		streaming.WithBatchWindow(50*time.Millisecond, 100000),
		streaming.WithMaxAttempts(1),
		streaming.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	released := dedup.releasedIDs()
	if len(released) != n/2 {
		t.Errorf("Release calls: got %d, want %d (only the claims we won)", len(released), n/2)
	}
	for _, id := range released {
		if unclaimable[id] {
			t.Errorf("released %q, a claim this worker never won: that drops the "+
				"winner's row and lets a third delivery re-apply the event", id)
		}
	}
}

// partialClaimDeduper wins the claim for every EventID except those in
// unclaimable, where MarkSeen reports a backend error — the fail-open path, where
// the runtime proceeds without owning the claim.
type partialClaimDeduper struct {
	mu          sync.Mutex
	unclaimable map[string]bool
	seen        map[string]bool
	released    []string
}

func (d *partialClaimDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.unclaimable[id] {
		return false, errors.New("dedup table throttled")
	}
	if d.seen[id] {
		return false, nil
	}
	d.seen[id] = true
	return true, nil
}

func (d *partialClaimDeduper) Release(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.released = append(d.released, id)
	delete(d.seen, id)
	return nil
}

func (*partialClaimDeduper) Close() error { return nil }

func (d *partialClaimDeduper) releasedIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.released...)
}
