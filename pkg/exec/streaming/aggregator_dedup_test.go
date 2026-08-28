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

// Release honours ctx the way the real dynamodb.Deduper does: it issues a
// DeleteItem, and a DeleteItem on a cancelled context fails without touching the
// table. Fakes that ignore ctx here silently bless a release that would never
// have landed in production — which is the entire reason ReleaseClaims detaches
// from the caller's context before releasing.
func (d *partialClaimDeduper) Release(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dedup Release %q: %w", id, err)
	}
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

// cancelDuringFlushStore blocks its first write until the run context is
// cancelled, then fails it — the exact shape of a SIGTERM landing on an in-flight
// flush, which is the likeliest way a flush fails at all. Later writes fail
// immediately so the shutdown drain cannot deadlock on it.
type cancelDuringFlushStore struct {
	entered chan struct{}
	once    sync.Once
}

func (*cancelDuringFlushStore) Get(context.Context, state.Key) (int64, bool, error) {
	return 0, false, nil
}

func (*cancelDuringFlushStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (s *cancelDuringFlushStore) MergeUpdate(ctx context.Context, _ state.Key, _ int64, _ time.Duration) error {
	s.once.Do(func() {
		close(s.entered)
		<-ctx.Done()
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	return errStoreDown
}

func (*cancelDuringFlushStore) Close() error { return nil }

func TestBatchWindow_ClaimsComeBackWhenCancellationKillsTheFlush(t *testing.T) {
	// The aggregator half of the detached-release contract. accept() claims the
	// EventID a whole flush window before the durable write; if the write dies
	// because ctx was cancelled, a release issued on that same ctx fails on arrival
	// and the claim outlives the delta it was taken for. Nothing downstream ever
	// learns: the redelivery just hits dedup_skip.
	dedup := newMemDeduper()
	store := &cancelDuringFlushStore{entered: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-store.entered:
			cancel()
		case <-time.After(10 * time.Second):
		}
	}()

	// maxBatch=1 makes accept flush the batch synchronously, so the cancellation
	// lands on a flush we know is in flight. The window is long enough that the
	// ticker never competes for the same batch.
	err := streaming.Run(ctx, hotKeyPipeline(&hotKeySource{n: 1, postID: "celeb-post"}, store),
		streaming.WithDedup(dedup),
		streaming.WithBatchWindow(time.Hour, 1),
		streaming.WithMaxAttempts(1),
		streaming.WithRetryBackoff(time.Millisecond, time.Millisecond),
	)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}

	if got := dedup.releaseCount(); got != 1 {
		t.Errorf("claims handed back after cancellation killed the flush: got %d, want 1", got)
	}
	if got := dedup.claimedCount(); got != 0 {
		t.Errorf("claims still outstanding: got %d, want 0 (the event's delta never "+
			"reached the store, and its redelivery will now be skipped)", got)
	}
}

// budgetDeduper records the deadline of the context each Release is handed. One
// ReleaseClaims call is one context with one deadline, so the number of DISTINCT
// deadlines seen across a drain is the number of release budgets that drain paid
// for.
type budgetDeduper struct {
	mu        sync.Mutex
	seen      map[string]struct{}
	released  int
	deadlines map[time.Time]int
}

func newBudgetDeduper() *budgetDeduper {
	return &budgetDeduper{seen: map[string]struct{}{}, deadlines: map[time.Time]int{}}
}

func (d *budgetDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, dup := d.seen[id]; dup {
		return false, nil
	}
	d.seen[id] = struct{}{}
	return true, nil
}

func (d *budgetDeduper) Release(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dedup Release %q: %w", id, err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("dedup Release %q: release context carries no deadline", id)
	}
	// Cost a little wall-clock so two consecutive budgets cannot be minted inside
	// the same clock tick and read as one.
	time.Sleep(50 * time.Microsecond)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deadlines[deadline]++
	d.released++
	delete(d.seen, id)
	return nil
}

func (*budgetDeduper) Close() error { return nil }

func (d *budgetDeduper) releaseCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.released
}

func (d *budgetDeduper) claimedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

// budgetCount is how many distinct release budgets were spent.
func (d *budgetDeduper) budgetCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.deadlines)
}

// multiKeySource emits perKey records for each of `keys` distinct entities and
// then parks until released, so every record sits in its own aggregator batch and
// the only flush that ever runs is the shutdown drain.
type multiKeySource struct {
	keys    int
	perKey  int
	release chan struct{}
}

func (s *multiKeySource) Read(_ context.Context, out chan<- source.Record[likeEvent]) error {
	for k := 0; k < s.keys; k++ {
		for i := 0; i < s.perKey; i++ {
			out <- source.Record[likeEvent]{
				EventID: fmt.Sprintf("mk-%d-%d", k, i),
				Value:   likeEvent{postID: fmt.Sprintf("post-%d", k), country: "US"},
				Ack:     func() error { return nil },
			}
		}
	}
	<-s.release
	return nil
}

func (*multiKeySource) Name() string { return "multikey" }
func (*multiKeySource) Close() error { return nil }

func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchWindow_ShutdownDrainSpendsOneReleaseBudget(t *testing.T) {
	// A shutdown drain walks every batch the worker is holding. Releasing per batch
	// buys a fresh multi-second budget K times over, so a drain across K failed
	// batches can run K × budget — well past any SIGTERM grace period, at which
	// point the process dies with the tail of its claims never handed back. The
	// whole drain has to cost one budget.
	const (
		keys   = 8
		perKey = 5
	)
	dedup := newBudgetDeduper()
	src := &multiKeySource{keys: keys, perKey: perKey, release: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		// A one-hour window means no ticker flush; the only drain is the one
		// cancellation triggers, which is the case under test.
		runErr <- streaming.Run(ctx, hotKeyPipeline(src, &deadStore{}),
			streaming.WithDedup(dedup),
			streaming.WithBatchWindow(time.Hour, 1_000_000),
			streaming.WithMaxAttempts(1),
			streaming.WithRetryBackoff(time.Millisecond, time.Millisecond),
		)
	}()

	waitForCond(t, "every record to be accepted and claimed",
		func() bool { return dedup.claimedCount() == keys*perKey })
	cancel()
	waitForCond(t, "the shutdown drain to hand every claim back",
		func() bool { return dedup.releaseCount() == keys*perKey })

	if got := dedup.budgetCount(); got != 1 {
		t.Errorf("release budgets spent by one shutdown drain: got %d, want 1 "+
			"(%d failed batches paying a multi-second budget each is not a bounded "+
			"shutdown; the batches the drain never reaches leak their claims)",
			got, keys)
	}
	if got := dedup.claimedCount(); got != 0 {
		t.Errorf("claims still outstanding after the drain: got %d, want 0", got)
	}

	close(src.release)
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the source was released")
	}
}
