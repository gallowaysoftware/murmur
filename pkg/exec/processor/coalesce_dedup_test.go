package processor_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gallowaysoftware/murmur/pkg/exec/processor"
	"github.com/gallowaysoftware/murmur/pkg/metrics"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// partialClaimDeduper wins the claim for every EventID except those in unclaimable,
// where MarkSeen reports a backend error. That is the fail-open path: the caller
// proceeds WITHOUT owning the claim, and must not release what it does not own.
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

// hotKeyEventIDs returns n synthetic EventIDs, all destined for the same key.
func hotKeyEventIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "evt-" + intToKey(i)
	}
	return ids
}

// addHotKey folds one unit per EventID into a single hot key.
func addHotKey(t *testing.T, ctx context.Context, c *processor.Coalescer[int64], ids []string) {
	t.Helper()
	for _, id := range ids {
		if err := c.AddMany(ctx, id, time.Now(), []string{"hot"}, int64(1)); err != nil {
			t.Fatalf("AddMany %q: %v", id, err)
		}
	}
}

func TestCoalescer_FlushFailureReleasesDedupClaims(t *testing.T) {
	// The claim is taken at AddMany time, a whole batch before the durable write.
	// If a failed flush leaves it standing, the redelivery that the FlushError is
	// asking the driver for arrives to a dedup_skip and every event in the batch
	// is lost permanently — silent count loss for Sum / HLL / TopK.
	const n = 100
	ids := hotKeyEventIDs(n)

	dedup := &memDeduper{}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup
	cfg.MaxAttempts = 1

	ctx := context.Background()
	down := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](&failingStore{}), nil, nil)
	addHotKey(t, ctx, down, ids)

	if err := down.Flush(ctx); err == nil {
		t.Fatal("expected the flush to fail while the store is down")
	}

	if dedup.releases != n {
		t.Errorf("Release calls after a failed flush: got %d, want %d", dedup.releases, n)
	}
	for _, id := range ids {
		if dedup.claimed(id) {
			t.Fatalf("EventID %q is still claimed after a failed flush: the "+
				"redelivery will be skipped and the event lost forever", id)
		}
	}
	if got := rec.SnapshotOne("test:dedup_release").EventsProcessed; got != n {
		t.Errorf("dedup_release events: got %d, want %d", got, n)
	}

	// The redelivery against a healthy store has to land all 100 units.
	live := newCountingStore()
	replay := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](live), nil, nil)
	addHotKey(t, ctx, replay, ids)
	if err := replay.Flush(ctx); err != nil {
		t.Fatalf("replay flush after released claims: %v", err)
	}
	if got := live.m[state.Key{Entity: "hot"}]; got != n {
		t.Errorf("replayed total: got %d, want %d", got, n)
	}
}

func TestCoalescer_CancelledFlushReleasesDedupClaims(t *testing.T) {
	// The cancel branch drops the pending map on the floor and leans on
	// at-least-once redelivery to re-emit it. That only works if the claims go
	// back too — and cancellation is the likeliest way a flush fails, since it is
	// what SIGTERM looks like from in here.
	const n = 100
	ids := hotKeyEventIDs(n)

	dedup := &memDeduper{}
	cfg := processor.Defaults()
	cfg.Recorder = metrics.NewInMemory()
	cfg.Dedup = dedup
	cfg.MaxAttempts = 1

	ctx, cancel := context.WithCancel(context.Background())
	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](&cancellingStore{}), nil, nil)
	addHotKey(t, ctx, c, ids)
	cancel()

	err := c.Flush(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if dedup.releases != n {
		t.Errorf("Release calls after a cancelled flush: got %d, want %d", dedup.releases, n)
	}
	for _, id := range ids {
		if dedup.claimed(id) {
			t.Fatalf("EventID %q is still claimed after a cancelled flush", id)
		}
	}

	// Post-shutdown redelivery against a healthy store and a live context.
	live := newCountingStore()
	replay := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](live), nil, nil)
	addHotKey(t, context.Background(), replay, ids)
	if err := replay.Flush(context.Background()); err != nil {
		t.Fatalf("replay flush after released claims: %v", err)
	}
	if got := live.m[state.Key{Entity: "hot"}]; got != n {
		t.Errorf("replayed total: got %d, want %d", got, n)
	}
}

func TestCoalescer_UnwonClaimIsNeverReleased(t *testing.T) {
	// MarkSeen is fail-open: a dedup-backend error buffers the event anyway,
	// WITHOUT owning the claim. Releasing on that path would delete a row some
	// other worker won, letting a third delivery re-apply the event on top of the
	// winner's write.
	const n = 10
	ids := hotKeyEventIDs(n)
	unclaimable := map[string]bool{}
	for i, id := range ids {
		if i%2 == 1 {
			unclaimable[id] = true
		}
	}

	dedup := &partialClaimDeduper{unclaimable: unclaimable, seen: map[string]bool{}}
	cfg := processor.Defaults()
	cfg.Recorder = metrics.NewInMemory()
	cfg.Dedup = dedup
	cfg.MaxAttempts = 1

	ctx := context.Background()
	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](&failingStore{}), nil, nil)
	addHotKey(t, ctx, c, ids)

	if err := c.Flush(ctx); err == nil {
		t.Fatal("expected the flush to fail while the store is down")
	}

	if got := len(dedup.released); got != n/2 {
		t.Errorf("Release calls: got %d, want %d (only the claims we won)", got, n/2)
	}
	for _, id := range dedup.released {
		if unclaimable[id] {
			t.Errorf("released %q, a claim this coalescer never won: that drops "+
				"the winner's row and lets a third delivery re-apply the event", id)
		}
	}
}

// latentDeduper models a dedup backend with real per-call latency. The production
// Deduper is a DynamoDB DeleteItem — a network round trip, not a map write — and
// the release budget only has to be sized correctly because each release costs
// something. MarkSeen stays instant so the test's setup does not pay for it; the
// budget under test is the release one.
type latentDeduper struct {
	latency time.Duration

	mu       sync.Mutex
	seen     map[string]bool
	released int
}

func newLatentDeduper(latency time.Duration) *latentDeduper {
	return &latentDeduper{latency: latency, seen: map[string]bool{}}
}

func (d *latentDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[id] {
		return false, nil
	}
	d.seen[id] = true
	return true, nil
}

func (d *latentDeduper) Release(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dedup Release %q: %w", id, err)
	}
	time.Sleep(d.latency)
	// The budget can expire while we are on the wire, exactly as a DeleteItem can.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dedup Release %q: %w", id, err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.released++
	delete(d.seen, id)
	return nil
}

func (*latentDeduper) Close() error { return nil }

func (d *latentDeduper) releaseCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.released
}

func (d *latentDeduper) outstanding() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

// keyFailingStore fails MergeUpdate for exactly one entity and serves every other
// key normally — a hot partition being throttled while its siblings are fine.
type keyFailingStore struct {
	failEntity string

	mu sync.Mutex
	m  map[state.Key]int64
}

func newKeyFailingStore(failEntity string) *keyFailingStore {
	return &keyFailingStore{failEntity: failEntity, m: map[state.Key]int64{}}
}

func (s *keyFailingStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}

func (s *keyFailingStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}

func (s *keyFailingStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	if k.Entity == s.failEntity {
		return errFail
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] += d
	return nil
}

func (s *keyFailingStore) Close() error { return nil }

func (s *keyFailingStore) value(k state.Key) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[k]
}

func TestCoalescer_LargeFailedFlushReleasesEveryClaim(t *testing.T) {
	// claimedIDs holds one entry per EVENT, not per key: a single hot key under a
	// 1s FlushTick at a few thousand events/sec, or a buffer that fills to
	// DefaultMaxKeys, hands ReleaseClaims thousands of IDs in one call. A release
	// budget that does not scale with that count covers the first few hundred and
	// silently strands the rest — and a stranded claim is an event that redelivery
	// can never re-apply.
	const n = processor.DefaultMaxKeys
	ids := hotKeyEventIDs(n)

	dedup := newLatentDeduper(time.Millisecond)
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup
	cfg.MaxAttempts = 1

	ctx := context.Background()
	// One key, n events — so the flush is a single failing store call and the whole
	// cost under test is the release of n claims.
	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](&failingStore{}), nil, nil)
	addHotKey(t, ctx, c, ids)

	if err := c.Flush(ctx); err == nil {
		t.Fatal("expected the flush to fail while the store is down")
	}

	if got := dedup.releaseCount(); got != n {
		t.Errorf("claims handed back after a failed flush of %d events: got %d, want %d "+
			"(%d claims outlived the deltas they were taken for; every one of those "+
			"events is lost the moment it is redelivered)", n, got, n, n-got)
	}
	if got := dedup.outstanding(); got != 0 {
		t.Errorf("claims still outstanding: got %d, want 0", got)
	}
	if got := rec.SnapshotOne("test:dedup_release_failed").EventsProcessed; got != 0 {
		t.Errorf("dedup_release_failed events: got %d, want 0", got)
	}
	if got := rec.SnapshotOne("test:dedup_release").EventsProcessed; got != n {
		t.Errorf("dedup_release events: got %d, want %d", got, n)
	}
}

func TestCoalescer_MixedOutcomeFlushReleasesOnlyTheFailedKeysClaims(t *testing.T) {
	// One key lands, a sibling key fails, and their contributing events are
	// DISJOINT. Release has to be decided per key outcome: hand back the whole
	// pending map and the events that already reached the store get redelivered on
	// top of a write that succeeded, which over-counts the key that was never in
	// trouble.
	const per = 5
	goodIDs := make([]string, per)
	badIDs := make([]string, per)
	for i := 0; i < per; i++ {
		goodIDs[i] = "good-" + intToKey(i)
		badIDs[i] = "bad-" + intToKey(i)
	}

	dedup := &memDeduper{}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup
	cfg.MaxAttempts = 1

	ctx := context.Background()
	store := newKeyFailingStore("bad")
	c := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](store), nil, nil)
	for i := 0; i < per; i++ {
		if err := c.AddMany(ctx, goodIDs[i], time.Now(), []string{"good"}, int64(1)); err != nil {
			t.Fatalf("AddMany %q: %v", goodIDs[i], err)
		}
		if err := c.AddMany(ctx, badIDs[i], time.Now(), []string{"bad"}, int64(1)); err != nil {
			t.Fatalf("AddMany %q: %v", badIDs[i], err)
		}
	}

	err := c.Flush(ctx)
	var fe *processor.FlushError
	if !errors.As(err, &fe) {
		t.Fatalf("Flush error: got %v, want a *FlushError", err)
	}
	if len(fe.FailedKeys) != 1 || fe.FailedKeys[0].Key.Entity != "bad" {
		t.Fatalf("failed keys: got %+v, want exactly one entry for %q", fe.FailedKeys, "bad")
	}
	if got := store.value(state.Key{Entity: "good"}); got != per {
		t.Fatalf("the surviving key did not land: got %d, want %d", got, per)
	}

	for _, id := range badIDs {
		if dedup.claimed(id) {
			t.Errorf("event %q contributed only to the FAILED key and is still claimed: "+
				"its redelivery will be skipped and the event lost", id)
		}
	}
	for _, id := range goodIDs {
		if !dedup.claimed(id) {
			t.Errorf("event %q reached the store on key %q, but its claim was handed back: "+
				"the redelivery that release invites re-applies the delta and over-counts %q",
				id, "good", "good")
		}
	}
	if got := rec.SnapshotOne("test:dedup_release").EventsProcessed; got != per {
		t.Errorf("dedup_release events: got %d, want %d (one per event on the failed key only)",
			got, per)
	}

	// The redelivery settles it. Replaying every EventID through a healthy store
	// must add the failed key's units and NOTHING to the key that already landed.
	live := newCountingStore()
	replay := processor.NewCoalescer(&cfg, processor.DefaultCoalesceConfig(), "test",
		core.Count(), state.Store[int64](live), nil, nil)
	for i := 0; i < per; i++ {
		if err := replay.AddMany(ctx, goodIDs[i], time.Now(), []string{"good"}, int64(1)); err != nil {
			t.Fatalf("replay AddMany %q: %v", goodIDs[i], err)
		}
		if err := replay.AddMany(ctx, badIDs[i], time.Now(), []string{"bad"}, int64(1)); err != nil {
			t.Fatalf("replay AddMany %q: %v", badIDs[i], err)
		}
	}
	if err := replay.Flush(ctx); err != nil {
		t.Fatalf("replay flush: %v", err)
	}
	if got := live.m[state.Key{Entity: "bad"}]; got != per {
		t.Errorf("replayed units on the failed key: got %d, want %d", got, per)
	}
	if got := live.m[state.Key{Entity: "good"}]; got != 0 {
		t.Errorf("replayed units on the key that already succeeded: got %d, want 0 "+
			"(those claims were released even though their deltas landed, so the "+
			"redelivery double-counted %q)", got, "good")
	}
}

func TestReleaseClaims_ReleasesOnACancelledContext(t *testing.T) {
	// The direct guard on the shared helper both batching paths call. The single
	// likeliest reason a merge failed is that ctx was just cancelled by SIGTERM, so
	// a release issued on the caller's own context fails on arrival and the claim
	// survives exactly the shutdown it most needs not to survive.
	dedup := &memDeduper{seen: map[string]bool{"ev-1": true, "ev-2": true}}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	processor.ReleaseClaims(ctx, &cfg, "test", []string{"ev-1", "ev-2"})

	for _, id := range []string{"ev-1", "ev-2"} {
		if dedup.claimed(id) {
			t.Errorf("%q is still claimed after ReleaseClaims on a cancelled context: "+
				"its redelivery will be skipped and the event lost", id)
		}
	}
	if got := rec.SnapshotOne("test:dedup_release").EventsProcessed; got != 2 {
		t.Errorf("dedup_release events: got %d, want 2", got)
	}
}

func TestReleaseClaims_ReleasesRepeatedIDsOnce(t *testing.T) {
	// An event fanned out over several failing keys arrives once per key. The
	// budget is sized off the count, so the de-duplication has to happen before the
	// budget is chosen, not while spending it.
	dedup := &memDeduper{seen: map[string]bool{"ev-1": true}}
	rec := metrics.NewInMemory()
	cfg := processor.Defaults()
	cfg.Recorder = rec
	cfg.Dedup = dedup

	processor.ReleaseClaims(context.Background(), &cfg, "test",
		[]string{"ev-1", "", "ev-1", "ev-1"})

	if dedup.releases != 1 {
		t.Errorf("Release calls: got %d, want 1", dedup.releases)
	}
	if got := rec.SnapshotOne("test:dedup_release").EventsProcessed; got != 1 {
		t.Errorf("dedup_release events: got %d, want 1", got)
	}
}

// deadlineDeduper records the deadline carried by the context ReleaseClaims hands
// it. Deadline-minus-call-time is the budget ReleaseClaims sized for the batch,
// readable without paying for it.
type deadlineDeduper struct {
	mu       sync.Mutex
	seen     map[string]bool
	deadline time.Time
	haveDL   bool
}

func newDeadlineDeduper(ids []string) *deadlineDeduper {
	d := &deadlineDeduper{seen: make(map[string]bool, len(ids))}
	for _, id := range ids {
		d.seen[id] = true
	}
	return d
}

func (d *deadlineDeduper) MarkSeen(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen[id] {
		return false, nil
	}
	d.seen[id] = true
	return true, nil
}

func (d *deadlineDeduper) Release(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dedup Release %q: %w", id, err)
	}
	dl, ok := ctx.Deadline()
	d.mu.Lock()
	defer d.mu.Unlock()
	if ok && !d.haveDL {
		d.deadline, d.haveDL = dl, true
	}
	delete(d.seen, id)
	return nil
}

func (*deadlineDeduper) Close() error { return nil }

func (d *deadlineDeduper) observedDeadline() (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadline, d.haveDL
}

func TestReleaseClaims_BudgetScalesWithClaimCount(t *testing.T) {
	// The release budget is spent across a whole batch of claims, and claimedIDs
	// holds one entry per EVENT: DefaultMaxKeys' worth of hot-key events, or a 1s
	// FlushTick at a few thousand events/sec, arrives as one call with thousands of
	// IDs. A budget that does not grow with that count covers the first few hundred
	// releases and strands the rest — silently, since a stranded claim only shows up
	// as a dedup_skip on some later redelivery.
	measure := func(n int) time.Duration {
		t.Helper()
		ids := hotKeyEventIDs(n)
		dedup := newDeadlineDeduper(ids)
		cfg := processor.Defaults()
		cfg.Recorder = metrics.NewInMemory()
		cfg.Dedup = dedup

		start := time.Now()
		processor.ReleaseClaims(context.Background(), &cfg, "test", ids)

		dl, ok := dedup.observedDeadline()
		if !ok {
			t.Fatalf("release context for %d claims carried no deadline", n)
		}
		return dl.Sub(start)
	}

	small := measure(8)
	large := measure(processor.DefaultMaxKeys)

	if small < 4*time.Second {
		t.Errorf("budget for 8 claims: got %v, want at least a few seconds "+
			"(one round trip's worth of headroom is the floor)", small)
	}
	if large < small+time.Second {
		t.Errorf("budget for %d claims (%v) is not meaningfully larger than the budget "+
			"for 8 (%v): the same wall-clock has to cover 1250x the releases, so the "+
			"tail of the batch never gets handed back",
			processor.DefaultMaxKeys, large, small)
	}
	if large > time.Minute {
		t.Errorf("budget for %d claims: got %v, want a bounded ceiling — a shutdown "+
			"path cannot wait this long", processor.DefaultMaxKeys, large)
	}
}
