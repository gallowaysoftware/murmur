package processor_test

import (
	"context"
	"errors"
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

func (d *partialClaimDeduper) Release(_ context.Context, id string) error {
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
