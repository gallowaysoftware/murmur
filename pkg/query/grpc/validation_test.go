package grpc_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/state"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
)

// TestQueryServer_GetRejectsWindowedPipeline pins the routing guard. On a windowed
// pipeline, Get and GetMany read bucket 0, which is simultaneously the all-time
// sentinel and the epoch bucket — so they always reported absent no matter how much
// the pipeline had counted, and four shipped runbooks told operators to call them.
func TestQueryServer_GetRejectsWindowedPipeline(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := fakeStore{
		state.Key{Entity: "page-A", Bucket: w.BucketID(now)}: 42,
	}
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanup()
	ctx := context.Background()

	// fresh_read must not route around the guard: a windowed pipeline has no
	// all-time row to read freshly.
	for _, fresh := range []bool{false, true} {
		_, err := client.Get(ctx, connect.NewRequest(&pb.GetRequest{Entity: "page-A", FreshRead: fresh}))
		if err == nil {
			t.Fatalf("Get(fresh_read=%v) on a windowed pipeline: got nil error, want FailedPrecondition", fresh)
		}
		if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
			t.Errorf("Get(fresh_read=%v): got code %v (%v), want FailedPrecondition", fresh, code, err)
		}

		_, err = client.GetMany(ctx, connect.NewRequest(&pb.GetManyRequest{
			Entities: []string{"page-A"}, FreshRead: fresh,
		}))
		if err == nil {
			t.Fatalf("GetMany(fresh_read=%v) on a windowed pipeline: got nil error, want FailedPrecondition", fresh)
		}
		if code := connect.CodeOf(err); code != connect.CodeFailedPrecondition {
			t.Errorf("GetMany(fresh_read=%v): got code %v (%v), want FailedPrecondition", fresh, code, err)
		}
	}

	// The RPC the guard points at still answers.
	resp, err := client.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
		Entity: "page-A", DurationSeconds: 86400,
	}))
	if err != nil {
		t.Fatalf("GetWindow: %v", err)
	}
	if got := decodeInt64(resp.Msg.GetValue().GetData()); got != 42 {
		t.Errorf("GetWindow: got %d, want 42", got)
	}
}

// TestQueryServer_GetAllowsZeroGranularityWindow keeps the escape hatch open: a
// Window whose Granularity is zero writes bucket 0 for real, so Get is the right
// RPC for it.
func TestQueryServer_GetAllowsZeroGranularityWindow(t *testing.T) {
	w := windowed.Config{Retention: 30 * 24 * time.Hour}
	store := fakeStore{state.Key{Entity: "page-A"}: 42}
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	resp, err := client.Get(context.Background(), connect.NewRequest(&pb.GetRequest{Entity: "page-A"}))
	if err != nil {
		t.Fatalf("Get with Granularity=0: %v", err)
	}
	if !resp.Msg.GetValue().GetPresent() {
		t.Error("Get with Granularity=0: reported absent, want present")
	}
}

func TestQueryServer_GetRangeRejectsDegenerateBounds(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store: fakeStore{}, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name             string
		startUnix        int64
		endUnix          int64
		freshRead        bool
		wantErrSubstring string
	}{
		{name: "start after end", startUnix: now.Unix(), endUnix: now.Add(-24 * time.Hour).Unix()},
		{name: "both bounds unset", startUnix: 0, endUnix: 0},
		{name: "both bounds unset, fresh_read", startUnix: 0, endUnix: 0, freshRead: true},
		// What pkg/query/typed puts on the wire for a zero time.Time.
		{name: "zero time.Time start", startUnix: -62135596800, endUnix: now.Unix()},
		{name: "year 9999 end", startUnix: now.Add(-24 * time.Hour).Unix(), endUnix: 253402300799},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := client.GetRange(ctx, connect.NewRequest(&pb.GetRangeRequest{
				Entity: "page-A", StartUnix: tc.startUnix, EndUnix: tc.endUnix, FreshRead: tc.freshRead,
			}))
			if err == nil {
				t.Fatalf("GetRange(%d, %d): got value{present=%v}, want InvalidArgument",
					tc.startUnix, tc.endUnix, resp.Msg.GetValue().GetPresent())
			}
			if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
				t.Errorf("GetRange(%d, %d): got code %v (%v), want InvalidArgument",
					tc.startUnix, tc.endUnix, code, err)
			}
		})
	}

	t.Run("GetRangeMany", func(t *testing.T) {
		_, err := client.GetRangeMany(ctx, connect.NewRequest(&pb.GetRangeManyRequest{
			Entities: []string{"page-A"}, StartUnix: now.Unix(), EndUnix: now.Add(-24 * time.Hour).Unix(),
		}))
		if err == nil {
			t.Fatal("GetRangeMany with start after end: got nil error, want InvalidArgument")
		}
		if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
			t.Errorf("GetRangeMany: got code %v (%v), want InvalidArgument", code, err)
		}
	})
}

func TestQueryServer_GetWindowRejectsInvalidDuration(t *testing.T) {
	// 7 days of retention: 7 daily buckets live, anything longer reads evicted
	// buckets and folds the holes in as Identity.
	w := windowed.Daily(7 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store: fakeStore{}, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name            string
		durationSeconds int64
	}{
		{"unset (proto3 zero)", 0},
		{"negative", -3600},
		{"beyond retention", 90 * 86400},
		// Overflows the nanosecond Duration and used to wrap to a negative one.
		{"nanosecond overflow", 10_000_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, fresh := range []bool{false, true} {
				_, err := client.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
					Entity: "page-A", DurationSeconds: tc.durationSeconds, FreshRead: fresh,
				}))
				if err == nil {
					t.Fatalf("GetWindow(duration_seconds=%d, fresh_read=%v): got nil error, want InvalidArgument",
						tc.durationSeconds, fresh)
				}
				if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
					t.Errorf("GetWindow(duration_seconds=%d, fresh_read=%v): got code %v (%v), want InvalidArgument",
						tc.durationSeconds, fresh, code, err)
				}
			}
			_, err := client.GetWindowMany(ctx, connect.NewRequest(&pb.GetWindowManyRequest{
				Entities: []string{"page-A"}, DurationSeconds: tc.durationSeconds,
			}))
			if err == nil {
				t.Fatalf("GetWindowMany(duration_seconds=%d): got nil error, want InvalidArgument", tc.durationSeconds)
			}
			if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
				t.Errorf("GetWindowMany(duration_seconds=%d): got code %v (%v), want InvalidArgument",
					tc.durationSeconds, code, err)
			}
		})
	}

	if _, err := client.GetWindow(ctx, connect.NewRequest(&pb.GetWindowRequest{
		Entity: "page-A", DurationSeconds: 7 * 86400,
	})); err != nil {
		t.Errorf("GetWindow at exactly the retention window: %v", err)
	}
}

// blockingStore parks every GetMany until release is closed, so a test can hold a
// singleflight group open and observe what the other callers in it experience. It
// honors the context it is handed — that is the whole point: a leader-scoped
// context used to cancel this call for everyone.
//
// It also records the deadline each call's context carried, which is how the
// deadline-propagation test reads the answer off the context instead of waiting for
// a timer to fire.
type blockingStore struct {
	values  fakeStore
	release chan struct{}
	arrived chan []state.Key

	mu        sync.Mutex
	calls     int
	deadlines []time.Time
}

func newBlockingStore(values fakeStore) *blockingStore {
	return &blockingStore{
		values:  values,
		release: make(chan struct{}),
		arrived: make(chan []state.Key, 8),
	}
}

// deadlineAt returns the deadline the i'th store call's context carried, zero if it
// had none.
func (s *blockingStore) deadlineAt(i int) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i >= len(s.deadlines) {
		return time.Time{}
	}
	return s.deadlines[i]
}

func (s *blockingStore) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	vs, oks, err := s.GetMany(ctx, []state.Key{k})
	if err != nil {
		return 0, false, err
	}
	return vs[0], oks[0], nil
}

func (s *blockingStore) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	deadline, _ := ctx.Deadline()
	s.mu.Lock()
	s.calls++
	s.deadlines = append(s.deadlines, deadline)
	s.mu.Unlock()
	s.arrived <- ks
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	// Cancellation is re-checked here rather than decided by the select above. With
	// both channels ready select picks at random, and that coin flip is precisely what
	// let the cancellation test below pass 4 runs in 20 against the unfixed server:
	// half the time the release won the race and the store returned a value even
	// though its context was already dead. Whether the coalesced call still holds a
	// live context when its turn finally comes is the whole question, so answer it.
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return s.values.GetMany(ctx, ks)
}

func (s *blockingStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	return nil
}
func (s *blockingStore) Close() error { return nil }

func (s *blockingStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestQueryServer_CoalesceKeepsEntityListsDistinct drives two concurrent
// GetWindowMany requests whose entity lists collided under the old '|'-joined,
// sorted coalesce key. The second caller used to be handed the first caller's
// result slice — a different length, or the same length with every value
// attributed to the wrong entity.
func TestQueryServer_CoalesceKeepsEntityListsDistinct(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	bucket := w.BucketID(now)

	cases := []struct {
		name    string
		first   []string
		second  []string
		wantSec []int64
	}{
		// A literal '|' inside an entity key is not hypothetical: the shipped
		// codegen key_template builds entity keys that contain one.
		{"separator inside an entity", []string{"a|b"}, []string{"a", "b"}, []int64{1, 2}},
		{"separator straddling entities", []string{"a|b", "c"}, []string{"a", "b|c"}, []int64{1, 5}},
		// Sorting made these one group, so the values came back transposed.
		{"permuted order", []string{"a", "b"}, []string{"b", "a"}, []int64{2, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newBlockingStore(fakeStore{
				state.Key{Entity: "a", Bucket: bucket}:   1,
				state.Key{Entity: "b", Bucket: bucket}:   2,
				state.Key{Entity: "c", Bucket: bucket}:   3,
				state.Key{Entity: "a|b", Bucket: bucket}: 4,
				state.Key{Entity: "b|c", Bucket: bucket}: 5,
			})
			client, cleanup := startServer(t, mgrpc.Config[int64]{
				Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
				Now: func() time.Time { return now },
			})
			defer cleanup()

			type result struct {
				values []int64
				err    error
			}
			run := func(entities []string) <-chan result {
				out := make(chan result, 1)
				go func() {
					resp, err := client.GetWindowMany(context.Background(), connect.NewRequest(&pb.GetWindowManyRequest{
						Entities: entities, DurationSeconds: 86400,
					}))
					if err != nil {
						out <- result{err: err}
						return
					}
					vals := make([]int64, 0, len(resp.Msg.GetValues()))
					for _, v := range resp.Msg.GetValues() {
						vals = append(vals, decodeInt64(v.GetData()))
					}
					out <- result{values: vals}
				}()
				return out
			}

			firstDone := run(tc.first)
			// Wait for the first request to reach the store: its group is now
			// open, which is the only state in which the second can collide
			// with it.
			<-store.arrived

			secondDone := run(tc.second)
			// The second request forms its own group and reaches the store too.
			// Under the colliding key it never did, so fall through after a
			// grace period and let the assertions below report what it got.
			select {
			case <-store.arrived:
			case <-time.After(2 * time.Second):
			}
			close(store.release)

			<-firstDone
			second := <-secondDone
			if second.err != nil {
				t.Fatalf("second GetWindowMany(%v): %v", tc.second, second.err)
			}
			if len(second.values) != len(tc.wantSec) {
				t.Fatalf("second GetWindowMany(%v): got %d values %v, want %d (%v) — coalesced onto the first caller's result",
					tc.second, len(second.values), second.values, len(tc.wantSec), tc.wantSec)
			}
			for i := range tc.wantSec {
				if second.values[i] != tc.wantSec[i] {
					t.Fatalf("second GetWindowMany(%v): got %v, want %v — values transposed by a shared coalesce key",
						tc.second, second.values, tc.wantSec)
				}
			}
		})
	}
}

// joinContext reports the moment its holder becomes a singleflight waiter.
//
// coalesce evaluates ctx.Done() only in the select it runs AFTER sf.DoChan returns,
// and DoChan has already appended the caller to the in-flight call's channel list by
// the time it returns. A signal here is therefore proof that the peer is parked on
// the leader's group — where a sleep was only ever a guess that it had got there.
type joinContext struct {
	context.Context
	once   sync.Once
	joined chan struct{}
}

func newJoinContext(parent context.Context) *joinContext {
	return &joinContext{Context: parent, joined: make(chan struct{})}
}

func (c *joinContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
}

// TestQueryServer_CoalescedPeerSurvivesCallerCancellation covers the other half of
// the singleflight bug: the shared store call ran on the leader's context, so one
// client hanging up failed everybody coalesced behind it with CodeInternal.
//
// Every step is gated on an explicit channel rather than a sleep, because the sleeping
// version of this test only failed 16 runs in 20 against the unfixed server and a
// regression test that passes a fifth of the time is not one. Two gates carry it: the
// leader is provably inside the store call before the cancel, and the peer is provably
// a waiter on the leader's group before it. The server is driven in-process rather than
// over HTTP so the cancellation reaches the store through nothing but context plumbing
// — the HTTP path stays covered by the coalesce-key test above.
func TestQueryServer_CoalescedPeerSurvivesCallerCancellation(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := newBlockingStore(fakeStore{
		state.Key{Entity: "page-A", Bucket: w.BucketID(now)}: 42,
	})
	srv := mgrpc.NewServer(mgrpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	req := func() *connect.Request[pb.GetWindowRequest] {
		return connect.NewRequest(&pb.GetWindowRequest{Entity: "page-A", DurationSeconds: 86400})
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	leaderErr := make(chan error, 1)
	go func() {
		_, err := srv.GetWindow(leaderCtx, req())
		leaderErr <- err
	}()
	// Gate 1: the leader is inside the store call, so its group is open.
	<-store.arrived

	peerCtx := newJoinContext(context.Background())
	type peerResult struct {
		value int64
		err   error
	}
	peerDone := make(chan peerResult, 1)
	go func() {
		resp, err := srv.GetWindow(peerCtx, req())
		if err != nil {
			peerDone <- peerResult{err: err}
			return
		}
		peerDone <- peerResult{value: decodeInt64(resp.Msg.GetValue().GetData())}
	}()
	// Gate 2: the peer is a waiter on that group, not merely dispatched towards it.
	<-peerCtx.joined

	cancelLeader()
	if err := <-leaderErr; connect.CodeOf(err) != connect.CodeCanceled {
		t.Fatalf("leader GetWindow after hanging up: got %v (code %v), want Canceled",
			err, connect.CodeOf(err))
	}
	// The leader is gone and its context is dead. The store call is still parked;
	// whether it kept a live context of its own is what the peer now reports.
	close(store.release)

	got := <-peerDone
	if got.err != nil {
		t.Fatalf("peer GetWindow failed after the leading caller cancelled: %v (code %v)",
			got.err, connect.CodeOf(got.err))
	}
	if got.value != 42 {
		t.Errorf("peer GetWindow: got %d, want 42", got.value)
	}
	if n := store.callCount(); n != 1 {
		t.Fatalf("store called %d times, want 1: the peer never coalesced onto the leader's group, so this run proved nothing", n)
	}
}

// TestQueryServer_CoalescedWorkKeepsCallerDeadline pins that detaching the shared store
// call from the leader's CANCELLATION did not also detach it from the leader's DEADLINE.
//
// context.WithoutCancel plus a flat CoalesceTimeout dropped both. Before coalescing
// existed, a client hanging up shed its store work immediately; with the deadline gone
// a burst of abandoned requests would instead keep a full CoalesceTimeout of fan-out
// alive per group with nobody left to read the answer.
//
// Both cases read the deadline straight off the context the store was handed, so
// neither waits for a timer.
func TestQueryServer_CoalescedWorkKeepsCallerDeadline(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	// An hour of CoalesceTimeout, far longer than any deadline below, so what the
	// assertions measure is which of the two bounds the store call inherited.
	const coalesceTimeout = time.Hour

	newFixture := func(t *testing.T) (*mgrpc.Server[int64], *blockingStore) {
		t.Helper()
		store := newBlockingStore(fakeStore{
			state.Key{Entity: "page-A", Bucket: w.BucketID(now)}: 42,
		})
		return mgrpc.NewServer(mgrpc.Config[int64]{
			Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
			Now: func() time.Time { return now }, CoalesceTimeout: coalesceTimeout,
		}), store
	}
	req := connect.NewRequest(&pb.GetWindowRequest{Entity: "page-A", DurationSeconds: 86400})

	t.Run("caller deadline is inherited", func(t *testing.T) {
		srv, store := newFixture(t)
		// Generous enough that it cannot expire mid-test; it is never waited out.
		callerDeadline := time.Now().Add(30 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
		defer cancel()

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = srv.GetWindow(ctx, req)
		}()
		<-store.arrived
		close(store.release)
		<-done

		got := store.deadlineAt(0)
		if got.IsZero() {
			t.Fatal("the coalesced store call ran with no deadline at all")
		}
		if got.After(callerDeadline) {
			t.Errorf("coalesced store call carries a deadline %s past the caller's own; the caller's deadline was dropped for the flat %s CoalesceTimeout",
				got.Sub(callerDeadline), coalesceTimeout)
		}
	})

	t.Run("CoalesceTimeout bounds a caller with no deadline", func(t *testing.T) {
		srv, store := newFixture(t)
		before := time.Now()

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = srv.GetWindow(context.Background(), req)
		}()
		<-store.arrived
		close(store.release)
		<-done

		got := store.deadlineAt(0)
		if got.IsZero() {
			t.Fatal("a caller with no deadline left the coalesced store call unbounded; a wedged store would pin the group forever")
		}
		if got.Before(before) || got.After(before.Add(coalesceTimeout+time.Minute)) {
			t.Errorf("coalesced store call deadline is %s from the call, want about the %s CoalesceTimeout",
				got.Sub(before), coalesceTimeout)
		}
	})
}
