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
type blockingStore struct {
	values  fakeStore
	release chan struct{}
	arrived chan []state.Key

	mu    sync.Mutex
	calls int
}

func newBlockingStore(values fakeStore) *blockingStore {
	return &blockingStore{
		values:  values,
		release: make(chan struct{}),
		arrived: make(chan []state.Key, 8),
	}
}

func (s *blockingStore) Get(ctx context.Context, k state.Key) (int64, bool, error) {
	vs, oks, err := s.GetMany(ctx, []state.Key{k})
	if err != nil {
		return 0, false, err
	}
	return vs[0], oks[0], nil
}

func (s *blockingStore) GetMany(ctx context.Context, ks []state.Key) ([]int64, []bool, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	s.arrived <- ks
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
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

// TestQueryServer_CoalescedPeerSurvivesCallerCancellation covers the other half of
// the singleflight bug: the shared store call ran on the leader's context, so one
// client hanging up failed everybody coalesced behind it with CodeInternal.
func TestQueryServer_CoalescedPeerSurvivesCallerCancellation(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	store := newBlockingStore(fakeStore{
		state.Key{Entity: "page-A", Bucket: w.BucketID(now)}: 42,
	})
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanup()

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _ = client.GetWindow(leaderCtx, connect.NewRequest(&pb.GetWindowRequest{
			Entity: "page-A", DurationSeconds: 86400,
		}))
	}()
	// The leader is now parked inside the store with its group open.
	<-store.arrived

	peerDone := make(chan error, 1)
	peerValue := make(chan int64, 1)
	go func() {
		resp, err := client.GetWindow(context.Background(), connect.NewRequest(&pb.GetWindowRequest{
			Entity: "page-A", DurationSeconds: 86400,
		}))
		if err != nil {
			peerDone <- err
			return
		}
		peerValue <- decodeInt64(resp.Msg.GetValue().GetData())
		peerDone <- nil
	}()
	// Give the peer time to join the leader's group (or, if it forms its own,
	// to reach the store) before the leader hangs up.
	select {
	case <-store.arrived:
	case <-time.After(500 * time.Millisecond):
	}

	cancelLeader()
	<-leaderDone
	close(store.release)

	if err := <-peerDone; err != nil {
		t.Fatalf("peer GetWindow failed after the leading caller cancelled: %v (code %v)", err, connect.CodeOf(err))
	}
	if got := <-peerValue; got != 42 {
		t.Errorf("peer GetWindow: got %d, want 42", got)
	}
	if store.callCount() == 0 {
		t.Error("store was never called")
	}
}
