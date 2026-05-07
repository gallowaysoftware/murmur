package grpc_test

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/state"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// fakeStore is an in-memory state.Store[int64] for the query server tests.
type fakeStore map[state.Key]int64

func (s fakeStore) Get(_ context.Context, k state.Key) (int64, bool, error) {
	v, ok := s[k]
	return v, ok, nil
}
func (s fakeStore) GetMany(_ context.Context, ks []state.Key) ([]int64, []bool, error) {
	vs := make([]int64, len(ks))
	oks := make([]bool, len(ks))
	for i, k := range ks {
		vs[i], oks[i] = s[k]
	}
	return vs, oks, nil
}
func (s fakeStore) MergeUpdate(_ context.Context, k state.Key, d int64, _ time.Duration) error {
	s[k] += d
	return nil
}
func (s fakeStore) Close() error { return nil }

// startServer spins up an httptest.Server hosting the Connect QueryService and
// returns a client speaking the Connect protocol. The same server would also
// serve grpc / grpc-web traffic — Connect handles all three on one mux.
func startServer[V any](t *testing.T, cfg mgrpc.Config[V]) (murmurv1connect.QueryServiceClient, func()) {
	t.Helper()
	srv := mgrpc.NewServer(cfg)
	mux := http.NewServeMux()
	mux.Handle(srv.Handler())
	httpSrv := httptest.NewServer(mux)
	client := murmurv1connect.NewQueryServiceClient(httpSrv.Client(), httpSrv.URL)
	return client, httpSrv.Close
}

func decodeInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func TestQuery_Get_NonWindowed(t *testing.T) {
	store := fakeStore{
		state.Key{Entity: "page-A"}: 42,
		state.Key{Entity: "page-B"}: 7,
	}
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	ctx := context.Background()
	cases := []struct {
		entity      string
		wantPresent bool
		wantValue   int64
	}{
		{"page-A", true, 42},
		{"page-B", true, 7},
		{"missing", false, 0},
	}
	for _, tc := range cases {
		resp, err := client.Get(ctx, connect.NewRequest(&pb.GetRequest{Entity: tc.entity}))
		if err != nil {
			t.Fatalf("Get %s: %v", tc.entity, err)
		}
		v := resp.Msg.GetValue()
		if v.GetPresent() != tc.wantPresent {
			t.Errorf("%s present: got %v, want %v", tc.entity, v.GetPresent(), tc.wantPresent)
		}
		if tc.wantPresent && decodeInt64(v.GetData()) != tc.wantValue {
			t.Errorf("%s value: got %d, want %d", tc.entity, decodeInt64(v.GetData()), tc.wantValue)
		}
	}
}

func TestQuery_GetMany(t *testing.T) {
	store := fakeStore{
		state.Key{Entity: "a"}: 1,
		state.Key{Entity: "c"}: 3,
	}
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	resp, err := client.GetMany(context.Background(), connect.NewRequest(&pb.GetManyRequest{
		Entities: []string{"a", "missing", "c"},
	}))
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	values := resp.Msg.GetValues()
	if len(values) != 3 {
		t.Fatalf("len(values) = %d, want 3", len(values))
	}
	if !values[0].Present || decodeInt64(values[0].Data) != 1 {
		t.Errorf("[0] a: got present=%v val=%d, want true 1", values[0].Present, decodeInt64(values[0].Data))
	}
	if values[1].Present {
		t.Errorf("[1] missing: present=true, want false")
	}
	if !values[2].Present || decodeInt64(values[2].Data) != 3 {
		t.Errorf("[2] c: got present=%v val=%d, want true 3", values[2].Present, decodeInt64(values[2].Data))
	}
}

func TestQuery_GetWindow_Daily(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)

	store := fakeStore{}
	for i := 0; i < 5; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		store[state.Key{Entity: "page-A", Bucket: bucket}] = int64(i + 1)
	}
	// Bucket sums: today=1, -1d=2, -2d=3, -3d=4, -4d=5. Sum 1..5 = 15.

	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Window: &w,
		Encode: mgrpc.Int64LE(),
		Now:    func() time.Time { return now },
	})
	defer cleanup()

	cases := []struct {
		name     string
		duration time.Duration
		want     int64
	}{
		{"Last1Day", 1 * 24 * time.Hour, 1},
		{"Last3Days", 3 * 24 * time.Hour, 6}, // 1+2+3
		{"Last5Days", 5 * 24 * time.Hour, 15},
		{"Last30Days", 30 * 24 * time.Hour, 15},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := client.GetWindow(context.Background(), connect.NewRequest(&pb.GetWindowRequest{
				Entity:          "page-A",
				DurationSeconds: int64(tc.duration / time.Second),
			}))
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := decodeInt64(resp.Msg.GetValue().GetData()); got != tc.want {
				t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
			}
		})
	}
}

func TestQuery_GetWindow_NotWindowed(t *testing.T) {
	store := fakeStore{}
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		// Window: nil — pipeline is not windowed.
		Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	_, err := client.GetWindow(context.Background(), connect.NewRequest(&pb.GetWindowRequest{
		Entity: "x", DurationSeconds: 60,
	}))
	if err == nil {
		t.Fatal("expected error for GetWindow on non-windowed pipeline; got nil")
	}
	// Sanity-check that the error code surfaced via Connect carries the expected
	// FailedPrecondition signal — clients should be able to branch on this.
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("error code: got %s, want failed_precondition", connect.CodeOf(err))
	}
}
