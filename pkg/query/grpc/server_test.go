package grpc_test

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/state"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
)

// fakeStore is an in-memory state.Store[int64] for the gRPC server tests.
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

// startServer spins up a gRPC Server backed by Server[V] over an in-memory bufconn.
// Returns a connected client and a cleanup func.
func startServer[V any](t *testing.T, cfg mgrpc.Config[V]) (pb.QueryServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterQueryServiceServer(srv, mgrpc.NewServer(cfg))
	go func() { _ = srv.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
	}
	return pb.NewQueryServiceClient(conn), cleanup
}

func decodeInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func TestGRPC_Get_NonWindowed(t *testing.T) {
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
		v, err := client.Get(ctx, &pb.GetRequest{Entity: tc.entity})
		if err != nil {
			t.Fatalf("Get %s: %v", tc.entity, err)
		}
		if v.GetPresent() != tc.wantPresent {
			t.Errorf("%s present: got %v, want %v", tc.entity, v.GetPresent(), tc.wantPresent)
		}
		if tc.wantPresent && decodeInt64(v.GetData()) != tc.wantValue {
			t.Errorf("%s value: got %d, want %d", tc.entity, decodeInt64(v.GetData()), tc.wantValue)
		}
	}
}

func TestGRPC_GetMany(t *testing.T) {
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

	resp, err := client.GetMany(context.Background(), &pb.GetManyRequest{
		Entities: []string{"a", "missing", "c"},
	})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(resp.GetValues()) != 3 {
		t.Fatalf("len(values) = %d, want 3", len(resp.GetValues()))
	}
	if !resp.Values[0].Present || decodeInt64(resp.Values[0].Data) != 1 {
		t.Errorf("[0] a: got present=%v val=%d, want true 1", resp.Values[0].Present, decodeInt64(resp.Values[0].Data))
	}
	if resp.Values[1].Present {
		t.Errorf("[1] missing: present=true, want false")
	}
	if !resp.Values[2].Present || decodeInt64(resp.Values[2].Data) != 3 {
		t.Errorf("[2] c: got present=%v val=%d, want true 3", resp.Values[2].Present, decodeInt64(resp.Values[2].Data))
	}
}

func TestGRPC_GetWindow_Daily(t *testing.T) {
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
			resp, err := client.GetWindow(context.Background(), &pb.GetWindowRequest{
				Entity:          "page-A",
				DurationSeconds: int64(tc.duration / time.Second),
			})
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := decodeInt64(resp.GetData()); got != tc.want {
				t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
			}
		})
	}
}

func TestGRPC_GetWindow_NotWindowed(t *testing.T) {
	store := fakeStore{}
	client, cleanup := startServer(t, mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		// Window: nil — pipeline is not windowed.
		Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	_, err := client.GetWindow(context.Background(), &pb.GetWindowRequest{
		Entity: "x", DurationSeconds: 60,
	})
	if err == nil {
		t.Fatal("expected error for GetWindow on non-windowed pipeline; got nil")
	}
}
