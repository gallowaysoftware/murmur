package wrapper_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	wrapper "github.com/gallowaysoftware/murmur/examples/typed-wrapper"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	"github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/state"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

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
func (fakeStore) Close() error { return nil }

func startMurmur(t *testing.T, store fakeStore, w *windowed.Config) (murmurv1connect.QueryServiceClient, func()) {
	t.Helper()
	srv := grpc.NewServer(grpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Window: w,
		Encode: grpc.Int64LE(),
		Now:    func() time.Time { return time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC) },
	})
	mux := http.NewServeMux()
	mux.Handle(srv.Handler())
	httpSrv := httptest.NewServer(mux)
	client := murmurv1connect.NewQueryServiceClient(httpSrv.Client(), httpSrv.URL)
	return client, httpSrv.Close
}

func TestServer_GetBotInteractionCount(t *testing.T) {
	likesStore := fakeStore{
		state.Key{Entity: "bot:b-1|user:u-1"}: 42,
	}
	likesClient, cleanup := startMurmur(t, likesStore, nil)
	defer cleanup()

	// Windowed pipeline isn't exercised in this test; pass the same
	// client so Server is fully wired.
	srv := wrapper.NewServer(likesClient, likesClient)

	resp, err := srv.GetBotInteractionCount(context.Background(),
		&wrapper.BotInteractionRequest{BotID: "b-1", UserID: "u-1"})
	if err != nil {
		t.Fatalf("GetBotInteractionCount: %v", err)
	}
	if !resp.Present {
		t.Errorf("present: got false, want true")
	}
	if resp.Count != 42 {
		t.Errorf("count: got %d, want 42", resp.Count)
	}
}

func TestServer_GetBotInteractionCount_Missing(t *testing.T) {
	likesStore := fakeStore{}
	likesClient, cleanup := startMurmur(t, likesStore, nil)
	defer cleanup()

	srv := wrapper.NewServer(likesClient, likesClient)
	resp, err := srv.GetBotInteractionCount(context.Background(),
		&wrapper.BotInteractionRequest{BotID: "b-1", UserID: "u-1"})
	if err != nil {
		t.Fatalf("GetBotInteractionCount: %v", err)
	}
	if resp.Present {
		t.Errorf("missing entity: present = true, want false")
	}
}

func TestServer_GetBotInteractionCountWindow(t *testing.T) {
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 8, 12, 0, 0, 0, time.UTC)

	winStore := fakeStore{}
	for i := 0; i < 7; i++ {
		bucket := w.BucketID(now.Add(-time.Duration(i) * 24 * time.Hour))
		winStore[state.Key{Entity: "bot:b-1|user:u-1", Bucket: bucket}] = int64(i + 1)
	}
	winClient, cleanup := startMurmur(t, winStore, &w)
	defer cleanup()

	srv := wrapper.NewServer(winClient, winClient)

	resp, err := srv.GetBotInteractionCountWindow(context.Background(),
		&wrapper.BotInteractionWindowRequest{
			BotID:    "b-1",
			UserID:   "u-1",
			Duration: 7 * 24 * time.Hour,
		})
	if err != nil {
		t.Fatalf("GetBotInteractionCountWindow: %v", err)
	}
	// 1+2+3+4+5+6+7 = 28
	if resp.Count != 28 {
		t.Errorf("Last7Days: got %d, want 28", resp.Count)
	}
}

func TestServer_RejectsEmptyKeys(t *testing.T) {
	likesClient, cleanup := startMurmur(t, fakeStore{}, nil)
	defer cleanup()

	srv := wrapper.NewServer(likesClient, likesClient)
	_, err := srv.GetBotInteractionCount(context.Background(),
		&wrapper.BotInteractionRequest{BotID: "", UserID: "u-1"})
	if err == nil {
		t.Error("empty BotID: expected error")
	}
}
