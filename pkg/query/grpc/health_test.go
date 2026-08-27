package grpc_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/grpchealth"

	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/state"
)

// healthStore records Get calls so the tests can prove the readiness result
// is cached rather than hitting the store on every probe.
type healthStore struct {
	gets atomic.Int64
	err  atomic.Pointer[error]
}

func (s *healthStore) Get(_ context.Context, _ state.Key) (int64, bool, error) {
	s.gets.Add(1)
	if e := s.err.Load(); e != nil {
		return 0, false, *e
	}
	return 0, false, nil
}
func (s *healthStore) GetMany(context.Context, []state.Key) ([]int64, []bool, error) {
	return nil, nil, nil
}
func (s *healthStore) MergeUpdate(context.Context, state.Key, int64, time.Duration) error {
	return nil
}
func (s *healthStore) Close() error { return nil }

func (s *healthStore) fail(err error) {
	if err == nil {
		s.err.Store(nil)
		return
	}
	s.err.Store(&err)
}

func newServer(store state.Store[int64]) *mgrpc.Server[int64] {
	return mgrpc.NewServer(mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Encode: mgrpc.Int64LE(),
	})
}

func TestHealthzHandler_LivenessIgnoresTheStore(t *testing.T) {
	// Liveness must stay 200 even when the store is down. Answering 503 here
	// makes an orchestrator restart a healthy task and turns a store blip into
	// a crash loop.
	store := &healthStore{}
	store.fail(errors.New("dynamodb is down"))
	h := newServer(store).HealthzHandler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("liveness with a dead store: got %d, want 200", rec.Code)
	}
	if n := store.gets.Load(); n != 0 {
		t.Errorf("liveness touched the store %d times, want 0", n)
	}
}

func TestHealthzHandler_ReadinessTracksTheStore(t *testing.T) {
	store := &healthStore{}
	h := newServer(store).HealthzHandler(mgrpc.WithHealthCacheTTL(0))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthy store: got %d, want 200", rec.Code)
	}

	store.fail(errors.New("dynamodb is down"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("dead store: got %d, want 503", rec.Code)
	}
}

func TestHealthzHandler_ReadinessIsCached(t *testing.T) {
	// An ALB probes every 15s per target; k8s often tighter. Without caching,
	// health checking becomes steady billed read traffic and couples probe
	// latency to store latency.
	store := &healthStore{}
	h := newServer(store).HealthzHandler(mgrpc.WithHealthCacheTTL(time.Minute))

	for i := 0; i < 25; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("probe %d: got %d", i, rec.Code)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Errorf("store reads across 25 probes: got %d, want 1 (cache is not working)", n)
	}
}

func TestHealthzHandler_UnknownPathIs404(t *testing.T) {
	h := newServer(&healthStore{}).HealthzHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown path: got %d, want 404", rec.Code)
	}
}

func TestHealthHandler_ServesGrpcHealthV1(t *testing.T) {
	// The whole point: something must actually answer
	// /grpc.health.v1.Health/Check, so an ALB GRPC target group can match on
	// status 0 rather than on UNIMPLEMENTED landing inside a 0-99 matcher.
	store := &healthStore{}
	srv := newServer(store)

	mux := http.NewServeMux()
	mux.Handle(srv.HealthHandler(mgrpc.WithHealthCacheTTL(0)))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := grpchealth.NewClient(ts.Client(), ts.URL)

	resp, err := client.Check(context.Background(), &grpchealth.CheckRequest{})
	if err != nil {
		t.Fatalf("health check RPC failed — the service is not mounted: %v", err)
	}
	if resp.Status != grpchealth.StatusServing {
		t.Errorf("healthy store: got %v, want SERVING", resp.Status)
	}

	store.fail(errors.New("dynamodb is down"))
	resp, err = client.Check(context.Background(), &grpchealth.CheckRequest{})
	if err != nil {
		t.Fatalf("health check RPC failed: %v", err)
	}
	if resp.Status != grpchealth.StatusNotServing {
		t.Errorf("dead store: got %v, want NOT_SERVING", resp.Status)
	}
}

func TestHealthHandler_CustomProbeReplacesTheStoreRoundTrip(t *testing.T) {
	store := &healthStore{}
	called := 0
	h := newServer(store).HealthzHandler(mgrpc.WithHealthProbe(func(context.Context) error {
		called++
		return nil
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if called != 1 {
		t.Errorf("custom probe calls: got %d, want 1", called)
	}
	if n := store.gets.Load(); n != 0 {
		t.Errorf("custom probe should replace the store read, but store saw %d", n)
	}
}
