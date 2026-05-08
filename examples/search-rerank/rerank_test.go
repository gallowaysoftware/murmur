package rerank_test

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	rerank "github.com/gallowaysoftware/murmur/examples/search-rerank"
	"github.com/gallowaysoftware/murmur/pkg/monoid/core"
	"github.com/gallowaysoftware/murmur/pkg/monoid/windowed"
	mgrpc "github.com/gallowaysoftware/murmur/pkg/query/grpc"
	"github.com/gallowaysoftware/murmur/pkg/state"
	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// fakeRecaller returns a fixed list per query.
type fakeRecaller struct {
	by map[string][]rerank.Candidate
}

func (f *fakeRecaller) Recall(_ context.Context, query string, topN int) ([]rerank.Candidate, error) {
	cs := f.by[query]
	if len(cs) > topN {
		return cs[:topN], nil
	}
	return cs, nil
}

// fakeStore implements state.Store[int64] with a map.
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

// startMurmur spins up a real Murmur QueryService over httptest, backed
// by an in-memory store. Used to validate the rerank service's GetMany /
// GetWindowMany call path end-to-end without external infra.
func startMurmur[V any](t *testing.T, cfg mgrpc.Config[V]) (murmurv1connect.QueryServiceClient, func()) {
	t.Helper()
	srv := mgrpc.NewServer(cfg)
	mux := http.NewServeMux()
	mux.Handle(srv.Handler())
	httpSrv := httptest.NewServer(mux)
	client := murmurv1connect.NewQueryServiceClient(httpSrv.Client(), httpSrv.URL)
	return client, httpSrv.Close
}

func TestSearch_ReranksByCounterFeatures(t *testing.T) {
	// Recall returns 3 candidates with comparable BM25.
	recaller := &fakeRecaller{by: map[string][]rerank.Candidate{
		"go": {
			{ID: "post-A", BM25: 1.0},
			{ID: "post-B", BM25: 1.0},
			{ID: "post-C", BM25: 1.0},
		},
	}}

	// Murmur all-time likes: post-A=10, post-B=1000, post-C=100.
	store := fakeStore{
		state.Key{Entity: "post-A"}: 10,
		state.Key{Entity: "post-B"}: 1000,
		state.Key{Entity: "post-C"}: 100,
	}
	client, cleanup := startMurmur[int64](t, mgrpc.Config[int64]{
		Store:  store,
		Monoid: core.Sum[int64](),
		Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	svc, err := rerank.New(rerank.Config{
		Recall:       recaller,
		LikesAllTime: client,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := svc.Search(context.Background(), "go")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(got): %d, want 3", len(got))
	}
	// BM25 is constant; all-time likes determine order. Expected:
	// post-B (1000) > post-C (100) > post-A (10).
	want := []string{"post-B", "post-C", "post-A"}
	for i := range want {
		if got[i].ID != want[i] {
			t.Errorf("[%d]: got %q, want %q (full: %+v)", i, got[i].ID, want[i], got)
		}
	}
}

func TestSearch_DegradesGracefullyWhenMurmurFails(t *testing.T) {
	recaller := &fakeRecaller{by: map[string][]rerank.Candidate{
		"go": {
			{ID: "post-A", BM25: 2.0},
			{ID: "post-B", BM25: 1.0},
		},
	}}

	// nil LikesAllTime client → counter feature missing; rerank falls back
	// to BM25 alone.
	svc, err := rerank.New(rerank.Config{
		Recall: recaller,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := svc.Search(context.Background(), "go")
	if err != nil {
		t.Fatalf("Search should not error when counter-fetch is unavailable: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got): %d, want 2", len(got))
	}
	// post-A has higher BM25; without counter features it ranks first.
	if got[0].ID != "post-A" {
		t.Errorf("[0]: got %q, want post-A (BM25-only fallback)", got[0].ID)
	}
}

func TestSearch_WindowedFeatureBoostsRecent(t *testing.T) {
	// Two posts with identical all-time likes (1000 each), but post-A
	// has 800 likes in the last 24h while post-B has 100. Windowed
	// feature should pull post-A above post-B.
	recaller := &fakeRecaller{by: map[string][]rerank.Candidate{
		"go": {
			{ID: "post-A", BM25: 1.0},
			{ID: "post-B", BM25: 1.0},
		},
	}}

	// All-time store.
	allStore := fakeStore{
		state.Key{Entity: "post-A"}: 1000,
		state.Key{Entity: "post-B"}: 1000,
	}
	allClient, cleanupAll := startMurmur[int64](t, mgrpc.Config[int64]{
		Store: allStore, Monoid: core.Sum[int64](), Encode: mgrpc.Int64LE(),
	})
	defer cleanupAll()

	// Windowed store — daily buckets, last 24h. Today only.
	w := windowed.Daily(30 * 24 * time.Hour)
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	winStore := fakeStore{}
	bucket := w.BucketID(now)
	winStore[state.Key{Entity: "post-A", Bucket: bucket}] = 800
	winStore[state.Key{Entity: "post-B", Bucket: bucket}] = 100
	winClient, cleanupWin := startMurmur[int64](t, mgrpc.Config[int64]{
		Store: winStore, Monoid: core.Sum[int64](), Window: &w, Encode: mgrpc.Int64LE(),
		Now: func() time.Time { return now },
	})
	defer cleanupWin()

	svc, err := rerank.New(rerank.Config{
		Recall:         recaller,
		LikesAllTime:   allClient,
		LikesWindowed:  winClient,
		WindowDuration: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got, err := svc.Search(context.Background(), "go")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len: got %d, want 2", len(got))
	}
	if got[0].ID != "post-A" {
		t.Errorf("expected post-A first (more recent likes), got %q. Full: %+v", got[0].ID, got)
	}
	// Verify the feature values surfaced to ScoredCandidate.
	if got[0].Likes24h != 800 {
		t.Errorf("post-A Likes24h: got %d, want 800", got[0].Likes24h)
	}
	if got[0].LikesAll != 1000 {
		t.Errorf("post-A LikesAll: got %d, want 1000", got[0].LikesAll)
	}
}

func TestSearch_TopKLimits(t *testing.T) {
	// Recall returns 100 candidates; with TopK=5 we get 5 back.
	cands := make([]rerank.Candidate, 100)
	for i := range cands {
		cands[i] = rerank.Candidate{ID: charBased(i), BM25: float64(100 - i)}
	}
	recaller := &fakeRecaller{by: map[string][]rerank.Candidate{"q": cands}}

	svc, _ := rerank.New(rerank.Config{
		Recall: recaller,
		TopK:   5,
	})
	got, err := svc.Search(context.Background(), "q")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("TopK=5: got %d", len(got))
	}
}

func TestSearch_RecallEmpty(t *testing.T) {
	recaller := &fakeRecaller{by: map[string][]rerank.Candidate{}}
	svc, _ := rerank.New(rerank.Config{Recall: recaller})
	got, err := svc.Search(context.Background(), "no-results-query")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if got != nil {
		t.Errorf("got %d, want nil", len(got))
	}
}

func TestSearch_RecallError(t *testing.T) {
	bad := &erroringRecaller{}
	svc, _ := rerank.New(rerank.Config{Recall: bad})
	_, err := svc.Search(context.Background(), "x")
	if err == nil {
		t.Error("expected error from failing recaller")
	}
}

type erroringRecaller struct{}

func (*erroringRecaller) Recall(context.Context, string, int) ([]rerank.Candidate, error) {
	return nil, errors.New("recall down")
}

func TestNew_RequiresRecaller(t *testing.T) {
	_, err := rerank.New(rerank.Config{})
	if err == nil {
		t.Error("expected error when Recall is nil")
	}
}

func TestStats_TrackedAcrossSearches(t *testing.T) {
	recaller := &fakeRecaller{by: map[string][]rerank.Candidate{
		"q1": {{ID: "a", BM25: 1.0}, {ID: "b", BM25: 1.0}},
		"q2": {{ID: "c", BM25: 1.0}},
	}}
	store := fakeStore{state.Key{Entity: "a"}: 1, state.Key{Entity: "c"}: 1}
	client, cleanup := startMurmur[int64](t, mgrpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	svc, _ := rerank.New(rerank.Config{Recall: recaller, LikesAllTime: client})
	if _, err := svc.Search(context.Background(), "q1"); err != nil {
		t.Fatalf("q1: %v", err)
	}
	if _, err := svc.Search(context.Background(), "q2"); err != nil {
		t.Fatalf("q2: %v", err)
	}
	if got := svc.Stats().Searches.Load(); got != 2 {
		t.Errorf("Searches: got %d, want 2", got)
	}
	if got := svc.Stats().CandidatesFetched.Load(); got != 3 {
		t.Errorf("CandidatesFetched: got %d, want 3", got)
	}
	// 2 hits for "a", "c" present in store — q1 has 2 candidates, 1 hits ("a"); q2 has 1, hits.
	if got := svc.Stats().LikesAllHits.Load(); got != 2 {
		t.Errorf("LikesAllHits: got %d, want 2", got)
	}
}

func TestDefaultScore_RecencyBoostDominates(t *testing.T) {
	// Same BM25, same all-time, different recent. Recent dominates.
	c := rerank.Candidate{ID: "x", BM25: 1.0}
	cold := rerank.DefaultScore(c, 1000, 0)   // cold-but-classic
	hot := rerank.DefaultScore(c, 1000, 1000) // hot-and-classic
	if hot <= cold {
		t.Errorf("DefaultScore: expected hot (>0 24h likes) to score above cold; got hot=%v cold=%v", hot, cold)
	}
}

func TestDefaultScore_BM25LinearScales(t *testing.T) {
	// Doubling BM25 doubles the score for fixed counter features.
	a := rerank.Candidate{ID: "a", BM25: 1.0}
	b := rerank.Candidate{ID: "b", BM25: 2.0}
	sa := rerank.DefaultScore(a, 100, 100)
	sb := rerank.DefaultScore(b, 100, 100)
	if sb < 1.99*sa || sb > 2.01*sa {
		t.Errorf("DefaultScore: doubled BM25 should ~double score; got sa=%v sb=%v", sa, sb)
	}
}

// Sanity check on connect-rpc path: if the rerank service issues a
// concurrent burst of identical Searches, the singleflight inside the
// Murmur QueryService should collapse the underlying GetMany calls.
// This exercises the integration between rerank and the singleflight
// layer added in pkg/query/grpc.
func TestSearch_ConcurrentIdenticalCallsCoalesce(t *testing.T) {
	store := fakeStore{state.Key{Entity: "a"}: 1}
	client, cleanup := startMurmur[int64](t, mgrpc.Config[int64]{
		Store: store, Monoid: core.Sum[int64](), Encode: mgrpc.Int64LE(),
	})
	defer cleanup()

	svc, _ := rerank.New(rerank.Config{
		Recall: &fakeRecaller{by: map[string][]rerank.Candidate{
			"q": {{ID: "a", BM25: 1.0}},
		}},
		LikesAllTime: client,
	})

	const N = 20
	done := make(chan error, N)
	for i := 0; i < N; i++ {
		go func() {
			_, err := svc.Search(context.Background(), "q")
			done <- err
		}()
	}
	for i := 0; i < N; i++ {
		if err := <-done; err != nil {
			t.Errorf("call %d: %v", i, err)
		}
	}
	// Beyond compilability, this is mostly a smoke test — the
	// singleflight effectiveness is measured at the gRPC layer
	// (TestQuery_Get_CoalescesConcurrentReads in pkg/query/grpc).
}

// Helpers.

func charBased(i int) string {
	a := byte('a' + (i / 26))
	b := byte('a' + (i % 26))
	return string([]byte{a, b})
}

// Pulled in by the rerank package; used here only via reference.
var _ = connect.NewRequest[pb.GetRequest]
var _ = binary.LittleEndian
