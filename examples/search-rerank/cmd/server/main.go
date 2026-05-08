// HTTP server fronting the rerank service. Exposes GET /search?q=...
// and returns top-K reranked candidates as JSON.
//
// In production the recall stage is OpenSearch via the official SDK; this
// binary uses a minimal stub so the example runs without a search cluster.
// Swap the `staticRecaller` for an OpenSearch-backed implementation by
// implementing the rerank.Recaller interface — that's the entire seam.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"

	rerank "github.com/gallowaysoftware/murmur/examples/search-rerank"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	allTimeURL := flag.String("likes-alltime-url", os.Getenv("LIKES_ALLTIME_URL"),
		"Murmur QueryService URL for the all-time likes counter (e.g. http://localhost:50051)")
	windowedURL := flag.String("likes-windowed-url", os.Getenv("LIKES_WINDOWED_URL"),
		"Murmur QueryService URL for the daily-windowed likes counter")
	flag.Parse()

	if *allTimeURL == "" {
		log.Fatal("--likes-alltime-url or LIKES_ALLTIME_URL is required")
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}

	allClient := murmurv1connect.NewQueryServiceClient(httpClient, *allTimeURL)
	var winClient murmurv1connect.QueryServiceClient
	if *windowedURL != "" {
		winClient = murmurv1connect.NewQueryServiceClient(httpClient, *windowedURL)
	}

	// In production, replace with an OpenSearch-backed implementation
	// of the rerank.Recaller interface. For demo purposes the static
	// recaller returns a small fixed corpus.
	svc, err := rerank.New(rerank.Config{
		Recall:        &staticRecaller{},
		LikesAllTime:  allClient,
		LikesWindowed: winClient,
	})
	if err != nil {
		log.Fatalf("rerank.New: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q == "" {
			http.Error(w, "missing ?q=", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		results, err := svc.Search(ctx, q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(results)
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := svc.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"searches":           s.Searches.Load(),
			"candidates_fetched": s.CandidatesFetched.Load(),
			"likes_all_hits":     s.LikesAllHits.Load(),
			"likes_window_hits":  s.LikesWindowHits.Load(),
			"likes_misses":       s.LikesMisses.Load(),
		})
	})

	server := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("rerank server listening on %s (alltime=%s windowed=%s)",
		*addr, *allTimeURL, *windowedURL)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// staticRecaller is the demo recaller — returns 10 hard-coded candidates
// for any query. Production: implement Recaller against OpenSearch.
type staticRecaller struct{}

func (*staticRecaller) Recall(_ context.Context, query string, topN int) ([]rerank.Candidate, error) {
	cs := []rerank.Candidate{
		{ID: "post-001", BM25: 8.4},
		{ID: "post-002", BM25: 7.9},
		{ID: "post-003", BM25: 7.7},
		{ID: "post-004", BM25: 7.2},
		{ID: "post-005", BM25: 6.8},
		{ID: "post-006", BM25: 6.5},
		{ID: "post-007", BM25: 6.1},
		{ID: "post-008", BM25: 5.9},
		{ID: "post-009", BM25: 5.5},
		{ID: "post-010", BM25: 5.2},
	}
	if topN > 0 && len(cs) > topN {
		cs = cs[:topN]
	}
	_ = query
	return cs, nil
}
