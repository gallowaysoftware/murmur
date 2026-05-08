// Package rerank is the runnable Pattern A reference implementation
// from doc/search-integration.md. It exposes an HTTP search endpoint
// that does two-stage retrieval:
//
//  1. Recall — call OpenSearch (or any candidate-source), get top N by text
//  2. Rerank — fetch counter features from Murmur via GetMany / GetWindowMany,
//     score each candidate, return top K
//
// Counters never go in the OpenSearch index. The text-side index serves
// only slow-moving fields; popularity is fetched fresh per query and
// applied as a second-stage rerank in this service.
//
// Pairs with examples/search-projector (Pattern B). Production deployments
// use both: B for filterable bucket fields ("≥1k likes"), A for fine
// ranking within a bucket. See doc/search-integration.md.
//
// # Counter feature lookup
//
// The rerank stage reads counters from Murmur via the QueryService.GetMany
// (all-time) and GetWindowMany (windowed-recent) RPCs. Both are batched —
// one BatchGetItem-shaped fetch covers up to 100 entities, with the
// singleflight coalescing layer collapsing concurrent identical
// requests. For N=200 candidates the fetch is two BatchGetItems in
// parallel, ~10ms p99.
//
// # Score function
//
// The default score is a hand-tuned combination of BM25 and log-scaled
// likes:
//
//	score = bm25 * (1 + log10(likes_24h + 1))
//
// Real production systems use learned models (XGBoost / cross-encoder
// transformers); Murmur is invariant under that choice — it just
// provides the counter-feature inputs. Override ScoreFn to plug a
// different model in.
package rerank

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	pb "github.com/gallowaysoftware/murmur/proto/gen/murmur/v1"
	"github.com/gallowaysoftware/murmur/proto/gen/murmur/v1/murmurv1connect"
)

// Candidate is the input to the rerank stage — what the recall stage
// returned, before counter features are applied.
type Candidate struct {
	ID   string  // document ID
	BM25 float64 // text-relevance score from OpenSearch
}

// ScoredCandidate is the output of the rerank stage — the candidate
// plus its final blended score and the raw feature values that produced
// it (handy for explainability and A/B logging).
type ScoredCandidate struct {
	ID    string
	Score float64

	// Features carried through for inspection.
	BM25     float64
	LikesAll int64
	Likes24h int64
}

// ScoreFn computes the final ranking score from a candidate's BM25 and
// counter features. Default — see DefaultScore — uses log10-scaled likes;
// override to plug in a learned model.
type ScoreFn func(c Candidate, likesAll, likes24h int64) float64

// DefaultScore is the default ScoreFn — a multiplicative combination of
// BM25 and log10-scaled likes. Typical product-search shape; tunable
// per-app.
func DefaultScore(c Candidate, likesAll, likes24h int64) float64 {
	// Use likes_24h dominantly with a small all-time fallback so cold-but-
	// classic content doesn't get crowded out by trending content.
	recencyBoost := 1 + math.Log10(float64(likes24h)+1)
	allTimeBoost := 1 + 0.2*math.Log10(float64(likesAll)+1)
	return c.BM25 * recencyBoost * allTimeBoost
}

// Recaller is the abstraction over the candidate-generation stage.
// Production: the OpenSearch BM25 + filter query path. Tests: a fake
// that returns a fixed list. The interface is intentionally minimal —
// the rerank service doesn't need to know anything about the recall
// implementation beyond "give me up to N candidates for this query."
type Recaller interface {
	Recall(ctx context.Context, query string, topN int) ([]Candidate, error)
}

// Config configures the rerank service. ScoreFn defaults to
// DefaultScore; set it explicitly for ML rerankers.
type Config struct {
	// Recall is the candidate-generation stage. Required.
	Recall Recaller

	// LikesAllTime is the Murmur QueryService client for the all-time
	// likes counter. Optional — set nil to skip this feature.
	LikesAllTime murmurv1connect.QueryServiceClient

	// LikesWindowed is the Murmur QueryService client for the windowed
	// likes counter. Same client type, but typically points at a
	// DIFFERENT pipeline (a windowed counter over the last 24h, not the
	// all-time one). Set nil to skip the windowed feature.
	LikesWindowed murmurv1connect.QueryServiceClient

	// WindowDuration is the duration to query against LikesWindowed.
	// Default: 24 hours.
	WindowDuration time.Duration

	// TopN is the recall set size. Default: 200.
	TopN int

	// TopK is the final returned set size. Default: 20.
	TopK int

	// ScoreFn is the final-stage scoring function. Default: DefaultScore.
	ScoreFn ScoreFn
}

// Service is the rerank service — wraps a Recaller plus Murmur clients
// behind a single Search method.
type Service struct {
	cfg   Config
	stats *Stats
}

// Stats reports per-search latency and counter coverage. Useful for
// dashboards and for verifying the rerank stage is doing what you
// think.
type Stats struct {
	Searches          atomic.Int64
	CandidatesFetched atomic.Int64
	LikesAllHits      atomic.Int64
	LikesWindowHits   atomic.Int64
	LikesMisses       atomic.Int64
}

// New constructs a Service.
func New(cfg Config) (*Service, error) {
	if cfg.Recall == nil {
		return nil, errors.New("rerank: Recall is required")
	}
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = 24 * time.Hour
	}
	if cfg.TopN <= 0 {
		cfg.TopN = 200
	}
	if cfg.TopK <= 0 {
		cfg.TopK = 20
	}
	if cfg.ScoreFn == nil {
		cfg.ScoreFn = DefaultScore
	}
	return &Service{cfg: cfg, stats: &Stats{}}, nil
}

// Stats returns a pointer to live counters. Safe for concurrent reads.
func (s *Service) Stats() *Stats { return s.stats }

// Search runs the two-stage retrieval and returns the top-K reranked
// candidates. Failures in the counter-fetch stage degrade gracefully —
// missing features are treated as zero, the recall result still scores
// (BM25 alone), and the response carries on. The user sees a slightly
// less relevant ranking, not an error.
func (s *Service) Search(ctx context.Context, query string) ([]ScoredCandidate, error) {
	s.stats.Searches.Add(1)

	// Stage 1: recall.
	candidates, err := s.cfg.Recall.Recall(ctx, query, s.cfg.TopN)
	if err != nil {
		return nil, fmt.Errorf("recall %q: %w", query, err)
	}
	s.stats.CandidatesFetched.Add(int64(len(candidates)))
	if len(candidates) == 0 {
		return nil, nil
	}

	ids := make([]string, len(candidates))
	for i, c := range candidates {
		ids[i] = c.ID
	}

	// Stage 2: counter feature fetch — batched.
	// Both calls run in parallel; either failing degrades gracefully to zero.
	type allRes struct{ vals []int64 }
	allCh := make(chan allRes, 1)
	winCh := make(chan allRes, 1)

	go func() {
		allCh <- allRes{vals: s.fetchAllTime(ctx, ids)}
	}()
	go func() {
		winCh <- allRes{vals: s.fetchWindowed(ctx, ids)}
	}()

	likesAll := (<-allCh).vals
	likesWin := (<-winCh).vals

	// Score and sort.
	scored := make([]ScoredCandidate, len(candidates))
	for i, c := range candidates {
		var la, lw int64
		if i < len(likesAll) {
			la = likesAll[i]
		}
		if i < len(likesWin) {
			lw = likesWin[i]
		}
		scored[i] = ScoredCandidate{
			ID:       c.ID,
			Score:    s.cfg.ScoreFn(c, la, lw),
			BM25:     c.BM25,
			LikesAll: la,
			Likes24h: lw,
		}
	}
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})

	if len(scored) > s.cfg.TopK {
		scored = scored[:s.cfg.TopK]
	}
	return scored, nil
}

// fetchAllTime calls QueryService.GetMany. Returns zeros and records the
// miss when the client is nil or any error occurs.
func (s *Service) fetchAllTime(ctx context.Context, ids []string) []int64 {
	out := make([]int64, len(ids))
	if s.cfg.LikesAllTime == nil {
		s.stats.LikesMisses.Add(int64(len(ids)))
		return out
	}
	resp, err := s.cfg.LikesAllTime.GetMany(ctx, connect.NewRequest(&pb.GetManyRequest{
		Entities: ids,
	}))
	if err != nil {
		s.stats.LikesMisses.Add(int64(len(ids)))
		return out
	}
	for i, v := range resp.Msg.GetValues() {
		if v.GetPresent() && len(v.GetData()) >= 8 {
			out[i] = decodeInt64LE(v.GetData())
			s.stats.LikesAllHits.Add(1)
		}
	}
	return out
}

// fetchWindowed calls QueryService.GetWindowMany. The windowed pipeline
// is typically a separate Murmur pipeline over the same source events
// with daily/hourly windowing.
func (s *Service) fetchWindowed(ctx context.Context, ids []string) []int64 {
	out := make([]int64, len(ids))
	if s.cfg.LikesWindowed == nil {
		return out
	}
	resp, err := s.cfg.LikesWindowed.GetWindowMany(ctx, connect.NewRequest(&pb.GetWindowManyRequest{
		Entities:        ids,
		DurationSeconds: int64(s.cfg.WindowDuration / time.Second),
	}))
	if err != nil {
		return out
	}
	for i, v := range resp.Msg.GetValues() {
		if v.GetPresent() && len(v.GetData()) >= 8 {
			out[i] = decodeInt64LE(v.GetData())
			s.stats.LikesWindowHits.Add(1)
		}
	}
	return out
}

// decodeInt64LE mirrors mgrpc.Int64LE — the Murmur server-side encoder
// for int64 monoid values is little-endian 8 bytes.
func decodeInt64LE(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}
