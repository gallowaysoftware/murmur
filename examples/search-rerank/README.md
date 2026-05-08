# Example: search rerank service (Pattern A from doc/search-integration.md)

A runnable reference implementation of the **query-time rescore** pattern from [`doc/search-integration.md`](../../doc/search-integration.md). HTTP server that takes a query, recalls candidates from an upstream search engine (OpenSearch in production), fetches counter features from Murmur, and returns top-K reranked candidates.

## What it does

```
Client → /search?q=...
       │
       ├─ 1: Recall (e.g. OpenSearch BM25)         → top N=200 candidates
       │
       ├─ 2: Murmur GetMany / GetWindowMany        → counter features per candidate
       │     (batched; one BatchGetItem-shaped call covers all N)
       │
       └─ 3: Score(BM25, likes_all, likes_24h)     → top K=20 results
                  ↓
              JSON response
```

Counters never go in the OpenSearch index. The text-side index serves only slow-moving fields; popularity is fetched fresh per query and applied as a second-stage rerank in this service. The pattern's full justification — including pagination, ML rerank framing, and when to combine with Pattern B — is in `doc/search-integration.md`.

## Files

| File | Purpose |
|---|---|
| `rerank.go` | The pure rerank logic. `Service.Search` does recall → counter-fetch → score → top-K. The `Recaller` interface is the seam for swapping OpenSearch in. |
| `cmd/server/main.go` | The HTTP server. Wires a static demo recaller against the Murmur QueryService client. Swap the recaller to implement against your search backend. |
| `rerank_test.go` | 11 unit tests against a real Murmur QueryService (httptest-backed) so the integration path is exercised end-to-end without external infra. |

## Run the tests

```sh
go test ./examples/search-rerank/...
```

Notable test: `TestSearch_WindowedFeatureBoostsRecent` proves the windowed-counter feature pulls a recently-active post above an equally-popular-but-cold post. `TestSearch_DegradesGracefullyWhenMurmurFails` proves the service returns BM25-only results when Murmur is unavailable rather than 500-ing the user.

## Run the server

```sh
# In one terminal, start a Murmur query service (the page-view-counters
# example or any other Counter pipeline).
go run ./examples/page-view-counters/cmd/query

# In another terminal:
go run ./examples/search-rerank/cmd/server \
    --likes-alltime-url http://localhost:50051 \
    --addr :8080

# Hit it:
curl 'http://localhost:8080/search?q=anything'
```

The static recaller returns a fixed set of 10 candidates for any query. To wire OpenSearch in production, implement the `rerank.Recaller` interface — that's the entire seam.

## Score function

The default score is a hand-tuned multiplicative blend of BM25 and log-scaled likes:

```go
func DefaultScore(c Candidate, likesAll, likes24h int64) float64 {
    recencyBoost := 1 + math.Log10(float64(likes24h)+1)
    allTimeBoost := 1 + 0.2*math.Log10(float64(likesAll)+1)
    return c.BM25 * recencyBoost * allTimeBoost
}
```

Replace it for ML rerank. Murmur is invariant under the choice — same `GetMany` / `GetWindowMany` call shape. See `doc/search-integration.md` "Two-pass ML ranking" for the framing.

## Composition with Pattern B

This example pairs cleanly with [`examples/search-projector/`](../search-projector/). Use both:

- **Projector** (Pattern B) keeps a coarse `popularity_bucket` field indexed in OpenSearch for filtering and rough ordering.
- **Rerank service** (Pattern A) does the fine ranking within the bucket via fresh counter fetches.

The recall stage's query becomes:

```
filter: popularity_bucket >= 3
sort:   popularity_bucket DESC, _score DESC, doc_id ASC
limit:  N=200
```

Pagination uses `search_after` on the composite sort key for stable cursor navigation; per-page rerank is N=20 — cheap. See doc/search-integration.md "Pagination" section for the full treatment of cursor-based pagination with external rescore.

## What's NOT in this example

- **OpenSearch integration.** The `Recaller` interface is the seam; production wires it against `opensearch-go/v3` with the AWS-signed transport for managed OpenSearch Service.
- **A learned ranking model.** `ScoreFn` is a simple BM25 × log(likes) function. For production ML rerank, replace it with a call into your inference service (XGBoost endpoint, cross-encoder transformer, LLM). The data flow is unchanged — Murmur provides the counter features regardless.
- **Search-session caches for pagination.** The example doesn't implement the cached-rescored-list pattern from doc/search-integration.md "Pagination". For shallow pagination (page 1–3) it's not needed; for infinite-feed UIs add a Valkey-backed session cache in front of `Search`.
- **Personalization.** Only global features (likes-per-post). Per-(user, item) features need a feature store or an embedding-based recommender — Murmur isn't the right tool for those, see doc/search-integration.md "Where Murmur isn't the answer".
