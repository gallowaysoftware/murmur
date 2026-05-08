# Example: search projector (Pattern B from doc/search-integration.md)

A runnable reference implementation of the **bucketed-indexing** pattern from [`doc/search-integration.md`](../../doc/search-integration.md). This Lambda tails a Murmur counter table via DynamoDB Streams and projects bucket transitions into an OpenSearch index, so the search side can `filter` and `sort` on a coarse-but-cheap popularity field.

## What it does

```
Murmur counter pipeline → DDB → DDB Streams → THIS Lambda → OpenSearch
                                                  │
                                                  └─ on bucket transition only
```

For a counter going from 0 to 1M, the projector emits **6 OpenSearch updates** total (boundary crossings at 10, 100, 1k, 10k, 100k, 1M). Compare to the naive "reindex on every event" pattern: 1M updates. Six orders of magnitude reduction in index write rate.

The bucket function is `floor(log10(v))` by default — see [`pkg/projection`](../../pkg/projection/bucket.go) for `LogBucket`, `LinearBucket`, and `ManualBucket` shapes plus `HysteresisBucket` to suppress flap on documents oscillating around bucket boundaries.

## Files

| File | Purpose |
|---|---|
| `projector.go` | Pure logic: decode DDB Streams record → bucket transition → OpenSearch UpdateDoc. Has zero AWS-specific dependencies; the `IndexClient` interface is the seam for production swaps. |
| `cmd/projector/main.go` | The Lambda binary — wires Murmur's `pkg/exec/lambda/dynamodbstreams` Lambda runtime against the projector and a minimal HTTP-based OpenSearch client. |
| `projector_test.go` | Unit tests with a recording fake `IndexClient`; covers the headline behaviors (bucket transitions, no-op skips, logarithmic reindex budget, tombstones, hysteresis). |

## Run the tests

```sh
go test ./examples/search-projector/...
```

The headline assertion: `TestProjector_LogarithmicReindexBudget` simulates a counter rising from 0 to 1M with ~100 sampled writes and asserts the projector emits 5–8 OpenSearch updates (one per bucket transition), regardless of the underlying event rate.

## Deploy

### Build

The Lambda runtime is `provided.al2` on arm64:

```sh
GOOS=linux GOARCH=arm64 go build -tags lambda.norpc \
    -o bootstrap ./examples/search-projector/cmd/projector
zip lambda.zip bootstrap
```

### Wire-up

DynamoDB Streams must be enabled on the Murmur counter table with the right view type:

```sh
aws dynamodb update-table --table-name page_views \
    --stream-specification 'StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES'
```

Both `OldImage` and `NewImage` are required — the projector decodes both to compute the transition.

Create the event-source mapping:

```sh
aws lambda create-event-source-mapping \
    --event-source-arn <stream-arn> \
    --function-name search-projector \
    --batch-size 100 \
    --maximum-batching-window-in-seconds 5 \
    --function-response-types ReportBatchItemFailures \
    --starting-position LATEST
```

`FunctionResponseTypes=[ReportBatchItemFailures]` is required so a single OpenSearch failure only causes that record to redeliver, not the whole batch.

### Configuration

The Lambda reads from environment:

| Variable | Required? | Default | Purpose |
|---|---|---|---|
| `OPENSEARCH_ENDPOINT` | yes | — | `https://your-domain.us-east-1.es.amazonaws.com` |
| `OPENSEARCH_INDEX` | yes | — | Target index, e.g. `posts` |
| `OPENSEARCH_FIELD` | no | `popularity_bucket` | The numeric field on the document to update. Use a per-counter name when projecting multiple counters into the same index (`post_likes_bucket`, `post_views_bucket`, ...). |
| `HYSTERESIS` | no | `0` (disabled) | Fractional hysteresis band, e.g. `0.10` for 10%. Suppresses flap on documents oscillating around boundaries. See `pkg/projection.HysteresisBucket`. |

## Querying the indexed bucket

In OpenSearch, the field is a regular numeric. Filter and sort on it directly:

```json
GET /posts/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "title": "kubernetes operator" } }
      ],
      "filter": [
        { "range": { "popularity_bucket": { "gte": 3 } } }
      ]
    }
  },
  "sort": [
    { "popularity_bucket": "desc" },
    "_score"
  ]
}
```

`popularity_bucket: 3` corresponds to ≥1k likes. The bucket function is identical between the projector and the search-side filter — codify it once and import from both. See `pkg/projection.LogBucket`.

## Composing with hierarchical rollups

If your Murmur pipeline uses [`KeyByMany`](../../pkg/pipeline/pipeline.go) for hierarchical rollups (`post:<id>` + `post:<id>|country:US` + `country:US` + `global`), the projector receives one DDB Streams record per emitted key. The current single-binary implementation projects all of them into the same OpenSearch field; for cleanly separating per-cohort filters (`popularity_bucket_US` vs `popularity_bucket`), dispatch on the `pk` shape inside the projector and route to different fields. This is straightforward to write but needs to be designed alongside the OpenSearch query patterns. See doc/search-integration.md "Composing patterns: hierarchical rollups + bucketing."

## What's NOT in this example

- **A search service.** The projector is just the write-side. The query-side is whatever your search application already is — it just gains the `popularity_bucket` field to filter/sort on, plus the option to layer Pattern A's query-time rescore on top via `pkg/query.GetMany` for finer ranking within a bucket.
- **Cross-invocation hysteresis state.** Hysteresis here suppresses oscillation within a single Lambda invocation. For multi-invocation oscillation (the same document flapping across batches), persist the previous bucket per entity in a small DDB table; the projector looks it up per record. Not yet wired.
- **The slow-moving-field projector.** The same OpenSearch documents need their text content updated when the user edits their post body — that's a separate projector fed from the OLTP-side change stream. Two projectors, one OpenSearch index. Layered cleanly because partial-update only touches the field you specify.
- **OpenSearch IAM auth.** The bundled `openSearchClient` is a minimal HTTP client — fine for non-IAM-protected endpoints (master-user auth or local clusters). For Amazon OpenSearch Service with SigV4 IAM auth, swap in `opensearch-go/v3` with the AWS-signed transport. The `IndexClient` interface is the seam.
