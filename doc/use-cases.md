# Use cases & usage patterns

Murmur is a streaming aggregation framework, but framework-shaped
docs don't always answer the question you actually have: *what kind
of problem is this for, and how do I wire it up?* This page is the
shape-oriented complement to [`README.md`](../README.md) (status &
quick taste), [`architecture.md`](architecture.md) (load-bearing
ideas), and [`design.md`](design.md) (the deep treatment). It
catalogues the common pipeline shapes, picks the right monoid /
runtime / state-store for each, and walks the **CDC + backfill**
pattern end-to-end as the worked example.

If you're new, skim the [quick decision guide](#quick-decision-guide)
first, then jump to the section that matches your problem.

---

## Quick decision guide

| You want… | Reach for |
|---|---|
| "Count events per X" (likes, page views, errors) | [`murmur.Counter`](#1-real-time-counters-page-view-style) |
| "How many *unique* X" (distinct visitors, devices) | [`murmur.UniqueCount`](#2-unique-cardinality-hll) |
| "Top-N most-popular X" (trending products, hot endpoints) | [`murmur.TopN`](#3-top-n-feeds-misra-gries) |
| "Have we already seen this X" (dedup, fingerprinting, allow-lists) | [`bloom.Bloom`](#4-membership--seen-this-before-bloom) |
| "What's trending *right now*" (with time-decay) | [`murmur.Trending`](#5-trending-with-time-decay) |
| Same pipeline fed by *two* sources at once (Kafka + Kinesis) | [Multi-source aggregation](#6-multi-source-aggregation-one-row-two-feeds) |
| Backfill years of history into a fresh table | [CDC + backfill](#cdc--backfill-the-end-to-end-walkthrough) |
| Combine batch totals with realtime deltas | [Lambda merge](#7-lambda-batch-view--realtime-delta) |
| Pair counters with full-text search | [Search rerank](#8-search-rerank) |

Every shape on this page is a thin wrapper over the same
DSL (`pkg/pipeline.NewPipeline[T,V]`). The presets in `pkg/murmur`
just pre-pick the monoid + value extractor. Drop down to the DSL if
the preset doesn't fit.

---

## The eight common shapes

### 1. Real-time counters (page-view style)

**Problem.** "Count views per page per day, query the trailing 7/30/90
days." The canonical Murmur shape.

**Pipeline.**

```go
import (
    "time"
    "github.com/gallowaysoftware/murmur/pkg/murmur"
)

pipe := murmur.Counter[PageView]("page_views").
    From(src).
    KeyBy(func(e PageView) string { return e.PageID }).
    Daily(90 * 24 * time.Hour).         // 90-day daily-bucket retention
    StoreIn(store).
    Build()
```

**Monoid.** `core.Sum[int64]` under the hood — `Counter` hardcodes
`Value(func(T) int64 { return 1 })`. For monetary or weighted sums,
drop to `pipeline.NewPipeline` and supply your own `Value` extractor.

**State.** `dynamodb.Int64SumStore` — atomic `UpdateItem ADD`, scales
to ~5–10k events/s/worker against DDB. Pair with
`valkey.Int64Cache` for sub-millisecond reads.

**Caveats.**

- Sum is **not** idempotent under at-least-once delivery. Pair with
  `streaming.WithDedup(d)` (a [`Deduper`](../pkg/state/dynamodb/))
  to absorb Kafka rebalance redeliveries.
- Minute-granularity windows have high read-amplification on long
  ranges (1440 buckets/day); daily is the default for a reason.

**Runnable example.** [`examples/page-view-counters/`](../examples/page-view-counters/)
— full worker + query binaries + Dockerfile + Terraform.

---

### 2. Unique cardinality (HLL)

**Problem.** "How many distinct users visited this page today?"
Exact count is prohibitive at scale; HyperLogLog gives ~1.6% error
in fixed (~12 KB) memory per sketch.

**Pipeline.**

```go
pipe := murmur.UniqueCount[PageView]("unique_visitors").
    From(src).
    KeyBy(func(e PageView) string { return e.PageID }).
    ElementBy(func(e PageView) []byte { return []byte(e.UserID) }).
    Daily(30 * 24 * time.Hour).
    StoreIn(bytesStore).
    Build()
```

**Monoid.** `hll.HLL()` — axiomhq/hyperloglog under the hood,
encoded as bytes. Combine is set-union of the register arrays.

**State.** `dynamodb.BytesStore` (CAS-with-retry).

**Acceleration.** `valkey.HLLCache` runs side-by-side with the
BytesStore for hot-path PFCOUNT (Valkey-native, server-side, no
unmarshal round-trip). Independent estimator of the same set, both
within ~1.6% error.

**Caveats.**

- HLL is also not idempotent — pair with `WithDedup`.
- Windowed unique-cardinality across N buckets is the union over N
  HLLs (server merges via `Combine`), not the sum of per-bucket
  cardinalities.

---

### 3. Top-N feeds (Misra-Gries)

**Problem.** "Top-10 most-clicked products, last hour." Misra-Gries
sketch tracks the K heaviest hitters in O(K) memory.

**Pipeline.**

```go
pipe := murmur.TopN[ProductClick](10, "top_products").
    From(src).
    KeyBy(func(c ProductClick) string { return c.CategoryID }).
    ElementBy(func(c ProductClick) string { return c.ProductID }).
    Hourly(24 * time.Hour).
    StoreIn(bytesStore).
    Build()
```

**Monoid.** `topk.New(k)` — merges two Misra-Gries summaries into
one summary of the same K. Approximate but predictable: any item
that exceeded `1/K`-of-stream-weight is guaranteed present.

**Typed reads.** `typed.TopKClient.GetWindow(ctx, "books", 24*time.Hour)`
returns `[]TopKItem{Key, Count}` ordered by count.

**Caveats.**

- TopK is non-idempotent (an event's contribution can shift the
  Counts column); pair with `WithDedup`.
- The K you pick is fixed for the pipeline's lifetime. Cross-K merges
  are not defined — sketches with mismatched K refuse to merge.

---

### 4. Membership / "seen this before" (Bloom)

**Problem.** "Has this device fingerprint already triggered this
notification today?" Probabilistic membership with a tunable false-
positive rate; no false negatives.

**Pipeline (DSL form — there's no facade preset for Bloom yet):**

```go
import (
    "github.com/gallowaysoftware/murmur/pkg/monoid/sketch/bloom"
    "github.com/gallowaysoftware/murmur/pkg/pipeline"
)

pipe := pipeline.NewPipeline[Notification, []byte]("seen_devices").
    From(src).
    Key(func(n Notification) string { return n.NotificationID }).
    Value(func(n Notification) []byte {
        return bloom.Single([]byte(n.DeviceID))
    }).
    Aggregate(bloom.Bloom(), windowed.Daily(24 * time.Hour)).
    StoreIn(bytesStore)
```

**Monoid.** `bloom.Bloom()` (default 100k capacity, p=0.01). Use
`bloom.NewWithCapacity(n, p)` for larger sets.

**Acceleration.** `valkey.BloomCache` exposes `BF.ADD` / `BF.MADD` /
`BF.EXISTS` / `BF.MEXISTS` against the valkey-bloom module for
single-digit-ms membership checks. The docker-compose stack ships
`valkey/valkey-bundle:8` which includes the module; bare `valkey`
will reject `BF.*` with "unknown command".

**Caveats.**

- The BloomCache and the BytesStore-authoritative bits-and-blooms
  filter are *independent* probabilistic estimators of the same set;
  a specific element may be reported present in one and absent in
  the other (both within the configured FPR). Pin reads to one side
  if strict deduplication matters.

---

### 5. Trending (with time-decay)

**Problem.** "Top-K but recent activity counts more." Exponential
decay over an event's age.

**Pipeline.**

```go
pipe := murmur.Trending[Click](
    "trending_articles",
    24 * time.Hour, // half-life
).
    From(src).
    KeyBy(func(c Click) string { return c.ArticleID }).
    Daily(7 * 24 * time.Hour).
    StoreIn(bytesStore).
    Build()
```

**Monoid.** `compose.DecayedSumBytes` — exponentially-decayed sum
serialized as bytes. Half-life is the wall-clock period over which
an event's contribution drops by half.

**Caveats.**

- Decay associativity is approximate (FP-rounding-bounded); see
  `STABILITY.md`'s row on `pkg/monoid/compose`.
- The "now" used for decay comes from `EventTime` on each record,
  not processing-time. Late-arriving events still get their original
  decay applied — they don't artificially count as "fresh."

---

### 6. Multi-source aggregation (one row, two feeds)

**Problem.** "Top-K recently-interacted products, where interactions
arrive from *both* the Kinesis click stream and the Kafka
purchase-event stream." Two sources of truth, one merged view.

**Pattern.** Two binaries — one streaming worker per source — both
writing through the **same DDB row** via the same monoid Combine.
The DDB store's CAS-with-retry semantics (for BytesStore) or atomic
ADD (for Int64SumStore) make concurrent writes from different
binaries safe.

```go
// Worker A: Kinesis Lambda handler
murmur.MustKinesisHandler(buildPipeline(clickDecoder))

// Worker B: Kafka ECS worker
murmur.RunStreamingWorker(ctx, buildPipeline(purchaseDecoder))
```

Both call `buildPipeline()` with the same monoid (`topk.New(10)`),
same key extractor (`func(e Event) string { return e.CategoryID }`),
same DDB table. The store's `MergeUpdate` semantics ensure neither
worker's contribution is lost.

**Runnable example.** [`examples/recently-interacted-topk/`](../examples/recently-interacted-topk/).

---

### 7. Lambda batch view ⊕ realtime delta

**Problem.** "Nightly Spark job computes per-customer totals from
the data lake; live worker tracks the delta since the batch ran;
queries need to return the merged sum."

**Pattern.** Two state tables — `customer_totals_batch` (rebuilt
nightly by Spark) and `customer_totals_realtime` (continuously
updated by the streaming worker). `pkg/query.LambdaQuery` reads
both and merges via the monoid's Combine.

```go
lambda := query.NewLambdaQuery(
    batchStore,     // Spark-rebuilt nightly
    realtimeStore,  // live worker
    core.Sum[int64](),
)
total, _, _ := lambda.Get(ctx, "customer-A")
```

**Cutover.** When the nightly batch finishes, `pkg/swap` atomically
points the alias at the new batch table and the realtime store's
window is reset (typically to the batch run's high-water-mark
timestamp). The Terraform module's `swap_enabled = true` mode
provisions the control table and IAM; see
[`deploy/terraform/modules/pipeline-counter`](../deploy/terraform/modules/pipeline-counter/).

---

### 8. Search rerank

**Problem.** "OpenSearch returns relevance-ranked results; the
business wants live-counter signal (likes, views) to influence
ranking." Two-stage retrieval pattern.

**Pattern A (rescore).** OpenSearch fetches `K * N` candidates by
text relevance; a service hits Murmur's `GetMany` to fetch live
counters per candidate; sorts in-memory by a weighted score.
Runnable: [`examples/search-rerank/`](../examples/search-rerank/).

**Pattern B (bucketed projection).** A `pkg/projection` Lambda tails
DDB Streams on the counter table and projects log-bucket transitions
into an OpenSearch index field, so queries can `filter` on
`popularity_bucket: 6` instead of paying the per-event reindex cost.
Runnable: [`examples/search-projector/`](../examples/search-projector/).

Both patterns plus a third (`snapshot+delta`) are covered in detail
in [`doc/search-integration.md`](search-integration.md).

---

## CDC + backfill: the end-to-end walkthrough

This is the pattern people most often ask Murmur to solve, and it
demonstrates almost every architectural piece in one diagram.

### The problem

Your source of truth is a primary database — Mongo, Postgres, DDB
itself. You want a Murmur pipeline that aggregates over the table:
per-customer order totals, per-product like counts, whatever. You
have two facts to reconcile:

1. **The table already has years of history.** A live CDC stream
   only emits *changes*; it doesn't re-emit rows that existed
   before the stream started. If the consumer attaches today, every
   row inserted yesterday is invisible to it.
2. **The stream is continuous after that.** You don't want to
   reload the whole table on every restart.

The fix is the classic *snapshot then stream* pattern: do one
**bootstrap pass** over the table that folds every existing row
through the pipeline, then hand off to a **live consumer** of the
CDC stream that picks up from a checkpoint captured at the start of
the snapshot. No gaps, no double-counting beyond what dedup
absorbs.

```
┌────────────────┐
│  Source-of-    │  orders / events / users / …
│  truth table   │
└────────┬───────┘
         │
         │ ① CaptureHandoff() → resume token / Streams ARN / Kafka offset
         ▼
┌────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ pkg/exec/      │───▶│ Same pipeline    │───▶│ State store      │
│ bootstrap      │    │ definition       │    │ (DDB)            │
└────────────────┘    │ (KeyBy, Monoid,  │    └──────────────────┘
                      │  StoreIn)        │            ▲
                      └──────────────────┘            │
                                                      │
         ── handoff token threaded into ─┐            │
                                         ▼            │
                                ┌──────────────────┐  │
                                │ CDC source       │  │
                                │ (Mongo Change    │  │
                                │  Streams /       │  │
                                │  Debezium→Kafka /│  │
                                │  DDB Streams)    │  │
                                └────────┬─────────┘  │
                                         │            │
                                         ▼            │
                                ┌──────────────────┐  │
                                │ pkg/exec/        │  │
                                │ streaming +      │──┘
                                │ WithDedup        │
                                └──────────────────┘
```

### Why the pieces are exactly what they are

- **Bootstrap mode.** `pkg/exec/bootstrap.Run` reads from a
  `snapshot.Source` — Mongo collection scan, DDB ParallelScan,
  JSON-Lines from S3, Parquet from a Spark export — and feeds every
  row through the pipeline's monoid Combine. It uses the same
  `pkg/exec/processor` core as the streaming runtime, so retries,
  dedup, metrics, and `KeyByMany` fanout all behave identically.

- **Handoff token.** Every snapshot source implements
  `CaptureHandoff(ctx) (snapshot.HandoffToken, error)`. The
  bootstrap binary calls this *before* the scan starts so the token
  marks a point in time strictly *behind* the scan's first row.
  - Mongo: a Change Streams resume token.
  - DDB Streams source: a Streams shard timestamp.
  - JSON-Lines / S3 / Parquet: caller-supplied (typically the
    Kinesis shard timestamp or Kafka offset captured externally
    when the dump was taken).

- **Live mode.** `pkg/exec/streaming.Run` consumes the CDC source
  from the handoff token. The first event it observes is the first
  change since the snapshot began. Everything older was covered by
  the bootstrap pass.

- **Dedup.** The overlap window between bootstrap and live — the
  span from "scan started" to "handoff token" — can re-deliver some
  records. For non-idempotent monoids (Sum, HLL, TopK)
  `streaming.WithDedup(d)` against a DDB-backed `Deduper` makes
  this an exactly-once effect at the monoid level. Configure the
  `EventID` extractor to use the **upstream primary key** (Mongo
  `_id`, Postgres LSN, etc.) — *not* the Kafka offset, which
  changes on Debezium re-delivery.

### The pipeline definition (shared by both binaries)

The reason the pattern works cleanly is that the **pipeline
definition is execution-mode-agnostic**. One function describes the
schema, key extractor, value extractor, monoid, and store; both
binaries import it and only differ in which runtime they call.

```go
// pipeline.go — shared by cmd/bootstrap and cmd/worker
package orderstats

import (
    "context"

    "github.com/gallowaysoftware/murmur/pkg/monoid/core"
    "github.com/gallowaysoftware/murmur/pkg/pipeline"
    "github.com/gallowaysoftware/murmur/pkg/source/snapshot/mongo"
    mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
    "github.com/gallowaysoftware/murmur/pkg/state"
    mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type Order struct {
    ID         string `bson:"_id" json:"_id"`
    CustomerID string `bson:"customer_id" json:"customer_id"`
    Amount     int64  `bson:"amount" json:"amount"`
}

// BuildBootstrap wires the bootstrap source.
func BuildBootstrap(ctx context.Context, cfg Config) (*pipeline.Pipeline[Order, int64], *mongo.Source[Order], state.Store[int64], error) {
    store := mddb.NewInt64SumStore(ddbClient(cfg), cfg.DDBTable)
    src, err := mongo.NewSource(ctx, mongo.Config[Order]{
        URI: cfg.MongoURI, Database: cfg.MongoDB, Collection: cfg.MongoCollection,
        Decode: mongo.BSONDecoder[Order](),
    })
    if err != nil { return nil, nil, nil, err }

    pipe := pipeline.NewPipeline[Order, int64]("order_totals").
        Key(func(o Order) string { return o.CustomerID }).
        Value(func(o Order) int64 { return o.Amount }).
        Aggregate(core.Sum[int64]()).
        StoreIn(store)
    return pipe, src, store, nil
}

// BuildLive wires the CDC source. Note the EventID extractor pulls
// the upstream Mongo _id so Debezium retransmits fold idempotently
// (a single logical change may land at multiple Kafka offsets).
func BuildLive(ctx context.Context, cfg Config) (*pipeline.Pipeline[Order, int64], state.Store[int64], state.Deduper, error) {
    store := mddb.NewInt64SumStore(ddbClient(cfg), cfg.DDBTable)
    deduper := mddb.NewDeduper(ddbClient(cfg), cfg.DDBDedupTable, 24*time.Hour)

    src, err := mkafka.NewSource(mkafka.Config[Order]{
        Brokers: brokers(cfg), Topic: cfg.KafkaTopic, ConsumerGroup: cfg.ConsumerGroup,
        Decode:  mkafka.JSONDecoder[Order](),
        EventID: func(o Order) string { return o.ID }, // <-- upstream PK, not Kafka offset
    })
    if err != nil { return nil, nil, nil, err }

    pipe := pipeline.NewPipeline[Order, int64]("order_totals").
        From(src).
        Key(func(o Order) string { return o.CustomerID }).
        Value(func(o Order) int64 { return o.Amount }).
        Aggregate(core.Sum[int64]()).
        StoreIn(store)
    return pipe, store, deduper, nil
}
```

`Key`, `Value`, `Aggregate`, `StoreIn` are identical between the
two builders. Only the source and the dedup wiring differ.

### The two binaries

```go
// cmd/bootstrap/main.go — runs once, captures a handoff token, exits.
func main() {
    ctx := context.Background()
    pipe, src, _, err := orderstats.BuildBootstrap(ctx, cfg)
    must(err)
    defer src.Close()

    token, err := src.CaptureHandoff(ctx)
    must(err)
    fmt.Fprintf(os.Stderr, "handoff token: %x\n", token)

    if err := bootstrap.Run(ctx, pipe, src); err != nil {
        log.Fatal(err)
    }
    // Persist the token somewhere the live worker can read it
    // (S3 object, SSM parameter, a DDB row, …).
    persistToken(token)
}
```

```go
// cmd/worker/main.go — long-running CDC consumer.
func main() {
    ctx := context.Background()
    pipe, _, deduper, err := orderstats.BuildLive(ctx, cfg)
    must(err)

    rc := murmur.RunStreamingWorker(ctx, pipe,
        streaming.WithDedup(deduper),
        // For Mongo Change Streams driven directly (not via Debezium),
        // additionally:
        //   mongo.WithStartAfter(loadToken()),
    )
    os.Exit(rc)
}
```

### Production wiring

In production the CDC stream is almost always Debezium → Kafka, not
direct Mongo Change Streams. Debezium's MongoDB connector emits
per-document JSON envelopes (`before`, `after`, `op`, `source.ts_ms`)
on a topic like `dbserver.shop.orders`. Your `Decode` unwraps the
envelope:

```go
type DebeziumEnvelope struct {
    Op     string `json:"op"`     // "c" insert, "u" update, "d" delete
    After  Order  `json:"after"`
    Source struct {
        TsMs int64 `json:"ts_ms"`
    } `json:"source"`
}

mkafka.Config[Order]{
    // ...
    Decode: func(b []byte) (Order, error) {
        var env DebeziumEnvelope
        if err := json.Unmarshal(b, &env); err != nil {
            return Order{}, err
        }
        if env.Op == "d" {
            // Deletes: emit a compensating event if the monoid is
            // delta-shaped (Sum), or return an ErrSkip if the
            // monoid is absolute (Max, Set, Last).
            return Order{}, source.ErrSkipRecord
        }
        return env.After, nil
    },
    EventID: func(o Order) string { return o.ID },
}
```

For PostgreSQL the shape is identical with Debezium's Postgres
connector and `source.lsn` as the EventID. For DDB itself the
analogue is the `pkg/exec/lambda/dynamodbstreams` runtime, which
receives the full change record and unpacks `NewImage` / `OldImage`
directly.

### Rebuilding the table from scratch

When the source-of-truth changes shape (schema migration, monoid
change, new windowed retention), you don't need downtime. The
pattern:

1. Provision a fresh state table `<alias>_v2` (the existing one is
   conventionally `<alias>_v1`).
2. Run a fresh bootstrap against the live source-of-truth, writing
   into `<alias>_v2`. Concurrently, the live v1 worker keeps
   updating `<alias>_v1` from CDC, so queries continue to be
   correct against v1.
3. When the v2 bootstrap catches up (and a brief CDC tail-replay
   into v2 closes the gap), call
   `swap.Manager.SetActive(ctx, alias, 2)` — this atomically advances
   the alias pointer.
4. Query servers refresh their alias resolution and start reading
   v2. Old v1 can be dropped after the read-grace period.

The Terraform module's `swap_enabled = true` mode provisions the
control table, IAM, and `SWAP_CONTROL_TABLE` / `SWAP_ALIAS` env
vars consumed by the binaries; see
[`deploy/terraform/modules/pipeline-counter/swap.tf`](../deploy/terraform/modules/pipeline-counter/swap.tf).

### Runnable example

[`examples/mongo-cdc-orderstats/`](../examples/mongo-cdc-orderstats/)
is the worked version of everything above. The
[`test/e2e/mongo_cdc_orderstats_test.go`](../test/e2e/mongo_cdc_orderstats_test.go)
e2e test seeds Mongo, runs Bootstrap, produces additional events to
Kafka, runs the live worker briefly, and verifies the merged
per-customer totals equal `bootstrapTotals + liveDeltas` to the
cent.

For a backfill that reads **Parquet snapshots** produced by a Spark
job rather than a live Mongo collection, see
[`examples/backfill-from-spark/`](../examples/backfill-from-spark/).
For an S3 archive replay (the kappa pattern, when Kinesis source
retention is shorter than the desired backfill window), see the
[`pkg/replay/s3`](../pkg/replay/s3/) docs.

---

## Operational checklist

Before you ship a Murmur pipeline to production:

- [ ] **Pick the runtime.** Kafka ECS worker
      (`murmur.RunStreamingWorker`) or AWS Lambda
      (`murmur.MustKinesisHandler` / `MustDynamoDBStreamsHandler` /
      `MustSQSHandler`). Kinesis production path is Lambda, **not**
      ECS — the polling `pkg/source/kinesis` is dev-only.
- [ ] **Dedup if non-idempotent.** Sum / HLL / TopK / DecayedSum all
      need `streaming.WithDedup(d)` paired with a DDB-backed
      `Deduper`. Idempotent monoids (Set, Min, Max, Bloom) are fine
      without it.
- [ ] **Plumb metrics.** `streaming.WithMetrics(rec)` +
      `state.NewInstrumented(store, rec)` give per-pipeline
      throughput / errors / store-latency. The `pkg/admin` server
      shows them; `pkg/observability/autoscale` emits scaling
      signals from them.
- [ ] **Close the admin server.** `pkg/admin` has CORS *and* auth
      off by default. Either run it behind an authenticating
      reverse proxy, or pass `WithAuthToken(...)` /
      `WithJWTVerifier(...)` and `WithAllowedOrigins(...)` —
      especially if you're exposing it beyond the VPC.
- [ ] **Wire scaling signals.**
      [`autoscale.EventsPerSecond`](../pkg/observability/autoscale/)
      against `pkg/metrics` emits CloudWatch metrics suitable for
      ECS Fargate target-tracking.
- [ ] **Enable swap if you'll ever backfill.** The Terraform module's
      `swap_enabled = true` opt-in costs you one extra DDB table
      and a few IAM lines today; without it, rebuilding the state
      table requires a manual cutover.
- [ ] **Decide your retention.**
      `windowed.Daily(retention)` sets a TTL on each row; DDB's
      native TTL evicts. Pick `max(query-window) + slack`.

## Further reading

- [`README.md`](../README.md) — status table, quick taste, build
  instructions.
- [`STABILITY.md`](../STABILITY.md) — per-package experimental /
  mostly-stable matrix and sharp edges.
- [`doc/architecture.md`](architecture.md) — load-bearing concepts
  with the reasoning behind them.
- [`doc/design.md`](design.md) — the 19-section deep design
  treatment (3000+ lines with diagrams).
- [`doc/search-integration.md`](search-integration.md) — three
  patterns for combining counters with text search.
- [`AGENTS.md`](../AGENTS.md) — repo-wide instructions for AI
  coding agents working in the codebase.
