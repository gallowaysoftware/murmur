# Murmur: Lambda-Architecture-Aware Streaming Aggregation Framework for Go

> Repo: `gallowaysoftware/murmur` &middot; License: Apache 2.0

## Context

Kyle is building a Summingbird-spiritual-successor in Go for 2026. The core problem: maintaining stateful aggregations (counters, cardinality, top-K) over Kinesis/Kafka event streams, with the ability to backfill or recompute history, queryable via gRPC. Existing options have gaps:

- **Apache Storm / Heron**: effectively dead. Heron stalled in Apache Incubator, Storm is legacy.
- **Scalding**: abandoned by Twitter.
- **Apache Beam on AWS**: deployment is painful; Dataflow is the only first-class runner and that's GCP-only.
- **Apache Flink (incl. Amazon Managed Service for Apache Flink)**: powerful and mature, but Java/Scala/Python only — no Go. JVM tax for Go shops, and no auto-generated query layer.
- **Goka**: Kafka-only, no batch story, no query layer, small community.
- **reugn/go-streams, gstream, etc.**: closer to Akka-Streams DSLs than serious aggregation frameworks.

There's a real gap in Go for: *unified pipeline DSL + lambda-architecture-aware deployment + auto-generated gRPC query service* for counter-class aggregations on AWS.

### Why not Apache Beam

Worth recording the rationale, since "just use Beam" is the obvious counter-proposal:

1. **Go SDK is unmaintained.** Per the [Beam Go SDK Roadmap](https://beam.apache.org/roadmap/go-sdk/), the SDK *"is not actively being developed beyond bugfixes due to lack of contributors. Version 2.32.0 is the last experimental release of the Go SDK."* Still flagged experimental. Building a production framework on top of this is not defensible.
2. **Spark runner is batch-only.** Per the [Beam Spark Runner docs](https://beam.apache.org/documentation/runners/spark/): *"The Spark Runner is still experimental with partial coverage of the Beam model and currently only supports batch mode."* Streaming on EMR Spark via Beam is impossible — only the Flink runner supports streaming, which puts us back in Flink territory anyway.
3. **EMR deployment is operationally heavy.** Fat JARs, spark-submit via EMR Steps, and for Python pipelines a Docker-spawn permissions dance the hadoop user fails by default. EMR Serverless has no documented native Beam-runner support as of late 2025.
4. **Cross-language SDK perf tax.** The Go SDK runs everything through the portability framework's gRPC protocol, marshaling each record across language boundaries. Java meaningfully outperforms Go on Spark/Flink Beam runners because of this.

Beam's promise — write once, run on any runner — is real on Dataflow (GCP), partially real on Flink-anywhere, and a fiction on Spark-streaming. For a Go-first, AWS-first counter framework in 2026, Beam isn't a fit.

The framework's three value propositions, all required to justify building (vs. just using Flink + a hand-rolled Lambda + DDB):
1. **Unified Go programming model** — one pipeline definition, multiple execution drivers (live stream, Kafka replay, S3 replay, batch backfill).
2. **Auto-generated query layer** — gRPC service that correctly merges batch view + realtime delta using the pipeline's monoid Combine. This is the layer nobody else has built.
3. **AWS-opinionated deployment** — Terraform/CDK modules so deploying a counter pipeline is a few lines, not a quarter of platform-eng work.

## Architecture Recommendation

### Programming model: structural-monoid pipeline DSL

Pipeline = `Source → KeyExtract → Aggregator[K, V] → StateStore → QueryService`, where `Aggregator[K, V]` requires `Combine(V, V) V` satisfying associativity. Implemented with Go generics. Execution-mode-agnostic.

**Critical design point: monoids are structural, not opaque Go code.** Well-known monoids (`Sum`, `Min`, `Max`, `HLL`, `TopK`, `Bloom`, `Set`, `MapMerge`) are recognized symbols, each with multiple backend implementations: Go (for ECS workers and Go-batch), Spark (for EMR backfill via spark-connect-go or Scala codegen), Valkey-native (for sketch-accelerated paths), DDB-native (atomic adds, conditional writes). Custom monoids implemented as opaque Go closures work on Go execution backends only; users opt out of Spark/Valkey acceleration when they go custom. This is how algebird-spark achieved cross-runtime correctness, and it's the only way to honor the Summingbird "one DSL, multiple engines" promise.

**Windowed monoids are first-class.** The most common counter use case in practice isn't "count all-time," it's "count last 30 days" or "count last 7 days." Murmur ships a `Windowed[M Monoid]` wrapper that adds a time-bucket dimension to the state key. The underlying monoid is unchanged — `Windowed[Counter]`, `Windowed[HLL]`, `Windowed[TopK]` all work the same way. Buckets are tumbling at a configured granularity (typically daily or hourly); queries assemble sliding windows by merging the N most recent buckets via the monoid Combine. DDB TTL handles bucket eviction past the retention horizon. This is the same trick Druid and Pinot use for time-series rollups; we expose it as a single DSL option.

```go
// sketch — final shape TBD
pipeline := murmur.NewPipeline("page_views").
    From(sources.Kinesis("events-stream")).
    Key(func(e Event) string { return e.PageID }).
    Aggregate(
        monoids.Counter,
        murmur.WindowDaily(retention: 90*24*time.Hour),  // daily buckets, 90-day retention
    ).
    StoreIn(state.DynamoDB("page_views_table")).
    Cache(state.Valkey("hot-counters")).         // optional read/sketch accelerator
    ServeOn(query.GRPC(":50051"))

// Generated query service exposes:
//   Get(key, AllTime)
//   GetWindow(key, Last7Days)        // merges 7 daily buckets
//   GetWindow(key, Last30Days)       // merges 30 daily buckets
//   GetRange(key, start, end)        // merges buckets in range
```

For the all-time use case, omit the window option:

```go
.Aggregate(monoids.Counter)   // single value per key, no bucketing
```

### Execution modes: Bootstrap → Live → Replay

Three first-class execution modes, all driving the same pipeline DSL:

1. **Live**: streaming consumer reads Kinesis/Kafka, applies monoid combine to state.
2. **Bootstrap**: one-shot driver scans a source-of-truth datastore (Mongo collection, DDB table, JDBC, S3 dump) and emits synthetic events into the same combine logic. Populates initial state when the live stream lacks full history (typical for CDC sources from Mongo/Dynamo). Mirrors Debezium's "incremental snapshot" pattern — chunked, watermarked, non-blocking, can run in parallel with live capture.
3. **Replay**: driver reads from Kafka offset range or S3-archived events (Firehose-/MSK-Connect-partitioned) and feeds through the pipeline. For backfill into a new state table, then atomic swap.

The pipeline DSL doesn't know which mode it's in. The orchestration is a deployment-time concern.

### Default architecture: pluggable kappa-first, lambda opt-in

- **Default: Kappa with bootstrap.** Pipeline runs Bootstrap once at first deploy (or on-demand re-bootstrap), then Live indefinitely. Backfill = Replay into a fresh state table, swap atomically.
- **Opt-in: Lambda mode.** Same DSL deploys *additionally* to a scheduled batch executor that runs over S3 archive. Query layer merges `batch_view ⊕ realtime_delta` via the user's monoid. Engaged when users hit petabyte-scale backfill cost or want strict cost-tier separation.

### Streaming runtime (the "engine")

**Not building Flink-in-Go.** Two deployment shapes share one pipeline DSL, one retry/dedup/metrics core (`pkg/exec/processor`), and one state-store contract:

- **Kafka / MSK** → long-running ECS Fargate workers via `pkg/exec/streaming.Run` (franz-go). Kafka's consumer-group coordination is built into the broker; ECS is the natural fit for a persistent connection.
- **Kinesis / DynamoDB Streams / SQS** → AWS Lambda via `pkg/exec/lambda/{kinesis,dynamodbstreams,sqs}`. Lambda's event-source mapping owns shard discovery, lease coordination, autoscaling on shard count (via `ParallelizationFactor`), checkpointing, and partial-batch retry (`BatchItemFailures`). We deliberately do NOT bring KCL v3 Go in-tree — Lambda is operationally cheaper and the supported production path for AWS-native event sources.

Both shapes write through the same DDB-backed `state.Store`, so a single pipeline can ingest from BOTH a Kinesis Lambda and an ECS Kafka worker simultaneously (`examples/recently-interacted-topk/`). **Default semantics: at-least-once with per-event-ID dedup** (recently-seen event IDs tracked in DDB with TTL; duplicates within the dedup window are dropped). Exactly-once via DDB transactions is queued as a Phase 2 extension for users who can't tolerate the dedup-window edge cases.

For users with Flink-class needs (complex joins, watermark semantics, exactly-once across stages), the framework points them at Amazon Managed Service for Apache Flink — we don't try to be it.

### Batch runtime: three executors, one DSL

Day 1 ships three batch backends, picked per-pipeline at deploy time:

1. **`ECSFargateExecutor`**: pure Go, runs the exact same monoid combine code as streaming. Scales to single-digit TB before becoming uncomfortable. The "no-Spark, no-EMR-cluster" path. Good for users without an EMR investment.
2. **`SparkConnectExecutor`**: uses [`pequalsnp/spark-connect-go`](https://github.com/pequalsnp/spark-connect-go) (Kyle's fork — known-stable in production at GSS) to dispatch DataFrame operations against a *user-supplied* persistent EMR cluster's Spark Connect server. Best for ad-hoc backfills, interactive re-aggregation, and users with persistent EMR (Kyle's case). Framework treats Spark Connect server bring-up as out-of-scope and connects to a known endpoint configured at deploy time. The fork is what production validates against; upstream `apache/spark-connect-go` may also work but isn't the support contract.
3. **`SparkScalaCodegen`**: generates Scala from well-known monoid pipelines, submits as an EMR Step (or to EMR Serverless Spark for non-persistent). More brittle (we maintain a codegen template) but more battle-tested for petabyte/scheduled batch. Used when SparkConnectExecutor's interactive style isn't right.

Custom monoids (opaque Go closures) only work on `ECSFargateExecutor`. Well-known monoids work on all three.

### State store: DDB is source of truth, Valkey is a cache

- **DynamoDB**: always source of truth for state. Atomic counters via `UpdateItem ADD`, conditional writes for non-commutative monoidal merges, point queries with millisecond latency. Durable, multi-AZ, fully managed.
- **Valkey** (BSD-licensed Linux Foundation fork; default over Redis OSS post-AGPL): optional cache + sketch-accelerator layer. Native HLL (PFADD/PFCOUNT/PFMERGE), native Bloom (Valkey 8), sorted-sets for Top-K, multithreaded I/O. Pattern: write-through to DDB + Valkey on event ingestion; periodic Valkey-state snapshot back to DDB for durability. On Valkey node loss, repopulate from DDB; degrade gracefully to DDB-only-with-CAS during recovery.
- **AWS MemoryDB** (Phase 3+): for users who want durable in-memory (durable transaction log before the in-memory apply). Drop-in alternative to Valkey when durability requirements rule out cache semantics.

This reframing means: Valkey is never trusted as ground truth. If Valkey goes away tomorrow, every pipeline can rebuild its accelerator state from DDB. That's the right invariant.

### Query layer (the differentiator)

Two-tier surface, both built and shipping:

1. **Generic Connect-RPC server** (`pkg/query/grpc`) — one service definition (`proto/murmur/v1/query.proto`) covers every pipeline. Values are byte-encoded so the same handler serves Sum / HLL / TopK / Bloom / custom. RPCs: `Get`, `GetMany`, `GetWindow`, `GetWindowMany`, `GetRange`, `GetRangeMany`. Singleflight coalescing, per-RPC metrics, `fresh_read` flag, optional Valkey-cache fronting.
2. **Typed per-pipeline codegen** (`cmd/murmur-codegen-typed`) — YAML pipeline-spec → typed `.proto` + Go Connect-RPC server stub that delegates to `pkg/query/typed` and returns the per-kind shape directly (`int64`, `repeated TopKItem`, `BloomShape`, …). Supported pipeline kinds: Sum / HLL / TopK / Bloom. Supported method kinds: `get_all_time` / `get_window` / `get_window_many` / `get_many` / `get_range`. The last two are Sum-only until the typed clients gain matching methods on HLL/TopK/Bloom.

Both layers:

- Read the live state store directly in kappa mode.
- In lambda mode (`pkg/query.LambdaQuery`): read `batch_view` (immutable, from last batch run) + `realtime_delta` (increments since last batch checkpoint) and merge via the user's monoid Combine.
- Single port speaks gRPC + gRPC-Web + Connect/HTTP-JSON (Connect-RPC).
- Require no user query code for the common path.

### Aggregation primitives (day 1)

Monoid library:
- **Counters**: Sum, Count, Min, Max, First, Last, Set.
- **Sketches**: HyperLogLog (cardinality), Top-K (Misra-Gries / Space-Saving), Bloom.
- **Composition**: Map[K, V] monoid, tuple monoids, semigroup composition.

Sketches require Redis state store for first-class performance; DDB-only works but slower.

### Deployment

Day 1: Terraform module that takes a pipeline definition and provisions:
- Kinesis stream(s) and/or MSK topic(s) (or accepts existing ones)
- Firehose to S3 archive (for kappa replay capability)
- DDB tables (state, checkpoints)
- Optional ElastiCache cluster
- ECS Fargate service for streaming workers (with autoscaling)
- ECS Fargate task definition for batch backfill jobs (run via Step Functions or ad hoc)
- ALB + ECS service for the gRPC query layer
- IAM roles, CloudWatch dashboards, alarms

CDK constructs as a parallel deliverable if there's appetite.

## Repo Structure

```
<repo>/
├── README.md
├── LICENSE                          # Apache 2.0 default — confirm
├── go.mod
├── doc/
│   ├── architecture.md
│   ├── lambda-vs-kappa.md
│   ├── monoid-cookbook.md
│   ├── bootstrap-and-replay.md      # how Bootstrap/Live/Replay compose
│   └── deployment.md
├── pkg/
│   ├── pipeline/                    # Core DSL: Pipeline, Source, Aggregator, Sink, Cache
│   ├── monoid/                      # structural monoids w/ multi-backend impls
│   │   ├── core/                    # Sum, Count, Min, Max, First, Last, Set
│   │   ├── sketch/                  # HLL, TopK, Bloom (Go + Valkey + Spark mappings)
│   │   ├── windowed/                # Windowed[M] wrapper: tumbling buckets + sliding-window queries
│   │   ├── compose/                 # Map, Tuple, decayed-value
│   │   └── custom/                  # user-defined opaque monoids (Go-only backends)
│   ├── source/
│   │   ├── kinesis/                 # polling consumer (dev / demo only — production via pkg/exec/lambda/kinesis)
│   │   ├── kafka/                   # franz-go wrapper (live, ECS)
│   │   └── snapshot/                # bootstrap drivers
│   │       ├── mongo/               # collection scan, incremental chunked
│   │       ├── dynamodb/            # ParallelScan
│   │       ├── jdbc/                # generic JDBC table scan
│   │       └── s3dump/              # one-shot S3 archive ingest
│   ├── state/
│   │   ├── dynamodb/                # source of truth
│   │   └── valkey/                  # cache + sketch accelerator
│   ├── replay/
│   │   ├── kafka/                   # KafkaReplayDriver (offset range)
│   │   └── s3/                      # S3ReplayDriver (Firehose/MSK-Connect partitioned)
│   ├── query/
│   │   ├── codegen/                 # protobuf+grpc service generation from pipeline
│   │   └── runtime/                 # merge logic (kappa direct read; lambda batch+delta merge)
│   └── exec/
│       ├── streaming/               # ECS Fargate streaming worker entrypoint (Kafka)
│       ├── lambda/
│       │   ├── kinesis/             # Kinesis-trigger Lambda handler
│       │   ├── dynamodbstreams/     # DDB-Streams-trigger Lambda handler
│       │   └── sqs/                 # SQS-trigger Lambda handler
│       ├── processor/               # shared retry / dedup / metrics core (streaming + bootstrap + replay + lambda)
│       ├── bootstrap/               # ECS Fargate bootstrap-driver entrypoint
│       ├── replay/                  # ECS Fargate replay-driver entrypoint
│       └── batch/
│           ├── fargate/             # pure-Go batch on ECS Fargate
│           ├── sparkconnect/        # spark-connect-go driver against persistent EMR
│           └── sparkscala/          # Scala codegen + EMR Step submission
├── cmd/
│   ├── <fw>-cli/                    # scaffold, deploy, replay, bootstrap, swap-state commands
│   └── <fw>-worker/                 # generic worker binary, configured via env
├── deploy/
│   ├── terraform/
│   │   └── modules/
│   │       ├── pipeline/            # top-level: composes the rest
│   │       ├── streaming-worker/    # ECS Fargate + autoscaling
│   │       ├── bootstrap-job/       # ECS Fargate one-shot
│   │       ├── batch-fargate/       # ECS Fargate batch
│   │       ├── batch-sparkconnect/  # connects to existing persistent EMR
│   │       ├── batch-sparkstep/     # EMR Step submission
│   │       ├── query-service/       # ALB + ECS gRPC service
│   │       ├── archive/             # Firehose + S3 partitioning
│   │       └── valkey-cache/        # ElastiCache Valkey (optional)
│   └── cdk/                         # parallel CDK constructs (TS)
├── examples/
│   ├── page-view-counters/          # Kinesis live + DDB state + gRPC query
│   ├── unique-visitors-hll/         # Valkey-accelerated HLL with DDB snapshot durability
│   ├── top-k-products/              # TopK sketch over Kafka source
│   └── mongo-cdc-orderstats/        # Mongo bootstrap + CDC live + Spark Connect backfill
├── proto/
│   └── query.proto                  # base proto, generated services per-pipeline
└── test/
    ├── integration/                 # localstack-based source/state/replay tests
    └── e2e/                         # full pipeline-to-query smoke tests
```

## Phased Scope

### MVP (Phase 1) — first usable version

- Core pipeline DSL with generics, structural monoid registry
- **Sources (live)**: Kafka/MSK (franz-go) via ECS workers; Kinesis / DDB Streams / SQS via AWS Lambda event-source mappings
- **Sources (bootstrap)**: Mongo collection scan, DDB ParallelScan
- **State**: DynamoDB (source of truth) + Valkey (cache/sketch accelerator)
- **Monoids**: Counter (Sum, Count, Min, Max, First, Last, Set), HLL, TopK, Bloom
- **Windowed monoids first-class**: `Windowed[M]` with daily/hourly granularity, sliding-window queries (`GetWindow(key, Last7Days)`, `GetRange`)
- **Streaming runtime**: ECS Fargate worker (Kafka) + Lambda handlers (Kinesis / DDB Streams / SQS) sharing the `pkg/exec/processor` core
- **Bootstrap runtime**: ECS Fargate one-shot driver
- **Replay**: S3ReplayDriver (Firehose/MSK-Connect archives) + KafkaReplayDriver (offset range)
- **Batch executors**: ECSFargateExecutor + SparkConnectExecutor (using `pequalsnp/spark-connect-go` fork)
- **Semantics**: at-least-once with per-event-ID dedup (default; only mode in Phase 1)
- gRPC query layer auto-generation, kappa-mode merge, window-aware query interface
- Atomic state-table swap for backfill cutover
- Terraform modules for the full set
- Two worked examples (page-view counters with windowed HLL uniques; Mongo-CDC order stats with Spark Connect backfill)
- Integration tests via docker-compose (DynamoDB Local, Kafka, Valkey, Mongo, MinIO)
- Smoke tests against a real AWS sandbox + Kyle's persistent EMR cluster

### Phase 2

- Lambda deploy mode (separate batch executor + query-time merge logic; window-aware merge)
- Exactly-once semantics via DDB transactions (opt-in alternative to at-least-once + dedup)
- SparkScalaCodegen executor (EMR Step / EMR Serverless submission)
- HTTP/JSON gateway via grpc-gateway
- CDK constructs (parallel to Terraform)
- JDBC + S3-dump bootstrap sources
- Decayed-value monoids; minute-granularity windows
- Observability: CloudWatch dashboards, structured logs, OpenTelemetry traces, sampled tracing through Combine
- More examples (top-K products, hierarchical-window rollups)

### Phase 3

- AWS MemoryDB state store (durable in-memory)
- EKS/Kubernetes deployment backend
- Additional state stores (Postgres, ScyllaDB) if real demand
- Multi-tenant deployment (one engine, many pipelines)
- Watermark support and event-time windowing
- Performance hardening: zero-alloc hot path, batched DDB writes, sketch merge optimizations
- Pluggable bootstrap → live handoff for non-Debezium CDC sources

## Critical Files (when implementation starts)

These are the load-bearing files where architectural decisions live and where most design pressure will be felt:

- `pkg/pipeline/pipeline.go` — the DSL surface, must be ergonomic and stable
- `pkg/monoid/monoid.go` — the structural Monoid interface with backend dispatch table; this is the keystone abstraction
- `pkg/monoid/sketch/hll.go` — HLL with Go + Valkey-native + Spark backend implementations, bit-compatible across them
- `pkg/state/state.go` — the StateStore interface; abstracts DDB-as-truth from Valkey-as-cache
- `pkg/source/snapshot/snapshot.go` — the SnapshotSource interface; common shape across Mongo, DDB, JDBC, S3-dump
- `pkg/replay/replay.go` — the ReplayDriver interface; abstracts live vs Kafka-replay vs S3-replay
- `pkg/exec/batch/sparkconnect/executor.go` — DSL → spark-connect-go DataFrame ops translation
- `pkg/query/codegen/generator.go` — pipeline-to-protobuf generation
- `pkg/query/runtime/merge.go` — lambda-mode merge logic; this is where subtle bugs hide
- `deploy/terraform/modules/pipeline/main.tf` — the top-level deployment composition

## Reused / External Dependencies

- [aws/aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) — AWS SDK
- [aws/aws-lambda-go](https://github.com/aws/aws-lambda-go) — Lambda event types (`events.KinesisEvent`, `events.DynamoDBEvent`, `events.SQSEvent`) and `lambda.Start` entry point
- [twmb/franz-go](https://github.com/twmb/franz-go) — Kafka client
- [valkey-io/valkey-go](https://github.com/valkey-io/valkey-go) — Valkey client (Valkey-native, also Redis-protocol-compatible)
- [pequalsnp/spark-connect-go](https://github.com/pequalsnp/spark-connect-go) — Kyle's fork; production-validated at GSS. Upstream is `apache/spark-connect-go` (community-supported in Spark 4.0)
- [mongodb/mongo-go-driver](https://github.com/mongodb/mongo-go-driver) — Mongo driver for snapshot source
- [grpc-ecosystem/grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway) — HTTP/JSON gateway (Phase 2)
- [axiomhq/hyperloglog](https://github.com/axiomhq/hyperloglog) — pure-Go HLL impl; cross-validate bit-compatibility with Valkey's HLL encoding or implement a portable variant
- Standard `google.golang.org/protobuf`, `google.golang.org/grpc`

## Decisions Locked

1. **Repo name**: `murmur`. **GitHub org**: `gallowaysoftware`. URL: `github.com/gallowaysoftware/murmur`.
2. **License**: Apache 2.0.
3. **Semantics**: at-least-once with per-event-ID dedup (Phase 1). Exactly-once via DDB transactions queued for Phase 2 as opt-in alternative.
4. **Spark Connect dependency**: pin to `pequalsnp/spark-connect-go`; treat upstream `apache/spark-connect-go` as unsupported but probably-compatible. Production validation comes from GSS's existing usage.
5. **Test infrastructure**: docker-compose with open-source containers (`amazon/dynamodb-local`, `confluentinc/cp-kafka` or `bitnami/kafka`, `valkey/valkey`, `mongo:7`, `minio/minio` for S3-compat). LocalStack ruled out (paid tier). Kinesis-touching tests run against a real AWS sandbox account (no good local sub).

## Open Questions

Status legend: ✅ closed in the current implementation · 🟡 partially closed · ⏳ deferred.

1. **Sketch portability across runtimes** ⏳. Valkey HLL uses Redis's encoding; Spark uses its own; pure-Go libraries vary. We need bit-compatibility OR explicit conversion at runtime boundaries. Decision sketch: standardize on a portable encoding (HLL++ / Druid layout) for state stored in DDB; backend-native encodings only inside live workers; conversion at the Valkey-cache and Spark-output boundaries. **Status:** the BytesCache approach (`pkg/state/valkey.BytesCache`) sidesteps the portability issue by storing the SAME bytes the BytesStore stores — using Valkey purely as a key-value cache rather than its native HLL primitives. Native Valkey HLL acceleration remains roadmap.

2. **Worker autoscaling signal** ✅. Kinesis: shard count (already in CloudWatch). Kafka: consumer lag (custom). **Closed by `pkg/observability/autoscale`**: a generic Signal → Emitter → Run loop with a reference CloudWatch emitter and `EventsPerSecond` helper for rate-driven autoscaling. Wires into ECS Fargate target tracking against the published CloudWatch namespace.

3. **Bootstrap-to-live handoff watermarking** ✅. **Closed for Mongo (change-stream resume token in `pkg/source/snapshot/mongo`) and DynamoDB (Streams shard timestamp in `pkg/source/snapshot/dynamodb`)**. Generic JSON-Lines bootstrap (`pkg/source/snapshot/jsonl`) accepts a caller-supplied handoff token via `Config.HandoffToken` for cases where the handoff position is captured externally (e.g., before generating the snapshot).

4. **Window bucket-merge cost at query time** 🟡. A "last 30 days" query on a daily-windowed pipeline reads 30 DDB items and merges. **Closed for the read-amplification dimension via `pkg/state/valkey.BytesCache`** (cached merged-bucket reads serve the same data at sub-ms latency) and **`pkg/query.GetWindowMany` / `GetRangeMany`** (one batched fetch over N entities × M buckets vs. N per-entity calls). Pre-rolled rollup buckets (e.g., a separate "last-7-days" pipeline) remain a future optimization for very-fine-granularity windows.

5. **Window boundary semantics under replay** ✅. **Closed**: replay drivers respect `EventTime` from the source record for bucket assignment when present; streaming and Lambda runtimes do the same with a `time.Now()` fallback when the source-side timestamp is unset. The SQS Lambda handler additionally honors `SentTimestamp` from the SQS message attributes so delayed deliveries land in the correct bucket.

## Verification Plan

How we'll know the framework actually works end-to-end at MVP:

1. **Unit tests** for each monoid (associativity, identity, cross-backend bit-compatibility for HLL/TopK/Bloom), state store contract, snapshot source contract, replay driver contract.
2. **Integration tests via docker-compose** (open-source containers, no LocalStack):
   - Stack: `amazon/dynamodb-local`, `confluentinc/cp-kafka`, `valkey/valkey`, `mongo:7`, `minio/minio` (S3-compat).
   - Live pipeline: Kafka topic → page-view counter aggregator → DDB-Local → gRPC Get returns expected counts.
   - Windowed: same pipeline with `WindowDaily(retention: 30 days)`; verify GetWindow(Last7Days) and GetRange(start, end) return correct sums for synthetic event distributions; verify daily TTL eviction works.
   - Bootstrap: pre-populate Mongo container, run bootstrap driver, verify state matches direct count.
   - Replay: MinIO-archived events replayed into fresh state table → atomic swap → gRPC returns same counts as live path.
   - Valkey accelerator: HLL pipeline writes to both Valkey + DDB-Local; kill Valkey container, verify Get falls back to DDB and is correct; restart Valkey, verify repopulation from DDB.
3. **End-to-end smoke test in a real AWS sandbox + GSS persistent EMR cluster**:
   - Deploy the `mongo-cdc-orderstats` example via Terraform: bootstrap from a sandbox Mongo, switch to live Kinesis CDC, ad-hoc backfill via `SparkConnectExecutor` against the persistent EMR cluster.
   - Generate synthetic load: 1k events/sec for 5 minutes through Kinesis; concurrent Mongo writes propagating through CDC.
   - Verify `gRPC GetWindow(order_id, Last7Days)` returns expected aggregates within the staleness budget; verify Last30Days too.
   - Run a Spark Connect backfill over 30 days of S3-archived CDC events; verify state matches a known-good ground truth from a parallel Mongo aggregation pipeline; verify daily-bucketed counts match expected per-day distribution.
   - Verify HLL unique-customers-per-region per window is within HLL error bounds (~1-2%) of ground truth.
4. **Bootstrap-to-live handoff test**: take a snapshot, generate writes during the bootstrap window, verify no gap and no duplicate after the cutover (resume-token correctness for Mongo; shard-iterator-position correctness for DDB).
5. **Failure injection**: kill a streaming worker mid-aggregation, verify at-least-once-with-idempotency or exactly-once semantics hold per the configured mode. Kill the Spark Connect executor mid-backfill, verify resumability.
6. **Documentation walkthrough**: a fresh developer can clone the repo, follow the example README, and have a working counter pipeline deployed in their AWS account in under 30 minutes. This is the real ergonomic bar.
