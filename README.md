# Murmur

Lambda-architecture-aware streaming aggregation framework for Go.

Murmur is a spiritual successor to [Twitter's Summingbird](https://github.com/twitter/summingbird), built for Go-shop AWS deployments in 2026. One pipeline definition, three execution modes (live stream, snapshot bootstrap, archive replay), monoid-typed state in DynamoDB with optional Valkey acceleration, and a generic gRPC query layer that merges across windows.

## Status

**Pre-1.0, experimental.** The architecture is built and exercised end-to-end against a docker-compose stack. Rough edges are tracked openly in [`STABILITY.md`](STABILITY.md). The generic `Get`/`GetWindow`/etc. adapter and the per-pipeline typed codegen ([`cmd/murmur-codegen-typed`](cmd/murmur-codegen-typed)) both ship; the typed codegen covers Sum / HLL / TopK / Bloom across `get_all_time` / `get_window` / `get_window_many` / `get_many` / `get_range` (the last two are Sum-only until `pkg/query/typed` grows the matching methods on HLL/TopK/Bloom).

| Feature | Status |
|---|---|
| Pipeline DSL with structural monoids | ✅ |
| Live mode: Kafka source ([franz-go](https://github.com/twmb/franz-go)) | ✅ |
| Live mode: Kinesis (production path: AWS Lambda trigger) | ✅ [`pkg/exec/lambda/kinesis`](pkg/exec/lambda/kinesis) — Lambda owns shard discovery, leasing, scaling, checkpointing; [`pkg/source/kinesis`](pkg/source/kinesis) remains as a dev/demo polling consumer |
| Bootstrap mode: Mongo SnapshotSource + handoff token | ✅ |
| Replay mode: S3 / MinIO JSON-Lines | ✅ |
| State: DynamoDB Int64SumStore (atomic ADD) + BytesStore (CAS) | ✅ |
| Cache: Valkey Int64Cache (write-through, INCRBY) | ✅ |
| Monoids: Sum, Count, First, Last, Set | ✅ |
| Monoids: Min, Max (`Bounded[V]`) | ✅ |
| Sketches: HyperLogLog, TopK (Misra-Gries), Bloom | ✅ |
| Windowed aggregations + sliding-window queries | ✅ |
| Generic gRPC query service (`Get` / `GetMany` / `GetWindow` / `GetWindowMany` / `GetRange` / `GetRangeMany`) | ✅ [`pkg/query/grpc`](pkg/query/grpc/) Connect-RPC server with singleflight coalescing |
| Atomic state-table swap (alias version pointer) | ✅ |
| Spark Connect batch executor (user-supplied SQL) | ✅ validated locally against `apache/spark:4.0.1` |
| Lambda mode (batch view ⊕ realtime delta merge) | ✅ via [`pkg/query.LambdaQuery`](pkg/query/lambda.go) |
| Decayed-value monoid (exponential decay) | ✅ via [`pkg/monoid/compose.DecayedSum`](pkg/monoid/compose/decayed.go) |
| Minute / hour / daily windowed buckets | ✅ |
| Web UI (dark mode, pipeline DAG, live metrics, query console) | ✅ [`cmd/murmur-ui`](cmd/murmur-ui) |
| Admin control plane — Connect-RPC, single port speaks gRPC + gRPC-Web + Connect/HTTP-JSON; proto-defined contract for any-language clients | ✅ [`pkg/admin`](pkg/admin), [`proto/murmur/admin/v1/admin.proto`](proto/murmur/admin/v1/admin.proto) |
| Metrics recorder hook in streaming runtime | ✅ [`pkg/metrics`](pkg/metrics) + `streaming.WithMetrics` |
| DX facade (`Counter` / `UniqueCount` / `TopN` presets) | ✅ [`pkg/murmur`](pkg/murmur) |
| Terraform `pipeline-counter` module | ✅ |
| Worked example: `page-view-counters` (worker + query binaries) | ✅ |
| Per-pipeline gRPC codegen (typed responses) | ✅ [`cmd/murmur-codegen-typed`](cmd/murmur-codegen-typed) — Sum / HLL / TopK / Bloom; method kinds `get_all_time` / `get_window` / `get_window_many` / `get_many` / `get_range` (every method kind on every pipeline kind) |
| Valkey-native Bloom acceleration | ✅ [`pkg/state/valkey.BloomCache`](pkg/state/valkey/bloomcache.go) (BF.ADD / BF.MADD / BF.EXISTS / BF.MEXISTS / BF.RESERVE / BF.INFO; requires the valkey-bloom or RedisBloom module) |
| Kafka source: per-partition decode concurrency | ✅ [`pkg/source/kafka.Config.Concurrency`](pkg/source/kafka/source.go) — N decoder goroutines plus one fetcher, partition-pinned for in-order semantics |
| Kafka source: DLQ producer | ✅ [`pkg/source/kafka.NewDLQProducer`](pkg/source/kafka/dlq.go) — wires poison records to a dead-letter topic with diagnostic headers |
| Admin auth (bearer token + JWT) | ✅ [`pkg/admin.WithAuthToken`](pkg/admin/auth.go) + [`WithJWTVerifier`](pkg/admin/auth.go); off by default for same-origin deploys |
| Parquet S3 replay driver | ✅ [`pkg/replay/s3.ParquetDriver`](pkg/replay/s3/parquet.go) (Arrow-based; co-exists with the JSON-Lines `Driver` on the same bucket) |
| Atomic state-table swap wired into Terraform | ✅ [`deploy/terraform/modules/pipeline-counter`](deploy/terraform/modules/pipeline-counter/swap.tf) — opt-in `swap_enabled` provisions the [`pkg/swap`](pkg/swap) control table, IAM, and `SWAP_CONTROL_TABLE` / `SWAP_ALIAS` env vars |

## Limitations to read before adopting

- **`replace` directive only for Spark Connect.** The root `github.com/gallowaysoftware/murmur` module no longer depends on `apache/spark-connect-go` — `pkg/exec/batch/sparkconnect` carries its own `go.mod`. Consumers who don't use Spark Connect (95% of users) get a clean `go.mod`. Consumers who DO use the sparkconnect submodule must mirror its `replace github.com/apache/spark-connect-go => github.com/pequalsnp/spark-connect-go …` line in their own `go.mod` — Go does not propagate replace directives transitively.
- **At-least-once with optional dedup.** Pass `streaming.WithDedup(d)` (where `d` is a `pkg/state/dynamodb.Deduper`) to make replay-after-crash idempotent for any monoid. Without it, the streaming runtime is at-least-once with no per-EventID dedup — fine for idempotent monoids (Set, Min, Max, Bloom) but double-counts non-idempotent ones (Sum, HLL, TopK).
- **Single-goroutine streaming runtime.** Phase-1 streaming processes records sequentially per worker. Throughput ceiling is roughly 5–10 k events/s/worker against DDB-local depending on item size. Use [`streaming.WithConcurrency(N)`](pkg/exec/streaming) for in-process key-hash fanout, or scale horizontally with Kafka partitions. For decode-bound workloads (heavy Protobuf, encrypted payloads), [`kafka.Config.Concurrency`](pkg/source/kafka/source.go) parallelizes the source-side decode with per-partition order preserved.
- **Kinesis production path is Lambda, not ECS.** Use [`pkg/exec/lambda/kinesis`](pkg/exec/lambda/kinesis) — AWS Lambda's event-source mapping owns shard discovery, lease coordination, autoscaling on shard count (via `ParallelizationFactor`), and partial-batch retry semantics via `BatchItemFailures`. The same pipeline definition runs as either a Kafka ECS worker or a Kinesis Lambda; both share state through DDB. The ECS polling path in [`pkg/source/kinesis`](pkg/source/kinesis) is single-instance, has no checkpointing, and is kept for dev / demo only.
- ~~Min / Max monoids violate the identity law.~~ Fixed: lift inputs via `core.NewBounded(v)`; the monoid value type is `core.Bounded[V]` and Identity is the unset wrapper.
- **CORS is closed by default.** Pass `admin.WithAllowedOrigins("https://dashboard.example", …)` (or `cmd/murmur-ui --allow-origin=…`) to open it up. The admin API is read-only but still leaks pipeline metadata, so don't expose it to the public internet without auth in front.
- **CI runs on every PR.** `gofmt` / `go vet` / unit tests with `-race` / `golangci-lint` / web `tsc` + `eslint` + `vite build`. Dependabot is wired up for Go, npm, and Actions.

## Quick taste

```go
import (
    "context"
    "time"

    "github.com/gallowaysoftware/murmur/pkg/murmur"
    mkafka "github.com/gallowaysoftware/murmur/pkg/source/kafka"
    mddb "github.com/gallowaysoftware/murmur/pkg/state/dynamodb"
)

type PageView struct {
    PageID string `json:"page_id"`
    UserID string `json:"user_id"`
}

func main() {
    src, err := mkafka.NewSource(mkafka.Config[PageView]{
        Brokers:       []string{"localhost:9092"},
        Topic:         "page_views",
        ConsumerGroup: "page_views_worker",
        Decode:        mkafka.JSONDecoder[PageView](),
    })
    if err != nil {
        panic(err)
    }
    defer src.Close()

    store := mddb.NewInt64SumStore(ddbClient, "page_views")
    defer store.Close()

    pipe := murmur.Counter[PageView]("page_views").
        From(src).
        KeyBy(func(e PageView) string { return e.PageID }).
        Daily(90 * 24 * time.Hour).
        StoreIn(store).
        Build()

    ctx := context.Background()
    if rc := murmur.RunStreamingWorker(ctx, pipe); rc != 0 {
        panic("worker exited with non-zero code")
    }
}
```

For the runnable version, see [`examples/page-view-counters/`](examples/page-view-counters/).

The generic gRPC service exposes `Get` / `GetMany` / `GetWindow` / `GetWindowMany` / `GetRange` / `GetRangeMany` — wire it up with [`pkg/query/grpc.NewServer`](pkg/query/grpc/server.go); proto definitions in [`proto/murmur/v1/query.proto`](proto/murmur/v1/query.proto). Values are byte-encoded so one service covers every monoid Kind. For type-safe responses, run [`cmd/murmur-codegen-typed`](cmd/murmur-codegen-typed) against a YAML pipeline-spec: it emits a per-service `.proto` + Connect-RPC Go server stub that delegates to [`pkg/query/typed`](pkg/query/typed) and returns the per-kind shape directly (`int64`, `TopKItem[]`, `BloomShape`, …). Examples in [`examples/typed-rpc-codegen/`](examples/typed-rpc-codegen/).

## Architecture

The full design is documented in [`doc/architecture.md`](doc/architecture.md). For shape-oriented "which monoid / runtime / store fits my problem" guidance, see [`doc/use-cases.md`](doc/use-cases.md) — eight common pipeline shapes plus an end-to-end walkthrough of the CDC + backfill pattern. For the canonical "how do I integrate Murmur with text search" question, see [`doc/search-integration.md`](doc/search-integration.md) — three patterns (query-time rescore, bucketed indexing, snapshot+delta), their tradeoffs, and a reference DDB-Streams Lambda projector.

The headline ideas:

1. **Structural monoids.** Each well-known monoid (`Sum`, `HLL`, `TopK`, `Bloom`, …) carries a `Kind` that backend executors dispatch on — DDB picks atomic ADD vs CAS, Spark picks the right SQL aggregation, Valkey picks PFADD vs INCRBY. Custom monoids work as opaque Go closures on Go-only execution backends.
2. **Three execution modes, one DSL.** A pipeline definition is execution-mode-agnostic. The same monoid Combine runs from a Kafka consumer (live), a Mongo collection scan (bootstrap), or an S3 JSON-Lines archive (replay).
3. **DDB is source of truth, Valkey is a cache.** State that's lost in Valkey is repopulatable from DDB. The cache is never trusted as ground truth.
4. **Windowed monoids first-class.** `windowed.Daily(retention)` adds a time-bucket dimension to state keys; queries assemble sliding windows by merging the N most-recent buckets via the monoid Combine.
5. **No Beam, no Flink-in-Go.** Beam's Go SDK is unmaintained and its Spark runner is batch-only. Murmur is *not* a streaming engine — it's a framework that runs your monoid Combine on ECS Fargate workers reading from Kafka, on AWS Lambda (Kinesis / DDB Streams / SQS triggers), and dispatches batch through Spark Connect.

## Why not Beam, why not Flink, why not Goka?

[`doc/architecture.md`](doc/architecture.md#why-not-apache-beam) has the full version. Short version:

- **Apache Beam Go SDK** is unmaintained as of 2.32 and the Spark runner is batch-only — Beam streaming on EMR is not actually possible.
- **Apache Flink** (incl. Amazon Managed Service for Apache Flink) is mature but JVM-only. JVM tax for Go shops, and no auto-generated query layer.
- **Goka** is Kafka-only, no batch story, no query layer, small community.

Murmur fills the gap with: unified Go DSL, structural monoids that dispatch to multiple backends, three execution modes, time-windowed aggregations, and a generic gRPC service that does the merge.

## Run locally

```sh
make compose-up   # bring up kafka, dynamodb-local, valkey, mongo, minio, spark-connect
                  # plus rs.initiate for Mongo (idempotent)
make seed-ddb     # create the page_views DDB table the example reads from
make test-unit    # fast unit tests, no infra
make test-integration  # full E2E suite against the docker-compose stack
make ui           # build the web UI and run cmd/murmur-ui --demo on :8080
```

`make help` lists every target.

The end-to-end tests in [`test/e2e/`](test/e2e/) exercise:

- Counter pipeline: Kafka → Sum → DDB (`counter_test.go`)
- HLL pipeline: Kafka → HLL → DDB BytesStore CAS (`hll_test.go`)
- Windowed counters with `Last1/2/3/7/10/30Days` queries (`windowed_test.go`)
- Mongo bootstrap with Change Stream resume token (`mongo_bootstrap_test.go`)
- DDB ParallelScan bootstrap with re-run idempotency under DDB-backed Deduper (`ddb_bootstrap_test.go`)
- S3 replay into a shadow table (`s3_replay_test.go`)
- Spark Connect batch SUM aggregation → DDB (`spark_connect_test.go`)

## Production-readiness packages

Beyond the core pipeline DSL, several packages exist to make Murmur deployable:

- [`pkg/exec/lambda/{kinesis,dynamodbstreams,sqs}`](pkg/exec/lambda/) — three Lambda runtimes for the AWS-native event sources, all sharing the same retry / dedup / BatchItemFailures contract via [`pkg/exec/processor`](pkg/exec/processor/).
- [`pkg/source/snapshot/{mongo,dynamodb,jsonl,s3}`](pkg/source/snapshot/) — bootstrap sources for the four common shapes: Mongo collections, DynamoDB ParallelScan, raw JSON Lines, and S3-prefix-scan-of-JSON-Lines for partitioned archives.
- [`pkg/state/{dynamodb,valkey}`](pkg/state/) — DDB as source-of-truth (Int64SumStore / BytesStore + Deduper), Valkey as cache (Int64Cache / BytesCache + warmup helpers in `pkg/query`).
- [`pkg/query/grpc`](pkg/query/grpc/) — Connect-RPC server speaking gRPC + gRPC-Web + Connect/HTTP-JSON on one port. Singleflight coalescing + `fresh_read` flag + per-RPC metrics + batched windowed reads (`GetWindowMany` / `GetRangeMany`).
- [`pkg/projection`](pkg/projection/) — bucket functions (`LogBucket` / `LinearBucket` / `ManualBucket`) + `HysteresisBucket` for change-data-capture into search indices.
- [`pkg/observability/autoscale`](pkg/observability/autoscale/) — `Signal → Emitter` loop for publishing scaling-signal metrics. Reference CloudWatch emitter for ECS Fargate target tracking on Kafka consumer lag / Kinesis iterator-age / events-per-second.
- [`pkg/state.NewInstrumented`](pkg/state/instrumented.go) — decorator for any `Store[V]` / `Cache[V]` that adds metrics.Recorder hooks (per-op latency + errors). Zero-overhead when the recorder is nil.
- [`pkg/murmur`](pkg/murmur/) — facade with the common-case presets (`Counter`, `UniqueCount`, `TopN`, `Trending`) plus `RunStreamingWorker` and the Lambda-side `KinesisHandler` / `DynamoDBStreamsHandler` / `SQSHandler` / `MustHandler` wrappers.

## Worked examples

- [`examples/page-view-counters/`](examples/page-view-counters/) — runnable two-binary pipeline (`cmd/worker` + `cmd/query`), a Dockerfile producing a multi-binary distroless image, and the Terraform deployment via [`deploy/terraform/modules/pipeline-counter/`](deploy/terraform/modules/pipeline-counter/).
- [`examples/mongo-cdc-orderstats/`](examples/mongo-cdc-orderstats/) — Mongo collection bootstrap → Kafka CDC live, with upstream-id dedup so re-deliveries fold idempotently.
- [`examples/recently-interacted-topk/`](examples/recently-interacted-topk/) — single Top-N pipeline fed by **two sources at once**: Kinesis (consumed via an AWS Lambda trigger) plus Kafka (consumed by a long-running ECS worker). Both binaries write through the same DDB row; the Misra-Gries `Combine` produces a unified ranking across channels.
- [`examples/search-projector/`](examples/search-projector/) — runnable Pattern B from [`doc/search-integration.md`](doc/search-integration.md): a Lambda that tails Murmur's counter table via DDB Streams and projects bucket transitions into an OpenSearch index, reducing search-side index write rate from per-event to per-order-of-magnitude (~6 reindexes for a 0→1M counter rise vs 1M).
- [`examples/search-rerank/`](examples/search-rerank/) — runnable Pattern A from the same doc: an HTTP search service that does two-stage retrieval (OpenSearch recall + Murmur counter rerank). Pairs with the search-projector to form the canonical "filter on bucket + rank by live counters" shape.
- [`examples/typed-wrapper/`](examples/typed-wrapper/) — count-core-shaped reference for the typed-wrapper pattern: how application services expose Murmur counter pipelines through their own typed Connect-RPC API instead of the generic `Value{bytes}` shape. Uses [`pkg/query/typed`](pkg/query/typed/) as the building block.

## Web UI and admin API

```sh
make ui   # builds the UI, builds the binary, runs --demo on :8080
# open http://localhost:8080
```

`--demo` registers three synthetic pipelines and ticks fake metrics so the dashboard, DAG, and query console have data to show. Real workers register via [`pkg/admin.Server.Register`](pkg/admin/server.go).

The bundled UI is one client of the admin API; **anyone can sub in their own**. The contract lives in [`proto/murmur/admin/v1/admin.proto`](proto/murmur/admin/v1/admin.proto) and the server uses [Connect-RPC](https://connectrpc.com), so a single port speaks gRPC, gRPC-Web, and Connect (HTTP+JSON) — pick whichever your client supports. Generate bindings in your language of choice with `buf generate`. Hit it from `curl` if you want:

```sh
curl -X POST http://localhost:8080/api/murmur.admin.v1.AdminService/ListPipelines \
    -H 'Content-Type: application/json' -d '{}'
# → {"pipelines":[{"name":"page_views","monoidKind":"sum",...}, ...]}
```

## Contributing

PRs welcome. See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the
local-setup, development-loop, PR-process, and what-we-push-back-on
guides. The repo follows the [Contributor Covenant 2.1](CODE_OF_CONDUCT.md);
report conduct issues to `conduct@gallowaysoftware.ca` and security
issues to `security@gallowaysoftware.ca` (see [`SECURITY.md`](SECURITY.md)).

For first-time orientation, [`AGENTS.md`](AGENTS.md) is the
canonical project guide (same content as `CLAUDE.md`, vendor-neutral
[agents.md](https://agents.md) convention). For "which monoid /
runtime fits my problem," start at [`doc/use-cases.md`](doc/use-cases.md).

## License

Apache 2.0.
