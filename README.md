# Murmur

Lambda-architecture-aware streaming aggregation framework for Go.

Murmur is a spiritual successor to [Twitter's Summingbird](https://github.com/twitter/summingbird), built for Go-shop AWS deployments in 2026. One pipeline definition, three execution modes (live stream, snapshot bootstrap, archive replay), monoid-typed state in DynamoDB with optional Valkey acceleration, and a generic gRPC query layer that merges across windows.

## Status

**Pre-1.0, experimental.** The architecture is built and exercised end-to-end against a docker-compose stack. Several rough edges are tracked openly in [`STABILITY.md`](STABILITY.md) — most notably error-handling gaps and the gRPC `Get`/`GetWindow`/etc. surface being a generic adapter today rather than per-pipeline codegen.

| Feature | Status |
|---|---|
| Pipeline DSL with structural monoids | ✅ |
| Live mode: Kafka source ([franz-go](https://github.com/twmb/franz-go)) | ✅ |
| Live mode: Kinesis source | ⚠️ single-instance only, no checkpointing — KCL v3 multi-instance is on the roadmap |
| Bootstrap mode: Mongo SnapshotSource + handoff token | ✅ |
| Replay mode: S3 / MinIO JSON-Lines | ✅ |
| State: DynamoDB Int64SumStore (atomic ADD) + BytesStore (CAS) | ✅ |
| Cache: Valkey Int64Cache (write-through, INCRBY) | ✅ |
| Monoids: Sum, Count, First, Last, Set | ✅ |
| Monoids: Min, Max (`Bounded[V]`) | ✅ |
| Sketches: HyperLogLog, TopK (Misra-Gries), Bloom | ✅ |
| Windowed aggregations + sliding-window queries | ✅ |
| Generic gRPC query service (`Get` / `GetMany` / `GetWindow` / `GetRange`) | ✅ — typed-per-pipeline codegen is on the roadmap |
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
| Per-pipeline gRPC codegen (typed responses) | 🛣 roadmap |
| Valkey-native HLL/Bloom acceleration | 🛣 roadmap |
| KCL-v3 Kinesis source | 🛣 roadmap |

## Limitations to read before adopting

- **`replace` directive in `go.mod`.** Murmur depends on a personal fork of `apache/spark-connect-go` for the batch executor. Anyone importing `pkg/exec/batch/sparkconnect` must mirror the `replace` directive in their own `go.mod`. Tracked: upstream the patches or split that package out.
- **At-least-once, with caveats.** The streaming runtime acks Kafka records after writing to DynamoDB, but there is no per-EventID dedup yet — a worker that dies between the DDB write and the ack will replay and double-count. Make pipelines idempotent at the monoid layer or pin to exactly-once-tolerant aggregations.
- **Single-goroutine streaming runtime.** Phase-1 streaming processes records sequentially per worker. Throughput ceiling is roughly 5–10 k events/s/worker against DDB-local depending on item size. Scale horizontally with Kafka partitions until per-partition parallelism lands.
- ~~Min / Max monoids violate the identity law.~~ Fixed: lift inputs via `core.NewBounded(v)`; the monoid value type is `core.Bounded[V]` and Identity is the unset wrapper.
- **CORS is permissive on the admin server.** `pkg/admin` ships with `Access-Control-Allow-Origin: *`. Do not expose the admin API to the public internet today; keep it on a private subnet behind your VPC.
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

The generic gRPC service exposes `Get(entity)`, `GetWindow(entity, duration)`, `GetMany(entities)`, and `GetRange(entity, start, end)` — wire it up with [`pkg/query/grpc.NewServer`](pkg/query/grpc/server.go); proto definitions in [`proto/murmur/v1/query.proto`](proto/murmur/v1/query.proto). Per-pipeline typed responses (today everything is `bytes`) are tracked on the roadmap.

## Architecture

The full design is documented in [`doc/architecture.md`](doc/architecture.md). The headline ideas:

1. **Structural monoids.** Each well-known monoid (`Sum`, `HLL`, `TopK`, `Bloom`, …) carries a `Kind` that backend executors dispatch on — DDB picks atomic ADD vs CAS, Spark picks the right SQL aggregation, Valkey picks PFADD vs INCRBY. Custom monoids work as opaque Go closures on Go-only execution backends.
2. **Three execution modes, one DSL.** A pipeline definition is execution-mode-agnostic. The same monoid Combine runs from a Kafka consumer (live), a Mongo collection scan (bootstrap), or an S3 JSON-Lines archive (replay).
3. **DDB is source of truth, Valkey is a cache.** State that's lost in Valkey is repopulatable from DDB. The cache is never trusted as ground truth.
4. **Windowed monoids first-class.** `windowed.Daily(retention)` adds a time-bucket dimension to state keys; queries assemble sliding windows by merging the N most-recent buckets via the monoid Combine.
5. **No Beam, no Flink-in-Go.** Beam's Go SDK is unmaintained and its Spark runner is batch-only. Murmur is *not* a streaming engine — it's a framework that runs your monoid Combine on ECS Fargate workers reading from Kinesis/Kafka, and dispatches batch through Spark Connect.

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
- S3 replay into a shadow table (`s3_replay_test.go`)
- Spark Connect batch SUM aggregation → DDB (`spark_connect_test.go`)

## Worked example

See [`examples/page-view-counters/`](examples/page-view-counters/) for a runnable two-binary pipeline (`cmd/worker` + `cmd/query`), a Dockerfile producing a multi-binary distroless image, and the Terraform deployment via [`deploy/terraform/modules/pipeline-counter/`](deploy/terraform/modules/pipeline-counter/).

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

## License

Apache 2.0.
