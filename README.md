# Murmur

Lambda-architecture-aware streaming aggregation framework for Go.

Murmur is a spiritual successor to [Twitter's Summingbird](https://github.com/twitter/summingbird), built for Go-shop AWS deployments in 2026. One pipeline definition, three execution modes (live stream, snapshot bootstrap, archive replay), monoid-typed state in DynamoDB with optional Valkey acceleration, and an auto-generated gRPC query layer that knows how to merge across windows.

## Status

Phase 1 in progress. The core architecture is built and exercised end-to-end against a docker-compose stack. Not yet hardened for production, but the shape is real.

| Feature | Status |
|---|---|
| Pipeline DSL with structural monoids | ✅ |
| Live mode: Kafka source ([franz-go](https://github.com/twmb/franz-go)) | ✅ |
| Live mode: Kinesis source (single-instance) | ✅ |
| Bootstrap mode: Mongo SnapshotSource + handoff token | ✅ |
| Replay mode: S3 / MinIO JSON-Lines | ✅ |
| State: DynamoDB Int64SumStore (atomic ADD) + BytesStore (CAS) | ✅ |
| Cache: Valkey Int64Cache (write-through, INCRBY) | ✅ |
| Monoids: Sum, Count, Min, Max, First, Last, Set | ✅ |
| Sketches: HyperLogLog, TopK (Misra-Gries), Bloom | ✅ |
| Windowed aggregations + sliding-window queries | ✅ |
| gRPC query service (`Get` / `GetMany` / `GetWindow` / `GetRange`) | ✅ |
| Atomic state-table swap (alias version pointer) | ✅ |
| Spark Connect batch executor (user-supplied SQL) | 🚧 skeleton, needs EMR validation |
| Terraform `pipeline-counter` module | ✅ |
| Worked example: `page-view-counters` (worker + query binaries) | ✅ |
| Kinesis source via KCL v3 (multi-instance, lease management) | Phase 2 |
| Lambda mode (separate batch + realtime merge at query time) | Phase 2 |
| gRPC codegen per pipeline (typed responses) | Phase 2 |
| Valkey-native HLL/Bloom acceleration | Phase 2 |

## Quick taste

```go
pipeline := pipeline.NewPipeline[PageView, int64]("page_views").
    From(kafka.NewSource(...)).
    Key(func(e PageView) string { return e.PageID }).
    Value(func(PageView) int64 { return 1 }).
    Aggregate(core.Sum[int64](), windowed.Daily(90 * 24 * time.Hour)).
    StoreIn(dynamodb.NewInt64SumStore(ddbClient, "page_views")).
    Cache(valkey.NewInt64Cache(...))

streaming.Run(ctx, pipeline)
```

The auto-generated gRPC service exposes `Get(entity)`, `GetWindow(entity, duration)`, `GetMany(entities)`, and `GetRange(entity, start, end)` — see [`proto/murmur/v1/query.proto`](proto/murmur/v1/query.proto).

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

Murmur fills the gap with: unified Go DSL, structural monoids that dispatch to multiple backends, three execution modes, time-windowed aggregations, and an auto-generated gRPC service that does the merge.

## Run locally

```sh
docker compose up -d kafka dynamodb-local valkey mongo minio

# initiate the mongo replica set (needed for snapshot resume tokens)
docker exec murmur-mongo mongosh --quiet --eval \
  "rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]})"

# run all unit + integration tests
DDB_LOCAL_ENDPOINT=http://localhost:8000 \
KAFKA_BROKERS=localhost:9092 \
VALKEY_ADDRESS=localhost:6379 \
S3_ENDPOINT=http://localhost:9000 \
MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" \
go test ./...
```

The end-to-end tests in [`test/e2e/`](test/e2e/) exercise:

- Counter pipeline: Kafka → Sum → DDB (`counter_test.go`)
- HLL pipeline: Kafka → HLL → DDB BytesStore CAS (`hll_test.go`)
- Windowed counters with `Last1/2/3/7/10/30Days` queries (`windowed_test.go`)
- Mongo bootstrap with Change Stream resume token (`mongo_bootstrap_test.go`)
- S3 replay into a shadow table (`s3_replay_test.go`)

## Worked example

See [`examples/page-view-counters/`](examples/page-view-counters/) for a runnable two-binary pipeline (`cmd/worker` + `cmd/query`), a Dockerfile producing a multi-binary distroless image, and the Terraform deployment via [`deploy/terraform/modules/pipeline-counter/`](deploy/terraform/modules/pipeline-counter/).

## License

Apache 2.0.
