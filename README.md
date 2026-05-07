# Murmur

Lambda-architecture-aware streaming aggregation framework for Go.

Murmur is a spiritual successor to [Twitter's Summingbird](https://github.com/twitter/summingbird), built for Go-shop AWS deployments in 2026. It lets you define monoid-based aggregations (counters, cardinality, top-K, time-windowed rollups) once in Go, then run them across multiple execution engines — live streams from Kinesis or Kafka, snapshot-bootstraps from Mongo or DynamoDB, batch backfills on ECS Fargate or Spark Connect against EMR — and serves the results through an auto-generated gRPC query layer that knows how to merge across modes.

## Status

Early development. Phase 1 MVP underway. Not yet usable in production.

See [the design plan](doc/architecture.md) for the full architecture.

## Quick taste

```go
pipeline := murmur.NewPipeline("page_views").
    From(sources.Kinesis("events-stream")).
    Key(func(e Event) string { return e.PageID }).
    Aggregate(
        monoids.Counter,
        murmur.WindowDaily(retention: 90*24*time.Hour),
    ).
    StoreIn(state.DynamoDB("page_views_table")).
    Cache(state.Valkey("hot-counters")).
    ServeOn(query.GRPC(":50051"))
```

The generated query service exposes `Get(key, AllTime)`, `GetWindow(key, Last7Days)`, `GetWindow(key, Last30Days)`, and `GetRange(key, start, end)` over the same monoid Combine — no query-side code required for the common path.

## Why

Existing options have gaps for Go-on-AWS counter workloads:

- **Apache Flink** (incl. Amazon Managed Service for Apache Flink) is mature, but JVM-only. No Go SDK.
- **Apache Beam Go SDK** is unmaintained as of Spark 4.0, and the Spark runner is batch-only.
- **Goka** is Kafka-only, no batch story, no query layer.
- Rolling your own gives you Kinesis consumers + DDB + ad-hoc gRPC — fine, but everyone reinvents the same merging logic for batch + realtime views.

Murmur fills the gap with: unified Go DSL, structural monoids that compile to multiple backends, first-class time-windowed aggregations, and an auto-generated gRPC service that does the lambda-architecture merge correctly.

## License

Apache 2.0.
