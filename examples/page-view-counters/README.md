# Example: page-view counters

A complete Murmur pipeline that ingests page-view events from Kafka, maintains windowed
counters per page in DynamoDB (with optional Valkey acceleration), and serves results
over a gRPC query service.

This example demonstrates the three principal pieces of Murmur stitched together:

1. **Streaming runtime** — `cmd/worker` reads from a Kafka topic and writes to DDB
2. **Cache acceleration** — Valkey is configured as a write-through INCRBY-based cache
3. **gRPC query layer** — `cmd/query` serves Get/GetWindow/GetRange against the same store

## Schema

```go
type pageView struct {
    PageID string `json:"page_id"`
    UserID string `json:"user_id"`
}
```

## Pipeline

```go
murmur.NewPipeline[pageView, int64]("page_views").
    From(kafka.NewSource(...)).
    Key(func(e pageView) string { return e.PageID }).
    Value(func(pageView) int64 { return 1 }).
    Aggregate(core.Sum[int64](), windowed.Daily(90 * 24 * time.Hour)).
    StoreIn(dynamodb.NewInt64SumStore(...)).
    Cache(valkey.NewInt64Cache(...)).
    ServeOn(grpc.Config{...})
```

## Run locally

Bring up the docker-compose dependencies:

```sh
cd ../..
docker compose up -d kafka dynamodb-local valkey
```

Run the streaming worker:

```sh
go run ./examples/page-view-counters/cmd/worker
```

In another terminal, run the gRPC query server:

```sh
go run ./examples/page-view-counters/cmd/query
```

Produce some events (using your tool of choice — `kcat`, a small Go producer, etc.) and
then query:

```sh
grpcurl -plaintext -d '{"entity": "page-A"}' localhost:50051 murmur.v1.QueryService/Get
grpcurl -plaintext -d '{"entity": "page-A", "duration_seconds": 86400}' \
  localhost:50051 murmur.v1.QueryService/GetWindow
```

## Production deployment

In production each component runs as its own ECS Fargate service. See `terraform/` for
the deployment module (in progress).
