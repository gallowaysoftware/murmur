# Example: recently-interacted Top-N (Kinesis + Kafka, one pipeline)

A Murmur pipeline that maintains a daily-windowed Top-N "recently interacted
entities" sketch fed by **two sources at once**:

- **Kinesis**, consumed via an AWS Lambda Kinesis trigger (`cmd/lambda`).
  Operationally cheap path for managed analytics ingest (Segment-style event
  buses, AWS-native event sources). Lambda owns shard polling, fan-out, and
  partial-batch retry.
- **Kafka** (or MSK), consumed by a long-running ECS Fargate streaming worker
  (`cmd/worker`). Right fit for internal application events / CDC streams.

Both binaries share one `pipeline.go` definition and write to the same
DynamoDB row. The Misra-Gries `Combine` collapses duplicates correctly across
sources, so a single query against `cmd/query` returns the merged Top-N over
*all* incoming interactions, regardless of which channel they came in on.

## Why two sources?

Real systems frequently have one event channel for managed third-party data
(Segment → Kinesis is the canonical case) and a separate channel for internal
events (a Kafka topic). Murmur's structural-monoid pipeline DSL is
execution-mode-agnostic: the same `Aggregate(topk.New(K), windowed.Daily(...))`
runs unchanged behind both drivers. Single source of truth, two ingest paths.

## Schema

```go
type Interaction struct {
    EntityID string  // ranked thing — product ID, content ID, etc.
    UserID   string  // actor (carried through; not part of the agg key)
    Source   string  // "kinesis" or "kafka" — diagnostics only
}
```

## Pipeline (shared between all three binaries)

```go
pipeline.NewPipeline[Interaction, []byte]("recently_interacted").
    Key(func(Interaction) string { return "global" }).
    Value(func(e Interaction) []byte {
        return topk.SingleN(K, e.EntityID, 1)
    }).
    Aggregate(topk.New(K), windowed.Daily(30*24*time.Hour)).
    StoreIn(dynamodb.NewBytesStore(...))
```

The Lambda binary leaves `Source` unset (Lambda owns polling). The Kafka
worker calls `AttachKafkaSource(pipe, cfg)` to attach a franz-go source.

## Run locally

Stand up dependencies:

```sh
cd ../..
docker compose up -d kafka dynamodb-local
```

Create the DDB tables (first time only):

```sh
aws --endpoint-url=http://localhost:8000 dynamodb create-table \
    --table-name recently_interacted \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=N \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST

aws --endpoint-url=http://localhost:8000 dynamodb create-table \
    --table-name recently_interacted_dedup \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

Run the Kafka worker:

```sh
export DDB_ENDPOINT=http://localhost:8000
export DDB_DEDUP_TABLE=recently_interacted_dedup
go run ./examples/recently-interacted-topk/cmd/worker
```

The Lambda binary is meant for AWS — locally, you can drive it via `sam local
invoke` with a synthetic Kinesis event payload, or run the test under
`./test/...` which exercises the same `NewKinesisHandler` against a fake
DynamoDB.

Run the query server in a third terminal:

```sh
go run ./examples/recently-interacted-topk/cmd/query
```

Then query the merged Top-N:

```sh
# All time (single bucket if non-windowed)
grpcurl -plaintext -d '{"entity":"global"}' \
    localhost:50051 murmur.v1.QueryService/Get

# Last 7 days (merges 7 daily Misra-Gries summaries)
grpcurl -plaintext -d '{"entity":"global","duration_seconds":604800}' \
    localhost:50051 murmur.v1.QueryService/GetWindow
```

The response's `data` is a serialized Misra-Gries summary (raw bytes); decode
via `pkg/monoid/sketch/topk.Decode`, or use the embedded admin UI which
renders the items + counts directly.

## Production deployment

- The Lambda binary builds for `provided.al2` / arm64 — see the comment block
  in `cmd/lambda/main.go` for the exact `go build` invocation.
- Configure the event-source mapping with
  `FunctionResponseTypes=["ReportBatchItemFailures"]` so failed records
  (after the in-handler retry budget) are isolated rather than redelivering
  the whole batch.
- The Kafka worker runs as a standard Murmur ECS Fargate service.
- Both processes use the same `DDB_TABLE` and `DDB_DEDUP_TABLE`. The dedup
  table is *strongly recommended* in production — Lambda BatchItemFailures
  may redeliver records adjacent to a failure, and Kafka rebalances may
  re-emit unacked records. Without dedup, a redelivery spuriously increments
  the Misra-Gries count.
