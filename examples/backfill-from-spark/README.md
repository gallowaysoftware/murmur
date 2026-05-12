# Example: backfill from Spark-aggregated S3 JSON-Lines

A runnable reference for bootstrapping a Murmur counter pipeline from S3 JSON-Lines that an upstream Spark job has pre-aggregated. This is the canonical "snapshot then stream" bootstrap step: Spark folds raw events from the warehouse into hourly summary rows, lands them in S3, and `bootstrap.Run` scans the prefix and feeds every row through the same pipeline a live worker would.

```
Redshift / warehouse → Spark aggregation → S3 (jsonl.gz) → bootstrap.Run → DDB Sum store
                                                                  ▲
                                                                  │
                                                   live Kafka/Kinesis worker
                                                   (takes over from HandoffToken)
```

Because the Murmur Sum monoid is associative, a pre-aggregated row with `count=42` produces the same bucket value as forty-two streaming events with `count=1`. The pipeline definition is identical; only the source differs.

## Canonical schema

Each line is a single JSON object:

| Field | Type | Required | Purpose |
|---|---|---|---|
| `entity_id` | string | yes | The keying dimension the pipeline buckets by (bot ID, master media ID, prompter ID, …). Maps to the pipeline's `Key`/`KeyByMany` output. |
| `count` | int64 | yes | The pre-aggregated count for this `(entity_id, bucket)`. Becomes `pipeline.Value` and feeds the Sum monoid. |
| `occurred_at` | string (RFC3339 UTC) | yes | The bucket-mid timestamp. The streaming runtime uses `EventTime` for bucket assignment; the backfill source emits a `Record.EventTime` from this field so the same windowing logic places the row in the right Daily or Hourly bucket. |
| `year` | int | informational | The partition the Spark job wrote this row under. Kept on the struct so the field survives the round-trip and is available to logs / observability. |
| `month` | int | informational | |
| `day` | int | informational | |
| `hour` | int | informational | |

The `entity_id` + `occurred_at` fields are the load-bearing ones; `year/month/day/hour` are denormalized partition keys, redundant with `occurred_at` but useful for ad hoc SQL over the archive.

## File layout

Hive-style partitioned under a per-counter prefix, gzipped JSON-Lines, one record per line:

```
s3://my-bucket/counters/<counter-name>/year=2026/month=05/day=08/hour=14/part-00000.jsonl.gz
s3://my-bucket/counters/<counter-name>/year=2026/month=05/day=08/hour=14/part-00001.jsonl.gz
s3://my-bucket/counters/<counter-name>/year=2026/month=05/day=08/hour=15/part-00000.jsonl.gz
…
```

`s3.Config.Prefix = "counters/<counter-name>/"` recursively lists every key under that prefix; `KeyFilter` (optional) excludes `_SUCCESS` / `_manifest.json` companion objects.

S3 returns keys in lexicographic order, so the Hive partitioning above scans chronologically. For backfill that doesn't matter — Sum is commutative — but it makes the operator-side log line readable when watching a 40-day backfill walk forward.

## Why this shape

Three things drove this schema:

1. **Spark already wants partitioned JSON-Lines.** It's the lowest-friction sink format for Spark Connect; we don't have to teach Spark about DDB or our wire format.
2. **The streaming source produces the same logical event.** A live Kafka decoder emits `CountEvent{Count: 1}`; the backfill decoder emits `CountEvent{Count: N}` for a pre-aggregated row. One pipeline definition handles both because the Sum monoid is associative.
3. **`(entity_id, hour-bucket)` is a natural dedup key.** Re-running the Spark job on the same window re-emits the same rows; hashing `(entity_id, occurred_at.truncate(1h))` gives a stable `EventID` so `bootstrap.Run`'s `WithDedup` collapses re-runs to no-ops.

## Usage

```go
import (
    "github.com/gallowaysoftware/murmur/examples/backfill-from-spark"
    "github.com/gallowaysoftware/murmur/pkg/exec/bootstrap"
    "github.com/gallowaysoftware/murmur/pkg/source/snapshot/s3"
)

src, err := s3.NewSource(s3.Config[backfill.CountEvent]{
    Client:      s3Client,
    Bucket:      "my-bucket",
    Prefix:      "counters/bot_interaction/",
    Concurrency: 8,                     // parallel fetches
    Decode:      backfill.DecodeJSONL,  // canonical decoder
    EventID:     backfill.StableEventID, // (entity, hour) hash for dedup
})
if err != nil { /* ... */ }

token, err := bootstrap.Run(ctx, pipe, src,
    bootstrap.WithDedup(deduper), // idempotent re-runs
)
```

Run it:

```sh
go run ./examples/backfill-from-spark/cmd/backfill \
    -bucket=my-bucket \
    -prefix=counters/bot_interaction/ \
    -table=bot_interaction_counts \
    -name=bot_interaction \
    -concurrency=8 \
    -retention=720h        # 30 days
```

The pipeline shape is intentionally minimal — `Key` + `Value` + `Sum` over a `Daily` window — to keep the example focused on the JSON-Lines source. Real counters add `KeyByMany` for scope-key fan-out (`lifetime:<id>`, `hourly:<id>:<bucket>`, `trailing_7d:<id>`), a cache layer, and a `state.Deduper`.

## Bounded concurrency

`s3.Config.Concurrency` caps the number of objects fetched and decoded in parallel. Default 1 (sequential, preserves S3 lexicographic order). Bump to 4–16 when:

- The prefix has many small objects (each `GetObject` round-trip is a fixed cost).
- The DDB-side write throughput exceeds what a single goroutine can drive.

With `Concurrency > 1` the record emission order is non-deterministic across keys; within a single key, lines are emitted in file order. The dedup contract handles the rest: `EventID` is derived from the row payload, not the file position, so re-ordering across keys doesn't change the dedup decision.

## Files

| File | Purpose |
|---|---|
| `event.go` | `CountEvent` struct, `DecodeJSONL`, and the `StableEventID` extractor. The decoder is wired into `s3.Config.Decode`; the extractor into `s3.Config.EventID`. |
| `cmd/backfill/main.go` | Minimal runnable binary: flags → pipeline → `bootstrap.Run`. Loads AWS config from the ambient environment. |

## What's NOT in this example

- **A Spark job.** The Spark side of the pipeline lives in the consumer repository (each counter has its own aggregation SQL). This example documents the schema the Spark job must produce.
- **A `state.Deduper`.** Wire `bootstrap.WithDedup(...)` in `main.go` when you need re-run idempotence; left out here to keep the example single-step.
- **Custom partition pruning.** `s3.Config.KeyFilter` is the seam — wire it to skip partitions outside the target backfill window, or to exclude `_SUCCESS` markers.
- **Handoff to live mode.** `bootstrap.Run` returns a `HandoffToken` (nil here because S3 has no live-mode resume position); a production wrapper persists the token alongside its progress and starts the live Kafka/Kinesis worker pointed at the same DDB table.
