# backfill-from-spark

This example shows how to seed a Murmur pipeline from a Spark-produced
backfill written to S3.

Two interchange formats are supported. Pick the right one for the data
size:

| Format | Source | Best for |
|---|---|---|
| **JSON Lines** | `pkg/source/snapshot/s3` (gzip-aware) | Hand-built dumps, DB exports, small backfills |
| **Parquet**    | `pkg/source/snapshot/parquet#S3Source` | Spark-produced backfills; 5–20× smaller, columnar, partition-prunable |

Both sources implement `snapshot.Source[T]` and plug directly into
`bootstrap.Run`. The Parquet path is recommended whenever Spark is the
producer — it's Spark's native output format and avoids a wasteful
`.write.json(...).gz` round-trip.

```
Redshift / warehouse → Spark aggregation → S3 → bootstrap.Run → DDB Sum store
                                                       ▲
                                                       │
                                        live Kafka/Kinesis worker
                                        (takes over from HandoffToken)
```

Because the Murmur Sum monoid is associative, a pre-aggregated row with `count=42` produces the same bucket value as forty-two streaming events with `count=1`. The pipeline definition is identical; only the source differs.

## Canonical Parquet schema

Spark jobs targeting Murmur should write rows in this shape:

```
message CountEvent {
  required binary entity_id  (STRING);
  required int64  count;
  required int64  occurred_at_unix_ms;
  required int32  year;
  required int32  month;
  required int32  day;
  required int32  hour;
}
```

The `year` / `month` / `day` / `hour` columns are **duplicated** in two
places:

1. As columns inside every row (so the data is self-describing without
   needing the S3 key).
2. As Hive-style partition segments in the object key, e.g.
   `events/year=2026/month=05/day=08/hour=14/part-00000.parquet`.

The duplication is what makes `PartitionFilter` predicate-pushdown
viable: the S3Source can drop entire `.parquet` objects without
reading them based on the partition values parsed from the key path.

## Spark writer (PySpark)

```python
(events
  .withColumn("year",  year("occurred_at"))
  .withColumn("month", month("occurred_at"))
  .withColumn("day",   dayofmonth("occurred_at"))
  .withColumn("hour",  hour("occurred_at"))
  .withColumn("occurred_at_unix_ms", (unix_timestamp("occurred_at") * 1000).cast("long"))
  .select("entity_id", "count", "occurred_at_unix_ms", "year", "month", "day", "hour")
  .write
  .partitionBy("year", "month", "day", "hour")
  .mode("overwrite")
  .parquet("s3://bucket/murmur/page_views/v2/"))
```

Spark defaults to Snappy compression — leave it. The Parquet snapshot
source handles Snappy / gzip / zstd / lz4 transparently.

## Go bootstrap (Parquet)

```go
import (
    "context"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/gallowaysoftware/murmur/pkg/source/snapshot/parquet"
)

type CountEvent struct {
    EntityID         string
    Count            int64
    OccurredAtUnixMs int64
}

func decode(rec arrow.Record, row int) (CountEvent, error) {
    sc := rec.Schema()
    var e CountEvent
    e.EntityID         = rec.Column(sc.FieldIndices("entity_id")[0]).(*array.String).Value(row)
    e.Count            = rec.Column(sc.FieldIndices("count")[0]).(*array.Int64).Value(row)
    e.OccurredAtUnixMs = rec.Column(sc.FieldIndices("occurred_at_unix_ms")[0]).(*array.Int64).Value(row)
    return e, nil
}

func newSource(ctx context.Context, client *awss3.Client) (*parquet.S3Source[CountEvent], error) {
    return parquet.NewS3Source(parquet.S3Config[CountEvent]{
        Client:         client,
        Bucket:         "my-bucket",
        Prefix:         "murmur/page_views/v2/",
        Decode:         decode,
        MaxConcurrency: 8,

        // Optional: prune entire partitions. Spark may have written
        // historical hours we don't want to replay.
        PartitionFilter: func(p parquet.Partition) bool {
            return p.Values["year"] == "2026" && p.Values["month"] == "05"
        },

        // Optional: use the natural key for dedup so re-runs are
        // idempotent under at-least-once.
        EventID: func(e CountEvent, _ string, _ int) string {
            return e.EntityID + "|" + strconv.FormatInt(e.OccurredAtUnixMs, 10)
        },
    })
}
```

Wire the returned `*parquet.S3Source` into `bootstrap.Run` exactly as
you would the JSON-Lines variant — the Source contract is identical.

## JSON Lines variant

For ad-hoc backfills or sources Spark isn't the producer for,
`pkg/source/snapshot/s3` is the right choice. The runnable binary in
`cmd/backfill/` exercises this path end-to-end.

The schema mirrors the Parquet shape but JSON-encoded one event per line:

| Field | Type | Required | Purpose |
|---|---|---|---|
| `entity_id` | string | yes | The keying dimension. Maps to the pipeline's `Key`/`KeyByMany` output. |
| `count` | int64 | yes | The pre-aggregated count for this `(entity_id, bucket)`. |
| `occurred_at` | string (RFC3339 UTC) | yes | Bucket-mid timestamp; the source emits `Record.EventTime` from this so windowed counters bucket correctly. |
| `year` / `month` / `day` / `hour` | int | informational | Partition components, redundant with `occurred_at` but useful for ad hoc SQL over the archive. |

File layout — Hive-style partitioned, gzipped JSON-Lines:

```
s3://my-bucket/counters/<counter-name>/year=2026/month=05/day=08/hour=14/part-00000.jsonl.gz
s3://my-bucket/counters/<counter-name>/year=2026/month=05/day=08/hour=15/part-00000.jsonl.gz
…
```

S3 returns keys in lexicographic order, so the Hive partitioning above scans chronologically. For backfill that doesn't matter — Sum is commutative — but it makes the operator-side log line readable when watching a 40-day backfill walk forward.

Gzip is auto-detected by `.gz` suffix. For Snappy / Zstd, supply a
custom `OpenObject` hook (see the s3 source's `OpenObject` field).

### Usage

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
    Concurrency: 8,                      // parallel fetches
    Decode:      backfill.DecodeJSONL,   // canonical decoder
    EventID:     backfill.StableEventID, // (entity, hour) hash for dedup
})
if err != nil { /* ... */ }

token, err := bootstrap.Run(ctx, pipe, src,
    bootstrap.WithDedup(deduper), // idempotent re-runs
)
```

Run the included binary:

```sh
go run ./examples/backfill-from-spark/cmd/backfill \
    -bucket=my-bucket \
    -prefix=counters/bot_interaction/ \
    -table=bot_interaction_counts \
    -name=bot_interaction \
    -concurrency=8 \
    -retention=720h        # 30 days
```

The pipeline shape is intentionally minimal — `Key` + `Value` + `Sum` over a `Daily` window — to keep the example focused on the source. Real counters add `KeyByMany` for scope-key fan-out (`lifetime:<id>`, `hourly:<id>:<bucket>`, `trailing_7d:<id>`), a cache layer, and a `state.Deduper`.

### Bounded concurrency

`s3.Config.Concurrency` caps the number of objects fetched and decoded in parallel. Default 1 (sequential, preserves S3 lexicographic order). Bump to 4–16 when:

- The prefix has many small objects (each `GetObject` round-trip is a fixed cost).
- The DDB-side write throughput exceeds what a single goroutine can drive.

With `Concurrency > 1` the record emission order is non-deterministic across keys; within a single key, lines are emitted in file order. The dedup contract handles the rest: `EventID` is derived from the row payload, not the file position, so re-ordering across keys doesn't change the dedup decision.

### Files

| File | Purpose |
|---|---|
| `event.go` | `CountEvent` struct, `DecodeJSONL`, and the `StableEventID` extractor. Wired into `s3.Config.Decode` / `s3.Config.EventID`. |
| `cmd/backfill/main.go` | Minimal runnable binary: flags → pipeline → `bootstrap.Run`. Loads AWS config from the ambient environment. |

### What's NOT in the JSONL example

- **A Spark job.** Lives in the consumer repository (each counter has its own aggregation SQL). This example documents the schema the Spark job must produce.
- **A `state.Deduper`.** Wire `bootstrap.WithDedup(...)` in `main.go` when you need re-run idempotence; left out here to keep the example single-step.
- **Custom partition pruning.** `s3.Config.KeyFilter` is the seam — wire it to skip partitions outside the target backfill window or to exclude `_SUCCESS` markers.
- **Handoff to live mode.** `bootstrap.Run` returns a `HandoffToken` (nil for S3, which has no live-mode resume position); a production wrapper persists the token alongside its progress and starts the live Kafka/Kinesis worker pointed at the same DDB table.

## Picking between the two

- **Spark is the producer** → Parquet (5–20× smaller, no extra job to
  serialize as JSON).
- **DB export, Firehose archive, hand-written dump** → JSON Lines
  (zero schema setup, every tool can read it).
- **Mixed** → wire both, run bootstrap twice with a shared `Deduper`.
