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

## Go bootstrap

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

For ad-hoc backfills or sources that Spark is not the producer for,
`pkg/source/snapshot/s3` is the right choice. The schema is the same
shape, JSON-encoded one event per line:

```json
{"entity_id":"u-1","count":5,"occurred_at_unix_ms":1717420800000}
{"entity_id":"u-2","count":3,"occurred_at_unix_ms":1717420860000}
```

Gzip is auto-detected by `.gz` suffix. For Snappy / Zstd, supply a
custom `OpenObject` hook (see the s3 source's `OpenObject` field).

## Picking between the two

- **Spark is the producer** → Parquet (5–20× smaller, no extra job to
  serialize as JSON).
- **DB export, Firehose archive, hand-written dump** → JSON Lines
  (zero schema setup, every tool can read it).
- **Mixed** → wire both, run bootstrap twice with a shared `Deduper`.
