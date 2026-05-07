# Example: Mongo CDC order-stats (Bootstrap → Live)

A full lambda-architecture-flavored pipeline that demonstrates the
Bootstrap → Live transition Kyle's adversarial review specifically called
out: a Mongo collection holds the source-of-truth, a CDC stream (Mongo Change
Streams piped through Kafka via Debezium in production) carries every change
that happens after, and Murmur folds them into per-customer order totals.

```
┌──────────────┐
│  Mongo       │ orders collection (source of truth)
│  ─────────── │
│  rs0         │
└──────┬───────┘
       │ initial snapshot (bootstrap binary)
       ▼
┌──────────────┐    ┌─────────────────┐   ┌─────────────────┐
│ pkg/exec/    │───▶│ Sum monoid per  │──▶│ DDB             │
│ bootstrap    │    │ customer_id     │   │ order_totals    │
└──────────────┘    └─────────────────┘   └─────────────────┘
       │
       │ captures Change Stream resume token
       │ (handed to live consumer's StartAfter)
       ▼
┌──────────────┐
│ Mongo Change │ (in production: Debezium → Kafka)
│ Streams      │
└──────┬───────┘
       │ live CDC events
       ▼
┌──────────────┐    ┌─────────────────┐   ┌─────────────────┐
│ pkg/exec/    │───▶│ Same Sum monoid │──▶│ Same DDB        │
│ streaming    │    │ same key fn     │   │ order_totals    │
│ + WithDedup  │    │ same value fn   │   │ table           │
└──────────────┘    └─────────────────┘   └─────────────────┘
```

The two binaries share a single `pipeline.go` that defines the order
schema, key extractor, value extractor, monoid, and store. `BuildBootstrap`
adds a Mongo `SnapshotSource`; `BuildLive` adds a Kafka source plus an
optional Deduper.

## Why both modes are needed

- **Bootstrap.** Mongo Change Streams only emit changes; they don't surface
  the documents that already existed when streaming started. Without the
  bootstrap pass the per-customer totals would only reflect orders placed
  after the consumer attached — anything older would be invisible. The
  bootstrap binary scans the collection once and folds every existing
  document through the same monoid Combine.
- **Live.** After bootstrap finishes, the CDC stream takes over. The
  bootstrap binary captures a Change Stream resume token at the start of the
  scan (Debezium's standard "snapshot then stream" pattern); the deployment
  hands that token to the live consumer so the first change observed there
  is the first change since the bootstrap began. No gaps, no duplicates
  beyond what `WithDedup` already absorbs.

## Production wiring

In production the live source is Mongo Change Streams piped through Debezium
into Kafka. Debezium's MongoDB connector emits per-document JSON envelopes
on a topic like `dbserver.shop.orders`; the Kafka source's `Decode` function
unwraps the envelope's `after` key into an `Order`. The example's
`pipeline.go` decoder assumes a flatter shape (raw `Order` JSON) so the
docker-compose stack can drive it directly without standing up Debezium —
swap in your envelope-unwrapping decoder when wiring against real Debezium.

## Run locally

```sh
make compose-up        # mongo (replset), dynamodb-local, kafka, valkey, minio
make seed-ddb DDB_TABLE=order_totals

# Seed Mongo with some historical orders:
docker exec -i murmur-mongo mongosh --quiet shop <<'EOF'
db.orders.insertMany([
  {_id: "o1", customer_id: "cust-a", amount: 100},
  {_id: "o2", customer_id: "cust-a", amount: 50},
  {_id: "o3", customer_id: "cust-b", amount: 25},
])
EOF

# Bootstrap. Prints the resume token (hex) on stdout.
DDB_LOCAL_ENDPOINT=http://localhost:8000 \
  go run ./examples/mongo-cdc-orderstats/cmd/bootstrap

# Now run the live worker against a Kafka topic carrying additional CDC events.
DDB_LOCAL_ENDPOINT=http://localhost:8000 \
KAFKA_BROKERS=localhost:9092 \
DDB_DEDUP_TABLE=order_totals_dedup \
  go run ./examples/mongo-cdc-orderstats/cmd/worker
```

## Test

The end-to-end test in [`test/e2e/mongo_cdc_orderstats_test.go`](../../test/e2e/mongo_cdc_orderstats_test.go)
exercises the full flow: seeds Mongo, runs Bootstrap, produces additional
events to Kafka, runs the live worker briefly, and verifies the merged
per-customer totals equal `bootstrapTotals + liveDeltas` to the cent.
