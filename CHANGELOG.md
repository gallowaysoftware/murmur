# Changelog

All notable changes to Murmur are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Production-readiness pass

A focused push closing the gaps that separate Murmur from "deployable to production AWS shops at meaningful scale." Each entry below references a real package or commit; for the per-commit history use `git log`.

#### Lambda runtimes (closes the AWS-native ingest matrix)

- **`pkg/exec/lambda/kinesis`** — Kinesis-trigger Lambda handler. `BatchItemFailures` for partial-batch retry, dedup-aware via `state.Deduper`, decoder-error callback for poison-pill DLQ routing, retry/backoff via the shared `pkg/exec/processor` core.
- **`pkg/exec/lambda/dynamodbstreams`** — DDB Streams Lambda handler. Decoder takes the whole change record so callers branch on `EventName` / inspect `OldImage`. `ErrSkipRecord` sentinel for "ignore deletes" cases.
- **`pkg/exec/lambda/sqs`** — SQS Lambda handler. Default EventID is `<arn>/<MessageId>`; override via `WithEventID` for FIFO content-dedup or upstream-key dedup. Honors SQS `SentTimestamp` for windowed-bucket assignment so delayed deliveries land in the correct bucket.
- **`pkg/exec/processor`** — shared retry / dedup / metrics core. Streaming, bootstrap, replay, and all three Lambda handlers delegate here, replacing four hand-maintained copies of the same state machine. `MergeMany` is the canonical multi-key entry point.
- **`pkg/murmur.{KinesisHandler,DynamoDBStreamsHandler,SQSHandler,MustHandler}`** — facade wrappers parallel to `RunStreamingWorker`. One-line construction with the standard production option set.

#### Bootstrap sources

- **`pkg/source/snapshot/dynamodb`** — DDB ParallelScan bootstrap. Multi-segment fanout; `CaptureHandoff` returns a Streams shard timestamp so the live consumer resumes gap-and-duplicate-free.
- **`pkg/source/snapshot/jsonl`** — JSON Lines bootstrap from any `io.Reader`. Per-line `OnDecodeError`, `MaxLineSize` cap, `EventIDFn` for re-run idempotency.
- **`pkg/source/snapshot/s3`** — S3 prefix-scan bootstrap composing `jsonl` with `ListObjectsV2 + GetObject + auto-gzip`. Right tool for "bootstrap from a partitioned S3 archive" (Firehose, daily DDB exports, Hive partitions).

#### Query layer

- **`GetWindowMany` / `GetRangeMany`** RPCs — batched windowed reads. For ML rerank with N=200 candidates × M=7 daily buckets, collapses N sequential `GetWindow` calls (~4 s aggregate) into one batched fetch (~20 ms p99).
- **`fresh_read`** flag on every read RPC — bypasses singleflight for read-your-writes flows.
- **Singleflight coalescing** (`pkg/query/grpc.Server`) — concurrent identical reads collapse to one underlying store call. `TestQuery_Get_CoalescesConcurrentReads` proves 50 concurrent identical Gets become 1 store.Get.
- **Per-RPC metrics** — every RPC fires `<pipeline>:query_get` / `query_get_many` / `query_get_window` / `query_get_range` / `query_get_window_many` / `query_get_range_many` latency + event counts.
- **`pkg/query.WarmupWindowed` / `WarmupNonWindowed`** — cache-prefetch helpers for cold-cache p99 mitigation.

#### State layer

- **`pkg/state.NewInstrumented[V]` / `NewInstrumentedCache[V]`** — decorator wrappers that add `metrics.Recorder` hooks (per-op latency + errors). Zero-overhead fallthrough when the recorder is nil.
- **`pkg/state/dynamodb.BytesStore.MergeUpdate`** — CAS retries gain exponential backoff + full jitter, matching the BatchGetItem retry policy.
- **`pkg/state/valkey.BytesCache`** — sketch-shaped state cache for HLL / TopK / Bloom / DecayedSumBytes. Sub-ms reads.

#### Streaming runtime

- **`streaming.WithBatchWindow`** — write aggregation. Per-(entity, bucket) deltas accumulate in memory and flush as a single `MergeUpdate` per key. Confirmed: 1000 hot-key events → 1 store call. Production-critical hot-key feature.
- **`pipeline.KeyByMany`** — multi-key fanout. One event contributes to many aggregation keys at once. Dedup applies once per event regardless of fanout.
- **Bootstrap & Replay shared core** — both runtimes delegate to `processor.MergeMany`, gaining retry, KeyByMany support, and the shared metrics surface. Bootstrap previously failed-fast on transient store errors; now retries with backoff.

#### Algebra / monoids

- **`murmur.Trending[T]`** preset — time-decayed-sum pipeline built on `compose.DecayedSumBytes`.
- **Monoid law coverage** extended to HLL, First, Last, MapMerge, TupleMonoid2, DecayedSumBytes. Every shipped monoid now runs through the property-based law harness in CI.
- **`pkg/projection`** — `LogBucket` / `LinearBucket` / `ManualBucket` + `HysteresisBucket`. Closes the "Hot-document tail" oscillation pathology: a doc oscillating across the log10 boundary at 1000 produces 0 reindexes vs N for the naive case.

#### Observability

- **`pkg/observability/autoscale`** — Signal → Emitter → Run loop for publishing scaling-signal metrics. Reference CloudWatch emitter for ECS Fargate target tracking. `EventsPerSecond` helper.
- **Processor benchmarks** — MergeOne hot path 76 ns/op, 0 allocs. MergeMany 4-key fanout 267 ns/op. Confirms the design-doc claim that the processor is sub-microsecond.

#### Worked examples

- **`examples/search-projector/`** — runnable Pattern B from `doc/search-integration.md`. Lambda projecting bucket transitions into OpenSearch. 0→1M counter rise emits 7 reindexes vs 1M naive.
- **`examples/search-rerank/`** — runnable Pattern A. HTTP search service doing two-stage retrieval (recall + Murmur counter rerank).
- **`examples/recently-interacted-topk/`** — multi-source TopK fed by Kinesis (Lambda) + Kafka (ECS) into the same DDB row.

#### Documentation

- **`doc/design.md`** — 2981-line "magnum opus" deep design treatment. 19 sections, 8 mermaid diagrams. Covers structural monoids, pipeline DSL, execution model, Lambda runtimes, state stores, query layer, observability, bootstrap-to-live handoff, Spark Connect, wire contracts, operational shape, failure model, performance characteristics, testing philosophy, frontiers.
- **`doc/search-integration.md`** — counters + text search architecture. Three patterns (rescore / bucketed indexing / snapshot+delta), pagination treatment for external rescore, two-pass ML ranking framing.
- **`doc/architecture.md`** — Open Questions section updated with status legend. 3 of 5 closed, 1 partial, 1 deferred.

#### Closed STABILITY priorities

1. **Silent error paths** ✅ — closed by `pkg/exec/processor` consolidation.
2. **Worker autoscaling signal** ✅ — `pkg/observability/autoscale`.
3. **Bootstrap-to-live handoff watermarking** ✅ — Mongo, DDB, JSON-Lines all capture handoff tokens.
4. **Window boundary semantics under replay** ✅ — replay / streaming / Lambda all honor `EventTime` from source records.

### Added — earlier in the Unreleased window

- **PR 1 — docs honesty.** New `STABILITY.md` per-package experimental /
  mostly-stable matrix; new `CHANGELOG.md`; README "Limitations to read before
  adopting" section flagging the `replace` directive, single-goroutine
  streaming runtime, permissive CORS, and Kinesis lack of checkpointing.
  README "Quick taste" rewritten to compile and use the recommended
  `pkg/murmur` facade.
- **PR 2 — observability.** `bootstrap.WithMetrics` and `replay.WithMetrics`
  options match streaming's; all three runtimes now record events / errors /
  store_merge / cache_merge latencies. Errors include pipeline name + entity +
  bucket. `OnDecodeError` and `OnFetchError` callbacks added to Kafka,
  Kinesis, S3-replay, and Mongo-bootstrap sources for poison-pill DLQ wiring.
  DynamoDB `BatchGetItem` now retries `UnprocessedKeys` with bounded
  exponential backoff and surfaces an error if any key remains unfetched.
  Mongo `extractID` now returns an error for unsupported `_id` BSON types
  (UUID/Decimal128 explicitly handled) instead of mangling raw bytes into a
  string.
- **PR 3 — monoid laws.** New `pkg/monoid/monoidlaws` package exposes
  `TestMonoid[V](t, m, gen, opts...)` that fuzzes associativity and identity;
  exercised by every built-in monoid in CI. `core.Min` and `core.Max` now
  return `Monoid[Bounded[V]]` so Identity is the unset wrapper rather than
  the zero value of V — fixes the law violation for negative inputs.
  `compose.Decayed` gained a `Set` field so `(0, time.Unix(0, 0))` is no
  longer misclassified as Identity.
- **PR 4 — UI polish.** New `useLivePolling` hook for the React app:
  AbortController per tick, `document.hidden` pause, exponential backoff on
  failure. New `ErrorBoundary` wraps `<Outlet />`. Server-side `?decode=true`
  flag returns monoid-aware decoded values (`int64` for Sum/Count/Min/Max).
  Query Console gains URL-state synchronization for shareable queries.
  Throughput chart switched from cumulative to rate-of-change. Sidebar
  collapses to a hamburger drawer below `md`. Stat grids gain `sm`/`lg`
  breakpoints. WCAG-AA contrast restored on muted text. ReactFlow controls
  re-styled to match the dark theme.
- **CI furniture.** GitHub Actions workflow runs `gofmt`, `go vet`, unit
  tests with `-race`, `golangci-lint`, and the web `tsc --noEmit` / `lint` /
  `build` pipeline. Dependabot configured for Go modules / npm / Actions.
  Top-level `Makefile` exposes `make help`, `make ci`, `make test-unit`,
  `make test-integration`, `make web-build`, `make ui`, `make seed-ddb`,
  `make compose-up`. `scripts/init-mongo-replset.sh` makes the Mongo
  replica-set init idempotent. `.golangci.yml` lint config, issue and PR
  templates, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md` added.

### Changed

- `pkg/admin/dist/` and `web/dist/` are now `.gitignore`'d. Build the UI
  with `make web-build` before `go build ./cmd/murmur-ui`. CI runs both.
- Decayed associativity is documented as approximate (FP) rather than exact.
- `recharts` dropped from `web/package.json` (was unused; sparkline is a
  hand-rolled SVG).

### Breaking changes (pre-1.0)

- `core.Min[V]()` / `core.Max[V]()` now return `Monoid[Bounded[V]]`. Lift
  observations via `core.NewBounded(v)`. Any caller that was using the old
  shape needs a one-line migration.

## [0.1.0] — 2026-05-07

Initial public release. Phase-1 architecture exercised end-to-end against the
docker-compose stack:

- Pipeline DSL with structural monoids (`pkg/pipeline`, `pkg/murmur`)
- Three execution modes: live (Kafka via franz-go; single-instance Kinesis),
  bootstrap (Mongo Change Stream resume token), replay (S3 JSON-Lines)
- Two state stores: DDB `Int64SumStore` (atomic ADD) and `BytesStore` (CAS)
- Valkey `Int64Cache` write-through accelerator
- Monoid library: Sum / Count / Min / Max / First / Last / Set; HLL / TopK
  (Misra-Gries) / Bloom; MapMerge / Tuple2 / DecayedSum
- Windowed aggregations (Daily / Hourly / Minute) with sliding-window queries
- Generic gRPC query service (`Get` / `GetMany` / `GetWindow` / `GetRange`)
- `LambdaQuery` for batch view ⊕ realtime delta merge
- Atomic state-table swap helper (`pkg/swap`)
- Spark Connect batch executor (user-supplied SQL) validated against
  `apache/spark:4.0.1`
- Admin REST API + dark-mode-default web UI (`cmd/murmur-ui`)
- Metrics recorder wired into the streaming runtime
- Terraform `pipeline-counter` module
- Worked example: `examples/page-view-counters` (worker + query binaries +
  Dockerfile)
