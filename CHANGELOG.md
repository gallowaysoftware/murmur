# Changelog

All notable changes to Murmur are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-28

The first tag since `v0.1.0` on 2026-05-12, and mostly a release about making
Murmur honest with itself. It closes a long list of verified defects, most of one class:
something reported success without checking anything. A build that could not
compile on current Go while CI stayed green. Six CloudWatch alarms green on a
pipeline that had stopped. An ALB health check passing against a service that
implemented no health endpoint. A `test/e2e` suite that "passed" by never
running. A dedup claim that made a redelivered record vanish with no error and
no metric, and a graceful shutdown that acked 1,005 records it never merged.

Alongside the fixes: the AWS-native ingest matrix (Kinesis / DynamoDB Streams /
SQS Lambda runtimes over one shared processor core), typed query-client parity
across all four monoid kinds, real health and metrics surfaces, thirteen
packages promoted off `experimental`, and end-to-end Terraform for the
real-AWS soak that gates `v1.0.0`.

Murmur is pre-1.0, so breaking changes are permitted. Every one is listed under
**Changed → Breaking changes** below with the action it requires.

### Security

#### Dependency refresh (round 3) — closes two Go advisories and seven npm ones

Replaces the stale PR #52 bundle, which had drifted three months and would have
landed gRPC *inside* an advisory range.

- **`GO-2026-6061`** — vulnerabilities in gRPC's xDS RBAC engine and HTTP/2
  transport server. Affects `google.golang.org/grpc` below **1.82.1**; `main`
  was on 1.81.0 and PR #52 moved it only to 1.81.1, still inside the range.
  Now on **1.83.2**. The server half is the one that matters here: it is the
  serving path `pkg/query/grpc` runs, and a resource-exhaustion bug there is
  exactly what goes unnoticed in an unattended deployment.
- **`GO-2026-5841`** — out-of-bounds read in `klauspost/compress/s2`. Fixed in
  1.18.7; both #52 and Dependabot #64 land 1.18.6, one patch short. Bumped
  explicitly to **1.19.2**. It is an `// indirect` dependency, so no Dependabot
  PR will ever propose it on its own — it moves only when franz-go's
  requirement moves, and franz-go doesn't force it past 1.18.6. It sits in the
  Kafka record-decompression path, parsing broker-supplied bytes.
- **npm: 7 advisories → 0** (5 high). The one that counts is `react-router`, a
  *runtime* dependency shipped inside the embedded admin UI, carrying five high
  advisories including an open redirect and an XSS. Also cleared: `vite`,
  `postcss`, `nanoid`, `brace-expansion`, `@babel/core`.
- Unlike #52, the npm `^` ranges are preserved rather than silently converted
  to exact pins.

Both Go advisories went unproposed for months because Dependabot could not open
any PR at all — see **Changed → Dependencies, toolchain and dependency
automation**.

### Added

#### Lambda runtimes (closes the AWS-native ingest matrix)

- **`pkg/exec/lambda/kinesis`** — Kinesis-trigger Lambda handler.
  `BatchItemFailures` for partial-batch retry, dedup-aware via `state.Deduper`,
  decoder-error callback for poison-pill DLQ routing, retry/backoff via the
  shared `pkg/exec/processor` core.
- **`pkg/exec/lambda/dynamodbstreams`** — DDB Streams Lambda handler. The
  decoder takes the whole change record so callers branch on `EventName` /
  inspect `OldImage`. `ErrSkipRecord` sentinel for "ignore deletes" cases.
- **`pkg/exec/lambda/sqs`** — SQS Lambda handler. Default EventID is
  `<arn>/<MessageId>`; override via `WithEventID` for FIFO content-dedup or
  upstream-key dedup. Honors SQS `SentTimestamp` for windowed-bucket assignment
  so delayed deliveries land in the correct bucket.
- **`pkg/exec/processor`** — shared retry / dedup / metrics core. Streaming,
  bootstrap, replay and all three Lambda handlers delegate here, replacing four
  hand-maintained copies of the same state machine. `MergeMany` is the
  canonical multi-key entry point.
- **`pkg/murmur.{KinesisHandler,DynamoDBStreamsHandler,SQSHandler,MustHandler}`**
  — facade wrappers parallel to `RunStreamingWorker`. One-line construction
  with the standard production option set.
- **`pipeline.KeyByMany`** — multi-key fanout. One event contributes to many
  aggregation keys at once. Dedup applies once per event regardless of fanout.
- **Bootstrap & replay share the processor core** — both delegate to
  `processor.MergeMany`, gaining retry, KeyByMany support and the shared
  metrics surface. Bootstrap previously failed-fast on transient store errors;
  it now retries with backoff.

#### Dedup: claims can be given back

- **`state.Deduper.Release(ctx, eventID) error`** — the interface had no way to
  hand a claim back, which is what made a failed-merge-after-claim an
  unrecoverable drop (see **Fixed**). Releasing an unclaimed ID must be a
  no-op, not an error. Breaking for out-of-tree implementors.
- **`pkg/state/dynamodb.Deduper.Release`** deletes the claim row via
  unconditional `DeleteItem`, which is naturally idempotent and treats a TTL
  eviction that beat it as success. The `dynamodb:DeleteItem` grant already
  exists in both Terraform modules, so no IAM change is required.
- **`processor.ReleaseClaims(ctx, cfg, pipelineName, eventIDs)`** — the shared
  detached-context release path behind `MergeMany`, `Coalescer.Flush` and the
  streaming aggregator. Out-of-tree drivers that claim EventIDs at buffer time
  should call it on their failure path. One time budget spans the whole slice
  so a large failed batch cannot stall shutdown; anything it cannot cover is
  reported as `dedup_release_failed` instead of going out silently.
- **`(*Deduper).ForPipeline`** derives a sibling scope over the same table for
  a worker process hosting several pipelines.
- New metrics: `<pipeline>:dedup_release`, `<pipeline>:dedup_release_failed`
  and `<pipeline>:shutdown_unacked`.

#### Sources and replay

- **`pkg/source/snapshot/dynamodb`** — DDB ParallelScan bootstrap.
  Multi-segment fanout; `CaptureHandoff` returns a Streams shard timestamp so
  the live consumer resumes gap-and-duplicate-free.
- **`pkg/source/snapshot/jsonl`** — JSON Lines bootstrap from any `io.Reader`.
  Per-line `OnDecodeError`, `MaxLineSize` cap, `EventIDFn` for re-run
  idempotency.
- **`pkg/source/snapshot/s3`** — S3 prefix-scan bootstrap composing `jsonl`
  with `ListObjectsV2 + GetObject + auto-gzip`. The right tool for "bootstrap
  from a partitioned S3 archive" (Firehose, daily DDB exports, Hive
  partitions).
- **`pkg/replay/s3.ParquetDriver`** pairs with the existing JSON-Lines
  `Driver`: list S3 objects under a prefix, GetObject, read each as Parquet via
  apache/arrow-go/v18, emit one `source.Record` per row via a user-supplied
  `ParquetDecoder(arrow.Record, row int) -> T`. The same archive can hold both
  formats; the default `KeyFilter` selects only `*.parquet`. No new direct
  dependencies — arrow-go was already pulled in by
  `pkg/source/snapshot/parquet`.
- **`pkg/source/kafka.NewDLQProducer`** — convenience wiring of a franz-go
  producer into the existing `OnDecodeError` / `OnFetchError` callbacks so
  poison pills land on a dead-letter topic with diagnostic headers
  (`x-murmur-source-topic` / `-partition` / `-offset` / `-error` /
  `-error-kind`).
- **`pkg/source/kafka.Config.Concurrency`** — N decoder goroutines plus one
  fetcher; each partition pinned to worker `partition mod N` so per-partition
  order is preserved while decode-heavy formats (Protobuf with schema lookups,
  encrypted payloads) saturate multiple cores. Default `Concurrency=1` keeps
  the historical single-goroutine path verbatim. Lifts the "no per-partition
  parallelism" line.

#### Query layer

- **`GetWindowMany` / `GetRangeMany`** RPCs — batched windowed reads. For ML
  rerank with N=200 candidates × M=7 daily buckets, this collapses N sequential
  `GetWindow` calls (~4 s aggregate) into one batched fetch (~20 ms p99).
- **`fresh_read`** flag on every read RPC — bypasses singleflight for
  read-your-writes flows.
- **Singleflight coalescing** (`pkg/query/grpc.Server`) — concurrent identical
  reads collapse to one underlying store call.
  `TestQuery_Get_CoalescesConcurrentReads` proves 50 concurrent identical Gets
  become 1 `store.Get`.
- **Per-RPC metrics** — every RPC fires `<pipeline>:query_get` /
  `query_get_many` / `query_get_window` / `query_get_range` /
  `query_get_window_many` / `query_get_range_many` latency + event counts.
- **`pkg/query.WarmupWindowed` / `WarmupNonWindowed`** — cache-prefetch helpers
  for cold-cache p99 mitigation.
- **Typed-client parity** — `pkg/query/typed`'s `HLLClient`, `TopKClient` and
  `BloomClient` all grew `GetMany` + `GetRange` to match `SumClient`.
  Per-entity present flags on `GetMany` (the underlying RPC surfaces them);
  merged response on `GetRange` (the RPC merges before returning).
- **`query.ErrInvalidQuery`** — the sentinel the new degenerate-range and
  retention checks match against.
- **`grpc.Config.CoalesceTimeout`** (default 10s) bounds detached shared work.

#### Typed gRPC codegen

- **`get_window_many`** methods alongside `get_all_time` / `get_window`, with
  per-kind response shapes: **sum / hll** → `repeated int64 values` (delegates
  to `typed.{Sum,HLL}Client.GetWindowMany`); **topk** → `repeated TopKItemList
  entries` (proto3 disallows nested repeated, so each entry wraps the
  per-entity ranking); **bloom** → `repeated BloomShape entries` (per-entity
  filter structural metadata).
- **`get_many`** — batched all-time read with a per-entity present flag.
  Response: `repeated int64 values + repeated bool present`. Lets callers
  distinguish "absent" from "present-and-zero", which `get_window_many` cannot.
- **`get_range`** — absolute Unix-second start/end range read. Response:
  `int64 value`, merged across the buckets in range.
- Both shipped Sum-only first, gated at validate time to `pipeline_kind=sum`
  because only `SumClient` exposed the methods. That gate is now lifted: new
  per-kind render branches emit shape-appropriate response messages and
  server-stub Go code for HLL / TopK / Bloom too.
- **Spec validation requires the `key_template` to reference the
  `many_key_field`**; otherwise every element of the batch produces the same
  key and the loop variable is unused.
- All three example specs (`bot-interactions` / `top-products` /
  `recent-visitors`) grew a `Get*WindowMany` method; `bot-interactions`
  additionally grew `GetCountMany` + `GetCountRange`, and `top-products` /
  `recent-visitors` grew `GetTopMany` / `GetTopRange` /
  `GetFilterShapeMany` / `GetFilterShapeRange` so the new render paths are
  covered by the existing golden tests. Goldens regenerated.
- Closes the "Per-pipeline gRPC codegen (typed responses)" roadmap row in
  `README.md` for the four supported pipeline kinds.

#### State layer

- **`pkg/state.NewInstrumented[V]` / `NewInstrumentedCache[V]`** — decorator
  wrappers that add `metrics.Recorder` hooks (per-op latency + errors).
  Zero-overhead fallthrough when the recorder is nil.
- **`pkg/state/dynamodb.BytesStore.MergeUpdate`** — CAS retries gain
  exponential backoff + full jitter, matching the BatchGetItem retry policy.
- **CAS retry knobs**: `WithCASRetries` and `WithCASBackoff` (previously 8
  attempts and ~17 s of backoff, hardcoded with no setter), plus
  `WithCASMetrics`, which counts contention under `<pipeline>:cas_conflict`. A
  hot key used to burn its whole budget and dead-letter with nothing in the
  metrics to say the writes were racing rather than failing.
  `ErrMaxRetriesExceeded` now carries the table, key and attempt count.
- **`ErrItemTooLarge` / `*ItemTooLargeError`** (carrying the key and measured
  size) — the typed result of the new 400KB pre-flight described under
  **Fixed**.
- **`pkg/state/valkey.BytesCache`** — sketch-shaped state cache for HLL / TopK
  / Bloom / DecayedSumBytes. Sub-ms reads.
- **`pkg/state/valkey.BloomCache`** mirrors `HLLCache` for Bloom filters using
  the `valkey-bloom` / RedisBloom `BF.*` command surface (`BF.ADD` / `BF.MADD`
  / `BF.EXISTS` / `BF.MEXISTS` / `BF.RESERVE` / `BF.INFO`). Side-by-side with
  the BytesStore-authoritative bits-and-blooms sketch; independent FPR
  realizations. Integration tests gate on `VALKEY_BLOOM_ENABLED` and skip
  cleanly when the module isn't loaded. Closes the only explicit roadmap row in
  `README.md`.

#### Streaming runtime

- **`streaming.WithBatchWindow`** — write aggregation. Per-(entity, bucket)
  deltas accumulate in memory and flush as a single `MergeUpdate` per key.
  Confirmed: 1000 hot-key events → 1 store call. Production-critical hot-key
  feature.

#### Algebra / monoids

- **`murmur.Trending[T]`** preset — time-decayed-sum pipeline built on
  `compose.DecayedSumBytes`.
- **`WithDecodeErrorHandler(func(error))`** on `hll`, `topk`, `bloom` and
  `compose.DecayedSumBytes`. All four recover from a `Combine` decode failure
  by returning whichever operand decoded and discarding the other —
  `Monoid.Combine` is `Combine(a, b) V` with no error return, so that recovery
  is the only option short of corrupting merged state or panicking a worker
  mid-batch — but it was **silent**, and the affected key just quietly lost
  cardinality or counts. Errors name the sketch and which operand failed, and
  wrap the underlying cause so callers can `errors.Unwrap` rather than
  string-match. Variadic, so no existing call site changes.
- **`compose.ClampFuture(t, now, bound)` and `compose.DefaultSkewBound(halfLife)`**
  (experimental) — the future-timestamp clamp, applied at the lift rather than
  in `Combine`. Use them in any pipeline whose value extractor takes the
  timestamp from the event rather than from the clock; `murmur.Trending` stamps
  at its own clock and is unaffected.
- **`pkg/monoid/monoidlaws` fuzzes a non-default capacity**: a Bloom monoid
  whose `(m, k)` differs from its operands'. A mismatched-K TopK case waits on
  the deferred wire-format work above — against today's `Combine` it would
  fail, which is the point.
- **`pkg/projection`** — `LogBucket` / `LinearBucket` / `ManualBucket` +
  `HysteresisBucket`. Closes the "hot-document tail" oscillation pathology: a
  doc oscillating across the log10 boundary at 1000 produces 0 reindexes vs N
  for the naive case.

#### Health, metrics and observability

- **Real health endpoints on the query server.** Nothing in the tree
  implemented `grpc.health.v1.Health`, yet `pipeline-counter` provisioned an
  ALB target group probing `/grpc.health.v1.Health/Check`. That probe "passed"
  only because ALB read the gRPC `UNIMPLEMENTED` status (12) and the matcher
  was a permissive `0-99`. It proved the port was open and nothing else — a
  task that could not reach DynamoDB stayed healthy and kept receiving traffic.
  - **`Server.HealthHandler`** serves the standard `grpc.health.v1.Health`
    service via `connectrpc.com/grpchealth` (**new direct dependency**; it
    requires only `connect` and `protobuf`, both already direct, so it adds no
    new transitive dependencies).
  - **`Server.HealthzHandler`** serves plain HTTP for callers that cannot speak
    gRPC health — an HTTP1 target group, a Kubernetes probe, curl. It separates
    the two questions orchestrators actually ask, which one endpoint conflates:
    `/healthz` is **liveness** and is always 200, because answering 503 there
    makes the orchestrator restart a healthy task and turns a store blip into a
    crash loop; `/readyz` is **readiness** and reports whether the store
    answered.
  - **Readiness results are cached** (`DefaultHealthCacheTTL`, 10 s). An ALB
    probes every 15 s per target and Kubernetes often tighter, so an uncached
    probe turns health checking into steady billed reads and couples probe
    latency to store latency — a slow-but-working table would start failing
    health checks. `WithHealthCacheTTL`, `WithHealthProbe` and
    `WithHealthSentinelKey` override the defaults.
- **`pkg/metrics/emf`** implements `metrics.Recorder` on top of the CloudWatch
  Embedded Metric Format. This closes a real observability hole rather than
  adding a nicety: both example binaries used `metrics.NewInMemory()` — an
  in-process map that nothing ever read. Every in-pipeline signal
  (`dedup_skip`, `dedup_release`, `dedup_release_failed`, decode errors, retry
  counts, store latency) was therefore invisible, while the CloudWatch alarms
  could only see Lambda's `Errors` / `Throttles` / `IteratorAge` — none of
  which move when records are silently dropped or deduplicated. A pipeline
  could discard records for a whole quarter with every alarm green.
  - **No new IAM or SDK client.** EMF metrics are extracted from structured
    JSON on stdout, which Lambda forwards natively and ECS forwards via the
    awslogs driver.
  - **Aggregated, not per-event.** One EMF document per pipeline per flush
    interval (default 60 s), carrying counters as sums and latencies as EMF
    StatisticSets. CloudWatch Logs bills by the byte, so per-record emission
    would make observability cost scale with throughput; at the default a
    pipeline emits 1440 documents a day whether it processed a hundred records
    or a hundred million. An idle pipeline emits nothing.
  - **Sub-event names are split, not passed through.** Runtimes encode
    sub-events as `pipeline:sub_event`; emitting that verbatim would create a
    separate `Pipeline` dimension value per sub-event and fragment every
    dashboard. `orders:dedup_skip` becomes metric `DedupSkip` on
    `Pipeline=orders`. Sub-scoped errors also increment the pipeline's `Errors`
    total, so an alarm on `Errors` cannot miss them.
  - Both `examples/recently-interacted-topk` binaries now use it. The Lambda
    flushes per invocation — Lambda freezes the execution environment on
    return, so a background ticker is not guaranteed to fire and a low-traffic
    function would otherwise emit nothing at all.
- **`pkg/observability/autoscale`** — Signal → Emitter → Run loop for
  publishing scaling-signal metrics. Reference CloudWatch emitter for ECS
  Fargate target tracking. `EventsPerSecond` helper.
- **Processor benchmarks** — MergeOne hot path 76 ns/op, 0 allocs. MergeMany
  4-key fanout 267 ns/op. Confirms the design-doc claim that the processor is
  sub-microsecond.

#### Admin and UI

- **`pkg/admin.WithAuthToken`** — static-bearer-token Authenticator with
  constant-time comparison and multi-token rotation. Tokens that don't match
  yield 401 with a `WWW-Authenticate: Bearer realm=...` hint and no body.
- **`pkg/admin.WithJWTVerifier`** — wraps a user-supplied `JWTVerifier` (OIDC /
  JWKS / JWT library of choice) as an Authenticator.
- **`pkg/admin.Authenticator`** — pluggable interface for callers wiring their
  own auth schemes.
- Middleware ordering: CORS (with OPTIONS short-circuit) outside Auth outside
  the Connect handler. Auth is off by default — unchanged for same-origin /
  network-isolated deploys.
- **`cmd/murmur-ui --auth-token`** + `MURMUR_ADMIN_TOKEN` env fallback makes
  the bundled UI auth-aware without code changes.
- **UI polish** — new `useLivePolling` hook (AbortController per tick,
  `document.hidden` pause, exponential backoff on failure); new `ErrorBoundary`
  wrapping `<Outlet />`; server-side `?decode=true` flag returning
  monoid-aware decoded values (`int64` for Sum/Count/Min/Max); URL-state
  synchronization in the Query Console for shareable queries; throughput chart
  switched from cumulative to rate-of-change; sidebar collapses to a hamburger
  drawer below `md`; stat grids gain `sm`/`lg` breakpoints; WCAG-AA contrast
  restored on muted text; ReactFlow controls re-styled to match the dark theme.

#### Deploy: real-AWS composition for `recently-interacted-topk`

End-to-end Terraform under
[`examples/recently-interacted-topk/terraform/`](examples/recently-interacted-topk/terraform/)
that stands up the canonical multi-source TopK example on real AWS in one
`terraform apply`. Targeted at the v1 real-AWS soak; exercises every
`experimental`-flagged package gated on operational evidence (Lambda runtimes,
processor core, Kafka per-partition concurrency).

- **`pipeline-counter`** grew an optional `dedup_enabled` flag that provisions
  a sibling DDB dedup table (pk:S hash key, native TTL on `ttl`), grants the
  worker + bootstrap roles read+write, and injects `DDB_DEDUP_TABLE` into all
  three task environments. Exposed as `dedup_table_arn` / `dedup_table_name`
  outputs so sibling modules can share the same table.
- **`pipeline-lambda-kinesis`** is a new sibling module: Kinesis data stream
  (or BYO via `kinesis_stream_arn`), Lambda function (provided.al2, arm64 by
  default), event-source mapping with `BatchItemFailures` +
  `bisect_batch_on_function_error`, Lambda IAM with Kinesis consumer perms +
  DDB state + optional dedup grants + optional SQS/SNS on-failure destination,
  CloudWatch log group with caller-controlled retention.
- The composition provisions CloudWatch alarms (Lambda Errors / IteratorAge /
  Throttles, DDB WriteThrottles on state+dedup, Kinesis WriteThrottles) and
  ships a `terraform.tfvars.example` plus a runbook covering build → push →
  apply → smoke-test → teardown.
- **Atomic state swap wired into Terraform.**
  `deploy/terraform/modules/pipeline-counter` gained opt-in `swap_enabled =
  true`, which provisions a [`pkg/swap`](pkg/swap) control DDB table, seeds the
  alias pointer when `swap_initial_version` is set, grants the worker / query /
  bootstrap task roles the right IAM (read on worker+query, read+write on
  bootstrap), and injects `SWAP_CONTROL_TABLE` + `SWAP_ALIAS` env vars into
  every task definition. Module README gains a v1 → v2 cutover recipe. Default
  behavior is unchanged (`swap_enabled = false`).

#### Deploy: soak observability and cost controls

- **Liveness alarms.** All six existing alarms were positive-polarity
  (`count > 0`) with `treat_missing_data = "notBreaching"`, so a pipeline that
  simply STOPPED reported OK on every one — an all-green console for a dead
  soak, which is the inverse of the signal it was meant to give. Added
  `lambda-silent` (Invocations), `kinesis-silent` (IncomingRecords),
  `worker-not-running` and `query-not-running` (ECS RunningTaskCount), all
  `LessThanThreshold` with `treat_missing_data = "breaching"`. The ECS/Kafka
  half previously had no alarm coverage of any kind.
- **`ddb-write-runaway`** on `ConsumedWriteCapacityUnits`. TopK is a
  non-coalescable CAS monoid on a single row, and a `PAY_PER_REQUEST` table
  under CAS contention does not throttle — it just bills — so the existing
  `WriteThrottleEvents` alarms cannot detect a runaway.
- **`assign_public_ip`** on `pipeline-counter` (default `false`, unchanged
  behaviour). Fargate tasks were hardcoded into private subnets; with no NAT
  gateway `apply` still succeeds and the tasks then loop forever on
  `CannotPullContainerError` — silently, since nothing alarmed on ECS. Public
  IPs cost ~$14.60/mo for four tasks against ~$32.85/mo for one NAT, and the
  tasks stay unreachable inbound.
- **`query_alb_enabled`** (default `true`, unchanged behaviour) makes the ALB
  optional. It is a ~$17–22/mo standing charge that exists only to front the
  query service, which a soak does not need.
- **`soak.tfvars.example`** — the ~$60–80/mo shape, versus ~$180–225/mo for the
  production-shape defaults.

#### CI and test infrastructure

- **CI runs the `test/e2e` suite.** The nine-file library-shape suite (counter,
  HLL, windowed, Mongo bootstrap, Mongo CDC, DDB bootstrap, S3 replay, S3
  Parquet replay, Kafka concurrency) had **never run in CI**. `README.md`
  advertises it and `doc/design.md` stated "The CI runs them on every PR" —
  which was false; it ran only via `make test-integration`, which (see
  **Fixed**) did not work either. The new `Library-shape E2E (test/e2e)` job
  stands up the compose stack and runs it. It complements the existing
  `integration` job rather than duplicating it: that one exercises the
  *deployed* shape (built images run as containers), this one the *library*
  shape (murmur as a Go package against real infra).
- **The E2E job fails on a skip.** These tests gate themselves on infra env
  vars, so "ran nothing" and "everything passed" are the same exit code —
  exactly the property that let the suite rot unnoticed.
  `scripts/assert-tests-ran.py` enforces it.
- **CI gained a `Go (latest stable toolchain)` job.** Every existing Go job
  pins `go-version-file: go.mod`, so a dependency that breaks only under a
  newer toolchain could never be caught. The new lane builds and unit-tests
  against current Go as a hard gate.
- **New dedup coverage**: release at `DefaultMaxKeys` scale, budget scaling in
  isolation, a mixed-outcome flush (one key lands, a sibling fails, disjoint
  events) whose replay proves the surviving key is not double-counted, a
  cancellation landing on an in-flight aggregator flush, and a shutdown drain
  spending exactly one release budget. Regression tests also cover all three
  release directions: a failed merge releases and the redelivery applies, a
  successful merge keeps its claim, and a release failure is recorded without
  masking the merge error.
- **`topk` saturation tests** documenting that a `K=32` summary drops from 32
  counters covering 45,932 events to 29 counters covering 29 when a 33rd
  entity appears, and that the counts are Misra-Gries lower bounds with an
  `n/(K+1)` error bar that callers must size from an `n` the sketch does not
  record.
- **The replay dedup-TTL contract** moved from a unit test asserting against
  its own hand-rolled expiring fake to `test/e2e/replay_dedup_ttl_test.go`,
  which exercises the real `pkg/state/dynamodb.Deduper` behind the
  `DDB_LOCAL_ENDPOINT` gate (identical re-run → 100; re-run past a 1h TTL →
  200).
- **CI furniture** (earlier in the window): GitHub Actions workflow running
  `gofmt`, `go vet`, unit tests with `-race`, `golangci-lint`, and the web
  `tsc --noEmit` / `lint` / `build` pipeline; Dependabot configured for Go
  modules / npm / Actions; a top-level `Makefile` exposing `make help`,
  `make ci`, `make test-unit`, `make test-integration`, `make web-build`,
  `make ui`, `make seed-ddb`, `make compose-up`;
  `scripts/init-mongo-replset.sh` making the Mongo replica-set init idempotent;
  `.golangci.yml`, issue and PR templates, `CONTRIBUTING.md`, `SECURITY.md`,
  `CODE_OF_CONDUCT.md`.

#### Worked examples

- **`examples/search-projector/`** — runnable Pattern B from
  `doc/search-integration.md`. Lambda projecting bucket transitions into
  OpenSearch. A 0→1M counter rise emits 7 reindexes vs 1M naive.
- **`examples/search-rerank/`** — runnable Pattern A. HTTP search service doing
  two-stage retrieval (recall + Murmur counter rerank).
- **`examples/recently-interacted-topk/`** — multi-source TopK fed by Kinesis
  (Lambda) + Kafka (ECS) into the same DDB row.

#### Documentation

- **`doc/design.md`** — 2981-line deep design treatment. 19 sections, 8 mermaid
  diagrams. Covers structural monoids, pipeline DSL, execution model, Lambda
  runtimes, state stores, query layer, observability, bootstrap-to-live
  handoff, Spark Connect, wire contracts, operational shape, failure model,
  performance characteristics, testing philosophy, frontiers.
- **`doc/search-integration.md`** — counters + text search architecture. Three
  patterns (rescore / bucketed indexing / snapshot+delta), pagination treatment
  for external rescore, two-pass ML ranking framing.
- **`doc/architecture.md`** — Open Questions section updated with a status
  legend. 3 of 5 closed, 1 partial, 1 deferred.
- **`STABILITY.md`** (earlier in the window) — per-package experimental /
  mostly-stable matrix; this `CHANGELOG.md`; and a README "Limitations to read
  before adopting" section flagging the `replace` directive, the
  single-goroutine streaming runtime, permissive CORS, and Kinesis's lack of
  checkpointing. README "Quick taste" rewritten to compile and to use the
  recommended `pkg/murmur` facade.
- **Observability plumbing** (earlier in the window): `bootstrap.WithMetrics`
  and `replay.WithMetrics` matching streaming's; all three runtimes recording
  events / errors / store_merge / cache_merge latencies, with errors carrying
  pipeline name + entity + bucket; `OnDecodeError` / `OnFetchError` callbacks
  added to the Kafka, Kinesis, S3-replay and Mongo-bootstrap sources for
  poison-pill DLQ wiring.

### Changed

#### Breaking changes

- **`go.mod` now declares `go 1.25.0`** (was `go 1.26.2`). The old
  patch-level directive was never a deliberate choice — it is whatever
  `go mod init` wrote against a local toolchain in the initial commit — and it
  forced every consumer onto Go >= 1.26.2. 1.25.0 is the real floor: five
  direct dependencies (`golang.org/x/net`, `google.golang.org/grpc`,
  `golang.org/x/sys`, `connectrpc.com/connect`, `apache/arrow-go`) declare it.
  *Action:* none, unless you were on 1.24 or older, in which case this is the
  first release you can build.

Pre-1.0, so these are permitted. Each names what an operator or caller must do.

- **`state.Deduper` gains `Release(ctx, eventID) error`.** Breaking for anyone
  implementing the interface outside this repo. *Action:* implement `Release`;
  releasing an unclaimed ID must be a no-op, not an error. In-tree
  implementations and both Terraform modules already have the required
  `dynamodb:DeleteItem` grant, so no IAM change is needed.
- **`pkg/state/dynamodb.NewDeduper` takes a pipeline name:
  `NewDeduper(client, table, pipeline, ttl)`** — and this changes the
  **on-disk dedup key format** to `"<pipeline>#<EventID>"`. *Action:* an
  operator with an existing dedup table gets one window of re-processing as
  previously-claimed IDs re-claim under their namespaced keys; for
  non-idempotent monoids (Sum, HLL, TopK), drain in-flight records before
  deploying, or let the old rows age out via the table's TTL first. No table
  schema change — the partition key attribute is unchanged, only its contents.
- **`pkg/state/dynamodb.NewBytesStore` takes variadic `BytesStoreOption`s.**
  *Action:* none for existing three-argument calls, which compile unchanged.
- **`QueryService.Get` / `GetMany` now reject windowed pipelines** with
  `FAILED_PRECONDITION` instead of reporting `present: false`. *Action:*
  windowed pipelines must call `GetWindow` / `GetWindowMany`; four shipped
  runbooks pointed at the wrong RPC and are corrected. `fresh_read` does not
  bypass the check. A `Granularity` of zero still legitimately writes bucket 0
  and stays allowed.
- **Degenerate windows and ranges now error.** `INVALID_ARGUMENT` instead of a
  fabricated monoid-identity value: `duration_seconds` must be positive and
  must not overflow the nanosecond `time.Duration`, absolute ranges must set at
  least one bound, and `start_unix > end_unix` is rejected. The `pkg/query`
  equivalents (`GetWindow` / `GetRange` / `GetWindowMany` / `GetRangeMany` /
  `LambdaQuery` / `WarmupWindowed`) return errors matching the new
  `query.ErrInvalidQuery`. *Action:* callers that relied on a zero-value
  response for an unset range must now supply a valid range or handle the
  error — in particular anything passing a zero `time.Time` through
  `pkg/query/typed`, which sends it as `-62135596800`.
- **`windowed.Config.MaxBucketSpan()` counts both ends of an inclusive range**,
  so the default derived from `Retention` is now
  `ceil(Retention/Granularity) + 1`. *Action:* none if you set `MaxBuckets`
  explicitly — an explicit value is still used verbatim. Anything asserting on
  the derived number must add one.
- **A `windowed.Config` with a `Granularity` but neither `MaxBuckets` nor
  `Retention` is no longer unbounded**; it caps reads at the new
  `DefaultMaxBucketSpan` (100,000 buckets). *Action:* set `Retention` or
  `MaxBuckets` if you legitimately need a wider fan-out. A `Config` with no
  `Granularity` still reports 0 — it assigns everything to bucket 0 and cannot
  fan out.
- **`windowed.Config.EventTimeField` is deleted.** It was documented as honored
  by backends and read by nothing — event time has only ever come from
  `source.Record.EventTime`. *Action:* delete the field from your `Config`
  literal; callers setting it were configuring nothing. To bucket by a
  timestamp inside the payload, use the source's own `EventTime` extractor (the
  S3 / JSONL / Parquet snapshot readers take one).
- **The web build requires Node 24.** Node 20 → 24 (active LTS), pinned in
  three places that can no longer drift apart: `.nvmrc` (new), `engines` in
  `web/package.json` (new), and `node-version` in `ci.yml`. *Action:* install
  Node 24 before `make web-build` / `make ui`; Node 20 has passed
  end-of-life.
- **`bloom.Identity()` returns an empty slice** rather than a marshaled empty
  filter, so it is an identity for an operand of any shape. *Action:* none for
  pipelines; callers comparing against a marshaled empty filter must compare
  against the empty slice.
- **`compose.DecodeDecayed` returns `(Decayed, error)`.** Any length other than
  0 or 17 is now an error. *Action:* handle the second return value.
- **`compose.EvaluateAt` no longer scales a value up when evaluated before its
  reference time**; it returns the stored value. *Action:* none, unless you
  depended on un-decaying — which is an unbounded over-estimate (`+Inf` for a
  four-year-ahead row at `halfLife=24h`).
- **`compose.DecayedSum` / `DecayedSumBytes` and `bloom.Bloom` take variadic
  options.** *Action:* none; existing calls compile unchanged.
- **`core.Min[V]()` / `core.Max[V]()` return `Monoid[Bounded[V]]`.** *Action:*
  lift observations via `core.NewBounded(v)` — a one-line migration per call
  site. This is what makes `Identity` the unset wrapper rather than the zero
  value of `V`, fixing the law violation for negative inputs.
- **`pkg/query/typed`'s batched clients (`GetMany`, `GetWindowMany` on all four
  clients) return an error when the response's value count does not match the
  requested entity count.** *Action:* handle the error; the wire contract is
  positional, so a mismatch means the values cannot be attributed at all.
- **Terraform: `pipeline-lambda-kinesis` replaces the `dedup_table_arn != null`
  gate with an explicit `dedup_enabled` bool.** *Action:* set `dedup_enabled`;
  the ARN now scopes the IAM policy only. Passing only the ARN no longer
  enables dedup — and the old shape could not even complete a `plan`.
- **Terraform: the AWS provider constraint tightens from `>= 5.0` to `~> 6.0`**
  across all three configurations. *Action:* re-run `terraform init -upgrade`
  on provider v6. The code reads `data.aws_region.current.region`, which exists
  only on v6 (v5 exposes `.name`), so the old floor was wrong — and an
  open-ended upper bound let an unattended re-init during a long soak pull a
  future v7 and plan destructive replacements against live resources.
- **Terraform: health-check matchers are tightened** — `0-99` → `0` and
  `200-499` → `200` — and the HTTP1 target-group path moves from `/` to
  `/readyz`. *Action:* deploy a query image built from this release before
  applying; the old matchers passed on gRPC `UNIMPLEMENTED` and on an HTTP 404,
  and the new path requires the new `HealthzHandler`.
- **Terraform: `protocol_version = GRPC` is now gated on a new
  `query_certificate_arn`.** *Action:* supply a certificate to get an HTTPS
  listener with a GRPC target group; without one the listener falls back to
  HTTP1, which still serves Connect's HTTP+JSON on the same port, since
  `pkg/query/grpc` speaks gRPC, gRPC-Web and Connect from one handler.
- **Terraform: the ECS liveness alarms now assert Container Insights at plan
  time**, via a `postcondition` on a new `aws_ecs_cluster` data source.
  *Action:* enable Container Insights on the (caller-owned) cluster — the
  enabling command is in the error message — or set the
  `require_container_insights` escape hatch.

#### Stability matrix

- **Thirteen packages move off `experimental`.** Promotion criteria: the
  package's feature surface has been exercised by integration / e2e tests, the
  STABILITY rows tracked against it in earlier passes have all closed, and
  there are no open known sharp edges in its notes column. Promoted —
  **State**: `pkg/state/dynamodb`, `pkg/state/valkey`. **Streaming runtime**:
  `pkg/exec/streaming`, `pkg/exec/bootstrap`, `pkg/exec/replay`. **Sources**:
  `pkg/source/snapshot/jsonl`, `pkg/source/snapshot/s3`. **Replay**:
  `pkg/replay/s3`. **Query**: `pkg/query/grpc`, `pkg/query/typed`. **Admin**:
  `pkg/admin`. **Algebra**: `pkg/monoid/compose`. **Tools**:
  `cmd/murmur-codegen-typed`.
- **Holding `experimental` on purpose**: `pkg/source/kafka` (per-partition
  concurrency is fresh; needs soak); `pkg/exec/processor` +
  `pkg/exec/lambda/{kinesis,dynamodbstreams,sqs}` (processor's docstring ties
  its stability to the Lambda runtimes, which haven't been exercised against
  real, non-`local` AWS yet — promote the four together after the real-AWS
  soak); `pkg/source/snapshot/mongo`, `pkg/source/snapshot/dynamodb` (known
  sharp edges still documented in the notes column);
  `pkg/monoid/sketch/{hll,topk,bloom}` (cross-runtime encoding portability not
  yet proven); `pkg/projection`, `pkg/observability/autoscale` (too new);
  `pkg/pipeline`, `pkg/murmur` (author-flagged "expect renames before v1");
  `pkg/exec/batch/sparkconnect` (the `replace`-directive gotcha persists until
  the fork is upstreamed); `cmd/murmur-ui` (explicitly "demo-grade
  dashboard").
- **Closed STABILITY rows**: `pkg/source/kafka` (DLQ hook + per-partition
  parallelism), `pkg/state/valkey` (Bloom accelerator), `pkg/replay/s3`
  (Parquet), `pkg/admin` (auth middleware), `pkg/swap` (Terraform
  integration), `pkg/query/typed` + `cmd/murmur-codegen-typed` (Sum-only
  restriction on `get_many` / `get_range` lifted).
- **Closed STABILITY priorities**: (1) silent error paths — closed by the
  `pkg/exec/processor` consolidation; (2) worker autoscaling signal —
  `pkg/observability/autoscale`; (3) bootstrap-to-live handoff watermarking —
  Mongo, DDB and JSON-Lines all capture handoff tokens; (4) window boundary
  semantics under replay — replay / streaming / Lambda all honor `EventTime`
  from source records.
- **`pkg/source/kinesis` is now flagged "dev / demo only".** It is a
  single-instance polling consumer with no checkpointing — fine for
  integration tests and local one-shot consumers, not for production.
  Production Kinesis ingest goes through `pkg/exec/lambda/kinesis`, which lets
  AWS Lambda's event-source mapping own shard discovery, lease coordination,
  autoscaling (via `ParallelizationFactor`), checkpointing, and partial-batch
  retry (`BatchItemFailures`).
- **`STABILITY.md`'s `pkg/exec/processor` row now states the over-count
  exposure plainly.** In the Coalescer, a **partial** flush failure releases
  the claims of events that also contributed to sibling keys whose writes
  **succeeded**, so redelivery re-applies those events to the successful keys
  and their Sum / Count / TopK values end up above the true count. The
  over-count is bounded by the fan-out of the failing record's key set. The
  same trade applies to `KeyByMany`: at-least-once permits re-application but
  never permits loss, and dedup is a best-effort mitigation layered on top, not
  a stronger guarantee.

#### Monoids and query semantics

- **`DecayedSum` / `DecayedSumBytes` `Combine` is a pure function of its
  operands** — it does not read the wall clock. `BytesStore.MergeUpdate`
  recomputes `Combine` on every CAS retry, so a clock-reading `Combine`
  returned a different answer on each attempt, and the monoid-law fuzzer
  evaluated the two associativity groupings at different instants. The
  future-timestamp skew bound therefore lives at the **lift**, as
  `ClampFuture` / `DefaultSkewBound`, not in the merge path. It cannot be
  derived from `Combine`'s operands: a state dated into the future is
  indistinguishable there from a legitimately idle key, so a pairwise clamp
  would resurrect stale mass. (An intermediate `WithClock` /
  `WithClockSkewBound` pair existed inside this development window and was
  withdrawn before release; neither ever shipped in a tag.)
- **`bloom.NewWithCapacity(n, p)`'s parameters are now enforced as a
  declaration.** `Combine` reports, via `WithDecodeErrorHandler`, both a shape
  clash between operands and operands that agree with each other but not with
  the configured `(m, k)` — the latter is how a `NewWithCapacity(1_000, 0.01)`
  pipeline could aggregate `DefaultCapacity` filters at a false-positive rate
  nothing in the configuration predicted.
- **`bloom.Combine` no longer allocates two discarded ~120 KB bit arrays per
  merge.**
- **`windowed.Config` gains `MaxBuckets`; `grpc.Config` gains
  `CoalesceTimeout`.** Both default from existing fields, so existing configs
  keep working.
- **`pkg/query/typed.TopKItem.Count` is documented as the Misra-Gries lower
  bound it has always been.** The sketch does not record the ingested total, so
  the error bound cannot be derived from what the client returns — carry it out
  of band, e.g. an event counter on the same pipeline.
- **Decayed associativity is documented as approximate (FP) rather than
  exact.**
- **The same pipeline definition runs as either a Kafka ECS worker
  (`streaming.Run`) or a Kinesis Lambda** — both share state via DDB, so
  `examples/recently-interacted-topk/` can ingest from both simultaneously.

#### Dependencies, toolchain and dependency automation

- **Dependabot's open-PR cap was the binding constraint, so security updates
  never arrived.** Only two groups were defined, so every other dependency came
  as its own PR. Both ecosystems sat pinned at their cap (gomod 10/10, npm 5/5)
  for months, which means Dependabot could not open *anything* new — including
  security updates. `GO-2026-6061` and `GO-2026-5841` went unproposed the
  entire time and had to be found by hand. A catch-all minor/patch group per
  ecosystem now keeps the standing PR count at one or two, so the cap stops
  being the thing that matters.
- **`pkg/exec/batch/sparkconnect` was never watched at all.** Dependabot only
  covered `directory: /`, and that submodule carries its own `go.mod` — so its
  `aws-sdk`, `arrow-go` and `spark-connect-go` dependencies had never received
  a single automated update in the repo's history. It now has its own entry.
  Its `replace` for the `pequalsnp/spark-connect-go` fork stays manual;
  Dependabot cannot update a replace target.
- **GitHub Actions are grouped.** `checkout` / `setup-go` / `setup-node`
  arrived as three PRs whose diff hunks overlapped in `ci.yml`, so merging any
  one forced the other two to rebase.
- **Majors stay ungrouped deliberately** — they need individual review, and
  burying one inside a "minor and patch" batch is how a breaking change gets
  merged on the strength of a green checkmark. `@types/node` additionally
  ignores majors: it must track the Node major that actually executes, which is
  the drift this repo already had (types on 25 while CI ran 20).
- **Go dependencies moved to upstream-latest** rather than the open Dependabot
  PR targets, every one of which was already stale. Notable: `franz-go` 1.21.1
  → 1.21.6 (265 commits, concentrated in KIP-848 group rejoin/heartbeat and
  fetch-manager concurrency — treat as a minor), `mongo-driver` 2.6.0 → 2.8.2
  (2.8.1 fixes a write whose retried attempt could return `ErrNoDocuments`
  instead of the real error — silent write loss), `testcontainers-go` 0.42 →
  0.44, and the aws-sdk group forward three minors.
- **`@types/node` 25.x → `^24.13.3`**, deliberately *down* a major to match the
  runtime. Typing against a newer Node than the one that executes lets `tsc`
  accept APIs that do not exist at runtime — the repo was typed against Node 25
  while CI ran Node 20, so the type-checker was two majors ahead of reality.
  Every toolchain package accepts Node 24: vite and `@vitejs/plugin-react` want
  `^20.19.0 || >=22.12.0`, eslint wants `^20.19.0 || ^22.13.0 || >=24`.
- **`actions/checkout`, `actions/setup-go`, `actions/setup-node` v6 → v7.**
  Folded into the Node bump because they touch the same lines and would
  otherwise conflict. Each breaking change was checked against every call site:
  checkout v7 only changes fork-PR behaviour under `pull_request_target` /
  `workflow_run` (this workflow uses neither), setup-go v7 is an ESM migration
  with no input changes, and setup-node v7 drops a dummy `NODE_AUTH_TOKEN`
  export used only for registry publishing, which this workflow does not do.
- **`golangci-lint` pinned to v2.13.1** in CI (was `version: latest`). A
  floating linter makes CI non-reproducible — an untouched commit can go red
  months later because the action pulled a release with new checks. Pinning it
  surfaced one such pre-existing finding, `unparam` on
  `kafka.(*Source).readSerial`, now documented with a `//nolint` explaining why
  the always-nil error result is deliberate signature symmetry with
  `readConcurrent`.
- **The local docker-compose stack moved to the official
  `apache/kafka:3.9.0`** (KRaft-native, unprefixed `KAFKA_*` variables rather
  than Bitnami's `KAFKA_CFG_*`), with single-node replication-factor settings —
  the defaults of 3 leave the internal topics unable to elect a leader and the
  broker never becomes usable. `minio` is pinned too: a floating `:latest` on
  infrastructure images is how a local stack breaks on a day nobody changed
  anything, which is precisely what happened here.

#### Examples, docs and build

- **`examples/recently-interacted-topk`: K resolves once**, through
  `Config.ResolveK()` (default `example.DefaultK` = 32), for every binary, and
  `cmd/query` reads `TOPK_K` instead of hard-coding 32. `topk.DefaultK` is
  unchanged at 10. `Config.Metrics` hands a `metrics.Recorder` to the byte
  store, and both binaries build the recorder before the pipeline so CAS
  contention on that pipeline's single `"global"` row is visible.
  `multisource_test.go` drives the example's real `Build()` instead of a
  hand-rolled copy that had drifted to `K=10` against the deployment's `K=32`,
  and asserts the built sketch's K against `Config.ResolveK()`.
- **The soak runbook's cost table was wrong by 2×** in the direction that
  matters: it omitted the ALB and NAT hourly lines entirely (both bill 24/7
  regardless of throughput) and understated DynamoDB by 7–15× by not accounting
  for TopK's read-plus-two-conditional-writes per event on a single hot row.
  Rewritten as a standing-vs-usage split with the arithmetic shown.
- **`pkg/admin/dist/` and `web/dist/` are now `.gitignore`'d.** Build the UI
  with `make web-build` before `go build ./cmd/murmur-ui`. CI runs both.
- **`recharts` dropped from `web/package.json`** (it was unused; the sparkline
  is a hand-rolled SVG).
- **Mongo `extractID` returns an error for unsupported `_id` BSON types**
  (UUID / Decimal128 explicitly handled) instead of mangling raw bytes into a
  string.
- **`compose.Decayed` gained a `Set` field** so `(0, time.Unix(0, 0))` is no
  longer misclassified as `Identity`.

### Removed

- **`windowed.Config.EventTimeField`** — see **Breaking changes** for the
  migration.
- **The KCL-v3 Kinesis source is off the roadmap.** Lambda is the supported
  production path; KCL v3 Go will not be brought in-tree. The roadmap row in
  `README.md` is gone.

### Fixed

#### Silent event loss

- **A merge that failed after its dedup claim dropped the event forever.**
  `pkg/exec/processor` claimed an EventID via `Deduper.MarkSeen` *before*
  running the merge, and `state.Deduper` had no way to give a claim back. Any
  merge that failed after the claim succeeded — a store outage outlasting the
  retry budget, a Lambda timeout mid-batch, a context cancellation — left the
  EventID marked seen forever. The source's redelivery then hit `dedup_skip`
  and the event was dropped permanently. The failure is silent by
  construction: no error reaches the caller (the record is already "handled"),
  no metric moves, and nothing distinguishes it from a legitimate duplicate.
  For non-idempotent monoids (Sum / HLL / TopK) counts drift downward with no
  signal. The most likely trigger in a Lambda deployment is a timeout: on
  timeout the function returns no `BatchItemFailures` at all, so the whole
  batch is redelivered and every claimed-but-unmerged record in it is already
  invisible. `processor.MergeMany` now releases the claim when a merge fails,
  and only when *this* call won it; the underlying merge error is still what
  reaches the caller, so `BatchItemFailures` and source retry are unaffected.
- **Graceful shutdown acked 1,005 records it never merged.** `streaming.Run`
  treated a cancelled context as a poison record. The comment at the error
  branch said cancellation would "just return; the runtime is exiting anyway",
  but the code fell straight through it into the dead-letter callback **and
  `rec.Ack()`** — telling the source a record had been handled when it had
  never been merged. `Run` then returned `nil`, so the worker logged a clean
  exit and exited 0. `murmur.RunStreamingWorker` wires SIGINT/SIGTERM into that
  context, so this fired on **every ECS deploy, scale-in and task
  replacement** — the ordinary steady state of the very pipeline gating v1,
  needing no misconfiguration and no unusual input. Measured in a regression
  test: a single 40 ms shutdown acked **1,005 records it never merged** at
  concurrency 1, and 249 at concurrency 8. (At the soak's deliberate 1
  event/sec only a record or two is ever in flight; at production rates the
  loss scales with throughput.) Cancellation now returns **without acking**, so
  the source has not advanced and the record is redelivered, and it no longer
  invokes the dead-letter callback — a shutdown is not a poison record, and
  reporting it as one buries real poison records in noise. Emits
  `<pipeline>:shutdown_unacked`; detected with `errors.Is`, since
  `pkg/exec/processor` wraps the cause.
- **The dedup release could not fire during the shutdown it was written for.**
  `processor.MergeMany` released a claim on merge failure using **the same
  context that had just been cancelled**, so the `DeleteItem` failed
  immediately and the claim survived. The most likely reason a merge fails is
  that the context was cancelled, which made the release useless in precisely
  the case it existed to handle: the claim outlived the failed merge, the
  redelivery hit `dedup_skip`, and the event was lost permanently. Release now
  runs on `context.WithoutCancel(ctx)` with a 5 s timeout. The two fixes are
  interdependent: not acking is what causes the redelivery, and releasing the
  claim is what lets that redelivery actually apply.
- **Dedup claims leaked out of the batching write paths.** Both
  `processor.Coalescer` and `streaming.WithBatchWindow` claim an event's ID at
  buffer time — a whole batch, or a whole flush window, before the durable
  write — and both then dropped the buffer without handing the claim back when
  that write failed. The redelivery each failure explicitly asks for arrived to
  a `dedup_skip`: no error, no metric, and for the non-idempotent monoids the
  counts were simply gone. `processor.MergeMany` had been fixed for this; the
  batching paths had not. Covered now on the retry-exhausted branch, the
  cancelled-flush branch, and the aggregator's dead-letter branch. The
  aggregator case was the sharpest, because it acks the poison batch —
  replaying those EventIDs out of the DLQ is the only way to recover them, and
  that replay was the thing being swallowed.
- **Claim ownership is tracked per EventID rather than per batch.**
  `Deduper.MarkSeen` is fail-open, so a dedup-table outage buffers an event
  without owning its claim; releasing on that path would delete a row another
  worker won and let a third delivery re-apply the event over the winner's
  write. Only claims this worker actually won are released.
- **Batched releases run on a context detached from cancellation**, matching
  `MergeMany`. The likeliest reason a flush fails is SIGTERM cancelling the
  context, and releasing with that same context fails immediately — the claim
  would have survived exactly the shutdown it must not survive.
- **The dedup-release budget now scales with the number of claims** instead of
  being a fixed 5 s. `claimedIDs` holds one entry per event, so a failed flush
  of a hot key at `DefaultMaxKeys` handed `ReleaseClaims` 10,000 IDs and the
  fixed budget released fewer than half of them — the rest outlived the deltas
  they were taken for and are lost on redelivery. The budget is now
  `5 s + 20 ms` per worker-pool round (`ceil(n/16)`), capped at 30 s, and the
  releases run across a bounded pool of 16.
- **A shutdown drain no longer pays one release budget per failed batch.** The
  `pkg/exec/streaming` write aggregator called `ReleaseClaims` once per batch
  inside `flushOne`, so a drain across K failed batches paid K independent
  budgets and could outlive the SIGTERM grace period before reaching the last
  one. `flushAll` now gathers every failed batch's claims and makes a single
  bounded release call. The visible trade: a dead-lettered record's claim comes
  back at the end of the drain rather than the instant its own batch failed.
- **The test that covered the detached-context release could not fail.** The
  deduper fakes ignored the context on `Release`, where the real
  `dynamodb.Deduper` honours it. Reverting `context.WithoutCancel(ctx)` to
  `ctx` left the entire suite green — including the test written to cover that
  exact line. The fakes now return `ctx.Err()` when the context is done.

#### DynamoDB state layer

- **A key that had silently stopped updating read as healthy.** `BytesStore`
  now pre-flights DynamoDB's 400KB item limit before `PutItem` and returns a
  typed `ErrItemTooLarge`. Sketch size tracks key length, not just K — a TopK
  that lifts an unbounded wire field into its keys overflows the row on a
  couple of entries — and previously the oversized write failed non-retryably
  on every attempt while `Get` kept serving the last value that fit, as
  `Present:true`.
- **Two pipelines sharing one dedup table starved each other.** Dedup claim
  keys are now namespaced by pipeline name (`"<pipeline>#<EventID>"`). EventIDs
  are only unique within a source, so under the shared-table layout
  `doc/design.md` §13.4 recommends, whichever pipeline claimed an ID first made
  the other skip a merge that never ran. This is the key format §13.4 already
  described.
- **A retried claim could drop a first delivery.** Each `Deduper.MarkSeen` now
  stamps a per-call `claimant` token into the claim row and admits it in the
  condition (`attribute_not_exists(#pk) OR #claimant = :me`). A claim DynamoDB
  had committed whose response was lost to a connection reset previously came
  back as a plain `ConditionalCheckFailedException` on the SDK's retry,
  indistinguishable from a peer's claim. The token works because the SDK's
  retry middleware sits after serialization, so the replay carries the
  identical value.
- **A duplicate key in a batched read failed the whole RPC.** `BatchGetItem`
  rejects a repeated key (`Provided list of item keys contains duplicates`),
  and whether it reproduced depended on the two copies landing in the same
  100-key chunk. `Int64SumStore.GetMany` and `BytesStore.GetMany` now chunk a
  de-duplicated key set and scatter results back through the
  `(entity, bucket)` map they already keep; `Int64MaxStore` inherits the fix.
  `query.WarmupWindowed` / `WarmupNonWindowed` collapse repeated entities
  before fetching (their reported "warmed" count is now distinct entities ×
  buckets).
- **`BatchGetItem` retries `UnprocessedKeys`** with bounded exponential backoff
  and surfaces an error if any key remains unfetched.

#### Query layer

- **`Get` / `GetMany` on a windowed pipeline could never return data.** Both
  RPCs address bucket 0, which is simultaneously the all-time sentinel and the
  epoch bucket, so on a windowed pipeline they reported `present: false` no
  matter how much the pipeline had counted. Four shipped runbooks pointed
  operators at them.
- **Degenerate time bounds returned a fabricated `present: true` zero.**
  `start_unix > end_unix`, bounds left at the proto3 zero, a non-positive
  `duration_seconds`, a `duration_seconds` large enough to overflow the
  nanosecond `time.Duration`, `end_unix = 253402300799`, and the zero
  `time.Time` that `pkg/query/typed` sends as `-62135596800` — the last two
  wrap `UnixNano` and landed on arbitrary negative buckets. All now return
  `INVALID_ARGUMENT`, mirroring the check `pkg/admin` already had.
- **Retention was advisory on the read path.** A window longer than `Retention`
  read TTL-evicted buckets, folded the holes in as the monoid identity, and
  returned the result labelled as a full window. Windowed reads now reject a
  duration that reaches past what retention keeps.
- **Absolute ranges are now held to `Retention` as trailing windows already
  were.** Setting `windowed.Config.MaxBuckets` higher than `Retention` let
  `GetRange`, `GetRangeMany` and `LambdaQuery.GetRange` read past TTL — a year
  against a 7-day `Retention` fanned out over 366 buckets, folded 359 evicted
  ones in as `Identity`, and returned the week's total labelled as a year. The
  bound is on a range's width, not its age; `checkRetention` documents why.
- **A range over exactly the retention window is accepted again.**
  `MaxBucketSpan` derived `ceil(Retention/Granularity)`, but bucket ranges are
  inclusive at both ends, so a range of duration `Retention` touches
  `Retention/Granularity + 1` buckets and was rejected as `InvalidArgument`.
- **Bucket fan-out was unbounded.** With `windowed.Minute(24h)`,
  `GetRange(entity, Unix(0,0), now)` built 29,797,201 keys (~715MB) of which
  all but 1,440 addressed buckets TTL had already evicted. `windowed.Config`
  gains `MaxBuckets`, defaulting to `ceil(Retention/Granularity)`, enforced on
  every path that fans a read across buckets (`GetWindow` / `GetRange` and
  their `Many` forms, `LambdaQuery`, `WarmupWindowed`).
- **The singleflight coalesce key was ambiguous.** Entities were sorted and
  joined with `|`, so `["a|b"]` shared a group with `["a","b"]` and
  `["a|b","c"]` with `["a","b|c"]` — and the shipped codegen `key_template`
  puts a literal `|` inside entity keys. Sorting also merged `["a","b"]` with
  `["b","a"]`, and since responses are positional the second caller received
  the first caller's values against the wrong entities. The key is now
  length-prefixed and order-preserving; permutation coalescing is deliberately
  given up.
- **One client disconnect failed every coalesced peer.** The shared store call
  ran on the context of whichever caller happened to lead the singleflight
  group, so a hang-up cancelled the read for everyone with `CodeInternal` —
  under exactly the concurrent load coalescing exists to serve. The shared work
  now runs detached under a server-side `Config.CoalesceTimeout` (default 10 s)
  with each waiter selecting on its own context.
- **Detaching the shared call must not discard the leader's deadline.**
  Dropping the deadline along with the cancellation meant a burst of abandoned
  requests each held up to `CoalesceTimeout` of fan-out open with nobody left
  to read it; before coalescing existed, a client hangup shed that work
  immediately. The shared context's deadline is now derived from the leader's,
  capped by `CoalesceTimeout`.
- **`pkg/query/typed`'s `SumClient` wrote past its output slice.** It sized the
  slice from the entity list but indexed it by the server's value count, so a
  server returning more values than entities caused an index-out-of-range panic
  inside the calling application. The sketch clients'
  `if i >= len(out) { break }` guard turned the same bug into silent
  truncation. Every batched typed client now rejects a value/entity count
  mismatch. `examples/search-rerank` was fixed the same way.

#### Monoids and sketches

- **Unbounded allocation decoding a malformed TopK sketch.** `topk.decode` read
  the item count `n` straight off the wire and passed it to
  `make([]Item, 0, n)` with no validation. `Item` is 24 bytes, so a corrupt or
  truncated sketch — a partially-written DynamoDB item, a truncated read, bytes
  from a different monoid — decodes `n` as up to 2^32-1 and triggers a **~100
  GB allocation attempt**. The worker OOMs instead of degrading, which defeats
  the entire purpose of `Combine`'s error path: it never gets reached. `keyLen`
  had the same problem, and the key was read with `Reader.Read`, which may
  return a short read without an error and silently truncate the key. Both are
  now bounded by the bytes actually remaining, and the key is read with
  `io.ReadFull`. Rejecting 8 bytes of garbage went from **20ms to 16.5µs** —
  1200× — and the sketch test package dropped from ~35s to 1s. Found by
  investigating why a new test was slow, which is a reminder that "this test is
  oddly slow" is sometimes a bug report.
- **A `compose.Decayed` row could be poisoned by a foreign blob.** The wire
  form is a bare 17 bytes with no magic and no length prefix, so a 200-byte HLL
  sketch decoded to a `Set=true` observation assembled from its first 17 bytes
  and merged into the row as if it were real. `DecodeDecayed` now rejects any
  length but 0 and 17.
- **Bloom `Identity` no longer imposes a shape.** `(m, k)` is written into every
  marshaled filter and read back out of it by `UnmarshalBinary`, so a marshaled
  empty `Identity` was an identity only for filters of its own shape. A
  pipeline built with `bloom.NewWithCapacity` whose value extractor called the
  default-sized `bloom.Single` merged every event into a shape mismatch and the
  stored row stayed empty forever. `Identity` is now the empty slice, which
  `Combine` already short-circuits.
- **Bloom shape mismatches are reported.** A `(m, k)` clash between two real
  filters now fires `WithDecodeErrorHandler` naming both shapes and the
  monoid's own — the case that hook's doc always claimed to cover and never
  did, because mismatched filters decode fine and the merge was abandoned in
  silence.
- **The TopK wire-K mismatch is documented, not yet fixed.** A sketch's header
  carries the K it was written with, and `decode` still discards it, so a
  `topk.New(10)` client merging a saturated K=32 row truncates it to 10
  counters and writes the truncation back. Honouring the operand's K means
  merging at the widest of the three, which changes the header a row is written
  with — a wire-format change, deferred with the ingested-weight field to a
  staged read-both/write-old release. Until then, keep K identical across every
  binary that writes a given state table; `examples/recently-interacted-topk`
  now derives it from one place so the writer and the query server cannot
  disagree.
- **TopK saturation is now pinned by tests, though still not detectable from a
  sketch's bytes.** A summary retaining 0.06% of its stream is
  byte-indistinguishable from an exact one: 32 counters summing to 45,932 over
  32 distinct entities, 29 summing to 29 over 33. Tests document the cliff and
  the heavy-hitter error bound. Making it detectable needs an ingested-weight
  header, which is a wire-format change and is deliberately **not** in this
  release — it has to land as a staged read-both/write-old change so a rolling
  deploy cannot lose live rows.
- **One future-dated event no longer freezes a `DecayedSum` key.** `Combine`
  adopts the newer timestamp as the reference frame, so a four-year skew made
  the frame unreachable: `2^(-4y/24h)` underflows to exactly zero, the
  accumulated mass was annihilated, and every event that followed was itself
  the older operand and was annihilated in turn. The key froze at whatever the
  skewed event carried, and the read path scaled that up by
  `2^(+gap/halfLife)` — `+Inf`, or 7.5e109 for a one-year skew at
  `halfLife=24h` — pinning it to rank #1 permanently. The clamp now happens at
  the lift (`ClampFuture` / `DefaultSkewBound`), and `EvaluateAt` before the
  reference time returns the stored value instead of un-decaying it.
- **`DecayedSum` with `halfLife <= 0` no longer means two different things.**
  `Combine` computed `2^(-dt/0)`: `NaN` at `dt=0`, which round-trips through
  `Encode`/`Decode` and poisons the row forever, and `0` at `dt>0`, which
  silently dropped every older contribution — while `EvaluateAt` on the same
  row reported it undecayed, and a negative half-life made the value grow. Both
  now mean "no decay". Reachable by accident via
  `murmur.Trending(name, cfg.HalfLife)` with an unset `Duration` field.
- **Sketch decode failures were silent.** `hll`, `topk` and `bloom` each
  recover from a `Combine` decode failure by discarding an operand, and the
  affected key just quietly lost cardinality or counts. Bloom is the easiest to
  hit silently: every merged sketch must share the `(m, k)` shape, so one
  caller constructing the monoid with different capacity parameters produces
  filters that fail to decode against each other — and membership answers just
  quietly go wrong. See `WithDecodeErrorHandler` under **Added**.
- **`core.Min` / `core.Max` violated the identity law for negative inputs** —
  fixed by returning `Monoid[Bounded[V]]` (see **Breaking changes**).

#### Lambda and DynamoDB Streams

- **Partial-batch failures named an identifier AWS cannot resolve.** The
  handler reported each failed record's `eventID` as the `BatchItemFailures`
  ItemIdentifier, where Lambda matches against the shard's **sequence
  numbers**. The outcome was one of: the whole batch redelivered (duplicate
  merges — dedup is off by default), a stalled iterator, or the failure
  silently discarded. `pkg/exec/lambda/dynamodbstreams` and
  `examples/search-projector` now report `Change.SequenceNumber`; the `eventID`
  keeps its real job as the dedup key. `pkg/exec/lambda/kinesis` was already
  correct.
- **An empty `SequenceNumber` produced an empty `ItemIdentifier`.** Lambda
  treats a null or empty `itemIdentifier` as a malformed response and
  redelivers the entire batch, re-merging every record that had already
  succeeded. Both the handler and the example now drop the unreportable entry
  and surface it — via `metrics.RecordError` plus a
  `<name>:unreportable_failure` event in the handler, and via a new
  `Stats.Unreportable` counter in the example.

#### Build, local stack and CI

- **Murmur did not compile on Go 1.27** (`undefined: http2.TrailerPrefix`), and
  CI could not see it. `golang.org/x/net` 0.54.0 gates its legacy `http2`
  implementation behind `//go:build !(go1.27 && !http2legacy)` and delegates to
  the standard library instead, but the Go 1.27 wrapper never re-exported the
  `TrailerPrefix` constant. `google.golang.org/grpc` references
  `http2.TrailerPrefix` (at every version through v1.83.2), so any build of
  murmur on Go 1.27 failed in
  `grpc/internal/transport/handler_server.go`. x/net 0.55.0 hoists the constant
  into an ungated `server_common.go`; bumping the pin **0.54.0 → 0.55.0** is
  the whole fix — no gRPC change is involved, and no gRPC bump resolves it.
  Every existing Go job pins `go-version-file: go.mod`, which is why the
  breakage was invisible; see the new latest-toolchain job under **Added**.
- **The local docker-compose stack could not start at all.**
  `docker-compose.yml` pinned `bitnami/kafka:latest`. Bitnami withdrew their
  public Docker Hub catalogue in 2025 and that repository now has **no tags** —
  so `make compose-up` failed with `manifest unknown`, and with it
  `make test-integration` and every way of running `test/e2e` by hand. The
  documented local development workflow in `CONTRIBUTING.md` had been dead for
  some time and nothing surfaced it, because no CI job used this stack.
- **The Mongo replica-set init raced the daemon, silently.** `make compose-up`
  ran `init-mongo-replset.sh` immediately after `docker compose up -d`, but
  `up -d` returns when containers are *created*, not when the daemons inside
  them accept connections — so `rs.initiate()` ran against a still-starting
  mongod and failed. It was invisible because the call was `|| true`, so a
  failed replica-set init was silent and the Mongo CDC tests simply skipped.
  Both the Makefile and the CI job now wait for mongod, then for the set to
  elect a primary.

#### Deploy (Terraform)

- **A `GRPC` target group under an `HTTP` listener could never apply.** ALB
  rejects the pair outright (`InvalidLoadBalancerAction`) — gRPC over ALB
  requires TLS. **No review caught this — only a real `CreateListener` call
  did.** See **Breaking changes** for the `query_certificate_arn` gate.
- **The `query_task` ingress never converged.** It was an inline `ingress`
  block whose `security_groups` went empty when the ALB was disabled. An
  ingress rule with no source is meaningless, AWS will not store it, and
  Terraform therefore re-proposed it on every plan — a configuration that can
  never reach `No changes`. Now a `dynamic` block that is simply absent.
- **Teardown ordering hazard, documented.** When the ALB security group must go
  away, Terraform tries to delete it *before* updating the `query-task` rule
  referencing it, with no dependency edge forcing the other order. It retries
  for 15 minutes and fails with `DependencyViolation`; the rule has to be
  revoked by hand. The underlying fix is separate
  `aws_vpc_security_group_ingress_rule` resources rather than inline blocks.
- These three shipped as claims in PR #72 but were left uncommitted in a
  working tree; only the earlier commit merged, so `main` still shipped the
  broken listener until now.
- **The soak composition could not be planned or applied.** Three independent
  blockers, none of which `terraform validate` can see:
  - **`plan` could not complete at all.** `pipeline-lambda-kinesis` gated two
    `count` arguments on `var.dedup_table_arn != null`, and that ARN comes from
    a sibling module in the same run — unknown at plan time. Terraform aborted
    with *"The count value depends on resource attributes that cannot be
    determined until apply."* The module's own README demonstrated the broken
    pattern and is corrected too.
  - **`AWS_REGION` is a reserved Lambda environment key.** The module injected
    it into every function's environment, so `CreateFunction` rejected the
    centrepiece resource outright. The runtime sets it and the SDK reads it
    automatically. (Unchanged for `pipeline-counter`'s ECS tasks — the
    restriction is Lambda's.)
  - **The default `name` broke ELBv2 validation.** `recently_interacted` flows
    into `aws_lb` / `aws_lb_target_group`, which accept only alphanumerics and
    hyphens. `var.name` doubles as the DynamoDB table name, where underscores
    are legal, so it is sanitized at the ELB sites rather than constrained
    globally, plus a `validation` block on `pipeline-counter`'s `var.name`.
    Root variables are unknown during `terraform validate`, so the provider
    skips its own name checks — a `validation` block is the only form of this
    that a validate-based CI gate can enforce.
- **The ECS liveness alarms alarmed on themselves.** `worker-not-running` and
  `query-not-running` read `ECS/ContainerInsights`, which publishes nothing
  unless Container Insights is enabled on the cluster — and the cluster is
  caller-owned, so the module cannot enable it. With it off the metric does not
  exist, and because these alarms deliberately treat missing data as breaching
  (so a dead service alarms instead of looking healthy), they sat in permanent
  ALARM while both services ran fine. That is worse than no alarm: it trains
  the operator to ignore the two that watch the Kafka half. Now asserted at
  plan time — see **Breaking changes**.

#### Documentation that contradicted the code

- **`STABILITY.md` claimed a sharp edge was closed when it was not.** Sharp
  edge #1 struck through "sources, caches, and sketch `Combine` swallow real
  failures" and declared it closed. It was closed for sources and runtimes, but
  all three sketch monoids still dropped an operand with no error, no metric
  and no log line. A sharp-edges list that wrongly reads "closed" is worse than
  the bug it hides, because it tells readers not to look. It now states plainly
  what is closed, what remains, and that properly closing it means giving
  `Monoid.Combine` an error return — a v1-scoped decision affecting every
  implementation and call site.
- **`doc/design.md` §15 attributed its performance numbers to measurements that
  do not exist.** It claimed they came from "the docker-compose integration
  suite (`test/e2e/`)" and "production-shape micro-benchmarks against
  DDB-local". Both are false: `test/e2e/` holds correctness assertions with no
  `b.N` anywhere, and no benchmark in the repo touches DDB-local. The four that
  exist run against in-memory fakes — the headline "10× speedup at N=16" is
  goroutines contending on a `sync/atomic` slot array with a `time.Sleep`
  standing in for store latency. §15 now opens with an explicit provenance
  note, and the same caveat is applied to the throughput claim in `README.md`
  and the 10× claim in `STABILITY.md`. Measured numbers become a deliverable of
  the v1 soak.
- **`doc/design.md` §6.2 taught the `eventID` bug.** It claimed the
  `BatchItemFailures` `ItemIdentifier` is the `eventID` for DynamoDB Streams;
  it is the stream record's `SequenceNumber`, which is what Lambda resolves
  against the shard's checkpoint. Replaced with a per-source table and an
  explanation of why the two identifiers are not interchangeable (`eventID`
  feeds the `Deduper`; the sequence number is the cursor). The copy-pasteable
  Lambda handler in `doc/search-integration.md` had the same defect in code
  form and now reports `rec.Change.SequenceNumber`.
- **`WithBatchWindow`'s crash-safety documentation was wrong.** The
  `pkg/exec/streaming` option doc, `doc/design.md` §5.4, §14.1, §14.4 and the
  failure-mode diagram all asserted crash safety on the premise that the dedup
  claim is taken at flush time. It is taken in `aggregator.accept`, when the
  record enters the accumulator. The consequence — with `WithDedup`, a crash
  before the flush loses the accumulated batch, because the surviving claim
  suppresses the very redelivery that would restore it; without a `Deduper`,
  the redelivery is the records' first apply and nothing is lost — is now
  stated plainly, along with the contrast to `processor.MergeOne`'s
  release-on-failed-merge.
- **`replay.WithDedup` promised more than it delivers.** Re-running an archive
  folds idempotently only inside the deduper's TTL horizon: claims expire, and
  an archive replayed after they do is indistinguishable from new data and
  merges a second time. Documented on `replay.WithDedup`, the `replay` package,
  and `dynamodb.NewDeduper`, and pinned by tests.
- **The `Trending` decay clock is processing-time, not event time.** `Clock` is
  a `func() time.Time` and never sees the event, so a replayed archive scores
  as fresh. The docs said otherwise.
- **`STABILITY.md`'s `pkg/metrics` row was factually wrong**, claiming "only
  `streaming.Run` is wired today; bootstrap / replay / sources are not."
  `Recorder` has been wired through `pkg/exec/processor` — and therefore
  through bootstrap, replay, and every Lambda handler — since the processor
  consolidation. Sources genuinely aren't instrumented; the row now says so
  precisely.
- **`README.md`'s Status paragraph contradicted its own feature table** and
  this changelog, saying `get_many` / `get_range` were "Sum-only until
  `pkg/query/typed` grows the matching methods." That gate was lifted during
  the typed-client parity work.
- **The v1 release criteria were unfollowable.** "`v1.0.0` will ship after PR
  1–4 land" had three incompatible readings in-repo, and under every
  non-literal one all four had already landed. Replaced with a checkable
  four-part list naming the soak target, the promotions it unblocks, the five
  code blockers it does *not* fix (with the `Build()` → `Validate()` rename
  flagged as the only one with a hard deadline), and the release-engineering
  gap.
- **`recently-interacted-topk` built a K the query server could not read.**
  `Config{K: 0}` resolved to `topk.DefaultK` (10) while the Config doc, both
  writers' `TOPK_K` defaults, and the query server all said 32. Mismatched-K
  Misra-Gries sketches refuse to merge, so the symptom is an empty or stale
  Top-N rather than an error. See **Changed → Examples, docs and build**.

## [0.1.0] - 2026-05-12

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

[Unreleased]: https://github.com/gallowaysoftware/murmur/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/gallowaysoftware/murmur/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/gallowaysoftware/murmur/releases/tag/v0.1.0
