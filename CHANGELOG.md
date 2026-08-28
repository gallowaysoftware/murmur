# Changelog

All notable changes to Murmur are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed — DynamoDB state-layer limits and dedup key scoping

- `pkg/state/dynamodb`: `BytesStore` now pre-flights DynamoDB's 400KB item
  limit before `PutItem` and returns a typed `ErrItemTooLarge` (with
  `*ItemTooLargeError` carrying the key and measured size). Sketch size tracks
  key length, not just K — a TopK that lifts an unbounded wire field into its
  keys overflows the row on a couple of entries — and previously the oversized
  write failed non-retryably on every attempt while `Get` kept serving the last
  value that fit, as `Present:true`. A key that had silently stopped updating
  read as healthy.
- `pkg/state/dynamodb`: dedup claim keys are namespaced by pipeline name
  (`"<pipeline>#<EventID>"`). EventIDs are only unique within a source, so two
  pipelines sharing one dedup table — the layout `doc/design.md` §13.4
  recommends — starved each other: whichever claimed an ID first made the other
  skip a merge that never ran. This is the key format §13.4 already described.
- `pkg/state/dynamodb`: each `Deduper.MarkSeen` stamps a per-call `claimant`
  token into the claim row and admits it in the condition
  (`attribute_not_exists(#pk) OR #claimant = :me`). A claim DynamoDB had
  committed whose response was lost to a connection reset previously came back
  as a plain `ConditionalCheckFailedException` on the SDK's retry,
  indistinguishable from a peer's claim, and dropped a first delivery. The
  token works because the SDK's retry middleware sits after serialization, so
  the replay carries the identical value.

#### Added alongside

- `pkg/state/dynamodb`: `BytesStore` CAS retries are configurable via
  `WithCASRetries` and `WithCASBackoff` (previously 8 attempts and ~17s of
  backoff, hardcoded with no setter), and contention is counted under
  `<pipeline>:cas_conflict` via `WithCASMetrics` — a hot key used to burn its
  whole budget and dead-letter with nothing in the metrics to say the writes
  were racing rather than failing. `ErrMaxRetriesExceeded` now carries the
  table, key and attempt count.
- `pkg/state/dynamodb`: `(*Deduper).ForPipeline` derives a sibling scope over
  the same table for a worker process hosting several pipelines.

#### Changed alongside

- **BREAKING** `pkg/state/dynamodb.NewDeduper` takes a pipeline name:
  `NewDeduper(client, table, pipeline, ttl)`. This changes the on-disk dedup
  key format. An operator with an existing dedup table gets one window of
  re-processing as previously-claimed IDs re-claim under their namespaced keys;
  for non-idempotent monoids (Sum, HLL, TopK), drain in-flight records before
  deploying, or let the old rows age out via the table's TTL first. No table
  schema change — the partition key attribute is unchanged, only its contents.
- **BREAKING** `pkg/state/dynamodb.NewBytesStore` takes variadic
  `BytesStoreOption`s. Existing three-argument calls compile unchanged.
- `examples/recently-interacted-topk`: `Config.Metrics` hands a
  `metrics.Recorder` to the byte store, and both binaries build the recorder
  before the pipeline so CAS contention on that pipeline's single `"global"`
  row is visible.

### Fixed

- **Dedup claims no longer leak out of the batching write paths.** Both
  `processor.Coalescer` and `streaming.WithBatchWindow` claim an event's ID at
  buffer time — a whole batch, or a whole flush window, before the durable write
  — and both then dropped the buffer without handing the claim back when that
  write failed. The redelivery each failure explicitly asks for arrived to a
  `dedup_skip`: no error, no metric, and for the non-idempotent monoids (Sum,
  HLL, TopK) the counts were simply gone. `processor.MergeMany` was fixed for
  this previously; the batching paths were not. Covered now on the
  retry-exhausted branch, the cancelled-flush branch, and the aggregator's
  dead-letter branch. The aggregator case was the sharpest, because it acks the
  poison batch — replaying those EventIDs out of the DLQ is the only way to
  recover them, and that replay was the thing being swallowed.
- Claim ownership is tracked **per EventID** rather than per batch.
  `Deduper.MarkSeen` is fail-open, so a dedup-table outage buffers an event
  without owning its claim; releasing on that path would delete a row another
  worker won and let a third delivery re-apply the event over the winner's
  write. Only claims this worker actually won are released.
- Batched releases run on a context detached from cancellation, matching
  `MergeMany`. The likeliest reason a flush fails is SIGTERM cancelling the
  context, and releasing with that same context fails immediately — the claim
  would have survived exactly the shutdown it must not survive.

### Added

- `processor.ReleaseClaims(ctx, cfg, pipelineName, eventIDs)` — the shared
  detached-context release path behind `MergeMany`, `Coalescer.Flush` and the
  streaming aggregator. Out-of-tree drivers that claim EventIDs at buffer time
  should call it on their failure path. One time budget spans the whole slice so
  a large failed batch cannot stall shutdown; anything it cannot cover is
  reported as `dedup_release_failed` instead of going out silently.

### Fixed

- `pkg/exec/processor`: the dedup-release budget now scales with the number of
  claims instead of being a fixed 5 s. `claimedIDs` holds one entry per event, so
  a failed flush of a hot key at `DefaultMaxKeys` handed `ReleaseClaims` 10,000
  IDs and the fixed budget released fewer than half of them — the rest outlived
  the deltas they were taken for and are lost on redelivery. The budget is now
  `5 s + 20 ms` per worker-pool round (`ceil(n/16)`), capped at 30 s, and the
  releases run across a bounded pool of 16.
- `pkg/exec/streaming`: the write aggregator called `ReleaseClaims` once per
  batch inside `flushOne`, so a shutdown drain across K failed batches paid K
  independent release budgets and could outlive the SIGTERM grace period before
  reaching the last one. `flushAll` now gathers every failed batch's claims and
  makes a single bounded release call. The visible trade: a dead-lettered
  record's claim comes back at the end of the drain rather than the instant its
  own batch failed.

### Changed

- `STABILITY.md`: the `pkg/exec/processor` row now states the over-count exposure
  plainly. In the Coalescer, a **partial** flush failure releases the claims of
  events that also contributed to sibling keys whose writes **succeeded**, so
  redelivery re-applies those events to the successful keys and their
  Sum / Count / TopK values end up above the true count. The over-count is
  bounded by the fan-out of the failing record's key set.

### Testing

- The deduper fakes now honour the context on `Release` (returning `ctx.Err()`
  when it is done), the way the real `dynamodb.Deduper` does. They previously
  ignored it, which made the detached-context release in `ReleaseClaims`
  untestable: reverting `context.WithoutCancel(ctx)` to `ctx` left the entire
  suite green, including the test written to cover it.
- New coverage: release at `DefaultMaxKeys` scale, budget scaling in isolation,
  a mixed-outcome flush (one key lands, a sibling fails, disjoint events) whose
  replay proves the surviving key is not double-counted, a cancellation landing
  on an in-flight aggregator flush, and a shutdown drain spending exactly one
  release budget.

- **Bloom `Identity` no longer imposes a shape.** `(m, k)` is written into every
  marshaled filter and read back out of it by `UnmarshalBinary`, so a marshaled
  empty `Identity` was an identity only for filters of its own shape. A pipeline
  built with `bloom.NewWithCapacity` whose value extractor called the
  default-sized `bloom.Single` merged every event into a shape mismatch and the
  stored row stayed empty forever. `Identity` is now the empty slice, which
  `Combine` already short-circuits.
- **Bloom shape mismatches are reported.** A `(m, k)` clash between two real
  filters now fires `WithDecodeErrorHandler` naming both shapes and the monoid's
  own — the case that hook's doc always claimed to cover and never did, because
  mismatched filters decode fine and the merge was abandoned in silence.
- **TopK honours the wire K.** The K in a sketch's header was decoded and
  discarded, so a `topk.New(10)` client merging a saturated K=32 row truncated it
  to 10 counters and wrote the truncation back. `Combine` now merges at the
  widest K of the monoid and both operands (`max` is associative, so merge order
  still cannot change the result) and reports the mismatch through
  `WithDecodeErrorHandler`. A K adopted from the wire is capped, so a corrupt
  header cannot switch off eviction and let a row grow until the store refuses it.
- **TopK saturation is visible.** A summary retaining 0.06% of its stream was
  byte-indistinguishable from an exact one: 32 counters summing to 45,932 over 32
  distinct entities, 29 summing to 29 over 33. The header now carries total
  ingested weight as a plain associative `uint64` sum, and the new `topk.Inspect`
  returns a `Summary` with `Ingested` / `Retained` / `Coverage` / `Saturated` /
  `MaxError`. Discarded mass is deliberately not accumulated — it is
  merge-order-dependent and would break associativity.
- **One future-dated event no longer freezes a `DecayedSum` key.** `Combine`
  adopts the newer timestamp as the reference frame, so a four-year skew made the
  frame unreachable: `2^(-4y/24h)` underflows to exactly zero, the accumulated
  mass was annihilated, and every event that followed was itself the older operand
  and was annihilated in turn. The key froze at whatever the skewed event carried,
  and the read path scaled that up by `2^(+gap/halfLife)` — `+Inf`, or 7.5e109 for
  a one-year skew at `halfLife=24h` — pinning it to rank #1 permanently. `Combine`
  now bounds the reference frame against the wall clock (`WithClockSkewBound`,
  two half-lives by default; `WithClock` to override the clock), and `EvaluateAt`
  before the reference time returns the stored value instead of un-decaying it.
- **`DecayedSum` with `halfLife <= 0` no longer means two different things.**
  `Combine` computed `2^(-dt/0)`: `NaN` at `dt=0`, which round-trips through
  `Encode`/`Decode` and poisons the row forever, and `0` at `dt>0`, which silently
  dropped every older contribution — while `EvaluateAt` on the same row reported
  it undecayed, and a negative half-life made the value grow. Both now mean "no
  decay". Reachable by accident via `murmur.Trending(name, cfg.HalfLife)` with an
  unset `Duration` field.
- **`compose` gained a decode-error handler.** The `Decayed` wire form is a bare
  17 bytes with no magic and no length prefix, so a 200-byte HLL sketch decoded to
  a `Set=true` observation assembled from its first 17 bytes and merged into the
  row as if it were real. `DecodeDecayed` now rejects any length but 0 and 17, and
  `DecayedSumBytes` takes `WithDecodeErrorHandler` like `hll` / `topk` / `bloom`.

### Changed

- `pkg/query/typed.TopKItem.Count` is documented as the Misra-Gries lower bound it
  has always been, pointing at `topk.Inspect` for the error bound and coverage.
- `monoidlaws` now fuzzes non-default capacities: a Bloom monoid whose `(m, k)`
  differs from its operands', and a K=4 TopK monoid over K=32 sketches.

### Breaking (pre-1.0)

- The TopK wire format gains a flagged `uint64` ingested-weight header field.
  Rows written by older binaries still decode (flagged `PartialWeight`); rows
  written by this version do **not** decode correctly on older binaries.
- `bloom` `Identity()` returns an empty slice rather than a marshaled empty filter.
- `compose.DecodeDecayed` returns `(Decayed, error)`.
- `compose.EvaluateAt` no longer scales a value up when evaluated before its
  reference time; it returns the stored value.
- `compose.DecayedSum` / `DecayedSumBytes` and `bloom.Bloom` take variadic options.

### Changed

- `pkg/monoid/compose`: `DecayedSum` / `DecayedSumBytes` `Combine` is a pure function of its operands again — it no longer reads the wall clock. `BytesStore.MergeUpdate` recomputes `Combine` on every CAS retry, so a clock-reading `Combine` returned a different answer on each attempt, and the monoid-law fuzzer evaluated the two associativity groupings at different instants. **Breaking:** `WithClock` and `WithClockSkewBound` are removed.
- `pkg/monoid/compose`: the future-timestamp skew bound moves from the merge path to the lift, as `ClampFuture(t, now, bound)` and `DefaultSkewBound(halfLife)`. Use it in any pipeline whose value extractor takes the timestamp from the event rather than from the clock; `murmur.Trending` stamps at its own clock and is unaffected. The bound cannot be derived from `Combine`'s operands: a state dated into the future is indistinguishable there from a legitimately idle key, so a pairwise clamp would resurrect stale mass.
- `pkg/monoid/compose`: a non-positive half-life now means "no decay" in `Combine` as well as `EvaluateAt`. It previously computed `2^(-dt/0)` — NaN at `dt=0`, which round-trips through the wire format and poisons the row permanently, and `0` at `dt>0`, which silently dropped every older contribution.
- `pkg/monoid/compose`: `EvaluateAt` at a time before the reference timestamp returns the stored value instead of scaling it up. Un-decaying is an unbounded over-estimate (`+Inf` for a four-year-ahead row at `halfLife=24h`).
- **Breaking:** `pkg/monoid/compose.DecodeDecayed` returns `(Decayed, error)`. Any length other than 0 or 17 is now an error rather than a `Set=true` value assembled from a foreign blob's first 17 bytes.
- `pkg/monoid/sketch/bloom`: `Identity` is the empty slice rather than a marshaled empty filter, so it is an identity for an operand of any shape. A monoid built with `NewWithCapacity` whose extractor called the default-sized `Single` previously merged every event into a shape mismatch and left the row empty forever.
- `pkg/monoid/sketch/bloom`: `NewWithCapacity(n, p)`'s parameters are now enforced as a declaration. `Combine` reports, via `WithDecodeErrorHandler`, both a shape clash between operands and operands that agree with each other but not with the configured `(m, k)` — the latter is how a `NewWithCapacity(1_000, 0.01)` pipeline could aggregate `DefaultCapacity` filters at a false-positive rate nothing in the configuration predicted.
- `pkg/monoid/sketch/bloom`: `Combine` no longer allocates two discarded ~120 KB bit arrays per merge.

### Added

- `pkg/monoid/compose.ClampFuture` and `pkg/monoid/compose.DefaultSkewBound` (experimental).
- `pkg/monoid/sketch/topk`: saturation tests documenting that a `K=32` summary drops from 32 counters covering 45,932 events to 29 counters covering 29 when a 33rd entity appears, and that the counts are Misra-Gries lower bounds with an `n/(K+1)` error bar that callers must size from an `n` the sketch does not record.

### Fixed — graceful shutdown silently lost every in-flight record

`streaming.Run` treated a cancelled context as a poison record. The comment at
the error branch said cancellation would "just return; the runtime is exiting
anyway", but the code fell straight through it into the dead-letter callback
**and `rec.Ack()`** — telling the source a record had been handled when it had
never been merged. `Run` then returned `nil`, so the worker logged a clean exit
and exited 0.

`murmur.RunStreamingWorker` wires SIGINT/SIGTERM into that context, so this
fired on **every ECS deploy, scale-in and task replacement** — the ordinary
steady state of the very pipeline gating v1, needing no misconfiguration and no
unusual input. Measured in a regression test: a single 40 ms shutdown acked
**1,005 records it never merged** at concurrency 1, and 249 at concurrency 8.
(At the soak's deliberate 1 event/sec only a record or two is ever in flight;
at production rates the loss scales with throughput.)

- Cancellation now returns **without acking**, so the source has not advanced
  and the record is redelivered. It also no longer invokes the dead-letter
  callback — a shutdown is not a poison record, and reporting it as one buries
  real poison records in noise. Emits `<pipeline>:shutdown_unacked`.
- Detected with `errors.Is`, since `pkg/exec/processor` wraps the cause.

### Fixed — the dedup release could not fire during the shutdown it was written for

`processor.MergeMany` released a dedup claim on merge failure using **the same
context that had just been cancelled**, so the `DeleteItem` failed immediately
and the claim survived. The most likely reason a merge fails is that the
context was cancelled, which made the release useless in precisely the case it
existed to handle: the claim outlived the failed merge, the redelivery hit
`dedup_skip`, and the event was lost permanently.

Release now runs on `context.WithoutCancel(ctx)` with a 5s timeout. The two
fixes are interdependent: not acking is what causes the redelivery, and
releasing the claim is what lets that redelivery actually apply.

### Changed — Dependabot: catch-all groups, and the sparkconnect submodule is finally covered

Two concrete failures drove this.

**The open-PR cap was the binding constraint, so security updates never
arrived.** Only two groups were defined, so every other dependency came as its
own PR. Both ecosystems sat pinned at their cap (gomod 10/10, npm 5/5) for
months, which means Dependabot could not open *anything* new — including
security updates. `GO-2026-6061` (gRPC HTTP/2 server) and `GO-2026-5841`
(`klauspost/compress/s2`, in the Kafka decompression path) went unproposed the
entire time and had to be found by hand. A catch-all minor/patch group per
ecosystem keeps the standing PR count at one or two, so the cap stops being
the thing that matters.

**`pkg/exec/batch/sparkconnect` was never watched at all.** Dependabot only
covered `directory: /`, and that submodule carries its own `go.mod` — so its
`aws-sdk`, `arrow-go` and `spark-connect-go` dependencies had never received a
single automated update in the repo's history. Now has its own entry. Its
`replace` for the `pequalsnp/spark-connect-go` fork stays manual; Dependabot
cannot update a replace target.

**GitHub Actions are grouped.** `checkout` / `setup-go` / `setup-node` arrived
as three PRs whose diff hunks overlapped in `ci.yml`, so merging any one forced
the other two to rebase.

Majors stay ungrouped deliberately — they need individual review, and burying
one inside a "minor and patch" batch is how a breaking change gets merged on
the strength of a green checkmark. `@types/node` additionally ignores majors:
it must track the Node major that actually executes, which is the drift this
repo already had (types on 25 while CI ran 20).

### Fixed — the local docker-compose stack could not start at all

`docker-compose.yml` pinned `bitnami/kafka:latest`. Bitnami withdrew their
public Docker Hub catalogue in 2025 and that repository now has **no tags** —
so `make compose-up` failed with `manifest unknown`, and with it
`make test-integration` and every way of running `test/e2e` by hand. The
documented local development workflow in `CONTRIBUTING.md` had been dead for
some time and nothing surfaced it, because no CI job used this stack.

- Switched to the official **`apache/kafka:3.9.0`**, which is KRaft-native and
  uses unprefixed `KAFKA_*` variables rather than Bitnami's `KAFKA_CFG_*`.
  Added the single-node replication-factor settings — the defaults of 3 leave
  the internal topics unable to elect a leader and the broker never becomes
  usable.
- Pinned `minio` too. A floating `:latest` on infrastructure images is how a
  local stack breaks on a day nobody changed anything, which is precisely what
  happened here.

### Added — CI runs the `test/e2e` suite

The nine-file library-shape suite (counter, HLL, windowed, Mongo bootstrap,
Mongo CDC, DDB bootstrap, S3 replay, S3 Parquet replay, Kafka concurrency) had
**never run in CI**. `README.md` advertises it and `doc/design.md` stated "The
CI runs them on every PR" — which was false; it ran only via
`make test-integration`, which as of the entry above did not work either.

The new `Library-shape E2E (test/e2e)` job stands up the compose stack and runs
it. Getting it green surfaced a third bug: `make compose-up` ran
`init-mongo-replset.sh` immediately after `docker compose up -d`, but `up -d`
returns when containers are *created*, not when the daemons inside them accept
connections — so `rs.initiate()` ran against a still-starting mongod and
failed. It was invisible because the call was `|| true`, so a failed replica-set
init was silent and the Mongo CDC tests simply skipped. Both the Makefile and
the CI job now wait for mongod, then for the set to elect a primary. It also **fails on a skip**: these tests gate themselves on infra env vars,
so "ran nothing" and "everything passed" are the same exit code — exactly the
property that let the suite rot unnoticed. `scripts/assert-tests-ran.py`
enforces it.

This complements the existing `integration` job rather than duplicating it:
that one exercises the *deployed* shape (built images run as containers), this
one the *library* shape (murmur as a Go package against real infra).

### Fixed — unbounded allocation decoding a malformed TopK sketch

`topk.decode` read the item count `n` straight off the wire and passed it to
`make([]Item, 0, n)` with no validation. `Item` is 24 bytes, so a corrupt or
truncated sketch — a partially-written DynamoDB item, a truncated read, bytes
from a different monoid — decodes `n` as up to 2^32-1 and triggers a **~100 GB
allocation attempt**. The worker OOMs instead of degrading, which defeats the
entire purpose of `Combine`'s error path: it never gets reached.

`keyLen` had the same problem, and the key was read with `Reader.Read`, which
may return a short read without an error and silently truncate the key.

Both are now bounded by the bytes actually remaining, and the key is read with
`io.ReadFull`. Rejecting 8 bytes of garbage went from **20ms to 16.5µs** —
1200× — and the sketch test package dropped from ~35s to 1s.

Found by investigating why a new test was slow, which is a reminder that
"this test is oddly slow" is sometimes a bug report.

### Added — sketch decode failures are reportable

`hll`, `topk` and `bloom` each recover from a `Combine` decode failure by
returning whichever operand decoded and discarding the other. `Monoid.Combine`
is `Combine(a, b) V` with no error return, so that recovery is the only option
short of corrupting merged state or panicking a worker mid-batch — but it was
**silent**, and the affected key just quietly lost cardinality or counts.

- All three constructors now take `WithDecodeErrorHandler(func(error))`.
  Errors name the sketch and which operand failed, and wrap the underlying
  cause so callers can `errors.Unwrap` rather than string-match. Variadic, so
  no existing call site changes.
- Bloom is the easiest to hit silently: every merged sketch must share the
  `(m, k)` shape, so one caller constructing the monoid with different
  capacity parameters produces filters that fail to decode against each other
  — and membership answers just quietly go wrong.

### Fixed — STABILITY.md claimed a sharp edge was closed when it was not

Sharp edge #1 struck through "sources, caches, and sketch `Combine` swallow
real failures" and declared it closed. It was closed for sources and runtimes,
but all three sketch monoids still dropped an operand with no error, no metric
and no log line. A sharp-edges list that wrongly reads "closed" is worse than
the bug it hides, because it tells readers not to look. Now states plainly
what is closed, what remains, and that properly closing it means giving
`Monoid.Combine` an error return — a v1-scoped decision affecting every
implementation and call site.

### Added — real health endpoints on the query server

Nothing in the tree implemented `grpc.health.v1.Health`, yet
`pipeline-counter` provisioned an ALB target group probing
`/grpc.health.v1.Health/Check`. That probe "passed" only because ALB read the
gRPC `UNIMPLEMENTED` status (12) and the matcher was a permissive `0-99`. It
proved the port was open and nothing else — a task that could not reach
DynamoDB stayed healthy and kept receiving traffic.

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
- **Readiness results are cached** (`DefaultHealthCacheTTL`, 10s). An ALB
  probes every 15s per target and Kubernetes often tighter, so an uncached
  probe turns health checking into steady billed reads and couples probe
  latency to store latency — a slow-but-working table would start failing
  health checks. `WithHealthCacheTTL`, `WithHealthProbe` and
  `WithHealthSentinelKey` override the defaults.
- **The Terraform matchers are correspondingly tightened**, `0-99` → `0` and
  `200-499` → `200`, and the HTTP1 path moves from `/` to `/readyz`. The old
  values passed on gRPC `UNIMPLEMENTED` and on an HTTP 404.

### Added — CloudWatch EMF metrics recorder

**`pkg/metrics/emf`** implements `metrics.Recorder` on top of the CloudWatch
Embedded Metric Format.

This closes a real observability hole rather than adding a nicety. Both
example binaries used `metrics.NewInMemory()` — an in-process map that nothing
ever read. Every in-pipeline signal (`dedup_skip`, `dedup_release`,
`dedup_release_failed`, decode errors, retry counts, store latency) was
therefore invisible, while the CloudWatch alarms could only see Lambda's
`Errors` / `Throttles` / `IteratorAge` — none of which move when records are
silently dropped or deduplicated. A pipeline could discard records for a whole
quarter with every alarm green.

- **No new IAM or SDK client.** EMF metrics are extracted from structured JSON
  on stdout, which Lambda forwards natively and ECS forwards via the awslogs
  driver.
- **Aggregated, not per-event.** One EMF document per pipeline per flush
  interval (default 60s), carrying counters as sums and latencies as EMF
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
  flushes per invocation — Lambda freezes the execution environment on return,
  so a background ticker is not guaranteed to fire and a low-traffic function
  would otherwise emit nothing at all.

### Changed — Node 24 toolchain

- **Node 20 → 24 (active LTS)** in CI, and now pinned in one more place than
  before so the three cannot drift apart: `.nvmrc` (new), `engines` in
  `web/package.json` (new), and `node-version` in `ci.yml`.
- **`@types/node` 25.x → `^24.13.3`**, deliberately *down* a major to match
  the runtime. Typing against a newer Node than the one that executes lets
  `tsc` accept APIs that do not exist at runtime — the repo was typed against
  Node 25 while CI ran Node 20, so the type-checker was two majors ahead of
  reality. Node 20 also passed end-of-life.
- Every toolchain package accepts Node 24: vite and `@vitejs/plugin-react`
  want `^20.19.0 || >=22.12.0`, eslint wants `^20.19.0 || ^22.13.0 || >=24`.
- **`actions/checkout`, `actions/setup-go`, `actions/setup-node` v6 → v7.**
  Folded in here because they touch the same lines as the Node bump and would
  otherwise conflict — and because they arrived as three separate Dependabot
  PRs whose diff hunks overlapped each other. Checked each breaking change
  against every call site: checkout v7 only changes fork-PR behaviour under
  `pull_request_target` / `workflow_run` (this workflow uses neither),
  setup-go v7 is an ESM migration with no input changes, and setup-node v7
  drops a dummy `NODE_AUTH_TOKEN` export used only for registry publishing,
  which this workflow does not do.

### Fixed — three more soak blockers, found by actually applying

PR #72 claimed these but they were left uncommitted in a working tree; only
the earlier commit merged. `main` still shipped the broken listener until now.

- **`GRPC` target group under an `HTTP` listener could never apply.** ALB
  rejects the pair outright (`InvalidLoadBalancerAction`) — gRPC over ALB
  requires TLS. `protocol_version` is now gated on a new
  `query_certificate_arn`: with a certificate the listener is HTTPS and the
  target group GRPC; without one it falls back to HTTP1, which still serves
  Connect's HTTP+JSON on the same port, since `pkg/query/grpc` speaks gRPC,
  gRPC-Web and Connect from one handler. **No review caught this — only a
  real `CreateListener` call did.**
- **`query_alb_enabled`** (default `true`, unchanged behaviour) makes the ALB
  optional. It is a ~$17-22/mo standing charge that exists only to front the
  query service, which a soak does not need.
- **The `query_task` ingress never converged.** It was an inline `ingress`
  block whose `security_groups` went empty when the ALB was disabled. An
  ingress rule with no source is meaningless, AWS will not store it, and
  Terraform therefore re-proposed it on every plan — a configuration that can
  never reach `No changes`. Now a `dynamic` block that is simply absent.
- **Teardown ordering hazard, documented.** When the ALB security group must
  go away, Terraform tries to delete it *before* updating the `query-task`
  rule referencing it, with no dependency edge forcing the other order. It
  retries for 15 minutes and fails with `DependencyViolation`; the rule has
  to be revoked by hand. The underlying fix is separate
  `aws_vpc_security_group_ingress_rule` resources rather than inline blocks.

### Fixed — the ECS liveness alarms alarmed on themselves

`worker-not-running` and `query-not-running` read `ECS/ContainerInsights`,
which publishes nothing unless Container Insights is enabled on the cluster —
and the cluster is caller-owned, so the module cannot enable it. With it off
the metric does not exist, and because these alarms deliberately treat missing
data as breaching (so a dead service alarms instead of looking healthy), they
sat in permanent ALARM while both services ran fine. That is worse than no
alarm: it trains the operator to ignore the two that watch the Kafka half.

Now asserted at plan time via a `postcondition` on a new
`aws_ecs_cluster` data source, with the enabling command in the error message
and a `require_container_insights` escape hatch.

### Security

#### Dependency refresh (round 3) — closes two Go advisories and seven npm ones

Replaces the stale PR #52 bundle, which had drifted three months and would
have landed grpc *inside* an advisory range.

- **`GO-2026-6061`** — vulnerabilities in gRPC's xDS RBAC engine and HTTP/2
  transport server. Affects `google.golang.org/grpc` below **1.82.1**; `main`
  was on 1.81.0 and PR #52 moved it only to 1.81.1, still inside the range.
  Now on **1.83.2**. The server half is the one that matters here: it is the
  serving path `pkg/query/grpc` runs, and a resource-exhaustion bug there is
  exactly what goes unnoticed in an unattended deployment.
- **`GO-2026-5841`** — out-of-bounds read in `klauspost/compress/s2`. Fixed in
  1.18.7; both #52 and Dependabot #64 land 1.18.6, one patch short. Bumped
  explicitly to **1.19.2**. It is an `// indirect` dependency, so no
  Dependabot PR will ever propose it on its own — it moves only when
  franz-go's requirement moves, and franz-go doesn't force it past 1.18.6.
  It sits in the Kafka record-decompression path, parsing broker-supplied
  bytes.
- **npm: 7 advisories → 0** (5 high). The one that counts is `react-router`,
  a *runtime* dependency shipped inside the embedded admin UI, carrying five
  high advisories including an open redirect and an XSS. Also cleared:
  `vite`, `postcss`, `nanoid`, `brace-expansion`, `@babel/core`.

Unlike #52, the npm `^` ranges are preserved rather than silently converted
to exact pins.

### Changed

- Go dependencies moved to upstream-latest rather than the open Dependabot
  PR targets, every one of which was already stale — Dependabot has been
  saturated at both open-PR limits (gomod 10/10, npm 5/5) for months and so
  has been unable to open anything new, including security bumps.
  Notable: `franz-go` 1.21.1 → 1.21.6 (265 commits, concentrated in KIP-848
  group rejoin/heartbeat and fetch-manager concurrency — treat as a minor),
  `mongo-driver` 2.6.0 → 2.8.2 (2.8.1 fixes a write whose retried attempt
  could return `ErrNoDocuments` instead of the real error — silent write
  loss), `testcontainers-go` 0.42 → 0.44, and the aws-sdk group forward
  three minors.

### Fixed

#### Build failure on Go 1.27 (`undefined: http2.TrailerPrefix`)

- **`golang.org/x/net` 0.54.0 → 0.55.0.** Murmur did not compile on Go 1.27.
  x/net 0.54.0 gates its legacy `http2` implementation behind
  `//go:build !(go1.27 && !http2legacy)` and delegates to the standard
  library instead, but the Go 1.27 wrapper never re-exported the
  `TrailerPrefix` constant. `google.golang.org/grpc` references
  `http2.TrailerPrefix` (at every version through v1.83.2), so any build of
  murmur on Go 1.27 failed with `undefined: http2.TrailerPrefix` in
  `grpc/internal/transport/handler_server.go`. x/net 0.55.0 hoists the
  constant into an ungated `server_common.go`; bumping the pin is the whole
  fix — no gRPC change is involved, and no gRPC bump resolves it.
- **CI gained a `Go (latest stable toolchain)` job.** Every existing Go job
  pins `go-version-file: go.mod`, so a dependency that breaks only under a
  newer toolchain could never be caught. The new lane builds and unit-tests
  against current Go as a hard gate.
- **`golangci-lint` pinned to v2.13.1** in CI (was `version: latest`). A
  floating linter makes CI non-reproducible — an untouched commit can go red
  months later because the action pulled a release with new checks. Pinning it
  surfaced one such pre-existing finding, `unparam` on
  `kafka.(*Source).readSerial`, now documented with a `//nolint` explaining
  why the always-nil error result is deliberate signature symmetry with
  `readConcurrent`.

### Fixed — documentation that contradicted the code

Found while walking the matrix for the promotions below. Each of these is
something a prospective adopter reads before they read any code.

- **`doc/design.md` §15 attributed its performance numbers to measurements
  that do not exist.** It claimed they came from "the docker-compose
  integration suite (`test/e2e/`)" and "production-shape micro-benchmarks
  against DDB-local". Both are false: `test/e2e/` holds correctness
  assertions with no `b.N` anywhere, and no benchmark in the repo touches
  DDB-local. The four that exist run against in-memory fakes — the headline
  "10× speedup at N=16" is goroutines contending on a `sync/atomic` slot
  array with a `time.Sleep` standing in for store latency. §15 now opens with
  an explicit provenance note, and the same caveat is applied to the
  throughput claim in `README.md` and the 10× claim in `STABILITY.md`.
  Measured numbers become a deliverable of the v1 soak.
- **`STABILITY.md`'s `pkg/metrics` row was factually wrong**, claiming "only
  `streaming.Run` is wired today; bootstrap / replay / sources are not."
  `Recorder` has been wired through `pkg/exec/processor` — and therefore
  through bootstrap, replay, and every Lambda handler — since the processor
  consolidation. Sources genuinely aren't instrumented; the row now says so
  precisely.
- **`README.md`'s Status paragraph contradicted its own feature table** and
  the CHANGELOG, saying `get_many` / `get_range` were "Sum-only until
  `pkg/query/typed` grows the matching methods." That gate was lifted during
  the typed-client parity work.
- **The v1 release criteria were unfollowable.** "`v1.0.0` will ship after PR
  1–4 land" had three incompatible readings in-repo, and under every
  non-literal one all four had already landed. Replaced with a checkable
  four-part list naming the soak target, the promotions it unblocks, the five
  code blockers it does *not* fix (with the `Build()` → `Validate()` rename
  flagged as the only one with a hard deadline), and the release-engineering
  gap.

### Changed — STABILITY.md promotions (experimental → mostly stable)

Thirteen packages move off the `experimental` row. Promotion criteria:
the package's feature surface has been exercised by integration / e2e
tests, the STABILITY rows tracked against it in earlier passes have
all closed, and there are no open known sharp edges in the package's
notes column.

Promoted:

- **State**: `pkg/state/dynamodb`, `pkg/state/valkey`.
- **Streaming runtime**: `pkg/exec/streaming`, `pkg/exec/bootstrap`,
  `pkg/exec/replay`.
- **Sources**: `pkg/source/snapshot/jsonl`, `pkg/source/snapshot/s3`.
- **Replay**: `pkg/replay/s3`.
- **Query**: `pkg/query/grpc`, `pkg/query/typed`.
- **Admin**: `pkg/admin`.
- **Algebra**: `pkg/monoid/compose`.
- **Tools**: `cmd/murmur-codegen-typed`.

Holding `experimental` on purpose:

- `pkg/source/kafka` — per-partition concurrency is fresh; needs soak.
- `pkg/exec/processor` + `pkg/exec/lambda/{kinesis,dynamodbstreams,sqs}`
  — processor's docstring ties its stability to the Lambda runtimes,
  which haven't been exercised against real (non-`local`) AWS yet.
  Promote the four together after the real-AWS soak.
- `pkg/source/snapshot/mongo`, `pkg/source/snapshot/dynamodb` —
  known sharp edges still documented in the notes column.
- `pkg/monoid/sketch/{hll,topk,bloom}` — cross-runtime encoding
  portability not yet proven.
- `pkg/projection`, `pkg/observability/autoscale` — too new.
- `pkg/pipeline`, `pkg/murmur` — author-flagged "expect renames
  before v1."
- `pkg/exec/batch/sparkconnect` — `replace`-directive gotcha persists
  until the fork is upstreamed.
- `cmd/murmur-ui` — explicitly "demo-grade dashboard."

#### Silent event loss when a merge fails after the dedup claim (data-loss bug)

`pkg/exec/processor` claimed an EventID via `Deduper.MarkSeen` *before*
running the merge, and `state.Deduper` had no way to give a claim back. Any
merge that failed after the claim succeeded — a store outage outlasting the
retry budget, a Lambda timeout mid-batch, a context cancellation — left the
EventID marked seen forever. The source's redelivery then hit `dedup_skip`
and the event was dropped permanently.

The failure is silent by construction: no error reaches the caller (the
record is already "handled"), no metric moves, and nothing distinguishes it
from a legitimate duplicate. For non-idempotent monoids (Sum / HLL / TopK)
counts drift downward with no signal. The most likely trigger in a Lambda
deployment is a timeout: on timeout the function returns no
`BatchItemFailures` at all, so the whole batch is redelivered and every
claimed-but-unmerged record in it is already invisible.

- **`state.Deduper` gains `Release(ctx, eventID) error`** — **breaking
  change** for anyone implementing the interface outside this repo.
  Releasing an unclaimed ID must be a no-op, not an error.
- **`pkg/state/dynamodb.Deduper.Release`** deletes the claim row via
  unconditional `DeleteItem`, which is naturally idempotent and treats a TTL
  eviction that beat it as success. The `dynamodb:DeleteItem` grant already
  exists in both Terraform modules, so no IAM change is required.
- **`processor.MergeMany` releases the claim when a merge fails**, and only
  when *this* call won it. Emits `<pipeline>:dedup_release`, or
  `<pipeline>:dedup_release_failed` if the release itself errors — the
  underlying merge error is still what reaches the caller, so
  `BatchItemFailures` and source retry are unaffected.
- Regression tests cover all three directions: a failed merge releases and
  the redelivery applies; a successful merge keeps its claim; a release
  failure is recorded without masking the merge error.

**`KeyByMany` caveat:** if an earlier key in a multi-key merge already
succeeded, releasing lets the redelivery re-apply it, so a hierarchical
rollup can over-count that key. That is the correct trade — at-least-once
permits re-application but never permits loss, and dedup is a best-effort
mitigation layered on top, not a stronger guarantee.

### Fixed — soak deploy composition (`terraform-multisource`)

The composition added for the real-AWS soak could not be applied. Three
independent blockers, none of which `terraform validate` can see:

- **`plan` could not complete at all.** `pipeline-lambda-kinesis` gated two
  `count` arguments on `var.dedup_table_arn != null`, and that ARN comes from
  a sibling module in the same run — unknown at plan time. Terraform aborted
  with *"The count value depends on resource attributes that cannot be
  determined until apply."* Replaced with an explicit plan-time
  `dedup_enabled` bool; the ARN now scopes the IAM policy only, where
  apply-time values are fine. The module's own README demonstrated the broken
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
  Note that root variables are unknown during `terraform validate`, so the
  provider skips its own name checks — a `validation` block is the only form
  of this that a validate-based CI gate can enforce.

### Added — soak observability and cost controls

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
- **`soak.tfvars.example`** — the ~$60–80/mo shape, versus ~$180–225/mo for
  the production-shape defaults.

### Changed

- **The runbook's cost table was wrong by 2×** in the direction that matters:
  it omitted the ALB and NAT hourly lines entirely (both bill 24/7 regardless
  of throughput) and understated DynamoDB by 7–15× by not accounting for
  TopK's read-plus-two-conditional-writes per event on a single hot row.
  Rewritten as a standing-vs-usage split with the arithmetic shown.
- **AWS provider constraint tightened** from `>= 5.0` to `~> 6.0` across all
  three configurations. The code reads `data.aws_region.current.region`, which
  only exists on v6 (v5 exposes `.name`), so the old floor was wrong — and an
  open-ended upper bound let an unattended re-init during a long soak pull a
  future v7 and plan destructive replacements against live resources.

### Added — real-AWS deploy composition for `recently-interacted-topk`

End-to-end Terraform under [`examples/recently-interacted-topk/terraform/`](examples/recently-interacted-topk/terraform/)
that stands up the canonical multi-source TopK example on real AWS in one
`terraform apply`. Targeted at the v1 real-AWS soak; exercises every
`experimental`-flagged package gated on operational evidence (Lambda runtimes,
processor core, Kafka per-partition concurrency).

Module-side additions:

- **`pipeline-counter`** grew an optional `dedup_enabled` flag that
  provisions a sibling DDB dedup table (pk:S hash key, native TTL on `ttl`),
  grants the worker + bootstrap roles read+write, and injects
  `DDB_DEDUP_TABLE` into all three task environments. Exposed as
  `dedup_table_arn` / `dedup_table_name` outputs so sibling modules can share
  the same table.
- **`pipeline-lambda-kinesis`** is a new sibling module: Kinesis data stream
  (or BYO via `kinesis_stream_arn`), Lambda function (provided.al2, arm64 by
  default), event-source mapping with `BatchItemFailures` +
  `bisect_batch_on_function_error`, Lambda IAM with Kinesis consumer perms +
  DDB state + optional dedup grants + optional SQS/SNS on-failure
  destination, CloudWatch log group with caller-controlled retention.
- **`examples/recently-interacted-topk/terraform/`** composes both modules,
  provisions CloudWatch alarms (Lambda Errors / IteratorAge / Throttles, DDB
  WriteThrottles on state+dedup, Kinesis WriteThrottles), and ships a
  `terraform.tfvars.example` plus a runbook covering build → push → apply →
  smoke-test → teardown.

### Added — v1 readiness pass

A focused push closing the remaining gaps before tagging `v1.0.0`. Each
entry below references a real package or commit; for the per-commit
history use `git log v1-prep`.

#### Valkey-native Bloom acceleration

- **`pkg/state/valkey.BloomCache`** mirrors `HLLCache` for Bloom filters
  using the `valkey-bloom` / RedisBloom `BF.*` command surface
  (`BF.ADD` / `BF.MADD` / `BF.EXISTS` / `BF.MEXISTS` / `BF.RESERVE` /
  `BF.INFO`). Side-by-side with the BytesStore-authoritative
  bits-and-blooms sketch; independent FPR realizations. Integration
  tests gate on `VALKEY_BLOOM_ENABLED` and skip cleanly when the module
  isn't loaded. Closes the only explicit roadmap row in `README.md`.

#### Kafka source: DLQ producer + per-partition concurrency

- **`pkg/source/kafka.NewDLQProducer`** — convenience wiring of a
  franz-go producer into the existing `OnDecodeError` / `OnFetchError`
  callbacks so poison pills land on a dead-letter topic with diagnostic
  headers (`x-murmur-source-topic` / `-partition` / `-offset` /
  `-error` / `-error-kind`).
- **`pkg/source/kafka.Config.Concurrency`** — N decoder goroutines plus
  one fetcher; each partition pinned to worker `partition mod N` so
  per-partition order is preserved while decode-heavy formats
  (Protobuf with schema lookups, encrypted payloads) saturate multiple
  cores. Default `Concurrency=1` keeps the historical single-goroutine
  path verbatim. Lifts the "no per-partition parallelism" line.

#### Typed-client parity (HLL / TopK / Bloom)

- **`pkg/query/typed`** — `HLLClient`, `TopKClient`, `BloomClient` all
  grew `GetMany` + `GetRange` methods to match `SumClient`. Per-entity
  present flags on `GetMany` (the underlying `GetMany` RPC surfaces
  them); merged response on `GetRange` (the RPC merges before
  returning).
- **`cmd/murmur-codegen-typed`** — dropped the Sum-only gate on
  `get_many` / `get_range`; new per-kind render branches emit shape-
  appropriate response messages and server-stub Go code. The
  `top-products` and `recent-visitors` example specs grew
  `GetTopMany` / `GetTopRange` / `GetFilterShapeMany` /
  `GetFilterShapeRange` methods so the new render paths are covered by
  the existing golden tests.

#### Admin auth middleware

- **`pkg/admin.WithAuthToken`** — static-bearer-token Authenticator
  with constant-time comparison and multi-token rotation. Tokens that
  don't match yield 401 with a `WWW-Authenticate: Bearer realm=...`
  hint and no body.
- **`pkg/admin.WithJWTVerifier`** — wraps a user-supplied `JWTVerifier`
  (OIDC / JWKS / JWT library of choice) as an Authenticator.
- **`pkg/admin.Authenticator`** — pluggable interface for callers
  wiring their own auth schemes.
- Middleware ordering: CORS (with OPTIONS short-circuit) outside Auth
  outside the Connect handler. Auth is off by default — unchanged for
  same-origin / network-isolated deploys.
- **`cmd/murmur-ui --auth-token`** + `MURMUR_ADMIN_TOKEN` env fallback
  makes the bundled UI auth-aware without code changes.

#### Parquet S3 replay driver

- **`pkg/replay/s3.ParquetDriver`** pairs with the existing JSON-Lines
  `Driver`: list S3 objects under a prefix, GetObject, read each as
  Parquet via apache/arrow-go/v18, emit one `source.Record` per row via
  a user-supplied `ParquetDecoder(arrow.Record, row int) -> T`. Same
  archive can hold both formats; the default `KeyFilter` selects only
  `*.parquet`. No new direct dependencies — arrow-go was already
  pulled in by `pkg/source/snapshot/parquet`.

#### Atomic state-table swap wired into Terraform

- **`deploy/terraform/modules/pipeline-counter`** — opt-in
  `swap_enabled = true` provisions a [`pkg/swap`](pkg/swap) control DDB
  table, seeds the alias pointer when `swap_initial_version` is set,
  grants the worker / query / bootstrap task roles the right IAM
  (read on worker+query, read+write on bootstrap), and injects
  `SWAP_CONTROL_TABLE` + `SWAP_ALIAS` env vars into every task
  definition. Module README gains a v1 → v2 cutover recipe. Default
  behavior is unchanged (`swap_enabled = false`).

#### Closed STABILITY rows

- `pkg/source/kafka` — DLQ hook + per-partition parallelism both
  shipped.
- `pkg/state/valkey` — Bloom accelerator shipped.
- `pkg/replay/s3` — Parquet shipped.
- `pkg/admin` — auth middleware shipped.
- `pkg/swap` — Terraform integration shipped.
- `pkg/query/typed` + `cmd/murmur-codegen-typed` — Sum-only restriction
  on `get_many` / `get_range` lifted.

### Changed — Kinesis is Lambda-only in production

- **`pkg/source/kinesis`** is now flagged "dev / demo only" in `STABILITY.md`. It's a single-instance polling consumer with no checkpointing — fine for integration tests and local one-shot consumers, not for production. Production Kinesis ingest goes through **`pkg/exec/lambda/kinesis`**, which lets AWS Lambda's event-source mapping own shard discovery, lease coordination, autoscaling (via `ParallelizationFactor`), checkpointing, and partial-batch retry (`BatchItemFailures`).
- **`KCL-v3 Kinesis source` removed from the roadmap.** Lambda is the supported production path; we will not bring KCL v3 Go in-tree. The roadmap row in `README.md` is gone.
- The same pipeline definition runs as either a Kafka ECS worker (`streaming.Run`) or a Kinesis Lambda — both share state via DDB, so `examples/recently-interacted-topk/` can ingest from both simultaneously.

### Added — Typed gRPC codegen: get_window_many / get_many / get_range

- `cmd/murmur-codegen-typed` now emits `get_window_many` methods alongside `get_all_time` / `get_window`. Per-kind response shapes:
  - **sum / hll** → `repeated int64 values` (delegates to `typed.{Sum,HLL}Client.GetWindowMany`)
  - **topk** → `repeated TopKItemList entries` (proto3 disallows nested repeated, so each entry wraps the per-entity ranking)
  - **bloom** → `repeated BloomShape entries` (per-entity filter structural metadata)
- Spec validation requires the `key_template` to reference the `many_key_field`; otherwise every element of the batch produces the same key and the loop variable is unused.
- `get_many` (Sum-only) — batched all-time read with per-entity present flag. Response: `repeated int64 values + repeated bool present`. Lets callers distinguish "absent" from "present-and-zero", which `get_window_many` cannot.
- `get_range` (Sum-only) — absolute Unix-second start/end range read. Response: `int64 value` (merged across the buckets in range).
- `get_many` / `get_range` are gated to `pipeline_kind=sum` at validate time, because the typed clients in `pkg/query/typed` only expose `GetMany` / `GetRange` on `SumClient`. Lift the restriction when HLL/TopK/Bloom typed clients grow those methods.
- All three example pipeline-specs (`bot-interactions` / `top-products` / `recent-visitors`) grew a `Get*WindowMany` method; `bot-interactions` additionally grew `GetCountMany` + `GetCountRange`. Goldens regenerated.
- Closes the "Per-pipeline gRPC codegen (typed responses)" roadmap row in `README.md` for the four currently-supported pipeline kinds.

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
