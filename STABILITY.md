# Stability

Murmur is **pre-1.0**. Public APIs may change without notice. This document tracks
where each package sits on the experimental → stable spectrum, and the known sharp
edges callers should plan around.

## Per-package status

| Package | Status | Notes |
|---|---|---|
| `pkg/pipeline` | experimental | DSL surface is likely to gain `Validate()` (renamed from `Build()`) and per-stage type narrowing |
| `pkg/murmur` | experimental | Builder presets are the recommended entry point; expect renames before v1 |
| `pkg/monoid/core` | mostly stable | `Min` / `Max` use `Bounded[V]` for a proper Identity; lift inputs via `core.NewBounded(v)`. `Monotonic[V](identity)` is the raw-V counterpart that pairs with conditional-update stores like `pkg/state/dynamodb.Int64MaxStore` for the SetCountIfGreater pattern (out-of-order absolute-value safety) |
| `pkg/monoid/sketch/{hll,topk,bloom}` | experimental | On a decode error `Combine` returns the operand that decoded and discards the other — unavoidable while `Monoid.Combine` has no error return, but now reportable via `WithDecodeErrorHandler`; wire it to a `metrics.Recorder` or the loss stays invisible. For `bloom` the same hook now reports **shape problems**: two filters whose (m, k) differ cannot be OR'd (`Combine` keeps the left operand), and operands that agree with each other but not with the (n, p) passed to `NewWithCapacity` are merged but reported — without that report those parameters were decorative, since every filter carries its own shape on the wire. `bloom.Bloom` / `bloom.NewWithCapacity` Identity is now the empty slice, so it is an identity for an operand of ANY shape. **`topk` counts are Misra-Gries lower bounds and the wire format does not record the stream size**, so a summary that retained 29 of 45,932 events is indistinguishable from an exact answer over 29; carry `n` out of band to size the `n/(K+1)` error bar. Making that visible in the header is a staged wire-format change that has not landed. Cross-runtime encoding portability not yet proven |
| `pkg/monoid/compose` | mostly stable | `MapMerge` / `Tuple2` / `DecayedSum`; FP-associativity caveats apply to `DecayedSum`. `DecayedSum` / `DecayedSumBytes` take one experimental option, `WithDecodeErrorHandler` (bytes only — a non-17-byte operand is now an error, not a value assembled from someone else's blob), and `DecodeDecayed` returns `(Decayed, error)`. A non-positive half-life means "no decay" in `Combine` as well as `EvaluateAt`, and `EvaluateAt` before the reference time returns the stored value rather than scaling it up. `Combine` is a pure function of its operands — it reads no clock, because `BytesStore.MergeUpdate` recomputes it on every CAS retry. A future-dated observation therefore still freezes a key, and the guard is `compose.ClampFuture(t, now, compose.DefaultSkewBound(halfLife))` applied at the **lift**, which is the last point where a clock reading is honest; `murmur.Trending` stamps at its own clock and is not exposed |
| `pkg/monoid/windowed` | mostly stable | bucket math is solid; minute-granularity has high read-amplification on long ranges |
| `pkg/state` (interfaces) | mostly stable | `Store` / `Cache` interfaces unlikely to change before v1. `state.NewInstrumented` / `state.NewInstrumentedCache` decorate any store/cache with metrics.Recorder hooks (store_get / store_get_many / store_merge_update / cache_get / cache_repopulate latencies + errors) |
| `pkg/state/dynamodb` | mostly stable | `BatchGetItem` retries `UnprocessedKeys` with chunking + jittered backoff; CAS path retries CCF with the same backoff policy, tunable per store via `WithCASRetries` / `WithCASBackoff` and counted under `<pipeline>:cas_conflict` when `WithCASMetrics` is wired. `BytesStore` pre-flights DynamoDB's 400KB item limit and returns `ErrItemTooLarge` / `*ItemTooLargeError` — sketch size tracks key length, not just K, and an oversized row otherwise fails every write while `Get` keeps serving the last value that fit. `Deduper` claim keys are `"<pipeline>#<EventID>"` (`NewDeduper` takes the pipeline name; `ForPipeline` derives a sibling scope) and carry a per-call `claimant` token so a claim whose response was lost is not mistaken for a peer's. `Int64MaxStore` ships the SetCountIfGreater pattern via DDB `UpdateItem` with conditional expression — out-of-order events with lower values are silently dropped |
| `pkg/state/valkey` | mostly stable | `Int64Cache` (atomic INCRBY) + `BytesCache` (RMW with caller-supplied byte-monoid; works with HLL/TopK/Bloom/DecayedSumBytes) + `HLLCache` (Valkey-native PFADD/PFCOUNT/PFMERGE accelerator) + `BloomCache` (Valkey-native BF.ADD/BF.MADD/BF.EXISTS/BF.MEXISTS accelerator; requires the valkey-bloom or RedisBloom module loaded into the server). The sketch accelerators run side-by-side with the BytesStore-authoritative sketches — independent estimators, both within the monoid's error bound. No portable axiomhq↔HYLL or bits-and-blooms↔valkey-bloom byte conversion: on Valkey loss the accelerators can only be repopulated by re-feeding events |
| `pkg/source/kafka` | experimental | poison pills are routed via `Config.OnDecodeError` (no surface change for default-drop semantics); convenience `NewDLQProducer` wires a franz-go producer that publishes poison records + diagnostic headers to a dead-letter topic. Per-partition decode concurrency via `Config.Concurrency` — N decoder goroutines plus one fetcher, with each partition pinned to worker `partition mod N` so per-partition order is preserved while decode-heavy formats saturate multiple cores |
| `pkg/source/kinesis` | dev / demo only | Polling ECS consumer: single-instance, no checkpointing, no resharding. Production path is `pkg/exec/lambda/kinesis` — AWS Lambda event-source mapping owns shard discovery / leasing / autoscaling / checkpointing. KCL v3 Go is NOT planned in-tree |
| `pkg/source/snapshot/mongo` | experimental | `extractID` is brittle for non-`_id` types beyond ObjectID/string/int |
| `pkg/source/snapshot/dynamodb` | experimental | DDB ParallelScan bootstrap source. CaptureHandoff captures a Streams shard timestamp for gap-and-duplicate-free bootstrap → live transition. Resume restarts from the beginning rather than per-segment LastEvaluatedKey checkpointing — at-least-once dedup absorbs the duplicates |
| `pkg/source/snapshot/jsonl` | mostly stable | JSON-Lines bootstrap source. Reads from any io.Reader (local file, S3 GetObject body, gzip stream). HandoffToken is caller-supplied (bootstrap from a snapshot whose live-source resume position was captured externally). Default EventID is `<name>:<line-num>`; override with EventIDFn for re-run idempotency |
| `pkg/source/snapshot/s3` | mostly stable | S3 prefix-scan bootstrap source. Composes ListObjectsV2 + GetObject + auto-gzip with the jsonl line decoder. Right tool for "bootstrap from a partitioned S3 archive" (Firehose, daily DDB exports, Hive-style partitions). KeyFilter, custom OpenObject hook, custom EventIDFn |
| `pkg/replay/s3` | mostly stable | JSON Lines via `Driver`; Parquet via `ParquetDriver` (apache/arrow-go/v18; the same archive can hold both formats — the default Parquet `KeyFilter` selects only `*.parquet`) |
| `pkg/exec/streaming` | mostly stable | per-record retry + DLQ via WithMaxAttempts / WithDeadLetter; opt-in write aggregation (`WithBatchWindow`) collapses N hot-key records into 1 store call per flush window, and a dead-lettered flush returns the batch's dedup claims so replaying those EventIDs out of the DLQ re-applies them instead of hitting `dedup_skip` (a shutdown drain gathers every failed batch's claims into ONE bounded release instead of paying a fresh budget per batch, so a claim comes back at the end of the drain rather than the instant its own batch failed); `WithConcurrency(N)` distributes records across N worker goroutines via key-hash routing (10× at N=16 in an in-memory benchmark whose "store latency" is a `time.Sleep` — not a measurement against DynamoDB); `RunFanout` runs N pipelines against ONE shared source with counted-tee Ack semantics (one underlying source.Ack after every pipeline has processed); `WithKeyDebounce` drops same-key records within a window, `WithValueDebounce` drops same-(key,value) records within a window — both safe with idempotent / absolute-value monoids (Max/Min/Set/Last/HLL/Bloom/Monotonic), unsafe with delta-accumulating monoids (Sum/Count/TopK) |
| `pkg/exec/processor` | experimental | shared retry / dedup / metrics core used by streaming.Run + every Lambda handler. `MergeOne` is the canonical entry point for out-of-tree drivers. A merge that fails after the dedup claim now releases it (`Deduper.Release`) so the redelivery re-applies rather than being silently skipped — see CHANGELOG. The batching paths release too: `Coalescer.Flush` and the `pkg/exec/streaming` aggregator hand back the claims of every event whose delta failed to reach the store, on both the retry-exhausted and the cancelled branch, and the release runs on a context DETACHED from the caller's — the likeliest reason a flush failed at all is that the caller's context was just cancelled by SIGTERM. `ReleaseClaims` is the exported helper behind all of them — out-of-tree batching drivers that claim at buffer time should call it on their failure path, once per drain rather than once per failed batch: one call is one budget, sized from the claim count and spent across a bounded worker pool, and claims the budget doesn't cover surface as `dedup_release_failed`. Ownership is tracked per EventID, not per batch: `MarkSeen` is fail-open, so a buffered event may be riding another worker's claim and only the winner may release. **Releasing trades loss for over-count on multi-key (`KeyByMany`) records, and the batching paths widen that trade.** In `MergeMany`, a record that failed on its third key re-applies its first two on redelivery. In the Coalescer it is worse: a PARTIAL flush failure releases the claims of events that also contributed to sibling keys whose writes SUCCEEDED, so the redelivery re-applies those events to the successful keys as well and their Sum / Count / TopK values end up above the true count. The over-count is bounded by the fan-out of the failing record's key set, not by the number of retries. At-least-once permits re-application and never permits loss; dedup is a best-effort mitigation layered on top, not a stronger guarantee |
| `pkg/projection` | experimental | bucket functions (Log/Linear/Manual) and hysteresis-band transition detection for projector-style change-data-capture into search indices. The pkg-level building block for doc/search-integration.md Pattern B |
| `pkg/observability/autoscale` | experimental | Periodic Signal → Emitter loop for publishing scaling-signal metrics. Reference CloudWatch emitter; Signal helpers like `EventsPerSecond` derive rates from the metrics recorder. Closes `doc/architecture.md` open question #2 (worker autoscaling) |
| `pkg/exec/bootstrap` | mostly stable | Shares the `pkg/exec/processor` core with streaming + Lambda. Per-record retry via `WithMaxAttempts` / `WithRetryBackoff`; permissive on dead-letter by default (use `WithFailOnError` to abort). Honors `KeyByMany` hierarchical rollups |
| `pkg/exec/replay` | mostly stable | Shares the `pkg/exec/processor` core. Same retry / dead-letter / `KeyByMany` semantics as bootstrap. metrics.Recorder fully wired; the historical "metrics integration not yet wired" note is fixed |
| `pkg/exec/batch/sparkconnect` | experimental | own Go submodule (separate `go.mod`) so root `github.com/gallowaysoftware/murmur` doesn't pull `apache/spark-connect-go`. Consumers who DO depend on this submodule must mirror its `replace` line for the `pequalsnp/spark-connect-go` fork in their own `go.mod` |
| `pkg/exec/lambda/kinesis` | experimental | `NewHandler` returns the Lambda Kinesis handler signature; partial-batch failures via BatchItemFailures; pair with `WithDedup` so adjacent-redelivered records fold idempotently |
| `pkg/exec/lambda/dynamodbstreams` | experimental | DDB Streams Lambda handler; same retry/dedup/BatchItemFailures shape as the Kinesis variant. Decoder takes the whole change record so callers can branch on EventName / inspect OldImage. BatchItemFailures report the record's SequenceNumber (what Lambda checkpoints on); the eventID feeds dedup only. A failed record with an empty SequenceNumber gets no entry at all (an empty ItemIdentifier makes Lambda redeliver the whole batch) and is surfaced via metrics.RecordError plus a `<name>:unreportable_failure` event |
| `pkg/exec/lambda/sqs` | experimental | SQS Lambda handler; same shape as kinesis/dynamodbstreams. Default EventID is "<arn>/<MessageId>"; override via WithEventID for FIFO content-dedup or upstream-key dedup. Uses SQS SentTimestamp for windowed-bucket assignment so delayed deliveries land in the correct bucket |
| `pkg/query` | mostly stable | `Get` / `GetWindow` / `GetRange` / `LambdaQuery` are likely v1 surface |
| `pkg/query/grpc` | mostly stable | generic byte-encoded responses; `cmd/murmur-codegen-typed` emits per-service typed `.proto` + Go server stubs (sum / hll / topk / bloom; get_all_time / get_window / get_window_many / get_many / get_range) over `pkg/query/typed` clients. `HealthHandler` serves `grpc.health.v1.Health` and `HealthzHandler` serves `/healthz` (liveness, always 200) + `/readyz` (readiness, store round-trip, cached so probe traffic is not billed reads) |
| `pkg/query/typed` | mostly stable | typed-client wrappers over the generic QueryService — `SumClient`, `HLLClient`, `TopKClient`, `BloomClient`. All four expose `Get` / `GetMany` / `GetWindow` / `GetWindowMany` / `GetRange`. `GetMany` returns parallel value + present arrays so callers can distinguish "absent" from "present-and-empty"; `GetWindowMany` can't (the generic RPC merges before returning). The decoders + typed shape behind application-service typed-wrapper RPCs (see `examples/typed-wrapper`). Building block under `cmd/murmur-codegen-typed` |
| `pkg/admin` | mostly stable | CORS is closed by default; opt in via `WithAllowedOrigins`. Bearer-token (`WithAuthToken`, constant-time, multi-token for rotation) and JWT (`WithJWTVerifier`, BYO verifier) auth via a single middleware; auth is off by default — same-origin / network-isolated deploys keep the historical behavior. The `cmd/murmur-ui` binary exposes `--auth-token` + `MURMUR_ADMIN_TOKEN` env fallback |
| `pkg/swap` | mostly stable | small surface; integrated into `deploy/terraform/modules/pipeline-counter` via opt-in `swap_enabled` (control table + IAM + seed + `SWAP_CONTROL_TABLE` / `SWAP_ALIAS` env vars in every task definition) |
| `pkg/metrics/emf` | experimental | CloudWatch Embedded Metric Format `Recorder`. Aggregates in memory and emits one document per flush interval as counters plus EMF StatisticSets, so the hot path stays cheap and CloudWatch Logs ingestion does not scale with throughput. Needs no CloudWatch API permission or SDK client — Lambda and the ECS awslogs driver already ship stdout to Logs. Splits `pipeline:sub_event` names so `dedup_skip` / `dedup_release` become their own metrics rather than fragmenting the Pipeline dimension. Lambda callers should `Flush()` per invocation; the environment freezes on return and a background ticker may never fire |
| `pkg/metrics` | mostly stable | `Recorder` is wired through `pkg/exec/processor`, so `streaming.Run`, `bootstrap`, `replay` and every Lambda handler record through it — including `RecordBatch` with a mode tag. Sources are not instrumented; they surface failures via their own `OnDecodeError` / `OnFetchError` callbacks instead |
| `cmd/murmur-ui` | experimental | demo-grade dashboard; not yet a production ops surface |
| `cmd/murmur-codegen-typed` | mostly stable | YAML pipeline-spec → typed Connect-RPC `.proto` + Go server stub (delegates to `pkg/query/typed`). Sum / HLL / TopK / Bloom pipelines; method kinds `get_all_time` / `get_window` / `get_window_many` / `get_many` / `get_range`. TopK emits a `TopKItem { string key; int64 count; }` message plus `TopKItemList` when get_window_many or get_many is used; Bloom emits `(capacity_bits, hash_functions, approx_size, present)` plus `BloomShape` when get_window_many or get_many is used. All method kinds are now available on every pipeline kind — the Sum-only gate on `get_many` / `get_range` was lifted alongside the typed-client parity work |

## Known sharp edges (priority order)

1. **Silent error paths.** Mostly closed, with one caveat that was previously
   struck through in error.

   Closed for sources and runtimes by the `pkg/exec/processor` consolidation
   (streaming, bootstrap, replay, and every Lambda handler share one
   retry/dedup/metrics core) plus per-source `OnDecodeError` / `OnFetchError`
   callbacks for poison-pill routing. The remaining `_ = err` sites are
   documented non-fatal cleanup paths (e.g. franz-go `CommitMarkedOffsets`
   during Close).

   **Still open by design in the sketch monoids.** `Monoid.Combine` is
   `Combine(a, b) V` with no error return, so when `hll` / `topk` / `bloom`
   cannot decode an operand the only recovery is to return the one that did
   and discard the other. That recovery is deliberate — it beats corrupting
   the merged state or panicking a worker mid-batch — but the loss is real:
   the affected key silently loses cardinality or counts.

   It is now at least *observable*. Each constructor takes
   `WithDecodeErrorHandler(func(error))`; wire it to a `metrics.Recorder` so
   dropped operands are counted and alarmable. Closing this properly means
   changing the `Monoid` interface to return an error, which is a v1-scoped
   decision affecting every implementation and every call site.

2. ~~**Monoid laws.**~~ Fixed in PR-3: `Min` / `Max` now use `core.Bounded[V]`
   so Identity is the unset wrapper rather than the zero value of `V`. `Decayed`
   gained an explicit `Set` field so `(0, time.Unix(0, 0))` is no longer
   misclassified as Identity. The new `pkg/monoid/monoidlaws` package fuzzes
   associativity and identity for every built-in monoid in CI; users adding
   custom monoids can drop into the same harness.

3. ~~**At-least-once dedup is not implemented.**~~ Fixed: `state.Deduper`
   contract + `pkg/state/dynamodb.NewDeduper` (DDB-backed, atomic
   PutItem-with-condition claim, native TTL for eviction). Wire it into the
   streaming runtime via `streaming.WithDedup(d)`; duplicates are Ack'd and
   counted under `<pipeline>:dedup_skip` rather than re-applied to the
   monoid. A 16-way race test against dynamodb-local confirms exactly one
   MarkSeen wins. Claim keys are namespaced by pipeline name — pipelines
   sharing one dedup table no longer starve each other on colliding
   EventIDs — and carry a claimant token so a lost claim response is
   distinguishable from a peer's claim.

4. ~~**Min/Max under empty/missing buckets.**~~ Resolved by the
   `Bounded[V]`-based Min/Max from PR-3: empty buckets fold as the unset
   `Bounded[V]{Set: false}` (which IS the identity), so a windowed `Min`
   over a partially-empty range correctly reports the min of populated
   buckets and `Set: false` if everything was empty.

5. ~~**`go.mod` `replace` directive.**~~ Partially fixed:
   `pkg/exec/batch/sparkconnect` now carries its own `go.mod` so the root
   `github.com/gallowaysoftware/murmur` module doesn't depend on
   `apache/spark-connect-go`. Non-Spark consumers get a clean root `go.mod`.
   Consumers who DO depend on the sparkconnect submodule must still mirror
   the `replace github.com/apache/spark-connect-go => github.com/pequalsnp/spark-connect-go …`
   line in their own `go.mod` (Go doesn't propagate replace directives
   transitively). Full fix is upstreaming the fork's patches to
   `apache/spark-connect-go`; tracked separately.

6. ~~**CORS.**~~ Fixed: `pkg/admin.NewServer` now defaults to no CORS headers
   (same-origin only). Open it up to the origins you want via
   `admin.WithAllowedOrigins("https://your-dashboard", …)` or pass `"*"` for
   permissive — `cmd/murmur-ui` exposes `--allow-origin` for the latter.

7. ~~**No CI.**~~ Fixed: `.github/workflows/ci.yml` runs `gofmt`, `go vet`,
   unit tests with `-race`, `golangci-lint`, and the web `tsc` / `lint` /
   `build` pipeline. Dependabot wired for Go modules, npm, and Actions.

## Versioning

Murmur follows SemVer. Until `v1.0.0`:

- Minor versions (`v0.X`) may break public APIs.
- Patch versions (`v0.X.Y`) are bug-fix-only.
- Anything in `internal/` is private.
- Anything documented as "experimental" in this file may be removed entirely
  before v1.

### What gates `v1.0.0`

The former wording here — "after PR 1–4 land" — was dead. That phrase had three
incompatible readings in this repo (the CHANGELOG's set, `doc/design.md`
§17.5's sharp-edges 1–4, and the literal GitHub PRs #1–#4, which are
Dependabot bumps), and under every non-literal reading all four had already
landed. Replaced with a checkable list.

**1. Operational evidence.** The framework must be exercised against real
(non-`local`) AWS for at least one full quarter. The soak target is
`examples/recently-interacted-topk` — the only example that exercises the
Lambda Kinesis runtime, the shared `pkg/exec/processor` core, the Kafka
source, and the multi-source DDB merge simultaneously, which is exactly the
set of packages held at `experimental` for want of operational evidence rather
than API churn. Deploy composition and runbook:
`examples/recently-interacted-topk/terraform/`.

**2. Promotions the soak unblocks.** `pkg/exec/processor`,
`pkg/exec/lambda/kinesis`, and `pkg/source/kafka` → `mostly stable`. Note that
the composition exercises only the *Kinesis* Lambda runtime;
`pkg/exec/lambda/{sqs,dynamodbstreams}` will still have no real-AWS exposure
and need either their own soak or an explicit "promoted by analogy" note.

**3. Code blockers the soak does not fix.** These are independent work:

   - `pkg/pipeline`: the `Build()` → `Validate()` rename. **This is the only
     item with a hard deadline** — after a v1 tag it requires a v2 module path.
   - `pkg/murmur`: the builder-preset renames flagged in the matrix above.
   - `pkg/monoid/sketch/{hll,topk,bloom}`: `Combine` silently returns the
     wrong operand on a decode error. Sharp edge #1 is marked closed for
     sources and runtimes but is genuinely still open here.
   - `pkg/source/snapshot/dynamodb`: per-segment `LastEvaluatedKey`
     checkpointing on resume.
   - `pkg/exec/batch/sparkconnect`: the `replace` directive (sharp edge #5).
     Externally gated on `apache/spark-connect-go`'s merge queue, so the
     realistic options are to accept and document it, vendor the needed
     subset, or drop Spark Connect from the v1 surface. **Decide this before
     the soak ends** — it is the one blocker whose resolution date is not ours
     to control.

**4. Release engineering.** There is exactly one tag in the repo's history,
`v0.1.0`, and it is far behind `main`. Cut an interim `v0.2.0` reflecting the
current tree — jumping from a stale `v0.1.0` straight to `v1.0.0` is not a
good look for a framework asking to be adopted.
