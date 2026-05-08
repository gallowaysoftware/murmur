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
| `pkg/monoid/sketch/{hll,topk,bloom}` | experimental | `Combine` returning the wrong operand on decode error is tracked; cross-runtime encoding portability not yet proven |
| `pkg/monoid/compose` | experimental | `MapMerge` / `Tuple2` / `DecayedSum`; FP-associativity caveats apply to `DecayedSum` |
| `pkg/monoid/windowed` | mostly stable | bucket math is solid; minute-granularity has high read-amplification on long ranges |
| `pkg/state` (interfaces) | mostly stable | `Store` / `Cache` interfaces unlikely to change before v1. `state.NewInstrumented` / `state.NewInstrumentedCache` decorate any store/cache with metrics.Recorder hooks (store_get / store_get_many / store_merge_update / cache_get / cache_repopulate latencies + errors) |
| `pkg/state/dynamodb` | experimental | `BatchGetItem` retries `UnprocessedKeys` with chunking + jittered backoff; CAS path retries CCF with the same backoff policy |
| `pkg/state/valkey` | experimental | `Int64Cache` (atomic INCRBY) + `BytesCache` (RMW with caller-supplied byte-monoid; works with HLL/TopK/Bloom/DecayedSumBytes). No native PFADD path — Valkey-native HLL is incompatible with axiomhq's encoding; bridging is roadmap |
| `pkg/source/kafka` | experimental | poison pills are silently dropped (no DLQ hook yet); no per-partition parallelism |
| `pkg/source/kinesis` | experimental, single-instance | NO checkpointing, NO multi-instance leasing; KCL v3 upgrade is roadmap |
| `pkg/source/snapshot/mongo` | experimental | `extractID` is brittle for non-`_id` types beyond ObjectID/string/int |
| `pkg/source/snapshot/dynamodb` | experimental | DDB ParallelScan bootstrap source. CaptureHandoff captures a Streams shard timestamp for gap-and-duplicate-free bootstrap → live transition. Resume restarts from the beginning rather than per-segment LastEvaluatedKey checkpointing — at-least-once dedup absorbs the duplicates |
| `pkg/source/snapshot/jsonl` | experimental | JSON-Lines bootstrap source. Reads from any io.Reader (local file, S3 GetObject body, gzip stream). HandoffToken is caller-supplied (bootstrap from a snapshot whose live-source resume position was captured externally). Default EventID is `<name>:<line-num>`; override with EventIDFn for re-run idempotency |
| `pkg/source/snapshot/s3` | experimental | S3 prefix-scan bootstrap source. Composes ListObjectsV2 + GetObject + auto-gzip with the jsonl line decoder. Right tool for "bootstrap from a partitioned S3 archive" (Firehose, daily DDB exports, Hive-style partitions). KeyFilter, custom OpenObject hook, custom EventIDFn |
| `pkg/replay/s3` | experimental | JSON Lines only; Parquet is roadmap |
| `pkg/exec/streaming` | experimental | per-record retry + DLQ via WithMaxAttempts / WithDeadLetter; opt-in write aggregation (`WithBatchWindow`) collapses N hot-key records into 1 store call per flush window; `WithConcurrency(N)` distributes records across N worker goroutines via key-hash routing (10× speedup at N=16 under 5ms-store-latency benchmark); `RunFanout` runs N pipelines against ONE shared source with counted-tee Ack semantics (one underlying source.Ack after every pipeline has processed) |
| `pkg/exec/processor` | experimental | shared retry / dedup / metrics core used by streaming.Run + every Lambda handler. `MergeOne` is the canonical entry point for out-of-tree drivers |
| `pkg/projection` | experimental | bucket functions (Log/Linear/Manual) and hysteresis-band transition detection for projector-style change-data-capture into search indices. The pkg-level building block for doc/search-integration.md Pattern B |
| `pkg/observability/autoscale` | experimental | Periodic Signal → Emitter loop for publishing scaling-signal metrics. Reference CloudWatch emitter; Signal helpers like `EventsPerSecond` derive rates from the metrics recorder. Closes `doc/architecture.md` open question #2 (worker autoscaling) |
| `pkg/exec/bootstrap` | experimental | Shares the `pkg/exec/processor` core with streaming + Lambda. Per-record retry via `WithMaxAttempts` / `WithRetryBackoff`; permissive on dead-letter by default (use `WithFailOnError` to abort). Honors `KeyByMany` hierarchical rollups |
| `pkg/exec/replay` | experimental | Shares the `pkg/exec/processor` core. Same retry / dead-letter / `KeyByMany` semantics as bootstrap. metrics.Recorder fully wired; the historical "metrics integration not yet wired" note is fixed |
| `pkg/exec/batch/sparkconnect` | experimental | depends on a `replace`d fork of `apache/spark-connect-go` |
| `pkg/exec/lambda/kinesis` | experimental | `NewHandler` returns the Lambda Kinesis handler signature; partial-batch failures via BatchItemFailures; pair with `WithDedup` so adjacent-redelivered records fold idempotently |
| `pkg/exec/lambda/dynamodbstreams` | experimental | DDB Streams Lambda handler; same retry/dedup/BatchItemFailures shape as the Kinesis variant. Decoder takes the whole change record so callers can branch on EventName / inspect OldImage |
| `pkg/exec/lambda/sqs` | experimental | SQS Lambda handler; same shape as kinesis/dynamodbstreams. Default EventID is "<arn>/<MessageId>"; override via WithEventID for FIFO content-dedup or upstream-key dedup. Uses SQS SentTimestamp for windowed-bucket assignment so delayed deliveries land in the correct bucket |
| `pkg/query` | mostly stable | `Get` / `GetWindow` / `GetRange` / `LambdaQuery` are likely v1 surface |
| `pkg/query/grpc` | experimental | generic byte-encoded responses; per-pipeline codegen is roadmap |
| `pkg/admin` | experimental | CORS is closed by default; opt in via `WithAllowedOrigins`. No auth middleware yet |
| `pkg/swap` | mostly stable | small surface; the Terraform module does not yet integrate it |
| `pkg/metrics` | mostly stable | only `streaming.Run` is wired today; bootstrap / replay / sources are not |
| `cmd/murmur-ui` | experimental | demo-grade dashboard; not yet a production ops surface |

## Known sharp edges (priority order)

1. **Silent error paths.** ~~Many `_ = err` sites across sources, caches, and sketch
   `Combine` swallow real failures.~~ Closed by the `pkg/exec/processor` consolidation
   (streaming, bootstrap, replay, and every Lambda handler now share one
   retry/dedup/metrics core) plus per-source `OnDecodeError` and `OnFetchError`
   callbacks for poison-pill routing. The remaining `_ = err` sites are documented
   non-fatal cleanup paths (e.g. franz-go `CommitMarkedOffsets` during Close).

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
   MarkSeen wins.

4. ~~**Min/Max under empty/missing buckets.**~~ Resolved by the
   `Bounded[V]`-based Min/Max from PR-3: empty buckets fold as the unset
   `Bounded[V]{Set: false}` (which IS the identity), so a windowed `Min`
   over a partially-empty range correctly reports the min of populated
   buckets and `Set: false` if everything was empty.

5. **`go.mod` `replace` directive.** Importing
   `pkg/exec/batch/sparkconnect` requires consumers to mirror the
   `replace github.com/apache/spark-connect-go => github.com/pequalsnp/spark-connect-go ...`
   line. Tracked: upstream the patches or split the package into its own module.

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

`v1.0.0` will ship after PR 1–4 land and the framework has been exercised against
real (non-`local`) AWS for at least one full quarter.
