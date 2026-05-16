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
| `pkg/state/dynamodb` | experimental | `BatchGetItem` retries `UnprocessedKeys` with chunking + jittered backoff; CAS path retries CCF with the same backoff policy. `Int64MaxStore` ships the SetCountIfGreater pattern via DDB `UpdateItem` with conditional expression — out-of-order events with lower values are silently dropped |
| `pkg/state/valkey` | experimental | `Int64Cache` (atomic INCRBY) + `BytesCache` (RMW with caller-supplied byte-monoid; works with HLL/TopK/Bloom/DecayedSumBytes) + `HLLCache` (Valkey-native PFADD/PFCOUNT/PFMERGE accelerator) + `BloomCache` (Valkey-native BF.ADD/BF.MADD/BF.EXISTS/BF.MEXISTS accelerator; requires the valkey-bloom or RedisBloom module loaded into the server). The sketch accelerators run side-by-side with the BytesStore-authoritative sketches — independent estimators, both within the monoid's error bound. No portable axiomhq↔HYLL or bits-and-blooms↔valkey-bloom byte conversion: on Valkey loss the accelerators can only be repopulated by re-feeding events |
| `pkg/source/kafka` | experimental | poison pills are routed via `Config.OnDecodeError` (no surface change for default-drop semantics); convenience `NewDLQProducer` wires a franz-go producer that publishes poison records + diagnostic headers to a dead-letter topic. Per-partition decode concurrency via `Config.Concurrency` — N decoder goroutines plus one fetcher, with each partition pinned to worker `partition mod N` so per-partition order is preserved while decode-heavy formats saturate multiple cores |
| `pkg/source/kinesis` | dev / demo only | Polling ECS consumer: single-instance, no checkpointing, no resharding. Production path is `pkg/exec/lambda/kinesis` — AWS Lambda event-source mapping owns shard discovery / leasing / autoscaling / checkpointing. KCL v3 Go is NOT planned in-tree |
| `pkg/source/snapshot/mongo` | experimental | `extractID` is brittle for non-`_id` types beyond ObjectID/string/int |
| `pkg/source/snapshot/dynamodb` | experimental | DDB ParallelScan bootstrap source. CaptureHandoff captures a Streams shard timestamp for gap-and-duplicate-free bootstrap → live transition. Resume restarts from the beginning rather than per-segment LastEvaluatedKey checkpointing — at-least-once dedup absorbs the duplicates |
| `pkg/source/snapshot/jsonl` | experimental | JSON-Lines bootstrap source. Reads from any io.Reader (local file, S3 GetObject body, gzip stream). HandoffToken is caller-supplied (bootstrap from a snapshot whose live-source resume position was captured externally). Default EventID is `<name>:<line-num>`; override with EventIDFn for re-run idempotency |
| `pkg/source/snapshot/s3` | experimental | S3 prefix-scan bootstrap source. Composes ListObjectsV2 + GetObject + auto-gzip with the jsonl line decoder. Right tool for "bootstrap from a partitioned S3 archive" (Firehose, daily DDB exports, Hive-style partitions). KeyFilter, custom OpenObject hook, custom EventIDFn |
| `pkg/replay/s3` | experimental | JSON Lines via `Driver`; Parquet via `ParquetDriver` (apache/arrow-go/v18; the same archive can hold both formats — the default Parquet `KeyFilter` selects only `*.parquet`) |
| `pkg/exec/streaming` | experimental | per-record retry + DLQ via WithMaxAttempts / WithDeadLetter; opt-in write aggregation (`WithBatchWindow`) collapses N hot-key records into 1 store call per flush window; `WithConcurrency(N)` distributes records across N worker goroutines via key-hash routing (10× speedup at N=16 under 5ms-store-latency benchmark); `RunFanout` runs N pipelines against ONE shared source with counted-tee Ack semantics (one underlying source.Ack after every pipeline has processed); `WithKeyDebounce` drops same-key records within a window, `WithValueDebounce` drops same-(key,value) records within a window — both safe with idempotent / absolute-value monoids (Max/Min/Set/Last/HLL/Bloom/Monotonic), unsafe with delta-accumulating monoids (Sum/Count/TopK) |
| `pkg/exec/processor` | experimental | shared retry / dedup / metrics core used by streaming.Run + every Lambda handler. `MergeOne` is the canonical entry point for out-of-tree drivers |
| `pkg/projection` | experimental | bucket functions (Log/Linear/Manual) and hysteresis-band transition detection for projector-style change-data-capture into search indices. The pkg-level building block for doc/search-integration.md Pattern B |
| `pkg/observability/autoscale` | experimental | Periodic Signal → Emitter loop for publishing scaling-signal metrics. Reference CloudWatch emitter; Signal helpers like `EventsPerSecond` derive rates from the metrics recorder. Closes `doc/architecture.md` open question #2 (worker autoscaling) |
| `pkg/exec/bootstrap` | experimental | Shares the `pkg/exec/processor` core with streaming + Lambda. Per-record retry via `WithMaxAttempts` / `WithRetryBackoff`; permissive on dead-letter by default (use `WithFailOnError` to abort). Honors `KeyByMany` hierarchical rollups |
| `pkg/exec/replay` | experimental | Shares the `pkg/exec/processor` core. Same retry / dead-letter / `KeyByMany` semantics as bootstrap. metrics.Recorder fully wired; the historical "metrics integration not yet wired" note is fixed |
| `pkg/exec/batch/sparkconnect` | experimental | own Go submodule (separate `go.mod`) so root `github.com/gallowaysoftware/murmur` doesn't pull `apache/spark-connect-go`. Consumers who DO depend on this submodule must mirror its `replace` line for the `pequalsnp/spark-connect-go` fork in their own `go.mod` |
| `pkg/exec/lambda/kinesis` | experimental | `NewHandler` returns the Lambda Kinesis handler signature; partial-batch failures via BatchItemFailures; pair with `WithDedup` so adjacent-redelivered records fold idempotently |
| `pkg/exec/lambda/dynamodbstreams` | experimental | DDB Streams Lambda handler; same retry/dedup/BatchItemFailures shape as the Kinesis variant. Decoder takes the whole change record so callers can branch on EventName / inspect OldImage |
| `pkg/exec/lambda/sqs` | experimental | SQS Lambda handler; same shape as kinesis/dynamodbstreams. Default EventID is "<arn>/<MessageId>"; override via WithEventID for FIFO content-dedup or upstream-key dedup. Uses SQS SentTimestamp for windowed-bucket assignment so delayed deliveries land in the correct bucket |
| `pkg/query` | mostly stable | `Get` / `GetWindow` / `GetRange` / `LambdaQuery` are likely v1 surface |
| `pkg/query/grpc` | experimental | generic byte-encoded responses; `cmd/murmur-codegen-typed` emits per-service typed `.proto` + Go server stubs (sum / hll / topk / bloom; get_all_time / get_window / get_window_many / get_many / get_range) over `pkg/query/typed` clients |
| `pkg/query/typed` | experimental | typed-client wrappers over the generic QueryService — `SumClient`, `HLLClient`, `TopKClient`, `BloomClient`. All four expose `Get` / `GetMany` / `GetWindow` / `GetWindowMany` / `GetRange`. `GetMany` returns parallel value + present arrays so callers can distinguish "absent" from "present-and-empty"; `GetWindowMany` can't (the generic RPC merges before returning). The decoders + typed shape behind application-service typed-wrapper RPCs (see `examples/typed-wrapper`). Building block under `cmd/murmur-codegen-typed` |
| `pkg/admin` | experimental | CORS is closed by default; opt in via `WithAllowedOrigins`. Bearer-token (`WithAuthToken`, constant-time, multi-token for rotation) and JWT (`WithJWTVerifier`, BYO verifier) auth via a single middleware; auth is off by default — same-origin / network-isolated deploys keep the historical behavior. The `cmd/murmur-ui` binary exposes `--auth-token` + `MURMUR_ADMIN_TOKEN` env fallback |
| `pkg/swap` | mostly stable | small surface; integrated into `deploy/terraform/modules/pipeline-counter` via opt-in `swap_enabled` (control table + IAM + seed + `SWAP_CONTROL_TABLE` / `SWAP_ALIAS` env vars in every task definition) |
| `pkg/metrics` | mostly stable | only `streaming.Run` is wired today; bootstrap / replay / sources are not |
| `cmd/murmur-ui` | experimental | demo-grade dashboard; not yet a production ops surface |
| `cmd/murmur-codegen-typed` | experimental | YAML pipeline-spec → typed Connect-RPC `.proto` + Go server stub (delegates to `pkg/query/typed`). Sum / HLL / TopK / Bloom pipelines; method kinds `get_all_time` / `get_window` / `get_window_many` / `get_many` / `get_range`. TopK emits a `TopKItem { string key; int64 count; }` message plus `TopKItemList` when get_window_many or get_many is used; Bloom emits `(capacity_bits, hash_functions, approx_size, present)` plus `BloomShape` when get_window_many or get_many is used. All method kinds are now available on every pipeline kind — the Sum-only gate on `get_many` / `get_range` was lifted alongside the typed-client parity work |

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

`v1.0.0` will ship after PR 1–4 land and the framework has been exercised against
real (non-`local`) AWS for at least one full quarter.
