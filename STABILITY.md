# Stability

Murmur is **pre-1.0**. Public APIs may change without notice. This document tracks
where each package sits on the experimental → stable spectrum, and the known sharp
edges callers should plan around.

## Per-package status

| Package | Status | Notes |
|---|---|---|
| `pkg/pipeline` | experimental | DSL surface is likely to gain `Validate()` (renamed from `Build()`) and per-stage type narrowing |
| `pkg/murmur` | experimental | Builder presets are the recommended entry point; expect renames before v1 |
| `pkg/monoid/core` | mostly stable | `Min` / `Max` now use `Bounded[V]` for a proper Identity; lift inputs via `core.NewBounded(v)` |
| `pkg/monoid/sketch/{hll,topk,bloom}` | experimental | `Combine` returning the wrong operand on decode error is tracked; cross-runtime encoding portability not yet proven |
| `pkg/monoid/compose` | experimental | `MapMerge` / `Tuple2` / `DecayedSum`; FP-associativity caveats apply to `DecayedSum` |
| `pkg/monoid/windowed` | mostly stable | bucket math is solid; minute-granularity has high read-amplification on long ranges |
| `pkg/state` (interfaces) | mostly stable | `Store` / `Cache` interfaces unlikely to change before v1 |
| `pkg/state/dynamodb` | experimental | `BatchGetItem` does not yet retry `UnprocessedKeys`; CAS path lacks backoff |
| `pkg/state/valkey` | experimental | only `Int64Cache` ships; sketch-bytes cache is roadmap; no native PFADD path |
| `pkg/source/kafka` | experimental | poison pills are silently dropped (no DLQ hook yet); no per-partition parallelism |
| `pkg/source/kinesis` | experimental, single-instance | NO checkpointing, NO multi-instance leasing; KCL v3 upgrade is roadmap |
| `pkg/source/snapshot/mongo` | experimental | `extractID` is brittle for non-`_id` types beyond ObjectID/string/int |
| `pkg/replay/s3` | experimental | JSON Lines only; Parquet is roadmap |
| `pkg/exec/streaming` | experimental | single-goroutine; per-record retry + DLQ via WithMaxAttempts / WithDeadLetter; runtime no longer crashes on a transient store failure |
| `pkg/exec/bootstrap` | experimental | metrics integration not yet wired |
| `pkg/exec/replay` | experimental | metrics integration not yet wired |
| `pkg/exec/batch/sparkconnect` | experimental | depends on a `replace`d fork of `apache/spark-connect-go` |
| `pkg/query` | mostly stable | `Get` / `GetWindow` / `GetRange` / `LambdaQuery` are likely v1 surface |
| `pkg/query/grpc` | experimental | generic byte-encoded responses; per-pipeline codegen is roadmap |
| `pkg/admin` | experimental | CORS is permissive by default; no auth middleware |
| `pkg/swap` | mostly stable | small surface; the Terraform module does not yet integrate it |
| `pkg/metrics` | mostly stable | only `streaming.Run` is wired today; bootstrap / replay / sources are not |
| `cmd/murmur-ui` | experimental | demo-grade dashboard; not yet a production ops surface |

## Known sharp edges (priority order)

1. **Silent error paths.** Many `_ = err` sites across sources, caches, and sketch
   `Combine` swallow real failures. Tracked: PR-2 wires `metrics.Recorder` into
   bootstrap / replay / source layers and exposes poison-pill callbacks.

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

4. **Min/Max under empty/missing buckets.** `windowed.MergeBuckets` seeds the
   fold with `m.Identity()`, so a windowed `Min` over a partially-empty range
   reports `0` for those buckets — usually wrong.

5. **`go.mod` `replace` directive.** Importing
   `pkg/exec/batch/sparkconnect` requires consumers to mirror the
   `replace github.com/apache/spark-connect-go => github.com/pequalsnp/spark-connect-go ...`
   line. Tracked: upstream the patches or split the package into its own module.

6. **CORS.** `pkg/admin` ships `Access-Control-Allow-Origin: *` for local-dev
   convenience. Do not expose to the public internet without first wiring
   `WithAllowedOrigins([]string)` (tracked).

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
