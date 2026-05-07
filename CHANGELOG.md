# Changelog

All notable changes to Murmur are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
