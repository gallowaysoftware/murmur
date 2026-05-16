# AGENTS.md

Repository-wide guidance for AI coding agents working in this codebase
(Claude Code, Cursor, Aider, Codex, OpenCode, etc). This file follows
the [agents.md](https://agents.md) convention and is the canonical
source of truth; tool-specific files like `CLAUDE.md` link back to it.

## What this is

Murmur is a lambda-architecture-aware streaming aggregation framework
for Go (spiritual successor to Twitter's Summingbird). One pipeline
DSL → three execution modes (live stream / bootstrap / replay) →
monoid-typed state in DynamoDB with optional Valkey cache → generic
gRPC query layer that merges across time windows. Pre-1.0; status &
roadmap in `README.md`, sharp edges in `STABILITY.md`, full design in
`doc/architecture.md`, deep design treatise in `doc/design.md`,
worked use cases in `doc/use-cases.md`.

## Module layout

Multi-module workspace (`go.work`):

- **Root module** (`github.com/gallowaysoftware/murmur`) — the bulk of
  the framework.
- **`pkg/exec/batch/sparkconnect`** — separate `go.mod` so non-Spark
  consumers don't pull `apache/spark-connect-go`. It uses a `replace`
  directive pointing at the `pequalsnp/spark-connect-go` fork;
  consumers who depend on this submodule must mirror that `replace`
  (Go doesn't propagate it transitively).

When running `go build`, `go vet`, `go test` against the whole tree,
also run them inside `pkg/exec/batch/sparkconnect`. The Makefile does
this via the `SUBMODULES` variable — prefer the Make targets.

## Commands

```sh
make help              # list all targets

# Tests
make test-unit         # fast unit tests, no infra (-short, 60s timeout)
make test-integration  # full E2E suite; brings up docker-compose first
                       # sets DDB_LOCAL_ENDPOINT, KAFKA_BROKERS,
                       # VALKEY_ADDRESS, VALKEY_BLOOM_ENABLED,
                       # S3_ENDPOINT, SPARK_CONNECT_REMOTE, MONGO_URI

# Single test (Go's normal -run pattern works)
go test -run TestE2E_CounterPipeline ./test/e2e/...
go test -short -run TestInt64SumStore_AtomicAdd ./pkg/state/dynamodb/...

# Lint / format (CI gate is `make ci` = fmt-check + vet + test-unit)
make fmt               # gofmt -w
make fmt-check         # non-mutating, fails if anything isn't gofmt'd
make vet               # go vet (including submodules)
make lint              # vet + fmt-check + golangci-lint (v2 config in .golangci.yml)

# Web UI (React + Vite, in web/, embedded into pkg/admin via embed.FS)
make web-build         # tsc + vite build, copies dist into pkg/admin/dist
make web-typecheck     # tsc --noEmit
make web-lint          # eslint
make ui                # web-build + go build + run cmd/murmur-ui --demo on :8080

# Docker stack (kafka, dynamodb-local, valkey-bundle, mongo, minio, spark-connect)
make compose-up        # also runs scripts/init-mongo-replset.sh
make compose-down
make seed-ddb          # create the `page_views` table for the example

# Proto codegen (buf v2; managed mode rewrites go_package to proto/gen/...)
make proto-tools       # installs protoc-gen-go / -go-grpc / -connect-go
make proto             # regenerate Go bindings under proto/gen/
make web-proto         # regenerate TS bindings under web/src/gen/
```

E2E tests in `test/e2e/` skip themselves when the relevant env vars
(`KAFKA_BROKERS`, `DDB_LOCAL_ENDPOINT`, `MONGO_URI`, `S3_ENDPOINT`,
`SPARK_CONNECT_REMOTE`, `VALKEY_BLOOM_ENABLED`) are unset, so
`make test-unit` is genuinely infra-free.

## Architecture: the load-bearing ideas

You can navigate the code mechanically, but four cross-cutting concepts
let you predict where things live and how packages compose.

### 1. Structural monoids dispatch on `Kind`

`pkg/monoid/monoid.go` defines `Monoid[V]` with `Identity()`,
`Combine(a, b)`, and `Kind()`. The `Kind` constants (`KindSum`,
`KindHLL`, `KindTopK`, `KindBloom`, …) are the joint between abstract
aggregation and backend dispatch:

- DynamoDB store picks atomic `ADD` vs `CAS` based on Kind.
- Spark / Valkey backends pick native aggregations / sketch commands
  by Kind.
- The admin server + `cmd/murmur-codegen-typed` pick typed decoders
  by Kind.
- `KindCustom` is the opaque-Go-closure escape hatch; only Go
  executors run it.

Adding a new well-known monoid: add a `Kind` constant in
`pkg/monoid/monoid.go`, implement it under
`pkg/monoid/{core,sketch,compose}/`, add a case to
`pkg/monoid/monoidlaws/laws_test.go` (associativity + identity get
fuzzed in CI), and add backend dispatch cases anywhere there's a Kind
switch.

### 2. One DSL, three execution modes via `pkg/exec/processor`

`pkg/pipeline.Pipeline[T, V]` is execution-mode-agnostic. The
runtimes are:

- `pkg/exec/streaming` — long-running worker (Kafka, Kinesis).
- `pkg/exec/bootstrap` — one-shot scan of a source-of-truth datastore.
- `pkg/exec/replay` — re-ingest from a Kafka offset range or
  S3 archive.
- `pkg/exec/lambda/{kinesis,dynamodbstreams,sqs}` — Lambda handler
  entry points.

All of them share `pkg/exec/processor` for retry / dedup / metrics /
BatchItemFailures semantics. `processor.MergeOne` is the canonical
entry point for out-of-tree drivers. When adding a new runtime, plumb
through `processor` rather than re-implementing those concerns.

The DSL surface (verbose) is `pkg/pipeline`; the facade for the 90%
case is `pkg/murmur` (`Counter`, `UniqueCount`, `TopN`, `Trending`
presets + `RunStreamingWorker` / `Must*Handler` wrappers).

### 3. DDB is source of truth; Valkey is a cache, never trusted

`pkg/state/state.go` defines `Store[V]` (durable) and `Cache[V]`
(best-effort). `pkg/state/dynamodb` provides:

- `Int64SumStore` — atomic `UpdateItem ADD`
- `Int64MaxStore` — conditional `UpdateItem` (SetCountIfGreater
  pattern, out-of-order-safe)
- `BytesStore` — CAS-with-retry for sketch state
- `Deduper` — atomic PutItem-with-condition + native TTL; wire via
  `streaming.WithDedup(d)` to make replays idempotent for
  non-idempotent monoids (Sum, HLL, TopK)

`pkg/state/valkey` provides `Int64Cache` (INCRBY), `BytesCache` (RMW
for sketches), `HLLCache` (Valkey-native PFADD/PFCOUNT/PFMERGE
accelerator), and `BloomCache` (BF.ADD/BF.MADD/BF.EXISTS/BF.MEXISTS
accelerator; requires valkey-bloom or RedisBloom module). The
invariant: anything in Valkey must be repopulatable from DDB. Sketch
accelerators run side-by-side with the BytesStore-authoritative
sketches — they're independent estimators, both within the monoid's
error bound.

`pkg/state.NewInstrumented` / `NewInstrumentedCache` are zero-overhead
decorators that plumb `metrics.Recorder` hooks (latency + errors)
around any store/cache.

### 4. Windowed monoids + the generic query layer

`pkg/monoid/windowed.Config` adds a time-bucket dimension (daily /
hourly / minute) to state keys. The same `Counter`, `UniqueCount`,
etc. work windowed and all-time. Queries assemble sliding windows by
merging the N most-recent buckets via the monoid's `Combine`.

The query layer is generic over Kind, hence the `Value{bytes}` shape
today:

- `pkg/query` — `Get` / `GetMany` / `GetWindow` / `GetRange` /
  `GetWindowMany` / `GetRangeMany`, plus `LambdaQuery` for batch-view
  ⊕ realtime-delta merge.
- `pkg/query/grpc` — Connect-RPC server on a single port speaks gRPC
  + gRPC-Web + Connect (HTTP+JSON). Has singleflight coalescing,
  per-RPC metrics, `fresh_read` flag.
- `pkg/query/typed` — typed-client wrappers (`SumClient`, `HLLClient`,
  `TopKClient`, `BloomClient`) that decode the generic bytes for
  application code. Every client supports `Get` / `GetMany` /
  `GetWindow` / `GetWindowMany` / `GetRange`.
- `cmd/murmur-codegen-typed` — YAML pipeline spec → typed Connect-RPC
  `.proto` + Go server stub that delegates to `pkg/query/typed`.
  Supports Sum / HLL / TopK / Bloom pipelines with method kinds
  `get_all_time` / `get_window` / `get_window_many` / `get_many` /
  `get_range` (every method kind on every pipeline kind). The
  generated server is the typed alternative to the generic-bytes
  `pkg/query/grpc.Server`.

## Where things live (when you can't grep your way)

- Admin / control plane: `pkg/admin` (Connect-RPC server, embeds the
  web UI from `pkg/admin/dist`). CORS is closed by default — pass
  `admin.WithAllowedOrigins(...)` or `cmd/murmur-ui --allow-origin=...`.
  Auth is off by default — pass `admin.WithAuthToken(...)` /
  `admin.WithJWTVerifier(...)` or `cmd/murmur-ui --auth-token=...`
  (`MURMUR_ADMIN_TOKEN` env fallback) for public-internet exposure.
- Proto definitions: `proto/murmur/v1/query.proto`,
  `proto/murmur/admin/v1/admin.proto`. Generated Go at `proto/gen/`,
  generated TS at `web/src/gen/`.
- Observability: `pkg/observability/autoscale` for scaling-signal
  emission (reference CloudWatch emitter; `EventsPerSecond` helper).
  `pkg/metrics` for the `Recorder` interface that stores / caches /
  streaming hook into.
- Projector pattern: `pkg/projection`
  (`LogBucket`/`LinearBucket`/`ManualBucket` + `HysteresisBucket`) for
  change-data-capture into search indices. Used in
  `examples/search-projector/`.
- Atomic state swap: `pkg/swap` (alias-version pointer; integrated
  into `deploy/terraform/modules/pipeline-counter` via opt-in
  `swap_enabled = true`).
- Examples worth copying patterns from: `examples/page-view-counters/`
  (full worker+query+Dockerfile+Terraform),
  `examples/mongo-cdc-orderstats/` (bootstrap → CDC handoff),
  `examples/recently-interacted-topk/` (one pipeline, two sources
  writing same DDB row), `examples/typed-wrapper/` (application-service
  typed RPC over `pkg/query/typed`),
  `examples/backfill-from-spark/` (Spark-produced Parquet bootstrap),
  `examples/search-projector/` + `examples/search-rerank/` (counters
  ⊕ text search).

## Conventions to follow

From `CONTRIBUTING.md` + `.golangci.yml`:

- Test names: `Test<Subject>_<Behavior>` (e.g.
  `TestInt64SumStore_AtomicAdd`).
- New monoid → drop into `pkg/monoid/monoidlaws/laws_test.go` so CI
  exercises associativity + identity. FP monoids need `WithEqual` for
  a tolerance comparator. Expensive Combine monoids (Bloom) should
  pass `WithSamples(8)` to fit the CI `-race + 2m` budget.
- New source / state store / replay driver → mirror existing
  implementations: `OnDecodeError` / `OnFetchError` callback,
  `metrics.Recorder` plumbed through, retries with bounded backoff,
  no `_ = err`.
- New public API → add a row to `STABILITY.md` marking it
  experimental.
- Breaking change pre-v1 → note under `[Unreleased]` in
  `CHANGELOG.md`.
- Don't add new direct dependencies without flagging in the PR
  description.
- Package doc explains the *why*; per-symbol doc opens with the
  symbol name.
- `golangci-lint` v2 with `errcheck`, `govet`, `ineffassign`,
  `revive`, `staticcheck`, `unused`, `unparam`, `misspell`,
  `gocritic`. Test files and `proto/gen/` have relaxed rules.

## Gotchas

- The `replace` directive for `pequalsnp/spark-connect-go` lives only
  in `pkg/exec/batch/sparkconnect/go.mod`. Code in the root module
  cannot import the sparkconnect submodule directly — it would
  re-introduce the `apache/spark-connect-go` dependency the split was
  meant to avoid.
- `go list ./...` walks `web/node_modules` if it has Go files; the
  Makefile filters this via `GO_PACKAGES`. If you're invoking `go test
  ./...` directly, use the `make` target or replicate the filter.
- Kinesis production path is **AWS Lambda**
  (`pkg/exec/lambda/kinesis`), not ECS — the Lambda event-source
  mapping owns shard discovery, leasing, autoscaling, checkpointing,
  and partial-batch retry via `BatchItemFailures`. The polling
  `pkg/source/kinesis` is dev / demo only (single-instance, no
  checkpointing). KCL v3 Go is deliberately NOT in-tree.
- Streaming runtime is single-goroutine per worker by default.
  `streaming.WithConcurrency(N)` distributes via key-hash routing
  in-process; `kafka.Config.Concurrency` parallelizes decode at the
  source (per-partition pinned). Scale horizontally with Kafka
  partitions otherwise (~5–10k events/s/worker against DDB-local).
- Default semantics are at-least-once. For non-idempotent monoids
  (Sum, HLL, TopK) you must pair with `streaming.WithDedup(d)`
  (DDB-backed) or replays will double-count. Idempotent monoids (Set,
  Min, Max, Bloom) are fine without it.
- The web build needs `web/src/gen/` (gitignored, regenerated by
  `npx buf generate`). CI runs the generate step before `tsc`; for
  local hacks, run `make web-proto` first.
- `BloomCache` integration tests need the valkey-bloom module loaded
  into Valkey. The docker-compose stack ships `valkey/valkey-bundle:8`
  which includes it; bare `valkey/valkey:8-alpine` will skip those
  tests (`VALKEY_BLOOM_ENABLED` env-gated).
