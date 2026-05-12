# Typed RPC codegen example

Demonstrates `cmd/murmur-codegen-typed`: a CLI that emits a typed
Connect-RPC service from a YAML pipeline-spec, on top of Murmur's
generic `QueryService`.

This is the **auto-generated** version of the manual pattern in
[`examples/typed-wrapper`](../typed-wrapper) — same output shape, no
hand-written wrapper code.

## What the codegen produces

Given a pipeline-spec YAML, the tool emits two files:

| File | Purpose |
|---|---|
| `<service>.proto` | Proto messages + `service` definition. The contract. Feed to `buf` or `protoc + connect-go-plugin` to generate typed Go message types and the Connect-RPC handler interface. |
| `<service>_server.go` | Go implementation of that interface. Delegates each RPC to the relevant `pkg/query/typed` client (`SumClient` for Sum pipelines, `HLLClient` for HLL). Imports the buf-generated proto types. |

The user owns the proto pipeline (buf, protoc, plugin versions). The
codegen does not invoke them. After running the codegen, the typical
build sequence is:

```
go run ./cmd/murmur-codegen-typed --in pipeline-spec.yaml --out gen/
buf generate gen/<service>.proto                # (or protoc equivalent)
go build ./...                                  # _server.go now compiles
```

## Worked example: `bot-interactions/`

```
bot-interactions/
├── pipeline-spec.yaml          # input
└── _expected/                  # checked-in golden output (underscore prefix
                                 # so Go tooling skips it — the *_server.go
                                 # imports the user's buf-generated proto)
    ├── bot_interaction_count_service.proto
    └── bot_interaction_count_service_server.go
```

Spec defines `BotInteractionCountService` over a Sum pipeline named
`bot_interactions`, with five RPCs that demonstrate every method-kind:

- `GetCount(bot_id, user_id) → (value, present)` — all-time count
- `GetCountWindow(bot_id, user_id, duration_seconds) → (value, present)` — last N seconds
- `GetCountWindowMany(bot_id, user_ids[], duration_seconds) → values[]` — batched windowed read over N users
- `GetCountMany(bot_id, user_ids[]) → (values[], present[])` — batched all-time read with per-entity present flag
- `GetCountRange(bot_id, user_id, start_unix, end_unix) → value` — merged value over an absolute time range

Run:

```
go run ./cmd/murmur-codegen-typed \
    --in examples/typed-rpc-codegen/bot-interactions/pipeline-spec.yaml \
    --out examples/typed-rpc-codegen/bot-interactions/_expected/
```

(Re-running with the same `--out` regenerates the expected files; the
checked-in copies under `_expected/` are what
`cmd/murmur-codegen-typed/codegen_test.go` golden-compares against.)

## Spec schema

| Field | Required | Notes |
|---|---|---|
| `proto_package` | yes | Proto package (e.g. `example.bot.v1`) |
| `go_package` | yes | Go import path for the generated server stub |
| `proto_go_package` | no | Go import path for the buf-generated proto types; defaults to `go_package` |
| `service.name` | yes | Service name in the generated proto (UpperCamelCase) |
| `service.pipeline_name` | yes | Murmur pipeline name (used by typed-clients to route to the right gRPC endpoint) |
| `service.pipeline_kind` | yes | `sum`, `hll`, `topk`, or `bloom` |
| `service.methods[].name` | yes | RPC method name (UpperCamelCase) |
| `service.methods[].kind` | yes | `get_all_time` / `get_window` / `get_window_many` / `get_many` / `get_range` |
| `service.methods[].request` | yes | Field list (name + type). Supported types: `string`, `int64`, `repeated string` |
| `service.methods[].key_template` | yes | Printf-style template with `{field}` references; emits `fmt.Sprintf("...")` over request fields. For batched kinds, must reference the `many_key_field`. |
| `service.methods[].window_duration_field` | for `get_window` / `get_window_many` | Name of the int64 request field carrying the window duration in seconds |
| `service.methods[].many_key_field` | for `get_window_many` / `get_many` | Name of the `repeated string` request field holding the per-entity values to batch over |
| `service.methods[].range_start_field` / `range_end_field` | for `get_range` | Names of the int64 request fields carrying Unix-second start / end timestamps |

`get_many` and `get_range` are Sum-only (validated at codegen time) — the typed clients in `pkg/query/typed` only expose those methods on `SumClient`.

## Response-message shape per (pipeline kind, method kind)

Singleton reads (`get_all_time` / `get_window`):

| Kind | Response fields |
|---|---|
| `sum` | `int64 value`, `bool present` |
| `hll` | `int64 value` (cardinality), `bool present` |
| `topk` | `repeated TopKItem items`, `bool present` — proto also defines `TopKItem { string key; int64 count; }` |
| `bloom` | `int64 capacity_bits`, `int32 hash_functions`, `int64 approx_size`, `bool present` |

Batched windowed reads (`get_window_many`):

| Kind | Response fields |
|---|---|
| `sum` / `hll` | `repeated int64 values` (no `present` — `pkg/query/typed.{Sum,HLL}Client.GetWindowMany` doesn't surface per-entity presence) |
| `topk` | `repeated TopKItemList entries` — proto defines `TopKItemList { repeated TopKItem items; }` (proto3 disallows repeated-of-repeated) |
| `bloom` | `repeated BloomShape entries` — proto defines `BloomShape { int64 capacity_bits; int32 hash_functions; int64 approx_size; }` |

Sum-only reads:

| Kind | Response fields |
|---|---|
| `sum` / `get_many` | `repeated int64 values`, `repeated bool present` (per-entity present flag distinguishes "absent" from "present-and-zero") |
| `sum` / `get_range` | `int64 value` (merged across the buckets in the absolute range) |

## More examples

- **`bot-interactions/`** (Sum pipeline) — `BotInteractionCountService` covering all five method kinds (`GetCount`, `GetCountWindow`, `GetCountWindowMany`, `GetCountMany`, `GetCountRange`).
- **`top-products/`** (TopK pipeline) — `TopProductsService` with all-time, windowed, and windowed-many ranked Misra-Gries items per category.
- **`recent-visitors/`** (Bloom pipeline) — `RecentVisitorsService` exposing Bloom-filter structural metadata, all-time and windowed-many.

## What this does NOT cover

- **`GetMany` / `GetRange` for HLL / TopK / Bloom** — those methods don't exist on `pkg/query/typed.{HLL,TopK,Bloom}Client` yet, so the codegen rejects `get_many` / `get_range` for non-Sum pipelines at validate time. When the typed clients grow them, lift the `serviceKind != PipelineSum` restriction in `cmd/murmur-codegen-typed/spec.go`.
- **`GetRangeMany`** — no typed-client wrapper exists yet for any pipeline kind. Hand-write against the generic `pkg/query/grpc` if needed.
- **`buf` / `protoc` invocation** — run those yourself with whichever plugin versions you've pinned.
