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
`bot_interactions`, with two RPCs:

- `GetCount(bot_id, user_id) → (value, present)` — all-time count
- `GetCountWindow(bot_id, user_id, duration_seconds) → (value, present)` — last N seconds

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
| `service.methods[].kind` | yes | `get_all_time` or `get_window` |
| `service.methods[].request` | yes | Field list (name + type). Supported types: `string`, `int64` |
| `service.methods[].key_template` | yes | Printf-style template with `{field}` references; emits `fmt.Sprintf("...")` over request fields |
| `service.methods[].window_duration_field` | for `get_window` | Name of the int64 request field carrying the window duration in seconds |

## Response-message shape per pipeline kind

| Kind | Response fields |
|---|---|
| `sum` | `int64 value`, `bool present` |
| `hll` | `int64 value` (cardinality), `bool present` |
| `topk` | `repeated TopKItem items`, `bool present` — proto also defines `TopKItem { string key; int64 count; }` |
| `bloom` | `int64 capacity_bits`, `int32 hash_functions`, `int64 approx_size`, `bool present` |

## More examples

- **`bot-interactions/`** (Sum pipeline) — `BotInteractionCountService` with all-time and windowed RPCs over a `Sum[int64]` aggregator.
- **`top-products/`** (TopK pipeline) — `TopProductsService` returning ranked Misra-Gries items per category, all-time and windowed.
- **`recent-visitors/`** (Bloom pipeline) — `RecentVisitorsService` exposing a Bloom filter's structural metadata per campaign.

## What this does NOT cover

- **`get_window_many` (multi-key window)** — `pkg/query/typed.SumClient` supports it via `GetWindowMany`; the codegen does not yet template it. Hand-write that one method alongside the generated ones if you need it. (Adding it to the codegen requires the typed clients for HLL / TopK / Bloom to gain the same method; tracked.)
- **`buf` / `protoc` invocation** — run those yourself with whichever plugin versions you've pinned.

These are roadmap gaps, not architectural ones; lift them off `cmd/murmur-codegen-typed/server.go` template when needed.
