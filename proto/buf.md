# Murmur protocol contracts

This module is the source of truth for every wire-level interaction Murmur
exposes. Two services live here:

| Service | Plane | Used by |
|---|---|---|
| `murmur.admin.v1.AdminService` | control / dashboard | `cmd/murmur-ui`, the embedded React app, any custom dashboard |
| `murmur.v1.QueryService` | application data plane | application services that read aggregated state |

Both services are served via [Connect-RPC](https://connectrpc.com), so every
endpoint speaks **gRPC**, **gRPC-Web**, and **Connect (HTTP+JSON)** on a single
port. Pick whichever your client supports — there is no transport-specific
endpoint.

## Generating clients

The Murmur repo ships pre-generated bindings for two languages so the project
stays self-contained:

- **Go** — `proto/gen/murmur/...`, regenerated via `make proto`.
- **TypeScript** — `web/src/gen/murmur/...`, regenerated via `make web-proto`.

For any other language, point [`buf generate`](https://buf.build/docs/generate)
at this directory with the language-appropriate plugin (Java / Kotlin / Rust /
Python / Swift / Dart / etc.). Connect-RPC clients exist in most ecosystems;
for languages that only ship gRPC, the Connect server transparently downgrades.

A minimal `buf.gen.yaml` for a Java consumer:

```yaml
version: v2
plugins:
  - remote: buf.build/protocolbuffers/java
    out: gen
  - remote: buf.build/grpc/java
    out: gen
inputs:
  - module: github.com/gallowaysoftware/murmur//proto
```

## Conventions

These services follow the
[buf-STANDARD](https://buf.build/docs/lint/rules#standard) lint preset:

- **Per-RPC request and response messages.** Each RPC has its own request and
  response wrapper type (e.g. `GetStateResponse` rather than reusing
  `StateValue` directly). The wrapper allows independent schema evolution per
  endpoint without forcing coordinated rollouts on shared payloads.
- **Versioned packages.** `v1` lives in its own protobuf package; future
  breaking changes go in `v2` and run alongside `v1` for a deprecation window.
- **No required fields, no enums of unknown semantics.** Every field is
  optional in the wire sense; semantics are documented per field. New
  enum values must include `_UNSPECIFIED = 0`.
- **No streaming RPCs (yet).** Both services are unary today. Server-streaming
  for live metrics is a planned addition; clients should poll until then.

## Stability

Both services are pre-1.0 and may change. See the repo's `STABILITY.md`
for the per-package matrix. The `buf.yaml` `breaking` configuration is set
to `FILE` granularity so a future `buf breaking` check will block any
backwards-incompatible change at PR time.

## Browsing

Clone the repo or browse the .proto files directly on
[GitHub](https://github.com/gallowaysoftware/murmur/tree/main/proto/murmur).
