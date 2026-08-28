# Security policy

## Supported versions

Murmur is **pre-1.0**. Only the `main` branch receives security fixes; no
back-porting. Once `v1.0.0` ships, the latest two minor versions will be
supported.

## Reporting a vulnerability

Please do **not** open a public GitHub issue.

Email `security@gallowaysoftware.ca` with:

- A description of the issue and where it occurs (file paths or the
  reproduction).
- The Murmur revision/SHA you reproduced against.
- The impact you believe it has.

We aim to acknowledge within 3 business days and to ship a fix or coordinated
disclosure plan within 14 days. If the fix is non-trivial we'll keep you in the
loop on the timeline.

## Known sharp edges

[`STABILITY.md`](STABILITY.md) tracks correctness and operational hazards that
are publicly known. The "Known sharp edges" list there enumerates issues that
are not strictly *security* vulnerabilities but can still cause data loss or
operational failure if an integrator isn't aware of them — most notably:

- `pkg/admin` and `pkg/query/grpc` ship with **auth off by default**. CORS is
  closed by default (no headers at all unless you pass
  `admin.WithAllowedOrigins`), but the admin API is still unauthenticated
  unless you configure `admin.WithAuthToken` or `admin.WithJWTVerifier` — and
  it leaks pipeline metadata. Do not expose either to the public internet
  without auth in front.
- The `replace` directive in **`pkg/exec/batch/sparkconnect/go.mod`** pinning
  `apache/spark-connect-go` to a fork. If you import that submodule you must
  mirror the `replace` in your own `go.mod` — Go does not propagate replace
  directives transitively — which means you are depending on the fork
  deliberately. Audit it yourself before doing so.

## Scope

In scope: vulnerabilities in `pkg/...`, `cmd/...`, the embedded web UI under
`web/`, the Terraform module, and the docker-compose stack used by the test
suite. Out of scope: third-party dependencies (please report upstream); local
development convenience (e.g. dynamodb-local on a laptop).
