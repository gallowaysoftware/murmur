# Example: typed wrapper around Murmur's QueryService

A count-core-shaped reference implementation of the **typed-wrapper pattern**. Murmur's QueryService returns generic `Value{bytes}`; application services wrap it with their own typed Connect-RPC API so the bytes-decoding boilerplate doesn't leak into every caller.

## The pattern

```
Caller                      Application's typed proto              Generic Murmur proto
                            (your repo)                            (this repo)

ranker / ui  ──────────►   BotInteractionCountService.Get()  ──►  QueryService.Get()
                            returns int64                          returns Value{bytes}
                                  │                                       │
                                  │   Server is a thin wrapper:           │
                                  │   1. typed request → entity-key      │
                                  │   2. call Murmur via                 │
                                  │      pkg/query/typed.SumClient        │
                                  │   3. return typed response            │
```

The application owns its proto. Murmur stays monoid-agnostic. Per-pipeline typed-server codegen is on Murmur's Phase 2 roadmap; until then, this thin-wrapper pattern is the right shape for production.

## Files

- `wrapper.go` — `Server` struct that holds two `*typed.SumClient`s (one all-time, one windowed) and exposes a typed `BotInteractionService` interface. Real count-core code substitutes its actual proto types; the structure is identical.
- `wrapper_test.go` — exercises the wrapper end-to-end against an httptest-backed Murmur QueryService.

## Why this beats the bytes-everywhere alternative

- **Type safety at the call site.** Callers see `int64`, not `Value{Present, Data []byte}`. No accidental `binary.LittleEndian.Uint64` typos.
- **Separation of concerns.** The application's API is whatever shape it wants — count-core's `BotInteractionCountService` doesn't have to match Murmur's `QueryService`. Schema evolution is per-application.
- **Easy to test.** Tests against the typed shape don't have to construct wire bytes.
- **Decoder lives in `pkg/query/typed`, not at every call site.** When Murmur's wire format ever changes (Phase 2 codegen), the typed package absorbs the change; the wrapper code is unchanged.

## When to use

Anyone running a Murmur counter pipeline behind their own typed RPC API. The two production patterns:

1. **One Murmur pipeline → one typed RPC.** Simple case. Wrap once, expose the typed API.
2. **N Murmur pipelines → one typed RPC service.** count-core's case. The wrapper holds N typed clients (one per Murmur pipeline) and exposes a single application-service surface that fans out internally to the right pipeline per request.

## What this is NOT

- **Auto-codegen.** No tool here generates the typed proto from a pipeline definition. That's Phase 2 in `doc/architecture.md`. This example is the *building block* the codegen would emit; until codegen lands, you write the wrapper yourself (typically ~50 lines per service).
- **A different Murmur API.** Murmur's QueryService is unchanged. The typed wrapper is purely a client-side / application-side concern.

See [`pkg/query/typed`](../../pkg/query/typed/) for the underlying decoder + typed-client shipping.
