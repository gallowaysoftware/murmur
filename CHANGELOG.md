# Changelog

All notable changes to Murmur are recorded here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Honest `STABILITY.md` matrix marking each package experimental / mostly stable.
- README "Limitations to read before adopting" section calling out the
  `replace` directive, single-goroutine streaming runtime, Min/Max identity
  hazard, permissive CORS, and Kinesis lack of checkpointing.
- README "Quick taste" snippet now uses the recommended `pkg/murmur` facade and
  actually compiles.

### Changed

- README status table marks Min/Max ⚠️ instead of ✅ until the `Set`-sentinel
  fix lands; Kinesis source ⚠️ pending KCL upgrade; gRPC service described
  honestly as a generic adapter rather than "auto-generated."

### Removed

- The duplicate "Lambda mode" row in the status table.

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
