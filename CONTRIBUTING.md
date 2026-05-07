# Contributing to Murmur

Thanks for your interest. Murmur is pre-1.0 and the public API is shifting; if
you're planning a non-trivial change, please open an issue first so we can
align on shape before you spend time on a PR.

## Quick start

```sh
git clone https://github.com/gallowaysoftware/murmur
cd murmur

# All-in-one bring-up (docker, mongo replset init, table seed):
make compose-up
make seed-ddb

# Run the unit tests (no infra needed):
make test-unit

# Run the full E2E suite (requires the docker-compose stack):
make test-integration

# Run the dashboard with synthetic data:
make ui   # opens at http://localhost:8080
```

## Before sending a PR

- `make ci` passes (gofmt, vet, unit tests). CI runs the same gate plus
  `golangci-lint` and the web `tsc`/`lint`/`build`.
- New monoid? Add a case to `pkg/monoid/monoidlaws/laws_test.go` so the
  associativity + identity laws are exercised in CI. If your monoid uses
  floating point, supply a tolerance comparator via `WithEqual`.
- New source / state store / replay driver? Mirror the patterns in the
  existing implementations: `OnDecodeError` callback, `metrics.Recorder` plumbed
  through, retries with bounded backoff, no `_ = err`.
- New public API? Update `STABILITY.md` with a row marking it experimental.
- Breaking change before v1.0? Mention it in `CHANGELOG.md` under `[Unreleased]`.

## Style

- `gofmt -s` formatting, `go vet` clean. CI enforces.
- Follow the existing godoc style: package doc explains the *why*; per-symbol
  doc opens with the symbol name.
- Test names are `Test<Subject>_<Behavior>`: `TestInt64SumStore_AtomicAdd`,
  `TestLambda_GetMissingInDelta`.
- Don't add new direct dependencies without flagging them in the PR
  description; we'd rather wrap a focused stdlib path than pull a heavy
  transitive tree.

## Reporting bugs

Open a [GitHub issue](https://github.com/gallowaysoftware/murmur/issues) with:

- The smallest reproducer you can produce (a failing test is ideal).
- The output of `go env`, `docker compose version`, and (if relevant) the
  AWS SDK / Spark / Mongo versions you're running against.
- What you expected vs. what happened.

For security issues, see [SECURITY.md](SECURITY.md) — please don't file a public
issue.

## Licensing

Murmur is Apache 2.0. By submitting a contribution you agree to license it
under the same terms (the `Co-Authored-By` line at the bottom of the commit
message is fine).
