# Contributing to Murmur

Welcome — and thanks for considering a contribution. Murmur is pre-1.0
and actively evolving, but the contribution loop is conventional Go +
React project shape: fork, branch, change, `make ci`, PR.

This doc is the practical contributor's guide. For the project's
architectural shape, read [`AGENTS.md`](AGENTS.md) (canonical
agent-instructions, equally readable as human onboarding) and
[`doc/architecture.md`](doc/architecture.md). For "what shape do I
build for X," see [`doc/use-cases.md`](doc/use-cases.md).

## Code of conduct

By participating in this project you agree to abide by the
[Contributor Covenant 2.1](CODE_OF_CONDUCT.md). Reports go to
`conduct@gallowaysoftware.ca`.

## Security

Don't open public issues for vulnerabilities — email
`security@gallowaysoftware.ca`. The full policy is in
[`SECURITY.md`](SECURITY.md).

## Where to start

- **First time here:** skim the [`README.md`](README.md) status table,
  then [`AGENTS.md`](AGENTS.md) for the load-bearing concepts.
- **Looking for a bug to bite into:** check open issues labeled
  `good first issue` and `help wanted`. Things tagged with a package
  path (`pkg/xxx`) tell you which file tree to read.
- **Have a use-case Murmur doesn't fit yet:** open a Feature Request
  issue with the [template](.github/ISSUE_TEMPLATE/feature_request.md)
  *before* writing code. Architectural changes need a quick alignment
  conversation first.
- **Fixing something small:** PRs without prior discussion are
  welcome for typos, doc cleanup, lint fixes, dependency bumps,
  obvious bug fixes with a failing test.

## Local setup

### Required

- **Go 1.26** (see `go.mod`).
- **Docker** + `docker compose v2` — for the integration test stack.
- **Node 20+** and **npm 10+** — for `web/`.

### Recommended

- **`buf`** CLI — only if you'll touch `.proto` files. The web build
  generates TS bindings automatically via `npx buf generate`; the Go
  side needs `make proto`.
- **`golangci-lint v2`** — the CI lint config in `.golangci.yml`.
  Install via `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`.
- **`terraform`** ≥ 1.5 — only if you're touching
  `deploy/terraform/`.

### First-time bootstrap

```sh
git clone https://github.com/gallowaysoftware/murmur
cd murmur

# Go side
go mod download
(cd pkg/exec/batch/sparkconnect && go mod download)

# Web side
cd web && npm ci && cd ..

# Sanity check
make ci          # fmt-check + vet + test-unit; should be green on main
```

## Development loop

```sh
# Daily flow:
make fmt          # gofmt -w
make ci           # CI gate (fmt-check + vet + test-unit); ~30s
make lint         # golangci-lint, slower

# Targeted tests:
go test -run TestE2E_CounterPipeline ./test/e2e/...
go test -short -run TestInt64SumStore_AtomicAdd ./pkg/state/dynamodb/...

# Integration tests (needs docker compose stack):
make compose-up   # kafka, dynamodb-local, valkey-bundle, mongo, minio, spark-connect
make test-integration
make compose-down

# Web:
cd web && npx buf generate && npm run lint && npx tsc -b --noEmit && npm run build
```

`make help` lists every Make target. The Makefile is the source of
truth — if a workflow isn't in `make help`, it's not a documented
path.

## What kinds of contributions are welcome

- **Bug fixes** — please include a failing test if at all possible.
- **New monoids** — drop into `pkg/monoid/{core,sketch,compose}/`,
  add a case to `pkg/monoid/monoidlaws/laws_test.go` (CI fuzzes
  associativity + identity). Expensive `Combine` monoids (Bloom-shape)
  should pass `WithSamples(8)` to fit the CI `-race + 2m` budget.
- **New sources / state stores / replay drivers** — mirror the
  existing implementations: `OnDecodeError` / `OnFetchError`
  callback, `metrics.Recorder` plumbed through, retries with bounded
  backoff. No swallowed errors (`_ = err` is a smell; flag the rare
  cases where it's correct with a comment).
- **New runtimes (e.g. another Lambda trigger source)** — plumb
  through [`pkg/exec/processor`](pkg/exec/processor/) rather than
  re-implementing retry / dedup / metrics.
- **Doc improvements** — particularly worked-example walkthroughs in
  `doc/` or `examples/`. The bar is "could I follow this end-to-end
  without already knowing Murmur's shape?"
- **Operational tooling** — Terraform modules, autoscale signals,
  observability integrations.

## What we'll push back on

- **New direct dependencies without discussion.** Murmur ships with
  a small dep tree on purpose. Flag any addition in the PR
  description with the justification.
- **Premature abstractions.** Three similar lines is better than a
  premature `interface` with two implementations. Frameworks-on-
  frameworks turn into Beam.
- **Code that bypasses the Kind dispatch.** If you find yourself
  type-switching on a monoid implementation instead of adding a
  `Kind`, talk to us — there's almost always a better shape.
- **Behavior changes to the streaming runtime that aren't
  benchmarked.** The processor hot path is sub-microsecond
  (`processor.MergeOne` 76 ns/op, 0 allocs). Don't regress it
  without a measurement showing the regression is paid back somewhere.

## PR process

1. **Branch off `main`.** Name it whatever; we don't enforce a
   prefix convention. The dependabot pattern (`<scope>-<change>`)
   is fine.
2. **Make small, reviewable commits** — each commit should pass
   `make ci` on its own when possible. Squashing is fine if your
   intermediate commits are noisy, but readable per-commit history
   is preferred.
3. **Open the PR** against `main`. The
   [PR template](.github/pull_request_template.md) prompts for
   *Summary / Test plan / Notes for reviewers / Pre-flight*; fill
   it in honestly. "Tested locally" is fine for small changes; for
   anything touching production paths, the test plan should list
   what you actually ran.
4. **CI must pass** — the workflow runs gofmt / go vet / unit tests
   (`-race`, `-timeout 120s`) / golangci-lint / web tsc / web lint
   / web build. Flakes happen; if you see one re-run before
   investigating.
5. **Address review feedback.** Adversarial code review is part of
   the design culture here ([`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
   covers the bar). Disagreement is fine; talk through it.
6. **Merge.** Maintainers merge with `--merge` (preserves the PR
   boundary) for multi-commit PRs, or `--squash` for single-commit
   PRs. We avoid `--rebase` to keep main's history readable.

### Pre-flight checklist

- [ ] `make ci` passes locally.
- [ ] If a new monoid: registered in
      `pkg/monoid/monoidlaws/laws_test.go`.
- [ ] If a new public API: a row in
      [`STABILITY.md`](STABILITY.md) marking it experimental, and
      an entry in [`CHANGELOG.md`](CHANGELOG.md) under
      `[Unreleased]`.
- [ ] If a breaking change (pre-v1): noted in `CHANGELOG.md`
      `[Unreleased]` under a `### Breaking changes` heading.
- [ ] If a new direct dependency: flagged in the PR description
      with the why.
- [ ] If touching `examples/`: the example still runs end-to-end
      (verified locally; the docker-compose stack helps).
- [ ] If touching `web/`: `npx buf generate && npm run build`
      succeeds. The CI runs this; double-check `lint` too.

### Commit message format

Conventional shape, not religious:

```
short imperative subject (≤ 72 chars)

Body explaining *why* the change is needed, not what it does (the
diff shows what it does). Wrap at ~72 cols.

For multi-part changes, bullet the pieces:
- one thing
- another thing
```

Co-authorship via the `Co-Authored-By:` trailer is welcome (humans,
agents, paired sessions).

## Conventions

These come from `AGENTS.md` and `.golangci.yml`. The short version:

- **Test names** follow `Test<Subject>_<Behavior>` (e.g.
  `TestInt64SumStore_AtomicAdd`).
- **Package doc** explains the *why*; per-symbol doc opens with the
  symbol name (revive's `exported` rule enforces this).
- **No `_ = err`** unless there's a comment explaining why the error
  is non-fatal. `pkg/exec/processor` is the canonical retry / dedup
  surface for runtimes.
- **Lint config** is `golangci-lint v2` with `errcheck`, `govet`,
  `ineffassign`, `revive`, `staticcheck`, `unused`, `unparam`,
  `misspell`, `gocritic`. Test files and `proto/gen/` get relaxed
  rules.
- **Don't break `make ci`.** If you need to (a deliberate breaking
  change, a new dep, a config tweak), flag it loudly in the PR.

## Stability and the CHANGELOG

Murmur is pre-1.0. Public API surface is in flux:

- New public symbols → [`STABILITY.md`](STABILITY.md) row marking
  them `experimental`.
- Promoting from experimental → `mostly stable` requires:
  - The API has been in use for ≥ one quarter without a breaking
    change.
  - There's at least one runnable example in `examples/`.
  - There are unit tests + at least one e2e test.
- Breaking changes pre-v1 land under
  [`CHANGELOG.md`](CHANGELOG.md)'s `[Unreleased]` → `### Breaking
  changes` block. Don't break things silently.

## Licensing

Murmur is Apache 2.0. By submitting a contribution you agree to
license it under the same terms; the `Co-Authored-By:` trailer at
the bottom of the commit message is fine for attribution.

## Maintainers

This project is maintained by Galloway Software. Issues, PRs, and
discussion happen on GitHub. For private security disclosures, use
`security@gallowaysoftware.ca` (see [`SECURITY.md`](SECURITY.md)).

## Questions

- For "how do I use Murmur for X" — open a Discussion (GitHub) or
  read [`doc/use-cases.md`](doc/use-cases.md) first.
- For "is this a bug" — open an issue using the
  [bug-report template](.github/ISSUE_TEMPLATE/bug_report.md).
- For "I want to contribute X" that touches more than a single file
  — open a Feature Request issue first so we can align on the shape
  before you build it.

Thanks again, and welcome.
