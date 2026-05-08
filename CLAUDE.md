# CLAUDE.md

Guidance for working in this repository.

## What this project is

A Hazelcast storage backend for the Caddy [`cache-handler`](https://github.com/caddyserver/cache-handler) (Souin). It implements the `core.Storer` interface from `darkweak/storages/core` and registers a Caddy module with ID **`storages.cache.hazelcast`**.

Status: pre-alpha. Only Phase 1 / Step 1.1 (config + validation) currently has production code. See `implementation-plan.md` for the full 16-PR roadmap and `design-plan.md` for the architectural rationale.

**Always consult `implementation-plan.md` before adding code.** Each numbered step (`0.1`, `1.1`, `2.1`, ...) is a single PR with an explicit test list and a narrowly defined code surface. Do not expand scope beyond the step you are working on.

## Repo layout

Single Go module rooted at the repo. Two packages:

- `hazelcast/` — provider package (`core.Storer` implementation, config, client, eventually metrics).
- `caddy/` — Caddy module shim that registers `storages.cache.hazelcast` and parses the Caddyfile block.

Each is independently testable; the Caddy shim depends on `hazelcast/`, never the other way around.

## Locked technical decisions

These are pinned in `implementation-plan.md` — do not change without updating that doc and the README compatibility matrix in the same PR.

| Topic | Choice |
|---|---|
| Module ID | `storages.cache.hazelcast` |
| Caddy | v2.11.x |
| `caddyserver/cache-handler` | v0.16.0 |
| `darkweak/storages/core` | v0.0.19 |
| `hazelcast/hazelcast-go-client` | v1.5.0 |
| Go | 1.26 (language floor 1.25 in `go.mod`; lint analysis target 1.25) |
| Compression | lz4 only (matches upstream) |
| Storage primitive | Hazelcast `IMap` (AP). CP / `CPMap` epoch fencing is **deferred to v2**. |
| Integration tests | `testcontainers-go` against `hazelcast/hazelcast` 5.x, gated by `//go:build integration` |
| Vary / ETag / mapping | Use `core.MappingElection` and `core.MappingUpdater`. Do not reimplement. |
| TTL convention | Store with `ttl + provider.stale` (matches the badger provider). |

## TDD discipline

This project is TDD-first. Every PR ships tests-then-code, and `go test -race ./...` must pass before commit.

- Write the test from the step's test bullet list **first**, watch it fail, then add the smallest production code that turns it green.
- Unit tests must not require Docker. Anything that needs a Hazelcast cluster goes behind `//go:build integration` and lives next to the unit file (e.g. `provider_test.go` + `provider_integration_test.go`).
- For the provider package, introduce a small internal interface around the `*hazelcast.Map` methods we use (`Get/Set/SetWithTTL/Remove/Lock/Unlock/GetKeySet/GetEntrySet`) so unit tests can use a fake while integration tests exercise the real client.
- Match the existing test style in `hazelcast/config_test.go`: table-driven, external test package (`package hazelcast_test`), `errors.Is` for typed errors, `reflect.DeepEqual` for value comparisons, `t.Fatalf` for setup failures and `t.Errorf` for assertion failures.

## Commands

```sh
make test              # go test -race ./...
make test-integration  # go test -race -tags=integration ./...   (needs Docker)
make vet               # go vet ./...
make lint              # golangci-lint run
make bench             # go test -bench=. -benchmem -run=^$ ./...
make build-caddy       # xcaddy build with cache-handler + this module
```

CI runs `go vet`, `go test -race`, and `golangci-lint` on Linux + macOS (see `.github/workflows/ci.yml`). Integration is Linux-only (`integration.yml`).

## Coding conventions

The conventions below are enforced by `.golangci.yml` (errcheck, gocritic, govet, ineffassign, revive, staticcheck, unused) and by the existing code. Match the style in `hazelcast/config.go` for new files in the same package.

- Errors:
  - Return typed sentinel errors via `errors.New` for caller-checkable conditions (see `ErrNoAddresses`); use `fmt.Errorf` with `%w` for context wrapping.
  - Prefix every error string with `"hazelcast: "` so the source is obvious in Caddy logs.
  - Lower-case error strings, no trailing punctuation (revive `error-strings`).
- Public API:
  - Every exported identifier needs a doc comment that starts with the identifier name (revive `exported`).
  - Don't grow the exported surface beyond what the current `implementation-plan.md` step authorizes.
- Comments: only when the *why* is non-obvious. Don't restate what the code does.
- Concurrency: `go test -race ./...` is the floor, not the ceiling. Phase 2.5's read-modify-write of mapping bytes must be guarded by `imap.Lock(MappingKeyPrefix+baseKey)` / `Unlock`; mapping decode/encode goes through `core.MappingUpdater`.
- Lifecycle: `Reset` must close the client and remove the instance from the registry. `Provision` after `Reset` must succeed. `goleak.VerifyNone` should pass in lifecycle tests once Phase 4.1 lands.
- Logging: structured (zap), with `op`, `key_prefix`, and `latency_ms` fields once observability lands in Phase 4.3.

## Scope discipline

The plan deliberately defers things that look tempting. Do not add them without an explicit ask:

- CP-subsystem / `CPMap` epoch fencing → v2.
- Near Cache → v2.
- Failover-cluster config → v2.
- Compression algorithms beyond lz4 → out.
- Upstreaming to `darkweak/storages` → only after `v0.1.0` + soak.

If a step's bullet list doesn't mention something, it doesn't ship in that PR.

## Working in this repo

- Branch: develop on `claude/create-claude-md-cp5h8` (or whatever branch the task specifies). Never push to `main`.
- Commits: descriptive, focused on the single implementation-plan step they advance. Reference the step number (e.g. "Step 1.2: Hazelcast client factory").
- Pre-PR checklist (from `implementation-plan.md`):
  - [ ] Tests written first; commit history shows red→green where reasonable.
  - [ ] `go test -race ./...` green.
  - [ ] `golangci-lint run` clean.
  - [ ] Integration tests gated behind `//go:build integration` if they need Docker.
  - [ ] No new public API surface beyond what the step description names.
  - [ ] No new dependencies beyond the locked version pins (or PR explicitly justifies one).
- Do not create a PR unless the user explicitly asks. Push the branch, then stop.
