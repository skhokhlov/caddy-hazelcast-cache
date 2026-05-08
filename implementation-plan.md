# Hazelcast storage backend for Caddy — implementation plan

Derived from `design-plan.md`. Each step below is a single reviewable PR.
**TDD discipline:** every step lists tests first, then production code that turns
them green. No step ships without `go test -race ./...` passing.

## Decisions locked

| Topic | Choice |
|---|---|
| v1 scope | `core.Storer` impl over Hazelcast `IMap`; TLS; lz4 (matching upstream); full unit + integration coverage; Caddy shim publishing module ID `storages.cache.hazelcast`. |
| Deferred to v2 | CP/`CPMap` epoch fencing, Near Cache, failover-cluster config, advanced surrogate/vary refinements beyond what `storages/core` provides. |
| Repo layout | Single Go module at repo root. Packages: `hazelcast/` (provider) and `caddy/` (Caddy shim). One `go.mod`. |
| Integration tests | `testcontainers-go` with the official `hazelcast/hazelcast` 5.x image, gated by `//go:build integration`. Unit tests stay Docker-free. |
| Version pins | Caddy v2.11.x, `cache-handler` v0.16.0, `storages/core` v0.0.19, `hazelcast-go-client` v1.5.0, Go 1.26. |

## Implementation surface (from `core.Storer`)

The interface we implement (12 methods):

```
MapKeys(prefix string) map[string]string
ListKeys() []string
Get(key string) []byte
Set(key string, value []byte, duration time.Duration) error
Delete(key string)
DeleteMany(key string)              // regex
Init() error
Name() string
Uuid() string
Reset() error
GetMultiLevel(key, req, validator) (fresh, stale *http.Response)
SetMultiLevel(baseKey, variedKey, value, variedHeaders, etag, duration, realKey) error
```

Vary handling, ETag revalidation, and the protobuf mapping layer are already
provided by `core.MappingElection` / `core.MappingUpdater`. The provider only
owns key/value storage, regex purge, and lifecycle.

---

## Phase 0 — Project scaffolding (1 PR)

### Step 0.1 — Bootstrap module layout
- **Test:** `go test ./...` exits 0 (zero packages); `go vet ./...` clean; CI workflow runs both.
- **Code:**
  - `hazelcast/doc.go`, `caddy/doc.go` placeholder packages.
  - `.golangci.yml` (revive, gocritic, errcheck, govet, ineffassign, staticcheck, unused).
  - `.github/workflows/ci.yml`: matrix `{ubuntu, macos}`, runs `go test -race ./...`, `golangci-lint`, `go vet`.
  - `.github/workflows/integration.yml`: Linux only, builds the `integration` tag, depends on Docker.
  - `LICENSE` (Apache-2.0).
  - `Makefile`: `test`, `test-integration`, `lint`, `build-caddy`, `bench`.
  - `README.md` skeleton.
- No business logic.

---

## Phase 1 — Configuration & connection lifecycle (3 PRs)

### Step 1.1 — Config struct + validation
- **Test:** `hazelcast/config_test.go` — table-driven:
  - empty addresses → error
  - default `cluster_name=dev`, default `map_name=souin-cache`
  - bad TLS combos rejected (key without cert, etc.)
  - JSON roundtrip equality
- **Code:** `hazelcast/config.go` defining `Config{Addresses, ClusterName, MapName, Username, Password, TLS, ReadTimeout, WriteTimeout, ConnectTimeout, SmartRouting}` and `TLSConfig{CACertFile, CertFile, KeyFile, ServerName, InsecureSkipVerify}`. Pure validation; no client yet.

### Step 1.2 — Hazelcast client factory (no Caddy)
- **Test:**
  - Unit: `newClient(nil) → error`; defaults applied; cluster name overrides honored.
  - Integration (`//go:build integration`): testcontainers spins Hazelcast 5.x, asserts connect, ping a map, `Close` returns nil.
- **Code:** `hazelcast/client.go` — `newClient(ctx, cfg, logger) (*hazelcast.Client, error)` translating `Config` → `hazelcast.Config`. No TLS yet.

### Step 1.3 — TLS support
- **Test:**
  - Unit: builds `*tls.Config` from config (mTLS + ServerName); errors on unreadable cert; `InsecureSkipVerify=true` requires explicit env opt-in.
  - Integration (skipped if no TLS image available): connect over mTLS.
- **Code:** extend `hazelcast/client.go` with `buildTLSConfig`; wire into `hazelcast.Config.Cluster.Network.SSL`.

---

## Phase 2 — `core.Storer` implementation (6 PRs)

Interface abstraction note: introduce a small internal interface around the
parts of `*hazelcast.Map` we use (`Get/Set/SetWithTTL/Remove/Lock/Unlock/GetKeySet/GetEntrySet`).
This keeps unit tests fast (fake) and integration tests honest (real client).

### Step 2.1 — Provider skeleton: `Name`, `Uuid`, `Init`, `Reset`
- **Test:** `hazelcast/provider_test.go`
  - `Name() == "HAZELCAST"`
  - `Uuid()` deterministic from `cluster_name + map_name + stale`
  - `Init` idempotent
  - `Reset` closes client and removes from instance registry
  - Re-`Provision` after `Reset` works
- **Code:** `hazelcast/provider.go` with `Hazelcast` struct (`client`, `imap`, `stale`, `logger`). `sync.Map`-backed instance registry keyed by `Uuid()` (mirrors badger; prevents leaks on Caddy reload).

### Step 2.2 — `Get` and `Set` (round-trip with TTL)
- **Test:**
  - Unit (fake imap): Set/Get round trip; missing key returns nil.
  - Integration: `Set("k", v, 50ms)` → `Get` returns bytes; after `ttl + stale + safety`, `Get` returns nil.
  - Race: 100 concurrent Get/Set on disjoint keys, no panics, `-race` clean.
- **Code:** `Get` via `imap.Get`; `Set` via `imap.SetWithTTL` using `ttl + provider.stale` (matches badger convention).

### Step 2.3 — `Delete` and `DeleteMany` (regex purge)
- **Test:**
  - Integration: populate 50 keys with mixed prefixes; `DeleteMany("^SURROGATE_user-42_")` removes only matches; `Delete("k")` removes one.
  - Unit: invalid regex returns silently (interface has no error return).
- **Code:** `Delete` → `imap.Remove`. `DeleteMany` iterates `imap.GetKeySet()`, compiles regex, batches removes. **Note:** scan strategy is the v1 default; if benchmarks at Phase 4.2 show it's unacceptable past 100k keys, swap to Hazelcast SQL `DELETE … WHERE __key LIKE …` with regex→LIKE translation, scan fallback.

### Step 2.4 — `MapKeys` and `ListKeys`
- **Test:** integration:
  - `MapKeys("IDX_")` returns mapping entries with prefix stripped.
  - `ListKeys()` decodes entries via `core.DecodeMapping` and returns `RealKey`s.
- **Code:** iterate via `imap.GetEntrySet()` filtered by prefix; reuse `core.DecodeMapping`.

### Step 2.5 — `SetMultiLevel`
- **Test:** integration:
  - Round-trip: `(baseKey, variedKey, etag, duration, varied headers)` → lz4-compressed body stored under `variedKey` with TTL `duration + stale`; `IDX_<baseKey>` mapping protobuf updated.
  - Concurrency: 20 goroutines `SetMultiLevel` on the same `baseKey` with distinct `variedKey`s — decode mapping afterwards, all 20 entries present (no lost updates).
- **Code:** `SetMultiLevel` in `hazelcast/provider.go`. Hazelcast `IMap` has no multi-key transactions, so guard the read-modify-write of the mapping with `imap.Lock(MappingKeyPrefix+baseKey)` / `Unlock`. Body write is unconditional `SetWithTTL`. Reuse `core.MappingUpdater`.

### Step 2.6 — `GetMultiLevel`
- **Test:** integration covering:
  - fresh hit returns `fresh != nil`
  - stale returns `stale != nil`
  - no match returns both nil
  - Vary mismatch returns nil
  - ETag revalidation flow updates `core.Revalidator.Matched`
- **Code:** fetch mapping bytes from `IDX_<key>`, delegate to `core.MappingElection(provider, item, req, validator, logger)`.

---

## Phase 3 — Caddy module shim (2 PRs)

### Step 3.1 — Module registration
- **Test:** `caddy/module_test.go`
  - `caddy.GetModule("storages.cache.hazelcast")` returns the module.
  - `Provision` constructs a `Storer` and calls `core.RegisterStorage`.
  - `ServeHTTP` is a pass-through to `next`.
- **Code:** `caddy/hazelcast.go` mirrors `darkweak/storages/badger/caddy/badger.go`. Embeds `core.Configuration`; calls a `hazelcast.Factory(cfg.Provider, logger, cfg.Stale)` (constructor introduced here, wrapping the work from Phase 2).

### Step 3.2 — Caddyfile parser + JSON config + end-to-end smoke
- **Test:**
  - `caddy/caddyfile_test.go`: parse a Caddyfile block
    ```
    hazelcast {
      addresses hz-0:5701 hz-1:5701
      cluster_name prod-cache
      map_name responses
      tls {
        ca_cert_file /etc/certs/ca.pem
        cert_file    /etc/certs/client.pem
        key_file     /etc/certs/client.key
        server_name  hazelcast.internal
      }
      read_timeout  50ms
      write_timeout 200ms
    }
    ```
    into `Config`; assert every field.
  - `caddy/integration_test.go` (build-tagged): `xcaddy` builds a binary with `cache-handler` + this module; start Caddy with a Caddyfile pointing at a testcontainers Hazelcast + a fake origin; `GET /x` twice; second response has `Cache-Status: …; hit`.
- **Code:** `UnmarshalCaddyfile` populates `core.CacheProvider.Configuration` so the factory sees nested config; ship a working `Caddyfile` example.

---

## Phase 4 — Hardening (4 PRs)

### Step 4.1 — Reload / lifecycle correctness
- **Test:** integration:
  - `Provision` twice with the same Uuid returns the same underlying client (no leak).
  - `Reset` then `Provision` again works.
  - Killing the cluster mid-flight surfaces typed errors, no panics, no goroutine leaks (`goleak.VerifyNone`).
- **Code:** tighten instance registry; ensure `Reset` removes its registry entry only after `Close` completes; add reconnect-backoff observability.

### Step 4.2 — Benchmarks + `DeleteMany` strategy decision
- **Test:** `BenchmarkGet`, `BenchmarkSet`, `BenchmarkDeleteMany` at 1k / 10k / 100k cardinalities. Results checked in to `bench/RESULTS.md`.
- **Code:** if regex scan is unacceptable at 100k, switch `DeleteMany` to Hazelcast SQL `DELETE FROM "<map>" WHERE __key LIKE ?` (regex→LIKE when possible, scan fallback). Decision recorded in `bench/RESULTS.md`.

### Step 4.3 — Observability
- **Test:**
  - Unit: every public method increments the right counter; structured logs include `op`, `key_prefix`, `latency_ms`.
  - Integration: Prometheus registry exposes `hazelcast_provider_*` metrics with expected labels.
- **Code:** add a Prometheus collector (or hook into Souin's exporter if available — verify in this PR). Emit:
  - `hazelcast_provider_get_total`, `_set_total`, `_delete_total`
  - `hazelcast_provider_error_total{op,reason}`
  - `hazelcast_provider_latency_seconds{op}` (histogram)
  - `hazelcast_provider_payload_bytes{direction}`
  - `hazelcast_provider_cluster_connected` (gauge), `_member_count`

### Step 4.4 — Chaos & race coverage
- **Test:** integration matrix:
  - Kill a member during writes (testcontainers `Stop` on one node).
  - Cluster restart mid-traffic.
  - Network partition (testcontainers network manipulation or `pumba`).
  - Assert: no panic, errors typed, no stale entries reappear after a confirmed `Reset`.
  - Nightly CI job: `go test -race -count=20` over unit suite.
- **Code:** whatever fixes fall out — typically extra `ctx` plumbing, tighter timeouts, idempotent close.

---

## Phase 5 — Release (1 PR)

### Step 5.1 — Docs, release plumbing, v0.1.0
- **Code (no logic changes):**
  - `README.md`: quickstart, Caddyfile examples, production hardening, troubleshooting.
  - `SECURITY.md`, `CONTRIBUTING.md`, `CODEOWNERS`.
  - Compatibility matrix: Caddy / cache-handler / Souin / storages-core / Hazelcast / Go.
  - GoReleaser config: signed source archives + checksums.
  - Tag `v0.1.0`.

---

## Out of scope for v1 (tracked, deferred)

- CP-subsystem epoch fencing via `CPMap` (v2 candidate, Phase 6).
- Near Cache.
- Failover-cluster config (`hazelcast.Config.Failover`).
- Compression algorithms beyond lz4.
- Upstreaming to `darkweak/storages` — only after v0.1.0 + soak period.

---

## Summary

**16 PRs across 6 phases.** Each PR targets ~200–500 lines of diff including
tests. Heaviest reviews: **2.5** (multi-level set with `IMap.Lock`) and **4.2**
(`DeleteMany` strategy + benchmarks). Everything else is reviewable in one
sitting.

### PR checklist (per step)

- [ ] Tests written first; commit history shows red→green where reasonable.
- [ ] `go test -race ./...` green.
- [ ] `golangci-lint run` clean.
- [ ] Integration tests gated behind `//go:build integration` if they need Docker.
- [ ] No new public API surface beyond what the step description names.
- [ ] No new dependencies beyond the locked version pins (or PR explicitly justifies one).
