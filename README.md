# caddy-cache-hazelcast

A [Hazelcast](https://hazelcast.com/) storage backend for the
[Caddy](https://caddyserver.com/) [cache-handler](https://github.com/caddyserver/cache-handler)
(Souin), implementing the `storages-core` `Storer` interface.

> Status: pre-alpha. Module scaffolding only — see `implementation-plan.md`.

## Module ID

`storages.cache.hazelcast`

## Layout

- `hazelcast/` — Hazelcast-backed `core.Storer` provider.
- `caddy/` — Caddy module shim that registers the provider.

## Development

```sh
make test              # unit tests, race detector
make test-integration  # integration tests (requires Docker)
make lint              # golangci-lint
make vet               # go vet
make bench             # benchmarks
make build-caddy       # build a Caddy binary with cache-handler + this module
```

## Compatibility

| Component       | Version  |
|-----------------|----------|
| Caddy           | v2.11.x  |
| cache-handler   | v0.16.0  |
| storages/core   | v0.0.19  |
| hazelcast-go    | v1.5.0   |
| Go              | 1.26     |

## License

Apache-2.0. See [LICENSE](./LICENSE).
