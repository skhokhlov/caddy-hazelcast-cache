.PHONY: test test-integration lint build-caddy bench vet

GO ?= go

test:
	$(GO) test -race ./...

test-integration:
	$(GO) test -race -tags=integration ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

build-caddy:
	xcaddy build \
		--with github.com/caddyserver/cache-handler \
		--with github.com/skhokhlov/caddy-cache-hazelcast/caddy

bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...
