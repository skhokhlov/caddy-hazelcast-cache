//go:build integration

package hazelcast

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	hazelcastImage   = "hazelcast/hazelcast:5.5"
	hazelcastPort    = "5701/tcp"
	containerStartup = 2 * time.Minute
)

func TestNewClientIntegrationConnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), containerStartup+30*time.Second)
	defer cancel()

	container, err := testcontainers.Run(ctx, hazelcastImage,
		testcontainers.WithExposedPorts(hazelcastPort),
		testcontainers.WithWaitStrategy(
			wait.ForLog("is STARTED").WithStartupTimeout(containerStartup),
		),
	)
	if err != nil {
		t.Fatalf("start hazelcast container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	endpoint, err := container.PortEndpoint(ctx, hazelcastPort, "")
	if err != nil {
		t.Fatalf("get container endpoint: %v", err)
	}

	cfg := &Config{Addresses: []string{endpoint}}
	client, err := newClient(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}

	mapCtx, mapCancel := context.WithTimeout(ctx, 10*time.Second)
	defer mapCancel()
	imap, err := client.GetMap(mapCtx, cfg.MapName)
	if err != nil {
		t.Fatalf("GetMap(%q): %v", cfg.MapName, err)
	}
	if _, err := imap.Size(mapCtx); err != nil {
		t.Fatalf("imap.Size: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := client.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("client.Shutdown: %v", err)
	}
}
