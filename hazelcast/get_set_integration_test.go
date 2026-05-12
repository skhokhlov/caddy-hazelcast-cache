//go:build integration

package hazelcast

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startHazelcast(t *testing.T) string {
	t.Helper()
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
	return endpoint
}

func TestGetSetIntegrationRoundTrip(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-rt"}

	// Hazelcast's TTL eviction runs on a coarse scheduler (~1s by default),
	// so pad the stale window and the post-expiry wait.
	stale := 500 * time.Millisecond
	h, err := New(cfg, nil, stale)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	want := []byte("hello-hazelcast")
	ttl := 200 * time.Millisecond
	if err := h.Set("k", want, ttl); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := h.Get("k"); !bytes.Equal(got, want) {
		t.Fatalf("Get immediately: %q, want %q", got, want)
	}

	time.Sleep(ttl + stale + 3*time.Second)
	if got := h.Get("k"); got != nil {
		t.Errorf("Get after TTL+stale: %q, want nil", got)
	}
}

func TestGetSetIntegrationConcurrent(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-race"}

	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("race-%d", i)
			val := []byte(fmt.Sprintf("v-%d", i))
			if err := h.Set(key, val, 30*time.Second); err != nil {
				t.Errorf("Set %s: %v", key, err)
				return
			}
			if got := h.Get(key); !bytes.Equal(got, val) {
				t.Errorf("Get %s = %q, want %q", key, got, val)
			}
		}(i)
	}
	wg.Wait()
}
