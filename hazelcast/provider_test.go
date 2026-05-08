package hazelcast

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/logger"
)

func newProviderWithFake(t *testing.T, cfg *Config) (*Hazelcast, *fakeClient, *fakeMap) {
	t.Helper()
	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fc := &fakeClient{}
	fm := newFakeMap()
	h.connector = func(ctx context.Context, c *Config, l logger.Logger) (hzClient, mapAPI, error) {
		return fc, fm, nil
	}
	t.Cleanup(func() { _ = h.Reset() })
	return h, fc, fm
}

func TestProviderName(t *testing.T) {
	h, err := New(&Config{Addresses: []string{"hz:5701"}}, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := h.Name(); got != "HAZELCAST" {
		t.Errorf("Name() = %q, want HAZELCAST", got)
	}
}

func TestProviderUuidDeterministic(t *testing.T) {
	mk := func(cluster, mapName string, stale time.Duration) string {
		h, err := New(&Config{
			Addresses:   []string{"hz:5701"},
			ClusterName: cluster,
			MapName:     mapName,
		}, nil, stale)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return h.Uuid()
	}

	if a, b := mk("c", "m", time.Second), mk("c", "m", time.Second); a != b {
		t.Errorf("equal configs produced different Uuids: %q vs %q", a, b)
	}
	if a, b := mk("c", "m", time.Second), mk("c2", "m", time.Second); a == b {
		t.Errorf("differing cluster name did not change Uuid (%q)", a)
	}
	if a, b := mk("c", "m", time.Second), mk("c", "m2", time.Second); a == b {
		t.Errorf("differing map name did not change Uuid (%q)", a)
	}
	if a, b := mk("c", "m", time.Second), mk("c", "m", 2*time.Second); a == b {
		t.Errorf("differing stale did not change Uuid (%q)", a)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	if _, err := New(nil, nil, 0); !errors.Is(err, ErrNilConfig) {
		t.Errorf("New(nil): err = %v, want %v", err, ErrNilConfig)
	}
	if _, err := New(&Config{}, nil, 0); !errors.Is(err, ErrNoAddresses) {
		t.Errorf("New(empty): err = %v, want %v", err, ErrNoAddresses)
	}
}

func TestInitIdempotent(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "init-idem",
		MapName:     "m",
	})
	calls := 0
	h.connector = func(ctx context.Context, c *Config, l logger.Logger) (hzClient, mapAPI, error) {
		calls++
		return &fakeClient{}, newFakeMap(), nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init (second): %v", err)
	}
	if calls != 1 {
		t.Errorf("connector invoked %d times, want 1", calls)
	}
}

func TestInitConnectorErrorPropagates(t *testing.T) {
	h, err := New(&Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "init-err",
	}, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	boom := errors.New("hazelcast: boom")
	h.connector = func(ctx context.Context, c *Config, l logger.Logger) (hzClient, mapAPI, error) {
		return nil, nil, boom
	}
	if err := h.Init(); !errors.Is(err, boom) {
		t.Fatalf("Init: err = %v, want %v", err, boom)
	}
	if got := lookupInstance(h.Uuid()); got != nil {
		t.Errorf("registry stored provider after failed Init: got %v", got)
	}
}

func TestResetClosesClientAndDeregisters(t *testing.T) {
	h, fc, _ := newProviderWithFake(t, &Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "reset",
		MapName:     "m",
	})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := lookupInstance(h.Uuid()); got != h {
		t.Fatalf("lookupInstance after Init: got %v, want %v", got, h)
	}
	if err := h.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got := lookupInstance(h.Uuid()); got != nil {
		t.Errorf("lookupInstance after Reset: got %v, want nil", got)
	}
	if got := fc.shutdownCount(); got != 1 {
		t.Errorf("Shutdown calls = %d, want 1", got)
	}
}

func TestResetWithoutInitIsNoop(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "reset-noop",
	})
	if err := h.Reset(); err != nil {
		t.Errorf("Reset on uninitialised provider: %v", err)
	}
}

func TestProvisionAfterResetReconnects(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "reprov",
		MapName:     "m",
	})
	calls := 0
	h.connector = func(ctx context.Context, c *Config, l logger.Logger) (hzClient, mapAPI, error) {
		calls++
		return &fakeClient{}, newFakeMap(), nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init after Reset: %v", err)
	}
	if calls != 2 {
		t.Errorf("connector invoked %d times across Init/Reset/Init, want 2", calls)
	}
	if got := lookupInstance(h.Uuid()); got != h {
		t.Errorf("lookupInstance after re-Init: got %v, want %v", got, h)
	}
}
