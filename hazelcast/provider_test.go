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
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
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
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
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
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
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
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
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

func TestInitAdoptsExistingRegistryEntry(t *testing.T) {
	cfg := &Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "adopt",
		MapName:     "m",
	}
	first, fc, _ := newProviderWithFake(t, cfg)
	if err := first.Init(); err != nil {
		t.Fatalf("first Init: %v", err)
	}

	second, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New(second): %v", err)
	}
	dialCalls := 0
	second.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
		dialCalls++
		return &fakeClient{}, newFakeMap(), nil
	}
	if err := second.Init(); err != nil {
		t.Fatalf("second Init: %v", err)
	}
	if dialCalls != 0 {
		t.Errorf("second Init dialled %d times, want 0", dialCalls)
	}
	if got := lookupInstance(second.Uuid()); got != first {
		t.Errorf("registry owner changed: got %v, want %v", got, first)
	}

	// Resetting the borrower must not shut the shared client down.
	if err := second.Reset(); err != nil {
		t.Fatalf("second Reset: %v", err)
	}
	if got := fc.shutdownCount(); got != 0 {
		t.Errorf("Shutdown called %d times after borrower Reset, want 0", got)
	}
	if got := lookupInstance(first.Uuid()); got != first {
		t.Errorf("borrower Reset evicted owner: got %v, want %v", got, first)
	}

	// Owner Reset still shuts the client down exactly once.
	if err := first.Reset(); err != nil {
		t.Fatalf("first Reset: %v", err)
	}
	if got := fc.shutdownCount(); got != 1 {
		t.Errorf("Shutdown called %d times after owner Reset, want 1", got)
	}
	if got := lookupInstance(first.Uuid()); got != nil {
		t.Errorf("owner Reset did not deregister: got %v, want nil", got)
	}
}

func TestInitConnectorErrorReleasesRegistrySlot(t *testing.T) {
	h, err := New(&Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "init-err-release",
	}, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	boom := errors.New("hazelcast: boom")
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
		return nil, nil, boom
	}
	if err := h.Init(); !errors.Is(err, boom) {
		t.Fatalf("Init: err = %v, want %v", err, boom)
	}
	if got := lookupInstance(h.Uuid()); got != nil {
		t.Errorf("registry still holds %v after failed Init", got)
	}

	// A subsequent successful Init on the same provider must be able to claim
	// the slot again.
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
		return &fakeClient{}, newFakeMap(), nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("retry Init: %v", err)
	}
	if got := lookupInstance(h.Uuid()); got != h {
		t.Errorf("retry Init did not register provider: got %v, want %v", got, h)
	}
	if err := h.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
}

func TestResetPreservesStateOnShutdownError(t *testing.T) {
	h, err := New(&Config{
		Addresses:   []string{"hz:5701"},
		ClusterName: "reset-err",
		MapName:     "m",
	}, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	boom := errors.New("hazelcast: shutdown failed")
	fc := &fakeClient{err: boom}
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
		return fc, newFakeMap(), nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	if err := h.Reset(); !errors.Is(err, boom) {
		t.Fatalf("Reset: err = %v, want %v", err, boom)
	}

	// State and registry entry must survive a failed shutdown so the caller
	// can retry.
	h.mu.Lock()
	clientAfter := h.client
	imapAfter := h.imap
	h.mu.Unlock()
	if clientAfter == nil || imapAfter == nil {
		t.Errorf("Reset wiped state on shutdown failure: client=%v imap=%v", clientAfter, imapAfter)
	}
	if got := lookupInstance(h.Uuid()); got != h {
		t.Errorf("Reset deregistered after shutdown failure: got %v, want %v", got, h)
	}

	// A retry once shutdown succeeds clears state cleanly.
	fc.mu.Lock()
	fc.err = nil
	fc.mu.Unlock()
	if err := h.Reset(); err != nil {
		t.Fatalf("retry Reset: %v", err)
	}
	if got := lookupInstance(h.Uuid()); got != nil {
		t.Errorf("retry Reset did not deregister: got %v, want nil", got)
	}
}
