//go:build integration

package hazelcast

import (
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
)

func TestMapKeysIntegrationStripsPrefix(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-mk"}
	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	if err := h.Set(core.MappingKeyPrefix+"alpha", []byte("A"), 30*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := h.Set(core.MappingKeyPrefix+"beta", []byte("B"), 30*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := h.Set("PAYLOAD_x", []byte("X"), 30*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got := h.MapKeys(core.MappingKeyPrefix)
	want := map[string]string{"alpha": "A", "beta": "B"}
	if len(got) != len(want) {
		t.Fatalf("MapKeys size = %d, want %d (got=%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("MapKeys[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestListKeysIntegrationDecodesMapping(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-lk"}
	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	now := time.Now()
	bytesA, err := core.MappingUpdater("varied-A", nil, noopCoreLogger{}, now, now.Add(time.Minute), now.Add(2*time.Minute), http.Header{}, "etag-A", "real-A")
	if err != nil {
		t.Fatalf("MappingUpdater A: %v", err)
	}
	bytesB, err := core.MappingUpdater("varied-B", bytesA, noopCoreLogger{}, now, now.Add(time.Minute), now.Add(2*time.Minute), http.Header{}, "etag-B", "real-B")
	if err != nil {
		t.Fatalf("MappingUpdater B: %v", err)
	}
	if err := h.Set(core.MappingKeyPrefix+"basekey", bytesB, time.Minute); err != nil {
		t.Fatalf("Set mapping: %v", err)
	}

	got := h.ListKeys()
	sort.Strings(got)
	want := []string{"real-A", "real-B"}
	if !equalStrings(got, want) {
		t.Errorf("ListKeys = %v, want %v", got, want)
	}
}
