package hazelcast

import (
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
)

func TestMapKeysFiltersAndStripsPrefix(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "mk"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Set("IDX_a", []byte("alpha"), time.Second); err != nil {
		t.Fatalf("Set IDX_a: %v", err)
	}
	if err := h.Set("IDX_b", []byte("beta"), time.Second); err != nil {
		t.Fatalf("Set IDX_b: %v", err)
	}
	if err := h.Set("OTHER_c", []byte("gamma"), time.Second); err != nil {
		t.Fatalf("Set OTHER_c: %v", err)
	}

	got := h.MapKeys("IDX_")
	want := map[string]string{"a": "alpha", "b": "beta"}
	if len(got) != len(want) {
		t.Fatalf("MapKeys size: %d, want %d (got=%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("MapKeys[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestMapKeysBeforeInitReturnsEmpty(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "mk-uninit"})
	if got := h.MapKeys("IDX_"); len(got) != 0 {
		t.Errorf("MapKeys before Init: %v, want empty", got)
	}
}

func TestListKeysDecodesMappingRealKeys(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "lk"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Build two mapping entries via core.MappingUpdater to seed the IMap.
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
	// Junk entry under the prefix should be skipped silently.
	if err := h.Set(core.MappingKeyPrefix+"junk", []byte{0x01, 0x02, 0x03}, time.Minute); err != nil {
		t.Fatalf("Set junk: %v", err)
	}
	// Non-mapping key should be ignored.
	if err := h.Set("payload-real-A", []byte("body"), time.Minute); err != nil {
		t.Fatalf("Set payload: %v", err)
	}

	got := h.ListKeys()
	sort.Strings(got)
	want := []string{"real-A", "real-B"}
	if !equalStrings(got, want) {
		t.Errorf("ListKeys = %v, want %v", got, want)
	}
}

func TestListKeysBeforeInitReturnsNil(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "lk-uninit"})
	if got := h.ListKeys(); got != nil {
		t.Errorf("ListKeys before Init: %v, want nil", got)
	}
}

