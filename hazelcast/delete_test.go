package hazelcast

import (
	"sort"
	"testing"
	"time"
)

func TestDeleteRemovesEntry(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "del"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Set("k", []byte("v"), time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	h.Delete("k")
	if _, ok := fm.snapshot()["k"]; ok {
		t.Errorf("Delete left key in fake map")
	}
}

func TestDeleteMissingIsNoop(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "del-miss"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	h.Delete("absent") // must not panic
}

func TestDeleteBeforeInitIsNoop(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "del-uninit"})
	h.Delete("anything") // no panic, no Init required
}

func TestDeleteManyRegexMatches(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "dm"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	keys := []string{
		"SURROGATE_user-42_a",
		"SURROGATE_user-42_b",
		"SURROGATE_user-99_c",
		"OTHER_user-42_d",
	}
	for _, k := range keys {
		if err := h.Set(k, []byte("v"), time.Second); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	h.DeleteMany("^SURROGATE_user-42_")

	var remaining []string
	for k := range fm.snapshot() {
		remaining = append(remaining, k)
	}
	sort.Strings(remaining)
	want := []string{"OTHER_user-42_d", "SURROGATE_user-99_c"}
	if !equalStrings(remaining, want) {
		t.Errorf("remaining keys = %v, want %v", remaining, want)
	}
}

func TestDeleteManyInvalidRegexIsSilent(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "dm-bad"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Set("k", []byte("v"), time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	h.DeleteMany("([") // invalid regex; must not panic, must not delete
	if _, ok := fm.snapshot()["k"]; !ok {
		t.Errorf("DeleteMany with invalid regex deleted entries")
	}
}

func TestDeleteManyBeforeInitIsNoop(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "dm-uninit"})
	h.DeleteMany("^.*$") // no panic
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
