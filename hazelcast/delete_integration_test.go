//go:build integration

package hazelcast

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestDeleteIntegrationRemovesOne(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-del"}
	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	if err := h.Set("k", []byte("v"), 30*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	h.Delete("k")
	if got := h.Get("k"); got != nil {
		t.Errorf("Get after Delete: got %q, want nil", got)
	}
}

func TestDeleteManyIntegrationRegexPurge(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-dm"}
	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	const matchPrefix = "SURROGATE_user-42_"
	const total = 50
	var matching, other []string
	for i := 0; i < total; i++ {
		var key string
		switch i % 3 {
		case 0:
			key = fmt.Sprintf("%s%d", matchPrefix, i)
			matching = append(matching, key)
		case 1:
			key = fmt.Sprintf("SURROGATE_user-99_%d", i)
			other = append(other, key)
		default:
			key = fmt.Sprintf("OTHER_user-42_%d", i)
			other = append(other, key)
		}
		if err := h.Set(key, []byte("v"), 30*time.Second); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	h.DeleteMany("^" + matchPrefix)

	for _, k := range matching {
		if got := h.Get(k); got != nil {
			t.Errorf("Get %s after DeleteMany: got %q, want nil", k, got)
		}
	}
	for _, k := range other {
		if got := h.Get(k); got == nil {
			t.Errorf("non-matching key %s was deleted", k)
		}
	}
	sort.Strings(matching)
	sort.Strings(other)
	if t.Failed() {
		t.Logf("matching keys: %s", strings.Join(matching, ","))
	}
}
