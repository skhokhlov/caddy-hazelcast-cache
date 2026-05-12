package hazelcast

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/logger"
)

func TestGetBeforeInitReturnsNil(t *testing.T) {
	h, err := New(&Config{Addresses: []string{"hz:5701"}, ClusterName: "g-uninit"}, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := h.Get("k"); got != nil {
		t.Errorf("Get before Init: got %v, want nil", got)
	}
}

func TestSetBeforeInitErrors(t *testing.T) {
	h, err := New(&Config{Addresses: []string{"hz:5701"}, ClusterName: "s-uninit"}, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Set("k", []byte("v"), time.Second); !errors.Is(err, errNotInitialised) {
		t.Errorf("Set before Init: err = %v, want %v", err, errNotInitialised)
	}
}

func TestGetMissingReturnsNil(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "miss", MapName: "m"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := h.Get("absent"); got != nil {
		t.Errorf("Get(absent) = %q, want nil", got)
	}
}

func TestSetGetRoundTrip(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "rt", MapName: "m"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	want := []byte("payload-\x00\xff")
	if err := h.Set("k", want, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := h.Get("k"); !bytes.Equal(got, want) {
		t.Errorf("Get(k) = %q, want %q", got, want)
	}
}

func TestSetAppliesDurationPlusStale(t *testing.T) {
	stale := 7 * time.Second
	cfg := &Config{Addresses: []string{"hz:5701"}, ClusterName: "ttl-stale", MapName: "m"}
	h, err := New(cfg, nil, stale)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	fm := newFakeMap()
	h.connector = func(_ context.Context, _ *Config, _ logger.Logger) (hzClient, mapAPI, error) {
		return &fakeClient{}, fm, nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	ttl := 100 * time.Millisecond
	before := time.Now()
	if err := h.Set("k", []byte("v"), ttl); err != nil {
		t.Fatalf("Set: %v", err)
	}
	after := time.Now()

	entry, ok := fm.entryFor("k")
	if !ok {
		t.Fatal("entry not stored")
	}
	want := ttl + stale
	min := before.Add(want)
	max := after.Add(want)
	if entry.expires.Before(min) || entry.expires.After(max) {
		t.Errorf("expires = %v, want within [%v, %v] (ttl+stale=%v)", entry.expires, min, max, want)
	}
}

func TestSetWithoutTTLOrStaleIsUnbounded(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "no-ttl", MapName: "m"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Set("k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	entry, ok := fm.entryFor("k")
	if !ok {
		t.Fatal("entry not stored")
	}
	if !entry.expires.IsZero() {
		t.Errorf("expires = %v, want zero for ttl=0 stale=0", entry.expires)
	}
}

func TestGetSwallowsTransportError(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "get-err", MapName: "m"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	fm.mu.Lock()
	fm.getErr = errors.New("blip")
	fm.mu.Unlock()
	if got := h.Get("k"); got != nil {
		t.Errorf("Get with transport error: got %q, want nil", got)
	}
}

func TestSetPropagatesError(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "set-err", MapName: "m"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	upstream := errors.New("upstream down")
	fm.mu.Lock()
	fm.setErr = upstream
	fm.mu.Unlock()
	err := h.Set("k", []byte("v"), time.Second)
	if err == nil || !errors.Is(err, upstream) {
		t.Errorf("Set propagated err = %v, want wrap of %v", err, upstream)
	}
}

func TestGetSetConcurrentDisjointKeys(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "race", MapName: "m"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	const workers = 100
	const itemsPerWorker = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", worker, i)
				val := []byte(fmt.Sprintf("v-%d-%d", worker, i))
				if err := h.Set(key, val, time.Minute); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				got := h.Get(key)
				if !bytes.Equal(got, val) {
					t.Errorf("Get(%q) = %q, want %q", key, got, val)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}
