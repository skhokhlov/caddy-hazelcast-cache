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
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "g-uninit"})
	if got := h.Get("missing"); got != nil {
		t.Errorf("Get before Init: got %v, want nil", got)
	}
}

func TestSetBeforeInitErrors(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "s-uninit"})
	if err := h.Set("k", []byte("v"), time.Second); !errors.Is(err, ErrNotInitialised) {
		t.Errorf("Set before Init: err = %v, want %v", err, ErrNotInitialised)
	}
}

func TestSetGetRoundTrip(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "rt"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	want := []byte("payload")
	if err := h.Set("k", want, 5*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := h.Get("k"); !bytes.Equal(got, want) {
		t.Errorf("Get(k) = %q, want %q", got, want)
	}
}

func TestGetMissReturnsNil(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "miss"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := h.Get("absent"); got != nil {
		t.Errorf("Get absent: got %q, want nil", got)
	}
}

func TestSetUsesDurationPlusStale(t *testing.T) {
	cfg := &Config{Addresses: []string{"hz:5701"}, ClusterName: "ttl"}
	h, err := New(cfg, nil, 7*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	fm := newFakeMap()
	var capturedTTL time.Duration
	wrapper := &ttlSpyMap{fakeMap: fm, capture: &capturedTTL}
	h.connector = func(ctx context.Context, c *Config, l logger.Logger) (hzClient, mapAPI, error) {
		return &fakeClient{}, wrapper, nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.Set("k", []byte("v"), 3*time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, want := capturedTTL, 10*time.Second; got != want {
		t.Errorf("TTL passed to imap = %v, want %v (duration + stale)", got, want)
	}
}

func TestSetPropagatesError(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "set-err"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	fm.setErr = errors.New("upstream down")
	err := h.Set("k", []byte("v"), time.Second)
	if err == nil || !errors.Is(err, fm.setErr) {
		t.Errorf("Set with upstream error: got %v, want wrapping %v", err, fm.setErr)
	}
}

func TestGetSwallowsTransportError(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "get-err"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	fm.getErr = errors.New("blip")
	if got := h.Get("k"); got != nil {
		t.Errorf("Get with transport error: got %q, want nil", got)
	}
}

func TestGetSetConcurrentDisjointKeys(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "race"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			val := []byte(fmt.Sprintf("v-%d", i))
			if err := h.Set(key, val, time.Second); err != nil {
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

// ttlSpyMap captures the TTL passed to the most recent SetWithTTL call.
type ttlSpyMap struct {
	*fakeMap
	capture *time.Duration
}

func (s *ttlSpyMap) SetWithTTL(ctx context.Context, key, value any, ttl time.Duration) error {
	*s.capture = ttl
	return s.fakeMap.SetWithTTL(ctx, key, value, ttl)
}
