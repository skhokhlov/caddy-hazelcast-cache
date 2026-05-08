package hazelcast

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

func TestSetMultiLevelBeforeInitErrors(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "sml-uninit"})
	err := h.SetMultiLevel("base", "varied", []byte("body"), http.Header{}, "etag", time.Second, "real")
	if !errors.Is(err, ErrNotInitialised) {
		t.Errorf("SetMultiLevel before Init: err = %v, want %v", err, ErrNotInitialised)
	}
}

func TestSetMultiLevelRoundTripFake(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "sml-rt"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	body := []byte("hello body, repeated content content content content")
	headers := http.Header{"Accept-Language": []string{"en"}}
	if err := h.SetMultiLevel("base1", "varied1", body, headers, "etag1", 10*time.Second, "real1"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}

	stored := fm.snapshot()

	// Body stored at variedKey, lz4-compressed.
	compressed, ok := stored["varied1"]
	if !ok {
		t.Fatalf("variedKey not stored; got keys %v", keysOf(stored))
	}
	if bytes.Equal(compressed, body) {
		t.Errorf("body appears uncompressed; want lz4-compressed bytes")
	}
	got, err := lz4Decompress(compressed)
	if err != nil {
		t.Fatalf("lz4Decompress: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("decompressed body = %q, want %q", got, body)
	}

	// Mapping stored at IDX_base1 and decodes with the right entry.
	mappingBytes, ok := stored[core.MappingKeyPrefix+"base1"]
	if !ok {
		t.Fatalf("mapping key %q not stored", core.MappingKeyPrefix+"base1")
	}
	mapper, err := core.DecodeMapping(mappingBytes)
	if err != nil {
		t.Fatalf("DecodeMapping: %v", err)
	}
	idx, ok := mapper.GetMapping()["varied1"]
	if !ok {
		t.Fatalf("mapping does not contain variedKey 'varied1'; got %v", mapper.GetMapping())
	}
	if idx.GetRealKey() != "real1" {
		t.Errorf("RealKey = %q, want real1", idx.GetRealKey())
	}
	if idx.GetEtag() != "etag1" {
		t.Errorf("Etag = %q, want etag1", idx.GetEtag())
	}
}

func TestSetMultiLevelLocksAroundMappingUpdate(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "sml-lock"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.SetMultiLevel("k", "v", []byte("body"), nil, "e", time.Second, "r"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}
	mappingKey := core.MappingKeyPrefix + "k"
	wantSeq := []string{"L:" + mappingKey, "U:" + mappingKey}
	fm.mu.Lock()
	got := append([]string(nil), fm.lockSeq...)
	fm.mu.Unlock()
	if len(got) != 2 || got[0] != wantSeq[0] || got[1] != wantSeq[1] {
		t.Errorf("lock sequence = %v, want %v", got, wantSeq)
	}
}

func TestSetMultiLevelTTLIncludesStale(t *testing.T) {
	cfg := &Config{Addresses: []string{"hz:5701"}, ClusterName: "sml-ttl"}
	h, err := New(cfg, nil, 4*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	spy := &keyTTLSpy{fakeMap: newFakeMap(), captured: map[string]time.Duration{}}
	h.connector = func(ctx context.Context, c *Config, l logger.Logger) (hzClient, mapAPI, error) {
		return &fakeClient{}, spy, nil
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := h.SetMultiLevel("base", "varied", []byte("body"), nil, "etag", 6*time.Second, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}
	wantTTL := 10 * time.Second
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if got := spy.captured["varied"]; got != wantTTL {
		t.Errorf("TTL for varied = %v, want %v", got, wantTTL)
	}
	if got := spy.captured[core.MappingKeyPrefix+"base"]; got != wantTTL {
		t.Errorf("TTL for mapping = %v, want %v", got, wantTTL)
	}
}

func TestSetMultiLevelConcurrentSameBaseKey(t *testing.T) {
	h, _, fm := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "sml-conc"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			variedKey := fmt.Sprintf("v-%02d", i)
			body := []byte(fmt.Sprintf("body-%d", i))
			realKey := fmt.Sprintf("r-%02d", i)
			if err := h.SetMultiLevel("shared", variedKey, body, nil, fmt.Sprintf("etag-%d", i), time.Minute, realKey); err != nil {
				t.Errorf("SetMultiLevel %s: %v", variedKey, err)
			}
		}(i)
	}
	wg.Wait()

	mappingBytes, ok := fm.snapshot()[core.MappingKeyPrefix+"shared"]
	if !ok {
		t.Fatalf("mapping for shared base key missing")
	}
	mapper, err := core.DecodeMapping(mappingBytes)
	if err != nil {
		t.Fatalf("DecodeMapping: %v", err)
	}
	got := keysOfMap(mapper.GetMapping())
	sort.Strings(got)
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, fmt.Sprintf("v-%02d", i))
	}
	sort.Strings(want)
	if !equalStrings(got, want) {
		t.Errorf("variations after concurrent SetMultiLevel: got %v, want %v", got, want)
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func keysOfMap[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// keyTTLSpy records the most recent TTL passed for each key.
type keyTTLSpy struct {
	*fakeMap
	mu       sync.Mutex
	captured map[string]time.Duration
}

func (s *keyTTLSpy) SetWithTTL(ctx context.Context, key, value any, ttl time.Duration) error {
	s.mu.Lock()
	s.captured[key.(string)] = ttl
	s.mu.Unlock()
	return s.fakeMap.SetWithTTL(ctx, key, value, ttl)
}
