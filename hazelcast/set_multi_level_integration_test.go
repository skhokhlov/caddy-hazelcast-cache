//go:build integration

package hazelcast

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
)

func TestSetMultiLevelIntegrationRoundTrip(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-sml"}
	h, err := New(cfg, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	body := []byte("integration body content content content content content")
	headers := http.Header{"Accept-Language": []string{"en"}}
	if err := h.SetMultiLevel("base", "varied", body, headers, "etag", 30*time.Second, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}

	stored := h.Get("varied")
	if len(stored) == 0 {
		t.Fatalf("varied key not stored")
	}
	if bytes.Equal(stored, body) {
		t.Errorf("body appears uncompressed")
	}
	roundTrip, err := lz4Decompress(stored)
	if err != nil {
		t.Fatalf("lz4Decompress: %v", err)
	}
	if !bytes.Equal(roundTrip, body) {
		t.Errorf("decompressed body mismatch")
	}

	mappingBytes := h.Get(core.MappingKeyPrefix + "base")
	if len(mappingBytes) == 0 {
		t.Fatalf("mapping not stored")
	}
	mapper, err := core.DecodeMapping(mappingBytes)
	if err != nil {
		t.Fatalf("DecodeMapping: %v", err)
	}
	idx, ok := mapper.GetMapping()["varied"]
	if !ok {
		t.Fatalf("varied entry missing from mapping")
	}
	if idx.GetRealKey() != "real" || idx.GetEtag() != "etag" {
		t.Errorf("mapping entry mismatch: realkey=%q etag=%q", idx.GetRealKey(), idx.GetEtag())
	}
}

func TestSetMultiLevelIntegrationConcurrent(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-sml-conc"}
	h, err := New(cfg, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

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

	mappingBytes := h.Get(core.MappingKeyPrefix + "shared")
	if len(mappingBytes) == 0 {
		t.Fatalf("mapping for shared base key missing")
	}
	mapper, err := core.DecodeMapping(mappingBytes)
	if err != nil {
		t.Fatalf("DecodeMapping: %v", err)
	}
	got := make([]string, 0, len(mapper.GetMapping()))
	for k := range mapper.GetMapping() {
		got = append(got, k)
	}
	sort.Strings(got)
	want := make([]string, 0, n)
	for i := 0; i < n; i++ {
		want = append(want, fmt.Sprintf("v-%02d", i))
	}
	sort.Strings(want)
	if !equalStrings(got, want) {
		t.Errorf("variations after concurrent SetMultiLevel: got %v, want %v (lost updates?)", got, want)
	}
}
