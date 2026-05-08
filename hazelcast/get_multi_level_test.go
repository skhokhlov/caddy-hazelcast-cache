package hazelcast

import (
	"io"
	"net/http"
	"net/http/httputil"
	"strings"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
)

func dumpResponse(t *testing.T, status int, body string) []byte {
	t.Helper()
	resp := &http.Response{
		Status:        http.StatusText(status),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"text/plain"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	out, err := httputil.DumpResponse(resp, true)
	if err != nil {
		t.Fatalf("DumpResponse: %v", err)
	}
	return out
}

func TestGetMultiLevelBeforeInitReturnsNil(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "gml-uninit"})
	req, _ := http.NewRequest("GET", "/", nil)
	fresh, stale := h.GetMultiLevel("k", req, &core.Revalidator{})
	if fresh != nil || stale != nil {
		t.Errorf("GetMultiLevel before Init: fresh=%v stale=%v, want nil,nil", fresh, stale)
	}
}

func TestGetMultiLevelMissingMappingReturnsNil(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "gml-miss"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	req, _ := http.NewRequest("GET", "/", nil)
	fresh, stale := h.GetMultiLevel("absent", req, &core.Revalidator{})
	if fresh != nil || stale != nil {
		t.Errorf("GetMultiLevel missing: fresh=%v stale=%v, want nil,nil", fresh, stale)
	}
}

func TestGetMultiLevelFreshHit(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "gml-fresh"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	body := dumpResponse(t, http.StatusOK, "hello-fresh")
	if err := h.SetMultiLevel("base", "varied", body, http.Header{}, "etag-1", time.Hour, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}

	req, _ := http.NewRequest("GET", "/", nil)
	v := &core.Revalidator{}
	fresh, stale := h.GetMultiLevel("base", req, v)
	if fresh == nil {
		t.Fatalf("fresh hit returned nil; stale=%v matched=%v", stale, v.Matched)
	}
	if stale != nil {
		t.Errorf("stale must be nil on fresh hit, got %v", stale)
	}
	gotBody, err := io.ReadAll(fresh.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(gotBody) != "hello-fresh" {
		t.Errorf("body = %q, want hello-fresh", gotBody)
	}
	if !v.Matched {
		t.Errorf("validator.Matched not set after fresh hit")
	}
}

func TestGetMultiLevelStaleHit(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "gml-stale"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// Negative duration drives freshTime into the past; the provider's stale
	// window keeps staleTime in the future, exercising the stale branch of
	// MappingElection.
	h.stale = time.Hour
	body := dumpResponse(t, http.StatusOK, "stale-body")
	if err := h.SetMultiLevel("base", "varied", body, http.Header{}, "etag", -time.Second, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}

	req, _ := http.NewRequest("GET", "/", nil)
	fresh, stale := h.GetMultiLevel("base", req, &core.Revalidator{})
	if fresh != nil {
		t.Errorf("fresh expected nil on stale hit, got %v", fresh)
	}
	if stale == nil {
		t.Fatalf("stale hit returned nil")
	}
	gotBody, err := io.ReadAll(stale.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(gotBody) != "stale-body" {
		t.Errorf("stale body = %q, want stale-body", gotBody)
	}
}

func TestGetMultiLevelVaryMismatchReturnsNil(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "gml-vary"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	body := dumpResponse(t, http.StatusOK, "varied-body")
	headers := http.Header{"Accept-Language": []string{"en"}}
	if err := h.SetMultiLevel("base", "varied", body, headers, "etag", time.Hour, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}

	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Language", "fr") // mismatch
	fresh, stale := h.GetMultiLevel("base", req, &core.Revalidator{})
	if fresh != nil || stale != nil {
		t.Errorf("Vary mismatch: fresh=%v stale=%v, want nil,nil", fresh, stale)
	}
}

func TestGetMultiLevelEtagMatchedFlag(t *testing.T) {
	h, _, _ := newProviderWithFake(t, &Config{Addresses: []string{"hz:5701"}, ClusterName: "gml-etag"})
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}

	body := dumpResponse(t, http.StatusOK, "etag-body")
	if err := h.SetMultiLevel("base", "varied", body, http.Header{}, "abc", time.Hour, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}

	req, _ := http.NewRequest("GET", "/", nil)
	v := &core.Revalidator{
		IfNoneMatchPresent: true,
		IfNoneMatch:        []string{"abc"},
		RequestETags:       []string{"abc"},
	}
	if _, _ = h.GetMultiLevel("base", req, v); !v.Matched {
		t.Errorf("validator.Matched = false after If-None-Match equality")
	}
}
