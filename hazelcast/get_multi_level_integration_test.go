//go:build integration

package hazelcast

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/darkweak/storages/core"
)

func TestGetMultiLevelIntegrationFresh(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-gml-fresh"}
	h, err := New(cfg, nil, time.Hour)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	body := dumpResponse(t, http.StatusOK, "integration-fresh")
	if err := h.SetMultiLevel("base", "varied", body, http.Header{}, "etag", time.Hour, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}
	req, _ := http.NewRequest("GET", "/", nil)
	fresh, stale := h.GetMultiLevel("base", req, &core.Revalidator{})
	if fresh == nil || stale != nil {
		t.Fatalf("fresh=%v stale=%v want fresh+nil", fresh, stale)
	}
	gotBody, err := io.ReadAll(fresh.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(gotBody) != "integration-fresh" {
		t.Errorf("body = %q, want integration-fresh", gotBody)
	}
}

func TestGetMultiLevelIntegrationStale(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-gml-stale"}
	h, err := New(cfg, nil, time.Hour)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	body := dumpResponse(t, http.StatusOK, "integration-stale")
	if err := h.SetMultiLevel("base", "varied", body, http.Header{}, "etag", -time.Second, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}
	req, _ := http.NewRequest("GET", "/", nil)
	fresh, stale := h.GetMultiLevel("base", req, &core.Revalidator{})
	if fresh != nil || stale == nil {
		t.Fatalf("fresh=%v stale=%v want nil+stale", fresh, stale)
	}
}

func TestGetMultiLevelIntegrationNoMatch(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-gml-none"}
	h, err := New(cfg, nil, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	req, _ := http.NewRequest("GET", "/", nil)
	fresh, stale := h.GetMultiLevel("absent", req, &core.Revalidator{})
	if fresh != nil || stale != nil {
		t.Errorf("missing mapping: fresh=%v stale=%v want nil,nil", fresh, stale)
	}
}

func TestGetMultiLevelIntegrationVaryMismatch(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-gml-vary"}
	h, err := New(cfg, nil, time.Hour)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	body := dumpResponse(t, http.StatusOK, "vary")
	headers := http.Header{"Accept-Language": []string{"en"}}
	if err := h.SetMultiLevel("base", "varied", body, headers, "etag", time.Hour, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Language", "fr")
	fresh, stale := h.GetMultiLevel("base", req, &core.Revalidator{})
	if fresh != nil || stale != nil {
		t.Errorf("vary mismatch: fresh=%v stale=%v want nil,nil", fresh, stale)
	}
}

func TestGetMultiLevelIntegrationEtagRevalidation(t *testing.T) {
	endpoint := startHazelcast(t)
	cfg := &Config{Addresses: []string{endpoint}, ClusterName: "dev", MapName: "souin-it-gml-etag"}
	h, err := New(cfg, nil, time.Hour)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := h.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = h.Reset() })

	body := dumpResponse(t, http.StatusOK, "etag-flow")
	if err := h.SetMultiLevel("base", "varied", body, http.Header{}, "abc", time.Hour, "real"); err != nil {
		t.Fatalf("SetMultiLevel: %v", err)
	}
	req, _ := http.NewRequest("GET", "/", nil)
	v := &core.Revalidator{
		IfNoneMatchPresent: true,
		IfNoneMatch:        []string{"abc"},
		RequestETags:       []string{"abc"},
	}
	_, _ = h.GetMultiLevel("base", req, v)
	if !v.Matched {
		t.Errorf("validator.Matched = false after If-None-Match equality")
	}
}
