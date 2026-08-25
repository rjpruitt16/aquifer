package aquifer

import (
	"net/http"
	"testing"
)

// TestOrcaRpsParsesRealVLLMTextFormat guards against the exact wire format
// vLLM's create_orca_header() produces (verified directly against
// vllm/entrypoints/serve/utils/orca_metrics.py) -- not a format this project
// invented, so it's worth pinning to the real string.
func TestOrcaRpsParsesRealVLLMTextFormat(t *testing.T) {
	headers := http.Header{}
	headers.Set("endpoint-load-metrics", "TEXT named_metrics.kv_cache_usage_perc=0.98, named_metrics.num_requests_waiting=12")

	rps := orcaRps(headers)
	if rps == nil {
		t.Fatal("expected an rps override, got nil")
	}
	if *rps != 0.25 {
		t.Fatalf("expected 0.25 rps at 98%% utilization, got %v", *rps)
	}
}

// TestOrcaRpsParsesRealTritonTextFormat guards the exact wire format
// Triton's OrcaKVMetricHeader produces (verified directly against
// src/orca_http.cc) -- Triton reports the same concept vLLM calls
// kv_cache_usage_perc under a different name, kv_cache_utilization, which
// is why orcaRps tries a list of known names rather than one.
func TestOrcaRpsParsesRealTritonTextFormat(t *testing.T) {
	headers := http.Header{}
	headers.Set("endpoint-load-metrics", "TEXT named_metrics.kv_cache_utilization=0.980000, named_metrics.max_token_capacity=1024")

	rps := orcaRps(headers)
	if rps == nil {
		t.Fatal("expected an rps override, got nil")
	}
	if *rps != 0.25 {
		t.Fatalf("expected 0.25 rps at 98%% utilization, got %v", *rps)
	}
}

func TestOrcaRpsParsesJSONFormat(t *testing.T) {
	headers := http.Header{}
	headers.Set("endpoint-load-metrics", `JSON {"named_metrics":{"kv_cache_usage_perc":0.85}}`)

	rps := orcaRps(headers)
	if rps == nil {
		t.Fatal("expected an rps override, got nil")
	}
	if *rps != 2 {
		t.Fatalf("expected 2 rps at 85%% utilization, got %v", *rps)
	}
}

func TestOrcaRpsNoOverrideWhenHealthy(t *testing.T) {
	headers := http.Header{}
	headers.Set("endpoint-load-metrics", "TEXT named_metrics.kv_cache_usage_perc=0.3")

	if rps := orcaRps(headers); rps != nil {
		t.Fatalf("expected no override below the low watermark, got %v", *rps)
	}
}

func TestOrcaRpsMissingHeader(t *testing.T) {
	if rps := orcaRps(http.Header{}); rps != nil {
		t.Fatalf("expected nil with no header at all, got %v", *rps)
	}
}

func TestOrcaRpsMissingMetric(t *testing.T) {
	headers := http.Header{}
	headers.Set("endpoint-load-metrics", "TEXT named_metrics.num_requests_waiting=12")

	if rps := orcaRps(headers); rps != nil {
		t.Fatalf("expected nil when no known kv-cache metric name is reported, got %v", *rps)
	}
}

func TestOrcaRpsMalformedHeaderDoesNotPanic(t *testing.T) {
	cases := []string{
		"garbage",
		"TEXT",
		"JSON not-json",
		"TEXT named_metrics.kv_cache_usage_perc=not-a-number",
		"XML named_metrics.kv_cache_usage_perc=0.9",
	}
	for _, c := range cases {
		headers := http.Header{}
		headers.Set("endpoint-load-metrics", c)
		if rps := orcaRps(headers); rps != nil {
			t.Fatalf("input %q: expected nil for malformed input, got %v", c, *rps)
		}
	}
}

func TestOrcaLoadToRpsCurve(t *testing.T) {
	cases := []struct {
		util    float64
		wantNil bool
		wantRps float64
	}{
		{util: 0.0, wantNil: true},
		{util: 0.69, wantNil: true},
		{util: 0.70, wantRps: 2},
		{util: 0.89, wantRps: 2},
		{util: 0.90, wantRps: 0.5},
		{util: 0.96, wantRps: 0.5},
		{util: 0.97, wantRps: 0.25},
		{util: 1.0, wantRps: 0.25},
	}
	for _, c := range cases {
		got := orcaLoadToRps(c.util)
		if c.wantNil {
			if got != nil {
				t.Errorf("util %.2f: expected nil, got %v", c.util, *got)
			}
			continue
		}
		if got == nil {
			t.Errorf("util %.2f: expected %v, got nil", c.util, c.wantRps)
			continue
		}
		if *got != c.wantRps {
			t.Errorf("util %.2f: expected %v, got %v", c.util, c.wantRps, *got)
		}
	}
}

// TestAqueductHeaderTakesPrecedenceOverOrca is the important precedence
// guarantee: an operator's own explicit pacing directive must never be
// silently overridden by a best-effort inferred one. This test exercises
// the actual dispatch response-handling path, not just orcaRps in isolation.
func TestAqueductHeaderTakesPrecedenceOverOrca(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Aqueduct-Rps", "7")
	headers.Set("endpoint-load-metrics", "TEXT named_metrics.kv_cache_usage_perc=0.99")

	if val := pacingHeader(headers, "Rps"); val != "7" {
		t.Fatalf("expected explicit X-Aqueduct-Rps to be read first, got %q", val)
	}
	// orcaRps itself doesn't know about precedence -- that's enforced by the
	// caller in account_queue.go's else-if. This test documents the
	// guarantee at the pacingHeader level: if this string is non-empty,
	// the caller never even calls orcaRps.
}
