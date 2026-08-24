package aquifer

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMakeRequestRequestsOrcaMetricsByDefault(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get(orcaRequestHeaderName)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	job := &Job{Method: "POST"}
	resp, err := makeRequest(job, srv.URL, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("makeRequest failed: %v", err)
	}
	defer resp.Body.Close()

	if got != "TEXT" {
		t.Fatalf("expected %q request header to be %q, got %q", orcaRequestHeaderName, "TEXT", got)
	}
}

func TestMakeRequestDoesNotOverrideExplicitOrcaHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get(orcaRequestHeaderName)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	job := &Job{
		Method:  "POST",
		Headers: map[string]string{orcaRequestHeaderName: "JSON"},
	}
	resp, err := makeRequest(job, srv.URL, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("makeRequest failed: %v", err)
	}
	defer resp.Body.Close()

	if got != "JSON" {
		t.Fatalf("expected caller-provided header %q to survive, got %q", "JSON", got)
	}
}

func TestPacingHeaderPrefersAqueduct(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Aquifer-Rps", "2")
	headers.Set("X-Aqueduct-Rps", "5")

	if got := pacingHeader(headers, "Rps"); got != "5" {
		t.Fatalf("expected Aqueduct header to win, got %q", got)
	}
}

func TestPacingHeaderFallsBackToAquifer(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Aquifer-Max-Concurrent", "3")

	if got := pacingHeader(headers, "Max-Concurrent"); got != "3" {
		t.Fatalf("expected Aquifer fallback, got %q", got)
	}
}

func TestSetLoadHeaderWritesBothNamespaces(t *testing.T) {
	headers := http.Header{}
	setLoadHeader(headers, "Queue-Depth", "42")

	if got := headers.Get("X-Aqueduct-Queue-Depth"); got != "42" {
		t.Fatalf("expected Aqueduct header, got %q", got)
	}
	if got := headers.Get("X-Aquifer-Queue-Depth"); got != "42" {
		t.Fatalf("expected Aquifer header, got %q", got)
	}
}
