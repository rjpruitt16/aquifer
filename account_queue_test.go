package aquifer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestMakeRequestRequestsOrcaMetricsByDefault guards the exact casing sent:
// lowercase "text", not "TEXT". vLLM accepts either (metrics_format.lower()
// in orca_metrics.py), but Triton's ORCA support (src/orca_http.cc) does a
// case-sensitive comparison against the lowercase literal only -- "TEXT"
// makes Triton log an error and write no header at all, verified directly
// against Triton's source.
func TestMakeRequestRequestsOrcaMetricsByDefault(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get(orcaRequestHeaderName)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	job := &Job{Method: "POST"}
	resp, err := makeRequest(context.Background(), job, srv.URL, 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("makeRequest failed: %v", err)
	}
	defer resp.Body.Close()

	if got != "text" {
		t.Fatalf("expected %q request header to be %q, got %q", orcaRequestHeaderName, "text", got)
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
	resp, err := makeRequest(context.Background(), job, srv.URL, 0, 0, 0, nil)
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

// TestSlowStartBeginsAtMinRPS proves a queue constructed with slowStart=true
// starts dispatching at minRPS regardless of how high its configured
// ceiling is, rather than firing at the full configured rate immediately --
// a fresh queue's first request has no prior response to read a pacing
// signal from, so the starting point has to be decided up front, not
// adjusted reactively the way ordinary header-driven pacing is.
func TestSlowStartBeginsAtMinRPS(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir + "/aquifer.db")
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(dir+"/.l8-key", dir+"/l8-trust")

	const configuredRPS = 100.0
	q := NewAccountQueue("tenant-1", "https://example.com", configuredRPS, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {}, true, func(bool) {})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if q.RPS() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := q.RPS(); got != minRPS {
		t.Fatalf("expected slow-start queue to begin at minRPS (%v), got %v", minRPS, got)
	}
}

// TestSlowStartOffByDefaultStartsAtConfiguredRPS confirms the inverse: a
// queue that never opts in fires at its full configured rate immediately,
// same as before slow start existed.
func TestSlowStartOffByDefaultStartsAtConfiguredRPS(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir + "/aquifer.db")
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(dir+"/.l8-key", dir+"/l8-trust")

	const configuredRPS = 12.0
	q := NewAccountQueue("tenant-1", "https://example.com", configuredRPS, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, func(string) {}, false, func(bool) {})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if q.RPS() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := q.RPS(); got != configuredRPS {
		t.Fatalf("expected non-slow-start queue to begin at configuredRPS (%v), got %v", configuredRPS, got)
	}
}
