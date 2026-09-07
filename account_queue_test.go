package aquifer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeJobResultRecorder struct {
	hash   string
	result JobResult
	key    string
	ok     bool
}

func (f *fakeJobResultRecorder) RecordResult(hash string, result JobResult) (string, bool) {
	f.hash = hash
	f.result = result
	return f.key, f.ok
}

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

func TestRecordJobResultUsesIdempotencyHash(t *testing.T) {
	recorder := &fakeJobResultRecorder{key: "aqueduct:result:abc", ok: true}
	job := &Job{ID: "job-1", UserID: "user-1", IdempotentKey: "key-1", WebhookURL: "https://example.com/webhook"}

	got := recordJobResult(job, recorder, StatusCompleted, http.StatusOK, "application/json", `{"ok":true}`)

	if got != recorder.key {
		t.Fatalf("expected result key %q, got %q", recorder.key, got)
	}
	if recorder.hash != hashKey("user-1:key-1") {
		t.Fatalf("expected idempotency hash, got %q", recorder.hash)
	}
	if recorder.result.JobID != "job-1" || recorder.result.Status != StatusCompleted || recorder.result.ResponseStatus != http.StatusOK {
		t.Fatalf("unexpected recorded result: %+v", recorder.result)
	}
	if recorder.result.Body != `{"ok":true}` || recorder.result.ContentType != "application/json" {
		t.Fatalf("unexpected recorded body/content type: %+v", recorder.result)
	}
}

func TestRecordJobResultSkipsWebhookDeliveryJobs(t *testing.T) {
	recorder := &fakeJobResultRecorder{key: "aqueduct:result:abc", ok: true}
	job := &Job{ID: "webhook-job", UserID: "user-1", IdempotentKey: "webhook:job-1"}

	if got := recordJobResult(job, recorder, StatusCompleted, http.StatusOK, "application/json", "{}"); got != "" {
		t.Fatalf("expected no result key for webhook delivery job, got %q", got)
	}
	if recorder.hash != "" {
		t.Fatalf("expected recorder not to be called for webhook delivery job")
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
	broker := NewBroker()
	l8 := NewL8Registry(dir+"/.l8-key", dir+"/l8-trust")
	t.Cleanup(func() {
		l8.Close()
		store.Close()
	})

	const configuredRPS = 100.0
	q := NewAccountQueue("tenant-1", "https://example.com", configuredRPS, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, nil, func(string) {}, true, func(bool) {})
	t.Cleanup(q.Stop)

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
	broker := NewBroker()
	l8 := NewL8Registry(dir+"/.l8-key", dir+"/l8-trust")
	t.Cleanup(func() {
		l8.Close()
		store.Close()
	})

	const configuredRPS = 12.0
	q := NewAccountQueue("tenant-1", "https://example.com", configuredRPS, 5, nil, store, broker, l8, NoopMetricsAdapter{}, func(string, string, string, map[string]any) {}, nil, func(string) {}, false, func(bool) {})
	t.Cleanup(q.Stop)

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
