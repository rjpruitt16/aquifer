package aquifer

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func testServer(t *testing.T, limits AdmissionLimits) *Server {
	t.Helper()
	app, _ := testAquiferWithLimits(t, limits)
	return NewServer(app)
}

func TestCreateJobSucceedsUnderNormalConditions(t *testing.T) {
	srv := testServer(t, AdmissionLimits{})

	body, _ := json.Marshal(sampleJobRequest("user-1", "key-1"))
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestCreateJobRejectsOversizedBodyWith413(t *testing.T) {
	srv := testServer(t, AdmissionLimits{MaxBodyBytes: 10})

	// Any well-formed request is bigger than 10 bytes once encoded.
	body, _ := json.Marshal(sampleJobRequest("user-1", "key-1"))
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "limit_bytes") {
		t.Fatalf("expected response to identify the limit, got: %s", rec.Body.String())
	}
}

func TestCreateJobRejectsUnderMemoryPressureWith429(t *testing.T) {
	srv := testServer(t, AdmissionLimits{MemoryLimitMB: 1, RetryAfterSeconds: 7})

	body, _ := json.Marshal(sampleJobRequest("user-1", "key-1"))
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("expected Retry-After: 7, got %q", got)
	}
	if !strings.Contains(rec.Body.String(), `"limit_reason":"memory"`) {
		t.Fatalf("expected response to identify memory as the limit reason, got: %s", rec.Body.String())
	}
}

func TestCreateJobDuplicateSucceedsAsIdempotentEvenUnderPressure(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	t.Cleanup(func() { store.Close() })
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, nil)
	admission := NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1_000_000}, dbPath)
	app := NewAquifer(store, registry, broker, l8, admission, nil)
	srv := NewServer(app)

	body, _ := json.Marshal(sampleJobRequest("user-1", "same-key"))

	first := httptest.NewRecorder()
	srv.Routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if first.Code != http.StatusCreated {
		t.Fatalf("expected first request to be 201, got %d: %s", first.Code, first.Body.String())
	}

	// Now the system is over the limit — a fresh job would be rejected...
	app.admission = NewAdmissionController(AdmissionLimits{MemoryLimitMB: 1}, dbPath)

	// ...but resubmitting the same idempotent key must still succeed.
	second := httptest.NewRecorder()
	srv.Routes().ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body)))
	if second.Code != http.StatusOK {
		t.Fatalf("expected duplicate resubmission to be 200, got %d: %s", second.Code, second.Body.String())
	}
}
