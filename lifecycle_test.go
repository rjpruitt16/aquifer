package aquifer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestAquiferDrainRejectsNewWorkAndFailsReadiness(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	registry := NewRegistry(store, &Config{Defaults: RateConfig{RPS: 10, MaxConcurrent: 1}}, broker, l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, store.Path()), nil)
	t.Cleanup(app.Close)

	if !app.BeginDrain(0) {
		t.Fatal("expected first drain transition to succeed")
	}
	if app.BeginDrain(0) {
		t.Fatal("expected drain transition to be idempotent")
	}
	_, err := app.Enqueue(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "draining-key",
		URL:           "https://example.com/work",
		Method:        http.MethodPost,
		WebhookURL:    "https://example.com/hook",
	})
	if !errors.Is(err, ErrAquiferDraining) {
		t.Fatalf("expected draining error, got %v", err)
	}
	if counts := store.Counts(); counts.TotalJobs != 0 {
		t.Fatalf("draining request must not be persisted, got %+v", counts)
	}

	routes := NewServer(app).Routes()
	health := httptest.NewRecorder()
	routes.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK || !bytes.Contains(health.Body.Bytes(), []byte(`"status":"draining"`)) {
		t.Fatalf("expected live-but-draining health response, got %d %s", health.Code, health.Body.String())
	}

	ready := httptest.NewRecorder()
	routes.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable || ready.Header().Get(nodeStateHeader) != "draining" {
		t.Fatalf("expected draining readiness rejection, got %d headers=%v body=%s", ready.Code, ready.Header(), ready.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(ready.Body.Bytes(), &body); err != nil || body["error"] != ErrAquiferDraining.Error() {
		t.Fatalf("unexpected readiness body: %s", ready.Body.String())
	}
}

func TestRegistryWaitIdleLetsAcceptedWorkAndWebhookFinish(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	registry := NewRegistry(store, &Config{Defaults: RateConfig{RPS: 1000, MaxConcurrent: 1}}, broker, l8, NoopMetricsAdapter{}, nil)
	app := NewAquifer(store, registry, broker, l8, NewAdmissionController(AdmissionLimits{}, store.Path()), nil)
	t.Cleanup(app.Close)

	started := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	var webhooks atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		webhooks.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	result, err := app.Enqueue(JobRequest{
		UserID:        "accepted-user",
		IdempotentKey: "accepted-before-drain",
		URL:           upstream.URL,
		Method:        http.MethodPost,
		WebhookURL:    webhook.URL,
	})
	if err != nil {
		t.Fatalf("enqueue accepted job: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("accepted job never reached upstream")
	}
	app.BeginDrain(0)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- registry.WaitIdle(ctx) }()
	select {
	case err := <-drained:
		t.Fatalf("registry reported idle while accepted work was still in flight: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-drained; err != nil {
		t.Fatalf("wait for accepted work: %v", err)
	}
	if webhooks.Load() != 1 {
		t.Fatalf("expected completion webhook to drain before idle, got %d", webhooks.Load())
	}
	if job := store.GetJob(result.JobID); job == nil || job.Status != StatusCompleted {
		t.Fatalf("expected accepted job to complete during drain, got %+v", job)
	}
}

type shutdownTestAdapter struct {
	started     chan *Aquifer
	sawDraining chan bool
}

func (a *shutdownTestAdapter) Name() string { return "shutdown-test" }

func (a *shutdownTestAdapter) Start(ctx context.Context, app *Aquifer) error {
	a.started <- app
	<-ctx.Done()
	a.sawDraining <- app.IsDraining()
	return nil
}

func TestRunAdapterMarksDrainingBeforeStoppingAdapter(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &shutdownTestAdapter{started: make(chan *Aquifer, 1), sawDraining: make(chan bool, 1)}
	done := make(chan error, 1)
	go func() {
		done <- RunAdapter(ctx, adapter, RuntimeOptions{
			DBPath:     filepath.Join(dir, "aquifer.db"),
			L8KeyPath:  filepath.Join(dir, ".l8-key"),
			L8TrustDir: filepath.Join(dir, "l8-trust"),
			Config:     &Config{Defaults: RateConfig{RPS: 10, MaxConcurrent: 1}},
			ShutdownConfig: &ShutdownConfig{
				Timeout:        time.Second,
				Quiesce:        time.Millisecond,
				WebSocketGrace: time.Millisecond,
			},
		})
	}()
	<-adapter.started
	cancel()
	if saw := <-adapter.sawDraining; !saw {
		t.Fatal("adapter was stopped before Aquifer entered draining state")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run adapter shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunAdapter did not complete within the shutdown timeout")
	}
}

func TestLoadShutdownConfigAllowsImmediateQuiesceAndWebSocketHandoff(t *testing.T) {
	t.Setenv("AQUIFER_SHUTDOWN_TIMEOUT_SECONDS", "12")
	t.Setenv("AQUIFER_SHUTDOWN_QUIESCE_MS", "0")
	t.Setenv("AQUIFER_WS_DRAIN_GRACE_SECONDS", "0")

	cfg := LoadShutdownConfig()
	if cfg.Timeout != 12*time.Second || cfg.Quiesce != 0 || cfg.WebSocketGrace != 0 {
		t.Fatalf("unexpected shutdown config: %+v", cfg)
	}
}
