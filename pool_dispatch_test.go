package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestPoolBackedJobDispatchesToRegisteredMember is the end-to-end proof
// that a job submitted with pool_id (no url) actually resolves through
// the pool and reaches a real registered member's address, exercising
// the full path: Registry -> URLWorker -> AccountQueue -> Pool.Pick ->
// execute -> makeRequest.
func TestPoolBackedJobDispatchesToRegisteredMember(t *testing.T) {
	var hits atomic.Int64
	member := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer member.Close()

	webhookHits := make(chan struct{}, 1)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case webhookHits <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 50, MaxConcurrent: 5}}
	pools := NewPoolRegistry()
	t.Cleanup(pools.Stop)
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, pools)
	app := NewAquifer(store, registry, broker, l8, nil, pools)

	if err := app.RegisterPoolMember("workers", "w1", member.URL, 20, 30); err != nil {
		t.Fatalf("unexpected registration error: %v", err)
	}

	req := JobRequest{
		UserID:        "user-1",
		IdempotentKey: "pool-dispatch-key",
		PoolID:        "workers",
		Method:        "POST",
		WebhookURL:    webhook.URL,
	}
	if msg := req.Validate(); msg != "" {
		t.Fatalf("expected a valid pool-backed request, got validation error: %s", msg)
	}

	if _, err := app.Enqueue(req); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	select {
	case <-webhookHits:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the webhook to fire")
	}

	if hits.Load() != 1 {
		t.Fatalf("expected exactly one dispatch to the registered pool member, got %d", hits.Load())
	}
}

// TestPoolBackedJobWaitsForMembers confirms a job dispatched to an empty
// pool stays queued until a member registers, which is the restart-safe
// behavior for durable pool-backed work.
func TestPoolBackedJobWaitsForMembers(t *testing.T) {
	oldSleep := poolEmptySleepFunc.Load()
	poolEmptySleepFunc.Store(func(time.Duration) { time.Sleep(10 * time.Millisecond) })
	t.Cleanup(func() { poolEmptySleepFunc.Store(oldSleep) })

	var memberHits atomic.Int64
	member := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		memberHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer member.Close()

	webhookHits := make(chan struct{}, 1)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case webhookHits <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	app, store, _ := testPoolApp(t, 50, 5)

	req := JobRequest{
		UserID:        "user-1",
		IdempotentKey: "delayed-pool-key",
		PoolID:        "workers",
		Method:        "POST",
		WebhookURL:    webhook.URL,
	}

	result, err := app.Enqueue(req)
	if err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	select {
	case <-webhookHits:
		t.Fatal("empty pool should not fail or complete the job before a member registers")
	case <-time.After(50 * time.Millisecond):
	}

	if job := store.GetJob(result.JobID); job == nil || job.Status != StatusQueued {
		t.Fatalf("expected empty-pool job to remain queued, got %#v", job)
	}

	if err := app.RegisterPoolMember("workers", "w1", member.URL, 20, 30); err != nil {
		t.Fatalf("unexpected registration error: %v", err)
	}

	select {
	case <-webhookHits:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for queued job to dispatch after member registration")
	}
	if memberHits.Load() != 1 {
		t.Fatalf("expected one dispatch after member registration, got %d", memberHits.Load())
	}
	if job := store.GetJob(result.JobID); job == nil || job.Status != StatusCompleted {
		t.Fatalf("expected delayed pool job to complete, got %#v", job)
	}
}

func TestPoolBackedJobFailsOverToHealthyMember(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })

	var badHits atomic.Int64
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	var goodHits atomic.Int64
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()

	webhookHits := make(chan map[string]any, 1)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if _, ok := payload["job_id"]; ok {
			select {
			case webhookHits <- payload:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	app, store, pools := testPoolApp(t, 100, 1)
	if err := app.RegisterPoolMember("workers", "bad", bad.URL, 10, 30); err != nil {
		t.Fatalf("unexpected bad registration error: %v", err)
	}
	if err := app.RegisterPoolMember("workers", "good", good.URL, 10, 30); err != nil {
		t.Fatalf("unexpected good registration error: %v", err)
	}

	result, err := app.Enqueue(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "pool-failover-key",
		PoolID:        "workers",
		Method:        "POST",
		WebhookURL:    webhook.URL,
	})
	if err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	payload := waitForWebhook(t, webhookHits)
	if payload["status"] != "completed" {
		t.Fatalf("expected failover job to complete through healthy member, got payload %#v", payload)
	}
	if badHits.Load() == 0 {
		t.Fatal("expected the failing member to receive the first attempt")
	}
	if goodHits.Load() == 0 {
		t.Fatal("expected retry to fail over to the healthy member")
	}
	if job := store.GetJob(result.JobID); job == nil || job.Status != StatusCompleted {
		t.Fatalf("expected stored job to complete after failover, got %#v", job)
	}
	if got := pools.Get("workers").members["bad"].Reputation(); got >= 1.0 {
		t.Fatalf("expected bad member reputation to decay after failed attempt, got %v", got)
	}
}

func TestPoolBackedJobTreatsFinal500AsFailed(t *testing.T) {
	oldSleep := retrySleepFunc.Load()
	retrySleepFunc.Store(func(time.Duration) {})
	t.Cleanup(func() { retrySleepFunc.Store(oldSleep) })

	always500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("nope"))
	}))
	defer always500.Close()

	webhookHits := make(chan map[string]any, 1)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if _, ok := payload["job_id"]; ok {
			select {
			case webhookHits <- payload:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	app, store, _ := testPoolApp(t, 100, 1)
	if err := app.RegisterPoolMember("workers", "bad", always500.URL, 10, 30); err != nil {
		t.Fatalf("unexpected registration error: %v", err)
	}

	result, err := app.Enqueue(JobRequest{
		UserID:        "user-1",
		IdempotentKey: "pool-final-500-key",
		PoolID:        "workers",
		Method:        "POST",
		WebhookURL:    webhook.URL,
	})
	if err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	payload := waitForWebhook(t, webhookHits)
	if payload["status"] != "failed" {
		t.Fatalf("expected final 500 to fail the job, got payload %#v", payload)
	}
	if payload["response_status"] != float64(http.StatusInternalServerError) {
		t.Fatalf("expected response_status=500 in failure payload, got %#v", payload)
	}
	if job := store.GetJob(result.JobID); job == nil || job.Status != StatusFailed {
		t.Fatalf("expected stored job to fail after final 500, got %#v", job)
	}
}

func testPoolApp(t *testing.T, rps float64, maxConcurrent int) (*Aquifer, *Store, *PoolRegistry) {
	t.Helper()

	dir := t.TempDir()
	store := NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() {
		store.Close()
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: rps, MaxConcurrent: maxConcurrent}}
	pools := NewPoolRegistry()
	t.Cleanup(pools.Stop)
	registry := NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{}, pools)
	return NewAquifer(store, registry, broker, l8, nil, pools), store, pools
}

func waitForWebhook(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()

	select {
	case payload := <-ch:
		return payload
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for webhook")
	}
	return nil
}
