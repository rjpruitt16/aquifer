package aquifer

import (
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

// TestPoolBackedJobFailsCleanlyWithNoMembers confirms a job dispatched to
// a pool nobody has registered to fails with a clear reason instead of
// blocking forever.
func TestPoolBackedJobFailsCleanlyWithNoMembers(t *testing.T) {
	webhookHits := make(chan map[string]any, 1)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case webhookHits <- map[string]any{}:
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

	req := JobRequest{
		UserID:        "user-1",
		IdempotentKey: "empty-pool-key",
		PoolID:        "nobody-registered-here",
		Method:        "POST",
		WebhookURL:    webhook.URL,
	}

	result, err := app.Enqueue(req)
	if err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	select {
	case <-webhookHits:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the failure webhook to fire")
	}

	job := store.GetJob(result.JobID)
	if job == nil {
		t.Fatal("expected the job to still exist (failed, not deleted)")
	}
	if job.Status != StatusFailed {
		t.Fatalf("expected status failed for a pool with no members, got %s", job.Status)
	}
}
