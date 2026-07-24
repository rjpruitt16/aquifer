package aquifer

import (
	"path/filepath"
	"testing"
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	return NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{})
}

func jobFor(userID, apiKey string) *Job {
	return &Job{
		ID:         generateID(),
		UserID:     userID,
		URL:        "https://example.com/webhook",
		Method:     "POST",
		Headers:    map[string]string{"Authorization": apiKey},
		WebhookURL: "https://example.com/callback",
		Status:     StatusQueued,
	}
}

// TestAccountQueueHeaderIsolatesTenants proves the fix for the previously
// unreachable AccountQueue path: a job enqueued with the account-queue header
// set to "enabled" must land in a queue keyed per-tenant (jobQueueKey), not
// the single shared queue every job used before handleAccountQueueHeader had
// a caller. Without this, a noisy tenant's flood starves every other tenant
// hitting the same upstream domain.
func TestAccountQueueHeaderIsolatesTenants(t *testing.T) {
	r := testRegistry(t)

	noisy := jobFor("tenant-noisy", "key-noisy")
	quiet := jobFor("tenant-quiet", "key-quiet")

	r.Enqueue(noisy, "enabled")
	r.Enqueue(quiet, "enabled")

	key := domainKey(noisy.URL)
	w, ok := r.workers[key]
	if !ok {
		t.Fatalf("expected a worker for domain %q", key)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.accountQueueMode {
		t.Fatalf("expected accountQueueMode to be enabled after header, worker never had handleAccountQueueHeader called")
	}

	noisyKey := jobQueueKey(noisy)
	quietKey := jobQueueKey(quiet)

	if noisyKey == quietKey {
		t.Fatalf("test setup bug: expected distinct tenant keys, got same key for both")
	}

	if _, ok := w.queues[noisyKey]; !ok {
		t.Errorf("expected a dedicated queue for the noisy tenant, found none (queues: %v)", queueKeys(w.queues))
	}
	if _, ok := w.queues[quietKey]; !ok {
		t.Errorf("expected a dedicated queue for the quiet tenant, found none (queues: %v)", queueKeys(w.queues))
	}
	if _, ok := w.queues[sharedKey]; ok {
		t.Errorf("did not expect the shared queue to be used once account-queue mode is enabled")
	}
}

// TestAccountQueueHeaderOmittedSharesQueue confirms the inverse: without the
// header, tenants on the same domain still share a single fairness bucket
// (the pre-fix, default behavior), so the fix doesn't accidentally force
// isolation on for everyone.
func TestAccountQueueHeaderOmittedSharesQueue(t *testing.T) {
	r := testRegistry(t)

	a := jobFor("tenant-a", "key-a")
	b := jobFor("tenant-b", "key-b")

	r.Enqueue(a, "")
	r.Enqueue(b, "")

	key := domainKey(a.URL)
	w, ok := r.workers[key]
	if !ok {
		t.Fatalf("expected a worker for domain %q", key)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.accountQueueMode {
		t.Fatalf("expected accountQueueMode to stay off when header is never set")
	}
	if _, ok := w.queues[sharedKey]; !ok {
		t.Errorf("expected both jobs to land in the shared queue, found none (queues: %v)", queueKeys(w.queues))
	}
}

func queueKeys(m map[string]*AccountQueue) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
