package aquifer

import (
	"path/filepath"
	"testing"
	"time"
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "aquifer.db")
	store := NewStore(dbPath)
	t.Cleanup(func() {
		store.Close()
		// Enqueue starts a real background dispatch goroutine that can
		// still be mid-flight (holding a WAL file handle) the instant this
		// test function returns — Close() waits for in-flight queries, but
		// modernc.org/sqlite's own file release can trail Close() returning
		// by a beat. Without this, t.TempDir()'s cleanup (registered before
		// this one, so it runs right after) occasionally races it with a
		// "directory not empty" error.
		time.Sleep(20 * time.Millisecond)
	})
	broker := NewBroker()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &Config{Defaults: RateConfig{RPS: 100, MaxConcurrent: 1}}
	return NewRegistry(store, cfg, broker, l8, NoopMetricsAdapter{})
}

func jobFor(userID, apiKey string) *Job {
	return &Job{
		ID:     generateID(),
		UserID: userID,
		// postman-echo.com/post always returns a fast 200 for both the
		// dispatch and the webhook delivery — example.com's 405 on POST
		// made deliverWebhook retry 4x with exponential backoff (up to 15s),
		// leaving a background goroutine alive long after the test function
		// returns and racing t.TempDir()'s cleanup against the still-open
		// store.
		URL:        "https://postman-echo.com/post",
		Method:     "POST",
		Headers:    map[string]string{"Authorization": apiKey},
		WebhookURL: "https://postman-echo.com/post",
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
