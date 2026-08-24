package aquifer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

// TestWebhookDeliveryDoesNotChain is the direct regression test for the
// infinite-recursion hazard: a webhook-delivery job (Job.WebhookURL == "")
// must never itself trigger another webhook enqueue when it completes.
func TestWebhookDeliveryDoesNotChain(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	hits := make(chan struct{}, 8)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		hits <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	registry := testRegistry(t)

	job := &Job{
		ID:         generateID(),
		UserID:     "user-1",
		URL:        upstream.URL,
		Method:     "GET",
		WebhookURL: webhook.URL,
		Status:     StatusQueued,
	}
	registry.Enqueue(job, "")

	select {
	case <-hits:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the webhook to fire")
	}

	select {
	case <-hits:
		t.Fatal("webhook fired more than once -- a webhook-delivery job must not enqueue its own webhook")
	case <-time.After(300 * time.Millisecond):
		// expected: nothing more arrives
	}
}

// TestWebhookDeliveryIsDeduped proves EnqueueWebhook is safe to call twice
// for the same originating job -- durability via CheckOrInsert also gives
// this for free via the "webhook:"+originalJobID idempotent key.
func TestWebhookDeliveryIsDeduped(t *testing.T) {
	hits := make(chan struct{}, 8)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		hits <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	registry := testRegistry(t)

	registry.EnqueueWebhook("job-1", "user-1", webhook.URL, map[string]any{"status": "completed"})
	registry.EnqueueWebhook("job-1", "user-1", webhook.URL, map[string]any{"status": "completed"})

	select {
	case <-hits:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the webhook to fire")
	}
	select {
	case <-hits:
		t.Fatal("expected the second EnqueueWebhook call for the same job to be deduped, but the webhook fired twice")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestWebhookDeliveryIsPacedByAccountQueue is the direct proof of the
// actual feature: a webhook receiver's own X-Aqueduct-Rps response header
// throttles subsequent webhook deliveries to it, exactly the way it
// already throttles forward dispatch to a real upstream -- because webhook
// delivery now goes through the same domain-keyed AccountQueue instead of
// firing immediately with a fixed retry schedule.
func TestWebhookDeliveryIsPacedByAccountQueue(t *testing.T) {
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Aqueduct-Rps", "1")
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	registry := testRegistry(t)
	registry.EnqueueWebhook("job-1", "user-1", webhook.URL, map[string]any{"status": "completed"})

	key := domainKey(webhook.URL)
	deadline := time.Now().Add(3 * time.Second)
	var rps float64
	for time.Now().Before(deadline) {
		registry.mu.Lock()
		w, ok := registry.workers[key]
		registry.mu.Unlock()
		if ok {
			w.mu.Lock()
			q, ok := w.queues[sharedKey]
			w.mu.Unlock()
			if ok && q.RPS() <= 1.01 {
				rps = q.RPS()
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	if rps == 0 || rps > 1.01 {
		t.Fatalf("expected the webhook account queue's RPS to settle at the receiver's advertised 1 rps, got %v", rps)
	}
}

// testL8Receiver spins up a receiver implementing just enough of the L8
// protocol (GET /.well-known/l8, POST /l8/challenge -- the same two
// handlers server.go exposes) to let a sender's L8Registry.EnsureTrust
// actually establish trust, plus a catch-all that captures the headers of
// whatever gets POSTed to it afterward.
func testL8Receiver(t *testing.T) (*httptest.Server, <-chan http.Header) {
	t.Helper()
	dir := t.TempDir()
	l8 := NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	app := NewAquifer(nil, nil, nil, l8, nil, nil)

	captured := make(chan http.Header, 8)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/l8", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(app.L8Metadata(r.Host))
	})
	mux.HandleFunc("POST /l8/challenge", func(w http.ResponseWriter, r *http.Request) {
		var req L8ChallengeReq
		json.NewDecoder(r.Body).Decode(&req)
		resp, err := app.HandleL8Challenge(req)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		captured <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, captured
}

// TestWebhookDeliveryIsL8Signed proves the L8-signing fix: routing webhook
// delivery through execute()/makeRequest (the same code path as forward
// dispatch) must not silently drop the X-L8-* signature headers that
// deliverWithRetry used to attach directly.
func TestWebhookDeliveryIsL8Signed(t *testing.T) {
	receiver, captured := testL8Receiver(t)
	registry := testRegistry(t)

	registry.EnqueueWebhook("job-1", "user-1", receiver.URL, map[string]any{"status": "completed"})

	var headers http.Header
	select {
	case headers = <-captured:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the signed webhook delivery")
	}

	for _, name := range []string{"X-L8-Delivery-Id", "X-L8-Timestamp", "X-L8-Key-Id", "X-L8-Signature"} {
		if headers.Get(name) == "" {
			t.Fatalf("expected webhook delivery to carry %s, headers: %v", name, headers)
		}
	}
}

// TestForwardDispatchIsNotL8Signed is the negative-space guard: L8 proves
// Aquifer's identity to a webhook *receiver* and has no meaning for forward
// dispatch to an arbitrary upstream API -- a regular job's dispatch request
// must never carry X-L8-* headers, even when the upstream happens to also
// speak the L8 protocol (i.e. this isn't just "trust never got established").
func TestForwardDispatchIsNotL8Signed(t *testing.T) {
	receiver, captured := testL8Receiver(t)
	registry := testRegistry(t)

	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	job := &Job{
		ID:         generateID(),
		UserID:     "user-1",
		URL:        receiver.URL,
		Method:     "GET",
		WebhookURL: sink.URL,
		Status:     StatusQueued,
	}
	registry.Enqueue(job, "")

	var headers http.Header
	select {
	case headers = <-captured:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the forward-dispatch request")
	}

	for _, name := range []string{"X-L8-Delivery-Id", "X-L8-Timestamp", "X-L8-Key-Id", "X-L8-Signature"} {
		if headers.Get(name) != "" {
			t.Fatalf("forward dispatch must never carry %s, got headers: %v", name, headers)
		}
	}
}
