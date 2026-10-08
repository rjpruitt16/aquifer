package aquifer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type streamDeliveryHarness struct {
	t          *testing.T
	app        *Aquifer
	server     *httptest.Server
	release    chan struct{}
	upstream   *httptest.Server
	hooks      chan map[string]any
	hookHits   atomic.Int64
	webhookURL string
}

// newStreamDeliveryHarness runs real HTTP: Aquifer, an upstream that holds
// every request until release is closed, and a webhook sink.
func newStreamDeliveryHarness(t *testing.T) *streamDeliveryHarness {
	t.Helper()
	h := &streamDeliveryHarness{t: t, release: make(chan struct{}), hooks: make(chan map[string]any, 4)}
	h.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		<-h.release
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"answer":42}`))
	}))
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if skipL8Probe(w, r) {
			return
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if _, ok := payload["job_id"]; ok {
			h.hookHits.Add(1)
			h.hooks <- payload
		}
		w.WriteHeader(http.StatusOK)
	}))
	h.app, _ = testAquiferWithLimits(t, AdmissionLimits{})
	h.server = httptest.NewServer(NewServer(h.app).Routes())
	t.Cleanup(func() {
		select {
		case <-h.release:
		default:
			close(h.release)
		}
		h.server.Close()
		h.upstream.Close()
		hook.Close()
	})
	h.webhookURL = hook.URL
	return h
}

func (h *streamDeliveryHarness) submit() string {
	body, _ := json.Marshal(JobRequest{UserID: "u", IdempotentKey: "stream-" + generateID(), URL: h.upstream.URL, Method: "POST", WebhookURL: h.webhookURL})
	resp, err := http.Post(h.server.URL+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var res EnqueueResult
	json.NewDecoder(resp.Body).Decode(&res)
	return res.JobID
}

// stream opens the job's SSE stream and returns a reader positioned after
// the first event, plus a cancel func that drops the connection.
func (h *streamDeliveryHarness) stream(jobID string) (*bufio.Scanner, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.server.URL+"/jobs/"+jobID+"/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { resp.Body.Close() })
	lines := bufio.NewScanner(resp.Body)
	for lines.Scan() && !strings.HasPrefix(lines.Text(), "event: queued") {
	}
	return lines, cancel
}

func (h *streamDeliveryHarness) expectWebhook(within time.Duration) map[string]any {
	select {
	case payload := <-h.hooks:
		return payload
	case <-time.After(within):
		h.t.Fatal("expected a webhook")
		return nil
	}
}

func (h *streamDeliveryHarness) expectNoWebhook(window time.Duration) {
	time.Sleep(window)
	if n := h.hookHits.Load(); n != 0 {
		h.t.Fatalf("expected no webhook, got %d", n)
	}
}

func waitForEvent(t *testing.T, lines *bufio.Scanner, event string) {
	t.Helper()
	for lines.Scan() {
		if lines.Text() == "event: "+event {
			return
		}
	}
	t.Fatalf("stream ended before event %q", event)
}

func TestStreamedResultSkipsWebhookAndIsStored(t *testing.T) {
	h := newStreamDeliveryHarness(t)
	jobID := h.submit()
	lines, cancel := h.stream(jobID)
	defer cancel()

	close(h.release)
	waitForEvent(t, lines, "completed")
	h.expectNoWebhook(1500 * time.Millisecond)

	resp, err := http.Get(h.server.URL + "/jobs/" + jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var job map[string]any
	json.NewDecoder(resp.Body).Decode(&job)
	result, _ := job["result"].(map[string]any)
	if job["status"] != "completed" || result["body"] != `{"answer":42}` || result["response_status"] != float64(200) {
		t.Fatalf("expected the stored result on GET /jobs/{id}, got %v", job)
	}
}

func TestDroppedStreamStillGetsWebhook(t *testing.T) {
	h := newStreamDeliveryHarness(t)
	jobID := h.submit()
	_, cancel := h.stream(jobID)

	cancel() // the client gives up while the job is still waiting
	time.Sleep(200 * time.Millisecond)
	close(h.release)

	if payload := h.expectWebhook(5 * time.Second); payload["job_id"] != jobID || payload["status"] != "completed" {
		t.Fatalf("expected the completion webhook for a dropped stream, got %v", payload)
	}
}

func TestUnstreamedJobGetsWebhookWithoutWaiting(t *testing.T) {
	t.Setenv("AQUIFER_STREAM_DELIVERY_WAIT_MS", "10000")
	h := newStreamDeliveryHarness(t)
	h.submit()
	close(h.release)
	// Nobody streams, so there is nothing to wait for: well under the 10s grace.
	h.expectWebhook(3 * time.Second)
}

func TestStoresKeepResults(t *testing.T) {
	for name, store := range map[string]JobStore{
		"sqlite": NewStore(filepath.Join(t.TempDir(), "a.db")),
		"pebble": NewPebbleStore(filepath.Join(t.TempDir(), "p")),
	} {
		t.Run(name, func(t *testing.T) {
			defer store.Close()
			req := sampleJobRequest("u", "result-"+name)
			job := NewJob(&req)
			store.CheckOrInsert(job)
			if _, ok := store.GetResult(job.ID); ok {
				t.Fatal("no result before completion")
			}
			store.UpdateStatus(job.ID, StatusCompleted)
			store.PutResult(job.ID, JobResult{JobID: job.ID, Status: StatusCompleted, ResponseStatus: 201, Body: "done"})
			got, ok := store.GetResult(job.ID)
			if !ok || got.ResponseStatus != 201 || got.Body != "done" {
				t.Fatalf("result did not round-trip: %+v %v", got, ok)
			}
		})
	}
}

func (h *streamDeliveryHarness) proxy(ctx context.Context) (*http.Response, error) {
	body, _ := json.Marshal(JobRequest{UserID: "u", IdempotentKey: "proxy-" + generateID(), URL: h.upstream.URL, Method: "POST", WebhookURL: h.webhookURL})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+"/proxy", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func TestProxyDirectSuccessSkipsWebhookWhenRelayed(t *testing.T) {
	h := newStreamDeliveryHarness(t)
	close(h.release)

	resp, err := h.proxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || out["answer"] != float64(42) {
		t.Fatalf("expected the upstream response relayed directly, got %d %v", resp.StatusCode, out)
	}
	h.expectNoWebhook(1500 * time.Millisecond)
}

func TestProxyCallerWhoDropsStillGetsWebhook(t *testing.T) {
	h := newStreamDeliveryHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, err := h.proxy(ctx); err == nil {
			resp.Body.Close()
		}
	}()
	time.Sleep(300 * time.Millisecond)
	cancel() // caller leaves before the upstream answers
	<-done
	time.Sleep(200 * time.Millisecond)
	close(h.release)

	if payload := h.expectWebhook(10 * time.Second); payload["status"] != "completed" {
		t.Fatalf("expected the completion webhook for a caller who left, got %v", payload)
	}
}
