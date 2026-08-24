package a2aadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/rjpruitt16/aquifer"
)

// testAquifer builds a minimal, self-contained *aquifer.Aquifer the same
// way the MCP adapter's own tests do (mcp_adapter_test.go), using only
// exported constructors since this package sits outside aquifer's own
// package boundary.
func testAquifer(t *testing.T) *aquifer.Aquifer {
	t.Helper()

	dir := t.TempDir()
	store := aquifer.NewStore(filepath.Join(dir, "aquifer.db"))
	t.Cleanup(func() { store.Close() })
	broker := aquifer.NewBroker()
	l8 := aquifer.NewL8Registry(filepath.Join(dir, ".l8-key"), filepath.Join(dir, "l8-trust"))
	cfg := &aquifer.Config{Defaults: aquifer.RateConfig{RPS: 100, MaxConcurrent: 5}}
	registry := aquifer.NewRegistry(store, cfg, broker, l8, aquifer.NoopMetricsAdapter{}, nil)
	admission := aquifer.NewAdmissionController(aquifer.AdmissionLimits{}, filepath.Join(dir, "aquifer.db"))
	return aquifer.NewAquifer(store, registry, broker, l8, admission, nil)
}

// newTestHandler wires an executor into a real a2asrv.RequestHandler the
// same way Adapter.Start does, minus the HTTP transport -- these tests
// drive the handler directly, which is what actually exercises Execute's
// translation logic without the noise of JSON-RPC/HTTP round-tripping.
// allowPrivatePush relaxes the push sender's SSRF guard, which is correct
// production behavior (loopback push targets are blocked by default,
// matching A2A spec 13.2) but would make a same-machine test webhook
// receiver unreachable -- Start() itself always leaves the guard on.
func newTestHandler(t *testing.T, aq *aquifer.Aquifer, allowPrivatePush bool) a2asrv.RequestHandler {
	t.Helper()
	// A real, immediately-200ing sink -- Execute always sets WebhookURL to
	// this when a request doesn't set one, and Aquifer's own webhook
	// delivery (webhook.go) will actually POST to it in the background.
	// Pointing this at a dead address instead would just make every test
	// spend several seconds retrying with backoff for no reason.
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(sink.Close)
	exec := &executor{aq: aq, sinkURL: sink.URL}
	return a2asrv.NewHandler(exec,
		a2asrv.WithCapabilityChecks(&a2a.AgentCapabilities{Streaming: true, PushNotifications: true}),
		a2asrv.WithPushNotifications(push.NewInMemoryStore(), push.NewHTTPPushSender(&push.HTTPSenderConfig{
			AllowPrivateNetworks: allowPrivatePush,
		})),
	)
}

func jobRequestMessage(t *testing.T, req aquifer.JobRequest) *a2a.Message {
	t.Helper()
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal job request: %v", err)
	}
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("unmarshal job request into any: %v", err)
	}
	return a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(data))
}

func TestSendMessageCompletesAndReturnsArtifact(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	aq := testAquifer(t)
	handler := newTestHandler(t, aq, false)

	msg := jobRequestMessage(t, aquifer.JobRequest{
		UserID:        "agent-1",
		IdempotentKey: "completes-1",
		URL:           upstream.URL,
		Method:        "GET",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := handler.SendMessage(ctx, &a2a.SendMessageRequest{Message: msg})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	task, ok := result.(*a2a.Task)
	if !ok {
		t.Fatalf("expected *a2a.Task result, got %T", result)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("expected TaskStateCompleted, got %s", task.Status.State)
	}
	if len(task.Artifacts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(task.Artifacts))
	}
	data := task.Artifacts[0].Parts[0].Data()
	dataMap, ok := data.(map[string]any)
	if !ok {
		t.Fatalf("expected artifact data part to be an object, got %T", data)
	}
	if body, _ := dataMap["body"].(string); body != `{"ok":true}` {
		t.Fatalf("unexpected artifact body: %v", dataMap["body"])
	}
}

// TestSendMessageMissingDataPartReturnsError covers Execute's "before a task
// was created" error path (agentexec.go's own doc comment: "An error should
// be returned in special cases or before a task was created") -- Execute
// validates the message and returns before ever yielding NewSubmittedTask,
// so this surfaces as a real error from SendMessage, not a failed task.
func TestSendMessageMissingDataPartReturnsError(t *testing.T) {
	aq := testAquifer(t)
	handler := newTestHandler(t, aq, false)

	msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("no data part here"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := handler.SendMessage(ctx, &a2a.SendMessageRequest{Message: msg})
	if err == nil {
		t.Fatalf("expected an error for a message with no data part")
	}
	if !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("expected error to wrap a2a.ErrInvalidParams, got: %v", err)
	}
}

// TestDuplicateIdempotentKeyDoesNotHang is the direct regression test for
// the terminal-duplicate check in Execute: a second A2A task submitted with
// the same (user_id, idempotent_key) resolves to a job whose dispatch
// already finished and will never publish another broker event. Without
// checking job.Status up front, this would hang until the test's context
// deadline instead of completing.
func TestDuplicateIdempotentKeyDoesNotHang(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("first"))
	}))
	defer upstream.Close()

	aq := testAquifer(t)
	handler := newTestHandler(t, aq, false)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	firstReq := aquifer.JobRequest{
		UserID:        "agent-1",
		IdempotentKey: "dup-key",
		URL:           upstream.URL,
		Method:        "GET",
	}

	first, err := handler.SendMessage(ctx, &a2a.SendMessageRequest{Message: jobRequestMessage(t, firstReq)})
	if err != nil {
		t.Fatalf("first SendMessage: %v", err)
	}
	firstTask := first.(*a2a.Task)
	if firstTask.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("expected first task completed, got %s", firstTask.Status.State)
	}

	// Same user_id + idempotent_key, different A2A message/task -- Aquifer's
	// own idempotency ledger resolves this to the *same* underlying job,
	// which already reached a terminal state before this second task's
	// executor ever subscribed to it.
	second, err := handler.SendMessage(ctx, &a2a.SendMessageRequest{Message: jobRequestMessage(t, firstReq)})
	if err != nil {
		t.Fatalf("second SendMessage: %v", err)
	}
	secondTask, ok := second.(*a2a.Task)
	if !ok {
		t.Fatalf("expected *a2a.Task result, got %T", second)
	}
	if secondTask.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("expected duplicate task to resolve to completed, got %s", secondTask.Status.State)
	}
	if secondTask.ID == firstTask.ID {
		t.Fatalf("expected a distinct A2A task id for the duplicate submission")
	}
}

func TestSendStreamingMessageEmitsWorkingThenCompleted(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	aq := testAquifer(t)
	handler := newTestHandler(t, aq, false)

	msg := jobRequestMessage(t, aquifer.JobRequest{
		UserID:        "agent-1",
		IdempotentKey: "stream-1",
		URL:           upstream.URL,
		Method:        "GET",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var states []a2a.TaskState
	for event, err := range handler.SendStreamingMessage(ctx, &a2a.SendMessageRequest{Message: msg}) {
		if err != nil {
			t.Fatalf("streaming event error: %v", err)
		}
		switch v := event.(type) {
		case *a2a.Task:
			states = append(states, v.Status.State)
		case *a2a.TaskStatusUpdateEvent:
			states = append(states, v.Status.State)
		}
	}

	if len(states) < 2 {
		t.Fatalf("expected at least submitted+completed states, got %v", states)
	}
	if states[0] != a2a.TaskStateSubmitted {
		t.Fatalf("expected first state to be submitted, got %s", states[0])
	}
	last := states[len(states)-1]
	if last != a2a.TaskStateCompleted {
		t.Fatalf("expected last state to be completed, got %s", last)
	}
}

// TestCancelTaskReturnsUnsupportedNotSilentNoop is the required proof that
// Cancel doesn't fake support: Aquifer has no real cancellation mechanism,
// so a CancelTask call against a still-active task must surface as an
// error, not a quiet success that leaves the job running unaffected.
func TestCancelTaskReturnsUnsupportedNotSilentNoop(t *testing.T) {
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()
	defer close(release)

	aq := testAquifer(t)
	handler := newTestHandler(t, aq, false)

	msg := jobRequestMessage(t, aquifer.JobRequest{
		UserID:        "agent-1",
		IdempotentKey: "cancel-1",
		URL:           upstream.URL,
		Method:        "GET",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ReturnImmediately interrupts SendMessage on the very first event
	// (Execute's NewSubmittedTask, yielded before Enqueue is even called),
	// handing back the task ID synchronously without needing to poll
	// ListTasks -- whose in-memory store requires an authenticated caller
	// this test deliberately doesn't set up (see taskstore.InMemory.List).
	// The underlying execution keeps running in the background regardless
	// of whether the caller detaches early.
	submitResult, err := handler.SendMessage(ctx, &a2a.SendMessageRequest{
		Message: msg,
		Config:  &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	submittedTask, ok := submitResult.(*a2a.Task)
	if !ok {
		t.Fatalf("expected *a2a.Task result, got %T", submitResult)
	}
	taskID := submittedTask.ID

	deadline := time.Now().Add(2 * time.Second)
	var working bool
	for time.Now().Before(deadline) {
		task, err := handler.GetTask(ctx, &a2a.GetTaskRequest{ID: taskID})
		if err == nil && task.Status.State == a2a.TaskStateWorking {
			working = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !working {
		t.Fatalf("job never reached working state before deadline")
	}

	_, err = handler.CancelTask(ctx, &a2a.CancelTaskRequest{ID: taskID})
	if err == nil {
		t.Fatalf("expected CancelTask to return an error, got a silent success")
	}
	if !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Fatalf("expected error to wrap a2a.ErrUnsupportedOperation, got: %v", err)
	}
}

func TestCreateTaskPushConfigDeliversTaskEvents(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	received := make(chan []byte, 8)
	pushReceiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer pushReceiver.Close()

	aq := testAquifer(t)
	// Push targets a loopback test server, so the SSRF guard must be
	// relaxed here -- see newTestHandler's doc comment.
	handler := newTestHandler(t, aq, true)

	msg := jobRequestMessage(t, aquifer.JobRequest{
		UserID:        "agent-1",
		IdempotentKey: "push-1",
		URL:           upstream.URL,
		Method:        "GET",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &a2a.SendMessageRequest{
		Message: msg,
		Config: &a2a.SendMessageConfig{
			PushConfig: &a2a.PushConfig{URL: pushReceiver.URL},
		},
	}

	result, err := handler.SendMessage(ctx, req)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	task, ok := result.(*a2a.Task)
	if !ok || task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("expected completed task, got %#v", result)
	}

	select {
	case body := <-received:
		var decoded map[string]any
		if err := json.Unmarshal(body, &decoded); err != nil {
			t.Fatalf("push payload is not valid json: %v", err)
		}
		if len(decoded) == 0 {
			t.Fatalf("expected a non-empty push payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("push notification was never delivered")
	}
}

func TestAgentCardHasRequiredFields(t *testing.T) {
	card := agentCard("http://localhost:8090", a2a.AgentCapabilities{Streaming: true, PushNotifications: true})

	if card.Name == "" {
		t.Fatalf("agent card missing name")
	}
	if card.Provider == nil {
		t.Fatalf("agent card missing provider")
	}
	if len(card.SupportedInterfaces) != 1 {
		t.Fatalf("expected exactly 1 supported interface, got %d", len(card.SupportedInterfaces))
	}
	iface := card.SupportedInterfaces[0]
	if iface.URL != "http://localhost:8090" {
		t.Fatalf("unexpected interface url: %s", iface.URL)
	}
	if iface.ProtocolBinding != a2a.TransportProtocolJSONRPC {
		t.Fatalf("expected JSONRPC protocol binding, got %s", iface.ProtocolBinding)
	}
	if !card.Capabilities.Streaming || !card.Capabilities.PushNotifications {
		t.Fatalf("expected streaming and push notifications capabilities to be advertised")
	}
	if len(card.Skills) == 0 {
		t.Fatalf("expected at least one declared skill")
	}
}

// TestAdapterServesAgentCardAndJSONRPCOverHTTP is the one true end-to-end
// test: it starts Adapter.Start on a real loopback listener exactly the way
// cmd/aquifer/main.go does, and drives it purely over HTTP -- proving the
// mux wiring (JSON-RPC handler, agent card handler, webhook sink route)
// actually works together, not just each piece in isolation.
func TestAdapterServesAgentCardAndJSONRPCOverHTTP(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	aq := testAquifer(t)
	addr := "127.0.0.1:18790"
	publicURL := "http://" + addr
	adapter := NewAdapter(addr, publicURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveErr := make(chan error, 1)
	go func() { serveErr <- adapter.Start(ctx, aq) }()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(2 * time.Second)
	var cardResp *http.Response
	var err error
	for time.Now().Before(deadline) {
		cardResp, err = client.Get(publicURL + a2asrv.WellKnownAgentCardPath)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("adapter never came up: %v", err)
	}
	defer cardResp.Body.Close()

	var card a2a.AgentCard
	if err := json.NewDecoder(cardResp.Body).Decode(&card); err != nil {
		t.Fatalf("agent card response is not valid json: %v", err)
	}
	if card.Name != "Aquifer" {
		t.Fatalf("unexpected agent card name: %s", card.Name)
	}

	reqBody := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"data":{"user_id":"agent-1","idempotent_key":"http-e2e-1","url":%q,"method":"GET"}}]}}}`, upstream.URL)
	rpcResp, err := client.Post(publicURL+"/", "application/json", strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("json-rpc POST: %v", err)
	}
	defer rpcResp.Body.Close()

	var decoded map[string]any
	if err := json.NewDecoder(rpcResp.Body).Decode(&decoded); err != nil {
		t.Fatalf("json-rpc response is not valid json: %v", err)
	}
	if decoded["error"] != nil {
		t.Fatalf("json-rpc call returned an error: %v", decoded["error"])
	}
	if decoded["result"] == nil {
		t.Fatalf("json-rpc call returned no result")
	}

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("adapter.Start returned an error after shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("adapter did not shut down in time")
	}
}
