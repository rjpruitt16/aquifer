// Package a2aadapter exposes Aquifer as an A2A (Agent2Agent protocol, v1.0)
// agent over JSON-RPC/HTTPS. It lives in its own subpackage — like every
// other non-stdlib adapter would — so the a2a-go SDK dependency doesn't leak
// into the core module for consumers who only need the HTTP or MCP adapters.
package a2aadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/rjpruitt16/aquifer"
)

// Adapter implements aquifer.FrameworkAdapter, serving Aquifer's job queue
// as an A2A agent.
type Adapter struct {
	addr string
	// publicURL is the externally-reachable base URL for this adapter
	// (e.g. "http://localhost:8090"). It's used both to advertise the
	// agent's interface in the Agent Card and to build the loopback
	// webhook-sink URL below — Aquifer's own address, since this adapter
	// process is what's listening on addr.
	publicURL string
}

// NewAdapter creates an A2A adapter listening on addr (e.g. ":8090") and
// advertising publicURL (e.g. "http://localhost:8090") as its reachable
// base URL. publicURL is required — the Agent Card must declare an
// absolute URL, and there's no reliable way to derive one from a bare
// listen address alone (behind a proxy, addr and the public URL differ).
func NewAdapter(addr, publicURL string) *Adapter {
	return &Adapter{addr: addr, publicURL: strings.TrimSuffix(publicURL, "/")}
}

// Name implements aquifer.FrameworkAdapter.
func (a *Adapter) Name() string {
	return "a2a"
}

// sinkPath is where Execute points JobRequest.WebhookURL when the caller
// registered no real A2A push config. JobRequest.Validate requires a
// non-empty WebhookURL unconditionally (job.go), but A2A's SendMessage
// doesn't require a push config up front — most callers rely on the
// streaming response or GetTask polling instead. This route exists purely
// to satisfy that core validation; it accepts and discards the callback
// Aquifer's own webhook delivery (webhook.go's deliverWebhook, with its
// 4-retry backoff) sends here, since Execute already gets the same
// completion/failure information directly off aquifer.SubscribeJob.
const sinkPath = "/internal/a2a-webhook-sink"

// Start implements aquifer.FrameworkAdapter.
func (a *Adapter) Start(ctx context.Context, aq *aquifer.Aquifer) error {
	exec := &executor{aq: aq, sinkURL: a.publicURL + sinkPath}

	capabilities := &a2a.AgentCapabilities{
		Streaming:         true,
		PushNotifications: true,
	}

	handler := a2asrv.NewHandler(exec,
		a2asrv.WithCapabilityChecks(capabilities),
		// Real A2A task-event push notifications (CreateTaskPushNotificationConfig),
		// distinct from the WebhookURL/sink wiring above: this is the SDK's own
		// SSRF-hardened HTTP sender delivering translated a2a.Events, not Aquifer's
		// raw job-webhook payloads.
		a2asrv.WithPushNotifications(push.NewInMemoryStore(), push.NewHTTPPushSender(nil)),
	)

	mux := http.NewServeMux()
	mux.Handle("/", a2asrv.NewJSONRPCHandler(handler))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(agentCard(a.publicURL, *capabilities)))
	mux.HandleFunc("POST "+sinkPath, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{Addr: a.addr, Handler: mux}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func agentCard(publicURL string, capabilities a2a.AgentCapabilities) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "Aquifer",
		Description: "Durable, rate-controlled HTTP dispatch queue, reachable over A2A.",
		Version:     "0.1.0",
		Provider: &a2a.AgentProvider{
			Org: "rjpruitt16",
			URL: "https://github.com/rjpruitt16/aquifer",
		},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface(publicURL, a2a.TransportProtocolJSONRPC),
		},
		Capabilities:       capabilities,
		DefaultInputModes:  []string{"application/json"},
		DefaultOutputModes: []string{"application/json"},
		Skills: []a2a.AgentSkill{
			{
				ID:   "dispatch-http-request",
				Name: "Dispatch HTTP request",
				Description: "Queue an HTTP request for durable, rate-controlled dispatch to a target " +
					"URL or registered pool. Send a message with a single data part shaped like " +
					"aquifer.JobRequest (user_id, idempotent_key, url or pool_id, method, headers, " +
					"body). The upstream response is returned as a task artifact.",
				Tags: []string{"http", "queue", "webhook"},
				Examples: []string{
					`{"user_id":"agent-1","idempotent_key":"reset-1","url":"https://api.example.com/reset","method":"POST"}`,
				},
			},
		},
	}
}

// executor implements a2asrv.AgentExecutor, translating A2A SendMessage/
// SendStreamingMessage calls into aquifer.Enqueue + aquifer.SubscribeJob,
// and Aquifer's SSE event stream into a2a.Events.
type executor struct {
	aq      *aquifer.Aquifer
	sinkURL string
}

var _ a2asrv.AgentExecutor = (*executor)(nil)

// Execute implements a2asrv.AgentExecutor.
func (e *executor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		req, err := jobRequestFromMessage(execCtx.Message, execCtx.Tenant)
		if err != nil {
			yield(nil, fmt.Errorf("%w: %v", a2a.ErrInvalidParams, err))
			return
		}
		if req.WebhookURL == "" {
			req.WebhookURL = e.sinkURL
		}

		if execCtx.StoredTask == nil {
			if !yield(a2a.NewSubmittedTask(execCtx, execCtx.Message), nil) {
				return
			}
		}

		result, err := e.aq.Enqueue(*req)
		if err != nil {
			failMsg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(err.Error()))
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed, failMsg), nil)
			return
		}

		if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
			return
		}

		job, events, unsubscribe, err := e.aq.SubscribeJob(result.JobID)
		if err != nil {
			failMsg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(err.Error()))
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed, failMsg), nil)
			return
		}
		defer unsubscribe()

		// A duplicate idempotent_key can return a job that already reached a
		// terminal state — its dispatch goroutine already ran and published
		// its one completed/failed event, which nothing here was subscribed
		// to receive. Without this check Execute would hang forever waiting
		// on a channel that will never fire again. The response body from
		// that original dispatch isn't retrievable at this point (Job itself
		// doesn't retain it), so this is a best-effort terminal notification,
		// not a replay of the original result.
		switch job.Status {
		case aquifer.StatusCompleted:
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, alreadyTerminalMessage(job.ID, "completed")), nil)
			return
		case aquifer.StatusFailed:
			yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed, alreadyTerminalMessage(job.ID, "failed")), nil)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				switch ev.Event {
				case "dispatching":
					if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking, nil), nil) {
						return
					}
				case "position":
					// Queue-position hint only, no A2A task-state equivalent -- skip.
				case "completed":
					if !yield(a2a.NewArtifactEvent(execCtx, a2a.NewDataPart(ev.Data)), nil) {
						return
					}
					yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCompleted, nil), nil)
					return
				case "failed":
					failMsg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewDataPart(ev.Data))
					yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateFailed, failMsg), nil)
					return
				}
			}
		}
	}
}

// Cancel implements a2asrv.AgentExecutor. Aquifer has no job-cancellation
// mechanism at all today (store.DeleteJob's only caller rolls back an
// admission-rejected row, not an in-flight/queued cancel-on-request) -- so
// this deliberately does not fake support with a no-op success. Returning
// ErrUnsupportedOperation is the honest answer; real cancellation is a
// separate, sizeable core feature, not adapter glue.
func (e *executor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(nil, a2a.ErrUnsupportedOperation)
	}
}

func alreadyTerminalMessage(jobID, status string) *a2a.Message {
	return a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(
		fmt.Sprintf("job %s already reached a terminal state (%s) before this task subscribed; "+
			"original response is not retained for replay", jobID, status),
	))
}

// jobRequestFromMessage extracts an aquifer.JobRequest from the first data
// part of an incoming A2A message, mirroring the structured-JSON-args shape
// MCP's aquifer_enqueue_job tool already uses rather than attempting any
// natural-language interpretation. If the request doesn't set user_id, the
// A2A request's tenant (AgentInterface.tenant, threaded through as
// ExecutorContext.Tenant) is used instead -- letting one adapter endpoint
// serve multiple tenants via A2A's own routing field rather than assuming
// a single undifferentiated identity.
func jobRequestFromMessage(msg *a2a.Message, tenant string) (*aquifer.JobRequest, error) {
	if msg == nil {
		return nil, errors.New("message is required")
	}

	for _, part := range msg.Parts {
		data := part.Data()
		if data == nil {
			continue
		}

		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to re-marshal data part: %w", err)
		}

		var req aquifer.JobRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, fmt.Errorf("data part is not a valid job request: %w", err)
		}
		if req.UserID == "" {
			req.UserID = tenant
		}
		return &req, nil
	}

	return nil, errors.New(
		"message must contain a data part shaped like aquifer.JobRequest " +
			"(user_id, idempotent_key, url or pool_id, method, headers, body)",
	)
}
