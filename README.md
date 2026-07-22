# Aquifer — MCP Traffic Framework

**Self-hosted MCP server framework for coordinating HTTP traffic from distributed agents. Aquifer absorbs retry storms before they turn into a bigger LLM bill — durable queuing, controlled dispatch pace, and cryptographic agent identity via the L8 protocol, exposed through pluggable adapters.**

Built by [Rahmi Pruitt](https://rahmipruitt.me) — open to AI infra consulting, founding engineer, and contract work.

---

## The problem

Distributed agents call tools and APIs in bursts. Your backend gets overwhelmed on inbound. Your app gets 429s on outbound. One slow dependency takes everything else down with it.

Aquifer gives those agents a coordination layer. It absorbs the burst, queues requests durably to SQLite, and releases them at the rate you configure. Your backend decides the pace. The upstream decides the pace. Whoever needs to slow things down — wins.

---

## Two ways to use it

**MCP tools — coordinate distributed agents**
```
agents / MCP clients  →  aquifer_enqueue_job  →  Aquifer queue  →  target API
```
Agents call Aquifer as an MCP server instead of racing each other directly against the same backend or external API. Aquifer returns a job id immediately, dispatches the request at a controlled rate, and delivers the result to your webhook.

**HTTP API — protect your API**
```
agents / clients  →  POST /jobs to Aquifer  →  your backend (at controlled RPS)
```
Agents hammering your API over HTTP? Aquifer queues their requests and drains them to your backend at a pace it can handle. Your backend returns `X-Aqueduct-Rps` headers to signal how fast it wants traffic in real time.

**Outbound — respect external APIs**
```
your app  →  POST /jobs to Aquifer  →  OpenAI / Stripe / any API (at controlled RPS)
```
Calling a rate-limited upstream? Aquifer queues the calls and dispatches them at your configured rate. If the upstream signals a slowdown via headers, Aquifer backs off automatically.

In both cases — **the upstream response headers are the final say on pace.** Your config sets the ceiling. Headers can only reduce below it, never exceed it. When pressure clears, the rate recovers gradually back to your ceiling.

This is not only rate limiting after something breaks. It is dynamic pacing before the failure. Services you control can tell agent traffic to slow down while they keep serving requests, giving autoscalers time to add capacity instead of forcing clients into retries, 429 storms, or cascading outages. If many tools, agents, and services speak the same pacing headers, traffic across the internet can coordinate more gracefully instead of every client guessing alone.

---

## How it works

1. Client submits a job through an adapter (MCP tool or HTTP endpoint) and moves on
2. Aquifer persists it to SQLite — survives crashes, re-dispatches on restart
3. A per-upstream worker dispatches at your configured RPS with jitter
4. On completion Aquifer POSTs your webhook with the response body and status
5. The upstream can adjust the rate live via `X-Aqueduct-*` response headers

---

## Quick start

**Binary**
```bash
go install github.com/rjpruitt16/aquifer/cmd/aquifer@latest
aquifer
```

**Docker**
```bash
docker run -p 8080:8080 -v $(pwd)/data:/data \
  -e AQUIFER_ADAPTER=http \
  -e DB_PATH=/data/aquifer.db \
  ghcr.io/rjpruitt16/aquifer
```

**Fly.io**
```bash
git clone https://github.com/rjpruitt16/aquifer
cd aquifer
flyctl launch --name my-aquifer --no-deploy
flyctl volumes create aquifer_data --size 1 --region iad
flyctl deploy
```

---

## Configuration

Set `CONFIG_PATH` to a YAML file to configure rate limits per upstream hostname:

```yaml
# aquifer.yml — copy from aquifer.example.yml
defaults:
  rps: 2
  max_concurrent: 1

upstreams:
  api.openai.com:
    rps: 10
    max_concurrent: 3
  api.stripe.com:
    rps: 20
    max_concurrent: 5
  your-backend.internal:
    rps: 50
    max_concurrent: 10
```

| Env var       | Default      | Description                    |
|---------------|--------------|--------------------------------|
| `AQUIFER_ADAPTER` | `http` for binary, `mcp-stdio` in Docker image | Runtime adapter: `http` or `mcp-stdio` |
| `PORT`        | `8080`       | HTTP listen port               |
| `DB_PATH`     | `aquifer.db` | SQLite database path           |
| `CONFIG_PATH` | _(none)_     | Path to rate limit config YAML |

---

## Framework adapters

Aquifer has a framework-neutral core and adapter front doors. The core owns idempotency, persistence, rate control, dispatch, SSE events, L8 signing, and webhook delivery. Adapters translate framework-specific calls into that core.

```go
type FrameworkAdapter interface {
    Name() string
    Start(ctx context.Context, aquifer *Aquifer) error
}
```

Current adapters:

| Adapter | Env | Purpose |
|---------|-----|---------|
| HTTP | `AQUIFER_ADAPTER=http` | Existing REST/SSE API on `PORT` |
| MCP stdio | `AQUIFER_ADAPTER=mcp-stdio` | MCP server exposing Aquifer tools over stdio |

Run as an MCP stdio server:

```bash
AQUIFER_ADAPTER=mcp-stdio aquifer
```

The published Docker image defaults to `AQUIFER_ADAPTER=mcp-stdio` so MCP directories such as Glama can start and introspect it directly. Set `AQUIFER_ADAPTER=http` when running Aquifer as an HTTP queue service.

MCP tools:

| Tool | Purpose |
|------|---------|
| `aquifer_enqueue_job` | Queue an HTTP request for durable, rate-controlled dispatch |
| `aquifer_get_job` | Fetch job status and metadata |
| `aquifer_health` | Return health and protocol metadata |
| `aquifer_l8_metadata` | Return L8 public key metadata |
| `aquifer_l8_challenge` | Answer an L8 challenge |

MCP resources:

| Resource | Purpose |
|----------|---------|
| `aquifer://jobs/{job_id}` | Read current job status and metadata as JSON |

The HTTP adapter remains the default so existing deployments do not change.

### Writing an adapter

Adapter authors import Aquifer as a Go package, implement `FrameworkAdapter`, and pass the shared core into their framework. Built-in adapters are selected with `AQUIFER_ADAPTER`; third-party adapters normally ship as small custom binaries that call `aquifer.RunAdapter`.

```go
package myframework

import (
    "context"

    "github.com/rjpruitt16/aquifer"
)

type Adapter struct{}

func (a *Adapter) Name() string {
    return "my-mcp-framework"
}

func (a *Adapter) Start(ctx context.Context, app *aquifer.Aquifer) error {
    // Register framework handlers that call:
    // app.Enqueue(req)
    // app.GetJob(jobID)
    // app.SubscribeJob(jobID)
    // app.Health()
    return nil
}
```

Custom binaries can reuse Aquifer's runtime wiring:

```go
package main

import (
    "context"
    "log"

    "github.com/rjpruitt16/aquifer"
    myadapter "github.com/you/your-adapter"
)

func main() {
    runtime := aquifer.NewRuntime(aquifer.RuntimeOptions{
        DBPath:     "aquifer.db",
        ConfigPath: "aquifer.yml",
    })
    runtime.RecoverQueuedJobs("aquifer.db")

    adapter := myadapter.New()
    log.Fatal(adapter.Start(context.Background(), runtime.Aquifer))
}
```

For the shortest form, let Aquifer create the runtime and start your adapter:

```go
adapter := myadapter.New()
log.Fatal(aquifer.RunAdapter(context.Background(), adapter, aquifer.RuntimeOptions{
    DBPath:     "aquifer.db",
    ConfigPath: "aquifer.yml",
}))
```

See `examples/custom_adapter` for a complete compile-tested adapter binary.

---

## Metrics adapter

Aquifer emits lifecycle events through a pluggable metrics adapter. Implement
`MetricsAdapter` and pass it into `NewRegistry`:

```go
type MetricsAdapter interface {
    JobQueued(userID, upstream string)
    JobDispatched(userID, upstream string)
    JobCompleted(userID, upstream string, durationMs int64)
    JobFailed(userID, upstream string, reason string)
    WebhookDelivered(url string, attempt int)
    WebhookFailed(url string, attempts int)
    QueueDepth(upstream string, depth int)
    FlowRate(upstream string, rps float64)
}
```

Aquifer ships with `NoopMetricsAdapter`, so existing deployments do not change.

---

## API

### POST /jobs

```json
{
  "user_id":        "user-123",
  "idempotent_key": "invoice-42-notify",
  "url":            "https://api.openai.com/v1/chat/completions",
  "method":         "POST",
  "headers":        { "Authorization": "Bearer sk-..." },
  "body":           "{\"model\":\"gpt-4o\",\"messages\":[...]}",
  "webhook_url":    "https://yourapp.com/webhooks/aquifer"
}
```

Idempotent — duplicate `idempotent_key` per `user_id` returns the existing job.

**201** new job queued · **200 + `"duplicate": true`** already exists

### GET /jobs/:id

```json
{
  "job_id":     "a3f9...",
  "status":     "queued | in_flight | completed | failed",
  "url":        "https://api.openai.com/v1/chat/completions",
  "method":     "POST",
  "created_at": 1715000000000
}
```

### GET /jobs/:id/stream

Server-Sent Events stream for live job updates.

```
event: queued
data: {"job_id":"a3f9...","status":"queued"}

event: dispatching
data: {"job_id":"a3f9..."}

event: completed
data: {"job_id":"a3f9...","response_status":200,"body":"..."}
```

Or `event: failed` with `{"job_id":"...","reason":"..."}`.

**Position updates** — while the job waits in queue, a position event is broadcast every 2 seconds:
```
event: position
data: {"job_id":"a3f9...","position":4}
```

```bash
curl -N http://localhost:8080/jobs/<id>/stream
```

Connecting late is safe — you'll receive synthetic `queued` and `dispatching` catchup events for states you missed.

**The Aqueduct Protocol** — SSE is the live view. Webhook is the guaranteed delivery. Both always fire regardless of whether the stream was open. Think of it like a phone call with voicemail: stay on the line (SSE) for real-time updates, or hang up and the result goes to voicemail (webhook). You never lose the result.

### GET /health

```json
{ "status": "ok" }
```

---

## Webhook payload

**Completed**
```json
{
  "job_id":          "a3f9...",
  "status":          "completed",
  "response_status": 200,
  "body":            "..."
}
```

**Failed** (after 4 retries with exponential backoff)
```json
{
  "job_id": "a3f9...",
  "status": "failed",
  "reason": "connection refused"
}
```

Webhook delivery retries 4 times: 1 s · 2 s · 4 s · 8 s.

---

## L8 Protocol — trustless webhook delivery

Traditional webhook security requires sharing a secret between sender and receiver and storing it in a database on both sides. Aquifer implements **L8 v0.1**, a lightweight challenge-response protocol that eliminates shared secrets entirely.

**The attack surface problem L8 solves:** A shared HMAC secret is something that can be stolen, accidentally logged, forgotten to rotate, or compromised on either side. A stolen secret lets anyone forge webhook deliveries forever. L8 replaces that shared secret with public key cryptography — there is no secret to steal from a database.

**How it works:**

1. The receiver publishes a public key at `GET /.well-known/l8`
2. Before the first delivery, Aquifer challenges the receiver to prove ownership of the corresponding private key — a one-time handshake
3. Trust is cached to disk as `l8-trust/{domain}.json` — the handshake never runs again for that domain
4. Every webhook delivery carries `X-L8-Signature` headers the receiver verifies locally with no database lookup and no round-trip to any authority

**Why this keeps things fast:** Verification is a single local Ed25519 `verify()` call against a cached public key. No database query, no HTTP call, no shared state. Microseconds.

**Key management:**

Set `L8_PRIVATE_KEY` (base64 Ed25519 private key) for a stable identity across restarts. Without it, Aquifer auto-generates a key and saves it to `.l8-key` on first start.

To revoke trust with a domain: delete `l8-trust/{domain}.json`. The handshake re-runs on next delivery.

**Aquifer exposes:**

| Endpoint | Purpose |
|---|---|
| `GET /.well-known/l8` | Aquifer's public key and capabilities — receivers discover Aquifer here |
| `POST /l8/challenge` | Handles incoming challenges from receivers verifying Aquifer's identity |
| `GET /l8-spec` | The full L8 protocol spec — served on any running Aquifer instance |

**Protocol version:** `0.1`. The version is advertised in `/.well-known/l8` and `GET /health` so agents can detect what capabilities are available. Future versions will add payload encryption (0.2) and formalized key rotation (0.3).

The full protocol spec and verification examples are in [L8-SPEC.md](L8-SPEC.md), also browsable at `GET /l8-spec` on any running instance. The spec documents the receiver-side endpoints any service needs to implement to receive signed webhooks.

See `tests/l8_receiver.py` for a complete reference implementation of the receiver side, and `tests/test_l8.py` for end-to-end tests that verify the handshake, signed delivery, and cryptographic signature validation.

---

## Dynamic Pacing

The upstream controls pace at runtime via response headers. `X-Aqueduct-*` is the protocol namespace; `X-Aquifer-*` remains supported as a backward-compatible product alias.

| Header                      | Effect                                       |
|-----------------------------|----------------------------------------------|
| `X-Aqueduct-Rps`            | Reduce dispatch rate to this value           |
| `X-Aqueduct-Max-Concurrent` | Reduce max in-flight requests                |
| `X-Aqueduct-Account-Queue`  | `enabled` — isolate each tenant's queue      |

With `X-Aqueduct-Account-Queue: enabled`, each `(user_id, api_key)` pair gets its own independently paced queue. One tenant's burst can't slow down another.

Aquifer reads both namespaces, preferring `X-Aqueduct-*` when both are present:

| Preferred | Compatibility alias |
|-----------|---------------------|
| `X-Aqueduct-Rps` | `X-Aquifer-Rps` |
| `X-Aqueduct-Max-Concurrent` | `X-Aquifer-Max-Concurrent` |
| `X-Aqueduct-Account-Queue` | `X-Aquifer-Account-Queue` |

Dynamic pacing is useful for your own servers because it lets them shed pressure gradually while still making progress. A backend can lower RPS when CPU, queue depth, database latency, or downstream dependency pressure rises; Aquifer will honor that lower pace immediately, and then recover gradually toward the configured ceiling when pressure clears.

---

## Autoscaling

Aquifer sends machine load data as headers on every outgoing request to your service. It sends both `X-Aqueduct-*` and `X-Aquifer-*` names for compatibility.

| Header                    | Value                                              |
|---------------------------|----------------------------------------------------|
| `X-Aqueduct-Total-Jobs`   | Total jobs on this machine right now               |
| `X-Aqueduct-Queue-Depth`  | Jobs waiting to be dispatched                      |
| `X-Aqueduct-Flow-Rate`    | Current dispatch rate (RPS) for this queue         |

Your service reads these headers and calls your autoscaler when the queue is growing:

```python
total_jobs = int(request.headers.get("X-Aqueduct-Total-Jobs", 0))

if total_jobs > 500:
    scale_up()  # call Fly.io, AWS ASG, k8s HPA, etc.
```

This keeps the autoscaling decision in your hands — Aquifer exposes the signal, your service acts on it however fits your infrastructure.

---

## Reliability

- **Durable queue** — jobs persist to SQLite on every write
- **Crash recovery** — queued jobs re-dispatched automatically on restart
- **In-flight tracking** — jobs marked `in_flight` before dispatch; recovered immediately on panic without waiting for full restart
- **Stale job safety net** — in-flight jobs older than 5 min automatically reset to `queued`
- **Per-job panic isolation** — a panic in one job marks it failed and delivers the webhook; the worker keeps running

---

## Job TTLs

| Status      | TTL    |
|-------------|--------|
| `queued`    | 24 h   |
| `completed` | 30 min |
| `failed`    | 2 h    |

---

## Deployment model

Aquifer is designed as a **sidecar on a single machine**. One instance per app server, SQLite on a local persistent volume — no external database, no coordination overhead.

Running multiple instances against the same upstream without partitioning will multiply your request rate. If you scale horizontally, partition by upstream domain or tenant so each instance owns a distinct key space.

---

## License

MIT
