# Aquifer — API Aqueduct

**Self-hosted API request queue. Controls the pace of inbound and outbound traffic so partial outages don't cascade.**

---

## The problem

APIs get hit in bursts — by agents, schedulers, or high-volume clients. Your backend gets overwhelmed on inbound. Your app gets 429s on outbound. One slow dependency takes everything else down with it.

Aquifer absorbs the burst, queues requests durably to SQLite, and releases them at the rate you configure. Your backend decides the pace. The upstream decides the pace. Whoever needs to slow things down — wins.

---

## Two ways to use it

**Inbound — protect your API**
```
agents / clients  →  POST /jobs to Aquifer  →  your backend (at controlled RPS)
```
Agents hammering your API? Aquifer queues their requests and drains them to your backend at a pace it can handle. Your backend returns `X-Aquifer-Rps` headers to signal how fast it wants traffic in real time.

**Outbound — respect external APIs**
```
your app  →  POST /jobs to Aquifer  →  OpenAI / Stripe / any API (at controlled RPS)
```
Calling a rate-limited upstream? Aquifer queues the calls and dispatches them at your configured rate. If the upstream signals a slowdown via headers, Aquifer backs off automatically.

In both cases — **the upstream response headers are the final say on pace.** Your config sets the ceiling. Headers can only reduce below it, never exceed it. When pressure clears, the rate recovers gradually back to your ceiling.

---

## How it works

1. Client POSTs a job (target URL, method, headers, body, webhook URL) and moves on
2. Aquifer persists it to SQLite — survives crashes, re-dispatches on restart
3. A per-upstream worker dispatches at your configured RPS with jitter
4. On completion Aquifer POSTs your webhook with the response body and status
5. The upstream can adjust the rate live via `X-Aquifer-*` response headers

---

## Quick start

**Binary**
```bash
go install github.com/rjpruitt16/aquifer@latest
aquifer
```

**Docker**
```bash
docker run -p 8080:8080 -v $(pwd)/data:/data \
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
| `PORT`        | `8080`       | HTTP listen port               |
| `DB_PATH`     | `aquifer.db` | SQLite database path           |
| `CONFIG_PATH` | _(none)_     | Path to rate limit config YAML |

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

## Dynamic rate control

The upstream controls pace at runtime via response headers:

| Header                      | Effect                                       |
|-----------------------------|----------------------------------------------|
| `X-Aquifer-Rps`             | Reduce dispatch rate to this value           |
| `X-Aquifer-Max-Concurrent`  | Reduce max in-flight requests                |
| `X-Aquifer-Account-Queue`   | `enabled` — isolate each tenant's queue      |

With `X-Aquifer-Account-Queue: enabled`, each `(user_id, api_key)` pair gets its own independently paced queue. One tenant's burst can't slow down another.

---

## Autoscaling

Aquifer sends machine load data as headers on every outgoing request to your service:

| Header                    | Value                                              |
|---------------------------|----------------------------------------------------|
| `X-Aquifer-Total-Jobs`    | Total jobs on this machine right now               |
| `X-Aquifer-Queue-Depth`   | Jobs waiting to be dispatched                      |
| `X-Aquifer-Flow-Rate`     | Current dispatch rate (RPS) for this queue         |

Your service reads these headers and calls your autoscaler when the queue is growing:

```python
total_jobs = int(request.headers.get("X-Aquifer-Total-Jobs", 0))

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
