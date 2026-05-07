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

## Dynamic rate control

The upstream controls pace at runtime via response headers:

| Header                      | Effect                                       |
|-----------------------------|----------------------------------------------|
| `X-Aquifer-Rps`             | Reduce dispatch rate to this value           |
| `X-Aquifer-Max-Concurrent`  | Reduce max in-flight requests                |
| `X-Aquifer-Account-Queue`   | `enabled` — isolate each tenant's queue      |

With `X-Aquifer-Account-Queue: enabled`, each `(user_id, api_key)` pair gets its own independently paced queue. One tenant's burst can't slow down another.

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
