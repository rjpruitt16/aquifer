# API reference

## POST /jobs

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

**Do not expose Aquifer directly to untrusted callers.** `url` is dispatched as a real HTTP request — if an arbitrary or untrusted party can set it, Aquifer becomes an open relay/SSRF vector: it can be pointed at your internal network, cloud metadata endpoints (`169.254.169.254`), or anything else the machine Aquifer runs on can reach, using Aquifer's own network position and identity. The intended caller is **your own trusted backend or gateway code** dispatching to a destination it already knows about — not an agent, end user, or any other party choosing the destination itself. Run Aquifer on a private network, not bound to a public address, and put your own authorization and destination allow-listing in front if agents need to reach it indirectly.

Idempotent — duplicate `idempotent_key` per `user_id` returns the existing job.

**201** new job queued · **200 + `"duplicate": true`** already exists

## POST /proxy

Edge-gateway mode — see [Use cases](README.md#use-cases) for the deployment shape this is for. Same request body as `POST /jobs`, same idempotency/admission rules, but tries the upstream directly and synchronously first:

- **Succeeds directly** (2xx, no overload signal): the real upstream's status, headers, and body are relayed back verbatim, on this same connection. The queue is never touched.
- **Fails or the upstream signals overload** (timeout, 5xx, `429`, or an ORCA fallback threshold): falls back to the exact same durable-queue-and-delivery path `POST /jobs` uses — the connection seamlessly becomes the same SSE stream `GET /jobs/:id/stream` provides, rather than requiring a second call.

A domain that trips an overload signal has its direct attempts skipped entirely for a cooldown window — anchored to the upstream's own `Retry-After` header when it sends one (× a configurable safety multiplier, default 3), so a sustained outage doesn't cost every subsequent request the latency of a doomed direct attempt. Once the cooldown elapses, the next request is itself a real probe against the live upstream.

Pool-routed jobs (`pool_id` instead of `url`) always fall straight to queue+stream — there's no single canonical upstream to try directly.

```bash
curl -N -X POST http://localhost:8080/proxy -d '{ ... same shape as POST /jobs ... }'
```

## GET /jobs/:id

```json
{
  "job_id":     "a3f9...",
  "status":     "queued | in_flight | completed | failed",
  "url":        "https://api.openai.com/v1/chat/completions",
  "method":     "POST",
  "created_at": 1715000000000
}
```

## GET /jobs/:id/stream

Server-Sent Events stream for live job updates: `queued` → `dispatching` → `completed` (`{"job_id","response_status","body"}`) or `failed` (`{"job_id","reason"}`), plus a `position` event every 2s while queued. Connecting late is safe — you'll receive synthetic catchup events for states you missed. SSE is a convenience, not the source of truth: the webhook fires regardless of whether the stream was ever open.

```bash
curl -N http://localhost:8080/jobs/<id>/stream
```

## GET /health

```json
{
  "status": "ok",
  "l8_protocol": "0.1",
  "l8_public_key": "...",
  "admission": {
    "enabled": true,
    "memory_mb": 42,
    "memory_limit_mb": 400,
    "max_body_bytes": 1048576,
    "db_bytes": 81920,
    "db_max_bytes": 104857600,
    "retry_after_seconds": 5
  }
}
```

`admission.enabled` is `false` (with only that key present) when none of the
`AQUIFER_*` admission env vars are set.

## Webhooks

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

**Webhook delivery uses the same account-queue pacing as forward dispatch.** A webhook POST isn't fired immediately from the dispatch goroutine — it's enqueued as its own durable job, keyed by the webhook receiver's domain, and dispatched through the identical `AccountQueue`/`URLWorker` machinery described in the [Dynamic Pacing](README.md#dynamic-pacing) section of the README. Practically, this means:

- A webhook receiver can slow Aquifer down with the same `X-Aqueduct-Rps` / `X-Aqueduct-Max-Concurrent` response headers a real upstream uses, instead of just getting hammered.
- Delivery is crash-durable — a webhook still pending when the process restarts is recovered and retried, the same way a queued job is, rather than being lost with an in-memory retry loop.
- Retries trigger on `5xx` responses (not every non-`2xx`), matching forward dispatch's own retry condition — up to 4 attempts, exponential backoff 1 s · 2 s · 4 s · 8 s.
- L8 signing (see [README.md](README.md#l8-protocol--trustless-webhook-delivery)) still applies exactly as before — trust is established and delivery is signed the same way, just from inside the paced dispatch path instead of a separate one-shot retry loop.

Delivery is still at-least-once — see [Delivery semantics](README.md#how-it-works). (Drain mode's own ledger-flush webhook is unaffected — it stays synchronous, confirming delivery before clearing the local idempotency ledger.)

## Autoscaling

| Header                    | Value                                              |
|---------------------------|----------------------------------------------------|
| `X-Aqueduct-Total-Jobs`   | Total jobs on this machine right now               |
| `X-Aqueduct-Queue-Depth`  | Jobs waiting to be dispatched                      |
| `X-Aqueduct-Flow-Rate`    | Current dispatch rate (RPS) for this queue         |

Traditional load balancers and autoscalers rely on failure to start scaling — capacity only responds once something's already struggling. Aquifer treats resources as fluid, not fixed: it paces down as machines show strain and back up as capacity comes online, using the same signals it already reports on every response.
