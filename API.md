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

As a second, narrower layer on top of that — `AQUIFER_ALLOWED_URL_DOMAINS` (comma-separated hostnames, e.g. `api.openai.com,internal.yourapp.com`) restricts which domains `url` is allowed to target at all; a subdomain of an allowed entry is permitted (`api.example.com` matches an allowlisted `example.com`). Unset by default — unrestricted, unchanged behavior — this doesn't replace the network/authorization guidance above, it's what Aquifer itself can enforce regardless of who's calling it. Applies to `url`-routed jobs only; pool-routed jobs (`pool_id`) have no caller-supplied destination to check.

Idempotent — duplicate `idempotent_key` per `user_id` returns the existing job.

**201** new job queued · **200 + `"duplicate": true`** already exists

## Static cluster routing

Static cluster routing is optional HTTP-level partitioning for `POST /jobs` and `POST /proxy`. When enabled, every node builds the same rendezvous-hash ranking by `user_id`: if the receiving node is the highest-ranked healthy owner, it handles the request locally; otherwise it forwards to the highest-ranked peer and relays that peer's response back to the caller. Callers can hit any node; they do not need to know the topology.

If a peer cannot be reached during forwarding, Aquifer temporarily soft-prunes that member from this node's local routing view and tries the next ranked candidate. This is not gossip or consensus: the configured member list remains the source of truth, and the failed peer rejoins this node's routing view automatically when the prune TTL expires.

Configuration:

| Env var | Default | Description |
|---|---|---|
| `AQUIFER_CLUSTER_ENABLED` | `false` | Enables HTTP cluster routing |
| `AQUIFER_CLUSTER_SELF_ID` | _(required when enabled)_ | Stable id for this node |
| `AQUIFER_CLUSTER_SELF_ADDR` | _(required when enabled)_ | Base HTTP URL other nodes use to reach this node |
| `AQUIFER_CLUSTER_MEMBERS` | _(none)_ | Comma-separated `id=http://host:port` entries for the other known nodes; `SELF` is added automatically if omitted |
| `AQUIFER_CLUSTER_PRUNE_TTL_SECONDS` | `30` | How long this node excludes an unreachable peer before trying it again |

Example:

```bash
AQUIFER_CLUSTER_ENABLED=true
AQUIFER_CLUSTER_SELF_ID=aquifer-a
AQUIFER_CLUSTER_SELF_ADDR=http://aquifer-a:8080
AQUIFER_CLUSTER_MEMBERS=aquifer-b=http://aquifer-b:8080,aquifer-c=http://aquifer-c:8080
```

This is not a distributed database or a Redis Cluster clone. Aquifer still stores idempotency and account-queue state locally. Rendezvous ranking reduces tenant fragmentation during normal routing, but membership changes or soft pruning can move a `user_id` to a node that does not have that user's previous idempotency records. During that window, duplicate execution is possible unless a shared control plane such as Canalis or Valkey remote idempotency owns cross-node idempotency.

Membership changes can also temporarily create duplicate account queues for the same upstream URL on different nodes: old work may still be draining on the previous owner while new work hashes to the new owner. That is expected during rebalance/reconfiguration and should be short-lived under normal TTL/drain cleanup. If that tradeoff is unacceptable, keep membership stable or put a shared assignment/control plane in front.

## POST /proxy

Edge-gateway mode — see [Use cases](README.md#use-cases) for the deployment shape this is for. Same request body as `POST /jobs`, same idempotency/admission rules, but tries the upstream directly and synchronously first:

- **Succeeds directly** (2xx, or any status not classified as overload — see below): the real upstream's status, headers, and body are relayed back verbatim, on this same connection. The queue is never touched. A response the upstream didn't mark as overload (a plain `500`, a `404`, anything unclassified) is a **success** in this sense too — relayed directly, not queued, since nothing said it shouldn't be.
- **Fails, or the upstream signals overload** (timeout, a classified status code, or an ORCA fallback threshold): falls back to the exact same durable-queue-and-delivery path `POST /jobs` uses — the connection seamlessly becomes the same SSE stream `GET /jobs/:id/stream` provides, rather than requiring a second call. The very first event on that stream is `event: proxy_fallback`, `data: {"job_id", "reason", "upstream_status"}` (status omitted when no real response was received — a skipped attempt or a timeout) — so a client with no server of its own to explain this any other way (a browser, an agent) sees explicitly why it's in the queue before the normal `queued`/`dispatching`/terminal sequence starts. `reason` is one of `upstream_overloaded`, `upstream_unreachable`, `domain_degraded` (breaker open or this domain's queue already has backlog), or `pool_routed`.

**Which status codes count as overload is configurable, split into two kinds — queue locally, or also try cross-region redirect first (see below) — not lumped together the way "any 5xx" used to be.** Defaults are deliberately narrow: `429` → queue, `503` → reroute. Everything else (a plain `500`, `502`, `504`, `404`, anything not explicitly classified) is relayed to the caller as a normal, if unfortunate, direct response — not treated as overload at all. The reasoning: `429` is usually a *global* per-key/per-account rate limit, not a regional one — rerouting to a sibling region wouldn't help, since it hits the identical limit on the identical upstream, so it stays queue-only. `503` and an ORCA overload signal are reroute-eligible by default, since they more plausibly indicate *this* region/instance specifically is struggling.

Your own upstream can configure its own sets via response headers, comma-separated, each entry either a literal code or an HTTP status class (`5xx` matches every `500`–`599`):

| Header | Default | Effect |
|---|---|---|
| `X-Aqueduct-Queue-Codes` (or `X-Aquifer-Queue-Codes`) | `429` | Statuses that mean "queue this domain locally" |
| `X-Aqueduct-Reroute-Codes` (or `X-Aquifer-Reroute-Codes`) | `503` | Statuses that mean "also try cross-region redirect first" |

Due diligence is on you to say so if your upstream uses something nonstandard — e.g. `X-Aqueduct-Reroute-Codes: 502,503,504` to widen reroute eligibility, or `X-Aqueduct-Reroute-Codes: 5xx` to sweep in every 5xx the way earlier versions of this feature did unconditionally.

A domain that trips an overload signal has its direct attempts skipped entirely — anchored to the upstream's own `Retry-After` header when it sends one (× a configurable safety multiplier, default 3) as the minimum cooldown, but direct attempts stay skipped for as long as the domain's queue actually has real backlog, even past that cooldown — a fixed timer alone doesn't know whether the traffic it caused has finished draining. Once both the cooldown has elapsed *and* the queue is genuinely empty, the next request is itself a real probe against the live upstream. Which kind (queue vs. reroute) tripped the breaker is remembered for the whole cooldown — a domain breaker-tripped by a `429` stays queue-only on every retry during that window, not reroute-eligible just because *some* overload happened.

**Routine local backlog — no breaker tripped at all, just a queue with real jobs in it — never triggers redirect on its own**, even with the feature configured. A domain paced at a low rps under a normal traffic burst has jobs sitting in queue as a matter of course; that's Aquifer working as designed, not a signal anything is regionally degraded. Redirect only ever gets tried alongside an actual tripped breaker or a failed/overloaded direct attempt.

The upstream can also proactively request queueing itself, on an otherwise-healthy response: `X-Aqueduct-Queue-Active: true` (or the product alias `X-Aquifer-Queue-Active`) trips the same breaker (always queue-kind, never reroute — matching its own name) for future requests to that domain — without discarding the response that already came back. Useful for "I'm nearing capacity, stop firing directly at me" ahead of an actual overload status.

Pool-routed jobs (`pool_id` instead of `url`) always fall straight to queue+stream — there's no single canonical upstream to try directly.

```bash
curl -N -X POST http://localhost:8080/proxy -d '{ ... same shape as POST /jobs ... }'
```

### Cross-region redirect (Fly.io)

If `AQUIFER_FLY_REGIONS` is set, an `upstream_unreachable` fallback, or an `upstream_overloaded`/`domain_degraded` fallback specifically classified as reroute-eligible (see `X-Aqueduct-Reroute-Codes` above — `503` by default, not every overload), tries other regions Aquifer is deployed to — live, over Fly's private network — before falling back to this instance's own local queue. Off by default; unset, `/proxy` behaves exactly as described above with zero change.

When it triggers: every known-live region is tried for a fast direct success first, nearest (lowest measured round-trip time from the same health check that determines a region is live — Fly doesn't publish a region distance/latency table, so this doubles as the only real proximity signal available) first, except that two callers racing the same job always try one particular region first regardless of latency, so they tend to converge on the same region rather than each racing off after their own nearest option. If none can serve it directly, that same region is the one chosen to accept it into its own durable queue, and its live event stream is relayed back onto your original connection, so you see one continuous stream regardless of which region actually ends up handling the job.

A reroute is never silent to the caller — same principle as `proxy_fallback` above: a client with no server of its own to explain this (a browser, an agent) shouldn't have to wonder why its connection is still open or where the response actually came from.
- **Direct success via redirect:** the response carries an `X-Aquifer-Served-By-Region` header naming which region actually served it, alongside the relayed status/headers/body — purely additive, doesn't change anything you're already parsing.
- **Queued on another region:** before relaying that region's own stream, origin fires `event: rerouted`, `data: {"region"}` — so it arrives *before* that region's own `proxy_fallback`/`queued`/`dispatching` sequence, the same ordering `proxy_fallback` itself uses relative to `queued`.

**A full queued-on-reroute sequence, start to finish** — this is what actually crosses the wire (Go's JSON encoder sorts map keys, so this is the literal byte-for-byte shape, not approximated). `position` fires every ~2s for as long as the job is genuinely still queued — real backpressure ticking down, not a fixed animation:

```
event: rerouted
data: {"region":"ord"}

event: proxy_fallback
data: {"job_id":"b7e2...","reason":"domain_degraded"}

event: queued
data: {"job_id":"b7e2...","status":"queued"}

event: position
data: {"job_id":"b7e2...","position":3}

event: position
data: {"job_id":"b7e2...","position":2}

event: position
data: {"job_id":"b7e2...","position":1}

event: dispatching
data: {"job_id":"b7e2..."}

event: completed
data: {"body":"...","job_id":"b7e2...","response_status":200}
```

`job_id` throughout is **target's** ID, not origin's — origin deletes its own local row the instant redirect succeeds (`fallbackOutcome`), so there's no origin-side ID for the client to have seen and compare against. `proxy_fallback` has no `upstream_status` field here because `domain_degraded` always passes status `0`; that field only appears on an `upstream_overloaded`/`upstream_unreachable` fallback, where a real (or attempted) upstream response actually came back. Terminal event is `completed` on success or `failed` (`{"job_id","reason","response_status","body"}`) on failure — same shape `GET /jobs/:id/stream` uses, because this literally *is* that stream, just relayed from `target` through `origin`'s connection.

If the original request set `webhook_url`, a webhook fires too — but separately, from **target** (whichever region actually completed the job), as its own independently-paced, durably-queued POST, not inline in this stream:

```
POST https://yourapp.com/webhooks/aquifer
Content-Type: application/json

{"job_id":"b7e2...","status":"completed","response_status":200,"body":"..."}
```

Both happen on success — the SSE `completed` event and the webhook — exactly the same contract every non-rerouted job already has (see [Webhooks](#webhooks) below). Reroute doesn't change that, it just changes which instance ends up firing it.

If literally no known-live region can help either — none live at all, or every one tried and failed — the request is **rejected**, not queued locally: **429**, `Retry-After` set to `AQUIFER_REDIRECT_EXHAUSTED_RETRY_AFTER_SECONDS` (default 900 — a real regional outage, not a transient blip), `limit_reason: "redirect_exhausted"`, same response shape as an admission-control rejection. This is deliberate: queueing locally instead was never actually decided, so a caller (or its own retry/alerting logic) finds out the whole fleet is degraded rather than the request silently landing on one struggling instance's queue. A future per-deployment option to queue locally instead — plausible for an Aquifer instance dedicated to a single customer, where "queue and eventually deliver" might beat erroring — is a real possibility, just not the default and not built yet. This is separate from `AQUIFER_REDIRECT_GATE_COOLDOWN_SECONDS` (default 500), which is purely internal — how long this instance avoids re-running the whole candidate tour after finding nothing live, independent of what it tells the caller.

**Honest limitation, not silently glossed over:** Aquifer's idempotency check remains per-instance (local SQLite/Pebble), unchanged by this feature. If the exact same `idempotent_key` is independently submitted to two different regions at nearly the same moment (a real scenario — a caller's own client retrying after a timeout can land on a different region via Fly's anycast), each region may independently begin its own redirect tour, and in rare cases the job could end up durably queued in two places. The deterministic region selection above narrows this window but does not close it. During cross-region redirect specifically, treat delivery as at-least-once, not exactly-once — standard practice for any webhook consumer, just worth calling out plainly here since it's a real, if narrow, exception to Aquifer's otherwise-exactly-once idempotency guarantee.

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

Delivery is still at-least-once — see [Delivery semantics](README.md#how-it-works). Drain mode's own ledger webhook is separate: it sends acknowledged batches from the local drain-event journal and deletes only the events confirmed by a `2xx` response. See [DRAIN_MODE.md](DRAIN_MODE.md) for that payload contract.

## Remote idempotency

Remote idempotency is optional and generic. When `AQUIFER_REMOTE_IDEMPOTENCY_ENABLED=true`, Aquifer still checks its local SQLite/Pebble store first. If the key is new locally, Aquifer performs a bounded lookup against Valkey before dispatching the job. A remote hit returns `duplicate:true` and deletes the speculative local row; a timeout or Valkey error falls back to local-only behavior so Aquifer stays on the hot path.

Configuration:

| Env var | Default | Description |
|---|---|---|
| `AQUIFER_REMOTE_IDEMPOTENCY_ENABLED` | `false` | Enables pre-dispatch Valkey duplicate lookup |
| `AQUIFER_VALKEY_URL` | _(none)_ | `redis://` or `valkey://` URL |
| `AQUIFER_REMOTE_IDEMPOTENCY_TIMEOUT_MS` | `25` | Lookup/write timeout budget |
| `AQUIFER_REMOTE_IDEMPOTENCY_PREFIX` | `aqueduct:idempotency:` | Key prefix |
| `AQUIFER_REMOTE_IDEMPOTENCY_TTL_SECONDS` | `7200` | TTL for entries written by Valkey drain sink |
| `AQUIFER_REMOTE_RESULT_ENABLED` | `false` | Also writes a bounded terminal result snapshot to Valkey |
| `AQUIFER_REMOTE_RESULT_PREFIX` | `aqueduct:result:` | Key prefix for result snapshots |
| `AQUIFER_REMOTE_RESULT_MAX_BYTES` | `65536` | Maximum response-body bytes stored in a result snapshot; `0` stores metadata only |

Key:

```txt
{AQUIFER_REMOTE_IDEMPOTENCY_PREFIX}{sha256(user_id + ":" + idempotent_key)}
```

Value:

```json
{
  "job_id": "a3f9...",
  "status": "completed",
  "recorded_at": 1798053731000,
  "source": "aquifer",
  "result_key": "aqueduct:result:..."
}
```

`AQUIFER_DRAIN_SINK=valkey` uses the same prefix/value contract for completed and failed job records.

When `AQUIFER_REMOTE_RESULT_ENABLED=true`, Aquifer also writes the terminal response snapshot to:

```txt
{AQUIFER_REMOTE_RESULT_PREFIX}{sha256(user_id + ":" + idempotent_key)}
```

Value:

```json
{
  "job_id": "a3f9...",
  "status": "completed",
  "response_status": 200,
  "content_type": "application/json",
  "body": "{\"ok\":true}",
  "body_truncated": false,
  "recorded_at": 1798053731000,
  "source": "aquifer"
}
```

The result body is capped before writing to Valkey. This is meant for replaying or inspecting bounded API responses, not for storing large artifacts or unbounded streams. Use object storage or your own durable result store for large outputs.

## GET /results

Retrieves a bounded remote result snapshot by the same logical idempotency key used to create the job:

```bash
curl "http://localhost:8080/results?user_id=user-123&idempotent_key=invoice-42-notify"
```

**200**

```json
{
  "job_id": "a3f9...",
  "status": "completed",
  "response_status": 200,
  "content_type": "application/json",
  "body": "{\"ok\":true}",
  "body_truncated": false,
  "recorded_at": 1798053731000,
  "source": "aquifer"
}
```

Returns **404** if remote result recording is disabled, the result has expired, or the job has not reached a terminal state yet.

## Autoscaling

| Header                    | Value                                              |
|---------------------------|----------------------------------------------------|
| `X-Aqueduct-Total-Jobs`   | Total jobs on this machine right now               |
| `X-Aqueduct-Queue-Depth`  | Jobs waiting to be dispatched                      |
| `X-Aqueduct-Flow-Rate`    | Current dispatch rate (RPS) for this queue         |

Traditional load balancers and autoscalers rely on failure to start scaling — capacity only responds once something's already struggling. Aquifer treats resources as fluid, not fixed: it paces down as machines show strain and back up as capacity comes online, using the same signals it already reports on every response.
