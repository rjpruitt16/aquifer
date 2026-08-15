# Aquifer — MCP Traffic Framework

**Increase your rate limit without DDoSing your backend.**

Aquifer is a self-hosted agent-native load balancer and traffic coordination layer for agent workloads. It absorbs bursts into a durable queue, dispatches at a controlled rate, and spreads traffic across a pool of registered backend instances. Upstreams can dynamically slow Aquifer down with `X-Aqueduct-*` response headers, so an overloaded service can shed pressure before it starts returning 429s.

Exposed through pluggable adapters — an MCP server for agent tool-calling, or a plain HTTP API — with cryptographic agent identity via the L8 protocol for trustless webhook delivery.

Built by [Rahmi Pruitt](https://rahmipruitt.me) — open to AI infra consulting, founding engineer, and contract work.

---

## The problem

Distributed agents call tools and APIs in bursts. Your backend gets overwhelmed on inbound. Your app gets 429s on outbound. One slow dependency takes everything else down with it.

Aquifer gives those agents a coordination layer. It absorbs the burst, queues requests durably (SQLite by default, or Pebble — see below), and releases them at the rate you configure. The destination service can ask for a slower pace, and Aquifer honors whichever limit is lower.

**Benchmarked:** 10x traffic spikes absorbed with zero failures, 30/30 jobs surviving a `kill -9` mid-drain, and clean `429` admission shedding under sustained overload. See [benchmark.md](benchmark.md) for throughput ceilings, crash recovery, memory behavior, and capacity by machine size.

---

## Use cases

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

In all three, the upstream can lower the dispatch pace via response headers — see [Dynamic Pacing](#dynamic-pacing) for how the ceiling, backoff, and recovery actually work.

Long-term protocol goal: if more services emit `X-Aqueduct-*`, agents can respond to capacity signals instead of independently guessing retry and concurrency behavior. Aquifer works today without ecosystem adoption; broader protocol adoption is the longer-term goal.

---

## How it works

1. Client submits a job through an adapter (MCP tool or HTTP endpoint) and moves on
2. Aquifer persists it to SQLite — survives crashes, re-dispatches on restart
3. A per-upstream worker dispatches at your configured RPS with jitter
4. On completion Aquifer POSTs your webhook with the response body and status
5. The upstream can adjust the rate live via `X-Aqueduct-*` response headers

**Delivery semantics:** Aquifer provides at-least-once dispatch and webhook delivery, not exactly-once execution. If Aquifer crashes after a dispatch succeeds but before it records that completion, the recovered job dispatches to the upstream again on restart — so it's not just the webhook that can repeat, the upstream call itself can. Make both your upstream endpoint and your webhook handler idempotent on `job_id` (or `idempotent_key`) anywhere duplicate execution isn't safe, the same contract Stripe and GitHub webhooks already ask of you.

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
| `DB_PATH`     | `aquifer.db` | Storage path — a SQLite file, or a directory if `AQUIFER_STORE_BACKEND=pebble` |
| `CONFIG_PATH` | _(none)_     | Path to rate limit config YAML |
| `AQUIFER_STORE_BACKEND` | `sqlite` | Storage engine: `sqlite` or `pebble` (opt-in, pure-Go LSM store — see [benchmark.md](benchmark.md) for why you might want it) |
| `AQUIFER_PEBBLE_WAL_SYNC_INTERVAL_MS` | `5` | Pebble only — batches concurrent durable writes into fewer real fsyncs under load (Pebble's own group-commit); each caller still blocks until its own write is actually durable |
| `AQUIFER_MEMORY_LIMIT_MB` | _(none, disabled)_ | Reject new jobs with `429` once process memory exceeds this many MB |
| `AQUIFER_MAX_BODY_BYTES` | `1048576` (1MB) | Reject oversized request bodies with `413` |
| `AQUIFER_DB_MAX_BYTES` | `838860800` (800MB) | Reject new jobs with `429` once the SQLite file exceeds this size |
| `AQUIFER_RETRY_AFTER_SECONDS` | `5` | Base `Retry-After` value sent on `429` admission rejections |

Body-size and DB-size admission are on by default; memory admission stays off until you set a limit, since a safe default depends on your own deployment, not Aquifer's disk usage. Retry-After backs off exponentially under sustained rejection (5s → 10s → 20s → 40s → capped at 60s, resets on the next allowed request). See [CONFIGURATION.md](CONFIGURATION.md) for the full rationale and [benchmark.md](benchmark.md) for the numbers behind these defaults.

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

MCP resource: `aquifer://jobs/{job_id}` reads current job status and metadata as JSON. The HTTP adapter remains the default for the binary, so existing deployments do not change.

### Writing an adapter

Adapter authors import Aquifer as a Go package, implement `FrameworkAdapter`, and pass the shared core into their framework — see [ADAPTERS.md](ADAPTERS.md) for the interface, a complete example, and how to reuse Aquifer's runtime wiring in a custom binary. `examples/custom_adapter` has a compile-tested reference implementation.

### Writing a storage backend

Persistence is also pluggable. Every core component (`Registry`, `AccountQueue`, `URLWorker`, `Aquifer` itself) is coded against the `JobStore` interface, not the concrete SQLite/Pebble types:

```go
type JobStore interface {
    Path() string
    Close() error
    CheckOrInsert(job *Job) (string, bool)
    SetQueueKey(jobID, queueKey string)
    DeleteJob(jobID string)
    MarkInFlight(jobID string)
    RecoverInFlight(queueKey string) []*Job
    UpdateStatus(jobID string, status Status)
    Counts() StoreCounts
    GetJob(jobID string) *Job
    GetQueuedJobs() []*Job
}
```

Implement it against your own backend (Postgres, rqlite, or anything else that can give you atomic check-and-set) and pass it via `RuntimeOptions.Store` — no need to bypass `NewRuntime`/`RunAdapter` or hand-wire the lower-level constructors. Two things a custom backend should be aware of: `CheckOrInsert` needs the same atomicity guarantee SQLite's `INSERT OR IGNORE` and Pebble's own store give it today (a non-atomic check-then-write reintroduces the exact idempotency race this project has already found and fixed twice — once in each language); and `AQUIFER_DB_MAX_BYTES` admission control does a local `os.Stat` on `DB_PATH`, which is meaningless for a networked backend — set it to `0` to disable that check if your store isn't a local file or directory.

Aquifer doesn't ship a Postgres or rqlite backend itself — this is documented as an extension point for anyone who wants multi-instance durability without local-disk-per-instance, not a promise one exists yet.

### Metrics adapter

Aquifer emits lifecycle events through a pluggable metrics adapter — implement `MetricsAdapter` and pass it into `NewRegistry`:

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

**Do not expose Aquifer directly to untrusted callers.** `url` is dispatched as a real HTTP request — if an arbitrary or untrusted party can set it, Aquifer becomes an open relay/SSRF vector: it can be pointed at your internal network, cloud metadata endpoints (`169.254.169.254`), or anything else the machine Aquifer runs on can reach, using Aquifer's own network position and identity. The intended caller is **your own trusted backend or gateway code** dispatching to a destination it already knows about — not an agent, end user, or any other party choosing the destination itself. Run Aquifer on a private network, not bound to a public address, and put your own authorization and destination allow-listing in front if agents need to reach it indirectly.

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

Server-Sent Events stream for live job updates: `queued` → `dispatching` → `completed` (`{"job_id","response_status","body"}`) or `failed` (`{"job_id","reason"}`), plus a `position` event every 2s while queued. Connecting late is safe — you'll receive synthetic catchup events for states you missed. SSE is a convenience, not the source of truth: the webhook fires regardless of whether the stream was ever open.

```bash
curl -N http://localhost:8080/jobs/<id>/stream
```

### GET /health

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

### Webhooks

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

Webhook delivery retries 4 times: 1 s · 2 s · 4 s · 8 s. Delivery is at-least-once — see [Delivery semantics](#how-it-works) above.

---

## L8 Protocol — trustless webhook delivery

Traditional webhook security shares an HMAC secret between sender and receiver, stored in a database on both sides — something that can be stolen, logged accidentally, or forgotten during rotation, letting anyone forge deliveries forever once it leaks. Aquifer implements **L8 v0.1**, a lightweight challenge-response protocol that replaces the shared secret with public key cryptography — there's no secret to steal from a database.

**How it works:**

1. The receiver publishes a public key at `GET /.well-known/l8`
2. Before the first delivery, Aquifer challenges the receiver to prove ownership of the corresponding private key — a one-time handshake
3. Trust is cached to disk as `l8-trust/{domain}.json` — the handshake never runs again for that domain
4. Every delivery carries `X-L8-Signature` headers, verified locally with a single Ed25519 call — no database lookup, no round-trip to any authority, microseconds

Trust stays deliberately pairwise, not transitive, by design. For better security and less latency than a shared-secret scheme, see the [L8 spec](https://rjpruitt16.github.io/l8-protocol/) for the full protocol rationale.

Set `L8_PRIVATE_KEY` (base64 Ed25519 private key) for a stable identity across restarts, or let Aquifer auto-generate one on first start. Delete `l8-trust/{domain}.json` to revoke trust with a domain — the handshake re-runs on next delivery.

**Aquifer exposes:**

| Endpoint | Purpose |
|---|---|
| `GET /.well-known/l8` | Aquifer's public key and capabilities — receivers discover Aquifer here |
| `POST /l8/challenge` | Handles incoming challenges from receivers verifying Aquifer's identity |
| `GET /l8-spec` | The full L8 protocol spec, served locally for an agent/script with only network access to this instance |

Current protocol version `0.1`, advertised in `/.well-known/l8` and `GET /health` — the same canonical spec ezthrottle-local follows. A complete reference receiver implementation and end-to-end tests are in `tests/l8_receiver.py` and `tests/test_l8.py`.

---

## Dynamic Pacing

**Terminology**: Aquifer is this implementation; Aqueduct is the implementation-agnostic header protocol (`X-Aqueduct-*`) it speaks, so other services could speak it too.

The upstream controls pace at runtime via response headers. `X-Aqueduct-*` is the protocol namespace; `X-Aquifer-*` remains supported as a backward-compatible product alias.

| Header (preferred)          | Alias                        | Effect                                  |
|------------------------------|-------------------------------|------------------------------------------|
| `X-Aqueduct-Rps`            | `X-Aquifer-Rps`               | Reduce dispatch rate to this value      |
| `X-Aqueduct-Max-Concurrent` | `X-Aquifer-Max-Concurrent`    | Reduce max in-flight requests           |
| `X-Aqueduct-Account-Queue`  | `X-Aquifer-Account-Queue`     | `enabled` — isolate each tenant's queue |

Aquifer reads both namespaces, preferring `X-Aqueduct-*` when both are present.

With `X-Aqueduct-Account-Queue: enabled`, each `(user_id, api_key)` pair gets its own independently paced queue, so one tenant's burst can't slow down another. Each queue's pace still stays inside the upstream's actual budget — a background check throttles the *sum* of every active tenant queue proportionally if too many are active at once, so isolation never means an unbounded copy of the full rate per tenant.

A backend can lower RPS at any time via these headers when it's under pressure; Aquifer honors the lower pace immediately and recovers gradually toward the configured ceiling once pressure clears.

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

## Agent-native load balancing

Instead of dispatching to a fixed `url`, a job can target a named **pool** — a group of registered service instances Aquifer picks from at dispatch time. Useful when you have several interchangeable backends (or, e.g., a separate group of writers and a separate group of readers) instead of one fixed endpoint.

**Registering a member:**

```bash
curl -X POST https://your-aquifer/pools/writers/members \
  -d '{"member_id": "writer-1", "address": "http://10.0.1.5:8080", "capacity_rps": 20, "heartbeat_interval_seconds": 30}'
```

The same call is both initial registration and heartbeat — call it again periodically (at roughly your declared `heartbeat_interval_seconds`) to stay in the pool. Missing several consecutive expected heartbeats evicts a member. A member can register under more than one pool id.

**Dispatching to a pool:**

```json
{
  "user_id": "user-123",
  "idempotent_key": "job-1",
  "pool_id": "writers",
  "method": "POST",
  "webhook_url": "https://yourapp.com/webhooks/aquifer"
}
```

`pool_id` and `url` are mutually exclusive — a job sets exactly one.

**How a member gets picked:** proportional to `capacity_rps × reputation`, not equal-split round robin — a member declaring 100 RPS gets roughly 4x the dispatches of one declaring 25. The pool's aggregate ceiling is the live sum of every member's current effective rate, so it grows and shrinks automatically as members register, degrade, or drop out — no need to reconfigure Aquifer as your fleet autoscales.

**Reputation**: a dispatch failure halves a member's effective share; a success nudges it back up. A member isn't evicted on one bad response — only once its reputation has stayed at the floor continuously, with no interrupting success, for a sustained window. This avoids flapping a member in and out of the pool over a single transient error.

**Set `capacity_rps` conservatively, not at your true theoretical max.** Aquifer only learns a member died via a failed dispatch or a missed heartbeat, both of which lag the actual failure — leaving headroom in what you declare gives real slack for that detection delay. Reputation decay is a second line of defense on top of this: a member that's silently struggling gets throttled down by observed failures even if its last-declared capacity was optimistic.

Pool state isn't shared across Aquifer instances — see [Deployment model](#deployment-model) for how that constrains a given `pool_id` to one instance.

`GET /health` reports every pool's current members, their declared capacity, and current reputation.

---

## Reliability

- **Durable queue** — jobs persist to the configured storage backend on every write
- **Crash recovery** — queued jobs re-dispatched automatically on restart
- **In-flight tracking** — jobs marked `in_flight` before dispatch; recovered immediately on panic without waiting for full restart
- **Stale job safety net** — in-flight jobs older than 5 min automatically reset to `queued`
- **Per-job panic isolation** — a panic in one job marks it failed and delivers the webhook; the worker keeps running

**Job TTLs:**

| Status      | TTL    |
|-------------|--------|
| `queued`    | 24 h   |
| `completed` | 30 min |
| `failed`    | 2 h    |

---

## Deployment model

Aquifer runs three ways: as a **sidecar** alongside your app, as a **standalone service** multiple services point to, or **embedded directly as a Go library** in your own process (see [Framework adapters](#framework-adapters)). Each instance persists to its own SQLite volume — no external database or coordination service to run.

Scale by partitioning: run one instance per upstream domain or tenant, each owning a distinct key space, and total throughput scales with instance count. Multiple instances against the *same* upstream without partitioning multiplies your request rate against it instead — the one setup to avoid. The same applies to pools: a given `pool_id` should belong to exactly one instance, since pool state isn't shared across instances.

People run Aquifer in front of things like: internal coding platforms (GitLab, Forgejo), CI runners, database read replicas, and MCP servers — anywhere a burst of agent or service traffic needs to hit something that has its own capacity limit.

See the [security warning](#post-jobs) under `POST /jobs` — the same untrusted-caller risk applies regardless of deployment shape.

---

## Choosing a machine size

Earlier benchmarks hit an artificial ~200 req/s ceiling caused by a serialized SQLite connection; that bottleneck is fixed. See [benchmark.md](benchmark.md#7-capacity-and-drain-time--and-a-real-bug-this-test-found) for current throughput, capacity by machine size, and the benchmark methodology.

---

## License

MIT
