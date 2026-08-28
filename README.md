# Aquifer — Load balancer for agentic workloads

**Increase your rate limit without DDoSing your backend.**

Distributed agents call tools and APIs in bursts. Your backend gets overwhelmed on inbound. Your app gets 429s on outbound. One slow dependency takes everything else down with it, and the retries agents fire off while they wait only make it worse — [wasted utilization and higher cost](https://rahmipruitt.me/content/gpu-retry-tax/) on one end, [outages reactive autoscaling alone can't prevent](https://rahmipruitt.me/content/github-outage-reactive-scaling/) on the other.

Aquifer gives those agents a coordination layer: a self-hosted load balancer that absorbs the burst, queues requests durably (SQLite by default, or Pebble — see below), and releases them at a rate you configure — or a slower one, if the destination service asks for it.

Exposed through pluggable adapters — an MCP server for agent tool-calling, a plain HTTP API, or an A2A (Agent2Agent protocol) agent — with cryptographic agent identity via the L8 protocol for trustless webhook delivery.

**Benchmarked:** 10x traffic spikes absorbed with zero failures, 30/30 jobs surviving a `kill -9` mid-drain, and clean `429` admission shedding under sustained overload — including a real GPU under load, where the ORCA fallback signal cut peak backend queue depth from 449 to 8 waiting requests. See [benchmark.md](benchmark.md) for throughput ceilings, crash recovery, memory behavior, capacity by machine size, and the [GPU/vLLM run](benchmark.md#9-gpu-inference-and-the-retry-tax-runpodvllm).

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

**Outbound — a durable checkpoint in front of a rate-limited resource**
```
your app  →  POST /jobs to Aquifer  →  database / CI runner / OpenAI / Stripe / any rate-limited API
```
Calling something with its own capacity limit — a database read replica, a CI runner, a third-party API? Aquifer queues the calls durably and dispatches them at your configured rate, so a burst from your own side never becomes the thing that takes the downstream down. Works especially well closed-loop: if the downstream already speaks `X-Aqueduct-*` headers, it can tell Aquifer to back off in real time instead of you guessing a static rate.

**Edge load balancer → gateway — pace and route at the edge**
```
your users  →  POST /proxy to Aquifer  →  your resources (paced, routed, at the speed you can handle)
```
Point Aquifer at your resources like a normal reverse proxy, close to the caller. It tries the request directly first — a healthy resource sees no queue at all — and only falls back to durable queuing when something's actually overloaded, on the same connection, staying in queue mode until that domain's backlog is genuinely drained (not just until a cooldown timer expires) — see [`POST /proxy`](API.md#post-proxy) for the details, including the header an upstream can use to request queuing proactively. For low-latency cross-region failover on top of that: Fly.io's own [`fly-replay`](https://fly.io/docs/networking/dynamic-request-routing/) is a response header *your* app returns to tell Fly's edge "redeliver this request in a different region" — your own logic in front of Aquifer can watch for the same overload signal the circuit breaker already uses (429, 5xx, an ORCA threshold) and respond with `fly-replay` instead of just falling back locally, rerouting at Fly's edge rather than adding a round trip through your own infrastructure. Not something Aquifer implements itself — the same overload classification just composes naturally with it.

In all four, the upstream can lower the dispatch pace via response headers — see [Dynamic Pacing](#dynamic-pacing) for how the ceiling, backoff, and recovery actually work.

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

Cross-platform release binaries (linux/darwin, amd64/arm64) are attached to every [GitHub Release](https://github.com/rjpruitt16/aquifer/releases) — no Go toolchain required if you'd rather grab one directly.

---

## Configuration

Rate limits are set per upstream hostname via a YAML file (`CONFIG_PATH`) and admission/runtime behavior via env vars.

<details>
<summary>Full config reference — YAML shape, env var table, admission defaults</summary>

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
| `AQUIFER_ADAPTER` | `http` for binary, `mcp-stdio` in Docker image | Runtime adapter: `http`, `mcp-stdio`, or `a2a` |
| `AQUIFER_A2A_PUBLIC_URL` | `http://localhost:$PORT` | A2A adapter only — externally-reachable base URL advertised in the Agent Card |
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

</details>

---

## Framework adapters

Aquifer has a framework-neutral core — idempotency, persistence, rate control, dispatch, SSE events, L8 signing, webhook delivery — with pluggable front doors:

| Adapter | Env | Purpose |
|---------|-----|---------|
| HTTP | `AQUIFER_ADAPTER=http` | REST/SSE API on `PORT` (the default) |
| MCP stdio | `AQUIFER_ADAPTER=mcp-stdio` | MCP server exposing Aquifer tools over stdio (the published Docker image's default) |
| A2A | `AQUIFER_ADAPTER=a2a` | Agent2Agent protocol (v1.0) agent over JSON-RPC/HTTPS |

**[ADAPTERS.md](ADAPTERS.md)** has the full reference for each built-in adapter (MCP tool list, A2A Agent Card details), plus how to write your own `FrameworkAdapter`, storage backend, or metrics adapter.

---

## Dynamic Pacing

**Terminology**: Aquifer is this implementation; Aqueduct is the implementation-agnostic header protocol (`X-Aqueduct-*`) it speaks, so other services could speak it too.

The upstream controls pace at runtime via response headers — `X-Aqueduct-Rps`, `X-Aqueduct-Max-Concurrent`, and per-tenant queue isolation — and Aquifer honors a lower pace immediately, recovering gradually once pressure clears. For backends that can't speak Aqueduct directly, Aquifer also reads the real open [ORCA](https://github.com/cncf/xds/blob/main/xds/data/orca/v3/orca_load_report.proto) standard as a fallback signal — vLLM and Triton/TensorRT-LLM both work today, verified against their actual source.

<details>
<summary>Full pacing reference — header table, account-queue isolation, ORCA details</summary>

| Header (preferred)          | Alias                        | Effect                                  |
|------------------------------|-------------------------------|------------------------------------------|
| `X-Aqueduct-Rps`            | `X-Aquifer-Rps`               | Reduce dispatch rate to this value      |
| `X-Aqueduct-Max-Concurrent` | `X-Aquifer-Max-Concurrent`    | Reduce max in-flight requests           |
| `X-Aqueduct-Account-Queue`  | `X-Aquifer-Account-Queue`     | `enabled` — isolate each tenant's queue |

Aquifer reads both namespaces, preferring `X-Aqueduct-*` when both are present.

With `X-Aqueduct-Account-Queue: enabled`, each `(user_id, api_key)` pair gets its own independently paced queue, so one tenant's burst can't slow down another. Each queue's pace still stays inside the upstream's actual budget — a background check throttles the *sum* of every active tenant queue proportionally if too many are active at once, so isolation never means an unbounded copy of the full rate per tenant.

A backend can lower RPS at any time via these headers when it's under pressure; Aquifer honors the lower pace immediately and recovers gradually toward the configured ceiling once pressure clears.

Use the pacing headers for intentional backpressure. A `5xx` response is treated as a failed dispatch attempt and, for pool members, lowers that member's reputation. If a service is alive but overloaded, prefer `429` and/or `X-Aqueduct-Rps` / `X-Aqueduct-Max-Concurrent` so Aquifer slows down without interpreting the member as broken.

**ORCA fallback for backends that can't speak Aqueduct directly.** Some backends already report load in a different, real open standard — ORCA (Open Request Cost Aggregation), the gRPC/Envoy ecosystem's convention for backends to report utilization. Aquifer sends `endpoint-load-metrics-format: text` on every dispatch (the request-side opt-in both verified backends require — there's no server startup flag for this), and a backend that understands it replies with an `endpoint-load-metrics` header carrying a KV-cache utilization fraction. If a response carries no `X-Aqueduct-Rps`/`X-Aquifer-Rps`, Aquifer reads this header as a fallback and paces down as utilization rises: full configured rate below 70%, 2 RPS at 70-90%, 0.5 RPS at 90-97%, 0.25 RPS above that — never dropping to zero, same pacing-down-gracefully philosophy as everywhere else. An explicit `X-Aqueduct-Rps` always wins if present; this only fires when the backend hasn't opted into speaking Aqueduct's own headers.

Two backends verified directly against their own source, not assumed: **vLLM** (`vllm/entrypoints/serve/utils/orca_metrics.py`, metric name `kv_cache_usage_perc`, case-insensitive opt-in) and **Triton/TensorRT-LLM** (`src/orca_http.cc`, metric name `kv_cache_utilization`, case-sensitive lowercase-only opt-in — Aquifer sends lowercase specifically so both work). Aquifer tries both known metric names, so whichever backend you're running, this works without any configuration.

</details>

---

## API

```json
POST /jobs
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

**Do not expose Aquifer directly to untrusted callers.** `url` is dispatched as a real HTTP request — if an arbitrary or untrusted party can set it, Aquifer becomes an open relay/SSRF vector, using Aquifer's own network position and identity to reach anything the machine can reach. The intended caller is **your own trusted backend or gateway code** dispatching to a destination it already knows about — not an agent, end user, or any other party choosing the destination itself.

**[API.md](API.md)** has the full reference: `GET /jobs/:id`, the SSE stream, `POST /proxy` (edge-gateway mode — see [Use cases](#use-cases)), `GET /health`, webhook payload shapes, and the autoscaling headers.

---

## Agent-native load balancing

Instead of dispatching to a fixed `url`, a job can target a named **pool** — a group of registered service instances Aquifer picks from at dispatch time, weighted by declared capacity and live reputation. Useful when you have several interchangeable backends instead of one fixed endpoint, and it grows or shrinks automatically as members register, degrade, or drop out — no need to reconfigure Aquifer as your fleet autoscales.

<details>
<summary>Full pool reference — registration, dispatch, reputation model, scenario harness</summary>

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

**Reputation**: a dispatch failure halves a member's effective share; a successful dispatch nudges it back up, and heartbeats recover it more slowly after a restart. A member isn't evicted on one bad response — only once its reputation has stayed at the floor continuously, with no interrupting success, for a sustained window. This avoids flapping a member in and out of the pool over a single transient error.

**Treat `5xx` carefully.** Aquifer interprets connection errors and `5xx` responses as reliability signals for the selected member. One `5xx` does not fail the job by itself — Aquifer records failure for that member and retries another member when possible. If every retry across the pool still ends in connection errors or `5xx`, the job is marked failed. That behavior is intentional for reliability, but it means application bugs that accidentally return `5xx` on a new code path can reduce that member's traffic share or eventually remove it from the pool.

For overload, prefer explicit backpressure over generic server errors:

| Situation | Recommended signal |
|-----------|--------------------|
| Instance is healthy but needs less traffic | `X-Aqueduct-Rps` or `X-Aqueduct-Max-Concurrent` |
| Request should be retried later due to pressure | `429` with `Retry-After` |
| Instance/code path is actually failing | `5xx` |

Roll new members into a pool gradually. Start new versions with a conservative `capacity_rps`, send a small share of traffic first, watch `/health` reputation and your own error metrics, then raise capacity as confidence grows. Blue/green or canary rollout matters more here than with a blind round-robin balancer because Aquifer uses runtime failures as routing input.

**Set `capacity_rps` conservatively, not at your true theoretical max.** Aquifer only learns a member died via a failed dispatch or a missed heartbeat, both of which lag the actual failure — leaving headroom in what you declare gives real slack for that detection delay. Reputation decay is a second line of defense on top of this: a member that's silently struggling gets throttled down by observed failures even if its last-declared capacity was optimistic.

**Watch the model locally:** Aquifer includes a local scenario harness that starts one fake backend server with multiple logical workers, registers them as pool members, and prints per-second traffic, failures, dynamic header values, and reputation.

```bash
go run ./cmd/aquifer-scenario --scenario mixed --workers 10 --jobs 500 --duration 30s --rps 50
```

Scenarios: `steady`, `weighted`, `flapping`, `backpressure`, `recovering`, `mixed`, and `harsh`. The `harsh` scenario penalizes sustained overload by adding latency, then `5xx`, then simulated crash windows. Add `--mode regular` to compare against a simple round-robin load balancer model that retries on `5xx` but ignores Aquifer reputation and dynamic pacing headers.

Pool state isn't shared across Aquifer instances — see [Deployment model](#deployment-model) for how that constrains a given `pool_id` to one instance.

`GET /health` reports every pool's current members, their declared capacity, and current reputation.

</details>

---

## L8 Protocol — trustless webhook delivery

Traditional webhook security shares an HMAC secret between sender and receiver, stored in a database on both sides — something that can be stolen, logged accidentally, or forgotten during rotation, letting anyone forge deliveries forever once it leaks. Aquifer implements **L8 v0.1**, a lightweight challenge-response protocol that replaces the shared secret with public key cryptography: the receiver publishes a public key, a one-time handshake proves both sides own their private keys, and every delivery afterward carries a signature verified locally in microseconds — no database lookup, no round-trip to any authority.

The full protocol rationale, wire format, and a reference receiver implementation live at the **[L8 spec](https://rjpruitt16.github.io/l8-protocol/)** — also served locally at `GET /l8-spec` for an agent/script with only network access to this instance. Set `L8_PRIVATE_KEY` for a stable identity across restarts, or let Aquifer auto-generate one on first start.

---

## Reliability

Durable queue, automatic crash recovery, panic isolation per job — see [benchmark.md](benchmark.md) for the numbers behind these claims.

<details>
<summary>Full reliability reference — mechanisms, job TTLs</summary>

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

</details>

---

## Partitioning strategies

Running one instance for everything works fine until you have multiple tenants or multiple upstreams sharing it — then one tenant's burst, or one upstream's own rate limit, ends up affecting everyone else on that same instance. Two ways to split traffic apart so that doesn't happen, not mutually exclusive:

**Static partitioning** — decided once, at deploy time: dedicate one instance to a single protected resource — a CI runner, a database, a GPU, or a rate-limited external API you want to be nice to — so that resource only ever sees traffic paced the way you configured, up to whatever it can actually bear. Multiple tenants can safely share that same instance: turn on [account-queue isolation](#dynamic-pacing) and each tenant gets their own independently-paced queue, so one tenant's burst doesn't starve another's, and the resource itself never sees more aggregate load than it's rated for. The mistake to avoid: pointing multiple *instances* at the *same* resource instead of routing everyone through this one pacing checkpoint — that just multiplies your total request rate against it. Same rule for pools: a given `pool_id` should belong to exactly one instance, since pool state isn't shared across instances.

**Dynamic partitioning (drain mode)** — off by default, for a more specific shape: instead of deciding every assignment up front, an instance gets handed to one tenant at a time, absorbs and drains whatever burst that tenant sends, then frees itself up to be handed to a *different* tenant next — useful when you want dedicated capacity per user without hand-assigning it at deploy time. A normal single-instance or statically-partitioned deployment is completely unaffected unless you turn this on. When idle for `AQUIFER_DRAIN_TIMER_SECONDS`, the instance flushes its deduped idempotency ledger to a webhook and clears local state, moving through an `active` → `draining` → `unassigned` state machine visible via `GET /health`. See **[DRAIN_MODE.md](DRAIN_MODE.md)** for the full state machine, env vars, and webhook payload shape.

The two combine: a fleet can partition statically by upstream domain, while individual instances within a partition cycle through tenants dynamically via drain mode.

---

## Deployment model

Aquifer runs four ways: as a **sidecar** alongside your app, as a **standalone service** multiple services point to, **embedded directly as a Go library** in your own process (see [Framework adapters](#framework-adapters)), or as an **extension behind a Gateway API proxy** like Envoy Gateway in Kubernetes — the proxy owns routing and TLS, Aquifer owns the queue behind it (see [examples/kubernetes](examples/kubernetes)). Each instance persists to its own SQLite volume — no external database or coordination service to run.

People run Aquifer in front of things like: internal coding platforms (GitLab, Forgejo), CI runners, database read replicas, and MCP servers — anywhere a burst of agent or service traffic needs to hit something that has its own capacity limit.

<details>
<summary>Full deployment reference — partitioning, scaling, security note</summary>

See [Partitioning strategies](#partitioning-strategies) above for how to assign tenants to instances, statically or dynamically.

See the [security warning](API.md#post-jobs) under `POST /jobs` — the same untrusted-caller risk applies regardless of deployment shape.

</details>

**Choosing a machine size:** see [benchmark.md](benchmark.md#7-capacity-and-drain-time) for current throughput, capacity by machine size, and the benchmark methodology.

---

## Writing

- [Eliminate GPU Waste by Cutting the Retry Tax](https://rahmipruitt.me/content/gpu-retry-tax/) — the thesis behind [drain mode](#partitioning-strategies) and the ORCA fallback pacing [GPU benchmark](benchmark.md#9-gpu-inference-and-the-retry-tax-runpodvllm) above.
- [GitHub Outages Show the Limits of Reactive Scaling](https://rahmipruitt.me/content/github-outage-reactive-scaling/) — why reactive scaling and retry storms don't mix, the problem Aquifer absorbs instead.

## License

MIT

Built by [Rahmi Pruitt](https://rahmipruitt.me) — open to AI infra consulting, founding engineer, and contract work.
