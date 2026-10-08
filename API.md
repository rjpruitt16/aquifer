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
  "webhook_url":    "https://yourapp.com/webhooks/aquifer",
  "execute_before": 1798761600000
}
```

`execute_before` is optional Unix time in milliseconds. Aquifer checks it immediately before dispatch and before every retry. A job that has not started its next attempt by that time is never sent upstream; it becomes `failed` with reason `execution_deadline_exceeded`, remains available through the normal status/result retention window, and emits the normal failed SSE/webhook event.

**Do not expose Aquifer directly to untrusted callers.** `url` is dispatched as a real HTTP request — if an arbitrary or untrusted party can set it, Aquifer becomes an open relay/SSRF vector: it can be pointed at your internal network, cloud metadata endpoints (`169.254.169.254`), or anything else the machine Aquifer runs on can reach, using Aquifer's own network position and identity. The intended caller is **your own trusted backend or gateway code** dispatching to a destination it already knows about — not an agent, end user, or any other party choosing the destination itself. Run Aquifer on a private network, not bound to a public address, and put your own authorization and destination allow-listing in front if agents need to reach it indirectly.

As a second, narrower layer on top of that — `AQUIFER_ALLOWED_URL_DOMAINS` (comma-separated hostnames, e.g. `api.openai.com,internal.yourapp.com`) restricts which domains `url` is allowed to target at all; a subdomain of an allowed entry is permitted (`api.example.com` matches an allowlisted `example.com`). Unset by default — unrestricted, unchanged behavior — this doesn't replace the network/authorization guidance above, it's what Aquifer itself can enforce regardless of who's calling it. Applies to `url`-routed jobs only; pool-routed jobs (`pool_id`) have no caller-supplied destination to check.

Idempotent — duplicate `idempotent_key` per `user_id` returns the existing job.

**201** new job queued · **200 + `"duplicate": true`** already exists

**Shared idempotency scope.** Set `"idempotency_scope": "shared"` to dedup on `idempotent_key` alone, across every `user_id`. Many agents asking for the same resource then coalesce onto one upstream call: the first request creates the job, later ones get `200 + "duplicate": true` with the same `job_id`. On `/proxy` they are streamed that job's result; with remote results enabled, `GET /results?idempotency_scope=shared&idempotent_key=...` serves the cached response to the whole pool. Followers do not get their own webhook. Requires `AQUIFER_SHARED_IDEMPOTENCY_ENABLED=true` (default `false`); otherwise `"shared"` is rejected with `400`. Anyone who knows a shared key can read its result, so use keys that name a public resource (`weather:sf:2026-10-05`), never one that carries a user's private data. In a cluster, shared requests are routed by the shared key instead of `user_id`, so every caller for that key reaches the same owner node and coalesces there; per-user requests keep routing by `user_id`. Independent instances that share only Valkey (not one cluster) can each run the job once before the first result lands; the last write to Valkey wins.

**422** the upstream's L8 `request_schemas` rejects the body (see [Request schemas](#request-schemas-l8-02)).

### Fair queue admission

Each upstream has a soft shared backlog budget `B`, defaulting to `AQUIFER_MAX_PENDING_PER_UPSTREAM=10000`. The upstream may update it at runtime with `X-Aqueduct-Max-Backlog` (or `X-Aquifer-Max-Backlog`); `0` disables this count-based limit. Dynamic values are learned from direct proxy responses and queued dispatch responses.

With AccountQueue mode enabled, Aquifer gives idle capacity to whoever can use it while protecting smaller queues once the upstream becomes congested. For a prospective request, let `q` be its queue's projected backlog, `Q` the projected upstream backlog, and `N` the projected number of active account queues:

```text
F        = B / N
pressure = clamp((Q - 0.70B) / 0.30B, 0, 1)
excess   = clamp((q - F) / (B - F), 0, 1)
P(429)   = pressure * excess
```

A lone queue is never fairness-rejected and may use the whole budget. A queue at or below `F` is not fairness-rejected. Above 70% total pressure, disproportionately large queues receive progressively more `429` responses; when competitors drain, their capacity becomes borrowable again. If admitting a request would put `Q` above `B`, it is rejected deterministically. Without AccountQueue mode, traffic remains one shared queue (`N=1`), so only the shared backlog ceiling applies. Duplicates, recovered work, and internal webhook deliveries bypass this admission decision; accepted jobs are never discarded by it.

Accepted and fairness-rejected responses report `X-Aqueduct-Active-Queues`, `X-Aqueduct-Upstream-Backlog`, `X-Aqueduct-Queue-Backlog`, and `X-Aqueduct-Admission-Pressure` (with `X-Aquifer-*` aliases). Outbound dispatches report active queues and upstream backlog to the backend alongside the existing load headers. These values cover the relevant upstream on the serving node. `GET /health` reports node-wide local totals under `queues`; it is deliberately not presented as a fleet-wide count.

### Inbound pacing (`X-Aqueduct-Rps`)

Off by default. With `AQUIFER_INBOUND_RPS_ENABLED=true`, Aquifer tells its own callers how fast to submit: every `/jobs` and `/proxy` response carries `X-Aqueduct-Rps` (alias `X-Aquifer-Rps`), the same header an upstream uses to pace Aquifer. An Aquifer calling another Aquifer, or any client that honors the header, slows down before the server has to start returning `429`s.

| Env var | Default | Description |
|---|---:|---|
| `AQUIFER_INBOUND_RPS_ENABLED` | `false` | Start the controller and send the header |
| `AQUIFER_INBOUND_BENCHMARK_RPS` | `400` | Jobs per second you believe this machine can take end to end; the first guess at the knee |
| `AQUIFER_INBOUND_START_PERCENT` | `40` | Where the advertised rate starts, as a percent of the benchmark |

**How it moves.** Once a second the controller compares what callers sent against two signals:

- **Insert latency.** The p95 time to durably store a job, against a healthy baseline (the lowest p95 seen, drifting up slowly). Above 1.5x the baseline plus 2ms means storage is saturated.
- **Backlog growth.** Accepted work waiting to be dispatched. If it grows for 3 ticks in a row, Aquifer is accepting faster than it can dispatch, even when inserts still look fast. Intake and dispatch share the same storage writes, so this is usually the first signal to fire.

When either signal trips, the rate is cut by 30%, and the rate just under where it failed becomes the learned knee. Within one congestion episode it cuts once, then holds for up to 10 ticks while admitted work drains, so one slow patch doesn't drive the rate to the floor. While healthy, it grows 1.5x per tick below the knee and 5% per tick above it, and only when callers actually use at least 80% of what was advertised (unused headroom is never inflated). If callers send more than 1.5x the advertised rate, it holds instead of cutting: that is a client ignoring the header, and admission control handles it.

The total is split evenly across active callers (any `user_id` seen in the last 3 seconds), so `X-Aqueduct-Rps` is a per-caller number.

**Tuning.**

1. **Measure the benchmark on the real machine.** Run your normal workload (or `make perf`, see [benchmark.md](benchmark.md#11-per-job-overhead-make-perf)) on the same instance type and disk, and set `AQUIFER_INBOUND_BENCHMARK_RPS` to the sustained jobs/s where the backlog stays flat. Count whole jobs, not inserts: a job costs about 4 synced storage writes (accept, dispatch result, webhook), about 2 when the caller streams the result. On a Fly performance-1x with a volume, one client held about 170 jobs/s end to end, well under a 400 inserts/s micro-benchmark.
2. **Pick the start percent by how bursty callers are.** The default 40% ramps to the benchmark in about 3 seconds. Lower it (15 to 20) when one instance serves many callers and a cold start shouldn't flood it; raise it toward 80 for a single trusted caller that should get full speed right away.
3. **Watch `/health`.** With pacing on, `GET /health` reports `inbound`:

   ```json
   "inbound": {
     "advertised_rps": 168.2, "per_caller_rps": 56.1, "active_callers": 3,
     "inserts_per_sec": 171, "p95_insert_ms": 2.4, "baseline_insert_ms": 1.1,
     "benchmark_rps": 400, "threshold_rps": 181.0, "server_backlog": 212,
     "cuts": 4, "raises": 19, "last_adjustment": "raise"
   }
   ```

   `threshold_rps` settling well below `benchmark_rps` means the benchmark is optimistic; lower it so the ramp stops overshooting. Repeated `cut: backlog growing` means dispatch, not intake, is the bottleneck: more upstream capacity or more Aquifer instances (partition by `user_id`) helps; a faster disk alone won't. `hold: callers over advertised rate` means a caller is ignoring the header.
4. **Pacing is per instance.** Each node advertises its own capacity. To scale intake, add nodes and spread callers across them; one node's knee doesn't move because another node is idle.

## Regional cluster routing

Cluster routing is optional HTTP-level partitioning for `POST /jobs` and `POST /proxy`. Callers may hit any Aquifer instance; the receiving node resolves an owner by `user_id`, forwards when necessary, and returns `X-Aquifer-Cluster-Owner` with the instance that handled the request. This changes where a user is processed, not how local account queues are grouped: `X-Aqueduct-Account-Queue` still controls whether a URL worker isolates `(user_id, api_key)` queues locally.

Two providers are available:

- `static` (default) builds a deterministic rendezvous ranking from `AQUIFER_CLUSTER_MEMBERS`. An unreachable peer is soft-pruned from that instance's local view and retried after the prune TTL. There is no shared assignment state.
- `valkey` makes Aquifer instances heartbeat directly into a regional Valkey and atomically claims user assignments there. No separate control plane is required. Existing assignments stay sticky while their owner is active. For a new user, Aquifer walks the rendezvous ranking and chooses the first node below its active-user capacity. If every active node is full, it chooses the least-loaded node by `active_users / capacity` instead of rejecting work.

`AQUIFER_CLUSTER_MAX_ACTIVE_USERS` is therefore a soft placement target, not an admission-control ceiling. An active user is a distinct `user_id` with outstanding local work or a recently used assignment. Once its work reaches zero and it remains idle for `AQUIFER_CLUSTER_ASSIGNMENT_IDLE_SECONDS`, the owner releases it so that capacity becomes available again.

Configuration:

| Env var | Default | Description |
|---|---|---|
| `AQUIFER_CLUSTER_ENABLED` | `false` | Enables HTTP cluster routing |
| `AQUIFER_CLUSTER_PROVIDER` | `static` | `static` or `valkey` |
| `AQUIFER_CLUSTER_SELF_ID` | _(required when enabled)_ | Stable id for this node |
| `AQUIFER_CLUSTER_SELF_ADDR` | _(required when enabled)_ | Base HTTP URL other nodes use to reach this node |
| `AQUIFER_CLUSTER_MEMBERS` | _(none)_ | Comma-separated `id=http://host:port` bootstrap/static members; self is added automatically |
| `AQUIFER_CLUSTER_PRUNE_TTL_SECONDS` | `30` | How long this node excludes an unreachable peer before trying it again |
| `AQUIFER_CLUSTER_VALKEY_URL` | `AQUIFER_VALKEY_URL` | Shared regional `redis://`, `rediss://`, `valkey://`, or `valkeys://` endpoint |
| `AQUIFER_CLUSTER_VALKEY_PREFIX` | `aqueduct:` | Generic membership and assignment key prefix |
| `AQUIFER_CLUSTER_MAX_ACTIVE_USERS` | `100` | Soft active-user placement capacity advertised by this instance |
| `AQUIFER_CLUSTER_HEARTBEAT_SECONDS` | `5` | Membership refresh and assignment-renew interval |
| `AQUIFER_CLUSTER_INSTANCE_TTL_SECONDS` | `15` | Time without a heartbeat before an instance is no longer assignable; forced to at least three heartbeat intervals |
| `AQUIFER_CLUSTER_ASSIGNMENT_IDLE_SECONDS` | `300` | Completed user's idle window before its assignment is released |
| `AQUIFER_CLUSTER_VALKEY_TIMEOUT_MS` | `50` | Per-operation coordination budget before local fallback |

Example:

```bash
AQUIFER_CLUSTER_ENABLED=true
AQUIFER_CLUSTER_PROVIDER=valkey
AQUIFER_CLUSTER_SELF_ID=aquifer-a
AQUIFER_CLUSTER_SELF_ADDR=http://aquifer-a:8080
AQUIFER_CLUSTER_VALKEY_URL=valkey://valkey.internal:6379
AQUIFER_CLUSTER_MAX_ACTIVE_USERS=500
```

The Valkey provider uses these generic keys:

```text
aqueduct:instances
aqueduct:instance:<instance_id>
aqueduct:instance:<instance_id>:assignments
aqueduct:assignment:<sha256(user_id)>
```

Assignment and capacity selection happen in one Valkey Lua transaction using Valkey's clock, so concurrent healthy nodes converge on the first successful claim immediately rather than waiting for gossip. Heartbeats refresh membership; a graceful drain advertises `draining` before shutdown, and a crashed node becomes ineligible when its instance TTL expires. This mode is intended for Aquifer and Valkey instances in one region. The current multi-key transaction expects one Valkey primary/endpoint, not a sharded Redis Cluster keyspace.

If Valkey exceeds the operation timeout or is unavailable, Aquifer keeps serving through its cached deterministic rendezvous view. That availability choice means two nodes with temporarily different views can both accept the same user. Healthy coordination converges again on the next successful assignment operation; dead-node membership normally converges within the instance TTL. Use the separate [Valkey remote-idempotency](#remote-idempotency) feature when duplicate execution across such a partition is unacceptable.

This is not a distributed database. Job idempotency, queue contents, and pacing state remain local. A reassignment can therefore move a `user_id` to a node that does not have the previous owner's local history.

Membership changes can also temporarily create duplicate account queues for the same upstream URL on different nodes: old work may still be draining on the previous owner while new work moves to the new owner. That is expected during rebalance or a coordination outage and should be short-lived under normal TTL/drain cleanup. This availability-first mode does not provide a strict single-owner option.

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

## GET /websocket

WebSocket proxying with an ordered Valkey transcript, cursor replay, paced upstream connection admission, and automatic upstream reconnect. Aquifer does **not** authenticate callers or choose their destination. Put it behind a trusted gateway that authenticates the request and injects the upstream URL.

The endpoint enables automatically when `AQUIFER_VALKEY_URL` is configured; there is no separate feature flag to turn on. `AQUIFER_WS_ENABLED=false` remains an operational kill switch. Without a configured Valkey URL, the WebSocket subsystem stays inactive so ordinary HTTP/MCP installations do not acquire an external dependency. Active WebSocket sessions fail closed when Valkey is unavailable because Aquifer cannot uphold record-before-forward and record-before-deliver without the stream.

### Handshake

```http
GET /websocket?session_id=session-123&after=0-0 HTTP/1.1
Connection: Upgrade
Upgrade: websocket
Sec-WebSocket-Protocol: aqueduct.v1
Authorization: Bearer gateway-authenticated-identity
X-Aqueduct-Upstream-URL: wss://backend.internal/socket
```

`session_id` identifies the durable transcript. `after` is the last Valkey stream ID the client has processed and defaults to `0-0`. Aquifer replays backend messages after that cursor before following live events. A cursor older than retained history is rejected with **409** instead of silently skipping data.

The trusted `X-Aqueduct-Upstream-URL` must be an absolute `ws://` or `wss://` URL and is subject to `AQUIFER_ALLOWED_URL_DOMAINS`. Aquifer forwards end-to-end gateway headers such as `Authorization`, strips hop-by-hop and internal `X-Aqueduct-*`/`X-Aquifer-*` headers, and injects `X-Aqueduct-Session-ID` upstream.

### Messages

Every application message is a JSON `aqueduct.v1` envelope. Raw frame proxying is intentionally not supported.

Client command:

```json
{"type":"command","message_id":"command-42","payload":{"action":"start"}}
```

The `message_id` must be stable across client retries. Aquifer appends the command to Valkey before forwarding it and confirms that durable write with:

```json
{"type":"command_recorded","message_id":"command-42","stream_id":"1798053731000-0"}
```

Backend acknowledgement and events:

```json
{"type":"ack","message_id":"ack-42","caused_by":"command-42"}
{"type":"event","message_id":"event-43","caused_by":"command-42","payload":{"state":"running"}}
{"type":"event","message_id":"event-44","caused_by":"command-42","payload":{"state":"complete"}}
```

`caused_by` supports one command producing zero, one, or many events; it is not a one-request/one-response contract. Aquifer appends every backend `ack` and `event` before delivery, then adds its Valkey `stream_id` and upstream `generation` to the client-facing envelope.

Aquifer also sends non-durable control messages:

```json
{"type":"status","state":"replaying"}
{"type":"status","state":"replay_complete"}
{"type":"status","state":"waiting","position":2}
{"type":"status","state":"connecting"}
{"type":"status","state":"connected","generation":7}
{"type":"status","state":"reconnecting","retry_after_ms":914,"reason":"..."}
{"type":"status","state":"server_draining","retry_after_ms":5000,"reason":"reconnect through the gateway"}
```

While a session waits, Aquifer emits another `waiting` status whenever its FIFO position changes (for example, `2` then `1`). These progress messages are live control state and are not appended to the durable transcript.

On upstream loss, Aquifer keeps the client connection open and reconnects with jittered exponential backoff. Commands are durably recorded, but v1 does **not** automatically replay a command after an ambiguous upstream failure: Aquifer cannot know whether the backend acted before the connection disappeared. Clients may resend a command with the same `message_id`; backend actions must therefore be idempotent by `message_id`. Delivery of backend events is at least once when a client reconnects from its last acknowledged stream cursor.

Each session transcript retains approximately the newest `AQUIFER_WS_STREAM_MAX_EVENTS` entries and expires `AQUIFER_WS_STREAM_TTL_SECONDS` after its last recorded entry. Every append refreshes that TTL. Expired history is treated as a replay gap when a client supplies a nonzero cursor, so deletion cannot silently skip events.

Two clients may temporarily attach to the same `session_id` during an application-managed handoff; both follow the same durable backend event stream, and the client decides when to close the old socket. On `SIGTERM`, Aquifer sends `server_draining`, leaves the old socket open for `AQUIFER_WS_DRAIN_GRACE_SECONDS`, then closes it with WebSocket code **1012 Service Restart**. The client should open its replacement through the gateway using its last processed stream cursor. New handshakes receive **503** while the node drains.

### Capacity

Connection ceilings are local to one Aquifer process. `AQUIFER_WS_MAX_CLIENT_CONNECTIONS=1000` means that instance accepts at most 1,000 clients; it is not a fleet-wide semaphore. Ten identical instances can therefore admit up to 10,000 clients when the gateway distributes them.

The upstream can lower this instance's upstream-connection ceiling or opening rate during a successful handshake:

```http
X-Aqueduct-WS-Max-Connections: 250
X-Aqueduct-WS-Connect-Rps: 20
```

It can update either value on an established socket with `{"type":"aqueduct.capacity","max_connections":250,"connect_rps":20}`. Dynamic signals can only lower operator-configured ceilings, never raise them. `Retry-After` on a rejected upstream handshake becomes the minimum reconnect delay.

Opening starts at `AQUIFER_WS_SLOW_START_RPS` for each Aquifer process. Every successful upstream handshake doubles the current ramp rate until `AQUIFER_WS_CONNECT_RPS` is reached; a failed handshake resets the ramp. An established socket disconnecting does not penalize unrelated connections by resetting the process-wide ramp. Backend capacity signals can still lower the effective rate at any point. Set `AQUIFER_WS_SLOW_START_RPS` equal to `AQUIFER_WS_CONNECT_RPS` to disable ramping.

| Env var | Default | Description |
|---|---:|---|
| `AQUIFER_WS_ENABLED` | automatic | WebSockets are on when `AQUIFER_VALKEY_URL` is set; `false` explicitly disables them |
| `AQUIFER_VALKEY_URL` | _(required)_ | Shared `redis://`, `rediss://`, `valkey://`, or `valkeys://` stream store |
| `AQUIFER_WS_STREAM_PREFIX` | `aqueduct:ws:` | Stream key prefix; session IDs are SHA-256 hashed |
| `AQUIFER_WS_STREAM_MAX_EVENTS` | `10000` | Approximate retained entries per session |
| `AQUIFER_WS_STREAM_TTL_SECONDS` | `86400` | Sliding expiration after the last recorded session entry |
| `AQUIFER_WS_READ_BATCH` | `100` | Maximum events fetched per stream read |
| `AQUIFER_WS_READ_BLOCK_MS` | `1000` | Live stream blocking-read interval |
| `AQUIFER_WS_MAX_MESSAGE_BYTES` | `1048576` | Maximum client or backend message size |
| `AQUIFER_WS_HANDSHAKE_TIMEOUT_SECONDS` | `10` | Valkey check and upstream handshake timeout |
| `AQUIFER_WS_RECONNECT_MAX_SECONDS` | `30` | Maximum reconnect backoff before jitter |
| `AQUIFER_WS_MAX_CLIENT_CONNECTIONS` | `1000` | Client socket ceiling for this Aquifer instance |
| `AQUIFER_WS_MAX_UPSTREAM_CONNECTIONS` | `1000` | Active backend socket ceiling for this instance |
| `AQUIFER_WS_MAX_WAITING_CONNECTIONS` | `1000` | Local queue ceiling for sessions waiting on an upstream slot |
| `AQUIFER_WS_CONNECT_RPS` | `20` | Maximum upstream connection openings per second on this instance |
| `AQUIFER_WS_SLOW_START_RPS` | `1` | Initial and post-failure connection-opening rate before successful handshakes ramp it up |
| `AQUIFER_WS_DRAIN_GRACE_SECONDS` | `5` | Handoff window between `server_draining` and close code 1012 on `SIGTERM`; `0` closes immediately |

Handshake errors are returned before upgrade: **400** for invalid protocol/session/upstream input, **409** for a replay gap, **429** for a local connection or waiting ceiling, and **503** when Valkey is unavailable or the node is draining. `GET /health` exposes this instance's client, waiting, active-upstream, configured, ramp, and effective limits under `websocket`.

Measure a deployment with a controlled upstream before raising the defaults:

```bash
go run ./cmd/aquifer-websocket-bench \
  -target ws://localhost:8080/websocket \
  -upstream ws://localhost:6060/socket \
  -connections 1000 -ramp 10s -hold 30s
```

This measures one Aquifer instance. Run it against each machine size and include the gateway, Valkey latency, file-descriptor limits, and the real backend in capacity planning.

## Cross-region redirect (Fly.io)

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
  "status":     "queued | completed | failed",
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
`AQUIFER_*` admission env vars are set. With inbound pacing on, an `inbound` object is also present (see [Inbound pacing](#inbound-pacing-x-aqueduct-rps)).

`GET /health` is a liveness endpoint. It remains **200** during graceful shutdown, with `status: "draining"`, so an orchestrator does not kill the process before accepted work has a chance to finish.

## GET /ready

Readiness for load balancers and fleet discovery. An active node returns **200** with `{"status":"ready"}`. A draining node returns **503**, `Retry-After`, and both `X-Aqueduct-Node-State: draining` and the compatibility alias `X-Aquifer-Node-State: draining`.

Use `/ready`, not `/health`, for routing decisions. Fly region discovery uses `/ready`; cluster forwarding also soft-prunes a ranked owner when it returns the draining signal and tries the next owner.

## Graceful shutdown

The built-in HTTP, MCP stdio, and A2A adapters handle `SIGINT`/`SIGTERM` through the same bounded lifecycle:

1. Mark the node draining, fail `/ready`, reject new jobs, proxy work, and WebSocket handshakes with **503**, and announce `state: "draining"` when external registration is enabled.
2. Keep the listener available for a short quiescence window so gateways and peers can observe the transition, then stop accepting connections.
3. Let already-accepted jobs and their completion webhooks finish. Notify active WebSockets, allow their handoff grace period, then close them with code 1012.
4. Flush the drain-event ledger once more. Events are acknowledged locally only after the configured sink confirms receipt, and periodic/final flushes are serialized to prevent duplicate concurrent batches.
5. Report `state: "offline"` to the external registry and exit. When the total deadline expires, Aquifer closes with durable queued work left for normal startup recovery.

| Env var | Default | Description |
|---|---:|---|
| `AQUIFER_SHUTDOWN_TIMEOUT_SECONDS` | `30` | Total graceful-drain deadline; must be positive |
| `AQUIFER_SHUTDOWN_QUIESCE_MS` | `500` | Time to expose draining readiness before stopping the listener; `0` disables the delay |
| `AQUIFER_WS_DRAIN_GRACE_SECONDS` | `5` | WebSocket replacement window, capped at the shutdown timeout |

The process termination grace configured by your orchestrator must exceed `AQUIFER_SHUTDOWN_TIMEOUT_SECONDS`; otherwise the orchestrator may send `SIGKILL` before Aquifer can finish or preserve the intended shutdown sequence.

## Request schemas (L8 0.2)

An upstream that implements L8 0.2 can publish a JSON Schema (draft 2020-12) per route in its `GET /.well-known/l8`:

```json
{
  "protocol_version": "0.2",
  "public_key": "...",
  "challenge_endpoint": "/l8/challenge",
  "capabilities": ["signed_payloads", "request_schemas"],
  "schema_hash": "sha256:9c1e...",
  "request_schemas": {
    "POST /v1/chat/completions": { "type": "object", "required": ["model", "messages"] }
  }
}
```

With `AQUIFER_L8_SCHEMA_VALIDATION=true`, Aquifer fetches this once per upstream domain, caches it for up to 10 minutes, and checks `url`-routed job bodies whose method and path match a route. A mismatch never reaches the upstream:

```json
{
  "error": "request body does not match the schema the upstream advertises for POST /v1/chat/completions",
  "schema_route": "POST /v1/chat/completions",
  "schema_hash": "sha256:9c1e...",
  "schema_errors": "..."
}
```

When the upstream changes its contract it returns a new `X-Aqueduct-Schema-Hash` on any response. Aquifer then drops the cached schemas and the L8 trust for that domain, so the next request refetches metadata and re-runs the handshake. Routes without a schema, upstreams without L8, and metadata that fails to fetch or compile are all let through unchecked. External `$ref`s are never fetched.

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
- L8 encryption: when the receiver advertises `encrypted_payloads` and an X25519 `encryption_public_key`, the body is encrypted to that key (`x25519-hkdf-sha256-aes256gcm`) and the signature covers the ciphertext. The receiver verifies, then decrypts. See the [L8 spec](https://rjpruitt16.github.io/l8-protocol/) for the exact construction.

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

For `idempotency_scope: "shared"` jobs the hashed value is `"shared\0" + idempotent_key` instead, in both this key and the result key below.

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

For a shared-scope job, drop `user_id` and pass the scope (requires `AQUIFER_SHARED_IDEMPOTENCY_ENABLED=true`):

```bash
curl "http://localhost:8080/results?idempotency_scope=shared&idempotent_key=weather:sf:2026-10-05"
```

## Autoscaling

| Header                    | Value                                              |
|---------------------------|----------------------------------------------------|
| `X-Aqueduct-Total-Jobs`   | Total jobs on this machine right now               |
| `X-Aqueduct-Queue-Depth`  | Jobs waiting to be dispatched                      |
| `X-Aqueduct-Flow-Rate`    | Current dispatch rate (RPS) for this queue         |

Traditional load balancers and autoscalers rely on failure to start scaling — capacity only responds once something's already struggling. Aquifer treats resources as fluid, not fixed: it paces down as machines show strain and back up as capacity comes online, using the same signals it already reports on every response.
