# Use cases

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

**Webhook ingestion — keep noisy senders from monopolizing processing**
```
webhook providers  →  your verification endpoint  →  POST /jobs to Aquifer  →  webhook handlers
```
Verify each provider's signature at your edge, map the sender or customer to `user_id`, and use the provider's event ID as the `idempotent_key`. Aquifer absorbs bursts into its persisted queue and paces work into your handlers. With AccountQueue mode enabled, one noisy sender gets its own queue; under shared backlog pressure, fair admission increasingly rejects that oversized queue while leaving room for quieter senders. Aquifer does not replace provider authentication or translate arbitrary webhook formats. For supported providers, **[Inlet](https://github.com/rjpruitt16/inlet)** is the ready-made public front door; otherwise, use your own verification endpoint or custom adapter. For high-throughput intake, keep the default batched Pebble flush and resubmit the same event after a short jittered delay before acknowledging the provider; see the [confirmation best practice](../../API.md#best-practice-confirm-batched-webhook-intake). Use `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0` when every accepted event requires a strict disk-sync guarantee.

**Outbound — a durable checkpoint in front of a rate-limited resource**
```
your app  →  POST /jobs to Aquifer  →  database / CI runner / OpenAI / Stripe / any rate-limited API
```
Calling something with its own capacity limit (a database read replica, a CI runner, a third-party API)? Aquifer queues the calls durably and dispatches them at your configured rate, so a burst from your own side never becomes the thing that takes the target API down. Works especially well closed-loop: if the target API already speaks `X-Aqueduct-*` headers, it can tell Aquifer to back off in real time instead of you guessing a static rate.

**Capacity-aware reverse proxy — pace and route behind your gateway**
```
your users  →  trusted gateway  →  POST /proxy to Aquifer  →  your resources
```
Point your trusted gateway at Aquifer, then point Aquifer at your resources. Aquifer tries the request directly first (a healthy resource sees no queue at all) and only falls back to durable queuing when something's actually overloaded, on the same connection, staying in queue mode until that domain's backlog is genuinely drained (not just until a cooldown timer expires). See [`POST /proxy`](../../API.md#post-proxy) for the details, including the header a target API can use to request queuing proactively.

**WebSocket proxy — durable replay and paced reconnects**
```
clients  →  trusted gateway  →  GET /websocket on Aquifer  →  WebSocket backend
```
Aquifer can persist an ordered WebSocket transcript in Valkey, replay missed backend events after a client reconnects, and pace new or replacement target API connections. Authentication remains the gateway's job. See [`GET /websocket`](../../API.md#get-websocket) for the protocol and limits.

**Sleep through partial outages.** On Fly.io, set `AQUIFER_FLY_REGIONS` and that same overload signal drives real cross-region redirect: other regions Aquifer is deployed to get tried live, over Fly's private network, before this instance ever falls back to its own local queue, nearest-first by measured latency, deterministic enough that two callers racing the same job converge on the same region instead of each chasing their own nearest option. One region degrading routes around itself instead of paging you at 3am. If every known region is down too, that's a real fleet-wide problem, and Aquifer says so with a `429` and a long `Retry-After` rather than quietly queueing it somewhere and hoping. See [`POST /proxy`](../../API.md#post-proxy)'s "Cross-region redirect" section for the full mechanics, including the one honestly-documented tradeoff it doesn't try to hide.

In all six, the target API can lower the dispatch pace via response headers. See [Dynamic Pacing](pacing.md) for how the ceiling, backoff, and recovery actually work.

Long-term protocol goal: if more services emit `X-Aqueduct-*`, agents can respond to capacity signals instead of independently guessing retry and concurrency behavior. Aquifer works today without ecosystem adoption; broader protocol adoption is the longer-term goal.
