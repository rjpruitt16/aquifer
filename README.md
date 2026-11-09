# Aquifer

![How engineers respond to congestion: pacing and backpressure everywhere else in infrastructure, versus retrying everything immediately at the API layer](docs/images/how-engineers-respond-to-congestion.jpg)

A durable queue that paces traffic into a service at the rate it can take. Put it in front of your webhook handlers, a rate-limited third-party API, a GPU or your own backend: when a burst arrives faster than the target can handle, Aquifer holds the excess on disk instead of letting callers retry into it, and releases it at the rate you set for that target, slowing down whenever the target, or its load report, asks for less.

Use cases include API protection, webhook ingestion, coordinating agents and multigenerational search, where each lineage of an evolutionary or tree search gets its own queue so one can't starve the rest ([use cases](docs/pages/use-cases.md)).

- **Retry storm mitigation.** Callers wait in a durable queue instead of hammering a struggling service with retries, and the queue drains as capacity comes back.
- **Fairness.** Every tenant gets its own queue and they take turns. When one tenant flooded the GPU, database, CI runners and sandbox the agents shared, the small tenants' p99 stayed at 106 s, against 308 s when every agent called them directly, where a third of their workflows failed outright. The flooding tenant waited longer instead. Turns are equal by default, but the target can make them as unfair as it needs, live, by advertising a higher rate to one tenant: an emergency can take nearly all of a target's capacity for as long as it lasts ([dynamic priority](docs/pages/pacing.md#dynamic-priority)).
- **Delivery however the caller wants it:** results on the same connection (synchronous) or by durable webhook, and WebSocket sessions proxied with a transcript that replays anything missed after a reconnect.

![Traditional load balancing collapsing under a spike, round-robin flickering faster as instances die, versus Aquifer pacing that keeps the fleet stable while an autoscaler brings real capacity online](docs/images/fleet-degradation.gif)

## Why not Envoy, Redis, SQS or Hookdeck?

**Envoy** protects an overloaded service by rejecting the excess. A rejected request is gone, so every client retries on its own and they all compete for whatever capacity frees up next. Circuit breaking says "stop." Aquifer says "wait here, and I'll tell you when to go."

![Circuit breaking rejecting requests and clients retrying into a thundering herd when it reopens, versus Aquifer's per-client queues releasing work at the pace the backend asks for](docs/images/circuit-breaking-vs-queue.gif)

**Redis or SQS plus a rate-limited worker** can do the core of this: in the multi-agent benchmark, a carefully built Redis queue and dispatcher matched Aquifer's latency. Aquifer gives you the rest without writing it: a queue per tenant with fair admission, a pace the target sets live (`X-Aquifer-*` headers or vLLM's load report) instead of a fixed worker rate, `POST /proxy` falling back to the queue on the same connection, idempotency keys, and results by stream or webhook. It also drew about 5× fewer refusals from the benchmark's CI service, because it paced to the limits CI advertised.

**Hookdeck** queues inbound webhooks as a hosted service, billed by usage. Aquifer runs on your own machines: there's no per-event fee, but you run and pay for the infrastructure. It also paces outbound API calls, agent tool calls and WebSocket sessions.

**Temporal** makes workflows durable and can cap how fast a task queue runs, but that cap is a number you set, not what the downstream reports it can take right now.

They combine: Envoy in front as the gateway, Hookdeck delivering webhooks into Aquifer, and Temporal activities calling Aquifer's `POST /proxy`.

## Results

- **GPU work wasted on retries: 96.5% direct, 22% through Aquifer.** With load ramped far past a vLLM server's capacity, callers that timed out and retried left the GPU generating answers nobody was waiting for. Through Aquifer, capped at vLLM's real concurrency, vLLM's own queue stayed at 0 instead of peaking at 18,079 ([details](https://rjpruitt16.github.io/aquifer/benchmarks/#10-gpu-retry-tax-measured-in-tokens-runpodvllm)).
- **A fixed rate cut vLLM's peak queue from 449 waiting requests to 8** at the same offered load (40 requests a second).
- **Pacing from vLLM's own load report:** in a separate run, with the rate ceiling set past what vLLM can take, the load report slowed Aquifer whenever the KV cache passed 70%, holding it in a 22–79% cycle.
- **AI agents sharing a GPU, CI runners and a sandbox:** during bursts, compared with calling directly and retrying, the smallest tenants' p99 fell from 245 s to 149 s and each workflow needed 10 requests instead of 17, while the largest tenant's median rose from 168 s to 218 s: the cost of fair turns.
- **A 10× spike accepted without an error:** 3,000 jobs in 30 seconds, every one accepted, p99 166 ms to accept. That measures intake; the jobs then complete at the target's pace.

Methods and caveats: [benchmarks](https://rjpruitt16.github.io/aquifer/benchmarks/), and the end-to-end tests in front of Hatchet, vLLM and multi-agent workflows in [aqueduct-benchmarks](https://github.com/rjpruitt16/aqueduct-benchmarks).

## Quick start

```bash
go install github.com/rjpruitt16/aquifer/cmd/aquifer@latest
aquifer
```

To set a rate for a target, give it a config file keyed by host:

```bash
cat > aquifer.yml <<'EOF'
upstreams:
  httpbin.org:
    rps: 5             # at most 5 requests a second to this host
    max_concurrent: 2  # and at most 2 in flight per tenant
EOF
CONFIG_PATH=aquifer.yml aquifer
```

`rps` caps the host's total until the target advertises its own; `max_concurrent` is per tenant, so N busy tenants can have N × 2 in flight. Hosts without an entry get 2 requests a second, one at a time per tenant. The target can change its rate live with `X-Aquifer-Rps` and `X-Aquifer-Max-Concurrent` response headers.

Send it a job and watch it go through the line:

```bash
curl -s localhost:8080/jobs -H 'Content-Type: application/json' -d '{
  "user_id": "me", "idempotent_key": "first-job",
  "url": "https://httpbin.org/post", "method": "POST", "body": "{\"hello\":\"aquifer\"}"
}'
# {"job_id":"c22c473d…","status":"queued"}

curl -N localhost:8080/jobs/<job_id>/stream
# event: queued
# event: completed   (data carries the target API's response_status and body)
```

Or with Docker Compose:

```bash
docker compose build --pull --no-cache
docker compose up --detach
curl http://127.0.0.1:8080/ready
```

Fly.io, AWS and multi-node setups are in the [quick start guide](https://rjpruitt16.github.io/aquifer/quick-start/). Release binaries for Linux and macOS are on each [GitHub release](https://github.com/rjpruitt16/aquifer/releases) since v0.8.

Keep Aquifer on a private network behind a gateway you trust: it sends real HTTP requests to the URLs it's given, so it must never take them from untrusted callers. See [Why Aquifer](https://rjpruitt16.github.io/aquifer/) for where it sits.

## Durability and running more than one node

Each accepted job is written to the local disk of the node that accepted it (Pebble by default).

- **By default, writes are synced every 100 ms.** Aquifer answers before a write reaches disk, so a crash loses the jobs accepted since the last completed sync: about 100 ms of intake normally, more when the disk is slow. New jobs get `429` once that gap passes 1 second. For no loss of acknowledged jobs, set `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0`: every write waits for its own fsync, and 30 of 30 jobs survived a `kill -9`. It costs throughput: on a Fly performance-1x machine, about 150 jobs/s end to end instead of about 1,000.
- **A restarted node picks its queue back up** from its volume and dispatches it again. Delivery is at least once, so make the target and your webhook handler idempotent on the job's key.
- **Queued jobs are not replicated.** If a node's disk is lost, the jobs queued on it are lost with it. Where that matters, keep the original request until its result arrives, and resubmit with the same `idempotent_key` if it doesn't.
- **More than one node:** nodes are independent, each with its own queue and pacing. With [cluster routing](https://rjpruitt16.github.io/aquifer/api/#regional-cluster-routing) on, any node accepts a request and forwards it to that user's owner; when an owner is unreachable, its users' new requests go to the next node. Pacing is per node, so several nodes in front of one target multiply the rate it sees: either give each protected target its own node, or have the target divide its rate among them: Aquifer sends `X-Aquifer-Node-Id`, so it can count the nodes ([Partitioning](https://rjpruitt16.github.io/aquifer/partitioning/)).

## Documentation

**[rjpruitt16.github.io/aquifer](https://rjpruitt16.github.io/aquifer/)** covers pacing, streaming, pools, deployment, the full API and every setting.

For coding agents: [`llms.txt`](https://rjpruitt16.github.io/aquifer/llms.txt) lists every page as plain Markdown, so an agent can fetch only what it needs. To try Aquifer on your own workload, point your agent at the [Aquifer pilot skill](skills/aquifer-pilot/SKILL.md): it runs Aquifer in a shadow run on a branch and shows you the numbers. To benchmark, tune, partition or scale a deployment, use the [Aquifer engineer skill](skills/aquifer-engineer/SKILL.md).

The same pages are in this repository: [docs/pages](docs/pages), [API.md](docs/reference/API.md), [CONFIGURATION.md](docs/reference/CONFIGURATION.md), [ADAPTERS.md](docs/reference/ADAPTERS.md), [DRAIN_MODE.md](docs/reference/DRAIN_MODE.md), [REGISTRATION.md](docs/reference/REGISTRATION.md) and [benchmark.md](docs/reference/benchmark.md).

## Writing

- [Eliminate GPU Waste by Cutting the Retry Tax](https://rahmipruitt.me/content/gpu-retry-tax/)
- [GitHub Outages Show the Limits of Reactive Scaling](https://rahmipruitt.me/content/github-outage-reactive-scaling/)

## License

[Apache License 2.0](LICENSE). Releases before this change were MIT.

Built by [Rahmi Pruitt](https://rahmipruitt.me). Open to AI infra consulting, founding engineer, and contract work.
