---
name: aquifer-engineer
description: Engineer, benchmark, tune, deploy, scale, or review Inlet-to-Aquifer and Aqueduct-based traffic coordination. Use for webhook intake, Aquifer capacity planning, backend load tests, dynamic pacing headers, account-queue fairness, priority tiers, Valkey-backed regional partitioning, load-balancer pools, autoscaling signals, reliability drills, or production-readiness reviews.
---

# Aquifer Engineer

Pinned to Aquifer v0.15.1. Check the version they run (`aquifer --version`, or the image tag). This skill is new in v0.16.1. For a v0.16.1 or later release that's different, read the copy at that tag (https://github.com/rjpruitt16/aquifer/tree/<version>/skills/aquifer-engineer) or the one attached to the [GitHub release](https://github.com/rjpruitt16/aquifer/releases). For an older release there's no matching copy: use this one, and check each feature it mentions against that release's own docs before relying on it. Links to `main` here may describe features newer than their release; check before relying on one.

Engineer Inlet and Aquifer from measured limits, not guessed rate values. Treat Inlet as the authenticated provider-verification and ordered-intake edge, and Aquifer as the trusted, availability-first traffic coordinator behind it.

## Establish Current Behavior

1. Inspect current Aquifer source before changing or describing Aquifer behavior: this repository's `README.md`, `docs/reference/API.md`, `docs/reference/CONFIGURATION.md`, `docs/reference/benchmark.md`, and the relevant runtime code. If you are working from another repository, use a checked-out copy of [Aquifer](https://github.com/rjpruitt16/aquifer) or read it on GitHub.
2. If Inlet is in the path, read [Inlet's](https://github.com/rjpruitt16/inlet) `README.md`, `inlet.example.yml`, and relevant runtime code too.
3. Identify the path being tuned: provider to Inlet, caller to Canalis, ordered cloud queue, intake into Aquifer, queued HTTP dispatch, `POST /proxy`, pool dispatch, completion webhooks, or WebSockets.
4. Identify the protected bottleneck: application workers, database, cache, Valkey, external API, GPU, or Aquifer storage.
5. State the required throughput, p95/p99 latency, burst shape, tenant count, retry policy, durability level, and overload behavior.

Do not expose Aquifer directly to untrusted callers. Keep authentication, authorization, and destination selection in a trusted gateway or service. Inlet verifies supported provider signatures and selects destinations from deployment configuration; do not make destinations caller-controlled.

## Choose The Shape

Decide early which pieces the design needs, and say why. Each one does a job Aquifer doesn't:

- **Aquifer alone:** trusted callers (your own services or gateway) send jobs to one node, or to nodes split by protected resource. Start here unless one of the cases below applies.
- **Add [Inlet](https://github.com/rjpruitt16/inlet)** when the traffic is inbound webhooks from providers (Stripe, GitHub, Shopify and the others it supports). Inlet runs as a Lambda or Worker, verifies each provider's signature, maps the sender to `user_id`, uses the provider's event ID as `idempotent_key`, and acknowledges the provider only after the event is durably queued. Aquifer doesn't verify provider signatures itself.
- **Add [Canalis](https://github.com/rjpruitt16/canalis-rs)** when there are several Aquifer nodes and callers shouldn't have to know which one to use. Canalis is the gateway in front of them: it keeps each tenant on one node, sends new tenants only to nodes with room, and holds work in its own durable line when every node is full. Start from the [Canalis + Aquifer quick start](https://github.com/rjpruitt16/aquifer/blob/main/examples/canalis/README.md).
- **Both** for webhook intake at fleet scale: provider → Inlet → Canalis → Aquifer nodes → backend.

Don't add Canalis or Inlet just because they exist: each is another service to deploy, monitor and benchmark. If neither case applies, say so in the design.

## Benchmark Before Tuning

Benchmark the real backend before selecting `rps`, `max_concurrent`, backlog, or autoscaling thresholds. Measure dependencies separately and then measure the complete provider-to-Inlet-to-Aquifer path.

Read [references/tuning.md](references/tuning.md) for the benchmark sequence, Aqueduct headers, controller behavior, and priority-tier guidance.

Require evidence for every capacity claim:

- sustained throughput where backlog remains flat;
- p50, p95, and p99 latency at each queue boundary, the backend, and end to end;
- error, timeout, retry, `429`, dead-letter, and `5xx` rates;
- database, cloud queue, cache, and Valkey latency and saturation;
- Aquifer CPU, memory, storage, file descriptors, queue depth, and fullness;
- recovery after overload, dependency failure, process restart, and scale-up.

Use production-like payloads, network placement, machine sizes, durability settings, and provider limits. Keep headroom below the first unstable step.

Run load generators and long benchmarks on always-on machines (Fly Machines or VMs), not on a laptop or a dev sandbox that sleeps without an open session. Rerun runs that die on the cloud provider's API rather than counting them. Change one variable per comparison, or say which changes a result cannot separate.

## Configure Feedback

Set static Aquifer `CONFIG_PATH` values as hard ceilings. Let the backend lower the active pace at runtime with `X-Aqueduct-Rps`, `X-Aqueduct-Max-Concurrent`, and `X-Aqueduct-Max-Backlog`. Prefer `429` plus `Retry-After` or explicit pacing headers for healthy overload; reserve `5xx` for actual failures.

Describe Aquifer as TCP-inspired feedback control. Verify the implementation before naming a specific control law: queued dispatch and optional inbound pacing use different increase/decrease rules, and neither should be presented as strict AIMD without code changes.

`AQUIFER_MAX_PENDING_PER_UPSTREAM` is only the starting budget: a target's `X-Aqueduct-Max-Backlog` can raise it as well as lower it, so storage is bounded by the database size check (`AQUIFER_DB_MAX_BYTES`), not by that setting. Size `X-Aqueduct-Max-Backlog` in seconds of work: send `rate × seconds`, where `rate` is the peak rate jobs for that target API arrive at the Aquifer node across all tenants (intake fills the budget during an outage, not dispatch; the budget is per node, so size each node from its own intake) and the seconds cover at least the longest dependency outage the system should ride out. A budget sized as a small job count turns an outage into mass `429`s; in the Hatchet outage benchmark a 60-second budget at 20 rps gave small tenants a 107 s recovery p99, and a 600-second budget brought it to 5.5 s with no refusals. Don't let the budget shrink with a falling rate below the backlog Aquifer already holds.

Aquifer favors unbounded retries. It holds each job and paces every attempt, so for idempotent work submit with `max_retries: -1` (`X-Aqueduct-Max-Retries: -1`), which retries until success, `execute_before`, or 24 hours after submission; set `execute_before` on work with a deadline; the default of 4 retries runs out within about half a minute of an outage. Callers should submit with an `idempotent_key` (so repeats, including Inlet's confirmation resubmit before it acknowledges the provider, land on the same job), never cap attempts, and wait Aquifer's `Retry-After` after a `429` instead of stacking their own exponential backoff on top. Log `limit_reason`, `limit` and `current` from every `429` body; that is how an undersized budget shows up.

Don't recommend `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0`. It makes every write wait for its own fsync, cutting intake from about 1,000 to about 150 jobs/s on a Fly performance-1x machine, and it points at the wrong fix. Aquifer was never built to be the record of whether a job succeeded; its job is to move the most work through a bottleneck. With the default 100 ms flush, a crash can lose the last ~100 ms of accepted jobs, and recovery is a resubmit: the request that checks on a job is the same one that recovers it. Send the job again with the same `user_id` and `idempotent_key`; `200` with `"duplicate": true` means Aquifer still has it (same `job_id`), and `201` means Aquifer has no current record of it and has queued it again: it was lost, or its key expired (30 minutes after the job completed, 2 hours after it failed), so the target must deduplicate on `idempotent_key`. Inlet's confirmation resubmit before acknowledging the provider is this same check. Transactions belong in the caller's own database: a payment service records each charge there, keyed like the Aquifer job, and treats Aquifer as how the call reaches the processor, not as proof it happened.

Preserve Inlet's provider-derived fairness identity as Aquifer's `user_id` and enable account-queue isolation. Observe the load headers Aquifer sends to the backend and returns to callers; use them to tune capacity rather than hard-coding assumptions.

## Partition And Scale

Read [references/deployment.md](references/deployment.md) before designing a multi-node deployment.

- Keep one pacing checkpoint for each protected resource unless aggregate capacity is coordinated deliberately.
- Use Valkey-backed regional routing when callers may hit any Aquifer node and active users need sticky, load-aware placement.
- Point Aquifer at one writable primary or managed primary endpoint. Require healthy replicas and tested automatic failover; a read replica alone cannot perform Aquifer's coordination writes.
- Do not claim support for a sharded Redis Cluster keyspace. Current regional assignment uses a multi-key Lua transaction against one primary/endpoint.
- Keep Aquifer and its coordination store in the same region and benchmark the store under expected assignment, stream, and idempotency traffic.
- Scale from Aquifer fullness, queue/backlog trends, admission pressure, inbound latency, and resource saturation. Do not scale from request rate alone.

## Handle Priority Customers

Treat priority as an explicit service policy, not an arbitrary caller header. Derive the tier from gateway-authenticated identity or verified provider data and preserve a stable `user_id`.

Aquifer does not currently expose a priority header or a per-tier failure-decrease factor. Do not invent one. For soft priority, configure a ceiling high enough for the highest tier and let the trusted backend return tier-specific Aqueduct RPS/concurrency limits to each account queue. For hard guarantees, isolate tiers into separate upstream capacity, pools, or partitions.

Benchmark mixed-tier bursts and verify both the intended priority ratio and a nonzero floor for ordinary tenants. If implementing weighted scheduling or tier-specific decrease factors in Aquifer, add it as a first-class documented API with deterministic and integration tests.

## Validate Changes

Run focused unit tests, Aquifer's `go test ./...`, the Inlet test suite when Inlet is in the path, and the relevant shared Aqueduct Runner contract or scenario harness. For deployment changes, test provider retries, ordering, overload, latency, `429`, `5xx`, crash recovery, Valkey outage/failover, stale membership, autoscaler lag, and graceful drain.

Report measured before/after results, configuration, machine sizes, remaining bottlenecks, and failure semantics. Do not turn a synthetic best case into a production capacity promise.
