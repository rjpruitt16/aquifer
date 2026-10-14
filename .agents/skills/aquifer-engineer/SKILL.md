---
name: aquifer-engineer
description: Engineer, benchmark, tune, deploy, scale, or review Aquifer and Aqueduct-based traffic coordination. Use for Aquifer capacity planning, backend load tests, dynamic pacing headers, account-queue fairness, priority tiers, Valkey-backed regional partitioning, load-balancer pools, autoscaling signals, reliability drills, or production-readiness reviews.
---

# Aquifer Engineer

Engineer Aquifer from measured limits, not guessed rate values. Preserve its role as a trusted, availability-first traffic coordinator behind the caller's authentication and destination controls.

## Establish Current Behavior

1. Read the repository's `README.md`, `API.md`, `CONFIGURATION.md`, and relevant code before changing behavior. Treat them as newer than this skill.
2. Identify the path being tuned: intake into Aquifer, queued HTTP dispatch, `POST /proxy`, pool dispatch, completion webhooks, or WebSockets.
3. Identify the protected bottleneck: application workers, database, cache, Valkey, external API, GPU, or Aquifer storage.
4. State the required throughput, p95/p99 latency, burst shape, tenant count, retry policy, durability level, and overload behavior.

Do not expose Aquifer directly to untrusted callers. Keep authentication, authorization, and destination selection in a trusted gateway or service.

## Benchmark Before Tuning

Benchmark the real backend before selecting `rps`, `max_concurrent`, backlog, or autoscaling thresholds. Measure dependencies separately and then measure the complete path through Aquifer.

Read [references/tuning.md](references/tuning.md) for the benchmark sequence, Aqueduct headers, controller behavior, and priority-tier guidance.

Require evidence for every capacity claim:

- sustained throughput where backlog remains flat;
- p50, p95, and p99 latency at the backend and end to end;
- error, timeout, retry, `429`, and `5xx` rates;
- database, cache, and Valkey latency and saturation;
- Aquifer CPU, memory, storage, file descriptors, queue depth, and fullness;
- recovery after overload, dependency failure, process restart, and scale-up.

Use production-like payloads, network placement, machine sizes, durability settings, and provider limits. Keep headroom below the first unstable step.

## Configure Feedback

Set static `CONFIG_PATH` values as hard ceilings. Let the backend lower the active pace at runtime with `X-Aqueduct-Rps`, `X-Aqueduct-Max-Concurrent`, and `X-Aqueduct-Max-Backlog`. Prefer `429` plus `Retry-After` or explicit pacing headers for healthy overload; reserve `5xx` for actual failures.

Describe Aquifer as TCP-inspired feedback control. Verify the implementation before naming a specific control law: queued dispatch and optional inbound pacing use different increase/decrease rules, and neither should be presented as strict AIMD without code changes.

Enable `X-Aqueduct-Account-Queue: enabled` when `user_id` identifies tenants sharing an upstream. Observe the load headers Aquifer sends to the backend and returns to callers; use them to tune capacity rather than hard-coding assumptions.

## Partition And Scale

Read [references/deployment.md](references/deployment.md) before designing a multi-node deployment.

- Keep one pacing checkpoint for each protected resource unless aggregate capacity is coordinated deliberately.
- Use Valkey-backed regional routing when callers may hit any Aquifer node and active users need sticky, load-aware placement.
- Point Aquifer at one writable primary or managed primary endpoint. Require healthy replicas and tested automatic failover; a read replica alone cannot perform Aquifer's coordination writes.
- Do not claim support for a sharded Redis Cluster keyspace. Current regional assignment uses a multi-key Lua transaction against one primary/endpoint.
- Keep Aquifer and its coordination store in the same region and benchmark the store under expected assignment, stream, and idempotency traffic.
- Scale from Aquifer fullness, queue/backlog trends, admission pressure, inbound latency, and resource saturation. Do not scale from request rate alone.

## Handle Priority Customers

Treat priority as an explicit service policy, not an arbitrary caller header. Derive the tier from gateway-authenticated identity and preserve a stable `user_id`.

Aquifer does not currently expose a priority header or a per-tier failure-decrease factor. Do not invent one. For soft priority, configure a ceiling high enough for the highest tier and let the trusted backend return tier-specific Aqueduct RPS/concurrency limits to each account queue. For hard guarantees, isolate tiers into separate upstream capacity, pools, or partitions.

Benchmark mixed-tier bursts and verify both the intended priority ratio and a nonzero floor for ordinary tenants. If implementing weighted scheduling or tier-specific decrease factors in Aquifer, add it as a first-class documented API with deterministic and integration tests.

## Validate Changes

Run focused unit tests, `go test ./...`, and the relevant benchmark or scenario harness. For deployment changes, test overload, latency, `429`, `5xx`, crash recovery, Valkey outage/failover, stale membership, autoscaler lag, and graceful drain.

Report measured before/after results, configuration, machine sizes, remaining bottlenecks, and failure semantics. Do not turn a synthetic best case into a production capacity promise.
