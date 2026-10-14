# Aquifer Benchmark And Pacing Guide

## Benchmark Sequence

1. Benchmark each protected dependency directly: database queries, cache operations, Valkey commands, external APIs, GPU inference, and application handlers.
2. Increase offered load in steps. Record the last step where throughput tracks offered load, backlog remains flat, and latency/error objectives hold.
3. Repeat through Aquifer with the real storage backend, flush policy, webhook behavior, account queues, network placement, and payload sizes.
4. Add bursts, slow responses, `429`, `5xx`, timeouts, dependency loss, process restarts, and autoscaler delay.
5. Set initial ceilings below the unstable step, then verify recovery and utilization during a soak.

Measure both capacity and latency. A backend can maintain throughput while p99 latency and queue depth grow without bound.

## Aqueduct Signals

The backend receives load context including:

- `X-Aqueduct-Total-Jobs`
- `X-Aqueduct-Queue-Depth`
- `X-Aqueduct-Flow-Rate`
- `X-Aqueduct-Active-Queues`
- `X-Aqueduct-Upstream-Backlog`

It can respond with:

- `X-Aqueduct-Rps`: lower this queue's dispatch rate below its configured ceiling.
- `X-Aqueduct-Max-Concurrent`: lower this queue's in-flight limit.
- `X-Aqueduct-Max-Backlog`: change the upstream's shared admission budget.
- `X-Aqueduct-Slow-Start: true`: make later new queues for the domain start at the floor and ramp.
- `429` plus `Retry-After`: declare temporary pressure without pretending the service is broken.

Aquifer also reports admission/load state with `X-Aqueduct-Active-Queues`, `X-Aqueduct-Upstream-Backlog`, `X-Aqueduct-Queue-Backlog`, `X-Aqueduct-Admission-Pressure`, and `X-Aqueduct-Fullness`.

## Controller Behavior

Keep these two loops distinct:

- Queued dispatch: a retryable failure halves that account queue's current RPS, down to 0.5 RPS. A successful response with `X-Aqueduct-Rps` sets the rate up to the configured ceiling; otherwise success recovers by 5%.
- Optional inbound pacing: Aquifer watches insert p95 latency and backlog growth. It cuts the advertised intake rate by 30% on congestion, grows 1.5x below the learned knee, and grows 5% near or above it when callers use the available rate.

These are TCP-inspired feedback loops, not identical controllers. If a design requires strict additive increase, weighted decrease, or a different control interval, change and test that behavior explicitly.

## Priority Tiers

Use an authenticated identity-to-tier mapping outside Aquifer. Never trust an end user to assign its own tier.

For soft priority with account queues:

1. Set the static configured ceiling at the maximum any tier may receive.
2. Have the trusted backend return tier-specific `X-Aqueduct-Rps` and, when useful, `X-Aqueduct-Max-Concurrent` values.
3. Test sustained and burst traffic with all tiers active.
4. Verify ordinary tenants retain useful throughput and priority tenants receive the intended advantage.

This does not change the current failure rule: retryable failures halve every affected queue. Hard reservations or tier-specific decrease factors require separate capacity or a new scheduler/API feature.

## Evidence To Keep

Record the load generator, commit, configuration, machine types, region, dependency tiers, payload distribution, duration, offered/completed rates, latency percentiles, queue/backlog curves, resource use, failure counts, and recovery time. Keep raw output when practical so later changes can be compared on the same basis.
