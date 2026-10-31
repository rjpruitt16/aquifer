---
name: aquifer-pilot
description: Evaluate Aquifer against your own workload. A shadow pilot on a branch, run for a few hours to a day against a replica or staging, measuring saturation, errors, latency, fairness and cost. Use it when someone asks whether Aquifer fits their system, wants to pilot it, or wants numbers before adopting it.
---

# Aquifer pilot

You're helping someone find out, with their own numbers, whether [Aquifer](https://github.com/rjpruitt16/aquifer) helps their workload. Keep it simple:
- a branch, not a migration;
- a replica, staging or a shadow copy, never production;
- a few hours to a day of real-shaped traffic;
- a write-up they can share.

## Start: how involved does your user want to be?

Ask first: "Do you want to be part of this, or should I go off, do the whole pilot, and come back with the results?" Most people want the second. Make that easy.

Either way, get these answers in one message before you start. They're the only things you can't safely decide alone:

1. **Where it's safe to run:** the replica, staging or shadow copy you may send traffic to. Name what is production and off-limits.
2. **The budget:** how long you may run (a few hours to a day) and what you may spend on machines, or "local only".
3. **The traffic:** recorded traffic you may replay, or a description to model (rates, burst sizes, number of tenants).
4. **Permission to deploy:** whether you may start Aquifer and a load generator on that infrastructure.

Then work in the mode they chose:

- **Hands-off (the default):** do steps 1–6 without asking again. When a choice is yours to make, pick the sensible default and write it down in the report's config section. Interrupt only to stop something unsafe: you're about to touch production, exceed the budget, or a pass looks like it's harming the replica. Come back with the report and the raw data behind it.
- **Hands-on:** show the plan after the fit check, and check in after the benchmark and after each pass.

If you can't find something you need (a missing metric, a backend you can't instrument), work around it and say how in the report. Stop and ask only if there's no workaround.

## What Aquifer is (and isn't)

Aquifer is **admission control** for soft real-time work, and for webhook and WebSocket overload. It accepts a burst, persists it, and releases it at the pace the backend can take. The backend sets that pace through `X-Aqueduct-*` response headers. Tenants get their own queues, so one tenant's burst doesn't starve the others.

Clear these up early, because people often assume otherwise:

- **Aquifer is not a gateway.** For a managed gateway in front of several Aquifer nodes, use [Canalis](https://github.com/rjpruitt16/canalis-rs). If webhooks just need to land in Aquifer, use [Inlet](https://github.com/rjpruitt16/inlet), a Lambda/Worker that verifies them and pushes them in.
- **L8 is optional.** It's an extra layer of security (signed node identity and webhook delivery). A pilot doesn't need it.
- **It's tested enough for a pilot:**
  - contract tests on every change, run against Canalis and Inlet too;
  - a week-long soak under kill -9s, with a public job ledger.
- **It isn't a 100% guarantee.** Delivery is at-least-once, so make handlers idempotent on `idempotent_key`. Accepted jobs live on the node's own disk (Pebble). A node that restarts keeps them. Two things can lose them:
  - a crash can lose jobs accepted in the last ~100 ms (`AQUIFER_PEBBLE_FLUSH_INTERVAL_MS`; set `0` to sync every write);
  - a machine that's lost for good takes its queue with it.

  That's a deliberate choice. Aquifer is built to absorb the biggest shock it can, fast, and complete durability would slow that down. If you need 100% durability, keep a durable log as the source of truth: Kafka (or SQS, or your own database plus an outbox), writing into Canalis or Aquifer. Then replay from it if a node is ever lost. Build `idempotent_key` from the message's position (e.g. topic, partition and offset), so a replay lands on the jobs Aquifer already has.

  Aquifer is still tunable. `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0` syncs every write, trading intake speed for a smaller loss window. And it's open source (Apache-2.0), so you're welcome to fork it in another direction.

Why admission control is worth testing: CockroachDB measured the same idea in their database.
- [Without it, TPC-C throughput collapsed](https://www.cockroachlabs.com/blog/admission-control-in-cockroachdb/) from 1,200 to 400 txn/s within 10 minutes of overload. With it, throughput held at about 1,050 txn/s.
- [A background job spiked foreground latency over 1000x](https://www.cockroachlabs.com/blog/goroutine-scheduling-elastic-admission-control-cockroachdb/) (from under 2 ms to seconds) without it. With it, latency peaked at 4 ms.

Those are CockroachDB's own results, not Aquifer's. This pilot is how they get Aquifer's.

## 1. Fit check

Ask what sits behind their bottleneck, and say plainly if Aquifer isn't a fit.

- **Good fit:** capacity that is elastic but slow to come online or expensive to keep idle. Examples are GPUs and inference servers, rate-limited third-party APIs, CI runners, databases with bursty writers, and webhook or WebSocket floods.
- **Overkill:** a ceiling that is fixed for good, or traffic that never bursts past capacity.

## 2. Benchmark the backend first

Before Aquifer goes in front, find the backend's real limits. Ramp load against the replica until it degrades, and record:
- the knee (the rate where latency or errors turn up);
- the error rate past it;
- what saturation looks like: GPU utilization, KV-cache use, CPU, queue depth.

Everything after this is compared against these numbers, and the backend's pacing logic (step 3) needs them.

## 3. Teach the backend to set the pace

### What Aquifer tells the backend

Every request Aquifer sends carries these headers, so the backend knows what's coming (`X-Aquifer-*` aliases too):

| Header | Meaning |
|---|---|
| `X-Aqueduct-Flow-Rate` | This queue's current dispatch rate (rps) |
| `X-Aqueduct-Queue-Depth` | This queue's jobs still waiting |
| `X-Aqueduct-Active-Queues` | Queues on this node currently sending to this backend |
| `X-Aqueduct-Upstream-Backlog` | Jobs on this node waiting for this backend, across all its queues |
| `X-Aqueduct-Total-Jobs` | All jobs on this node right now |

Log these during the pilot. They show how much work is waiting for the backend, not just what has arrived.

### Account queues

They're on by default. Each tenant (`user_id` plus API key) gets its own queue at each backend, paced on its own, so one tenant's burst or retry storm can't slow the others down. Most fairness results come from this.

The backend has the last say: `X-Aqueduct-Account-Queue: disabled` puts everyone in one shared queue, for backends that need one global order. Check which mode the pilot ran in and say so in the report.

### A pool of backends

If the backend is several interchangeable instances (a fleet of GPU servers, a set of workers), don't put a load balancer in front of them and point Aquifer at that. Register them as a **pool** instead:
- Each instance registers with `POST /pools/{pool_id}/members` and keeps re-registering as a heartbeat. It declares its `capacity_rps`.
- Jobs then target `"pool_id"` instead of `"url"`.
- Aquifer picks a member for each dispatch, weighted by declared capacity × live reputation. A member declaring 100 rps gets about 4× the work of one declaring 25.
- A failure halves that member's share, and a success nudges it back up. A `5xx` is retried on another member.
- The pool's ceiling is the live sum of its members, so it grows and shrinks as instances register, degrade or drop out.

This is the most precise option Aquifer has: it paces each instance by what that instance says it can do, instead of spreading load evenly and hoping. For a pilot, record each member's declared capacity and its share of the work. Overload signals matter even more here: answer `429` or a lower `X-Aqueduct-Rps` when busy, because a `5xx` lowers that member's reputation.

### Several Aquifer nodes

Aquifer scales by partitioning, not by sharing state:
- **Static:** dedicate a node to one protected resource, such as one GPU pool or one third-party API.
- **Cluster routing:** nodes share a Valkey that keeps each tenant on one node.
- **Drain mode:** a node takes one tenant's burst at a time.
- **Canalis:** a gateway in front of many nodes.

Partitioning matters for the pacing math. **Pacing is per node and per queue.** A rate the backend advertises applies to each queue that received it. A node keeps the total across its own queues only within its configured budget, and nodes don't coordinate with each other. So the backend sees up to:

> advertised rate × active queues per node × number of nodes

Account for that in the AIMD:
- **Count the senders.** Read `X-Aqueduct-Active-Queues` per node. Tell nodes apart by `X-Aqueduct-Node-Id`, which every request carries. Add them up.
- **Divide.** Advertise each queue `budget ÷ total active queues`, not the whole budget.
- **Scale the step.** An additive step of +5 rps lands on every queue at once. With 20 queues across 4 nodes, that's +100 rps at the backend. Size the increase for the total, and cut on the total.

A feedback loop on the backend's own saturation signal corrects itself either way, because it measures the real total. But dividing first means fewer overshoots.

With `AQUIFER_SHARED_DESTINATION_LIMITS=1` (v0.16.0, off by default), the advertised values are totals per node instead: the node shares them round-robin across its tenant queues, and the backend sees up to advertised totals × nodes in flight. The rate total paces queued dispatches only: `/proxy` direct attempts count toward the in-flight total but not the rate, so a burst of them can briefly exceed the advertised rate. Divide by nodes only, not by `X-Aqueduct-Active-Queues`.

### The control loop

Aquifer follows the backend. The backend should run the control loop on the headers it returns. **Additive increase, multiplicative decrease** (AIMD) is a good default: TCP congestion control uses it, and so does Aquifer for its own intake (`internal/admission/inbound_rate.go`).

- While healthy, raise the advertised rate by a fixed step: `X-Aqueduct-Rps: <rate>`, and optionally `X-Aqueduct-Max-Concurrent`.
- On a sign of saturation, cut it by a factor (×0.5–0.7) and wait a round trip before cutting again.
- Remember the rate where trouble started (TCP's ssthresh). Grow quickly below it and slowly above it.
- Prefer `429` or a lower `X-Aqueduct-Rps` for "busy, slow down". Use `5xx` only for real failures.

Aquifer reacts on its own as well: each retryable failure (`5xx`, `429`, `408`) halves that queue's rate, and each success recovers it by 5% toward the ceiling.

The right saturation signal depends on what the backend is:

| Backend | Cut when |
|---|---|
| Database | p95/p99 query latency rises above its healthy baseline |
| Cache | the hit rate falls, or misses push latency up. After a deploy or restart, let it warm up with `X-Aqueduct-Slow-Start: true`, which starts new queues at a low rate and ramps up. |
| GPU / inference | KV-cache or GPU memory nears full, or time-to-first-token rises. vLLM and Triton already report KV-cache use over ORCA, and Aquifer reads that with no backend change. |
| App server or third-party API | the error rate or timeouts rise, or the API's own rate-limit headers drop |

**Size the backlog budget in seconds.** `X-Aqueduct-Max-Backlog` is a job count, but choose it as time: `rate × seconds` Aquifer should be able to hold, where `rate` is the peak rate jobs for that backend arrive at the node across all tenants (intake fills it during an outage, not dispatch; the budget is per node), at least the longest outage the pilot should ride out. Teams get this wrong more than anything else, and it looks like Aquifer failing. A 60-second budget at 20 rps (1,200 jobs) turned a 5-minute database outage in the Hatchet benchmark into about 25,000 `429`s, and small tenants' recovery p99 was 107 s (3.8 s behind Envoy); a 600-second budget gave them no refusals and a 5.5 s p99. If the backend lowers its rate when it's in trouble, don't let the budget fall with it below the backlog Aquifer already holds.

**Priority tenants.** With account queues on (the default), every tenant (`user_id` and API key) has its own queue, and each queue follows the headers on its own responses, up to the configured ceiling. So the backend can favour priority customers today: cut them more gently (×0.8 instead of ×0.5), or advertise them a higher rate. It recognizes them by their API key or a header on the job. Slow priority tenants with a lower `X-Aqueduct-Rps` on successful responses, not with `429`s: every `429` halves that queue's rate, whatever the backend intended.

### Retries: let Aquifer keep trying

Aquifer is built for unbounded retries. It holds the job and paces each attempt, so retrying costs the backend nothing extra, and a job that stops retrying is work lost during the very outage Aquifer is there to absorb.

- **Dispatch:** for idempotent work, submit with `max_retries: -1` (or `X-Aqueduct-Max-Retries: -1`). Aquifer then retries until the job succeeds, up to `execute_before` or 24 hours; set `execute_before` on work that has a deadline. The default of 4 retries runs out within about half a minute, so a longer outage fails jobs.
- **Callers:** submit once with an `idempotent_key` and don't cap attempts; use `execute_before` for a deadline instead. When Aquifer refuses with `429`, wait its `Retry-After` and submit again. Don't stack exponential backoff on top: a client whose own backoff doubles to 32 s waited about 100 s per document in the Hatchet benchmark, against Aquifer's `Retry-After`, which for backlog refusals (`upstream_queue`, `queue_fair_share`) stays at its base, `AQUIFER_RETRY_AFTER_SECONDS` (5 s by default). Memory, disk-size and sync-lag refusals in a row double it, up to 60 s.
- **Record the reason for every `429`:** log `limit_reason`, `limit` and `current` from the body. They say which limit refused the job and how full it was, and that's how a budget that's too small shows up.

Say in the report which retry settings each pass used.

## 4. Shadow run

Have the coding agent put Aquifer on a branch. Use whichever shape fits:
- the [binary or Docker Compose quick start](https://github.com/rjpruitt16/aquifer#quick-start);
- the [Canalis + Aquifer quick start](https://github.com/rjpruitt16/aquifer/blob/main/examples/canalis/README.md) for a gateway in front of several nodes;
- [Inlet](https://github.com/rjpruitt16/inlet) for webhooks.

Then:
- **Point it at a replica, staging or shadow copy.** Nothing in the pilot reaches production or causes real side effects: no real payments, emails or customer webhooks.
- **Drive real-shaped traffic.** Replay recorded traffic or mirror a slice of it, with bursts as big as the real ones, across several tenants.
- **Run two passes of the same traffic:**
  - a **baseline**, sent straight to the backend the way it's called today, retries included;
  - **through Aquifer**, with the backend's pacing headers on.
- **Run each pass for a few hours to a day,** long enough to include several bursts.
- **Run the load generator and orchestration on an always-on machine,** such as a Fly Machine or a VM, with the run started there and left to finish. A laptop or a dev sandbox that sleeps when nobody is connected pauses the load mid-run, and the results look like a stall.
- **Rerun a pass that dies on the cloud provider's API** (a failed machine create or volume, a timed-out deploy). Count it as a failed launch, not as a result for either side.
- **Change one thing between runs.** If a fix needs two changes, say the result can't tell them apart, or add a run with only one.

## 5. Measure

Measure the same things on both passes. On the Aquifer pass, scrape `GET /metrics` (Prometheus format, next to `/health`) every 15–30 s, and keep the scrapes. It has:
- jobs by outcome;
- attempts at the backend by status (`2xx`, `429`, `5xx`, no response);
- time waiting in Aquifer and end to end (histograms);
- 429s by reason;
- live active queues, backlog and flow rate;
- `aquifer_fairness_jain`.

For per-tenant detail, start Aquifer with `AQUIFER_METRICS_TENANT_LABELS=true` (the first 1,000 tenants get their own series). The baseline pass has no Aquifer, so have the load generator record each job's tenant, submit and finish times, status and attempts there. Do the same on the Aquifer pass too, as a cross-check. Have the backend log the `X-Aqueduct-*` headers as they arrive. The scrapes, those logs and the backend's own metrics are the evidence.

- **Saturation:** the share of time the backend was saturated, and its utilization the rest of the time. For GPUs, the useful number is how long they stay busy and accepting work without tipping over.
- **Errors:** 429/5xx rates, timeouts, and retries sent (Aquifer's pass should send far fewer).
- **Latency:** p50 and p99 end to end, including time spent waiting in Aquifer's queue.
- **Work done:** jobs completed per hour, and how long the backlog took to drain after each burst.
- **Fairness:**
  - each tenant's share of completed work;
  - each tenant's p99;
  - whether a bursty tenant slowed everyone else down;
  - [Jain's fairness index](https://en.wikipedia.org/wiki/Fairness_measure) over tenants' throughput (1.0 is perfectly fair). Aquifer reports it as `aquifer_fairness_jain`; compute it from the load generator's log for the baseline.
- **Cost:** turn saturation and work done into money. Examples: GPU hours needed for the same work, instances that no longer need overprovisioning, or third-party rate limit you no longer waste on retries.

## 6. Write it up

End with a short report they can share, whether it went well or not:

```markdown
## Aquifer pilot: <system>, <date>
**Backend:** what it is, its size, its benchmarked knee
**Traffic:** replayed or mirrored, duration, tenants, burst sizes
**Shape:** Aquifer alone / with Canalis / with Inlet; number of Aquifer nodes; account queues on/off; Aquifer version; pacing signal used

| | Baseline | Through Aquifer |
|---|---|---|
| Time saturated | | |
| 429 / 5xx rate | | |
| Retries sent | | |
| p50 / p99 end to end | | |
| Jobs per hour | | |
| Burst drain time | | |
| Worst tenant's p99 / fairness index | | |
| Estimated cost | | |

**What didn't improve:**
**Config:** headers and env vars used, and every default chosen without asking
**Raw data:** where the per-job log and backend metrics are
```

Report what didn't improve as plainly as what did. If they're happy to share it, they can post it, or open an issue on the Aquifer repo.
