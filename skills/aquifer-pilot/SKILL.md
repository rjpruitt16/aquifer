---
name: aquifer-pilot
description: Evaluate Aquifer against your own workload. A shadow pilot on a branch, run for a few hours to a day against a replica or staging, measuring saturation, errors, latency, fairness and cost. Use it when someone asks whether Aquifer fits their system, wants to pilot it, or wants numbers before adopting it.
---

# Aquifer pilot

Pinned to Aquifer v0.15.1. Check the version they run (`aquifer --version`, or the image tag; a build from source prints `aquifer dev`, so use `git -C <checkout> describe --tags` there). If it's different, read this skill at that release instead: https://github.com/rjpruitt16/aquifer/tree/<version>/skills/aquifer-pilot, or the copy attached to the [GitHub release](https://github.com/rjpruitt16/aquifer/releases) (releases before v0.16.1 don't have one). Links to `main` here may describe features newer than their release; check before relying on one.

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
  - a crash can lose jobs accepted in the last ~100 ms (`AQUIFER_PEBBLE_FLUSH_INTERVAL_MS`);
  - a machine that's lost for good takes its queue with it.

  That's a deliberate choice. Aquifer is built to absorb the biggest shock it can, fast, and complete durability would slow that down. If you need 100% durability, keep a durable log as the source of truth: Kafka (or SQS, or your own database plus an outbox), writing into Canalis or Aquifer. Then replay from it if a node is ever lost. Build `idempotent_key` from the message's position (e.g. topic, partition and offset), so a replay lands on the jobs Aquifer already has.

  **Don't recommend `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0`.** It makes every write wait for its own fsync, which cuts intake from about 1,000 to about 150 jobs/s on a Fly performance-1x machine. It also points at the wrong fix. Aquifer was never built to be the record of whether a job succeeded; its job is to move the most work through a bottleneck it can. The caller owns that record:
  - **Recovery is a resubmit.** The request that checks on a job is the same one that recovers it: send the job again with the same `user_id` and `idempotent_key`. `200` with `"duplicate": true` means Aquifer still has it (same `job_id`, read its status or result). `201` means it was lost in a crash and has just been queued again. So after a crash, the caller's usual retry loop repairs the gap without a special path.
  - **Money and other transactions belong in the caller's own database.** A payment service records each charge there, keyed the same way it keys the Aquifer job, and treats Aquifer as the way the call reaches the processor, not as proof it happened.

  The setting still exists for someone who has measured the cost and wants it, and Aquifer is open source (Apache-2.0), so you're welcome to fork it in another direction. But in a pilot, steer people to resubmit-by-key and their own records first.

Why admission control is worth testing: CockroachDB measured the same idea in their database.
- [Without it, TPC-C throughput collapsed](https://www.cockroachlabs.com/blog/admission-control-in-cockroachdb/) from 1,200 to 400 txn/s within 10 minutes of overload. With it, throughput held at about 1,050 txn/s.
- [A background job spiked foreground latency over 1000x](https://www.cockroachlabs.com/blog/goroutine-scheduling-elastic-admission-control-cockroachdb/) (from under 2 ms to seconds) without it. With it, latency peaked at 4 ms.

Those are CockroachDB's own results, not Aquifer's. This pilot is how they get Aquifer's.

## Running Aquifer locally

Everything a pilot needs is in the docs below; you shouldn't need Aquifer's Go source. If one of them doesn't answer a question, note it in the report: that's a docs bug.

- **Build and run.** From a checkout: `go build -o ./bin/aquifer ./cmd/aquifer`, or `go install github.com/rjpruitt16/aquifer/cmd/aquifer@<version>`. Then `PORT=8080 DB_PATH=./aquifer-data CONFIG_PATH=aquifer.yml ./bin/aquifer`. `GET /ready` answers once it's up.
- **On a laptop:** Aquifer listens on every interface (`:PORT`); there's no setting to bind it to loopback only. On first start it writes its L8 identity (`.l8-key`, `l8-trust`) into the working directory (`L8_KEY_PATH` moves the key), so start it from a scratch directory.
- **Every setting** (env vars and the YAML rate file) is in [docs/pages/configuration.md](../../docs/pages/configuration.md). The root `CONFIGURATION.md` only explains why some defaults are what they are.
- **Endpoints:** `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/stream`, `POST /proxy`, `GET /metrics`, `GET /health`, all in [API.md](../../API.md). Account queues (one queue per `user_id`) are on by default.
- **How the limits combine** (the question pilots most often go to the source for):
  - The YAML `rps` for a host is meant as the total for that target, but in v0.15.1 it's only loosely held: Aquifer scales active queues down every 3 s, and each queue climbs back up between checks, so N busy tenants can send close to N × `rps` ([#146](https://github.com/rjpruitt16/aquifer/issues/146)). In a local test with 3 tenants, `rps: 10` delivered about 13/s and `rps: 30` delivered about 70/s, which was the backend's own limit. Until that's fixed, bound the total with `max_concurrent` (it's per queue, so in-flight ≤ N × `max_concurrent`), and measure the delivered rate at the backend.
  - `max_concurrent` applies to each tenant's queue separately and isn't summed. With N active tenants, up to N × `max_concurrent` requests can be in flight at the target.
  - Pacing headers from the target (`X-Aqueduct-Rps`, `X-Aqueduct-Max-Concurrent`) set the pace of the queue whose request they answer, and only ever within the YAML ceiling. A target that wants a total has to divide it by the number of active queues; each request carries `X-Aqueduct-Active-Queues`.
  - Start with the YAML ceiling set from the step 2 benchmark. Add headers only if a measured run shows the static ceiling isn't enough, and compare runs with and without them.

### One pass in one command

Each pass needs the same plumbing: a fresh Aquifer with its own data directory, a wait until it's ready, metrics saved while the load runs, and everything stopped afterwards, even if the load fails. This script does that (tested against Aquifer v0.15.1). Save it as `pilot/pass.sh` and pass it the load command:

```bash
#!/usr/bin/env bash
# pass.sh NAME CONFIG.yml LOAD_CMD...
# One local pass: fresh Aquifer and data dir, wait until ready, scrape
# /metrics every 5 s while LOAD_CMD runs, save final metrics, stop everything.
set -euo pipefail
name=$1; cfg=$(cd "$(dirname "$2")" && pwd)/$(basename "$2"); shift 2
port=${AQ_PORT:-8080}; bin=$(cd "$(dirname "${AQ_BIN:-pilot/aquifer}")" && pwd)/$(basename "${AQ_BIN:-pilot/aquifer}")
dir=pilot/runs/$name; rm -rf "$dir"; mkdir -p "$dir"
aq=""; scr=""
trap '[ -n "$scr" ] && kill $scr 2>/dev/null; [ -n "$aq" ] && kill $aq 2>/dev/null; wait 2>/dev/null || true' EXIT
(cd "$dir" && PORT=$port DB_PATH=./aquifer-data CONFIG_PATH=$cfg exec "$bin") > "$dir/aquifer.log" 2>&1 &
aq=$!
for _ in $(seq 50); do curl -sf "localhost:$port/ready" >/dev/null && break; sleep 0.2; done
curl -sf "localhost:$port/ready" >/dev/null || { echo "aquifer didn't start; see $dir/aquifer.log"; exit 1; }
(while sleep 5; do curl -s "localhost:$port/metrics" > "$dir/metrics-$(date +%s).prom"; done) & scr=$!
"$@"
curl -s "localhost:$port/metrics" > "$dir/metrics-final.prom"
curl -s "localhost:$port/health" > "$dir/health.json"
```

```bash
AQ_PORT=8080 AQ_BIN=./bin/aquifer pilot/pass.sh aq-static pilot/aquifer.yml python3 pilot/load.py --target http://127.0.0.1:8080
```

The load command should return only when its jobs have finished. A job body for the target, and how to tell when it's done:

```json
{"user_id": "<tenant>", "idempotent_key": "<stable per request>", "url": "http://127.0.0.1:<backend port>/<path>",
 "method": "POST", "headers": {"Content-Type": "application/json"}, "body": "<the request body, as a string>", "max_retries": -1}
```

`POST /jobs` answers `201` with a `job_id` (`200` with `"duplicate": true` for a key it already has). `GET /jobs/<job_id>` shows `status` going `queued` → `completed` (or `failed`). On a `429`, wait the `Retry-After` and resubmit the same key. The metrics a pilot reads most: `aquifer_jobs_total`, `aquifer_tenant_jobs_total` (with `AQUIFER_METRICS_TENANT_LABELS=true`), `aquifer_admission_rejected_total`, `aquifer_queue_wait_seconds`, `aquifer_backlog_jobs` and `aquifer_active_queues`.

## 1. Fit check

Ask what sits behind their bottleneck, and say plainly if Aquifer isn't a fit.

- **Good fit:** capacity that is elastic but slow to come online or expensive to keep idle. Examples are GPUs and inference servers, rate-limited third-party APIs, CI runners, databases with bursty writers, and webhook or WebSocket floods.
- **Overkill:** a ceiling that is fixed for good, or traffic that never bursts past capacity.

While you're here, decide which pieces the design needs, and write the choice into the report:
- **Inlet** if the traffic is webhooks from providers. It verifies their signatures, which Aquifer doesn't, and pushes the events in with the provider's event ID as `idempotent_key`.
- **Canalis** if the pilot will run more than one Aquifer node behind one address. Use the [Canalis + Aquifer quick start](https://github.com/rjpruitt16/aquifer/blob/main/examples/canalis/README.md).
- **Neither** for a single node behind their own trusted service, which is the usual pilot. Don't add a piece the workload doesn't need.

## 2. Benchmark the backend first

Before Aquifer goes in front, find the backend's real limits. Ramp load against the replica until it degrades, and record:
- the knee (the rate where latency or errors turn up);
- the error rate past it;
- what saturation looks like: GPU utilization, KV-cache use, CPU, queue depth.

Everything after this is compared against these numbers, and the backend's pacing logic (step 3) needs them.

## 3. Teach the backend to set the pace

When you add headers to the backend, use the `X-Aqueduct-*` names below. They belong to the protocol, not to Aquifer, so the backend isn't tied to one implementation. Aquifer reads and sends every header in this section as `X-Aquifer-*` too, which is the form its README shows.

### What Aquifer tells the backend

Every request Aquifer sends carries these headers, so the backend knows what's coming (`X-Aquifer-*` aliases too):

| Header | Meaning |
|---|---|
| `X-Aqueduct-Flow-Rate` | This queue's current dispatch rate (rps) |
| `X-Aqueduct-Queue-Depth` | Jobs waiting on this whole node, across every queue and backend (not this queue's own) |
| `X-Aqueduct-Active-Queues` | Queues on this node currently sending to this backend |
| `X-Aqueduct-Upstream-Backlog` | Jobs on this node waiting for this backend, across all its queues |
| `X-Aqueduct-Total-Jobs` | All jobs on this node right now |

Log these during the pilot. They show how much work is waiting for the backend, not just what has arrived.

### Account queues

They're on by default. Each tenant (`user_id` plus API key) gets its own queue at each backend, paced on its own, so one tenant's burst or retry storm can't slow the others down. Most fairness results come from this.

The backend has the last say: `X-Aqueduct-Account-Queue: disabled` puts everyone in one shared queue, for backends that need one global order. Check which mode the pilot ran in and say so in the report.

### When you need more

Read these only when they apply:
- [references/pool.md](references/pool.md): the backend is several interchangeable instances (register them as a pool instead of putting a load balancer in front).
- [references/several-nodes.md](references/several-nodes.md): more than one Aquifer node, and how that multiplies the rate the backend sees.
- [references/control-loop.md](references/control-loop.md): the backend will send pacing headers. It covers AIMD, which saturation signal to use per backend type, sizing the backlog budget in seconds, and priority tenants.

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
