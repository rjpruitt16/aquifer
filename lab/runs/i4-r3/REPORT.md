## Aquifer pilot: orders-api, 2026-11-04

I ran the pilot hands-off on this laptop, following the `aquifer-pilot` skill in the checkout (`skills/aquifer-pilot/SKILL.md`).

**Bottom line:** Aquifer fixes the problem you described. With Aquifer in front, every order completed, including all of the small shops' orders. Without it, the small shops lost 44 of 586 orders (7.5%). The cost lands on bigbox: its orders still all go through, but they wait longer. Two things need work before production:
- **Partner contract:** partners would get "accepted" right away and their `order_id` later, instead of `201 + order_id` on the spot.
- **Duplicate orders:** `save_order` is not idempotent, so a redelivered job would write the order twice.

**Backend:** orders-api (`app.py`) with a 4-connection DB pool. Each write takes about 55 ms, and a request that can't get a connection within 2 s fails with `503`.
- Benchmark (`pilot/bench.py`, one attempt per request, 12 s per step): no errors and flat 55 ms latency up to 70 rps.
- At 80 rps there were still no errors, but p50 jumped to 473 ms and 62 requests were waiting for a connection.
- At 90 rps, 3% failed with `503`. At 110 rps, 21% failed and goodput topped out around 73–80 rps.
- **Knee: about 72–80 rps.** I set the ceiling at 65 rps.

**Traffic:** your partners.py sale burst, modelled exactly. It runs 30 s with 4 tenants: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s. Python's sleep timing under-delivers, so each pass got about 4,300–4,450 orders.
- Baseline: the same client behaviour as partners.py (5 attempts, 0.1 s apart, 5 s timeout, a shared pool of 400 workers).
- Cross-check: running your unmodified `partners.py` gave matching failure counts (8–11% of small-shop orders failed).
- Load stayed under 300 rps (peak 219 attempts/s in the baseline). Every pass finished in under 82 s.

**Shape:**
- One Aquifer node; no Canalis or Inlet. Partner orders aren't provider webhooks, so Inlet's signature checking doesn't apply.
- Account queues on (the default); the backend never overrode them.
- Aquifer built from the checkout, which is `v0.15.1-62-g01c3f59`. The skill is pinned to v0.15.1. Your rules ruled out GitHub, so I used the skill copy in the checkout.
- Ports: backend 19600, Aquifer 19601, webhook receiver 19602.

| | Baseline (direct) | Aquifer, static ceiling | Aquifer + backend headers, 10 ms SLO | Aquifer + headers, 30 ms SLO |
|---|---|---|---|---|
| Orders completed | 3,982 / 4,305 (92.5%) | 4,450 / 4,450 | 4,411 / 4,411 | 4,453 / 4,453 |
| Small-shop orders failed | 44 / 586 (7.5%) | 0 | 0 | 0 |
| Seconds the DB pool had waiters | 52 of 52 | 9 of 67 | 0 of 80 | 2 of 74 |
| Peak attempts/s at the DB | 219 | 75.7 | 64.7 | 67.7 |
| `503`s / `429`s | 5,472 `503` | 0 / 0 | 0 / 0 | 0 / 0 |
| Retries sent | about 5,150 | 0 | 0 | 0 |
| p50 / p99, all orders | 11.4 s / 24.7 s | 18.0 s / 37.4 s | 30.5 s / 50.0 s | 24.5 s / 44.1 s |
| Small shops p50 / p99 | 11–12.7 s / 23–26 s | 0.07 s / 0.09–0.10 s | 0.06 s / 0.07–0.26 s | 0.06 s / 0.07–0.08 s |
| bigbox p50 / p99 | 11.4 s / 24.7 s | 20.6 s / 37.5 s | 33.2 s / 50.0 s | 27.2 s / 44.1 s |
| Completions/s (per hour) | 74.7 (269k), with 7.5% dropped | 65.6 (236k) | 54.9 (198k) | 59.9 (215k) |
| Drain time after the burst ended | 23 s, with orders dropped | 38 s | 50 s | 44 s |
| Peak backlog held in Aquifer | none | 2,257 | 2,918 | 2,644 |

Latency is measured from the moment each order was due. The baseline numbers include time spent waiting for a client worker. That's why my baseline p50 is 11 s while partners.py prints about 4 s: partners.py starts its clock only when a worker picks the order up.

**Fairness:** In the baseline every tenant failed at about the same rate (Jain's index over success rate = 1.0). So the baseline was "fair" only in that everyone lost orders, small shops included. Through Aquifer:
- Each tenant got its own queue, so the small shops never waited behind bigbox's backlog.
- Their p99 fell from about 25 s to under 0.3 s.
- `aquifer_fairness_jain` dipped to 0.45–0.7 during the burst. That gauge counts jobs finished per queue, so it reflects bigbox's much larger demand, not starvation.

**Cost:** The 4-connection plan absorbed the sale without dropping orders, so a bigger DB plan isn't needed for bursts this size. About 5,150 wasted retries also go away; each of those held a server thread for up to 2 s. I didn't put a dollar figure on any of this.

### Finding: the YAML `rps` is a per-tenant limit in this build, not a total
The skill says a host's `rps` is the total across tenant queues, scaled down proportionally when several are active. I didn't see that.
- **Burst pass:** with `rps: 65` and four tenants active, the DB got 71–76 rps for the whole 30 s burst, and `aquifer_flow_rate_rps` read 130 and 181.
- **Dedicated check:** 3 tenants at 40/s each, with `rps: 30`. `aquifer_flow_rate_rps` read 90 (3 × 30), and the DB got about 70–76 rps (`pilot/runs/ceiling-check*`).

So the static pass was protected by the DB's own speed and by `max_concurrent` (4 per queue, 16 in flight), not by the rps ceiling. That pass also had the most seconds with pool waiters. This is why I added the header pass. The backend now enforces the total itself, and it held the DB at or under 65–68 rps.

This looks like a docs or code mismatch worth raising on the Aquifer repo.

### What didn't improve
- **bigbox waits longer.** Its p99 goes from 25 s to 37–50 s, and the burst takes 38–50 s to drain instead of 23 s. In exchange, none of its 270–280 orders fail. The backend physically can't do more than about 73 rps, so its backlog has to wait somewhere.
- **The header passes do less work per second than the static one.** That's because they hold the DB under its knee instead of at it. The 30 ms setting is the best balance I measured (2 s of pool waiters, 60 jobs/s). I only did one tuning step.
- **The partner contract changes.** Today partners get `201` with an `order_id`. Through Aquifer they'd get "queued", with the `order_id` coming later from the job result or a webhook to you.
  - Aquifer must not be exposed to partners directly: the docs warn about SSRF, because a caller who can set the job's `url` can make Aquifer request anything it can reach.
  - orders-api would need a thin intake endpoint that validates the order and submits the job with a fixed `url`. In this pilot the load generator played that role; I didn't build it.
- **Duplicate orders.** Delivery is at-least-once, and `db.save_order` isn't idempotent. It needs a unique key on (partner, partner order number), which should also be the `idempotent_key` (I used `partner-n`). Orders are money, so your DB stays the record of truth.
- **Crash window.** A crash can lose jobs accepted in the last ~100 ms. The fix is for the intake to resubmit by the same key, not `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0`.
- **A simpler alternative exists.** A per-partner rate limit inside orders-api (send `429` to bigbox above its share) would also stop the small shops failing, with no new component. Aquifer's advantage is that it keeps bigbox's excess instead of rejecting it.

### Config
- **`pilot/aquifer-static.yml`:**
  - `127.0.0.1:19600` (orders-api): `rps: 65`, `max_concurrent: 4`.
  - `127.0.0.1:19602` (webhook receiver): `rps: 250`, `max_concurrent: 20`.
  - I gave the receiver its own entry because webhooks use the same per-host pacing, and the default of `rps: 2` would have throttled completions. I didn't test that default.
- **Env:** `AQUIFER_METRICS_TENANT_LABELS=true`; everything else default. That includes the backlog budget, `AQUIFER_MAX_PENDING_PER_UPSTREAM=10000`.
- **Jobs:** `max_retries: -1`, `user_id` = partner, and a `webhook_url` for completion. On a `429` the load generator waits `Retry-After` and resubmits; none happened.
- **Backend headers (`pacing.py`):**
  - Off unless `PACING=on`; the 30 ms pass also set `PACING_WAIT_SLO_MS=30`. Headers are sent only on requests that carry `X-Aqueduct-Node-Id`, i.e. ones from Aquifer.
  - The saturation signal is p95 pool-checkout wait above the SLO (10 ms, or 30 ms). It's AIMD on a total budget (60 to start, 65 cap, +5/+1 per 0.5 s step, ×0.7 cut, hold 2 ticks after a cut).
  - Each queue is advertised `max(budget ÷ active queues, budget − other queues' recent usage)`, which keeps bigbox from being capped at an even share.
  - A pool timeout returns `429` with `Retry-After: 1` to Aquifer; it never fired. Direct callers still get `503`.
- **Changes in `db.py` and `app.py`:** `db.py` gets an `on_checkout` hook. In `app.py`, `/stats` also reports the pacing state.
- **Defaults I chose:** a 30 s burst instead of hours, to fit your 2-minute limit; the 65 rps ceiling; 4 concurrent per queue; metrics scraped every 5 s (from the skill's script).
- **Not tested:** `X-Aqueduct-Max-Backlog`. The peak backlog was 2,918, far below the 10,000 default. But at 170 orders/s, 10,000 holds only about 60 s of incoming orders. To ride out a DB outage of N seconds during a sale, set it to about 170 × N.

### Caveats
- These are 30 s bursts on a laptop, not hours on a replica, and the DB is your simulated pool.
- Other agents' load tests were running on this machine at the same time (on other ports). Treat the rates as indicative within ±10%.

### Raw data and code
- Everything is in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i4-r3`. I ran `git init` there (it wasn't a repo) and committed the original code on `main`; all pilot work is on the `aquifer-pilot` branch.
- `pilot/runs/<pass>-load/`: `jobs.jsonl` (per order), `backend-stats.jsonl` (DB pool sampled every second), `rejections.jsonl` and `summary.json`. The baseline pass is in `pilot/runs/baseline/`.
- `pilot/runs/<pass>/metrics-*.prom` and `health.json`: Aquifer's scrapes.
- `pilot/runs/aq-headers*-backend-headers.jsonl`: every `X-Aqueduct-*` header the backend received.
- `pilot/runs/bench/bench.jsonl`: the benchmark steps.
- `pilot/runs/baseline-partners-py.txt`: the unmodified partners.py cross-check.

Nothing in aq-lab was modified (`git status` is clean). Every process I started has been stopped, and nothing is listening on ports 19600–19609.