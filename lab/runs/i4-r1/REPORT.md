## Aquifer pilot: orders-api, 2026-11-04 (local only, hands-off)

**Short answer:** Aquifer fixes the problem you described. With Aquifer in front of the database writer, the small shops' orders went from about 8% failing with a 34 s p99 to none failing with a p99 of about 0.1 s. Every one of bigbox's orders also succeeded, where about 8% failed before. It doesn't add database capacity. Bigbox waits longer than before (p99 34 s became 40 s) and total writes per second dropped about 4%. If the sale's peak lasts longer than the queue's backlog budget, bigbox will start getting 429s.

**Backend:** `app.py` + `db.py` run unchanged: a 4-connection pool, about 52 ms per insert, and a 2 s wait for a free connection before it answers 503.
- Benchmark (one caller, no retries, 12 s per step): flat 55 ms latency up to 70 rps. At 75 rps p99 was 98 ms. At 80 rps p50 was 0.47 s. At 90 rps 3.4% failed with 503 and p50 was 1.3 s. At 110 rps 21% failed.
- **The knee is about 75 rps.** The hard ceiling is about 77 rps, which is 4 connections ÷ 52 ms.

**Traffic:** `partners.py`'s sale schedule: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s, a 30 s burst, 5,100 orders per pass.
- The baseline copies the partners' client exactly: 5 attempts 0.1 s apart, 5 s timeout, 400 workers.
- My generator keeps to the schedule exactly. Your `partners.py` drifts and only sent 3,759 bigbox orders, but it showed the same failure pattern.
- Each pass stayed under 300 rps and 2 minutes. Peak at the backend was 225 req/s on the baseline and 77 through Aquifer. Peak submit calls to Aquifer were 248/s.

**Shape:** one Aquifer node, no Canalis, no Inlet. Account queues on (the default), one per partner `user_id`. Built from the checkout, `v0.15.1-63-g4d4345e`, which prints `aquifer dev`. The skill is pinned to 0.15.1; you told me not to use GitHub, so I didn't fetch a release-specific copy. Pacing signal: the static YAML ceiling. A header-driven pass was tested and lost.

### Results (2 runs each where shown; numbers are ranges or the mean)

| | Baseline (direct) | Aquifer, ceiling 70 (×2) | Aquifer, ceiling 80 (×2) |
|---|---|---|---|
| Time database pool saturated (requests waiting for a connection) | 99.6% | 21–23% (p99 wait 43 ms) | 25–28% (p99 wait 45 ms) |
| Orders lost | 7.4–8.2% (378–420 of 5,100) | 0 | 0 |
| Small shops' orders lost | 7.8–9.0% | 0 | 0 |
| 503s at backend / retries sent | 59% of requests / 6,730–6,790 retries | 0 / 0 | 0 / 0 |
| Small shops p50 / p99 end to end | ~16.3 s / ~34.5 s | 0.07 s / 0.10–0.15 s | 0.07 s / 0.10 s |
| Bigbox p50 / p99 | 16.3 s / 34 s | 25.5 s / 44 s | 24 s / 40 s |
| Successful writes/s while busy | 75.7 | 68.4 | 72.6 |
| Time to finish after the burst ends | 32 s, with ~400 orders lost | 44.5 s | 40 s |
| Worst small-shop p99 / Jain index (completed ÷ sent per tenant) | 34.6 s / 1.0 (all tenants equally bad) | 0.15 s / 1.0 | 0.10 s / 1.0 |

Baseline latency is measured from when the order was scheduled, so it includes time queued in the partners' 400-worker client. Measured from the first attempt, as `partners.py` reports it, the baseline was p50 4.2 s and p99 10.5 s.

Aquifer's own `aquifer_fairness_jain` read 0.46–0.69 during the burst. That gap comes from demand (150/s against 5/s), not from unfairness: the small queues were drained as fast as they arrived.

**Two more passes:**
- **AIMD pacing headers, same ceiling of 80.** The backend advertised `X-Aqueduct-Rps` = budget ÷ `X-Aqueduct-Active-Queues`, cut on pool wait. This was worse: 66.9 writes/s, bigbox p50 29.7 s, 46 s to finish. The database never reached the cut threshold, so the only effect was the even split, which capped bigbox whenever a small shop's queue was briefly active. This is the failure mode the skill warns about. Use the static ceiling.
- **Simulated long sale, with the backlog budget shrunk to 1,500 (`AQUIFER_MAX_PENDING_PER_UPSTREAM=1500`).** Bigbox alone got 1,413 refusals (`queue_fair_share` 683, `upstream_queue` 730, each with `Retry-After: 5`). The small shops were never refused and still had a 0.10 s p99. Every order eventually completed.
  - **Finding that matters for your integration:** on the first try of this pass, the small shops' p99 was 16.8 s. That delay was entirely in my caller: bigbox submissions sleeping out `Retry-After` filled a worker pool shared by all partners. Inside Aquifer the small shops' p99 was 0.095 s. With one submit pool per partner it dropped back to 0.10 s.
  - So whatever submits to Aquifer must not make every partner wait behind bigbox's `Retry-After`. Either give each partner its own worker pool or pass the 429 back to the partner.

**Cost:** Aquifer adds no capacity. Your 4-connection database plan tops out at about 75 orders/s against 170/s of sale demand. What it saves:
- About 6,700 wasted 503 attempts per 30 s burst (59% of what reached the database), each holding a server thread for 2 s.
- The roughly 8% of orders that were lost.

Shortening bigbox's wait needs a bigger database plan or batched inserts.

### What didn't improve
- **Bigbox waits longer** (p99 34 s became 40 s at ceiling 80), even though all of its orders now succeed.
- **Throughput dropped about 4%** (75.7 to 72.6 writes/s). The ceiling sits just below the knee on purpose; ceiling 80 is the better setting of the two.
- **A long sale outgrows the default budget.** At 170 in and 75 out, the backlog grows about 95/s. The default budget of 10,000 fills about 105 s into the peak, and after that bigbox gets 429s. I only tested that with the scaled-down budget, because a full-size test would break the 2-minute rule.
- **Partners lose their immediate answer.** Today they get a synchronous `201 {order_id}`. Through `POST /jobs` they'd get "accepted", with the result arriving later (I used webhooks). `POST /proxy` can keep the call synchronous, but I didn't test it, and bigbox's 40 s waits would exceed their 5 s timeout.
- **Duplicate orders are possible.** Delivery is at-least-once, and `save_order` has no idempotency. None appeared here (5,100 writes for 5,100 orders in every Aquifer pass), but before production give the orders table a unique key matching the `idempotent_key`.
- **Partners must not call Aquifer directly.** Aquifer listens on every interface and must not face untrusted callers. Your own orders-api intake should submit to it.
- **A metric looks wrong.** `aquifer_flow_rate_rps` read 114–140 with several queues active, above the 70 rps ceiling. The backend never saw more than 77 req/s. Either the metric or the docs line saying "the sum stays within it" needs fixing.
- **Pilot limits.**
  - Bursts were 30 s on a laptop, not the hours to a day the skill asks for.
  - There are 2 runs per main configuration and only 1 for the pacing and budget passes.

### Config
- **YAML:**
  - `127.0.0.1:19401`: `rps: 70` (`pilot/aquifer.yml`) or `80` (`pilot/aquifer-80.yml`), `max_concurrent: 4` per tenant queue, so one tenant alone can use the whole pool.
  - Webhook collector at `127.0.0.1:19402`: `rps: 280`, `max_concurrent: 16`.
- **Env:** `AQUIFER_METRICS_TENANT_LABELS=true`. Only the budget pass set `AQUIFER_MAX_PENDING_PER_UPSTREAM=1500`.
- **Defaults I chose without asking:**
  - Jobs: `max_retries: -1`, no `execute_before`, `idempotent_key = run-partner-n`, `user_id = partner name`.
  - Completion detected by `webhook_url`, so the caller doesn't poll.
  - Everything else left at Aquifer's defaults: Retry-After 5 s, flush interval 100 ms, inbound pacing, 10,000 backlog budget.
- **Ports:** Aquifer 19400, orders-api copy 19401, collector 19402.
- **Your code:** I didn't modify `app.py`, `db.py` or `partners.py`. `pilot/backend.py` imports them and adds request and header logging, a pool-stats sampler, and the optional AIMD. The directory isn't a git repo, so the pilot lives in `pilot/` instead of on a branch.

### Raw data
Everything is in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i4-r1/pilot/`:
- **Scripts:** `pass.sh` (adapted from the skill's), `load.py`, `backend.py`, `analyze.py`.
- **Per pass, in `runs/<pass>/`:**
  - `jobs.jsonl`: one line per order.
  - `jobs-summary.json`.
  - `jobs-rejections.jsonl`: each 429 with `limit_reason`, `limit` and `current`.
  - `backend.jsonl`: every request at the database writer, with its `X-Aqueduct-*` headers.
  - `backend-pool.jsonl`: pool use sampled at 4 Hz.
  - Prometheus scrapes every 5 s, `metrics-final.prom`, `health.jsonl` and `aquifer.log`.
- **Benchmark:** `runs/bench/`.
- **Your unmodified `partners.py` run:** `runs/partners-orig/`.

I deleted Aquifer's data directories (about 20 MB per pass).

**Cleanup:** every process I started (Aquifer, orders-api copies, load generators, metrics scrapers) has been stopped, and nothing is listening on ports 19400–19409. The aq-lab checkout is unmodified (`git status` clean). The only thing written outside i4-r1 was the Go build cache.