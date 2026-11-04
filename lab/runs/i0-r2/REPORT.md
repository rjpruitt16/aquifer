## Aquifer pilot: orders-api, 2026-11-04

**Short answer:** Aquifer fixes the problem you described. With Aquifer in front of the database writer, the small shops' orders all went through, with a p99 of 0.09 s. Before, about 9% of their orders failed and the ones that succeeded had a p99 of about 23 s. No order from any partner failed, and the database stopped getting retry storms.

It costs two things:
- **bigbox waits longer.** Its p99 went from about 23 s to about 31 s.
- **Peak write rate drops about 10%.** The database committed 63 orders/s during the burst, against 70/s in the baseline, so the backlog took about 10 s longer to clear.

Aquifer adds no capacity. The database pool is the ceiling either way. The real adoption cost is an API change, covered under "What didn't improve".

**Backend:** orders-api (`app.py`, Python `ThreadingHTTPServer`) over a simulated managed Postgres with a pool of 4 connections, 50 ms per insert and a 2 s checkout timeout. In theory that's at most 80 writes/s. The benchmarked knee is **about 68–70 orders/s**: p99 stays at 71 ms at 68 rps offered, rises to 814 ms at 76 rps, and at 90 rps 12% of requests get `503`. Goodput tops out at about 79/s, then falls to 70/s at 106 rps offered (table below).

**Traffic:** the sale-day shape modelled on `partners.py`: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s, for 30 s. It isn't replayed production traffic. Each partner's sleep-based scheduler actually achieves about 133 orders/s in total, with about 4,000 orders per run, 3,400 of them bigbox. `partners.py` itself behaves the same way, so the load generator keeps that behaviour. The baseline uses the partners' real retry policy: up to 5 attempts, 0.1 s apart. 4 tenants.

**Shape:**
- Aquifer alone: 1 node, no Canalis, no Inlet. The orders come from your own partner integration, not from signed provider webhooks, and there is only one node.
- Account queues on (the default). Each partner is a `user_id`.
- Aquifer v0.15.1, built from `aq-lab` at commit 9423cbe.
- Pacing signal: database pool checkout wait (p95 over 0.5 s, plus checkout timeouts), fed into an AIMD loop in the backend.

### Results

The baseline numbers are the mean of two runs. The Aquifer column is the recommended config (`aquifer-v6-even-ceiling100`). The same design with a ceiling of 80 ran twice and landed within 1 s of these numbers (see "All runs").

| | Baseline (direct, retries) | Through Aquifer |
|---|---|---|
| Time saturated (seconds with requests waiting for a DB connection) | 100% of the burst+drain, up to 390 waiting | 21% of seconds, at most 3 waiting |
| DB pool busy | 98.5% | 91% |
| 503 / 429 rate at the backend | **59%** of requests (503) | **0%** |
| Orders that failed for good | **332 per burst (8.3%)**, every tenant | **0** |
| Retries sent to the DB | ~5,280 per 30 s burst (1.3 per order) | 0 |
| p50 / p99 end to end, all orders | 10.7 s / 22.9 s | 19.3 s / 31.0 s |
| Small shops: success / p99 | 91.4% / 22.9 s | **100% / 0.09 s** |
| bigbox: success / p99 | 91.4% / 22.9 s | 100% / 31.0 s |
| Commits per second during the burst | 70.6 | 62.8 |
| Jobs per hour at peak (commits/s × 3600) | ~254k | ~226k |
| Burst drain time (last commit after the burst ended) | 21.5 s, with 8% of orders dropped | 31.2 s, with every order done |
| Worst tenant's p99 / fairness | 23.9 s / every tenant suffered equally | 31.0 s (bigbox) / small shops untouched |
| Estimated cost | same database plan | same plan plus one small Aquifer process; no extra DB capacity needed |

End to end means from when the partner's scheduler sends the order to its first successful database commit. That includes waiting in Aquifer. In the baseline it also includes waiting in the partner's own 400-thread client, which `partners.py` leaves out of the latency it prints.

On fairness: Jain's index over each tenant's completion ratio is 1.0 in both passes, because both treat tenants the same in aggregate. The difference is what each tenant gets:
- **Baseline:** everyone gets an equally bad result.
- **Aquifer:** the light tenants are unaffected and the heavy tenant absorbs the queueing.

`aquifer_fairness_jain` flipped between 1 and 0.5 across scrapes. It only counts queues that have work waiting, so with one heavy tenant and three light ones it isn't a useful number here.

Aquifer's own intake was never the bottleneck:
- It accepted every job at up to 138 jobs/s, with a p99 of 6 ms to accept.
- It refused nothing with `429`.
- Its backlog peaked at about 2,200 jobs, well under the default budget of 10,000.

### What didn't improve

- **Throughput went down about 10%, not up.** The baseline keeps the pool 98–99% busy, with 390 threads queued for a connection. Rate-based pacing leaves small gaps.
  - Burst commits were 59–63/s through Aquifer against 70–71/s directly.
  - Raising the ceiling from 80 to 100 (v6) recovered about 3/s.
  - The rest comes from Aquifer's dispatch jitter and the per-queue concurrency cap. Four in flight at about 57 ms each is about 70/s for one queue.
- **bigbox is slower.** Its p50 went from about 11 s to about 21 s, and its p99 from about 23 s to about 31 s. That's the cost of fair turns: the small shops' share now comes out of bigbox's queue.
  - In the baseline, bigbox also lost about 290 orders per burst. Through Aquifer it lost none.
- **The partner API changes.** Today, partners `POST /orders` and get `201 {order_id}` on the same connection. Aquifer must not be exposed to them (it's an SSRF relay, so it belongs on a private network). The production shape would be:
  1. A thin public `/orders` accepts the order.
  2. It submits a job to Aquifer with `user_id = partner` and `idempotent_key = partner's order id`.
  3. It returns `202` with a reference.
  4. The partner gets the `order_id` later, by webhook (`webhook_url`) or by polling.

  `POST /proxy` keeps the response synchronous while the database is healthy. Under overload, though, it switches the connection to an SSE stream, which the partners' current clients wouldn't parse. **This contract change is the biggest real cost of adopting Aquifer. The pilot didn't build or test it:** the load generator called Aquifer directly, standing in for that thin front.
- **Idempotency:** Aquifer delivers at least once. `save_order` has no idempotency key, so a redelivery after a crash would create a duplicate order. There were no duplicate commits in these runs, but no crash was tested either. Add a unique key (partner + partner order id) to the insert before production.
- **Durability:** jobs live on the Aquifer node's local disk, and up to about 100 ms of accepted jobs can be lost in a crash. The partner (or the thin front) should keep its order until it gets a result, and resubmit with the same `idempotent_key` if none arrives. A `200` with `"duplicate": true` means Aquifer still has the job.
- **Longer sales:** with the default backlog budget of 10,000 jobs, a burst at about 135/s with about 63/s of drain fills the budget in about 140 s. Past that, bigbox starts getting `429` at intake; that budget limit wasn't tested here. For a sale longer than about 2 minutes, have the backend advertise `X-Aqueduct-Max-Backlog`, sized as peak arrival rate × the longest burst to absorb. For example, 150/s × 600 s = 90,000. Check disk use at that size.
- **The water-filling split I wrote first did no better than the skill's simple default** (backend advertises `budget ÷ X-Aqueduct-Active-Queues`). Under the simple default, the small shops' p99 was even lower (0.09 s against 1.4 s). Use the simple version.

### Findings about Aquifer itself (v0.15.1)

1. **`X-Aqueduct-Queue-Depth` is the node-wide queued count, not this queue's.** The pilot skill's table says "This queue's jobs still waiting". In this version the value comes from `store.Counts().QueueDepth` (`internal/dispatch/account_queue.go:661`). kiosk's requests arrived with `Queue-Depth: 1,600+` while kiosk had almost nothing queued.
   - Effect on the pilot: my first per-tenant split (v1) relied on this header, so it treated every queue as backlogged. Burst throughput fell to 29/s.
   - Workaround: classify queues by their usage against the rate last advertised to them.
   - This is worth an issue on the Aquifer repo, as either a doc fix or a per-queue header.
2. `aquifer_flow_rate_rps` (the sum of every queue's advertised rate) reached 187 with a ceiling of 80. With the even split, each light queue is told `budget ÷ active` and doesn't use it. It did no harm, because those queues are limited by their own demand, but don't read that gauge as the load on the target.
3. When load starts, each new account queue fires at the configured ceiling before its first response teaches it the backend's rate. The backend's first AIMD tick saw a pool-wait spike and cut. Starting the AIMD at the benchmarked knee (70), rather than 40, removed the slow climb that followed (v3 to v4).

### All runs

Every Aquifer run is 30 s of burst followed by its drain. Load plus drain stayed under 2 minutes each, and every HTTP request was capped at 300/s by a token bucket that never engaged. The heaviest load on the backend was 201 req/s, in the baseline. Between runs, one thing changed, except where noted.

| pass | orders | done | failed | backend 503/429 % | retries to DB | commits/s in burst | drain after burst s | p50 s | p99 s | small shops ok % | small shops p99 s | bigbox p99 s | pool busy % | time with waiters % |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 3982 | 3644 | 338 | 59.5 | 5362 | 70.2 | 21.8 | 10.92 | 23.33 | 92.0 | 23.31 | 23.33 | 98.1 | 100.0 |
| baseline-2 | 3958 | 3633 | 325 | 58.9 | 5198 | 71.0 | 21.1 | 10.49 | 22.5 | 90.8 | 22.47 | 22.5 | 99.0 | 100.0 |
| aquifer-v1-waterfill-buggy | 3971 | 3971 | 0 | 0.0 | 0 | 29.1 | 51.8 | 39.77 | 51.6 | 100.0 | 0.49 | 51.64 | 64.2 | 0.0 |
| aquifer-v2-waterfill-uncapped | 4023 | 4023 | 0 | 0.0 | 0 | 63.8 | 31.1 | 18.67 | 30.91 | 100.0 | 1.41 | 30.94 | 92.2 | 26.2 |
| aquifer-v3-waterfill-30ms | 4076 | 4076 | 0 | 0.0 | 0 | 48.9 | 38.6 | 26.23 | 38.4 | 100.0 | 1.4 | 38.43 | 81.2 | 2.9 |
| aquifer-v4-waterfill-start70 | 4047 | 4047 | 0 | 0.0 | 0 | 51.4 | 37.4 | 25.23 | 37.19 | 100.0 | 1.39 | 37.22 | 83.2 | 6.0 |
| aquifer-v5-waterfill-start70-100ms | 4012 | 4012 | 0 | 0.0 | 0 | 61.2 | 32.5 | 20.38 | 32.25 | 100.0 | 1.44 | 32.29 | 89.1 | 16.1 |
| aquifer-v5-even-start70-100ms | 3973 | 3973 | 0 | 0.0 | 0 | 59.0 | 32.7 | 20.85 | 32.48 | 100.0 | 0.09 | 32.52 | 88.3 | 11.3 |
| aquifer-v5-even-start70-100ms-2 | 4012 | 4012 | 0 | 0.0 | 0 | 59.6 | 32.8 | 20.77 | 32.51 | 100.0 | 0.08 | 32.55 | 85.9 | 11.3 |
| **aquifer-v6-even-ceiling100** | 4011 | 4011 | 0 | 0.0 | 0 | 62.8 | 31.2 | 19.31 | 30.97 | 100.0 | 0.09 | 31.0 | 91.0 | 21.3 |

What changed at each step:
- **v1:** water-fill keyed on `Queue-Depth`. Broken by finding 1.
- **v2:** two changes. The budget now grows when `X-Aqueduct-Upstream-Backlog` exceeds it, and queues are classified by usage. A bug let a reserve grow past the budget, so the backend was effectively unpaced and v2's throughput is accidental.
- **v3:** reserves capped at the budget.
- **v4:** AIMD starts at the knee (70) instead of 40.
- **v5:** cut threshold on p95 pool wait raised from 30 ms to 100 ms. The p95 sits at 40–50 ms when the pool is healthy and nearly full, so 30 ms cut it at about 60/s.
- **v5-even:** the split changed to the skill's default, `budget ÷ Active-Queues`. Run twice.
- **v6:** Aquifer's ceiling and the AIMD ceiling raised from 80 to 100. Those are two settings, but one decision: let the wait signal, not a fixed number, find the top.

v6 was run once. v5-even, which differs from v6 only in the ceiling, repeated within 0.6/s. The ceiling-100 gain (+3/s) is close to the run-to-run spread, so treat it as small.

Backend benchmark (open-loop, single attempt, no retries, about 10 s per step):

| nominal | offered rps | ok rps | 503 % | timeout % | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|---|---|
| 40 | 31.7 | 31.7 | 0.0 | 0.0 | 59 | 62 | 62 |
| 60 | 45.0 | 45.0 | 0.0 | 0.0 | 59 | 61 | 62 |
| 70 | 53.2 | 53.2 | 0.0 | 0.0 | 59 | 61 | 62 |
| 80 | 60.4 | 60.4 | 0.0 | 0.0 | 59 | 61 | 62 |
| 90 | 68.0 | 68.0 | 0.0 | 0.0 | 60 | 67 | 71 |
| 100 | 75.8 | 75.8 | 0.0 | 0.0 | 413 | 774 | 814 |
| 120 | 90.2 | 79.5 | 11.9 | 0.0 | 2008 | 2062 | 2068 |
| 140 | 106.5 | 70.5 | 33.8 | 0.0 | 2050 | 2065 | 2068 |

### Config

Code is on branch `aquifer-pilot` in a git repo I initialized in this directory. `main` holds orders-api exactly as I found it. Everything I added to the service is off by default, so `python3 app.py --port 9000` still behaves the way it did.

**Backend (`app.py`), with `--pace`:**
- Every response carries `X-Aqueduct-Rps: <budget ÷ X-Aqueduct-Active-Queues>` (with `--pace-split even`) and `X-Aqueduct-Max-Concurrent: 4` (the pool size).
- The AIMD runs on a 0.5 s tick.
  - Cut ×0.7 when there's a checkout timeout or the p95 checkout wait goes over `--pace-wait-ms` (100). It cuts at most once a second.
  - Grow by +4 below the last trouble point and +1 above it, when the budget is at least 80% used or Aquifer's `Upstream-Backlog` is larger than the budget.
  - Start at `--pace-start 70`, never go below 5 and never above `--pace-ceiling 100`.
- A checkout timeout returns `429` with `Retry-After: 1` instead of `503`. None happened under Aquifer.
- `--log` writes one JSON line per request (with every `X-Aqueduct-*` header received), a sample every 1 s and a pacer tick every 0.5 s.

**Aquifer:**
- `pilot/aquifer-ceiling100.yml`: `"127.0.0.1:19200": {rps: 100, max_concurrent: 4}`. Earlier runs used `pilot/aquifer.yml` with rps 80.
- Environment: `PORT=19201`, `DB_PATH` and `L8_KEY_PATH` under each run directory, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_MEMORY_LIMIT_MB=400`. Everything else was left at its default, including Pebble's 100 ms flush, `AQUIFER_MAX_PENDING_PER_UPSTREAM=10000` and `AQUIFER_RETRY_AFTER_SECONDS=5`.

**Callers** (`pilot/loadgen.py aquifer`):
- `POST /jobs` with `user_id=<partner>`, `idempotent_key=<run>-<partner>-<n>` and `max_retries: -1`, with no `execute_before`.
- On a `429`, wait `Retry-After` and resubmit the same key, with no backoff stacked on top. There were no `429`s.
- Give up 100 s after the start. That never happened.

**Defaults I chose without asking:**
- Single node, account queues on.
- The pacing signal is pool checkout wait. That's the database row of the skill's table, with checkout wait standing in for query latency.
- A 30 ms and then a 100 ms threshold. The 100 ms value came from the benchmark: a healthy, nearly full pool has a p95 of 40–50 ms.
- Metrics scraped every 5 s, not 15–30 s, because the runs were short.
- Results were checked through the backend's commit log, not `GET /jobs/:id`. Aquifer's `aquifer_jobs_total{outcome="completed"}` matched the commit count on every run.

**Departures from the pilot skill, and why:**
- **Runs lasted about a minute, not hours.** Your limit was under 2 minutes per load test. Each run covers one sale burst, so this pilot says nothing about long-run behaviour, disk growth or several bursts back to back.
- **Everything ran on this laptop, with simulated traffic and a simulated database.** No production systems were touched.
- **Another agent's lab (`lab-runs/i0-r3`, port 19300) was running on the same machine during the later runs.** I didn't touch it. CPU contention may have added noise, but the repeated runs agree within about 1/s.
- **New listening processes on this machine took 0–35 s before they accepted connections.** That looks like local security software. Every run therefore waits for `/health` and `/ready` before starting load. The first benchmark attempt, which didn't wait, was discarded.
- **I didn't read `aq-lab/lab/`** (`PROMPT.md`, `grade.py`). It's the repo's onboarding lab and grader, not part of the pilot skill.

### Raw data

All of it is in `pilot/run/<pass>/`:
- `loadgen.jsonl`: every order, with its scheduled, accepted and finished times, attempts and any refusals.
- `backend.jsonl`: every request at the database writer, plus 1 s samples and the pacer's ticks.
- `metrics.txt` and `health.jsonl`: Aquifer `/metrics` and `/health` every 5 s.
- `metrics_final.txt`, `aquifer.log` and `summary.json`.
- The benchmark is in `pilot/run/bench/`.

To reproduce:
- `pilot/run_aquifer_pass.sh <name> --pace --pace-split even --pace-wait-ms 100 --pace-start 70 --pace-ceiling 100`, with `AQ_CONFIG=pilot/aquifer-ceiling100.yml`.
- `python3 pilot/analyze.py pilot/run/<name>` summarizes one pass, and `pilot/table.py` builds the table.

The Aquifer binary was built to `pilot/bin/aquifer` (git-ignored). Nothing in `aq-lab` was modified, and every process I started has been stopped.

### Recommendation

Adopt it if your partners can accept asynchronous order confirmation, by webhook or by polling with an order reference:
1. Put Aquifer between a thin public `/orders` front and the database writer.
2. Keep the pacing headers from this branch (even split, start at about 70, cut on a 100 ms pool wait).
3. Make the insert idempotent on the partner's order id.
4. Size `X-Aqueduct-Max-Backlog` for your longest sale.

If partners can't change their integration, Aquifer can't sit in front of them as it stands; it needs that front to change first. Then the nearest alternative is inside orders-api itself: a per-partner concurrency limit, with `429` and `Retry-After` instead of `503` with immediate retries. That keeps small shops' orders going through, but bigbox's excess is still refused rather than held, so its retry pressure stays on your side.

If the drain time matters more than fairness, only a bigger database plan helps: more connections, or faster inserts. Aquifer can't make the database faster.
