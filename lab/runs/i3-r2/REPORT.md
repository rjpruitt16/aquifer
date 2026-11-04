## Aquifer pilot: orders-api, 2026-11-04 (local, laptop)

**Bottom line:** Aquifer fixes the problem you described. With Aquifer in front of orders-api, the small shops' orders all went through in under 0.6 s at p99 while the sale was running. No order failed and orders-api returned no 503s. Bigbox gives something up for this: its orders all land, but they queue for a median of about 33 s and up to about 68 s, and the backlog takes about 57 s to clear after a 40 s sale. Your service can't write faster than about 75 orders/s, so no setting changes that, and bigbox alone sends 150/s during a sale.

I ran three trials of each pass. They agreed closely (fail rates within about 3 points, latencies within about 0.5 s), so the tables below show one representative run.

**Backend:** orders-api (`app.py` + `db.py`, unchanged) on 127.0.0.1:19500. The pool is 4 connections and each insert holds one for about 50 ms, so the ceiling is about 85 orders/s. In the benchmark, which sent load at fixed rates with no retries:
- 40–75/s: no errors, p99 under 91 ms.
- 80/s: p99 rises to 1.07 s. The knee is about 75/s.
- 90/s: 5.6% 503s.
- 120/s: 29% 503s, while successful orders stayed flat at about 85/s.

**Traffic:** modelled on your `partners.py`: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s, for 40 s, which is 6,804 orders per pass. The baseline keeps the partners' habit of retrying immediately, up to 5 attempts.

**Shape:** a single Aquifer node, built from source at v0.15.1 (the version the skill is pinned to), on :19501. Account queues were on (the default), so each partner got its own queue. Pacing came only from the static YAML ceiling, and orders-api sends no pacing headers. Neither Inlet nor Canalis was needed: this is one node, and the traffic isn't provider webhooks.

| | Baseline (direct) | Through Aquifer |
|---|---|---|
| Time saturated (seconds with a request waiting for a DB connection) | 100%; up to 392 waiting | 72–77%; never more than 3 waiting |
| 503 rate | 59.5% of attempts (about 9,200) | 0 |
| Orders that failed | 8.0–8.3% bigbox, 7–10.9% small shops | 0 |
| Retries sent to orders-api | about 8,600 (15,400 attempts for 6,804 orders) | 0 (6,804 attempts). 4 connection resets were retried with no duplicates |
| Small shops, p99 from order to write | 44.8–45.7 s (10.5 s counted from the first attempt) | 0.56–0.58 s |
| Bigbox p50 / p99 | 21.6 / 44.5 s (4.2 / 10.5 s from first attempt), with 8% failing | 33 / 56 s, with none failing |
| Orders written per second during the sale | 74.7 | 72.5 |
| Time to clear the backlog after the sale | about 43 s (the client's own queue of retries) | about 57 s (Aquifer's queue) |
| Small shops' orders done within 5 s | 9–10% | 100% |
| Jain's index over the share of each partner's orders done within 5 s | 0.99 (equally bad for everyone) | 0.78 (small shops 100%, bigbox 6%) |
| Peak requests per second at orders-api | about 220 | 76 |

**Extra pass, with one change:** a backlog budget of 1,500 (`AQUIFER_MAX_PENDING_PER_UPSTREAM=1500`). This previews a sale long enough to fill the default budget of 10,000, which at 98 orders/s of excess takes about 100 s of sale.
- Bigbox got about 4,600 `429`s (`upstream_queue` 3,125, `queue_fair_share` 1,458). The load generator waited the `Retry-After` and resubmitted each one.
- The small shops were never refused (0 `429`s).
- Nothing failed, and still no 503s.
- From the moment a small shop's order was first submitted, p99 was 0.1 s. Measured from when the order was scheduled it was 28.8 s. That delay comes from my test client, not from Aquifer: all four partners share one 400-thread load generator, and bigbox's waits on `Retry-After` used up its threads. Real partners run separate clients.

**What didn't improve:**
- **Throughput.** It's fixed at about 72–75 orders/s either way. Aquifer replaces bigbox's 8% failures with waits of 30–70 s; it doesn't make anything faster. If sales run longer than a few minutes at these rates, you need more database connections or faster inserts. Aquifer only keeps that from hurting the small shops.
- **Saturation.** The pool was still busy about 75% of the time, but with at most 3 requests waiting instead of 392. That's the intended result: the pool stays full without overflowing.
- **The API contract changes.** Today partners get `201` with an `order_id` straight away. Through Aquifer they get a `job_id` and the order is written later. You'd need a trusted intake layer in front: your public handler submits to Aquifer with `user_id` set to the partner and returns `202`, and partners poll the job or get a webhook. Aquifer must not be reachable by partners directly, because it can be told to call any URL.
- **Duplicate orders.** Aquifer delivers at least once, so `/orders` needs to dedupe on a key, for example partner plus the partner's order number. Make that key the `idempotent_key`. There were no duplicates in these runs, but nothing in the code prevents them today.
- **A simpler alternative.** Your real issue is that one tenant can starve the others on a fixed pool, so per-partner concurrency limits inside orders-api (a semaphore per partner) would likely solve most of it without a new service. Aquifer adds durable queuing, pacing and retries that cost orders-api nothing on top of that.

**Config:**
- `pilot/aquifer.yml`: `"127.0.0.1:19500": {rps: 72, max_concurrent: 4}`. The 72 is just under the 75/s knee, and 4 matches the pool size.
- Env vars: `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-node-1`. Everything else was left at its default, including account queues on, a backlog budget of 10,000 and `Retry-After` of 5 s.
- Jobs were submitted with `max_retries: -1`, `execute_before` set to now + 15 min, `user_id` set to the partner, and `idempotent_key` set to run-partner-order. The client waited `Retry-After` on each `429`. I added no backend pacing headers: the static ceiling caused no saturation, so the skill says not to.

**Limits of this pilot:**
- Your limits (local only, at most 300 req/s, each test under 2 min) meant 40 s bursts instead of the hours-long runs the skill asks for.
- The load generator ran on the laptop rather than an always-on machine.
- Metrics were scraped every 5 s instead of every 15–30 s, because the runs were short.
- The load generator caps its own requests at 300/s. It never hit that cap: the peak was about 225 req/s at orders-api in the baseline and 170 req/s of new submissions to Aquifer.
- Baseline latency counted from when an order was scheduled includes time it spent in the client's 400-thread queue, which `partners.py` has too. The table gives the figure from the first attempt alongside it.

**Things to know about Aquifer:**
1. **A queue's rate collapsed after a burst of connection errors and recovered slowly.** My first Aquifer run had a bug in my request-logging wrapper that broke keep-alive. Each resulting connection error halved bigbox's queue rate, down to the 0.5 req/s floor, and it took about 40–80 s to climb back at 5% per success. Throughput fell to about 17 orders/s. The small shops' queues were unaffected. I discarded that run (kept in `pilot/data/aq-0-instrumentation-bug`), fixed the bug and reran. The behaviour itself is real: a few transient errors can stall one partner's queue for a long time.
2. **`aquifer_fairness_jain` read about 0.46 during the sale, even though every partner got what it asked for.** The gauge counts raw completions per queue without adjusting for how much each partner sent, so uneven demand looks unfair. I computed fairness from the per-order logs instead.

**Raw data:** `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i3-r2/pilot/data/`
- `bench-ramp.jsonl`: the benchmark.
- One folder per pass (`base-1..3`, `aq-1..3`, `aq-budget1500`), each with:
  - `lg-jobs.jsonl`: one line per order.
  - `backend.jsonl`: one line per request orders-api received, with the `X-Aqueduct-*` headers.
  - `lg-stats.jsonl`: orders-api `/stats` once a second.
  - `lg-metrics.txt` and `lg-health.txt`: Aquifer scrapes every 5 s.
  - `summary.json` and `aquifer.log`.
- The scripts are in `pilot/`: `backend_logged.py`, `ramp.py`, `loadgen.py`, `run_pass.sh`, `analyze.py`, `table.py`.

**Where the changes are:** I ran `git init` in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i3-r2`. It has a commit of the files as I received them on `master` and the pilot on branch `aquifer-pilot`. `app.py`, `db.py` and `partners.py` are unchanged. The Aquifer binary is in `bin/` and is gitignored, as are the Aquifer data folders and the `.l8-key` file Aquifer created.

**Cleanup:**
- Every orders-api, Aquifer and load-generator process I started is stopped, and nothing is listening on ports 19500–19509.
- I changed nothing in `aq-lab`: I only read it and built the binary from it with the output written outside it. When I first looked, `git status` there listed a modified `lab/RESULTS.md` and untracked `lab/runs/*`; at the end it showed nothing. I didn't touch either.