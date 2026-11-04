## Aquifer pilot: orders-api, 2026-11-04

**Short answer.** Aquifer fixes the problem you described. In the replayed sale, the small shops (corner-shop, kiosk, deli) went from about 10% failed orders and a 36 s p99 to no failures and a 0.11 s p99. Every bigbox order also landed, where about 10% used to fail. It doesn't add capacity, though. The database still writes about 70 orders a second either way, so bigbox now waits longer instead of failing: its p99 rose from 36 s (orders that succeeded) to 45 s (all orders). The real cost of adopting it is on the partners' side: orders become asynchronous (you get a job id first and the `order_id` later), so their integration has to change.

All of this ran on one laptop with short (30 s) bursts against a local copy of orders-api. Read the limits section before relying on the numbers.

**Backend:** orders-api (`app.py`, Python `ThreadingHTTPServer`) writing through `db.py`'s 4-connection pool (50 ms per insert, 2 s checkout timeout, then `503`). Benchmarked knee: **about 70 orders/s**. Latency is flat up to 70/s (p99 0.085 s), climbs at 75/s (p99 0.66 s), and from 90/s on, goodput stays flat at about 85/s over a 10 s window (about 74/s sustained) while everything above it turns into 2 s waits and `503`s.

**Traffic:** your `partners.py` sale burst, modelled exactly: bigbox 150/s, corner-shop 10/s, kiosk 5/s and deli 5/s for 30 s, so 5,100 orders at 170/s, 2.4x the knee. The pilot's generator (`pilot/load.py`) uses a fixed schedule because `partners.py` drifts. It also logs every order. Requests stayed at or under 222/s at all times (rule: 300).

**Shape:** Aquifer alone, 1 node, no Canalis (single node) and no Inlet (these aren't provider webhooks). Account queues on (the default; one queue per partner). Aquifer v0.15.1, built from the local checkout. Pacing: a static ceiling in `aquifer.yml` (70 rps, `max_concurrent` 4 = the pool size), plus AIMD `X-Aqueduct-Rps` headers from the backend.

### Results (median of runs; per-run numbers below)

| | Baseline (direct, today's retries) | Through Aquifer |
|---|---|---|
| Time saturated (backend seconds with `503`s or median write > 2x healthy) | 100% (64–65 of 64–65 s) | 0% (0 of 73–77 s) |
| 503 rate at the backend | 61% (7,098 / 11,637 and 7,186 / 11,800) | 0% (0 / 5,100) |
| Orders lost after retries | 9.5–11% (486–561 of 5,100), every partner alike | 0 |
| Retries sent | 6,537–6,700 (about 2.3 requests per order) | 0 at the backend; 0 `429`s from Aquifer |
| p50 / p99 end to end, all orders | 17 s / 36 s (successful orders only) | 24 s / 45 s (every order) |
| Small shops' p99 (corner-shop, kiosk, deli) | 35.4–36.6 s, and 9–13% of their orders failed | **0.11–0.16 s, none failed** |
| bigbox p99 | 35.4–36.6 s (successful orders only; 9–11% failed) | 42.5–46.2 s (all 4,500 landed) |
| Orders completed per second while busy | 71 | 66–70 |
| Drain time after the 30 s burst | 34–35 s (and about 10% given up) | 43–47 s (nothing given up) |
| Share of each partner's orders done within the burst window | about 40% for every partner | small shops 100%, bigbox 33% |
| Jain index on completion ratio, burst / overall | 1.00 / 1.00 | 0.89 / 1.00 |
| Estimated cost | same database plan | same database plan, plus one small Aquifer process (about 90 MB of disk per run here) |

**How to read the fairness row.** The baseline scores a "perfect" 1.0 because every partner suffers equally: the shops that send 5 orders a second fail and wait just as much as the one sending 150. Aquifer scores lower during the burst because bigbox, the partner causing the overload, waits while everyone else is served immediately. That's the outcome you asked for, even though the index goes down. Aquifer's own `aquifer_fairness_jain` read 1 throughout, because only bigbox's queue ever held a backlog.

**Baseline latency, two ways.** The numbers above count from when the partner meant to send the order. `partners.py` runs every partner through one shared 400-thread pool and starts its clock only when a thread picks the order up. Measured its way, the baseline p99 is 10.5 s for every partner, both in my generator and in an unmodified `partners.py` run (`pilot/data/partnerspy-output.txt`: 9–10% failed for every partner). Real partners don't share one thread pool, so the small shops' true baseline wait is probably somewhere between those two. Their failure rate doesn't depend on how latency is measured.

### What didn't improve

- **Throughput.** The database (4 connections) is the ceiling, at about 70 orders/s in both setups. Aquifer can't push more through it. It decides who waits.
- **bigbox's latency got worse.** It gets every order through instead of losing about 10%, but its p99 is up to about 10 s longer and the backlog takes 43–47 s to drain instead of 35 s.
- **The backend's pacing headers made no measurable difference.** Runs with and without them were within noise (aq1/aq2 vs aq3). I also raised Aquifer's rps ceiling to 150 on purpose (aq4 with headers, aq5 without), and the backend still never got more than 72 requests a second and never returned a `503`. The pacing came from `max_concurrent: 4` (one in-flight request per database connection), not from rps or the headers. The AIMD loop never had to cut, so it's untested against a database that gets slower than it was in the benchmark. Keep it for that case, but this pilot doesn't show it working.
- **Longer sales weren't tested.** Each burst lasted 30 s because load tests had to stay under 2 minutes. During a burst the backlog grows by about 100 orders a second, so it reaches Aquifer's default budget of 10,000 queued jobs per target after roughly 100 s of sale. After that, bigbox (and only bigbox, since it's over its fair share) gets `429` with `Retry-After: 5` and has to resubmit. The small shops stay protected. To absorb a sale of S seconds without refusals, set the budget to about 170 × S jobs: send `X-Aqueduct-Max-Backlog` from the backend, or set `AQUIFER_MAX_PENDING_PER_UPSTREAM`. Expect bigbox's wait to grow by about 1.4 s for every second the sale lasts. If sales last hours, the lasting fix for bigbox is more database connections or an agreed rate with that partner. Aquifer keeps the small shops whole in the meantime.

### What adopting it would take

- **Partners change how they call you.** `POST /jobs` answers `201 {"job_id", "status":"queued"}` right away. The `order_id` comes later, from `GET /jobs/{id}`, the `GET /jobs/{id}/stream` SSE stream, or a `webhook_url`. Partners that need the `order_id` in the response would use `POST /proxy`: it answers directly when orders-api is healthy, but once it's busy the reply turns into an SSE stream. Either way their client changes. I didn't test `/proxy`.
- **Partners should stop counting attempts.** Submit once with `idempotent_key` = their order id and `max_retries: -1`. Resubmit the same key after `Retry-After` on a `429`. Keep the order until its result arrives.
- **Writes must be idempotent.** Delivery is at least once, and a crash can lose about the last 100 ms of accepted jobs, which partners recover by resubmitting the same key. `db.save_order` doesn't deduplicate today. Add a unique key on (partner, partner order id). The pilot saw no duplicate writes, but it never crashed a node.
- **Keep Aquifer private.** It sends real HTTP to whatever `url` a job names, so partners shouldn't call it directly. Put it behind your own API, which fills in the url. Note that it binds to all interfaces (`:PORT`). The pilot set `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1` as a second guard.

### Config

- **Aquifer** (`pilot/aquifer.yml`): `127.0.0.1:19100: {rps: 70, max_concurrent: 4}`. rps is set to the benchmarked knee. `max_concurrent` is per tenant queue, so 4 partners could have up to 16 requests in flight. In practice the small queues rarely held anything, and the database queued the rest briefly without timing out. Variant config `pilot/aquifer-ceiling150.yml` was used only for aq4 and aq5.
- **Environment:** `PORT=19101`, `DB_PATH`/`L8_KEY_PATH` under `pilot/data/aq-<run>/` (fresh Pebble store for each run), `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1`. Everything else at its default: account queues on, fair-admission budget 10,000, `Retry-After` 5 s, 100 ms Pebble flush, inbound pacing at defaults, no memory limit (Aquifer warns about this; set `AQUIFER_MEMORY_LIMIT_MB` in production).
- **Backend headers** (branch `aquifer-pilot` in this directory, `app.py --pacing`): `X-Aqueduct-Rps` from an AIMD loop. It cuts ×0.7 (at most once a second) when the p95 wait for a connection goes over 100 ms or any `503` occurs, adds +10 below the last trouble point and +2 above it, and is capped at 70. It also sends `X-Aqueduct-Max-Concurrent: 4`. I sent the full rate to each queue rather than dividing it by `X-Aqueduct-Active-Queues` as the skill suggests: with one node, Aquifer's configured budget already caps the total, and an equal split would have held bigbox to a quarter of capacity while the small shops left theirs unused. `503` was left unchanged for direct callers. `app.py --log` writes one JSON line per request, including every `X-Aqueduct-*` header received.
- **Retry settings.** Baseline: `partners.py`'s own (5 attempts, 0.1 s apart, 5 s timeout). Aquifer: `max_retries: -1` per job, and callers resubmit after `Retry-After` on a `429` (none happened).
- **Choices I made without asking:** 30 s bursts with drain capped at 115 s (your 2-minute rule; all runs drained in 73–78 s), ports 19100 (orders-api) and 19101 (Aquifer), and a git repo in this directory (`main` = as found, `aquifer-pilot` = pilot changes). `aq-lab` was not modified: its `git status` was clean before and after, and the binary was built into `pilot/bin/`.

### Limits of this pilot

- The skill asks for passes of hours to a day on an always-on machine. This was 2 baseline runs and 5 Aquifer runs of about 75 s each on a laptop. The runs were very consistent (within about 1 s of each other), but they cover one 30 s burst shape, not a day of real traffic.
- The database is `db.py`'s simulation (a semaphore plus `sleep`), not Postgres. A real database's knee will differ, so benchmark the real one with `pilot/bench.py` before setting `rps` and `max_concurrent`.
- No crash or restart was tested.
- Local quirk: `http.server` does a reverse-DNS lookup of 127.0.0.1 when it binds, which takes about 20–35 s on this laptop, so orders-api starts slowly. `pilot/start_app.sh` waits for it. It doesn't affect the results.

### Runs

| Run | Setup | Lost | Small shops p99 | bigbox p99 | Peak backend rps | 503s |
|---|---|---|---|---|---|---|
| base1 | direct | 561 | 35.4–35.5 s | 35.4 s | 220 | 7,098 |
| base2 | direct | 486 | 36.6 s | 36.6 s | 222 | 7,186 |
| aq1 | Aquifer, headers on | 0 | 0.11 s | 45.3 s | 71 | 0 |
| aq2 | Aquifer, headers on (repeat) | 0 | 0.11 s | 45.4 s | 71 | 0 |
| aq3 | Aquifer, headers off | 0 | 0.11–0.16 s | 46.2 s | 73 | 0 |
| aq4 | Aquifer, ceiling 150, headers on | 0 | 0.11 s | 45.5 s | 72 | 0 |
| aq5 | Aquifer, ceiling 150, headers off | 0 | 0.11–0.12 s | 42.5 s | 72 | 0 |
| partners.py | unmodified replay, direct | 323 of 3,943 sent | 10.6 s (its own measure) | 10.6 s | – | 5,262 |

### Raw data

All of it is in `pilot/data/` in this directory:
- `<run>-load.jsonl`: one line per order, with partner, scheduled/start/end times, attempts and status codes (Aquifer runs: submits, job id, `429` reasons). `<run>-load.meta.json` holds requests per second from the generator.
- `<run>-backend.jsonl`: the backend's per-request log, with timing, status and the `X-Aqueduct-*` headers received. With `--pacing`, the AIMD's cut and raise lines are mixed in.
- `<run>-metrics.txt`: Aquifer `/metrics` and `/health` scraped every 15 s, plus a final scrape. The final scrapes agree with the load logs: `aquifer_jobs_total{outcome="completed"} 5100`, all attempts `2xx`, no `aquifer_admission_rejected_total`.
- `<run>-summary.json`: the output of `pilot/analyze.py`. `bench.jsonl` holds the knee benchmark. `aq-<run>/aquifer.log` holds Aquifer's logs.

To reproduce: `pilot/bench.py`, `PACING=1 pilot/run_aquifer_pass.sh <run>`, `pilot/load.py --mode direct`, then `pilot/analyze.py pilot/data/<run>`.
