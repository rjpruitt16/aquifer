## Aquifer pilot: orders-api, 2026-11-04

**Short version.** Aquifer fixes the problem you described. In the sale-day replay, small shops lose 22% of their orders today. Through Aquifer they lost none, and their p99 fell from 10.5 s to under 1 s. Nobody's orders failed: 0 of 6,800, against 993 today. The database got 0 `503`s, against 11,433.

It does not add capacity. The database still writes about 70 orders/s. Bigbox sends 150/s, so its surplus now waits in Aquifer's queue (p50 about 34 s, p99 about 60 s) instead of failing. If you need bigbox's orders written faster during a sale, you need a bigger database plan. Aquifer only makes sure the overload falls on the partner causing it.

Everything ran on this laptop. Each load test lasted under 2 minutes, with 40-second bursts. The pilot skill asks for a few hours to a day of traffic, so read these numbers as a first measurement, not a soak.

---

**Backend:** orders-api (`app.py`, Python `ThreadingHTTPServer`) writing to a simulated Postgres with a 4-connection pool. Each write holds a connection for 50 ms, and a request gives up after waiting 2 s for one (`503`).
Benchmarked knee (one tenant, no retries, stepped load, `pilot/runs/01-bench/summary.txt`):

| offered orders/s | 20 | 40 | 60 | 70 | 80 | 90 | 100 | 120 |
|---|---|---|---|---|---|---|---|---|
| written/s | 20 | 40 | 60 | 69.5 | 69.9 | 70.1 | 70.0 | 70.1 |
| p50 / p99 ms | 59/62 | 59/61 | 58/62 | 64/81 | 623/1162 | 1137/2058 | 1520/2066 | 2048/2067 |
| `503` | 0 | 0 | 0 | 0 | 0 | 2.2% | 12.2% | 26.7% |

The knee is **about 70 orders/s**. Past it, latency climbs to the 2 s pool timeout and `503`s follow. Saturation looks like this: all 4 connections busy, hundreds of requests waiting for one.

**Traffic:** modeled on `partners.py`. The four partners keep their sale-day rates: bigbox 150/s, corner-shop 10/s, kiosk 5/s and deli 5/s, which is 170/s against a 70/s backend. Each burst lasts 40 s, 6,800 orders in all. I used a load generator (`pilot/loadgen.py`) on an exact schedule, because `partners.py`'s `sleep(1/rate)` loop drifts: bigbox actually sent about 113/s. The baseline keeps the partners' retry behavior: up to 5 attempts, 0.1 s apart, 5 s timeout.

The generator gives each partner its own client thread pool (bigbox 400, corner-shop 120, kiosk and deli 60 each). `partners.py` puts every partner in one 400-thread pool, so small shops' orders wait on the *client* behind bigbox's. Separate stores wouldn't do that, and it hides the server-side problem. I ran that shape too, as a reference (see "Other runs").

**Shape:** Aquifer alone, 1 node, no Canalis (single node) and no Inlet (the traffic is partner API calls, not provider webhooks). Account queues on (the default), one queue per partner (`user_id` = partner). Aquifer v0.15.1: the checkout is `v0.15.1-53-g2df7a82`, and `Version` = `0.15.1` matches the skill's pin. A source build prints `aquifer dev`. Pacing signal: a static YAML ceiling (passes A and C), plus the backend's `X-Aqueduct-Rps` AIMD on pool wait (pass B).

### Results

Baseline is run 04. "Through Aquifer" is pass A, the static ceiling of 70 rps; the two numbers are run 05 and its repeat, run 08.

| | Baseline | Through Aquifer (static ceiling) |
|---|---|---|
| Orders lost (gave up after 5 tries) | **993 / 6,800 (14.6%)** | **0** (all 6,800 completed) |
| Small shops' orders lost | **22%** (corner-shop 87/400, kiosk 44/200, deli 43/200) | **0** |
| Time saturated (pool full, sampled each 1–2 s) | 95% of samples, up to 509 requests waiting for a connection, pool wait p50 2.0 s | 74% / 56% of samples, at most 3 waiting, pool wait p99 51 / 43 ms |
| `503` rate at orders-api | 66.3% of requests (11,433 of 17,240) | 0% |
| Retries sent to orders-api | 10,440 | 0 |
| p50 / p99 end to end, all orders | 21.8 s / 43.2 s (completed orders only) | 32.6 s / 61.1 s · 30.1 s / 58.8 s |
| Small shops p50 / p99 | 4.2 s / 10.5 s (and 22% never) | 0.08 s / 0.9 s · 0.08 s / 0.9 s |
| Bigbox p50 / p99 | 24.5 s / 43.2 s (and 14% never) | 36.0 s / 61.1 s · 33.6 s / 58.8 s |
| Orders written per hour (rate while working) | 254k/h (70.6/s) | 241k/h (66.9/s) · 246k/h (68.5/s) |
| Burst drain time (after sending stopped) | 42 s, ended partly by giving up | 62 s · 59 s, all written |
| Worst tenant's p99 / fairness | bigbox 43.2 s; Jain over success rates 0.998 | bigbox 61.1 s; Jain over success rates 1.0 |
| Peak requests/s reaching orders-api | 302 | 72 · 77 |

End-to-end latency runs from the moment an order was due to be sent to the moment orders-api wrote it, taken from the backend's own log.

**How to read the fairness rows.** The baseline is "fair" only in the sense that everyone fails at about the same rate. Its small shops lost 22% and bigbox 14%. Jain's index over raw throughput is the wrong measure here, because the partners' demand differs by 30×. That includes Aquifer's own `aquifer_fairness_jain`, which averaged 0.85 and dipped to 0.46 during the burst. It reads "unfair" while the small shops are getting everything they ask for. So I scored fairness as each partner's share of its own orders served. Through Aquifer, every small-shop order was written within 1 s of being sent. Bigbox absorbed all of the delay.

**Estimated cost.** Today a 40-second sale burst loses about 1,000 orders (~25/s), and two thirds of the database requests are wasted `503`s. Through Aquifer, nothing is lost and the same 4-connection plan carries the same ~70 orders/s. You don't need a bigger database plan to stop the failures. You would need one, roughly 2× the connections for bigbox's 150/s (untested), to stop bigbox's orders queueing for up to a minute during a sale.

### Other runs

| Run | What changed (one thing) | Result |
|---|---|---|
| 02 `partners.py` as shipped, 40 s | the user's own replay script | ~10% failed for **every** partner, small shops included. p50 4.2 s, p99 10.6 s, measured from when a client thread picked the order up. The backend wrote 4,794 and refused 7,289. |
| 03 baseline, one shared 400-thread client pool | client shape of `partners.py` | small shops 88–92% success, but p50 22–24 s. They queue behind bigbox inside the shared client. |
| 06 pass B: pass A + backend `X-Aqueduct-Rps` | backend runs AIMD on p95 pool wait (cut ×0.6 above 50 ms, +5 rps steps), advertising `total ÷ X-Aqueduct-Active-Queues` | small shops p99 **0.10 s** (vs 0.9 s). Bigbox p50 / p99 42.2 / 67.3 s (worse), and throughput 63.1/s, about 6% lower than pass A. Pool wait p99 39 ms. |
| 07 pass C: pass A with `AQUIFER_MAX_PENDING_PER_UPSTREAM=2000` | backlog budget of about 12 s of arrivals, standing in for a sale longer than the budget | backlog held at 2,000. Bigbox got 3,927 `429`s (1,850 `queue_fair_share`, 2,077 `upstream_queue`, all `Retry-After: 5`). The generator waited and resubmitted, and all 6,800 orders were still written; bigbox's slowest took 84.8 s. **Small shops got 0 refusals**, p99 0.9 s. |

### What didn't improve

- **Throughput.** Aquifer wrote 67–69 orders/s against the baseline's 70.6/s. With the ceiling at the knee, about 3–5% of the pool sits idle between dispatches. A ceiling around 75 might close that gap; I didn't test it.
- **Bigbox's latency got worse.** Its p50 went from about 24 s to about 34 s and its p99 from 43 s to about 60 s. The baseline numbers only look better because the baseline dropped 14% of bigbox's orders. During a sale longer than 40 s, bigbox's backlog grows by about 95 orders/s. It hits the 10,000-job default budget after about 100 s, and from then on bigbox gets `429`s (pass C shows that path) and its waits keep growing. Only more database capacity fixes that.
- **Pacing headers didn't pay for themselves here.** The static ceiling from the benchmark was already enough (pool wait p99 ~50 ms). The AIMD made small shops faster (p99 0.9 → 0.1 s) but cost about 6% throughput. Start with the static ceiling. Revisit headers if the database's speed varies (other writers, vacuum), where a fixed 70 would be wrong.
- **Partners' integration changes.** Through Aquifer, an order is *accepted* (`201` + `job_id`) right away but *written* later. Partners need the order id asynchronously, via a webhook or `GET /jobs/{id}`, or orders-api has to give them its own id up front.

### How this would go into production (design, not built)

- **Partners must not call Aquifer directly.** `POST /jobs` takes a destination URL, which makes Aquifer an open relay for untrusted callers (API.md). Keep orders-api's public `POST /orders`. Have it authenticate the partner, then submit a job to Aquifer with `user_id` = partner, a fixed internal URL (e.g. `/internal/orders/write`), and `idempotent_key` = the partner's own order number. Return `202` with the id. Set `AQUIFER_ALLOWED_URL_DOMAINS` as a second guard. In the pilot, the load generator stood in for that intake layer.
- **Make the write idempotent.** Delivery is at-least-once. The pilot added an in-memory `Idempotency-Key` dedupe to `app.py`. In Postgres, use a unique constraint on (partner, partner order number). No duplicate deliveries happened in these runs (0 retries), so that path is in place but unexercised.
- **Retries:** jobs used `max_retries: -1` with `execute_before` = submit + 10 min. Callers submit once per order and, on `429`, wait `Retry-After` and resubmit, with no extra backoff and no attempt cap. Log `limit_reason` / `limit` / `current`, as the pilot did.
- **Size the backlog budget in seconds** for the longest sale peak you want to absorb without refusals. At ~100 orders/s of surplus, the default 10,000 holds about 100 s. A longer peak is fine too; bigbox just gets paced `429`s, as in pass C.
- **Durability:** Aquifer's default accepts a job before it reaches disk, so a crash can lose roughly the last 100 ms of accepted jobs. If every order must survive a crash, keep a durable record and resubmit by the same key. Don't set `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0`. The pilot did not crash-test.

### Config

- **Ports:** orders-api 127.0.0.1:19100, Aquifer :19101. Nothing else was used.
- **Aquifer:** built from the checkout with `go build ./cmd/aquifer`, output to `pilot/bin/aquifer` so nothing in the Aquifer repo changed. Env: `PORT=19101 DB_PATH=<run>/aquifer-data CONFIG_PATH=pilot/aquifer-static.yml AQUIFER_METRICS_TENANT_LABELS=true AQUIFER_NODE_ID=pilot-node-1`. Pass C added `AQUIFER_MAX_PENDING_PER_UPSTREAM=2000`. Everything else was default, including account queues on, inbound pacing benchmark 400, `Retry-After` 5 s and Pebble flush 100 ms. `AQUIFER_MEMORY_LIMIT_MB` was unset: macOS memory isn't readable, so it is unbounded, and Aquifer logs this.
- **YAML:** `127.0.0.1:19100: {rps: 70, max_concurrent: 4}`, with `rps` set at the benchmarked knee. `max_concurrent` is per queue, so the backend can see up to 16 in flight; at 70 rps it averaged 3.4–3.6 in use.
- **Jobs:** `user_id` = partner, `idempotent_key` = `<partner>-<n>`, `max_retries: -1`, `execute_before` = run start + 600 s, plus an `Idempotency-Key` header to the backend. No `webhook_url`. Completion was taken from the backend log and checked against `GET /jobs/{id}` (all `completed`) and `aquifer_tenant_jobs_total` (6000/400/200/200).
- **Pass B headers:** `X-Aqueduct-Rps` = AIMD total ÷ `X-Aqueduct-Active-Queues`. The total starts at 40 rps with a cap of 80 and a floor of 5. It cuts ×0.6 when p95 pool wait exceeds 50 ms, at most once a second, adds +5 below ssthresh and +1 above, and is evaluated every 0.5 s. The YAML ceiling of 70 still caps it.
- **Baseline client:** retries copied from `partners.py`. 503s and timeouts are retried.
- **Defaults I chose without asking:**
  - 40 s bursts instead of hours, to fit your 2-minute limit (each run, drain included, finished in 83–112 s);
  - per-partner client pools;
  - an exact send schedule;
  - metrics scraped every 1–2 s instead of 15–30 s, because the runs were short;
  - a 10-minute order deadline;
  - running the pacing and small-budget passes.
- **Changes to orders-api:** `app.py` gained an optional per-order JSON log (`--log`), an optional `--pacing` flag, and `Idempotency-Key` dedupe. With no flags, behavior is unchanged. The original is at `pilot/app.orig.py`.

### Notes and problems

- **Load cap overshoot.** The baseline's retry storm reached **302 requests/s** in one 1-second bucket, 2 over your 300 limit. Every other second of every run stayed at or under 300. Pass C's resubmissions peaked at an estimated 266/s at Aquifer.
- **`aquifer_flow_rate_rps` overstates the real rate.** It read up to 280 (4 queues × 70) in passes A and C, and 170 during pass A's burst, while orders-api never saw more than 77/s. The ceiling was enforced correctly; the gauge appears to sum each queue's own rate before the per-target scale-down. Don't use it as "rate at the backend".
- **`aquifer_fairness_jain` isn't demand-aware**, as described above. Pair it with per-tenant completion counts.
- **No bind-address setting is documented.** Aquifer listened on all interfaces (`:19101`), not just loopback. On a laptop or shared host, firewall it.
- **Startup time.** orders-api took up to ~35 s to answer `/health` on this machine (Python startup), so the first benchmark attempt was rerun after a readiness wait. The pilot scripts now wait for it.
- **A process I didn't start.** `app.py --port 19000` (pid 48139, started 13:14 from `aq-lab/lab/sample-app`) was already running, outside my port range. I left it alone. Every process I started (orders-api, Aquifer, load generators) is stopped.

### Raw data

All in `pilot/runs/`, one directory per run:

- `jobs.jsonl`: per order: partner, scheduled/submit/accept times, attempts, statuses, `429` reasons, final Aquifer status;
- `backend.jsonl`: every request orders-api saw, with pool wait and every `X-Aqueduct-*` header;
- `samples.jsonl`: orders-api `/stats` and Aquifer `/metrics` scrapes;
- `summary.json`: output of `pilot/analyze.py`;
- `aquifer.log` and `final_metrics.txt`.

Aquifer's Pebble data directories and its generated L8 key were deleted after each run.

To rerun:
```
pilot/start_backend.sh pilot/runs/X [--pacing]
pilot/start_aquifer.sh runs/X aquifer-static.yml
python3 pilot/loadgen.py {bench|direct|aquifer} --out pilot/runs/X
python3 pilot/analyze.py pilot/runs/X
```
