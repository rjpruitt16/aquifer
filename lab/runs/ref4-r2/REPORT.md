## Aquifer pilot: orders-api, 2026-11-04

Short answer: Aquifer fixes the problem you described. With Aquifer in front, no order failed in three runs, the small shops' orders landed in under 0.15 s at p99 (it was 10.5 s directly), and the database pool never ran out of connections. The cost falls on bigbox: its orders wait in line, 24 s at the median and 40 s at p99. Directly, bigbox waited less but lost about 8% of its orders.

**Backend:** orders-api (Python `ThreadingHTTPServer`) with a simulated Postgres: 4 pooled connections, 50 ms per insert, 2 s wait for a free connection, then `503`. That caps it at about 80 orders/s. I measured the limit by sending a fixed rate with no retries, 8 s per step:

| Rate sent | Result |
|---|---|
| 20 to 70/s | p50 55 ms, no errors |
| 80/s | p50 338 ms, p99 605 ms, no errors yet |
| 90/s | p50 870 ms |
| 100/s | 6.6% errors |
| 130/s | 28% errors, about 93/s getting through |

So the healthy ceiling is 70/s, and latency starts climbing at 80/s.

**Traffic:** the sale burst modelled in `partners.py` (no recorded traffic existed): bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s for 30 s, which is 5,104 orders over 4 tenants. Both passes sent the identical schedule.

**Shape:** Aquifer alone on one node, account queues on (the default), no pacing headers, a static YAML ceiling, no Canalis, Inlet or L8. I built Aquifer from the checkout at `v0.15.1-61-g0dbeff3`. The pilot skill is pinned to v0.15.1; I used the copy in your checkout as you asked.

The fit is good: bursty writers against a database plan with a fixed number of connections. Inlet isn't needed because these aren't signed provider webhooks, and Canalis isn't needed for one node.

### Results
The Aquifer column is the run with a ceiling of 80/s, which ran twice with near-identical numbers. The 70/s run is noted where it differs. The baseline also ran twice; both runs are shown.

| | Baseline (direct, today's 5-attempt retries) | Through Aquifer |
|---|---|---|
| Orders failed | 443 and 411 of 5,104 (8.7% / 8.1%) | **0** (all three runs) |
| Small shops' orders failed | corner-shop 7–11%, kiosk 8–14%, deli 9–15% | 0 |
| Backend `503` rate | 59% of all attempts (6,690 of 11,351) | 0 |
| Retries sent | about 6,250 (2.2 attempts per order) | 0. No Aquifer `429`s either, so no client resubmits |
| p50 / p99, all orders | 4.1 s / 10.5 s (counts only orders that succeeded) | 22.0 s / 40.3 s |
| Small shops p50 / p99 | 2–4 s / 10.5 s | **0.06–0.07 s / 0.09–0.14 s** |
| Bigbox p50 / p99 | 4.1 s / 10.5 s, and about 8% lost | 24.2 s / 40.3 s (70/s ceiling: 25.8 s / 44.6 s), none lost |
| Orders completed per second | 74.6 | 72.3 (70/s ceiling: 68.1) |
| Time after the burst until the last order completed | 32.5 s (that is when clients gave up) | 40.6 s (70/s ceiling: 45.0 s), every order delivered |
| Database pool | full in 100% of samples, up to 393 requests waiting for a connection, timing out at 2 s | full in about 85% of samples but never more than 3 waiting; wait for a connection p99 41 ms, max 72 ms |
| Worst tenant's p99 | about 10.5 s for every tenant, plus lost orders | bigbox at 40.3 s; every small shop under 0.15 s |
| Fairness index over each tenant's share of its own orders served | 0.999 | 1.0 |
| Cost | — | runs on the existing database plan (see below) |

Why the small shops win: each tenant gets its own queue, and Aquifer splits the 70 or 80/s ceiling between active queues in proportion to what each needs. During the burst the small shops got their full 10/5/5 per second every second, and bigbox got the remaining 50–55/s.

On cost: without Aquifer, about 59% of the database's request handling went on `503`s and retries. Fixing that without Aquifer means a larger connection pool, which means a bigger plan. Aquifer ran on the same machine with the same 4 connections. I don't know your plan's prices, so I can't give a dollar figure.

**What didn't improve:**
- **Bigbox waits much longer per order:** p50 went from 4 s to 24 s and p99 from 10.5 s to 40 s. That's the price of fair turns and of pacing below the limit. Its baseline numbers look better partly because the orders it lost aren't in them.
- **Throughput is slightly lower** (72.3/s against 74.6/s). The baseline pushes the database past its healthy limit; Aquifer deliberately doesn't. The burst therefore takes about 8 s longer to clear, but nothing is dropped.
- **Partners' integrations would have to change.** Aquifer accepts a job in milliseconds, but the final result comes later. Their current call (wait for the `201` with an `order_id`, give up after 5 s) can't wait 40 s in line. They would need to accept a "queued" reply and get the result by webhook or `GET /jobs/{id}`. Aquifer's `POST /proxy` tries the database directly first and falls back to the queue on the same connection; I didn't test it. That integration change is the main cost of adopting this.
- **Partners must not call Aquifer directly.** It sends real HTTP requests to whatever URL it's given. Your own authenticated front door should be the thing that submits to Aquifer. In this pilot the load generator called Aquifer directly as a stand-in for that front door.

**Limits of this pilot:**
- The passes are 30 s bursts of about 1 minute each, on this laptop. The skill asks for hours on an always-on machine; your 2-minute, local-only rules ruled that out.
- The database is the simulated one, with one burst per pass and no crash or restart test.
- Other agents were running load tests on this laptop at the same time (on ports 19500 and 19600, not mine), so there was some CPU contention. Repeated runs matched closely (each baseline run within 32 failed orders of the other; both 80/s runs within 0.01 s at p50/p99).

**Config:**
- **Aquifer settings** (`pilot/aquifer.yml`, and `pilot/aquifer-rps80.yml` for the second pass): upstream `127.0.0.1:19200` at `rps: 70`, then `80`, with `max_concurrent: 4` (the pool size; it applies to each tenant's queue separately).
  - The 70 is the highest healthy step from the limit test.
  - The 80 pass was the only change between the two Aquifer passes. It shortened the queue time and still caused no pool timeouts.
  - I'd start production at 70–75 for headroom, because real insert times vary more than the simulation's.
- **Env vars:** `PORT=19201`, a fresh `DB_PATH` for each pass, `CONFIG_PATH`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-node-1`. Everything else at its default: `AQUIFER_MAX_PENDING_PER_UPSTREAM=10000`, `AQUIFER_RETRY_AFTER_SECONDS=5`, writes synced to disk every 100 ms.
- **Jobs:** `user_id` = the partner, `idempotent_key` = `<partner>-<n>`, `max_retries: -1`, no `execute_before`, no `webhook_url`. Completions were measured from orders-api's own log.
- **Client:** submits once; on a `429` it waits `Retry-After` and resubmits with no attempt cap. That never happened.
- **Baseline client:** `partners.py`'s exact behaviour (5 attempts, 0.1 s apart, 5 s timeout, 400 workers). One difference: requests are scheduled against the clock instead of `sleep(1/rate)`, so it sends exactly 170/s. Both passes used it.
- **Pacing headers:** none. The skill says to add them only if a measured run shows the static ceiling isn't enough, and it was enough.
- **Backlog budget:** left at the default of 10,000 jobs. At 170 orders/s that holds about 60 s of a sale. For longer sales, set `AQUIFER_MAX_PENDING_PER_UPSTREAM` to about 170 × the longest burst in seconds (not tested).
- **Load limits:** every request the load generator sent went through a 300/s cap, which never kicked in. The baseline peaked at about 182 attempts/s; the Aquifer passes sent about 170/s in and at most 78/s out to orders-api. Each test finished in 62–76 s.

**Changes to orders-api (branch `aquifer-pilot`):** the folder wasn't a git repository, so I ran `git init`. `main` is the code as you gave it; `aquifer-pilot` has two commits:
1. Duplicate orders are ignored, keyed on (partner, order). Aquifer can deliver the same job more than once, so this is required. It held a connection like a real `ON CONFLICT DO NOTHING` insert, and no duplicates happened in these runs. The same commit adds a `--log` option that writes one line per request, including the `X-Aqueduct-*` headers.
2. The `pilot/` scripts: `bench.py`, `loadgen.py`, `scrape.py`, `analyze.py`, `run_aquifer_pass.sh`, and the two YAML files.

**Problems in Aquifer's docs and metrics, worth reporting upstream:**
1. The `aquifer_flow_rate_rps` metric read 210, 118.69 and 84.04 while 2–3 queues were active. The docs call it the sum of the queues' current rates, but orders-api actually received 72–78/s against a 70 ceiling. It looks like the metric adds up the rates before they are scaled down to fit the ceiling.
2. Against a 70/s ceiling, orders-api received up to 78/s in a single second, about 72/s on average during the burst: roughly 5% over.
3. `aquifer_fairness_jain` dipped to 0.45–0.69 during the burst while every small shop was fully served almost instantly. It counts jobs finished per queue, so tenants with different demand look unfair. On its own it would mislead a pilot.
4. Aquifer listens on every network interface (on 19201 here), as its docs say; there is no option to restrict it to this machine.
5. I didn't need Aquifer's Go source; the docs answered everything.

**Raw data:** `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/ref4-r2/pilot-data/` (git ignores it). Folders:
- `bench/knee.jsonl`: the limit test.
- `p0-baseline` and `p0-baseline-r2`: the direct runs.
- `p1-aquifer-static`: Aquifer at 70/s.
- `p2-aquifer-rps80` and `p2-aquifer-rps80-r2`: Aquifer at 80/s.

Each run folder has:
- `loadgen.jsonl`: one line per order, with attempts, response codes and any refusal reasons.
- `backend.jsonl`: one line per request orders-api received, with the Aquifer headers and the wait for a connection.
- `backend_stats.jsonl`: pool samples every 0.5 s.
- `summary.json`: the computed results.

The Aquifer runs also have `aquifer_metrics.txt` (scraped every 5 s instead of the skill's 15–30 s, because the runs were short), `aquifer_health.jsonl`, the final metrics and health, and `aquifer.log`. The Aquifer binary is at `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/pilot-ref4-r2/bin/aquifer`.

**Cleanup:** every process I started (orders-api, Aquifer, scraper, load generator) is stopped, and nothing is listening on ports 19200–19209. The `aq-ref` checkout wasn't modified (`git status` is clean). I didn't stop the other agents' processes.