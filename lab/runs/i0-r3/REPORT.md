## Aquifer pilot: orders-api, 2026-11-04

**Short answer.** Aquifer fixes the small-shop problem, but it doesn't add capacity. Behind Aquifer, corner-shop, kiosk and deli lost no orders and their worst p99 was 0.1 s. Called directly, as partners do today, each of them lost 6–8% of orders and had a p99 of about 24 s. orders-api received 4,052 requests instead of 8,910, and sent no 503s instead of 5,279. Throughput stayed at about 67–70 orders/s, the database's limit. bigbox paid for the fairness: its p99 rose from 23 s to 30 s, but it also lost nothing (it lost 8% before). Most of the gain came from two settings: a static rate at the knee, and `max_concurrent` equal to the DB pool size. In these short runs, the backend pacing headers added nothing measurable.

**Backend:** orders-api (Python `ThreadingHTTPServer`) on a simulated managed-Postgres pool. The pool has 4 connections, each insert takes 50 ms, and a connection checkout times out after 2 s, which becomes a 503. Theoretical ceiling: 80 orders/s.
**Benchmarked knee:** about 67–70 orders/s of goodput (table below). Past the knee, goodput stays flat at about 70/s, latency jumps to the 2 s checkout timeout, and the extra requests become 503s.
**Traffic:** the `partners.py` sale burst, modelled not replayed: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s for 30 s. The Python generator actually schedules about 131–135 orders/s (≈3,950–4,050 orders per burst), the same as the original `partners.py`. Two scenarios: the burst on its own, and the burst plus a DB slowdown (inserts take 100 ms from 10 s to 40 s, which halves capacity).
**Shape:** Aquifer on its own (no Canalis: one node; no Inlet: these are API calls, not provider webhooks). 1 Aquifer node, built from the checkout at `aq-lab` (version.go 0.15.1, matching the skill's pin). Account queues ON (the default), one per partner (`user_id` = partner). Pacing signal: a static ceiling, plus, in the "paced" passes, `X-Aqueduct-Rps` from an AIMD loop on DB connection-checkout wait.

### Backend benchmark (open loop, one attempt per order, 15 s per step)

| nominal | offered/s | 201 | 503 | error rate | goodput/s | p50 | p99 | max pool_waiting |
|---|---|---|---|---|---|---|---|---|
| 40 | 31.2 | 468 | 0 | 0.0% | 31.2 | 59 ms | 62 ms | 0 |
| 60 | 44.7 | 669 | 0 | 0.0% | 44.7 | 59 ms | 62 ms | 0 |
| 75 | 56.0 | 840 | 0 | 0.0% | 56.1 | 59 ms | 62 ms | 0 |
| 90 | 67.1 | 1007 | 0 | 0.0% | 67.1 | 59 ms | 70 ms | 1 |
| 100 | 75.2 | 1127 | 0 | 0.0% | 70.5 | 607 ms | 1071 ms | 71 |
| 110 | 82.8 | 1196 | 45 | 3.6% | 70.4 | 1327 ms | 2065 ms | 165 |
| 125 | 93.9 | 1197 | 212 | 15.0% | 70.4 | 2009 ms | 2069 ms | 190 |
| 150 | 113.3 | 1200 | 500 | 29.4% | 70.6 | 2011 ms | 2069 ms | 232 |

### Sale burst: baseline vs through Aquifer

| | Baseline (direct, partners' retry loop) | Through Aquifer, static ceiling | Aquifer + pacing headers (budget ÷ active queues) | Aquifer + pacing headers (full budget) |
|---|---|---|---|---|
| Orders scheduled | 3943 (131/s) | 4052 (135/s) | 4057 (135/s) | 4029 (134/s) |
| Orders lost | **312 (7.9%)** | **0** | 0 | 0 |
| Requests reaching orders-api | 8910 (peak 199/s) | 4052 (peak 72/s) | 4057 (peak 67/s) | 4029 (peak 73/s) |
| 503 / 429 from orders-api | 5279 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Retries sent | 4967 | 0 | 0 | 0 |
| Time saturated (samples with requests waiting on the DB pool) | ~100% (up to 388 waiting) | 35% (max 3 waiting) | 16% (max 2) | 37% (max 3) |
| p50 / p99 end to end, all orders that landed | 10.5 s / 22.9 s | 16.1 s / 29.9 s | 23.5 s / 37.1 s | 15.7 s / 29.2 s |
| bigbox p50 / p99 | 10.5 s / 22.9 s | 18.6 s / 29.9 s | 25.5 s / 37.1 s | 18.0 s / 29.2 s |
| Small shops' loss (corner-shop / kiosk / deli) | 7.4% / 6.2% / 7.6% | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| **Small shops' worst p99** | **24.0 s** | **0.10 s** | 0.09 s | 0.10 s |
| Commits per second (burst start to last commit) | 70.6 | 67.4 | 60.2 | 67.8 |
| Burst drain time (last commit after the burst ended) | 21.4 s* | 30.2 s | 37.4 s | 29.4 s |
| Fairness, Jain over per-tenant completion ratio | 1.00 (everyone loses ~8%) | 1.00 (nobody loses) | 1.00 | 1.00 |
| Estimated cost | see "Cost" | | | |

\* The baseline drains sooner only because it gives up on 8% of the orders. Its latency figures cover only the orders that landed, so they flatter it.

### DB slowdown during the burst (inserts 100 ms from t=10 s to t=40 s)

| | Baseline | Aquifer, static ceiling | Aquifer + pacing headers (full budget) |
|---|---|---|---|
| Orders lost | **711 (18.0%)** | **0** | 0 |
| Requests reaching orders-api / 503s | 10471 / 7222 | 3937 / 0 | 3949 / 0 |
| Retries sent | 6511 | 0 | 0 |
| Small shops' loss / worst p99 | 17–18.5% / 31.4 s | 0 / 0.28 s | 0 / 0.26 s |
| bigbox p50 / p99 | 17.8 s / 31.9 s | 31.7 s / 42.1 s | 36.1 s / 46.9 s |
| Commits per second | 54.2 | 54.4 | 51.2 |
| Burst drain time | 29.9 s* | 42.4 s | 47.1 s |

### What the numbers say

- **The small shops' problem is congestion, not unfairness in who gets dropped.** In the baseline, every partner loses the same ~8% and waits the same ~23 s p99 (Jain index 1.0). bigbox's burst plus everyone's immediate retries (2.3 requests per order, up to 199 req/s against a 70/s database) keep up to 388 requests queued on the pool. Small shops sit in that same line. Aquifer gives each partner its own queue, so a small shop's order waits for only its own handful of queued jobs: 0.1 s p99, about 240x better.
- **Throughput did not improve.** The database is the ceiling either way (≈67–70/s). Aquifer turns failures and retries into waiting time, and that wait falls on bigbox, the partner causing the burst. Its median order now waits 18 s instead of 10.5 s, but none of its orders are dropped.
- **The retry storm disappears.** orders-api handled 2.2x fewer requests (2.7x fewer under the slowdown) for more completed orders. The DB pool queue peaked at 3 instead of 388.
- **`max_concurrent: 4` (the pool size) did the protecting.** Under the slowdown, the static pass never caused a single checkout timeout. Aquifer never had more than 4 inserts in flight per queue, so it slowed down with the database on its own (per-second commits fell from ~70 to ~37 and came back to ~65 without any header).

### What didn't improve

- **Throughput and bigbox latency.** Commits per second were the same or slightly lower (67.4 vs 70.6/s), and bigbox's p99 went up by 7 s (normal burst) and 10 s (slowdown). If bigbox has a client timeout shorter than ~30 s on order confirmation, it needs an async flow (see "Adoption caveat").
- **My pacing headers didn't help, and two versions hurt.**
  - Dividing the budget by `X-Aqueduct-Active-Queues`, as the skill suggests, cut throughput from 67 to 60/s. A small shop with one order in flight counts as an active queue, so bigbox was told 17–35/s during the burst while the small shops couldn't use their share.
  - Under the slowdown, my AIMD cut too deep: it went down to 17/s against a real capacity of ~37/s, then climbed back slowly (`pilot/data/slow-aq-paced/pacer.json`), about 5 s worse than static. That's a tuning flaw in the controller I wrote (the wait threshold doesn't scale with insert time), not an Aquifer bug. But it shows the headers only pay off with a well-tuned controller, and this workload didn't need one.
- **`aquifer_fairness_jain` looks bad when service was fair.** During the static pass it read 0.52–0.68 while the backlog drained, even though the small shops were fully served. It compares jobs finished among queues with work waiting, so with unequal demand it reports "unfair". Use per-tenant p99 and loss instead.
- **`aquifer_flow_rate_rps` overstates load.** It read 114–140 rps (the sum of each queue's allowed rate, above the 70 ceiling) while orders-api never saw more than 72/s, because the small queues don't use their allowance.

### Adoption caveat (biggest real-world change)

Behind Aquifer, `POST /jobs` returns `201 {"job_id", "status":"queued"}`, not orders-api's `order_id`. Partners would need either:
- (a) to accept an async confirmation: a webhook (`webhook_url`), `GET /jobs/{id}`, or the SSE stream; or
- (b) `POST /proxy`, which answers synchronously when it can and falls back to an SSE stream on the same connection.

Option (b) wasn't tested here. Either way, Aquifer must sit behind your own trusted service: it dispatches whatever URL it's given, so partners must not reach it directly. Writes are at-least-once, and orders-api isn't idempotent today. Use the partner's order ID as `idempotent_key`, and dedupe on it in the database (0 duplicates were seen in these runs, but nothing prevents one).

### Cost

No machines were bought, so this is in requests rather than dollars. The same sale took 4,052 database requests instead of 8,910 (5,279 of them wasted on 503s). The alternative fix for the 8% loss is a larger Postgres plan with more connections. With Aquifer, the current 4-connection plan completed every order; the cost is that bigbox waits up to ~30 s during a 30 s burst. Aquifer adds one small process with local Pebble storage.

### Cheaper alternatives worth weighing

- A per-partner concurrency cap inside orders-api (e.g. bigbox <= 3 connections), plus answering 429 with `Retry-After` instead of 503, would likely protect small shops too, for a few lines of code. It wouldn't stop the 8% loss, though: partners give up after 5 fast retries, and that queueing is what Aquifer adds. Not tested.
- Asking partners to back off (honour `Retry-After`) reduces the storm but not the loss.

### Config

- **aquifer.yml** (`pilot/aquifer.yml`): `upstreams."127.0.0.1:19300": {rps: 70, max_concurrent: 4}`, defaults `rps 2 / max_concurrent 1`. 70 = the measured knee. 4 = the pool size; this is per account queue, so up to 16 in flight in theory, <= 6 waiting observed.
- **Env:** `PORT=19301`, `DB_PATH`/`L8_KEY_PATH` under each pass's data dir, `CONFIG_PATH`, `AQUIFER_METRICS_TENANT_LABELS=true`. Everything else at defaults: `AQUIFER_ACCOUNT_QUEUE` on, `AQUIFER_MAX_PENDING_PER_UPSTREAM` 10,000, Pebble flush 100 ms, inbound benchmark 400. L8 not used.
- **Jobs:** `user_id`=partner, `idempotent_key`=`<run>-<partner>-<n>`, `max_retries: -1`, `execute_before` = submit + 10 min. Callers submit once; on 429 they wait `Retry-After` and resubmit with no cap. No 429 ever happened, so no `limit_reason` was recorded. The generator did not honour Aquifer's inbound `X-Aqueduct-Rps` (it sent ~135/s; Aquifer's inbound controller logged 3 cuts but never refused anything).
- **Backend headers (paced passes only, `app.py --pacing`):** `X-Aqueduct-Rps` from AIMD on p95 checkout wait. Saturated means p95 > 100 ms or any timeout: cut x0.7 and hold 2 s. Healthy: +8/s below ssthresh, +2/s above. Start 40, ceiling 70. Also `X-Aqueduct-Max-Backlog: 20000` (≈170/s nominal peak x 120 s). A pool timeout answers `429` + `Retry-After: 1` instead of 503 (it never fired).
- **Defaults I chose without asking:**
  - local-only, ports 19300 (orders-api) and 19301 (Aquifer);
  - single node;
  - 30 s bursts (each pass, drain included, finishes within ~112 s to respect the 2-minute rule), so this is minutes of traffic, not the skill's "hours to a day";
  - `/metrics` scraped every 5 s instead of 15–30 s because runs are short;
  - one run per configuration (no repeats);
  - a load generator cap of 300 attempts/s (never reached; peak 199/s);
  - the DB slowdown scenario is my invention, to test the pacing headers.
- **Baseline client:** `pilot/loadgen.py --mode direct` reproduces `partners.py` exactly: 400 threads, a 5 s timeout, up to 5 attempts, 0.1 s between them. It was cross-checked against an unmodified `partners.py` run (same ~8% loss per partner; `pilot/data/partners-orig/`). The baseline measures latency from when each order was scheduled. `partners.py` measures from when a worker thread picks the order up, so it reports a lower p50 (4.2 s).

### Caveats

- The laptop was shared with other pilot runs (load average ~5 on 14 cores). Absolute numbers are laptop numbers, and each configuration ran once.
- Aquifer's HTTP server binds `:19301` on all interfaces, and there's no setting for a host. Runs were short and nothing sensitive was reachable, but in production keep it on a private network as its README says.
- `http.server` in orders-api takes ~19 s to start here (a reverse-DNS lookup at bind), so `pilot/start_app.sh` waits on `/health`.
- Every process I started (orders-api, Aquifer, scrapers, load generators) was stopped. The `aq-lab` checkout is unmodified (`git status` clean).

### Raw data

All under `pilot/` in this folder:
- `pilot/data/<pass>/`:
  - `orders.csv`: the generator's per-order log (scheduled time, attempts, status, job_id, 429 reasons);
  - `backend.csv`: orders-api's per-request log (time, partner, order, status, save time, and the `X-Aqueduct-Node-Id/Flow-Rate/Queue-Depth/Active-Queues/Upstream-Backlog/Total-Jobs` headers it received);
  - `stats.log`: DB pool every 0.5 s;
  - `metrics.scrapes`: `/metrics` + `/health` every 5 s;
  - `metrics.final`, `health.final`;
  - `pacer.json`: the AIMD trace for the paced passes;
  - `aquifer.log`, `aquifer.db/`;
  - `summary.json`.
  - Passes: `baseline`, `aq-static`, `aq-paced`, `aq-paced-full`, `slow-baseline`, `slow-aq-static`, `slow-aq-paced`.
- `pilot/data/bench/`: the backend benchmark.
- `pilot/data/table-*.md`: the tables above.
- Scripts: `pilot/loadgen.py`, `pilot/run_pass.sh`, `pilot/start_app.sh`, `pilot/analyze_bench.py`, `pilot/analyze_pass.py`, `pilot/table.py`, `pilot/pacer_trace.py`. The Aquifer binary is `pilot/bin/aquifer`.
- Service changes: `app.py` gains opt-in flags (`--order-log`, `--pacing*`, `--slow-window`), and with no flags it behaves as before. The originals are in `pilot/app.orig.py` and `pilot/partners.orig.py`. `db.py` and `partners.py` are unchanged.

### Recommendation

If bigbox can live with order confirmations arriving up to ~30 s late during a sale, put Aquifer in front with a static ceiling at the knee (`rps: 70`) and `max_concurrent` equal to the DB pool size. Leave the pacing headers off until capacity actually varies, for example a bigger DB plan or replicas. Before adopting, decide how partners get their `order_id` (webhook vs `/proxy`) and make inserts idempotent on the partner's order ID. If confirmations need to stay synchronous and fast for bigbox, first try the few-line per-partner connection cap in orders-api.
