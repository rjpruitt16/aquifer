## Aquifer pilot: orders-api, 2026-11-04

Aquifer fixes the small-shop problem. With Aquifer in front of orders-api, all of the small shops' orders went through during the sale burst, and their p99 dropped from about 34 s to about 0.11 s. Every order from every partner was written, with no 503s and no retries. The cost lands on bigbox: its orders wait longer, about 25 s at p50 and 44 s at p99, against 17 s and 35 s today. Aquifer adds no database capacity. It changes who waits and makes sure nothing is lost.

I ran the pilot hands-off with the aquifer-pilot skill from your checkout, all on this laptop. Everything used ports 19300–19309. No test sent more than 225 requests a second and none ran longer than 115 s. `aq-ref` was not modified, and every process I started has been stopped.

**Backend:** orders-api (Python, `ThreadingHTTPServer`) in front of a simulated Postgres pool: 4 connections, a 50 ms insert, and a 2 s checkout timeout that returns 503. The benchmark is single attempts at stepped rates, 10 s per step:

| Offered rate | What happened |
|---|---|
| 70 rps | clean, p99 61 ms |
| 80 rps | p50 jumps to 400 ms |
| 90 rps | 6.6% errors, p50 1.7 s |
| 100–120 rps | goodput falls to about 74–75/s, with 26–38% errors |

The limit is about 75–80 orders/s (4 connections × 20 inserts/s each).

**Traffic:** your `partners.py` sale burst, re-implemented in a pilot load generator with per-order logging:
- bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s;
- 30 s long, 5,100 orders;
- that is about 2.2× the database's capacity.

The baseline keeps your partners' retry policy: up to 5 attempts, 0.1 s apart. Latency is measured from when each order was due to be sent.

**Shape:** a single Aquifer node, with no Canalis and no Inlet. I left out Inlet because these are partner API calls from your own integration, not provider webhooks. Account queues were on (the default), with one queue per partner. Aquifer was built from source (`v0.15.1-61-g0dbeff3`), and the pacing signal was a static YAML ceiling.

| | Baseline (today) | Through Aquifer (static ceiling) |
|---|---|---|
| Orders completed | 4,769 / 5,100 and 4,720 / 5,100 (6.5–7.5% lost) | 5,100 / 5,100 in both runs |
| Time the DB pool was queueing | 100% | 24–27% (wait p99 42 ms) |
| 429 / 5xx | 6,822 and 6,761 503s | 0 / 0 |
| Requests sent (retries included) | 11,591 and 11,481 | 5,100 (no retries needed) |
| p50 / p99 end to end, all orders | 16.7 s / 34.8 s | 22.9 s / 44.3 s |
| Small shops' p99 | 33.9–34.9 s, about 7% of their orders lost | 0.105–0.11 s, none lost |
| Bigbox p50 / p99 | 16.8 s / 34.8 s, 286–337 orders lost | 25.5 s / 44.4 s, none lost |
| Orders completed per minute | about 4,490 | about 4,100 (ceiling set at 70, below the limit) |
| Time to drain after the burst | about 33 s, with orders dropped | about 44 s, nothing dropped |
| Worst tenant's p99 | 34.9 s (everyone is equally bad) | 44.4 s (bigbox) |
| Peak requests/s at the DB | 223 | 77 |
| Duplicate inserts | 0 | 0 |
| Cost | — | No machines used. About 56% fewer requests and no 2 s pool timeouts tying up app threads |

Both rows were run twice, and the repeat runs came out within a few percent of each other. Before Aquifer, every partner failed at the same rate, about 7%, so per-partner completion ratios look perfectly fair (Jain index 1.0) even though it is equally bad for everyone. The real difference is in the small shops' rows: during the burst they were served in full and immediately.

**What didn't improve:**
- **Total throughput is slightly lower.** The ceiling I set (70) is a little under the limit (about 75–80), so the backlog took about 11 s longer to drain. Setting `rps: 75` would probably close most of that gap; I did not test it.
- **Bigbox waits longer.** Its p50 and p99 rose by about 9 s, because it now queues behind the small shops instead of losing about 7% of its orders.
- **Aquifer can't fix a sale that lasts.** At 170/s against about 70/s, the backlog grows by about 100 jobs a second; this 30 s burst peaked at about 2,900. For an hour-long sale, Aquifer's default backlog limit (10,000 waiting jobs) fills in under 2 minutes. After that, bigbox gets 429s while the small shops keep getting in. Sustained sales need a bigger database plan or batched inserts. I couldn't test anything longer because of the 2-minute limit per test.
- **Backend pacing headers made things worse, so I'm not recommending them.** I tried an AIMD loop in orders-api that sets each partner's `X-Aqueduct-Rps` from what that partner actually used (the `aq-pacing70` run). It pushed the small shops' p99 from 0.11 s up to 5.8–9.2 s, and draining took 5 s longer. The flaw is in that loop's design: the rate it measures is the rate Aquifer already capped, so a low cap keeps itself low. The backend sees only the total backlog, not each partner's, so it can't tell "capped" apart from "idle". The static ceiling is enough here, which matches what the skill says to try first.

**Fit:** this is a good fit for the fairness problem: a database with bursty writers. It is not a fix for capacity, because the pool size is fixed by your plan. A cheaper alternative worth weighing is fair limiting inside orders-api itself: cap each partner's share of the 4 connections and fail fast with a 429 instead of holding a 2 s wait. That fixes the small shops without new infrastructure, but it doesn't keep bigbox's orders and replay them later.

**Before adopting it for real:**
- **The partner contract changes.** Today partners get a synchronous `201` with an `order_id`. Through Aquifer the order is written later (up to about 45 s for bigbox in this test), so partners would need an "accepted" response plus a webhook or a status check.
- **Don't expose Aquifer to partners.** If a caller can choose the URL, Aquifer can be pointed at your internal network (an SSRF risk). Your own intake should submit jobs to it with a fixed URL.
- **Make inserts idempotent.** Aquifer delivers at least once, so add a unique key on (partner, order id) in `save_order`. I saw no duplicates, but nothing in the code prevents them.
- **Know what a crash can lose.** A crash can drop jobs accepted in the last ~100 ms. The fix is to resubmit with the same `idempotent_key` (`201` means it had been lost, `200 duplicate:true` means Aquifer still has it), and to keep the order record in your own database. Don't turn the flush interval to 0.

**Config:**
- **`aquifer.yml`:** `127.0.0.1:19300` at `rps: 70`, `max_concurrent: 4`; the webhook receiver `127.0.0.1:19302` at `rps: 300`, `max_concurrent: 50`; defaults 2/1.
- **Env:** `PORT=19301`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1`, `AQUIFER_NODE_ID=pilot-1`. Everything else was left at defaults: 10,000-job backlog budget, 100 ms flush, account queues on, `Retry-After` 5 s.
- **Jobs:** `user_id` = partner, `idempotent_key` = run-partner-n, `max_retries: -1`, and a webhook to the load generator. The load generator would have waited for `Retry-After` and resubmitted on a 429, but no 429s happened.
- **Defaults I chose without asking:**
  - 70 rps, about 10% under the limit;
  - a 30 s burst, matching `partners.py`'s default;
  - `/metrics` scraped every 5 s, not the skill's 15–30 s, because the runs were short;
  - a client-side cap of 300 requests/s, which was never reached.
- **My first Aquifer run (`aq-static1`) used `max_concurrent: 2`.** That limit applies to each partner's queue separately, so it held bigbox alone to about 37/s, and 382 orders were still queued when the test hit its time limit. Raising it to 4 was the only change before `aq-static2` and `aq-static3`. The rule is that `max_concurrent` has to be at least the rate times the backend's latency (70 × 0.055 s ≈ 4).

**Docs gaps found (things to raise on the Aquifer repo):**
1. The rate between `max_concurrent` and rps above isn't documented, and it cost a full run.
2. `aquifer_flow_rate_rps` read 129–183 while 4 queues were active, against a 70 rps ceiling, even though the database actually received only 71–74/s. Either the metric or the docs' "the sum stays within it" claim is misleading.
3. The timeout Aquifer uses when it calls the backend isn't documented anywhere.
4. `configuration.md` says `Retry-After` always doubles under repeated refusals, while the skill says backlog refusals stay at 5 s. I didn't hit any 429s, so this went untested.
5. The skill is pinned to v0.15.1, but the checkout is 61 commits past it. I used the checkout's own copy, as you asked.

**Run conditions:**
- Aquifer listens on every network interface, and there is no setting to keep it on this machine only. To limit that, I restricted the URLs it would call to 127.0.0.1.
- Aquifer's memory limit is off on macOS.
- Other agents' pilots were running on this Mac at the same time (ports 19100, 19200, 19600), so CPU was shared. I didn't touch their processes, and my repeat runs still matched closely.

**Raw data and changes:**
- **Pilot directory:** `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/ref4-r3/pilot/`
  - `data/<run>/` for each run: `orders.csv` (one row per order: partner, due time, finish time, attempts, response codes), `backend.csv` (every database attempt, its pool wait and the `X-Aqueduct-*` headers received), `backend.csv.pool` (pool samples every 0.5 s), `metrics.txt` (the `/metrics` and `/health` scrapes), `metrics_final.txt`, `aquifer.log`, and the `aquifer.yml` that run used.
  - The runs are `bench`, `baseline1`, `baseline2`, `aq-static1`, `aq-static2`, `aq-static3` and `aq-pacing70`.
  - `bench.py`, `load.py`, `run_pass.sh` and `analyze.py` (`python3 analyze.py data/<run>` re-derives the numbers above), plus `aquifer.yml` and `bin/aquifer`.
- **`app.py` changes:** I added two flags, both off by default: `--pilot-log` and `--pacing`. Running `python3 app.py` behaves exactly as before. The original is at `pilot/app.py.orig`, and `db.py` is unchanged.