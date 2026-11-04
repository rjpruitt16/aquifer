## Aquifer pilot: orders-api, 2026-11-04

I ran the pilot hands-off on this laptop, following the `aquifer-pilot` skill in the local checkout. **Result: Aquifer fixes the small-shop problem. In the sale replay their orders went from about 20% failing to 0% failing, with a p99 of about 0.1 s instead of about 32 s. No orders were dropped and the database stopped timing out. The cost is that bigbox waits longer for its orders: p50 about 25 s, against about 9 s in the baseline.**

The runs had to be much shorter than the skill asks for. The skill wants passes of hours on an always-on machine; your rules allowed under 2 minutes per test on the laptop. So these are 30 s sale replays (15 s for the slow-database test). The direct and Aquifer passes were each run twice and agreed closely; the two slow-database runs were done once each.

**Backend:** orders-api (Python `ThreadingHTTPServer`). The bottleneck is the database connection pool: 4 connections, about 50 ms per insert, 2 s wait for a free connection, then `503`.
- **Measured knee:** a flat 54 ms p50 up to 70 orders/s. At 80/s p99 rises to 555 ms. At 90/s or more, 2–37% of requests get `503` and successful orders drop to about 75/s.

**Traffic:** the model from `partners.py`, a 30 s sale with 5,100 orders.
- bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s.
- In the direct pass, partners retry a failed order 0.1 s later, up to 5 attempts, with a 5 s timeout, as `partners.py` does.

**Shape:**
- One Aquifer node, no Canalis or Inlet (one trusted caller, not provider webhooks).
- Account queues on (the default).
- Aquifer built from the checkout: `aquifer dev`, `git describe` = `v0.15.1-56-gf4a5df3`, which matches the skill's v0.15.1 pin closely enough.
- Pacing signal: a fixed limit in the rate file only. Backend headers were tried separately (see below).

| | Direct (runs 02 / 05) | Through Aquifer (runs 03 / 04) |
|---|---|---|
| Time the database pool was saturated | 97–98% | 24% (at most 3 requests waiting) |
| Orders failed | 19.4% (about 1,000 of 5,100), every tenant | 0% |
| Backend `503` rate | 71.8% of 14,500 attempts | 0% of 5,100 |
| Requests sent to the backend | 14,531 / 14,554 (2.85 per order) | 5,100 (1.0 per order) |
| Insert time at the backend, p50 / p99 | 2,050 / 2,060 ms (most requests waited out the full 2 s) | 50–60 / 90–100 ms |
| End-to-end p50 / p99, all orders | 9.0–9.5 / 32.6 s (successful orders only) | 22–23 / 43–45 s |
| Small shops: failed / p99 | 18–24% / 27–33 s | 0% / 0.11–0.14 s |
| bigbox: failed / p50 / p99 | 19.4% / 9.1–9.7 / 32.6 s | 0% / 25–26 / 43–45 s |
| Orders per hour while busy | about 270k (but about 1,000 orders lost) | about 243–249k |
| Last order done after the sale ended | 24.7 s (the rest had failed) | 43.6–45.6 s (everything) |
| Duplicate orders written | 0 | 0 |

How to read the latency and fairness rows:
- **Direct latency is optimistic.** It counts only orders that succeeded, and it includes time my 300 req/s cap held orders back.
- **Fairness:** Aquifer serves small shops' full demand and gives bigbox the rest. So Jain's index over each tenant's fraction served during the sale is 0.90, against 1.00 direct. Direct scores higher only because every tenant failed equally.
- **`aquifer_fairness_jain` read 1** throughout, but it only counts queues with work waiting, and that was only ever bigbox. It tells you little here.

**Slow-database test** (inserts set to 100 ms, so capacity is about 37/s; 15 s sale, 2,550 orders):
- **Fixed limit only (run 06):** 0 `503`s and 0 failures. Small shops' p99 was 0.30 s; bigbox's p99 was 51.5 s.
- **Why it held:** `max_concurrent: 4` per tenant matched the 4-connection pool, so bigbox alone could never take more than the pool. The rate limit of 70/s was wrong for this database and didn't matter.
- **With backend pacing headers (run 07, `app.py --pacing`):** small shops' p99 fell to 0.13 s and insert p99 from 280 to 160 ms. Requests waiting for a connection fell to at most 1, and the pool was saturated 0.7% of the time instead of 21.6%. bigbox finished about 3 s later and orders per hour were about 5% lower.
- **Which header did it:** probably the concurrency header (`X-Aqueduct-Max-Concurrent`, pool size divided by active queues). The rate controller in `pacing.py` never cut, because waits never crossed its threshold, and just grew to its 100/s cap, above the 70/s limit. So the rate header had no effect here, and its cut logic went untested.
- This is one run per arm and the differences are small.

**What didn't improve:**
- **bigbox waits longer.** Everything it sends gets through, but its p50 roughly tripled. Aquifer makes it take turns instead of crowding everyone out. If bigbox needs orders confirmed fast, that cost is real.
- **Peak orders per second is slightly lower** (70/s limit against about 75/s achievable). I chose the limit at the healthy edge; I didn't test 75.
- **Partners can no longer get an `order_id` synchronously.** They get a `job_id`, and the order result arrives by webhook (or `GET /jobs/{id}`). That is an integration change for partners, or for a gateway you put in front of Aquifer. I didn't test `POST /proxy`, which keeps the call synchronous.
- **A queue can't create capacity.** A sale that runs at twice capacity for an hour builds a backlog of hours. Aquifer keeps it fair and refuses the excess with `429`s once the backlog limit (10,000 jobs by default) fills, but it can't drain it faster.
- **Orders need deduplication.** Delivery is at least once, and `save_order` doesn't deduplicate. No duplicates happened here, but production needs a unique `idempotent_key` per order in the database.

**Config:**
- **Aquifer:** `PORT=19601`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-node-1`, everything else default. Its data directory and its L8 key files were created outside your repo and deleted afterwards.
- **Rate file** (`pilot/aquifer.yml`): `127.0.0.1:19600` at 70 rps with `max_concurrent: 4` per tenant; the pilot's webhook receiver `127.0.0.1:19602` at 250 rps with `max_concurrent: 16`.
- **Jobs:** `user_id` = partner, `idempotent_key` = run, partner and order number. `max_retries: -1`, `execute_before` 10 minutes after submission, and `webhook_url` pointing at the load generator.
- **Caller retries:** on an Aquifer `429`, wait for its `Retry-After` and resubmit with the same key. No `429`s happened.
- **Defaults I chose without asking:**
  - ports 19600 (orders-api), 19601 (Aquifer) and 19602 (webhook receiver);
  - load capped at 290 requests a second, evenly spaced with no bursts. The cap was always binding in the direct pass, so real partners would retry harder and fail more than measured.
  - sale length 30 s, the `partners.py` default.

**Problems with the setup:**
- **Discarded runs.** My first load generator used threads. It let bursts reach about 415 requests/s for a few seconds, over your 300 limit, and with 2,000 threads its timing was unreliable. I discarded those runs (`pilot/data/discarded/`) and replaced it with an asyncio version that never sent more than 293 requests in a second.
- **The laptop was shared.** Other pilot agents were running load on other ports, and the load average was 46–224 during runs 01–03. Runs 04–07 were at about 5, and they matched the earlier runs.
- **Docs gap:** the docs don't say that `GET /metrics` uses chunked encoding. My client first saved empty scrapes, so run 03 has no metrics files; run 04 has them.

**Changes in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i2-r3`:** that folder isn't a git repo, so there's no branch. The original files are in `pilot/original/` for diffing.
- **`app.py`:** two flags, both off by default, so behaviour is unchanged without them. `--log-orders PATH` writes a line for each order attempt, with the `X-Aqueduct-*` headers Aquifer sent. `--pacing` turns on the response headers.
- **`pacing.py`** (new): the rate controller, plus a concurrency limit split across active queues.
- **`db.py`:** `ORDERS_DB_WRITE_SECONDS` environment variable, default 0.05, for the slow-database test.

**Raw data:** in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i2-r3/pilot/data/`, runs 01-ramp through 07-slowdb-headers. Each run folder has:
- `jobs.jsonl`: one line per order (tenant, submit and finish times, attempts, Aquifer `429` reasons);
- `backend-orders.jsonl`: each attempt with the headers it carried;
- `stats.jsonl`: the pool sampled every 0.5 s;
- `metrics/`: `/metrics` and `/health` every 15 s, Aquifer runs only;
- `analysis.json`, `summary.json` and `aquifer.log`.

The scripts are in `pilot/`: `loadgen.py`, `analyze.py`, `analyze_ramp.py`, `pass.sh`, `aquifer-pass.sh` and `aquifer.yml`.

**Cleanup:** every process I started is stopped and nothing is listening on 19600–19609. The Aquifer checkout is unmodified (`git status` is clean). The Aquifer binary I built is still in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/pilot-work/bin/aquifer`.

**Next step, if this goes further:** repeat runs 03/04 on an always-on Linux machine for hours, with recorded sale traffic and a limit of 75. Before production, add order deduplication on `idempotent_key`.