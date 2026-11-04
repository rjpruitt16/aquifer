## Aquifer pilot: orders-api, 2026-11-04

On your sale-day replay, Aquifer stopped the small shops' orders from failing. corner-shop, kiosk and deli went from about 85% success (p99 10.5 s) to 100% success (p99 about 0.1 s). Every order was delivered on the first try, so the database got no retries and no 503s. It doesn't add database capacity, though. bigbox's orders all got through, but they waited longer (p99 about 40 s), because the 4-connection pool still tops out at about 75 orders/s. Whether this is worth adopting depends mostly on whether partners can live with an asynchronous answer (see "What didn't improve").

I ran it hands-off and entirely on this laptop. Everything used ports 19400 (orders-api) and 19401 (Aquifer). Each load test lasted 30 s plus drain time (all under 2 minutes), and the request rate peaked at 236/s against the 300/s cap. I made no changes in aq-lab (`git status` is clean). Every process I started has been stopped.

**Backend:** orders-api (Python) with a simulated 4-connection Postgres pool: 50 ms per insert, 2 s checkout timeout, then 503.
- **Benchmarked knee:** about 75 orders/s.
  - Flat at 55 ms p50 up to 70/s.
  - At 80/s, p50 is 0.47 s.
  - At 100/s, 13% fail with 503 and p50 is 2.0 s.
  - Goodput never went above 74.8/s.

**Traffic:** your `partners.py` sale shape, replayed by `pilot/loadgen.py` for 30 s per pass.
- **Rates:** bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s. That's 5,104 orders, about 2.3x capacity.
- **Baseline retries:** kept as they are today: 5 attempts, 0.1 s apart, 5 s timeout.
- **One change to the replay:** each partner gets its own client thread pool. `partners.py` shares one 400-thread pool, so small shops' orders queued behind bigbox's inside the load generator itself. Shared-pool runs are kept as `baseline-sharedpool*`.

**Shape:**
- One Aquifer node, nothing else: no Inlet (these aren't provider webhooks), no Canalis, no L8.
- Account queues on (the default); the backend didn't override it.
- Aquifer built from the aq-lab checkout. It reports `v0.15.1-56-gf4a5df3`, 56 commits past the v0.15.1 the skill is pinned to. I used the skill shipped in that checkout, as you asked.
- **Pacing:** YAML ceiling only. Backend headers were tried and made things worse (below).

| | Baseline (2 runs) | Through Aquifer, rps 75 ceiling (1 run; rps 70 ran twice, matching) |
|---|---|---|
| Time saturated (pool had waiters) | 98–100% of seconds | 33% |
| 503 rate at backend | 57–60% of attempts | 0% |
| Retries sent to backend | 6,910 / 6,974 | 0 (5,104 attempts for 5,104 orders) |
| Orders lost | 448 / 523 (8.8–10%) | 0 |
| Small shops: success, p99 | 85.4% / 83.6%, p99 10.5 s | 100%, p99 0.11 s |
| bigbox: success, p50 / p99 | 92.0% / 90.6%, p50 20 s, p99 36 s | 100%, p50 24 s, p99 40.5 s |
| Orders/hour (completed ÷ span) | about 258k / 255k | about 259k |
| Delivered rate during burst | 72/s | 73/s |
| Burst drain (last completion after the burst ends) | 35 s (partly by giving up) | 41 s (all orders) |
| Fairness | demand-normalized Jain 0.95 | 0.90 (small shops got all their demand, bigbox took the whole shortfall) |
| Estimated cost | about 9–10 connections needed to serve the peak directly (estimate, not tested) | the current 4 connections serve the small shops in full |

**Other passes (one change each):**
- **Ceiling 70 vs 75:** 70 ran twice with matching results. 75 cut bigbox's p99 from 44 s to 40.5 s and drain from 44.6 s to 40.9 s, with no errors. 75 is the recommended setting.
- **Ceiling 75 plus backend pacing headers (`app.py --pacing`):** worse. Burst delivery fell from 73/s to 59/s and bigbox's p99 rose to 46.5 s.
  - **Why:** the skill advises advertising `budget ÷ X-Aqueduct-Active-Queues`. With 4 active queues that caps bigbox at about 20/s, while the small shops only use 5–10/s each.
  - **What it means:** here, the YAML ceiling already splits capacity correctly, because queues that use less than their share leave the rest for others. The even-split advice in the skill hurts workloads where tenants are this uneven. That's worth reporting as a skill/docs issue.
- **Backlog budget scaled down to 2,000** (`AQUIFER_MAX_PENDING_PER_UPSTREAM`), to imitate a sale longer than 2 minutes:
  - All 1,436 `429` refusals went to bigbox (835 `queue_fair_share`, 601 `upstream_queue`, e.g. `limit 2000, current 1439`). Small shops got 0.
  - All 5,104 orders still completed through resubmit-by-key after `Retry-After`, with at most 6 submits per order.

**What didn't improve:**
- **Total capacity:** still about 75 orders/s. A sale that stays above that just grows bigbox's backlog by roughly 80–90 orders/s.
  - With the default 10,000 budget, fair-share `429`s for bigbox would likely start about 1.5–2 minutes into a sustained sale. That's projected from the budget-2000 run, not measured.
  - A long sale still needs more connections, or bigbox accepting delay.
- **bigbox's latency:** p50 and p99 got 4–5 s worse. Every bigbox order lands, but none fail fast any more.
- **Demand-normalized Jain** went down (0.95 to 0.90). It's the wrong lens here, since the goal was to protect the small tenants. The `aquifer_fairness_jain` gauge read 0.59 while all 4 queues were active.
- **Integration cost (biggest adoption item, not tested):**
  - Partners get a synchronous `201` with an `order_id` today. Through Aquifer they would get a `job_id` and have to fetch the result via `GET /jobs/{id}`, a webhook, or `/proxy` streaming. That's an API change for partners.
  - Aquifer must not be exposed to partners directly (the docs warn it becomes an open relay). You'd need a thin trusted intake that fixes the `url` and sets `user_id` from the partner's auth.
- **Limits of the test:**
  - The skill asks for hours-long passes on an always-on machine. These were 30 s bursts on a laptop because of your rules.
  - The database is simulated.
  - Only two repeats per main condition.

**Config:**
- **Aquifer YAML** (`pilot/aquifer.yml`): `"127.0.0.1:19400": rps 75, max_concurrent 4`, plus defaults `rps 2 / max_concurrent 1`.
  - `max_concurrent` is per tenant queue, and 4 is the minimum that lets a lone queue reach 75/s at 55 ms per write.
- **Env:**
  - `PORT=19401`
  - `DB_PATH=<run dir>/aquifer-data`
  - `AQUIFER_METRICS_TENANT_LABELS=true`
  - `AQUIFER_MAX_PENDING_PER_UPSTREAM=2000` in the aq5 pass only.
  - Everything else default: Pebble 100 ms flush, `Retry-After` 5 s, account queues on.
- **Jobs:**
  - `user_id` = partner, `idempotent_key` = `<run>-<partner>-<n>`.
  - `max_retries: -1` with `execute_before` = start + 600 s, no `webhook_url`.
  - An `Idempotency-Key` header goes to the backend.
  - The caller resubmits the same key after `Retry-After` on a `429`, with no attempt cap.
- **Changes to orders-api (`app.py`):** with no flags it behaves as before. The original is saved as `pilot/app.orig.py`. Additions:
  - an optional `--log` per-request JSONL, which records `X-Aqueduct-*` headers;
  - an optional `--pacing` flag (AIMD on pool wait: answers busy with `429 + Retry-After` and sends `X-Aqueduct-Rps`);
  - an in-memory `Idempotency-Key` dedupe. In Postgres, use a unique constraint on (partner, order number).
- **Defaults I chose without asking:** ceiling 75, `max_concurrent` 4, per-partner client pools, completion time taken from the backend's log (cross-checked against `aquifer_tenant_jobs_total`: 4501/301/151/151).
- **Not a git repo:** i2-r1 isn't a git repository, so there's no branch. All pilot files are in `pilot/`.

**Raw data:** in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i2-r1/pilot/data/`. Each run directory holds:
- `jobs.jsonl`: per-order load-generator log;
- `backend.jsonl`: per-request backend log with Aquifer headers;
- `sample.stats.jsonl`: pool stats every 1 s;
- `sample.metrics.txt`: `/metrics` and `/health` every 15 s;
- `summary.json`;
- `aquifer.yml.used`;
- `aquifer.log`.

The run directories are `bench/`, `baseline1/`, `baseline2/`, `baseline-sharedpool1/`, `baseline-sharedpool2/`, `aq1/`, `aq2/`, `aq3-rps75/`, `aq4-rps75-pacing/` and `aq5-budget2000/`. Aquifer's `.l8-key` and `l8-trust` were written inside each run directory, not in the service root. The data is 108 MB in total; delete `pilot/data` when you're done with it.

The scripts are in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i2-r1/pilot/`:
- `loadgen.py`, `analyze.py`, `sampler.py`;
- `run_aq.sh`, `start_aquifer.sh`, `restart_app.sh`;
- `bin/aquifer`.

**Process cleanup:** after the coordinator's resume message I checked the ports. The only process of mine still running was the orders-api left over from the baseline. An Aquifer from another run (`lab-runs/ref2-r2`) and two orders-api instances on ports 19000 and 19200 weren't mine, so I didn't touch them. At the end I stopped my Aquifer and orders-api, and nothing was listening on 19400–19409.