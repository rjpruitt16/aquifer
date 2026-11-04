## Aquifer pilot: orders-api, 2026-11-04

**Short answer:** Aquifer fixes the problem you described. In front of orders-api, every order from every partner got stored (5,100 of 5,100 in each of 3 runs). The small shops' orders went into the database in about 0.1 s at p99. Today, 10–17% of their orders fail and the ones that succeed take up to 10.5 s. The cost is that bigbox waits longer: its orders queue in Aquifer while the database works through the backlog. Before adopting it you need a small trusted intake in front of Aquifer, async order IDs, and a dedupe key in the database (see "What didn't improve").

I ran the pilot hands-off using the aquifer-pilot skill from your checkout (pinned to v0.15.1, which matches `version.go`). Everything ran locally on ports 19600 (orders-api) and 19601 (Aquifer). The load generator never sent more than 300 requests/s, and no test ran longer than about 100 s. The aq-lab checkout is unmodified (`git status` is clean), and every process I started has been stopped.

**Backend:** orders-api, a Python ThreadingHTTPServer on a simulated Postgres pool of 4 connections. Each insert holds a connection for about 50 ms, and a request gets a 503 after waiting 2 s for a connection.
- Benchmarked knee: about 75 orders/s. At 75/s, p99 was 0.077 s. At 78/s it rose to 0.46 s, at 90/s the first 503s appeared, and at 120/s 22% of requests got 503s.

**Traffic:** the sale-day burst modeled from partners.py: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s. That is 170 orders/s, about 2.3 times capacity, for 30 s, or 5,100 orders. Each partner has its own client pool of 400 workers. The baseline keeps partners.py's own retry: 5 attempts, 0.1 s apart, 5 s timeout.

**Shape:** a single Aquifer node, with no Canalis and no Inlet. This is your own service with a fixed target, so neither is needed. Account queues were on (the default), giving one queue per partner. I used only the static YAML ceiling, no backend pacing headers.

| | Baseline (3 runs) | Through Aquifer, rps 70 (3 runs) |
|---|---|---|
| Orders stored | 4,562–4,601 of 5,100 (about 10% lost) | 5,100 of 5,100 every run |
| Time pool saturated (a request waiting for a connection) | 99.6–100% | 22–25%, and no request hit the 2 s timeout |
| Pool utilization while busy | about 99.7% | about 91% |
| 503 rate at the backend | 63.4% (about 7,900 of 12,490 requests) | 0% (0 of 5,100) |
| Retries sent | about 7,385 (1.45 per order) | 0 (no Aquifer 429s, no backend failures) |
| p50 / p99 end to end, all partners | 15.8 s / 32.3 s (stored orders only; failures excluded) | 22.7 s / 43.8 s (every order) |
| Small shops: stored / p99 | 82.7–90% / 10.5 s | 100% / 0.10–0.13 s |
| bigbox: stored / p99 | 90% / 32.3 s | 100% / 43.8 s |
| Orders stored per second, burst through drain | about 75/s (about 270k per hour, burst rate) | about 68.7/s (about 247k per hour) |
| Burst drain time (after sending stopped) | about 31 s, ending because retries gave up | about 44 s, ending because the backlog was finished |
| Jain fairness index (per-tenant success ratio) | 0.999 | 1.0 |
| Estimated cost | The 4-connection plan drops about 10% of sale orders. Serving 170/s directly would need roughly 2.3 times the pool (about 9–10 connections; an estimate, not measured). | Same 4-connection plan, no lost orders, no database upgrade needed for a 30 s burst |

**The baseline reproduces your complaint.** Small shops lost a larger share of their orders (up to 17%) than bigbox did (10%). Bigbox's flood fills the shared pool, and a shop sending one order every 200 ms has its 5 attempts used up in about 10 s.

**Variant, one change: ceiling raised from 70 to 75 rps** (1 run). Still 0 503s and 5,100 of 5,100 stored. Drain finished about 3 s sooner (71.4 s versus 74.2 s), bigbox p99 was 41.1 s, and small shops stayed at 0.11 s. If a second run confirms it, 75 is a better ceiling than 70.

**What didn't improve:**
- **Bigbox waits longer.** Its p50 went from about 17.7 s to about 25 s and its p99 from 32 s to 44 s. That is the cost of finishing every order instead of dropping 10%, but it is longer. The baseline's p99 also looks better than it is, because the failed orders are left out of it.
- **Raw throughput fell slightly.** It went from about 75/s to about 68.7/s, because I set the ceiling to 70, below the knee. Raising it to 75 got most of that back.
- **Your callers have to change.** The skill says not to expose Aquifer to untrusted callers: the job's `url` is caller-supplied, so open access turns it into an open relay. So partners can't call it directly. orders-api needs a thin intake endpoint that accepts the order and submits a job with a fixed URL. In the pilot, the load generator played that intake.
  - Orders also become asynchronous. Partners get a `job_id`, not an `order_id`, and would read the result through `GET /jobs/{id}`, the stream, or a webhook.
- **The database needs a dedupe key.** Delivery is at least once, and `save_order` has no unique key. I saw 0 duplicate inserts, but production needs a unique constraint on (partner, order).
- **Longer sales are untested.** With the default backlog budget (`AQUIFER_MAX_PENDING_PER_UPSTREAM=10000`), the backlog grows by about 100 orders/s (170 in, 70 out). That fills the budget in about 100 s, and after that bigbox's submissions start getting 429s. Fair-share admission should still let the small shops in.
  - For real sales, size the budget in seconds: peak arrival rate times the longest burst. For example, 170/s × 600 s ≈ 100,000 jobs.
  - I couldn't test this under the 2-minute limit.
- **Aquifer's own fairness gauge doesn't help here.** `aquifer_fairness_jain` read 1 whenever only one queue was active. Per-tenant latency from the load generator and backend logs is the evidence that shows the difference.
- **Aquifer's pacing header went unused.** Aquifer advertised about 55/s per caller in `X-Aqueduct-Rps`, but the load generator ignores it, as partner integrations would. It made no difference because Aquifer refused nothing.

**Config:**
- **YAML** (`pilot/aquifer.yml`): `127.0.0.1:19600: rps 70, max_concurrent 4`. `max_concurrent` applies to each tenant's queue, and a lone tenant needs 4 to use the whole pool. Defaults are rps 2, max_concurrent 1.
- **Env vars:** `PORT=19601`, `DB_PATH=./aquifer-data` (Pebble, wiped before each run), `CONFIG_PATH`, `AQUIFER_METRICS_TENANT_LABELS=true`. Everything else stayed at its default: account queues on, retry-after 5 s, backlog budget 10000, Pebble flush 100 ms.
- **Jobs:** each order became one job with `user_id` set to the partner, `idempotent_key` set to `<run>-<partner>-<n>`, `max_retries: -1`, and `execute_before` 100 s after the order was scheduled. On a 429 the client was set to wait `Retry-After` and resubmit with the same key; no 429s happened.
- **Backend pacing headers:** none. Per the skill's step 3, the pool is a fixed capacity, and the static pass showed no saturation the ceiling failed to prevent (0 503s), so I didn't add the control loop.
- **Defaults I chose without asking:**
  - a 30 s burst;
  - 3 runs per pass, plus 1 run of the rps-75 variant;
  - per-partner client pools;
  - "saturated" means `pool_waiting > 0` in a 0.25 s `/stats` sample.
- **Load-generator limits:**
  - The load generator caps everything it sends at 300 requests/s. In the baseline that cap applied about 8,300 times, so an uncapped retry storm would be bigger and the real baseline somewhat worse.
  - A first baseline used one shared client pool, like partners.py. That delayed the small shops inside the client, before the server was involved, so I discarded it and kept it under `results/discarded-baseline-sharedpool`.
- **Caveats:**
  - Each test lasted about 1–2 minutes, against the skill's hours to a day.
  - The database is simulated.
  - Another lab run (`lab-runs/i3-r2`, with its own aquifer and loadgen on ports 19500/19501) was running on the laptop at the same time. I didn't start it and left it alone, but it may have competed for CPU.
- **Docs notes:**
  - A source build reports `aquifer --version` as "dev", not 0.15.1.
  - On macOS Aquifer can't read the machine's memory, so the memory limit stays unbounded unless `AQUIFER_MEMORY_LIMIT_MB` is set.
  - Aquifer listens on `*:19601`, all interfaces. I found no documented setting to bind it to localhost only, which matters given the advice not to expose it.

**Changes in your repo:**
- `app.py` has an opt-in `--log PATH` flag that writes one JSON line per order request, with its status and the `X-Aqueduct-*` headers. It is off by default, and the original is saved as `pilot/app.py.orig`.
- Everything else is new and lives under `pilot/`.

**Files are in** `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i3-r3/pilot/`:
- `loadgen.py`, `sampler.py`, `run_pass.sh`, `start_aquifer.sh`, `analyze.py`, `analyze_ramp.py`, `aggregate.py`
- `aquifer.yml` (rps 70), `aquifer-75.yml`
- `bin/aquifer`, built from aq-lab, 31 MB
- `.l8-key` and `l8-trust/`, which Aquifer generated on start
- Raw data in `results/`:
  - `ramp/` holds the backend benchmark.
  - `baseline-{1,2,3}/`, `aquifer-{1,2,3}/` and `aquifer75-1/` hold the passes. Each has `orders.jsonl` (per-order log from the load generator), `backend.jsonl` (per-request backend log with Aquifer headers), `stats.jsonl` (0.25 s pool samples) and `summary.json`. The Aquifer passes also have `metrics/*.prom` (`/metrics` scrapes every 15 s) and `aquifer.log`.
  - To print all passes side by side: `python3 pilot/aggregate.py`, run from `i3-r3`.