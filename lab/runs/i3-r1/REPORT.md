## Aquifer pilot: orders-api, 2026-11-04

**Summary:** Aquifer fits this problem. With the same sale-day burst, 0 orders failed through Aquifer, against 330 of 5,100 (6.5%) sent straight to orders-api. The small shops' orders landed in about 0.1 s at p99, against about 35 s today. They no longer wait behind bigbox. The cost is that bigbox's own orders now wait in Aquifer's queue (p99 44 s) instead of some of them failing, and partners would get the order id asynchronously instead of in the response.

**Backend:** orders-api (Python `ThreadingHTTPServer`) in front of a simulated Postgres pool: 4 connections, 50 ms per insert, 2 s checkout timeout, then `503`.
- **Benchmark (step 2), open loop, no retries, 8 s per rate:**
  - 40–70 rps: p99 about 60 ms, no waits for a connection.
  - 75 rps: the knee. The pool is full 94% of the time and p99 reaches 99 ms.
  - 80 rps: p99 628 ms.
  - 110 rps: 15% `503`s.
- **Most orders it can write:** about 75 a second.

**Traffic:** a model of `partners.py`, not recorded traffic. Same tenants and rates: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s, for a 30 s burst (5,100 orders). That is about 2.3 times what the database can write. In the baseline, partners keep their real retry behaviour: up to 5 attempts, 0.1 s apart, from 400 client threads.

**Shape:** a single Aquifer node (built from the aq-lab source, version 0.15.1, which matches the skill's pin). No Canalis, because there is only one node. No Inlet, because these are direct API calls, not provider webhooks. L8 is off. Account queues are on, the default, so each partner gets its own queue. Pacing comes from a static YAML ceiling only, with no backend headers.

| | Baseline (direct) | Through Aquifer (pass A1) |
|---|---|---|
| Time saturated (DB pool) | full 99% of samples, requests waiting for a connection 100% | full 68%, waiting 25%, no timeouts |
| 503 rate | 6,771 of 11,541 requests (59%) | 0 of 5,100 |
| Retries sent to backend | 6,441 (2.26 requests per order) | 0 (1.00 request per order) |
| Orders lost | 330 / 5,100 (6.5%), spread evenly across partners | 0 |
| p50 / p99 end to end (all) | 16.7 s / 35.0 s | 22.7 s / 44.1 s |
| Small shops p50 / p99 | 16.6 s / 34.8 s, and 4.7–7.7% of their orders failed | 0.07 s / 0.11 s, none failed |
| bigbox p50 / p99 | 16.7 s / 35.0 s, 6.4% failed | 25.3 s / 44.1 s, none failed |
| Orders written per second | about 75/s (4,770 written) | about 70/s, held at the ceiling (5,100 written) |
| Burst drain after sending stopped | 33.8 s (with 330 lost) | 44.5 s |
| Fairness | all partners fail at the same rate, so small shops share bigbox's congestion | small shops get everything they ask for at once; bigbox gets the rest |
| Cost | 56% of the database's request handling went to requests that failed; each one held a server thread for up to 2 s | no wasted requests; no need for a larger connection plan for a 30 s burst |

**Second Aquifer pass (A2), one change:** `AQUIFER_MAX_PENDING_PER_UPSTREAM=1500` instead of 10,000. In A1 the backlog peaked at 2,921, so the default budget never refused anything. A2 shows what a sale longer than about 100 s would do at the default.
- **Who gets refused:** Aquifer refused only bigbox, 4,394 times (`upstream_queue` 2,571, `queue_fair_share` 1,823). Small shops were never refused.
- **Outcome:** all 5,100 orders landed, with 0 `503`s and 0 duplicate writes. bigbox p99 rose to 54 s.
- **Small-shop p99 of 1.96 s is an artifact of my load generator, not Aquifer.** All tenants shared one client-side cap (250 rps), and bigbox's 429 resubmits filled it. Once Aquifer had accepted a small shop's order, it was written within 0.093 s at p99.

**What didn't improve:**
- **bigbox is slower.** Its p99 went from 35 s to 44 s, because the work that used to fail now waits in the queue.
- **Throughput is slightly lower.** The 70 rps ceiling is about 7% below the 75/s the database can do at most. That headroom is what keeps the pool free of `503`s.
- **The whole burst takes longer to finish:** 74.5 s against 63.8 s, but with 330 more orders written.
- **The partner contract changes.** `POST /jobs` returns a job id, not the `order_id`. Partners would read the result from `GET /jobs/{id}`, a webhook, or `/proxy`. This is the main cost of adopting it.
- **orders-api isn't idempotent.** Aquifer delivers at least once, so orders-api should reject a repeat of the same partner order reference (a unique key) before going live. The pilot saw no duplicates.
- **Today's latency numbers understate the problem.** `partners.py` starts its clock when a client thread picks up an order, so it hides the time orders sit in its own queue. I measured from the moment each order was scheduled.

**Config:**
- **YAML (`pilot/aquifer.yml`):** `127.0.0.1:19400` with `rps: 70` and `max_concurrent: 4`. The rate comes from the benchmark knee. The concurrency is per queue: bigbox needs about 3.5 requests in flight to use its share.
- **Env vars:** `PORT=19401`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1`, `AQUIFER_NODE_ID=pilot-1`. Pass A2 added `AQUIFER_MAX_PENDING_PER_UPSTREAM=1500`.
- **Retries:**
  - Jobs use `max_retries: -1`, so Aquifer keeps retrying until each lands.
  - The caller submits once with `idempotent_key = <run>-<partner>-<n>`. On a 429 it waits for `Retry-After` (5 s) and resubmits, with no backoff of its own. It logs `limit_reason`, `limit` and `current` for every refusal.
- **Backend headers:** none. The skill says to try the static ceiling first, and it already removed every `503`, so the AIMD control loop wasn't needed. I didn't test it.
- **Ports:** orders-api on 19400, Aquifer on 19401.
- **Load limits:** the generator's own cap was 280 rps (250 in A2).
  - Peak backend requests: 219/s in the baseline, 75–77/s through Aquifer.
  - Peak submissions to Aquifer: 252/s in A2.
  - The baseline limiter only spaced requests out within each second. Its peak stayed below the cap, so it didn't hold back how much retry traffic partners send today.
- **Pass length:** each burst was 30 s and each pass took 64–75 s, so every load test stayed under 2 minutes.

**Deviations from the skill:**
- **Short passes on a laptop.** The skill asks for passes of a few hours to a day on an always-on machine; the lab rules required local runs under 2 minutes.
- **Traffic was modelled from `partners.py`,** not replayed from recorded traffic.
- **Metrics were scraped every 5 s, not every 15–30 s,** because the passes were so short.
- **Order completion times come from the backend.** I added a request log to orders-api (off by default). End-to-end times are measured from when the schedule produced each order to its first successful database write. I also checked Aquifer's `/metrics` and each job's `GET /jobs/{id}` status; both agree, with 5,100 completed per pass.
- **Possible CPU noise in A2.** Another lab's load generator (from `lab-runs/i3-r2`, not started by me) started during pass A2, so the laptop's CPU may have been shared. A2's dispatch numbers match A1 closely.

**Docs notes:**
- **Version check:** a source build reports `aquifer dev` from `--version`, so I took the version from `version.go` (0.15.1).
- **Bind address:** Aquifer listens on all interfaces (`*:19401`), and I found no documented setting to bind it to loopback only.
- **Memory limit:** memory admission is off on macOS, and the startup log says so.
- **`.l8-key`:** Aquifer writes it into the current working directory even when L8 isn't in use.

**Raw data and code:**
- **Code:** everything is on git branch `aquifer-pilot` in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i3-r1`. `main` holds the code as I found it; I created both branches with `git init`. The pilot files:
  - `pilot/loadgen.py`
  - `pilot/analyze.py`
  - `pilot/analyze_bench.py`
  - `pilot/aquifer.yml`
  - `pilot/run_aquifer.sh`
  - `app.py`, with the request log added
- **Data:** in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i3-r1/pilot/data/`, which is gitignored. There is one folder per pass: `bench/`, `baseline/`, `aquifer1/`, `aquifer2/`. Each holds:
  - `jobs.jsonl`: one record per order;
  - `backend.jsonl`: every backend request, with the `X-Aqueduct-*` headers it received;
  - `jobs.jsonl.stats`: the pool sampled every 0.25 s;
  - `jobs.jsonl.scrapes/`: `/metrics` and `/health` every 5 s (Aquifer passes only);
  - `aquifer.log` and the Aquifer database (Aquifer passes only).
- **To recompute:** `python3 pilot/analyze.py pilot/data/<pass> baseline|aquifer`.

**Cleanup:** I stopped every orders-api and Aquifer process I started. Nothing is listening on ports 19400–19409. I didn't modify anything in aq-lab; the Aquifer build output went to `lab-runs/i3-r1/pilot/bin/`.