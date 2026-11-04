## Aquifer pilot: orders-api, 2026-11-04

**Short answer:** Aquifer fixes the small-shop problem. Across five Aquifer passes, corner-shop, kiosk and deli lost no orders and their p99 was 0.12–0.38 s. Sent straight to orders-api, about 6–9% of their orders failed and their p99 was about 34 s. No order was lost through Aquifer, and the database saw no 503s and no retries. What it costs: bigbox's orders now wait in a queue, and its p99 is a few seconds worse than today (37.7 s against 34 s). Its orders all land, though, where today 6–9% are dropped. Partners would also get "accepted, queued" back instead of an order_id.

**Backend:** orders-api (Python, ThreadingHTTPServer) on a simulated database pool: 4 connections, 50 ms per insert, 2 s checkout timeout. That gives a hard capacity of about 80 orders/s. The ramp benchmark (no retries) showed:
- flat p99 of 0.06 s up to 70 rps;
- p50 of 0.35 s at 80 rps;
- 503s from 90 rps (1%), rising to 22% at 120 rps.

I put the knee at about 75–80 rps.

**Traffic:** the burst from `partners.py`, replayed on fixed schedule times: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s, for 30 s. That is 5,100 orders per pass, about 2.1 times what the database can take. Every pass was one burst, under 2 minutes including drain. The skill asks for hours of traffic with several bursts; your 2-minute limit and the laptop ruled that out, so read these as single-burst results.

**Shape:**
- Aquifer alone: one node, no Canalis, no Inlet. Partners aren't signed provider webhooks and there's only one node.
- Account queues on (the default), one per partner (`user_id` = partner).
- Version: built from source, `v0.15.1-56-gf4a5df3`. The pilot skill is pinned to v0.15.1, so this build is 56 commits past what the skill describes.
- Pacing signal: a static YAML ceiling, and in two passes also backend AIMD headers driven by the 95th-percentile pool checkout wait.
- Ports: Aquifer on 19500, orders-api on 19501. Peak load was 228 requests/s on the baseline and about 173 on the Aquifer passes, under the 300/s limit.

### Results

"Best Aquifer" is run a5: ceiling 85 rps, `max_concurrent` 8, static. The other Aquifer runs are listed below.

| | Baseline (2 runs) | Best Aquifer (a5) |
|---|---|---|
| Time saturated (seconds with a 503 or a pool wait over 100 ms) | 63 of 63, 64 of 64 | 34 of 69 (pool waits about 0.1 s, no 503s); 2 of 71 with pacing (a4) |
| Orders lost | 429 and 328 (8.4%, 6.4%) | 0 |
| 503s at the database | 58% of 11.2k–11.6k requests | 0 of 5,100 |
| Retries sent | 6,538 and 6,828 (2.2 times the orders) | 0 to the database, 0 from callers (no 429s) |
| p50 / p99 end to end, all orders | 16.5 s / 33.9 s (successful orders only) | 21.1 s / 37.7 s (every order) |
| Orders per hour while busy | about 268k | about 270k (delivered about 75/s) |
| Burst drain time (after the burst ends) | 33 s | 38 s |
| Small shops' p99 / failed orders | 34 s / 40–42 | 0.23 s / 0 (0.12 s with pacing) |
| Worst tenant's p99 | 34.8 s (everyone suffers equally) | 37.7 s (bigbox) |
| Fairness index (Jain's, on completion ratio) | 1.0, because everyone fails equally | 1.0, because everyone completes |
| Cost | 2.2 requests per order; matching this without queueing needs about twice the database connections | Same database plan, no wasted requests |

Each change between Aquifer runs, one at a time:
- **a1** (rps 70, `max_concurrent` 4): bigbox p99 49.7 s, drain 50 s. Once the burst ended and bigbox was the only queue, it got only about 66/s.
- **a2** (`max_concurrent` 8): bigbox p99 42.6 s, drain 43 s.
- **a3** (a2 plus pacing headers): bigbox p99 48.0 s, small shops p99 0.12 s, pool waits about 35 ms. This was slower than a2 because the AIMD loop starts at 40 rps.
- **a4** (rps 85 with pacing): bigbox p99 40.3 s, drain 40.6 s, small shops p99 0.12 s, almost no saturation.
- **a5** (rps 85, static): bigbox p99 37.7 s, drain 38 s, small shops p99 0.23 s.

Across a2–a5, everything else was identical: 0 lost, 0 503s, 0 retries, 0 caller 429s.

### What didn't improve
- **Bigbox's latency got worse, not better.** Its p99 went from about 34 s to 37.7–49.7 s depending on config, and the burst drains 5–17 s more slowly. The baseline's p50 and p99 only count orders that succeeded; through Aquifer, every order is counted.
- **Aquifer delivered a little under its ceiling once a queue ran alone.** It managed about 66/s at rps 70 and about 75/s at rps 85, so the database was never fully used. During the multi-tenant part of the burst at rps 70, it ran slightly over: 71–76/s.
- **The pacing headers added nothing on this backend.** The AIMD loop never cut, because the static ceiling already kept pool waits under its 100 ms threshold. All it did was trade some bigbox drain time for lower small-shop latency.
- **An honest fit caveat:** the database ceiling is fixed by your plan. The skill's own fit check calls a ceiling that never changes overkill. Here the win comes from fair per-tenant queueing and from replacing partners' immediate 5-attempt retries, not from riding elastic capacity. A per-partner queue or rate limit inside orders-api would capture much of the same benefit. Aquifer gives you that off the shelf, plus durability and the metrics.
- **The integration changes.** Partners would get Aquifer's `201` with a job_id instead of an order_id. orders-api needs a small intake that submits each order to Aquifer with `idempotent_key` = partner + order ID, and the database write has to be idempotent, because delivery is at-least-once. I added that dedupe; no duplicates showed up.
- **Two findings about `partners.py` itself:**
  - Its latency clock starts only when a worker thread picks up the order. Its 4 s p50 hides about 12 s of local queueing; measured from when each order was scheduled, the p50 is 16.5 s.
  - It actually sends bigbox at about 124/s, not 150/s.

### Config
`pilot/aquifer.yml` sets the host key `127.0.0.1:19501`: `rps` 85 (70 in a1–a3), `max_concurrent` 8 (4 in a1). Defaults are `rps` 2 and `max_concurrent` 1. Env vars:
- `PORT=19500`, a fresh `DB_PATH` per run, `CONFIG_PATH`;
- `AQUIFER_METRICS_TENANT_LABELS=true`;
- `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1`.

Jobs were submitted with `max_retries: -1` and no `execute_before`; callers waited `Retry-After` on any 429, with no attempt cap. None arrived. Everything else stayed at the defaults I chose:
- account queues on;
- backlog budget 10,000 (the peak backlog was about 3,300, about 20 s at 170/s);
- Pebble flush 100 ms;
- memory limit unset (macOS can't read it);
- inbound `X-Aqueduct-Rps` not honored by callers, because partners don't honor it today.

Pacing (`app.py --pace`, `pacing.py`): budget starts at 40, +5/s below ssthresh, +1/s above it, ×0.6 when the p95 pool wait goes over 100 ms or any checkout times out; ceiling 90, divided by `X-Aqueduct-Active-Queues`. The baseline used today's retry behaviour: 5 attempts, 0.1 s apart.

Where I worked around the docs or found gaps:
- Aquifer listens on every interface (`*:19500`). There's no way to bind it to loopback only.
- `max_concurrent` 4 quietly capped a lone queue at about 66/s. The docs don't warn that, with a fast backend, `max_concurrent` can bind before `rps` does.
- Delivering under and over the ceiling (above) isn't described in the docs.

### Raw data
Everything is in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i2-r2/pilot/runs/`, one directory per run: `b0-partners-py`, `b1-direct`, `b2-direct-repeat`, `a1-static`, `a2-conc8`, `a3-conc8-pace`, `a4-rps85-pace`, `a5-rps85-static`. Each has:
- `orders.jsonl`, one line per order from the load generator;
- `backend.jsonl`, one line per request with the incoming `X-Aqueduct-*` headers and the pool wait;
- `scrapes.jsonl`, `/metrics`, `/health` and `/stats` every 5 s;
- `summary.json` and `meta.json` (requests per second);
- `aquifer.yml` and the Aquifer logs.

The benchmark is in `bench.json` in the same folder. Aquifer's data directories and `.l8-key` files are in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/aq-pilot-run/`, so nothing landed in your repo. The binary was built there too; nothing in the Aquifer checkout (aq-lab) was modified.

### Changes in orders-api
I ran `git init` in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i2-r2`. `main` holds the code as I found it; branch `aquifer-pilot` (commit 608836f) has the pilot changes:
- `app.py`: per-request logging (off by default), idempotent writes keyed on an `idem` field, and an optional `--pace` flag that sends the headers;
- new `pacing.py`;
- new `pilot/` folder with `bench.py`, `loadgen.py`, `analyze.py`, `pass.sh`, `run-aquifer.sh` and `aquifer.yml`.

By default the service behaves as before. `partners.py` is unchanged.

### Cleanup
Every process I started is stopped, and nothing listens on ports 19500–19509. One mid-pilot mistake: a shell loop mis-split its arguments, so the first try at runs a4 and a5 ran without the pacing flag. I deleted those results and reran both. A `loadgen.py` still running on this machine belongs to a different directory (`lab-runs/i2-r3`) and isn't mine, so I left it alone.