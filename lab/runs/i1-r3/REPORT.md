# Aquifer pilot: orders-api, 2026-11-04

## Short version

With Aquifer in front of the database write, the small shops stopped failing. Their orders reached the database in 0.07 s at the median and 0.11 s at p99 during the sale burst. Without it the same orders took 11 s at the median and 24 s at p99, and 7 to 10% of them failed after five attempts. Every order from every partner was written, and orders-api's database sent no 503s.

The cost lands on bigbox. Its orders now wait in its own queue instead of failing: p50 went from 11 s to 18.5 s and p99 from 24 s to 29.5 s, but none of its 3,400+ orders failed (before, about 270 per burst did). Aquifer adds no database capacity. The pool still tops out around 70 writes/s, so a burst that sends ~135 orders/s for 30 s still takes about a minute to clear.

There's one change for partners to plan for: orders-api now answers `202 Accepted` with a job id within about 10 ms, instead of `201` with an `order_id` after the write. Partners that need the `order_id` right away would need a status lookup or a webhook (details below).

## Setup

**Backend:** orders-api (Python `ThreadingHTTPServer`) in front of a simulated managed Postgres: a pool of 4 connections, 50 ms per insert, 2 s checkout timeout. Benchmarked knee: about **70 writes/s**. Up to ~67/s, p99 stays at 60 to 73 ms. At ~76/s, p50 climbs to 0.36 s, and at ~89/s offered, 5% of writes time out (table below).

**Traffic:** a model of the sale burst built from `partners.py`, with the same partner rates, the same client (400 threads, 5 attempts 0.1 s apart, 5 s timeout) and the same retry behavior. Each burst ran 30 s and covered 4 tenants: bigbox at 150/s nominal, corner-shop 10/s, kiosk 5/s and deli 5/s. The Python scheduler actually reached ~115/s for bigbox and ~133/s in total, the same on every pass. Every pass sent about 4,000 orders.

**Shape:** Aquifer alone (no Canalis, no Inlet), one node, account queues **on** (the default), Aquifer v0.15.1 built from the local checkout. The pacing signal was the static YAML ceiling. A second variant added backend pacing headers and is reported separately below.

How it's wired, on branch `aquifer-pilot` (the `git init` here is mine, and `main` holds the code as received):
- Partners still call `POST /orders`. With `--aquifer URL`, orders-api submits the order to Aquifer as a job and answers `202`. The job uses `user_id` = partner, `idempotent_key` = `<partner>-<order>`, `max_retries: -1`, and its target is the new `POST /internal/orders`.
- Aquifer calls `POST /internal/orders`, which does the same database write. On a pool timeout it answers `429` + `Retry-After: 1` instead of `503`, because the database is busy, not broken.
- Without the new flags, the app behaves exactly as before, and the baseline ran that way.

Fit: this is the "database with bursty writers" case. The ceiling (4 connections) is fixed by the plan, but sales push demand past it only for a while, so a queue can absorb the overflow and drain it afterwards. Inlet isn't needed because these are partner API calls, not signed provider webhooks. Canalis isn't needed because there is one node.

### Backend benchmark (step 2)

Single tenant, no retries, 7 s per step (`pilot/data/bench-summary.txt`). "Target" is what the generator aimed for, and "ok/s" is what it actually achieved.

| target/s | ok/s | errors | p50 | p99 | pool fully busy |
|---|---|---|---|---|---|
| 60 | 45 | 0% | 59 ms | 61 ms | 0% |
| 80 | 61 | 0% | 58 ms | 61 ms | 37% |
| 90 | 67 | 0% | 59 ms | 73 ms | 76% |
| 100 | 76 | 0% | 359 ms | 643 ms | 86% |
| 120 | 85 | 5.1% | 1.54 s | 2.07 s | 97% |

## Results

Baseline = orders-api as it runs today. Aquifer = static ceiling of 70 rps. Each column is two runs (base/base2 and aq1/aq2), and the runs agreed within a few percent. The figures are from base2 and aq2.

| | Baseline | Through Aquifer |
|---|---|---|
| Time DB pool fully busy (all 4 connections) | 95% | 77% |
| Max requests waiting for a connection | 390 | 3 |
| 503s from orders-api / 429s from Aquifer | 5,428 503s (59% of attempts) | 0 / 0 |
| Orders failed after all attempts | 307 of 4,016 (7.6%) | 0 of 4,031 |
| Retries sent (attempts beyond the first) | 5,121 (2.28 attempts/order) | 0 from partners, 0 from Aquifer to the DB |
| Partner-visible response, p50 / p99 | 11.1 s / 24.0 s | 0.00 s / 0.01 s (202 at intake) |
| Order written to DB, p50 / p99: bigbox | 11.1 s / 24.1 s | 18.5 s / 29.5 s |
| Order written to DB, p50 / p99: small shops | 10.9–12.0 s / 23.5–24.8 s | 0.07 s / 0.10–0.11 s |
| Writes/s while busy | 70.6 | 67.5 |
| Burst drain time after sending stopped | 22.6 s (with 307 orders dropped) | 29.7 s (nothing dropped) |
| Worst tenant's p99 | 24.8 s (deli) | 29.5 s (bigbox) |
| Fairness index (Jain over each tenant's written/sent) | 0.9997 | 1.0 |
| Estimated cost | no change in DB plan needed to stop small-shop failures (see below) | |

About the fairness row: the baseline scores as "fair" because every tenant failed and waited about equally, and that equal suffering is the complaint. The useful fairness number here is the small shops' p99: 24 s before, 0.1 s after. Aquifer's own `aquifer_fairness_jain` read 0.46 to 1.0 during the burst. It's computed over queues with work waiting, weighted by jobs finished, and with tenants whose demand differs 30× it mostly reflects demand, not unfairness.

What Aquifer's metrics showed (`pilot/data/aq2-metrics/`):
- `aquifer_dispatch_attempts_total` counted only `2xx`.
- `aquifer_admission_rejected_total` stayed at zero.
- The backlog peaked around 1,900 jobs, all of them bigbox's.
- Most of the time `X-Aqueduct-Active-Queues` was 1 to 2.5, because the small shops' queues emptied as fast as they filled. That let bigbox use nearly the whole 70 rps ceiling, so capacity wasn't left idle.

### Variant: backend pacing headers (aq3)

The only change from aq2 was turning on `--pacing`. `/internal/orders` returned `X-Aqueduct-Rps` from an AIMD loop:
- Signal: p95 connection-wait over 0.5 s windows, cut ×0.7 above 25 ms (at most once per second).
- Growth: +5 below the remembered threshold and +1 above it.
- Range: 10 to 80, divided by `X-Aqueduct-Active-Queues` as the skill recommends.

It made things **worse**: 57 writes/s instead of 67.5, and bigbox p50/p99 of 28.9 s / 40.3 s instead of 18.5 s / 29.5 s. Small shops were unchanged at about 0.06/0.09 s. `pilot/data/aq3-timeline.txt` shows the cause. Dividing the budget evenly across ~2.3 active queues capped bigbox at ~40 rps while the small queues used only ~20 rps in total, which left capacity unused. The static ceiling was enough here (no errors, at most 3 requests waiting for a connection), so I'd leave headers off. If you add them later, divide only the share the other queues actually use, not an even split.

## What didn't improve

- **Throughput.** The database still does about 67 to 70 writes/s. Aquifer moves the overflow into a queue, so bigbox waits longer per order (p50 +7 s, p99 +5.5 s) in exchange for zero failures. A sale that runs for many minutes at ~135/s grows that backlog by ~65 jobs/s; the default 10,000-job budget is about 2.5 minutes of that. For longer sales, set `AQUIFER_MAX_PENDING_PER_UPSTREAM` or `X-Aqueduct-Max-Backlog` to `peak intake rate × the seconds you want to hold`. Past that, the only thing that shortens bigbox's wait is more database connections.
- **The partner contract.** Partners get `202` + `job_id` instead of `201` + `order_id`. If they need the `order_id` synchronously, the options are `GET /jobs/{id}` from orders-api, a completion webhook, or Aquifer's `POST /proxy` (tries directly, falls back to queue + SSE stream). I tested none of these.
- **Duplicates.** Delivery is at-least-once. This run saw 0 duplicate writes, but the real insert needs a unique key on (partner, order number) so a redelivery is a no-op.
- **Durability.** An accepted order lives on Aquifer's disk. A crash can lose roughly the last 100 ms of accepted jobs. Partners' existing retries carry the same order number, so a resubmit with the same `idempotent_key` repairs that. Keep the database as the record of which orders exist.

## Config

Aquifer (`pilot/aquifer.yml` and `pilot/run_aquifer_pass.sh`):
- `127.0.0.1:19300`: `rps: 70` (the benchmarked knee) and `max_concurrent: 4` (the pool size, so a lone tenant can use the whole pool). Defaults are `rps: 2` and `max_concurrent: 1` and aren't used.
- `PORT=19301`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-1`, and a fresh `DB_PATH` per pass.
- Everything else is at its default: account queues on, Pebble with a 100 ms flush, a backlog budget of 10,000, Retry-After 5 s, and inbound pacing benchmark 400.
- Memory admission was off because macOS memory can't be read (logged at startup).

Defaults I chose without asking:
- Jobs: `max_retries: -1` with no `execute_before`.
- Partner client: unchanged, so it still retries 5 times 0.1 s apart and ignores `Retry-After` and the `X-Aqueduct-Rps` intake hint. It never received a 429, so this didn't matter.
- orders-api: forwards Aquifer's 429 and Retry-After to partners if one ever occurs, and ignores the `X-Aqueduct-Rps` intake hint.
- Ports: orders-api on 19300, Aquifer on 19301.
- Safety cap: a 300 attempts/s limiter in the load generator. The baseline peaked at 206 attempts/s, so the cap never limited the rate. It only spaced attempts by about 3 ms each.

Retry settings by pass:
- Baseline: partners make 5 attempts, 0.1 s apart.
- Aquifer passes: the same partner client, plus Aquifer `max_retries: -1`. Aquifer's own retries were never used (0 non-2xx).

### Differences from the pilot skill

These were forced by your limits:
- Each pass was one 30 s burst plus drain, about 1 minute in total, not hours. There were no repeated bursts and no hours-long soak.
- It ran on this laptop, not an always-on machine. Other pilot runs were active on the same laptop, which may add noise. Both repeated pairs agreed closely.
- `/metrics` was scraped every 5 s instead of every 15 to 30 s, because each pass was short.

### Gaps I hit in Aquifer's docs and tooling

- `aquifer --version` prints `dev` for a source build. I read the version (0.15.1, which matches the skill's pin) from `version.go`.
- Aquifer writes or reads `.l8-key` in its working directory, configurable by `L8_KEY_PATH`, and that variable isn't in `docs/pages/configuration.md`. On my first Aquifer pass it picked up an existing `.l8-key` from my shell's working directory, which was outside the pilot. It only read the file and changed nothing. Later passes ran from `pilot/data`.
- Early in the burst `aquifer_flow_rate_rps` read 210 with 3 active queues, which is above the 70 rps ceiling. The docs say the per-queue rates are scaled so their sum stays within the ceiling. The database never saw more than ~71 writes/s, so it was harmless here, but the metric and the docs disagree.
- The skill's advice to advertise `budget ÷ active queues` cost throughput when tenants' demand is very uneven (see aq3).

## Raw data

Everything is in `pilot/data/`:

| Data | Files |
|---|---|
| Per-order load generator logs, first line meta incl. attempts per second | `base.jsonl`, `base2.jsonl`, `aq1.jsonl`, `aq2.jsonl`, `aq3.jsonl` |
| Backend logs: every write with its connection wait, every `X-Aqueduct-*` request header, pool samples every 100 ms, intake refusals | `*-app.jsonl` |
| Aquifer `/metrics` and `/health` scrapes every 5 s | `aq*-metrics/` |
| Aquifer logs | `aq*-aquifer.log` |
| Summaries | `*-summary.txt` / `*-summary.json`, `bench-summary.txt`, `aq2-timeline.txt`, `aq3-timeline.txt` |
| Scripts | `pilot/loadgen.py`, `pilot/run_baseline_pass.sh`, `pilot/run_aquifer_pass.sh`, `pilot/analyze_bench.py`, `pilot/analyze_burst.py`, `pilot/hdr_timeline.py` |

To rerun a pass: `pilot/run_baseline_pass.sh NAME` or `pilot/run_aquifer_pass.sh NAME [--pacing]`, then `python3 pilot/analyze_burst.py pilot/data/NAME.jsonl pilot/data/NAME-app.jsonl`.

All processes I started (orders-api, Aquifer, load generators) have been stopped, and nothing is listening on 19300 to 19309.
