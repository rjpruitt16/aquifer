## Aquifer pilot: orders-api, 2026-11-04

**Short answer:** Aquifer fixes the small shops' failures. In the test, orders from corner-shop, kiosk and deli went from a 6–10% failure rate with a 4.2 s median to no failures, about a 0.1 s median and a p99 under 0.4 s. Not one of bigbox's orders failed either. The cost is that bigbox now waits: its median was 12.6 s and its p99 was 22 s. Aquifer doesn't add database capacity. The 4-connection pool still writes about 70–75 orders/s either way. What changes is that the excess waits in line instead of being turned away with a 503, and bigbox's burst can no longer crowd out the small shops.

Before you adopt it, two things matter:
1. **Partners' integrations would have to change.** Today they expect an immediate `201` with an `order_id`. Behind Aquifer they get "queued" back right away and the confirmation comes later, by webhook or by polling. Bigbox's 20 s+ waits also exceed their 5 s client timeout. So orders-api would need an intake that answers `202` plus a reference.
2. **A cheaper fix exists if you only care about the small shops.** A per-partner limit inside orders-api, such as reserving a pool connection for small partners, would probably stop their failures with no new service. I didn't test this. Bigbox would keep getting 503s under it.

**Backend:** orders-api (Python `ThreadingHTTPServer`) with a simulated Postgres pool: 4 connections, 50 ms per write, 2 s wait for a free connection, then 503. That gives a theoretical ceiling of 80 writes/s. Step benchmark (open loop, one attempt per request, 7 s per step):

| Rate sent | Result |
|---|---|
| up to 70 rps | flat, p99 about 0.06 s |
| 80 rps | p99 0.53 s, the knee |
| 90 rps | p99 1.9 s, a few 503s |
| 100 rps | 24% errors, about 76 orders/s succeed |
| 120 rps | 37% errors, about 75 orders/s succeed |

**Traffic:** the sale shape modeled on `partners.py`: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s. The scheduler's sleep overhead makes real rates about 125/s for bigbox and 20/s for the three small shops together, roughly 2,860–2,960 orders per pass. Each sale lasted 20 s (`partners.py` defaults to 30 s), so every test stayed under 2 minutes. Each pass was run once.

**Shape:** Aquifer alone, one node. No Inlet (these aren't provider webhooks) and no Canalis (one node). Account queues on (the default). The pacing signal was a static YAML ceiling; pacing headers were tested separately and didn't help (below). Built from the aq-ref checkout: `version.go` says 0.15.1, git says `v0.15.1-54-gcc79d2f`, and the binary reports `dev`.

### Baseline vs the recommended config (pass C)

| | Baseline (direct, partner retries) | Through Aquifer (pass C) |
|---|---|---|
| Time saturated (pool wait over 0.1 s, or a 503) | 36 of 36 s | 1 of 43 s |
| 503 rate at orders-api | 55.7% (3,364 of 6,035 requests) | 0% |
| Orders that failed for good | 186 of 2,857 (6.5%); small shops 6.1–10.1% | 0 of 2,963 |
| Retries sent to orders-api | 3,178 | 0 |
| Peak requests/s at orders-api | 209 | 77 |
| p50 / p99, small shops | 4.16 s / 10.49 s (successful orders only) | 0.09–0.10 s / 0.12–0.14 s |
| p50 / p99, bigbox | 2.18 s / 10.49 s (successful orders only) | 12.6 s / 21.8 s |
| Orders written per second | 74.5 (about 268k/h) | 70.6 (about 254k/h) |
| Burst drain time after the sale ends | 5.2 s, with 6.5% of orders dropped | 22 s, every order written |
| Worst tenant's p99 | 10.5 s (every tenant) | 21.8 s (bigbox) |
| Jain index, completed ÷ demand | 1.00 | 1.00 |
| Estimated cost | Same database plan; failures cost partner orders and support load | Same database plan plus one small Aquifer process; no upgrade needed for small shops |

Aquifer accepted jobs fast: p50 0.7 ms, p99 7.5 ms.

### Other passes (one change each)

- **A: YAML ceiling only** (`rps: 70`, `max_concurrent: 4`). Almost the same as C: no failures, small shops' p99 ≤0.39 s, bigbox p50 13.4 s, 68.1 writes/s.
- **B: A plus backend pacing headers.** Worse: 52.7 writes/s and a bigbox p50 of 22 s. My controller read `X-Aqueduct-Queue-Depth`, which turned out to be node-wide (first docs bug below). Every partner looked backlogged, so each was offered a quarter of the budget.
- **B2: B with the controller fixed** to judge demand from what each queue actually used. Same as A: 67.2 writes/s, bigbox p50 14.6 s. The headers add nothing here because the database's capacity is fixed and the static ceiling never saturated it. They'd matter if capacity changed, for example on a database slowdown, which I didn't test.
- **C: `max_concurrent` raised from 4 to 6.** 70.6 writes/s and slightly better small-shop p99s. I recommend this config. The gain over A is a few percent from one run each, so it may be noise.
- **D: C with `AQUIFER_MAX_PENDING_PER_UPSTREAM=1000`**, about 6 s of peak intake. This stands in for a long sale that fills the backlog budget. Only bigbox was refused: 696 `queue_fair_share` and 45 `upstream_queue` 429s. It resubmitted after `Retry-After` and every order was eventually written. The small shops were never refused (p99 0.13 s).

### What didn't improve

- **Throughput.** The database writes the same ~70–75/s. Keeping Aquifer's ceiling below the knee costs about 5% peak write rate compared with flooding.
- **Bigbox's wait.** Its p99 doubled (10.5 s to 22 s). In exchange none of its orders fail, where 6.2% failed before.
- **Long sales.** Bigbox's real sale rate is about twice capacity. If that lasts, its backlog grows by roughly 50–90 orders/s, fills the default 10,000-job budget within a few minutes, and then bigbox gets 429s (pass D shows this). Only a bigger database plan fixes sustained overload. I could only test 20 s sales.
- **Pacing headers.** No measurable gain at a fixed capacity.

### Docs and implementation issues found

1. The skill says `X-Aqueduct-Queue-Depth` is "this queue's jobs still waiting". It's actually the node-wide queued count: `internal/dispatch/account_queue.go:662` passes `store.Counts().QueueDepth` (`internal/storage/pebble.go:569`). The kiosk's nearly empty queue reported 755. A backend that trusts the docs mis-paces, which is what happened in pass B.
2. `X-Aqueduct-Flow-Rate` showed 70 on every queue while 4 were active. The measured total ran at about 74–77/s against the 70 YAML ceiling. That's harmless here, but it doesn't match "sum stays within it" exactly.
3. There's no setting for the address Aquifer listens on. It binds `":"+PORT`, so all interfaces. As a guard I set `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1`.
4. The docs don't say whether `idempotent_key` reaches the target, so I put the key in the order body.
5. On macOS, `AQUIFER_MEMORY_LIMIT_MB` can't be detected, and startup logs that memory is unbounded.

### Config

- **YAML (C):** `127.0.0.1:19101: rps 70, max_concurrent 6`. The rate sits just under the benchmarked knee.
- **Env:** `PORT=19100`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-1`, `AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1`. Everything else is default: backlog budget 10,000 (1,000 in D only), `Retry-After` 5 s, Pebble 100 ms flush.
- **Retries:** baseline used partners' own behavior (immediate retry after 0.1 s, 5 attempts). The Aquifer passes used `max_retries: -1` with `execute_before` = submit + 10 min. Callers submitted once and resubmitted with the same key after `Retry-After`, uncapped until the test ended.
- **Defaults I chose without asking:**
  - a 20 s sale;
  - `/metrics` scraped every 5 s, because runs are under 2 minutes (the skill says 15–30 s);
  - runs on the laptop, not an always-on machine;
  - an idempotency check on the order key added to orders-api, so at-least-once delivery can't create duplicate orders (none occurred).
- **Ports used:** 19100 (Aquifer) and 19101 (orders-api).
- **Load:** stayed under 300 req/s. Baseline peaked at 209/s at orders-api. In the Aquifer passes, orders-api saw at most 78/s and Aquifer received about 125–185 submissions/s (estimated, not counted per second).

**Caveats:** single runs only. Other agents' load tests were running on this machine at the same time (other directories, ports 19000–19600), so CPU contention may have moved the numbers somewhat. The pilot lasted minutes, not the hours the skill suggests.

### Raw data and code

Everything is in /private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/ref2-r1 on git branch `aquifer-pilot`. `main` holds the code as received, committed as-is.

- **Runs:** `runs/bench`, `runs/baseline`, `runs/aqA`, `runs/aqB`, `runs/aqB2`, `runs/aqC`, `runs/aqD`. Each has `jobs.jsonl` (per-job log from the load generator) and `backend.jsonl` (per-request log from orders-api, including the `X-Aqueduct-*` headers received). The Aquifer runs also have `scrapes/`, `metrics-final.txt` and `aquifer.log`.
- **Code:**
  - `pilot/loadgen.py`: load generator (benchmark ramp, baseline, and through-Aquifer modes)
  - `pilot/analyze.py`: per-tenant analysis
  - `pilot/run_aquifer_pass.sh`: starts orders-api, Aquifer and the scraper for one pass, then stops them
  - `pilot/aquifer.yml`, `pilot/aquifer-c.yml`: the YAML configs
  - `app.py`: added `--log`, `--pace` and the order-key idempotency check; `db.py`: records pool wait time
- **Side effect:** Aquifer created `.l8-key` in that directory. It's gitignored.

All processes I started are stopped and nothing is listening on 19100–19109. aq-ref was not modified.