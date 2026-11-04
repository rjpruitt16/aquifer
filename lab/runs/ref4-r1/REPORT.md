## Aquifer pilot: orders-api, 2026-11-04

I ran the whole pilot on this laptop using the `aquifer-pilot` skill from `aq-ref/skills/aquifer-pilot/SKILL.md`. **Result: Aquifer fixes the small-shop problem.** With it in front, every order from every partner landed. The small shops' orders finished in about 0.1 s even at p99, against about 24 s now with roughly 1 in 10 failing. The cost lands on bigbox: all of its orders land too, but they wait longer (median about 19 s instead of 10.6 s). The database's ceiling of about 75–80 orders/s doesn't move either way.

**Backend:** orders-api (`app.py`) in front of a simulated 4-connection Postgres pool (50 ms per insert, 2 s checkout timeout, about 80 orders/s in theory). I ramped single attempts with no retries:

| Offered (req/s) | Sent (req/s) | p50 / p99 | Errors | Pool full |
|---|---|---|---|---|
| 60 | 51.5 | 55 / 57 ms | 0% | 0% |
| 80 | 69 | 55 / 60 ms | 0% | 56% |
| 90 | 77 | 0.20 / 0.37 s | 0% | 94% |
| 100 | 84 | 0.67 / 1.3 s | 0.2% | 97% |
| 120 | 100 | 1.8 / 2.1 s | 10.9% | 94% |

The knee is about 77 req/s.

**Traffic:** your `partners.py` schedule, unchanged: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s for 30 s. Python's sleep loop actually produces about 4,000–4,500 orders per burst, about 135/s. Through Aquifer the load generator stands in for a trusted intake: each partner is its own tenant (`user_id`), and each order has an idempotency key. That is 4 tenants and one 30 s burst per pass. Your rules (under 2 minutes per test, at most 300 req/s) mean each pass is about 1 minute, not the hours the skill asks for. The busiest second on any pass was 220 req/s.

**Shape:** one Aquifer node, no Canalis, no Inlet. Account queues were on (the default). It's built from source at `v0.15.1-61-g0dbeff3` (`aquifer dev`); the skill is pinned to v0.15.1. The pacing signal is a static YAML ceiling with no backend headers, because headers made things worse (see below).

| | Baseline (2 runs) | Through Aquifer (ceiling 80, 2 runs) |
|---|---|---|
| Orders completed | 90.5–90.6% (about 380 lost per burst) | 100% |
| Small shops completed | 89–91% | 100% |
| Pool saturated (all 4 connections in use) | 93–96% of the time, about 380 threads waiting | 75–86%, at most 3 waiting |
| Share of backend attempts that were 503s | 59.7% | 0% |
| Retries sent to the backend | about 5,000 per burst (about 9,000 attempts for about 4,000 orders) | 0 (one attempt per order) |
| p50 / p99, all orders | 10.6 / 23.5 s | 17.2 / 30.5–30.8 s |
| Small shops' p50 / p99 | 10.2–10.8 / 20.7–24.0 s | 0.07 / 0.10–0.11 s |
| bigbox p50 / p99 | 10.5–10.8 / 23.2–23.7 s | 19.1–19.5 / 30.5–30.8 s |
| Orders per hour while saturated | about 253k, minus the 9.4% lost | 240k–263k, all of them |
| Drain after the burst ends | about 22 s (failed orders just drop out) | about 31 s |
| Worst tenant's p99 | 24.0 s | 30.8 s (bigbox) |
| Jain index on each tenant's completion ratio | 1.0 | 1.0 |
| Cost | n/a | No bigger database plan needed. About 5,000 wasted 503 attempts per burst go away, each of which had held a server thread for up to 2 s. |

**Why the small shops fail today:** every order, from any shop, waits in the same pile for the same 4 connections. A shop sending 5/s had the same roughly 10% failure rate and 24 s p99 as bigbox. The partners' "retry at once, up to 5 times" loop more than doubles the requests hitting the backend.

The Jain index of 1.0 in both columns hides a big difference. In the baseline, every tenant fails at the same rate, which is exactly your complaint. Through Aquifer, every tenant gets all of its orders through.

Aquifer's own `aquifer_fairness_jain` gauge read 0.44–0.72 during the burst. It measures raw throughput per queue and doesn't account for demand, so bigbox doing about 54/s next to shops doing 5–10/s looks "unfair" to it even though every shop got its full demand. The baseline scores about 0.33 on the same measure.

**Other passes (raw data is in the branch):**
- **YAML ceiling 70 instead of 80** (2 runs): the same fairness and 100% completion, but about 249k orders/h, p99 34 s and the pool full 65% of the time. 80 is the better setting. Alone, bigbox's queue tops out at about 72/s, because 4 requests in flight at about 55 ms each can't go faster.
- **Ceiling 80 plus backend pacing headers:** worse. Throughput fell to 238k/h, bigbox's p99 rose to 35.9 s, and the small shops' p99 rose to 0.4–0.9 s. My split of the budget reserved headroom for the small shops that they never used, which held bigbox at about 44/s. Aquifer's own sharing already gives unused capacity to bigbox. Recommendation: keep the static ceiling and no headers, unless the database's capacity actually changes, for example when other writers share it.
- **Small backlog budget** (`AQUIFER_MAX_PENDING_PER_UPSTREAM=1500`, about 10 s of peak intake; a stress test, not a recommendation): Aquifer refused only bigbox, 1,124 times for `queue_fair_share` and 265 for `upstream_queue`. The small shops were never refused. The load generator waited `Retry-After` (5 s) and resubmitted the same key, and all orders still completed.

**What didn't improve:**
- **Bigbox waits longer.** Its median goes from about 10.6 s to about 19 s and its p99 from about 23.5 s to about 31 s. In return none of its orders are lost (today about 9% are). This is what fair turns cost.
- **Throughput stays at the database's limit.** Aquifer queues the excess; it doesn't add capacity. To make bigbox faster you need more connections or faster inserts.
- **Orders become asynchronous.** Partners get a job id instead of an immediate `201` with an `order_id`. They'd need to poll `GET /jobs/{id}`, receive a webhook, or use `POST /proxy`, which answers directly when there's room and otherwise holds the connection open as a stream, up to about 30 s at this burst size. This is the biggest change for partners' integrations.
- **Partners can't call Aquifer directly.** Aquifer sends real HTTP requests to whatever URL a job names, so it must stay private. orders-api needs a thin intake endpoint that authenticates the partner, sets `user_id` to the partner, fixes the target URL, and uses the partner's order id as `idempotent_key`. Also set `AQUIFER_ALLOWED_URL_DOMAINS`.
- **Inserts must be idempotent.** Delivery is at least once, and `db.save_order` doesn't deduplicate today. Add a unique constraint on (partner, partner order id). I saw 0 duplicate writes in these runs.
- **Durability:** a crash can lose about the last 100 ms of accepted orders. The fix is for callers to resubmit with the same key, not to set the flush interval to 0.
- **Backlog budget:** the default of 10,000 jobs is about 70 s at bigbox's peak rate. These bursts peaked at about 2,300 queued. For sales that last minutes, set `X-Aqueduct-Max-Backlog` to about `150 × <seconds to absorb>`, for example 90,000 for 10 minutes. Otherwise bigbox will get 429s.

**Config:**
- Aquifer env: `PORT=19101`, `DB_PATH=<run>/aq-work/aquifer-data` (Pebble), `CONFIG_PATH=pilot/aquifer-80.yml` (`127.0.0.1:19100: rps 80, max_concurrent 4`), `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-node-1`. Everything else was left at its default: flush interval 100 ms, backlog budget 10,000, `Retry-After` 5 s, account queues on.
- Jobs were sent with `max_retries: -1` and `execute_before` set to now + 10 min. On a 429, callers waited `Retry-After` and resubmitted the same key, with no backoff of their own.
- The baseline used the partners' own retry behaviour (5 attempts, 0.1 s apart).
- Choices I made myself:
  - Ports: backend on 19100, Aquifer on 19101.
  - I scraped `/metrics` every 5 s instead of every 15–30 s, because the passes were short.
  - My load generator caps itself at 300 req/s, as a safety guard.
  - Latency is measured from when the schedule issued each order. My first two baseline runs measured from when a client worker picked the order up, which leaves out the client's own queueing. They were superseded by the rerun, and the table uses the rerun.
- Aquifer logged that macOS memory can't be read, so its memory limit was unbounded.

**Caveats:**
- The passes are about 1 minute each, not hours, and the "database" is simulated.
- Other lab runs were using this laptop at the same time, which likely explains some of the noise between repeat runs (for example 240k vs 263k orders/h).
- Aquifer listened on all network interfaces; it has no loopback-only setting.

**Two documentation gaps:**
- The `X-Aqueduct-Flow-Rate` header showed the configured rate on every queue: 70 on each of 4 queues, which adds up to 280. Actual dispatch was about 73/s. A backend that adds these headers up would badly overestimate its load.
- At ceiling 70, total dispatch during the burst was 72–74/s, slightly over the YAML total.

**Raw data and code:** everything is on a git branch, `aquifer-pilot`, in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/ref4-r1`. The folder wasn't a git repository, so I initialised one: `main` holds your code exactly as I received it.
- `pilot/runs/<pass>/` has, for each pass:
  - `jobs.jsonl`: one line per order.
  - `backend.jsonl`: every backend attempt with the `X-Aqueduct-*` headers it received, plus samples of the connection pool every 250 ms.
  - `attempts.jsonl`: requests per second.
  - `metrics.scrapes` and `metrics.final`: Aquifer's metrics.
  - `aquifer.log`.
  - The baseline is `07-baseline-sched-1` and `-2`. The Aquifer passes in the main table are `04-aquifer-static80` and `08-aquifer-static80-sched`. The others are `01-benchmark`, `03-aquifer-static-1/2`, `05-aquifer-80-pacing` and `06-aquifer-80-budget1500`. `02-baseline-*` were superseded, as described under Config.
- Code: `pilot/loadgen.py`, `pilot/analyze.py`, `pilot/table.py`, `pilot/run_aquifer_pass.sh`, `pilot/aquifer.yml`, `pilot/aquifer-80.yml` and `pilot_hooks.py`.
- `app.py` gets small changes, all off by default: logging only with `--pilot-log`, pacing headers only with `--pacing`.

I made no changes in `aq-ref`; its `git status` is clean. Every process I started is stopped, and nothing is listening on ports 19100–19109.