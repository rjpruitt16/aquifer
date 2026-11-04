## Aquifer pilot: orders-api, 2026-11-04

**Result:** Putting Aquifer in front of orders-api fixed the small shops' problem in this test. Across two runs each, the small shops lost 3 to 9% of their orders when calling orders-api directly. Through Aquifer they lost none, and their slowest orders finished in about 0.1 s instead of about 35 s. Bigbox also lost nothing (it lost 7% directly), but its orders took longer. Aquifer doesn't add database capacity; it decides who waits for it.

This was a short laptop pilot of 30-second sale bursts, not the hours-to-a-day run the skill asks for. That was required by your 2-minute and 300 requests/s limits.

**Backend.** orders-api is a 4-connection database pool, each write holds a connection for 50 ms, and a request gets a 503 after waiting 2 s for a connection. The capacity ramp (`pilot/runs/bench/ramp.jsonl`):

| Offered | Errors | p50 / p99 | Notes |
|---|---|---|---|
| up to 70/s | 0 | about 55 ms | |
| 80/s | 0 | 0.33 / 0.6 s | goodput stops at 74.5/s |
| 100/s | 6.5% | 1.4 / 2.06 s | |
| 120/s | 22% | 2.0 / 2.06 s | |

The point where latency and errors turn up is about 74 orders/s.

**Traffic.** Your partners.py sale shape: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s for 30 s, which is 5,104 orders and 170/s offered, more than twice what the database can take.
- **Direct calls:** partners retry a failure after 0.1 s, up to 5 attempts, exactly as partners.py does.
- **Through Aquifer:** each order is submitted once with its partner as `user_id`, `idempotent_key` = partner-order number and `max_retries: -1`. A 429 would be resubmitted after its `Retry-After`, but none happened.

The load generator never went above 223 requests/s.

**Shape:** Aquifer on its own, one node, account queues on (the default, so one queue per partner), no Inlet or Canalis. Static ceiling only, with no pacing headers. Aquifer was built from the checkout, which is `v0.15.1-63-g4d4345e`. The skill is pinned to v0.15.1; as you asked, I used the checkout's own copy rather than GitHub.

### Results (2 direct runs, 2 Aquifer runs at 70/s, 1 Aquifer run at 75/s)

| | Direct calls | Through Aquifer (ceiling 70/s) |
|---|---|---|
| Orders lost | 368 and 366 of 5,104 (7.2%) | 0 of 5,104, both runs |
| Small shops' orders lost | corner-shop 7.0 / 9.0%, kiosk 6.0 / 6.6%, deli 3.3 / 8.6% | 0 |
| 503s at the backend | about 6,800 (59% of attempts) | 0 |
| 429s from Aquifer | none (no Aquifer) | 0 |
| Retries sent | about 6,400 | 0 (5,104 attempts for 5,104 orders) |
| Backend pool full | 97–98% of samples, up to about 390 requests waiting for a connection | 56–64%, at most 3 waiting |
| Database insert time p99 | 2.06 s (the 2 s connection wait) | 0.09 s |
| Small shops p50 / p99 | about 16.5 / 34–35 s | 0.07 / 0.105 s |
| Bigbox p50 / p99 | about 16.6 / 34 s | about 25 / 44 s |
| Orders written per second | 74.5 | 68.5 (71.2 at a 75/s ceiling) |
| Drain after the burst | last success 33 s after the burst, with 7% lost | everything done 44.6 s after the burst |
| Fairness | every partner fails at about the same rate | small shops unaffected by bigbox |
| Duplicate writes | 0 | 0 |

How to read the table:
- **Latency** is measured from each order's scheduled send time to its first successful database write. On the Aquifer side the write time comes from the backend's log. Aquifer's own per-partner metrics agree: corner-shop averaged 66 ms and bigbox 24.3 s.
- **Fairness index:** Jain's index over each partner's share of completed orders is 0.32 in both setups, because demand itself is that uneven, so it isn't a useful number here. The useful fairness results are the small-shop rows above.
- **Your own replay:** I also ran partners.py, unchanged, as a reference. It showed the same picture: about 8% of orders lost, small shops included, and 5,461 503s.
- **Cost:** none of this needed a bigger database plan. To avoid failures by provisioning instead, the pool would have to handle the 170/s peak, about 9 or more connections instead of 4.

**What didn't improve:**
- **Total throughput fell slightly:** 68.5/s against 74.5/s, because I set the ceiling under the measured limit. At 75/s it reached 71.2/s with still no 503s, but in that single run one kiosk order took 2.4 s, so I'd treat 70 as the safe setting and 75 as untested.
- **Bigbox waits longer:** its p99 rose from 34 s to 44 s, and the backlog takes about 11 s longer to clear. It was offering 150/s to a 74/s database; it now waits in line instead of failing 7% of the time.

**What you'd have to change to adopt this:**
1. **Partners would no longer get an `order_id` straight back.** Aquifer answers with a `job_id` right away and the write happens later, up to about 44 s later for bigbox. Partners would read the result from `GET /jobs/{id}` or a `webhook_url`.
2. **Partners must not call Aquifer directly.** Aquifer sends requests to whatever URL a job names, so outside callers could aim it at your internal network. The safe shape is orders-api's `POST /orders` becoming a thin intake that submits to Aquifer on the partner's behalf, with `AQUIFER_ALLOWED_URL_DOMAINS` set as a second guard.
3. **`save_order` must ignore repeats.** Aquifer can deliver the same order more than once. A unique key on (partner, order number) would do; the pilot saw no duplicates, but it didn't test crashes.
4. **A crash can lose very recent orders.** A crash loses roughly the last 100 ms of accepted orders. Partners recover them by resending with the same key: an answer of 201 means it was lost and has been queued again, and 200 with `duplicate: true` means Aquifer already had it.
5. **The backlog limit needs sizing for your real sale length.** It is 10,000 orders by default (`AQUIFER_MAX_PENDING_PER_UPSTREAM`); the peak here was about 2,900. In a sale of a few minutes, bigbox's backlog grows about 80/s and would reach 10,000 in roughly 2 minutes. After that, bigbox's new orders are refused with 429s while the small shops keep getting in. I couldn't test that under the 2-minute limit.

**Settings used.** The YAML ceiling for `127.0.0.1:19501` was `rps: 70`, `max_concurrent: 4`. The per-partner limit needs at least 4 in flight, or bigbox alone could never fill the 4 connections. I also set `AQUIFER_METRICS_TENANT_LABELS=true`; every other Aquifer setting was left at its default. Ports: Aquifer 19500, orders-api 19501.

**Limits on these numbers:**
- **Short runs:** 30-second bursts on a laptop rather than hours on an always-on machine.
- **Shared laptop:** other agents' load tests were running on it at the same time (other ports, not mine), so CPU was contended. The repeat runs came out nearly identical, so the effect looks small.
- **No pacing headers tested:** the fixed ceiling produced no errors, so the skill's advice was not to add them.
- **A metric that looked wrong:** Aquifer's `aquifer_flow_rate_rps` gauge sometimes read 107 to 230 even though the ceiling was 70. Orders actually reaching the backend peaked at 78 in one second, so the ceiling held. The docs don't explain that gauge's reading; it may be worth an issue on the Aquifer repo.

**Raw data and scripts** are in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/i4-r2/pilot/`:
- `backend.py`: wraps your app.py, which I didn't change, and logs every order with its status and the headers Aquifer sends.
- `bench.py`: the capacity ramp.
- `load.py`: the sale-burst load generator for both setups.
- `pass.sh`: the skill's script for one Aquifer run. `with_backend.sh` starts and stops orders-api around a run.
- `aquifer.yml` and `aquifer-75.yml`: the two ceilings tested.
- `aquifer`: the binary built from the checkout.
- `runs/<name>/` holds each run:
  - `summary.json`, `jobs.jsonl` and `backend-stats.jsonl` from the load generator;
  - `metrics-*.prom` scrapes and `aquifer.log` for the Aquifer runs;
  - `runs/<name>-backend/backend.jsonl` for the backend's log on those runs.
- Run names: `bench`, `baseline`, `baseline-2`, `aq-static`, `aq-static-2`, `aq-75`, `partners-py-ref`.

The Aquifer checkout is unchanged (`git status` is clean). Every process I started has stopped, and nothing is listening on ports 19500 to 19509.