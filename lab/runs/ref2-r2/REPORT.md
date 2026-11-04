## Aquifer pilot: orders-api, 2026-11-04

**Result:** Aquifer fixes the problem you described. With Aquifer in front, the small shops lost no orders and their slowest orders took 0.21 s. Sending straight to orders-api, about 9.5% of their orders failed and their slow orders took about 26 s. Bigbox lost no orders either, but its orders now wait longer, because they queue behind the small shops' fair share. Throughput stayed the same: the database is the limit either way.

I ran the pilot end to end with the aquifer-pilot skill from your checkout (Aquifer v0.15.1, the version the skill is written for). Everything ran on this laptop, on ports 19200 (orders-api) and 19201 (Aquifer). Each load test lasted under 2 minutes and the load generator never sent more than 151 requests a second. On the direct runs, orders-api itself received up to 221 requests a second, because the partners' retries pile on; that stays under your 300 limit. I changed nothing in aq-ref (`git status` is clean).

**Backend:** orders-api (Python `ThreadingHTTPServer`) with a 4-connection database pool. Each write holds a connection for 50 ms, so the most it can do is 80 orders a second. I ramped load against it with no retries:
- Latency stayed flat at about 55 ms up to 70 requests a second.
- At 80 a second, p99 latency jumped to 760 ms.
- At 90 a second, p99 reached about 2 s and 0.2% of requests got `503`.
- At 100 a second, 10% got `503`; at 120 a second, 26%.

So the backend handles about 70 a second comfortably, and above about 75 a second it starts to break down.

**Traffic:** the replay from your `partners.py`, unchanged: bigbox sends 150 orders a second, corner-shop 10, kiosk 5 and deli 5, for 30 seconds. Partners retry a failed order up to 5 times, 0.1 s apart. Because of drift in the script's timing, about 4,300 to 4,450 orders went out per run. I ran each final setup 3 times.

**Shape:** one Aquifer node on its own; no Canalis, because it's one node, and no Inlet, because the orders aren't provider webhooks. Each partner got its own queue (the default; the backend never turned that off). Aquifer was paced by a fixed limit in its config file: 80 requests a second and 8 in flight. The pacing headers I added to orders-api were tested in separate runs and are not part of the final setup.

| | Direct to orders-api (3 runs) | Through Aquifer (3 runs) |
|---|---|---|
| Orders lost after 5 tries (all partners / small shops) | 6.5–7.7% / 9.3–9.5% | 0% / 0% |
| Time the database pool was full | 97–99.5% | 94–96% |
| Connections waiting for the pool (peak) | 392 | 8 |
| Share of requests answered `503` | 58% | 0% |
| Retries hitting orders-api | 5,071–5,401 (2.2 requests per order) | 0 (1.0 per order) |
| Peak requests a second reaching orders-api | 216–221 | 78–81 |
| p50 / p99 time to save an order, all partners | 11.3–12.5 s / 24.5–26.5 s (saved orders only) | 16.4–16.8 s / 28.8–29.4 s |
| Small shops' slowest order (worst p99) | 25–27.6 s | 0.19–0.21 s |
| Bigbox p50 / p99 | 11.3–12.5 s / 24.5–26.4 s, and 6.4–7.6% lost | 18.3–18.6 s / 28.8–29.4 s, none lost |
| Orders saved per hour while busy | about 269,000 | about 268,000–269,000 |
| Time to clear the backlog after the 30 s burst | 23–25 s | 29–30 s |
| Order refusals (`429`) from Aquifer | — | 0 |
| Cost | no change: the same 4-connection plan carried the sale either way | |

**What didn't improve, and what it costs:**
- **Bigbox waits longer.** Its median wait went from about 12 s to about 18.6 s, and its p99 from about 26 s to about 29 s. The backlog takes 5–6 s longer to clear. That's the price of fair turns: the small shops now get all the capacity they ask for, and the database's total output is the same. The direct runs only look faster for bigbox because the 7% of its orders that failed are left out of the latency.
- **Throughput is unchanged** at about 75 saved orders a second. Aquifer doesn't add database capacity; the 4-connection pool is still the limit.
- **Partners would see a different response.** Today a partner gets `201` with an `order_id` immediately. Through Aquifer, the order is accepted at once but saved up to about 30 s later. Adopting it means putting a thin intake in front of Aquifer that returns `202`, or using Aquifer's `POST /proxy`, which answers on the same connection and queues on overload. I didn't test `/proxy`.
- **My pacing headers made things worse.** I added an AIMD loop to orders-api that sends `X-Aqueduct-Rps`, driven by how long orders wait for a database connection. With it, throughput fell to 53–61 orders a second, against 75 with the fixed limit alone. In the first version (run aq4), a doc mismatch (below) made me split the rate evenly, so bigbox got only 20 a second. The fixed version (run aq5) still lost capacity to headroom held for the small shops and to the loop's periodic 30% cuts. For this backend, a fixed limit set from the benchmark was enough, so I left the headers out of the final setup. The code is in `pacing.py` and only runs with `--pacing`.
- **A first limit set too low cost throughput.** I started at 70 a second with 4 in flight. Aquifer's own overhead per request (about 60 ms round trip) capped that at about 65 a second, and the pool was full only 47% of the time. Raising it to 8 in flight, then to 80 a second, fixed it. In the runs, Aquifer sent about 6% below the rate it was given when one partner was alone.

**Things in Aquifer worth knowing (bugs to report):**
- `X-Aqueduct-Queue-Depth` sent to orders-api held the whole backlog for orders-api, the same number as `X-Aqueduct-Upstream-Backlog`, not the sending partner's own queue. That's true in about 98% of requests. The skill documents it as "this queue's jobs still waiting".
- The README and `docs/pages/configuration.md` recommend `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=0` for zero loss. The pilot skill says not to recommend it. I left it at the default (100 ms).
- Aquifer listened on all network interfaces (`*:19201`), and I found no documented setting to bind it to loopback only. The docs say to keep it on a private network.
- On macOS, Aquifer logs that it can't read memory, so its memory limit is off.

**Caveats:**
- The skill asks for passes of a few hours to a day on an always-on machine. You limited each test to under 2 minutes on this laptop, so I ran 30-second bursts, repeated 3 times per setup. The results were very consistent between repeats.
- Other agents' processes were running on this laptop at the same time, on other ports.
- I didn't test a crash: Aquifer can lose orders it accepted in the last ~100 ms if it crashes. No order was saved twice in any run. Still, delivery is at-least-once, so orders-api should skip duplicate orders by partner and order number before going live.
- If partners hold onto a failed order and resend it with the same `idempotent_key`, a crash costs them nothing.

**Settings used:**
- Environment variables: `PORT=19201`, `DB_PATH=pilot/data/<run>/aquifer-data`, `CONFIG_PATH=pilot/aquifer.yml`, `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-1`.
- Final config (`pilot/aquifer.yml`): the `127.0.0.1:19200` entry has `rps: 80` and `max_concurrent: 8`.
- Every order through Aquifer had `user_id` set to the partner, `idempotent_key` set to `run:partner:order`, `max_retries: -1`, and no `execute_before`. On a `429`, the load generator would wait for `Retry-After` and send again; that never happened.
- Everything else was left at its default, including the backlog budget of 10,000. The burst only reached about 2,200 queued orders.
- How the runs differed:
  - aq1: 70/s with 4 in flight.
  - aq2: 70/s with 8 in flight.
  - aq3, aq6, aq7: 80/s with 8 in flight (the final setup).
  - aq4: aq3 plus the pacing headers (first version).
  - aq5: aq3 plus the fixed pacing headers.

**Changes to orders-api:** I made the folder a git repo. `main` has the code as I found it; branch `pilot/aquifer` (commit 2806bd3) adds:
- optional `--log` for one line per order, including the `X-Aqueduct-*` headers;
- optional `--pacing`, implemented in `pacing.py`;
- `db.save_order_timed`, which also returns how long each order waited for a connection;
- the load generator, analysis scripts and Aquifer config under `pilot/`.

**Raw data:** everything is in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/ref2-r2/pilot/data/` (112 MB, kept out of git), one folder per run: `bench`, `base1`–`base3`, and `aq1`–`aq7`. Each has:
- `orders.jsonl`: every order with its partner, send and finish times, status and attempts;
- `backend.jsonl`: every request orders-api received, with the headers it got and sent;
- `backend_stats.jsonl`: database pool use, sampled every 0.25 s;
- for Aquifer runs, `aquifer_metrics.txt`, `/metrics` and `/health` scraped every 5 s; I scraped more often than the skill's 15–30 s because the runs were short.

To rebuild the table, run `python3 pilot/summary.py base1 base2 base3 aq3 aq6 aq7` from the repo folder.

**Cleanup:** I stopped every process I started; nothing is listening on ports 19200–19209. The Aquifer and Python processes still running belong to other runs (i2-r1 and i2-r3, on other ports), and I left them alone. I deleted the `.l8-key` file Aquifer generated in the repo folder.