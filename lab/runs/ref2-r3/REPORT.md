## Aquifer pilot: orders-api, 2026-11-04

I ran the whole pilot on this laptop using the aquifer-pilot skill from the aq-ref checkout. With Aquifer in front, the small shops stopped losing orders. In the passes where the load generator correctly held under 300 requests a second, about 17% to 21% of their orders failed without Aquifer, and none failed with it. Their worst-case wait went from about 11 s to 0.1 s.

The cost lands on bigbox. All of its orders now get through (without Aquifer about 12% failed), but its slowest orders take longer: p99 went from about 34 s to 40 to 44 s. The 30-second burst also takes 40 to 44 s to clear after it ends, against about 32 s without Aquifer.

The cause is your database, not traffic volume. Your plan allows 4 connections and each order insert takes 50 ms, so the most it can ever write is 80 orders a second, and in practice it topped out around 75. During a sale the partners send 170 a second. Without Aquifer, every partner's requests fight for those 4 connections. Requests that wait more than 2 s get a `503`, and the partners retry immediately, which adds more load: about 2.9 requests reached the database for every order saved. Aquifer gives each partner its own queue and feeds the database at its own pace. The small shops send well under their share, so their orders go almost straight through, and bigbox's backlog waits in its own queue.

**Fit and shape:** It's a good fit: the skill lists databases with bursty writers. I used one Aquifer node and no Inlet or Canalis, because the partners call your API directly rather than sending provider webhooks. Each partner got its own queue (the default). The source is Aquifer v0.15.1, the version the skill is written for; the binary I built prints `dev`.

**Knee (measured):** One sender, no retries, 8 s at each rate:

| Rate sent | What happened |
|---|---|
| Up to 60/s | no errors, p99 under 60 ms |
| 75/s | no errors, p99 77 ms |
| 90/s | errors begin (0.3%); writes cap at 74/s and p50 jumps to 880 ms |
| 110/s and 140/s | 15% and 33% errors; p50 about 2 s |

**Traffic:** I replayed `partners.py`'s 30-second sale burst: bigbox 150/s, corner-shop 10/s, kiosk 5/s, deli 5/s, 5,100 orders a pass. Each pass ran 60 to 84 s, all under 2 minutes, which is far shorter than the hours the skill suggests.

| | Baseline (A3 / A4) | Through Aquifer, 80 rps ceiling (C1 / C1b) |
|---|---|---|
| Orders lost | 705 / 664 of 5,100 (13 to 14%) | 0 / 0 |
| Small shops lost | corner-shop 50 to 52 of 300; kiosk 29 to 31 of 150; deli 31 to 32 of 150 | 0 |
| Small shops' p50 / p99 | 4.2 s / about 10.6 s | 0.07 s / 0.10 s |
| Bigbox lost; p50 / p99 | about 12%; 18.5 s / 33 to 34 s | 0%; 24 to 26 s / 40 to 44 s |
| `503`s / requests to the database | about 8,400 / about 12,900 (about 2.9 per saved order) | 0 / 5,100 (exactly 1 per order) |
| Seconds the database was saturated (any `503`, or p95 connection wait over 100 ms) | 62 of 62 | 0 of 71 to 75 |
| Orders written per second | about 71 | 69 to 73 |
| Time to clear the burst after it ended | about 32 s | 40 to 44 s |
| Fairness index (each partner's saved share of what it sent, 1.0 is even) | 0.998 | 1.0 |
| Cost | none measured | none measured |

There are no money figures because the pilot was local only.

**Other passes:**
- **B1, 70 rps ceiling:** set a little under the knee. Also lost nothing, but writes fell to 61/s and the burst took 53 s to clear, so 80 is the better ceiling.
- **C2, 80 rps ceiling plus pacing headers:** I added pacing headers to orders-api that set the rate from how long writes wait for a connection. They made things slightly worse: the burst took 46 s to clear and writes ran at 67/s. The controller starts at 40 rps and climbs, and with the ceiling already at the hard limit it never had a reason to cut. For a fixed 4-connection pool the plain ceiling is enough, so I recommend leaving the headers off.
- **Submissions refused by Aquifer:** none in any Aquifer pass. The largest backlog was about 3,000 jobs, well under the default limit of 10,000.

**What didn't improve:**
- Total capacity is still about 75 to 80 orders a second; Aquifer only queues the excess. Bigbox's slowest orders get slower (p99 33 s to about 42 s), and the burst takes about 10 s longer to clear. Without Aquifer bigbox looked faster only because about 12% of its orders failed.
- Getting through a sale faster takes more connections or quicker inserts.
- Aquifer is a new component to run. Delivery is at-least-once, so the insert should dedupe on the order key (the pilot saw no duplicates). A crash can lose roughly the last 100 ms of accepted orders, so partners should resubmit an order with the same key if they don't see it completed.

**What partners would have to change:** They would send each order to Aquifer (`POST /jobs`) with `user_id` set to the partner and `idempotent_key` set to `<partner>-<order>`, and stop retrying it themselves. Because Aquifer isn't safe to expose to outside callers, in practice orders-api or a thin front service of yours would do that submission for them.

**Config, including defaults I chose myself:**
- **Aquifer file:** `127.0.0.1:19300` set to rps 80 and max_concurrent 4. That cap applies to each partner's queue separately, so up to 16 orders can be in flight at once. Everything else left at the default rps 2 / max_concurrent 1.
- **Aquifer settings:** `AQUIFER_METRICS_TENANT_LABELS=true`, `AQUIFER_NODE_ID=pilot-1`, the rest default.
- **Jobs:** `max_retries: -1`. Partners submitted each order once and, on a `429`, would wait the `Retry-After` and resend the same key; that never happened.
- **Header pass (C2):** `X-Aqueduct-Rps` set to the total budget divided by `X-Aqueduct-Active-Queues`, starting at 40, +4 per second when healthy, ×0.6 when p95 connection wait goes over 100 ms, capped at 80. "Database busy" returned `429` instead of `503`.
- **Load generator:**
  - Each partner has its own client pool (bigbox 400 workers, the others 60 each).
  - `partners.py` shares one 400-worker pool across all partners, which makes small shops queue behind bigbox inside the test tool. I kept one pass that way for comparison: A0, where everyone's p50 was about 17 s.
  - Baseline retries copy `partners.py`: 5 attempts, 0.1 s apart, 5 s timeout.
  - Every outbound request goes through a 300/s cap.
- **Ports:** orders-api 19300, Aquifer 19301.

**Rule breach:** The first two per-partner baselines (A1 and A2) briefly broke your 300 requests a second limit. The cap let a full second's worth of requests through at once, and A1 hit 324 in one second. I fixed it to allow only 5 at a time, reran them as A3 and A4 (peak 299 to 302 in any one-second window, so about 300), and the table uses only A3 and A4. I kept A1 and A2 in folders ending `-capbug`. The Aquifer passes never got near the limit: about 170 submissions a second, and at most 76 requests a second reaching the database.

**Changes to your service:**
- `app.py` gained `--log` (one record per order request, including the `X-Aqueduct-*` headers Aquifer sends) and `--pace` (the header pass). Both are off by default, so it behaves as before without them.
- `db.py`'s `save_order` can optionally return how long it waited for a connection.
- Copies of the original files are in `pilot/`.

**Files** are in `/private/tmp/claude-501/-Users-rahmijamalpruitt-SAAS-portfolio-site/1e6a8020-537a-4f48-b26f-57758cf6119a/scratchpad/lab-runs/ref2-r3/pilot/`:
- `bench.py`: knee ramp
- `loadgen.py`: sale-burst load generator
- `run_pass.sh`: runs one pass
- `analyze.py`: summarizes a pass
- `bin/aquifer`: the binary I built
- `data/bench.json`: knee results
- `data/<pass>/`: each pass's per-order log, the database's own log, and `summary.json`. Aquifer passes also have `metrics.prom` (scraped every 15 s), `metrics.final.prom`, `health.json` and `aquifer.log`.

**Docs gap:** The docs don't say that completion webhooks go to the receiver at the default rps 2 unless you add its host to the file. I avoided webhooks and measured completion from the database's own log.

**Cleanup:** Everything I started is stopped and nothing is listening on 19300 to 19309. Other processes are running on 19000, 19200 and 19400, started from ref2-r2, i2-r1 and aq-lab; they aren't mine, so I left them running. aq-ref is unchanged (`git status` is clean).