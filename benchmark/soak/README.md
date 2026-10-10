# Soak test

A week of hostile synthetic load against one Aquifer node, looking for what
short benchmarks miss: slow leaks, storage growth, drift in latency or in the
throughput ceiling, and jobs lost across many crashes. Tracking issue:
[#27](https://github.com/rjpruitt16/aquifer/issues/27).

## Pieces

**Target** (`aquifer-soak`, performance-1x, 10GB volume, Pebble, default 100ms
flush). Aquifer runs under `supervisor`, which restarts it when it exits and,
on private port 9090:

- `POST /kill` sends SIGKILL (the same as `kill -9`);
- `POST /reboot` exits the machine's main process, so Fly restarts the machine;
- `GET /stats` reports RSS, threads, open file descriptors, data and Pebble
  size and file count, free volume space, restarts, and goroutines and heap
  read from Aquifer's pprof listener (`AQUIFER_DEBUG_ADDR`, loopback only);
- `GET /logs?n=&grep=` tails Aquifer's log (also kept on the volume, capped at
  64MB).

The target has no public service. The driver reaches it over Fly's private
network.

**Driver** (`aquifer-soak-driver`, performance-2x) runs everything else:

- **Load:**
  - 500 jobs/s steady, about half of one performance-1x machine's measured
    capacity;
  - 2,000 jobs/s for 60 seconds every hour;
  - every day at 04:00 UTC, a ramp from 500 to 3,000 jobs/s in one-minute
    steps, with both misbehaving endpoints held healthy. The highest step
    Aquifer sustains is that day's ceiling.
- **Misbehaving upstream (`/work`):** every 10 minutes it picks one mode:

  | Mode | Share of slots | Behavior |
  |---|---|---|
  | healthy | 70% | normal responses |
  | slow | 10% | 0.5–3s latency |
  | errors | 8% | 30% of requests get 503 |
  | rate limited | 7% | 30% of requests get 429 with `Retry-After: 3` |
  | down | 5% | 2–5 minutes of 503s |

- **Misbehaving webhook receiver (`/hook`):** every 10 minutes it picks one mode:

  | Mode | Share of slots | Behavior |
  |---|---|---|
  | healthy | 85% | normal responses |
  | slow | 8% | 0.2–2s latency |
  | errors | 5% | 20% of requests get 503 |
  | down | 2% | 1–3 minutes of 503s |

- **Chaos:**
  - `kill -9` every 3–5 hours;
  - every 36 hours, a full machine restart instead of the kill.

  Neither happens during a burst or a ramp.
- **Job ledger.** Every job gets a sequence number carried in its webhook URL,
  and one bit per job marks "webhook received", so duplicates are caught all
  week. A job still unaccounted for 5 minutes after acceptance is checked with
  `GET /jobs/{id}`. It ends up in one of these buckets:

  | Outcome | Meaning |
  |---|---|
  | `completed_webhooks`, `failed_webhooks` | finished, webhook received |
  | `finished_without_webhook` | Aquifer finished it, but gave up on the webhook. Expected when the receiver's outage outlasts the webhook retries |
  | `lost` | accepted, then gone from the store without ever finishing. **This should stay 0** |
  | `lost_within_flush_window_of_kill` | lost, but the `201` arrived at most 300ms before a kill. The 100ms flush allows these |
  | `stuck_over_24h` | still queued a day later |
  | `duplicate_webhooks` | at-least-once delivery. Also counted separately within 10 minutes of a restart |

  The ledger is saved to the driver's volume every 5 minutes, and sequence
  numbers are reserved on disk ahead of use. If the driver itself crashes, the
  jobs it sent since the last save are reported as a gap instead of as losses.
- **Observer:**
  - every minute, it records rates, accept and end-to-end p50/p99, shed rate,
    and the target's stats;
  - every hour, it appends a summary to `hourly.jsonl`;
  - every day, it appends one to `daily.jsonl`.

## Status pages (public, read-only)

`https://aquifer-soak-driver.fly.dev/` is a plain-text summary with alerts. The JSON endpoints are:

| Path | Contents |
|---|---|
| `/status` | everything current: alerts, ledger, last minute, last hour, daily summaries, recent events |
| `/status/hourly?n=168` | hourly summaries, for trends |
| `/status/minutes?n=180` | the last three hours, minute by minute |
| `/status/daily` | one line per day, including the ramp ceiling |
| `/events?n=200` | mode changes, bursts, ramps, kills |
| `/logs?n=500&grep=` | Aquifer's recent log lines |

`/work` and `/hook` are served on a separate private port, so nobody outside
Fly can forge webhooks into the ledger.

## Deploy

From the repository root, in the same Fly organization so the two apps share
a private network:

```sh
fly apps create aquifer-soak --org ezthrottle
fly apps create aquifer-soak-driver --org ezthrottle
fly deploy -c benchmark/soak/fly.target.toml --dockerfile benchmark/soak/Dockerfile.target --ha=false
fly deploy -c benchmark/soak/fly.driver.toml --dockerfile benchmark/soak/Dockerfile.driver --ha=false
```

Tear down with `fly apps destroy aquifer-soak` and `fly apps destroy
aquifer-soak-driver`. Before that, copy `hourly.jsonl`, `daily.jsonl` and
`events.jsonl` off the driver's volume with `fly ssh sftp get`.

Cost at current Fly prices: about $8 a week for the target, $16 for the
driver, and cents for the volumes.

## Monitor

A scheduled [Claude Managed Agent](https://platform.claude.com/docs/en/managed-agents/scheduled-deployments)
checks the status pages every 6 hours (at :15 UTC past 00, 06, 12 and 18). Its instructions are in
[`monitor/system.md`](monitor/system.md), and `monitor/setup.py` creates the agent. The agent:

- runs on Claude Haiku 5.5, capped at $1 per run (the dry run cost about 2 cents);
- can reach only the status host and `api.github.com`;
- holds two secrets in a vault, each sent only to its own host: the report token, and a fine-grained GitHub token limited to Issues;
- posts each report to `/reports`, which is also its memory between runs;
- on a warning or problem, opens a `soak-alert` issue, or comments on the open one, and goes no further. A person decides what happens next.

## Running it locally

At a light rate, for checking the harness itself:

```sh
go build -o /tmp/soak/aquifer ./cmd/aquifer
go build -o /tmp/soak/supervisor ./benchmark/soak/supervisor
go build -o /tmp/soak/driver ./benchmark/soak/driver
DB_PATH=/tmp/soak/t/aquifer.db PORT=18080 AQUIFER_DEBUG_ADDR=127.0.0.1:16060 \
  CONFIG_PATH=benchmark/soak/aquifer-soak.yml L8_KEY_PATH=/tmp/soak/t/l8-key \
  /tmp/soak/supervisor -listen 127.0.0.1:19090 -cmd /tmp/soak/aquifer -data /tmp/soak/t -debug http://127.0.0.1:16060 &
/tmp/soak/driver -target http://127.0.0.1:18080 -supervisor http://127.0.0.1:19090 \
  -self http://127.0.0.1:8080 -data /tmp/soak/d -rate 50 -kill-min 2m -kill-max 3m -reboot-every 0 -ramp-hour -1
curl localhost:8081/
```

`/proc`-based stats (RSS, file descriptors) are Linux-only and are missing on macOS.
