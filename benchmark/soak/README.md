# Soak test

A week of hostile synthetic load against one node, looking for what short
benchmarks miss: slow leaks, storage growth, drift in latency or in the
throughput ceiling, and jobs lost across many crashes. HTTP jobs and
WebSocket sessions run together, so interference between them shows up.
The same harness soaks Spillway in parallel for comparison. Tracking issue:
[#27](https://github.com/rjpruitt16/aquifer/issues/27).

Results of finished runs are in [`results/`](results/). Each run keeps its
data in its own directory on the volumes (`/data/run-2`, ...).

## Pieces

**Target** (`aquifer-soak`, performance-1x, 10GB volume, Pebble, default 100ms
flush, plus a local Valkey for WebSocket transcripts, started outside the
supervisor so a kill leaves it up). Aquifer runs under `supervisor`, which
restarts it when it exits and, on private port 9090:

- `POST /kill` sends SIGKILL (the same as `kill -9`);
- `POST /reboot` exits the machine's main process, so Fly restarts the machine;
- `GET /stats` reports RSS, threads, open file descriptors, data and Pebble
  size and file count, free volume space, restarts, and goroutines and heap
  read from Aquifer's pprof listener (`AQUIFER_DEBUG_ADDR`, loopback only);
- `GET /logs?n=&grep=` tails Aquifer's log (also kept on the volume, capped at
  64MB).

The target has no public service. The driver reaches it over Fly's private
network.

**Spillway target** (`spillway-soak`, same size and volume, Mnesia) runs
Spillway under the same supervisor. Its image is built from this repository
with Spillway cloned at a pinned commit (`Dockerfile.spillway-target`).

**Driver** (`aquifer-soak-driver`, and `spillway-soak-driver` with the same
image pointed at Spillway through `SOAK_*` environment variables,
performance-2x) runs everything else:

- **Load:**
  - 500 jobs/s steady, about half of one performance-1x machine's measured
    capacity;
  - 2,000 jobs/s for 60 seconds at the top of every hour;
  - 200 WebSocket sessions, each sending a command a second through the
    target's `/websocket` to the driver's own aqueduct.v1 backend, which
    answers each with an ack and 3 events;
  - 1,000 sessions for 60 seconds at half past every hour, so WebSocket
    peaks fall between the HTTP ones;
  - every day at 16:15 UTC, both peaks at once;
  - every day at 04:00 UTC, a ramp with every misbehaving endpoint held
    healthy: HTTP jobs from 500 to 3,000/s on even days of the year,
    WebSocket sessions from 200 to 2,500 on odd days, in one-minute steps.
    The highest step the target sustains is that day's ceiling.
- **Misbehaving target API (`/work`):** every 10 minutes it picks one mode:

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

- **Misbehaving WebSocket backend (`/socket`):** every 10 minutes it picks one mode:

  | Mode | Share of slots | Behavior |
  |---|---|---|
  | healthy | 75% | normal |
  | slow | 10% | 50–500ms before each event |
  | dropping | 10% | about one socket in 30 closed each second |
  | down | 5% | 1–3 minutes of 503 handshakes, open sockets closed |

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

  WebSocket commands have their own ledger (`websocket` on `/status`). A
  session reconnects from the stream ID of the last message it processed and
  resends, with the same `message_id`, any command not answered within 15
  seconds. A command still incomplete 5 minutes after it was first sent is
  `lost_commands` if the target confirmed recording it (**this should stay
  0**), `unrecorded_commands` otherwise. Duplicate and out-of-order events
  are counted, separating duplicates that follow a resend.

  The ledger is saved to the driver's volume every 5 minutes, and sequence
  numbers are reserved on disk ahead of use. If the driver itself crashes, the
  jobs it sent since the last save are reported as a gap instead of as losses.
- **Observer:**
  - every minute, it records rates, accept and end-to-end p50/p99, shed rate,
    and the target's stats;
  - every hour, it appends a summary to `hourly.jsonl`;
  - every day, it appends one to `daily.jsonl`.

## Status pages (public, read-only)

`https://aquifer-soak-driver.fly.dev/` and `https://spillway-soak-driver.fly.dev/`
are plain-text summaries with alerts. The JSON endpoints are:

| Path | Contents |
|---|---|
| `/status` | everything current: alerts, ledger, last minute, last hour, daily summaries, recent events |
| `/status/hourly?n=168` | hourly summaries, for trends |
| `/status/minutes?n=180` | the last three hours, minute by minute |
| `/status/daily` | one line per day, including the ramp ceiling |
| `/events?n=200` | mode changes, bursts, ramps, kills |
| `/logs?n=500&grep=` | Aquifer's recent log lines |

`POST /control/pause` and `POST /control/resume` (same bearer token as
`/reports`) stop and restart the load. Use them around a target deploy: pause,
wait for `ledger.outstanding` on `/status` to reach 0, deploy, resume. The
supervisor forwards the platform's stop signal to Aquifer as SIGTERM, so
Aquifer drains gracefully either way.

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

Spillway, with a `SECRET_KEY_BASE` secret on `spillway-soak` and a
`SOAK_REPORT_TOKEN` on `spillway-soak-driver`:

```sh
fly apps create spillway-soak --org ezthrottle
fly apps create spillway-soak-driver --org ezthrottle
fly deploy -c benchmark/soak/fly.spillway-target.toml --dockerfile benchmark/soak/Dockerfile.spillway-target \
  --build-arg SPILLWAY_REF=<commit> --ha=false
fly deploy -c benchmark/soak/fly.spillway-driver.toml --dockerfile benchmark/soak/Dockerfile.driver --ha=false
```

For a new run, change the run directory (`SOAK_DATA_DIR`, `DB_PATH` or
`MNESIA_DIR`, and the driver's `SOAK_DATA`) in the four `fly.*.toml` files.

Tear down with `fly apps destroy aquifer-soak` and `fly apps destroy
aquifer-soak-driver`. Before that, copy `hourly.jsonl`, `daily.jsonl` and
`events.jsonl` off the driver's volume with `fly ssh sftp get`.

Cost at current Fly prices: about $8 a week for the target, $16 for the
driver, and cents for the volumes.

## Monitor

One scheduled [Claude Managed Agent](https://platform.claude.com/docs/en/managed-agents/scheduled-deployments)
per target checks its status pages every 6 hours: Aquifer's at :15 UTC past 00, 06, 12 and 18,
Spillway's at :45. Their instructions are one template, [`monitor/system.md`](monitor/system.md),
and `monitor/setup.py` creates or updates an agent for the target named by `SOAK_MONITOR_TARGET`
(`aquifer`, the default, or `spillway`), keeping its IDs in `monitor/ids.json` or
`monitor/ids-spillway.json`. Each agent:

- runs on Claude Haiku 5.5, capped at $1 per run (the dry run cost about 2 cents);
- can reach only the status host and `api.github.com`;
- holds two secrets in a vault, each sent only to its own host: its driver's report token, and a fine-grained GitHub token limited to Issues on its own repository;
- posts each report to `/reports`, which is also its memory between runs;
- on a warning or problem, opens a `soak-alert` issue in its own repository (aquifer or spillway), or comments on the open one, and goes no further. A person decides what happens next.

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
