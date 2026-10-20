# Soak run 3c

One Aquifer node (Fly performance-1x: 1 CPU, 2GB, 10GB volume, Pebble with
the default 100ms flush) under the harness in [`benchmark/soak`](../../README.md),
HTTP jobs and WebSocket sessions together, with chaos on. Built from `main` at
3fea9f1 and deployed 2026-10-12 23:18 UTC. Started 23:24 UTC that day,
stopped 2026-10-20 22:10 UTC after 190 hours (7 days 22 hours).

Load: 500 jobs/s from 50 users, a 2,000/s burst every hour at :00, a
1,000-session WebSocket burst every hour at :30, a daily ramp at 04:00 UTC
(HTTP and WebSocket on alternate days), and a combined peak at 16:00 UTC.
Every few minutes the target API, the webhook receiver and the WebSocket
backend switch between healthy, slow, 503s, 429s, dropping and down.

## Totals

| | |
|---|---|
| Submitted | 361.1M |
| Accepted | 318.7M |
| Turned away when full (429/503) | 41.1M |
| Requests that errored on the client side | 1.3M |
| Webhooks received (completed / failed) | 318.6M / 110k |
| Restarts caused by the harness (`kill -9` and machine reboots) | 47 |
| Full machine reboots | 5 |
| **Lost** (accepted, never finished, outside any flush window) | **37**, all at machine reboots, explained below |
| Lost inside the flush window (accepted within 100ms before a kill; counted separately from the 37) | 342 |
| Duplicate webhooks, all within 10 minutes of a restart | 2,499 |
| Finished without a webhook | 4,641 |
| Stuck over 24 hours | 0 |
| WebSocket commands sent / completed | 123.7M / 122.6M |
| WebSocket commands not completed within 5 minutes | 1.08M (0.87%) |

Run 1 for comparison: 878 lost and 161,249 finished without a webhook in 45
hours.

## What held

- **No accepted job lost to Aquifer.** The 37 counted as lost all came from
  machine reboots (10 on Oct 14, 24 on Oct 18, 3 on Oct 20). Every one that
  can still be checked was accepted 168-194ms after the driver asked the
  supervisor to reboot.
  The supervisor answers that request and kills Aquifer 200ms later, but
  the driver recorded the restart at request time and only excused jobs
  accepted up to 50ms after it. So these jobs were in the flush window, and
  the driver counted them wrong. #93 fixes the driver. No job was counted
  lost after any of the plain kills.
- **No leaks over nearly eight days.**
  - RSS at the end of each hour: median 274MB, highest 658MB, during a burst.
  - Goroutines at the end of each hour: median 1.8k.
  - Pebble stayed between 640MB and 1.08GB after its first day.
  - Open file descriptors at the end of each hour: median 867.
  - Volume free space never went below 52.8%.
- **Restarts recover.** After each kill or reboot, the backlog drained and
  accepts returned to 500/s within a few minutes.
- **Throughput.** Hourly accepts: median 467/s, lowest 343/s, during an
  outage-heavy hour. Accept p99 median 393ms. The HTTP ramp sustained 750/s
  on every HTTP ramp day (Oct 13, 15, 17, 19).
- **Duplicates only around restarts.** All 2,499 duplicate webhooks arrived
  within 10 minutes of a kill or reboot, which at-least-once delivery
  allows.
- **WebSocket ordering.** 0 out-of-order events and 0 messages for unknown
  commands across 368M events.

## What didn't hold

### WebSocket commands not completed within 5 minutes: 1.08M

0.87% of commands were not confirmed within 5 minutes, and median command
latency rose to 30-60s in the later days. The losses arrive in jumps
(+20-40k in an hour) during the WebSocket backend's dropping and down
windows and at bursts, not steadily. 4.85M of their messages arrived after
the 5-minute cutoff, so many were late rather than gone. The count can't
tell late from lost. Tracked in #53.

### Finished without a webhook: 4,641

Each of these is a webhook that ran out of retries during a receiver
outage. An unset `webhook_max_retries` gives 4 retries, about 15 seconds of
backoff. #99 raises the default to 10 (about 13 minutes), the same change
that took Spillway from 36,316 to under 250 (Spillway #69).

### One unplanned restart

At about 20:34 UTC on Oct 16, during a 1,000-session WebSocket burst,
Aquifer exited on its own and the supervisor restarted it. Accepts were 0
from 20:35 to 20:46. On restart Aquifer replayed its WAL and recovered 349
queued jobs, and nothing was lost.

Open file descriptors had risen to 2,499, and at 20:39 the driver's own
ledger save failed with "too many open files" on the shared volume. Later
peaks of 4,900 and then 6,466 caused no failure, so descriptors alone
don't explain the exit. Aquifer's logs had rotated past it before anyone
looked, so the cause is unknown. It didn't recur in the remaining four
days.

### Harness

The WebSocket ramp (Oct 14, 16, 18, 20) stalled at its first step and
reports a ceiling of 0. This is a driver measurement problem, not an
Aquifer limit.

## Files

`status.json`, `hourly.json` (191 hours), `daily.json`, `minutes.json` (the
last 3 hours), `events.json` (the last 500 events) and `reports.json` (the
monitoring agent's 64 reports), all fetched from the driver just before it
stopped.
