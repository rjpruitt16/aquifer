# Soak run 1

One Aquifer node (Fly performance-1x, Pebble, 100ms flush) under the harness
in [`benchmark/soak`](../../README.md), HTTP jobs only. Started 2026-10-10
17:00 UTC, stopped 2026-10-12 14:30 UTC after 45 hours. Aquifer was built from
`main` as of 2026-10-10, before the pacing fix in #46.

## Totals

| | |
|---|---|
| Submitted | 85.7M |
| Accepted | 75.9M |
| Turned away when full (429/503) | 9.55M |
| Webhooks received (completed / failed) | 74.96M / 763k |
| Process kills (`kill -9`) | 9 |
| Full machine restarts | 1 (2026-10-12 10:11) |
| **Lost** | **878** |
| Lost with the `201` at most 300ms before a kill | 46 |
| Duplicate webhooks, all within 10 minutes of a restart | 657 |
| Finished without a webhook | 161,249 |
| Stuck over 24 hours | 0 |

## What held

- **No leaks.** RSS stayed between 200 and 300 MB at the end of each hour, Pebble
  between 0.8 and 1.1 GB after its first day of compaction, open file
  descriptors returned to a few hundred after every burst.
- **Restarts recover.** After each kill the backlog drained within a minute
  and the accept rate returned to 500/s.
- **Ramp ceiling 750/s on both days.** The 1,000/s step was not sustained:
  873/s accepted with 122/s shed on day one, 932/s and 64/s on day two.
- **Duplicates only around restarts.** All 657 duplicate webhooks arrived
  within 10 minutes of a kill or the machine restart, which at-least-once
  delivery allows.

## What failed

### 868 jobs lost after the 05:20 kill

All 868 were accepted between 05:17:00 and 05:17:54 on 2026-10-12, while the
webhook receiver was down for 140 seconds and the queue sat at its 10,000-job
backlog cap. Aquifer answered each with `201`. The kill came at 05:20:46,
about 3.5 minutes later, and after it `GET /jobs/{id}` returned 404 for all
of them without the driver ever seeing them finish.

Writes are synced every 100ms, so the flush window does not explain jobs
accepted minutes before the kill. Something deleted those jobs, or never
stored them, under a webhook outage combined with a full backlog. The root
cause is not known yet. The log buffer had rotated past these job IDs by the
time they were checked, so reproducing it is the next step.

The other 10 losses were accepted about 200ms after the machine restart was
requested at 10:11:48, so they are flush-window losses the classifier did not
recognise because the 201 arrived after the restart request rather than
before it.

### Finished without a webhook: 161,249

A failed response that advertised a rate (`X-Aqueduct-Rps`) replaced the
halved pace instead of capping it. The driver's misbehaving receiver stamps
`X-Aqueduct-Rps: 100000` on every response, 503s included, so during outages
the queue kept full speed and webhook deliveries used up their four default
retries within seconds. The same bug made failed jobs climb during target API
`down` slots. Fixed in #46 (and spillway#36): a failed response can now only
lower the pace. `webhook_max_retries`, also added in #46, lets a caller ask for
retries until delivery.

## Open questions

1. What deletes or never stores an accepted job when a webhook outage meets a
   full backlog? This should be reproduced and fixed before the next release.
2. Did the backlog of pending webhook deliveries contribute to it?
3. Should the ledger count a job accepted after a restart request but before
   the process exits as a flush-window loss?

## Files

- `summary.txt`, `status.json`: the status page and full status at the end
- `daily.json(l)`, `hourly.json(l)`: per-day and per-hour summaries
- `minutes.json`: the last 24 hours, minute by minute
- `events.json(l)`: mode changes, bursts, ramps, kills, the restart
- `reports.jsonl`: the monitoring agent's reports
