# Soak run 2 (2026-10-12, stopped after about 5 hours)

Run 2 mixed HTTP jobs and WebSocket sessions and ran Spillway in parallel
with Aquifer on the same harness. It was stopped early: each problem it found
was fixed mid-run, so its totals mix several builds. Run 3 restarts both
systems on fixed builds at the same time.

## Final ledgers

| | Aquifer | Spillway |
|---|---:|---:|
| Accepted | 6,636,108 | 572,420 |
| Turned away (429/503) | 825,378 | 2,696,671 |
| Lost | 0 | 0 |
| Duplicate webhooks | 10 | 970 |
| Finished without webhook | 144 | 1 |
| WebSocket commands completed | 370,017 (since 19:13) | 271,036 |
| WebSocket commands lost | 0 (since 19:13) | 0 |

Aquifer ran from 15:23 UTC. Spillway ran from 14:47 UTC, was down from about
15:30 to 19:06 (see below), and ran on 4GB from 19:06.

## What it found

**Aquifer: the run loop stalled at its minimum pace** (fixed in #48). After a
short target API outage halved the pace to the floor, the queue's run loop
slept out each dispatch interval in place and stopped reading completions and
new jobs, including its own webhook deliveries. It took about 17 minutes to
notice the target API had recovered. Run 2 was restarted on the fix at 15:23.

**Driver: WebSocket resend storm** (fixed in #51). The driver resent every
unanswered command every 15 seconds, which produced 2M resends and 70k false
"lost" commands. It now resends only after a reconnect.

**Aquifer: CPU-bound on one vCPU** (#54). With 500 jobs/s and 200 WebSocket
sessions, the target had no idle CPU, so the WebSocket backlog from each burst
never drained (command p50 40-150 s after 17:00). A CPU profile
(`aquifer-cpu-1819.pprof`) put 20.5% of CPU in the expiry cleanup loop, which
read every expired job twice and deleted each in its own synced write, and 9%
in idempotency lookups for keys that don't exist yet. #54 batched the cleanup
and added bloom filters. In the first 10 minutes after the deploy, command
p50 was 0.3-8 s; the CPU stayed at 100%.

**Driver: reused WebSocket session IDs** (fixed in #55). Burst sessions
reconnected under their old session ID with no cursor, so Aquifer replayed
each old transcript to them: 172,164 messages counted as "for commands this
driver never sent", plus extra load at every burst. "Lost" (no answer within
5 minutes) also counted late commands while p99 was near 300 s. Since the fix,
both are 0.

**Spillway: memory grows with retention until it can't boot** (spillway #38,
issue #39). Spillway keeps finished jobs in Mnesia `disc_copies` tables, which
live in RAM, for 30 minutes. At 500 jobs/s that outgrew the 2GB machine. Its
memory admission compared the limit with the VM's own count while resident
memory ran 1.14-1.4x higher, and this config had raised
`SPILLWAY_DB_MAX_BYTES` to 6GB, so nothing refused work before the kernel
killed it. Every boot then reloaded the 730MB store and was killed again: 81
restarts with no load. #38 evicts the oldest finished jobs under memory
pressure and checks resident memory. Spillway moved to 4GB with a fresh store.

**Spillway: throughput falls as the store grows.** On 4GB with #38, Spillway
was stable (no restarts) but accepted 300 jobs/s at 520MB, 200 at 815MB, 158
at 1.2GB and 71 during the WebSocket burst at 1.8GB, with target API and
webhooks healthy and the CPU at 100%. Its own benchmark notes 100k retained
jobs halving throughput, partly from Mnesia rewriting table files. Not yet
profiled: nothing could connect to the node (fixed in spillway #40). Run 3
profiles it.

## Open questions for run 3

- Is Aquifer still CPU-bound after #54, and how much of the change was the
  bloom filters (their effect grows as compaction rewrites old tables)?
- What takes Spillway's CPU as its store grows: Mnesia dumps, the WebSocket
  transcript tables, or something else?
- Both run on one vCPU. Is that a ceiling to measure or code to speed up?

## Files

`*-status.json`, `*-events.json`, `*-hourly.json` and `*-minutes.json` were
read from each driver at the end of the run. Aquifer's driver restarted at
19:13, so its minutes and events before then are in the `*-until-1813.json`
snapshots.
