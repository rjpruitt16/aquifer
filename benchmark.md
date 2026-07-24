# Benchmarks

Real runs against a live deployment, not simulated numbers. Target: `aquifer-bench.fly.dev`, a single `shared-cpu-1x` / 512MB Fly.io machine in `iad`, `AQUIFER_MEMORY_LIMIT_MB=400`, default pacing config (2 RPS / 1 concurrent per upstream domain — no `CONFIG_PATH` set). Load generated with [vegeta](https://github.com/tsenart/vegeta); scripts are in [`benchmark/`](benchmark/).

**A note on the default pacing config:** scenarios 1-6 below dispatch to the same dummy upstream (`postman-echo.com`) at the out-of-the-box default of 2 requests/sec per upstream domain — intentional, since Aquifer's whole job is to protect upstreams that haven't told it otherwise. That means a large batch of jobs against one domain will queue for a while before fully draining; it's not a bottleneck in Aquifer itself, it's the configured ceiling doing exactly what it's supposed to. Production deployments set real per-domain rates via `CONFIG_PATH` (see the README's Configuration section). Scenario 7 (capacity by machine size) is the exception — it deliberately raises the pacing ceiling so machine size, not the default rate limit, is what's being measured; see that section for details.

---

## 1. Sustained throughput

Can Aquifer accept and durably persist requests at a steady rate without ingest-side latency creeping up? This measures acceptance (`POST /jobs` returning 201), not end-to-end dispatch — ingest and dispatch are decoupled by design.

**50 req/s for 30s (1,500 requests):**

```
Requests      [total, rate, throughput]         1500, 50.03, 49.93
Duration      [total, attack, wait]             30.042s, 29.98s, 62.016ms
Latencies     [min, mean, 50, 90, 95, 99, max]  53.541ms, 69.83ms, 64.969ms, 72.174ms, 76.454ms, 187.179ms, 565.627ms
Success       [ratio]                           100.00%
Status Codes  [code:count]                      201:1500
```

100% acceptance, ~65ms median latency, p99 under 190ms. No degradation over the run.

---

## 2. Burst absorption (retry-storm simulation)

Baseline traffic, then a 10x spike for 30s, then back to baseline — a simulated retry storm. Success means the burst window absorbs cleanly and the recovery window looks just like baseline.

```
## Phase: baseline (rate=10/s duration=15s)
Requests      [total, rate, throughput]  150, 10.07, 10.02
Latencies     [50, 95, 99]               63.967ms, 72.986ms, 194.462ms
Success       [ratio]                    100.00%
Status Codes                             201:150

## Phase: burst (rate=100/s duration=30s)
Requests      [total, rate, throughput]  3000, 100.04, 99.84
Latencies     [50, 95, 99]               66.196ms, 75.881ms, 165.757ms
Success       [ratio]                    100.00%
Status Codes                             201:3000

## Phase: recovery (rate=10/s duration=15s)
Requests      [total, rate, throughput]  150, 10.07, 10.02
Latencies     [50, 95, 99]               65.014ms, 110.103ms, 188.517ms
Success       [ratio]                    100.00%
Status Codes                             201:150
```

100% success through the entire 10x spike — every request accepted and queued, no 5xx, no dropped connections. Recovery-phase latency matches baseline; the burst left no residual damage. This is the headline retry-storm claim: the spike gets absorbed into the queue instead of hammering the upstream or falling over.

---

## 3. Admission control under memory pressure

The headline safety mechanism: Aquifer sheds load with clean `429`s (never 5xx, never a crash) once memory exceeds a configured ceiling.

At the realistic production setting (`AQUIFER_MEMORY_LIMIT_MB=400`), this single-instance workload never got the process past ~21MB resident even under a 150 req/s, 45s sustained hammer (6,750 requests, 100% accepted) — the ceiling was never exercised. That's a real, useful data point on its own: Aquifer's baseline memory footprint is small, so a 400MB ceiling gives a wide safety margin before shedding kicks in on real workloads.

To actually demonstrate the shedding behavior, the same deployment was redeployed with `AQUIFER_MEMORY_LIMIT_MB=15` (deliberately below its own resting footprint) — a provoked test, not representative of a normal deployment:

```
Requests      [total, rate, throughput]  600, 30.05, 0.00
Success       [ratio]                    0.00%
Status Codes  [code:count]               429:600

## First rejection
  rejected (429): 600 / 600
  first 429 at: 2026-07-24T08:27:49-07:00
```

Every request over the ceiling gets a clean `429` with a structured body and a `Retry-After` header:

```
HTTP/2 429
retry-after: 5
content-type: application/json

{"current":21,"error":"admission rejected: memory at 21 exceeds limit 15","limit":15,"limit_reason":"memory"}
```

No crash, no hang, no partial writes — the process stays responsive and reports exactly why it rejected the request.

**One subtlety worth stating explicitly:** if a request is rejected by admission control, its row is deleted — it was never durably accepted. Retrying the *same* `idempotent_key` while the system is still over the limit can get rejected again; the idempotency guarantee ("a retried job succeeds") only covers jobs that were already durably accepted before the limit tripped, not ones that were shed. This is verified directly by `TestAdmissionDuplicateStillSucceedsUnderPressure` and `TestAdmissionRejectedJobLeavesNoGhostRow` in the test suite — a job accepted *before* pressure hits still succeeds on retry *after* pressure hits; a job that was never accepted does not get a free pass just because it's been retried.

---

## 4. Crash recovery

Durability isn't a claim until it's demonstrated: enqueue jobs, `SIGKILL` the machine mid-drain, restart it, confirm every job reaches a real terminal state.

30 jobs enqueued against a clean instance, machine killed ~3s in (mid-dispatch), restarted, polled for up to 60s:

```
jobs enqueued:        30
completed:            30
failed (but tracked): 0
still queued after 60s: 0
not found (lost):      0

PASS: all 30 jobs survived the crash and drained to a real terminal state
(completed or failed) — not just 'still present'.
```

Zero jobs lost, zero stuck. Every job persisted to SQLite before dispatch, so a kill mid-flight just means the recovery loop re-enqueues it on restart — no client-visible failure, no orphaned work.

---

## 5. Multi-tenant fairness (`X-Aqueduct-Account-Queue`)

Without per-tenant isolation, every job hitting the same upstream domain shares one dispatch queue — a single noisy tenant can starve every other tenant behind it. Aquifer isolates pacing per `(user_id, api_key)` when a request sets `X-Aqueduct-Account-Queue: enabled` (or the `X-Aquifer-*` alias).

**This scenario surfaced a real bug during benchmarking, now fixed.** The isolation mechanism (`AccountQueue`) existed in the code, but the method that turns it on (`handleAccountQueueHeader`) was never called anywhere in the HTTP request path — there was no way for a client to actually reach it. Every job silently shared one queue regardless of the header. Filed and fixed as [issue #5](https://github.com/rjpruitt16/aquifer/issues/5): the HTTP adapter now reads `X-Aqueduct-Account-Queue`/`X-Aquifer-Account-Queue` off the request and wires it through `Registry.Enqueue` to the worker before dispatch. Covered by two new tests (`TestAccountQueueHeaderIsolatesTenants`, `TestAccountQueueHeaderOmittedSharesQueue`) that assert on the actual queue bucketing, not just that the code compiles.

With the header now set on every request, a noisy tenant flooding 100 concurrent jobs against the same upstream domain, alongside a quiet tenant sending 5 steady jobs one second apart:

```
quiet job 0: status=completed elapsed=5s
quiet job 1: status=completed elapsed=4s
quiet job 2: status=completed elapsed=4s
quiet job 3: status=completed elapsed=2s
quiet job 4: status=completed elapsed=1s
```

The quiet tenant's jobs complete on their own schedule — a few seconds each — regardless of the 100-job flood happening concurrently on the same domain. Before the fix, this same scenario left the quiet tenant's jobs stuck for 37-52 seconds each, queued behind the noisy tenant's backlog, because there was no way to actually turn isolation on.

---

## 6. Retry-After backs off exponentially under sustained pressure

A single `429` returns the configured base `Retry-After` (default 5s). Consecutive rejections — no allowed request in between — double it, capped at 60s. The moment a request is allowed again, it resets. Verified against a live instance deliberately held over its memory ceiling, five requests in a row:

```
attempt 1: retry-after=5
attempt 2: retry-after=10
attempt 3: retry-after=20
attempt 4: retry-after=40
attempt 5: retry-after=60
```

The point is to stop clients from hammering a fixed 5-second ceiling forever once an instance is genuinely overloaded — that hammering pattern is exactly what prevents an overloaded instance from ever catching up. Spreading retries out over time gives it room to drain.

---

## 7. Capacity and drain time — and a real bug this test found

The question this started out answering: **given a traffic shape, what machine size actually fits it, and if a burst exceeds that, how long until the queue is caught up again?** It ended up finding something more important than a capacity number: a real correctness/scaling bug in the SQLite layer, unrelated to machine size at all.

### What the first pass showed

`aquifer-bench` was redeployed at `shared-cpu-1x` with `--vm-memory` set to 256, 512, and 1024MB — CPU held constant at 1 shared vCPU across all three so any difference would isolate RAM. All three broke at the **identical point**: 100% success through 100 req/s, then ~68-73% success at 200 req/s, with connection-level failures (`vegeta` code `0`, not a clean `429`) at only ~107-111MB of actual memory use — nowhere near even the smallest box's admission ceiling. A follow-up sweep varying real Fly.io CPU tiers instead (1 vCPU/256MB, 2 vCPU/512MB, 4 vCPU/1024MB — each the official paired memory for that tier) broke at the **same exact point too**: 68.3%, 68.3%, and 69.5% respectively. Quadrupling the CPU allocation changed nothing.

That ruled out both memory *and* CPU as the bottleneck. Something else was capping every configuration at the same place.

### The real cause: one SQLite connection for the whole process

`store.go` had `db.SetMaxOpenConns(1)` — every HTTP request that touches the database (which is every request) queued behind a single connection, regardless of how much CPU or memory the box had. That's a code-level ceiling, not a hardware one, and it explains why five different machine configurations all broke at the same rate.

Raising the pool size on its own didn't fully fix it: SQLite pragmas (`journal_mode=WAL`, `busy_timeout`) were being set via `db.Exec()` right after `sql.Open()`, which only configures whichever single connection happens to service that call. With `MaxOpenConns(1)` there was only ever one connection to configure, so this worked by accident. The moment the pool can open more than one, every additional connection silently runs with SQLite's defaults — no WAL, no busy_timeout — and fails instantly with `SQLITE_BUSY` under any real concurrency. The fix moves the pragmas into the connection DSN itself (`file:path?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)`), so every connection the pool ever opens gets them.

Fixing *that* surfaced a third, more serious issue: `CheckOrInsert` decided "is this a duplicate?" by running `INSERT OR IGNORE` and then a separate `SELECT` to see what got written — two non-atomic statements. Under genuine concurrency (only possible once the pool was more than one connection), if the `INSERT` raced or transiently failed, the follow-up `SELECT` could find nothing, and the function would report a **brand-new job as a duplicate with an empty job ID** — silently discarding it. This had been latent in the code the whole time; it just could never trigger while every request was serialized through one connection. The fix reads the `INSERT`'s own `RowsAffected()` as the source of truth (atomic, race-free) instead of trusting a read-after-write that can race. Covered by `TestStoreHandlesConcurrentInserts`, which fires 50 concurrent inserts with unique keys and asserts none are misreported as duplicates.

### What changed after the fix

Same 1 vCPU / 512MB instance, same ramp sequence, post-fix:

| Rate | Before fix | After fix |
|------|-----------|-----------|
| 25-100/s | 100% success | 100% success |
| 200/s | ~68-73% success, connection refused | 74-100% success (run-to-run variance), no more connection refusals — failures that remain are slow responses, not refused ones |
| 400/s | *(not tested before — was already broken at 200)* | 15.4% success, mean latency 28.6s, memory 107→264MB — a real, load-driven ceiling |

200 req/s went from a **hard, repeatable failure** (every single one of five separate machine configurations hit ~68-73% and never higher) to a **usually-clean, occasionally-marginal** rate — a substantial reliability improvement, though not a guarantee at that exact rate. 400 req/s is unambiguously over a real capacity edge post-fix: memory climbs meaningfully (107→264MB, genuine backlog, not a connection-pool artifact) and latency saturates vegeta's 30s timeout. That's what an actual hardware/throughput ceiling looks like, as opposed to what a single-connection bug looks like (identical failure point regardless of machine size).

**Drain time** (fire 500 jobs as fast as possible against a freshly deployed, otherwise idle instance) was identical across every machine/CPU configuration tested, before and after the fix — 75-79 seconds regardless of size. This makes sense once the ceiling is understood: drain throughput here is governed by the configured dispatch pace (50 RPS / 20 concurrent in this test's `bench-config.yml`), not by how much CPU or RAM sits behind it. Scale that pace linearly against your own `CONFIG_PATH` rate to estimate your own catch-up time after a burst.

**Practical takeaway:** don't reach for a bigger machine to fix a throughput ceiling before checking whether it's actually a resource limit. This one looked exactly like "needs more CPU" — identical breakdown point across 256MB/512MB/1024MB *and* across 1/2/4 shared vCPUs — and was actually a single hardcoded database connection. The lesson generalizes: a ceiling that doesn't move when you change the resource you'd expect to relieve it is a signal to look at the code path, not the instance size.

---

## Reproducing these results

```bash
cd benchmark
./throughput.sh <target-url> 50 30s
./burst.sh <target-url> 10 100
./admission_degradation.sh <target-url> 150 45s
./crash_recovery.sh <target-url> <fly-app-name> 30
./fairness.sh <target-url> 100

# Capacity/drain by machine size — much slower (multiple full redeploys),
# run separately, not part of the GitHub Action's regular pass:
./capacity_by_size.sh <target-url> <fly-app-name> "256 512 1024" 500
```

Each script is self-contained bash + vegeta + a little Python for JSON shaping. See [`benchmark/`](benchmark/) for source.
