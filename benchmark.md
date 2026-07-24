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

## 7. Capacity and drain time by machine size

The question this answers: **given a traffic shape, what machine size actually fits it, and if a burst exceeds that, how long until the queue is caught up again?**

Methodology: `aquifer-bench` redeployed at `shared-cpu-1x` with `--vm-memory` set to 256, 512, and 1024MB in turn — CPU was deliberately held constant at **1 shared vCPU across all three sizes** (only memory was varied), so any difference in the results isolates the effect of RAM alone. Per-domain dispatch pacing was raised to 50 RPS / 20 concurrent via a dedicated `Dockerfile.bench` + `bench-config.yml` (see `fly.capacity.toml`) — the standard 2 RPS default would otherwise cap drain speed identically regardless of machine size, making it impossible to see anything. The admission memory ceiling was set to 80% of each box's total RAM.

**Ingest ramp** (sustained rate held for 15s per step, each size tested on a freshly deployed instance):

| Rate | 256MB | 512MB | 1024MB |
|------|-------|-------|--------|
| 25/s | 100% success, mem 7→13MB | 100% success, mem 7→13MB | 100% success, mem 7→13MB |
| 50/s | 100% success, mem 13MB | 100% success, mem 13MB | 100% success, mem 13MB |
| 100/s | 100% success, mem 13MB | 100% success, mem 13MB | 100% success, mem 13MB |
| 200/s | **69.6%** success, mem 13→111MB | **72.7%** success, mem 13→111MB | **68.0%** success, mem 13→107MB |

**All three sizes broke at the identical point** — 200 req/s, roughly 70% success, memory only 107-111MB (nowhere near even the smallest box's 204MB admission ceiling). The failures at that rate are connection-level (`vegeta` status code `0` — connection refused/timeout, not a `429`), meaning the instance became unresponsive before its own memory-based admission control ever got a chance to shed load cleanly. Since CPU was held constant across all three configurations, this is strong evidence that **on `shared-cpu-1x`, the real ceiling is the single shared vCPU, not RAM.** Giving Aquifer more memory alone does not raise this ceiling — it needs more CPU (`shared-cpu-2x`/`4x`), and/or `AQUIFER_MEMORY_LIMIT_MB` set low enough to shed load with clean `429`s before hitting the CPU wall, not just before hitting an unreachable memory number.

**Drain time** (fire 500 jobs as fast as possible against a freshly deployed, otherwise idle instance, then time until every job reaches `completed`):

| Size | Drained | Time |
|------|---------|------|
| 256MB | 500/500 | 75s |
| 512MB | 500/500 | 76s |
| 1024MB | 500/500 | 77s |

Also identical across all three sizes (within noise) — consistent with the ramp finding: drain throughput is bound by the same single shared vCPU regardless of how much RAM sits behind it. A 500-job burst against a domain paced at 50 RPS / 20 concurrent takes just over a minute to fully clear on any of these three configurations.

**Practical takeaway:** for this workload shape (small JSON jobs, external HTTP dispatch), machine memory size alone doesn't move the ceiling — CPU does. If your traffic sustains under ~100-150 req/s, any of these sizes (even 256MB) handles it identically; there's no reason to pay for more RAM on this axis alone. If you need higher sustained ingest, the fix is more shared CPU, not more RAM. Regardless of size, a burst of 500 queued jobs clears in about a minute and 15 seconds at a 50 RPS dispatch pace — scale that linearly for your own `CONFIG_PATH` rate to estimate your own catch-up time after a spike.

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
