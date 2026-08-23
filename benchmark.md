# Benchmarks

Real runs against a live deployment, not simulated numbers. Target: `aquifer-bench.fly.dev`, a single `shared-cpu-1x`/512MB Fly.io machine in `iad`, `AQUIFER_MEMORY_LIMIT_MB=400`, default pacing (2 RPS/1 concurrent per upstream domain, no `CONFIG_PATH`). Load generated with [vegeta](https://github.com/tsenart/vegeta); scripts are in [`benchmark/`](benchmark/).

Scenarios 1-6 dispatch to a dummy upstream (`postman-echo.com`) at the default 2 req/s ceiling — intentional, since Aquifer protects upstreams that haven't told it otherwise. A large batch queues for a while before draining; that's the configured ceiling working as intended, not a bottleneck. Production sets real per-domain rates via `CONFIG_PATH`. Scenario 7 deliberately raises the ceiling so machine size, not the default rate limit, is what's measured.

---

## 1. Sustained throughput

Measures acceptance (`POST /jobs` → 201), not end-to-end dispatch — ingest and dispatch are decoupled by design.

**50 req/s for 30s (1,500 requests):**

```
Requests   1500, rate 50.03, throughput 49.93
Latencies  min 53.5ms, mean 69.8ms, p50 65.0ms, p90 72.2ms, p99 187.2ms, max 565.6ms
Success    100.00% | Status 201:1500
```

100% acceptance, ~65ms median latency, no degradation over the run.

---

## 2. Burst absorption (retry-storm simulation)

Baseline traffic, a 10x spike for 30s, then back to baseline.

```
Phase: baseline (10/s, 15s)  — 150 req, 100% success, p99 194ms
Phase: burst    (100/s, 30s) — 3000 req, 100% success, p99 166ms
Phase: recovery (10/s, 15s)  — 150 req, 100% success, p99 189ms
```

100% success through the entire 10x spike, no 5xx, no dropped connections. Recovery-phase latency matches baseline — the burst gets absorbed into the queue instead of hammering the upstream.

---

## 3. Admission control under memory pressure

Aquifer sheds load with clean `429`s (never 5xx, never a crash) once memory exceeds a configured ceiling.

At the realistic setting (`AQUIFER_MEMORY_LIMIT_MB=400`), a 150 req/s, 45s hammer (6,750 requests, 100% accepted) never pushed the process past ~21MB resident — the ceiling was never exercised, which is itself a useful data point about baseline memory footprint.

To demonstrate shedding, redeployed with `AQUIFER_MEMORY_LIMIT_MB=15` (deliberately below resting footprint):

```
600 requests, 30.05 req/s → 0.00% success, 429:600 (all rejected)
```

Every rejected request gets a clean `429` with a structured body and `Retry-After`:

```
HTTP/2 429
retry-after: 5
{"current":21,"error":"admission rejected: memory at 21 exceeds limit 15","limit":15,"limit_reason":"memory"}
```

No crash, no hang, no partial writes.

**One subtlety:** a rejected request's row is deleted — it was never durably accepted. The idempotency guarantee only covers jobs already accepted before the limit tripped, not ones that were shed. Verified by `TestAdmissionDuplicateStillSucceedsUnderPressure` and `TestAdmissionRejectedJobLeavesNoGhostRow`.

---

## 4. Crash recovery

30 jobs enqueued, machine `SIGKILL`'d ~3s in (mid-dispatch), restarted, polled for up to 60s:

```
enqueued: 30 | completed: 30 | failed: 0 | still queued: 0 | lost: 0
PASS: all 30 jobs reached a real terminal state
```

Every job persisted to SQLite before dispatch, so a kill mid-flight just means the recovery loop re-enqueues it on restart.

---

## 5. Multi-tenant fairness (`X-Aqueduct-Account-Queue`)

Without isolation, every job hitting the same upstream domain shares one dispatch queue — a noisy tenant can starve everyone behind it. Setting `X-Aqueduct-Account-Queue: enabled` isolates pacing per `(user_id, api_key)`.

**Surfaced a real bug during benchmarking:** the isolation mechanism existed, but the handler that turns it on was never actually wired into the HTTP request path — every job silently shared one queue regardless of the header. Filed and fixed as [issue #5](https://github.com/rjpruitt16/aquifer/issues/5), covered by `TestAccountQueueHeaderIsolatesTenants` and `TestAccountQueueHeaderOmittedSharesQueue`.

With the header now working — a noisy tenant flooding 100 concurrent jobs alongside a quiet tenant sending 5 steady jobs:

```
quiet jobs: 5s, 4s, 4s, 2s, 1s
```

completing on their own schedule regardless of the flood. Before the fix, the same scenario left the quiet tenant stuck for 37-52s each.

---

## 6. Retry-After backs off exponentially under sustained pressure

A single `429` returns the base value (default 5s). Consecutive rejections double it, capped at 60s; the next allowed request resets it. Verified against a live instance held over its memory ceiling:

```
attempt 1-5: retry-after = 5, 10, 20, 40, 60
```

This stops clients from hammering a fixed 5-second ceiling forever once an instance is genuinely overloaded, giving it room to drain.

---

## 7. Capacity and drain time

Started as "what machine size fits a given traffic shape." Found something bigger: a real correctness/scaling bug in the SQLite layer, unrelated to machine size.

**What the first pass showed:** `aquifer-bench` redeployed at 256/512/1024MB (1 vCPU held constant) — all three broke at the identical point: 100% success through 100 req/s, then ~68-73% at 200 req/s, with connection failures at only ~110MB memory use. A follow-up sweep across real CPU tiers (1/2/4 vCPU, paired memory) broke at the same point too: 68.3%, 68.3%, 69.5%. That ruled out memory and CPU as the bottleneck.

**The real cause:** `store.go` had `db.SetMaxOpenConns(1)` — every request queued behind one SQLite connection regardless of machine size. Raising the pool size alone didn't fix it either: WAL/busy-timeout pragmas were being set via `db.Exec()` on whichever single connection served that call, so opening more connections meant most of them silently ran with SQLite's unsafe defaults and failed under concurrency. Fixed by moving the pragmas into the connection DSN itself, so every pooled connection gets them.

That fix surfaced a third issue: `CheckOrInsert` checked for duplicates via `INSERT OR IGNORE` then a separate `SELECT` — non-atomic, so a raced `INSERT` could make the `SELECT` find nothing and report a **brand-new job as a duplicate with an empty job ID**, silently discarding it. Latent the whole time; only triggerable once the pool had more than one connection. Fixed by reading `INSERT`'s own `RowsAffected()` instead of a racy read-after-write. Covered by `TestStoreHandlesConcurrentInserts` (50 concurrent inserts, unique keys, none misreported).

**After the fix**, same 1 vCPU/512MB instance:

| Rate | Before fix | After fix |
|------|-----------|-----------|
| 25-100/s | 100% success | 100% success |
| 200/s | ~68-73% success, connection refused | 74-100% success (variance), no refusals — remaining failures are slow, not refused |
| 400/s | *(not tested — already broken at 200)* | 15.4% success, mean latency 28.6s, memory 107→264MB — a real, load-driven ceiling |

**Drain time** (500 jobs against an idle instance) was identical across every configuration, before and after the fix: 75-79 seconds — governed by the configured dispatch pace, not CPU/RAM.

**Takeaway:** a ceiling that doesn't move when you change the resource you'd expect to relieve it is a signal to check the code path, not the instance size.

### Checklist for picking a size

- [ ] **Traffic sustains under ~200 req/s?** Any size works, including 256MB.
- [ ] **Traffic sustains above ~200-300 req/s?** Re-run `benchmark/capacity_by_size.sh` against your own traffic shape first — verify the ceiling is actually hardware-bound, not code-level.
- [ ] **Bursts followed by quiet periods?** A 500-job burst drains in ~75-79s at a 50 RPS dispatch pace, regardless of machine size — scale linearly against your own `CONFIG_PATH` rate.
- [ ] **A ceiling looks identical no matter what you scale?** Treat it as a code-path question, not a bigger-machine question.
- [ ] **Need per-tenant fairness under a shared upstream?** Set `X-Aqueduct-Account-Queue: enabled`.

---

## 8. An alternative storage backend: Pebble

ezthrottle-local (Elixir/Mnesia) showed a materially higher throughput ceiling than SQLite here — Mnesia writes memory-first with disk catching up in the background, SQLite is disk-first and single-writer. That raised the question: would a memory-first store change Aquifer's ceiling too, without giving up Go?

Added `PebbleStore` ([cockroachdb/pebble](https://github.com/cockroachdb/pebble), pure Go, no CGo) behind the `JobStore` interface, opt-in via `AQUIFER_STORE_BACKEND=pebble` — default stays SQLite, additive not a rewrite.

**A durability gap Pebble doesn't warn you about:** the assumption going in was that `WriteOptions.Sync: false` behaves like SQLite's `synchronous=NORMAL` (writes through to the OS, only relaxes fsync). Wrong — caught by the same write/`kill -9`/restart/read test used throughout this suite:

```
sync_write: {:atomic, :ok}
# kill -9 immediately after, restart
[JOB 1] WAL file stopped reading at offset 0; replayed 0 keys
read err: pebble: not found
```

Unlike SQLite, `Sync: false` never writes through to the OS at all. `Sync: true` is required for any crash-durability guarantee; confirmed it survives the identical kill test once set. Pebble's `WALMinSyncInterval` (its own group-commit) batches concurrent syncs into fewer real fsyncs while still blocking each caller until its own write is durable — no silent-loss window. Configurable via `AQUIFER_PEBBLE_WAL_SYNC_INTERVAL_MS` (default 5ms). Verified: 30/30 jobs survive a real `kill -9` on Fly.io, matching SQLite and Mnesia.

Idempotency also needed explicit handling: Pebble has no insert-if-absent primitive, so a naive `Get`-then-`Set` would reproduce the same race already fixed in SQLite and Mnesia. Guarded by a lock striped across 256 shards keyed by idempotent hash. Covered by `TestPebbleStoreHandlesConcurrentInserts`.

**Throughput**, same tier, same ramp:

| Rate | SQLite (post-fix) | Pebble |
|------|--------------------|--------|
| 50/s | 100% success, mean 69.6ms | 100% success, mean 72.7ms |
| 200/s | 74-100% success (variance) | 100% success, mean 1.68s |
| 400/s | 15.4% success — real ceiling | 100% success, mean 11.4s |
| 600/s | *(already broken)* | 66.6% success |
| 800/s | *(already broken)* | 40.0% success |
| 1000/s | *(already broken)* | 17.2% success |

Pebble roughly doubles the point where Aquifer stops reliably completing requests (~200-400 req/s vs. ~400-600 req/s). The failure mode differs too: SQLite fails outright past its ceiling; Pebble keeps succeeding well past its comfort zone, just slower — arguably the better mode for reliable eventual delivery, though it makes Pebble's "ceiling" fuzzier to define than SQLite's hard wall.

**CPU scaling:** 4 vCPUs instead of 1, same rates — essentially no difference (400/s: 11.4s → 10.7s mean; 600/s: 66.6% → 60.4%; 800/s: 40.0% → 38.1%, all within noise). The opposite of Mnesia's result. Likely reason: Pebble's WAL/memtable write path is a single ordered pipeline per instance — faster than SQLite's B-tree, but still fundamentally serialized, unlike Mnesia's per-request independent BEAM processes. More cores help when the bottleneck is application concurrency; they don't when it's a storage engine's own internal serialization.

**Takeaway:** Pebble is a legitimate, meaningfully faster alternative to SQLite for this workload (`AQUIFER_STORE_BACKEND=pebble`), not a magic unlock past ~600 req/s — that ceiling is the storage engine's serialized commit path. Horizontal partitioning remains the right lever past that point, for either backend.

---

## 9. GPU inference and the retry tax (RunPod/vLLM)

Everything above dispatches to a dummy HTTP echo upstream. This one targets a real GPU: `Qwen/Qwen2.5-1.5B-Instruct` on a single RunPod RTX PRO 4500 Blackwell running vLLM (`--max-num-seqs 48`, KV cache deliberately capped at 512MB so a realistic burst can actually pressure it), pacing driven entirely by the ORCA fallback signal (see [README.md](README.md), "Dynamic pacing") — `kv_cache_usage_perc` on vLLM's own response headers, no `X-Aqueduct-*` config needed. Requests forced to `min_tokens=max_tokens=300` so each one holds GPU/KV-cache resources long enough to matter, not a one-token echo.

**Same offered load (40 req/s for 30s), direct-to-vLLM vs. through Aquifer:**

| | Direct | Through Aquifer |
|---|---|---|
| Client success | 100% | 100% |
| Client mean latency | 11.2s | 1.0ms (durable enqueue, dispatch decoupled) |
| Client p99 latency | 20.1s | 3.2ms |
| Peak vLLM-side queue depth | 449 waiting | 8 waiting |
| Peak `kv_cache_usage_perc` | 69.4% | 59.1% |

Hitting vLLM directly at 40 req/s doesn't fail — vLLM queues everything internally rather than rejecting — but its own backlog balloons to 449 requests deep, and callers wait up to 20 seconds for a response that eventually succeeds. That's the retry tax: technically 100% success, but exactly the shape of slowness that makes a caller give up and retry, doubling load on a GPU that's already behind. Through Aquifer, the burst is durably absorbed in ~1ms per request and dispatched to vLLM at a controlled pace; vLLM's own internal queue barely builds.

**Does the ORCA signal itself actually engage** (not just a static rate cap doing the work)? Loosened Aquifer's static ceiling above what vLLM can sustain (80 RPS / 120 concurrent vs. vLLM's real ~48-concurrent capacity) and pushed a sustained 60 req/s for 40s. `kv_cache_usage_perc` oscillated 22% → 79% → 22% → ... in a repeating cycle: cache crosses the 70% threshold, ORCA tells Aquifer to cut to 2 RPS, backlog drains, cache falls, Aquifer ramps back up (5%/dispatch recovery), cache climbs again. A real, self-regulating feedback loop against a live GPU, not a static number that happened to be tuned right.

**Takeaway:** the durable-queue-plus-paced-dispatch model already prevents the worst of the retry tax on its own; ORCA adds automatic, no-config self-regulation on top for backends that report their own load, which vLLM already does.

---

## Reproducing these results

```bash
cd benchmark
./throughput.sh <target-url> 50 30s
./burst.sh <target-url> 10 100
./admission_degradation.sh <target-url> 150 45s
./crash_recovery.sh <target-url> <fly-app-name> 30
./fairness.sh <target-url> 100

# Capacity/drain by machine size -- much slower (multiple full redeploys),
# run separately, not part of the GitHub Action's regular pass:
./capacity_by_size.sh <target-url> <fly-app-name> "256 512 1024" 500

# GPU retry tax -- needs a real vLLM instance (RunPod or otherwise),
# not part of the regular pass either:
./gpu_retry_tax.sh <vllm-url> <aquifer-url> 40 40 300
```

For Pebble instead of SQLite: deploy with `-e AQUIFER_STORE_BACKEND=pebble -e DB_PATH=/data/pebble` and run the same scripts unchanged.
