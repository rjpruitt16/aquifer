#!/usr/bin/env python3
"""Render benchmark.md from raw output captured by the benchmark/*.sh scripts.

Each --<scenario> flag points at a text file holding that scenario's stdout
verbatim. The prose framing is static (explains what each test proves); the
code blocks are always the real captured output from the run, never
fabricated or hand-edited.
"""
import argparse


def read(path):
    if not path:
        return "(not run)"
    with open(path) as f:
        return f.read().strip()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--throughput")
    p.add_argument("--burst")
    p.add_argument("--crash-recovery")
    p.add_argument("--fairness")
    p.add_argument("--admission-provoked")
    p.add_argument("--out", default="benchmark.md")
    args = p.parse_args()

    throughput = read(args.throughput)
    burst = read(args.burst)
    crash_recovery = read(args.crash_recovery)
    fairness = read(args.fairness)
    admission_provoked = read(args.admission_provoked)

    doc = f"""# Benchmarks

Real runs against a live deployment, not simulated numbers. Target: `aquifer-bench.fly.dev`,
a single `shared-cpu-1x` / 512MB Fly.io machine in `iad`, default pacing config
(2 RPS / 1 concurrent per upstream domain — no `CONFIG_PATH` set). Load generated with
[vegeta](https://github.com/tsenart/vegeta); scripts are in [`benchmark/`](benchmark/).
Regenerated automatically by [`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml)
on manual dispatch — every number below came from an actual run against a fresh instance.

---

## 1. Sustained throughput

Can Aquifer accept and durably persist requests at a steady rate without ingest-side latency
creeping up? This measures acceptance (`POST /jobs` returning 201), not end-to-end dispatch —
ingest and dispatch are decoupled by design.

```
{throughput}
```

---

## 2. Burst absorption (retry-storm simulation)

Baseline traffic, then a 10x spike, then back to baseline — a simulated retry storm. Success
means the burst window absorbs cleanly and the recovery window looks just like baseline.

```
{burst}
```

---

## 3. Admission control under memory pressure

The safety mechanism: Aquifer sheds load with clean `429`s (never 5xx, never a crash) once
memory exceeds a configured ceiling. This run deliberately sets `AQUIFER_MEMORY_LIMIT_MB` below
the process's own resting footprint to force shedding — not representative of a normal
deployment, just a way to prove the mechanism actually engages.

```
{admission_provoked}
```

**One subtlety worth stating explicitly:** if a request is rejected by admission control, its
row is deleted — it was never durably accepted. Retrying the *same* `idempotent_key` while the
system is still over the limit can get rejected again; the idempotency guarantee ("a retried job
succeeds") only covers jobs that were already durably accepted before the limit tripped, not ones
that were shed. Verified directly by `TestAdmissionDuplicateStillSucceedsUnderPressure` and
`TestAdmissionRejectedJobLeavesNoGhostRow` in the test suite.

---

## 4. Crash recovery

Durability isn't a claim until it's demonstrated: enqueue jobs, `SIGKILL` the machine mid-drain,
restart it, confirm every job reaches a real terminal state. Run in isolation on a freshly
deployed instance so no other scenario's backlog interferes.

```
{crash_recovery}
```

---

## 5. Multi-tenant fairness (`X-Aqueduct-Account-Queue`)

Without per-tenant isolation, every job hitting the same upstream domain shares one dispatch
queue — a single noisy tenant can starve every other tenant behind it. Setting
`X-Aqueduct-Account-Queue: enabled` (or the `X-Aquifer-*` alias) on a request isolates pacing per
`(user_id, api_key)`. Covered by `TestAccountQueueHeaderIsolatesTenants` and
`TestAccountQueueHeaderOmittedSharesQueue`, which assert on the actual queue bucketing rather
than just that the code compiles.

```
{fairness}
```

The quiet tenant's jobs should complete on their own schedule — a few seconds each — regardless
of a concurrent flood from a noisy tenant on the same domain.

---

## Reproducing these results

```bash
cd benchmark
./throughput.sh <target-url> 50 30s
./burst.sh <target-url> 10 100
./admission_degradation.sh <target-url> 150 45s
./crash_recovery.sh <target-url> <fly-app-name> 30
./fairness.sh <target-url> 100
```

Or trigger the `Benchmark` GitHub Action manually from the Actions tab to regenerate this file
against a fresh deployment.
"""

    with open(args.out, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()
