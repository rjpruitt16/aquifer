# Drain mode

**Off by default.** A normal deployment (a single long-lived instance, or static domain/tenant
partitioning as described in [README.md](README.md#deployment-model)) is completely unaffected unless you explicitly turn this on — no
background watchdog runs, no added overhead, nothing about default behavior changes.

Aquifer's idempotency store exists to dedupe retries while a burst is actively draining, not to be a
permanent system of record. Drain mode is for a specific deployment pattern: instances get handed to a
tenant, absorb and drain their burst, then get freed for reassignment to a different tenant. When
enabled, and an instance goes completely idle (no requests anywhere on the whole process, not just one
tenant's queue) for `AQUIFER_DRAIN_TIMER_SECONDS`, Aquifer flushes everything it's deduped since the
last flush to a webhook, and only on confirmed delivery, clears its local ledger — making the instance
safe to hand to someone else.

**Aquifer does not decide who gets a freed instance next**, and does not retain the ledger itself
beyond the next flush. That orchestration — durable long-term storage, and assigning tenants to
instances — is entirely up to whatever service you build to receive this webhook. Aquifer only detects
idle and hands off what it has.

**State machine**, visible via `GET /health` (`"drain": {"state": "..."}`, only present when enabled):

| State | Meaning |
|---|---|
| `active` | At least one upstream has live work. Normal state, drain mode enabled or not. |
| `draining` | Every upstream has gone idle, but either the drain timer hasn't elapsed yet or a flush attempt is in flight/being retried. Not yet safe to hand off. |
| `unassigned` | The ledger was flushed (or there was nothing to flush) and local state is clear — safe to hand off. Reverts to `active` the instant new work arrives. |

`unassigned` is a status label, not an access gate — Aquifer keeps accepting new jobs in every state.
Nothing stops a job from landing on an instance mid-handoff; if your orchestrator needs a hard
guarantee that never happens, enforce it on your own end before routing traffic there.

**Env vars:**

| Var | Default | Notes |
|---|---|---|
| `AQUIFER_DRAIN_ENABLED` | `false` | The real gate — the other two vars are only read when this is `true`. |
| `AQUIFER_DRAIN_TIMER_SECONDS` | `45` | How long the whole instance must be idle before flushing. Deliberately separate from the per-tenant-queue self-GC timer below, which reclaims one queue's memory and has nothing to do with instance-wide handoff — but drain mode's own countdown only starts once every queue has already self-torn-down via that timer, so a real drain flush is gated by both. |
| `AQUIFER_DRAIN_WEBHOOK_URL` | *(none)* | Required if enabled — if unset, drain mode logs a warning and stays off rather than flushing with nowhere to send it. |
| `AQUIFER_IDLE_TIMEOUT_SECONDS` | `300` (5min) | The per-tenant-queue self-GC timer itself. Exists mainly so contract tests don't have to burn 5+ real minutes to prove a real drain flush — leave this at the default in production. |

**Webhook payload:**

```json
{
  "event": "instance_idle",
  "flushed_at": "2026-08-23T14:02:11Z",
  "ledger": [
    { "idempotent_key_hash": "3fa9c1...", "job_id": "a3f9...", "status": "completed" }
  ]
}
```

`idempotent_key_hash` is `sha256(user_id + ":" + idempotent_key)`, hex-encoded lowercase — the exact
hash Aquifer already computes internally, never the plaintext key. A downstream consumer re-checking a
key for a duplicate must hash it the same way.

If you're also running [ezthrottle-local](https://github.com/rjpruitt16/ezthrottle-local), its drain
mode hashes the identical way — both systems share one hash-key namespace for the same
`(user_id, idempotent_key)` pair, so a downstream consumer can hash lookups the same way regardless of
which system a given ledger entry came from.
