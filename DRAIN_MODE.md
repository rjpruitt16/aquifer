# Drain mode

**Off by default.** A normal deployment (a single long-lived instance, or static domain/tenant
partitioning as described in [README.md](README.md#deployment-model)) is completely unaffected unless you explicitly turn this on — no
background watchdog runs, no added overhead, nothing about default behavior changes.

Aquifer's idempotency store exists to dedupe retries while a burst is actively draining, not to be a
permanent system of record. Drain mode is for a specific deployment pattern: instances get handed to a
tenant, absorb and drain their burst, then get freed for reassignment to a different tenant. When
enabled, Aquifer records completed/failed user jobs into a local drain-event journal. It can stream
that journal in acknowledged batches while the instance is still active, and when the instance goes
completely idle (no requests anywhere on the whole process, not just one tenant's queue) for
`AQUIFER_DRAIN_TIMER_SECONDS`, it flushes any remaining events and only then clears its local ledger —
making the instance safe to hand to someone else.

**Aquifer does not decide who gets a freed instance next**, and does not retain the ledger itself
beyond the next flush. That orchestration — durable long-term storage, and assigning tenants to
instances — is entirely up to whatever service you build to receive this webhook. Aquifer only detects
idle and hands off what it has.

**[canalis-rs](https://github.com/rjpruitt16/canalis-rs) is a real example of that orchestrator, and of
why draining to a genuinely stateless handoff is worth building.** A freed instance carries no memory
of who it served last, so canalis-rs can hand it to any tenant currently waiting — durably queuing
that tenant's work in Valkey if none is free yet — without needing this instance recreated or
reconfigured first. Scaling the fleet becomes adding or removing interchangeable instances, not
reprovisioning per-tenant ones.

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
| `AQUIFER_DRAIN_ENABLED` | `false` | The real gate — the other drain vars only matter when this is `true`. |
| `AQUIFER_DRAIN_TIMER_SECONDS` | `45` | How long the whole instance must be idle before flushing. Deliberately separate from the per-tenant-queue self-GC timer below, which reclaims one queue's memory and has nothing to do with instance-wide handoff — but drain mode's own countdown only starts once every queue has already self-torn-down via that timer, so a real drain flush is gated by both. |
| `AQUIFER_DRAIN_SINK` | `webhook` | `webhook` posts batch payloads to `AQUIFER_DRAIN_WEBHOOK_URL`; `valkey` writes idempotency entries directly to Valkey. |
| `AQUIFER_DRAIN_WEBHOOK_URL` | *(none)* | Required when `AQUIFER_DRAIN_SINK=webhook` — if unset, drain mode logs a warning and stays off rather than flushing with nowhere to send it. |
| `AQUIFER_DRAIN_BATCH_ENABLED` | `false` | When true, Aquifer periodically sends pending drain events before the final idle flush. |
| `AQUIFER_DRAIN_BATCH_INTERVAL_SECONDS` | `60` | Periodic batch interval. |
| `AQUIFER_DRAIN_BATCH_MAX_EVENTS` | `1000` | Maximum events sent in one webhook payload. |
| `AQUIFER_VALKEY_URL` | *(none)* | Required when `AQUIFER_DRAIN_SINK=valkey`, and also used by remote idempotency lookup. Supports `redis://` and `valkey://` URLs. |
| `AQUIFER_REMOTE_IDEMPOTENCY_PREFIX` | `aqueduct:idempotency:` | Key prefix for Valkey idempotency entries. |
| `AQUIFER_REMOTE_IDEMPOTENCY_TTL_SECONDS` | `7200` | TTL for remote idempotency entries written to Valkey. |
| `AQUIFER_REMOTE_RESULT_ENABLED` | `false` | Also writes bounded terminal result snapshots to Valkey. |
| `AQUIFER_REMOTE_RESULT_PREFIX` | `aqueduct:result:` | Key prefix for terminal result snapshots. |
| `AQUIFER_REMOTE_RESULT_MAX_BYTES` | `65536` | Maximum response-body bytes stored in each result snapshot; `0` stores metadata only. |
| `AQUIFER_IDLE_TIMEOUT_SECONDS` | `300` (5min) | The per-tenant-queue self-GC timer itself. Exists mainly so contract tests don't have to burn 5+ real minutes to prove a real drain flush — leave this at the default in production. |

**Webhook payloads:**

Periodic batches use `event: "ledger_batch"`:

```json
{
  "event": "ledger_batch",
  "batch_id": "12-48",
  "sequence_start": 12,
  "sequence_end": 48,
  "flushed_at": "2026-08-23T14:02:11Z",
  "ledger": [
    {
      "sequence": 12,
      "idempotent_key_hash": "3fa9c1...",
      "job_id": "a3f9...",
      "status": "completed",
      "recorded_at": 1798053731000
    }
  ]
}
```

The final idle flush uses the same payload shape with `event: "instance_idle"`.

```json
{
  "event": "instance_idle",
  "batch_id": "49-50",
  "sequence_start": 49,
  "sequence_end": 50,
  "flushed_at": "2026-08-23T14:02:11Z",
  "ledger": [
    {
      "sequence": 49,
      "idempotent_key_hash": "3fa9c1...",
      "job_id": "a3f9...",
      "status": "completed",
      "recorded_at": 1798053731000
    }
  ]
}
```

Aquifer deletes drain events only after the webhook returns `2xx`. A failed delivery leaves the batch
in local storage and retries it later. The downstream receiver should still treat `(instance_id or
sender identity, sequence)` or `batch_id` as idempotent, because webhook delivery remains
at-least-once across process/network failures.

When `AQUIFER_DRAIN_SINK=valkey`, Aquifer skips the webhook payload and writes each event directly to:

```txt
{AQUIFER_REMOTE_IDEMPOTENCY_PREFIX}{idempotent_key_hash}
```

The value is:

```json
{
  "job_id": "a3f9...",
  "status": "completed",
  "recorded_at": 1798053731000,
  "source": "aquifer",
  "result_key": "aqueduct:result:..."
}
```

Those Valkey writes are treated the same way as webhook delivery: only successful writes are
acknowledged and deleted from Aquifer's local drain-event journal.

If `AQUIFER_REMOTE_RESULT_ENABLED=true`, Aquifer also writes:

```txt
{AQUIFER_REMOTE_RESULT_PREFIX}{idempotent_key_hash}
```

with the terminal response status, content type, body up to `AQUIFER_REMOTE_RESULT_MAX_BYTES`,
and a `body_truncated` flag. The idempotency entry's `result_key` points at that snapshot.

`idempotent_key_hash` is `sha256(user_id + ":" + idempotent_key)`, hex-encoded lowercase — the exact
hash Aquifer already computes internally, never the plaintext key. A downstream consumer re-checking a
key for a duplicate must hash it the same way.

If you're also running [ezthrottle-local](https://github.com/rjpruitt16/ezthrottle-local), its drain
mode hashes the identical way — both systems share one hash-key namespace for the same
`(user_id, idempotent_key)` pair, so a downstream consumer can hash lookups the same way regardless of
which system a given ledger entry came from.
