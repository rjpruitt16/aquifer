# Reliability

Durable queue, automatic crash recovery, panic isolation per job. See [benchmark.md](../../benchmark.md) for the numbers behind these claims.


- **Durable queue**: jobs persist to the configured storage backend. With Pebble's default `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=100`, Aquifer answers before a write reaches disk, so a crash loses the jobs accepted since the last completed sync; set it to `0` for every write to wait for its own fsync (see [Configuration](configuration.md))
- **Crash recovery**: queued jobs re-dispatched automatically on restart
- **Panic recovery**: if a queue's loop panics, its unfinished jobs are re-enqueued from storage immediately, without waiting for a restart
- **Stale job safety net**: in-flight jobs older than 5 min automatically reset to `queued`
- **Per-job panic isolation**: a panic in one job marks it failed and delivers the webhook; the worker keeps running

**Job TTLs:**

| Status      | TTL    |
|-------------|--------|
| `queued`    | 24 h   |
| `completed` | 30 min |
| `failed`    | 2 h    |
