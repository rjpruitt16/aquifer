# Configuration

Rate limits are set per target API hostname via a YAML file (`CONFIG_PATH`) and admission/runtime behavior via env vars.


Set `CONFIG_PATH` to a YAML file to configure rate limits per target API hostname. `rps` is the host's total across all tenants. `max_concurrent` applies to each tenant's queue, so with N busy tenants up to N × `max_concurrent` requests can be in flight at the host. A key is matched against the job URL's host exactly, port included: for `http://your-backend.internal:8080`, the key is `your-backend.internal:8080`. A URL whose host matches no key gets `defaults`.

```yaml
# aquifer.yml — copy from examples/aquifer.example.yml
defaults:
  rps: 2
  max_concurrent: 1

upstreams:
  api.openai.com:
    rps: 10
    max_concurrent: 3
  api.stripe.com:
    rps: 20
    max_concurrent: 5
  your-backend.internal:
    rps: 50
    max_concurrent: 10
```

| Env var       | Default      | Description                    |
|---------------|--------------|--------------------------------|
| `AQUIFER_ADAPTER` | `http` for binary, `mcp-stdio` in Docker image | Runtime adapter: `http`, `mcp-stdio`, or `a2a` |
| `AQUIFER_A2A_PUBLIC_URL` | `http://localhost:$PORT` | A2A adapter only: externally-reachable base URL advertised in the Agent Card |
| `PORT`        | `8080`       | HTTP listen port               |
| `DB_PATH`     | `aquifer.db` | Storage path: a directory for Pebble (the default), or a file for SQLite |
| `CONFIG_PATH` | _(none)_     | Path to rate limit config YAML |
| `AQUIFER_STORE_BACKEND` | `pebble` | Storage engine: `pebble` (pure-Go LSM store, the faster backend; see [benchmark.md](../reference/benchmark.md)) or `sqlite`. If `DB_PATH` is an existing SQLite file, Aquifer stays on SQLite so an upgrade doesn't orphan queued jobs |
| `AQUIFER_PEBBLE_WAL_SYNC_INTERVAL_MS` | `5` | Pebble only: batches concurrent durable writes into fewer real fsyncs under load (Pebble's own group-commit); each caller still blocks until its own write is actually durable |
| `AQUIFER_PEBBLE_FLUSH_INTERVAL_MS` | `100` | Pebble only: answer before a write reaches disk and sync the log on this timer. A crash loses the jobs accepted since the start of the last completed sync: normally about this many milliseconds, but a sync slowed by heavy writes stretches it (seconds in the Canalis soak). `AQUIFER_MAX_SYNC_LAG_MS` and inbound pacing (`X-Aqueduct-Rps`, see [API.md](../reference/API.md#inbound-pacing-x-aqueduct-rps)) keep it short; `aquifer_wal_sync_lag_seconds` shows it. On Fly it took one machine from about 150 to about 700 jobs/s end to end (see [benchmark.md](../reference/benchmark.md#11-per-job-overhead-make-perf)). Set `0` to make every write wait for its own fsync: zero acknowledged-job loss, much lower throughput |
| `AQUIFER_PEBBLE_CACHE_MB` | `64` | Pebble only: block cache size. Pebble's own default (8MB) is too small to keep recently written jobs in memory, so status updates read them back from disk |
| `AQUIFER_DEBUG_ADDR` | unset | Serves Go's `net/http/pprof` (goroutine counts, heap, CPU profiles) on its own listener, e.g. `127.0.0.1:6060`. Off when unset. Bind it to loopback or a private network; it is not meant to be public |
| `AQUIFER_MEMORY_LIMIT_MB` | 70% of the container's memory limit, or of the machine's memory | Reject new jobs with `429` once the memory the Go runtime is holding (mapped from the OS, minus what it has returned) exceeds this many MB. It falls back below the limit once a spike's memory is returned, so intake reopens. `GET /health` shows it as `admission.memory_mb`, beside `memory_reserved_mb` (everything ever mapped). The default is logged at startup and is only a guess: if anything else runs on the machine (Valkey, a sidecar) or the machine is resized, set it yourself. `0` disables it, and then nothing is refused before the kernel's OOM killer acts. Off by default where memory can't be read (macOS). |
| `AQUIFER_RECOVERY_PAGE_SIZE` | `1000` | On restart, each queue loads this many of its unfinished jobs before Aquifer starts serving; the rest stay on disk, counted in its backlog, and load a page at a time as it drains. Restart memory then doesn't grow with the backlog. Jobs stored by versions before this load all at once, as before, the first time. |
| `AQUIFER_MAX_SYNC_LAG_MS` | `1000` | Reject new jobs with `429` (`sync_lag`) while accepted jobs may have gone this long without reaching disk. With the default Pebble flush (`AQUIFER_PEBBLE_FLUSH_INTERVAL_MS=100`), an accepted job is written to disk within about 100ms; a crash loses what was accepted since the last sync, so this stops adding to that window when the machine is too busy to keep syncing. Jobs accepted just before still wait for the sync in progress, so the window can outlast this limit while one sync is slow. `0` disables it. `GET /health` shows `admission.sync_lag_ms`. |
| `AQUIFER_MAX_BODY_BYTES` | `1048576` (1MB) | Reject oversized request bodies with `413` |
| `AQUIFER_DB_MAX_BYTES` | 80% of the disk | Reject new jobs with `429` once the database exceeds this size. Unset, Aquifer sizes it at startup to 80% of the space the database can use (free space on its filesystem plus its current size), so it fits the volume without configuration. The startup log shows the value. Set a number to override, or `0` to disable. Falls back to 800MB where disk space can't be read |
| `AQUIFER_MAX_PENDING_PER_UPSTREAM` | `10000` | Shared per-target-API backlog budget used by fair admission; `0` disables it |
| `AQUIFER_ACCOUNT_QUEUE` | `on` | `on`: each tenant gets its own queue at every target API; `off`: tenants share one queue per target API. A target API's own `X-Aqueduct-Account-Queue` response header overrides this; a sender can only ask for `on` |
| `AQUIFER_METRICS_TENANT_LABELS` | `false` | `true` adds per-tenant (`user_id`) series to `GET /metrics`, for the first 1,000 tenants seen (the rest share `other`). Off by default because tenant IDs are unbounded and often customer data. See [API.md](../reference/API.md#get-metrics) |
| `AQUIFER_NODE_ID` | `AQUIFER_CLUSTER_SELF_ID`, else `FLY_MACHINE_ID`, else the host name | This node's id, sent to target APIs as `X-Aqueduct-Node-Id` so a backend can count how many Aquifer nodes are sending to it |
| `AQUIFER_RETRY_AFTER_SECONDS` | `5` | Base `Retry-After` value sent on `429` admission rejections |
| `AQUIFER_IDLE_TIMEOUT_SECONDS` | `300` (5min) | How long a per-tenant/per-domain queue can sit idle before self-tearing-down; see [drain mode](partitioning.md) for why this gates a real drain flush |
| `AQUIFER_ALLOWED_URL_DOMAINS` | _(none, unrestricted)_ | Comma-separated hostnames `url`-routed jobs are permitted to target; see [`POST /jobs`](../reference/API.md#post-jobs) |
| `AQUIFER_FLY_REGIONS` | _(none, feature off)_ | Comma-separated Fly region codes this app is deployed to; enables `/proxy`'s cross-region redirect on Fly. See [`POST /proxy`](../reference/API.md#post-proxy) |
| `AQUIFER_FLY_POLL_INTERVAL_SECONDS` | `30` | How often to poll sibling regions over Fly's private network for liveness |
| `AQUIFER_REDIRECT_GATE_COOLDOWN_SECONDS` | `500` | How long to stop attempting cross-region redirect after a tour finds no reachable region at all, before trying again: internal probe throttling, not what's told to the caller |
| `AQUIFER_REDIRECT_EXHAUSTED_RETRY_AFTER_SECONDS` | `900` (15min) | `Retry-After` sent to the caller when cross-region redirect is configured but exhausted: no known-live region could serve or queue the request. Request is rejected (429), not queued locally. See [`POST /proxy`](../reference/API.md#post-proxy) |

Body-size, DB-size and memory admission are all on by default. Memory defaults to 70% of the container's limit (or the machine's memory); `AQUIFER_MEMORY_LIMIT_MB=0` turns it off. Retry-After backs off exponentially under sustained rejection (5s → 10s → 20s → 40s → capped at 60s, resets on the next allowed request). See [CONFIGURATION.md](../reference/CONFIGURATION.md) for the full rationale and [benchmark.md](../reference/benchmark.md) for the numbers behind these defaults.
