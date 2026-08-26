# Adapters

Aquifer has a framework-neutral core — idempotency, persistence, rate control, dispatch, SSE events, L8 signing, webhook delivery — with pluggable front doors. This covers the three built-in adapters, then how to write your own.

## Built-in adapters

| Adapter | Env | Purpose |
|---------|-----|---------|
| HTTP | `AQUIFER_ADAPTER=http` | REST/SSE API on `PORT` — see [API.md](API.md) |
| MCP stdio | `AQUIFER_ADAPTER=mcp-stdio` | MCP server exposing Aquifer tools over stdio |
| A2A | `AQUIFER_ADAPTER=a2a` | Agent2Agent protocol (v1.0) agent over JSON-RPC/HTTPS on `PORT` |

### MCP stdio

```bash
AQUIFER_ADAPTER=mcp-stdio aquifer
```

The published Docker image defaults to `AQUIFER_ADAPTER=mcp-stdio` so MCP directories such as Glama can start and introspect it directly. Set `AQUIFER_ADAPTER=http` when running Aquifer as an HTTP queue service.

MCP tools:

| Tool | Purpose |
|------|---------|
| `aquifer_enqueue_job` | Queue an HTTP request for durable, rate-controlled dispatch |
| `aquifer_get_job` | Fetch job status and metadata |
| `aquifer_health` | Return health and protocol metadata |
| `aquifer_l8_metadata` | Return L8 public key metadata |
| `aquifer_l8_challenge` | Answer an L8 challenge |

MCP resource: `aquifer://jobs/{job_id}` reads current job status and metadata as JSON. The HTTP adapter remains the default for the binary, so existing deployments do not change.

### A2A

Run as an A2A agent (JSON-RPC/HTTPS only for v1 — gRPC and REST bindings aren't wired up):

```bash
AQUIFER_ADAPTER=a2a AQUIFER_A2A_PUBLIC_URL=http://localhost:8080 aquifer
```

`AQUIFER_A2A_PUBLIC_URL` is the externally-reachable base URL to advertise in the Agent Card — it defaults to `http://localhost:$PORT`, which is only correct for local use; set it explicitly behind any proxy or real deployment. The Agent Card is served at `/.well-known/agent-card.json` (the standard A2A convention); send a `SendMessage`/`SendStreamingMessage` request whose message contains a single data part shaped like `JobRequest` (`user_id`, `idempotent_key`, `url` or `pool_id`, `method`, `headers`, `body`) — the same structured-JSON shape MCP's `aquifer_enqueue_job` tool already takes. The upstream response comes back as a task artifact. `CreateTaskPushNotificationConfig` is supported (backed by the SDK's SSRF-hardened HTTP sender); `CancelTask` deliberately returns an unsupported-operation error rather than a silent no-op, since Aquifer has no real job-cancellation mechanism yet. See [a2aadapter/a2a_adapter.go](a2aadapter/a2a_adapter.go) for the full translation between A2A's task model and Aquifer's `Enqueue`/`SubscribeJob`.

## Writing an adapter

Adapter authors import Aquifer as a Go package, implement `FrameworkAdapter`, and pass the shared core into their framework. Built-in adapters are selected with `AQUIFER_ADAPTER`; third-party adapters normally ship as small custom binaries that call `aquifer.RunAdapter`.

```go
package myframework

import (
    "context"

    "github.com/rjpruitt16/aquifer"
)

type Adapter struct{}

func (a *Adapter) Name() string {
    return "my-mcp-framework"
}

func (a *Adapter) Start(ctx context.Context, app *aquifer.Aquifer) error {
    // Register framework handlers that call:
    // app.Enqueue(req)
    // app.GetJob(jobID)
    // app.SubscribeJob(jobID)
    // app.Health()
    return nil
}
```

Custom binaries can reuse Aquifer's runtime wiring:

```go
package main

import (
    "context"
    "log"

    "github.com/rjpruitt16/aquifer"
    myadapter "github.com/you/your-adapter"
)

func main() {
    runtime := aquifer.NewRuntime(aquifer.RuntimeOptions{
        DBPath:     "aquifer.db",
        ConfigPath: "aquifer.yml",
    })
    runtime.RecoverQueuedJobs("aquifer.db")

    adapter := myadapter.New()
    log.Fatal(adapter.Start(context.Background(), runtime.Aquifer))
}
```

For the shortest form, let Aquifer create the runtime and start your adapter:

```go
adapter := myadapter.New()
log.Fatal(aquifer.RunAdapter(context.Background(), adapter, aquifer.RuntimeOptions{
    DBPath:     "aquifer.db",
    ConfigPath: "aquifer.yml",
}))
```

See `examples/custom_adapter` for a complete compile-tested adapter binary.

## Writing a storage backend

Persistence is also pluggable. Every core component (`Registry`, `AccountQueue`, `URLWorker`, `Aquifer` itself) is coded against the `JobStore` interface, not the concrete SQLite/Pebble types:

```go
type JobStore interface {
    Path() string
    Close() error
    CheckOrInsert(job *Job) (string, bool)
    SetQueueKey(jobID, queueKey string)
    DeleteJob(jobID string)
    MarkInFlight(jobID string)
    RecoverInFlight(queueKey string) []*Job
    UpdateStatus(jobID string, status Status)
    Counts() StoreCounts
    GetJob(jobID string) *Job
    GetQueuedJobs() []*Job
}
```

Implement it against your own backend (Postgres, rqlite, or anything else that can give you atomic check-and-set) and pass it via `RuntimeOptions.Store` — no need to bypass `NewRuntime`/`RunAdapter` or hand-wire the lower-level constructors. Two things a custom backend should be aware of: `CheckOrInsert` needs the same atomicity guarantee SQLite's `INSERT OR IGNORE` and Pebble's own store give it today (a non-atomic check-then-write reintroduces the exact idempotency race this project has already found and fixed twice — once in each language); and `AQUIFER_DB_MAX_BYTES` admission control does a local `os.Stat` on `DB_PATH`, which is meaningless for a networked backend — set it to `0` to disable that check if your store isn't a local file or directory.

Aquifer doesn't ship a Postgres or rqlite backend itself — this is documented as an extension point for anyone who wants multi-instance durability without local-disk-per-instance, not a promise one exists yet.

## Metrics adapter

Aquifer emits lifecycle events through a pluggable metrics adapter — implement `MetricsAdapter` and pass it into `NewRegistry`:

```go
type MetricsAdapter interface {
    JobQueued(userID, upstream string)
    JobDispatched(userID, upstream string)
    JobCompleted(userID, upstream string, durationMs int64)
    JobFailed(userID, upstream string, reason string)
    WebhookDelivered(url string, attempt int)
    WebhookFailed(url string, attempts int)
    QueueDepth(upstream string, depth int)
    FlowRate(upstream string, rps float64)
}
```

Aquifer ships with `NoopMetricsAdapter`, so existing deployments do not change.
