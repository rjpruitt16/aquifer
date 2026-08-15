# Writing an adapter

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
