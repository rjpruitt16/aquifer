# Deployment

Aquifer runs four ways: as a **sidecar** alongside your app, as a **standalone service** multiple services point to, **embedded directly as a Go library** in your own process (see [Framework adapters](adapters.md)), or as an **extension behind a Gateway API proxy** like Envoy Gateway in Kubernetes, where the proxy owns routing and TLS and Aquifer owns the queue behind it (see [examples/kubernetes](../../examples/kubernetes)). Each instance persists to its own volume (a Pebble directory by default, or a SQLite file when `AQUIFER_STORE_BACKEND=sqlite` or `DB_PATH` already holds a SQLite database), with no external database or coordination service to run.

People run Aquifer in front of things like: internal coding platforms (GitLab, Forgejo), CI runners, database read replicas, and MCP servers, anywhere a burst of agent or service traffic needs to hit something that has its own capacity limit.


See [Partitioning strategies](partitioning.md) above for how to assign tenants to instances, statically or dynamically.

See the [security warning](../../API.md#post-jobs) under `POST /jobs`; the same untrusted-caller risk applies regardless of deployment shape.


**Choosing a machine size:** with Pebble and the default 100ms flush, one Fly machine sustained about 1,000 jobs/s on performance-1x, 1,500 on 2x and 3,000 on 4x, end to end including webhooks. See [benchmark.md](../../benchmark.md#12-capacity-per-machine-pebble-flyio-2026-10-07) for the method and caveats, and [section 7](../../benchmark.md#7-capacity-and-drain-time) for earlier capacity work.
