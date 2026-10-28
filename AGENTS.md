# AGENTS.md

Aquifer is a Go load balancer for agentic workloads: durable per-tenant queues, dynamic pacing driven by `X-Aqueduct-*` headers from the target API, `POST /proxy` direct-then-fallback, cluster routing, drain mode, Valkey-backed remote idempotency, and L8 signed (and optionally encrypted) webhooks.

## Related repos

These repos are developed together and are usually cloned as siblings under one folder (`SAAS/`). Before you search the web or guess, check whether the sibling exists locally at the path below and read it.

| Repo | Local path | GitHub | Role |
|---|---|---|---|
| aquifer | `../aquifer` | https://github.com/rjpruitt16/aquifer | Go load balancer for agentic workloads; reference implementation |
| spillway | `../spillway` | https://github.com/rjpruitt16/spillway | Elixir/Phoenix sibling of Aquifer; kept feature-for-feature in sync |
| l8-protocol | `../l8-protocol` | https://github.com/rjpruitt16/l8-protocol | L8 spec (`index.md`, `spec.json`): handshake, signing, encryption, request schemas |
| aqueduct-runner | `../aqueduct-runner` | https://github.com/rjpruitt16/aqueduct-runner | Cross-repo contract tests (Dagger + Hurl) run against real Aquifer and Spillway containers |
| canalis-rs | `../canalis-rs` | https://github.com/rjpruitt16/canalis-rs | Rust control plane for Aquifer/Spillway fleets |

Shared contracts that must stay identical across Aquifer and Spillway: `X-Aqueduct-*` request/response headers, job JSON shape, idempotency hashing (`sha256(user_id + ":" + key)`, or `sha256("shared\0" + key)` for `idempotency_scope: "shared"`), drain ledger events, `POST /proxy` direct-then-fallback behavior, and L8. A change to any of these in one repo needs the matching change in the other, an update to `l8-protocol` if it touches L8, and ideally a contract test in `aqueduct-runner`.

## Commands

- Unit tests: `go test ./...` (add `-race` for concurrency changes). The macOS `ld: warning: ... malformed LC_DYSYMTAB` line is harmless.
- Python L8 integration (needs `pip install cryptography requests`): `make integration-test`, or run `tests/l8_receiver.py` + `tests/test_l8.py` by hand.
- Cross-repo contracts: from `../aqueduct-runner`, e.g. `make contract-test-aquifer`, `make contract-test-all`.

## Where things live

- `aquifer.go` (`PrepareJob`, `Enqueue`), `server.go` (HTTP routes), `proxy.go` (`/proxy`).
- `internal/dispatch`: the dispatch path. `Registry` (one `URLWorker` per target API), `AccountQueue` (per-tenant queues; `execute` and `MakeRequest` are the single target API dispatch path), pools, the SSE broker, webhook delivery, drain mode and registration. Root `dispatch.go` keeps `Registry`, `NewRegistry`, `Config` and the rest as aliases.
- `internal/jobs`: `Job`, `Request`, `Result`, validation, `DedupHash` (the only place idempotency hashes are derived). Root `job.go` keeps `Job`, `JobRequest`, `JobResult` and the status and retry constants as aliases.
- `internal/storage`: the `Store` interface, `SQLite`, `Pebble`, `Open` (picks the backend), and `Remote` (the shared idempotency and result ledger in Valkey). Root `store.go` keeps `JobStore`, `Store`, `PebbleStore` and their constructors as aliases.
- `cluster_routing.go` (`clusterRoutingKey`).
- `l8/` (public package `github.com/rjpruitt16/aquifer/l8`): L8 handshake, signing, payload encryption, request-schema validation. `l8_compat.go` keeps the old root names (`NewL8Registry`, `L8Registry`, ...) as aliases.
- `internal/websocket`: the durable WebSocket proxy (`Manager`, Valkey transcripts, follower, scheduler, transcript export). `websocket.go` keeps the old root names (`WebSocketManager`, `NewWebSocketManager`, ...) as aliases.
- `internal/admission` (admission limits, fair admission, inbound rate), `internal/region` (region discovery, redirect helpers), `internal/cluster` (membership, routing, Valkey coordination, `State`), `internal/metrics` (the Prometheus adapter, `TargetStats`), `internal/orca` (ORCA load headers). Root `admission.go`, `region.go`, `cluster.go`, `lifecycle.go` and `metrics.go` keep the old root names as aliases.
- `mcpadapter/` and `a2aadapter/`: the MCP and A2A adapters, outside the root package.
- `internal/env`, `internal/httpx`, `internal/retry`, `internal/nodeid`, `internal/urlpolicy`: small helpers shared by the root and the internal packages (env parsing, JSON errors and drain responses, jitter, node ID, the URL allowlist).
- `internal/` packages can't be imported from outside this module, so they can change freely; the root package and `l8/` are the public API.
- `API.md` is the endpoint and env-var reference; `README.md` is the overview.

## Cross-repo changes and CI

- Contract-test CI picks branches by name. In aquifer and spillway it checks out aqueduct-runner at the branch with the same name as the PR's branch when one exists, otherwise `main`. aqueduct-runner's own CI does the same for each backend. Pushes to `main` always use `main`. The `Pick the ... branch` step's log says which branch was used.
- For a change that spans repos (for example a behavior change in Aquifer and Spillway plus its contract in aqueduct-runner), use the same branch name in every repo involved. Each PR's CI then tests against the others' unmerged code.
- Merge order: merge whatever the other side's CI calls first. A runner contract that needs new backend behavior merges after the backends; a backend workflow step that calls a new runner function merges after the runner. Branches are deleted on merge, so a PR that merges second falls back to the other repo's `main`, which by then has the change.
- Before merging, confirm every CI job is green (`gh pr checks`), contract tests included. Local `go test` / `mix test` passing is not enough.

## Conventions

- New behavior is opt-in: gate it behind an env flag that defaults off. Background loops and processes should not start at all when their flag is off.
  - Exception: inbound pacing (`X-Aqueduct-Rps` on `/jobs` and `/proxy` responses) is always on, by the owner's decision. A caller that ignores the header loses nothing, and a gateway like Canalis in front of Aquifer should get pacing without configuration.
- Work on a feature branch and open a PR for review. Do not push to `main` or merge.
- Commits: no `Co-Authored-By` or `Claude-Session` trailers.
- Docs prose: avoid em dashes outside titles and headings.
- Report failures and limits honestly in PR descriptions; don't overstate results.
