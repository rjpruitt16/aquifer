# AGENTS.md

Aquifer is a Go load balancer for agentic workloads: durable per-tenant queues, dynamic pacing driven by `X-Aqueduct-*` headers from the upstream, `POST /proxy` direct-then-fallback, cluster routing, drain mode, Valkey-backed remote idempotency, and L8 signed (and optionally encrypted) webhooks.

## Related repos

These repos are developed together and are usually cloned as siblings under one folder (`SAAS/`). Before you search the web or guess, check whether the sibling exists locally at the path below and read it.

| Repo | Local path | GitHub | Role |
|---|---|---|---|
| aquifer | `../aquifer` | https://github.com/rjpruitt16/aquifer | Go load balancer for agentic workloads; reference implementation |
| ezthrottle-local | `../ezthrottle-local` | https://github.com/rjpruitt16/ezthrottle-local | Elixir/Phoenix sibling of Aquifer; kept feature-for-feature in sync |
| l8-protocol | `../l8-protocol` | https://github.com/rjpruitt16/l8-protocol | L8 spec (`index.md`, `spec.json`): handshake, signing, encryption, request schemas |
| aqueduct-runner | `../aqueduct-runner` | https://github.com/rjpruitt16/aqueduct-runner | Cross-repo contract tests (Dagger + Hurl) run against real Aquifer and ezthrottle-local containers |
| canalis-rs | `../canalis-rs` | https://github.com/rjpruitt16/canalis-rs | Rust control plane for Aquifer/ezthrottle-local fleets |

Shared contracts that must stay identical across Aquifer and ezthrottle-local: `X-Aqueduct-*` request/response headers, job JSON shape, idempotency hashing (`sha256(user_id + ":" + key)`, or `sha256("shared\0" + key)` for `idempotency_scope: "shared"`), drain ledger events, `POST /proxy` direct-then-fallback behavior, and L8. A change to any of these in one repo needs the matching change in the other, an update to `l8-protocol` if it touches L8, and ideally a contract test in `aqueduct-runner`.

## Commands

- Unit tests: `go test ./...` (add `-race` for concurrency changes). The macOS `ld: warning: ... malformed LC_DYSYMTAB` line is harmless.
- Python L8 integration (needs `pip install cryptography requests`): `make integration-test`, or run `tests/l8_receiver.py` + `tests/test_l8.py` by hand.
- Cross-repo contracts: from `../aqueduct-runner`, e.g. `make contract-test-aquifer`, `make contract-test-all`.

## Where things live

- `aquifer.go` (`PrepareJob`, `Enqueue`), `server.go` (HTTP routes), `proxy.go` (`/proxy`), `account_queue.go` (`execute`, `makeRequest`: the single upstream dispatch path).
- `job.go`: `JobRequest`, `Job`, validation, `dedupHash` (the only place idempotency hashes are derived).
- `store.go` (SQLite), `pebble_store.go`, `remote_idempotency.go` (Valkey), `cluster.go` (`clusterRoutingKey`).
- `l8.go`, `l8_encrypt.go`, `l8_schema.go`: L8 handshake, signing, payload encryption, request-schema validation.
- `API.md` is the endpoint and env-var reference; `README.md` is the overview.

## Conventions

- New behavior is opt-in: gate it behind an env flag that defaults off. Background loops and processes should not start at all when their flag is off.
- Work on a feature branch and open a PR for review. Do not push to `main` or merge.
- Commits: no `Co-Authored-By` or `Claude-Session` trailers.
- Docs prose: avoid em dashes outside titles and headings.
- Report failures and limits honestly in PR descriptions; don't overstate results.
