# External registration

**Off by default.** A normal deployment (a single long-lived instance, or static domain/tenant
partitioning as described in [README.md](README.md#deployment-model)) is completely unaffected unless
you explicitly turn this on — no background goroutine runs, no added overhead, nothing about default
behavior changes.

When `AQUIFER_REGISTRY_URL` is set, this instance periodically reports its own listening port to that
URL — so an external control plane (Canalis, or anything else someone builds against the same contract)
can assign tenants to it. Generic on purpose: Aquifer has no idea what's on the other end, and doesn't
need to — it's a webhook target, delivered through the same retrying, L8-signed path drain mode's ledger
flush already uses.

**Only the port is reported, not a full address.** Whatever receives the ping already sees the real
source IP on the connection itself — a self-reported address would just be redundant with that, and
less trustworthy (Aquifer claiming its own reachable address is a weaker guarantee than the receiver's
own connection telling it directly).

**Aquifer does not decide who gets assigned where.** That orchestration — assignment, scaling, the whole
control-plane layer — is entirely up to whatever service receives these pings. Aquifer only reports that
it exists and is reachable.

**Env vars:**

| Var | Default | Notes |
|---|---|---|
| `AQUIFER_REGISTRY_URL` | *(none)* | The real gate — unset means no goroutine ever starts. |
| `AQUIFER_REGISTRY_INTERVAL_SECONDS` | `15` | How often to ping. Pings once immediately on start too, so a freshly-booted instance doesn't sit unregistered for the first interval. |
| `PORT` | `8080` | Already read elsewhere (the port Aquifer itself listens on) — reused here rather than introducing a second way to say the same thing. |
| `AQUIFER_REGISTRY_RESERVED_FOR` | *(none)* | A `user_id` this instance is dedicated to. Reported as `reserved_for`; a control plane should route only that user here and never hand the instance to anyone else. |
| `AQUIFER_REGISTRY_ROLE` | *(none)* | What kind of traffic this instance is for, e.g. `http` or `ws` (lowercased). Reported as `role`; lets a control plane send WebSocket and HTTP traffic to instances dedicated to each. |

**Ping payload:**

```json
{
  "port": "8080",
  "state": "active",
  "reported_at": "2026-09-03T22:00:00Z",
  "reserved_for": "acme",
  "role": "http",
  "drain_state": "unassigned",
  "fullness": 35
}
```

`fullness` is how full this instance is, `0`–`100`: the same number as the `X-Aqueduct-Fullness` response header (see [API.md](API.md)), so a control plane knows before sending an instance anything.

`reserved_for` and `role` are only present when their env vars are set. `drain_state` is only present when drain mode is enabled ([DRAIN_MODE.md](DRAIN_MODE.md)): it's drain mode's own `active` / `draining` / `unassigned`, kept separate from `state` because drain mode's "draining" means "finishing one tenant's burst", not "shutting down". Reaching `unassigned` sends a ping right away instead of waiting for the next heartbeat, since that's the control plane's cue to hand this instance to someone else.

`state` is `active`, `draining`, or `offline`. Normal heartbeats report `active`. On `SIGTERM`, Aquifer immediately sends `draining` before its quiescence window so the control plane can stop assigning new work, then makes one best-effort `offline` report after accepted work and WebSockets have drained. Receivers should still expire nodes that stop heartbeating because abrupt termination cannot send either transition.
