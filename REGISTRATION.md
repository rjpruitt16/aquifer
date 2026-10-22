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
| `AQUIFER_REGISTRY_POOLS` | *(none)* | Comma-separated pools this instance joins, e.g. `gpu,eu`. Reported as `pools`; a control plane like Canalis only sends it requests for those pools. None means the `default` pool. Every ping carries the current list, so changing it (and restarting) is how an instance joins or leaves a pool. |
| `AQUIFER_REGISTRY_ROLE` | *(none)* | What kind of traffic this instance is for, e.g. `http` or `ws` (lowercased). Reported as `role`; lets a control plane send WebSocket and HTTP traffic to instances dedicated to each. |

**Ping payload:**

```json
{
  "port": "8080",
  "state": "active",
  "reported_at": "2026-09-03T22:00:00Z",
  "reserved_for": "acme",
  "role": "http",
  "pools": ["gpu", "eu"],
  "drain_state": "unassigned",
  "fullness": 35,
  "http_users": 12,
  "http_capacity": 70,
  "ws_sessions": 100,
  "ws_capacity": 1000
}
```

`fullness` is how full this instance is, `0`–`100`: the same number as the `X-Aqueduct-Fullness` response header (see [API.md](API.md)), so a control plane knows before sending an instance anything.

### Load: HTTP users and WebSocket sessions

`http_users` is how many users have jobs or webhook deliveries still owed on this instance, and `ws_sessions` how many WebSocket clients are connected. `http_capacity` and `ws_capacity` are how many of each it can hold. A control plane like Canalis gives the instance no more HTTP users or sessions than that, counting each kind on its own, so open sessions never use up the room HTTP users need. Every response carries the same four numbers as `X-Aqueduct-Http-Users`, `X-Aqueduct-Http-Capacity`, `X-Aqueduct-WS-Sessions` and `X-Aqueduct-WS-Capacity`.

Both kinds of traffic spend the same CPU, so the capacities come from one budget: what one kind isn't using, the other can have. At most every 5 seconds, Aquifer reads the CPU time the process used, and how many jobs and WebSocket messages it handled. Readings are averaged over about 30 seconds. The CPU in use is split between the two kinds by what each costs, measured in [benchmark.md](benchmark.md): about 750 jobs/s and 1,500 WebSocket round trips/s per CPU, which is 3,000 messages, since each round trip is a client message and a backend one. That gives the CPU one user or one session costs, and:

```
headroom      = 80% of CPU - CPU in use
http_capacity = http_users + headroom / CPU per user          (at most 10,000)
ws_capacity   = the least of  ws_sessions + headroom / CPU per session
                              ws_sessions + memory left under AQUIFER_MEMORY_LIMIT_MB / 90 KB
                              AQUIFER_WS_MAX_CLIENT_CONNECTIONS
```

Neither number is ever below what the instance already has. With nothing to measure yet, a user is taken to send 10 jobs/s and a session 1 message/s: an idle 1-CPU instance reports 60 users and 1,000 sessions, an idle 2-CPU instance 120 users. Spillway reports the same four numbers the same way, with its own costs.

`reserved_for`, `role` and `pools` are only present when their env vars are set. `drain_state` is only present when drain mode is enabled ([DRAIN_MODE.md](DRAIN_MODE.md)): it's drain mode's own `active` / `draining` / `unassigned`, kept separate from `state` because drain mode's "draining" means "finishing one tenant's burst", not "shutting down". Reaching `unassigned` sends a ping right away instead of waiting for the next heartbeat, since that's the control plane's cue to hand this instance to someone else.

`state` is `active`, `draining`, or `offline`. Normal heartbeats report `active`. On `SIGTERM`, Aquifer immediately sends `draining` before its quiescence window so the control plane can stop assigning new work, then makes one best-effort `offline` report after accepted work and WebSockets have drained. Receivers should still expire nodes that stop heartbeating because abrupt termination cannot send either transition.
