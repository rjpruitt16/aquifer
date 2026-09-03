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

**Ping payload:**

```json
{
  "port": "8080",
  "reported_at": "2026-09-03T22:00:00Z"
}
```
