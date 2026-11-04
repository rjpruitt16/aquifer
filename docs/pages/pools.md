# Pools

Instead of dispatching to a fixed `url`, a job can target a named **pool**: a group of registered service instances Aquifer picks from at dispatch time, weighted by declared capacity and live reputation. Useful when you have several interchangeable backends instead of one fixed endpoint, and it grows or shrinks automatically as members register, degrade, or drop out, with no need to reconfigure Aquifer as your fleet autoscales.


**Registering a member:**

```bash
curl -X POST https://your-aquifer/pools/writers/members \
  -d '{"member_id": "writer-1", "address": "http://10.0.1.5:8080", "capacity_rps": 20, "heartbeat_interval_seconds": 30}'
```

The same call is both initial registration and heartbeat: call it again periodically (at roughly your declared `heartbeat_interval_seconds`) to stay in the pool. Missing several consecutive expected heartbeats evicts a member. A member can register under more than one pool id.

**Dispatching to a pool:**

```json
{
  "user_id": "user-123",
  "idempotent_key": "job-1",
  "pool_id": "writers",
  "method": "POST",
  "webhook_url": "https://yourapp.com/webhooks/aquifer"
}
```

`pool_id` and `url` are mutually exclusive: a job sets exactly one.

**How a member gets picked:** proportional to `capacity_rps × reputation`, not equal-split round robin: a member declaring 100 RPS gets roughly 4x the dispatches of one declaring 25. The pool's aggregate ceiling is the live sum of every member's current effective rate, so it grows and shrinks automatically as members register, degrade, or drop out, with no need to reconfigure Aquifer as your fleet autoscales.

**Reputation**: a dispatch failure halves a member's effective share; a successful dispatch nudges it back up, and heartbeats recover it more slowly after a restart. A member isn't evicted on one bad response, only once its reputation has stayed at the floor continuously, with no interrupting success, for a sustained window. This avoids flapping a member in and out of the pool over a single transient error.

**Treat `5xx` carefully.** Aquifer interprets connection errors and `5xx` responses as reliability signals for the selected member. One `5xx` does not fail the job by itself: Aquifer records failure for that member and retries another member when possible. If every retry across the pool still ends in connection errors or `5xx`, the job is marked failed. That behavior is intentional for reliability, but it means application bugs that accidentally return `5xx` on a new code path can reduce that member's traffic share or eventually remove it from the pool.

For overload, prefer explicit backpressure over generic server errors:

| Situation | Recommended signal |
|-----------|--------------------|
| Instance is healthy but needs less traffic | `X-Aqueduct-Rps` or `X-Aqueduct-Max-Concurrent` |
| Request should be retried later due to pressure | `429` with `Retry-After` |
| Instance/code path is actually failing | `5xx` |

Roll new members into a pool gradually. Start new versions with a conservative `capacity_rps`, send a small share of traffic first, watch `/health` reputation and your own error metrics, then raise capacity as confidence grows. Blue/green or canary rollout matters more here than with a blind round-robin balancer because Aquifer uses runtime failures as routing input.

**Set `capacity_rps` conservatively, not at your true theoretical max.** Aquifer only learns a member died via a failed dispatch or a missed heartbeat, both of which lag the actual failure, so leaving headroom in what you declare gives real slack for that detection delay. Reputation decay is a second line of defense on top of this: a member that's silently struggling gets throttled down by observed failures even if its last-declared capacity was optimistic.

**Watch the model locally:** Aquifer includes a local scenario harness that starts one fake backend server with multiple logical workers, registers them as pool members, and prints per-second traffic, failures, dynamic header values, and reputation.

```bash
go run ./cmd/aquifer-scenario --scenario mixed --workers 10 --jobs 500 --duration 30s --rps 50
```

Scenarios: `steady`, `weighted`, `flapping`, `backpressure`, `recovering`, `mixed`, and `harsh`. The `harsh` scenario penalizes sustained overload by adding latency, then `5xx`, then simulated crash windows. Add `--mode regular` to compare against a simple round-robin load balancer model that retries on `5xx` but ignores Aquifer reputation and dynamic pacing headers.

Pool state isn't shared across Aquifer instances; see [Deployment model](deployment.md) for how that constrains a given `pool_id` to one instance.

`GET /health` reports every pool's current members, their declared capacity, and current reputation.
