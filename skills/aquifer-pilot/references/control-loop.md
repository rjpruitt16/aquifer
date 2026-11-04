# The control loop

Part of the aquifer-pilot skill, step 3. Read it when the backend will send pacing headers, after a static-ceiling pass.

Aquifer follows the backend. The backend should run the control loop on the headers it returns. **Additive increase, multiplicative decrease** (AIMD) is a good default: TCP congestion control uses it, and so does Aquifer for its own intake (`internal/admission/inbound_rate.go`).

- While healthy, raise the advertised rate by a fixed step: `X-Aqueduct-Rps: <rate>`, and optionally `X-Aqueduct-Max-Concurrent`.
- On a sign of saturation, cut it by a factor (×0.5–0.7) and wait a round trip before cutting again.
- Remember the rate where trouble started (TCP's ssthresh). Grow quickly below it and slowly above it.
- Prefer `429` or a lower `X-Aqueduct-Rps` for "busy, slow down". Use `5xx` only for real failures.

Aquifer reacts on its own as well: each retryable failure (`5xx`, `429`, `408`) halves that queue's rate, and each success recovers it by 5% toward the ceiling.

The right saturation signal depends on what the backend is:

| Backend | Cut when |
|---|---|
| Database | p95/p99 query latency rises above its healthy baseline |
| Cache | the hit rate falls, or misses push latency up. After a deploy or restart, let it warm up with `X-Aqueduct-Slow-Start: true`, which starts new queues at a low rate and ramps up. |
| GPU / inference | KV-cache or GPU memory nears full, or time-to-first-token rises. vLLM and Triton already report KV-cache use over ORCA, and Aquifer reads that with no backend change. |
| App server or third-party API | the error rate or timeouts rise, or the API's own rate-limit headers drop |

**Size the backlog budget in seconds.** `X-Aqueduct-Max-Backlog` is a job count, but choose it as time: `rate × seconds` Aquifer should be able to hold, where `rate` is the peak rate jobs for that backend arrive at the node across all tenants (intake fills it during an outage, not dispatch; the budget is per node), at least the longest outage the pilot should ride out. Teams get this wrong more than anything else, and it looks like Aquifer failing. A 60-second budget at 20 rps (1,200 jobs) turned a 5-minute database outage in the Hatchet benchmark into about 25,000 `429`s, and small tenants' recovery p99 was 107 s (3.8 s behind Envoy); a 600-second budget gave them no refusals and a 5.5 s p99. If the backend lowers its rate when it's in trouble, don't let the budget fall with it below the backlog Aquifer already holds.

**Priority tenants.** With account queues on (the default), every tenant (`user_id` and API key) has its own queue, and each queue follows the headers on its own responses, up to the configured ceiling. So the backend can favour priority customers today: cut them more gently (×0.8 instead of ×0.5), or advertise them a higher rate. It recognizes them by their API key or a header on the job. Slow priority tenants with a lower `X-Aqueduct-Rps` on successful responses, not with `429`s: every `429` halves that queue's rate, whatever the backend intended.
