# How it works

1. Client submits a job through an adapter (MCP tool or HTTP endpoint) and moves on
2. Aquifer persists it to disk (Pebble by default), surviving crashes and re-dispatching on restart
3. A per-target-API worker dispatches at your configured RPS with jitter
4. On completion Aquifer POSTs your webhook with the response body and status, or skips it if you [stayed on the line](streaming.md) and got the result there
5. The target API can adjust the rate live via `X-Aqueduct-*` response headers

**Delivery semantics:** Aquifer provides at-least-once dispatch and webhook delivery, not exactly-once execution. If Aquifer crashes after a dispatch succeeds but before it records that completion, the recovered job dispatches to the target API again on restart, so it's not just the webhook that can repeat, the target API call itself can. Make both your target API endpoint and your webhook handler idempotent on `job_id` (or `idempotent_key`) anywhere duplicate execution isn't safe, the same contract Stripe and GitHub webhooks already ask of you.
