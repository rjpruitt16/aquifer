# API overview

```json
POST /jobs
{
  "user_id":        "user-123",
  "idempotent_key": "invoice-42-notify",
  "url":            "https://api.openai.com/v1/chat/completions",
  "method":         "POST",
  "headers":        { "Authorization": "Bearer sk-..." },
  "body":           "{\"model\":\"gpt-4o\",\"messages\":[...]}",
  "webhook_url":    "https://yourapp.com/webhooks/aquifer"
}
```

**Do not expose Aquifer directly to untrusted callers.** `url` is dispatched as a real HTTP request. If an arbitrary or untrusted party can set it, Aquifer becomes an open relay/SSRF vector, using Aquifer's own network position and identity to reach anything the machine can reach. The intended caller is **your own trusted backend or gateway code** dispatching to a destination it already knows about, not an agent, end user, or any other party choosing the destination itself.

**[API.md](../../API.md)** has the full reference: `GET /jobs/:id`, the SSE stream, `POST /proxy` (capacity-aware reverse-proxy mode, see [Use cases](use-cases.md)), health/readiness and graceful shutdown, webhook payload shapes, and the autoscaling headers.
