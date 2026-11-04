# Streaming

After submitting a job you choose how to get the result. Stay on the line with `GET /jobs/:id/stream` (or call `POST /proxy`, which streams automatically when it has to queue), and watch your job move through the line in real time. Or hang up whenever you like: the result still arrives at your `webhook_url`, and it's stored for `GET /jobs/:id` too.

```
event: queued
data: {"job_id":"abc123","status":"queued"}

event: position
data: {"job_id":"abc123","position":3}

event: dispatching
data: {"job_id":"abc123","attempt":1}

event: completed
data: {"job_id":"abc123","response_status":200,"body":"..."}
```

The webhook is only a fallback. If you stayed on the line and received the final event (or a `/proxy` direct response), Aquifer doesn't send it, so you never pay for the same result twice. If you hung up first, or never streamed, the webhook goes out as usual. A client that disappears without closing its connection (a dropped network, a sleeping laptop) can't be told apart from one that's still listening, so it may miss both; that's what the stored result on `GET /jobs/:id` is for.
