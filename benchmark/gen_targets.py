#!/usr/bin/env python3
"""Generate a vegeta JSON targets file of unique POST /jobs requests.

Each job needs a unique idempotent_key or Aquifer will correctly treat
repeats as duplicates rather than new load. The dummy upstream/webhook
target is postman-echo.com/post, a public echo endpoint that accepts POST
and returns 200 quickly — this matters because these are real jobs Aquifer
will actually dispatch and retry: a GET-only target (like this app's own
/health) causes every dispatch and webhook delivery to fail with 405 and
retry indefinitely, silently filling the queue with a permanent backlog.
"""
import base64
import json
import sys
import uuid


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    prefix = sys.argv[2] if len(sys.argv) > 2 else "bench"
    base_url = sys.argv[3] if len(sys.argv) > 3 else "https://aquifer-bench.fly.dev"

    for _ in range(n):
        body = {
            "user_id": f"{prefix}-user",
            "idempotent_key": f"{prefix}-{uuid.uuid4()}",
            "url": "https://postman-echo.com/post",
            "method": "POST",
            "webhook_url": "https://postman-echo.com/post",
        }
        target = {
            "method": "POST",
            "url": f"{base_url}/jobs",
            "header": {"Content-Type": ["application/json"]},
            "body": base64.b64encode(json.dumps(body).encode()).decode(),
        }
        print(json.dumps(target))


if __name__ == "__main__":
    main()
