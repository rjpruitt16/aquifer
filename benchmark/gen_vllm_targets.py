#!/usr/bin/env python3
"""Generate vegeta JSON targets for the GPU retry tax benchmark.

Two modes:
  - "direct": fires straight at vLLM's OpenAI-compatible chat completions
    endpoint, unpaced -- this is the naive baseline.
  - "aquifer": fires at Aquifer's POST /jobs, wrapping the same vLLM request
    as the job's url/body/method, so Aquifer durably queues it and paces
    dispatch to vLLM instead of the raw burst hitting it directly.

Both use unique idempotent_key/request content so nothing gets deduped or
cached in a way that would misrepresent load.
"""
import base64
import json
import sys
import uuid


def vllm_body(prefix: str, i: int, max_tokens: int) -> dict:
    return {
        "model": "Qwen/Qwen2.5-1.5B-Instruct",
        "messages": [
            {
                "role": "user",
                "content": f"[{prefix}-{i}] Write a long, detailed essay about distributed systems.",
            }
        ],
        "max_tokens": max_tokens,
        # Forces the model to keep decoding for the full length instead of
        # stopping early on a natural EOS -- without this, the model answers
        # briefly regardless of max_tokens, generation finishes fast, and KV
        # cache never actually gets held long enough to build up usage.
        "min_tokens": max_tokens,
    }


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "direct"
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    prefix = sys.argv[3] if len(sys.argv) > 3 else "bench"
    base_url = sys.argv[4] if len(sys.argv) > 4 else "http://localhost:8000"
    # Only used in aquifer mode: where Aquifer itself is listening, and
    # where it should dispatch to -- these differ from base_url when
    # Aquifer isn't co-located with vLLM.
    aquifer_url = sys.argv[5] if len(sys.argv) > 5 else "http://localhost:8080"
    vllm_url = sys.argv[6] if len(sys.argv) > 6 else base_url
    max_tokens = int(sys.argv[7]) if len(sys.argv) > 7 else 32

    for i in range(n):
        if mode == "direct":
            target = {
                "method": "POST",
                "url": f"{base_url}/v1/chat/completions",
                "header": {"Content-Type": ["application/json"]},
                "body": base64.b64encode(json.dumps(vllm_body(prefix, i, max_tokens)).encode()).decode(),
            }
        elif mode == "aquifer":
            job_body = {
                "user_id": f"{prefix}-user",
                "idempotent_key": f"{prefix}-{uuid.uuid4()}",
                "url": f"{vllm_url}/v1/chat/completions",
                "method": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(vllm_body(prefix, i, max_tokens)),
                # postman-echo just needs to accept the POST -- the webhook
                # delivery itself isn't what this benchmark is measuring.
                "webhook_url": "https://postman-echo.com/post",
            }
            target = {
                "method": "POST",
                "url": f"{aquifer_url}/jobs",
                "header": {"Content-Type": ["application/json"]},
                "body": base64.b64encode(json.dumps(job_body).encode()).decode(),
            }
        else:
            print(f"unknown mode {mode!r}, expected 'direct' or 'aquifer'", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(target))


if __name__ == "__main__":
    main()
