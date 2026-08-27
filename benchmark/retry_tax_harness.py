#!/usr/bin/env python3
"""Open-loop ramp-to-saturation load harness for the GPU retry-tax experiment.

What the existing gpu_retry_tax.sh benchmark (vegeta-driven) doesn't measure:

  1. Tokens/sec. It captures latency, queue depth, and kv_cache_usage_perc,
     but never asks vLLM how much generation work actually happened.
  2. A real retry storm. Its "direct" run is 100% success at a fixed offered
     load with a long tail -- nobody's client actually gives up and retries,
     so no duplicate GPU work ever gets created to measure.

This harness fixes both. It's an open-loop load generator (fires requests on
a schedule independent of how fast responses come back -- vegeta already
does this; a naive "wait then send next" loop would not) that ramps offered
load upward over the run, with a client that behaves like a real caller:
times out after --client-timeout seconds and fires a retry, up to
--max-retries, instead of waiting patiently forever.

The number this produces that the old benchmark couldn't: useful tokens/sec
vs. raw tokens/sec. Raw comes straight from vLLM's own /metrics
(generation_tokens_total) -- ground truth for how much the GPU actually
generated, including work for requests whose caller already gave up and
retried. Useful is the sum of completion_tokens across responses a client
actually received. wasted = raw - useful. That's the actual "GPU waste"
number the retry-tax thesis claims exists; nothing before this measured it.

Two modes:
  --mode direct   Fires straight at vLLM's OpenAI-compatible endpoint, with
                   the retry-on-timeout client behavior described above.
  --mode aquifer  POSTs to Aquifer's /jobs (near-instant durable enqueue,
                   so the client-timeout/retry loop should barely ever fire
                   here -- that asymmetry IS the point, not a bug in the
                   comparison) then opens the job's SSE stream and waits for
                   the real `completed` event to get actual completion
                   timestamp + token count. No webhook/polling involved.

Output: one JSONL event log (every send/success/timeout/retry/abandon, one
line each) and one JSONL vLLM /metrics timeseries (scraped once a second
throughout the run). analyze_retry_tax.py turns these into the actual
report. Nothing here fabricates or estimates a number -- every value in the
event log came from an actual response or an actual timeout.

Requires: pip install aiohttp (see requirements.txt in this directory).
"""
import argparse
import asyncio
import json
import random
import re
import time
import uuid
from dataclasses import dataclass

import aiohttp

# vLLM /metrics series this experiment actually needs. Counters get summed
# across all label combinations (a model may report per-model-name series);
# gauges likewise -- in practice there's one series each for a single-model
# deployment, but summing is correct either way and doesn't silently drop
# data if that assumption is ever wrong.
WANTED_METRICS = (
    "vllm:generation_tokens_total",
    "vllm:prompt_tokens_total",
    "vllm:num_requests_running",
    "vllm:num_requests_waiting",
    "vllm:num_preemptions_total",
)
_METRIC_LINE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE.+\-]+)\s*$")


def parse_vllm_metrics(text: str) -> dict:
    """Sum every wanted metric's series values. Missing series -> 0.0."""
    totals = {name: 0.0 for name in WANTED_METRICS}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _METRIC_LINE_RE.match(line)
        if not m:
            continue
        name, _labels, value = m.groups()
        if name in totals:
            try:
                totals[name] += float(value)
            except ValueError:
                pass
    return totals


class EventLog:
    """Line-buffered JSONL writer -- flushed per line so a killed run still
    leaves a readable partial log instead of losing the whole thing to an
    unflushed buffer."""

    def __init__(self, path: str):
        self._f = open(path, "w")

    def write(self, **fields):
        fields.setdefault("ts", time.time())
        self._f.write(json.dumps(fields) + "\n")
        self._f.flush()

    def close(self):
        self._f.close()


def chat_body(request_id: str, model: str, max_tokens: int) -> dict:
    return {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": f"[{request_id}] Write a long, detailed essay about distributed systems.",
            }
        ],
        "max_tokens": max_tokens,
        # Forces full-length decoding instead of an early natural stop --
        # otherwise generation finishes fast regardless of max_tokens and
        # never holds GPU/KV-cache resources long enough to matter. Same
        # trick gen_vllm_targets.py already uses for the vegeta benchmark.
        "min_tokens": max_tokens,
    }


async def run_direct_request(
    session: aiohttp.ClientSession,
    request_id: str,
    vllm_url: str,
    model: str,
    max_tokens: int,
    client_timeout: float,
    max_retries: int,
    log: EventLog,
):
    body = chat_body(request_id, model, max_tokens)
    for attempt in range(1, max_retries + 2):
        sent_ts = time.time()
        log.write(request_id=request_id, attempt=attempt, event="sent", ts=sent_ts)
        try:
            timeout = aiohttp.ClientTimeout(total=client_timeout)
            async with session.post(f"{vllm_url}/v1/chat/completions", json=body, timeout=timeout) as resp:
                text = await resp.text()
                completed_ts = time.time()
                if resp.status == 200:
                    try:
                        tokens = json.loads(text).get("usage", {}).get("completion_tokens", 0)
                    except json.JSONDecodeError:
                        tokens = 0
                    log.write(
                        request_id=request_id,
                        attempt=attempt,
                        event="success",
                        ts=completed_ts,
                        latency=completed_ts - sent_ts,
                        completion_tokens=tokens,
                        status=resp.status,
                    )
                    return
                log.write(
                    request_id=request_id,
                    attempt=attempt,
                    event="http_error",
                    ts=completed_ts,
                    status=resp.status,
                    latency=completed_ts - sent_ts,
                )
        except asyncio.TimeoutError:
            giveup_ts = time.time()
            log.write(
                request_id=request_id,
                attempt=attempt,
                event="client_timeout",
                ts=giveup_ts,
                latency=giveup_ts - sent_ts,
            )
        except aiohttp.ClientError as e:
            log.write(request_id=request_id, attempt=attempt, event="client_error", error=str(e))
    log.write(request_id=request_id, attempt=None, event="abandoned")


async def read_sse_completion(resp: aiohttp.ClientResponse, sent_ts: float, request_id: str, job_id: str, log: EventLog):
    """Parse the SSE stream line-by-line for a completed/failed event and
    log it. Aquifer closes the connection right after writing one of these
    (server.go's streamJob returns), so reading to EOF is sufficient."""
    current_event = None
    async for raw_line in resp.content:
        line = raw_line.decode("utf-8", errors="replace").rstrip("\n")
        if line.startswith("event:"):
            current_event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            if current_event in ("completed", "failed"):
                payload = json.loads(line[len("data:"):].strip())
                completed_ts = time.time()
                tokens = 0
                body_str = payload.get("body", "")
                try:
                    tokens = json.loads(body_str).get("usage", {}).get("completion_tokens", 0)
                except (json.JSONDecodeError, AttributeError):
                    pass
                log.write(
                    request_id=request_id,
                    job_id=job_id,
                    event="success" if current_event == "completed" else "job_failed",
                    ts=completed_ts,
                    latency=completed_ts - sent_ts,
                    completion_tokens=tokens,
                    status=payload.get("response_status"),
                )
                return
            current_event = None


async def run_aquifer_request(
    session: aiohttp.ClientSession,
    request_id: str,
    aquifer_url: str,
    vllm_url: str,
    model: str,
    max_tokens: int,
    sse_timeout: float,
    log: EventLog,
):
    job_body = {
        "user_id": "retrytax-bench",
        "idempotent_key": request_id,
        "url": f"{vllm_url}/v1/chat/completions",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(chat_body(request_id, model, max_tokens)),
        # Required by JobRequest.Validate but unused -- completion is read
        # off the SSE stream below, not this webhook.
        "webhook_url": "https://postman-echo.com/post",
    }
    sent_ts = time.time()
    log.write(request_id=request_id, attempt=1, event="sent", ts=sent_ts)
    try:
        async with session.post(f"{aquifer_url}/jobs", json=job_body, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            job_id = data.get("job_id")
            enqueue_ts = time.time()
            if resp.status not in (200, 201) or not job_id:
                log.write(request_id=request_id, event="enqueue_error", status=resp.status, body=str(data))
                return
            log.write(
                request_id=request_id,
                job_id=job_id,
                event="enqueued",
                ts=enqueue_ts,
                latency=enqueue_ts - sent_ts,
            )
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.write(request_id=request_id, event="enqueue_error", error=str(e))
        return

    try:
        timeout = aiohttp.ClientTimeout(total=sse_timeout, sock_read=sse_timeout)
        async with session.get(f"{aquifer_url}/jobs/{job_id}/stream", timeout=timeout) as resp:
            await read_sse_completion(resp, sent_ts, request_id, job_id, log)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        log.write(request_id=request_id, job_id=job_id, event="sse_timeout")


async def scrape_vllm_metrics(vllm_url: str, out_path: str, stop_event: asyncio.Event, interval: float = 1.0):
    async with aiohttp.ClientSession() as session:
        with open(out_path, "w") as f:
            while not stop_event.is_set():
                ts = time.time()
                try:
                    async with session.get(f"{vllm_url}/metrics", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        text = await resp.text()
                    metrics = parse_vllm_metrics(text)
                    metrics["ts"] = ts
                    f.write(json.dumps(metrics) + "\n")
                except Exception as e:  # noqa: BLE001 -- a scrape failure shouldn't kill the run
                    f.write(json.dumps({"ts": ts, "error": str(e)}) + "\n")
                f.flush()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass


def rate_at(t: float, start_rate: float, end_rate: float, ramp_seconds: float) -> float:
    if ramp_seconds <= 0:
        return end_rate
    frac = min(max(t / ramp_seconds, 0.0), 1.0)
    return start_rate + (end_rate - start_rate) * frac


def generate_arrivals(start_rate: float, end_rate: float, ramp_seconds: float):
    """Thinning method for a non-homogeneous Poisson process: sample
    candidate arrivals at the run's peak rate, keep each with probability
    rate(t)/peak_rate. Standard technique for simulating a time-varying
    arrival rate without biasing the distribution toward either end of the
    ramp."""
    peak_rate = max(start_rate, end_rate, 0.01)
    t = 0.0
    while t < ramp_seconds:
        t += random.expovariate(peak_rate)
        if t >= ramp_seconds:
            return
        if random.random() <= rate_at(t, start_rate, end_rate, ramp_seconds) / peak_rate:
            yield t


async def run(args):
    log = EventLog(args.out_events)
    stop_scrape = asyncio.Event()
    scrape_task = asyncio.create_task(scrape_vllm_metrics(args.vllm_url, args.out_metrics, stop_scrape))

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        run_start = time.time()
        arrivals = list(generate_arrivals(args.start_rate, args.end_rate, args.ramp_seconds))
        print(
            f"# {args.mode} run: {len(arrivals)} requests scheduled over {args.ramp_seconds}s "
            f"(rate {args.start_rate}/s -> {args.end_rate}/s)"
        )
        for t in arrivals:
            now = time.time() - run_start
            if t > now:
                await asyncio.sleep(t - now)
            request_id = f"{args.prefix}-{uuid.uuid4().hex[:12]}"
            if args.mode == "direct":
                coro = run_direct_request(
                    session, request_id, args.vllm_url, args.model, args.max_tokens,
                    args.client_timeout, args.max_retries, log,
                )
            else:
                coro = run_aquifer_request(
                    session, request_id, args.aquifer_url, args.vllm_url, args.model,
                    args.max_tokens, args.sse_timeout, log,
                )
            tasks.append(asyncio.create_task(coro))

        print(f"# all {len(tasks)} requests fired, waiting for in-flight to finish...")
        await asyncio.gather(*tasks, return_exceptions=True)

    stop_scrape.set()
    await scrape_task
    log.close()
    print(f"# done. events -> {args.out_events}  vllm metrics -> {args.out_metrics}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["direct", "aquifer"], required=True)
    p.add_argument("--vllm-url", required=True)
    p.add_argument("--aquifer-url", help="required for --mode aquifer")
    p.add_argument("--model", default="Qwen/Qwen2.5-1.5B-Instruct")
    p.add_argument("--max-tokens", type=int, default=300)
    p.add_argument("--start-rate", type=float, default=5.0)
    p.add_argument("--end-rate", type=float, default=100.0)
    p.add_argument("--ramp-seconds", type=float, default=300.0)
    p.add_argument("--client-timeout", type=float, default=5.0, help="direct mode: seconds before a caller gives up and retries")
    p.add_argument("--max-retries", type=int, default=3, help="direct mode: retries after the first attempt before abandoning")
    p.add_argument("--sse-timeout", type=float, default=60.0, help="aquifer mode: seconds to wait on the SSE stream for completion")
    p.add_argument("--prefix", default=f"rt{int(time.time())}")
    p.add_argument("--out-events", default=None)
    p.add_argument("--out-metrics", default=None)
    args = p.parse_args()

    if args.mode == "aquifer" and not args.aquifer_url:
        p.error("--mode aquifer requires --aquifer-url")
    if args.out_events is None:
        args.out_events = f"/tmp/retry_tax_events_{args.mode}_{args.prefix}.jsonl"
    if args.out_metrics is None:
        args.out_metrics = f"/tmp/retry_tax_vllm_metrics_{args.mode}_{args.prefix}.jsonl"

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
