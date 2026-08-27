#!/usr/bin/env python3
"""Turn retry_tax_harness.py's raw JSONL logs into the actual report.

Every number below is computed from the event log and the vLLM /metrics
timeseries a harness run produced -- nothing here is estimated or
fabricated, matching this project's existing benchmark convention
(generate_report.py embeds only real captured output).

The key numbers this produces that the old vegeta-based benchmark couldn't:

  useful tokens/sec  -- sum of completion_tokens across responses a client
                        actually received (from "success" events), over the
                        run's wall-clock duration.
  raw tokens/sec     -- delta of vLLM's own generation_tokens_total counter
                        over the same window. Ground truth for how much the
                        GPU actually generated, independent of who received
                        it.
  wasted tokens/sec  -- raw - useful. Tokens generated for a request whose
                        caller had already given up and retried (direct
                        mode) or that never made it back to a listener at
                        all. This is the number the "GPU waste" thesis
                        actually rests on.

Usage: analyze_retry_tax.py --events <events.jsonl> --metrics <metrics.jsonl>
"""
import argparse
import json


def load_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * p
    f, c = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def analyze(events_path: str, metrics_path: str):
    events = load_jsonl(events_path)
    metrics = load_jsonl(metrics_path)
    metrics = [m for m in metrics if "error" not in m]

    if not events:
        print("no events logged -- nothing to analyze")
        return

    run_start = min(e["ts"] for e in events)
    run_end = max(e["ts"] for e in events)
    duration = max(run_end - run_start, 1e-9)

    logical_requests = {e["request_id"] for e in events}
    successes = [e for e in events if e["event"] == "success"]
    abandoned = [e for e in events if e["event"] == "abandoned"]
    timeouts = [e for e in events if e["event"] == "client_timeout"]
    enqueue_errors = [e for e in events if e["event"] in ("enqueue_error", "sse_timeout")]

    total_attempts = sum(1 for e in events if e["event"] == "sent")
    retried_requests = sum(1 for e in events if e["event"] == "sent" and e.get("attempt", 1) and e["attempt"] > 1)

    latencies = sorted(e["latency"] for e in successes if "latency" in e)
    useful_tokens = sum(e.get("completion_tokens", 0) or 0 for e in successes)
    useful_tps = useful_tokens / duration

    raw_tokens = None
    raw_tps = None
    if len(metrics) >= 2:
        metrics_sorted = sorted(metrics, key=lambda m: m["ts"])
        first, last = metrics_sorted[0], metrics_sorted[-1]
        span = max(last["ts"] - first["ts"], 1e-9)
        raw_tokens = last.get("vllm:generation_tokens_total", 0) - first.get("vllm:generation_tokens_total", 0)
        raw_tps = raw_tokens / span

    wasted_tps = None if raw_tps is None else max(raw_tps - useful_tps, 0.0)
    wasted_pct = None if not raw_tps else (wasted_tps / raw_tps * 100 if raw_tps > 0 else 0.0)

    peak_running = max((m.get("vllm:num_requests_running", 0) for m in metrics), default=None)
    peak_waiting = max((m.get("vllm:num_requests_waiting", 0) for m in metrics), default=None)
    peak_preemptions = max((m.get("vllm:num_preemptions_total", 0) for m in metrics), default=None)

    print(f"# Retry-tax analysis: {events_path}")
    print(f"duration: {duration:.1f}s")
    print(f"logical requests: {len(logical_requests)}")
    print(f"  succeeded: {len(successes)} ({len(successes) / max(len(logical_requests), 1) * 100:.1f}%)")
    print(f"  abandoned (exhausted retries): {len(abandoned)}")
    print(f"  enqueue/stream errors: {len(enqueue_errors)}")
    print(f"total attempts sent: {total_attempts}  (retried at least once: {retried_requests})")
    print(f"client-side timeouts (attempts that never got a response in time): {len(timeouts)}")
    print()
    if latencies:
        print("latency (successful requests only):")
        print(f"  mean: {sum(latencies) / len(latencies):.3f}s")
        print(f"  p50:  {percentile(latencies, 0.50):.3f}s")
        print(f"  p95:  {percentile(latencies, 0.95):.3f}s")
        print(f"  p99:  {percentile(latencies, 0.99):.3f}s")
    print()
    print("tokens/sec:")
    print(f"  useful (delivered to a listener): {useful_tps:.1f} tok/s  ({useful_tokens} tokens total)")
    if raw_tps is not None:
        print(f"  raw (vLLM generation_tokens_total delta): {raw_tps:.1f} tok/s  ({raw_tokens:.0f} tokens total)")
        print(f"  wasted (raw - useful): {wasted_tps:.1f} tok/s  ({wasted_pct:.1f}% of raw generation)")
    else:
        print("  raw: unavailable (need >=2 metrics samples)")
    print()
    print("vLLM saturation signals (peak over the run):")
    print(f"  num_requests_running: {peak_running}")
    print(f"  num_requests_waiting: {peak_waiting}")
    print(f"  num_preemptions_total: {peak_preemptions}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", required=True)
    p.add_argument("--metrics", required=True)
    args = p.parse_args()
    analyze(args.events, args.metrics)


if __name__ == "__main__":
    main()
