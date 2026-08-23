#!/usr/bin/env bash
# The GPU retry tax benchmark: does pacing through Aquifer (via the ORCA
# fallback signal from vLLM's own kv_cache_usage_perc) keep a GPU serving
# cleanly under a burst that would otherwise overload it directly?
#
# Two runs against the same vLLM instance, same burst shape:
#   1. direct  -- vegeta fires straight at vLLM, unpaced. This is the naive
#      baseline: whatever vLLM's own admission/queueing does under raw load.
#   2. aquifer -- vegeta fires the same burst at Aquifer's POST /jobs
#      instead. Aquifer durably queues every job immediately (ingest should
#      stay ~100% regardless of burst size -- that's the "absorb the burst"
#      claim) and paces actual dispatch to vLLM using the ORCA
#      endpoint-load-metrics fallback built for this.
#
# Compare: vLLM's own success rate/latency in each run, and vLLM's
# Prometheus counters (request counts, KV-cache usage) snapshotted before
# and after each phase -- a real, measured before/after, not illustrative.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

VLLM_URL="${1:?usage: gpu_retry_tax.sh <vllm-url> <aquifer-url> [baseline-rate] [burst-rate] [max-tokens]}"
AQUIFER_URL="${2:?usage: gpu_retry_tax.sh <vllm-url> <aquifer-url> [baseline-rate] [burst-rate] [max-tokens]}"
BASELINE_RATE="${3:-2}"
BURST_RATE="${4:-20}"
MAX_TOKENS="${5:-32}"
STAMP=$(date +%s)

snapshot_metrics() {
  local label="$1"
  echo "--- vLLM /metrics snapshot: ${label} ---"
  curl -s "${VLLM_URL}/metrics" 2>/dev/null \
    | grep -E "^vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc|request_success_total|generation_tokens_total)" \
    || echo "(metrics endpoint unreachable or these series not yet emitted)"
}

# Peaks are what matter for "did this phase actually stress the GPU" --
# before/after snapshots bracket the whole scenario and miss a spike that
# drains during the recovery phase, so poll continuously during the phase
# itself and report the max seen.
poll_peak_metrics() {
  local outfile="$1"
  : > "$outfile"
  while true; do
    curl -s -m 2 "${VLLM_URL}/metrics" 2>/dev/null \
      | grep -E "^vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc)\{" >> "$outfile" || true
    sleep 1
  done
}

report_peak() {
  local outfile="$1"
  local peak_running peak_waiting peak_kv
  peak_running=$(grep "num_requests_running" "$outfile" 2>/dev/null | awk '{print $NF}' | sort -g | tail -1)
  peak_waiting=$(grep "num_requests_waiting{" "$outfile" 2>/dev/null | awk '{print $NF}' | sort -g | tail -1)
  peak_kv=$(grep "kv_cache_usage_perc" "$outfile" 2>/dev/null | awk '{print $NF}' | sort -g | tail -1)
  echo "peak num_requests_running=${peak_running:-n/a} peak num_requests_waiting=${peak_waiting:-n/a} peak kv_cache_usage_perc=${peak_kv:-n/a}"
}

run_phase() {
  local mode="$1" label="$2" rate="$3" duration="$4"
  local n=$(( ${duration%s} * rate + 20 ))
  local targets="/tmp/vegeta_gputax_${mode}_${label}_targets.json"

  if [ "$mode" = "direct" ]; then
    python3 "$DIR/gen_vllm_targets.py" direct "$n" "gputax-${label}-${STAMP}" "$VLLM_URL" "" "" "$MAX_TOKENS" > "$targets"
  else
    python3 "$DIR/gen_vllm_targets.py" aquifer "$n" "gputax-${label}-${STAMP}" "$VLLM_URL" "$AQUIFER_URL" "$VLLM_URL" "$MAX_TOKENS" > "$targets"
  fi

  local peakfile="/tmp/vllm_peak_${mode}_${label}.log"
  poll_peak_metrics "$peakfile" &
  local poll_pid=$!

  echo ""
  echo "## [$mode] Phase: ${label} (rate=${rate}/s duration=${duration})"
  vegeta attack -format=json -targets="$targets" -rate="${rate}/1s" -duration="${duration}" -timeout=30s \
    | vegeta report

  kill "$poll_pid" 2>/dev/null || true
  wait "$poll_pid" 2>/dev/null || true
  echo "$(report_peak "$peakfile")"
}

run_scenario() {
  local mode="$1"
  echo ""
  echo "=========================================="
  echo "# Scenario: ${mode}"
  echo "=========================================="
  snapshot_metrics "before (${mode})"
  run_phase "$mode" "baseline" "$BASELINE_RATE" "15s"
  run_phase "$mode" "burst" "$BURST_RATE" "30s"
  snapshot_metrics "immediately after burst (${mode})"
  run_phase "$mode" "recovery" "$BASELINE_RATE" "15s"
  snapshot_metrics "after full scenario (${mode})"
}

echo "# GPU retry tax benchmark"
echo "# vllm=${VLLM_URL} aquifer=${AQUIFER_URL} baseline=${BASELINE_RATE}/s burst=${BURST_RATE}/s"

run_scenario "direct"
run_scenario "aquifer"

echo ""
echo "=========================================="
echo "Compare the two 'burst' phase reports above: direct hits vLLM's"
echo "--max-num-seqs 8 ceiling immediately (this instance was deliberately"
echo "constrained so a modest burst is enough to saturate it); aquifer"
echo "should show ~100% ingest success with dispatch to vLLM paced down"
echo "once kv_cache_usage_perc crosses the ORCA thresholds in orca.go."
