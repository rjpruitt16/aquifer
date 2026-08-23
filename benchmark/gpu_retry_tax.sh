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

VLLM_URL="${1:?usage: gpu_retry_tax.sh <vllm-url> <aquifer-url> [baseline-rate] [burst-rate]}"
AQUIFER_URL="${2:?usage: gpu_retry_tax.sh <vllm-url> <aquifer-url> [baseline-rate] [burst-rate]}"
BASELINE_RATE="${3:-2}"
BURST_RATE="${4:-20}"
STAMP=$(date +%s)

snapshot_metrics() {
  local label="$1"
  echo "--- vLLM /metrics snapshot: ${label} ---"
  curl -s "${VLLM_URL}/metrics" 2>/dev/null \
    | grep -E "^vllm:(num_requests_running|num_requests_waiting|kv_cache_usage_perc|request_success_total|generation_tokens_total)" \
    || echo "(metrics endpoint unreachable or these series not yet emitted)"
}

run_phase() {
  local mode="$1" label="$2" rate="$3" duration="$4"
  local n=$(( ${duration%s} * rate + 20 ))
  local targets="/tmp/vegeta_gputax_${mode}_${label}_targets.json"

  if [ "$mode" = "direct" ]; then
    python3 "$DIR/gen_vllm_targets.py" direct "$n" "gputax-${label}-${STAMP}" "$VLLM_URL" > "$targets"
  else
    python3 "$DIR/gen_vllm_targets.py" aquifer "$n" "gputax-${label}-${STAMP}" "$VLLM_URL" "$AQUIFER_URL" "$VLLM_URL" > "$targets"
  fi

  echo ""
  echo "## [$mode] Phase: ${label} (rate=${rate}/s duration=${duration})"
  vegeta attack -format=json -targets="$targets" -rate="${rate}/1s" -duration="${duration}" -timeout=30s \
    | vegeta report
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
  run_phase "$mode" "recovery" "$BASELINE_RATE" "15s"
  snapshot_metrics "after (${mode})"
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
