#!/usr/bin/env bash
# Burst absorption: baseline traffic, then a 10x spike for 30s, then back to
# baseline. This is "retry storm," simulated. Success looks like the burst
# window absorbing into the queue (status codes stay healthy or shed
# cleanly with 429s) and the recovery window looking just like baseline.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_URL="${1:-https://aquifer-bench.fly.dev}"
BASELINE_RATE="${2:-10}"
BURST_RATE="${3:-100}"
STAMP=$(date +%s)

run_phase() {
  local label="$1" rate="$2" duration="$3"
  local n=$(( ${duration%s} * rate + 200 ))
  python3 "$DIR/gen_targets.py" "$n" "burst-${label}-${STAMP}" "$TARGET_URL" > "/tmp/vegeta_burst_${label}_targets.json"
  echo ""
  echo "## Phase: ${label} (rate=${rate}/s duration=${duration})"
  vegeta attack -format=json -targets="/tmp/vegeta_burst_${label}_targets.json" -rate="${rate}/1s" -duration="${duration}" \
    | vegeta report
}

echo "# Burst absorption test target=${TARGET_URL} baseline=${BASELINE_RATE}/s burst=${BURST_RATE}/s"
run_phase "baseline" "$BASELINE_RATE" "15s"
run_phase "burst" "$BURST_RATE" "30s"
run_phase "recovery" "$BASELINE_RATE" "15s"
