#!/usr/bin/env bash
# Sustained ingest throughput: how many POST /jobs/sec Aquifer accepts and
# durably persists before latency degrades, with admission limits in effect
# but not tripped (this is a baseline capacity number, not a stress test).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_URL="${1:-https://aquifer-bench.fly.dev}"
RATE="${2:-50}"
DURATION="${3:-30s}"

DURATION_SECS="${DURATION%s}"
N=$(( DURATION_SECS * RATE + 500 ))

echo "# Sustained throughput: rate=${RATE}/s duration=${DURATION} target=${TARGET_URL}"
python3 "$DIR/gen_targets.py" "$N" "throughput-$(date +%s)" "$TARGET_URL" > /tmp/vegeta_throughput_targets.json

vegeta attack -format=json -targets=/tmp/vegeta_throughput_targets.json -rate="${RATE}/1s" -duration="${DURATION}" \
  | tee /tmp/vegeta_throughput_results.bin \
  | vegeta report
