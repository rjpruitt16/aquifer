#!/usr/bin/env bash
# The headline scenario: push sustained load past the configured memory
# ceiling and confirm Aquifer sheds load with clean 429s instead of falling
# over. Polls /health concurrently with the attack so the memory timeline
# can be correlated against when rejections start.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_URL="${1:-https://aquifer-bench.fly.dev}"
RATE="${2:-150}"
DURATION="${3:-45s}"
STAMP=$(date +%s)

DURATION_SECS="${DURATION%s}"
N=$(( DURATION_SECS * RATE + 1000 ))

HEALTH_LOG="/tmp/admission_health_timeline_${STAMP}.log"
: > "$HEALTH_LOG"

# Background health poller — runs for the attack duration plus a small tail
# so we can see recovery after the attack stops.
poll_health() {
  local end=$(( $(date +%s) + DURATION_SECS + 10 ))
  while [ "$(date +%s)" -lt "$end" ]; do
    ts=$(date +%s)
    resp=$(curl -s --max-time 3 "${TARGET_URL}/health" || echo '{}')
    mem=$(echo "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('admission',{}).get('memory_mb','?'))" 2>/dev/null || echo "?")
    echo "${ts} memory_mb=${mem}" >> "$HEALTH_LOG"
    sleep 2
  done
}

echo "# Admission degradation test: rate=${RATE}/s duration=${DURATION} target=${TARGET_URL}"
poll_health &
POLL_PID=$!

python3 "$DIR/gen_targets.py" "$N" "admission-${STAMP}" "$TARGET_URL" > "/tmp/vegeta_admission_targets.json"

vegeta attack -format=json -targets=/tmp/vegeta_admission_targets.json -rate="${RATE}/1s" -duration="${DURATION}" \
  > "/tmp/vegeta_admission_results_${STAMP}.bin"

wait "$POLL_PID" 2>/dev/null || true

echo ""
echo "## Vegeta report"
vegeta report < "/tmp/vegeta_admission_results_${STAMP}.bin"

echo ""
echo "## Status code breakdown"
vegeta report -type=json < "/tmp/vegeta_admission_results_${STAMP}.bin" \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
codes = d.get('status_codes', {})
for code, count in sorted(codes.items()):
    print(f'  {code}: {count}')
"

echo ""
echo "## Memory timeline during test (sampled every 2s)"
cat "$HEALTH_LOG"

echo ""
echo "## First rejection (429) timestamp relative to attack start"
python3 -c "
import json
with open('/tmp/vegeta_admission_results_${STAMP}.bin', 'rb') as f:
    pass
" 2>/dev/null || true
vegeta encode < "/tmp/vegeta_admission_results_${STAMP}.bin" \
  | python3 -c "
import json, sys
first_429_ts = None
first_ts = None
total = 0
rejected = 0
for line in sys.stdin:
    r = json.loads(line)
    total += 1
    if first_ts is None:
        first_ts = r['timestamp']
    if r['code'] == 429:
        rejected += 1
        if first_429_ts is None:
            first_429_ts = r['timestamp']
print(f'  total requests: {total}')
print(f'  rejected (429): {rejected}')
if first_429_ts:
    print(f'  first 429 at: {first_429_ts}')
else:
    print('  no rejections observed (limit not reached at this rate/duration)')
"
