#!/usr/bin/env bash
# Capacity and drain-time by machine size. Answers two questions an operator
# actually needs before picking a box: (1) how much sustained ingest rate can
# this machine size take before it starts shedding, and (2) if it takes a
# burst bigger than that, how long until it's fully caught up again. Uses
# Dockerfile.bench / fly.capacity.toml, which raises per-domain dispatch
# pacing to 50 rps / 20 concurrent — the out-of-the-box 2 rps default would
# otherwise bottleneck drain time identically regardless of machine size.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/.." && pwd)"

TARGET_URL="${1:-https://aquifer-bench.fly.dev}"
FLY_APP="${2:-aquifer-bench}"
SIZES="${3:-256 512 1024}"
BURST_N="${4:-500}"

RAMP_RATES="25 50 100 200 400"
RAMP_DURATION="15s"

clean_deploy() {
  local mem_mb="$1" admission_mb="$2"
  echo "## Clean deploy: vm=${mem_mb}mb admission_limit=${admission_mb}mb"
  (
    cd "$ROOT"
    MACHINE_ID=$(fly machine list --app "$FLY_APP" --json 2>/dev/null | python3 -c "import json,sys
d=json.load(sys.stdin)
print(d[0]['id'] if d else '')")
    if [ -n "$MACHINE_ID" ]; then
      fly machine destroy "$MACHINE_ID" --app "$FLY_APP" --force || true
    fi
    VOLUME_ID=$(fly volumes list --app "$FLY_APP" --json 2>/dev/null | python3 -c "import json,sys
d=json.load(sys.stdin)
print(d[0]['id'] if d else '')")
    if [ -n "$VOLUME_ID" ]; then
      fly volumes destroy "$VOLUME_ID" --app "$FLY_APP" --yes || true
    fi
    fly volumes create aquifer_data --app "$FLY_APP" --region iad --size 1 --yes
    fly deploy --app "$FLY_APP" --config fly.capacity.toml \
      --vm-memory "${mem_mb}" -e "AQUIFER_MEMORY_LIMIT_MB=${admission_mb}"
  )
  for i in $(seq 1 30); do
    if curl -sf --max-time 3 "${TARGET_URL}/health" > /dev/null; then
      echo "healthy after ~${i}s"
      return
    fi
    sleep 1
  done
  echo "WARNING: never became healthy within 30s"
}

mem_of() {
  curl -s --max-time 3 "${TARGET_URL}/health" \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('admission',{}).get('memory_mb','?'))" 2>/dev/null || echo "?"
}

ramp() {
  local size_mb="$1"
  echo ""
  echo "### Ingest ramp @ ${size_mb}mb"
  for rate in $RAMP_RATES; do
    local n=$(( ${RAMP_DURATION%s} * rate + 100 ))
    python3 "$DIR/gen_targets.py" "$n" "capacity-${size_mb}-${rate}-$(date +%s)" "$TARGET_URL" > "/tmp/vegeta_capacity_${size_mb}_${rate}.json"
    local before_mem
    before_mem=$(mem_of)
    vegeta attack -format=json -targets="/tmp/vegeta_capacity_${size_mb}_${rate}.json" -rate="${rate}/1s" -duration="${RAMP_DURATION}" \
      > "/tmp/vegeta_capacity_${size_mb}_${rate}_results.bin"
    local after_mem
    after_mem=$(mem_of)
    local success
    success=$(vegeta report < "/tmp/vegeta_capacity_${size_mb}_${rate}_results.bin" | grep "Success" | awk '{print $3}')
    local codes
    codes=$(vegeta report -type=json < "/tmp/vegeta_capacity_${size_mb}_${rate}_results.bin" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status_codes',{}))")
    echo "  rate=${rate}/s success=${success} memory_mb: ${before_mem} -> ${after_mem} codes=${codes}"
    if [[ "$success" != "100.00%" ]]; then
      echo "  (non-100% success at rate=${rate}/s — stopping ramp for this size; check codes= above for 429 vs transient)"
      break
    fi
  done
}

drain_test() {
  local size_mb="$1"
  echo ""
  echo "### Burst + drain @ ${size_mb}mb (N=${BURST_N})"

  local stamp
  stamp=$(date +%s)
  local id_file="/tmp/drain_${size_mb}_ids.txt"
  : > "$id_file"

  echo "  firing ${BURST_N} jobs as fast as possible..."
  for i in $(seq 1 "$BURST_N"); do
    (
      body=$(python3 -c "
import json
print(json.dumps({
    'user_id': 'drain-${size_mb}',
    'idempotent_key': 'drain-${size_mb}-${stamp}-${i}',
    'url': 'https://postman-echo.com/post',
    'method': 'POST',
    'webhook_url': 'https://postman-echo.com/post',
}))
")
      curl -s -X POST "${TARGET_URL}/jobs" -H "Content-Type: application/json" -d "$body" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])" >> "$id_file"
    ) &
  done
  wait

  local total
  total=$(wc -l < "$id_file" | tr -d ' ')
  echo "  ${total} job ids captured, polling until fully drained..."

  local start_ts
  start_ts=$(date +%s)
  local deadline=$(( start_ts + 300 ))
  local completed=0

  while [ "$(date +%s)" -lt "$deadline" ]; do
    completed=0
    while IFS= read -r id; do
      [ -z "$id" ] && continue
      status=$(curl -s --max-time 3 "${TARGET_URL}/jobs/${id}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "?")
      if [ "$status" = "completed" ] || [ "$status" = "failed" ]; then
        completed=$((completed+1))
      fi
    done < "/tmp/drain_${size_mb}_ids.txt"
    if [ "$completed" -ge "$total" ]; then
      break
    fi
    sleep 3
  done

  local end_ts
  end_ts=$(date +%s)
  local elapsed=$(( end_ts - start_ts ))
  echo "  drained ${completed}/${total} jobs in ${elapsed}s"
}

echo "# Capacity by machine size: sizes=[${SIZES}] app=${FLY_APP} burst_n=${BURST_N}"

for size in $SIZES; do
  admission_mb=$(( size * 80 / 100 ))
  echo ""
  echo "=================================================="
  echo "SIZE: ${size}mb (admission ceiling: ${admission_mb}mb)"
  echo "=================================================="
  clean_deploy "$size" "$admission_mb"
  ramp "$size"
  # Ramp leaves a backlog in the shared per-domain queue (postman-echo.com);
  # redeploy clean again so drain time measures N fresh jobs on an otherwise
  # idle instance, not N jobs queued behind whatever the ramp didn't finish.
  clean_deploy "$size" "$admission_mb"
  drain_test "$size"
done
