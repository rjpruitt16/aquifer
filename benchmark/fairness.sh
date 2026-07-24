#!/usr/bin/env bash
# Multi-tenant fairness: one "noisy" tenant floods the same upstream while a
# "quiet" tenant sends light steady traffic. Aquifer isolates dispatch pacing
# per (user_id, upstream) pair via AccountQueue, so the quiet tenant's jobs
# should complete on their own schedule regardless of the flood.
set -euo pipefail
STAMP=$(date +%s)
TARGET_URL="${1:-https://aquifer-bench.fly.dev}"
NOISY_COUNT="${2:-100}"

enqueue() {
  local user="$1" key="$2"
  body=$(python3 -c "
import json
print(json.dumps({
    'user_id': '${user}',
    'idempotent_key': '${key}',
    'url': 'https://postman-echo.com/post',
    'method': 'POST',
    'webhook_url': 'https://postman-echo.com/post',
}))
")
  curl -s -X POST "${TARGET_URL}/jobs" -H "Content-Type: application/json" \
    -H "X-Aquifer-Account-Queue: enabled" -d "$body" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])"
}

echo "# Fairness test: noisy-tenant floods ${NOISY_COUNT} jobs, quiet-tenant sends 5 steady jobs"

echo "## Flooding as noisy-tenant..."
for i in $(seq 1 "$NOISY_COUNT"); do
  enqueue "fairness-noisy-tenant" "fairness-noisy-${STAMP}-${i}" > /dev/null &
done

echo "## Submitting quiet-tenant jobs, timing each one's completion..."
quiet_ids=()
quiet_submit_times=()
for i in $(seq 1 5); do
  sleep 1
  submit_ts=$(date +%s)
  id=$(enqueue "fairness-quiet-tenant" "fairness-quiet-${STAMP}-${i}")
  quiet_ids+=("$id")
  quiet_submit_times+=("$submit_ts")
done

wait

echo "## Polling quiet-tenant job completion times..."
for idx in "${!quiet_ids[@]}"; do
  id="${quiet_ids[$idx]}"
  submit_ts="${quiet_submit_times[$idx]}"
  deadline=$(( $(date +%s) + 30 ))
  status="queued"
  while [ "$(date +%s)" -lt "$deadline" ]; do
    resp=$(curl -s --max-time 3 "${TARGET_URL}/jobs/${id}" || echo '{}')
    status=$(echo "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status','?'))" 2>/dev/null || echo "?")
    if [ "$status" = "completed" ] || [ "$status" = "failed" ]; then
      break
    fi
    sleep 1
  done
  done_ts=$(date +%s)
  elapsed=$(( done_ts - submit_ts ))
  echo "  quiet job ${idx}: status=${status} elapsed=${elapsed}s"
done

echo ""
echo "If quiet-tenant jobs complete within a few seconds each (their own ~2 RPS pace)"
echo "despite ${NOISY_COUNT} concurrent jobs from noisy-tenant, per-tenant isolation is working."
