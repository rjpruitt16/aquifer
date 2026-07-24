#!/usr/bin/env bash
# Durability isn't a claim until it's demonstrated: enqueue N jobs, SIGKILL
# the machine mid-drain, restart it, and confirm every job still dispatches.
# Default per-upstream pacing is 2 RPS / concurrency 1 (no CONFIG_PATH set),
# so N=30 jobs takes ~15s to drain — plenty of window to kill mid-flight.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_URL="${1:-https://aquifer-bench.fly.dev}"
FLY_APP="${2:-aquifer-bench}"
N="${3:-30}"
STAMP=$(date +%s)

echo "# Crash recovery test: N=${N} jobs, app=${FLY_APP}, target=${TARGET_URL}"

echo "## Enqueueing ${N} jobs..."
job_ids=()
for i in $(seq 1 "$N"); do
  body=$(python3 -c "
import json
print(json.dumps({
    'user_id': 'crash-recovery-user',
    'idempotent_key': 'crash-recovery-${STAMP}-${i}',
    'url': 'https://postman-echo.com/post',
    'method': 'POST',
    'webhook_url': 'https://postman-echo.com/post',
}))
")
  resp=$(curl -s -X POST "${TARGET_URL}/jobs" -H "Content-Type: application/json" -d "$body")
  job_id=$(echo "$resp" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])")
  job_ids+=("$job_id")
done
echo "Enqueued ${#job_ids[@]} jobs."

echo "## Waiting 3s to let dispatch begin..."
sleep 3

echo "## Killing machine (SIGKILL)..."
MACHINE_ID=$(fly machine list --app "$FLY_APP" --json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['id'])")
echo "Machine ID: ${MACHINE_ID}"
fly machine kill "$MACHINE_ID" --app "$FLY_APP" 2>&1 || echo "(kill command returned non-zero, continuing — machine may already be transitioning)"

echo "## Restarting machine..."
sleep 3
fly machine start "$MACHINE_ID" --app "$FLY_APP" 2>&1 || echo "(start command returned non-zero — may auto-start on next request instead)"

echo "## Waiting for machine to come back up..."
for i in $(seq 1 30); do
  if curl -s --max-time 3 "${TARGET_URL}/health" > /dev/null 2>&1; then
    echo "Machine responding again after ~${i}s."
    break
  fi
  sleep 1
done

echo "## Polling job statuses for up to 60s..."
completed=0
failed=0
still_queued=0
not_found=0
deadline=$(( $(date +%s) + 60 ))
declare -A final_status

while [ "$(date +%s)" -lt "$deadline" ]; do
  all_terminal=true
  for id in "${job_ids[@]}"; do
    if [ -n "${final_status[$id]:-}" ]; then
      continue
    fi
    resp=$(curl -s --max-time 3 "${TARGET_URL}/jobs/${id}" || echo '{}')
    status=$(echo "$resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status','not_found'))" 2>/dev/null || echo "not_found")
    if [ "$status" = "completed" ] || [ "$status" = "failed" ] || [ "$status" = "not_found" ]; then
      final_status[$id]="$status"
    else
      all_terminal=false
    fi
  done
  if [ "$all_terminal" = true ]; then
    break
  fi
  sleep 2
done

for id in "${job_ids[@]}"; do
  status="${final_status[$id]:-still_queued}"
  case "$status" in
    completed) completed=$((completed+1)) ;;
    failed) failed=$((failed+1)) ;;
    not_found) not_found=$((not_found+1)) ;;
    *) still_queued=$((still_queued+1)) ;;
  esac
done

echo ""
echo "## Results"
echo "  jobs enqueued:        ${#job_ids[@]}"
echo "  completed:            ${completed}"
echo "  failed (but tracked): ${failed}"
echo "  still queued after ${deadline_secs:-60}s:  ${still_queued}"
echo "  not found (lost):     ${not_found}"
echo ""
if [ "$not_found" -eq 0 ] && [ $(( completed + failed )) -eq "${#job_ids[@]}" ]; then
  echo "PASS: all ${#job_ids[@]} jobs survived the crash and drained to a real terminal state (completed or failed) — not just 'still present'."
elif [ "$not_found" -gt 0 ]; then
  echo "FAIL: ${not_found} job(s) were lost across the crash (row no longer exists)."
else
  echo "PARTIAL: ${still_queued} job(s) survived the crash and are still present, but hadn't drained to completed/failed within the poll window — no jobs were lost, but this isn't a full drain confirmation."
fi
