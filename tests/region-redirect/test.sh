#!/usr/bin/env bash
# Drives the real redirect scenario against the deployment from deploy.sh,
# entirely via `fly proxy` tunnels (never a public URL), and asserts on
# both the HTTP response and origin's own [region_redirect] log lines.
#
# Scenario: origin's region has genuine backlog on a domain (from a real
# /jobs submission against a temporarily-failing upstream), the other
# region doesn't -- so a real /proxy request on origin should redirect and
# come back served directly by the other region.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$DIR/lib.sh"
require_state

SUFFIX="$(date +%s)-$RANDOM"

trap proxies_down EXIT
echo "== Opening fly proxy tunnels to $AQUIFER_APP ($ORIGIN_MACHINE_ID) and $RECORDER_APP =="
proxies_up

RECORDER_INTERNAL_URL="http://$RECORDER_APP.internal:5000"

echo ""
echo "== Scenario: origin's region has real backlog on a domain, target region doesn't =="

curl -sf -X POST "http://localhost:$RECORDER_PROXY_PORT/reset" > /dev/null

echo "-- Step 1: configure the shared upstream to fail, submit a real /jobs request to origin (creates real backlog for this domain on origin specifically)"
curl -sf -X POST "http://localhost:$RECORDER_PROXY_PORT/upstream/configure" \
  -H "Content-Type: application/json" \
  -d '{"status": 503, "body": "still down"}' > /dev/null

BACKLOG_JOB_ID="$(curl -sf -X POST "http://localhost:$AQUIFER_PROXY_PORT/jobs" \
  -H "Content-Type: application/json" \
  -d "{\"user_id\":\"redirect-e2e\",\"idempotent_key\":\"backlog-$SUFFIX\",\"url\":\"$RECORDER_INTERNAL_URL/upstream/target\",\"method\":\"POST\",\"webhook_url\":\"$RECORDER_INTERNAL_URL/webhook\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')"
echo "   queued backlog job: $BACKLOG_JOB_ID"

echo "-- Step 2: reconfigure the upstream to succeed (target region can now serve it directly; origin still has real backlog from step 1)"
curl -sf -X POST "http://localhost:$RECORDER_PROXY_PORT/upstream/configure" \
  -H "Content-Type: application/json" \
  -d '{"status": 200, "body": "{\"ok\": true}", "headers": {"X-Served-By-Test": "recorder"}}' > /dev/null

echo "-- Step 3: the real test request, via /proxy on origin"
RESP_HEADERS="$(mktemp)"
RESP_BODY="$(curl -sf -D "$RESP_HEADERS" -X POST "http://localhost:$AQUIFER_PROXY_PORT/proxy" \
  -H "Content-Type: application/json" \
  -d "{\"user_id\":\"redirect-e2e\",\"idempotent_key\":\"redirect-test-$SUFFIX\",\"url\":\"$RECORDER_INTERNAL_URL/upstream/target\",\"method\":\"POST\",\"webhook_url\":\"$RECORDER_INTERNAL_URL/webhook\"}")"

echo "   response headers:"
cat "$RESP_HEADERS"
echo "   response body: $RESP_BODY"

FAIL=0
if ! grep -qi "^x-served-by-test: recorder" "$RESP_HEADERS"; then
  echo "FAIL: expected the recorder's response header relayed verbatim (direct success via redirect)" >&2
  FAIL=1
fi
rm -f "$RESP_HEADERS"

echo ""
echo "== Confirming via origin's own logs that redirect actually triggered =="
# Fly's log shipping lags a few seconds behind real-time -- retry rather
# than trust a single --no-tail snapshot taken right after the request.
LOGS=""
for i in $(seq 1 10); do
  LOGS="$(fly logs --app "$AQUIFER_APP" --no-tail 2>&1 | grep '\[region_redirect\]' || true)"
  if echo "$LOGS" | grep -q "succeeded directly\|accepted it into its own queue"; then
    break
  fi
  sleep 3
done
echo "$LOGS"
if ! echo "$LOGS" | grep -q "trying candidates"; then
  echo "FAIL: expected origin's logs to show a real redirect attempt was made" >&2
  FAIL=1
fi
if ! echo "$LOGS" | grep -q "succeeded directly\|accepted it into its own queue"; then
  echo "FAIL: expected origin's logs to show the redirect actually resolved somewhere" >&2
  FAIL=1
fi

if [ "$FAIL" -ne 0 ]; then
  echo ""
  echo "== FAIL: cross-region redirect test did not pass =="
  exit 1
fi

echo ""
echo "== PASS: cross-region redirect confirmed against real Fly infrastructure =="
