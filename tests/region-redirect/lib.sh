# Shared vars/functions for the region-redirect deploy/test/destroy scripts.
# Sourced, not executed directly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
AQUEDUCT_RUNNER_DIR="${AQUEDUCT_RUNNER_DIR:-$REPO_ROOT/../aqueduct-runner}"
RECORDER_DIR="$AQUEDUCT_RUNNER_DIR/recorder"
STATE_FILE="$SCRIPT_DIR/.state"

AQUIFER_PROXY_PORT=18080
RECORDER_PROXY_PORT=18081

require_state() {
  if [ ! -f "$STATE_FILE" ]; then
    echo "error: no deployment found (missing $STATE_FILE) -- run 'make region-redirect-deploy' first" >&2
    exit 1
  fi
  # shellcheck disable=SC1090
  source "$STATE_FILE"
}

proxies_up() {
  # Defensive: a prior crashed run can leave a `fly proxy` process bound to
  # these ports, which makes the new one fail with "address already in
  # use" instead of an actionable error.
  stale_pids="$(lsof -ti:"$AQUIFER_PROXY_PORT","$RECORDER_PROXY_PORT" 2>/dev/null || true)"
  if [ -n "$stale_pids" ]; then
    kill $stale_pids 2>/dev/null || true
    sleep 1
  fi
  # remote_host must be Fly's internal-DNS form for one specific machine --
  # a bare machine ID is not a valid host on its own.
  ( fly proxy "$AQUIFER_PROXY_PORT:8080" --app "$AQUIFER_APP" "$ORIGIN_MACHINE_ID.vm.$AQUIFER_APP.internal" > "$SCRIPT_DIR/.aquifer_proxy.log" 2>&1 & echo $! > "$SCRIPT_DIR/.aquifer_proxy.pid" )
  ( fly proxy "$RECORDER_PROXY_PORT:5000" --app "$RECORDER_APP" > "$SCRIPT_DIR/.recorder_proxy.log" 2>&1 & echo $! > "$SCRIPT_DIR/.recorder_proxy.pid" )
  sleep 8
  for i in $(seq 1 20); do
    if curl -sf "http://localhost:$AQUIFER_PROXY_PORT/health" > /dev/null 2>&1 && curl -sf "http://localhost:$RECORDER_PROXY_PORT/health" > /dev/null 2>&1; then
      return 0
    fi
    sleep 3
  done
  echo "error: proxy tunnels never came up. aquifer proxy log:" >&2
  cat "$SCRIPT_DIR/.aquifer_proxy.log" >&2 2>/dev/null || true
  echo "recorder proxy log:" >&2
  cat "$SCRIPT_DIR/.recorder_proxy.log" >&2 2>/dev/null || true
  return 1
}

proxies_down() {
  if [ -f "$SCRIPT_DIR/.aquifer_proxy.pid" ]; then
    kill "$(cat "$SCRIPT_DIR/.aquifer_proxy.pid")" 2>/dev/null || true
  fi
  if [ -f "$SCRIPT_DIR/.recorder_proxy.pid" ]; then
    kill "$(cat "$SCRIPT_DIR/.recorder_proxy.pid")" 2>/dev/null || true
  fi
  rm -f "$SCRIPT_DIR/.aquifer_proxy.pid" "$SCRIPT_DIR/.recorder_proxy.pid" "$SCRIPT_DIR/.aquifer_proxy.log" "$SCRIPT_DIR/.recorder_proxy.log"
}
