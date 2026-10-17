#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
FLY_DIR="$ROOT_DIR/examples/canalis/fly"
APP_PREFIX=${APP_PREFIX:-canalis-aquifer-$(date +%s)}
FLY_REGION=${FLY_REGION:-iad}
VOLUME_SIZE_GB=${VOLUME_SIZE_GB:-1}
AQUIFER_VERSION=${AQUIFER_VERSION:-latest}
CANALIS_VERSION=${CANALIS_VERSION:-0.2}
AQUIFER_ALLOWED_URL_DOMAINS=${AQUIFER_ALLOWED_URL_DOMAINS:-postman-echo.com}
CANALIS_LOCAL_PORT=${CANALIS_LOCAL_PORT:-18080}

VALKEY_APP="$APP_PREFIX-valkey"
CANALIS_APP="$APP_PREFIX-canalis"
AQUIFER_A_APP="$APP_PREFIX-aquifer-a"
AQUIFER_B_APP="$APP_PREFIX-aquifer-b"

for command_name in flyctl curl; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "$command_name is required" >&2
    exit 1
  fi
done

flyctl auth whoami >/dev/null

create_app() {
  local app_name=$1
  if flyctl status --app "$app_name" >/dev/null 2>&1; then
    return
  fi
  local args=("$app_name" --yes)
  if [[ -n "${FLY_ORG:-}" ]]; then
    args+=(--org "$FLY_ORG")
  fi
  flyctl apps create "${args[@]}"
}

create_volume() {
  local app_name=$1
  local volume_name=$2
  if flyctl volumes list --app "$app_name" | grep -Eq "(^|[[:space:]])$volume_name([[:space:]]|$)"; then
    return
  fi
  flyctl volumes create "$volume_name" \
    --app "$app_name" \
    --region "$FLY_REGION" \
    --size "$VOLUME_SIZE_GB" \
    --vm-cpu-kind shared \
    --vm-cpus 1 \
    --vm-memory 512 \
    --yes
}

for app_name in "$VALKEY_APP" "$CANALIS_APP" "$AQUIFER_A_APP" "$AQUIFER_B_APP"; do
  create_app "$app_name"
done

create_volume "$VALKEY_APP" valkey_data
create_volume "$CANALIS_APP" canalis_data
create_volume "$AQUIFER_A_APP" aquifer_data
create_volume "$AQUIFER_B_APP" aquifer_data

flyctl deploy "$FLY_DIR" \
  --app "$VALKEY_APP" \
  --config "$FLY_DIR/valkey.fly.toml" \
  --image valkey/valkey:8 \
  --primary-region "$FLY_REGION" \
  --no-public-ips \
  --ha=false \
  --yes

flyctl secrets set \
  --app "$CANALIS_APP" \
  "CANALIS_VALKEY_URL=redis://$VALKEY_APP.internal:6379"
flyctl deploy "$FLY_DIR" \
  --app "$CANALIS_APP" \
  --config "$FLY_DIR/canalis.fly.toml" \
  --image "ghcr.io/rjpruitt16/canalis-rs:$CANALIS_VERSION" \
  --primary-region "$FLY_REGION" \
  --flycast \
  --no-public-ips \
  --ha=false \
  --yes

for app_name in "$AQUIFER_A_APP" "$AQUIFER_B_APP"; do
  flyctl secrets set \
    --app "$app_name" \
    "AQUIFER_REGISTRY_URL=http://$CANALIS_APP.internal:8080/register" \
    "AQUIFER_ALLOWED_URL_DOMAINS=$AQUIFER_ALLOWED_URL_DOMAINS"
  flyctl deploy "$ROOT_DIR" \
    --app "$app_name" \
    --config "$FLY_DIR/aquifer.fly.toml" \
    --dockerfile "$ROOT_DIR/deploy/Dockerfile.release" \
    --primary-region "$FLY_REGION" \
    --build-arg "AQUIFER_VERSION=$AQUIFER_VERSION" \
    --flycast \
    --no-public-ips \
    --ha=false \
    --yes
done

PROXY_LOG=$(mktemp)
flyctl proxy "$CANALIS_LOCAL_PORT:8080" --app "$CANALIS_APP" >"$PROXY_LOG" 2>&1 &
PROXY_PID=$!
cleanup_proxy() {
  kill "$PROXY_PID" >/dev/null 2>&1 || true
  wait "$PROXY_PID" >/dev/null 2>&1 || true
  rm -f "$PROXY_LOG"
}
trap cleanup_proxy EXIT

echo "Waiting for both Aquifer nodes to register with Canalis..."
for attempt in $(seq 1 60); do
  HEALTH=$(curl --connect-timeout 2 --max-time 5 --silent --show-error "http://127.0.0.1:$CANALIS_LOCAL_PORT/health" 2>/dev/null || true)
  if grep --quiet '"total":2' <<<"$HEALTH"; then
    printf '%s\n' "$HEALTH"
    echo "Canalis sees both Aquifer nodes."
    printf '\nKeep this prefix for inspection and cleanup: %s\n' "$APP_PREFIX"
    printf 'Connect again: flyctl proxy 8080:8080 --app %s\n' "$CANALIS_APP"
    printf 'Cleanup: APP_PREFIX=%s ./examples/canalis/fly/destroy-quickstart.sh\n' "$APP_PREFIX"
    exit 0
  fi
  sleep 2
done

cat "$PROXY_LOG" >&2
echo "Canalis did not report two Aquifer nodes within two minutes." >&2
echo "Inspect with: flyctl logs --app $CANALIS_APP" >&2
echo "Cleanup with: APP_PREFIX=$APP_PREFIX ./examples/canalis/fly/destroy-quickstart.sh" >&2
exit 1
