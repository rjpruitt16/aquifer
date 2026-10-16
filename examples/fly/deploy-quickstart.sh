#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
APP_NAME=${APP_NAME:-aquifer-$(date +%s)}
FLY_REGION=${FLY_REGION:-iad}
VOLUME_SIZE_GB=${VOLUME_SIZE_GB:-1}
REQUESTED_VERSION=${AQUIFER_VERSION:-latest}

for command_name in flyctl curl; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "$command_name is required" >&2
    exit 1
  fi
done

flyctl auth whoami >/dev/null

if [ "$REQUESTED_VERSION" = "latest" ]; then
  RELEASE_URL=$(curl --fail --silent --show-error --location --head \
    --output /dev/null --write-out '%{url_effective}' \
    https://github.com/rjpruitt16/aquifer/releases/latest)
  AQUIFER_VERSION=$(basename "$RELEASE_URL")
else
  AQUIFER_VERSION="$REQUESTED_VERSION"
  case "$AQUIFER_VERSION" in
    v*) ;;
    *) AQUIFER_VERSION="v$AQUIFER_VERSION" ;;
  esac
fi

if ! flyctl status --app "$APP_NAME" >/dev/null 2>&1; then
  CREATE_ARGS=("$APP_NAME" --yes)
  if [ -n "${FLY_ORG:-}" ]; then
    CREATE_ARGS+=(--org "$FLY_ORG")
  fi
  flyctl apps create "${CREATE_ARGS[@]}"
fi

if ! flyctl volumes list --app "$APP_NAME" | grep -Eq '(^|[[:space:]])aquifer_data([[:space:]]|$)'; then
  flyctl volumes create aquifer_data \
    --app "$APP_NAME" \
    --region "$FLY_REGION" \
    --size "$VOLUME_SIZE_GB" \
    --vm-cpu-kind shared \
    --vm-cpus 1 \
    --vm-memory 512 \
    --yes
fi

flyctl deploy "$ROOT_DIR" \
  --app "$APP_NAME" \
  --config "$ROOT_DIR/fly.toml" \
  --dockerfile "$ROOT_DIR/deploy/Dockerfile.release" \
  --primary-region "$FLY_REGION" \
  --build-arg "AQUIFER_VERSION=$AQUIFER_VERSION" \
  --flycast \
  --no-public-ips \
  --ha=false \
  --yes

flyctl status --app "$APP_NAME"
printf '\nAquifer %s is private on Fly.io. Connect with:\n' "$AQUIFER_VERSION"
printf '  flyctl proxy 8080:8080 --app %s\n' "$APP_NAME"
printf '  curl http://127.0.0.1:8080/ready\n'
