#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${APP_PREFIX:-}" ]]; then
  echo "Set APP_PREFIX to the value printed by deploy-quickstart.sh." >&2
  exit 1
fi

for suffix in aquifer-a aquifer-b canalis valkey; do
  app_name="$APP_PREFIX-$suffix"
  if flyctl status --app "$app_name" >/dev/null 2>&1; then
    flyctl apps destroy "$app_name" --yes
  fi
done
