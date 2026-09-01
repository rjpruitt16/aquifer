#!/usr/bin/env bash
# Tears down everything deploy.sh created. Always destroys, never leaves
# apps merely scaled-to-zero -- these are ephemeral test apps in a
# public-repo-adjacent org.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$DIR/lib.sh"

if [ ! -f "$STATE_FILE" ]; then
  echo "nothing to destroy (no $STATE_FILE)"
  exit 0
fi
# shellcheck disable=SC1090
source "$STATE_FILE"

proxies_down

echo "== Destroying $AQUIFER_APP =="
fly apps destroy "$AQUIFER_APP" -y 2>&1 || echo "warning: failed to destroy $AQUIFER_APP -- check manually"
echo "== Destroying $RECORDER_APP =="
fly apps destroy "$RECORDER_APP" -y 2>&1 || echo "warning: failed to destroy $RECORDER_APP -- check manually"

if [ -n "${WORKDIR:-}" ]; then
  rm -rf "$WORKDIR"
fi
rm -f "$STATE_FILE"
echo "== Done =="
