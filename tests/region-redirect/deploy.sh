#!/usr/bin/env bash
# Deploys two PRIVATE-ONLY Fly apps for the cross-region /proxy redirect
# test, fixed names (mirrors ezthrottle4.0's ezthrottle-staging /
# ezthrottle-webhook-test convention): an Aquifer test instance with
# machines in two real regions, and aqueduct-runner/recorder as a
# controllable fake upstream + webhook receiver. Neither app has an
# [http_service]/[[services]] block -- no public exposure at all, driven
# entirely via `fly proxy`. Destroyed at the end of a full
# `make region-redirect-e2e` run (region-redirect-destroy); safe to do
# since there's no public HTTPS/TLS cert involved for either app to
# re-provision on the next run. State (app names, machine id, regions) is
# written to .state so test.sh/destroy.sh can pick up the same deployment.
#
# Usage: FLY_ORG=ezthrottle ./deploy.sh
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "$DIR/lib.sh"

: "${FLY_ORG:?FLY_ORG is required, e.g. FLY_ORG=ezthrottle make region-redirect-deploy}"
REGION_1="${AQUIFER_REGION_1:-iad}"
REGION_2="${AQUIFER_REGION_2:-ord}"

if [ -f "$STATE_FILE" ]; then
  echo "error: $STATE_FILE already exists -- run 'make region-redirect-destroy' first" >&2
  exit 1
fi

if [ ! -d "$RECORDER_DIR" ]; then
  echo "error: recorder not found at $RECORDER_DIR (expects aqueduct-runner as a sibling of this repo, or set AQUEDUCT_RUNNER_DIR)" >&2
  exit 1
fi

fly auth whoami > /dev/null 2>&1 || { echo "error: not logged in to Fly -- run 'flyctl auth login' first" >&2; exit 1; }

# Fixed names (not a random per-run suffix) -- mirrors ezthrottle4.0's
# ezthrottle-staging/ezthrottle-webhook-test convention. Since these apps
# stay private-only (no [[services]]/[http_service] block, never
# internet-routable regardless of name), there's no public DNS/TLS cert to
# worry about on repeated destroy+recreate, unlike a public Fly app.
AQUIFER_APP="aquifer-redirect-test"
RECORDER_APP="aquifer-redirect-recorder"
if fly apps list --org "$FLY_ORG" 2>/dev/null | grep -q "$AQUIFER_APP\|$RECORDER_APP"; then
  echo "error: $AQUIFER_APP or $RECORDER_APP already exists in org $FLY_ORG -- run 'make region-redirect-destroy' first" >&2
  exit 1
fi
WORKDIR="$(mktemp -d)"

# flyctl resolves a relative `dockerfile =` path against the fly.toml's own
# directory, not the WORKING_DIRECTORY positional -- so the generated
# configs live directly inside each app's real build context (repo root /
# recorder dir), under a name distinct enough not to collide with anything,
# and get cleaned up on exit regardless of success or failure.
AQUIFER_CONFIG="$REPO_ROOT/fly.region-redirect-test.toml"
RECORDER_CONFIG="$RECORDER_DIR/fly.region-redirect-test.toml"
cleanup_configs() { rm -f "$AQUIFER_CONFIG" "$RECORDER_CONFIG"; }
trap cleanup_configs EXIT

echo "== Deploying $AQUIFER_APP (private-only, regions: $REGION_1,$REGION_2) =="

cat > "$AQUIFER_CONFIG" <<EOF
app = "$AQUIFER_APP"
primary_region = "$REGION_1"

[build]
  dockerfile = "Dockerfile.bench"

[env]
  AQUIFER_ADAPTER = "http"
  PORT = "8080"
  DB_PATH = "/tmp/aquifer.db"
  AQUIFER_FLY_REGIONS = "$REGION_1,$REGION_2"
  AQUIFER_FLY_POLL_INTERVAL_SECONDS = "5"

[[vm]]
  memory = "256mb"
  cpu_kind = "shared"
  cpus = 1
EOF
# Deliberately NO [http_service]/[[services]] block -- private 6PN
# connectivity works regardless of it (any port the app listens on is
# reachable via <region>.$AQUIFER_APP.internal from other machines in this
# org), but omitting it means Fly's public edge proxy has nothing to route
# to this app at all. See API.md's "Cross-region redirect" section.

fly apps create "$AQUIFER_APP" --org "$FLY_ORG"
fly deploy "$REPO_ROOT" --config "$AQUIFER_CONFIG" --app "$AQUIFER_APP" --ha=false -y

echo "== Adding a second machine in $REGION_2 =="
ORIGIN_MACHINE_ID="$(fly machine list --app "$AQUIFER_APP" --json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')"
fly machine clone "$ORIGIN_MACHINE_ID" --app "$AQUIFER_APP" --region "$REGION_2"

echo "== Deploying $RECORDER_APP (private-only, $REGION_1) =="
cat > "$RECORDER_CONFIG" <<EOF
app = "$RECORDER_APP"
primary_region = "$REGION_1"

[build]

[[vm]]
  memory = "256mb"
  cpu_kind = "shared"
  cpus = 1
EOF
fly apps create "$RECORDER_APP" --org "$FLY_ORG"
fly deploy "$RECORDER_DIR" --config "$RECORDER_CONFIG" --app "$RECORDER_APP" --ha=false -y

echo "== Waiting for both apps to report healthy machines =="
for app in "$AQUIFER_APP" "$RECORDER_APP"; do
  for i in $(seq 1 30); do
    state="$(fly machine list --app "$app" --json | python3 -c 'import json,sys; ms=json.load(sys.stdin); print(",".join(m["state"] for m in ms))' 2>/dev/null || echo "")"
    echo "  $app: $state"
    if [ -n "$state" ] && ! echo "$state" | grep -qv "started"; then
      break
    fi
    sleep 5
  done
done

cat > "$STATE_FILE" <<EOF
AQUIFER_APP="$AQUIFER_APP"
RECORDER_APP="$RECORDER_APP"
ORIGIN_MACHINE_ID="$ORIGIN_MACHINE_ID"
REGION_1="$REGION_1"
REGION_2="$REGION_2"
WORKDIR="$WORKDIR"
EOF

echo ""
echo "== Deployed. State saved to $STATE_FILE =="
echo "   Run 'make region-redirect-test' next, then 'make region-redirect-destroy' when done."
