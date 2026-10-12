#!/bin/sh
# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0
#
# Entry point for both soak targets. Starts Valkey when SOAK_VALKEY=1
# (Aquifer's WebSocket transcripts live there; it runs outside the
# supervisor, so a kill -9 of the target leaves it up, and a machine restart
# brings it back from its append-only file), then hands over to the
# supervisor, which runs the target and restarts it when it exits.
set -e
DATA="${SOAK_DATA_DIR:-/data}"
mkdir -p "$DATA"
if [ "${SOAK_VALKEY:-0}" = "1" ]; then
  mkdir -p "$DATA/valkey"
  valkey-server --bind 127.0.0.1 --port 6379 --dir "$DATA/valkey" \
    --appendonly yes --appendfsync everysec --save "" \
    --maxmemory "${SOAK_VALKEY_MAXMEMORY:-600mb}" --maxmemory-policy noeviction \
    --daemonize yes --logfile "$DATA/valkey/valkey.log"
fi
exec /app/supervisor -cmd "${SOAK_CMD:-/app/aquifer}" -data "$DATA" -store "${SOAK_STORE:-aquifer.db}"
