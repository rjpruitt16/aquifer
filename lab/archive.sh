#!/bin/bash
# archive.sh RUN_DIR NAME: copy a run's report and changed app files into lab/runs/NAME
set -e
src=$1; dst=$(dirname "$0")/runs/$2; mkdir -p "$dst"
cp "$src/REPORT.md" "$dst/" 2>/dev/null || true
for f in "$src"/*.py; do cmp -s "$f" "$(dirname "$0")/sample-app/$(basename "$f")" || cp "$f" "$dst/"; done
