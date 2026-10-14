#!/usr/bin/env bash
# Build gold and load the warehouse for every staged day, in date order. Safe to re-run.
#   bash scripts/gold_all.sh
set -euo pipefail
for d in $(ls data/local_lake/raw | sed 's/dt=//' | sort); do
  log="/tmp/gold_$d.log"
  python -m glue_jobs.build_gold --dt "$d" > "$log" 2>&1 || { tail -40 "$log"; exit 1; }
  grep -E "^gold for|rows|dropped" "$log" || true
  python -m warehouse.load_day --dt "$d"
done
echo "all days loaded"