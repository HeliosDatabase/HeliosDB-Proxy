#!/usr/bin/env bash
set -euo pipefail

ROOT="${DEMOGROUND:-$HOME/HDB/Proxy-Demogrounds}"
MAP="$ROOT/module-map.md"
if [[ ! -f "$MAP" ]]; then
  MAP="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/module-map.md"
fi

echo "HeliosProxy 46-module tour"
echo "Map: $MAP"
echo
awk -F'|' '
  function trim(s) { gsub(/^[ \t]+|[ \t]+$/, "", s); return s }
  /^\| [0-9]+ \|/ {
    printf "%-3s %-8s %-28s %-18s %s\n", trim($2), trim($3), trim($4), trim($5), trim($6)
  }
' "$MAP"

echo
echo "Fast proof points:"
echo "  Admin UI:      http://localhost:9090/"
echo "  Admin REST:    curl -s http://localhost:9090/topology | jq ."
echo "  Anomalies:     curl -s http://localhost:9090/anomalies?limit=10 | jq ."
echo "  Plugins:       curl -s http://localhost:9090/plugins | jq ."
echo "  Shadow:        bin/run-main-demos.sh shadow"
echo "  Replay:        bin/run-main-demos.sh switchover"
