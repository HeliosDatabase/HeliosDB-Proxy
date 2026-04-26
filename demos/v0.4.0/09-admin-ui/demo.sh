#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-up}" in
  down) docker compose down -v; exit 0 ;;
esac

echo "=== Admin UI Demo ==="
echo "[1/2] Starting proxy + Postgres"
docker compose up -d
../_shared/wait-for.sh localhost 9090 60

echo "[2/2] Dashboard at http://localhost:9090/"
echo
echo "   Open in your browser. Auto-refresh every 5s."
echo "   Tear down: ./demo.sh down"

# Best-effort browser launch.
if command -v xdg-open >/dev/null; then xdg-open http://localhost:9090/ &>/dev/null || true
elif command -v open >/dev/null; then open http://localhost:9090/ &>/dev/null || true
fi
