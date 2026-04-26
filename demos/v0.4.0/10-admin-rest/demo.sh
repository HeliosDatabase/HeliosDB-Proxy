#!/usr/bin/env bash
# curl tour of the 8 new v0.4.0 admin endpoints. Pretty-prints
# every response with jq.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-run}" in
  down) docker compose down -v; exit 0 ;;
esac

echo "=== Admin REST tour ==="
docker compose up -d
../_shared/wait-for.sh localhost 9090 60

show() {
  local label="$1"; shift
  echo
  echo "$label"
  out=$("$@" 2>&1) && echo "   $out" | sed 's/^/   /' | head -10 || echo "   (error: $out)"
}

show "GET /topology" curl -s http://localhost:9090/topology
show "GET /plugins"  curl -s http://localhost:9090/plugins
show "GET /anomalies?limit=5" curl -s "http://localhost:9090/anomalies?limit=5"
show "GET /api/edge" curl -s http://localhost:9090/api/edge

show "POST /api/edge/register {edge_id:e1,...}" \
  curl -s -X POST http://localhost:9090/api/edge/register \
    -H 'Content-Type: application/json' \
    -d '{"edge_id":"e1","region":"us-east","base_url":"http://e1"}'

show "POST /api/edge/invalidate {tables:[users]}" \
  curl -s -X POST http://localhost:9090/api/edge/invalidate \
    -H 'Content-Type: application/json' \
    -d '{"tables":["users"]}'

show "GET /api/chaos" curl -s http://localhost:9090/api/chaos

show "POST /api/chaos {force_unhealthy pg-primary:5432}" \
  curl -s -X POST http://localhost:9090/api/chaos \
    -H 'Content-Type: application/json' \
    -d '{"action":"force_unhealthy","target_node":"pg-primary:5432"}'

show "POST /api/chaos {reset}" \
  curl -s -X POST http://localhost:9090/api/chaos \
    -H 'Content-Type: application/json' \
    -d '{"action":"reset"}'

show "POST /api/replay {1h window, target=pg-primary}" \
  curl -s -X POST http://localhost:9090/api/replay \
    -H 'Content-Type: application/json' \
    -d "{\"from\":\"$(date -u -d '1 hour ago' +%FT%TZ)\",\"to\":\"$(date -u +%FT%TZ)\",\"target_host\":\"pg-primary\",\"target_port\":5432,\"target_user\":\"postgres\",\"target_password\":\"postgres\",\"target_database\":\"demo\"}"

show "POST /api/shadow {SELECT 1, source=shadow=pg-primary}" \
  curl -s -X POST http://localhost:9090/api/shadow \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT 1","source_host":"pg-primary","source_port":5432,"source_user":"postgres","source_password":"postgres","source_database":"demo","shadow_host":"pg-primary","shadow_port":5432,"shadow_user":"postgres","shadow_password":"postgres","shadow_database":"demo"}'

echo
echo "Tear down: ./demo.sh down"
