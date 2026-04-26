#!/usr/bin/env bash
# Edge / Geo Proxy demo — show cache hit, invalidation broadcast,
# cache-miss-after-invalidation in one run.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-run}" in
  down) docker compose down -v; exit 0 ;;
  up)
    docker compose up -d
    ../_shared/wait-for.sh localhost 9090 60
    ../_shared/wait-for.sh localhost 9091 60
    echo "home  PG=localhost:6432  admin=localhost:9090"
    echo "edge  PG=localhost:6433  admin=localhost:9091"
    exit 0 ;;
esac

echo "=== Edge Proxy Demo ==="
echo "[1/5] Starting home + edge + Postgres..."
docker compose up -d
../_shared/wait-for.sh localhost 9090 60
../_shared/wait-for.sh localhost 9091 60

echo "[2/5] Registering edge with home..."
curl -s -X POST http://localhost:9090/api/edge/register \
  -H 'Content-Type: application/json' \
  -d '{"edge_id":"edge-eu-west","region":"eu-west","base_url":"http://proxy-edge:9090"}' \
  | jq -r '"   {\"edge_id\":\"\(.edge_id)\",\"registered_at\":\"\(.registered_at)\"}"'

export PGPASSWORD=postgres
echo "[3/5] First read at edge (cache cold):"
t=$(/usr/bin/time -f '%e' psql -h localhost -p 6433 -U postgres -d demo --no-password \
  -c "SELECT name FROM users WHERE id = 1" -At 2>&1 1>/dev/null || true)
echo "   query took ${t}s"

echo "[4/5] Second read at edge (cache hit):"
t=$(/usr/bin/time -f '%e' psql -h localhost -p 6433 -U postgres -d demo --no-password \
  -c "SELECT name FROM users WHERE id = 1" -At 2>&1 1>/dev/null || true)
echo "   query took ${t}s   (compare: ./demo.sh stats for hit/miss counters)"

echo "[5/5] Write at home + invalidate broadcast:"
psql -h localhost -p 6432 -U postgres -d demo --no-password \
  -c "UPDATE users SET name = 'CHANGED' WHERE id = 1" >/dev/null
curl -s -X POST http://localhost:9090/api/edge/invalidate \
  -H 'Content-Type: application/json' \
  -d '{"tables":["users"]}' | jq -c .

echo
echo "[6/5] Read at edge again (cache miss → fresh data):"
val=$(psql -h localhost -p 6433 -U postgres -d demo --no-password -At \
  -c "SELECT name FROM users WHERE id = 1")
echo "   value = \"${val}\""

echo
echo "Cache stats on edge:"
curl -s http://localhost:9091/api/edge | jq .cache
echo
echo "Tear down: ./demo.sh down"
