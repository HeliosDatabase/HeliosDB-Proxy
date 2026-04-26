#!/usr/bin/env bash
# Anomaly Detection demo — fires SQL injection + auth burst payloads
# at the proxy, then prints the detections from /anomalies.
set -euo pipefail
cd "$(dirname "$0")"

action="${1:-run}"
case "$action" in
  down)
    docker compose down -v
    exit 0
    ;;
  up)
    docker compose up -d
    ../_shared/wait-for.sh localhost 9090 60
    echo "proxy up at localhost:6432 (PG) + localhost:9090 (admin)"
    exit 0
    ;;
  run|"")
    ;;
  *)
    echo "usage: $0 [run|up|down]" >&2
    exit 1
    ;;
esac

echo "=== Anomaly Detection Demo ==="
echo "[1/4] Starting services..."
docker compose up -d

echo "[2/4] Waiting for proxy admin port (9090)..."
../_shared/wait-for.sh localhost 9090 60

echo "[3/4] Firing attack payloads..."

# Three SQL-injection payloads. Use psql --no-password so a missed
# pg_hba.conf doesn't open an interactive prompt.
export PGPASSWORD=postgres

echo "   - classic OR injection"
psql -h localhost -p 6432 -U postgres -d demo --no-password -c \
  "SELECT * FROM users WHERE id = 1 OR 1=1 -- ;" >/dev/null 2>&1 || true

echo "   - stacked-query injection"
psql -h localhost -p 6432 -U postgres -d demo --no-password -c \
  "SELECT 1; DROP TABLE events; --" >/dev/null 2>&1 || true

echo "   - time-based blind probe"
psql -h localhost -p 6432 -U postgres -d demo --no-password -c \
  "SELECT * FROM users WHERE id = pg_sleep(5)" >/dev/null 2>&1 || true

echo "   - 12 failed-auth attempts from 10.0.0.99 (simulated via PG)"
# We can't easily fake the source IP from the host; the auth_burst
# detector still fires on (user, ip) where ip is whatever PG sees.
# Generate failures using a wrong password.
PGPASSWORD=wrongpw \
  bash -c 'for i in $(seq 1 12); do
    psql -h localhost -p 6432 -U alice -d demo --no-password -c "SELECT 1" >/dev/null 2>&1 || true
  done'

echo "[4/4] Polling /anomalies:"
echo
sleep 1

# Pretty-print events. jq does the heavy lifting.
curl -s "http://localhost:9090/anomalies?limit=50" | jq -r '
  .events[]
  | "  - \(.severity // "info" | ascii_upcase | (. + "       ") | .[0:8]) \(.kind | (. + "                  ") | .[0:18]) \(
      if .kind == "sql_injection" then (.patterns_matched | join(",") + " | " + (.sql_excerpt // "" | .[0:60]))
      elif .kind == "auth_burst" then "user=\(.user) ip=\(.client_ip) failures=\(.failures)"
      elif .kind == "rate_spike" then "tenant=\(.tenant) rate=\(.rate_per_sec) z=\(.z_score)"
      elif .kind == "novel_query" then (.sql_excerpt // "" | .[0:60])
      else . | tostring end
    )"
'

echo
echo "Total events in buffer: $(curl -s http://localhost:9090/anomalies?limit=1 | jq -r .buffer_total)"
echo
echo "Proxy still running. Tear down with: ./demo.sh down"
