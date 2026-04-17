#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# induce-lag.sh — Add or remove artificial replication lag on the
#                 asynchronous standby for the lag-aware routing demo.
#
# Usage:
#   ./induce-lag.sh on       Add 500ms network delay to async standby
#   ./induce-lag.sh off      Remove artificial delay
#   ./induce-lag.sh status   Show current lag from proxy admin API
#   ./induce-lag.sh load     Generate heavy writes to create natural lag
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

ASYNC_CONTAINER="lag-standby-async"
PROXY_ADMIN="http://localhost:69090"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

header() { echo -e "\n${CYAN}═══ $1 ═══${NC}\n"; }

case "${1:-status}" in
  on)
    header "Inducing 500ms network delay on $ASYNC_CONTAINER"
    docker exec "$ASYNC_CONTAINER" sh -c '
      apk add --no-cache iproute2 > /dev/null 2>&1 || true
      tc qdisc del dev eth0 root 2>/dev/null || true
      tc qdisc add dev eth0 root netem delay 500ms
    '
    echo -e "${RED}Delay added.${NC} Replication from primary will lag ~500ms."
    echo -e "Run ${YELLOW}./induce-lag.sh status${NC} to see the effect."
    ;;

  off)
    header "Removing network delay from $ASYNC_CONTAINER"
    docker exec "$ASYNC_CONTAINER" sh -c '
      tc qdisc del dev eth0 root 2>/dev/null || true
    '
    echo -e "${GREEN}Delay removed.${NC} Async standby will catch up shortly."
    ;;

  load)
    header "Generating heavy write load on primary to create natural lag"
    echo "Inserting 50,000 rows in rapid succession..."
    PGPASSWORD=apppass psql -h localhost -p 65432 -U app -d appdb -q <<'SQL'
      CREATE TABLE IF NOT EXISTS lag_load (
        id   serial PRIMARY KEY,
        data text,
        ts   timestamptz DEFAULT now()
      );
      INSERT INTO lag_load (data)
      SELECT repeat('x', 1000)
      FROM generate_series(1, 50000);
SQL
    echo -e "${YELLOW}Write load complete.${NC} Check lag with: ./induce-lag.sh status"
    ;;

  status)
    header "Current replication lag (from proxy admin API)"
    if curl -sf "$PROXY_ADMIN/nodes" > /dev/null 2>&1; then
      curl -sf "$PROXY_ADMIN/nodes" | jq -r '
        .[] | "\(.name)\t\(.role)\t\(.lag_ms // "n/a") ms\t\(.healthy)"
      ' | column -t -N "NODE,ROLE,LAG,HEALTHY" -s $'\t'
    else
      echo -e "${YELLOW}Proxy admin API not reachable. Checking PostgreSQL directly...${NC}"
      echo ""
      echo "Primary view of replication:"
      PGPASSWORD=apppass psql -h localhost -p 65432 -U app -d appdb -x -c \
        "SELECT application_name, state, sync_state,
                pg_wal_lsn_diff(sent_lsn, replay_lsn) AS replay_lag_bytes,
                replay_lag
         FROM pg_stat_replication
         ORDER BY application_name;"
    fi
    ;;

  *)
    echo "Usage: $0 {on|off|load|status}"
    exit 1
    ;;
esac
