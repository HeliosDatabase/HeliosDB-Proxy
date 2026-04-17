#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# observe.sh — Watch lag-aware routing decisions in real time.
#
# Shows which backend node handles each query, lag metrics per node,
# and read-your-writes (RYW) routing behavior after writes.
#
# Usage:
#   ./observe.sh            # Loop every 1s (Ctrl-C to stop)
#   ./observe.sh --once     # Run once and exit
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

PROXY_HOST="localhost"
PROXY_PORT=66432
ADMIN_URL="http://localhost:69090"
DB_USER="app"
DB_PASS="apppass"
DB_NAME="appdb"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

ONCE=false
[[ "${1:-}" == "--once" ]] && ONCE=true

# Ensure the test table exists
PGPASSWORD="$DB_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" \
  -U "$DB_USER" -d "$DB_NAME" -q -c \
  "CREATE TABLE IF NOT EXISTS ryw_test (id serial PRIMARY KEY, val text, ts timestamptz DEFAULT now());" \
  2>/dev/null || true

iteration=0

while true; do
  iteration=$((iteration + 1))
  clear
  echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════════╗${NC}"
  echo -e "${BOLD}${CYAN}║          HeliosProxy — Lag-Aware Routing Observer               ║${NC}"
  echo -e "${BOLD}${CYAN}║          Iteration #${iteration}  $(date '+%H:%M:%S')                              ║${NC}"
  echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════════╝${NC}"

  # ── 1. Node lag metrics from admin API ────────────────────────────
  echo -e "\n${BOLD}[1] Node Status & Lag Metrics${NC}"
  echo -e "${DIM}────────────────────────────────────────────────${NC}"
  if curl -sf "$ADMIN_URL/nodes" > /dev/null 2>&1; then
    curl -sf "$ADMIN_URL/nodes" | jq -r '
      .[] | [.name, .role,
             (if .lag_ms then
                if .lag_ms > 100 then "\u001b[31m\(.lag_ms) ms\u001b[0m"
                elif .lag_ms > 50 then "\u001b[33m\(.lag_ms) ms\u001b[0m"
                else "\u001b[32m\(.lag_ms) ms\u001b[0m"
                end
              else "n/a" end),
             (if .healthy then "\u001b[32mhealthy\u001b[0m"
              else "\u001b[31munhealthy\u001b[0m" end),
             (if .eligible_for_reads then "\u001b[32myes\u001b[0m"
              else "\u001b[31mno\u001b[0m" end)
      ] | @tsv
    ' | column -t -N "NODE,ROLE,LAG,HEALTH,READS?" -s $'\t' 2>/dev/null || \
    curl -sf "$ADMIN_URL/nodes" | jq '.'
  else
    echo -e "${YELLOW}  Admin API not reachable at $ADMIN_URL${NC}"
    echo "  Checking PostgreSQL replication directly..."
    PGPASSWORD="$DB_PASS" psql -h localhost -p 65432 -U "$DB_USER" -d "$DB_NAME" -c \
      "SELECT application_name AS node,
              sync_state,
              pg_wal_lsn_diff(sent_lsn, replay_lsn) AS lag_bytes,
              replay_lag
       FROM pg_stat_replication
       ORDER BY application_name;" 2>/dev/null || echo "  Could not reach primary either."
  fi

  # ── 2. Read query — which node handles it? ────────────────────────
  echo -e "\n${BOLD}[2] Read Query Routing${NC}"
  echo -e "${DIM}────────────────────────────────────────────────${NC}"
  read_result=$(PGPASSWORD="$DB_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" \
    -U "$DB_USER" -d "$DB_NAME" -tA -c \
    "SELECT inet_server_addr() || ' (' || current_setting('cluster_name', true) || ')';" 2>/dev/null || echo "error")
  echo -e "  SELECT routed to: ${GREEN}${read_result}${NC}"

  # Do a second read to show round-robin
  read_result2=$(PGPASSWORD="$DB_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" \
    -U "$DB_USER" -d "$DB_NAME" -tA -c \
    "SELECT inet_server_addr() || ' (' || current_setting('cluster_name', true) || ')';" 2>/dev/null || echo "error")
  echo -e "  SELECT routed to: ${GREEN}${read_result2}${NC}"

  # ── 3. RYW test: write then immediate read in same session ────────
  echo -e "\n${BOLD}[3] Read-Your-Writes (RYW) Test${NC}"
  echo -e "${DIM}────────────────────────────────────────────────${NC}"
  ryw_val="ryw_$(date +%s%N)"
  ryw_result=$(PGPASSWORD="$DB_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" \
    -U "$DB_USER" -d "$DB_NAME" -tA <<SQL 2>/dev/null || echo "error"
    INSERT INTO ryw_test (val) VALUES ('$ryw_val');
    SELECT val || ' -> node: ' || inet_server_addr()
    FROM ryw_test WHERE val = '$ryw_val';
SQL
  )
  echo -e "  INSERT '$ryw_val' then immediate SELECT:"
  if echo "$ryw_result" | grep -q "$ryw_val"; then
    echo -e "  ${GREEN}Found own write${NC} — $ryw_result"
    echo -e "  ${DIM}(RYW pinned read to primary/sync standby after write)${NC}"
  else
    echo -e "  ${RED}Did not find own write!${NC} — stale read from lagging replica"
  fi

  # ── 4. Routing summary ────────────────────────────────────────────
  echo -e "\n${BOLD}[4] Routing Summary${NC}"
  echo -e "${DIM}────────────────────────────────────────────────${NC}"
  if curl -sf "$ADMIN_URL/stats" > /dev/null 2>&1; then
    curl -sf "$ADMIN_URL/stats" | jq -r '
      "  Queries routed:  \(.queries_routed // "n/a")",
      "  Reads to primary (RYW): \(.ryw_primary_reads // "n/a")",
      "  Lag reroutes:    \(.lag_reroutes // "n/a")"
    ' 2>/dev/null || echo "  (stats format varies by version)"
  else
    echo -e "  ${DIM}Admin API stats not available${NC}"
  fi

  echo -e "\n${DIM}Press Ctrl-C to stop. Refresh interval: 1s${NC}"

  $ONCE && break
  sleep 1
done
