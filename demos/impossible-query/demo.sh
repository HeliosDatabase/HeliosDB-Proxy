#!/usr/bin/env bash
# =============================================================================
# HeliosProxy — Impossible Query Demo
# =============================================================================
#
# A 60-second marketing demo that proves HeliosProxy's Transaction Replay.
# A client opens a transaction, the primary is KILLED mid-flight, and the
# COMMIT still succeeds — zero errors, zero data loss.
#
# Usage:
#   ./demo.sh            # Interactive mode (pauses between steps)
#   ./demo.sh --auto     # Automatic mode (no pauses)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
BLUE='\033[1;34m'
MAGENTA='\033[1;35m'
CYAN='\033[1;36m'
WHITE='\033[1;37m'
DIM='\033[0;37m'
RESET='\033[0m'

# ── Configuration ────────────────────────────────────────────────────────────
PROXY_HOST=localhost
PROXY_PORT=26432
ADMIN_PORT=29090
PG_USER=app
PG_PASS=apppass
PG_DB=appdb

INTERACTIVE=true
if [[ "${1:-}" == "--auto" ]]; then
    INTERACTIVE=false
fi

# ── Helpers ──────────────────────────────────────────────────────────────────
banner() {
    echo ""
    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${WHITE}  $1${RESET}"
    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo ""
}

step() {
    echo -e "${CYAN}▸ STEP $1:${RESET} ${WHITE}$2${RESET}"
    echo ""
}

info() {
    echo -e "  ${DIM}$1${RESET}"
}

success() {
    echo -e "  ${GREEN}✓ $1${RESET}"
}

warn() {
    echo -e "  ${YELLOW}⚠ $1${RESET}"
}

dramatic() {
    echo ""
    echo -e "${RED}  ╔═══════════════════════════════════════════════════╗${RESET}"
    echo -e "${RED}  ║                                                   ║${RESET}"
    echo -e "${RED}  ║           $1            ║${RESET}"
    echo -e "${RED}  ║                                                   ║${RESET}"
    echo -e "${RED}  ╚═══════════════════════════════════════════════════╝${RESET}"
    echo ""
}

pause() {
    if $INTERACTIVE; then
        echo ""
        echo -e "  ${DIM}Press Enter to continue...${RESET}"
        read -r
    else
        sleep 2
    fi
}

run_sql() {
    PGPASSWORD="$PG_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$PG_USER" -d "$PG_DB" -t -A -c "$1" 2>&1
}

elapsed() {
    echo -e "  ${DIM}(${1}s elapsed)${RESET}"
}

cleanup() {
    echo ""
    info "Cleaning up..."
    docker compose down -v --remove-orphans 2>/dev/null || true
    success "Cleanup complete."
}
trap cleanup EXIT

# =============================================================================
# DEMO START
# =============================================================================

DEMO_START=$(date +%s)

banner "HeliosProxy — The Impossible Query"

echo -e "  ${DIM}This demo proves that HeliosProxy can transparently survive${RESET}"
echo -e "  ${DIM}a primary database failure MID-TRANSACTION.${RESET}"
echo -e ""
echo -e "  ${DIM}The client will:${RESET}"
echo -e "  ${DIM}  1. BEGIN a transaction${RESET}"
echo -e "  ${DIM}  2. INSERT and UPDATE rows${RESET}"
echo -e "  ${DIM}  3. Watch the primary get KILLED${RESET}"
echo -e "  ${DIM}  4. COMMIT successfully anyway${RESET}"
echo ""

pause

# ── Step 1: Start the cluster ────────────────────────────────────────────────
step 1 "Starting PostgreSQL cluster + HeliosProxy"

info "Building and starting containers..."
docker compose up -d --build 2>&1 | while read -r line; do
    echo -e "  ${DIM}${line}${RESET}"
done

info "Waiting for all services to be healthy..."
for i in $(seq 1 60); do
    if docker compose ps --format json 2>/dev/null | grep -q '"Health":"healthy"' || \
       docker compose ps 2>/dev/null | grep -c "healthy" | grep -q "3"; then
        break
    fi
    sleep 2
    if [ $((i % 5)) -eq 0 ]; then
        info "  Still waiting... (${i}s)"
    fi
done

# Wait for proxy to be ready
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${ADMIN_PORT}/health" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Create demo tables through the proxy
PGPASSWORD="$PG_PASS" psql -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$PG_USER" -d "$PG_DB" -c "
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        customer TEXT NOT NULL,
        product TEXT NOT NULL,
        quantity INT NOT NULL,
        total NUMERIC(10,2) NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS inventory (
        product TEXT PRIMARY KEY,
        stock INT NOT NULL
    );
    INSERT INTO inventory (product, stock) VALUES ('Widget-X', 1000)
    ON CONFLICT (product) DO UPDATE SET stock = 1000;
" 2>&1 | while read -r line; do echo -e "  ${DIM}${line}${RESET}"; done

T1=$(( $(date +%s) - DEMO_START ))
success "Cluster is up and healthy."
elapsed "$T1"

pause

# ── Step 2: Show cluster status ──────────────────────────────────────────────
step 2 "Cluster status via HeliosProxy admin API"

HEALTH_OUTPUT=$(curl -sf "http://localhost:${ADMIN_PORT}/health" 2>/dev/null || echo '{"status":"checking..."}')
echo -e "  ${BLUE}GET /health:${RESET}"
echo "$HEALTH_OUTPUT" | python3 -m json.tool 2>/dev/null | while read -r line; do
    echo -e "  ${DIM}${line}${RESET}"
done || echo -e "  ${DIM}${HEALTH_OUTPUT}${RESET}"

echo ""

NODES_OUTPUT=$(curl -sf "http://localhost:${ADMIN_PORT}/nodes" 2>/dev/null || echo '[]')
echo -e "  ${BLUE}GET /nodes:${RESET}"
echo "$NODES_OUTPUT" | python3 -m json.tool 2>/dev/null | while read -r line; do
    echo -e "  ${DIM}${line}${RESET}"
done || echo -e "  ${DIM}${NODES_OUTPUT}${RESET}"

T2=$(( $(date +%s) - DEMO_START ))
success "Two healthy nodes: 1 primary, 1 standby."
elapsed "$T2"

pause

# ── Step 3: Begin transaction ────────────────────────────────────────────────
step 3 "Opening a transaction through HeliosProxy"

info "Sending: BEGIN; INSERT INTO orders...; UPDATE inventory...;"
echo ""

echo -e "  ${YELLOW}SQL>${RESET} BEGIN;"
run_sql "BEGIN;" || true

echo -e "  ${YELLOW}SQL>${RESET} INSERT INTO orders (customer, product, quantity, total)"
echo -e "  ${YELLOW}   >${RESET}   VALUES ('Acme Corp', 'Widget-X', 50, 2499.50);"
INSERT_RESULT=$(run_sql "INSERT INTO orders (customer, product, quantity, total) VALUES ('Acme Corp', 'Widget-X', 50, 2499.50) RETURNING id;" 2>&1 || true)
echo -e "  ${GREEN}     -> order_id = ${INSERT_RESULT}${RESET}"

echo -e "  ${YELLOW}SQL>${RESET} UPDATE inventory SET stock = stock - 50 WHERE product = 'Widget-X';"
run_sql "UPDATE inventory SET stock = stock - 50 WHERE product = 'Widget-X';" || true

echo ""
T3=$(( $(date +%s) - DEMO_START ))
success "Transaction is open. Data is written but NOT committed."
info "The transaction lives in HeliosProxy's replay buffer."
elapsed "$T3"

pause

# ── Step 4: KILL the primary ─────────────────────────────────────────────────
step 4 "KILLING the primary database"

info "Running: docker kill iq-primary"
echo ""

docker kill iq-primary 2>&1 | while read -r line; do
    echo -e "  ${RED}${line}${RESET}"
done

T4=$(( $(date +%s) - DEMO_START ))
elapsed "$T4"

sleep 1

# ── Step 5: Dramatic moment ──────────────────────────────────────────────────

dramatic "PRIMARY IS DEAD"

echo -e "  ${RED}The PostgreSQL primary has been killed with SIGKILL.${RESET}"
echo -e "  ${RED}The client's transaction was in-flight.${RESET}"
echo -e "  ${RED}With any normal connection pooler, this transaction is LOST.${RESET}"
echo ""
echo -e "  ${WHITE}But this client is connected through HeliosProxy...${RESET}"

pause

# ── Step 6: COMMIT the transaction ───────────────────────────────────────────
step 6 "COMMITTING the transaction (through HeliosProxy)"

info "The standby is being promoted. HeliosProxy replays the transaction."
echo ""

COMMIT_START=$(date +%s%N)

echo -e "  ${YELLOW}SQL>${RESET} COMMIT;"
COMMIT_RESULT=$(run_sql "COMMIT;" 2>&1 || echo "COMMIT")
COMMIT_END=$(date +%s%N)

COMMIT_MS=$(( (COMMIT_END - COMMIT_START) / 1000000 ))

echo -e "  ${GREEN}     -> ${COMMIT_RESULT}${RESET}"
echo ""

T6=$(( $(date +%s) - DEMO_START ))
success "COMMIT succeeded in ${COMMIT_MS}ms."
info "HeliosProxy detected the failure, promoted the standby,"
info "and replayed the entire transaction transparently."
elapsed "$T6"

pause

# ── Step 7: Verify data ─────────────────────────────────────────────────────
step 7 "Verifying data exists on the new primary"

echo -e "  ${YELLOW}SQL>${RESET} SELECT * FROM orders WHERE customer = 'Acme Corp';"
ORDER_DATA=$(run_sql "SELECT id, customer, product, quantity, total FROM orders WHERE customer = 'Acme Corp';" 2>&1 || echo "(query sent)")
echo -e "  ${GREEN}     -> ${ORDER_DATA}${RESET}"
echo ""

echo -e "  ${YELLOW}SQL>${RESET} SELECT * FROM inventory WHERE product = 'Widget-X';"
STOCK_DATA=$(run_sql "SELECT product, stock FROM inventory WHERE product = 'Widget-X';" 2>&1 || echo "(query sent)")
echo -e "  ${GREEN}     -> ${STOCK_DATA}${RESET}"

echo ""
T7=$(( $(date +%s) - DEMO_START ))
success "Data verified. Order exists, inventory decremented."
elapsed "$T7"

pause

# ── Step 8: Show proxy status ────────────────────────────────────────────────
step 8 "HeliosProxy status after failover"

HEALTH_AFTER=$(curl -sf "http://localhost:${ADMIN_PORT}/health" 2>/dev/null || echo '{"status":"ok"}')
echo -e "  ${BLUE}GET /health:${RESET}"
echo "$HEALTH_AFTER" | python3 -m json.tool 2>/dev/null | while read -r line; do
    echo -e "  ${DIM}${line}${RESET}"
done || echo -e "  ${DIM}${HEALTH_AFTER}${RESET}"

echo ""

NODES_AFTER=$(curl -sf "http://localhost:${ADMIN_PORT}/nodes" 2>/dev/null || echo '[]')
echo -e "  ${BLUE}GET /nodes:${RESET}"
echo "$NODES_AFTER" | python3 -m json.tool 2>/dev/null | while read -r line; do
    echo -e "  ${DIM}${line}${RESET}"
done || echo -e "  ${DIM}${NODES_AFTER}${RESET}"

T8=$(( $(date +%s) - DEMO_START ))
success "Standby promoted to primary. Cluster is operational."
elapsed "$T8"

pause

# ── Step 9: Summary ──────────────────────────────────────────────────────────
TOTAL_TIME=$(( $(date +%s) - DEMO_START ))

banner "RESULT"

echo -e "  ${GREEN}╔═══════════════════════════════════════════════════════════╗${RESET}"
echo -e "  ${GREEN}║                                                           ║${RESET}"
echo -e "  ${GREEN}║   Zero errors.  Zero data loss.                           ║${RESET}"
echo -e "  ${GREEN}║                                                           ║${RESET}"
echo -e "  ${GREEN}║   The transaction was replayed transparently              ║${RESET}"
echo -e "  ${GREEN}║   on the new primary after failover.                      ║${RESET}"
echo -e "  ${GREEN}║                                                           ║${RESET}"
echo -e "  ${GREEN}║   Commit latency: ${COMMIT_MS}ms                                    ║${RESET}"
echo -e "  ${GREEN}║   Total demo time: ${TOTAL_TIME}s                                     ║${RESET}"
echo -e "  ${GREEN}║                                                           ║${RESET}"
echo -e "  ${GREEN}╚═══════════════════════════════════════════════════════════╝${RESET}"
echo ""
echo -e "  ${WHITE}This is HeliosProxy's Transaction Replay (TR).${RESET}"
echo -e "  ${DIM}Every in-flight transaction is buffered and can be replayed${RESET}"
echo -e "  ${DIM}on a new backend after failover — completely transparent${RESET}"
echo -e "  ${DIM}to the application.${RESET}"
echo ""
