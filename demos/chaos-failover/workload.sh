#!/usr/bin/env bash
# =============================================================================
# HeliosProxy — Chaos Failover Workload Generator
# =============================================================================
#
# Continuous INSERT/UPDATE/SELECT workload through the proxy.
# Tracks success/failure rates and prints a summary on exit.
#
# Usage:
#   ./workload.sh              # Run until Ctrl+C
#   ./workload.sh 300          # Run for 300 seconds
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Configuration ────────────────────────────────────────────────────────────
PROXY_HOST=localhost
PROXY_PORT=36432
export PGPASSWORD=apppass
PG_USER=app
PG_DB=appdb

DURATION="${1:-0}"  # 0 = run forever

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
DIM='\033[0;37m'
RESET='\033[0m'

# ── Counters ─────────────────────────────────────────────────────────────────
TOTAL=0
SUCCESS=0
FAILED=0
ITERATION=0
START_TIME=$(date +%s)

# ── Cleanup / Summary ────────────────────────────────────────────────────────
print_summary() {
    local end_time=$(date +%s)
    local elapsed=$(( end_time - START_TIME ))
    local rate=0
    if [ "$TOTAL" -gt 0 ]; then
        rate=$(awk "BEGIN { printf \"%.2f\", ($SUCCESS / $TOTAL) * 100 }")
    fi
    local ops_per_sec=0
    if [ "$elapsed" -gt 0 ]; then
        ops_per_sec=$(awk "BEGIN { printf \"%.1f\", $TOTAL / $elapsed }")
    fi

    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${CYAN}  WORKLOAD SUMMARY${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "  Duration:     ${elapsed}s"
    echo -e "  Iterations:   ${ITERATION}"
    echo -e "  Total ops:    ${TOTAL}"
    echo -e "  ${GREEN}Successful:   ${SUCCESS}${RESET}"
    echo -e "  ${RED}Failed:       ${FAILED}${RESET}"
    echo -e "  Success rate: ${rate}%"
    echo -e "  Throughput:   ${ops_per_sec} ops/s"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
}

trap print_summary EXIT

# ── Create tables ────────────────────────────────────────────────────────────
echo -e "${CYAN}Initialising workload tables...${RESET}"

for attempt in $(seq 1 30); do
    if psql -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$PG_USER" -d "$PG_DB" -c "
        CREATE TABLE IF NOT EXISTS chaos_workload (
            id SERIAL PRIMARY KEY,
            iteration INT NOT NULL,
            op TEXT NOT NULL,
            value TEXT NOT NULL,
            ts TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS chaos_counter (
            name TEXT PRIMARY KEY,
            count INT NOT NULL DEFAULT 0
        );
        INSERT INTO chaos_counter (name, count) VALUES ('ops', 0)
        ON CONFLICT (name) DO NOTHING;
    " >/dev/null 2>&1; then
        echo -e "${GREEN}Tables ready.${RESET}"
        break
    fi
    echo -e "${DIM}Waiting for proxy... (attempt $attempt)${RESET}"
    sleep 2
done

echo -e "${CYAN}Starting workload...${RESET}"
echo ""

# ── Main loop ────────────────────────────────────────────────────────────────
run_op() {
    local sql="$1"
    local desc="$2"
    TOTAL=$((TOTAL + 1))
    if psql -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$PG_USER" -d "$PG_DB" \
         -t -A -c "$sql" >/dev/null 2>&1; then
        SUCCESS=$((SUCCESS + 1))
        return 0
    else
        FAILED=$((FAILED + 1))
        echo -e "  ${RED}FAIL: ${desc}${RESET}"
        return 1
    fi
}

while true; do
    ITERATION=$((ITERATION + 1))
    ITER_START=$(date +%s%N)

    # Check duration limit
    if [ "$DURATION" -gt 0 ]; then
        elapsed=$(( $(date +%s) - START_TIME ))
        if [ "$elapsed" -ge "$DURATION" ]; then
            echo -e "${YELLOW}Duration limit reached (${DURATION}s).${RESET}"
            break
        fi
    fi

    # INSERT
    run_op "INSERT INTO chaos_workload (iteration, op, value) VALUES ($ITERATION, 'insert', 'iter-$ITERATION');" \
        "INSERT iter $ITERATION"

    # UPDATE counter
    run_op "UPDATE chaos_counter SET count = count + 1 WHERE name = 'ops';" \
        "UPDATE counter iter $ITERATION"

    # SELECT verification
    run_op "SELECT COUNT(*) FROM chaos_workload WHERE iteration <= $ITERATION;" \
        "SELECT count iter $ITERATION"

    ITER_END=$(date +%s%N)
    ITER_MS=$(( (ITER_END - ITER_START) / 1000000 ))

    # Status line
    local_rate=0
    if [ "$TOTAL" -gt 0 ]; then
        local_rate=$(awk "BEGIN { printf \"%.1f\", ($SUCCESS / $TOTAL) * 100 }")
    fi

    echo -e "${DIM}[$(date +%H:%M:%S)]${RESET} iter=${CYAN}${ITERATION}${RESET}  ops=${TOTAL}  ${GREEN}ok=${SUCCESS}${RESET}  ${RED}fail=${FAILED}${RESET}  rate=${local_rate}%  ${DIM}${ITER_MS}ms${RESET}"

    # Small delay to avoid hammering
    sleep 0.5
done
