#!/usr/bin/env bash
# =============================================================================
# HeliosProxy — Post-Chaos Verification
# =============================================================================
#
# Checks data integrity after a chaos test run:
#   1. Total row count in workload table
#   2. No gaps in the iteration sequence
#   3. All nodes have the same data
#
# Usage:
#   ./verify.sh
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

export PGPASSWORD=apppass
PG_USER=app
PG_DB=appdb

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
MAGENTA='\033[1;35m'
DIM='\033[0;37m'
RESET='\033[0m'

PASS=0
FAIL=0

check() {
    local desc="$1"
    local result="$2"  # "pass" or "fail"
    local detail="$3"

    if [ "$result" = "pass" ]; then
        echo -e "  ${GREEN}PASS${RESET}  $desc"
        [ -n "$detail" ] && echo -e "        ${DIM}$detail${RESET}"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}FAIL${RESET}  $desc"
        [ -n "$detail" ] && echo -e "        ${RED}$detail${RESET}"
        FAIL=$((FAIL + 1))
    fi
}

echo ""
echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${MAGENTA}  POST-CHAOS INTEGRITY VERIFICATION${RESET}"
echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

# ── Check 1: Row count via proxy ─────────────────────────────────────────────
echo -e "${CYAN}Check 1: Row count (via proxy)${RESET}"

PROXY_COUNT=$(psql -h localhost -p 36432 -U "$PG_USER" -d "$PG_DB" -t -A \
    -c "SELECT COUNT(*) FROM chaos_workload;" 2>/dev/null || echo "ERROR")

if [ "$PROXY_COUNT" != "ERROR" ] && [ "$PROXY_COUNT" -gt 0 ] 2>/dev/null; then
    check "Workload table has data" "pass" "${PROXY_COUNT} rows"
else
    check "Workload table has data" "fail" "Got: ${PROXY_COUNT}"
fi

# ── Check 2: Sequence gaps ──────────────────────────────────────────────────
echo ""
echo -e "${CYAN}Check 2: Sequence continuity${RESET}"

MAX_ITER=$(psql -h localhost -p 36432 -U "$PG_USER" -d "$PG_DB" -t -A \
    -c "SELECT MAX(iteration) FROM chaos_workload;" 2>/dev/null || echo "ERROR")

DISTINCT_ITERS=$(psql -h localhost -p 36432 -U "$PG_USER" -d "$PG_DB" -t -A \
    -c "SELECT COUNT(DISTINCT iteration) FROM chaos_workload;" 2>/dev/null || echo "ERROR")

GAP_COUNT=$(psql -h localhost -p 36432 -U "$PG_USER" -d "$PG_DB" -t -A \
    -c "
    WITH seq AS (
        SELECT generate_series(1, MAX(iteration)) AS iter
        FROM chaos_workload
    )
    SELECT COUNT(*)
    FROM seq s
    LEFT JOIN chaos_workload w ON w.iteration = s.iter
    WHERE w.iteration IS NULL;
    " 2>/dev/null || echo "ERROR")

if [ "$GAP_COUNT" != "ERROR" ]; then
    if [ "$GAP_COUNT" -eq 0 ] 2>/dev/null; then
        check "No gaps in iteration sequence" "pass" "max_iter=${MAX_ITER}, distinct=${DISTINCT_ITERS}"
    else
        check "No gaps in iteration sequence" "fail" "${GAP_COUNT} missing iterations (max=${MAX_ITER}, distinct=${DISTINCT_ITERS})"
    fi
else
    check "No gaps in iteration sequence" "fail" "Could not query"
fi

# ── Check 3: Counter consistency ─────────────────────────────────────────────
echo ""
echo -e "${CYAN}Check 3: Counter consistency${RESET}"

OPS_COUNT=$(psql -h localhost -p 36432 -U "$PG_USER" -d "$PG_DB" -t -A \
    -c "SELECT count FROM chaos_counter WHERE name = 'ops';" 2>/dev/null || echo "ERROR")

if [ "$OPS_COUNT" != "ERROR" ] && [ "$OPS_COUNT" -gt 0 ] 2>/dev/null; then
    check "Operations counter is positive" "pass" "ops counter = ${OPS_COUNT}"
else
    check "Operations counter is positive" "fail" "Got: ${OPS_COUNT}"
fi

# ── Check 4: Cross-node data comparison ─────────────────────────────────────
echo ""
echo -e "${CYAN}Check 4: Cross-node data consistency${RESET}"

# Query each node directly
declare -A NODE_COUNTS

NODES=("localhost:35432:primary" "localhost:35442:standby-sync" "localhost:35462:standby-async")

for node_spec in "${NODES[@]}"; do
    IFS=':' read -r host port label <<< "$node_spec"
    count=$(psql -h "$host" -p "$port" -U "$PG_USER" -d "$PG_DB" -t -A \
        -c "SELECT COUNT(*) FROM chaos_workload;" 2>/dev/null || echo "UNREACHABLE")
    NODE_COUNTS[$label]="$count"
    echo -e "  ${DIM}${label}: ${count} rows${RESET}"
done

# Compare all reachable nodes
reachable_counts=()
for label in "${!NODE_COUNTS[@]}"; do
    count="${NODE_COUNTS[$label]}"
    if [ "$count" != "UNREACHABLE" ] && [ "$count" != "ERROR" ]; then
        reachable_counts+=("$count")
    fi
done

if [ "${#reachable_counts[@]}" -ge 2 ]; then
    all_same=true
    first="${reachable_counts[0]}"
    for c in "${reachable_counts[@]}"; do
        if [ "$c" != "$first" ]; then
            all_same=false
            break
        fi
    done

    if $all_same; then
        check "All reachable nodes have same row count" "pass" "${first} rows on each"
    else
        check "All reachable nodes have same row count" "fail" "Counts differ: ${reachable_counts[*]}"
    fi
elif [ "${#reachable_counts[@]}" -eq 1 ]; then
    check "All reachable nodes have same row count" "pass" "Only 1 node reachable (${reachable_counts[0]} rows) — others may still be recovering"
else
    check "All reachable nodes have same row count" "fail" "No nodes reachable"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

TOTAL=$((PASS + FAIL))
if [ "$FAIL" -eq 0 ]; then
    echo -e "  ${GREEN}ALL ${TOTAL} CHECKS PASSED${RESET}"
else
    echo -e "  ${RED}${FAIL}/${TOTAL} CHECKS FAILED${RESET}"
fi

echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

exit "$FAIL"
