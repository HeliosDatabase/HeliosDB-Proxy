#!/usr/bin/env bash
# =============================================================================
# HeliosProxy — Chaos Monkey
# =============================================================================
#
# Randomly kills and restarts PostgreSQL nodes to test HeliosProxy's
# failover and recovery capabilities.
#
# Usage:
#   ./chaos.sh           # Run for 300s (default)
#   ./chaos.sh 600       # Run for 600s
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DURATION="${1:-300}"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
MAGENTA='\033[1;35m'
DIM='\033[0;37m'
RESET='\033[0m'

# ── Node definitions (container names) ───────────────────────────────────────
NODES=("chaos-primary" "chaos-standby-sync" "chaos-standby-async")
NODE_LABELS=("pg-primary" "pg-standby-sync" "pg-standby-async")

# Weighted selection: 50% primary, 25% each standby
WEIGHTS=(50 25 25)

# ── Counters ─────────────────────────────────────────────────────────────────
declare -A KILL_COUNT
for node in "${NODES[@]}"; do
    KILL_COUNT[$node]=0
done
TOTAL_KILLS=0

START_TIME=$(date +%s)

# ── Summary on exit ──────────────────────────────────────────────────────────
print_summary() {
    local elapsed=$(( $(date +%s) - START_TIME ))
    echo ""
    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${MAGENTA}  CHAOS MONKEY SUMMARY${RESET}"
    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "  Duration:     ${elapsed}s"
    echo -e "  Total kills:  ${TOTAL_KILLS}"
    echo ""
    for i in "${!NODES[@]}"; do
        local node="${NODES[$i]}"
        local label="${NODE_LABELS[$i]}"
        echo -e "  ${label}: ${KILL_COUNT[$node]} kills"
    done
    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
}

trap print_summary EXIT

# ── Weighted random node selection ───────────────────────────────────────────
pick_node() {
    local roll=$(( RANDOM % 100 ))
    local cumulative=0
    for i in "${!NODES[@]}"; do
        cumulative=$(( cumulative + WEIGHTS[i] ))
        if [ "$roll" -lt "$cumulative" ]; then
            echo "$i"
            return
        fi
    done
    echo "0"
}

# ── Random sleep (min, max) ──────────────────────────────────────────────────
random_sleep() {
    local min=$1
    local max=$2
    local duration=$(( RANDOM % (max - min + 1) + min ))
    sleep "$duration"
}

# =============================================================================
# MAIN LOOP
# =============================================================================

echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${MAGENTA}  CHAOS MONKEY — ${DURATION}s of destruction${RESET}"
echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

while true; do
    elapsed=$(( $(date +%s) - START_TIME ))
    if [ "$elapsed" -ge "$DURATION" ]; then
        echo -e "${YELLOW}Duration limit reached (${DURATION}s). Stopping chaos.${RESET}"
        break
    fi

    remaining=$(( DURATION - elapsed ))
    echo -e "${DIM}[$(date +%H:%M:%S)] ${remaining}s remaining${RESET}"

    # Pick a node to kill
    idx=$(pick_node)
    node="${NODES[$idx]}"
    label="${NODE_LABELS[$idx]}"

    # Kill it
    echo -e "${RED}[$(date +%H:%M:%S)] KILLING ${label} (${node})${RESET}"
    docker kill "$node" 2>/dev/null || echo -e "${YELLOW}  (node already down)${RESET}"
    KILL_COUNT[$node]=$(( ${KILL_COUNT[$node]} + 1 ))
    TOTAL_KILLS=$((TOTAL_KILLS + 1))

    # Wait 10-20s with the node down
    down_time=$(( RANDOM % 11 + 10 ))
    echo -e "${DIM}  Node down for ${down_time}s...${RESET}"
    sleep "$down_time"

    # Check if we've exceeded duration during the sleep
    elapsed=$(( $(date +%s) - START_TIME ))
    if [ "$elapsed" -ge "$DURATION" ]; then
        # Restart the node before exiting
        echo -e "${GREEN}[$(date +%H:%M:%S)] RESTARTING ${label} (cleanup)${RESET}"
        docker start "$node" 2>/dev/null || true
        break
    fi

    # Restart it
    echo -e "${GREEN}[$(date +%H:%M:%S)] RESTARTING ${label} (${node})${RESET}"
    docker start "$node" 2>/dev/null || echo -e "${YELLOW}  (restart failed)${RESET}"

    # Wait 15-30s for recovery before next kill
    recovery_time=$(( RANDOM % 16 + 15 ))
    echo -e "${DIM}  Recovery period: ${recovery_time}s...${RESET}"
    sleep "$recovery_time"
done
