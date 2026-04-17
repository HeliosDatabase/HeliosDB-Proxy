#!/usr/bin/env bash
# =============================================================================
# HeliosProxy — Live Terminal Dashboard
# =============================================================================
#
# Refreshes every 2 seconds showing proxy health, node status, pool metrics.
# Press Ctrl+C to stop.
#
# Usage:
#   ./dashboard.sh
# =============================================================================

set -uo pipefail

ADMIN_HOST=localhost
ADMIN_PORT=39090

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[1;31m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
CYAN='\033[1;36m'
MAGENTA='\033[1;35m'
WHITE='\033[1;37m'
DIM='\033[0;37m'
RESET='\033[0m'

# ── Cleanup ──────────────────────────────────────────────────────────────────
cleanup() {
    tput cnorm 2>/dev/null  # Show cursor
    echo ""
    echo -e "${DIM}Dashboard stopped.${RESET}"
}
trap cleanup EXIT

# Hide cursor
tput civis 2>/dev/null

START_TIME=$(date +%s)

while true; do
    # Clear screen
    tput clear 2>/dev/null || clear

    ELAPSED=$(( $(date +%s) - START_TIME ))

    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${MAGENTA}  HELIOSPROXY CHAOS DASHBOARD        $(date +%H:%M:%S)   uptime: ${ELAPSED}s${RESET}"
    echo -e "${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo ""

    # ── Proxy Health ─────────────────────────────────────────────────────
    echo -e "${WHITE}  PROXY HEALTH${RESET}"
    echo -e "${DIM}  ────────────────────────────────────────────────────────────${RESET}"

    HEALTH=$(curl -sf --max-time 2 "http://${ADMIN_HOST}:${ADMIN_PORT}/health" 2>/dev/null || echo "UNREACHABLE")
    if echo "$HEALTH" | grep -qi '"healthy"\|"ok"\|"up"' 2>/dev/null; then
        echo -e "  Status: ${GREEN}HEALTHY${RESET}"
    elif [ "$HEALTH" = "UNREACHABLE" ]; then
        echo -e "  Status: ${RED}UNREACHABLE${RESET}"
    else
        echo -e "  Status: ${YELLOW}DEGRADED${RESET}"
    fi

    if [ "$HEALTH" != "UNREACHABLE" ]; then
        echo "$HEALTH" | python3 -m json.tool 2>/dev/null | while read -r line; do
            echo -e "  ${DIM}${line}${RESET}"
        done
    fi
    echo ""

    # ── Node Status ──────────────────────────────────────────────────────
    echo -e "${WHITE}  NODE STATUS${RESET}"
    echo -e "${DIM}  ────────────────────────────────────────────────────────────${RESET}"

    NODES=$(curl -sf --max-time 2 "http://${ADMIN_HOST}:${ADMIN_PORT}/nodes" 2>/dev/null || echo "UNREACHABLE")
    if [ "$NODES" = "UNREACHABLE" ]; then
        echo -e "  ${RED}Cannot reach admin API${RESET}"
    else
        echo "$NODES" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if isinstance(data, list):
        for node in data:
            name = node.get('name', 'unknown')
            role = node.get('role', '?')
            health = node.get('health', node.get('status', '?'))
            if health in ('healthy', 'up', 'online'):
                color = '\033[1;32m'
            elif health in ('unhealthy', 'down', 'offline'):
                color = '\033[1;31m'
            else:
                color = '\033[1;33m'
            print(f'  {color}{name:20s}  role={role:10s}  status={health}\033[0m')
    else:
        print(f'  \033[0;37m{json.dumps(data, indent=2)}\033[0m')
except:
    print(f'  \033[0;37m{sys.stdin.read()}\033[0m')
" 2>/dev/null || echo "$NODES" | while read -r line; do echo -e "  ${DIM}${line}${RESET}"; done
    fi
    echo ""

    # ── Docker Container Status ──────────────────────────────────────────
    echo -e "${WHITE}  CONTAINER STATUS${RESET}"
    echo -e "${DIM}  ────────────────────────────────────────────────────────────${RESET}"

    for container in chaos-primary chaos-standby-sync chaos-standby-async chaos-proxy; do
        state=$(docker inspect --format='{{.State.Status}}' "$container" 2>/dev/null || echo "not found")
        health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "n/a")

        if [ "$state" = "running" ] && [ "$health" = "healthy" ]; then
            color="${GREEN}"
            status="RUNNING (healthy)"
        elif [ "$state" = "running" ]; then
            color="${YELLOW}"
            status="RUNNING (${health})"
        else
            color="${RED}"
            status="${state}"
        fi
        printf "  ${color}%-25s %s${RESET}\n" "$container" "$status"
    done
    echo ""

    # ── Pool Metrics ─────────────────────────────────────────────────────
    echo -e "${WHITE}  POOL METRICS${RESET}"
    echo -e "${DIM}  ────────────────────────────────────────────────────────────${RESET}"

    POOL=$(curl -sf --max-time 2 "http://${ADMIN_HOST}:${ADMIN_PORT}/pool" 2>/dev/null || \
           curl -sf --max-time 2 "http://${ADMIN_HOST}:${ADMIN_PORT}/metrics" 2>/dev/null || \
           echo "UNREACHABLE")
    if [ "$POOL" = "UNREACHABLE" ]; then
        echo -e "  ${DIM}No pool data available${RESET}"
    else
        echo "$POOL" | python3 -m json.tool 2>/dev/null | head -20 | while read -r line; do
            echo -e "  ${DIM}${line}${RESET}"
        done || echo -e "  ${DIM}$(echo "$POOL" | head -20)${RESET}"
    fi
    echo ""

    echo -e "${DIM}  Refreshing every 2s... Press Ctrl+C to stop.${RESET}"

    sleep 2
done
