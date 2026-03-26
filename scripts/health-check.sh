#!/bin/bash
# ============================================================================
# HeliosProxy Health Check Script
# ============================================================================
#
# Kubernetes liveness and readiness probe for HeliosProxy.
#
# Usage:
#   ./health-check.sh              # Default: readiness check on localhost:9090
#   ./health-check.sh liveness     # Liveness check only
#   ./health-check.sh readiness    # Readiness check (has healthy backends?)
#   ./health-check.sh full         # Both checks + node status
#
# Environment variables:
#   PROXY_ADMIN_HOST  Host for admin API  (default: localhost)
#   PROXY_ADMIN_PORT  Port for admin API  (default: 9090)
#   PROBE_TIMEOUT     curl timeout in sec (default: 3)
#
# Exit codes:
#   0 — healthy / ready
#   1 — unhealthy / not ready
#
# Kubernetes pod spec example:
#
#   livenessProbe:
#     exec:
#       command: ["/scripts/health-check.sh", "liveness"]
#     initialDelaySeconds: 15
#     periodSeconds: 10
#     timeoutSeconds: 5
#     failureThreshold: 3
#
#   readinessProbe:
#     exec:
#       command: ["/scripts/health-check.sh", "readiness"]
#     initialDelaySeconds: 5
#     periodSeconds: 5
#     timeoutSeconds: 3
#     failureThreshold: 2
#
# ============================================================================

set -euo pipefail

ADMIN_HOST="${PROXY_ADMIN_HOST:-localhost}"
ADMIN_PORT="${PROXY_ADMIN_PORT:-9090}"
TIMEOUT="${PROBE_TIMEOUT:-3}"
BASE_URL="http://${ADMIN_HOST}:${ADMIN_PORT}"

check_liveness() {
    local response
    local http_code

    http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
        --connect-timeout "$TIMEOUT" \
        --max-time "$TIMEOUT" \
        "${BASE_URL}/health/live" 2>/dev/null) || true

    if [ "$http_code" = "200" ]; then
        return 0
    fi

    # Fall back to the basic /health endpoint
    http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
        --connect-timeout "$TIMEOUT" \
        --max-time "$TIMEOUT" \
        "${BASE_URL}/health" 2>/dev/null) || true

    if [ "$http_code" = "200" ]; then
        return 0
    fi

    return 1
}

check_readiness() {
    local http_code

    http_code=$(curl -sf -o /dev/null -w "%{http_code}" \
        --connect-timeout "$TIMEOUT" \
        --max-time "$TIMEOUT" \
        "${BASE_URL}/health/ready" 2>/dev/null) || true

    if [ "$http_code" = "200" ]; then
        return 0
    fi

    return 1
}

check_full() {
    local exit_code=0

    echo "HeliosProxy Health Check"
    echo "========================"
    echo "Target: ${BASE_URL}"
    echo ""

    # Liveness
    if check_liveness; then
        echo "Liveness:  PASS"
    else
        echo "Liveness:  FAIL"
        exit_code=1
    fi

    # Readiness
    if check_readiness; then
        echo "Readiness: PASS"
    else
        echo "Readiness: FAIL"
        exit_code=1
    fi

    # Node status (informational)
    echo ""
    echo "Backend Nodes:"
    local nodes
    nodes=$(curl -sf --connect-timeout "$TIMEOUT" --max-time "$TIMEOUT" \
        "${BASE_URL}/nodes" 2>/dev/null) || nodes="[]"

    if command -v jq >/dev/null 2>&1; then
        echo "$nodes" | jq -r '.[] | "  \(.name // .address): healthy=\(.healthy) failures=\(.failure_count // 0)"' 2>/dev/null || echo "  (unable to parse)"
    else
        echo "  (install jq for formatted output)"
        echo "  $nodes"
    fi

    # Metrics summary
    echo ""
    echo "Metrics:"
    local metrics
    metrics=$(curl -sf --connect-timeout "$TIMEOUT" --max-time "$TIMEOUT" \
        "${BASE_URL}/metrics" 2>/dev/null) || metrics="{}"

    if command -v jq >/dev/null 2>&1; then
        echo "$metrics" | jq -r '"  connections_active: \(.connections_active // "N/A")\n  queries_processed:  \(.queries_processed // "N/A")\n  failovers:          \(.failovers // "N/A")"' 2>/dev/null || echo "  (unable to parse)"
    fi

    return $exit_code
}

# ── Main ─────────────────────────────────────────────────────────────

case "${1:-readiness}" in
    liveness|live)
        check_liveness
        ;;
    readiness|ready)
        check_readiness
        ;;
    full|status)
        check_full
        ;;
    *)
        echo "Usage: $0 {liveness|readiness|full}" >&2
        exit 1
        ;;
esac
