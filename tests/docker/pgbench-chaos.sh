#!/usr/bin/env bash
# pgbench-under-chaos runner for the HeliosProxy integration-test cluster.
#
# Two modes:
#
#   init              — initialise pgbench tables on the primary.
#   run <secs>        — run a `pgbench` client pointed at the proxy for
#                       `secs` seconds while logging per-transaction
#                       outcomes. Output goes to /tmp/pgbench-chaos.log.
#   scenario <name>   — inject a fault. Scenarios:
#                         primary-sigkill
#                         primary-sigstop <secs>
#                         standby-sync-netem-delay <secs> <ms>
#                         standby-async-netem-delay <secs> <ms>
#                         primary-partition <secs>
#   status            — current healthy/unhealthy nodes from proxy admin.
#
# Run from the repository root:
#   tests/docker/pgbench-chaos.sh init
#   tests/docker/pgbench-chaos.sh run 300 &
#   tests/docker/pgbench-chaos.sh scenario primary-sigkill
#   wait

set -euo pipefail

COMPOSE="docker compose -f $(dirname "$0")/cluster.yml"
PROXY_HOST="${PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${PROXY_PORT:-5500}"
ADMIN_URL="${ADMIN_URL:-http://127.0.0.1:9090}"

require_compose() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not in PATH" >&2
    exit 2
  fi
}

init_pgbench() {
  require_compose
  echo "[init] creating pgbench schema on primary via the proxy"
  $COMPOSE exec -T pgbench-chaos \
    pgbench -h "$PROXY_HOST" -p "$PROXY_PORT" -U helios -d appdb \
    --initialize --scale=5 --no-vacuum
}

run_load() {
  local secs="${1:-300}"
  require_compose
  local log="/tmp/pgbench-chaos.log"
  echo "[run] $secs s of pgbench against proxy, log=$log"
  $COMPOSE exec -T pgbench-chaos \
    pgbench -h "$PROXY_HOST" -p "$PROXY_PORT" -U helios -d appdb \
    --time="$secs" --client=16 --jobs=4 --progress=5 \
    --report-per-command --no-vacuum \
    > "$log" 2>&1 &
  echo "$!"
}

status() {
  require_compose
  echo "[status] proxy admin /health/ready:"
  curl -fsS "$ADMIN_URL/health/ready" || true
  echo ""
  echo "[status] proxy admin /nodes:"
  curl -fsS "$ADMIN_URL/nodes" | head -c 4096 || true
  echo ""
}

scenario_primary_sigkill() {
  echo "[chaos] SIGKILL pg-primary"
  $COMPOSE kill -s KILL pg-primary
}

scenario_primary_sigstop() {
  local secs="${1:-15}"
  echo "[chaos] SIGSTOP pg-primary for $secs s"
  $COMPOSE kill -s STOP pg-primary
  sleep "$secs"
  $COMPOSE kill -s CONT pg-primary
}

scenario_netem_delay() {
  local svc="$1"
  local secs="$2"
  local ms="$3"
  echo "[chaos] netem delay $ms ms on $svc for $secs s"
  # Attach tc/netem inside the target container's network namespace.
  $COMPOSE exec -T "$svc" sh -c "tc qdisc add dev eth0 root netem delay ${ms}ms" || {
    echo "tc/netem not available in $svc image — install iproute2 or run host-side" >&2
    return 1
  }
  sleep "$secs"
  $COMPOSE exec -T "$svc" sh -c "tc qdisc del dev eth0 root netem" || true
}

scenario_partition() {
  local svc="$1"
  local secs="${2:-15}"
  echo "[chaos] partition $svc for $secs s (drop 100% egress on eth0)"
  $COMPOSE exec -T "$svc" sh -c "tc qdisc add dev eth0 root netem loss 100%" || {
    echo "tc/netem not available in $svc image" >&2
    return 1
  }
  sleep "$secs"
  $COMPOSE exec -T "$svc" sh -c "tc qdisc del dev eth0 root netem" || true
}

usage() {
  sed -n '2,17p' "$0" >&2
  exit 1
}

case "${1:-}" in
  init)
    init_pgbench
    ;;
  run)
    run_load "${2:-300}"
    ;;
  status)
    status
    ;;
  scenario)
    case "${2:-}" in
      primary-sigkill)
        scenario_primary_sigkill
        ;;
      primary-sigstop)
        scenario_primary_sigstop "${3:-15}"
        ;;
      standby-sync-netem-delay)
        scenario_netem_delay pg-standby-sync "${3:-30}" "${4:-5000}"
        ;;
      standby-async-netem-delay)
        scenario_netem_delay pg-standby-async "${3:-30}" "${4:-5000}"
        ;;
      primary-partition)
        scenario_partition pg-primary "${3:-15}"
        ;;
      *)
        usage
        ;;
    esac
    ;;
  *)
    usage
    ;;
esac
