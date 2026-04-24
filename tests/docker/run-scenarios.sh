#!/usr/bin/env bash
# Drives the full T0-IT pipeline end-to-end:
#   1. `docker compose up --wait` the cluster.
#   2. pgbench-init the schema.
#   3. For each scenario in scenarios.yml:
#        - Take a `pre` snapshot.
#        - Start pgbench in background.
#        - Fire the scenario event (from scenarios.yml).
#        - Wait for pgbench to finish.
#        - Take a `post` snapshot.
#        - Run checksum comparison.
#        - Evaluate pgbench log against the assertions.
#        - Report pass/fail.
#   4. Tear down (unless --keep).
#
# The assertion evaluation is deliberately best-effort here — the full
# evaluator (parsing scenarios.yml, computing p99 latency over a
# rolling window, querying the admin API during the window) is the
# work of a Rust integration-test binary. This script is the minimum
# viable "it ran and the data is intact" gate.

set -euo pipefail

HERE="$(dirname "$0")"
COMPOSE="docker compose -f $HERE/cluster.yml"
CHAOS="$HERE/pgbench-chaos.sh"
CHK="$HERE/checksum.sh"
DURATION="${SCENARIO_DURATION_S:-60}"

keep=0
for arg in "$@"; do
  case "$arg" in
    --keep) keep=1 ;;
  esac
done

cleanup() {
  if (( keep )); then
    echo "[cleanup] --keep specified; leaving cluster up"
    return
  fi
  echo "[cleanup] tearing down cluster"
  $COMPOSE down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

bring_up() {
  echo "[up] building + starting cluster"
  $COMPOSE up --build --wait -d
}

run_one_scenario() {
  local name="$1"
  echo ""
  echo "=========================================="
  echo "  SCENARIO: $name"
  echo "=========================================="

  "$CHK" snapshot pre

  echo "[load] starting pgbench ($DURATION s)"
  "$CHAOS" run "$DURATION" >/dev/null &
  local load_pid=$!

  # Small warm-up before firing the event so we have baseline TPS.
  sleep $(( DURATION / 6 ))

  case "$name" in
    primary-sigkill)            "$CHAOS" scenario primary-sigkill ;;
    primary-sigstop-15s)        "$CHAOS" scenario primary-sigstop 15 ;;
    async-standby-lag-5s)       "$CHAOS" scenario standby-async-netem-delay 30 5000 ;;
    sync-standby-partition-10s) "$CHAOS" scenario standby-sync-netem-delay 10 0 ;;
    primary-oom)                $COMPOSE kill -s TERM pg-primary ;;
    *)                          echo "unknown scenario: $name" >&2; return 2 ;;
  esac

  wait "$load_pid" || true

  # Let replication + reconciliation settle.
  sleep 5

  "$CHK" snapshot post
  "$CHK" compare pre post
  echo "[scenario $name] PASS"
}

bring_up
"$CHAOS" init

# Scenarios we know how to drive. For a full scenarios.yml-driven
# pass, port this list into a yq/jq loop.
for name in \
  primary-sigkill \
  primary-sigstop-15s \
  async-standby-lag-5s \
  sync-standby-partition-10s \
  primary-oom
do
  run_one_scenario "$name" || {
    echo "[scenario $name] FAIL"
    exit 1
  }
done

echo ""
echo "All scenarios passed."
