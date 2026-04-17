#!/usr/bin/env bash
# Bank Ledger Demo — Concurrent Transfer Workers
# Usage: ./workers.sh [num_workers] [duration_secs]
set -euo pipefail

NUM_WORKERS="${1:-20}"
DURATION="${2:-120}"
PROXY_HOST="${PROXY_HOST:-localhost}"
PROXY_PORT="${PROXY_PORT:-46432}"
CONNSTR="postgresql://app:apppass@${PROXY_HOST}:${PROXY_PORT}/bankdb"

RESULTS_DIR="$(mktemp -d)"
trap 'rm -rf "$RESULTS_DIR"' EXIT

echo "=== Bank Ledger Workers ==="
echo "Workers:  $NUM_WORKERS"
echo "Duration: ${DURATION}s"
echo "Proxy:    ${PROXY_HOST}:${PROXY_PORT}"
echo ""

worker() {
  local id=$1
  local ok=0
  local err=0
  local end_time=$((SECONDS + DURATION))

  while [ $SECONDS -lt $end_time ]; do
    local src=$((RANDOM % 100 + 1))
    local dst=$((RANDOM % 100 + 1))
    while [ "$dst" -eq "$src" ]; do
      dst=$((RANDOM % 100 + 1))
    done
    local amount=$((RANDOM % 500 + 1))

    if psql "$CONNSTR" -q -v ON_ERROR_STOP=1 <<SQL 2>/dev/null
BEGIN;
UPDATE accounts SET balance = balance - ${amount}.00 WHERE id = ${src};
UPDATE accounts SET balance = balance + ${amount}.00 WHERE id = ${dst};
INSERT INTO transfers (from_acct, to_acct, amount) VALUES (${src}, ${dst}, ${amount}.00);
COMMIT;
SQL
    then
      ok=$((ok + 1))
    else
      err=$((err + 1))
      sleep 0.1
    fi
  done

  echo "${ok} ${err}" > "$RESULTS_DIR/worker_${id}"
}

echo "Starting $NUM_WORKERS workers for ${DURATION}s..."
for i in $(seq 1 "$NUM_WORKERS"); do
  worker "$i" &
done

wait

echo ""
echo "=== Per-Worker Results ==="
printf "%-10s %-15s %-10s\n" "Worker" "Transfers" "Errors"
printf "%-10s %-15s %-10s\n" "------" "---------" "------"

total_ok=0
total_err=0
for i in $(seq 1 "$NUM_WORKERS"); do
  if [ -f "$RESULTS_DIR/worker_${i}" ]; then
    read -r ok err < "$RESULTS_DIR/worker_${i}"
    printf "%-10s %-15s %-10s\n" "$i" "$ok" "$err"
    total_ok=$((total_ok + ok))
    total_err=$((total_err + err))
  fi
done

echo ""
echo "=== Totals ==="
echo "Successful transfers: $total_ok"
echo "Errors encountered:   $total_err"
echo ""
echo "Run ./audit.sh to verify the $1,000,000 invariant."
