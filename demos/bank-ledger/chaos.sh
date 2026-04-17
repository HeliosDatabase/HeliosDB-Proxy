#!/usr/bin/env bash
# Bank Ledger Demo — Chaos: kill primary repeatedly
# Usage: ./chaos.sh [num_cycles]
set -euo pipefail

NUM_CYCLES="${1:-5}"
CONTAINER="bl-primary"

echo "=== Chaos Agent ==="
echo "Will kill primary $NUM_CYCLES times during the test."
echo ""

for i in $(seq 1 "$NUM_CYCLES"); do
  delay=$((RANDOM % 21 + 10))
  echo "[Cycle $i/$NUM_CYCLES] Sleeping ${delay}s before kill..."
  sleep "$delay"

  echo "[Cycle $i/$NUM_CYCLES] Killing $CONTAINER..."
  docker kill "$CONTAINER"

  echo "[Cycle $i/$NUM_CYCLES] Primary down. Waiting 5s..."
  sleep 5

  echo "[Cycle $i/$NUM_CYCLES] Restarting $CONTAINER..."
  docker start "$CONTAINER"

  echo "[Cycle $i/$NUM_CYCLES] Waiting for primary to become healthy..."
  until docker inspect --format='{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null | grep -q healthy; do
    sleep 2
  done
  echo "[Cycle $i/$NUM_CYCLES] Primary healthy again."
  echo ""
done

echo "=== Chaos Complete ==="
echo "Killed and restarted primary $NUM_CYCLES times."
