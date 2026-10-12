#!/usr/bin/env bash
# Disposable end-to-end demo: Patroni (etcd + 2 members) elects/promotes, HeliosProxy follows.
# Usage: ./run-demo.sh /path/to/heliosdb-proxy      (binary built with --features all-features,postgres-topology)
# Needs: docker compose, python3 + psycopg2, curl. Creates ONLY resources named hp-patroni-demo-*
# and removes exactly those on exit (no `down -v`, no filters by image).
set -euo pipefail
BIN=${1:?usage: run-demo.sh <heliosdb-proxy binary>}
cd "$(dirname "$0")"
P=hp-patroni-demo
PROXY_PID=

cleanup() {
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
  docker rm -f ${P}-etcd ${P}-pg-a ${P}-pg-b >/dev/null 2>&1 || true
  docker volume rm ${P}-pg-a-data ${P}-pg-b-data >/dev/null 2>&1 || true
  docker rmi ${P}:local >/dev/null 2>&1 || true
}
trap cleanup EXIT

leader_ready() {  # leader running + a streaming replica, as Patroni reports it
  local c; c=$(curl -s -m 3 http://127.0.0.1:35611/cluster || curl -s -m 3 http://127.0.0.1:35612/cluster || true)
  grep -q '"streaming"' <<<"$c" && grep -q '"leader"' <<<"$c"
}
wait_for() {
  timeout "$1" bash -c "until $2; do sleep 2; done" || {
    echo "timed out waiting for: $2" >&2
    curl -s -m 3 http://127.0.0.1:35611/cluster >&2 || curl -s -m 3 http://127.0.0.1:35612/cluster >&2 || true
    curl -s -m 3 http://127.0.0.1:35690/topology >&2 || true
    return 1
  }
}
export -f leader_ready

docker compose -p $P up -d --build
wait_for 180 leader_ready
"$BIN" --config proxy.toml >proxy.log 2>&1 & PROXY_PID=$!
wait_for 30 "curl -s http://127.0.0.1:35690/topology | grep -q authoritative"

echo "=== scenario 1: planned switchover with an open transaction ==="
python3 demo.py switchover

echo "=== scenario 2: a NEW session opened after the promotion must reach the new leader ==="
wait_for 120 leader_ready
python3 demo.py new-session

echo "=== scenario 3: leader container stopped (unplanned failover) with an open transaction ==="
python3 demo.py failover
