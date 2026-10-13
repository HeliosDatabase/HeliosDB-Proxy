#!/usr/bin/env bash
# Disposable end-to-end demo: Patroni (etcd + 2 members) elects/promotes, HeliosProxy follows.
# Usage: ./run-demo.sh /path/to/heliosdb-proxy [h02]   (binary built with --features all-features,postgres-topology)
#   default: switchover, new session after promotion, stopped-leader failover
#   h02:     leader-lock drill (frozen leader) and control-plane partition drill
# Needs: docker compose, python3 + psycopg2, curl. Creates ONLY resources named hp-patroni-demo-*
# and removes exactly those on exit (no `down -v`, no filters by image).
set -euo pipefail
BIN=${1:?usage: run-demo.sh <heliosdb-proxy binary> [h02]}
MODE=${2:-default}
cd "$(dirname "$0")"
P=hp-patroni-demo
PROXY_PID=
RELAY_PIDS=

cleanup() {
  local rc=$?
  if [ "$rc" != 0 ]; then
    # Keep the members' and etcd's logs of a failed run next to proxy.log.
    for c in etcd pg-a pg-b; do
      docker logs --timestamps ${P}-$c >"demo-$c.log" 2>&1 || true
    done
  fi
  [ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null || true
  for r in $RELAY_PIDS; do kill -CONT "$r" 2>/dev/null; kill "$r" 2>/dev/null || true; done
  rm -f proxy-h02.generated.toml
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

if [ "$MODE" = h02 ]; then
  # The proxy polls Patroni through killable relays (35631/35632 -> 35611/35612), so a
  # drill can cut ONLY the proxy's control path; Patroni and the data path are untouched.
  python3 ../../scripts/regress/tcp-relay.py 35631 127.0.0.1 35611 & RELAY_PIDS="$!"
  python3 ../../scripts/regress/tcp-relay.py 35632 127.0.0.1 35612 & RELAY_PIDS="$RELAY_PIDS $!"
  sed 's#"http://127.0.0.1:35611", "http://127.0.0.1:35612"#"http://127.0.0.1:35631", "http://127.0.0.1:35632"#' \
    proxy.toml >proxy-h02.generated.toml
  grep -q 35631 proxy-h02.generated.toml
  "$BIN" --config proxy-h02.generated.toml >proxy.log 2>&1 & PROXY_PID=$!
  wait_for 30 "curl -s http://127.0.0.1:35690/topology | grep -q authoritative"
  echo "=== H-02 drill 1: leader frozen (docker pause); Patroni moves the lock; the old leader returns ==="
  rc=0
  python3 demo.py lock-drill || rc=1
  wait_for 240 leader_ready
  echo "=== H-02 drill 2: the proxy's Patroni control path is cut for 25 s (relays frozen) ==="
  python3 demo.py control-partition $RELAY_PIDS || rc=1
  exit $rc
fi

"$BIN" --config proxy.toml >proxy.log 2>&1 & PROXY_PID=$!
wait_for 30 "curl -s http://127.0.0.1:35690/topology | grep -q authoritative"

echo "=== scenario 1: planned switchover with an open transaction ==="
python3 demo.py switchover

echo "=== scenario 2: a NEW session opened after the promotion must reach the new leader ==="
wait_for 120 leader_ready
python3 demo.py new-session

echo "=== scenario 3: leader container stopped (unplanned failover) with an open transaction ==="
python3 demo.py failover
