#!/usr/bin/env bash
# Query-cancellation forwarding test (Batch F).
#
# Runs a long query through the proxy, then SIGINTs the client so it issues
# a PostgreSQL CancelRequest on a fresh connection. If the proxy forwards
# the cancel to the right backend, the long query aborts immediately with
# "canceling statement due to user request"; if not, it runs to completion.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/cancel-test}"; mkdir -p "$OUT"

"$BIN" --config "$HERE/proxy-pg.toml" >"$OUT/proxy.log" 2>&1 &
PROXYPID=$!
cleanup(){ kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT

# wait for readiness
for i in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "select 1" >/dev/null 2>&1 && break
  sleep 0.5
done

# Launch a 20s query through the proxy; docker run forwards SIGINT to psql.
docker run --rm --network host --name cancel_probe -e PGPASSWORD=benchpass "$IMG" \
  psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -c "SELECT pg_sleep(20)" >"$OUT/psql.out" 2>&1 &
DPID=$!

sleep 3   # let the query reach the backend (active in pg_stat_activity)
start=$(date +%s)
# Ask docker to forward SIGINT to the client (psql cancels on SIGINT).
docker kill --signal=INT cancel_probe >/dev/null 2>&1

# Wait up to 10s for the client to exit.
for i in $(seq 1 100); do kill -0 $DPID 2>/dev/null || break; sleep 0.1; done
kill -0 $DPID 2>/dev/null && { kill $DPID 2>/dev/null; docker kill cancel_probe 2>/dev/null; }
wait $DPID 2>/dev/null
elapsed=$(( $(date +%s) - start ))

echo "--- psql output ---"; cat "$OUT/psql.out"
echo "--- elapsed after cancel: ${elapsed}s ---"
if grep -qi "canceling statement due to user request" "$OUT/psql.out"; then
  echo -e "\033[32mPASS\033[0m cancel_forwarding: query aborted via forwarded CancelRequest (${elapsed}s)"
  exit 0
else
  echo -e "\033[31mFAIL\033[0m cancel_forwarding: no cancel — query was not aborted (elapsed ${elapsed}s)"
  exit 1
fi
