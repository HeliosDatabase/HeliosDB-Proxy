#!/usr/bin/env bash
# HeliosProxy live test — circuit breaker (feature: circuit-breaker).
#
# Through a live proxy with a working primary + a DEAD node, proves:
#   1. repeated failures to the dead node trip its circuit and the proxy starts
#      FAST-FAILING with a clean ErrorResponse ("circuit open", SQLSTATE 08006)
#      instead of dropping the connection;
#   2. the breaker is per-node — the healthy primary keeps serving while the
#      dead node's circuit is open.
#
# Usage:  ./circuit-breaker-test.sh /path/to/heliosdb-proxy   (binary built with
# the `circuit-breaker` + `routing-hints` features).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: circuit-breaker-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-circuit.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-circuit}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== circuit-breaker live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info,helios::routing=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# 1. Fire forced queries at the dead node. The first `failure_threshold` (3)
#    fail at connect (connection dropped); after the circuit opens the proxy
#    fast-fails with a clean "circuit open" ErrorResponse.
tripped=0; trip_at=0
for i in $(seq 1 6); do
  r=$(pP -tAc "/*helios:node=pg-dead*/ select 1" 2>&1)
  if printf '%s' "$r" | grep -qi 'circuit open'; then tripped=1; [ "$trip_at" = 0 ] && trip_at=$i; fi
done
[ "$tripped" = 1 ] && ok circuit_trips_and_fast_fails "(first fast-fail at query #$trip_at)" \
  || bad circuit_trips_and_fast_fails "no 'circuit open' fast-fail seen in 6 forced queries"

# 2. the fast-fail carries SQLSTATE 08006 (verbose error code from the proxy).
v=$(pP -tA -v VERBOSITY=verbose -c "/*helios:node=pg-dead*/ select 1" 2>&1)
printf '%s' "$v" | grep -q '08006' && ok fast_fail_sqlstate_08006 \
  || bad fast_fail_sqlstate_08006 "no 08006 in fast-fail error"

# 3. per-node: the healthy primary keeps serving while pg-dead's circuit is open.
got=$(pP -tAc "select 42" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "42" ] && ok healthy_primary_unaffected "($got)" || bad healthy_primary_unaffected "got=[$got]"

echo "== circuit-breaker: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
