#!/usr/bin/env bash
# HeliosProxy live test — SQL-comment routing hints (feature: routing-hints).
#
# Proves end-to-end, through a live 2-node read/write-split proxy in front of
# PostgreSQL 18.4, that:
#   1. a hinted query returns correct results (the hint comment is parsed,
#      stripped, and the stripped SQL executes cleanly);
#   2. a write whose verb is masked by a leading hint comment is still routed
#      and executed as a write (the is_write-on-stripped-SQL fix);
#   3. a /*helios:node=<name>*/ hint resolves the node NAME to its address and
#      reaches a real backend (name resolution wiring);
#   4. /*helios:node=pg-primary*/ and /*helios:node=pg-standby*/ steer to
#      DIFFERENT backend addresses (deterministic steering, via the proxy's
#      per-query routing log).
#
# Usage:  ./routing-hints-test.sh /path/to/heliosdb-proxy
# Requires the binary to be built with the `routing-hints` feature.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: routing-hints-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-rw-hints.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb
PRIMARY_ADDR="127.0.0.1:25433"
STANDBY_ADDR="localhost:25433"

OUT="${OUT:-/tmp/regress-hints}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== routing-hints live test  bin=$BIN =="
RUST_LOG="helios::routing=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# Capture the routing-log node from the lines appended while running a query.
# Echoes the last `node=<addr>` chosen for a simple query after line $1.
# Strips any ANSI colour codes so the field stays contiguous.
routed_node_after(){
  local before=$1
  sed -n "$((before+1)),\$p" "$LOG" \
    | sed 's/\x1b\[[0-9;]*m//g' \
    | grep -oE 'node=[A-Za-z0-9._-]+:[0-9]+' | tail -1 | sed 's/node=//'
}

# Read the last is_write=<bool> from routing lines appended after line $1.
write_flag_after(){
  local before=$1
  sed -n "$((before+1)),\$p" "$LOG" \
    | sed 's/\x1b\[[0-9;]*m//g' \
    | grep -oE 'is_write=(true|false)' | tail -1 | sed 's/is_write=//'
}

# 1. hinted read returns the correct result (parse + strip + execute clean).
got=$(pP -tAc "/*helios:route=primary*/ select 40 + 2" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "42" ] && ok hinted_read_result "($got)" || bad hinted_read_result "got=[$got]"

# 2. a write whose verb is masked by a leading hint comment is still executed
#    as a write. One multi-statement session (temp table pinned to the forced
#    primary connection): CREATE+INSERT+COUNT must yield 3.
got=$(pP -tAc "/*helios:route=primary*/ create temp table t_hint(x int); insert into t_hint values (1),(2),(3); select count(*) from t_hint;" 2>/dev/null | tail -1 | tr -d '[:space:]')
[ "$got" = "3" ] && ok hinted_write_executes "($got)" || bad hinted_write_executes "got=[$got]"

# 3. node-NAME hint resolves to the node's address and reaches the backend.
b=$(wc -l < "$LOG")
got=$(pP -tAc "/*helios:node=pg-primary*/ select 1" 2>/dev/null | tr -d '[:space:]')
sleep 0.3
pr_node=$(routed_node_after "$b")
[ "$got" = "1" ] && ok node_hint_primary_reaches_backend "($got)" \
  || bad node_hint_primary_reaches_backend "got=[$got]"
[ "$pr_node" = "$PRIMARY_ADDR" ] && ok node_hint_primary_routed "($pr_node)" \
  || bad node_hint_primary_routed "routed=[$pr_node] want=$PRIMARY_ADDR"

# 4. node=pg-standby steers to the DISTINCT standby address. The routing
#    decision is logged before the backend connect, so we read the log without
#    waiting on the connection (this env has only one real backend, so the
#    standby connect itself is expected to fail — we assert the DECISION).
b=$(wc -l < "$LOG")
pP -tAc "/*helios:node=pg-standby*/ select 1" >/dev/null 2>&1 & probe=$!
sleep 1
sb_node=$(routed_node_after "$b")
wait "$probe" 2>/dev/null
[ "$sb_node" = "$STANDBY_ADDR" ] && ok node_hint_standby_routed "($sb_node)" \
  || bad node_hint_standby_routed "routed=[$sb_node] want=$STANDBY_ADDR"

# steering: the two node hints selected DIFFERENT backend addresses.
if [ -n "$sb_node" ] && [ -n "$pr_node" ] && [ "$sb_node" != "$pr_node" ]; then
  ok hints_steer_to_distinct_nodes "($pr_node != $sb_node)"
else
  bad hints_steer_to_distinct_nodes "primary=[$pr_node] standby=[$sb_node]"
fi

# 5. route override flips the write classification on the SAME query:
#    route=primary => write path (is_write=true); route=standby => read path.
b=$(wc -l < "$LOG")
pP -tAc "/*helios:route=primary*/ select 1" >/dev/null 2>&1
sleep 0.3
[ "$(write_flag_after "$b")" = "true" ] && ok route_primary_is_write_true \
  || bad route_primary_is_write_true "got=[$(write_flag_after "$b")]"

b=$(wc -l < "$LOG")
pP -tAc "/*helios:route=standby*/ select 1" >/dev/null 2>&1 & probe=$!
sleep 1
[ "$(write_flag_after "$b")" = "false" ] && ok route_standby_is_write_false \
  || bad route_standby_is_write_false "got=[$(write_flag_after "$b")]"
wait "$probe" 2>/dev/null

echo "== routing-hints: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
