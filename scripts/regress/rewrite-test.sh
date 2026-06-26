#!/usr/bin/env bash
# HeliosProxy live test — SQL query rewriting (feature: query-rewriting).
#
# A rewrite rule maps table `rw_orders` -> `rw_neworders`. The two tables hold
# DIFFERENT values, so a query that asks for rw_orders but returns rw_neworders's
# value proves the SQL was genuinely rewritten on the path (not just routed).
#   - rw_orders.v    = 1   (what the client asks for)
#   - rw_neworders.v = 2   (what the rewrite redirects to)
#
# Usage:  ./rewrite-test.sh /path/to/heliosdb-proxy  (binary built with the
# `query-rewriting` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: rewrite-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-rewrite.toml"
IMG="postgres:18.4-bookworm"
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-rewrite}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

# pP -> through the proxy (:6432); pD -> direct to the backend (:25433).
pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6432  -U "$BUSER" -d "$BDB" "$@"; }
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ pD -tAc "drop table if exists rw_orders; drop table if exists rw_neworders" >/dev/null 2>&1; [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== query-rewriting live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info,helios::rewrite=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# setup: two tables with distinct values, created DIRECTLY on the backend so
# the proxy's rewrite rule (rw_orders -> rw_neworders) doesn't rename the DDL.
pD -tAc "drop table if exists rw_orders; drop table if exists rw_neworders;
         create table rw_orders(v int); insert into rw_orders values (1);
         create table rw_neworders(v int); insert into rw_neworders values (2)" >/dev/null 2>&1

# control: the backend table rw_orders really holds 1.
direct=$(pD -tAc "select v from rw_orders" 2>/dev/null | tr -d '[:space:]')
[ "$direct" = "1" ] && ok control_backend_rw_orders_is_1 "($direct)" \
  || bad control_backend_rw_orders_is_1 "got=$direct"

# 1. through the proxy: a query for rw_orders is rewritten to rw_neworders, so
#    it returns 2 (rw_neworders's value), not 1.
got=$(pP -tAc "select v from rw_orders" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "2" ] && ok rewrite_redirects_table "(asked rw_orders, got rw_neworders=$got)" \
  || bad rewrite_redirects_table "got=$got (want 2 = rewritten to rw_neworders)"

# 2. an unrelated query is untouched.
got2=$(pP -tAc "select v from rw_neworders" 2>/dev/null | tr -d '[:space:]')
[ "$got2" = "2" ] && ok unrelated_query_untouched "($got2)" || bad unrelated_query_untouched "got=$got2"

# 3. the rewrite was logged.
grep -qi 'query rewritten' "$LOG" && ok rewrite_logged || bad rewrite_logged "no 'query rewritten' in log"

echo "== query-rewriting: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
