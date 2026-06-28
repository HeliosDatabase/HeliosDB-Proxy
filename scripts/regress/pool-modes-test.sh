#!/usr/bin/env bash
# HeliosProxy live test — Transaction-mode connection pooling (feature:
# pool-modes), through a live proxy in front of PostgreSQL 18.4.
#
# Proves the data-path pooler does real work (previously the pool manager was
# never acquire/release'd on the query path):
#   1. REUSE — after the first query, each subsequent query in the session
#      reuses a parked backend connection ("reused pooled backend connection"
#      appears in the proxy log). Session mode would never emit this.
#   2. RESET — a temp table created by one autocommit statement is gone by the
#      next, because the connection is `DISCARD ALL`-reset when parked. Session
#      mode would keep the temp table.
#   3. CORRECTNESS — ordinary queries return correct results through the pool.
#   4. RETAIN — after the client disconnects, the backend connection is parked
#      (still visible in pg_stat_activity, idle) for reuse rather than dropped.
#
# Usage:  ./pool-modes-test.sh /path/to/heliosdb-proxy   (binary built with the
# default features, which include `pool-modes`).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: pool-modes-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-poolmodes.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb
APP=poolprobe

OUT="${OUT:-/tmp/regress-poolmodes}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

# Through the proxy, with a distinctive application_name so we can count the
# proxy's backend connections in pg_stat_activity.
pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" -e PGAPPNAME="$APP" -v "$OUT":/w "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }
# Direct to the backend (out of band), for pg_stat_activity inspection.
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== pool-modes (transaction) live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info,helios=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# 1+3. One session, six autocommit statements. After the first, every query
# leases a parked connection → the reuse log fires, and the values are correct.
printf 'select %s;\n' 11 22 33 44 55 66 > "$OUT/seq.sql"
out=$(pP -tA -f /w/seq.sql 2>&1)
vals=$(printf '%s\n' "$out" | grep -cE '^(11|22|33|44|55|66)$')
[ "$vals" -eq 6 ] && ok correct_results "(rows=$vals)" || bad correct_results "got $vals/6: $out"

reuses=$(grep -c 'reused pooled backend connection' "$LOG")
[ "$reuses" -ge 3 ] && ok connection_reused "(reuse log x$reuses)" \
  || bad connection_reused "reuse log appeared $reuses times (expected >=3)"

# 2. RESET proof: a temp table from one autocommit statement is discarded
# before the next, because the parked connection is DISCARD ALL-reset.
printf 'create temp table pooltmp(x int);\nselect count(*) from pooltmp;\n' > "$OUT/reset.sql"
rout=$(pP -tA -f /w/reset.sql 2>&1)
printf '%s\n' "$rout" | grep -qiE 'pooltmp.*does not exist|relation .*pooltmp.* does not exist' \
  && ok reset_discards_temp_table "(temp table gone after park/reset)" \
  || bad reset_discards_temp_table "temp table survived (no reset?): $rout"

# 4. RETAIN proof: after the client sessions ended, the proxy still holds a
# parked backend connection (idle) for this identity, rather than closing it.
sleep 1
parked=$(pD -tAc "select count(*) from pg_stat_activity where application_name='$APP'" 2>/dev/null | tr -d '[:space:]')
[ "${parked:-0}" -ge 1 ] 2>/dev/null && ok connection_parked_for_reuse "(idle backend conns=$parked)" \
  || bad connection_parked_for_reuse "no parked backend conn (count=$parked)"

echo "== pool-modes: PASS=$PASS FAIL=$FAIL =="
echo "   (pool log lines:)"; grep 'helios::pool' "$LOG" | sed 's/^/   /' | head -12
[ "$FAIL" -eq 0 ]
