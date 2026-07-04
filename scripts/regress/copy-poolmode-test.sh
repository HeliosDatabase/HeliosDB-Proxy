#!/usr/bin/env bash
# HeliosProxy live test — COPY FROM STDIN under TRANSACTION pooling mode.
#
# Regression for the pool-release/COPY interaction (Group 2, 2.0.a): in
# transaction/statement mode, `stream_until_ready` yields on CopyInResponse
# ('G') without marking the session in-copy, so the main loop's
# `release_to_pool_if_idle` used to reset+park the connection mid-COPY —
# breaking the copy and hanging the client. This drives a real COPY FROM STDIN
# through a transaction-mode proxy and asserts the rows land and the SAME
# session keeps working afterward.
#
# Usage:  ./copy-poolmode-test.sh /path/to/heliosdb-proxy   (default features)
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: copy-poolmode-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-poolmodes.toml"     # mode = "transaction"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb
N=500

OUT="${OUT:-/tmp/regress-copy-poolmode}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

# psql through the proxy; -v ON_ERROR_STOP so a hang/abort surfaces as failure.
pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" -v "$OUT":/w "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== COPY-FROM-STDIN under transaction pooling  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info,helios=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# Build a single-session SQL script: reset table, COPY N rows FROM STDIN, then
# TWO follow-up queries on the SAME session. If the COPY hangs or the connection
# was reset mid-copy, ON_ERROR_STOP + the 25s client timeout make this fail.
{
  echo 'drop table if exists _copypool;'
  echo 'create table _copypool(id int, name text);'
  echo 'copy _copypool from stdin;'
  seq 1 $N | awk '{print $1"\tname"$1}'
  echo '\.'
  echo 'select count(*) from _copypool;'
  echo 'select sum(id) from _copypool;'
} > "$OUT/copy.sql"

# Bound the whole thing so a genuine hang fails fast instead of blocking CI.
res=$(timeout 25 docker run --rm --network host -e PGPASSWORD="$BPASS" -v "$OUT":/w "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" -v ON_ERROR_STOP=1 -tA -f /w/copy.sql 2>&1)
rc=$?

if [ "$rc" -eq 124 ]; then
  bad copy_no_hang "psql timed out (25s) — COPY FROM STDIN hung under transaction pooling"
else
  ok copy_no_hang "(session completed, rc=$rc)"
fi

cnt=$(printf '%s\n' "$res" | grep -xE '[0-9]+' | head -1)
[ "${cnt:-0}" = "$N" ] && ok copy_row_count "($cnt rows)" || bad copy_row_count "got '$cnt' want $N; out=$res"

expsum=$(( N*(N+1)/2 ))
gotsum=$(printf '%s\n' "$res" | grep -xE '[0-9]+' | sed -n '2p')
[ "${gotsum:-0}" = "$expsum" ] && ok copy_values_intact "(sum=$gotsum)" || bad copy_values_intact "got '$gotsum' want $expsum"

# A follow-up plain query on a fresh session must also work (pool not poisoned).
fu=$(pP -tAc "select 42" 2>&1 | tr -d '[:space:]')
[ "$fu" = "42" ] && ok pool_usable_after_copy "(followup ok)" || bad pool_usable_after_copy "got '$fu'"

echo "== copy-poolmode: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
