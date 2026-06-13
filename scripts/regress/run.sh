#!/usr/bin/env bash
# HeliosProxy live regression battery.
#
# Starts a given proxy binary in front of a backend (PostgreSQL 18.4 or
# HeliosDB-Nano) and runs a battery of wire-protocol correctness +
# performance checks through it, comparing results against the backend
# directly. Exits non-zero on any FAIL.
#
# Usage:
#   BK=pg   ./run.sh /path/to/heliosdb-proxy            # PostgreSQL 18.4
#   BK=nano ./run.sh /path/to/heliosdb-proxy            # HeliosDB-Nano
#
# Client tooling (psql/pgbench 18.4) runs from the local postgres docker
# image over --network host so it can reach proxy + both backends.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: run.sh <proxy-binary>}"
BK="${BK:-pg}"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1
PROXY_PORT=6432
ADMIN_PORT=9099

case "$BK" in
  pg)   CFG="$HERE/proxy-pg.toml";   BHOST=127.0.0.1;  BPORT=25433; BUSER=bench;    BPASS=benchpass;                          BDB=benchdb ;;
  nano) CFG="$HERE/proxy-nano.toml"; BHOST=100.64.0.2; BPORT=54320; BUSER=postgres; BPASS=OTPZ7Mxh9FJEeeKF3qqSKmW64lmT2u3;    BDB=postgres ;;
  *) echo "unknown BK=$BK (use pg|nano)"; exit 2 ;;
esac

OUT="${OUT:-/tmp/regress-$BK}"; mkdir -p "$OUT"
PASS=0; FAIL=0; SKIP=0; RESULTS=()
note(){ printf '  %s\n' "$*"; }
ok(){   PASS=$((PASS+1)); RESULTS+=("PASS $1"); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){  FAIL=$((FAIL+1)); RESULTS+=("FAIL $1"); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }
skip(){ SKIP=$((SKIP+1)); RESULTS+=("SKIP $1"); printf '  \033[33mSKIP\033[0m %s %s\n' "$1" "${2:-}"; }

# client helpers — run psql/pgbench against PROXY (P) or backend direct (D)
pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql   -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql   -h $BHOST      -p $BPORT      -U "$BUSER" -d "$BDB" "$@"; }
bP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" -v "$OUT":/w "$IMG" pgbench -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== regression battery: BK=$BK  bin=$BIN =="
"$BIN" --config "$CFG" >"$OUT/proxy.log" 2>&1 &
PROXYPID=$!
# wait for readiness (proxy serves SELECT 1)
ready=0
for i in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$OUT/proxy.log"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$OUT/proxy.log"; exit 1; }

# 1. simple-protocol correctness vs direct backend
exp=$(pD -tAc "select count(*) from generate_series(1,100)" 2>/dev/null | tr -d '[:space:]')
got=$(pP -tAc "select count(*) from generate_series(1,100)" 2>/dev/null | tr -d '[:space:]')
[ -n "$got" ] && [ "$got" = "$exp" ] && ok simple_select "($got)" || bad simple_select "proxy=$got direct=$exp"

# 2. multi-row result fidelity (ordered checksum through proxy == direct).
#    SKIP when the backend itself can't run the query (direct result empty).
q="select md5(string_agg(g::text, ',' order by g)) from generate_series(1,1000) g"
exp=$(pD -tAc "$q" 2>/dev/null | tr -d '[:space:]')
got=$(pP -tAc "$q" 2>/dev/null | tr -d '[:space:]')
if [ -z "$exp" ]; then skip multirow_fidelity "backend unsupported"
elif [ "$got" = "$exp" ]; then ok multirow_fidelity
else bad multirow_fidelity "proxy=$got direct=$exp"; fi

# 3. data-type round-trip (text/int/float/bool/null/bytea)
q="select 'x'::text, 42::int, 3.5::float8, true, null::int, '\\xdead'::bytea"
exp=$(pD -tAc "$q" 2>/dev/null); got=$(pP -tAc "$q" 2>/dev/null)
[ -n "$got" ] && [ "$got" = "$exp" ] && ok datatype_roundtrip || bad datatype_roundtrip "proxy=[$got] direct=[$exp]"

# 4. transaction (BEGIN/INSERT/ROLLBACK) — temp table, pinned to one conn.
#    psql -c emits command tags too; the scalar count is the only digit-only line.
#    SKIP when the backend can't run temp tables (direct attempt yields nothing).
txq="begin; create temp table _rt(x int); insert into _rt values(1),(2); select count(*) from _rt; rollback;"
dtx=$(pD -tAc "$txq" 2>/dev/null | grep -xE '[0-9]+' | head -1)
tx=$(pP -tAc "$txq" 2>/dev/null | grep -xE '[0-9]+' | head -1)
if [ "$dtx" != "2" ]; then skip txn_temptable "backend unsupported"
elif [ "$tx" = "2" ]; then ok txn_temptable "($tx)"
else bad txn_temptable "got=$tx"; fi

# 5. pgbench simple protocol
printf 'SELECT 1;\n' > "$OUT/s.sql"
if bP -n -M simple -f /w/s.sql -t 200 -c 4 >"$OUT/pgb_simple.txt" 2>&1; then
  tps=$(grep -oE 'tps = [0-9.]+' "$OUT/pgb_simple.txt" | head -1 | awk '{print $3}')
  ok pgbench_simple "tps=$tps"
else bad pgbench_simple "$(tail -2 "$OUT/pgb_simple.txt"|tr '\n' ' ')"; fi

# 6. pgbench EXTENDED protocol (Parse/Bind/Execute) — Batch B gate
printf 'SELECT :x;\n' > "$OUT/e.sql"
if bP -n -M extended -f /w/e.sql -D x=7 -t 200 -c 4 >"$OUT/pgb_ext.txt" 2>&1; then
  tps=$(grep -oE 'tps = [0-9.]+' "$OUT/pgb_ext.txt" | head -1 | awk '{print $3}')
  ok pgbench_extended "tps=$tps"
else bad pgbench_extended "$(tail -2 "$OUT/pgb_ext.txt"|tr '\n' ' ')"; fi

# 7. pgbench PREPARED (named prepared statements over extended) — Batch B/C gate
if bP -n -M prepared -f /w/e.sql -D x=7 -t 200 -c 4 >"$OUT/pgb_prep.txt" 2>&1; then
  tps=$(grep -oE 'tps = [0-9.]+' "$OUT/pgb_prep.txt" | head -1 | awk '{print $3}')
  ok pgbench_prepared "tps=$tps"
else bad pgbench_prepared "$(tail -2 "$OUT/pgb_prep.txt"|tr '\n' ' ')"; fi

# 8. large-result streaming: actually stream ~100MB of rows to the client
#    (1,000,000 rows x 100 bytes). Proxy RSS must stay bounded while relaying.
#    On a store-and-forward proxy the whole result is buffered in RAM, so RSS
#    balloons; a streaming relay keeps it flat. Gate: peak RSS < 250MB.
rss_before=$(awk '/VmRSS/{print $2}' /proc/$PROXYPID/status 2>/dev/null)
( pP -tAc "select repeat('x',100) from generate_series(1,1000000)" 2>/dev/null | wc -l > "$OUT/big.txt" 2>&1 ) &
BIGQ=$!; peak=0
while kill -0 $BIGQ 2>/dev/null; do
  r=$(awk '/VmRSS/{print $2}' /proc/$PROXYPID/status 2>/dev/null); [ -n "$r" ] && [ "$r" -gt "$peak" ] && peak=$r
  sleep 0.02
done
wait $BIGQ; bigres=$(tr -d '[:space:]' < "$OUT/big.txt")
peak_mb=$((peak/1024)); before_mb=$((rss_before/1024))
if [ "$bigres" = "1000000" ]; then
  if [ "$peak_mb" -lt 60 ]; then ok large_stream "rss ${before_mb}->${peak_mb}MB"; else bad large_stream "rss ${before_mb}->${peak_mb}MB (>60: store-and-forward buffers whole result; Batch B streaming fixes this)"; fi
else bad large_stream "wrong row count: $bigres (want 1000000)"; fi

# 9. admin health endpoint
hc=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$ADMIN_PORT/health" 2>/dev/null)
[ "$hc" = "200" ] && ok admin_health || bad admin_health "http=$hc"

echo "== BK=$BK  PASS=$PASS FAIL=$FAIL SKIP=$SKIP =="
printf '%s\n' "${RESULTS[@]}" > "$OUT/summary.txt"
exit $FAIL
