#!/usr/bin/env bash
# COPY sub-protocol passthrough test (Batch B capability; serves Batch G2
# migration mirror + the Nano P3 cross-team validation).
#
# Drives COPY FROM STDIN (ingest) and COPY TO STDOUT (export) THROUGH the
# proxy and checks the round-trip. Works against any COPY-supporting backend:
#   BK=pg   ./copy-test.sh <proxy-binary>     # PostgreSQL 18.4
#   BK=nano ./copy-test.sh <proxy-binary>     # HeliosDB-Nano (once it lands COPY)
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: copy-test.sh <proxy-binary>}"
BK="${BK:-pg}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/copy-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

case "$BK" in
  pg)   CFG="$HERE/proxy-pg.toml";   BUSER=bench;    BPASS=benchpass;                       BDB=benchdb ;;
  nano) CFG="$HERE/proxy-nano.toml"; BUSER=postgres; BPASS=OTPZ7Mxh9FJEeeKF3qqSKmW64lmT2u3; BDB=postgres ;;
  *) echo "unknown BK=$BK"; exit 2 ;;
esac

"$BIN" --config "$CFG" >"$OUT/proxy.log" 2>&1 &
PROXYPID=$!
cleanup(){ kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT
for i in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6432 -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1 && break
  kill -0 "$PROXYPID" 2>/dev/null || { echo "proxy died"; tail -5 "$OUT/proxy.log"; exit 1; }
  sleep 0.5
done

N=1000
out=$(docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" bash -c "
  C='host=127.0.0.1 port=6432 user=$BUSER dbname=$BDB sslmode=disable'
  psql \"\$C\" -c 'DROP TABLE IF EXISTS _copyt; CREATE TABLE _copyt(id int, name text);' >/dev/null 2>&1
  # COPY FROM STDIN
  seq 1 $N | awk '{print \$1\"\tname\"\$1}' | psql \"\$C\" -c 'COPY _copyt FROM STDIN' 2>&1 | tail -1
  # row count
  echo COUNT=\$(psql \"\$C\" -tAc 'SELECT count(*) FROM _copyt' 2>&1)
  # COPY TO STDOUT round-trip checksum
  echo TOHASH=\$(psql \"\$C\" -c 'COPY (SELECT * FROM _copyt ORDER BY id) TO STDOUT' 2>/dev/null | md5sum | cut -d' ' -f1)
  echo EXPHASH=\$(seq 1 $N | awk '{print \$1\"\tname\"\$1}' | md5sum | cut -d' ' -f1)
")
echo "$out"
echo "$out" | grep -q "COPY $N"           && ok "copy_from_stdin: $N rows ingested via proxy" || bad "copy_from_stdin"
[ "$(echo "$out" | grep -oP 'COUNT=\K[0-9]+')" = "$N" ] && ok "copy_rowcount: $N" || bad "copy_rowcount"
th=$(echo "$out" | grep -oP 'TOHASH=\K\S+'); eh=$(echo "$out" | grep -oP 'EXPHASH=\K\S+')
[ -n "$th" ] && [ "$th" = "$eh" ] && ok "copy_to_stdout: round-trip checksum matches" || bad "copy_to_stdout: $th vs $eh"

echo "== COPY test (BK=$BK): PASS=$PASS FAIL=$FAIL =="
exit $FAIL
