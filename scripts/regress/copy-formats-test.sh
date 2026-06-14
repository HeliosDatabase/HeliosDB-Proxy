#!/usr/bin/env bash
# COPY text+CSV, both directions, THROUGH the proxy (Batch G2 validation).
#
# Exercises the proxy's COPY sub-protocol relay against a backend that supports
# COPY FROM STDIN + COPY TO STDOUT in FORMAT text and FORMAT csv. The relay is
# format/direction-agnostic (it forwards CopyInResponse/CopyOutResponse +
# CopyData/CopyDone verbatim and ends at the single trailing RFQ); this proves
# round-trips stay byte-exact end to end.
#
#   BACKEND_PORT=54399 ./copy-formats-test.sh <proxy-binary>   # default Nano test port
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
BPORT="${BACKEND_PORT:-54399}"
BUSER="${BACKEND_USER:-postgres}"
BDB="${BACKEND_DB:-postgres}"
BPASS="${BACKEND_PASS:-}"
OUT="${OUT:-/tmp/copy-formats}"; rm -rf "$OUT"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[pool]
min_connections = 2
max_connections = 50
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 5
test_on_acquire = true
[load_balancer]
read_strategy = "round_robin"
read_write_split = false
latency_threshold_ms = 100
[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"
[[nodes]]
host = "127.0.0.1"
port = $BPORT
role = "primary"
weight = 100
enabled = true
name = "backend"
EOF

"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 &
P=$!
trap 'kill "$P" 2>/dev/null; wait "$P" 2>/dev/null' EXIT
C="host=127.0.0.1 port=6432 user=$BUSER dbname=$BDB sslmode=disable"
psqlp(){ docker run --rm -i --network host -e PGPASSWORD="$BPASS" "$IMG" psql "$C" "$@" 2>&1; }
for i in $(seq 1 30); do psqlp -tAc "select 1" >/dev/null 2>&1 && break; sleep 0.3; done

# ---- 1. text FROM STDIN ----
psqlp -c "DROP TABLE IF EXISTS ct; CREATE TABLE ct(id int, name text);" >/dev/null
printf '1\talice\n2\tbob\n3\tcarol\n\\.\n' | psqlp -c "COPY ct FROM STDIN" >/dev/null 2>&1
cnt=$(psqlp -tAc "SELECT count(*) FROM ct" | tr -d '[:space:]')
sum=$(psqlp -tAc "SELECT sum(id) FROM ct" | tr -d '[:space:]')
[ "$cnt" = "3" ] && [ "$sum" = "6" ] && ok "text FROM STDIN (3 rows, sum 6)" || bad "text FROM: cnt=$cnt sum=$sum"

# ---- 2. text TO STDOUT round-trip (table form; order-independent compare) ----
exp=$(printf '1\talice\n2\tbob\n3\tcarol\n' | LC_ALL=C sort | md5sum | cut -d' ' -f1)
got=$(psqlp -c "COPY ct TO STDOUT" 2>/dev/null | LC_ALL=C sort | md5sum | cut -d' ' -f1)
[ "$got" = "$exp" ] && ok "text TO STDOUT (round-trip checksum matches)" || bad "text TO: $got vs $exp"

# ---- 3. CSV FROM STDIN (quoting + NULL convention) ----
psqlp -c "DROP TABLE IF EXISTS cc; CREATE TABLE cc(id int, name text, note text);" >/dev/null
# Row 2: quoted field with embedded comma. Row 3: doubled quotes -> say "hi".
# Row 4: unquoted empty name -> NULL. Row 5: quoted empty note -> '' (not NULL).
printf '1,alice,hello\n2,"bob, jr",world\n3,carol,"say ""hi"""\n4,,n4\n5,dave,""\n' \
  | psqlp -c "COPY cc FROM STDIN WITH (FORMAT csv)" >/dev/null 2>&1
cnt=$(psqlp -tAc "SELECT count(*) FROM cc" | tr -d '[:space:]')
comma=$(psqlp -tAc "SELECT name FROM cc WHERE id=2")
dq=$(psqlp -tAc "SELECT note FROM cc WHERE id=3")
nulls=$(psqlp -tAc "SELECT count(*) FROM cc WHERE name IS NULL" | tr -d '[:space:]')
emptys=$(psqlp -tAc "SELECT count(*) FROM cc WHERE note = ''" | tr -d '[:space:]')
echo "    csv parsed: cnt=$cnt comma=[$comma] dq=[$dq] nulls=$nulls emptys=$emptys"
[ "$cnt" = "5" ] && ok "CSV FROM STDIN (5 rows ingested)" || bad "CSV FROM: cnt=$cnt"
[ "$comma" = "bob, jr" ] && ok "CSV: quoted embedded-comma field intact" || bad "CSV comma: [$comma]"
[ "$dq" = 'say "hi"' ] && ok "CSV: doubled-quote unescaped to one quote" || bad "CSV dq: [$dq]"
{ [ "$nulls" = "1" ] && [ "$emptys" = "1" ]; } && ok "CSV: NULL (unquoted empty) vs '' (quoted empty)" || bad "CSV null/empty: nulls=$nulls emptys=$emptys"

# ---- 4. CSV TO STDOUT relay transparency (proxy bytes == direct-backend bytes) ----
# The proxy relay is byte-transparent; assert that what it streams for a COPY
# TO STDOUT is identical to what the backend emits directly. (The CSV round-trip
# *semantics* depend on the backend's NULL/'' rendering, not on the relay.)
psqlp -c "COPY cc TO STDOUT WITH (FORMAT csv)" > "$OUT/proxy.csv" 2>/dev/null
docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
  psql "host=127.0.0.1 port=$BPORT user=$BUSER dbname=$BDB sslmode=disable" \
  -c "COPY cc TO STDOUT WITH (FORMAT csv)" > "$OUT/direct.csv" 2>/dev/null
if cmp -s "$OUT/proxy.csv" "$OUT/direct.csv" && [ -s "$OUT/proxy.csv" ]; then
  ok "CSV TO STDOUT (relay byte-transparent: proxy == direct backend)"
else bad "CSV TO: proxy/direct bytes differ"; fi

echo "== copy-formats test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
