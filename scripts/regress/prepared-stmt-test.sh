#!/usr/bin/env bash
# Prepared-statement tracking test (Batch F.4).
#
# Drives the *real* extended protocol — named Parse / Bind / Describe / Close —
# through the proxy using psql's \parse, \bind_named, \close_prepared
# meta-commands. This exercises the per-session statement registry + per-
# connection prepared-set bookkeeping that F.4 added on the hot path: if that
# bookkeeping corrupted the wire stream, these exchanges would desync and fail.
#
# (The transparent re-prepare-after-switch path itself is unit-tested in
# server.rs against an in-memory duplex; a live trigger needs a mid-session
# backend failover, which this stock single-node harness does not stage.)
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/prep-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

cat > "$OUT/proxy.toml" <<'EOF'
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
port = 25433
role = "primary"
weight = 100
enabled = true
name = "pg"
EOF

"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 &
P=$!
trap 'kill "$P" 2>/dev/null; wait "$P" 2>/dev/null' EXIT
for i in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
    psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "select 1" >/dev/null 2>&1 && break
  sleep 0.3
done

run(){ docker run --rm -i --network host -e PGPASSWORD=benchpass "$IMG" \
  psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -v ON_ERROR_STOP=1 2>&1; }

# 1. Single named prepared statement: Parse -> Bind/Execute (twice) -> Close.
out=$(run <<'SQL'
SELECT 100 AS v \parse ps1
\bind_named ps1 \g
\bind_named ps1 \g
\close_prepared ps1
SQL
)
n=$(echo "$out" | grep -c '^ *100$')
[ "$n" = "2" ] && ok "named Parse/Bind/Execute x2 + Close ($n rows)" || bad "single-stmt: got $n hundreds; out=[$out]"

# 2. Two interleaved named statements with a Describe, mixed re-bind order,
#    then close one and keep re-using the other. Stresses the registry +
#    per-batch defines/refs/closes tracking.
out=$(run <<'SQL'
SELECT 11 AS a \parse pa
SELECT 22 AS b \parse pb
\bind_named pb \g
\bind_named pa \g
\bind_named pb \g
\close_prepared pa
\bind_named pb \g
\close_prepared pb
SQL
)
a=$(echo "$out" | grep -c '^ *11$')
b=$(echo "$out" | grep -c '^ *22$')
[ "$a" = "1" ] && [ "$b" = "3" ] && ok "interleaved pa/pb (pa=$a pb=$b)" || bad "interleaved: pa=$a pb=$b out=[$out]"

# 3. Parameterised named statement through the proxy (Bind carries params).
out=$(run <<'SQL'
SELECT $1::int + $2::int AS s \parse padd
\bind_named padd 40 2 \g
\close_prepared padd
SQL
)
echo "$out" | grep -qE '^ *42$' && ok "parameterised named statement (=42)" || bad "param: out=[$out]"

# 4. Re-using a closed name must re-Parse cleanly (registry forgot the old one).
out=$(run <<'SQL'
SELECT 7 AS v \parse px
\bind_named px \g
\close_prepared px
SELECT 8 AS v \parse px
\bind_named px \g
\close_prepared px
SQL
)
echo "$out" | grep -qE '^ *7$' && echo "$out" | grep -qE '^ *8$' && ok "name reuse after Close (7 then 8)" || bad "reuse: out=[$out]"

echo "== prepared-stmt test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
