#!/usr/bin/env bash
# Unnamed-Parse promotion correctness test (Batch H).
#
# psql's \bind drives the UNNAMED extended protocol (Parse "" + Bind + Execute).
# Repeating the same query reuses the unnamed statement, which the proxy promotes
# (skips the redundant Parse to the backend, synthesizes ParseComplete). This
# verifies results stay correct: repeated same query, alternating queries (forces
# re-Parse), and interleaved named (\parse) + unnamed (\bind). Runs with the
# optimization ON (default) and OFF (kill-switch) — results must be identical.
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/unnamed-parse}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

gen_cfg(){ # $1 = optimize_unnamed_parse (true|false)
cat <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
optimize_unnamed_parse = $1
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
}

run(){ docker run --rm -i --network host -e PGPASSWORD=benchpass "$IMG" \
  psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" -v ON_ERROR_STOP=1 2>&1; }

test_round(){ # $1 = mode label
  local mode="$1"
  # 1. Same unnamed query repeated 4x via \bind — each must return its arg.
  out=$(run <<'SQL'
SELECT $1::int AS v \bind 10 \g
SELECT $1::int AS v \bind 20 \g
SELECT $1::int AS v \bind 30 \g
SELECT $1::int AS v \bind 40 \g
SQL
)
  got=$(echo "$out" | grep -oE '^ *[0-9]+$' | tr -d ' ' | paste -sd, -)
  [ "$got" = "10,20,30,40" ] && ok "[$mode] repeated unnamed \\bind returns each arg ($got)" || bad "[$mode] repeated: got=[$got] out=[$out]"

  # 2. Alternating DIFFERENT unnamed SQL (forces re-Parse on SQL change).
  out=$(run <<'SQL'
SELECT $1::int + 1 AS v \bind 5 \g
SELECT $1::int * 2 AS v \bind 5 \g
SELECT $1::int + 1 AS v \bind 5 \g
SELECT $1::int * 2 AS v \bind 5 \g
SQL
)
  got=$(echo "$out" | grep -oE '^ *[0-9]+$' | tr -d ' ' | paste -sd, -)
  [ "$got" = "6,10,6,10" ] && ok "[$mode] alternating unnamed SQL re-Parses correctly ($got)" || bad "[$mode] alternating: got=[$got]"

  # 3. Interleave named (\parse/\bind_named) with unnamed (\bind).
  out=$(run <<'SQL'
SELECT $1::int AS v \parse pn
SELECT $1::int + 100 AS v \bind 7 \g
\bind_named pn 7 \g
SELECT $1::int + 100 AS v \bind 8 \g
\bind_named pn 9 \g
\close_prepared pn
SQL
)
  got=$(echo "$out" | grep -oE '^ *[0-9]+$' | tr -d ' ' | paste -sd, -)
  # unnamed 7+100=107, named 7, unnamed 8+100=108, named 9
  [ "$got" = "107,7,108,9" ] && ok "[$mode] named + unnamed interleaved ($got)" || bad "[$mode] interleaved: got=[$got] out=[$out]"

  # 4. Text/typed round-trip through the unnamed path.
  out=$(run <<'SQL'
SELECT $1::text || '-x' AS v \bind hello \g
SELECT $1::text || '-x' AS v \bind world \g
SQL
)
  echo "$out" | grep -q "hello-x" && echo "$out" | grep -q "world-x" && ok "[$mode] typed unnamed values round-trip" || bad "[$mode] typed: out=[$out]"
}

for mode in on off; do
  [ "$mode" = on ] && gen_cfg true > "$OUT/proxy.toml" || gen_cfg false > "$OUT/proxy.toml"
  "$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy-$mode.log" 2>&1 &
  P=$!
  for i in $(seq 1 30); do run <<<'SELECT 1' >/dev/null 2>&1 && break; sleep 0.3; done
  test_round "$mode"
  kill "$P" 2>/dev/null; wait "$P" 2>/dev/null
done

echo "== unnamed-parse test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
