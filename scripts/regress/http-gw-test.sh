#!/usr/bin/env bash
# HTTP SQL gateway test (Batch G — Neon-serverless-compatible POST /sql).
#   BK=pg|nano  ./http-gw-test.sh <proxy-binary>
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
BK="${BK:-pg}"
OUT="${OUT:-/tmp/http-gw-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

case "$BK" in
  pg)   BHOST=127.0.0.1; BPORT=25433; BUSER=bench;    BPASS=benchpass; BDB=benchdb ;;
  nano) BHOST=127.0.0.1; BPORT=55337; BUSER=postgres; BPASS=trust;     BDB=postgres ;;
  *) echo "unknown BK=$BK"; exit 2 ;;
esac
TOKEN="gw-token-xyz"

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[http_gateway]
enabled = true
listen_address = "127.0.0.1:9093"
backend_host = "$BHOST"
backend_port = $BPORT
backend_user = "$BUSER"
backend_password = "$BPASS"
backend_database = "$BDB"
auth_token = "$TOKEN"
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
host = "$BHOST"
port = $BPORT
role = "primary"
weight = 100
enabled = true
name = "backend"
EOF

"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 &
P=$!
cleanup(){ kill "$P" 2>/dev/null; wait "$P" 2>/dev/null; }
trap cleanup EXIT
for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9093/health" && break; sleep 0.3; done

G="http://127.0.0.1:9093/sql"
AUTH="Authorization: Bearer $TOKEN"

# 1. unauthorized without token
[ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$G" -d '{"query":"SELECT 1"}')" = "401" ] \
  && ok "auth: 401 without token" || bad "auth: not 401"

# 2. object-mode result
r=$(curl -s -H "$AUTH" -X POST "$G" -d '{"query":"SELECT 7 AS n, '\''hi'\'' AS g"}')
echo "$r" | grep -q '"command":"SELECT"' && echo "$r" | grep -q '"n":"7"' && echo "$r" | grep -q '"g":"hi"' \
  && ok "object_mode: rows as objects + command" || bad "object_mode: $r"

# 3. fields metadata + rowCount
echo "$r" | grep -q '"dataTypeID"' && echo "$r" | grep -q '"rowCount":1' && ok "fields+rowCount present" || bad "fields/rowCount: $r"

# 4. array mode
a=$(curl -s -H "$AUTH" -H 'Neon-Array-Mode: true' -X POST "$G" -d '{"query":"SELECT 1, 2, 3"}')
echo "$a" | grep -q '"rowAsArray":true' && echo "$a" | grep -q '\[\["1","2","3"\]\]' && ok "array_mode: rows as arrays" || bad "array_mode: $a"

# 5. parameterized query
p=$(curl -s -H "$AUTH" -X POST "$G" -d '{"query":"SELECT $1::int + $2::int AS sum","params":[40,2]}')
echo "$p" | grep -q '"sum":"42"' && ok "params: \$1 + \$2 = 42" || bad "params: $p"

# 6. SQL error surfaces as JSON error
e=$(curl -s -H "$AUTH" -X POST "$G" -d '{"query":"SELECT * FROM no_such_table_zzz"}')
echo "$e" | grep -q '"error"' && ok "error: SQL error returned as JSON" || bad "error: $e"

echo "== HTTP gateway test (BK=$BK): PASS=$PASS FAIL=$FAIL =="
exit $FAIL
