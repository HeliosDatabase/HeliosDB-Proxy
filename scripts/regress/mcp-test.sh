#!/usr/bin/env bash
# MCP agent gateway test (Batch G — AI-data-plane differentiator).
#
# Starts the proxy with the MCP gateway enabled in front of a backend, then
# drives the JSON-RPC surface with curl: initialize -> tools/list -> tools/call
# (query / list_tables / explain), and verifies the read-only guardrail.
#   BK=pg|nano  ./mcp-test.sh <proxy-binary>
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
BK="${BK:-pg}"
OUT="${OUT:-/tmp/mcp-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

case "$BK" in
  pg)   BHOST=127.0.0.1;  BPORT=25433; BUSER=bench;    BPASS=benchpass;                       BDB=benchdb ;;
  nano) BHOST=127.0.0.1;  BPORT=55337; BUSER=postgres; BPASS=trust;                           BDB=postgres ;;
  *) echo "unknown BK=$BK"; exit 2 ;;
esac

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[mcp]
enabled = true
listen_address = "127.0.0.1:9092"
backend_host = "$BHOST"
backend_port = $BPORT
backend_user = "$BUSER"
backend_password = "$BPASS"
backend_database = "$BDB"
read_only = true
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
for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9092" -d '{}' && break; sleep 0.3; done

M="http://127.0.0.1:9092"
rpc(){ curl -s "$M" -H 'Content-Type: application/json' -d "$1"; }

# 1. initialize
init=$(rpc '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}')
echo "$init" | grep -q '"name":"heliosproxy-mcp"' && ok "initialize: serverInfo returned" || bad "initialize: $init"

# 2. tools/list
tl=$(rpc '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}')
echo "$tl" | grep -q '"query"' && echo "$tl" | grep -q '"list_tables"' && echo "$tl" | grep -q '"explain"' \
  && ok "tools/list: query + list_tables + explain" || bad "tools/list: $tl"

# 3. tools/call query SELECT
q=$(rpc '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 42 AS answer"}}}')
echo "$q" | grep -q '42' && echo "$q" | grep -q '"isError":false' && ok "tools/call query: returned 42" || bad "tools/call query: $q"

# 4. tools/call list_tables
lt=$(rpc '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_tables","arguments":{}}}')
echo "$lt" | grep -q '"isError":false' && ok "tools/call list_tables: ok" || bad "tools/call list_tables: $lt"

# 5. read-only guardrail blocks a write
w=$(rpc '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"query","arguments":{"sql":"DELETE FROM nothing"}}}')
echo "$w" | grep -q '"isError":true' && echo "$w" | grep -qi 'read-only' && ok "read_only_guardrail: write refused" || bad "read_only_guardrail: $w"

# 6. notification gets no body (202)
nb=$(curl -s -o /dev/null -w '%{http_code}' "$M" -d '{"jsonrpc":"2.0","method":"notifications/initialized"}')
[ "$nb" = "202" ] && ok "notification: 202 no body" || bad "notification: got $nb"

echo "== MCP test (BK=$BK): PASS=$PASS FAIL=$FAIL =="
exit $FAIL
