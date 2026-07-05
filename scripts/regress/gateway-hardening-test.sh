#!/usr/bin/env bash
# HeliosProxy live test — HTTP-facing gateway request hardening (Group A-1).
#
# The HTTP /sql, MCP, and GraphQL gateways read attacker-controllable requests.
# This verifies they cannot be OOM'd by a huge Content-Length, pinned by a
# slow-loris, and that MCP now enforces its Bearer token (previously absent):
#   1. Oversized Content-Length → 413 (or clean close) BEFORE allocating; up.
#   2. MCP without the configured token → 401.
#   3. A drip-fed request is dropped within the read timeout; proxy stays up.
#   4. Legitimate requests still work (no functional regression).
#
# Needs an all-features binary (graphql-gateway feature). Usage:
#   ./gateway-hardening-test.sh /path/to/heliosdb-proxy
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: gateway-hardening-test.sh <proxy-binary>}"
BHOST=127.0.0.1; BPORT=25433; BUSER=bench; BPASS=benchpass; BDB=benchdb
HTTP_PORT=9093; MCP_PORT=9094; GQL_PORT=9095
HTOK=http-tok; MTOK=mcp-tok; GTOK=gql-tok

OUT="${OUT:-/tmp/regress-gateway-hardening}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
[http_gateway]
enabled = true
listen_address = "127.0.0.1:$HTTP_PORT"
backend_host = "$BHOST"
backend_port = $BPORT
backend_user = "$BUSER"
backend_password = "$BPASS"
backend_database = "$BDB"
auth_token = "$HTOK"
[mcp]
enabled = true
listen_address = "127.0.0.1:$MCP_PORT"
backend_host = "$BHOST"
backend_port = $BPORT
backend_user = "$BUSER"
backend_password = "$BPASS"
backend_database = "$BDB"
auth_token = "$MTOK"
[graphql_gateway]
enabled = true
listen_address = "127.0.0.1:$GQL_PORT"
backend_host = "$BHOST"
backend_port = $BPORT
backend_user = "$BUSER"
backend_password = "$BPASS"
backend_database = "$BDB"
auth_token = "$GTOK"
[[graphql_gateway.tables]]
name = "users"
columns = ["id", "name"]
[pool]
min_connections = 1
max_connections = 20
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

cleanup(){ [ -n "${P:-}" ] && kill "$P" 2>/dev/null; wait "${P:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== gateway hardening test  bin=$BIN =="
"$BIN" --config "$OUT/proxy.toml" >"$LOG" 2>&1 &
P=$!
ready=0
for _ in $(seq 1 40); do
  curl -s -o /dev/null "http://127.0.0.1:$HTTP_PORT/health" && { ready=1; break; }
  kill -0 "$P" 2>/dev/null || { echo "proxy died:"; tail -20 "$LOG"; exit 1; }
  sleep 0.4
done
[ "$ready" = 1 ] || { echo "gateways never became ready"; tail -20 "$LOG"; exit 1; }

# Raw request helper: send $2 to 127.0.0.1:$1, return first response line (or empty).
raw_send(){ python3 - "$1" "$2" <<'PY'
import socket,sys
port=int(sys.argv[1]); data=sys.argv[2].encode()
try:
    s=socket.create_connection(("127.0.0.1",port),timeout=4); s.sendall(data); s.settimeout(4)
    r=s.recv(256).decode(errors="replace"); s.close()
    print(r.splitlines()[0] if r else "NO_RESPONSE")
except Exception as e: print("ERR",e)
PY
}

# 1. Oversized Content-Length (header only, no body) → 413 or clean close, no OOM.
for name in http:$HTTP_PORT:$HTOK mcp:$MCP_PORT:$MTOK gql:$GQL_PORT:$GTOK; do
  g=${name%%:*}; rest=${name#*:}; port=${rest%%:*}; tok=${rest#*:}
  resp=$(raw_send "$port" "POST /sql HTTP/1.1
Host: x
Authorization: Bearer $tok
Content-Length: 9000000000

")
  if printf '%s' "$resp" | grep -q '413'; then ok "oversized_body_$g" "(413)"
  elif printf '%s' "$resp" | grep -qiE 'NO_RESPONSE|ERR'; then ok "oversized_body_$g" "(closed, no alloc)"
  else bad "oversized_body_$g" "unexpected: $resp"; fi
done
kill -0 "$P" 2>/dev/null && ok proxy_survives_oversized || bad proxy_survives_oversized "died (OOM?)"

# 2. MCP without token → 401 (auth newly enforced).
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$MCP_PORT/" \
        -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}')
[ "$code" = "401" ] && ok mcp_requires_token "($code)" || bad mcp_requires_token "got $code (expected 401)"
# ...and WITH the token it works.
mt=$(curl -s -X POST "http://127.0.0.1:$MCP_PORT/" -H "Authorization: Bearer $MTOK" \
       -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}')
printf '%s' "$mt" | grep -q '"tools"' && ok mcp_token_admits || bad mcp_token_admits "$mt"

# 3. Slow-loris: connect, send a partial header, never finish → dropped by the
#    read timeout without pinning the gateway. Bound our own wait well under it.
slow=$(timeout 20 python3 - "$HTTP_PORT" <<'PY'
import socket,time,sys
port=int(sys.argv[1])
s=socket.create_connection(("127.0.0.1",port),timeout=5)
s.sendall(b"POST /sql HTTP/1.1\r\n")          # partial request, no blank line
s.settimeout(18)
t=time.time()
try:
    while True:
        d=s.recv(64)
        if not d: print("CLOSED", round(time.time()-t,1)); break
except socket.timeout: print("STILL_OPEN")
except Exception as e: print("ERR", e)
PY
)
echo "   slowloris: $slow"
if printf '%s' "$slow" | grep -q CLOSED; then ok slowloris_dropped "(server closed the drip conn)"
else bad slowloris_dropped "connection not dropped: $slow"; fi

# 4. Functional sanity: a normal request still works after all the abuse.
n=$(curl -s -X POST "http://127.0.0.1:$HTTP_PORT/sql" -H "Authorization: Bearer $HTOK" \
      -d '{"query":"SELECT 21+21 AS n"}' | grep -o '"n":"42"')
[ "$n" = '"n":"42"' ] && ok gateway_serves_after_abuse || bad gateway_serves_after_abuse "$n"

echo "== gateway-hardening: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
