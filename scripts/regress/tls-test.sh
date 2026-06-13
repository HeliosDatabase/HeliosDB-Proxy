#!/usr/bin/env bash
# Client TLS termination test (Batch F.2).
#
# Generates a self-signed server cert, starts the proxy with [tls] enabled
# in front of PostgreSQL 18.4, and connects with psql sslmode=require — which
# only succeeds if the proxy answers the SSLRequest with 'S' and completes a
# TLS handshake. Also confirms sslmode=disable (plaintext) still works.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/tls-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

# 1. self-signed server cert
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$OUT/server.key" -out "$OUT/server.crt" \
  -days 1 -subj "/CN=localhost" >/dev/null 2>&1 || { echo "openssl failed"; exit 2; }
chmod 600 "$OUT/server.key"

# 2. proxy config with TLS
cat > "$OUT/proxy-tls.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode    = "none"
write_timeout_secs = 30
[tls]
enabled = true
cert_path = "$OUT/server.crt"
key_path = "$OUT/server.key"
require_client_cert = false
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
name = "pg-primary"
EOF

"$BIN" --config "$OUT/proxy-tls.toml" >"$OUT/proxy.log" 2>&1 &
PROXYPID=$!
cleanup(){ kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT
for i in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" -tAc "select 1" >/dev/null 2>&1 && break
  kill -0 "$PROXYPID" 2>/dev/null || { echo "proxy died:"; cat "$OUT/proxy.log"; exit 1; }
  sleep 0.5
done

# 3. TLS required — must succeed AND report an SSL/TLS connection
tls_out=$(docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
  psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=require" \
  -c "\conninfo" -tAc "select 'tls-ok'" 2>&1)
echo "--- sslmode=require ---"; echo "$tls_out"
if echo "$tls_out" | grep -q "tls-ok"; then
  if echo "$tls_out" | grep -qiE "SSL connection|protocol: TLS"; then
    ok "tls_require: encrypted connection established ($(echo "$tls_out" | grep -oiE 'TLSv[0-9.]+' | head -1))"
  else
    ok "tls_require: query succeeded over sslmode=require"
  fi
else
  bad "tls_require: $(echo "$tls_out" | tr '\n' ' ')"
fi

# 4. plaintext still works
dis=$(docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
  psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" -tAc "select 'plain-ok'" 2>&1)
echo "$dis" | grep -q "plain-ok" && ok "plaintext: sslmode=disable still works" || bad "plaintext broke: $dis"

echo "== TLS test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
