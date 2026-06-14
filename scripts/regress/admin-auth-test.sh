#!/usr/bin/env bash
# Admin API authentication test (Batch G — closes the audit's unauthenticated
# admin-surface release blocker).
#
# With admin_token set: protected endpoints require Authorization: Bearer
# <token> (401 without, 200 with); liveness probes stay open.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
OUT="${OUT:-/tmp/admin-auth-test}"; mkdir -p "$OUT"
TOKEN="s3cret-admin-token"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
admin_token = "$TOKEN"
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
cleanup(){ kill "$P" 2>/dev/null; wait "$P" 2>/dev/null; }
trap cleanup EXIT
for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9099/health" && break; sleep 0.3; done

code(){ curl -s -o /dev/null -w '%{http_code}' "$@"; }
A="http://127.0.0.1:9099"

# 1. protected endpoint without token -> 401
[ "$(code $A/topology)" = "401" ] && ok "no_token: /topology -> 401" || bad "no_token: got $(code $A/topology)"
# 2. wrong token -> 401
[ "$(code -H "Authorization: Bearer wrong" $A/topology)" = "401" ] && ok "wrong_token: /topology -> 401" || bad "wrong_token"
# 3. correct token -> 200
[ "$(code -H "Authorization: Bearer $TOKEN" $A/topology)" = "200" ] && ok "good_token: /topology -> 200" || bad "good_token: got $(code -H "Authorization: Bearer $TOKEN" $A/topology)"
# 4. liveness stays open (no token) -> 200
[ "$(code $A/health)" = "200" ] && ok "liveness_open: /health -> 200 without token" || bad "liveness_open: got $(code $A/health)"
# 5. SQL console protected too
[ "$(code -X POST -d '{"query":"SELECT 1"}' $A/api/sql)" = "401" ] && ok "sql_console_protected: /api/sql -> 401 without token" || bad "sql_console_protected: got $(code -X POST -d '{}' $A/api/sql)"

echo "== admin-auth test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
