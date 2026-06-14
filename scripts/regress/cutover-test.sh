#!/usr/bin/env bash
# Migration cutover test (Batch G2).
#
# With the mirror caught up, POST /api/migration/cutover transparently
# redirects NEW client connections from the PostgreSQL 18.4 primary to the
# promoted HeliosDB-Nano 3.37 target. Verified via SELECT version(): the client
# keeps its PG credentials but the proxy substitutes the target's, so the same
# psql command lands on a different engine after cutover.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/cutover-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[mirror]
enabled = true
writes_only = true
backend_host = "127.0.0.1"
backend_port = 55337
backend_user = "postgres"
backend_password = "trust"
backend_database = "postgres"
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

"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 &
P=$!
cleanup(){ kill "$P" 2>/dev/null; wait "$P" 2>/dev/null; }
trap cleanup EXIT
for i in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "select 1" >/dev/null 2>&1 && break
  sleep 0.5
done

# Client always uses its PG creds; the proxy decides where it lands.
ver(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "SELECT version()" 2>&1; }

# 1. Before cutover -> PostgreSQL.
v1=$(ver); echo "before: $v1"
echo "$v1" | grep -q "PostgreSQL 18" && ! echo "$v1" | grep -qi "Nano" && ok "before: traffic on PostgreSQL 18.x" || bad "before: $v1"

# 2. Cutover (migration_ready since no backlog).
co=$(curl -s -X POST "http://127.0.0.1:9099/api/migration/cutover")
echo "cutover: $co"
echo "$co" | grep -q '"ok":true' && ok "cutover: promoted target accepted" || bad "cutover: $co"

# 3. After cutover -> Nano (new connection, same client creds).
v2=$(ver); echo "after: $v2"
echo "$v2" | grep -qi "Nano" && ok "after: NEW connection lands on HeliosDB-Nano" || bad "after: $v2"

# 4. status reflects cutover_active.
st=$(curl -s "http://127.0.0.1:9099/api/migration/status")
echo "$st" | grep -q '"cutover_active":true' && ok "status: cutover_active=true" || bad "status: $st"

# 5. Rollback -> back to PostgreSQL.
curl -s -X POST "http://127.0.0.1:9099/api/migration/cutover/rollback" >/dev/null
v3=$(ver); echo "rollback: $v3"
echo "$v3" | grep -q "PostgreSQL 18" && ! echo "$v3" | grep -qi "Nano" && ok "rollback: traffic back on PostgreSQL" || bad "rollback: $v3"

echo "== cutover test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
