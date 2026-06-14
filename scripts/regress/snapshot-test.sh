#!/usr/bin/env bash
# Migration snapshot-bootstrap test (Batch G2).
#
# Pre-existing data on a PostgreSQL 18.4 primary is copied to a HeliosDB-Nano
# 3.37 secondary via POST /api/migration/snapshot — seeding the migration with
# existing rows (the half the continuous write-tail never saw).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/snapshot-test}"; mkdir -p "$OUT"
NANO_PW='OTPZ7Mxh9FJEeeKF3qqSKmW64lmT2u3'
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

pg(){   docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 25433 -U bench -d benchdb -tAc "$1" 2>/dev/null; }
nano(){ docker run --rm --network host -e PGPASSWORD="$NANO_PW" "$IMG" psql -h 127.0.0.1 -p 55337 -U postgres -d postgres -tAc "$1" 2>/dev/null; }

# 1. Seed EXISTING data on the PG primary directly (never seen by the tail).
pg "DROP TABLE IF EXISTS _snaptest" >/dev/null
pg "CREATE TABLE _snaptest(id int, name text, active boolean)" >/dev/null
for i in $(seq 1 8); do pg "INSERT INTO _snaptest VALUES ($i, 'user$i', $([ $((i%2)) -eq 0 ] && echo true || echo false))" >/dev/null; done
# clean slate on Nano
nano "DROP TABLE IF EXISTS _snaptest" >/dev/null 2>&1

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
source_host = "127.0.0.1"
source_port = 25433
source_user = "bench"
source_password = "benchpass"
source_database = "benchdb"
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
for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9099/health" && break; sleep 0.3; done

# 2. Trigger snapshot bootstrap.
resp=$(curl -s -X POST "http://127.0.0.1:9099/api/migration/snapshot" -d '{"tables":["_snaptest"]}')
echo "--- snapshot response ---"; echo "$resp"
echo "$resp" | grep -q '"ok":true' && echo "$resp" | grep -q '"rows_copied":8' && ok "snapshot: response ok, 8 rows copied" || bad "snapshot resp: $resp"

# 3. Verify Nano now holds the bootstrapped table + rows.
nc=$(nano "SELECT count(*) FROM _snaptest" | tr -d '[:space:]')
[ "$nc" = "8" ] && ok "bootstrap: Nano has 8 rows after snapshot" || bad "Nano count: '$nc'"
# spot-check a value round-tripped
v=$(nano "SELECT name FROM _snaptest WHERE id = 3" | tr -d '[:space:]')
[ "$v" = "user3" ] && ok "fidelity: row value round-trips (user3)" || bad "value: '$v'"

# cleanup
nano "DROP TABLE IF EXISTS _snaptest" >/dev/null 2>&1
pg   "DROP TABLE IF EXISTS _snaptest" >/dev/null 2>&1
echo "== snapshot test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
