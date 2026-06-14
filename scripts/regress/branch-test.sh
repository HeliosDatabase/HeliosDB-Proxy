#!/usr/bin/env bash
# Branch-database test (Batch G — instant branches via the proxy).
#
# Provision a CREATE DATABASE ... TEMPLATE clone through POST /api/branch,
# connect to the branch through the proxy, verify it cloned the base's data,
# verify writes to the branch don't touch the base (isolation), then drop it.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/branch-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

# direct PG psql against a chosen database
pgd(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 25433 -U bench -d "$1" -tAc "$2" 2>&1; }
# psql to a database THROUGH the proxy
viaproxy(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d "$1" -tAc "$2" 2>&1; }

# 0. Build a base template DB with data (nothing stays connected to it).
pgd postgres "DROP DATABASE IF EXISTS feat1" >/dev/null 2>&1
pgd postgres "DROP DATABASE IF EXISTS branchbase" >/dev/null 2>&1
pgd postgres "CREATE DATABASE branchbase" >/dev/null 2>&1
pgd branchbase "CREATE TABLE t(id int); INSERT INTO t VALUES (1),(2),(3);" >/dev/null 2>&1

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[branch]
enabled = true
backend_host = "127.0.0.1"
backend_port = 25433
admin_user = "bench"
admin_password = "benchpass"
admin_database = "postgres"
base_database = "branchbase"
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
cleanup(){ kill "$P" 2>/dev/null; wait "$P" 2>/dev/null; pgd postgres "DROP DATABASE IF EXISTS feat1" >/dev/null 2>&1; pgd postgres "DROP DATABASE IF EXISTS branchbase" >/dev/null 2>&1; }
trap cleanup EXIT
for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9099/health" && break; sleep 0.3; done
B="http://127.0.0.1:9099/api/branch"

# 1. Create a branch from branchbase.
cr=$(curl -s -X POST "$B" -d '{"name":"feat1","base":"branchbase"}')
echo "create: $cr"
echo "$cr" | grep -q '"ok":true' && ok "create: branch feat1 provisioned" || bad "create: $cr"

# 2. List includes the branch.
ls=$(curl -s "$B")
echo "$ls" | grep -q 'feat1' && ok "list: feat1 present" || bad "list: $ls"

# 3. Connect to the branch through the proxy -> it cloned the base's 3 rows.
c=$(viaproxy feat1 "SELECT count(*) FROM t" | tr -d '[:space:]')
[ "$c" = "3" ] && ok "clone: branch has the base's 3 rows" || bad "clone count: '$c'"

# 4. Isolation: write to the branch, base is unchanged.
viaproxy feat1 "INSERT INTO t VALUES (4),(5)" >/dev/null
bc=$(pgd branchbase "SELECT count(*) FROM t" | tr -d '[:space:]')
fc=$(viaproxy feat1 "SELECT count(*) FROM t" | tr -d '[:space:]')
[ "$bc" = "3" ] && [ "$fc" = "5" ] && ok "isolation: branch=5, base still 3" || bad "isolation: base=$bc branch=$fc"

# 5. Drop the branch.
sleep 1
dr=$(curl -s -X DELETE "$B?name=feat1")
echo "drop: $dr"
echo "$dr" | grep -q '"ok":true' && ok "drop: branch removed" || bad "drop: $dr"
ls2=$(curl -s "$B")
echo "$ls2" | grep -q 'feat1' && bad "drop: feat1 still listed" || ok "drop: feat1 gone from list"

echo "== branch test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
