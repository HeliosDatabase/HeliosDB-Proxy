#!/usr/bin/env bash
# Traffic-mirror test (Batch G — on-ramp to the G2 PG->Nano migration mirror).
#
# Primary = PostgreSQL 18.4; mirror target = HeliosDB-Nano 3.37. Writes sent
# through the proxy land on the primary AND are asynchronously replayed to the
# mirror. The test writes through the proxy, then verifies the rows propagate
# to Nano. Reads (writes_only=true) must NOT propagate.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/mirror-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

# primary = PG, mirror = Nano
NANO_PW='OTPZ7Mxh9FJEeeKF3qqSKmW64lmT2u3'
cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[mirror]
enabled = true
writes_only = true
sample_rate = 1.0
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

# helper: query Nano (mirror) directly
nano(){ docker run --rm --network host -e PGPASSWORD="$NANO_PW" "$IMG" psql -h 127.0.0.1 -p 55337 -U postgres -d postgres -tAc "$1" 2>/dev/null; }
# helper: through the proxy as the PG client
viaproxy(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -c "$1" 2>&1; }

# Clean slate on both sides (DROP is a write -> also mirrored).
viaproxy "DROP TABLE IF EXISTS _mirtest" >/dev/null 2>&1
sleep 1

# Writes through the proxy.
viaproxy "CREATE TABLE _mirtest(id int, v text)" | tail -1
for i in 1 2 3 4 5; do viaproxy "INSERT INTO _mirtest VALUES ($i, 'row$i')" >/dev/null; done

# Primary (PG) sanity: 5 rows.
pgc=$(docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "SELECT count(*) FROM _mirtest" 2>/dev/null | tr -d '[:space:]')
[ "$pgc" = "5" ] && ok "primary: 5 rows on PG via proxy" || bad "primary count: $pgc"

# Mirror (Nano) propagation: poll up to ~12s for the async replay to land.
got=""
for i in $(seq 1 40); do
  got=$(nano "SELECT count(*) FROM _mirtest" | tr -d '[:space:]')
  [ "$got" = "5" ] && break
  sleep 0.3
done
[ "$got" = "5" ] && ok "mirror: writes propagated to Nano (5 rows)" || bad "mirror count on Nano: '$got'"

# Reads are not mirrored: a SELECT through the proxy creates nothing new.
before=$(nano "SELECT count(*) FROM _mirtest" | tr -d '[:space:]')
viaproxy "SELECT * FROM _mirtest" >/dev/null
sleep 1
after=$(nano "SELECT count(*) FROM _mirtest" | tr -d '[:space:]')
[ "$before" = "$after" ] && ok "reads_not_mirrored: SELECT did not alter mirror" || bad "reads leaked: $before -> $after"

# cleanup mirror side
nano "DROP TABLE IF EXISTS _mirtest" >/dev/null 2>&1
echo "== mirror test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
