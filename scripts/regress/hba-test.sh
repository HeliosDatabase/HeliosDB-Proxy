#!/usr/bin/env bash
# pg_hba-style admission test (Batch F.3a).
#
# 1. A reject rule for the bench user must block the connection with SQLSTATE
#    28000 before any backend work.
# 2. An allow-then-default-deny ruleset must still admit the matching user.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/hba-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

base_cfg() { cat <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode    = "none"
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
name = "pg-primary"
EOF
}

run_case() {  # $1=label  $2=cfgfile  -> echoes psql output
  "$BIN" --config "$2" >"$OUT/proxy.log" 2>&1 &
  local pid=$!
  for i in $(seq 1 30); do
    docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "select 1" >/dev/null 2>&1 && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.4
  done
  local out
  out=$(docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 6432 -U bench -d benchdb -tAc "select 'admitted'" 2>&1)
  kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
  echo "$out"
}

# Case 1: reject bench
{ base_cfg; printf '\n[[hba]]\naction = "reject"\nuser = "bench"\ndatabase = "all"\naddress = "all"\n'; } > "$OUT/reject.toml"
out1=$(run_case reject "$OUT/reject.toml")
echo "--- reject case ---"; echo "$out1"
if echo "$out1" | grep -qi "rejected by proxy admission"; then ok "hba_reject: bench blocked (28000)"; else bad "hba_reject: expected rejection, got: $out1"; fi

# Case 2: allow bench from localhost, default-deny everyone else
{ base_cfg; printf '\n[[hba]]\naction = "allow"\nuser = "bench"\ndatabase = "all"\naddress = "127.0.0.1/32"\n[[hba]]\naction = "reject"\nuser = "all"\ndatabase = "all"\naddress = "all"\n'; } > "$OUT/allow.toml"
out2=$(run_case allow "$OUT/allow.toml")
echo "--- allow case ---"; echo "$out2"
if echo "$out2" | grep -q "admitted"; then ok "hba_allow: bench admitted by matching allow rule"; else bad "hba_allow: expected admit, got: $out2"; fi

echo "== HBA test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
