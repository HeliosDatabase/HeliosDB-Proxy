#!/usr/bin/env bash
# Admin-listener SO_REUSEPORT test (Batch H — completes item 84 handoff).
#
# Two proxies bind the SAME admin address concurrently. Without SO_REUSEPORT the
# second admin bind fails (EADDRINUSE). Proof that the second actually bound it:
# kill the first and confirm the admin port still answers (only the second is
# left). Both also share the client listen address (already SO_REUSEPORT).
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
OUT="${OUT:-/tmp/admin-reuseport}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }
admin_up(){ [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:9099/health" 2>/dev/null)" = "200" ]; }

cat > "$OUT/proxy.toml" <<'EOF'
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
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

"$BIN" --config "$OUT/proxy.toml" >"$OUT/a.log" 2>&1 & A=$!
"$BIN" --config "$OUT/proxy.toml" >"$OUT/b.log" 2>&1 & B=$!
trap 'kill "$A" "$B" 2>/dev/null; wait 2>/dev/null' EXIT
for i in $(seq 1 30); do admin_up && break; sleep 0.3; done

# 1. Both processes alive (neither died on a bind collision).
{ kill -0 "$A" 2>/dev/null && kill -0 "$B" 2>/dev/null; } && ok "both proxies bound the shared client+admin addresses" || bad "a process died on bind"

# 2. Neither logged an admin bind failure.
if grep -qi "Failed to bind admin" "$OUT/a.log" "$OUT/b.log"; then bad "an admin bind failed: $(grep -i 'Failed to bind admin' "$OUT"/*.log)"; else ok "no admin bind failure logged"; fi

# 3. Admin port answers.
admin_up && ok "admin :9099 responds" || bad "admin :9099 not responding"

# 4. Kill A; the admin port must STILL answer -> B genuinely bound it (SO_REUSEPORT).
kill "$A" 2>/dev/null; wait "$A" 2>/dev/null
sleep 1
admin_up && ok "admin :9099 still responds after killing the first proxy (B bound it too)" || bad "admin died with A — B did not bind the admin port"

echo "== admin-reuseport test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
