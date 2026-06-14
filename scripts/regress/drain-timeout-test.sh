#!/usr/bin/env bash
# Drain-timeout config test (Batch H — shutdown_drain_timeout_secs).
#
# Proves the configured graceful-drain deadline is honored: a connection that
# outlives the timeout is dropped so the handoff completes in bounded time
# (the deadline branch handoff-test doesn't exercise). Uses the config field —
# the HELIOS_DRAIN_TIMEOUT_SECS env override is explicitly unset here.
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/drain-timeout}"; mkdir -p "$OUT"
unset HELIOS_DRAIN_TIMEOUT_SECS
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

cat > "$OUT/proxy.toml" <<'EOF'
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
shutdown_drain_timeout_secs = 2
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

"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 & P=$!
trap 'kill "$P" 2>/dev/null; wait "$P" 2>/dev/null' EXIT
for i in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
    psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" -tAc "select 1" >/dev/null 2>&1 && break
  sleep 0.3
done

# Open a connection that sleeps far longer (20s) than the 2s drain timeout.
( docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
    psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" \
    -tAc "SELECT pg_sleep(20)" >/dev/null 2>&1 ) &
sleep 1.5

# Trigger graceful drain and time how long until the proxy exits.
start=$(date +%s)
kill -USR2 "$P"
gone=0
for i in $(seq 1 60); do kill -0 "$P" 2>/dev/null || { gone=1; break; }; sleep 0.2; done
elapsed=$(( $(date +%s) - start ))

[ "$gone" = 1 ] && ok "proxy exited after drain (not hung on the 20s connection)" || bad "proxy still running after 12s"
# Should be ~2s (the configured deadline), well under the 20s sleep.
{ [ "$elapsed" -ge 1 ] && [ "$elapsed" -le 6 ]; } && ok "exited at ~configured 2s deadline (elapsed ${elapsed}s, < 20s sleep)" || bad "drain took ${elapsed}s (expected ~2s, not ~20s)"
grep -qi "drain timeout reached" "$OUT/proxy.log" && ok "log shows the deadline branch ('drain timeout reached')" || bad "deadline log line missing"
# strip ANSI colour codes before matching the structured field
sed 's/\x1b\[[0-9;]*m//g' "$OUT/proxy.log" | grep -q "timeout_secs=2" && ok "config value (2s) was used, not the 60s default" || bad "config drain timeout not honored: $(sed 's/\x1b\[[0-9;]*m//g' "$OUT/proxy.log" | grep -i 'draining in-flight')"

echo "== drain-timeout test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
