#!/usr/bin/env bash
# Zero-downtime SIGHUP config reload test (Batch H, item 84).
#
# Proves the four guarantees of a live reload:
#   T1  an in-flight connection is NOT dropped when the config is reloaded
#   T2  a NEW connection picks up the reloaded config (hba reject takes effect)
#   T3  reload is reversible (restore allow-all -> new connections work again)
#   T4  a broken config file on SIGHUP is rejected; the proxy keeps running on
#       the last-good config (new connections still work)
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/reload-test}"; mkdir -p "$OUT"
CFG="$OUT/proxy.toml"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

base_cfg(){ cat <<'EOF'
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
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
}

# psql through the proxy
viaproxy(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
  psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" "$@" 2>&1; }

base_cfg > "$CFG"
"$BIN" --config "$CFG" >"$OUT/proxy.log" 2>&1 &
P=$!
trap 'kill "$P" 2>/dev/null; wait "$P" 2>/dev/null' EXIT
for i in $(seq 1 30); do viaproxy -tAc "select 1" >/dev/null 2>&1 && break; sleep 0.3; done

# Sanity: a new connection works on the base (allow-all) config.
[ "$(viaproxy -tAc 'select 42' | tr -d '[:space:]')" = "42" ] && ok "baseline: query works" || bad "baseline failed"

# T1 setup: open a long-lived in-flight connection (3s server-side sleep) that
# will straddle the reload.
( viaproxy -tAc "SELECT pg_sleep(3); SELECT 'survived'" > "$OUT/longconn.out" 2>&1 ) &
LONG=$!
sleep 1.5   # connection is established and mid-sleep

# T2: reload with an hba rule that REJECTS new bench connections.
{ base_cfg; printf '\n[[hba]]\naction = "reject"\nuser = "bench"\ndatabase = "all"\naddress = "all"\n'; } > "$CFG"
kill -HUP "$P"; sleep 1
out=$(viaproxy -tAc "select 99")
echo "$out" | grep -qi "rejected by proxy admission" && ok "T2: new connection sees reloaded hba (rejected)" || bad "T2: expected rejection, got: $out"

# T1 check: the in-flight connection that started BEFORE the reload completes.
wait "$LONG" 2>/dev/null
grep -q "survived" "$OUT/longconn.out" && ok "T1: in-flight connection survived the reload" || bad "T1: long conn output: $(cat "$OUT/longconn.out")"

# T3: reload back to allow-all; new connections work again.
base_cfg > "$CFG"
kill -HUP "$P"; sleep 1
[ "$(viaproxy -tAc 'select 7' | tr -d '[:space:]')" = "7" ] && ok "T3: reload reversible (new connection works again)" || bad "T3: expected 7"

# T4: a broken config on SIGHUP is rejected; the proxy keeps the last-good config.
printf 'this is = not valid toml [[[\n' > "$CFG"
kill -HUP "$P"; sleep 1
kill -0 "$P" 2>/dev/null && ok "T4: proxy still running after bad-config SIGHUP" || bad "T4: proxy died on bad config"
[ "$(viaproxy -tAc 'select 5' | tr -d '[:space:]')" = "5" ] && ok "T4: still serving on last-good config" || bad "T4: not serving after bad reload"

echo "--- reload log lines ---"; grep -iE "SIGHUP|reload" "$OUT/proxy.log" | tail -6
echo "== reload test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
