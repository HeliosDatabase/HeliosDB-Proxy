#!/usr/bin/env bash
# Zero-downtime binary handoff test (Batch H, item 84 — SO_REUSEPORT + drain).
#
# Models a binary upgrade: instance A is serving; instance B (the "new binary")
# binds the SAME listen address via SO_REUSEPORT while A still runs; A is sent
# SIGUSR2 to close its listener and gracefully drain. Proves:
#   T1  B binds the shared listen port while A runs (SO_REUSEPORT works)
#   T2  every NEW connection during/after A's drain succeeds (zero downtime)
#   T3  A's in-flight connection (opened before the handoff) runs to completion
#   T4  A exits once its connections drain
#   T5  B keeps serving after A is gone
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/handoff-test}"; rm -rf "$OUT"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

gen_cfg(){ # $1=admin_port
cat <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:$1"
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
gen_cfg 9099 > "$OUT/a.toml"
gen_cfg 9100 > "$OUT/b.toml"

q(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" \
  psql "host=127.0.0.1 port=6432 user=bench dbname=benchdb sslmode=disable" "$@" 2>&1; }
admin_up(){ [ "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$1/health" 2>/dev/null)" = "200" ]; }

# Start instance A.
HELIOS_DRAIN_TIMEOUT_SECS=20 "$BIN" --config "$OUT/a.toml" >"$OUT/a.log" 2>&1 &
A=$!
cleanup(){ kill "$A" 2>/dev/null; kill "$B" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT
for i in $(seq 1 30); do admin_up 9099 && break; sleep 0.3; done
[ "$(q -tAc 'select 1' | tr -d '[:space:]')" = "1" ] && ok "A: serving" || bad "A failed to serve"

# Open an in-flight connection to A (4s server-side) BEFORE the handoff — only
# A exists right now, so it is pinned to A's process.
( q -tAc "SELECT pg_sleep(4); SELECT 'A-survived'" > "$OUT/inflight.out" 2>&1 ) &
INFLIGHT=$!
sleep 1   # established + registered in A's session map, mid-sleep

# T1: start instance B on the SAME listen port (SO_REUSEPORT) — a plain bind
# would fail with EADDRINUSE.
"$BIN" --config "$OUT/b.toml" >"$OUT/b.log" 2>&1 &
B=$!
bok=0; for i in $(seq 1 30); do admin_up 9100 && { bok=1; break; }; sleep 0.3; done
if [ "$bok" = 1 ]; then ok "T1: B bound the shared :6432 via SO_REUSEPORT (admin :9100 up)"
else bad "T1: B failed to start/bind: $(tail -3 "$OUT/b.log")"; fi

# Hand off: tell A to drain.
kill -USR2 "$A"
sleep 0.5

# T2: a burst of NEW connections during/after A's drain — all must succeed.
n_ok=0; for i in $(seq 1 8); do
  [ "$(q -tAc 'select 1' | tr -d '[:space:]')" = "1" ] && n_ok=$((n_ok+1))
done
[ "$n_ok" = 8 ] && ok "T2: 8/8 new connections served during handoff (zero downtime)" || bad "T2: only $n_ok/8 new connections succeeded"

# T3: the in-flight connection opened before the handoff completes.
wait "$INFLIGHT" 2>/dev/null
grep -q "A-survived" "$OUT/inflight.out" && ok "T3: in-flight connection survived the drain" || bad "T3: inflight=$(cat "$OUT/inflight.out")"

# T4: A exits once it has drained (poll up to ~12s).
gone=0; for i in $(seq 1 60); do kill -0 "$A" 2>/dev/null || { gone=1; break; }; sleep 0.2; done
[ "$gone" = 1 ] && ok "T4: A exited after draining" || bad "T4: A still running after drain window"

# T5: B keeps serving after A is gone.
admin_up 9100 && [ "$(q -tAc 'select 5' | tr -d '[:space:]')" = "5" ] && ok "T5: B still serving after A exited" || bad "T5: B not serving after handoff"

echo "--- A drain log ---"; grep -iE "SIGUSR2|drain" "$OUT/a.log" | tail -4
echo "== handoff test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
