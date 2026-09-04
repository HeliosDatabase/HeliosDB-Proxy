#!/usr/bin/env bash
# Proxy-terminated SCRAM-SHA-256 test (Batch F.3b).
#
# The proxy authenticates the client itself (mode = "scram") against an
# auth_file, then connects to a trust-mode backend (set NANO_HOST/NANO_PORT;
# default 127.0.0.1:55337 — any trust-auth PG-wire backend works, e.g. a local
# `heliosdb-nano ... --auth trust`). A client with the right password must be
# admitted via SCRAM; a wrong password must be rejected by the PROXY (the
# backend is never reached). When no trust backend is reachable the good-password
# path SKIPs (the proxy-rejection path needs no backend and still runs).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/scram-test}"; mkdir -p "$OUT"
PASS=0; FAIL=0; SKIP=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }
skip(){ SKIP=$((SKIP+1)); printf '  \033[33mSKIP\033[0m %s\n' "$*"; }

NANO_HOST="${NANO_HOST:-127.0.0.1}"
NANO_PORT="${NANO_PORT:-55337}"
printf 'postgres:proxypw123\n' > "$OUT/users.txt"
chmod 600 "$OUT/users.txt"

cat > "$OUT/proxy-scram.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode    = "none"
write_timeout_secs = 30
[auth]
mode = "scram"
auth_file = "$OUT/users.txt"
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
host = "$NANO_HOST"
port = $NANO_PORT
role = "primary"
weight = 100
enabled = true
name = "nano-trust"
EOF

"$BIN" --config "$OUT/proxy-scram.toml" >"$OUT/proxy.log" 2>&1 &
PROXYPID=$!
cleanup(){ kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT
sleep 2
kill -0 "$PROXYPID" 2>/dev/null || { echo "proxy died:"; cat "$OUT/proxy.log"; exit 1; }

# 1. Correct password -> proxy SCRAM-validates -> trust backend -> query runs.
#    Requires a reachable trust-mode backend; SKIP (not FAIL) when none is present.
if timeout 2 bash -c "exec 3<>/dev/tcp/$NANO_HOST/$NANO_PORT" 2>/dev/null; then
  good=$(docker run --rm --network host -e PGPASSWORD=proxypw123 "$IMG" \
    psql "host=127.0.0.1 port=6432 user=postgres dbname=postgres sslmode=disable" -tAc "select 'scram-ok'" 2>&1)
  echo "--- correct password ---"; echo "$good"
  echo "$good" | grep -q "scram-ok" && ok "scram_good: correct password admitted via proxy SCRAM" || bad "scram_good: $good"
else
  skip "scram_good: no trust backend at $NANO_HOST:$NANO_PORT (set NANO_HOST/NANO_PORT to a trust-mode PG-wire backend)"
fi

# 2. Wrong password -> proxy rejects (SCRAM proof mismatch), never reaches backend.
wrongp=$(docker run --rm --network host -e PGPASSWORD=WRONGPW "$IMG" \
  psql "host=127.0.0.1 port=6432 user=postgres dbname=postgres sslmode=disable" -tAc "select 'should-not-run'" 2>&1)
echo "--- wrong password ---"; echo "$wrongp"
if echo "$wrongp" | grep -qiE "authentication failed|password authentication"; then
  ok "scram_bad: wrong password rejected by proxy"
else bad "scram_bad: expected rejection, got: $wrongp"; fi

echo "== SCRAM test: PASS=$PASS FAIL=$FAIL SKIP=$SKIP =="
exit $FAIL
