#!/usr/bin/env bash
# Single-variant scalability run: start <proxy-binary> in front of a backend
# and measure simple-protocol throughput (tps) at increasing client
# concurrency. Emits "concurrency tps" lines on stdout; logs to $OUT.
#
# Backend selected via env: BHOST BPORT BUSER BPASS BDB (defaults = Nano 3.57).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: scale.sh <proxy-binary>}"
IMG="postgres:18.4-bookworm"
BHOST="${BHOST:-100.64.0.2}"; BPORT="${BPORT:-54320}"
BUSER="${BUSER:-postgres}"; BPASS="${BPASS:-OTPZ7Mxh9FJEeeKF3qqSKmW64lmT2u3}"; BDB="${BDB:-postgres}"
DUR="${DUR:-6}"
LEVELS="${LEVELS:-1 8 16 32 64}"
OUT="${OUT:-/tmp/scale}"; mkdir -p "$OUT"
MAXJ=16

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode    = "none"
write_timeout_secs = 30
[pool]
min_connections = 4
max_connections = 200
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
host = "$BHOST"
port = $BPORT
role = "primary"
weight = 100
enabled = true
name = "backend"
EOF

"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 &
PROXYPID=$!
cleanup(){ kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT

# readiness
ready=0
for i in $(seq 1 40); do
  if docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6432 -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  kill -0 "$PROXYPID" 2>/dev/null || { echo "# proxy died"; tail -5 "$OUT/proxy.log"; exit 1; }
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "# proxy not ready"; exit 1; }

printf 'SELECT 1;\n' > "$OUT/sel.sql"
for c in $LEVELS; do
  j=$c; [ "$j" -gt "$MAXJ" ] && j=$MAXJ
  res=$(docker run --rm --network host -e PGPASSWORD="$BPASS" -v "$OUT":/w "$IMG" \
    pgbench -h 127.0.0.1 -p 6432 -U "$BUSER" -d "$BDB" -n -M simple -f /w/sel.sql \
    -c "$c" -j "$j" -T "$DUR" 2>>"$OUT/pgbench.log")
  tps=$(echo "$res" | grep -oiE 'tps = [0-9.]+' | head -1 | awk '{print $3}')
  tps=${tps%.*}
  printf '%s %s\n' "$c" "${tps:-0}"
done
