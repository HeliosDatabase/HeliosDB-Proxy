#!/usr/bin/env bash
# HeliosProxy live test — read-your-writes (feature: lag-routing).
#
# Through a live 2-node read/write-split proxy, proves the read-your-writes
# guarantee: a read issued within the window after a write in the SAME session
# is routed to the primary (so the client observes its own write), whereas a
# read with no preceding write is eligible for the standby. Verified via the
# per-query routing log (the decision is logged before the backend connect).
#
# Usage:  ./ryw-test.sh /path/to/heliosdb-proxy   (binary built with the
# `lag-routing` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: ryw-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-ryw.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb
PRIMARY_ADDR="127.0.0.1:25433"
STANDBY_ADDR="localhost:25433"

OUT="${OUT:-/tmp/regress-ryw}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== read-your-writes live test  bin=$BIN =="
RUST_LOG="helios::routing=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "/* warmup */ select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# Log delta (ANSI-stripped) appended after line $1.
delta_after(){ sed -n "$(($1+1)),\$p" "$LOG" | sed 's/\x1b\[[0-9;]*m//g'; }

# 1. Read-your-writes: one session does a write then a read. The read must
#    trigger the RYW pin (proven by the distinctive pin log) and route to the
#    primary. This is env-independent: it asserts the RYW code path executed,
#    not merely that the read landed on the primary.
b=$(wc -l < "$LOG")
pP -tA -c "create temp table ryw_t(x int)" -c "select 1" >/dev/null 2>&1
sleep 0.3
d=$(delta_after "$b")
write_node=$(printf '%s\n' "$d" | grep 'is_write=true'  | grep -oE 'node=[A-Za-z0-9._-]+:[0-9]+' | head -1 | sed 's/node=//')
read_node=$(printf '%s\n' "$d"  | grep 'is_write=false' | grep -oE 'node=[A-Za-z0-9._-]+:[0-9]+' | head -1 | sed 's/node=//')
[ "$write_node" = "$PRIMARY_ADDR" ] && ok write_routed_to_primary "($write_node)" \
  || bad write_routed_to_primary "write=[$write_node]"
printf '%s\n' "$d" | grep -q 'read-your-writes: pinning' \
  && ok ryw_pin_fires_after_write \
  || bad ryw_pin_fires_after_write "no RYW pin log after write+read"
[ "$read_node" = "$PRIMARY_ADDR" ] && ok ryw_read_on_primary "($read_node)" \
  || bad ryw_read_on_primary "read=[$read_node] want=$PRIMARY_ADDR"

# 2. A cold read (no preceding write in the session) must NOT trigger RYW —
#    proving the pin is caused by the write, not applied to every read.
b=$(wc -l < "$LOG")
pP -tAc "select 1" >/dev/null 2>&1
sleep 0.3
if printf '%s\n' "$(delta_after "$b")" | grep -q 'read-your-writes: pinning'; then
  bad ryw_silent_without_write "RYW pin fired on a read with no preceding write"
else
  ok ryw_silent_without_write
fi

echo "== read-your-writes: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
