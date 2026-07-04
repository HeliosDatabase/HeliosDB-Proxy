#!/usr/bin/env bash
# HeliosProxy live test — LISTEN/NOTIFY delivery to an IDLE client (Group 3, 3.C).
#
# A client that LISTENs and then sits idle (not issuing further queries) must
# still receive asynchronous NOTIFY messages promptly. Before the main-loop
# backend watch, the proxy — parked reading the CLIENT while idle — never read
# the backend socket, so a NotificationResponse sat unread in the proxy↔backend
# buffer and never reached the client until its next query. This test proves the
# notification now arrives while the listener is idle.
#
# The listener (psycopg2, host python) issues LISTEN, then blocks in
# select() on its own socket WITHOUT issuing any query — it only wakes if the
# proxy relays the NOTIFY bytes to it. A separate connection fires the NOTIFY.
#
# Usage:  ./notify-test.sh /path/to/heliosdb-proxy
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: notify-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg.toml"
IMG="postgres:18.4-bookworm"
PXHOST=127.0.0.1; PXPORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb
CHAN=heliostest; PAYLOAD=helios-notify-42

OUT="${OUT:-/tmp/regress-notify}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

command -v python3 >/dev/null && python3 -c 'import psycopg2' 2>/dev/null || {
  echo "SKIP: python3 + psycopg2 required"; exit 0; }

echo "== LISTEN/NOTIFY idle-delivery test  bin=$BIN =="
RUST_LOG=warn NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
       psql -h $PXHOST -p $PXPORT -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# Listener: LISTEN through the proxy, then idle-wait for the NOTIFY.
cat > "$OUT/listen.py" <<'PY'
import psycopg2, psycopg2.extensions, select, sys
host, port, chan, readyf = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
conn = psycopg2.connect(host=host, port=port, user='bench', password='benchpass', dbname='benchdb')
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute(f"LISTEN {chan};")
open(readyf, "w").write("ready")           # signal: LISTEN issued, going idle
# Idle-wait for an async notification WITHOUT issuing any further query. The
# proxy must relay the NOTIFY to our socket for select() to fire.
if select.select([conn], [], [], 12) == ([], [], []):
    print("TIMEOUT"); sys.exit(2)
conn.poll()
if conn.notifies:
    n = conn.notifies.pop(0)
    print(f"GOT channel={n.channel} payload={n.payload}"); sys.exit(0)
print("WOKE-BUT-NO-NOTIFY"); sys.exit(3)
PY

rm -f "$OUT/ready"
python3 "$OUT/listen.py" "$PXHOST" "$PXPORT" "$CHAN" "$OUT/ready" > "$OUT/listen.out" 2>&1 &
LPID=$!
# Wait for the listener to have issued LISTEN and gone idle.
for _ in $(seq 1 40); do [ -f "$OUT/ready" ] && break; sleep 0.25; done
sleep 0.5   # ensure it is parked in select()

# Fire the NOTIFY from a separate connection (direct to PG; NOTIFY is db-wide).
docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
  psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" -tAc "NOTIFY $CHAN, '$PAYLOAD'" >/dev/null 2>&1

wait "$LPID"; lrc=$?
lout=$(cat "$OUT/listen.out" 2>/dev/null)
echo "   listener: $lout"
if [ "$lrc" = 0 ] && printf '%s' "$lout" | grep -q "payload=$PAYLOAD"; then
  ok idle_notify_delivered "(received while idle)"
elif printf '%s' "$lout" | grep -q TIMEOUT; then
  bad idle_notify_delivered "listener idle-timed-out — NOTIFY not relayed to the idle client"
else
  bad idle_notify_delivered "unexpected: $lout"
fi

# Sanity: the proxy is still healthy and serving after the async relay.
got=$(docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
       psql -h $PXHOST -p $PXPORT -U "$BUSER" -d "$BDB" -tAc "select 7*6" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "42" ] && ok proxy_serving_after "($got)" || bad proxy_serving_after "got '$got'"

echo "== notify test: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
