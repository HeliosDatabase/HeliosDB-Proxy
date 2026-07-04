#!/usr/bin/env bash
# HeliosProxy live test — slow-client authentication (Group 3, 3.A).
#
# The proxy relays the client<->backend auth exchange. The pre-fix relay read
# the backend untimed, then polled the client with a FIXED 100ms window; a
# client that answered a SCRAM/password challenge more than 100ms after
# receiving it (WAN RTT, slow client) missed the window and the proxy then
# re-blocked on the untimed backend read while the backend waited for that very
# response — a deadlock until PostgreSQL's authentication_timeout fired.
#
# This test puts a delay-relay in front of the proxy that adds ~250ms of latency
# to every CLIENT->proxy chunk (so each of PG's multi-round SCRAM responses is
# delayed well past 100ms), then authenticates a real psql session through it.
# With the select()-based relay the handshake completes; the pre-fix binary
# hangs (and the test times out -> FAIL).
#
# Usage:  ./slow-auth-test.sh /path/to/heliosdb-proxy
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: slow-auth-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg.toml"
IMG="postgres:18.4-bookworm"
PXHOST=127.0.0.1; PXPORT=6432
RELAY_PORT=6543
DELAY_MS=250
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-slow-auth}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

cleanup(){
  [ -n "${RELAYPID:-}" ] && kill "$RELAYPID" 2>/dev/null
  [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT

echo "== slow-client auth  bin=$BIN =="
RUST_LOG=warn NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
# Wait for the proxy to be up (direct, no delay).
ready=0
for _ in $(seq 1 40); do
  if docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
       psql -h $PXHOST -p $PXPORT -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# Delay-relay: forward RELAY_PORT <-> proxy, delaying every client->proxy chunk
# by DELAY_MS so each SCRAM round-trip response reaches the proxy well past the
# old 100ms poll window.
cat > "$OUT/relay.py" <<'PY'
import asyncio, sys
listen_port = int(sys.argv[1]); up_host = sys.argv[2]; up_port = int(sys.argv[3])
delay = int(sys.argv[4]) / 1000.0

async def pump(reader, writer, delay_each):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            if delay_each:
                await asyncio.sleep(delay)     # simulate slow client->proxy leg
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try: writer.close()
        except Exception: pass

async def handle(client_reader, client_writer):
    try:
        up_reader, up_writer = await asyncio.open_connection(up_host, up_port)
    except Exception:
        client_writer.close(); return
    await asyncio.gather(
        pump(client_reader, up_writer, True),    # client -> proxy: DELAYED
        pump(up_reader, client_writer, False),   # proxy -> client: immediate
    )

async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", listen_port)
    async with server:
        await server.serve_forever()

asyncio.run(main())
PY
python3 "$OUT/relay.py" "$RELAY_PORT" "$PXHOST" "$PXPORT" "$DELAY_MS" >"$OUT/relay.log" 2>&1 &
RELAYPID=$!
PYRELAY_STARTED=$RELAYPID
sleep 1
kill -0 "$RELAYPID" 2>/dev/null || { echo "relay failed to start"; cat "$OUT/relay.log"; bad relay_start; }

# Authenticate through the delay-relay. Bound it so a deadlock (pre-fix) fails
# fast rather than waiting for PG's 60s auth timeout.
res=$(timeout 20 docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql "host=$PXHOST port=$RELAY_PORT user=$BUSER dbname=$BDB connect_timeout=18" \
        -tAc "select 'slow_auth_ok'" 2>&1)
rc=$?
echo "   psql rc=$rc out=$(printf '%s' "$res" | tr '\n' ' ')"
if [ "$rc" -eq 124 ]; then
  bad slow_client_authenticates "psql timed out — auth relay deadlocked on the slow client"
elif printf '%s' "$res" | grep -q 'slow_auth_ok'; then
  ok slow_client_authenticates "(SCRAM completed through +${DELAY_MS}ms/round delay)"
else
  bad slow_client_authenticates "unexpected: $res"
fi

# Proxy must still be healthy afterwards.
if kill -0 "$PROXYPID" 2>/dev/null; then ok proxy_alive_after; else bad proxy_alive_after "proxy died"; fi

echo "== slow-auth: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
