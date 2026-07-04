#!/usr/bin/env bash
# HeliosProxy live test — extended-protocol Flush latency (Group 3, 3.B).
#
# A driver that sends Parse+Flush, waits for ParseComplete, then sends
# Bind/Execute/Sync used to eat up to ~200ms per prepare cycle: the proxy's
# post-Flush relay blocked the session loop for a fixed 200ms quiet period
# after the last backend byte before it would read the client's next message,
# so the Bind/Execute the client had already sent sat unprocessed. With the
# non-blocking flush drain + the main-loop backend watch that stall is gone.
#
# A raw-protocol client (SCRAM-SHA-256 auth in-line, since high-level drivers
# use Sync not Flush) times a full Parse+Flush → ParseComplete → Bind/Execute/
# Sync → result cycle. Old proxy: >=~200ms. New: a few ms.
#
# Usage:  ./flush-latency-test.sh /path/to/heliosdb-proxy
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: flush-latency-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg.toml"
IMG="postgres:18.4-bookworm"
PXHOST=127.0.0.1; PXPORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb
THRESH_MS=120   # old stall ~200ms; fixed path is single-digit ms

OUT="${OUT:-/tmp/regress-flush-latency}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

command -v python3 >/dev/null || { echo "SKIP: python3 required"; exit 0; }

echo "== extended-protocol Flush latency test  bin=$BIN =="
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

cat > "$OUT/flush.py" <<'PY'
import socket, struct, hashlib, hmac, base64, os, time, sys
host, port, user, pw, db = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]

def recvn(s, n):
    b = b''
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c: raise EOFError("eof")
        b += c
    return b
def recv_msg(s):
    h = recvn(s, 5)
    ln = struct.unpack('!I', h[1:5])[0]
    return h[0:1], recvn(s, ln - 4)
def msg(tag, payload=b''):
    return tag + struct.pack('!I', len(payload) + 4) + payload

s = socket.create_connection((host, port), timeout=10)
# startup
params = b''.join(k.encode()+b'\0'+v.encode()+b'\0' for k,v in
                  [('user',user),('database',db),('client_encoding','UTF8')]) + b'\0'
body = struct.pack('!I', 196608) + params
s.sendall(struct.pack('!I', len(body)+4) + body)

# SCRAM-SHA-256
tag, pl = recv_msg(s)
assert tag == b'R', tag
atype = struct.unpack('!I', pl[:4])[0]
assert atype == 10, ("expected SASL", atype)
cnonce = base64.b64encode(os.urandom(18)).decode()
cfirst_bare = f"n=,r={cnonce}"
gs2 = "n,,"
init = gs2 + cfirst_bare
mech = b"SCRAM-SHA-256\0"
s.sendall(msg(b'p', mech + struct.pack('!I', len(init)) + init.encode()))
tag, pl = recv_msg(s); assert tag == b'R', tag
atype = struct.unpack('!I', pl[:4])[0]; assert atype == 11, atype
sfirst = pl[4:].decode()
d = dict(kv.split('=',1) for kv in sfirst.split(','))
salt = base64.b64decode(d['s']); iters = int(d['i']); rnonce = d['r']
salted = hashlib.pbkdf2_hmac('sha256', pw.encode(), salt, iters)
ckey = hmac.new(salted, b'Client Key', hashlib.sha256).digest()
skey = hashlib.sha256(ckey).digest()
cfinal_bare = f"c={base64.b64encode(gs2.encode()).decode()},r={rnonce}"
auth_msg = f"{cfirst_bare},{sfirst},{cfinal_bare}"
csig = hmac.new(skey, auth_msg.encode(), hashlib.sha256).digest()
proof = bytes(a ^ b for a, b in zip(ckey, csig))
cfinal = f"{cfinal_bare},p={base64.b64encode(proof).decode()}"
s.sendall(msg(b'p', cfinal.encode()))
# consume SASLFinal + AuthOk + params + BackendKeyData + RFQ
while True:
    tag, pl = recv_msg(s)
    if tag == b'Z':  # ReadyForQuery
        break

# ---- timed extended-protocol Parse+Flush → ParseComplete → Bind/Exec/Sync ----
parse = msg(b'P', b'\0' + b'SELECT 42\0' + struct.pack('!H', 0))  # unnamed, 0 param types
flush = msg(b'H')
bind  = msg(b'B', b'\0\0' + struct.pack('!HHH', 0, 0, 0))          # unnamed portal+stmt
execu = msg(b'E', b'\0' + struct.pack('!I', 0))
sync  = msg(b'S')

t0 = time.monotonic()
s.sendall(parse + flush)
# wait for ParseComplete ('1')
while True:
    tag, pl = recv_msg(s)
    if tag == b'1': break
    if tag == b'E': print("PARSE_ERROR"); sys.exit(4)
# got ParseComplete → now drive the rest, as a real driver would
s.sendall(bind + execu + sync)
got_row = False
while True:
    tag, pl = recv_msg(s)
    if tag == b'D': got_row = True
    if tag == b'Z': break
    if tag == b'E': print("EXEC_ERROR"); sys.exit(5)
elapsed_ms = (time.monotonic() - t0) * 1000.0
print(f"ELAPSED_MS={elapsed_ms:.1f} ROW={got_row}")
PY

out=$(python3 "$OUT/flush.py" "$PXHOST" "$PXPORT" "$BUSER" "$BPASS" "$BDB" 2>&1)
echo "   $out"
ms=$(printf '%s' "$out" | sed -n 's/.*ELAPSED_MS=\([0-9.]*\).*/\1/p')
if [ -z "$ms" ]; then
  bad flush_cycle_completes "raw client did not complete: $out"
else
  ok flush_cycle_completes "(round-trip ${ms}ms)"
  # integer compare
  mi=${ms%.*}
  if [ "${mi:-9999}" -lt "$THRESH_MS" ]; then
    ok no_flush_stall "(${ms}ms < ${THRESH_MS}ms — no 200ms Flush stall)"
  else
    bad no_flush_stall "(${ms}ms >= ${THRESH_MS}ms — Flush stall present)"
  fi
fi

echo "== flush-latency test: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
