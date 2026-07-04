#!/usr/bin/env bash
# HeliosProxy live test — malformed / abusive input hardening (Group 4).
#
# Verifies the proxy survives unauthenticated malformed input and cannot be
# crashed or OOM'd by it:
#   1. A PG frame with a sub-minimum length field (len<4) is rejected without
#      panicking the connection task or the process (M1 decode guard).
#   2. A short startup packet (len<8) likewise cannot crash the pre-auth path.
#   3. An admin request with an enormous Content-Length is refused with 413
#      instead of allocating gigabytes (G4 admin cap) — the proxy stays up.
#   4. After all the abuse the proxy still serves real queries and /health.
#
# Usage:  BK=pg ./malformed-input-test.sh /path/to/heliosdb-proxy
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: malformed-input-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg.toml"
IMG="postgres:18.4-bookworm"
PXHOST=127.0.0.1; PXPORT=6432; ADMIN=127.0.0.1:9099
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-malformed}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $PXHOST -p $PXPORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== malformed-input hardening  bin=$BIN =="
RUST_LOG=warn NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# 1+2. Send malformed PG frames over raw sockets. The proxy must not crash;
# the connection may be closed, but the process stays up.
python3 - "$PXHOST" "$PXPORT" <<'PY'
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
# Regular frame with length field 0 (below the 4-byte minimum).
for payload in (b"Q\x00\x00\x00\x00", b"\x00\x00\x00\x04"):
    try:
        s = socket.create_connection((host, port), timeout=3)
        s.sendall(payload)
        s.settimeout(2)
        try: s.recv(64)
        except Exception: pass
        s.close()
    except Exception:
        pass
print("sent")
PY

sleep 0.3
if kill -0 "$PROXYPID" 2>/dev/null; then ok proxy_survives_malformed_frames; else bad proxy_survives_malformed_frames "proxy died"; fi

# 3. Oversized admin Content-Length: send only the header (no body) and expect a
# 413 (or a clean close), NOT an OOM / crash.
resp=$(python3 - "$ADMIN" <<'PY'
import socket, sys
host, port = sys.argv[1].split(":")
try:
    s = socket.create_connection((host, int(port)), timeout=3)
    req = ("POST /api/branch HTTP/1.1\r\nHost: x\r\n"
           "Content-Length: 99999999999\r\n\r\n")
    s.sendall(req.encode())
    s.settimeout(3)
    data = s.recv(256).decode(errors="replace")
    print(data.splitlines()[0] if data else "NO_RESPONSE")
    s.close()
except Exception as e:
    print("ERR", e)
PY
)
echo "   admin response: $resp"
if printf '%s' "$resp" | grep -q '413'; then
  ok admin_rejects_oversized_body "(413)"
elif printf '%s' "$resp" | grep -qiE 'NO_RESPONSE|ERR'; then
  # A clean close without allocating is also acceptable hardening.
  if kill -0 "$PROXYPID" 2>/dev/null; then ok admin_rejects_oversized_body "(closed, proxy up)"; else bad admin_rejects_oversized_body "proxy died"; fi
else
  bad admin_rejects_oversized_body "unexpected: $resp"
fi
if kill -0 "$PROXYPID" 2>/dev/null; then ok proxy_survives_admin_abuse; else bad proxy_survives_admin_abuse "proxy died (OOM?)"; fi

# 4. Proxy still fully functional afterwards.
got=$(pP -tAc "select 123" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "123" ] && ok proxy_serves_after_abuse "($got)" || bad proxy_serves_after_abuse "got '$got'"
hc=$(curl -s -o /dev/null -w '%{http_code}' "http://$ADMIN/health" 2>/dev/null)
[ "$hc" = "200" ] && ok admin_health_after_abuse || bad admin_health_after_abuse "http=$hc"

echo "== malformed-input: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
