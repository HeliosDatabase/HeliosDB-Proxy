#!/usr/bin/env bash
# HeliosProxy live test — rate limiting (feature: rate-limiting).
#
# Through a live proxy in front of PostgreSQL 18.4 with a tiny per-user token
# bucket (qps=1, burst=2), proves:
#   1. a rapid burst of queries from one session is throttled — only ~burst
#      queries pass, the rest are denied with a PG ErrorResponse (SQLSTATE
#      53400) and the proxy keeps serving (no connection drop);
#   2. after the bucket refills, queries succeed again (limit is transient,
#      not a hard block).
#
# Usage:  ./rate-limit-test.sh /path/to/heliosdb-proxy   (binary built with the
# `rate-limiting` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: rate-limit-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-ratelimit.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-ratelimit}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" -v "$OUT":/w "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== rate-limit live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# Build a 20-statement script (mounted file; avoids docker-stdin quirks).
printf 'select 1;\n%.0s' {1..20} > "$OUT/burst.sql"

# Let the bucket refill to full (burst=2) after readiness consumed tokens.
sleep 3

# Fire all 20 back-to-back in ONE session. ~burst succeed, the rest are denied
# with the proxy's distinctive rate-limit ErrorResponse.
out=$(pP -tA -f /w/burst.sql 2>&1)
allowed=$(printf '%s\n' "$out" | grep -c '^1$')
denied=$(printf '%s\n' "$out" | grep -c 'rate limit exceeded')

# 1. some queries allowed (the burst), many denied (the limit bit).
[ "$allowed" -ge 1 ] && ok burst_some_allowed "(allowed=$allowed)" \
  || bad burst_some_allowed "allowed=$allowed"
[ "$denied" -ge 5 ] && ok burst_denied_over_limit "(denied=$denied)" \
  || bad burst_denied_over_limit "denied=$denied (expected >=5)"
# 2. the denial carries SQLSTATE 53400 (verbose error code from the proxy).
verbose=$(pP -tA -v VERBOSITY=verbose -f /w/burst.sql 2>&1)
printf '%s\n' "$verbose" | grep -q '53400' && ok denial_sqlstate_53400 \
  || bad denial_sqlstate_53400 "no 53400 in output"

# 3. proxy still serves (it didn't drop the connection on denial): after a
#    refill, a single query succeeds again.
sleep 3
got=$(pP -tAc "select 42" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "42" ] && ok recovers_after_refill "($got)" || bad recovers_after_refill "got=[$got]"

echo "== rate-limit: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
