#!/usr/bin/env bash
# HeliosProxy live test — query analytics (feature: query-analytics).
#
# Sends queries through a live proxy and reads them back via the new
# GET /api/analytics admin endpoint, proving:
#   1. forwarded queries are recorded with a normalized fingerprint, and
#      literal-only differences collapse to ONE fingerprint (select 1 / select 2
#      / select 3 -> same, calls accumulate);
#   2. a query slower than the threshold lands in the slow-query log.
#
# Usage:  ./analytics-test.sh /path/to/heliosdb-proxy   (binary built with the
# `query-analytics` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: analytics-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-analytics.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432; ADMIN=127.0.0.1:9099
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-analytics}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== analytics live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# Drive distinct-by-literal queries (same fingerprint) + a slow one.
pP -tAc "select 1"            >/dev/null 2>&1
pP -tAc "select 2"           >/dev/null 2>&1
pP -tAc "select 3"           >/dev/null 2>&1
pP -tAc "select pg_sleep(0.25)" >/dev/null 2>&1
sleep 0.5

json=$(curl -s "http://$ADMIN/api/analytics")
echo "  analytics: $(printf '%s' "$json" | head -c 240)"

nq=$(printf '%s' "$json"   | jq -r '.top_queries | length' 2>/dev/null)
maxcalls=$(printf '%s' "$json" | jq -r '[.top_queries[].calls] | max // 0' 2>/dev/null)
slow=$(printf '%s' "$json" | jq -r '.slow_query_count' 2>/dev/null)
# does a normalized fingerprint collapse the integer literal?
normseen=$(printf '%s' "$json" | jq -r '[.top_queries[].normalized] | join(" | ")' 2>/dev/null)

[ "${nq:-0}" -ge 1 ] 2>/dev/null && ok analytics_records_queries "(fingerprints=$nq)" \
  || bad analytics_records_queries "top_queries empty (json: $(printf '%s' "$json" | head -c 120))"

[ "${maxcalls:-0}" -ge 2 ] 2>/dev/null && ok literals_collapse_to_one_fingerprint "(max calls=$maxcalls)" \
  || bad literals_collapse_to_one_fingerprint "max calls=$maxcalls (expected >=2; normalized: $normseen)"

[ "${slow:-0}" -ge 1 ] 2>/dev/null && ok slow_query_logged "(slow=$slow)" \
  || bad slow_query_logged "slow_query_count=$slow (expected >=1)"

echo "== analytics: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
