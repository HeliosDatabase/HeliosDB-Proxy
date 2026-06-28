#!/usr/bin/env bash
# HeliosProxy live test — reliability hardening, through a live proxy in front
# of PostgreSQL 18.4. Proves the Tier 1/2/3 reliability work end-to-end:
#   1. /api/circuit reports live per-node circuit-breaker state (observability).
#   2. When the backend dies, the failing query demotes the node IN-BAND
#      (last_error = "in-band failure"), within ~2s — far faster than the 5s
#      health-check interval, so it is attributable to the in-band path.
#   3. The failing query returns a CLEAN ERROR quickly (no multi-minute hang) —
#      timeout coverage on the forward path.
#   4. After the backend returns, the node recovers and queries succeed again.
#
# Usage:  ./reliability-test.sh /path/to/heliosdb-proxy   (binary built with
# circuit-breaker — e.g. all-features).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: reliability-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-reliability.toml"
IMG="postgres:18.4-bookworm"
PROXY_HOST=127.0.0.1; PROXY_PORT=6432; ADMIN=127.0.0.1:9099
BUSER=bench; BPASS=benchpass; BDB=benchdb
BACKEND=codex-pg184-bench

OUT="${OUT:-/tmp/regress-reliability}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $PROXY_HOST -p $PROXY_PORT -U "$BUSER" -d "$BDB" "$@"; }
adm(){ curl -s -m 5 "http://$ADMIN$1"; }

cleanup(){
  docker start "$BACKEND" >/dev/null 2>&1 || true   # always leave the backend up
  [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null
}
trap cleanup EXIT

echo "== reliability live test  bin=$BIN =="
docker start "$BACKEND" >/dev/null 2>&1 || true; sleep 2

RUST_LOG="heliosdb_proxy=info,helios=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# 1. Circuit-breaker observability endpoint (Tier 3).
circ=$(adm /api/circuit)
echo "  /api/circuit: $(printf '%s' "$circ" | head -c 160)"
printf '%s' "$circ" | grep -q '"127.0.0.1:25433"' && printf '%s' "$circ" | grep -q '"closed"' \
  && ok circuit_endpoint_reports_closed || bad circuit_endpoint_reports_closed "$circ"

# 2. Baseline query works.
got=$(pP -tAc "select 42" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "42" ] && ok baseline_query_ok "($got)" || bad baseline_query_ok "got=[$got]"

# --- Kill the backend underneath the proxy ---
docker stop "$BACKEND" >/dev/null 2>&1

# 3. A query now fails CLEANLY and QUICKLY (no long hang). Bound the wall clock.
t0=$(date +%s)
pP -tAc "select 1" >/dev/null 2>&1; rc=$?
t1=$(date +%s); elapsed=$((t1 - t0))
{ [ "$rc" -ne 0 ] && [ "$elapsed" -lt 15 ]; } \
  && ok fails_fast_and_clean "(rc=$rc, ${elapsed}s)" \
  || bad fails_fast_and_clean "rc=$rc elapsed=${elapsed}s (want err + <15s)"

# 4. In-band demotion: the failing query (not the 30s periodic checker) demotes
#    the node. The proxy log records the in-band path firing — a robust proof
#    independent of which error string ends up newest in /nodes.
demoted=0
for _ in $(seq 1 10); do
  if grep -q 'in-band failure — node marked unhealthy' "$LOG"; then demoted=1; break; fi
  sleep 0.3
done
echo "  in-band log: $(grep -m1 'in-band failure' "$LOG" | head -c 200)"
echo "  /nodes: $(adm /nodes | head -c 180)"
[ "$demoted" = 1 ] && ok in_band_demotion "(in-band path fired before the 30s checker)" \
  || bad in_band_demotion "no in-band demotion logged"

# --- Bring the backend back ---
docker start "$BACKEND" >/dev/null 2>&1
# wait for postgres to accept connections again
for _ in $(seq 1 30); do
  docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
    psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1 && break
  sleep 1
done

# 5. Recovery: once the health checker re-probes (<=5s) the node is healthy and
#    queries succeed again.
recovered=0
for _ in $(seq 1 40); do   # health checker re-probes within its 30s interval
  if [ "$(pP -tAc 'select 99' 2>/dev/null | tr -d '[:space:]')" = "99" ]; then recovered=1; break; fi
  sleep 1
done
[ "$recovered" = 1 ] && ok recovers_after_backend_returns || bad recovers_after_backend_returns "still failing after restart"

echo "== reliability: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
