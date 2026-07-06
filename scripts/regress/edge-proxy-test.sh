#!/usr/bin/env bash
# HeliosProxy live two-region test — edge-proxy (feature: edge-proxy).
#
# Two proxy processes against one real PG:
#   HOME (role=home, :6440, admin :9440) — backend = PG :25433; authoritative
#     cache; mints versions; on writes it invalidates + broadcasts SSE.
#   EDGE (role=edge, :6441, admin :9441) — backend = the HOME's :6440; serves
#     reads from its own cache; subscribes to the home admin's SSE stream.
#
# Proves the full loop end-to-end:
#   1. a read through the edge is served from the edge's local cache on repeat
#      (edge cache `hits` increments; a value mutated DIRECTLY on PG stays
#      stale through the edge — the reply cannot have come from the backend);
#   2. the edge holds a live SSE subscription (home /api/edge lists it);
#   3. a write through the HOME broadcasts an SSE invalidation that reaches the
#      edge (edge `invalidations_received` increments) and the next identical
#      read through the edge reflects the write — WITHIN the SSE window, well
#      under the 60s TTL, so TTL expiry cannot explain it;
#   4. a write through the EDGE forwards over PG-wire to the home and lands on
#      PG (visible directly), and reads through the edge then reflect it.
#
# Usage:  ./edge-proxy-test.sh /path/to/heliosdb-proxy   (built --features
# edge-proxy). Requires docker (psql), curl, python3, and PG on 127.0.0.1:25433.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: edge-proxy-test.sh <proxy-binary>}"
HOME_CFG="$HERE/proxy-edge-home.toml"
EDGE_CFG="$HERE/proxy-edge-edge.toml"
IMG="postgres:18.4-bookworm"
BUSER=bench; BPASS=benchpass; BDB=benchdb
TOKEN="edge-e2e-secret"

OUT="${OUT:-/tmp/regress-edge}"; mkdir -p "$OUT"
HLOG="$OUT/home.log"; ELOG="$OUT/edge.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

# psql helpers: pD=direct PG, pH=through home, pE=through edge. PG uses SCRAM;
# the proxies relay it transparently, so the client presents the real password
# (a passing pE also proves end-to-end SCRAM across both proxy hops).
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }
pH(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6440  -U "$BUSER" -d "$BDB" "$@"; }
pE(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6441  -U "$BUSER" -d "$BDB" "$@"; }
# admin GET helpers (home needs the bearer; edge admin is open on loopback).
aH(){ curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:9440$1"; }
aE(){ curl -s "http://127.0.0.1:9441$1"; }
# extract a nested field from JSON on stdin: jget '["cache"]["hits"]'
jget(){ python3 -c "import sys,json
try: d=json.load(sys.stdin); print(eval('d'+sys.argv[1]))
except Exception as e: print('ERR:'+str(e))" "$1"; }

cleanup(){
  pD -tAc "drop table if exists edge_probe" >/dev/null 2>&1
  [ -n "${EDGEPID:-}" ] && kill "$EDGEPID" 2>/dev/null; wait "${EDGEPID:-}" 2>/dev/null
  [ -n "${HOMEPID:-}" ] && kill "$HOMEPID" 2>/dev/null; wait "${HOMEPID:-}" 2>/dev/null
}
trap cleanup EXIT

# Pre-flight: the proxy binds with SO_REUSEPORT, so a stray proxy left on any of
# these ports by an earlier run would silently JOIN the load-balancing group —
# the edge would subscribe to one home process but its writes could land on the
# other (registry has no edge → invalidations lost). Kill anything holding them.
for port in 6440 6441 9440 9441; do
  for pid in $(ss -ltnHp "sport = :$port" 2>/dev/null | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -u); do
    echo "  pre-flight: killing stray pid $pid on :$port"
    kill "$pid" 2>/dev/null
  done
done
sleep 1

echo "== edge-proxy two-region live test  bin=$BIN =="

# ── start HOME, wait until it proxies to PG ─────────────────────────────
RUST_LOG="heliosdb_proxy=info,helios::edge=debug" NO_COLOR=1 "$BIN" --config "$HOME_CFG" >"$HLOG" 2>&1 &
HOMEPID=$!
ready=0
for _ in $(seq 1 40); do
  pH -tAc "select 1" >/dev/null 2>&1 && { ready=1; break; }
  kill -0 "$HOMEPID" 2>/dev/null || { echo "home died on startup:"; tail -30 "$HLOG"; exit 1; }
  sleep 0.5
done
[ "$ready" = 1 ] && ok home_ready || { bad home_ready; tail -30 "$HLOG"; exit 1; }

# ── start EDGE, wait until it proxies (edge -> home -> PG) ───────────────
RUST_LOG="heliosdb_proxy=info,helios::edge=debug" NO_COLOR=1 "$BIN" --config "$EDGE_CFG" >"$ELOG" 2>&1 &
EDGEPID=$!
ready=0
for _ in $(seq 1 40); do
  pE -tAc "select 1" >/dev/null 2>&1 && { ready=1; break; }
  kill -0 "$EDGEPID" 2>/dev/null || { echo "edge died on startup:"; tail -30 "$ELOG"; exit 1; }
  sleep 0.5
done
[ "$ready" = 1 ] && ok edge_ready || { bad edge_ready; tail -30 "$ELOG"; exit 1; }

# ── wait for the edge's SSE subscription to register at the home ─────────
reg=0
for _ in $(seq 1 30); do
  n=$(aH /api/edge | jget "['registered']" 2>/dev/null)
  echo "$n" | grep -q "edge-e2e-1" && { reg=1; break; }
  sleep 0.5
done
[ "$reg" = 1 ] && ok edge_registered_via_sse "($(aH /api/edge | jget "[\"registered\"][0][\"edge_id\"]"))" \
  || bad edge_registered_via_sse "home /api/edge registered=$(aH /api/edge | jget "['registered']")"

# ── setup: one-row table v=1 (created on PG through the home) ────────────
pH -tAc "drop table if exists edge_probe; create table edge_probe(v int); insert into edge_probe values (1)" >/dev/null 2>&1

# 1. first read through the edge -> miss -> forwards to home -> caches v=1.
r1=$(pE -tAc "select v from edge_probe" 2>/dev/null | tr -d '[:space:]')
[ "$r1" = "1" ] && ok edge_read_caches "($r1)" || bad edge_read_caches "got=$r1"
h0=$(aE /api/edge | jget "['cache']['hits']")

# 2. mutate DIRECTLY on PG (neither proxy sees it); edge read must stay stale
#    from its local cache, and the edge `hits` counter must advance.
pD -tAc "update edge_probe set v = 2" >/dev/null 2>&1
bk=$(pD -tAc "select v from edge_probe" 2>/dev/null | tr -d '[:space:]')
r2=$(pE -tAc "select v from edge_probe" 2>/dev/null | tr -d '[:space:]')
h1=$(aE /api/edge | jget "['cache']['hits']")
[ "$r2" = "1" ] && ok edge_cache_hit_serves_stale "(edge=$r2 backend=$bk)" \
  || bad edge_cache_hit_serves_stale "edge=$r2 backend=$bk (want cached 1)"
[ "${h1:-0}" -gt "${h0:-0}" ] 2>/dev/null && ok edge_hit_counter_advanced "($h0->$h1)" \
  || bad edge_hit_counter_advanced "hits $h0 -> $h1"

# 3. write THROUGH THE HOME -> home invalidates + broadcasts SSE -> the edge
#    drops the entry. Poll the edge read briefly; a refresh well under the 60s
#    TTL proves the SSE invalidation (not TTL expiry) drove it.
inv0=$(aE /api/edge | jget "['cache']['invalidations_received']")
pH -tAc "update edge_probe set v = 3" >/dev/null 2>&1
r3=""; for _ in $(seq 1 20); do
  r3=$(pE -tAc "select v from edge_probe" 2>/dev/null | tr -d '[:space:]')
  [ "$r3" = "3" ] && break
  sleep 0.25
done
[ "$r3" = "3" ] && ok sse_invalidation_refetch "($r3, <5s so not TTL)" \
  || bad sse_invalidation_refetch "got=$r3 (want 3)"
inv1=$(aE /api/edge | jget "['cache']['invalidations_received']")
[ "${inv1:-0}" -gt "${inv0:-0}" ] 2>/dev/null && ok edge_invalidations_received "($inv0->$inv1)" \
  || bad edge_invalidations_received "invalidations_received $inv0 -> $inv1"

# 4. write THROUGH THE EDGE -> forwards over PG-wire to home -> PG. Visible
#    directly on PG, and reads through the edge reflect it.
pE -tAc "update edge_probe set v = 4" >/dev/null 2>&1
bk4=$(pD -tAc "select v from edge_probe" 2>/dev/null | tr -d '[:space:]')
[ "$bk4" = "4" ] && ok edge_write_forwards_to_pg "($bk4)" || bad edge_write_forwards_to_pg "backend=$bk4 (want 4)"
r4=""; for _ in $(seq 1 20); do
  r4=$(pE -tAc "select v from edge_probe" 2>/dev/null | tr -d '[:space:]')
  [ "$r4" = "4" ] && break
  sleep 0.25
done
[ "$r4" = "4" ] && ok edge_reads_own_write "($r4)" || bad edge_reads_own_write "got=$r4 (want 4)"

# 5. log evidence: home broadcast on write, edge applied an invalidation.
grep -qiE 'broadcast|notif|invalidat' "$HLOG" && ok home_logged_broadcast \
  || bad home_logged_broadcast "no broadcast/invalidate line in home log"
grep -qiE 'invalidat|subscrib' "$ELOG" && ok edge_logged_invalidation \
  || bad edge_logged_invalidation "no invalidate/subscribe line in edge log"

echo "== edge-proxy: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
