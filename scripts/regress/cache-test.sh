#!/usr/bin/env bash
# HeliosProxy live test — query-result cache (feature: query-cache).
#
# Proves the cache genuinely serves reads without a backend round-trip, and
# that writes invalidate it. Trick: after a value is cached, it is mutated
# DIRECTLY on the backend (bypassing the proxy); a proxy read that returns the
# stale cached value can only have come from the cache.
#   1. read v -> caches it (L2, shared across connections);
#   2. mutate v directly on the backend;
#   3. proxy read returns the STALE cached value (cache hit, no backend trip);
#   4. a write THROUGH the proxy invalidates the table's cache;
#   5. the next proxy read returns the fresh value.
#
# Usage:  ./cache-test.sh /path/to/heliosdb-proxy  (binary built with the
# `query-cache` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: cache-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-cache.toml"
IMG="postgres:18.4-bookworm"
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-cache}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

# pP -> through the proxy (:6432); pD -> direct to the backend (:25433).
pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6432  -U "$BUSER" -d "$BDB" "$@"; }
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ pP -tAc "drop table if exists cache_probe" >/dev/null 2>&1; [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== query-cache live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info,helios::cache=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# setup: a one-row table with v=1.
pP -tAc "drop table if exists cache_probe; create table cache_probe(v int); insert into cache_probe values (1)" >/dev/null 2>&1

# 1. first read: cache miss -> caches v=1.
r1=$(pP -tAc "select v from cache_probe" 2>/dev/null | tr -d '[:space:]')
[ "$r1" = "1" ] && ok first_read_caches "($r1)" || bad first_read_caches "got=$r1"

# 2. mutate the value directly on the backend (the proxy/cache never sees it).
pD -tAc "update cache_probe set v = 2" >/dev/null 2>&1
# sanity: the backend really is 2 now.
bk=$(pD -tAc "select v from cache_probe" 2>/dev/null | tr -d '[:space:]')

# 3. read through the proxy: a cache HIT returns the stale cached value (1),
#    proving the result came from cache, not the backend (which is now 2).
r2=$(pP -tAc "select v from cache_probe" 2>/dev/null | tr -d '[:space:]')
[ "$r2" = "1" ] && ok cache_hit_serves_stale "(proxy=$r2, backend=$bk)" \
  || bad cache_hit_serves_stale "proxy=$r2 backend=$bk (want cached 1)"

# 4. a write THROUGH the proxy invalidates the cache for cache_probe.
pP -tAc "update cache_probe set v = 3" >/dev/null 2>&1

# 5. read through the proxy: cache was invalidated -> backend -> 3.
r3=$(pP -tAc "select v from cache_probe" 2>/dev/null | tr -d '[:space:]')
[ "$r3" = "3" ] && ok write_invalidates_cache "($r3)" || bad write_invalidates_cache "got=$r3 (want 3)"

# 6. the proxy logged at least one cache hit.
grep -qi 'cache hit' "$LOG" && ok cache_hit_logged || bad cache_hit_logged "no 'cache hit' in proxy log"

echo "== query-cache: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
