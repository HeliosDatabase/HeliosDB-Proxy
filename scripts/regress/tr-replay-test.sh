#!/usr/bin/env bash
# HeliosProxy live test — Transaction Replay (feature: ha-tr).
#
# Proves the write journal is populated on the query path AND that the journaled
# writes can be re-applied via the replay engine (the same machinery the
# failover path uses to bring a promoted primary up to date):
#   1. write rows through the proxy (tr_enabled -> journaled, and applied);
#   2. empty the table directly on the backend (the journal still has the writes);
#   3. POST /api/replay over the window -> the journaled INSERT is re-executed
#      on the target and the rows reappear.
#
# Usage:  ./tr-replay-test.sh /path/to/heliosdb-proxy  (binary built with the
# `ha-tr` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: tr-replay-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-tr.toml"
IMG="postgres:18.4-bookworm"
ADMIN=127.0.0.1:9099
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-tr}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6432  -U "$BUSER" -d "$BDB" "$@"; }
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ pD -tAc "drop table if exists tr_src" >/dev/null 2>&1; [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== transaction-replay live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

FROM=$(date -u -d '-1 hour' +%Y-%m-%dT%H:%M:%SZ)

# setup: empty table directly on the backend.
pD -tAc "drop table if exists tr_src; create table tr_src(v int)" >/dev/null 2>&1

# 1. write through the proxy -> journaled (and applied).
pP -tAc "insert into tr_src values (1),(2),(3)" >/dev/null 2>&1
applied=$(pD -tAc "select count(*) from tr_src" 2>/dev/null | tr -d '[:space:]')
[ "$applied" = "3" ] && ok write_applied_through_proxy "($applied)" || bad write_applied_through_proxy "got=$applied"

# 2. empty the table on the backend (the journal still holds the INSERT).
pD -tAc "truncate tr_src" >/dev/null 2>&1
empty=$(pD -tAc "select count(*) from tr_src" 2>/dev/null | tr -d '[:space:]')
[ "$empty" = "0" ] && ok table_emptied "($empty)" || bad table_emptied "got=$empty"

TO=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)

# 3. replay the journaled window onto the backend.
body=$(printf '{"from":"%s","to":"%s","target_host":"127.0.0.1","target_port":25433,"target_user":"%s","target_password":"%s","target_database":"%s"}' \
  "$FROM" "$TO" "$BUSER" "$BPASS" "$BDB")
resp=$(curl -s -X POST "http://$ADMIN/api/replay" -H 'Content-Type: application/json' -d "$body")
echo "  replay resp: $(printf '%s' "$resp" | head -c 200)"
printf '%s' "$resp" | grep -qiE 'execut|statement|replay' && ok replay_endpoint_ran || bad replay_endpoint_ran "resp=$resp"

# 4. the journaled writes were re-applied -> rows reappear.
sleep 0.5
after=$(pD -tAc "select count(*) from tr_src" 2>/dev/null | tr -d '[:space:]')
[ "$after" = "3" ] && ok journaled_writes_replayed "($after)" || bad journaled_writes_replayed "got=$after (want 3)"

echo "== transaction-replay: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
