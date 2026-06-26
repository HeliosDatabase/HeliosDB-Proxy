#!/usr/bin/env bash
# HeliosProxy live test — GraphQL-to-SQL gateway (feature: graphql-gateway).
#
# Proves the gateway genuinely executes a GraphQL query against the backend and
# returns real rows (the engine previously returned null/empty for every field):
#   - backend table gqlitem(id, name) = (1,'alice'), (2,'bob')
#   - POST { gqlitems { id name } } -> {"data":{"gqlitems":[{id,name}...]}}
#
# Usage:  ./graphql-test.sh /path/to/heliosdb-proxy  (binary built with the
# `graphql-gateway` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: graphql-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-graphql.toml"
IMG="postgres:18.4-bookworm"
GQL=127.0.0.1:19091
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-graphql}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ pD -tAc "drop table if exists gqlitems" >/dev/null 2>&1; [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== graphql-gateway live test  bin=$BIN =="
# seed the backend table BEFORE the gateway starts.
pD -tAc "drop table if exists gqlitems; create table gqlitems(id int, name text);
         insert into gqlitems values (1,'alice'),(2,'bob')" >/dev/null 2>&1

RUST_LOG="heliosdb_proxy=info" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if curl -s "http://$GQL/health" 2>/dev/null | grep -q ok; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "graphql gateway never became ready"; tail -30 "$LOG"; exit 1; }

# 1. POST a GraphQL list query; expect real rows shaped under "gqlitems".
resp=$(curl -s -X POST "http://$GQL/" -H 'Content-Type: application/json' \
  -d '{"query":"{ gqlitems { id name } }"}')
echo "  resp: $(printf '%s' "$resp" | head -c 300)"

printf '%s' "$resp" | grep -q 'alice' && ok returns_real_row_alice || bad returns_real_row_alice "no alice in: $resp"
printf '%s' "$resp" | grep -q 'bob'   && ok returns_real_row_bob   || bad returns_real_row_bob   "no bob"
printf '%s' "$resp" | grep -q '"gqlitems"' && ok shaped_under_field_key || bad shaped_under_field_key "no gqlitems key"

# 2. data is non-null (the old engine returned null for every field).
nrows=$(printf '%s' "$resp" | jq -r '.data.gqlitems | length' 2>/dev/null)
[ "${nrows:-0}" -ge 2 ] 2>/dev/null && ok two_rows_returned "(rows=$nrows)" \
  || bad two_rows_returned "rows=$nrows (want >=2)"

echo "== graphql-gateway: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
