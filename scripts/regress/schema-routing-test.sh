#!/usr/bin/env bash
# HeliosProxy live test — schema/workload-aware routing (feature: schema-routing).
#
# An analytical (OLAP) query — aggregation + GROUP BY — is routed to the
# configured analytics node, while a simple (OLTP) query uses default routing
# to the primary. The routing decision is asserted via the proxy's per-query
# routing log (the analytics node is disabled/unreachable here, so we assert the
# DECISION, not a returned row).
#
# Usage:  ./schema-routing-test.sh /path/to/heliosdb-proxy  (binary built with
# the `schema-routing` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: schema-routing-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-schema.toml"
IMG="postgres:18.4-bookworm"
BUSER=bench; BPASS=benchpass; BDB=benchdb
PRIMARY_ADDR="127.0.0.1:25433"
ANALYTICS_ADDR="localhost:25433"

OUT="${OUT:-/tmp/regress-schema}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 6432 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== schema-routing live test  bin=$BIN =="
RUST_LOG="helios::routing=debug,helios::schema=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

routed_node_after(){
  sed -n "$(($1+1)),\$p" "$LOG" | sed 's/\x1b\[[0-9;]*m//g' \
    | grep 'routed simple query' | grep -oE 'node=[A-Za-z0-9._-]+:[0-9]+' | head -1 | sed 's/node=//'
}

# 1. an OLAP query (aggregation + GROUP BY) routes to the analytics node.
b=$(wc -l < "$LOG")
pP -tAc "select count(*) from generate_series(1,100) g group by (g % 2)" >/dev/null 2>&1 & probe=$!
sleep 1
olap_node=$(routed_node_after "$b")
wait "$probe" 2>/dev/null
[ "$olap_node" = "$ANALYTICS_ADDR" ] && ok olap_routed_to_analytics_node "($olap_node)" \
  || bad olap_routed_to_analytics_node "routed=[$olap_node] want=$ANALYTICS_ADDR"

# 2. a simple (OLTP) query uses default routing (primary).
b=$(wc -l < "$LOG")
pP -tAc "select 42" >/dev/null 2>&1
sleep 0.3
oltp_node=$(routed_node_after "$b")
[ "$oltp_node" = "$PRIMARY_ADDR" ] && ok oltp_routed_to_primary "($oltp_node)" \
  || bad oltp_routed_to_primary "routed=[$oltp_node] want=$PRIMARY_ADDR"

# distinction: OLAP and OLTP went to different nodes.
if [ "$olap_node" = "$ANALYTICS_ADDR" ] && [ "$oltp_node" = "$PRIMARY_ADDR" ]; then
  ok workload_distinguishes_routing "(OLAP=$olap_node OLTP=$oltp_node)"
else
  bad workload_distinguishes_routing "OLAP=$olap_node OLTP=$oltp_node"
fi

# the OLAP classification was logged.
grep -qi 'OLAP query routed to analytics node' "$LOG" && ok olap_classified_logged || bad olap_classified_logged

echo "== schema-routing: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
