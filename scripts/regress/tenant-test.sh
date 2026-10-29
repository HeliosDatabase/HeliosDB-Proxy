#!/usr/bin/env bash
# HeliosProxy live test — multi-tenancy row isolation (feature: multi-tenancy).
#
# One shared table `mt_data(tid, v)` holds rows for two tenants. The proxy
# identifies the tenant from the connection's application_name and injects a
# `WHERE tid = '<tenant>'` filter, so each tenant sees ONLY its own rows.
#   rows: ('acme',1) ('acme',10) ('globex',2)   -> unfiltered sum = 13
#   tenant acme  sees sum = 11
#   tenant globex sees sum = 2
#
# Then the active-query cap (ac858d61400b): globex may run one request at a
# time (`tenant_max_active_queries = { globex = 1 }`, 300 ms wait), so its
# second concurrent query is refused with 53300 while acme is unaffected, and
# a session holding a session-level advisory lock keeps its permit (a second
# session asking for the same lock is refused instead of blocking on it).
#
# Usage:  ./tenant-test.sh /path/to/heliosdb-proxy  (binary built with the
# `multi-tenancy` feature).
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: tenant-test.sh <proxy-binary>}"
CFG="$HERE/proxy-pg-tenant.toml"
IMG="postgres:18.4-bookworm"
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-tenant}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

# pProxy as a given tenant (application_name); pD = direct backend.
pT(){ app="$1"; shift; docker run --rm --network host -e PGPASSWORD="$BPASS" -e PGAPPNAME="$app" "$IMG" psql -h 127.0.0.1 -p 6432  -U "$BUSER" -d "$BDB" "$@"; }
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" psql -h 127.0.0.1 -p 25433 -U "$BUSER" -d "$BDB" "$@"; }

cleanup(){ pD -tAc "drop table if exists mt_data" >/dev/null 2>&1; [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== multi-tenancy live test  bin=$BIN =="
RUST_LOG="heliosdb_proxy=info,helios::tenant=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 30); do
  if pT acme -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -20 "$LOG"; exit 1; }

# setup directly on the backend (bypassing tenant injection).
pD -tAc "drop table if exists mt_data; create table mt_data(tid text, v int);
         insert into mt_data values ('acme',1),('acme',10),('globex',2)" >/dev/null 2>&1

# control: unfiltered the table sums to 13.
all=$(pD -tAc "select coalesce(sum(v),0) from mt_data" 2>/dev/null | tr -d '[:space:]')
[ "$all" = "13" ] && ok control_unfiltered_sum_is_13 "($all)" || bad control_unfiltered_sum_is_13 "got=$all"

# tenant acme sees only its rows (1 + 10 = 11).
a=$(pT acme -tAc "select coalesce(sum(v),0) from mt_data" 2>/dev/null | tr -d '[:space:]')
[ "$a" = "11" ] && ok acme_sees_only_its_rows "($a)" || bad acme_sees_only_its_rows "got=$a (want 11)"

# tenant globex sees only its row (2).
g=$(pT globex -tAc "select coalesce(sum(v),0) from mt_data" 2>/dev/null | tr -d '[:space:]')
[ "$g" = "2" ] && ok globex_sees_only_its_rows "($g)" || bad globex_sees_only_its_rows "got=$g (want 2)"

# the two tenants genuinely got different views (isolation).
if [ "$a" = "11" ] && [ "$g" = "2" ]; then
  ok tenants_isolated "(acme=$a globex=$g, unfiltered=$all)"
else
  bad tenants_isolated "acme=$a globex=$g"
fi

# the filter injection was logged.
grep -qi 'tenant filter injected' "$LOG" && ok tenant_filter_logged || bad tenant_filter_logged "no log"

# --- active-query cap (ac858d61400b) ---
# globex runs one long query; its second concurrent query waits 300 ms for
# the tenant's only permit, then is refused with 53300.
pT globex -tAc "select pg_sleep(4)" >/dev/null 2>&1 &
SLEEPER=$!
sleep 1.5
err=$(pT globex -v VERBOSITY=verbose -tAc "select 1" 2>&1)
if printf '%s' "$err" | grep -q 53300 \
   && printf '%s' "$err" | grep -q 'too many active queries for tenant "globex"'; then
  ok globex_second_query_refused_53300
  refused=1
else
  bad globex_second_query_refused_53300 "got=$(printf '%s' "$err" | tr '\n' ' ')"
  refused=0
fi
if [ "$refused" = 1 ]; then
  # another tenant is unaffected by globex's cap.
  a=$(pT acme -tAc "select 1" 2>/dev/null | tr -d '[:space:]')
  [ "$a" = "1" ] && ok acme_unaffected_by_globex_cap || bad acme_unaffected_by_globex_cap "got=$a"
  wait "$SLEEPER" 2>/dev/null
  # the permit returned with the long query: globex is served again.
  g=$(pT globex -tAc "select 1" 2>/dev/null | tr -d '[:space:]')
  [ "$g" = "1" ] && ok globex_served_after_release || bad globex_served_after_release "got=$g"
else
  wait "$SLEEPER" 2>/dev/null
  bad acme_unaffected_by_globex_cap "not exercised (no refusal)"
  bad globex_served_after_release "not exercised (no refusal)"
fi

# the refusal was logged (the first one of a tenant is a warning).
grep -q 'active-query cap reached' "$LOG" && ok query_refusal_logged || bad query_refusal_logged "no log"

# A session holding a session-level advisory lock keeps its permit after
# ReadyForQuery 'I' (session pooling): a second globex session asking for the
# same lock is refused at the cap instead of blocking on the lock forever.
holder="$OUT/advisory-holder.out"
pT globex -tAc "select pg_advisory_lock(42)" -c '\! sleep 2' -c "select pg_advisory_unlock(42)" >"$holder" 2>&1 &
HOLDER=$!
sleep 0.7
t0=$(date +%s%N)
err=$(pT globex -v VERBOSITY=verbose -tAc "select pg_advisory_lock(42)" 2>&1)
ms=$(( ($(date +%s%N) - t0) / 1000000 ))
wait "$HOLDER" 2>/dev/null
last=$(tail -n 1 "$holder" | tr -d '[:space:]')
if printf '%s' "$err" | grep -q 53300 && [ "$last" = "t" ]; then
  ok advisory_lock_holder_keeps_its_permit "(second session refused after ${ms} ms)"
else
  bad advisory_lock_holder_keeps_its_permit "second=$(printf '%s' "$err" | tr '\n' ' ') holder_last=$last (${ms} ms)"
fi

echo "== multi-tenancy: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
