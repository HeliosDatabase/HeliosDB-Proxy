#!/usr/bin/env bash
# HeliosProxy live test — conditional reset (pool_mode.skip_clean_reset).
#
# With skip_clean_reset ON, Transaction pooling parks a *provably clean*
# connection WITHOUT running DISCARD ALL (saving a backend round-trip), but MUST
# still fully reset any connection that touched session state. This test proves
# BOTH directions against a real PostgreSQL backend:
#
#   POSITIVE  — a clean autocommit SELECT workload actually skips the reset
#               (the optimisation engages), and connections are reused.
#   NO-LEAK   — for each session-state category (temp table, prepared statement,
#               GUC/SET, LISTEN), state created by client A does NOT leak to a
#               client B that reuses A's pooled connection. A classifier that
#               wrongly called any of these "clean" would leak → FAIL.
#
# Each leak probe only asserts when it can confirm client B actually reused a
# pooled connection (via the proxy's reuse log); otherwise it SKIPs as
# inconclusive rather than passing vacuously.
#
# Usage:  ./conditional-reset-test.sh /path/to/heliosdb-proxy   (default features)
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: conditional-reset-test.sh <proxy-binary>}"
IMG="postgres:18.4-bookworm"
PXHOST=127.0.0.1; PXPORT=6432
BHOST=127.0.0.1; BPORT=25433
BUSER=bench; BPASS=benchpass; BDB=benchdb

OUT="${OUT:-/tmp/regress-conditional-reset}"; mkdir -p "$OUT"
LOG="$OUT/proxy.log"
CFG="$OUT/proxy.toml"
PASS=0; FAIL=0; SKIP=0
ok(){   PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){  FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }
skip(){ SKIP=$((SKIP+1)); printf '  \033[33mSKIP\033[0m %s %s\n' "$1" "${2:-}"; }

pP(){ docker run --rm --network host -e PGPASSWORD="$BPASS" -v "$OUT":/w "$IMG" \
        psql -h $PXHOST -p $PXPORT -U "$BUSER" -d "$BDB" "$@"; }
pD(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
        psql -h $BHOST -p $BPORT -U "$BUSER" -d "$BDB" "$@"; }
reuse_count(){ local n; n=$(grep -c 'reused pooled backend connection' "$LOG" 2>/dev/null); echo "${n:-0}"; }

cat > "$CFG" <<EOF
listen_address = "$PXHOST:$PXPORT"
admin_address  = "$PXHOST:9099"
tr_enabled = false
tr_mode    = "none"

[pool]
min_connections = 1
max_connections = 20
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 5
test_on_acquire = true

[pool_mode]
mode = "transaction"
max_pool_size = 20
reset_query = "DISCARD ALL"
skip_clean_reset = true

[load_balancer]
read_strategy = "round_robin"
read_write_split = false
latency_threshold_ms = 100

[health]
check_interval_secs = 30
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"

[[nodes]]
host = "$BHOST"
port = $BPORT
role = "primary"
weight = 100
enabled = true
name = "pg-primary"
EOF

cleanup(){ [ -n "${PROXYPID:-}" ] && kill "$PROXYPID" 2>/dev/null; wait "${PROXYPID:-}" 2>/dev/null; }
trap cleanup EXIT

echo "== conditional-reset (skip_clean_reset) live test  bin=$BIN =="
# Clear any persistent leftovers from a prior run (temp/prepared/GUC are
# session-scoped, but be defensive about a real table).
pD -tAc "DROP TABLE IF EXISTS leak_persist" >/dev/null 2>&1

RUST_LOG="heliosdb_proxy=debug,helios=debug" NO_COLOR=1 "$BIN" --config "$CFG" >"$LOG" 2>&1 &
PROXYPID=$!
ready=0
for _ in $(seq 1 40); do
  if pP -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
[ "$ready" = 1 ] || { echo "proxy never became ready"; tail -30 "$LOG"; exit 1; }

# ---- POSITIVE: a clean SELECT workload skips the reset --------------------
printf 'select %s;\n' 1 2 3 4 5 6 > "$OUT/clean.sql"
pP -tA -f /w/clean.sql >/dev/null 2>&1
skipped=$(grep -c 'reset skipped' "$LOG" 2>/dev/null); skipped=${skipped:-0}
[ "$skipped" -ge 1 ] && ok skip_engages_on_clean "(reset skipped x$skipped)" \
  || bad skip_engages_on_clean "clean SELECTs did not skip any reset (expected the optimisation to engage)"

# ---- Helper: a 2-statement session (create state, then probe on the REUSED
# connection). Statement 1 dirties (or not) the connection; at its ReadyForQuery
# the proxy releases+reparks it; statement 2 checks it back OUT of the pool — so
# statement 2 runs on the same physical backend connection statement 1 used,
# after whatever reset decision was made. This deterministically exercises the
# reset-or-skip path (the exact mechanism that prevents cross-client leakage):
# a classifier that wrongly skipped the reset would let statement 2 observe
# statement 1's session state. Sets REUSED=1 iff statement 2 reused a pooled
# conn; writes the session output to $OUT/probe_out.
REUSED=0
probe(){
  local setup=$1 probe_sql=$2 before after
  printf '%s;\n%s;\n' "$setup" "$probe_sql" > "$OUT/probe.sql"
  before=$(reuse_count)
  pP -v ON_ERROR_STOP=0 -tA -f /w/probe.sql > "$OUT/probe_out" 2>&1
  after=$(reuse_count)
  if [ "$after" -gt "$before" ]; then REUSED=1; else REUSED=0; fi
}

# ---- NO-LEAK: temp table -------------------------------------------------
probe "CREATE TEMP TABLE leak_tmp(x int)" "SELECT 'TMPROWS='||count(*) FROM leak_tmp"; out=$(cat "$OUT/probe_out")
if [ "$REUSED" != 1 ]; then skip leak_temp_table "(2nd stmt did not reuse the pooled conn)"
elif printf '%s' "$out" | grep -qiE 'does not exist'; then
  ok leak_temp_table "(temp table cleared before reuse)"
elif printf '%s' "$out" | grep -q 'TMPROWS='; then
  bad leak_temp_table "LEAK — temp table survived onto the reused conn: $out"
else ok leak_temp_table "(no leak; $out)"; fi

# ---- NO-LEAK: prepared statement -----------------------------------------
probe "PREPARE leak_ps AS SELECT 424242" "EXECUTE leak_ps"; out=$(cat "$OUT/probe_out")
if [ "$REUSED" != 1 ]; then skip leak_prepared_stmt "(2nd stmt did not reuse the pooled conn)"
elif printf '%s' "$out" | grep -qiE 'does not exist'; then
  ok leak_prepared_stmt "(prepared statement cleared before reuse)"
elif printf '%s' "$out" | grep -q '424242'; then
  bad leak_prepared_stmt "LEAK — prepared statement survived onto the reused conn: $out"
else ok leak_prepared_stmt "(no leak; $out)"; fi

# ---- NO-LEAK: GUC / SET --------------------------------------------------
probe "SET work_mem = '55555kB'" "SHOW work_mem"; out=$(cat "$OUT/probe_out")
if [ "$REUSED" != 1 ]; then skip leak_guc_set "(2nd stmt did not reuse the pooled conn)"
elif printf '%s' "$out" | grep -q '55555kB'; then
  bad leak_guc_set "LEAK — work_mem survived onto the reused conn: $out"
else ok leak_guc_set "(GUC cleared before reuse; work_mem=$(printf '%s' "$out" | tail -1))"; fi

# ---- NO-LEAK: LISTEN -----------------------------------------------------
probe "LISTEN leak_chan" "SELECT 'LC='||count(*) FROM pg_listening_channels() c WHERE c = 'leak_chan'"; out=$(cat "$OUT/probe_out")
if [ "$REUSED" != 1 ]; then skip leak_listen "(2nd stmt did not reuse the pooled conn)"
elif printf '%s' "$out" | grep -q 'LC=0'; then
  ok leak_listen "(LISTEN registration cleared before reuse)"
elif printf '%s' "$out" | grep -q 'LC=1'; then
  bad leak_listen "LEAK — LISTEN registration survived onto the reused conn: $out"
else ok leak_listen "(no leak; $out)"; fi

# ---- CORRECTNESS: results still correct through the optimisation ----------
got=$(pP -tAc "select 40 + 2" 2>/dev/null | tr -d '[:space:]')
[ "$got" = "42" ] && ok results_correct "($got)" || bad results_correct "got '$got'"

echo "== conditional-reset: PASS=$PASS FAIL=$FAIL SKIP=$SKIP =="
echo "   (pool log sample:)"; grep -E 'helios::pool' "$LOG" | tail -6 | sed 's/^/   /'
[ "$FAIL" -eq 0 ]
