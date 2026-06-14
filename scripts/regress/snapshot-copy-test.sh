#!/usr/bin/env bash
# COPY-based bulk snapshot test (Batch G2).
#
# Snapshots existing rows from a source DB to a DISTINCT target DB via
# POST /api/migration/snapshot, exercising the COPY ... FROM STDIN bulk-load
# path and its per-row INSERT fallback (forced with HELIOS_SNAPSHOT_USE_COPY=0).
# Self-contained: source = PG benchdb, target = PG `postgres` db (both support
# COPY). Adversarial data (tab/newline/backslash/NULL/literal "\N") proves the
# COPY text encoding round-trips and the COPY path == the INSERT path.
set -u
BIN="${1:-./target/release/heliosdb-proxy}"
IMG="postgres:18.4-bookworm"
OUT="${OUT:-/tmp/snapshot-copy}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }

src(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 25433 -U bench -d benchdb  -tAc "$1" 2>&1; }
tgt(){ docker run --rm --network host -e PGPASSWORD=benchpass "$IMG" psql -h 127.0.0.1 -p 25433 -U bench -d postgres -tAc "$1" 2>&1; }

# 1. Seed adversarial source data (never seen by the write-tail).
src "DROP TABLE IF EXISTS _snapcopy" >/dev/null
src "CREATE TABLE _snapcopy(id int, t text)" >/dev/null
src "INSERT INTO _snapcopy VALUES (1,'plain'), (2, E'tab\there'), (3, E'line1\nline2'), (4, E'back\\\\slash'), (5, NULL), (6, '\\N'), (7, '')" >/dev/null
srccount=$(src "SELECT count(*) FROM _snapcopy" | tr -d '[:space:]')

cat > "$OUT/proxy.toml" <<EOF
listen_address = "127.0.0.1:6432"
admin_address  = "127.0.0.1:9099"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 30
[mirror]
enabled = true
writes_only = true
backend_host = "127.0.0.1"
backend_port = 25433
backend_user = "bench"
backend_password = "benchpass"
backend_database = "postgres"
source_host = "127.0.0.1"
source_port = 25433
source_user = "bench"
source_password = "benchpass"
source_database = "benchdb"
[pool]
min_connections = 2
max_connections = 50
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 5
test_on_acquire = true
[load_balancer]
read_strategy = "round_robin"
read_write_split = false
latency_threshold_ms = 100
[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"
[[nodes]]
host = "127.0.0.1"
port = 25433
role = "primary"
weight = 100
enabled = true
name = "pg"
EOF

verify_target(){ # $1 = label
  local label="$1"
  local n; n=$(tgt "SELECT count(*) FROM _snapcopy" | tr -d '[:space:]')
  [ "$n" = "$srccount" ] && ok "$label: row count matches source ($n)" || { bad "$label: count $n != $srccount"; return; }
  # adversarial fidelity
  local tabv nl bs nullid litn empt
  tabv=$(tgt "SELECT t = E'tab\there' FROM _snapcopy WHERE id=2")
  nl=$(tgt   "SELECT t = E'line1\nline2' FROM _snapcopy WHERE id=3")
  bs=$(tgt   "SELECT t = E'back\\\\slash' FROM _snapcopy WHERE id=4")
  nullid=$(tgt "SELECT t IS NULL FROM _snapcopy WHERE id=5")
  litn=$(tgt  "SELECT t = '\\N' FROM _snapcopy WHERE id=6")          # literal backslash-N, NOT null
  empt=$(tgt  "SELECT t = '' FROM _snapcopy WHERE id=7")
  if [ "$tabv" = t ] && [ "$nl" = t ] && [ "$bs" = t ] && [ "$nullid" = t ] && [ "$litn" = t ] && [ "$empt" = t ]; then
    ok "$label: tab/newline/backslash/NULL/literal-\\N/empty all round-trip"
  else
    bad "$label: fidelity tab=$tabv nl=$nl bs=$bs null=$nullid litN=$litn empty=$empt"
  fi
}

run_snapshot(){ # $1=label  $2=extra env (USE_COPY)
  tgt "DROP TABLE IF EXISTS _snapcopy" >/dev/null 2>&1
  : > "$OUT/proxy.log"
  env $2 "$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 & local P=$!
  for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9099/health" && break; sleep 0.3; done
  local resp; resp=$(curl -s -X POST "http://127.0.0.1:9099/api/migration/snapshot" -d '{"tables":["_snapcopy"]}')
  echo "  [$1] resp: $resp"
  echo "$resp" | grep -q "\"rows_copied\":$srccount" && ok "$1: snapshot reports $srccount rows copied" || bad "$1: resp $resp"
  verify_target "$1"
  echo "$3"  # extra per-mode log assertion handled by caller via $OUT/proxy.log
  kill "$P" 2>/dev/null; wait "$P" 2>/dev/null
}

# 2. COPY path (default).
run_snapshot "COPY" "" ""
grep -qi "falling back" "$OUT/proxy.log" && bad "COPY: unexpectedly fell back to INSERT" || ok "COPY: used the COPY path (no fallback in log)"

# 3. Forced-INSERT fallback path (kill-switch) — identical result.
run_snapshot "INSERT" "HELIOS_SNAPSHOT_USE_COPY=0" ""

# 4. Idempotency fence: target now has rows (from step 3); a re-snapshot must be
#    REFUSED with no duplication (non-destructive default).
"$BIN" --config "$OUT/proxy.toml" >"$OUT/proxy.log" 2>&1 & FP=$!
for i in $(seq 1 30); do curl -s -o /dev/null "http://127.0.0.1:9099/health" && break; sleep 0.3; done
before=$(tgt "SELECT count(*) FROM _snapcopy" | tr -d '[:space:]')
resp=$(curl -s -X POST "http://127.0.0.1:9099/api/migration/snapshot" -d '{"tables":["_snapcopy"]}')
after=$(tgt "SELECT count(*) FROM _snapcopy" | tr -d '[:space:]')
echo "  [fence] before=$before resp=$resp after=$after"
{ echo "$resp" | grep -qi "refusing snapshot" && echo "$resp" | grep -q '"ok":false'; } && ok "fence: re-snapshot of a non-empty target is refused" || bad "fence: not refused: $resp"
{ [ "$before" = "$after" ] && [ "$after" = "$srccount" ]; } && ok "fence: target unchanged, no duplication (still $after)" || bad "fence: rows changed $before->$after"
kill "$FP" 2>/dev/null; wait "$FP" 2>/dev/null

# cleanup
src "DROP TABLE IF EXISTS _snapcopy" >/dev/null 2>&1
tgt "DROP TABLE IF EXISTS _snapcopy" >/dev/null 2>&1
echo "== snapshot-copy test: PASS=$PASS FAIL=$FAIL =="
exit $FAIL
