#!/usr/bin/env bash
# HeliosProxy vs PgBouncer — Side-by-Side Failover Comparison
# Runs identical workloads through both proxies, kills both primaries
# simultaneously, and compares the results.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HP_CONNSTR="postgresql://app:apppass@localhost:56432/benchdb"
PB_CONNSTR="postgresql://app:apppass@localhost:56532/benchdb"
HP_DIRECT="postgresql://app:apppass@localhost:55432/benchdb"
PB_DIRECT="postgresql://app:apppass@localhost:55532/benchdb"

CONCURRENCY=20
WARMUP=30
RECOVERY_WAIT=30

RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       HeliosProxy vs PgBouncer — Failover Comparison        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ─── Step 1: Start both clusters ─────────────────────────────────────
echo "[Step 1] Starting both clusters..."
docker compose up -d

echo "Waiting for all services to be healthy..."
for svc in hp-primary hp-standby pb-primary pb-standby; do
  echo -n "  Waiting for $svc..."
  until docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null | grep -q healthy; do
    sleep 2
  done
  echo " ready."
done

# Wait for proxies
sleep 5
echo "  All services healthy."
echo ""

# ─── Step 2: Create identical schema ─────────────────────────────────
echo "[Step 2] Creating identical schema on both clusters..."

SCHEMA_SQL="
CREATE TABLE IF NOT EXISTS workload (
  id SERIAL PRIMARY KEY,
  worker_id INT NOT NULL,
  seq INT NOT NULL,
  payload TEXT,
  created_at TIMESTAMP DEFAULT now()
);
TRUNCATE workload;
"

psql "$HP_DIRECT" -c "$SCHEMA_SQL" -q 2>/dev/null
psql "$PB_DIRECT" -c "$SCHEMA_SQL" -q 2>/dev/null
echo "  Schema created on both clusters."
echo ""

# ─── Workload function ───────────────────────────────────────────────
run_workload() {
  local label=$1
  local connstr=$2
  local output_file=$3
  local ok=0
  local err=0
  local start_ts=$(date +%s%N)
  local first_error_ts=""
  local last_error_ts=""

  while [ -f "$RESULTS_DIR/.running" ]; do
    if psql "$connstr" -q -v ON_ERROR_STOP=1 -c \
      "INSERT INTO workload (worker_id, seq, payload) VALUES ($RANDOM, $((ok+err+1)), 'data-$RANDOM');" \
      2>/dev/null; then
      ok=$((ok + 1))
    else
      err=$((err + 1))
      if [ -z "$first_error_ts" ]; then
        first_error_ts=$(date +%s%N)
      fi
      last_error_ts=$(date +%s%N)
      sleep 0.05
    fi
  done

  local downtime_ms=0
  if [ -n "$first_error_ts" ] && [ -n "$last_error_ts" ]; then
    downtime_ms=$(( (last_error_ts - first_error_ts) / 1000000 ))
  fi

  echo "${ok} ${err} ${downtime_ms}" > "$output_file"
}

# ─── Step 3: Start workloads ─────────────────────────────────────────
echo "[Step 3] Starting workloads ($CONCURRENCY workers each)..."
touch "$RESULTS_DIR/.running"

for i in $(seq 1 "$CONCURRENCY"); do
  run_workload "HP" "$HP_CONNSTR" "$RESULTS_DIR/hp_worker_${i}" &
  run_workload "PB" "$PB_CONNSTR" "$RESULTS_DIR/pb_worker_${i}" &
done

echo "  $((CONCURRENCY * 2)) total workers started."
echo ""

# ─── Step 4: Warm-up ─────────────────────────────────────────────────
echo "[Step 4] Warm-up period (${WARMUP}s)..."
sleep "$WARMUP"
echo "  Warm-up complete."
echo ""

# ─── Step 5: Kill BOTH primaries simultaneously ──────────────────────
echo "[Step 5] KILLING BOTH PRIMARIES..."
docker kill hp-primary pb-primary
echo "  Both primaries killed at $(date '+%H:%M:%S')."
echo ""

# ─── Step 6: Wait for recovery ───────────────────────────────────────
echo "[Step 6] Waiting ${RECOVERY_WAIT}s for recovery period..."
sleep 5
echo "  Restarting primaries..."
docker start hp-primary pb-primary

# Wait for health
for svc in hp-primary pb-primary; do
  echo -n "  Waiting for $svc..."
  until docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null | grep -q healthy; do
    sleep 2
  done
  echo " healthy."
done

remaining=$((RECOVERY_WAIT - 15))
[ "$remaining" -gt 0 ] && sleep "$remaining"
echo "  Recovery period complete."
echo ""

# ─── Step 7: Stop workloads ──────────────────────────────────────────
echo "[Step 7] Stopping workloads..."
rm -f "$RESULTS_DIR/.running"
sleep 3
wait 2>/dev/null || true
echo "  All workers stopped."
echo ""

# ─── Step 8: Collect metrics ─────────────────────────────────────────
echo "[Step 8] Collecting metrics..."

hp_total_ok=0; hp_total_err=0; hp_max_downtime=0
pb_total_ok=0; pb_total_err=0; pb_max_downtime=0

for i in $(seq 1 "$CONCURRENCY"); do
  if [ -f "$RESULTS_DIR/hp_worker_${i}" ]; then
    read -r ok err dt < "$RESULTS_DIR/hp_worker_${i}"
    hp_total_ok=$((hp_total_ok + ok))
    hp_total_err=$((hp_total_err + err))
    [ "$dt" -gt "$hp_max_downtime" ] && hp_max_downtime=$dt
  fi
  if [ -f "$RESULTS_DIR/pb_worker_${i}" ]; then
    read -r ok err dt < "$RESULTS_DIR/pb_worker_${i}"
    pb_total_ok=$((pb_total_ok + ok))
    pb_total_err=$((pb_total_err + err))
    [ "$dt" -gt "$pb_max_downtime" ] && pb_max_downtime=$dt
  fi
done

hp_total=$((hp_total_ok + hp_total_err))
pb_total=$((pb_total_ok + pb_total_err))

# Count rows actually in each database
hp_rows=$(psql "$HP_DIRECT" -tAc "SELECT COUNT(*) FROM workload;" 2>/dev/null || echo "N/A")
pb_rows=$(psql "$PB_DIRECT" -tAc "SELECT COUNT(*) FROM workload;" 2>/dev/null || echo "N/A")

hp_lost=$((hp_total_ok - ${hp_rows:-0})) 2>/dev/null || hp_lost="N/A"
pb_lost=$((pb_total_ok - ${pb_rows:-0})) 2>/dev/null || pb_lost="N/A"

# ─── Step 9: Generate report ─────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                        RESULTS                              ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║ %-25s │ %12s │ %12s ║\n" "Metric" "PgBouncer" "HeliosProxy"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║ %-25s │ %12s │ %12s ║\n" "Queries attempted"     "$pb_total"        "$hp_total"
printf "║ %-25s │ %12s │ %12s ║\n" "Successful queries"    "$pb_total_ok"     "$hp_total_ok"
printf "║ %-25s │ %12s │ %12s ║\n" "Client errors"         "$pb_total_err"    "$hp_total_err"
printf "║ %-25s │ %12s │ %12s ║\n" "Rows in database"      "$pb_rows"         "$hp_rows"
printf "║ %-25s │ %12s │ %12s ║\n" "Rows lost"             "$pb_lost"         "$hp_lost"
printf "║ %-25s │ %10sms │ %10sms ║\n" "Max client downtime" "$pb_max_downtime" "$hp_max_downtime"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Write markdown report
REPORT="$RESULTS_DIR/report.md"
sed -e "s/{{PB_TOTAL}}/$pb_total/g" \
    -e "s/{{HP_TOTAL}}/$hp_total/g" \
    -e "s/{{PB_OK}}/$pb_total_ok/g" \
    -e "s/{{HP_OK}}/$hp_total_ok/g" \
    -e "s/{{PB_ERRORS}}/$pb_total_err/g" \
    -e "s/{{HP_ERRORS}}/$hp_total_err/g" \
    -e "s/{{PB_ROWS}}/$pb_rows/g" \
    -e "s/{{HP_ROWS}}/$hp_rows/g" \
    -e "s/{{PB_LOST}}/$pb_lost/g" \
    -e "s/{{HP_LOST}}/$hp_lost/g" \
    -e "s/{{PB_DOWNTIME}}/${pb_max_downtime}ms/g" \
    -e "s/{{HP_DOWNTIME}}/${hp_max_downtime}ms/g" \
    -e "s/{{CONCURRENCY}}/$CONCURRENCY/g" \
    -e "s/{{DATE}}/$(date '+%Y-%m-%d %H:%M:%S')/g" \
    "$SCRIPT_DIR/report-template.md" > "$REPORT"

echo "Report written to: $REPORT"
echo ""

if [ "$hp_total_err" -lt "$pb_total_err" ]; then
  echo ">>> HeliosProxy had fewer client errors during failover. <<<"
elif [ "$hp_total_err" -eq "$pb_total_err" ]; then
  echo ">>> Both proxies had equal error counts. <<<"
else
  echo ">>> PgBouncer had fewer client errors (unexpected). <<<"
fi

# Cleanup temp files
rm -f "$RESULTS_DIR"/hp_worker_* "$RESULTS_DIR"/pb_worker_* "$RESULTS_DIR/.running"
