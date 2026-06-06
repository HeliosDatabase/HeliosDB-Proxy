#!/usr/bin/env bash
set -euo pipefail

ROOT="${DEMOGROUND:-$HOME/HDB/Proxy-Demogrounds}"
ENV_FILE="$ROOT/.env"
load_env_defaults() {
  local key value
  while IFS='=' read -r key value; do
    [[ -z "$key" || "$key" == \#* ]] && continue
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    if [[ -z "${!key+x}" ]]; then
      export "$key=$value"
    fi
  done < "$ENV_FILE"
}
[[ -f "$ENV_FILE" ]] && load_env_defaults

POSTGRES_PORT="${POSTGRES_PORT:-55432}"
NANO_PORT="${NANO_PORT:-16432}"
NANO_HTTP_PORT="${NANO_HTTP_PORT:-18180}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-heliosproxy-demo-postgres}"
DB_NAME="${DB_NAME:-postgres}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"
ACCOUNTS="${ACCOUNTS:-1000}"
CLIENTS="${CLIENTS:-16}"
JOBS="${JOBS:-4}"
NANO_CLIENTS="${NANO_CLIENTS:-1}"
NANO_JOBS="${NANO_JOBS:-1}"
DURATION="${DURATION:-60}"
PGBENCH_PROTOCOL="${PGBENCH_PROTOCOL:-simple}"
NANO_BIN="${NANO_BIN:-$HOME/HDB/Nano/target/release/heliosdb-nano}"

SCRIPT="$ROOT/assets/oltp.pgbench.sql"
LOG_DIR="$ROOT/logs"
RUN_DIR="$ROOT/run"
STATE_DIR="$ROOT/state"
mkdir -p "$LOG_DIR" "$RUN_DIR" "$STATE_DIR"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 2
  }
}

psql_cmd() {
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="$DB_PASSWORD" psql "$@"
  else
    require_cmd docker
    docker run -i --rm --network host -e PGPASSWORD="$DB_PASSWORD" postgres:16-alpine psql "$@"
  fi
}

pgbench_cmd() {
  if command -v pgbench >/dev/null 2>&1; then
    PGPASSWORD="$DB_PASSWORD" pgbench "$@"
  else
    require_cmd docker
    docker run --rm --network host -e PGPASSWORD="$DB_PASSWORD" \
      -v "$SCRIPT:$SCRIPT:ro" postgres:16-alpine pgbench "$@"
  fi
}

wait_psql() {
  local label="$1" port="$2" deadline="${3:-60}"
  local start now
  start=$(date +%s)
  while true; do
    if psql_cmd -h 127.0.0.1 -p "$port" -U "$DB_USER" -d "$DB_NAME" -AtXc "SELECT 1" >/dev/null 2>&1; then
      echo "[$label] ready on 127.0.0.1:$port"
      return 0
    fi
    now=$(date +%s)
    if (( now - start > deadline )); then
      echo "[$label] did not become ready within ${deadline}s" >&2
      return 1
    fi
    sleep 1
  done
}

port_in_use() {
  local port="$1"
  command -v ss >/dev/null 2>&1 && ss -ltn | awk '{print $4}' | grep -Eq "[:.]${port}$"
}

start_postgres() {
  require_cmd docker
  if docker ps --format '{{.Names}}' | grep -qx "$POSTGRES_CONTAINER"; then
    echo "[postgres] container already running"
  else
    docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
    docker run -d --name "$POSTGRES_CONTAINER" \
      -e POSTGRES_USER="$DB_USER" \
      -e POSTGRES_PASSWORD="$DB_PASSWORD" \
      -e POSTGRES_DB="$DB_NAME" \
      -p "$POSTGRES_PORT:5432" \
      postgres:16-alpine >/dev/null
  fi
  wait_psql postgres "$POSTGRES_PORT" 90
}

start_nano() {
  if [[ ! -x "$NANO_BIN" ]]; then
    cat >&2 <<EOF
Nano binary not executable:
  $NANO_BIN

Build or copy a Nano binary outside this script, then set NANO_BIN in:
  $ENV_FILE
EOF
    exit 2
  fi
  if [[ -f "$RUN_DIR/nano.pid" ]] && kill -0 "$(cat "$RUN_DIR/nano.pid")" >/dev/null 2>&1; then
    echo "[nano] already running pid $(cat "$RUN_DIR/nano.pid")"
  else
    if port_in_use "$NANO_PORT"; then
      echo "[nano] port $NANO_PORT is already in use; set NANO_PORT in $ENV_FILE" >&2
      exit 2
    fi
    if [[ "$NANO_HTTP_PORT" != "0" ]] && port_in_use "$NANO_HTTP_PORT"; then
      echo "[nano] HTTP port $NANO_HTTP_PORT is already in use; set NANO_HTTP_PORT in $ENV_FILE" >&2
      exit 2
    fi
    rm -rf "$STATE_DIR/nano-data"
    mkdir -p "$STATE_DIR/nano-data"
    "$NANO_BIN" start \
      --data-dir "$STATE_DIR/nano-data" \
      --listen 127.0.0.1 \
      --port "$NANO_PORT" \
      --http-listen 127.0.0.1 \
      --http-port "$NANO_HTTP_PORT" \
      --max-connections 512 \
      --daemon \
      --pid-file "$RUN_DIR/nano.pid" \
      > "$LOG_DIR/nano.log" 2>&1
    sleep 1
    if ! kill -0 "$(cat "$RUN_DIR/nano.pid")" >/dev/null 2>&1; then
      echo "[nano] process exited during startup" >&2
      tail -80 "$LOG_DIR/nano.log" >&2 || true
      exit 1
    fi
  fi
  wait_psql nano "$NANO_PORT" 90
}

sql_exec() {
  local port="$1" sql="$2"
  psql_cmd -h 127.0.0.1 -p "$port" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -q -c "$sql"
}

init_one() {
  local label="$1" port="$2"
  echo "[$label] loading OLTP schema"
  sql_exec "$port" "DROP TABLE IF EXISTS demo_ledger; DROP TABLE IF EXISTS demo_accounts;"
  sql_exec "$port" "CREATE TABLE demo_accounts (id INT PRIMARY KEY, balance INT NOT NULL, version INT NOT NULL);"
  sql_exec "$port" "CREATE TABLE demo_ledger (id SERIAL PRIMARY KEY, account_id INT NOT NULL, delta INT NOT NULL, note TEXT);"
  local batch="" i
  for i in $(seq 1 "$ACCOUNTS"); do
    batch="${batch}($i,1000,0),"
    if (( i % 100 == 0 )); then
      sql_exec "$port" "INSERT INTO demo_accounts(id,balance,version) VALUES ${batch%,};"
      batch=""
    fi
  done
  if [[ -n "$batch" ]]; then
    sql_exec "$port" "INSERT INTO demo_accounts(id,balance,version) VALUES ${batch%,};"
  fi
  sql_exec "$port" "SELECT COUNT(*) AS accounts_loaded FROM demo_accounts;"
}

run_one() {
  local target="$1" port label log
  case "$target" in
    postgres|pg) label="PostgreSQL"; port="$POSTGRES_PORT"; clients="$CLIENTS"; jobs="$JOBS" ;;
    nano|heliosdb-nano) label="HeliosDB-Nano"; port="$NANO_PORT"; clients="$NANO_CLIENTS"; jobs="$NANO_JOBS" ;;
    *) echo "unknown run target: $target" >&2; exit 2 ;;
  esac
  log="$LOG_DIR/oltp-${target}-$(date -u +%Y%m%dT%H%M%SZ).log"
  echo "[$label] pgbench -M $PGBENCH_PROTOCOL -c $clients -j $jobs -T $DURATION"
  pgbench_cmd \
    -h 127.0.0.1 -p "$port" -U "$DB_USER" \
    -n -M "$PGBENCH_PROTOCOL" -c "$clients" -j "$jobs" \
    -T "$DURATION" -P 2 -D "accounts=$ACCOUNTS" -f "$SCRIPT" "$DB_NAME" 2>&1 | tee "$log"
  echo "[$label] log: $log"
}

status_one() {
  local label="$1" port="$2"
  if ! sql_exec "$port" "SELECT COUNT(*) AS ledger_rows, COALESCE(SUM(delta),0) AS ledger_delta FROM demo_ledger;"; then
    echo "[$label] unavailable on port $port"
    return 0
  fi
  sql_exec "$port" "SELECT COALESCE(SUM(balance),0) AS total_balance, COALESCE(SUM(version),0) AS total_versions FROM demo_accounts;" || true
}

stop_all() {
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  if [[ -f "$RUN_DIR/nano.pid" ]]; then
    kill "$(cat "$RUN_DIR/nano.pid")" >/dev/null 2>&1 || true
    rm -f "$RUN_DIR/nano.pid"
  fi
}

case "${1:-}" in
  up)
    start_postgres
    start_nano
    ;;
  init)
    init_one postgres "$POSTGRES_PORT"
    init_one nano "$NANO_PORT"
    ;;
  run-one)
    run_one "${2:?postgres|nano}"
    ;;
  run)
    run_one postgres &
    pg_pid=$!
    run_one nano &
    nano_pid=$!
    wait "$pg_pid"
    wait "$nano_pid"
    ;;
  status)
    echo "[postgres]"
    status_one postgres "$POSTGRES_PORT"
    echo "[nano]"
    status_one nano "$NANO_PORT"
    ;;
  down)
    stop_all
    ;;
  *)
    cat >&2 <<EOF
usage: $(basename "$0") {up|init|run|run-one postgres|run-one nano|status|down}
EOF
    exit 2
    ;;
esac
