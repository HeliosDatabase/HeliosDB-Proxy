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
PROXY_REPO="${PROXY_REPO:-$HOME/HDB/Proxy}"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"

wait_http() {
  local url="$1" deadline="${2:-90}" start now
  start=$(date +%s)
  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    now=$(date +%s)
    if (( now - start > deadline )); then
      echo "timed out waiting for $url" >&2
      return 1
    fi
    sleep 1
  done
}

psql_cmd() {
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="${PGPASSWORD:-helios}" psql "$@"
  else
    command -v docker >/dev/null 2>&1 || {
      echo "missing psql and docker; cannot run PostgreSQL client commands" >&2
      exit 2
    }
    docker run -i --rm --network host -e PGPASSWORD="${PGPASSWORD:-helios}" postgres:16-alpine psql "$@"
  fi
}

demo_switchover() {
  cd "$PROXY_REPO"
  echo "== Lossless switchover / transaction journal =="
  docker compose -f tests/docker/cluster.yml up --build --wait -d
  tests/docker/pgbench-chaos.sh init
  tests/docker/checksum.sh snapshot pre
  tests/docker/pgbench-chaos.sh run "${SWITCHOVER_DURATION:-90}" > "$LOG_DIR/switchover-pgbench.pid"
  load_pid=$(cat "$LOG_DIR/switchover-pgbench.pid")
  sleep 12
  tests/docker/pgbench-chaos.sh status | tee "$LOG_DIR/switchover-before.txt"
  tests/docker/pgbench-chaos.sh scenario primary-sigkill
  sleep 10
  tests/docker/pgbench-chaos.sh status | tee "$LOG_DIR/switchover-after.txt"
  wait "$load_pid" 2>/dev/null || true
  tests/docker/checksum.sh snapshot post
  tests/docker/checksum.sh compare pre post | tee "$LOG_DIR/switchover-checksum.txt"
  curl -s -X POST http://127.0.0.1:9090/api/replay \
    -H 'Content-Type: application/json' \
    -d "{\"from\":\"$(date -u -d '10 minutes ago' +%FT%TZ)\",\"to\":\"$(date -u +%FT%TZ)\",\"target_host\":\"pg-standby-async\",\"target_port\":5432,\"target_user\":\"helios\",\"target_password\":\"helios\",\"target_database\":\"appdb\"}" \
    | tee "$LOG_DIR/switchover-replay.json"
  echo
  echo "Logs: $LOG_DIR/switchover-*"
}

demo_shadow() {
  cd "$PROXY_REPO"
  echo "== Shadow execute PG14 source vs PG16 candidate =="
  docker compose -f tests/docker/upgrade-matrix.yml up --build --wait -d
  wait_http http://127.0.0.1:59002/health 90

  for port in 55014 55016; do
    PGPASSWORD=helios psql_cmd -h 127.0.0.1 -p "$port" -U helios -d appdb -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS shadow_probe;
CREATE TABLE shadow_probe(id INT PRIMARY KEY, label TEXT);
INSERT INTO shadow_probe VALUES (1, 'same'), (2, 'candidate');
SQL
  done
  PGPASSWORD=helios psql_cmd -h 127.0.0.1 -p 55016 -U helios -d appdb -v ON_ERROR_STOP=1 -q \
    -c "UPDATE shadow_probe SET label = 'candidate-different' WHERE id = 2;"

  echo
  echo "[clean] same rows on both sides"
  curl -s -X POST http://127.0.0.1:59002/api/shadow \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT 1 AS ok","source_host":"pg-14","source_port":5432,"source_user":"helios","source_password":"helios","source_database":"appdb","shadow_host":"pg-16","shadow_port":5432,"shadow_user":"helios","shadow_password":"helios","shadow_database":"appdb"}' \
    | jq .

  echo
  echo "[data drift] seeded candidate row differs"
  curl -s -X POST http://127.0.0.1:59002/api/shadow \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT id,label FROM shadow_probe ORDER BY id","source_host":"pg-14","source_port":5432,"source_user":"helios","source_password":"helios","source_database":"appdb","shadow_host":"pg-16","shadow_port":5432,"shadow_user":"helios","shadow_password":"helios","shadow_database":"appdb"}' \
    | tee "$LOG_DIR/shadow-data-drift.json" | jq .

  echo
  echo "[version divergence] regexp_count exists on PG16, not PG14"
  curl -s -X POST http://127.0.0.1:59002/api/shadow \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT regexp_count('\''abc123abc'\'','\''abc'\'')","source_host":"pg-14","source_port":5432,"source_user":"helios","source_password":"helios","source_database":"appdb","shadow_host":"pg-16","shadow_port":5432,"shadow_user":"helios","shadow_password":"helios","shadow_database":"appdb"}' \
    | tee "$LOG_DIR/shadow-version-divergence.json" | jq .
}

demo_anomaly() {
  cd "$PROXY_REPO/demos/v0.4.0/01-anomaly-detection"
  echo "== Wire-edge anomaly detection =="
  docker compose up -d
  "$PROXY_REPO/demos/v0.4.0/_shared/wait-for.sh" localhost 9090 60

  PGPASSWORD=postgres psql_cmd -h 127.0.0.1 -p 6432 -U postgres -d demo --no-password -c \
    "SELECT * FROM users WHERE id = 1 OR 1=1 -- ;" >/dev/null 2>&1 || true
  PGPASSWORD=postgres psql_cmd -h 127.0.0.1 -p 6432 -U postgres -d demo --no-password -c \
    "SELECT 1; DROP TABLE events; --" >/dev/null 2>&1 || true
  PGPASSWORD=postgres psql_cmd -h 127.0.0.1 -p 6432 -U postgres -d demo --no-password -c \
    "SELECT * FROM users WHERE id = pg_sleep(5)" >/dev/null 2>&1 || true
  for _ in $(seq 1 12); do
    PGPASSWORD=wrongpw psql_cmd -h 127.0.0.1 -p 6432 -U alice -d demo --no-password \
      -c "SELECT 1" >/dev/null 2>&1 || true
  done

  sleep 1
  curl -s "http://127.0.0.1:9090/anomalies?limit=50" | tee "$LOG_DIR/anomaly-events.json" | jq -r '
    .events[]
    | "  - \(.severity // "info") \(.kind): \(
        if .kind == "sql_injection" then (.patterns_matched | join(",") + " | " + (.sql_excerpt // ""))
        elif .kind == "auth_burst" then "user=\(.user) ip=\(.client_ip) failures=\(.failures)"
        elif .kind == "rate_spike" then "tenant=\(.tenant) rate=\(.rate_per_sec) z=\(.z_score)"
        else (.sql_excerpt // . | tostring) end
      )"
  '
}

demo_plugins() {
  cat <<'EOF'
== WASM plugin 60-second tour ==

Hot-reload host:
  demos/v0.4.0/03-plugin-kv
  demos/v0.4.0/04-plugin-crypto
  demos/v0.4.0/05-plugin-signatures
  demos/v0.4.0/06-plugin-oci
  demos/v0.4.0/07-route-block
  demos/v0.4.0/08-trust-root

First-party plugins:
  11 cost-governor      - per-tenant query cost budget
  12 ai-classifier      - LLM SQL traffic tagging
  13 token-budget       - per-agent/model token spend gate
  14 llm-guardrail      - refuses dangerous AI SQL
  15 pgvector-router    - vector top-K routing
  16 column-mask        - PII masking by role
  17 audit-chain        - hash-chained audit evidence
  18 residency-router   - geo/data-residency enforcement

Runnable examples:
  cd ~/HDB/Proxy/demos/v0.4.0/11-cost-governor && ./demo.sh
  cd ~/HDB/Proxy/demos/v0.4.0/16-column-mask && ./demo.sh
  curl -s http://localhost:9090/plugins | jq .
EOF
}

case "${1:-}" in
  switchover) demo_switchover ;;
  shadow) demo_shadow ;;
  anomaly) demo_anomaly ;;
  plugins) demo_plugins ;;
  all)
    demo_switchover
    demo_shadow
    demo_anomaly
    demo_plugins
    ;;
  *)
    cat >&2 <<EOF
usage: $(basename "$0") {switchover|shadow|anomaly|plugins|all}
EOF
    exit 2
    ;;
esac
