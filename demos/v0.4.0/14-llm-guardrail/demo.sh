#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PLUGIN_CRATE=helios-plugin-llm-guardrail
PLUGIN_WASM=helios_plugin_llm_guardrail.wasm
source ../_shared/plugin-demo.sh

export PGPASSWORD=postgres

echo
echo "=== llm-guardrail demo ==="

try() {
  local app="$1"; local sql="$2"; local label="$3"
  echo
  echo "→ [$label]  application_name=$app"
  echo "  query: $sql"
  out=$(PGAPPNAME="$app" psql -h localhost -p 6432 -U postgres -d demo \
    --no-password -c "$sql" 2>&1 || true)
  # Show only the relevant line (PG ERROR or row count).
  echo "  result:"
  echo "$out" | grep -E '^(ERROR|.*row)' | head -2 | sed 's/^/    /'
}

try "claude-bot" "DROP TABLE events"                                  "AI: DROP"
try "claude-bot" "DELETE FROM users"                                   "AI: DELETE no WHERE"
try "claude-bot" "SELECT * FROM events"                                "AI: SELECT large no LIMIT"
try "claude-bot" "SELECT * FROM users LIMIT 5"                         "AI: missing tenant_id"
try "claude-bot" "SELECT * FROM users WHERE tenant_id='acme' LIMIT 5"  "AI: clean (allowed)"

echo
echo "Same patterns from psql (no AI tag) pass through:"
try "psql" "SELECT * FROM events LIMIT 1"                              "human: large table"

echo
echo "Tear down: ./demo.sh down"
