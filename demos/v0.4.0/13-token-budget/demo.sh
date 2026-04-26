#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PLUGIN_CRATE=helios-plugin-token-budget
PLUGIN_WASM=helios_plugin_token_budget.wasm
source ../_shared/plugin-demo.sh

export PGPASSWORD=postgres

echo
echo "=== token-budget demo ==="
echo "[seed] Writing budget for (rag-bot, claude-opus): minute=10, day=100"
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-token-budget/agent:rag-bot:claude-opus:budget \
  --data-raw '{"minute":10.0,"day":100.0}' || true

echo
echo "[run] 5 queries with application_name=rag-bot-claude-opus"
for i in 1 2 3 4 5; do
  PGAPPNAME=rag-bot-claude-opus-4-7 \
    psql -h localhost -p 6432 -U postgres -d demo --no-password \
    -c "SELECT count(*) FROM users" >/dev/null 2>&1 || true
  echo "   q$i sent"
done
echo
echo "[check] usage after 5 queries:"
curl -s http://localhost:9090/admin/kv/helios-plugin-token-budget/agent:rag-bot:claude-opus:spend 2>/dev/null \
  | jq . 2>/dev/null || echo "   (admin KV read endpoint not exposed in this build)"

echo
echo "Tear down: ./demo.sh down"
