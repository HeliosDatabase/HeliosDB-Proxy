#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p plugins
PLUGIN_CRATE=helios-plugin-pgvector-router
PLUGIN_WASM=helios_plugin_pgvector_router.wasm
source ../_shared/plugin-demo.sh

export PGPASSWORD=postgres

echo
echo "=== pgvector-router demo ==="

echo "→ Vector top-K query (should route to pg-vector):"
PGAPPNAME='helios.vector_node:pg-vector' \
  psql -h localhost -p 6432 -U postgres -d demo --no-password \
  -c "SELECT title FROM docs ORDER BY embedding <=> '[1,0,0]' LIMIT 3"

echo
echo "→ Non-vector query (default routing → pg-primary):"
psql -h localhost -p 6432 -U postgres -d demo --no-password \
  -c "SELECT id, name FROM users WHERE id = 1"

echo
echo "→ Distance operator without ORDER BY (incidental — default routing):"
PGAPPNAME='helios.vector_node:pg-vector' \
  psql -h localhost -p 6432 -U postgres -d demo --no-password \
  -c "SELECT 1 WHERE '[1,0,0]'::vector <=> '[0,1,0]' < 2.0" 2>&1 | head -3 || true

echo
echo "Inspect routing decisions in proxy logs:"
echo "   docker compose logs proxy | grep -E 'route|vector'"
echo
echo "Tear down: ./demo.sh down"
