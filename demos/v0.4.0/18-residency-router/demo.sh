#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PLUGIN_CRATE=helios-plugin-residency-router
PLUGIN_WASM=helios_plugin_residency_router.wasm
source ../_shared/plugin-demo.sh

export PGPASSWORD=postgres

echo
echo "=== residency-router demo ==="

echo "[seed] Writing region map to KV"
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-residency-router/region_map \
  --data-raw '[["eu-west","pg-eu-west:5432"],["us-east","pg-us-east:5432"]]' || true
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-residency-router/enforce \
  --data-raw 'true' || true

try_region() {
  local r="$1"; local label="$2"
  echo
  echo "→ helios.region=$r  ($label)"
  out=$(psql -h localhost -p 6432 -U postgres -d demo --no-password \
    -c "SET helios.region='$r'; SELECT 1" 2>&1 || true)
  echo "$out" | grep -E '^(ERROR|.*1)' | head -2 | sed 's/^/    /'
}

try_region "eu-west"     "configured EU node → routed"
try_region "us-east"     "configured US node → routed"
try_region "antarctica"  "no in-region replica + enforce=true → blocked"

echo
echo "Tear down: ./demo.sh down"
