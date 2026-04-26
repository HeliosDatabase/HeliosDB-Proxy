#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PLUGIN_CRATE=helios-plugin-column-mask
PLUGIN_WASM=helios_plugin_column_mask.wasm
source ../_shared/plugin-demo.sh

echo
echo "=== column-mask demo ==="

# Seed mask rules into the plugin's KV namespace.
echo "[seed] Writing mask rules to KV"
RULES='[
  {"table":"users","column":"ssn","mask_function":"mask_ssn","unmask_role":"pii_reader"},
  {"table":"users","column":"email","mask_function":"mask_email","unmask_role":"pii_reader"}
]'
curl -s -X PUT \
  http://localhost:9090/admin/kv/helios-plugin-column-mask/rules \
  --data-raw "$RULES" || echo "  (admin KV write requires the build to expose it)"

run_as() {
  local user="$1"; local pw="$2"; local label="$3"
  echo
  echo "→ user=$user  ($label)"
  PGPASSWORD="$pw" psql -h localhost -p 6432 -U "$user" -d demo \
    --no-password \
    -c "SET helios.identity.roles = '$user'; SELECT name, email, ssn FROM users WHERE id = 1" \
    2>&1 | grep -A 2 'name' | head -4
}

run_as app_user   app  "no pii_reader role → masked"
run_as pii_reader pii  "has pii_reader role → raw"

echo
echo "Tear down: ./demo.sh down"
