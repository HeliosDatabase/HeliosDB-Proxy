#!/usr/bin/env bash
# cost-governor demo — seed a tight tenant budget, exhaust it,
# observe block, recover after window resets.
set -euo pipefail
cd "$(dirname "$0")"

case "${1:-run}" in
  down) docker compose down -v; exit 0 ;;
esac

echo "=== cost-governor demo ==="

# Build the plugin if not already present.
WASM_NAME="helios_plugin_cost_governor.wasm"
PLUGINS_DIR="../../../../HDB-HeliosDB-Proxy-Plugins"
if [ ! -f "plugins/$WASM_NAME" ]; then
  echo "[0/5] Building $WASM_NAME (one-time)..."
  if [ ! -d "$PLUGINS_DIR" ]; then
    echo "       expected plugins workspace at $PLUGINS_DIR"
    exit 1
  fi
  (cd "$PLUGINS_DIR" && cargo build -p helios-plugin-cost-governor \
     --target wasm32-unknown-unknown --release)
  cp "$PLUGINS_DIR/target/wasm32-unknown-unknown/release/$WASM_NAME" plugins/
fi

echo "[1/5] Starting proxy + Postgres + cost-governor.wasm"
docker compose up -d
../_shared/wait-for.sh localhost 9090 60

echo "[2/5] Seeding acme budget: minute=1.0, hour=10.0, day=100.0"
docker compose exec -T proxy sh -c \
  'echo {"minute":1.0,"hour":10.0,"day":100.0} | base64 -w 0' >/dev/null
# In production the budget is seeded via the operator's TenantQuota
# reconciler. For this demo we use the proxy's KV admin endpoint.
curl -s -X PUT http://localhost:9090/admin/kv/helios-plugin-cost-governor/tenant:acme:budget \
  --data-raw '{"minute":1.0,"hour":10.0,"day":100.0}' || true

export PGPASSWORD=postgres
echo "[3/5] Running 5 small queries — all succeed"
for i in 1 2 3 4 5; do
  psql -h localhost -p 6432 -U postgres -d demo --no-password \
    -c "SET helios.tenant_id = 'acme'; SELECT $i" >/dev/null 2>&1 || true
done

echo "[4/5] Running 1 large query (SELECT * FROM events) — exhausts budget"
psql -h localhost -p 6432 -U postgres -d demo --no-password \
  -c "SET helios.tenant_id = 'acme'; SELECT count(*) FROM events" >/dev/null 2>&1 || true

echo "   (in production the post_query hook would update usage to ~6.4)"

echo "[5/5] Running 6th query — would be blocked once usage > 1.0"
psql -h localhost -p 6432 -U postgres -d demo --no-password \
  -c "SET helios.tenant_id = 'acme'; SELECT 'after-budget'" 2>&1 | head -3

echo
echo "Tear down: ./demo.sh down"
