#!/usr/bin/env bash
# Shared bring-up logic for the per-plugin demos. Source this from
# the per-demo demo.sh:
#
#   #!/usr/bin/env bash
#   set -euo pipefail
#   cd "$(dirname "$0")"
#   PLUGIN_CRATE=helios-plugin-cost-governor
#   PLUGIN_WASM=helios_plugin_cost_governor.wasm
#   source ../_shared/plugin-demo.sh
#
# After sourcing, the proxy + Postgres are running and the plugin
# is loaded. Per-demo scripts then run their psql / curl calls.

case "${1:-run}" in
  down) docker compose down -v; exit 0 ;;
esac

PLUGINS_DIR="${PLUGINS_DIR:-../../../../HDB-HeliosDB-Proxy-Plugins}"

if [ ! -f "plugins/${PLUGIN_WASM:?must set PLUGIN_WASM}" ]; then
  echo "[setup] Building $PLUGIN_WASM (one-time)..."
  if [ ! -d "$PLUGINS_DIR" ]; then
    echo "        expected plugins workspace at $PLUGINS_DIR"
    exit 1
  fi
  (cd "$PLUGINS_DIR" && cargo build -p "${PLUGIN_CRATE:?must set PLUGIN_CRATE}" \
     --target wasm32-unknown-unknown --release)
  cp "$PLUGINS_DIR/target/wasm32-unknown-unknown/release/$PLUGIN_WASM" plugins/
fi

echo "[setup] Starting proxy + Postgres + ${PLUGIN_WASM}"
docker compose up -d
../_shared/wait-for.sh localhost 9090 60
echo "[setup] proxy at localhost:6432 (PG) + localhost:9090 (admin)"
