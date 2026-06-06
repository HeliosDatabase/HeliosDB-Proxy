#!/usr/bin/env bash
set -euo pipefail

REPO="${PROXY_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
ROOT="${DEMOGROUND:-$HOME/HDB/Proxy-Demogrounds}"

mkdir -p "$ROOT"/{bin,configs,logs,run,state,notes,assets}

cp "$REPO/demos/live-talk/bin/oltp-race.sh" "$ROOT/bin/"
cp "$REPO/demos/live-talk/bin/launch-tmux.sh" "$ROOT/bin/"
cp "$REPO/demos/live-talk/bin/run-main-demos.sh" "$ROOT/bin/"
cp "$REPO/demos/live-talk/bin/run-46-module-tour.sh" "$ROOT/bin/"
cp "$REPO/demos/live-talk/assets/oltp.pgbench.sql" "$ROOT/assets/"
cp "$REPO/demos/live-talk/module-map.md" "$ROOT/module-map.md"
cp "$REPO/demos/live-talk/README.md" "$ROOT/README.md"

chmod +x "$ROOT"/bin/*.sh

if [[ ! -f "$ROOT/.env" ]]; then
  cat > "$ROOT/.env" <<EOF
PROXY_REPO=$REPO
NANO_REPO=$HOME/HDB/Nano
NANO_BIN=$HOME/HDB/Nano/target/release/heliosdb-nano
POSTGRES_PORT=55432
NANO_PORT=16432
NANO_HTTP_PORT=18180
NANO_REPLICATION_PORT=19432
POSTGRES_CONTAINER=heliosproxy-demo-postgres
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres
ACCOUNTS=1000
CLIENTS=16
JOBS=4
NANO_CLIENTS=1
NANO_JOBS=1
DURATION=60
PGBENCH_PROTOCOL=simple
EOF
fi

cat <<EOF
Demoground ready:
  $ROOT

Next:
  source "$ROOT/.env"
  "$ROOT/bin/oltp-race.sh" up
  "$ROOT/bin/oltp-race.sh" init
  "$ROOT/bin/launch-tmux.sh"

Main demos:
  "$ROOT/bin/run-main-demos.sh" switchover
  "$ROOT/bin/run-main-demos.sh" shadow
  "$ROOT/bin/run-main-demos.sh" anomaly
  "$ROOT/bin/run-main-demos.sh" plugins
EOF
