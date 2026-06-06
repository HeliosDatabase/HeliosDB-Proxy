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

SESSION="${SESSION:-heliosproxy-talk}"
BIN="$ROOT/bin/oltp-race.sh"

command -v tmux >/dev/null 2>&1 || {
  echo "tmux not in PATH" >&2
  exit 2
}

"$BIN" up
"$BIN" init

if tmux has-session -t "$SESSION" 2>/dev/null; then
  tmux kill-session -t "$SESSION"
fi

tmux new-session -d -s "$SESSION" -n "pg-vs-nano" \
  "cd '$ROOT' && '$BIN' run-one postgres; exec bash"
tmux split-window -h -t "$SESSION:pg-vs-nano" \
  "cd '$ROOT' && '$BIN' run-one nano; exec bash"
tmux select-layout -t "$SESSION:pg-vs-nano" even-horizontal

tmux new-window -t "$SESSION" -n "proxy-demos" \
  "cd '$ROOT' && echo 'Main demos:' && echo '  bin/run-main-demos.sh switchover' && echo '  bin/run-main-demos.sh shadow' && echo '  bin/run-main-demos.sh anomaly' && echo '  bin/run-main-demos.sh plugins' && exec bash"

tmux new-window -t "$SESSION" -n "module-tour" \
  "cd '$ROOT' && bin/run-46-module-tour.sh; exec bash"

tmux select-window -t "$SESSION:pg-vs-nano"
tmux attach-session -t "$SESSION"
