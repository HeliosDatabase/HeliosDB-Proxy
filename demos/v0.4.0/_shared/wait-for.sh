#!/usr/bin/env bash
# Poll a host:port until it accepts connections, or fail after timeout.
# Usage: ./wait-for.sh <host> <port> [timeout_secs]
set -euo pipefail
host="${1:?host required}"
port="${2:?port required}"
timeout="${3:-30}"
deadline=$(( $(date +%s) + timeout ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if (echo > /dev/tcp/"$host"/"$port") 2>/dev/null; then
    exit 0
  fi
  sleep 0.5
done
echo "wait-for: timeout waiting for $host:$port" >&2
exit 1
