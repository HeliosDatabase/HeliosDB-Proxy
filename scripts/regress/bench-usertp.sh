#!/usr/bin/env bash
# P-01 user-path benchmark — committed TPS, latency tails, first-row latency,
# client error/unknown rates and proxy RSS/CPU against a live backend.
#
# This is the harness the P-01 acceptance calls for: it measures the user path,
# not a mean of nanosecond microbenchmarks, and it writes a machine-readable
# result file so a baseline/candidate pair can be compared in the same window.
#
# Prerequisites (same as scripts/regress/bench-scalability.sh):
#   - a PostgreSQL 18.4 backend already running at 127.0.0.1:25433 with the
#     pgbench tables initialized (`pgbench -i`), user bench/benchpass, db benchdb
#   - Docker (used only to run the psql/pgbench client image)
#   - the proxy binary under test
#
# Usage:  ./bench-usertp.sh /path/to/heliosdb-proxy [label]
# Env:    CLIENTS="1 16 64"  DUR=10  MODES="direct session transaction"
#         OUT=/tmp/bench-usertp
#
# The script starts its own proxy per mode (Session / Transaction pool configs
# are generated next to this script's conventions). It does NOT start the
# backend and does not touch any other service.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: bench-usertp.sh <proxy-binary> [label]}"
LABEL="${2:-$(basename "$(dirname "$(dirname "$BIN")")")}"

IMG="postgres:18.4-bookworm"
PGHOST=127.0.0.1; PGPORT=25433
PXHOST=127.0.0.1; PXPORT=6432; ADMIN=127.0.0.1:9099
BUSER=bench; BPASS=benchpass; BDB=benchdb

CLIENTS="${CLIENTS:-1 16 64}"
DUR="${DUR:-10}"
MODES="${MODES:-direct session transaction}"
OUT="${OUT:-/tmp/bench-usertp}"; mkdir -p "$OUT"
RUNS_JSON="$OUT/$LABEL-results.json"
PROXYPID=""

pg(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" "$@"; }
# pgbench runs in a container: mount $OUT so its -l log lands on the host.
pgv(){ docker run --rm --network host -v "$OUT":/w -e PGPASSWORD="$BPASS" "$IMG" "$@"; }
cleanup(){ [ -n "$PROXYPID" ] && kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT

write_proxy_config(){
  local mode=$1 path=$2
  cat > "$path" <<EOF
listen_address = "127.0.0.1:$PXPORT"
admin_address  = "127.0.0.1:$ADMIN"
tr_enabled     = true
tr_mode        = "session"
write_timeout_secs = 30

[pool]
min_connections = 1
max_connections = 32
idle_timeout_secs = 60
max_lifetime_secs = 300
acquire_timeout_secs = 5
test_on_acquire = false

[pool_mode]
mode = "$mode"
max_pool_size = 32

[load_balancer]
read_strategy = "round_robin"
read_write_split = true
latency_threshold_ms = 100

[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"

[[nodes]]
host = "$PGHOST"
port = $PGPORT
role = "primary"
weight = 100
enabled = true
EOF
}

start_proxy(){
  local mode=$1
  local cfg="$OUT/$LABEL-$mode.toml" log="$OUT/$LABEL-$mode.log"
  write_proxy_config "$mode" "$cfg"
  NO_COLOR=1 RUST_LOG="heliosdb_proxy=warn" "$BIN" --config "$cfg" >"$log" 2>&1 &
  PROXYPID=$!
  local ready=0
  for _ in $(seq 1 40); do
    if pg psql -h "$PXHOST" -p "$PXPORT" -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1; then
      ready=1; break
    fi
    kill -0 "$PROXYPID" 2>/dev/null || { echo "proxy died:"; tail -20 "$log"; exit 1; }
    sleep 0.5
  done
  [ "$ready" = 1 ] || { echo "proxy never became ready"; exit 1; }
  echo "$log"
}

sample_proxy(){
  # rss_kb cpu_percent of the running proxy (empty for direct mode)
  if [ -n "$PROXYPID" ] && kill -0 "$PROXYPID" 2>/dev/null; then
    ps -o rss= -o pcpu= -p "$PROXYPID" | awk '{printf "%s %s", $1, $2}'
  else
    echo "0 0"
  fi
}

# pgbench → (tps, failed_txns); writes the per-transaction latency log.
run_pgbench(){
  local host=$1 port=$2 clients=$3 dur=$4 log=$5; shift 5
  local out
  out=$(pgv pgbench -h "$host" -p "$port" -U "$BUSER" -d "$BDB" -n \
        -c "$clients" -j 4 -T "$dur" -l --log-prefix="/w/$(basename "$log")" "$@" 2>&1)
  local tps failed
  tps=$(printf '%s' "$out" | grep -oE 'tps = [0-9.]+' | head -1 | grep -oE '[0-9.]+')
  failed=$(printf '%s' "$out" | grep -oE 'number of failed transactions: [0-9]+' | grep -oE '[0-9]+$')
  echo "${tps:-0} ${failed:-0}"
}

# Percentile of the pgbench per-transaction latency log (seconds).
percentiles(){
  local log=$1
  python3 - "$log" <<'PY'
import glob, sys, statistics
vals = []
for path in glob.glob(sys.argv[1] + ".*"):
    with open(path) as fh:
        for line in fh:
            parts = line.split()
            if len(parts) >= 3:
                try:
                    vals.append(float(parts[2]))
                except ValueError:
                    pass
if not vals:
    print("0 0 0 0 0")
else:
    vals.sort()
    def p(q): return vals[min(len(vals) - 1, int(q * len(vals)))]
    print(f"{statistics.mean(vals)*1000:.3f} {p(.50)*1000:.3f} {p(.95)*1000:.3f} {p(.99)*1000:.3f} {p(.999)*1000:.3f}")
PY
}

# First-row latency proxy: N round-trips of a one-row SELECT through the target.
first_row_ms(){
  local host=$1 port=$2
  local start end
  start=$(date +%s%N)
  for _ in $(seq 1 20); do
    pg psql -h "$host" -p "$port" -U "$BUSER" -d "$BDB" -tAc \
      "select aid from pgbench_accounts limit 1" >/dev/null 2>&1
  done
  end=$(date +%s%N)
  python3 -c "print(f'{($end - $start)/20_000_000:.2f}')"
}

echo "[]" > "$RUNS_JSON"   # results are appended below by python3
RESULTS=()

record(){
  local mode=$1 clients=$2 kind=$3 tps=$4 failed=$5 p=$6 rss=$7 cpu=$8 frow=$9
  RESULTS+=("{\"mode\":\"$mode\",\"clients\":$clients,\"workload\":\"$kind\",\"tps\":$tps,\"failed_txns\":$failed,\"latency_ms\":[$p],\"rss_kb\":${rss:-0},\"cpu_percent\":${cpu:-0},\"first_row_ms\":${frow:-0}}")
}

for mode in $MODES; do
  case "$mode" in
    direct)          host=$PGHOST; port=$PGPORT; pcfg="pool_mode=mode n/a" ;;
    session)         host=$PXHOST; port=$PXPORT; start_proxy "session" >/dev/null ;;
    transaction)     host=$PXHOST; port=$PXPORT; start_proxy "transaction" >/dev/null ;;
    *) echo "unknown mode $mode" >&2; exit 2 ;;
  esac

  frow=$(first_row_ms "$host" "$port")
  for clients in $CLIENTS; do
    # Read path (select-only) — committed TPS is meaningless here, but the
    # tails are the read-path SLA.
    readlog="$OUT/$LABEL-$mode-read-$clients.log"
    read_out=$(run_pgbench "$host" "$port" "$clients" "$DUR" "$readlog" -S)
    read_tps=${read_out%% *}; read_failed=${read_out##* }
    read_p=$(percentiles "$readlog")
    sample=$(sample_proxy); rss=${sample%% *}; cpu=${sample##* }
    record "$mode" "$clients" "read" "$read_tps" "$read_failed" "$read_p" "$rss" "$cpu" "$frow"

    # Write path (simple-update) — this is the committed TPS number.
    writelog="$OUT/$LABEL-$mode-write-$clients.log"
    write_out=$(run_pgbench "$host" "$port" "$clients" "$DUR" "$writelog" -b simple-update)
    write_tps=${write_out%% *}; write_failed=${write_out##* }
    write_p=$(percentiles "$writelog")
    record "$mode" "$clients" "committed_write" "$write_tps" "$write_failed" "$write_p" "$rss" "$cpu" "$frow"
    echo "mode=$mode clients=$clients read_tps=$read_tps committed_tps=$write_tps failed=$write_failed"
  done

  # Unknown-outcome signals in the proxy log (direct mode has none).
  if [ "$mode" != "direct" ]; then
    log="$OUT/$LABEL-$mode.log"
    unknown=$(grep -cE '08007|transaction_resolution_unknown' "$log" 2>/dev/null || true)
    echo "mode=$mode unknown_outcome_events=${unknown:-0} log=$log"
  fi

  cleanup
  PROXYPID=""
done

python3 - "$RUNS_JSON" "${RESULTS[@]}" <<'PY'
import json, sys
path, *rows = sys.argv[1:]
data = [json.loads(r) for r in rows]
with open(path, "w") as fh:
    json.dump(data, fh, indent=1)
print(f"wrote {path} ({len(data)} rows)")
PY
