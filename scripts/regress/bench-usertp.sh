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
[ -n "${PGOPTIONS:-}" ] && echo "PGOPTIONS=$PGOPTIONS"
# TR_ENABLED=false / EXTRA_TOML=$'[journal]\nmax_committed_transactions = 1' isolate one subsystem's cost.
[ -n "${TR_ENABLED:-}${EXTRA_TOML:-}" ] && echo "TR_ENABLED=${TR_ENABLED:-true} EXTRA_TOML=${EXTRA_TOML:-}"
RUNS_JSON="$OUT/$LABEL-results.json"
PROXYPID=""

pg(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" "$@"; }
# pgbench runs in a container: mount $OUT so its -l log lands on the host.
# PGOPTIONS (e.g. "-c synchronous_commit=off") is passed through to pgbench so the
# write path can be measured CPU-bound instead of fsync-bound when the backend's
# commit latency would otherwise swamp the proxy's share.
pgv(){ docker run --rm --network host -v "$OUT":/w -e PGPASSWORD="$BPASS" -e PGOPTIONS="${PGOPTIONS:-}" "$IMG" "$@"; }
cleanup(){ [ -n "$PROXYPID" ] && kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; }
trap cleanup EXIT

write_proxy_config(){
  local mode=$1 path=$2
  cat > "$path" <<EOF
listen_address = "127.0.0.1:$PXPORT"
admin_address  = "$ADMIN"
tr_enabled     = ${TR_ENABLED:-true}
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
${EXTRA_TOML:-}
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

# Percentiles of the pgbench per-transaction latency log. pgbench logs
# microseconds; we report milliseconds, comma-separated (they go into a JSON array).
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
    print("0,0,0,0,0")
else:
    vals.sort()
    def p(q): return vals[min(len(vals) - 1, int(q * len(vals)))]
    print(",".join(f"{v/1000:.3f}" for v in (statistics.mean(vals), p(.50), p(.95), p(.99), p(.999))))
PY
}

# First-row latency proxy: N round-trips of a one-row SELECT through the target.
first_row_ms(){
  local host=$1 port=$2
  # One container, 20 psql round-trips timed inside it: a container start per
  # round-trip (~60 ms) would otherwise swamp a sub-millisecond first-row latency.
  pg bash -c "s=\$(date +%s%N); for _ in \$(seq 1 20); do psql -h $host -p $port -U $BUSER -d $BDB -tAc 'select aid from pgbench_accounts limit 1' >/dev/null 2>&1; done; e=\$(date +%s%N); echo \$(( (e - s) / 20000 ))" \
    | python3 -c "import sys; print(f'{int(sys.stdin.read().strip() or 0)/1000:.2f}')"
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

  # Unknown-outcome signals (direct mode has none): the proxy's own counter of
  # SQLSTATE 08007 errors returned, read from the admin API while it still runs.
  if [ "$mode" != "direct" ]; then
    log="$OUT/$LABEL-$mode.log"
    unknown=$(curl -sS --max-time 3 "http://$ADMIN/metrics" 2>/dev/null \
      | python3 -c 'import json,sys
try:
    m=json.load(sys.stdin); print(m.get("tr_unknown_outcome_errors_total", m.get("tr",{}).get("unknown_outcome_errors",0)))
except Exception: print("n/a")')
    echo "mode=$mode unknown_outcome_events=${unknown:-n/a} log=$log"
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
