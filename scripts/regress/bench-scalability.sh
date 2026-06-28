#!/usr/bin/env bash
# HeliosProxy scalability + performance benchmark, against a live PostgreSQL
# 18.4 backend. Two axes:
#
#   A. THROUGHPUT / LATENCY / OVERHEAD — pgbench select-only (-S), persistent
#      connections, at a client-concurrency sweep. Compares direct-to-PG against
#      the proxy in session and transaction pool modes. Shows the proxy's
#      per-query overhead and how throughput scales with concurrency.
#
#   B. BACKEND-CONNECTION EFFICIENCY — rate-limited clients with think time
#      (-R), so each client idles between transactions. Session mode pins one
#      backend connection per client; transaction mode returns the connection to
#      a shared pool at each transaction boundary, so the same client population
#      is served by far fewer backend connections. Measured by sampling
#      pg_stat_activity on the backend. This is the scalability property a
#      connection proxy exists to provide.
#
# Usage:  ./bench-scalability.sh /path/to/heliosdb-proxy [label]
# Env:    CLIENTS="1 16 64"  DUR=8  MODES="session transaction"  EFF_CLIENTS=32
#         EFF_RATE=200
#
# Read-only (-S) is used deliberately: it removes WAL/vacuum/write-contention
# variance on a single backend so the numbers reflect the proxy path, not
# backend write amplification.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: bench-scalability.sh <proxy-binary> [label]}"
LABEL="${2:-$(basename "$(dirname "$(dirname "$BIN")")")}"

IMG="postgres:18.4-bookworm"
PGHOST=127.0.0.1; PGPORT=25433             # backend (direct)
PXHOST=127.0.0.1; PXPORT=6432; ADMIN=127.0.0.1:9099
BUSER=bench; BPASS=benchpass; BDB=benchdb

CLIENTS="${CLIENTS:-1 16 64}"
DUR="${DUR:-8}"
MODES="${MODES:-session transaction}"
EFF_CLIENTS="${EFF_CLIENTS:-32}"
EFF_RATE="${EFF_RATE:-200}"

OUT="${OUT:-/tmp/bench-scalability}"; mkdir -p "$OUT"
LOG="$OUT/proxy-$LABEL.log"
PROXYPID=""

pg(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" "$@"; }
adm(){ curl -s -m 5 "http://$ADMIN$1"; }

# pgbench → (tps, latency_ms). $1 host $2 port; rest = extra pgbench args.
run_pgbench(){
  local host=$1 port=$2; shift 2
  local out
  out=$(pg pgbench -h "$host" -p "$port" -U "$BUSER" -d "$BDB" -n "$@" 2>&1)
  local tps lat
  tps=$(printf '%s' "$out" | grep -oE 'tps = [0-9.]+' | head -1 | grep -oE '[0-9.]+')
  lat=$(printf '%s' "$out" | grep -oE 'latency average = [0-9.]+' | head -1 | grep -oE '[0-9.]+')
  echo "${tps:-NA} ${lat:-NA}"
}

start_proxy(){
  local mode=$1
  local cfg="$OUT/proxy-$mode.toml"
  cat > "$cfg" <<EOF
listen_address = "$PXHOST:$PXPORT"
admin_address  = "$ADMIN"
tr_enabled = false
tr_mode    = "none"
write_timeout_secs = 30

[pool]
min_connections = 1
max_connections = 100
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 5
test_on_acquire = true

[pool_mode]
mode = "$mode"
max_pool_size = 100
reset_query = "DISCARD ALL"

[load_balancer]
read_strategy = "round_robin"
read_write_split = false
latency_threshold_ms = 100

[health]
check_interval_secs = 30
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
name = "pg-primary"
EOF
  RUST_LOG=warn NO_COLOR=1 "$BIN" --config "$cfg" >"$LOG" 2>&1 &
  PROXYPID=$!
  local ready=0
  for _ in $(seq 1 40); do
    if pg psql -h "$PXHOST" -p "$PXPORT" -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1; then ready=1; break; fi
    if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "  proxy died on startup ($mode):"; tail -8 "$LOG"; return 1; fi
    sleep 0.4
  done
  [ "$ready" = 1 ] || { echo "  proxy never ready ($mode)"; tail -12 "$LOG"; return 1; }
}
stop_proxy(){ [ -n "$PROXYPID" ] && kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; PROXYPID=""; }

# Peak backend client-backends seen during a rate-limited run (efficiency probe).
backend_conns(){
  pg psql -h "$PGHOST" -p "$PGPORT" -U "$BUSER" -d "$BDB" -tAc \
    "select count(*) from pg_stat_activity where datname='$BDB' and usename='$BUSER' and backend_type='client backend' and application_name <> 'conn_probe'" 2>/dev/null | tr -d '[:space:]'
}

trap 'stop_proxy; docker start codex-pg184-bench >/dev/null 2>&1 || true' EXIT
echo "=================================================================="
echo " HeliosProxy scalability bench  —  $LABEL  ($("$BIN" --version 2>/dev/null))"
echo " backend=$PGHOST:$PGPORT  scale=20  select-only  dur=${DUR}s  clients=[$CLIENTS]"
echo "=================================================================="

# ---- Axis A: throughput / latency sweep --------------------------------------
declare -A TPS LAT
echo; echo "[A] throughput/latency sweep (persistent conns, pgbench -S)"
for c in $CLIENTS; do
  j=$(( c < 8 ? c : 8 ))
  read -r t l < <(run_pgbench "$PGHOST" "$PGPORT" -S -c "$c" -j "$j" -T "$DUR")
  TPS["direct,$c"]=$t; LAT["direct,$c"]=$l
  printf '  direct        c=%-3s tps=%-12s lat=%sms\n' "$c" "$t" "$l"
done
for mode in $MODES; do
  start_proxy "$mode" || { echo "  skip mode $mode"; continue; }
  for c in $CLIENTS; do
    j=$(( c < 8 ? c : 8 ))
    read -r t l < <(run_pgbench "$PXHOST" "$PXPORT" -S -c "$c" -j "$j" -T "$DUR")
    TPS["$mode,$c"]=$t; LAT["$mode,$c"]=$l
    printf '  proxy/%-11s c=%-3s tps=%-12s lat=%sms\n' "$mode" "$c" "$t" "$l"
  done
  stop_proxy
done

# ---- Axis B: backend-connection efficiency -----------------------------------
echo; echo "[B] backend-connection efficiency ($EFF_CLIENTS clients, -R $EFF_RATE tps, think time)"
declare -A EFF
for mode in $MODES; do
  start_proxy "$mode" || continue
  # launch the rate-limited load in the background, sample backend conns mid-run
  pg pgbench -h "$PXHOST" -p "$PXPORT" -U "$BUSER" -d "$BDB" -n -S \
     -c "$EFF_CLIENTS" -j 8 -T "$DUR" -R "$EFF_RATE" >/dev/null 2>&1 &
  LOADPID=$!
  peak=0
  for _ in $(seq 1 $((DUR*2))); do
    n=$(backend_conns); [ -n "$n" ] && [ "$n" -gt "$peak" ] 2>/dev/null && peak=$n
    sleep 0.5
  done
  wait "$LOADPID" 2>/dev/null
  EFF["$mode"]=$peak
  printf '  proxy/%-11s peak backend client-conns = %s  (for %s proxy clients)\n' "$mode" "$peak" "$EFF_CLIENTS"
  stop_proxy
done

# ---- Summary tables ----------------------------------------------------------
echo; echo "================  SUMMARY: $LABEL  ================"
echo "[A] TPS (higher=better) / latency ms (lower=better), select-only"
printf '%-14s' "clients"; for c in $CLIENTS; do printf '%14s' "$c"; done; echo
printf '%-14s' "direct"; for c in $CLIENTS; do printf '%14s' "${TPS[direct,$c]}"; done; echo
for mode in $MODES; do
  printf '%-14s' "proxy/$mode"; for c in $CLIENTS; do printf '%14s' "${TPS[$mode,$c]:-NA}"; done; echo
done
echo "(latency avg ms)"
printf '%-14s' "direct"; for c in $CLIENTS; do printf '%14s' "${LAT[direct,$c]}"; done; echo
for mode in $MODES; do
  printf '%-14s' "proxy/$mode"; for c in $CLIENTS; do printf '%14s' "${LAT[$mode,$c]:-NA}"; done; echo
done
echo; echo "[B] backend client-connections for $EFF_CLIENTS bursty clients (lower=more scalable)"
for mode in $MODES; do printf '  proxy/%-12s %s\n' "$mode" "${EFF[$mode]:-NA}"; done
echo "==================================================="
