#!/bin/bash
# V-02 HA/chaos laboratory driver.
#
#   tests/lab/run-lab.sh PROXY_BINARY SCENARIO [OUTDIR]
#
# SCENARIO:
#   promote-under-load   ledger through both proxies; pg-a is killed and pg-b
#                        (synchronous standby) promoted with pg_promote().
#   stop-old-primary     ledger through both proxies; pg-a is frozen
#                        (docker pause = SIGSTOP of every process), pg-b
#                        promoted, then pg-a returns unfenced while the ledger
#                        runs; it is fenced (stopped) later.
#   chaos-force-leader   b251750f1d54 drill: ledger through both proxies; pg-a
#                        is held out through each proxy's POST /api/chaos
#                        force_unhealthy (no process touched: the leader is
#                        dropped at once and writes wait), then pg-b is
#                        promoted while pg-a stays up unfenced; the overrides
#                        are reset at the end and pg-a fenced (stopped).
#   drain-restart        ledger through both proxies; proxy-a gets SIGUSR2 (graceful
#                        drain: listener closed, in-flight sessions finish, process
#                        exits) and is started again; writes through it must resume
#                        with no unknown outcome and no cluster change.
#   stale-sighup         ledger through both proxies; proxy-a is sent SIGHUP twice:
#                        first with a file that does not parse (the reload must be
#                        refused and the running config kept), then with a stale
#                        file that predates pg-c (it must be applied: pg-c leaves
#                        /nodes) while writes keep flowing to the provider's leader.
#   tls-rotation         proxy-a terminates client TLS (proxy-a-tls.toml); the ledger's
#                        psql connects with sslmode=prefer. A new certificate is written
#                        over the old files and SIGHUP sent: new connections must show
#                        the new fingerprint, the ledger must not notice.
#   edge-resume          C-03 drill: proxy-a is an edge home, proxy-b its edge,
#                        subscribed through a killable relay. The relay is cut
#                        while a write goes through the home, then restored
#                        (expect a WARM resume: missed invalidation replayed,
#                        rest of the cache kept); then the home is restarted
#                        (expect a GAP: the edge flushes).
#   cache-logical        C-02 slice 2: proxy-a caches reads and follows a
#                        pgoutput slot on pg-a. Writes through proxy-b, direct
#                        writes on pg-a and a trigger's side effect must all
#                        stop proxy-a serving the old cached value.
#   reconnect-wave       H-05 acceptance drill: two tenants (application_name)
#                        drive proxy-a with pgbench, a "big" one with
#                        WAVE_CLIENTS (48) writing clients and a "small" one
#                        with WAVE_SMALL (2) readers, for WAVE_SECS (60); pg-a
#                        is killed and pg-b promoted. Judged: the small tenant
#                        keeps being served, reconnects in progress stay within
#                        [limits] max_concurrent_reconnects_per_node, the proxy's
#                        RSS stays bounded, the big tenant recovers. A
#                        "heavy" tenant (ac858d61400b) runs WAVE_HEAVY (6)
#                        clients of 4 x 64 KiB-statement transactions against
#                        its active-query cap (2) and replay-byte budget
#                        (300000): judged within both, both engaged, and its
#                        replay bytes returned at the end.
#                        LAB_WAVE_CONFIG replaces proxy-wave.toml (e.g. a
#                        control run with another reconnect limit); then
#                        WAVE_HEAVY defaults to 0 (no heavy tenant, no heavy
#                        checks) since the control config has no such tenant.
#
# PROXY_BINARY must be built with `postgres-topology` (and runs inside the
# postgres:18.4-bookworm image, so a glibc build from this host works).
#
# Safety: re-executes itself under the fleet build lock and a 24 GiB / no-swap
# systemd scope; every step has a timeout; the EXIT trap always removes the
# `helios-lab` compose project. Only objects carrying that project's label are
# touched, and no volumes exist (data lives on tmpfs). Never `down -v`.
# Networking: host network, 127.0.0.1 only, ports 27701-27792 (this host cannot
# create Docker bridge networks); the run refuses to start if one is taken.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$(realpath "${1:?usage: run-lab.sh PROXY_BINARY SCENARIO [OUTDIR]}")"
SCENARIO="${2:?usage: run-lab.sh PROXY_BINARY SCENARIO [OUTDIR]}"
OUT="${3:-/home/gpc/HDB/sprint/baselines/proxy/ha-lab-$(date -u +%Y%m%dT%H%M%SZ)-$SCENARIO}"
LOCK="${LOCK:-/home/gpc/HDB/sprint/coordination/build.lock}"
LEDGER_SECS="${LEDGER_SECS:-60}"
LEDGER_WORKERS="${LEDGER_WORKERS:-4}"
STEP_TIMEOUT="${STEP_TIMEOUT:-180}"

case "$SCENARIO" in
  promote-under-load | stop-old-primary | chaos-force-leader | drain-restart) ;;
  stale-sighup)
    # proxy-a reads a copy the driver can rewrite between SIGHUPs.
    mkdir -p "$OUT"
    cp "$HERE/proxy-a.toml" "$OUT/proxy-a-live.toml"
    export LAB_PROXY_A_CONFIG="$OUT/proxy-a-live.toml" ;;
  tls-rotation)
    mkdir -p "$OUT/tls"
    (umask 077; openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=lab-proxy-a \
       -keyout "$OUT/tls/server.key" -out "$OUT/tls/server.crt" >/dev/null 2>&1) \
      || { echo "openssl could not create the lab certificate" >&2; exit 2; }
    chmod 644 "$OUT/tls/server.key" "$OUT/tls/server.crt"
    export LAB_PROXY_A_CONFIG=./proxy-a-tls.toml LAB_TLS_DIR="$OUT/tls" ;;
  edge-resume)
    export LAB_PROXY_A_CONFIG=./proxy-home.toml LAB_PROXY_B_CONFIG=./proxy-edge.toml ;;
  cache-logical)
    export LAB_PROXY_A_CONFIG=./proxy-cache.toml ;;
  reconnect-wave)
    export LAB_PROXY_A_CONFIG="${LAB_WAVE_CONFIG:-./proxy-wave.toml}" ;;
  *) echo "unknown scenario: $SCENARIO" >&2; exit 2 ;;
esac
[ -x "$BIN" ] || { echo "not executable: $BIN" >&2; exit 2; }

# reconnect-wave: the heavy tenant's clients; a control config (LAB_WAVE_CONFIG)
# has no heavy tenant unless asked for.
WAVE_HEAVY="${WAVE_HEAVY:-$([ -n "${LAB_WAVE_CONFIG:-}" ] && echo 0 || echo 6)}"

if [ "${LAB_LOCKED:-0}" != 1 ]; then
  exec flock "$LOCK" systemd-run --user --scope --collect --quiet \
    -p MemoryMax=24G -p MemorySwapMax=0 \
    env LAB_LOCKED=1 LEDGER_SECS="$LEDGER_SECS" LEDGER_WORKERS="$LEDGER_WORKERS" \
    WAVE_CLIENTS="${WAVE_CLIENTS:-48}" WAVE_SMALL="${WAVE_SMALL:-2}" WAVE_SECS="${WAVE_SECS:-60}" \
    WAVE_HEAVY="$WAVE_HEAVY" \
    STEP_TIMEOUT="$STEP_TIMEOUT" LAB_PROXY_A_CONFIG="${LAB_PROXY_A_CONFIG:-./proxy-a.toml}" \
    LAB_TLS_DIR="${LAB_TLS_DIR:-./}" \
    LAB_PROXY_B_CONFIG="${LAB_PROXY_B_CONFIG:-./proxy-b.toml}" bash "$0" "$BIN" "$SCENARIO" "$OUT"
fi

export HELIOS_PROXY_BIN="$BIN"
PROJECT=helios-lab
COMPOSE=(docker compose -p "$PROJECT" -f "$HERE/lab.yml")
mkdir -p "$OUT"
LAB_PORTS="27701 27702 27703 27710 27711 27790 27791 27792"
port_of() {
  case "$1" in
    pg-a) echo 27701 ;; pg-b) echo 27702 ;; pg-c) echo 27703 ;;
    proxy-a) echo 27710 ;; proxy-b) echo 27711 ;;
  esac
}
admin_of() { case "$1" in proxy-a) echo 27790 ;; proxy-b) echo 27791 ;; esac; }
admin_get() { curl -s -m 5 -H "Authorization: Bearer helios-lab-admin" "http://127.0.0.1:$(admin_of "$1")/$2"; }
admin_post() { curl -s -m 5 -X POST -H "Authorization: Bearer helios-lab-admin" -H "Content-Type: application/json" -d "$3" "http://127.0.0.1:$(admin_of "$1")/$2"; }
RELAY_PID=
SAMPLER_PID=
log() { echo "[$(date -u +%T)] $*" | tee -a "$OUT/driver.log"; }
# The SHA-256 fingerprint of the certificate proxy-a presents to a new client.
tls_fingerprint() {
  openssl s_client -connect 127.0.0.1:27710 -starttls postgres </dev/null 2>/dev/null \
    | openssl x509 -noout -fingerprint -sha256 2>/dev/null | sed 's/^.*=//'
}
proxy_container_state() { docker inspect -f '{{.State.Status}}' "$PROJECT-$1-1" 2>/dev/null || echo missing; }
# Bound every step. `timeout` cannot run a shell function, so the lab's own
# helpers (ledger, pg, via) bound themselves and are called directly.
step() {
  if declare -F "$1" >/dev/null; then "$@"; else timeout "$STEP_TIMEOUT" "$@"; fi
}
lab_objects() {
  docker ps -aq --filter "label=com.docker.compose.project=$PROJECT"
  docker network ls -q --filter "label=com.docker.compose.project=$PROJECT"
  docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT"
}

cleanup() {
  local rc=$?
  set +e
  log "teardown (exit $rc)"
  [ -n "$RELAY_PID" ] && kill "$RELAY_PID" 2>/dev/null
  [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null
  "${COMPOSE[@]}" logs --no-color --timestamps >"$OUT/compose.log" 2>&1
  "${COMPOSE[@]}" down --remove-orphans --timeout 5 >>"$OUT/driver.log" 2>&1
  local left
  left="$(lab_objects)"
  if [ -n "$left" ]; then
    log "LEFTOVER helios-lab objects: $(echo "$left" | tr '\n' ' ')"
  else
    log "no helios-lab containers, networks or volumes left"
  fi
}

if [ -n "$(lab_objects)" ]; then
  echo "helios-lab objects already exist (a previous run did not finish); remove them with:" >&2
  echo "  docker compose -p $PROJECT -f $HERE/lab.yml down --remove-orphans" >&2
  exit 2
fi
for port in $LAB_PORTS; do
  if ss -ltn "( sport = :$port )" | grep -q LISTEN; then
    echo "port $port is already in use; the lab needs 127.0.0.1:{$LAB_PORTS}" >&2
    exit 2
  fi
done
trap cleanup EXIT

sha256sum "$BIN" | tee "$OUT/binary.sha256"
log "scenario $SCENARIO, ledger ${LEDGER_SECS}s x ${LEDGER_WORKERS} workers"
start_relay() { # the edge's path to its home's admin API (edge-resume)
  python3 -I "$HERE/../../scripts/regress/tcp-relay.py" 27792 127.0.0.1 27790 >>"$OUT/relay.log" 2>&1 &
  RELAY_PID=$!
  # A relay that cannot listen must fail the run here, not as a missing edge
  # subscription later.
  for _ in $(seq 1 50); do
    ss -ltnH 2>/dev/null | grep -q "127.0.0.1:27792 " && return 0
    kill -0 "$RELAY_PID" 2>/dev/null || { log "relay could not listen on 127.0.0.1:27792 (relay.log)"; exit 1; }
    sleep 0.1
  done
  log "relay did not listen on 127.0.0.1:27792 within 5 s"; exit 1
}
[ "$SCENARIO" = edge-resume ] && start_relay
lab_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
step "${COMPOSE[@]}" up --detach --wait --wait-timeout 150 >>"$OUT/driver.log" 2>&1
ledger() { timeout "$((STEP_TIMEOUT + LEDGER_SECS))" "${COMPOSE[@]}" exec -T ledger bash /lab/ledger.sh "$@"; }
pg() { local node=$1; shift; timeout "$STEP_TIMEOUT" "${COMPOSE[@]}" exec -T "$node" psql -X -At -h 127.0.0.1 -p "$(port_of "$node")" -U lab -d lab "$@"; }
via() { local name=$1; shift; timeout "$STEP_TIMEOUT" "${COMPOSE[@]}" exec -T ledger psql -X -At -h 127.0.0.1 -p "$(port_of "$name")" -U lab -d lab -v ON_ERROR_STOP=1 -c "$*"; }

# Both proxies must be serving before the ledger starts.
for p in proxy-a proxy-b; do
  for _ in $(seq 1 60); do
    via "$p" "SELECT 1" >/dev/null 2>&1 && break
    sleep 1
  done
done
log "replication: $(pg pg-a -c "SELECT string_agg(application_name || ':' || sync_state, ',') FROM pg_stat_replication")"

if [ "$SCENARIO" = reconnect-wave ]; then
  WAVE_CLIENTS="${WAVE_CLIENTS:-48}" WAVE_SMALL="${WAVE_SMALL:-2}" WAVE_SECS="${WAVE_SECS:-60}"
  step "${COMPOSE[@]}" exec -T ledger pgbench -i -q -s 4 -h 127.0.0.1 -p 27701 -U lab lab >>"$OUT/driver.log" 2>&1
  pgb() { # TENANT CLIENTS THREADS PGBENCH-ARGS...
    local tenant=$1 clients=$2 threads=$3; shift 3
    timeout "$((WAVE_SECS + STEP_TIMEOUT))" "${COMPOSE[@]}" exec -T -e PGAPPNAME="$tenant" ledger \
      pgbench -h 127.0.0.1 -p 27710 -U lab -n -c "$clients" -j "$threads" -T "$WAVE_SECS" -P 1 "$@" lab
  }
  # The heavy tenant's transaction: the large statements first, so each one
  # holds ~262 kB of replay history for ~0.3 s and the other permit holder's
  # statements overflow the tenant's 300000-byte budget.
  if [ "$WAVE_HEAVY" -gt 0 ]; then
    lit=$(head -c 65536 /dev/zero | tr '\0' x)
    { echo "BEGIN;"; for _ in 1 2 3 4; do echo "SELECT length('$lit');"; done
      echo "SELECT pg_sleep(0.3);"; echo "END;"; } \
      | "${COMPOSE[@]}" exec -T ledger sh -c 'cat >/results/wave-heavy.sql'
  fi
  proxy_pid="$(docker inspect -f '{{.State.Pid}}' "$PROJECT-proxy-a-1")"
  python3 -I "$HERE/wave_sampler.py" "http://127.0.0.1:27790" helios-lab-admin "$proxy_pid" \
    "$OUT/wave-samples.tsv" "$OUT/wave-stop" 2>>"$OUT/driver.log" &
  SAMPLER_PID=$!
  date +%s%3N >"$OUT/wave_start_ms"
  pgb big "$WAVE_CLIENTS" 4 -b tpcb-like >"$OUT/pgbench-big.log" 2>&1 &
  BIG_PID=$!
  pgb small "$WAVE_SMALL" 1 -S >"$OUT/pgbench-small.log" 2>&1 &
  SMALL_PID=$!
  HEAVY_PID=
  if [ "$WAVE_HEAVY" -gt 0 ]; then
    pgb heavy "$WAVE_HEAVY" 1 -f /results/wave-heavy.sql >"$OUT/pgbench-heavy.log" 2>&1 &
    HEAVY_PID=$!
  fi
  sleep 15
  log "kill -9 pg-a"
  # Timed before the kill is issued: `docker compose kill` can take seconds on a
  # loaded host, and the verdict's "served before the kill" window ends here.
  date +%s%3N >"$OUT/kill_ms"
  "${COMPOSE[@]}" kill -s KILL pg-a >>"$OUT/driver.log" 2>&1
  sleep 2
  date +%s%3N >"$OUT/promoted_ms"
  log "promote pg-b: $(pg pg-b -c "SELECT pg_promote(true, 60)")"
  wait "$BIG_PID" || true
  wait "$SMALL_PID" || true
  [ -n "${HEAVY_PID:-}" ] && { wait "$HEAVY_PID" || true; }
  touch "$OUT/wave-stop"
  wait "$SAMPLER_PID" || true
  SAMPLER_PID=
  admin_get proxy-a metrics/prometheus >"$OUT/metrics-prometheus-proxy-a.prom" 2>&1 || true
  admin_get proxy-a metrics >"$OUT/metrics-proxy-a.json" 2>&1 || true
  echo "$WAVE_SECS" >"$OUT/wave_secs"
  python3 -I "$HERE/verdict.py" "$SCENARIO" "$OUT" | tee "$OUT/verdict.txt"
  exit "${PIPESTATUS[0]}"
fi

if [ "$SCENARIO" = cache-logical ]; then
  metrics() { admin_get proxy-a metrics; }
  settle() { sleep 2; }  # > 3 x logical_feedback_ms (500)
  step pg pg-a -v ON_ERROR_STOP=1 -c "
    CREATE TABLE kv (id int PRIMARY KEY, v int);
    CREATE TABLE audit (id int PRIMARY KEY, n int);
    CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS
      'BEGIN UPDATE audit SET n = n + 1 WHERE id = 1; RETURN NEW; END';
    CREATE TRIGGER kv_audit AFTER UPDATE ON kv FOR EACH ROW EXECUTE FUNCTION bump();
    INSERT INTO kv VALUES (1, 10); INSERT INTO audit VALUES (1, 0);" >>"$OUT/driver.log" 2>&1
  settle
  for _ in 1 2 3; do
    via proxy-a "SELECT v FROM kv WHERE id = 1" >/dev/null
    via proxy-a "SELECT n FROM audit WHERE id = 1" >/dev/null
  done
  metrics >"$OUT/metrics-0-warmed.json"
  # 1. a write through the OTHER proxy (bypasses proxy-a entirely)
  via proxy-b "UPDATE kv SET v = 11 WHERE id = 1" >>"$OUT/driver.log" 2>&1
  settle
  r1kv="$(via proxy-a "SELECT v FROM kv WHERE id = 1")"; r1au="$(via proxy-a "SELECT n FROM audit WHERE id = 1")"
  # 2. a direct write on the primary
  pg pg-a -c "UPDATE kv SET v = 12 WHERE id = 1" >>"$OUT/driver.log" 2>&1
  settle
  r2kv="$(via proxy-a "SELECT v FROM kv WHERE id = 1")"; r2au="$(via proxy-a "SELECT n FROM audit WHERE id = 1")"
  # 3. a write through proxy-a itself: it invalidates kv by the statement; audit is
  #    written only by the trigger, which only the logical source can see.
  via proxy-a "SELECT n FROM audit WHERE id = 1" >/dev/null
  via proxy-a "UPDATE kv SET v = 13 WHERE id = 1" >>"$OUT/driver.log" 2>&1
  settle
  r3au="$(via proxy-a "SELECT n FROM audit WHERE id = 1")"
  metrics >"$OUT/metrics-1-end.json"
  printf '{"after_proxy_b":{"kv":"%s","audit":"%s"},"after_direct":{"kv":"%s","audit":"%s"},"after_trigger":{"audit":"%s"}}\n' \
    "$r1kv" "$r1au" "$r2kv" "$r2au" "$r3au" >"$OUT/cache-drill.json"
  cat "$OUT/cache-drill.json" | tee -a "$OUT/driver.log"
  python3 -I "$HERE/verdict.py" "$SCENARIO" "$OUT" | tee "$OUT/verdict.txt"
  exit "${PIPESTATUS[0]}"
fi

if [ "$SCENARIO" = edge-resume ]; then
  edge_api() { admin_get proxy-b api/edge; }
  edge_log_has() { "${COMPOSE[@]}" logs --no-color --since "$1" proxy-b 2>&1 | grep -q "$2"; }
  wait_log() { # SINCE PATTERN SECS
    for _ in $(seq 1 "$3"); do edge_log_has "$1" "$2" && return 0; sleep 1; done
    return 1
  }
  # Edge invalidation is per table: the entry the warm resume must keep lives
  # in a table the missed write does not touch.
  step via proxy-a "CREATE TABLE kv (id int PRIMARY KEY, v int); INSERT INTO kv VALUES (1, 10);
    CREATE TABLE kv_other (id int PRIMARY KEY, v int); INSERT INTO kv_other VALUES (2, 20)" >>"$OUT/driver.log" 2>&1
  wait_log "$lab_start" "subscribed to home invalidation stream" 30 || log "edge never reported a subscription"
  for _ in 1 2 3; do
    via proxy-b "SELECT v FROM kv WHERE id = 1" >/dev/null
    via proxy-b "SELECT v FROM kv_other WHERE id = 2" >/dev/null
  done
  edge_api >"$OUT/edge-0-warmed.json"
  log "warm drill: cut the edge's stream (kill the relay), write through the home, restore it"
  kill "$RELAY_PID"; wait "$RELAY_PID" 2>/dev/null; RELAY_PID=
  sleep 3
  step via proxy-a "UPDATE kv SET v = 11 WHERE id = 1" >>"$OUT/driver.log" 2>&1
  sleep 3
  since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  start_relay
  if wait_log "$since" "warm resume" 60; then resumed=warm
  elif edge_log_has "$since" "invalidation gap"; then resumed=gap
  else resumed=none; fi
  log "edge reconnect after the stream cut: $resumed"
  edge_api >"$OUT/edge-1-reconnected.json"
  read1="$(via proxy-b "SELECT v FROM kv WHERE id = 1")"
  edge_api >"$OUT/edge-2-after-id1.json"
  read2="$(via proxy-b "SELECT v FROM kv_other WHERE id = 2")"
  edge_api >"$OUT/edge-3-after-id2.json"
  log "gap drill: restart the home (new boot identity)"
  since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "${COMPOSE[@]}" restart proxy-a >>"$OUT/driver.log" 2>&1
  if wait_log "$since" "invalidation gap\|home epoch changed" 90; then restarted=flushed; else restarted=none; fi
  log "edge after home restart: $restarted"
  edge_api >"$OUT/edge-4-home-restarted.json"
  read3="$(via proxy-b "SELECT v FROM kv_other WHERE id = 2")"
  edge_api >"$OUT/edge-5-after-id2.json"
  printf '{"resumed":"%s","read1":"%s","read2":"%s","home_restart":"%s","read3":"%s"}\n' \
    "$resumed" "$read1" "$read2" "$restarted" "$read3" >"$OUT/edge-drill.json"
  cat "$OUT/edge-drill.json" | tee -a "$OUT/driver.log"
  python3 -I "$HERE/verdict.py" "$SCENARIO" "$OUT" | tee "$OUT/verdict.txt"
  exit "${PIPESTATUS[0]}"
fi

step ledger init >>"$OUT/driver.log" 2>&1

ledger run "$LEDGER_SECS" "$LEDGER_WORKERS" >"$OUT/ledger-run.log" 2>&1 &
LEDGER_PID=$!
sleep 10

case "$SCENARIO" in
promote-under-load)
  log "kill -9 pg-a"
  "${COMPOSE[@]}" kill -s KILL pg-a >>"$OUT/driver.log" 2>&1
  sleep 2
  date +%s%3N >"$OUT/promoted_ms"
  log "promote pg-b: $(pg pg-b -c "SELECT pg_promote(true, 60)")"
  ;;
stop-old-primary)
  log "pause (SIGSTOP) pg-a"
  "${COMPOSE[@]}" pause pg-a >>"$OUT/driver.log" 2>&1
  sleep 2
  date +%s%3N >"$OUT/promoted_ms"
  log "promote pg-b: $(pg pg-b -c "SELECT pg_promote(true, 60)")"
  sleep 15
  log "unpause pg-a: it returns as an unfenced, writable former primary"
  "${COMPOSE[@]}" unpause pg-a >>"$OUT/driver.log" 2>&1
  date +%s%3N >"$OUT/unpaused_ms"
  sleep 15
  log "pg-a in_recovery=$(pg pg-a -c "SELECT pg_is_in_recovery()"), pg-b in_recovery=$(pg pg-b -c "SELECT pg_is_in_recovery()")"
  ;;
chaos-force-leader)
  for p in proxy-a proxy-b; do
    log "$p: force_unhealthy pg-a -> $(admin_post "$p" api/chaos '{"action":"force_unhealthy","target_node":"127.0.0.1:27701"}')"
    admin_get "$p" topology >"$OUT/topology-forced-$p.json" 2>&1 || true
    admin_get "$p" nodes >"$OUT/nodes-forced-$p.json" 2>&1 || true
  done
  log "proxy-a topology after the override: $(cat "$OUT/topology-forced-proxy-a.json")"
  sleep 2
  date +%s%3N >"$OUT/promoted_ms"
  log "promote pg-b: $(pg pg-b -c "SELECT pg_promote(true, 60)")"
  sleep 15
  log "pg-a in_recovery=$(pg pg-a -c "SELECT pg_is_in_recovery()"), pg-b in_recovery=$(pg pg-b -c "SELECT pg_is_in_recovery()")"
  ;;
drain-restart)
  log "SIGUSR2 proxy-a: graceful drain (listener closed, in-flight sessions finish, then exit)"
  date +%s%3N >"$OUT/drained_ms"
  "${COMPOSE[@]}" kill -s USR2 proxy-a >>"$OUT/driver.log" 2>&1
  for _ in $(seq 1 240); do
    [ "$(proxy_container_state proxy-a)" = exited ] && break
    sleep 0.5
  done
  date +%s%3N >"$OUT/exited_ms"
  log "proxy-a after the drain: $(proxy_container_state proxy-a) ($(( ($(cat "$OUT/exited_ms") - $(cat "$OUT/drained_ms")) )) ms)"
  log "start proxy-a again"
  "${COMPOSE[@]}" up -d --no-deps proxy-a >>"$OUT/driver.log" 2>&1
  for _ in $(seq 1 120); do via proxy-a "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5; done
  date +%s%3N >"$OUT/promoted_ms"   # the instant writes through proxy-a may resume
  log "proxy-a serving again ($(( ($(cat "$OUT/promoted_ms") - $(cat "$OUT/exited_ms")) )) ms after it exited)"
  ;;
stale-sighup)
  log "SIGHUP proxy-a with a file that does not parse: the reload must be refused"
  printf 'listen_address = "127.0.0.1:27710"\n[this is not toml\n' >"$OUT/proxy-a-live.toml"
  "${COMPOSE[@]}" kill -s HUP proxy-a >>"$OUT/driver.log" 2>&1
  sleep 3
  admin_get proxy-a nodes >"$OUT/nodes-after-broken-reload.json" 2>&1 || true
  log "SIGHUP proxy-a with a stale file that predates pg-c: pg-c must leave /nodes, writes must go on"
  sed '/^\[\[nodes\]\]$/{N;N;N;/port = 27703/d}' "$HERE/proxy-a.toml" >"$OUT/proxy-a-live.toml"
  grep -q 27703 "$OUT/proxy-a-live.toml" && { log "stale file still names pg-c"; exit 1; }
  "${COMPOSE[@]}" kill -s HUP proxy-a >>"$OUT/driver.log" 2>&1
  sleep 3
  admin_get proxy-a nodes >"$OUT/nodes-after-stale-reload.json" 2>&1 || true
  admin_get proxy-a topology >"$OUT/topology-after-stale-reload.json" 2>&1 || true
  date +%s%3N >"$OUT/promoted_ms"   # no promotion: the instant after the second reload
  ;;
tls-rotation)
  before="$(tls_fingerprint)"
  log "proxy-a presents $before; writing a new certificate over the old files, then SIGHUP"
  (umask 077; openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=lab-proxy-a-rotated \
     -keyout "$OUT/tls/server.key" -out "$OUT/tls/server.crt" >/dev/null 2>&1)
  chmod 644 "$OUT/tls/server.key" "$OUT/tls/server.crt"
  file="$(openssl x509 -in "$OUT/tls/server.crt" -noout -fingerprint -sha256 | sed 's/^.*=//')"
  date +%s%3N >"$OUT/rotated_ms"
  "${COMPOSE[@]}" kill -s HUP proxy-a >>"$OUT/driver.log" 2>&1
  sleep 2
  after="$(tls_fingerprint)"
  log "after SIGHUP proxy-a presents $after (new file: $file)"
  printf '{"before":"%s","after":"%s","file":"%s"}\n' "$before" "$after" "$file" >"$OUT/tls-fingerprints.json"
  date +%s%3N >"$OUT/promoted_ms"   # no promotion: the instant after the rotation
  ;;
esac

wait "$LEDGER_PID" || true
log "ledger: $(tail -1 "$OUT/ledger-run.log")"
for p in proxy-a proxy-b; do
  admin_get "$p" topology >"$OUT/topology-$p.json" 2>&1 || true
  admin_get "$p" metrics/prometheus >"$OUT/metrics-prometheus-$p.prom" 2>&1 || true
done

# The node the ledger is checked against: pg-b after a promotion, pg-a when the
# scenario never promotes (a standby refuses the check's temp table with 25006).
case "$SCENARIO" in
  drain-restart | stale-sighup | tls-rotation) CHECK_HOST=pg-a ;;
  *) CHECK_HOST=pg-b ;;
esac
if [ "$SCENARIO" = stop-old-primary ] || [ "$SCENARIO" = chaos-force-leader ]; then
  step ledger check pg-b pg-a >"$OUT/check.json" 2>&1 || true
  if [ "$SCENARIO" = chaos-force-leader ]; then
    for p in proxy-a proxy-b; do
      admin_get "$p" api/chaos >"$OUT/chaos-$p.json" 2>&1 || true
      log "$p: chaos reset -> $(admin_post "$p" api/chaos '{"action":"reset"}')"
    done
  fi
  log "fence pg-a (stop)"
  "${COMPOSE[@]}" stop --timeout 5 pg-a >>"$OUT/driver.log" 2>&1
else
  step ledger check "$CHECK_HOST" >"$OUT/check.json" 2>&1 || true
fi
"${COMPOSE[@]}" exec -T ledger cat /results/outcomes.tsv >"$OUT/outcomes.tsv" 2>/dev/null || true
cat "$OUT/check.json" | tee -a "$OUT/driver.log"
# proxy-a's log for the verdicts that read it (compose.log is written at teardown).
"${COMPOSE[@]}" logs --no-color --timestamps proxy-a >"$OUT/proxy-a.log" 2>&1 || true

python3 -I "$HERE/verdict.py" "$SCENARIO" "$OUT" | tee "$OUT/verdict.txt"
