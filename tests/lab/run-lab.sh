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
#
# PROXY_BINARY must be built with `postgres-topology` (and runs inside the
# postgres:18.4-bookworm image, so a glibc build from this host works).
#
# Safety: re-executes itself under the fleet build lock and a 24 GiB / no-swap
# systemd scope; every step has a timeout; the EXIT trap always removes the
# `helios-lab` compose project. Only objects carrying that project's label are
# touched, and no volumes exist (data lives on tmpfs). Never `down -v`.
# Networking: host network, 127.0.0.1 only, ports 35701-35792 (this host cannot
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
  promote-under-load | stop-old-primary) ;;
  edge-resume)
    export LAB_PROXY_A_CONFIG=./proxy-home.toml LAB_PROXY_B_CONFIG=./proxy-edge.toml ;;
  cache-logical)
    export LAB_PROXY_A_CONFIG=./proxy-cache.toml ;;
  *) echo "unknown scenario: $SCENARIO" >&2; exit 2 ;;
esac
[ -x "$BIN" ] || { echo "not executable: $BIN" >&2; exit 2; }

if [ "${LAB_LOCKED:-0}" != 1 ]; then
  exec flock "$LOCK" systemd-run --user --scope --collect --quiet \
    -p MemoryMax=24G -p MemorySwapMax=0 \
    env LAB_LOCKED=1 LEDGER_SECS="$LEDGER_SECS" LEDGER_WORKERS="$LEDGER_WORKERS" \
    STEP_TIMEOUT="$STEP_TIMEOUT" LAB_PROXY_A_CONFIG="${LAB_PROXY_A_CONFIG:-./proxy-a.toml}" \
    LAB_PROXY_B_CONFIG="${LAB_PROXY_B_CONFIG:-./proxy-b.toml}" bash "$0" "$BIN" "$SCENARIO" "$OUT"
fi

export HELIOS_PROXY_BIN="$BIN"
PROJECT=helios-lab
COMPOSE=(docker compose -p "$PROJECT" -f "$HERE/lab.yml")
mkdir -p "$OUT"
LAB_PORTS="35701 35702 35703 35710 35711 35790 35791 35792"
port_of() {
  case "$1" in
    pg-a) echo 35701 ;; pg-b) echo 35702 ;; pg-c) echo 35703 ;;
    proxy-a) echo 35710 ;; proxy-b) echo 35711 ;;
  esac
}
admin_of() { case "$1" in proxy-a) echo 35790 ;; proxy-b) echo 35791 ;; esac; }
admin_get() { curl -s -m 5 -H "Authorization: Bearer helios-lab-admin" "http://127.0.0.1:$(admin_of "$1")/$2"; }
RELAY_PID=
log() { echo "[$(date -u +%T)] $*" | tee -a "$OUT/driver.log"; }
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
  python3 -I "$HERE/../../scripts/regress/tcp-relay.py" 35792 127.0.0.1 35790 >>"$OUT/relay.log" 2>&1 &
  RELAY_PID=$!
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
esac

wait "$LEDGER_PID" || true
log "ledger: $(tail -1 "$OUT/ledger-run.log")"
for p in proxy-a proxy-b; do
  admin_get "$p" topology >"$OUT/topology-$p.json" 2>&1 || true
  admin_get "$p" metrics/prometheus >"$OUT/metrics-prometheus-$p.prom" 2>&1 || true
done

if [ "$SCENARIO" = stop-old-primary ]; then
  step ledger check pg-b pg-a >"$OUT/check.json" 2>&1 || true
  log "fence pg-a (stop)"
  "${COMPOSE[@]}" stop --timeout 5 pg-a >>"$OUT/driver.log" 2>&1
else
  step ledger check pg-b >"$OUT/check.json" 2>&1 || true
fi
"${COMPOSE[@]}" exec -T ledger cat /results/outcomes.tsv >"$OUT/outcomes.tsv" 2>/dev/null || true
cat "$OUT/check.json" | tee -a "$OUT/driver.log"

python3 -I "$HERE/verdict.py" "$SCENARIO" "$OUT" | tee "$OUT/verdict.txt"
