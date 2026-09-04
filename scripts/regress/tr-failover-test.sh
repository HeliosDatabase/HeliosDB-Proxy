#!/usr/bin/env bash
# HeliosProxy live test — in-session Transaction Replay (`tr_mode`), F3.
#
# Proves that a LIVE client session survives (or fails cleanly through) the loss
# of its backend connection, per tr_mode. No second PostgreSQL is needed: the
# proxy is configured with TWO `role = "primary"` nodes that reach the SAME
# database — node A is a killable TCP relay (scripts/regress/tcp-relay.py) in
# front of PG, node B is PG directly. Sessions start on A (first healthy
# primary); SIGTERM-ing the relay kills every backend socket of the sessions on
# A exactly like a backend crash, and the proxy must fail the session over to B.
#
# Scenarios (psycopg2, simple protocol) per mode:
#   S1 idle-in-tx : SET application_name; BEGIN; INSERT 'a'; SELECT count —
#                   kill relay while idle in the transaction — INSERT 'b'; COMMIT
#   S2 in-flight  : autocommit `SELECT pg_sleep(0.5), 42` with the relay killed
#                   0.1 s into the statement (outcome unknown)
#   S3 idle-no-tx : SET; kill relay while idle; SELECT 1 / SHOW application_name
#   S4 commit-race: (transaction mode) BEGIN; INSERT 'c'; kill relay and send
#                   COMMIT immediately -> 08007 (0 rows) OR transparent (1 row);
#                   never 2 rows.
#   S5 commit-in-flight: (transaction mode) a deferred constraint trigger makes
#                   COMMIT take 0.6 s; the relay dies mid-COMMIT -> 08007, no
#                   replay, 0 or 1 row (never 2), session still usable.
#
# Usage:  scripts/regress/tr-failover-test.sh /path/to/heliosdb-proxy
# Needs: PG 18.4 at 127.0.0.1:25433 (bench/benchpass/benchdb), python3 + psycopg2.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="${1:?usage: tr-failover-test.sh <proxy-binary>}"
PGHOST=${PGHOST:-127.0.0.1}; PGPORT=${PGPORT:-25433}
BUSER=${BUSER:-bench}; BPASS=${BPASS:-benchpass}; BDB=${BDB:-benchdb}
PROXY_PORT=${PROXY_PORT:-65431}
ADMIN_PORT=${ADMIN_PORT:-9199}
RELAY_PORT=${RELAY_PORT:-25441}
MODES=${MODES:-"none session select transaction"}

OUT="${OUT:-/tmp/regress-tr-failover}"; mkdir -p "$OUT"
PASS=0; FAIL=0
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s %s\n' "$1" "${2:-}"; }
bad(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s %s\n' "$1" "${2:-}"; }

PROXYPID=""; RELAYPID=""
cleanup(){
  [ -n "$PROXYPID" ] && kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null
  [ -n "$RELAYPID" ] && kill "$RELAYPID" 2>/dev/null; wait "$RELAYPID" 2>/dev/null
}
trap cleanup EXIT

python3 -c "import psycopg2" 2>/dev/null || { echo "python3 psycopg2 required"; exit 1; }
for p in "$PROXY_PORT" "$ADMIN_PORT" "$RELAY_PORT"; do
  if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$p\$"; then echo "port $p in use"; exit 1; fi
done

# The proxy must be the auth boundary to re-home a session onto a SCRAM backend:
# in pass-through mode it never sees the client's password, so a fresh backend
# connection could only be opened to a trust backend. A plaintext auth_file
# entry lets the proxy authenticate the client (SCRAM) AND its own backend
# connections (SCRAM client) with the same secret.
printf '%s:%s\n' "$BUSER" "$BPASS" > "$OUT/users.txt"; chmod 600 "$OUT/users.txt"

write_cfg(){ # $1 = mode
cat > "$OUT/proxy-$1.toml" <<EOF
listen_address = "127.0.0.1:$PROXY_PORT"
admin_address  = "127.0.0.1:$ADMIN_PORT"
tr_enabled = false
tr_mode    = "$1"
write_timeout_secs = 10

[auth]
mode = "scram"
auth_file = "$OUT/users.txt"

[pool]
min_connections = 1
max_connections = 20
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 5
test_on_acquire = true

[load_balancer]
read_strategy = "round_robin"
read_write_split = false
latency_threshold_ms = 100

[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
check_query = "SELECT 1"

[limits]
tr_max_replay_statements = 1000
tr_max_replay_bytes = 4194304
tr_max_session_set_statements = 256

[[nodes]]
host = "127.0.0.1"
port = $RELAY_PORT
role = "primary"
weight = 100
enabled = true
name = "relay-a"

[[nodes]]
host = "127.0.0.1"
port = $PGPORT
role = "primary"
weight = 100
enabled = true
name = "pg-b"
EOF
}

# The client driver: runs every scenario for one mode against the running proxy,
# managing the relay's lifecycle (kill / respawn) itself. Prints `OK <name>` /
# `BAD <name> <detail>` lines.
cat > "$OUT/client.py" <<'PYEOF'
import atexit, json, os, signal, subprocess, sys, threading, time, urllib.request
import psycopg2

MODE, PROXY_PORT, ADMIN_PORT, RELAY_PORT, RELAY_PID, RELAY_PY, PGHOST, PGPORT, USER, PW, DB = sys.argv[1:12]
PROXY_PORT = int(PROXY_PORT); ADMIN_PORT = int(ADMIN_PORT); RELAY_PORT = int(RELAY_PORT); PGPORT = int(PGPORT)
relay_pid = int(RELAY_PID)
RELAY_ADDR = f"127.0.0.1:{RELAY_PORT}"

def ok(n, d=""): print(f"OK {n} {d}".rstrip(), flush=True)
def bad(n, d=""): print(f"BAD {n} {d}".rstrip(), flush=True)
def check(cond, n, d=""): (ok if cond else bad)(n, d)

def admin(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{ADMIN_PORT}{path}", timeout=5) as r:
        return json.loads(r.read())

def relay_health():
    for n in admin("/nodes"):
        if n["address"] == RELAY_ADDR:
            return n["healthy"]
    return None

def wait_relay(healthy, timeout=8.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if relay_health() is healthy:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False

def kill_relay():
    global relay_pid
    if relay_pid:
        try:
            os.kill(relay_pid, signal.SIGTERM)
            for _ in range(50):
                try:
                    os.kill(relay_pid, 0); time.sleep(0.05)
                except ProcessLookupError:
                    break
        except ProcessLookupError:
            pass
        relay_pid = 0

atexit.register(kill_relay)  # never leave a spawned relay behind, even on a crash

def start_relay():
    global relay_pid
    p = subprocess.Popen([sys.executable, RELAY_PY, str(RELAY_PORT), PGHOST, str(PGPORT)],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    relay_pid = p.pid
    # readiness: the proxy's health checker must see it (1 s interval)
    if not wait_relay(True, 10):
        bad("relay_restart_healthy", "relay never became healthy on /nodes")
    time.sleep(0.3)

def connect(via_proxy=True):
    c = psycopg2.connect(host="127.0.0.1", port=PROXY_PORT if via_proxy else PGPORT,
                         user=USER, password=PW, dbname=DB, connect_timeout=10, sslmode="disable")
    c.autocommit = True
    return c

def q(c, sql):
    cur = c.cursor(); cur.execute(sql)
    try:
        return cur.fetchall()
    except psycopg2.ProgrammingError:
        return None

def pg_rows(v):
    d = connect(False); r = q(d, f"SELECT count(*) FROM tr_f3_t WHERE v = '{v}'")[0][0]; d.close(); return r

def metrics(): return admin("/metrics")

def wait_metric(name, pred, timeout=8.0):
    """The admin /metrics snapshot is synced periodically: poll until `pred(value)`."""
    t0 = time.time(); v = None
    while time.time() - t0 < timeout:
        v = metrics()[name]
        if pred(v):
            return True, v
        time.sleep(0.25)
    return False, v

def code_of(e):
    return getattr(e, "pgcode", None) or ""

def closed_after(c):
    if c.closed: return True
    try:
        q(c, "SELECT 1"); return False
    except Exception:
        return True

# ---------------------------------------------------------------- setup
direct = connect(False)
q(direct, "CREATE TABLE IF NOT EXISTS tr_f3_t(id serial PRIMARY KEY, v text)")
q(direct, "TRUNCATE tr_f3_t")
direct.close()
check(wait_relay(True, 10), f"{MODE}.relay_healthy_at_start")

# ---------------------------------------------------------------- S1: idle in tx, relay killed
m0 = metrics()
c = connect()
q(c, "SET application_name = 'tr-f3'")
q(c, "BEGIN")
q(c, "INSERT INTO tr_f3_t(v) VALUES ('a')")
check(q(c, "SELECT count(*) FROM tr_f3_t")[0][0] == 1, f"{MODE}.s1_in_tx_visible")
kill_relay()
check(wait_relay(False, 6), f"{MODE}.s1_relay_unhealthy_on_nodes")
time.sleep(0.5)
err = None
try:
    q(c, "INSERT INTO tr_f3_t(v) VALUES ('b')")
    q(c, "COMMIT")
except Exception as e:
    err = e
if MODE == "none":
    check(err is not None, f"{MODE}.s1_error_raised", repr(err)[:120])
    cd = code_of(err)
    check(cd in ("57P01", ""), f"{MODE}.s1_sqlstate_57P01_or_closed", f"pgcode={cd!r} msg={str(err).strip()[:100]}")
    if cd: check(cd == "57P01", f"{MODE}.s1_sqlstate_is_57P01", cd)
    check(closed_after(c), f"{MODE}.s1_connection_closed")
    check(pg_rows("a") == 0 and pg_rows("b") == 0, f"{MODE}.s1_nothing_committed", f"a={pg_rows('a')} b={pg_rows('b')}")
elif MODE in ("session", "select"):
    check(err is not None and code_of(err) == "57P01", f"{MODE}.s1_insert_b_57P01", f"pgcode={code_of(err)!r} {str(err).strip()[:100]}")
    try:
        q(c, "ROLLBACK"); ok(f"{MODE}.s1_rollback_ok")
    except Exception as e:
        bad(f"{MODE}.s1_rollback_ok", repr(e)[:120])
    try:
        check(q(c, "SELECT 1")[0][0] == 1, f"{MODE}.s1_select1_same_connection")
        check(q(c, "SHOW application_name")[0][0] == "tr-f3", f"{MODE}.s1_application_name_restored", str(q(c, "SHOW application_name")))
    except Exception as e:
        bad(f"{MODE}.s1_connection_alive", repr(e)[:120])
    check(pg_rows("a") == 0 and pg_rows("b") == 0, f"{MODE}.s1_tx_rolled_back_nothing_committed", f"a={pg_rows('a')} b={pg_rows('b')}")
    okm, v = wait_metric("tr_failovers_total", lambda v: v == m0["tr_failovers_total"] + 1)
    check(okm, f"{MODE}.s1_tr_failovers_incremented", str(v))
elif MODE == "transaction":
    check(err is None, f"{MODE}.s1_insert_b_and_commit_transparent", repr(err)[:160])
    try:
        rows = [r[0] for r in q(c, "SELECT v FROM tr_f3_t ORDER BY id")]
        check(rows == ["a", "b"], f"{MODE}.s1_rows_a_b", str(rows))
        check(q(c, "SHOW application_name")[0][0] == "tr-f3", f"{MODE}.s1_application_name_restored")
    except Exception as e:
        bad(f"{MODE}.s1_connection_alive", repr(e)[:120])
    okm, v = wait_metric("tr_transactions_replayed_total", lambda v: v >= 1)
    check(okm, f"{MODE}.s1_tr_transactions_replayed_total", str(v))
    okm, v = wait_metric("tr_failovers_total", lambda v: v == m0["tr_failovers_total"] + 1)
    check(okm, f"{MODE}.s1_tr_failovers_incremented", str(v))
try: c.close()
except Exception: pass

# ---------------------------------------------------------------- S2: statement in flight (outcome unknown)
start_relay()
m0 = metrics()
c = connect()
q(c, "SET application_name = 'tr-f3'")
t = threading.Timer(0.1, kill_relay); t.start()
err = None; val = None
try:
    val = q(c, "SELECT pg_sleep(0.5), 42")[0][1]
except Exception as e:
    err = e
t.join()
if MODE in ("select", "transaction"):
    check(err is None and val == 42, f"{MODE}.s2_inflight_read_transparent", f"err={repr(err)[:100]} val={val}")
    okm, v = wait_metric("tr_statements_reexecuted_total", lambda v: v == m0["tr_statements_reexecuted_total"] + 1)
    check(okm, f"{MODE}.s2_reexecuted_metric", str(v))
    okm, v = wait_metric("tr_failovers_total", lambda v: v == m0["tr_failovers_total"] + 1)
    check(okm, f"{MODE}.s2_failover_happened", str(v))
    try:
        check(q(c, "SHOW application_name")[0][0] == "tr-f3", f"{MODE}.s2_application_name_restored")
    except Exception as e:
        bad(f"{MODE}.s2_connection_alive", repr(e)[:120])
elif MODE == "session":
    check(err is not None and code_of(err) == "08007", f"{MODE}.s2_inflight_08007", f"pgcode={code_of(err)!r} {str(err).strip()[:100]}")
    try:
        check(q(c, "SELECT 1")[0][0] == 1, f"{MODE}.s2_connection_alive_after_08007")
        check(q(c, "SHOW application_name")[0][0] == "tr-f3", f"{MODE}.s2_application_name_restored")
    except Exception as e:
        bad(f"{MODE}.s2_connection_alive_after_08007", repr(e)[:120])
    okm, v = wait_metric("tr_unknown_outcome_errors_total", lambda v: v == m0["tr_unknown_outcome_errors_total"] + 1)
    check(okm, f"{MODE}.s2_unknown_outcome_metric", str(v))
else:  # none
    check(err is not None, f"{MODE}.s2_error_raised", repr(err)[:120])
    cd = code_of(err)
    if cd: check(cd == "57P01", f"{MODE}.s2_sqlstate_is_57P01", cd)
    check(closed_after(c), f"{MODE}.s2_connection_closed")
try: c.close()
except Exception: pass

# ---------------------------------------------------------------- S3: idle, not in tx
start_relay()
c = connect()
q(c, "SET application_name = 'tr-f3'")
kill_relay()
check(wait_relay(False, 6), f"{MODE}.s3_relay_unhealthy_on_nodes")
time.sleep(0.5)
try:
    check(q(c, "SELECT 1")[0][0] == 1, f"{MODE}.s3_select1_after_idle_death")
    name = q(c, "SHOW application_name")[0][0]
    if MODE == "none":
        ok(f"{MODE}.s3_plain_redial_no_restore", f"application_name={name!r} (not restored by design)")
    else:
        check(name == "tr-f3", f"{MODE}.s3_application_name_restored", repr(name))
except Exception as e:
    bad(f"{MODE}.s3_connection_survives_idle_death", repr(e)[:140])
try: c.close()
except Exception: pass

# ---------------------------------------------------------------- S4: COMMIT racing the fault (transaction mode)
if MODE == "transaction":
    outcomes = {"08007": 0, "transparent": 0}
    for i in range(3):
        start_relay()
        direct = connect(False); q(direct, "DELETE FROM tr_f3_t WHERE v = 'c'"); direct.close()
        c = connect()
        q(c, "BEGIN")
        q(c, "INSERT INTO tr_f3_t(v) VALUES ('c')")
        kill_relay()
        err = None
        try:
            q(c, "COMMIT")
        except Exception as e:
            err = e
        n = pg_rows("c")
        if err is None:
            outcomes["transparent"] += 1
            check(n == 1, f"{MODE}.s4_{i}_transparent_commit_exactly_once", f"rows c={n}")
        else:
            outcomes["08007"] += 1
            check(code_of(err) == "08007", f"{MODE}.s4_{i}_commit_unknown_outcome_08007", f"pgcode={code_of(err)!r} {str(err).strip()[:100]}")
            check(n == 0, f"{MODE}.s4_{i}_no_double_apply", f"rows c={n}")
            try:
                q(c, "ROLLBACK"); check(q(c, "SELECT 1")[0][0] == 1, f"{MODE}.s4_{i}_connection_usable_after_08007")
            except Exception as e:
                bad(f"{MODE}.s4_{i}_connection_usable_after_08007", repr(e)[:120])
        check(n <= 1, f"{MODE}.s4_{i}_never_two_rows", f"rows c={n}")
        try: c.close()
        except Exception: pass
    ok(f"{MODE}.s4_outcomes", str(outcomes))

    # ------------------------------------------------------------ S5: COMMIT itself in flight (outcome unknown)
    # A deferred constraint trigger makes the COMMIT take ~0.6 s on the server;
    # the relay dies 0.15 s into it. The COMMIT was delivered, so its outcome is
    # unknown: the proxy must return 08007, must NOT replay+re-commit (the
    # server may well have completed the commit), and must keep the session.
    start_relay()
    direct = connect(False)
    q(direct, "DELETE FROM tr_f3_t WHERE v = 'd'")
    q(direct, "CREATE OR REPLACE FUNCTION tr_f3_slow_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.6); RETURN NULL; END $$")
    q(direct, "DROP TRIGGER IF EXISTS tr_f3_slow ON tr_f3_t")
    q(direct, "CREATE CONSTRAINT TRIGGER tr_f3_slow AFTER INSERT ON tr_f3_t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tr_f3_slow_commit()")
    direct.close()
    m0 = metrics()
    c = connect()
    q(c, "BEGIN")
    q(c, "INSERT INTO tr_f3_t(v) VALUES ('d')")
    t = threading.Timer(0.15, kill_relay); t.start()
    err = None
    try:
        q(c, "COMMIT")
    except Exception as e:
        err = e
    t.join()
    n = pg_rows("d")
    check(err is not None and code_of(err) == "08007", f"{MODE}.s5_commit_inflight_08007", f"pgcode={code_of(err)!r} {str(err).strip()[:110]}")
    check(n <= 1, f"{MODE}.s5_never_double_applied", f"rows d={n} (0 or 1 are both legal: outcome unknown)")
    okm, v = wait_metric("tr_transactions_replayed_total", lambda v: v == m0["tr_transactions_replayed_total"])
    check(okm, f"{MODE}.s5_no_replay_for_unknown_commit", str(v))
    okm, v = wait_metric("tr_unknown_outcome_errors_total", lambda v: v == m0["tr_unknown_outcome_errors_total"] + 1)
    check(okm, f"{MODE}.s5_unknown_outcome_metric", str(v))
    try:
        q(c, "ROLLBACK"); check(q(c, "SELECT 1")[0][0] == 1, f"{MODE}.s5_connection_usable_after_08007")
    except Exception as e:
        bad(f"{MODE}.s5_connection_usable_after_08007", repr(e)[:120])
    try: c.close()
    except Exception: pass
    direct = connect(False)
    q(direct, "DROP TRIGGER IF EXISTS tr_f3_slow ON tr_f3_t"); q(direct, "DROP FUNCTION IF EXISTS tr_f3_slow_commit()")
    direct.close()

kill_relay()
PYEOF

echo "== tr-failover live test  bin=$BIN  modes=[$MODES] =="
for MODE in $MODES; do
  echo "-- tr_mode = $MODE"
  write_cfg "$MODE"
  python3 "$HERE/tcp-relay.py" "$RELAY_PORT" "$PGHOST" "$PGPORT" >"$OUT/relay-$MODE.log" 2>&1 &
  RELAYPID=$!
  sleep 0.3
  RUST_LOG="heliosdb_proxy=info" NO_COLOR=1 "$BIN" --config "$OUT/proxy-$MODE.toml" >"$OUT/proxy-$MODE.log" 2>&1 &
  PROXYPID=$!
  ready=0
  for _ in $(seq 1 40); do
    if curl -sf "http://127.0.0.1:$ADMIN_PORT/health" >/dev/null 2>&1; then ready=1; break; fi
    if ! kill -0 "$PROXYPID" 2>/dev/null; then echo "proxy died on startup:"; tail -20 "$OUT/proxy-$MODE.log"; exit 1; fi
    sleep 0.25
  done
  [ "$ready" = 1 ] || { bad "$MODE.proxy_ready"; tail -20 "$OUT/proxy-$MODE.log"; kill "$PROXYPID" "$RELAYPID" 2>/dev/null; continue; }
  ok "$MODE.proxy_ready"

  # The client manages relay kills/respawns; pass the initial relay pid.
  python3 "$OUT/client.py" "$MODE" "$PROXY_PORT" "$ADMIN_PORT" "$RELAY_PORT" "$RELAYPID" "$HERE/tcp-relay.py" \
      "$PGHOST" "$PGPORT" "$BUSER" "$BPASS" "$BDB" 2>&1 | tee "$OUT/client-$MODE.log" | while IFS= read -r line; do
    case "$line" in
      OK\ *)  ok "${line#OK }" ;;
      BAD\ *) bad "${line#BAD }" ;;
      *)      echo "    $line" ;;
    esac
  done
  # tee|while runs in a subshell: recount from the log.
  PASS=$((PASS + $(grep -c '^OK ' "$OUT/client-$MODE.log")))
  FAIL=$((FAIL + $(grep -c '^BAD ' "$OUT/client-$MODE.log")))
  if grep -q '^Traceback' "$OUT/client-$MODE.log"; then FAIL=$((FAIL+1)); echo "  client crashed (see $OUT/client-$MODE.log)"; fi

  kill "$PROXYPID" 2>/dev/null; wait "$PROXYPID" 2>/dev/null; PROXYPID=""
  kill "$RELAYPID" 2>/dev/null; wait "$RELAYPID" 2>/dev/null; RELAYPID=""
  # Any relay the client respawned exits with it (it kills the last one).
  sleep 0.3
done

echo "== tr-failover: PASS=$PASS FAIL=$FAIL  (logs: $OUT) =="
[ "$FAIL" -eq 0 ]
