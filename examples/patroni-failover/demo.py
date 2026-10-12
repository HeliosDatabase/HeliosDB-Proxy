#!/usr/bin/env python3
"""Drive a Patroni switchover/failover through HeliosProxy and report what the client saw.

Patroni (not the proxy) promotes and fences. This script only: opens ONE client session
through the proxy, starts a transaction, asks Patroni to change leader, continues the same
transaction, and then checks the databases directly. Exits non-zero if the session or data
check fails. Requires psycopg2 and a running demo cluster + proxy (see run-demo.sh).
"""
import json, os, signal, subprocess, sys, threading, time, urllib.request
import psycopg2

PROXY = dict(host="127.0.0.1", port=35600, user="postgres", dbname="postgres", connect_timeout=10)
ADMIN = "http://127.0.0.1:35690"
NODES = {"pg-a": (35601, 35611), "pg-b": (35602, 35612)}


def cluster():
    for _, (_, rest) in NODES.items():
        try:
            return json.load(urllib.request.urlopen(f"http://127.0.0.1:{rest}/cluster", timeout=3))["members"]
        except Exception:
            continue
    return []


def leader():
    for m in cluster():
        if m["role"] == "leader" and m["state"] == "running":
            return m["name"]


def wait_leader(not_name, timeout=90):
    end = time.time() + timeout
    while time.time() < end:
        n = leader()
        if n and n != not_name:
            return n
        time.sleep(1)
    raise SystemExit(f"no new leader within {timeout}s")


def direct(name, sql):
    c = psycopg2.connect(host="127.0.0.1", port=NODES[name][0], user="postgres", dbname="postgres", connect_timeout=5)
    c.autocommit = True
    cur = c.cursor(); cur.execute(sql)
    rows = cur.fetchall(); c.close(); return rows


def main(mode):
    old = leader()
    print(f"[patroni] leader before: {old}")
    setup = psycopg2.connect(**PROXY); setup.autocommit = True
    cur = setup.cursor()
    cur.execute("DROP TABLE IF EXISTS hp_demo"); cur.execute("CREATE TABLE hp_demo(id int primary key, note text)")
    setup.close()

    conn = psycopg2.connect(**PROXY, application_name="hp-demo-session")
    conn.autocommit = True  # probes outside the transaction: they differ per backend, so they must not be replayed
    cur = conn.cursor()
    cur.execute("SET search_path = public")
    cur.execute("SELECT inet_server_port(), pg_is_in_recovery()")
    before = cur.fetchone()
    conn.autocommit = False
    print(f"[client] session on backend port {before[0]} (in_recovery={before[1]})")
    cur.execute("INSERT INTO hp_demo VALUES (1, 'before leader change')")
    print("[client] transaction open, row 1 written, NOT committed")

    if mode == "switchover":
        cand = next(n for n in NODES if n != old)
        req = urllib.request.Request(f"http://127.0.0.1:{NODES[old][1]}/switchover", method="POST",
                                     data=json.dumps({"leader": old, "candidate": cand}).encode(),
                                     headers={"Content-Type": "application/json"})
        print("[patroni]", urllib.request.urlopen(req, timeout=10).read().decode().strip())
    else:
        import subprocess
        subprocess.run(["docker", "stop", "-t", "0", f"hp-patroni-demo-{old}"], check=True, capture_output=True)
        print(f"[patroni] stopped leader container hp-patroni-demo-{old}")
    new = wait_leader(old)
    print(f"[patroni] leader after: {new}")

    outcome = {"mode": mode, "old_leader": old, "new_leader": new, "port_before": before[0]}
    try:
        cur.execute("INSERT INTO hp_demo VALUES (2, 'after leader change')")
        conn.commit()
        outcome["client_result"] = "continued: row 2 written and COMMIT succeeded on the same client connection"
    except Exception as e:  # report exactly what the client saw
        outcome["client_result"] = f"client error: {type(e).__name__}: {str(e).strip()}"
        try: conn.rollback()
        except Exception: pass
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT inet_server_port(), pg_is_in_recovery()")
        after = cur.fetchone()
        outcome["port_after"], outcome["in_recovery_after"] = after
    except Exception as e:
        outcome["after_error"] = f"{type(e).__name__}: {str(e).strip()}"
    # Ground truth straight from the new leader, bypassing the proxy.
    outcome["rows_on_new_leader"] = direct(new, "SELECT id, note FROM hp_demo ORDER BY id")
    outcome["new_leader_in_recovery"] = direct(new, "SELECT pg_is_in_recovery()")[0][0]
    ok = (outcome["new_leader_in_recovery"] is False and outcome["new_leader"] != old
          and outcome.get("client_result", "").startswith("continued")
          and [r[0] for r in outcome["rows_on_new_leader"]] == [1, 2])
    outcome["pass"] = ok
    print(json.dumps(outcome, indent=2, default=str))
    sys.exit(0 if ok else 1)


def routing_view(lead, timeout=60):
    """Wait until the proxy's published routing snapshot (GET /topology -> routing) names
    Patroni's leader as the write target and the other member, once it is back and healthy,
    as the only read replica. Returns what was last seen and how long it took."""
    want_leader = f"127.0.0.1:{NODES[lead][0]}"
    want_replicas = [f"127.0.0.1:{port}" for name, (port, _) in NODES.items() if name != lead]
    start = time.time()
    seen = {}
    while time.time() - start < timeout:
        routing = json.load(urllib.request.urlopen(f"{ADMIN}/topology", timeout=5)).get("routing") or {}
        seen = {"leader": routing.get("leader"), "replicas": [r["address"] for r in routing.get("replicas", [])],
                "epoch": routing.get("epoch"), "leader_changes_total": routing.get("leaderChangesTotal")}
        if seen["leader"] == want_leader and seen["replicas"] == want_replicas:
            return True, seen, round(time.time() - start, 1)
        time.sleep(1)
    return False, seen, round(time.time() - start, 1)


def new_session():
    """A brand-new client session opened AFTER a leader change must land on the current leader."""
    lead = leader()
    conn = psycopg2.connect(**PROXY, application_name="hp-demo-new-session")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT inet_server_port(), pg_is_in_recovery()")
    port, in_rec = cur.fetchone()
    outcome = {"mode": "new-session", "leader": lead, "backend_port": port, "in_recovery": in_rec}
    try:
        cur.execute("INSERT INTO hp_demo VALUES (3, 'new session after leader change')")
        outcome["write"] = "ok"
    except Exception as e:
        outcome["write"] = f"{type(e).__name__}: {str(e).strip()}"
    outcome["rows_on_leader"] = [r[0] for r in direct(lead, "SELECT id FROM hp_demo ORDER BY id")]
    # The routing snapshot must have followed Patroni: the demoted former leader is the
    # read replica now, whatever role proxy.toml gives it.
    routing_ok, outcome["routing"], outcome["routing_settled_s"] = routing_view(lead)
    outcome["pass"] = (port == NODES[lead][0] and in_rec is False and outcome["write"] == "ok"
                       and 3 in outcome["rows_on_leader"] and routing_ok)
    print(json.dumps(outcome, indent=2, default=str))
    sys.exit(0 if outcome["pass"] else 1)


# --- H-02 drills: run with `run-demo.sh <binary> h02` (needs both members) ---------------

ADMIN = "http://127.0.0.1:35690"
LEASE_SECS = 8  # proxy.toml lease_timeout_secs


def write_loop(stop, out, label):
    """Fresh session per write so each write is routed anew; record what the client saw."""
    i = 0
    while not stop.is_set():
        i += 1
        rid, t0 = f"{label}-{i}", time.time()
        try:
            c = psycopg2.connect(**PROXY, application_name="hp-demo-writer")
            c.autocommit = True
            c.cursor().execute("INSERT INTO hp_lock VALUES (%s, now())", (rid,))
            c.close()
            out.append({"id": rid, "outcome": "committed", "t0": t0, "t1": time.time()})
        except Exception as e:
            code = getattr(e, "pgcode", None)
            # 08007 or a dropped connection: the commit may or may not have happened.
            outcome = "unknown" if code in (None, "08007") else "error"
            out.append({"id": rid, "outcome": outcome, "code": code, "t0": t0, "t1": time.time(),
                        "detail": str(e).strip()[:120]})
        time.sleep(0.25)


def sample_topology(stop, out):
    while not stop.is_set():
        try:
            topo = json.load(urllib.request.urlopen(f"{ADMIN}/topology", timeout=2))
            out.append({"t": time.time(), "leader": (topo.get("routing") or {}).get("leader"),
                        "valid": (topo.get("authoritative") or {}).get("valid")})
        except Exception as e:
            out.append({"t": time.time(), "error": str(e)[:80]})
        time.sleep(0.5)


def ids_on(name):
    for _ in range(30):
        try:
            return {r[0] for r in direct(name, "SELECT id FROM hp_lock")}
        except Exception:
            time.sleep(2)
    return None


def with_load(drill):
    """Run `drill()` while 3 writers and a /topology sampler run; return their records."""
    setup = psycopg2.connect(**PROXY)
    setup.autocommit = True
    setup.cursor().execute("DROP TABLE IF EXISTS hp_lock")
    setup.cursor().execute("CREATE TABLE hp_lock(id text PRIMARY KEY, at timestamptz)")
    setup.close()
    stop, writes, topo = threading.Event(), [], []
    threads = [threading.Thread(target=write_loop, args=(stop, writes, f"w{k}")) for k in range(3)]
    threads.append(threading.Thread(target=sample_topology, args=(stop, topo)))
    for th in threads:
        th.start()
    try:
        marks = drill()
    finally:
        stop.set()
        for th in threads:
            th.join(timeout=90)
    return marks, writes, topo


def lock_drill():
    """Freeze the leader (docker pause); Patroni moves the lock; the old leader returns.
    The proxy must never acknowledge a write that is missing on the lock holder, never
    route to the frozen or returned former leader after the move, and resume writes."""
    old = leader()
    def drill():
        time.sleep(5)
        t_pause = time.time()
        subprocess.run(["docker", "pause", f"hp-patroni-demo-{old}"], check=True, capture_output=True)
        new = wait_leader(old, timeout=120)
        t_new = time.time()
        time.sleep(10)
        subprocess.run(["docker", "unpause", f"hp-patroni-demo-{old}"], check=True, capture_output=True)
        t_unpause = time.time()
        time.sleep(20)
        return {"old": old, "new": new, "t_pause": t_pause, "t_new": t_new, "t_unpause": t_unpause}
    marks, writes, topo = with_load(drill)
    new, old_addr = marks["new"], f"127.0.0.1:{NODES[old][0]}"
    on_new = ids_on(new) or set()
    on_old = ids_on(old)  # the returned member, after Patroni rejoined it
    committed = [w for w in writes if w["outcome"] == "committed"]
    routed_old_after_move = [s for s in topo if s.get("t", 0) > marks["t_new"] + LEASE_SECS
                             and s.get("leader") == old_addr]
    outcome = {
        "mode": "lock-drill", **{k: marks[k] for k in ("old", "new")},
        "seconds_to_new_leader": round(marks["t_new"] - marks["t_pause"], 1),
        "writes": {o: sum(w["outcome"] == o for w in writes) for o in ("committed", "error", "unknown")},
        "committed_after_move": sum(w["t0"] > marks["t_new"] for w in committed),
        "acknowledged_missing_on_new_leader": [w["id"] for w in committed if w["id"] not in on_new],
        "acknowledged_only_on_old_leader": sorted(((on_old or set()) - on_new)
                                                  & {w["id"] for w in committed}),
        "old_leader_readable_after_return": on_old is not None,
        "proxy_named_old_leader_after_move": len(routed_old_after_move),
        "errors_seen": sorted({w.get("code") or "conn" for w in writes if w["outcome"] != "committed"}),
    }
    outcome["pass"] = (not outcome["acknowledged_missing_on_new_leader"]
                       and not outcome["acknowledged_only_on_old_leader"]
                       and outcome["proxy_named_old_leader_after_move"] == 0
                       and outcome["committed_after_move"] > 0)
    print(json.dumps(outcome, indent=2, default=str))
    sys.exit(0 if outcome["pass"] else 1)


def control_partition(relay_pids):
    """Freeze the TCP relays that carry the proxy's GET /cluster polls (the data path and
    Patroni itself are untouched). Once the lease runs out the proxy must stop writing (no
    write that started after expiry completes before the heal) and resume after it."""
    pids = [int(p) for p in relay_pids]
    def drill():
        time.sleep(5)
        t_cut = time.time()
        for p in pids:
            os.kill(p, signal.SIGSTOP)
        time.sleep(25)
        t_heal = time.time()
        for p in pids:
            os.kill(p, signal.SIGCONT)
        time.sleep(20)
        return {"t_cut": t_cut, "t_heal": t_heal}
    marks, writes, topo = with_load(drill)
    lead = leader()
    on_leader = ids_on(lead) or set()
    expiry = marks["t_cut"] + LEASE_SECS + 2  # lease + one poll of slack
    committed = [w for w in writes if w["outcome"] == "committed"]
    completed_while_cut = [w["id"] for w in committed
                           if w["t0"] > expiry and w["t1"] < marks["t_heal"]]
    no_leader_samples = [s for s in topo if expiry < s.get("t", 0) < marks["t_heal"]
                         and s.get("leader") is None]
    outcome = {
        "mode": "control-partition", "leader": lead,
        "partition_seconds": round(marks["t_heal"] - marks["t_cut"], 1),
        "writes": {o: sum(w["outcome"] == o for w in writes) for o in ("committed", "error", "unknown")},
        "completed_after_lease_expiry_while_cut": completed_while_cut,
        "topology_samples_without_write_target_while_cut": len(no_leader_samples),
        "committed_after_heal": sum(w["t1"] > marks["t_heal"] for w in committed),
        "acknowledged_missing_on_leader": [w["id"] for w in committed if w["id"] not in on_leader],
    }
    outcome["pass"] = (not completed_while_cut and bool(no_leader_samples)
                       and outcome["committed_after_heal"] > 0
                       and not outcome["acknowledged_missing_on_leader"])
    print(json.dumps(outcome, indent=2, default=str))
    sys.exit(0 if outcome["pass"] else 1)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "switchover"
    if mode == "new-session":
        new_session()
    elif mode == "lock-drill":
        lock_drill()
    elif mode == "control-partition":
        control_partition(sys.argv[2:])
    else:
        main(mode)
