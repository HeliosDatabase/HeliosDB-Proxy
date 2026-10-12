#!/usr/bin/env python3
"""Drive a Patroni switchover/failover through HeliosProxy and report what the client saw.

Patroni (not the proxy) promotes and fences. This script only: opens ONE client session
through the proxy, starts a transaction, asks Patroni to change leader, continues the same
transaction, and then checks the databases directly. Exits non-zero if the session or data
check fails. Requires psycopg2 and a running demo cluster + proxy (see run-demo.sh).
"""
import json, sys, time, urllib.request
import psycopg2

PROXY = dict(host="127.0.0.1", port=35600, user="postgres", dbname="postgres", connect_timeout=10)
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


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "switchover")
