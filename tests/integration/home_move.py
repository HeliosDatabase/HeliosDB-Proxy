#!/usr/bin/env python3
"""A session whose home stops being the write leader while its connection stays
open (Sprinter 480d8084f833): tr_mode = "session", [topology] provider = "patroni".

Starts and removes its own bounded PostgreSQL Docker container (host network,
listening on 127.0.0.1 and 127.0.0.2), a fake Patroni REST API (GET /cluster) in
this process, and its own proxies; uses no existing database and no third-party
Python packages. Each proxy sees the one server as two nodes, 127.0.0.1 and
127.0.0.2, so `host(inet_server_addr())` tells which node ran a statement. Each
case starts with 127.0.0.1 as the Patroni leader, then names 127.0.0.2 the leader
while the session's connection to 127.0.0.1 stays open (a switchover that left
the old primary running), and sends a write.
Usage: python3 tests/integration/home_move.py PROXY [--output evidence.json]
Requires the already available postgres:18.4-bookworm image (never pulls images)
and a proxy built with the postgres-topology feature.

Cases:
  settings   SET search_path, then the move: the write runs on the new leader and
             the session keeps its search_path (the proxy re-homes it and restores
             its settings instead of moving it to a bare connection).
  unknown    set_config(..., false) (a setting the proxy cannot record), then the
             move: the write is refused with 08006 rather than run without it.
  in_tx      BEGIN and an INSERT, then the move: the next INSERT reports the
             transaction lost (57P01) and neither row is committed.
  mirrored   read offload on: a SET, a read offloaded to 127.0.0.2 (which mirrors
             the SET onto that connection), then set_config(..., false); the leader
             moves to 127.0.0.2. The mirrored connection must not become the home
             with the tracked subset only: the write is refused with 08006.
Exit 1 if any case fails its expectation.
"""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, HERE / file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tr08 = load("tr08", "tr_prepared_replay.py")
f2 = load("f2", "prepared_close_routing.py")
topo = load("topo", "topology_failover.py")
wire = tr08.wire
OLD, NEW = f2.PRIMARY, f2.STANDBY
PROBE = "SELECT host(inet_server_addr()), current_setting('search_path')"


def query(client, sql):
    """(rows, SQLSTATEs) of one simple query; SQLSTATEs ['closed'] when the
    proxy closed the connection instead of answering."""
    try:
        r = tr08.send(client, wire.frame(b"Q", sql.encode() + b"\0"))
    except (OSError, EOFError):
        return [], ["closed"]
    return tr08.rows(r), tr08.errors(r)


def probe(client):
    rows, errs = query(client, PROBE)
    return rows[0] if rows else errs


def case_settings(client, move):
    r = {"set": query(client, "SET search_path TO hm_schema, public")[1]}
    r["before"] = probe(client)
    move()
    r["write"] = query(client, "INSERT INTO hm_t VALUES (1)")[1]
    r["after"] = probe(client)
    r["ok"] = (r["before"] == [OLD, "hm_schema, public"] and r["write"] == []
               and r["after"] == [NEW, "hm_schema, public"])
    return r


def case_unknown(client, move):
    r = {"set_config": query(client, "SELECT set_config('search_path', 'hm_schema', false)")[1]}
    r["before"] = probe(client)
    move()
    r["write"] = query(client, "INSERT INTO hm_t VALUES (2)")[1]
    r["ok"] = (r["before"] == [OLD, "hm_schema"]
               and r["write"] in (["08006"], ["closed"]))
    return r


def case_in_tx(client, move):
    r = {"open": [query(client, "BEGIN")[1], query(client, "INSERT INTO hm_t VALUES (10)")[1]]}
    move()
    r["write"] = query(client, "INSERT INTO hm_t VALUES (11)")[1]
    time.sleep(0.5)  # the dropped connection's transaction rolls back
    r["committed"] = tr08.pg_query("SELECT count(*) FROM hm_t WHERE a >= 10")
    r["ok"] = (r["open"] == [[], []] and r["write"] == ["57P01"]
               and r["committed"] == [["0"]])
    return r


def case_mirrored(client, move):
    r = {"set": query(client, "SET search_path TO hm_schema, public")[1]}
    r["offloaded"] = probe(client)
    r["set_config"] = query(client, "SELECT set_config('application_name', 'hm-cfg', false)")[1]
    move()
    r["write"] = query(client, "INSERT INTO hm_t VALUES (3)")[1]
    r["ok"] = (r["offloaded"] == [NEW, "hm_schema, public"] and r["set_config"] == []
               and r["write"] in (["08006"], ["closed"]))
    return r


SPLIT_OFF = "read_write_split = false\n"
OFFLOAD = "read_your_writes_ms = 0\n"


def start_proxy(binary, temp, pg_port, authority, load_balancer):
    port, admin = wire.free_port(), wire.free_port()
    cfg = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = true
tr_mode = "session"
tr_read_functions = ["host", "inet_server_addr", "current_setting"]
write_timeout_secs = 15
[pool_mode]
mode = "session"
[load_balancer]
{load_balancer}[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
[topology]
provider = "patroni"
poll_interval_secs = 1
lease_timeout_secs = 5
patroni_endpoints = ["http://127.0.0.1:{authority.server.server_port}"]
patroni_confirm_leader_lock = false
[[nodes]]
host = "{OLD}"
port = {pg_port}
role = "primary"
[[nodes]]
host = "{NEW}"
port = {pg_port}
role = "standby"
'''
    (temp / "proxy.toml").write_text(cfg)
    log = (temp / "proxy.log").open("w+")
    proc = subprocess.Popen([binary, "--config", str(temp / "proxy.toml")], stdout=log, stderr=log)
    topo.wait_topology(admin, f"{OLD}:{pg_port}", proc, log)
    deadline = time.monotonic() + 15
    while True:
        try:
            return proc, log, admin, tr08.startup(("127.0.0.1", port))
        except (OSError, RuntimeError, EOFError):
            if proc.poll() is not None or time.monotonic() >= deadline:
                log.seek(0)
                raise RuntimeError("proxy did not start: " + log.read()[-3000:])
            time.sleep(0.1)


def lead(authority, leader, replica, port):
    authority.members = [dict(host=leader, port=port, role="leader", state="running"),
                         dict(host=replica, port=port, role="replica", state="running")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    name = "proxy-home-" + uuid.uuid4().hex[:12]
    tr08.PG_ADDRESS = (OLD, wire.free_port())
    f2.tr08.PG_ADDRESS = tr08.PG_ADDRESS  # f2 loaded its own copy of the helpers
    pg_port = tr08.PG_ADDRESS[1]
    authority = topo.Authority()
    reports = []
    try:
        version = f2.start_postgres(name, pg_port)
        tr08.pg_query("CREATE SCHEMA hm_schema")
        tr08.pg_query("CREATE TABLE hm_t (a int)")
        for case, lb, run in [("settings", SPLIT_OFF, case_settings),
                              ("unknown", SPLIT_OFF, case_unknown),
                              ("in_tx", SPLIT_OFF, case_in_tx),
                              ("mirrored", OFFLOAD, case_mirrored)]:
            lead(authority, OLD, NEW, pg_port)
            report = {"case": case}
            with tempfile.TemporaryDirectory(prefix="proxy-home-") as temp:
                temp = pathlib.Path(temp)
                proc, log, admin, client = start_proxy(args.binary, temp, pg_port, authority, lb)
                time.sleep(2.5)  # both nodes pass their health check

                def move():
                    lead(authority, NEW, OLD, pg_port)
                    topo.wait_topology(admin, f"{NEW}:{pg_port}", proc, log)

                try:
                    report.update(run(client, move))
                except Exception as e:  # report and continue with the next case
                    report.update({"ok": False, "exception": repr(e)})
                finally:
                    client.close()
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
                    log.seek(0)
                    report["proxy_log_tail"] = [line for line in log.read().splitlines()
                                                if "helios::tr" in line][-4:]
                    log.close()
            report["invariant"] = "PASS" if report.pop("ok") else "FAIL"
            reports.append(report)
            print(json.dumps(report), flush=True)
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "one disposable PostgreSQL as 127.0.0.1 + 127.0.0.2 behind a "
                               "fake Patroni /cluster",
                    "postgres_version": version, "binary": args.binary,
                    "sha256": digest.hexdigest(), "cases": reports, "failures": failures}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return int(failures > 0)
    finally:
        authority.close()
        # The unique name belongs to this run, including a partially started container.
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=20)


if __name__ == "__main__":
    raise SystemExit(main())
