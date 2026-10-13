#!/usr/bin/env python3
"""F2: a Close reaches only the backend connection its batch is routed to, so
the proxy must keep the session's other cached connections from running, or
tripping over, a statement the client closed.

Starts and removes its own bounded PostgreSQL Docker container (host network,
listening on 127.0.0.1 and 127.0.0.2) and its own proxy; uses no existing
database and no third-party Python packages. The proxy sees the one server as
two nodes: 127.0.0.1 is the primary (pg-primary) and 127.0.0.2 the standby
(pg-standby). A session starts on the primary and a read stays on the node the
session is on, so the reads below name the standby with a routing hint
(`/*helios:node=pg-standby*/`, `[routing_hints] enabled = true`); a write
moves the session back to the primary. The session therefore holds one backend
connection per address, each with its own prepared statements, and
`host(inet_server_addr())` reports which of them ran a statement.
Usage: python3 tests/integration/prepared_close_routing.py PROXY [--output evidence.json]
Requires the already available postgres:18.4-bookworm image (never pulls images).

Cases (each on a new client connection; <SELECT> carries the standby hint):
  replaced     Parse a1 (an UPDATE) on the primary; `Close a1; Parse a1 <SELECT>`
               runs on the standby; after a write, Bind/Execute a1 stays on the
               primary. Expected: the SELECT runs there, the UPDATE never does.
  closed       Parse b1 (an UPDATE) on the primary; after a hinted read moves
               the session to the standby, `Close b1` runs there; after a write, Bind/Execute b1 on the primary.
               Expected: 26000 (the client closed b1), the UPDATE never runs.
  reparsed     Parse c1 on the primary; `Close c1` runs on the standby; a new
               `Parse c1 <INSERT>` goes to the primary. Expected: no 42P05.
  replacement  `Close d1; Parse d1 <SELECT>` on the standby (d1 never existed);
               after a write, Bind/Execute d1 on the primary must re-prepare it
               from the session's record. Expected: the SELECT runs, no 26000.
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
spec = importlib.util.spec_from_file_location("tr08", HERE / "tr_prepared_replay.py")
tr08 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tr08)
wire = tr08.wire
frame = tr08.frame
SYNC = tr08.SYNC
PRIMARY, STANDBY = "127.0.0.1", "127.0.0.2"
# Runs on the standby; once prepared, a Bind of it reports wherever it runs.
WHERE = "/*helios:node=pg-standby*/ SELECT host(inet_server_addr())"


def query(client, sql):
    return tr08.send(client, frame(b"Q", sql.encode() + b"\0"))


def tags(response):
    return [body.rstrip(b"\0").decode() for tag, body in response if tag == b"C"]


def value():
    return tr08.pg_query("SELECT v FROM f2t WHERE id = 1")[0][0]


def write(client):
    """A write: the session moves to the primary connection."""
    r = query(client, "UPDATE f2t SET v = v WHERE id = 2")
    assert not tr08.errors(r), r


def run_bind(client, name, report):
    r = tr08.send(client, tr08.bind("", name) + tr08.execute("") + SYNC)
    report["bind_rows"] = tr08.rows(r)
    report["bind_tags"] = tags(r)
    report["bind_sqlstates"] = tr08.errors(r)


def case_replaced(client):
    report = {"v_before": value()}
    r = tr08.send(client, tr08.parse("a1", "UPDATE f2t SET v = v + 1 WHERE id = 1") + SYNC)
    assert not tr08.errors(r), r
    r = tr08.send(client, tr08.close_statement("a1") + tr08.parse("a1", WHERE)
                  + tr08.bind("", "a1") + tr08.execute("") + SYNC)
    report["replaced_on"] = tr08.rows(r)
    write(client)
    run_bind(client, "a1", report)
    report["v_after"] = value()
    report["ok"] = (report["replaced_on"] == [[STANDBY]]
                    and report["bind_rows"] == [[PRIMARY]]
                    and report["bind_sqlstates"] == []
                    and report["v_after"] == report["v_before"])
    return report


def close_on_standby(client, name, report):
    r = query(client, WHERE)
    report["read_on"] = tr08.rows(r)
    r = tr08.send(client, tr08.close_statement(name) + SYNC)
    assert not tr08.errors(r), r


def case_closed(client):
    report = {"v_before": value()}
    r = tr08.send(client, tr08.parse("b1", "UPDATE f2t SET v = v + 100 WHERE id = 1") + SYNC)
    assert not tr08.errors(r), r
    close_on_standby(client, "b1", report)
    write(client)
    run_bind(client, "b1", report)
    report["v_after"] = value()
    report["ok"] = (report["read_on"] == [[STANDBY]]
                    and report["bind_sqlstates"] == ["26000"]
                    and report["v_after"] == report["v_before"])
    return report


def case_reparsed(client):
    report = {}
    r = tr08.send(client, tr08.parse("c1", "UPDATE f2t SET v = v WHERE id = 3") + SYNC)
    assert not tr08.errors(r), r
    close_on_standby(client, "c1", report)
    r = tr08.send(client, tr08.parse("c1", "INSERT INTO f2t VALUES (3, 3) ON CONFLICT (id) DO NOTHING")
                  + tr08.bind("", "c1") + tr08.execute("") + SYNC)
    report["reparse_tags"] = tags(r)
    report["reparse_sqlstates"] = tr08.errors(r)
    report["ok"] = (report["read_on"] == [[STANDBY]]
                    and report["reparse_sqlstates"] == []
                    and report["reparse_tags"] == ["INSERT 0 1"])
    return report


def case_replacement(client):
    report = {}
    r = tr08.send(client, tr08.close_statement("d1") + tr08.parse("d1", WHERE)
                  + tr08.bind("", "d1") + tr08.execute("") + SYNC)
    report["defined_on"] = tr08.rows(r)
    write(client)
    run_bind(client, "d1", report)
    report["ok"] = (report["defined_on"] == [[STANDBY]]
                    and report["bind_rows"] == [[PRIMARY]]
                    and report["bind_sqlstates"] == [])
    return report


def start_postgres(name, port):
    subprocess.run(["docker", "run", "--detach", "--rm", "--pull=never", "--name", name,
                    "--memory=768m", "--memory-swap=768m", "--cpus=1", "--pids-limit=64",
                    "--network", "host", "--tmpfs", "/var/lib/postgresql:rw,size=256m",
                    "--env", "PGDATA=/var/lib/postgresql/audit",
                    "--env", f"PGPORT={port}",
                    "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--env", "POSTGRES_USER=audit",
                    "--env", "POSTGRES_DB=audit", "postgres:18.4-bookworm",
                    "-c", f"listen_addresses={PRIMARY},{STANDBY}", "-p", str(port)],
                   check=True, capture_output=True, timeout=30)
    deadline = time.monotonic() + 30
    while True:
        try:
            return tr08.pg_query("SHOW server_version")[0][0]
        except (OSError, EOFError, RuntimeError):
            if time.monotonic() >= deadline:
                raise TimeoutError("isolated PostgreSQL did not become ready")
            time.sleep(0.2)


def start_proxy(binary, temp, pg_port):
    port, admin = wire.free_port(), wire.free_port()
    cfg = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 15
[pool_mode]
mode = "session"
[load_balancer]
read_write_split = true
[routing_hints]
enabled = true
[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
[[nodes]]
host = "{PRIMARY}"
port = {pg_port}
role = "primary"
name = "pg-primary"
[[nodes]]
host = "{STANDBY}"
port = {pg_port}
role = "standby"
name = "pg-standby"
'''
    (temp / "proxy.toml").write_text(cfg)
    log = (temp / "proxy.log").open("w+")
    proc = subprocess.Popen([binary, "--config", str(temp / "proxy.toml")], stdout=log, stderr=log)
    address = ("127.0.0.1", port)
    deadline = time.monotonic() + 20
    while True:
        try:
            # Ready once a read reaches the standby (its health check passed).
            with tr08.startup(address) as probe:
                if tr08.rows(query(probe, WHERE)) == [[STANDBY]]:
                    return proc, log, address
        except (OSError, RuntimeError, EOFError):
            pass
        if proc.poll() is not None or time.monotonic() >= deadline:
            log.seek(0)
            raise RuntimeError("proxy did not start routing reads: " + log.read()[-3000:])
        time.sleep(0.2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    name = "proxy-f2-" + uuid.uuid4().hex[:12]
    tr08.PG_ADDRESS = (PRIMARY, wire.free_port())
    reports = []
    try:
        version = start_postgres(name, tr08.PG_ADDRESS[1])
        tr08.pg_query("CREATE TABLE f2t (id int PRIMARY KEY, v int)")
        tr08.pg_query("INSERT INTO f2t VALUES (1, 0), (2, 0)")
        with tempfile.TemporaryDirectory(prefix="proxy-f2-") as temp:
            temp = pathlib.Path(temp)
            proc, log, address = start_proxy(args.binary, temp, tr08.PG_ADDRESS[1])
            try:
                for case, run in [("replaced", case_replaced), ("closed", case_closed),
                                  ("reparsed", case_reparsed), ("replacement", case_replacement)]:
                    report = {"case": case}
                    try:
                        with tr08.startup(address) as client:
                            report.update(run(client))
                    except Exception as e:  # report and continue with the next case
                        report.update({"ok": False, "exception": repr(e)})
                    report["invariant"] = "PASS" if report.pop("ok") else "FAIL"
                    reports.append(report)
                    print(json.dumps(report), flush=True)
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                log.close()
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "one disposable PostgreSQL as primary 127.0.0.1 + standby 127.0.0.2",
                    "postgres_version": version, "binary": args.binary,
                    "sha256": digest.hexdigest(), "cases": reports, "failures": failures}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return int(failures > 0)
    finally:
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=20)


if __name__ == "__main__":
    raise SystemExit(main())
