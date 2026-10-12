#!/usr/bin/env python3
"""TR-08: a transaction that uses named statements prepared in EARLIER protocol
cycles survives the loss of its backend connection (tr_mode = "transaction").

Starts and removes its own bounded, loopback-only PostgreSQL Docker container
and its own proxy; uses no existing database and no third-party Python packages.
Usage: python3 tests/integration/tr_prepared_replay.py PROXY [--output evidence.json]
Requires the already available postgres:18.4-bookworm image (never pulls images).

Cases (each kills the proxy's backend session with pg_terminate_backend while the
transaction is open, then continues on the same client connection):
  named_insert   Parse "ins" (binary int4 + text params) in its own cycle, BEGIN,
                 Bind/Execute ins in a later cycle, kill, Bind/Execute ins again,
                 COMMIT. Expected: no error, rows {1, 2} exactly once each.
  named_portal   Parse "sel" in its own cycle, BEGIN, Bind portal "cur" to it and
                 Execute 3 rows in one cycle, kill, Execute "cur" 3 more rows in a
                 later cycle. Expected: rows 4..6, then COMMIT.
  redefined      Like named_insert, but the session re-Parses "ins" after Close in
                 an errored batch first: the name is uncertain, so the proxy must
                 refuse (57P01) rather than replay a statement it cannot vouch for.
Exit 1 if any case fails its expectation.
"""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import socket
import struct
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "boundary", pathlib.Path(__file__).resolve().parents[2] / "scripts/regress/tr-boundary-test.py")
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)
frame = wire.frame
PG_ADDRESS = None


def startup(address, user="audit"):
    conn = socket.create_connection(address, timeout=10)
    params = struct.pack("!I", 196608) + f"user\0{user}\0database\0audit\0\0".encode()
    conn.sendall(struct.pack("!I", len(params) + 4) + params)
    response = wire.drain(conn)
    if any(tag == b"E" for tag, _ in response):
        conn.close()
        raise RuntimeError(f"startup failed: {response!r}")
    return conn


def rows(response):
    out = []
    for tag, body in response:
        if tag == b"D":
            count = struct.unpack("!H", body[:2])[0]
            pos, values = 2, []
            for _ in range(count):
                size = struct.unpack("!i", body[pos:pos + 4])[0]
                pos += 4
                values.append(None if size < 0 else body[pos:pos + size].decode())
                pos += max(size, 0)
            out.append(values)
    return out


def errors(response):
    return [part[1:].decode() for tag, body in response if tag == b"E"
            for part in body.split(b"\0") if part.startswith(b"C")]


def pg_query(sql):
    with startup(PG_ADDRESS) as conn:
        response = wire.query(conn, sql)
        if errors(response):
            raise RuntimeError(f"PG query failed: {response!r}")
        return rows(response)


def parse(name, sql, oids=()):
    body = name.encode() + b"\0" + sql.encode() + b"\0" + struct.pack("!H", len(oids))
    body += b"".join(struct.pack("!I", oid) for oid in oids)
    return frame(b"P", body)


def bind_ins(portal, stmt, ident, note):
    # Parameter formats: int4 binary, text; result format: text.
    value = note.encode()
    body = portal.encode() + b"\0" + stmt.encode() + b"\0"
    body += struct.pack("!HHH", 2, 1, 0)
    body += struct.pack("!H", 2) + struct.pack("!Ii", 4, ident) + struct.pack("!I", len(value)) + value
    body += struct.pack("!H", 0)
    return frame(b"B", body)


def bind(portal, stmt):
    return frame(b"B", portal.encode() + b"\0" + stmt.encode() + b"\0" + struct.pack("!HHH", 0, 0, 0))


def execute(portal, max_rows=0):
    return frame(b"E", portal.encode() + b"\0" + struct.pack("!I", max_rows))


def close_statement(name):
    return frame(b"C", b"S" + name.encode() + b"\0")


SYNC = frame(b"S")


def send(conn, wire_bytes):
    conn.sendall(wire_bytes)
    return wire.drain(conn)


def kill_backend(pid):
    pg_query(f"SELECT pg_terminate_backend({int(pid)})")
    deadline = time.monotonic() + 10
    while pg_query(f"SELECT count(*) FROM pg_stat_activity WHERE pid = {int(pid)}") != [["0"]]:
        if time.monotonic() >= deadline:
            raise TimeoutError("backend did not exit")
        time.sleep(0.05)
    time.sleep(0.5)  # let the proxy's idle watch see the closed socket


def start_proxy(binary, temp, pg_port, mode="transaction", top="", limits=""):
    port, admin = wire.free_port(), wire.free_port()
    cfg = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = true
tr_mode = "{mode}"
write_timeout_secs = 15
{top}[limits]
{limits}[pool_mode]
mode = "session"
[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
[[nodes]]
host = "127.0.0.1"
port = {pg_port}
role = "primary"
'''
    config = temp / "proxy.toml"
    config.write_text(cfg)
    log = (temp / "proxy.log").open("w+")
    proc = subprocess.Popen([binary, "--config", str(config)], stdout=log, stderr=log)
    deadline = time.monotonic() + 15
    while True:
        try:
            return proc, log, startup(("127.0.0.1", port))
        except (OSError, RuntimeError, EOFError):
            if proc.poll() is not None or time.monotonic() >= deadline:
                log.seek(0)
                raise RuntimeError("proxy did not start: " + log.read()[-3000:])
            time.sleep(0.1)


def case_named_insert(client, table, redefine=False):
    report = {}
    send(client, frame(b"Q", f"CREATE TABLE {table} (id int PRIMARY KEY, note text)\0".encode()))
    ins = f"INSERT INTO {table} VALUES ($1, $2)"
    r = send(client, parse("ins", ins, (23, 25)) + SYNC)
    assert not errors(r), r
    if redefine:
        # A re-Parse of an existing name fails (42P05): the proxy can no longer
        # vouch for what the backend holds under "ins".
        r = send(client, parse("ins", f"INSERT INTO {table} VALUES ($1, $2 || '!')", (23, 25)) + SYNC)
        report["redefine_sqlstates"] = errors(r)
    pid = rows(send(client, frame(b"Q", b"SELECT pg_backend_pid()\0")))[0][0]
    send(client, frame(b"Q", b"BEGIN\0"))
    r = send(client, bind_ins("", "ins", 1, "before") + execute("") + SYNC)
    assert not errors(r), r
    kill_backend(pid)
    r = send(client, bind_ins("", "ins", 2, "after") + execute("") + SYNC)
    report["second_insert_sqlstates"] = errors(r)
    r = send(client, frame(b"Q", b"COMMIT\0"))
    report["commit_tags"] = [body.rstrip(b"\0").decode() for tag, body in r if tag == b"C"]
    report["commit_sqlstates"] = errors(r)
    report["rows"] = pg_query(f"SELECT id, note FROM {table} ORDER BY id")
    return report


def case_named_portal(client):
    report = {}
    sel = "SELECT g FROM generate_series(1, 10) AS g ORDER BY g"
    r = send(client, parse("sel", sel) + SYNC)
    assert not errors(r), r
    pid = rows(send(client, frame(b"Q", b"SELECT pg_backend_pid()\0")))[0][0]
    send(client, frame(b"Q", b"BEGIN\0"))
    r = send(client, bind("cur", "sel") + execute("cur", 3) + SYNC)
    report["first_fetch"] = [v[0] for v in rows(r)]
    kill_backend(pid)
    r = send(client, execute("cur", 3) + SYNC)
    report["second_fetch_sqlstates"] = errors(r)
    report["second_fetch"] = [v[0] for v in rows(r)]
    r = send(client, frame(b"Q", b"COMMIT\0"))
    report["commit_sqlstates"] = errors(r)
    return report


def start_postgres(name):
    """Start the disposable PostgreSQL on PG_ADDRESS; returns its version."""
    subprocess.run(["docker", "run", "--detach", "--rm", "--pull=never", "--name", name,
                    "--memory=768m", "--memory-swap=768m", "--cpus=1", "--pids-limit=64",
                    "--network", "host", "--tmpfs", "/var/lib/postgresql:rw,size=256m",
                    "--env", "PGDATA=/var/lib/postgresql/audit",
                    "--env", f"PGPORT={PG_ADDRESS[1]}",
                    "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--env", "POSTGRES_USER=audit",
                    "--env", "POSTGRES_DB=audit", "postgres:18.4-bookworm",
                    "-c", "listen_addresses=127.0.0.1", "-p", str(PG_ADDRESS[1])],
                   check=True, capture_output=True, timeout=30)
    deadline = time.monotonic() + 30
    while True:
        try:
            return pg_query("SHOW server_version")[0][0]
        except (OSError, EOFError, RuntimeError):
            if time.monotonic() >= deadline:
                raise TimeoutError("isolated PostgreSQL did not become ready")
            time.sleep(0.2)


def main():
    global PG_ADDRESS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    name = "proxy-tr08-" + uuid.uuid4().hex[:12]
    PG_ADDRESS = ("127.0.0.1", wire.free_port())
    reports = []
    try:
        version = start_postgres(name)
        cases = [
            ("named_insert", lambda c: case_named_insert(c, "tr08_ledger"),
             lambda r: (r["second_insert_sqlstates"] == [] and r["commit_sqlstates"] == []
                        and r["rows"] == [["1", "before"], ["2", "after"]])),
            ("named_portal", case_named_portal,
             lambda r: (r["first_fetch"] == ["1", "2", "3"] and r["second_fetch_sqlstates"] == []
                        and r["second_fetch"] == ["4", "5", "6"] and r["commit_sqlstates"] == [])),
            ("redefined", lambda c: case_named_insert(c, "tr08_uncertain", redefine=True),
             lambda r: (r["redefine_sqlstates"] == ["42P05"]
                        and r["second_insert_sqlstates"] == ["57P01"]
                        and r["rows"] == [])),
        ]
        for case, run, expect in cases:
            with tempfile.TemporaryDirectory(prefix="proxy-tr08-") as temp:
                temp = pathlib.Path(temp)
                proc, log, client = start_proxy(args.binary, temp, PG_ADDRESS[1])
                try:
                    report = {"case": case}
                    try:
                        report.update(run(client))
                        report["invariant"] = "PASS" if expect(report) else "FAIL"
                    except (AssertionError, OSError, EOFError, RuntimeError, ValueError) as error:
                        report["error"] = repr(error)
                        report["invariant"] = "FAIL"
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
                                                if "helios::tr" in line][-6:]
                    log.close()
            reports.append(report)
            print(json.dumps(report), flush=True)
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "disposable PostgreSQL; pg_terminate_backend mid-transaction",
                    "postgres_version": version, "binary": args.binary,
                    "sha256": digest.hexdigest(), "cases": reports, "failures": failures}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return int(failures > 0)
    finally:
        # The unique name belongs to this run, including a partially started container.
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=20)


if __name__ == "__main__":
    raise SystemExit(main())
