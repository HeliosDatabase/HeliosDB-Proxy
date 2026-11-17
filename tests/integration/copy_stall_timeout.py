#!/usr/bin/env python3
"""Native PG COPY inactivity/rollback/capacity checks on an owned fixture.

Run with the fleet lock and bounded scope. Never uses an existing backend.
Usage: copy_stall_timeout.py PROXY_BINARY --output NEW_DIRECTORY
"""
import argparse
import importlib.util
import json
import pathlib
import secrets
import socket
import struct
import subprocess
import sys
import time
import traceback
import urllib.request

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "wire_fixture", pathlib.Path(__file__).with_name("cache_prepared_commit.py"))
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def fields(body):
    pairs = {}
    while body and body[0]:
        tag, rest = chr(body[0]), body[1:]
        value, _, body = rest.partition(b"\0")
        pairs[tag] = value.decode(errors="replace")
    return pairs


def start_copy(client, table, extended=False):
    sql = f"COPY {table} FROM STDIN".encode()
    if extended:
        packet = (wire.frame(b"P", b"\0" + sql + b"\0\0\0")
                  + wire.frame(b"B", b"\0\0" + bytes(6))
                  + wire.frame(b"E", b"\0" + bytes(4)) + wire.frame(b"H"))
    else:
        packet = wire.frame(b"Q", sql + b"\0")
    client.sendall(packet)
    response = wire.drain(client, b"G")
    assert response[-1][0] == b"G", response


def copy_backend_pid(direct, table):
    # Identify the actual COPY connection on the owned backend without a
    # function call through the proxy or depending on a virtual cancel key.
    rows = wire.query(direct, f"SELECT pid FROM pg_stat_activity WHERE query = 'COPY {table} FROM STDIN'")
    assert len(rows) == 1, ("expected one owned COPY backend", table, rows)
    return int(rows[0])


def proxy_row_count(client, table):
    # Pure SQL keeps journal assertions independent of read-function trust.
    return [str(len(wire.query(client, f"SELECT v FROM {table}")))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    binary, out = args.binary.resolve(), args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    name = "proxy-copy-stall-" + secrets.token_hex(12)
    pgport, px, admin = wire.port(), wire.port(), wire.port()
    assert len({pgport, px, admin}) == 3, "port allocation collided"
    owned = proxy = None
    reports, proxy_cleanup = [], []
    initial_digest = wire.binary_digest(binary)
    backend = None
    backend_version = None
    try:
        owned = subprocess.check_output([
            "docker", "run", "-d", "--rm", "--pull=never", "--name", name,
            "--label", "proxy-cache-2pc-owned=" + name, "--network", "host",
            "--memory", "1g", "--memory-swap", "1g", "--cpus", "2",
            "--pids-limit", "256", "--tmpfs", "/var/lib/postgresql:rw,size=512m",
            "-e", "POSTGRES_USER=audit", "-e", "POSTGRES_DB=audit",
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:18.4-bookworm",
            "postgres", "-c", "listen_addresses=127.0.0.1", "-c", f"port={pgport}",
            "-c", f"cluster_name={name}", "-c", "shared_buffers=64MB",
        ], text=True, timeout=60).strip()
        deadline = time.monotonic() + 60
        while True:
            try:
                backend = wire.backend_identity(owned, name, pgport)
                break
            except (OSError, EOFError, RuntimeError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.25)
        # The initdb temporary server must finish before test connections.
        time.sleep(2)
        assert wire.backend_identity(owned, name, pgport) == backend
        config = out / "proxy.toml"
        config.write_text(f'''strict_config = true
listen_address = "127.0.0.1:{px}"
admin_address = "127.0.0.1:{admin}"
tr_mode = "session"
[[nodes]]
host = "127.0.0.1"
port = {pgport}
role = "primary"
[pool_mode]
mode = "session"
[limits]
copy_idle_timeout_secs = 1
[multi_tenancy]
enabled = true
identify_by = "user"
tenants = ["audit"]
max_active_queries_per_tenant = 1
active_query_wait_ms = 0
''')
        with (out / "proxy.log").open("wb") as log:
            proxy = subprocess.Popen([str(binary), "--config", str(config), "--log-level", "debug"], stdout=log, stderr=log)
            deadline = time.monotonic() + 30
            while True:
                if proxy.poll() is not None:
                    raise RuntimeError("proxy exited before readiness")
                if wire.owns_listener(proxy.pid, px):
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("owned proxy did not listen")
                time.sleep(.1)
            with wire.connect(pgport) as direct:
                def timeout_count():
                    with urllib.request.urlopen(f"http://127.0.0.1:{admin}/metrics", timeout=5) as response:
                        return json.load(response)["copy_input_timeouts"]

                assert timeout_count() == 0

                backend_version = wire.query(direct, "SHOW server_version")[0]
                assert backend_version.split()[0] == "18.4", backend_version
                def check_identity():
                    assert proxy.poll() is None and wire.owns_listener(proxy.pid, px)
                    assert wire.backend_identity(owned, name, pgport) == backend

                def capacity_refused(other):
                    other.sendall(wire.frame(b"Q", b"SELECT 1\0"))
                    errors = [fields(body) for tag, body in wire.drain(other) if tag == b"E"]
                    assert [error.get("C") for error in errors] == ["53300"], ("expected immediate tenant-cap refusal", errors)

                for extended in [False, True]:
                    for partial in [False, True]:
                        check_identity()
                        table = "copy_stall_" + secrets.token_hex(6)
                        wire.query(direct, f"CREATE TABLE {table}(v text)")
                        with wire.connect(px) as a, wire.connect(px) as b:
                            start_copy(a, table, extended)
                            pid = copy_backend_pid(direct, table)
                            capacity_refused(b)
                            # Make PostgreSQL accept actual uncommitted rows.
                            a.sendall(wire.frame(b"d", b"accepted-before-stall\n"))
                            if partial:
                                a.sendall(b"d" + struct.pack("!I", 16) + b"x")
                            began = time.monotonic()
                            tag, body = wire.receive(a)
                            error = fields(body)
                            assert tag == b"E" and error.get("C") == "57014" and error.get("S") == "FATAL", (tag, error)
                            assert "COPY input timeout" in error.get("M", ""), error
                            assert .8 <= time.monotonic() - began < 4, "wrong inactivity interval"
                            assert a.recv(1) == b"", "timeout must close without fabricated ReadyForQuery"
                            deadline = time.monotonic() + 5
                            while wire.query(direct, f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}") != ["0"]:
                                if time.monotonic() > deadline:
                                    raise RuntimeError("COPY backend remained alive after timeout")
                                time.sleep(.05)
                            assert proxy_row_count(b, table) == ["0"], "COPY rows survived rollback or tenant capacity leaked"
                            assert wire.query(direct, f"SELECT count(*) FROM {table}") == ["0"]
                        assert timeout_count() == len(reports) + 1, "timeout counter did not match expiries"
                        reports.append({"case": f"extended={extended}/partial={partial}", "pass": True, "rolled_back": True, "capacity_reclaimed": True})
                        wire.query(direct, f"DROP TABLE {table}")

                table = "copy_progress_" + secrets.token_hex(6)
                wire.query(direct, f"CREATE TABLE {table}(v text)")
                with wire.connect(px) as a, wire.connect(px) as b:
                    start_copy(a, table)
                    capacity_refused(b)
                    for _ in range(3):
                        time.sleep(.4)
                        a.sendall(wire.frame(b"d", b"progress\n"))
                    a.sendall(wire.frame(b"c"))
                    assert not any(tag == b"E" for tag, _ in wire.drain(a))
                    assert proxy_row_count(b, table) == ["3"]
                    assert wire.query(a, "SELECT 1") == ["1"]
                assert timeout_count() == 4, "client progress was counted as a timeout"
                reports.append({"case": "progress-resets-inactivity", "pass": True})
                wire.query(direct, f"TRUNCATE {table}")
                with wire.connect(px) as a, wire.connect(px) as b:
                    start_copy(a, table)
                    pid = copy_backend_pid(direct, table)
                    a.sendall(wire.frame(b"d", b"cancelled-row\n"))
                    assert wire.query(direct, f"SELECT pg_cancel_backend({pid})") == ["t"]
                    # Let a native COPY read observe the pending interrupt.
                    # The backend's simple-query RFQ must then disarm the
                    # timer through the buffered watch, without client Sync.
                    a.sendall(wire.frame(b"d", b"drive-interrupt\n"))
                    tag, body = wire.receive(a)
                    error = fields(body)
                    assert tag == b"E" and error.get("C") == "57014" and error.get("S") == "ERROR", (tag, error)
                    response = wire.drain(a)
                    assert not any(tag == b"E" for tag, _ in response), response
                    time.sleep(1.2)
                    assert wire.query(a, "SELECT 1") == ["1"], "backend-ended COPY retained its deadline"
                    assert proxy_row_count(b, table) == ["0"]
                assert timeout_count() == 4, "backend cancellation was counted as client inactivity"
                reports.append({"case": "backend-cancellation-disarms-deadline", "pass": True})
                wire.query(direct, f"DROP TABLE {table}")

                def committed():
                    with urllib.request.urlopen(f"http://127.0.0.1:{admin}/metrics", timeout=5) as response:
                        return json.load(response)["journal_committed_total"]

                def wait_committed(expected):
                    deadline = time.monotonic() + 5
                    while committed() != expected:
                        if time.monotonic() > deadline:
                            raise AssertionError(("journal commit count", committed(), expected))
                        time.sleep(.1)

                # Only the progress COPY committed; cancelled COPYs did not.
                wait_committed(1)
                table = "journal_flush_" + secrets.token_hex(6)
                wire.query(direct, f"CREATE TABLE {table}(v text)")
                with wire.connect(px) as a:
                    for sql in ["SELECT 1", f"INSERT INTO {table} VALUES('first')"]:
                        raw = sql.encode()
                        a.sendall(wire.frame(b"P", b"\0" + raw + b"\0\0\0")
                                  + wire.frame(b"B", b"\0\0" + bytes(6))
                                  + wire.frame(b"E", b"\0" + bytes(4)) + wire.frame(b"H"))
                        wire.drain(a, b"C")
                    assert wire.query(direct, f"SELECT count(*) FROM {table}") == ["0"], "Flush must not commit"
                    a.sendall(wire.frame(b"S"))
                    wire.drain(a)
                    assert wire.query(direct, f"SELECT count(*) FROM {table}") == ["1"]
                    wait_committed(2)
                    wire.query(a, "SELECT 1", mode="extended_sync")
                    wire.query(a, f"INSERT INTO {table} VALUES('second')", mode="extended_sync")
                    assert wire.query(direct, f"SELECT count(*) FROM {table}") == ["2"]
                    wait_committed(3)
                reports.append({"case": "flushed-write-journal-and-cycle-positions", "pass": True,
                                "native_rows": 2, "journaled_writes": 2})
                wire.query(direct, f"DROP TABLE {table}")
                # Every extended COPY still owes a final Sync. Both a
                # client end and an asynchronous backend error must be bounded.
                for ending in ["done", "fail", "backend-error"]:
                    check_identity()
                    table = "copy_sync_" + secrets.token_hex(6)
                    wire.query(direct, f"CREATE TABLE {table}(v text)")
                    with wire.connect(px) as a, wire.connect(px) as b:
                        start_copy(a, table, extended=True)
                        pid = copy_backend_pid(direct, table)
                        a.sendall(wire.frame(b"d", b"must-roll-back\n"))
                        if ending == "backend-error":
                            assert wire.query(direct, f"SELECT pg_cancel_backend({pid})") == ["t"]
                            a.sendall(wire.frame(b"d", b"drive-interrupt\n"))
                        else:
                            a.sendall(wire.frame(b"c") if ending == "done" else wire.frame(b"f", b"cancelled\0"))
                        tag, body = wire.receive(a)
                        assert tag == (b"C" if ending == "done" else b"E"), (ending, tag, body)
                        capacity_refused(b)
                        tag, body = wire.receive(a)
                        error = fields(body)
                        assert tag == b"E" and error.get("S") == "FATAL" and error.get("C") == "57014", (ending, tag, error)
                        assert a.recv(1) == b"", "withheld final Sync must close without RFQ"
                        deadline = time.monotonic() + 5
                        while wire.query(direct, f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}") != ["0"]:
                            if time.monotonic() > deadline: raise RuntimeError("final-Sync backend remained alive")
                            time.sleep(.05)
                        assert proxy_row_count(b, table) == ["0"]
                        assert wire.query(direct, f"SELECT count(*) FROM {table}") == ["0"]
                    reports.append({"case": "withheld-final-sync-" + ending, "pass": True,
                                    "rolled_back": True, "capacity_reclaimed": True})
                    wire.query(direct, f"DROP TABLE {table}")
                wait_committed(3)  # COPYs abandoned without Sync never committed.
                for done in [True, False]:
                    journal_before = committed()
                    table = "copy_sync_ok_" + secrets.token_hex(6)
                    wire.query(direct, f"CREATE TABLE {table}(v text)")
                    if done:
                        # Completing COPY may take longer than its client
                        # inactivity limit. Only time waiting for final Sync
                        # after backend acknowledgement belongs to that limit.
                        wire.query(direct, f"CREATE FUNCTION {table}_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1.4); RETURN NEW; END $$")
                        wire.query(direct, f"CREATE TRIGGER delay_copy BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {table}_delay()")
                    with wire.connect(px) as a, wire.connect(px) as b:
                        if done:
                            a.sendall(wire.frame(b"P", b"\0SELECT 42\0\0\0")
                                      + wire.frame(b"B", b"\0\0" + bytes(6))
                                      + wire.frame(b"E", b"\0" + bytes(4)) + wire.frame(b"H"))
                            assert wire.drain(a, b"C")[-1][0] == b"C"
                        start_copy(a, table, extended=True)
                        started = time.monotonic()
                        a.sendall(wire.frame(b"d", b"row\n"))
                        a.sendall(wire.frame(b"c") if done else wire.frame(b"f", b"cancelled\0"))
                        tag, body = wire.receive(a)
                        assert tag == (b"C" if done else b"E"), (tag, body)
                        if done:
                            assert time.monotonic() - started >= 1.2, "backend-work control did not exceed the one-second input timeout"
                        a.sendall(wire.frame(b"S")); wire.drain(a)
                        wait_committed(journal_before + int(done))
                        # Keep the same connection: a COPY completion/error
                        # must neither lose nor poison the next Flush prefix.
                        for sql in ["SELECT 42", f"INSERT INTO {table} VALUES('after-copy')"]:
                            raw = sql.encode()
                            a.sendall(wire.frame(b"P", b"\0" + raw + b"\0\0\0")
                                      + wire.frame(b"B", b"\0\0" + bytes(6))
                                      + wire.frame(b"E", b"\0" + bytes(4)) + wire.frame(b"H"))
                            wire.drain(a, b"C")
                        assert wire.query(direct, f"SELECT count(*) FROM {table}") == [str(int(done))], "post-COPY Flush committed early"
                        assert committed() == journal_before + int(done), "journal committed before post-COPY Sync"
                        a.sendall(wire.frame(b"S")); wire.drain(a)
                        wait_committed(journal_before + int(done) + 1)
                        time.sleep(1.2)
                        assert wire.query(a, "SELECT 1") == ["1"]
                        expected = [str(int(done) + 1)]
                        assert proxy_row_count(b, table) == expected
                        assert wire.query(direct, f"SELECT count(*) FROM {table}") == expected
                    reports.append({"case": "timely-final-sync-" + str(done), "pass": True,
                                    "copy_rows": int(done), "native_rows": int(done) + 1,
                                    "post_copy_flushed_write_journaled": True,
                                    "backend_work_exceeded_copy_timeout": done})
                    wire.query(direct, f"DROP TABLE {table}")
                    if done:
                        wire.query(direct, f"DROP FUNCTION {table}_delay()")
                assert timeout_count() == 7, "four input stalls plus three withheld final Syncs"
                check_identity()
    except Exception as error:
        reports.append({"case": "failure", "pass": False, "error": repr(error), "traceback": traceback.format_exc()})
    finally:
        if proxy is not None:
            proxy_cleanup.append(wire.stop_proxy(proxy))
        cleanup = wire.stop_container(owned, name)
        final_digest = wire.binary_digest(binary)
        result = {"binary": str(binary), "binary_sha256": initial_digest,
                  "final_binary_sha256": final_digest, "backend_identity": backend,
                  "backend_version": backend_version,
                  "reports": reports, "proxy_cleanup": proxy_cleanup, "cleanup": cleanup,
                  "script_sha256": wire.binary_digest(pathlib.Path(__file__)),
                  "pass": len(reports) == 12 and all(r["pass"] for r in reports)
                          and initial_digest == final_digest and cleanup["removed"]
                          and not cleanup["errors"] and len(proxy_cleanup) == 1
                          and all(r["reaped"] and not r["errors"] for r in proxy_cleanup)}
        (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
