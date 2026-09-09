#!/usr/bin/env python3
"""Bounded PG-wire fault probes; no database or third-party Python packages.

Usage: python3 scripts/regress/tr-boundary-test.py /path/to/heliosdb-proxy
Two disposable loopback protocol fixtures capture what the actual proxy sends.
These test retry/stream/state boundaries, not PostgreSQL execution, replication,
or durability. Exit 1 means a safety invariant was violated; 2 is a harness error.
"""

import argparse
import hashlib
import json
import pathlib
import socket
import struct
import subprocess
import tempfile
import threading
import time


def frame(tag, body=b""):
    return tag + struct.pack("!I", len(body) + 4) + body


def exact(sock, count):
    result = b""
    while len(result) < count:
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise EOFError("peer closed")
        result += chunk
    return result


def receive(sock):
    tag = exact(sock, 1)
    size = struct.unpack("!I", exact(sock, 4))[0]
    if not 4 <= size <= 1024 * 1024:
        raise ValueError("invalid fixture frame length")
    return tag, exact(sock, size - 4)


def result_rows(values, complete=True):
    desc = struct.pack("!H", 1) + b"v\0" + struct.pack("!IhIhih", 0, 0, 25, -1, -1, 0)
    wire = frame(b"T", desc)
    for value in values:
        value = str(value).encode()
        wire += frame(b"D", struct.pack("!HI", 1, len(value)) + value)
    if complete:
        wire += frame(b"C", f"SELECT {len(values)}\0".encode())
    return wire


class Backend:
    def __init__(self, fault_sql=None, partial=False):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.sock.settimeout(0.1)
        self.port = self.sock.getsockname()[1]
        self.fault_sql = fault_sql
        self.fault_after = 0
        self.fault_triggered = threading.Event()
        self.partial = partial
        self.stop = threading.Event()
        self.queries = []
        self.errors = []
        self.clients = []
        self.workers = []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            self.clients.append(conn)
            worker = threading.Thread(target=self.serve, args=(conn,), daemon=True)
            self.workers.append(worker)
            worker.start()

    def serve(self, conn):
        conn.settimeout(5)
        try:
            size = struct.unpack("!I", exact(conn, 4))[0]
            startup = exact(conn, size - 4)
            if startup == struct.pack("!I", 80877103):
                conn.sendall(b"N")
                return  # daemon health probe
            conn.sendall(frame(b"R", struct.pack("!I", 0))
                         + frame(b"S", b"server_version\0" + b"18.0\0")
                         + frame(b"K", struct.pack("!II", 123, 456)) + frame(b"Z", b"I"))
            status = b"I"
            statements, portals = {}, {}
            pending = b""
            while not self.stop.is_set():
                tag, body = receive(conn)
                if tag == b"X":
                    return
                if tag == b"P":
                    name, sql, _ = body.split(b"\0", 2)
                    statements[name] = sql.decode()
                    pending += frame(b"1")
                    continue
                if tag == b"B":
                    portal, name, _ = body.split(b"\0", 2)
                    portals[portal] = statements[name]
                    pending += frame(b"2")
                    continue
                if tag == b"S":
                    conn.sendall(pending + frame(b"Z", status))
                    pending = b""
                    continue
                if tag == b"H":
                    conn.sendall(pending)
                    pending = b""
                    continue
                if tag == b"E":
                    sql = portals[body.split(b"\0", 1)[0]]
                elif tag == b"Q":
                    sql = body.rstrip(b"\0").decode()
                else:
                    raise ValueError(f"unexpected frontend tag {tag!r}")
                self.queries.append(sql)
                if sql == self.fault_sql and self.queries.count(sql) > self.fault_after:
                    # Refuse future health probes as well as ending this socket.
                    self.stop.set()
                    self.sock.close()
                    if self.partial:
                        if self.partial == "complete":
                            prefix = result_rows([1], complete=True)
                        elif self.partial == "error":
                            prefix = frame(b"E", b"SERROR\0CXX000\0Mfixture error\0\0")
                        elif self.partial == "fragment":
                            prefix = result_rows([1], complete=False)[:3]
                        elif self.partial == "large":
                            prefix = result_rows(["x" * 65536], complete=False)
                        else:
                            prefix = result_rows([1], complete=False)
                        conn.sendall((pending if tag == b"E" else b"") + prefix)
                        time.sleep(0.1)
                    self.fault_triggered.set()
                    return
                upper = sql.strip().upper()
                if upper.startswith("/* AUDIT */"):
                    upper = upper[len("/* AUDIT */"):].strip()
                if upper.startswith("BEGIN"):
                    status = b"T"
                elif upper in ("COMMIT", "ROLLBACK"):
                    status = b"I"
                if upper.startswith("SELECT"):
                    response = result_rows([1, 2])
                else:
                    response = frame(b"C", upper.split()[0].encode() + b"\0")
                if tag == b"Q":
                    conn.sendall(response + frame(b"Z", status))
                else:
                    pending += response
        except (EOFError, ConnectionError, socket.timeout):
            pass
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            conn.close()

    def close(self):
        self.stop.set()
        self.sock.close()
        for conn in self.clients:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(1)
        for worker in self.workers:
            worker.join(1)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def drain(client):
    messages = []
    while len(messages) < 100:
        msg = receive(client)
        messages.append(msg)
        if msg[0] == b"Z":
            return messages
    raise ValueError("too many fixture frames")


def query_wire(sql, boundary=b"S", unnamed=False):
    if isinstance(sql, list):
        wire = b""
        for i, statement in enumerate(sql):
            name, portal = (b"", b"") if unnamed else (f"s{i}".encode(), f"p{i}".encode())
            wire += frame(b"P", name + b"\0" + statement.encode() + b"\0\0\0")
            wire += frame(b"B", portal + b"\0" + name + b"\0" + b"\0" * 6)
            wire += frame(b"E", portal + b"\0" + struct.pack("!I", 0))
        return wire + frame(boundary)
    else:
        return frame(b"Q", sql.encode() + b"\0")



def query(client, sql, unnamed=False):
    client.sendall(query_wire(sql, unnamed=unnamed))
    return drain(client)


def fault_response(client, slow_reader=False):
    """Retain complete frames and an incomplete tail when the proxy closes.

    Malformed proxy output is an invariant failure, not a harness exception.
    """
    messages, pending = [], b""
    while len(messages) < 100:
        while len(pending) >= 5:
            size = struct.unpack("!I", pending[1:5])[0]
            if not 4 <= size <= 1024 * 1024:
                return messages, False, len(pending), True, False
            if len(pending) < size + 1:
                break
            tag, body = pending[:1], pending[5:size + 1]
            pending = pending[size + 1:]
            messages.append((tag, body))
            if tag == b"Z":
                return messages, False, len(pending), False, False
        try:
            if slow_reader:
                time.sleep(0.001)
            chunk = client.recv(1024 if slow_reader else 65536)
        except ConnectionResetError:
            return messages, True, len(pending), False, False
        except socket.timeout:
            return messages, False, len(pending), False, True
        if not chunk:
            return messages, True, len(pending), False, False
        pending += chunk
    raise ValueError("too many fixture response frames")


def scenario(binary, name, mode, setup, fault_sql, partial=False, cap=256, flush=False,
             slow_reader=False, unnamed=False, warm=False):
    a, b = Backend(fault_sql[-1] if isinstance(fault_sql, list) else fault_sql, partial), Backend()
    a.fault_after = int(warm)
    proc = None
    client = None
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-tr-boundary-") as temp:
            temp = pathlib.Path(temp)
            port, admin = free_port(), free_port()
            cfg = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = false
tr_mode = "{mode}"
optimize_unnamed_parse = true
write_timeout_secs = 2
[pool]
min_connections = 0
max_connections = 4
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 2
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
tr_max_session_set_statements = {cap}
[[nodes]]
host = "127.0.0.1"
port = {a.port}
role = "primary"
weight = 100
enabled = true
[[nodes]]
host = "127.0.0.1"
port = {b.port}
role = "primary"
weight = 100
enabled = true
'''
            config = temp / "proxy.toml"
            config.write_text(cfg)
            with (temp / "proxy.log").open("w+") as log:
                proc = subprocess.Popen([binary, "--config", str(config)], stdout=log, stderr=log)
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    try:
                        client = socket.create_connection(("127.0.0.1", port), timeout=5)
                        break
                    except OSError:
                        if proc.poll() is not None:
                            log.seek(0)
                            raise RuntimeError(log.read()[-3000:])
                        time.sleep(0.05)
                if client is None:
                    raise TimeoutError("proxy did not listen")
                params = struct.pack("!I", 196608) + b"user\0audit\0database\0audit\0\0"
                client.sendall(struct.pack("!I", len(params) + 4) + params)
                drain(client)
                for sql in setup:
                    if any(tag == b"E" for tag, _ in query(client, sql, unnamed)):
                        raise RuntimeError(f"setup rejected: {sql}")
                if warm and any(tag == b"E" for tag, _ in query(client, fault_sql, unnamed)):
                    raise RuntimeError("warm-up query was rejected")
                client.sendall(query_wire(fault_sql, b"H" if flush else b"S", unnamed))
                if flush:
                    # Let the backend watch deliver the Flush prefix before
                    # sending Sync to the session whose backend has now died.
                    time.sleep(0.2)
                    try:
                        client.sendall(frame(b"S"))
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                response, closed, trailing, malformed, timed_out = fault_response(client, slow_reader)
                if not a.fault_triggered.wait(timeout=1):
                    raise RuntimeError("backend fixture did not reach its intended fault boundary")
                tags = "".join(tag.decode() for tag, _ in response)
                codes = [part[1:].decode() for tag, body in response if tag == b"E"
                         for part in body.split(b"\0") if part.startswith(b"C")]
                row_count = sum(tag == b"D" for tag, _ in response)
                report = {"case": name, "mode": mode, "response_tags": tags,
                          "sqlstates": codes, "data_rows": row_count,
                          "replacement_queries": list(b.queries), "connection_closed": closed,
                          "unnamed": unnamed, "warm": warm,
                          "response_timed_out": timed_out,
                          "trailing_response_bytes": trailing, "malformed_response": malformed}
                if a.errors or b.errors:
                    raise RuntimeError(str(a.errors + b.errors))
                return report
    finally:
        if client:
            client.close()
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        a.close()
        b.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--case", action="append", help="Run only the named case (repeatable)")
    parser.add_argument("--output", type=pathlib.Path, help="Save JSON evidence, including binary hash")
    args = parser.parse_args()
    cases = [
        ("plain_commit_unknown", "transaction", ["BEGIN", "INSERT INTO audit VALUES (1)"], "COMMIT", False, 256),
        ("commented_commit_unknown", "transaction", ["BEGIN", "INSERT INTO audit VALUES (1)"], "/* audit */ COMMIT", False, 256),
        ("extended_commit_unknown", "transaction", ["BEGIN", "INSERT INTO audit VALUES (1)"], ["SELECT 1", "COMMIT"], False, 256),
        ("partial_select", "select", [], "SELECT 1", True, 256),
        ("partial_select_large", "select", [], "SELECT 1", "large", 256),
        ("partial_select_slow_reader", "select", [], "SELECT 1", "large", 256, False, True),
        ("partial_none", "none", [], "SELECT 1", True, 256),
        ("partial_session", "session", [], "SELECT 1", True, 256),
        ("partial_transaction", "transaction", ["BEGIN", "INSERT INTO audit VALUES (1)"], "SELECT 1", True, 256),
        ("partial_select_complete", "select", [], "SELECT 1", "complete", 256),
        ("partial_select_error", "select", [], "SELECT 1", "error", 256),
        ("partial_extended", "select", [], ["SELECT 1"], True, 256),
        ("partial_extended_flush", "select", [], ["SELECT 1"], True, 256, True),
        ("partial_extended_fragment", "select", [], ["SELECT 1"], "fragment", 256, True),
        ("partial_unnamed", "select", [], ["SELECT 1"], True, 256, False, False, True),
        ("partial_unnamed_warm", "select", [], ["SELECT 1"], True, 256, False, False, True, True),
        ("partial_unnamed_fragment", "select", [], ["SELECT 1"], "fragment", 256, True, False, True),
        ("partial_none_flush", "none", [], ["SELECT 1"], True, 256, True),
        ("partial_session_flush", "session", [], ["SELECT 1"], True, 256, True),
        ("volatile_select", "select", [], "SELECT audit_side_effect()", False, 256),
        ("guc_cap", "session", ["SET application_name = 'first'", "SET application_name = 'latest'"], "SELECT 1", False, 1),
        ("guc_savepoint", "session", ["SET application_name = 'base'", "BEGIN", "SAVEPOINT s", "SET application_name = 'undone'", "ROLLBACK TO s", "COMMIT"], "SELECT 1", False, 256),
        ("guc_reset_rollback", "session", ["SET application_name = 'base'", "BEGIN", "RESET ALL", "ROLLBACK"], "SELECT 1", False, 256),
        ("none_control", "none", [], "SELECT 1", False, 256),
        ("transaction_replay_control", "transaction", ["BEGIN", "INSERT INTO audit VALUES (1)"], "SELECT 1", False, 256),
        ("extended_replay_control", "transaction", ["BEGIN", ["INSERT INTO audit VALUES (1)"]], ["SELECT 1"], False, 256),
    ]
    if args.case:
        unknown = set(args.case) - {case[0] for case in cases}
        if unknown:
            parser.error(f"unknown cases: {sorted(unknown)}")
        cases = [case for case in cases if case[0] in args.case]
    failures = 0
    reports = []
    for case in cases:
        report = scenario(args.binary, *case)
        name, _, _, fault = case[:4]
        queries, codes = report["replacement_queries"], report["sqlstates"]
        if name in ("plain_commit_unknown", "commented_commit_unknown", "extended_commit_unknown"):
            commit = fault[-1] if isinstance(fault, list) else fault
            passed = codes == ["08007"] and commit not in queries
        elif name.startswith("partial_"):
            target = fault[-1] if isinstance(fault, list) else fault
            passed = target not in queries and not report["malformed_response"]
            passed &= report["response_tags"].count("T") <= 1 and report["response_tags"].count("E") <= 1
            passed &= report["response_tags"].count("C") <= 1
            passed &= report["data_rows"] == (0 if case[4] in ("error", "fragment") else 1)
            if case[4] in ("complete", "error", "fragment") or (len(case) > 6 and case[6]):
                passed &= report["connection_closed"]
            # An incomplete trailing frame must never reach the client: it is
            # unparseable, and nothing can be written after it. The relay now
            # reassembles asynchronous frames and drops whatever is still
            # partial when the backend dies, so a "fragment" case ends with the
            # client holding only whole frames — same invariant as every other
            # case, rather than the 3 raw bytes the earlier relay forwarded.
            passed &= report["trailing_response_bytes"] == 0
        elif name == "volatile_select":
            passed = fault not in queries
        elif name == "guc_cap":
            passed = any("latest" in q for q in queries) or "08006" in codes
        elif name == "guc_savepoint":
            passed = not any("undone" in q for q in queries)
        elif name == "guc_reset_rollback":
            passed = any("base" in q for q in queries)
        elif name == "none_control":
            passed = codes == ["57P01"] and not queries
        else:
            passed = not codes and queries == ["BEGIN", "INSERT INTO audit VALUES (1)", "SELECT 1"]
        passed &= not report["response_timed_out"]
        report["invariant"] = "PASS" if passed else "FAIL"
        reports.append(report)
        failures += not passed
        print(json.dumps(report), flush=True)
    print(json.dumps({"cases": len(cases), "failures": failures}), flush=True)
    if args.output:
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"binary": args.binary, "sha256": digest.hexdigest(),
                                          "fixture": "synthetic PG-wire; no database",
                                          "cases": reports, "failures": failures}, indent=2) + "\n")
    return int(failures > 0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"harness_error": repr(error)}), flush=True)
        raise SystemExit(2)
