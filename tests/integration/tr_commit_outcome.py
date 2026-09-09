#!/usr/bin/env python3
"""Lose real PostgreSQL COMMIT responses and assert that HeliosProxy never replays.

Starts and removes its own bounded, loopback-only PostgreSQL Docker container.
Uses no existing database, host data directory, credentials, or Python packages.
Usage: python3 tests/integration/tr_commit_outcome.py PROXY [--output evidence.json]
Use --suite stream for real PostgreSQL response-publication fault probes.
Requires the already available postgres:18.4-bookworm image (never pulls images).
This tests durable transaction execution before response loss, not HA promotion.
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
import time
import uuid

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "boundary", pathlib.Path(__file__).resolve().parents[2] / "scripts/regress/tr-boundary-test.py")
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)
PG_ADDRESS = None


def connect_pg():
    conn = socket.create_connection(PG_ADDRESS, timeout=5)
    params = struct.pack("!I", 196608) + b"user\0audit\0database\0audit\0\0"
    conn.sendall(struct.pack("!I", len(params) + 4) + params)
    response = wire.drain(conn)
    if any(tag == b"E" for tag, _ in response):
        conn.close()
        raise RuntimeError(f"PG startup failed: {response!r}")
    return conn


def pg_query(sql):
    with connect_pg() as conn:
        response = wire.query(conn, sql)
        if any(tag == b"E" for tag, _ in response):
            raise RuntimeError(f"PG query failed: {response!r}")
        return [body[6:].decode() for tag, body in response if tag == b"D"]


class CommitResponseLoss(wire.Backend):
    """Forward unchanged frames; drop only after PG reports successful completion."""
    def response_prefix(self, response):
        if any(tag == b"E" for tag, _ in response):
            raise RuntimeError(f"fault target was rejected by PostgreSQL: {response!r}")
        if not any(tag == b"C" and body.startswith((b"COMMIT", b"PREPARE TRANSACTION"))
                   for tag, body in response):
            raise RuntimeError(f"fault did not follow a commit boundary: {response!r}")
        return b""

    def serve(self, conn):
        conn.settimeout(5)
        backend = None
        try:
            size = struct.unpack("!I", wire.exact(conn, 4))[0]
            startup = wire.exact(conn, size - 4)
            if startup == struct.pack("!I", 80877103):
                conn.sendall(b"N")
                return
            backend = socket.create_connection(PG_ADDRESS, timeout=5)
            backend.sendall(struct.pack("!I", size) + startup)
            for tag, body in wire.drain(backend):
                conn.sendall(wire.frame(tag, body))
            statements, portals = {}, {}
            pending = b""
            drop_response = False
            while not self.stop.is_set():
                tag, body = wire.receive(conn)
                if tag == b"X":
                    return
                sql = None
                if tag == b"P":
                    name, text, _ = body.split(b"\0", 2)
                    statements[name] = text.decode()
                elif tag == b"B":
                    portal, name, _ = body.split(b"\0", 2)
                    portals[portal] = statements.get(name)
                elif tag == b"E":
                    sql = portals.get(body.split(b"\0", 1)[0])
                elif tag == b"Q":
                    sql = body[:-1].decode()
                if sql is not None:
                    self.queries.append(sql)
                    drop_response |= (sql == self.fault_sql
                                      and self.queries.count(sql) > self.fault_after)
                pending += wire.frame(tag, body)
                if tag not in (b"Q", b"S", b"H"):
                    continue
                backend.sendall(pending)
                pending = b""
                if tag == b"H":
                    # This suite Flushes a complete Parse/Bind/Execute. PG
                    # supplies CommandComplete/ErrorResponse but no RFQ yet.
                    response = []
                    while len(response) < 100:
                        message = wire.receive(backend)
                        response.append(message)
                        if message[0] in (b"C", b"E"):
                            break
                    else:
                        raise RuntimeError("Flush fixture exceeded its response-frame bound")
                else:
                    response = wire.drain(backend)
                if drop_response:
                    prefix = self.response_prefix(response)
                    if prefix:
                        conn.sendall(prefix)
                    self.fault_triggered.set()
                    # PostgreSQL has finished COMMIT/PREPARE and sent RFQ. Lose
                    # that response, and prevent reconnection to this relay.
                    self.stop.set()
                    self.sock.close()
                    return
                for tag, body in response:
                    conn.sendall(wire.frame(tag, body))
        except (EOFError, ConnectionError, socket.timeout):
            pass
        except Exception as error:
            self.errors.append(repr(error))
        finally:
            if backend:
                backend.close()
            conn.close()


class StreamingResponseLoss(CommitResponseLoss):
    """Publish an exact PG response prefix, then close the backend route."""
    def response_prefix(self, response):
        if self.partial == "error":
            if not any(tag == b"E" for tag, _ in response):
                raise RuntimeError("PostgreSQL did not produce the expected error")
        elif any(tag == b"E" for tag, _ in response):
            raise RuntimeError(f"stream target failed: {response!r}")
        prefix = b""
        for tag, body in response:
            if tag == b"Z":
                break
            encoded = wire.frame(tag, body)
            if tag == b"D" and self.partial == "fragment":
                return prefix + encoded[:3]
            prefix += encoded
            if tag == b"D" and self.partial not in ("complete", "error"):
                return prefix
            if tag == b"E" and self.partial == "error":
                return prefix
            if tag == b"C" and self.partial == "complete":
                return prefix
        raise RuntimeError("PostgreSQL response lacked the requested publication boundary")


def main():
    global PG_ADDRESS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--suite", choices=("commit", "stream"), default="commit")
    args = parser.parse_args()
    name = "proxy-tr01-" + uuid.uuid4().hex[:12]
    reports = []
    PG_ADDRESS = ("127.0.0.1", wire.free_port())
    try:
        subprocess.run(["docker", "run", "--detach", "--rm", "--pull=never", "--name", name,
                        "--memory=768m", "--memory-swap=768m", "--cpus=1", "--pids-limit=64",
                        "--network", "host", "--tmpfs", "/var/lib/postgresql:rw,size=256m",
                        "--env", "PGDATA=/var/lib/postgresql/audit",
                        "--env", f"PGPORT={PG_ADDRESS[1]}",
                        "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--env", "POSTGRES_USER=audit",
                        "--env", "POSTGRES_DB=audit", "postgres:18.4-bookworm",
                        "-c", "max_prepared_transactions=8", "-c", "listen_addresses=127.0.0.1",
                        "-p", str(PG_ADDRESS[1])], check=True, capture_output=True, timeout=30)
        deadline = time.monotonic() + 30
        while True:
            try:
                version = pg_query("SHOW server_version")[0]
                break
            except (OSError, EOFError, RuntimeError):
                if time.monotonic() >= deadline:
                    raise TimeoutError("isolated PostgreSQL did not become ready")
                time.sleep(0.2)
        pg_query("CREATE TABLE audit_ledger (case_name text)")
        wire.Backend = CommitResponseLoss
        cases = [
            ("plain_commit", "COMMIT"),
            ("commented_commit", "/* audit /* nested */ */ COMMIT"),
            ("commented_end", "-- commit alias\nEND WORK"),
            ("multi_query_commit", "SELECT ';COMMIT'; COMMIT"),
            ("extended_commit", ["SELECT 1", "/* audit */ COMMIT"]),
            ("commit_and_begin", "COMMIT; BEGIN"),
            ("prepared_transaction", "PREPARE/* audit */TRANSACTION 'audit_prepared'"),
        ]
        for case, fault in cases if args.suite == "commit" else []:
            report = wire.scenario(args.binary, case, "transaction",
                                   ["BEGIN", f"INSERT INTO audit_ledger VALUES ('{case}')"], fault)
            if case == "prepared_transaction":
                report["prepared_outcomes"] = pg_query("SELECT gid FROM pg_prepared_xacts WHERE gid = 'audit_prepared'")
                if report["prepared_outcomes"] == ["audit_prepared"]:
                    pg_query("COMMIT PREPARED 'audit_prepared'")
            count = int(pg_query(f"SELECT count(*) FROM audit_ledger WHERE case_name = '{case}'")[0])
            report["ledger_rows"] = count
            report["invariant"] = "PASS" if (count == 1 and report["sqlstates"] == ["08007"]
                                                 and not report["replacement_queries"]) else "FAIL"
            reports.append(report)
            print(json.dumps(report), flush=True)
        if args.suite == "stream":
            wire.Backend = StreamingResponseLoss
            read = "SELECT 'first' UNION ALL SELECT 'second'"
            stream_cases = [
                ("rows", "select", [], read, True, False, False),
                ("large_slow_reader", "select", [], "SELECT repeat('x', 65536)", True, False, True),
                ("complete", "select", [], "SELECT 'first'", "complete", False, False),
                ("error", "select", [], "SELECT 1/0", "error", False, False),
                ("extended", "select", [], [read], True, False, False),
                ("unnamed", "select", [], [read], True, False, False, True),
                ("unnamed_warm", "select", [], [read], True, False, False, True, True),
                ("unnamed_fragment", "select", [], [read], "fragment", True, False, True),
                ("flush", "select", [], [read], True, True, False),
                ("fragment", "select", [], [read], "fragment", True, False),
                ("none_flush", "none", [], [read], True, True, False),
                ("session_rows", "session", [], read, True, False, False),
                ("transaction_rows", "transaction", ["BEGIN"], read, True, False, False),
            ]
            for case, mode, setup, fault, partial, flush, slow, *options in stream_cases:
                unnamed = bool(options and options[0])
                warm = bool(len(options) > 1 and options[1])
                report = wire.scenario(args.binary, case, mode, setup, fault,
                                       partial, 256, flush, slow, unnamed, warm)
                passed = (not report["replacement_queries"] and not report["malformed_response"]
                          and not report["response_timed_out"])
                passed &= report["data_rows"] == (0 if partial in ("error", "fragment") else 1)
                passed &= all(report["response_tags"].count(tag) <= 1 for tag in "TCEZ")
                # No incomplete frame may reach the client, in any case: the
                # relay reassembles asynchronous frames and drops whatever is
                # still partial when the backend dies, rather than forwarding
                # bytes the client cannot parse and nothing may follow.
                passed &= report["trailing_response_bytes"] == 0
                if flush or partial in ("complete", "error"):
                    passed &= report["connection_closed"]
                report["invariant"] = "PASS" if passed else "FAIL"
                reports.append(report)
                print(json.dumps(report), flush=True)
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "disposable PostgreSQL; controlled response publication and loss",
                    "suite": args.suite,
                    "postgres_version": version, "binary": args.binary,
                    "sha256": digest.hexdigest(),
                    "cases": reports, "failures": failures}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return int(failures > 0)
    finally:
        # The unique name belongs to this run, including a partially started container.
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=20)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        detail = repr(error)
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            detail += ": " + (error.stderr.decode() if isinstance(error.stderr, bytes) else error.stderr)
        print(json.dumps({"harness_error": detail}), flush=True)
        raise SystemExit(2)
