#!/usr/bin/env python3
"""TR-09: a read whose response is held until ReadyForQuery survives the loss
of its backend after PostgreSQL already flushed part of the rows.

Starts and removes its own bounded, loopback-only PostgreSQL Docker container
and its own proxy (tr_mode = "select"); no third-party Python packages.
Usage: python3 tests/integration/tr_small_read_hold.py PROXY [--output evidence.json]
Requires the already available postgres:18.4-bookworm image (never pulls images).

The read returns 200 rows of 1000 bytes and sleeps at row 100 (pg_sleep is
listed in tr_read_functions, so the proxy may re-execute it); PostgreSQL has
flushed about 100 KB by then. The test terminates the backend during the sleep.
  held     [limits] tr_buffer_small_reads_bytes = 1048576: the client receives
           all 200 rows, no error, on the same connection.
  streamed tr_buffer_small_reads_bytes = 0 (control): rows were already shown,
           so the proxy closes the session instead of appending a second result.
Exit 1 if either case differs.
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
import threading
import time
import uuid

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("tr08", HERE / "tr_prepared_replay.py")
tr08 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tr08)
wire = tr08.wire
frame = wire.frame

def read_response(client, limit=1000):
    """Complete frames until ReadyForQuery or close (the shared fault reader
    stops at 100 frames; this read returns 200 rows). Returns (messages,
    closed, trailing_bytes, malformed, timed_out) like wire.fault_response."""
    messages, pending = [], b""
    while len(messages) < limit:
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
            chunk = client.recv(65536)
        except ConnectionResetError:
            return messages, True, len(pending), False, False
        except socket.timeout:
            return messages, False, len(pending), False, True
        if not chunk:
            return messages, True, len(pending), False, False
        pending += chunk
    raise ValueError("response exceeded the frame limit")


READ = ("SELECT g, repeat('x', 1000), CASE WHEN g = 100 THEN pg_sleep(4) END "
        "FROM generate_series(1, 200) AS g")


def run_case(binary, hold):
    report = {"case": "held" if hold else "streamed", "hold_bytes": hold}
    with tempfile.TemporaryDirectory(prefix="proxy-tr09-") as temp:
        temp = pathlib.Path(temp)
        extra = 'tr_read_functions = ["pg_sleep"]\n'
        proc, log, client = tr08.start_proxy(binary, temp, tr08.PG_ADDRESS[1], mode="select",
                                             top=extra, limits=f"tr_buffer_small_reads_bytes = {hold}\n")
        try:
            pid = tr08.rows(tr08.send(client, frame(b"Q", b"SELECT pg_backend_pid()\0")))[0][0]
            killer = threading.Thread(target=lambda: (time.sleep(1.5), tr08.pg_query(
                f"SELECT pg_terminate_backend({int(pid)})")))
            killer.start()
            client.settimeout(30)
            client.sendall(frame(b"Q", READ.encode() + b"\0"))
            messages, closed, trailing, malformed, timed_out = read_response(client)
            killer.join()
            report.update({
                "data_rows": sum(tag == b"D" for tag, _ in messages),
                "sqlstates": tr08.errors(messages),
                "ready": any(tag == b"Z" for tag, _ in messages),
                "connection_closed": closed, "trailing_bytes": trailing,
                "malformed": malformed, "timed_out": timed_out,
            })
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
    if hold:
        ok = (report["data_rows"] == 200 and report["sqlstates"] == [] and report["ready"]
              and not report["connection_closed"])
    else:
        # Rows were shown: no second result may follow, so the client gets
        # an error after them or the connection closes.
        ok = (0 < report["data_rows"] < 200
              and (report["connection_closed"] or report["sqlstates"] != []))
    ok &= not report["malformed"] and not report["timed_out"] and report["trailing_bytes"] == 0
    report["invariant"] = "PASS" if ok else "FAIL"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    name = "proxy-tr09-" + uuid.uuid4().hex[:12]
    tr08.PG_ADDRESS = ("127.0.0.1", wire.free_port())
    reports = []
    try:
        version = tr08.start_postgres(name)
        for hold in (1048576, 0):
            report = run_case(args.binary, hold)
            reports.append(report)
            print(json.dumps(report), flush=True)
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "disposable PostgreSQL; pg_terminate_backend mid-result",
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
