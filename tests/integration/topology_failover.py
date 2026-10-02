#!/usr/bin/env python3
"""Check daemon failover routing with disposable PG-wire and Patroni fixtures.

Usage: python3 tests/integration/topology_failover.py PROXY [--output evidence.json]
Requires postgres-topology unless --static-only is set. No third-party packages,
Docker, existing databases, or persistent data. This proves routing behavior,
not actual database promotion, replication, fencing, or durability.
"""

import argparse
import hashlib
import http.server
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
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "boundary", pathlib.Path(__file__).resolve().parents[2] / "scripts/regress/tr-boundary-test.py")
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


class Authority:
    def __init__(self):
        self.members = []
        authority = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path != "/cluster":
                    self.send_error(404)
                    return
                body = json.dumps({"members": authority.members}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def leaders(self, *backends):
        self.members = [dict(host="127.0.0.1", port=b.port, role="leader",
                             state="running", timeline=index + 1)
                        for index, b in enumerate(backends)]

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


def attempt_write(port, sql):
    with socket.create_connection(("127.0.0.1", port), timeout=7) as client:
        params = struct.pack("!I", 196608) + b"user\0audit\0database\0audit\0\0"
        client.sendall(struct.pack("!I", len(params) + 4) + params)
        messages, closed, trailing, malformed, timed_out = wire.fault_response(client)
        assert not timed_out and not malformed and trailing == 0, "invalid startup response"
        if not closed and not any(tag == b"E" for tag, _ in messages):
            client.sendall(wire.query_wire(sql))
            messages, closed, trailing, malformed, timed_out = wire.fault_response(client)
        assert not timed_out and not malformed and trailing == 0, "invalid write response"
        errors = [body.decode(errors="replace") for tag, body in messages if tag == b"E"]
        return {"success": any(tag == b"C" for tag, _ in messages) and not errors,
                "errors": errors, "closed": closed,
                "tags": "".join(tag.decode() for tag, _ in messages)}


def wait_topology(admin, primary, process, log, timeout=12):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log.seek(0)
            raise RuntimeError(f"proxy exited: {log.read()[-4000:]}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{admin}/topology", timeout=1) as response:
                last = json.load(response)
            if last.get("currentPrimary") == primary:
                return last
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.05)
    raise AssertionError(f"topology never became {primary!r}: {last!r}")


def scenario(binary, provider):
    a, b = wire.Backend(), wire.Backend()
    authority = Authority()
    authority.leaders(a)
    process = None
    report = {"provider": provider}
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-topology-") as directory:
            directory = pathlib.Path(directory)
            port, admin = wire.free_port(), wire.free_port()
            config = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = false
tr_mode = "none"
write_timeout_secs = 1
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
[topology]
provider = "{provider}"
poll_interval_secs = 1
lease_timeout_secs = 2
patroni_endpoints = ["http://127.0.0.1:{authority.server.server_port}"]
[[nodes]]
host = "127.0.0.1"
port = {a.port}
role = "primary"
weight = 100
enabled = true
[[nodes]]
host = "127.0.0.1"
port = {b.port}
role = "standby"
weight = 100
enabled = true
'''
            config_path = directory / "proxy.toml"
            config_path.write_text(config)
            with (directory / "proxy.log").open("w+") as log:
                process = subprocess.Popen([binary, "--config", str(config_path)], stdout=log, stderr=log)
                report["initial_topology"] = wait_topology(admin, f"127.0.0.1:{a.port}", process, log)
                before = "INSERT INTO fixture VALUES (1)"
                report["initial_write"] = attempt_write(port, before)
                assert report["initial_write"]["success"] and before in a.queries
                assert before not in b.queries

                a.close()
                authority.leaders()
                report["no_primary_topology"] = wait_topology(admin, None, process, log)
                blocked = "INSERT INTO fixture VALUES (2)"
                report["unauthorized_write"] = attempt_write(port, blocked)
                assert not report["unauthorized_write"]["success"]
                assert report["unauthorized_write"]["errors"], "expected a protocol error"
                assert blocked not in a.queries and blocked not in b.queries

                if provider == "patroni":
                    # Simulate the HA manager's observed promotion, without
                    # changing either configured node role or the config file.
                    authority.leaders(b)
                    report["promoted_topology"] = wait_topology(admin, f"127.0.0.1:{b.port}", process, log)
                    after = "INSERT INTO fixture VALUES (3)"
                    report["promoted_write"] = attempt_write(port, after)
                    assert report["promoted_write"]["success"]
                    assert after in b.queries and after not in a.queries
                    assert config_path.read_text() == config

                    # Two healthy leader claims must not authorize either node.
                    authority.leaders(a, b)
                    report["conflict_topology"] = wait_topology(admin, None, process, log)
                    conflict = "INSERT INTO fixture VALUES (4)"
                    report["conflict_write"] = attempt_write(port, conflict)
                    assert not report["conflict_write"]["success"]
                    assert report["conflict_write"]["errors"]
                    assert conflict not in a.queries and conflict not in b.queries
                assert not a.errors and not b.errors, a.errors + b.errors
                report["backend_queries"] = {"initial": list(a.queries), "standby": list(b.queries)}
                report["passed"] = True
                return report
    finally:
        if process:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        a.close()
        b.close()
        authority.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    digest = hashlib.sha256()
    with open(args.binary, "rb") as binary_file:
        for chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
            digest.update(chunk)
    report = {"binary": args.binary,
              "sha256": digest.hexdigest(),
              "version": subprocess.check_output([args.binary, "--version"], text=True).strip(),
              "scope": "daemon routing with protocol fixtures; no database promotion or durability proof",
              "cases": [scenario(args.binary, provider) for provider in
                        (["static"] if args.static_only else ["static", "patroni"])]}
    result = json.dumps(report, indent=2)
    print(result)
    if args.output:
        args.output.write_text(result + "\n")


if __name__ == "__main__":
    main()
