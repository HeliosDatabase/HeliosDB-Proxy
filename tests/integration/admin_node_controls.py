#!/usr/bin/env python3
"""Admin node controls reach routing (Sprinter b251750f1d54): `POST /api/chaos
force_unhealthy` and `POST /nodes/{addr}/disable` take a backend out of routing
within the request, survive the health sweeps, and `restore` / `/enable` /
`reset` give it back.

Usage: python3 tests/integration/admin_node_controls.py PROXY [--output evidence.json]
                                                        [--static-only]
Disposable fake PG-wire backends (scripts/regress/tr-boundary-test.py) and a fake
Patroni REST API in this process; no Docker, no existing database, no third-party
packages. The patroni case needs a proxy built with postgres-topology.

Cases (static topology: two configured primaries a and b, one standby s):
  force        force a: /nodes says healthy=false with the override in the same
               request, /topology names b, a write lands on b and never on a; still
               so after three health sweeps; GET /api/chaos lists it; restore gives
               a back within a sweep and the next write lands on a.
  fail_closed  force a and disable b: currentPrimary is null, a write fails with
               08006 inside write_timeout_secs and reaches neither; enable b resumes.
  session      a session with an open connection to a: after force a its next write
               is not served by a.
  reset        disable s and force a; reset clears the chaos fault only, s stays
               disabled; enable clears s.
  patroni      provider = patroni, a is the leader: force a makes writes wait and
               fail closed (08006) without reaching a; restore resumes.
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
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve()


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


wire = load("boundary", HERE.parents[2] / "scripts/regress/tr-boundary-test.py")
topo = load("topology", HERE.parent / "topology_failover.py")


def http(admin, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{admin}{path}", data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"null")


def node(admin, addr):
    _, nodes = http(admin, "GET", "/nodes")
    for entry in nodes:
        if entry["address"] == addr:
            return entry
    raise AssertionError(f"{addr} not in /nodes: {nodes!r}")


def primary(admin):
    return http(admin, "GET", "/topology")[1].get("currentPrimary")


def wait_primary(admin, expected, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if primary(admin) == expected:
            return True
        time.sleep(0.1)
    return primary(admin) == expected


class Session:
    """One client connection kept open across writes."""

    def __init__(self, port):
        self.client = socket.create_connection(("127.0.0.1", port), timeout=7)
        params = struct.pack("!I", 196608) + b"user\0audit\0database\0audit\0\0"
        self.client.sendall(struct.pack("!I", len(params) + 4) + params)
        messages, closed, trailing, malformed, timed_out = wire.fault_response(self.client)
        assert not timed_out and not malformed and trailing == 0 and not closed, "startup failed"
        assert not any(tag == b"E" for tag, _ in messages), "startup refused"

    def write(self, sql):
        self.client.sendall(wire.query_wire(sql))
        messages, closed, trailing, malformed, timed_out = wire.fault_response(self.client)
        errors = [body.decode(errors="replace") for tag, body in messages if tag == b"E"]
        return {"success": any(tag == b"C" for tag, _ in messages) and not errors,
                "errors": errors, "closed": closed, "timed_out": timed_out}

    def close(self):
        self.client.close()


def config_text(port, admin, nodes, provider=None, authority=None):
    text = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = false
tr_mode = "none"
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
[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
'''
    if provider:
        text += f'''[topology]
provider = "{provider}"
poll_interval_secs = 1
lease_timeout_secs = 2
patroni_endpoints = ["http://127.0.0.1:{authority.server.server_port}"]
'''
    for backend, role in nodes:
        text += f'''[[nodes]]
host = "127.0.0.1"
port = {backend.port}
role = "{role}"
'''
    return text


class Proxy:
    def __init__(self, binary, directory, text):
        self.path = directory / "proxy.toml"
        self.path.write_text(text)
        self.log = (directory / "proxy.log").open("w+")
        self.process = subprocess.Popen([binary, "--config", str(self.path)],
                                        stdout=self.log, stderr=self.log)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            self.process.wait(timeout=5)
        self.log.close()


def addr(backend):
    return f"127.0.0.1:{backend.port}"


def check(report, name, ok, detail):
    report[name] = {"ok": bool(ok), **detail}
    print(f"  {'PASS' if ok else 'FAIL'} {name}: {detail}")


def static_cases(binary, report):
    a, b, s = wire.Backend(), wire.Backend(), wire.Backend()
    proxy = None
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-nodectl-") as directory:
            directory = pathlib.Path(directory)
            port, admin = wire.free_port(), wire.free_port()
            proxy = Proxy(binary, directory, config_text(
                port, admin, [(a, "primary"), (b, "primary"), (s, "standby")]))
            topo.wait_topology(admin, addr(a), proxy.process, proxy.log)
            time.sleep(2.2)  # two sweeps: every node has proven itself

            # ---- force
            status, body = http(admin, "POST", "/api/chaos",
                                {"action": "force_unhealthy", "target_node": addr(a)})
            n = node(admin, addr(a))
            at_once = (status == 200 and n["healthy"] is False
                       and n.get("override", {}).get("kind") == "force_unhealthy"
                       and primary(admin) == addr(b))
            sql = "INSERT INTO fixture VALUES (1)"
            w = topo.attempt_write(port, sql)
            landed = w["success"] and sql in b.queries and sql not in a.queries
            time.sleep(3.2)  # three sweeps + the admin sync
            after = node(admin, addr(a))
            held = after["healthy"] is False and primary(admin) == addr(b)
            _, overrides = http(admin, "GET", "/api/chaos")
            listed = addr(a) in overrides
            check(report, "force", at_once and landed and held and listed, {
                "status": status, "node_at_once": n, "write": w, "a_queries": list(a.queries),
                "b_queries": list(b.queries), "node_after_3s": after, "chaos": overrides})

            status, _ = http(admin, "POST", "/api/chaos", {"action": "restore", "target_node": addr(a)})
            back = wait_primary(admin, addr(a))
            sql = "INSERT INTO fixture VALUES (2)"
            w = topo.attempt_write(port, sql)
            check(report, "restore", status == 200 and back and w["success"] and sql in a.queries, {
                "status": status, "primary": primary(admin), "write": w,
                "node": node(admin, addr(a))})

            # ---- fail_closed
            http(admin, "POST", "/api/chaos", {"action": "force_unhealthy", "target_node": addr(a)})
            status, _ = http(admin, "POST", f"/nodes/{addr(b)}/disable")
            none = primary(admin) is None and node(admin, addr(b)).get("override", {}).get("kind") == "disabled"
            sql = "INSERT INTO fixture VALUES (3)"
            started = time.monotonic()
            w = topo.attempt_write(port, sql)
            elapsed = time.monotonic() - started
            refused = (not w["success"] and any("08006" in e for e in w["errors"])
                       and sql not in a.queries and sql not in b.queries and elapsed < 6)
            status_enable, _ = http(admin, "POST", f"/nodes/{addr(b)}/enable")
            resumed = wait_primary(admin, addr(b))
            sql = "INSERT INTO fixture VALUES (4)"
            w2 = topo.attempt_write(port, sql)
            check(report, "fail_closed",
                  status == 200 and none and refused and status_enable == 200 and resumed
                  and w2["success"] and sql in b.queries,
                  {"primary_after_disable": None if none else primary(admin), "write": w,
                   "elapsed_s": round(elapsed, 2), "write_after_enable": w2})
            http(admin, "POST", "/api/chaos", {"action": "restore", "target_node": addr(a)})
            wait_primary(admin, addr(a))

            # ---- session
            session = Session(port)
            sql = "INSERT INTO fixture VALUES (5)"
            first = session.write(sql)
            on_a = first["success"] and sql in a.queries
            http(admin, "POST", "/api/chaos", {"action": "force_unhealthy", "target_node": addr(a)})
            sql = "INSERT INTO fixture VALUES (6)"
            second = session.write(sql)
            not_a = sql not in a.queries
            session.close()
            check(report, "session", on_a and not_a, {
                "first": first, "second": second, "served_by_b": sql in b.queries})
            http(admin, "POST", "/api/chaos", {"action": "restore", "target_node": addr(a)})
            wait_primary(admin, addr(a))

            # ---- reset
            http(admin, "POST", f"/nodes/{addr(s)}/disable")
            http(admin, "POST", "/api/chaos", {"action": "force_unhealthy", "target_node": addr(a)})
            status, body = http(admin, "POST", "/api/chaos", {"action": "reset"})
            _, overrides = http(admin, "GET", "/api/chaos")
            s_node = node(admin, addr(s))
            kept = s_node.get("override", {}).get("kind") == "disabled" and s_node["healthy"] is False
            a_back = wait_primary(admin, addr(a))
            status_enable, _ = http(admin, "POST", f"/nodes/{addr(s)}/enable")
            time.sleep(1.5)
            s_after = node(admin, addr(s))
            check(report, "reset",
                  status == 200 and body.get("restored") == [addr(a)] and overrides == {}
                  and kept and a_back and status_enable == 200 and "override" not in s_after
                  and s_after["healthy"] is True,
                  {"reset": body, "chaos_after": overrides, "standby_after_reset": s_node,
                   "standby_after_enable": s_after})

            status, body = http(admin, "POST", "/nodes/127.0.0.1:1/disable")
            check(report, "unknown_404", status == 404, {"status": status, "body": body})
    finally:
        if proxy:
            proxy.close()
        for backend in (a, b, s):
            backend.close()


def patroni_case(binary, report):
    a, b = wire.Backend(), wire.Backend()
    authority = topo.Authority()
    authority.leaders(a)
    proxy = None
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-nodectl-") as directory:
            directory = pathlib.Path(directory)
            port, admin = wire.free_port(), wire.free_port()
            proxy = Proxy(binary, directory, config_text(
                port, admin, [(a, "primary"), (b, "standby")], "patroni", authority))
            topo.wait_topology(admin, addr(a), proxy.process, proxy.log)
            time.sleep(2.2)
            status, _ = http(admin, "POST", "/api/chaos",
                             {"action": "force_unhealthy", "target_node": addr(a)})
            dropped = primary(admin) is None
            sql = "INSERT INTO fixture VALUES (7)"
            started = time.monotonic()
            w = topo.attempt_write(port, sql)
            elapsed = time.monotonic() - started
            refused = (not w["success"] and any("08006" in e for e in w["errors"])
                       and sql not in a.queries and sql not in b.queries)
            http(admin, "POST", "/api/chaos", {"action": "restore", "target_node": addr(a)})
            back = wait_primary(admin, addr(a))
            sql = "INSERT INTO fixture VALUES (8)"
            w2 = topo.attempt_write(port, sql)
            check(report, "patroni", status == 200 and dropped and refused and back
                  and w2["success"] and sql in a.queries,
                  {"leader_after_force": None if dropped else primary(admin), "write": w,
                   "elapsed_s": round(elapsed, 2), "write_after_restore": w2})
    finally:
        if proxy:
            proxy.close()
        authority.close()
        a.close()
        b.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("--output")
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args()
    digest = hashlib.sha256(pathlib.Path(args.binary).read_bytes()).hexdigest()
    report = {"binary": args.binary, "sha256": digest, "cases": {}}
    print(f"admin node controls: {args.binary} ({digest[:12]})")
    static_cases(args.binary, report["cases"])
    if not args.static_only:
        patroni_case(args.binary, report["cases"])
    failed = [name for name, case in report["cases"].items() if not case["ok"]]
    report["failed"] = failed
    if args.output:
        pathlib.Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    print("FAILED: " + ", ".join(failed) if failed else "all cases passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
