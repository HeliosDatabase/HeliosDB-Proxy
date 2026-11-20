#!/usr/bin/env python3
"""Compare cached reads with native PostgreSQL under distinct session identities.

Uses an owned, disposable loopback PG container and stdlib PG-wire clients. Never
connects to the shared benchmark backend. Exit zero means the requested expected
state (vulnerable baseline, or fixed candidate) was actually observed.
"""
import argparse
import hashlib
import os
import secrets
import json
import socket
import struct
import subprocess
import time
import urllib.request
from pathlib import Path


class Client:
    def __init__(self, port, **params):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        values = {"user": "bench", "database": "benchdb", "application_name": "pre3-c06"}
        values.update(params)
        body = struct.pack("!I", 196608)
        for key, value in values.items():
            body += key.encode() + b"\0" + value.encode() + b"\0"
        body += b"\0"
        self.sock.sendall(struct.pack("!I", len(body) + 4) + body)
        self.response()

    def read(self, count):
        out = b""
        while len(out) < count:
            chunk = self.sock.recv(count - len(out))
            if not chunk:
                raise RuntimeError("unexpected PostgreSQL EOF")
            out += chunk
        return out

    def response(self):
        rows = []
        errors = []
        while True:
            tag = self.read(1)
            length = struct.unpack("!I", self.read(4))[0]
            if not 4 <= length <= 16 * 1024 * 1024:
                raise RuntimeError("invalid fixture frame length")
            body = self.read(length - 4)
            if tag == b"R" and body != b"\0\0\0\0":
                raise RuntimeError("fixture must use loopback trust auth")
            if tag == b"E":
                errors.append(body.decode(errors="replace"))
            if tag == b"D":
                fields = struct.unpack("!H", body[:2])[0]
                offset = 2
                row = []
                for _ in range(fields):
                    size = struct.unpack("!i", body[offset:offset + 4])[0]
                    offset += 4
                    row.append(None if size == -1 else body[offset:offset + size].decode())
                    if size >= 0:
                        offset += size
                rows.append(row)
            if tag == b"Z":
                if errors:
                    raise RuntimeError("PostgreSQL error: " + repr(errors))
                return rows

    def query(self, sql):
        body = sql.encode() + b"\0"
        self.sock.sendall(b"Q" + struct.pack("!I", len(body) + 4) + body)
        return self.response()

    def extended(self, sql):
        def frame(tag, body):
            return tag + struct.pack("!I", len(body) + 4) + body
        self.sock.sendall(
            frame(b"P", b"\0" + sql.encode() + b"\0\0\0")
            + frame(b"B", b"\0\0" + bytes(6))
            + frame(b"E", b"\0" + bytes(4)) + frame(b"S", b""))
        return self.response()

    def close(self):
        self.sock.close()


def allocate_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def owns_listener(pid, port):
    inodes = set()
    for fd in Path("/proc/" + str(pid) + "/fd").iterdir():
        try:
            link = os.readlink(fd)
        except FileNotFoundError:
            continue
        if link.startswith("socket:["):
            inodes.add(link[8:-1])
    address = "0100007F:" + format(port, "04X")
    for line in Path("/proc/net/tcp").read_text().splitlines()[1:]:
        columns = line.split()
        if columns[1] == address and columns[3] == "0A" and columns[9] in inodes:
            return True
    return False


def ready(port, proc=None):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError("proxy exited before readiness")
        try:
            if proc is not None and not owns_listener(proc.pid, port):
                time.sleep(.1)
                continue
            client = Client(port)
            client.query("SELECT 1")
            client.close()
            return
        except (OSError, RuntimeError):
            time.sleep(0.2)
    raise RuntimeError("fixture did not become ready")


def run(args):
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    pg_port, proxy_port, admin_port = allocate_ports(3)
    name = "proxy-pre3-c06-" + secrets.token_hex(12)
    initial_digest = hashlib.sha256(Path(args.proxy_binary).read_bytes()).hexdigest()
    if subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0:
        raise RuntimeError("refusing to reuse existing fixture container")
    container = None
    proxy = None
    clients = []
    cases = []
    logfile = output.with_suffix(".proxy.log").open("w")
    try:
        container = subprocess.check_output([
            "docker", "run", "-d", "--rm", "--pull=never", "--name", name,
            "--label", "proxy-c06-owned=" + name,
            "--network", "host", "--memory", "2g", "--memory-swap", "2g", "--cpus", "2", "--pids-limit", "256",
            "--tmpfs", "/var/lib/postgresql:rw,size=1g",
            "-e", "POSTGRES_USER=bench", "-e", "POSTGRES_PASSWORD=benchpass",
            "-e", "POSTGRES_DB=benchdb", "-e", "PGPORT=" + str(pg_port),
            "-e", "POSTGRES_INITDB_ARGS=--auth-host=trust --auth-local=trust",
            "postgres:18.4-bookworm", "-c", "port=" + str(pg_port),
            "-c", "listen_addresses=127.0.0.1", "-c", "cluster_name=" + name,
        ], text=True).strip()
        ready(pg_port)
        setup = Client(pg_port)
        clients.append(setup)
        assert setup.query("SHOW cluster_name") == [[name]], "endpoint is not our owned fixture; refusing writes"
        inspected = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[0]
        assert inspected["Config"]["Labels"].get("proxy-c06-owned") == name and inspected["State"]["Running"]
        setup.query("""
            CREATE ROLE c06_a; CREATE ROLE c06_b;
            CREATE TABLE c06_roles(id INT, owner TEXT);
            INSERT INTO c06_roles VALUES(1,'c06_a'),(2,'c06_b');
            ALTER TABLE c06_roles ENABLE ROW LEVEL SECURITY;
            CREATE POLICY by_role ON c06_roles USING (owner = current_user);
            GRANT SELECT ON c06_roles TO c06_a,c06_b;
            CREATE TABLE c06_guc(id INT, tenant TEXT);
            INSERT INTO c06_guc VALUES(1,'alpha'),(2,'beta');
            ALTER TABLE c06_guc ENABLE ROW LEVEL SECURITY;
            CREATE POLICY by_setting ON c06_guc USING (tenant = current_setting('app.tenant_id',true));
            GRANT SELECT ON c06_guc TO c06_a;
            CREATE SCHEMA c06_s1; CREATE SCHEMA c06_s2;
            CREATE TABLE c06_s1.kv(val TEXT); CREATE TABLE c06_s2.kv(val TEXT);
            INSERT INTO c06_s1.kv VALUES('schema-one');
            INSERT INTO c06_s2.kv VALUES('schema-two');
            CREATE FUNCTION public.c06_mutator() RETURNS integer LANGUAGE plpgsql VOLATILE
              AS $$ BEGIN PERFORM set_config('search_path','c06_s2',false); RETURN 1; END; $$;

        """)
        config = output.with_suffix(".toml")
        config.write_text(f'''strict_config = true
listen_address = "127.0.0.1:{proxy_port}"
admin_address = "127.0.0.1:{admin_port}"
tr_mode = "none"
[[nodes]]
host = "127.0.0.1"
port = {pg_port}
role = "primary"
[cache]
enabled = true
ttl_secs = 300
extended_protocol = "on"
''')
        proxy = subprocess.Popen([str(Path(args.proxy_binary).resolve()), "--config", str(config),
                                  "--log-level", "warn"], stdout=logfile, stderr=logfile)
        ready(proxy_port, proxy)

        def compare(label, sql, before_a=(), before_b=(), params_a=None, params_b=None, extended_settings=False):
            assert proxy.poll() is None and owns_listener(proxy.pid, proxy_port), "owned proxy listener changed"
            assert setup.query("SHOW cluster_name") == [[name]], "owned backend identity changed"
            a = Client(proxy_port, **(params_a or {}))
            b = Client(proxy_port, **(params_b or {}))
            reference = Client(pg_port, **(params_b or {}))
            clients.extend([a, b, reference])
            for statement in before_a:
                (a.extended if extended_settings else a.query)(statement)
            for statement in before_b:
                (b.extended if extended_settings else b.query)(statement)
                (reference.extended if extended_settings else reference.query)(statement)
            expected = reference.query(sql)
            first = a.query(sql)
            observed = b.query(sql)
            uncached = b.query(sql + " /*helios:cache=skip*/")
            if uncached != expected:
                raise RuntimeError(label + ": uncached proxy differs from native reference")
            # Validate the fixture really exercised distinct results.
            if first == expected:
                raise RuntimeError(label + ": fixture identities did not change results")
            cases.append({"name": label, "expected_native": expected, "first_identity": first,
                          "observed_cached": observed, "uncached_proxy": uncached,
                          "matches_native": observed == expected})

        compare("role-and-RLS", "SELECT id FROM c06_roles ORDER BY id",
                ["SET ROLE c06_a"], ["SET ROLE c06_b"])
        compare("tenant-GUC", "SELECT id FROM c06_guc ORDER BY id",
                ["SET ROLE c06_a", "SET app.tenant_id='alpha'"],
                ["SET ROLE c06_a", "SET app.tenant_id='beta'"])
        compare("runtime-search-path", "SELECT val FROM kv",
                ["SET search_path=c06_s1"], ["SET search_path=c06_s2"])
        # A different query text keeps this case independent of the preceding fill.
        compare("startup-options", "SELECT val FROM kv ORDER BY val",
                params_a={"options": "-c search_path=c06_s1"},
                params_b={"options": "-c search_path=c06_s2"})
        compare("extended-role-and-RLS", "SELECT id FROM c06_roles WHERE id > 0 ORDER BY id",
                ["SET ROLE c06_a"], ["SET ROLE c06_b"], extended_settings=True)
        compare("opaque-function-context-change", "SELECT val FROM kv WHERE length(val) > 0",
                before_b=["SELECT public.c06_mutator()"],
                params_a={"options": "-c search_path=c06_s1"},
                params_b={"options": "-c search_path=c06_s1"})
        a = Client(proxy_port, options="-c search_path=c06_s1")
        reference = Client(pg_port, options="-c search_path=c06_s2")
        clients.extend([a, reference])
        sql = "SELECT val FROM kv WHERE val <> 'never'"
        first = a.query(sql)
        a.query("SET search_path=c06_s2")
        expected = reference.query(sql)
        observed = a.query(sql)
        uncached = a.query(sql + " /*helios:cache=skip*/")
        if first == expected or uncached != expected:
            raise RuntimeError("same-connection fixture/control failed")
        cases.append({"name": "same-connection-L1-after-SET", "expected_native": expected,
                      "first_identity": first, "observed_cached": observed,
                      "uncached_proxy": uncached, "matches_native": observed == expected})

        def hits():
            with urllib.request.urlopen("http://127.0.0.1:" + str(admin_port) + "/metrics", timeout=5) as response:
                counters = json.load(response)["query_cache"]["hits"]
            return sum(counters.values())
        control = Client(proxy_port)
        clients.append(control)
        # The default cache policy intentionally bypasses table reads unless
        # their effects are explicitly trusted. Use a literal scalar here so
        # this final hit-counter control proves the cache is live without
        # weakening the identity-isolation cases above.
        sql = "SELECT 1"
        control.query(sql)
        before_hits = hits()
        control.query(sql)
        after_hits = hits()
        if after_hits <= before_hits:
            raise RuntimeError("neutral read control did not produce a real cache hit")
        mismatches = sum(not case["matches_native"] for case in cases)
        final_digest = hashlib.sha256(Path(args.proxy_binary).read_bytes()).hexdigest()
        assert initial_digest == final_digest, "binary changed during the drill"
        report = {"expected_state": args.expect, "cases": cases, "mismatches": mismatches,
                  "binary_sha256": initial_digest, "final_binary_sha256": final_digest,
                  "fixture": {"container": container, "nonce": name, "pg_port": pg_port,
                              "proxy_port": proxy_port, "admin_port": admin_port, "proxy_pid": proxy.pid},
                  "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  "proxy_binary": str(Path(args.proxy_binary).resolve()),
                  "neutral_read_cache_hits_delta": after_hits - before_hits}
        output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        expected = mismatches > 0 if args.expect == "vulnerable" else mismatches == 0
        if not expected:
            raise RuntimeError("observed result did not meet requested expected state")
    finally:
        for client in clients:
            client.close()
        if proxy is not None:
            proxy.terminate()
            try:
                proxy.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proxy.kill()
                proxy.wait()
        logfile.close()
        if container:
            subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proxy-binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expect", choices=["vulnerable", "fixed"], required=True)
    run(parser.parse_args())
