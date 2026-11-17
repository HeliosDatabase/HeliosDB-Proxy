#!/usr/bin/env python3
"""Bounded native 2PC admission drill using an owned PostgreSQL 18.4 fixture.

Run under the fleet lock and a 24 GiB/no-swap scope. No existing database,
cancellation, SQL PROGRAM, or backend-file operation is used.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import secrets
import socket
import subprocess
import time
import traceback


HELPER = Path(__file__).with_name("cache_prepared_commit.py")
spec = importlib.util.spec_from_file_location("owned_2pc", HELPER)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)


def checked_query(client, sql):
    return h.query(client, sql)


def error_code(reply):
    for tag, body in reply:
        if tag == b"E":
            for field in body.split(b"\0"):
                if field.startswith(b"C"):
                    return field[1:].decode()
    return None


def run(args):
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    digest = h.binary_digest(binary)
    name = "proxy-tenant-2pc-" + secrets.token_hex(12)
    pg, px, admin = h.port(), h.port(), h.port()
    owned = proxy = None
    cases = []
    error = trace = backend = None
    clients = []
    cleanup = {}
    log = (out / "proxy.log").open("w")
    try:
        owned = subprocess.check_output([
            "docker", "run", "-d", "--rm", "--pull=never", "--name", name,
            "--label", "proxy-cache-2pc-owned=" + name, "--network", "host",
            "--memory", "1g", "--memory-swap", "1g", "--cpus", "2", "--pids-limit", "128",
            "--tmpfs", "/var/lib/postgresql:rw,size=512m", "-e", "POSTGRES_USER=audit",
            "-e", "POSTGRES_DB=audit", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
            "-e", "PGPORT=" + str(pg), "postgres:18.4-bookworm", "postgres",
            "-c", "listen_addresses=127.0.0.1", "-c", f"port={pg}",
            "-c", "cluster_name=" + name, "-c", "max_prepared_transactions=16",
            "-c", "shared_buffers=64MB", "-c", "lock_timeout=10000",
            "-c", "statement_timeout=20000",
        ], text=True, timeout=60).strip()
        deadline = time.monotonic() + 60
        while True:
            try:
                direct = h.connect(pg)
                version = checked_query(direct, "SHOW server_version")[0]
                break
            except (OSError, EOFError, RuntimeError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.2)
        clients.append(direct)
        assert version.startswith("18.4"), version
        time.sleep(2)
        backend = h.backend_identity(owned, name, pg)
        cfg = out / "proxy.toml"
        cfg.write_text(f'''listen_address="127.0.0.1:{px}"
admin_address="127.0.0.1:{admin}"
tr_enabled=false
optimize_unnamed_parse=true
[[nodes]]
host="127.0.0.1"
port={pg}
role="primary"
[multi_tenancy]
enabled=true
identify_by="user"
tenants=["audit"]
max_active_queries_per_tenant=1
active_query_wait_ms=0
''')
        proxy = subprocess.Popen([str(binary), "--config", str(cfg)], stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while not h.owns_listener(proxy.pid, px):
            if proxy.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("owned proxy did not become ready")
            time.sleep(.1)
        assert h.binary_digest(Path(f"/proc/{proxy.pid}/exe")) == digest
        a, b, control = h.connect(px), h.connect(px), h.connect(px)
        clients.extend([a, b, control])
        a_pid = int(checked_query(a, "SELECT pg_backend_pid()")[0])
        checked_query(direct, "CREATE TABLE tenant_2pc_ledger(id integer PRIMARY KEY, v integer NOT NULL)")
        checked_query(direct, "INSERT INTO tenant_2pc_ledger VALUES (1,0)")
        for command, packing in [("COMMIT", "one-write"), ("ROLLBACK", "one-write"),
                                 ("COMMIT", "frames"), ("ROLLBACK", "bytes")]:
            assert h.backend_identity(owned, name, pg) == backend
            assert proxy.poll() is None and h.owns_listener(proxy.pid, px)
            checked_query(direct, "UPDATE tenant_2pc_ledger SET v=0")
            gid = "tenant2pc_" + secrets.token_hex(8)
            checked_query(direct, "BEGIN")
            checked_query(direct, "UPDATE tenant_2pc_ledger SET v=10 WHERE id=1")
            checked_query(direct, f"PREPARE TRANSACTION '{gid}'")
            checked_query(a, "BEGIN")
            # This transaction owns the sole permit and waits for the exact
            # prepared transaction that B must complete to make progress.
            a.sendall(h.frame(b"Q", b"UPDATE tenant_2pc_ledger SET v=v+1 WHERE id=1\0"))
            deadline = time.monotonic() + 3
            while checked_query(direct, f"SELECT wait_event_type FROM pg_stat_activity WHERE pid={a_pid}") != ["Lock"]:
                if time.monotonic() > deadline:
                    raise RuntimeError("permit holder did not wait for prepared lock")
                time.sleep(.02)
            control.sendall(h.frame(b"Q", b"SELECT 1\0"))
            refused = h.drain(control)
            assert error_code(refused) == "53300", "held tenant cap was not enforced"
            assert any(tag == b"E" and b"too many active queries for tenant" in body for tag, body in refused)
            sql = f"{command} PREPARED '{gid}'"
            frames = [h.frame(b"P", b"\0" + sql.encode() + b"\0\0\0"),
                      h.frame(b"B", bytes(8)), h.frame(b"E", bytes(5)), h.frame(b"S")]
            if packing == "one-write":
                b.sendall(b"".join(frames))
            elif packing == "frames":
                for frame in frames:
                    b.sendall(frame)
                    time.sleep(.02)
            else:
                for byte in b"".join(frames):
                    b.sendall(bytes([byte]))
                    time.sleep(.002)
            reply = h.drain(b)
            code = error_code(reply)
            expected = None if args.expect == "fixed" else "53300"
            assert code == expected, (command, packing, code, expected, reply)
            if args.expect == "fixed":
                assert b"".join(tag for tag, _ in reply) == b"12CZ", reply
                assert [body for tag, body in reply if tag == b"C"] == [(command + " PREPARED").encode() + b"\0"]
            else:
                assert any(tag == b"E" and b"too many active queries for tenant" in body for tag, body in reply)
                # Owner cleanup releases A without any cancellation request.
                checked_query(direct, f"ROLLBACK PREPARED '{gid}'")
            a_reply = h.drain(a)
            assert error_code(a_reply) is None and a_reply[-1] == (b"Z", b"T"), a_reply
            checked_query(a, "COMMIT")
            expected_value = "11" if args.expect == "fixed" and command == "COMMIT" else "1"
            assert checked_query(direct, "SELECT v FROM tenant_2pc_ledger WHERE id=1") == [expected_value]
            assert checked_query(direct, f"SELECT count(*) FROM pg_prepared_xacts WHERE gid='{gid}'") == ["0"]
            checked_query(control, "SELECT 1")
            assert h.backend_identity(owned, name, pg) == backend
            cases.append({"command": command, "packing": packing, "permit_holder_waited": "Lock",
                          "completion_sqlstate": code, "value": expected_value, "pass": True})
            print(json.dumps(cases[-1]), flush=True)
    except Exception as exc:
        error, trace = repr(exc), traceback.format_exc()
    finally:
        for client in clients:
            try:
                client.close()
            except OSError:
                pass
        if proxy is not None:
            cleanup["proxy"] = h.stop_proxy(proxy)
        cleanup["backend"] = h.stop_container(owned, name)
        log.close()
    clean = cleanup.get("backend", {}).get("removed") and not cleanup.get("backend", {}).get("errors")
    clean = clean and cleanup.get("proxy", {}).get("reaped") and not cleanup.get("proxy", {}).get("errors")
    result = {"binary": str(binary), "binary_sha256": digest, "helper_sha256": hashlib.sha256(HELPER.read_bytes()).hexdigest(),
              "expected": args.expect, "backend": backend, "cases": cases, "error": error, "trace": trace,
              "cleanup": cleanup, "pass_expected_state": error is None and len(cases) == 4 and bool(clean)}
    (out / "RESULT.json").write_text(json.dumps(result, indent=2) + "\n")
    return 0 if result["pass_expected_state"] else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("binary", type=Path)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--expect", choices=["baseline", "fixed"], required=True)
    raise SystemExit(run(ap.parse_args()))
