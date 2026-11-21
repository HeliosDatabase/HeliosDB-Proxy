#!/usr/bin/env python3
"""Legacy FunctionCall and extended-protocol cache drill on an owned PostgreSQL 18.4.

Run under the fleet lock and a 24 GiB/no-swap scope. No existing database is
used: the drill starts its own disposable PostgreSQL container (tmpfs data,
loopback only) and its own proxy, and removes both.

Usage: fastpath_extended_cache.py PROXY_BINARY --output NEW_DIRECTORY

Checks, each through the proxy with raw protocol frames:

- the large-object fastpath functions (lo_creat, lo_open, lowrite, lo_lseek,
  loread, lo_close, lo_unlink) work as FunctionCall messages, inside and
  outside a transaction, and what they wrote is what the backend holds;
- any other function OID is answered 0A000 with ReadyForQuery and the session
  goes on, under `function_call = "large-objects"`; `"any"` forwards it and
  `"refuse"` refuses the large-object functions too;
- a malformed FunctionCall ends the session with 08P01;
- an extended-protocol read is answered from the query cache byte for byte,
  per parameter value and result format, for an unnamed and for a named
  statement, is refilled after a write through the proxy, and leaves the
  unnamed statement usable by a later Bind without a Parse.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import secrets
import struct
import subprocess
import time
import traceback
import urllib.request


HELPER = Path(__file__).with_name("cache_prepared_commit.py")
spec = importlib.util.spec_from_file_location("owned_backend", HELPER)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)

LO_CREAT, LO_OPEN, LO_CLOSE, LOREAD, LOWRITE, LO_LSEEK, LO_UNLINK = 957, 952, 953, 954, 955, 956, 964
PG_ADVISORY_LOCK, PG_ADVISORY_UNLOCK_ALL = 2880, 2892
INV_READ_WRITE = 0x00020000 | 0x00040000


def int4(value):
    return struct.pack("!i", value)


def function_call(oid, *args):
    """A FunctionCall of `oid` with binary arguments and a binary result."""
    body = struct.pack("!IHHH", oid, 1, 1, len(args))
    for arg in args:
        body += struct.pack("!I", len(arg)) + arg
    return h.frame(b"F", body + struct.pack("!H", 1))


def call(client, oid, *args):
    """Run one FunctionCall; its result bytes (None for NULL) and status."""
    client.sendall(function_call(oid, *args))
    reply = h.drain(client)
    errors = [body for tag, body in reply if tag == b"E"]
    if errors:
        raise RuntimeError(("FunctionCall failed", oid, errors))
    results = [body for tag, body in reply if tag == b"V"]
    assert len(results) == 1 and reply[-1][0] == b"Z", reply
    (length,) = struct.unpack("!i", results[0][:4])
    return (None if length < 0 else results[0][4:4 + length]), reply[-1][1]


def sqlstate(reply):
    for tag, body in reply:
        if tag == b"E":
            for field in body.split(b"\0"):
                if field.startswith(b"C"):
                    return field[1:].decode()
    return None


def parse(name, sql):
    return h.frame(b"P", name.encode() + b"\0" + sql.encode() + b"\0\0\0")


def bind(statement, value, result_format):
    body = b"\0" + statement.encode() + b"\0" + struct.pack("!HH", 0, 1)
    body += struct.pack("!I", len(value)) + value + struct.pack("!HH", 1, result_format)
    return h.frame(b"B", body)


EXECUTE = h.frame(b"E", b"\0" + struct.pack("!I", 0))
SYNC = h.frame(b"S")
DESCRIBE_PORTAL = h.frame(b"D", b"P\0")


def exchange(client, wire):
    client.sendall(wire)
    return h.drain(client)


def cache_counters(admin):
    with urllib.request.urlopen(f"http://127.0.0.1:{admin}/metrics", timeout=5) as response:
        cache = json.load(response)["query_cache"]
    return {"hits": cache["hits"]["l2"] + cache["hits"]["l3"], "misses": cache["misses"], "puts": cache["puts"]}


def start_proxy(binary, out, label, pg, extra):
    px, admin = h.port(), h.port()
    cfg = out / f"proxy-{label}.toml"
    cfg.write_text(f'''listen_address="127.0.0.1:{px}"
admin_address="127.0.0.1:{admin}"
{extra}
[[nodes]]
host="127.0.0.1"
port={pg}
role="primary"
[cache]
enabled=true
trust_backend_read_effects=true
extended_protocol="on"
''')
    log = (out / f"proxy-{label}.log").open("w")
    proxy = subprocess.Popen([str(binary), "--config", str(cfg)], stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 30
    while not h.owns_listener(proxy.pid, px):
        if proxy.poll() is not None or time.monotonic() > deadline:
            raise RuntimeError(f"owned proxy {label} did not become ready")
        time.sleep(.1)
    return proxy, log, px, admin


def large_objects(px, direct):
    """The libpq large-object sequence as raw FunctionCall messages."""
    client = h.connect(px)
    try:
        oid_bytes, status = call(client, LO_CREAT, int4(-1))
        assert status == b"I"
        (oid,) = struct.unpack("!I", oid_bytes)
        payload = secrets.token_bytes(3000)
        h.query(client, "BEGIN")
        fd_bytes, status = call(client, LO_OPEN, struct.pack("!I", oid), int4(INV_READ_WRITE))
        assert status == b"T"
        (written,) = struct.unpack("!i", call(client, LOWRITE, fd_bytes, payload)[0])
        assert written == len(payload)
        assert struct.unpack("!i", call(client, LO_LSEEK, fd_bytes, int4(0), int4(0))[0]) == (0,)
        assert call(client, LOREAD, fd_bytes, int4(len(payload)))[0] == payload
        assert struct.unpack("!i", call(client, LO_CLOSE, fd_bytes)[0]) == (0,)
        h.query(client, "COMMIT")
        stored = h.query(direct, f"SELECT encode(lo_get({oid}), 'hex')")
        assert stored == [payload.hex()], "the backend holds what the client wrote"
        # A rolled-back write leaves the object as it was.
        h.query(client, "BEGIN")
        fd_bytes, _ = call(client, LO_OPEN, struct.pack("!I", oid), int4(INV_READ_WRITE))
        call(client, LOWRITE, fd_bytes, b"overwritten")
        h.query(client, "ROLLBACK")
        assert h.query(direct, f"SELECT encode(lo_get({oid}), 'hex')") == [payload.hex()]
        assert struct.unpack("!i", call(client, LO_UNLINK, struct.pack("!I", oid))[0]) == (1,)
        assert h.query(direct, f"SELECT count(*) FROM pg_largeobject_metadata WHERE oid = {oid}") == ["0"]
        # Another function is refused, in and out of a transaction, and the
        # session goes on.
        reply = exchange(client, function_call(PG_ADVISORY_LOCK, struct.pack("!q", 7)))
        assert sqlstate(reply) == "0A000" and reply[-1] == (b"Z", b"I"), reply
        h.query(client, "BEGIN")
        reply = exchange(client, function_call(PG_ADVISORY_LOCK, struct.pack("!q", 7)))
        assert sqlstate(reply) == "0A000" and reply[-1] == (b"Z", b"T"), reply
        assert h.query(client, "SELECT 1") == ["1"]
        h.query(client, "COMMIT")
        assert h.query(direct, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'") == ["0"]
    finally:
        client.close()
    # A malformed FunctionCall ends its session.
    client = h.connect(px)
    try:
        client.sendall(h.frame(b"F", struct.pack("!I", LO_CREAT)))
        reply = [h.receive(client)]
        assert sqlstate(reply) == "08P01" and b"FATAL" in reply[0][1], reply
        assert client.recv(1) == b"", "the session is closed"
    finally:
        client.close()
    return {"case": "large objects (function_call = large-objects)", "bytes": len(payload), "pass": True}


def function_call_modes(binary, out, pg, direct):
    results = []
    for mode, lock_forwarded, lo_forwarded in [("any", True, True), ("refuse", False, False)]:
        proxy, log, px, _ = start_proxy(binary, out, mode, pg, f'function_call="{mode}"')
        client = None
        try:
            client = h.connect(px)
            reply = exchange(client, function_call(PG_ADVISORY_LOCK, struct.pack("!q", 11)))
            assert (sqlstate(reply) is None) == lock_forwarded, (mode, reply)
            held = h.query(direct, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'")
            assert held == ["1" if lock_forwarded else "0"], (mode, held)
            if lock_forwarded:
                exchange(client, function_call(PG_ADVISORY_UNLOCK_ALL))
            reply = exchange(client, function_call(LO_CREAT, int4(-1)))
            assert (sqlstate(reply) is None) == lo_forwarded, (mode, reply)
            assert reply[-1] == (b"Z", b"I"), reply
            assert h.query(client, "SELECT 1") == ["1"]
        finally:
            if client is not None:
                client.close()
            stopped = h.stop_proxy(proxy)
            log.close()
            assert stopped["reaped"] and not stopped["errors"], stopped
        results.append({"case": f"function_call = {mode}", "advisory_lock_forwarded": lock_forwarded,
                        "large_object_forwarded": lo_forwarded, "pass": True})
    return results


def extended_cache(px, admin):
    reader, writer = h.connect(px), h.connect(px)
    sql = "SELECT v FROM ext_cache_kv WHERE id = $1"
    try:
        def unnamed(value, result_format=0, describe=False):
            wire = parse("", sql) + bind("", value, result_format)
            return exchange(reader, wire + (DESCRIBE_PORTAL if describe else b"") + EXECUTE + SYNC)

        def rows(reply):
            return [body for tag, body in reply if tag == b"D"]

        before = cache_counters(admin)
        first = unnamed(b"1")
        assert b"".join(tag for tag, _ in first) == b"12DCZ", first
        assert rows(first) == [struct.pack("!HI", 1, 2) + b"10"], first
        assert unnamed(b"1") == first and unnamed(b"1") == first
        after = cache_counters(admin)
        assert after["hits"] - before["hits"] == 2 and after["puts"] - before["puts"] == 1, (before, after)
        # Another value, a binary result and a described portal are their own entries.
        assert rows(unnamed(b"2")) == [struct.pack("!HI", 1, 2) + b"20"]
        binary = unnamed(b"1", result_format=1)
        assert rows(binary) == [struct.pack("!HI", 1, 4) + int4(10)], binary
        assert unnamed(b"1", result_format=1) == binary
        described = unnamed(b"1", describe=True)
        assert b"".join(tag for tag, _ in described) == b"12TDCZ", described
        assert unnamed(b"1", describe=True) == described
        assert unnamed(b"1") == first, "the text entry is still its own"
        # A hit leaves the unnamed statement in place for a Bind without a Parse.
        hits = cache_counters(admin)["hits"]
        assert unnamed(b"2")[-1] == (b"Z", b"I")
        assert cache_counters(admin)["hits"] == hits + 1
        rebound = exchange(reader, bind("", b"3", 0) + EXECUTE + SYNC)
        assert b"".join(tag for tag, _ in rebound) == b"2DCZ", rebound
        assert rows(rebound) == [struct.pack("!HI", 1, 2) + b"30"], rebound
        # A named statement.
        prepared = exchange(reader, parse("by_id", sql) + SYNC)
        assert b"".join(tag for tag, _ in prepared) == b"1Z", prepared
        named = exchange(reader, bind("by_id", b"1", 0) + EXECUTE + SYNC)
        assert b"".join(tag for tag, _ in named) == b"2DCZ" and rows(named) == rows(first), named
        hits = cache_counters(admin)["hits"]
        assert exchange(reader, bind("by_id", b"1", 0) + EXECUTE + SYNC) == named
        assert cache_counters(admin)["hits"] == hits + 1
        # A write through the proxy retires the entries; the next read is the backend's.
        h.query(writer, "UPDATE ext_cache_kv SET v = v + 1 WHERE id = 1")
        refilled = unnamed(b"1")
        assert rows(refilled) == [struct.pack("!HI", 1, 2) + b"11"], refilled
        assert unnamed(b"1") == refilled
        assert rows(exchange(reader, bind("by_id", b"1", 0) + EXECUTE + SYNC)) == rows(refilled)
        # Inside a transaction the backend answers, and sees its own write.
        h.query(reader, "BEGIN")
        h.query(reader, "UPDATE ext_cache_kv SET v = 99 WHERE id = 2")
        assert rows(unnamed(b"2")) == [struct.pack("!HI", 1, 2) + b"99"]
        h.query(reader, "ROLLBACK")
        counters = cache_counters(admin)
    finally:
        reader.close()
        writer.close()
    return {"case": "extended-protocol cache", "counters": counters, "pass": True}


def run(args):
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    digest = h.binary_digest(binary)
    name = "proxy-fastpath-cache-" + secrets.token_hex(12)
    pg = h.port()
    owned = proxy = log = None
    cases, clients, cleanup = [], [], {}
    error = trace = backend = None
    try:
        owned = subprocess.check_output([
            "docker", "run", "-d", "--rm", "--pull=never", "--name", name,
            "--label", "proxy-cache-2pc-owned=" + name, "--network", "host",
            "--memory", "1g", "--memory-swap", "1g", "--cpus", "2", "--pids-limit", "128",
            "--tmpfs", "/var/lib/postgresql:rw,size=512m", "-e", "POSTGRES_USER=audit",
            "-e", "POSTGRES_DB=audit", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
            "-e", "PGPORT=" + str(pg), "postgres:18.4-bookworm", "postgres",
            "-c", "listen_addresses=127.0.0.1", "-c", f"port={pg}",
            "-c", "cluster_name=" + name, "-c", "shared_buffers=64MB",
            "-c", "lock_timeout=10000", "-c", "statement_timeout=20000",
        ], text=True, timeout=60).strip()
        deadline = time.monotonic() + 60
        while True:
            try:
                direct = h.connect(pg)
                version = h.query(direct, "SHOW server_version")[0]
                break
            except (OSError, EOFError, RuntimeError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.2)
        clients.append(direct)
        assert version.startswith("18.4"), version
        time.sleep(2)
        backend = h.backend_identity(owned, name, pg)
        h.query(direct, "CREATE TABLE ext_cache_kv(id integer PRIMARY KEY, v integer NOT NULL)")
        h.query(direct, "INSERT INTO ext_cache_kv VALUES (1,10),(2,20),(3,30)")
        proxy, log, px, admin = start_proxy(binary, out, "default", pg, "")
        assert h.binary_digest(Path(f"/proc/{proxy.pid}/exe")) == digest
        for case in (lambda: large_objects(px, direct), lambda: extended_cache(px, admin)):
            cases.append(case())
            print(json.dumps(cases[-1]), flush=True)
            assert h.backend_identity(owned, name, pg) == backend
            assert proxy.poll() is None and h.owns_listener(proxy.pid, px)
        cleanup["proxy"] = h.stop_proxy(proxy)
        proxy = None
        for case in function_call_modes(binary, out, pg, direct):
            cases.append(case)
            print(json.dumps(case), flush=True)
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
        if log is not None:
            log.close()
        cleanup["backend"] = h.stop_container(owned, name)
    clean = cleanup.get("backend", {}).get("removed") and not cleanup.get("backend", {}).get("errors")
    clean = clean and cleanup.get("proxy", {}).get("reaped") and not cleanup.get("proxy", {}).get("errors")
    result = {"binary": str(binary), "binary_sha256": digest,
              "helper_sha256": hashlib.sha256(HELPER.read_bytes()).hexdigest(),
              "backend": backend, "cases": cases, "error": error, "trace": trace, "cleanup": cleanup,
              "pass": error is None and len(cases) == 4 and bool(clean)}
    (out / "RESULT.json").write_text(json.dumps(result, indent=2) + "\n")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("binary", type=Path)
    ap.add_argument("--output", type=Path, required=True)
    raise SystemExit(run(ap.parse_args()))
