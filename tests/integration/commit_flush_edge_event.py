#!/usr/bin/env python3
"""Native PG proof that commit-at-Flush publishes one home edge event pre-Sync.

Uses only an owned PostgreSQL 18.4 container and the supplied proxy binary.
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
spec = importlib.util.spec_from_file_location("owned_edge_flush", HELPER)
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("binary", type=Path)
    ap.add_argument("--output", required=True, type=Path)
    args = ap.parse_args()
    out, binary = args.output.resolve(), args.binary.resolve()
    out.mkdir(parents=True, exist_ok=False)
    name = "proxy-edge-flush-" + secrets.token_hex(12)
    pg, px, admin = h.port(), h.port(), h.port()
    owned = proxy = None
    clients, cleanup, error, trace = [], {}, None, None
    log = (out / "proxy.log").open("w")
    digest = h.binary_digest(binary)
    try:
        owned = subprocess.check_output([
            "docker", "run", "-d", "--rm", "--pull=never", "--name", name,
            "--label", "proxy-cache-2pc-owned=" + name, "--network", "host",
            "--memory", "1g", "--memory-swap", "1g", "--cpus", "2",
            "--pids-limit", "128", "--tmpfs", "/var/lib/postgresql:rw,size=512m",
            "-e", "POSTGRES_USER=audit", "-e", "POSTGRES_DB=audit",
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:18.4-bookworm", "postgres",
            "-c", "listen_addresses=127.0.0.1", "-c", f"port={pg}",
            "-c", "cluster_name=" + name, "-c", "shared_buffers=64MB",
        ], text=True, timeout=60).strip()
        deadline = time.monotonic() + 60
        while True:
            try:
                direct = h.connect(pg)
                assert h.query(direct, "SHOW cluster_name") == [name]
                clients.append(direct)
                break
            except (OSError, EOFError, RuntimeError, AssertionError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(.2)
        time.sleep(2)
        backend = h.backend_identity(owned, name, pg)
        cfg = out / "proxy.toml"
        cfg.write_text(f'''listen_address="127.0.0.1:{px}"
admin_address="127.0.0.1:{admin}"
tr_enabled=false
optimize_unnamed_parse=false
[[nodes]]
host="127.0.0.1"
port={pg}
role="primary"
[edge]
enabled=true
role="home"
''')
        proxy = subprocess.Popen([str(binary), "--config", str(cfg)], stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        while not h.owns_listener(proxy.pid, px):
            if proxy.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("owned proxy did not become ready")
            time.sleep(.1)
        assert h.binary_digest(Path(f"/proc/{proxy.pid}/exe")) == digest

        import urllib.request
        req = urllib.request.Request(
            f"http://127.0.0.1:{admin}/api/edge/subscribe?edge_id=flush-test&region=local&base_url=http%3A%2F%2F127.0.0.1",
            headers={"Accept": "text/event-stream"})
        stream = urllib.request.urlopen(req, timeout=5)
        clients.append(stream)
        # urllib consumes the HTTP status; read through the initial hello.
        def sse_frame():
            frame = bytearray()
            while True:
                line = stream.readline()
                if not line:
                    raise EOFError("home SSE stream closed")
                frame.extend(line)
                if len(frame) > 8192:
                    raise ValueError("home SSE frame exceeded bound")
                if line in (b"\n", b"\r\n"):
                    return bytes(frame)

        hello = sse_frame()
        while b'"up_to_version"' not in hello:
            hello = sse_frame()

        a = h.connect(px)
        clients.append(a)
        h.query(direct, "CREATE TABLE edge_flush_ledger(id integer PRIMARY KEY, v integer NOT NULL)")
        h.query(direct, "INSERT INTO edge_flush_ledger VALUES (1,0)")
        h.query(a, "BEGIN")
        h.query(a, "UPDATE edge_flush_ledger SET v=1 WHERE id=1")
        wire = (h.frame(b"P", b"\0COMMIT\0\0\0") + h.frame(b"B", bytes(8))
                + h.frame(b"E", b"\0" + (0).to_bytes(4, "big")) + h.frame(b"H"))
        a.sendall(wire)
        replies = h.drain(a, b"C")
        assert any(tag == b"C" and body == b"COMMIT\0" for tag, body in replies), replies

        # Commit event must arrive on the real home SSE stream while Sync is
        # still withheld, and its table/version must cover the commit.
        event = sse_frame()
        while b'"tables"' not in event:
            event = sse_frame()
        payload = event.split(b"data: ", 1)[1].strip()
        item = json.loads(payload)
        assert "edge_flush_ledger" in item["tables"], item
        assert item["up_to_version"] > 0, item

        a.sendall(h.frame(b"S"))
        tail = h.drain(a)
        assert tail[-1][0] == b"Z" and tail[-1][1] == b"I", tail
        stream.fp.raw._sock.settimeout(.35)
        duplicate = False
        try:
            while True:
                later = sse_frame()
                if b'"tables"' in later and b"edge_flush_ledger" in later:
                    duplicate = True
                    break
        except (TimeoutError, socket.timeout):
            pass
        assert not duplicate, "Sync must not publish the commit invalidation twice"
        assert h.query(direct, "SELECT v FROM edge_flush_ledger WHERE id=1") == ["1"]
        assert h.backend_identity(owned, name, pg) == backend
        result = {"pass": True, "binary_sha256": digest, "event": item,
                  "sync_tail": [t.decode(errors="replace") for t, _ in tail],
                  "duplicate_event": duplicate, "backend": backend}
        (out / "RESULT.json").write_text(json.dumps(result, indent=2) + "\n")
    except Exception as exc:
        error, trace = repr(exc), traceback.format_exc()
    finally:
        for client in clients:
            try:
                client.close()
            except Exception:
                pass
        if proxy is not None:
            cleanup["proxy"] = h.stop_proxy(proxy)
        cleanup["backend"] = h.stop_container(owned, name)
        log.close()
    passed = error is None and cleanup.get("backend", {}).get("removed") and cleanup.get("proxy", {}).get("reaped")
    report = {"pass_expected_state": bool(passed), "error": error, "trace": trace,
              "cleanup": cleanup, "binary_sha256": digest}
    (out / "CLEANUP.json").write_text(json.dumps(report, indent=2) + "\n")
    if not passed:
        print(json.dumps(report, indent=2))
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
