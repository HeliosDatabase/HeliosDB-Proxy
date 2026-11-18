#!/usr/bin/env python3
"""The shutdown drain of the real daemon: SIGTERM and SIGINT.

Usage: python3 tests/integration/shutdown_drain.py PROXY [--output evidence.json]
Uses a disposable loopback protocol fixture as the backend: no database, no
Docker, no third-party packages. SQL is captured, not executed.

Cases (each starts its own daemon and fixture):
  sigterm        an idle session and a session inside a transaction, then SIGTERM:
                 the listener closes and /readyz answers 503 "Proxy is draining";
                 the idle session gets FATAL 57P01 at once; the transaction's next
                 statement and its COMMIT are served, then that session gets 57P01
                 too; the process exits 0 well inside the drain timeout.
  sigint         the same drain on SIGINT, with an idle session.
  deadline       shutdown_drain_timeout_secs = 1 and a transaction that never ends:
                 the process exits 0 after about one second, cutting the session.
  second_signal  a long drain timeout and a transaction that never ends: a second
                 SIGTERM ends the wait at once.
  handoff        SIGUSR2 (binary handoff) drains the same way: the idle session gets
                 57P01 at once, the open transaction finishes, then the process exits.
  gateway        the HTTP SQL gateway takes part: after SIGTERM its listener refuses
                 new connections, a request it accepted before is still answered,
                 and the process exits once it was.
Exit 1 if any case fails its expectation.
"""
import argparse
import contextlib
import hashlib
import importlib.util
import json
import pathlib
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "boundary", pathlib.Path(__file__).resolve().parents[2] / "scripts/regress/tr-boundary-test.py")
wire = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wire)


def get_json(admin, path):
    request = urllib.request.Request(f"http://127.0.0.1:{admin}{path}")
    try:
        response = urllib.request.urlopen(request, timeout=2)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.code, json.load(response)


@contextlib.contextmanager
def daemon(binary, drain_secs, extra=""):
    backend = wire.Backend()
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-shutdown-drain-") as temporary:
            directory = pathlib.Path(temporary)
            port, admin = wire.free_port(), wire.free_port()
            while admin == port:
                admin = wire.free_port()
            config = directory / "proxy.toml"
            config.write_text(f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
shutdown_drain_timeout_secs = {drain_secs}
[[nodes]]
host = "127.0.0.1"
port = {backend.port}
role = "primary"
{extra}''')
            with (directory / "proxy.log").open("w+") as log:
                process = subprocess.Popen([binary, "--config", str(config)],
                                           stdout=log, stderr=log)
                deadline = time.monotonic() + 12
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(f"proxy exited: {log.read()[-4000:]}")
                    try:
                        if get_json(admin, "/livez")[0] == 200:
                            break
                    except (OSError, urllib.error.URLError):
                        pass
                    if time.monotonic() >= deadline:
                        log.seek(0)
                        raise TimeoutError(f"proxy did not become live:\n{log.read()[-4000:]}")
                    time.sleep(0.05)
                try:
                    yield process, port, admin, backend, log
                    assert not backend.errors, backend.errors
                except Exception:
                    log.seek(0)
                    print(log.read()[-5000:], file=sys.stderr)
                    raise
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        backend.close()


def connect(port):
    client = socket.create_connection(("127.0.0.1", port), timeout=7)
    params = struct.pack("!I", 196608) + b"user\0drain\0database\0drain\0\0"
    client.sendall(struct.pack("!I", len(params) + 4) + params)
    messages = wire.drain(client)
    assert not any(tag == b"E" for tag, _ in messages), messages
    return client


def served(client, sql, status):
    messages = wire.query(client, sql)
    assert not any(tag == b"E" for tag, _ in messages), (sql, messages)
    assert messages[-1] == (b"Z", status), (sql, messages)


def fields(body):
    return {part[:1].decode(): part[1:].decode() for part in body.split(b"\0") if part}


def expect_drained(client):
    """The next frame is the drain's FATAL 57P01, then the proxy closes."""
    tag, body = wire.receive(client)
    assert tag == b"E", (tag, body)
    error = fields(body)
    assert error["S"] == "FATAL" and error["C"] == "57P01", error
    assert client.recv(1) == b"", "the proxy closes after the FATAL"
    return error


def expect_quiet(client, seconds=0.3):
    client.settimeout(seconds)
    try:
        data = client.recv(1)
    except socket.timeout:
        return
    finally:
        client.settimeout(7)
    raise AssertionError(f"unexpected bytes or close from the proxy: {data!r}")


def wait_exit(process, within):
    started = time.monotonic()
    code = process.wait(timeout=within)
    return code, round(time.monotonic() - started, 2)


def case_drain(binary, signum):
    with daemon(binary, 20) as (process, port, admin, backend, log):
        idle = connect(port)
        served(idle, "SELECT 1", b"I")
        busy = None
        if signum == signal.SIGTERM:
            busy = connect(port)
            served(busy, "BEGIN", b"T")
        assert get_json(admin, "/readyz")[0] == 200

        process.send_signal(signum)
        error = expect_drained(idle)
        report = {"idle_session": error}
        if busy is not None:
            status, ready = get_json(admin, "/readyz")
            assert status == 503 and ready["message"] == "Proxy is draining", (status, ready)
            metrics = get_json(admin, "/metrics")[1]
            assert metrics["draining"] is True and metrics["sessions_drained"] == 1, metrics
            try:
                socket.create_connection(("127.0.0.1", port), timeout=2).close()
                raise AssertionError("the listener still accepts connections")
            except ConnectionRefusedError:
                pass
            expect_quiet(busy)
            assert process.poll() is None, "the proxy exited with a transaction open"
            served(busy, "INSERT INTO fixture VALUES (1)", b"T")
            served(busy, "COMMIT", b"I")
            report["transaction_session"] = expect_drained(busy)
            report["readyz_while_draining"] = {"status": status, **ready}
        code, seconds = wait_exit(process, 5)
        assert code == 0, code
        report.update({"exit_code": code, "exit_seconds_after_last_session": seconds})
        log.seek(0)
        text = log.read()
        assert "drain complete" in text, text[-2000:]
        if busy is not None:
            assert backend.queries[-2:] == ["INSERT INTO fixture VALUES (1)", "COMMIT"], backend.queries
        return report


def case_handoff(binary):
    with daemon(binary, 20) as (process, port, admin, backend, log):
        idle = connect(port)
        served(idle, "SELECT 1", b"I")
        busy = connect(port)
        served(busy, "BEGIN", b"T")
        process.send_signal(signal.SIGUSR2)
        report = {"idle_session": expect_drained(idle)}
        expect_quiet(busy)
        assert process.poll() is None, "the handoff waits for the open transaction"
        served(busy, "COMMIT", b"I")
        report["transaction_session"] = expect_drained(busy)
        code, seconds = wait_exit(process, 5)
        assert code == 0, code
        log.seek(0)
        text = log.read()
        assert "SIGUSR2" in text and "drain complete" in text, text[-2000:]
        report.update({"exit_code": code, "exit_seconds_after_last_session": seconds})
        return report


def http_get_health(stream):
    stream.sendall(b"GET /health HTTP/1.1\r\nHost: drain\r\n\r\n")
    data = b""
    while True:
        chunk = stream.recv(4096)
        if not chunk:
            return data.decode()
        data += chunk


def case_gateway(binary):
    gateway = wire.free_port()
    extra = f'''[http_gateway]
enabled = true
listen_address = "127.0.0.1:{gateway}"
'''
    with daemon(binary, 20, extra) as (process, port, admin, backend, log):
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", gateway), timeout=2) as probe:
                    assert http_get_health(probe).startswith("HTTP/1.1 200")
                break
            except OSError:
                assert time.monotonic() < deadline, "the HTTP gateway never listened"
                time.sleep(0.05)
        held = socket.create_connection(("127.0.0.1", gateway), timeout=7)
        time.sleep(0.2)  # accepted, request not sent yet
        process.send_signal(signal.SIGTERM)
        refused_at = time.monotonic() + 5
        while True:
            try:
                socket.create_connection(("127.0.0.1", gateway), timeout=1).close()
            except ConnectionRefusedError:
                break
            assert time.monotonic() < refused_at, "the gateway listener stayed open"
            time.sleep(0.05)
        assert process.poll() is None, "the drain waits for the accepted gateway request"
        response = http_get_health(held)
        assert response.startswith("HTTP/1.1 200"), response
        code, seconds = wait_exit(process, 5)
        assert code == 0, code
        return {"held_request": response.split("\r\n", 1)[0], "exit_code": code,
                "exit_seconds_after_request": seconds}


def case_deadline(binary):
    with daemon(binary, 1) as (process, port, admin, backend, log):
        busy = connect(port)
        served(busy, "BEGIN", b"T")
        process.send_signal(signal.SIGTERM)
        code, seconds = wait_exit(process, 6)
        assert code == 0, code
        assert 0.8 <= seconds <= 4, f"exited after {seconds}s with a 1s drain timeout"
        # Cut, not drained: the transaction never ended.
        busy.settimeout(2)
        assert busy.recv(16) == b"", "the session is cut at the deadline"
        log.seek(0)
        assert "drain timeout reached" in log.read()
        return {"exit_code": code, "exit_seconds": seconds}


def case_second_signal(binary):
    with daemon(binary, 60) as (process, port, admin, backend, log):
        busy = connect(port)
        served(busy, "BEGIN", b"T")
        process.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        assert process.poll() is None, "the drain waits for the open transaction"
        process.send_signal(signal.SIGTERM)
        code, seconds = wait_exit(process, 5)
        assert code == 0, code
        log.seek(0)
        assert "second SIGTERM" in log.read()
        return {"exit_code": code, "exit_seconds_after_second_signal": seconds}


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("binary", type=lambda path: str(pathlib.Path(path).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    digest = hashlib.sha256()
    with open(args.binary, "rb") as binary_file:
        for chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
            digest.update(chunk)
    cases, failed = [], False
    for name, run in [
        ("sigterm", lambda: case_drain(args.binary, signal.SIGTERM)),
        ("sigint", lambda: case_drain(args.binary, signal.SIGINT)),
        ("deadline", lambda: case_deadline(args.binary)),
        ("second_signal", lambda: case_second_signal(args.binary)),
        ("handoff", lambda: case_handoff(args.binary)),
        ("gateway", lambda: case_gateway(args.binary)),
    ]:
        try:
            cases.append({"case": name, "passed": True, **run()})
        except Exception as error:  # noqa: BLE001 - every failure is reported
            failed = True
            cases.append({"case": name, "passed": False, "error": repr(error)})
    report = {"binary": args.binary, "sha256": digest.hexdigest(),
              "version": subprocess.check_output([args.binary, "--version"], text=True).strip(),
              "scope": "real daemon and signals with a protocol fixture; SQL is not executed",
              "cases": cases}
    result = json.dumps(report, indent=2)
    print(result)
    if args.output:
        args.output.write_text(result + "\n")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
