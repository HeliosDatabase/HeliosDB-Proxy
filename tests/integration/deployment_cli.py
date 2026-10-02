#!/usr/bin/env python3
"""Exercise deployment fixes through real CLI, PG-wire, and admin HTTP interfaces.

Usage: python3 tests/integration/deployment_cli.py PROXY [--output evidence.json]
Requires an all-features binary for anomaly detection (or use --cli-only).
Uses disposable loopback
protocol fixtures, no database or third-party packages. SQL is captured, not
executed; transaction durability is outside this fixture's scope.
"""

import argparse
import contextlib
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "topology", pathlib.Path(__file__).with_name("topology_failover.py"))
topology = importlib.util.module_from_spec(spec)
spec.loader.exec_module(topology)
wire = topology.wire


def get_json(admin, path, token=None):
    headers = {} if token is None else {"Authorization": f"Bearer {token}"}
    request = urllib.request.Request(f"http://127.0.0.1:{admin}{path}", headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=2)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.code, json.load(response)


@contextlib.contextmanager
def daemon(binary, config_file=False, token=None, insecure=False):
    backend = wire.Backend()
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-deployment-cli-") as temporary:
            directory = pathlib.Path(temporary)
            port, admin = wire.free_port(), wire.free_port()
            while admin == port:
                admin = wire.free_port()
            command = [binary]
            if config_file:
                config = directory / "minimal.toml"
                config.write_text(f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
[[nodes]]
host = "127.0.0.1"
port = {backend.port}
role = "primary"
''')
                command += ["--config", str(config)]
            else:
                command += ["--listen", f"127.0.0.1:{port}", "--admin", f"127.0.0.1:{admin}",
                            "--primary", f"127.0.0.1:{backend.port}"]
                if token is not None:
                    command += ["--admin-token", token]
                if insecure:
                    command += ["--admin-allow-insecure"]
            with (directory / "proxy.log").open("w+") as log:
                process = subprocess.Popen(command, stdout=log, stderr=log)
                deadline = time.monotonic() + 12
                last_probe = None
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(f"proxy exited: {log.read()[-4000:]}")
                    try:
                        status, health = get_json(admin, "/livez")
                        last_probe = (status, health)
                        if status == 200:
                            break
                    except (OSError, urllib.error.URLError) as error:
                        last_probe = repr(error)
                    time.sleep(0.05)
                else:
                    log.seek(0)
                    raise TimeoutError(
                        f"proxy did not become live on {admin}; last probe: {last_probe!r}; "
                        f"log:\n{log.read()[-5000:]}")
                try:
                    yield port, admin, backend
                    assert not backend.errors, backend.errors
                except Exception:
                    log.seek(0)
                    print(log.read()[-5000:], file=sys.stderr)
                    raise
    finally:
        if process:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        backend.close()


def relay(port, backend, sql):
    response = topology.attempt_write(port, sql)
    assert response["success"], response
    assert backend.queries.count(sql) == 1, backend.queries
    return response


def stacked_events(value):
    assert isinstance(value.get("events"), list), value
    return [event for event in value["events"]
            if "stacked_queries" in event.get("patterns_matched", [])]


def scenarios(binary, cli_only=False):
    cases = []
    with daemon(binary, config_file=True) as (port, admin, backend):
        response = relay(port, backend, "INSERT INTO fixture VALUES (10)")
        cases.append({"case": "minimal_file_config", "write": response, "passed": True})

        if not cli_only:
            normal = "BEGIN; INSERT INTO test_replication (name) VALUES ('delta'); COMMIT;"
            response = relay(port, backend, normal)
            status, normal_events = get_json(admin, "/anomalies")
            assert status == 200, (status, normal_events)
            assert not stacked_events(normal_events), normal_events
            suspicious = "SELECT 1; DROP TABLE fixture;"
            alert_response = relay(port, backend, suspicious)
            deadline = time.monotonic() + 2
            while True:
                status, suspicious_events = get_json(admin, "/anomalies")
                assert status == 200, (status, suspicious_events)
                matched = stacked_events(suspicious_events)
                if matched or time.monotonic() >= deadline:
                    break
                time.sleep(0.05)
            assert any(event.get("sql_excerpt") == suspicious for event in matched), suspicious_events
            cases.append({"case": "transaction_anomaly_interface", "transaction_response": response,
                          "normal_stacked_events": stacked_events(normal_events),
                          "suspicious_response": alert_response,
                          "suspicious_stacked_events": matched, "passed": True})

    # This is a disposable test value, not a deployment credential. Never
    # include the returned config in evidence unless it passed the leak check.
    token = "deployment-fixture-token-" + str(time.monotonic_ns())
    with daemon(binary, token=token) as (port, admin, backend):
        anonymous_status, _ = get_json(admin, "/config")
        wrong_status, _ = get_json(admin, "/config", "wrong-fixture-token")
        authorized_status, config = get_json(admin, "/config", token)
        assert anonymous_status == 401 and wrong_status == 401
        assert authorized_status == 200
        assert token not in json.dumps(config), "admin response exposed its bearer token"
        response = relay(port, backend, "INSERT INTO fixture VALUES (20)")
        cases.append({"case": "admin_token_cli", "anonymous_status": anonymous_status,
                      "wrong_token_status": wrong_status, "authorized_status": authorized_status,
                      "token_absent_from_config": True, "write": response, "passed": True})

    for insecure in (False, True):
        with daemon(binary, insecure=insecure) as (port, admin, backend):
            status, _ = get_json(admin, "/config")
            assert status == 200
            response = relay(port, backend, "INSERT INTO fixture VALUES (30)")
            cases.append({"case": "explicit_insecure_cli_loopback" if insecure else "default_cli_loopback",
                          "anonymous_config_status": status, "write": response, "passed": True})
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda path: str(pathlib.Path(path).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--cli-only", action="store_true", help="Skip the anomaly feature case")
    args = parser.parse_args()
    digest = hashlib.sha256()
    with open(args.binary, "rb") as binary_file:
        for chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
            digest.update(chunk)
    report = {"binary": args.binary, "sha256": digest.hexdigest(),
              "version": subprocess.check_output([args.binary, "--version"], text=True).strip(),
              "scope": "real daemon interfaces with protocol fixtures; SQL is not executed",
              "cli_only": args.cli_only,
              "cases": scenarios(args.binary, cli_only=args.cli_only)}
    result = json.dumps(report, indent=2)
    print(result)
    if args.output:
        args.output.write_text(result + "\n")


if __name__ == "__main__":
    main()
