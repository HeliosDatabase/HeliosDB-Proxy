#!/usr/bin/env python3
"""Prove configured WASM plugins execute on the daemon's simple-query path.

Usage: python3 tests/integration/deployment_wasm.py PROXY [--output evidence.json]
Requires a wasm-plugins build. Uses temporary plugins, loopback protocol fixtures,
and Python's standard library. No wasm32 toolchain, sibling plugin artifacts,
Docker, real database, or persistent data is required. Tests both rewriting and
blocking, with a disabled-plugin control and admin invocation counters.
"""

import argparse
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import urllib.request

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location(
    "topology_fixture", pathlib.Path(__file__).with_name("topology_failover.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)
wire = fixture.wire


def leb(value, signed=False):
    """Encode a WebAssembly unsigned/signed LEB128 integer."""
    result = bytearray()
    while True:
        byte = value & 0x7f
        value >>= 7
        done = (value == 0 and (not signed or not byte & 0x40)) or (
            signed and value == -1 and byte & 0x40)
        result.append(byte if done else byte | 0x80)
        if done:
            return bytes(result)


def vector(items):
    items = list(items)
    return leb(len(items)) + b"".join(items)


def name(value):
    value = value.encode()
    return leb(len(value)) + value


def wasm_plugin(action, value):
    """A tiny real module: alloc/dealloc/memory plus constant pre_query JSON.

    Equivalent WAT (RESULT is (1024 << 32) | payload.len()):
      (module (memory (export "memory") 1)
        (func (export "alloc") (param i32) (result i32) i32.const 4096)
        (func (export "dealloc") (param i32 i32))
        (func (export "pre_query") (param i32 i32) (result i64) i64.const RESULT)
        (data (i32.const 1024) "JSON_PAYLOAD"))
    """
    payload = json.dumps({"action": action, "value": value}, separators=(",", ":")).encode()
    assert len(payload) < 1024
    types = vector([b"\x60\x01\x7f\x01\x7f",  # (i32) -> i32
                    b"\x60\x02\x7f\x7f\x00",  # (i32, i32) -> ()
                    b"\x60\x02\x7f\x7f\x01\x7e"])  # (i32, i32) -> i64
    exports = vector([name("memory") + b"\x02\x00",
                      name("alloc") + b"\x00\x00",
                      name("dealloc") + b"\x00\x01",
                      name("pre_query") + b"\x00\x02"])
    bodies = [b"\x00\x41" + leb(4096, signed=True) + b"\x0b",
              b"\x00\x0b",
              b"\x00\x42" + leb((1024 << 32) | len(payload), signed=True) + b"\x0b"]
    code = vector(leb(len(body)) + body for body in bodies)
    data = vector([b"\x00\x41" + leb(1024, signed=True) + b"\x0b"
                   + leb(len(payload)) + payload])
    sections = [(1, types), (3, b"\x03\x00\x01\x02"),
                (5, b"\x01\x00\x01"), (7, exports), (10, code), (11, data)]
    return b"\x00asm\x01\x00\x00\x00" + b"".join(
        bytes([kind]) + leb(len(content)) + content for kind, content in sections)


def plugins(admin):
    with urllib.request.urlopen(f"http://127.0.0.1:{admin}/plugins", timeout=2) as response:
        return json.load(response)


def scenario(binary, action, enabled=True):
    backend = wire.Backend()
    process = None
    report = {"action": action, "plugins_enabled": enabled}
    original = "INSERT INTO fixture VALUES (10)"
    rewritten = "INSERT INTO fixture VALUES (20)"
    reason = "deployment-wasm-fixture-blocked"
    try:
        with tempfile.TemporaryDirectory(prefix="proxy-wasm-") as directory:
            directory = pathlib.Path(directory)
            plugin_dir = directory / "plugins"
            plugin_dir.mkdir()
            wasm = wasm_plugin(action, rewritten if action == "rewrite" else reason)
            (plugin_dir / "fixture.wasm").write_bytes(wasm)
            (plugin_dir / "fixture.json").write_text(json.dumps({
                "name": "deployment-wasm-fixture", "version": "1.0.0",
                "license": "Apache-2.0", "hooks": ["pre_query"]}))
            report["plugin_sha256"] = hashlib.sha256(wasm).hexdigest()
            port, admin = wire.free_port(), wire.free_port()
            config = f'''listen_address = "127.0.0.1:{port}"
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
latency_threshold_ms = 100
[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
check_query = "SELECT 1"
[plugins]
enabled = {str(enabled).lower()}
plugin_dir = {json.dumps(str(plugin_dir))}
hot_reload = false
timeout_ms = 1000
fuel_metering = true
fuel_limit = 100000
[[nodes]]
host = "127.0.0.1"
port = {backend.port}
role = "primary"
weight = 100
enabled = true
'''
            config_path = directory / "proxy.toml"
            config_path.write_text(config)
            with (directory / "proxy.log").open("w+") as log:
                process = subprocess.Popen([binary, "--config", str(config_path)], stdout=log, stderr=log)
                try:
                    fixture.wait_topology(admin, f"127.0.0.1:{backend.port}", process, log)
                    if enabled:
                        report["plugins_before"] = plugins(admin)
                        assert len(report["plugins_before"]) == 1, report
                        plugin = report["plugins_before"][0]
                        assert plugin["name"] == "deployment-wasm-fixture", plugin
                        assert plugin["hooks"] == ["pre_query"], plugin
                        assert plugin["invocations"] == plugin["errors"] == 0, plugin
                    report["query_response"] = fixture.attempt_write(port, original)
                    if not enabled:
                        assert report["query_response"]["success"], report
                        assert original in backend.queries and rewritten not in backend.queries
                    elif action == "rewrite":
                        assert report["query_response"]["success"], report
                        assert rewritten in backend.queries and original not in backend.queries
                    else:
                        assert not report["query_response"]["success"], report
                        assert any(reason in error for error in report["query_response"]["errors"]), report
                        assert original not in backend.queries and rewritten not in backend.queries
                    if enabled:
                        report["plugins_after"] = plugins(admin)
                        plugin = report["plugins_after"][0]
                        assert plugin["invocations"] == 1 and plugin["errors"] == 0, plugin
                    assert not backend.errors, backend.errors
                    report["backend_queries"] = list(backend.queries)
                    report["passed"] = True
                    return report
                except Exception:
                    log.flush()
                    log.seek(0)
                    print(log.read()[-8000:], file=sys.stderr)
                    raise
                finally:
                    # End our daemon before TemporaryDirectory removes its files.
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
    finally:
        backend.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda value: str(pathlib.Path(value).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    digest = hashlib.sha256()
    with open(args.binary, "rb") as binary_file:
        for chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
            digest.update(chunk)
    report = {"binary": args.binary, "sha256": digest.hexdigest(),
              "version": subprocess.check_output([args.binary, "--version"], text=True).strip(),
              "scope": "native daemon WASM execution on simple-query PG-wire messages",
              "cases": [scenario(args.binary, "rewrite", enabled=False),
                        scenario(args.binary, "rewrite"), scenario(args.binary, "block")]}
    output = json.dumps(report, indent=2)
    print(output)
    if args.output:
        args.output.write_text(output + "\n")


if __name__ == "__main__":
    main()
