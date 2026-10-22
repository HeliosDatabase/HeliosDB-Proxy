#!/usr/bin/env python3
"""HeliosProxy in front of a HeliosDB-Nano physical primary + standby pair.

Usage: python3 scripts/regress/nano-pair-drill.py --nano-binary PATH --fixture PATH
          --proxy-binary PATH [--dir DIR] [--load-rate N] [--output evidence.json]
          [--fixture-arg ARG ...]

`--fixture` is Nano's scripts/regress/nano-physical-pair.sh (branch feat/physical-standby);
every `--fixture-arg` (for example `--tls`, Nano P2.2) is passed to each of its invocations.
The pair listens on 127.0.0.1:27721/27722 (replication 27741, HTTP 27751/27752); the
proxy uses scripts/regress/proxy-nano-pair.toml (27730, admin 27731) with
write_timeout_secs lowered to 5 so the fail-closed cases settle quickly. Needs only
python3 (standard library). Every Nano process is started and stopped by the fixture;
the proxy is terminated at the end.

Cases:
  startup        the config loads with no ignored key; /topology names the primary;
                 both nodes healthy; the standby's lag is measured (sequence units)
  write_read     a write lands and is read back through the proxy
  offload        reads outside the read-your-writes window are served by the standby
                 (reads_offloaded grows)
  chaos_standby  force_unhealthy on the standby empties the read set, reads keep
                 working from the primary; restore brings it back
  chaos_primary  force_unhealthy on the primary drops the leader, a write fails closed
                 (08006) inside write_timeout; restore resumes
  kill_standby   SIGKILL the standby: it leaves the read set; --restart brings it back
                 ready, with lag measured again
  load           N inserts/s on the primary: the standby stays healthy and under the
                 lag ceiling (nodes sampled every 0.5 s)
  kill_primary   SIGKILL the primary: the leader is dropped, writes fail closed; the
                 restarted primary is the leader again and the standby survived
Exit 1 if any case fails its expectation.
"""
import argparse
import hashlib
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

HERE = pathlib.Path(__file__).resolve().parent
PRIMARY = "127.0.0.1:27721"
STANDBY = "127.0.0.1:27722"
PROXY_PORT = 27730
ADMIN = 27731


# ---------------------------------------------------------------- PG-wire client
def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("backend closed")
        buf += chunk
    return buf


def recv_message(sock):
    tag = recv_exact(sock, 1)
    length = struct.unpack("!I", recv_exact(sock, 4))[0]
    return tag, recv_exact(sock, length - 4)


class Client:
    """One simple-query session through the proxy (trust auth)."""

    def __init__(self, port, user="postgres", database="postgres"):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=20)
        params = struct.pack("!I", 196608) + f"user\0{user}\0database\0{database}\0\0".encode()
        self.sock.sendall(struct.pack("!I", len(params) + 4) + params)
        while True:
            tag, body = recv_message(self.sock)
            if tag == b"E":
                raise RuntimeError(f"startup refused: {body!r}")
            if tag == b"Z":
                break

    def query(self, sql):
        payload = sql.encode() + b"\0"
        self.sock.sendall(b"Q" + struct.pack("!I", len(payload) + 4) + payload)
        rows, errors, tags = [], [], []
        while True:
            tag, body = recv_message(self.sock)
            tags.append(tag.decode())
            if tag == b"D":
                ncols = struct.unpack("!H", body[:2])[0]
                at, row = 2, []
                for _ in range(ncols):
                    size = struct.unpack("!i", body[at:at + 4])[0]
                    at += 4
                    if size < 0:
                        row.append(None)
                    else:
                        row.append(body[at:at + size].decode(errors="replace"))
                        at += size
                rows.append(row)
            elif tag == b"E":
                fields = {}
                for part in body.split(b"\0"):
                    if part:
                        fields[chr(part[0])] = part[1:].decode(errors="replace")
                errors.append(fields)
            elif tag == b"Z":
                break
        return {"rows": rows, "errors": errors, "tags": "".join(tags),
                "sqlstate": errors[0].get("C") if errors else None}

    def close(self):
        try:
            self.sock.sendall(b"X" + struct.pack("!I", 4))
        except OSError:
            pass
        self.sock.close()


def write(sql):
    client = Client(PROXY_PORT)
    try:
        return client.query(sql)
    except (ConnectionError, OSError, RuntimeError) as error:
        return {"rows": [], "errors": [{"M": str(error)}], "tags": "", "sqlstate": "connection"}
    finally:
        client.close()


# ---------------------------------------------------------------- admin API
def http(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{ADMIN}{path}", timeout=5) as r:
        return json.loads(r.read())


def post(path, body):
    request = urllib.request.Request(f"http://127.0.0.1:{ADMIN}{path}", method="POST",
                                     data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"null")


def node(addr):
    for entry in http("/nodes"):
        if entry["address"] == addr:
            return entry
    raise AssertionError(f"{addr} missing from /nodes")


def replicas():
    return sorted(r["address"] for r in http("/topology")["routing"]["replicas"])


def primary():
    return http("/topology").get("currentPrimary")


def wait_for(predicate, timeout, step=0.25):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except (OSError, urllib.error.URLError, AssertionError, KeyError):
            pass
        time.sleep(step)
    return last


def reads_offloaded():
    """`heliosdb_proxy_reads_offloaded_total` from the Prometheus text (the JSON
    `/metrics` endpoint does not carry this counter)."""
    text = http("/metrics/prometheus")["text"]
    for line in text.splitlines():
        if line.startswith("heliosdb_proxy_reads_offloaded_total "):
            return int(float(line.split()[1]))
    return 0


# ---------------------------------------------------------------- fixture
class Fixture:
    def __init__(self, script, nano_binary, directory, log, extra=()):
        self.base = ["bash", str(script), "--binary", str(nano_binary), "--dir", str(directory), *extra]
        self.log = log

    def run(self, *actions, timeout=240):
        proc = subprocess.run(self.base + list(actions), capture_output=True, text=True, timeout=timeout)
        self.log.write(f"$ nano-physical-pair.sh {' '.join(actions)} -> {proc.returncode}\n{proc.stderr}\n")
        self.log.flush()
        summary = None
        for line in reversed(proc.stdout.splitlines()):
            if line.startswith("{"):
                summary = json.loads(line)
                break
        if proc.returncode != 0:
            raise RuntimeError(f"fixture {' '.join(actions)} failed ({proc.returncode}): {proc.stderr[-800:]}")
        return summary


def check(report, name, ok, detail):
    report[name] = {"ok": bool(ok), **detail}
    print(f"  {'PASS' if ok else 'FAIL'} {name}: {json.dumps(detail, default=str)[:400]}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nano-binary", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--proxy-binary", required=True)
    parser.add_argument("--dir")
    parser.add_argument("--load-rate", type=int, default=500)
    parser.add_argument("--load-duration", type=int, default=8)
    parser.add_argument("--output")
    parser.add_argument("--fixture-arg", action="append", default=[],
                        help="extra option passed to every fixture invocation (repeatable)")
    args = parser.parse_args()

    workdir = pathlib.Path(args.dir) if args.dir else pathlib.Path(tempfile.mkdtemp(prefix="nano-pair-drill-"))
    workdir.mkdir(parents=True, exist_ok=True)
    log = (workdir / "drill.log").open("w")
    report = {"nano_binary": args.nano_binary,
              "nano_sha256": hashlib.sha256(pathlib.Path(args.nano_binary).read_bytes()).hexdigest(),
              "proxy_binary": args.proxy_binary,
              "proxy_sha256": hashlib.sha256(pathlib.Path(args.proxy_binary).read_bytes()).hexdigest(),
              "dir": str(workdir), "fixture_args": args.fixture_arg, "cases": {}}
    cases = report["cases"]
    print(f"nano pair drill: nano {report['nano_sha256'][:12]} proxy {report['proxy_sha256'][:12]} dir {workdir}")

    fixture = Fixture(args.fixture, args.nano_binary, workdir / "pair", log, args.fixture_arg)
    proxy = None
    proxy_log = None
    try:
        started = fixture.run("--start")
        report["fixture_start"] = started

        config_text = (HERE / "proxy-nano-pair.toml").read_text().replace(
            "write_timeout_secs = 30", "write_timeout_secs = 5")
        config = workdir / "proxy-nano-pair.toml"
        config.write_text(config_text)
        proxy_log = (workdir / "proxy.log").open("w+")
        proxy = subprocess.Popen([args.proxy_binary, "--config", str(config)], stdout=proxy_log, stderr=proxy_log)

        # ---- startup
        leader = wait_for(lambda: primary() == PRIMARY and primary(), 30)
        both = wait_for(lambda: node(PRIMARY)["healthy"] and node(STANDBY)["healthy"], 15)
        lag = wait_for(lambda: node(STANDBY).get("replication_lag_bytes") is not None and node(STANDBY), 15)
        ignored = http("/config").get("ignored_config_keys")
        proxy_log.flush()
        proxy_log.seek(0)
        unknown_lines = [l for l in proxy_log.read().splitlines() if "unknown config key" in l]
        check(cases, "startup", leader == PRIMARY and both and lag and ignored == [] and not unknown_lines
              and replicas() == [STANDBY],
              {"currentPrimary": leader, "standby": lag if lag else node(STANDBY), "ignored_config_keys": ignored,
               "replicas": replicas(), "fixture": started})

        # ---- write_read
        write("CREATE TABLE IF NOT EXISTS drill (id INT PRIMARY KEY, v TEXT)")
        write("DELETE FROM drill")
        inserted = write("INSERT INTO drill VALUES (1, 'one')")
        time.sleep(1.5)
        read = write("SELECT count(*) FROM drill")
        check(cases, "write_read", not inserted["errors"] and read["rows"] == [["1"]],
              {"insert": inserted, "read": read})

        # ---- offload
        before = reads_offloaded()
        time.sleep(1.5)  # outside read_your_writes_ms
        results = [write("SELECT count(*) FROM drill") for _ in range(6)]
        after = reads_offloaded()
        check(cases, "offload", after > before and all(r["rows"] == [["1"]] for r in results),
              {"reads_offloaded_before": before, "after": after,
               "standby_lag": node(STANDBY).get("replication_lag_bytes")})

        # ---- chaos_standby
        status, body = post("/api/chaos", {"action": "force_unhealthy", "target_node": STANDBY})
        forced = node(STANDBY)
        empty = replicas()
        during = reads_offloaded()
        reads = [write("SELECT count(*) FROM drill") for _ in range(4)]
        still = reads_offloaded()
        post("/api/chaos", {"action": "restore", "target_node": STANDBY})
        back = wait_for(lambda: replicas() == [STANDBY] and replicas(), 10)
        check(cases, "chaos_standby",
              status == 200 and forced["healthy"] is False and forced.get("override", {}).get("kind") == "force_unhealthy"
              and empty == [] and all(r["rows"] == [["1"]] for r in reads) and still == during and back == [STANDBY],
              {"status": status, "node_forced": forced, "replicas_forced": empty,
               "reads_offloaded_during": (during, still), "replicas_after_restore": back})

        # ---- chaos_primary
        status, body = post("/api/chaos", {"action": "force_unhealthy", "target_node": PRIMARY})
        dropped = wait_for(lambda: primary() is None and True, 5)
        t0 = time.monotonic()
        refused = write("INSERT INTO drill VALUES (2, 'two')")
        elapsed = time.monotonic() - t0
        post("/api/chaos", {"action": "restore", "target_node": PRIMARY})
        back = wait_for(lambda: primary() == PRIMARY and primary(), 15)
        resumed = write("INSERT INTO drill VALUES (3, 'three')")
        check(cases, "chaos_primary",
              status == 200 and dropped and refused["sqlstate"] == "08006" and elapsed < 12
              and back == PRIMARY and not resumed["errors"],
              {"leader_dropped": dropped, "write_while_forced": refused, "elapsed_s": round(elapsed, 2),
               "leader_after_restore": back, "write_after_restore": resumed})

        # ---- kill_standby
        fixture.run("--kill", "standby")
        gone = wait_for(lambda: node(STANDBY)["healthy"] is False and replicas() == [] and node(STANDBY), 15)
        restarted = fixture.run("--restart", "standby")
        ready = wait_for(lambda: node(STANDBY)["healthy"] and replicas() == [STANDBY]
                         and node(STANDBY).get("replication_lag_bytes") is not None and node(STANDBY), 60)
        check(cases, "kill_standby", bool(gone) and bool(ready),
              {"after_kill": gone, "fixture_restart": restarted, "after_restart": ready})

        # ---- load
        samples = []
        stop = threading.Event()

        def sampler():
            while not stop.is_set():
                try:
                    n = node(STANDBY)
                    samples.append((round(time.monotonic(), 2), n["healthy"], n.get("replication_lag_bytes"),
                                    STANDBY in replicas()))
                except (OSError, urllib.error.URLError, AssertionError):
                    samples.append((round(time.monotonic(), 2), None, None, None))
                time.sleep(0.5)

        thread = threading.Thread(target=sampler, daemon=True)
        thread.start()
        loaded = fixture.run("--load", str(args.load_rate), "--duration", str(args.load_duration),
                             timeout=args.load_duration + 120)
        time.sleep(3)
        stop.set()
        thread.join(5)
        lags = [s[2] for s in samples if s[2] is not None]
        check(cases, "load",
              bool(samples) and all(s[1] is True for s in samples) and all(s[3] for s in samples)
              and lags and max(lags) < 30000,
              {"ops_per_s": loaded.get("ops_per_s") if loaded else None, "samples": len(samples),
               "max_lag": max(lags) if lags else None, "unhealthy_samples": sum(1 for s in samples if s[1] is not True),
               "out_of_read_set_samples": sum(1 for s in samples if not s[3])})

        # ---- kill_primary
        fixture.run("--kill", "primary")
        dropped = wait_for(lambda: primary() is None and node(PRIMARY)["healthy"] is False and True, 15)
        refused = write("INSERT INTO drill VALUES (4, 'four')")
        standby_alive = node(STANDBY)
        restarted = fixture.run("--restart", "primary")
        back = wait_for(lambda: primary() == PRIMARY and node(PRIMARY)["healthy"] and primary(), 60)
        resumed = write("INSERT INTO drill VALUES (5, 'five')")
        standby_after = wait_for(lambda: node(STANDBY)["healthy"] and STANDBY in replicas() and node(STANDBY), 30)
        check(cases, "kill_primary",
              bool(dropped) and refused["sqlstate"] in ("08006", "connection") and back == PRIMARY
              and not resumed["errors"] and bool(standby_after),
              {"leader_dropped": dropped, "write_while_down": refused, "standby_while_primary_down": standby_alive,
               "fixture_restart": restarted, "leader_after_restart": back, "write_after_restart": resumed,
               "standby_after": standby_after})
    except Exception as error:  # noqa: BLE001 - the report must say what broke
        cases["exception"] = {"ok": False, "error": repr(error)}
        print(f"  FAIL exception: {error!r}")
    finally:
        if proxy is not None:
            proxy.terminate()
            try:
                proxy.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proxy.kill()
        if proxy_log is not None:
            proxy_log.close()
        try:
            report["fixture_stop"] = fixture.run("--stop")
        except Exception as error:  # noqa: BLE001
            report["fixture_stop"] = repr(error)
        log.close()

    failed = [name for name, case in cases.items() if not case["ok"]]
    report["failed"] = failed
    if args.output:
        pathlib.Path(args.output).write_text(json.dumps(report, indent=2, default=str) + "\n")
    print("FAILED: " + ", ".join(failed) if failed else "all cases passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
