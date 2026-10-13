#!/usr/bin/env python3
"""Read offload to standbys ([load_balancer] standby_reads, Sprinter 6b0487365969).

Starts and removes its own bounded PostgreSQL Docker container (host network,
listening on 127.0.0.1 and 127.0.0.2) and its own proxies; uses no existing
database and no third-party Python packages. Each proxy sees the one server as
two nodes, 127.0.0.1 (pg-primary) and 127.0.0.2 (pg-standby), so
`host(inet_server_addr())` tells which node ran a statement. host,
inet_server_addr and current_setting are listed in tr_read_functions so the
probe reads count as side-effect-free.
Usage: python3 tests/integration/read_routing.py PROXY [--output evidence.json]
Requires the already available postgres:18.4-bookworm image (never pulls images).

Cases (each on a new client connection unless noted):
  async          default settings: a plain read runs on the standby; right after a
                 write the session reads from the primary (read_your_writes_ms),
                 later from the standby again; reads inside BEGIN/COMMIT and
                 SELECT ... FOR UPDATE run on the primary; a session SET is
                 mirrored onto the standby; extended-protocol reads (unnamed,
                 named, re-executed by Bind alone) run on the standby and a
                 prepared write re-executed by Bind alone on the primary; after
                 CREATE TEMP TABLE the session reads from the primary.
  ryw_off        read_your_writes_ms = 0: a read right after a write uses the standby.
  primary        standby_reads = "primary": reads stay on the primary.
  sync_none      standby_reads = "sync", standby not marked synchronous: primary.
  sync           standby_reads = "sync", [[nodes]] synchronous = true: standby.
  split_off      read_write_split = false: primary.
Exit 1 if any case fails its expectation.
"""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import struct
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("tr08", HERE / "tr_prepared_replay.py")
tr08 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tr08)
spec = importlib.util.spec_from_file_location("f2", HERE / "prepared_close_routing.py")
f2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f2)
wire = tr08.wire
frame = tr08.frame
SYNC = tr08.SYNC
PRIMARY, STANDBY = f2.PRIMARY, f2.STANDBY
WHERE = "SELECT host(inet_server_addr())"
RYW_WAIT = 1.3  # past the default read_your_writes_ms (1000)


def query(client, sql):
    return tr08.send(client, frame(b"Q", sql.encode() + b"\0"))


def where(client, sql=WHERE):
    r = query(client, sql)
    errs = tr08.errors(r)
    return tr08.rows(r)[0][0] if not errs else f"error {errs}"


def extended(client, name, sql, parse=True):
    msgs = (tr08.parse(name, sql) if parse else b"") + tr08.bind("", name) + tr08.execute("") + SYNC
    r = tr08.send(client, msgs)
    errs = tr08.errors(r)
    return tr08.rows(r)[0][0] if tr08.rows(r) else (f"error {errs}" if errs else "no rows")


def case_async(address):
    r = {}
    with tr08.startup(address) as c:
        r["plain_read"] = where(c)
        query(c, "INSERT INTO rr_t VALUES (1)")
        r["read_after_write"] = where(c)
        time.sleep(RYW_WAIT)
        r["read_later"] = where(c)
        query(c, "BEGIN")
        r["read_in_tx"] = where(c)
        query(c, "COMMIT")
        time.sleep(RYW_WAIT)
        r["for_update"] = where(c, "SELECT host(inet_server_addr()) FROM rr_t LIMIT 1 FOR UPDATE")
        query(c, "SET application_name = 'rr-mirrored'")
        time.sleep(RYW_WAIT)
        res = tr08.rows(query(c, "SELECT host(inet_server_addr()), current_setting('application_name')"))
        r["set_mirrored"] = res[0] if res else None
        r["ext_unnamed"] = extended(c, "", WHERE)
        r["ext_named"] = extended(c, "rr_w", WHERE)
        r["ext_named_bind_only"] = extended(c, "rr_w", WHERE, parse=False)
        extended(c, "rr_ins", "INSERT INTO rr_t VALUES (2) RETURNING host(inet_server_addr())")
        time.sleep(RYW_WAIT)
        r["ext_write_bind_only"] = extended(c, "rr_ins", "", parse=False)
        time.sleep(RYW_WAIT)
        query(c, "CREATE TEMP TABLE rr_tmp (a int)")
        time.sleep(RYW_WAIT)
        r["read_after_temp_table"] = where(c)
    r["ok"] = (r["plain_read"] == STANDBY and r["read_after_write"] == PRIMARY
               and r["read_later"] == STANDBY and r["read_in_tx"] == PRIMARY
               and r["for_update"] == PRIMARY and r["set_mirrored"] == [STANDBY, "rr-mirrored"]
               and r["ext_unnamed"] == STANDBY and r["ext_named"] == STANDBY
               and r["ext_named_bind_only"] == STANDBY and r["ext_write_bind_only"] == PRIMARY
               and r["read_after_temp_table"] == PRIMARY)
    return r


def case_ryw_off(address):
    r = {}
    with tr08.startup(address) as c:
        query(c, "INSERT INTO rr_t VALUES (3)")
        r["read_after_write"] = where(c)
    r["ok"] = r["read_after_write"] == STANDBY
    return r


def case_expect(expected):
    def run(address):
        with tr08.startup(address) as c:
            r = {"read": where(c)}
        r["ok"] = r["read"] == expected
        return r
    return run


def start_proxy(binary, temp, pg_port, load_balancer, standby_extra=""):
    port, admin = wire.free_port(), wire.free_port()
    cfg = f'''listen_address = "127.0.0.1:{port}"
admin_address = "127.0.0.1:{admin}"
tr_enabled = false
tr_mode = "none"
tr_read_functions = ["host", "inet_server_addr", "current_setting"]
write_timeout_secs = 15
[pool_mode]
mode = "session"
[load_balancer]
{load_balancer}
[health]
check_interval_secs = 1
check_timeout_secs = 1
failure_threshold = 1
success_threshold = 1
[[nodes]]
host = "{PRIMARY}"
port = {pg_port}
role = "primary"
name = "pg-primary"
[[nodes]]
host = "{STANDBY}"
port = {pg_port}
role = "standby"
name = "pg-standby"
{standby_extra}
'''
    (temp / "proxy.toml").write_text(cfg)
    log = (temp / "proxy.log").open("w+")
    proc = subprocess.Popen([binary, "--config", str(temp / "proxy.toml")], stdout=log, stderr=log)
    address = ("127.0.0.1", port)
    deadline = time.monotonic() + 20
    while True:
        try:
            with tr08.startup(address) as probe:
                query(probe, "SELECT 1")
                return proc, log, address
        except (OSError, RuntimeError, EOFError):
            if proc.poll() is not None or time.monotonic() >= deadline:
                log.seek(0)
                raise RuntimeError("proxy did not start: " + log.read()[-3000:])
            time.sleep(0.2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    name = "proxy-reads-" + uuid.uuid4().hex[:12]
    tr08.PG_ADDRESS = (PRIMARY, wire.free_port())
    f2.tr08.PG_ADDRESS = tr08.PG_ADDRESS  # f2 loaded its own copy of the helpers
    reports = []
    cases = [
        ("async", "", "", case_async),
        ("ryw_off", "read_your_writes_ms = 0", "", case_ryw_off),
        ("primary", 'standby_reads = "primary"', "", case_expect(PRIMARY)),
        ("sync_none", 'standby_reads = "sync"', "", case_expect(PRIMARY)),
        ("sync", 'standby_reads = "sync"', "synchronous = true", case_expect(STANDBY)),
        ("split_off", "read_write_split = false", "", case_expect(PRIMARY)),
    ]
    try:
        version = f2.start_postgres(name, tr08.PG_ADDRESS[1])
        tr08.pg_query("CREATE TABLE rr_t (a int)")
        for case, lb, standby_extra, run in cases:
            report = {"case": case}
            with tempfile.TemporaryDirectory(prefix="proxy-reads-") as temp:
                temp = pathlib.Path(temp)
                proc, log, address = start_proxy(args.binary, temp, tr08.PG_ADDRESS[1], lb, standby_extra)
                try:
                    time.sleep(2.5)  # both nodes pass their health check
                    try:
                        report.update(run(address))
                    except Exception as e:  # report and continue with the next case
                        report.update({"ok": False, "exception": repr(e)})
                finally:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
                    log.close()
            report["invariant"] = "PASS" if report.pop("ok") else "FAIL"
            reports.append(report)
            print(json.dumps(report), flush=True)
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "one disposable PostgreSQL as primary 127.0.0.1 + standby 127.0.0.2",
                    "postgres_version": version, "binary": args.binary,
                    "sha256": digest.hexdigest(), "cases": reports, "failures": failures}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return int(failures > 0)
    finally:
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=20)


if __name__ == "__main__":
    raise SystemExit(main())
