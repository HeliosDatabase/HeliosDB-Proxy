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
  ext_set        a SET sent with the extended protocol (Parse/Bind/Execute/Sync, as JDBC
                 sends it) and a SET behind a sqlcommenter tag are mirrored onto the
                 standby (a tagged read offloads too); so is an extended SET inside a
                 transaction once a simple COMMIT ends it.
  ext_set_tx     transactions in the extended protocol, one session each (75c0c5eceac3):
                 JDBC's BEGIN + SET cycle then an extended COMMIT; a simple BEGIN and SET
                 ended by an extended COMMIT (mixed protocol); an extended ROLLBACK drops
                 the SET; an extended ROLLBACK TO SAVEPOINT undoes the later SET; pgjdbc
                 autosave (a SAVEPOINT in front of each statement); a SET LOCAL batch
                 (changes no session setting); two SETs in one cycle; a simple ROLLBACK TO
                 with a comment inside it. Each reads from the standby with exactly the
                 committed setting. A cycle where a procedure commits before an error
                 (CALL that COMMITs, then 1/0) makes the session's settings unknown: its
                 reads stay on the primary and heliosdb_proxy_tr_session_set_untracked_total
                 counts it.
  role           SET ROLE is mirrored onto the standby, RESET ALL keeps it (as in
                 PostgreSQL), RESET ROLE drops it there too; a SERIALIZABLE default
                 isolation level keeps reads on the primary.
  scram_login    pass-through auth and a login the standby wants a password for (a
                 scram-sha-256 pg_hba row for user rr_pw, connecting with psql from the
                 container): its reads fall back to the primary without an error, a later
                 session of that login skips the standby (one fallback counted in
                 heliosdb_proxy_read_mirror_failures_total), and a trusted login still
                 reads from the standby.
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
import urllib.request
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
RR_PW_PASSWORD = "rr-pw-secret"
METRICS_SYNC = 1.5  # the admin API's copy of the counters syncs once a second


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


def case_async(address, _admin):
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


def case_ryw_off(address, _admin):
    r = {}
    with tr08.startup(address) as c:
        query(c, "INSERT INTO rr_t VALUES (3)")
        r["read_after_write"] = where(c)
    r["ok"] = r["read_after_write"] == STANDBY
    return r


def case_expect(expected):
    def run(address, _admin):
        with tr08.startup(address) as c:
            r = {"read": where(c)}
        r["ok"] = r["read"] == expected
        return r
    return run


def cycle(client, *sqls):
    """One extended-protocol cycle (Sync to Sync) running each statement in turn on the
    unnamed statement and portal."""
    msgs = b"".join(tr08.parse("", sql) + tr08.bind("", "") + tr08.execute("") for sql in sqls)
    errs = tr08.errors(tr08.send(client, msgs + SYNC))
    return f"error {errs}" if errs else "ok"


def case_ext_set(address, _admin):
    probe = "SELECT host(inet_server_addr()), current_setting('application_name')"
    r = {}
    with tr08.startup(address) as c:
        r["ext_set"] = extended(c, "", "SET application_name = 'rr-ext'")
        time.sleep(RYW_WAIT)
        res = tr08.rows(query(c, probe))
        r["ext_set_mirrored"] = res[0] if res else None
        query(c, "/* app=rr */ SET application_name = 'rr-tagged'")
        time.sleep(RYW_WAIT)
        res = tr08.rows(query(c, "/* app=rr */ " + probe))
        r["tagged_set_mirrored"] = res[0] if res else None
        query(c, "BEGIN")
        r["ext_set_in_tx"] = extended(c, "", "SET application_name = 'rr-in-tx'")
        query(c, "COMMIT")
        time.sleep(RYW_WAIT)
        res = tr08.rows(query(c, probe))
        r["read_after_tx_set"] = res[0] if res else None
    r["ok"] = (r["ext_set"] == "no rows" and r["ext_set_mirrored"] == [STANDBY, "rr-ext"]
               and r["tagged_set_mirrored"] == [STANDBY, "rr-tagged"]
               and r["read_after_tx_set"] == [STANDBY, "rr-in-tx"])
    return r


def case_ext_set_tx(address, admin):
    probe = "SELECT host(inet_server_addr()), current_setting('application_name')"
    app = "SET application_name = '{}'".format
    scenarios = {
        # JDBC with autocommit off: BEGIN in front of the first statement.
        "jdbc_begin_set": ([["BEGIN", app("rr-jdbc")], ["COMMIT"]], "rr-jdbc"),
        # A driver mixing the protocols: simple BEGIN and SET, extended COMMIT.
        "mixed_commit": (["BEGIN", app("rr-mixed"), ["COMMIT"]], "rr-mixed"),
        "ext_rollback": ([["BEGIN"], [app("rr-rolled-back")], ["ROLLBACK"]], ""),
        "ext_savepoint": ([["BEGIN"], [app("rr-kept")], ["SAVEPOINT s"], [app("rr-undone")],
                           ["ROLLBACK TO SAVEPOINT s"], ["COMMIT"]], "rr-kept"),
        "jdbc_autosave": ([["BEGIN", "SAVEPOINT PGJDBC_AUTOSAVE", app("rr-autosave")],
                           ["SAVEPOINT PGJDBC_AUTOSAVE", "SELECT 1"], ["COMMIT"]], "rr-autosave"),
        "set_local_batch": ([["BEGIN", "SET LOCAL application_name = 'rr-local'", "SELECT 1"],
                             ["COMMIT"]], ""),
        "two_sets_one_cycle": ([[app("rr-a"), app("rr-b")]], "rr-b"),
        "comment_in_rollback_to": (["BEGIN", "SAVEPOINT s", app("rr-comment-undone"),
                                    "ROLLBACK /* undo */ TO s", "COMMIT"], ""),
    }
    r = {}
    for name, (steps, expected) in scenarios.items():
        with tr08.startup(address) as c:
            query(c, "SET application_name = ''")
            time.sleep(RYW_WAIT)
            # A list is one extended cycle, a string one simple query.
            done = [cycle(c, *step) if isinstance(step, list) else
                    ("ok" if not tr08.errors(query(c, step)) else "error") for step in steps]
            time.sleep(RYW_WAIT)
            res = tr08.rows(query(c, probe))
            r[name] = {"steps": done, "read": res[0] if res else None}
            r[name]["ok"] = (all(d == "ok" for d in done)
                             and r[name]["read"] == [STANDBY, expected])
    time.sleep(METRICS_SYNC)
    r["untracked_before"] = metric(admin, "heliosdb_proxy_tr_session_set_untracked_total")
    with tr08.startup(address) as c:
        query(c, "SET application_name = ''")
        # The procedure's COMMIT keeps the SET; the error ends the cycle.
        r["call_cycle"] = cycle(c, app("rr-call"), "CALL rr_commit()", "SELECT 1/0")
        time.sleep(RYW_WAIT)
        res = tr08.rows(query(c, probe))
        r["read_after_call"] = res[0] if res else None
    time.sleep(METRICS_SYNC)
    r["untracked_after"] = metric(admin, "heliosdb_proxy_tr_session_set_untracked_total")
    r["ok"] = (all(r[name].pop("ok") for name in scenarios)
               and r["call_cycle"].startswith("error")
               and (r["read_after_call"] or [None])[0] == PRIMARY
               and r["untracked_before"] == 0 and r["untracked_after"] == 1)
    return r


def case_role(address, _admin):
    probe = "SELECT host(inet_server_addr()), current_user"
    r = {}
    with tr08.startup(address) as c:
        for step, sql in [("after_set_role", "SET ROLE rr_tenant"),
                          ("after_reset_all", "RESET ALL"),
                          ("after_reset_role", "RESET ROLE")]:
            query(c, sql)
            time.sleep(RYW_WAIT)
            res = tr08.rows(query(c, probe))
            r[step] = res[0] if res else None
        query(c, "SET default_transaction_isolation = 'serializable'")
        time.sleep(RYW_WAIT)
        r["serializable_read"] = where(c)
    r["ok"] = (r["after_set_role"] == [STANDBY, "rr_tenant"]
               and r["after_reset_all"] == [STANDBY, "rr_tenant"]
               and r["after_reset_role"] == [STANDBY, "audit"]
               and r["serializable_read"] == PRIMARY)
    return r


def psql_rr_pw(name, address, *sqls):
    """Run `sqls` in one psql session (SCRAM login rr_pw) through the proxy."""
    cmd = ["docker", "exec", "--env", f"PGPASSWORD={RR_PW_PASSWORD}", name, "psql", "-X", "-At",
           "-v", "ON_ERROR_STOP=1",
           f"host={address[0]} port={address[1]} user=rr_pw dbname=audit sslmode=disable"]
    for sql in sqls:
        cmd += ["-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return r.stdout.split() if r.returncode == 0 else [f"error {r.stderr.strip()[-300:]}"]


def metric(admin, name):
    """A counter from /metrics/prometheus (the exposition text is in its JSON `text` field)."""
    with urllib.request.urlopen(f"http://127.0.0.1:{admin}/metrics/prometheus", timeout=5) as resp:
        text = json.loads(resp.read())["text"]
    for line in text.splitlines():
        if line.startswith(name + " "):
            return float(line.split()[1])
    return None


def case_scram_login(name):
    def run(address, admin):
        r = {"first_session": psql_rr_pw(name, address, WHERE, WHERE),
             "second_session": psql_rr_pw(name, address, WHERE)}
        time.sleep(METRICS_SYNC)
        r["fallbacks"] = metric(admin, "heliosdb_proxy_read_mirror_failures_total")
        with tr08.startup(address) as c:
            r["trusted_login"] = where(c)
        r["ok"] = (r["first_session"] == [PRIMARY, PRIMARY] and r["second_session"] == [PRIMARY]
                   and r["fallbacks"] == 1 and r["trusted_login"] == STANDBY)
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
                return proc, log, address, admin
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
        ("ext_set", "", "", case_ext_set),
        ("ext_set_tx", "", "", case_ext_set_tx),
        ("role", "", "", case_role),
        ("scram_login", "", "", case_scram_login(name)),
    ]
    try:
        version = f2.start_postgres(name, tr08.PG_ADDRESS[1])
        tr08.pg_query("CREATE TABLE rr_t (a int)")
        tr08.pg_query(f"CREATE ROLE rr_pw LOGIN PASSWORD '{RR_PW_PASSWORD}'")
        tr08.pg_query("CREATE ROLE rr_tenant")
        tr08.pg_query("CREATE PROCEDURE rr_commit() LANGUAGE plpgsql AS $$BEGIN COMMIT; END$$")
        subprocess.run(["docker", "exec", name, "sh", "-c",
                        'sed -i "1i host all rr_pw all scram-sha-256" "$PGDATA/pg_hba.conf"'],
                       check=True, capture_output=True, timeout=20)
        tr08.pg_query("SELECT pg_reload_conf()")
        for case, lb, standby_extra, run in cases:
            report = {"case": case}
            with tempfile.TemporaryDirectory(prefix="proxy-reads-") as temp:
                temp = pathlib.Path(temp)
                proc, log, address, admin = start_proxy(args.binary, temp, tr08.PG_ADDRESS[1], lb,
                                                        standby_extra)
                try:
                    time.sleep(2.5)  # both nodes pass their health check
                    try:
                        report.update(run(address, admin))
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
