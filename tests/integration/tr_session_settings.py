#!/usr/bin/env python3
"""Session settings across a transaction-replay re-home (tr_mode = "session";
Sprinter 75c0c5eceac3 and 72494e4aaebf).

Starts and removes its own bounded, loopback-only PostgreSQL Docker container
and its own proxies; uses no existing database and no third-party Python packages.
Usage: python3 tests/integration/tr_session_settings.py PROXY [--output evidence.json]
Requires the already available postgres:18.4-bookworm image (never pulls images).

Each case sets session state through the proxy, reads it back, kills the proxy's
backend with pg_terminate_backend while the session is idle, and reads it again on
the same client connection: the proxy re-homes the session and restores the
settings it tracked. The re-homed session must show exactly what the old one did.
  pool_reset     SET ROLE and search_path, then Npgsql's pool reset in one simple
                 query (SET SESSION AUTHORIZATION DEFAULT;RESET ALL;...): the
                 re-homed session must not get the previous user's role back.
  batch_set      a SET batched with a SELECT in one simple query is restored.
  ext_tx_set     an extended-protocol SET inside an extended BEGIN/COMMIT is restored.
  savepoint      BEGIN; SAVEPOINT s; SET; ROLLBACK /* undo */ TO s; COMMIT: the
                 undone SET is not restored.
  two_resets     SET ROLE, a pool reset, another tenant's SET ROLE: that role is
                 restored (SET SESSION AUTHORIZATION ends a SET ROLE, and the
                 role set after it must be restored after it).
  call_in_cycle  a SET and a CALL of a procedure that COMMITs in one extended cycle:
                 PostgreSQL refuses the COMMIT (2D000) and rolls the SET back, but the
                 proxy cannot count on every backend refusing: the SET is unknown and
                 the re-home is refused with 08006.
  multi_copy     a SET and two COPY FROM STDIN in one simple query: the proxy follows
                 both COPYs and reads the SET once the query ends; it is restored.
  unknown        set_config(..., false) changes a setting the proxy cannot record:
                 the re-home is refused with 08006 instead of continuing without it.
Exit 1 if any case fails its expectation.
"""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import uuid

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("tr08", HERE / "tr_prepared_replay.py")
tr08 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tr08)
frame = tr08.frame
SYNC = tr08.SYNC
PROBE = ("SELECT current_user, current_setting('search_path'), "
         "current_setting('application_name')")


def query(client, sql):
    """(rows, SQLSTATEs) of one simple query; SQLSTATEs ['closed'] when the
    proxy closed the connection instead of answering."""
    try:
        r = tr08.send(client, frame(b"Q", sql.encode() + b"\0"))
    except (OSError, EOFError):
        return [], ["closed"]
    return tr08.rows(r), tr08.errors(r)


def cycle(client, *sqls):
    """One extended-protocol cycle running each statement on the unnamed
    statement and portal; its SQLSTATEs."""
    msgs = b"".join(tr08.parse("", s) + tr08.bind("", "") + tr08.execute("") for s in sqls)
    return tr08.errors(tr08.send(client, msgs + SYNC))


def state(client):
    rows, errs = query(client, PROBE)
    return rows[0] if rows else errs


def pid(client):
    return query(client, "SELECT pg_backend_pid()")[0][0][0]


def kill_and_read(client):
    tr08.kill_backend(pid(client))
    return state(client)


def case_pool_reset(client):
    r = {"setup": [query(client, "SET ROLE rr_tenant")[1],
                   query(client, "SET search_path TO rr_schema, public")[1]]}
    r["before_reset"] = state(client)
    r["reset"] = query(client, "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;CLOSE ALL;"
                               "UNLISTEN *;SELECT pg_advisory_unlock_all();"
                               "DISCARD SEQUENCES;DISCARD TEMP")[1]
    r["before"] = state(client)
    r["after"] = kill_and_read(client)
    # search_path tells the states apart whatever SET SESSION AUTHORIZATION
    # does to the role.
    r["ok"] = (r["before_reset"][:2] == ["rr_tenant", "rr_schema, public"]
               and r["before"][1] != "rr_schema, public" and r["after"] == r["before"])
    return r


def case_batch_set(client):
    r = {"batch": query(client, "SET application_name = 'rr-batch'; SELECT 1")[1]}
    r["before"] = state(client)
    r["after"] = kill_and_read(client)
    r["ok"] = r["before"][2] == "rr-batch" and r["after"] == r["before"]
    return r


def case_ext_tx_set(client):
    r = {"cycles": [cycle(client, "BEGIN"),
                    cycle(client, "SET application_name = 'rr-ext-tx'"),
                    cycle(client, "COMMIT")]}
    r["before"] = state(client)
    r["after"] = kill_and_read(client)
    r["ok"] = r["before"][2] == "rr-ext-tx" and r["after"] == r["before"]
    return r


def case_savepoint(client):
    r = {"steps": [query(client, sql)[1] for sql in (
        "SET application_name = 'rr-base'", "BEGIN", "SAVEPOINT s",
        "SET application_name = 'rr-undone'", "ROLLBACK /* undo */ TO s", "COMMIT")]}
    r["before"] = state(client)
    r["after"] = kill_and_read(client)
    r["ok"] = r["before"][2] == "rr-base" and r["after"] == r["before"]
    return r


def case_two_resets(client):
    reset = "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;CLOSE ALL"
    r = {"steps": [query(client, sql)[1] for sql in (
        "SET ROLE rr_tenant", reset, "SET ROLE rr_tenant_b", reset, "SET ROLE rr_tenant")]}
    r["before"] = state(client)
    r["after"] = kill_and_read(client)
    r["ok"] = r["before"][0] == "rr_tenant" and r["after"] == r["before"]
    return r


def case_call_in_cycle(client):
    r = {"call_cycle": cycle(client, "SET application_name = 'rr-call'", "CALL rr_commit()")}
    r["before"] = state(client)
    tr08.kill_backend(pid(client))
    r["after_kill"] = query(client, "SELECT 1")[1]
    r["ok"] = (r["call_cycle"] == ["2D000"] and r["before"][2] == ""
               and r["after_kill"] in (["08006"], ["closed"]))
    return r


def case_multi_copy(client):
    sql = "SET application_name = 'rr-copy'; COPY rr_copy FROM STDIN; COPY rr_copy FROM STDIN"
    client.sendall(frame(b"Q", sql.encode() + b"\0"))
    tags = []
    for _ in range(2):
        while True:
            tag, _body = tr08.wire.receive(client)
            tags.append(tag.decode())
            if tag == b"G":
                break
            if tag in (b"Z", b"E"):
                raise RuntimeError(f"COPY not started: {tags}")
        client.sendall(frame(b"d", b"1\n") + frame(b"c"))
    r = {"frames": tags, "errors": tr08.errors(tr08.wire.drain(client))}
    r["before"] = state(client)
    r["after"] = kill_and_read(client)
    r["ok"] = (tags.count("G") == 2 and r["errors"] == [] and r["before"][2] == "rr-copy"
               and r["after"] == r["before"])
    return r


def case_unknown(client):
    r = {"set_config": query(client,
                             "SELECT set_config('application_name', 'rr-cfg', false)")[1]}
    r["before"] = state(client)
    tr08.kill_backend(pid(client))
    r["after_kill"] = query(client, "SELECT 1")[1]
    r["ok"] = (r["set_config"] == [] and r["before"][2] == "rr-cfg"
               and r["after_kill"] in (["08006"], ["closed"]))
    return r


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=lambda p: str(pathlib.Path(p).resolve()))
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    name = "proxy-trset-" + uuid.uuid4().hex[:12]
    tr08.PG_ADDRESS = ("127.0.0.1", tr08.wire.free_port())
    reports = []
    try:
        version = tr08.start_postgres(name)
        tr08.pg_query("CREATE ROLE rr_tenant")
        tr08.pg_query("CREATE ROLE rr_tenant_b")
        tr08.pg_query("CREATE SCHEMA rr_schema")
        tr08.pg_query("CREATE TABLE rr_copy (a int)")
        tr08.pg_query("CREATE PROCEDURE rr_commit() LANGUAGE plpgsql AS $$BEGIN COMMIT; END$$")
        for case, run in [("pool_reset", case_pool_reset), ("batch_set", case_batch_set),
                          ("ext_tx_set", case_ext_tx_set), ("savepoint", case_savepoint),
                          ("two_resets", case_two_resets), ("call_in_cycle", case_call_in_cycle),
                          ("multi_copy", case_multi_copy), ("unknown", case_unknown)]:
            with tempfile.TemporaryDirectory(prefix="proxy-trset-") as temp:
                temp = pathlib.Path(temp)
                proc, log, client = tr08.start_proxy(args.binary, temp, tr08.PG_ADDRESS[1],
                                                     mode="session")
                report = {"case": case}
                try:
                    report.update(run(client))
                except (AssertionError, IndexError, OSError, EOFError, RuntimeError,
                        ValueError) as error:
                    report.update({"ok": False, "error": repr(error)})
                finally:
                    client.close()
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
                    log.seek(0)
                    report["proxy_log_tail"] = [line for line in log.read().splitlines()
                                                if "helios::tr" in line][-4:]
                    log.close()
            report["invariant"] = "PASS" if report.pop("ok") else "FAIL"
            reports.append(report)
            print(json.dumps(report), flush=True)
        failures = sum(r["invariant"] != "PASS" for r in reports)
        digest = hashlib.sha256()
        with open(args.binary, "rb") as binary:
            for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                digest.update(chunk)
        evidence = {"fixture": "disposable PostgreSQL; pg_terminate_backend on an idle session",
                    "postgres_version": version, "binary": args.binary,
                    "sha256": digest.hexdigest(), "cases": reports, "failures": failures}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2) + "\n")
        return int(failures > 0)
    finally:
        # The unique name belongs to this run, including a partially started container.
        subprocess.run(["docker", "rm", "--force", name], capture_output=True, timeout=20)


if __name__ == "__main__":
    raise SystemExit(main())
