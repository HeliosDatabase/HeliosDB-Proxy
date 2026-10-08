#!/usr/bin/env python3
"""Run Criterion cases in adjacent A/B pairs inside the caller's bounded fleet lock.

Uses the canonical gate's frozen executable/case inventories. Each invocation
must create exactly one fresh raw result and leave prior results untouched.
This changes process lifecycle versus whole-suite rounds; thresholds are unchanged.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
import pathlib
import re
import signal
import subprocess
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(root, label):
    return {str(p): (sha(p), p.stat().st_mtime_ns)
            for d in root.glob(f"**/gate-{label}-r*") if d.is_dir()
            for p in d.iterdir() if p.is_file()}


class CaseError(RuntimeError):
    def __init__(self, message, record):
        super().__init__(message)
        self.record = record


def cleanup(process):
    """Bounded cleanup of only the child session created by this invocation."""
    events = []
    for sig, seconds in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
        try:
            os.killpg(process.pid, sig)
            events.append({"signal": sig.name, "sent": True})
        except ProcessLookupError:
            events.append({"signal": sig.name, "group_gone": True})
            break
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            process.poll()
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            continue
        break
    try:
        events.append({"reaped": True, "exit": process.wait(timeout=5)})
    except subprocess.TimeoutExpired:
        events.append({"reaped": False})
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        events.append({"group_gone": True})
    else:
        events.append({"group_gone": False})
    return events


def execute(tree, executable, digest, case, label, round_number, log, timeout=300):
    root = tree / "target/criterion"
    before = snapshot(root, label)
    pattern = "^" + re.escape(case) + "$"
    command = [str(executable), "--bench", "--save-baseline",
               f"gate-{label}-r{round_number}", pattern]
    record = {"command": command, "cwd": str(tree), "sha256": digest,
              "filter": pattern, "started": time.time(), "timeout_seconds": timeout}
    process = None
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}

    def interrupted(number, _frame):
        record["signal"] = number
        # Do not interrupt Popen before its owned child is assigned.
        if process is not None:
            raise InterruptedError(f"received signal {number}")

    try:
        for s in previous:
            signal.signal(s, interrupted)
        if sha(executable) != digest:
            raise ValueError(f"executable changed before invocation: {executable}")
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(tree / "target")
        process = subprocess.Popen(command, cwd=tree, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        record["pid"] = process.pid
        if "signal" in record:
            raise InterruptedError(f"received signal {record['signal']}")
        running = pathlib.Path(f"/proc/{process.pid}/exe")
        if running.resolve() != executable.resolve() or sha(running) != digest:
            raise ValueError("running executable does not match frozen identity")
        record["exit"] = process.wait(timeout=timeout)
        if record["exit"] != 0:
            raise ValueError("case process failed")
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise ValueError("case left a surviving process group")
        if sha(executable) != digest:
            raise ValueError(f"executable changed during invocation: {executable}")
        after = snapshot(root, label)
        if any(after.get(p) != v for p, v in before.items()):
            raise ValueError("invocation rewrote or removed an earlier result")
        added = set(after) - set(before)
        directories = {pathlib.Path(p).parent for p in added}
        names = {pathlib.Path(p).name for p in added}
        required = {"benchmark.json", "estimates.json", "sample.json"}
        if len(directories) != 1 or not required <= names or not names <= required | {"tukey.json"}:
            raise ValueError(f"expected one raw result with benchmark/estimates/sample metadata, got {sorted(added)}")
        directory = directories.pop()
        if directory.name != f"gate-{label}-r{round_number}":
            raise ValueError("invocation wrote a different round")
        spec = importlib.util.spec_from_file_location(
            "compare", pathlib.Path(__file__).with_name("bench-gate-compare.py"))
        compare = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(compare)
        meta = compare.strict_load(directory / "benchmark.json")
        if meta.get("full_id") != case:
            raise ValueError(f"wrong raw case: {meta.get('full_id')!r} != {case!r}")
        for p in added:
            compare.strict_load(p)
            if pathlib.Path(p).stat().st_mtime_ns < int(record["started"] * 1e9):
                raise ValueError(f"stale raw result: {p}")
        record["raw"] = {p: {"sha256": after[p][0], "mtime_ns": after[p][1]} for p in sorted(added)}
        record["ended"] = time.time()
        return record
    except BaseException as error:
        # Defer further interrupts while bounding cleanup of the owned session.
        for s in previous:
            signal.signal(s, signal.SIG_IGN)
        if process is not None:
            try:
                record["cleanup"] = cleanup(process)
            except OSError as cleanup_error:
                record["cleanup_error"] = repr(cleanup_error)
            record["exit"] = process.returncode
        record["ended"] = time.time()
        record["error"] = str(error)
        raise CaseError(str(error), record) from error
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)


def run(base, cand, out, label, rounds, timeout=300):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    spec = importlib.util.spec_from_file_location(
        "compare", pathlib.Path(__file__).with_name("bench-gate-compare.py"))
    compare = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(compare)
    cases = {a: compare.read_cases(out / f"cases-{a}.tsv") for a in ("base", "cand")}
    if not cases["base"] or cases["base"] != cases["cand"]:
        raise ValueError("paired mode requires identical nonempty target/full-ID inventories")
    executables = {}
    trees = {"base": base, "cand": cand}
    for arm in trees:
        executables[arm] = {}
        for line in (out / f"executables-{arm}.txt").read_text().splitlines():
            target, digest, raw_path = line.split(" ", 2)
            path = pathlib.Path(raw_path)
            if target in executables[arm] or not re.fullmatch(r"[a-f0-9]{64}", digest):
                raise ValueError("invalid executable inventory")
            if not path.is_absolute() or not path.resolve().is_relative_to((trees[arm] / "target").resolve()):
                raise ValueError("executable outside its own target directory")
            if not os.access(path, os.X_OK) or sha(path) != digest:
                raise ValueError("missing or changed executable")
            executables[arm][target] = (path, digest)
        if not set(cases[arm].values()) <= executables[arm].keys():
            raise ValueError("case target lacks a frozen executable")
        if any((trees[arm] / "target/criterion").glob(f"**/gate-{label}-r*")):
            raise ValueError("paired output label already exists")
    with (out / "paired-invocations.jsonl").open("x") as ledger:
        for r in range(1, rounds + 1):
            for case in sorted(cases["base"]):
                for arm in (("base", "cand") if r % 2 else ("cand", "base")):
                    target = cases[arm][case]
                    executable, digest = executables[arm][target]
                    identity = {"arm": arm, "round": r, "target": target, "full_id": case,
                                "executable": str(executable), "sha256": digest,
                                "filter": "^" + re.escape(case) + "$"}
                    ledger.write(json.dumps(identity | {"event": "start", "time": time.time()}) + "\n")
                    ledger.flush()
                    try:
                        with (out / f"{arm}-r{r}.log").open("a") as log:
                            log.write(f"Paired execution: target={target} full_id={case!r} executable={executable}\n")
                            log.flush()
                            record = execute(trees[arm], executable, digest, case, label, r, log, timeout)
                    except Exception as error:
                        ledger.write(json.dumps(identity | getattr(error, "record", {}) |
                                                {"event": "error", "error": str(error),
                                                 "time": time.time()}) + "\n")
                        ledger.flush()
                        raise
                    ledger.write(json.dumps(identity | record | {"event": "complete"}) + "\n")
                    ledger.flush()
                    print(f"paired round {r} {arm} {case}", flush=True)
    # Detect inter-invocation or late changes as well as changes during a call.
    for line in (out / "paired-invocations.jsonl").read_text().splitlines():
        receipt = json.loads(line)
        if receipt["event"] == "complete":
            for name, identity in receipt["raw"].items():
                p = pathlib.Path(name)
                if sha(p) != identity["sha256"] or p.stat().st_mtime_ns != identity["mtime_ns"]:
                    raise ValueError(f"completed invocation artifact changed: {p}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base", type=pathlib.Path, required=True)
    p.add_argument("--cand", type=pathlib.Path, required=True)
    p.add_argument("--out", type=pathlib.Path, required=True)
    p.add_argument("--label", required=True)
    p.add_argument("--rounds", type=int, required=True)
    p.add_argument("--timeout", type=float, default=300)
    a = p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9._-]+", a.label) or a.rounds < 1 or a.base.resolve() == a.cand.resolve():
        p.error("invalid label, rounds, or duplicate trees")
    run(a.base, a.cand, a.out, a.label, a.rounds, a.timeout)


if __name__ == "__main__":
    main()
