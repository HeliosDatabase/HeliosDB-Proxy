#!/usr/bin/env python3
"""Function-level attribution of one Criterion case for scripts/bench-gate.sh.

scripts/bench-gate-fde.py classifies a whole bench executable: one changed
function anywhere makes every case of it `code-changed`. This tool asks the
narrower question for a single case: is the code the case RUNS the same in
both builds?

It runs the case alone in each executable (`--bench --profile-time SECONDS
'^ID$'`), samples the user-space instruction pointer of every thread with
perf_event_open(2) (cycles; no perf binary is needed), maps each sample onto
the function holding it (`.eh_frame` FDE ranges, so stripped executables
work) and looks that function's normalized body up in the other executable
(same normalization as bench-gate-fde.py: a function that only moved compares
equal).

Classification (owner decision 2026-11-19: a reproduced separated regression
on `function-identical` code is reported, not failing):

  function-identical  in BOTH arms at most MAX_CHANGED_PCT (default 0.5) of
                      the samples fall in a function whose body the other
                      executable does not hold, or inside the executable but
                      in no function. The case executes the same
                      instructions; only their addresses differ.
  code-changed        more than that in either arm (the functions are listed).
  unknown             fewer than MIN_SAMPLES (default 2000) samples in an arm,
                      the sampler could not start, or the tools failed. The
                      gate treats it as code-changed.

Limits, for the reviewer: samples are of the whole process, so the setup the
bench target runs before the case counts too (it can only add changed-code
samples, never hide them, beyond diluting a share that is already under the
threshold). Identical instructions on different DATA are still called
function-identical: a case whose input a changed setup function builds
differently needs the reviewer's eye.

Usage:
  bench-gate-hot.py run --base-exe P --cand-exe P --base-cwd D --cand-cwd D
                        --id FULL_ID [--seconds 5] [--out FILE.json]
Env: HOT_MAX_CHANGED_PCT, HOT_MIN_SAMPLES, PSAMPLE_PERIOD (cycles per sample).
Exit: 0 function-identical, 1 code-changed, 2 unknown / usage error.
"""
import argparse
import bisect
import collections
import ctypes
import importlib.util
import json
import mmap
import os
import pathlib
import re
import struct
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("bench_gate_fde", HERE / "bench-gate-fde.py")
fde = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fde)

SYS_perf_event_open = 298  # x86_64
EXCLUDE_KERNEL, EXCLUDE_HV = 1 << 5, 1 << 6
PERF_SAMPLE_IP = 1
PERF_RECORD_LOST, PERF_RECORD_SAMPLE = 2, 9
PAGE = mmap.PAGESIZE
DATA_PAGES = 64
TOP = 8


def attribute(samples, bodies, other_hashes):
    """Split `samples` ({address in the executable: count}) by the function
    holding each address: `bodies` is [(start, end, hash)] sorted by start,
    `other_hashes` the body hashes of the other executable."""
    starts = [start for start, _end, _digest in bodies]
    same = changed = orphan = 0
    by_changed = collections.Counter()
    for address, count in samples.items():
        i = bisect.bisect_right(starts, address) - 1
        if i < 0 or address >= bodies[i][1]:
            orphan += count
        elif bodies[i][2] in other_hashes:
            same += count
        else:
            changed += count
            by_changed[bodies[i][0]] += count
    return {
        "identical": same,
        "changed": changed,
        "outside_functions": orphan,
        "changed_functions": [{"start": f"{start:x}", "samples": count}
                              for start, count in by_changed.most_common(TOP)],
    }


def decide(base, cand, max_changed_pct, min_samples):
    """Classification of a case from both arms' sample counts (`attribute`
    plus "elsewhere": samples outside the executable)."""
    shares = {}
    for name, arm in (("base", base), ("cand", cand)):
        total = arm["identical"] + arm["changed"] + arm["outside_functions"] + arm["elsewhere"]
        if total < min_samples:
            return "unknown", f"{name}: {total} samples, fewer than {min_samples}", shares
        shares[name] = 100.0 * (arm["changed"] + arm["outside_functions"]) / total
    worst = max(shares, key=shares.get)
    if shares[worst] > max_changed_pct:
        return ("code-changed",
                f"{worst}: {shares[worst]:.2f}% of samples in changed code "
                f"(limit {max_changed_pct}%)", shares)
    return ("function-identical",
            f"changed-code samples base {shares['base']:.2f}% cand {shares['cand']:.2f}% "
            f"(limit {max_changed_pct}%)", shares)


class Sampler:
    """One cycles sampler on one thread, user space only."""

    def __init__(self, tid, period):
        libc = ctypes.CDLL(None, use_errno=True)
        attr = struct.pack("IIQQQQQIIQ", 0, 64, 0, period, PERF_SAMPLE_IP, 0,
                           EXCLUDE_KERNEL | EXCLUDE_HV, 1, 0, 0)
        self.fd = libc.syscall(SYS_perf_event_open, ctypes.create_string_buffer(attr),
                               tid, -1, -1, 0)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), f"perf_event_open tid {tid}")
        self.ring = mmap.mmap(self.fd, (1 + DATA_PAGES) * PAGE, mmap.MAP_SHARED,
                              mmap.PROT_READ | mmap.PROT_WRITE)
        self.size = DATA_PAGES * PAGE
        self.tail = 0
        self.lost = 0

    def _read(self, at, length):
        at %= self.size
        raw = bytes(self.ring[PAGE + at:PAGE + min(at + length, self.size)])
        if len(raw) < length:
            raw += bytes(self.ring[PAGE:PAGE + length - len(raw)])
        return raw

    def drain(self, ips):
        head = struct.unpack_from("Q", self.ring, 1024)[0]
        while self.tail < head:
            kind, _misc, length = struct.unpack("IHH", self._read(self.tail, 8))
            if length == 0:
                break
            if kind == PERF_RECORD_SAMPLE:
                ips[struct.unpack("Q", self._read(self.tail + 8, 8))[0]] += 1
            elif kind == PERF_RECORD_LOST:
                self.lost += 1
            self.tail += length
        struct.pack_into("Q", self.ring, 1032, self.tail)

    def close(self):
        self.ring.close()
        os.close(self.fd)


def load_base(pid, exe):
    """Address the executable is loaded at, and its mapped range."""
    real = os.path.realpath(exe)
    base = low = high = None
    with open(f"/proc/{pid}/maps") as maps:
        for line in maps:
            fields = line.split()
            if len(fields) < 6 or os.path.realpath(fields[5]) != real:
                continue
            start, end = (int(x, 16) for x in fields[0].split("-"))
            if base is None:
                base, low = start - int(fields[2], 16), start
            high = end
    return base, low, high


def profile(exe, cwd, full_id, seconds, period):
    """Run the case alone and return ({address in exe: samples}, samples
    elsewhere, lost records)."""
    child = subprocess.Popen(
        [exe, "--bench", "--profile-time", str(seconds), "^" + re.escape(full_id) + "$"],
        cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    samplers, ips, mapping = {}, collections.Counter(), (None, None, None)
    try:
        while child.poll() is None:
            if mapping[0] is None:
                try:
                    mapping = load_base(child.pid, exe)
                except OSError:
                    pass
            try:
                tids = [int(t) for t in os.listdir(f"/proc/{child.pid}/task")]
            except OSError:
                tids = []
            for tid in tids:
                if tid not in samplers:
                    try:
                        samplers[tid] = Sampler(tid, period)
                    except OSError:
                        if tid == child.pid:
                            raise
            for sampler in samplers.values():
                sampler.drain(ips)
            time.sleep(0.01)
        for sampler in samplers.values():
            sampler.drain(ips)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
        lost = sum(s.lost for s in samplers.values())
        for sampler in samplers.values():
            sampler.close()
    if child.returncode != 0:
        raise RuntimeError(f"{exe} exited with {child.returncode}")
    base, low, high = mapping
    if base is None:
        raise RuntimeError("executable mapping not seen")
    inside, elsewhere = collections.Counter(), 0
    for ip, count in ips.items():
        if low <= ip < high:
            inside[ip - base] += count
        else:
            elsewhere += count
    return inside, elsewhere, lost


def run(args):
    max_changed = float(os.environ.get("HOT_MAX_CHANGED_PCT", "0.5"))
    min_samples = int(os.environ.get("HOT_MIN_SAMPLES", "2000"))
    period = int(os.environ.get("PSAMPLE_PERIOD", "200003"))
    result = {"full_id": args.id, "seconds": args.seconds, "period": period,
              "max_changed_pct": max_changed, "min_samples": min_samples,
              "base_exe": args.base_exe, "cand_exe": args.cand_exe}
    try:
        bodies = {"base": fde.bodies(args.base_exe)[0], "cand": fde.bodies(args.cand_exe)[0]}
        hashes = {arm: {digest for _s, _e, digest in rows} for arm, rows in bodies.items()}
        arms = {}
        for arm, exe, cwd, other in (("base", args.base_exe, args.base_cwd, "cand"),
                                     ("cand", args.cand_exe, args.cand_cwd, "base")):
            inside, elsewhere, lost = profile(exe, cwd, args.id, args.seconds, period)
            arms[arm] = dict(attribute(inside, bodies[arm], hashes[other]),
                             elsewhere=elsewhere, lost_records=lost)
        classification, reason, shares = decide(arms["base"], arms["cand"],
                                                max_changed, min_samples)
        result.update(classification=classification, reason=reason,
                      changed_pct=shares, base=arms["base"], cand=arms["cand"])
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        result.update(classification="unknown", reason=repr(error))
    if args.out:
        with open(args.out, "w") as out:
            json.dump(result, out, indent=1)
    print(f"{args.id}: {result['classification']} — {result['reason']}")
    return {"function-identical": 0, "code-changed": 1}.get(result["classification"], 2)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    for name in ("base-exe", "cand-exe", "base-cwd", "cand-cwd", "id"):
        r.add_argument("--" + name, required=True)
    r.add_argument("--seconds", type=float, default=5.0)
    r.add_argument("--out")
    return run(parser.parse_args(argv[1:]))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
