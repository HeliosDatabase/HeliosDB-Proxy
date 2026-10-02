#!/usr/bin/env python3
"""Compare the function bodies of two builds of the same Criterion bench executable.

Usage: bench-gate-fde.py BASE_EXECUTABLE CAND_EXECUTABLE [--json]

Release bench executables are stripped, so functions are delimited by their
`.eh_frame` FDE pc ranges (`readelf -wF`), and each function's `.text`
instructions (`objdump -d`) are normalized: RIP-relative displacements,
branch/call targets and large immediates (addresses) are replaced by
placeholders, so a function that only MOVED compares equal to itself.

The result classifies the pair:
  body-identical  the multiset of normalized function bodies is the same in both
                  executables: no executed instruction changed, only placement
                  (link order / alignment). A separated Criterion regression on
                  such a pair is layout class by construction.
  code-changed    at least one function body differs (or exists in one only).
  unknown         the tools failed (missing binutils, unreadable file).

The classification is attribution evidence for the reviewer; scripts/bench-gate.sh
records it but does not change its verdict because of it.
"""
import bisect
import collections
import hashlib
import json
import re
import subprocess
import sys

LEAD = re.compile(r"^\s*([0-9a-f]+):\s*(.*)$")
RIP_COMMENT = re.compile(r"\s*#\s*[0-9a-f]+(\s*<[^>]*>)?")
TARGET = re.compile(r"\b[0-9a-f]{4,}\s*(<[^>]*>)?")
RIP_REL = re.compile(r"-?0x[0-9a-f]+\(%rip\)")
BIG_IMM = re.compile(r"\$0x[0-9a-f]{5,}")


def normalize(insn):
    insn = RIP_COMMENT.sub("", insn)
    insn = RIP_REL.sub("RIP", insn)
    insn = TARGET.sub("TGT", insn)
    insn = BIG_IMM.sub("$IMM", insn)
    return insn.strip()


def functions(path):
    fdes = subprocess.run(["readelf", "-wF", path], capture_output=True, text=True, check=True).stdout
    ranges = sorted((int(a, 16), int(b, 16))
                    for a, b in re.findall(r"FDE cie=[0-9a-f]+ pc=([0-9a-f]+)\.\.([0-9a-f]+)", fdes))
    dis = subprocess.run(["objdump", "-d", "--no-show-raw-insn", "-j", ".text", path],
                         capture_output=True, text=True, check=True).stdout
    insns = []
    for line in dis.splitlines():
        m = LEAD.match(line)
        if m and not line.endswith(":"):
            insns.append((int(m.group(1), 16), normalize(m.group(2))))
    addrs = [a for a, _ in insns]
    out = []
    for start, end in ranges:
        i, j = bisect.bisect_left(addrs, start), bisect.bisect_left(addrs, end)
        body = "\n".join(t for _, t in insns[i:j])
        out.append((start, hashlib.sha256(body.encode()).hexdigest()))
    return out, len(insns)


def compare(base, cand):
    try:
        fb, nb = functions(base)
        fc, nc = functions(cand)
    except (OSError, subprocess.CalledProcessError) as error:
        return {"classification": "unknown", "error": repr(error), "base": base, "candidate": cand}
    hb = collections.Counter(h for _, h in fb)
    hc = collections.Counter(h for _, h in fc)
    only_base = sum((hb - hc).values())
    only_cand = sum((hc - hb).values())
    same_place = len({(a, h) for a, h in fb} & {(a, h) for a, h in fc})
    common = sum((hb & hc).values())
    if not fb or not fc:
        cls = "unknown"
    elif only_base == 0 and only_cand == 0:
        cls = "body-identical"
    else:
        cls = "code-changed"
    return {
        "classification": cls,
        "base": base, "candidate": cand,
        "functions_base": len(fb), "functions_candidate": len(fc),
        "instructions_base": nb, "instructions_candidate": nc,
        "identical_bodies": common, "moved": common - same_place,
        "only_in_base": only_base, "only_in_candidate": only_cand,
    }


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--json"]
    if len(args) != 2:
        sys.exit(__doc__)
    result = compare(*args)
    if "--json" in sys.argv:
        print(json.dumps(result, indent=1))
    else:
        print(f"{result['classification']}: " + ", ".join(
            f"{k}={v}" for k, v in result.items() if k not in ("classification", "base", "candidate")))
