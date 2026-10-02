#!/usr/bin/env python3
"""Fail-closed comparator for scripts/bench-gate.sh (CLAUDE.md quality gate 3).

Usage:
  bench-gate-compare.py --base-root DIR --cand-root DIR --label LABEL --rounds N
                        --base-cases FILE --cand-cases FILE --out DIR
                        [--budget 3] [--sep 2] [--start EPOCH] [--classes FILE]
                        [--baseline-name NAME] [--candidate-name NAME]

--base-root/--cand-root  each tree's Criterion output directory (<tree>/target/criterion)
--base-cases/--cand-cases  "<bench target>\t<full Criterion id>" lines: the case set each
                         tree's executables declare (`<exe> --bench --list`)
--classes                JSON {target: bench-gate-fde.py result} (optional)
--start                  epoch seconds the measurement started; any output older than
                         this is stale (0 disables the check, e.g. to replay an archive)

Verdict rules (unchanged from the original gate):
  FAIL if the mean of per-case median deltas exceeds --budget, or any case is a
  separated regression (every candidate round slower than every baseline round)
  above --sep percent, or a case the baseline declares is missing from the candidate
  (a removed benchmark cannot hide a regression silently).
New, fail-closed: the run is INVALID (exit 2) — never PASS or FAIL — when an expected
case is missing or duplicated in any round, an output is malformed or non-finite, or
an output predates --start. Cases only the candidate declares are reported as NEW
(they need a benches/BASELINE.md entry) and are not compared.

Attribution evidence (does not change the verdict): every case carries its bench
target's classification from bench-gate-fde.py. A separated regression on a
`body-identical` executable is layout class by construction (no executed
instruction changed); one on a `code-changed` executable needs a code-level look.
Exit codes: 0 PASS, 1 FAIL, 2 INVALID.
"""
import argparse
import glob
import json
import math
import os
import statistics
import sys


def strict_load(path):
    def no_duplicates(pairs):
        keys = [k for k, _ in pairs]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate keys")
        return dict(pairs)

    def bad_constant(name):
        raise ValueError(f"non-finite constant {name}")

    with open(path) as f:
        return json.load(f, object_pairs_hook=no_duplicates, parse_constant=bad_constant)


def finite_positive(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x) and x > 0


def read_cases(path):
    cases = {}
    for line in open(path):
        line = line.rstrip("\n")
        if not line:
            continue
        target, full_id = line.split("\t", 1)
        if full_id in cases:
            raise SystemExit(f"duplicate case {full_id!r} in {path}")
        cases[full_id] = target
    return cases


def collect(root, label, r, start, problems, arm):
    """full_id -> (median_ns, case_dir) for the saved baseline gate-<label>-r<r>."""
    res = {}
    name = f"gate-{label}-r{r}"
    for est in glob.glob(os.path.join(root, "**", name, "estimates.json"), recursive=True):
        d = os.path.dirname(est)
        where = f"{arm} r{r} {os.path.relpath(d, root)}"
        try:
            meta = strict_load(os.path.join(d, "benchmark.json"))
            e = strict_load(est)
            sample = strict_load(os.path.join(d, "sample.json"))
        except (OSError, ValueError) as error:
            problems.append(f"{where}: unreadable output: {error}")
            continue
        if start:
            for p in (est, os.path.join(d, "benchmark.json"), os.path.join(d, "sample.json")):
                if os.path.getmtime(p) < start:
                    problems.append(f"{where}: stale output {os.path.basename(p)} predates this run")
        med = e.get("median", {})
        pe = med.get("point_estimate")
        ci = med.get("confidence_interval", {})
        lo, hi = ci.get("lower_bound"), ci.get("upper_bound")
        if not all(finite_positive(x) for x in (pe, lo, hi)) or not lo <= pe <= hi:
            problems.append(f"{where}: invalid median estimate {med}")
            continue
        iters, times = sample.get("iters", []), sample.get("times", [])
        if not iters or len(iters) != len(times) or not all(finite_positive(x) for x in iters + times):
            problems.append(f"{where}: invalid sample arrays")
            continue
        full_id = meta.get("full_id")
        if not isinstance(full_id, str):
            problems.append(f"{where}: benchmark.json lacks full_id")
            continue
        if full_id in res:
            problems.append(f"{where}: duplicate case {full_id}")
        res[full_id] = (pe, os.path.relpath(os.path.dirname(d), root))
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-root", required=True)
    ap.add_argument("--cand-root", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--rounds", type=int, required=True)
    ap.add_argument("--base-cases", required=True)
    ap.add_argument("--cand-cases", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--budget", type=float, default=3.0)
    ap.add_argument("--sep", type=float, default=2.0)
    ap.add_argument("--start", type=float, default=0.0)
    ap.add_argument("--classes")
    ap.add_argument("--baseline-name", default="")
    ap.add_argument("--candidate-name", default="")
    ap.add_argument("--invalid", action="append", default=[],
                    help="a problem found by the caller (e.g. executable identity); makes the run INVALID")
    a = ap.parse_args()

    base_cases, cand_cases = read_cases(a.base_cases), read_cases(a.cand_cases)
    classes = strict_load(a.classes) if a.classes else {}
    problems = list(a.invalid)
    if not base_cases or not cand_cases:
        problems.append("a tree declares no benchmark cases")
    compared = sorted(set(base_cases) & set(cand_cases))
    removed = sorted(set(base_cases) - set(cand_cases))
    new = sorted(set(cand_cases) - set(base_cases))

    B = [collect(a.base_root, a.label, r, a.start, problems, "base") for r in range(1, a.rounds + 1)]
    C = [collect(a.cand_root, a.label, r, a.start, problems, "cand") for r in range(1, a.rounds + 1)]
    for arm, rounds, declared in (("base", B, base_cases), ("cand", C, cand_cases)):
        for r, got in enumerate(rounds, 1):
            missing = sorted(set(declared) - set(got))
            extra = sorted(set(got) - set(declared))
            if missing:
                problems.append(f"{arm} r{r}: {len(missing)} declared case(s) missing: {missing[:10]}")
            if extra:
                problems.append(f"{arm} r{r}: {len(extra)} undeclared case(s) present: {extra[:10]}")

    rows = []
    for case in compared:
        if not all(case in x for x in B + C):
            continue
        b = [x[case][0] for x in B]
        c = [x[case][0] for x in C]
        bm, cm = statistics.median(b), statistics.median(c)
        delta = (cm - bm) / bm * 100.0
        separated = (min(c) > max(b)) or (max(c) < min(b))
        target = cand_cases[case]
        rows.append(dict(case=B[0][case][1], full_id=case, target=target,
                         executable=classes.get(target, {}).get("classification", "unknown"),
                         base_ns=bm, cand_ns=cm, delta_pct=delta, separated=separated,
                         base_rounds=b, cand_rounds=c))

    deltas = [r["delta_pct"] for r in rows]
    mean = statistics.fmean(deltas) if deltas else float("nan")
    median = statistics.median(deltas) if deltas else float("nan")
    # EPS keeps "exactly at the threshold" on the passing side despite float rounding.
    EPS = 1e-9
    sep_reg = [r for r in rows if r["separated"] and r["delta_pct"] > a.sep + EPS]
    sep_imp = [r for r in rows if r["separated"] and r["delta_pct"] < -a.sep - EPS]
    if problems:
        verdict = "INVALID"
    elif mean <= a.budget + EPS and not sep_reg and not removed:
        verdict = "PASS"
    else:
        verdict = "FAIL"
    layout_only = [r["case"] for r in sep_reg if r["executable"] == "body-identical"]
    needs_review = [r["case"] for r in sep_reg if r["executable"] != "body-identical"]
    if sep_reg and not needs_review:
        attribution = "all separated regressions are on function-body-identical executables (layout class)"
    elif needs_review:
        attribution = f"{len(needs_review)} separated regression(s) on code-changed/unclassified executables need review"
    else:
        attribution = ""

    os.makedirs(a.out, exist_ok=True)
    report = dict(label=a.label, rounds=a.rounds, baseline=a.baseline_name, candidate=a.candidate_name,
                  budget_pct=a.budget, sep_pct=a.sep, mean_delta_pct=mean, median_delta_pct=median,
                  matched=len(rows), verdict=verdict, invalid_problems=problems,
                  separated_regressions=[r["case"] for r in sep_reg],
                  separated_improvements=[r["case"] for r in sep_imp],
                  removed_cases=removed, new_cases=new,
                  executable_classes=classes, layout_only_regressions=layout_only,
                  regressions_needing_review=needs_review, attribution=attribution, cases=rows)
    with open(os.path.join(a.out, "bench-gate.json"), "w") as f:
        json.dump(report, f, indent=1)

    lines = [f"bench-gate {a.label}: {verdict}"]
    lines.append(f"matched={len(rows)} rounds={a.rounds} mean_delta={mean:+.3f}% median_delta={median:+.3f}% "
                 f"budget={a.budget}% separated_regressions(>{a.sep}%)={len(sep_reg)} "
                 f"separated_improvements(>{a.sep}%)={len(sep_imp)}")
    for p in problems[:40]:
        lines.append(f"INVALID: {p}")
    if removed:
        lines.append(f"REMOVED cases (declared by the baseline, absent from the candidate): {removed}")
    if new:
        lines.append(f"NEW cases (candidate only, no baseline; add a benches/BASELINE.md entry): {new}")
    for target, c in sorted(classes.items()):
        lines.append(f"executable {target}: {c.get('classification')} "
                     f"(bodies identical {c.get('identical_bodies')}, moved {c.get('moved')}, "
                     f"only-base {c.get('only_in_base')}, only-cand {c.get('only_in_candidate')})")
    if attribution:
        lines.append(f"attribution evidence: {attribution}")
    lines.append("")
    lines.append(f"{'case':58s} {'base_ns':>12s} {'cand_ns':>12s} {'delta%':>9s} sep  exe  base_rounds | cand_rounds")
    for r in sorted(rows, key=lambda r: -r["delta_pct"]):
        br = " ".join(f"{v:.1f}" for v in r["base_rounds"])
        cr = " ".join(f"{v:.1f}" for v in r["cand_rounds"])
        exe = {"body-identical": "=", "code-changed": "C"}.get(r["executable"], "?")
        lines.append(f"{r['case']:58s} {r['base_ns']:12.2f} {r['cand_ns']:12.2f} {r['delta_pct']:+9.2f} "
                     f"{'Y' if r['separated'] else '.':>3s}  {exe:>3s}  {br} | {cr}")
    txt = "\n".join(lines) + "\n"
    with open(os.path.join(a.out, "bench-gate-summary.txt"), "w") as f:
        f.write(txt)
    print(txt)
    sys.exit({"PASS": 0, "FAIL": 1, "INVALID": 2}[verdict])


if __name__ == "__main__":
    main()
