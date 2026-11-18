#!/usr/bin/env python3
"""Second-build confirmation for scripts/bench-gate.sh (CONFIRM=1, the default).

The primary comparison (scripts/bench-gate-compare.py) FAILs on any separated
regression. On this kind of shared host most of them do not survive a second
measurement: each new build separates a different set of cases whose code did
not change. The release rule (owner decision, 2026-11-17) is:

  * the cumulative budget stands: a mean of per-case deltas over BUDGET_PCT, a
    removed case, or an INVALID run fails as before;
  * a separated regression on a `body-identical` executable (no function body
    changed: placement only) is reported, not failing;
  * a separated regression on a `code-changed` (or unclassified) executable
    fails only if it reproduces in a second, independent build of both trees:
    re-measured there over the same interleaved rounds, it is again separated
    (every candidate round slower than every baseline round) and slower.

Subcommands (all paths are files written or read by bench-gate.sh):

  plan PRIMARY_JSON
      Print INVALID, FAIL, LAYOUT_ONLY, or the full Criterion IDs to confirm,
      one per line.
  cases CASES_TSV IDS_FILE OUT_TSV
      Keep only the cases whose full ID is listed in IDS_FILE.
  regex IDS_FILE
      Print an anchored Criterion filter matching exactly those IDs.
  verdict PRIMARY_JSON CONFIRM_JSON OUT_DIR
      Decide; write OUT_DIR/bench-gate-final.json and append the decision to
      OUT_DIR/bench-gate-summary.txt. Exit 0 PASS, 1 FAIL, 2 INVALID.
"""
import json
import os
import re
import sys

EPS = 1e-9


def plan(primary):
    """What the confirmation must do after the primary comparison."""
    if primary.get("verdict") == "INVALID" or primary.get("invalid_problems"):
        return "INVALID", []
    if primary["mean_delta_pct"] > primary["budget_pct"] + EPS or primary.get("removed_cases"):
        return "FAIL", []
    review = set(primary.get("regressions_needing_review", []))
    ids = [c["full_id"] for c in primary["cases"] if c["case"] in review]
    if not ids:
        return "LAYOUT_ONLY", []
    return "CONFIRM", ids


def confirmed(confirm):
    """Cases the confirmation run separated as regressions again."""
    return [c for c in confirm["cases"] if c["separated"] and c["delta_pct"] > EPS]


def decide(primary, confirm):
    """(verdict, record) for the primary run and, when one ran, its confirmation."""
    action, ids = plan(primary)
    record = {
        "label": primary.get("label"),
        "rule": "second-build confirmation (owner decision 2026-11-17)",
        "primary_verdict": primary.get("verdict"),
        "mean_delta_pct": primary.get("mean_delta_pct"),
        "budget_pct": primary.get("budget_pct"),
        "layout_only_regressions": primary.get("layout_only_regressions", []),
        "to_confirm": ids,
    }
    if action == "INVALID":
        return "INVALID", dict(record, reason="the primary run is INVALID")
    if action == "FAIL":
        return "FAIL", dict(record, reason="mean over budget or a case removed")
    if action == "LAYOUT_ONLY":
        reason = ("every separated regression is on a body-identical executable"
                  if primary.get("separated_regressions") else "no separated regression")
        return "PASS", dict(record, reason=reason)
    if confirm is None:
        return "INVALID", dict(record, reason="confirmation needed but not run")
    if confirm.get("verdict") == "INVALID" or confirm.get("invalid_problems"):
        return "INVALID", dict(record, reason="the confirmation run is INVALID",
                               confirm_problems=confirm.get("invalid_problems"))
    measured = {c["full_id"] for c in confirm["cases"]}
    missing = sorted(set(ids) - measured)
    if missing:
        return "INVALID", dict(record, reason="cases to confirm were not measured", missing=missing)
    again = confirmed(confirm)
    record["confirmed_regressions"] = [
        {"full_id": c["full_id"], "delta_pct": c["delta_pct"],
         "base_rounds": c["base_rounds"], "cand_rounds": c["cand_rounds"]}
        for c in again
    ]
    record["not_reproduced"] = sorted(set(ids) - {c["full_id"] for c in again})
    if again:
        return "FAIL", dict(record, reason="separated regressions reproduced in the second build")
    return "PASS", dict(record, reason="no separated regression on changed code reproduced")


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd = argv[1]
    if cmd == "plan" and len(argv) == 3:
        action, ids = plan(json.load(open(argv[2])))
        print("\n".join(ids) if action == "CONFIRM" else action)
        return 0
    if cmd == "cases" and len(argv) == 5:
        wanted = {line.strip() for line in open(argv[3]) if line.strip()}
        kept = [line for line in open(argv[2])
                if line.rstrip("\n").split("\t", 1)[-1] in wanted]
        with open(argv[4], "w") as out:
            out.writelines(kept)
        return 0
    if cmd == "regex" and len(argv) == 3:
        ids = [line.strip() for line in open(argv[2]) if line.strip()]
        print("^(" + "|".join(re.escape(i) for i in ids) + ")$")
        return 0
    if cmd == "verdict" and len(argv) == 5:
        primary = json.load(open(argv[2]))
        confirm = json.load(open(argv[3])) if os.path.exists(argv[3]) else None
        verdict, record = decide(primary, confirm)
        record["verdict"] = verdict
        with open(os.path.join(argv[4], "bench-gate-final.json"), "w") as out:
            json.dump(record, out, indent=1)
        lines = [f"final verdict (second-build confirmation): {verdict} — {record['reason']}"]
        for c in record.get("confirmed_regressions", []):
            lines.append(f"  reproduced: {c['full_id']} {c['delta_pct']:+.2f}%")
        if record.get("not_reproduced"):
            lines.append(f"  not reproduced: {', '.join(record['not_reproduced'])}")
        if record["layout_only_regressions"]:
            lines.append(f"  layout only (reported): {', '.join(record['layout_only_regressions'])}")
        with open(os.path.join(argv[4], "bench-gate-summary.txt"), "a") as out:
            out.write("\n".join(lines) + "\n")
        print("\n".join(lines))
        return {"PASS": 0, "FAIL": 1, "INVALID": 2}[verdict]
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
