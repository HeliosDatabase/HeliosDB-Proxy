#!/usr/bin/env python3
"""Tests for scripts/bench-gate-confirm.py (the second-build confirmation rule).

Run: python3 scripts/tests/bench_gate_confirm_test.py
"""
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
SCRIPT = HERE.parent / "bench-gate-confirm.py"
spec = importlib.util.spec_from_file_location("confirm", SCRIPT)
confirm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(confirm)


def case(name, delta, separated, executable="code-changed"):
    return {
        "case": name.replace("/", "_", 1),
        "full_id": name,
        "executable": executable,
        "delta_pct": delta,
        "separated": separated,
        "base_rounds": [100.0, 101.0, 100.5],
        "cand_rounds": [100.0 + delta] * 3,
    }


def primary(cases, mean=-1.0, budget=3.0, removed=(), invalid=(), verdict="FAIL"):
    sep_reg = [c for c in cases if c["separated"] and c["delta_pct"] > 2.0]
    return {
        "label": "t",
        "verdict": verdict,
        "mean_delta_pct": mean,
        "budget_pct": budget,
        "removed_cases": list(removed),
        "invalid_problems": list(invalid),
        "separated_regressions": [c["case"] for c in sep_reg],
        "layout_only_regressions": [c["case"] for c in sep_reg
                                    if c["executable"] == "body-identical"],
        "regressions_needing_review": [c["case"] for c in sep_reg
                                       if c["executable"] != "body-identical"],
        "cases": cases,
    }


def confirmation(cases, invalid=()):
    return {"verdict": "FAIL" if any(c["separated"] and c["delta_pct"] > 0 for c in cases) else "PASS",
            "invalid_problems": list(invalid), "cases": cases}


class Plan(unittest.TestCase):
    def test_budget_removed_and_invalid_are_final(self):
        cases = [case("a/b", 5.0, True)]
        self.assertEqual(confirm.plan(primary(cases, mean=3.5))[0], "FAIL")
        self.assertEqual(confirm.plan(primary(cases, removed=["x/y"]))[0], "FAIL")
        self.assertEqual(confirm.plan(primary(cases, invalid=["stale"], verdict="INVALID"))[0],
                         "INVALID")

    def test_layout_only_regressions_need_no_confirmation(self):
        cases = [case("a/b", 9.0, True, "body-identical"), case("a/c", 1.0, True)]
        self.assertEqual(confirm.plan(primary(cases)), ("LAYOUT_ONLY", []))

    def test_changed_code_regressions_are_confirmed_by_full_id(self):
        cases = [case("pool/create/10", 6.0, True), case("pool/create/100", 5.0, False),
                 case("journal/x", 4.0, True, "body-identical")]
        self.assertEqual(confirm.plan(primary(cases)), ("CONFIRM", ["pool/create/10"]))


class Decide(unittest.TestCase):
    def test_a_regression_that_does_not_reproduce_passes(self):
        p = primary([case("a/b", 6.0, True), case("a/c", 3.0, True)])
        c = confirmation([case("a/b", 1.5, False), case("a/c", -2.0, True)])
        verdict, record = confirm.decide(p, c)
        self.assertEqual(verdict, "PASS")
        self.assertEqual(record["not_reproduced"], ["a/b", "a/c"])

    def test_a_reproduced_regression_fails_whatever_its_size(self):
        p = primary([case("a/b", 6.0, True), case("a/c", 3.0, True)])
        c = confirmation([case("a/b", 0.8, True), case("a/c", 2.5, False)])
        verdict, record = confirm.decide(p, c)
        self.assertEqual(verdict, "FAIL")
        self.assertEqual([r["full_id"] for r in record["confirmed_regressions"]], ["a/b"])
        self.assertEqual(record["not_reproduced"], ["a/c"])

    def test_a_reproduced_regression_on_function_identical_code_is_reported(self):
        p = primary([case("a/b", 6.0, True), case("a/c", 3.0, True)])
        c = confirmation([case("a/b", 5.0, True), case("a/c", 2.5, True)])
        same = {"classification": "function-identical", "reason": "placement"}
        verdict, record = confirm.decide(p, c, {"a/b": same, "a/c": same})
        self.assertEqual(verdict, "PASS")
        self.assertIn("function-identical", record["reason"])
        self.assertEqual([r["full_id"] for r in record["function_identical_regressions"]],
                         ["a/b", "a/c"])
        self.assertEqual(record["failing_regressions"], [])
        self.assertEqual(len(record["confirmed_regressions"]), 2)

    def test_changed_unknown_or_unchecked_code_still_fails(self):
        p = primary([case("a/b", 6.0, True), case("a/c", 3.0, True), case("a/d", 4.0, True)])
        c = confirmation([case("a/b", 5.0, True), case("a/c", 2.5, True),
                          case("a/d", 4.0, True)])
        for other in ({"classification": "code-changed", "reason": "x"},
                      {"classification": "unknown", "reason": "no samples"}, None):
            hot = {"a/b": {"classification": "function-identical", "reason": "placement"}}
            if other is not None:
                hot["a/c"] = hot["a/d"] = other
            verdict, record = confirm.decide(p, c, hot)
            self.assertEqual(verdict, "FAIL")
            self.assertEqual([r["full_id"] for r in record["failing_regressions"]],
                             ["a/c", "a/d"])
            self.assertEqual(record["failing_regressions"][0]["hot"],
                             other["classification"] if other else "not checked")

    def test_reproduced_lists_the_cases_the_hot_check_must_see(self):
        cases = [case("a/b", 6.0, True), case("a/c", 3.0, True)]
        cases[0]["target"], cases[1]["target"] = "relay", "cache"
        p = primary(cases)
        c = confirmation([case("a/b", 5.0, True), case("a/c", 2.5, False)])
        self.assertEqual(confirm.reproduced(p, c), [("relay", "a/b")])
        self.assertEqual(confirm.reproduced(p, None), [])
        self.assertEqual(confirm.reproduced(primary(cases, mean=4.0), c), [])

    def test_an_unmeasured_or_invalid_confirmation_is_invalid(self):
        p = primary([case("a/b", 6.0, True), case("a/c", 3.0, True)])
        self.assertEqual(confirm.decide(p, None)[0], "INVALID")
        self.assertEqual(confirm.decide(p, confirmation([case("a/b", 0.0, False)]))[0], "INVALID")
        self.assertEqual(
            confirm.decide(p, confirmation([case("a/b", 0.0, False), case("a/c", 0.0, False)],
                                           invalid=["stale"]))[0],
            "INVALID")

    def test_the_budget_still_fails_without_a_confirmation_run(self):
        p = primary([case("a/b", 6.0, True)], mean=3.2)
        self.assertEqual(confirm.decide(p, None)[0], "FAIL")
        p = primary([case("a/b", 1.0, False)], verdict="PASS")
        self.assertEqual(confirm.decide(p, None), ("PASS", confirm.decide(p, None)[1]))
        self.assertEqual(confirm.decide(p, None)[1]["reason"], "no separated regression")


class Cli(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True)

    def test_cases_regex_and_verdict_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            (tmp / "cases.tsv").write_text("pooling\tpool/create/10\npooling\tpool/create/100\n"
                                           "cache\tcache/l2/x(1)\n")
            (tmp / "ids.txt").write_text("pool/create/10\ncache/l2/x(1)\n")
            self.assertEqual(self.run_cli("cases", str(tmp / "cases.tsv"), str(tmp / "ids.txt"),
                                          str(tmp / "kept.tsv")).returncode, 0)
            self.assertEqual((tmp / "kept.tsv").read_text(),
                             "pooling\tpool/create/10\ncache\tcache/l2/x(1)\n")
            regex = self.run_cli("regex", str(tmp / "ids.txt")).stdout.strip()
            self.assertEqual(regex, r"^(pool/create/10|cache/l2/x\(1\))$")

            p = primary([case("pool/create/10", 6.0, True)])
            (tmp / "primary.json").write_text(json.dumps(p))
            (tmp / "bench-gate-summary.txt").write_text("primary summary\n")
            plan = self.run_cli("plan", str(tmp / "primary.json"))
            self.assertEqual(plan.stdout.strip(), "pool/create/10")
            (tmp / "confirm.json").write_text(json.dumps(
                confirmation([case("pool/create/10", 3.0, True)])))
            out = self.run_cli("verdict", str(tmp / "primary.json"), str(tmp / "confirm.json"),
                               str(tmp))
            self.assertEqual(out.returncode, 1, out.stdout + out.stderr)
            final = json.loads((tmp / "bench-gate-final.json").read_text())
            self.assertEqual(final["verdict"], "FAIL")
            summary = (tmp / "bench-gate-summary.txt").read_text()
            self.assertIn("final verdict (second-build confirmation): FAIL", summary)
            self.assertIn("reproduced: pool/create/10 +3.00%", summary)
            listed = self.run_cli("reproduced", str(tmp / "primary.json"),
                                  str(tmp / "confirm.json"))
            self.assertEqual(listed.stdout, "\tpool/create/10\n")
            (tmp / "hot").mkdir()
            (tmp / "hot" / "0.json").write_text(json.dumps(
                {"full_id": "pool/create/10", "classification": "function-identical",
                 "reason": "placement"}))
            out = self.run_cli("verdict", str(tmp / "primary.json"), str(tmp / "confirm.json"),
                               str(tmp), str(tmp / "hot"))
            self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
            self.assertIn("reproduced: pool/create/10 +3.00% [function-identical, reported]",
                          (tmp / "bench-gate-summary.txt").read_text())
            os.remove(tmp / "confirm.json")
            missing = self.run_cli("verdict", str(tmp / "primary.json"),
                                   str(tmp / "confirm.json"), str(tmp))
            self.assertEqual(missing.returncode, 2)


class RecordedGate(unittest.TestCase):
    """The rule against the real output of gate r5 (2026-11-17), when present."""

    PATH = pathlib.Path("/home/admin/HDB/sprint/baselines/proxy/"
                        "pre3-criterion-hooks-drain-admin-20261117-r5/bench-gate.json")

    @unittest.skipUnless(PATH.exists(), "recorded gate output not on this host")
    def test_r5_needs_a_confirmation_of_its_changed_code_cases_only(self):
        p = json.loads(self.PATH.read_text())
        action, ids = confirm.plan(p)
        self.assertEqual(action, "CONFIRM")
        self.assertEqual(len(ids), len(p["regressions_needing_review"]))
        self.assertNotIn("protocol/query_text/short_select", ids, "body-identical: reported only")


if __name__ == "__main__":
    unittest.main()
