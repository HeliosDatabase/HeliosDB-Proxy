#!/usr/bin/env python3
"""Synthetic tests for scripts/bench-gate-compare.py (no cargo, no benchmarks).

Run: python3 scripts/tests/bench_gate_compare_test.py
Each case builds two fake Criterion output trees in a temporary directory and checks
the comparator's verdict, exit code and report fields.
"""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest

COMPARE = pathlib.Path(__file__).resolve().parents[1] / "bench-gate-compare.py"
LABEL = "t"
CASES = {"g/a": "pooling", "g/b": "pooling", "h/c": "relay"}


def write_case(root, full_id, r, ns, *, raw=None, sample=None):
    d = pathlib.Path(root) / full_id.replace("/", os.sep) / f"gate-{LABEL}-r{r}"
    d.mkdir(parents=True, exist_ok=True)
    (d / "benchmark.json").write_text(json.dumps({"full_id": full_id}))
    est = raw if raw is not None else json.dumps(
        {"median": {"point_estimate": ns, "confidence_interval": {"lower_bound": ns * 0.99, "upper_bound": ns * 1.01}}})
    (d / "estimates.json").write_text(est)
    (d / "sample.json").write_text(json.dumps(sample if sample is not None else {"iters": [1, 2], "times": [ns, 2 * ns]}))


class Fixture:
    def __init__(self, base=None, cand=None, base_cases=None, cand_cases=None, classes=None, rounds=3):
        self.tmp = tempfile.TemporaryDirectory()
        t = pathlib.Path(self.tmp.name)
        self.base, self.cand, self.out = t / "base", t / "cand", t / "out"
        self.rounds = rounds
        self.base_cases = base_cases or dict(CASES)
        self.cand_cases = cand_cases or dict(CASES)
        base = base or {c: [100.0] * rounds for c in self.base_cases}
        cand = cand or {c: [100.0] * rounds for c in self.cand_cases}
        for root, values in ((self.base, base), (self.cand, cand)):
            for case, per_round in values.items():
                for r, ns in enumerate(per_round, 1):
                    if ns is not None:
                        write_case(root, case, r, ns)
        for name, cases in (("base.tsv", self.base_cases), ("cand.tsv", self.cand_cases)):
            (t / name).write_text("".join(f"{tg}\t{c}\n" for c, tg in cases.items()))
        (t / "classes.json").write_text(json.dumps(classes or {}))
        self.t = t

    def __del__(self):
        self.tmp.cleanup()

    def run(self, *extra):
        p = subprocess.run([sys.executable, str(COMPARE), "--base-root", str(self.base), "--cand-root", str(self.cand),
                            "--label", LABEL, "--rounds", str(self.rounds), "--base-cases", str(self.t / "base.tsv"),
                            "--cand-cases", str(self.t / "cand.tsv"), "--classes", str(self.t / "classes.json"),
                            "--out", str(self.out), *extra], capture_output=True, text=True)
        report = json.loads((self.out / "bench-gate.json").read_text())
        return p.returncode, report


IDENT = {"classification": "body-identical"}
CHANGED = {"classification": "code-changed"}


class CompareTest(unittest.TestCase):
    def test_flat_run_passes(self):
        rc, rep = Fixture().run()
        self.assertEqual((rc, rep["verdict"], rep["matched"]), (0, "PASS", 3))

    def test_separated_regression_on_changed_executable_fails_and_needs_review(self):
        f = Fixture(cand={"g/a": [110, 111, 112], "g/b": [100] * 3, "h/c": [100] * 3},
                    classes={"pooling": CHANGED, "relay": IDENT})
        rc, rep = f.run()
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual(rep["regressions_needing_review"], ["g/a"])

    def test_separated_regression_on_identical_executable_still_fails_with_layout_evidence(self):
        f = Fixture(cand={"g/a": [110, 111, 112], "g/b": [100] * 3, "h/c": [100] * 3},
                    classes={"pooling": IDENT, "relay": IDENT})
        rc, rep = f.run("--budget", "10")
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual(rep["layout_only_regressions"], ["g/a"])
        self.assertIn("layout class", rep["attribution"])

    def test_overlapping_rounds_are_not_separated(self):
        f = Fixture(base={"g/a": [100, 120, 100], "g/b": [100] * 3, "h/c": [100] * 3},
                    cand={"g/a": [110, 111, 112], "g/b": [100] * 3, "h/c": [100] * 3})
        rc, rep = f.run("--budget", "10")
        self.assertEqual((rc, rep["verdict"], rep["separated_regressions"]), (0, "PASS", []))

    def test_mean_over_budget_fails(self):
        f = Fixture(base={c: [100, 120, 100] for c in CASES}, cand={c: [104, 105, 106] for c in CASES})
        rc, rep = f.run()
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertAlmostEqual(rep["mean_delta_pct"], 5.0)

    def test_boundaries_exactly_at_thresholds_pass(self):
        # every case +3.0 % (mean == budget) and exactly 2.0 % separated is not "> sep"
        f = Fixture(base={"g/a": [100] * 3, "g/b": [100] * 3, "h/c": [100] * 3},
                    cand={"g/a": [102, 102, 102], "g/b": [103.5, 103.5, 103.5], "h/c": [103.5, 103.5, 103.5]})
        rc, rep = f.run("--sep", "3.5")
        self.assertEqual((rc, rep["verdict"]), (0, "PASS"))
        self.assertAlmostEqual(rep["mean_delta_pct"], 3.0)

    def test_missing_case_in_one_round_is_invalid(self):
        rc, rep = Fixture(cand={"g/a": [100, 100, None], "g/b": [100] * 3, "h/c": [100] * 3}).run()
        self.assertEqual((rc, rep["verdict"]), (2, "INVALID"))

    def test_missing_case_everywhere_is_invalid(self):
        rc, rep = Fixture(cand={"g/a": [100] * 3, "g/b": [100] * 3}).run()
        self.assertEqual((rc, rep["verdict"]), (2, "INVALID"))

    def test_malformed_nan_and_duplicate_key_outputs_are_invalid(self):
        for raw in ("{not json", '{"median": {"point_estimate": NaN, "confidence_interval": {"lower_bound": 1, "upper_bound": 2}}}',
                    '{"median": {"point_estimate": 100, "point_estimate": 100, "confidence_interval": {"lower_bound": 99, "upper_bound": 101}}}',
                    '{"median": {"point_estimate": -1, "confidence_interval": {"lower_bound": -2, "upper_bound": 0}}}'):
            f = Fixture()
            write_case(f.cand, "g/a", 2, 100, raw=raw)
            rc, rep = f.run()
            self.assertEqual((rc, rep["verdict"]), (2, "INVALID"), raw)

    def test_bad_samples_are_invalid(self):
        f = Fixture()
        write_case(f.cand, "h/c", 1, 100, sample={"iters": [1, 2], "times": [100]})
        self.assertEqual(f.run()[0], 2)

    def test_stale_outputs_are_invalid(self):
        f = Fixture()
        rc, rep = f.run("--start", str(time.time() + 3600))
        self.assertEqual((rc, rep["verdict"]), (2, "INVALID"))
        self.assertTrue(any("stale" in p for p in rep["invalid_problems"]))

    def test_extra_round_output_and_undeclared_case_are_invalid(self):
        f = Fixture()
        write_case(f.cand, "g/zz", 1, 100)
        self.assertEqual(f.run()[0], 2)

    def test_removed_case_fails_and_new_case_is_reported(self):
        cand_cases = {"g/a": "pooling", "g/b": "pooling", "h/new": "relay"}
        f = Fixture(cand_cases=cand_cases, cand={c: [100] * 3 for c in cand_cases})
        rc, rep = f.run()
        self.assertEqual((rc, rep["verdict"]), (1, "FAIL"))
        self.assertEqual((rep["removed_cases"], rep["new_cases"], rep["matched"]), (["h/c"], ["h/new"], 2))

    def test_new_case_alone_passes(self):
        cand_cases = dict(CASES, **{"h/new": "relay"})
        f = Fixture(cand_cases=cand_cases, cand={c: [100] * 3 for c in cand_cases})
        rc, rep = f.run()
        self.assertEqual((rc, rep["verdict"], rep["new_cases"]), (0, "PASS", ["h/new"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
