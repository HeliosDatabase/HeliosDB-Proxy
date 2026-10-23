#!/usr/bin/env python3
"""Synthetic tests for scripts/regress/compare-usertp.py (no Docker, no backend).

Run: python3 scripts/tests/compare_usertp_test.py
Each case writes base/candidate result files shaped like
scripts/regress/bench-usertp.sh's into a temporary directory and checks the
comparator's protocol report, warnings and exit code.
"""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

COMPARE = pathlib.Path(__file__).resolve().parents[1] / "regress" / "compare-usertp.py"
PREFIX = "t"
PASSES = 2


def row(mode, clients, workload, tps, p99, protocol=None):
    r = {
        "mode": mode,
        "clients": clients,
        "workload": workload,
        "tps": tps,
        "failed_txns": 0,
        "latency_ms": [1.0, 1.0, 1.5, p99, p99 * 2],
        "rss_kb": 0,
        "cpu_percent": 0,
        "first_row_ms": 0.5,
        "checkpoints": 0,
    }
    if protocol is not None:
        r["protocol"] = protocol
    return r


def rows(protocol=None, write_tps=1000.0):
    return [
        row("session", 16, "read", 5000.0, 2.0, protocol),
        row("session", 16, "committed_write", write_tps, 4.0, protocol),
    ]


class Fixture:
    def __init__(self, base_rows, cand_rows):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = pathlib.Path(self.tmp.name)
        for label, data in (("base", base_rows), ("cand", cand_rows)):
            for p in range(1, PASSES + 1):
                path = self.out / f"{PREFIX}-{label}-p{p}-results.json"
                path.write_text(json.dumps(data))

    def __del__(self):
        self.tmp.cleanup()

    def run(self):
        env = dict(os.environ, OUT=str(self.out), PREFIX=PREFIX, PASSES=str(PASSES), CAND="cand")
        env.pop("JSON_OUT", None)
        env.pop("BUDGET_PCT", None)
        p = subprocess.run([sys.executable, str(COMPARE)], capture_output=True, text=True, env=env)
        summary = json.loads((self.out / f"{PREFIX}-compare-summary.json").read_text())
        return p, summary


class CompareUsertpTest(unittest.TestCase):
    def test_prepared_on_both_sides_is_reported(self):
        p, summary = Fixture(rows("prepared"), rows("prepared")).run()
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(summary["protocol_base"], ["prepared"])
        self.assertEqual(summary["protocol_cand"], ["prepared"])
        self.assertTrue(p.stdout.startswith("pgbench protocol: base=prepared cand=prepared\n"), p.stdout)
        self.assertNotIn("WARN", p.stderr)

    def test_rows_without_a_protocol_count_as_simple(self):
        p, summary = Fixture(rows(), rows()).run()
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(summary["protocol_base"], ["simple"])
        self.assertEqual(summary["protocol_cand"], ["simple"])
        self.assertTrue(p.stdout.startswith("pgbench protocol: base=simple cand=simple\n"), p.stdout)

    def test_mismatched_protocols_warn_without_changing_the_exit_code(self):
        p, summary = Fixture(rows("simple"), rows("prepared")).run()
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertEqual(summary["exit_code"], 0)
        self.assertEqual((summary["protocol_base"], summary["protocol_cand"]), (["simple"], ["prepared"]))
        self.assertIn("WARN: base ran pgbench protocol ['simple'], cand ran ['prepared']", p.stderr)
        self.assertTrue(p.stdout.startswith("pgbench protocol: base=simple cand=prepared\n"), p.stdout)

    def test_committed_write_regression_still_fails_the_gate(self):
        p, summary = Fixture(rows("prepared"), rows("prepared", write_tps=900.0)).run()
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)
        self.assertEqual(summary["exit_code"], 1)
        self.assertEqual([(f["mode"], f["clients"]) for f in summary["gated_failures"]], [("session", 16)])
        self.assertIn("GATE FAIL", p.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
