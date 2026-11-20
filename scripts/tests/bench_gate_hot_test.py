#!/usr/bin/env python3
"""Tests for scripts/bench-gate-hot.py (function-level attribution of a case).

Run: python3 scripts/tests/bench_gate_hot_test.py
"""
import importlib.util
import pathlib
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("hot", HERE.parent / "bench-gate-hot.py")
hot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hot)

# Three functions: [0x100, 0x140) "a", [0x140, 0x200) "b", [0x240, 0x280) "c".
BODIES = [(0x100, 0x140, "a"), (0x140, 0x200, "b"), (0x240, 0x280, "c")]


def arm(identical, changed=0, outside=0, elsewhere=0):
    return {"identical": identical, "changed": changed, "outside_functions": outside,
            "elsewhere": elsewhere}


class Attribute(unittest.TestCase):
    def test_samples_are_split_by_the_function_holding_them(self):
        samples = {0x100: 5, 0x13f: 1, 0x140: 7, 0x1ff: 2, 0x27f: 3}
        got = hot.attribute(samples, BODIES, {"a", "c"})
        self.assertEqual((got["identical"], got["changed"], got["outside_functions"]),
                         (9, 9, 0))
        self.assertEqual(got["changed_functions"], [{"start": "140", "samples": 9}])

    def test_an_address_in_no_function_is_not_identical(self):
        samples = {0xff: 1, 0x200: 2, 0x23f: 3, 0x280: 4, 0x110: 10}
        got = hot.attribute(samples, BODIES, {"a", "b", "c"})
        self.assertEqual((got["identical"], got["changed"], got["outside_functions"]),
                         (10, 0, 10))


class Decide(unittest.TestCase):
    def test_identical_when_both_arms_stay_under_the_limit(self):
        kind, _reason, shares = hot.decide(arm(9960, 40), arm(9000, 0, 0, 1000), 0.5, 2000)
        self.assertEqual(kind, "function-identical")
        self.assertAlmostEqual(shares["base"], 0.4)
        self.assertEqual(shares["cand"], 0.0)

    def test_changed_when_either_arm_exceeds_it(self):
        for base, cand in ((arm(9900, 100), arm(10000)), (arm(10000), arm(9900, 100)),
                           (arm(10000), arm(9900, 0, 100))):
            kind, reason, _shares = hot.decide(base, cand, 0.5, 2000)
            self.assertEqual(kind, "code-changed", reason)

    def test_unknown_without_enough_samples(self):
        kind, reason, _shares = hot.decide(arm(10000), arm(1999), 0.5, 2000)
        self.assertEqual(kind, "unknown")
        self.assertIn("cand", reason)


if __name__ == "__main__":
    unittest.main()
