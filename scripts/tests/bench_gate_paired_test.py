#!/usr/bin/env python3
"""Tests for bench-gate-paired.py; synthetic fixtures only (Linux and cc required).

The test copies the frozen comparator beside the draft in a temporary directory,
compiles a tiny native ELF launcher, and exercises identity, ordering, freshness,
and inventory checks without building the product or running workloads.
"""
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]
DRAFT = SCRIPTS / "bench-gate-paired.py"
COMPARATOR = SCRIPTS / "bench-gate-compare.py"


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class PairedGateDraftTests(unittest.TestCase):
    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="paired-gate-test-"))
        self.work = self.tmp / "work"
        self.out = self.tmp / "out"
        self.work.mkdir()
        self.out.mkdir()
        self.run_dir = self.work / "target/criterion"
        self.run_dir.mkdir(parents=True)
        self.sibling = self.tmp / "bench-gate-compare.py"
        shutil.copy2(COMPARATOR, self.sibling)
        self.mod = load_module(self.draft_copy, "paired_draft")
        self.cases = ["grp/alpha", "grp/re.$case"]
        self.targets = {self.cases[0]: "pooling", self.cases[1]: "routing"}
        for arm in ("base", "cand"):
            (self.out / f"cases-{arm}.tsv").write_text(
                "\n".join(f"{self.targets[c]}\t{c}" for c in self.cases) + "\n")
            target = self.work / arm / "target"
            target.mkdir(parents=True)
            exe = target / f"{arm}-bench"
            shutil.copy2(self.launcher, exe)
            exe.chmod(0o755)
            (self.out / f"executables-{arm}.txt").write_text(
                f"pooling {self.sha(exe)} {exe}\n"
                f"routing {self.sha(exe)} {exe}\n")

    @staticmethod
    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    @classmethod
    def setUpClass(cls):
        cls.cc = shutil.which("cc")
        if not cls.cc:
            raise RuntimeError("cc is required for native process identity tests")
        cls.root = pathlib.Path(tempfile.mkdtemp(prefix="paired-gate-cc-"))
        cls.draft_copy = cls.root / "bench-gate-paired-draft.py"
        shutil.copy2(DRAFT, cls.draft_copy)
        shutil.copy2(COMPARATOR, cls.root / "bench-gate-compare.py")
        src = cls.root / "launcher.c"
        src.write_text(r'''#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
int main(int argc, char **argv) {
  if (argc < 2) return 64;
  pid_t p = fork();
  if (p == 0) { char *s = getenv("PAIRED_FIXTURE_SCRIPT"); if (!s) _exit(126);
    execlp("python3", "python3", s, argv[1], argv[2], argv[3], argv[4], NULL); _exit(127); }
  int st = 0; waitpid(p, &st, 0); return WIFEXITED(st) ? WEXITSTATUS(st) : 125;
}
''')
        cls.launcher = cls.root / "native-launcher"
        subprocess.run([cls.cc, "-O2", "-Wall", "-Werror", str(src), "-o", str(cls.launcher)], check=True)
        cls.fixture = cls.root / "fixture.py"
        cls.fixture.write_text(r'''import json, pathlib, sys, os, time, re
case = os.environ["PAIRED_FIXTURE_CASE"]
tree = pathlib.Path(os.environ["PAIRED_FIXTURE_TREE"])
label = sys.argv[3]
pattern = sys.argv[4]
if not re.fullmatch(pattern, case): raise SystemExit(8)
mode = os.environ.get("PAIRED_FIXTURE_MODE", "ok")
root = tree / "target/criterion" / ("case-" + case.replace("/", "_")) / label
root.mkdir(parents=True, exist_ok=True)
d = root
if mode == "nonzero": raise SystemExit(7)
if mode == "sleep": time.sleep(5)
if mode == "wrong": full = "wrong/id"
else: full = case
(d / "benchmark.json").write_text(json.dumps({"full_id": full}))
(d / "estimates.json").write_text(json.dumps({"median":{"point_estimate":100,"confidence_interval":{"lower_bound":99,"upper_bound":101}}}))
(d / "sample.json").write_text(json.dumps({"iters":[1],"times":[100]}))
if mode == "missing": (d / "sample.json").unlink()
if mode == "tukey": (d / "tukey.json").write_text(json.dumps({"ok": True}))
if mode == "extra": (d / "unexpected.json").write_text("{}")
if mode == "stale":
    for p in d.iterdir(): os.utime(p, (1, 1))
if mode == "rewrite":
    old = tree / "target/criterion" / ("case-" + case.replace("/", "_")) / "gate-rewrite-r1"
    old.mkdir(parents=True, exist_ok=True); (old / "old.json").write_text("changed")
''')
        os.environ["PAIRED_FIXTURE_SCRIPT"] = str(cls.fixture)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.root, ignore_errors=True)

    def test_exact_regex_and_process_identity(self):
        # The paired command escapes regex metacharacters and checks /proc/exe + SHA.
        executable = self.work / "base/target/base-bench"
        digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[1]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        log = (self.out / "base.log").open("w")
        try:
            record = self.mod.execute(self.work / "base", executable, digest,
                                      self.cases[1], "t", 1, log)
        finally:
            log.close()
        self.assertEqual(record["sha256"], digest)
        self.assertEqual(record["filter"], r"^grp/re\.\$case$")
        self.assertEqual(len(record["raw"]), 3)

    def test_three_round_abba_order_and_append_logs(self):
        calls = []
        def fake_execute(tree, executable, digest, case, label, round_number, log, timeout):
            calls.append((tree.name, round_number, case))
            log.write(f"Paired execution: {case}\n")
            return {"raw": {}}
        with mock.patch.object(self.mod, "execute", side_effect=fake_execute):
            self.mod.run(self.work / "base", self.work / "cand", self.out, "order", 3)
        expected = []
        for r in range(1, 4):
            for case in sorted(self.cases):
                for arm in (("base", "cand") if r % 2 else ("cand", "base")):
                    expected.append((arm, r, case))
        self.assertEqual(calls, expected)
        self.assertIn("Paired execution:", (self.out / "base-r1.log").read_text())

    def test_same_id_unequal_target_inventory_rejected(self):
        (self.out / "cases-cand.tsv").write_text("routing\tgrp/alpha\nrouting\tgrp/re.$case\n")
        with self.assertRaises((ValueError, self.mod.CaseError)):
            self.mod.run(self.work / "base", self.work / "cand", self.out, "t2", 3)

    def test_empty_inventory_rejected(self):
        (self.out / "cases-base.tsv").write_text("")
        with self.assertRaises((ValueError, self.mod.CaseError)):
            self.mod.run(self.work / "base", self.work / "cand", self.out, "t3", 3)

    def test_changed_hash_rejected_before_child(self):
        executable = self.work / "base/target/base-bench"
        digest = self.sha(executable)
        executable.write_bytes(executable.read_bytes() + b"changed")
        log = (self.out / "changed.log").open("w")
        try:
            with self.assertRaises((ValueError, self.mod.CaseError)):
                self.mod.execute(self.work / "base", executable, digest, self.cases[0], "t4", 1, log)
        finally:
            log.close()

    def test_optional_tukey_file_is_accepted(self):
        executable = self.work / "base/target/base-bench"
        digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[0]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        os.environ["PAIRED_FIXTURE_MODE"] = "tukey"
        log = (self.out / "tukey.log").open("w")
        try:
            record = self.mod.execute(self.work / "base", executable, digest,
                                      self.cases[0], "tukey", 1, log)
        finally:
            log.close(); os.environ.pop("PAIRED_FIXTURE_MODE", None)
        self.assertEqual(len(record["raw"]), 4)

    def test_unknown_raw_extra_is_rejected(self):
        executable = self.work / "base/target/base-bench"
        digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[0]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        os.environ["PAIRED_FIXTURE_MODE"] = "extra"
        log = (self.out / "extra.log").open("w")
        try:
            with self.assertRaises((ValueError, self.mod.CaseError)):
                self.mod.execute(self.work / "base", executable, digest,
                                 self.cases[0], "extra", 1, log)
        finally:
            log.close(); os.environ.pop("PAIRED_FIXTURE_MODE", None)

    def test_stale_label_and_wrong_raw_case_controls(self):
        # A pre-existing output is rejected by run() before any child invocation.
        stale = self.work / "base/target/criterion/gate-stale-r1/old"
        stale.mkdir(parents=True)
        with self.assertRaises((ValueError, self.mod.CaseError)):
            self.mod.run(self.work / "base", self.work / "cand", self.out, "stale", 3)

    def test_wrong_full_id_is_rejected(self):
        executable = self.work / "base/target/base-bench"
        digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[0]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        os.environ["PAIRED_FIXTURE_MODE"] = "wrong"
        log = (self.out / "wrong.log").open("w")
        try:
            with self.assertRaises((ValueError, self.mod.CaseError)):
                self.mod.execute(self.work / "base", executable, digest,
                                 self.cases[0], "wrong", 1, log)
        finally:
            log.close(); os.environ.pop("PAIRED_FIXTURE_MODE", None)

    def test_nonzero_child_is_rejected(self):
        executable = self.work / "base/target/base-bench"
        digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[0]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        os.environ["PAIRED_FIXTURE_MODE"] = "nonzero"
        log = (self.out / "nonzero.log").open("w")
        try:
            with self.assertRaises((ValueError, self.mod.CaseError)):
                self.mod.execute(self.work / "base", executable, digest,
                                 self.cases[0], "nonzero", 1, log)
        finally:
            log.close(); os.environ.pop("PAIRED_FIXTURE_MODE", None)

    def run_mode(self, mode, label="mode", timeout=2):
        executable = self.work / "base/target/base-bench"; digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[0]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        os.environ["PAIRED_FIXTURE_MODE"] = mode
        log = (self.out / f"{mode}.log").open("w")
        try:
            return self.mod.execute(self.work / "base", executable, digest,
                                    self.cases[0], label, 1, log, timeout)
        finally:
            log.close(); os.environ.pop("PAIRED_FIXTURE_MODE", None)

    def test_timeout_has_bounded_cleanup_record(self):
        with self.assertRaises(self.mod.CaseError) as ctx:
            self.run_mode("sleep", "timeout", 0.1)
        self.assertIn("cleanup", ctx.exception.record)
        self.assertIn("exit", ctx.exception.record)
        self.assertIn("timed out", str(ctx.exception))
        self.assertTrue(any(e.get("reaped") for e in ctx.exception.record["cleanup"]))
        self.assertTrue(any(e.get("group_gone") for e in ctx.exception.record["cleanup"]))

    def test_external_sigterm_has_cleanup_record(self):
        timer = threading.Timer(0.15, lambda: os.kill(os.getpid(), 15))
        timer.start()
        try:
            with self.assertRaises(self.mod.CaseError) as ctx:
                self.run_mode("sleep", "external", 2)
            self.assertIn("cleanup", ctx.exception.record)
            self.assertIn("received signal", str(ctx.exception))
            self.assertTrue(any(e.get("reaped") for e in ctx.exception.record["cleanup"]))
            self.assertEqual(ctx.exception.record["signal"], 15)
            self.assertTrue(any(e.get("group_gone") for e in ctx.exception.record["cleanup"]))
        finally:
            timer.cancel()

    def test_missing_stale_and_rewrite_outputs_rejected(self):
        for mode, label in (("missing", "missing"), ("stale", "stale")):
            with self.assertRaises(self.mod.CaseError):
                self.run_mode(mode, label)

    def test_earlier_round_rewrite_is_rejected(self):
        prior = self.work / "base/target/criterion/case-grp_alpha/gate-rewrite-r1"
        prior.mkdir(parents=True)
        old = prior / "old.json"; old.write_text("original")
        executable = self.work / "base/target/base-bench"; digest = self.sha(executable)
        os.environ["PAIRED_FIXTURE_CASE"] = self.cases[0]
        os.environ["PAIRED_FIXTURE_TREE"] = str(self.work / "base")
        os.environ["PAIRED_FIXTURE_MODE"] = "rewrite"
        log = (self.out / "rewrite.log").open("w")
        try:
            with self.assertRaises(self.mod.CaseError) as ctx:
                self.mod.execute(self.work / "base", executable, digest,
                                 self.cases[0], "rewrite", 2, log, 2)
            self.assertIn("earlier", str(ctx.exception).lower())
        finally:
            log.close(); os.environ.pop("PAIRED_FIXTURE_MODE", None)

    def assert_late_mutation_rejected(self, kind):
        first = self.tmp / "completed-result.json"
        calls = []

        def fake_execute(*args):
            calls.append(args[3])
            if len(calls) == 1:
                first.write_text("original")
                return {"raw": {str(first): {"sha256": self.sha(first),
                                            "mtime_ns": first.stat().st_mtime_ns}}}
            if len(calls) == 2:
                stat = first.stat()
                if kind == "sha":
                    first.write_text("modified")
                    os.utime(first, ns=(stat.st_atime_ns, stat.st_mtime_ns))
                else:
                    os.utime(first, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
            return {"raw": {}}

        with mock.patch.object(self.mod, "execute", side_effect=fake_execute):
            with self.assertRaisesRegex(ValueError, "completed invocation artifact changed"):
                self.mod.run(self.work / "base", self.work / "cand", self.out, "late", 1)

    def test_late_content_mutation_rejected(self):
        self.assert_late_mutation_rejected("sha")

    def test_late_mtime_mutation_rejected(self):
        self.assert_late_mutation_rejected("mtime")

    def test_zero_selected_target_is_not_executed(self):
        calls = []
        extra = self.work / "base/target/unused-bench"
        shutil.copy2(self.work / "base/target/base-bench", extra)
        with (self.out / "executables-base.txt").open("a") as f:
            f.write(f"unused {self.sha(extra)} {extra}\n")
        with (self.out / "executables-cand.txt").open("a") as f:
            cand = self.work / "cand/target/unused-bench"; shutil.copy2(extra, cand)
            f.write(f"unused {self.sha(cand)} {cand}\n")
        def fake(*args): calls.append(args[3]); return {"raw": {}}
        with mock.patch.object(self.mod, "execute", side_effect=fake):
            self.mod.run(self.work / "base", self.work / "cand", self.out, "zero", 1)
        self.assertNotIn("unused", calls)


if __name__ == "__main__":
    unittest.main(verbosity=2)
