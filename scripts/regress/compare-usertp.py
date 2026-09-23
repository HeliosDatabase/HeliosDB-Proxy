#!/usr/bin/env python3
"""Compare P-01 user-path results: baseline vs candidate binary, N interleaved
passes each (produced by scripts/regress/bench-usertp.sh, normally driven by
scripts/perf-gate-userpath.sh).

Per (mode, clients, workload) cell: median-over-passes TPS / p99 latency /
first-row latency for base and candidate, and the base->candidate delta.

A cell that is 0 TPS (or 0 p99) on BOTH binaries is excluded from every
delta/aggregate/gate computation and a warning is printed — a 0/0 delta is
undefined, not "no regression". The tr07 harness hit exactly this for
transaction/64 (a pre-existing connect-burst failure, sprinter 5600bb2bced4).
A cell where only the CANDIDATE collapsed to 0 while the baseline had
throughput is NOT excluded: it is scored as a full -100% regression, because
silently calling that "n/a" is exactly the kind of miss this harness exists
to catch (Criterion missed the 2026-09-21 TR-07 async-lock convoy; see
CLAUDE.md gate 3).

Exit code: 1 if any proxy-mode (mode != "direct") committed_write cell
regresses beyond BUDGET_PCT (default 3%) in TPS or p99, medians over passes;
0 otherwise (a gate with nothing ungated to check — e.g. every committed_write
cell excluded as 0/0 — is reported but is not itself a failure).

Env:
  OUT         results directory (default /tmp/bench-usertp-tr07b)
  PREFIX      result-file prefix; matches the <label> passed to
              perf-gate-userpath.sh / bench-usertp.sh (default "tr07")
  PASSES      number of interleaved passes to load (default 2)
  CAND        candidate label (default "cand"); baseline label is always "base"
  BUDGET_PCT  regression budget in percent, TPS and p99 (default 3.0)
  JSON_OUT    path for the machine-readable summary
              (default $OUT/$PREFIX-compare-summary.json)
"""
import json
import os
import statistics
import sys
from datetime import datetime, timezone

OUT = os.environ.get("OUT", "/tmp/bench-usertp-tr07b")
PREFIX = os.environ.get("PREFIX", "tr07")
PASSES = int(os.environ.get("PASSES", "2"))
CAND = os.environ.get("CAND", "cand")
BASE = "base"
BUDGET_PCT = float(os.environ.get("BUDGET_PCT", "3.0"))
JSON_OUT = os.environ.get("JSON_OUT", os.path.join(OUT, f"{PREFIX}-compare-summary.json"))


def load(label):
    runs = []
    for pass_no in range(1, PASSES + 1):
        f = os.path.join(OUT, f"{PREFIX}-{label}-p{pass_no}-results.json")
        if os.path.exists(f):
            with open(f) as fh:
                runs.append(json.load(fh))
    return runs


def key(r):
    return (r["mode"], r["clients"], r["workload"])


def agg(runs):
    out = {}
    for run in runs:
        for r in run:
            out.setdefault(key(r), []).append(r)
    return out


def med(rs, f):
    vals = [f(r) for r in rs if f(r) is not None]
    return statistics.median(vals) if vals else None


def pct(r, i):
    lat = r.get("latency_ms") or []
    return lat[i] if len(lat) > i else None


def delta(b, c):
    """(delta_pct or None, status). status:
    'ok'                 both non-zero -> normal percent delta
    'excluded-zero-both' both 0 -> undefined, excluded from every computation
    'baseline-zero'      base 0, cand > 0 -> can't express as a %, not a regression
    'cand-zero'          base > 0, cand 0 -> scored as a full -100% regression
    'missing'            one or both sides absent (no result for this cell)
    """
    if b is None or c is None:
        return None, "missing"
    if b == 0 and c == 0:
        return None, "excluded-zero-both"
    if b == 0:
        return None, "baseline-zero"
    if c == 0:
        return -100.0, "cand-zero"
    return (c - b) / b * 100, "ok"


base_runs, cand_runs = agg(load(BASE)), agg(load(CAND))

cells = []
warnings = []
tps_deltas, p99_deltas = [], []
gated_failures = []

fmt = lambda v, w: (f"{v:{w}.1f}" if isinstance(v, (int, float)) else f"{'n/a':>{w}s}")

print(
    f"{'mode':12s} {'cl':>3s} {'workload':14s} {'tps base':>10s} {'tps cand':>10s} {'d%':>7s} "
    f"{'p99 base':>9s} {'p99 cand':>9s} {'d%':>7s} {'frow base':>9s} {'frow cand':>9s} "
    f"{'fail b/c':>8s} {'verdict':>8s}"
)

for k in sorted(set(base_runs) | set(cand_runs)):
    mode, clients, workload = k
    b, c = base_runs.get(k, []), cand_runs.get(k, [])
    tb, tc = med(b, lambda r: r["tps"]), med(c, lambda r: r["tps"])
    pb, pc = med(b, lambda r: pct(r, 3)), med(c, lambda r: pct(r, 3))
    fb, fc = med(b, lambda r: r.get("first_row_ms")), med(c, lambda r: r.get("first_row_ms"))
    failb = sum(r.get("failed_txns", 0) for r in b)
    failc = sum(r.get("failed_txns", 0) for r in c)

    dt, tstatus = delta(tb, tc)
    dp, pstatus = delta(pb, pc)
    gated = mode != "direct" and workload == "committed_write"

    if tstatus == "excluded-zero-both" or pstatus == "excluded-zero-both":
        verdict = "EXCL"
        warnings.append(
            f"WARN: excluding {mode} clients={clients} workload={workload} from every "
            f"delta/gate computation — 0 TPS/p99 on both binaries "
            f"(tps base={tb} cand={tc}, p99 base={pb} cand={pc})"
        )
    elif tstatus == "missing" or pstatus == "missing":
        verdict = "N/A"
    else:
        if mode != "direct":
            # matches the historical "proxy modes only" aggregate: every
            # non-direct, non-excluded cell (read and committed_write alike).
            if dt is not None:
                tps_deltas.append(dt)
            if dp is not None:
                p99_deltas.append(dp)
        tps_bad = dt is not None and dt < -BUDGET_PCT
        p99_bad = dp is not None and dp > BUDGET_PCT
        verdict = "FAIL" if (tps_bad or p99_bad) else "PASS"
        if gated and verdict == "FAIL":
            gated_failures.append(
                {
                    "mode": mode,
                    "clients": clients,
                    "workload": workload,
                    "tps_base": tb,
                    "tps_cand": tc,
                    "tps_delta_pct": dt,
                    "p99_base": pb,
                    "p99_cand": pc,
                    "p99_delta_pct": dp,
                }
            )

    print(
        f"{mode:12s} {clients:>3d} {workload:14s} {fmt(tb,10)} {fmt(tc,10)} {fmt(dt,7)} "
        f"{fmt(pb,9)} {fmt(pc,9)} {fmt(dp,7)} {fmt(fb,9)} {fmt(fc,9)} "
        f"{failb:>3d}/{failc:<4d} {verdict:>8s}"
    )

    cells.append(
        {
            "mode": mode,
            "clients": clients,
            "workload": workload,
            "tps_base": tb,
            "tps_cand": tc,
            "tps_delta_pct": dt,
            "p99_base": pb,
            "p99_cand": pc,
            "p99_delta_pct": dp,
            "first_row_ms_base": fb,
            "first_row_ms_cand": fc,
            "failed_txns_base": failb,
            "failed_txns_cand": failc,
            "gated": gated,
            "verdict": verdict,
        }
    )

for w in warnings:
    print(w, file=sys.stderr)

if tps_deltas:
    print(
        f"\nproxy modes only: mean TPS delta {statistics.fmean(tps_deltas):+.2f}% "
        f"(median {statistics.median(tps_deltas):+.2f}%), mean p99 delta "
        f"{statistics.fmean(p99_deltas):+.2f}% (median {statistics.median(p99_deltas):+.2f}%)  "
        f"[budget {BUDGET_PCT:g}% on committed TPS and p99]"
    )

if gated_failures:
    print(f"\nGATE FAIL: {len(gated_failures)} committed_write cell(s) exceed the {BUDGET_PCT:g}% budget:")
    for f in gated_failures:
        print(
            f"  {f['mode']} clients={f['clients']}: tps {f['tps_base']:.1f} -> {f['tps_cand']:.1f} "
            f"({f['tps_delta_pct']:+.1f}%), p99 {f['p99_base']:.2f} -> {f['p99_cand']:.2f} "
            f"({f['p99_delta_pct']:+.1f}%)"
        )
else:
    print("\nGATE PASS: no gated committed_write cell exceeds the budget.")

exit_code = 1 if gated_failures else 0

summary = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "out_dir": OUT,
    "prefix": PREFIX,
    "passes": PASSES,
    "base_label": BASE,
    "cand_label": CAND,
    "budget_pct": BUDGET_PCT,
    "cells": cells,
    "gated_failures": gated_failures,
    "aggregate_proxy_modes": {
        "tps_delta_mean_pct": statistics.fmean(tps_deltas) if tps_deltas else None,
        "tps_delta_median_pct": statistics.median(tps_deltas) if tps_deltas else None,
        "p99_delta_mean_pct": statistics.fmean(p99_deltas) if p99_deltas else None,
        "p99_delta_median_pct": statistics.median(p99_deltas) if p99_deltas else None,
    },
    "exit_code": exit_code,
}
os.makedirs(os.path.dirname(JSON_OUT) or ".", exist_ok=True)
with open(JSON_OUT, "w") as fh:
    json.dump(summary, fh, indent=1)
print(f"\nwrote {JSON_OUT}")

sys.exit(exit_code)
