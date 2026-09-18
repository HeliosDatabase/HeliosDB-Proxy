#!/usr/bin/env bash
# Reproducible Criterion A/B gate (CLAUDE.md quality gate 3).
#
# Runs the baseline tree and the candidate tree back-to-back, interleaved over
# ROUNDS rounds (A B / B A / A B ...), each tree with its OWN target directory,
# every heavy step wrapped in the fleet build lock + bounded systemd scope that
# this host mandates. Per-case medians are taken across rounds, then compared:
#
#   FAIL if the mean of per-case median deltas exceeds BUDGET_PCT (default 3),
#   FAIL if any case is a *separated* regression above SEP_PCT (default 2):
#        every candidate round slower than every baseline round AND the delta
#        exceeds SEP_PCT — i.e. a shift that run-to-run scatter cannot explain.
#
# Everything (per-round Criterion estimates, per-case table, verdict) is
# archived under OUT so the run can be audited or re-compared later.
#
# Usage:
#   scripts/bench-gate.sh <baseline-tree> <candidate-tree> <label>
# Env:
#   ROUNDS=3               interleaved rounds per tree
#   FEATURES=all-features  cargo feature set
#   BENCHES="pooling routing protocol relay"   bench targets (Criterion, harness=false)
#   FILTER=""              optional Criterion filter regex (subset run)
#   BUDGET_PCT=3 SEP_PCT=2 gate thresholds (percent)
#   OUT=/home/gpc/HDB/sprint/baselines/proxy/<label>   archive directory
#   LOCK=/home/gpc/HDB/sprint/coordination/build.lock  fleet build lock
#   MEM=24G                systemd MemoryMax for each heavy step
#   NO_LOCK=1              skip flock/systemd-run (CI containers only)
#   SKIP_BUILD=1           trees are already compiled (skips `cargo bench --no-run`)
set -euo pipefail

BASE="${1:?usage: bench-gate.sh <baseline-tree> <candidate-tree> <label>}"
CAND="${2:?usage: bench-gate.sh <baseline-tree> <candidate-tree> <label>}"
LABEL="${3:?usage: bench-gate.sh <baseline-tree> <candidate-tree> <label>}"
ROUNDS="${ROUNDS:-3}"
FEATURES="${FEATURES:-all-features}"
BENCHES="${BENCHES:-pooling routing protocol relay}"
FILTER="${FILTER:-}"
BUDGET_PCT="${BUDGET_PCT:-3}"
SEP_PCT="${SEP_PCT:-2}"
OUT="${OUT:-/home/gpc/HDB/sprint/baselines/proxy/$LABEL}"
LOCK="${LOCK:-/home/gpc/HDB/sprint/coordination/build.lock}"
MEM="${MEM:-24G}"
NO_LOCK="${NO_LOCK:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"

BASE="$(cd "$BASE" && pwd)"; CAND="$(cd "$CAND" && pwd)"
[ "$BASE" != "$CAND" ] || { echo "baseline and candidate must be different trees" >&2; exit 2; }
mkdir -p "$OUT"
LOG="$OUT/bench-gate.log"
BENCH_ARGS=(); for b in $BENCHES; do BENCH_ARGS+=(--bench "$b"); done

# Own target dir per tree: never share CARGO_TARGET_DIR across trees (identical
# crate name+version → identical unit hashes → false-fresh artifacts).
target_of() { echo "${1}/target"; }

heavy() {
  # fleet lock OUTSIDE, memory bound INSIDE — both mandatory on this host.
  if [ "$NO_LOCK" = 1 ]; then "$@"; else
    flock "$LOCK" systemd-run --user --scope --collect --quiet \
      -p MemoryMax="$MEM" -p MemorySwapMax=0 "$@"
  fi
}

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[$(stamp)] $*" | tee -a "$LOG"; }

{
  echo "bench-gate label=$LABEL rounds=$ROUNDS features=$FEATURES benches=$BENCHES filter='${FILTER}'"
  echo "baseline:  $BASE @ $(git -C "$BASE" rev-parse --short HEAD) $(git -C "$BASE" status --porcelain | wc -l) dirty"
  echo "candidate: $CAND @ $(git -C "$CAND" rev-parse --short HEAD) $(git -C "$CAND" status --porcelain | wc -l) dirty"
  echo "rustflags: ${RUSTFLAGS:-<none>}"
  echo "toolchain: $(rustc --version)  host: $(hostname)  governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "start: $(stamp)  load: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee "$OUT/stamp.txt"

if [ "$SKIP_BUILD" != 1 ]; then
  for tree in "$BASE" "$CAND"; do
    say "build $tree"
    ( cd "$tree" && CARGO_TARGET_DIR="$(target_of "$tree")" \
        heavy cargo bench --features "$FEATURES" "${BENCH_ARGS[@]}" --no-run ) >>"$LOG" 2>&1
  done
fi

run_tree() { # $1=tree $2=round $3=name(base|cand)
  local tree=$1 round=$2 name=$3
  say "round $round $name ($tree) load=$(cut -d' ' -f1-3 /proc/loadavg)"
  ( cd "$tree" && CARGO_TARGET_DIR="$(target_of "$tree")" \
      heavy cargo bench --features "$FEATURES" "${BENCH_ARGS[@]}" -- \
        --save-baseline "gate-${LABEL}-r${round}" ${FILTER:+"$FILTER"} ) \
    > "$OUT/${name}-r${round}.log" 2>&1
}

for r in $(seq 1 "$ROUNDS"); do
  if [ $((r % 2)) = 1 ]; then
    run_tree "$BASE" "$r" base; run_tree "$CAND" "$r" cand
  else
    run_tree "$CAND" "$r" cand; run_tree "$BASE" "$r" base
  fi
done

say "collect + compare"
rc=0
python3 - "$BASE" "$CAND" "$LABEL" "$ROUNDS" "$OUT" "$BUDGET_PCT" "$SEP_PCT" <<'EOF' || rc=$?
import json, os, sys, statistics, glob
base, cand, label, rounds, out, budget, sep = sys.argv[1:8]
rounds = int(rounds); budget = float(budget); sep = float(sep)

def collect(tree, r):
    """case -> (median_ns, lo, hi) for saved baseline gate-<label>-r<r>."""
    res = {}
    root = os.path.join(tree, "target", "criterion")
    name = f"gate-{label}-r{r}"
    for path in glob.glob(os.path.join(root, "**", name, "estimates.json"), recursive=True):
        case = os.path.relpath(os.path.dirname(os.path.dirname(path)), root)
        try:
            e = json.load(open(path))["median"]
        except Exception:
            continue
        res[case] = (e["point_estimate"], e["confidence_interval"]["lower_bound"],
                     e["confidence_interval"]["upper_bound"])
    return res

B = [collect(base, r) for r in range(1, rounds + 1)]
C = [collect(cand, r) for r in range(1, rounds + 1)]
cases = sorted(set.intersection(*[set(x) for x in B + C])) if B and C else []
rows = []
for case in cases:
    b = [x[case][0] for x in B]; c = [x[case][0] for x in C]
    bm, cm = statistics.median(b), statistics.median(c)
    delta = (cm - bm) / bm * 100.0
    separated = (min(c) > max(b)) or (max(c) < min(b))
    rows.append(dict(case=case, base_ns=bm, cand_ns=cm, delta_pct=delta,
                     separated=separated, base_rounds=b, cand_rounds=c))

deltas = [r["delta_pct"] for r in rows]
mean = statistics.fmean(deltas) if deltas else 0.0
median = statistics.median(deltas) if deltas else 0.0
sep_reg = [r for r in rows if r["separated"] and r["delta_pct"] > sep]
sep_imp = [r for r in rows if r["separated"] and r["delta_pct"] < -sep]
missing = [c for x in B + C for c in x if c not in cases]
verdict = "PASS" if (mean <= budget and not sep_reg and rows) else "FAIL"
if not rows:
    verdict = "FAIL (no matched cases)"

json.dump(dict(label=label, rounds=rounds, baseline=base, candidate=cand,
               budget_pct=budget, sep_pct=sep, mean_delta_pct=mean,
               median_delta_pct=median, matched=len(rows), verdict=verdict,
               separated_regressions=[r["case"] for r in sep_reg],
               separated_improvements=[r["case"] for r in sep_imp], cases=rows),
          open(os.path.join(out, "bench-gate.json"), "w"), indent=1)

lines = []
lines.append(f"bench-gate {label}: {verdict}")
lines.append(f"matched={len(rows)} rounds={rounds} mean_delta={mean:+.3f}% median_delta={median:+.3f}% "
             f"budget={budget}% separated_regressions(>{sep}%)={len(sep_reg)} "
             f"separated_improvements(>{sep}%)={len(sep_imp)}")
if missing:
    lines.append(f"unmatched (present in some rounds only): {sorted(set(missing))}")
lines.append("")
lines.append(f"{'case':58s} {'base_ns':>12s} {'cand_ns':>12s} {'delta%':>9s} sep  base_rounds | cand_rounds")
for r in sorted(rows, key=lambda r: -r["delta_pct"]):
    br = " ".join(f"{v:.1f}" for v in r["base_rounds"]); cr = " ".join(f"{v:.1f}" for v in r["cand_rounds"])
    lines.append(f"{r['case']:58s} {r['base_ns']:12.2f} {r['cand_ns']:12.2f} {r['delta_pct']:+9.2f} "
                 f"{'Y' if r['separated'] else '.':>3s}  {br} | {cr}")
txt = "\n".join(lines) + "\n"
open(os.path.join(out, "bench-gate-summary.txt"), "w").write(txt)
print(txt)
sys.exit(0 if verdict == "PASS" else 1)
EOF
say "done verdict rc=$rc (summary: $OUT/bench-gate-summary.txt)"
exit $rc
