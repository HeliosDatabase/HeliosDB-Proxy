#!/usr/bin/env bash
# Reproducible Criterion A/B gate (CLAUDE.md quality gate 3).
#
# Runs the baseline tree and the candidate tree back-to-back, interleaved over
# ROUNDS rounds (A B / B A / A B ...), each tree with its OWN target directory,
# every heavy step wrapped in the fleet build lock + bounded systemd scope that
# this host mandates. Per-case medians are taken across rounds, then compared
# by scripts/bench-gate-compare.py:
#
#   FAIL if the mean of per-case median deltas exceeds BUDGET_PCT (default 3),
#   FAIL if any case is a *separated* regression above SEP_PCT (default 2):
#        every candidate round slower than every baseline round AND the delta
#        exceeds SEP_PCT,
#   FAIL if a case the baseline declares is missing from the candidate (removed),
#   INVALID (exit 2, never PASS/FAIL) if any declared case is missing, duplicated,
#        malformed, non-finite or stale in any round, if outputs for LABEL already
#        exist before the run, or if the executables that ran are not exactly the
#        ones built (hash and path).
# Cases only the candidate declares are reported as NEW (they need a
# benches/BASELINE.md entry) and are not compared.
#
# Attribution evidence: each bench executable pair is classified by
# scripts/bench-gate-fde.py. "body-identical" means no function body changed,
# only placement, so a separated regression there is layout class;
# "code-changed" means a function body differs.
#
# Second-build confirmation (CONFIRM=1, the default; owner decision 2026-11-17,
# scripts/bench-gate-confirm.py): when the primary comparison FAILs only on
# separated regressions (mean within BUDGET_PCT, nothing removed), those on
# body-identical executables are reported, not failing, and those on
# code-changed (or unclassified) executables are measured again: both trees
# are built a second time into their own CONFIRM_TARGET directories and the
# cases are re-run alone over the same interleaved rounds. A case fails the
# gate only if it is again separated and slower there. The final verdict is in
# bench-gate-final.json and at the end of the summary; the primary run's own
# verdict stays in bench-gate.json. CONFIRM=0 keeps the primary verdict.
# The reviewer still attributes every reproduced regression.
#
# Everything (per-round Criterion logs, executable list + hashes, declared
# cases, classification, per-case table, verdict) is archived under OUT.
#
# Usage:
#   scripts/bench-gate.sh <baseline-tree> <candidate-tree> <label>
# Env:
#   ROUNDS=3               interleaved rounds per tree
#   FEATURES=all-features  cargo feature set
#   BENCHES="pooling routing protocol relay cache"   bench targets (Criterion, harness=false)
#   PAIR_CASES=0          set1 to run adjacent A/B per fullID; requires equal inventories,
#                         uses fresh processes per case and records an invocation ledger
#   PAIR_TIMEOUT_SECS=300  per-case deadline; Criterion timing remains unchanged
#   FILTER=""              optional Criterion filter regex (subset run; also
#                          restricts the declared case set)
#   BUDGET_PCT=3 SEP_PCT=2 gate thresholds (percent)
#   CONFIRM=1              second-build confirmation of separated regressions
#                          on changed code (see above); 0 = primary verdict only
#   CONFIRM_TARGET=<tree>/target-confirm   target directory of the second build
#   OUT=$HELIOS_SPRINT/baselines/proxy/<label>   archive directory
#   LOCK=$HELIOS_SPRINT/coordination/build.lock  fleet build lock
#   HELIOS_SPRINT=$HOME/HDB/sprint                   the maintainers' sprint tree
#   MEM=24G                systemd MemoryMax for each heavy step
#   NO_LOCK=1              skip flock/systemd-run (only inside an already-held
#                          fleet lock + bounded scope, or in CI containers)
#   SKIP_BUILD=1           accepted for compatibility; the `cargo bench --no-run`
#                          step always runs (a no-op on built trees) because it
#                          records the exact executables the rounds must use
# Exit: 0 PASS, 1 FAIL, 2 INVALID / usage error.
# Requires: python3; readelf + objdump (binutils) for the classification
# (without them the executables are classified "unknown").
set -euo pipefail

BASE="${1:?usage: bench-gate.sh <baseline-tree> <candidate-tree> <label>}"
CAND="${2:?usage: bench-gate.sh <baseline-tree> <candidate-tree> <label>}"
LABEL="${3:?usage: bench-gate.sh <baseline-tree> <candidate-tree> <label>}"
ROUNDS="${ROUNDS:-3}"
FEATURES="${FEATURES:-all-features}"
BENCHES="${BENCHES:-pooling routing protocol relay cache}"
FILTER="${FILTER:-}"
PAIR_CASES="${PAIR_CASES:-0}"
BUDGET_PCT="${BUDGET_PCT:-3}"
SEP_PCT="${SEP_PCT:-2}"
OUT="${OUT:-${HELIOS_SPRINT:-$HOME/HDB/sprint}/baselines/proxy/$LABEL}"
LOCK="${LOCK:-${HELIOS_SPRINT:-$HOME/HDB/sprint}/coordination/build.lock}"
MEM="${MEM:-24G}"
NO_LOCK="${NO_LOCK:-0}"
CONFIRM="${CONFIRM:-1}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "bench-gate: $*" >&2; exit 2; }
BASE="$(cd "$BASE" && pwd)"; CAND="$(cd "$CAND" && pwd)"
[ "$BASE" != "$CAND" ] || die "baseline and candidate must be different trees"
[[ "$ROUNDS" =~ ^[1-9][0-9]*$ ]] || die "ROUNDS must be a positive integer"
[[ "$PAIR_CASES" = 0 || "$PAIR_CASES" = 1 ]] || die "PAIR_CASES must be 0 or 1"
[[ "$CONFIRM" = 0 || "$CONFIRM" = 1 ]] || die "CONFIRM must be 0 or 1"
[[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]] || die "LABEL may only contain letters, digits, '.', '_' and '-'"
python3 -c "import sys; [float(x) for x in sys.argv[1:]]" "$BUDGET_PCT" "$SEP_PCT" 2>/dev/null \
  || die "BUDGET_PCT and SEP_PCT must be numbers"
# Fail closed BEFORE any work: earlier outputs under this label could stand in for
# missing ones, so a label is single-use per tree.
confirm_target_of() { echo "${CONFIRM_TARGET:-${1}/target-confirm}"; }
for tree in "$BASE" "$CAND"; do
  for dir in "$tree/target/criterion" "$(confirm_target_of "$tree")/criterion"; do
    if [ -d "$dir" ] && \
       find "$dir" -type d \( -name "gate-${LABEL}-r*" -o -name "gate-${LABEL}-confirm-r*" \) \
         -print -quit | grep -q .; then
      die "outputs for label '$LABEL' already exist under $dir; use a fresh label"
    fi
  done
done
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
  echo "bench-gate label=$LABEL rounds=$ROUNDS features=$FEATURES benches=$BENCHES pair_cases=$PAIR_CASES filter='${FILTER}'"
  echo "baseline:  $BASE @ $(git -C "$BASE" rev-parse --short HEAD) $(git -C "$BASE" status --porcelain | wc -l) dirty"
  echo "candidate: $CAND @ $(git -C "$CAND" rev-parse --short HEAD) $(git -C "$CAND" status --porcelain | wc -l) dirty"
  echo "rustflags: ${RUSTFLAGS:-<none>}"
  echo "toolchain: $(rustc --version)  host: $(hostname)  governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
  echo "start: $(stamp)  load: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee "$OUT/stamp.txt"

# Build (no-op when fresh) and record the exact executable + declared cases per bench target.
for arm in base cand; do
  tree=$BASE; [ "$arm" = cand ] && tree=$CAND
  say "build/list executables $arm ($tree)"
  ( cd "$tree" && CARGO_TARGET_DIR="$(target_of "$tree")" \
      heavy cargo bench --features "$FEATURES" "${BENCH_ARGS[@]}" --no-run ) > "$OUT/build-$arm.log" 2>&1 \
    || die "cargo bench --no-run failed for $arm (see $OUT/build-$arm.log)"
  : > "$OUT/executables-$arm.txt"; : > "$OUT/cases-$arm.tsv"
  for b in $BENCHES; do
    path=$(sed -n "s|^ *Executable benches/$b\.rs (\(.*\))\$|\1|p" "$OUT/build-$arm.log" | tail -n 1)
    [ -n "$path" ] || die "no executable recorded for bench '$b' ($arm)"
    case $path in /*) ;; *) path="$tree/$path" ;; esac
    [ -x "$path" ] || die "recorded executable for '$b' ($arm) is missing: $path"
    echo "$b $(sha256sum "$path" | cut -d' ' -f1) $path" >> "$OUT/executables-$arm.txt"
    listing=$( cd "$tree" && "$path" --bench --list ${FILTER:+"$FILTER"} 2>/dev/null ) \
      || die "listing cases of '$b' ($arm) failed"
    printf '%s\n' "$listing" | sed -n 's/: benchmark$//p' | sed "s|^|$b\t|" >> "$OUT/cases-$arm.tsv"
  done
done

say "classify executables (function bodies, scripts/bench-gate-fde.py)"
python3 - "$HERE/bench-gate-fde.py" "$OUT" $BENCHES <<'EOF' >> "$LOG" 2>&1 || echo '{}' > "$OUT/classes.json"
import importlib.util, json, os, sys
spec = importlib.util.spec_from_file_location("fde", sys.argv[1])
fde = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fde)
out, benches = sys.argv[2], sys.argv[3:]
exe = {}
for arm in ("base", "cand"):
    for line in open(os.path.join(out, f"executables-{arm}.txt")):
        target, _sha, path = line.rstrip("\n").split(" ", 2)
        exe[(arm, target)] = path
classes = {b: fde.compare(exe[("base", b)], exe[("cand", b)]) for b in benches}
with open(os.path.join(out, "classes.json"), "w") as f:
    json.dump(classes, f, indent=1)
for b, c in classes.items():
    print(b, c["classification"])
EOF
[ -s "$OUT/classes.json" ] || echo '{}' > "$OUT/classes.json"

run_tree() { # $1=tree $2=round $3=name(base|cand)
  local tree=$1 round=$2 name=$3
  say "round $round $name ($tree) load=$(cut -d' ' -f1-3 /proc/loadavg)"
  ( cd "$tree" && CARGO_TARGET_DIR="$(target_of "$tree")" \
      heavy cargo bench --features "$FEATURES" "${BENCH_ARGS[@]}" -- \
        --save-baseline "gate-${LABEL}-r${round}" ${FILTER:+"$FILTER"} ) \
    > "$OUT/${name}-r${round}.log" 2>&1
}

START=$(date +%s)
if [ "$PAIR_CASES" = 1 ]; then
  # FILTER has already selected both declared inventories. The helper rejects
  # differing target/full-ID mappings and verifies one fresh raw case per run.
  say "adjacent case pairs (fresh process per case; AB/BA/AB round order)"
  heavy python3 "$HERE/bench-gate-paired.py" --base "$BASE" --cand "$CAND" \
    --out "$OUT" --label "$LABEL" --rounds "$ROUNDS" --timeout "${PAIR_TIMEOUT_SECS:-300}" >> "$LOG" 2>&1 \
    || die "paired execution failed (see invocation ledger and bench-gate.log)"
else
  for r in $(seq 1 "$ROUNDS"); do
    if [ $((r % 2)) = 1 ]; then
      run_tree "$BASE" "$r" base; run_tree "$CAND" "$r" cand
    else
      run_tree "$CAND" "$r" cand; run_tree "$BASE" "$r" base
    fi
  done
fi

say "verify executables + collect + compare"
exe_problem=""
for arm in base cand; do
  tree=$BASE; [ "$arm" = cand ] && tree=$CAND
  while read -r b sha path; do
    [ "$(sha256sum "$path" | cut -d' ' -f1)" = "$sha" ] || exe_problem+="$arm/$b executable changed during the run; "
  done < "$OUT/executables-$arm.txt"
  if [ "$PAIR_CASES" = 1 ]; then
    # Direct execution is bound through /proc and the invocation ledger, not
    # fabricated Cargo output. Zero-case targets are still hashed above.
    if ! python3 - "$OUT" "$arm" <<'VERIFY'
import json, pathlib, sys
out, arm = pathlib.Path(sys.argv[1]), sys.argv[2]
targets = {line.split("\t", 1)[0] for line in (out / f"cases-{arm}.tsv").read_text().splitlines() if line}
expected = {line.split(" ", 2)[2] for line in (out / f"executables-{arm}.txt").read_text().splitlines() if line.split(" ", 1)[0] in targets}
records = [json.loads(line) for line in (out / "paired-invocations.jsonl").read_text().splitlines()]
ran = {r["executable"] for r in records if r["arm"] == arm and r["event"] == "complete"}
if ran != expected:
    raise SystemExit("paired executable set differs from selected case inventory")
VERIFY
    then exe_problem+="$arm paired executable ledger mismatch; "; fi
  else
  ran=$(cat "$OUT/$arm"-r*.log | sed -n 's|^ *Running benches/[^ ]* (\(.*\))$|\1|p' \
        | while read -r p; do case $p in /*) echo "$p" ;; *) echo "$tree/$p" ;; esac; done | sort -u)
  built=$(cut -d' ' -f3- "$OUT/executables-$arm.txt" | sort -u)
  [ "$ran" = "$built" ] || exe_problem+="$arm rounds ran executables other than the ones built; "
  fi
done

INVALID_ARGS=(); [ -z "$exe_problem" ] || INVALID_ARGS=(--invalid "$exe_problem")
rc=0
python3 "$HERE/bench-gate-compare.py" "${INVALID_ARGS[@]}" \
  --base-root "$(target_of "$BASE")/criterion" --cand-root "$(target_of "$CAND")/criterion" \
  --label "$LABEL" --rounds "$ROUNDS" --base-cases "$OUT/cases-base.tsv" --cand-cases "$OUT/cases-cand.tsv" \
  --classes "$OUT/classes.json" --budget "$BUDGET_PCT" --sep "$SEP_PCT" --start "$START" --out "$OUT" \
  --baseline-name "$BASE" --candidate-name "$CAND" >> "$LOG" 2>&1 || rc=$?
cat "$OUT/bench-gate-summary.txt" 2>/dev/null | head -n 20

if [ "$CONFIRM" = 1 ] && [ "$rc" != 2 ]; then
  plan=$(python3 "$HERE/bench-gate-confirm.py" plan "$OUT/bench-gate.json")
  CONF_OUT="$OUT/confirm"
  case "$plan" in
    INVALID|FAIL|LAYOUT_ONLY) say "confirmation: none needed ($plan)" ;;
    *)
      mkdir -p "$CONF_OUT"
      printf '%s\n' "$plan" > "$CONF_OUT/ids.txt"
      CONF_LABEL="${LABEL}-confirm"
      CONF_FILTER=$(python3 "$HERE/bench-gate-confirm.py" regex "$CONF_OUT/ids.txt")
      say "confirmation: $(wc -l < "$CONF_OUT/ids.txt") case(s) on changed code, second build of both trees"
      for arm in base cand; do
        tree=$BASE; [ "$arm" = cand ] && tree=$CAND
        python3 "$HERE/bench-gate-confirm.py" cases "$OUT/cases-$arm.tsv" "$CONF_OUT/ids.txt" \
          "$CONF_OUT/cases-$arm.tsv"
        ctarget="$(confirm_target_of "$tree")"
        say "confirmation build $arm ($tree, target $ctarget)"
        ( cd "$tree" && CARGO_TARGET_DIR="$ctarget" \
            heavy cargo bench --features "$FEATURES" "${BENCH_ARGS[@]}" --no-run ) \
          > "$CONF_OUT/build-$arm.log" 2>&1 \
          || die "confirmation build failed for $arm (see $CONF_OUT/build-$arm.log)"
        : > "$CONF_OUT/executables-$arm.txt"
        for b in $BENCHES; do
          path=$(sed -n "s|^ *Executable benches/$b\.rs (\(.*\))\$|\1|p" "$CONF_OUT/build-$arm.log" | tail -n 1)
          [ -n "$path" ] || die "no confirmation executable recorded for bench '$b' ($arm)"
          case $path in /*) ;; *) path="$tree/$path" ;; esac
          echo "$b $(sha256sum "$path" | cut -d' ' -f1) $path" >> "$CONF_OUT/executables-$arm.txt"
        done
      done
      run_confirm() { # $1=tree $2=round $3=name(base|cand)
        local tree=$1 round=$2 name=$3
        say "confirmation round $round $name ($tree) load=$(cut -d' ' -f1-3 /proc/loadavg)"
        ( cd "$tree" && CARGO_TARGET_DIR="$(confirm_target_of "$tree")" \
            heavy cargo bench --features "$FEATURES" "${BENCH_ARGS[@]}" -- \
              --save-baseline "gate-${CONF_LABEL}-r${round}" "$CONF_FILTER" ) \
          > "$CONF_OUT/${name}-r${round}.log" 2>&1
      }
      CONF_START=$(date +%s)
      for r in $(seq 1 "$ROUNDS"); do
        if [ $((r % 2)) = 1 ]; then
          run_confirm "$BASE" "$r" base; run_confirm "$CAND" "$r" cand
        else
          run_confirm "$CAND" "$r" cand; run_confirm "$BASE" "$r" base
        fi
      done
      conf_problem=""
      for arm in base cand; do
        tree=$BASE; [ "$arm" = cand ] && tree=$CAND
        while read -r b sha path; do
          [ "$(sha256sum "$path" | cut -d' ' -f1)" = "$sha" ] \
            || conf_problem+="$arm/$b confirmation executable changed during the run; "
        done < "$CONF_OUT/executables-$arm.txt"
      done
      CONF_ARGS=(); [ -z "$conf_problem" ] || CONF_ARGS=(--invalid "$conf_problem")
      python3 "$HERE/bench-gate-compare.py" "${CONF_ARGS[@]}" \
        --base-root "$(confirm_target_of "$BASE")/criterion" \
        --cand-root "$(confirm_target_of "$CAND")/criterion" \
        --label "$CONF_LABEL" --rounds "$ROUNDS" \
        --base-cases "$CONF_OUT/cases-base.tsv" --cand-cases "$CONF_OUT/cases-cand.tsv" \
        --classes "$OUT/classes.json" --budget 1000000 --sep 0 --start "$CONF_START" \
        --out "$CONF_OUT" --baseline-name "$BASE" --candidate-name "$CAND" >> "$LOG" 2>&1 || true
      ;;
  esac
  rc=0
  python3 "$HERE/bench-gate-confirm.py" verdict "$OUT/bench-gate.json" \
    "$CONF_OUT/bench-gate.json" "$OUT" | tee -a "$LOG" || rc=$?
  # `tee` keeps the helper's exit status out of the pipeline's; read it back.
  rc=$(python3 -c 'import json,sys; print({"PASS":0,"FAIL":1,"INVALID":2}[json.load(open(sys.argv[1]))["verdict"]])' \
    "$OUT/bench-gate-final.json") || rc=2
fi
say "done verdict rc=$rc (0 PASS, 1 FAIL, 2 INVALID; summary: $OUT/bench-gate-summary.txt)"
exit $rc
