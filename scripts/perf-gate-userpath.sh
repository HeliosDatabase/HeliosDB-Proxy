#!/usr/bin/env bash
# P-01 user-path performance gate: base proxy binary vs candidate, interleaved
# passes of the live-backend harness, gated on committed-write TPS/p99 vs a
# budget. This is the MANDATORY gate (CLAUDE.md gate 3, CONTRIBUTING.md
# "Releasing") for changes that touch src/server.rs relay paths,
# src/journal_capture.rs, src/transaction_journal.rs, or src/pool/, and for
# every release. Criterion (scripts/bench-gate.sh) stays the gate for
# microbenchmarks; it runs one task per lock and cannot see the scheduler
# convoys that only show up under concurrent write load on a live backend —
# that is exactly how the 2026-09-21 TR-07 regression (Criterion +2.97%, this
# harness -40% committed TPS at 16 clients, fixed in 8f412dc) got through.
#
# Usage: scripts/perf-gate-userpath.sh <base-binary> <cand-binary> <label>
#
# Env (defaults match the item's acceptance run; do not raise CLIENTS beyond
# what CLAUDE.md's Resource Constraints allow):
#   CLIENTS       pgbench client counts to sweep      (default "16 64")
#   DUR            seconds per pgbench run              (default 12)
#   PASSES         interleaved base/cand pass count     (default 3)
#   MODES          proxy pool modes to test             (default "session transaction")
#   PGOPTIONS      passed through to pgbench            (default "-c synchronous_commit=off")
#   BUDGET_PCT     regression budget, TPS and p99        (default 3.0, see compare-usertp.py)
#   BASELINE_ROOT  where labelled runs are archived      (default /home/gpc/HDB/sprint/baselines/proxy)
#   OUT            this run's archive dir                (default $BASELINE_ROOT/$LABEL/usertp)
#   LOCK           fleet-wide heavy-job lock file         (default /home/gpc/HDB/sprint/coordination/build.lock)
#   MEM            systemd-run MemoryMax per heavy job    (default 24G)
#   NO_LOCK=1      skip flock/systemd-run (CI containers only — matches scripts/bench-gate.sh)
#
# Prerequisites (same as scripts/regress/bench-usertp.sh): a PostgreSQL 18.4
# backend already running at 127.0.0.1:25433, user bench/benchpass, db
# benchdb, and Docker for the psql/pgbench client image. This script does not
# start, stop, or otherwise touch the backend or any other container beyond
# short-lived `docker run --rm` clients.
#
# Every heavy step below (backend init, each base/cand pass) is individually
# wrapped in the fleet build lock + a memory-bounded systemd scope (same
# fleet-lock-OUTSIDE, memory-bound-INSIDE pattern as scripts/bench-gate.sh),
# per CLAUDE.md's "Bounded benchmark invocation" (root cause of the
# 2026-07-08 host crash) — so invoke THIS script directly; do not also wrap
# the whole invocation in another flock/systemd-run, or the nested flock on
# the same lock file will deadlock. Running this gate needs the same explicit
# owner approval as any other Docker/pgbench harness run in this repo — get
# it before invoking this script, and record it in the GATE-RECORD alongside
# the archived output.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_BIN="${1:?usage: perf-gate-userpath.sh <base-binary> <cand-binary> <label>}"
CAND_BIN="${2:?usage: perf-gate-userpath.sh <base-binary> <cand-binary> <label>}"
LABEL="${3:?usage: perf-gate-userpath.sh <base-binary> <cand-binary> <label>}"

PGHOST=127.0.0.1; PGPORT=25433
BUSER=bench; BPASS=benchpass; BDB=benchdb
IMG="postgres:18.4-bookworm"

CLIENTS="${CLIENTS:-16 64}"
DUR="${DUR:-12}"
PASSES="${PASSES:-3}"
MODES="${MODES:-session transaction}"
PGOPTIONS="${PGOPTIONS:--c synchronous_commit=off}"
BUDGET_PCT="${BUDGET_PCT:-3.0}"
BASELINE_ROOT="${BASELINE_ROOT:-/home/gpc/HDB/sprint/baselines/proxy}"
OUT="${OUT:-$BASELINE_ROOT/$LABEL/usertp}"
LOCK="${LOCK:-/home/gpc/HDB/sprint/coordination/build.lock}"
MEM="${MEM:-24G}"
NO_LOCK="${NO_LOCK:-0}"

for bin in "$BASE_BIN" "$CAND_BIN"; do
  [ -x "$bin" ] || { echo "FATAL: $bin is not an executable file" >&2; exit 2; }
done
command -v docker >/dev/null || { echo "FATAL: docker is required (pgbench/psql client image)" >&2; exit 2; }
if [ "$NO_LOCK" != 1 ]; then
  command -v flock >/dev/null || { echo "FATAL: flock is required (fleet build lock; or set NO_LOCK=1)" >&2; exit 2; }
  command -v systemd-run >/dev/null || { echo "FATAL: systemd-run is required (bounded benchmark invocation; or set NO_LOCK=1)" >&2; exit 2; }
fi

mkdir -p "$OUT"

pg(){ docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" "$@"; }
# fleet lock OUTSIDE, memory bound INSIDE — both mandatory on this host (see
# scripts/bench-gate.sh, same pattern). NO_LOCK=1 is for CI containers only.
heavy(){
  if [ "$NO_LOCK" = 1 ]; then "$@"; else
    flock "$LOCK" systemd-run --user --scope --collect --quiet \
      -p MemoryMax="$MEM" -p MemorySwapMax=0 "$@"
  fi
}

echo "== perf-gate-userpath($LABEL): preflight =="
if ! pg psql -h "$PGHOST" -p "$PGPORT" -U "$BUSER" -d "$BDB" -tAc "select 1" >/dev/null 2>&1; then
  echo "FATAL: backend $PGHOST:$PGPORT (db $BDB, user $BUSER) is not reachable. This gate does not start a backend; see CLAUDE.md Resource Constraints." >&2
  exit 2
fi

rows="$(pg psql -h "$PGHOST" -p "$PGPORT" -U "$BUSER" -d "$BDB" -tAc "select count(*) from pgbench_accounts" 2>/dev/null | tr -d '[:space:]')"
if [ "${rows:-0}" != "1000000" ]; then
  echo "pgbench_accounts has ${rows:-0} row(s) (want 1,000,000) — initializing with pgbench -i -s 10"
  # inlined rather than via the pg() shell function: heavy()'s systemd-run
  # exec's argv directly and would not see a function defined in this shell.
  heavy docker run --rm --network host -e PGPASSWORD="$BPASS" "$IMG" \
    pgbench -h "$PGHOST" -p "$PGPORT" -U "$BUSER" -d "$BDB" -i -s 10
fi

run_pass(){
  local bin=$1 tag=$2 pass=$3
  local plabel="$LABEL-$tag-p$pass"
  echo "-- pass $pass/$PASSES: $tag ($bin) --"
  # invoked via `bash` rather than relying on the file's executable bit —
  # scripts/regress/bench-usertp.sh is tracked without +x in git.
  heavy env OUT="$OUT" CLIENTS="$CLIENTS" DUR="$DUR" MODES="$MODES" PGOPTIONS="$PGOPTIONS" \
    bash "$HERE/regress/bench-usertp.sh" "$bin" "$plabel"
}

echo "== perf-gate-userpath($LABEL): $PASSES interleaved pass(es), CLIENTS=\"$CLIENTS\" DUR=$DUR MODES=\"$MODES\" =="
for p in $(seq 1 "$PASSES"); do
  run_pass "$BASE_BIN" base "$p"
  run_pass "$CAND_BIN" cand "$p"
done

echo "== perf-gate-userpath($LABEL): compare (budget ${BUDGET_PCT}%) =="
set +e
OUT="$OUT" PREFIX="$LABEL" PASSES="$PASSES" CAND=cand BUDGET_PCT="$BUDGET_PCT" \
  python3 "$HERE/regress/compare-usertp.py" | tee "$OUT/$LABEL-compare.txt"
status="${PIPESTATUS[0]}"
set -e

echo "perf-gate-userpath($LABEL): exit $status  (archive: $OUT)"
exit "$status"
