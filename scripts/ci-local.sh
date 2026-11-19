#!/usr/bin/env bash
# Run .github/workflows/ci.yml's jobs locally, one heavy step at a time, with the
# toolchains CI uses: STABLE (default `stable`; set it to the version CI resolved,
# e.g. 1.100.0, since CI always takes the newest stable) and MSRV 1.86.
#
# Usage: scripts/ci-local.sh [job...]   jobs: fmt check clippy test msrv bench plugins
#   (default: all). Each heavy step runs under the fleet lock and a memory-bounded
#   scope unless NO_LOCK=1. Status lines go to $STATUS (default ci-local.status in
#   the target dir). Exit status: number of failed steps (capped at 100).
#
# Env: STABLE, LOCK, MEM (24G), NO_LOCK, CARGO_TARGET_DIR (default target-ci-$STABLE),
#      PLUGINS_DIR (HeliosDB-Proxy-Plugins checkout, default ./plugins).
set -u
cd "$(dirname "$0")/.."
STABLE="${STABLE:-stable}"
MSRV=1.86.0
LOCK="${LOCK:-${HELIOS_SPRINT:-$HOME/HDB/sprint}/coordination/build.lock}"
MEM="${MEM:-24G}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target-ci-$STABLE}"
mkdir -p "$CARGO_TARGET_DIR"
STATUS="${STATUS:-$CARGO_TARGET_DIR/ci-local.status}"
LOGS="$CARGO_TARGET_DIR/ci-local-logs"; mkdir -p "$LOGS"
failed=0
step() { # name cmd...
  local name=$1; shift
  echo "[$(date -u +%FT%TZ)] $name queued" >> "$STATUS"
  if [ "${NO_LOCK:-0}" = 1 ]; then "$@"; else
    flock "$LOCK" systemd-run --user --scope --collect --quiet -p MemoryMax="$MEM" -p MemorySwapMax=0 "$@"
  fi > "$LOGS/$name.log" 2>&1
  local rc=$?
  echo "[$(date -u +%FT%TZ)] $name rc=$rc" >> "$STATUS"
  [ $rc = 0 ] || failed=$((failed + 1))
}
features_of() { case $1 in "") echo "";; *) echo "--features $1";; esac; }
job_fmt()    { step fmt cargo +"$STABLE" fmt --check; }
job_check()  { for f in "" ha-tr all-features all-features,postgres-topology; do
                 step "check-${f:-default}" cargo +"$STABLE" check $(features_of "$f"); done; }
job_clippy() { for f in "" ha-tr all-features all-features,postgres-topology msrv-features; do
                 step "clippy-${f:-default}" cargo +"$STABLE" clippy $(features_of "$f") --tests -- -D warnings; done
               step clippy-wasm-plugins cargo +"$STABLE" clippy --no-default-features --features wasm-plugins --tests -- -D warnings; }
job_test()   { for f in "" ha-tr all-features all-features,postgres-topology; do
                 step "test-${f:-default}" cargo +"$STABLE" test $(features_of "$f"); done; }
job_msrv()   { step msrv cargo +"$MSRV" check --locked --features msrv-features; }
job_bench()  { step bench-compile cargo +"$STABLE" bench --features all-features --no-run
               step gate-comparators bash -c 'python3 scripts/tests/bench_gate_compare_test.py && python3 scripts/tests/bench_gate_confirm_test.py && python3 scripts/tests/compare_usertp_test.py'; }
# The first-party plugins come from the HeliosDB-Proxy-Plugins repository (CI checks
# out its default branch into ./plugins); PLUGINS_DIR points at a checkout of it.
job_plugins() {
  local dir="${PLUGINS_DIR:-$PWD/plugins}"
  if [ ! -f "$dir/Cargo.toml" ]; then
    echo "[$(date -u +%FT%TZ)] plugins rc=2 (no HeliosDB-Proxy-Plugins checkout at $dir; set PLUGINS_DIR)" >> "$STATUS"
    failed=$((failed + 1)); return
  fi
  step plugins-build bash -c "cd '$dir' && CARGO_TARGET_DIR='$dir/target' cargo +$STABLE build --target wasm32-unknown-unknown --release"
  step plugins-e2e env HELIOS_PLUGINS_DIR="$dir/target/wasm32-unknown-unknown/release" HELIOS_PLUGINS_REQUIRED=1 \
    cargo +"$STABLE" test --features wasm-plugins --test wasm_plugin_e2e
}
jobs=("$@"); [ ${#jobs[@]} = 0 ] && jobs=(fmt check clippy test msrv bench plugins)
for j in "${jobs[@]}"; do "job_$j"; done
echo "[$(date -u +%FT%TZ)] done failed=$failed" >> "$STATUS"
[ $failed -gt 100 ] && failed=100
exit $failed
