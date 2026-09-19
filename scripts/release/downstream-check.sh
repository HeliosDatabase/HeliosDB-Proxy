#!/usr/bin/env bash
# Release-time downstream consumer check for heliosdb-proxy.
#
# WHY: Proxy 1.6.0 broke HeliosDB-Lite twice and nothing detected it (sprinter
# c57ebe1d1ae9). Lite pins `heliosdb-proxy` EXACTLY (`=X.Y.Z`, path = ../Proxy), so a
# version bump in Proxy's Cargo.toml breaks every cargo command in Lite at dependency
# resolution; and a library-API change that only affects external constructors (a new
# private field on a pub struct) is invisible to Proxy's own tests — only compiling a
# consumer surfaces it. This script does, per known consumer, what
# ../Lite/scripts/proxy_compat_gate.sh does from Lite's side, driven from the release.
#
# USAGE
#   scripts/release/downstream-check.sh            # report pins; build consumers against
#                                                  # the version in Proxy's Cargo.toml
#   scripts/release/downstream-check.sh --apply    # also rewrite each exact pin to the
#                                                  # new version in the consumer's Cargo.toml
#                                                  # (the release step; commit it there)
#   scripts/release/downstream-check.sh --dry-run  # pins and plan only, no cargo
#
# ENV: CONSUMERS (default "Lite:ha-proxy"; space-separated "<dir-under-HDB>:<feature>"),
#      HDB (default: the parent of this repo), LOCK (fleet build lock; NO_LOCK=1 to skip),
#      MEM (systemd scope MemoryMax, default 24G), CARGO_TARGET_DIR is left to the consumer.
# EXIT: 0 = every consumer resolves and compiles the proxy-backed feature at this version.
#       Non-zero with the reason. A consumer that cannot be found is a FAIL, not a skip.
set -uo pipefail

HERE="$(cd "$(dirname "$0")/../.." && pwd)"
HDB="${HDB:-$(dirname "$HERE")}"
CONSUMERS="${CONSUMERS:-Lite:ha-proxy}"
LOCK="${LOCK:-/home/gpc/HDB/sprint/coordination/build.lock}"
MEM="${MEM:-24G}"
MODE=check
case "${1:-}" in
  --apply) MODE=apply ;;
  --dry-run) MODE=dry ;;
  "") ;;
  *) echo "usage: $0 [--apply|--dry-run]" >&2; exit 2 ;;
esac

fail() { echo "DOWNSTREAM: FAIL — $*" >&2; exit 1; }

VERSION="$(grep -m1 '^version[[:space:]]*=' "$HERE/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')"
[ -n "$VERSION" ] || fail "cannot read version from $HERE/Cargo.toml"
echo "DOWNSTREAM: heliosdb-proxy $VERSION at $HERE (mode: $MODE)"

# Wrap heavy cargo work in the fleet lock + bounded scope (CLAUDE.md resource rule).
run_bounded() {
  if [ -n "${NO_LOCK:-}" ]; then "$@"; return; fi
  flock "$LOCK" systemd-run --user --scope --collect -q \
    -p "MemoryMax=$MEM" -p MemorySwapMax=0 "$@"
}

# Consumers that reference the crate without a pin are reported, not failed: a path
# dependency without `version` follows the sibling checkout automatically.
status=0
for spec in $CONSUMERS; do
  name="${spec%%:*}"; feature="${spec#*:}"
  dir="$HDB/$name"
  manifest="$dir/Cargo.toml"
  [ -f "$manifest" ] || { echo "DOWNSTREAM: FAIL — consumer $name has no Cargo.toml at $dir"; status=1; continue; }
  line="$(grep -m1 -E '^[[:space:]]*heliosdb-proxy[[:space:]]*=' "$manifest" || true)"
  if [ -z "$line" ]; then
    echo "DOWNSTREAM: $name — no heliosdb-proxy dependency in its root manifest (nothing to check)"
    continue
  fi
  pin="$(printf '%s' "$line" | sed -n 's/.*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p')"
  echo "DOWNSTREAM: $name pins '${pin:-<none>}' (feature: $feature)"
  if [ "$pin" != "=$VERSION" ] && [ -n "$pin" ]; then
    if [ "$MODE" = apply ]; then
      sed -i -E "s|(heliosdb-proxy[[:space:]]*=.*version[[:space:]]*=[[:space:]]*\")[^\"]*(\")|\1=$VERSION\2|" "$manifest"
      echo "DOWNSTREAM: $name pin rewritten to =$VERSION (commit this in $name)"
    else
      echo "DOWNSTREAM: FAIL — $name pins '$pin' but Proxy is $VERSION; every cargo command in" \
           "$name fails at resolution until the pin is =$VERSION (rerun with --apply)"
      status=1; continue
    fi
  fi
  [ "$MODE" = dry ] && { echo "DOWNSTREAM: $name — dry run, skipping cargo"; continue; }
  log="${TMPDIR:-/tmp}/downstream-$name.log"
  echo "DOWNSTREAM: $name — cargo check --features $feature (log: $log)"
  if ! (cd "$dir" && run_bounded cargo check --locked --features "$feature") >"$log" 2>&1; then
    # --locked fails when the lockfile must change for the new graph; that is expected
    # exactly at release time, so retry once allowing the lock to resolve.
    if grep -q -- '--locked' "$log" && (cd "$dir" && run_bounded cargo check --features "$feature") >"$log" 2>&1; then
      echo "DOWNSTREAM: $name — resolved with a lockfile update (commit Cargo.lock in $name)"
    else
      echo "DOWNSTREAM: FAIL — $name does not compile --features $feature against $VERSION. Last 25 lines:"
      tail -25 "$log" | sed 's/^/    /'
      status=1; continue
    fi
  fi
  echo "DOWNSTREAM: $name — PASS"
done

if [ "$status" -eq 0 ]; then [ "$MODE" = dry ] && echo "DOWNSTREAM: dry run — pins consistent with $VERSION (nothing compiled)" || echo "DOWNSTREAM: PASS — all consumers compile against $VERSION"; fi
exit "$status"
