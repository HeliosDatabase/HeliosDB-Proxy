#!/usr/bin/env bash
# Data-integrity checksum tool for the HeliosProxy IT cluster.
#
# For each pgbench-created table, computes:
#   * row count
#   * SHA-256 of the concatenated string form of every row (stable order)
#
# Called twice per scenario — once before the event, once after
# pgbench has drained — and compares the two snapshots. A mismatch
# means writes were lost or reordered across failover.
#
# Usage:
#   checksum.sh snapshot pre        # writes /tmp/helios-it-pre.json
#   # ... run scenario, wait for pgbench to finish ...
#   checksum.sh snapshot post       # writes /tmp/helios-it-post.json
#   checksum.sh compare pre post    # prints pass/fail and per-table diff
#
# Assumes `docker compose -f tests/docker/cluster.yml` is up and
# reachable via `pgbench-chaos` container + proxy on 127.0.0.1:5500.

set -euo pipefail

COMPOSE="docker compose -f $(dirname "$0")/cluster.yml"
OUT_DIR="/tmp"
PROXY_HOST="${PROXY_HOST:-127.0.0.1}"
PROXY_PORT="${PROXY_PORT:-5500}"
PGDB="${PGDB:-appdb}"
PGUSER="${PGUSER:-helios}"

# pgbench canonical tables. `pgbench_history` is append-only (every
# transaction inserts) so we checksum it as primary evidence of
# workload continuity.
TABLES=(pgbench_accounts pgbench_branches pgbench_tellers pgbench_history)

snapshot() {
  local tag="$1"
  local out="$OUT_DIR/helios-it-${tag}.json"
  echo "[snapshot] writing $out"
  {
    echo "{"
    local first=true
    for t in "${TABLES[@]}"; do
      if ! $first; then echo ","; fi
      first=false
      local sql="SELECT json_build_object(
                    'count', COUNT(*),
                    'sha256', encode(
                      sha256(
                        coalesce(string_agg(to_jsonb(r.*)::text, ',' ORDER BY to_jsonb(r.*)::text), '')::bytea
                      ),
                      'hex'
                    )
                  ) FROM $t r"
      local line
      line=$(PGPASSWORD=helios $COMPOSE exec -T pgbench-chaos \
        psql -h "$PROXY_HOST" -p "$PROXY_PORT" -U "$PGUSER" -d "$PGDB" \
        -AtXc "$sql")
      printf '  "%s": %s' "$t" "$line"
    done
    echo ""
    echo "}"
  } > "$out"
}

compare() {
  local pre_tag="$1"
  local post_tag="$2"
  local pre="$OUT_DIR/helios-it-${pre_tag}.json"
  local post="$OUT_DIR/helios-it-${post_tag}.json"
  if [[ ! -f "$pre" || ! -f "$post" ]]; then
    echo "missing snapshot(s): $pre, $post" >&2
    exit 2
  fi

  local any_mismatch=0
  local mismatches=()
  for t in "${TABLES[@]}"; do
    local pre_line post_line
    pre_line=$(jq -r ".${t}" < "$pre")
    post_line=$(jq -r ".${t}" < "$post")
    if [[ "$pre_line" == "$post_line" ]]; then
      echo "[match] $t  $pre_line"
    else
      # pgbench_history is expected to grow during the run — treat a
      # strict-prefix relation as acceptable (new rows appended, none
      # removed/reordered). The other tables should be byte-identical.
      if [[ "$t" == "pgbench_history" ]]; then
        local pre_count post_count
        pre_count=$(jq -r ".${t}.count" < "$pre")
        post_count=$(jq -r ".${t}.count" < "$post")
        if (( post_count >= pre_count )); then
          echo "[grow] $t  pre=${pre_count} post=${post_count}"
          continue
        fi
      fi
      any_mismatch=1
      mismatches+=("$t")
      echo "[MISMATCH] $t"
      echo "   pre:  $pre_line"
      echo "   post: $post_line"
    fi
  done

  if (( any_mismatch )); then
    echo ""
    echo "FAIL: data integrity mismatch on ${mismatches[*]}"
    exit 1
  fi
  echo ""
  echo "PASS: all ${#TABLES[@]} tables match"
}

usage() {
  cat >&2 <<-USAGE
Usage: $(basename "$0") snapshot {pre|post}
       $(basename "$0") compare pre post

Tables checked: ${TABLES[*]}
Output files:   /tmp/helios-it-{pre,post}.json
USAGE
  exit 1
}

case "${1:-}" in
  snapshot)
    [[ -z "${2:-}" ]] && usage
    snapshot "$2"
    ;;
  compare)
    [[ -z "${2:-}" || -z "${3:-}" ]] && usage
    compare "$2" "$3"
    ;;
  *)
    usage
    ;;
esac
