#!/bin/bash
# Ledger client for the HA lab (runs inside the `ledger` container).
#
#   ledger.sh init                       create accounts/transfers through proxy-a
#   ledger.sh run SECONDS WORKERS        transfers until the deadline; even workers
#                                        use proxy-a, odd workers proxy-b
#   ledger.sh check HOST [OTHER_HOST]    verify against the database on HOST
#                                        (OTHER_HOST: a node that must hold no
#                                        acknowledged transfer HOST lacks)
#
# Each transfer is one simple-query batch
#   BEGIN; debit; credit; INSERT transfers(id uuid PRIMARY KEY); COMMIT;
# and its client-visible outcome is appended to /results/outcomes.tsv:
#   id  outcome  sqlstate  via  start_ms  end_ms
# outcome: committed | error (an ERROR with a SQLSTATE: the proxy or server
#          reports the batch rolled back) | unknown (08007, a FATAL, or the
#          connection broke after the batch was sent) |
#          not_sent (no connection: the batch never reached a server)
set -u

ACCOUNTS=100
START_BALANCE=10000
OUT=/results/outcomes.tsv
PSQL=(psql -X -q -At -U lab -d lab -v ON_ERROR_STOP=1 -v VERBOSITY=sqlstate)
# Every lab service listens on 127.0.0.1 (host network); `at NAME` sets AT to
# the psql connection arguments for that service.
at() {
  local port
  case "$1" in
    proxy-a) port=27710 ;; proxy-b) port=27711 ;;
    pg-a) port=27701 ;; pg-b) port=27702 ;; pg-c) port=27703 ;;
    *) echo "unknown lab endpoint $1" >&2; return 1 ;;
  esac
  AT=(-h 127.0.0.1 -p "$port")
}

now_ms() { date +%s%3N; }

transfer() {
  local via=$1 id from to amount sql t0 t1 err rc level state outcome
  id=$(cat /proc/sys/kernel/random/uuid)
  from=$((RANDOM % ACCOUNTS + 1))
  to=$(((from + RANDOM % (ACCOUNTS - 1)) % ACCOUNTS + 1))
  amount=$((RANDOM % 50 + 1))
  sql="BEGIN; UPDATE accounts SET balance = balance - $amount WHERE id = $from;
UPDATE accounts SET balance = balance + $amount WHERE id = $to;
INSERT INTO transfers (id, from_id, to_id, amount, via) VALUES ('$id', $from, $to, $amount, '$via');
COMMIT;"
  t0=$(now_ms)
  at "$via"
  err=$(PGCONNECT_TIMEOUT=5 "${PSQL[@]}" "${AT[@]}" -c "$sql" 2>&1 >/dev/null)
  rc=$?
  t1=$(now_ms)
  level=$(grep -oE '^(ERROR|FATAL):  [0-9A-Z]{5}' <<<"$err" | head -1)
  state=${level##* }
  if [ "$rc" -eq 0 ]; then
    outcome=committed
  elif [ "$state" = 08007 ] || [ "${level%%:*}" = FATAL ]; then
    # 08007: the proxy cannot tell whether COMMIT happened. FATAL: the session
    # was terminated, possibly after the commit.
    outcome=unknown
  elif [ -n "$state" ]; then
    outcome=error
  elif grep -qE 'could not connect|Connection refused|could not translate host|timeout expired' <<<"$err"; then
    outcome=not_sent
  else
    outcome=unknown
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$outcome" "${state:--}" "$via" "$t0" "$t1"
}

case "${1:-}" in
init)
  at proxy-a
  "${PSQL[@]}" "${AT[@]}" <<SQL
CREATE TABLE accounts (id int PRIMARY KEY, balance bigint NOT NULL);
CREATE TABLE transfers (id uuid PRIMARY KEY, from_id int NOT NULL, to_id int NOT NULL,
                        amount int NOT NULL, via text NOT NULL, at timestamptz DEFAULT now());
INSERT INTO accounts SELECT g, $START_BALANCE FROM generate_series(1, $ACCOUNTS) g;
SQL
  ;;
run)
  secs=$2 workers=$3
  : >"$OUT"
  deadline=$(($(date +%s) + secs))
  for w in $(seq 0 $((workers - 1))); do
    if ((w % 2 == 0)); then via=proxy-a; else via=proxy-b; fi
    (
      while (($(date +%s) < deadline)); do
        transfer "$via" >>"/results/outcomes-$w.tsv"
      done
    ) &
  done
  wait
  cat /results/outcomes-*.tsv >"$OUT"
  rm -f /results/outcomes-*.tsv
  cut -f2 "$OUT" | sort | uniq -c | awk '{printf "%s=%s ", $2, $1} END {print ""}'
  ;;
check)
  host=$2 other=${3:-}
  # Load the client's view next to the ledger, on the node being checked.
  at "$host"
  "${PSQL[@]}" "${AT[@]}" <<SQL
CREATE TEMP TABLE outcomes (id uuid, outcome text, sqlstate text, via text, t0 bigint, t1 bigint);
\copy outcomes FROM '$OUT'
SELECT json_build_object(
  'node', '$host',
  'transfers_attempted', (SELECT count(*) FROM outcomes),
  'outcomes', (SELECT json_object_agg(outcome, n) FROM
                 (SELECT outcome, count(*) n FROM outcomes GROUP BY outcome) s),
  'rows_in_ledger', (SELECT count(*) FROM transfers),
  'balance_sum', (SELECT sum(balance) FROM accounts),
  'balance_sum_expected', $ACCOUNTS * $START_BALANCE,
  'committed_missing', (SELECT count(*) FROM outcomes o WHERE o.outcome = 'committed'
                          AND NOT EXISTS (SELECT 1 FROM transfers t WHERE t.id = o.id)),
  'failed_but_present', (SELECT count(*) FROM outcomes o WHERE o.outcome IN ('error', 'not_sent')
                          AND EXISTS (SELECT 1 FROM transfers t WHERE t.id = o.id)),
  'unknown_present', (SELECT count(*) FROM outcomes o WHERE o.outcome = 'unknown'
                        AND EXISTS (SELECT 1 FROM transfers t WHERE t.id = o.id)),
  'unknown_absent', (SELECT count(*) FROM outcomes o WHERE o.outcome = 'unknown'
                       AND NOT EXISTS (SELECT 1 FROM transfers t WHERE t.id = o.id)),
  'not_from_client', (SELECT count(*) FROM transfers t
                        WHERE NOT EXISTS (SELECT 1 FROM outcomes o WHERE o.id = t.id)),
  'committed_by_proxy', (SELECT json_object_agg(via, n) FROM
                           (SELECT via, count(*) n FROM outcomes WHERE outcome = 'committed'
                            GROUP BY via) s)
);
SQL
  if [ -n "$other" ]; then
    # Transfers the other node holds but $host does not: written after the
    # two diverged. An acknowledged one is a split-brain loss.
    at "$other"
    "${PSQL[@]}" "${AT[@]}" -c "COPY (SELECT id FROM transfers) TO STDOUT" >/results/other-ids.txt 2>/results/other.err \
      || { echo '{"other_node":"unreachable"}'; exit 0; }
    at "$host"
    "${PSQL[@]}" "${AT[@]}" <<SQL
CREATE TEMP TABLE outcomes (id uuid, outcome text, sqlstate text, via text, t0 bigint, t1 bigint);
\copy outcomes FROM '$OUT'
CREATE TEMP TABLE other_ids (id uuid);
\copy other_ids FROM '/results/other-ids.txt'
SELECT json_build_object(
  'other_node', '$other',
  'only_on_other', (SELECT count(*) FROM other_ids x
                      WHERE NOT EXISTS (SELECT 1 FROM transfers t WHERE t.id = x.id)),
  'acknowledged_only_on_other', (SELECT count(*) FROM other_ids x JOIN outcomes o USING (id)
                                   WHERE o.outcome = 'committed'
                                   AND NOT EXISTS (SELECT 1 FROM transfers t WHERE t.id = x.id))
);
SQL
  fi
  ;;
*)
  echo "usage: ledger.sh init | run SECONDS WORKERS | check HOST [OTHER_HOST]" >&2
  exit 2
  ;;
esac
