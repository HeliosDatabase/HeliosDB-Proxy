#!/usr/bin/env bash
# Bank Ledger Demo — Audit: verify the $1,000,000 invariant
set -euo pipefail

PROXY_HOST="${PROXY_HOST:-localhost}"
PROXY_PORT="${PROXY_PORT:-46432}"
STANDBY_PORT="${STANDBY_PORT:-45442}"
CONNSTR="postgresql://app:apppass@${PROXY_HOST}:${PROXY_PORT}/bankdb"
STANDBY_CONNSTR="postgresql://app:apppass@${PROXY_HOST}:${STANDBY_PORT}/bankdb"
EXPECTED="1000000.00"

echo "============================================"
echo "  BANK LEDGER AUDIT"
echo "============================================"
echo ""

# --- Query via proxy ---
echo "--- Via HeliosProxy (port $PROXY_PORT) ---"

total=$(psql "$CONNSTR" -tAc "SELECT SUM(balance) FROM accounts;")
transfers=$(psql "$CONNSTR" -tAc "SELECT COUNT(*) FROM transfers;")
minmax=$(psql "$CONNSTR" -tAc "SELECT MIN(balance), MAX(balance) FROM accounts;" | tr '|' ' ')
min_bal=$(echo "$minmax" | awk '{print $1}')
neg_count=$(psql "$CONNSTR" -tAc "SELECT COUNT(*) FROM accounts WHERE balance < 0;")

echo "Total balance:  \$$total"
echo "Transfers done: $transfers"
echo "Balance range:  \$$(echo "$minmax" | sed 's/ / ... $/')"
echo "Negative accts: $neg_count"
echo ""

# --- Query standby directly ---
echo "--- Via Standby directly (port $STANDBY_PORT) ---"

standby_total=$(psql "$STANDBY_CONNSTR" -tAc "SELECT SUM(balance) FROM accounts;" 2>/dev/null || echo "UNREACHABLE")
echo "Total balance:  \$$standby_total"
echo ""

# --- Verdict ---
echo "============================================"
if [ "$total" = "$EXPECTED" ] && [ "$neg_count" = "0" ]; then
  echo "  RESULT: PASS"
  echo ""
  echo "  The money quote:"
  echo "  SUM(balance) = \$$total"
  echo "  Zero cents lost across all failovers."
  echo "  $transfers transfers completed successfully."
  echo "============================================"
  exit 0
else
  echo "  RESULT: FAIL"
  echo ""
  if [ "$total" != "$EXPECTED" ]; then
    echo "  INVARIANT VIOLATED: expected \$$EXPECTED, got \$$total"
  fi
  if [ "$neg_count" != "0" ]; then
    echo "  $neg_count accounts have negative balances!"
  fi
  echo "============================================"
  exit 1
fi
