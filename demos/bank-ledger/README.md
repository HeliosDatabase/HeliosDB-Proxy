# Demo 3: The Auditor's Demo (Bank Ledger)

A financial integrity stress test. 20 concurrent workers perform random bank
transfers while the primary database is killed and restarted 5 times. At the
end, a balance-sum query checks conservation of the seeded money.

## What invariant is tested?

Every transfer is a paired DEBIT + CREDIT inside a single transaction.
The total balance across all 100 accounts must always equal exactly
**$1,000,000.00** -- the original seed amount. If any transaction is
partially applied, this number can be wrong. A lost or duplicated complete
DEBIT+CREDIT pair can preserve the sum, so this check alone does not prove
exactly-once execution or absence of lost acknowledged transfers.

## How to run

```bash
# 1. Start the cluster
docker compose up -d
docker compose exec pg-primary psql -U app -d bankdb -f /dev/stdin < schema.sql

# 2. Run workers and chaos in parallel
./workers.sh 20 120 &
./chaos.sh 5
wait

# 3. Audit
./audit.sh
```

## Expected results

```
Total balance:  $1000000.00
Transfers done: <thousands>
Balance range:  $XXX.XX ... $XXXXX.XX
Negative accts: 0

RESULT: PASS

The money quote:
SUM(balance) = $1000000.00
Zero cents lost across all failovers.
```

The legacy audit's "Zero cents lost" label describes its balance-sum check,
not a durability guarantee. Compare unique acknowledged transfer IDs with
persisted ledger entries to establish loss or duplication.

This demo restarts the original primary; the daemon does not promote a standby.
Promotion-based recovery needs an external manager with fencing and a compatible
topology provider. Transaction Replay has explicit eligibility limits, and an
unknown `COMMIT` outcome produces `08007` rather than a blind replay. Client
errors during an outage are possible; inspect the actual workload results.

## Configuration

- **Workers:** 20 concurrent (configurable: `./workers.sh 50 180`)
- **Chaos cycles:** 5 kills (configurable: `./chaos.sh 10`)
- **Transfer amount:** random $1-$500 per transfer
- **Pool mode:** transaction
- **Sync replication:** enabled (synchronous_standby_names = '*')

## Cleanup

```bash
docker compose down -v
```
