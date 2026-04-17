# Demo 3: The Auditor's Demo (Bank Ledger)

A financial integrity stress test. 20 concurrent workers perform random bank
transfers while the primary database is killed and restarted 5 times. At the
end, a single query proves that not a single cent was lost.

## What invariant is tested?

Every transfer is a paired DEBIT + CREDIT inside a single transaction.
The total balance across all 100 accounts must always equal exactly
**$1,000,000.00** -- the original seed amount. If any transaction is
partially applied, double-applied, or lost during failover, this number
will be wrong.

## How to run

```bash
# 1. Start the cluster
docker compose up -d --build
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

HeliosProxy's Transaction Replay captures every statement in the active
transaction. When the primary dies mid-commit, the proxy detects the failure,
promotes the standby, and replays the exact sequence of statements. The
client sees a brief pause but never an error -- and the DEBIT+CREDIT pair
is always atomic.

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
