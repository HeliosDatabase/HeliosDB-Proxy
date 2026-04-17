# Demo 4: HeliosProxy vs PgBouncer

A side-by-side competitive comparison. Two identical PostgreSQL clusters run
identical workloads. Both primaries are killed simultaneously. One cluster
is fronted by PgBouncer, the other by HeliosProxy with Transaction Replay.

## What this proves

PgBouncer is a connection pooler -- it does not handle failover. When the
primary dies, clients get errors. HeliosProxy detects the failure, promotes
the standby, and replays in-flight transactions. The difference is visible
in a single table of numbers.

## Prerequisites

- Docker and Docker Compose
- `psql` client installed locally
- Ports 55432-56532 and 59090 available

## How to run

```bash
./run-compare.sh
```

The script handles everything:

1. Starts both clusters (HeliosProxy + PgBouncer)
2. Creates identical schema on both
3. Starts 20 concurrent workers on each proxy
4. Waits 30s for warm-up
5. Kills BOTH primaries simultaneously
6. Waits for recovery
7. Stops workloads and collects metrics
8. Prints comparison table and writes `results/report.md`

## How to interpret results

Look at these columns:

- **Client errors** -- PgBouncer will show many; HeliosProxy should show zero or near-zero
- **Rows lost** -- Queries the client thought succeeded but are not in the database
- **Max client downtime** -- How long clients saw errors

## Architecture

```
Workload ──> HeliosProxy ──> hp-primary (killed)
                         └─> hp-standby (promoted)

Workload ──> PgBouncer ───> pb-primary (killed)
                        └─> pb-standby (unused by PgBouncer)
```

## Cleanup

```bash
docker compose down -v
rm -rf results/
```
