# Demo 4: HeliosProxy vs PgBouncer

A side-by-side competitive comparison. Two identical PostgreSQL clusters run
identical workloads. Both primaries are killed simultaneously. One cluster
is fronted by PgBouncer, the other by HeliosProxy with Transaction Replay.

## What this measures

The scripts collect client successes, errors, and recovery times under the
configured outage. Neither proxy promotes a database in this setup. HeliosProxy
can follow an external promotion through a compatible topology provider and
replay eligible work; it does not guarantee error-free recovery or replay an
unknown `COMMIT`. Review the actual HA configuration before interpreting results.

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

- **Client errors** -- Measure both; no zero-error expectation is established
- **Rows lost** -- Queries the client thought succeeded but are not in the database
- **Max client downtime** -- How long clients saw errors

## Architecture

```
Workload ──> HeliosProxy ──> hp-primary (killed)
                         └─> hp-standby (requires external promotion)

Workload ──> PgBouncer ───> pb-primary (killed)
                        └─> pb-standby (unused by PgBouncer)
```

## Cleanup

```bash
docker compose down -v
rm -rf results/
```
