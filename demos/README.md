# HeliosProxy Demos

Five legacy Docker demos illustrating HeliosProxy scenarios. Review each demo's
limitations before running it. They are not acceptance evidence for database
promotion or zero data loss: the daemon requires an external promotion/fencing
authority and a compatible topology provider for promotion-based recovery.

## Prerequisites

- Docker and Docker Compose v2
- `psql` (PostgreSQL client)
- `curl`
- `jq`

## Demo Index

| # | Demo | Duration | Audience | What It Proves |
|---|------|----------|----------|----------------|
| 1 | [Impossible Query](impossible-query/) | 60 seconds | Executives, investors | Legacy scripted scenario; separate SQL sessions and masked errors prevent it from proving transaction recovery. |
| 2 | [Chaos Failover](chaos-failover/) | 5 minutes | DevOps, SREs | Measure errors and recovery while nodes are killed and restarted; no promotion authority configured. |
| 3 | [Bank Ledger](bank-ledger/) | 3 minutes | Developers, auditors | Check balance conservation during primary restarts; conservation alone cannot detect a lost or duplicated whole transfer. |
| 4 | [vs PgBouncer](vs-pgbouncer/) | 5 minutes | Technical evaluators | Record client errors and recovery for the actual HA configuration; neither proxy promotes a database in this setup. |
| 5 | [Lag-Aware Routing](lag-aware-routing/) | 5 minutes | Developers, DBAs | Induce replication lag and watch reads automatically reroute to healthy standbys. Read-your-writes consistency guaranteed. |

## Quick Start

Each demo is a single `docker compose up` away:

```bash
# Demo 1: Impossible Query (60s marketing wow)
cd impossible-query && docker compose up -d && ./demo.sh

# Demo 2: Chaos Failover (5-min stress test)
cd chaos-failover && docker compose up -d && ./chaos.sh

# Demo 3: Bank Ledger (ACID integrity proof)
cd bank-ledger && docker compose up -d
docker compose exec pg-primary psql -U app -d bankdb -f /dev/stdin < schema.sql
./workers.sh 20 120 & ./chaos.sh 5; wait; ./audit.sh

# Demo 4: vs PgBouncer (competitive comparison)
cd vs-pgbouncer && ./run-compare.sh

# Demo 5: Lag-Aware Routing (educational)
cd lag-aware-routing && docker compose up -d && ./observe.sh
```

## Cleanup

Each demo uses isolated Docker volumes. To tear down any demo:

```bash
cd <demo-directory>
docker compose down -v
```

## Port Ranges

Each demo uses a distinct port range to avoid conflicts:

| Demo | Client Port | Admin Port | PG Ports |
|------|------------|------------|----------|
| Impossible Query | 26432 | 29090 | 25432, 25442 |
| Chaos Failover | 36432 | 39090 | 35432, 35442, 35452 |
| Bank Ledger | 46432 | 49090 | 45432, 45442 |
| vs PgBouncer | 56432 | 59090 | 55432, 55442 |
| Lag-Aware Routing | 66432 | 69090 | 65432, 65442, 65462 |
