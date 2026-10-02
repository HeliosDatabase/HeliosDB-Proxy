# Chaos Failover Stress Test

A 5-minute stress test that runs continuous database workload while randomly killing
and restarting PostgreSQL nodes. This legacy demo exercises outage detection and
recovery of the original primary; it does not configure a promotion authority.

## Architecture

```
                        +------------------+
                        |   HeliosProxy    |
                        |  (port 36432)    |
                        +--------+---------+
                                 |
              +------------------+------------------+
              |                  |                  |
     +--------v-------+ +-------v--------+ +-------v--------+
     |  pg-primary    | | pg-standby-sync| | pg-standby-async|
     |  (port 35432)  | |  (port 35442)  | |  (port 35462)  |
     +----------------+ +----------------+ +----------------+
```

- **pg-primary**: Read-write primary
- **pg-standby-sync**: Synchronous streaming standby
- **pg-standby-async**: Asynchronous streaming standby
- **HeliosProxy**: Connection router with TR and health checks

The static configuration does not promote standbys. Writes wait for the original
primary to recover and fail after `write_timeout_secs` if it remains unavailable.
For promotion-based failover, configure an external HA manager with fencing and
a compatible [topology provider](../../docs/topology-providers.md).

The chaos monkey kills nodes with weighted probability: 50% primary, 25% each standby.

## Prerequisites

- Docker and Docker Compose
- `psql` (PostgreSQL client)
- `curl` and `python3`
- Three terminal windows

## How to Run

### 1. Start the cluster

```bash
docker compose up -d
```

Wait for all services to be healthy:

```bash
docker compose ps
```

### 2. Open three terminals

**Terminal 1 — Workload generator:**
```bash
./workload.sh          # Runs until Ctrl+C
./workload.sh 300      # Runs for 300 seconds
```

**Terminal 2 — Chaos monkey:**
```bash
./chaos.sh             # 300s default
./chaos.sh 600         # 600s of chaos
```

**Terminal 3 — Live dashboard:**
```bash
./dashboard.sh
```

### 3. Observe

Watch the dashboard for:
- Node status changes (healthy -> unhealthy -> healthy)
- Primary unavailability and recovery
- Pool metrics during failures

Watch the workload for:
- Failed operations during kills (including bounded write timeouts)
- Recovery after restarts
- Overall success rate

### 4. Verify

After the chaos test completes:

```bash
./verify.sh
```

This checks:
- Total rows in the workload table
- No gaps in the iteration sequence
- All nodes have consistent data

## Expected Results

- **Success rate**: Measure it; primary outages can cause write failures until restart
- **Detection time**: Health checks use a 2s interval and a 2-failure threshold; detection is separate from promotion
- **Data consistency**: All reachable nodes converge to the same data after recovery
- **Durability**: Determined by PostgreSQL replication and commit settings; Transaction Replay does not guarantee zero data loss

## Cleanup

```bash
docker compose down -v --remove-orphans
```
