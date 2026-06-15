# Chaos Failover Stress Test

A 5-minute stress test that runs continuous database workload while randomly killing
and restarting PostgreSQL nodes. Proves HeliosProxy maintains high availability under
sustained failure conditions.

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
- **HeliosProxy**: Connection router with TR, lag-aware routing, auto-failover

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
- Primary failover and promotion events
- Pool metrics during failures

Watch the workload for:
- Failed operations during kills (should be minimal)
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

- **Success rate**: 95%+ of operations succeed despite continuous node kills
- **Failover time**: Proxy detects failures within ~4 seconds (2s interval, 2 failures)
- **Data consistency**: All reachable nodes converge to the same data after recovery
- **Zero data loss**: Transaction Replay ensures committed transactions survive failover

## Cleanup

```bash
docker compose down -v --remove-orphans
```
