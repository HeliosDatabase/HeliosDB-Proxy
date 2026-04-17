# Demo 5: Lag-Aware Routing

An educational demo showing how HeliosProxy detects replication lag and automatically reroutes reads to healthy standbys, plus read-your-writes (RYW) consistency guarantees.

## What This Teaches

### Why Replication Lag Matters

PostgreSQL streaming replication is asynchronous by default. A standby can fall seconds (or minutes) behind the primary under heavy write load, network issues, or resource contention. If your application reads from a lagging standby, it gets **stale data** — a user might create a record, refresh the page, and not see it.

### How Lag-Aware Routing Protects Against Stale Reads

HeliosProxy continuously monitors each standby's replication lag via health checks (every 2s in this demo). When a standby's lag exceeds `max_replica_lag_ms` (100ms here), the proxy **stops routing reads to it** until it catches up. Reads automatically shift to healthy standbys or the primary.

### How Read-Your-Writes (RYW) Works

1. A client **writes** (INSERT/UPDATE/DELETE) through the proxy.
2. The proxy **tags the session** with the write's WAL LSN.
3. Subsequent **reads in that session** are routed only to nodes that have replayed past that LSN — typically the primary or the synchronous standby.
4. Once the async standby catches up past the tagged LSN, it becomes eligible for reads again.

This guarantees a user always sees their own changes, even with read/write splitting.

## Architecture

```
                    ┌──────────────────┐
                    │   HeliosProxy    │
                    │  lag routing +   │
                    │  RYW enabled     │
                    └────┬───┬───┬─────┘
                         │   │   │
              ┌──────────┘   │   └──────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ Primary  │  │  Sync    │  │  Async   │
        │ pg:65432 │─▶│  Standby │  │  Standby │
        │ (writes) │  │ pg:65442 │  │ pg:65462 │
        └──────────┘  │ (low lag)│  │(has lag) │
                      └──────────┘  └──────────┘
```

## Step-by-Step

### 1. Start the cluster

```bash
docker compose up -d
docker compose ps        # Wait for all 4 services healthy
```

### 2. Watch routing decisions

```bash
./observe.sh
```

You will see reads round-robin across **both** standbys (sync and async) since lag is near zero.

### 3. Induce lag on the async standby

In a second terminal:

```bash
./induce-lag.sh on       # Adds 500ms network delay via tc/netem
```

### 4. Watch routing shift

Back in the `observe.sh` terminal, within a few seconds you will see:
- The async standby's lag climbs above 100ms
- Reads **stop** going to the async standby
- All reads shift to the **sync standby** (or primary)

### 5. Test RYW consistency

The observer automatically tests RYW each iteration:
- It INSERTs a uniquely-tagged row through the proxy
- It immediately SELECTs that row in the same session
- Even with the async standby lagging, the read finds the row because RYW pins the read to the primary or sync standby

### 6. Remove lag, watch recovery

```bash
./induce-lag.sh off      # Removes network delay
```

Within seconds, the async standby catches up and becomes eligible for reads again. The observer shows reads returning to round-robin across both standbys.

### 7. Check lag status anytime

```bash
./induce-lag.sh status   # Shows per-node lag from proxy admin API
```

### 8. Tear down

```bash
docker compose down -v
```

## Ports

| Service           | Port  | Purpose                  |
|-------------------|-------|--------------------------|
| pg-primary        | 65432 | Direct primary access    |
| pg-standby-sync   | 65442 | Direct sync standby      |
| pg-standby-async  | 65462 | Direct async standby     |
| heliosproxy       | 66432 | Proxy client connections |
| heliosproxy admin | 69090 | Admin/metrics API        |

## Key Configuration

| Setting              | Value | Effect                                          |
|----------------------|-------|-------------------------------------------------|
| `lag_routing_enabled`| true  | Enable lag-based read routing                   |
| `max_replica_lag_ms` | 100   | Standbys above 100ms are excluded from reads    |
| `ryw_enabled`        | true  | After a write, reads pin to consistent nodes    |
| `read_strategy`      | round_robin | Distribute reads across eligible standbys |
| `check_interval_secs`| 2     | Health/lag checks every 2 seconds               |
