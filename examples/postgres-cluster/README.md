# Postgres Cluster Example

A self-contained 3-node PostgreSQL cluster fronted by HeliosProxy with automatic read/write splitting and connection pooling.

## Architecture

```
                     +-----------+
  Client  --------→ | HeliosProxy| :6432 (PG wire) / :9090 (admin API)
                     +-----+-----+
                           |
         +-----------+-----+-----+-----------+
         |           |                       |
   +-----+-----+  +------+------+  +--------+----+
   | pg-primary |  | pg-standby1 |  | pg-standby2 |
   |  (R/W)    |  |  (read-only)|  |  (read-only) |
   +-----+-----+  +------+------+  +--------+----+
         |                ↑                  ↑
         +--- streaming replication ---------+
```

## Quick Start

```bash
# Start everything
docker compose up -d

# Watch proxy logs
docker compose logs -f proxy

# Wait for health
curl http://localhost:9090/health
```

## Connecting

All connections go through the proxy on port 6432:

```bash
# psql
psql postgresql://app:apppass@localhost:6432/appdb

# Or with flags
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb
```

## What to Observe

### Read/Write Splitting

```sql
-- This INSERT is routed to the primary
INSERT INTO test (name) VALUES ('hello');

-- This SELECT is load-balanced across standbys
SELECT * FROM test;
```

### Admin API

```bash
# Health status
curl http://localhost:9090/health

# List backend nodes and their health
curl http://localhost:9090/nodes | jq .

# View current metrics (connections, queries, failovers)
curl http://localhost:9090/metrics | jq .

# Prometheus-format metrics
curl http://localhost:9090/metrics/prometheus

# Connection pool statistics
curl http://localhost:9090/pools | jq .

# Current proxy configuration
curl http://localhost:9090/config | jq .
```

### Disable a Node for Maintenance

```bash
# Take standby-1 out of rotation
curl -X POST http://localhost:9090/nodes/pg-standby1:5432/disable

# Confirm it is disabled
curl http://localhost:9090/nodes | jq .

# Re-enable it
curl -X POST http://localhost:9090/nodes/pg-standby1:5432/enable
```

## Configuration

See `proxy.toml` for all tunable parameters:

- `pool_mode.mode` -- `session`, `transaction`, or `statement`
- `load_balancer.read_strategy` -- `round_robin`, `least_connections`, `latency_based`, `random`
- `health.failure_threshold` -- consecutive failures before marking a node unhealthy
- `tr_enabled` / `tr_mode` -- Transaction Replay for session continuity during failover

## Teardown

```bash
docker compose down -v
```
