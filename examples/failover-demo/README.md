# Failover Demo

Step-by-step demonstration of HeliosProxy automatic failover with Transaction Replay (TR).

## What This Demonstrates

1. **Health-check driven failover** -- the proxy detects a dead backend within seconds and re-routes traffic.
2. **Transaction Replay (TR)** -- in-flight sessions are replayed on the new target so clients do not see connection errors.
3. **Automatic recovery** -- when the failed node comes back, health checks detect it and restore it to the routing pool.

## Prerequisites

- Docker and Docker Compose
- `curl` and `jq` for API inspection
- `psql` (optional, for connecting through the proxy)

## Step-by-Step

### 1. Start the Cluster

```bash
docker compose up -d
docker compose logs -f proxy
```

Wait until you see the proxy report all three nodes as healthy.

### 2. Verify Healthy State

```bash
# All nodes should show healthy=true
curl -s http://localhost:9090/nodes | jq '.[] | {name: .name, healthy: .healthy}'
```

Expected output:

```json
{ "name": "primary",   "healthy": true }
{ "name": "standby-1", "healthy": true }
{ "name": "standby-2", "healthy": true }
```

### 3. Run Some Traffic

Open a second terminal and run queries through the proxy:

```bash
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
  "CREATE TABLE IF NOT EXISTS demo (id serial, ts timestamptz default now());"

# Insert a few rows (routed to primary)
for i in $(seq 1 5); do
  PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
    "INSERT INTO demo DEFAULT VALUES;"
done

# Read them back (routed to a standby)
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
  "SELECT * FROM demo;"
```

### 4. Kill the Primary

```bash
docker compose stop pg-primary
```

Watch the proxy logs. Within ~4 seconds you should see:

- Health check failure detected for `primary`
- Node marked unhealthy after 2 consecutive failures
- Failover counter incremented

```bash
# Confirm the primary is now unhealthy
curl -s http://localhost:9090/nodes | jq '.[] | {name: .name, healthy: .healthy}'
```

```bash
# Check failover count
curl -s http://localhost:9090/metrics | jq '.failovers'
```

### 5. Verify Reads Still Work

Read queries continue to be served by the standbys:

```bash
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
  "SELECT * FROM demo;"
```

Write queries will wait up to `write_timeout_secs` (15s) for a primary, then return an error:

```bash
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
  "INSERT INTO demo DEFAULT VALUES;" 2>&1 || echo "Write failed (expected -- no primary)"
```

### 6. Restart the Primary

```bash
docker compose start pg-primary
```

Watch the proxy logs again:

- Health check succeeds for `primary`
- After 2 consecutive successes, node is marked healthy
- Writes resume automatically

```bash
# Confirm recovery
curl -s http://localhost:9090/nodes | jq '.[] | {name: .name, healthy: .healthy}'
```

```bash
# Writes work again
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
  "INSERT INTO demo DEFAULT VALUES RETURNING *;"
```

### 7. (Optional) Kill a Standby

```bash
docker compose stop pg-standby1

# Reads still work -- routed to standby-2
PGPASSWORD=apppass psql -h localhost -p 6432 -U app -d appdb -c \
  "SELECT count(*) FROM demo;"

# Bring it back
docker compose start pg-standby1
```

## Configuration Highlights

The `proxy.toml` in this example differs from the basic cluster:

| Parameter | Value | Why |
|-----------|-------|-----|
| `tr_mode` | `transaction` | Full in-flight transaction replay after failover |
| `health.check_interval_secs` | `2` | Fast detection (default is 5) |
| `health.failure_threshold` | `2` | Failover after ~4s (default is 3) |
| `write_timeout_secs` | `15` | Shorter write timeout for demo visibility |
| `load_balancer.read_strategy` | `least_connections` | Distribute reads to least-loaded standby |

## Teardown

```bash
docker compose down -v
```
