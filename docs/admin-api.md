# HeliosProxy Admin API Reference

The Admin API provides REST endpoints for monitoring, management, and SQL routing. It runs on a dedicated TCP listener, separate from the PostgreSQL client port.

**Default address:** `0.0.0.0:9090` (configurable via `admin_address` in `config.toml` or `--admin` on the command line).

All responses use `Content-Type: application/json` unless otherwise noted.

---

## Endpoint Summary

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check (at least one healthy backend) |
| `GET` | `/health/live` | Simple alive indicator |
| `GET` | `/nodes` | List all backend nodes with health status |
| `GET` | `/nodes/{address}` | Single node health status |
| `POST` | `/nodes/{address}/enable` | Enable a disabled node |
| `POST` | `/nodes/{address}/disable` | Disable a node (remove from routing) |
| `GET` | `/config` | Current configuration snapshot |
| `GET` | `/metrics` | Proxy metrics (JSON) |
| `GET` | `/metrics/prometheus` | Proxy metrics (Prometheus text format) |
| `GET` | `/sessions` | Active session count |
| `GET` | `/pools` | Connection pool statistics |
| `GET` | `/version` | Proxy version and build information |
| `POST` | `/api/sql` | Execute SQL with transparent write routing |

---

## Health Endpoints

### GET /health

Basic liveness check. Returns 200 if the proxy process is running.

```bash
curl http://localhost:9090/health
```

**Response (200 OK):**

```json
{
  "status": "ok"
}
```

---

### GET /health/ready

Readiness check. Returns 200 if at least one backend node is healthy. Returns 503 if no healthy nodes are available. Suitable for Kubernetes readiness probes.

```bash
curl http://localhost:9090/health/ready
```

**Response (200 OK):**

```json
{
  "ready": true,
  "message": "Proxy is ready"
}
```

**Response (503 Service Unavailable):**

```json
{
  "ready": false,
  "message": "Proxy is not ready"
}
```

---

### GET /health/live

Simple alive indicator. Always returns 200 if the admin server is accepting connections. Suitable for Kubernetes liveness probes.

```bash
curl http://localhost:9090/health/live
```

**Response (200 OK):**

```json
{
  "alive": true
}
```

---

## Node Management

### GET /nodes

List all configured backend nodes with their current health status.

```bash
curl http://localhost:9090/nodes
```

**Response (200 OK):**

```json
[
  {
    "address": "db-primary.internal:5432",
    "healthy": true,
    "last_check": "2026-03-26T10:15:30.123Z",
    "failure_count": 0,
    "last_error": null,
    "latency_ms": 0.5,
    "replication_lag_bytes": null
  },
  {
    "address": "db-standby-1.internal:5432",
    "healthy": true,
    "last_check": "2026-03-26T10:15:30.456Z",
    "failure_count": 0,
    "last_error": null,
    "latency_ms": 1.2,
    "replication_lag_bytes": 1024
  },
  {
    "address": "db-replica-1.internal:5432",
    "healthy": false,
    "last_check": "2026-03-26T10:15:25.789Z",
    "failure_count": 3,
    "last_error": "connection refused",
    "latency_ms": 0.0,
    "replication_lag_bytes": null
  }
]
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `address` | string | Node address in `host:port` format. |
| `healthy` | bool | Whether the node is currently passing health checks. |
| `last_check` | string | ISO 8601 timestamp of the most recent health check. |
| `failure_count` | u32 | Number of consecutive health check failures. Resets to 0 when a check succeeds. |
| `last_error` | string or null | Error message from the most recent failed health check. |
| `latency_ms` | f64 | Round-trip latency of the most recent successful health check, in milliseconds. |
| `replication_lag_bytes` | u64 or null | Replication lag in bytes. Only reported for standby and replica nodes. |

---

### GET /nodes/{address}

Retrieve health status for a single node. The `{address}` parameter is the node's `host:port` string.

```bash
curl http://localhost:9090/nodes/db-primary.internal:5432
```

**Response (200 OK):**

```json
{
  "address": "db-primary.internal:5432",
  "healthy": true,
  "last_check": "2026-03-26T10:15:30.123Z",
  "failure_count": 0,
  "last_error": null,
  "latency_ms": 0.5,
  "replication_lag_bytes": null
}
```

**Response (404 Not Found):**

```json
{
  "error": "Node not found"
}
```

---

### POST /nodes/{address}/enable

Re-enable a previously disabled node. The node will begin receiving health checks and, if healthy, will be included in query routing.

```bash
curl -X POST http://localhost:9090/nodes/db-replica-1.internal:5432/enable
```

**Response (200 OK):**

```json
{
  "status": "enabled"
}
```

---

### POST /nodes/{address}/disable

Disable a node. The node is immediately removed from query routing. Active connections to the node are allowed to complete, but no new queries are routed to it.

This is useful for performing maintenance on a backend node without removing it from the configuration.

```bash
curl -X POST http://localhost:9090/nodes/db-replica-1.internal:5432/disable
```

**Response (200 OK):**

```json
{
  "status": "disabled"
}
```

---

## Configuration

### GET /config

Returns a snapshot of the current proxy configuration.

```bash
curl http://localhost:9090/config
```

**Response (200 OK):**

```json
{
  "listen_address": "0.0.0.0:6432",
  "admin_address": "0.0.0.0:9090",
  "tr_enabled": true,
  "tr_mode": "session",
  "pool_min_connections": 5,
  "pool_max_connections": 100,
  "nodes": [
    {
      "address": "db-primary.internal:5432",
      "role": "primary",
      "weight": 100,
      "enabled": true
    },
    {
      "address": "db-standby-1.internal:5432",
      "role": "standby",
      "weight": 100,
      "enabled": true
    }
  ]
}
```

---

## Metrics

### GET /metrics

Returns proxy metrics in JSON format.

```bash
curl http://localhost:9090/metrics
```

**Response (200 OK):**

```json
{
  "connections_accepted": 15234,
  "connections_closed": 15100,
  "connections_active": 134,
  "queries_processed": 892451,
  "bytes_received": 45623891,
  "bytes_sent": 189234567,
  "failovers": 1
}
```

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `connections_accepted` | u64 | Total client connections accepted since startup. |
| `connections_closed` | u64 | Total client connections closed since startup. |
| `connections_active` | u64 | Currently active client connections (`accepted - closed`). |
| `queries_processed` | u64 | Total queries routed to backend nodes. |
| `bytes_received` | u64 | Total bytes received from clients. |
| `bytes_sent` | u64 | Total bytes sent to clients. |
| `failovers` | u64 | Total number of failover events since startup. |

---

### GET /metrics/prometheus

Returns proxy metrics in Prometheus text exposition format. Suitable for scraping by a Prometheus server.

```bash
curl http://localhost:9090/metrics/prometheus
```

**Response (200 OK):**

```json
{
  "text": "# HELP heliosdb_proxy_connections_total Total connections accepted\n# TYPE heliosdb_proxy_connections_total counter\nheliosdb_proxy_connections_total 15234\n# HELP heliosdb_proxy_connections_closed Total connections closed\n# TYPE heliosdb_proxy_connections_closed counter\nheliosdb_proxy_connections_closed 15100\n# HELP heliosdb_proxy_queries_total Total queries processed\n# TYPE heliosdb_proxy_queries_total counter\nheliosdb_proxy_queries_total 892451\n# HELP heliosdb_proxy_bytes_received_total Total bytes received\n# TYPE heliosdb_proxy_bytes_received_total counter\nheliosdb_proxy_bytes_received_total 45623891\n# HELP heliosdb_proxy_bytes_sent_total Total bytes sent\n# TYPE heliosdb_proxy_bytes_sent_total counter\nheliosdb_proxy_bytes_sent_total 189234567\n# HELP heliosdb_proxy_failovers_total Total failovers\n# TYPE heliosdb_proxy_failovers_total counter\nheliosdb_proxy_failovers_total 1\n"
}
```

The `text` field contains the raw Prometheus exposition format. Available metric names:

| Metric | Type | Description |
|--------|------|-------------|
| `heliosdb_proxy_connections_total` | counter | Total connections accepted. |
| `heliosdb_proxy_connections_closed` | counter | Total connections closed. |
| `heliosdb_proxy_queries_total` | counter | Total queries processed. |
| `heliosdb_proxy_bytes_received_total` | counter | Total bytes received from clients. |
| `heliosdb_proxy_bytes_sent_total` | counter | Total bytes sent to clients. |
| `heliosdb_proxy_failovers_total` | counter | Total failover events. |

---

## Sessions

### GET /sessions

Returns the number of currently active client sessions.

```bash
curl http://localhost:9090/sessions
```

**Response (200 OK):**

```json
{
  "active_sessions": 42
}
```

---

## Connection Pools

### GET /pools

Returns connection pool statistics for each backend node.

```bash
curl http://localhost:9090/pools
```

**Response (200 OK):**

```json
[
  {
    "node": "db-primary.internal:5432",
    "active_connections": 15,
    "idle_connections": 35,
    "pending_requests": 0,
    "total_connections_created": 1234,
    "total_connections_closed": 1184
  },
  {
    "node": "db-standby-1.internal:5432",
    "active_connections": 8,
    "idle_connections": 42,
    "pending_requests": 0,
    "total_connections_created": 892,
    "total_connections_closed": 842
  }
]
```

---

## Version

### GET /version

Returns proxy version and build information.

```bash
curl http://localhost:9090/version
```

**Response (200 OK):**

```json
{
  "version": "0.3.0",
  "build_time": "0.3.0"
}
```

---

## SQL Execution API

### POST /api/sql

Execute a SQL query through the proxy with transparent write routing (TWR). Write queries are routed to the primary node. Read queries are load-balanced across healthy standby and replica nodes.

This endpoint forwards the query to the backend node's HTTP SQL API and returns the result along with routing metadata.

```bash
# Read query -- routed to a standby or replica
curl -X POST http://localhost:9090/api/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}'
```

**Response (200 OK):**

```json
{
  "query_type": "read",
  "routed_to": "db-standby-1.internal:5432",
  "node_role": "standby",
  "result": {
    "columns": ["id", "name", "email"],
    "rows": [
      [1, "Alice", "alice@example.com"],
      [2, "Bob", "bob@example.com"]
    ],
    "row_count": 2
  }
}
```

```bash
# Write query -- routed to the primary
curl -X POST http://localhost:9090/api/sql \
  -H "Content-Type: application/json" \
  -d '{"query": "INSERT INTO users (name, email) VALUES ('\''Charlie'\'', '\''charlie@example.com'\'')"}'
```

**Response (200 OK):**

```json
{
  "query_type": "write",
  "routed_to": "db-primary.internal:5432",
  "node_role": "primary",
  "result": {
    "command": "INSERT",
    "rows_affected": 1
  }
}
```

**Request body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | Yes | SQL query to execute. |
| `params` | array | No | Query parameters for future prepared statement support. |

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `query_type` | string | `"read"` or `"write"`. |
| `routed_to` | string | Address of the backend node that executed the query. |
| `node_role` | string | Role of the target node: `"primary"`, `"standby"`, or `"readreplica"`. |
| `result` | object | Query result as returned by the backend node's HTTP API. |

**Query classification:**

The following statement types are classified as write operations and routed to the primary:

- `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `UPSERT`
- `CREATE`, `ALTER`, `DROP`, `TRUNCATE`
- `GRANT`, `REVOKE`
- `VACUUM`, `REINDEX`
- `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`

All other statements (primarily `SELECT`) are classified as read operations and load-balanced across healthy standby and replica nodes.

**Error responses:**

```json
{
  "error": "No healthy primary node available"
}
```

```json
{
  "error": "Empty query"
}
```

---

## Error Handling

All endpoints return errors in a consistent JSON format:

```json
{
  "error": "Description of the error"
}
```

| Status Code | Meaning |
|-------------|---------|
| `200` | Success. |
| `400` | Bad request (malformed input, empty query). |
| `404` | Resource not found (unknown node address, unknown endpoint). |
| `500` | Internal server error. |
| `503` | Service unavailable (proxy not ready, no healthy backends). |

---

## Usage Examples

### Monitor cluster health in a script

```bash
#!/bin/bash
# Check if proxy is ready
if curl -sf http://localhost:9090/health/ready > /dev/null; then
    echo "Proxy is ready"
else
    echo "Proxy is NOT ready" >&2
    exit 1
fi

# List unhealthy nodes
curl -s http://localhost:9090/nodes | \
    jq '.[] | select(.healthy == false) | .address'
```

### Drain a node for maintenance

```bash
# 1. Disable the node to stop new connections
curl -X POST http://localhost:9090/nodes/db-standby-1.internal:5432/disable

# 2. Wait for active connections to drain
while true; do
    active=$(curl -s http://localhost:9090/pools | \
        jq '.[] | select(.node == "db-standby-1.internal:5432") | .active_connections')
    [ "$active" = "0" ] && break
    echo "Waiting for $active connections to drain..."
    sleep 5
done

# 3. Perform maintenance on the backend node
# ...

# 4. Re-enable the node
curl -X POST http://localhost:9090/nodes/db-standby-1.internal:5432/enable
```

### Prometheus scrape configuration

```yaml
scrape_configs:
  - job_name: heliosproxy
    metrics_path: /metrics/prometheus
    static_configs:
      - targets:
          - "heliosproxy:9090"
```

Note: The `/metrics/prometheus` endpoint returns the metrics wrapped in a JSON `text` field. For direct Prometheus scraping, enable the `observability` feature flag, which exposes a standard Prometheus text endpoint at `/metrics`.

---

## See Also

- [Architecture](architecture.md)
- [Configuration Reference](configuration.md)
- [Deployment Guides](deployment/)
