# HeliosProxy Configuration Reference

Complete reference for all HeliosProxy configuration options. HeliosProxy uses TOML format for configuration files.

---

## Usage

```bash
# Start with a configuration file
heliosdb-proxy --config /etc/heliosproxy/config.toml

# Start with command-line arguments
heliosdb-proxy \
  --listen 0.0.0.0:6432 \
  --admin 0.0.0.0:9090 \
  --primary db-primary:5432 \
  --standby db-standby-1:5432 \
  --standby db-standby-2:5432

# Override log level
heliosdb-proxy --config config.toml --log-level debug

# Enable JSON-structured logging
heliosdb-proxy --config config.toml --json-logs
```

---

## Command-Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--config`, `-c` | *(none)* | Path to TOML configuration file |
| `--listen`, `-l` | `0.0.0.0:5432` | Client listen address |
| `--admin` | `0.0.0.0:9090` | Admin API listen address |
| `--primary` | *(none)* | Primary node `host:port` |
| `--standby` | *(none)* | Standby node `host:port` (repeatable) |
| `--tr` | `true` | Enable Transaction Replay |
| `--log-level` | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `--json-logs` | `false` | Emit logs in JSON format |

When `--config` is provided, the file takes precedence. Command-line node arguments (`--primary`, `--standby`) are used only when no configuration file is specified.

---

## Top-Level Options

```toml
listen_address = "0.0.0.0:5432"
admin_address = "0.0.0.0:9090"
tr_enabled = true
tr_mode = "session"
write_timeout_secs = 30
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `listen_address` | string | `"0.0.0.0:5432"` | Address and port for PostgreSQL client connections. |
| `admin_address` | string | `"0.0.0.0:9090"` | Address and port for the REST admin API. |
| `tr_enabled` | bool | `true` | Enable Transaction Replay. Requires the `ha-tr` feature flag at build time. |
| `tr_mode` | string | `"session"` | Transaction Replay mode. See table below. |
| `write_timeout_secs` | u64 | `30` | Maximum seconds to buffer write queries during failover before returning an error. |

### Transaction Replay Modes (`tr_mode`)

| Mode | Description |
|------|-------------|
| `none` | Transaction Replay disabled. In-flight transactions are aborted on failover. |
| `session` | Re-establish session state (SET parameters, prepared statements) on the new primary. Transactions are not replayed. |
| `select` | Restore session state and re-execute SELECT queries. Write transactions are not replayed. |
| `transaction` | Full transaction replay. All journaled statements are re-executed on the new primary. Provides the strongest failover guarantee. |

---

## Pool Mode Configuration (`[pool_mode]`)

Controls connection pooling behavior. Requires the `pool-modes` feature flag.

```toml
[pool_mode]
mode = "transaction"
max_pool_size = 100
min_idle = 10
idle_timeout_secs = 600
max_lifetime_secs = 3600
acquire_timeout_secs = 5
reset_query = "DISCARD ALL"
prepared_statement_mode = "track"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | string | `"session"` | Pooling mode: `session`, `transaction`, or `statement`. |
| `max_pool_size` | u32 | `100` | Maximum backend connections per node. |
| `min_idle` | u32 | `10` | Minimum idle connections to maintain. |
| `idle_timeout_secs` | u64 | `600` | Close idle connections after this many seconds. |
| `max_lifetime_secs` | u64 | `3600` | Close connections after this many seconds regardless of activity. |
| `acquire_timeout_secs` | u64 | `5` | Maximum seconds to wait when acquiring a connection from the pool. |
| `reset_query` | string | `"DISCARD ALL"` | SQL executed when a connection is returned to the pool. |
| `prepared_statement_mode` | string | `"disable"` | Prepared statement handling: `disable`, `track`, or `named`. |

### Pooling Modes

| Mode | Connection Returns To Pool | Best For |
|------|---------------------------|----------|
| `session` | When the client disconnects. 1:1 client-to-backend mapping. | Legacy applications, applications using prepared statements, long-running sessions. |
| `transaction` | After `COMMIT` or `ROLLBACK`. Connections are shared across clients between transactions. | Web applications, microservices, connection-starved environments. |
| `statement` | After each individual statement. Maximum connection sharing. | Simple query workloads, read-heavy applications with no multi-statement transactions. |

### Prepared Statement Modes

| Mode | Behavior |
|------|----------|
| `disable` | Prepared statements are not tracked. Safest for transaction and statement pooling modes. |
| `track` | The proxy tracks PREPARE/DEALLOCATE and recreates prepared statements when a client is assigned a new backend connection. |
| `named` | Uses PostgreSQL protocol-level named statements. Compatible with session pooling mode. |

---

## Connection Pool Configuration (`[pool]`)

Basic connection pool settings. These apply to the core connection pool regardless of pooling mode.

```toml
[pool]
min_connections = 2
max_connections = 100
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 30
test_on_acquire = true
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `min_connections` | usize | `2` | Minimum connections maintained per backend node. |
| `max_connections` | usize | `100` | Maximum connections per backend node. |
| `idle_timeout_secs` | u64 | `300` | Close connections idle longer than this value. |
| `max_lifetime_secs` | u64 | `1800` | Maximum lifetime of a connection before it is closed and replaced. |
| `acquire_timeout_secs` | u64 | `30` | Maximum time to wait for a connection from the pool. |
| `test_on_acquire` | bool | `true` | Execute a health check query before handing a connection to a client. |

---

## Load Balancer Configuration (`[load_balancer]`)

Controls how queries are distributed across backend nodes.

```toml
[load_balancer]
read_strategy = "round_robin"
read_write_split = true
latency_threshold_ms = 100
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `read_strategy` | string | `"round_robin"` | Strategy for distributing read queries. See table below. |
| `read_write_split` | bool | `true` | When enabled, write queries route to the primary and read queries route to standby/replica nodes. |
| `latency_threshold_ms` | u64 | `100` | Nodes with latency above this threshold are considered unhealthy for routing purposes. |

### Routing Strategies

| Strategy | Description |
|----------|-------------|
| `round_robin` | Rotate through available nodes in order, distributing queries equally. |
| `weighted_round_robin` | Rotate through nodes proportionally to their configured `weight` values. |
| `least_connections` | Route each query to the node with the fewest active connections. |
| `latency_based` | Route each query to the node with the lowest observed latency. |
| `random` | Select a node at random for each query. |

---

## Health Check Configuration (`[health]`)

Backend node health monitoring settings.

```toml
[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `check_interval_secs` | u64 | `5` | Interval between health check probes. |
| `check_timeout_secs` | u64 | `3` | Maximum time to wait for a health check response. |
| `failure_threshold` | u32 | `3` | Number of consecutive failures before marking a node unhealthy. |
| `success_threshold` | u32 | `2` | Number of consecutive successes before marking an unhealthy node as healthy again. |
| `check_query` | string | `"SELECT 1"` | SQL query used for health checks. |

---

## Node Configuration (`[[nodes]]`)

Define one or more backend nodes. Each `[[nodes]]` entry is a separate backend.

```toml
[[nodes]]
host = "db-primary.internal"
port = 5432
http_port = 8080
role = "primary"
weight = 100
enabled = true
name = "primary-1"

[[nodes]]
host = "db-standby-1.internal"
port = 5432
http_port = 8080
role = "standby"
weight = 100
enabled = true
name = "standby-1"

[[nodes]]
host = "db-replica-1.internal"
port = 5432
http_port = 8080
role = "replica"
weight = 50
enabled = true
name = "replica-1"
```

| Key | Type | Default | Required | Description |
|-----|------|---------|----------|-------------|
| `host` | string | -- | Yes | Backend hostname or IP address. |
| `port` | u16 | -- | Yes | PostgreSQL protocol port. |
| `http_port` | u16 | `8080` | No | HTTP API port on the backend node. Used for SQL API forwarding (TWR). |
| `role` | string | -- | Yes | Node role: `primary`, `standby`, or `replica`. |
| `weight` | u32 | `100` | No | Load balancing weight. Higher values receive proportionally more traffic. |
| `enabled` | bool | `true` | No | Whether this node is available for routing. Can be toggled at runtime via the Admin API. |
| `name` | string | *(none)* | No | Human-readable name used in logs, metrics, and Admin API responses. |

### Node Roles

| Role | Description |
|------|-------------|
| `primary` | The read/write node. All write queries and transaction-control statements are routed here. At least one primary is required. |
| `standby` | A synchronous or asynchronous standby. Eligible for promotion during failover. Receives read traffic when `read_write_split` is enabled. |
| `replica` | A read-only replica. Not eligible for promotion. Receives read traffic only. |

---

## TLS Configuration (`[tls]`)

Optional TLS termination for client connections to the proxy.

```toml
[tls]
enabled = true
cert_path = "/etc/heliosproxy/server.crt"
key_path = "/etc/heliosproxy/server.key"
ca_path = "/etc/heliosproxy/ca.crt"
require_client_cert = false
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable TLS for client-facing connections. |
| `cert_path` | string | *(required if enabled)* | Path to the PEM-encoded server certificate. |
| `key_path` | string | *(required if enabled)* | Path to the PEM-encoded private key. |
| `ca_path` | string | *(optional)* | Path to the CA certificate for client certificate verification. |
| `require_client_cert` | bool | `false` | Require clients to present a valid certificate signed by the CA. |

---

## Query Cache Configuration (`[cache]`)

Requires the `query-cache` feature flag.

```toml
[cache]
enabled = true
max_memory_mb = 512
default_ttl_secs = 60
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable the query result cache. |
| `max_memory_mb` | u64 | `512` | Maximum memory for cached query results. |
| `default_ttl_secs` | u64 | `60` | Default time-to-live for cached entries. |

---

## Lag-Aware Routing Configuration (`[routing]`)

Requires the `lag-routing` feature flag.

```toml
[routing]
max_replica_lag_ms = 100
lag_check_interval_secs = 1
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_replica_lag_ms` | u64 | `100` | Maximum acceptable replication lag in milliseconds. Replicas exceeding this are excluded from read routing. |
| `lag_check_interval_secs` | u64 | `1` | How often to poll replica lag metrics. |

---

## Rate Limiting Configuration (`[rate_limit]`)

Requires the `rate-limiting` feature flag.

```toml
[rate_limit]
queries_per_second = 1000
burst_size = 100
per_user = true
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `queries_per_second` | u64 | `1000` | Maximum sustained query rate. |
| `burst_size` | u64 | `100` | Maximum burst size above the sustained rate. |
| `per_user` | bool | `true` | Apply limits per connecting user rather than globally. |

---

## Circuit Breaker Configuration (`[circuit_breaker]`)

Requires the `circuit-breaker` feature flag.

```toml
[circuit_breaker]
failure_threshold = 5
recovery_timeout_secs = 30
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `failure_threshold` | u32 | `5` | Number of consecutive failures before opening the circuit. |
| `recovery_timeout_secs` | u64 | `30` | Time to wait before attempting a recovery probe (half-open state). |

---

## Query Analytics Configuration (`[analytics]`)

Requires the `query-analytics` feature flag.

```toml
[analytics]
enabled = true
slow_query_threshold_ms = 100
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable query analytics and fingerprinting. |
| `slow_query_threshold_ms` | u64 | `100` | Queries exceeding this duration are logged to the slow query log. |

---

## Multi-Tenancy Configuration (`[[tenants]]`)

Requires the `multi-tenancy` feature flag.

```toml
[[tenants]]
id = "tenant_a"
max_connections = 50
rate_limit = 500
databases = ["tenant_a_db"]

[[tenants]]
id = "tenant_b"
max_connections = 25
rate_limit = 200
databases = ["tenant_b_db"]
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `id` | string | *(required)* | Unique tenant identifier. |
| `max_connections` | u32 | *(required)* | Maximum backend connections for this tenant. |
| `rate_limit` | u64 | *(required)* | Maximum queries per second for this tenant. |
| `databases` | array | *(required)* | List of database names this tenant is authorized to access. |

---

## WASM Plugin Configuration (`[[plugins]]`)

Requires the `wasm-plugins` feature flag.

```toml
[[plugins]]
name = "audit_logger"
path = "/plugins/audit.wasm"
config = { log_level = "info" }
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `name` | string | *(required)* | Plugin name for logging and metrics. |
| `path` | string | *(required)* | Path to the compiled `.wasm` module. |
| `config` | table | `{}` | Plugin-specific configuration passed to the WASM module at initialization. |

---

## GraphQL Gateway Configuration (`[graphql]`)

Requires the `graphql-gateway` feature flag.

```toml
[graphql]
enabled = true
endpoint = "/graphql"
introspection = true
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable the GraphQL endpoint on the admin port. |
| `endpoint` | string | `"/graphql"` | URL path for the GraphQL endpoint. |
| `introspection` | bool | `true` | Allow GraphQL introspection queries. |

---

## Schema Routing Configuration (`[[schema_routes]]`)

Requires the `schema-routing` feature flag.

```toml
[[schema_routes]]
tables = ["events", "logs"]
target = "analytics_replica"

[[schema_routes]]
tables = ["orders", "payments"]
target = "primary"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `tables` | array | *(required)* | List of table names to match. |
| `target` | string | *(required)* | Name of the target node (matches the `name` field in `[[nodes]]`). |

---

## Distributed Cache Configuration (`[distribcache]`)

Requires the `distribcache` feature flag.

```toml
[distribcache]
enabled = true
l1_size_mb = 64
l2_size_mb = 512
l3_nodes = ["cache1:6379", "cache2:6379"]

[distribcache.ai]
embedding_prefetch = true
conversation_ttl_secs = 3600
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable distributed caching. |
| `l1_size_mb` | u64 | `64` | Size of the L1 (in-process) hot cache. |
| `l2_size_mb` | u64 | `512` | Size of the L2 (shared memory) warm cache. |
| `l3_nodes` | array | `[]` | Addresses of external Redis-compatible nodes for L3 distributed cache. |
| `distribcache.ai.embedding_prefetch` | bool | `false` | Enable embedding prefetch for RAG pipelines. |
| `distribcache.ai.conversation_ttl_secs` | u64 | `3600` | TTL for cached conversation context in agentic workloads. |

---

## Environment Variable Overrides

Any configuration value can be overridden with an environment variable. The naming convention is `HELIOS_PROXY_` followed by the uppercase, underscore-separated key path.

```bash
HELIOS_PROXY_LISTEN_ADDRESS=0.0.0.0:6432
HELIOS_PROXY_ADMIN_ADDRESS=0.0.0.0:9090
HELIOS_PROXY_POOL_MODE=transaction
HELIOS_PROXY_MAX_POOL_SIZE=200
HELIOS_PROXY_WRITE_TIMEOUT_SECS=60
```

Environment variables take highest precedence, followed by the configuration file, followed by built-in defaults.

---

## Configuration Validation

The proxy validates the configuration at startup and refuses to start if any rule is violated:

1. At least one node must be configured.
2. Exactly one node with role `primary` must be present and enabled.
3. `max_connections` must be greater than or equal to `min_connections`.
4. All required fields must be present and have valid values.
5. Listen address and admin address must be valid socket addresses.

Invalid configurations produce a descriptive error message and a non-zero exit code.

---

## Complete Example

See `config/` in the project root for ready-to-use configuration examples.

```toml
# HeliosProxy - Full Configuration Example
# See docs/configuration.md for a description of every key.

listen_address = "0.0.0.0:6432"
admin_address = "0.0.0.0:9090"
tr_enabled = true
tr_mode = "session"
write_timeout_secs = 30

[pool_mode]
mode = "transaction"
max_pool_size = 100
min_idle = 10
idle_timeout_secs = 600
max_lifetime_secs = 3600
acquire_timeout_secs = 5
reset_query = "DISCARD ALL"
prepared_statement_mode = "track"

[pool]
min_connections = 5
max_connections = 100
idle_timeout_secs = 300
max_lifetime_secs = 1800
acquire_timeout_secs = 30
test_on_acquire = true

[load_balancer]
read_strategy = "least_connections"
read_write_split = true
latency_threshold_ms = 50

[health]
check_interval_secs = 5
check_timeout_secs = 3
failure_threshold = 3
success_threshold = 2
check_query = "SELECT 1"

[[nodes]]
host = "db-primary.internal"
port = 5432
http_port = 8080
role = "primary"
weight = 100
enabled = true
name = "primary"

[[nodes]]
host = "db-standby-1.internal"
port = 5432
http_port = 8080
role = "standby"
weight = 100
enabled = true
name = "standby-1"

[[nodes]]
host = "db-replica-1.internal"
port = 5432
http_port = 8080
role = "replica"
weight = 50
enabled = true
name = "replica-1"

[tls]
enabled = false
# cert_path = "/etc/heliosproxy/server.crt"
# key_path = "/etc/heliosproxy/server.key"
```

---

## See Also

- [Architecture](architecture.md)
- [Feature Flags](feature-flags.md)
- [Admin API Reference](admin-api.md)
