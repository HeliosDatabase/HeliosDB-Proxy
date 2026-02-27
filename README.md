# HeliosProxy

Intelligent PostgreSQL connection router, load balancer, and failover manager.

## Features

- **Connection Pooling**: Session, Transaction, and Statement pooling modes
- **Load Balancing**: Round-robin, least-connections, and latency-based routing with read/write splitting
- **Health Monitoring**: Continuous node health checks with configurable thresholds
- **Transaction Replay (TR)**: Transparent transaction replay after node failure
- **Admin API**: REST API for monitoring, configuration, and node management
- **PostgreSQL Wire Protocol**: Full protocol forwarding with transparent routing

## Quick Start

```bash
# Start with a config file
heliosdb-proxy --config proxy.toml

# Or with command line options
heliosdb-proxy \
  --listen 0.0.0.0:5432 \
  --primary db-primary:5432 \
  --standby db-standby-1:5432 \
  --standby db-standby-2:5432
```

## Configuration

```toml
[proxy]
listen_address = "0.0.0.0:5432"
admin_address = "0.0.0.0:9090"

[pool]
min_connections = 5
max_connections = 100
idle_timeout_secs = 300

[load_balancer]
strategy = "round_robin"  # or "least_connections", "latency_based"
read_write_split = true

[health]
check_interval_secs = 5
failure_threshold = 3

[[nodes]]
host = "db-primary"
port = 5432
role = "primary"

[[nodes]]
host = "db-standby-1"
port = 5432
role = "standby"
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tr` | Yes | Transaction Replay for failover resilience |
| `pool-modes` | Yes | Session/Transaction/Statement connection pooling |
| `observability` | No | Prometheus metrics + OpenTelemetry tracing |

## Building

```bash
# Default build (TR + pool-modes)
cargo build --release

# Minimal build
cargo build --release --no-default-features

# With observability
cargo build --release --features observability
```

## Deployment

### Standalone Binary

```bash
cargo install heliosdb-proxy
heliosdb-proxy --config /etc/heliosdb/proxy.toml
```

### Docker

```dockerfile
FROM rust:1.82 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /app/target/release/heliosdb-proxy /usr/local/bin/
ENTRYPOINT ["heliosdb-proxy"]
```

### Kubernetes Sidecar

Deploy as a sidecar container alongside your application pod for minimal latency.

## Admin API

The admin API runs on port 9090 by default:

```bash
# Health check
curl http://localhost:9090/health

# Node status
curl http://localhost:9090/nodes

# Pool metrics
curl http://localhost:9090/metrics
```

## License

Apache-2.0
