# Docker Deployment

This guide covers building and running HeliosProxy in Docker, including Docker Compose configurations for multi-node database clusters.

---

## Docker Image

### Building the Image

Create a multi-stage Dockerfile for a minimal production image:

```dockerfile
# Stage 1: Build
FROM rust:1.82-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

# Build with desired features
ARG FEATURES="all-features,postgres-topology"
RUN cargo build --release --features "${FEATURES}"

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home heliosproxy

COPY --from=builder /app/target/release/heliosdb-proxy /usr/local/bin/heliosdb-proxy

RUN mkdir -p /etc/heliosproxy && chown heliosproxy:heliosproxy /etc/heliosproxy

USER heliosproxy

EXPOSE 6432 9090

HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

ENTRYPOINT ["heliosdb-proxy"]
CMD ["--config", "/etc/heliosproxy/config.toml"]
```

Build the image:

```bash
docker build -t heliosdb/proxy:latest .

# Build with specific features
docker build --build-arg FEATURES="pool-modes,ha-tr,postgres-topology" \
    -t heliosdb/proxy:ha .
```

### Image Variants

| Tag | Features | Size |
|-----|----------|------|
| `latest` | All features + PostgreSQL topology | ~35 MB |
| `ha` | Pool modes + HA-TR + PostgreSQL topology | ~25 MB |
| `minimal` | Pool modes only | ~20 MB |

---

## Running with Docker

### Quick Start

```bash
# Run with a configuration file
docker run -d \
    --name heliosproxy \
    -p 6432:6432 \
    -p 9090:9090 \
    -v $(pwd)/config.toml:/etc/heliosproxy/config.toml:ro \
    heliosdb/proxy:latest

# Run with command-line arguments
docker run -d \
    --name heliosproxy \
    -p 6432:6432 \
    -p 9090:9090 \
    heliosdb/proxy:latest \
    --listen 0.0.0.0:6432 \
    --admin 0.0.0.0:9090 \
    --primary db-primary:5432 \
    --standby db-standby-1:5432
```

### Environment Variables

Configuration values can be overridden with environment variables:

```bash
docker run -d \
    --name heliosproxy \
    -p 6432:6432 \
    -p 9090:9090 \
    -e HELIOS_PROXY_LISTEN_ADDRESS=0.0.0.0:6432 \
    -e HELIOS_PROXY_ADMIN_ADDRESS=0.0.0.0:9090 \
    -e HELIOS_PROXY_POOL_MODE=transaction \
    -e HELIOS_PROXY_MAX_POOL_SIZE=200 \
    -e RUST_LOG=heliosdb_proxy=info \
    -v $(pwd)/config.toml:/etc/heliosproxy/config.toml:ro \
    heliosdb/proxy:latest
```

### Health Checks

```bash
# Liveness
docker exec heliosproxy curl -s http://localhost:9090/health

# Readiness
docker exec heliosproxy curl -s http://localhost:9090/health/ready

# Node status
docker exec heliosproxy curl -s http://localhost:9090/nodes
```

---

## Docker Compose: Single Proxy with PostgreSQL

A basic setup with one proxy and a PostgreSQL primary/standby pair.

```yaml
version: "3.8"

services:
  heliosproxy:
    image: heliosdb/proxy:latest
    ports:
      - "6432:6432"
      - "9090:9090"
    volumes:
      - ./config/proxy.toml:/etc/heliosproxy/config.toml:ro
    depends_on:
      pg-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9090/health"]
      interval: 5s
      timeout: 3s
      retries: 3
    restart: unless-stopped
    networks:
      - db-network

  pg-primary:
    image: postgres:16
    environment:
      POSTGRES_USER: helios
      POSTGRES_PASSWORD: helios
      POSTGRES_DB: appdb
    ports:
      - "5432:5432"
    volumes:
      - pg-primary-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U helios"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - db-network

volumes:
  pg-primary-data:

networks:
  db-network:
    driver: bridge
```

Proxy configuration (`config/proxy.toml`):

```toml
listen_address = "0.0.0.0:6432"
admin_address = "0.0.0.0:9090"
tr_enabled = false
write_timeout_secs = 30

[pool_mode]
mode = "transaction"
max_pool_size = 100
min_idle = 5

[pool]
min_connections = 2
max_connections = 50

[load_balancer]
read_write_split = false
read_strategy = "round_robin"

[health]
check_interval_secs = 5
failure_threshold = 3

[[nodes]]
host = "pg-primary"
port = 5432
role = "primary"
name = "primary"
```

---

## Docker Compose: 3-Node HA Cluster

A production-grade setup with a primary, two standbys, and a proxy.

```yaml
version: "3.8"

services:
  # ── HeliosProxy ─────────────────────────────────────────────
  heliosproxy:
    image: heliosdb/proxy:latest
    ports:
      - "6432:6432"
      - "9090:9090"
    volumes:
      - ./config/ha-proxy.toml:/etc/heliosproxy/config.toml:ro
    depends_on:
      db-primary:
        condition: service_healthy
      db-standby-1:
        condition: service_healthy
      db-standby-2:
        condition: service_healthy
    environment:
      RUST_LOG: heliosdb_proxy=info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9090/health/ready"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped
    networks:
      - cluster-network

  # ── Primary Node ────────────────────────────────────────────
  db-primary:
    image: heliosdb/heliosdb-lite:latest
    hostname: db-primary
    environment:
      HELIOSDB_ROLE: primary
      HELIOSDB_PORT: "5432"
      HELIOSDB_REPL_PORT: "5433"
      HELIOSDB_HTTP_PORT: "8080"
      HELIOSDB_SYNC_MODE: sync
    ports:
      - "15432:5432"
      - "18080:8080"
    volumes:
      - primary-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - cluster-network

  # ── Standby Node 1 (Sync) ──────────────────────────────────
  db-standby-1:
    image: heliosdb/heliosdb-lite:latest
    hostname: db-standby-1
    environment:
      HELIOSDB_ROLE: standby
      HELIOSDB_PORT: "5432"
      HELIOSDB_REPL_PORT: "5433"
      HELIOSDB_HTTP_PORT: "8080"
      HELIOSDB_PRIMARY_HOST: "db-primary:5433"
      HELIOSDB_SYNC_MODE: sync
    ports:
      - "15442:5432"
      - "18081:8080"
    volumes:
      - standby1-data:/data
    depends_on:
      db-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - cluster-network

  # ── Standby Node 2 (Async) ─────────────────────────────────
  db-standby-2:
    image: heliosdb/heliosdb-lite:latest
    hostname: db-standby-2
    environment:
      HELIOSDB_ROLE: standby
      HELIOSDB_PORT: "5432"
      HELIOSDB_REPL_PORT: "5433"
      HELIOSDB_HTTP_PORT: "8080"
      HELIOSDB_PRIMARY_HOST: "db-primary:5433"
      HELIOSDB_SYNC_MODE: async
    ports:
      - "15452:5432"
      - "18082:8080"
    volumes:
      - standby2-data:/data
    depends_on:
      db-primary:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - cluster-network

volumes:
  primary-data:
  standby1-data:
  standby2-data:

networks:
  cluster-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16
```

HA proxy configuration (`config/ha-proxy.toml`):

```toml
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

# Primary
[[nodes]]
host = "db-primary"
port = 5432
http_port = 8080
role = "primary"
weight = 100
enabled = true
name = "primary"

# Sync standby (failover candidate)
[[nodes]]
host = "db-standby-1"
port = 5432
http_port = 8080
role = "standby"
weight = 100
enabled = true
name = "standby-sync"

# Async standby (read replica)
[[nodes]]
host = "db-standby-2"
port = 5432
http_port = 8080
role = "standby"
weight = 50
enabled = true
name = "standby-async"
```

### Starting the Cluster

```bash
# Start all services
docker compose up -d

# View logs
docker compose logs -f heliosproxy

# Verify cluster health
curl http://localhost:9090/nodes | jq .

# Connect through the proxy
psql -h localhost -p 6432 -U helios -d appdb
```

### Testing Failover

```bash
# 1. Verify current primary
curl -s http://localhost:9090/nodes | jq '.[] | select(.address | startswith("db-primary"))'

# 2. Stop the primary
docker compose stop db-primary

# 3. Watch failover in proxy logs
docker compose logs -f heliosproxy

# 4. Verify new primary detected
curl -s http://localhost:9090/nodes | jq .

# 5. Restart the old primary
docker compose start db-primary
```

---

## Docker Compose: PostgreSQL with Proxy

For standard PostgreSQL deployments (not HeliosDB), adjust the node configuration:

```yaml
version: "3.8"

services:
  heliosproxy:
    image: heliosdb/proxy:latest
    ports:
      - "6432:6432"
      - "9090:9090"
    volumes:
      - ./config/pg-proxy.toml:/etc/heliosproxy/config.toml:ro
    depends_on:
      - pg-primary
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9090/health"]
      interval: 5s
      timeout: 3s
      retries: 3
    networks:
      - pg-network

  pg-primary:
    image: postgres:16
    environment:
      POSTGRES_USER: appuser
      POSTGRES_PASSWORD: apppass
      POSTGRES_DB: appdb
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U appuser"]
      interval: 5s
      timeout: 3s
      retries: 5
    networks:
      - pg-network

networks:
  pg-network:
```

---

## Resource Limits

Set resource constraints for production deployments:

```yaml
services:
  heliosproxy:
    image: heliosdb/proxy:latest
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 512M
        reservations:
          cpus: "0.5"
          memory: 128M
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
```

---

## Logging

### View Proxy Logs

```bash
# Follow logs
docker compose logs -f heliosproxy

# View last 200 lines
docker compose logs --tail 200 heliosproxy

# Filter by log level (requires RUST_LOG=debug)
docker compose logs heliosproxy 2>&1 | grep -i error
```

### Enable Debug Logging

```yaml
services:
  heliosproxy:
    environment:
      RUST_LOG: heliosdb_proxy=debug
```

### Enable JSON Structured Logging

```yaml
services:
  heliosproxy:
    command: ["--config", "/etc/heliosproxy/config.toml", "--json-logs"]
```

---

## See Also

- [Standalone Deployment](standalone.md)
- [Kubernetes Deployment](kubernetes.md)
- [Configuration Reference](../configuration.md)
