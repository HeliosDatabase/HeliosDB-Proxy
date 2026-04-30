# HeliosProxy

**High-performance connection router, intelligent query processor, and failover manager for PostgreSQL-wire-compatible databases.**

HeliosProxy operates at the PostgreSQL wire protocol level, making it compatible with any database that speaks the PostgreSQL protocol — including PostgreSQL, HeliosDB, CockroachDB, YugabyteDB, and others.

---

## Overview

HeliosProxy sits between your application and your database cluster, providing transparent connection pooling, automatic failover, intelligent query routing, and 24 enterprise-grade feature modules — all without application code changes.

```
┌──────────────┐     ┌─────────────────────────────────────────────────┐     ┌──────────────┐
│              │     │                  HeliosProxy                    │     │   Primary    │
│  Application ├────►│                                                 ├────►│   (R/W)      │
│              │     │  ┌───────────┐ ┌──────────┐ ┌───────────────┐  │     └──────────────┘
│  (psql, any  │     │  │Connection │ │  Query   │ │   Failover    │  │     ┌──────────────┐
│   PG driver) │     │  │  Pooling  │ │ Routing  │ │  Controller   │  ├────►│  Standby     │
│              │     │  └───────────┘ └──────────┘ └───────────────┘  │     │  (Sync)      │
└──────────────┘     │  ┌───────────┐ ┌──────────┐ ┌───────────────┐  │     └──────────────┘
                     │  │  Caching  │ │   Auth   │ │  Rate Limit   │  │     ┌──────────────┐
                     │  │  (L1/L2)  │ │  Proxy   │ │  & Circuit    │  ├────►│  Standby     │
                     │  └───────────┘ └──────────┘ └───────────────┘  │     │  (Async)     │
                     └─────────────────────────────────────────────────┘     └──────────────┘
```

---

## Key Capabilities

### Protocol-Level Compatibility

All 24 feature modules operate at the PostgreSQL wire protocol level. They inspect, route, cache, and transform queries without requiring changes to your application or database. Any client library that connects to PostgreSQL connects to HeliosProxy.

### Supported Backends

| Backend | Status | Notes |
|---------|--------|-------|
| **PostgreSQL** 12+ | Fully supported | Including Aurora, RDS, Cloud SQL |
| **HeliosDB-Lite** | Fully supported | Native topology integration available |
| **HeliosDB-Full** | Fully supported | Cluster-aware routing |
| **CockroachDB** | Compatible | PostgreSQL wire protocol |
| **YugabyteDB** | Compatible | PostgreSQL wire protocol |
| **TimescaleDB** | Compatible | PostgreSQL extension |
| **Citus** | Compatible | PostgreSQL extension |

---

## Feature Modules

HeliosProxy ships 24 feature modules, each independently enabled via Cargo feature flags. Enable only what you need — from a lightweight connection pooler to a full-featured intelligent proxy.

### Connection Management

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Connection Pooling** | `pool-modes` | Session, Transaction, and Statement pooling with automatic lease management, prepared statement forwarding, and configurable reset sequences |
| **Load Balancer** | *(core)* | Round-robin, least-connections, and latency-based routing with automatic read/write splitting across primary and standby nodes |
| **Health Checker** | *(core)* | Continuous node health monitoring with configurable check intervals, failure thresholds, and custom health-check queries |
| **Pipeline** | *(core)* | PostgreSQL extended query protocol pipelining — batches Parse/Bind/Execute messages for reduced round trips |
| **Batch Operations** | *(core)* | Automatic INSERT coalescing — groups individual INSERT statements into multi-row batches for higher throughput |

### High Availability

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Failover Controller** | *(core)* | Automatic failover with sync-standby preference, candidate ranking by replication lag, and configurable promotion policies |
| **Transaction Replay (TR)** | `ha-tr` | Journals in-flight transactions and transparently replays them on a new primary after failover — zero data loss for committed work |
| **Failover Replay** | `ha-tr` | Coordinates the replay of journaled transactions during failover, maintaining statement ordering and parameter fidelity |
| **Session Migration** | `ha-tr` | Captures and restores full session state (SET parameters, prepared statements, advisory locks) when moving connections between nodes |
| **Cursor Restore** | `ha-tr` | Preserves open cursor positions across failover — clients resume fetching without re-executing the query |
| **Switchover Buffer** | *(core)* | Buffers incoming queries during planned switchover, drains them to the new primary once promotion completes |
| **Primary Tracker** | *(core)* | Pluggable topology discovery — tracks the current primary via `pg_is_in_recovery()` polling (PostgreSQL), HeliosDB topology events, or manual API calls |
| **Transaction Journal** | `ha-tr` | Write-ahead journal for in-flight transactions with statement-level granularity, parameter capture, and configurable retention |

### Query Intelligence

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Query Cache** | `query-cache` | Three-tier result cache (L1 hot / L2 warm / L3 semantic) with TTL-based expiration, pattern-based invalidation, and normalized query fingerprinting |
| **Query Routing** | `routing-hints` | Hint-based routing via SQL comments (`/*+ route=primary */`) and automatic classification of read vs. write queries |
| **Lag-Aware Routing** | `lag-routing` | Routes reads to replicas within an acceptable replication lag threshold, with read-your-writes (RYW) session consistency guarantees |
| **Query Rewriter** | `query-rewriting` | Rule-based SQL transformation engine — rewrite, redirect, or annotate queries before they reach the backend |
| **Query Analytics** | `query-analytics` | Real-time query fingerprinting, execution statistics, slow query log, N+1 detection, and intent classification |
| **Schema Routing** | `schema-routing` | Routes queries based on table schema metadata, data temperature classification, and workload type (OLTP vs. analytics vs. vector) |

### Security & Access Control

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Auth Proxy** | `auth-proxy` | Authentication gateway supporting JWT, OAuth 2.0, API keys, LDAP, and certificate-based authentication with pluggable role mapping |
| **Rate Limiter** | `rate-limiting` | Token bucket and sliding window rate limiting with per-user, per-tenant, and per-IP policies, query cost estimation, and concurrency guards |
| **Circuit Breaker** | `circuit-breaker` | Adaptive circuit breaker with sliding-window failure detection, automatic recovery probes, and per-node breaker state |

### Multi-Tenancy & Extensibility

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Multi-Tenancy** | `multi-tenancy` | Tenant-aware connection routing with per-tenant pools, schema isolation, resource quotas, and automatic tenant identification from connection metadata |
| **WASM Plugins** | `wasm-plugins` | Sandboxed WebAssembly plugin runtime with hot-reload, host function bindings, and per-plugin resource limits |
| **GraphQL Gateway** | `graphql-gateway` | GraphQL-to-SQL translation layer with automatic schema introspection, DataLoader batching, and query validation |

### Distributed Caching

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **DistribCache** | `distribcache` | AI-powered distributed query cache with three-tier storage (hot/warm/distributed), workload classification, access heatmaps, and intelligent prefetching |

---

## Quick Start

### Connect to a PostgreSQL Cluster

```bash
heliosdb-proxy --config proxy.toml
```

```toml
[proxy]
listen_address = "0.0.0.0:6432"
admin_address = "0.0.0.0:9090"

[pool]
mode = "transaction"
min_connections = 5
max_connections = 100
idle_timeout_secs = 300

[load_balancer]
strategy = "least_connections"
read_write_split = true

[health]
check_interval_secs = 5
check_query = "SELECT 1"
failure_threshold = 3

[[nodes]]
host = "pg-primary.internal"
port = 5432
role = "primary"

[[nodes]]
host = "pg-standby-sync.internal"
port = 5432
role = "standby"
sync = true

[[nodes]]
host = "pg-standby-async.internal"
port = 5432
role = "standby"
```

### Connect Your Application

```bash
# HeliosProxy is transparent — use any PostgreSQL client
psql -h localhost -p 6432 -U myapp -d mydb

# Or set DATABASE_URL
export DATABASE_URL="postgres://myapp:password@localhost:6432/mydb"
```

---

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `pool-modes` | Yes | Session, Transaction, and Statement connection pooling |
| `ha-tr` | No | Transaction Replay — failover replay, cursor restore, session migration |
| `query-cache` | No | L1/L2/L3 multi-tier query result caching |
| `routing-hints` | No | SQL comment-based query routing hints |
| `lag-routing` | No | Replica lag-aware routing with read-your-writes |
| `rate-limiting` | No | Token bucket and sliding window rate limiting |
| `circuit-breaker` | No | Adaptive circuit breaker pattern |
| `query-analytics` | No | Query fingerprinting, slow query log, intent classification |
| `multi-tenancy` | No | Tenant-aware routing, isolation, and resource quotas |
| `auth-proxy` | No | JWT, OAuth 2.0, API key, and LDAP authentication |
| `query-rewriting` | No | Rule-based SQL query transformation |
| `wasm-plugins` | No | Sandboxed WASM plugin runtime with hot-reload |
| `graphql-gateway` | No | GraphQL-to-SQL translation with introspection |
| `schema-routing` | No | Schema-aware routing and workload classification |
| `distribcache` | No | AI-powered distributed query caching |
| `postgres-topology` | No | PostgreSQL primary discovery via `pg_is_in_recovery()` |
| `heliosdb-topology` | No | HeliosDB native topology integration |
| `observability` | No | Prometheus metrics and OpenTelemetry tracing |
| `all-features` | No | Enables all proxy features (choose a topology provider separately) |

### Build Examples

```bash
# Default — connection pooling only
cargo build --release

# Production — all features with PostgreSQL topology
cargo build --release --features "all-features,postgres-topology"

# Lightweight — pooling + failover only
cargo build --release --features "pool-modes,ha-tr"

# With HeliosDB integration
cargo build --release --features "all-features,heliosdb-topology"
```

---

## Topology Providers

HeliosProxy supports pluggable topology discovery for automatic primary tracking:

### PostgreSQL (`postgres-topology`)

Polls `pg_is_in_recovery()` on each configured node to detect the current primary. Compatible with any PostgreSQL HA solution:

- **Native streaming replication** — standard PostgreSQL primary/standby
- **Patroni** — automatic failover with etcd/ZooKeeper
- **pg_auto_failover** — Citus-managed automatic failover
- **Stolon** — cloud-native PostgreSQL HA
- **AWS RDS / Aurora** — managed PostgreSQL

### HeliosDB (`heliosdb-topology`)

Integrates with the HeliosDB internal topology manager for event-driven primary tracking. Zero-polling, instant failover detection.

### Manual / API

Set and update the primary programmatically via the Admin API — suitable for custom orchestration or external failover managers.

---

## Admin API

```bash
# Cluster health
curl http://localhost:9090/health

# Node status and replication lag
curl http://localhost:9090/nodes

# Connection pool metrics
curl http://localhost:9090/metrics

# Trigger manual failover
curl -X POST http://localhost:9090/failover

# Drain node for maintenance
curl -X POST http://localhost:9090/nodes/{id}/drain
```

---

## Deployment

### Standalone Binary

```bash
cargo install heliosdb-proxy
heliosdb-proxy --config /etc/heliosproxy/proxy.toml
```

### Docker

```dockerfile
FROM rust:1.82 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --features "all-features,postgres-topology"

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/heliosdb-proxy /usr/local/bin/
ENTRYPOINT ["heliosdb-proxy"]
```

```bash
docker run -v /etc/heliosproxy:/config heliosdb-proxy --config /config/proxy.toml
```

### Kubernetes Sidecar

Deploy as a sidecar container for minimal latency between your application and the proxy:

```yaml
containers:
  - name: app
    image: myapp:latest
    env:
      - name: DATABASE_URL
        value: "postgres://user:pass@localhost:6432/mydb"
  - name: heliosproxy
    image: heliosdb/heliosproxy:latest
    args: ["--config", "/config/proxy.toml"]
    ports:
      - containerPort: 6432
      - containerPort: 9090
```

---

## Benchmarks

HeliosProxy adds minimal overhead while providing significant throughput improvements through connection pooling, query caching, and pipelining:

| Metric | Direct Connection | HeliosProxy | Improvement |
|--------|-------------------|-------------|-------------|
| Connection setup | 12ms | 0.3ms | 40x faster |
| Pooled throughput | 8,200 q/s | 42,000 q/s | 5.1x higher |
| Cached query | — | 0.05ms | Sub-millisecond |
| Failover time | Manual | 1.2s automatic | Zero downtime |

---

## License

Apache-2.0 (Apache License, Version 2.0).
