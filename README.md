# HeliosProxy

**High-performance connection router, intelligent query processor, and failover manager for PostgreSQL-wire-compatible databases.**

HeliosProxy operates at the PostgreSQL wire protocol level, making it compatible with any database that speaks the PostgreSQL protocol — including PostgreSQL, HeliosDB, CockroachDB, YugabyteDB, and others.

---

## Overview

HeliosProxy sits between your application and your database cluster, providing transparent connection pooling, automatic failover, intelligent query routing, programmable plugins, and operations tooling without application code changes.

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

All 46 feature modules operate at the PostgreSQL wire protocol level. They inspect, route, cache, and transform queries without requiring changes to your application or database. Any client library that connects to PostgreSQL connects to HeliosProxy.

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

HeliosProxy features are grouped into a connection-routing tier and a programmable platform tier. Each module is independently enabled via Cargo feature flags, so deployments can range from a lightweight connection pooler to a full programmable data-plane.

## Connection-Routing Tier

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
| **Transaction Replay (TR)** | `ha-tr` | Journals in-flight transactions and transparently replays them on a new primary after failover — zero data loss for committed work, with statement ordering and parameter fidelity preserved |
| **Session Migration** | `ha-tr` | Captures and restores full session state (SET parameters, prepared statements, advisory locks) when moving connections between nodes |
| **Cursor Restore** | `ha-tr` | Preserves open cursor positions across failover — clients resume fetching without re-executing the query |
| **Switchover Buffer** | *(core)* | Buffers incoming queries during planned switchover, drains them to the new primary once promotion completes |
| **Primary Tracker** | *(core)* | Pluggable topology discovery — tracks the current primary via `pg_is_in_recovery()` polling (PostgreSQL), HeliosDB topology events, or manual API calls |
| **Transaction Journal** | `ha-tr` | Write-ahead journal for in-flight transactions with statement-level granularity, parameter capture, and configurable retention |

### Query Intelligence

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Query Cache** | `query-cache` / `distribcache` | Three-tier result cache (L1 hot / L2 warm / L3 semantic) with TTL-based expiration, pattern-based invalidation, and normalized query fingerprinting. The optional `distribcache` flag adds an AI-powered distributed tier with workload classification, access heatmaps, and intelligent prefetching |
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

## Platform Tier

The platform tier builds on the connection-routing core with a hardened WASM plugin host, an operations and observability surface, first-party plugins, and infrastructure-as-code companion projects.

### Detection & Edge

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Anomaly Detection** | `anomaly-detection` | In-process sliding-window detector: rate spikes (z-score vs. rolling EWMA), credential-stuffing bursts, six classes of SQL-injection patterns, and novel query shapes. Events stream over `GET /anomalies` — no external SIEM required |
| **Edge Mode** | `edge-proxy` | Cache-first geo/edge proxy. Each edge terminates reads against a local LRU+TTL+version cache; the home proxy broadcasts table-scoped invalidations on writes. Last-write-wins, no consensus |

### Programmable WASM Data-Plane

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Plugin Host KV** | `wasm-plugins` | `env.kv_get` / `kv_set` / `kv_delete` host imports with per-plugin namespaced state that persists across hook invocations |
| **Plugin Host Crypto** | `wasm-plugins` | `env.sha256_hex` host import backed by the audited `sha2` crate — plugins compute real SHA-256 without embedding the algorithm |
| **Plugin Signatures** | `wasm-plugins` | Optional Ed25519 trust root: every loaded `.wasm` requires a verifying `.sig` sidecar. Interoperates with `openssl` / `signify` |
| **OCI Plugin Artefacts** | `wasm-plugins` | `.tar.gz` distribution format (`manifest.json` + `plugin.wasm` + optional `plugin.sig`); the loader validates the wasm SHA-256 against the manifest |
| **Plugin Route-Block** | `wasm-plugins` | `RouteResult::Block { reason }` ABI variant — a plugin can hard-reject a query, synthesising a PostgreSQL `ErrorResponse` + `ReadyForQuery` |
| **Plugin Trust Root Config** | `wasm-plugins` | `[plugins].trust_root` in `proxy.toml` auto-attaches the signature verifier; permissive when unset for dev-loop ergonomics |

### Admin Surface

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Admin Web UI** | *(core)* | Single embedded HTML dashboard at `/` and `/ui`. Ten auto-refreshing panels: Nodes, Topology, Plugins, Anomalies, Edge Mode, Chaos Mode, Shadow Execution, Time-Travel Replay, SQL Console, and Traffic |
| **Admin REST** | *(core)* | Endpoints surfacing platform capabilities: `/topology`, `/plugins`, `/anomalies`, `/api/edge*`, `/api/chaos`, `/api/shadow`, `/api/replay`. Operator and UI consume the same JSON |

### First-Party Plugins

Eight signed WASM plugins (shipped in the companion `HDB-HeliosDB-Proxy-Plugins` repo, loaded via `wasm-plugins`):

| Module | Description |
|--------|-------------|
| **cost-governor** | Per-tenant query cost budgets (minute / hour / day windows), tracked in the plugin KV namespace |
| **ai-classifier** | Detects LLM-generated SQL via `application_name` keywords and generated-by markers; extracts `agent_id` + `model_id` for downstream plugins |
| **token-budget** | Per-`(agent, model)` cost gating for AI traffic with sliding-window budgets |
| **llm-guardrail** | Refuses dangerous AI SQL: `DROP`/`TRUNCATE`, `DELETE`/`UPDATE` without `WHERE`, `SELECT` without `LIMIT` on large tables, missing tenant filter |
| **pgvector-router** | Routes pgvector top-K queries (`<->`, `<#>`, `<=>`) to a topology-tagged vector replica |
| **column-mask** | Per-role column masking via SQL rewriting (`SELECT ssn` → `SELECT mask_ssn(ssn) AS ssn`) |
| **audit-chain** | Hash-chained tamper-evident audit log using the `env.sha256_hex` host import |
| **residency-router** | Per-user data-residency routing by `helios.region`; blocks cross-region access with a proper PG error |

### Companion Projects

| Module | Description |
|--------|-------------|
| **`helios-plugin` CLI** | Pack, inspect, and verify WASM plugin artefacts as portable `.tar.gz` using the same Ed25519 trust-root format as the proxy loader |
| **Kubernetes Operator** | CRDs for `HeliosProxy`, `PoolProfile`, `RoutingRule`, `AuditPolicy`, `TenantQuota`; reconciler renders ConfigMap + Deployment + Service and polls `/topology` for status |
| **Terraform Provider** | Five resources mirroring the operator CRDs (`heliosproxy_instance`, `_pool_profile`, `_routing_rule`, `_audit_policy`, `_tenant_quota`) |
| **Pulumi Provider** | Wraps the Terraform provider via `pulumi-terraform-bridge` — same five resources in TypeScript / Python / Go / .NET |

---

## Ecosystem

HeliosDB is maintained under the **HeliosDatabase** organization. The database
editions, SDKs, proxy data-plane, and MCP knowledge tooling are designed to work
together across the same family.

- **[HeliosDatabase/HeliosDB-SDKs](https://github.com/HeliosDatabase/HeliosDB-SDKs)** — Official client SDKs (Python, TypeScript, Rust, Go) + integrations (VS Code, n8n, Zapier, Make, Retool, AutoGen) + cross-platform CLI. Apache 2.0.
- **[HeliosDatabase/HeliosDB-Nano](https://github.com/HeliosDatabase/HeliosDB-Nano)** — Single-binary embedded database with PostgreSQL/MySQL wire compatibility, HNSW vector search, branching, time-travel, and MCP endpoint support. Apache 2.0.
- **[HeliosDatabase/HeliosDB-Lite](https://github.com/HeliosDatabase/HeliosDB-Lite)** — Production self-hosted database with HeliosProxy + HeliosCore baked in. SSPL-1.0.
- **[HeliosDatabase/HeliosDB-Full](https://github.com/HeliosDatabase/HeliosDB-Full)** — Distributed enterprise database with 14 native wire protocols. SSPL-1.0.
- **[HeliosDatabase/HeliosDB-Proxy](https://github.com/HeliosDatabase/HeliosDB-Proxy)** — Programmable Postgres data-plane (PgBouncer drop-in + WASM plugins + zero-downtime PG-12->17 upgrade). Apache 2.0.
- **[HeliosDatabase/HeliosDB-Proxy-Plugins](https://github.com/HeliosDatabase/HeliosDB-Proxy-Plugins)** · **[Operator](https://github.com/HeliosDatabase/HeliosDB-Proxy-Operator)** · **[Terraform](https://github.com/HeliosDatabase/terraform-provider-HeliosDB-Proxy)** · **[Pulumi](https://github.com/HeliosDatabase/pulumi-HeliosDB-Proxy)** — Proxy ecosystem.

**[HeliosDatabase/HeliosDB-CodeKB-MCP](https://github.com/HeliosDatabase/HeliosDB-CodeKB-MCP)** provides an MCP server that turns HeliosDB codebases and docs into queryable technical knowledge for Claude Code, Codex, and other MCP clients.

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
| `anomaly-detection` | No | In-process anomaly detection (rate spikes, credential stuffing, SQLi heuristics, novel queries) |
| `edge-proxy` | No | Cache-first edge / geo proxy mode with last-write-wins TTL coherence |
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

### Platform endpoints

```bash
# Cluster topology for external controllers (currentPrimary, healthy/unhealthy nodes)
curl http://localhost:9090/topology

# Loaded WASM plugins with invocation + error counts
curl http://localhost:9090/plugins

# Recent anomaly events (rate spikes, credential bursts, SQLi patterns, novel queries)
curl http://localhost:9090/anomalies?limit=50

# Chaos engineering — fault injection to validate the failover path
curl -X POST http://localhost:9090/api/chaos -d '{"action":"force_unhealthy","node":"pg-primary"}'

# Shadow execution — run a query against source + shadow backends and diff results
curl -X POST http://localhost:9090/api/shadow -d '{"query":"SELECT count(*) FROM orders"}'

# Time-travel replay — replay a journal window against a target backend
curl -X POST http://localhost:9090/api/replay -d '{"from":"...","to":"..."}'

# Edge mode — cache stats, edge registration, invalidation broadcast
curl http://localhost:9090/api/edge
```

The embedded admin Web UI exposes all of the above at `/` and `/ui`.

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
