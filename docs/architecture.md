# HeliosProxy Architecture

This document describes the internal architecture of HeliosProxy, including its 24 feature modules, data-flow pipeline, and topology provider abstraction.

---

## System Overview

HeliosProxy is a PostgreSQL wire-protocol proxy that sits between client applications and a cluster of PostgreSQL-compatible backends. It intercepts, inspects, routes, caches, and transforms queries without requiring changes to client code or database schemas.

```
                                   HeliosProxy
                      +-----------------------------------------+
                      |                                         |
  Client Connections  |  Protocol   Pipeline   Load Balancer    |  Backend Connections
  ───────────────────>|  Decoder ──> Engine ──> & Router ───────|──────────────────>
  (PostgreSQL wire)   |     |          |            |           |  (Primary, Standby,
                      |     v          v            v           |   Replica nodes)
                      |  Auth     Query Cache   Failover       |
                      |  Proxy    (L1/L2/L3)    Controller     |
                      |     |          |            |           |
                      |     v          v            v           |
                      |  Rate      Analytics    Switchover     |
                      |  Limiter   & Slow Log   Buffer         |
                      |                                         |
                      |  Admin API (REST) ── Port 9090          |
                      +-----------------------------------------+
```

---

## Module Map

The 24 feature modules are organized into six categories. Modules marked *(core)* are always compiled; all others are gated behind Cargo feature flags and excluded from the binary when not needed.

### Core Modules (Always Available)

These modules form the minimum viable proxy. They are compiled unconditionally and provide connection management, routing, health checking, and failover.

| Module | Source | Responsibility |
|--------|--------|----------------|
| `server` | `src/server.rs` | TCP listener, client session management, PostgreSQL startup handshake |
| `protocol` | `src/protocol.rs` | PostgreSQL wire-protocol codec (frontend/backend message framing) |
| `config` | `src/config.rs` | TOML configuration loading, validation, and environment variable overrides |
| `admin` | `src/admin.rs` | REST API server on the admin port, Prometheus metrics, SQL routing API |
| `connection_pool` | `src/connection_pool.rs` | Basic connection pool with min/max sizing, idle timeout, and health-on-acquire |
| `load_balancer` | `src/load_balancer.rs` | Read/write splitting, five routing strategies, weighted node selection |
| `health_checker` | `src/health_checker.rs` | Periodic backend health probes with configurable thresholds |
| `failover_controller` | `src/failover_controller.rs` | Automatic failover with candidate ranking, sync-standby preference |
| `switchover_buffer` | `src/switchover_buffer.rs` | Query buffer during planned switchover, drains to new primary |
| `primary_tracker` | `src/primary_tracker.rs` | Pluggable topology discovery, primary change events |
| `pipeline` | `src/pipeline.rs` | Extended query protocol pipelining (Parse/Bind/Execute batching) |
| `batch` | `src/batch.rs` | Automatic INSERT coalescing into multi-row batches |

### Connection Pooling Modes (`pool-modes`)

| Module | Source | Responsibility |
|--------|--------|----------------|
| `pool::mode` | `src/pool/mode.rs` | Session, Transaction, and Statement pooling mode logic |
| `pool::manager` | `src/pool/manager.rs` | Pool lifecycle management, lease acquisition, connection return |
| `pool::lease` | `src/pool/lease.rs` | Connection lease tracking with automatic release |
| `pool::prepared` | `src/pool/prepared.rs` | Prepared statement tracking across pooled connections |
| `pool::reset` | `src/pool/reset.rs` | Connection reset sequences (DISCARD ALL, SET defaults) |
| `pool::hardening` | `src/pool/hardening.rs` | Connection validation and hardening before reuse |
| `pool::session` | `src/pool/session.rs` | Session-mode pool backend |
| `pool::transaction` | `src/pool/transaction.rs` | Transaction-mode pool backend |
| `pool::statement` | `src/pool/statement.rs` | Statement-mode pool backend |

### Transaction Replay (`ha-tr`)

| Module | Source | Responsibility |
|--------|--------|----------------|
| `transaction_journal` | `src/transaction_journal.rs` | Write-ahead journal for in-flight transactions |
| `failover_replay` | `src/failover_replay.rs` | Replay coordinator during failover |
| `session_migrate` | `src/session_migrate.rs` | Session state capture and restore (SET parameters, prepared statements) |
| `cursor_restore` | `src/cursor_restore.rs` | Cursor position preservation across failover |

### Query Intelligence

| Module | Feature Flag | Source | Responsibility |
|--------|-------------|--------|----------------|
| Query Cache | `query-cache` | `src/cache/` | Three-tier result cache (L1 hot, L2 warm, L3 semantic) |
| Query Routing | `routing-hints` | `src/routing/` | SQL comment-based routing hints, automatic read/write classification |
| Lag-Aware Routing | `lag-routing` | `src/lag/` | Replication lag monitoring, read-your-writes consistency |
| Query Rewriter | `query-rewriting` | `src/rewriter/` | Rule-based SQL transformation engine |
| Query Analytics | `query-analytics` | `src/analytics/` | Query fingerprinting, slow query log, N+1 detection, intent classification |
| Schema Routing | `schema-routing` | `src/schema_routing/` | Table-level routing rules, data temperature classification |

### Security and Access Control

| Module | Feature Flag | Source | Responsibility |
|--------|-------------|--------|----------------|
| Auth Proxy | `auth-proxy` | `src/auth/` | JWT, OAuth 2.0, API key, LDAP, and certificate-based authentication |
| Rate Limiter | `rate-limiting` | `src/rate_limit/` | Token bucket, sliding window, concurrency guards |
| Circuit Breaker | `circuit-breaker` | `src/circuit_breaker/` | Adaptive circuit breaker with sliding-window failure detection |

### Multi-Tenancy and Extensibility

| Module | Feature Flag | Source | Responsibility |
|--------|-------------|--------|----------------|
| Multi-Tenancy | `multi-tenancy` | `src/multi_tenancy/` | Tenant isolation, per-tenant pools, resource quotas |
| WASM Plugins | `wasm-plugins` | `src/plugins/` | Sandboxed WASM runtime with hot-reload |
| GraphQL Gateway | `graphql-gateway` | `src/graphql/` | GraphQL-to-SQL translation with DataLoader batching |

### Distributed Caching

| Module | Feature Flag | Source | Responsibility |
|--------|-------------|--------|----------------|
| DistribCache | `distribcache` | `src/distribcache/` | Multi-tier distributed cache with AI workload classification |

---

## Data-Flow Pipeline

Every client query traverses a well-defined pipeline. Each stage is optional and only executes when its corresponding feature flag is enabled.

```
Client Connection (TCP, PostgreSQL wire protocol)
        |
        v
  +-----------+
  | Protocol  |  Decode PostgreSQL frontend messages
  | Decoder   |  (Query, Parse, Bind, Execute, etc.)
  +-----------+
        |
        v
  +-----------+
  | Auth      |  [auth-proxy] JWT / OAuth / API key validation
  | Proxy     |  Skip if feature not enabled; pass-through auth to backend
  +-----------+
        |
        v
  +-----------+
  | Tenant    |  [multi-tenancy] Identify tenant from connection metadata
  | Identify  |  Apply per-tenant pool and quota limits
  +-----------+
        |
        v
  +-----------+
  | Rate      |  [rate-limiting] Token bucket / sliding window check
  | Limiter   |  Reject with error if rate exceeded
  +-----------+
        |
        v
  +-----------+
  | Circuit   |  [circuit-breaker] Check breaker state for target node
  | Breaker   |  Reject immediately if circuit is open
  +-----------+
        |
        v
  +-----------+
  | WASM      |  [wasm-plugins] Execute on_query_start hooks
  | Plugins   |  Plugins may modify, reject, or annotate the query
  +-----------+
        |
        v
  +-----------+
  | Query     |  [query-rewriting] Apply rewrite rules
  | Rewriter  |  Schema prefixing, query normalization
  +-----------+
        |
        v
  +-----------+
  | Query     |  [query-analytics] Fingerprint and classify the query
  | Analytics |  Record execution start time for latency tracking
  +-----------+
        |
        v
  +-----------+
  | Routing   |  Parse SQL hints (/*helios:route=primary*/)
  | Hints     |  [routing-hints] Override default routing decision
  +-----------+
        |
        v
  +-----------+
  | Query     |  [query-cache] Check L1 -> L2 -> L3 cache tiers
  | Cache     |  Return cached result if hit; skip backend entirely
  +-----------+
        |
        v  (cache miss)
  +-----------+
  | Schema    |  [schema-routing] Route based on table metadata
  | Router    |  and data temperature classification
  +-----------+
        |
        v
  +-----------+
  | Lag-Aware |  [lag-routing] Filter replicas by replication lag
  | Router    |  Enforce read-your-writes consistency
  +-----------+
        |
        v
  +-----------+
  | Load      |  Select target node (round-robin, least-conn, latency-based)
  | Balancer  |  Read/write splitting: writes -> primary, reads -> standbys
  +-----------+
        |
        v
  +-----------+
  | Connection|  Acquire backend connection from pool
  | Pool      |  [pool-modes] Session / Transaction / Statement mode
  +-----------+
        |
        v
  +-----------+
  | Pipeline  |  Batch Parse/Bind/Execute for reduced round trips
  | Engine    |  [ha-tr] Journal statement in transaction journal
  +-----------+
        |
        v
  +-----------+
  | Batch     |  Coalesce individual INSERTs into multi-row batches
  | INSERT    |
  +-----------+
        |
        v
  Backend Node (Primary / Standby / Replica)
        |
        v  (response)
  +-----------+
  | Query     |  [query-cache] Populate cache on miss
  | Cache     |  [query-analytics] Record execution time, update statistics
  | Populate  |  [wasm-plugins] Execute on_query_complete hooks
  +-----------+
        |
        v
  Client Response (PostgreSQL wire protocol)
```

---

## Topology Provider Abstraction

The `PrimaryTracker` is the central component for primary node discovery. It is decoupled from any specific database backend through the `TopologyProvider` trait.

### TopologyProvider Trait

```rust
pub trait TopologyProvider: Send + Sync + 'static {
    /// Subscribe to topology change events.
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent>;

    /// Get the current primary node, if one exists.
    fn get_primary(&self) -> Option<TopologyNodeInfo>;

    /// Look up a node by its UUID.
    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo>;
}
```

### Topology Events

```rust
pub enum TopologyEvent {
    PrimaryChanged { old_primary: Option<Uuid>, new_primary: Uuid },
    NodeLeft { node_id: Uuid },
    HealthChanged { node_id: Uuid, is_healthy: bool },
}
```

### Provider Implementations

| Provider | Feature Flag | Discovery Method | Failover Detection |
|----------|-------------|------------------|--------------------|
| PostgreSQL | `postgres-topology` | Polls `pg_is_in_recovery()` on each node | Detects role change across polling intervals |
| HeliosDB | `heliosdb-topology` | Subscribes to internal TopologyManager events | Event-driven, zero polling |
| Manual / Standalone | *(none)* | API calls: `set_primary()`, `clear_primary()` | External orchestration |

### PrimaryTracker Operating Modes

1. **Provider-backed** -- Created with `PrimaryTracker::with_provider()`. Subscribes to `TopologyEvent` broadcasts and runs a periodic consistency check. Fully automatic.

2. **Standalone** -- Created with `PrimaryTracker::new_standalone()`. Primary is managed through explicit `set_primary()`, `confirm_primary()`, and `clear_primary()` calls. Suitable for external failover managers (Patroni, pg_auto_failover, Stolon) that notify the proxy via the Admin API.

### Primary Lifecycle

```
  set_primary(id, address)       confirm_primary()        clear_primary()
  ───────────────────────> PENDING ─────────────────> CONFIRMED ──────────> NONE
                             |                                     ^
                             |          (node lost / unhealthy)    |
                             +─────────────────────────────────────+
```

Events are broadcast to all subscribers (load balancer, failover controller, switchover buffer) on each transition.

---

## Failover Sequence

When the primary becomes unreachable, the proxy orchestrates the following sequence:

```
1. Health checker detects primary failure (failure_threshold consecutive failures)
      |
2. Failover controller enters FailoverState::Detecting
      |
3. Switchover buffer activates -- incoming writes are buffered
      |
4. Failover controller ranks candidates by:
   a. Sync standby preference (is_sync = true ranked first)
   b. Replication lag (lowest lag_bytes preferred)
   c. Configured priority
      |
5. Best candidate selected, primary tracker updated
      |
6. [ha-tr] Transaction journal identifies in-flight transactions
      |
7. [ha-tr] Failover replay re-executes journaled statements on new primary
      |
8. [ha-tr] Session migrate restores SET parameters and prepared statements
      |
9. [ha-tr] Cursor restore repositions open cursors
      |
10. Primary tracker confirmed, switchover buffer drains to new primary
      |
11. Normal operation resumes
```

---

## Connection Pool Architecture

The connection pool operates in three modes, each with different connection return semantics:

```
                    +-----------+
                    |  Client   |
                    | Session   |
                    +-----+-----+
                          |
                   acquire_connection()
                          |
                    +-----v-----+
                    |   Pool    |
                    |  Manager  |
                    +-----+-----+
                          |
              +-----------+-----------+
              |           |           |
        +-----v---+ +----v----+ +----v----+
        | Session | | Txn     | | Stmt    |
        | Mode    | | Mode    | | Mode    |
        +---------+ +---------+ +---------+
        Return on    Return on   Return on
        disconnect   COMMIT/     each
                     ROLLBACK    statement
```

Each mode passes the connection through a reset sequence (`DISCARD ALL` by default) before returning it to the pool. Prepared statement tracking (`track` or `named` mode) preserves statement definitions across connection reuse.

---

## Admin API Architecture

The admin server runs on a separate TCP listener (default port 9090) and exposes a REST API for monitoring, management, and SQL routing.

```
  Admin Port (9090)
        |
  +-----v-----------+
  |  HTTP Parser     |
  |  (built-in)      |
  +-----+-----------+
        |
  +-----v-----------+
  |  Request Router  |
  |  (method + path) |
  +-----+-----------+
        |
        +-- GET  /health         --> Liveness check
        +-- GET  /health/ready   --> Readiness (at least one healthy node)
        +-- GET  /health/live    --> Simple alive check
        +-- GET  /nodes          --> All node health status
        +-- GET  /nodes/{addr}   --> Single node health
        +-- POST /nodes/{addr}/enable  --> Enable node
        +-- POST /nodes/{addr}/disable --> Disable node
        +-- GET  /config         --> Current configuration snapshot
        +-- GET  /metrics        --> JSON metrics
        +-- GET  /metrics/prometheus --> Prometheus text format
        +-- GET  /sessions       --> Active session count
        +-- GET  /pools          --> Pool statistics
        +-- GET  /version        --> Proxy version
        +-- POST /api/sql        --> SQL execution with TWR routing
```

---

## See Also

- [Configuration Reference](configuration.md)
- [Feature Flags](feature-flags.md)
- [Admin API Reference](admin-api.md)
- [Topology Providers](topology-providers.md)
