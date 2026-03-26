# HeliosProxy Feature Flags

HeliosProxy uses Cargo feature flags to control which modules are compiled into the binary. This allows operators to build a proxy tailored to their exact requirements, from a lightweight connection pooler to a full-featured intelligent proxy.

---

## Feature Flag Reference

### Default Features

| Feature | Default | Description |
|---------|---------|-------------|
| `pool-modes` | **Yes** | Session, Transaction, and Statement connection pooling. |

The `pool-modes` feature is enabled by default. To build without it, pass `--no-default-features`.

### Core Proxy Features

| Feature | Default | Description | Dependencies |
|---------|---------|-------------|--------------|
| `pool-modes` | Yes | Connection pooling modes (Session/Transaction/Statement). | None |
| `ha-tr` | No | Transaction Replay -- failover replay, cursor restore, session migration. | None |
| `query-cache` | No | L1/L2/L3 multi-tier query result caching. | None |
| `routing-hints` | No | SQL comment-based query routing hints. | None |
| `lag-routing` | No | Replica lag-aware routing with read-your-writes consistency. | None |
| `rate-limiting` | No | Token bucket and sliding window rate limiting. | None |
| `circuit-breaker` | No | Adaptive circuit breaker pattern per backend node. | None |
| `query-analytics` | No | Query fingerprinting, slow query log, N+1 detection, intent classification. | None |
| `multi-tenancy` | No | Tenant-aware routing, per-tenant pools, resource quotas. | None |
| `auth-proxy` | No | JWT, OAuth 2.0, API key, and LDAP authentication. | None |
| `query-rewriting` | No | Rule-based SQL query transformation. | None |
| `wasm-plugins` | No | Sandboxed WASM plugin runtime with hot-reload. | None |
| `graphql-gateway` | No | GraphQL-to-SQL translation with schema introspection. | None |
| `schema-routing` | No | Schema-aware routing and workload classification. | None |
| `distribcache` | No | AI-powered distributed query caching (L1/L2/L3 tiers). | None |

### Topology Providers

| Feature | Default | Description |
|---------|---------|-------------|
| `postgres-topology` | No | PostgreSQL primary discovery via `pg_is_in_recovery()` polling. |
| `heliosdb-topology` | No | HeliosDB native topology integration (event-driven, zero-polling). |

Topology providers are independent of each other. Choose one based on your backend. If neither is enabled, the proxy operates in standalone mode with manual primary management via the Admin API.

### Observability

| Feature | Default | Description |
|---------|---------|-------------|
| `observability` | No | Prometheus metrics export and OpenTelemetry tracing. Adds `prometheus` and `opentelemetry` crate dependencies. |

### Bundle Feature

| Feature | Default | Description |
|---------|---------|-------------|
| `all-features` | No | Enables all proxy feature modules. Does **not** include a topology provider -- choose `postgres-topology` or `heliosdb-topology` separately. |

---

## Feature Details

### `pool-modes` -- Connection Pooling Modes

**Modules:** `src/pool/`

Enables three connection pooling modes that control when backend connections are returned to the pool.

| Mode | Return Trigger | Use Case |
|------|---------------|----------|
| Session | Client disconnect | Legacy applications, prepared statements, long sessions |
| Transaction | COMMIT / ROLLBACK | Web applications, microservices, APIs |
| Statement | Each individual statement | Read-heavy workloads, connection-constrained environments |

Includes prepared statement tracking, connection reset sequences, lease management, and pool hardening.

### `ha-tr` -- Transaction Replay

**Modules:** `src/transaction_journal.rs`, `src/failover_replay.rs`, `src/session_migrate.rs`, `src/cursor_restore.rs`

Provides zero-data-loss failover for in-flight transactions. When the primary fails during an active transaction, the proxy:

1. Journals all statements issued within the transaction.
2. Detects failover and identifies a new primary.
3. Re-executes the journaled statements on the new primary.
4. Restores session state (SET parameters, prepared statements).
5. Repositions open cursors.

The client experiences a brief pause but does not receive an error.

### `query-cache` -- Query Caching

**Modules:** `src/cache/`

Three-tier query result cache:

- **L1 (Hot):** In-process hash map with LRU eviction. Microsecond access.
- **L2 (Warm):** Larger shared cache with normalized query fingerprinting. Sub-millisecond access.
- **L3 (Semantic):** Semantic similarity matching for near-duplicate queries.

Supports TTL-based expiration, table-based invalidation on writes, and cache bypass via SQL hints (`/* helios:cache=skip */`).

### `routing-hints` -- Query Routing Hints

**Modules:** `src/routing/`

Enables SQL comment-based routing directives:

```sql
SELECT /* helios:route=primary */ balance FROM accounts WHERE id = 1;
SELECT /* helios:route=standby */ * FROM analytics_data;
SELECT /* helios:cache=skip */ * FROM live_metrics;
```

The hint parser extracts directives from SQL comments and overrides the default routing decision made by the load balancer.

### `lag-routing` -- Lag-Aware Routing

**Modules:** `src/lag/`

Monitors replication lag on all standby and replica nodes. Reads are routed only to nodes within the configured `max_replica_lag_ms` threshold.

Includes a read-your-writes (RYW) consistency module that tracks the last write LSN per session and ensures subsequent reads from that session go to a node that has replicated past that LSN.

### `rate-limiting` -- Rate Limiting

**Modules:** `src/rate_limit/`

Provides three rate limiting algorithms:

- **Token bucket:** Sustained rate with configurable burst.
- **Sliding window:** Fixed-window counting with sub-window smoothing.
- **Concurrency guard:** Limits the number of concurrent in-flight queries.

Supports per-user, per-tenant, and per-IP policies. Includes a query cost estimator that weights complex queries higher than simple ones.

### `circuit-breaker` -- Circuit Breaker

**Modules:** `src/circuit_breaker/`

Adaptive circuit breaker with three states:

| State | Behavior |
|-------|----------|
| Closed | Normal operation. Failures are counted. |
| Open | All requests to the affected node are rejected immediately. Entered after `failure_threshold` consecutive failures. |
| Half-Open | After `recovery_timeout_secs`, a single probe request is sent. If it succeeds, the circuit closes. If it fails, the circuit remains open. |

Each backend node has an independent breaker. The breaker uses a sliding-window failure counter to avoid false triggers from transient errors.

### `query-analytics` -- Query Analytics

**Modules:** `src/analytics/`

Real-time query analysis:

- **Fingerprinting:** Normalizes queries by replacing literals with placeholders, grouping identical query patterns.
- **Statistics:** Per-fingerprint execution count, latency histogram (P50/P95/P99), rows returned.
- **Slow query log:** Logs queries exceeding the configured threshold with full SQL, parameters, and execution time.
- **N+1 detection:** Identifies repeated identical queries within a session that indicate an N+1 query pattern.
- **Intent classification:** Classifies queries as OLTP, OLAP, DDL, or administrative.

### `multi-tenancy` -- Multi-Tenancy

**Modules:** `src/multi_tenancy/`

Tenant-aware proxy operation:

- **Identification:** Extracts tenant ID from connection metadata (database name, username, or application_name).
- **Isolation:** Per-tenant connection pools with independent sizing.
- **Quotas:** Per-tenant rate limits and maximum connection limits.
- **Schema isolation:** Optional schema-prefix transformation for shared-database multi-tenancy.

### `auth-proxy` -- Authentication Proxy

**Modules:** `src/auth/`

Proxy-level authentication supporting multiple backends:

- **JWT validation:** Verify JSON Web Tokens with configurable claims and JWKS endpoints.
- **OAuth 2.0:** Token introspection and refresh token flow.
- **API keys:** Static or database-backed API key validation.
- **LDAP:** Bind-based authentication against an LDAP directory.
- **Certificate-based:** Mutual TLS with client certificate verification.

Includes a role mapper that translates authentication identities to PostgreSQL roles.

### `query-rewriting` -- Query Rewriting

**Modules:** `src/rewriter/`

Rule-based SQL transformation engine. Rules are evaluated in order and can:

- Add schema prefixes to unqualified table names.
- Replace deprecated syntax with modern equivalents.
- Inject query hints for routing or caching.
- Redirect queries to different tables or schemas.

### `wasm-plugins` -- WASM Plugin System

**Modules:** `src/plugins/`

Sandboxed WebAssembly plugin runtime:

- **Hooks:** `on_query_start`, `on_query_complete`, `on_connection`, `on_error`.
- **Hot-reload:** Plugins can be updated without restarting the proxy.
- **Sandbox:** Each plugin runs in an isolated WASM sandbox with configurable memory limits and a 100ms execution timeout.
- **Host functions:** Plugins can call back into the proxy to read metrics, log messages, and modify query routing.

### `graphql-gateway` -- GraphQL Gateway

**Modules:** `src/graphql/`

Automatic GraphQL endpoint on the admin port:

- **Schema introspection:** Reflects the database schema into a GraphQL schema.
- **Query translation:** Converts GraphQL queries into optimized SQL.
- **DataLoader batching:** Automatically batches N+1 field resolvers into single queries.
- **Validation:** Validates GraphQL queries against the reflected schema before execution.

### `schema-routing` -- Schema-Aware Routing

**Modules:** `src/schema_routing/`

Routes queries based on table-level metadata:

- **Data temperature:** Classify tables as hot, warm, or cold and route accordingly.
- **Workload type:** OLTP tables to low-latency nodes, analytics tables to read replicas.
- **Admin API:** Runtime management of schema routing rules.

### `distribcache` -- Distributed Cache

**Modules:** `src/distribcache/`

Multi-tier intelligent caching for AI and traditional workloads:

- **L1 (Hot):** In-process cache with microsecond access. Sized by `l1_size_mb`.
- **L2 (Warm):** Shared-memory cache with sub-millisecond access. Sized by `l2_size_mb`.
- **L3 (Distributed):** External Redis-compatible cluster for cross-instance cache sharing.
- **Workload classification:** OLTP, OLAP, Vector, AI/RAG -- each with tuned caching strategies.
- **Heatmap:** Access frequency tracking for intelligent eviction and prefetch.
- **AI-specific:** Embedding prefetch for RAG pipelines, conversation context caching for AI agents.

---

## Recommended Combinations

### Lightweight Connection Pooler

Minimal overhead. Suitable for replacing PgBouncer.

```bash
cargo build --release --features "pool-modes"
```

Included: connection pooling (Session/Transaction/Statement), load balancing, health checking, basic failover.

### High-Availability Proxy

Connection pooling with full failover protection and topology discovery.

```bash
cargo build --release --features "pool-modes,ha-tr,postgres-topology"
```

Included: all of the above plus Transaction Replay, cursor restore, session migration, and automatic primary discovery via PostgreSQL polling.

### Intelligent Query Proxy

Adds query-level intelligence to the HA proxy.

```bash
cargo build --release --features "pool-modes,ha-tr,postgres-topology,query-cache,routing-hints,lag-routing,query-analytics"
```

Included: all of the above plus query caching, routing hints, lag-aware routing, and query analytics.

### Enterprise Proxy

Full security and multi-tenancy stack.

```bash
cargo build --release --features "pool-modes,ha-tr,postgres-topology,query-cache,routing-hints,lag-routing,query-analytics,rate-limiting,circuit-breaker,multi-tenancy,auth-proxy,query-rewriting"
```

### Full-Featured Proxy

Every module enabled. Maximum capability.

```bash
cargo build --release --features "all-features,postgres-topology,observability"
```

### HeliosDB-Integrated Proxy

For deployment within the HeliosDB ecosystem with native topology events.

```bash
cargo build --release --features "all-features,heliosdb-topology,observability"
```

---

## Build Size Impact

Each feature flag adds only the modules it requires. Approximate binary size impact (release build, `x86_64-unknown-linux-gnu`):

| Configuration | Approximate Size |
|---------------|-----------------|
| `pool-modes` only | ~8 MB |
| HA proxy (`pool-modes,ha-tr,postgres-topology`) | ~12 MB |
| Intelligent proxy (+ cache, analytics, routing) | ~18 MB |
| Full-featured (`all-features,postgres-topology,observability`) | ~28 MB |

Exact sizes depend on the target platform and link-time optimization settings.

---

## See Also

- [Architecture](architecture.md) -- Module map and data-flow pipeline
- [Configuration Reference](configuration.md) -- Every configuration key documented
- [Deployment Guides](deployment/) -- Standalone, Docker, and Kubernetes deployment
