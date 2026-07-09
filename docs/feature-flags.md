# HeliosProxy Feature Flags

HeliosProxy uses Cargo feature flags to control which modules are compiled into the binary. This allows operators to build a proxy tailored to their exact requirements, from a lightweight connection pooler to a full-featured intelligent proxy.

The authoritative list is the `[features]` table in `Cargo.toml`. This document tracks it flag-for-flag.

---

## Feature Flag Reference

### Default Features

| Feature | Default | Description |
|---------|---------|-------------|
| `pool-modes` | **Yes** | Session, Transaction, and Statement connection pooling. |

`pool-modes` is the only default feature (`default = ["pool-modes"]`). To build without it, pass `--no-default-features`.

### Core Proxy Features

All of the following are **off by default**. Enable them individually or via the `all-features` bundle.

| Feature | Pulls extra deps | Description |
|---------|------------------|-------------|
| `pool-modes` | None | Connection pooling modes (Session/Transaction/Statement). Enabled by default. |
| `ha-tr` | None | Transaction Replay -- failover replay, cursor restore, session migration. |
| `query-cache` | None | L1 hot / L2 warm / L3 semantic query result caching. |
| `routing-hints` | None | SQL comment-based query routing hints. |
| `lag-routing` | None | Replica lag-aware routing with read-your-writes consistency. |
| `rate-limiting` | None | Token bucket, sliding window, and concurrency rate limiting. |
| `circuit-breaker` | None | Adaptive circuit breaker pattern per backend node. |
| `query-analytics` | None | Query fingerprinting, slow query log, N+1 detection, intent classification. |
| `multi-tenancy` | None | Tenant-aware routing, per-tenant pools, resource quotas. |
| `auth-proxy` | None | Proxy-level authentication scaffolding (JWT, OAuth, API keys). |
| `ldap-auth` | `ldap3` | LDAP search + bind auth. Implies `auth-proxy`. Off by default even within `auth-proxy` because it pulls the `ldap3` client and its rustls stack. |
| `query-rewriting` | None | Rule-based SQL query transformation. |
| `wasm-plugins` | `wasmtime` | Sandboxed WASM plugin runtime with hot-reload. |
| `graphql-gateway` | None | GraphQL-to-SQL gateway with schema introspection. |
| `schema-routing` | None | Schema-aware routing and workload classification. |
| `distribcache` | None | DistribCache library module (see caveat under Feature Details). |
| `anomaly-detection` | None | In-process anomaly heuristics: rate spikes, credential stuffing, SQL-injection patterns, novel query shapes. |
| `edge-proxy` | `lru` | Edge / geo proxy mode: cache-first handling, last-write-wins TTL coherence, pull-on-miss + invalidation push. |

### Topology Providers

| Feature | Default | Description |
|---------|---------|-------------|
| `postgres-topology` | No | PostgreSQL primary discovery via `pg_is_in_recovery()` polling. Used throughout `src/primary_tracker.rs`. |
| `heliosdb-topology` | No | Bridge to the HeliosDB-Lite internal `TopologyManager` (event-driven). Compiles a provider module that is only usable when HeliosProxy is built as part of the HeliosDB-Lite workspace. |

Topology providers are independent of each other. Choose one based on your backend. If neither is enabled, the proxy operates in standalone mode with manual primary management via the Admin API.

### Observability

| Feature | Default | Description |
|---------|---------|-------------|
| `observability` | No | Pulls the `prometheus` and `opentelemetry` crate dependencies **only**. See the caveat below -- in the current tree it wires no code. |

> **Caveat (verify before relying on it):** `observability = ["dep:prometheus", "dep:opentelemetry"]` has **zero** `#[cfg(feature = "observability")]` guards anywhere in `src/` (grep count: 0). Enabling it compiles the two dependency crates into the binary but activates no exporter, tracer, or metrics wiring. Prometheus-style metrics are served regardless of this flag by the Admin HTTP API at `GET /metrics` and `GET /metrics/prometheus` (`src/admin.rs`), which are independent of `observability`.

### Bundle Features

| Feature | Description |
|---------|-------------|
| `all-features` | Every proxy feature module. Does **not** include a topology provider or `observability` -- add those separately. |
| `msrv-features` | MSRV CI verification bundle. Same as `all-features` **minus `wasm-plugins` and `ldap-auth`** (both pull heavy optional deps -- `wasmtime` and `ldap3` -- that slow the MSRV compile). |

`all-features` expands to: `pool-modes`, `ha-tr`, `query-cache`, `routing-hints`, `lag-routing`, `rate-limiting`, `circuit-breaker`, `query-analytics`, `multi-tenancy`, `auth-proxy`, `ldap-auth`, `query-rewriting`, `wasm-plugins`, `graphql-gateway`, `schema-routing`, `distribcache`, `anomaly-detection`, `edge-proxy`.

`msrv-features` expands to the same set **without** `wasm-plugins` and `ldap-auth`.

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

Supports TTL-based expiration, table-based invalidation on writes, and cache bypass via SQL hints.

### `routing-hints` -- Query Routing Hints

**Modules:** `src/routing/`

Enables SQL comment-based routing directives (`/*helios:key=value,...*/`, parsed by `src/routing/hint_parser.rs`):

```sql
SELECT /*helios:route=primary*/ balance FROM accounts WHERE id = 1;
SELECT /*helios:route=standby*/ * FROM analytics_data;
```

The hint parser extracts directives from SQL comments and overrides the default routing decision made by the load balancer. Requires `[routing_hints] enabled = true` in `proxy.toml`.

### `lag-routing` -- Lag-Aware Routing

**Modules:** `src/lag/`

Monitors replication lag on all standby and replica nodes. Reads are routed only to nodes within the configured lag threshold.

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

Proxy-level authentication scaffolding: JWT validation, OAuth 2.0 token flows, and API-key validation, plus a role mapper that translates authentication identities to PostgreSQL roles.

> Note: the wired `proxy.toml` `[auth]` section currently exposes only `mode = "passthrough" | "scram"` plus `auth_file` (see `ProxyConfig` in `src/config.rs`). LDAP is a separate feature -- see `ldap-auth` below.

### `ldap-auth` -- LDAP Authentication

**Implies:** `auth-proxy` · **Pulls:** `ldap3`

Search + bind authentication against an LDAP directory. Kept as a distinct feature (rather than folded into `auth-proxy`) because it pulls the `ldap3` client and its rustls TLS stack. Off by default even when `auth-proxy` is enabled; excluded from `msrv-features`.

### `query-rewriting` -- Query Rewriting

**Modules:** `src/rewriter/`

Rule-based SQL transformation engine. Rules are evaluated in order and can:

- Add schema prefixes to unqualified table names.
- Replace deprecated syntax with modern equivalents.
- Inject query hints for routing or caching.
- Redirect queries to different tables or schemas.

### `wasm-plugins` -- WASM Plugin System

**Modules:** `src/plugins/` · **Pulls:** `wasmtime`

Sandboxed WebAssembly plugin runtime:

- **Hooks:** query-start, query-complete, connection, and error events.
- **Hot-reload:** Plugins can be updated without restarting the proxy.
- **Sandbox:** Each plugin runs in an isolated WASM sandbox with configurable memory limits and an execution timeout.
- **Host functions:** Plugins can call back into the proxy to read metrics, log messages, and modify query routing.

Excluded from `msrv-features` because `wasmtime` significantly slows the MSRV compile.

### `graphql-gateway` -- GraphQL Gateway

**Modules:** `src/graphql/` (`src/graphql_gateway.rs`)

GraphQL endpoint (separate HTTP listener):

- **Schema introspection:** Reflects the database schema into a GraphQL schema.
- **Query translation:** Converts GraphQL queries into SQL.
- **Validation:** Validates GraphQL queries against the reflected schema before execution.

Tables are exposed via `[[graphql_gateway.tables]]` in `proxy.toml`.

### `schema-routing` -- Schema-Aware Routing

**Modules:** `src/schema_routing/`

Routes queries based on table-level metadata:

- **Data temperature:** Classify tables as hot, warm, or cold and route accordingly.
- **Workload type:** OLTP tables to low-latency nodes, analytics tables to read replicas.
- **Admin API:** Runtime management of schema routing rules.

### `distribcache` -- Distributed Cache (library module)

**Modules:** `src/distribcache/`

> **Caveat (verify before relying on it):** `distribcache` is a **library-only module**. It is compiled behind `#[cfg(feature = "distribcache")]` in `src/lib.rs` but is **not** referenced anywhere outside its own module (grep for `distribcache::` outside `src/distribcache/` returns nothing), and there is **no `[distribcache]` section in `ProxyConfig`** -- `proxy.toml` cannot configure or activate it. Enabling the feature makes the module available to code linking the crate as a library; it does **not** turn on a turnkey distributed proxy cache on the data path. For an operator-facing result cache, use `query-cache` (and `edge-proxy` for cross-region invalidation).

The module itself implements multi-tier intelligent caching (L1 in-process / L2 shared / L3 external Redis-compatible), workload classification, an access heatmap, and AI-specific strategies (embedding prefetch, conversation-context caching).

### `anomaly-detection` -- Anomaly Detection

**Modules:** `src/anomaly/` (runs on hardcoded `AnomalyConfig::default()` -- there is no `proxy.toml` anomaly section)

In-process, sliding-window heuristics with no external data store:

- **Rate spikes:** z-score-based traffic-spike detection.
- **Credential stuffing:** auth-burst / failed-login clustering.
- **SQL injection:** pattern heuristics over incoming SQL.
- **Novel query shapes:** flags query fingerprints not seen before.

Results are surfaced at the Admin API `/anomalies` endpoint.

### `edge-proxy` -- Edge / Geo Proxy

**Pulls:** `lru`

Cache-first edge/geo mode: home-authoritative clock, last-write-wins TTL coherence, pull-on-miss with server-pushed (SSE) invalidation over a PG-wire data plane. Edges register with, subscribe to, and receive invalidation from the home proxy via the Admin API `/api/edge` routes (`/register`, `/subscribe`, `/invalidate`) and the `[edge]` `proxy.toml` section.

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
cargo build --release --features "pool-modes,ha-tr,postgres-topology,query-cache,routing-hints,lag-routing,query-analytics,rate-limiting,circuit-breaker,multi-tenancy,auth-proxy,ldap-auth,query-rewriting,anomaly-detection"
```

### Full-Featured Proxy

Every proxy module enabled. Add a topology provider explicitly (the bundle omits it).

```bash
cargo build --release --features "all-features,postgres-topology"
```

For the HeliosDB-Lite workspace build, swap the topology provider:

```bash
cargo build --release --features "all-features,heliosdb-topology"
```

> `observability` may be appended, but note it only compiles the `prometheus`/`opentelemetry` deps and wires no code (see the Observability caveat). The `/metrics` endpoints work without it.

### MSRV Verification

Matches the CI MSRV gate (Rust 1.86). Same as `all-features` minus `wasm-plugins` and `ldap-auth`.

```bash
cargo check --locked --features msrv-features
```

---

## See Also

- [Architecture](architecture.md) -- Module map and data-flow pipeline
- [Configuration Reference](configuration.md) -- Every configuration key documented
- [Deployment Guides](deployment/) -- Standalone, Docker, and Kubernetes deployment
</content>
</invoke>
