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
| `ha-tr` | None | **Deprecated no-op.** Transaction Replay (in-session recovery, journal, `/api/replay`, `/api/shadow`) ships in the default build and is toggled by `tr_enabled` in `proxy.toml`. The feature name is kept so downstream mappings keep resolving. |
| `query-cache` | `lru`, `zstd`, `lz4_flex` | L1 hot / L2 warm / L3 semantic query result caching. `zstd` and `lz4_flex` back the opt-in L2 payload compression (`[cache] l2_compression`). |
| `routing-hints` | None | SQL comment-based query routing hints. |
| `lag-routing` | None | Replica lag-aware routing with read-your-writes consistency. |
| `rate-limiting` | None | Token bucket, opt-in sliding window, and per-bucket concurrency limiting. |
| `circuit-breaker` | None | Circuit breaker per backend node, with half-open recovery probes. |
| `query-analytics` | None | Query fingerprinting and per-fingerprint statistics (`/analytics`); N+1, intent and slow-query results are library-only. |
| `multi-tenancy` | None | Tenant identification, row-level tenant filter, per-tenant client, active-query, replay and journal budgets. |
| `auth-proxy` | None | `[auth] mode = "passthrough" \| "scram"` client authentication; the JWT, OAuth and API-key modules are a library API. |
| `ldap-auth` | `ldap3` | LDAP search + bind auth. Implies `auth-proxy`. Off by default even within `auth-proxy` because it pulls the `ldap3` client and its rustls stack. |
| `query-rewriting` | None | Rule-based SQL query transformation. |
| `wasm-plugins` | `wasmtime` | Sandboxed WASM plugin runtime with hot-reload. |
| `graphql-gateway` | None | GraphQL-to-SQL gateway over the tables listed in `[[graphql_gateway.tables]]`. |
| `schema-routing` | None | Sends analytical reads to `[schema_routing] analytics_node`. |
| `distribcache` | `zstd`, `lz4_flex` | DistribCache library module (see caveat under Feature Details). |
| `anomaly-detection` | None | In-process anomaly heuristics: rate spikes, credential stuffing, SQL-injection patterns, novel query shapes. |
| `edge-proxy` | `lru` | Edge / geo proxy mode: cache-first handling, last-write-wins TTL coherence, pull-on-miss + invalidation push. |

### Topology Providers

| Feature | Default | Description |
|---------|---------|-------------|
| `postgres-topology` | No | PostgreSQL primary discovery via `pg_is_in_recovery()` polling. Used throughout `src/primary_tracker.rs`. |
| `heliosdb-topology` | No | Bridge to the HeliosDB-Lite internal `TopologyManager` (event-driven). Compiles a provider module that is only usable when HeliosProxy is built as part of the HeliosDB-Lite workspace. |

Topology providers are independent of each other. Choose one based on your backend. If neither is enabled, the daemon uses static topology: the configured `role = "primary"` node that passes its health check (there is no admin endpoint to set the primary). The prebuilt release binaries and the Docker image are built with `all-features,postgres-topology` (since 3.0.0 for the binaries), so they include the `postgres` and `patroni` providers.

### Observability

| Feature | Default | Description |
|---------|---------|-------------|
| `observability` | No | Reserved **no-op**. Historically pulled `prometheus`/`opentelemetry` without wiring any code; those unused dependencies were removed (they carried the protobuf advisory). `/metrics` and `/metrics/prometheus` are always served by the admin API. |

> **Note:** `observability = []` compiles nothing extra and activates no exporter,
> tracer, or metrics wiring; grep still finds zero `#[cfg(feature = "observability")]`
> guards in `src/`. Prometheus-style metrics are served regardless of this flag by the
> Admin HTTP API at `GET /metrics` and `GET /metrics/prometheus` (`src/admin.rs`).
> Wiring a real observability stack is tracked as P-03 in the reliability backlog.

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

Includes prepared statement tracking, connection reset sequences, lease management, and pool hardening. Statement mode returns the connection after every statement, so no transaction block may stay open across statements: a lone `BEGIN` / `START TRANSACTION` is refused with `0A000`, and a request that leaves a block open ends its session (FATAL `0A000`), as in PgBouncer's statement mode. See `[pool_mode]` in [configuration.md](configuration.md#pool-mode-pool_mode).

### `ha-tr` -- deprecated no-op (Transaction Replay is in the default build)

**Modules (always compiled since the post-1.7.0 change):** `src/transaction_journal.rs`,
`src/failover_replay.rs`, `src/replay/`, `src/session_migrate.rs`, `src/cursor_restore.rs`,
`src/upgrade_orchestrator.rs`, `src/shadow_execute/`.

Transaction Replay is **on by default**; the `ha-tr` cargo feature gates nothing. The
runtime switch is `tr_enabled` in `proxy.toml` (`--tr=false` on the CLI). Setting it to
`false` stops write journaling, forces the effective `tr_mode` to `none`, and makes
`POST /api/replay` return `503`. `POST /api/shadow` stays available: it is an explicit
validation tool, not part of failover recovery.

In-session recovery is the same path the client always took: the proxy records the
statements, session state and response digests of an open transaction, and on a backend
fault re-homes the session to a new primary, restores its `SET` state and replays the
transaction under one `write_timeout_secs` deadline, verifying each replayed response
against what the client already saw. See [transaction-replay.md](transaction-replay.md).

The operator tooling built on the same journal:

1. A post-response journal of write SQL, bounded in memory and not persisted.
2. The operator-driven `POST /api/replay` endpoint — replay a time window of that journal
   against a target backend, for failover validation, staging hydration or forensics.
3. `POST /api/shadow` — dual-execute a query against two backends and diff the results.
4. The `FailoverController`/`PrimaryTracker`/`SwitchoverBuffer` library components for
   embedded use.

`session_migrate` and `cursor_restore` are library modules and are **not** on the recovery
path. Protocol-level prepared statements are re-prepared on the replacement backend, but a
session holding SQL `PREPARE`d statements, `WITH HOLD` cursors, temporary tables, `LISTEN`
registrations or session-level advisory locks is ended with `08006` at a failover instead
of continuing without them.

Recovery is transparent where it can be. Where it cannot be — an unknown `COMMIT` outcome,
a response already partly delivered, a snapshot-pinned transaction — the client gets a
clear SQLSTATE (`08007`, `40001`, `08006`) instead of a silently wrong result.

### `query-cache` -- Query Caching

**Modules:** `src/cache/`

Three-tier query result cache:

- **L1 (Hot):** In-process hash map with LRU eviction. Microsecond access.
- **L2 (Warm):** Larger shared cache with normalized query fingerprinting. Sub-millisecond access.
- **L3 (Semantic):** opt-in, through `[cache] semantic_enabled` and a per-statement `/*helios:cache=semantic*/` hint. It accepts only single-table SELECTs of one small grammar (an allow-list; anything else uses L1/L2) and, by default (`semantic_match = "verbatim"`), serves only the same statement text, hint comments included, which is sound on every backend. With `semantic_match = "canonical"` (PostgreSQL semantics only; it turns itself off once a backend reports a HeliosDB `server_version`) it serves canonical-form matches (hint comments, keyword case, the table alias and which alias a qualifier uses do not matter; whether a column is qualified does, since `t.f` may call a function `f(t)`, and so does the case of every name, which HeliosDB-Lite does not fold and HeliosDB-Nano reports as written in output labels; any other comment makes a statement ineligible; literals, operators, the relation, ordering as written and limits must be identical), plus optional column substitution by name similarity when `semantic_similarity_threshold` is below its 1.0 default (only column names are embedded, in the background). Neither mode needs an embedding service at threshold 1.0. No tier caches a read naming a system catalog (`pg_*`, `information_schema`, or any Unicode-escaped `U&"…"` identifier) or one whose comment holds `AS OF` / `VERSIONS BETWEEN`; on HeliosDB-Nano, list its live `helios_*` / `heliosdb_*` system views in `exclude_tables`. See `docs/configuration.md`, "Semantic tier (L3)".

Supports TTL-based expiration (per table with `[[cache.tables]] ttl_secs`), commit-aware table-generation invalidation on writes, table exclusions that also bind `[edge]` (`exclude_tables`), and cache control via SQL-comment hints (`cache=skip`, `cache_ttl`, `cache=semantic`). Tier sizes are `[cache] l1_max_entries`, `l1_max_bytes` and `l2_max_bytes`; `l2_compression = "lz4" | "zstd"` (off by default) stores L2 payloads compressed. The feature pulls `zstd` and `lz4_flex` for that codec (they used to be unconditional dependencies; a build without `query-cache` and `distribcache` no longer compiles them).

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

What the daemon enforces under `[rate_limit] enabled = true`, per bucket (`key_by` =
`user`, `client_ip`, `database` or `global`):

- **Token bucket:** `default_qps` sustained with a `default_burst` depth, enforced at the
  configured rate (before 3.0.0 a hidden 60 000-per-minute window capped it near 1 000/s).
- **Sliding window (opt-in):** at most `window_max_requests` requests within any
  `window_secs`.
- **Concurrency cap:** at most `max_concurrent` requests in flight (`53400` above it).

Every request costs 1. Idle buckets are dropped every `cleanup_interval_secs`. The
library's tenant-keyed limits and query cost estimator are not wired into the daemon
(planned for a later 3.x release).

### `circuit-breaker` -- Circuit Breaker

**Modules:** `src/circuit_breaker/`

Per-node circuit breaker with three states:

| State | Behavior |
|-------|----------|
| Closed | Normal operation. Backend faults are counted in a 30-second window. |
| Open | Entered after `failure_threshold` faults in the window. Requests to the node are fast-failed (`08006`, HTTP 503 on the gateways) and reads prefer other nodes. |
| Half-Open | After `open_secs`, requests are admitted as probes (at most two at once). `success_threshold` successes close the circuit; a failed probe opens it again. |

Each backend node has an independent breaker. Before 3.0.0 the daemon never moved an open
circuit to half-open, so it stayed open until a restart. The adaptive threshold is a
library option the daemon does not set (planned for a later 3.x release).

### `query-analytics` -- Query Analytics

**Modules:** `src/analytics/`

Real-time query analysis, exposed at `GET /analytics` (`/api/analytics`):

- **Fingerprinting:** Normalizes queries by replacing literals with placeholders, grouping identical query patterns.
- **Statistics:** Per-fingerprint calls, average and P99 latency, rows and errors, plus the number of slow queries.

The library also implements a slow-query log, N+1 detection and intent classification;
the daemon does not expose their results (planned for a later 3.x release).

### `multi-tenancy` -- Multi-Tenancy

**Modules:** `src/multi_tenancy/`

Tenant-aware proxy operation:

- **Identification:** Reads the tenant from a startup parameter (`identify_by`: `database`, `user`, `application_name` or another key); `<db>@<branch>` resolves to `<db>`.
- **Row isolation:** Adds a tenant filter to statements on `tenant_tables`, on both wire protocols and the gateways; refuses shapes it cannot rewrite (`42501`) and, by default, sessions with no identified tenant (`unknown_tenant = "refuse"`).
- **Budgets:** Per-tenant client caps, active-query caps (PG-wire and gateways), replay-byte and recovery-journal budgets; a SIGHUP can resize them.

Per-tenant connection pools and schema isolation are not built (planned for a later 3.x
release). Proxy-side filtering is not a hard isolation boundary: use PostgreSQL
row-level security for that.

### `auth-proxy` -- Authentication Proxy

**Modules:** `src/auth/`

The daemon authenticates clients with `[auth] mode = "passthrough"` (the backend checks the password) or `"scram"` (against `auth_file`), plus an Authenticate plugin hook. The module also contains JWT validation, OAuth 2.0 token flows, API-key validation and a role mapper as a **library API**: they cannot be selected in `proxy.toml` in 3.0.0 (planned for a later 3.x release).

> Note: the wired `proxy.toml` `[auth]` section currently exposes only `mode = "passthrough" | "scram"` plus `auth_file` (see `ProxyConfig` in `src/config.rs`). LDAP is a separate feature -- see `ldap-auth` below.

### `ldap-auth` -- LDAP Authentication

**Implies:** `auth-proxy` · **Pulls:** `ldap3`

Search + bind authentication against an LDAP directory, as a library API (not selectable in `proxy.toml` in 3.0.0). Kept as a distinct feature (rather than folded into `auth-proxy`) because it pulls the `ldap3` client and its rustls TLS stack. Off by default even when `auth-proxy` is enabled; excluded from `msrv-features`.

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

- **Hooks the proxy calls:** `pre_query` (block, rewrite or answer from a plugin cache),
  `route` (node, branch or block), `post_query` and `authenticate`, on simple queries
  and on every `Parse` (and later `Bind`) of the extended protocol. A plugin declaring
  any other hook, or missing a declared export, is refused at load.
- **Distribution:** bare `.wasm` modules (with `.sig`, `.yaml` / `.json` sidecars) or
  signed `.tar.gz` artefacts built with the `helios-plugin` CLI (`keygen`, `pack`,
  `sign`, `inspect`, `verify`, `install`, `list`, `new`); `[plugins] trust_root` and
  `require_signed_manifest` decide what loads.
- **Hot reload:** the plugin directory is reconciled at startup, on SIGHUP, on
  `POST /plugins/reload` and, with `hot_reload = true`, every `hot_reload_interval_ms`;
  a new version replaces the running one only once it has loaded.
- **Sandbox:** each call runs in an isolated store with `memory_limit_mb`, `timeout_ms`
  and optional fuel metering.
- **Host functions:** per-plugin KV (`kv_get` / `kv_set` / `kv_delete`) and
  `sha256_hex`.

The proxy and Wasmtime runtime build for the host target (for example,
`x86_64-unknown-linux-gnu`). Installing `wasm32-unknown-unknown` is unnecessary
when building or running the proxy, including `--features all-features`. That
Rust target is needed only when compiling Rust plugin source into a `.wasm`
artifact; loading a prebuilt plugin does not require it.

To verify actual native-host execution without a Rust WASM toolchain or external
plugin artifacts, run:

```bash
cargo test --features wasm-plugins plugins::runtime::tests::
```

These tests compile inline WAT fixtures and execute hooks through Wasmtime,
including response payloads, host KV imports and execution timeouts. The separate
`wasm_plugin_e2e` integration suite loads Rust-built artifacts from the sibling
plugin workspace and skips those cases when artifacts are absent. A diagnostic
string found by `strings` is not evidence of which runtime path was executed.

Excluded from `msrv-features` because `wasmtime` significantly slows the MSRV compile.

### `graphql-gateway` -- GraphQL Gateway

**Modules:** `src/graphql/` (`src/graphql_gateway.rs`)

GraphQL endpoint (separate HTTP listener) that converts GraphQL queries into SQL over
the tables exposed via `[[graphql_gateway.tables]]` in `proxy.toml` (columns typed as
text). Results are capped by `max_result_rows` / `max_result_bytes`. The library's schema
introspector is not wired into the daemon: there is no schema introspection and no result
cache (planned for a later 3.x release).

### `schema-routing` -- Schema-Aware Routing

**Modules:** `src/schema_routing/`

The daemon sends analytical reads to `[schema_routing] analytics_node` (a configured
`[[nodes]]` entry), statement by statement on both protocols; an extended cycle goes
there only when every statement of it is an analytical read. Table metadata discovery,
data temperature and workload classes are library features the daemon does not populate,
and there is no admin API for schema routing rules.

### `distribcache` -- Distributed Cache (library module)

**Modules:** `src/distribcache/`

> **Caveat (verify before relying on it):** `distribcache` is a **library-only module**. It is compiled behind `#[cfg(feature = "distribcache")]` in `src/lib.rs` but is **not** referenced anywhere outside its own module (grep for `distribcache::` outside `src/distribcache/` returns nothing), and there is **no `[distribcache]` section in `ProxyConfig`** -- `proxy.toml` cannot configure or activate it. Enabling the feature makes the module available to code linking the crate as a library; it does **not** turn on a turnkey distributed proxy cache on the data path. For an operator-facing result cache, use `query-cache` (and `edge-proxy` for cross-region invalidation).

The module itself implements multi-tier caching (L1 and L2 in-process maps; L3 a custom TCP peer mesh — not Redis and not Redis-compatible), workload classification, an access heatmap, and AI-specific caches (conversation context, tool results, RAG chunks). There is no embedding prefetch: its prefetcher works on query-fingerprint patterns. The daemon's semantic result tier is the query cache's L3, configured by `[cache] semantic_*` — not this module.

### `anomaly-detection` -- Anomaly Detection

**Modules:** `src/anomaly/`

Configure the detector through `[anomaly]` in `proxy.toml`: rate and authentication
windows, alert thresholds, event-buffer size, novel-query reporting, and the
fingerprint cap. An absent section uses the documented defaults.

In-process, sliding-window heuristics with no external data store:

- **Rate spikes:** z-score-based traffic-spike detection.
- **Credential stuffing:** auth-burst / failed-login clustering.
- **SQL injection:** pattern heuristics over incoming SQL.
- **Novel query shapes:** flags query fingerprints not seen before.

Results are surfaced at the Admin API `/anomalies` endpoint. The `stacked_queries`
heuristic exempts complete ordinary DML batches enclosed by `BEGIN` (or
`START TRANSACTION`) and `COMMIT`/`ROLLBACK`. Other injection patterns still run;
DDL/COPY batches, dollar-quoted strings, and ambiguous or incomplete transaction
boundaries retain the stacked-query heuristic. This detector reports suspicious patterns, not a SQL
security guarantee.

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

Included: all of the above plus the Transaction Replay journal and operator replay tooling, and automatic primary discovery via PostgreSQL polling. (In-session replay itself is core.)

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

> `observability` may be appended, but it is a no-op: it compiles nothing extra and
> wires no code (see the Observability note). The `/metrics` endpoints work without it.

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
