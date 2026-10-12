# HeliosProxy

[![CI](https://github.com/HeliosDatabase/HeliosDB-Proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/HeliosDatabase/HeliosDB-Proxy/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/heliosdb-proxy.svg)](https://crates.io/crates/heliosdb-proxy)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/HeliosDatabase/HeliosDB-Proxy?style=social)](https://github.com/HeliosDatabase/HeliosDB-Proxy)

**One Rust binary in front of PostgreSQL: connection pooling (a PgBouncer-style pooler), read/write splitting and health checks (the routing half of what you use HAProxy for), and client sessions that survive a primary change through Transaction Replay.**

HeliosProxy replaces HAProxy + PgBouncer in front of a Patroni-managed or RDS/Aurora cluster. It follows the primary that your HA manager promotes and keeps client sessions alive through supported failover behavior. It does **not** replace Patroni: it does not promote or fence databases by default (see [Status & guarantees](#status--guarantees)).

It works at the PostgreSQL wire protocol level, so it fronts PostgreSQL, HeliosDB, CockroachDB, YugabyteDB, and other PG-wire databases without application changes.

**[Quick start](#quick-start-60-seconds) · [Failover](#what-happens-on-failover) · [Comparison](#how-it-compares) · [Status & guarantees](#status--guarantees) · [Configuration](docs/configuration.md) · [All modules](#feature-modules)**

---

## The four core jobs

| Job | What you get | Where to read more |
|-----|--------------|--------------------|
| **Pool connections** | Session, transaction, and statement pooling with prepared-statement handling (`[pool_mode]`) | [docs/configuration.md](docs/configuration.md) |
| **Route queries** | Read/write split, least-connections / latency / round-robin balancing, optional lag-aware reads with read-your-writes | [docs/configuration.md](docs/configuration.md) |
| **Track the primary** | Health checks plus a topology provider (`postgres` via `pg_is_in_recovery()`, or `patroni` REST) that follows the writable primary your HA manager chose | [docs/topology-providers.md](docs/topology-providers.md) |
| **Keep sessions alive** | Transaction Replay (`tr_mode`): re-home the session, restore `SET` state, re-run interrupted reads, or (opt-in) replay an uncommitted transaction with result verification | [docs/transaction-replay.md](docs/transaction-replay.md) |

Everything else (query cache, auth proxy, rate limiting, WASM plugins, GraphQL/HTTP/MCP gateways, edge mode) is optional and feature-gated; see [Feature Modules](#feature-modules).

```
┌──────────────┐     ┌─────────────────────────────────────────────────┐     ┌──────────────┐
│              │     │                  HeliosProxy                    │     │   Primary    │
│  Application ├────►│                                                 ├────►│   (R/W)      │
│              │     │  ┌───────────┐ ┌──────────┐ ┌───────────────┐  │     └──────────────┘
│  (psql, any  │     │  │Connection │ │  Query   │ │   Primary     │  │     ┌──────────────┐
│   PG driver) │     │  │  Pooling  │ │ Routing  │ │ tracking + TR │  ├────►│  Standby     │
│              │     │  └───────────┘ └──────────┘ └───────────────┘  │     │  (Sync)      │
└──────────────┘     │  ┌───────────┐ ┌──────────┐ ┌───────────────┐  │     └──────────────┘
                     │  │  Caching  │ │   Auth   │ │  Rate Limit   │  │     ┌──────────────┐
                     │  │  (L1/L2)  │ │  Proxy   │ │  & Circuit    │  ├────►│  Standby     │
                     │  └───────────┘ └──────────┘ └───────────────┘  │     │  (Async)     │
                     └─────────────────────────────────────────────────┘     └──────────────┘
```

---

## Quick start (60 seconds)

You need Docker and `psql`, plus Rust for `cargo install` (or download a prebuilt binary from the [GitHub Releases](https://github.com/HeliosDatabase/HeliosDB-Proxy/releases) page instead).

```bash
# 1. A throwaway PostgreSQL to point at
docker run -d --name hp-demo-pg -e POSTGRES_PASSWORD=secret -p 5433:5432 postgres:18

# 2. Install and start the proxy (listens on :6432, admin API on 127.0.0.1:9090)
cargo install heliosdb-proxy --locked
heliosdb-proxy --primary 127.0.0.1:5433 --listen 127.0.0.1:6432

# 3. In another terminal, connect through the proxy like any PostgreSQL server
PGPASSWORD=secret psql -h 127.0.0.1 -p 6432 -U postgres -c "select now()"
curl -s http://127.0.0.1:9090/nodes     # backend health as the proxy sees it

# Clean up
docker rm -f hp-demo-pg
```

Add standbys with repeated `--standby host:port`, or use a config file (smallest valid file: [config/proxy.minimal.toml](config/proxy.minimal.toml); fuller examples under [Quick Start: config file](#connect-to-a-postgresql-cluster) below). A plain `cargo install` builds the default feature set (pooling and Transaction Replay); the `postgres` / `patroni` topology providers need `--features postgres-topology` (the prebuilt release binaries are built with `all-features` only and do not include them), and cache, auth, and plugins have their own flags (see [Feature Flags](#feature-flags)).

If HeliosProxy saves you a PgBouncer plus an HAProxy, a ⭐ helps others find it.

---

## What happens on failover

HeliosProxy never decides *which* database becomes primary. The sequence is:

1. Your HA manager (Patroni, pg_auto_failover, Stolon, RDS/Aurora, or an operator) promotes a standby and fences the old primary.
2. The topology provider (`provider = "postgres"` or `"patroni"`) observes the new writable primary and routes writes to it. Until a writable primary appears, writes wait up to `write_timeout_secs`.
3. With Transaction Replay on, each client session reconnects to the new primary and restores its `SET` state. Depending on `tr_mode`, an interrupted read is re-run or an uncommitted transaction is replayed and verified. A `COMMIT` whose outcome is unknown is never retried (SQLSTATE `08007`), and a replay whose results diverge from what the client saw is rolled back (`40001`).

Runnable and verified: [examples/patroni-failover](examples/patroni-failover/README.md) runs etcd, two Patroni members and the proxy in disposable containers. In the recorded run (2026-10-12, PostgreSQL 18.4), one client session with an open transaction survived both a planned Patroni switchover and a stopped leader container: the transaction was replayed on the new leader and committed, leaving each row exactly once, and a new session opened after the promotion reached the new leader ([evidence](examples/patroni-failover/evidence-2026-10-12-after-fix.log)). Patroni did the promotion; the proxy followed it.

Other material, which does not configure a promotion authority and is not a recording of end-to-end promotion: [examples/failover-demo](examples/failover-demo/README.md) (outage detection and recovery of the original primary with TR configured), [demos/chaos-failover](demos/chaos-failover/README.md) (random node kills under load), and [docs/topology-providers.md](docs/topology-providers.md) (Patroni setup and a loopback test that a leader change moves writes).

---

## How it compares

| | HeliosProxy | PgBouncer | HAProxy |
|---|---|---|---|
| Language | Rust | C | C |
| Pooling modes | session, transaction, statement | session, transaction, statement [1] | none: TCP/L4 proxy [5] |
| Prepared statements in transaction pooling | opt-in (`prepared_statement_mode = "track"`), default off | yes, on by default since 1.24 (`max_prepared_statements`, default 200) [2][3] | n/a |
| Read/write split | yes (query classification, hints) | no: a `host` list is round-robined and "all hosts must be available at all times" [2] | separate ports/backends chosen by health checks, for example Patroni's `/primary` and `/replica` REST endpoints [4][5] |
| Session survives a primary change | yes, via Transaction Replay (needs an externally promoted primary) | no: connections to the old server end and clients reconnect | no: a TCP proxy cannot re-home an established session |
| Follows the promoted primary | yes (`postgres` / `patroni` providers) | no: re-point config or DNS [2] | via health checks: `pgsql-check` only tests that a server answers [5]; Patroni's REST check tests the role [4] |
| Query result cache | yes, opt-in | no | no (HTTP cache only) |
| Extensibility | WASM plugins (`wasm-plugins` feature) | none documented | Lua scripting [5] |
| Promotes / fences databases | no by default (external HA manager) | no | no |
| Maturity and ecosystem | young | very mature | very mature |

Competitor cells were checked against the vendors' documentation on 2026-10-12. Cells without a numbered source (language, what happens to sessions on failover) follow from the architecture rather than a quotation. PgCat, Supavisor, and Odyssey are not in this table because their cells have not been verified.

Sources: [1] [PgBouncer features](https://www.pgbouncer.org/features.html) · [2] [PgBouncer configuration](https://www.pgbouncer.org/config.html) (`pool_mode`, `max_prepared_statements`, `host`, `load_balance_hosts`) · [3] [PgBouncer 1.24.0 release notes](https://www.pgbouncer.org/2025/01/pgbouncer-1.24.0) · [4] [Patroni REST API](https://patroni.readthedocs.io/en/latest/rest_api.html) · [5] [HAProxy 3.0 configuration manual](https://docs.haproxy.org/3.0/configuration.html) (`option pgsql-check`, `mode tcp`, Lua).

---

## Status & guarantees

**Verification status (2026-09-25, 2.0.0):** the [September 2026 audit](docs/internal/audit-2026-09/README.md)
found reproducible Transaction Replay safety defects; all of them are fixed as of
1.7.0, verified against live PostgreSQL, and replay now checks its own work — a
replayed statement whose results diverge from what the client already saw is rolled
back rather than continued. 1.9.0 added the recovery-grade transaction journal; 2.0.0
makes query-cache invalidation commit-aware on both wire protocols and every tier and
adds Patroni as a write authority with conflicting primaries failing closed. The query
cache stays opt-in: its identity does not yet include session state (`SET ROLE`,
`search_path`, tenant settings), see docs/configuration.md. The audit also records
advertised library capabilities that are not wired into the daemon. Consult it and the
[prioritized improvement backlog](docs/internal/audit-2026-09/IMPROVEMENTS.md) when
evaluating HA, replay, or distributed deployment guarantees.

Plainly:

- **Database promotion and fencing require an external HA manager.** The daemon tracks the primary; `[ha] auto_failover` is rejected at startup. The opt-in `[auto_promote]` section (off by default) can promote a standby from a single proxy, but it is not a quorum: it does not stop a second proxy or operator from also promoting, so it is no substitute for Patroni ([docs/configuration.md](docs/configuration.md)).
- **Transaction Replay is bounded.** Unknown `COMMIT` outcomes, snapshot-pinned (`SERIALIZABLE` / `REPEATABLE READ`) transactions, and diverging replays are never silently continued. Cursors, temp tables and `PREPARE`d state are not migrated. Details: [docs/transaction-replay.md](docs/transaction-replay.md).
- **No zero-RPO claim.** A healthy tracked primary says nothing about asynchronous replica lag; use synchronous replication if you need that.
- **Some modules are library-only** (for example Session Migration, Cursor Restore, Failover Controller primitives, `distribcache`); the module tables below mark which.

---

## Key Capabilities

### Protocol-Level Compatibility

The feature modules operate at the PostgreSQL wire protocol level. They inspect, route, cache, and transform queries without requiring changes to your application or database. Any client library that connects to PostgreSQL connects to HeliosProxy.

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

The four core jobs above are the product; everything below is optional. HeliosProxy features are grouped into a connection-routing tier and a programmable platform tier. Each module is independently enabled via Cargo feature flags, so deployments can range from a lightweight connection pooler to a full programmable data-plane.

## Connection-Routing Tier

### Connection Management

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Connection Pooling** | `pool-modes` | Session, Transaction, and Statement pooling with automatic lease management, prepared statement forwarding, and configurable reset sequences. `[pool_mode].skip_clean_reset` opts into conditional connection reset (the G2c throughput win in the benchmarks below) |
| **Load Balancer** | *(core)* | Round-robin, least-connections, and latency-based routing with automatic read/write splitting across primary and standby nodes |
| **Health Checker** | *(core)* | Continuous node health monitoring with configurable check intervals, failure thresholds, and custom health-check queries |
| **Pipeline** | *(core)* | PostgreSQL extended query protocol pipelining — batches Parse/Bind/Execute messages for reduced round trips |
| **Batch Operations** | *(core)* | Automatic INSERT coalescing — groups individual INSERT statements into multi-row batches for higher throughput |

### High Availability

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Failover Controller** | *(library API)* | Candidate ranking and promotion primitives for external orchestration; the daemon tracks an externally promoted primary |
| **Transaction Replay (TR)** | *(default build)* | `tr_mode` keeps client sessions alive across a backend failure: `session` re-homes the connection to the new primary and restores `SET` state, `select` transparently re-runs interrupted reads, and opt-in `transaction` replays the uncommitted transaction on the new primary and continues. A `COMMIT` with unknown outcome is never retried (client gets SQLSTATE 08007), a replay whose results diverge from what the client already saw is rolled back (40001), and a transaction pinned to a `SERIALIZABLE`/`REPEATABLE READ` snapshot is never replayed. Failing over onto password-protected backends requires the proxy to be the auth boundary (`[auth] mode = "scram"`). `tr_enabled = false` turns TR off (journaling, in-session recovery, `/api/replay`) |
| **Session Migration** | *(default build; library)* | Captures and restores full session state (SET parameters, prepared statements, advisory locks) when moving connections between nodes — not wired into the recovery path |
| **Cursor Restore** | *(default build; library)* | Preserves open cursor positions across failover — not wired into the recovery path |
| **Switchover Buffer** | *(core)* | Buffers incoming queries during planned switchover, drains them to the new primary once promotion completes |
| **Primary Tracker** | *(core)* | Pluggable topology discovery — tracks the current primary via `pg_is_in_recovery()` polling (PostgreSQL), HeliosDB topology events, or manual API calls |
| **Transaction Journal** | *(default build)* | Write-ahead journal for in-flight transactions with statement-level granularity, parameter capture, and configurable retention |

### Query Intelligence

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Query Cache** | `query-cache` | Three-tier result cache (L1 hot / L2 warm / L3 semantic) with TTL-based expiration, pattern-based invalidation, and normalized query fingerprinting. (`distribcache` is a separate, experimental in-tree library module for distributed cache tiers — it is not wired into this proxy cache path; see the feature-flags table) |
| **Query Routing** | `routing-hints` | Hint-based routing via SQL comments (`/*helios:route=primary*/`) and automatic classification of read vs. write queries |
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
| **MCP Agent Gateway** | *(config-gated, `[mcp]`)* | JSON-RPC 2.0 over HTTP for AI agents — `query`, `list_tables`, `explain` tools. Optional bearer-token auth via `[mcp].auth_token` |

## Platform Tier

The platform tier builds on the connection-routing core with a hardened WASM plugin host, an operations and observability surface, first-party plugins, and infrastructure-as-code companion projects.

### Detection & Edge

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| **Anomaly Detection** | `anomaly-detection` | In-process sliding-window detector: rate spikes (z-score vs. rolling EWMA), credential-stuffing bursts, six classes of SQL-injection patterns, and novel query shapes. Events stream over `GET /anomalies` — no external SIEM required |
| **Edge Mode** | `edge-proxy` | Cache-first geo/edge proxy. Each edge terminates reads against a local LRU+TTL+version cache; the home proxy broadcasts table-scoped invalidations on writes. Last-write-wins, no consensus |

#### Edge mode in action

A two-region edge cache in front of a **HeliosDB-Nano** origin: reads are answered locally at the edge, and a write through the home is pushed to every edge over SSE so the caches stay last-write-wins coherent — no TTL guesswork, no polling. The same proxy fronts any PostgreSQL-compatible database. **[▶ Watch on asciinema](https://asciinema.org/a/mmCddtdB0FSkTIIM)**

<p align="center">
  <a href="https://asciinema.org/a/mmCddtdB0FSkTIIM"><img src="docs/edge-proxy-demo.gif" alt="HeliosDB edge-proxy — coherent multi-region cache demo" width="820"></a>
</p>

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
| **Admin REST** | *(core)* | Endpoints surfacing platform capabilities: `/topology`, `/plugins`, `/admin/kv/*` (plugin runtime KV), `/anomalies`, `/api/edge*`, `/api/chaos`, `/api/shadow`, `/api/replay`. Operator and UI consume the same JSON |

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
| **Kubernetes Operator** | CRDs for `HeliosProxy`, `PoolProfile`, `RoutingRule`, `AuditPolicy`, `TenantQuota`; reconciler renders ConfigMap + Deployment + Service and polls `/topology` for status. Available on request from [sales@heliosdb.com](mailto:sales@heliosdb.com); to deploy on Kubernetes without it, see the [Kubernetes deployment guide](https://heliosdb.com/docs/proxy/deployment/kubernetes/) |
| **Terraform Provider** | Five resources mirroring the operator CRDs (`heliosproxy_instance`, `_pool_profile`, `_routing_rule`, `_audit_policy`, `_tenant_quota`) |
| **Pulumi Provider** | Wraps the Terraform provider via `pulumi-terraform-bridge` — same five resources in TypeScript / Python / Go / .NET |

---

## Ecosystem

HeliosDB is maintained under the **HeliosDatabase** organization. The database
editions, SDKs, proxy data-plane, and MCP knowledge tooling are designed to work
together across the same family.

- **[HeliosDatabase/HeliosDB-SDKs](https://github.com/HeliosDatabase/HeliosDB-SDKs)** — Official client SDKs (Python, TypeScript, Rust, Go) + integrations (VS Code, n8n, Zapier, Make, Retool, AutoGen) + cross-platform CLI. Apache 2.0.
- **[HeliosDatabase/Any2HeliosDB](https://github.com/HeliosDatabase/Any2HeliosDB)** — Apache-2.0 `a2h` migration toolkit for moving Oracle, MySQL, PostgreSQL, and SQL Server into HeliosDB Nano/Lite/Full or stock PostgreSQL, with wizard setup, resumable loads, validation, CDC, and MCP support.
- **[HeliosDatabase/HeliosDB-Nano](https://github.com/HeliosDatabase/HeliosDB-Nano)** — Single-binary embedded database with PostgreSQL/MySQL wire compatibility, HNSW vector search, branching, time-travel, and MCP endpoint support. Apache 2.0.
- **[HeliosDB Lite](https://www.heliosdb.com/lite.html)** — Production self-hosted database with HeliosProxy + HeliosCore baked in. SSPL-1.0; source is not publicly released.
- **[HeliosDB Full](https://www.heliosdb.com/full.html)** — Distributed enterprise database with 14 native wire protocols. SSPL-1.0; source is not publicly released.
- **[HeliosDatabase/HeliosDB-Proxy](https://github.com/HeliosDatabase/HeliosDB-Proxy)** — Programmable Postgres data-plane (PgBouncer drop-in + WASM plugins + zero-downtime PG-12->17 upgrade). Apache 2.0.
- **[HeliosDatabase/HeliosDB-Proxy-Plugins](https://github.com/HeliosDatabase/HeliosDB-Proxy-Plugins)** — Plugin interfaces and examples for extending HeliosProxy. Apache 2.0. The Kubernetes Operator, Terraform provider, and Pulumi provider are not publicly released; they are available on request from [sales@heliosdb.com](mailto:sales@heliosdb.com). For Kubernetes, see the public [Kubernetes deployment guide](https://heliosdb.com/docs/proxy/deployment/kubernetes/).

**[HeliosDatabase/HeliosDB-CodeKB-MCP](https://github.com/HeliosDatabase/HeliosDB-CodeKB-MCP)** provides an MCP server that turns HeliosDB codebases and docs into queryable technical knowledge for Claude Code, Codex, and other MCP clients.

---

## Quick Start

### Connect to a PostgreSQL Cluster

```bash
heliosdb-proxy --config proxy.toml
```

```toml
# Top-level keys — there is no [proxy] table (ProxyConfig reads these at the
# document root; a wrapping section would be silently ignored).
#
# Values support environment-variable substitution: `${NAME}` expands to the
# env var (error if unset) and `${NAME:-default}` falls back to a literal
# default when unset — e.g. listen_address = "0.0.0.0:${PORT:-6432}".
listen_address = "0.0.0.0:6432"
# Admin API is loopback-only by default. To expose it off-loopback, set an
# admin_token (recommended) or admin_allow_insecure = true.
admin_address = "127.0.0.1:9090"

# Pooling mode lives in [pool_mode] (mode = session | transaction | statement),
# NOT [pool]. [pool] holds the size/timeout knobs.
[pool_mode]
mode = "transaction"

[pool]
min_connections = 5
max_connections = 100
idle_timeout_secs = 300

[load_balancer]
# Field is read_strategy: round_robin | weighted_round_robin |
# least_connections | latency_based | random.
read_strategy = "least_connections"
read_write_split = true

[health]
check_interval_secs = 5
check_query = "SELECT 1"
failure_threshold = 3

[[nodes]]
host = "pg-primary.internal"
port = 5432
role = "primary"

# Node fields: host, port, http_port, role, weight, enabled, name. There is no
# `sync` attribute — a standby is just role = "standby".
[[nodes]]
host = "pg-standby-1.internal"
port = 5432
role = "standby"

[[nodes]]
host = "pg-standby-2.internal"
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
| `ha-tr` | n/a | **Deprecated no-op.** Transaction Replay ships in the default build and is toggled by `tr_enabled` in `proxy.toml` — see the Transaction Replay row above |
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
| `distribcache` | No | Experimental in-tree library module (`src/distribcache`) for distributed cache tiers. Compiling it in only makes the module available to library consumers — it is **not** wired into the proxy request path and has no `proxy.toml` section |
| `anomaly-detection` | No | In-process anomaly detection (rate spikes, credential stuffing, SQLi heuristics, novel queries) |
| `edge-proxy` | No | Cache-first edge / geo proxy mode with last-write-wins TTL coherence |
| `postgres-topology` | No | PostgreSQL primary discovery via `pg_is_in_recovery()` |
| `heliosdb-topology` | No | HeliosDB native topology integration |
| `observability` | No | Reserved no-op. It does not pull any dependency or wire any metrics/tracing; `/metrics` and `/metrics/prometheus` are always available regardless of this flag |
| `ldap-auth` | No | LDAP directory authentication (search + bind) inside `auth-proxy`; pulls the `ldap3` client. Enable in addition to `auth-proxy` for directory-backed auth |
| `all-features` | No | Enables all proxy features (choose a topology provider separately) |
| `msrv-features` | No | MSRV verification bundle — `all-features` minus `wasm-plugins` and `ldap-auth` so the Rust 1.86 `cargo check` step compiles in reasonable time |

### Build Examples

```bash
# Default — connection pooling only
cargo build --release

# Production — all features with PostgreSQL topology
cargo build --release --features "all-features,postgres-topology"

# Lightweight — pooling only (Transaction Replay is always in the default build)
cargo build --release --features "pool-modes"

# With HeliosDB integration
cargo build --release --features "all-features,heliosdb-topology"
```

---

## Topology Providers

HeliosProxy supports pluggable topology discovery for automatic primary tracking.
By default the daemon does not promote databases or fence failed primaries. A database HA
manager must do that first (or opt in to the single-proxy `[auto_promote]` described in
[docs/configuration.md](docs/configuration.md)); `[topology] provider = "postgres"` or `"patroni"` then tracks the
writable primary (requires `postgres-topology`). Static topology uses configured roles.
`[ha] auto_failover` is unsupported and rejected at startup. See
[deployment and promotion setup](docs/topology-providers.md).

For a minimal configuration, copy [config/proxy.minimal.toml](config/proxy.minimal.toml).
Omitted settings use documented defaults; each backend still requires `host` and `role`.
CLI-only deployments can set `--admin-token <TOKEN>` for remote admin access.

### PostgreSQL (`postgres-topology`)

Polls `pg_is_in_recovery()` on each configured node to detect the current primary. Compatible with any PostgreSQL HA solution:

- **Native streaming replication** — standard PostgreSQL primary/standby
- **Patroni** — automatic failover with etcd/ZooKeeper
- **pg_auto_failover** — Citus-managed automatic failover
- **Stolon** — cloud-native PostgreSQL HA
- **AWS RDS / Aurora** — managed PostgreSQL

### HeliosDB (`heliosdb-topology`)

The library exposes an event-driven adapter for an embedded HeliosDB topology manager.
This adapter is not selected by the standalone daemon configuration; for Nano deployments,
verify compatible role probes and external promotion before choosing a daemon provider.

### Manual / library API

Library users can update `PrimaryTracker` programmatically. The daemon admin API
has no generic promote/set-primary endpoint. With static topology, update the
configured node roles after database promotion and restart; for automatic tracking
use a supported authoritative topology provider.

---

## Admin API

```bash
# Cluster health
curl http://localhost:9090/health

# Kubernetes-style probe aliases (token-exempt, always routed):
#   /healthz -> liveness, /livez -> liveness, /readyz -> readiness (200/503)
curl http://localhost:9090/healthz
curl http://localhost:9090/readyz

# Node status and replication lag
curl http://localhost:9090/nodes

# Connection pool metrics
curl http://localhost:9090/metrics

# Force a node unhealthy to exercise failover (chaos/fault injection).
# Actions: force_unhealthy | restore | reset; the node field is target_node.
curl -X POST http://localhost:9090/api/chaos -d '{"action":"force_unhealthy","target_node":"pg-primary.internal:5432"}'

# Take a node out of rotation for maintenance
curl -X POST http://localhost:9090/nodes/pg-standby-async.internal:5432/disable
```

> Failover is automatic: a backend that fails a query is demoted within ~1
> query and the next query reroutes. There is no `/failover` endpoint. Graceful
> whole-proxy drain is triggered by `SIGUSR2`, not an HTTP call.

### Platform endpoints

```bash
# Cluster topology for external controllers (currentPrimary, healthy/unhealthy nodes)
curl http://localhost:9090/topology

# Loaded WASM plugins with invocation + error counts
curl http://localhost:9090/plugins

# Recent anomaly events (rate spikes, credential bursts, SQLi patterns, novel queries)
curl http://localhost:9090/anomalies?limit=50

# Chaos engineering — fault injection to validate the failover path
curl -X POST http://localhost:9090/api/chaos -d '{"action":"force_unhealthy","target_node":"pg-primary.internal:5432"}'

# Shadow execution — run a query against source + shadow backends and diff results.
# Body requires sql plus source_host/source_port and shadow_host/shadow_port.
curl -X POST http://localhost:9090/api/shadow \
  -d '{"sql":"SELECT count(*) FROM orders","source_host":"pg-primary.internal","source_port":5432,"shadow_host":"pg-new.internal","shadow_port":5432}'

# Time-travel replay — replay a journal window against a target backend.
# Requires an RFC-3339 window (from/to) and the target host+port; credentials
# are optional (falls back to the startup template when omitted).
curl -X POST http://localhost:9090/api/replay \
  -d '{"from":"2026-07-09T00:00:00Z","to":"2026-07-09T01:00:00Z","target_host":"staging.internal","target_port":5432}'

# Edge mode — cache stats, edge registration, invalidation broadcast
curl http://localhost:9090/api/edge

# Edge invalidation stream — edges subscribe here for a live SSE push of
# table-scoped invalidations from the home proxy
curl -N "http://localhost:9090/api/edge/subscribe?edge_id=e1&region=us-east"
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
FROM rust:1.86 AS builder
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

A proxy sits in the data path, so on a raw single-query throughput contest a
direct connection wins — that is expected. HeliosProxy's value is what a direct
connection cannot do: multiplex many client connections onto a small, bounded
backend pool, serve repeated reads from cache, and fail over transparently.

Measured on this project's evidence host (PG 18.4, `pgbench -S`, see
[`docs/perf-2026-07/README.md`](docs/perf-2026-07/README.md) for full tables and
conditions):

| What the proxy buys you | Result |
|---|---|
| Backend-connection fan-in | 32 bursty clients served by ~35 backend connections (session/transaction pooling) |
| Transaction-pooling throughput after conditional reset (G2c) | +48% at 16 clients, +31% at 64 clients vs always-reset |
| Idle-relay Flush latency (G3b) | 202 ms → 0.8 ms |
| Cached read | sub-millisecond, no backend round-trip |
| Failover | automatic — a failing backend is demoted within ~1 query and the next query reroutes |

Raw scalability (direct vs proxy tps at 1/4/16/64 clients) is tabulated in the
perf report; the proxy trades some peak tps for connection efficiency and the
features above. Reproduce with `scripts/regress/bench-scalability.sh`.

---

## License

Apache-2.0 (Apache License, Version 2.0).
