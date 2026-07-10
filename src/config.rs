//! Proxy Configuration
//!
//! Configuration management for HeliosDB Proxy.

use crate::{ProxyError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;

// =============================================================================
// POOL MODE TYPES
// =============================================================================

/// Connection pooling mode
///
/// Determines when connections are returned to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PoolingMode {
    /// Session mode: 1:1 client-to-backend mapping
    #[default]
    Session,
    /// Transaction mode: Return after COMMIT/ROLLBACK
    Transaction,
    /// Statement mode: Return after each statement
    Statement,
}

/// Prepared statement handling mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PreparedStatementMode {
    /// Disable prepared statements
    #[default]
    Disable,
    /// Track and recreate on new connections
    Track,
    /// Use protocol-level named statements
    Named,
}

/// Pool mode configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolModeConfig {
    /// Default pooling mode
    #[serde(default)]
    pub mode: PoolingMode,
    /// Maximum connections per node
    #[serde(default = "default_pool_mode_max_size")]
    pub max_pool_size: u32,
    /// Minimum idle connections
    #[serde(default = "default_pool_mode_min_idle")]
    pub min_idle: u32,
    /// Idle timeout (seconds)
    #[serde(default = "default_pool_mode_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// Max connection lifetime (seconds)
    #[serde(default = "default_pool_mode_max_lifetime")]
    pub max_lifetime_secs: u64,
    /// Acquire timeout (seconds)
    #[serde(default = "default_pool_mode_acquire_timeout")]
    pub acquire_timeout_secs: u64,
    /// Reset query to run when returning connection to pool
    #[serde(default = "default_reset_query")]
    pub reset_query: String,
    /// Prepared statement mode
    #[serde(default)]
    pub prepared_statement_mode: PreparedStatementMode,
    /// Conditional reset (Transaction/Statement pooling): when true, a
    /// connection that provably touched no session state (no `SET`/GUC, temp
    /// table, prepared statement, `LISTEN`, advisory lock, …) is parked WITHOUT
    /// running `reset_query`, saving a backend round-trip per clean transaction.
    /// Classification is conservative — anything not provably session-neutral
    /// still runs the full reset — so a misclassification only ever costs an
    /// unnecessary reset, never leaks state. Off by default; intended for
    /// autocommit / simple-protocol workloads. See the `stmt_leaves_session_state`
    /// classifier for the exact (documented) limitation around session-setting
    /// user functions.
    #[serde(default)]
    pub skip_clean_reset: bool,
}

fn default_pool_mode_max_size() -> u32 {
    100
}

fn default_pool_mode_min_idle() -> u32 {
    10
}

fn default_pool_mode_idle_timeout() -> u64 {
    600
}

fn default_pool_mode_max_lifetime() -> u64 {
    3600
}

fn default_pool_mode_acquire_timeout() -> u64 {
    5
}

fn default_reset_query() -> String {
    "DISCARD ALL".to_string()
}

impl Default for PoolModeConfig {
    fn default() -> Self {
        Self {
            mode: PoolingMode::default(),
            max_pool_size: default_pool_mode_max_size(),
            min_idle: default_pool_mode_min_idle(),
            idle_timeout_secs: default_pool_mode_idle_timeout(),
            max_lifetime_secs: default_pool_mode_max_lifetime(),
            acquire_timeout_secs: default_pool_mode_acquire_timeout(),
            reset_query: default_reset_query(),
            prepared_statement_mode: PreparedStatementMode::default(),
            skip_clean_reset: false,
        }
    }
}

impl PoolModeConfig {
    /// Create config for session mode
    pub fn session_mode() -> Self {
        Self {
            mode: PoolingMode::Session,
            prepared_statement_mode: PreparedStatementMode::Named,
            ..Default::default()
        }
    }

    /// Create config for transaction mode
    pub fn transaction_mode() -> Self {
        Self {
            mode: PoolingMode::Transaction,
            prepared_statement_mode: PreparedStatementMode::Track,
            ..Default::default()
        }
    }

    /// Create config for statement mode
    pub fn statement_mode() -> Self {
        Self {
            mode: PoolingMode::Statement,
            prepared_statement_mode: PreparedStatementMode::Disable,
            ..Default::default()
        }
    }

    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }

    /// Get max lifetime as Duration
    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime_secs)
    }

    /// Get acquire timeout as Duration
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_secs)
    }
}

// =============================================================================
// MAIN PROXY CONFIG
// =============================================================================

/// Proxy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Listen address for client connections
    pub listen_address: String,
    /// Admin API address
    pub admin_address: String,
    /// Bearer token required on admin API requests. When set, every admin
    /// endpoint except liveness probes (`/health*`, `/livez`, `/readyz`)
    /// requires `Authorization: Bearer <token>`. Absent (default) = open on
    /// loopback only — the proxy refuses to start with a non-loopback
    /// `admin_address` and no token (see `validate`), because the admin API runs
    /// privileged operations (arbitrary SQL, forced failover, cutover, branch
    /// DROP DATABASE, replay against arbitrary targets).
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Explicit opt-in to expose the admin API on a non-loopback address WITHOUT
    /// a token. Default `false` — leave it so unless you front the admin port
    /// with your own authenticating proxy/network policy.
    #[serde(default)]
    pub admin_allow_insecure: bool,
    /// Enable TR (Transaction Replay)
    pub tr_enabled: bool,
    /// TR mode
    pub tr_mode: TrMode,
    /// Connection pool configuration
    pub pool: PoolConfig,
    /// Pool mode configuration (Session/Transaction/Statement)
    #[serde(default)]
    pub pool_mode: PoolModeConfig,
    /// Load balancer configuration
    pub load_balancer: LoadBalancerConfig,
    /// Health check configuration
    pub health: HealthConfig,
    /// Backend nodes
    pub nodes: Vec<NodeConfig>,
    /// TLS configuration
    pub tls: Option<TlsConfig>,
    /// Write timeout during failover (seconds)
    /// When primary is unavailable, wait this long for a new primary before returning error
    #[serde(default = "default_write_timeout_secs")]
    pub write_timeout_secs: u64,
    /// Plugin system configuration. Only consumed when the `wasm-plugins`
    /// feature is enabled; on a feature-off build, values are parsed and
    /// ignored so existing configs don't break.
    #[serde(default)]
    pub plugins: PluginToml,
    /// pg_hba-style connection admission rules, evaluated in order before any
    /// backend connection is opened. Empty (the default) means admit all
    /// (current behaviour preserved).
    #[serde(default)]
    pub hba: Vec<HbaRule>,
    /// Client authentication mode. Absent/default = pass-through (the proxy
    /// relays the client's auth to the backend, current behaviour).
    #[serde(default)]
    pub auth: AuthConfig,
    /// MCP (Model Context Protocol) agent gateway. Disabled by default.
    #[serde(default)]
    pub mcp: McpConfig,
    /// Per-agent SQL contracts (scoped grants). Referenced by id from the
    /// MCP gateway (`[mcp] contract`). Empty by default.
    #[serde(default)]
    pub agent_contracts: Vec<crate::agent_contract::AgentContract>,
    /// HTTP SQL gateway (Neon-serverless-driver compatible). Disabled by
    /// default — lets edge/serverless clients run SQL over HTTP.
    #[serde(default)]
    pub http_gateway: HttpGatewayConfig,
    /// Continuous traffic mirroring to a secondary backend. Disabled by
    /// default — the on-ramp to a PG->Nano migration mirror.
    #[serde(default)]
    pub mirror: MirrorConfig,
    /// Edge / geo proxy mode (role=home caches reads + broadcasts
    /// invalidations to subscribed edges; role=edge serves reads from a
    /// local cache and forwards misses/writes to the home). Disabled by
    /// default. Parsed on every build so configs round-trip, but
    /// `enabled = true` requires the `edge-proxy` compile-time feature —
    /// `validate()` rejects it otherwise.
    #[serde(default)]
    pub edge: crate::edge::EdgeConfig,
    /// Instant branch databases. Disabled by default — provisions
    /// CREATE DATABASE ... TEMPLATE clones through the proxy.
    #[serde(default)]
    pub branch: BranchConfig,
    /// SQL-comment routing hints (`/*helios:route=primary*/`). Disabled by
    /// default — when enabled, the proxy parses hints from query SQL and
    /// applies them as a route override that wins over the default verb
    /// routing (but never over a plugin `Block`). Only consumed when the
    /// `routing-hints` feature is compiled in; parsed-and-ignored otherwise.
    #[serde(default)]
    pub routing_hints: RoutingHintsConfig,
    /// Multi-dimensional rate limiting (token bucket + concurrency). Disabled
    /// by default. Only enforced when the `rate-limiting` feature is compiled
    /// in; parsed-and-ignored otherwise.
    #[serde(default)]
    pub rate_limit: RateLimitToml,
    /// Per-node circuit breaker (trip failing backends out of rotation,
    /// fast-fail while open). Disabled by default. Only enforced when the
    /// `circuit-breaker` feature is compiled in.
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerToml,
    /// Query analytics (fingerprinting, per-query statistics, slow-query log,
    /// pattern detection). Disabled by default. Only active when the
    /// `query-analytics` feature is compiled in.
    #[serde(default)]
    pub analytics: AnalyticsToml,
    /// In-process anomaly detector tunables (rate-spike z-score, credential-
    /// stuffing burst window, novel-query fingerprint cap, event ring buffer).
    /// Parsed on every build so configs round-trip; only consumed when the
    /// `anomaly-detection` feature is compiled in. Defaults reproduce the
    /// detector's historical hardcoded behaviour exactly.
    #[serde(default)]
    pub anomaly: AnomalyToml,
    /// Replica-lag-aware routing + read-your-writes. Disabled by default. Only
    /// enforced when the `lag-routing` feature is compiled in.
    #[serde(default)]
    pub lag_routing: LagRoutingToml,
    /// Query-result cache (L1 hot / L2 warm). Disabled by default. Only active
    /// when the `query-cache` feature is compiled in.
    #[serde(default)]
    pub cache: CacheToml,
    /// SQL query rewriting (rules engine). Disabled by default. Only active
    /// when the `query-rewriting` feature is compiled in.
    #[serde(default)]
    pub query_rewrite: QueryRewriteToml,
    /// Multi-tenancy (per-tenant row isolation via injected predicates).
    /// Disabled by default. Only active when the `multi-tenancy` feature is
    /// compiled in.
    #[serde(default)]
    pub multi_tenancy: MultiTenancyToml,
    /// Schema/workload-aware routing (route OLAP queries to an analytics node).
    /// Disabled by default. Only active when the `schema-routing` feature is on.
    #[serde(default)]
    pub schema_routing: SchemaRoutingToml,
    /// GraphQL-to-SQL gateway (separate HTTP listener). Disabled by default.
    /// Only active when the `graphql-gateway` feature is compiled in.
    #[serde(default)]
    pub graphql_gateway: GraphqlGatewayConfig,
    /// Proxy-side unnamed-`Parse` promotion (Batch H). When a client re-sends an
    /// identical unnamed extended `Parse` (the dominant pgbench/ORM pattern),
    /// the proxy skips forwarding it to a backend that already holds that exact
    /// unnamed statement and synthesizes the `ParseComplete` locally — cutting
    /// the per-cycle re-`Parse` overhead. Default on; a kill-switch for drivers
    /// that somehow depend on the redundant round trip.
    #[serde(default = "default_true")]
    pub optimize_unnamed_parse: bool,
    /// How long a graceful binary-handoff drain (SIGUSR2) keeps serving
    /// in-flight connections before the old process exits (Batch H). After this
    /// many seconds, any still-open connections are dropped so the handoff
    /// completes in bounded time. Overridable at runtime via the
    /// `HELIOS_DRAIN_TIMEOUT_SECS` env var.
    #[serde(default = "default_drain_timeout_secs")]
    pub shutdown_drain_timeout_secs: u64,
    /// Operational safety limits and timeouts for the PG-wire data path
    /// (cancel-key map size, handshake/read/write deadlines, per-session
    /// prepared-statement and buffer caps, idle-pool ceiling + reaper cadence).
    /// Every key defaults to the value it had as a hardcoded constant, so a
    /// config without a `[limits]` block is byte-for-byte unchanged.
    #[serde(default)]
    pub limits: LimitsToml,
}

fn default_drain_timeout_secs() -> u64 {
    60
}

/// Branch-database configuration: the maintenance connection the proxy uses
/// to provision `CREATE DATABASE <branch> TEMPLATE <base>` clones.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_localhost")]
    pub backend_host: String,
    #[serde(default = "default_pg_port")]
    pub backend_port: u16,
    /// A role with CREATEDB privilege.
    #[serde(default = "default_pg_user")]
    pub admin_user: String,
    pub admin_password: Option<String>,
    /// Maintenance database to issue CREATE/DROP DATABASE against (not the
    /// branch itself). Defaults to "postgres".
    #[serde(default = "default_admin_db")]
    pub admin_database: String,
    /// Default template database to branch from when a request omits `base`.
    #[serde(default = "default_admin_db")]
    pub base_database: String,
}

impl Default for BranchConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend_host: default_localhost(),
            backend_port: default_pg_port(),
            admin_user: default_pg_user(),
            admin_password: None,
            admin_database: default_admin_db(),
            base_database: default_admin_db(),
        }
    }
}

fn default_admin_db() -> String {
    "postgres".to_string()
}

/// Traffic-mirror configuration: replay a sampled share of live (simple-query)
/// writes to a secondary backend, asynchronously and off the client hot path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Fraction of eligible statements to mirror, 0.0..=1.0.
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,
    /// Mirror only write/DDL statements (default). When false, all simple
    /// queries are mirrored.
    #[serde(default = "default_true_bool")]
    pub writes_only: bool,
    /// Bounded queue depth; when full, statements are dropped (and counted)
    /// rather than blocking the client path.
    #[serde(default = "default_mirror_queue")]
    pub queue_size: usize,
    #[serde(default = "default_localhost")]
    pub backend_host: String,
    #[serde(default = "default_pg_port")]
    pub backend_port: u16,
    #[serde(default = "default_pg_user")]
    pub backend_user: String,
    pub backend_password: Option<String>,
    pub backend_database: Option<String>,
    /// Source (primary) connection used by `POST /api/migration/snapshot` to
    /// read existing data when bootstrapping the secondary. Defaults mirror
    /// the listener-side backend; set explicitly for a snapshot.
    #[serde(default = "default_localhost")]
    pub source_host: String,
    #[serde(default = "default_pg_port")]
    pub source_port: u16,
    #[serde(default = "default_pg_user")]
    pub source_user: String,
    pub source_password: Option<String>,
    pub source_database: Option<String>,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sample_rate: 1.0,
            writes_only: true,
            queue_size: 10_000,
            backend_host: default_localhost(),
            backend_port: default_pg_port(),
            backend_user: default_pg_user(),
            backend_password: None,
            backend_database: None,
            source_host: default_localhost(),
            source_port: default_pg_port(),
            source_user: default_pg_user(),
            source_password: None,
            source_database: None,
        }
    }
}

fn default_sample_rate() -> f64 {
    1.0
}
fn default_mirror_queue() -> usize {
    10_000
}

/// HTTP SQL gateway configuration. A Neon-`@neondatabase/serverless`-style
/// `POST /sql` endpoint that runs one statement over the backend PG-wire
/// client and returns `{ command, rowCount, rows, fields }`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpGatewayConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_http_gw_listen")]
    pub listen_address: String,
    #[serde(default = "default_localhost")]
    pub backend_host: String,
    #[serde(default = "default_pg_port")]
    pub backend_port: u16,
    #[serde(default = "default_pg_user")]
    pub backend_user: String,
    pub backend_password: Option<String>,
    pub backend_database: Option<String>,
    /// Optional Bearer token required on requests.
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for HttpGatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_address: default_http_gw_listen(),
            backend_host: default_localhost(),
            backend_port: default_pg_port(),
            backend_user: default_pg_user(),
            backend_password: None,
            backend_database: None,
            auth_token: None,
        }
    }
}

fn default_http_gw_listen() -> String {
    "127.0.0.1:9093".to_string()
}

/// MCP agent-gateway configuration. When enabled, the proxy exposes a native
/// MCP server so AI agents call `query`/`list_tables`/`explain` tools instead
/// of opening raw SQL connections — each call gated by the gateway's policy
/// (read-only by default) and logged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpConfig {
    #[serde(default)]
    pub enabled: bool,
    /// HTTP listen address for the MCP JSON-RPC endpoint.
    #[serde(default = "default_mcp_listen")]
    pub listen_address: String,
    /// Backend the gateway runs tool SQL against.
    #[serde(default = "default_localhost")]
    pub backend_host: String,
    #[serde(default = "default_pg_port")]
    pub backend_port: u16,
    #[serde(default = "default_pg_user")]
    pub backend_user: String,
    pub backend_password: Option<String>,
    pub backend_database: Option<String>,
    /// When true (default), the gateway refuses write/DDL statements — agents
    /// get a read-only database surface.
    #[serde(default = "default_true_bool")]
    pub read_only: bool,
    /// Name of an `[[agent_contracts]]` entry to enforce on every tool call
    /// (scoped grants + repair hints). None = only the `read_only` guardrail.
    #[serde(default)]
    pub contract: Option<String>,
    /// Bearer token required on every MCP request. When set, a request without
    /// `Authorization: Bearer <token>` is rejected. Absent (default) = open, so
    /// set this for any non-loopback deployment — like the HTTP/GraphQL
    /// gateways, MCP exposes SQL and must not be anonymous off localhost.
    #[serde(default)]
    pub auth_token: Option<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_address: default_mcp_listen(),
            backend_host: default_localhost(),
            backend_port: default_pg_port(),
            backend_user: default_pg_user(),
            backend_password: None,
            backend_database: None,
            read_only: true,
            contract: None,
            auth_token: None,
        }
    }
}

fn default_mcp_listen() -> String {
    "127.0.0.1:9092".to_string()
}
fn default_localhost() -> String {
    "127.0.0.1".to_string()
}
fn default_pg_port() -> u16 {
    5432
}
fn default_pg_user() -> String {
    "postgres".to_string()
}
fn default_true_bool() -> bool {
    true
}

/// Client-side authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// `passthrough` (default) relays client auth to the backend.
    /// `scram` makes the proxy terminate SCRAM-SHA-256 itself against
    /// `auth_file`, becoming the auth boundary (foundation for pooling).
    #[serde(default)]
    pub mode: AuthMode,
    /// Path to a pgbouncer-style user list (`user:secret`, secret = plaintext
    /// or a `SCRAM-SHA-256$...` verifier). Required when `mode = "scram"`.
    #[serde(default)]
    pub auth_file: Option<String>,
}

/// Proxy client-authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// Relay the client's auth exchange straight to the backend.
    #[default]
    Passthrough,
    /// Terminate SCRAM-SHA-256 at the proxy against `auth_file`.
    Scram,
}

/// A single pg_hba-style admission rule. The first rule whose `user`,
/// `database`, and `address` all match the incoming connection decides the
/// outcome (`allow`/`reject`). If no rule matches, the connection is
/// admitted (rules are an explicit deny/allow list, not default-deny — add a
/// trailing `{ action = "reject", user = "all", database = "all", address =
/// "all" }` for default-deny).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HbaRule {
    /// "allow" or "reject".
    pub action: HbaAction,
    /// Matching PostgreSQL user, or "all".
    #[serde(default = "hba_all")]
    pub user: String,
    /// Matching database, or "all".
    #[serde(default = "hba_all")]
    pub database: String,
    /// Matching client address: "all", a bare IP, or a CIDR (e.g.
    /// "10.0.0.0/8", "::1/128").
    #[serde(default = "hba_all")]
    pub address: String,
}

fn hba_all() -> String {
    "all".to_string()
}

/// Admission action for an [`HbaRule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HbaAction {
    Allow,
    Reject,
}

fn default_write_timeout_secs() -> u64 {
    30 // 30 seconds default write timeout during failover
}

/// A table exposed by the GraphQL gateway, with its selectable columns.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GqlTableToml {
    pub name: String,
    pub columns: Vec<String>,
}

/// GraphQL-to-SQL gateway configuration. A separate HTTP listener; only active
/// when the `graphql-gateway` feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GraphqlGatewayConfig {
    /// Serve the GraphQL gateway. Default `false`.
    pub enabled: bool,
    /// HTTP listen address (e.g. `0.0.0.0:9091`).
    pub listen_address: String,
    /// Backend the generated SQL runs against.
    pub backend_host: String,
    pub backend_port: u16,
    pub backend_user: String,
    pub backend_password: Option<String>,
    pub backend_database: Option<String>,
    /// Optional Bearer token required on requests.
    pub auth_token: Option<String>,
    /// Tables exposed as GraphQL types.
    pub tables: Vec<GqlTableToml>,
}

impl Default for GraphqlGatewayConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_address: "0.0.0.0:9091".to_string(),
            backend_host: "127.0.0.1".to_string(),
            backend_port: 5432,
            backend_user: "postgres".to_string(),
            backend_password: None,
            backend_database: None,
            auth_token: None,
            tables: Vec::new(),
        }
    }
}

/// Schema/workload-aware routing configuration (always present). Only active
/// when the `schema-routing` feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchemaRoutingToml {
    /// Route analytical (OLAP) queries — aggregations, GROUP BY, window
    /// functions — to a dedicated node. Default `false`.
    pub enabled: bool,
    /// Name of the node analytical queries are routed to.
    pub analytics_node: String,
}

/// Multi-tenancy configuration (always present). Converted to a
/// `multi_tenancy::TenantManager` at startup; only active when the
/// `multi-tenancy` feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MultiTenancyToml {
    /// Enforce per-tenant row isolation. Default `false`.
    pub enabled: bool,
    /// Which connection attribute names the tenant: a startup parameter name
    /// (e.g. `application_name`, `user`) or the literal `database`.
    pub identify_by: String,
    /// The row-level tenant column injected into queries (e.g. `tenant_id`).
    pub tenant_column: String,
    /// Tables that are tenant-scoped (get the filter injected). Other tables
    /// pass through unchanged.
    pub tenant_tables: Vec<String>,
    /// Known tenant ids.
    pub tenants: Vec<String>,
}

impl Default for MultiTenancyToml {
    fn default() -> Self {
        Self {
            enabled: false,
            identify_by: "application_name".to_string(),
            tenant_column: "tenant_id".to_string(),
            tenant_tables: Vec::new(),
            tenants: Vec::new(),
        }
    }
}

/// A single SQL-rewrite rule in TOML form. Maps to a `rewriter::RewriteRule`:
/// `match_table`/`match_regex` choose which queries it applies to (default: all),
/// and the first set transformation field is applied.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RewriteRuleToml {
    /// Apply to queries referencing this table.
    pub match_table: Option<String>,
    /// Apply to queries matching this regex.
    pub match_regex: Option<String>,
    /// Rewrite `match_table` -> this table name.
    pub replace_table_with: Option<String>,
    /// Append `AND <expr>` to the query's WHERE clause.
    pub append_where: Option<String>,
    /// Add a `LIMIT n` to an unbounded query.
    pub add_limit: Option<u32>,
}

/// SQL query-rewriting configuration (always present). Converted to a
/// `rewriter::QueryRewriter` at startup; only active when the `query-rewriting`
/// feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryRewriteToml {
    /// Rewrite query SQL on the path per the rules below. Default `false`.
    pub enabled: bool,
    /// Ordered rewrite rules.
    pub rules: Vec<RewriteRuleToml>,
}

/// Query-result cache configuration (TOML-friendly, always present). Converted
/// to `crate::cache::CacheConfig` at startup and only active when the
/// `query-cache` feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheToml {
    /// Serve read SELECT results from an in-process L1/L2 cache. Default `false`.
    pub enabled: bool,
    /// Time-to-live for cached results, seconds.
    pub ttl_secs: u64,
    /// Maximum single result size to cache, bytes (larger results bypass).
    pub max_result_bytes: usize,
}

impl Default for CacheToml {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl_secs: 300,
            max_result_bytes: 1024 * 1024,
        }
    }
}

// =============================================================================
// OPERATIONAL LIMITS & TIMEOUTS (session/protocol safety bounds)
// =============================================================================

/// Operational safety limits and timeouts for the PG-wire data path. Every
/// value here was previously a hardcoded `const` in `src/server.rs`; exposing
/// them as a `[limits]` section makes each one tunable via `proxy.toml` without
/// a recompile.
///
/// Defaults are byte-for-byte the prior compiled-in constants, so a config
/// without a `[limits]` block (or one that omits any individual key) behaves
/// exactly as before. Every timeout is expressed in whole seconds and every
/// count/byte cap as a plain integer; all MUST be > 0. A `0` here would disable
/// a safety bound rather than mean "unbounded" in any useful way, so
/// [`ProxyConfig::validate`] rejects it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsToml {
    /// Capacity of the query-cancellation key map (`BackendKeyData` → backend
    /// address). At capacity the oldest entries are FIFO-evicted; a dropped
    /// stale entry only means one best-effort `CancelRequest` is not forwarded.
    /// Prior constant `MAX_CANCEL_KEYS`.
    #[serde(default = "default_max_cancel_keys")]
    pub max_cancel_keys: usize,
    /// Deadline (seconds) for the pre-auth startup exchange (client TLS
    /// negotiation + PostgreSQL startup/authentication). Bounds slow-loris
    /// connections that stall mid-handshake; the query loop itself is not
    /// bounded. Prior constant `STARTUP_TIMEOUT`.
    #[serde(default = "default_startup_timeout_secs")]
    pub startup_timeout_secs: u64,
    /// Timeout (seconds) for a single backend write on the forward path — a
    /// blackholed or hung backend must never pin a client task indefinitely.
    /// Prior constant `BACKEND_WRITE_TIMEOUT`.
    #[serde(default = "default_backend_write_timeout_secs")]
    pub backend_write_timeout_secs: u64,
    /// Timeout (seconds) for a single backend read on the relay path — a backend
    /// that accepts the query but then emits no bytes must not pin a client task
    /// forever. The paired counterpart to `backend_write_timeout_secs`; a
    /// slow-but-healthy backend read (large sort / lock wait) is not itself
    /// treated as a fault (see `is_backend_fault`).
    #[serde(default = "default_backend_read_timeout_secs")]
    pub backend_read_timeout_secs: u64,
    /// Timeout (seconds) for a single client write — a wedged or very slow
    /// client must not pin a proxy task (and the backend connection it holds)
    /// forever. Prior constant `CLIENT_WRITE_TIMEOUT`.
    #[serde(default = "default_client_write_timeout_secs")]
    pub client_write_timeout_secs: u64,
    /// Timeout (seconds) for the out-of-band re-prepare exchange (write
    /// Parse+Flush, read ParseComplete) performed on a backend connection
    /// switch. Prior constant `REPREPARE_TIMEOUT`.
    #[serde(default = "default_reprepare_timeout_secs")]
    pub reprepare_timeout_secs: u64,
    /// Per-session cap on distinct named prepared statements — bounds the
    /// per-session statement registry against a client issuing unbounded
    /// `Parse`s. Prior constant `MAX_PREPARED_STATEMENTS`.
    #[serde(default = "default_max_prepared_statements")]
    pub max_prepared_statements: usize,
    /// Per-session cap on the aggregate bytes retained in the statement
    /// registry (each entry holds the full encoded `Parse`, so the count cap
    /// alone does not bound memory). Prior constant `MAX_PREPARED_BYTES`.
    #[serde(default = "default_max_prepared_bytes")]
    pub max_prepared_bytes: usize,
    /// Per-session cap on the un-flushed extended-protocol `pending` buffer: a
    /// client must reach a Sync/Flush boundary before this many bytes pile up.
    /// Prior constant `MAX_PENDING_BYTES`.
    #[serde(default = "default_max_pending_bytes")]
    pub max_pending_bytes: usize,
    /// Global ceiling on idle connections parked in the data-path backend pool
    /// across ALL `(node,user,db)` identities — bounds total file descriptors
    /// regardless of how many distinct identities connect. Only consumed when
    /// the `pool-modes` feature is compiled in; parsed-and-ignored otherwise.
    /// Prior constant `MAX_TOTAL_IDLE_BACKEND_CONNS`.
    #[serde(default = "default_max_total_idle_backend_conns")]
    pub max_total_idle_backend_conns: usize,
    /// How often (seconds) the idle-connection reaper runs. Prior constant
    /// `POOL_REAP_INTERVAL`.
    #[serde(default = "default_pool_reap_interval_secs")]
    pub pool_reap_interval_secs: u64,
}

fn default_max_cancel_keys() -> usize {
    100_000
}
fn default_startup_timeout_secs() -> u64 {
    30
}
fn default_backend_write_timeout_secs() -> u64 {
    30
}
fn default_backend_read_timeout_secs() -> u64 {
    30
}
fn default_client_write_timeout_secs() -> u64 {
    60
}
fn default_reprepare_timeout_secs() -> u64 {
    15
}
fn default_max_prepared_statements() -> usize {
    8192
}
fn default_max_prepared_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_max_pending_bytes() -> usize {
    64 * 1024 * 1024
}
fn default_max_total_idle_backend_conns() -> usize {
    8192
}
fn default_pool_reap_interval_secs() -> u64 {
    30
}

impl Default for LimitsToml {
    fn default() -> Self {
        Self {
            max_cancel_keys: default_max_cancel_keys(),
            startup_timeout_secs: default_startup_timeout_secs(),
            backend_write_timeout_secs: default_backend_write_timeout_secs(),
            backend_read_timeout_secs: default_backend_read_timeout_secs(),
            client_write_timeout_secs: default_client_write_timeout_secs(),
            reprepare_timeout_secs: default_reprepare_timeout_secs(),
            max_prepared_statements: default_max_prepared_statements(),
            max_prepared_bytes: default_max_prepared_bytes(),
            max_pending_bytes: default_max_pending_bytes(),
            max_total_idle_backend_conns: default_max_total_idle_backend_conns(),
            pool_reap_interval_secs: default_pool_reap_interval_secs(),
        }
    }
}

/// Replica-lag-aware routing + read-your-writes configuration (always present;
/// only enforced when the `lag-routing` feature is compiled in AND enabled).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LagRoutingToml {
    /// Enable lag-aware read routing + read-your-writes. Default `false`.
    pub enabled: bool,
    /// Reads issued within this many milliseconds after a write in the same
    /// session are pinned to the primary (read-your-writes), so the client
    /// observes its own writes despite replica lag. 0 disables the window.
    pub ryw_window_ms: u64,
    /// Exclude a standby from read routing when its measured replication lag
    /// exceeds this many bytes. 0 = no lag-based exclusion (default; the proxy
    /// does not yet populate per-node lag without a configured monitor).
    pub max_lag_bytes: u64,
}

impl Default for LagRoutingToml {
    fn default() -> Self {
        Self {
            enabled: false,
            ryw_window_ms: 500,
            max_lag_bytes: 0,
        }
    }
}

/// Query-analytics configuration (TOML-friendly, always present). Converted to
/// `crate::analytics::AnalyticsConfig` at startup and only active when the
/// `query-analytics` feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AnalyticsToml {
    /// Record per-query statistics, slow-query log, and pattern detection.
    /// Default `false`.
    pub enabled: bool,
    /// Queries slower than this (milliseconds) are added to the slow-query log.
    pub slow_query_ms: u64,
    /// Maximum distinct query fingerprints to track.
    pub max_fingerprints: u32,
}

impl Default for AnalyticsToml {
    fn default() -> Self {
        Self {
            enabled: false,
            slow_query_ms: 1000,
            max_fingerprints: 10000,
        }
    }
}

/// Anomaly-detector configuration (TOML-friendly, always present so configs
/// round-trip on any build). Converted to `crate::anomaly::AnomalyConfig` at
/// startup and only consumed when the `anomaly-detection` feature is compiled
/// in. Every default reproduces the detector's historical hardcoded
/// `AnomalyConfig::default()` (and the old `MAX_SEEN_FINGERPRINTS` const)
/// EXACTLY, so an absent `[anomaly]` section changes nothing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyToml {
    /// Rolling window for the per-tenant rate EWMA, seconds. Must be >= 1.
    #[serde(default = "default_anomaly_rate_window_secs")]
    pub rate_window_secs: u64,
    /// Minimum z-score before a rate spike fires. Must be finite and > 0.
    #[serde(default = "default_anomaly_spike_z_threshold")]
    pub spike_z_threshold: f64,
    /// Window for failed-auth (credential-stuffing) bursts, seconds. Must be
    /// >= 1.
    #[serde(default = "default_anomaly_auth_window_secs")]
    pub auth_window_secs: u64,
    /// Failures inside the auth window that escalate to Critical.
    #[serde(default = "default_anomaly_auth_critical_count")]
    pub auth_critical_count: u32,
    /// Failures inside the auth window that escalate to Warning. Must be
    /// <= `auth_critical_count`.
    #[serde(default = "default_anomaly_auth_warning_count")]
    pub auth_warning_count: u32,
    /// Maximum events kept in the in-memory ring buffer. Must be >= 1.
    #[serde(default = "default_anomaly_event_buffer_size")]
    pub event_buffer_size: usize,
    /// Emit novel-query fingerprints as informational events. Set `false` to
    /// suppress on high-churn workloads (e.g. ad-hoc analytics).
    #[serde(default = "default_anomaly_emit_novel_queries")]
    pub emit_novel_queries: bool,
    /// Upper bound on the novel-query fingerprint set before it is cleared
    /// (bounds memory on high-cardinality SQL). Must be >= 1.
    #[serde(default = "default_anomaly_max_seen_fingerprints")]
    pub max_seen_fingerprints: usize,
}

fn default_anomaly_rate_window_secs() -> u64 {
    60
}
fn default_anomaly_spike_z_threshold() -> f64 {
    3.0
}
fn default_anomaly_auth_window_secs() -> u64 {
    60
}
fn default_anomaly_auth_critical_count() -> u32 {
    10
}
fn default_anomaly_auth_warning_count() -> u32 {
    5
}
fn default_anomaly_event_buffer_size() -> usize {
    1024
}
fn default_anomaly_emit_novel_queries() -> bool {
    true
}
fn default_anomaly_max_seen_fingerprints() -> usize {
    100_000
}

impl Default for AnomalyToml {
    fn default() -> Self {
        Self {
            rate_window_secs: default_anomaly_rate_window_secs(),
            spike_z_threshold: default_anomaly_spike_z_threshold(),
            auth_window_secs: default_anomaly_auth_window_secs(),
            auth_critical_count: default_anomaly_auth_critical_count(),
            auth_warning_count: default_anomaly_auth_warning_count(),
            event_buffer_size: default_anomaly_event_buffer_size(),
            emit_novel_queries: default_anomaly_emit_novel_queries(),
            max_seen_fingerprints: default_anomaly_max_seen_fingerprints(),
        }
    }
}

#[cfg(feature = "anomaly-detection")]
impl AnomalyToml {
    /// Build the runtime detector config from the parsed `[anomaly]` section.
    /// Field-for-field; the defaults above guarantee this equals
    /// `crate::anomaly::AnomalyConfig::default()` when the section is absent.
    pub fn to_anomaly_config(&self) -> crate::anomaly::AnomalyConfig {
        crate::anomaly::AnomalyConfig {
            rate_window_secs: self.rate_window_secs,
            spike_z_threshold: self.spike_z_threshold,
            auth_window_secs: self.auth_window_secs,
            auth_critical_count: self.auth_critical_count,
            auth_warning_count: self.auth_warning_count,
            event_buffer_size: self.event_buffer_size,
            emit_novel_queries: self.emit_novel_queries,
            max_seen_fingerprints: self.max_seen_fingerprints,
        }
    }
}

/// Circuit-breaker configuration (TOML-friendly, always present). Converted to
/// `crate::circuit_breaker::ManagerConfig` at startup and only enforced when
/// the `circuit-breaker` feature is compiled in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CircuitBreakerToml {
    /// Trip backends out of rotation after repeated failures. Default `false`.
    pub enabled: bool,
    /// Consecutive failures (within the failure window) that open a node's
    /// circuit.
    pub failure_threshold: u32,
    /// How long a circuit stays open before a half-open probe is allowed.
    pub open_secs: u64,
    /// Successful probes required to close a half-open circuit.
    pub success_threshold: u32,
}

impl Default for CircuitBreakerToml {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: 5,
            open_secs: 10,
            success_threshold: 3,
        }
    }
}

/// How rate-limit buckets are keyed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitKeyBy {
    /// One bucket per authenticated user (startup `user` param).
    #[default]
    User,
    /// One bucket per client IP address.
    ClientIp,
    /// One bucket per target database.
    Database,
    /// A single global bucket for the whole proxy.
    Global,
}

/// Rate-limiting configuration (TOML-friendly, always present so configs
/// round-trip on any build). Converted to `crate::rate_limit::RateLimitConfig`
/// at startup and only enforced when the `rate-limiting` feature is compiled
/// in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitToml {
    /// Enforce rate limits. Default `false`.
    pub enabled: bool,
    /// Sustained queries per second per bucket.
    pub default_qps: u32,
    /// Burst capacity (token-bucket depth) per bucket.
    pub default_burst: u32,
    /// Max concurrent in-flight queries per bucket (0 = use the engine default).
    pub max_concurrent: u32,
    /// What each bucket is keyed on.
    pub key_by: RateLimitKeyBy,
}

impl Default for RateLimitToml {
    fn default() -> Self {
        Self {
            enabled: false,
            default_qps: 1000,
            default_burst: 2000,
            max_concurrent: 0,
            key_by: RateLimitKeyBy::User,
        }
    }
}

/// SQL-comment routing-hint configuration.
///
/// Always present on `ProxyConfig` so configs round-trip on any build, but the
/// hints are only parsed and honored when the `routing-hints` feature is
/// compiled in AND `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingHintsConfig {
    /// Parse and honor `/*helios:...*/` routing hints. Default `false`
    /// (preserves the pure verb-based routing behaviour).
    pub enabled: bool,
    /// Strip the hint comment from the SQL before forwarding to the backend.
    /// Default `true`. Hint comments are valid SQL comments, so leaving them
    /// in is harmless; stripping keeps backend query logs clean.
    pub strip_hints: bool,
}

impl Default for RoutingHintsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            strip_hints: true,
        }
    }
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0:5432".to_string(),
            // Loopback by default: the admin API is privileged and must not be
            // exposed to the network unless the operator opts in (with a token,
            // or admin_allow_insecure). A fresh install is safe.
            admin_address: "127.0.0.1:9090".to_string(),
            admin_token: None,
            admin_allow_insecure: false,
            tr_enabled: true,
            tr_mode: TrMode::Session,
            pool: PoolConfig::default(),
            pool_mode: PoolModeConfig::default(),
            load_balancer: LoadBalancerConfig::default(),
            health: HealthConfig::default(),
            nodes: Vec::new(),
            tls: None,
            write_timeout_secs: default_write_timeout_secs(),
            plugins: PluginToml::default(),
            hba: Vec::new(),
            auth: AuthConfig::default(),
            mcp: McpConfig::default(),
            agent_contracts: Vec::new(),
            http_gateway: HttpGatewayConfig::default(),
            mirror: MirrorConfig::default(),
            edge: crate::edge::EdgeConfig::default(),
            branch: BranchConfig::default(),
            routing_hints: RoutingHintsConfig::default(),
            rate_limit: RateLimitToml::default(),
            circuit_breaker: CircuitBreakerToml::default(),
            analytics: AnalyticsToml::default(),
            anomaly: AnomalyToml::default(),
            lag_routing: LagRoutingToml::default(),
            cache: CacheToml::default(),
            query_rewrite: QueryRewriteToml::default(),
            multi_tenancy: MultiTenancyToml::default(),
            schema_routing: SchemaRoutingToml::default(),
            graphql_gateway: GraphqlGatewayConfig::default(),
            optimize_unnamed_parse: true,
            shutdown_drain_timeout_secs: default_drain_timeout_secs(),
            limits: LimitsToml::default(),
        }
    }
}

// =============================================================================
// PLUGIN SYSTEM CONFIG (TOML-friendly shape)
// =============================================================================

/// Plugin-system configuration, in a TOML-friendly shape.
///
/// Always present on `ProxyConfig` so existing configs round-trip, but only
/// consumed when the `wasm-plugins` feature is enabled. When
/// `plugins.enabled` is `false` (the default), plugin loading is skipped
/// entirely and every plugin-hook call site becomes a zero-cost no-op.
///
/// Converted to `crate::plugins::PluginRuntimeConfig` at startup via a
/// feature-gated `From` impl in `src/plugins/config.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginToml {
    /// Enable the plugin subsystem. Defaults to `false` — plugins are
    /// strictly opt-in.
    #[serde(default)]
    pub enabled: bool,
    /// Directory to scan at startup for `.wasm` plugin files.
    #[serde(default = "default_plugin_dir")]
    pub plugin_dir: String,
    /// Watch `plugin_dir` for file changes and reload plugins hot.
    #[serde(default)]
    pub hot_reload: bool,
    /// Memory limit per plugin instance, in megabytes.
    #[serde(default = "default_plugin_memory_mb")]
    pub memory_limit_mb: usize,
    /// Execution timeout per hook call, in milliseconds.
    #[serde(default = "default_plugin_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum number of concurrently-loaded plugins.
    #[serde(default = "default_plugin_max")]
    pub max_plugins: usize,
    /// Enable per-call CPU-cycle (fuel) metering to bound plugin runtime.
    #[serde(default = "default_true")]
    pub fuel_metering: bool,
    /// Fuel units allowed per hook call when `fuel_metering = true`.
    #[serde(default = "default_plugin_fuel")]
    pub fuel_limit: u64,
    /// Optional Ed25519 trust-root directory. When set, every loaded
    /// .wasm requires a sidecar .sig that verifies against one of
    /// the *.pub files in this directory. When omitted, signatures
    /// are not checked (preserves the dev-loop ergonomic of dropping
    /// unsigned .wasm files in the plugin dir).
    #[serde(default)]
    pub trust_root: Option<String>,
    /// Max bytes for a single plugin-KV value (via `kv_set` or the
    /// `PUT /admin/kv/<plugin>/<key>` endpoint). `0` = unlimited.
    #[serde(default = "default_plugin_kv_max_value_bytes")]
    pub kv_max_value_bytes: usize,
    /// Max distinct keys per plugin KV namespace. `0` = unlimited.
    #[serde(default = "default_plugin_kv_max_keys")]
    pub kv_max_keys_per_plugin: usize,
    /// Max distinct plugin KV namespaces that may exist at once. Bounds
    /// how many `<plugin>` namespaces the `PUT /admin/kv/<plugin>/<key>`
    /// endpoint can bring into existence, so a token-holding caller
    /// cannot exhaust memory by writing to unboundedly-many namespace
    /// names. `0` = unlimited.
    #[serde(default = "default_plugin_kv_max_plugins")]
    pub kv_max_plugins: usize,
}

fn default_plugin_dir() -> String {
    "/etc/heliosproxy/plugins".to_string()
}
fn default_plugin_memory_mb() -> usize {
    64
}
fn default_plugin_timeout_ms() -> u64 {
    100
}
fn default_plugin_max() -> usize {
    20
}
fn default_true() -> bool {
    true
}
fn default_plugin_fuel() -> u64 {
    1_000_000
}
fn default_plugin_kv_max_value_bytes() -> usize {
    65536
}
fn default_plugin_kv_max_keys() -> usize {
    1024
}
fn default_plugin_kv_max_plugins() -> usize {
    256
}

impl Default for PluginToml {
    fn default() -> Self {
        Self {
            enabled: false,
            plugin_dir: default_plugin_dir(),
            hot_reload: false,
            memory_limit_mb: default_plugin_memory_mb(),
            timeout_ms: default_plugin_timeout_ms(),
            max_plugins: default_plugin_max(),
            fuel_metering: true,
            fuel_limit: default_plugin_fuel(),
            trust_root: None,
            kv_max_value_bytes: default_plugin_kv_max_value_bytes(),
            kv_max_keys_per_plugin: default_plugin_kv_max_keys(),
            kv_max_plugins: default_plugin_kv_max_plugins(),
        }
    }
}

// =============================================================================
// ENV-VAR SUBSTITUTION + UNKNOWN-KEY DETECTION (config-loader helpers)
// =============================================================================

/// Expand `${NAME}` and `${NAME:-default}` environment-variable references in a
/// raw config file, in place, BEFORE it is parsed as TOML.
///
/// * `${NAME}`          → the value of env var `NAME`; returns an `Err` naming
///   `NAME` if it is unset (fail-fast, 12-factor — the literal is never left
///   in the output).
/// * `${NAME:-default}` → env var `NAME` if set, otherwise the literal
///   `default` (which may be empty; the default text runs up to the first `}`).
///
/// `NAME` must match `[A-Za-z_][A-Za-z0-9_]*`. Substitution is IN PLACE, so an
/// unquoted `${POOL_MAX:-50}` becomes the bare token `50` (valid TOML) and a
/// quoted `"${X:-y}"` becomes `"y"`.
///
/// SECURITY: this is plain string substitution. It performs ONLY an env lookup
/// plus the `:-` default operator — it never spawns a shell or evaluates
/// anything. Trust boundary: substitution is textual and runs BEFORE the TOML
/// parse, so a substituted value (env var or `:-default`) that contains a quote
/// or newline can inject arbitrary TOML structure. Env vars and the config file
/// are operator-controlled, so this is acceptable; do NOT feed an
/// untrusted-party-controlled env var into a `${...}` reference.
///
/// Comment-awareness is intentionally LINE-LEVEL only: a line whose first
/// non-whitespace byte is `#` (a full-line TOML comment) is copied verbatim so
/// the many commented `${VAR}` examples in the shipped reference configs (some
/// with no `:-default`) do not trigger the unset-variable error. A trailing
/// `#` comment on a value line is NOT treated as a comment — a full
/// TOML-comment-aware tokenizer is overkill for the shipped configs and out of
/// scope here.
fn substitute_env(text: &str) -> Result<String> {
    // `split_inclusive` keeps the line terminator attached to each piece, so
    // reassembling preserves the original text (including a missing trailing
    // newline) exactly outside of the substituted spans.
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        if line.trim_start().starts_with('#') {
            // Full-line comment: copy verbatim, never substitute.
            out.push_str(line);
        } else {
            substitute_line(line, &mut out)?;
        }
    }
    Ok(out)
}

/// Expand every `${...}` reference in a single (non-comment) line into `out`.
fn substitute_line(line: &str, out: &mut String) -> Result<()> {
    let mut rest = line;
    while let Some(idx) = rest.find("${") {
        out.push_str(&rest[..idx]);
        let body = &rest[idx + 2..]; // text after the opening "${"
        match parse_placeholder(body)? {
            Some((value, consumed)) => {
                out.push_str(&value);
                rest = &body[consumed..];
            }
            None => {
                // Not a well-formed `${NAME...}`: emit the literal "${" and
                // keep scanning after it so a later valid placeholder on the
                // same line still expands.
                out.push_str("${");
                rest = body;
            }
        }
    }
    out.push_str(rest);
    Ok(())
}

/// Parse the body of a placeholder (the text AFTER the opening `${`).
///
/// On success returns `(replacement, consumed)` where `consumed` is the number
/// of bytes of `body` consumed INCLUDING the closing `}`. Returns `Ok(None)`
/// when `body` is not a well-formed placeholder (the caller leaves it literal).
/// Returns `Err` only for a well-formed `${NAME}` (no default) whose env var is
/// unset.
fn parse_placeholder(body: &str) -> Result<Option<(String, usize)>> {
    let bytes = body.as_bytes();
    // NAME = [A-Za-z_][A-Za-z0-9_]*
    let mut n = 0;
    while n < bytes.len() {
        let b = bytes[n];
        let valid = if n == 0 {
            b.is_ascii_alphabetic() || b == b'_'
        } else {
            b.is_ascii_alphanumeric() || b == b'_'
        };
        if valid {
            n += 1;
        } else {
            break;
        }
    }
    if n == 0 {
        return Ok(None); // no valid NAME after `${`
    }
    let name = &body[..n];
    let after = &body[n..];

    if after.starts_with('}') {
        // `${NAME}` — must be set, no fallback.
        match std::env::var(name) {
            Ok(v) => Ok(Some((v, n + 1))),
            Err(_) => Err(ProxyError::Config(format!(
                "config env-var substitution: `${{{name}}}` references environment \
                 variable `{name}`, which is not set (and no `:-default` fallback \
                 was given)"
            ))),
        }
    } else if let Some(after_op) = after.strip_prefix(":-") {
        // `${NAME:-default}` — default runs up to the first `}`.
        match after_op.find('}') {
            Some(end) => {
                let default = &after_op[..end];
                let value = std::env::var(name).unwrap_or_else(|_| default.to_string());
                // consumed = NAME(n) + ":-"(2) + default(end) + "}"(1)
                Ok(Some((value, n + 2 + end + 1)))
            }
            None => Ok(None), // unterminated placeholder: leave literal
        }
    } else {
        Ok(None) // NAME followed by an unexpected char: leave literal
    }
}

/// Top-level keys recognised by [`ProxyConfig`]. Keep in sync with the struct's
/// fields (there are no field-level `#[serde(rename)]`s, so these are the exact
/// Rust field names). Used ONLY to warn on unknown top-level sections/keys;
/// deserialization itself still silently ignores unknowns (there is no
/// `deny_unknown_fields`), so a stale entry here only mutes or duplicates a
/// warning — it can never reject a config. The
/// `test_known_top_level_keys_cover_struct_fields` drift guard fails CI if a
/// new non-optional field is added without updating this list.
const KNOWN_TOP_LEVEL_KEYS: &[&str] = &[
    "listen_address",
    "admin_address",
    "admin_token",
    "admin_allow_insecure",
    "tr_enabled",
    "tr_mode",
    "pool",
    "pool_mode",
    "load_balancer",
    "health",
    "nodes",
    "tls",
    "write_timeout_secs",
    "plugins",
    "hba",
    "auth",
    "mcp",
    "agent_contracts",
    "http_gateway",
    "mirror",
    "edge",
    "branch",
    "routing_hints",
    "rate_limit",
    "circuit_breaker",
    "analytics",
    "anomaly",
    "lag_routing",
    "cache",
    "query_rewrite",
    "multi_tenancy",
    "schema_routing",
    "graphql_gateway",
    "optimize_unnamed_parse",
    "shutdown_drain_timeout_secs",
    "limits",
];

/// Detect TOP-LEVEL TOML keys that are not fields of [`ProxyConfig`] (silent
/// doc drift / typos). Pure and testable: parses `text` as a TOML table and
/// diffs its top-level keys against [`KNOWN_TOP_LEVEL_KEYS`], returning the
/// unknowns sorted. Nested unknown keys (e.g. `[cache.l1]`) are intentionally
/// OUT OF SCOPE. If `text` does not parse as a TOML table this returns empty —
/// the caller's `toml::from_str` already reports genuine parse errors.
fn unknown_top_level_keys(text: &str) -> Vec<String> {
    let Ok(value) = toml::from_str::<toml::Value>(text) else {
        return Vec::new();
    };
    let Some(table) = value.as_table() else {
        return Vec::new();
    };
    let mut unknown: Vec<String> = table
        .keys()
        .filter(|k| !KNOWN_TOP_LEVEL_KEYS.contains(&k.as_str()))
        .cloned()
        .collect();
    unknown.sort();
    unknown
}

impl ProxyConfig {
    /// Get write timeout as Duration
    pub fn write_timeout(&self) -> Duration {
        Duration::from_secs(self.write_timeout_secs)
    }

    /// Load configuration from file
    pub fn from_file(path: &str) -> Result<Self> {
        let path = Path::new(path);

        if !path.exists() {
            return Err(ProxyError::Config(format!(
                "Configuration file not found: {}",
                path.display()
            )));
        }

        let raw = std::fs::read_to_string(path)
            .map_err(|e| ProxyError::Config(format!("Failed to read config: {}", e)))?;

        // Expand `${VAR}` / `${VAR:-default}` references before parsing — the
        // 12-factor substitution the shipped example configs advertise and use.
        // Fails fast (naming the variable) if a bare `${VAR}` has no value and
        // no `:-default` fallback.
        let contents = substitute_env(&raw)?;

        let config: Self = toml::from_str(&contents)
            .map_err(|e| ProxyError::Config(format!("Failed to parse config: {}", e)))?;

        // Surface unknown TOP-LEVEL sections/keys (e.g. documented-but-
        // unimplemented `[ha]`/`[logging]`/`[metrics]` blocks) as warnings so
        // silent doc drift becomes visible. We deliberately do NOT reject them:
        // there is no `deny_unknown_fields`, so deserialization already ignores
        // unknown fields and pre-existing configs with such sections keep
        // loading. Nested unknown keys are out of scope.
        for key in unknown_top_level_keys(&contents) {
            tracing::warn!(
                "unknown config section/key '{}' ignored (not part of ProxyConfig)",
                key
            );
        }

        config.validate()?;

        Ok(config)
    }

    /// Add a node from host:port string
    pub fn add_node(&mut self, host_port: &str, role: &str) -> Result<()> {
        let parts: Vec<&str> = host_port.rsplitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(ProxyError::Config(format!(
                "Invalid host:port format: {}",
                host_port
            )));
        }

        let port: u16 = parts[0]
            .parse()
            .map_err(|_| ProxyError::Config(format!("Invalid port: {}", parts[0])))?;

        let host = parts[1].to_string();

        let role = match role {
            "primary" => NodeRole::Primary,
            "standby" => NodeRole::Standby,
            "replica" => NodeRole::ReadReplica,
            _ => return Err(ProxyError::Config(format!("Unknown role: {}", role))),
        };

        self.nodes.push(NodeConfig {
            host,
            port,
            http_port: default_http_port(),
            role,
            weight: 100,
            enabled: true,
            name: None,
        });

        Ok(())
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Must have at least one node
        if self.nodes.is_empty() {
            return Err(ProxyError::Config(
                "No backend nodes configured".to_string(),
            ));
        }

        // Must have a primary node
        let has_primary = self.nodes.iter().any(|n| n.role == NodeRole::Primary);
        if !has_primary {
            return Err(ProxyError::Config("No primary node configured".to_string()));
        }

        // Validate pool config
        if self.pool.max_connections < self.pool.min_connections {
            return Err(ProxyError::Config(
                "max_connections must be >= min_connections".to_string(),
            ));
        }

        // A zero health-check interval panics `tokio::time::interval` at
        // construction, silently killing the health task (which then never
        // probes, so the proxy keeps routing to dead backends). Reject it here;
        // the checker also clamps to a 1s floor as belt-and-suspenders.
        if self.health.check_interval_secs == 0 {
            return Err(ProxyError::Config(
                "health.check_interval_secs must be >= 1".to_string(),
            ));
        }

        // Refuse to expose the admin API beyond loopback without a token. The
        // admin surface runs privileged operations (arbitrary SQL via
        // /api/sql, forced failover via /api/chaos, migration cutover, branch
        // CREATE/DROP DATABASE, replay/shadow against operator-chosen targets),
        // so an anonymous non-loopback bind is a critical hole. Only enforced
        // when the address parses to a concrete IP; a hostname is left to the
        // operator's DNS/network policy.
        if self.admin_token.is_none() && !self.admin_allow_insecure {
            if let Ok(sa) = self.admin_address.parse::<std::net::SocketAddr>() {
                if !sa.ip().is_loopback() {
                    return Err(ProxyError::Config(format!(
                        "admin_address '{}' is not loopback but admin_token is unset — the admin \
                         API runs privileged operations and must not be exposed anonymously. Set \
                         admin_token, bind admin_address to 127.0.0.1, or set \
                         admin_allow_insecure = true to override.",
                        self.admin_address
                    )));
                }
            }
        }

        // Operational limits/timeouts. Each of these was a compiled-in safety
        // bound; a 0 disables the bound (slow-loris handshake never times out,
        // an unbounded statement registry, a zero-capacity map, a reaper that
        // panics `tokio::time::interval`) rather than meaning anything useful,
        // so reject 0 up front with a message that names the key.
        {
            let l = &self.limits;
            let zero_checks: [(&str, u64); 7] = [
                ("limits.startup_timeout_secs", l.startup_timeout_secs),
                (
                    "limits.backend_write_timeout_secs",
                    l.backend_write_timeout_secs,
                ),
                (
                    "limits.backend_read_timeout_secs",
                    l.backend_read_timeout_secs,
                ),
                (
                    "limits.client_write_timeout_secs",
                    l.client_write_timeout_secs,
                ),
                ("limits.reprepare_timeout_secs", l.reprepare_timeout_secs),
                ("limits.pool_reap_interval_secs", l.pool_reap_interval_secs),
                ("limits.max_cancel_keys", l.max_cancel_keys as u64),
            ];
            for (name, value) in zero_checks {
                if value == 0 {
                    return Err(ProxyError::Config(format!("{name} must be >= 1")));
                }
            }
            if l.max_prepared_statements == 0 {
                return Err(ProxyError::Config(
                    "limits.max_prepared_statements must be >= 1".to_string(),
                ));
            }
            if l.max_prepared_bytes == 0 {
                return Err(ProxyError::Config(
                    "limits.max_prepared_bytes must be >= 1".to_string(),
                ));
            }
            if l.max_pending_bytes == 0 {
                return Err(ProxyError::Config(
                    "limits.max_pending_bytes must be >= 1".to_string(),
                ));
            }
            if l.max_total_idle_backend_conns == 0 {
                return Err(ProxyError::Config(
                    "limits.max_total_idle_backend_conns must be >= 1".to_string(),
                ));
            }
        }

        // Edge / geo proxy mode. The [edge] section is parsed on every build
        // (so configs round-trip), but enabling it needs the compile-time
        // feature, and an edge role needs both its control plane (the home's
        // admin URL for the invalidation subscription) and its data plane
        // (at least one [[nodes]] entry pointing at the home's PG-wire
        // listener, through which misses and writes forward).
        if self.edge.enabled {
            if !cfg!(feature = "edge-proxy") {
                return Err(ProxyError::Config(
                    "edge.enabled = true but this binary was built without the 'edge-proxy' \
                     feature — rebuild with `--features edge-proxy` or remove/disable the \
                     [edge] section."
                        .to_string(),
                ));
            }
            // Same rationale as health.check_interval_secs: a zero interval
            // panics `tokio::time::interval` at construction, silently killing
            // the registry GC task. And a zero liveness window would prune
            // every edge on the first sweep. Reject both up front.
            if self.edge.subscribe_gc_secs == 0 {
                return Err(ProxyError::Config(
                    "edge.subscribe_gc_secs must be >= 1".to_string(),
                ));
            }
            if self.edge.liveness_window_secs == 0 {
                return Err(ProxyError::Config(
                    "edge.liveness_window_secs must be >= 1".to_string(),
                ));
            }
            // A zero TTL births every entry expired — the cache would
            // silently never serve a hit while looking enabled.
            if self.edge.default_ttl_secs == 0 {
                return Err(ProxyError::Config(
                    "edge.default_ttl_secs must be >= 1 when edge is enabled".to_string(),
                ));
            }
            if self.edge.role == crate::edge::EdgeRole::Edge {
                if self.edge.home_url.trim().is_empty() {
                    return Err(ProxyError::Config(
                        "edge.role = 'edge' requires edge.home_url — the home proxy's admin \
                         base URL (e.g. \"https://home-proxy:9090\") the edge subscribes to \
                         for cache invalidations."
                            .to_string(),
                    ));
                }
                // The auth_token is the home's ADMIN bearer (arbitrary SQL,
                // chaos, replay). Never transmit it in cleartext across the
                // edge<->home WAN link: require https, or an explicit opt-out
                // for provably private links (mirrors admin_allow_insecure).
                // URL schemes are case-insensitive (RFC 3986); compare lowered
                // so a legitimate `HTTPS://` is not wrongly rejected here (nor
                // left without downgrade protection in the client).
                if !self.edge.auth_token.is_empty()
                    && !self.edge.allow_insecure_home_url
                    && !self
                        .edge
                        .home_url
                        .trim()
                        .to_ascii_lowercase()
                        .starts_with("https://")
                {
                    return Err(ProxyError::Config(format!(
                        "edge.home_url '{}' is not https:// but edge.auth_token is set — the \
                         token is the home's admin bearer and must not cross the network in \
                         cleartext. Front the home admin port with a TLS terminator and use \
                         https://, or set edge.allow_insecure_home_url = true for private \
                         links (VPN/WireGuard/service mesh).",
                        self.edge.home_url.trim()
                    )));
                }
                // The query-result cache is not wired to SSE invalidations:
                // on an edge it would keep serving rows the home already
                // invalidated, for the full query-cache TTL. Refuse the
                // combination (the edge cache covers cacheable SELECTs here).
                if cfg!(feature = "query-cache") && self.cache.enabled {
                    return Err(ProxyError::Config(
                        "edge.role = 'edge' cannot be combined with [cache] enabled = true — \
                         the query-result cache does not receive edge invalidations and would \
                         serve stale rows past the edge coherence bound. Disable [cache] on \
                         edge-role proxies; the edge cache serves cacheable SELECTs there."
                            .to_string(),
                    ));
                }
                // Redundant with the global no-nodes check above today, but
                // kept for a message that says *what the node is for* in
                // edge mode should that check ever be relaxed.
                if self.nodes.is_empty() {
                    return Err(ProxyError::Config(
                        "edge.role = 'edge' requires at least one [[nodes]] entry pointing \
                         at the home proxy's PG-wire listener — cache misses and writes \
                         forward there."
                            .to_string(),
                    ));
                }
            }
        }

        // Anomaly detector tunables. Parsed on every build; only consumed when
        // the `anomaly-detection` feature is compiled in, but the values are
        // degenerate regardless of feature, so validate them unconditionally.
        // A zero rate/auth window would divide by an empty sliding window; a
        // zero event buffer or fingerprint cap would break the ring buffer /
        // novel-query set; a non-positive or non-finite z-threshold would never
        // (or always) fire; a warning count above the critical count inverts
        // the severity ladder.
        {
            let a = &self.anomaly;
            if a.rate_window_secs == 0 {
                return Err(ProxyError::Config(
                    "anomaly.rate_window_secs must be >= 1".to_string(),
                ));
            }
            if a.auth_window_secs == 0 {
                return Err(ProxyError::Config(
                    "anomaly.auth_window_secs must be >= 1".to_string(),
                ));
            }
            if a.event_buffer_size == 0 {
                return Err(ProxyError::Config(
                    "anomaly.event_buffer_size must be >= 1".to_string(),
                ));
            }
            if a.max_seen_fingerprints == 0 {
                return Err(ProxyError::Config(
                    "anomaly.max_seen_fingerprints must be >= 1".to_string(),
                ));
            }
            if !(a.spike_z_threshold.is_finite() && a.spike_z_threshold > 0.0) {
                return Err(ProxyError::Config(
                    "anomaly.spike_z_threshold must be a finite value > 0".to_string(),
                ));
            }
            if a.auth_critical_count == 0 {
                // A 0 critical threshold fires Critical on the very first failed
                // auth (count starts at 1 >= 0), turning every login typo into a
                // critical alert. Require at least 1.
                return Err(ProxyError::Config(
                    "anomaly.auth_critical_count must be >= 1".to_string(),
                ));
            }
            if a.auth_warning_count > a.auth_critical_count {
                return Err(ProxyError::Config(format!(
                    "anomaly.auth_warning_count ({}) must be <= anomaly.auth_critical_count ({})",
                    a.auth_warning_count, a.auth_critical_count
                )));
            }
        }

        Ok(())
    }

    /// Get primary node
    pub fn primary_node(&self) -> Option<&NodeConfig> {
        self.nodes
            .iter()
            .find(|n| n.role == NodeRole::Primary && n.enabled)
    }

    /// Get standby nodes
    pub fn standby_nodes(&self) -> Vec<&NodeConfig> {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Standby && n.enabled)
            .collect()
    }

    /// Get all enabled nodes
    pub fn enabled_nodes(&self) -> Vec<&NodeConfig> {
        self.nodes.iter().filter(|n| n.enabled).collect()
    }
}

/// TR (Transaction Replay) mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum TrMode {
    /// No transaction replay
    None,
    /// Re-establish session only
    #[default]
    Session,
    /// Re-execute SELECT queries
    Select,
    /// Full transaction replay
    Transaction,
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Minimum connections per node
    pub min_connections: usize,
    /// Maximum connections per node
    pub max_connections: usize,
    /// Connection idle timeout (seconds)
    pub idle_timeout_secs: u64,
    /// Maximum connection lifetime (seconds)
    pub max_lifetime_secs: u64,
    /// Connection acquire timeout (seconds)
    pub acquire_timeout_secs: u64,
    /// Test connection before use
    pub test_on_acquire: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 2,
            max_connections: 100,
            idle_timeout_secs: 300,
            max_lifetime_secs: 1800,
            acquire_timeout_secs: 30,
            test_on_acquire: true,
        }
    }
}

impl PoolConfig {
    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }

    /// Get max lifetime as Duration
    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime_secs)
    }

    /// Get acquire timeout as Duration
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_secs)
    }
}

/// Load balancer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    /// Routing strategy for read queries
    pub read_strategy: Strategy,
    /// Enable read/write splitting
    pub read_write_split: bool,
    /// Latency threshold for unhealthy marking (ms)
    pub latency_threshold_ms: u64,
}

impl Default for LoadBalancerConfig {
    fn default() -> Self {
        Self {
            read_strategy: Strategy::RoundRobin,
            read_write_split: true,
            latency_threshold_ms: 100,
        }
    }
}

/// Load balancing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    /// Round-robin across nodes
    RoundRobin,
    /// Weighted round-robin
    WeightedRoundRobin,
    /// Route to least loaded node
    LeastConnections,
    /// Route to lowest latency node
    LatencyBased,
    /// Random selection
    Random,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Check interval (seconds)
    pub check_interval_secs: u64,
    /// Check timeout (seconds)
    pub check_timeout_secs: u64,
    /// Failures before marking unhealthy
    pub failure_threshold: u32,
    /// Successes before marking healthy
    pub success_threshold: u32,
    /// Health check query
    pub check_query: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: 5,
            check_timeout_secs: 3,
            failure_threshold: 3,
            success_threshold: 2,
            check_query: "SELECT 1".to_string(),
        }
    }
}

impl HealthConfig {
    /// Get check interval as Duration
    pub fn check_interval(&self) -> Duration {
        Duration::from_secs(self.check_interval_secs)
    }

    /// Get check timeout as Duration
    pub fn check_timeout(&self) -> Duration {
        Duration::from_secs(self.check_timeout_secs)
    }
}

/// Backend node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node host
    pub host: String,
    /// Node port (PostgreSQL protocol)
    pub port: u16,
    /// Node HTTP API port (for SQL API forwarding)
    /// Defaults to 8080 if not specified
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    /// Node role
    pub role: NodeRole,
    /// Weight for load balancing
    pub weight: u32,
    /// Whether node is enabled
    pub enabled: bool,
    /// Optional node name for logging
    pub name: Option<String>,
}

fn default_http_port() -> u16 {
    8080
}

impl NodeConfig {
    /// Get address string
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Get display name
    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.host)
    }
}

/// Node role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    /// Primary node (accepts writes)
    Primary,
    /// Standby node (can be promoted)
    Standby,
    /// Read replica (read-only, cannot be promoted)
    #[serde(rename = "replica")]
    ReadReplica,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS for client connections
    pub enabled: bool,
    /// Path to certificate file
    pub cert_path: String,
    /// Path to private key file
    pub key_path: String,
    /// Path to CA certificate (for client verification)
    pub ca_path: Option<String>,
    /// Require client certificates
    pub require_client_cert: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ProxyConfig::default();
        assert_eq!(config.listen_address, "0.0.0.0:5432");
        assert!(config.tr_enabled);
    }

    #[test]
    fn test_add_node() {
        let mut config = ProxyConfig::default();
        config.add_node("localhost:5432", "primary").unwrap();
        config.add_node("localhost:5433", "standby").unwrap();

        assert_eq!(config.nodes.len(), 2);
        assert!(config.primary_node().is_some());
        assert_eq!(config.standby_nodes().len(), 1);
    }

    // -------------------------------------------------------------------------
    // Env-var substitution (`${VAR}` / `${VAR:-default}`)
    //
    // Tests use `HELIOS_SUBST_TEST_*` variable names that no shipped config
    // references, so they cannot perturb `test_all_shipped_configs_parse`
    // (Cargo runs tests in parallel threads that share one process env).
    // -------------------------------------------------------------------------

    #[test]
    fn test_substitute_env_set_value_wins() {
        std::env::set_var("HELIOS_SUBST_TEST_SET", "hello");
        // `${NAME:-default}`: a set var beats the default.
        assert_eq!(
            substitute_env("x = \"${HELIOS_SUBST_TEST_SET:-fallback}\"").unwrap(),
            "x = \"hello\""
        );
        // `${NAME}`: bare form uses the set value.
        assert_eq!(
            substitute_env("x = \"${HELIOS_SUBST_TEST_SET}\"").unwrap(),
            "x = \"hello\""
        );
        std::env::remove_var("HELIOS_SUBST_TEST_SET");
    }

    #[test]
    fn test_substitute_env_default_fallback() {
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_A");
        assert_eq!(
            substitute_env("s = \"${HELIOS_SUBST_TEST_UNSET_A:-abc}\"").unwrap(),
            "s = \"abc\""
        );
    }

    #[test]
    fn test_substitute_env_empty_default() {
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_B");
        assert_eq!(
            substitute_env("s = \"${HELIOS_SUBST_TEST_UNSET_B:-}\"").unwrap(),
            "s = \"\""
        );
    }

    #[test]
    fn test_substitute_env_missing_no_default_errors() {
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_C");
        let err = substitute_env("s = \"${HELIOS_SUBST_TEST_UNSET_C}\"").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("HELIOS_SUBST_TEST_UNSET_C"),
            "error must name the missing variable, got: {msg}"
        );
    }

    #[test]
    fn test_substitute_env_skips_full_line_comments() {
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_D");
        // A commented line carrying a bare `${VAR}` (no default) must neither
        // error nor be substituted — it is copied verbatim.
        let input = "  # default_password = \"${HELIOS_SUBST_TEST_UNSET_D}\"\nx = 1\n";
        assert_eq!(substitute_env(input).unwrap(), input);
    }

    #[test]
    fn test_substitute_env_multiple_on_one_line() {
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_E");
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_F");
        assert_eq!(
            substitute_env(
                "addr = \"${HELIOS_SUBST_TEST_UNSET_E:-host}:${HELIOS_SUBST_TEST_UNSET_F:-5432}\""
            )
            .unwrap(),
            "addr = \"host:5432\""
        );
    }

    #[test]
    fn test_substitute_env_unquoted_numeric_position() {
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_G");
        // Unquoted `${..:-50}` must become the bare token `50` (valid TOML).
        let out = substitute_env("max_connections = ${HELIOS_SUBST_TEST_UNSET_G:-50}").unwrap();
        assert_eq!(out, "max_connections = 50");
        // ...and that must now deserialize as an integer, not a string.
        #[derive(serde::Deserialize)]
        struct P {
            max_connections: u32,
        }
        let p: P = toml::from_str(&out).unwrap();
        assert_eq!(p.max_connections, 50);
    }

    #[test]
    fn test_substitute_env_leaves_malformed_literal() {
        // A `$` not opening a valid `${NAME...}` is left untouched.
        assert_eq!(substitute_env("cost = $5.00\n").unwrap(), "cost = $5.00\n");
        std::env::remove_var("HELIOS_SUBST_TEST_UNSET_H");
        // Unterminated placeholder is left literal (no error).
        assert_eq!(
            substitute_env("x = \"${HELIOS_SUBST_TEST_UNSET_H:-oops\"").unwrap(),
            "x = \"${HELIOS_SUBST_TEST_UNSET_H:-oops\""
        );
    }

    // -------------------------------------------------------------------------
    // Unknown top-level key detection
    // -------------------------------------------------------------------------

    #[test]
    fn test_unknown_top_level_keys_detection() {
        let text = "listen_address = \"x\"\n\
                    [pool]\nmin_connections = 1\n\
                    [ha]\nenabled = true\n\
                    [logging]\nlevel = \"info\"\n";
        // `listen_address` and `pool` are known; `ha` and `logging` are not.
        assert_eq!(
            unknown_top_level_keys(text),
            vec!["ha".to_string(), "logging".to_string()]
        );
    }

    #[test]
    fn test_unknown_top_level_keys_nested_are_out_of_scope() {
        // `[cache.l1]` is a NESTED unknown key under the known `cache` table;
        // it must NOT be reported (nested detection is out of scope).
        let text = "[cache]\nenabled = true\n[cache.l1]\nsize = 500\n";
        assert!(unknown_top_level_keys(text).is_empty());
    }

    #[test]
    fn test_known_top_level_keys_cover_struct_fields() {
        // Drift guard: every key a default `ProxyConfig` serialises to must be
        // present in `KNOWN_TOP_LEVEL_KEYS`. `Option` fields that default to
        // `None` (`tls`, `admin_token`) are absent from the serialised form, so
        // this is a subset check — it still catches a new non-optional field
        // added without updating the list.
        let value = toml::Value::try_from(ProxyConfig::default()).unwrap();
        let table = value.as_table().unwrap();
        for k in table.keys() {
            assert!(
                KNOWN_TOP_LEVEL_KEYS.contains(&k.as_str()),
                "field '{k}' is present in a serialised default ProxyConfig but \
                 missing from KNOWN_TOP_LEVEL_KEYS"
            );
        }
    }

    // -------------------------------------------------------------------------
    // CI guard: every shipped config must load after substitution
    // -------------------------------------------------------------------------

    #[test]
    fn test_all_shipped_configs_parse() {
        // For every `config/*.toml` and `scripts/regress/*.toml`: run env
        // substitution (with NO env vars set → `:-default` fallbacks) and assert
        // it deserializes into `ProxyConfig`.
        //
        // For the `config/*.toml` reference configs we go further and run the
        // FULL load path — `ProxyConfig::from_file`, which does substitution +
        // parse + `validate()` exactly as `heliosdb-proxy -c <file>` does. This
        // is the real guard behind "the shipped configs load": a deserialize-only
        // check passes even when `validate()` would reject the file (e.g. the
        // admin-loopback guard), so it would not have caught the non-loopback
        // `admin_address` default that made every reference config fail to start.
        //
        // The `scripts/regress/*.toml` files are intentionally deserialize-only:
        // they reference placeholder/non-loopback backend hosts, some enable the
        // `[edge]` section (which `validate()` gates on the compile-time
        // `edge-proxy` feature and on an `edge.home_url`), so `validate()` is not
        // universally applicable there. We still guarantee they parse.
        let manifest = env!("CARGO_MANIFEST_DIR");
        let config_dir = format!("{manifest}/config");
        let regress_dir = format!("{manifest}/scripts/regress");

        // Reference configs: must survive the entire from_file() load path.
        let mut config_checked = 0usize;
        let entries = std::fs::read_dir(&config_dir)
            .unwrap_or_else(|e| panic!("config dir {config_dir} unreadable: {e}"));
        for entry in entries {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }
            let path_str = path
                .to_str()
                .unwrap_or_else(|| panic!("non-UTF-8 config path {}", path.display()));
            let loaded = ProxyConfig::from_file(path_str);
            assert!(
                loaded.is_ok(),
                "shipped config {} failed to load via from_file() \
                 (substitute + parse + validate): {}",
                path.display(),
                loaded.err().unwrap()
            );
            config_checked += 1;
        }
        assert!(
            config_checked >= 3,
            "expected to load at least the 3 config/*.toml files, checked {config_checked}"
        );

        // Regression harness configs: must at least deserialize after
        // substitution (validate() deliberately skipped — see above).
        if let Ok(entries) = std::fs::read_dir(&regress_dir) {
            for entry in entries {
                let path = entry.unwrap().path();
                if path.extension().and_then(|e| e.to_str()) != Some("toml") {
                    continue;
                }
                let raw = std::fs::read_to_string(&path).unwrap();
                let substituted = substitute_env(&raw).unwrap_or_else(|e| {
                    panic!("env substitution failed for {}: {e}", path.display())
                });
                let parsed = toml::from_str::<ProxyConfig>(&substituted);
                assert!(
                    parsed.is_ok(),
                    "regress config {} failed to deserialize: {}",
                    path.display(),
                    parsed.err().unwrap()
                );
            }
        }
    }

    #[test]
    fn test_validate_no_nodes() {
        let config = ProxyConfig::default();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_no_primary() {
        let mut config = ProxyConfig::default();
        config.add_node("localhost:5432", "standby").unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_success() {
        let mut config = ProxyConfig::default();
        config.add_node("localhost:5432", "primary").unwrap();
        assert!(config.validate().is_ok());
    }

    // -------------------------------------------------------------------------
    // [anomaly] section — tunability of the in-process anomaly detector
    // -------------------------------------------------------------------------

    #[test]
    fn test_anomaly_toml_defaults_match_historical_values() {
        // Every default must reproduce the detector's old hardcoded
        // `AnomalyConfig::default()` (and the old `MAX_SEEN_FINGERPRINTS`
        // const) EXACTLY, so an absent [anomaly] section changes nothing.
        let a = AnomalyToml::default();
        assert_eq!(a.rate_window_secs, 60);
        assert_eq!(a.spike_z_threshold, 3.0);
        assert_eq!(a.auth_window_secs, 60);
        assert_eq!(a.auth_critical_count, 10);
        assert_eq!(a.auth_warning_count, 5);
        assert_eq!(a.event_buffer_size, 1024);
        assert!(a.emit_novel_queries);
        assert_eq!(a.max_seen_fingerprints, 100_000);
    }

    #[test]
    fn test_anomaly_toml_absent_section_uses_defaults() {
        // A full, valid ProxyConfig whose serialized TOML has the [anomaly]
        // table removed must fall back to the historical defaults via the
        // `#[serde(default)]` on the `anomaly` field.
        let mut base = ProxyConfig::default();
        base.add_node("localhost:5432", "primary").unwrap();
        let mut val = toml::Value::try_from(&base).unwrap();
        val.as_table_mut().unwrap().remove("anomaly");
        assert!(
            val.get("anomaly").is_none(),
            "anomaly section should be absent for this test"
        );
        let s = toml::to_string(&val).unwrap();
        let cfg: ProxyConfig = toml::from_str(&s).unwrap();
        let a = AnomalyToml::default();
        assert_eq!(cfg.anomaly.rate_window_secs, a.rate_window_secs);
        assert_eq!(cfg.anomaly.max_seen_fingerprints, a.max_seen_fingerprints);
        assert_eq!(cfg.anomaly.emit_novel_queries, a.emit_novel_queries);
    }

    #[test]
    fn test_anomaly_toml_block_parses_and_overrides() {
        // A present [anomaly] block round-trips through a full ProxyConfig and
        // overrides every field.
        let mut base = ProxyConfig::default();
        base.add_node("localhost:5432", "primary").unwrap();
        base.anomaly = AnomalyToml {
            rate_window_secs: 30,
            spike_z_threshold: 4.5,
            auth_window_secs: 120,
            auth_critical_count: 20,
            auth_warning_count: 8,
            event_buffer_size: 4096,
            emit_novel_queries: false,
            max_seen_fingerprints: 250_000,
        };
        let s = toml::to_string(&base).unwrap();
        assert!(s.contains("[anomaly]"), "serialized config: {s}");
        let cfg: ProxyConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg.anomaly.rate_window_secs, 30);
        assert_eq!(cfg.anomaly.spike_z_threshold, 4.5);
        assert_eq!(cfg.anomaly.auth_window_secs, 120);
        assert_eq!(cfg.anomaly.auth_critical_count, 20);
        assert_eq!(cfg.anomaly.auth_warning_count, 8);
        assert_eq!(cfg.anomaly.event_buffer_size, 4096);
        assert!(!cfg.anomaly.emit_novel_queries);
        assert_eq!(cfg.anomaly.max_seen_fingerprints, 250_000);
    }

    #[test]
    fn test_anomaly_toml_partial_block_fills_rest_from_defaults() {
        // A partial [anomaly] section overrides only the listed keys; the rest
        // fall back to the per-field serde defaults. Parsed as the section
        // struct directly (ProxyConfig's own top-level fields are required).
        let a: AnomalyToml = toml::from_str("spike_z_threshold = 5.0\n").unwrap();
        assert_eq!(a.spike_z_threshold, 5.0);
        assert_eq!(a.rate_window_secs, 60);
        assert_eq!(a.event_buffer_size, 1024);
        assert_eq!(a.max_seen_fingerprints, 100_000);
    }

    #[test]
    fn test_validate_rejects_degenerate_anomaly_values() {
        let base = || {
            let mut c = ProxyConfig::default();
            c.add_node("localhost:5432", "primary").unwrap();
            c
        };
        // Sanity: the untouched defaults validate.
        assert!(base().validate().is_ok());

        // rate_window_secs = 0 -> rejected with a clear message.
        let mut c = base();
        c.anomaly.rate_window_secs = 0;
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains("anomaly.rate_window_secs"),
            "unexpected error: {err}"
        );

        // auth_window_secs = 0 -> rejected.
        let mut c = base();
        c.anomaly.auth_window_secs = 0;
        assert!(c.validate().is_err());

        // event_buffer_size = 0 -> rejected.
        let mut c = base();
        c.anomaly.event_buffer_size = 0;
        assert!(c.validate().is_err());

        // max_seen_fingerprints = 0 -> rejected.
        let mut c = base();
        c.anomaly.max_seen_fingerprints = 0;
        assert!(c.validate().is_err());

        // non-finite / non-positive z threshold -> rejected.
        let mut c = base();
        c.anomaly.spike_z_threshold = 0.0;
        assert!(c.validate().is_err());
        let mut c = base();
        c.anomaly.spike_z_threshold = f64::NAN;
        assert!(c.validate().is_err());

        // warning count above critical count inverts the ladder -> rejected.
        let mut c = base();
        c.anomaly.auth_warning_count = 11;
        c.anomaly.auth_critical_count = 10;
        assert!(c.validate().is_err());

        // auth_critical_count = 0 would fire Critical on the first failed auth.
        let mut c = base();
        c.anomaly.auth_critical_count = 0;
        c.anomaly.auth_warning_count = 0; // keep warning <= critical
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains("anomaly.auth_critical_count"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "anomaly-detection")]
    #[test]
    fn test_anomaly_toml_to_anomaly_config_roundtrip() {
        // The conversion is field-for-field; defaults must equal the runtime
        // detector's own default, and explicit values must carry through.
        let default_rt = AnomalyToml::default().to_anomaly_config();
        let expected = crate::anomaly::AnomalyConfig::default();
        assert_eq!(default_rt.rate_window_secs, expected.rate_window_secs);
        assert_eq!(default_rt.spike_z_threshold, expected.spike_z_threshold);
        assert_eq!(default_rt.auth_window_secs, expected.auth_window_secs);
        assert_eq!(default_rt.auth_critical_count, expected.auth_critical_count);
        assert_eq!(default_rt.auth_warning_count, expected.auth_warning_count);
        assert_eq!(default_rt.event_buffer_size, expected.event_buffer_size);
        assert_eq!(default_rt.emit_novel_queries, expected.emit_novel_queries);
        assert_eq!(
            default_rt.max_seen_fingerprints,
            expected.max_seen_fingerprints
        );

        let toml = AnomalyToml {
            rate_window_secs: 15,
            spike_z_threshold: 2.5,
            auth_window_secs: 90,
            auth_critical_count: 12,
            auth_warning_count: 6,
            event_buffer_size: 2048,
            emit_novel_queries: false,
            max_seen_fingerprints: 500_000,
        };
        let rt = toml.to_anomaly_config();
        assert_eq!(rt.rate_window_secs, 15);
        assert_eq!(rt.spike_z_threshold, 2.5);
        assert_eq!(rt.auth_window_secs, 90);
        assert_eq!(rt.auth_critical_count, 12);
        assert_eq!(rt.auth_warning_count, 6);
        assert_eq!(rt.event_buffer_size, 2048);
        assert!(!rt.emit_novel_queries);
        assert_eq!(rt.max_seen_fingerprints, 500_000);
    }

    #[test]
    fn test_validate_refuses_anonymous_nonloopback_admin() {
        let base = || {
            let mut c = ProxyConfig::default();
            c.add_node("localhost:5432", "primary").unwrap();
            c
        };
        // Loopback + no token: allowed (the default).
        let mut c = base();
        c.admin_address = "127.0.0.1:9090".to_string();
        assert!(c.validate().is_ok());
        // Non-loopback + no token: REFUSED.
        let mut c = base();
        c.admin_address = "0.0.0.0:9090".to_string();
        assert!(
            c.validate().is_err(),
            "anonymous 0.0.0.0 admin must be refused"
        );
        // Non-loopback + token: allowed.
        let mut c = base();
        c.admin_address = "0.0.0.0:9090".to_string();
        c.admin_token = Some("secret".to_string());
        assert!(c.validate().is_ok());
        // Non-loopback + explicit opt-in: allowed.
        let mut c = base();
        c.admin_address = "0.0.0.0:9090".to_string();
        c.admin_allow_insecure = true;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_zero_health_interval() {
        // A zero health-check interval panics tokio::time::interval; validation
        // must reject it up front rather than let the health task die silently.
        let mut config = ProxyConfig::default();
        config.add_node("localhost:5432", "primary").unwrap();
        config.health.check_interval_secs = 0;
        assert!(config.validate().is_err());
        config.health.check_interval_secs = 1;
        assert!(config.validate().is_ok());
    }

    // -------------------------------------------------------------------------
    // [limits] section (operational safety bounds, formerly hardcoded consts)
    // -------------------------------------------------------------------------

    #[test]
    fn test_limits_defaults_equal_prior_constants() {
        // Every default MUST reproduce the exact value the constant held in
        // src/server.rs, so an existing deployment with no `[limits]` block is
        // byte-for-byte unchanged.
        let l = LimitsToml::default();
        assert_eq!(l.max_cancel_keys, 100_000);
        assert_eq!(l.startup_timeout_secs, 30);
        assert_eq!(l.backend_write_timeout_secs, 30);
        assert_eq!(l.backend_read_timeout_secs, 30);
        assert_eq!(l.client_write_timeout_secs, 60);
        assert_eq!(l.reprepare_timeout_secs, 15);
        assert_eq!(l.max_prepared_statements, 8192);
        assert_eq!(l.max_prepared_bytes, 64 * 1024 * 1024);
        assert_eq!(l.max_pending_bytes, 64 * 1024 * 1024);
        assert_eq!(l.max_total_idle_backend_conns, 8192);
        assert_eq!(l.pool_reap_interval_secs, 30);
        // And the field on a default ProxyConfig matches.
        assert_eq!(
            ProxyConfig::default().limits.max_prepared_bytes,
            64 * 1024 * 1024
        );
    }

    #[test]
    fn test_limits_toml_partial_overrides_and_fills_defaults() {
        // Parsing a partial `[limits]` table (as `LimitsToml`) overrides the
        // listed keys and fills the rest from the per-field serde defaults.
        let limits: LimitsToml = toml::from_str(
            "startup_timeout_secs = 5\nmax_prepared_statements = 100\nmax_cancel_keys = 42\n",
        )
        .expect("parse partial LimitsToml");
        // Overridden.
        assert_eq!(limits.startup_timeout_secs, 5);
        assert_eq!(limits.max_prepared_statements, 100);
        assert_eq!(limits.max_cancel_keys, 42);
        // Untouched keys keep their const defaults.
        assert_eq!(limits.client_write_timeout_secs, 60);
        assert_eq!(limits.max_pending_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.pool_reap_interval_secs, 30);
    }

    #[test]
    fn test_proxyconfig_partial_limits_section_overrides() {
        // Full ProxyConfig load path: a `[limits]` section with only some keys
        // set overrides those and defaults the rest. Built from a serialized
        // default config so the required non-limits fields are all present.
        let mut val = toml::Value::try_from(ProxyConfig::default()).unwrap();
        let mut partial = toml::value::Table::new();
        partial.insert("startup_timeout_secs".into(), toml::Value::Integer(5));
        partial.insert("max_cancel_keys".into(), toml::Value::Integer(42));
        val.as_table_mut()
            .unwrap()
            .insert("limits".into(), toml::Value::Table(partial));
        let text = toml::to_string(&val).unwrap();
        let cfg: ProxyConfig = toml::from_str(&text).expect("parse config with partial [limits]");
        assert_eq!(cfg.limits.startup_timeout_secs, 5);
        assert_eq!(cfg.limits.max_cancel_keys, 42);
        // Unset key defaults.
        assert_eq!(cfg.limits.client_write_timeout_secs, 60);
    }

    #[test]
    fn test_proxyconfig_absent_limits_section_is_default() {
        // A config with NO `[limits]` table at all → the whole section defaults
        // (the `#[serde(default)]` on the field). Built by stripping `limits`
        // from a serialized default config.
        let mut val = toml::Value::try_from(ProxyConfig::default()).unwrap();
        val.as_table_mut().unwrap().remove("limits");
        assert!(val.as_table().unwrap().get("limits").is_none());
        let text = toml::to_string(&val).unwrap();
        let cfg: ProxyConfig = toml::from_str(&text).expect("parse config without [limits]");
        assert_eq!(cfg.limits.startup_timeout_secs, 30);
        assert_eq!(cfg.limits.max_total_idle_backend_conns, 8192);
        assert_eq!(cfg.limits.pool_reap_interval_secs, 30);
    }

    #[test]
    fn test_validate_rejects_zero_limits() {
        let base = || {
            let mut c = ProxyConfig::default();
            c.add_node("localhost:5432", "primary").unwrap();
            c
        };
        // A pristine config validates.
        assert!(base().validate().is_ok());

        // Each timeout at 0 is rejected.
        let mut c = base();
        c.limits.startup_timeout_secs = 0;
        assert!(
            c.validate().is_err(),
            "zero startup_timeout must be rejected"
        );

        let mut c = base();
        c.limits.backend_write_timeout_secs = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.client_write_timeout_secs = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.reprepare_timeout_secs = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.pool_reap_interval_secs = 0;
        assert!(c.validate().is_err());

        // Each cap at 0 is rejected.
        let mut c = base();
        c.limits.max_cancel_keys = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.max_prepared_statements = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.max_prepared_bytes = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.max_pending_bytes = 0;
        assert!(c.validate().is_err());

        let mut c = base();
        c.limits.max_total_idle_backend_conns = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn test_validate_edge_disabled_section_is_inert() {
        // The default [edge] section (enabled = false) must never affect
        // validation, whatever features the binary was built with.
        let mut config = ProxyConfig::default();
        config.add_node("localhost:5432", "primary").unwrap();
        assert!(!config.edge.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_edge_enabled_requires_feature() {
        let mut config = ProxyConfig::default();
        config.add_node("localhost:5432", "primary").unwrap();
        config.edge.enabled = true;
        // role=home needs nothing beyond the compile-time feature.
        if cfg!(feature = "edge-proxy") {
            assert!(config.validate().is_ok());
        } else {
            assert!(
                config.validate().is_err(),
                "edge.enabled on a build without the edge-proxy feature must be refused"
            );
        }
    }

    #[cfg(feature = "edge-proxy")]
    #[test]
    fn test_validate_edge_rejects_zero_intervals() {
        // Zero GC cadence panics tokio::time::interval; zero liveness
        // window prunes every edge on the first sweep. Both refused.
        let base = || {
            let mut c = ProxyConfig::default();
            c.add_node("localhost:5432", "primary").unwrap();
            c.edge.enabled = true;
            c
        };
        let mut c = base();
        c.edge.subscribe_gc_secs = 0;
        assert!(c.validate().is_err());
        let mut c = base();
        c.edge.liveness_window_secs = 0;
        assert!(c.validate().is_err());
        // Only enforced when the section is enabled: an inert config
        // with odd values must not fail validation.
        let mut c = base();
        c.edge.enabled = false;
        c.edge.subscribe_gc_secs = 0;
        assert!(c.validate().is_ok());
    }

    #[cfg(feature = "edge-proxy")]
    #[test]
    fn test_validate_edge_role_requires_home_url() {
        let base = || {
            let mut c = ProxyConfig::default();
            c.add_node("localhost:5432", "primary").unwrap();
            c.edge.enabled = true;
            c.edge.role = crate::edge::EdgeRole::Edge;
            c
        };
        // role=edge without home_url: refused, and the message says why.
        let c = base();
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("home_url"), "unexpected error: {}", err);
        // role=edge + home_url (+ the node from base): allowed.
        let mut c = base();
        c.edge.home_url = "http://home-proxy:9090".to_string();
        assert!(c.validate().is_ok());
    }

    #[cfg(feature = "edge-proxy")]
    #[test]
    fn test_validate_edge_zero_ttl_refused_when_enabled() {
        let mut c = ProxyConfig::default();
        c.add_node("localhost:5432", "primary").unwrap();
        c.edge.enabled = true;
        c.edge.default_ttl_secs = 0;
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains("default_ttl_secs"),
            "unexpected error: {}",
            err
        );
        // Inert section: odd values tolerated when disabled.
        c.edge.enabled = false;
        assert!(c.validate().is_ok());
    }

    #[cfg(feature = "edge-proxy")]
    #[test]
    fn test_validate_edge_token_requires_https_home_url() {
        let base = || {
            let mut c = ProxyConfig::default();
            c.add_node("localhost:5432", "primary").unwrap();
            c.edge.enabled = true;
            c.edge.role = crate::edge::EdgeRole::Edge;
            c.edge.home_url = "http://home-proxy:9090".to_string();
            c
        };
        // Tokenless plain-http: fine (no credential to leak).
        assert!(base().validate().is_ok());
        // Token over plain http: refused, message names both remedies.
        let mut c = base();
        c.edge.auth_token = "secret".to_string();
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("https"), "unexpected error: {}", err);
        assert!(err.contains("allow_insecure_home_url"), "{}", err);
        // Explicit opt-out for private links: allowed.
        let mut c = base();
        c.edge.auth_token = "secret".to_string();
        c.edge.allow_insecure_home_url = true;
        assert!(c.validate().is_ok());
        // Token over https: allowed.
        let mut c = base();
        c.edge.auth_token = "secret".to_string();
        c.edge.home_url = "https://home-proxy:9090".to_string();
        assert!(c.validate().is_ok());
    }

    #[cfg(all(feature = "edge-proxy", feature = "query-cache"))]
    #[test]
    fn test_validate_edge_role_rejects_query_cache_combo() {
        // The query-result cache never hears SSE invalidations — on an
        // edge it would serve stale rows past the edge coherence bound.
        let mut c = ProxyConfig::default();
        c.add_node("localhost:5432", "primary").unwrap();
        c.edge.enabled = true;
        c.edge.role = crate::edge::EdgeRole::Edge;
        c.edge.home_url = "https://home-proxy:9090".to_string();
        c.cache.enabled = true;
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("[cache]"), "unexpected error: {}", err);
        // Home role + query-cache stays permitted (local writes
        // invalidate both caches on the same path).
        c.edge.role = crate::edge::EdgeRole::Home;
        c.edge.home_url.clear();
        assert!(c.validate().is_ok());
        // Edge role with the cache disabled is fine.
        c.edge.role = crate::edge::EdgeRole::Edge;
        c.edge.home_url = "https://home-proxy:9090".to_string();
        c.cache.enabled = false;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn test_pool_config_durations() {
        let config = PoolConfig::default();
        assert_eq!(config.idle_timeout(), Duration::from_secs(300));
        assert_eq!(config.max_lifetime(), Duration::from_secs(1800));
    }

    #[test]
    fn test_pool_mode_default() {
        let config = PoolModeConfig::default();
        assert_eq!(config.mode, PoolingMode::Session);
        assert_eq!(config.max_pool_size, 100);
        assert_eq!(config.min_idle, 10);
        assert_eq!(config.reset_query, "DISCARD ALL");
    }

    #[test]
    fn test_pool_mode_session() {
        let config = PoolModeConfig::session_mode();
        assert_eq!(config.mode, PoolingMode::Session);
        assert_eq!(config.prepared_statement_mode, PreparedStatementMode::Named);
    }

    #[test]
    fn test_pool_mode_transaction() {
        let config = PoolModeConfig::transaction_mode();
        assert_eq!(config.mode, PoolingMode::Transaction);
        assert_eq!(config.prepared_statement_mode, PreparedStatementMode::Track);
    }

    #[test]
    fn test_pool_mode_statement() {
        let config = PoolModeConfig::statement_mode();
        assert_eq!(config.mode, PoolingMode::Statement);
        assert_eq!(
            config.prepared_statement_mode,
            PreparedStatementMode::Disable
        );
    }

    #[test]
    fn test_pool_mode_durations() {
        let config = PoolModeConfig::default();
        assert_eq!(config.idle_timeout(), Duration::from_secs(600));
        assert_eq!(config.max_lifetime(), Duration::from_secs(3600));
        assert_eq!(config.acquire_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn test_proxy_config_has_pool_mode() {
        let config = ProxyConfig::default();
        assert_eq!(config.pool_mode.mode, PoolingMode::Session);
    }

    /// `plugins` defaults to `enabled = false` so adding the field to
    /// `ProxyConfig` doesn't spontaneously turn on the plugin subsystem
    /// for existing deployments.
    #[test]
    fn test_plugin_toml_default_is_disabled() {
        let config = ProxyConfig::default();
        assert!(!config.plugins.enabled);
        assert_eq!(config.plugins.plugin_dir, "/etc/heliosproxy/plugins");
        assert_eq!(config.plugins.memory_limit_mb, 64);
        assert_eq!(config.plugins.timeout_ms, 100);
    }

    /// Existing TOML configs (written before this field existed) must
    /// round-trip through `Deserialize` without failing. The `plugins`
    /// section is `#[serde(default)]`, so omitting it yields the default.
    #[test]
    fn test_proxy_config_toml_without_plugins_section_still_parses() {
        let toml_text = r#"
            listen_address = "0.0.0.0:5432"
            admin_address = "0.0.0.0:9090"
            tr_enabled = true
            tr_mode = "session"
            nodes = []

            [pool]
            min_connections = 2
            max_connections = 10
            idle_timeout_secs = 300
            max_lifetime_secs = 1800
            acquire_timeout_secs = 30
            test_on_acquire = true

            [load_balancer]
            read_strategy = "round_robin"
            read_write_split = true
            latency_threshold_ms = 100

            [health]
            check_interval_secs = 5
            check_timeout_secs = 3
            failure_threshold = 3
            success_threshold = 2
            check_query = "SELECT 1"
        "#;
        let config: ProxyConfig = toml::from_str(toml_text).expect("parse");
        assert!(!config.plugins.enabled);
    }

    /// A `[plugins]` section with overrides round-trips and populates the
    /// struct correctly.
    #[test]
    fn test_plugin_toml_overrides_parse() {
        let toml_text = r#"
            listen_address = "0.0.0.0:5432"
            admin_address = "0.0.0.0:9090"
            tr_enabled = true
            tr_mode = "session"
            nodes = []

            [pool]
            min_connections = 2
            max_connections = 10
            idle_timeout_secs = 300
            max_lifetime_secs = 1800
            acquire_timeout_secs = 30
            test_on_acquire = true

            [load_balancer]
            read_strategy = "round_robin"
            read_write_split = true
            latency_threshold_ms = 100

            [health]
            check_interval_secs = 5
            check_timeout_secs = 3
            failure_threshold = 3
            success_threshold = 2
            check_query = "SELECT 1"

            [plugins]
            enabled = true
            plugin_dir = "/tmp/helios-plugins"
            hot_reload = true
            memory_limit_mb = 128
            timeout_ms = 250
        "#;
        let config: ProxyConfig = toml::from_str(toml_text).expect("parse");
        assert!(config.plugins.enabled);
        assert_eq!(config.plugins.plugin_dir, "/tmp/helios-plugins");
        assert!(config.plugins.hot_reload);
        assert_eq!(config.plugins.memory_limit_mb, 128);
        assert_eq!(config.plugins.timeout_ms, 250);
        // Un-specified fields retain their defaults.
        assert_eq!(config.plugins.max_plugins, 20);
        assert!(config.plugins.fuel_metering);
    }
}
