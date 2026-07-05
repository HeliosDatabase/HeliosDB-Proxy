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
    /// requires `Authorization: Bearer <token>`. Absent (default) = open
    /// (current behaviour) — set this for any non-loopback deployment.
    #[serde(default)]
    pub admin_token: Option<String>,
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
            admin_address: "0.0.0.0:9090".to_string(),
            admin_token: None,
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
            branch: BranchConfig::default(),
            routing_hints: RoutingHintsConfig::default(),
            rate_limit: RateLimitToml::default(),
            circuit_breaker: CircuitBreakerToml::default(),
            analytics: AnalyticsToml::default(),
            lag_routing: LagRoutingToml::default(),
            cache: CacheToml::default(),
            query_rewrite: QueryRewriteToml::default(),
            multi_tenancy: MultiTenancyToml::default(),
            schema_routing: SchemaRoutingToml::default(),
            graphql_gateway: GraphqlGatewayConfig::default(),
            optimize_unnamed_parse: true,
            shutdown_drain_timeout_secs: default_drain_timeout_secs(),
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
        }
    }
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

        let contents = std::fs::read_to_string(path)
            .map_err(|e| ProxyError::Config(format!("Failed to read config: {}", e)))?;

        let config: Self = toml::from_str(&contents)
            .map_err(|e| ProxyError::Config(format!("Failed to parse config: {}", e)))?;

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
