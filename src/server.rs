//! Proxy Server Implementation
//!
//! Main server that accepts client connections and routes them to backends.
//! Implements PostgreSQL wire protocol forwarding with TWR (Transparent Write Routing).

use crate::admin::{AdminServer, AdminState, ConfigSnapshot, NodeSnapshot};
#[cfg(feature = "ha-tr")]
use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::client_tls::{build_tls_acceptor, ClientStream};
use crate::config::{HbaAction, HbaRule, NodeConfig, NodeRole, ProxyConfig, TrMode};
#[cfg(feature = "wasm-plugins")]
use crate::protocol::QueryMessage;
use crate::protocol::{
    ErrorResponse, Message, MessageType, ProtocolCodec, StartupMessage, TransactionStatus,
};
use crate::{ProxyError, Result};
use arc_swap::ArcSwap;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

// Pool-modes feature imports
#[cfg(feature = "pool-modes")]
use crate::pool::lease::ClientId;
#[cfg(feature = "pool-modes")]
use crate::pool::{ConnectionPoolManager, PoolModeConfig, PoolingMode};
#[cfg(feature = "pool-modes")]
use crate::NodeEndpoint;

// WASM plugin system imports
#[cfg(feature = "wasm-plugins")]
use crate::plugins::{
    AuthRequest as PluginAuthRequest, AuthResult, HookContext, HookType, Identity, PluginManager,
    PostQueryOutcome, PreQueryResult, QueryContext, RouteResult,
};

/// Proxy server
pub struct ProxyServer {
    config: ProxyConfig,
    state: Arc<ServerState>,
    shutdown_tx: broadcast::Sender<()>,
    /// Path the config was loaded from, retained so `SIGHUP` can re-read it
    /// for a zero-downtime reload (Batch H). `None` when the config was built
    /// from CLI flags/defaults rather than a file.
    config_path: Option<String>,
}

/// Stand-in "signal stream" on platforms without Unix signals: its `recv()`
/// never resolves, so the `SIGHUP` select arm is simply inert there.
#[cfg(not(unix))]
struct HangupNever;
#[cfg(not(unix))]
impl HangupNever {
    async fn recv(&mut self) -> Option<()> {
        std::future::pending().await
    }
}

/// Build the BackendConfig template the time-travel replay engine
/// uses for its target connection. The replay handler swaps in
/// `target_host` / `target_port` per request; everything else
/// (auth, TLS policy, timeouts) comes from this template.
///
/// Auth defaults to the bare PostgreSQL `postgres` superuser without
/// a password — sensible for local development against `trust` auth,
/// never for production. Per-call credential overrides on
/// ReplayRequestBody land in FU-21.
///
/// `_config` is kept in the signature so future iterations can pull
/// shared TLS / timeout settings from the proxy config without
/// changing the call site.
#[cfg(feature = "ha-tr")]
fn build_replay_backend_template(_config: &ProxyConfig) -> BackendConfig {
    BackendConfig {
        host: "placeholder".to_string(),
        port: 0,
        user: "postgres".to_string(),
        password: None,
        database: None,
        application_name: Some("heliosdb-proxy-replay".to_string()),
        tls_mode: TlsMode::Disable,
        connect_timeout: Duration::from_secs(5),
        query_timeout: Duration::from_secs(30),
        tls_config: default_client_config(),
    }
}

/// Cheap query-shape fingerprint for the anomaly detector. Replaces
/// numeric and string literals with `?` placeholders, lower-cases
/// keywords, and collapses whitespace. Same shape regardless of
/// literal values — `SELECT * FROM users WHERE id = 1` and
/// `SELECT * FROM users WHERE id = 99` map to the same fingerprint.
///
/// Not a parser. The analytics module has the canonical normaliser
/// when query-analytics is on; this is a lightweight standalone so
/// the anomaly detector works even when analytics is off.
#[cfg(feature = "anomaly-detection")]
fn anomaly_fingerprint(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut in_single = false;
    let mut prev_space = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            in_single = !in_single;
            // Replace the entire string literal (open + body +
            // close) with a single ?.
            if in_single {
                out.push('?');
                while let Some(&n) = chars.peek() {
                    chars.next();
                    if n == '\'' {
                        in_single = false;
                        break;
                    }
                }
                prev_space = false;
                continue;
            }
        }
        if c.is_ascii_digit() {
            if !out.ends_with('?') {
                out.push('?');
            }
            // Skip the rest of the number.
            while matches!(chars.peek(), Some(c) if c.is_ascii_digit() || *c == '.') {
                chars.next();
            }
            prev_space = false;
            continue;
        }
        if c.is_ascii_whitespace() {
            if !prev_space && !out.is_empty() {
                out.push(' ');
                prev_space = true;
            }
            continue;
        }
        out.push(c.to_ascii_lowercase());
        prev_space = false;
    }
    out.trim_end().to_string()
}

/// Server runtime state
struct ServerState {
    /// Active client sessions
    sessions: RwLock<HashMap<Uuid, Arc<ClientSession>>>,
    /// Node health status
    // Read-mostly: only the periodic health checker writes (a full-map
    // swap), every query reads. ArcSwap makes the per-query read a single
    // lock-free atomic load with no await, no semaphore, no guard held
    // across the routing awaits.
    health: ArcSwap<HashMap<String, NodeHealth>>,
    /// Write-serialization lock for `health`. Every reader stays lock-free on
    /// the ArcSwap; every *writer* (periodic checker, in-band demotion, SIGHUP
    /// reconcile) holds this across its load → clone → mutate → store so the
    /// non-atomic read-modify-write cannot lose updates under concurrency.
    health_write: parking_lot::Mutex<()>,
    /// Live, reloadable proxy configuration (Batch H). The accept loop snapshots
    /// this per new connection and the health checker reads it each tick, so a
    /// SIGHUP that swaps it takes effect for new connections and node health
    /// without dropping any in-flight session. The fields that can only be
    /// applied at startup (listen/admin socket addresses) are ignored on reload
    /// with a warning. Existing connections keep the snapshot they started with.
    live_config: ArcSwap<ProxyConfig>,
    /// Metrics
    metrics: ServerMetrics,
    /// Query-cancellation routing. Maps the BackendKeyData (pid, secret)
    /// the backend handed to the client onto the backend address that
    /// issued it, so a later out-of-band CancelRequest (which arrives on a
    /// fresh connection) can be forwarded to the right backend instead of
    /// being dropped. Bounded; best-effort.
    cancel_map: Arc<DashMap<(u32, u32), String>>,
    /// Insertion order of `cancel_map` keys, so an overflow evicts the OLDEST
    /// entries (FIFO) instead of clearing the whole map — a busy proxy no
    /// longer loses every in-flight cancel registration at once.
    cancel_order: Arc<parking_lot::Mutex<std::collections::VecDeque<(u32, u32)>>>,
    /// Client-facing TLS acceptor, built from `[tls]` config when enabled.
    /// `None` => the proxy rejects SSLRequests with `N` (plaintext only).
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    /// Proxy-terminated SCRAM auth state. `Some` when `[auth] mode = "scram"`:
    /// the proxy authenticates clients itself against this user list instead
    /// of relaying their credentials to the backend.
    auth_file: Option<Arc<crate::auth_scram::AuthFile>>,
    /// Traffic-mirror handle. `Some` when `[mirror] enabled`: the data path
    /// offers write statements to a background mirror worker.
    mirror: Option<crate::mirror::MirrorHandle>,
    /// Migration cutover switch. When `Some`, NEW client connections are
    /// transparently redirected to the promoted target (the former mirror)
    /// instead of the configured primary. Set via POST /api/migration/cutover.
    cutover: Arc<ArcSwap<Option<Arc<crate::mirror::CutoverTarget>>>>,
    /// Load balancer state
    lb_state: LoadBalancerState,
    /// SQL-comment routing-hint parser. `Some` when `[routing_hints] enabled`
    /// and the `routing-hints` feature is compiled in; the parser's own
    /// `strip_hints` flag records whether to rewrite the SQL before forwarding.
    /// Applied per query, taking precedence over default verb routing.
    #[cfg(feature = "routing-hints")]
    hint_parser: Option<crate::routing::HintParser>,
    /// Multi-dimensional rate limiter. `Some` when `[rate_limit] enabled`;
    /// every query is checked against it before being forwarded to a backend.
    #[cfg(feature = "rate-limiting")]
    rate_limiter: Option<Arc<crate::rate_limit::RateLimiter>>,
    /// Per-node circuit breaker manager. `Some` when `[circuit_breaker]
    /// enabled`. Records per-node success/failure on the forward path, excludes
    /// open nodes from read selection, and fast-fails queries to an open node.
    #[cfg(feature = "circuit-breaker")]
    circuit_breaker: Option<Arc<crate::circuit_breaker::CircuitBreakerManager>>,
    /// Query analytics engine. `Some` when `[analytics] enabled`. Every
    /// forwarded query is recorded (fingerprint, latency, slow-query log).
    #[cfg(feature = "query-analytics")]
    analytics: Option<Arc<crate::analytics::QueryAnalytics>>,
    /// Query-result cache (L1 hot / L2 warm). `Some` when `[cache] enabled`.
    /// Read SELECTs are served from it; writes invalidate referenced tables.
    #[cfg(feature = "query-cache")]
    query_cache: Option<Arc<crate::cache::QueryCache>>,
    /// SQL query rewriter. `Some` when `[query_rewrite] enabled` with rules.
    /// Rewrites the query SQL on the path before forwarding.
    #[cfg(feature = "query-rewriting")]
    rewriter: Option<Arc<crate::rewriter::QueryRewriter>>,
    /// Multi-tenancy manager. `Some` when `[multi_tenancy] enabled`. Identifies
    /// the tenant for a session and injects a row-level tenant filter.
    #[cfg(feature = "multi-tenancy")]
    tenant_manager: Option<Arc<crate::multi_tenancy::TenantManager>>,
    /// Schema/workload query analyzer. `Some` when `[schema_routing] enabled`;
    /// analytical (OLAP) queries are routed to the configured analytics node.
    #[cfg(feature = "schema-routing")]
    schema_analyzer: Option<Arc<crate::schema_routing::QueryAnalyzer>>,
    /// Pool manager for Session/Transaction/Statement modes
    #[cfg(feature = "pool-modes")]
    pool_manager: Option<Arc<ConnectionPoolManager>>,
    /// Data-path idle backend-connection pool. `Some` only when pooling is
    /// active (mode is Transaction or Statement); `None` leaves the 1:1
    /// session-pinned path completely unchanged. This is the raw-stream pool
    /// the data path actually leases from, keyed by `(node, user, database)`.
    #[cfg(feature = "pool-modes")]
    backend_pool: Option<Arc<crate::pool::BackendIdlePool>>,
    /// WASM plugin manager. `None` means no plugins loaded — the per-query
    /// hook path becomes a fast no-op. When `Some`, `PreQuery` / `PostQuery`
    /// hooks fire on every simple-query message.
    #[cfg(feature = "wasm-plugins")]
    plugin_manager: Option<Arc<PluginManager>>,
    /// Shared transaction journal — single sink for per-session
    /// statement journaling. The replay engine reads windows from
    /// this directly. Always present when the `ha-tr` feature is on;
    /// journaling self-disables internally when not configured.
    #[cfg(feature = "ha-tr")]
    transaction_journal: Arc<crate::transaction_journal::TransactionJournal>,
    /// Anomaly detector (T3.1). Records every query and every
    /// auth outcome; surfaces detections via /api/anomalies.
    #[cfg(feature = "anomaly-detection")]
    anomaly_detector: Arc<crate::anomaly::AnomalyDetector>,
    /// Edge cache + home registry (T3.2). Both always-present even
    /// in Home mode (the cache is a no-op there); avoids an extra
    /// Option in the hot path.
    #[cfg(feature = "edge-proxy")]
    edge_cache: Arc<crate::edge::EdgeCache>,
    #[cfg(feature = "edge-proxy")]
    edge_registry: Arc<crate::edge::EdgeRegistry>,
}

/// Node health status
#[derive(Debug, Clone)]
pub struct NodeHealth {
    /// Node address
    pub address: String,
    /// Whether node is healthy
    pub healthy: bool,
    /// Last check time
    pub last_check: chrono::DateTime<chrono::Utc>,
    /// Consecutive failures
    pub failure_count: u32,
    /// Last error message
    pub last_error: Option<String>,
    /// Average latency (ms)
    pub latency_ms: f64,
    /// Replication lag (if applicable)
    pub replication_lag_bytes: Option<u64>,
}

/// Server metrics
#[derive(Default)]
struct ServerMetrics {
    /// Total connections accepted
    connections_accepted: AtomicU64,
    /// Total connections closed
    connections_closed: AtomicU64,
    /// Total queries processed
    queries_processed: AtomicU64,
    /// Total bytes received from clients
    bytes_received: AtomicU64,
    /// Total bytes sent to clients
    bytes_sent: AtomicU64,
    /// Failover count
    failovers: AtomicU64,
}

/// Load balancer state
struct LoadBalancerState {
    /// Round-robin counter. Atomic so the read-routing path never
    /// takes a write lock just to advance the rotation.
    rr_counter: AtomicU64,
}

/// Client session
pub struct ClientSession {
    /// Session ID
    pub id: Uuid,
    /// Client address
    pub client_addr: SocketAddr,
    /// Current backend node
    pub current_node: RwLock<Option<String>>,
    /// Fast, lock-free "in a transaction" flag — the single per-query hot-path
    /// read/write of transaction state. Written from the ReadyForQuery status
    /// byte at each response boundary; read by pool-release, read-node
    /// selection, and cache-eligibility checks. This is authoritative on the
    /// data path; `tx_state` (below) retains the richer structure for TR/replay
    /// consumers but is no longer touched per query, so the relay pays no
    /// `RwLock` acquisition just to test in-transaction.
    pub in_transaction: std::sync::atomic::AtomicBool,
    /// Set while the session is mid-COPY (the backend sent CopyInResponse /
    /// CopyBothResponse and is awaiting CopyData from the client). A COPY is
    /// NOT a clean transaction boundary even though no ReadyForQuery has been
    /// seen yet, so Transaction/Statement pool release must be suppressed while
    /// it is set — otherwise the connection would be reset (`DISCARD ALL`) and
    /// parked in the middle of a copy, aborting it and hanging the client.
    /// Cleared once the COPY drains to ReadyForQuery.
    pub copy_in_progress: std::sync::atomic::AtomicBool,
    /// Rich transaction state (tx id, statement log, savepoints) for
    /// Transaction-Replay/library consumers. Not read or written on the
    /// per-query forward path — see `in_transaction` above.
    pub tx_state: RwLock<TransactionState>,
    /// Session variables
    pub variables: RwLock<HashMap<String, String>>,
    /// Created at
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// TR mode for this session
    pub tr_mode: TrMode,
    /// Wall-clock instant of this session's most recent write, for
    /// read-your-writes routing: reads within the configured window after a
    /// write are pinned to the primary so the client observes its own writes
    /// despite replica lag.
    #[cfg(feature = "lag-routing")]
    pub last_write_at: RwLock<Option<std::time::Instant>>,
    /// Client ID for pool-modes lease tracking
    #[cfg(feature = "pool-modes")]
    pub pool_client_id: ClientId,
    /// Identity returned by an `Authenticate` plugin, if any. Downstream
    /// plugins (masking, residency routing, cost governor) read this to
    /// gate per-user policy. `None` when no plugin ran or every plugin
    /// deferred to the default auth flow.
    #[cfg(feature = "wasm-plugins")]
    pub plugin_identity: RwLock<Option<Identity>>,
}

/// Transaction state
#[derive(Debug, Clone, Default)]
pub struct TransactionState {
    /// Whether in a transaction
    pub in_transaction: bool,
    /// Transaction ID
    pub tx_id: Option<Uuid>,
    /// Statements executed in current transaction
    pub statements: Vec<StatementLog>,
    /// Read-only transaction
    pub read_only: bool,
    /// Savepoints
    pub savepoints: Vec<String>,
}

/// Logged statement for TR replay
#[derive(Debug, Clone)]
pub struct StatementLog {
    /// Statement SQL
    pub sql: String,
    /// Parameters
    pub params: Vec<String>,
    /// Result checksum
    pub result_checksum: Option<u64>,
    /// Execution time
    pub executed_at: chrono::DateTime<chrono::Utc>,
}

/// A cached per-session backend connection plus the set of *named* prepared
/// statements known to be live on **this** socket.
///
/// Tying the prepared-statement set to the socket (rather than to the node
/// address) is what makes prepared statements survive a backend switch: when a
/// connection is dropped and redialed, or when a session is routed to a
/// different node, the fresh `BackendConn` starts with an empty set, so the
/// proxy transparently re-issues the original `Parse` for any named statement
/// the target connection is missing before forwarding a `Bind`/`Describe` that
/// references it (Batch F.4). The session keeps the canonical `Parse` bytes in
/// a separate registry; this set is just "what does *this* socket already
/// know".
struct BackendConn {
    stream: TcpStream,
    prepared: HashSet<String>,
    /// Signature (query text + parameter-type OIDs) of the *unnamed* prepared
    /// statement currently established on this socket, if any. When the client
    /// re-sends an identical unnamed `Parse`, the proxy can skip forwarding it
    /// (the backend's unnamed statement already holds that SQL) and synthesize
    /// the `ParseComplete` locally — the unnamed-Parse promotion (Batch H).
    unnamed_sig: Option<bytes::Bytes>,
    /// Whether a simple-query statement forwarded on this socket may have left
    /// session-level state behind (a `SET`, temp table, `LISTEN`, advisory
    /// lock, …). Used only by the conditional-reset optimisation
    /// (`pool_mode.skip_clean_reset`): a connection is eligible to be parked
    /// WITHOUT running the reset query only when it is provably clean —
    /// `!dirty && prepared.is_empty() && unnamed_sig.is_none()`. Set
    /// conservatively (any statement not provably session-neutral sets it), so
    /// the worst outcome of a misclassification is an unnecessary reset, never
    /// leaked state. Always `false` on a fresh/reused connection.
    dirty: bool,
}

impl BackendConn {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            prepared: HashSet::new(),
            unnamed_sig: None,
            dirty: false,
        }
    }
}

/// Bind a TCP listener with `SO_REUSEADDR` + `SO_REUSEPORT` so a second process
/// can bind the same address concurrently (the kernel then load-balances new
/// connections across both). This is what lets a new binary take over new
/// connections while the old one drains — used for both the client and admin
/// listeners so a binary handoff can re-bind every address (Batch H).
pub(crate) fn bind_reuseport(addr: &str) -> Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sockaddr: SocketAddr = addr
        .parse()
        .map_err(|e| ProxyError::Config(format!("invalid listen address '{}': {}", addr, e)))?;
    let domain = if sockaddr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(|e| ProxyError::Network(format!("socket(): {}", e)))?;
    socket
        .set_reuse_address(true)
        .map_err(|e| ProxyError::Network(format!("SO_REUSEADDR: {}", e)))?;
    #[cfg(all(unix, not(target_os = "solaris")))]
    socket
        .set_reuse_port(true)
        .map_err(|e| ProxyError::Network(format!("SO_REUSEPORT: {}", e)))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| ProxyError::Network(format!("set_nonblocking: {}", e)))?;
    socket
        .bind(&sockaddr.into())
        .map_err(|e| ProxyError::Network(format!("Failed to bind {}: {}", addr, e)))?;
    socket
        .listen(1024)
        .map_err(|e| ProxyError::Network(format!("listen(): {}", e)))?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
        .map_err(|e| ProxyError::Network(format!("from_std listener: {}", e)))
}

/// Disposition produced by the pre-query plugin hook stage.
///
/// When the `wasm-plugins` feature is off, only `Forward` is ever produced —
/// the hook dispatch is compiled out entirely and the variant list exists
/// purely for pattern-match symmetry.
#[derive(Debug)]
#[allow(dead_code)] // Block/Cached only constructed under wasm-plugins
enum PreQueryAction {
    /// Send the message to the backend as usual.
    Forward,
    /// A plugin blocked the query. The caller sends an error + ReadyForQuery
    /// to the client and skips backend forwarding.
    Block(String),
    /// A plugin returned a cached response. Not yet wired — response
    /// synthesis from raw bytes requires building a full protocol reply
    /// (RowDescription + DataRow(s) + CommandComplete + ReadyForQuery),
    /// which is the next step of T0-a. For now the caller falls back to
    /// `Forward` and logs a warning.
    Cached(Vec<u8>),
}

/// Override produced by the Route plugin hook. Consumed by `route_and_forward`
/// when deciding which backend to talk to.
///
/// As with `PreQueryAction`, only `None` is ever produced when the
/// `wasm-plugins` feature is off.
#[derive(Debug)]
#[allow(dead_code)] // Primary/Standby/Node/Block only constructed under wasm-plugins
enum RouteOverride {
    /// No override — use the default SQL-verb-based routing.
    None,
    /// Force the write path (use `select_primary_with_timeout`).
    Primary,
    /// Force the read path (use `select_read_node`).
    Standby,
    /// Use this exact node address. Takes precedence over the is_write
    /// heuristic; the proxy will still verify the node is healthy before
    /// connecting (via the normal switch-vs-reuse flow).
    Node(String),
    /// Reject the query: write a PG ErrorResponse + ReadyForQuery to
    /// the client and skip the forward. Carries the reason the plugin
    /// supplied. Takes precedence over every other field — the proxy
    /// short-circuits before any backend selection.
    Block(String),
}

impl ProxyServer {
    /// Build a `PluginManager` from config and preload plugins from disk.
    ///
    /// Returns `None` when plugins are disabled in config, when the
    /// runtime fails to initialise, or when the plugin directory is
    /// missing. Individual per-file load failures are logged but do not
    /// abort startup — the remaining plugins load normally and the
    /// proxy stays up.
    #[cfg(feature = "wasm-plugins")]
    fn init_plugin_manager(
        toml_cfg: &crate::config::PluginToml,
    ) -> Option<Arc<crate::plugins::PluginManager>> {
        if !toml_cfg.enabled {
            return None;
        }

        let runtime_cfg = crate::plugins::PluginRuntimeConfig::from(toml_cfg);
        let plugin_dir = runtime_cfg.plugin_dir.clone();

        let pm = match crate::plugins::PluginManager::new(runtime_cfg) {
            Ok(pm) => Arc::new(pm),
            Err(e) => {
                tracing::error!(error = %e, "Failed to create plugin manager; plugins disabled");
                return None;
            }
        };

        match std::fs::read_dir(&plugin_dir) {
            Ok(entries) => {
                let mut loaded = 0usize;
                let mut failed = 0usize;
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) != Some("wasm") {
                        continue;
                    }
                    match pm.load_plugin(&path) {
                        Ok(()) => loaded += 1,
                        Err(e) => {
                            failed += 1;
                            tracing::warn!(
                                path = %path.display(),
                                error = %e,
                                "Failed to load plugin"
                            );
                        }
                    }
                }
                tracing::info!(
                    dir = %plugin_dir.display(),
                    loaded = loaded,
                    failed = failed,
                    "Plugin loading complete"
                );
            }
            Err(e) => {
                tracing::warn!(
                    dir = %plugin_dir.display(),
                    error = %e,
                    "Plugin directory not readable; no plugins loaded"
                );
            }
        }

        Some(pm)
    }

    /// Create a new proxy server
    pub fn new(config: ProxyConfig) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        // Initialize health status
        let mut health = HashMap::new();
        for node in &config.nodes {
            health.insert(
                node.address(),
                NodeHealth {
                    address: node.address(),
                    healthy: true, // Assume healthy until proven otherwise
                    last_check: chrono::Utc::now(),
                    failure_count: 0,
                    last_error: None,
                    latency_ms: 0.0,
                    replication_lag_bytes: None,
                },
            );
        }

        // Initialize pool manager if pool-modes feature is enabled
        #[cfg(feature = "pool-modes")]
        let pool_manager = {
            use crate::pool::PreparedStatementMode as PoolPreparedStatementMode;

            let pool_config = PoolModeConfig {
                default_mode: match config.pool_mode.mode {
                    crate::config::PoolingMode::Session => PoolingMode::Session,
                    crate::config::PoolingMode::Transaction => PoolingMode::Transaction,
                    crate::config::PoolingMode::Statement => PoolingMode::Statement,
                },
                max_pool_size: config.pool_mode.max_pool_size,
                min_idle: config.pool_mode.min_idle,
                idle_timeout_secs: config.pool_mode.idle_timeout_secs,
                max_lifetime_secs: config.pool_mode.max_lifetime_secs,
                acquire_timeout_secs: config.pool_mode.acquire_timeout_secs,
                reset_query: config.pool_mode.reset_query.clone(),
                prepared_statement_mode: match config.pool_mode.prepared_statement_mode {
                    crate::config::PreparedStatementMode::Disable => {
                        PoolPreparedStatementMode::Disable
                    }
                    crate::config::PreparedStatementMode::Track => PoolPreparedStatementMode::Track,
                    crate::config::PreparedStatementMode::Named => PoolPreparedStatementMode::Named,
                },
                test_on_acquire: config.pool.test_on_acquire,
                validation_query: "SELECT 1".to_string(),
                queue_timeout_secs: 30,
                max_queue_size: 0,
            };
            Some(Arc::new(ConnectionPoolManager::new(pool_config)))
        };

        // The raw-stream data-path pool is only built when pooling is active
        // (Transaction/Statement). Session mode leaves it `None` so the hot
        // path is byte-for-byte unchanged.
        #[cfg(feature = "pool-modes")]
        let backend_pool = match config.pool_mode.mode {
            crate::config::PoolingMode::Transaction | crate::config::PoolingMode::Statement => {
                tracing::info!(
                    mode = ?config.pool_mode.mode,
                    max_idle_per_identity = config.pool_mode.max_pool_size,
                    "pool-modes: data-path connection pooling enabled"
                );
                Some(Arc::new(crate::pool::BackendIdlePool::new(
                    config.pool_mode.max_pool_size as usize,
                    Self::MAX_TOTAL_IDLE_BACKEND_CONNS,
                )))
            }
            crate::config::PoolingMode::Session => None,
        };

        // Initialize plugin manager if the wasm-plugins feature is enabled
        // AND plugins are turned on in config. Scans plugin_dir for `.wasm`
        // files and loads each; a missing directory is non-fatal and logs
        // a warning so empty deployments don't fail startup.
        #[cfg(feature = "wasm-plugins")]
        let plugin_manager = Self::init_plugin_manager(&config.plugins);

        // Build the client TLS acceptor if [tls] is configured + enabled.
        // A bad cert/key is fatal at startup (fail fast, don't silently
        // fall back to plaintext for a deployment that asked for TLS).
        let tls_acceptor = match config.tls.as_ref() {
            Some(tls) if tls.enabled => match build_tls_acceptor(tls) {
                Ok(acc) => {
                    tracing::info!(
                        mtls = tls.require_client_cert,
                        "client TLS termination enabled"
                    );
                    Some(acc)
                }
                Err(e) => {
                    return Err(ProxyError::Config(format!("TLS init failed: {}", e)));
                }
            },
            _ => None,
        };

        // Load the SCRAM auth_file when proxy-terminated auth is requested.
        // Misconfiguration is fatal at startup (fail fast).
        let auth_file = if config.auth.mode == crate::config::AuthMode::Scram {
            let path = config.auth.auth_file.as_ref().ok_or_else(|| {
                ProxyError::Config("auth mode 'scram' requires auth_file".to_string())
            })?;
            let af = crate::auth_scram::AuthFile::load(path)
                .map_err(|e| ProxyError::Config(format!("auth_file: {}", e)))?;
            tracing::info!(users = %(!af.is_empty()), "proxy SCRAM auth enabled");
            Some(Arc::new(af))
        } else {
            None
        };

        // Spawn the traffic-mirror worker when enabled (we are inside the
        // tokio runtime here — main is #[tokio::main]).
        let mirror = if config.mirror.enabled {
            tracing::info!(target = %format!("{}:{}", config.mirror.backend_host, config.mirror.backend_port),
                writes_only = config.mirror.writes_only, "traffic mirroring enabled");
            Some(crate::mirror::spawn(config.mirror.clone()))
        } else {
            None
        };

        // Build the rate limiter from the TOML config when enabled.
        #[cfg(feature = "rate-limiting")]
        let rate_limiter = if config.rate_limit.enabled {
            let rl = &config.rate_limit;
            tracing::info!(
                qps = rl.default_qps,
                burst = rl.default_burst,
                key_by = ?rl.key_by,
                "rate limiting enabled"
            );
            let rlc = crate::rate_limit::RateLimitConfig {
                enabled: true,
                default_qps: rl.default_qps,
                default_burst: rl.default_burst,
                default_concurrency: if rl.max_concurrent > 0 {
                    rl.max_concurrent
                } else {
                    crate::rate_limit::RateLimitConfig::default().default_concurrency
                },
                ..Default::default()
            };
            Some(Arc::new(crate::rate_limit::RateLimiter::new(rlc)))
        } else {
            None
        };

        // Build the per-node circuit breaker manager when enabled.
        #[cfg(feature = "circuit-breaker")]
        let circuit_breaker = if config.circuit_breaker.enabled {
            let cb = &config.circuit_breaker;
            tracing::info!(
                failure_threshold = cb.failure_threshold,
                open_secs = cb.open_secs,
                "circuit breaker enabled"
            );
            let cbc = crate::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: cb.failure_threshold,
                cooldown: Duration::from_secs(cb.open_secs),
                half_open_success_threshold: cb.success_threshold,
                ..Default::default()
            };
            let mgr = crate::circuit_breaker::CircuitBreakerManager::new(
                crate::circuit_breaker::ManagerConfig::new(cbc),
            );
            Some(Arc::new(mgr))
        } else {
            None
        };

        // Build the query-analytics engine when enabled.
        #[cfg(feature = "query-analytics")]
        let analytics = if config.analytics.enabled {
            let a = &config.analytics;
            tracing::info!(
                slow_query_ms = a.slow_query_ms,
                max_fingerprints = a.max_fingerprints,
                "query analytics enabled"
            );
            let ac = crate::analytics::AnalyticsConfig {
                enabled: true,
                max_fingerprints: a.max_fingerprints as usize,
                slow_query: crate::analytics::SlowQueryConfig {
                    threshold: Duration::from_millis(a.slow_query_ms),
                    ..Default::default()
                },
                ..Default::default()
            };
            Some(Arc::new(crate::analytics::QueryAnalytics::new(ac)))
        } else {
            None
        };

        // Build the query-result cache when enabled.
        #[cfg(feature = "query-cache")]
        let query_cache = if config.cache.enabled {
            let c = &config.cache;
            tracing::info!(
                ttl_secs = c.ttl_secs,
                max_result_bytes = c.max_result_bytes,
                "query cache enabled (L1 hot + L2 warm)"
            );
            let ttl = Duration::from_secs(c.ttl_secs);
            let cc = crate::cache::CacheConfig {
                enabled: true,
                default_ttl: ttl,
                max_result_size: c.max_result_bytes,
                l1: crate::cache::L1Config {
                    ttl,
                    ..Default::default()
                },
                l2: crate::cache::L2Config {
                    ttl,
                    ..Default::default()
                },
                ..Default::default()
            };
            Some(Arc::new(crate::cache::QueryCache::new(cc)))
        } else {
            None
        };

        // Build the SQL query rewriter from the configured rules.
        #[cfg(feature = "query-rewriting")]
        let rewriter = if config.query_rewrite.enabled && !config.query_rewrite.rules.is_empty() {
            use crate::rewriter::{
                QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation,
            };
            let rw = QueryRewriter::new(RewriterConfig {
                enabled: true,
                ..Default::default()
            });
            let mut n = 0usize;
            for (i, r) in config.query_rewrite.rules.iter().enumerate() {
                let transformation =
                    if let (Some(from), Some(to)) = (&r.match_table, &r.replace_table_with) {
                        Transformation::ReplaceTable {
                            from: from.clone(),
                            to: to.clone(),
                        }
                    } else if let Some(w) = &r.append_where {
                        Transformation::AppendWhereAnd(w.clone())
                    } else if let Some(limit) = r.add_limit {
                        Transformation::AddLimit(limit)
                    } else {
                        continue; // no transformation specified — skip
                    };
                let pattern = if let Some(t) = &r.match_table {
                    QueryPattern::Table(t.clone())
                } else if let Some(re) = &r.match_regex {
                    QueryPattern::regex(re.clone())
                } else {
                    QueryPattern::All
                };
                rw.add_rule(
                    RewriteRule::build(format!("rule-{i}"))
                        .pattern(pattern)
                        .transform(transformation)
                        .build(),
                );
                n += 1;
            }
            tracing::info!(rules = n, "query rewriting enabled");
            Some(Arc::new(rw))
        } else {
            None
        };

        // Build the multi-tenancy manager from the configured tenants.
        #[cfg(feature = "multi-tenancy")]
        let tenant_manager =
            if config.multi_tenancy.enabled && !config.multi_tenancy.tenants.is_empty() {
                use crate::multi_tenancy::{
                    IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantConfig,
                    TenantId, TenantManagerBuilder, TenantQueryTransformer,
                };
                let mt = &config.multi_tenancy;
                let identification = match mt.identify_by.as_str() {
                    "database" => IdentificationMethod::DatabaseName,
                    param => IdentificationMethod::Header {
                        header_name: param.to_string(),
                    },
                };
                let mtc = MultiTenancyConfig {
                    enabled: true,
                    identification,
                    ..Default::default()
                };
                // Configure which tables are tenant-scoped + the filter column.
                let table_refs: Vec<&str> = mt.tenant_tables.iter().map(|s| s.as_str()).collect();
                let transformer = TenantQueryTransformer::new()
                    .register_tables(&table_refs, mt.tenant_column.clone());
                let tm = TenantManagerBuilder::new()
                    .config(mtc)
                    .query_transformer(transformer)
                    .build();
                for id in &mt.tenants {
                    tm.register_tenant(TenantConfig::new(
                        TenantId::new(id.clone()),
                        IsolationStrategy::row("public", mt.tenant_column.clone()),
                    ));
                }
                tracing::info!(
                    tenants = mt.tenants.len(),
                    identify_by = %mt.identify_by,
                    "multi-tenancy enabled"
                );
                Some(Arc::new(tm))
            } else {
                None
            };

        // Build the schema/workload query analyzer when enabled.
        #[cfg(feature = "schema-routing")]
        let schema_analyzer =
            if config.schema_routing.enabled && !config.schema_routing.analytics_node.is_empty() {
                tracing::info!(
                    analytics_node = %config.schema_routing.analytics_node,
                    "schema/workload routing enabled (OLAP -> analytics node)"
                );
                let registry = Arc::new(crate::schema_routing::SchemaRegistry::new());
                Some(Arc::new(crate::schema_routing::QueryAnalyzer::new(
                    registry,
                )))
            } else {
                None
            };

        let state = Arc::new(ServerState {
            sessions: RwLock::new(HashMap::new()),
            health: ArcSwap::from_pointee(health),
            health_write: parking_lot::Mutex::new(()),
            live_config: ArcSwap::from_pointee(config.clone()),
            metrics: ServerMetrics::default(),
            cancel_map: Arc::new(DashMap::new()),
            cancel_order: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
            tls_acceptor,
            auth_file,
            mirror,
            cutover: Arc::new(ArcSwap::from_pointee(None)),
            lb_state: LoadBalancerState {
                rr_counter: AtomicU64::new(0),
            },
            #[cfg(feature = "routing-hints")]
            hint_parser: if config.routing_hints.enabled {
                tracing::info!(
                    strip = config.routing_hints.strip_hints,
                    "SQL-comment routing hints enabled"
                );
                Some(if config.routing_hints.strip_hints {
                    crate::routing::HintParser::new()
                } else {
                    crate::routing::HintParser::without_stripping()
                })
            } else {
                None
            },
            #[cfg(feature = "rate-limiting")]
            rate_limiter,
            #[cfg(feature = "circuit-breaker")]
            circuit_breaker,
            #[cfg(feature = "query-analytics")]
            analytics,
            #[cfg(feature = "query-cache")]
            query_cache,
            #[cfg(feature = "query-rewriting")]
            rewriter,
            #[cfg(feature = "multi-tenancy")]
            tenant_manager,
            #[cfg(feature = "schema-routing")]
            schema_analyzer,
            #[cfg(feature = "pool-modes")]
            pool_manager,
            #[cfg(feature = "pool-modes")]
            backend_pool,
            #[cfg(feature = "wasm-plugins")]
            plugin_manager,
            #[cfg(feature = "ha-tr")]
            transaction_journal: Arc::new(crate::transaction_journal::TransactionJournal::new()),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(crate::anomaly::AnomalyDetector::new(
                crate::anomaly::AnomalyConfig::default(),
            )),
            #[cfg(feature = "edge-proxy")]
            edge_cache: Arc::new(crate::edge::EdgeCache::new(10_000)),
            #[cfg(feature = "edge-proxy")]
            edge_registry: Arc::new(crate::edge::EdgeRegistry::new(
                32,
                std::time::Duration::from_secs(120),
            )),
        });

        Ok(Self {
            config,
            state,
            shutdown_tx,
            config_path: None,
        })
    }

    /// Record the config file path so `SIGHUP` can re-read it for a live
    /// reload (Batch H). Without a path (config built from CLI flags/defaults)
    /// a `SIGHUP` is logged and ignored — there is nothing to re-read.
    pub fn with_config_path(mut self, path: Option<String>) -> Self {
        self.config_path = path;
        self
    }

    /// A stream that yields once per `SIGHUP`. On non-Unix platforms it never
    /// yields (config reload is Unix-signal driven).
    #[cfg(unix)]
    fn hangup_stream() -> tokio::signal::unix::Signal {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .expect("failed to install SIGHUP handler")
    }
    #[cfg(not(unix))]
    fn hangup_stream() -> HangupNever {
        HangupNever
    }

    /// A stream that yields once per `SIGUSR2` — the graceful binary-handoff
    /// drain trigger. Never yields on non-Unix platforms.
    #[cfg(unix)]
    fn usr2_stream() -> tokio::signal::unix::Signal {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())
            .expect("failed to install SIGUSR2 handler")
    }
    #[cfg(not(unix))]
    fn usr2_stream() -> HangupNever {
        HangupNever
    }

    /// Wait for in-flight client connections to finish, up to `timeout`. Used by
    /// the graceful drain after the listener is closed — the session map is the
    /// live active-connection gauge (one entry per accepted connection).
    async fn drain_connections(state: &Arc<ServerState>, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let active = state.sessions.read().await.len();
            if active == 0 {
                tracing::info!("drain complete — all in-flight connections finished");
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    active,
                    "drain timeout reached — exiting with connections still open"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Graceful-drain timeout: how long to keep serving in-flight connections
    /// after SIGUSR2 before exiting. Sourced from `shutdown_drain_timeout_secs`
    /// in the live config, with the `HELIOS_DRAIN_TIMEOUT_SECS` env var as a
    /// runtime override.
    fn drain_timeout(config_secs: u64) -> Duration {
        let secs = std::env::var("HELIOS_DRAIN_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(config_secs);
        Duration::from_secs(secs)
    }

    /// Re-read the config file and hot-swap the live config (Batch H).
    ///
    /// New connections immediately use the reloaded config; in-flight sessions
    /// keep the snapshot they began with, so nothing is dropped. A parse error
    /// keeps the running config untouched. Socket-bound fields (listen/admin
    /// address) cannot change on an already-bound listener and are reported but
    /// not applied. The node set is reconciled into the health map so routing
    /// sees additions/removals at once.
    async fn reload_config(&self) {
        let Some(path) = self.config_path.as_deref() else {
            tracing::warn!(
                "SIGHUP received but config was not loaded from a file — nothing to reload"
            );
            return;
        };
        tracing::info!(path, "SIGHUP: reloading configuration");
        let new_config = match ProxyConfig::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(path, error = %e, "SIGHUP reload failed to parse — keeping current config");
                return;
            }
        };
        let old = self.state.live_config.load_full();
        if new_config.listen_address != old.listen_address {
            tracing::warn!(old = %old.listen_address, new = %new_config.listen_address,
                "listen_address change needs a restart/handoff; the bound socket is kept");
        }
        if new_config.admin_address != old.admin_address {
            tracing::warn!(old = %old.admin_address, new = %new_config.admin_address,
                "admin_address change needs a restart; the bound socket is kept");
        }
        // Reconcile node health to the new node set before publishing the
        // config, so the first connection on the new config can route to it.
        Self::reconcile_health(&self.state, &new_config);
        let nodes = new_config.nodes.len();
        let hba_rules = new_config.hba.len();
        let pool_max = new_config.pool.max_connections;
        self.state.live_config.store(Arc::new(new_config));
        tracing::info!(
            nodes,
            hba_rules,
            pool_max,
            "SIGHUP: configuration reloaded — applies to new connections"
        );
    }

    /// Rebuild the health map for `config`'s node set: surviving nodes keep
    /// their current health; new nodes are seeded healthy (immediately
    /// routable, the next check confirms); removed nodes are dropped.
    fn reconcile_health(state: &Arc<ServerState>, config: &ProxyConfig) {
        // Serialize against the periodic checker and in-band demotions so this
        // rebuild neither clobbers nor is clobbered by a concurrent write.
        let _writers = state.health_write.lock();
        let current = state.health.load_full();
        let mut next: HashMap<String, NodeHealth> = HashMap::new();
        for node in &config.nodes {
            let addr = node.address();
            match current.get(&addr) {
                Some(existing) => {
                    next.insert(addr, existing.clone());
                }
                None => {
                    tracing::info!(node = %addr, "SIGHUP: new node added — seeding healthy");
                    next.insert(
                        addr.clone(),
                        NodeHealth {
                            address: addr,
                            healthy: true,
                            last_check: chrono::Utc::now(),
                            failure_count: 0,
                            last_error: None,
                            latency_ms: 0.0,
                            replication_lag_bytes: None,
                        },
                    );
                }
            }
        }
        for gone in current.keys().filter(|k| !next.contains_key(*k)) {
            tracing::info!(node = %gone, "SIGHUP: node removed from config");
        }
        state.health.store(Arc::new(next));
    }

    /// Run the proxy server
    pub async fn run(&self) -> Result<()> {
        // Bind with SO_REUSEPORT so a freshly-started binary can bind the SAME
        // listen address concurrently — the kernel load-balances new
        // connections across both processes. That is the mechanism behind the
        // zero-downtime binary handoff: start the new binary, then SIGUSR2 the
        // old one to close its listener and drain (Batch H, item 84).
        let listener = bind_reuseport(&self.config.listen_address)?;

        tracing::info!(
            "Proxy listening on {} (SO_REUSEPORT)",
            self.config.listen_address
        );

        // Start background tasks
        let health_task = self.spawn_health_checker();
        let pool_task = self.spawn_pool_manager();

        // Start admin server
        let admin_task = self.spawn_admin_server();

        // Start the MCP agent gateway when enabled.
        let mcp_task = if self.config.mcp.enabled {
            let mcp_cfg = self.config.mcp.clone();
            // Resolve the configured agent contract (scoped grants) by id.
            let contract = mcp_cfg.contract.as_ref().and_then(|id| {
                let found = self.config.agent_contracts.iter().find(|c| &c.id == id).cloned();
                if found.is_none() {
                    tracing::warn!(%id, "mcp.contract names an unknown agent_contract; gateway runs with only the read-only guardrail");
                }
                found
            });
            Some(tokio::spawn(async move {
                if let Err(e) = crate::mcp::McpServer::new(mcp_cfg, contract).run().await {
                    tracing::error!("MCP gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Start the HTTP SQL gateway (Neon-serverless compatible) when enabled.
        let http_gw_task = if self.config.http_gateway.enabled {
            let gw_cfg = self.config.http_gateway.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = crate::http_gateway::HttpGateway::new(gw_cfg).run().await {
                    tracing::error!("HTTP gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Start the GraphQL-to-SQL gateway when enabled.
        #[cfg(feature = "graphql-gateway")]
        let _graphql_gw_task = if self.config.graphql_gateway.enabled {
            let gw_cfg = self.config.graphql_gateway.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = crate::graphql_gateway::GraphqlGateway::new(gw_cfg)
                    .run()
                    .await
                {
                    tracing::error!("GraphQL gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // SIGHUP -> zero-downtime config reload; SIGUSR2 -> graceful drain for
        // binary handoff (Batch H). On platforms without Unix signals these are
        // simply never readable.
        let mut sighup = Self::hangup_stream();
        let mut sigusr2 = Self::usr2_stream();
        let mut graceful = false;

        loop {
            tokio::select! {
                _ = sighup.recv() => {
                    self.reload_config().await;
                }
                _ = sigusr2.recv() => {
                    tracing::info!(
                        "SIGUSR2: graceful binary-handoff drain — closing the listener so new \
                         connections route to the sibling process; finishing in-flight connections"
                    );
                    graceful = true;
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            // PG wire traffic is small request/response
                            // frames; Nagle + delayed-ACK costs tens of
                            // ms per round-trip if left on.
                            let _ = stream.set_nodelay(true);
                            self.state.metrics.connections_accepted.fetch_add(1, Ordering::Relaxed);
                            let state = self.state.clone();
                            // Snapshot the *live* config so a SIGHUP reload
                            // applies to new connections; in-flight sessions
                            // keep the snapshot they began with (Batch H).
                            let config = (*self.state.live_config.load_full()).clone();
                            let shutdown_tx = self.shutdown_tx.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_client(stream, addr, state, config, shutdown_tx).await {
                                    tracing::error!("Client handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Shutdown signal received");
                    break;
                }
            }
        }

        // Close the listening socket so the kernel stops routing new connections
        // to this process's accept queue (with SO_REUSEPORT they would otherwise
        // sit unaccepted) — all new connections now go to the sibling listener.
        drop(listener);

        // On a graceful handoff, keep serving in-flight connections until they
        // finish (or the drain deadline), so nothing in flight is dropped.
        if graceful {
            let timeout =
                Self::drain_timeout(self.state.live_config.load().shutdown_drain_timeout_secs);
            tracing::info!(
                timeout_secs = timeout.as_secs(),
                "draining in-flight connections"
            );
            Self::drain_connections(&self.state, timeout).await;
        }

        // Wait for background tasks
        health_task.abort();
        pool_task.abort();
        admin_task.abort();
        if let Some(t) = mcp_task {
            t.abort();
        }
        if let Some(t) = http_gw_task {
            t.abort();
        }

        Ok(())
    }

    /// Spawn admin API server
    fn spawn_admin_server(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Create admin state
            let admin_state = Arc::new(AdminState::new());

            // Initialize config snapshot
            {
                let mut snapshot = admin_state.config_snapshot.write().await;
                *snapshot = ConfigSnapshot {
                    listen_address: config.listen_address.clone(),
                    admin_address: config.admin_address.clone(),
                    tr_enabled: config.tr_enabled,
                    tr_mode: format!("{:?}", config.tr_mode),
                    pool_min_connections: config.pool.min_connections,
                    pool_max_connections: config.pool.max_connections,
                    nodes: config
                        .nodes
                        .iter()
                        .map(|n| NodeSnapshot {
                            address: n.address(),
                            role: format!("{:?}", n.role),
                            weight: n.weight,
                            enabled: n.enabled,
                        })
                        .collect(),
                };
            }

            // Set proxy config for SQL routing
            admin_state.set_proxy_config(config.clone()).await;

            // Require a Bearer token on admin requests when configured.
            admin_state
                .with_auth_token(config.admin_token.clone())
                .await;

            // Branch-database provisioning surface.
            if config.branch.enabled {
                admin_state.with_branch(config.branch.clone()).await;
            }

            // Surface traffic-mirror / migration status when mirroring is on.
            if let Some(ref mirror) = state.mirror {
                admin_state
                    .with_migration(crate::admin::MigrationInfo {
                        target: mirror.target().to_string(),
                        writes_only: mirror.writes_only(),
                        metrics: mirror.metrics.clone(),
                        config: config.mirror.clone(),
                        cutover: state.cutover.clone(),
                        cutover_target: crate::mirror::CutoverTarget {
                            addr: format!(
                                "{}:{}",
                                config.mirror.backend_host, config.mirror.backend_port
                            ),
                            user: config.mirror.backend_user.clone(),
                            password: config.mirror.backend_password.clone(),
                            database: config.mirror.backend_database.clone(),
                        },
                    })
                    .await;
            }

            // Attach the plugin manager so /plugins + the admin UI
            // surface real loaded modules. Cheap Arc-clone — no
            // duplicate state, both AdminState and ServerState hold
            // the same manager.
            #[cfg(feature = "wasm-plugins")]
            if let Some(ref pm) = state.plugin_manager {
                admin_state.with_plugin_manager(pm.clone()).await;
            }

            // Attach the pool manager so /api/pools surfaces real per-node
            // pool statistics instead of an empty list.
            #[cfg(feature = "pool-modes")]
            if let Some(ref pm) = state.pool_manager {
                admin_state.with_pool_manager(pm.clone()).await;
            }

            // Attach the circuit-breaker manager so /api/circuit reports live
            // per-node breaker state.
            #[cfg(feature = "circuit-breaker")]
            if let Some(ref cb) = state.circuit_breaker {
                admin_state.with_circuit_breaker(cb.clone()).await;
            }

            // Attach the time-travel replay engine. The engine reads
            // windows from the shared TransactionJournal and replays
            // statements against a target backend supplied per-request.
            // Per-call credential overrides land via FU-21's
            // ReplayRequestBody.target_user / target_password /
            // target_database fields.
            #[cfg(feature = "ha-tr")]
            {
                let template = build_replay_backend_template(&config);
                let engine = Arc::new(crate::replay::ReplayEngine::new(
                    state.transaction_journal.clone(),
                    template,
                ));
                admin_state.with_replay_engine(engine).await;
            }

            // Attach the anomaly detector — same Arc the server
            // populates from the query path. /api/anomalies polls
            // this for surfaced detections.
            #[cfg(feature = "anomaly-detection")]
            admin_state
                .with_anomaly_detector(state.anomaly_detector.clone())
                .await;

            // Attach the query-analytics engine so /api/analytics can read it.
            #[cfg(feature = "query-analytics")]
            if let Some(a) = state.analytics.as_ref() {
                admin_state.with_analytics(a.clone()).await;
            }

            // Attach the edge cache + registry. Both surfaced via
            // /api/edge/* admin routes.
            #[cfg(feature = "edge-proxy")]
            admin_state
                .with_edge(state.edge_cache.clone(), state.edge_registry.clone())
                .await;

            // Create admin server
            let admin_server = AdminServer::new(config.admin_address.clone(), admin_state.clone());

            // Spawn state sync task
            let admin_state_sync = admin_state.clone();
            let server_state = state.clone();
            let sync_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;

                    // Sync health status
                    {
                        let health = server_state.health.load_full();
                        let mut admin_health = admin_state_sync.node_health.write().await;
                        *admin_health = (*health).clone();
                    }

                    // Sync metrics
                    {
                        let metrics = ServerMetricsSnapshot {
                            connections_accepted: server_state
                                .metrics
                                .connections_accepted
                                .load(Ordering::Relaxed),
                            connections_closed: server_state
                                .metrics
                                .connections_closed
                                .load(Ordering::Relaxed),
                            queries_processed: server_state
                                .metrics
                                .queries_processed
                                .load(Ordering::Relaxed),
                            bytes_received: server_state
                                .metrics
                                .bytes_received
                                .load(Ordering::Relaxed),
                            bytes_sent: server_state.metrics.bytes_sent.load(Ordering::Relaxed),
                            failovers: server_state.metrics.failovers.load(Ordering::Relaxed),
                        };
                        let mut admin_metrics = admin_state_sync.metrics.write().await;
                        *admin_metrics = metrics;
                    }

                    // Sync session count
                    {
                        let sessions = server_state.sessions.read().await;
                        let mut admin_sessions = admin_state_sync.active_sessions.write().await;
                        *admin_sessions = sessions.len() as u64;
                    }
                }
            });

            // Run admin server
            tokio::select! {
                result = admin_server.run() => {
                    if let Err(e) = result {
                        tracing::error!("Admin server error: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Admin server shutting down");
                }
            }

            sync_task.abort();
        })
    }

    /// Handle a client connection
    async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        state: Arc<ServerState>,
        config: ProxyConfig,
        _shutdown_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        tracing::debug!("New client connection from {}", addr);

        // Create session
        let session = Arc::new(ClientSession {
            id: Uuid::new_v4(),
            client_addr: addr,
            current_node: RwLock::new(None),
            in_transaction: std::sync::atomic::AtomicBool::new(false),
            copy_in_progress: std::sync::atomic::AtomicBool::new(false),
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: config.tr_mode,
            #[cfg(feature = "lag-routing")]
            last_write_at: RwLock::new(None),
            #[cfg(feature = "pool-modes")]
            pool_client_id: ClientId::new(),
            #[cfg(feature = "wasm-plugins")]
            plugin_identity: RwLock::new(None),
        });

        // Register session
        {
            let mut sessions = state.sessions.write().await;
            sessions.insert(session.id, session.clone());
        }

        // Negotiate client TLS (if the client sent SSLRequest). Produces a
        // ClientStream that is plaintext or TLS-wrapped; the rest of the
        // session is written against that single stream type. `pre` carries
        // a first startup/cancel message already read while peeking.
        //
        // Bound the pre-auth negotiation (first-message read + TLS handshake) in
        // time: a client that connects and then stalls must not pin this task
        // and its session-map slot indefinitely (slow-loris). The query loop
        // that follows is intentionally NOT under this deadline — only the
        // handshake is.
        let negotiated = match tokio::time::timeout(
            Self::STARTUP_TIMEOUT,
            Self::negotiate_client_tls(stream, &state),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!(client = %addr, "pre-auth negotiation timed out; closing");
                Err(ProxyError::Connection(
                    "startup negotiation timeout".to_string(),
                ))
            }
        };
        let result = match negotiated {
            Ok((mut client_stream, pre)) => {
                Self::client_loop(&mut client_stream, pre, &session, &state, &config).await
            }
            Err(e) => Err(e),
        };

        // Cleanup session
        {
            let mut sessions = state.sessions.write().await;
            sessions.remove(&session.id);
        }

        // Drop this session's per-connection L1 query cache. The cache keys L1
        // caches by connection id (the session's first u64), and without this
        // every session that ran one cacheable SELECT would leak its L1 cache
        // (up to hundreds of entries) forever — an unbounded leak under
        // connection churn. TTL only evicts on access, and an abandoned cache
        // is never accessed again, so teardown is the only reclaim point.
        #[cfg(feature = "query-cache")]
        if let Some(ref qc) = state.query_cache {
            qc.remove_l1_cache(session.id.as_u64_pair().0);
        }

        // Release any active pool lease if pool-modes is enabled
        #[cfg(feature = "pool-modes")]
        if let Some(ref pool_manager) = state.pool_manager {
            // Check if there's an active lease for this client and release it
            if pool_manager.has_active_lease(&session.pool_client_id) {
                tracing::debug!(
                    "Releasing pool lease for disconnecting client {:?}",
                    session.pool_client_id
                );
                // Note: The lease is released implicitly when the connection closes
                // The pool manager will clean up any orphaned leases
            }
        }

        state
            .metrics
            .connections_closed
            .fetch_add(1, Ordering::Relaxed);

        result
    }

    /// Main client processing loop with full PostgreSQL protocol handling
    async fn client_loop(
        stream: &mut ClientStream,
        pre: Option<StartupMessage>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<()> {
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(8192);

        // Handle startup phase. The session keeps a per-node cache of
        // authenticated backend connections (`conns`) instead of a single
        // stream: when read/write routing moves a session between primary
        // and standby it now reuses the already-authenticated connection to
        // each node rather than dropping the socket and paying a fresh TCP
        // connect + startup + SCRAM handshake on every switch (Batch C).
        // Connections are authenticated with the client's own credentials
        // (auth is pass-through). In Transaction/Statement pooling mode they
        // are returned to a shared, identity-keyed idle pool at each
        // transaction boundary (DISCARD ALL reset on release) and reused by the
        // next same-identity acquisition — see `release_to_pool_if_idle` /
        // `ensure_conn`. The first connection of a session is still
        // established through the authenticated startup path; drawing the
        // startup connection from the pool (to reduce *concurrent* backend
        // connections below the client count) additionally needs
        // proxy-terminated backend auth and is the documented next increment.
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        // Bound the startup/authentication exchange in time (the TLS path reads
        // the real startup packet here, after negotiation). A client that opens
        // the connection but never completes startup must not hold the task and
        // its session slot open indefinitely.
        let startup_result = match tokio::time::timeout(
            Self::STARTUP_TIMEOUT,
            Self::handle_startup(stream, &mut buffer, &codec, pre, session, state, config),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(ProxyError::Connection("startup timeout".to_string())),
        };
        let mut current_node: Option<String> = match startup_result {
            Ok((Some(stream_conn), node_addr)) => {
                conns.insert(node_addr.clone(), BackendConn::new(stream_conn));
                Some(node_addr)
            }
            Ok((None, _)) => {
                // SSL rejected or cancel request, connection should close
                return Ok(());
            }
            Err(e) => {
                tracing::error!("Startup failed: {}", e);
                // Send error to client
                let err_msg =
                    Self::create_error_response("08006", &format!("Startup failed: {}", e));
                let _ = stream.write_all(&err_msg).await;
                return Err(e);
            }
        };

        // Main query loop.
        //
        // Two wire shapes are handled. Simple-query (`Query`) messages are
        // self-contained: route, forward, and stream the response back
        // frame-by-frame until ReadyForQuery. Extended-protocol messages
        // (`Parse`/`Bind`/`Describe`/`Execute`/`Close`) carry no response of
        // their own until the client sends `Sync` (or `Flush`), so they are
        // accumulated into `pending` and forwarded as one batch at that
        // boundary — this is what stops the per-message 30s backend-read
        // timeout that made every prepared-statement driver unusable. The
        // routing decision for an extended batch is taken from the SQL in its
        // first `Parse`; a batch with no `Parse` (a re-`Bind`/`Execute` of a
        // named prepared statement) stays on the connection the statement was
        // prepared on.
        let mut pending = BytesMut::new();
        let mut pending_route_sql: Option<String> = None;
        // Prepared-statement tracking (Batch F.4). `stmt_registry` is the
        // session's canonical record of every *named* `Parse` the client has
        // issued (name -> full Parse message bytes) so the proxy can re-prepare
        // a statement on any backend connection that is missing it. `batch_*`
        // accumulate, for the in-flight extended batch, which named statements
        // it defines (Parse), references (Bind/Describe-S), and closes
        // (Close-S) — resolved at the Sync/Flush boundary.
        let mut stmt_registry: HashMap<String, bytes::Bytes> = HashMap::new();
        // Running sum of the bytes held in `stmt_registry`, kept in step with
        // it so the aggregate-size cap is O(1) per Parse (see MAX_PREPARED_BYTES).
        let mut stmt_registry_bytes: usize = 0;
        let mut batch_defines: Vec<String> = Vec::new();
        let mut batch_refs: Vec<String> = Vec::new();
        let mut batch_closes: Vec<String> = Vec::new();
        // Unnamed-`Parse` promotion (Batch H). `held_unnamed` parks an unnamed
        // Parse that is the FIRST message of a batch (so the batch stays the
        // clean Parse→Bind→…→Sync shape) — it is NOT appended to `pending`; the
        // decision to forward or skip it is taken at the batch boundary once the
        // target connection is known. Holds (full Parse message, signature).
        let promote_unnamed = config.optimize_unnamed_parse;
        let mut held_unnamed: Option<(bytes::Bytes, bytes::Bytes)> = None;
        loop {
            // Read the client's next message directly into the accumulation
            // buffer (no intermediate zeroed scratch, no extra copy). `read_buf`
            // appends into `buffer`'s spare capacity and advances its length.
            //
            // While waiting, ALSO watch the session's current backend connection
            // so unsolicited backend traffic — LISTEN/NOTIFY notifications,
            // NoticeResponse, ParameterStatus, and the delayed tail of a `Flush`
            // response — is relayed to the client promptly instead of sitting
            // unread until the next query, and a backend that dies while the
            // session is idle is noticed at once. At the top of the loop the
            // backend is always quiescent (every query response is fully drained
            // before we return here), so any bytes it produces are out-of-band
            // and are relayed verbatim. A mid-COPY backend is excluded — it is
            // legitimately awaiting CopyData, which the client drives.
            buffer.reserve(16384);
            let watch_node: Option<String> = if session
                .copy_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                None
            } else {
                current_node.clone().filter(|node| conns.contains_key(node))
            };
            let n: usize = 'client_read: {
                let Some(node) = watch_node.as_deref() else {
                    break 'client_read stream
                        .read_buf(&mut buffer)
                        .await
                        .map_err(|e| ProxyError::Network(format!("Read error: {}", e)))?;
                };
                loop {
                    let mut backend_gone = false;
                    let mut client_bytes: Option<usize> = None;
                    {
                        let bc = conns.get_mut(node).expect("watch_node is in conns");
                        let mut abuf = [0u8; 16384];
                        tokio::select! {
                            r = stream.read_buf(&mut buffer) => {
                                client_bytes = Some(r.map_err(|e| {
                                    ProxyError::Network(format!("Read error: {}", e))
                                })?);
                            }
                            r = bc.stream.read(&mut abuf) => match r {
                                Ok(0) => backend_gone = true,
                                Ok(bn) => {
                                    stream.write_all(&abuf[..bn]).await.map_err(|e| {
                                        ProxyError::Network(format!("Client write error: {}", e))
                                    })?;
                                    state.metrics.bytes_sent.fetch_add(bn as u64, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    tracing::debug!(node = %node, error = %e, "backend read error while idle; dropping cached connection");
                                    backend_gone = true;
                                }
                            },
                        }
                    }
                    if backend_gone {
                        // Drop the dead cached connection but keep the client
                        // session alive — the next query redials. (Mid-transaction
                        // the next forward fails and surfaces the error.)
                        conns.remove(node);
                        if current_node.as_deref() == Some(node) {
                            current_node = None;
                        }
                        break 'client_read stream
                            .read_buf(&mut buffer)
                            .await
                            .map_err(|e| ProxyError::Network(format!("Read error: {}", e)))?;
                    }
                    if let Some(cn) = client_bytes {
                        break 'client_read cn;
                    }
                    // Otherwise we relayed async backend bytes; keep watching.
                }
            };

            if n == 0 {
                // Client disconnected
                break;
            }

            state
                .metrics
                .bytes_received
                .fetch_add(n as u64, Ordering::Relaxed);

            // Bound a single in-flight message: refuse before the accumulation
            // buffer for one (possibly malicious) oversized frame can exhaust
            // memory. A legitimate client never needs a single >64 MiB message.
            if buffer.len() > Self::MAX_PENDING_BYTES {
                let emsg =
                    Self::create_error_response("53400", "message exceeds per-session size limit");
                let _ = stream.write_all(&emsg).await;
                let _ = stream.write_all(&Self::create_ready_for_query(b'I')).await;
                tracing::warn!(
                    client = %session.client_addr,
                    bytes = buffer.len(),
                    "inbound message exceeds size cap; closing connection"
                );
                return Ok(());
            }

            // Process all complete messages in buffer
            while let Some(msg) = codec.decode_message(&mut buffer)? {
                match msg.msg_type {
                    MessageType::Terminate => return Ok(()),

                    // ---- Simple query protocol ----
                    MessageType::Query => {
                        // Anomaly detector — record every Query message before
                        // the plugin hook so a detection lands in the audit
                        // trail even if a plugin later blocks.
                        #[cfg(feature = "anomaly-detection")]
                        Self::record_anomaly_observation(&msg, state, session);

                        // Plugin pre-query hook — may rewrite the SQL, block,
                        // or return a cached response.
                        let (msg, action) = Self::apply_pre_query_hook(msg, state, session);

                        if let PreQueryAction::Block(reason) = &action {
                            tracing::info!(reason = %reason, "pre-query plugin blocked query");
                            Self::send_block_response(stream, reason, state).await?;
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        #[cfg(feature = "wasm-plugins")]
                        if let PreQueryAction::Cached(bytes) = &action {
                            match Self::synthesise_cached_response(bytes) {
                                Ok(reply) => {
                                    stream.write_all(&reply).await.map_err(|e| {
                                        ProxyError::Network(format!("Write error: {}", e))
                                    })?;
                                    state
                                        .metrics
                                        .bytes_sent
                                        .fetch_add(reply.len() as u64, Ordering::Relaxed);
                                    state
                                        .metrics
                                        .queries_processed
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "failed to synthesise cached response; falling back to backend");
                                }
                            }
                        }

                        // Traffic mirror: offer the (final, post-rewrite)
                        // statement to the secondary backend. Non-blocking —
                        // never delays the client path.
                        if let Some(ref mirror) = state.mirror {
                            if let Some(sql) = crate::protocol::query_text(&msg.payload) {
                                mirror.offer(sql, Self::is_write_query(sql));
                            }
                        }

                        #[cfg(feature = "wasm-plugins")]
                        let forward_start = std::time::Instant::now();
                        let fr = Self::forward_simple_query(
                            stream,
                            &msg,
                            &mut conns,
                            current_node.as_deref(),
                            session,
                            state,
                            config,
                        )
                        .await;
                        #[cfg(feature = "wasm-plugins")]
                        Self::fire_post_query_hook(
                            &msg,
                            session,
                            state,
                            &fr,
                            forward_start.elapsed(),
                        );
                        let (used_node, sent) = fr?;
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        // Transaction/Statement pooling: park the connection
                        // back to the shared pool once the session is idle.
                        #[cfg(feature = "pool-modes")]
                        Self::release_to_pool_if_idle(
                            &mut conns,
                            current_node.as_deref(),
                            session,
                            state,
                            config,
                        )
                        .await;
                        state.metrics.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                        state
                            .metrics
                            .queries_processed
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    // ---- Extended query protocol: accumulate until Sync/Flush ----
                    MessageType::Parse
                    | MessageType::Bind
                    | MessageType::Describe
                    | MessageType::Execute
                    | MessageType::Close => {
                        // Whether this message is appended to `pending`. An
                        // unnamed Parse held aside for promotion is the lone
                        // exception (resolved at the batch boundary).
                        let mut add_to_pending = true;
                        match msg.msg_type {
                            MessageType::Parse => {
                                // Register named statements so they can be
                                // re-prepared on a different backend later, and
                                // borrow the query (2nd cstring) for routing.
                                let name = Self::parse_stmt_name(&msg.payload);
                                let unnamed = name.is_empty();
                                if !unnamed {
                                    let name = name.to_string();
                                    let existed = stmt_registry.contains_key(&name);
                                    // Cap distinct prepared statements per session so a
                                    // client issuing unbounded named `Parse`s can't grow
                                    // `stmt_registry` without limit.
                                    if !existed
                                        && stmt_registry.len() >= Self::MAX_PREPARED_STATEMENTS
                                    {
                                        let emsg = Self::create_error_response(
                                            "54000",
                                            "too many prepared statements for this session",
                                        );
                                        let _ = stream.write_all(&emsg).await;
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                        tracing::warn!(
                                            client = %session.client_addr,
                                            limit = Self::MAX_PREPARED_STATEMENTS,
                                            "prepared-statement cap exceeded; closing connection"
                                        );
                                        return Ok(());
                                    }
                                    let encoded = msg.encode().freeze();
                                    // Bound the AGGREGATE bytes retained, not just the
                                    // count: a (possibly re-Parsed) statement that would
                                    // push the session over the byte cap is refused.
                                    let old_len =
                                        stmt_registry.get(&name).map(|b| b.len()).unwrap_or(0);
                                    let projected =
                                        stmt_registry_bytes.saturating_sub(old_len) + encoded.len();
                                    if projected > Self::MAX_PREPARED_BYTES {
                                        let emsg = Self::create_error_response(
                                            "54000",
                                            "prepared-statement memory limit exceeded for this session",
                                        );
                                        let _ = stream.write_all(&emsg).await;
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                        tracing::warn!(
                                            client = %session.client_addr,
                                            limit = Self::MAX_PREPARED_BYTES,
                                            "prepared-statement byte cap exceeded; closing connection"
                                        );
                                        return Ok(());
                                    }
                                    stmt_registry.insert(name.clone(), encoded);
                                    stmt_registry_bytes = projected;
                                    batch_defines.push(name);
                                }
                                if pending_route_sql.is_none() {
                                    if let Some(end) = msg.payload.iter().position(|&b| b == 0) {
                                        if let Some(q) =
                                            crate::protocol::query_text(&msg.payload[end + 1..])
                                        {
                                            if !q.is_empty() {
                                                pending_route_sql = Some(q.to_string());
                                                #[cfg(feature = "anomaly-detection")]
                                                Self::record_anomaly_sql(q, state, session);
                                            }
                                        }
                                    }
                                }
                                // Promotion: park an unnamed Parse that opens a
                                // fresh batch. Its signature is the payload after
                                // the empty statement-name NUL (query + param
                                // types). Anything that breaks the clean shape
                                // (a second Parse, a non-empty `pending`) un-parks
                                // it back into `pending` to preserve wire order.
                                if promote_unnamed
                                    && unnamed
                                    && pending.is_empty()
                                    && held_unnamed.is_none()
                                {
                                    let sig = bytes::Bytes::copy_from_slice(&msg.payload[1..]);
                                    held_unnamed = Some((msg.encode().freeze(), sig));
                                    add_to_pending = false;
                                } else if let Some((held_msg, _)) = held_unnamed.take() {
                                    let mut combined =
                                        BytesMut::with_capacity(held_msg.len() + pending.len());
                                    combined.extend_from_slice(&held_msg);
                                    combined.extend_from_slice(&pending);
                                    pending = combined;
                                }
                            }
                            MessageType::Bind => {
                                if let Some(name) = Self::bind_stmt_ref(&msg.payload) {
                                    batch_refs.push(name.to_string());
                                }
                            }
                            MessageType::Describe => {
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    batch_refs.push(name.to_string());
                                }
                            }
                            MessageType::Close => {
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    batch_closes.push(name.to_string());
                                }
                            }
                            _ => {}
                        }
                        if add_to_pending {
                            pending.extend_from_slice(&msg.encode());
                        }
                    }

                    // ---- Extended batch boundary ----
                    MessageType::Sync | MessageType::Flush => {
                        let wait_ready = msg.msg_type == MessageType::Sync;
                        pending.extend_from_slice(&msg.encode());
                        let batch = pending.split().freeze();
                        // Re-prepare any named statement this batch references
                        // but does not itself define, in case the target
                        // connection (after a switch/redial) is missing it.
                        let reprepare: Vec<String> = batch_refs
                            .iter()
                            .filter(|r| !batch_defines.contains(r))
                            .cloned()
                            .collect();
                        let (used_node, sent) = Self::forward_extended_batch(
                            stream,
                            &batch,
                            pending_route_sql.as_deref(),
                            wait_ready,
                            &mut conns,
                            current_node.as_deref(),
                            &stmt_registry,
                            &reprepare,
                            &batch_defines,
                            held_unnamed.take(),
                            session,
                            state,
                            config,
                        )
                        .await?;
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        // A `Sync` is the extended-protocol transaction/statement
                        // boundary (it yields ReadyForQuery); a `Flush` is not, so
                        // only a Sync triggers a pool release.
                        #[cfg(feature = "pool-modes")]
                        if wait_ready {
                            Self::release_to_pool_if_idle(
                                &mut conns,
                                current_node.as_deref(),
                                session,
                                state,
                                config,
                            )
                            .await;
                        }
                        state.metrics.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                        // Closed statements are deallocated everywhere — forget
                        // their canonical Parse so they are never re-prepared.
                        for name in batch_closes.drain(..) {
                            if let Some(removed) = stmt_registry.remove(&name) {
                                stmt_registry_bytes =
                                    stmt_registry_bytes.saturating_sub(removed.len());
                            }
                        }
                        if wait_ready {
                            // Sync ends the extended cycle; reset routing so the
                            // next Parse can re-route. Flush leaves it intact so
                            // the rest of the in-flight sequence stays put.
                            pending_route_sql = None;
                            batch_defines.clear();
                            batch_refs.clear();
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // ---- COPY sub-protocol (client -> backend) ----
                    MessageType::CopyData | MessageType::CopyDone | MessageType::CopyFail => {
                        let is_copy_end =
                            matches!(msg.msg_type, MessageType::CopyDone | MessageType::CopyFail);
                        let conn = current_node.as_ref().and_then(|n| conns.get_mut(n));
                        match conn {
                            Some(b) => {
                                b.stream.write_all(&msg.encode()).await.map_err(|e| {
                                    ProxyError::Network(format!("Backend copy write error: {}", e))
                                })?;
                                if is_copy_end {
                                    let node = current_node.clone().unwrap();
                                    let r = Self::stream_until_ready(
                                        stream,
                                        &mut b.stream,
                                        session,
                                        state,
                                    )
                                    .await;
                                    // Copy has drained back to ReadyForQuery — the
                                    // session is no longer mid-COPY, so pool release
                                    // is allowed again.
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    match r {
                                        Ok(sent) => {
                                            state
                                                .metrics
                                                .bytes_sent
                                                .fetch_add(sent, Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            conns.remove(&node);
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                            None => {
                                // The client is streaming COPY frames but the
                                // backend connection is gone (dropped/redialed).
                                // Silently discarding them hangs the client
                                // forever; instead tell it the copy failed and
                                // return it to a clean idle state.
                                session
                                    .copy_in_progress
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                                if is_copy_end {
                                    let emsg = Self::create_error_response(
                                        "57000",
                                        "COPY aborted: backend connection lost",
                                    );
                                    let _ = stream.write_all(&emsg).await;
                                    let _ =
                                        stream.write_all(&Self::create_ready_for_query(b'I')).await;
                                }
                            }
                        }
                    }

                    // ---- Anything else: forward to current backend best-effort ----
                    _ => {
                        if let Some(ref node) = current_node {
                            if let Some(b) = conns.get_mut(node) {
                                let _ = b.stream.write_all(&msg.encode()).await;
                            }
                        }
                    }
                }
            }

            // Bound un-flushed extended-protocol accumulation: a client must
            // reach a Sync/Flush boundary before this many bytes pile up in
            // `pending` (otherwise a never-syncing client grows it unbounded).
            if pending.len() > Self::MAX_PENDING_BYTES {
                let emsg = Self::create_error_response(
                    "53400",
                    "un-flushed extended-protocol buffer exceeds per-session limit",
                );
                let _ = stream.write_all(&emsg).await;
                let _ = stream.write_all(&Self::create_ready_for_query(b'I')).await;
                tracing::warn!(
                    client = %session.client_addr,
                    pending = pending.len(),
                    "pending extended-protocol buffer cap exceeded; closing connection"
                );
                return Ok(());
            }
        }

        // On disconnect, park this session's still-idle connections so a later
        // same-identity client can reuse them (cross-client pooling). Anything
        // mid-transaction is left to drop (closed → backend rolls back).
        #[cfg(feature = "pool-modes")]
        if state.backend_pool.is_some() {
            let nodes: Vec<String> = conns.keys().cloned().collect();
            for node in nodes {
                Self::release_to_pool_if_idle(
                    &mut conns,
                    Some(node.as_str()),
                    session,
                    state,
                    config,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Peek the first startup-phase message and negotiate client TLS.
    ///
    /// On `SSLRequest` the proxy answers `S` and runs a rustls server
    /// handshake when a TLS acceptor is configured, otherwise `N`
    /// (plaintext). A `Startup`/`CancelRequest` arriving first (no
    /// SSLRequest) is returned in `pre` so the caller doesn't re-read it.
    async fn negotiate_client_tls(
        mut tcp: TcpStream,
        state: &Arc<ServerState>,
    ) -> Result<(ClientStream, Option<StartupMessage>)> {
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(1024);
        let mut read_buf = vec![0u8; 1024];

        let first = loop {
            if let Some(msg) = codec.decode_startup(&mut buffer)? {
                break msg;
            }
            let n = tcp
                .read(&mut read_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("Startup read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "client closed before startup".to_string(),
                ));
            }
            buffer.extend_from_slice(&read_buf[..n]);
        };

        match first {
            StartupMessage::SSLRequest => match state.tls_acceptor.as_ref() {
                Some(acceptor) => {
                    tcp.write_all(b"S")
                        .await
                        .map_err(|e| ProxyError::Network(format!("SSL accept write: {}", e)))?;
                    let tls = acceptor
                        .accept(tcp)
                        .await
                        .map_err(|e| ProxyError::Network(format!("TLS handshake failed: {}", e)))?;
                    if tls.get_ref().1.peer_certificates().is_some() {
                        tracing::debug!("client presented a certificate (mTLS)");
                    }
                    Ok((ClientStream::Tls(Box::new(tls)), None))
                }
                None => {
                    tcp.write_all(b"N")
                        .await
                        .map_err(|e| ProxyError::Network(format!("SSL reject write: {}", e)))?;
                    Ok((ClientStream::Plain(tcp), None))
                }
            },
            other => Ok((ClientStream::Plain(tcp), Some(other))),
        }
    }

    /// Handle PostgreSQL startup phase (authentication). TLS/SSLRequest is
    /// already handled upstream in `negotiate_client_tls`; `pre` carries the
    /// first startup/cancel message when it was read during negotiation.
    async fn handle_startup(
        client_stream: &mut ClientStream,
        buffer: &mut BytesMut,
        codec: &ProtocolCodec,
        pre: Option<StartupMessage>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // Use the message already read during TLS negotiation, or read one
        // now (the TLS case, where the real startup follows the handshake).
        let startup_msg = match pre {
            Some(msg) => Some(msg),
            None => {
                let mut read_buf = vec![0u8; 1024];
                loop {
                    if let Some(msg) = codec.decode_startup(buffer)? {
                        break Some(msg);
                    }
                    let n = client_stream
                        .read(&mut read_buf)
                        .await
                        .map_err(|e| ProxyError::Network(format!("Startup read error: {}", e)))?;
                    if n == 0 {
                        return Ok((None, String::new()));
                    }
                    buffer.extend_from_slice(&read_buf[..n]);
                }
            }
        };

        match startup_msg {
            Some(StartupMessage::SSLRequest) => {
                // SSL is negotiated upstream; a second SSLRequest here is a
                // protocol error — reject defensively.
                client_stream
                    .write_all(b"N")
                    .await
                    .map_err(|e| ProxyError::Network(format!("SSL reject error: {}", e)))?;
                Err(ProxyError::Protocol(
                    "unexpected SSLRequest after startup".to_string(),
                ))
            }
            Some(StartupMessage::CancelRequest { pid, key }) => {
                // Forward the cancel to the backend that owns this key, then
                // close (the client opened this connection only to cancel).
                Self::forward_cancel_request(state, pid, key).await;
                Ok((None, String::new()))
            }
            Some(StartupMessage::Startup { params, .. }) => {
                Self::connect_and_authenticate(client_stream, &params, session, state, config).await
            }
            None => Err(ProxyError::Protocol(
                "Incomplete startup message".to_string(),
            )),
        }
    }

    /// Evaluate pg_hba-style admission rules in order. The first rule whose
    /// user, database, and address all match decides; if none match, admit.
    fn hba_admits(rules: &[HbaRule], ip: std::net::IpAddr, user: &str, database: &str) -> bool {
        for r in rules {
            let user_ok = r.user == "all" || r.user == user;
            let db_ok = r.database == "all" || r.database == database;
            if user_ok && db_ok && Self::hba_addr_matches(&r.address, ip) {
                return r.action == HbaAction::Allow;
            }
        }
        true
    }

    /// Match a client address against an hba `address` spec: "all", a bare
    /// IP, or a CIDR (`10.0.0.0/8`, `::1/128`).
    fn hba_addr_matches(spec: &str, ip: std::net::IpAddr) -> bool {
        use std::net::IpAddr;
        if spec == "all" {
            return true;
        }
        if let Some((net, bits)) = spec.split_once('/') {
            let bits: u32 = match bits.parse() {
                Ok(b) => b,
                Err(_) => return false,
            };
            match (net.parse::<IpAddr>(), ip) {
                (Ok(IpAddr::V4(n)), IpAddr::V4(i)) if bits <= 32 => {
                    let mask = if bits == 0 {
                        0
                    } else {
                        u32::MAX << (32 - bits)
                    };
                    (u32::from(n) & mask) == (u32::from(i) & mask)
                }
                (Ok(IpAddr::V6(n)), IpAddr::V6(i)) if bits <= 128 => {
                    let mask = if bits == 0 {
                        0
                    } else {
                        u128::MAX << (128 - bits)
                    };
                    (u128::from(n) & mask) == (u128::from(i) & mask)
                }
                _ => false,
            }
        } else {
            spec.parse::<IpAddr>().map(|s| s == ip).unwrap_or(false)
        }
    }

    /// Run a proxy-terminated SCRAM-SHA-256 server exchange against the
    /// client, validating its password with the configured `auth_file`. On
    /// success the client is authenticated by the proxy (no AuthenticationOk
    /// is sent here — the backend's is forwarded later). On any failure
    /// returns Err; the caller emits an ErrorResponse and closes.
    async fn proxy_scram_auth(
        client: &mut ClientStream,
        user: &str,
        state: &Arc<ServerState>,
    ) -> std::result::Result<(), String> {
        use crate::auth_scram::ScramServer;
        let auth_file = state.auth_file.as_ref().ok_or("scram not configured")?;

        // 1. AuthenticationSASL: advertise SCRAM-SHA-256.
        let mut sasl = BytesMut::new();
        sasl.put_i32(10); // SASL
        sasl.extend_from_slice(b"SCRAM-SHA-256\0");
        sasl.put_u8(0); // end of mechanism list
        Self::write_auth_frame(client, &sasl).await?;

        // 2. Read SASLInitialResponse ('p'): mechanism cstring + i32 len + data.
        let init = Self::read_password_message(client).await?;
        let mech_end = init
            .iter()
            .position(|&b| b == 0)
            .ok_or("malformed SASLInitialResponse (no mechanism)")?;
        if init.len() < mech_end + 5 {
            return Err("short SASLInitialResponse".into());
        }
        let client_first =
            std::str::from_utf8(&init[mech_end + 5..]).map_err(|_| "client-first not UTF-8")?;

        // 3. Look up the verifier (unknown user -> generic failure).
        let verifier = auth_file.get(user).ok_or("no such user")?.clone();

        // 4. server-first.
        let server_nonce = Self::random_nonce();
        let (server, server_first) = ScramServer::start(verifier, client_first, &server_nonce)?;

        // 5. AuthenticationSASLContinue.
        let mut cont = BytesMut::new();
        cont.put_i32(11);
        cont.extend_from_slice(server_first.as_bytes());
        Self::write_auth_frame(client, &cont).await?;

        // 6. Read SASLResponse ('p'): payload = client-final.
        let client_final_raw = Self::read_password_message(client).await?;
        let client_final =
            std::str::from_utf8(&client_final_raw).map_err(|_| "client-final not UTF-8")?;

        // 7. Verify -> server-final.
        let server_final = server.finish(client_final)?;

        // 8. AuthenticationSASLFinal (no AuthenticationOk — backend's follows).
        let mut fin = BytesMut::new();
        fin.put_i32(12);
        fin.extend_from_slice(server_final.as_bytes());
        Self::write_auth_frame(client, &fin).await?;
        Ok(())
    }

    /// Write an AuthenticationRequest ('R') frame with the given payload.
    async fn write_auth_frame(
        client: &mut ClientStream,
        payload: &[u8],
    ) -> std::result::Result<(), String> {
        let mut frame = BytesMut::with_capacity(payload.len() + 5);
        frame.put_u8(b'R');
        frame.put_u32((payload.len() + 4) as u32);
        frame.extend_from_slice(payload);
        client
            .write_all(&frame)
            .await
            .map_err(|e| format!("client write: {}", e))
    }

    /// Read one Password/SASL ('p') message from the client, returning its
    /// payload. Errors on EOF or any non-'p' frame.
    async fn read_password_message(
        client: &mut ClientStream,
    ) -> std::result::Result<BytesMut, String> {
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(1024);
        let mut read_buf = vec![0u8; 1024];
        loop {
            if let Some(msg) = codec
                .decode_message(&mut buffer)
                .map_err(|e| format!("decode: {}", e))?
            {
                if msg.msg_type == MessageType::Password {
                    return Ok(msg.payload);
                }
                return Err(format!("expected SASL response, got {:?}", msg.msg_type));
            }
            let n = client
                .read(&mut read_buf)
                .await
                .map_err(|e| format!("client read: {}", e))?;
            if n == 0 {
                return Err("client closed during SASL".into());
            }
            buffer.extend_from_slice(&read_buf[..n]);
        }
    }

    /// A fresh random SCRAM server nonce (printable, no comma).
    fn random_nonce() -> String {
        use rand::Rng;
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::thread_rng();
        (0..24)
            .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
            .collect()
    }

    /// Connect to backend and handle authentication
    async fn connect_and_authenticate(
        client_stream: &mut ClientStream,
        params: &HashMap<String, String>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // pg_hba-style admission: reject disallowed (user, database, client
        // address) combinations before opening any backend connection.
        let user = params.get("user").map(String::as_str).unwrap_or("");
        let database = params.get("database").map(String::as_str).unwrap_or(user);
        if !Self::hba_admits(&config.hba, session.client_addr.ip(), user, database) {
            tracing::info!(%user, %database, client = %session.client_addr, "connection rejected by hba rule");
            let err = Self::create_error_response(
                "28000",
                "connection rejected by proxy admission rules",
            );
            let _ = client_stream.write_all(&err).await;
            return Ok((None, String::new()));
        }

        // Proxy-terminated SCRAM-SHA-256: when an auth_file is configured the
        // proxy authenticates the client itself (becoming the auth boundary)
        // instead of relaying credentials to the backend. On success it falls
        // through to the normal backend connect, whose AuthenticationOk +
        // session messages are forwarded to the already-authenticated client.
        if state.auth_file.is_some() {
            if let Err(e) = Self::proxy_scram_auth(client_stream, user, state).await {
                tracing::info!(%user, error = %e, "proxy SCRAM auth failed");
                let err =
                    Self::create_error_response("28P01", &format!("authentication failed: {}", e));
                let _ = client_stream.write_all(&err).await;
                return Ok((None, String::new()));
            }
            tracing::debug!(%user, "client authenticated by proxy SCRAM");
        }

        // Plugin Authenticate hook — may deny the connection outright or
        // attach a richer identity (roles, tenant_id, claims) onto the
        // session for downstream plugins to consume. Happens before any
        // backend connection is opened so denials cost nothing on the
        // backend side.
        Self::apply_authenticate_hook(params, session, state).await?;

        // Migration cutover: when active, redirect this connection to the
        // promoted target, substituting the target's credentials/database for
        // the client's so the cutover is transparent to the application.
        let cutover = state.cutover.load_full();
        let (node_addr, effective_params) = if let Some(t) = cutover.as_ref() {
            let mut p = params.clone();
            p.insert("user".to_string(), t.user.clone());
            if let Some(ref db) = t.database {
                p.insert("database".to_string(), db.clone());
            } else {
                p.remove("database");
            }
            tracing::debug!(target = %t.addr, "routing connection to cutover target");
            (t.addr.clone(), p)
        } else {
            (
                Self::select_node(session, state, config).await?,
                params.clone(),
            )
        };

        // Connect to backend. A failure here (the node is down at the moment a
        // new client connects) demotes the node in-band too — not just failures
        // on the forward path — so a dead backend is detected on the very next
        // connection instead of waiting for the periodic health checker.
        let mut backend = match tokio::time::timeout(
            config.pool.acquire_timeout(),
            TcpStream::connect(&node_addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                let msg = format!("Failed to connect to {}: {}", node_addr, e);
                Self::note_backend_failure(state, &node_addr, &msg);
                return Err(ProxyError::Connection(msg));
            }
            Err(_) => {
                let msg = format!("Connection timeout to {}", node_addr);
                Self::note_backend_failure(state, &node_addr, &msg);
                return Err(ProxyError::Connection(msg));
            }
        };
        let _ = backend.set_nodelay(true);

        // Build and send startup message to backend
        let params = &effective_params;
        let startup_bytes = Self::build_startup_message(params);
        backend
            .write_all(&startup_bytes)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend startup write error: {}", e)))?;

        // Forward authentication messages between client and backend.
        // Registers the backend's BackendKeyData so a later CancelRequest
        // can be routed back to this node.
        Self::proxy_authentication(client_stream, &mut backend, state, &node_addr).await?;

        // Store session variables
        {
            let mut vars = session.variables.write().await;
            for (k, v) in params {
                vars.insert(k.clone(), v.clone());
            }
        }

        Ok((Some(backend), node_addr))
    }

    /// Build PostgreSQL startup message
    fn build_startup_message(params: &HashMap<String, String>) -> Vec<u8> {
        let mut payload = BytesMut::new();

        // Protocol version 3.0
        payload.put_u32(196608);

        // Parameters
        for (key, value) in params {
            payload.extend_from_slice(key.as_bytes());
            payload.put_u8(0);
            payload.extend_from_slice(value.as_bytes());
            payload.put_u8(0);
        }
        payload.put_u8(0); // Terminator

        // Build complete message with length prefix
        let mut msg = BytesMut::new();
        msg.put_u32((payload.len() + 4) as u32);
        msg.extend_from_slice(&payload);

        msg.to_vec()
    }

    /// Cap on the cancel-key map; the oldest entries are evicted on overflow
    /// (a dropped stale entry only means one best-effort cancel is not
    /// forwarded).
    const MAX_CANCEL_KEYS: usize = 100_000;

    /// Deadline for the pre-auth startup exchange (client TLS negotiation +
    /// PostgreSQL startup/authentication). A client that connects and then
    /// stalls must not hold a task and its session-map slot open forever
    /// (slow-loris). Only the handshake is bounded; the query loop is not.
    const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
    /// Timeout for a single backend write on the forward path — a blackholed or
    /// hung backend must never pin a client task indefinitely. Backend reads
    /// are already bounded (30s); this bounds writes symmetrically.
    const BACKEND_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
    /// Timeout for a single client write — a wedged or very slow client must
    /// not pin a proxy task (and the backend connection it holds) forever.
    const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(60);
    /// Timeout for the out-of-band re-prepare exchange (write Parse+Flush, read
    /// ParseComplete) performed on a backend connection switch.
    const REPREPARE_TIMEOUT: Duration = Duration::from_secs(15);
    /// Per-session cap on distinct named prepared statements — bounds the
    /// `stmt_registry` against a client that issues unbounded `Parse`s.
    const MAX_PREPARED_STATEMENTS: usize = 8192;
    /// Per-session cap on the aggregate bytes retained in `stmt_registry`. The
    /// count cap alone does not bound memory: each entry holds the full encoded
    /// `Parse`, so 8192 large statements could still retain multiple GiB. This
    /// bounds the total held bytes (statements are tiny in practice; a session
    /// that approaches this is pathological).
    const MAX_PREPARED_BYTES: usize = 64 * 1024 * 1024;
    /// Per-session cap on the un-flushed extended-protocol `pending` buffer: a
    /// client must reach a Sync/Flush boundary before this many bytes pile up.
    const MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
    /// Global ceiling on idle connections parked in the data-path backend pool
    /// across ALL `(node,user,db)` identities — bounds total file descriptors
    /// regardless of how many distinct identities connect.
    #[cfg(feature = "pool-modes")]
    const MAX_TOTAL_IDLE_BACKEND_CONNS: usize = 8192;
    /// How often the idle-connection reaper runs.
    const POOL_REAP_INTERVAL: Duration = Duration::from_secs(30);

    /// Record the backend that owns a BackendKeyData (pid, secret) pair.
    fn register_cancel_key(state: &Arc<ServerState>, pid: u32, key: u32, node_addr: &str) {
        // FIFO-evict the oldest registrations when at capacity, rather than
        // dropping all of them. Evict a small batch so we don't churn the lock
        // on every insert once full.
        {
            let mut order = state.cancel_order.lock();
            while state.cancel_map.len() >= Self::MAX_CANCEL_KEYS {
                match order.pop_front() {
                    Some(old) => {
                        state.cancel_map.remove(&old);
                    }
                    None => {
                        // Order queue empty but map full (shouldn't happen) —
                        // fall back to a clear to stay bounded.
                        state.cancel_map.clear();
                        break;
                    }
                }
            }
            order.push_back((pid, key));
        }
        state.cancel_map.insert((pid, key), node_addr.to_string());
    }

    /// Forward a client CancelRequest to the backend that issued the
    /// matching BackendKeyData. Best-effort: unknown keys are ignored.
    async fn forward_cancel_request(state: &Arc<ServerState>, pid: u32, key: u32) {
        let Some(addr) = state.cancel_map.get(&(pid, key)).map(|e| e.clone()) else {
            tracing::debug!(pid, "cancel request for unknown key; ignoring");
            return;
        };
        // CancelRequest: int32 len(16) + int32 code(80877102) + pid + key.
        let mut msg = BytesMut::with_capacity(16);
        msg.put_u32(16);
        msg.put_u32(80877102);
        msg.put_u32(pid);
        msg.put_u32(key);
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr)).await {
            Ok(Ok(mut conn)) => {
                let _ = conn.set_nodelay(true);
                if let Err(e) = conn.write_all(&msg).await {
                    tracing::warn!(node = %addr, error = %e, "failed to forward CancelRequest");
                }
                // PG closes the connection after handling a CancelRequest.
            }
            other => {
                tracing::warn!(node = %addr, ?other, "could not connect to forward CancelRequest")
            }
        }
    }

    /// Proxy authentication messages between client and backend
    async fn proxy_authentication(
        client_stream: &mut ClientStream,
        backend_stream: &mut TcpStream,
        state: &Arc<ServerState>,
        node_addr: &str,
    ) -> Result<()> {
        // Bidirectional relay driven by readiness, not a fixed poll. The old
        // loop read the backend (untimed), forwarded, then polled the client
        // with a fixed 100ms window; a client that answered an auth challenge
        // more than 100ms after receiving it (WAN RTT, slow SCRAM client) missed
        // its window, and the loop then re-blocked on the untimed backend read
        // while the backend waited for the very response the proxy never read —
        // a deadlock until PostgreSQL's authentication_timeout killed it. Here
        // both directions are relayed as either side becomes readable, under one
        // overall deadline, so multi-round SCRAM completes regardless of client
        // latency.
        //
        // Backend-side frames are inspected by RAW tag (the wire decoder is
        // direction-agnostic — 'E' would decode to the client-side `Execute`,
        // not `ErrorResponse`), so a backend auth error is recognised here
        // rather than falling through to a misleading timeout.
        let mut backend_buffer = BytesMut::with_capacity(4096);
        let mut cbuf = vec![0u8; 4096];
        let mut bbuf = vec![0u8; 4096];
        let deadline = tokio::time::Instant::now() + Self::STARTUP_TIMEOUT;

        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ProxyError::Auth(
                        "authentication timed out".to_string(),
                    ));
                }
                // Backend -> client: relay every byte, then scan complete frames
                // for the auth terminal states.
                r = backend_stream.read(&mut bbuf) => {
                    let n = r.map_err(|e| {
                        ProxyError::Network(format!("Backend auth read error: {}", e))
                    })?;
                    if n == 0 {
                        return Err(ProxyError::Connection(
                            "Backend closed during auth".to_string(),
                        ));
                    }
                    client_stream
                        .write_all(&bbuf[..n])
                        .await
                        .map_err(|e| ProxyError::Network(format!("Client auth write error: {}", e)))?;
                    backend_buffer.extend_from_slice(&bbuf[..n]);

                    // Walk complete frames by raw tag.
                    loop {
                        if backend_buffer.len() < 5 {
                            break;
                        }
                        let len = u32::from_be_bytes([
                            backend_buffer[1],
                            backend_buffer[2],
                            backend_buffer[3],
                            backend_buffer[4],
                        ]) as usize;
                        if len < 4 || backend_buffer.len() < len + 1 {
                            break;
                        }
                        let tag = backend_buffer[0];
                        let frame = backend_buffer.split_to(len + 1);
                        match tag {
                            // BackendKeyData: 5-byte header + pid(4) + key(4).
                            // Remember which backend owns this cancel key.
                            b'K' if frame.len() >= 13 => {
                                let pid = u32::from_be_bytes([
                                    frame[5], frame[6], frame[7], frame[8],
                                ]);
                                let key = u32::from_be_bytes([
                                    frame[9], frame[10], frame[11], frame[12],
                                ]);
                                Self::register_cancel_key(state, pid, key, node_addr);
                            }
                            // ReadyForQuery: authentication + startup complete.
                            b'Z' => return Ok(()),
                            // ErrorResponse: auth failed (already relayed to the
                            // client above); surface the failure to the caller.
                            b'E' => {
                                return Err(ProxyError::Auth("Authentication failed".to_string()));
                            }
                            _ => {}
                        }
                    }
                }
                // Client -> backend: relay the client's auth response(s)
                // whenever they arrive, with no artificial deadline of their own.
                r = client_stream.read(&mut cbuf) => {
                    let n = r.map_err(|e| {
                        ProxyError::Network(format!("Client auth read error: {}", e))
                    })?;
                    if n == 0 {
                        return Err(ProxyError::Connection(
                            "Client closed during auth".to_string(),
                        ));
                    }
                    backend_stream
                        .write_all(&cbuf[..n])
                        .await
                        .map_err(|e| {
                            ProxyError::Network(format!("Backend password write error: {}", e))
                        })?;
                }
            }
        }
    }

    /// Decide which node a request should be routed to, without doing any
    /// I/O. Reuses `current_node` when it is healthy and role-compatible
    /// (sticky session), otherwise selects a fresh primary/read node. The
    /// returned address is the key into the per-session connection cache.
    async fn choose_target_node(
        is_write: bool,
        forced_target: Option<String>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // After a migration cutover, every request stays on the promoted
        // target — never route back to the former primary.
        if let Some(t) = state.cutover.load_full().as_ref() {
            return Ok(t.addr.clone());
        }

        // Read-your-writes: within the window after a write, a read is pinned to
        // the primary (overriding the reuse-of-a-standby path) so the client
        // observes its own writes despite replica lag.
        #[cfg(feature = "lag-routing")]
        if !is_write && forced_target.is_none() && config.lag_routing.enabled {
            let last_write = *session.last_write_at.read().await;
            if Self::ryw_pins_primary(last_write, config.lag_routing.ryw_window_ms) {
                tracing::debug!(target: "helios::routing", "read-your-writes: pinning read to primary");
                return Self::select_primary_with_timeout(session, state, config).await;
            }
        }

        let need_switch = if let Some(ref forced) = forced_target {
            let health = state.health.load_full();
            let reuse = current_node
                .map(|c| c == forced && health.get(c).map(|h| h.healthy).unwrap_or(false))
                .unwrap_or(false);
            !reuse
        } else if let Some(current) = current_node {
            let health = state.health.load_full();
            let current_healthy = health.get(current).map(|h| h.healthy).unwrap_or(false);
            if !current_healthy {
                true
            } else if is_write {
                let is_primary = config
                    .nodes
                    .iter()
                    .find(|n| n.address() == *current)
                    .map(|n| n.role == NodeRole::Primary)
                    .unwrap_or(false);
                !is_primary
            } else {
                false
            }
        } else {
            true
        };

        if let Some(forced) = forced_target {
            // Resolve a node *name* to its address; an address is passed
            // through unchanged. This lets `/*helios:node=pg-standby*/` (and a
            // plugin `Node("name")`) target a node by its configured name
            // rather than requiring the raw host:port.
            let resolved = config
                .nodes
                .iter()
                .find(|n| n.name.as_deref() == Some(forced.as_str()) || n.address() == forced)
                .map(|n| n.address())
                .unwrap_or(forced);
            Ok(resolved)
        } else if need_switch {
            if is_write {
                Self::select_primary_with_timeout(session, state, config).await
            } else {
                Self::select_read_node(session, state, config).await
            }
        } else {
            Ok(current_node.unwrap().to_string())
        }
    }

    /// Ensure the per-session cache holds an authenticated backend connection
    /// to `target`, dialing + silently re-authenticating one (with the
    /// client's pass-through credentials) only if absent. The cached
    /// connection is then reused across read/write route switches.
    async fn ensure_conn(
        conns: &mut HashMap<String, BackendConn>,
        target: &str,
        session: &Arc<ClientSession>,
        config: &ProxyConfig,
        _state: &Arc<ServerState>,
    ) -> Result<()> {
        if conns.contains_key(target) {
            return Ok(());
        }

        // Transaction/Statement pooling: lease a parked, identity-matched
        // connection before paying for a fresh TCP connect + startup + auth.
        // The parked connection was `DISCARD ALL`-reset on release, so it is
        // clean for this (same-identity) client.
        #[cfg(feature = "pool-modes")]
        if let Some(pool) = _state.backend_pool.as_ref() {
            let key = Self::pool_key_for(target, session).await;
            if let Some(stream) = pool.checkout(&key) {
                tracing::info!(
                    target: "helios::pool",
                    node = %target,
                    "reused pooled backend connection"
                );
                conns.insert(target.to_string(), BackendConn::new(stream));
                return Ok(());
            }
        }

        let mut backend =
            tokio::time::timeout(config.pool.acquire_timeout(), TcpStream::connect(target))
                .await
                .map_err(|_| ProxyError::Connection(format!("Connection timeout to {}", target)))?
                .map_err(|e| {
                    ProxyError::Connection(format!("Failed to connect to {}: {}", target, e))
                })?;
        let _ = backend.set_nodelay(true);

        let params = session.variables.read().await.clone();
        let startup = Self::build_startup_message(&params);
        backend
            .write_all(&startup)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend startup error: {}", e)))?;
        Self::complete_backend_auth(&mut backend).await?;
        #[cfg(feature = "pool-modes")]
        if _state.backend_pool.is_some() {
            tracing::debug!(target: "helios::pool", node = %target, "dialed fresh backend connection (pool miss)");
        }
        tracing::debug!(node = %target, "opened backend connection");
        conns.insert(target.to_string(), BackendConn::new(backend));
        Ok(())
    }

    /// Startup parameters that change how the backend interprets or renders
    /// values and are reset by `DISCARD ALL`/`RESET ALL` to the connection's
    /// *startup* values. Two clients of the same `(node,user,db)` but different
    /// values for any of these must NOT share a pooled connection, or the
    /// borrower silently inherits the lender's encoding/date/number formatting.
    /// This mirrors PgBouncer's `track_extra_parameters` intent.
    #[cfg(feature = "pool-modes")]
    const POOL_IDENTITY_PARAMS: &'static [&'static str] = &[
        "client_encoding",
        "DateStyle",
        "TimeZone",
        "IntervalStyle",
        "standard_conforming_strings",
        "options",
    ];

    /// Build the pool key for the current session's connection identity.
    /// Base identity is `(node, user, database)` — connections are reused only
    /// within an identity, so a borrower always matches the principal the parked
    /// connection was authenticated as. When any routing-relevant startup GUC
    /// (see `POOL_IDENTITY_PARAMS`) is set, a hash of those is folded in so the
    /// borrower also matches the lender's value-formatting settings. The common
    /// case (no custom GUCs) keeps the bare `(node,user,db)` key unchanged.
    #[cfg(feature = "pool-modes")]
    async fn pool_key_for(target: &str, session: &Arc<ClientSession>) -> String {
        let vars = session.variables.read().await;
        let user = vars.get("user").map(|s| s.as_str()).unwrap_or("");
        // PostgreSQL defaults the database to the role name when unset.
        let database = vars.get("database").map(|s| s.as_str()).unwrap_or(user);
        let base = crate::pool::pool_key(target, user, database);

        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let mut any = false;
        for k in Self::POOL_IDENTITY_PARAMS {
            if let Some(v) = vars.get(*k) {
                any = true;
                k.hash(&mut h);
                v.hash(&mut h);
            }
        }
        if any {
            format!("{}\0{:016x}", base, h.finish())
        } else {
            base
        }
    }

    /// Reset a backend connection to a clean session state before parking it
    /// for reuse: runs the configured reset query (default `DISCARD ALL`,
    /// which deallocates prepared statements, drops temp tables, resets GUCs
    /// and advisory locks) and drains its response to `ReadyForQuery`. Returns
    /// `Err` if the connection is unhealthy OR the reset itself did not cleanly
    /// succeed — the caller then drops (closes) it instead of parking a
    /// poisoned connection.
    ///
    /// "Cleanly succeeded" means: no `ErrorResponse` frame in the reply AND the
    /// terminating `ReadyForQuery` reported idle (`'I'`, not in/failed
    /// transaction). The previous version returned `Ok` on the first
    /// `ReadyForQuery` regardless, so a failed `DISCARD ALL` (e.g. a copy-abort,
    /// or a custom `reset_query` that errors) would park a connection with its
    /// GUCs / temp tables / prepared statements intact — exactly what pooling
    /// must never do. Frames are walked by raw tag because the wire decoder is
    /// direction-agnostic (`'E'` decodes to the client-side `Execute`), so a
    /// backend `ErrorResponse` cannot be recognised via `msg_type`.
    #[cfg(feature = "pool-modes")]
    async fn reset_backend<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        stream: &mut S,
        reset_sql: &str,
    ) -> Result<()> {
        let msg = crate::protocol::QueryMessage {
            query: reset_sql.to_string(),
        }
        .encode();
        tokio::time::timeout(Self::BACKEND_WRITE_TIMEOUT, stream.write_all(&msg.encode()))
            .await
            .map_err(|_| ProxyError::Network("reset write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("reset write error: {}", e)))?;

        let mut buf = BytesMut::with_capacity(1024);
        let mut had_error = false;
        loop {
            // Walk complete frames by raw tag, tracking any ErrorResponse and
            // stopping at ReadyForQuery.
            let mut consumed = 0usize;
            let mut ready_status: Option<u8> = None;
            loop {
                let rem = &buf[consumed..];
                if rem.len() < 5 {
                    break;
                }
                let len = u32::from_be_bytes([rem[1], rem[2], rem[3], rem[4]]) as usize;
                if len < 4 || rem.len() < len + 1 {
                    break;
                }
                let mtype = rem[0];
                let frame_total = len + 1;
                if mtype == b'E' {
                    had_error = true;
                }
                consumed += frame_total;
                if mtype == b'Z' {
                    ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                    break;
                }
            }
            let _ = buf.split_to(consumed);

            if let Some(status) = ready_status {
                if had_error || status != b'I' {
                    return Err(ProxyError::Connection(format!(
                        "reset query did not cleanly succeed (error={}, status={})",
                        had_error, status as char
                    )));
                }
                return Ok(());
            }

            buf.reserve(1024);
            let n = tokio::time::timeout(Duration::from_secs(5), stream.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("reset drain timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("reset drain read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "backend closed during reset".to_string(),
                ));
            }
        }
    }

    /// Transaction/Statement pooling release point: when the session is at an
    /// idle boundary (`ReadyForQuery` reported not-in-transaction), reset the
    /// just-used connection and park it for reuse by the next same-identity
    /// client. A no-op in Session mode or when the feature is off. Never
    /// releases mid-transaction.
    #[cfg(feature = "pool-modes")]
    async fn release_to_pool_if_idle(
        conns: &mut HashMap<String, BackendConn>,
        node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        let Some(pool) = state.backend_pool.as_ref() else {
            return;
        };
        let Some(node) = node else {
            return;
        };
        // Only release at a clean transaction boundary — never mid-transaction
        // and never mid-COPY (the backend is awaiting CopyData; resetting +
        // parking the socket now aborts the copy and hangs the client).
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
            || session
                .copy_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let Some(mut bc) = conns.remove(node) else {
            return;
        };

        // Conditional reset: a connection that provably touched no session
        // state — no dirtying simple statement, no named prepared statement, no
        // unnamed prepared statement — can be parked WITHOUT the `DISCARD ALL`
        // round-trip, removing a backend RTT from the critical path for clean
        // autocommit workloads. Any of these three signals forces the full
        // reset. Extended-protocol traffic always has `prepared`/`unnamed_sig`
        // set, so it is never clean-skipped (conservative by construction).
        let clean = !bc.dirty && bc.prepared.is_empty() && bc.unnamed_sig.is_none();
        if config.pool_mode.skip_clean_reset && clean {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin(&key, bc.stream) {
                pool.note_reset_skipped();
                tracing::debug!(target: "helios::pool", node = %node, "parked clean backend connection (reset skipped)");
            }
            return;
        }

        if Self::reset_backend(&mut bc.stream, &config.pool_mode.reset_query)
            .await
            .is_ok()
        {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin(&key, bc.stream) {
                tracing::debug!(target: "helios::pool", node = %node, "parked backend connection for reuse");
            }
        }
        // On reset failure the connection is dropped here (closed).
    }

    /// Forward a simple-query (`Query`) message and stream its response back
    /// to the client frame-by-frame, ending at ReadyForQuery. Picks (and, if
    /// needed, opens) the target node's connection from the per-session
    /// cache. Returns `(Some(node_used), bytes)` — `None` node means the
    /// request was short-circuited (plugin block) without touching a backend.
    async fn forward_simple_query(
        client: &mut ClientStream,
        msg: &Message,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<String>, u64)> {
        // Rate-limit gate: deny before any backend selection.
        #[cfg(feature = "rate-limiting")]
        if let Some(mut resp) = Self::rate_limit_check(session, state, config).await {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        let default_is_write = Self::is_write_message(msg);
        let plugin_override = Self::apply_route_hook(msg, state, session);

        // Block short-circuits before any backend selection.
        if let RouteOverride::Block(reason) = plugin_override {
            let mut response = Vec::with_capacity(64 + reason.len());
            response.extend_from_slice(&Self::create_error_response(
                "42000",
                &format!("Query blocked by route plugin: {}", reason),
            ));
            response.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&response)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, response.len() as u64));
        }

        // SQL-comment routing hints (feature + `[routing_hints] enabled`)
        // refine the override, recompute the write flag on the stripped SQL,
        // and may rewrite the message to drop the hint comment.
        #[cfg(feature = "routing-hints")]
        let (route_override, default_is_write, stripped_msg) =
            Self::resolve_simple_route(msg, plugin_override, default_is_write, state);
        #[cfg(not(feature = "routing-hints"))]
        let (route_override, stripped_msg): (RouteOverride, Option<Message>) =
            (plugin_override, None);

        let (is_write, forced_target) = match route_override {
            RouteOverride::None => (default_is_write, None),
            RouteOverride::Primary => (true, None),
            RouteOverride::Standby => (false, None),
            RouteOverride::Node(name) => (default_is_write, Some(name)),
            RouteOverride::Block(_) => unreachable!("handled above"),
        };

        // Read-your-writes: stamp the session on a write so subsequent reads
        // pin to the primary for the configured window.
        #[cfg(feature = "lag-routing")]
        if is_write && config.lag_routing.enabled {
            *session.last_write_at.write().await = Some(std::time::Instant::now());
        }

        // Forward the stripped message when routing-hints rewrote it, else the
        // original (borrowed, no copy).
        let forward_msg = stripped_msg.as_ref().unwrap_or(msg);

        // Query rewriting: apply rules to the SQL; if any rule fired, forward a
        // rebuilt Query carrying the rewritten SQL (so caching + the backend
        // both see the rewritten form).
        #[cfg(feature = "query-rewriting")]
        let rewritten_msg: Option<Message> = state.rewriter.as_ref().and_then(|rw| {
            let sql = crate::protocol::query_text(&forward_msg.payload)?;
            match rw.rewrite(sql) {
                Ok(res) if res.was_rewritten() => {
                    tracing::debug!(target: "helios::rewrite", rules = ?res.rules_applied, "query rewritten");
                    Some(crate::protocol::QueryMessage { query: res.query().to_string() }.encode())
                }
                _ => None,
            }
        });
        #[cfg(feature = "query-rewriting")]
        let forward_msg = rewritten_msg.as_ref().unwrap_or(forward_msg);

        // Multi-tenancy: resolve the session's tenant and inject a row-level
        // tenant filter. Done BEFORE the cache lookup so each tenant's results
        // are cached under their own (filtered) SQL — no cross-tenant leakage.
        #[cfg(feature = "multi-tenancy")]
        let tenant_msg: Option<Message> = if let Some(tm) = state.tenant_manager.as_ref() {
            match crate::protocol::query_text(&forward_msg.payload) {
                Some(sql) => {
                    let ctx = Self::tenant_request_ctx(session).await;
                    match tm.identify_tenant(&ctx) {
                        Some(tenant) => {
                            let res = tm.transform_query(sql, &tenant);
                            if res.transformed {
                                tracing::debug!(target: "helios::tenant", tenant = %tenant.0, "tenant filter injected");
                                Some(crate::protocol::QueryMessage { query: res.query }.encode())
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                }
                None => None,
            }
        } else {
            None
        };
        #[cfg(feature = "multi-tenancy")]
        let forward_msg = tenant_msg.as_ref().unwrap_or(forward_msg);

        // Query cache: on a cacheable read, a hit is served from cache with no
        // backend round-trip; on a miss we keep the context to store the result.
        #[cfg(feature = "query-cache")]
        let cache_ctx: Option<crate::cache::CacheContext> = if is_write {
            None
        } else if let Some(qc) = state.query_cache.as_ref() {
            let sql = crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
            match Self::cacheable_read_ctx(session, sql).await {
                Some(ctx) => {
                    if let crate::cache::CacheLookup::Hit { result, level } =
                        qc.get(sql, &ctx).await
                    {
                        tracing::debug!(target: "helios::cache", level = %level, "cache hit");
                        client.write_all(&result.data).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, result.data.len() as u64));
                    }
                    Some(ctx)
                }
                None => None,
            }
        } else {
            None
        };

        // Schema/workload routing: pin an analytical (OLAP) read to the
        // configured analytics node, unless something already forced a target.
        #[cfg(feature = "schema-routing")]
        let forced_target = match state.schema_analyzer.as_ref() {
            Some(analyzer)
                if forced_target.is_none()
                    && !is_write
                    && !config.schema_routing.analytics_node.is_empty() =>
            {
                match crate::protocol::query_text(&forward_msg.payload) {
                    Some(sql) if analyzer.analyze(sql).is_analytics() => {
                        tracing::debug!(target: "helios::schema", "OLAP query routed to analytics node");
                        Some(config.schema_routing.analytics_node.clone())
                    }
                    _ => forced_target,
                }
            }
            _ => forced_target,
        };

        // Analytics: capture the forwarded SQL + start the latency timer.
        #[cfg(feature = "query-analytics")]
        let analytics_sql =
            crate::protocol::query_text(&forward_msg.payload).map(|s| s.to_string());
        #[cfg(feature = "query-analytics")]
        let started = std::time::Instant::now();

        let target = Self::choose_target_node(
            is_write,
            forced_target,
            current_node,
            session,
            state,
            config,
        )
        .await?;
        tracing::debug!(target: "helios::routing", node = %target, is_write, "routed simple query");

        // Circuit breaker: fast-fail when the chosen node's circuit is open.
        #[cfg(feature = "circuit-breaker")]
        if let Some(mut resp) = Self::circuit_fast_fail(state, &target) {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // A connect/auth failure trips the breaker (and is propagated as today).
        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Conditional-reset bookkeeping: if this statement is not provably
        // session-neutral, mark the connection dirty so it is fully reset (not
        // clean-skipped) when parked. Only evaluated when the optimisation is
        // enabled and the connection is not already dirty (one O(len) scan at
        // most, until the first dirtying statement).
        #[cfg(feature = "pool-modes")]
        if config.pool_mode.skip_clean_reset && !backend.dirty {
            if let Some(sql) = crate::protocol::query_text(&forward_msg.payload) {
                if Self::stmt_leaves_session_state(sql) {
                    backend.dirty = true;
                }
            }
        }

        let backend_err = match tokio::time::timeout(
            Self::BACKEND_WRITE_TIMEOUT,
            backend.stream.write_all(&forward_msg.encode()),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("Backend write error: {}", e)),
            Err(_) => Some("Backend write timeout".to_string()),
        };
        if let Some(msg) = backend_err {
            let e = ProxyError::Network(msg);
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            return Err(e);
        }

        // Cacheable read miss: capture the response frames and store them so a
        // later identical read is served from cache without a backend hit.
        #[cfg(feature = "query-cache")]
        if let (Some(ctx), Some(qc)) = (cache_ctx.as_ref(), state.query_cache.as_ref()) {
            return match Self::stream_until_ready_capture(client, &mut backend.stream, session)
                .await
            {
                Ok((sent, captured, cacheable, rows)) => {
                    #[cfg(feature = "circuit-breaker")]
                    Self::circuit_record(state, &target, true, "");
                    if cacheable && !captured.is_empty() {
                        let sql = crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
                        qc.put(
                            sql,
                            ctx,
                            bytes::Bytes::from(captured),
                            rows,
                            std::time::Duration::ZERO,
                        )
                        .await;
                    }
                    #[cfg(feature = "query-analytics")]
                    if let Some(sql) = analytics_sql.as_deref() {
                        Self::record_analytics(
                            state,
                            session,
                            sql,
                            &target,
                            started.elapsed(),
                            None,
                        )
                        .await;
                    }
                    Ok((Some(target), sent))
                }
                Err(e) => {
                    conns.remove(&target);
                    Self::record_backend_failure(state, &target, &e.to_string());
                    Err(e)
                }
            };
        }

        match Self::stream_until_ready(client, &mut backend.stream, session, state).await {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                // Invalidate cached reads referencing tables this write touched.
                #[cfg(feature = "query-cache")]
                if is_write {
                    if let Some(qc) = state.query_cache.as_ref() {
                        let sql = crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
                        qc.invalidate_query(sql).await;
                    }
                }
                // Transaction Replay: journal the write for failover/time-travel.
                #[cfg(feature = "ha-tr")]
                if is_write && config.tr_enabled {
                    if let Some(sql) = crate::protocol::query_text(&forward_msg.payload) {
                        Self::journal_write(state, session, sql).await;
                    }
                }
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(state, session, sql, &target, started.elapsed(), None)
                        .await;
                }
                Ok((Some(target), sent))
            }
            Err(e) => {
                // Drop the broken connection so the next use redials.
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(
                        state,
                        session,
                        sql,
                        &target,
                        started.elapsed(),
                        Some(e.to_string()),
                    )
                    .await;
                }
                Err(e)
            }
        }
    }

    /// Forward an accumulated extended-protocol batch (Parse/Bind/Describe/
    /// Execute/Close terminated by Sync or Flush) and stream the response.
    /// Routing is taken from `route_sql` (the first Parse's SQL); when it is
    /// `None` (a re-Bind/Execute of a named prepared statement) the request
    /// stays on the connection the statement was prepared on — no switch.
    ///
    /// `reprepare` lists named statements this batch references but does not
    /// itself define; any that the chosen connection has not seen are
    /// re-prepared from `registry` (their original `Parse`) before the batch is
    /// sent, so a named statement survives a backend switch/redial (Batch F.4).
    /// `defines` are the named statements this batch's own `Parse`s create —
    /// recorded against the connection once it accepts the batch.
    #[allow(clippy::too_many_arguments)]
    async fn forward_extended_batch(
        client: &mut ClientStream,
        batch: &[u8],
        route_sql: Option<&str>,
        wait_ready: bool,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        registry: &HashMap<String, bytes::Bytes>,
        reprepare: &[String],
        defines: &[String],
        unnamed: Option<(bytes::Bytes, bytes::Bytes)>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<String>, u64)> {
        // Rate-limit gate. The terminating ReadyForQuery is only appended when
        // the batch carried a Sync (`wait_ready`); a Flush-terminated batch
        // expects an ErrorResponse with no ReadyForQuery.
        #[cfg(feature = "rate-limiting")]
        if let Some(mut resp) = Self::rate_limit_check(session, state, config).await {
            if wait_ready {
                resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            }
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // Analytics: the routable SQL (first Parse) + latency timer.
        #[cfg(feature = "query-analytics")]
        let analytics_sql = route_sql.map(|s| s.to_string());
        #[cfg(feature = "query-analytics")]
        let started = std::time::Instant::now();

        let target = match route_sql {
            Some(sql) => {
                // Routing-hints, when active, can override the verb-based
                // target (and recompute the write flag on the stripped SQL).
                #[cfg(feature = "routing-hints")]
                let (is_write, forced) = Self::extended_hint_route(state, sql)
                    .unwrap_or_else(|| (Self::is_write_query(sql), None));
                #[cfg(not(feature = "routing-hints"))]
                let (is_write, forced): (bool, Option<String>) = (Self::is_write_query(sql), None);
                #[cfg(feature = "lag-routing")]
                if is_write && config.lag_routing.enabled {
                    *session.last_write_at.write().await = Some(std::time::Instant::now());
                }
                Self::choose_target_node(is_write, forced, current_node, session, state, config)
                    .await?
            }
            // No Parse in this batch: stay on the prepared-statement /
            // portal connection. Fall back to a read node only if the
            // session has no current connection yet.
            None => match current_node {
                Some(c) => c.to_string(),
                None => Self::select_read_node(session, state, config).await?,
            },
        };

        // Circuit breaker: fast-fail when the chosen node's circuit is open.
        #[cfg(feature = "circuit-breaker")]
        if let Some(mut resp) = Self::circuit_fast_fail(state, &target) {
            if wait_ready {
                resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            }
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Transparently re-prepare any referenced named statement this socket
        // is missing. Each is sent as its original `Parse` + `Flush`; the
        // resulting `ParseComplete` is consumed here so the client never sees
        // the extra round trip. A re-prepare failure recycles the connection.
        for name in reprepare {
            if backend.prepared.contains(name) {
                continue;
            }
            let Some(parse_bytes) = registry.get(name) else {
                continue; // unknown statement — let the batch surface the error
            };
            match Self::reprepare_statement(&mut backend.stream, parse_bytes).await {
                Ok(()) => {
                    backend.prepared.insert(name.clone());
                }
                Err(e) => {
                    conns.remove(&target);
                    return Err(e);
                }
            }
        }

        // Unnamed-`Parse` promotion: if the held unnamed Parse matches what this
        // connection's unnamed statement already holds, skip forwarding it and
        // synthesize its `ParseComplete` to the client; otherwise forward it
        // first (re-establishing the connection's unnamed statement) and record
        // its signature. A fresh/redialed connection has no signature, so the
        // Parse is always (re)forwarded there — correctness is preserved.
        let mut inject_parse_complete = false;
        let mut new_unnamed_sig: Option<bytes::Bytes> = None;
        if let Some((parse_msg, sig)) = unnamed.as_ref() {
            if backend.unnamed_sig.as_deref() == Some(&sig[..]) {
                inject_parse_complete = true;
            } else {
                if let Err(e) = backend
                    .stream
                    .write_all(parse_msg)
                    .await
                    .map_err(|e| ProxyError::Network(format!("Backend write error: {}", e)))
                {
                    conns.remove(&target);
                    return Err(e);
                }
                new_unnamed_sig = Some(sig.clone());
            }
        }

        let batch_err = match tokio::time::timeout(
            Self::BACKEND_WRITE_TIMEOUT,
            backend.stream.write_all(batch),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("Backend write error: {}", e)),
            Err(_) => Some("Backend write timeout".to_string()),
        };
        if let Some(msg) = batch_err {
            let e = ProxyError::Network(msg);
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            return Err(e);
        }

        // The client expects `ParseComplete` first; the backend won't send one
        // for a skipped Parse, so emit it here before relaying the response.
        let mut injected: u64 = 0;
        if inject_parse_complete {
            if let Err(e) = client
                .write_all(&[b'1', 0, 0, 0, 4])
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))
            {
                conns.remove(&target);
                return Err(e);
            }
            injected = 5;
        }

        let r = if wait_ready {
            Self::stream_until_ready(client, &mut backend.stream, session, state).await
        } else {
            Self::stream_flush(client, &mut backend.stream, session, state).await
        };
        match r {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(state, session, sql, &target, started.elapsed(), None)
                        .await;
                }
                // The connection now holds these named statements.
                for name in defines {
                    backend.prepared.insert(name.clone());
                }
                // ...and the (re)forwarded unnamed statement.
                if let Some(sig) = new_unnamed_sig {
                    backend.unnamed_sig = Some(sig);
                }
                Ok((Some(target), sent + injected))
            }
            Err(e) => {
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(
                        state,
                        session,
                        sql,
                        &target,
                        started.elapsed(),
                        Some(e.to_string()),
                    )
                    .await;
                }
                Err(e)
            }
        }
    }

    /// Re-issue one named `Parse` on a backend socket out-of-band: send the
    /// original `Parse` bytes followed by a `Flush`, then read and discard the
    /// single `ParseComplete` the backend emits. The statement persists on the
    /// connection (the implicit transaction is closed later by the real
    /// batch's `Sync`). An `ErrorResponse` means the re-prepare failed.
    async fn reprepare_statement<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        parse_bytes: &[u8],
    ) -> Result<()> {
        tokio::time::timeout(Self::REPREPARE_TIMEOUT, backend.write_all(parse_bytes))
            .await
            .map_err(|_| ProxyError::Network("re-prepare write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare write error: {}", e)))?;
        // Flush: 'H' + length 4.
        tokio::time::timeout(
            Self::REPREPARE_TIMEOUT,
            backend.write_all(&[b'H', 0, 0, 0, 4]),
        )
        .await
        .map_err(|_| ProxyError::Network("re-prepare flush timeout".to_string()))?
        .map_err(|e| ProxyError::Network(format!("re-prepare flush error: {}", e)))?;
        let mtype =
            tokio::time::timeout(Self::REPREPARE_TIMEOUT, Self::read_one_frame_type(backend))
                .await
                .map_err(|_| ProxyError::Network("re-prepare read timeout".to_string()))??;
        match mtype {
            b'1' => Ok(()), // ParseComplete
            b'E' => Err(ProxyError::Protocol(
                "re-prepare rejected by backend".to_string(),
            )),
            other => Err(ProxyError::Protocol(format!(
                "unexpected re-prepare reply: {}",
                other as char
            ))),
        }
    }

    /// Read exactly one backend message frame (5-byte header + body) and return
    /// its type byte, discarding the body. Used to consume the `ParseComplete`
    /// produced by an out-of-band re-prepare.
    async fn read_one_frame_type<S: AsyncReadExt + Unpin>(backend: &mut S) -> Result<u8> {
        let mut header = [0u8; 5];
        backend
            .read_exact(&mut header)
            .await
            .map_err(|e| ProxyError::Network(format!("re-prepare read error: {}", e)))?;
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let body_len = len.saturating_sub(4);
        if body_len > 0 {
            let mut body = vec![0u8; body_len];
            backend
                .read_exact(&mut body)
                .await
                .map_err(|e| ProxyError::Network(format!("re-prepare body read error: {}", e)))?;
        }
        Ok(header[0])
    }

    /// Name a `Parse` defines: its first cstring. `""` is the unnamed
    /// statement, which is per-protocol transient and never tracked.
    fn parse_stmt_name(payload: &[u8]) -> &str {
        let end = payload.iter().position(|&b| b == 0).unwrap_or(0);
        std::str::from_utf8(&payload[..end]).unwrap_or("")
    }

    /// Prepared-statement name a `Bind` references: the *second* cstring
    /// (portal name first, then statement name). `None` for the unnamed
    /// statement.
    fn bind_stmt_ref(payload: &[u8]) -> Option<&str> {
        let portal_end = payload.iter().position(|&b| b == 0)?;
        let rest = &payload[portal_end + 1..];
        let stmt_end = rest.iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&rest[..stmt_end]).ok()?;
        (!name.is_empty()).then_some(name)
    }

    /// Statement name a `Describe`/`Close` targets — only when it is
    /// statement-kind (`'S'`, not portal `'P'`). `None` otherwise.
    fn stmt_kind_name(payload: &[u8]) -> Option<&str> {
        if payload.first() != Some(&b'S') {
            return None;
        }
        let rest = &payload[1..];
        let end = rest.iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&rest[..end]).ok()?;
        (!name.is_empty()).then_some(name)
    }

    /// Stream backend response frames to the client until ReadyForQuery (end
    /// of a Sync/simple-query response). Forwards bytes verbatim, coalescing
    /// all currently-complete frames into one write and keeping only a
    /// partial-frame tail buffered, so proxy memory stays O(frame) rather
    /// than O(result). Also yields on CopyInResponse/CopyBothResponse so the
    /// client can supply COPY data. Updates `tx_state` from the RFQ status.
    /// Returns bytes streamed to the client.
    async fn stream_until_ready(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> Result<u64> {
        let _ = state;
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;

        loop {
            // Walk complete frames in `buf`, stopping at a boundary frame.
            let mut consumed = 0usize;
            let mut ready_status: Option<u8> = None;
            let mut yield_for_copy = false;
            loop {
                let rem = &buf[consumed..];
                if rem.len() < 5 {
                    break;
                }
                let len = u32::from_be_bytes([rem[1], rem[2], rem[3], rem[4]]) as usize;
                if len < 4 || rem.len() < len + 1 {
                    break; // incomplete or malformed length — need more bytes
                }
                let frame_total = len + 1;
                let mtype = rem[0];
                consumed += frame_total;
                if mtype == b'Z' {
                    // ReadyForQuery: payload is one status byte at rem[5].
                    ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                    break;
                }
                if mtype == b'G' || mtype == b'W' {
                    // CopyInResponse / CopyBothResponse: the backend now wants
                    // CopyData from the client — forward up to here and yield.
                    yield_for_copy = true;
                    break;
                }
            }

            if consumed > 0 {
                tokio::time::timeout(
                    Self::CLIENT_WRITE_TIMEOUT,
                    client.write_all(&buf[..consumed]),
                )
                .await
                .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                sent += consumed as u64;
                let _ = buf.split_to(consumed);
            }

            if let Some(status) = ready_status {
                let st = TransactionStatus::from_byte(status);
                session.in_transaction.store(
                    st != TransactionStatus::Idle,
                    std::sync::atomic::Ordering::Relaxed,
                );
                return Ok(sent);
            }
            if yield_for_copy {
                // The backend now awaits CopyData from the client; the session
                // is mid-COPY, not at a clean boundary. Mark it so pool release
                // is suppressed until the COPY drains (cleared in the CopyDone
                // path). Harmless in session mode (release is a no-op there).
                session
                    .copy_in_progress
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return Ok(sent);
            }

            // Read straight into the frame accumulator — no zeroed scratch, no
            // copy. `read_buf` appends to `buf`'s spare capacity.
            buf.reserve(16384);
            let n = tokio::time::timeout(Duration::from_secs(30), backend.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "Backend closed mid-response".to_string(),
                ));
            }
        }
    }

    /// Like `stream_until_ready` but also captures the full response bytes for
    /// caching. Returns `(bytes_sent, captured, cacheable, row_count)`.
    /// `cacheable` is false if the response carried an `ErrorResponse`, ended in
    /// a non-idle transaction status, or yielded for COPY — none of which may
    /// be cached.
    #[cfg(feature = "query-cache")]
    async fn stream_until_ready_capture(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
    ) -> Result<(u64, Vec<u8>, bool, usize)> {
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut captured: Vec<u8> = Vec::with_capacity(4096);
        let mut had_error = false;
        let mut row_count: usize = 0;

        loop {
            let mut consumed = 0usize;
            let mut ready_status: Option<u8> = None;
            let mut yield_for_copy = false;
            loop {
                let rem = &buf[consumed..];
                if rem.len() < 5 {
                    break;
                }
                let len = u32::from_be_bytes([rem[1], rem[2], rem[3], rem[4]]) as usize;
                if len < 4 || rem.len() < len + 1 {
                    break;
                }
                let frame_total = len + 1;
                let mtype = rem[0];
                if mtype == b'E' {
                    had_error = true;
                }
                if mtype == b'C' {
                    // CommandComplete tag, e.g. "SELECT 5" — take the row count.
                    if let Some(tag) = rem.get(5..frame_total) {
                        if let Some(end) = tag.iter().position(|&b| b == 0) {
                            if let Ok(s) = std::str::from_utf8(&tag[..end]) {
                                if let Some(n) =
                                    s.rsplit(' ').next().and_then(|x| x.parse::<usize>().ok())
                                {
                                    row_count = n;
                                }
                            }
                        }
                    }
                }
                consumed += frame_total;
                if mtype == b'Z' {
                    ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                    break;
                }
                if mtype == b'G' || mtype == b'W' {
                    yield_for_copy = true;
                    break;
                }
            }

            if consumed > 0 {
                tokio::time::timeout(
                    Self::CLIENT_WRITE_TIMEOUT,
                    client.write_all(&buf[..consumed]),
                )
                .await
                .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                captured.extend_from_slice(&buf[..consumed]);
                sent += consumed as u64;
                let _ = buf.split_to(consumed);
            }

            if let Some(status) = ready_status {
                let st = TransactionStatus::from_byte(status);
                session.in_transaction.store(
                    st != TransactionStatus::Idle,
                    std::sync::atomic::Ordering::Relaxed,
                );
                let cacheable = !had_error && status == b'I';
                return Ok((sent, captured, cacheable, row_count));
            }
            if yield_for_copy {
                session
                    .copy_in_progress
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                return Ok((sent, captured, false, row_count));
            }

            // Read straight into the frame accumulator — no zeroed scratch.
            buf.reserve(16384);
            let n = tokio::time::timeout(Duration::from_secs(30), backend.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "Backend closed mid-response".to_string(),
                ));
            }
        }
    }

    /// Relay whatever the backend has *already* produced in response to a
    /// `Flush` (which, unlike `Sync`, yields no ReadyForQuery), then return
    /// immediately — without waiting.
    ///
    /// Any Flush output that has not landed in the socket yet is delivered by
    /// the main loop's backend watch (which relays the current backend's
    /// out-of-band bytes while waiting for the client), so there is no fixed
    /// post-Flush stall: the previous version blocked the session loop for up to
    /// 200 ms after the last backend byte before it would read the client's next
    /// message, adding that latency to every `Parse`/`Flush`-then-`Bind` prepare
    /// cycle. Here we drain what is instantly available and hand control back;
    /// the client's next frames are read at once. The eventual `Sync` drains the
    /// final ReadyForQuery via `stream_until_ready`.
    async fn stream_flush(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> Result<u64> {
        let _ = (session, state);
        let mut read_buf = vec![0u8; 16384];
        let mut sent: u64 = 0;
        loop {
            match backend.try_read(&mut read_buf) {
                Ok(0) => {
                    return Err(ProxyError::Connection(
                        "Backend closed mid-flush".to_string(),
                    ))
                }
                Ok(n) => {
                    client
                        .write_all(&read_buf[..n])
                        .await
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    sent += n as u64;
                }
                // Nothing more instantly available — the backend watch delivers
                // any remaining Flush output as it arrives.
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(sent),
                Err(e) => return Err(ProxyError::Network(format!("Backend read error: {}", e))),
            }
        }
    }

    /// Check if a message is a write operation
    fn is_write_message(msg: &Message) -> bool {
        match msg.msg_type {
            MessageType::Query => {
                // Borrow the SQL straight out of the payload — the
                // message is forwarded verbatim, so no copy is needed
                // just to inspect the leading keyword.
                crate::protocol::query_text(&msg.payload)
                    .map(Self::is_write_query)
                    .unwrap_or(false)
            }
            MessageType::Parse => {
                // Parse payload = statement-name cstring + query
                // cstring; skip the name and borrow the query.
                msg.payload
                    .iter()
                    .position(|&b| b == 0)
                    .and_then(|end| crate::protocol::query_text(&msg.payload[end + 1..]))
                    .map(Self::is_write_query)
                    .unwrap_or(false)
            }
            // Execute, Bind, etc. maintain the current connection
            _ => false,
        }
    }

    /// Check if SQL query is a write operation
    fn is_write_query(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let trimmed = sql.trim();

        // Write operations
        if starts_with_ci(trimmed, "INSERT")
            || starts_with_ci(trimmed, "UPDATE")
            || starts_with_ci(trimmed, "DELETE")
            || starts_with_ci(trimmed, "CREATE")
            || starts_with_ci(trimmed, "DROP")
            || starts_with_ci(trimmed, "ALTER")
            || starts_with_ci(trimmed, "TRUNCATE")
            || starts_with_ci(trimmed, "GRANT")
            || starts_with_ci(trimmed, "REVOKE")
            || starts_with_ci(trimmed, "VACUUM")
            || starts_with_ci(trimmed, "REINDEX")
            || starts_with_ci(trimmed, "CLUSTER")
        {
            return true;
        }

        // Transaction control goes to current node
        if starts_with_ci(trimmed, "BEGIN")
            || starts_with_ci(trimmed, "START")
            || starts_with_ci(trimmed, "COMMIT")
            || starts_with_ci(trimmed, "ROLLBACK")
            || starts_with_ci(trimmed, "SAVEPOINT")
            || starts_with_ci(trimmed, "RELEASE")
        {
            return true;
        }

        // SET commands go to primary to maintain session state
        if starts_with_ci(trimmed, "SET") && !starts_with_ci(trimmed, "SET TRANSACTION READ ONLY") {
            return true;
        }

        false
    }

    /// Conservative classifier for the conditional-reset optimisation: could
    /// this forwarded simple-query SQL leave *session-level* state on the
    /// backend connection that `DISCARD ALL` would need to clear before another
    /// client reuses it (a `SET`/GUC, temp table, prepared statement, cursor
    /// WITH HOLD, `LISTEN`, advisory lock, session authorization, …)?
    ///
    /// Biased hard toward `true`. A false negative (calling a dirtying
    /// statement clean) would leak state to the next borrower — a correctness
    /// and security bug — so only statements *provably* session-neutral return
    /// `false`; everything ambiguous returns `true` (forcing the full reset,
    /// which is merely slower, never unsafe).
    ///
    /// Known, documented limitation: a `SELECT` that calls a user-defined
    /// function which internally runs `set_config(..., is_local => false)` or
    /// takes an advisory lock via an aliased path is NOT detectable from the
    /// SQL text. The direct forms (`set_config`, `pg_advisory*`, `nextval`,
    /// `setval`) ARE caught. This is why `skip_clean_reset` is opt-in and
    /// intended for autocommit/simple-protocol workloads.
    #[cfg(feature = "pool-modes")]
    fn stmt_leaves_session_state(sql: &str) -> bool {
        use crate::protocol::{contains_ci, starts_with_ci};
        let t = sql.trim();
        if t.is_empty() {
            return false;
        }
        // Multiple statements in one simple-query string: a leading-keyword
        // check cannot vouch for what follows a `;`, so treat any non-trailing
        // `;` as dirtying. A `;` inside a string literal also trips this —
        // safe, merely an unnecessary reset.
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return true;
        }
        // The statement's leading keyword must be one that provably leaves no
        // session state. CREATE / SET / PREPARE / DECLARE / LISTEN / DISCARD /
        // RESET / GRANT / ALTER / LOCK / COPY / … are all absent here, so they
        // fall through to `true` (dirtying).
        let neutral_lead = starts_with_ci(core, "SELECT")
            || starts_with_ci(core, "INSERT")
            || starts_with_ci(core, "UPDATE")
            || starts_with_ci(core, "DELETE")
            || starts_with_ci(core, "WITH")
            || starts_with_ci(core, "VALUES")
            || starts_with_ci(core, "TABLE")
            || starts_with_ci(core, "SHOW")
            || starts_with_ci(core, "EXPLAIN")
            || starts_with_ci(core, "FETCH")
            || starts_with_ci(core, "BEGIN")
            || starts_with_ci(core, "START")
            || starts_with_ci(core, "COMMIT")
            || starts_with_ci(core, "END")
            || starts_with_ci(core, "ROLLBACK")
            || starts_with_ci(core, "ABORT")
            || starts_with_ci(core, "SAVEPOINT")
            || starts_with_ci(core, "RELEASE");
        if !neutral_lead {
            return true;
        }
        // A neutral-lead statement can still create session state:
        //  * `SELECT ... INTO [TEMP] t` (and the `WITH … SELECT … INTO` form)
        //    creates a table. The `INTO` keyword is matched as a whole word (so
        //    a column name like `into_total` does not trip it) and ONLY for
        //    SELECT/WITH leads — `INSERT INTO`, `UPDATE`, `DELETE` use `INTO`
        //    (or not) as ordinary syntax and leave no session state.
        //  * `set_config()` sets a GUC; `pg_advisory*` takes a session lock;
        //    `nextval`/`setval` touch the per-session sequence cache.
        if (starts_with_ci(core, "SELECT") || starts_with_ci(core, "WITH"))
            && Self::contains_word_ci(core, "into")
        {
            return true;
        }
        const DIRTY_TOKENS: [&str; 4] = ["set_config", "advisory", "nextval", "setval"];
        DIRTY_TOKENS.iter().any(|tok| contains_ci(core, tok))
    }

    /// Case-insensitive whole-word (ASCII identifier-boundary) search — a match
    /// requires the token to be bounded by a non-`[A-Za-z0-9_]` char (or the
    /// string edge) on both sides, so a real SQL keyword like `INTO` is caught
    /// regardless of surrounding whitespace while an identifier substring
    /// (`into_total`) is not.
    #[cfg(feature = "pool-modes")]
    fn contains_word_ci(haystack: &str, word: &str) -> bool {
        let hb = haystack.as_bytes();
        let wb = word.as_bytes();
        if wb.is_empty() || hb.len() < wb.len() {
            return false;
        }
        let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        let mut i = 0;
        while i + wb.len() <= hb.len() {
            if hb[i..i + wb.len()].eq_ignore_ascii_case(wb) {
                let before_ok = i == 0 || !is_ident(hb[i - 1]);
                let after = i + wb.len();
                let after_ok = after == hb.len() || !is_ident(hb[after]);
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
        false
    }

    /// Derive the rate-limit bucket key for a session per the configured
    /// keying dimension.
    #[cfg(feature = "rate-limiting")]
    async fn rate_limit_key(
        session: &Arc<ClientSession>,
        config: &ProxyConfig,
    ) -> crate::rate_limit::LimiterKey {
        use crate::config::RateLimitKeyBy;
        use crate::rate_limit::LimiterKey;
        match config.rate_limit.key_by {
            RateLimitKeyBy::Global => LimiterKey::Global,
            RateLimitKeyBy::ClientIp => LimiterKey::ClientIp(session.client_addr.ip()),
            RateLimitKeyBy::Database => {
                let vars = session.variables.read().await;
                LimiterKey::Database(vars.get("database").cloned().unwrap_or_default())
            }
            RateLimitKeyBy::User => {
                let vars = session.variables.read().await;
                LimiterKey::User(vars.get("user").cloned().unwrap_or_default())
            }
        }
    }

    /// Check rate limits before a query is forwarded. Returns `Some(bytes)` —
    /// a PG `ErrorResponse` WITHOUT a trailing `ReadyForQuery` (the caller
    /// appends one as the protocol requires) — when the query is denied; `None`
    /// when it may proceed. A throttle/queue verdict is honored by sleeping for
    /// the engine-supplied delay (real backpressure, capped) and then allowing.
    #[cfg(feature = "rate-limiting")]
    async fn rate_limit_check(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Option<Vec<u8>> {
        use crate::rate_limit::RateLimitResult;
        let limiter = state.rate_limiter.as_ref()?;
        let key = Self::rate_limit_key(session, config).await;
        match limiter.check(&key, 1) {
            RateLimitResult::Allowed => None,
            RateLimitResult::Warned(msg) => {
                tracing::warn!(key = %key, reason = %msg, "rate limit warning");
                None
            }
            RateLimitResult::Throttled(d) | RateLimitResult::Queued(d) => {
                // Cap the backpressure sleep so a misconfiguration can't pin a
                // connection task indefinitely.
                tokio::time::sleep(d.min(Duration::from_secs(5))).await;
                None
            }
            RateLimitResult::Denied(exc) => {
                tracing::info!(key = %key, "rate limit exceeded");
                let msg = format!(
                    "rate limit exceeded: {} (retry after {}ms)",
                    exc.message,
                    exc.retry_after.as_millis()
                );
                Some(Self::create_error_response("53400", &msg))
            }
        }
    }

    /// In-band failure feedback. When a query fails against a backend, demote
    /// that node's health *immediately* — a copy-on-write update of the shared
    /// health snapshot, the same structure the periodic health checker
    /// maintains — so routing stops sending work to a dead node within one
    /// query instead of waiting up to a full health-check interval (the
    /// ~`check_interval` blind window). The periodic checker restores the node
    /// on its next successful probe, so this only ever *accelerates* detection.
    ///
    /// True when `err` is evidence the backend itself is unhealthy — and so
    /// should demote it in-band (and trip its circuit breaker) — as opposed to a
    /// client-side problem or a merely slow but healthy query.
    ///
    /// Excluded (return `false`, no penalty):
    /// * `Client …` — a failed or timed-out client write is the client's fault.
    /// * `Backend read timeout` — a backend that emits no bytes within the
    ///   streaming read window is indistinguishable from a legitimately slow but
    ///   healthy query (large sort/aggregate, lock wait, bulk DML). Demoting the
    ///   whole node — cluster-wide, for every session, bypassing the configured
    ///   `failure_threshold` — over one slow query is a false positive; a
    ///   genuinely unresponsive-but-connected backend is still caught by the
    ///   periodic protocol-level health probe.
    ///
    /// Still faults (return `true`): a backend read/write *error* (reset, EOF,
    /// broken pipe), a backend *write* timeout (the backend is not draining its
    /// socket), and any connect-time failure.
    fn is_backend_fault(err: &str) -> bool {
        !err.contains("Client") && !err.contains("Backend read timeout")
    }

    /// Errors that do not demote a backend are filtered via `is_backend_fault`:
    /// a client disconnecting mid-query, or one merely-slow query, must never
    /// take a healthy backend out of rotation for every session.
    fn note_backend_failure(state: &Arc<ServerState>, addr: &str, err: &str) {
        if !Self::is_backend_fault(err) {
            return;
        }
        // Serialize the read-modify-write of the shared health snapshot. ArcSwap
        // makes only the final pointer swap atomic; without this lock two
        // concurrent writers — in-band demotions for different nodes, or an
        // in-band demotion racing the periodic checker's full-map rebuild — can
        // each load the same snapshot and clobber the other's update (a lost
        // update that resurrects a demoted node, or evicts a recovered one,
        // until the next probe). The lock serializes writers only; every routing
        // read stays lock-free on the ArcSwap.
        let _writers = state.health_write.lock();
        let snapshot = state.health.load_full();
        // Only act (and pay the clone) when the node is currently marked
        // healthy — avoids churning the snapshot on an already-down node.
        if snapshot.get(addr).map(|h| h.healthy).unwrap_or(false) {
            let mut next = (*snapshot).clone();
            if let Some(nh) = next.get_mut(addr) {
                nh.healthy = false;
                nh.failure_count = nh.failure_count.saturating_add(1);
                nh.last_error = Some(format!("in-band failure: {}", err));
                tracing::warn!(
                    node = %addr,
                    error = %err,
                    "in-band failure — node marked unhealthy for fast failover"
                );
            }
            state.health.store(Arc::new(next));
        }
    }

    /// Record a backend forward failure: demote the node's health in-band AND
    /// (when the feature is on) trip its circuit breaker — the single place the
    /// data path reports "this backend just failed". Both signals consult the
    /// same `is_backend_fault` classifier, so they can never drift apart: a
    /// client-side error or a slow-query read timeout penalizes neither.
    fn record_backend_failure(state: &Arc<ServerState>, node: &str, err: &str) {
        Self::note_backend_failure(state, node, err);
        #[cfg(feature = "circuit-breaker")]
        if Self::is_backend_fault(err) {
            Self::circuit_record(state, node, false, err);
        }
    }

    /// True when `node`'s circuit is open (avoid it / fast-fail). A half-open
    /// circuit returns false so a probe query is admitted.
    #[cfg(feature = "circuit-breaker")]
    fn circuit_is_open(state: &Arc<ServerState>, node: &str) -> bool {
        state
            .circuit_breaker
            .as_ref()
            .map(|cb| {
                cb.get_breaker(node).get_state() == crate::circuit_breaker::CircuitState::Open
            })
            .unwrap_or(false)
    }

    /// Record the outcome of a forward to `node` on its circuit breaker.
    #[cfg(feature = "circuit-breaker")]
    fn circuit_record(state: &Arc<ServerState>, node: &str, success: bool, err: &str) {
        if let Some(cb) = state.circuit_breaker.as_ref() {
            let breaker = cb.get_breaker(node);
            if success {
                breaker.record_success();
            } else {
                breaker.record_failure(err);
            }
        }
    }

    /// If `node`'s circuit is open, build the fast-fail `ErrorResponse` (without
    /// a trailing `ReadyForQuery` — the caller appends one). `None` when the
    /// circuit is closed or half-open and the request may proceed.
    #[cfg(feature = "circuit-breaker")]
    fn circuit_fast_fail(state: &Arc<ServerState>, node: &str) -> Option<Vec<u8>> {
        if Self::circuit_is_open(state, node) {
            tracing::info!(node = %node, "circuit open — fast-failing");
            Some(Self::create_error_response(
                "08006",
                &format!("circuit open for node {node}: backend temporarily unavailable"),
            ))
        } else {
            None
        }
    }

    /// Read-your-writes decision: should reads be pinned to the primary given
    /// the session's last write and the configured window? Pure for testing.
    #[cfg(feature = "lag-routing")]
    fn ryw_pins_primary(last_write: Option<std::time::Instant>, window_ms: u64) -> bool {
        window_ms > 0
            && last_write
                .map(|t| t.elapsed() < Duration::from_millis(window_ms))
                .unwrap_or(false)
    }

    /// Lag-exclusion decision: should a standby be dropped from read routing
    /// given its measured lag and the configured ceiling? `max=0` disables
    /// exclusion; unknown lag (None) never excludes. Pure for testing.
    #[cfg(feature = "lag-routing")]
    fn lag_excludes_standby(lag_bytes: Option<u64>, max_lag_bytes: u64) -> bool {
        max_lag_bytes > 0 && lag_bytes.map(|l| l > max_lag_bytes).unwrap_or(false)
    }

    /// Pure predicate: is `sql` a plain, deterministic SELECT safe to cache?
    /// (Not WITH/locking/volatile.) Transaction state is checked separately.
    #[cfg(feature = "query-cache")]
    fn is_cacheable_read_sql(sql: &str) -> bool {
        use crate::protocol::{contains_ci, starts_with_ci};
        let t = sql.trim_start();
        if !starts_with_ci(t, "SELECT") {
            return false;
        }
        if contains_ci(t, "FOR UPDATE") || contains_ci(t, "FOR SHARE") {
            return false;
        }
        // Non-deterministic reads must not be reused.
        const VOLATILE: [&str; 10] = [
            "now(",
            "current_timestamp",
            "current_date",
            "current_time",
            "clock_timestamp",
            "statement_timestamp",
            "random(",
            "nextval(",
            "uuid_generate",
            "gen_random_uuid",
        ];
        !VOLATILE.iter().any(|v| contains_ci(t, v))
    }

    /// Decide whether a read query is safe to serve from / store in the cache,
    /// and build its `CacheContext`. Returns `None` for anything not a plain,
    /// deterministic, non-transactional SELECT.
    #[cfg(feature = "query-cache")]
    async fn cacheable_read_ctx(
        session: &Arc<ClientSession>,
        sql: &str,
    ) -> Option<crate::cache::CacheContext> {
        if !Self::is_cacheable_read_sql(sql) {
            return None;
        }
        // Never cache mid-transaction (visibility would be wrong).
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        let (user, database) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned(),
                vars.get("database")
                    .cloned()
                    .unwrap_or_else(|| "default".to_string()),
            )
        };
        Some(crate::cache::CacheContext {
            database,
            user,
            branch: None,
            connection_id: Some(session.id.as_u64_pair().0),
        })
    }

    /// Build a multi-tenancy `RequestContext` from the session's startup
    /// parameters (user, database, application_name, ...) so the configured
    /// identifier can resolve the tenant.
    #[cfg(feature = "multi-tenancy")]
    async fn tenant_request_ctx(
        session: &Arc<ClientSession>,
    ) -> crate::multi_tenancy::RequestContext {
        let vars = session.variables.read().await;
        crate::multi_tenancy::RequestContext {
            headers: vars.clone(),
            username: vars.get("user").cloned(),
            database: vars.get("database").cloned(),
            auth_token: None,
            sql_context: HashMap::new(),
            client_ip: Some(session.client_addr.ip().to_string()),
            connection_id: Some(session.id.as_u64_pair().0),
        }
    }

    /// Journal a successful write statement (Transaction Replay). Each write is
    /// recorded as its own auto-commit transaction so the time-travel/failover
    /// replay engine can re-apply it onto a promoted primary or a staging
    /// target. Best-effort: journal errors never fail the client query.
    #[cfg(feature = "ha-tr")]
    async fn journal_write(state: &Arc<ServerState>, session: &Arc<ClientSession>, sql: &str) {
        let tx_id = uuid::Uuid::new_v4();
        let j = &state.transaction_journal;
        if j.begin_transaction(tx_id, session.id, crate::NodeId::new(), 0)
            .await
            .is_ok()
        {
            let _ = j
                .log_statement(tx_id, sql.to_string(), Vec::new(), None, None, 0)
                .await;
        }
    }

    /// Record a forwarded query on the analytics engine (fingerprint, latency,
    /// slow-query log, pattern detection). No-op when analytics is disabled.
    #[cfg(feature = "query-analytics")]
    async fn record_analytics(
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
        sql: &str,
        node: &str,
        duration: Duration,
        error: Option<String>,
    ) {
        let Some(analytics) = state.analytics.as_ref() else {
            return;
        };
        let (user, database) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned().unwrap_or_default(),
                vars.get("database").cloned().unwrap_or_default(),
            )
        };
        let mut exec = crate::analytics::QueryExecution::new(sql, duration);
        exec.user = user;
        exec.database = database;
        exec.client_ip = session.client_addr.ip().to_string();
        exec.node = node.to_string();
        exec.session_id = Some(session.id.to_string());
        exec.error = error;
        analytics.record(exec);
    }

    /// Select primary node with write timeout during failover
    async fn select_primary_with_timeout(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        let timeout = config.write_timeout();
        let start = std::time::Instant::now();
        // Poll for the promoted primary fairly tightly so writes resume
        // quickly after a failover (was 500ms — a needless recovery floor).
        let check_interval = Duration::from_millis(100);

        loop {
            // Try to find healthy primary
            let health = state.health.load_full();
            let primary = config
                .nodes
                .iter()
                .find(|n| n.role == NodeRole::Primary && n.enabled);

            if let Some(primary_node) = primary {
                if let Some(node_health) = health.get(&primary_node.address()) {
                    if node_health.healthy {
                        // Update session's current node
                        let mut current = session.current_node.write().await;
                        *current = Some(primary_node.address());
                        return Ok(primary_node.address());
                    }
                }
            }
            drop(health);

            // Check if timeout exceeded
            if start.elapsed() >= timeout {
                state.metrics.failovers.fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::NoHealthyNodes);
            }

            tracing::warn!(
                "Primary unavailable, waiting for failover... ({:.1}s elapsed, {:.1}s timeout)",
                start.elapsed().as_secs_f64(),
                timeout.as_secs_f64()
            );

            // Wait before retry
            tokio::time::sleep(check_interval).await;
        }
    }

    /// Select node for read operations with load balancing
    async fn select_read_node(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // If in transaction, stick to current node
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(node) = session.current_node.read().await.clone() {
                return Ok(node);
            }
        }

        // Get healthy nodes (prefer standbys for reads)
        let health = state.health.load_full();
        let healthy_standbys: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| {
                let base = n.enabled
                    && (n.role == NodeRole::Standby || n.role == NodeRole::ReadReplica)
                    && health.get(&n.address()).map(|h| h.healthy).unwrap_or(false);
                // Drop a standby whose circuit is open so reads avoid it.
                #[cfg(feature = "circuit-breaker")]
                let base = base && !Self::circuit_is_open(state, &n.address());
                // Drop a standby lagging beyond the configured byte threshold.
                #[cfg(feature = "lag-routing")]
                let base = base
                    && !Self::lag_excludes_standby(
                        health
                            .get(&n.address())
                            .and_then(|h| h.replication_lag_bytes),
                        config.lag_routing.max_lag_bytes,
                    );
                base
            })
            .collect();

        if !healthy_standbys.is_empty() {
            // Round-robin across healthy standbys
            let ticket = state.lb_state.rr_counter.fetch_add(1, Ordering::Relaxed);
            let index = ticket as usize % healthy_standbys.len();
            let node_addr = healthy_standbys[index].address();

            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to primary if no healthy standbys
        Self::select_node(session, state, config).await
    }

    /// Complete backend authentication by reading until ReadyForQuery
    /// This is used when switching backends - we don't forward auth to client
    async fn complete_backend_auth(backend: &mut TcpStream) -> Result<()> {
        let mut buffer = BytesMut::with_capacity(4096);
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(ProxyError::Auth(
                    "Backend authentication timeout".to_string(),
                ));
            }

            buffer.reserve(4096);
            let n = tokio::time::timeout(Duration::from_secs(5), backend.read_buf(&mut buffer))
                .await
                .map_err(|_| ProxyError::Auth("Read timeout during backend auth".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend auth read error: {}", e)))?;

            if n == 0 {
                return Err(ProxyError::Connection(
                    "Backend closed during auth".to_string(),
                ));
            }

            // Walk complete frames by raw tag. The wire decoder is
            // direction-agnostic ('E' decodes to the client-side `Execute`), so
            // a backend ErrorResponse must be detected by its raw tag rather
            // than by `msg_type` — the previous version matched
            // `MessageType::ErrorResponse`, which never fired, so a failed
            // backend auth surfaced as a misleading timeout.
            loop {
                if buffer.len() < 5 {
                    break;
                }
                let len = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                if len < 4 || buffer.len() < len + 1 {
                    break;
                }
                let tag = buffer[0];
                let frame = buffer.split_to(len + 1);
                match tag {
                    // ReadyForQuery: authentication complete.
                    b'Z' => return Ok(()),
                    // ErrorResponse: parse its message for a clear error.
                    b'E' => {
                        let payload = BytesMut::from(&frame[5..]);
                        let err = ErrorResponse::parse(payload)
                            .map(|e| e.message().unwrap_or("Unknown error").to_string())
                            .unwrap_or_else(|_| "authentication failed".to_string());
                        return Err(ProxyError::Auth(err));
                    }
                    _ => {}
                }
            }
        }
    }

    /// Create PostgreSQL error response message
    fn create_error_response(code: &str, message: &str) -> Vec<u8> {
        let mut fields = HashMap::new();
        fields.insert('S', "ERROR".to_string());
        fields.insert('V', "ERROR".to_string());
        fields.insert('C', code.to_string());
        fields.insert('M', message.to_string());

        let err = ErrorResponse { fields };
        err.encode().encode().to_vec()
    }

    /// Create a `ReadyForQuery` frame with the given transaction-status byte
    /// (`b'I'` = idle, `b'T'` = in transaction, `b'E'` = failed transaction).
    fn create_ready_for_query(status: u8) -> Vec<u8> {
        let mut payload = BytesMut::with_capacity(1);
        payload.put_u8(status);
        Message::new(MessageType::ReadyForQuery, payload)
            .encode()
            .to_vec()
    }

    /// Synthesise a full PostgreSQL simple-query response from a cached
    /// payload produced by a plugin's `PreQueryResult::Cached`.
    ///
    /// # Payload format
    ///
    /// The plugin is expected to serialise a JSON document of the form:
    ///
    /// ```json
    /// {
    ///   "columns": [
    ///     {"name": "id",    "oid": 23},
    ///     {"name": "email", "oid": 25}
    ///   ],
    ///   "rows": [
    ///     ["1", "alice@example.com"],
    ///     ["2", null]
    ///   ]
    /// }
    /// ```
    ///
    /// `oid` is the PostgreSQL type OID (`23` = int4, `25` = text,
    /// `20` = int8, `16` = bool, `1184` = timestamptz, etc.). Row values
    /// are strings in text format; `null` encodes a SQL NULL. The type
    /// OID is advisory — pgwire clients accept `25` (text) universally
    /// and cast as needed.
    ///
    /// # Returned bytes
    ///
    /// One concatenated PostgreSQL wire response:
    ///
    /// ```text
    /// RowDescription (T) + DataRow (D) × N + CommandComplete (C: "SELECT N")
    ///                    + ReadyForQuery (Z: idle)
    /// ```
    ///
    /// Returns an error on malformed JSON; the caller falls back to
    /// backend forwarding.
    #[cfg(feature = "wasm-plugins")]
    fn synthesise_cached_response(bytes: &[u8]) -> Result<Vec<u8>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct CachedPayload {
            columns: Vec<ColumnDef>,
            rows: Vec<Vec<Option<String>>>,
        }

        #[derive(Deserialize)]
        struct ColumnDef {
            name: String,
            #[serde(default = "default_text_oid")]
            oid: u32,
        }

        fn default_text_oid() -> u32 {
            25 // text
        }

        let payload: CachedPayload = serde_json::from_slice(bytes)
            .map_err(|e| ProxyError::Protocol(format!("invalid cached payload JSON: {}", e)))?;

        if payload.columns.is_empty() {
            return Err(ProxyError::Protocol(
                "cached payload must declare at least one column".to_string(),
            ));
        }

        let mut reply = Vec::new();

        // RowDescription (tag 'T')
        let mut rd = BytesMut::new();
        rd.put_u16(payload.columns.len() as u16);
        for col in &payload.columns {
            rd.extend_from_slice(col.name.as_bytes());
            rd.put_u8(0); // cstring terminator
            rd.put_i32(0); // tableOID (unknown)
            rd.put_i16(0); // columnNumber (unknown)
            rd.put_u32(col.oid);
            rd.put_i16(-1); // typeLen (unspecified)
            rd.put_i32(-1); // typeMod (unspecified)
            rd.put_i16(0); // format code: text
        }
        reply.extend_from_slice(&Message::new(MessageType::RowDescription, rd).encode());

        // DataRow (tag 'D') per row
        let column_count = payload.columns.len();
        for row in &payload.rows {
            if row.len() != column_count {
                return Err(ProxyError::Protocol(format!(
                    "cached row has {} values but {} columns are declared",
                    row.len(),
                    column_count
                )));
            }
            let mut dr = BytesMut::new();
            dr.put_u16(row.len() as u16);
            for value in row {
                match value {
                    Some(s) => {
                        dr.put_i32(s.len() as i32);
                        dr.extend_from_slice(s.as_bytes());
                    }
                    None => {
                        dr.put_i32(-1); // NULL sentinel
                    }
                }
            }
            reply.extend_from_slice(&Message::new(MessageType::DataRow, dr).encode());
        }

        // CommandComplete (tag 'C')
        let tag = format!("SELECT {}", payload.rows.len());
        let mut cc = BytesMut::new();
        cc.extend_from_slice(tag.as_bytes());
        cc.put_u8(0);
        reply.extend_from_slice(&Message::new(MessageType::CommandComplete, cc).encode());

        // ReadyForQuery (tag 'Z', status 'I' idle)
        reply.extend_from_slice(&Self::create_ready_for_query(b'I'));

        Ok(reply)
    }

    /// Run the pre-query plugin hook on a client message.
    ///
    /// When the `wasm-plugins` feature is off, or the plugin manager has no
    /// loaded plugins, this is a zero-cost passthrough that returns the
    /// message untouched with `PreQueryAction::Forward`.
    ///
    /// Only simple-query (`MessageType::Query`) messages are inspected today.
    /// Extended-protocol messages (`Parse`/`Bind`/`Execute`) are passed
    /// through unchanged — a future task wires them in.
    fn apply_pre_query_hook(
        msg: Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) -> (Message, PreQueryAction) {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return (msg, PreQueryAction::Forward),
            };

            if msg.msg_type != MessageType::Query {
                return (msg, PreQueryAction::Forward);
            }

            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::PreQuery) {
                return (msg, PreQueryAction::Forward);
            }

            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return (msg, PreQueryAction::Forward),
            };

            let ctx = Self::build_query_context(&query_msg.query, session);

            match pm.execute_pre_query(&ctx) {
                PreQueryResult::Continue => (msg, PreQueryAction::Forward),
                PreQueryResult::Block(reason) => (msg, PreQueryAction::Block(reason)),
                PreQueryResult::Rewrite(new_sql) => {
                    let rewritten = QueryMessage { query: new_sql }.encode();
                    (rewritten, PreQueryAction::Forward)
                }
                PreQueryResult::Cached(bytes) => (msg, PreQueryAction::Cached(bytes)),
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (state, session);
            (msg, PreQueryAction::Forward)
        }
    }

    /// Feed the anomaly detector a per-query observation. Cheap —
    /// only the SQL-injection scan and the novel-fingerprint check
    /// are non-trivial, both well under a microsecond on
    /// representative queries. Returns nothing; detections land in
    /// the detector's ring buffer and are surfaced via /api/anomalies.
    #[cfg(feature = "anomaly-detection")]
    fn record_anomaly_observation(
        msg: &Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        if msg.msg_type != MessageType::Query {
            return;
        }
        // Borrow the SQL straight out of the payload — the message is
        // forwarded verbatim, so no deep copy of the frame is needed.
        if let Some(query) = crate::protocol::query_text(&msg.payload) {
            Self::record_anomaly_sql(query, state, session);
        }
    }

    /// Feed one SQL statement to the anomaly detector. Shared by the
    /// simple-query path and the extended-protocol `Parse` path so
    /// prepared-statement traffic is observed too.
    #[cfg(feature = "anomaly-detection")]
    fn record_anomaly_sql(query: &str, state: &Arc<ServerState>, session: &Arc<ClientSession>) {
        // Tenant identifier is the most-specific known per-session
        // attribute the proxy can attribute traffic to. Multi-tenancy
        // sets `tenant_id` in `variables`; otherwise we fall back to
        // the client address. session.variables is a tokio RwLock but this
        // is a sync helper — try_read avoids an await; on contention we
        // fall back to the client IP, still a valid per-source identifier.
        let tenant = match session.variables.try_read() {
            Ok(vars) => vars
                .get("tenant_id")
                .or_else(|| vars.get("user"))
                .cloned()
                .unwrap_or_else(|| session.client_addr.ip().to_string()),
            Err(_) => session.client_addr.ip().to_string(),
        };
        let fingerprint = anomaly_fingerprint(query);
        let obs = crate::anomaly::QueryObservation {
            tenant,
            fingerprint,
            sql: query.to_string(),
            timestamp: std::time::Instant::now(),
        };
        for ev in state.anomaly_detector.record_query(&obs) {
            tracing::warn!(anomaly = ?ev, "anomaly detected");
        }
    }

    /// Send the client a `Block`-outcome response: an error frame plus
    /// `ReadyForQuery` so the client's state machine returns to idle and
    /// the next query can be accepted.
    async fn send_block_response(
        stream: &mut ClientStream,
        reason: &str,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        let err =
            Self::create_error_response("42000", &format!("Query blocked by plugin: {}", reason));
        stream
            .write_all(&err)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        let rfq = Self::create_ready_for_query(b'I');
        stream
            .write_all(&rfq)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        state
            .metrics
            .bytes_sent
            .fetch_add((err.len() + rfq.len()) as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Build a `QueryContext` for the plugin hook. Populated fields: `query`
    /// (verbatim), `is_read_only` (derived from SQL verb), and `hook_context`
    /// with the session id as `client_id`. `normalized` and `tables` are
    /// left as cheap stand-ins until the analytics normaliser is wired in
    /// (T0-d, unified context).
    #[cfg(feature = "wasm-plugins")]
    fn build_query_context(query: &str, session: &Arc<ClientSession>) -> QueryContext {
        let is_read_only = !Self::is_write_query(query);
        let hook_context = HookContext {
            client_id: Some(session.id.to_string()),
            ..HookContext::default()
        };
        QueryContext {
            query: query.to_string(),
            normalized: query.to_string(),
            tables: Vec::new(),
            is_read_only,
            hook_context,
        }
    }

    /// Run the Authenticate plugin hook at startup. Called from
    /// `connect_and_authenticate` before any backend connection.
    ///
    /// Behaviour by `AuthResult`:
    /// * `Defer` — no plugin opinion; proceed with the default
    ///   PostgreSQL auth flow unchanged.
    /// * `Success(identity)` — store the identity on the session so
    ///   downstream plugins (masking, residency) can gate on roles /
    ///   tenant_id / claims. PostgreSQL backend auth still runs
    ///   normally afterwards (the plugin does not replace PG auth in
    ///   this iteration; that's a follow-up).
    /// * `Denied(reason)` — surfaces as `ProxyError::Auth`, which the
    ///   caller already handles by writing an ErrorResponse to the
    ///   client and closing the connection.
    ///
    /// The `AuthRequest` populated here carries username, database,
    /// and client IP from the PostgreSQL startup parameters. Password
    /// is deliberately `None` — PG protocol sends the password in
    /// response to the backend's challenge, not at startup, so
    /// password-aware plugin auth is a separate future task.
    async fn apply_authenticate_hook(
        _params: &HashMap<String, String>,
        _session: &Arc<ClientSession>,
        _state: &Arc<ServerState>,
    ) -> Result<()> {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match _state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return Ok(()),
            };

            let request = PluginAuthRequest {
                headers: HashMap::new(),
                username: _params.get("user").cloned(),
                password: None,
                client_ip: _session.client_addr.ip().to_string(),
                database: _params.get("database").cloned(),
            };

            match pm.execute_authenticate(&request) {
                AuthResult::Defer => Ok(()),
                AuthResult::Success(identity) => {
                    tracing::debug!(
                        user = %identity.username,
                        roles = ?identity.roles,
                        "plugin authenticated user"
                    );
                    *_session.plugin_identity.write().await = Some(identity);
                    Ok(())
                }
                AuthResult::Denied(reason) => {
                    tracing::info!(
                        reason = %reason,
                        client = %_session.client_addr,
                        user = ?_params.get("user"),
                        "plugin denied authentication"
                    );
                    Err(ProxyError::Auth(format!(
                        "authentication denied by plugin: {}",
                        reason
                    )))
                }
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            Ok(())
        }
    }

    /// Run the Route plugin hook on a message. Only simple-query messages
    /// are inspected; other message types always return `None`.
    fn apply_route_hook(
        msg: &Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) -> RouteOverride {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return RouteOverride::None,
            };
            if msg.msg_type != MessageType::Query {
                return RouteOverride::None;
            }
            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::Route) {
                return RouteOverride::None;
            }
            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return RouteOverride::None,
            };
            let ctx = Self::build_query_context(&query_msg.query, session);
            match pm.execute_route(&ctx) {
                RouteResult::Default => RouteOverride::None,
                RouteResult::Primary => RouteOverride::Primary,
                RouteResult::Standby => RouteOverride::Standby,
                RouteResult::Node(name) => RouteOverride::Node(name),
                RouteResult::Block(reason) => RouteOverride::Block(reason),
                RouteResult::Branch(name) => {
                    tracing::warn!(
                        branch = %name,
                        "Route hook returned Branch but branch routing is not yet wired — using default"
                    );
                    RouteOverride::None
                }
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (msg, state, session);
            RouteOverride::None
        }
    }

    /// Map parsed SQL-comment hints to a `RouteOverride`. Precedence:
    /// `node=` > `route=` > `consistency=strong`. Read-tier route targets
    /// (standby/sync/semisync/async/local) all map to the read path; `any`
    /// and `vector` impose no constraint. `lag=` / `consistency=bounded`
    /// freshness enforcement arrives with the lag-routing feature.
    #[cfg(feature = "routing-hints")]
    fn hint_to_override(hints: &crate::routing::ParsedHints) -> RouteOverride {
        use crate::routing::{ConsistencyLevel, RouteTarget};
        if let Some(node) = &hints.node {
            return RouteOverride::Node(node.clone());
        }
        if let Some(route) = hints.route {
            return match route {
                RouteTarget::Primary => RouteOverride::Primary,
                RouteTarget::Standby
                | RouteTarget::Sync
                | RouteTarget::SemiSync
                | RouteTarget::Async
                | RouteTarget::Local => RouteOverride::Standby,
                RouteTarget::Any | RouteTarget::Vector => RouteOverride::None,
            };
        }
        if hints.consistency == Some(ConsistencyLevel::Strong) {
            return RouteOverride::Primary;
        }
        RouteOverride::None
    }

    /// Resolve the effective routing for a simple `Query` when the
    /// routing-hints feature is active. Returns `(override, is_write,
    /// forward_msg)`: the write flag is recomputed on the hint-stripped SQL so
    /// a leading hint comment never masks the verb, and `forward_msg` is a
    /// rebuilt `Query` (hint removed) when stripping is on. An explicit
    /// positional hint wins over a plugin route override; a plugin `Block` is
    /// handled by the caller before this runs.
    #[cfg(feature = "routing-hints")]
    fn resolve_simple_route(
        msg: &Message,
        plugin_override: RouteOverride,
        default_is_write: bool,
        state: &Arc<ServerState>,
    ) -> (RouteOverride, bool, Option<Message>) {
        let parser = match state.hint_parser.as_ref() {
            Some(p) => p,
            None => return (plugin_override, default_is_write, None),
        };
        let sql = match crate::protocol::query_text(&msg.payload) {
            Some(s) => s,
            None => return (plugin_override, default_is_write, None),
        };
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return (plugin_override, default_is_write, None);
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        let effective = match Self::hint_to_override(&hints) {
            RouteOverride::None => plugin_override,
            hint_override => hint_override,
        };
        let forward = if parser.strip_hints {
            Some(crate::protocol::QueryMessage { query: stripped }.encode())
        } else {
            None
        };
        (effective, is_write, forward)
    }

    /// Resolve hint-driven routing for an extended-protocol batch from the
    /// first Parse's SQL. `Some((is_write, forced_node))` when hints are
    /// present (write flag computed on the stripped SQL), else `None` so the
    /// caller uses verb-based defaults. The hint comment is left in the
    /// forwarded `Parse` (a no-op SQL comment); rewriting the batch buffer is
    /// unnecessary for correctness.
    #[cfg(feature = "routing-hints")]
    fn extended_hint_route(state: &Arc<ServerState>, sql: &str) -> Option<(bool, Option<String>)> {
        let parser = state.hint_parser.as_ref()?;
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return None;
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        match Self::hint_to_override(&hints) {
            RouteOverride::Primary => Some((true, None)),
            RouteOverride::Standby => Some((false, None)),
            RouteOverride::Node(n) => Some((is_write, Some(n))),
            _ => Some((is_write, None)),
        }
    }

    /// Fire post-query hooks after a message has been forwarded (or failed
    /// to forward). Best-effort; errors from individual plugins are logged
    /// by the plugin manager and never surface here.
    #[cfg(feature = "wasm-plugins")]
    fn fire_post_query_hook(
        msg: &Message,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        result: &Result<(Option<String>, u64)>,
        elapsed: Duration,
    ) {
        let pm = match state.plugin_manager.as_ref() {
            Some(pm) => pm,
            None => return,
        };
        if msg.msg_type != MessageType::Query {
            return;
        }
        // Zero plugins registered for this hook — skip the payload
        // clone, SQL parse, and context construction entirely.
        if !pm.has_hook(HookType::PostQuery) {
            return;
        }
        let query_msg = match QueryMessage::parse(msg.payload.clone()) {
            Ok(q) => q,
            Err(_) => return,
        };
        let ctx = Self::build_query_context(&query_msg.query, session);
        let outcome = match result {
            Ok((node, bytes)) => PostQueryOutcome {
                success: true,
                target_node: node.clone(),
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: *bytes,
                error: None,
            },
            Err(e) => PostQueryOutcome {
                success: false,
                target_node: None,
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: 0,
                error: Some(e.to_string()),
            },
        };
        pm.execute_post_query(&ctx, &outcome);
    }

    /// Select a backend node for the request
    /// Select a backend node for initial connection
    /// Prefers primary but falls back to standbys for read connections
    async fn select_node(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // If in a transaction, stick to the current node
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(node) = session.current_node.read().await.clone() {
                return Ok(node);
            }
        }

        // Get healthy nodes
        let health = state.health.load_full();
        let healthy_nodes: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| n.enabled && health.get(&n.address()).map(|h| h.healthy).unwrap_or(false))
            .collect();

        if healthy_nodes.is_empty() {
            return Err(ProxyError::NoHealthyNodes);
        }

        // Try to find healthy primary first
        if let Some(primary) = healthy_nodes.iter().find(|n| n.role == NodeRole::Primary) {
            let node_addr = primary.address();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to standby if primary is unavailable
        // (Initial connection will work, writes will use write timeout to wait for primary)
        if let Some(standby) = healthy_nodes.iter().find(|n| n.role == NodeRole::Standby) {
            tracing::warn!("Primary unavailable, connecting to standby for initial session");
            let node_addr = standby.address();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // No nodes available
        Err(ProxyError::NoHealthyNodes)
    }

    /// Spawn health checker background task
    fn spawn_health_checker(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Clamp to a 1s floor: `tokio::time::interval` panics on a zero
            // period, which would silently kill this task (it would then never
            // probe, so the proxy keeps routing to dead backends). `validate()`
            // already rejects 0 in a file config; this defends a
            // programmatically-built or reloaded config too.
            let interval_secs = state.live_config.load().health.check_interval_secs.max(1);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Read the live config each tick so a SIGHUP that
                        // adds/removes nodes is checked on the next sweep.
                        let config = state.live_config.load_full();
                        Self::check_all_nodes(&state, &config).await;
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        })
    }

    /// Check health of all nodes.
    ///
    /// Probes run concurrently (one slow/unreachable node no longer delays
    /// detection on the others — lowers the failover-detection latency
    /// floor), then a single new health snapshot is published via ArcSwap so
    /// readers on the query path never block.
    async fn check_all_nodes(state: &Arc<ServerState>, config: &ProxyConfig) {
        // Probe every node in parallel (owned address + timeout so each
        // probe is 'static and runs on its own task).
        let timeout = Duration::from_secs(config.health.check_timeout_secs);
        let mut set = tokio::task::JoinSet::new();
        for node in &config.nodes {
            let addr = node.address();
            set.spawn(async move {
                let r = Self::check_node_addr(&addr, timeout).await;
                (addr, r)
            });
        }
        let mut results = Vec::with_capacity(config.nodes.len());
        while let Some(joined) = set.join_next().await {
            if let Ok(pair) = joined {
                results.push(pair);
            }
        }

        // Clone-and-modify the current snapshot, then atomically swap it in.
        // Hold the write lock so a concurrent in-band demotion landing in this
        // load→store window (or a SIGHUP reconcile) cannot clobber, or be
        // clobbered by, this full-map rebuild. All node probing above already
        // completed; no await is held under the guard.
        let _writers = state.health_write.lock();
        let mut next = (*state.health.load_full()).clone();
        for (addr, result) in results {
            if let Some(node_health) = next.get_mut(&addr) {
                match result {
                    Ok(latency) => {
                        node_health.healthy = true;
                        node_health.failure_count = 0;
                        node_health.latency_ms = latency;
                        node_health.last_error = None;
                    }
                    Err(e) => {
                        node_health.failure_count += 1;
                        node_health.last_error = Some(e.to_string());
                        if node_health.failure_count >= config.health.failure_threshold {
                            node_health.healthy = false;
                            tracing::warn!(
                                "Node {} marked unhealthy after {} failures",
                                addr,
                                node_health.failure_count
                            );
                        }
                    }
                }
                node_health.last_check = chrono::Utc::now();
            }
        }
        state.health.store(Arc::new(next));
    }

    /// Check health of a single node with a protocol-level liveness probe.
    ///
    /// A bare TCP connect is not enough: a wedged backend (postmaster stuck,
    /// out of backend slots, mid-crash-recovery) still *accepts* the socket but
    /// never processes the wire protocol, so a connect-only probe reports it
    /// healthy. Instead we connect, send a PostgreSQL `SSLRequest`, and require
    /// the postmaster to answer (`S`/`N`) within the timeout. The SSLRequest is
    /// auth-free and not logged, so it costs the backend essentially nothing,
    /// yet it proves the server is actually servicing the protocol. Returns the
    /// round-trip latency in milliseconds.
    async fn check_node_addr(addr: &str, timeout: Duration) -> Result<f64> {
        // length(8) + SSLRequest code 80877103 (0x04D2162F).
        const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];
        let start = std::time::Instant::now();
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("Timeout connecting to {}", addr)))?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("Failed to connect to {}: {}", addr, e))
            })?;

        let probe = async {
            stream.write_all(&SSL_REQUEST).await?;
            let mut resp = [0u8; 1];
            stream.read_exact(&mut resp).await?;
            Ok::<u8, std::io::Error>(resp[0])
        };
        // Budget whatever time is left after the connect for the handshake.
        let remaining = timeout
            .saturating_sub(start.elapsed())
            .max(Duration::from_millis(1));
        let byte = tokio::time::timeout(remaining, probe)
            .await
            .map_err(|_| {
                ProxyError::HealthCheck(format!("{} did not answer protocol probe in time", addr))
            })?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("{} protocol probe error: {}", addr, e))
            })?;
        // 'S' (TLS available) or 'N' (not) both prove the postmaster is live and
        // talking the protocol; anything else means a non-PostgreSQL listener.
        if byte != b'S' && byte != b'N' {
            return Err(ProxyError::HealthCheck(format!(
                "{} sent unexpected probe reply {:#x}",
                addr, byte
            )));
        }
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        Ok(latency)
    }

    /// Spawn pool manager background task
    fn spawn_pool_manager(&self) -> tokio::task::JoinHandle<()> {
        // Only referenced by the pool-modes eviction/cleanup arms below.
        #[cfg(feature = "pool-modes")]
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Self::POOL_REAP_INTERVAL);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Evict idle connections from pool-modes manager
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref pool_manager) = state.pool_manager {
                            pool_manager.evict_idle().await;
                            tracing::trace!("Pool-modes idle eviction completed");
                        }
                        // Reap data-path idle backend connections older than the
                        // configured idle timeout, so a connection the backend
                        // would close on its own idle timeout is never handed out
                        // stale and idle FDs are returned to the OS.
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref backend_pool) = state.backend_pool {
                            let ttl = std::time::Duration::from_secs(
                                state.live_config.load().pool_mode.idle_timeout_secs,
                            );
                            // idle_timeout_secs = 0 means "no idle TTL" (the
                            // PgBouncer convention). Skip reaping entirely rather
                            // than reaping every parked connection each cycle
                            // (elapsed() < ZERO is always false → retain drops
                            // all), which would defeat connection reuse.
                            let n = if ttl.is_zero() {
                                0
                            } else {
                                backend_pool.reap_idle(ttl)
                            };
                            if n > 0 {
                                tracing::debug!(
                                    target: "helios::pool",
                                    reaped = n,
                                    idle_remaining = backend_pool.idle_count(),
                                    "reaped idle backend connections (TTL)"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        // Cleanup on shutdown
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref pool_manager) = state.pool_manager {
                            pool_manager.close_all().await;
                            tracing::info!("Pool-modes manager closed all connections");
                        }
                        break;
                    }
                }
            }
        })
    }

    /// Shutdown the server
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get pool mode statistics (if pool-modes feature enabled)
    #[cfg(feature = "pool-modes")]
    pub async fn pool_mode_stats(&self) -> Option<PoolModeStatsSnapshot> {
        if let Some(ref pool_manager) = self.state.pool_manager {
            let stats = pool_manager.get_stats().await;
            let metrics = pool_manager.metrics().snapshot();
            let default_mode = pool_manager.default_mode();

            // Calculate average lease duration across all modes
            let avg_lease_duration_ms = metrics
                .mode_stats
                .get(&default_mode)
                .map(|s| s.avg_lease_duration_ms as u64)
                .unwrap_or(0);

            Some(PoolModeStatsSnapshot {
                mode: format!("{:?}", default_mode),
                total_connections: stats.total_connections,
                active_leases: stats.active_connections,
                idle_connections: stats.idle_connections,
                node_count: stats.node_count,
                acquires: metrics.acquires,
                releases: metrics.releases,
                acquire_failures: metrics.acquire_failures,
                acquire_timeouts: metrics.acquire_timeouts,
                transactions_completed: metrics.transactions_completed,
                statements_executed: metrics.statements_executed,
                avg_lease_duration_ms,
            })
        } else {
            None
        }
    }

    /// Add a node to the pool manager (if pool-modes feature enabled)
    #[cfg(feature = "pool-modes")]
    pub async fn add_node_to_pool(&self, node: &NodeConfig) {
        if let Some(ref pool_manager) = self.state.pool_manager {
            let endpoint = NodeEndpoint::new(&node.host, node.port)
                .with_role(match node.role {
                    NodeRole::Primary => crate::NodeRole::Primary,
                    NodeRole::Standby => crate::NodeRole::Standby,
                    NodeRole::ReadReplica => crate::NodeRole::ReadReplica,
                })
                .with_weight(node.weight);
            pool_manager.add_node(&endpoint).await;
            tracing::info!("Added node {} to pool manager", node.address());
        }
    }

    /// Get server metrics
    pub fn metrics(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            connections_accepted: self
                .state
                .metrics
                .connections_accepted
                .load(Ordering::Relaxed),
            connections_closed: self
                .state
                .metrics
                .connections_closed
                .load(Ordering::Relaxed),
            queries_processed: self.state.metrics.queries_processed.load(Ordering::Relaxed),
            bytes_received: self.state.metrics.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.state.metrics.bytes_sent.load(Ordering::Relaxed),
            failovers: self.state.metrics.failovers.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot for external consumption
#[derive(Debug, Clone)]
pub struct ServerMetricsSnapshot {
    pub connections_accepted: u64,
    pub connections_closed: u64,
    pub queries_processed: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub failovers: u64,
}

/// Pool mode statistics snapshot (when pool-modes feature is enabled)
#[cfg(feature = "pool-modes")]
#[derive(Debug, Clone)]
pub struct PoolModeStatsSnapshot {
    /// Current pooling mode
    pub mode: String,
    /// Total connections across all pools
    pub total_connections: usize,
    /// Active (leased) connections
    pub active_leases: usize,
    /// Idle connections
    pub idle_connections: usize,
    /// Number of nodes in the pool
    pub node_count: usize,
    /// Total connection acquires
    pub acquires: u64,
    /// Total connection releases
    pub releases: u64,
    /// Failed acquire attempts
    pub acquire_failures: u64,
    /// Acquire timeouts
    pub acquire_timeouts: u64,
    /// Completed transactions (Transaction mode)
    pub transactions_completed: u64,
    /// Total statements executed
    pub statements_executed: u64,
    /// Average lease duration in milliseconds
    pub avg_lease_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HealthConfig, LoadBalancerConfig, PoolConfig};
    #[cfg(not(feature = "wasm-plugins"))]
    use crate::protocol::QueryMessage;

    fn test_config() -> ProxyConfig {
        let mut config = ProxyConfig::default();
        config.listen_address = "127.0.0.1:0".to_string();
        config.add_node("127.0.0.1:5432", "primary").unwrap();
        config
    }

    #[test]
    fn test_server_creation() {
        let config = test_config();
        let server = ProxyServer::new(config);
        assert!(server.is_ok());
    }

    #[test]
    fn is_backend_fault_excludes_client_and_slow_query_errors() {
        // Real backend faults — these must demote the node in-band.
        assert!(ProxyServer::is_backend_fault(
            "Backend read error: connection reset"
        ));
        assert!(ProxyServer::is_backend_fault(
            "Backend write error: broken pipe"
        ));
        assert!(ProxyServer::is_backend_fault("Backend write timeout"));
        assert!(ProxyServer::is_backend_fault(
            "Failed to connect to 127.0.0.1:5432: Connection refused"
        ));
        // Not backend faults — a client-side problem, or a merely slow but
        // healthy query, must NEVER take a backend out of rotation cluster-wide.
        assert!(!ProxyServer::is_backend_fault("Backend read timeout"));
        assert!(!ProxyServer::is_backend_fault("Client write timeout"));
        assert!(!ProxyServer::is_backend_fault(
            "Client write error: broken pipe"
        ));
        // A backend READ timeout is exempt, but a backend read ERROR is a fault.
        assert!(!ProxyServer::is_backend_fault("Backend read timeout"));
        assert!(ProxyServer::is_backend_fault(
            "Backend read error: timed out"
        ));
    }

    #[test]
    fn test_hba_addr_matches() {
        use std::net::IpAddr;
        let v4 = |s: &str| s.parse::<IpAddr>().unwrap();
        // "all" matches everything
        assert!(ProxyServer::hba_addr_matches("all", v4("203.0.113.7")));
        // CIDR membership
        assert!(ProxyServer::hba_addr_matches("10.0.0.0/8", v4("10.1.2.3")));
        assert!(!ProxyServer::hba_addr_matches("10.0.0.0/8", v4("11.1.2.3")));
        assert!(ProxyServer::hba_addr_matches(
            "127.0.0.1/32",
            v4("127.0.0.1")
        ));
        assert!(!ProxyServer::hba_addr_matches(
            "127.0.0.1/32",
            v4("127.0.0.2")
        ));
        // bare IP exact match
        assert!(ProxyServer::hba_addr_matches(
            "192.168.1.1",
            v4("192.168.1.1")
        ));
        assert!(!ProxyServer::hba_addr_matches(
            "192.168.1.1",
            v4("192.168.1.2")
        ));
        // IPv6 CIDR + /0 catch-all
        assert!(ProxyServer::hba_addr_matches("::1/128", v4("::1")));
        assert!(ProxyServer::hba_addr_matches("0.0.0.0/0", v4("8.8.8.8")));
    }

    #[test]
    fn test_hba_admits() {
        use crate::config::{HbaAction, HbaRule};
        use std::net::IpAddr;
        let ip: IpAddr = "10.0.0.5".parse().unwrap();
        // No rules -> admit all
        assert!(ProxyServer::hba_admits(&[], ip, "bench", "benchdb"));
        // Reject a specific user, allow others (default admit)
        let rules = vec![HbaRule {
            action: HbaAction::Reject,
            user: "bench".into(),
            database: "all".into(),
            address: "all".into(),
        }];
        assert!(!ProxyServer::hba_admits(&rules, ip, "bench", "benchdb"));
        assert!(ProxyServer::hba_admits(&rules, ip, "alice", "benchdb"));
        // First match wins: allow bench from 10/8, reject everything else
        let rules = vec![
            HbaRule {
                action: HbaAction::Allow,
                user: "bench".into(),
                database: "all".into(),
                address: "10.0.0.0/8".into(),
            },
            HbaRule {
                action: HbaAction::Reject,
                user: "all".into(),
                database: "all".into(),
                address: "all".into(),
            },
        ];
        assert!(ProxyServer::hba_admits(&rules, ip, "bench", "benchdb"));
        assert!(!ProxyServer::hba_admits(
            &rules,
            "192.168.0.1".parse().unwrap(),
            "bench",
            "benchdb"
        ));
        assert!(!ProxyServer::hba_admits(&rules, ip, "alice", "benchdb"));
    }

    #[test]
    fn test_initial_metrics() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let metrics = server.metrics();
        assert_eq!(metrics.connections_accepted, 0);
        assert_eq!(metrics.queries_processed, 0);
    }

    #[tokio::test]
    async fn test_session_creation() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();

        let sessions = server.state.sessions.read().await;
        assert!(sessions.is_empty());
    }

    #[tokio::test]
    async fn test_node_health_initialization() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();

        let health = server.state.health.load_full();
        assert!(!health.is_empty());

        for node_health in health.values() {
            assert!(node_health.healthy);
            assert_eq!(node_health.failure_count, 0);
        }
    }

    /// Build a minimal `ClientSession` for plugin-hook unit tests.
    fn make_test_session() -> Arc<ClientSession> {
        Arc::new(ClientSession {
            id: Uuid::new_v4(),
            client_addr: "127.0.0.1:0".parse().unwrap(),
            current_node: RwLock::new(None),
            in_transaction: std::sync::atomic::AtomicBool::new(false),
            copy_in_progress: std::sync::atomic::AtomicBool::new(false),
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: crate::config::TrMode::default(),
            #[cfg(feature = "lag-routing")]
            last_write_at: RwLock::new(None),
            #[cfg(feature = "pool-modes")]
            pool_client_id: crate::pool::lease::ClientId::default(),
            #[cfg(feature = "wasm-plugins")]
            plugin_identity: RwLock::new(None),
        })
    }

    /// With no plugin manager attached, `apply_route_hook` must be a
    /// zero-cost `None` return so the default SQL-verb routing applies.
    /// Verifies the feature-gated early-return path.
    #[tokio::test]
    async fn test_apply_route_hook_no_plugin_manager_returns_none() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let msg = QueryMessage {
            query: "SELECT * FROM users".to_string(),
        }
        .encode();

        let decision = ProxyServer::apply_route_hook(&msg, &server.state, &session);
        assert!(matches!(decision, RouteOverride::None));
    }

    /// Same invariant for the pre-query hook: without a plugin manager,
    /// `apply_pre_query_hook` must return the message unchanged with
    /// `PreQueryAction::Forward`.
    #[tokio::test]
    async fn test_apply_pre_query_hook_no_plugin_manager_forwards() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let original = QueryMessage {
            query: "SELECT 1".to_string(),
        }
        .encode();
        let original_bytes = original.encode().to_vec();

        let (msg_out, action) =
            ProxyServer::apply_pre_query_hook(original, &server.state, &session);

        assert!(matches!(action, PreQueryAction::Forward));
        // The message must survive the hook byte-for-byte when no plugins run.
        assert_eq!(msg_out.encode().to_vec(), original_bytes);
    }

    /// Non-Query message types (e.g., extended-protocol Parse/Execute) must
    /// bypass the Route hook entirely regardless of plugin state, because
    /// we haven't wired SQL extraction for those variants yet.
    #[tokio::test]
    async fn test_apply_route_hook_skips_non_query_messages() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let sync_msg = Message::empty(MessageType::Sync);
        let decision = ProxyServer::apply_route_hook(&sync_msg, &server.state, &session);
        assert!(matches!(decision, RouteOverride::None));
    }

    /// By default, `[plugins].enabled = false`, so `init_plugin_manager`
    /// short-circuits without touching the filesystem or wasmtime and
    /// returns `None`. The proxy starts normally whether or not a plugin
    /// directory exists on the host.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_init_plugin_manager_disabled_by_default_returns_none() {
        let config = test_config();
        assert!(!config.plugins.enabled);
        let pm = ProxyServer::init_plugin_manager(&config.plugins);
        assert!(pm.is_none());
    }

    /// Plugins enabled but pointing at a directory that doesn't exist
    /// must still initialise the manager (so new plugins can be hot-
    /// loaded later) and log a warning — it must NOT fail startup.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_init_plugin_manager_missing_dir_logs_warning() {
        let mut config = test_config();
        config.plugins.enabled = true;
        config.plugins.plugin_dir = "/definitely/not/a/real/path".to_string();

        // Manager is created; no panic; Some(pm) returned even with empty dir.
        let pm = ProxyServer::init_plugin_manager(&config.plugins);
        assert!(pm.is_some());
    }

    /// With no plugin manager attached, `apply_authenticate_hook` is a
    /// zero-cost `Ok(())` that leaves session identity unset — the
    /// default PG auth flow applies.
    #[tokio::test]
    async fn test_apply_authenticate_hook_no_plugin_manager_defers() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let mut params = HashMap::new();
        params.insert("user".to_string(), "alice".to_string());
        params.insert("database".to_string(), "app".to_string());

        let result = ProxyServer::apply_authenticate_hook(&params, &session, &server.state).await;
        assert!(result.is_ok());

        // No plugin → no identity stored.
        #[cfg(feature = "wasm-plugins")]
        {
            let ident = session.plugin_identity.read().await;
            assert!(ident.is_none());
        }
    }

    /// Cached-response synthesis round-trip: a well-formed plugin
    /// payload must produce concatenated wire frames in the order
    /// `T D D C Z`. We inspect the raw tag bytes directly because
    /// `MessageType::from_tag` conflates server→client DataRow (`'D'`)
    /// with client→server Describe (same byte) — a known quirk of the
    /// shared `MessageType` enum that the real proxy side-steps by
    /// knowing the direction at the call site.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_roundtrip() {
        let payload = br#"{
            "columns": [
                {"name": "id",    "oid": 23},
                {"name": "email", "oid": 25}
            ],
            "rows": [
                ["1", "alice@example.com"],
                ["2", null]
            ]
        }"#;
        let reply = ProxyServer::synthesise_cached_response(payload).expect("synthesis");

        // Walk the concatenation frame-by-frame via length prefixes.
        // Each PG message: tag(1) + length(4, big-endian, includes self) + payload.
        let mut tags = Vec::new();
        let mut i = 0;
        while i < reply.len() {
            let tag = reply[i];
            let len = u32::from_be_bytes([reply[i + 1], reply[i + 2], reply[i + 3], reply[i + 4]])
                as usize;
            tags.push(tag);
            i += 1 + len;
        }
        assert_eq!(i, reply.len(), "no trailing bytes");
        assert_eq!(tags, vec![b'T', b'D', b'D', b'C', b'Z'], "wire frame order");

        // Spot-check the final ReadyForQuery payload is 'I' (idle).
        assert_eq!(*reply.last().unwrap(), b'I');
    }

    /// Row width mismatch between columns and row data is rejected so
    /// the plugin author can't produce ambiguous wire frames.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_rejects_row_width_mismatch() {
        let payload = br#"{
            "columns": [{"name": "id", "oid": 23}, {"name": "name", "oid": 25}],
            "rows": [["1", "alice", "extra"]]
        }"#;
        let result = ProxyServer::synthesise_cached_response(payload);
        assert!(matches!(result, Err(ProxyError::Protocol(_))));
    }

    /// Empty payload (no columns) is rejected — a RowDescription with
    /// zero columns is technically valid PG but useless and likely a
    /// plugin bug.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_rejects_empty_columns() {
        let payload = br#"{ "columns": [], "rows": [] }"#;
        let result = ProxyServer::synthesise_cached_response(payload);
        assert!(matches!(result, Err(ProxyError::Protocol(_))));
    }

    /// Malformed JSON must return a Protocol error, not panic. The
    /// caller treats this as "fall back to backend."
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_rejects_bad_json() {
        let payload = b"not json at all";
        let result = ProxyServer::synthesise_cached_response(payload);
        assert!(matches!(result, Err(ProxyError::Protocol(_))));
    }

    /// Denied by plugin surfaces as `ProxyError::Auth` so the existing
    /// error-response path in `handle_client` writes an ErrorResponse
    /// and closes the connection. Here we prove the error variant
    /// when the plugin manager is present but denies. We build a
    /// PluginManager with no plugins loaded — so it defers — and
    /// verify the Ok path. (Denial path requires an actual
    /// auth-plugin `.wasm`; covered by the plugin unit tests in
    /// `plugins::tests`.)
    #[cfg(feature = "wasm-plugins")]
    #[tokio::test]
    async fn test_apply_authenticate_hook_with_manager_no_plugins_defers() {
        use crate::plugins::{PluginManager, PluginRuntimeConfig};

        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        // Synthesise a state with a real PluginManager but zero
        // registered plugins — every hook must defer.
        let pm = Arc::new(PluginManager::new(PluginRuntimeConfig::default()).unwrap());
        let augmented_state = Arc::new(ServerState {
            sessions: RwLock::new(HashMap::new()),
            health: ArcSwap::from_pointee(HashMap::new()),
            health_write: parking_lot::Mutex::new(()),
            live_config: ArcSwap::from_pointee(ProxyConfig::default()),
            metrics: ServerMetrics::default(),
            cancel_map: Arc::new(DashMap::new()),
            cancel_order: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
            tls_acceptor: None,
            auth_file: None,
            mirror: None,
            cutover: Arc::new(ArcSwap::from_pointee(None)),
            lb_state: LoadBalancerState {
                rr_counter: AtomicU64::new(0),
            },
            #[cfg(feature = "routing-hints")]
            hint_parser: None,
            #[cfg(feature = "rate-limiting")]
            rate_limiter: None,
            #[cfg(feature = "circuit-breaker")]
            circuit_breaker: None,
            #[cfg(feature = "query-analytics")]
            analytics: None,
            #[cfg(feature = "query-cache")]
            query_cache: None,
            #[cfg(feature = "query-rewriting")]
            rewriter: None,
            #[cfg(feature = "multi-tenancy")]
            tenant_manager: None,
            #[cfg(feature = "schema-routing")]
            schema_analyzer: None,
            #[cfg(feature = "pool-modes")]
            pool_manager: None,
            #[cfg(feature = "pool-modes")]
            backend_pool: None,
            plugin_manager: Some(pm),
            #[cfg(feature = "ha-tr")]
            transaction_journal: Arc::new(crate::transaction_journal::TransactionJournal::new()),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(crate::anomaly::AnomalyDetector::new(
                crate::anomaly::AnomalyConfig::default(),
            )),
            #[cfg(feature = "edge-proxy")]
            edge_cache: Arc::new(crate::edge::EdgeCache::new(10_000)),
            #[cfg(feature = "edge-proxy")]
            edge_registry: Arc::new(crate::edge::EdgeRegistry::new(
                32,
                std::time::Duration::from_secs(120),
            )),
        });

        let mut params = HashMap::new();
        params.insert("user".to_string(), "alice".to_string());

        let result =
            ProxyServer::apply_authenticate_hook(&params, &session, &augmented_state).await;
        assert!(result.is_ok());
        let ident = session.plugin_identity.read().await;
        assert!(ident.is_none());
        // Unused bindings for the sync-state build path.
        let _ = server;
    }

    // ---- Batch F.4: prepared-statement tracking across backend switches ----

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    #[test]
    fn parse_stmt_name_extracts_named_and_unnamed() {
        // Parse payload = stmt-name cstring + query cstring + int16 nparams.
        let mut named = cstr("ps1");
        named.extend_from_slice(&cstr("SELECT 1"));
        named.extend_from_slice(&[0, 0]);
        assert_eq!(ProxyServer::parse_stmt_name(&named), "ps1");

        let mut unnamed = cstr("");
        unnamed.extend_from_slice(&cstr("SELECT 1"));
        unnamed.extend_from_slice(&[0, 0]);
        assert_eq!(ProxyServer::parse_stmt_name(&unnamed), "");
    }

    #[test]
    fn bind_stmt_ref_reads_second_cstring() {
        // Bind payload = portal cstring + statement cstring + ...
        let mut named = cstr("portal_a");
        named.extend_from_slice(&cstr("ps1"));
        named.extend_from_slice(&[0, 0]); // 0 param-format codes, 0 params
        assert_eq!(ProxyServer::bind_stmt_ref(&named), Some("ps1"));

        // Unnamed statement (empty second cstring) is not tracked.
        let mut unnamed = cstr("");
        unnamed.extend_from_slice(&cstr(""));
        assert_eq!(ProxyServer::bind_stmt_ref(&unnamed), None);
    }

    #[test]
    fn stmt_kind_name_only_matches_statement_kind() {
        // Describe/Close 'S' (statement) carries a trackable name.
        let mut stmt = vec![b'S'];
        stmt.extend_from_slice(&cstr("ps1"));
        assert_eq!(ProxyServer::stmt_kind_name(&stmt), Some("ps1"));

        // 'P' (portal) is not a statement reference.
        let mut portal = vec![b'P'];
        portal.extend_from_slice(&cstr("portal_a"));
        assert_eq!(ProxyServer::stmt_kind_name(&portal), None);

        // Statement-kind but unnamed -> nothing to track.
        let mut empty = vec![b'S'];
        empty.extend_from_slice(&cstr(""));
        assert_eq!(ProxyServer::stmt_kind_name(&empty), None);
    }

    #[tokio::test]
    async fn read_one_frame_type_consumes_full_frame() {
        // ParseComplete '1' with empty body, followed by a second frame to
        // prove only the first frame is consumed.
        let (mut a, mut b) = tokio::io::duplex(64);
        // frame 1: '1' + len(4) + no body; frame 2: 'Z' + len(5) + 'I'.
        let bytes = [b'1', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'];
        b.write_all(&bytes).await.unwrap();
        let t = ProxyServer::read_one_frame_type(&mut a).await.unwrap();
        assert_eq!(t, b'1');
        // The next frame's type byte is still readable -> we stopped cleanly.
        let t2 = ProxyServer::read_one_frame_type(&mut a).await.unwrap();
        assert_eq!(t2, b'Z');
    }

    #[tokio::test]
    async fn reprepare_statement_accepts_parse_complete_and_rejects_error() {
        // Backend answers ParseComplete -> Ok.
        let (mut client, mut backend) = tokio::io::duplex(64);
        backend.write_all(&[b'1', 0, 0, 0, 4]).await.unwrap();
        let parse = {
            let mut p = vec![b'P', 0, 0, 0, 0];
            p.extend_from_slice(&cstr("ps1"));
            p.extend_from_slice(&cstr("SELECT 1"));
            p.extend_from_slice(&[0, 0]);
            p
        };
        assert!(ProxyServer::reprepare_statement(&mut client, &parse)
            .await
            .is_ok());

        // Backend answers ErrorResponse -> Err.
        let (mut client2, mut backend2) = tokio::io::duplex(64);
        backend2.write_all(&[b'E', 0, 0, 0, 4]).await.unwrap();
        assert!(ProxyServer::reprepare_statement(&mut client2, &parse)
            .await
            .is_err());
    }

    // ---- routing-hints: SQL-comment hint → RouteOverride mapping ----

    #[cfg(feature = "routing-hints")]
    mod routing_hints {
        use super::*;
        use crate::routing::HintParser;

        fn over(sql: &str) -> RouteOverride {
            let hints = HintParser::new().parse(sql);
            ProxyServer::hint_to_override(&hints)
        }

        #[test]
        fn route_primary_maps_to_primary() {
            assert!(matches!(
                over("/*helios:route=primary*/ SELECT 1"),
                RouteOverride::Primary
            ));
        }

        #[test]
        fn read_tier_targets_map_to_standby() {
            for t in ["standby", "sync", "semisync", "async", "local"] {
                assert!(
                    matches!(
                        over(&format!("/*helios:route={t}*/ SELECT 1")),
                        RouteOverride::Standby
                    ),
                    "route={t} should map to Standby"
                );
            }
        }

        #[test]
        fn any_and_vector_impose_no_constraint() {
            assert!(matches!(
                over("/*helios:route=any*/ SELECT 1"),
                RouteOverride::None
            ));
            assert!(matches!(
                over("/*helios:route=vector*/ SELECT 1"),
                RouteOverride::None
            ));
        }

        #[test]
        fn node_hint_maps_to_node_and_wins_over_route() {
            // node= beats route= (precedence).
            match over("/*helios:node=pg-standby,route=primary*/ SELECT 1") {
                RouteOverride::Node(n) => assert_eq!(n, "pg-standby"),
                other => panic!("expected Node, got {other:?}"),
            }
        }

        #[test]
        fn consistency_strong_forces_primary() {
            assert!(matches!(
                over("/*helios:consistency=strong*/ SELECT 1"),
                RouteOverride::Primary
            ));
        }

        #[test]
        fn no_hint_yields_none() {
            assert!(matches!(over("SELECT 1"), RouteOverride::None));
        }

        // The core correctness fix: a leading hint comment must NOT hide the
        // verb from write-detection. Raw classification misfires; classifying
        // on the stripped SQL is correct.
        #[test]
        fn write_verb_classified_after_strip() {
            let parser = HintParser::new();
            let raw = "/*helios:route=primary*/ INSERT INTO t VALUES (1)";
            // Raw (unstripped) wrongly looks like a read because it starts
            // with the comment.
            assert!(!ProxyServer::is_write_query(raw));
            // Stripped is correctly a write.
            assert!(ProxyServer::is_write_query(&parser.strip(raw)));
        }

        #[test]
        fn strip_removes_hint_comment() {
            let parser = HintParser::new();
            assert_eq!(
                parser.strip("/*helios:route=standby*/ SELECT 42"),
                "SELECT 42"
            );
        }
    }

    // ---- rate-limiting: the burst-then-deny contract the gate relies on ----

    #[cfg(feature = "rate-limiting")]
    mod rate_limiting {
        use crate::rate_limit::{LimiterKey, RateLimitConfig, RateLimitResult, RateLimiter};

        #[test]
        fn burst_allows_then_denies() {
            // Mirror the wiring's config conversion: tiny bucket, reject on
            // exceed (the engine default).
            let cfg = RateLimitConfig {
                enabled: true,
                default_qps: 1,
                default_burst: 2,
                ..Default::default()
            };
            let limiter = RateLimiter::new(cfg);
            let key = LimiterKey::User("u".to_string());

            // The first `burst` checks are admitted.
            assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));
            assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));

            // Rapid over-burst checks must produce at least one hard denial.
            let mut denied = false;
            for _ in 0..5 {
                if matches!(limiter.check(&key, 1), RateLimitResult::Denied(_)) {
                    denied = true;
                }
            }
            assert!(denied, "over-burst checks must yield a Denied verdict");
        }

        #[test]
        fn distinct_keys_have_independent_buckets() {
            let cfg = RateLimitConfig {
                enabled: true,
                default_qps: 1,
                default_burst: 1,
                ..Default::default()
            };
            let limiter = RateLimiter::new(cfg);
            // Each user gets its own bucket: both first checks are admitted.
            assert!(matches!(
                limiter.check(&LimiterKey::User("a".to_string()), 1),
                RateLimitResult::Allowed
            ));
            assert!(matches!(
                limiter.check(&LimiterKey::User("b".to_string()), 1),
                RateLimitResult::Allowed
            ));
        }
    }

    // ---- circuit-breaker: open-after-threshold contract the gate relies on ----

    #[cfg(feature = "circuit-breaker")]
    mod circuit_breaker {
        use crate::circuit_breaker::{
            CircuitBreakerConfig, CircuitBreakerManager, CircuitState, ManagerConfig,
        };
        use std::time::Duration;

        fn mgr(threshold: u32) -> CircuitBreakerManager {
            let cfg = CircuitBreakerConfig {
                failure_threshold: threshold,
                cooldown: Duration::from_secs(10),
                ..Default::default()
            };
            CircuitBreakerManager::new(ManagerConfig::new(cfg))
        }

        #[test]
        fn opens_after_threshold_failures() {
            let m = mgr(3);
            let b = m.get_breaker("n1");
            assert_eq!(b.get_state(), CircuitState::Closed);
            b.record_failure("boom");
            b.record_failure("boom");
            // Under threshold: still serving.
            assert_eq!(b.get_state(), CircuitState::Closed);
            // Threshold reached: tripped open.
            b.record_failure("boom");
            assert_eq!(b.get_state(), CircuitState::Open);
        }

        #[test]
        fn healthy_node_stays_closed() {
            let m = mgr(3);
            let b = m.get_breaker("n2");
            b.record_success();
            b.record_success();
            assert_eq!(b.get_state(), CircuitState::Closed);
        }
    }

    // ---- query-analytics: record + literal-collapsing normalizer ----

    #[cfg(feature = "query-analytics")]
    mod query_analytics {
        use crate::analytics::{AnalyticsConfig, OrderBy, QueryAnalytics, QueryExecution};
        use std::time::Duration;

        #[test]
        fn records_and_collapses_literals() {
            let a = QueryAnalytics::new(AnalyticsConfig::default());
            for n in [1, 2, 3] {
                a.record(QueryExecution::new(
                    format!("select {n}"),
                    Duration::from_millis(1),
                ));
            }
            let top = a.top_queries(OrderBy::Calls, 10);
            assert!(!top.is_empty(), "no fingerprints recorded");
            // The three literal variants collapse to one fingerprint (3 calls).
            assert!(
                top.iter().any(|s| s.calls >= 3),
                "literals did not collapse: {:?}",
                top.iter()
                    .map(|s| (s.normalized.clone(), s.calls))
                    .collect::<Vec<_>>()
            );
        }
    }

    // ---- lag-routing: read-your-writes window + lag-exclusion decisions ----

    #[cfg(feature = "lag-routing")]
    mod lag_routing {
        use super::ProxyServer;

        #[test]
        fn ryw_pins_recent_write() {
            // A write "now" falls inside a 1s window -> pin to primary.
            assert!(ProxyServer::ryw_pins_primary(
                Some(std::time::Instant::now()),
                1000
            ));
        }

        #[test]
        fn ryw_releases_old_write() {
            let old = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(10))
                .unwrap();
            assert!(!ProxyServer::ryw_pins_primary(Some(old), 1000));
        }

        #[test]
        fn ryw_no_write_or_disabled() {
            assert!(!ProxyServer::ryw_pins_primary(None, 1000));
            // window=0 disables read-your-writes entirely.
            assert!(!ProxyServer::ryw_pins_primary(
                Some(std::time::Instant::now()),
                0
            ));
        }

        #[test]
        fn lag_exclusion_thresholds() {
            // max=0 disables exclusion.
            assert!(!ProxyServer::lag_excludes_standby(Some(999_999), 0));
            // unknown lag never excludes.
            assert!(!ProxyServer::lag_excludes_standby(None, 1000));
            // within ceiling stays in rotation.
            assert!(!ProxyServer::lag_excludes_standby(Some(500), 1000));
            // beyond ceiling is dropped.
            assert!(ProxyServer::lag_excludes_standby(Some(2000), 1000));
        }
    }

    // ---- query-cache: which read SQL is safe to cache ----

    #[cfg(feature = "query-cache")]
    mod query_cache {
        use super::ProxyServer;

        #[test]
        fn plain_selects_are_cacheable() {
            assert!(ProxyServer::is_cacheable_read_sql("select v from t"));
            assert!(ProxyServer::is_cacheable_read_sql(
                "  SELECT a, b FROM users WHERE id = 5"
            ));
        }

        #[test]
        fn writes_and_non_selects_are_not_cacheable() {
            assert!(!ProxyServer::is_cacheable_read_sql(
                "insert into t values (1)"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql("update t set v = 1"));
            assert!(!ProxyServer::is_cacheable_read_sql("show search_path"));
        }

        #[test]
        fn locking_and_volatile_selects_are_not_cacheable() {
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * from t for update"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql("select now()"));
            assert!(!ProxyServer::is_cacheable_read_sql("select random()"));
            assert!(!ProxyServer::is_cacheable_read_sql("select nextval('s')"));
        }
    }

    // ---- query-rewriting: the rules-engine rewrite contract ----

    #[cfg(feature = "query-rewriting")]
    mod query_rewriting {
        use crate::rewriter::{
            QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation,
        };

        fn rw_with_table_replace() -> QueryRewriter {
            let rw = QueryRewriter::new(RewriterConfig {
                enabled: true,
                ..Default::default()
            });
            rw.add_rule(
                RewriteRule::build("t")
                    .pattern(QueryPattern::Table("a".to_string()))
                    .transform(Transformation::ReplaceTable {
                        from: "a".to_string(),
                        to: "b".to_string(),
                    })
                    .build(),
            );
            rw
        }

        #[test]
        fn matching_query_is_rewritten() {
            let res = rw_with_table_replace().rewrite("select * from a").unwrap();
            assert!(res.was_rewritten(), "rule did not fire");
            assert!(res.query().contains('b'), "rewritten: {}", res.query());
            assert!(
                !res.query().contains("from a"),
                "still references a: {}",
                res.query()
            );
        }

        #[test]
        fn unmatched_query_is_unchanged() {
            let res = rw_with_table_replace()
                .rewrite("select * from other")
                .unwrap();
            assert!(!res.was_rewritten());
            assert_eq!(res.query(), "select * from other");
        }
    }

    // ---- multi-tenancy: row-filter injection per tenant ----

    #[cfg(feature = "multi-tenancy")]
    mod multi_tenancy {
        use crate::multi_tenancy::{
            IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId,
            TenantManager, TenantManagerBuilder, TenantQueryTransformer,
        };

        fn manager() -> TenantManager {
            let transformer = TenantQueryTransformer::new().register_tables(&["t"], "tid");
            let tm = TenantManagerBuilder::new()
                .config(MultiTenancyConfig {
                    enabled: true,
                    identification: IdentificationMethod::Header {
                        header_name: "application_name".to_string(),
                    },
                    ..Default::default()
                })
                .query_transformer(transformer)
                .build();
            tm.register_tenant(TenantConfig::new(
                TenantId::new("acme"),
                IsolationStrategy::row("public", "tid"),
            ));
            tm
        }

        #[test]
        fn tenant_table_gets_filter() {
            let res = manager().transform_query("select * from t", &TenantId::new("acme"));
            assert!(res.transformed, "expected a tenant filter to be injected");
            let q = res.query.to_lowercase();
            assert!(
                q.contains("tid") && q.contains("acme"),
                "filter missing: {}",
                res.query
            );
        }

        #[test]
        fn non_tenant_table_passes_through() {
            let res = manager().transform_query("select * from other", &TenantId::new("acme"));
            assert!(!res.transformed);
        }
    }

    // ---- ha-tr: the journal records statements the replay engine reads ----

    #[cfg(feature = "ha-tr")]
    mod ha_tr {
        use crate::transaction_journal::TransactionJournal;
        use crate::NodeId;

        #[tokio::test]
        async fn journal_records_and_windows_a_statement() {
            let j = TransactionJournal::new();
            let from = chrono::Utc::now() - chrono::Duration::seconds(60);
            let tx = uuid::Uuid::new_v4();
            j.begin_transaction(tx, uuid::Uuid::new_v4(), NodeId::new(), 0)
                .await
                .unwrap();
            j.log_statement(
                tx,
                "insert into t values (1)".to_string(),
                Vec::new(),
                None,
                None,
                0,
            )
            .await
            .unwrap();
            let to = chrono::Utc::now() + chrono::Duration::seconds(60);
            let entries = j.entries_in_window(from, to).await;
            assert_eq!(entries.len(), 1, "journaled statement should be in window");
            assert!(entries[0].1.statement.contains("insert"));
        }
    }

    // ---- schema-routing: OLAP vs OLTP workload classification ----

    #[cfg(feature = "schema-routing")]
    mod schema_routing {
        use crate::schema_routing::{QueryAnalyzer, SchemaRegistry};
        use std::sync::Arc;

        fn analyzer() -> QueryAnalyzer {
            QueryAnalyzer::new(Arc::new(SchemaRegistry::new()))
        }

        #[test]
        fn aggregation_group_by_is_analytics() {
            let a = analyzer();
            assert!(a
                .analyze("select count(*) from orders group by region")
                .is_analytics());
        }

        #[test]
        fn simple_point_query_is_not_analytics() {
            let a = analyzer();
            assert!(!a
                .analyze("select * from orders where id = 1")
                .is_analytics());
        }
    }

    /// The conditional-reset classifier must call every session-state-creating
    /// statement DIRTY (so it is reset before reuse) and only provably neutral
    /// statements CLEAN. A false "clean" would leak state across clients, so the
    /// dirty cases here are the security-critical half of the test.
    #[cfg(feature = "pool-modes")]
    #[test]
    fn stmt_classifier_is_conservative() {
        let clean = ProxyServer::stmt_leaves_session_state;
        // ---- Provably clean (reset may be skipped) ----
        assert!(!clean(
            "SELECT abalance FROM pgbench_accounts WHERE aid = 12345"
        ));
        assert!(!clean("SELECT 1"));
        assert!(!clean("SELECT 1;")); // single trailing ';'
        assert!(!clean("  select now()  ")); // read of a volatile fn: no session state
        assert!(!clean("INSERT INTO t VALUES (1)")); // INTO is INSERT syntax, not SELECT INTO
        assert!(!clean("UPDATE t SET c = 1 WHERE id = 2")); // "SET" is UPDATE syntax, not a GUC
        assert!(!clean("DELETE FROM t WHERE id = 3"));
        assert!(!clean("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!clean("SELECT into_total FROM ledger")); // column named into_total, not INTO kw
        assert!(!clean("BEGIN"));
        assert!(!clean("COMMIT"));
        assert!(!clean("SELECT current_setting('work_mem')")); // reading a GUC is fine

        // ---- Must be DIRTY (reset required) ----
        assert!(clean("SET work_mem = '1GB'"), "SET GUC");
        assert!(clean("set search_path to public"), "lowercase SET");
        assert!(clean("CREATE TEMP TABLE t(x int)"), "temp table");
        assert!(clean("CREATE TEMPORARY TABLE t(x int)"), "temp table");
        assert!(clean("SELECT * INTO TEMP t FROM src"), "SELECT INTO temp");
        assert!(clean("select a into t from s"), "SELECT INTO lowercase");
        assert!(clean("PREPARE p AS SELECT 1"), "prepared statement");
        assert!(clean("DEALLOCATE p"), "deallocate");
        assert!(
            clean("DECLARE c CURSOR WITH HOLD FOR SELECT 1"),
            "held cursor"
        );
        assert!(clean("LISTEN my_channel"), "listen");
        assert!(clean("SELECT pg_advisory_lock(42)"), "advisory lock");
        assert!(clean("SELECT pg_try_advisory_lock(1)"), "try advisory lock");
        assert!(
            clean("SELECT set_config('work_mem','1GB',false)"),
            "set_config fn"
        );
        assert!(clean("SELECT nextval('s')"), "sequence cache");
        assert!(clean("SET ROLE admin"), "set role");
        assert!(clean("SET SESSION AUTHORIZATION bob"), "session auth");
        assert!(clean("DISCARD ALL"), "explicit discard");
        assert!(clean("RESET ALL"), "reset");
        // Multi-statement: a neutral lead cannot vouch for what follows a ';'.
        assert!(clean("SELECT 1; SET work_mem='1GB'"), "hidden SET after ;");
        assert!(
            clean("SELECT 1; CREATE TEMP TABLE t(x int)"),
            "hidden temp after ;"
        );
        // ';' inside a literal → conservatively dirty (safe over-reset).
        assert!(clean("SELECT 'a;b'"), "semicolon in literal");
        // Non-neutral leads.
        assert!(clean("COPY t FROM STDIN"), "copy");
        assert!(clean("GRANT SELECT ON t TO bob"), "grant");
        assert!(clean("ALTER TABLE t ADD COLUMN c int"), "ddl");
    }

    /// `reset_backend` must only report success when the reset query cleanly
    /// completed — no ErrorResponse and an idle ReadyForQuery. A poisoned reset
    /// (error, or a non-idle transaction status) must return `Err` so the caller
    /// drops the connection instead of parking it dirty (Group 2, 2.0.b).
    #[cfg(feature = "pool-modes")]
    #[tokio::test]
    async fn reset_backend_rejects_error_and_nonidle() {
        use tokio::io::AsyncWriteExt as _;
        fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![tag];
            v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            v.extend_from_slice(body);
            v
        }
        let rfq = |st: u8| frame(b'Z', &[st]);
        let cc = frame(b'C', b"DISCARD ALL\0");
        let err = frame(b'E', b"SERROR\0C25P02\0Mreset failed\0\0");

        // Clean: CommandComplete + ReadyForQuery('I') -> Ok.
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut resp = cc.clone();
        resp.extend_from_slice(&rfq(b'I'));
        server.write_all(&resp).await.unwrap();
        assert!(
            ProxyServer::reset_backend(&mut client, "DISCARD ALL")
                .await
                .is_ok(),
            "clean reset must succeed"
        );

        // ErrorResponse before RFQ -> Err (connection is poisoned).
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut resp = err.clone();
        resp.extend_from_slice(&rfq(b'I'));
        server.write_all(&resp).await.unwrap();
        assert!(
            ProxyServer::reset_backend(&mut client, "DISCARD ALL")
                .await
                .is_err(),
            "reset that errored must be rejected"
        );

        // Non-idle status ('T') -> Err (still in a transaction).
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut resp = cc.clone();
        resp.extend_from_slice(&rfq(b'T'));
        server.write_all(&resp).await.unwrap();
        assert!(
            ProxyServer::reset_backend(&mut client, "DISCARD ALL")
                .await
                .is_err(),
            "reset leaving a non-idle txn must be rejected"
        );
    }

    /// The pool identity key stays the bare `(node,user,db)` triple when no
    /// routing-relevant startup GUC is set (backward-compatible with existing
    /// pooling), but diverges when a client sets a different `client_encoding` /
    /// `DateStyle` / etc., so such clients never share a connection (Group 2,
    /// 2.0.c).
    #[cfg(feature = "pool-modes")]
    #[tokio::test]
    async fn pool_key_folds_startup_params() {
        let base = make_test_session();
        {
            let mut v = base.variables.write().await;
            v.insert("user".into(), "u".into());
            v.insert("database".into(), "d".into());
        }
        let k_plain = ProxyServer::pool_key_for("n:5432", &base).await;
        assert_eq!(k_plain, crate::pool::pool_key("n:5432", "u", "d"));

        // Same identity but a distinct client_encoding must produce a
        // different key (no cross-encoding sharing).
        let utf8 = make_test_session();
        let latin1 = make_test_session();
        for (s, enc) in [(&utf8, "UTF8"), (&latin1, "LATIN1")] {
            let mut v = s.variables.write().await;
            v.insert("user".into(), "u".into());
            v.insert("database".into(), "d".into());
            v.insert("client_encoding".into(), enc.into());
        }
        let k_utf8 = ProxyServer::pool_key_for("n:5432", &utf8).await;
        let k_latin1 = ProxyServer::pool_key_for("n:5432", &latin1).await;
        assert_ne!(k_utf8, k_latin1, "different client_encoding must not share");
        assert_ne!(k_utf8, k_plain, "GUC-bearing key must differ from bare key");
    }
}
