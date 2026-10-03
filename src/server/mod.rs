//! Proxy Server Implementation
//!
//! Main server that accepts client connections and routes them to backends.
//! Implements PostgreSQL wire protocol forwarding with TWR (Transparent Write Routing).

use crate::admin::{AdminServer, AdminState, ConfigSnapshot, NodeSnapshot};
use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::client_tls::{build_tls_acceptor, ClientStream};
use crate::config::{HbaAction, HbaRule, NodeConfig, NodeRole, ProxyConfig, Strategy, TrMode};
use crate::primary_tracker::PrimaryTracker;
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
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

mod limits;
use limits::ResolvedLimits;

mod metrics;
#[cfg(feature = "pool-modes")]
pub use metrics::PoolModeStatsSnapshot;
use metrics::ServerMetrics;
#[cfg(test)]
use metrics::TrMetrics;
pub use metrics::{ServerMetricsSnapshot, TrMetricsSnapshot};

mod tr;
use tr::{BackendFault, FaultPhase, InFlight, ResponseFailure, ResponseProgress, TrSession};
#[cfg(test)]
use tr::{ReplayFailure, StmtKind, TrAction};

mod tr_policy;
pub use tr_policy::TrReadPolicy;
use tr_policy::{GucOp, Observation};
#[cfg(test)]
use tr_policy::{TR_CALL_KEYWORDS, TR_PURE_BUILTINS};

mod frames;
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
use frames::RelayLimits;
use frames::{backend_frame_len, read_budget, validate_backend_frame_len};

mod session;
use session::{BackendConn, SessionGuard};
pub use session::{ClientSession, ExtendedBatchLog, StatementLog, TransactionState};

mod stmt_facts;
#[cfg(test)]
use stmt_facts::stmt_fact_classifications;
#[cfg(feature = "query-cache")]
use stmt_facts::CacheWork;
#[cfg(feature = "query-cache")]
pub(crate) use stmt_facts::TxCacheStage;
use stmt_facts::{ObservedCycle, StmtFacts};

mod client;

mod auth;

mod backend_conn;

mod relay;

mod sql_classify;

mod policy;

mod journal_hooks;

mod routing;

mod responses;

/// Proxy server
pub struct ProxyServer {
    config: ProxyConfig,
    state: Arc<ServerState>,
    shutdown_tx: broadcast::Sender<()>,
    /// Path the config was loaded from, retained so `SIGHUP` can re-read it
    /// for a zero-downtime reload (Batch H). `None` when the config was built
    /// from CLI flags/defaults rather than a file.
    config_path: Option<String>,
    /// TR-07: committed transactions recovered from `[journal] dir` at
    /// construction, loaded into the journal's committed store when `run`
    /// starts (the load takes the journal's async lock).
    journal_recovered: std::sync::Mutex<Vec<crate::transaction_journal::TransactionJournalEntry>>,
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
///
/// Writes into `out` (cleared first) rather than returning a fresh
/// `String` so the per-query hot path can hand it a reusable buffer
/// and allocate nothing once the buffer has grown.
#[cfg(feature = "anomaly-detection")]
fn anomaly_fingerprint_into(sql: &str, out: &mut String) {
    out.clear();
    out.reserve(sql.len());
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
    // Same as the old `out.trim_end().to_string()`, in place.
    let trimmed_len = out.trim_end().len();
    out.truncate(trimmed_len);
}

/// Owning wrapper around [`anomaly_fingerprint_into`]. Test-only —
/// the hot path uses the buffer form.
#[cfg(all(test, feature = "anomaly-detection"))]
fn anomaly_fingerprint(sql: &str) -> String {
    let mut out = String::new();
    anomaly_fingerprint_into(sql, &mut out);
    out
}

/// Concrete provider poll task (H-01). The `static` provider has none; the
/// postgres provider polls `pg_is_in_recovery()` and the patroni provider
/// polls Patroni's `GET /cluster`, each in its own background task,
/// publishing events the tracker consumes.
enum TopologyPoller {
    Static,
    #[cfg(feature = "postgres-topology")]
    Postgres(Arc<crate::primary_tracker::PostgresTopologyProvider>),
    #[cfg(feature = "postgres-topology")]
    Patroni(Arc<crate::primary_tracker::PatroniTopologyProvider>),
}

/// Deterministic 64-bit mix (splitmix64), used for jitter and sampling
/// (H-03/H-05).
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Parse PostgreSQL's textual LSN (`"0/16B3748"`) into the standard u64
/// encoding (high 32 bits / low 32 bits), used for WAL-lag arithmetic (H-04).
fn parse_pg_lsn(text: &str) -> Option<u64> {
    let (hi, lo) = text.trim().split_once('/')?;
    let hi = u64::from_str_radix(hi, 16).ok()?;
    let lo = u64::from_str_radix(lo, 16).ok()?;
    Some((hi << 32) | (lo & 0xffff_ffff))
}

/// Server runtime state
struct ServerState {
    /// Operational limits/timeouts resolved from `[limits]` config at startup.
    limits: ResolvedLimits,
    /// Permit pool bounding concurrently-served client connections, sized from
    /// `[limits] max_client_connections` when that is > 0 (`None` = unlimited,
    /// the default and the historical behaviour). One owned permit is taken per
    /// connection by `admit_client_slot` — after its first startup message is
    /// classified, so a `CancelRequest` is never refused — and handed to the
    /// connection's `SessionGuard`, so the permit is returned on every exit
    /// path including a panic unwind. Sized ONCE at startup: a SIGHUP reload
    /// cannot shrink it (permits already held by in-flight sessions could not
    /// be revoked), so the reload path logs and ignores a change to the key.
    client_slots: Option<Arc<tokio::sync::Semaphore>>,
    /// Active client sessions. A `DashMap` (not a `tokio::RwLock<HashMap>`) so
    /// register/deregister are synchronous and lock-free-sharded — which lets a
    /// `SessionGuard`'s `Drop` remove the entry on ANY exit, including a panic
    /// unwind, and removes a per-connection async write-lock from the accept and
    /// teardown paths.
    sessions: DashMap<Uuid, Arc<ClientSession>>,
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
    /// Authoritative primary tracker (H-01). Standalone (manual) by default;
    /// when `[topology] provider != "static"` it is backed by a topology
    /// provider and its answer is authoritative for the write path.
    primary_tracker: Arc<PrimaryTracker>,
    /// True when `[topology] provider` is not `static`. The write path then
    /// uses only the tracker's leader and never falls back to configured
    /// roles, so a missing provider answer fails closed instead of writing
    /// to a stale primary.
    authoritative_topology: bool,
    /// Provider poll task (H-01): `static` has none; `postgres` polls in
    /// `run()` and feeds the tracker.
    topology_poller: TopologyPoller,
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
    /// this directly. Always present; journaling is skipped when
    /// `tr_enabled = false`.
    transaction_journal: Arc<crate::transaction_journal::TransactionJournal>,
    /// TR-03 read re-execution eligibility (built-ins + `tr_read_functions`).
    tr_read_policy: Arc<TrReadPolicy>,
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
    /// Consecutive successful probes (H-03 recovery threshold)
    pub success_count: u32,
    /// Last error message
    pub last_error: Option<String>,
    /// Average latency (ms)
    pub latency_ms: f64,
    /// Replication lag (if a WAL-position probe succeeded; H-04). `None`
    /// means unknown — a strict policy can refuse unknown lag.
    pub replication_lag_bytes: Option<u64>,
    /// When `replication_lag_bytes` was last sampled (H-04). `None` when lag
    /// has never been measured.
    pub lag_sampled_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Load balancer state
struct LoadBalancerState {
    /// Round-robin counter. Atomic so the read-routing path never
    /// takes a write lock just to advance the rotation.
    rr_counter: AtomicU64,
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

/// Outcome of waiting for the client's next protocol message.
#[derive(Debug, PartialEq, Eq)]
enum ClientRead {
    /// Bytes were appended to the session buffer (`0` = the client closed).
    Bytes(usize),
    /// The `[limits] client_idle_timeout_secs` deadline expired while the
    /// session sat idle between statements.
    IdleTimeout,
}

/// SQLSTATE `too_many_connections`: the backend is at `max_connections`.
const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";

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

        // Resolve the [limits] section once (secs -> Duration) so every hot-path
        // use site reads a ready value; stored on ServerState below.
        let resolved_limits = ResolvedLimits::from_toml(&config.limits);

        // TR-07: the recovery journal, optionally backed by the durable
        // segment store under `[journal] dir`. Recovery happens here (sync
        // file I/O at startup); the recovered transactions are loaded into
        // the committed store when `run` starts.
        let mut journal = crate::transaction_journal::TransactionJournal::new()
            .with_max_journals(config.journal.max_active_transactions)
            .with_max_entries(config.journal.max_entries_per_transaction)
            .with_max_size(config.journal.max_bytes_per_transaction)
            .with_max_committed(config.journal.max_committed_transactions)
            .with_max_committed_bytes(config.journal.max_committed_bytes);
        let mut journal_recovered = Vec::new();
        if let Some(store_cfg) = crate::journal_store::StoreConfig::from_toml(&config.journal) {
            let dir = store_cfg.dir.clone();
            let (store, recovered) = crate::journal_store::SegmentStore::open(
                store_cfg,
                config.journal.max_committed_transactions,
                config.journal.max_committed_bytes,
            )
            .map_err(|e| ProxyError::Config(format!("journal.dir {}: {}", dir.display(), e)))?;
            tracing::info!(
                dir = %dir.display(),
                segments = recovered.segments,
                records = recovered.records_read,
                retained = recovered.transactions.len(),
                commit_seq_high = recovered.commit_seq_high,
                truncated_bytes = recovered.truncated_bytes,
                "recovery journal opened"
            );
            let sink = store.spawn_writer().map_err(|e| {
                ProxyError::Config(format!("journal.dir {}: writer: {}", dir.display(), e))
            })?;
            journal = journal.with_sink(sink);
            journal.set_next_commit_seq(recovered.commit_seq_high + 1);
            journal_recovered = recovered.transactions;
        }

        // Initialize health status
        let mut health = HashMap::new();
        for node in &config.nodes {
            health.insert(
                node.address().to_string(),
                NodeHealth {
                    address: node.address().to_string(),
                    healthy: true, // Assume healthy until proven otherwise
                    last_check: chrono::Utc::now(),
                    failure_count: 0,
                    success_count: 0,
                    last_error: None,
                    latency_ms: 0.0,
                    replication_lag_bytes: None,
                    lag_sampled_at: None,
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
                    resolved_limits.max_total_idle_backend_conns,
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
                queue_capacity = a.queue_capacity,
                fingerprint_cache_size = a.fingerprint_cache_size,
                fingerprint_cache_max_sql_bytes = a.fingerprint_cache_max_sql_bytes,
                "query analytics enabled"
            );
            let ac = crate::analytics::AnalyticsConfig {
                enabled: true,
                max_fingerprints: a.max_fingerprints as usize,
                queue_capacity: a.queue_capacity as usize,
                fingerprint_cache_size: a.fingerprint_cache_size as usize,
                fingerprint_cache_max_sql_bytes: a.fingerprint_cache_max_sql_bytes as usize,
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

        // Size the client-connection permit pool once, here at startup. A `0`
        // cap (the default) leaves it `None` so the accept path is unchanged.
        let client_slots = match resolved_limits.max_client_connections {
            0 => None,
            n => {
                tracing::info!(
                    max_client_connections = n,
                    "client connection cap enabled; excess connections are refused with SQLSTATE 53300"
                );
                Some(Arc::new(tokio::sync::Semaphore::new(n)))
            }
        };
        // H-01: authoritative topology tracker (static/standalone by default).
        let (primary_tracker, authoritative_topology, topology_poller) =
            Self::build_primary_tracker(&config);
        let state = Arc::new(ServerState {
            limits: resolved_limits,
            client_slots,
            sessions: DashMap::new(),
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
            primary_tracker,
            authoritative_topology,
            topology_poller,
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
            transaction_journal: Arc::new(journal),
            tr_read_policy: Arc::new(TrReadPolicy::from_config(&config.tr_read_functions)),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(crate::anomaly::AnomalyDetector::new(
                config.anomaly.to_anomaly_config(),
            )),
            #[cfg(feature = "edge-proxy")]
            edge_cache: Arc::new(crate::edge::EdgeCache::with_budget(
                config.edge.max_entries.max(1),
                config.cache.max_cacheable_response_bytes,
                if config.edge.max_total_bytes == 0 {
                    usize::MAX
                } else {
                    config.edge.max_total_bytes
                },
            )),
            #[cfg(feature = "edge-proxy")]
            edge_registry: Arc::new(crate::edge::EdgeRegistry::new(
                config.edge.max_edges,
                std::time::Duration::from_secs(config.edge.liveness_window_secs),
            )),
        });

        Ok(Self {
            journal_recovered: std::sync::Mutex::new(journal_recovered),
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
            let active = state.sessions.len();
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
        let mut new_config = match ProxyConfig::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(path, error = %e, "SIGHUP reload failed to parse — keeping current config");
                return;
            }
        };
        let old = self.state.live_config.load_full();
        // [edge] is applied at startup only: the EdgeClient subscription and
        // the registry GC task are spawned once in run() from the boot
        // config. Letting the per-connection data path follow a reloaded
        // [edge] would half-enable the feature (e.g. role=edge serving
        // cached reads with no invalidation subscription ever spawned), so
        // carry the running section forward — same warn-and-keep treatment
        // as listen_address/admin_address.
        if new_config.edge != old.edge {
            tracing::warn!(
                "[edge] configuration changed on SIGHUP but is applied at startup only — \
                 keeping the running edge settings (restart to apply)"
            );
            new_config.edge = old.edge.clone();
        }
        // `[limits]` is resolved once at startup into `state.limits` (and, for
        // max_client_connections, into the already-sized `client_slots`
        // semaphore whose outstanding permits could not be revoked anyway), so
        // a reload cannot apply it. Warn on the two opt-in stability bounds
        // rather than let an operator believe a SIGHUP armed them.
        if new_config.limits.max_client_connections != old.limits.max_client_connections
            || new_config.limits.client_idle_timeout_secs != old.limits.client_idle_timeout_secs
            || new_config.limits.client_admission_wait_secs != old.limits.client_admission_wait_secs
        {
            tracing::warn!(
                old_max_client_connections = old.limits.max_client_connections,
                new_max_client_connections = new_config.limits.max_client_connections,
                old_client_idle_timeout_secs = old.limits.client_idle_timeout_secs,
                new_client_idle_timeout_secs = new_config.limits.client_idle_timeout_secs,
                old_client_admission_wait_secs = old.limits.client_admission_wait_secs,
                new_client_admission_wait_secs = new_config.limits.client_admission_wait_secs,
                "[limits] max_client_connections / client_idle_timeout_secs / client_admission_wait_secs changed on SIGHUP but are applied at startup only — keeping the running values (restart to apply)"
            );
            // Keep the published config truthful: `/config` must not advertise
            // a cap/idle timeout that is not the one in effect (same treatment
            // as `[edge]` above).
            new_config.limits.max_client_connections = old.limits.max_client_connections;
            new_config.limits.client_idle_timeout_secs = old.limits.client_idle_timeout_secs;
            new_config.limits.client_admission_wait_secs = old.limits.client_admission_wait_secs;
        }
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
            let addr = node.address().to_string();
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
                            success_count: 0,
                            last_error: None,
                            latency_ms: 0.0,
                            replication_lag_bytes: None,
                            lag_sampled_at: None,
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
        // TR-07: make recovered committed history visible to replay before
        // the first client is served.
        let recovered = std::mem::take(
            &mut *self
                .journal_recovered
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        if !recovered.is_empty() {
            let n = recovered.len();
            self.state
                .transaction_journal
                .load_committed(recovered)
                .await;
            tracing::info!(transactions = n, "recovery journal reloaded");
        }

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
        // H-01: authoritative topology loop. Only spawned when a provider is
        // configured; static configs keep the historical routing path. Two
        // tasks: the provider polls the database, the tracker translates its
        // events into the leader the write path and /topology consume.
        let topology_task = if self.state.authoritative_topology {
            let tracker = self.state.primary_tracker.clone();
            let poller: Option<tokio::task::JoinHandle<()>> = match &self.state.topology_poller {
                TopologyPoller::Static => None,
                #[cfg(feature = "postgres-topology")]
                TopologyPoller::Postgres(p) => {
                    let p = p.clone();
                    Some(tokio::spawn(async move { p.start().await }))
                }
                #[cfg(feature = "postgres-topology")]
                TopologyPoller::Patroni(p) => {
                    let p = p.clone();
                    Some(tokio::spawn(async move { p.start().await }))
                }
            };
            let tracker_task = tokio::spawn(async move { tracker.run().await });
            Some((poller, tracker_task))
        } else {
            None
        };

        // Single background analytics consumer. Everything expensive about
        // recording a query (fingerprint normalization, statistics, slow-query
        // log, pattern detection, cost attribution) runs here instead of on the
        // connection task that served the query; the relay only pays a
        // `try_send` onto a bounded queue. Started exactly once, here, and
        // aborted with the other background tasks at shutdown.
        #[cfg(feature = "query-analytics")]
        let analytics_task = self
            .state
            .analytics
            .as_ref()
            .and_then(|a| a.start_consumer());

        // Edge registry GC — prunes edges not seen within the liveness window.
        #[cfg(feature = "edge-proxy")]
        let _edge_maintenance_task = if self.config.edge.enabled {
            Some(self.spawn_edge_maintenance())
        } else {
            None
        };

        // Start admin server
        let admin_task = self.spawn_admin_server();

        // H-06: per-gateway idle pools for the non-PG-wire gateways, so the
        // HTTP, MCP and GraphQL gateways reuse authenticated backend
        // connections instead of dialing per request. Kept separate because a
        // pool's session policy (e.g. MCP read-only GUC) must not leak into
        // another gateway's connections.
        let gw_max = self.state.limits.gateway_pool_max_idle;
        let mcp_pool = Arc::new(crate::gateway_pool::BackendClientPool::new(gw_max));
        let http_pool = Arc::new(crate::gateway_pool::BackendClientPool::new(gw_max));
        #[cfg(feature = "graphql-gateway")]
        let gql_pool = Arc::new(crate::gateway_pool::BackendClientPool::new(gw_max));

        // Start the MCP agent gateway when enabled.
        let mcp_task = if self.config.mcp.enabled {
            let mcp_cfg = self.config.mcp.clone();
            let pool = mcp_pool.clone();
            // Resolve the configured agent contract (scoped grants) by id.
            let contract = mcp_cfg.contract.as_ref().and_then(|id| {
                let found = self.config.agent_contracts.iter().find(|c| &c.id == id).cloned();
                if found.is_none() {
                    tracing::warn!(%id, "mcp.contract names an unknown agent_contract; gateway runs with only the read-only guardrail");
                }
                found
            });
            Some(tokio::spawn(async move {
                if let Err(e) = crate::mcp::McpServer::new(mcp_cfg, contract, pool)
                    .run()
                    .await
                {
                    tracing::error!("MCP gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Start the HTTP SQL gateway (Neon-serverless compatible) when enabled.
        let http_gw_task = if self.config.http_gateway.enabled {
            let gw_cfg = self.config.http_gateway.clone();
            let pool = http_pool.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = crate::http_gateway::HttpGateway::new(gw_cfg, pool)
                    .run()
                    .await
                {
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
            let pool = gql_pool.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = crate::graphql_gateway::GraphqlGateway::new(gw_cfg, pool)
                    .run()
                    .await
                {
                    tracing::error!("GraphQL gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Edge role: hold an SSE subscription against the home's admin API so
        // invalidations drop matching local cache entries as they arrive.
        #[cfg(feature = "edge-proxy")]
        let _edge_client_task =
            if self.config.edge.enabled && self.config.edge.role == crate::edge::EdgeRole::Edge {
                tracing::info!(
                    home_url = %self.config.edge.home_url,
                    region = %self.config.edge.region,
                    "edge role: subscribing to home invalidation stream"
                );
                Some(crate::edge::client::EdgeClient::spawn(
                    self.config.edge.clone(),
                    self.state.edge_cache.clone(),
                ))
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
                            // NOTE: the `[limits] max_client_connections` cap is
                            // NOT applied here. A slot is taken inside
                            // `handle_client`, once the first startup message has
                            // been classified — a `CancelRequest` arrives as its
                            // own fresh connection and must still be served when
                            // the proxy is saturated (that is exactly when an
                            // operator needs to cancel a query), just as
                            // PostgreSQL handles cancels in the postmaster
                            // without consuming a `max_connections` slot.
                            self.state.metrics.connections_accepted.fetch_add(1, Ordering::Relaxed);
                            let state = self.state.clone();
                            // Snapshot the *live* config so a SIGHUP reload
                            // applies to new connections; in-flight sessions
                            // keep the snapshot they began with (Batch H). This
                            // is an Arc clone (cheap, atomic refcount bump), not
                            // a deep clone of ProxyConfig — see handle_client.
                            let config = self.state.live_config.load_full();
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
        if let Some((poller, tracker_task)) = topology_task {
            if let Some(t) = poller {
                t.abort();
            }
            tracker_task.abort();
        }
        admin_task.abort();
        // Drain what the connection tasks already queued before killing the
        // consumer, so a graceful handoff does not silently lose the last few
        // hundred samples. Bounded by the same `shutdown_drain_timeout_secs`
        // budget as the connection drain — the queue is small and this is
        // normally instant, but a wedged consumer must not hold up exit.
        #[cfg(feature = "query-analytics")]
        if let Some(t) = analytics_task {
            if let Some(a) = self.state.analytics.as_ref() {
                let budget =
                    Self::drain_timeout(self.state.live_config.load().shutdown_drain_timeout_secs);
                if tokio::time::timeout(budget, a.flush()).await.is_err() {
                    tracing::warn!("analytics ingest queue did not drain before shutdown");
                }
            }
            t.abort();
        }
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

            // H-01/H-02: share the daemon's authoritative tracker only when a
            // provider is configured; static configs keep the historical
            // role+health topology response.
            if state.authoritative_topology {
                admin_state
                    .with_primary_tracker(state.primary_tracker.clone())
                    .await;
            }

            // Initialize config snapshot
            {
                let mut snapshot = admin_state.config_snapshot.write().await;
                *snapshot = ConfigSnapshot {
                    listen_address: config.listen_address.clone(),
                    admin_address: config.admin_address.clone(),
                    tr_enabled: config.tr_enabled,
                    tr_mode: format!("{:?}", config.effective_tr_mode()),
                    pool_min_connections: config.pool.min_connections,
                    pool_max_connections: config.pool.max_connections,
                    nodes: config
                        .nodes
                        .iter()
                        .map(|n| NodeSnapshot {
                            address: n.address().to_string(),
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
            {
                let template = build_replay_backend_template(&config);
                let engine = Arc::new(
                    crate::replay::ReplayEngine::new(state.transaction_journal.clone(), template)
                        .with_deadline(state.limits.replay_deadline),
                );
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
                            connections_rejected: server_state
                                .metrics
                                .connections_rejected
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
                            cache_capture_oversize: server_state
                                .metrics
                                .cache_capture_oversize
                                .load(Ordering::Relaxed),
                            admission_waited: server_state
                                .metrics
                                .admission_waited
                                .load(Ordering::Relaxed),
                            admission_timeouts: server_state
                                .metrics
                                .admission_timeouts
                                .load(Ordering::Relaxed),
                            reconnect_attempts: server_state
                                .metrics
                                .reconnect_attempts
                                .load(Ordering::Relaxed),
                            backend_capacity_waits: server_state
                                .metrics
                                .backend_capacity_waits
                                .load(Ordering::Relaxed),
                            backend_capacity_refusals: server_state
                                .metrics
                                .backend_capacity_refusals
                                .load(Ordering::Relaxed),
                            journal_committed: server_state
                                .metrics
                                .journal_committed
                                .load(Ordering::Relaxed),
                            journal_rolled_back: server_state
                                .metrics
                                .journal_rolled_back
                                .load(Ordering::Relaxed),
                            journal_statements: server_state
                                .metrics
                                .journal_statements
                                .load(Ordering::Relaxed),
                            tr: server_state.metrics.tr.snapshot(),
                        };
                        let mut admin_metrics = admin_state_sync.metrics.write().await;
                        *admin_metrics = metrics;
                    }

                    // Sync session count
                    {
                        let mut admin_sessions = admin_state_sync.active_sessions.write().await;
                        *admin_sessions = server_state.sessions.len() as u64;
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
        // Both the fingerprint and the SQL are *lent* to the detector:
        // the fingerprint from a buffer sized for this call, the SQL
        // straight from the wire frame. The detector copies only on
        // the rare paths that retain something (first-seen
        // fingerprint, emitted event excerpt). The fingerprint buffer
        // is allocated per call rather than cached in a thread-local,
        // so nothing is retained between queries — a client sending
        // one very large statement does not leave its buffer
        // permanently resident on the worker thread.
        let mut fingerprint = String::with_capacity(query.len());
        anomaly_fingerprint_into(query, &mut fingerprint);
        let obs = crate::anomaly::QueryObservation {
            tenant,
            fingerprint: std::borrow::Cow::Borrowed(fingerprint.as_str()),
            sql: std::borrow::Cow::Borrowed(query),
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
                        // Run the sweep in a child task so an unexpected panic in
                        // check_all_nodes surfaces as a JoinError and is logged —
                        // the health loop keeps running instead of dying silently
                        // and freezing health at its last snapshot forever.
                        let st = state.clone();
                        if let Err(e) = tokio::spawn(async move {
                            Self::check_all_nodes(&st, &config).await;
                        })
                        .await
                        {
                            tracing::error!(error = %e, "health-check sweep panicked; health loop continuing");
                        }
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
        // H-04: with credentials configured, sample the primary's current WAL
        // position once per sweep so each standby's replay position becomes a
        // byte lag. Best-effort; any failure leaves lag unknown.
        let primary_lsn = if config.health.user.is_some() && config.lag_routing.enabled {
            Self::probe_primary_lsn(config, timeout).await
        } else {
            None
        };

        let mut set = tokio::task::JoinSet::new();
        for node in &config.nodes {
            let addr = node.address().to_string();
            let health_cfg = config.health.clone();
            let role = node.role;
            set.spawn(async move {
                let r = Self::probe_node(&health_cfg, &addr, role, primary_lsn, timeout).await;
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
                let (ok, latency, lag) = match result {
                    Ok((latency, lag)) => (true, Some(latency), lag),
                    Err(e) => {
                        node_health.last_error = Some(e.to_string());
                        (false, None, None)
                    }
                };
                let (healthy, sc, fc) = Self::advance_health(
                    node_health.healthy,
                    node_health.success_count,
                    node_health.failure_count,
                    ok,
                    config.health.failure_threshold,
                    config.health.success_threshold,
                );
                let was_healthy = node_health.healthy;
                node_health.healthy = healthy;
                node_health.success_count = sc;
                node_health.failure_count = fc;
                if let Some(latency) = latency {
                    node_health.latency_ms = latency;
                    node_health.last_error = None;
                }
                if let Some(lag) = lag {
                    node_health.replication_lag_bytes = Some(lag);
                    node_health.lag_sampled_at = Some(chrono::Utc::now());
                }
                if was_healthy && !healthy {
                    tracing::warn!(
                        "Node {} marked unhealthy after {} failures",
                        addr,
                        node_health.failure_count
                    );
                } else if !was_healthy && healthy {
                    tracing::info!(
                        "Node {} recovered after {} consecutive successes",
                        addr,
                        node_health.success_count
                    );
                }
                node_health.last_check = chrono::Utc::now();
            }
        }
        state.health.store(Arc::new(next));
    }

    /// Advance the consecutive success/failure counters and decide health
    /// (H-03). A healthy node stays healthy on success; a node marked
    /// unhealthy only returns to healthy after `success_threshold` consecutive
    /// successes. Failures always count toward `failure_threshold`. Pure so the
    /// policy is unit-tested without a backend.
    fn advance_health(
        healthy: bool,
        success_count: u32,
        failure_count: u32,
        ok: bool,
        failure_threshold: u32,
        success_threshold: u32,
    ) -> (bool, u32, u32) {
        if ok {
            let sc = success_count.saturating_add(1);
            (healthy || sc >= success_threshold.max(1), sc, 0)
        } else {
            let fc = failure_count.saturating_add(1);
            let now_healthy = if fc >= failure_threshold.max(1) {
                false
            } else {
                healthy
            };
            (now_healthy, 0, fc)
        }
    }

    /// One node probe (H-03/H-04): a credential-less protocol probe by default,
    /// or `check_query` plus a standby WAL-position lag probe when
    /// `[health] user` is configured. Returns `(latency_ms, lag_bytes)`.
    async fn probe_node(
        health: &crate::config::HealthConfig,
        addr: &str,
        role: NodeRole,
        primary_lsn: Option<u64>,
        timeout: Duration,
    ) -> Result<(f64, Option<u64>)> {
        if health.user.is_none() {
            return Self::check_node_addr(addr, timeout)
                .await
                .map(|l| (l, None));
        }
        Self::check_node_query(health, addr, role, primary_lsn, timeout).await
    }

    /// Credentialed probe: connect, run `[health] check_query` and, for a
    /// standby/read-replica with a known primary LSN, `pg_last_wal_replay_lsn()`
    /// to compute the byte lag. The lag probe is best-effort and never fails
    /// the health check (non-PostgreSQL backends simply report unknown lag).
    async fn check_node_query(
        health: &crate::config::HealthConfig,
        addr: &str,
        role: NodeRole,
        primary_lsn: Option<u64>,
        timeout: Duration,
    ) -> Result<(f64, Option<u64>)> {
        use crate::backend::{
            tls::default_client_config, BackendClient, BackendConfig, TextValue, TlsMode,
        };
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| ProxyError::HealthCheck(format!("bad node address {}", addr)))?;
        let port: u16 = port
            .parse()
            .map_err(|_| ProxyError::HealthCheck(format!("bad node port {}", addr)))?;
        let mk_cfg = |user: String| BackendConfig {
            host: host.to_string(),
            port,
            user,
            password: health.password.clone(),
            database: health.database.clone(),
            application_name: Some("heliosdb-proxy-health".into()),
            tls_mode: TlsMode::Prefer,
            connect_timeout: timeout,
            query_timeout: timeout,
            tls_config: default_client_config(),
        };
        let cfg = mk_cfg(
            health
                .user
                .clone()
                .unwrap_or_else(|| "postgres".to_string()),
        );
        let start = std::time::Instant::now();
        let mut client = tokio::time::timeout(timeout, BackendClient::connect(&cfg))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("Timeout connecting to {}", addr)))?
            .map_err(|e| ProxyError::HealthCheck(format!("connect {}: {}", addr, e)))?;
        tokio::time::timeout(timeout, client.simple_query(&health.check_query))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("{} check_query timed out", addr)))?
            .map_err(|e| ProxyError::HealthCheck(format!("{} check_query: {}", addr, e)))?;
        let latency = start.elapsed().as_secs_f64() * 1000.0;

        let mut lag = None;
        if matches!(role, NodeRole::Standby | NodeRole::ReadReplica) {
            if let Some(primary) = primary_lsn {
                if let Ok(res) = client
                    .simple_query("SELECT pg_last_wal_replay_lsn()::text")
                    .await
                {
                    let text = res
                        .rows
                        .first()
                        .and_then(|r| r.first())
                        .and_then(|v| match v {
                            TextValue::Text(s) => Some(s.clone()),
                            TextValue::Null => None,
                        });
                    if let Some(text) = text.as_deref().and_then(parse_pg_lsn) {
                        lag = Some(primary.saturating_sub(text));
                    }
                }
            }
        }
        client.close().await;
        Ok((latency, lag))
    }

    /// Sample the configured primary's current WAL position once per sweep.
    /// `None` on any failure (the standby probes then report unknown lag).
    async fn probe_primary_lsn(config: &ProxyConfig, timeout: Duration) -> Option<u64> {
        use crate::backend::{
            tls::default_client_config, BackendClient, BackendConfig, TextValue, TlsMode,
        };
        let primary = config
            .nodes
            .iter()
            .find(|n| n.role == NodeRole::Primary && n.enabled)?;
        let cfg = BackendConfig {
            host: primary.host.clone(),
            port: primary.port,
            user: config
                .health
                .user
                .clone()
                .unwrap_or_else(|| "postgres".to_string()),
            password: config.health.password.clone(),
            database: config.health.database.clone(),
            application_name: Some("heliosdb-proxy-health".into()),
            tls_mode: TlsMode::Prefer,
            connect_timeout: timeout,
            query_timeout: timeout,
            tls_config: default_client_config(),
        };
        let mut client = tokio::time::timeout(timeout, BackendClient::connect(&cfg))
            .await
            .ok()?
            .ok()?;
        let out = tokio::time::timeout(
            timeout,
            client.simple_query("SELECT pg_current_wal_lsn()::text"),
        )
        .await
        .ok()?
        .ok()?;
        client.close().await;
        let text = out
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| match v {
                TextValue::Text(s) => Some(s.as_str()),
                TextValue::Null => None,
            })?;
        parse_pg_lsn(text)
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
        // Resolved from [limits] at startup; captured before the move so the
        // reaper cadence is configurable without a recompile.
        let reap_interval = self.state.limits.pool_reap_interval;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(reap_interval);

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

    /// Spawn the edge-registry maintenance task: a periodic GC sweep that
    /// prunes edges not seen within the liveness window. Healthy subscribers
    /// are continually refreshed by their SSE heartbeat writes (registry
    /// `touch`), so this is a backstop that reaps wedged or dead peers whose
    /// heartbeats stopped succeeding — a pruned edge simply re-registers on
    /// its next reconnect.
    #[cfg(feature = "edge-proxy")]
    fn spawn_edge_maintenance(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        // validate() rejects 0 when edge is enabled; .max(1) keeps the
        // interval panic-proof regardless of how the config was built.
        let gc_secs = self.config.edge.subscribe_gc_secs.max(1);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(gc_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let pruned = state.edge_registry.prune_stale();
                        if pruned > 0 {
                            tracing::debug!(
                                target: "helios::edge",
                                pruned,
                                remaining = state.edge_registry.count(),
                                "edge registry GC pruned stale edges"
                            );
                        }
                    }
                    _ = shutdown_rx.recv() => break,
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
            connections_rejected: self
                .state
                .metrics
                .connections_rejected
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
            cache_capture_oversize: self
                .state
                .metrics
                .cache_capture_oversize
                .load(Ordering::Relaxed),
            admission_waited: self.state.metrics.admission_waited.load(Ordering::Relaxed),
            admission_timeouts: self
                .state
                .metrics
                .admission_timeouts
                .load(Ordering::Relaxed),
            reconnect_attempts: self
                .state
                .metrics
                .reconnect_attempts
                .load(Ordering::Relaxed),
            backend_capacity_waits: self
                .state
                .metrics
                .backend_capacity_waits
                .load(Ordering::Relaxed),
            backend_capacity_refusals: self
                .state
                .metrics
                .backend_capacity_refusals
                .load(Ordering::Relaxed),
            journal_committed: self.state.metrics.journal_committed.load(Ordering::Relaxed),
            journal_rolled_back: self
                .state
                .metrics
                .journal_rolled_back
                .load(Ordering::Relaxed),
            journal_statements: self
                .state
                .metrics
                .journal_statements
                .load(Ordering::Relaxed),
            tr: self.state.metrics.tr.snapshot(),
        }
    }
}

#[cfg(test)]
mod tests;
