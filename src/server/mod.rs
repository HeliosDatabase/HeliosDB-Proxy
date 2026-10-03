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

    /// Forward a simple-query (`Query`) message and stream its response back
    /// to the client frame-by-frame, ending at ReadyForQuery. Picks (and, if
    /// needed, opens) the target node's connection from the per-session
    /// cache. Returns `(Some(node_used), bytes)` — `None` node means the
    /// request was short-circuited (plugin block) without touching a backend.
    ///
    /// On a backend fault the broken connection is dropped, the node is
    /// demoted, `fault` is filled with the failed node and whether the
    /// statement had already been delivered, and `Err` is returned so the
    /// caller can run the `tr_mode` recovery. A client-side error leaves
    /// `fault` untouched.
    #[allow(clippy::too_many_arguments)]
    async fn forward_simple_query(
        client: &mut ClientStream,
        msg: &Message,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        fault: &mut Option<BackendFault>,
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

        // Lazily-memoized lexical classification: every cheap fact this
        // forward path may need about the statement (write? session-dirtying?
        // cacheable read? multi-statement?) is derived AT MOST ONCE here,
        // instead of each gate re-walking the same SQL string — and only if
        // the gate that needs it is actually enabled, so the default
        // configuration keeps paying for exactly one classification (the write
        // check below, which every route decision needs). Rebuilt further down
        // only if a hint strip / rewrite / tenant transform replaces the text.
        let mut facts = StmtFacts::of_query(msg);
        let default_is_write = facts.is_write();
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
        if rewritten_msg.is_some() {
            // The recorded client text is not what executes — the enclosing
            // transaction must not be blindly replayed after a failover.
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
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
        if tenant_msg.is_some() {
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "multi-tenancy")]
        let forward_msg = tenant_msg.as_ref().unwrap_or(forward_msg);

        // `forward_msg` is now final. The hint strip / rewrite / tenant
        // transforms above may each have replaced the SQL, and facts only
        // describe the text they were derived from — so rebuild them (which
        // clears every memo cell) when any of them fired. An untouched query
        // (the common path) keeps the value built from the original message,
        // and rebuilding classifies nothing by itself. The block is gated on
        // the features that read facts below, so a build with none of them
        // carries no dead assignment.
        #[cfg(any(
            feature = "pool-modes",
            feature = "query-cache",
            feature = "edge-proxy"
        ))]
        {
            let sql_changed = stripped_msg.is_some();
            #[cfg(feature = "query-rewriting")]
            let sql_changed = sql_changed || rewritten_msg.is_some();
            #[cfg(feature = "multi-tenancy")]
            let sql_changed = sql_changed || tenant_msg.is_some();
            if sql_changed {
                facts = StmtFacts::of_query(forward_msg);
            }
        }

        // Edge cache: independent of the query-cache below — when both are
        // enabled the edge lookup runs first, so an edge hit returns before
        // the query-cache is consulted. On a miss the read's stamp/epoch is
        // taken BEFORE forwarding, so the store gate can reject the store if
        // an invalidation lands while the read is in flight.
        //
        // Session-state stickiness (F2): the shared cache cannot model
        // session-local execution context (SET search_path / SET ROLE / RLS
        // GUCs / temp objects). Any statement that leaves such state makes
        // the session permanently ineligible for edge lookup AND store —
        // conservative, but the alternative is cross-session wrong rows.
        #[cfg(feature = "edge-proxy")]
        if config.edge.enabled
            && !session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
            && facts.leaves_session_state()
        {
            session
                .edge_ineligible
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "edge-proxy")]
        let edge_ctx: Option<(crate::edge::CacheKey, Vec<String>, u64, u64)> = if is_write
            || !config.edge.enabled
            || session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            None
        } else {
            let sql = facts.sql();
            // Same gate as the query-cache: a plain deterministic SELECT, and
            // never mid-transaction (visibility would be wrong).
            if facts.is_cacheable_read()
                && !session
                    .in_transaction
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                // Tenant identity + result-affecting startup params
                // (TimeZone, options=-c..., extra_float_digits, ...) all
                // partition the key: identical SQL under a different
                // session environment must not share a slot.
                let (database, user, session_vars) = {
                    let vars = session.variables.read().await;
                    let database = vars
                        .get("database")
                        .cloned()
                        .unwrap_or_else(|| "default".to_string());
                    let user = vars.get("user").cloned().unwrap_or_default();
                    let mut rest: Vec<(String, String)> = vars
                        .iter()
                        .filter(|(k, _)| k.as_str() != "database" && k.as_str() != "user")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    rest.sort();
                    (database, user, rest)
                };
                let fp = crate::edge::fingerprint::analyze(sql, &database, &user, &session_vars);
                // A read with no extractable tables could never be dropped by
                // a table-targeted invalidation — never cache it (coherence
                // rule).
                if fp.tables.is_empty() {
                    None
                } else {
                    let key = crate::edge::CacheKey {
                        fingerprint: fp.fingerprint,
                        params_hash: fp.params_hash,
                        database,
                        user,
                    };
                    if let Some(entry) = state.edge_cache.get(&key) {
                        tracing::debug!(target: "helios::edge", "edge cache hit");
                        client.write_all(&entry.response_bytes).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, entry.response_bytes.len() as u64));
                    }
                    // Miss: stamp the read before it reaches a backend, in
                    // the INVALIDATION clock's domain. Home role: mint from
                    // the local counter (the same clock that versions
                    // writes). Edge role: writes are versioned by the HOME,
                    // so stamp with the last observed home version — any
                    // later home write mints a strictly greater version and
                    // always sweeps this entry; the store race is closed by
                    // the invalidation-epoch snapshot instead of the hwm.
                    let (read_version, inval_epoch) =
                        if config.edge.role == crate::edge::EdgeRole::Edge {
                            (
                                state.edge_cache.observed_home_version(),
                                state.edge_cache.invalidation_epoch(),
                            )
                        } else {
                            (state.edge_cache.next_version(), 0)
                        };
                    Some((key, fp.tables, read_version, inval_epoch))
                }
            } else {
                None
            }
        };

        // Query cache: on a cacheable read, a hit is served from cache with no
        // backend round-trip; on a miss we keep the context to store the result.
        // The lookup's hint parse + normalization are carried over to the
        // store below (`put_prepared`), so a miss+store normalizes the SQL
        // once instead of twice.
        #[cfg(feature = "query-cache")]
        let cache_ctx: Option<(crate::cache::CacheContext, crate::cache::QueryPrep)> = if is_write {
            None
        } else if let Some(qc) = state.query_cache.as_ref() {
            let sql = facts.sql();
            match Self::cacheable_read_ctx(session, facts.is_cacheable_read()).await {
                Some(ctx) => {
                    let (lookup, prep) = qc.get_with_prep(sql, &ctx).await;
                    if let crate::cache::CacheLookup::Hit { result, level } = lookup {
                        tracing::debug!(target: "helios::cache", level = %level, "cache hit");
                        client.write_all(&result.data).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, result.data.len() as u64));
                    }
                    Some((ctx, prep))
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
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Conditional-reset bookkeeping: if this statement is not provably
        // session-neutral, mark the connection dirty so it is fully reset (not
        // clean-skipped) when parked. Still evaluated only when the
        // optimisation is enabled and the connection is not already dirty (one
        // O(len) scan at most, until the first dirtying statement) — the facts
        // memo just means the edge gate above, when it is also on, shares that
        // one scan instead of doing its own.
        #[cfg(feature = "pool-modes")]
        if config.pool_mode.skip_clean_reset && !backend.dirty && facts.leaves_session_state() {
            backend.dirty = true;
        }

        let backend_err = match tokio::time::timeout(
            state.limits.backend_write_timeout,
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
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }

        // Cacheable read miss (query-cache and/or edge cache): capture the
        // response frames ONCE and store them so a later identical read is
        // served from cache without a backend hit. Both caches share the one
        // captured buffer — never capture twice.
        // TR-07: register the statement the backend is now executing so the
        // relay collects its outcome (writes, COPY and transaction control
        // only — reads register nothing and arm nothing).
        if config.tr_enabled || Self::cache_needs_capture(state) {
            if let Some(sql) = crate::protocol::query_text(&forward_msg.payload) {
                Self::journal_register_simple(session, sql);
            }
        }

        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            #[cfg(feature = "query-cache")]
            let qc_wants = cache_ctx.is_some() && state.query_cache.is_some();
            #[cfg(not(feature = "query-cache"))]
            let qc_wants = false;
            #[cfg(feature = "edge-proxy")]
            let edge_wants = edge_ctx.is_some();
            #[cfg(not(feature = "edge-proxy"))]
            let edge_wants = false;
            if qc_wants || edge_wants {
                return match Self::stream_until_ready_capture(
                    client,
                    &mut backend.stream,
                    session,
                    state.limits.relay(),
                    config.cache.max_cacheable_response_bytes,
                    &state.metrics,
                )
                .await
                {
                    Ok((sent, captured, cacheable, rows)) => {
                        #[cfg(not(feature = "query-cache"))]
                        let _ = rows; // row count only feeds the query-cache
                        #[cfg(feature = "circuit-breaker")]
                        Self::circuit_record(state, &target, true, "");
                        // One zero-copy conversion; both caches then share the
                        // refcounted buffer (no per-miss memcpy, F20).
                        let body = bytes::Bytes::from(captured);
                        // Edge store. The race-checked insert variants re-verify
                        // the gate UNDER the map lock: a concurrent invalidation
                        // that completed between the read and this store must
                        // reject it (read-after-invalidate TOCTOU, F18). Home
                        // role gates on the version hwm; edge role on the
                        // invalidation-epoch snapshot taken before forwarding.
                        #[cfg(feature = "edge-proxy")]
                        if let Some((key, tables, read_version, inval_epoch)) = edge_ctx {
                            if cacheable && !body.is_empty() {
                                let entry = crate::edge::CacheEntry {
                                    version: read_version,
                                    response_bytes: body.clone(),
                                    tables,
                                    expires_at: std::time::Instant::now()
                                        + config.edge.default_ttl(),
                                };
                                let stored = if config.edge.role == crate::edge::EdgeRole::Edge {
                                    state.edge_cache.insert_if_epoch(key, entry, inval_epoch)
                                } else {
                                    state.edge_cache.insert_if_fresh(key, entry)
                                };
                                if !stored {
                                    tracing::debug!(
                                        target: "helios::edge",
                                        "edge store skipped — invalidation raced the read"
                                    );
                                }
                            }
                        }
                        #[cfg(feature = "query-cache")]
                        if let (Some((ctx, prep)), Some(qc)) =
                            (cache_ctx.as_ref(), state.query_cache.as_ref())
                        {
                            if cacheable && !body.is_empty() {
                                let sql =
                                    crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
                                qc.put_prepared(
                                    sql,
                                    ctx,
                                    prep,
                                    body.clone(),
                                    rows,
                                    std::time::Duration::ZERO,
                                )
                                .await;
                            }
                        }
                        let _ = body;
                        // TR-07: the capture relay has no journal handle; reconcile
                        // the transaction state from the status it recorded.
                        Self::journal_observe_status(session, state).await;
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
                        BackendFault::set_response(fault, &target, &e);
                        Err(e.error)
                    }
                };
            }
        }

        match Self::stream_until_ready(client, &mut backend.stream, session, state).await {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                // Query-cache invalidation for this write (C-02) already ran
                // from the journal capture's ops in `stream_until_ready`, on
                // both protocols, before the ReadyForQuery reached the client.
                // Edge cache: drop local entries for the touched tables and
                // (home role only) fan the invalidation out to every
                // registered edge over SSE. Also fires for SELECT-leading
                // multi-statement strings (the trailing write would otherwise
                // invalidate nothing); bare transaction-control statements
                // (BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK) change no rows and
                // are exempted — but COMMIT and SET keep their conservative
                // full flush. A `COPY ... FROM` is deferred to its CopyDone
                // drain (rows become visible then).
                #[cfg(feature = "edge-proxy")]
                if config.edge.enabled {
                    let sql = facts.sql();
                    if Self::edge_write_needs_invalidation(
                        is_write,
                        sql,
                        facts.has_interior_semicolon(),
                    ) {
                        // `tables_only` skips the fingerprint/params-hash work
                        // (discarded here) — a bulk INSERT must not pay full-
                        // buffer regex rewrites on the forward path. An EMPTY
                        // table set invalidates everything at or below the
                        // version — an unparseable write must not leave stale
                        // entries behind (coherence rule).
                        let tables = crate::edge::fingerprint::tables_only(sql);
                        if session
                            .copy_in_progress
                            .load(std::sync::atomic::Ordering::Relaxed)
                        {
                            *session
                                .pending_edge_copy_tables
                                .lock()
                                .unwrap_or_else(|e| e.into_inner()) = Some(tables);
                        } else {
                            Self::edge_invalidate_write(state, config, tables).await;
                        }
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
                BackendFault::set_response(fault, &target, &e);
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
                Err(e.error)
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
    ///
    /// `fault` is filled on a backend fault exactly like `forward_simple_query`.
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
        unnamed: Option<&(bytes::Bytes, bytes::Bytes)>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        fault: &mut Option<BackendFault>,
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
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
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
            match Self::reprepare_statement(
                &mut backend.stream,
                parse_bytes,
                state.limits.reprepare_timeout,
                state.limits.max_backend_frame_bytes,
            )
            .await
            {
                Ok(()) => {
                    backend.prepared.insert(name.clone());
                }
                Err(e) => {
                    conns.remove(&target);
                    // A socket-level failure here is a backend fault (the batch
                    // was never sent); a backend *rejecting* the re-prepare is
                    // a protocol-level problem and is propagated as before.
                    if matches!(e, ProxyError::Network(_)) {
                        Self::record_backend_failure(state, &target, &e.to_string());
                        BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
                    }
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
        if let Some((parse_msg, sig)) = unnamed {
            if backend.unnamed_sig.as_deref() == Some(&sig[..]) {
                inject_parse_complete = true;
            } else {
                if let Err(e) = tokio::time::timeout(
                    state.limits.backend_write_timeout,
                    backend.stream.write_all(parse_msg),
                )
                .await
                .map_err(|_| ProxyError::Network("Backend write timeout".to_string()))
                .and_then(|r| {
                    r.map_err(|e| ProxyError::Network(format!("Backend write error: {}", e)))
                }) {
                    conns.remove(&target);
                    Self::record_backend_failure(state, &target, &e.to_string());
                    BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
                    return Err(e);
                }
                new_unnamed_sig = Some(sig.clone());
            }
        }

        if let Err((e, phase)) = Self::tr_write_batch(
            &mut backend.stream,
            batch,
            state.limits.backend_write_timeout,
        )
        .await
        {
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, phase, &e);
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

        // TR-07: register the batch's Parse/Bind/Execute/Close (and the held
        // unnamed Parse the backend already holds) so the relay collects each
        // Execute's outcome with its bound parameter values.
        if config.tr_enabled || Self::cache_needs_capture(state) {
            Self::journal_register_batch(session, batch, unnamed.map(|(m, _)| &m[..]));
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
            Err(mut e) => {
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                e.progress.bytes += injected;
                BackendFault::set_response(fault, &target, &e);
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
                Err(e.error)
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
        reprepare_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<()> {
        tokio::time::timeout(reprepare_timeout, backend.write_all(parse_bytes))
            .await
            .map_err(|_| ProxyError::Network("re-prepare write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare write error: {}", e)))?;
        // Flush: 'H' + length 4.
        tokio::time::timeout(reprepare_timeout, backend.write_all(&[b'H', 0, 0, 0, 4]))
            .await
            .map_err(|_| ProxyError::Network("re-prepare flush timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare flush error: {}", e)))?;
        let mtype = tokio::time::timeout(
            reprepare_timeout,
            Self::read_one_frame_type(backend, max_frame_bytes),
        )
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
    async fn read_one_frame_type<S: AsyncReadExt + Unpin>(
        backend: &mut S,
        max_frame_bytes: usize,
    ) -> Result<u8> {
        let mut header = [0u8; 5];
        backend
            .read_exact(&mut header)
            .await
            .map_err(|e| ProxyError::Network(format!("re-prepare read error: {}", e)))?;
        // H-07: validate before trusting the length, and never allocate the
        // advertised body size — discard it through a fixed scratch buffer.
        let len = backend_frame_len(&header, max_frame_bytes)?.expect("5 header bytes were read");
        let mut remaining = len - 4;
        let mut scratch = [0u8; 16 * 1024];
        while remaining > 0 {
            let n = remaining.min(scratch.len());
            backend
                .read_exact(&mut scratch[..n])
                .await
                .map_err(|e| ProxyError::Network(format!("re-prepare body read error: {}", e)))?;
            remaining -= n;
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
    ) -> std::result::Result<u64, ResponseFailure> {
        let client_write_timeout = state.limits.client_write_timeout;
        let backend_read_timeout = state.limits.backend_read_timeout;
        let response_deadline = state
            .limits
            .backend_response_timeout
            .map(|d| tokio::time::Instant::now() + d);
        // TR-06: observe only inside an explicit transaction under a recording
        // mode; everything else pays no hashing.
        let mut observation = (session.in_transaction.load(Ordering::Relaxed)
            && matches!(session.tr_mode, TrMode::Select | TrMode::Transaction))
        .then(|| Observation::new(state.limits.tr_max_observation_bytes));
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut had_error = false;
        let mut command_complete = false;
        // TR-07: per-statement completions and the first error are collected
        // only when the forward path registered something for this cycle.
        let capture_armed = session.journal_armed.load(Ordering::Relaxed);
        let mut completions: Vec<crate::journal_capture::Completion> = Vec::new();
        let mut first_error: Option<(String, String)> = None;

        let response = async {
            loop {
                // Walk complete frames in `buf`, stopping at a boundary frame.
                let mut consumed = 0usize;
                let mut ready_status: Option<u8> = None;
                let mut yield_for_copy = false;
                loop {
                    let rem = &buf[consumed..];
                    // H-07: a malformed header (len < 4, or above the budget) is
                    // an error NOW, not "wait for more bytes" — no byte count can
                    // make it valid and the accumulator must not chase it.
                    let Some(len) = backend_frame_len(rem, state.limits.max_backend_frame_bytes)?
                    else {
                        break;
                    };
                    if rem.len() < len + 1 {
                        break; // incomplete — need more bytes
                    }
                    let frame_total = len + 1;
                    let mtype = rem[0];
                    consumed += frame_total;
                    if mtype == b'E' {
                        had_error = true;
                    }
                    command_complete |= mtype == b'C';
                    if let Some(obs) = observation.as_mut() {
                        obs.note(&rem[..frame_total]);
                    }
                    if capture_armed {
                        Self::journal_note_frame(
                            &rem[..frame_total],
                            &mut completions,
                            &mut first_error,
                        );
                    }
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

                // C-02: with the query cache on, settle the cycle's capture
                // ops — and move the cache generations of what it wrote —
                // BEFORE the ReadyForQuery that acknowledges it reaches the
                // client, so no read that starts after the acknowledgement is
                // served a pre-commit entry. Without a cache nothing depends
                // on that ordering, so the observation stays after the write,
                // off the client's round trip (as before C-02).
                let mut outcome =
                    ready_status.map(|status| crate::journal_capture::ResponseOutcome {
                        status,
                        completions: std::mem::take(&mut completions),
                        error: first_error.take(),
                    });
                let early = if Self::cache_needs_capture(state) {
                    outcome
                        .take()
                        .and_then(|o| Self::journal_observe_sync(session, state, o))
                } else {
                    None
                };

                if consumed > 0 {
                    tokio::time::timeout(client_write_timeout, client.write_all(&buf[..consumed]))
                        .await
                        .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    sent += consumed as u64;
                    let _ = buf.split_to(consumed);
                }

                if let Some(status) = ready_status {
                    Self::note_ready_for_query(session, status, had_error);
                    Self::note_observation(session, observation.as_ref());
                    let observed = early.or_else(|| {
                        outcome
                            .take()
                            .and_then(|o| Self::journal_observe_sync(session, state, o))
                    });
                    if let Some(cycle) = observed {
                        Self::journal_observe_finish(session, state, cycle).await;
                    }
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
                let n = tokio::time::timeout(
                    read_budget(backend_read_timeout, response_deadline)?,
                    backend.read_buf(&mut buf),
                )
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
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: had_error || command_complete,
                raw: false,
            },
        })
    }

    /// Like `stream_until_ready` but also captures the full response bytes for
    /// caching (query-cache and/or edge cache — one shared capture).
    /// Returns `(bytes_sent, captured, cacheable, row_count)`.
    /// `cacheable` is false if the response carried an `ErrorResponse`, ended in
    /// a non-idle transaction status, yielded for COPY, or contained an
    /// asynchronous backend frame — NotificationResponse ('A'), NoticeResponse
    /// ('N'), or ParameterStatus ('S'). Async frames belong to THIS session's
    /// moment in time (a LISTENing session's notification, a GUC change);
    /// replaying them to every later hitter would leak the notification
    /// payload cross-session and desynchronize hitters' parameter state. The
    /// frames are still forwarded to the live requester — only the store is
    /// suppressed.
    ///
    /// `max_capture_bytes` ([cache] `max_cacheable_response_bytes`) bounds the
    /// capture buffer: the capture is a *transient* held per concurrent
    /// session ON TOP of the bytes already streamed out, so an unbounded one
    /// let a single `SELECT * FROM big_table` pin the whole result set in the
    /// proxy. The moment appending would cross the bound the buffer is dropped
    /// (memory freed immediately), nothing further is captured, and the
    /// response is reported non-cacheable. Forwarding to the client is
    /// untouched — the client still receives every byte, byte-for-byte.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    async fn stream_until_ready_capture(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        relay: RelayLimits,
        max_capture_bytes: usize,
        metrics: &ServerMetrics,
    ) -> std::result::Result<(u64, Vec<u8>, bool, usize), ResponseFailure> {
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut captured: Vec<u8> = Vec::with_capacity(4096);
        let mut had_error = false;
        let mut command_complete = false;
        let mut saw_async = false;
        let mut row_count: usize = 0;
        // Set once the response outgrows `max_capture_bytes`; `captured` is
        // then empty and stays empty for the rest of the response.
        let mut oversize = false;
        let response_deadline = relay
            .response_timeout
            .map(|d| tokio::time::Instant::now() + d);
        let mut observation = (session.in_transaction.load(Ordering::Relaxed)
            && matches!(session.tr_mode, TrMode::Select | TrMode::Transaction))
        .then(|| Observation::new(relay.observation_bytes));

        let response = async {
            loop {
                let mut consumed = 0usize;
                let mut ready_status: Option<u8> = None;
                let mut yield_for_copy = false;
                loop {
                    let rem = &buf[consumed..];
                    let Some(len) = backend_frame_len(rem, relay.max_frame_bytes)? else {
                        break;
                    };
                    if rem.len() < len + 1 {
                        break;
                    }
                    let frame_total = len + 1;
                    let mtype = rem[0];
                    if mtype == b'E' {
                        had_error = true;
                    }
                    command_complete |= mtype == b'C';
                    if let Some(obs) = observation.as_mut() {
                        obs.note(&rem[..frame_total]);
                    }
                    // Async backend frames (backend 'S' is unambiguous here —
                    // PortalSuspended is lowercase 's').
                    if mtype == b'A' || mtype == b'N' || mtype == b'S' {
                        saw_async = true;
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
                        relay.client_write_timeout,
                        client.write_all(&buf[..consumed]),
                    )
                    .await
                    .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                    .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    if !oversize {
                        if captured.len().saturating_add(consumed) > max_capture_bytes {
                            oversize = true;
                            // Free the transient NOW (`clear` alone keeps the
                            // allocation alive for the rest of the response).
                            captured = Vec::new();
                            metrics
                                .cache_capture_oversize
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                target: "helios::cache",
                                limit = max_capture_bytes,
                                "response exceeds cache.max_cacheable_response_bytes — \
                                 capture abandoned, response not cached"
                            );
                        } else {
                            captured.extend_from_slice(&buf[..consumed]);
                        }
                    }
                    sent += consumed as u64;
                    let _ = buf.split_to(consumed);
                }

                if let Some(status) = ready_status {
                    Self::note_ready_for_query(session, status, had_error);
                    Self::note_observation(session, observation.as_ref());
                    let cacheable = !had_error && status == b'I' && !saw_async && !oversize;
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
                let n = tokio::time::timeout(
                    read_budget(relay.backend_read_timeout, response_deadline)?,
                    backend.read_buf(&mut buf),
                )
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
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: had_error || command_complete,
                raw: false,
            },
        })
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
    ) -> std::result::Result<u64, ResponseFailure> {
        let _ = session;
        // Reused across every read of this call via `try_read_buf`, which
        // writes straight into the buffer's spare capacity from the read
        // syscall — unlike `vec![0u8; 16384]`, nothing here is
        // zero-initialized before use.
        let mut read_buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let response = async {
            loop {
                read_buf.clear();
                match backend.try_read_buf(&mut read_buf) {
                    Ok(0) => {
                        return Err(ProxyError::Connection(
                            "Backend closed mid-flush".to_string(),
                        ))
                    }
                    Ok(n) => {
                        tokio::time::timeout(
                            state.limits.client_write_timeout,
                            client.write_all(&read_buf[..n]),
                        )
                        .await
                        .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                        sent += n as u64;
                    }
                    // Nothing more instantly available — the backend watch delivers
                    // any remaining Flush output as it arrives.
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(sent),
                    Err(e) => {
                        return Err(ProxyError::Network(format!("Backend read error: {}", e)))
                    }
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: false,
                raw: sent > 0,
            },
        })
    }

    /// Append `name` to a per-cycle extended-batch tracker at most once,
    /// bounded by the session's named-statement cap (O-05). A client that
    /// never sends Sync cannot grow `batch_refs`/`batch_defines`/`batch_closes`
    /// without bound, and the re-prepare filter stays linear in the cap.
    fn remember_batch_name(list: &mut Vec<String>, name: &str, cap: usize) {
        if list.len() >= cap || list.iter().any(|n| n == name) {
            return;
        }
        list.push(name.to_string());
    }

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

    /// Should this successfully-executed simple-query string trigger an edge
    /// invalidation?
    ///
    /// - Bare transaction control (BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK)
    ///   changes no rows: exempt, or every ORM transaction would full-flush
    ///   the whole fleet twice per request. COMMIT is deliberately NOT
    ///   exempt (its flush closes the invalidate-at-statement vs
    ///   commit-visibility window for in-transaction writes), nor is SET
    ///   (the flush is the interim mitigation for GUC-sensitive results).
    /// - A multi-statement string (interior `;`) may hide a trailing write
    ///   behind a read-classified lead — always invalidate (the fingerprint
    ///   extracts tables from every sub-statement; over-invalidation only).
    /// - `COPY ... FROM` loads rows but is not classified `is_write` (it
    ///   must not be re-routed); catch it here for invalidation.
    ///
    /// `multi_stmt` is `StmtFacts::has_interior_semicolon` for the same SQL —
    /// passed in rather than re-scanned (`stmt_has_interior_semicolon` is the
    /// single definition of that fact).
    #[cfg(feature = "edge-proxy")]
    fn edge_write_needs_invalidation(is_write: bool, sql: &str, multi_stmt: bool) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if !multi_stmt
            && (starts_with_ci(core, "BEGIN")
                || starts_with_ci(core, "START")
                || starts_with_ci(core, "SAVEPOINT")
                || starts_with_ci(core, "RELEASE")
                || starts_with_ci(core, "ROLLBACK"))
        {
            return false;
        }
        is_write
            || multi_stmt
            || Self::is_edge_copy_write_sql(core)
            || Self::is_edge_procedural_sql(core)
            || Self::is_edge_txn_end_sql(core)
    }

    /// Does the simple-query string carry more than one statement? A `;`
    /// before the optional trailing one means a leading-keyword check cannot
    /// vouch for what follows. (A `;` inside a string literal also trips this —
    /// conservative, and only ever costs an extra invalidation/reset.)
    #[cfg(feature = "edge-proxy")]
    fn stmt_has_interior_semicolon(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        core.contains(';')
    }

    /// `COPY ... FROM ...` (STDIN or file) loads rows. Word-boundary FROM so
    /// `COPY t TO ...` stays a read; a `COPY (SELECT ... FROM t) TO ...`
    /// false-positive only over-invalidates — safe.
    #[cfg(feature = "edge-proxy")]
    fn is_edge_copy_write_sql(sql: &str) -> bool {
        crate::protocol::starts_with_ci(sql.trim_start(), "COPY")
            && Self::contains_word_ci(sql, "from")
    }

    /// Which of a batch's Closed statement names may have their edge
    /// invalidation metadata pruned at a Sync boundary. A name Closed and then
    /// re-Parsed in the SAME batch must keep its FRESH metadata (dropping it
    /// would silently disable invalidation for the live re-prepared statement —
    /// the Npgsql statement-replacement pattern), so it is excluded. Pruning is
    /// additionally gated on the Sync by the caller, so a Close seen at an
    /// earlier Flush keeps its metadata alive for the terminating Sync's
    /// invalidation hook. Regression guard for the G1 finding.
    #[cfg(feature = "edge-proxy")]
    fn edge_meta_prunable<'a>(closes: &'a [String], defines: &[String]) -> Vec<&'a str> {
        closes
            .iter()
            .filter(|n| !defines.contains(n))
            .map(String::as_str)
            .collect()
    }

    /// SQL-level statements whose written table set cannot be attributed
    /// statically — `EXECUTE` (runs a prepared plan), `CALL` (a procedure),
    /// `DO` (an anonymous block, often dynamic SQL). Treated as an
    /// invalidate-everything wildcard write. Rare on the wire (no mainstream
    /// driver emits them for data changes), so the full flush is cheap in
    /// practice and over-invalidation is always safe.
    #[cfg(feature = "edge-proxy")]
    fn is_edge_procedural_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        starts_with_ci(t, "EXECUTE") || starts_with_ci(t, "CALL") || starts_with_ci(t, "DO")
    }

    /// Transaction-ending statements: `COMMIT`/`END` and their variants
    /// (`COMMIT WORK`/`AND CHAIN`/`PREPARED`, `END TRANSACTION`). They make a
    /// transaction's in-flight writes visible, so — like the simple-path
    /// COMMIT — they trigger the conservative wildcard flush that closes the
    /// invalidate-at-statement vs commit-visibility window. `BEGIN`/`START`/
    /// `SAVEPOINT`/`RELEASE`/`ROLLBACK` are excluded (they make nothing newly
    /// visible). `END` is a COMMIT synonym here; a rare non-transaction
    /// statement that happens to start with `END` only over-invalidates.
    #[cfg(feature = "edge-proxy")]
    fn is_edge_txn_end_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        starts_with_ci(t, "COMMIT") || starts_with_ci(t, "END")
    }

    /// Extended-protocol statement classifier for edge invalidation: does
    /// this Parse'd SQL modify table data when executed? Deliberately NOT
    /// `is_write_query` — that routing classifier counts BEGIN/COMMIT/SET as
    /// writes, and their empty table set would full-flush the fleet on every
    /// transaction commit. WITH-prefixed statements are checked for
    /// data-modifying CTE verbs by word (false positives over-invalidate
    /// only).
    #[cfg(feature = "edge-proxy")]
    fn is_edge_dml_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        if starts_with_ci(t, "INSERT")
            || starts_with_ci(t, "UPDATE")
            || starts_with_ci(t, "DELETE")
            || starts_with_ci(t, "MERGE")
            || starts_with_ci(t, "CREATE")
            || starts_with_ci(t, "DROP")
            || starts_with_ci(t, "ALTER")
            || starts_with_ci(t, "TRUNCATE")
            || starts_with_ci(t, "GRANT")
            || starts_with_ci(t, "REVOKE")
        {
            return true;
        }
        if starts_with_ci(t, "COPY") {
            return Self::contains_word_ci(t, "from");
        }
        if starts_with_ci(t, "WITH") {
            return Self::contains_word_ci(t, "insert")
                || Self::contains_word_ci(t, "update")
                || Self::contains_word_ci(t, "delete")
                || Self::contains_word_ci(t, "merge");
        }
        false
    }

    /// Union the invalidation table set for an extended-protocol batch from
    /// the per-statement metadata memoized at Parse time. Returns `None`
    /// when the batch references no DML statement; `Some(vec![])` (the
    /// invalidate-everything wildcard) when any referenced DML has an
    /// unattributable table set.
    #[cfg(feature = "edge-proxy")]
    fn edge_extended_batch_tables(
        refs: &[String],
        bound_unnamed: bool,
        named_meta: &HashMap<String, Option<Vec<String>>>,
        unnamed_meta: &Option<Vec<String>>,
    ) -> Option<Vec<String>> {
        let mut any_dml = false;
        let mut wipe_all = false;
        let mut union: Vec<String> = Vec::new();
        {
            let mut consider = |meta: &Option<Vec<String>>| {
                if let Some(tables) = meta {
                    any_dml = true;
                    if tables.is_empty() {
                        wipe_all = true;
                    } else {
                        for t in tables {
                            if !union.contains(t) {
                                union.push(t.clone());
                            }
                        }
                    }
                }
            };
            for name in refs {
                if let Some(meta) = named_meta.get(name) {
                    consider(meta);
                }
            }
            if bound_unnamed {
                consider(unnamed_meta);
            }
        }
        if !any_dml {
            None
        } else if wipe_all {
            Some(Vec::new())
        } else {
            Some(union)
        }
    }

    /// Version-stamp a completed write, drop matching local entries, and
    /// (home role) fan the invalidation out over SSE. An edge never
    /// broadcasts — the home versions writes, so the edge sweeps in the
    /// observed-home domain and lets the home's own event follow.
    #[cfg(feature = "edge-proxy")]
    async fn edge_invalidate_write(
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        tables: Vec<String>,
    ) {
        if config.edge.role == crate::edge::EdgeRole::Home {
            let version = state.edge_cache.next_version();
            let dropped = state.edge_cache.invalidate(version, &tables);
            let (notified, pruned) = state
                .edge_registry
                .broadcast(crate::edge::InvalidationEvent {
                    seq: 0,
                    up_to_version: version,
                    tables,
                    committed_at: chrono::Utc::now().to_rfc3339(),
                    epoch: state.edge_cache.epoch(),
                })
                .await;
            tracing::debug!(
                target: "helios::edge",
                version,
                dropped,
                notified,
                pruned,
                "write invalidation broadcast to edges"
            );
        } else {
            // Edge-local sweep in the observed-home domain: every locally
            // cached entry is stamped at or below the observed version, so
            // this drops all entries for the touched tables. It also bumps
            // the invalidation epoch, rejecting in-flight read stores that
            // raced this write.
            let version = state.edge_cache.observed_home_version();
            let dropped = state.edge_cache.invalidate(version, &tables);
            tracing::debug!(
                target: "helios::edge",
                version,
                dropped,
                "write invalidated local edge cache (edge role — home broadcasts)"
            );
        }
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
    ///
    /// Also reused by the edge cache as its sticky session-eligibility
    /// gate: a session that leaves session state (GUCs, SET ROLE, temp
    /// objects) no longer matches the shared cache's key model and is
    /// permanently excluded from edge lookup/store.
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    fn stmt_leaves_session_state(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
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
        // Same one-pass-lowercase trick as `is_cacheable_read_sql`: rather
        // than 4 separate case-insensitive windowed scans over `core`,
        // lowercase it once and use plain `str::contains`.
        const DIRTY_TOKENS: [&str; 4] = ["set_config", "advisory", "nextval", "setval"];
        let lower = core.to_ascii_lowercase();
        DIRTY_TOKENS.iter().any(|tok| lower.contains(tok))
    }

    /// Case-insensitive whole-word (ASCII identifier-boundary) search — a match
    /// requires the token to be bounded by a non-`[A-Za-z0-9_]` char (or the
    /// string edge) on both sides, so a real SQL keyword like `INTO` is caught
    /// regardless of surrounding whitespace while an identifier substring
    /// (`into_total`) is not. Always compiled: the in-session TR statement
    /// classifier (`tr_classify`) uses it on every build.
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
    /// keying dimension, memoizing it on the session.
    ///
    /// Every input is fixed for the life of a connection: `key_by` comes from
    /// the config snapshot this connection was accepted under, the client
    /// address never changes, and `user`/`database` are set once from the
    /// startup packet. So the key (and its rendered metrics string) is built
    /// once and every later query borrows it — no `LimiterKey` allocation, no
    /// `variables` read lock, and no `format!` on the per-query gate.
    ///
    /// The one case that is *not* memoized is a key derived from a startup
    /// parameter that is not present yet: that would freeze a placeholder for
    /// the rest of the session, so it is recomputed (exactly as before) until
    /// the parameter appears.
    #[cfg(feature = "rate-limiting")]
    async fn rate_limit_key<'a>(
        session: &'a Arc<ClientSession>,
        config: &ProxyConfig,
    ) -> std::borrow::Cow<'a, crate::rate_limit::CachedLimiterKey> {
        use crate::config::RateLimitKeyBy;
        use crate::rate_limit::{CachedLimiterKey, LimiterKey};
        use std::borrow::Cow;

        if let Some(cached) = session.rate_limit_key.get() {
            return Cow::Borrowed(cached);
        }

        // `stable` is false only when the key had to fall back to a default
        // because the startup parameter it keys on is not populated yet.
        let (key, stable) = match config.rate_limit.key_by {
            RateLimitKeyBy::Global => (LimiterKey::Global, true),
            RateLimitKeyBy::ClientIp => (LimiterKey::ClientIp(session.client_addr.ip()), true),
            RateLimitKeyBy::Database => {
                let vars = session.variables.read().await;
                match vars.get("database") {
                    Some(db) => (LimiterKey::Database(db.clone()), true),
                    None => (LimiterKey::Database(String::new()), false),
                }
            }
            RateLimitKeyBy::User => {
                let vars = session.variables.read().await;
                match vars.get("user") {
                    Some(user) => (LimiterKey::User(user.clone()), true),
                    None => (LimiterKey::User(String::new()), false),
                }
            }
        };

        let resolved = CachedLimiterKey::new(key);
        if !stable {
            return Cow::Owned(resolved);
        }

        // A concurrent racer may win the init; either way the stored value is
        // the same key, so borrow whatever landed.
        Cow::Borrowed(session.rate_limit_key.get_or_init(|| resolved))
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
        let key = key.as_ref();
        match limiter.check_cached(key, 1) {
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
    fn lag_excludes_standby(
        lag_bytes: Option<u64>,
        max_lag_bytes: u64,
        require_known: bool,
    ) -> bool {
        match lag_bytes {
            Some(lag) => max_lag_bytes > 0 && lag > max_lag_bytes,
            // Unknown lag is allowed by default (historical behaviour); a
            // strict policy refuses it rather than treating `None` as fresh
            // (H-04).
            None => require_known,
        }
    }

    /// Pure predicate: is `sql` a plain, deterministic, SINGLE-statement
    /// SELECT safe to cache? (Not WITH/locking/volatile/multi-statement/
    /// SELECT INTO.) Transaction state is checked separately. Shared by the
    /// query-cache and edge-cache read gates.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    fn is_cacheable_read_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        if !starts_with_ci(t, "SELECT") {
            return false;
        }
        // Multi-statement simple-query strings (`SELECT ...; UPDATE ...`)
        // must never be cached: replaying the capture would fabricate the
        // trailing statements' results while executing nothing. One
        // trailing `;` is fine; a `;` inside a string literal only costs
        // cacheability — safe.
        let core = t.trim_end();
        let core = core.strip_suffix(';').map(str::trim_end).unwrap_or(core);
        if core.contains(';') {
            return false;
        }
        // `SELECT ... INTO t` CREATES a table (CREATE TABLE AS synonym);
        // replaying it from cache would silently skip the DDL. Word-boundary
        // match, so newline/tab-delimited INTO is caught while a column
        // named `into_x` is not (a literal containing the word merely skips
        // caching).
        if Self::contains_word_ci(core, "into") {
            return false;
        }
        // The remaining checks are all "does `t` contain one of these
        // needles, case-insensitively" — instead of the 13 separate
        // windowed case-insensitive scans (FOR UPDATE, FOR SHARE, 11
        // VOLATILE tokens) that used to each walk `t` byte-by-byte,
        // lowercase `t` ONCE into a scratch buffer and do plain `str::contains`
        // checks against it (Two-Way substring search — no SIMD, but a single
        // linear scan replacing 13 windowed case-insensitive ones is still a
        // clear win). ASCII-only lowercasing (`to_ascii_lowercase`) leaves any
        // non-ASCII bytes untouched, matching `contains_ci`'s byte-for-byte
        // semantics for non-ASCII input.
        let lower = t.to_ascii_lowercase();
        if lower.contains("for update") || lower.contains("for share") {
            return false;
        }
        // Non-deterministic or side-effectful reads must not be reused
        // (set_config emits ParameterStatus and mutates GUCs — replaying it
        // would suppress the side effect).
        const VOLATILE: [&str; 11] = [
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
            "set_config(",
        ];
        !VOLATILE.iter().any(|v| lower.contains(v))
    }

    /// Decide whether a read query is safe to serve from / store in the cache,
    /// and build its `CacheContext`. Returns `None` for anything not a plain,
    /// deterministic, non-transactional SELECT.
    ///
    /// `is_cacheable_read` is `StmtFacts::is_cacheable_read` for the statement
    /// (i.e. `is_cacheable_read_sql`, already computed once for this message).
    #[cfg(feature = "query-cache")]
    async fn cacheable_read_ctx(
        session: &Arc<ClientSession>,
        is_cacheable_read: bool,
    ) -> Option<crate::cache::CacheContext> {
        if !is_cacheable_read {
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

    // ---- TR-07 recovery-journal capture hooks --------------------------

    /// Register a simple-query string for capture; arms the relay when the
    /// statement's outcome matters (write, COPY, EXECUTE, transaction control).
    fn journal_register_simple(session: &ClientSession, sql: &str) {
        let armed = session
            .journal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .register_simple(sql);
        if armed {
            session.journal_armed.store(true, Ordering::Relaxed);
        }
    }

    /// Register every `Parse` / `Bind` / `Execute` / `Close` of an extended
    /// batch (raw wire bytes), preceded by the held unnamed `Parse` message
    /// when promotion kept it off the wire but the backend still holds it.
    fn journal_register_batch(session: &ClientSession, batch: &[u8], held_unnamed: Option<&[u8]>) {
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(msg) = held_unnamed {
            if msg.len() >= 5 && msg[0] == b'P' {
                Self::journal_note_parse(&mut cap, &msg[5..]);
            }
        }
        let mut i = 0usize;
        while i + 5 <= batch.len() {
            let mtype = batch[i];
            let len = u32::from_be_bytes([batch[i + 1], batch[i + 2], batch[i + 3], batch[i + 4]])
                as usize;
            if len < 4 || i + 1 + len > batch.len() {
                break;
            }
            let payload = &batch[i + 5..i + 1 + len];
            match mtype {
                b'P' => Self::journal_note_parse(&mut cap, payload),
                b'B' => cap.note_bind(payload),
                b'E' => {
                    let end = payload
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(payload.len());
                    let portal = std::str::from_utf8(&payload[..end]).unwrap_or("");
                    cap.note_execute(portal);
                }
                b'C' => cap.note_close(payload),
                _ => {}
            }
            i += 1 + len;
        }
        if cap.armed() {
            session.journal_armed.store(true, Ordering::Relaxed);
        }
    }

    /// Decode a `Parse` payload (name, query, param type OIDs) into the capture.
    fn journal_note_parse(cap: &mut crate::journal_capture::SessionCapture, payload: &[u8]) {
        let Some(n_end) = payload.iter().position(|&b| b == 0) else {
            return;
        };
        let name = std::str::from_utf8(&payload[..n_end]).unwrap_or("");
        let rest = &payload[n_end + 1..];
        let Some(q_end) = rest.iter().position(|&b| b == 0) else {
            return;
        };
        let Ok(query) = std::str::from_utf8(&rest[..q_end]) else {
            return;
        };
        let mut types = Vec::new();
        let t = &rest[q_end + 1..];
        if t.len() >= 2 {
            let n = u16::from_be_bytes([t[0], t[1]]) as usize;
            let mut off = 2;
            for _ in 0..n {
                if off + 4 > t.len() {
                    break;
                }
                types.push(u32::from_be_bytes([
                    t[off],
                    t[off + 1],
                    t[off + 2],
                    t[off + 3],
                ]));
                off += 4;
            }
        }
        cap.note_parse(name, query, types);
    }

    /// Note one backend frame for the capture: `CommandComplete` tags,
    /// `PortalSuspended`, `EmptyQueryResponse` and the first `ErrorResponse`.
    fn journal_note_frame(
        frame: &[u8],
        completions: &mut Vec<crate::journal_capture::Completion>,
        first_error: &mut Option<(String, String)>,
    ) {
        use crate::journal_capture::Completion;
        match frame.first() {
            Some(b'C') => {
                let body = frame.get(5..).unwrap_or(&[]);
                let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
                completions.push(Completion::Tag(
                    String::from_utf8_lossy(&body[..end]).into_owned(),
                ));
            }
            Some(b's') => completions.push(Completion::Suspended),
            Some(b'I') => completions.push(Completion::Empty),
            Some(b'E') if first_error.is_none() => {
                let mut code = String::new();
                let mut message = String::new();
                let mut body = frame.get(5..).unwrap_or(&[]);
                while let Some((&field, rest)) = body.split_first() {
                    if field == 0 {
                        break;
                    }
                    let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
                    let value = String::from_utf8_lossy(&rest[..end]);
                    match field {
                        b'C' => code = value.into_owned(),
                        b'M' => message = value.into_owned(),
                        _ => {}
                    }
                    body = rest.get(end + 1..).unwrap_or(&[]);
                }
                *first_error = Some((code, message));
            }
            _ => {}
        }
    }

    /// Source identity of the session's current transaction for the journal.
    async fn journal_source(session: &ClientSession) -> crate::transaction_journal::SourceIdentity {
        let (user, database, tenant) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned().unwrap_or_default(),
                vars.get("database").cloned().unwrap_or_default(),
                vars.get("tenant_id").cloned(),
            )
        };
        let backend = session
            .current_node
            .read()
            .await
            .clone()
            .unwrap_or_default();
        crate::transaction_journal::SourceIdentity {
            client_addr: session.client_addr.to_string(),
            user,
            database,
            backend,
            tenant,
        }
    }

    /// Apply capture operations and count them. Runs under the session's
    /// capture lock and never awaits: an open explicit transaction lives in
    /// the session (`SessionCapture::active_mut`) and only a commit touches
    /// shared state (`journal_capture::apply_ops`).
    fn journal_apply(
        state: &ServerState,
        ops: Vec<crate::journal_capture::JournalOp>,
        source: &crate::transaction_journal::SourceIdentity,
        session_id: Uuid,
        local: &mut Option<crate::transaction_journal::TransactionJournalEntry>,
    ) {
        if ops.is_empty() {
            return;
        }
        let node_id = crate::journal_capture::node_id_for_backend(&source.backend);
        let applied = crate::journal_capture::apply_ops(
            &state.transaction_journal,
            ops,
            session_id,
            node_id,
            source,
            local,
        );
        if applied.committed > 0 {
            state
                .metrics
                .journal_committed
                .fetch_add(applied.committed, Ordering::Relaxed);
        }
        if applied.rolled_back > 0 {
            state
                .metrics
                .journal_rolled_back
                .fetch_add(applied.rolled_back, Ordering::Relaxed);
        }
        if applied.statements > 0 {
            state
                .metrics
                .journal_statements
                .fetch_add(applied.statements, Ordering::Relaxed);
        }
    }

    /// A backend response completed (`ReadyForQuery` relayed): reconcile the
    /// capture with what the backend answered and apply the journal ops.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy", test))]
    async fn journal_observe(
        session: &ClientSession,
        state: &ServerState,
        outcome: crate::journal_capture::ResponseOutcome,
    ) {
        if let Some(cycle) = Self::journal_observe_sync(session, state, outcome) {
            Self::journal_observe_finish(session, state, cycle).await;
        }
    }

    /// Phase 1 of observing a completed cycle, synchronous and run before
    /// the `ReadyForQuery` is forwarded: reconcile the capture with what the
    /// backend answered and move the query-cache generations of the tables
    /// the cycle wrote (C-02). `None` on the idle fast path.
    fn journal_observe_sync(
        session: &ClientSession,
        state: &ServerState,
        outcome: crate::journal_capture::ResponseOutcome,
    ) -> Option<ObservedCycle> {
        let armed = session.journal_armed.swap(false, Ordering::Relaxed);
        // Fast path: an idle autocommit response with nothing registered and
        // no transaction being captured changes nothing.
        if !armed && outcome.status == b'I' && !session.journal_open.load(Ordering::Relaxed) {
            return None;
        }
        let ops = {
            let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
            let ops = cap.observe(&outcome);
            session
                .journal_open
                .store(cap.in_transaction(), Ordering::Relaxed);
            ops
        };
        #[cfg(feature = "query-cache")]
        let purge = match state.query_cache.as_ref() {
            Some(qc) if !ops.is_empty() => {
                let work = {
                    let mut stage = session
                        .tx_cache_stage
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    Self::cache_work_from_ops(qc, &ops, &mut stage)
                };
                if work.all {
                    qc.invalidate_all();
                }
                qc.mark_written(&work.tables);
                work.tables
            }
            _ => Vec::new(),
        };
        #[cfg(not(feature = "query-cache"))]
        let _ = state;
        Some(ObservedCycle {
            ops,
            #[cfg(feature = "query-cache")]
            purge,
        })
    }

    /// Phase 2, after the client has its answer: purge the L2 entries of the
    /// written tables and apply the journal ops (skipped when TR is off and
    /// the capture ran only for the cache).
    async fn journal_observe_finish(
        session: &ClientSession,
        state: &ServerState,
        cycle: ObservedCycle,
    ) {
        #[cfg(feature = "query-cache")]
        if !cycle.purge.is_empty() {
            if let Some(qc) = state.query_cache.as_ref() {
                qc.purge_tables(&cycle.purge).await;
            }
        }
        if cycle.ops.is_empty() || !state.live_config.load().tr_enabled {
            return;
        }
        // The source identity is the only thing that awaits; it is read
        // before the capture lock so nothing is held across an await.
        let source = Self::journal_source(session).await;
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        Self::journal_apply(state, cycle.ops, &source, session.id, cap.active_mut());
    }

    /// Whether the journal capture must run for the query cache even when TR
    /// is off: commit-aware invalidation (C-02) is driven by its ops.
    fn cache_needs_capture(state: &ServerState) -> bool {
        #[cfg(feature = "query-cache")]
        {
            state.query_cache.is_some()
        }
        #[cfg(not(feature = "query-cache"))]
        {
            let _ = state;
            false
        }
    }

    /// Translate one cycle's capture ops into cache invalidation (C-02).
    /// Statement-time: every successful write invalidates its tables at once
    /// (a later read must not hit a pre-write entry). Commit-time: the tables
    /// an explicit transaction staged are invalidated again when the backend
    /// reports the commit, closing the window in which another session
    /// refilled an entry from pre-commit data. A rollback drops the stage.
    /// A write whose tables cannot be read from its text invalidates
    /// everything.
    #[cfg(feature = "query-cache")]
    fn cache_work_from_ops(
        qc: &crate::cache::QueryCache,
        ops: &[crate::journal_capture::JournalOp],
        stage: &mut TxCacheStage,
    ) -> CacheWork {
        use crate::journal_capture::JournalOp;
        let mut work = CacheWork::default();
        let add = |tables: &mut Vec<String>, t: Vec<String>| {
            for table in t {
                if !tables.contains(&table) {
                    tables.push(table);
                }
            }
        };
        for op in ops {
            match op {
                // Inside an explicit transaction a write is only staged: no
                // other session can see it before the commit, and the
                // session's own reads are never served from the cache while
                // it is in a transaction. Marking it at statement time as well
                // cost every in-transaction statement a generation bump and a
                // purge before its ReadyForQuery (C-02 user-path gate).
                JournalOp::Log { entry, .. } => match qc.write_tables(&entry.statement) {
                    Some(t) => add(&mut stage.tables, t),
                    None => stage.unknown = true,
                },
                JournalOp::Incomplete { .. } => stage.unknown = true,
                JournalOp::AutoCommit {
                    entries,
                    incomplete,
                    ..
                } => {
                    if incomplete.is_some() {
                        work.all = true;
                    }
                    for e in entries {
                        match qc.write_tables(&e.statement) {
                            Some(t) => add(&mut work.tables, t),
                            None => work.all = true,
                        }
                    }
                }
                JournalOp::Commit { .. } => {
                    let staged = std::mem::take(&mut stage.tables);
                    add(&mut work.tables, staged);
                    if std::mem::take(&mut stage.unknown) {
                        work.all = true;
                    }
                }
                JournalOp::Rollback { .. } => {
                    stage.tables.clear();
                    stage.unknown = false;
                }
                JournalOp::Begin { .. }
                | JournalOp::Savepoint { .. }
                | JournalOp::RollbackTo { .. } => {}
            }
        }
        work
    }

    /// `journal_observe` for a relay that recorded only the status byte
    /// (the cacheable-read relay never carries a registered statement).
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    async fn journal_observe_status(session: &ClientSession, state: &ServerState) {
        let status = session.last_rfq_status.load(Ordering::Relaxed);
        Self::journal_observe(
            session,
            state,
            crate::journal_capture::ResponseOutcome::status_only(status),
        )
        .await;
    }

    /// The proxy synthesized the `ReadyForQuery` the client just saw: nothing
    /// registered for the cycle reached the backend.
    fn journal_discard(session: &ClientSession, status: u8) {
        session.journal_armed.store(false, Ordering::Relaxed);
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        cap.discard(status);
        // Keep the relay's fast path off until a deferred rollback has been
        // applied by the next `observe` (it only runs under the lock).
        session.journal_open.store(
            cap.in_transaction() || cap.has_deferred(),
            Ordering::Relaxed,
        );
    }

    /// The session ended.
    async fn journal_close(session: &ClientSession, state: &ServerState) {
        #[cfg(feature = "query-cache")]
        {
            let mut stage = session
                .tx_cache_stage
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *stage = TxCacheStage::default();
        }
        let source = Self::journal_source(session).await;
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        let ops = cap.close();
        session.journal_open.store(false, Ordering::Relaxed);
        Self::journal_apply(state, ops, &source, session.id, cap.active_mut());
    }

    /// Hand a forwarded query to the analytics engine. This only builds the
    /// `QueryExecution` and pushes it onto the engine's bounded queue — the
    /// fingerprinting, metrics, slow-query log and pattern detection all run
    /// on the single background consumer task (see
    /// `QueryAnalytics::start_consumer`), so the connection task never pays
    /// for them. A full queue drops the sample rather than stalling the relay.
    /// No-op when analytics is disabled.
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
        // Both pre-rendered at session creation — see `ClientSession`.
        exec.client_ip = session.client_ip_str.clone();
        exec.node = node.to_string();
        exec.session_id = Some(session.session_id_str.clone());
        exec.error = error;
        analytics.record(exec);
    }

    /// Select primary node with write timeout during failover
    async fn select_primary_with_timeout(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        let deadline = tokio::time::Instant::now() + config.write_timeout();
        Self::select_primary_until(session, state, config, deadline).await
    }

    /// Build the authoritative primary tracker from `[topology]` (H-01).
    ///
    /// `static` (the default) returns a standalone tracker and
    /// `authoritative = false`, preserving the historical write-path
    /// behaviour. `postgres` builds a `PostgresTopologyProvider` over every
    /// configured node and `patroni` a `PatroniTopologyProvider` over
    /// `topology.patroni_endpoints` (both feature `postgres-topology`); either
    /// marks the tracker authoritative, so a promotion moves the write
    /// destination without any `proxy.toml` edit.
    fn build_primary_tracker(config: &ProxyConfig) -> (Arc<PrimaryTracker>, bool, TopologyPoller) {
        #[cfg(feature = "postgres-topology")]
        {
            if config.topology.provider == crate::config::TopologyProviderKind::Patroni {
                // The leader Patroni names must be one of these addresses,
                // exactly as `[[nodes]]` spells them, or nothing is authorized.
                let nodes = config
                    .nodes
                    .iter()
                    .map(|n| crate::primary_tracker::PatroniNode {
                        node_id: uuid::Uuid::new_v4(),
                        address: n.address().to_string(),
                    })
                    .collect();
                let provider = Arc::new(
                    crate::primary_tracker::PatroniTopologyProvider::new(
                        config.topology.patroni_endpoints.clone(),
                        nodes,
                        Duration::from_millis(config.topology.patroni_request_timeout_ms.max(1)),
                    )
                    .with_poll_interval(Duration::from_secs(
                        config.topology.poll_interval_secs.max(1),
                    )),
                );
                tracing::info!(
                    endpoints = config.topology.patroni_endpoints.len(),
                    nodes = config.nodes.len(),
                    poll_interval_secs = config.topology.poll_interval_secs,
                    lease_timeout_secs = config.topology.lease_timeout_secs,
                    "authoritative topology provider enabled: patroni (GET /cluster polling)"
                );
                let tracker = Arc::new(
                    PrimaryTracker::with_provider(provider.clone()).with_lease_timeout(
                        Duration::from_secs(config.topology.lease_timeout_secs.max(1)),
                    ),
                );
                return (tracker, true, TopologyPoller::Patroni(provider));
            }
            if config.topology.provider == crate::config::TopologyProviderKind::Postgres {
                let nodes = config
                    .nodes
                    .iter()
                    .map(|n| crate::primary_tracker::PostgresNode {
                        node_id: uuid::Uuid::new_v4(),
                        host: n.host.clone(),
                        port: n.port,
                        user: config.topology.user.clone(),
                        password: config.topology.password.clone(),
                        database: config.topology.database.clone(),
                    })
                    .collect();
                let provider = Arc::new(
                    crate::primary_tracker::PostgresTopologyProvider::new(nodes)
                        .with_poll_interval(Duration::from_secs(
                            config.topology.poll_interval_secs.max(1),
                        )),
                );
                tracing::info!(
                    nodes = config.nodes.len(),
                    poll_interval_secs = config.topology.poll_interval_secs,
                    lease_timeout_secs = config.topology.lease_timeout_secs,
                    "authoritative topology provider enabled: postgres (pg_is_in_recovery polling)"
                );
                let tracker = Arc::new(
                    PrimaryTracker::with_provider(provider.clone()).with_lease_timeout(
                        Duration::from_secs(config.topology.lease_timeout_secs.max(1)),
                    ),
                );
                return (tracker, true, TopologyPoller::Postgres(provider));
            }
        }
        let _ = config;
        (
            Arc::new(PrimaryTracker::new_standalone()),
            false,
            TopologyPoller::Static,
        )
    }

    /// Resolve the authoritative provider's leader against the configured
    /// nodes (H-01/H-02). Returns `Some(address)` only while the tracker's
    /// authority lease is valid (the provider was reached within
    /// `topology.lease_timeout_secs`) and the leader is an **enabled**
    /// configured node; `None` means the write path must wait (fail closed)
    /// instead of falling back to roles or using stale knowledge.
    fn authoritative_leader(config: &ProxyConfig, tracker: &PrimaryTracker) -> Option<String> {
        if !tracker.authority_valid() {
            return None;
        }
        let addr = tracker.get_primary_address()?;
        config
            .nodes
            .iter()
            .any(|n| n.enabled && n.address() == addr)
            .then_some(addr)
    }

    /// Full-jitter exponential backoff for the primary-wait loops (H-05):
    /// base 100 ms doubling to a 2 s cap, mixed with a per-session seed so a
    /// fleet waking after the same failover does not stampede the promoted
    /// primary in lockstep. Deterministic per `(seed, attempt)`.
    fn reconnect_backoff(attempt: u32, seed: u64) -> Duration {
        let base_ms = 100u64;
        let cap_ms = 2_000u64;
        let window = base_ms
            .saturating_mul(1u64 << attempt.min(5))
            .clamp(1, cap_ms);
        Duration::from_millis(
            splitmix64(seed ^ (attempt as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)) % window + 1,
        )
    }

    /// `select_primary_with_timeout` against a caller-owned deadline, so one
    /// recovery deadline can be carried through every phase (TR-06).
    async fn select_primary_until(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        deadline: tokio::time::Instant,
    ) -> Result<String> {
        let start = tokio::time::Instant::now();
        let timeout = deadline.saturating_duration_since(start);
        // H-05: per-session full-jitter backoff instead of a fixed 100 ms poll,
        // so a fleet that lost the same primary does not reconnect in lockstep.
        let backoff_seed = session.id.as_u128() as u64;
        let mut attempt: u32 = 0;

        loop {
            // H-01: when a topology provider is authoritative, only its
            // leader is eligible. If the provider has no leader (or the
            // leader is disabled) new writes wait — they must never fall
            // back to a configured role the provider has not authorised.
            if state.authoritative_topology {
                if let Some(addr) = Self::authoritative_leader(config, &state.primary_tracker) {
                    let mut current = session.current_node.write().await;
                    *current = Some(addr.clone());
                    return Ok(addr);
                }
                if tokio::time::Instant::now() >= deadline {
                    state.metrics.failovers.fetch_add(1, Ordering::Relaxed);
                    return Err(ProxyError::NoHealthyNodes);
                }
                tracing::warn!(
                    "Authoritative topology has no eligible primary; waiting... ({:.1}s elapsed)",
                    start.elapsed().as_secs_f64()
                );
                state
                    .metrics
                    .reconnect_attempts
                    .fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Self::reconnect_backoff(attempt, backoff_seed)).await;
                attempt = attempt.saturating_add(1);
                continue;
            }

            // Try to find a healthy primary. Every enabled primary is
            // considered in config order (not just the first): a config that
            // lists a promoted/secondary primary must be able to fail over to
            // it once the first is demoted in-band — `select_node_for_startup`
            // already applies the same rule for new sessions.
            let health = state.health.load_full();
            let primary = config.nodes.iter().find(|n| {
                n.role == NodeRole::Primary
                    && n.enabled
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false)
            });

            if let Some(primary_node) = primary {
                // Update session's current node
                let node_addr = primary_node.address().to_string();
                let mut current = session.current_node.write().await;
                *current = Some(node_addr.clone());
                return Ok(node_addr);
            }
            drop(health);

            // Check if the deadline passed
            if tokio::time::Instant::now() >= deadline {
                state.metrics.failovers.fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::NoHealthyNodes);
            }

            tracing::warn!(
                "Primary unavailable, waiting for failover... ({:.1}s elapsed, {:.1}s timeout)",
                start.elapsed().as_secs_f64(),
                timeout.as_secs_f64()
            );

            // Wait before retry (jittered exponential backoff, H-05).
            state
                .metrics
                .reconnect_attempts
                .fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Self::reconnect_backoff(attempt, backoff_seed)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    /// Choose one index among the eligible read nodes for `strategy` (H-03).
    ///
    /// Pure and unit-tested: callers pass parallel per-node arrays (weights,
    /// measured health-probe latency, sessions currently attached) plus the
    /// monotonic round-robin ticket. `ticket` also seeds the Random and
    /// PowerOfTwo samplers so the sequence stays deterministic in tests while
    /// behaving as a uniform stream in production.
    ///
    /// Strategies that need load accounting are only invoked after the caller
    /// gathered it, so the default RoundRobin path stays O(1).
    fn pick_read_node(
        strategy: crate::config::Strategy,
        weights: &[u32],
        latency_ms: &[f64],
        attached: &[u64],
        ticket: u64,
    ) -> Option<usize> {
        use crate::config::Strategy;
        let len = weights.len();
        if len == 0 || latency_ms.len() != len || attached.len() != len {
            return None;
        }
        let pick_pair = |t: u64, len: usize| -> (usize, usize) {
            let i = (splitmix64(t) % len as u64) as usize;
            let mut j = (splitmix64(t ^ 0x5deece66d) % len as u64) as usize;
            if j == i {
                j = (j + 1) % len;
            }
            (i, j)
        };

        match strategy {
            Strategy::RoundRobin => Some((ticket % len as u64) as usize),
            Strategy::WeightedRoundRobin => {
                let total: u64 = weights.iter().map(|w| *w as u64).sum();
                if total == 0 {
                    return Some((ticket % len as u64) as usize);
                }
                let mut target = ticket % total;
                for (i, w) in weights.iter().enumerate() {
                    if target < *w as u64 {
                        return Some(i);
                    }
                    target -= *w as u64;
                }
                Some(len - 1)
            }
            Strategy::LeastConnections | Strategy::PowerOfTwo => {
                let score = |i: usize| (attached[i], latency_ms[i]);
                if strategy == Strategy::LeastConnections {
                    return (0..len).min_by(|a, b| {
                        score(*a)
                            .partial_cmp(&score(*b))
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then(a.cmp(b))
                    });
                }
                let (i, j) = pick_pair(ticket, len);
                let (a, b) = (score(i), score(j));
                Some(
                    if a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
                        != std::cmp::Ordering::Greater
                    {
                        i
                    } else {
                        j
                    },
                )
            }
            Strategy::LatencyBased => (0..len).min_by(|a, b| {
                latency_ms[*a]
                    .partial_cmp(&latency_ms[*b])
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.cmp(b))
            }),
            Strategy::Random => Some((splitmix64(ticket) % len as u64) as usize),
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
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false);
                // Drop a standby whose circuit is open so reads avoid it.
                #[cfg(feature = "circuit-breaker")]
                let base = base && !Self::circuit_is_open(state, n.address());
                // Drop a standby lagging beyond the configured byte threshold.
                #[cfg(feature = "lag-routing")]
                let base = base
                    && !Self::lag_excludes_standby(
                        health
                            .get(n.address())
                            .and_then(|h| h.replication_lag_bytes),
                        config.lag_routing.max_lag_bytes,
                        config.lag_routing.require_known_lag,
                    );
                base
            })
            .collect();

        if !healthy_standbys.is_empty() {
            let strategy = config.load_balancer.read_strategy;
            let weights: Vec<u32> = healthy_standbys.iter().map(|n| n.weight).collect();
            let latency: Vec<f64> = healthy_standbys
                .iter()
                .map(|n| health.get(n.address()).map(|h| h.latency_ms).unwrap_or(0.0))
                .collect();
            // Load accounting is only gathered for the strategies that use it,
            // so the default RoundRobin path stays O(1). "Attached" is the
            // best-effort load signal available on the data path: how many
            // live sessions currently have this node as their backend.
            let attached: Vec<u64> =
                if matches!(strategy, Strategy::LeastConnections | Strategy::PowerOfTwo) {
                    healthy_standbys
                        .iter()
                        .map(|n| {
                            let addr = n.address();
                            state
                                .sessions
                                .iter()
                                .filter(|s| {
                                    s.current_node
                                        .try_read()
                                        .ok()
                                        .and_then(|g| g.clone())
                                        .map(|cur| cur == addr)
                                        .unwrap_or(false)
                                })
                                .count() as u64
                        })
                        .collect()
                } else {
                    vec![0; healthy_standbys.len()]
                };

            let ticket = state.lb_state.rr_counter.fetch_add(1, Ordering::Relaxed);
            let index = Self::pick_read_node(strategy, &weights, &latency, &attached, ticket)
                .unwrap_or_else(|| (ticket as usize) % healthy_standbys.len());
            let node_addr = healthy_standbys[index].address().to_string();

            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to primary if no healthy standbys
        Self::select_node(session, state, config).await
    }

    /// Create PostgreSQL error response message
    fn create_error_response(code: &str, message: &str) -> Vec<u8> {
        Self::create_severity_response("ERROR", code, message)
    }

    /// Create a PostgreSQL `ErrorResponse` with `FATAL` severity — the severity
    /// PostgreSQL itself uses for a connection it is about to close (e.g.
    /// `53300 too_many_connections`, `57P05 idle_session_timeout`). Drivers
    /// (pgx, npgsql, JDBC) key on `FATAL` to mark the connection dead; with
    /// `ERROR` they try to keep using it and then hit a bare EOF.
    fn create_fatal_response(code: &str, message: &str) -> Vec<u8> {
        Self::create_severity_response("FATAL", code, message)
    }

    fn create_severity_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
        // One allocation, no HashMap + four Strings per error frame (O-05).
        // Framing is byte-identical to `ErrorResponse { fields }.encode()
        // .encode()`: 'E' tag, u32 length (payload + 4), fields S/V/C/M each
        // followed by NUL, then the payload terminator.
        let mut payload = Vec::with_capacity(16 + severity.len() * 2 + code.len() + message.len());
        for (field, value) in [
            ('S', severity),
            ('V', severity),
            ('C', code),
            ('M', message),
        ] {
            payload.push(field as u8);
            payload.extend_from_slice(value.as_bytes());
            payload.push(0);
        }
        payload.push(0);

        let mut frame = Vec::with_capacity(payload.len() + 5);
        frame.push(b'E');
        frame.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
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
            .filter(|n| n.enabled && health.get(n.address()).map(|h| h.healthy).unwrap_or(false))
            .collect();

        if healthy_nodes.is_empty() {
            return Err(ProxyError::NoHealthyNodes);
        }

        // Try to find healthy primary first
        if let Some(primary) = healthy_nodes.iter().find(|n| n.role == NodeRole::Primary) {
            let node_addr = primary.address().to_string();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to standby if primary is unavailable
        // (Initial connection will work, writes will use write timeout to wait for primary)
        if let Some(standby) = healthy_nodes.iter().find(|n| n.role == NodeRole::Standby) {
            tracing::warn!("Primary unavailable, connecting to standby for initial session");
            let node_addr = standby.address().to_string();
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
