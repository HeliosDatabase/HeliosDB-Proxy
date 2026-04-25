//! Proxy Server Implementation
//!
//! Main server that accepts client connections and routes them to backends.
//! Implements PostgreSQL wire protocol forwarding with TWR (Transparent Write Routing).

use crate::admin::{AdminServer, AdminState, ConfigSnapshot, NodeSnapshot};
#[cfg(feature = "ha-tr")]
use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::config::{NodeConfig, NodeRole, ProxyConfig, TrMode};
use crate::protocol::{
    AuthRequest, ErrorResponse, Message, MessageType, ParseMessage, ProtocolCodec, QueryMessage,
    StartupMessage, TransactionStatus,
};
use crate::{ProxyError, Result};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use uuid::Uuid;

// Pool-modes feature imports
#[cfg(feature = "pool-modes")]
use crate::pool::{
    ConnectionLease, ConnectionPoolManager, LeaseAction, PoolModeConfig, PoolingMode,
};
#[cfg(feature = "pool-modes")]
use crate::pool::lease::ClientId;
#[cfg(feature = "pool-modes")]
use crate::NodeEndpoint;

// WASM plugin system imports
#[cfg(feature = "wasm-plugins")]
use crate::plugins::{
    AuthRequest as PluginAuthRequest, AuthResult, HookContext, Identity, PluginManager,
    PostQueryOutcome, PreQueryResult, QueryContext, RouteResult,
};

/// Proxy server
pub struct ProxyServer {
    config: ProxyConfig,
    state: Arc<ServerState>,
    shutdown_tx: broadcast::Sender<()>,
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
    /// Connection pools per node
    pools: RwLock<HashMap<String, NodePool>>,
    /// Node health status
    health: RwLock<HashMap<String, NodeHealth>>,
    /// Metrics
    metrics: ServerMetrics,
    /// Load balancer state
    lb_state: RwLock<LoadBalancerState>,
    /// Pool manager for Session/Transaction/Statement modes
    #[cfg(feature = "pool-modes")]
    pool_manager: Option<Arc<ConnectionPoolManager>>,
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
}

/// Per-node connection pool
struct NodePool {
    /// Node configuration
    config: NodeConfig,
    /// Available connections
    connections: RwLock<Vec<BackendConnection>>,
    /// Connection limit semaphore
    semaphore: Semaphore,
    /// Active connection count
    active_count: AtomicU64,
}

/// Backend connection
struct BackendConnection {
    /// Connection ID
    id: Uuid,
    /// TCP stream (wrapped for protocol handling)
    stream: Option<TcpStream>,
    /// Creation time
    created_at: chrono::DateTime<chrono::Utc>,
    /// Last used time
    last_used: chrono::DateTime<chrono::Utc>,
    /// Whether connection is healthy
    healthy: bool,
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
    /// Round-robin counter
    rr_counter: u64,
    /// Node weights for weighted round-robin
    weights: HashMap<String, u32>,
    /// Current weight counter
    weight_counter: HashMap<String, u32>,
}

/// Client session
pub struct ClientSession {
    /// Session ID
    pub id: Uuid,
    /// Client address
    pub client_addr: SocketAddr,
    /// Current backend node
    pub current_node: RwLock<Option<String>>,
    /// Transaction state
    pub tx_state: RwLock<TransactionState>,
    /// Session variables
    pub variables: RwLock<HashMap<String, String>>,
    /// Created at
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// TR mode for this session
    pub tr_mode: TrMode,
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

/// Disposition produced by the pre-query plugin hook stage.
///
/// When the `wasm-plugins` feature is off, only `Forward` is ever produced —
/// the hook dispatch is compiled out entirely and the variant list exists
/// purely for pattern-match symmetry.
#[derive(Debug)]
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
    #[allow(dead_code)]
    Cached(Vec<u8>),
}

/// Override produced by the Route plugin hook. Consumed by `route_and_forward`
/// when deciding which backend to talk to.
///
/// As with `PreQueryAction`, only `None` is ever produced when the
/// `wasm-plugins` feature is off.
#[derive(Debug)]
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

        // Initialize pools for each node
        let mut pools = HashMap::new();
        for node in &config.nodes {
            let pool = NodePool {
                config: node.clone(),
                connections: RwLock::new(Vec::new()),
                semaphore: Semaphore::new(config.pool.max_connections),
                active_count: AtomicU64::new(0),
            };
            pools.insert(node.address(), pool);
        }

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

        // Initialize load balancer state
        let mut weights = HashMap::new();
        let mut weight_counter = HashMap::new();
        for node in &config.nodes {
            weights.insert(node.address(), node.weight);
            weight_counter.insert(node.address(), node.weight);
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
                    crate::config::PreparedStatementMode::Track => {
                        PoolPreparedStatementMode::Track
                    }
                    crate::config::PreparedStatementMode::Named => {
                        PoolPreparedStatementMode::Named
                    }
                },
                test_on_acquire: config.pool.test_on_acquire,
                validation_query: "SELECT 1".to_string(),
                queue_timeout_secs: 30,
                max_queue_size: 0,
            };
            Some(Arc::new(ConnectionPoolManager::new(pool_config)))
        };

        // Initialize plugin manager if the wasm-plugins feature is enabled
        // AND plugins are turned on in config. Scans plugin_dir for `.wasm`
        // files and loads each; a missing directory is non-fatal and logs
        // a warning so empty deployments don't fail startup.
        #[cfg(feature = "wasm-plugins")]
        let plugin_manager = Self::init_plugin_manager(&config.plugins);

        let state = Arc::new(ServerState {
            sessions: RwLock::new(HashMap::new()),
            pools: RwLock::new(pools),
            health: RwLock::new(health),
            metrics: ServerMetrics::default(),
            lb_state: RwLock::new(LoadBalancerState {
                rr_counter: 0,
                weights,
                weight_counter,
            }),
            #[cfg(feature = "pool-modes")]
            pool_manager,
            #[cfg(feature = "wasm-plugins")]
            plugin_manager,
            #[cfg(feature = "ha-tr")]
            transaction_journal: Arc::new(
                crate::transaction_journal::TransactionJournal::new(),
            ),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(
                crate::anomaly::AnomalyDetector::new(
                    crate::anomaly::AnomalyConfig::default(),
                ),
            ),
        });

        Ok(Self {
            config,
            state,
            shutdown_tx,
        })
    }

    /// Run the proxy server
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address)
            .await
            .map_err(|e| ProxyError::Network(format!("Failed to bind: {}", e)))?;

        tracing::info!("Proxy listening on {}", self.config.listen_address);

        // Start background tasks
        let health_task = self.spawn_health_checker();
        let pool_task = self.spawn_pool_manager();

        // Start admin server
        let admin_task = self.spawn_admin_server();

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            self.state.metrics.connections_accepted.fetch_add(1, Ordering::Relaxed);
                            let state = self.state.clone();
                            let config = self.config.clone();
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

        // Wait for background tasks
        health_task.abort();
        pool_task.abort();
        admin_task.abort();

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
                    nodes: config.nodes.iter().map(|n| NodeSnapshot {
                        address: n.address(),
                        role: format!("{:?}", n.role),
                        weight: n.weight,
                        enabled: n.enabled,
                    }).collect(),
                };
            }

            // Set proxy config for SQL routing
            admin_state.set_proxy_config(config.clone()).await;

            // Attach the plugin manager so /plugins + the admin UI
            // surface real loaded modules. Cheap Arc-clone — no
            // duplicate state, both AdminState and ServerState hold
            // the same manager.
            #[cfg(feature = "wasm-plugins")]
            if let Some(ref pm) = state.plugin_manager {
                admin_state.with_plugin_manager(pm.clone()).await;
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
                        let health = server_state.health.read().await;
                        let mut admin_health = admin_state_sync.node_health.write().await;
                        *admin_health = health.clone();
                    }

                    // Sync metrics
                    {
                        let metrics = ServerMetricsSnapshot {
                            connections_accepted: server_state.metrics.connections_accepted.load(Ordering::Relaxed),
                            connections_closed: server_state.metrics.connections_closed.load(Ordering::Relaxed),
                            queries_processed: server_state.metrics.queries_processed.load(Ordering::Relaxed),
                            bytes_received: server_state.metrics.bytes_received.load(Ordering::Relaxed),
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
        mut stream: TcpStream,
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
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: config.tr_mode,
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

        // Main client loop
        let result = Self::client_loop(&mut stream, &session, &state, &config).await;

        // Cleanup session
        {
            let mut sessions = state.sessions.write().await;
            sessions.remove(&session.id);
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
        stream: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<()> {
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(8192);
        let mut backend_stream: Option<TcpStream> = None;
        let mut backend_node: Option<String> = None;

        // Handle startup phase
        let startup_result =
            Self::handle_startup(stream, &mut buffer, &codec, session, state, config).await;

        match startup_result {
            Ok((Some(stream_conn), node_addr)) => {
                backend_stream = Some(stream_conn);
                backend_node = Some(node_addr);
            }
            Ok((None, _)) => {
                // SSL rejected or cancel request, connection should close
                return Ok(());
            }
            Err(e) => {
                tracing::error!("Startup failed: {}", e);
                // Send error to client
                let err_msg = Self::create_error_response("08006", &format!("Startup failed: {}", e));
                let _ = stream.write_all(&err_msg).await;
                return Err(e);
            }
        }

        // Main query loop
        loop {
            // Read from client
            let mut read_buf = vec![0u8; 8192];
            let n = stream
                .read(&mut read_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("Read error: {}", e)))?;

            if n == 0 {
                // Client disconnected
                break;
            }

            buffer.extend_from_slice(&read_buf[..n]);
            state.metrics.bytes_received.fetch_add(n as u64, Ordering::Relaxed);

            // Process all complete messages in buffer
            while let Some(msg) = codec.decode_message(&mut buffer)? {
                // Handle Terminate message
                if msg.msg_type == MessageType::Terminate {
                    return Ok(());
                }

                // Anomaly detector — record every Query message
                // (rate window, novel-fingerprint detector, SQLi
                // pattern scan). Fires before the plugin hook so a
                // detection lands in the audit trail even if a
                // plugin later blocks.
                #[cfg(feature = "anomaly-detection")]
                Self::record_anomaly_observation(&msg, state, session);

                // Plugin pre-query hook — may rewrite the SQL, block the
                // query with an error, or (future) return a cached response.
                let (msg, action) = Self::apply_pre_query_hook(msg, state, session);

                if let PreQueryAction::Block(reason) = &action {
                    tracing::info!(reason = %reason, "pre-query plugin blocked query");
                    Self::send_block_response(stream, reason, state).await?;
                    state.metrics.queries_processed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Plugin returned a fully-formed cached response. Synthesise
                // the PG wire reply (RowDescription + DataRows +
                // CommandComplete + ReadyForQuery) and send it directly —
                // the backend is never touched. On malformed payloads we
                // log + fall through to normal forwarding so a buggy plugin
                // degrades gracefully instead of taking the proxy down.
                #[cfg(feature = "wasm-plugins")]
                if let PreQueryAction::Cached(bytes) = &action {
                    match Self::synthesise_cached_response(bytes) {
                        Ok(reply) => {
                            stream
                                .write_all(&reply)
                                .await
                                .map_err(|e| {
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
                            tracing::warn!(
                                error = %e,
                                "failed to synthesise cached response; falling back to backend"
                            );
                            // fall through to normal forwarding
                        }
                    }
                }

                // Route and process the message
                let forward_start = std::time::Instant::now();
                let forward_result = Self::route_and_forward(
                    &msg,
                    backend_stream.take(),
                    backend_node.take(),
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
                    &forward_result,
                    forward_start.elapsed(),
                );
                let (response, new_backend, new_node) = forward_result?;

                backend_stream = new_backend;
                backend_node = new_node;

                // Send response to client
                if !response.is_empty() {
                    stream
                        .write_all(&response)
                        .await
                        .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;

                    state
                        .metrics
                        .bytes_sent
                        .fetch_add(response.len() as u64, Ordering::Relaxed);
                }

                state.metrics.queries_processed.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Handle PostgreSQL startup phase (SSL, authentication)
    async fn handle_startup(
        client_stream: &mut TcpStream,
        buffer: &mut BytesMut,
        codec: &ProtocolCodec,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // Read startup message
        let mut read_buf = vec![0u8; 1024];
        let n = client_stream
            .read(&mut read_buf)
            .await
            .map_err(|e| ProxyError::Network(format!("Startup read error: {}", e)))?;

        if n == 0 {
            return Ok((None, String::new()));
        }

        buffer.extend_from_slice(&read_buf[..n]);

        // Parse startup message
        let startup_msg = codec.decode_startup(buffer)?;

        match startup_msg {
            Some(StartupMessage::SSLRequest) => {
                // Reject SSL (send 'N')
                client_stream
                    .write_all(&[b'N'])
                    .await
                    .map_err(|e| ProxyError::Network(format!("SSL reject error: {}", e)))?;

                // Read actual startup message
                buffer.clear();
                let n = client_stream
                    .read(&mut read_buf)
                    .await
                    .map_err(|e| ProxyError::Network(format!("Post-SSL read error: {}", e)))?;

                if n == 0 {
                    return Ok((None, String::new()));
                }

                buffer.extend_from_slice(&read_buf[..n]);

                // Parse the real startup message
                return Self::process_startup(
                    client_stream,
                    buffer,
                    codec,
                    session,
                    state,
                    config,
                )
                .await;
            }
            Some(StartupMessage::CancelRequest { .. }) => {
                // Cancel requests are handled separately, just close connection
                return Ok((None, String::new()));
            }
            Some(StartupMessage::Startup { params, .. }) => {
                // Connect to backend and forward startup
                return Self::connect_and_authenticate(
                    client_stream,
                    &params,
                    session,
                    state,
                    config,
                )
                .await;
            }
            None => {
                return Err(ProxyError::Protocol("Incomplete startup message".to_string()));
            }
        }
    }

    /// Process startup message after SSL negotiation
    async fn process_startup(
        client_stream: &mut TcpStream,
        buffer: &mut BytesMut,
        codec: &ProtocolCodec,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        let startup_msg = codec.decode_startup(buffer)?;

        match startup_msg {
            Some(StartupMessage::Startup { params, .. }) => {
                Self::connect_and_authenticate(client_stream, &params, session, state, config).await
            }
            _ => Err(ProxyError::Protocol("Expected startup message".to_string())),
        }
    }

    /// Connect to backend and handle authentication
    async fn connect_and_authenticate(
        client_stream: &mut TcpStream,
        params: &HashMap<String, String>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // Plugin Authenticate hook — may deny the connection outright or
        // attach a richer identity (roles, tenant_id, claims) onto the
        // session for downstream plugins to consume. Happens before any
        // backend connection is opened so denials cost nothing on the
        // backend side.
        Self::apply_authenticate_hook(params, session, state).await?;

        // Select initial backend node (primary for now)
        let node_addr = Self::select_node(session, state, config).await?;

        // Connect to backend
        let mut backend = tokio::time::timeout(
            config.pool.acquire_timeout(),
            TcpStream::connect(&node_addr),
        )
        .await
        .map_err(|_| ProxyError::Connection(format!("Connection timeout to {}", node_addr)))?
        .map_err(|e| ProxyError::Connection(format!("Failed to connect to {}: {}", node_addr, e)))?;

        // Build and send startup message to backend
        let startup_bytes = Self::build_startup_message(params);
        backend
            .write_all(&startup_bytes)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend startup write error: {}", e)))?;

        // Forward authentication messages between client and backend
        Self::proxy_authentication(client_stream, &mut backend).await?;

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

    /// Proxy authentication messages between client and backend
    async fn proxy_authentication(
        client_stream: &mut TcpStream,
        backend_stream: &mut TcpStream,
    ) -> Result<()> {
        let codec = ProtocolCodec::new();
        let mut backend_buffer = BytesMut::with_capacity(4096);
        let mut client_buffer = BytesMut::with_capacity(4096);

        loop {
            // Read from backend
            let mut read_buf = vec![0u8; 4096];
            let n = backend_stream
                .read(&mut read_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("Backend auth read error: {}", e)))?;

            if n == 0 {
                return Err(ProxyError::Connection("Backend closed during auth".to_string()));
            }

            backend_buffer.extend_from_slice(&read_buf[..n]);

            // Forward all data to client
            client_stream
                .write_all(&read_buf[..n])
                .await
                .map_err(|e| ProxyError::Network(format!("Client auth write error: {}", e)))?;

            // Check for authentication complete or error
            while let Some(msg) = codec.decode_message(&mut backend_buffer.clone())? {
                match msg.msg_type {
                    MessageType::AuthRequest => {
                        // Check if auth OK
                        if msg.payload.len() >= 4 {
                            let auth_type =
                                i32::from_be_bytes([msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3]]);
                            if auth_type == 0 {
                                // AuthenticationOk - continue to read ReadyForQuery
                            }
                        }
                    }
                    MessageType::ReadyForQuery => {
                        // Authentication complete
                        return Ok(());
                    }
                    MessageType::ErrorResponse => {
                        // Authentication failed - error already sent to client
                        return Err(ProxyError::Auth("Authentication failed".to_string()));
                    }
                    _ => {
                        // Continue forwarding
                    }
                }
                // Advance the actual buffer
                let _ = codec.decode_message(&mut backend_buffer)?;
            }

            // If backend requires password, forward client's response
            // Read password from client if needed
            let n = tokio::time::timeout(Duration::from_millis(100), client_stream.read(&mut read_buf))
                .await;

            if let Ok(Ok(n)) = n {
                if n > 0 {
                    client_buffer.extend_from_slice(&read_buf[..n]);
                    backend_stream
                        .write_all(&read_buf[..n])
                        .await
                        .map_err(|e| ProxyError::Network(format!("Backend password write error: {}", e)))?;
                }
            }
        }
    }

    /// Route message and forward to appropriate backend
    async fn route_and_forward(
        msg: &Message,
        mut backend_stream: Option<TcpStream>,
        current_node: Option<String>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Vec<u8>, Option<TcpStream>, Option<String>)> {
        // Determine if this is a write operation (from SQL verb).
        let default_is_write = Self::is_write_message(msg);

        // Plugin Route hook may override the routing decision — force
        // primary/standby, or pin the query to a specific node.
        let route_override = Self::apply_route_hook(msg, state, session);

        // Block short-circuits before any backend selection: synthesise
        // a PG ErrorResponse + ReadyForQuery, hand the existing backend
        // stream and current node back unchanged so the caller can
        // continue the session normally with the next message.
        if let RouteOverride::Block(reason) = route_override {
            let mut response = Vec::with_capacity(64 + reason.len());
            response.extend_from_slice(&Self::create_error_response(
                "42000",
                &format!("Query blocked by route plugin: {}", reason),
            ));
            response.extend_from_slice(&Self::create_ready_for_query(b'I'));
            state
                .metrics
                .bytes_sent
                .fetch_add(response.len() as u64, Ordering::Relaxed);
            return Ok((response, backend_stream, current_node));
        }

        // Derive effective (is_write, forced_target) after override.
        let (is_write, forced_target) = match route_override {
            RouteOverride::None => (default_is_write, None),
            RouteOverride::Primary => (true, None),
            RouteOverride::Standby => (false, None),
            RouteOverride::Node(name) => (default_is_write, Some(name)),
            RouteOverride::Block(_) => unreachable!("handled above"),
        };

        // Sticky session mode: stay on the same backend if healthy and
        // compatible with the routing decision. A forced target shortcuts
        // the usual write-needs-primary check: the only question is whether
        // the current connection already points at the forced node.
        let need_switch = if let Some(ref forced) = forced_target {
            let health = state.health.read().await;
            let reuse = current_node
                .as_ref()
                .map(|c| c == forced && health.get(c).map(|h| h.healthy).unwrap_or(false))
                .unwrap_or(false);
            !reuse
        } else if let Some(ref current) = current_node {
            let health = state.health.read().await;
            let current_healthy = health.get(current).map(|h| h.healthy).unwrap_or(false);

            if !current_healthy {
                true
            } else if is_write {
                // Check if current is primary
                let is_primary = config.nodes.iter()
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

        let target_node = if let Some(forced) = forced_target {
            forced
        } else if need_switch {
            if is_write {
                Self::select_primary_with_timeout(session, state, config).await?
            } else {
                Self::select_read_node(session, state, config).await?
            }
        } else {
            current_node.clone().unwrap()
        };

        let mut backend = if need_switch {
            // Close old connection if any
            drop(backend_stream);

            // Connect to new backend
            let new_backend = tokio::time::timeout(
                config.pool.acquire_timeout(),
                TcpStream::connect(&target_node),
            )
            .await
            .map_err(|_| ProxyError::Connection(format!("Connection timeout to {}", target_node)))?
            .map_err(|e| {
                ProxyError::Connection(format!("Failed to connect to {}: {}", target_node, e))
            })?;

            // Re-authenticate to new backend (silently, without forwarding to client)
            let params = session.variables.read().await.clone();
            let startup = Self::build_startup_message(&params);
            let mut backend = new_backend;
            backend
                .write_all(&startup)
                .await
                .map_err(|e| ProxyError::Network(format!("Backend startup error: {}", e)))?;

            // Complete authentication by reading until ReadyForQuery
            Self::complete_backend_auth(&mut backend).await?;

            tracing::debug!(
                "Switched backend from {:?} to {} for {} query",
                current_node,
                target_node,
                if is_write { "write" } else { "read" }
            );

            backend
        } else {
            backend_stream.unwrap()
        };

        // Forward the message to backend
        let encoded = msg.encode();
        backend
            .write_all(&encoded)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend write error: {}", e)))?;

        // Read response from backend
        let mut response = Vec::new();
        let mut response_buffer = BytesMut::with_capacity(8192);
        let codec = ProtocolCodec::new();

        loop {
            let mut read_buf = vec![0u8; 8192];
            let n = tokio::time::timeout(Duration::from_secs(30), backend.read(&mut read_buf))
                .await
                .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;

            if n == 0 {
                break;
            }

            response.extend_from_slice(&read_buf[..n]);
            response_buffer.extend_from_slice(&read_buf[..n]);

            // Check if we've received ReadyForQuery (end of response)
            while let Some(resp_msg) = codec.decode_message(&mut response_buffer.clone())? {
                if resp_msg.msg_type == MessageType::ReadyForQuery {
                    // Update transaction state
                    if !resp_msg.payload.is_empty() {
                        let status = TransactionStatus::from_byte(resp_msg.payload[0]);
                        let mut tx_state = session.tx_state.write().await;
                        tx_state.in_transaction = status != TransactionStatus::Idle;
                    }
                    return Ok((response, Some(backend), Some(target_node)));
                }
                let _ = codec.decode_message(&mut response_buffer)?;
            }
        }

        Ok((response, Some(backend), Some(target_node)))
    }

    /// Check if a message is a write operation
    fn is_write_message(msg: &Message) -> bool {
        match msg.msg_type {
            MessageType::Query => {
                // Parse query and check if it's a write
                if let Ok(query_msg) = QueryMessage::parse(msg.payload.clone()) {
                    Self::is_write_query(&query_msg.query)
                } else {
                    false
                }
            }
            MessageType::Parse => {
                // Parse prepared statement
                if let Ok(parse_msg) = ParseMessage::parse(msg.payload.clone()) {
                    Self::is_write_query(&parse_msg.query)
                } else {
                    false
                }
            }
            // Execute, Bind, etc. maintain the current connection
            _ => false,
        }
    }

    /// Check if SQL query is a write operation
    fn is_write_query(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();

        // Write operations
        if upper.starts_with("INSERT")
            || upper.starts_with("UPDATE")
            || upper.starts_with("DELETE")
            || upper.starts_with("CREATE")
            || upper.starts_with("DROP")
            || upper.starts_with("ALTER")
            || upper.starts_with("TRUNCATE")
            || upper.starts_with("GRANT")
            || upper.starts_with("REVOKE")
            || upper.starts_with("VACUUM")
            || upper.starts_with("REINDEX")
            || upper.starts_with("CLUSTER")
        {
            return true;
        }

        // Transaction control goes to current node
        if upper.starts_with("BEGIN")
            || upper.starts_with("START")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
            || upper.starts_with("RELEASE")
        {
            return true;
        }

        // SET commands go to primary to maintain session state
        if upper.starts_with("SET") && !upper.starts_with("SET TRANSACTION READ ONLY") {
            return true;
        }

        false
    }

    /// Select primary node with write timeout during failover
    async fn select_primary_with_timeout(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        let timeout = config.write_timeout();
        let start = std::time::Instant::now();
        let check_interval = Duration::from_millis(500);

        loop {
            // Try to find healthy primary
            let health = state.health.read().await;
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
        {
            let tx_state = session.tx_state.read().await;
            if tx_state.in_transaction {
                if let Some(node) = session.current_node.read().await.clone() {
                    return Ok(node);
                }
            }
        }

        // Get healthy nodes (prefer standbys for reads)
        let health = state.health.read().await;
        let healthy_standbys: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| {
                n.enabled
                    && (n.role == NodeRole::Standby || n.role == NodeRole::ReadReplica)
                    && health
                        .get(&n.address())
                        .map(|h| h.healthy)
                        .unwrap_or(false)
            })
            .collect();

        if !healthy_standbys.is_empty() {
            // Round-robin across healthy standbys
            let mut lb_state = state.lb_state.write().await;
            let index = lb_state.rr_counter as usize % healthy_standbys.len();
            lb_state.rr_counter = lb_state.rr_counter.wrapping_add(1);
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
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(4096);
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(ProxyError::Auth("Backend authentication timeout".to_string()));
            }

            let mut read_buf = vec![0u8; 4096];
            let n = tokio::time::timeout(Duration::from_secs(5), backend.read(&mut read_buf))
                .await
                .map_err(|_| ProxyError::Auth("Read timeout during backend auth".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend auth read error: {}", e)))?;

            if n == 0 {
                return Err(ProxyError::Connection("Backend closed during auth".to_string()));
            }

            buffer.extend_from_slice(&read_buf[..n]);

            // Check for complete messages
            loop {
                if buffer.len() < 5 {
                    break;
                }

                // Parse message
                let mut temp_buffer = buffer.clone();
                match codec.decode_message(&mut temp_buffer)? {
                    Some(msg) => {
                        match msg.msg_type {
                            MessageType::ReadyForQuery => {
                                // Authentication complete
                                return Ok(());
                            }
                            MessageType::ErrorResponse => {
                                let err = ErrorResponse::parse(msg.payload)
                                    .map(|e| e.message().unwrap_or("Unknown error").to_string())
                                    .unwrap_or_else(|_| "Parse error".to_string());
                                return Err(ProxyError::Auth(err));
                            }
                            _ => {
                                // Continue reading (AuthRequest, ParameterStatus, BackendKeyData, etc.)
                            }
                        }
                        // Consume the message from actual buffer
                        let _ = codec.decode_message(&mut buffer)?;
                    }
                    None => {
                        // Need more data
                        break;
                    }
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

        let payload: CachedPayload = serde_json::from_slice(bytes).map_err(|e| {
            ProxyError::Protocol(format!("invalid cached payload JSON: {}", e))
        })?;

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
        let query_msg = match QueryMessage::parse(msg.payload.clone()) {
            Ok(q) => q,
            Err(_) => return,
        };
        // Tenant identifier is the most-specific known per-session
        // attribute the proxy can attribute traffic to. Multi-tenancy
        // sets `tenant_id` in `variables`; otherwise we fall back to
        // the client address (string-shaped per-client rate window).
        // session.variables is a tokio RwLock — but record_anomaly is
        // a sync helper. Use try_read so we don't add an await; on
        // contention we fall back to the client IP, which is still a
        // valid per-source identifier.
        let tenant = match session.variables.try_read() {
            Ok(vars) => vars
                .get("tenant_id")
                .or_else(|| vars.get("user"))
                .cloned()
                .unwrap_or_else(|| session.client_addr.ip().to_string()),
            Err(_) => session.client_addr.ip().to_string(),
        };
        let fingerprint = anomaly_fingerprint(&query_msg.query);
        let now = std::time::Instant::now();
        let iso = chrono::Utc::now().to_rfc3339();
        let obs = crate::anomaly::QueryObservation {
            tenant,
            fingerprint,
            sql: query_msg.query,
            timestamp: now,
            iso_timestamp: iso,
        };
        for ev in state.anomaly_detector.record_query(&obs) {
            tracing::warn!(
                anomaly = ?ev,
                "anomaly detected"
            );
        }
    }

    /// Send the client a `Block`-outcome response: an error frame plus
    /// `ReadyForQuery` so the client's state machine returns to idle and
    /// the next query can be accepted.
    async fn send_block_response(
        stream: &mut TcpStream,
        reason: &str,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        let err = Self::create_error_response(
            "42000",
            &format!("Query blocked by plugin: {}", reason),
        );
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
        let mut hook_context = HookContext::default();
        hook_context.client_id = Some(session.id.to_string());
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

    /// Fire post-query hooks after a message has been forwarded (or failed
    /// to forward). Best-effort; errors from individual plugins are logged
    /// by the plugin manager and never surface here.
    #[cfg(feature = "wasm-plugins")]
    fn fire_post_query_hook(
        msg: &Message,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        result: &Result<(Vec<u8>, Option<TcpStream>, Option<String>)>,
        elapsed: Duration,
    ) {
        let pm = match state.plugin_manager.as_ref() {
            Some(pm) => pm,
            None => return,
        };
        if msg.msg_type != MessageType::Query {
            return;
        }
        let query_msg = match QueryMessage::parse(msg.payload.clone()) {
            Ok(q) => q,
            Err(_) => return,
        };
        let ctx = Self::build_query_context(&query_msg.query, session);
        let outcome = match result {
            Ok((resp, _, node)) => PostQueryOutcome {
                success: true,
                target_node: node.clone(),
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: resp.len() as u64,
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
        {
            let tx_state = session.tx_state.read().await;
            if tx_state.in_transaction {
                if let Some(node) = session.current_node.read().await.clone() {
                    return Ok(node);
                }
            }
        }

        // Get healthy nodes
        let health = state.health.read().await;
        let healthy_nodes: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| {
                n.enabled
                    && health
                        .get(&n.address())
                        .map(|h| h.healthy)
                        .unwrap_or(false)
            })
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

    /// Get a connection from the pool
    async fn get_connection(
        node_addr: &str,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<BackendConnection> {
        let pools = state.pools.read().await;
        let pool = pools
            .get(node_addr)
            .ok_or_else(|| ProxyError::Pool(format!("No pool for node: {}", node_addr)))?;

        // Try to get existing connection
        {
            let mut conns = pool.connections.write().await;
            if let Some(conn) = conns.pop() {
                if conn.healthy {
                    pool.active_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(conn);
                }
            }
        }

        // Acquire permit for new connection
        let _permit = pool
            .semaphore
            .acquire()
            .await
            .map_err(|_| ProxyError::Pool("Failed to acquire connection permit".to_string()))?;

        // Create new connection
        let stream = tokio::time::timeout(
            config.pool.acquire_timeout(),
            TcpStream::connect(node_addr),
        )
        .await
        .map_err(|_| ProxyError::Connection(format!("Connection timeout to {}", node_addr)))?
        .map_err(|e| ProxyError::Connection(format!("Failed to connect to {}: {}", node_addr, e)))?;

        let conn = BackendConnection {
            id: Uuid::new_v4(),
            stream: Some(stream),
            created_at: chrono::Utc::now(),
            last_used: chrono::Utc::now(),
            healthy: true,
        };

        pool.active_count.fetch_add(1, Ordering::Relaxed);
        Ok(conn)
    }

    /// Spawn health checker background task
    fn spawn_health_checker(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(config.health.check_interval_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::check_all_nodes(&state, &config).await;
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        })
    }

    /// Check health of all nodes
    async fn check_all_nodes(state: &Arc<ServerState>, config: &ProxyConfig) {
        for node in &config.nodes {
            let result = Self::check_node_health(node, config).await;
            let mut health = state.health.write().await;

            if let Some(node_health) = health.get_mut(&node.address()) {
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
                                node.address(),
                                node_health.failure_count
                            );
                        }
                    }
                }
                node_health.last_check = chrono::Utc::now();
            }
        }
    }

    /// Check health of a single node
    async fn check_node_health(node: &NodeConfig, config: &ProxyConfig) -> Result<f64> {
        let start = std::time::Instant::now();

        let timeout = std::time::Duration::from_secs(config.health.check_timeout_secs);
        let _stream = tokio::time::timeout(timeout, TcpStream::connect(node.address()))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("Timeout connecting to {}", node.address())))?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("Failed to connect to {}: {}", node.address(), e))
            })?;

        // In a real implementation, we would execute the health check query here
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        Ok(latency)
    }

    /// Spawn pool manager background task
    fn spawn_pool_manager(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        let config = self.config.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::cleanup_pools(&state, &config).await;

                        // Also evict idle connections from pool-modes manager
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref pool_manager) = state.pool_manager {
                            pool_manager.evict_idle().await;
                            tracing::trace!("Pool-modes idle eviction completed");
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

    /// Cleanup idle connections from pools
    async fn cleanup_pools(state: &Arc<ServerState>, config: &ProxyConfig) {
        let pools = state.pools.read().await;
        let now = chrono::Utc::now();
        let idle_timeout = chrono::Duration::seconds(config.pool.idle_timeout_secs as i64);

        for pool in pools.values() {
            let mut conns = pool.connections.write().await;
            conns.retain(|conn| now - conn.last_used < idle_timeout);
        }
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

    /// Process SQL statement completion with pool-modes awareness
    #[cfg(feature = "pool-modes")]
    fn process_statement_for_pool_mode(
        pool_manager: &ConnectionPoolManager,
        client_id: &ClientId,
        sql: &str,
    ) -> Option<LeaseAction> {
        // This would be called after a statement completes to determine if
        // the lease should be released based on the pooling mode
        // Note: In a full implementation, we'd need to track leases per-client
        // For now, this is a placeholder for the integration point
        tracing::trace!(
            "Processing statement for pool mode: client={:?}, sql_prefix={}",
            client_id,
            if sql.len() > 50 { &sql[..50] } else { sql }
        );
        None
    }

    /// Get server metrics
    pub fn metrics(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            connections_accepted: self.state.metrics.connections_accepted.load(Ordering::Relaxed),
            connections_closed: self.state.metrics.connections_closed.load(Ordering::Relaxed),
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

    fn test_config() -> ProxyConfig {
        let mut config = ProxyConfig::default();
        config.listen_address = "127.0.0.1:0".to_string();
        config
            .add_node("127.0.0.1:5432", "primary")
            .unwrap();
        config
    }

    #[test]
    fn test_server_creation() {
        let config = test_config();
        let server = ProxyServer::new(config);
        assert!(server.is_ok());
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

        let health = server.state.health.read().await;
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
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: crate::config::TrMode::default(),
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

        let result =
            ProxyServer::apply_authenticate_hook(&params, &session, &server.state).await;
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
        let reply =
            ProxyServer::synthesise_cached_response(payload).expect("synthesis");

        // Walk the concatenation frame-by-frame via length prefixes.
        // Each PG message: tag(1) + length(4, big-endian, includes self) + payload.
        let mut tags = Vec::new();
        let mut i = 0;
        while i < reply.len() {
            let tag = reply[i];
            let len = u32::from_be_bytes([
                reply[i + 1],
                reply[i + 2],
                reply[i + 3],
                reply[i + 4],
            ]) as usize;
            tags.push(tag);
            i += 1 + len;
        }
        assert_eq!(i, reply.len(), "no trailing bytes");
        assert_eq!(
            tags,
            vec![b'T', b'D', b'D', b'C', b'Z'],
            "wire frame order"
        );

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
            pools: RwLock::new(HashMap::new()),
            health: RwLock::new(HashMap::new()),
            metrics: ServerMetrics::default(),
            lb_state: RwLock::new(LoadBalancerState {
                rr_counter: 0,
                weights: HashMap::new(),
                weight_counter: HashMap::new(),
            }),
            #[cfg(feature = "pool-modes")]
            pool_manager: None,
            plugin_manager: Some(pm),
            #[cfg(feature = "ha-tr")]
            transaction_journal: Arc::new(
                crate::transaction_journal::TransactionJournal::new(),
            ),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(
                crate::anomaly::AnomalyDetector::new(
                    crate::anomaly::AnomalyConfig::default(),
                ),
            ),
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
}
