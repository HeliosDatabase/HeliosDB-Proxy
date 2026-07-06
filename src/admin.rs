//! Admin API
//!
//! REST API for proxy management, monitoring, and configuration.
//! Includes HTTP SQL API for transparent write routing (TWR) and load balancing.

#[cfg(feature = "anomaly-detection")]
use crate::anomaly::AnomalyDetector;
use crate::config::{NodeConfig, NodeRole, ProxyConfig};
#[cfg(feature = "edge-proxy")]
use crate::edge::{EdgeCache, EdgeRegistry, InvalidationEvent};
#[cfg(feature = "wasm-plugins")]
use crate::plugins::PluginManager;
#[cfg(feature = "ha-tr")]
use crate::replay::{ReplayEngine, TimeTravelRequest};
use crate::server::{NodeHealth, ServerMetricsSnapshot};
use crate::{ProxyError, Result};
#[cfg(feature = "ha-tr")]
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, RwLock};

/// Static admin UI (vanilla HTML + JS). Compiled into the binary via
/// `include_str!` so deployments are a single binary — no extra file
/// serving or asset bundling. Served at `GET /` and `GET /ui`.
const ADMIN_UI_HTML: &str = include_str!("admin_ui.html");

/// Admin API server
/// Constant-time string comparison (admin token check).
fn constant_time_eq_str(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

pub struct AdminServer {
    /// Listen address
    listen_address: String,
    /// Shared state with proxy
    state: Arc<AdminState>,
    /// Shutdown channel
    shutdown_tx: broadcast::Sender<()>,
}

/// Shared admin state
pub struct AdminState {
    /// Node health status
    pub node_health: RwLock<HashMap<String, NodeHealth>>,
    /// Server metrics
    pub metrics: RwLock<ServerMetricsSnapshot>,
    /// Active sessions count
    pub active_sessions: RwLock<u64>,
    /// Configuration (read-only)
    pub config_snapshot: RwLock<ConfigSnapshot>,
    /// Full proxy config (for SQL routing)
    pub proxy_config: RwLock<Option<ProxyConfig>>,
    /// Round-robin counter for read load balancing
    read_lb_counter: AtomicUsize,
    /// Registered command handlers
    commands: RwLock<HashMap<String, CommandHandler>>,
    /// Connection pool manager (Session/Transaction/Statement modes).
    /// Attached at startup; `/api/pools` returns real per-node pool
    /// stats when present, an empty list otherwise.
    #[cfg(feature = "pool-modes")]
    pub pool_manager: RwLock<Option<Arc<crate::pool::ConnectionPoolManager>>>,
    /// Circuit-breaker manager. Attached at startup; `/api/circuit` reports
    /// each node's live circuit state (closed / open / half-open).
    #[cfg(feature = "circuit-breaker")]
    pub circuit_breaker: RwLock<Option<Arc<crate::circuit_breaker::CircuitBreakerManager>>>,
    /// Time-travel replay engine. Optional so test fixtures don't have
    /// to wire a backend template; production startup attaches it via
    /// `with_replay_engine`. Endpoint returns 503 when missing.
    #[cfg(feature = "ha-tr")]
    pub replay_engine: RwLock<Option<Arc<ReplayEngine>>>,
    /// WASM plugin manager. None when the proxy started without
    /// plugins (or with a different feature set). `/plugins`
    /// endpoint returns 503 when missing; UI panel says "no plugin
    /// manager attached".
    #[cfg(feature = "wasm-plugins")]
    pub plugin_manager: RwLock<Option<Arc<PluginManager>>>,
    /// Chaos-mode overrides: per-node-address marker that the chaos
    /// system (POST /api/chaos) has forced this node to a particular
    /// state. Lets the UI distinguish "operationally disabled" from
    /// "chaos-injected fault" and lets `Reset` restore everything.
    pub chaos_overrides: RwLock<HashMap<String, ChaosOverride>>,
    /// Anomaly detector — same Arc the server populates from the
    /// query path. /api/anomalies polls this for the recent-events
    /// ring buffer.
    #[cfg(feature = "anomaly-detection")]
    pub anomaly_detector: RwLock<Option<Arc<AnomalyDetector>>>,
    /// Query-analytics engine — same Arc the server records on from the query
    /// path. `/api/analytics` reads top queries + slow-query log from it.
    #[cfg(feature = "query-analytics")]
    pub analytics: RwLock<Option<Arc<crate::analytics::QueryAnalytics>>>,
    /// Edge proxy cache + registry. Cache surfaces stats; registry
    /// is the home-side fanout for invalidations.
    #[cfg(feature = "edge-proxy")]
    pub edge_cache: RwLock<Option<Arc<EdgeCache>>>,
    #[cfg(feature = "edge-proxy")]
    pub edge_registry: RwLock<Option<Arc<EdgeRegistry>>>,
    /// Bearer token required on admin requests (except liveness probes).
    /// `None` = open. Set once at startup from `config.admin_token`.
    pub auth_token: RwLock<Option<String>>,
    /// Traffic-mirror / migration info for `/api/migration/status`. `Some`
    /// when `[mirror] enabled`.
    pub migration: RwLock<Option<MigrationInfo>>,
    /// Branch-database config for `/api/branch`. `Some` when `[branch]
    /// enabled`.
    pub branch: RwLock<Option<crate::config::BranchConfig>>,
}

/// What the admin API needs to report migration status, without owning the
/// mirror worker.
#[derive(Clone)]
pub struct MigrationInfo {
    pub target: String,
    pub writes_only: bool,
    pub metrics: Arc<crate::mirror::MirrorMetrics>,
    /// Mirror config (source + target) for snapshot bootstrap.
    pub config: crate::config::MirrorConfig,
    /// The proxy's cutover switch and the target to promote to.
    pub cutover: Arc<arc_swap::ArcSwap<Option<Arc<crate::mirror::CutoverTarget>>>>,
    pub cutover_target: crate::mirror::CutoverTarget,
}

/// Chaos override applied to a single node. Today only the
/// `ForceUnhealthy` flavour is implemented — `inject_query_delay`
/// is the natural follow-up but wants per-query interception that
/// lives in the server message loop, not here.
#[derive(Debug, Clone, Serialize)]
pub struct ChaosOverride {
    /// Wall-clock when the override was applied (RFC 3339).
    pub since: String,
    /// "force_unhealthy" | "delay_ms"
    pub kind: String,
    /// Free-form description shown in admin UI.
    pub note: String,
}

/// Command handler type
type CommandHandler = Arc<dyn Fn(&[&str]) -> Result<String> + Send + Sync>;

/// Configuration snapshot for admin API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSnapshot {
    pub listen_address: String,
    pub admin_address: String,
    pub tr_enabled: bool,
    pub tr_mode: String,
    pub pool_min_connections: usize,
    pub pool_max_connections: usize,
    pub nodes: Vec<NodeSnapshot>,
}

/// Node configuration snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSnapshot {
    pub address: String,
    pub role: String,
    pub weight: u32,
    pub enabled: bool,
}

impl AdminServer {
    /// Create a new admin server
    pub fn new(listen_address: String, state: Arc<AdminState>) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            listen_address,
            state,
            shutdown_tx,
        }
    }

    /// Run the admin server
    pub async fn run(&self) -> Result<()> {
        // SO_REUSEPORT like the client listener, so a binary handoff can re-bind
        // the admin address concurrently while the old process drains (Batch H).
        let listener = crate::server::bind_reuseport(&self.listen_address)?;

        tracing::info!(
            "Admin API listening on {} (SO_REUSEPORT)",
            self.listen_address
        );

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        // Bound concurrent admin connections so a flood can't spawn unbounded
        // tasks (each may buffer up to the body cap). Excess connections are
        // dropped rather than queued.
        let conn_limit = std::sync::Arc::new(tokio::sync::Semaphore::new(Self::MAX_ADMIN_CONNS));

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let permit = match conn_limit.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    tracing::warn!(%addr, "admin connection limit reached; dropping");
                                    drop(stream);
                                    continue;
                                }
                            };
                            let state = self.state.clone();
                            tokio::spawn(async move {
                                let _permit = permit; // released when the connection ends
                                if let Err(e) = Self::handle_connection(stream, addr, state).await {
                                    tracing::error!("Admin connection error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Admin accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Admin server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Overall deadline for reading one admin request (headers + body). Bounds
    /// slow-loris clients on the default-open admin listener.
    /// Max concurrent admin connections; excess are dropped, not queued.
    const MAX_ADMIN_CONNS: usize = 256;
    const ADMIN_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
    /// Bound on every SSE write+flush (preamble, event frame,
    /// heartbeat), mirroring the data path's CLIENT_WRITE_TIMEOUT
    /// convention: a subscriber that stops reading must be reaped, not
    /// pin its admin task + connection permit forever. Comfortably
    /// above one 15s heartbeat interval.
    #[cfg(feature = "edge-proxy")]
    const ADMIN_SSE_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    /// Max number of header lines accepted per admin request.
    const MAX_ADMIN_HEADERS: usize = 100;
    /// Max total bytes of the header section.
    const MAX_ADMIN_HEADER_BYTES: usize = 64 * 1024;
    /// Max admin request body. Admin payloads (config fragments, replay windows)
    /// are small; this bounds the `vec![0u8; content_length]` allocation.
    const MAX_ADMIN_BODY_BYTES: usize = 8 * 1024 * 1024;

    /// Handle an admin connection
    async fn handle_connection(
        mut stream: TcpStream,
        addr: SocketAddr,
        state: Arc<AdminState>,
    ) -> Result<()> {
        tracing::debug!("Admin connection from {}", addr);

        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        // Read HTTP request headers
        let mut headers = Vec::new();
        let mut content_length: usize = 0;
        let mut header_bytes: usize = 0;

        loop {
            line.clear();
            // Bound the whole request-read phase in time so a slow-loris client
            // dribbling bytes cannot pin this admin task indefinitely. The admin
            // listener is open by default, so this is unauthenticated input.
            let bytes_read =
                match tokio::time::timeout(Self::ADMIN_READ_TIMEOUT, reader.read_line(&mut line))
                    .await
                {
                    Ok(r) => r.map_err(|e| ProxyError::Network(format!("Read error: {}", e)))?,
                    Err(_) => return Ok(()), // read timeout — drop the connection
                };

            if bytes_read == 0 || line == "\r\n" {
                break;
            }

            // Bound header size + count so a client streaming header lines
            // forever cannot grow memory without limit (unauthenticated DoS).
            header_bytes += bytes_read;
            if headers.len() >= Self::MAX_ADMIN_HEADERS
                || header_bytes > Self::MAX_ADMIN_HEADER_BYTES
            {
                Self::send_response(
                    &mut writer,
                    431,
                    "Request Header Fields Too Large",
                    "header section too large",
                )
                .await?;
                return Ok(());
            }

            // Parse Content-Length header
            let trimmed = line.trim();
            if trimmed.to_lowercase().starts_with("content-length:") {
                if let Some(len_str) = trimmed.split(':').nth(1) {
                    content_length = len_str.trim().parse().unwrap_or(0);
                }
            }
            headers.push(trimmed.to_string());
        }

        if headers.is_empty() {
            return Ok(());
        }

        // Reject an oversized declared body BEFORE allocating for it: the old
        // `vec![0u8; content_length]` would zero-fill an attacker-chosen size
        // (e.g. `Content-Length: 99999999999`) and OOM the whole process on the
        // default-open admin port.
        if content_length > Self::MAX_ADMIN_BODY_BYTES {
            Self::send_response(
                &mut writer,
                413,
                "Payload Too Large",
                "request body exceeds admin size limit",
            )
            .await?;
            return Ok(());
        }

        // Parse request line
        let request_line = &headers[0];
        let parts: Vec<&str> = request_line.split_whitespace().collect();

        if parts.len() < 2 {
            Self::send_response(&mut writer, 400, "Bad Request", "Invalid request line").await?;
            return Ok(());
        }

        let method = parts[0];
        let path = parts[1];

        // Bearer-token gate. Liveness probes stay open so orchestrators can
        // health-check without the token; everything else is rejected with
        // 401 unless `Authorization: Bearer <token>` matches.
        {
            let required = state.auth_token.read().await.clone();
            if let Some(token) = required {
                let path_only = path.split('?').next().unwrap_or(path);
                let is_liveness = method == "GET"
                    && matches!(path_only, "/health" | "/healthz" | "/livez" | "/readyz");
                if !is_liveness && !Self::admin_authorized(&headers, &token) {
                    Self::send_response(
                        &mut writer,
                        401,
                        "Unauthorized",
                        "{\"error\":\"missing or invalid admin bearer token\"}",
                    )
                    .await?;
                    return Ok(());
                }
            }
        }

        // Long-lived SSE subscription for edge invalidations (T3.2, H5).
        // Intercepted here — before the one-shot `route_request` dispatch —
        // because `send_json_response` frames with `Content-Length` +
        // `Connection: close`, which can't hold a stream open. The admin
        // bearer gate above has already run, so an unauthenticated
        // subscribe gets the exact same 401 as any other protected route.
        // ADMIN_READ_TIMEOUT bounded only the request-read phase; the held
        // SSE response is deliberately unbounded in time, but every WRITE
        // on it is bounded by ADMIN_SSE_WRITE_TIMEOUT — with the 15s
        // heartbeat that caps a wedged subscriber's lifetime, so held
        // MAX_ADMIN_CONNS permits can never exceed live subscribers
        // (`max_edges`, far below the 256-connection cap) for long.
        #[cfg(feature = "edge-proxy")]
        if method == "GET" && path.split('?').next().unwrap_or(path) == "/api/edge/subscribe" {
            let params = parse_query_params(path);
            let edge_id = params.get("edge_id").map(String::as_str).unwrap_or("");
            if edge_id.is_empty() {
                Self::send_json_response(
                    &mut writer,
                    400,
                    &serde_json::json!({ "error": "edge_id query parameter is required" }),
                )
                .await?;
                return Ok(());
            }
            let region = params.get("region").map(String::as_str).unwrap_or("");
            let base_url = params.get("base_url").map(String::as_str).unwrap_or("");
            return Self::handle_edge_subscribe(&mut writer, &state, edge_id, region, base_url)
                .await;
        }

        // Read request body for POST/PUT requests (size already bounded above).
        let body = if content_length > 0 && (method == "POST" || method == "PUT") {
            let mut body_buf = vec![0u8; content_length];
            match tokio::time::timeout(Self::ADMIN_READ_TIMEOUT, reader.read_exact(&mut body_buf))
                .await
            {
                Ok(r) => {
                    r.map_err(|e| ProxyError::Network(format!("Body read error: {}", e)))?;
                }
                Err(_) => return Ok(()), // body read timeout — drop the connection
            }
            Some(String::from_utf8_lossy(&body_buf).to_string())
        } else {
            None
        };

        // Static admin UI — single HTML file compiled into the binary.
        // Served at `/` and `/ui`; all other routes remain JSON.
        if method == "GET" && (path == "/" || path == "/ui" || path == "/ui/") {
            Self::send_html_response(&mut writer, 200, ADMIN_UI_HTML).await?;
            return Ok(());
        }

        // Route request
        let response = Self::route_request(method, path, body.as_deref(), &state).await;

        match response {
            Ok((status, body)) => {
                Self::send_json_response(&mut writer, status, &body).await?;
            }
            Err(e) => {
                let error = ErrorResponse {
                    error: e.to_string(),
                };
                Self::send_json_response(&mut writer, 500, &error).await?;
            }
        }

        Ok(())
    }

    /// True if the request carries `Authorization: Bearer <token>` matching
    /// the configured admin token (constant-time compare).
    fn admin_authorized(headers: &[String], token: &str) -> bool {
        let expected = format!("Bearer {}", token);
        for h in headers {
            let mut sp = h.splitn(2, ':');
            let name = sp.next().unwrap_or("").trim();
            if name.eq_ignore_ascii_case("authorization") {
                let value = sp.next().unwrap_or("").trim();
                return constant_time_eq_str(value, &expected);
            }
        }
        false
    }

    /// Serve a text/html HTTP response. Used by the admin UI route.
    async fn send_html_response(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        status: u16,
        html: &str,
    ) -> Result<()> {
        let status_text = match status {
            200 => "OK",
            404 => "Not Found",
            _ => "Unknown",
        };
        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            status_text,
            html.len(),
            html
        );
        writer
            .write_all(response.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        Ok(())
    }

    /// Route a request to the appropriate handler
    async fn route_request(
        method: &str,
        path: &str,
        body: Option<&str>,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        match (method, path) {
            // SQL API - Execute SQL with TWR (Transparent Write Routing)
            ("POST", "/api/sql") => Self::handle_sql_request(body, state).await,

            // Health endpoints
            ("GET", "/health") => {
                let health = HealthResponse { status: "ok" };
                Ok((200, serde_json::to_value(health)?))
            }
            ("GET", "/health/ready") => {
                let ready = Self::check_readiness(state).await;
                let response = ReadinessResponse {
                    ready,
                    message: if ready {
                        "Proxy is ready"
                    } else {
                        "Proxy is not ready"
                    },
                };
                let status = if ready { 200 } else { 503 };
                Ok((status, serde_json::to_value(response)?))
            }
            ("GET", "/health/live") => {
                let response = LivenessResponse { alive: true };
                Ok((200, serde_json::to_value(response)?))
            }

            // Metrics
            ("GET", "/metrics") => {
                let metrics = state.metrics.read().await.clone();
                Ok((200, serde_json::to_value(MetricsResponse::from(metrics))?))
            }
            ("GET", "/metrics/prometheus") => {
                let metrics = state.metrics.read().await.clone();
                let prometheus = Self::format_prometheus_metrics(&metrics);
                Ok((200, serde_json::json!({ "text": prometheus })))
            }

            // Node management
            ("GET", "/nodes") => {
                let health = state.node_health.read().await;
                let nodes: Vec<NodeHealthResponse> = health
                    .values()
                    .map(|h| NodeHealthResponse::from(h.clone()))
                    .collect();
                Ok((200, serde_json::to_value(nodes)?))
            }
            ("GET", path) if path.starts_with("/nodes/") => {
                let node_addr = path.trim_start_matches("/nodes/");
                let health = state.node_health.read().await;
                match health.get(node_addr) {
                    Some(h) => Ok((
                        200,
                        serde_json::to_value(NodeHealthResponse::from(h.clone()))?,
                    )),
                    None => Ok((404, serde_json::json!({ "error": "Node not found" }))),
                }
            }
            ("POST", path) if path.starts_with("/nodes/") && path.ends_with("/enable") => {
                let node_addr = path
                    .trim_start_matches("/nodes/")
                    .trim_end_matches("/enable");
                Self::set_node_enabled(state, node_addr, true).await?;
                Ok((200, serde_json::json!({ "status": "enabled" })))
            }
            ("POST", path) if path.starts_with("/nodes/") && path.ends_with("/disable") => {
                let node_addr = path
                    .trim_start_matches("/nodes/")
                    .trim_end_matches("/disable");
                Self::set_node_enabled(state, node_addr, false).await?;
                Ok((200, serde_json::json!({ "status": "disabled" })))
            }

            // Topology — joins config (role) with node_health (healthy)
            // so external controllers (operator, ops dashboards) can
            // populate `currentPrimary` / `healthyNodes` /
            // `unhealthyNodes` in one round-trip. Designed for
            // poll-friendly use; never blocks.
            ("GET", "/topology") => {
                let topo = Self::compute_topology(state).await;
                Ok((200, serde_json::to_value(topo)?))
            }

            // Time-travel replay — replays a journal window against a
            // target backend (typically a staging DB). Body shape is
            // `ReplayRequestBody` below.
            #[cfg(feature = "ha-tr")]
            ("POST", "/api/replay") => Self::handle_replay_request(body, state).await,
            #[cfg(not(feature = "ha-tr"))]
            ("POST", "/api/replay") => Ok((
                503,
                serde_json::json!({ "error": "ha-tr feature not compiled in" }),
            )),

            // Shadow execution (T3.4) — runs a query against a source
            // backend AND a shadow backend, diffs the result. Used for
            // major-version upgrade validation, schema-migration
            // canaries, replica-drift detection. Body is
            // `ShadowRequestBody`.
            #[cfg(feature = "ha-tr")]
            ("POST", "/api/shadow") => Self::handle_shadow_request(body).await,
            #[cfg(not(feature = "ha-tr"))]
            ("POST", "/api/shadow") => Ok((
                503,
                serde_json::json!({ "error": "ha-tr feature not compiled in" }),
            )),

            // Loaded WASM plugins — name, version, hooks, state,
            // invocation count. Returns 503 when no plugin manager
            // is attached (proxy started without --features
            // wasm-plugins or with plugins disabled in config).
            ("GET", "/plugins") => Self::handle_plugins_list(state).await,

            // Anomaly detector recent-events feed (T3.1). Optional
            // ?limit query string clamps the response size; default
            // is 100 events newest-first.
            #[cfg(feature = "anomaly-detection")]
            ("GET", p) if p == "/anomalies" || p.starts_with("/anomalies?") => {
                Self::handle_anomalies_list(p, state).await
            }
            #[cfg(not(feature = "anomaly-detection"))]
            ("GET", p) if p == "/anomalies" || p.starts_with("/anomalies?") => Ok((
                503,
                serde_json::json!({ "error": "anomaly-detection feature not compiled in" }),
            )),

            // Query analytics: top queries by call count + slow-query log.
            #[cfg(feature = "query-analytics")]
            ("GET", p)
                if p == "/api/analytics"
                    || p == "/analytics"
                    || p.starts_with("/api/analytics?")
                    || p.starts_with("/analytics?") =>
            {
                Self::handle_analytics(p, state).await
            }
            #[cfg(not(feature = "query-analytics"))]
            ("GET", p)
                if p == "/api/analytics"
                    || p == "/analytics"
                    || p.starts_with("/api/analytics?")
                    || p.starts_with("/analytics?") =>
            {
                Ok((
                    503,
                    serde_json::json!({ "error": "query-analytics feature not compiled in" }),
                ))
            }

            // Edge mode (T3.2). Stats panel for the home; the home's
            // registered edges + cache stats; and a manual
            // invalidation endpoint for ops drills. (The live SSE
            // stream, `GET /api/edge/subscribe`, never reaches this
            // dispatch — `handle_connection` intercepts it first,
            // since the one-shot JSON writers here can't hold a
            // stream open.)
            #[cfg(feature = "edge-proxy")]
            ("GET", "/api/edge") => Self::handle_edge_status(state).await,
            #[cfg(feature = "edge-proxy")]
            ("POST", "/api/edge/register") => Self::handle_edge_register(body, state).await,
            #[cfg(feature = "edge-proxy")]
            ("POST", "/api/edge/invalidate") => Self::handle_edge_invalidate(body, state).await,
            #[cfg(not(feature = "edge-proxy"))]
            ("GET", "/api/edge")
            | ("POST", "/api/edge/register")
            | ("POST", "/api/edge/invalidate") => Ok((
                503,
                serde_json::json!({ "error": "edge-proxy feature not compiled in" }),
            )),
            // Without the feature the subscribe intercept above is compiled
            // out, so the SSE path falls through to here: same 503 as its
            // sibling edge routes (query string included in the match).
            #[cfg(not(feature = "edge-proxy"))]
            ("GET", p) if p == "/api/edge/subscribe" || p.starts_with("/api/edge/subscribe?") => {
                Ok((
                    503,
                    serde_json::json!({ "error": "edge-proxy feature not compiled in" }),
                ))
            }

            // Chaos engineering — controlled fault injection for HA
            // testing. Body is `ChaosRequestBody`; supported actions
            // are `force_unhealthy` / `restore` / `reset`.
            ("POST", "/api/chaos") => Self::handle_chaos_request(body, state).await,
            // Read current overrides so the UI can show "what's
            // currently broken on purpose".
            ("GET", "/api/chaos") => {
                let overrides = state.chaos_overrides.read().await.clone();
                Ok((200, serde_json::to_value(overrides)?))
            }

            // Live per-node circuit-breaker state (closed / open / half-open)
            // so an operator can see which backends the breaker has tripped.
            #[cfg(feature = "circuit-breaker")]
            ("GET", "/api/circuit") => Self::handle_circuit_status(state).await,
            #[cfg(not(feature = "circuit-breaker"))]
            ("GET", "/api/circuit") => Ok((
                503,
                serde_json::json!({ "error": "circuit-breaker feature not enabled" }),
            )),

            // Migration / traffic-mirror status
            ("GET", "/api/migration/status") | ("GET", "/migration/status") => {
                match state.migration.read().await.as_ref() {
                    Some(info) => {
                        let st =
                            crate::mirror::status(&info.target, info.writes_only, &info.metrics);
                        let mut v = serde_json::to_value(st)?;
                        let cut = info.cutover.load_full().is_some();
                        v["cutover_active"] = serde_json::json!(cut);
                        Ok((200, v))
                    }
                    None => Ok((
                        503,
                        serde_json::json!({ "error": "traffic mirroring not enabled" }),
                    )),
                }
            }

            // Promote the mirror target to primary: new connections route there.
            ("POST", "/api/migration/cutover") | ("POST", "/migration/cutover") => {
                let info = state.migration.read().await.clone();
                let Some(info) = info else {
                    return Ok((
                        503,
                        serde_json::json!({ "error": "traffic mirroring not enabled" }),
                    ));
                };
                let force = path.contains("force=true")
                    || body.map(|b| b.contains("\"force\":true")).unwrap_or(false);
                let st = crate::mirror::status(&info.target, info.writes_only, &info.metrics);
                if !st.migration_ready && !force {
                    return Ok((
                        409,
                        serde_json::json!({
                            "ok": false,
                            "error": "not migration_ready (backlog/drops present); pass force=true to override",
                            "status": st,
                        }),
                    ));
                }
                info.cutover
                    .store(Arc::new(Some(Arc::new(info.cutover_target.clone()))));
                tracing::warn!(target = %info.cutover_target.addr, "migration cutover: new connections now route to the promoted target");
                Ok((
                    200,
                    serde_json::json!({ "ok": true, "promoted_to": info.cutover_target.addr }),
                ))
            }

            // Roll a cutover back to the original primary.
            ("POST", "/api/migration/cutover/rollback")
            | ("POST", "/migration/cutover/rollback") => {
                let info = state.migration.read().await.clone();
                let Some(info) = info else {
                    return Ok((
                        503,
                        serde_json::json!({ "error": "traffic mirroring not enabled" }),
                    ));
                };
                info.cutover.store(Arc::new(None));
                Ok((200, serde_json::json!({ "ok": true, "rolled_back": true })))
            }

            // Snapshot-bootstrap named tables from the source into the mirror.
            ("POST", "/api/migration/snapshot") | ("POST", "/migration/snapshot") => {
                let info = state.migration.read().await.clone();
                let Some(info) = info else {
                    return Ok((
                        503,
                        serde_json::json!({ "error": "traffic mirroring not enabled" }),
                    ));
                };
                let body = body.unwrap_or("{}");
                let req: serde_json::Value = serde_json::from_str(body)
                    .map_err(|e| ProxyError::Internal(format!("invalid JSON: {}", e)))?;
                let tables: Vec<String> = req
                    .get("tables")
                    .and_then(|t| t.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                if tables.is_empty() {
                    return Ok((
                        400,
                        serde_json::json!({ "error": "provide a non-empty 'tables' array" }),
                    ));
                }
                match crate::mirror::snapshot_tables(&info.config, &tables).await {
                    Ok(rep) => {
                        let total: u64 = rep.iter().map(|t| t.copied).sum();
                        Ok((
                            200,
                            serde_json::json!({ "ok": true, "tables": rep, "rows_copied": total }),
                        ))
                    }
                    Err(e) => Ok((500, serde_json::json!({ "ok": false, "error": e }))),
                }
            }

            // Branch databases: list / create / drop.
            ("GET", p) if p == "/api/branch" || p == "/branch" || p.starts_with("/api/branch?") => {
                let cfg = state.branch.read().await.clone();
                let Some(cfg) = cfg else {
                    return Ok((
                        503,
                        serde_json::json!({ "error": "branch databases not enabled" }),
                    ));
                };
                match crate::branch::list(&cfg).await {
                    Ok(branches) => Ok((200, serde_json::json!({ "branches": branches }))),
                    Err(e) => Ok((500, serde_json::json!({ "error": e }))),
                }
            }
            ("POST", p) if p == "/api/branch" || p == "/branch" => {
                let cfg = state.branch.read().await.clone();
                let Some(cfg) = cfg else {
                    return Ok((
                        503,
                        serde_json::json!({ "error": "branch databases not enabled" }),
                    ));
                };
                let req: serde_json::Value = serde_json::from_str(body.unwrap_or("{}"))
                    .map_err(|e| ProxyError::Internal(format!("invalid JSON: {}", e)))?;
                let name = req.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if name.is_empty() {
                    return Ok((400, serde_json::json!({ "error": "provide 'name'" })));
                }
                let base = req.get("base").and_then(|v| v.as_str());
                match crate::branch::create(&cfg, name, base).await {
                    Ok(()) => Ok((
                        200,
                        serde_json::json!({ "ok": true, "branch": name,
                        "base": base.unwrap_or(&cfg.base_database) }),
                    )),
                    Err(e) => Ok((500, serde_json::json!({ "ok": false, "error": e }))),
                }
            }
            ("DELETE", p) if p.starts_with("/api/branch") || p.starts_with("/branch") => {
                let cfg = state.branch.read().await.clone();
                let Some(cfg) = cfg else {
                    return Ok((
                        503,
                        serde_json::json!({ "error": "branch databases not enabled" }),
                    ));
                };
                let name = p.find("name=").map(|i| &p[i + 5..]).unwrap_or("");
                if name.is_empty() {
                    return Ok((
                        400,
                        serde_json::json!({ "error": "provide ?name=<branch>" }),
                    ));
                }
                match crate::branch::drop(&cfg, name).await {
                    Ok(()) => Ok((200, serde_json::json!({ "ok": true, "dropped": name }))),
                    Err(e) => Ok((500, serde_json::json!({ "ok": false, "error": e }))),
                }
            }

            // Configuration
            ("GET", "/config") => {
                let config = state.config_snapshot.read().await.clone();
                Ok((200, serde_json::to_value(config)?))
            }

            // Sessions
            ("GET", "/sessions") => {
                let count = *state.active_sessions.read().await;
                let response = SessionsResponse {
                    active_sessions: count,
                };
                Ok((200, serde_json::to_value(response)?))
            }

            // Pools
            ("GET", "/pools") => {
                let pools = Self::get_pool_stats(state).await;
                Ok((200, serde_json::to_value(pools)?))
            }

            // Version
            ("GET", "/version") => {
                let response = VersionResponse {
                    version: crate::VERSION.to_string(),
                    build_time: env!("CARGO_PKG_VERSION").to_string(),
                };
                Ok((200, serde_json::to_value(response)?))
            }

            // Not found
            _ => Ok((404, serde_json::json!({ "error": "Not found" }))),
        }
    }

    /// Handle SQL execution request with TWR (Transparent Write Routing)
    async fn handle_sql_request(
        body: Option<&str>,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        // Parse request body
        let body = body.ok_or_else(|| ProxyError::Internal("Missing request body".to_string()))?;
        let request: SqlRequest = serde_json::from_str(body)
            .map_err(|e| ProxyError::Internal(format!("Invalid JSON: {}", e)))?;

        let sql = request.query.trim();
        if sql.is_empty() {
            return Ok((400, serde_json::json!({ "error": "Empty query" })));
        }

        // Classify query as read or write
        let is_write = Self::is_write_query(sql);
        let query_type = if is_write { "write" } else { "read" };

        // Get proxy config
        let proxy_config = state.proxy_config.read().await;
        let config = proxy_config
            .as_ref()
            .ok_or_else(|| ProxyError::Internal("Proxy config not initialized".to_string()))?;

        // Get node health
        let health = state.node_health.read().await;

        // Select target node based on query type
        let target_node = if is_write {
            // Write queries always go to primary
            Self::select_primary_node(config, &health)?
        } else {
            // Read queries can go to any healthy node with load balancing
            Self::select_read_node(config, &health, state)?
        };

        let target_address = format!("{}:{}", target_node.host, target_node.port);
        // Use HTTP port from node config (defaults to 8080)
        let http_port = target_node.http_port;
        let http_url = format!("http://{}:{}/api/sql", target_node.host, http_port);

        tracing::debug!(
            "Routing {} query to {} ({})",
            query_type,
            target_address,
            match target_node.role {
                NodeRole::Primary => "primary",
                NodeRole::Standby => "standby",
                NodeRole::ReadReplica => "replica",
            }
        );

        // Forward request to backend node
        let result = Self::forward_sql_request(&http_url, sql).await?;

        // Return result with routing metadata
        let response = SqlResponse {
            query_type: query_type.to_string(),
            routed_to: target_address,
            node_role: format!("{:?}", target_node.role).to_lowercase(),
            result,
        };

        Ok((200, serde_json::to_value(response)?))
    }

    /// Determine if a query is a write operation
    fn is_write_query(sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();

        // Write operations
        if upper.starts_with("INSERT")
            || upper.starts_with("UPDATE")
            || upper.starts_with("DELETE")
            || upper.starts_with("CREATE")
            || upper.starts_with("ALTER")
            || upper.starts_with("DROP")
            || upper.starts_with("TRUNCATE")
            || upper.starts_with("GRANT")
            || upper.starts_with("REVOKE")
            || upper.starts_with("VACUUM")
            || upper.starts_with("REINDEX")
            || upper.starts_with("MERGE")
            || upper.starts_with("UPSERT")
        {
            return true;
        }

        // Transaction control that might contain writes
        if upper.starts_with("BEGIN")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
        {
            // Transaction control goes to primary for safety
            return true;
        }

        // Read operations
        false
    }

    /// Select primary node for write queries
    fn select_primary_node<'a>(
        config: &'a ProxyConfig,
        health: &HashMap<String, NodeHealth>,
    ) -> Result<&'a NodeConfig> {
        config
            .nodes
            .iter()
            .find(|n| {
                n.role == NodeRole::Primary
                    && n.enabled
                    && health.get(&n.address()).map(|h| h.healthy).unwrap_or(false)
            })
            .ok_or_else(|| ProxyError::Internal("No healthy primary node available".to_string()))
    }

    /// Select node for read queries with load balancing
    fn select_read_node<'a>(
        config: &'a ProxyConfig,
        health: &HashMap<String, NodeHealth>,
        state: &AdminState,
    ) -> Result<&'a NodeConfig> {
        // Get all healthy nodes (primary, standby, or replica)
        let healthy_nodes: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| n.enabled && health.get(&n.address()).map(|h| h.healthy).unwrap_or(false))
            .collect();

        if healthy_nodes.is_empty() {
            return Err(ProxyError::Internal(
                "No healthy nodes available".to_string(),
            ));
        }

        // If read/write splitting is enabled and there are standbys, prefer them
        if config.load_balancer.read_write_split {
            let read_nodes: Vec<&NodeConfig> = healthy_nodes
                .iter()
                .filter(|n| n.role == NodeRole::Standby || n.role == NodeRole::ReadReplica)
                .copied()
                .collect();

            if !read_nodes.is_empty() {
                // Round-robin across read nodes
                let counter = state.read_lb_counter.fetch_add(1, Ordering::Relaxed);
                let index = counter % read_nodes.len();
                return Ok(read_nodes[index]);
            }
        }

        // Fall back to round-robin across all healthy nodes
        let counter = state.read_lb_counter.fetch_add(1, Ordering::Relaxed);
        let index = counter % healthy_nodes.len();
        Ok(healthy_nodes[index])
    }

    /// Forward SQL request to backend node's HTTP API
    async fn forward_sql_request(url: &str, sql: &str) -> Result<serde_json::Value> {
        // Build HTTP request
        let request_body = serde_json::json!({ "query": sql });
        let body_bytes = serde_json::to_vec(&request_body)
            .map_err(|e| ProxyError::Internal(format!("JSON serialization error: {}", e)))?;

        // Parse URL
        let url_parts: Vec<&str> = url.trim_start_matches("http://").splitn(2, '/').collect();
        if url_parts.is_empty() {
            return Err(ProxyError::Internal("Invalid URL".to_string()));
        }

        let host_port = url_parts[0];
        let path = if url_parts.len() > 1 {
            format!("/{}", url_parts[1])
        } else {
            "/".to_string()
        };

        // Connect to backend
        let stream = TcpStream::connect(host_port).await.map_err(|e| {
            ProxyError::Network(format!("Failed to connect to {}: {}", host_port, e))
        })?;

        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        // Send HTTP request
        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            path,
            host_port,
            body_bytes.len()
        );

        writer
            .write_all(request.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        writer
            .write_all(&body_bytes)
            .await
            .map_err(|e| ProxyError::Network(format!("Write body error: {}", e)))?;

        // Read response headers
        let mut response_headers = Vec::new();
        let mut line = String::new();
        let mut content_length: usize = 0;

        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .map_err(|e| ProxyError::Network(format!("Response read error: {}", e)))?;

            if bytes_read == 0 || line == "\r\n" {
                break;
            }

            let trimmed = line.trim();
            if trimmed.to_lowercase().starts_with("content-length:") {
                if let Some(len_str) = trimmed.split(':').nth(1) {
                    content_length = len_str.trim().parse().unwrap_or(0);
                }
            }
            response_headers.push(trimmed.to_string());
        }

        // Read response body
        let mut body_buf = vec![0u8; content_length];
        if content_length > 0 {
            reader
                .read_exact(&mut body_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("Response body read error: {}", e)))?;
        }

        let response_body = String::from_utf8_lossy(&body_buf);

        // Parse JSON response
        serde_json::from_str(&response_body).map_err(|e| {
            ProxyError::Internal(format!(
                "Invalid JSON response: {} - body: {}",
                e, response_body
            ))
        })
    }

    /// Check if proxy is ready to accept connections
    async fn check_readiness(state: &Arc<AdminState>) -> bool {
        let health = state.node_health.read().await;

        // Need at least one healthy primary
        health.values().any(|h| h.healthy)
    }

    /// Set node enabled status
    async fn set_node_enabled(
        state: &Arc<AdminState>,
        node_addr: &str,
        enabled: bool,
    ) -> Result<()> {
        let mut health = state.node_health.write().await;

        if let Some(node_health) = health.get_mut(node_addr) {
            node_health.healthy = enabled;
            Ok(())
        } else {
            Err(ProxyError::Config(format!("Node not found: {}", node_addr)))
        }
    }

    /// Get pool statistics
    async fn get_pool_stats(_state: &Arc<AdminState>) -> Vec<PoolStatsResponse> {
        // Real per-node pool stats from the attached pool manager. Returns
        // an empty list when pool-modes is off or no manager is attached.
        #[cfg(feature = "pool-modes")]
        if let Some(mgr) = _state.pool_manager.read().await.clone() {
            let stats = mgr.get_stats().await;
            return stats
                .node_stats
                .into_iter()
                .map(|ns| PoolStatsResponse {
                    node: ns.node_id.0.to_string(),
                    active_connections: ns.active as u64,
                    idle_connections: ns.idle as u64,
                    // Per-node pending/created/closed counters are not tracked
                    // separately by the manager; total is the live pool size.
                    pending_requests: 0,
                    total_connections_created: ns.total as u64,
                    total_connections_closed: 0,
                })
                .collect();
        }
        Vec::new()
    }

    /// Handle `POST /api/replay`. Body is a JSON `ReplayRequestBody`.
    /// Returns 503 when no replay engine is attached, 400 on a malformed
    /// body or inverted window, 200 with `ReplaySummary` on success.
    #[cfg(feature = "ha-tr")]
    async fn handle_replay_request(
        body: Option<&str>,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        let raw =
            body.ok_or_else(|| ProxyError::Internal("replay: empty request body".to_string()))?;
        let req: ReplayRequestBody = match serde_json::from_str(raw) {
            Ok(r) => r,
            Err(e) => {
                return Ok((
                    400,
                    serde_json::json!({ "error": format!("invalid body: {}", e) }),
                ));
            }
        };
        let engine = match state.replay_engine.read().await.clone() {
            Some(e) => e,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "replay engine not attached" }),
                ));
            }
        };
        let tt = TimeTravelRequest {
            from: req.from,
            to: req.to,
            target_host: req.target_host,
            target_port: req.target_port,
            target_user: req.target_user,
            target_password: req.target_password,
            target_database: req.target_database,
        };
        match engine.replay_window(&tt).await {
            Ok(summary) => Ok((200, serde_json::to_value(summary)?)),
            Err(e) => Ok((
                500,
                serde_json::json!({ "error": format!("replay failed: {}", e) }),
            )),
        }
    }

    /// `GET /api/edge` — surfaces edge-mode state: cache stats +
    /// the list of registered edges (when running in home mode).
    #[cfg(feature = "edge-proxy")]
    async fn handle_edge_status(state: &Arc<AdminState>) -> Result<(u16, serde_json::Value)> {
        let cache_stats = state.edge_cache.read().await.clone().map(|c| c.stats());
        let edges = match state.edge_registry.read().await.clone() {
            Some(r) => r.list(),
            None => Vec::new(),
        };
        Ok((
            200,
            serde_json::json!({
                "cache":          cache_stats,
                "registered":     edges,
                "edge_count":     edges.len(),
            }),
        ))
    }

    /// `POST /api/edge/register` — ack-only compatibility path. Body
    /// shape: `{"edge_id":"e1","region":"us-east","base_url":"https://e1"}`.
    /// Returns 201 with the assigned slot, 503 when registry full.
    /// The live invalidation stream is `GET /api/edge/subscribe`, which
    /// registers AND holds the receiver open for the connection's
    /// lifetime — edges should use that instead of this endpoint.
    #[cfg(feature = "edge-proxy")]
    async fn handle_edge_register(
        body: Option<&str>,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        let raw =
            body.ok_or_else(|| ProxyError::Internal("edge register: empty body".to_string()))?;
        let req: EdgeRegisterBody = match serde_json::from_str(raw) {
            Ok(r) => r,
            Err(e) => {
                return Ok((
                    400,
                    serde_json::json!({ "error": format!("invalid body: {}", e) }),
                ));
            }
        };
        let registry = match state.edge_registry.read().await.clone() {
            Some(r) => r,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "edge registry not attached" }),
                ));
            }
        };
        let now = chrono::Utc::now().to_rfc3339();
        match registry.register(&req.edge_id, &req.region, &req.base_url, &now) {
            Ok(_rx) => {
                // Receiver intentionally dropped (H5 resolved): this
                // JSON endpoint only acknowledges. The live stream is
                // `GET /api/edge/subscribe`, whose handler holds the
                // receiver for the connection's lifetime. An edge that
                // registers here without subscribing is pruned on the
                // next broadcast.
                Ok((
                    201,
                    serde_json::json!({
                        "edge_id":  req.edge_id,
                        "region":   req.region,
                        "base_url": req.base_url,
                        "registered_at": now,
                    }),
                ))
            }
            Err(e) => Ok((503, serde_json::json!({ "error": e.to_string() }))),
        }
    }

    /// `GET /api/edge/subscribe?edge_id=..&region=..&base_url=..` — the
    /// live invalidation stream (SSE). Registers the edge, then holds
    /// the registry receiver open for the connection's lifetime,
    /// forwarding every `InvalidationEvent` as an SSE `invalidate`
    /// frame with a `: keepalive` heartbeat every 15s in between.
    /// Resolves H5: the receiver lives exactly as long as the
    /// subscriber's connection; returning drops it, and the next
    /// broadcast prunes the dead sender.
    ///
    /// Writes to the raw write half — the SSE response is unframed
    /// (no Content-Length) and stays open until the edge disconnects
    /// or the registry evicts the subscription. Auth was already
    /// enforced by `handle_connection`'s bearer gate.
    #[cfg(feature = "edge-proxy")]
    async fn handle_edge_subscribe(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        state: &Arc<AdminState>,
        edge_id: &str,
        region: &str,
        base_url: &str,
    ) -> Result<()> {
        let registry = match state.edge_registry.read().await.clone() {
            Some(r) => r,
            None => {
                Self::send_json_response(
                    writer,
                    503,
                    &serde_json::json!({ "error": "edge registry not attached" }),
                )
                .await?;
                return Ok(());
            }
        };
        let now = chrono::Utc::now().to_rfc3339();
        let mut rx = match registry.register(edge_id, region, base_url, &now) {
            Ok(rx) => rx,
            // CapacityExceeded — same 503 shape as the JSON register path.
            Err(e) => {
                Self::send_json_response(
                    writer,
                    503,
                    &serde_json::json!({ "error": e.to_string() }),
                )
                .await?;
                return Ok(());
            }
        };

        // SSE preamble, straight onto the socket. Every SSE write is
        // bounded by ADMIN_SSE_WRITE_TIMEOUT: a subscriber that stops
        // reading (zero-window peer) must not pin this task — and its
        // MAX_ADMIN_CONNS permit — forever. The 15s heartbeat
        // guarantees a write attempt on every connection, so a wedged
        // subscriber is reaped within one beat + the timeout,
        // releasing both the permit and the receiver.
        if !Self::write_sse(
            writer,
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
        )
        .await
        {
            return Ok(());
        }

        // Hello frame: an invalidate event carrying the home's
        // per-boot epoch and current version. The edge (a) detects a
        // home restart immediately instead of at the first post-restart
        // write, (b) re-syncs its observed-home clock, and (c) flushes
        // entries cached while it was disconnected (empty table set =
        // wildcard), closing the missed-event window on reconnect.
        if let Some(cache) = state.edge_cache.read().await.clone() {
            let hello = InvalidationEvent {
                up_to_version: cache.current_version(),
                tables: Vec::new(),
                committed_at: now.clone(),
                epoch: cache.epoch(),
            };
            let json = serde_json::to_string(&hello)
                .map_err(|e| ProxyError::Internal(format!("JSON error: {}", e)))?;
            let frame = format!("event: invalidate\ndata: {}\n\n", json);
            if !Self::write_sse(writer, frame.as_bytes()).await {
                return Ok(());
            }
        }

        tracing::info!(edge_id, region, "edge subscribed to invalidation stream");

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(15));
        // The first interval tick completes immediately — consume it so
        // heartbeats start 15s after the preamble, not on top of it.
        heartbeat.tick().await;

        loop {
            tokio::select! {
                ev = rx.recv() => {
                    match ev {
                        Some(ev) => {
                            // `to_string` is single-line, per the SSE
                            // framing contract (exactly one `data:` line).
                            let json = serde_json::to_string(&ev)
                                .map_err(|e| ProxyError::Internal(format!("JSON error: {}", e)))?;
                            let frame = format!("event: invalidate\ndata: {}\n\n", json);
                            if !Self::write_sse(writer, frame.as_bytes()).await {
                                // Edge gone (or wedged past the write
                                // timeout). Returning drops `rx`; the
                                // next broadcast prunes the sender.
                                tracing::debug!(edge_id, "edge SSE write failed; closing stream");
                                return Ok(());
                            }
                        }
                        // Sender side gone: the registry evicted this
                        // subscription (prune_stale, unregister, or a
                        // re-register under the same edge_id replaced it).
                        None => {
                            tracing::debug!(edge_id, "edge subscription evicted by registry");
                            return Ok(());
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    // Comment-only keepalive — detects a dead client via
                    // the write error/timeout long before TCP keepalive
                    // would.
                    if !Self::write_sse(writer, b": keepalive\n\n").await {
                        tracing::debug!(edge_id, "edge SSE heartbeat failed; closing stream");
                        return Ok(());
                    }
                    // A delivered heartbeat proves the edge is reading:
                    // refresh registry liveness so a write-idle home
                    // never GC-prunes healthy subscribers (prune_stale
                    // stays as the backstop for wedged/dead peers).
                    registry.touch(edge_id, &chrono::Utc::now().to_rfc3339());
                }
            }
        }
    }

    /// One timeout-bounded SSE write+flush. `false` = the connection
    /// is dead or wedged (treat exactly like a write error and close).
    #[cfg(feature = "edge-proxy")]
    async fn write_sse(writer: &mut tokio::net::tcp::WriteHalf<'_>, bytes: &[u8]) -> bool {
        let write = async {
            writer.write_all(bytes).await?;
            writer.flush().await
        };
        matches!(
            tokio::time::timeout(Self::ADMIN_SSE_WRITE_TIMEOUT, write).await,
            Ok(Ok(()))
        )
    }

    /// `POST /api/edge/invalidate` — manual invalidation for ops
    /// drills. The proxy normally fans out invalidations
    /// automatically on writes; this endpoint is for "I just ran
    /// a migration outside the proxy, please drop caches".
    /// Body: `{"tables":["users"],"up_to_version":null}` — null
    /// version means "use the cache's current version" (drop all).
    #[cfg(feature = "edge-proxy")]
    async fn handle_edge_invalidate(
        body: Option<&str>,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        let raw =
            body.ok_or_else(|| ProxyError::Internal("edge invalidate: empty body".to_string()))?;
        let req: EdgeInvalidateBody = match serde_json::from_str(raw) {
            Ok(r) => r,
            Err(e) => {
                return Ok((
                    400,
                    serde_json::json!({ "error": format!("invalid body: {}", e) }),
                ));
            }
        };
        let cache = match state.edge_cache.read().await.clone() {
            Some(c) => c,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "edge cache not attached" }),
                ));
            }
        };
        let registry = match state.edge_registry.read().await.clone() {
            Some(r) => r,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "edge registry not attached" }),
                ));
            }
        };
        // Null version = "use the cache's current version": strictly
        // greater than every stamp this process has minted, so the
        // sweep covers all live entries (a fetch_add mint would also
        // burn a version for nothing). An explicit version is CLAMPED to the
        // current clock: a value above it would, once applied on the edges via
        // observe_home_version, push their observed-home counter past anything
        // the home will mint for a long time, so every subsequent real
        // invalidation (a smaller version) would sweep nothing — a permanent
        // fleet-wide poison from one oversized admin request.
        let version = req
            .up_to_version
            .map(|v| v.min(cache.current_version()))
            .unwrap_or_else(|| cache.current_version());
        // Local cache invalidation (home-side cache, if any).
        let dropped_local = cache.invalidate(version, &req.tables);
        // Fan out to every registered edge.
        let ev = InvalidationEvent {
            up_to_version: version,
            tables: req.tables.clone(),
            committed_at: chrono::Utc::now().to_rfc3339(),
            epoch: cache.epoch(),
        };
        let (sent, pruned) = registry.broadcast(ev).await;
        Ok((
            200,
            serde_json::json!({
                "version":         version,
                "tables":          req.tables,
                "dropped_local":   dropped_local,
                "edges_notified":  sent,
                "edges_pruned":    pruned,
            }),
        ))
    }

    /// Handle `GET /anomalies`. Returns the anomaly detector's
    /// recent-events ring buffer as JSON. Optional `?limit=N`
    /// query string clamps the response size (default 100, max 1024).
    /// Returns 503 when the detector hasn't been attached.
    #[cfg(feature = "anomaly-detection")]
    async fn handle_anomalies_list(
        path: &str,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        let limit = parse_limit_query(path, 100, 1024);
        let det = match state.anomaly_detector.read().await.clone() {
            Some(d) => d,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "anomaly detector not attached" }),
                ));
            }
        };
        let events = det.recent_events(limit);
        Ok((
            200,
            serde_json::json!({
                "count":     events.len(),
                "limit":     limit,
                "events":    events,
                "buffer_total": det.event_count(),
            }),
        ))
    }

    /// `GET /api/analytics` — top queries by call count plus the slow-query
    /// count. Returns 503 when analytics is not attached/enabled.
    #[cfg(feature = "query-analytics")]
    async fn handle_analytics(
        path: &str,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        use crate::analytics::OrderBy;
        let limit = parse_limit_query(path, 50, 1024);
        let a = match state.analytics.read().await.clone() {
            Some(a) => a,
            None => {
                return Ok((503, serde_json::json!({ "error": "analytics not enabled" })));
            }
        };
        let top: Vec<serde_json::Value> = a
            .top_queries(OrderBy::Calls, limit)
            .into_iter()
            .map(|s| {
                serde_json::json!({
                    "fingerprint": s.fingerprint_hash,
                    "normalized":  s.normalized,
                    "calls":       s.calls,
                    "avg_ms":      s.avg_time.as_secs_f64() * 1000.0,
                    "p99_ms":      s.p99.as_secs_f64() * 1000.0,
                    "rows":        s.rows,
                    "errors":      s.errors,
                })
            })
            .collect();
        let slow_count = a.slow_queries(limit).len();
        Ok((
            200,
            serde_json::json!({
                "limit":            limit,
                "top_queries":      top,
                "slow_query_count": slow_count,
            }),
        ))
    }

    /// Handle `POST /api/shadow`. Body is a JSON `ShadowRequestBody`.
    /// Connects to both source and shadow backends, runs the SQL on
    /// each, returns a `ShadowExecuteReport` with the diff.
    ///
    /// Status codes:
    ///   200 — both sides ran (report carries pass/fail details)
    ///   400 — malformed body
    ///   500 — source connect failure (shadow connect failures end up
    ///         in the report rather than the HTTP status)
    #[cfg(feature = "ha-tr")]
    async fn handle_shadow_request(body: Option<&str>) -> Result<(u16, serde_json::Value)> {
        use crate::backend::{
            tls::default_client_config, BackendClient, BackendConfig, ParamValue, TlsMode,
        };
        use crate::shadow_execute::shadow_execute;

        let raw =
            body.ok_or_else(|| ProxyError::Internal("shadow: empty request body".to_string()))?;
        let req: ShadowRequestBody = match serde_json::from_str(raw) {
            Ok(r) => r,
            Err(e) => {
                return Ok((
                    400,
                    serde_json::json!({ "error": format!("invalid body: {}", e) }),
                ));
            }
        };

        // Build the two configs from the request. TLS off + 5s
        // connect / 30s query timeouts mirror the replay defaults.
        let mk_cfg = |host: String,
                      port: u16,
                      user: Option<String>,
                      password: Option<String>,
                      database: Option<String>| BackendConfig {
            host,
            port,
            user: user.unwrap_or_else(|| "postgres".into()),
            password,
            database,
            application_name: Some("heliosdb-proxy-shadow".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: std::time::Duration::from_secs(5),
            query_timeout: std::time::Duration::from_secs(30),
            tls_config: default_client_config(),
        };
        let source_cfg = mk_cfg(
            req.source_host,
            req.source_port,
            req.source_user,
            req.source_password,
            req.source_database,
        );
        let shadow_cfg = mk_cfg(
            req.shadow_host,
            req.shadow_port,
            req.shadow_user,
            req.shadow_password,
            req.shadow_database,
        );

        // Connect to source. Connect failure here is a real HTTP
        // error since we can't even attempt the diff; shadow connect
        // failures land inside the report as `shadow_error`.
        let mut source = match BackendClient::connect(&source_cfg).await {
            Ok(c) => c,
            Err(e) => {
                return Ok((
                    500,
                    serde_json::json!({ "error": format!("source connect: {}", e) }),
                ));
            }
        };

        let params: Vec<ParamValue> = req
            .params
            .unwrap_or_default()
            .into_iter()
            .map(ParamValue::Text)
            .collect();

        let outcome = shadow_execute(&mut source, &shadow_cfg, &req.sql, &params).await;
        source.close().await;

        match outcome {
            Ok((_qr, report)) => Ok((
                200,
                serde_json::json!({
                    "sql":                report.sql,
                    "both_succeeded":     report.both_succeeded,
                    "row_count_match":    report.row_count_match,
                    "row_hash_match":     report.row_hash_match,
                    "primary_elapsed_us": report.primary_elapsed_us,
                    "shadow_elapsed_us":  report.shadow_elapsed_us,
                    "primary_error":      report.primary_error,
                    "shadow_error":       report.shadow_error,
                    "is_clean":           report.is_clean(),
                }),
            )),
            Err(e) => Ok((
                500,
                serde_json::json!({ "error": format!("shadow execute: {}", e) }),
            )),
        }
    }

    /// Handle `POST /api/chaos`. Body is a JSON `ChaosRequestBody`.
    ///
    /// Supported actions (intentionally narrow — the goal is "test
    /// the failover machinery without external chaos tooling", not
    /// "ship a kitchen-sink fault injector"):
    ///
    ///   force_unhealthy { target_node }  — flip the node's health flag
    ///                                      to false; the failover
    ///                                      controller observes this and
    ///                                      reroutes traffic.
    ///   restore         { target_node }  — flip the node's health flag
    ///                                      back to true and clear the
    ///                                      override entry.
    ///   reset                            — restore every overridden
    ///                                      node in one call.
    /// `GET /api/circuit` — live per-node circuit-breaker state. Reports each
    /// configured node's breaker as `closed` / `open` / `half_open` so an
    /// operator can see which backends the breaker has tripped out of rotation.
    #[cfg(feature = "circuit-breaker")]
    async fn handle_circuit_status(state: &Arc<AdminState>) -> Result<(u16, serde_json::Value)> {
        let mgr = match state.circuit_breaker.read().await.clone() {
            Some(m) => m,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "circuit breaker not attached" }),
                ))
            }
        };
        let nodes = state.config_snapshot.read().await.nodes.clone();
        let circuits: Vec<serde_json::Value> = nodes
            .iter()
            .map(|n| {
                let st = mgr.get_breaker(&n.address).get_state();
                serde_json::json!({
                    "node": n.address,
                    "state": format!("{:?}", st).to_lowercase(),
                })
            })
            .collect();
        Ok((200, serde_json::json!({ "circuits": circuits })))
    }

    async fn handle_chaos_request(
        body: Option<&str>,
        state: &Arc<AdminState>,
    ) -> Result<(u16, serde_json::Value)> {
        let raw =
            body.ok_or_else(|| ProxyError::Internal("chaos: empty request body".to_string()))?;
        let action: ChaosAction = match serde_json::from_str(raw) {
            Ok(a) => a,
            Err(e) => {
                return Ok((
                    400,
                    serde_json::json!({ "error": format!("invalid body: {}", e) }),
                ));
            }
        };
        match action {
            ChaosAction::ForceUnhealthy { target_node } => {
                if let Err(e) = Self::set_node_enabled(state, &target_node, false).await {
                    return Ok((404, serde_json::json!({ "error": e.to_string() })));
                }
                state.chaos_overrides.write().await.insert(
                    target_node.clone(),
                    ChaosOverride {
                        since: chrono::Utc::now().to_rfc3339(),
                        kind: "force_unhealthy".to_string(),
                        note: "forced unhealthy via chaos endpoint".to_string(),
                    },
                );
                Ok((
                    200,
                    serde_json::json!({
                        "applied":     "force_unhealthy",
                        "target_node": target_node,
                    }),
                ))
            }
            ChaosAction::Restore { target_node } => {
                if let Err(e) = Self::set_node_enabled(state, &target_node, true).await {
                    return Ok((404, serde_json::json!({ "error": e.to_string() })));
                }
                state.chaos_overrides.write().await.remove(&target_node);
                Ok((
                    200,
                    serde_json::json!({
                        "restored":    target_node,
                    }),
                ))
            }
            ChaosAction::Reset => {
                let overrides: Vec<String> =
                    state.chaos_overrides.read().await.keys().cloned().collect();
                let mut restored = Vec::with_capacity(overrides.len());
                for addr in overrides {
                    let _ = Self::set_node_enabled(state, &addr, true).await;
                    restored.push(addr);
                }
                state.chaos_overrides.write().await.clear();
                Ok((
                    200,
                    serde_json::json!({
                        "reset":      true,
                        "restored":   restored,
                    }),
                ))
            }
        }
    }

    /// Handle `GET /plugins`. Returns 503 when no plugin manager is
    /// attached, 200 with `Vec<PluginListEntry>` otherwise. Building
    /// the response in admin.rs (rather than serialising
    /// `plugins::PluginInfo` directly) keeps the plugins module
    /// independent of serde — only the wire shape lives here.
    #[cfg(feature = "wasm-plugins")]
    async fn handle_plugins_list(state: &Arc<AdminState>) -> Result<(u16, serde_json::Value)> {
        let pm = match state.plugin_manager.read().await.clone() {
            Some(p) => p,
            None => {
                return Ok((
                    503,
                    serde_json::json!({ "error": "plugin manager not attached" }),
                ));
            }
        };
        let plugins: Vec<PluginListEntry> = pm
            .list_plugins()
            .into_iter()
            .map(|info| PluginListEntry {
                name: info.name,
                version: info.version,
                description: info.description,
                hooks: info
                    .hooks
                    .iter()
                    .map(|h| h.export_name().to_string())
                    .collect(),
                state: format!("{:?}", info.state),
                invocations: info.stats.total_calls,
                errors: info.stats.error_count,
            })
            .collect();
        Ok((200, serde_json::to_value(plugins)?))
    }

    #[cfg(not(feature = "wasm-plugins"))]
    async fn handle_plugins_list(_state: &Arc<AdminState>) -> Result<(u16, serde_json::Value)> {
        Ok((
            503,
            serde_json::json!({ "error": "wasm-plugins feature not compiled in" }),
        ))
    }

    /// Compute the joined topology view used by `/topology`.
    ///
    /// `currentPrimary` is the address of the first node whose role
    /// is "primary" and whose health entry is `healthy = true`. None
    /// is the legitimate answer when failover is in progress.
    async fn compute_topology(state: &Arc<AdminState>) -> TopologyResponse {
        let health = state.node_health.read().await;
        let cfg = state.config_snapshot.read().await;

        let mut current_primary: Option<String> = None;
        for n in &cfg.nodes {
            if n.role.eq_ignore_ascii_case("primary") {
                let healthy = health.get(&n.address).map(|h| h.healthy).unwrap_or(false);
                if healthy {
                    current_primary = Some(n.address.clone());
                    break;
                }
            }
        }

        let healthy_nodes = health.values().filter(|h| h.healthy).count() as u32;
        let unhealthy_nodes = health.values().filter(|h| !h.healthy).count() as u32;
        let total_nodes = cfg.nodes.len() as u32;

        TopologyResponse {
            current_primary,
            healthy_nodes,
            unhealthy_nodes,
            total_nodes,
            last_failover_at: None,
        }
    }

    /// Format metrics as Prometheus text format
    fn format_prometheus_metrics(metrics: &ServerMetricsSnapshot) -> String {
        let mut output = String::new();

        output.push_str("# HELP heliosdb_proxy_connections_total Total connections accepted\n");
        output.push_str("# TYPE heliosdb_proxy_connections_total counter\n");
        output.push_str(&format!(
            "heliosdb_proxy_connections_total {}\n",
            metrics.connections_accepted
        ));

        output.push_str("# HELP heliosdb_proxy_connections_closed Total connections closed\n");
        output.push_str("# TYPE heliosdb_proxy_connections_closed counter\n");
        output.push_str(&format!(
            "heliosdb_proxy_connections_closed {}\n",
            metrics.connections_closed
        ));

        output.push_str("# HELP heliosdb_proxy_queries_total Total queries processed\n");
        output.push_str("# TYPE heliosdb_proxy_queries_total counter\n");
        output.push_str(&format!(
            "heliosdb_proxy_queries_total {}\n",
            metrics.queries_processed
        ));

        output.push_str("# HELP heliosdb_proxy_bytes_received_total Total bytes received\n");
        output.push_str("# TYPE heliosdb_proxy_bytes_received_total counter\n");
        output.push_str(&format!(
            "heliosdb_proxy_bytes_received_total {}\n",
            metrics.bytes_received
        ));

        output.push_str("# HELP heliosdb_proxy_bytes_sent_total Total bytes sent\n");
        output.push_str("# TYPE heliosdb_proxy_bytes_sent_total counter\n");
        output.push_str(&format!(
            "heliosdb_proxy_bytes_sent_total {}\n",
            metrics.bytes_sent
        ));

        output.push_str("# HELP heliosdb_proxy_failovers_total Total failovers\n");
        output.push_str("# TYPE heliosdb_proxy_failovers_total counter\n");
        output.push_str(&format!(
            "heliosdb_proxy_failovers_total {}\n",
            metrics.failovers
        ));

        output
    }

    /// Send HTTP response
    async fn send_response(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        status: u16,
        status_text: &str,
        body: &str,
    ) -> Result<()> {
        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            status_text,
            body.len(),
            body
        );

        writer
            .write_all(response.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;

        Ok(())
    }

    /// Send JSON HTTP response
    async fn send_json_response<T: Serialize>(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        status: u16,
        body: &T,
    ) -> Result<()> {
        let json = serde_json::to_string(body)
            .map_err(|e| ProxyError::Internal(format!("JSON error: {}", e)))?;

        let status_text = match status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            _ => "Unknown",
        };

        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status,
            status_text,
            json.len(),
            json
        );

        writer
            .write_all(response.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;

        Ok(())
    }

    /// Shutdown the admin server
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

impl AdminState {
    /// Create new admin state
    pub fn new() -> Self {
        Self {
            node_health: RwLock::new(HashMap::new()),
            metrics: RwLock::new(ServerMetricsSnapshot {
                connections_accepted: 0,
                connections_closed: 0,
                queries_processed: 0,
                bytes_received: 0,
                bytes_sent: 0,
                failovers: 0,
            }),
            active_sessions: RwLock::new(0),
            config_snapshot: RwLock::new(ConfigSnapshot {
                listen_address: String::new(),
                admin_address: String::new(),
                tr_enabled: false,
                tr_mode: String::new(),
                pool_min_connections: 0,
                pool_max_connections: 0,
                nodes: Vec::new(),
            }),
            proxy_config: RwLock::new(None),
            read_lb_counter: AtomicUsize::new(0),
            commands: RwLock::new(HashMap::new()),
            #[cfg(feature = "pool-modes")]
            pool_manager: RwLock::new(None),
            #[cfg(feature = "circuit-breaker")]
            circuit_breaker: RwLock::new(None),
            #[cfg(feature = "ha-tr")]
            replay_engine: RwLock::new(None),
            #[cfg(feature = "wasm-plugins")]
            plugin_manager: RwLock::new(None),
            chaos_overrides: RwLock::new(HashMap::new()),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: RwLock::new(None),
            #[cfg(feature = "query-analytics")]
            analytics: RwLock::new(None),
            #[cfg(feature = "edge-proxy")]
            edge_cache: RwLock::new(None),
            #[cfg(feature = "edge-proxy")]
            edge_registry: RwLock::new(None),
            auth_token: RwLock::new(None),
            migration: RwLock::new(None),
            branch: RwLock::new(None),
        }
    }

    /// Set the admin Bearer token (wired by the server at startup).
    pub async fn with_auth_token(&self, token: Option<String>) {
        *self.auth_token.write().await = token;
    }

    /// Attach traffic-mirror info so `/api/migration/status` can report it.
    pub async fn with_migration(&self, info: MigrationInfo) {
        *self.migration.write().await = Some(info);
    }

    /// Attach branch-database config so `/api/branch` can provision.
    pub async fn with_branch(&self, cfg: crate::config::BranchConfig) {
        *self.branch.write().await = Some(cfg);
    }

    /// Attach the connection pool manager so `/api/pools` reports real
    /// per-node pool statistics. Wired by the server at startup.
    #[cfg(feature = "pool-modes")]
    pub async fn with_pool_manager(&self, manager: Arc<crate::pool::ConnectionPoolManager>) {
        *self.pool_manager.write().await = Some(manager);
    }

    /// Attach the circuit-breaker manager so `/api/circuit` reports live
    /// per-node circuit state. Wired by the server at startup.
    #[cfg(feature = "circuit-breaker")]
    pub async fn with_circuit_breaker(
        &self,
        manager: Arc<crate::circuit_breaker::CircuitBreakerManager>,
    ) {
        *self.circuit_breaker.write().await = Some(manager);
    }

    /// Attach an anomaly detector. Mirror of with_replay_engine /
    /// with_plugin_manager — wired by the server at startup.
    #[cfg(feature = "anomaly-detection")]
    pub async fn with_anomaly_detector(&self, detector: Arc<AnomalyDetector>) {
        *self.anomaly_detector.write().await = Some(detector);
    }

    /// Attach the query-analytics engine so `/api/analytics` can read it.
    #[cfg(feature = "query-analytics")]
    pub async fn with_analytics(&self, analytics: Arc<crate::analytics::QueryAnalytics>) {
        *self.analytics.write().await = Some(analytics);
    }

    /// Attach edge cache + registry. Server calls this once at
    /// startup; both Arcs are the same instances ServerState holds.
    #[cfg(feature = "edge-proxy")]
    pub async fn with_edge(&self, cache: Arc<EdgeCache>, registry: Arc<EdgeRegistry>) {
        *self.edge_cache.write().await = Some(cache);
        *self.edge_registry.write().await = Some(registry);
    }

    /// Attach a time-travel replay engine. Production startup calls
    /// this once with a `ReplayEngine` constructed from the proxy's
    /// shared `TransactionJournal` + a `BackendConfig` template; the
    /// `/api/replay` endpoint returns 503 until this is set.
    #[cfg(feature = "ha-tr")]
    pub async fn with_replay_engine(&self, engine: Arc<ReplayEngine>) {
        *self.replay_engine.write().await = Some(engine);
    }

    /// Attach a WASM plugin manager. Production startup calls this
    /// once with the same Arc held by ProxyServer; the `/plugins`
    /// endpoint returns 503 until set.
    #[cfg(feature = "wasm-plugins")]
    pub async fn with_plugin_manager(&self, manager: Arc<PluginManager>) {
        *self.plugin_manager.write().await = Some(manager);
    }

    /// Set the proxy configuration for SQL routing
    pub async fn set_proxy_config(&self, config: ProxyConfig) {
        let mut proxy_config = self.proxy_config.write().await;
        *proxy_config = Some(config);
    }

    /// Register a command handler
    pub async fn register_command<F>(&self, name: &str, handler: F)
    where
        F: Fn(&[&str]) -> Result<String> + Send + Sync + 'static,
    {
        let mut commands = self.commands.write().await;
        commands.insert(name.to_string(), Arc::new(handler));
    }

    /// Execute a command
    pub async fn execute_command(&self, name: &str, args: &[&str]) -> Result<String> {
        let commands = self.commands.read().await;
        match commands.get(name) {
            Some(handler) => handler(args),
            None => Err(ProxyError::Internal(format!("Unknown command: {}", name))),
        }
    }
}

impl Default for AdminState {
    fn default() -> Self {
        Self::new()
    }
}

// Request and Response types

/// SQL execution request
#[derive(Debug, Deserialize)]
struct SqlRequest {
    /// SQL query to execute
    query: String,
}

/// SQL execution response
#[derive(Debug, Serialize)]
struct SqlResponse {
    /// Query type (read/write)
    query_type: String,
    /// Node the query was routed to
    routed_to: String,
    /// Role of the target node
    node_role: String,
    /// Query result from backend
    result: serde_json::Value,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadinessResponse {
    ready: bool,
    message: &'static str,
}

#[derive(Serialize)]
struct LivenessResponse {
    alive: bool,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct MetricsResponse {
    connections_accepted: u64,
    connections_closed: u64,
    connections_active: u64,
    queries_processed: u64,
    bytes_received: u64,
    bytes_sent: u64,
    failovers: u64,
}

impl From<ServerMetricsSnapshot> for MetricsResponse {
    fn from(m: ServerMetricsSnapshot) -> Self {
        Self {
            connections_accepted: m.connections_accepted,
            connections_closed: m.connections_closed,
            connections_active: m.connections_accepted.saturating_sub(m.connections_closed),
            queries_processed: m.queries_processed,
            bytes_received: m.bytes_received,
            bytes_sent: m.bytes_sent,
            failovers: m.failovers,
        }
    }
}

#[derive(Serialize)]
struct NodeHealthResponse {
    address: String,
    healthy: bool,
    last_check: String,
    failure_count: u32,
    last_error: Option<String>,
    latency_ms: f64,
    replication_lag_bytes: Option<u64>,
}

impl From<NodeHealth> for NodeHealthResponse {
    fn from(h: NodeHealth) -> Self {
        Self {
            address: h.address,
            healthy: h.healthy,
            last_check: h.last_check.to_rfc3339(),
            failure_count: h.failure_count,
            last_error: h.last_error,
            latency_ms: h.latency_ms,
            replication_lag_bytes: h.replication_lag_bytes,
        }
    }
}

#[derive(Serialize)]
struct SessionsResponse {
    active_sessions: u64,
}

/// JSON body for `POST /api/edge/register`.
#[cfg(feature = "edge-proxy")]
#[derive(Debug, Deserialize)]
struct EdgeRegisterBody {
    edge_id: String,
    region: String,
    base_url: String,
}

/// JSON body for `POST /api/edge/invalidate`. `up_to_version` is
/// optional — when None, the cache mints the next version stamp
/// (effectively "drop everything matching `tables`").
#[cfg(feature = "edge-proxy")]
#[derive(Debug, Deserialize)]
struct EdgeInvalidateBody {
    #[serde(default)]
    tables: Vec<String>,
    #[serde(default)]
    up_to_version: Option<u64>,
}

/// Parse `?limit=N` from a path. Returns clamped value, or `default`
/// when the param is missing / unparseable.
///
/// Shared by the `/anomalies` (`anomaly-detection`) and `/api/analytics`
/// (`query-analytics`) handlers, so it must compile whenever *either* feature
/// is enabled — not just `anomaly-detection`.
#[cfg(any(feature = "anomaly-detection", feature = "query-analytics"))]
fn parse_limit_query(path: &str, default: usize, max: usize) -> usize {
    let q = match path.find('?') {
        Some(i) => &path[i + 1..],
        None => return default,
    };
    for kv in q.split('&') {
        let mut it = kv.splitn(2, '=');
        if let (Some(k), Some(v)) = (it.next(), it.next()) {
            if k == "limit" {
                if let Ok(n) = v.parse::<usize>() {
                    return n.min(max);
                }
            }
        }
    }
    default
}

/// Decode `%XX` percent-escapes in a query-string value. Invalid or
/// truncated escapes pass through literally (lenient — the decoded
/// value feeds an identifier/URL echo, not a security decision). `+`
/// is NOT decoded to space: subscribers send RFC 3986 percent-encoded
/// values, not `application/x-www-form-urlencoded`.
#[cfg(feature = "edge-proxy")]
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push(((hi as u8) << 4) | lo as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parse a URL query string (`?k=v&k2=v2`) into a map, percent-decoding
/// the values. Keys without a `=` are ignored. Used by the edge SSE
/// subscribe route — the only admin route taking query params beyond
/// the single `?limit=` that `parse_limit_query` covers.
#[cfg(feature = "edge-proxy")]
fn parse_query_params(path: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let q = match path.find('?') {
        Some(i) => &path[i + 1..],
        None => return out,
    };
    for kv in q.split('&') {
        let mut it = kv.splitn(2, '=');
        if let (Some(k), Some(v)) = (it.next(), it.next()) {
            if !k.is_empty() {
                out.insert(k.to_string(), percent_decode(v));
            }
        }
    }
    out
}

/// JSON body for `POST /api/shadow`.
#[cfg(feature = "ha-tr")]
#[derive(Debug, Deserialize)]
struct ShadowRequestBody {
    /// SQL to execute on both sides.
    sql: String,
    /// Optional text-format parameters interpolated into `sql`. None
    /// or empty list runs as a simple_query.
    #[serde(default)]
    params: Option<Vec<String>>,

    /// Source backend (the side whose result the application would
    /// see in production).
    source_host: String,
    source_port: u16,
    #[serde(default)]
    source_user: Option<String>,
    #[serde(default)]
    source_password: Option<String>,
    #[serde(default)]
    source_database: Option<String>,

    /// Shadow backend (the side being validated — typically a
    /// new-version replica or post-migration schema).
    shadow_host: String,
    shadow_port: u16,
    #[serde(default)]
    shadow_user: Option<String>,
    #[serde(default)]
    shadow_password: Option<String>,
    #[serde(default)]
    shadow_database: Option<String>,
}

/// Chaos actions the proxy supports today. Forward-compatible —
/// unknown actions deserialise as an error.
///
/// Wire shape: `{"action":"force_unhealthy","target_node":"..."}`.
#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ChaosAction {
    /// Mark a node unhealthy until restored (or until reset is
    /// called). Triggers the failover path the same way a real
    /// health-check failure would.
    ForceUnhealthy { target_node: String },
    /// Mark a previously-overridden node healthy again.
    Restore { target_node: String },
    /// Reset every chaos override in one call. Idempotent.
    Reset,
}

/// JSON entry returned by `GET /plugins`. Built in admin.rs so the
/// plugins module doesn't need a serde dep.
#[cfg(feature = "wasm-plugins")]
#[derive(Serialize)]
struct PluginListEntry {
    name: String,
    version: String,
    description: String,
    /// Hook export names (`pre_query`, `post_query`, `route`, ...).
    hooks: Vec<String>,
    /// `Loading` | `Running` | `Paused` | `Error(...)` | `Unloading`.
    state: String,
    invocations: u64,
    errors: u64,
}

/// JSON body for `POST /api/replay`.
#[cfg(feature = "ha-tr")]
#[derive(Debug, Deserialize)]
struct ReplayRequestBody {
    /// RFC 3339 inclusive window start.
    from: DateTime<Utc>,
    /// RFC 3339 inclusive window end.
    to: DateTime<Utc>,
    /// Target backend host (typically a staging DB).
    target_host: String,
    /// Target backend port.
    target_port: u16,
    /// Optional credential overrides — when omitted, the engine uses
    /// the template values set at server startup. Production callers
    /// targeting a separate staging DB pass these explicitly so the
    /// proxy doesn't need to hold staging credentials in its own
    /// config.
    #[serde(default)]
    target_user: Option<String>,
    #[serde(default)]
    target_password: Option<String>,
    #[serde(default)]
    target_database: Option<String>,
}

/// Joined view exposed at `/topology`. Field names use camelCase so
/// they map cleanly into the Kubernetes operator's CRD status
/// (`HeliosProxyStatus.currentPrimary`, etc).
#[derive(Serialize)]
struct TopologyResponse {
    #[serde(rename = "currentPrimary")]
    current_primary: Option<String>,
    #[serde(rename = "healthyNodes")]
    healthy_nodes: u32,
    #[serde(rename = "unhealthyNodes")]
    unhealthy_nodes: u32,
    #[serde(rename = "totalNodes")]
    total_nodes: u32,
    /// RFC 3339 timestamp of the last detected primary change.
    /// `None` when the proxy hasn't observed a failover since boot.
    #[serde(rename = "lastFailoverAt")]
    last_failover_at: Option<String>,
}

#[derive(Serialize)]
struct PoolStatsResponse {
    node: String,
    active_connections: u64,
    idle_connections: u64,
    pending_requests: u64,
    total_connections_created: u64,
    total_connections_closed: u64,
}

#[derive(Serialize)]
struct VersionResponse {
    version: String,
    build_time: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_admin_state_creation() {
        let state = AdminState::new();
        let sessions = state.active_sessions.read().await;
        assert_eq!(*sessions, 0);
    }

    #[test]
    fn test_admin_authorized() {
        let h = |s: &str| vec!["GET /topology HTTP/1.1".to_string(), s.to_string()];
        assert!(AdminServer::admin_authorized(
            &h("Authorization: Bearer s3cret"),
            "s3cret"
        ));
        // case-insensitive header name, exact token
        assert!(AdminServer::admin_authorized(
            &h("authorization: Bearer s3cret"),
            "s3cret"
        ));
        // wrong token / wrong scheme / missing header all rejected
        assert!(!AdminServer::admin_authorized(
            &h("Authorization: Bearer nope"),
            "s3cret"
        ));
        assert!(!AdminServer::admin_authorized(
            &h("Authorization: Basic s3cret"),
            "s3cret"
        ));
        assert!(!AdminServer::admin_authorized(
            &["GET / HTTP/1.1".to_string()],
            "s3cret"
        ));
    }

    #[test]
    fn test_constant_time_eq_str() {
        assert!(constant_time_eq_str("abc", "abc"));
        assert!(!constant_time_eq_str("abc", "abd"));
        assert!(!constant_time_eq_str("abc", "abcd"));
    }

    #[tokio::test]
    async fn test_readiness_check_no_nodes() {
        let state = Arc::new(AdminState::new());
        let ready = AdminServer::check_readiness(&state).await;
        assert!(!ready);
    }

    #[tokio::test]
    async fn test_readiness_check_with_healthy_node() {
        let state = Arc::new(AdminState::new());

        {
            let mut health = state.node_health.write().await;
            health.insert(
                "localhost:5432".to_string(),
                NodeHealth {
                    address: "localhost:5432".to_string(),
                    healthy: true,
                    last_check: chrono::Utc::now(),
                    failure_count: 0,
                    last_error: None,
                    latency_ms: 1.0,
                    replication_lag_bytes: None,
                },
            );
        }

        let ready = AdminServer::check_readiness(&state).await;
        assert!(ready);
    }

    #[tokio::test]
    async fn test_command_registration() {
        let state = AdminState::new();

        state
            .register_command("test", |args| {
                Ok(format!("Test command with {} args", args.len()))
            })
            .await;

        let result = state.execute_command("test", &["arg1", "arg2"]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Test command with 2 args");
    }

    #[tokio::test]
    async fn test_unknown_command() {
        let state = AdminState::new();
        let result = state.execute_command("unknown", &[]).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_prometheus_metrics_format() {
        let metrics = ServerMetricsSnapshot {
            connections_accepted: 100,
            connections_closed: 50,
            queries_processed: 1000,
            bytes_received: 50000,
            bytes_sent: 100000,
            failovers: 2,
        };

        let output = AdminServer::format_prometheus_metrics(&metrics);
        assert!(output.contains("heliosdb_proxy_connections_total 100"));
        assert!(output.contains("heliosdb_proxy_queries_total 1000"));
        assert!(output.contains("heliosdb_proxy_failovers_total 2"));
    }

    #[test]
    fn test_metrics_response_active_connections() {
        let snapshot = ServerMetricsSnapshot {
            connections_accepted: 100,
            connections_closed: 30,
            queries_processed: 500,
            bytes_received: 10000,
            bytes_sent: 20000,
            failovers: 1,
        };

        let response = MetricsResponse::from(snapshot);
        assert_eq!(response.connections_active, 70);
    }

    /// Helper: build an AdminState with the given (address, role,
    /// healthy) tuples seeded into config + node_health.
    async fn topology_state(nodes: &[(&str, &str, bool)]) -> Arc<AdminState> {
        let state = Arc::new(AdminState::new());
        {
            let mut cfg = state.config_snapshot.write().await;
            cfg.nodes = nodes
                .iter()
                .map(|(addr, role, _)| NodeSnapshot {
                    address: (*addr).to_string(),
                    role: (*role).to_string(),
                    weight: 100,
                    enabled: true,
                })
                .collect();
        }
        {
            let mut health = state.node_health.write().await;
            for (addr, _, healthy) in nodes {
                health.insert(
                    (*addr).to_string(),
                    NodeHealth {
                        address: (*addr).to_string(),
                        healthy: *healthy,
                        last_check: chrono::Utc::now(),
                        failure_count: 0,
                        last_error: None,
                        latency_ms: 1.0,
                        replication_lag_bytes: None,
                    },
                );
            }
        }
        state
    }

    #[tokio::test]
    async fn test_topology_returns_healthy_primary() {
        let state = topology_state(&[
            ("primary.svc:5432", "primary", true),
            ("standby-a.svc:5432", "standby", true),
            ("standby-b.svc:5432", "standby", false),
        ])
        .await;

        let topo = AdminServer::compute_topology(&state).await;
        assert_eq!(topo.current_primary.as_deref(), Some("primary.svc:5432"));
        assert_eq!(topo.healthy_nodes, 2);
        assert_eq!(topo.unhealthy_nodes, 1);
        assert_eq!(topo.total_nodes, 3);
    }

    #[tokio::test]
    async fn test_topology_no_primary_when_primary_unhealthy() {
        // Failover in progress: the configured primary is sick and
        // no other node has been promoted yet.
        let state = topology_state(&[
            ("primary.svc:5432", "primary", false),
            ("standby.svc:5432", "standby", true),
        ])
        .await;

        let topo = AdminServer::compute_topology(&state).await;
        assert_eq!(topo.current_primary, None);
        assert_eq!(topo.healthy_nodes, 1);
        assert_eq!(topo.unhealthy_nodes, 1);
    }

    #[tokio::test]
    async fn test_topology_handles_empty_cluster() {
        let state = Arc::new(AdminState::new());
        let topo = AdminServer::compute_topology(&state).await;
        assert_eq!(topo.current_primary, None);
        assert_eq!(topo.healthy_nodes, 0);
        assert_eq!(topo.unhealthy_nodes, 0);
        assert_eq!(topo.total_nodes, 0);
    }

    #[tokio::test]
    async fn test_topology_role_match_is_case_insensitive() {
        let state = topology_state(&[("primary.svc:5432", "PRIMARY", true)]).await;
        let topo = AdminServer::compute_topology(&state).await;
        assert_eq!(topo.current_primary.as_deref(), Some("primary.svc:5432"));
    }

    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_replay_returns_503_when_engine_unattached() {
        let state = Arc::new(AdminState::new());
        let body = r#"{
            "from": "2026-04-25T10:00:00Z",
            "to":   "2026-04-25T11:00:00Z",
            "target_host": "127.0.0.1",
            "target_port": 5432
        }"#;
        let (status, value) = AdminServer::handle_replay_request(Some(body), &state)
            .await
            .expect("handler returns Ok with status code");
        assert_eq!(status, 503);
        assert_eq!(value["error"], "replay engine not attached");
    }

    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_replay_400_on_malformed_body() {
        let state = Arc::new(AdminState::new());
        let (status, _) = AdminServer::handle_replay_request(Some("not json"), &state)
            .await
            .expect("handler returns Ok with status code");
        assert_eq!(status, 400);
    }

    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_replay_errors_on_empty_body() {
        let state = Arc::new(AdminState::new());
        let err = AdminServer::handle_replay_request(None, &state).await;
        assert!(err.is_err(), "empty body must surface as Err");
    }

    #[cfg(feature = "wasm-plugins")]
    #[tokio::test]
    async fn test_plugins_list_returns_503_when_manager_unattached() {
        let state = Arc::new(AdminState::new());
        let (status, value) = AdminServer::handle_plugins_list(&state)
            .await
            .expect("handler returns Ok with status code");
        assert_eq!(status, 503);
        assert_eq!(value["error"], "plugin manager not attached");
    }

    #[cfg(not(feature = "wasm-plugins"))]
    #[tokio::test]
    async fn test_plugins_list_503_without_feature() {
        let state = Arc::new(AdminState::new());
        let (status, _) = AdminServer::handle_plugins_list(&state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 503);
    }

    /// Helper: state with a single healthy node seeded into health.
    async fn chaos_state_with_node(addr: &str) -> Arc<AdminState> {
        let state = Arc::new(AdminState::new());
        state.node_health.write().await.insert(
            addr.to_string(),
            NodeHealth {
                address: addr.to_string(),
                healthy: true,
                last_check: chrono::Utc::now(),
                failure_count: 0,
                last_error: None,
                latency_ms: 1.0,
                replication_lag_bytes: None,
            },
        );
        state
    }

    #[tokio::test]
    async fn test_chaos_force_unhealthy_flips_node_and_records_override() {
        let state = chaos_state_with_node("primary.svc:5432").await;
        let body = r#"{"action":"force_unhealthy","target_node":"primary.svc:5432"}"#;
        let (status, value) = AdminServer::handle_chaos_request(Some(body), &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 200);
        assert_eq!(value["applied"], "force_unhealthy");
        // Health flag flipped.
        assert!(!state.node_health.read().await["primary.svc:5432"].healthy);
        // Override recorded.
        assert!(state
            .chaos_overrides
            .read()
            .await
            .contains_key("primary.svc:5432"));
    }

    #[tokio::test]
    async fn test_chaos_restore_clears_override_and_flips_back() {
        let state = chaos_state_with_node("primary.svc:5432").await;
        let _ = AdminServer::handle_chaos_request(
            Some(r#"{"action":"force_unhealthy","target_node":"primary.svc:5432"}"#),
            &state,
        )
        .await
        .unwrap();
        let (status, _) = AdminServer::handle_chaos_request(
            Some(r#"{"action":"restore","target_node":"primary.svc:5432"}"#),
            &state,
        )
        .await
        .unwrap();
        assert_eq!(status, 200);
        assert!(state.node_health.read().await["primary.svc:5432"].healthy);
        assert!(state.chaos_overrides.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_chaos_reset_restores_all_overrides() {
        let state = chaos_state_with_node("a:5432").await;
        state.node_health.write().await.insert(
            "b:5432".to_string(),
            NodeHealth {
                address: "b:5432".to_string(),
                healthy: true,
                last_check: chrono::Utc::now(),
                failure_count: 0,
                last_error: None,
                latency_ms: 1.0,
                replication_lag_bytes: None,
            },
        );
        for addr in &["a:5432", "b:5432"] {
            let body = format!(r#"{{"action":"force_unhealthy","target_node":"{}"}}"#, addr);
            let _ = AdminServer::handle_chaos_request(Some(&body), &state)
                .await
                .unwrap();
        }
        let (status, value) =
            AdminServer::handle_chaos_request(Some(r#"{"action":"reset"}"#), &state)
                .await
                .unwrap();
        assert_eq!(status, 200);
        assert_eq!(value["reset"], true);
        let restored = value["restored"].as_array().unwrap();
        assert_eq!(restored.len(), 2);
        // Both nodes back to healthy + overrides cleared.
        for addr in &["a:5432", "b:5432"] {
            assert!(state.node_health.read().await[*addr].healthy);
        }
        assert!(state.chaos_overrides.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_chaos_force_unhealthy_404s_when_node_unknown() {
        let state = Arc::new(AdminState::new());
        let body = r#"{"action":"force_unhealthy","target_node":"missing.svc:5432"}"#;
        let (status, _) = AdminServer::handle_chaos_request(Some(body), &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn test_chaos_400_on_malformed_body() {
        let state = Arc::new(AdminState::new());
        let (status, _) = AdminServer::handle_chaos_request(Some("not json"), &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn test_chaos_400_on_unknown_action() {
        let state = Arc::new(AdminState::new());
        let body = r#"{"action":"format_disk","target_node":"x"}"#;
        let (status, _) = AdminServer::handle_chaos_request(Some(body), &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 400);
    }

    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_shadow_400_on_malformed_body() {
        let (status, _) = AdminServer::handle_shadow_request(Some("not json"))
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 400);
    }

    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_shadow_500_on_source_unreachable() {
        // Address that nothing is listening on (port 1 = tcpmux,
        // refused by everything reasonable).
        let body = r#"{
            "sql": "SELECT 1",
            "source_host": "127.0.0.1",
            "source_port": 1,
            "shadow_host": "127.0.0.1",
            "shadow_port": 1
        }"#;
        let (status, value) = AdminServer::handle_shadow_request(Some(body))
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 500);
        let err = value["error"].as_str().expect("error field");
        assert!(
            err.contains("source connect"),
            "expected source connect error, got {}",
            err
        );
    }

    #[cfg(feature = "ha-tr")]
    #[tokio::test]
    async fn test_shadow_errors_on_empty_body() {
        let err = AdminServer::handle_shadow_request(None).await;
        assert!(err.is_err(), "empty body must surface as Err");
    }

    #[cfg(feature = "anomaly-detection")]
    #[tokio::test]
    async fn test_anomalies_returns_503_when_detector_unattached() {
        let state = Arc::new(AdminState::new());
        let (status, value) = AdminServer::handle_anomalies_list("/anomalies", &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 503);
        assert_eq!(value["error"], "anomaly detector not attached");
    }

    #[cfg(feature = "anomaly-detection")]
    #[tokio::test]
    async fn test_anomalies_returns_attached_detector_events() {
        use crate::anomaly::{AnomalyConfig, AnomalyDetector, QueryObservation};
        let state = Arc::new(AdminState::new());
        let det = Arc::new(AnomalyDetector::new(AnomalyConfig::default()));
        // Seed a SQL injection event into the detector.
        let _ = det.record_query(&QueryObservation {
            tenant: "test".into(),
            fingerprint: "fp".into(),
            sql: "SELECT * FROM users WHERE id = 1 OR 1=1 --".into(),
            timestamp: std::time::Instant::now(),
        });
        state.with_anomaly_detector(det.clone()).await;

        let (status, value) = AdminServer::handle_anomalies_list("/anomalies", &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 200);
        let count = value["count"].as_u64().expect("count field");
        assert!(count > 0, "expected at least one event, got {}", count);
    }

    #[cfg(feature = "anomaly-detection")]
    #[tokio::test]
    async fn test_anomalies_limit_query_string_respected() {
        use crate::anomaly::{AnomalyConfig, AnomalyDetector, QueryObservation};
        let state = Arc::new(AdminState::new());
        let det = Arc::new(AnomalyDetector::new(AnomalyConfig::default()));
        for i in 0..50 {
            let fp = format!("fp{}", i);
            let _ = det.record_query(&QueryObservation {
                tenant: "test".into(),
                fingerprint: fp,
                sql: "SELECT 1".into(),
                timestamp: std::time::Instant::now(),
            });
        }
        state.with_anomaly_detector(det).await;

        let (status, value) = AdminServer::handle_anomalies_list("/anomalies?limit=5", &state)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 200);
        assert_eq!(value["limit"].as_u64().unwrap(), 5);
        assert_eq!(value["events"].as_array().unwrap().len(), 5);
    }

    #[cfg(any(feature = "anomaly-detection", feature = "query-analytics"))]
    #[test]
    fn test_parse_limit_query_helper() {
        assert_eq!(parse_limit_query("/anomalies", 100, 1024), 100);
        assert_eq!(parse_limit_query("/anomalies?limit=42", 100, 1024), 42);
        assert_eq!(parse_limit_query("/anomalies?limit=99999", 100, 1024), 1024);
        assert_eq!(parse_limit_query("/anomalies?limit=abc", 100, 1024), 100);
        assert_eq!(
            parse_limit_query("/anomalies?other=x&limit=7", 100, 1024),
            7
        );
    }

    #[cfg(feature = "edge-proxy")]
    async fn edge_state() -> Arc<AdminState> {
        use crate::edge::{EdgeCache, EdgeRegistry};
        use std::time::Duration;
        let s = Arc::new(AdminState::new());
        let cache = Arc::new(EdgeCache::new(100));
        let registry = Arc::new(EdgeRegistry::new(8, Duration::from_secs(60)));
        s.with_edge(cache, registry).await;
        s
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_status_returns_empty_lists_initially() {
        let s = edge_state().await;
        let (status, value) = AdminServer::handle_edge_status(&s)
            .await
            .expect("handler returns Ok");
        assert_eq!(status, 200);
        assert_eq!(value["edge_count"].as_u64().unwrap(), 0);
        assert_eq!(value["registered"].as_array().unwrap().len(), 0);
        assert!(value["cache"].is_object(), "cache stats present");
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_register_then_status_lists_edge() {
        let s = edge_state().await;
        let body = r#"{"edge_id":"e1","region":"us-east","base_url":"https://e1.svc"}"#;
        let (status, _) = AdminServer::handle_edge_register(Some(body), &s)
            .await
            .expect("handler ok");
        assert_eq!(status, 201);
        let (status2, value2) = AdminServer::handle_edge_status(&s).await.unwrap();
        assert_eq!(status2, 200);
        assert_eq!(value2["edge_count"].as_u64().unwrap(), 1);
        assert_eq!(value2["registered"][0]["edge_id"].as_str().unwrap(), "e1");
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_register_400_on_malformed_body() {
        let s = edge_state().await;
        let (status, _) = AdminServer::handle_edge_register(Some("not json"), &s)
            .await
            .expect("handler ok");
        assert_eq!(status, 400);
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_invalidate_drops_local_cache_entries() {
        use crate::edge::{CacheEntry, CacheKey};
        use std::time::{Duration, Instant};
        let s = edge_state().await;
        // Seed an entry into the local cache.
        let cache = s.edge_cache.read().await.clone().unwrap();
        cache.insert(
            CacheKey::new("fp1", "p1"),
            CacheEntry {
                version: 1,
                response_bytes: bytes::Bytes::from_static(b"row"),
                tables: vec!["users".into()],
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );
        assert!(cache.get(&CacheKey::new("fp1", "p1")).is_some());

        let body = r#"{"tables":["users"]}"#;
        let (status, value) = AdminServer::handle_edge_invalidate(Some(body), &s)
            .await
            .expect("handler ok");
        assert_eq!(status, 200);
        assert_eq!(value["dropped_local"].as_u64().unwrap(), 1);
        assert!(cache.get(&CacheKey::new("fp1", "p1")).is_none());
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_invalidate_503_when_cache_unattached() {
        let s = Arc::new(AdminState::new());
        let body = r#"{"tables":["users"]}"#;
        let (status, _) = AdminServer::handle_edge_invalidate(Some(body), &s)
            .await
            .expect("handler ok");
        assert_eq!(status, 503);
    }

    // ---- edge SSE subscribe: query parsing + gate behaviour ----

    #[cfg(feature = "edge-proxy")]
    #[test]
    fn test_parse_query_params_decodes_and_defaults() {
        let p = parse_query_params(
            "/api/edge/subscribe?edge_id=e1&region=us-east&base_url=http%3A%2F%2Fe1.svc%3A9090",
        );
        assert_eq!(p.get("edge_id").unwrap(), "e1");
        assert_eq!(p.get("region").unwrap(), "us-east");
        assert_eq!(p.get("base_url").unwrap(), "http://e1.svc:9090");
        // No query string at all -> empty map.
        assert!(parse_query_params("/api/edge/subscribe").is_empty());
        // Key without '=' is ignored; truncated / invalid escapes pass
        // through literally.
        let p2 = parse_query_params("/x?flag&edge_id=a%2&b=%zz");
        assert!(!p2.contains_key("flag"));
        assert_eq!(p2.get("edge_id").unwrap(), "a%2");
        assert_eq!(p2.get("b").unwrap(), "%zz");
    }

    /// Drive one raw HTTP request through `handle_connection` over a
    /// real localhost socket (the SSE route is intercepted there,
    /// before `route_request`, so handler-level tests can't reach it).
    #[cfg(feature = "edge-proxy")]
    async fn connect_admin(state: Arc<AdminState>) -> tokio::net::TcpStream {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            let _ = AdminServer::handle_connection(stream, peer, state).await;
        });
        tokio::net::TcpStream::connect(addr).await.unwrap()
    }

    /// Read from `stream` until `needle` appears (or a 2s deadline)
    /// and return everything read so far.
    #[cfg(feature = "edge-proxy")]
    async fn read_until(stream: &mut tokio::net::TcpStream, needle: &str) -> String {
        let mut buf = Vec::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let mut chunk = [0u8; 1024];
            let n = tokio::time::timeout_at(deadline, stream.read(&mut chunk))
                .await
                .expect("read timed out")
                .expect("read failed");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if String::from_utf8_lossy(&buf).contains(needle) {
                break;
            }
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_subscribe_unauthenticated_gets_401() {
        let s = edge_state().await;
        *s.auth_token.write().await = Some("s3cret".to_string());
        let mut c = connect_admin(s).await;
        c.write_all(b"GET /api/edge/subscribe?edge_id=e1 HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let resp = read_until(&mut c, "401").await;
        assert!(resp.starts_with("HTTP/1.1 401"), "got: {resp}");
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_subscribe_400_without_edge_id() {
        let s = edge_state().await;
        let mut c = connect_admin(s).await;
        c.write_all(b"GET /api/edge/subscribe?region=us-east HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let resp = read_until(&mut c, "edge_id").await;
        assert!(resp.starts_with("HTTP/1.1 400"), "got: {resp}");
        assert!(resp.contains("edge_id query parameter is required"));
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_subscribe_503_when_registry_full() {
        use crate::edge::{EdgeCache, EdgeRegistry};
        let s = Arc::new(AdminState::new());
        s.with_edge(
            Arc::new(EdgeCache::new(16)),
            Arc::new(EdgeRegistry::new(1, std::time::Duration::from_secs(60))),
        )
        .await;
        let registry = s.edge_registry.read().await.clone().unwrap();
        // Occupy the single slot; the receiver must stay alive so the
        // subscribe below hits CapacityExceeded, not a pruned slot.
        let _held = registry.register("occupant", "r", "u", "ts").unwrap();
        let mut c = connect_admin(s).await;
        c.write_all(b"GET /api/edge/subscribe?edge_id=e2 HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let resp = read_until(&mut c, "full").await;
        assert!(resp.starts_with("HTTP/1.1 503"), "got: {resp}");
    }

    #[cfg(feature = "edge-proxy")]
    #[tokio::test]
    async fn test_edge_subscribe_streams_preamble_and_invalidations() {
        let s = edge_state().await;
        *s.auth_token.write().await = Some("s3cret".to_string());
        let registry = s.edge_registry.read().await.clone().unwrap();
        let mut c = connect_admin(s).await;
        c.write_all(
            b"GET /api/edge/subscribe?edge_id=e1&region=eu&base_url=http%3A%2F%2Fe1 HTTP/1.1\r\nAuthorization: Bearer s3cret\r\n\r\n",
        )
        .await
        .unwrap();
        let preamble = read_until(&mut c, "\r\n\r\n").await;
        assert!(preamble.starts_with("HTTP/1.1 200 OK"), "got: {preamble}");
        assert!(preamble.contains("Content-Type: text/event-stream"));

        // Registration (with percent-decoded params) happened before the
        // preamble was written, so it's visible once we've read it — and
        // the receiver is held open by the handler.
        let nodes = registry.list();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].edge_id, "e1");
        assert_eq!(nodes[0].region, "eu");
        assert_eq!(nodes[0].base_url, "http://e1");

        // The subscribe-time hello frame arrives first: an invalidate
        // event carrying the home's per-boot epoch (never 0) so a
        // reconnecting edge detects restarts and re-syncs immediately.
        // (Its bytes may already have ridden along with the preamble
        // read — accumulate until the frame's closing brace.)
        let mut hello = preamble;
        if !hello.contains("}\n\n") {
            hello.push_str(&read_until(&mut c, "}\n\n").await);
        }
        assert!(hello.contains("event: invalidate"), "got: {hello}");
        assert!(hello.contains("\"epoch\":"), "got: {hello}");
        assert!(
            !hello.contains("\"epoch\":0}"),
            "hello epoch must be non-zero: {hello}"
        );

        // A broadcast arrives as an SSE invalidate frame.
        let (sent, _) = registry
            .broadcast(InvalidationEvent {
                up_to_version: 7,
                tables: vec!["users".into()],
                committed_at: "ts".into(),
                epoch: 0,
            })
            .await;
        assert_eq!(sent, 1);
        let frame = read_until(&mut c, "\"up_to_version\":7").await;
        assert!(frame.contains("event: invalidate"), "got: {frame}");
        assert!(frame.contains("\"up_to_version\":7"), "got: {frame}");
    }
}
