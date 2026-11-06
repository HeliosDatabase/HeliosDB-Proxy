//! MCP (Model Context Protocol) agent gateway.
//!
//! When `[mcp] enabled = true`, the proxy exposes a native MCP server so AI
//! agents call structured, policy-gated tools (`query`, `list_tables`,
//! `explain`) instead of opening raw SQL connections. This is the AI-data-
//! plane differentiator: every tool call goes through one auditable surface
//! and a read-only-by-default guardrail, and runs over the proxy's backend
//! PG-wire client so it is backend-agnostic (PostgreSQL or HeliosDB-Nano).
//!
//! Transport: JSON-RPC 2.0 over HTTP POST (the simplest MCP transport; an
//! SSE/Streamable-HTTP upgrade is a follow-on). Methods implemented:
//! `initialize`, `notifications/initialized`, `ping`, `tools/list`,
//! `tools/call`.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::agent_contract::{self, AgentContract};
use crate::backend::client::{BoundedQueryResult, QueryResult};
use crate::backend::types::TextValue;
use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::config::{McpConfig, MCP_DEFAULT_PRINCIPAL};
use crate::gateway_policy::{GatewayPolicy, RequestIdentity};
use crate::{ProxyError, Result};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// The backend side of one MCP connection: its pool, the request policies
/// and the client's address (for the rate-limit key).
/// Tool results cut by `[mcp] max_result_rows` / `max_result_bytes`, process-wide.
static RESULTS_TRUNCATED: AtomicU64 = AtomicU64::new(0);

/// Total `query` / `list_tables` / `explain` results cut by the result caps
/// since the process started (`heliosdb_proxy_mcp_results_truncated_total`).
pub fn results_truncated_total() -> u64 {
    RESULTS_TRUNCATED.load(Ordering::Relaxed)
}

struct Gateway<'a> {
    pool: &'a crate::gateway_pool::SharedBackendPool,
    policy: &'a GatewayPolicy,
    peer: Option<std::net::IpAddr>,
    /// The tenant the request acts for (`[multi_tenancy]` + `tenant_header`).
    tenant: Option<&'a crate::gateway_policy::GatewayTenant>,
    /// The principal the request's bearer token named ([`Principals`]).
    principal: &'a Arc<str>,
    /// The tool-result cache, when `[mcp] tool_cache_enabled`.
    #[cfg(feature = "query-cache")]
    cache: Option<&'a crate::mcp_cache::McpToolCache>,
}

impl Gateway<'_> {
    /// The tool cache's `query` and `list_tables` TTLs, when it runs.
    fn cache_ttls(&self) -> Option<(Duration, Duration)> {
        #[cfg(feature = "query-cache")]
        if let Some(cache) = self.cache {
            let s = cache.settings();
            return Some((s.ttl, s.catalog_ttl));
        }
        None
    }
}

/// A tool whose result the tool cache may serve (`[mcp] tool_cache_*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CachedTool {
    /// `query`.
    Query,
    /// `list_tables`.
    ListTables,
}

impl CachedTool {
    /// Number of cacheable tools.
    pub const COUNT: usize = 2;
    /// Every cacheable tool, in label order.
    pub const ALL: [CachedTool; CachedTool::COUNT] = [CachedTool::Query, CachedTool::ListTables];

    /// The tool name (the `tool` metric label).
    pub fn as_str(self) -> &'static str {
        match self {
            CachedTool::Query => "query",
            CachedTool::ListTables => "list_tables",
        }
    }

    /// Position in [`Self::ALL`].
    pub fn index(self) -> usize {
        match self {
            CachedTool::Query => 0,
            CachedTool::ListTables => 1,
        }
    }
}

/// A tool result: its text and, when a result cap cut it, the
/// `structuredContent` truncation note.
pub type ToolOutput = (String, Option<Value>);

/// The statement `list_tables` runs.
const LIST_TABLES_SQL: &str = "SELECT table_schema, table_name FROM information_schema.tables \
                               WHERE table_schema NOT IN ('pg_catalog','information_schema') \
                               ORDER BY table_schema, table_name";

/// The bearer tokens the MCP gateway accepts and the principal each names:
/// `[mcp] auth_token` names [`MCP_DEFAULT_PRINCIPAL`], each
/// `[[mcp.principals]]` entry its `id` (Sprinter 20bcc218385f). Only the
/// tokens' SHA-256 digests are kept.
pub(crate) struct Principals {
    /// `(principal id, SHA-256 of its token)`.
    entries: Vec<(Arc<str>, [u8; 32])>,
    /// [`MCP_DEFAULT_PRINCIPAL`], the principal of every request when no
    /// token is configured.
    default: Arc<str>,
}

impl Principals {
    /// The principals `cfg` configures.
    pub(crate) fn from_config(cfg: &McpConfig) -> Self {
        let default: Arc<str> = Arc::from(MCP_DEFAULT_PRINCIPAL);
        let digest = |token: &str| -> [u8; 32] { Sha256::digest(token.as_bytes()).into() };
        let mut entries = Vec::with_capacity(cfg.principals.len() + 1);
        if let Some(token) = cfg.auth_token.as_deref() {
            entries.push((default.clone(), digest(token)));
        }
        for p in &cfg.principals {
            entries.push((Arc::from(p.id.as_str()), digest(&p.token)));
        }
        Self { entries, default }
    }

    /// The principal a request whose `Authorization` header value is
    /// `authorization` acts as: `default` when no token is configured;
    /// otherwise the principal whose token its `Bearer` token is, found by
    /// comparing SHA-256 digests in constant time against every principal,
    /// with no early exit. `None` (answer 401) for a missing, malformed or
    /// unknown token.
    pub(crate) fn resolve(&self, authorization: Option<&str>) -> Option<Arc<str>> {
        if self.entries.is_empty() {
            return Some(self.default.clone());
        }
        let presented = authorization.and_then(|v| v.strip_prefix("Bearer "));
        let digest: [u8; 32] = Sha256::digest(presented.unwrap_or("").as_bytes()).into();
        let mut hit = usize::MAX;
        for (i, (_, expected)) in self.entries.iter().enumerate() {
            let diff = digest
                .iter()
                .zip(expected)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b));
            // All ones when the digests are equal, zero otherwise.
            let mask = ((diff as usize).wrapping_sub(1) >> (usize::BITS - 1)).wrapping_neg();
            hit = (hit & !mask) | (i & mask);
        }
        presented
            .and_then(|_| self.entries.get(hit))
            .map(|(id, _)| id.clone())
    }

    /// Number of principals that hold a token (0: every caller is
    /// `default`).
    pub(crate) fn count(&self) -> usize {
        self.entries.len()
    }
}

/// What every connection of one MCP gateway shares.
pub(crate) struct McpShared {
    cfg: McpConfig,
    contract: Option<AgentContract>,
    pool: crate::gateway_pool::SharedBackendPool,
    policy: crate::gateway_policy::SharedGatewayPolicy,
    /// Admission cap (H-06 `max_concurrent_requests`): guards only
    /// `tools/call` (the method that runs SQL against the backend); the
    /// cheap protocol handshake methods and notifications are never gated.
    admission: tokio::sync::Semaphore,
    principals: Principals,
    /// The tool-result cache, when `[mcp] tool_cache_enabled`.
    #[cfg(feature = "query-cache")]
    cache: Option<Arc<crate::mcp_cache::McpToolCache>>,
}

/// The MCP gateway server.
pub struct McpServer {
    config: McpConfig,
    contract: Option<AgentContract>,
    pool: crate::gateway_pool::SharedBackendPool,
    policy: crate::gateway_policy::SharedGatewayPolicy,
    /// Refuse to start in read-only mode when the backend role can write
    /// (`strict_config`); otherwise only warn (H-06).
    strict_read_only_role: bool,
    /// The tool-result cache (`[mcp] tool_cache_enabled`), built by the
    /// daemon on its shared write generations.
    #[cfg(feature = "query-cache")]
    cache: Option<Arc<crate::mcp_cache::McpToolCache>>,
}

impl McpServer {
    pub fn new(
        config: McpConfig,
        contract: Option<AgentContract>,
        pool: crate::gateway_pool::SharedBackendPool,
    ) -> Self {
        Self {
            config,
            contract,
            pool,
            policy: Arc::new(GatewayPolicy::default()),
            strict_read_only_role: false,
            #[cfg(feature = "query-cache")]
            cache: None,
        }
    }

    /// Serve `query` and `list_tables` through `cache` (`[mcp]
    /// tool_cache_enabled`). The cache belongs to this gateway: `[mcp]` is
    /// read at startup only, so nothing reloads under it.
    #[cfg(feature = "query-cache")]
    pub fn with_tool_cache(mut self, cache: Option<Arc<crate::mcp_cache::McpToolCache>>) -> Self {
        self.cache = cache;
        self
    }

    /// With `true` (the daemon passes `strict_config`), read-only mode
    /// refuses to start when the backend role holds write privileges.
    pub fn with_strict_read_only_role(mut self, strict: bool) -> Self {
        self.strict_read_only_role = strict;
        self
    }

    /// Whether the backend role can INSERT/UPDATE/DELETE/TRUNCATE any user
    /// table. Read-only mode is then only the GUC backstop plus the lexical
    /// guard, not a role-level guarantee.
    async fn backend_role_can_write(cfg: &McpConfig) -> std::result::Result<bool, String> {
        let bcfg = BackendConfig {
            host: cfg.backend_host.clone(),
            port: cfg.backend_port,
            user: cfg.backend_user.clone(),
            password: cfg.backend_password.clone(),
            database: cfg.backend_database.clone(),
            application_name: Some("heliosproxy-mcp-probe".to_string()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(cfg.query_timeout_ms),
            query_timeout: Duration::from_millis(cfg.query_timeout_ms),
            tls_config: default_client_config(),
        };
        let mut client = crate::backend::BackendClient::connect(&bcfg)
            .await
            .map_err(|e| e.to_string())?;
        let res = client
            .simple_query(
                "SELECT coalesce(bool_or(has_table_privilege(c.oid, \
                 'INSERT,UPDATE,DELETE,TRUNCATE')), false) \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relkind IN ('r', 'p') \
                 AND n.nspname NOT IN ('pg_catalog', 'information_schema')",
            )
            .await
            .map_err(|e| e.to_string());
        client.close().await;
        let res = res?;
        Ok(matches!(
            res.rows.first().and_then(|r| r.first()),
            Some(TextValue::Text(v)) if v == "t"
        ))
    }

    /// Apply the PG-wire request policies (rate limit, rewrite, circuit
    /// breaker) to this gateway's tool calls (H-06).
    pub fn with_policy(mut self, policy: crate::gateway_policy::SharedGatewayPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Bind and serve the MCP HTTP endpoint until the task is dropped.
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address)
            .await
            .map_err(|e| {
                ProxyError::Network(format!("MCP bind {}: {}", self.config.listen_address, e))
            })?;
        tracing::info!(addr = %self.config.listen_address, read_only = self.config.read_only,
            contract = ?self.contract.as_ref().map(|c| &c.id), "MCP agent gateway listening");
        // H-06: read-only should be a role property, not only the GUC
        // backstop + lexical guard (volatile functions can still write).
        if self.config.read_only {
            match Self::backend_role_can_write(&self.config).await {
                Ok(true) if self.strict_read_only_role => {
                    return Err(ProxyError::Config(format!(
                        "[mcp] read_only = true but backend_user {:?} can write user tables; \
                         use a role without INSERT/UPDATE/DELETE/TRUNCATE privileges \
                         (strict_config = true refuses this)",
                        self.config.backend_user
                    )));
                }
                Ok(true) => tracing::warn!(
                    backend_user = %self.config.backend_user,
                    "[mcp] read_only = true but the backend role can write user tables; \
                     read-only rests on the default_transaction_read_only backstop and the \
                     statement guard only (volatile functions can still write)"
                ),
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    "[mcp] could not check the backend role's write privileges"
                ),
            }
        }
        let shared = self.into_shared();
        if shared.principals.count() > 0 {
            tracing::info!(
                principals = shared.principals.count(),
                "MCP gateway: bearer tokens select the principal"
            );
        }
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("MCP accept error: {}", e);
                    continue;
                }
            };
            let shared = shared.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(stream, peer, shared).await {
                    tracing::debug!(%peer, "MCP connection error: {}", e);
                }
            });
        }
    }

    /// What this gateway's connections share.
    pub(crate) fn into_shared(self) -> Arc<McpShared> {
        Arc::new(McpShared {
            admission: tokio::sync::Semaphore::new(self.config.max_concurrent_requests),
            principals: Principals::from_config(&self.config),
            cfg: self.config,
            contract: self.contract,
            pool: self.pool,
            policy: self.policy,
            #[cfg(feature = "query-cache")]
            cache: self.cache,
        })
    }

    pub(crate) async fn handle_connection(
        mut stream: tokio::net::TcpStream,
        peer: std::net::SocketAddr,
        shared: Arc<McpShared>,
    ) -> Result<()> {
        use crate::http_util;
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);

        // Bounded request read: overall deadline + header count/byte caps.
        let deadline = tokio::time::Instant::now() + http_util::HTTP_READ_TIMEOUT;
        let head = match http_util::read_head(&mut reader, deadline).await {
            Ok(h) => h,
            Err(_) => return Ok(()), // timeout / oversized headers / early close
        };

        // Bearer auth, when configured: the token selects the principal.
        // Unlike the wire port there is no pass-through identity here — the
        // gateway runs SQL under its own backend credentials — so an
        // unauthenticated request must be refused. Constant-time comparison
        // of digests against every principal (no `==` oracle, no early exit).
        let Some(principal) = shared.principals.resolve(head.header("authorization")) else {
            Self::write_http(
                &mut writer,
                401,
                "application/json",
                br#"{"error":"unauthorized"}"#,
            )
            .await?;
            return Ok(());
        };
        let (cfg, policy) = (&shared.cfg, &*shared.policy);

        // Reject an oversized declared body BEFORE allocating for it.
        if head.content_length > http_util::MAX_HTTP_BODY_BYTES {
            Self::write_http(
                &mut writer,
                413,
                "application/json",
                br#"{"error":"request body too large"}"#,
            )
            .await?;
            return Ok(());
        }
        let body = match http_util::read_body(&mut reader, head.content_length, deadline).await {
            Ok(b) => String::from_utf8_lossy(&b).to_string(),
            Err(_) => return Ok(()),
        };

        // H-06: the tenant this request acts for, when `tenant_header` is set.
        let tenant = match policy.request_tenant(|name| head.header(name)) {
            Ok(t) => t,
            Err(denied) => {
                let body = json!({ "error": denied.to_string() }).to_string();
                Self::write_http(
                    &mut writer,
                    denied.http_status(),
                    "application/json",
                    body.as_bytes(),
                )
                .await?;
                return Ok(());
            }
        };
        let gateway = Gateway {
            pool: &shared.pool,
            policy,
            peer: Some(peer.ip()),
            tenant: tenant.as_ref(),
            principal: &principal,
            #[cfg(feature = "query-cache")]
            cache: shared.cache.as_deref(),
        };
        let response = Self::dispatch_with(
            &body,
            cfg,
            shared.contract.as_ref(),
            &gateway,
            &shared.admission,
        )
        .await;
        match response {
            Some(v) => {
                let payload = serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string());
                Self::write_http(&mut writer, 200, "application/json", payload.as_bytes()).await
            }
            // Notifications get a bare 202 with no JSON-RPC body.
            None => Self::write_http(&mut writer, 202, "application/json", b"").await,
        }
    }

    /// [`Self::dispatch_with`] with no gateway policy (tests).
    #[cfg(test)]
    async fn dispatch(
        body: &str,
        cfg: &McpConfig,
        contract: Option<&AgentContract>,
        pool: &crate::gateway_pool::SharedBackendPool,
        admission: &tokio::sync::Semaphore,
    ) -> Option<Value> {
        let policy = GatewayPolicy::default();
        let principal: Arc<str> = Arc::from(MCP_DEFAULT_PRINCIPAL);
        let gateway = Gateway {
            pool,
            policy: &policy,
            peer: None,
            tenant: None,
            principal: &principal,
            #[cfg(feature = "query-cache")]
            cache: None,
        };
        Self::dispatch_with(body, cfg, contract, &gateway, admission).await
    }

    /// Dispatch one JSON-RPC request. Returns `None` for notifications.
    async fn dispatch_with(
        body: &str,
        cfg: &McpConfig,
        contract: Option<&AgentContract>,
        gateway: &Gateway<'_>,
        admission: &tokio::sync::Semaphore,
    ) -> Option<Value> {
        let req: Value = match serde_json::from_str(body) {
            Ok(v) => v,
            Err(e) => {
                return Some(rpc_error(
                    Value::Null,
                    -32700,
                    &format!("parse error: {}", e),
                ))
            }
        };
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        let params = req.get("params").cloned().unwrap_or(json!({}));

        match method {
            "initialize" => Some(rpc_ok(
                id,
                json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "serverInfo": { "name": "heliosproxy-mcp", "version": crate::VERSION },
                    "capabilities": { "tools": { "listChanged": false } }
                }),
            )),
            // Notifications (no id) — no response.
            "notifications/initialized" | "notifications/cancelled" => None,
            "ping" => Some(rpc_ok(id, json!({}))),
            "tools/list" => Some(rpc_ok(
                id,
                json!({ "tools": Self::tool_defs(cfg, gateway.cache_ttls()) }),
            )),
            "tools/call" => {
                // Admission cap (H-06 `max_concurrent_requests`): this is the
                // one method that runs SQL against the backend, so it is the
                // only one gated — a saturated gateway still answers cheap
                // protocol calls (`initialize`/`ping`/`tools/list`).
                let _permit = match admission.try_acquire() {
                    Ok(p) => p,
                    Err(_) => {
                        return Some(rpc_error(
                            id,
                            -32000,
                            "server busy: too many concurrent MCP requests, retry shortly",
                        ))
                    }
                };
                Some(Self::handle_tool_call(id, &params, cfg, contract, gateway).await)
            }
            other => Some(rpc_error(
                id,
                -32601,
                &format!("method not found: {}", other),
            )),
        }
    }

    /// The tool catalog. With the tool cache on (`cache_ttls` = the `query`
    /// and `list_tables` TTLs), those two tools say how old a result may be
    /// and take an optional `fresh`; with it off the catalog is unchanged.
    fn tool_defs(cfg: &McpConfig, cache_ttls: Option<(Duration, Duration)>) -> Value {
        let query_desc = if cfg.read_only {
            "Run a read-only SQL query and return rows. Writes/DDL are refused."
        } else {
            "Run a SQL query and return rows (or the command tag for writes)."
        };
        let list_desc = "List user tables (schema.table) in the connected database.";
        let mut query_props =
            json!({ "sql": { "type": "string", "description": "SQL to execute" } });
        let mut list_props = json!({});
        let (query_desc, list_desc) = match cache_ttls {
            None => (query_desc.to_string(), list_desc.to_string()),
            Some((ttl, catalog_ttl)) => {
                let fresh = json!({
                    "type": "boolean",
                    "description": "Skip the cached result and read the database now (the result \
                                    is cached again)."
                });
                query_props["fresh"] = fresh.clone();
                list_props["fresh"] = fresh;
                (
                    format!(
                        "{query_desc} A read may be answered from the proxy's cache: writes through \
                         this proxy retire it at once, writes made outside this proxy may show up \
                         to {} s late; pass \"fresh\": true to read the database now.",
                        ttl.as_secs()
                    ),
                    if catalog_ttl.is_zero() {
                        list_desc.to_string()
                    } else {
                        format!(
                            "{list_desc} May be answered from the proxy's cache: DDL through this \
                             proxy retires it at once, DDL made outside this proxy may show up \
                             to {} s late; pass \"fresh\": true to read the database now.",
                            catalog_ttl.as_secs()
                        )
                    },
                )
            }
        };
        json!([
            {
                "name": "query",
                "description": query_desc,
                "inputSchema": {
                    "type": "object",
                    "properties": query_props,
                    "required": ["sql"]
                }
            },
            {
                "name": "list_tables",
                "description": list_desc,
                "inputSchema": { "type": "object", "properties": list_props }
            },
            {
                "name": "explain",
                "description": "Return the query plan for a SQL statement (EXPLAIN).",
                "inputSchema": {
                    "type": "object",
                    "properties": { "sql": { "type": "string" } },
                    "required": ["sql"]
                }
            }
        ])
    }

    async fn handle_tool_call(
        id: Value,
        params: &Value,
        cfg: &McpConfig,
        contract: Option<&AgentContract>,
        gateway: &Gateway<'_>,
    ) -> Value {
        let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let args = params.get("arguments").cloned().unwrap_or(json!({}));

        let fresh = args.get("fresh").and_then(Value::as_bool).unwrap_or(false);
        let read_only = effective_read_only(cfg, contract);
        // `meta`: the tool cache's `_meta`, for a call that reached it.
        let (result, meta): (std::result::Result<ToolOutput, String>, Option<Value>) = match name {
            "query" => {
                let sql = args
                    .get("sql")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .trim();
                if sql.is_empty() {
                    (Err("missing 'sql'".to_string()), None)
                } else {
                    match Self::check_policy(cfg, contract, sql) {
                        Err(hint) => (Err(hint), None),
                        Ok(()) => {
                            Self::run_tool(cfg, gateway, sql, read_only, CachedTool::Query, fresh)
                                .await
                        }
                    }
                }
            }
            "list_tables" => {
                Self::run_tool(
                    cfg,
                    gateway,
                    LIST_TABLES_SQL,
                    read_only,
                    CachedTool::ListTables,
                    fresh,
                )
                .await
            }
            "explain" => {
                let sql = args
                    .get("sql")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .trim();
                let result = if sql.is_empty() {
                    Err("missing 'sql'".to_string())
                } else {
                    match Self::check_policy(cfg, contract, sql) {
                        Err(hint) => Err(hint),
                        // Policy is checked against the RAW sql (so `EXPLAIN a;
                        // DROP b` is caught as multi-statement); the `EXPLAIN `
                        // prefix is applied only for execution. Never cached:
                        // EXPLAIN ANALYZE runs the statement, and plans follow
                        // statistics no write generation tracks.
                        Ok(()) => {
                            Self::run_sql(cfg, gateway, &format!("EXPLAIN {}", sql), read_only)
                                .await
                                .map(render_result)
                        }
                    }
                };
                (result, None)
            }
            other => (Err(format!("unknown tool: {}", other)), None),
        };

        let mut result = match result {
            Ok((text, structured)) => {
                tracing::info!(tool = %name, principal = %gateway.principal, "MCP tool call ok");
                let mut result =
                    json!({ "content": [{ "type": "text", "text": text }], "isError": false });
                if let Some(structured) = structured {
                    result["structuredContent"] = structured;
                }
                result
            }
            Err(e) => {
                tracing::info!(tool = %name, principal = %gateway.principal, error = %e,
                    "MCP tool call error");
                // Tool errors are reported in-band (isError) per MCP, not as a
                // protocol error, so the agent can read + self-correct.
                json!({ "content": [{ "type": "text", "text": e }], "isError": true })
            }
        };
        // The tool cache's outcome (hit, age, stored, skip reason): never
        // text derived from data. Absent with the cache off.
        if let Some(meta) = meta {
            result["_meta"] = meta;
        }
        rpc_ok(id, result)
    }

    /// Run a cacheable tool's statement: the request policies, then the tool
    /// cache when it runs (keyed by owner, tool and the final statement),
    /// else the backend. The result, and the cache's `_meta` when it ran.
    async fn run_tool(
        cfg: &McpConfig,
        gateway: &Gateway<'_>,
        sql: &str,
        read_only: bool,
        tool: CachedTool,
        fresh: bool,
    ) -> (std::result::Result<ToolOutput, String>, Option<Value>) {
        let final_sql = match Self::prepare_sql(cfg, gateway, sql).await {
            Ok(text) => text,
            Err(denied) => return (Err(denied), None),
        };
        #[cfg(feature = "query-cache")]
        if let Some(cache) = gateway.cache {
            let owner = crate::mcp_cache::Owner::new(
                gateway.principal.clone(),
                gateway.tenant.map(|t| t.0.as_str()),
            );
            let (result, outcome) = cache
                .serve(&owner, tool, &final_sql, fresh, || async {
                    Self::execute_sql(cfg, gateway, &final_sql, read_only)
                        .await
                        .map(render_result)
                })
                .await;
            return (result, Some(outcome.meta()));
        }
        let _ = (tool, fresh);
        (
            Self::execute_sql(cfg, gateway, &final_sql, read_only)
                .await
                .map(render_result),
            None,
        )
    }

    /// Gate a SQL statement: when an agent contract is configured, validate
    /// against it and return a structured JSON repair hint on violation;
    /// otherwise apply the plain read-only guardrail.
    ///
    /// Both the contract path and the read-only path historically inspected
    /// only the leading verb of the string, which two shapes bypass:
    ///
    /// 1. multi-statement batches (`SELECT 1; DROP TABLE t`) — PostgreSQL's
    ///    simple-query protocol runs every `;`-separated statement, so the
    ///    trailing write executes even though the head is a SELECT; this also
    ///    defeats contract verb/table allow-lists.
    /// 2. data-modifying CTEs (`WITH x AS (INSERT ... RETURNING *) SELECT ...`)
    ///    — the head is WITH but the statement writes.
    ///
    /// We close both here (a lexical guard) and again in `run_sql` (a backend
    /// READ ONLY backstop).
    fn check_policy(
        cfg: &McpConfig,
        contract: Option<&AgentContract>,
        sql: &str,
    ) -> std::result::Result<(), String> {
        // Universal: refuse multi-statement batches regardless of mode, since
        // a second statement bypasses both the read-only check and contract
        // allow-lists.
        let stmts = split_statements(sql);
        if stmts.len() > 1 {
            return Err("multiple SQL statements are not permitted over the MCP \
                        gateway; send one statement per call"
                .to_string());
        }
        // The single statement to gate. If the splitter found none (e.g. a
        // comment-only body), fall back to the raw trimmed input.
        let stmt = stmts.first().copied().unwrap_or(sql).trim();

        if let Some(c) = contract {
            // `validate` only reads the leading verb; pass the single trimmed
            // statement (not the raw multi-statement string).
            agent_contract::validate(stmt, c).map_err(|v| v.to_json())?;
            // `validate` covers the leading-verb write case but not a
            // data-modifying CTE, so backstop that for a read-only contract.
            if c.read_only && has_data_modifying_cte(stmt) {
                return Err("write via data-modifying CTE refused: this agent \
                            contract is read-only"
                    .to_string());
            }
            Ok(())
        } else if cfg.read_only && (is_write_sql(stmt) || has_data_modifying_cte(stmt)) {
            Err("write/DDL refused: the MCP gateway is read-only".to_string())
        } else {
            Ok(())
        }
    }

    /// Connect to the configured backend, run one statement, return rows.
    ///
    /// When `read_only` is set, the fresh per-call connection is put into
    /// `default_transaction_read_only = on` BEFORE the user statement runs.
    /// This is the hard backstop behind the lexical guard: even if the guard
    /// has a gap, the backend refuses any write. It is safe against
    /// re-enabling — the guard rejects any statement starting with `SET` or
    /// `RESET` (in `is_write_sql`) and rejects multi-statement, and each call
    /// gets a fresh connection, so a single user statement cannot turn the GUC
    /// back off. The user statement runs as its OWN `simple_query` (not
    /// concatenated) so result parsing / command_tag stay correct.
    ///
    /// Known limitation: `default_transaction_read_only` blocks DML/DDL but NOT
    /// volatile functions with side effects (e.g. `SELECT nextval('s')`,
    /// `setval`, superuser file/lo functions). Constrain the gateway's backend
    /// role if that matters for your deployment.
    async fn run_sql(
        cfg: &McpConfig,
        gateway: &Gateway<'_>,
        sql: &str,
        read_only: bool,
    ) -> std::result::Result<BoundedQueryResult, String> {
        let sql = Self::prepare_sql(cfg, gateway, sql).await?;
        Self::execute_sql(cfg, gateway, &sql, read_only).await
    }

    /// The statement as the backend will run it. H-06: the PG-wire rate
    /// limit and rewrite rules apply to tool calls too (after the read-only
    /// guard saw the raw SQL), then the request tenant's row filter. Runs
    /// before the tool cache is consulted, so a hit consumes a rate-limit
    /// token too and a refused statement never reaches the cache.
    async fn prepare_sql<'s>(
        cfg: &McpConfig,
        gateway: &Gateway<'_>,
        sql: &'s str,
    ) -> std::result::Result<Cow<'s, str>, String> {
        let policy = gateway.policy;
        policy
            .admit_request(RequestIdentity {
                peer: gateway.peer,
                user: &cfg.backend_user,
                database: cfg.backend_database.as_deref(),
            })
            .await
            .map_err(|denied| denied.to_string())?;
        let rewritten = policy.rewrite(sql);
        let filtered = policy
            .tenant_sql(&rewritten, gateway.tenant)
            .map_err(|denied| denied.to_string())?;
        // Borrowed: the tenant filter left the rewritten text as it was.
        let filtered = match filtered {
            Cow::Borrowed(_) => None,
            Cow::Owned(text) => Some(text),
        };
        Ok(match filtered {
            Some(text) => Cow::Owned(text),
            None => rewritten,
        })
    }

    /// Run the final statement ([`Self::prepare_sql`]) on the backend: the
    /// circuit breaker (only now, so a cache hit needs no backend), the
    /// pooled connection, the read-only backstop and the bounded read.
    async fn execute_sql(
        cfg: &McpConfig,
        gateway: &Gateway<'_>,
        sql: &str,
        read_only: bool,
    ) -> std::result::Result<BoundedQueryResult, String> {
        let (pool, policy) = (gateway.pool, gateway.policy);
        let node = format!("{}:{}", cfg.backend_host, cfg.backend_port);
        policy
            .check_node(&node)
            .map_err(|denied| denied.to_string())?;
        // `[multi_tenancy]` active-query cap: taken here, where the call runs
        // SQL on the backend, so a tool-cache hit (which needs no backend)
        // takes none; held until this call's result is built, released on
        // every path.
        let _query_permit = policy
            .admit_tenant_query(gateway.tenant)
            .await
            .map_err(|denied| denied.to_string())?;
        let bcfg = BackendConfig {
            host: cfg.backend_host.clone(),
            port: cfg.backend_port,
            user: cfg.backend_user.clone(),
            password: cfg.backend_password.clone(),
            database: cfg.backend_database.clone(),
            application_name: Some("heliosproxy-mcp".to_string()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_millis(cfg.query_timeout_ms),
            tls_config: default_client_config(),
        };
        let mut client = match pool.acquire(&bcfg).await {
            Ok(c) => c,
            Err(e) => {
                policy.record(&node, false, &e.to_string());
                return Err(format!("backend connect: {}", e));
            }
        };
        policy.record(&node, true, "");
        if read_only {
            // Re-assert on every checkout: this pool is MCP-only and every
            // connection it retains has the GUC set, but asserting again is
            // cheap and makes the backstop independent of pool history.
            if let Err(e) = client
                .execute("SET default_transaction_read_only = on")
                .await
            {
                pool.discard(client);
                return Err(format!("failed to enforce read-only mode: {}", e));
            }
        }
        let res = client
            .simple_query_bounded(sql, cfg.max_result_rows, cfg.max_result_bytes)
            .await;
        // C-02: a write retires the cached reads of its tables, whatever the
        // outcome (a statement that timed out may still commit).
        policy
            .note_statement(sql, crate::gateway_policy::GatewayKind::Mcp)
            .await;
        match res {
            Ok(bounded) => {
                // read_only connections are retained (their only state is the
                // GUC, re-asserted on checkout); otherwise only session-neutral
                // statements may be pooled (H-06). A capped result leaves the
                // connection in sync, so it is pooled the same way.
                if read_only || crate::gateway_pool::statement_is_session_neutral(sql) {
                    pool.release(&bcfg, client);
                } else {
                    pool.discard(client);
                }
                if bounded.truncation.is_truncated() {
                    RESULTS_TRUNCATED.fetch_add(1, Ordering::Relaxed);
                }
                Ok(bounded)
            }
            Err(e) => {
                pool.discard(client);
                Err(format!("{}", e))
            }
        }
    }

    async fn write_http(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        status: u16,
        content_type: &str,
        body: &[u8],
    ) -> Result<()> {
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            status,
            if status == 200 { "OK" } else { "Accepted" },
            content_type,
            body.len()
        );
        writer
            .write_all(head.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("MCP write: {}", e)))?;
        if !body.is_empty() {
            writer
                .write_all(body)
                .await
                .map_err(|e| ProxyError::Network(format!("MCP write: {}", e)))?;
        }
        Ok(())
    }
}

fn rpc_ok(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn rpc_error(id: Value, code: i32, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// Render a bounded tool result: the text table, plus `structuredContent` only
/// when a cap cut it (`truncated`, `rows_returned`, `row_limit_hit`,
/// `byte_limit_hit`). An untruncated result carries no extra fields.
fn render_result(b: BoundedQueryResult) -> (String, Option<Value>) {
    let mut text = format_result(&b.result);
    if !b.truncation.is_truncated() {
        return (text, None);
    }
    let reason = match (b.truncation.row_limit_hit, b.truncation.byte_limit_hit) {
        (true, true) => "row and byte",
        (true, false) => "row",
        _ => "byte",
    };
    let rows_returned = b.result.rows.len();
    text.push_str(&format!(
        "\n(truncated: the {reason} limit was reached; the first {rows_returned} rows are shown)"
    ));
    (
        text,
        Some(json!({
            "truncated": true,
            "rows_returned": rows_returned,
            "row_limit_hit": b.truncation.row_limit_hit,
            "byte_limit_hit": b.truncation.byte_limit_hit,
        })),
    )
}

/// Render a QueryResult as a compact text table for the agent.
fn format_result(r: &QueryResult) -> String {
    if r.columns.is_empty() {
        return r.command_tag.clone();
    }
    let header: Vec<&str> = r.columns.iter().map(|c| c.name.as_str()).collect();
    let mut out = String::new();
    out.push_str(&header.join(" | "));
    out.push('\n');
    for row in &r.rows {
        let cells: Vec<String> = row
            .iter()
            .map(|v| match v {
                TextValue::Null => "NULL".to_string(),
                TextValue::Text(s) => s.clone(),
            })
            .collect();
        out.push_str(&cells.join(" | "));
        out.push('\n');
    }
    out.push_str(&format!("({} rows)", r.rows.len()));
    out
}

/// First-keyword write/DDL detection (read-only guardrail). `SET`/`RESET` are
/// included so that, in read-only mode, no single statement can flip the
/// backend's `default_transaction_read_only` GUC back off (see `run_sql`).
fn is_write_sql(sql: &str) -> bool {
    use crate::protocol::starts_with_ci;
    let s = sql.trim_start();
    for kw in [
        "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE",
        "COPY", "MERGE", "CALL", "DO", "VACUUM", "REINDEX", "CLUSTER", "LOCK", "COMMENT", "SET",
        "RESET",
    ] {
        if starts_with_ci(s, kw) {
            return true;
        }
    }
    false
}

/// Effective read-only decision for the gateway: its own `read_only` OR a
/// read-only agent contract. Both the lexical guard (`has_data_modifying_cte`
/// application in `check_policy`) and the backend READ ONLY backstop in
/// `run_sql` key off this. Pure so it can be unit-tested without a backend.
fn effective_read_only(cfg: &McpConfig, contract: Option<&AgentContract>) -> bool {
    cfg.read_only || contract.is_some_and(|c| c.read_only)
}

#[inline]
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// PostgreSQL identifier-continuation byte: `[A-Za-z0-9_$]` AND every high-bit
/// byte `0x80..=0xFF` (any UTF-8 byte of a non-ASCII identifier char such as
/// `é`). Distinct from `is_word_byte` because `$` and the multibyte range are
/// identifier-continuation chars but NOT valid dollar-quote tag bytes nor
/// keyword-boundary word chars. Used ONLY by the dollar-quote-open
/// token-boundary gates: a `$` opens a dollar quote only when it STARTS a token,
/// and PG's lexer keeps `$` inside a running identifier — `a$$b$$` AND `é$$` are
/// each ONE identifier, not a `$$…` dollar quote — so a `$` preceded by another
/// `$` OR by a multibyte identifier byte must NOT be treated as an opener.
/// Omitting the high-bit range reopens the statement-splitter under-count for a
/// multibyte-led alias (`SELECT 1 AS é$$ ; DROP TABLE t`). Mirrors the `< 0x80`
/// multibyte guard already used in `skip_single_quoted`.
#[inline]
fn is_ident_cont(b: u8) -> bool {
    is_word_byte(b) || b == b'$' || b >= 0x80
}

/// `bytes[i]` is `'` — return the index just past the closing quote (or
/// end-of-input if unterminated). `''` is an in-string escape, not a close.
///
/// If the string is an escape-string (`E'…'` / `e'…'` — a standalone `E`
/// immediately before the quote), a backslash escapes the next byte, so `\'`
/// does NOT close the string. Regular strings are treated as
/// standard-conforming (PostgreSQL's default: backslash is literal, only `''`
/// escapes), which is what the MCP gateway's fresh backend sessions use.
///
/// Detecting the `E` prefix EXACTLY matters for safety: a false positive would
/// skip a real closing quote and could hide a top-level `;` (an under-count →
/// bypass). So we require the preceding byte to be `E`/`e` AND the byte before
/// THAT to be an ASCII non-word boundary — a multibyte identifier char before
/// `E` (`éE'…'`, where `éE` is one identifier) is deliberately NOT treated as
/// an escape-string, which at worst over-rejects (fail-closed, safe).
fn skip_single_quoted(bytes: &[u8], i: usize) -> usize {
    let escape_string = i >= 1
        && (bytes[i - 1] == b'E' || bytes[i - 1] == b'e')
        && (i < 2 || (bytes[i - 2] < 0x80 && !is_word_byte(bytes[i - 2])));
    let mut j = i + 1;
    while j < bytes.len() {
        match bytes[j] {
            b'\\' if escape_string => {
                j += 2; // backslash escapes the next byte in an E'…' string
                continue;
            }
            b'\'' => {
                if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                    j += 2; // doubled quote → literal, stay in string
                    continue;
                }
                return j + 1;
            }
            _ => j += 1,
        }
    }
    bytes.len()
}

/// `bytes[i]` is `"` — return the index just past the closing quote (or
/// end-of-input if unterminated). `""` is an in-identifier escape.
fn skip_double_quoted(bytes: &[u8], i: usize) -> usize {
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == b'"' {
            if j + 1 < bytes.len() && bytes[j + 1] == b'"' {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    bytes.len()
}

/// `bytes[i..]` starts a `--` line comment — return the index of the
/// terminating newline (or end-of-input).
fn skip_line_comment(bytes: &[u8], i: usize) -> usize {
    let mut j = i + 2;
    while j < bytes.len() && bytes[j] != b'\n' {
        j += 1;
    }
    j
}

/// `bytes[i..]` starts a `/*` block comment — return the index just past the
/// matching `*/`. Postgres block comments NEST, so depth is tracked.
fn skip_block_comment(bytes: &[u8], i: usize) -> usize {
    let mut depth = 0usize;
    let mut j = i;
    while j + 1 < bytes.len() {
        if bytes[j] == b'/' && bytes[j + 1] == b'*' {
            depth += 1;
            j += 2;
        } else if bytes[j] == b'*' && bytes[j + 1] == b'/' {
            depth -= 1;
            j += 2;
            if depth == 0 {
                return j;
            }
        } else {
            j += 1;
        }
    }
    bytes.len()
}

/// If a dollar-quoted string opens at `i` (`bytes[i]` is `$`), return the
/// index just past its matching closing delimiter; else `None`. Handles the
/// empty tag (`$$...$$`) and named tags (`$tag$...$tag$`). A tag follows
/// identifier rules (starts with a letter/underscore, then alphanumerics/
/// underscores), so `$1` (a parameter placeholder) is NOT a dollar quote.
fn skip_dollar_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    // Parse an optional tag between the two `$`s.
    if j < bytes.len() && bytes[j] != b'$' {
        let c = bytes[j];
        if !(c.is_ascii_alphabetic() || c == b'_') {
            return None; // first tag char must not be a digit / symbol
        }
        j += 1;
        while j < bytes.len() && bytes[j] != b'$' {
            if !is_word_byte(bytes[j]) {
                return None; // invalid tag char → not a dollar quote
            }
            j += 1;
        }
    }
    if j >= bytes.len() || bytes[j] != b'$' {
        return None; // no closing `$` for the opening tag
    }
    let tag = &bytes[i..=j]; // includes both delimiting `$`s
    let mut k = j + 1;
    while k + tag.len() <= bytes.len() {
        if &bytes[k..k + tag.len()] == tag {
            return Some(k + tag.len());
        }
        k += 1;
    }
    Some(bytes.len()) // unterminated → consume to end
}

/// Split `sql` on top-level `;`, skipping semicolons that fall inside
/// single-quoted strings, double-quoted identifiers, dollar-quoted strings,
/// line comments (`-- … \n`), and (nested) block comments (`/* … */`).
/// Returns trimmed, non-empty statement slices, so a trailing `;` does not
/// produce an empty final statement.
fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => i = skip_line_comment(bytes, i),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => i = skip_block_comment(bytes, i),
            // A `$` only opens a dollar-quote when it STARTS a token. PostgreSQL's
            // lexer treats `$` as an identifier-continuation char (ident_cont =
            // [A-Za-z_0-9$]), so `a$x$` AND `a$$b$$` lex as SINGLE identifiers,
            // not as dollar-quote delimiters. The gate must therefore reject a `$`
            // preceded by ANY ident_cont byte — INCLUDING another `$` (that is why
            // `is_ident_cont`, not `is_word_byte`, is used here): otherwise the
            // second `$` of `a$$b$$` is misread as a tag opener, `skip_dollar_quoted`
            // runs off to EOF, and a real top-level `;` is hidden (e.g.
            // `SELECT 1 AS a$$b$$ ; DROP TABLE t` would under-count to one statement).
            b'$' if i == 0 || !is_ident_cont(bytes[i - 1]) => match skip_dollar_quoted(bytes, i) {
                Some(end) => i = end,
                None => i += 1,
            },
            b';' => {
                let seg = sql[start..i].trim();
                if !seg.is_empty() {
                    out.push(seg);
                }
                start = i + 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    let seg = sql[start..].trim();
    if !seg.is_empty() {
        out.push(seg);
    }
    out
}

/// Does a single statement whose head is `WITH` contain a data-modifying
/// sub-statement (INSERT/UPDATE/DELETE/MERGE) in code — i.e. OUTSIDE string
/// literals, quoted identifiers, comments, and dollar-quoted strings? A
/// leading-verb check misses `WITH x AS (INSERT … RETURNING *) SELECT …`,
/// which writes, so this backstops it.
///
/// Only meaningful when the head is `WITH` (also `WITH RECURSIVE`); returns
/// false otherwise. Matching is whole-word (ASCII boundaries) so `insert_col`
/// / `deleted_at` do NOT match. Trade-off: a read-only WITH that mentions one
/// of these words as an UNQUOTED identifier would be conservatively rejected;
/// that favors safety and such an identifier would normally need quoting.
fn has_data_modifying_cte(stmt: &str) -> bool {
    use crate::protocol::starts_with_ci;
    if !starts_with_ci(stmt.trim_start(), "WITH") {
        return false;
    }
    let bytes = stmt.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => i = skip_line_comment(bytes, i),
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => i = skip_block_comment(bytes, i),
            // Same token-boundary gate as `split_statements`: only treat `$` as a
            // dollar-quote opener when it starts a token, so a `$` inside an
            // identifier cannot swallow the rest of the statement and hide an
            // INSERT/UPDATE/DELETE/MERGE keyword from this scan.
            b'$' if i == 0 || !is_ident_cont(bytes[i - 1]) => match skip_dollar_quoted(bytes, i) {
                Some(end) => i = end,
                None => i += 1,
            },
            c if c.is_ascii_alphabetic() => {
                let at_word_start = i == 0 || !is_word_byte(bytes[i - 1]);
                if at_word_start {
                    for kw in ["INSERT", "UPDATE", "DELETE", "MERGE"] {
                        if matches_keyword(bytes, i, kw.as_bytes()) {
                            return true;
                        }
                    }
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    false
}

/// Whole-word case-insensitive keyword match at `bytes[i..]`. Assumes the
/// preceding byte is already known to be a word boundary; checks the trailing
/// boundary here.
fn matches_keyword(bytes: &[u8], i: usize, kw: &[u8]) -> bool {
    let end = i + kw.len();
    if end > bytes.len() {
        return false;
    }
    if !bytes[i..end].eq_ignore_ascii_case(kw) {
        return false;
    }
    end == bytes.len() || !is_word_byte(bytes[end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_guardrail() {
        assert!(is_write_sql("INSERT INTO t VALUES (1)"));
        assert!(is_write_sql("  drop table t"));
        assert!(is_write_sql("CREATE TABLE t(x int)"));
        // SET/RESET are flagged so no single statement can flip the read-only GUC.
        assert!(is_write_sql("SET default_transaction_read_only=off"));
        assert!(is_write_sql("RESET default_transaction_read_only"));
        assert!(is_write_sql("reset all"));
        assert!(!is_write_sql("SELECT * FROM t"));
        assert!(!is_write_sql("  with x as (select 1) select * from x"));
    }

    fn cfg_ro(read_only: bool) -> McpConfig {
        McpConfig {
            read_only,
            ..McpConfig::default()
        }
    }

    #[test]
    fn split_statements_single_and_trailing_semicolon() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
        // A trailing `;` must NOT yield an empty final statement.
        assert_eq!(split_statements("SELECT 1;"), vec!["SELECT 1"]);
        assert_eq!(
            split_statements("SELECT 1; DROP TABLE t"),
            vec!["SELECT 1", "DROP TABLE t"]
        );
        // Whitespace / empty inputs → no statements.
        assert!(split_statements("   ").is_empty());
        assert!(split_statements(";;").is_empty());
    }

    #[test]
    fn split_statements_semicolon_inside_single_quotes() {
        assert_eq!(split_statements("SELECT ';' AS s"), vec!["SELECT ';' AS s"]);
        // '' escape keeps us inside the string across the semicolon.
        assert_eq!(
            split_statements("SELECT 'a;''b;c' AS s"),
            vec!["SELECT 'a;''b;c' AS s"]
        );
    }

    #[test]
    fn split_statements_escape_string_backslash_quote() {
        // In an E'…' escape-string, `\'` is an escaped quote (NOT a close), so a
        // `;` after it stays inside the string → ONE statement. Regressing this
        // to over-reject (the pre-fix behavior) fails closed but breaks valid
        // agents; verify it is admitted as a single statement.
        assert_eq!(
            split_statements(r"SELECT E'a\';b' AS s"),
            vec![r"SELECT E'a\';b' AS s"]
        );
        assert_eq!(
            split_statements(r"select e'x\';drop' AS s"),
            vec![r"select e'x\';drop' AS s"]
        );
        // A REGULAR (non-E) string is standard-conforming: `\` is literal, so
        // `'a\'` closes at the `'` and the `;` IS top-level → 2 statements.
        // (The `E` detection must not leak into plain strings, or a real
        // separator would be hidden — an under-count bypass.)
        assert_eq!(
            split_statements(r"SELECT 'a\'; DROP TABLE t"),
            vec![r"SELECT 'a\'", "DROP TABLE t"]
        );
        // `someE'…'` — the `E` is the tail of an identifier, not an escape-string
        // prefix, so backslash stays literal and the `'` after `\` closes.
        assert_eq!(
            split_statements(r"SELECT xE'a\'; DROP TABLE t"),
            vec![r"SELECT xE'a\'", "DROP TABLE t"]
        );
    }

    #[test]
    fn split_statements_semicolon_inside_double_quotes() {
        assert_eq!(
            split_statements(r#"SELECT 1 AS "a;b""#),
            vec![r#"SELECT 1 AS "a;b""#]
        );
    }

    #[test]
    fn split_statements_semicolon_inside_dollar_quotes() {
        assert_eq!(
            split_statements("SELECT $$a;b$$ AS s"),
            vec!["SELECT $$a;b$$ AS s"]
        );
        assert_eq!(
            split_statements("SELECT $tag$a;b$tag$ AS s"),
            vec!["SELECT $tag$a;b$tag$ AS s"]
        );
        // `$1` is a parameter, not a dollar quote — the `;` after it splits.
        assert_eq!(
            split_statements("SELECT $1; DROP TABLE t"),
            vec!["SELECT $1", "DROP TABLE t"]
        );
    }

    #[test]
    fn split_statements_dollar_in_identifier_does_not_hide_semicolon() {
        // PostgreSQL's lexer treats `$` as an identifier-continuation char, so
        // `a$x$`, `a$$`, `a$qz$` are SINGLE identifiers — the `$` does NOT open a
        // dollar quote. A guard that misreads it as a dollar-quote opener would
        // swallow the real top-level `;` and the trailing write, under-counting
        // statements and defeating the multi-statement guard.
        assert_eq!(
            split_statements("SELECT 1 AS a$x$ ; DROP TABLE t"),
            vec!["SELECT 1 AS a$x$", "DROP TABLE t"]
        );
        assert_eq!(
            split_statements("SELECT 1 AS a$$ ; DELETE FROM t"),
            vec!["SELECT 1 AS a$$", "DELETE FROM t"]
        );
        assert_eq!(
            split_statements("SELECT 1 AS a$qz$ ; TRUNCATE users"),
            vec!["SELECT 1 AS a$qz$", "TRUNCATE users"]
        );
        // The `$$<tag>$$` adjacency: a `$` PRECEDED BY another `$` is still inside
        // the identifier (ident_cont includes `$`), so `a$$b$$` is ONE identifier.
        // The gate must use is_ident_cont (not is_word_byte, which omits `$`) or
        // the second `$` is misread as a tag opener that runs to EOF and hides the
        // top-level `;` (the real read_only/allow-list bypass this test guards).
        assert_eq!(
            split_statements("SELECT 1 AS a$$b$$ ; DROP TABLE t"),
            vec!["SELECT 1 AS a$$b$$", "DROP TABLE t"]
        );
        assert_eq!(
            split_statements("SELECT 1 AS a$$x$$ ; TRUNCATE users"),
            vec!["SELECT 1 AS a$$x$$", "TRUNCATE users"]
        );
        // Multibyte-led alias: PG's ident_cont includes high-bit bytes, so `é$$`
        // is ONE identifier. The gate must treat a `$` glued after a multibyte
        // byte as still-inside-identifier, or it reopens the under-count.
        assert_eq!(
            split_statements("SELECT 1 AS é$$ ; DROP TABLE t"),
            vec!["SELECT 1 AS é$$", "DROP TABLE t"]
        );
        assert_eq!(
            split_statements("SELECT 1 AS naïve$$x$$ ; DELETE FROM t"),
            vec!["SELECT 1 AS naïve$$x$$", "DELETE FROM t"]
        );
        // Guard must not over-fire: a genuine dollar-quote (opening `$` starts a
        // token) still masks its inner `;` — both empty-tag and named-tag forms.
        assert_eq!(
            split_statements("SELECT $$a;b$$ AS s ; SELECT 2"),
            vec!["SELECT $$a;b$$ AS s", "SELECT 2"]
        );
        assert_eq!(
            split_statements("SELECT $q$a;b$q$ AS s ; SELECT 2"),
            vec!["SELECT $q$a;b$q$ AS s", "SELECT 2"]
        );
    }

    #[test]
    fn split_statements_semicolon_inside_comments() {
        assert_eq!(
            split_statements("SELECT 1 -- a;b\n"),
            vec!["SELECT 1 -- a;b"]
        );
        assert_eq!(
            split_statements("SELECT 1 /* a;b */ FROM t"),
            vec!["SELECT 1 /* a;b */ FROM t"]
        );
        // Nested block comment: the inner `*/` must not end the outer one.
        assert_eq!(
            split_statements("SELECT 1 /* a /* b;c */ d;e */ FROM t"),
            vec!["SELECT 1 /* a /* b;c */ d;e */ FROM t"]
        );
    }

    #[test]
    fn has_data_modifying_cte_detects_writes() {
        assert!(has_data_modifying_cte(
            "WITH x AS (INSERT INTO t VALUES(1) RETURNING *) SELECT * FROM x"
        ));
        assert!(has_data_modifying_cte(
            "with recursive r as (delete from t returning *) select * from r"
        ));
        // Read-only CTE: no write keyword in code.
        assert!(!has_data_modifying_cte(
            "WITH x AS (SELECT 1) SELECT * FROM x"
        ));
        // Not a WITH head → never flagged (leading-verb guard handles it).
        assert!(!has_data_modifying_cte("INSERT INTO t VALUES(1)"));
        // Word-boundary: identifiers that merely contain the words don't match.
        assert!(!has_data_modifying_cte(
            "WITH x AS (SELECT deleted_at, insert_col FROM t) SELECT * FROM x"
        ));
        // The word appearing only inside a string literal doesn't match.
        assert!(!has_data_modifying_cte(
            "WITH x AS (SELECT 'delete me' AS s) SELECT * FROM x"
        ));
        // A `$` inside an identifier must NOT be misread as a dollar-quote
        // opener; if it were, skip_dollar_quoted would consume to EOF and hide
        // the INSERT keyword, wrongly reporting the CTE as read-only.
        assert!(has_data_modifying_cte(
            "WITH t AS (SELECT 1 AS a$x$), w AS (INSERT INTO logs VALUES(1) RETURNING *) SELECT * FROM w"
        ));
        assert!(has_data_modifying_cte(
            "WITH t AS (SELECT 1 AS a$$), w AS (DELETE FROM logs RETURNING *) SELECT * FROM w"
        ));
        // The `$$<tag>$$` adjacency inside a CTE: a `$` after another `$` must not
        // open a phantom dollar-quote that swallows the INSERT (the same
        // under-count class as the split_statements bypass).
        assert!(has_data_modifying_cte(
            "WITH t AS (SELECT 1 AS a$$b$$), w AS (INSERT INTO logs VALUES(1) RETURNING *) SELECT * FROM w"
        ));
        // Genuine dollar-quote (opening `$` starts a token) still masks the
        // word inside it — no false positive.
        assert!(!has_data_modifying_cte(
            "WITH x AS (SELECT $$insert$$ AS s) SELECT * FROM x"
        ));
    }

    #[test]
    fn check_policy_read_only_rejects_bypasses() {
        let cfg = cfg_ro(true);
        for sql in [
            "SELECT 1; DROP TABLE t",
            "select 1 ; drop table t",
            // `$`-in-identifier under-count bypasses: these must still split to
            // 2 and trip the multi-statement guard.
            "SELECT 1 AS a$x$ ; DROP TABLE t",
            "SELECT 1 AS a$$ ; TRUNCATE users",
            "SELECT 1 AS a$$b$$ ; DROP TABLE t",
            "SELECT 1 AS a$$x$$ ; DELETE FROM t",
            "SELECT 1 AS é$$ ; DROP TABLE t",
            "SELECT 1 AS a$x$ ; COMMIT ; SET default_transaction_read_only=off ; INSERT INTO t VALUES(1)",
            "WITH x AS (INSERT INTO t VALUES(1) RETURNING *) SELECT * FROM x",
            "with recursive r as (delete from t returning *) select * from r",
            "SET default_transaction_read_only=off",
            // A lone RESET must not un-flip the read-only GUC either.
            "RESET default_transaction_read_only",
            "RESET ALL",
        ] {
            assert!(
                McpServer::check_policy(&cfg, None, sql).is_err(),
                "expected rejection for: {sql}"
            );
        }
    }

    #[test]
    fn check_policy_read_only_admits_safe() {
        let cfg = cfg_ro(true);
        for sql in [
            "SELECT * FROM t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "SELECT ';' AS s",
            "SELECT 'delete me' AS s",
            // A genuine dollar-quote with an inner `;` is ONE statement — the
            // token-boundary guard must not over-fire and split/reject it.
            "SELECT $$a;b$$ AS s",
            "SELECT $tag$a;b$tag$ AS s",
            // A `$` inside an identifier is fine on its own — read-only SELECT.
            "SELECT 1 AS a$x$",
            // ...including the `$$`-adjacency shape: a lone `a$$b$$` is one
            // identifier, so this is a single read-only statement, not a batch.
            "SELECT 1 AS a$$b$$",
            // ...and a lone multibyte-led alias `é$$` — one identifier, admitted
            // (the ident_cont fix must not over-reject genuine single statements).
            "SELECT 1 AS é$$",
            // An E'…' escape-string whose escaped quote precedes a `;` is a
            // single read-only SELECT, not a batch — must be admitted.
            r"SELECT E'a\';b' AS s",
        ] {
            assert!(
                McpServer::check_policy(&cfg, None, sql).is_ok(),
                "expected admit for: {sql}"
            );
        }
    }

    #[test]
    fn check_policy_rejects_multi_statement_even_with_permissive_contract() {
        // Fully permissive contract (writes allowed) still must not let a
        // batch through — multi-statement defeats the allow-list model.
        let contract = AgentContract {
            id: "writer".into(),
            read_only: false,
            allowed_verbs: Some(vec!["INSERT".into(), "SELECT".into()]),
            allowed_tables: None,
            denied_tables: vec![],
            require_predicate_on: vec![],
            require_limit: false,
            max_rows: None,
        };
        let cfg = cfg_ro(false);
        assert!(McpServer::check_policy(
            &cfg,
            Some(&contract),
            "INSERT INTO a VALUES(1); DROP TABLE b"
        )
        .is_err());
        // `$`-in-identifier under-count: with a permissive contract there is NO
        // read-only backstop, so the multi-statement guard is the only defense.
        // A leading SELECT passes `validate`; the guard must still reject. The
        // `a$$b$$` / `a$$x$$` shapes are the ones a prior gate (using is_word_byte,
        // which omits `$`) mis-split into ONE statement, letting DROP/TRUNCATE run.
        for sql in [
            "SELECT 1 AS a$x$ ; DROP TABLE b",
            "SELECT 1 AS a$$ ; TRUNCATE b",
            "SELECT 1 AS a$qz$ ; INSERT INTO b VALUES(1)",
            "SELECT 1 AS a$$b$$ ; DROP TABLE b",
            "SELECT 1 AS a$$x$$ ; TRUNCATE b",
            // Multibyte-led alias — the exact write-through the re-audit found:
            // no read-only backstop on this path, so the split guard is the only
            // defense and MUST reject.
            "SELECT 1 AS é$$ ; DROP TABLE b",
        ] {
            assert!(
                McpServer::check_policy(&cfg, Some(&contract), sql).is_err(),
                "expected multi-statement rejection for: {sql}"
            );
        }
        // A single allowed INSERT still passes the contract.
        assert!(McpServer::check_policy(&cfg, Some(&contract), "INSERT INTO a VALUES(1)").is_ok());
    }

    #[test]
    fn effective_read_only_decision() {
        let ro = cfg_ro(true);
        let rw = cfg_ro(false);
        assert!(effective_read_only(&ro, None));
        assert!(!effective_read_only(&rw, None));
        let ro_contract = AgentContract {
            id: "r".into(),
            read_only: true,
            allowed_verbs: None,
            allowed_tables: None,
            denied_tables: vec![],
            require_predicate_on: vec![],
            require_limit: false,
            max_rows: None,
        };
        // A read-only contract forces read-only even when the gateway is not.
        assert!(effective_read_only(&rw, Some(&ro_contract)));
    }

    /// Zero-capacity pool: dispatch tests never reach a backend.
    fn test_pool() -> crate::gateway_pool::SharedBackendPool {
        std::sync::Arc::new(crate::gateway_pool::BackendClientPool::new(0))
    }

    /// Generous admission cap: dispatch tests other than the exhaustion test
    /// itself must never be rejected as busy.
    fn test_admission() -> tokio::sync::Semaphore {
        tokio::sync::Semaphore::new(4)
    }

    #[tokio::test]
    async fn initialize_and_tools_list() {
        let cfg = McpConfig::default();
        let init = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            &cfg,
            None,
            &test_pool(),
            &test_admission(),
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(init["result"]["serverInfo"]["name"], "heliosproxy-mcp");

        let tools = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
            &cfg,
            None,
            &test_pool(),
            &test_admission(),
        )
        .await
        .unwrap();
        let names: Vec<&str> = tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"query"));
        assert!(names.contains(&"list_tables"));
        assert!(names.contains(&"explain"));
    }

    #[tokio::test]
    async fn notification_has_no_response() {
        let cfg = McpConfig::default();
        let r = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            &cfg,
            None,
            &test_pool(),
            &test_admission(),
        )
        .await;
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn read_only_blocks_write_tool_call() {
        let cfg = McpConfig::default(); // read_only = true
        let r = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"query","arguments":{"sql":"DELETE FROM t"}}}"#,
            &cfg,
            None,
            &test_pool(),
            &test_admission(),
        )
        .await
        .unwrap();
        assert_eq!(r["result"]["isError"], true);
        assert!(r["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("read-only"));
    }

    #[tokio::test]
    async fn tools_call_rejected_with_jsonrpc_error_when_admission_cap_exhausted() {
        let cfg = McpConfig::default();
        let exhausted = tokio::sync::Semaphore::new(0);
        let r = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"query","arguments":{"sql":"SELECT 1"}}}"#,
            &cfg,
            None,
            &test_pool(),
            &exhausted,
        )
        .await
        .unwrap();
        assert_eq!(r["error"]["code"], -32000);
        assert!(r["error"]["message"]
            .as_str()
            .unwrap()
            .contains("too many concurrent MCP requests"));
    }

    #[tokio::test]
    async fn other_methods_bypass_the_admission_cap() {
        // Only `tools/call` is gated: `ping` must still succeed even when
        // every permit is exhausted.
        let cfg = McpConfig::default();
        let exhausted = tokio::sync::Semaphore::new(0);
        let r = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":5,"method":"ping","params":{}}"#,
            &cfg,
            None,
            &test_pool(),
            &exhausted,
        )
        .await
        .unwrap();
        assert_eq!(r["result"], json!({}));
    }

    fn principal(id: &str, token: &str) -> crate::config::McpPrincipal {
        crate::config::McpPrincipal {
            id: id.into(),
            token: token.into(),
        }
    }

    /// With no token configured every caller is `default`; with tokens, the
    /// bearer token selects its principal and anything else is refused.
    #[test]
    fn bearer_tokens_select_the_principal() {
        let open = Principals::from_config(&McpConfig::default());
        assert_eq!(open.count(), 0);
        for header in [None, Some("Bearer x"), Some("garbage")] {
            assert_eq!(open.resolve(header).as_deref(), Some("default"));
        }

        let shared_only = Principals::from_config(&McpConfig {
            auth_token: Some("shared".into()),
            ..McpConfig::default()
        });
        assert_eq!(
            shared_only.resolve(Some("Bearer shared")).as_deref(),
            Some("default")
        );
        assert_eq!(shared_only.resolve(Some("Bearer other")), None);
        assert_eq!(shared_only.resolve(None), None);

        let cfg = McpConfig {
            auth_token: Some("shared".into()),
            principals: vec![principal("etl-agent", "t-etl"), principal("bi", "t-bi")],
            ..McpConfig::default()
        };
        let p = Principals::from_config(&cfg);
        assert_eq!(p.count(), 3);
        assert_eq!(
            p.resolve(Some("Bearer t-etl")).as_deref(),
            Some("etl-agent")
        );
        assert_eq!(p.resolve(Some("Bearer t-bi")).as_deref(), Some("bi"));
        assert_eq!(p.resolve(Some("Bearer shared")).as_deref(), Some("default"));
        for header in [
            None,
            Some(""),
            Some("Bearer "),
            Some("Bearer t-et"),
            Some("Bearer t-etl "),
            Some("t-etl"),
            Some("Basic t-etl"),
        ] {
            assert_eq!(p.resolve(header), None, "{header:?}");
        }

        // Principals without the shared token: a token is still required.
        let agents_only = Principals::from_config(&McpConfig {
            principals: vec![principal("a", "ta")],
            ..McpConfig::default()
        });
        assert_eq!(agents_only.resolve(None), None);
        assert_eq!(agents_only.resolve(Some("Bearer ta")).as_deref(), Some("a"));
    }

    /// One HTTP exchange with a gateway connection; the raw response.
    async fn exchange(shared: Arc<McpShared>, request: String) -> String {
        use tokio::io::AsyncReadExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        let task = tokio::spawn(McpServer::handle_connection(server, peer, shared));
        client.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        task.await.unwrap().unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn ping(authorization: Option<&str>) -> String {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let auth = authorization
            .map(|a| format!("Authorization: {a}\r\n"))
            .unwrap_or_default();
        format!(
            "POST / HTTP/1.1\r\n{auth}Content-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    /// A request whose token names no principal gets 401 before any work.
    #[tokio::test]
    async fn an_unknown_token_is_refused_with_401() {
        let cfg = McpConfig {
            principals: vec![principal("etl-agent", "t-etl")],
            ..McpConfig::default()
        };
        let shared = McpServer::new(cfg, None, test_pool()).into_shared();
        for auth in [None, Some("Bearer nope"), Some("Bearer default")] {
            let resp = exchange(shared.clone(), ping(auth)).await;
            assert!(resp.starts_with("HTTP/1.1 401"), "{auth:?}: {resp}");
            assert!(resp.contains("unauthorized"), "{resp}");
        }
        let resp = exchange(shared, ping(Some("Bearer t-etl"))).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(resp.contains(r#""result":{}"#), "{resp}");
    }

    /// The shared `auth_token` keeps working unchanged next to principals.
    #[tokio::test]
    async fn the_shared_token_still_authenticates() {
        let cfg = McpConfig {
            auth_token: Some("shared".into()),
            principals: vec![principal("etl-agent", "t-etl")],
            ..McpConfig::default()
        };
        let shared = McpServer::new(cfg, None, test_pool()).into_shared();
        let resp = exchange(shared.clone(), ping(Some("Bearer shared"))).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        let resp = exchange(shared, ping(None)).await;
        assert!(resp.starts_with("HTTP/1.1 401"), "{resp}");
    }

    fn bounded_fixture(
        rows: usize,
        truncation: crate::backend::client::Truncation,
    ) -> BoundedQueryResult {
        use crate::backend::client::ColumnMeta;
        BoundedQueryResult {
            result: QueryResult {
                columns: vec![ColumnMeta {
                    name: "v".into(),
                    type_oid: 25,
                }],
                rows: (0..rows)
                    .map(|_| vec![TextValue::Text("x".into())])
                    .collect(),
                command_tag: format!("SELECT {rows}"),
            },
            truncation,
        }
    }

    #[test]
    fn render_result_untruncated_adds_no_structured_fields() {
        let (text, structured) = render_result(bounded_fixture(2, Default::default()));
        assert!(
            structured.is_none(),
            "untruncated results carry no extra fields"
        );
        assert!(text.ends_with("(2 rows)"), "{text}");
        assert!(!text.contains("truncated"));
    }

    #[test]
    fn render_result_row_cap_reports_counts_and_flags() {
        let t = crate::backend::client::Truncation {
            row_limit_hit: true,
            byte_limit_hit: false,
        };
        let (text, structured) = render_result(bounded_fixture(3, t));
        let s = structured.expect("truncated results carry structuredContent");
        assert_eq!(s["truncated"], true);
        assert_eq!(s["rows_returned"], 3);
        assert_eq!(s["row_limit_hit"], true);
        assert_eq!(s["byte_limit_hit"], false);
        assert!(
            text.contains("(truncated: the row limit was reached; the first 3 rows are shown)"),
            "{text}"
        );
    }

    #[test]
    fn render_result_byte_cap_reports_byte_reason() {
        let t = crate::backend::client::Truncation {
            row_limit_hit: false,
            byte_limit_hit: true,
        };
        let (text, structured) = render_result(bounded_fixture(1, t));
        let s = structured.unwrap();
        assert_eq!(s["byte_limit_hit"], true);
        assert_eq!(s["row_limit_hit"], false);
        assert!(text.contains("the byte limit was reached"), "{text}");
    }

    #[test]
    fn render_result_both_caps_named() {
        let t = crate::backend::client::Truncation {
            row_limit_hit: true,
            byte_limit_hit: true,
        };
        let (text, _) = render_result(bounded_fixture(1, t));
        assert!(
            text.contains("the row and byte limit was reached"),
            "{text}"
        );
    }

    /// A port nothing listens on (the backend an admitted call dials).
    #[cfg(feature = "multi-tenancy")]
    async fn closed_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    /// `[multi_tenancy]` active-query cap on the `query` tool: while the
    /// tenant's permit is held, its call is refused in-band (isError, the
    /// tool-error shape); another tenant is unaffected; once the permit is
    /// returned the call reaches the backend again.
    #[cfg(feature = "multi-tenancy")]
    #[tokio::test]
    async fn a_query_tool_call_is_refused_at_the_tenant_active_query_cap() {
        use crate::gateway_policy::GatewayTenant;
        let policy = crate::gateway_policy::capped_tenancy_policy("X-Helios-Tenant");
        let acme = GatewayTenant("acme".into());
        let globex = GatewayTenant("globex".into());
        let cfg = McpConfig {
            backend_port: closed_port().await,
            ..McpConfig::default()
        };
        let pool: crate::gateway_pool::SharedBackendPool =
            Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        let admission = tokio::sync::Semaphore::new(4);
        let body = json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "query", "arguments": { "sql": "SELECT 1" } }
        })
        .to_string();
        let text_of = |v: &Value| -> (bool, String) {
            (
                v["result"]["isError"].as_bool().unwrap_or(false),
                v["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or("")
                    .to_string(),
            )
        };

        let principal: Arc<str> = Arc::from(MCP_DEFAULT_PRINCIPAL);
        let in_flight = policy.admit_tenant_query(Some(&acme)).await.unwrap();
        assert!(in_flight.is_some());
        let gw = Gateway {
            pool: &pool,
            policy: &policy,
            peer: None,
            tenant: Some(&acme),
            principal: &principal,
            #[cfg(feature = "query-cache")]
            cache: None,
        };
        let refused = McpServer::dispatch_with(&body, &cfg, None, &gw, &admission)
            .await
            .unwrap();
        let (is_error, text) = text_of(&refused);
        assert!(is_error, "{refused}");
        assert!(
            text.contains("too many active queries for tenant \"acme\""),
            "{text}"
        );

        let gw_other = Gateway {
            pool: &pool,
            policy: &policy,
            peer: None,
            tenant: Some(&globex),
            principal: &principal,
            #[cfg(feature = "query-cache")]
            cache: None,
        };
        let other = McpServer::dispatch_with(&body, &cfg, None, &gw_other, &admission)
            .await
            .unwrap();
        let (_, text) = text_of(&other);
        assert!(!text.contains("too many active queries"), "{text}");

        drop(in_flight);
        let allowed = McpServer::dispatch_with(&body, &cfg, None, &gw, &admission)
            .await
            .unwrap();
        let (_, text) = text_of(&allowed);
        assert!(text.contains("backend connect"), "{text}");
    }
}
