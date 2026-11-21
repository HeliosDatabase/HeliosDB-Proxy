//! HTTP SQL gateway — `@neondatabase/serverless`-compatible.
//!
//! When `[http_gateway] enabled = true`, the proxy exposes a `POST /sql`
//! endpoint that runs one SQL statement over the backend PG-wire client and
//! returns a Neon-style JSON result (`{ command, rowCount, rows, fields }`).
//! This lets edge/serverless runtimes (Cloudflare Workers, Vercel Edge) that
//! cannot hold a TCP socket talk to vanilla PostgreSQL or HeliosDB-Nano over
//! plain HTTP.
//!
//! Parameterised queries (`$1`,`$2`) are supported via a JSON `params` array.
//! `Neon-Array-Mode: true` returns each row as an array instead of an object.
//! A WebSocket session/transaction mode is the planned follow-on; this is the
//! one-shot HTTP path the serverless driver uses for non-transactional queries.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::backend::client::QueryResult;
use crate::backend::types::TextValue;
use crate::backend::{tls::default_client_config, BackendConfig, ParamValue, TlsMode};
use crate::config::HttpGatewayConfig;
use crate::gateway_policy::{GatewayPolicy, GatewayTenant, RequestIdentity, SharedGatewayPolicy};
use crate::gateway_pool::{statement_is_session_neutral, SharedBackendPool};
use crate::{ProxyError, Result};

/// Why a `/sql` statement produced no result: a result past a result cap
/// (HTTP 413) or any other failure (HTTP 400).
enum SqlError {
    TooLarge(String),
    Failed(String),
}

#[derive(Clone, Default)]
pub(crate) struct HttpSqlCache {
    #[cfg(feature = "query-cache")]
    pub(crate) cache: Option<Arc<crate::cache::QueryCache>>,
    #[cfg(feature = "query-cache")]
    pub(crate) trust_backend_read_effects: bool,
    /// `[cache] extra_volatile_functions` with the built-in list (`None`:
    /// the built-in list alone), as the PG-wire relay reads them.
    #[cfg(feature = "query-cache")]
    pub(crate) volatile_scan: Option<Arc<crate::sql_text::VolatileScan>>,
    /// `[cache]` table exclusions, as the relay applies them.
    #[cfg(feature = "query-cache")]
    pub(crate) table_exclusions: Option<Arc<crate::sql_text::TableExclusions>>,
}

#[cfg(feature = "query-cache")]
impl HttpSqlCache {
    /// Whether the shared result cache may answer or store `sql` (the final
    /// statement, after the rewrite rules and the tenant filter): the
    /// relay's refusals (system catalogs, time-travel and ambiguous text,
    /// excluded tables), a read with no volatile or unknown call that leaves
    /// the session as it was, and either the operator's read-effects
    /// contract or a literal scalar read.
    pub(crate) fn may_cache(&self, sql: &str) -> bool {
        crate::sql_text::cache_refusal(sql, self.table_exclusions.as_deref()).is_none()
            && crate::server::ProxyServer::is_cacheable_read_sql_with(
                sql,
                self.volatile_scan
                    .as_deref()
                    .unwrap_or_else(|| crate::sql_text::VolatileScan::builtin()),
            )
            && matches!(
                crate::replay_sql::all_calls_satisfy(sql, |_| false),
                Ok(true)
            )
            && statement_is_session_neutral(sql)
            && (self.trust_backend_read_effects || crate::replay_sql::literal_scalar_select(sql))
    }
}

struct HttpSqlRequest<'a> {
    tenant: Option<&'a GatewayTenant>,
    sql: &'a str,
    params: &'a [ParamValue],
    array_mode: bool,
}

pub struct HttpGateway {
    config: HttpGatewayConfig,
    pool: SharedBackendPool,
    policy: SharedGatewayPolicy,
    sql_cache: HttpSqlCache,
    drain: crate::gateway_drain::GatewayDrain,
}

impl HttpGateway {
    pub fn new(config: HttpGatewayConfig, pool: SharedBackendPool) -> Self {
        Self {
            config,
            pool,
            policy: Arc::new(GatewayPolicy::default()),
            sql_cache: HttpSqlCache::default(),
            drain: crate::gateway_drain::GatewayDrain::new(),
        }
    }

    /// Take part in the daemon's shutdown drain: stop accepting when it
    /// closes, and count each accepted request until it is answered.
    pub fn with_drain(mut self, drain: crate::gateway_drain::GatewayDrain) -> Self {
        self.drain = drain;
        self
    }

    /// Share the daemon's SQL result cache with this stateless gateway.
    #[cfg(feature = "query-cache")]
    pub fn with_query_cache(mut self, cache: Option<Arc<crate::cache::QueryCache>>) -> Self {
        self.sql_cache.cache = cache;
        self
    }

    /// Allow broad read caching only when the operator vouches for the
    /// backend's side-effect and volatility contract. The default still
    /// permits the conservative literal scalar-read subset.
    #[cfg(feature = "query-cache")]
    pub fn with_trusted_read_effects(mut self, trusted: bool) -> Self {
        self.sql_cache.trust_backend_read_effects = trusted;
        self
    }

    /// Apply the relay's cache rules to this gateway's statements: the
    /// configured volatile functions and table exclusions (system catalogs,
    /// time-travel and ambiguous text are refused regardless).
    #[cfg(feature = "query-cache")]
    pub fn with_cache_rules(
        mut self,
        volatile_scan: Arc<crate::sql_text::VolatileScan>,
        table_exclusions: Option<Arc<crate::sql_text::TableExclusions>>,
    ) -> Self {
        self.sql_cache.volatile_scan = Some(volatile_scan);
        self.sql_cache.table_exclusions = table_exclusions;
        self
    }

    /// Apply the PG-wire request policies (rate limit, rewrite, circuit
    /// breaker) to this gateway's requests (H-06).
    pub fn with_policy(mut self, policy: SharedGatewayPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address)
            .await
            .map_err(|e| {
                ProxyError::Network(format!(
                    "HTTP gateway bind {}: {}",
                    self.config.listen_address, e
                ))
            })?;
        tracing::info!(addr = %self.config.listen_address, "HTTP SQL gateway listening");
        // Admission cap (H-06 `max_concurrent_requests`): the
        // `(max_concurrent_requests + 1)`-th concurrent request is rejected
        // with 503 + `Retry-After` instead of queuing forever.
        let admission = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_concurrent_requests,
        ));
        let drain = self.drain.clone();
        let cfg = Arc::new(self.config);
        let pool = self.pool.clone();
        let policy = self.policy.clone();
        let sql_cache = self.sql_cache.clone();
        loop {
            let accepted = tokio::select! {
                biased;
                () = drain.closed() => {
                    tracing::info!(addr = %cfg.listen_address, "HTTP SQL gateway draining: listener closed");
                    return Ok(());
                }
                accepted = listener.accept() => accepted,
            };
            let (stream, peer) = match accepted {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("HTTP gateway accept error: {}", e);
                    continue;
                }
            };
            let in_flight = drain.track();
            let cfg = cfg.clone();
            let pool = pool.clone();
            let admission = admission.clone();
            let policy = policy.clone();
            let sql_cache = sql_cache.clone();
            tokio::spawn(async move {
                let _in_flight = in_flight;
                if let Err(e) =
                    Self::handle(stream, peer, cfg, pool, admission, policy, sql_cache).await
                {
                    tracing::debug!(%peer, "HTTP gateway error: {}", e);
                }
            });
        }
    }

    pub(crate) async fn handle(
        mut stream: tokio::net::TcpStream,
        peer: std::net::SocketAddr,
        cfg: Arc<HttpGatewayConfig>,
        pool: SharedBackendPool,
        admission: Arc<tokio::sync::Semaphore>,
        policy: SharedGatewayPolicy,
        sql_cache: HttpSqlCache,
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
        let method = head.method.as_str();
        let path = head.path.as_str();

        // Liveness probe.
        if method == "GET" && (path == "/health" || path == "/") {
            return Self::respond(&mut writer, 200, &json!({"status":"ok"})).await;
        }
        // Constant-time Bearer check (no `==` short-circuit oracle; no per-request
        // format! of the expected token).
        let authorized = match cfg.auth_token.as_ref() {
            None => true,
            Some(tok) => head
                .header("authorization")
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|got| http_util::constant_time_eq_str(got, tok))
                .unwrap_or(false),
        };
        if !authorized {
            return Self::respond(&mut writer, 401, &json!({"error":"unauthorized"})).await;
        }
        // H-06: the tenant this request acts for, when `tenant_header` is set.
        let tenant = match policy.request_tenant(|name| head.header(name)) {
            Ok(t) => t,
            Err(denied) => {
                return Self::respond(
                    &mut writer,
                    denied.http_status(),
                    &json!({"error": denied.to_string()}),
                )
                .await
            }
        };
        if method != "POST" {
            return Self::respond(&mut writer, 405, &json!({"error":"use POST /sql"})).await;
        }
        // Reject an oversized declared body BEFORE allocating for it.
        if head.content_length > http_util::MAX_HTTP_BODY_BYTES {
            return Self::respond(&mut writer, 413, &json!({"error":"request body too large"}))
                .await;
        }
        let array_mode = head
            .header("neon-array-mode")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let body_buf = match http_util::read_body(&mut reader, head.content_length, deadline).await
        {
            Ok(b) => b,
            Err(_) => return Ok(()),
        };
        let req: Value = match serde_json::from_slice(&body_buf) {
            Ok(v) => v,
            Err(e) => {
                return Self::respond(
                    &mut writer,
                    400,
                    &json!({"error": format!("invalid JSON: {}", e)}),
                )
                .await
            }
        };
        let sql = req
            .get("query")
            .and_then(|q| q.as_str())
            .unwrap_or("")
            .trim();
        if sql.is_empty() {
            return Self::respond(&mut writer, 400, &json!({"error":"missing 'query'"})).await;
        }
        let params = parse_params(req.get("params"));

        // Admission cap (H-06 `max_concurrent_requests`): only the actual
        // backend-hitting work is gated, not the health probe / auth-failure
        // / malformed-request paths above.
        let _permit = match admission.try_acquire() {
            Ok(p) => p,
            Err(_) => return Self::respond_busy(&mut writer).await,
        };
        // H-06: the PG-wire rate limit and circuit breaker apply here too.
        let who = RequestIdentity {
            peer: Some(peer.ip()),
            user: &cfg.backend_user,
            database: cfg.backend_database.as_deref(),
        };
        let node = format!("{}:{}", cfg.backend_host, cfg.backend_port);
        // Held for the request: a half-open circuit's probe slot.
        let _node_admission = match policy
            .admit_request(who)
            .await
            .and_then(|()| policy.check_node(&node))
        {
            Ok(admission) => admission,
            Err(denied) => {
                return Self::respond(
                    &mut writer,
                    denied.http_status(),
                    &json!({"error": denied.to_string()}),
                )
                .await;
            }
        };

        // `[multi_tenancy]` active-query cap: the tenant's permit is held for
        // the request and returned on every path when it drops.
        let _query_permit = match policy.admit_tenant_query(tenant.as_ref()).await {
            Ok(p) => p,
            Err(denied) => {
                return Self::respond(
                    &mut writer,
                    denied.http_status(),
                    &json!({"error": denied.to_string()}),
                )
                .await
            }
        };
        match Self::run_sql(
            &cfg,
            &pool,
            &policy,
            HttpSqlRequest {
                tenant: tenant.as_ref(),
                sql,
                params: &params,
                array_mode,
            },
            &sql_cache,
        )
        .await
        {
            Ok(body) => Self::respond(&mut writer, 200, &body).await,
            Err(SqlError::TooLarge(msg)) => {
                Self::respond(&mut writer, 413, &json!({ "error": msg })).await
            }
            Err(SqlError::Failed(e)) => {
                Self::respond(&mut writer, 400, &json!({ "error": e })).await
            }
        }
    }

    async fn run_sql(
        cfg: &HttpGatewayConfig,
        pool: &SharedBackendPool,
        policy: &GatewayPolicy,
        request: HttpSqlRequest<'_>,
        _sql_cache: &HttpSqlCache,
    ) -> std::result::Result<Value, SqlError> {
        let HttpSqlRequest {
            tenant,
            sql,
            params,
            array_mode,
        } = request;
        let rewritten = policy.rewrite(sql);
        let filtered = policy
            .tenant_sql(&rewritten, tenant)
            .map_err(|denied| SqlError::Failed(denied.to_string()))?;
        let sql = filtered.as_ref();
        #[cfg(feature = "query-cache")]
        let cache_prep = if let Some(cache) = _sql_cache
            .cache
            .as_ref()
            .filter(|_| _sql_cache.may_cache(sql))
        {
            let context = crate::cache::CacheContext {
                database: cfg.backend_database.clone().unwrap_or_default(),
                user: Some(cfg.backend_user.clone()),
                branch: None,
                connection_id: None,
            };
            let namespace = http_cache_namespace(cfg, tenant, params, array_mode);
            let (lookup, prep) = cache.get_scoped(sql, None, &context, &namespace).await;
            if let crate::cache::CacheLookup::Hit { result, .. } = lookup {
                let body = serde_json::from_slice(&result.data).map_err(|error| {
                    SqlError::Failed(format!("cached HTTP result is invalid JSON: {error}"))
                })?;
                return Ok(body);
            }
            Some((context, prep))
        } else {
            None
        };
        let node = format!("{}:{}", cfg.backend_host, cfg.backend_port);
        let bcfg = BackendConfig {
            host: cfg.backend_host.clone(),
            port: cfg.backend_port,
            user: cfg.backend_user.clone(),
            password: cfg.backend_password.clone(),
            database: cfg.backend_database.clone(),
            application_name: Some("heliosproxy-http".to_string()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_millis(cfg.query_timeout_ms),
            tls_config: default_client_config(),
        };
        let mut client = match pool.acquire(&bcfg).await {
            Ok(c) => c,
            Err(e) => {
                policy.record(&node, false, &e.to_string());
                return Err(SqlError::Failed(format!("backend connect: {}", e)));
            }
        };
        #[cfg(feature = "query-cache")]
        if client
            .server_parameters
            .get("server_version")
            .is_some_and(|version| version.contains("HeliosDB"))
        {
            if let Some(cache) = _sql_cache.cache.as_ref() {
                cache.note_heliosdb_backend();
            }
        }
        policy.record(&node, true, "");
        // Bounded reads: a result past `max_result_rows` / `max_result_bytes`
        // is drained (the connection stays in sync) and then refused.
        let res = if params.is_empty() {
            client
                .simple_query_bounded(sql, cfg.max_result_rows, cfg.max_result_bytes)
                .await
        } else {
            client
                .query_with_params_bounded(sql, params, cfg.max_result_rows, cfg.max_result_bytes)
                .await
        };
        // C-02: a write retires the cached reads of its tables, whatever the
        // outcome (a statement that timed out may still commit).
        policy
            .note_statement(sql, crate::gateway_policy::GatewayKind::Http)
            .await;
        match res {
            Ok(bounded) => {
                // H-06: reuse the authenticated connection only when the
                // statement left no session state behind.
                if statement_is_session_neutral(sql) {
                    pool.release(&bcfg, client);
                } else {
                    pool.discard(client);
                }
                if bounded.truncation.is_truncated() {
                    crate::gateway_policy::note_result_refused(
                        crate::gateway_policy::ResultGateway::Http,
                    );
                    return Err(SqlError::TooLarge(
                        crate::gateway_policy::result_cap_message(
                            "http_gateway",
                            bounded.truncation,
                            cfg.max_result_rows,
                            cfg.max_result_bytes,
                        ),
                    ));
                }
                let body = neon_result(&bounded.result, array_mode);
                #[cfg(feature = "query-cache")]
                if let (Some(cache), Some((context, prep))) =
                    (_sql_cache.cache.as_ref(), cache_prep)
                {
                    if prep.storable() {
                        if let Ok(data) = serde_json::to_vec(&body) {
                            cache
                                .put_prepared(
                                    sql,
                                    &context,
                                    &prep,
                                    bytes::Bytes::from(data),
                                    bounded.result.rows.len(),
                                    Duration::ZERO,
                                )
                                .await;
                        }
                    }
                }
                Ok(body)
            }
            Err(e) => {
                pool.discard(client);
                Err(SqlError::Failed(format!("{}", e)))
            }
        }
    }

    async fn respond(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        status: u16,
        body: &Value,
    ) -> Result<()> {
        Self::respond_with_extra_headers(writer, status, body, "").await
    }

    /// Admission-cap rejection (H-06 `max_concurrent_requests`): 503 +
    /// `Retry-After`, telling the client this is transient load-shedding,
    /// not a permanent error.
    async fn respond_busy(writer: &mut tokio::net::tcp::WriteHalf<'_>) -> Result<()> {
        let body = json!({"error":"too many concurrent /sql requests"});
        let retry_after = format!(
            "Retry-After: {}\r\n",
            crate::http_util::GATEWAY_BUSY_RETRY_AFTER_SECS
        );
        Self::respond_with_extra_headers(writer, 503, &body, &retry_after).await
    }

    async fn respond_with_extra_headers(
        writer: &mut tokio::net::tcp::WriteHalf<'_>,
        status: u16,
        body: &Value,
        extra_headers: &str,
    ) -> Result<()> {
        let payload = serde_json::to_vec(body).unwrap_or_default();
        let status_text = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            405 => "Method Not Allowed",
            413 => "Payload Too Large",
            429 => "Too Many Requests",
            503 => "Service Unavailable",
            _ => "Error",
        };
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
            status, status_text, payload.len(), extra_headers,
        );
        writer
            .write_all(head.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("write: {}", e)))?;
        writer
            .write_all(&payload)
            .await
            .map_err(|e| ProxyError::Network(format!("write: {}", e)))?;
        Ok(())
    }
}

/// Map a JSON `params` array to text-format ParamValues.
fn parse_params(v: Option<&Value>) -> Vec<ParamValue> {
    match v.and_then(|v| v.as_array()) {
        None => Vec::new(),
        Some(arr) => arr
            .iter()
            .map(|p| match p {
                Value::Null => ParamValue::Null,
                Value::Bool(b) => ParamValue::Bool(*b),
                Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        ParamValue::Int(i)
                    } else {
                        ParamValue::Float(n.as_f64().unwrap_or(0.0))
                    }
                }
                Value::String(s) => ParamValue::Text(s.clone()),
                other => ParamValue::Text(other.to_string()),
            })
            .collect(),
    }
}

#[cfg(feature = "query-cache")]
fn http_cache_namespace(
    cfg: &HttpGatewayConfig,
    tenant: Option<&GatewayTenant>,
    params: &[ParamValue],
    array_mode: bool,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    fn field(hasher: &mut Sha256, bytes: &[u8]) {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }

    let mut hasher = Sha256::new();
    hasher.update(b"heliosdb-proxy-http-sql-cache-v1");
    field(&mut hasher, cfg.backend_host.as_bytes());
    hasher.update(cfg.backend_port.to_be_bytes());
    field(&mut hasher, cfg.backend_user.as_bytes());
    field(
        &mut hasher,
        cfg.backend_database.as_deref().unwrap_or("").as_bytes(),
    );
    if let Some(tenant) = tenant {
        hasher.update([1]);
        field(&mut hasher, tenant.0.as_bytes());
    } else {
        hasher.update([0]);
    }
    hasher.update([u8::from(array_mode)]);
    hasher.update((params.len() as u64).to_be_bytes());
    for param in params {
        match param {
            ParamValue::Null => hasher.update([0]),
            ParamValue::Bool(value) => hasher.update([1, u8::from(*value)]),
            ParamValue::Int(value) => {
                hasher.update([2]);
                hasher.update(value.to_be_bytes());
            }
            ParamValue::Float(value) => {
                hasher.update([3]);
                hasher.update(value.to_bits().to_be_bytes());
            }
            ParamValue::Text(value) => {
                hasher.update([4]);
                field(&mut hasher, value.as_bytes());
            }
            ParamValue::Lsn(value) => {
                hasher.update([5]);
                field(&mut hasher, value.as_bytes());
            }
        }
    }
    hasher.finalize().into()
}

/// Build the Neon-serverless-style result body.
fn neon_result(qr: &QueryResult, array_mode: bool) -> Value {
    let command = qr
        .command_tag
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string();
    let fields: Vec<Value> = qr
        .columns
        .iter()
        .map(|c| json!({ "name": c.name, "dataTypeID": c.type_oid }))
        .collect();
    let rows: Vec<Value> = qr
        .rows
        .iter()
        .map(|row| {
            if array_mode {
                Value::Array(row.iter().map(cell_to_json).collect())
            } else {
                let mut obj = serde_json::Map::new();
                for (i, c) in qr.columns.iter().enumerate() {
                    let v = row.get(i).map(cell_to_json).unwrap_or(Value::Null);
                    obj.insert(c.name.clone(), v);
                }
                Value::Object(obj)
            }
        })
        .collect();
    // rowCount mirrors libpq: affected rows for writes, else row count.
    let row_count = qr.rows_affected().unwrap_or(qr.rows.len() as u64);
    json!({
        "command": command,
        "rowCount": row_count,
        "rows": rows,
        "fields": fields,
        "rowAsArray": array_mode,
    })
}

fn cell_to_json(v: &TextValue) -> Value {
    match v {
        TextValue::Null => Value::Null,
        TextValue::Text(s) => Value::String(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::client::ColumnMeta;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpStream;

    fn qr() -> QueryResult {
        QueryResult {
            columns: vec![
                ColumnMeta {
                    name: "id".into(),
                    type_oid: 23,
                },
                ColumnMeta {
                    name: "name".into(),
                    type_oid: 25,
                },
            ],
            rows: vec![
                vec![TextValue::Text("1".into()), TextValue::Text("alice".into())],
                vec![TextValue::Text("2".into()), TextValue::Null],
            ],
            command_tag: "SELECT 2".into(),
        }
    }

    #[test]
    fn neon_object_mode() {
        let v = neon_result(&qr(), false);
        assert_eq!(v["command"], "SELECT");
        assert_eq!(v["rowCount"], 2);
        assert_eq!(v["rows"][0]["id"], "1");
        assert_eq!(v["rows"][0]["name"], "alice");
        assert_eq!(v["rows"][1]["name"], Value::Null);
        assert_eq!(v["fields"][0]["name"], "id");
        assert_eq!(v["fields"][0]["dataTypeID"], 23);
    }

    #[test]
    fn neon_array_mode() {
        let v = neon_result(&qr(), true);
        assert_eq!(v["rowAsArray"], true);
        assert_eq!(v["rows"][0][0], "1");
        assert_eq!(v["rows"][0][1], "alice");
    }

    #[cfg(feature = "query-cache")]
    #[test]
    fn http_cache_namespace_separates_parameters_tenants_and_result_shape() {
        let cfg = HttpGatewayConfig::default();
        let int_param = [ParamValue::Int(1)];
        let text_param = [ParamValue::Text("1".into())];
        let base = http_cache_namespace(&cfg, None, &int_param, false);
        assert_ne!(base, http_cache_namespace(&cfg, None, &text_param, false));
        assert_ne!(base, http_cache_namespace(&cfg, None, &int_param, true));
        assert_ne!(
            base,
            http_cache_namespace(&cfg, Some(&GatewayTenant("acme".into())), &int_param, false)
        );
    }

    /// The shutdown drain: closing it returns `run` and closes the listener,
    /// while a request accepted before is still answered and counted until
    /// its handler returns.
    #[tokio::test]
    async fn drain_closes_the_listener_and_finishes_accepted_requests() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = format!("127.0.0.1:{port}");
        let cfg = HttpGatewayConfig {
            listen_address: addr.clone(),
            ..HttpGatewayConfig::default()
        };
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        let drain = crate::gateway_drain::GatewayDrain::new();
        let task = tokio::spawn(HttpGateway::new(cfg, pool).with_drain(drain.clone()).run());

        // Connected (accepted) but the request is not sent yet.
        let mut held = loop {
            match TcpStream::connect(&addr).await {
                Ok(s) => break s,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while drain.in_flight() == 0 {
            assert!(tokio::time::Instant::now() < deadline, "never accepted");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        drain.close();
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("run returns once the drain closes")
            .unwrap()
            .unwrap();
        assert!(
            TcpStream::connect(&addr).await.is_err(),
            "the listener is closed"
        );
        assert_eq!(drain.in_flight(), 1, "the accepted request is in flight");

        held.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        held.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while drain.in_flight() != 0 {
            assert!(tokio::time::Instant::now() < deadline, "guard not released");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// The HTTP gateway applies the relay's cache rules: system catalogs and
    /// excluded tables are never cached, and the configured volatile
    /// functions count, not only the built-in list.
    #[cfg(feature = "query-cache")]
    #[test]
    fn the_http_cache_applies_the_relays_rules() {
        let cache = HttpSqlCache {
            trust_backend_read_effects: true,
            volatile_scan: Some(Arc::new(crate::sql_text::VolatileScan::new(["my_clock"]))),
            table_exclusions: Some(Arc::new(crate::sql_text::TableExclusions::new(["secrets"]))),
            ..HttpSqlCache::default()
        };
        assert!(cache.may_cache("SELECT id FROM orders WHERE id = 1"));
        for sql in [
            "SELECT * FROM secrets",
            "SELECT relname FROM pg_class",
            "SELECT my_clock()",
        ] {
            assert!(!cache.may_cache(sql), "{sql}");
        }
        // Without the operator's contract only literal scalar reads.
        let strict = HttpSqlCache::default();
        assert!(strict.may_cache("SELECT 1"));
        assert!(!strict.may_cache("SELECT id FROM orders"));
    }

    #[test]
    fn params_mapping() {
        let p = parse_params(Some(&json!([1, "x", true, null, 2.5])));
        assert!(matches!(p[0], ParamValue::Int(1)));
        assert!(matches!(p[1], ParamValue::Text(ref s) if s == "x"));
        assert!(matches!(p[2], ParamValue::Bool(true)));
        assert!(matches!(p[3], ParamValue::Null));
        assert!(matches!(p[4], ParamValue::Float(_)));
    }

    /// H-06: an open circuit on the gateway's backend answers 503 before any
    /// backend connection is attempted, as on the PG-wire listener.
    #[cfg(feature = "circuit-breaker")]
    #[tokio::test]
    async fn handle_fast_fails_while_the_backend_circuit_is_open() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();

        let cfg = Arc::new(HttpGatewayConfig::default());
        let node = format!("{}:{}", cfg.backend_host, cfg.backend_port);
        let cb = Arc::new(crate::circuit_breaker::CircuitBreakerManager::new(
            crate::circuit_breaker::ManagerConfig::new(
                crate::circuit_breaker::CircuitBreakerConfig {
                    failure_threshold: 1,
                    ..Default::default()
                },
            ),
        ));
        let policy = GatewayPolicy::default().with_circuit_breaker(cb);
        for _ in 0..5 {
            policy.record(&node, false, "connection refused");
        }
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        let admission = Arc::new(tokio::sync::Semaphore::new(4));
        let task = tokio::spawn(HttpGateway::handle(
            server,
            peer,
            cfg,
            pool,
            admission,
            Arc::new(policy),
            HttpSqlCache::default(),
        ));
        let body = br#"{"query":"select 1"}"#;
        let req = format!(
            "POST /sql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();
        task.await.unwrap().unwrap();
        assert!(resp.starts_with("HTTP/1.1 503"), "{resp}");
        assert!(resp.contains("circuit open"), "{resp}");
    }

    /// H-06: with a tenant_header, a request that names no known tenant is
    /// refused (403) before any backend work; the health probe still works.
    #[cfg(feature = "multi-tenancy")]
    #[tokio::test]
    async fn handle_refuses_a_request_without_a_known_tenant() {
        use crate::multi_tenancy::{
            IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId, TenantManagerBuilder,
        };
        let tm = TenantManagerBuilder::new()
            .config(MultiTenancyConfig {
                enabled: true,
                ..Default::default()
            })
            .build();
        tm.register_tenant(TenantConfig::new(
            TenantId::new("acme"),
            IsolationStrategy::row("public", "tid"),
        ));
        let policy =
            Arc::new(GatewayPolicy::default().with_tenancy(Arc::new(tm), "X-Helios-Tenant".into()));
        for (header, want) in [
            ("", "HTTP/1.1 403"),
            ("X-Helios-Tenant: globex\r\n", "HTTP/1.1 403"),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let mut client = TcpStream::connect(addr).await.unwrap();
            let (server, peer) = listener.accept().await.unwrap();
            let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
            let task = tokio::spawn(HttpGateway::handle(
                server,
                peer,
                Arc::new(HttpGatewayConfig::default()),
                pool,
                Arc::new(tokio::sync::Semaphore::new(4)),
                policy.clone(),
                HttpSqlCache::default(),
            ));
            let body = br#"{"query":"select 1"}"#;
            let req = format!(
                "POST /sql HTTP/1.1\r\n{header}Content-Length: {}\r\n\r\n",
                body.len()
            );
            client.write_all(req.as_bytes()).await.unwrap();
            client.write_all(body).await.unwrap();
            let mut buf = Vec::new();
            client.read_to_end(&mut buf).await.unwrap();
            let resp = String::from_utf8(buf).unwrap();
            task.await.unwrap().unwrap();
            assert!(resp.starts_with(want), "{resp}");
            assert!(resp.contains("tenant required"), "{resp}");
        }
    }

    #[tokio::test]
    async fn handle_health_bypasses_the_admission_cap() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();

        let cfg = Arc::new(HttpGatewayConfig::default());
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        let admission = Arc::new(tokio::sync::Semaphore::new(0));
        let task = tokio::spawn(HttpGateway::handle(
            server,
            peer,
            cfg,
            pool,
            admission,
            Arc::new(GatewayPolicy::default()),
            HttpSqlCache::default(),
        ));

        client
            .write_all(b"GET /health HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();
        task.await.unwrap().unwrap();

        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"));
    }

    #[tokio::test]
    async fn handle_rejects_with_503_and_retry_after_when_admission_cap_exhausted() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();

        let cfg = Arc::new(HttpGatewayConfig::default());
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        // Zero permits: the first request that reaches the admission check
        // must be rejected as busy rather than hit a (nonexistent) backend.
        let admission = Arc::new(tokio::sync::Semaphore::new(0));
        let task = tokio::spawn(HttpGateway::handle(
            server,
            peer,
            cfg,
            pool,
            admission,
            Arc::new(GatewayPolicy::default()),
            HttpSqlCache::default(),
        ));

        let body = br#"{"query":"select 1"}"#;
        let req = format!(
            "POST /sql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();
        task.await.unwrap().unwrap();

        assert!(resp.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
        assert!(resp.contains("Retry-After: 1\r\n"));
        assert!(resp.contains("too many concurrent /sql requests"));
    }

    /// A port nothing listens on: where a gateway test's admitted request
    /// goes, failing at connect after it has taken its tenant permit.
    #[cfg(feature = "multi-tenancy")]
    async fn closed_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    #[cfg(feature = "multi-tenancy")]
    async fn post_sql_as(policy: SharedGatewayPolicy, port: u16, header: &str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        let cfg = HttpGatewayConfig {
            backend_port: port,
            ..HttpGatewayConfig::default()
        };
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        let task = tokio::spawn(HttpGateway::handle(
            server,
            peer,
            Arc::new(cfg),
            pool,
            Arc::new(tokio::sync::Semaphore::new(4)),
            policy,
            HttpSqlCache::default(),
        ));
        let body = br#"{"query":"select 1"}"#;
        let req = format!(
            "POST /sql HTTP/1.1\r\n{header}Content-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        task.await.unwrap().unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// `[multi_tenancy]` active-query cap on /sql: while a tenant's permit is
    /// held by an in-flight request, another request of that tenant is
    /// refused with 429; another tenant is unaffected; once the permit is
    /// returned the tenant's requests run again, including after a request
    /// that failed at the backend (the permit is released on every path).
    #[cfg(feature = "multi-tenancy")]
    #[tokio::test]
    async fn handle_caps_a_tenant_at_its_active_query_limit() {
        let policy = Arc::new(crate::gateway_policy::capped_tenancy_policy(
            "X-Helios-Tenant",
        ));
        let port = closed_port().await;
        let in_flight = policy
            .admit_tenant_query(Some(&crate::gateway_policy::GatewayTenant("acme".into())))
            .await
            .unwrap();
        assert!(in_flight.is_some());

        let refused = post_sql_as(policy.clone(), port, "X-Helios-Tenant: acme\r\n").await;
        assert!(
            refused.starts_with("HTTP/1.1 429 Too Many Requests\r\n"),
            "{refused}"
        );
        assert!(
            refused.contains("too many active queries for tenant"),
            "{refused}"
        );

        let other = post_sql_as(policy.clone(), port, "X-Helios-Tenant: globex\r\n").await;
        assert!(!other.starts_with("HTTP/1.1 429"), "{other}");
        assert!(!other.contains("too many active queries"), "{other}");

        drop(in_flight);
        for _ in 0..2 {
            let resp = post_sql_as(policy.clone(), port, "X-Helios-Tenant: acme\r\n").await;
            assert!(!resp.starts_with("HTTP/1.1 429"), "{resp}");
            assert!(!resp.contains("too many active queries"), "{resp}");
        }
    }

    /// One `/sql` request through `handle` against `port`, sharing `pool`.
    async fn post_sql_to(cfg: HttpGatewayConfig, pool: SharedBackendPool, sql: &str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        let task = tokio::spawn(HttpGateway::handle(
            server,
            peer,
            Arc::new(cfg),
            pool,
            Arc::new(tokio::sync::Semaphore::new(4)),
            Arc::new(GatewayPolicy::default()),
            HttpSqlCache::default(),
        ));
        let body = format!(r#"{{"query":"{sql}"}}"#);
        let req = format!(
            "POST /sql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        task.await.unwrap().unwrap();
        String::from_utf8(buf).unwrap()
    }

    /// `[http_gateway] max_result_rows`: a result past the cap is refused with
    /// 413 and the gateway's error shape, not truncated; a result at the cap is
    /// served; both run over one pooled connection, which stays in sync.
    #[tokio::test]
    async fn handle_refuses_a_result_past_max_result_rows_and_serves_one_at_it() {
        use std::sync::atomic::Ordering;
        let fake = crate::gateway_fake_pg::spawn(5, 4).await;
        let cfg = HttpGatewayConfig {
            backend_port: fake.port,
            max_result_rows: 3,
            ..HttpGatewayConfig::default()
        };
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(4));

        let refused = post_sql_to(cfg.clone(), pool.clone(), "select 1").await;
        assert!(refused.starts_with("HTTP/1.1 413 "), "{refused}");
        assert!(
            refused.contains("result exceeds http_gateway.max_result_rows (3)"),
            "{refused}"
        );

        fake.rows.store(3, Ordering::SeqCst);
        let served = post_sql_to(cfg.clone(), pool.clone(), "select 1").await;
        assert!(served.starts_with("HTTP/1.1 200 "), "{served}");
        assert!(served.contains("\"rowCount\":3"), "{served}");

        fake.rows.store(4, Ordering::SeqCst);
        let refused = post_sql_to(cfg.clone(), pool.clone(), "select 1").await;
        assert!(refused.starts_with("HTTP/1.1 413 "), "{refused}");

        fake.rows.store(2, Ordering::SeqCst);
        let served = post_sql_to(cfg, pool, "select 1").await;
        assert!(served.contains("\"rowCount\":2"), "{served}");
        assert_eq!(
            fake.connections.load(Ordering::SeqCst),
            1,
            "connection reused"
        );
    }

    /// `[http_gateway] max_result_bytes`: cell bytes past the cap refuse the
    /// result (413, naming the byte key); a result at the cap is served.
    #[tokio::test]
    async fn handle_refuses_a_result_past_max_result_bytes_and_serves_one_at_it() {
        use std::sync::atomic::Ordering;
        // 3 rows of 4-byte cells: 12 bytes against a 10-byte cap.
        let fake = crate::gateway_fake_pg::spawn(3, 4).await;
        let cfg = HttpGatewayConfig {
            backend_port: fake.port,
            max_result_bytes: 10,
            ..HttpGatewayConfig::default()
        };
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(4));
        let refused = post_sql_to(cfg.clone(), pool.clone(), "select 1").await;
        assert!(refused.starts_with("HTTP/1.1 413 "), "{refused}");
        assert!(
            refused.contains("result exceeds http_gateway.max_result_bytes (10)"),
            "{refused}"
        );
        fake.rows.store(2, Ordering::SeqCst); // 8 bytes: at the cap
        let served = post_sql_to(cfg, pool, "select 1").await;
        assert!(served.starts_with("HTTP/1.1 200 "), "{served}");
        assert!(served.contains("\"rowCount\":2"), "{served}");
    }

    /// `[http_gateway] max_result_bytes` with one 64 MiB cell: refused (413)
    /// without the gateway holding the cell, the connection stays in sync, and
    /// the next request on it is served.
    #[tokio::test]
    async fn handle_refuses_an_oversized_cell_and_keeps_the_connection() {
        use std::sync::atomic::Ordering;
        const HUGE_CELL: usize = 64 * 1024 * 1024;
        let fake = crate::gateway_fake_pg::spawn(1, HUGE_CELL).await;
        let cfg = HttpGatewayConfig {
            backend_port: fake.port,
            max_result_bytes: 1024,
            ..HttpGatewayConfig::default()
        };
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(4));
        let refused = post_sql_to(cfg.clone(), pool.clone(), "select 1").await;
        assert!(refused.starts_with("HTTP/1.1 413 "), "{refused}");
        assert!(
            refused.contains("result exceeds http_gateway.max_result_bytes (1024)"),
            "{refused}"
        );
        fake.cell_len.store(4, Ordering::SeqCst);
        let served = post_sql_to(cfg, pool, "select 1").await;
        assert!(served.starts_with("HTTP/1.1 200 "), "{served}");
        assert!(served.contains("\"rowCount\":1"), "{served}");
        assert_eq!(
            fake.connections.load(Ordering::SeqCst),
            1,
            "connection reused"
        );
    }
}
