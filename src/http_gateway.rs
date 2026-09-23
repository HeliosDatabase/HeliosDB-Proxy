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
use crate::gateway_pool::{statement_is_session_neutral, SharedBackendPool};
use crate::{ProxyError, Result};

pub struct HttpGateway {
    config: HttpGatewayConfig,
    pool: SharedBackendPool,
}

impl HttpGateway {
    pub fn new(config: HttpGatewayConfig, pool: SharedBackendPool) -> Self {
        Self { config, pool }
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
        let cfg = Arc::new(self.config);
        let pool = self.pool.clone();
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("HTTP gateway accept error: {}", e);
                    continue;
                }
            };
            let cfg = cfg.clone();
            let pool = pool.clone();
            let admission = admission.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle(stream, cfg, pool, admission).await {
                    tracing::debug!(%peer, "HTTP gateway error: {}", e);
                }
            });
        }
    }

    async fn handle(
        mut stream: tokio::net::TcpStream,
        cfg: Arc<HttpGatewayConfig>,
        pool: SharedBackendPool,
        admission: Arc<tokio::sync::Semaphore>,
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

        match Self::run_sql(&cfg, &pool, sql, &params).await {
            Ok(qr) => {
                let body = neon_result(&qr, array_mode);
                Self::respond(&mut writer, 200, &body).await
            }
            Err(e) => Self::respond(&mut writer, 400, &json!({ "error": e })).await,
        }
    }

    async fn run_sql(
        cfg: &HttpGatewayConfig,
        pool: &SharedBackendPool,
        sql: &str,
        params: &[ParamValue],
    ) -> std::result::Result<QueryResult, String> {
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
        let mut client = pool
            .acquire(&bcfg)
            .await
            .map_err(|e| format!("backend connect: {}", e))?;
        let res = if params.is_empty() {
            client.simple_query(sql).await
        } else {
            client.query_with_params(sql, params).await
        };
        match res {
            Ok(qr) => {
                // H-06: reuse the authenticated connection only when the
                // statement left no session state behind.
                if statement_is_session_neutral(sql) {
                    pool.release(&bcfg, client);
                } else {
                    pool.discard(client);
                }
                Ok(qr)
            }
            Err(e) => {
                pool.discard(client);
                Err(format!("{}", e))
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

    #[test]
    fn params_mapping() {
        let p = parse_params(Some(&json!([1, "x", true, null, 2.5])));
        assert!(matches!(p[0], ParamValue::Int(1)));
        assert!(matches!(p[1], ParamValue::Text(ref s) if s == "x"));
        assert!(matches!(p[2], ParamValue::Bool(true)));
        assert!(matches!(p[3], ParamValue::Null));
        assert!(matches!(p[4], ParamValue::Float(_)));
    }

    #[tokio::test]
    async fn handle_health_bypasses_the_admission_cap() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();

        let cfg = Arc::new(HttpGatewayConfig::default());
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        let admission = Arc::new(tokio::sync::Semaphore::new(0));
        let task = tokio::spawn(HttpGateway::handle(server, cfg, pool, admission));

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
        let (server, _peer) = listener.accept().await.unwrap();

        let cfg = Arc::new(HttpGatewayConfig::default());
        let pool: SharedBackendPool = Arc::new(crate::gateway_pool::BackendClientPool::new(0));
        // Zero permits: the first request that reaches the admission check
        // must be rejected as busy rather than hit a (nonexistent) backend.
        let admission = Arc::new(tokio::sync::Semaphore::new(0));
        let task = tokio::spawn(HttpGateway::handle(server, cfg, pool, admission));

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
}
