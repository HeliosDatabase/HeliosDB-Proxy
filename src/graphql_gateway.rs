//! GraphQL-to-SQL gateway — HTTP listener.
//!
//! When `[graphql_gateway] enabled = true`, the proxy exposes an HTTP endpoint
//! that accepts a GraphQL query (`POST` with `{"query": "..."}`), generates SQL
//! from the configured schema, executes it over the backend PG-wire client, and
//! returns a GraphQL JSON response (`{"data": {...}}`). Flat top-level
//! selections are supported; nested-relationship shaping is a follow-on.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::config::GraphqlGatewayConfig;
use crate::graphql::introspector::{ColumnDefinition, TableDefinition};
use crate::graphql::{GraphQLConfig, GraphQLEngine, GraphQLRequest, SchemaIntrospector};
use crate::{ProxyError, Result};

pub struct GraphqlGateway {
    config: Arc<GraphqlGatewayConfig>,
    engine: Arc<GraphQLEngine>,
    /// Admission cap on in-flight requests (H-06 `max_concurrent_requests`).
    /// A permit is held for the lifetime of one request's SQL execution; the
    /// `(max_concurrent_requests + 1)`-th concurrent request is rejected with
    /// `503 Service Unavailable` + `Retry-After` instead of queuing forever.
    admission: Arc<tokio::sync::Semaphore>,
}

impl GraphqlGateway {
    pub fn new(config: GraphqlGatewayConfig, pool: crate::gateway_pool::SharedBackendPool) -> Self {
        // Build the GraphQL schema from the configured tables.
        let tabledefs: Vec<TableDefinition> = config
            .tables
            .iter()
            .map(|t| TableDefinition {
                name: t.name.clone(),
                schema: "public".to_string(),
                columns: t
                    .columns
                    .iter()
                    .map(|c| ColumnDefinition {
                        name: c.clone(),
                        data_type: "text".to_string(),
                        nullable: true,
                        is_primary_key: c == "id",
                        has_default: false,
                    })
                    .collect(),
                foreign_keys: Vec::new(),
            })
            .collect();
        let schema = SchemaIntrospector::new().build_schema(&tabledefs);

        let bcfg = BackendConfig {
            host: config.backend_host.clone(),
            port: config.backend_port,
            user: config.backend_user.clone(),
            password: config.backend_password.clone(),
            database: config.backend_database.clone(),
            application_name: Some("heliosproxy-graphql".to_string()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_millis(config.query_timeout_ms),
            tls_config: default_client_config(),
        };
        let engine = GraphQLEngine::new(GraphQLConfig::default(), schema)
            .with_backend(bcfg)
            .with_pool(pool);
        let admission = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_requests));

        Self {
            config: Arc::new(config),
            engine: Arc::new(engine),
            admission,
        }
    }

    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address)
            .await
            .map_err(|e| {
                ProxyError::Network(format!(
                    "GraphQL gateway bind {}: {}",
                    self.config.listen_address, e
                ))
            })?;
        tracing::info!(addr = %self.config.listen_address, "GraphQL gateway listening");
        let config = self.config.clone();
        let engine = self.engine.clone();
        let admission = self.admission.clone();
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("GraphQL gateway accept error: {}", e);
                    continue;
                }
            };
            let config = config.clone();
            let engine = engine.clone();
            let admission = admission.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle(stream, config, engine, admission).await {
                    tracing::debug!(%peer, "GraphQL gateway error: {}", e);
                }
            });
        }
    }

    async fn handle(
        mut stream: tokio::net::TcpStream,
        cfg: Arc<GraphqlGatewayConfig>,
        engine: Arc<GraphQLEngine>,
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

        if method == "GET" && (path == "/health" || path == "/") {
            return Self::respond(&mut writer, 200, &json!({"status":"ok"})).await;
        }
        // Constant-time Bearer check.
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
            return Self::respond(
                &mut writer,
                405,
                &json!({"error":"use POST with a GraphQL query"}),
            )
            .await;
        }
        // Reject an oversized declared body BEFORE allocating for it.
        if head.content_length > http_util::MAX_HTTP_BODY_BYTES {
            return Self::respond(&mut writer, 413, &json!({"error":"request body too large"}))
                .await;
        }

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
                    &json!({"errors":[{"message": format!("invalid JSON: {}", e)}]}),
                )
                .await
            }
        };
        let query = req
            .get("query")
            .and_then(|q| q.as_str())
            .unwrap_or("")
            .trim();
        if query.is_empty() {
            return Self::respond(
                &mut writer,
                400,
                &json!({"errors":[{"message":"missing 'query'"}]}),
            )
            .await;
        }

        // Admission cap (H-06 `max_concurrent_requests`): only the actual
        // backend-hitting work is gated, not the health probe / auth-failure /
        // malformed-request paths above, so a saturated gateway still answers
        // liveness checks. The permit is held for the rest of this request.
        let _permit = match admission.try_acquire() {
            Ok(p) => p,
            Err(_) => return Self::respond_busy(&mut writer).await,
        };

        let response = engine.execute(GraphQLRequest::new(query)).await;
        let errors = response.errors.map(|errs| {
            errs.iter()
                .map(|e| json!({ "message": e.to_string() }))
                .collect::<Vec<_>>()
        });
        let body = json!({ "data": response.data, "errors": errors });
        Self::respond(&mut writer, 200, &body).await
    }

    async fn respond<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        status: u16,
        body: &Value,
    ) -> Result<()> {
        Self::respond_with_extra_headers(writer, status, body, "").await
    }

    /// Admission-cap rejection (H-06 `max_concurrent_requests`): 503 +
    /// `Retry-After`, telling the client this is transient load-shedding, not
    /// a permanent error.
    async fn respond_busy<W: AsyncWriteExt + Unpin>(writer: &mut W) -> Result<()> {
        let body = json!({"errors":[{"message":"too many concurrent GraphQL requests"}]});
        let retry_after = format!(
            "Retry-After: {}\r\n",
            crate::http_util::GATEWAY_BUSY_RETRY_AFTER_SECS
        );
        Self::respond_with_extra_headers(writer, 503, &body, &retry_after).await
    }

    async fn respond_with_extra_headers<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        status: u16,
        body: &Value,
        extra_headers: &str,
    ) -> Result<()> {
        let payload = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            404 => "Not Found",
            405 => "Method Not Allowed",
            413 => "Payload Too Large",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            503 => "Service Unavailable",
            s if s < 400 => "OK",
            _ => "Error",
        };
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}Connection: close\r\n\r\n",
            status,
            reason,
            payload.len(),
            extra_headers,
        );
        writer
            .write_all(head.as_bytes())
            .await
            .map_err(|e| ProxyError::Network(format!("GraphQL gw write: {}", e)))?;
        writer
            .write_all(&payload)
            .await
            .map_err(|e| ProxyError::Network(format!("GraphQL gw write: {}", e)))?;
        let _ = writer.flush().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    /// A connected loopback TCP pair standing in for a real client/proxy
    /// socket. `handle()` takes a concrete `tokio::net::TcpStream` (it calls
    /// the inherent `TcpStream::split`, not a generic `AsyncRead + AsyncWrite`
    /// split), so a `tokio::io::duplex` pair — which is what the plan asked
    /// for — cannot be substituted for it. A loopback TCP pair is the
    /// equivalent that satisfies the concrete type while still involving no
    /// real backend. `respond()` below IS generic over `AsyncWriteExt`, so
    /// that test uses a literal `tokio::io::duplex`.
    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _peer) = listener.accept().await.unwrap();
        (client, server)
    }

    fn offline_engine() -> Arc<GraphQLEngine> {
        // No `.with_backend`/`.with_pool`: the engine runs in offline mode
        // (backend: None) and never dials out, so these tests exercise only
        // the HTTP parsing/response path in `handle()`.
        let schema = SchemaIntrospector::new().build_schema(&[]);
        Arc::new(GraphQLEngine::new(GraphQLConfig::default(), schema))
    }

    fn admission(n: usize) -> Arc<tokio::sync::Semaphore> {
        Arc::new(tokio::sync::Semaphore::new(n))
    }

    async fn read_all(client: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn status_line(resp: &str) -> &str {
        resp.lines().next().unwrap()
    }

    #[tokio::test]
    async fn handle_rejects_non_post_with_405() {
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(4)));

        client
            .write_all(b"GET /graphql HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 405 Method Not Allowed");
        assert!(resp.contains("use POST with a GraphQL query"));
    }

    #[tokio::test]
    async fn handle_rejects_malformed_json_with_400() {
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(4)));

        let body = b"{not valid json";
        let req = format!(
            "POST /graphql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 400 Bad Request");
        assert!(resp.contains("invalid JSON"));
    }

    #[tokio::test]
    async fn handle_rejects_missing_query_with_400() {
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(4)));

        let body = b"{}";
        let req = format!(
            "POST /graphql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 400 Bad Request");
        assert!(resp.contains("missing 'query'"));
    }

    #[tokio::test]
    async fn handle_rejects_oversized_declared_body_with_413() {
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(4)));

        // MAX_HTTP_BODY_BYTES (8 MiB) is checked against the DECLARED
        // Content-Length before any body bytes are read, so the client need
        // not actually send that many bytes to trigger the 413.
        let oversized = crate::http_util::MAX_HTTP_BODY_BYTES + 1;
        let req = format!(
            "POST /graphql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            oversized
        );
        client.write_all(req.as_bytes()).await.unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 413 Payload Too Large");
        assert!(resp.contains("request body too large"));
    }

    #[tokio::test]
    async fn handle_serves_health_without_post() {
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(4)));

        client
            .write_all(b"GET /health HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 200 OK");
        assert!(resp.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn handle_health_bypasses_the_admission_cap() {
        // A liveness probe must still succeed even when every permit is
        // exhausted — only backend-hitting requests are gated.
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(0)));

        client
            .write_all(b"GET /health HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 200 OK");
    }

    #[tokio::test]
    async fn handle_rejects_with_503_and_retry_after_when_admission_cap_exhausted() {
        let (mut client, server) = tcp_pair().await;
        let cfg = Arc::new(GraphqlGatewayConfig::default());
        let engine = offline_engine();
        // Zero permits: the first request that reaches the admission check
        // must be rejected as busy rather than block forever.
        let task = tokio::spawn(GraphqlGateway::handle(server, cfg, engine, admission(0)));

        let body = br#"{"query":"{ dummy }"}"#;
        let req = format!(
            "POST /graphql HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        client.write_all(req.as_bytes()).await.unwrap();
        client.write_all(body).await.unwrap();
        let resp = read_all(&mut client).await;
        task.await.unwrap().unwrap();

        assert_eq!(status_line(&resp), "HTTP/1.1 503 Service Unavailable");
        assert!(resp.contains("Retry-After: 1\r\n"));
        assert!(resp.contains("too many concurrent GraphQL requests"));
    }

    #[tokio::test]
    async fn respond_writes_status_line_content_type_and_length() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let body = json!({"data": {"ok": true}});
        GraphqlGateway::respond(&mut server, 200, &body)
            .await
            .unwrap();
        drop(server); // close the write half so the client sees EOF

        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        let resp = String::from_utf8(buf).unwrap();

        let payload = serde_json::to_vec(&body).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp.contains("Content-Type: application/json\r\n"));
        assert!(resp.contains(&format!("Content-Length: {}\r\n", payload.len())));
        assert!(resp.contains("Connection: close\r\n"));
        assert!(resp.ends_with(&String::from_utf8(payload).unwrap()));
    }

    #[tokio::test]
    async fn respond_reason_phrase_matches_the_status_code() {
        for (status, line) in [
            (413u16, "HTTP/1.1 413 Payload Too Large\r\n"),
            (500, "HTTP/1.1 500 Internal Server Error\r\n"),
            (204, "HTTP/1.1 204 OK\r\n"),
            (418, "HTTP/1.1 418 Error\r\n"),
        ] {
            let (mut client, mut server) = tokio::io::duplex(4096);
            GraphqlGateway::respond(&mut server, status, &json!({"error":"x"}))
                .await
                .unwrap();
            drop(server);
            let mut buf = Vec::new();
            client.read_to_end(&mut buf).await.unwrap();
            let resp = String::from_utf8(buf).unwrap();
            assert!(resp.starts_with(line), "{status}: {resp}");
        }
    }
}
