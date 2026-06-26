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
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::config::GraphqlGatewayConfig;
use crate::graphql::introspector::{ColumnDefinition, TableDefinition};
use crate::graphql::{GraphQLConfig, GraphQLEngine, GraphQLRequest, SchemaIntrospector};
use crate::{ProxyError, Result};

pub struct GraphqlGateway {
    config: Arc<GraphqlGatewayConfig>,
    engine: Arc<GraphQLEngine>,
}

impl GraphqlGateway {
    pub fn new(config: GraphqlGatewayConfig) -> Self {
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
            query_timeout: Duration::from_secs(30),
            tls_config: default_client_config(),
        };
        let engine = GraphQLEngine::new(GraphQLConfig::default(), schema).with_backend(bcfg);

        Self {
            config: Arc::new(config),
            engine: Arc::new(engine),
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
            tokio::spawn(async move {
                if let Err(e) = Self::handle(stream, config, engine).await {
                    tracing::debug!(%peer, "GraphQL gateway error: {}", e);
                }
            });
        }
    }

    async fn handle(
        mut stream: tokio::net::TcpStream,
        cfg: Arc<GraphqlGatewayConfig>,
        engine: Arc<GraphQLEngine>,
    ) -> Result<()> {
        use tokio::io::AsyncBufReadExt;
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let mut content_length = 0usize;
        let mut method = String::new();
        let mut path = String::new();
        let mut authorized = cfg.auth_token.is_none();
        let mut first = true;
        loop {
            line.clear();
            let n = reader
                .read_line(&mut line)
                .await
                .map_err(|e| ProxyError::Network(format!("GraphQL gw read: {}", e)))?;
            if n == 0 || line == "\r\n" {
                break;
            }
            if first {
                let mut parts = line.split_whitespace();
                method = parts.next().unwrap_or("").to_string();
                path = parts.next().unwrap_or("").to_string();
                first = false;
                continue;
            }
            let lower = line.to_ascii_lowercase();
            if lower.starts_with("content-length:") {
                content_length = line
                    .split(':')
                    .nth(1)
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
            } else if lower.starts_with("authorization:") {
                if let Some(tok) = cfg.auth_token.as_ref() {
                    let v = line.split_once(':').map(|x| x.1).unwrap_or("").trim();
                    authorized = v == format!("Bearer {}", tok);
                }
            }
        }

        if method == "GET" && (path == "/health" || path == "/") {
            return Self::respond(&mut writer, 200, &json!({"status":"ok"})).await;
        }
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

        let mut body_buf = vec![0u8; content_length];
        if content_length > 0 {
            reader
                .read_exact(&mut body_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("GraphQL gw body: {}", e)))?;
        }
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
        let payload = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
        let reason = match status {
            200 => "OK",
            400 => "Bad Request",
            401 => "Unauthorized",
            405 => "Method Not Allowed",
            _ => "OK",
        };
        let head = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            status,
            reason,
            payload.len()
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
