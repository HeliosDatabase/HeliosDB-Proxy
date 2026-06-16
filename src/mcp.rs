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

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::agent_contract::{self, AgentContract};
use crate::backend::client::QueryResult;
use crate::backend::types::TextValue;
use crate::backend::{tls::default_client_config, BackendClient, BackendConfig, TlsMode};
use crate::config::McpConfig;
use crate::{ProxyError, Result};

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// The MCP gateway server.
pub struct McpServer {
    config: McpConfig,
    contract: Option<AgentContract>,
}

impl McpServer {
    pub fn new(config: McpConfig, contract: Option<AgentContract>) -> Self {
        Self { config, contract }
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
        let cfg = Arc::new(self.config);
        let contract = Arc::new(self.contract);
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    tracing::warn!("MCP accept error: {}", e);
                    continue;
                }
            };
            let cfg = cfg.clone();
            let contract = contract.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(stream, cfg, contract).await {
                    tracing::debug!(%peer, "MCP connection error: {}", e);
                }
            });
        }
    }

    async fn handle_connection(
        mut stream: tokio::net::TcpStream,
        cfg: Arc<McpConfig>,
        contract: Arc<Option<AgentContract>>,
    ) -> Result<()> {
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let mut content_length = 0usize;
        // Read request line + headers.
        use tokio::io::AsyncBufReadExt;
        let mut first = true;
        loop {
            line.clear();
            let n = reader
                .read_line(&mut line)
                .await
                .map_err(|e| ProxyError::Network(format!("MCP read: {}", e)))?;
            if n == 0 || line == "\r\n" {
                break;
            }
            if first {
                first = false; // request line; we accept any method/path
            } else if line.to_ascii_lowercase().starts_with("content-length:") {
                if let Some(v) = line.split(':').nth(1) {
                    content_length = v.trim().parse().unwrap_or(0);
                }
            }
        }
        let body = if content_length > 0 {
            let mut buf = vec![0u8; content_length];
            reader
                .read_exact(&mut buf)
                .await
                .map_err(|e| ProxyError::Network(format!("MCP body read: {}", e)))?;
            String::from_utf8_lossy(&buf).to_string()
        } else {
            String::new()
        };

        let response = Self::dispatch(&body, &cfg, (*contract).as_ref()).await;
        match response {
            Some(v) => {
                let payload = serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string());
                Self::write_http(&mut writer, 200, "application/json", payload.as_bytes()).await
            }
            // Notifications get a bare 202 with no JSON-RPC body.
            None => Self::write_http(&mut writer, 202, "application/json", b"").await,
        }
    }

    /// Dispatch one JSON-RPC request. Returns `None` for notifications.
    async fn dispatch(
        body: &str,
        cfg: &McpConfig,
        contract: Option<&AgentContract>,
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
            "tools/list" => Some(rpc_ok(id, json!({ "tools": Self::tool_defs(cfg) }))),
            "tools/call" => Some(Self::handle_tool_call(id, &params, cfg, contract).await),
            other => Some(rpc_error(
                id,
                -32601,
                &format!("method not found: {}", other),
            )),
        }
    }

    fn tool_defs(cfg: &McpConfig) -> Value {
        let query_desc = if cfg.read_only {
            "Run a read-only SQL query and return rows. Writes/DDL are refused."
        } else {
            "Run a SQL query and return rows (or the command tag for writes)."
        };
        json!([
            {
                "name": "query",
                "description": query_desc,
                "inputSchema": {
                    "type": "object",
                    "properties": { "sql": { "type": "string", "description": "SQL to execute" } },
                    "required": ["sql"]
                }
            },
            {
                "name": "list_tables",
                "description": "List user tables (schema.table) in the connected database.",
                "inputSchema": { "type": "object", "properties": {} }
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
    ) -> Value {
        let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let args = params.get("arguments").cloned().unwrap_or(json!({}));

        let result: std::result::Result<String, String> = match name {
            "query" => {
                let sql = args
                    .get("sql")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .trim();
                if sql.is_empty() {
                    Err("missing 'sql'".to_string())
                } else {
                    match Self::check_policy(cfg, contract, sql) {
                        Err(hint) => Err(hint),
                        Ok(()) => Self::run_sql(cfg, sql).await.map(|r| format_result(&r)),
                    }
                }
            }
            "list_tables" => {
                let sql = "SELECT table_schema, table_name FROM information_schema.tables \
                           WHERE table_schema NOT IN ('pg_catalog','information_schema') \
                           ORDER BY table_schema, table_name";
                Self::run_sql(cfg, sql).await.map(|r| format_result(&r))
            }
            "explain" => {
                let sql = args
                    .get("sql")
                    .and_then(|s| s.as_str())
                    .unwrap_or("")
                    .trim();
                if sql.is_empty() {
                    Err("missing 'sql'".to_string())
                } else {
                    match Self::check_policy(cfg, contract, sql) {
                        Err(hint) => Err(hint),
                        Ok(()) => Self::run_sql(cfg, &format!("EXPLAIN {}", sql))
                            .await
                            .map(|r| format_result(&r)),
                    }
                }
            }
            other => Err(format!("unknown tool: {}", other)),
        };

        match result {
            Ok(text) => {
                tracing::info!(tool = %name, "MCP tool call ok");
                rpc_ok(
                    id,
                    json!({ "content": [{ "type": "text", "text": text }], "isError": false }),
                )
            }
            Err(e) => {
                tracing::info!(tool = %name, error = %e, "MCP tool call error");
                // Tool errors are reported in-band (isError) per MCP, not as a
                // protocol error, so the agent can read + self-correct.
                rpc_ok(
                    id,
                    json!({ "content": [{ "type": "text", "text": e }], "isError": true }),
                )
            }
        }
    }

    /// Gate a SQL statement: when an agent contract is configured, validate
    /// against it and return a structured JSON repair hint on violation;
    /// otherwise apply the plain read-only guardrail.
    fn check_policy(
        cfg: &McpConfig,
        contract: Option<&AgentContract>,
        sql: &str,
    ) -> std::result::Result<(), String> {
        if let Some(c) = contract {
            agent_contract::validate(sql, c).map_err(|v| v.to_json())
        } else if cfg.read_only && is_write_sql(sql) {
            Err("write/DDL refused: the MCP gateway is read-only".to_string())
        } else {
            Ok(())
        }
    }

    /// Connect to the configured backend, run one statement, return rows.
    async fn run_sql(cfg: &McpConfig, sql: &str) -> std::result::Result<QueryResult, String> {
        let bcfg = BackendConfig {
            host: cfg.backend_host.clone(),
            port: cfg.backend_port,
            user: cfg.backend_user.clone(),
            password: cfg.backend_password.clone(),
            database: cfg.backend_database.clone(),
            application_name: Some("heliosproxy-mcp".to_string()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_secs(30),
            tls_config: default_client_config(),
        };
        let mut client = BackendClient::connect(&bcfg)
            .await
            .map_err(|e| format!("backend connect: {}", e))?;
        let res = client.simple_query(sql).await.map_err(|e| format!("{}", e));
        client.close().await;
        res
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

/// First-keyword write/DDL detection (read-only guardrail).
fn is_write_sql(sql: &str) -> bool {
    use crate::protocol::starts_with_ci;
    let s = sql.trim_start();
    for kw in [
        "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE",
        "COPY", "MERGE", "CALL", "DO", "VACUUM", "REINDEX", "CLUSTER", "LOCK", "COMMENT", "SET",
    ] {
        if starts_with_ci(s, kw) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_guardrail() {
        assert!(is_write_sql("INSERT INTO t VALUES (1)"));
        assert!(is_write_sql("  drop table t"));
        assert!(is_write_sql("CREATE TABLE t(x int)"));
        assert!(!is_write_sql("SELECT * FROM t"));
        assert!(!is_write_sql("  with x as (select 1) select * from x"));
    }

    #[tokio::test]
    async fn initialize_and_tools_list() {
        let cfg = McpConfig::default();
        let init = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            &cfg,
            None,
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(init["result"]["serverInfo"]["name"], "heliosproxy-mcp");

        let tools = McpServer::dispatch(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#,
            &cfg,
            None,
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
        )
        .await
        .unwrap();
        assert_eq!(r["result"]["isError"], true);
        assert!(r["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("read-only"));
    }
}
