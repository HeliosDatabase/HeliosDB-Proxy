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
use tokio::io::{AsyncWriteExt, BufReader};
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
        use crate::http_util;
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);

        // Bounded request read: overall deadline + header count/byte caps.
        let deadline = tokio::time::Instant::now() + http_util::HTTP_READ_TIMEOUT;
        let head = match http_util::read_head(&mut reader, deadline).await {
            Ok(h) => h,
            Err(_) => return Ok(()), // timeout / oversized headers / early close
        };

        // Bearer auth, when configured. Unlike the wire port there is no
        // pass-through identity here — the gateway runs SQL under its own
        // backend credentials — so an unauthenticated request must be refused.
        // Constant-time comparison (no `==` oracle).
        if let Some(tok) = cfg.auth_token.as_ref() {
            let ok = head
                .header("authorization")
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|got| http_util::constant_time_eq_str(got, tok))
                .unwrap_or(false);
            if !ok {
                Self::write_http(
                    &mut writer,
                    401,
                    "application/json",
                    br#"{"error":"unauthorized"}"#,
                )
                .await?;
                return Ok(());
            }
        }

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
                        Ok(()) => Self::run_sql(cfg, sql, effective_read_only(cfg, contract))
                            .await
                            .map(|r| format_result(&r)),
                    }
                }
            }
            "list_tables" => {
                let sql = "SELECT table_schema, table_name FROM information_schema.tables \
                           WHERE table_schema NOT IN ('pg_catalog','information_schema') \
                           ORDER BY table_schema, table_name";
                Self::run_sql(cfg, sql, effective_read_only(cfg, contract))
                    .await
                    .map(|r| format_result(&r))
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
                        // Policy is checked against the RAW sql (so `EXPLAIN a;
                        // DROP b` is caught as multi-statement); the `EXPLAIN `
                        // prefix is applied only for execution.
                        Ok(()) => Self::run_sql(
                            cfg,
                            &format!("EXPLAIN {}", sql),
                            effective_read_only(cfg, contract),
                        )
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
    /// re-enabling — the guard rejects any statement starting with `SET`
    /// (in `is_write_sql`) and rejects multi-statement, and each call gets a
    /// fresh connection, so a single user statement cannot turn the GUC back
    /// off. The user statement runs as its OWN `simple_query` (not
    /// concatenated) so result parsing / command_tag stay correct.
    async fn run_sql(
        cfg: &McpConfig,
        sql: &str,
        read_only: bool,
    ) -> std::result::Result<QueryResult, String> {
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
        if read_only {
            if let Err(e) = client
                .execute("SET default_transaction_read_only = on")
                .await
            {
                client.close().await;
                return Err(format!("failed to enforce read-only mode: {}", e));
            }
        }
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

/// `bytes[i]` is `'` — return the index just past the closing quote (or
/// end-of-input if unterminated). `''` is an in-string escape, not a close.
fn skip_single_quoted(bytes: &[u8], i: usize) -> usize {
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == b'\'' {
            if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                j += 2; // doubled quote → literal, stay in string
                continue;
            }
            return j + 1;
        }
        j += 1;
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
            b'$' => match skip_dollar_quoted(bytes, i) {
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
            b'$' => match skip_dollar_quoted(bytes, i) {
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
    }

    #[test]
    fn check_policy_read_only_rejects_bypasses() {
        let cfg = cfg_ro(true);
        for sql in [
            "SELECT 1; DROP TABLE t",
            "select 1 ; drop table t",
            "WITH x AS (INSERT INTO t VALUES(1) RETURNING *) SELECT * FROM x",
            "with recursive r as (delete from t returning *) select * from r",
            "SET default_transaction_read_only=off",
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
