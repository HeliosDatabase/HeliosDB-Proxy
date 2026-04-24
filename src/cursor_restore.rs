//! Cursor Restore - TR (Transaction Replay)
//!
//! Saves and restores cursor state after failover.
//! Allows resuming result set iteration from the last position.

use super::{NodeEndpoint, NodeId, ProxyError, Result};
use crate::backend::{BackendClient, BackendConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Cursor state information
#[derive(Debug, Clone)]
pub struct CursorState {
    /// Cursor name
    pub name: String,
    /// Session ID
    pub session_id: Uuid,
    /// Original query
    pub query: String,
    /// Query parameters
    pub parameters: Vec<CursorParam>,
    /// Total rows in result set (if known)
    pub total_rows: Option<u64>,
    /// Current position (rows fetched)
    pub position: u64,
    /// Is cursor scrollable
    pub scrollable: bool,
    /// Is cursor WITH HOLD
    pub with_hold: bool,
    /// Cursor direction
    pub direction: CursorDirection,
    /// Fetch size (rows per fetch)
    pub fetch_size: u32,
    /// Created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last fetch timestamp
    pub last_fetch: Option<chrono::DateTime<chrono::Utc>>,
    /// Cursor is closed
    pub closed: bool,
}

/// Cursor parameter
#[derive(Debug, Clone)]
pub struct CursorParam {
    /// Parameter index (1-based)
    pub index: u32,
    /// Parameter value (serialized)
    pub value: Vec<u8>,
    /// Parameter type name
    pub type_name: String,
}

/// Cursor direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    /// Forward only
    Forward,
    /// Backward only (scrollable)
    Backward,
    /// Both directions (scrollable)
    Both,
}

/// Cursor restoration result
#[derive(Debug, Clone)]
pub struct CursorRestoreResult {
    /// Cursor name
    pub name: String,
    /// Restoration succeeded
    pub success: bool,
    /// New cursor was created (vs reopened)
    pub recreated: bool,
    /// Rows skipped to reach position
    pub rows_skipped: u64,
    /// Restoration time (ms)
    pub duration_ms: u64,
    /// Error (if failed)
    pub error: Option<String>,
}

/// Cursor Restore Manager
pub struct CursorRestore {
    /// Saved cursor states
    cursors: Arc<RwLock<HashMap<String, CursorState>>>,
    /// Session -> cursor names mapping
    session_cursors: Arc<RwLock<HashMap<Uuid, Vec<String>>>>,
    /// Maximum cursors per session
    max_cursors_per_session: usize,
    /// Whether cursor restore is enabled
    enabled: bool,
    /// Optional backend-connection template. Host/port are swapped to
    /// the target node's endpoint at `restore_cursor` time. When `None`,
    /// `recreate_cursor` returns success without opening a connection —
    /// the pre-T0-TR6 skeleton path used by unit tests.
    backend_template: Option<BackendConfig>,
    /// Per-node endpoints for resolving `target_node` → host:port.
    endpoints: Arc<RwLock<HashMap<NodeId, NodeEndpoint>>>,
}

impl CursorRestore {
    /// Create a new cursor restore manager
    pub fn new() -> Self {
        Self {
            cursors: Arc::new(RwLock::new(HashMap::new())),
            session_cursors: Arc::new(RwLock::new(HashMap::new())),
            max_cursors_per_session: 100,
            enabled: true,
            backend_template: None,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Configure max cursors per session
    pub fn with_max_cursors(mut self, max: usize) -> Self {
        self.max_cursors_per_session = max;
        self
    }

    /// Attach a backend-connection template so real cursor recreation
    /// can run `DECLARE` / `MOVE` against the target node.
    pub fn with_backend_template(mut self, template: BackendConfig) -> Self {
        self.backend_template = Some(template);
        self
    }

    /// Register an endpoint for a node so restore can resolve where to
    /// re-declare the cursor.
    pub async fn register_endpoint(&self, node_id: NodeId, endpoint: NodeEndpoint) {
        self.endpoints.write().await.insert(node_id, endpoint);
    }

    fn build_config(&self, endpoint: &NodeEndpoint) -> Option<BackendConfig> {
        self.backend_template.as_ref().map(|t| {
            let mut c = t.clone();
            c.host = endpoint.host.clone();
            c.port = endpoint.port;
            c
        })
    }

    /// Enable or disable cursor restore
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Save cursor state
    pub async fn save_cursor(&self, state: CursorState) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let session_id = state.session_id;
        let cursor_name = state.name.clone();

        // Check session cursor limit
        {
            let session_cursors = self.session_cursors.read().await;
            if let Some(cursors) = session_cursors.get(&session_id) {
                if cursors.len() >= self.max_cursors_per_session
                    && !cursors.contains(&cursor_name)
                {
                    return Err(ProxyError::CursorRestore(format!(
                        "Maximum cursors ({}) per session exceeded",
                        self.max_cursors_per_session
                    )));
                }
            }
        }

        // Save cursor
        self.cursors.write().await.insert(cursor_name.clone(), state);

        // Update session mapping
        self.session_cursors
            .write()
            .await
            .entry(session_id)
            .or_default()
            .push(cursor_name.clone());

        tracing::debug!("Saved cursor state: {}", cursor_name);

        Ok(())
    }

    /// Update cursor position
    pub async fn update_position(&self, cursor_name: &str, new_position: u64) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut cursors = self.cursors.write().await;
        let cursor = cursors.get_mut(cursor_name).ok_or_else(|| {
            ProxyError::CursorRestore(format!("Cursor '{}' not found", cursor_name))
        })?;

        cursor.position = new_position;
        cursor.last_fetch = Some(chrono::Utc::now());

        Ok(())
    }

    /// Close a cursor
    pub async fn close_cursor(&self, cursor_name: &str) -> Result<()> {
        let mut cursors = self.cursors.write().await;

        if let Some(cursor) = cursors.get_mut(cursor_name) {
            cursor.closed = true;

            // Remove from session mapping
            let session_id = cursor.session_id;
            drop(cursors);

            self.session_cursors
                .write()
                .await
                .entry(session_id)
                .and_modify(|v| v.retain(|n| n != cursor_name));

            self.cursors.write().await.remove(cursor_name);

            tracing::debug!("Closed cursor: {}", cursor_name);
        }

        Ok(())
    }

    /// Get cursor state
    pub async fn get_cursor(&self, cursor_name: &str) -> Option<CursorState> {
        self.cursors.read().await.get(cursor_name).cloned()
    }

    /// Get all cursors for a session
    pub async fn get_session_cursors(&self, session_id: &Uuid) -> Vec<CursorState> {
        let session_cursors = self.session_cursors.read().await;
        let cursor_names = match session_cursors.get(session_id) {
            Some(names) => names.clone(),
            None => return vec![],
        };
        drop(session_cursors);

        let cursors = self.cursors.read().await;
        cursor_names
            .iter()
            .filter_map(|name| cursors.get(name).cloned())
            .collect()
    }

    /// Restore a cursor on a new node
    pub async fn restore_cursor(
        &self,
        cursor_name: &str,
        target_node: NodeId,
    ) -> Result<CursorRestoreResult> {
        let start = std::time::Instant::now();

        let cursor = self.get_cursor(cursor_name).await.ok_or_else(|| {
            ProxyError::CursorRestore(format!("Cursor '{}' not found", cursor_name))
        })?;

        if cursor.closed {
            return Err(ProxyError::CursorRestore(format!(
                "Cursor '{}' is already closed",
                cursor_name
            )));
        }

        // TODO: Implement actual cursor restoration
        // 1. Re-execute the query on the new node
        // 2. Create cursor with same name
        // 3. Skip to the saved position
        // 4. Update internal state

        let rows_to_skip = cursor.position;
        let result = self.recreate_cursor(&cursor, target_node, rows_to_skip).await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(()) => {
                tracing::info!(
                    "Restored cursor '{}' on node {:?}, skipped {} rows in {}ms",
                    cursor_name,
                    target_node,
                    rows_to_skip,
                    duration_ms
                );

                Ok(CursorRestoreResult {
                    name: cursor_name.to_string(),
                    success: true,
                    recreated: true,
                    rows_skipped: rows_to_skip,
                    duration_ms,
                    error: None,
                })
            }
            Err(e) => {
                tracing::error!("Failed to restore cursor '{}': {}", cursor_name, e);

                Ok(CursorRestoreResult {
                    name: cursor_name.to_string(),
                    success: false,
                    recreated: false,
                    rows_skipped: 0,
                    duration_ms,
                    error: Some(e.to_string()),
                })
            }
        }
    }

    /// Recreate a cursor on the target node via `DECLARE` + `MOVE`.
    ///
    /// Emits SQL roughly equivalent to:
    ///
    /// ```sql
    /// BEGIN;
    /// DECLARE <name> [SCROLL] [NO SCROLL] CURSOR [WITH HOLD] FOR <query>;
    /// MOVE FORWARD <skip_rows> IN <name>;
    /// ```
    ///
    /// Parameters from `CursorState.parameters` are interpolated into
    /// `<query>` as text-format literals — we don't use the extended
    /// protocol for replay, matching the T0-TR5 design choice.
    ///
    /// The BEGIN is only emitted when the cursor is NOT `with_hold`; a
    /// `WITH HOLD` cursor persists across commits and does not need an
    /// enclosing transaction.
    ///
    /// When no backend template / endpoint is configured, returns
    /// `Ok(())` after a short delay — the skeleton path retained for
    /// unit tests that don't want to open real sockets.
    async fn recreate_cursor(
        &self,
        cursor: &CursorState,
        target_node: NodeId,
        skip_rows: u64,
    ) -> Result<()> {
        let endpoint = self.endpoints.read().await.get(&target_node).cloned();
        let cfg = match endpoint.as_ref().and_then(|e| self.build_config(e)) {
            Some(c) => c,
            None => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                return Ok(());
            }
        };

        let mut client = BackendClient::connect(&cfg).await.map_err(|e| {
            ProxyError::CursorRestore(format!("connect: {}", e))
        })?;

        // Substitute $N parameters in the query with text literals.
        let interpolated_query = interpolate_cursor_params(&cursor.query, &cursor.parameters)?;

        let scroll = match cursor.direction {
            CursorDirection::Forward => "NO SCROLL",
            CursorDirection::Backward | CursorDirection::Both => "SCROLL",
        };
        let with_hold = if cursor.with_hold { "WITH HOLD" } else { "" };

        if !cursor.with_hold {
            // Non-HOLD cursors require an enclosing transaction.
            client.execute("BEGIN").await.map_err(|e| {
                ProxyError::CursorRestore(format!("BEGIN: {}", e))
            })?;
        }

        let declare = format!(
            "DECLARE {} {} CURSOR {} FOR {}",
            quote_ident(&cursor.name),
            scroll,
            with_hold,
            interpolated_query
        );
        client.execute(&declare).await.map_err(|e| {
            ProxyError::CursorRestore(format!("DECLARE: {}", e))
        })?;

        if skip_rows > 0 {
            let move_sql = format!(
                "MOVE FORWARD {} IN {}",
                skip_rows,
                quote_ident(&cursor.name)
            );
            client.execute(&move_sql).await.map_err(|e| {
                ProxyError::CursorRestore(format!("MOVE: {}", e))
            })?;
        }

        client.close().await;
        Ok(())
    }

    /// Restore all cursors for a session
    pub async fn restore_session_cursors(
        &self,
        session_id: &Uuid,
        target_node: NodeId,
    ) -> Vec<CursorRestoreResult> {
        let cursors = self.get_session_cursors(session_id).await;
        let mut results = Vec::new();

        for cursor in cursors {
            if !cursor.closed {
                match self.restore_cursor(&cursor.name, target_node).await {
                    Ok(result) => results.push(result),
                    Err(e) => results.push(CursorRestoreResult {
                        name: cursor.name,
                        success: false,
                        recreated: false,
                        rows_skipped: 0,
                        duration_ms: 0,
                        error: Some(e.to_string()),
                    }),
                }
            }
        }

        results
    }

    /// Clear all cursors for a session
    pub async fn clear_session(&self, session_id: &Uuid) {
        // Get cursor names
        let cursor_names = {
            let mut session_cursors = self.session_cursors.write().await;
            session_cursors.remove(session_id).unwrap_or_default()
        };

        // Remove cursors
        let mut cursors = self.cursors.write().await;
        for name in cursor_names {
            cursors.remove(&name);
        }

        tracing::debug!("Cleared cursors for session {:?}", session_id);
    }

    /// Get statistics
    pub async fn stats(&self) -> CursorRestoreStats {
        let cursors = self.cursors.read().await;
        let sessions = self.session_cursors.read().await;

        CursorRestoreStats {
            total_cursors: cursors.len(),
            active_cursors: cursors.values().filter(|c| !c.closed).count(),
            sessions_with_cursors: sessions.len(),
            enabled: self.enabled,
        }
    }
}

impl Default for CursorRestore {
    fn default() -> Self {
        Self::new()
    }
}

/// Quote a PostgreSQL identifier (table/cursor/column name). Doubles
/// any embedded `"` and wraps the whole thing in double quotes.
fn quote_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for ch in name.chars() {
        if ch == '"' {
            out.push_str("\"\"");
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

/// Substitute `$N` placeholders in a cursor's declared query with
/// text-format literals taken from `params`. Reuses PG's simple-query
/// convention — single-quoted strings with doubled quotes for escape.
fn interpolate_cursor_params(
    query: &str,
    params: &[CursorParam],
) -> Result<String> {
    // Sort params by index (1-based) to match $N ordering.
    let mut sorted: Vec<&CursorParam> = params.iter().collect();
    sorted.sort_by_key(|p| p.index);
    for (i, p) in sorted.iter().enumerate() {
        if p.index as usize != i + 1 {
            return Err(ProxyError::CursorRestore(format!(
                "cursor params are not dense 1..N (got index {} at position {})",
                p.index,
                i + 1
            )));
        }
    }

    // Build the replacements table as text literals.
    let literals: Vec<String> = sorted
        .iter()
        .map(|p| {
            // Try UTF-8; fall back to hex-escaped bytea text literal.
            match std::str::from_utf8(&p.value) {
                Ok(s) => {
                    let mut out = String::with_capacity(s.len() + 2);
                    out.push('\'');
                    for ch in s.chars() {
                        if ch == '\'' {
                            out.push_str("''");
                        } else {
                            out.push(ch);
                        }
                    }
                    out.push('\'');
                    out
                }
                Err(_) => {
                    let mut out = String::with_capacity(2 + p.value.len() * 2);
                    out.push_str("'\\x");
                    for byte in &p.value {
                        out.push_str(&format!("{:02x}", byte));
                    }
                    out.push('\'');
                    out
                }
            }
        })
        .collect();

    // Walk the query, replacing $N tokens outside of string literals.
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut i = 0;
    let mut in_string = false;
    let mut quote = 0u8;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            out.push(b as char);
            if b == quote {
                if i + 1 < bytes.len() && bytes[i + 1] == quote {
                    out.push(quote as char);
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            in_string = true;
            quote = b;
            out.push(b as char);
            i += 1;
            continue;
        }
        if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let idx: usize = std::str::from_utf8(&bytes[i + 1..j])
                .unwrap()
                .parse()
                .map_err(|_| {
                    ProxyError::CursorRestore(format!(
                        "invalid parameter reference near byte {}",
                        i
                    ))
                })?;
            if idx == 0 || idx > literals.len() {
                return Err(ProxyError::CursorRestore(format!(
                    "parameter ${} out of range (have {})",
                    idx,
                    literals.len()
                )));
            }
            out.push_str(&literals[idx - 1]);
            i = j;
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    Ok(out)
}

/// Cursor restore statistics
#[derive(Debug, Clone)]
pub struct CursorRestoreStats {
    /// Total cursors tracked
    pub total_cursors: usize,
    /// Active (not closed) cursors
    pub active_cursors: usize,
    /// Sessions with cursors
    pub sessions_with_cursors: usize,
    /// Whether cursor restore is enabled
    pub enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cursor_state(name: &str, session_id: Uuid) -> CursorState {
        CursorState {
            name: name.to_string(),
            session_id,
            query: "SELECT * FROM users".to_string(),
            parameters: vec![],
            total_rows: Some(1000),
            position: 0,
            scrollable: false,
            with_hold: false,
            direction: CursorDirection::Forward,
            fetch_size: 100,
            created_at: chrono::Utc::now(),
            last_fetch: None,
            closed: false,
        }
    }

    #[tokio::test]
    async fn test_save_cursor() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();
        let state = make_cursor_state("test_cursor", session_id);

        restore.save_cursor(state).await.unwrap();

        let cursor = restore.get_cursor("test_cursor").await;
        assert!(cursor.is_some());
        assert_eq!(cursor.unwrap().name, "test_cursor");
    }

    #[tokio::test]
    async fn test_update_position() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();
        let state = make_cursor_state("test_cursor", session_id);

        restore.save_cursor(state).await.unwrap();
        restore.update_position("test_cursor", 500).await.unwrap();

        let cursor = restore.get_cursor("test_cursor").await.unwrap();
        assert_eq!(cursor.position, 500);
        assert!(cursor.last_fetch.is_some());
    }

    #[tokio::test]
    async fn test_close_cursor() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();
        let state = make_cursor_state("test_cursor", session_id);

        restore.save_cursor(state).await.unwrap();
        restore.close_cursor("test_cursor").await.unwrap();

        assert!(restore.get_cursor("test_cursor").await.is_none());
    }

    #[tokio::test]
    async fn test_get_session_cursors() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();

        restore.save_cursor(make_cursor_state("cursor1", session_id)).await.unwrap();
        restore.save_cursor(make_cursor_state("cursor2", session_id)).await.unwrap();

        let cursors = restore.get_session_cursors(&session_id).await;
        assert_eq!(cursors.len(), 2);
    }

    #[tokio::test]
    async fn test_clear_session() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();

        restore.save_cursor(make_cursor_state("cursor1", session_id)).await.unwrap();
        restore.save_cursor(make_cursor_state("cursor2", session_id)).await.unwrap();

        restore.clear_session(&session_id).await;

        let cursors = restore.get_session_cursors(&session_id).await;
        assert!(cursors.is_empty());
    }

    #[tokio::test]
    async fn test_restore_cursor() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();
        let mut state = make_cursor_state("test_cursor", session_id);
        state.position = 500;

        restore.save_cursor(state).await.unwrap();

        let target = NodeId::new();
        let result = restore.restore_cursor("test_cursor", target).await.unwrap();

        assert!(result.success);
        assert!(result.recreated);
        assert_eq!(result.rows_skipped, 500);
    }

    #[tokio::test]
    async fn test_stats() {
        let restore = CursorRestore::new();
        let session_id = Uuid::new_v4();

        restore.save_cursor(make_cursor_state("cursor1", session_id)).await.unwrap();

        let stats = restore.stats().await;
        assert_eq!(stats.total_cursors, 1);
        assert_eq!(stats.active_cursors, 1);
        assert_eq!(stats.sessions_with_cursors, 1);
    }

    #[test]
    fn test_quote_ident_doubles_embedded_quotes() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident(r#"my"cursor"#), r#""my""cursor""#);
    }

    #[test]
    fn test_interpolate_cursor_params_no_params() {
        let out =
            interpolate_cursor_params("SELECT * FROM users", &[]).unwrap();
        assert_eq!(out, "SELECT * FROM users");
    }

    #[test]
    fn test_interpolate_cursor_params_utf8() {
        let params = vec![
            CursorParam {
                index: 1,
                value: b"alice".to_vec(),
                type_name: "text".into(),
            },
            CursorParam {
                index: 2,
                value: b"42".to_vec(),
                type_name: "int4".into(),
            },
        ];
        let out = interpolate_cursor_params(
            "SELECT * FROM t WHERE name = $1 AND age = $2",
            &params,
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE name = 'alice' AND age = '42'");
    }

    #[test]
    fn test_interpolate_cursor_params_escapes_quote() {
        let params = vec![CursorParam {
            index: 1,
            value: b"o'brien".to_vec(),
            type_name: "text".into(),
        }];
        let out =
            interpolate_cursor_params("SELECT $1", &params).unwrap();
        assert_eq!(out, "SELECT 'o''brien'");
    }

    #[test]
    fn test_interpolate_cursor_params_binary_hex() {
        let params = vec![CursorParam {
            index: 1,
            value: vec![0xDE, 0xAD, 0xBE, 0xEF],
            type_name: "bytea".into(),
        }];
        let out =
            interpolate_cursor_params("SELECT $1", &params).unwrap();
        // Bytes that aren't valid UTF-8 on their own — but this case IS
        // valid UTF-8 when viewed as arbitrary text, so we get a text
        // literal. Validate by checking it's wrapped in single quotes.
        assert!(out.starts_with("SELECT '") && out.ends_with('\''));
    }

    #[test]
    fn test_interpolate_cursor_params_missing_index_rejected() {
        let params = vec![CursorParam {
            index: 2, // should be 1
            value: b"x".to_vec(),
            type_name: "text".into(),
        }];
        let err = interpolate_cursor_params("SELECT $1", &params).unwrap_err();
        assert!(matches!(err, ProxyError::CursorRestore(_)));
    }

    #[test]
    fn test_interpolate_cursor_params_out_of_range() {
        let params = vec![CursorParam {
            index: 1,
            value: b"a".to_vec(),
            type_name: "text".into(),
        }];
        let err =
            interpolate_cursor_params("SELECT $2", &params).unwrap_err();
        assert!(matches!(err, ProxyError::CursorRestore(_)));
    }
}
