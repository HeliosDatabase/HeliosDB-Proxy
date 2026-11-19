//! Session Migrate - TR (Transaction Replay)
//!
//! Saves and restores session state after failover.
//! Includes SET parameters, timezone, search_path, and custom variables.

use super::{NodeId, ProxyError, Result};
use crate::backend::BackendClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Quote a PostgreSQL identifier (table/column name). Same semantics as
/// `cursor_restore::quote_ident` — kept module-local to avoid a cross-
/// module coupling just for one helper.
fn quote_session_ident(name: &str) -> String {
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

/// A prepared-statement name as SQL: bare when it is a plain lower-case
/// identifier (how PostgreSQL folds an unquoted `PREPARE` name), quoted
/// otherwise so the exact recorded name is recreated.
fn prepared_name_sql(name: &str) -> String {
    let mut chars = name.chars();
    let plain = matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '$');
    if plain {
        name.to_string()
    } else {
        quote_session_ident(name)
    }
}

/// Quote a value as a PostgreSQL string literal. A value containing a
/// backslash uses the `E'...'` form with backslashes doubled, so it reads
/// the same whatever `standard_conforming_strings` is.
fn quote_session_literal(value: &str) -> String {
    let escape = value.contains('\\');
    let mut out = String::with_capacity(value.len() + 3);
    if escape {
        out.push('E');
    }
    out.push('\'');
    for ch in value.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' if escape => out.push_str("\\\\"),
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

/// `SELECT pg_catalog.set_config(<name>, <value>, false)`: sets a session
/// parameter to exactly the recorded text, including list values such as
/// `search_path` and dotted custom names, with both quoted as literals.
fn set_config_statement(name: &str, value: &str) -> String {
    format!(
        "SELECT pg_catalog.set_config({}, {}, false)",
        quote_session_literal(name),
        quote_session_literal(value)
    )
}

/// The `CREATE TEMP TABLE IF NOT EXISTS` that recreates `table`'s schema
/// (empty) from its recorded columns. Column types and default
/// expressions are copied verbatim.
pub fn temp_table_restore_statement(table: &TempTableInfo) -> String {
    let mut stmt = String::with_capacity(64 + table.name.len());
    stmt.push_str("CREATE TEMP TABLE IF NOT EXISTS ");
    stmt.push_str(&quote_session_ident(&table.name));
    stmt.push_str(" (");
    for (i, col) in table.columns.iter().enumerate() {
        if i > 0 {
            stmt.push_str(", ");
        }
        stmt.push_str(&quote_session_ident(&col.name));
        stmt.push(' ');
        stmt.push_str(&col.data_type);
        if !col.nullable {
            stmt.push_str(" NOT NULL");
        }
        if let Some(default) = &col.default_expr {
            stmt.push_str(" DEFAULT ");
            stmt.push_str(default);
        }
    }
    stmt.push(')');
    stmt
}

/// Session state information
#[derive(Debug, Clone)]
pub struct SessionState {
    /// Session ID
    pub session_id: Uuid,
    /// User name
    pub user: String,
    /// Database name
    pub database: String,
    /// Application name
    pub application_name: Option<String>,
    /// Client encoding
    pub client_encoding: String,
    /// Server encoding
    pub server_encoding: String,
    /// Timezone
    pub timezone: String,
    /// Search path
    pub search_path: Vec<String>,
    /// DateStyle
    pub datestyle: String,
    /// IntervalStyle
    pub intervalstyle: String,
    /// Custom SET parameters
    pub custom_parameters: HashMap<String, String>,
    /// Session-local temporary tables
    pub temp_tables: Vec<TempTableInfo>,
    /// Prepared statements
    pub prepared_statements: HashMap<String, PreparedStatementInfo>,
    /// Session created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last activity timestamp
    pub last_activity: chrono::DateTime<chrono::Utc>,
    /// Original node
    pub original_node: NodeId,
}

/// Temporary table information
#[derive(Debug, Clone)]
pub struct TempTableInfo {
    /// Table name
    pub name: String,
    /// Schema (usually pg_temp_N)
    pub schema: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Has data that needs migration
    pub has_data: bool,
    /// Row count (if known)
    pub row_count: Option<u64>,
}

/// Column definition
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: String,
    /// Is nullable
    pub nullable: bool,
    /// Default value expression
    pub default_expr: Option<String>,
}

/// Prepared statement information
#[derive(Debug, Clone)]
pub struct PreparedStatementInfo {
    /// Statement name
    pub name: String,
    /// SQL query
    pub query: String,
    /// Parameter types
    pub param_types: Vec<String>,
    /// Created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl SessionState {
    /// Create a new session state
    pub fn new(session_id: Uuid, user: String, database: String, node: NodeId) -> Self {
        Self {
            session_id,
            user,
            database,
            application_name: None,
            client_encoding: "UTF8".to_string(),
            server_encoding: "UTF8".to_string(),
            timezone: "UTC".to_string(),
            search_path: vec!["public".to_string()],
            datestyle: "ISO, MDY".to_string(),
            intervalstyle: "postgres".to_string(),
            custom_parameters: HashMap::new(),
            temp_tables: Vec::new(),
            prepared_statements: HashMap::new(),
            created_at: chrono::Utc::now(),
            last_activity: chrono::Utc::now(),
            original_node: node,
        }
    }

    /// Set a custom parameter
    pub fn set_parameter(&mut self, name: String, value: String) {
        // Handle well-known parameters
        match name.to_lowercase().as_str() {
            "timezone" => self.timezone = value,
            "search_path" => {
                self.search_path = value.split(',').map(|s| s.trim().to_string()).collect()
            }
            "client_encoding" => self.client_encoding = value,
            "datestyle" => self.datestyle = value,
            "intervalstyle" => self.intervalstyle = value,
            "application_name" => self.application_name = Some(value),
            _ => {
                self.custom_parameters.insert(name, value);
            }
        }
        self.last_activity = chrono::Utc::now();
    }

    /// Get a parameter value
    pub fn get_parameter(&self, name: &str) -> Option<String> {
        match name.to_lowercase().as_str() {
            "timezone" => Some(self.timezone.clone()),
            "search_path" => Some(self.search_path.join(", ")),
            "client_encoding" => Some(self.client_encoding.clone()),
            "server_encoding" => Some(self.server_encoding.clone()),
            "datestyle" => Some(self.datestyle.clone()),
            "intervalstyle" => Some(self.intervalstyle.clone()),
            "application_name" => self.application_name.clone(),
            _ => self.custom_parameters.get(name).cloned(),
        }
    }

    /// Add a prepared statement
    pub fn add_prepared_statement(&mut self, info: PreparedStatementInfo) {
        self.prepared_statements.insert(info.name.clone(), info);
        self.last_activity = chrono::Utc::now();
    }

    /// Remove a prepared statement
    pub fn remove_prepared_statement(&mut self, name: &str) {
        self.prepared_statements.remove(name);
    }

    /// Add a temp table
    pub fn add_temp_table(&mut self, info: TempTableInfo) {
        self.temp_tables.push(info);
        self.last_activity = chrono::Utc::now();
    }

    /// The statements that restore this session's parameters: one
    /// `SELECT pg_catalog.set_config(name, value, false)` per parameter
    /// (timezone, search_path, client_encoding, datestyle, intervalstyle,
    /// application_name when set, then the custom ones).
    pub fn parameter_restore_statements(&self) -> Vec<String> {
        let mut statements = vec![
            set_config_statement("timezone", &self.timezone),
            set_config_statement("search_path", &self.search_path.join(", ")),
            set_config_statement("client_encoding", &self.client_encoding),
            set_config_statement("datestyle", &self.datestyle),
            set_config_statement("intervalstyle", &self.intervalstyle),
        ];
        if let Some(ref app_name) = self.application_name {
            statements.push(set_config_statement("application_name", app_name));
        }
        for (name, value) in &self.custom_parameters {
            statements.push(set_config_statement(name, value));
        }
        statements
    }

    /// The `PREPARE` statements that recreate this session's prepared
    /// statements.
    pub fn prepared_restore_statements(&self) -> Vec<String> {
        self.prepared_statements
            .values()
            .map(|prep| {
                if prep.param_types.is_empty() {
                    format!(
                        "PREPARE {} AS {}",
                        prepared_name_sql(&prep.name),
                        prep.query
                    )
                } else {
                    format!(
                        "PREPARE {} ({}) AS {}",
                        prepared_name_sql(&prep.name),
                        prep.param_types.join(", "),
                        prep.query
                    )
                }
            })
            .collect()
    }

    /// Parameter then prepared-statement restore statements, in the order
    /// [`SessionMigrate::migrate_session`] runs them.
    pub fn generate_restore_statements(&self) -> Vec<String> {
        let mut statements = self.parameter_restore_statements();
        statements.extend(self.prepared_restore_statements());
        statements
    }
}

/// Session migration result
#[derive(Debug, Clone)]
pub struct SessionMigrateResult {
    /// Session ID
    pub session_id: Uuid,
    /// Every restore statement succeeded on the target connection
    pub success: bool,
    /// Target node
    pub target_node: NodeId,
    /// Session parameters restored
    pub parameters_restored: usize,
    /// Prepared statements restored
    pub prepared_statements_restored: usize,
    /// Temp tables recreated (schema only)
    pub temp_tables_migrated: usize,
    /// Temp tables not recreated because the migration failed first
    pub temp_tables_failed: usize,
    /// Migration time (ms)
    pub duration_ms: u64,
    /// Error (if failed)
    pub error: Option<String>,
}

/// Session Migrate Manager
pub struct SessionMigrate {
    /// Saved session states
    sessions: Arc<RwLock<HashMap<Uuid, SessionState>>>,
    /// Whether session migration is enabled
    enabled: bool,
    /// Migrate temp tables (expensive)
    migrate_temp_tables: bool,
    /// Maximum sessions to track
    max_sessions: usize,
}

impl SessionMigrate {
    /// Create a new session migrate manager
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            enabled: true,
            migrate_temp_tables: false, // Disabled by default (expensive)
            max_sessions: 10000,
        }
    }

    /// Configure max sessions
    pub fn with_max_sessions(mut self, max: usize) -> Self {
        self.max_sessions = max;
        self
    }

    /// Enable/disable temp table migration
    pub fn with_temp_table_migration(mut self, enabled: bool) -> Self {
        self.migrate_temp_tables = enabled;
        self
    }

    /// Enable or disable session migration
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Register a new session
    pub async fn register_session(&self, state: SessionState) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let session_id = state.session_id;

        // Check limit
        {
            let sessions = self.sessions.read().await;
            if sessions.len() >= self.max_sessions && !sessions.contains_key(&session_id) {
                return Err(ProxyError::SessionMigration(format!(
                    "Maximum sessions ({}) exceeded",
                    self.max_sessions
                )));
            }
        }

        self.sessions.write().await.insert(session_id, state);
        tracing::debug!("Registered session {:?}", session_id);

        Ok(())
    }

    /// Update session parameter
    pub async fn set_parameter(&self, session_id: Uuid, name: String, value: String) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            ProxyError::SessionMigration(format!("Session {:?} not found", session_id))
        })?;

        session.set_parameter(name, value);
        Ok(())
    }

    /// Add prepared statement to session
    pub async fn add_prepared_statement(
        &self,
        session_id: Uuid,
        info: PreparedStatementInfo,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            ProxyError::SessionMigration(format!("Session {:?} not found", session_id))
        })?;

        session.add_prepared_statement(info);
        Ok(())
    }

    /// Remove prepared statement from session
    pub async fn remove_prepared_statement(&self, session_id: Uuid, name: &str) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(&session_id) {
            session.remove_prepared_statement(name);
        }
        Ok(())
    }

    /// Add temp table to session
    pub async fn add_temp_table(&self, session_id: Uuid, info: TempTableInfo) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(&session_id).ok_or_else(|| {
            ProxyError::SessionMigration(format!("Session {:?} not found", session_id))
        })?;

        session.add_temp_table(info);
        Ok(())
    }

    /// Get session state
    pub async fn get_session(&self, session_id: &Uuid) -> Option<SessionState> {
        self.sessions.read().await.get(session_id).cloned()
    }

    /// Close session
    pub async fn close_session(&self, session_id: &Uuid) {
        self.sessions.write().await.remove(session_id);
        tracing::debug!("Closed session {:?}", session_id);
    }

    /// Restore the saved session `session_id` on `client`, the connection
    /// the session continues on at `target_node` after a failover: its
    /// parameters ([`SessionState::parameter_restore_statements`]), its
    /// prepared statements, then (with temp-table migration enabled) the
    /// schema of each temp table ([`temp_table_restore_statement`]), run
    /// in that order. Session state lives on a connection, so it is
    /// restored on the caller's connection, never on one of its own.
    ///
    /// `Err`, before anything is sent, when migration is disabled, the
    /// session is unknown, or its temp tables cannot be carried over:
    /// temp-table migration is disabled, or a table holds rows (they died
    /// with the failed node and are not recreated). A statement the
    /// backend refuses stops the migration and is reported in the result
    /// (`success: false`, `error`); `client` then holds a partial session
    /// and must be discarded, and the session keeps its original node.
    pub async fn migrate_session(
        &self,
        session_id: Uuid,
        target_node: NodeId,
        client: &mut BackendClient,
    ) -> Result<SessionMigrateResult> {
        let start = std::time::Instant::now();

        if !self.enabled {
            return Err(ProxyError::SessionMigration(
                "session migration is disabled".to_string(),
            ));
        }
        let session = self.get_session(&session_id).await.ok_or_else(|| {
            ProxyError::SessionMigration(format!("Session {:?} not found", session_id))
        })?;
        self.check_temp_tables(&session)?;

        let parameters = session.parameter_restore_statements();
        let prepared = session.prepared_restore_statements();
        let temp_tables: Vec<String> = if self.migrate_temp_tables {
            session
                .temp_tables
                .iter()
                .map(temp_table_restore_statement)
                .collect()
        } else {
            Vec::new()
        };

        let mut counts = [0usize; 3];
        let mut failure = None;
        'restore: for (kind, statements) in [&parameters, &prepared, &temp_tables]
            .into_iter()
            .enumerate()
        {
            for stmt in statements {
                if let Err(e) = client.execute(stmt).await {
                    failure = Some(format!("{}: {}", stmt, e));
                    break 'restore;
                }
                counts[kind] += 1;
            }
        }
        let [parameters_restored, prepared_statements_restored, temp_tables_migrated] = counts;
        let duration_ms = start.elapsed().as_millis() as u64;

        if let Some(error) = failure {
            tracing::error!(
                "Failed to migrate session {:?} to node {:?}: {}",
                session_id,
                target_node,
                error
            );
            return Ok(SessionMigrateResult {
                session_id,
                success: false,
                target_node,
                parameters_restored,
                prepared_statements_restored,
                temp_tables_migrated,
                temp_tables_failed: temp_tables.len() - temp_tables_migrated,
                duration_ms,
                error: Some(error),
            });
        }

        {
            let mut sessions = self.sessions.write().await;
            if let Some(s) = sessions.get_mut(&session_id) {
                s.original_node = target_node;
                s.last_activity = chrono::Utc::now();
            }
        }

        tracing::info!(
            "Migrated session {:?} to node {:?}: {} params, {} prepared, {} temp tables, {}ms",
            session_id,
            target_node,
            parameters_restored,
            prepared_statements_restored,
            temp_tables_migrated,
            duration_ms
        );

        Ok(SessionMigrateResult {
            session_id,
            success: true,
            target_node,
            parameters_restored,
            prepared_statements_restored,
            temp_tables_migrated,
            temp_tables_failed: 0,
            duration_ms,
            error: None,
        })
    }

    /// Refuse a migration that would silently lose temp tables: with
    /// temp-table migration disabled any temp table is lost, and a table
    /// holding rows cannot be recreated faithfully (only its schema can;
    /// the rows lived on the failed node — journal writes into it and use
    /// failover replay instead).
    fn check_temp_tables(&self, session: &SessionState) -> Result<()> {
        let names = |tables: Vec<&TempTableInfo>| {
            tables
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !self.migrate_temp_tables && !session.temp_tables.is_empty() {
            return Err(ProxyError::SessionMigration(format!(
                "session {:?} has temp tables ({}) and temp-table migration is disabled",
                session.session_id,
                names(session.temp_tables.iter().collect())
            )));
        }
        let with_rows: Vec<&TempTableInfo> =
            session.temp_tables.iter().filter(|t| t.has_data).collect();
        if !with_rows.is_empty() {
            return Err(ProxyError::SessionMigration(format!(
                "session {:?} temp tables ({}) hold rows that cannot be migrated from the \
                 failed node; only their schema could be recreated",
                session.session_id,
                names(with_rows)
            )));
        }
        Ok(())
    }

    /// Get statistics
    pub async fn stats(&self) -> SessionMigrateStats {
        let sessions = self.sessions.read().await;

        let total_prepared: usize = sessions.values().map(|s| s.prepared_statements.len()).sum();

        let total_temp_tables: usize = sessions.values().map(|s| s.temp_tables.len()).sum();

        SessionMigrateStats {
            active_sessions: sessions.len(),
            total_prepared_statements: total_prepared,
            total_temp_tables,
            enabled: self.enabled,
            temp_table_migration_enabled: self.migrate_temp_tables,
        }
    }
}

impl Default for SessionMigrate {
    fn default() -> Self {
        Self::new()
    }
}

/// Session migrate statistics
#[derive(Debug, Clone)]
pub struct SessionMigrateStats {
    /// Active sessions tracked
    pub active_sessions: usize,
    /// Total prepared statements across sessions
    pub total_prepared_statements: usize,
    /// Total temp tables across sessions
    pub total_temp_tables: usize,
    /// Whether session migration is enabled
    pub enabled: bool,
    /// Whether temp table migration is enabled
    pub temp_table_migration_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_state_new() {
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();
        let state = SessionState::new(session_id, "user".to_string(), "db".to_string(), node_id);

        assert_eq!(state.user, "user");
        assert_eq!(state.database, "db");
        assert_eq!(state.timezone, "UTC");
        assert_eq!(state.search_path, vec!["public"]);
    }

    #[test]
    fn test_set_get_parameter() {
        let mut state = SessionState::new(
            Uuid::new_v4(),
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );

        state.set_parameter("timezone".to_string(), "America/New_York".to_string());
        assert_eq!(
            state.get_parameter("timezone"),
            Some("America/New_York".to_string())
        );

        state.set_parameter("custom_param".to_string(), "custom_value".to_string());
        assert_eq!(
            state.get_parameter("custom_param"),
            Some("custom_value".to_string())
        );
    }

    #[test]
    fn test_generate_restore_statements() {
        let mut state = SessionState::new(
            Uuid::new_v4(),
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );

        state.set_parameter("timezone".to_string(), "UTC".to_string());
        state.add_prepared_statement(PreparedStatementInfo {
            name: "my_query".to_string(),
            query: "SELECT * FROM users WHERE id = $1".to_string(),
            param_types: vec!["integer".to_string()],
            created_at: chrono::Utc::now(),
        });

        let statements = state.generate_restore_statements();

        assert!(statements.iter().any(|s| s.contains("timezone")));
        assert!(statements
            .iter()
            .any(|s| s == "PREPARE my_query (integer) AS SELECT * FROM users WHERE id = $1"));
    }

    /// Parameter values and names are SQL literals, so list values, dotted
    /// custom names, quotes and backslashes reach the target unchanged.
    #[test]
    fn test_parameter_restore_statements_quote_values() {
        let mut state = SessionState::new(
            Uuid::new_v4(),
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );
        state.set_parameter("search_path".to_string(), "\"$user\", public".to_string());
        state.set_parameter("application_name".to_string(), "o'brien".to_string());
        state.set_parameter("myapp.tenant".to_string(), "a\\b".to_string());

        let statements = state.parameter_restore_statements();
        assert_eq!(
            statements[0],
            "SELECT pg_catalog.set_config('timezone', 'UTC', false)"
        );
        assert!(statements.contains(
            &"SELECT pg_catalog.set_config('search_path', '\"$user\", public', false)".to_string()
        ));
        assert!(statements.contains(
            &"SELECT pg_catalog.set_config('application_name', 'o''brien', false)".to_string()
        ));
        assert!(statements.contains(
            &"SELECT pg_catalog.set_config('myapp.tenant', E'a\\\\b', false)".to_string()
        ));
        assert_eq!(statements.len(), 7);
    }

    /// A prepared-statement name that PostgreSQL would fold or reject
    /// unquoted is quoted, so the recorded name is recreated exactly.
    #[test]
    fn test_prepared_restore_statements_quote_unusual_names() {
        let mut state = SessionState::new(
            Uuid::new_v4(),
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );
        state.add_prepared_statement(PreparedStatementInfo {
            name: "S_1".to_string(),
            query: "SELECT 1".to_string(),
            param_types: vec![],
            created_at: chrono::Utc::now(),
        });
        assert_eq!(
            state.prepared_restore_statements(),
            vec!["PREPARE \"S_1\" AS SELECT 1".to_string()]
        );
    }

    #[test]
    fn test_temp_table_restore_statement() {
        let table = TempTableInfo {
            name: "scratch".to_string(),
            schema: "pg_temp_3".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "integer".to_string(),
                    nullable: false,
                    default_expr: None,
                },
                ColumnDef {
                    name: "note".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default_expr: Some("'x'".to_string()),
                },
            ],
            has_data: false,
            row_count: None,
        };
        assert_eq!(
            temp_table_restore_statement(&table),
            "CREATE TEMP TABLE IF NOT EXISTS \"scratch\" (\"id\" integer NOT NULL, \"note\" text DEFAULT 'x')"
        );
    }

    #[tokio::test]
    async fn test_register_session() {
        let migrate = SessionMigrate::new();
        let session_id = Uuid::new_v4();
        let state = SessionState::new(
            session_id,
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );

        migrate.register_session(state).await.unwrap();

        let session = migrate.get_session(&session_id).await;
        assert!(session.is_some());
    }

    #[tokio::test]
    async fn test_set_parameter() {
        let migrate = SessionMigrate::new();
        let session_id = Uuid::new_v4();
        let state = SessionState::new(
            session_id,
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );

        migrate.register_session(state).await.unwrap();
        migrate
            .set_parameter(
                session_id,
                "timezone".to_string(),
                "Europe/London".to_string(),
            )
            .await
            .unwrap();

        let session = migrate.get_session(&session_id).await.unwrap();
        assert_eq!(session.timezone, "Europe/London");
    }

    use crate::backend::BackendClient;
    use crate::upgrade_orchestrator::test_backend::{Reply, ScriptedPg};

    fn temp_table(name: &str, has_data: bool) -> TempTableInfo {
        TempTableInfo {
            name: name.to_string(),
            schema: "pg_temp_3".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: true,
                default_expr: None,
            }],
            has_data,
            row_count: None,
        }
    }

    /// A fake backend accepting everything except statements containing
    /// `refuse`, and a client connected to it.
    async fn backend(refuse: &'static str) -> (ScriptedPg, BackendClient) {
        let pg = ScriptedPg::spawn(move |q| {
            if q.contains(refuse) {
                Reply::Error(format!("refused: {q}"))
            } else {
                Reply::Command("OK")
            }
        })
        .await;
        let client = BackendClient::connect(&pg.config()).await.unwrap();
        (pg, client)
    }

    #[tokio::test]
    async fn test_migrate_session_restores_on_the_given_connection() {
        let migrate = SessionMigrate::new().with_temp_table_migration(true);
        let session_id = Uuid::new_v4();
        let mut state = SessionState::new(
            session_id,
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );
        state.add_prepared_statement(PreparedStatementInfo {
            name: "q".to_string(),
            query: "SELECT 1".to_string(),
            param_types: vec![],
            created_at: chrono::Utc::now(),
        });
        state.add_temp_table(temp_table("scratch", false));
        migrate.register_session(state).await.unwrap();

        let (pg, mut client) = backend("never").await;
        let target = NodeId::new();
        let result = migrate
            .migrate_session(session_id, target, &mut client)
            .await
            .unwrap();

        assert!(result.success, "{:?}", result.error);
        assert_eq!(result.parameters_restored, 5);
        assert_eq!(result.prepared_statements_restored, 1);
        assert_eq!(result.temp_tables_migrated, 1);
        assert_eq!(result.temp_tables_failed, 0);
        let seen = pg.seen();
        assert_eq!(seen.len(), 7);
        assert!(seen[0].starts_with("SELECT pg_catalog.set_config('timezone'"));
        assert_eq!(seen[5], "PREPARE q AS SELECT 1");
        assert!(seen[6].starts_with("CREATE TEMP TABLE IF NOT EXISTS \"scratch\""));
        // The session now lives on the target node.
        let session = migrate.get_session(&session_id).await.unwrap();
        assert_eq!(session.original_node, target);
        client.close().await;
    }

    /// A statement the target refuses stops the migration: the result
    /// reports the failure and the session keeps its original node.
    #[tokio::test]
    async fn test_migrate_session_reports_a_refused_statement() {
        let migrate = SessionMigrate::new().with_temp_table_migration(true);
        let session_id = Uuid::new_v4();
        let origin = NodeId::new();
        let mut state = SessionState::new(session_id, "user".to_string(), "db".to_string(), origin);
        state.set_parameter("work_mem".to_string(), "bogus".to_string());
        state.add_temp_table(temp_table("scratch", false));
        migrate.register_session(state).await.unwrap();

        let (pg, mut client) = backend("work_mem").await;
        let result = migrate
            .migrate_session(session_id, NodeId::new(), &mut client)
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.error.as_deref().unwrap().contains("refused"));
        assert_eq!(result.parameters_restored, 5);
        assert_eq!(result.temp_tables_migrated, 0);
        assert_eq!(result.temp_tables_failed, 1);
        // Nothing ran after the refusal.
        assert_eq!(pg.seen().len(), 6);
        let session = migrate.get_session(&session_id).await.unwrap();
        assert_eq!(session.original_node, origin);
        client.close().await;
    }

    /// Temp tables that cannot be carried over refuse the migration before
    /// anything is sent: with temp-table migration disabled, and for a
    /// table holding rows.
    #[tokio::test]
    async fn test_migrate_session_refuses_to_lose_temp_tables() {
        let session_id = Uuid::new_v4();
        let mut state = SessionState::new(
            session_id,
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );
        state.add_temp_table(temp_table("scratch", false));
        let disabled = SessionMigrate::new();
        disabled.register_session(state.clone()).await.unwrap();
        let (pg, mut client) = backend("never").await;
        let err = disabled
            .migrate_session(session_id, NodeId::new(), &mut client)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("temp-table migration is disabled"),
            "{err}"
        );

        state.add_temp_table(temp_table("filled", true));
        let enabled = SessionMigrate::new().with_temp_table_migration(true);
        enabled.register_session(state).await.unwrap();
        let err = enabled
            .migrate_session(session_id, NodeId::new(), &mut client)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("filled"), "{err}");
        assert!(err.to_string().contains("hold rows"), "{err}");
        assert!(pg.seen().is_empty());
        client.close().await;
    }

    #[tokio::test]
    async fn test_migrate_session_disabled_or_unknown_fails() {
        let (pg, mut client) = backend("never").await;
        let mut migrate = SessionMigrate::new();
        let err = migrate
            .migrate_session(Uuid::new_v4(), NodeId::new(), &mut client)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
        migrate.set_enabled(false);
        let err = migrate
            .migrate_session(Uuid::new_v4(), NodeId::new(), &mut client)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("disabled"), "{err}");
        assert!(pg.seen().is_empty());
        client.close().await;
    }

    #[tokio::test]
    async fn test_close_session() {
        let migrate = SessionMigrate::new();
        let session_id = Uuid::new_v4();
        let state = SessionState::new(
            session_id,
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );

        migrate.register_session(state).await.unwrap();
        migrate.close_session(&session_id).await;

        assert!(migrate.get_session(&session_id).await.is_none());
    }

    #[tokio::test]
    async fn test_stats() {
        let migrate = SessionMigrate::new();
        let session_id = Uuid::new_v4();
        let mut state = SessionState::new(
            session_id,
            "user".to_string(),
            "db".to_string(),
            NodeId::new(),
        );

        state.add_prepared_statement(PreparedStatementInfo {
            name: "ps1".to_string(),
            query: "SELECT 1".to_string(),
            param_types: vec![],
            created_at: chrono::Utc::now(),
        });

        migrate.register_session(state).await.unwrap();

        let stats = migrate.stats().await;
        assert_eq!(stats.active_sessions, 1);
        assert_eq!(stats.total_prepared_statements, 1);
    }
}
