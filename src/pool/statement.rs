//! Statement Mode Handler
//!
//! Implements statement pooling mode where connections are returned
//! to the pool after each individual statement completes.

use super::lease::{ClientId, ConnectionLease, LeaseAction};
use super::mode::{PoolingMode, TransactionEvent};
use crate::connection_pool::PooledConnection;

/// Statement mode handler
///
/// In statement mode, connections are returned to the pool after every statement
/// outside of an explicit transaction. This provides maximum connection sharing
/// but with significant limitations.
///
/// Benefits:
/// - Maximum connection sharing
/// - Best for high-volume simple queries
/// - Good for read-heavy workloads with many clients
///
/// Limitations:
/// - CANNOT use server-side prepared statements (Parse/Bind/Execute)
/// - CANNOT use LISTEN/NOTIFY
/// - CANNOT rely on session variables
/// - CANNOT use temp tables effectively
/// - Must use simple query protocol
///
/// Use cases:
/// - Connection poolers like PgBouncer in statement mode
/// - High-throughput REST APIs with simple queries
/// - Serverless environments with many concurrent clients
pub struct StatementModeHandler {
    /// Whether autocommit is enabled (single statements are transactions)
    autocommit: bool,
}

impl Default for StatementModeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl StatementModeHandler {
    /// Create a new statement mode handler
    pub fn new() -> Self {
        Self { autocommit: true }
    }

    /// Create with specific autocommit setting
    pub fn with_autocommit(autocommit: bool) -> Self {
        Self { autocommit }
    }

    /// Create a lease for this mode
    pub fn create_lease(&self, connection: PooledConnection, client_id: ClientId) -> ConnectionLease {
        ConnectionLease::new(connection, PoolingMode::Statement, client_id)
    }

    /// Process a statement and determine action
    ///
    /// Returns the connection after every statement outside of explicit transaction.
    pub fn on_statement_complete(&self, lease: &mut ConnectionLease, sql: &str) -> LeaseAction {
        let event = TransactionEvent::detect(sql);

        // Update lease transaction state
        let action = lease.on_statement_complete(sql);

        // Override for statement mode specifics
        match event {
            TransactionEvent::Begin => {
                // Explicit transaction - hold
                LeaseAction::Hold
            }
            TransactionEvent::Commit | TransactionEvent::Rollback => {
                // Transaction ended - reset and release
                LeaseAction::Reset
            }
            _ => {
                // In explicit transaction, hold
                if lease.in_transaction() {
                    LeaseAction::Hold
                } else {
                    // Single statement, release immediately
                    LeaseAction::Reset
                }
            }
        }
    }

    /// Process transaction end signal from backend
    pub fn on_transaction_end(&self, lease: &mut ConnectionLease) -> LeaseAction {
        lease.on_transaction_end()
    }

    /// Check if connection should be released
    pub fn should_release(&self, lease: &ConnectionLease) -> bool {
        !lease.in_transaction()
    }

    /// Called when client disconnects
    pub fn on_client_disconnect(&self, _lease: ConnectionLease) -> LeaseAction {
        LeaseAction::Reset
    }

    /// Get the pooling mode
    pub fn mode(&self) -> PoolingMode {
        PoolingMode::Statement
    }

    /// Check if autocommit is enabled
    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Prepared statements are not supported in statement mode
    pub fn tracks_prepared_statements(&self) -> bool {
        false
    }

    /// Check if a query is safe for statement mode
    ///
    /// Returns false for queries that require session state.
    pub fn is_safe_query(&self, sql: &str) -> bool {
        let upper = sql.trim().to_uppercase();

        // These are not safe for statement mode
        if upper.starts_with("LISTEN")
            || upper.starts_with("UNLISTEN")
            || upper.starts_with("PREPARE")
            || upper.starts_with("EXECUTE")
            || upper.starts_with("DEALLOCATE")
            || upper.starts_with("DECLARE")
            || upper.starts_with("FETCH")
            || upper.starts_with("CLOSE")
            || upper.starts_with("MOVE")
            || upper.contains("CREATE TEMP")
            || upper.contains("CREATE TEMPORARY")
        {
            return false;
        }

        // SET commands that affect session state
        if upper.starts_with("SET ")
            && !upper.starts_with("SET LOCAL")
            && !upper.starts_with("SET TRANSACTION")
        {
            return false;
        }

        true
    }

    /// Get warning if query is unsafe for statement mode
    pub fn get_query_warning(&self, sql: &str) -> Option<&'static str> {
        let upper = sql.trim().to_uppercase();

        if upper.starts_with("LISTEN") || upper.starts_with("UNLISTEN") {
            return Some("LISTEN/UNLISTEN not supported in statement mode - notifications will be lost");
        }

        if upper.starts_with("PREPARE") || upper.starts_with("EXECUTE") || upper.starts_with("DEALLOCATE") {
            return Some("Prepared statements not supported in statement mode");
        }

        if upper.starts_with("DECLARE") || upper.starts_with("FETCH") || upper.starts_with("CLOSE") {
            return Some("Cursors not supported in statement mode outside explicit transactions");
        }

        if upper.contains("CREATE TEMP") || upper.contains("CREATE TEMPORARY") {
            return Some("Temporary tables may not persist correctly in statement mode");
        }

        if upper.starts_with("SET ") && !upper.starts_with("SET LOCAL") && !upper.starts_with("SET TRANSACTION") {
            return Some("Session variables may not persist in statement mode - use SET LOCAL within transaction");
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_pool::ConnectionState;
    use crate::NodeId;
    use uuid::Uuid;

    fn create_test_connection() -> PooledConnection {
        PooledConnection {
            id: Uuid::new_v4(),
            node_id: NodeId::new(),
            created_at: chrono::Utc::now(),
            last_used: chrono::Utc::now(),
            state: ConnectionState::InUse,
            use_count: 1,
            permit: None,
            client: None,
        }
    }

    #[test]
    fn test_statement_mode_releases_per_statement() {
        let handler = StatementModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        // Single statement should release
        assert_eq!(
            handler.on_statement_complete(&mut lease, "SELECT 1"),
            LeaseAction::Reset
        );
    }

    #[test]
    fn test_statement_mode_holds_during_transaction() {
        let handler = StatementModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        // BEGIN should hold
        assert_eq!(
            handler.on_statement_complete(&mut lease, "BEGIN"),
            LeaseAction::Hold
        );

        // Statements in transaction should hold
        assert_eq!(
            handler.on_statement_complete(&mut lease, "SELECT 1"),
            LeaseAction::Hold
        );
        assert_eq!(
            handler.on_statement_complete(&mut lease, "INSERT INTO t VALUES (1)"),
            LeaseAction::Hold
        );

        // COMMIT should release
        assert_eq!(
            handler.on_statement_complete(&mut lease, "COMMIT"),
            LeaseAction::Reset
        );
    }

    #[test]
    fn test_should_release() {
        let handler = StatementModeHandler::new();
        let conn = create_test_connection();
        let lease = handler.create_lease(conn, ClientId::new());

        // Not in transaction, should release
        assert!(handler.should_release(&lease));
    }

    #[test]
    fn test_safe_query_detection() {
        let handler = StatementModeHandler::new();

        // Safe queries
        assert!(handler.is_safe_query("SELECT * FROM users"));
        assert!(handler.is_safe_query("INSERT INTO users VALUES (1)"));
        assert!(handler.is_safe_query("UPDATE users SET name = 'foo'"));
        assert!(handler.is_safe_query("DELETE FROM users WHERE id = 1"));
        assert!(handler.is_safe_query("SET LOCAL work_mem = '1GB'"));

        // Unsafe queries
        assert!(!handler.is_safe_query("LISTEN channel"));
        assert!(!handler.is_safe_query("PREPARE stmt AS SELECT 1"));
        assert!(!handler.is_safe_query("EXECUTE stmt"));
        assert!(!handler.is_safe_query("DECLARE cursor CURSOR FOR SELECT 1"));
        assert!(!handler.is_safe_query("CREATE TEMP TABLE t (id int)"));
        assert!(!handler.is_safe_query("SET work_mem = '1GB'"));
    }

    #[test]
    fn test_query_warnings() {
        let handler = StatementModeHandler::new();

        assert!(handler.get_query_warning("LISTEN channel").is_some());
        assert!(handler.get_query_warning("PREPARE stmt AS SELECT 1").is_some());
        assert!(handler.get_query_warning("CREATE TEMP TABLE t (id int)").is_some());
        assert!(handler.get_query_warning("SET work_mem = '1GB'").is_some());

        assert!(handler.get_query_warning("SELECT 1").is_none());
        assert!(handler.get_query_warning("SET LOCAL work_mem = '1GB'").is_none());
    }

    #[test]
    fn test_mode() {
        let handler = StatementModeHandler::new();
        assert_eq!(handler.mode(), PoolingMode::Statement);
    }

    #[test]
    fn test_no_prepared_statement_support() {
        let handler = StatementModeHandler::new();
        assert!(!handler.tracks_prepared_statements());
    }
}
