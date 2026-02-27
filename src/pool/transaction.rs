//! Transaction Mode Handler
//!
//! Implements transaction pooling mode where connections are returned
//! to the pool after each transaction completes.

use super::lease::{ClientId, ConnectionLease, LeaseAction};
use super::mode::{PoolingMode, TransactionEvent};
use super::prepared::PreparedStatementTracker;
use crate::connection_pool::PooledConnection;

/// Transaction mode handler
///
/// In transaction mode, connections are held for the duration of a transaction
/// and returned to the pool after COMMIT or ROLLBACK.
///
/// Benefits:
/// - Good balance between connection sharing and compatibility
/// - Works with most PostgreSQL features within a transaction
/// - Supports prepared statements (with tracking/recreation)
///
/// Limitations:
/// - LISTEN/NOTIFY listeners are lost between transactions
/// - Session-level settings may need to be re-applied
/// - Temp tables persist but may not be on same connection
pub struct TransactionModeHandler {
    /// Whether to track and recreate prepared statements
    track_prepared_statements: bool,
    /// Prepared statement tracker
    prepared_tracker: Option<PreparedStatementTracker>,
}

impl Default for TransactionModeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionModeHandler {
    /// Create a new transaction mode handler
    pub fn new() -> Self {
        Self {
            track_prepared_statements: false,
            prepared_tracker: None,
        }
    }

    /// Create with prepared statement tracking enabled
    pub fn with_prepared_tracking() -> Self {
        Self {
            track_prepared_statements: true,
            prepared_tracker: Some(PreparedStatementTracker::new()),
        }
    }

    /// Create a lease for this mode
    pub fn create_lease(&self, connection: PooledConnection, client_id: ClientId) -> ConnectionLease {
        ConnectionLease::new(connection, PoolingMode::Transaction, client_id)
    }

    /// Process a statement and determine action
    ///
    /// Returns the connection on COMMIT/ROLLBACK outside of savepoints.
    pub fn on_statement_complete(&mut self, lease: &mut ConnectionLease, sql: &str) -> LeaseAction {
        let event = TransactionEvent::detect(sql);

        // Track prepared statements if enabled
        if self.track_prepared_statements {
            self.track_prepared_statement(sql);
        }

        // Let the lease handle transaction state tracking
        lease.on_statement_complete(sql)
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
        PoolingMode::Transaction
    }

    /// Check if prepared statement tracking is enabled
    pub fn tracks_prepared_statements(&self) -> bool {
        self.track_prepared_statements
    }

    /// Get prepared statement tracker
    pub fn prepared_tracker(&self) -> Option<&PreparedStatementTracker> {
        self.prepared_tracker.as_ref()
    }

    /// Get mutable prepared statement tracker
    pub fn prepared_tracker_mut(&mut self) -> Option<&mut PreparedStatementTracker> {
        self.prepared_tracker.as_mut()
    }

    /// Track a prepared statement from SQL
    fn track_prepared_statement(&mut self, sql: &str) {
        let upper = sql.trim().to_uppercase();

        if let Some(tracker) = &mut self.prepared_tracker {
            if upper.starts_with("PREPARE ") {
                if let Some((name, _types, query)) = super::prepared::parse_prepare_statement(sql) {
                    tracker.register(name, query, vec![]);
                }
            } else if upper.starts_with("DEALLOCATE ") {
                if let Some(name_opt) = super::prepared::parse_deallocate_statement(sql) {
                    match name_opt {
                        Some(name) => {
                            tracker.unregister(&name);
                        }
                        None => {
                            tracker.clear();
                        }
                    }
                }
            } else if upper.starts_with("EXECUTE ") {
                // Track execution count
                let parts: Vec<&str> = sql.split_whitespace().collect();
                if parts.len() >= 2 {
                    let name = parts[1].trim_end_matches(|c| c == '(' || c == ';');
                    tracker.record_execution(name);
                }
            }
        }
    }

    /// Get SQL to recreate prepared statements on a new connection
    pub fn get_prepared_recreation_sql(&self) -> Vec<String> {
        self.prepared_tracker
            .as_ref()
            .map(|t| t.generate_prepare_sql())
            .unwrap_or_default()
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
        }
    }

    #[test]
    fn test_transaction_mode_holds_during_transaction() {
        let mut handler = TransactionModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        // BEGIN should hold
        assert_eq!(
            handler.on_statement_complete(&mut lease, "BEGIN"),
            LeaseAction::Hold
        );
        assert!(lease.in_transaction());

        // Statements in transaction should hold
        assert_eq!(
            handler.on_statement_complete(&mut lease, "INSERT INTO users VALUES (1)"),
            LeaseAction::Hold
        );
    }

    #[test]
    fn test_transaction_mode_releases_on_commit() {
        let mut handler = TransactionModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        handler.on_statement_complete(&mut lease, "BEGIN");
        handler.on_statement_complete(&mut lease, "SELECT 1");

        // COMMIT should reset/release
        assert_eq!(
            handler.on_statement_complete(&mut lease, "COMMIT"),
            LeaseAction::Reset
        );
        assert!(!lease.in_transaction());
    }

    #[test]
    fn test_transaction_mode_releases_on_rollback() {
        let mut handler = TransactionModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        handler.on_statement_complete(&mut lease, "BEGIN");
        handler.on_statement_complete(&mut lease, "SELECT 1");

        // ROLLBACK should reset/release
        assert_eq!(
            handler.on_statement_complete(&mut lease, "ROLLBACK"),
            LeaseAction::Reset
        );
        assert!(!lease.in_transaction());
    }

    #[test]
    fn test_transaction_mode_savepoint_handling() {
        let mut handler = TransactionModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        handler.on_statement_complete(&mut lease, "BEGIN");
        handler.on_statement_complete(&mut lease, "SAVEPOINT sp1");

        // ROLLBACK TO SAVEPOINT should not release
        assert_eq!(
            handler.on_statement_complete(&mut lease, "ROLLBACK TO SAVEPOINT sp1"),
            LeaseAction::Hold
        );
        assert!(lease.in_transaction());

        // Final COMMIT should release
        assert_eq!(
            handler.on_statement_complete(&mut lease, "COMMIT"),
            LeaseAction::Reset
        );
    }

    #[test]
    fn test_should_release() {
        let handler = TransactionModeHandler::new();
        let conn = create_test_connection();
        let lease = handler.create_lease(conn, ClientId::new());

        // Not in transaction, should release
        assert!(handler.should_release(&lease));
    }

    #[test]
    fn test_prepared_tracking() {
        let mut handler = TransactionModeHandler::with_prepared_tracking();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        handler.on_statement_complete(
            &mut lease,
            "PREPARE get_user AS SELECT * FROM users WHERE id = $1",
        );

        let tracker = handler.prepared_tracker().unwrap();
        assert!(tracker.contains("get_user"));

        handler.on_statement_complete(&mut lease, "DEALLOCATE get_user");
        let tracker = handler.prepared_tracker().unwrap();
        assert!(!tracker.contains("get_user"));
    }

    #[test]
    fn test_mode() {
        let handler = TransactionModeHandler::new();
        assert_eq!(handler.mode(), PoolingMode::Transaction);
    }
}
