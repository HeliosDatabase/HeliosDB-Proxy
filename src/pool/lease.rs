//! Connection Lease Management
//!
//! Tracks connection state and determines when to release connections based on pooling mode.

use super::mode::{PoolingMode, TransactionEvent};
use crate::connection_pool::PooledConnection;
use std::time::Instant;
use uuid::Uuid;

/// A leased connection with mode-aware lifecycle management
pub struct ConnectionLease {
    /// The underlying pooled connection
    connection: PooledConnection,
    /// Pooling mode for this lease
    mode: PoolingMode,
    /// Whether currently in a transaction
    in_transaction: bool,
    /// When the lease was acquired
    leased_at: Instant,
    /// Number of statements executed on this lease
    statements_executed: u64,
    /// Client identifier
    client_id: ClientId,
    /// Current transaction nesting level (for savepoints)
    transaction_depth: u32,
}

/// Client identifier for tracking leases
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub Uuid);

impl ClientId {
    /// Create a new random client ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

/// Action to take after processing a statement
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseAction {
    /// Keep the connection leased
    Hold,
    /// Return connection to pool (no reset needed)
    Release,
    /// Reset connection state then return to pool
    Reset,
    /// Connection is invalid, close it
    Close,
}

impl ConnectionLease {
    /// Create a new connection lease
    pub fn new(connection: PooledConnection, mode: PoolingMode, client_id: ClientId) -> Self {
        Self {
            connection,
            mode,
            in_transaction: false,
            leased_at: Instant::now(),
            statements_executed: 0,
            client_id,
            transaction_depth: 0,
        }
    }

    /// Get the underlying connection (immutable)
    pub fn connection(&self) -> &PooledConnection {
        &self.connection
    }

    /// Get the underlying connection (mutable)
    pub fn connection_mut(&mut self) -> &mut PooledConnection {
        &mut self.connection
    }

    /// Take ownership of the underlying connection
    pub fn into_connection(self) -> PooledConnection {
        self.connection
    }

    /// Get the pooling mode
    pub fn mode(&self) -> PoolingMode {
        self.mode
    }

    /// Get the client ID
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Check if currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Get the number of statements executed
    pub fn statements_executed(&self) -> u64 {
        self.statements_executed
    }

    /// Get how long this lease has been held
    pub fn lease_duration(&self) -> std::time::Duration {
        self.leased_at.elapsed()
    }

    /// Process a statement and determine if connection should be released
    ///
    /// # Arguments
    /// * `sql` - The SQL statement that was executed
    ///
    /// # Returns
    /// The action to take with this connection
    pub fn on_statement_complete(&mut self, sql: &str) -> LeaseAction {
        self.statements_executed += 1;

        // Detect transaction boundaries
        let event = TransactionEvent::detect(sql);

        // Update transaction state
        match event {
            TransactionEvent::Begin => {
                self.in_transaction = true;
                self.transaction_depth = 1;
            }
            TransactionEvent::Savepoint => {
                if self.in_transaction {
                    self.transaction_depth += 1;
                }
            }
            TransactionEvent::ReleaseSavepoint | TransactionEvent::RollbackToSavepoint => {
                if self.transaction_depth > 1 {
                    self.transaction_depth -= 1;
                }
            }
            TransactionEvent::Commit | TransactionEvent::Rollback => {
                self.in_transaction = false;
                self.transaction_depth = 0;
            }
            TransactionEvent::Statement => {
                // No transaction state change
            }
        }

        // Determine action based on mode
        self.determine_action(event)
    }

    /// Called when transaction ends (from backend ReadyForQuery status)
    ///
    /// This is a more reliable way to detect transaction end than parsing SQL.
    pub fn on_transaction_end(&mut self) -> LeaseAction {
        self.in_transaction = false;
        self.transaction_depth = 0;

        match self.mode {
            PoolingMode::Session => LeaseAction::Hold,
            PoolingMode::Transaction | PoolingMode::Statement => LeaseAction::Reset,
        }
    }

    /// Update transaction state from backend ReadyForQuery status
    ///
    /// # Arguments
    /// * `in_transaction` - Whether backend reports being in a transaction
    pub fn update_transaction_state(&mut self, in_transaction: bool) {
        if !in_transaction && self.in_transaction {
            // Transaction ended
            self.in_transaction = false;
            self.transaction_depth = 0;
        } else if in_transaction && !self.in_transaction {
            // Transaction started (implicit)
            self.in_transaction = true;
            self.transaction_depth = 1;
        }
    }

    /// Check if connection should be released based on current state
    pub fn should_release(&self) -> bool {
        match self.mode {
            PoolingMode::Session => false,
            PoolingMode::Transaction => !self.in_transaction,
            PoolingMode::Statement => !self.in_transaction,
        }
    }

    /// Determine the lease action based on mode and transaction event
    fn determine_action(&self, event: TransactionEvent) -> LeaseAction {
        match self.mode {
            PoolingMode::Session => {
                // Session mode never releases until client disconnects
                LeaseAction::Hold
            }
            PoolingMode::Transaction => {
                // Transaction mode releases after transaction ends
                if event.is_transaction_end() && self.transaction_depth == 0 {
                    LeaseAction::Reset
                } else {
                    LeaseAction::Hold
                }
            }
            PoolingMode::Statement => {
                // Statement mode releases after every statement outside transaction
                if self.in_transaction {
                    LeaseAction::Hold
                } else {
                    LeaseAction::Reset
                }
            }
        }
    }
}

impl std::fmt::Debug for ConnectionLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionLease")
            .field("connection_id", &self.connection.id)
            .field("mode", &self.mode)
            .field("in_transaction", &self.in_transaction)
            .field("statements_executed", &self.statements_executed)
            .field("client_id", &self.client_id)
            .field("transaction_depth", &self.transaction_depth)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_pool::ConnectionState;
    use crate::NodeId;

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
    fn test_session_mode_never_releases() {
        let conn = create_test_connection();
        let mut lease = ConnectionLease::new(conn, PoolingMode::Session, ClientId::new());

        // Statement outside transaction
        assert_eq!(
            lease.on_statement_complete("SELECT 1"),
            LeaseAction::Hold
        );

        // Transaction
        assert_eq!(lease.on_statement_complete("BEGIN"), LeaseAction::Hold);
        assert_eq!(
            lease.on_statement_complete("SELECT * FROM users"),
            LeaseAction::Hold
        );
        assert_eq!(lease.on_statement_complete("COMMIT"), LeaseAction::Hold);
    }

    #[test]
    fn test_transaction_mode_releases_on_commit() {
        let conn = create_test_connection();
        let mut lease = ConnectionLease::new(conn, PoolingMode::Transaction, ClientId::new());

        // Transaction
        assert_eq!(lease.on_statement_complete("BEGIN"), LeaseAction::Hold);
        assert!(lease.in_transaction());

        assert_eq!(
            lease.on_statement_complete("INSERT INTO users VALUES (1)"),
            LeaseAction::Hold
        );

        // COMMIT should release
        assert_eq!(lease.on_statement_complete("COMMIT"), LeaseAction::Reset);
        assert!(!lease.in_transaction());
    }

    #[test]
    fn test_transaction_mode_releases_on_rollback() {
        let conn = create_test_connection();
        let mut lease = ConnectionLease::new(conn, PoolingMode::Transaction, ClientId::new());

        lease.on_statement_complete("BEGIN");
        lease.on_statement_complete("INSERT INTO users VALUES (1)");

        // ROLLBACK should release
        assert_eq!(lease.on_statement_complete("ROLLBACK"), LeaseAction::Reset);
        assert!(!lease.in_transaction());
    }

    #[test]
    fn test_statement_mode_releases_per_statement() {
        let conn = create_test_connection();
        let mut lease = ConnectionLease::new(conn, PoolingMode::Statement, ClientId::new());

        // Statement outside transaction should release
        assert_eq!(lease.on_statement_complete("SELECT 1"), LeaseAction::Reset);

        // But inside transaction, hold
        let conn2 = create_test_connection();
        let mut lease2 = ConnectionLease::new(conn2, PoolingMode::Statement, ClientId::new());
        assert_eq!(lease2.on_statement_complete("BEGIN"), LeaseAction::Hold);
        assert_eq!(
            lease2.on_statement_complete("SELECT * FROM users"),
            LeaseAction::Hold
        );
        assert_eq!(lease2.on_statement_complete("COMMIT"), LeaseAction::Reset);
    }

    #[test]
    fn test_savepoint_depth() {
        let conn = create_test_connection();
        let mut lease = ConnectionLease::new(conn, PoolingMode::Transaction, ClientId::new());

        lease.on_statement_complete("BEGIN");
        assert_eq!(lease.transaction_depth, 1);

        lease.on_statement_complete("SAVEPOINT sp1");
        assert_eq!(lease.transaction_depth, 2);

        lease.on_statement_complete("SAVEPOINT sp2");
        assert_eq!(lease.transaction_depth, 3);

        lease.on_statement_complete("RELEASE SAVEPOINT sp2");
        assert_eq!(lease.transaction_depth, 2);

        lease.on_statement_complete("COMMIT");
        assert_eq!(lease.transaction_depth, 0);
        assert!(!lease.in_transaction());
    }

    #[test]
    fn test_should_release_session_mode() {
        let conn = create_test_connection();
        // Session mode never releases
        let lease = ConnectionLease::new(conn, PoolingMode::Session, ClientId::new());
        assert!(!lease.should_release());
    }

    #[test]
    fn test_should_release_transaction_mode() {
        let conn = create_test_connection();
        // Transaction mode releases when not in transaction
        let lease = ConnectionLease::new(conn, PoolingMode::Transaction, ClientId::new());
        assert!(lease.should_release());
    }

    #[test]
    fn test_should_release_statement_mode() {
        let conn = create_test_connection();
        // Statement mode releases when not in transaction
        let lease = ConnectionLease::new(conn, PoolingMode::Statement, ClientId::new());
        assert!(lease.should_release());
    }

    #[test]
    fn test_statements_executed_counter() {
        let conn = create_test_connection();
        let mut lease = ConnectionLease::new(conn, PoolingMode::Session, ClientId::new());

        assert_eq!(lease.statements_executed(), 0);

        lease.on_statement_complete("SELECT 1");
        assert_eq!(lease.statements_executed(), 1);

        lease.on_statement_complete("SELECT 2");
        assert_eq!(lease.statements_executed(), 2);
    }
}
