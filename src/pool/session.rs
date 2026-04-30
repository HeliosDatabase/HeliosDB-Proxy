//! Session Mode Handler
//!
//! Implements session pooling mode where connections are held
//! for the entire client session lifetime.

use super::lease::{ClientId, ConnectionLease, LeaseAction};
use super::mode::PoolingMode;
use crate::connection_pool::PooledConnection;

/// Session mode handler
///
/// In session mode, a connection is held for the entire client session.
/// This provides 1:1 client-to-backend mapping, which is:
/// - Safest for all PostgreSQL features
/// - Compatible with server-side prepared statements
/// - Compatible with LISTEN/NOTIFY
/// - Compatible with temp tables and session variables
///
/// The downside is less connection sharing between clients.
pub struct SessionModeHandler {
    /// Whether to track prepared statements
    track_prepared_statements: bool,
}

impl Default for SessionModeHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionModeHandler {
    /// Create a new session mode handler
    pub fn new() -> Self {
        Self {
            track_prepared_statements: false,
        }
    }

    /// Create with prepared statement tracking enabled
    pub fn with_prepared_tracking() -> Self {
        Self {
            track_prepared_statements: true,
        }
    }

    /// Create a lease for this mode
    pub fn create_lease(&self, connection: PooledConnection, client_id: ClientId) -> ConnectionLease {
        ConnectionLease::new(connection, PoolingMode::Session, client_id)
    }

    /// Process a statement and determine action
    ///
    /// Session mode always holds the connection.
    pub fn on_statement_complete(&self, _lease: &mut ConnectionLease, _sql: &str) -> LeaseAction {
        LeaseAction::Hold
    }

    /// Process transaction end
    ///
    /// Session mode always holds the connection.
    pub fn on_transaction_end(&self, _lease: &mut ConnectionLease) -> LeaseAction {
        LeaseAction::Hold
    }

    /// Check if connection should be released
    ///
    /// Session mode never releases until client disconnects.
    pub fn should_release(&self, _lease: &ConnectionLease) -> bool {
        false
    }

    /// Called when client disconnects
    pub fn on_client_disconnect(&self, _lease: ConnectionLease) -> LeaseAction {
        // When client disconnects, reset and return to pool
        LeaseAction::Reset
    }

    /// Get the pooling mode
    pub fn mode(&self) -> PoolingMode {
        PoolingMode::Session
    }

    /// Check if prepared statement tracking is enabled
    pub fn tracks_prepared_statements(&self) -> bool {
        self.track_prepared_statements
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
    fn test_session_mode_always_holds() {
        let handler = SessionModeHandler::new();
        let conn = create_test_connection();
        let mut lease = handler.create_lease(conn, ClientId::new());

        // Any statement should hold
        assert_eq!(
            handler.on_statement_complete(&mut lease, "SELECT 1"),
            LeaseAction::Hold
        );

        // Transaction statements should hold
        assert_eq!(
            handler.on_statement_complete(&mut lease, "BEGIN"),
            LeaseAction::Hold
        );
        assert_eq!(
            handler.on_statement_complete(&mut lease, "COMMIT"),
            LeaseAction::Hold
        );

        // Transaction end should hold
        assert_eq!(handler.on_transaction_end(&mut lease), LeaseAction::Hold);
    }

    #[test]
    fn test_session_mode_never_releases() {
        let handler = SessionModeHandler::new();
        let conn = create_test_connection();
        let lease = handler.create_lease(conn, ClientId::new());

        assert!(!handler.should_release(&lease));
    }

    #[test]
    fn test_session_mode_disconnect_resets() {
        let handler = SessionModeHandler::new();
        let conn = create_test_connection();
        let lease = handler.create_lease(conn, ClientId::new());

        assert_eq!(handler.on_client_disconnect(lease), LeaseAction::Reset);
    }

    #[test]
    fn test_mode() {
        let handler = SessionModeHandler::new();
        assert_eq!(handler.mode(), PoolingMode::Session);
    }
}
