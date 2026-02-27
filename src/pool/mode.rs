//! Pooling Mode Definitions
//!
//! Defines the three connection pooling modes and related enums.

use serde::{Deserialize, Serialize};

/// Connection pooling mode
///
/// Determines when connections are returned to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PoolingMode {
    /// Session mode: 1:1 client-to-backend mapping
    ///
    /// Connection is held for the entire client session lifetime.
    /// This is the safest mode, compatible with all PostgreSQL features.
    #[default]
    Session,

    /// Transaction mode: Return connection after transaction ends
    ///
    /// Connection is returned to the pool after COMMIT or ROLLBACK.
    /// Provides good connection sharing while maintaining transaction integrity.
    /// Server-side prepared statements may need re-creation on new connections.
    Transaction,

    /// Statement mode: Return connection after each statement
    ///
    /// Most aggressive connection sharing - returns after every statement.
    /// Cannot use server-side prepared statements.
    /// Best for simple queries where maximum connection sharing is desired.
    Statement,
}

impl PoolingMode {
    /// Returns whether this mode supports server-side prepared statements
    pub fn supports_prepared_statements(&self) -> bool {
        match self {
            PoolingMode::Session => true,
            PoolingMode::Transaction => true, // With tracking/recreation
            PoolingMode::Statement => false,
        }
    }

    /// Returns a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            PoolingMode::Session => "Hold connection for entire client session",
            PoolingMode::Transaction => "Return connection after COMMIT/ROLLBACK",
            PoolingMode::Statement => "Return connection after each statement",
        }
    }

    /// Parse from string (case-insensitive)
    pub fn from_str_lossy(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "session" => PoolingMode::Session,
            "transaction" | "txn" => PoolingMode::Transaction,
            "statement" | "stmt" => PoolingMode::Statement,
            _ => PoolingMode::Session,
        }
    }
}

impl std::fmt::Display for PoolingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolingMode::Session => write!(f, "session"),
            PoolingMode::Transaction => write!(f, "transaction"),
            PoolingMode::Statement => write!(f, "statement"),
        }
    }
}

/// Prepared statement handling mode
///
/// Controls how server-side prepared statements are handled across connection switches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PreparedStatementMode {
    /// Disable prepared statements (safest for statement mode)
    ///
    /// Forces all queries to use simple query protocol.
    #[default]
    Disable,

    /// Track and recreate prepared statements
    ///
    /// Records PREPARE commands and replays them on new connections.
    /// Adds some overhead but maintains compatibility.
    Track,

    /// Use protocol-level named statements
    ///
    /// Leverages PostgreSQL extended query protocol for statement tracking.
    /// Most efficient but requires careful state management.
    Named,
}

impl PreparedStatementMode {
    /// Returns a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            PreparedStatementMode::Disable => "Disable prepared statements (safest)",
            PreparedStatementMode::Track => "Track and recreate on new connections",
            PreparedStatementMode::Named => "Use protocol-level named statements",
        }
    }

    /// Parse from string (case-insensitive)
    pub fn from_str_lossy(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "disable" | "disabled" | "off" => PreparedStatementMode::Disable,
            "track" | "tracking" => PreparedStatementMode::Track,
            "named" | "protocol" => PreparedStatementMode::Named,
            _ => PreparedStatementMode::Disable,
        }
    }
}

impl std::fmt::Display for PreparedStatementMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PreparedStatementMode::Disable => write!(f, "disable"),
            PreparedStatementMode::Track => write!(f, "track"),
            PreparedStatementMode::Named => write!(f, "named"),
        }
    }
}

/// Transaction boundary events
///
/// Used to detect when transactions begin and end for mode-aware pooling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEvent {
    /// BEGIN or START TRANSACTION
    Begin,
    /// COMMIT or END
    Commit,
    /// ROLLBACK
    Rollback,
    /// SAVEPOINT created
    Savepoint,
    /// RELEASE SAVEPOINT
    ReleaseSavepoint,
    /// ROLLBACK TO SAVEPOINT
    RollbackToSavepoint,
    /// Regular statement (not transaction control)
    Statement,
}

impl TransactionEvent {
    /// Parse SQL to detect transaction boundary
    ///
    /// # Arguments
    /// * `sql` - SQL statement to parse
    ///
    /// # Returns
    /// The detected transaction event type
    pub fn detect(sql: &str) -> Self {
        let upper = sql.trim().to_uppercase();
        let upper_ref = upper.as_str();

        // Check for transaction control commands
        if upper_ref.starts_with("BEGIN") {
            return TransactionEvent::Begin;
        }
        if upper_ref.starts_with("START TRANSACTION") || upper_ref.starts_with("START ") {
            // START could be START TRANSACTION
            if upper.contains("TRANSACTION") {
                return TransactionEvent::Begin;
            }
        }
        if upper_ref.starts_with("COMMIT") || upper_ref.starts_with("END") {
            // END is alias for COMMIT in PostgreSQL
            return TransactionEvent::Commit;
        }
        if upper_ref.starts_with("ROLLBACK") {
            // Check for ROLLBACK TO SAVEPOINT
            if upper.contains(" TO ") {
                return TransactionEvent::RollbackToSavepoint;
            }
            return TransactionEvent::Rollback;
        }
        if upper_ref.starts_with("SAVEPOINT") {
            return TransactionEvent::Savepoint;
        }
        if upper_ref.starts_with("RELEASE") {
            return TransactionEvent::ReleaseSavepoint;
        }

        TransactionEvent::Statement
    }

    /// Returns true if this event ends a transaction
    pub fn is_transaction_end(&self) -> bool {
        matches!(self, TransactionEvent::Commit | TransactionEvent::Rollback)
    }

    /// Returns true if this event starts a transaction
    pub fn is_transaction_start(&self) -> bool {
        matches!(self, TransactionEvent::Begin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pooling_mode_default() {
        assert_eq!(PoolingMode::default(), PoolingMode::Session);
    }

    #[test]
    fn test_pooling_mode_display() {
        assert_eq!(PoolingMode::Session.to_string(), "session");
        assert_eq!(PoolingMode::Transaction.to_string(), "transaction");
        assert_eq!(PoolingMode::Statement.to_string(), "statement");
    }

    #[test]
    fn test_pooling_mode_from_str() {
        assert_eq!(PoolingMode::from_str_lossy("SESSION"), PoolingMode::Session);
        assert_eq!(
            PoolingMode::from_str_lossy("transaction"),
            PoolingMode::Transaction
        );
        assert_eq!(PoolingMode::from_str_lossy("txn"), PoolingMode::Transaction);
        assert_eq!(
            PoolingMode::from_str_lossy("STATEMENT"),
            PoolingMode::Statement
        );
        assert_eq!(PoolingMode::from_str_lossy("stmt"), PoolingMode::Statement);
        assert_eq!(
            PoolingMode::from_str_lossy("unknown"),
            PoolingMode::Session
        );
    }

    #[test]
    fn test_prepared_statement_mode_default() {
        assert_eq!(
            PreparedStatementMode::default(),
            PreparedStatementMode::Disable
        );
    }

    #[test]
    fn test_transaction_event_detect() {
        assert_eq!(TransactionEvent::detect("BEGIN"), TransactionEvent::Begin);
        assert_eq!(
            TransactionEvent::detect("begin work"),
            TransactionEvent::Begin
        );
        assert_eq!(
            TransactionEvent::detect("START TRANSACTION"),
            TransactionEvent::Begin
        );
        assert_eq!(TransactionEvent::detect("COMMIT"), TransactionEvent::Commit);
        assert_eq!(TransactionEvent::detect("END"), TransactionEvent::Commit);
        assert_eq!(
            TransactionEvent::detect("ROLLBACK"),
            TransactionEvent::Rollback
        );
        assert_eq!(
            TransactionEvent::detect("ROLLBACK TO SAVEPOINT sp1"),
            TransactionEvent::RollbackToSavepoint
        );
        assert_eq!(
            TransactionEvent::detect("SAVEPOINT sp1"),
            TransactionEvent::Savepoint
        );
        assert_eq!(
            TransactionEvent::detect("RELEASE SAVEPOINT sp1"),
            TransactionEvent::ReleaseSavepoint
        );
        assert_eq!(
            TransactionEvent::detect("SELECT * FROM users"),
            TransactionEvent::Statement
        );
    }

    #[test]
    fn test_transaction_event_predicates() {
        assert!(TransactionEvent::Begin.is_transaction_start());
        assert!(!TransactionEvent::Begin.is_transaction_end());

        assert!(TransactionEvent::Commit.is_transaction_end());
        assert!(!TransactionEvent::Commit.is_transaction_start());

        assert!(TransactionEvent::Rollback.is_transaction_end());
        assert!(!TransactionEvent::Statement.is_transaction_end());
    }
}
