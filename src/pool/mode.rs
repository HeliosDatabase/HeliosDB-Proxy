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

/// The next SQL word of `sql` (ASCII letters, digits and `_`) behind any
/// whitespace PostgreSQL's lexer skips (space, tab, newline, vertical tab,
/// form feed, carriage return), and what follows the word. `None` when
/// something else comes first, or nothing.
fn next_word(sql: &[u8]) -> Option<(&[u8], &[u8])> {
    let start = sql
        .iter()
        .position(|b| !matches!(b, b' ' | b'\t'..=b'\r'))?;
    let sql = &sql[start..];
    let end = sql
        .iter()
        .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))
        .unwrap_or(sql.len());
    (end > 0).then(|| sql.split_at(end))
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
    /// The transaction boundary a statement is, read from its leading
    /// keywords (case-insensitive, separated by any ASCII whitespace):
    ///
    /// - `BEGIN ...`, `START TRANSACTION ...`: [`Self::Begin`]
    /// - `COMMIT`, `END`, with an optional `WORK` / `TRANSACTION`:
    ///   [`Self::Commit`]
    /// - `ROLLBACK`, `ABORT`, likewise: [`Self::Rollback`];
    ///   `ROLLBACK ... TO ...`: [`Self::RollbackToSavepoint`]
    /// - any of those four ending the transaction `AND CHAIN`:
    ///   [`Self::Begin`] (PostgreSQL starts the next transaction at once,
    ///   so the session never leaves the transaction block)
    /// - `SAVEPOINT`, `RELEASE`: [`Self::Savepoint`],
    ///   [`Self::ReleaseSavepoint`]
    ///
    /// Everything else is [`Self::Statement`], including a statement behind
    /// a leading comment. Allocation-free, and it reads only the first few
    /// words: a multi-megabyte `INSERT` costs what a short one does.
    pub fn detect(sql: &str) -> Self {
        let Some((word, rest)) = next_word(sql.as_bytes()) else {
            return TransactionEvent::Statement;
        };
        let is = |keyword: &str| word.eq_ignore_ascii_case(keyword.as_bytes());
        // One keyword comparison per statement: the word's length selects
        // the candidates before any of them is compared.
        match word.len() {
            5 if is("BEGIN") => TransactionEvent::Begin,
            5 if is("START") => match next_word(rest) {
                Some((next, _)) if next.eq_ignore_ascii_case(b"TRANSACTION") => {
                    TransactionEvent::Begin
                }
                _ => TransactionEvent::Statement,
            },
            6 if is("COMMIT") => Self::end_keyword(rest, TransactionEvent::Commit),
            3 if is("END") => Self::end_keyword(rest, TransactionEvent::Commit),
            8 if is("ROLLBACK") => Self::end_keyword(rest, TransactionEvent::Rollback),
            5 if is("ABORT") => Self::end_keyword(rest, TransactionEvent::Rollback),
            9 if is("SAVEPOINT") => TransactionEvent::Savepoint,
            7 if is("RELEASE") => TransactionEvent::ReleaseSavepoint,
            _ => TransactionEvent::Statement,
        }
    }

    /// [`Self::after_end_keyword`], with the bare keyword (`COMMIT`, the
    /// form drivers send) decided without the call.
    #[inline]
    fn end_keyword(rest: &[u8], end: Self) -> Self {
        if rest.is_empty() {
            return end;
        }
        Self::after_end_keyword(rest, end)
    }

    /// What `COMMIT` / `END` / `ROLLBACK` / `ABORT` followed by `rest` is:
    /// `end` itself, unless the transaction is chained (`AND CHAIN`) or the
    /// rollback names a savepoint (`TO`).
    fn after_end_keyword(rest: &[u8], end: Self) -> Self {
        let mut next = next_word(rest);
        if let Some((word, rest)) = next {
            if word.eq_ignore_ascii_case(b"WORK") || word.eq_ignore_ascii_case(b"TRANSACTION") {
                next = next_word(rest);
            }
        }
        match next {
            Some((word, rest)) if word.eq_ignore_ascii_case(b"AND") => match next_word(rest) {
                Some((chain, _)) if chain.eq_ignore_ascii_case(b"CHAIN") => TransactionEvent::Begin,
                _ => end,
            },
            Some((word, _))
                if end == TransactionEvent::Rollback && word.eq_ignore_ascii_case(b"TO") =>
            {
                TransactionEvent::RollbackToSavepoint
            }
            _ => end,
        }
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

    /// `detect` before it read words: prefix tests on the trimmed text.
    fn detect_by_prefix(sql: &str) -> TransactionEvent {
        use crate::protocol::{contains_ci, starts_with_ci};
        let trimmed = sql.trim();
        if starts_with_ci(trimmed, "BEGIN") {
            return TransactionEvent::Begin;
        }
        if starts_with_ci(trimmed, "START ") && contains_ci(trimmed, "TRANSACTION") {
            return TransactionEvent::Begin;
        }
        if starts_with_ci(trimmed, "COMMIT") || starts_with_ci(trimmed, "END") {
            return TransactionEvent::Commit;
        }
        if starts_with_ci(trimmed, "ROLLBACK") {
            if contains_ci(trimmed, " TO ") {
                return TransactionEvent::RollbackToSavepoint;
            }
            return TransactionEvent::Rollback;
        }
        if starts_with_ci(trimmed, "SAVEPOINT") {
            return TransactionEvent::Savepoint;
        }
        if starts_with_ci(trimmed, "RELEASE") {
            return TransactionEvent::ReleaseSavepoint;
        }
        TransactionEvent::Statement
    }

    /// On statements written with single spaces the word reader agrees with
    /// the prefix tests it replaced.
    #[test]
    fn detect_agrees_with_the_prefix_tests_on_plain_statements() {
        let statements = [
            "BEGIN",
            "BEGIN WORK",
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "START TRANSACTION",
            "START TRANSACTION READ ONLY",
            "COMMIT",
            "COMMIT WORK",
            "COMMIT PREPARED 'gid'",
            "END",
            "END TRANSACTION",
            "ROLLBACK",
            "ROLLBACK WORK",
            "ROLLBACK PREPARED 'gid'",
            "ROLLBACK TO sp1",
            "ROLLBACK TO SAVEPOINT sp1",
            "ROLLBACK WORK TO SAVEPOINT sp1",
            "SAVEPOINT sp1",
            "RELEASE sp1",
            "RELEASE SAVEPOINT sp1",
            "SELECT * FROM users WHERE id = 1",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1 WHERE b = 2",
            "SET search_path TO app",
            "",
        ];
        let mut checked = 0;
        for statement in statements {
            for cased in [
                statement.to_string(),
                statement.to_lowercase(),
                statement.to_uppercase(),
            ] {
                for (lead, tail) in [
                    ("", ""),
                    ("  ", ""),
                    ("", ";"),
                    ("\n", " ;\n"),
                    ("\t", "  "),
                ] {
                    let sql = format!("{lead}{cased}{tail}");
                    assert_eq!(
                        TransactionEvent::detect(&sql),
                        detect_by_prefix(&sql),
                        "{sql:?}"
                    );
                    checked += 1;
                }
            }
        }
        assert_eq!(checked, statements.len() * 3 * 5);
    }

    /// Where the prefix tests misread a statement, the word reader does not.
    #[test]
    fn detect_reads_words_not_prefixes() {
        use TransactionEvent::*;
        for (sql, event) in [
            // Any whitespace separates the keywords.
            ("START\tTRANSACTION", Begin),
            ("start\n  transaction read only", Begin),
            ("ROLLBACK\tTO\tSAVEPOINT sp1", RollbackToSavepoint),
            ("ROLLBACK\nTO sp1", RollbackToSavepoint),
            ("ROLLBACK TRANSACTION TO sp1", RollbackToSavepoint),
            // A keyword is a whole word.
            ("BEGINNING", Statement),
            ("ENDPOINT", Statement),
            ("COMMITTED", Statement),
            ("STARTLING TRANSACTION", Statement),
            ("START", Statement),
            ("START SLAVE", Statement),
            ("RELEASED", Statement),
            // `TO` elsewhere in the text names no savepoint.
            ("ROLLBACK; SET search_path TO app", Rollback),
            ("ROLLBACK PREPARED 'up TO date'", Rollback),
            // ABORT is ROLLBACK.
            ("ABORT", Rollback),
            ("abort transaction", Rollback),
            // A chained end starts the next transaction at once.
            ("COMMIT AND CHAIN", Begin),
            ("END WORK AND CHAIN", Begin),
            ("ROLLBACK AND CHAIN", Begin),
            ("ABORT TRANSACTION AND CHAIN", Begin),
            ("COMMIT AND NO CHAIN", Commit),
            ("ROLLBACK AND NO CHAIN", Rollback),
            // Nothing to read.
            ("   ", Statement),
            (";", Statement),
            ("/* c */ BEGIN", Statement),
        ] {
            assert_eq!(TransactionEvent::detect(sql), event, "{sql:?}");
        }
    }

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
        assert_eq!(PoolingMode::from_str_lossy("unknown"), PoolingMode::Session);
    }

    #[test]
    fn test_prepared_statement_mode_default() {
        assert_eq!(
            PreparedStatementMode::default(),
            PreparedStatementMode::Disable
        );
    }

    /// `START` begins a transaction exactly when its next word is
    /// `TRANSACTION`, whatever whitespace separates the two.
    #[test]
    fn start_begins_a_transaction_only_before_the_word_transaction() {
        for (sql, begins) in [
            ("START TRANSACTION", true),
            ("  start transaction read only\n", true),
            ("Start Transaction Isolation Level Serializable", true),
            ("START  TRANSACTION", true),
            ("START\tTRANSACTION", true),
            ("START\r\nTRANSACTION;", true),
            ("START WORK TRANSACTION", false),
            ("START TRANSACTIO", false),
            ("START TRANSACTIONS", false),
            ("START", false),
            ("START ", false),
            ("STARTTRANSACTION", false),
            ("START_REPLICATION SLOT s LOGICAL 0/0", false),
            ("RESTART TRANSACTION", false),
            ("SELECT 'START TRANSACTION'", false),
        ] {
            assert_eq!(
                TransactionEvent::detect(sql) == TransactionEvent::Begin,
                begins,
                "{sql:?}"
            );
        }
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
