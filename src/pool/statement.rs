//! Statement Mode Handler
//!
//! Implements statement pooling mode where connections are returned
//! to the pool after each individual statement completes.

use super::lease::{ClientId, ConnectionLease, LeaseAction};
use super::mode::{PoolingMode, TransactionEvent};
use crate::connection_pool::PooledConnection;
use crate::protocol::{contains_ci, starts_with_ci};

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
    pub fn create_lease(
        &self,
        connection: PooledConnection,
        client_id: ClientId,
    ) -> ConnectionLease {
        ConnectionLease::new(connection, PoolingMode::Statement, client_id)
    }

    /// Process a statement and determine action
    ///
    /// Returns the connection after every statement outside of explicit transaction.
    pub fn on_statement_complete(&self, lease: &mut ConnectionLease, sql: &str) -> LeaseAction {
        let event = TransactionEvent::detect(sql);

        // Update lease transaction state
        let _action = lease.on_statement_complete(sql);

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
    ///
    /// Runs per statement on the pool-mode path, so it must not allocate:
    /// ASCII case-insensitive prefix/substring tests on the trimmed input
    /// replace the uppercased copy. Semantics are unchanged.
    pub fn is_safe_query(&self, sql: &str) -> bool {
        let s = Self::trimmed(sql);
        Self::leading_keyword(s).is_none() && !Self::creates_temp(s) && !Self::is_session_set(s)
    }

    /// `sql.trim()`. A statement that starts and ends on a visible ASCII
    /// byte, the usual case, has nothing to trim and skips the Unicode
    /// whitespace scan from both ends.
    #[inline]
    fn trimmed(sql: &str) -> &str {
        match sql.as_bytes() {
            [first, .., last] if first.is_ascii_graphic() && last.is_ascii_graphic() => sql,
            _ => sql.trim(),
        }
    }

    /// The session-bound command the trimmed statement starts with, if any.
    /// A prefix test like the nine it replaces (`LISTENER` still counts as
    /// `LISTEN`), entered by the first byte so a statement that starts with
    /// none of them is compared against at most two keywords.
    #[inline]
    fn leading_keyword(trimmed: &str) -> Option<LeadingKeyword> {
        let s = trimmed.as_bytes();
        match s.first()? & !ASCII_CASE_BIT {
            b'L' if starts_with_letters(s, b"LISTEN") => Some(LeadingKeyword::Notification),
            b'U' if starts_with_letters(s, b"UNLISTEN") => Some(LeadingKeyword::Notification),
            b'P' if starts_with_letters(s, b"PREPARE") => Some(LeadingKeyword::Prepared),
            b'E' if starts_with_letters(s, b"EXECUTE") => Some(LeadingKeyword::Prepared),
            b'D' if starts_with_letters(s, b"DEALLOCATE") => Some(LeadingKeyword::Prepared),
            b'D' if starts_with_letters(s, b"DECLARE") => Some(LeadingKeyword::Cursor),
            b'F' if starts_with_letters(s, b"FETCH") => Some(LeadingKeyword::Cursor),
            b'C' if starts_with_letters(s, b"CLOSE") => Some(LeadingKeyword::Cursor),
            b'M' if starts_with_letters(s, b"MOVE") => Some(LeadingKeyword::CursorMove),
            _ => None,
        }
    }

    /// `CREATE TEMP ...` / `CREATE TEMPORARY ...` anywhere in the statement
    /// (multi-statement strings included). One scan: `CREATE TEMPORARY`
    /// starts with `CREATE TEMP`, so the second substring test was redundant.
    #[inline]
    fn creates_temp(trimmed: &str) -> bool {
        contains_ci(trimmed, "CREATE TEMP")
    }

    /// `SET ...` that is neither `SET LOCAL` nor `SET TRANSACTION` — i.e. a
    /// session-scoped setting that would not survive statement pooling.
    #[inline]
    fn is_session_set(trimmed: &str) -> bool {
        starts_with_ci(trimmed, "SET ")
            && !starts_with_ci(trimmed, "SET LOCAL")
            && !starts_with_ci(trimmed, "SET TRANSACTION")
    }

    /// Get warning if query is unsafe for statement mode
    pub fn get_query_warning(&self, sql: &str) -> Option<&'static str> {
        let s = Self::trimmed(sql);

        match Self::leading_keyword(s) {
            Some(LeadingKeyword::Notification) => {
                return Some(
                    "LISTEN/UNLISTEN not supported in statement mode - notifications will be lost",
                );
            }
            Some(LeadingKeyword::Prepared) => {
                return Some("Prepared statements not supported in statement mode");
            }
            Some(LeadingKeyword::Cursor) => {
                return Some(
                    "Cursors not supported in statement mode outside explicit transactions",
                );
            }
            // MOVE is unsafe but has no warning of its own.
            Some(LeadingKeyword::CursorMove) | None => {}
        }

        if Self::creates_temp(s) {
            return Some("Temporary tables may not persist correctly in statement mode");
        }

        if Self::is_session_set(s) {
            return Some("Session variables may not persist in statement mode - use SET LOCAL within transaction");
        }

        None
    }
}

/// The bit that separates an ASCII letter from its other case.
const ASCII_CASE_BIT: u8 = b'a' ^ b'A';

/// [`starts_with_ci`] for a keyword of upper-case ASCII letters only. A byte
/// with the case bit cleared equals an upper-case letter exactly when it is
/// that letter in either case, so each byte takes one mask and one compare
/// and a keyword of known length compiles to a few word compares.
#[inline(always)]
fn starts_with_letters(s: &[u8], keyword: &[u8]) -> bool {
    debug_assert!(keyword.iter().all(u8::is_ascii_uppercase));
    s.len() >= keyword.len()
        && s[..keyword.len()]
            .iter()
            .zip(keyword)
            .all(|(byte, letter)| byte & !ASCII_CASE_BIT == *letter)
}

/// The session-bound command a statement starts with
/// ([`StatementModeHandler::leading_keyword`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeadingKeyword {
    /// `LISTEN` / `UNLISTEN`.
    Notification,
    /// `PREPARE` / `EXECUTE` / `DEALLOCATE`.
    Prepared,
    /// `DECLARE` / `FETCH` / `CLOSE`.
    Cursor,
    /// `MOVE`.
    CursorMove,
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
    fn test_safe_query_detection_case_and_whitespace() {
        let handler = StatementModeHandler::new();
        // Mixed case + leading whitespace behave exactly like the uppercased copy did.
        assert!(!handler.is_safe_query("  listen ch"));
        assert!(!handler.is_safe_query("\nUnListen ch"));
        assert!(!handler.is_safe_query("prepare p as select 1"));
        assert!(!handler.is_safe_query("fetch 10 from c"));
        assert!(!handler.is_safe_query("move forward 1 in c"));
        assert!(!handler.is_safe_query("close c"));
        assert!(!handler
            .is_safe_query("insert into x select * from y; create temporary table z (i int)"));
        assert!(!handler.is_safe_query("set work_mem = '1GB'"));
        assert!(handler.is_safe_query("set local work_mem = '1GB'"));
        assert!(handler.is_safe_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
        // "SET" without a trailing space (e.g. SETTINGS, or bare SET) is not the
        // session-variable form and stays safe, as before.
        assert!(handler.is_safe_query("SET"));
        assert!(handler.is_safe_query("select 'ünïcode'"));
        assert!(handler.is_safe_query(""));
        assert!(handler.is_safe_query("LIS"));
        assert!(handler.get_query_warning("  listen ch").is_some());
        assert!(handler.get_query_warning("move forward 1 in c").is_none());
        assert!(handler
            .get_query_warning("select 1 /* create temp */")
            .is_some());
        assert!(!handler.is_safe_query("CREATE TEMPORARY TABLE t (i int)"));
        assert!(!handler.is_safe_query("create temp table t (i int)"));
        assert!(handler.is_safe_query("CREATE TABLE t (i int)"));
        assert!(handler.get_query_warning("SET").is_none());
        assert!(handler.get_query_warning("SEt LOCAL x = 1").is_none());
    }

    /// The scan before it dispatched on the first byte, kept as the oracle.
    fn reference(sql: &str) -> (bool, Option<&'static str>) {
        let s = sql.trim();
        let starts = |keywords: &[&str]| keywords.iter().any(|k| starts_with_ci(s, k));
        let temp = contains_ci(s, "CREATE TEMP");
        let session_set = starts_with_ci(s, "SET ")
            && !starts_with_ci(s, "SET LOCAL")
            && !starts_with_ci(s, "SET TRANSACTION");
        let safe = !starts(&[
            "LISTEN",
            "UNLISTEN",
            "PREPARE",
            "EXECUTE",
            "DEALLOCATE",
            "DECLARE",
            "FETCH",
            "CLOSE",
            "MOVE",
        ]) && !temp
            && !session_set;
        let warning = if starts(&["LISTEN", "UNLISTEN"]) {
            Some("LISTEN/UNLISTEN not supported in statement mode - notifications will be lost")
        } else if starts(&["PREPARE", "EXECUTE", "DEALLOCATE"]) {
            Some("Prepared statements not supported in statement mode")
        } else if starts(&["DECLARE", "FETCH", "CLOSE"]) {
            Some("Cursors not supported in statement mode outside explicit transactions")
        } else if temp {
            Some("Temporary tables may not persist correctly in statement mode")
        } else if session_set {
            Some("Session variables may not persist in statement mode - use SET LOCAL within transaction")
        } else {
            None
        };
        (safe, warning)
    }

    /// Every keyword, its truncations and extensions, in three casings, behind
    /// each kind of leading and trailing blank (ASCII, Unicode, none) and in
    /// front of each tail: the first-byte dispatch and the trim shortcut
    /// answer exactly as the sequential scan did.
    #[test]
    fn safety_scan_matches_the_sequential_reference() {
        let handler = StatementModeHandler::new();
        let words = [
            "LISTEN",
            "UNLISTEN",
            "PREPARE",
            "EXECUTE",
            "DEALLOCATE",
            "DECLARE",
            "FETCH",
            "CLOSE",
            "MOVE",
            "SET",
            "SET LOCAL",
            "SET TRANSACTION",
            "CREATE TEMP",
            "CREATE TEMPORARY",
            "CREATE",
            "SELECT",
            "DELETE",
            "DROP",
            "COMMIT",
            "UPDATE",
            "EXPLAIN",
            "LOCK",
            "MERGE",
            "PREPARED",
            "FETCHX",
            "",
        ];
        let edges = [
            "",
            " ",
            "\t\n",
            "\u{a0}",
            "\u{2003} ",
            "\u{e9}",
            "\u{0}",
            "-",
        ];
        let tails = [
            "",
            " x",
            " x; create temp table t (i int)",
            " x = 1",
            "x",
            "\u{e9}",
        ];
        let mut checked = 0usize;
        for word in words {
            for cut in 0..=word.len() {
                // Truncations only at the full word and one byte short of it
                // keep the corpus small without losing the boundary cases.
                if cut != word.len() && cut + 1 != word.len() && cut != 1 {
                    continue;
                }
                let stem = &word[..cut];
                let mixed: String = stem
                    .chars()
                    .enumerate()
                    .map(|(i, c)| {
                        if i % 2 == 0 {
                            c.to_ascii_lowercase()
                        } else {
                            c
                        }
                    })
                    .collect();
                for cased in [stem.to_string(), stem.to_ascii_lowercase(), mixed] {
                    for lead in edges {
                        for tail in tails {
                            for trail in edges {
                                let sql = format!("{lead}{cased}{tail}{trail}");
                                assert_eq!(
                                    (handler.is_safe_query(&sql), handler.get_query_warning(&sql)),
                                    reference(&sql),
                                    "{sql:?}"
                                );
                                checked += 1;
                            }
                        }
                    }
                }
            }
        }
        assert!(checked > 10_000, "corpus shrank to {checked}");
    }

    #[test]
    fn test_query_warnings() {
        let handler = StatementModeHandler::new();

        assert!(handler.get_query_warning("LISTEN channel").is_some());
        assert!(handler
            .get_query_warning("PREPARE stmt AS SELECT 1")
            .is_some());
        assert!(handler
            .get_query_warning("CREATE TEMP TABLE t (id int)")
            .is_some());
        assert!(handler.get_query_warning("SET work_mem = '1GB'").is_some());

        assert!(handler.get_query_warning("SELECT 1").is_none());
        assert!(handler
            .get_query_warning("SET LOCAL work_mem = '1GB'")
            .is_none());
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
