//! The C-02 write-generation clock, shared by every cache the proxy serves
//! SQL results from (Sprinter 6a0c6c34d27a).
//!
//! Each table has a generation that every write of it moves, and one global
//! generation moves for a write whose tables cannot be read from its text.
//! A cached result stores the sum of its tables' generations (and the global
//! one) observed before its fetch and is served only while that sum is
//! unchanged; a fill whose sum moved while it was fetched is not stored (the
//! reader-refill race).
//!
//! [`TableGenerations`] is owned by `ServerState`. The query cache
//! (`[cache]`) reads and moves it through [`super::QueryCache`], and every
//! path that writes moves it: the PG-wire relay (its journal capture, the
//! unseen-write signal, `COMMIT PREPARED`), the logical-decoding source, and
//! the HTTP `/sql`, MCP and GraphQL gateways ([`GatewayWrites`]), which run
//! SQL on their own backend clients the relay never sees.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;

use super::normalizer::QueryNormalizer;
use super::QueryCache;
use crate::gateway_policy::GatewayKind;

/// What a write moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteScope {
    /// The generations of these tables.
    Tables(Vec<String>),
    /// The global generation: the statement's tables are unknown.
    All,
}

/// Per-table write generations plus the global one (C-02).
#[derive(Debug, Default)]
pub struct TableGenerations {
    /// Bumped by every write of the table.
    tables: DashMap<String, u64>,
    /// Bumped by every write whose table set is unknown (DDL,
    /// `EXECUTE`/`CALL`/`DO`, `COPY`, unparsable writes): part of every sum.
    global: AtomicU64,
    /// Reads the tables of a write statement ([`Self::write_tables`]).
    normalizer: QueryNormalizer,
}

impl TableGenerations {
    /// A clock with every generation at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Move the write generations of `tables`. Synchronous and cheap: from
    /// here on no cache serves, and no in-flight fetch stores, a result that
    /// read these tables before this point.
    pub fn mark_written(&self, tables: &[String]) {
        for table in tables {
            // Runs before the write's ReadyForQuery: a known table (the common
            // case) is bumped in place without allocating its name again.
            if let Some(mut g) = self.tables.get_mut(table.as_str()) {
                *g += 1;
            } else {
                *self.tables.entry(table.clone()).or_insert(0) += 1;
            }
        }
    }

    /// Move the global generation, for a write whose table set is unknown:
    /// every cached result stops being served.
    pub fn invalidate_all(&self) {
        self.global.fetch_add(1, Ordering::Relaxed);
    }

    /// Sum of the global generation and the generations of `tables`.
    /// Generations only grow, so the sum changes whenever any of them does.
    pub fn generation_of(&self, tables: &[String]) -> u64 {
        let mut sum = self.global.load(Ordering::Relaxed);
        for table in tables {
            if let Some(g) = self.tables.get(table) {
                sum = sum.wrapping_add(*g);
            }
        }
        sum
    }

    /// The global generation alone: what a result that read no user table
    /// (a catalog listing) is checked against.
    pub fn global_generation(&self) -> u64 {
        self.global.load(Ordering::Relaxed)
    }

    /// The tables a write statement touches, or `None` when they cannot be
    /// determined from its text (DDL, `EXECUTE`/`CALL`/`DO`, `COPY`, a
    /// `MERGE` that is not `MERGE INTO`, an `INSERT` without `INTO`, an
    /// `UPDATE OR …` / `UPDATE REPLACE` or `INSERT INTO LOCAL` that
    /// HeliosDB-Nano and HeliosDB-Lite run on the table named next, no table
    /// found, backend-ambiguous text or more than one statement:
    /// `sql_text::single_statement`) — the caller then invalidates
    /// everything.
    pub fn write_tables(&self, sql: &str) -> Option<Vec<String>> {
        use crate::protocol::starts_with_ci;
        const OPAQUE: &[&str] = &[
            "CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "GRANT", "REVOKE", "EXECUTE", "CALL",
            "DO", "COPY", "IMPORT", "SECURITY", "REFRESH", "REINDEX", "CLUSTER", "VACUUM", "LOCK",
            "SELECT",
        ];
        // Only the table names are needed, and this runs before the client
        // sees the write's ReadyForQuery: one pass of the lexer gives the
        // first word past the leading comments (so one cannot hide a DDL
        // keyword) and the comment-free text the tables are matched on (so
        // an inner comment cannot hide a table name), without the literal
        // rewriting and hashing of `normalize`. A journaled `SELECT`
        // (`SELECT … INTO`, a function call) has an unknown scope.
        let (t, text) = crate::sql_text::single_statement(sql)?;
        // EXPLAIN ANALYZE: the tables of the statement it runs; unreadable
        // (or DDL under it) is unknown scope.
        let (t, text) = if starts_with_ci(t, "EXPLAIN") {
            crate::sql_text::single_statement(crate::sql_lex::explain_innermost(sql)?)?
        } else {
            (t, text)
        };
        if OPAQUE.iter().any(|kw| starts_with_ci(t, kw))
            || crate::sql_text::merge_without_into(t)
            || crate::sql_text::insert_without_into(t)
        {
            return None;
        }
        let tables = self.normalizer.write_statement_tables(&text)?;
        if tables.is_empty() {
            None
        } else {
            Some(tables)
        }
    }

    /// Move the generations a write statement touched: its tables
    /// ([`Self::write_tables`]), or everything when they are unknown
    /// (fail-closed). Returns what moved.
    pub fn note_write(&self, sql: &str) -> WriteScope {
        match self.write_tables(sql) {
            Some(tables) => {
                self.mark_written(&tables);
                WriteScope::Tables(tables)
            }
            None => {
                self.invalidate_all();
                WriteScope::All
            }
        }
    }
}

/// Whether a statement a gateway sent may have written, so its tables'
/// generations must move: anything the journal capture does not classify as
/// a read (a write, DDL, `COPY`, `EXECUTE`, transaction control), a string
/// whose backend-ambiguous text holds a `;` that may end a statement for one
/// backend where the capture reads none (the relay's unseen-write signal),
/// and an `EXPLAIN` that may `ANALYZE` (it executes the statement it
/// explains). A function called from a `SELECT` is a read here, as on the
/// PG-wire relay.
pub(crate) fn gateway_statement_may_write(sql: &str) -> bool {
    use crate::journal_capture::{classify, StmtKind};
    // `EXPLAIN ANALYZE` runs the statement it explains: that one decides.
    let Some(sql) = crate::sql_lex::explain_innermost(sql) else {
        return true;
    };
    if classify(sql).0 != StmtKind::Read {
        return true;
    }
    if sql.contains(';')
        && crate::sql_text::ambiguous_rest(sql).is_some_and(|rest| rest.contains(';'))
    {
        return true;
    }
    match crate::sql_lex::lead(sql) {
        crate::sql_lex::Lead::Start(start) => {
            crate::protocol::starts_with_ci(start, "EXPLAIN")
                && start
                    .as_bytes()
                    .windows(b"analyze".len())
                    .any(|w| w.eq_ignore_ascii_case(b"analyze"))
        }
        // The backends may end the leading comments apart: what the
        // statement is, is unknown.
        crate::sql_lex::Lead::Rest => true,
        crate::sql_lex::Lead::Nothing => false,
    }
}

/// Writes through a gateway since the process started, by gateway: one per
/// statement that moved a generation
/// (`heliosdb_proxy_cache_gateway_write_invalidations_total{gateway}`).
static GATEWAY_WRITE_INVALIDATIONS: [AtomicU64; GatewayKind::COUNT] =
    [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];

/// Statements through `gateway` that moved the write generations.
pub fn gateway_write_invalidations(gateway: GatewayKind) -> u64 {
    GATEWAY_WRITE_INVALIDATIONS[gateway.index()].load(Ordering::Relaxed)
}

/// How a gateway retires cached reads after a statement it ran (C-02 for
/// the HTTP `/sql`, MCP and GraphQL gateways, Sprinter 6a0c6c34d27a). Built
/// by the daemon when a cache runs and attached to every gateway's policy.
#[derive(Clone)]
pub struct GatewayWrites {
    generations: Arc<TableGenerations>,
    /// The query cache, when `[cache]` runs: its generations are
    /// `generations`, and its heatmap and invalidation index follow the
    /// write as they do for a relay write.
    query_cache: Option<Arc<QueryCache>>,
}

impl GatewayWrites {
    /// Writes through the gateways move `generations` (and, with `[cache]`,
    /// go through `query_cache`, whose generations they must be).
    pub fn new(generations: Arc<TableGenerations>, query_cache: Option<Arc<QueryCache>>) -> Self {
        debug_assert!(query_cache
            .as_ref()
            .is_none_or(|qc| Arc::ptr_eq(qc.generations(), &generations)));
        Self {
            generations,
            query_cache,
        }
    }

    /// The generations the gateways move.
    pub fn generations(&self) -> &Arc<TableGenerations> {
        &self.generations
    }

    /// Note one statement `gateway` sent to the backend. Call it once the
    /// backend answered, failed or timed out, whatever the outcome: a
    /// statement that timed out may still commit. A statement that may have
    /// written ([`gateway_statement_may_write`]) moves its tables'
    /// generations, or everything when they are unknown, as a relay write
    /// does; a read moves nothing. Returns what moved.
    pub async fn note_statement(&self, sql: &str, gateway: GatewayKind) -> Option<WriteScope> {
        if !gateway_statement_may_write(sql) {
            return None;
        }
        let scope = match self.generations.write_tables(sql) {
            Some(tables) => WriteScope::Tables(tables),
            None => WriteScope::All,
        };
        match (&self.query_cache, &scope) {
            (Some(qc), WriteScope::Tables(tables)) => qc.invalidate_tables(tables).await,
            (Some(qc), WriteScope::All) => qc.invalidate_all(),
            (None, WriteScope::Tables(tables)) => self.generations.mark_written(tables),
            (None, WriteScope::All) => self.generations.invalidate_all(),
        }
        GATEWAY_WRITE_INVALIDATIONS[gateway.index()].fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            target: "helios::cache",
            gateway = gateway.as_str(),
            scope = ?scope,
            "gateway write retired cached reads"
        );
        Some(scope)
    }
}

impl std::fmt::Debug for GatewayWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayWrites")
            .field("query_cache", &self.query_cache.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_table_write_moves_its_sum_and_the_global_one_moves_every_sum() {
        let g = TableGenerations::new();
        let accounts = names(&["accounts"]);
        let both = names(&["accounts", "users"]);
        let users = names(&["users"]);
        assert_eq!(g.generation_of(&both), 0);
        g.mark_written(&accounts);
        assert_eq!(g.generation_of(&accounts), 1);
        assert_eq!(g.generation_of(&both), 1);
        assert_eq!(g.generation_of(&users), 0, "other tables do not move");
        assert_eq!(g.global_generation(), 0);
        g.invalidate_all();
        assert_eq!(g.global_generation(), 1);
        assert_eq!(g.generation_of(&users), 1);
        assert_eq!(g.generation_of(&[]), 1);
        assert_eq!(g.generation_of(&both), 2);
    }

    #[test]
    fn note_write_marks_the_tables_or_everything() {
        let g = TableGenerations::new();
        assert_eq!(
            g.note_write("UPDATE accounts SET v = 2 WHERE id = 1"),
            WriteScope::Tables(names(&["accounts"]))
        );
        assert_eq!(g.generation_of(&names(&["accounts"])), 1);
        assert_eq!(g.global_generation(), 0);
        // Unknown scope: DDL, more than one statement.
        for sql in [
            "ALTER TABLE t ADD c int",
            "UPDATE a SET v = 1; UPDATE b SET v = 1",
        ] {
            assert_eq!(g.note_write(sql), WriteScope::All, "{sql}");
        }
        assert_eq!(g.global_generation(), 2);
    }

    #[test]
    fn gateway_statements_that_may_write() {
        for sql in [
            "UPDATE widgets SET v = 1",
            "insert into t values (1)",
            "DELETE FROM t",
            "CREATE TABLE t (a int)",
            "COPY t FROM STDIN",
            "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
            "SELECT 1; UPDATE t SET v = 1",
            "EXECUTE upd(1)",
            "COMMIT PREPARED 'g'",
            // A `;` in a backend-ambiguous rest may end a statement.
            "SELECT E'\\'' ; UPDATE b SET v = 1",
            // EXPLAIN ANALYZE runs the statement.
            "EXPLAIN ANALYZE UPDATE t SET v = 1",
            "explain (analyze, buffers) delete from t",
            // A leading comment the backends may end apart: unknown.
            "/* a /* b */ UPDATE t SET v = 1",
        ] {
            assert!(gateway_statement_may_write(sql), "{sql}");
        }
        for sql in [
            "SELECT value FROM widgets",
            "  select * from t where s = 'UPDATE'",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "EXPLAIN SELECT * FROM t",
            "SHOW work_mem",
            "",
        ] {
            assert!(!gateway_statement_may_write(sql), "{sql}");
        }
    }

    #[tokio::test]
    async fn gateway_writes_move_the_shared_generations() {
        let g = Arc::new(TableGenerations::new());
        let w = GatewayWrites::new(g.clone(), None);
        let before = gateway_write_invalidations(GatewayKind::Http);
        assert_eq!(
            w.note_statement("SELECT v FROM accounts", GatewayKind::Http)
                .await,
            None
        );
        assert_eq!(g.generation_of(&names(&["accounts"])), 0);
        assert_eq!(
            w.note_statement("UPDATE accounts SET v = 2", GatewayKind::Http)
                .await,
            Some(WriteScope::Tables(names(&["accounts"])))
        );
        assert_eq!(g.generation_of(&names(&["accounts"])), 1);
        assert_eq!(
            w.note_statement("DROP TABLE accounts", GatewayKind::Http)
                .await,
            Some(WriteScope::All)
        );
        assert_eq!(g.global_generation(), 1);
        assert!(gateway_write_invalidations(GatewayKind::Http) >= before + 2);
    }
}

#[cfg(test)]
mod explain_analyze_tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// An `EXPLAIN ANALYZE` write moves the tables of the statement it runs;
    /// an unknown scope inside it moves everything.
    #[test]
    fn an_explain_analyze_write_moves_the_tables_it_runs() {
        let g = TableGenerations::new();
        assert_eq!(
            g.write_tables("EXPLAIN ANALYZE UPDATE accounts SET v = 2 WHERE id = 1"),
            Some(names(&["accounts"]))
        );
        assert_eq!(
            g.write_tables("EXPLAIN (ANALYZE, COSTS OFF) DELETE FROM accounts"),
            Some(names(&["accounts"]))
        );
        assert_eq!(g.write_tables("EXPLAIN ANALYZE DROP TABLE accounts"), None);
        assert_eq!(g.write_tables("EXPLAIN ANALYZE"), None);
        assert_eq!(
            g.note_write("EXPLAIN ANALYZE UPDATE accounts SET v = 3"),
            WriteScope::Tables(names(&["accounts"]))
        );
        assert_eq!(g.generation_of(&names(&["accounts"])), 1);
        assert_eq!(g.global_generation(), 0);
    }

    /// A gateway statement that runs an EXPLAIN ANALYZE of a read may not
    /// write; an EXPLAIN without ANALYZE executes nothing.
    #[test]
    fn a_gateway_explain_analyze_moves_only_for_a_write() {
        assert!(!gateway_statement_may_write(
            "EXPLAIN ANALYZE SELECT v FROM accounts"
        ));
        assert!(gateway_statement_may_write(
            "EXPLAIN ANALYZE UPDATE accounts SET v = 1"
        ));
        assert!(!gateway_statement_may_write(
            "EXPLAIN UPDATE accounts SET v = 1"
        ));
    }
}
