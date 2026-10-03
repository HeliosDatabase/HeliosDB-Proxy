use super::*;

// Test-only tally of classifier evaluations performed on the CURRENT thread.
// `libtest` runs each test on its own thread, so the count is per-test and
// race-free; `StmtFacts` bumps it every time it actually walks the SQL, which
// is what lets the laziness contract be asserted rather than assumed.
#[cfg(test)]
thread_local! {
    static STMT_FACT_CLASSIFICATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Classifier evaluations performed on this thread so far (test-only).
#[cfg(test)]
pub(super) fn stmt_fact_classifications() -> usize {
    STMT_FACT_CLASSIFICATIONS.with(std::cell::Cell::get)
}

/// Tables the current explicit transaction wrote, staged for a commit-time
/// cache invalidation (C-02).
#[cfg(feature = "query-cache")]
#[derive(Debug, Default)]
pub(crate) struct TxCacheStage {
    pub(super) tables: Vec<String>,
    /// A statement whose tables could not be determined (DDL, `EXECUTE`,
    /// `COPY`, ...): the commit invalidates everything.
    pub(super) unknown: bool,
}

/// Cache invalidation owed by one observed request/response cycle (C-02).
#[cfg(feature = "query-cache")]
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct CacheWork {
    /// Tables to invalidate: written now, or staged by a transaction that
    /// just committed.
    pub(super) tables: Vec<String>,
    /// Invalidate everything (a write with an unknown table set).
    pub(super) all: bool,
}

/// Journal-capture ops of one completed cycle, observed before its
/// `ReadyForQuery` reached the client (the cache generations already moved);
/// the journal is applied after the client write.
pub(super) struct ObservedCycle {
    pub(super) ops: Vec<crate::journal_capture::JournalOp>,
    /// Tables whose L2 entries to purge once the client has its answer.
    #[cfg(feature = "query-cache")]
    pub(super) purge: Vec<String>,
}

/// The cheap lexical facts about ONE simple-query statement, memoized so each
/// fact is derived AT MOST ONCE per message — and ONLY if a gate that is
/// actually enabled asks for it.
///
/// Every getter delegates to the classifier that already owned that decision
/// (`is_write_query`, `stmt_leaves_session_state`, `is_cacheable_read_sql`,
/// …), so the semantics are byte-identical. What changes is the number of
/// passes over the SQL: `forward_simple_query` used to re-derive several of
/// these two or three times for the same string (once per *call site*); it now
/// derives each at most once (once per *fact*).
///
/// The memo is deliberately LAZY. Each of these classifiers sat behind a
/// runtime gate that is OFF in the stock configuration —
/// `[pool_mode] skip_clean_reset = false`, `[cache] enabled = false`,
/// `[edge] enabled = false` — so a default proxy classified the leading
/// keyword once and nothing else. Computing the whole set up front would make
/// that default hot path do strictly MORE work than before, worst of all for
/// the big statements (bulk INSERT, large SELECT text) these classifiers scan
/// end to end. Every getter is therefore called from inside the very gate that
/// used to guard the classification, and short-circuits with it.
///
/// Facts describe the text they were computed from: `sql` is borrowed from the
/// message payload. When a routing-hint strip, a rewrite rule, or a tenant
/// transform replaces the SQL, the caller rebuilds the whole value on the final
/// text — which resets every memo cell — before any gate consults it.
///
/// Cells are `cfg`-gated to the features that consume them so the struct
/// carries no dead state in a minimal build.
#[derive(Debug)]
pub(super) struct StmtFacts<'a> {
    /// The statement text every cell is derived from. `""` when the payload
    /// carried no valid query cstring — the same fallback the individual call
    /// sites used (`unwrap_or("")`, or a skipped `if let Some(sql)`: all four
    /// classifiers answer `false` for the empty string, so the two agree).
    pub(super) sql: &'a str,
    /// `is_write_query`: routing-relevant write / transaction-control / SET.
    pub(super) is_write: Option<bool>,
    /// A `;` before the (optional) trailing one — i.e. the simple-query string
    /// carries more than one statement, so no leading-keyword classification
    /// can vouch for what follows.
    #[cfg(feature = "edge-proxy")]
    pub(super) has_interior_semicolon: Option<bool>,
    /// `stmt_leaves_session_state`: not provably session-neutral.
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    pub(super) leaves_session_state: Option<bool>,
    /// `is_cacheable_read_sql`: plain, deterministic, single-statement SELECT.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) is_cacheable_read: Option<bool>,
}

impl<'a> StmtFacts<'a> {
    /// Facts for `sql`. Nothing is classified here — every cell is empty
    /// until a getter asks for it.
    pub(super) fn new(sql: &'a str) -> Self {
        Self {
            sql,
            is_write: None,
            #[cfg(feature = "edge-proxy")]
            has_interior_semicolon: None,
            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            leaves_session_state: None,
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            is_cacheable_read: None,
        }
    }

    /// Facts for a simple `Query` message, borrowing the SQL straight out of
    /// the payload (the message is forwarded verbatim, so no copy is needed).
    pub(super) fn of_query(msg: &'a Message) -> Self {
        Self::new(crate::protocol::query_text(&msg.payload).unwrap_or(""))
    }

    /// The statement text the facts describe — so a gate that also needs the
    /// SQL itself does not re-walk the payload for its own copy.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn sql(&self) -> &'a str {
        self.sql
    }

    /// Compute-on-first-use: run `classify` over the statement the first time
    /// a cell is read, remember the answer, never run it again.
    pub(super) fn memo<F: FnOnce(&str) -> bool>(
        cell: &mut Option<bool>,
        sql: &'a str,
        classify: F,
    ) -> bool {
        match *cell {
            Some(v) => v,
            None => {
                Self::note_classification();
                let v = classify(sql);
                *cell = Some(v);
                v
            }
        }
    }

    #[cfg(test)]
    pub(super) fn note_classification() {
        STMT_FACT_CLASSIFICATIONS.with(|c| c.set(c.get() + 1));
    }

    #[cfg(not(test))]
    #[inline(always)]
    pub(super) fn note_classification() {}

    /// See [`ProxyServer::is_write_query`].
    pub(super) fn is_write(&mut self) -> bool {
        Self::memo(&mut self.is_write, self.sql, ProxyServer::is_write_query)
    }

    /// See [`ProxyServer::stmt_has_interior_semicolon`].
    #[cfg(feature = "edge-proxy")]
    pub(super) fn has_interior_semicolon(&mut self) -> bool {
        Self::memo(
            &mut self.has_interior_semicolon,
            self.sql,
            ProxyServer::stmt_has_interior_semicolon,
        )
    }

    /// See [`ProxyServer::stmt_leaves_session_state`].
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    pub(super) fn leaves_session_state(&mut self) -> bool {
        Self::memo(
            &mut self.leaves_session_state,
            self.sql,
            ProxyServer::stmt_leaves_session_state,
        )
    }

    /// See [`ProxyServer::is_cacheable_read_sql`].
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn is_cacheable_read(&mut self) -> bool {
        Self::memo(
            &mut self.is_cacheable_read,
            self.sql,
            ProxyServer::is_cacheable_read_sql,
        )
    }
}
