//! Session state a replacement backend cannot be given.
//!
//! Transaction replay re-homes a session onto a new backend after its own
//! failed: it restores the session's tracked `SET`s, re-prepares its
//! protocol-level statements from the registry and, inside a transaction,
//! replays that transaction. Some state of a session lives only in the
//! backend that failed, and a re-homed session would silently run without
//! it: temporary tables, views and sequences; cursors declared `WITH HOLD`
//! (they outlive their transaction); statements prepared with SQL `PREPARE`;
//! `LISTEN` registrations; session-level advisory locks (released when that
//! backend died, so the application's mutual exclusion would be gone). The
//! client loop marks a session holding any of them ([`Bound`]); a failover of
//! such a session ends it with `08006`, naming what it held, instead of
//! continuing. `DISCARD ALL` (and the narrower statements below) clears the
//! marks again. Marks are conservative: a statement counts once it reached a
//! backend, also when it failed, and an object dropped one by one stays
//! counted until its whole kind is cleared.
//!
//! Inside a transaction the marks wait for its end ([`Bound::pending`]): a
//! transaction replay re-runs the statements that made them on the
//! replacement, so only state from before the transaction blocks it. An
//! aborted transaction keeps what PostgreSQL keeps after a rollback (SQL
//! prepared statements, advisory locks), so those block a re-home that does
//! not replay it.

use super::*;

/// Temporary tables, views or sequences.
pub(super) const TEMP_OBJECTS: u8 = 1 << 0;
/// A cursor declared `WITH HOLD`.
pub(super) const HELD_CURSOR: u8 = 1 << 1;
/// A statement prepared with SQL `PREPARE`.
pub(super) const SQL_PREPARED: u8 = 1 << 2;
/// A `LISTEN` registration.
pub(super) const LISTENING: u8 = 1 << 3;
/// A session-level advisory lock.
pub(super) const ADVISORY_LOCK: u8 = 1 << 4;
/// A session-level `SET`/`RESET` (or `set_config(..., false)`). Transaction
/// replay restores tracked settings, so this kind blocks only a re-home that
/// does not (`tr_mode = none`).
pub(super) const SETTINGS: u8 = 1 << 5;
/// Kinds PostgreSQL keeps when the transaction that made them rolls back.
pub(super) const SURVIVES_ROLLBACK: u8 = SQL_PREPARED | ADVISORY_LOCK;
const ALL: u8 = TEMP_OBJECTS | HELD_CURSOR | SQL_PREPARED | LISTENING | ADVISORY_LOCK | SETTINGS;

/// What one statement (or a whole request, folded in order) does to the
/// marks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Effect {
    /// Kinds the request may have created (also if it failed later).
    pub(super) sets: u8,
    /// Kinds its statements in order leave set, when they all succeeded.
    pub(super) sequential: u8,
    /// Kinds a statement of it cleared, when they all succeeded.
    pub(super) clears: u8,
}

impl Effect {
    /// Fold the next statement's effect after this one.
    pub(super) fn then(self, next: Effect) -> Effect {
        Effect {
            sets: self.sets | next.sets,
            sequential: (self.sequential & !next.clears) | next.sets,
            clears: self.clears | next.clears,
        }
    }

    fn set(kinds: u8) -> Effect {
        Effect {
            sets: kinds,
            sequential: kinds,
            clears: 0,
        }
    }

    fn clear(kinds: u8) -> Effect {
        Effect {
            sets: 0,
            sequential: 0,
            clears: kinds,
        }
    }
}

/// The session's marks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Bound {
    /// Held by the session outside any open transaction.
    pub(super) held: u8,
    /// Made inside the open transaction.
    pub(super) pending: u8,
}

impl Bound {
    /// Apply what a request that reached a backend did: `before`/`after`
    /// are its transaction statuses (`I`, `T`, `E`), `failed` whether its
    /// response carried an error.
    pub(super) fn apply(&mut self, effect: Effect, before: u8, after: u8, failed: bool) {
        if before == b'I' && after == b'I' {
            if failed {
                self.held |= effect.sets;
            } else {
                self.held = (self.held & !effect.clears) | effect.sequential;
            }
        } else {
            // Inside a transaction (or across its start or end) only marks
            // are taken; a clear there is not trusted.
            self.pending |= effect.sets;
        }
        if after == b'I' {
            // The transaction ended: what it made is the session's now
            // (conservatively, also when it rolled back).
            self.held |= std::mem::take(&mut self.pending);
        }
    }

    /// The open transaction was aborted by a failover and the session
    /// continues on a replacement: what it made there vanished with it, and
    /// what survives a rollback blocked that re-home ([`Bound::blocking`]).
    pub(super) fn transaction_lost(&mut self) {
        self.pending = 0;
    }

    /// What blocks re-homing the session: everything held, and, unless the
    /// replacement replays the open transaction, what of it survives a
    /// rollback. Settings block only where they are not restored.
    pub(super) fn blocking(&self, replays_transaction: bool, settings_restored: bool) -> u8 {
        let kinds = if replays_transaction {
            self.held
        } else {
            self.held | (self.pending & SURVIVES_ROLLBACK)
        };
        if settings_restored {
            kinds & !SETTINGS
        } else {
            kinds
        }
    }
}

/// A legacy `FunctionCall` of an arbitrary function (`function_call =
/// "any"`): it may have made any of them.
pub(super) fn any_function_effect() -> Effect {
    Effect::set(ALL)
}

/// `kinds` in words, for the client-visible error.
pub(super) fn describe(kinds: u8) -> String {
    [
        (TEMP_OBJECTS, "temporary tables, views or sequences"),
        (HELD_CURSOR, "a cursor declared WITH HOLD"),
        (SQL_PREPARED, "statements prepared with SQL PREPARE"),
        (LISTENING, "LISTEN registrations"),
        (ADVISORY_LOCK, "session-level advisory locks"),
        (SETTINGS, "session settings (SET)"),
    ]
    .into_iter()
    .filter(|(kind, _)| kinds & kind != 0)
    .map(|(_, words)| words)
    .collect::<Vec<_>>()
    .join(", ")
}

/// What `sql`, one statement or several, does to the marks, in order. Text
/// that names none of the statements involved costs a few keyword checks
/// and a byte search for each of a lock function and a temporary target.
pub(super) fn text_effect(sql: &str) -> Effect {
    let core = sql.trim_ascii_end();
    let single = memchr::memchr(b';', core.as_bytes()).is_none_or(|i| i + 1 == core.len());
    if single {
        return statement_effect(sql);
    }
    match crate::replay_sql::split_statements(sql) {
        Ok(statements) => statements
            .iter()
            .fold(Effect::default(), |acc, s| acc.then(statement_effect(s))),
        // Where the statements end cannot be told: whatever the text names
        // may have run, and nothing it would clear is trusted.
        Err(_) => Effect::set(whole_text_marks(sql)),
    }
}

/// The marks any statement of `sql` could make, read from the whole text.
fn whole_text_marks(sql: &str) -> u8 {
    let mut kinds = 0;
    if ProxyServer::takes_session_lock(sql.as_bytes()) {
        kinds |= ADVISORY_LOCK;
    }
    for (word, kind) in [
        ("temp", TEMP_OBJECTS),
        ("hold", HELD_CURSOR),
        ("prepare", SQL_PREPARED),
        ("listen", LISTENING),
    ] {
        if crate::protocol::contains_ci(sql, word) {
            kinds |= kind;
        }
    }
    kinds
}

/// What one statement does to the marks.
pub(super) fn statement_effect(sql: &str) -> Effect {
    let mut effect = Effect::default();
    if ProxyServer::takes_session_lock(sql.as_bytes()) {
        effect = Effect::set(ADVISORY_LOCK);
    }
    let head = crate::protocol::skip_leading_comments(sql);
    let kw = |k: &str| crate::protocol::leads_with_keyword(head.as_bytes(), k);
    let words = |n: usize| ProxyServer::tr_lead_words(head, n);
    let word_is =
        |w: &[(usize, usize, bool)], i: usize, k: &str| ProxyServer::tr_word_is(head, w, i, k);
    let made = if kw("CREATE") {
        // CREATE [OR REPLACE] [GLOBAL | LOCAL] {TEMP | TEMPORARY} ..., or an
        // object in the session's temporary schema.
        let temporary = words(5).is_some_and(|w| {
            (1..w.len()).any(|i| word_is(&w, i, "TEMP") || word_is(&w, i, "TEMPORARY"))
        }) || crate::protocol::contains_ci(head, "pg_temp");
        if temporary {
            TEMP_OBJECTS
        } else {
            0
        }
    } else if (kw("SELECT") || kw("WITH"))
        // SELECT ... INTO [TEMP | TEMPORARY] [TABLE] t; any other SELECT
        // falls through to the `set_config` check below.
        && crate::protocol::contains_ci(head, "into")
        && selects_into_temporary(head)
    {
        TEMP_OBJECTS
    } else if kw("DECLARE") {
        if declares_with_hold(head) {
            HELD_CURSOR
        } else {
            0
        }
    } else if kw("PREPARE") {
        // PREPARE TRANSACTION is two-phase commit, not a statement.
        if words(2).is_some_and(|w| word_is(&w, 1, "TRANSACTION")) {
            0
        } else {
            SQL_PREPARED
        }
    } else if kw("LISTEN") {
        LISTENING
    } else if kw("SET") || kw("RESET") {
        if ProxyServer::tr_session_level(head) == Some(true) {
            SETTINGS
        } else {
            0
        }
    } else if crate::protocol::contains_ci(head, "set_config") && ProxyServer::tr_sets_config(head)
    {
        SETTINGS
    } else {
        0
    };
    if made != 0 {
        return effect.then(Effect::set(made));
    }
    let cleared = if kw("DISCARD") {
        match words(2) {
            Some(w) if word_is(&w, 1, "ALL") => ALL,
            Some(w) if word_is(&w, 1, "TEMP") || word_is(&w, 1, "TEMPORARY") => TEMP_OBJECTS,
            _ => 0,
        }
    } else if kw("CLOSE") {
        if words(2).is_some_and(|w| word_is(&w, 1, "ALL")) {
            HELD_CURSOR
        } else {
            0
        }
    } else if kw("DEALLOCATE") {
        // DEALLOCATE [PREPARE] ALL
        if words(3).is_some_and(|w| {
            word_is(&w, 1, "ALL") || (word_is(&w, 1, "PREPARE") && word_is(&w, 2, "ALL"))
        }) {
            SQL_PREPARED
        } else {
            0
        }
    } else if kw("UNLISTEN") {
        if head[8..].trim_start().trim_end_matches(';').trim_end() == "*" {
            LISTENING
        } else {
            0
        }
    } else if ProxyServer::calls_unlock_all(head) {
        ADVISORY_LOCK
    } else {
        0
    };
    if cleared != 0 {
        effect.then(Effect::clear(cleared))
    } else {
        effect
    }
}

/// `SELECT ... INTO [TEMP | TEMPORARY] ...` (also behind a `WITH`): the
/// word after an unquoted `INTO` outside literals and comments.
fn selects_into_temporary(sql: &str) -> bool {
    let mut after_into = false;
    let mut temporary = false;
    let _ = crate::replay_sql::words(sql, |w| {
        if after_into {
            temporary = !w.quoted
                && (w.text.eq_ignore_ascii_case("TEMP")
                    || w.text.eq_ignore_ascii_case("TEMPORARY"));
            return false;
        }
        after_into = !w.quoted && w.text.eq_ignore_ascii_case("INTO");
        true
    });
    temporary
}

/// `DECLARE name [...] CURSOR WITH HOLD FOR ...`: an unquoted `WITH`
/// directly followed by `HOLD`.
fn declares_with_hold(sql: &str) -> bool {
    let mut after_with = false;
    let mut hold = false;
    let _ = crate::replay_sql::words(sql, |w| {
        if after_with && !w.quoted && w.text.eq_ignore_ascii_case("HOLD") {
            hold = true;
            return false;
        }
        after_with = !w.quoted && w.text.eq_ignore_ascii_case("WITH");
        // The cursor's query starts at FOR: nothing after it declares.
        w.quoted || !w.text.eq_ignore_ascii_case("FOR")
    });
    hold
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sets(sql: &str) -> u8 {
        text_effect(sql).sequential
    }

    #[test]
    fn statements_that_make_session_bound_state() {
        for (sql, kind) in [
            ("CREATE TEMP TABLE t (a int)", TEMP_OBJECTS),
            ("create temporary table t as select 1", TEMP_OBJECTS),
            ("CREATE GLOBAL TEMPORARY TABLE t (a int)", TEMP_OBJECTS),
            ("CREATE OR REPLACE TEMP VIEW v AS SELECT 1", TEMP_OBJECTS),
            ("create temp sequence s", TEMP_OBJECTS),
            ("CREATE TABLE pg_temp.t (a int)", TEMP_OBJECTS),
            ("SELECT * INTO TEMP t FROM x", TEMP_OBJECTS),
            ("select a into temporary table t from x", TEMP_OBJECTS),
            (
                "WITH q AS (SELECT 1) SELECT * INTO TEMP t FROM q",
                TEMP_OBJECTS,
            ),
            ("DECLARE c CURSOR WITH HOLD FOR SELECT 1", HELD_CURSOR),
            (
                "declare c no scroll cursor with hold for select 1",
                HELD_CURSOR,
            ),
            ("PREPARE p AS SELECT $1::int", SQL_PREPARED),
            ("prepare p(int) as select $1", SQL_PREPARED),
            ("LISTEN jobs", LISTENING),
            ("SELECT pg_advisory_lock(1)", ADVISORY_LOCK),
            (
                "/* lead */ select pg_try_advisory_lock_shared(7)",
                ADVISORY_LOCK,
            ),
        ] {
            assert_eq!(sets(sql), kind, "{sql}");
        }
    }

    #[test]
    fn statements_that_do_not() {
        for sql in [
            "CREATE TABLE t (a int)",
            "CREATE TABLE temperature (a int)",
            "SELECT * FROM t WHERE note = 'into temp'",
            "SELECT a INTO STRICT x FROM t",
            "DECLARE c CURSOR FOR SELECT * FROM t WITH HOLD_ME",
            "DECLARE c CURSOR FOR SELECT 1",
            "PREPARE TRANSACTION 'gid'",
            "SELECT * FROM job_advisory_locks",
            "SELECT pg_advisory_xact_lock(1)",
            "UNLISTEN jobs",
            "INSERT INTO t VALUES (1)",
        ] {
            assert_eq!(text_effect(sql), Effect::default(), "{sql}");
        }
    }

    #[test]
    fn statements_that_clear_them() {
        for (sql, kinds) in [
            ("DISCARD ALL", ALL),
            ("discard temp", TEMP_OBJECTS),
            ("DISCARD TEMPORARY;", TEMP_OBJECTS),
            ("CLOSE ALL", HELD_CURSOR),
            ("DEALLOCATE ALL", SQL_PREPARED),
            ("deallocate prepare all", SQL_PREPARED),
            ("UNLISTEN *", LISTENING),
            ("SELECT pg_advisory_unlock_all()", ADVISORY_LOCK),
        ] {
            assert_eq!(text_effect(sql).clears, kinds, "{sql}");
        }
        for sql in [
            "CLOSE c",
            "DEALLOCATE p",
            "DISCARD PLANS",
            "SELECT pg_advisory_unlock(1)",
        ] {
            assert_eq!(text_effect(sql).clears, 0, "{sql}");
        }
    }

    #[test]
    fn settings_block_only_where_they_are_not_restored() {
        for sql in [
            "SET search_path = app",
            "RESET work_mem",
            "SELECT set_config('a.b', 'c', false)",
        ] {
            assert_eq!(sets(sql), SETTINGS, "{sql}");
        }
        for sql in [
            "SET LOCAL work_mem = '1MB'",
            "SET TRANSACTION READ ONLY",
            "SELECT set_config('a.b', 'c', true)",
        ] {
            assert_eq!(sets(sql), 0, "{sql}");
        }
        let mut b = Bound::default();
        b.apply(text_effect("SET search_path = app"), b'I', b'I', false);
        assert_eq!(b.blocking(false, true), 0, "replay restores settings");
        assert_eq!(
            b.blocking(false, false),
            SETTINGS,
            "tr_mode = none does not"
        );
        b.apply(text_effect("DISCARD ALL"), b'I', b'I', false);
        assert_eq!(b.blocking(false, false), 0);
    }

    #[test]
    fn a_multi_statement_query_is_read_in_order() {
        let e = text_effect("CREATE TEMP TABLE t (a int); DISCARD ALL");
        assert_eq!((e.sets, e.sequential, e.clears), (TEMP_OBJECTS, 0, ALL));
        let e = text_effect("DISCARD ALL; LISTEN jobs; SELECT 1");
        assert_eq!(
            (e.sets, e.sequential, e.clears),
            (LISTENING, LISTENING, ALL)
        );
    }

    #[test]
    fn marks_outside_and_inside_a_transaction() {
        let mut b = Bound::default();
        b.apply(text_effect("LISTEN jobs"), b'I', b'I', false);
        assert_eq!(b.held, LISTENING);
        // A failed clear is not trusted; a successful one is.
        b.apply(text_effect("UNLISTEN *"), b'I', b'I', true);
        assert_eq!(b.held, LISTENING);
        b.apply(text_effect("UNLISTEN *"), b'I', b'I', false);
        assert_eq!(b.held, 0);
        // A failed statement may still have run: its marks count.
        b.apply(text_effect("CREATE TEMP TABLE t (a int)"), b'I', b'I', true);
        assert_eq!(b.held, TEMP_OBJECTS);
        b.apply(text_effect("DISCARD ALL"), b'I', b'I', false);
        assert_eq!(b.held, 0);

        // Inside a transaction: pending until it ends. A replay recreates
        // them; an aborted one keeps only what survives a rollback.
        b.apply(text_effect("BEGIN"), b'I', b'T', false);
        b.apply(
            text_effect("CREATE TEMP TABLE t (a int)"),
            b'T',
            b'T',
            false,
        );
        b.apply(text_effect("SELECT pg_advisory_lock(1)"), b'T', b'T', false);
        assert_eq!(b.held, 0);
        assert_eq!(b.blocking(true, true), 0);
        assert_eq!(b.blocking(false, true), ADVISORY_LOCK);
        let mut aborted = b;
        aborted.transaction_lost();
        assert_eq!(aborted.blocking(false, true), 0);
        // Committed: both are the session's.
        b.apply(text_effect("COMMIT"), b'T', b'I', false);
        assert_eq!(b.held, TEMP_OBJECTS | ADVISORY_LOCK);
        assert_eq!(b.blocking(true, true), TEMP_OBJECTS | ADVISORY_LOCK);
        assert_eq!(
            describe(b.held),
            "temporary tables, views or sequences, session-level advisory locks"
        );
    }
}
