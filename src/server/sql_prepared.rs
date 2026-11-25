//! Prepared statements across pooled connections (W02, `[pool_mode]
//! prepared_statement_mode`).
//!
//! In transaction and statement pooling a session borrows a backend
//! connection per transaction (or statement), and the pool's reset query
//! deallocates what the session prepared on it. Protocol-level named
//! statements live in the session's statement registry and are prepared
//! again wherever a batch uses them (`forward_extended_batch`). This module
//! does the same for statements prepared with SQL `PREPARE` under `track`:
//! the session keeps the `PREPARE` text of each, bounded by `[limits]
//! max_prepared_statements` and `max_prepared_bytes`; before a request
//! runs a statement that uses one (`EXECUTE name`, `EXPLAIN ... EXECUTE
//! name`, `CREATE TABLE ... AS EXECUTE name`, `DEALLOCATE name`) on a
//! connection that lacks it, the proxy runs the `PREPARE` there first; and
//! `DEALLOCATE` / `DISCARD ALL` forget it. Under `disable` a SQL `PREPARE`
//! (and a named protocol-level `Parse`) is refused with 0A000. Session
//! pooling keeps a session's statements on its own connection and is not
//! touched here.

use super::*;
use crate::config::PreparedStatementMode;

/// SQLSTATE of a prepared statement refused under `disable`
/// (`feature_not_supported`).
pub(super) const DISABLED_SQLSTATE: &str = "0A000";

/// SQLSTATE of a SQL `PREPARE` the session's registry has no room for
/// (`program_limit_exceeded`).
pub(super) const REGISTRY_FULL_SQLSTATE: &str = "54000";

/// Why a named `Parse` or a SQL `PREPARE` is refused under `disable`.
pub(super) const DISABLED_MESSAGE: &str =
    "prepared statements are disabled in transaction and statement pooling ([pool_mode] \
     prepared_statement_mode = \"disable\"): use unnamed statements, or set it to \"track\"";

/// What one SQL statement does to the session's SQL-prepared statements.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PrepareEffect {
    /// `PREPARE name ...`: the folded name.
    Prepare(String),
    /// `DEALLOCATE [PREPARE] name`.
    Deallocate(String),
    /// `DEALLOCATE [PREPARE] ALL`, `DISCARD ALL`.
    DeallocateAll,
}

/// Whether `sql` is one statement (no `;` but a final one): one memchr.
fn single_statement(sql: &str) -> bool {
    let core = sql.trim_ascii_end();
    memchr::memchr(b';', core.as_bytes()).is_none_or(|i| i + 1 == core.len())
}

/// The statements of `sql` as PostgreSQL runs a multi-statement query; text
/// whose statements cannot be told apart is read whole.
pub(super) fn statements(sql: &str) -> Vec<&str> {
    if single_statement(sql) {
        return vec![sql];
    }
    crate::replay_sql::split_statements(sql).unwrap_or_else(|_| vec![sql])
}

/// Whether `sql` leads with one of `keywords` (after leading comments).
fn leads_with_any(sql: &str, keywords: &[&str]) -> bool {
    let head = crate::protocol::skip_leading_comments(sql);
    keywords
        .iter()
        .any(|k| crate::protocol::leads_with_keyword(head.as_bytes(), k))
}

/// Whether `sql` may change the session's SQL-prepared statements: a
/// multi-statement text, or one statement leading with `PREPARE`,
/// `DEALLOCATE` or `DISCARD`. The request path's cheap gate: one memchr and
/// a few keyword compares, no allocation.
pub(super) fn may_change(sql: &str) -> bool {
    !single_statement(sql) || leads_with_any(sql, &["PREPARE", "DEALLOCATE", "DISCARD"])
}

/// An identifier as PostgreSQL resolves it: unquoted folded to lower case,
/// quoted taken as written (`""` is one `"`).
fn fold(text: &str, quoted: bool) -> String {
    if quoted {
        text.strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .unwrap_or(text)
            .replace("\"\"", "\"")
    } else {
        text.to_ascii_lowercase()
    }
}

/// The first `n` words of `sql` outside literals and comments: (folded
/// text, quoted).
fn lead_words(sql: &str, n: usize) -> Vec<(String, bool)> {
    let mut out = Vec::with_capacity(n);
    let _ = crate::replay_sql::words(sql, |w| {
        out.push((fold(w.text, w.quoted), w.quoted));
        out.len() < n
    });
    out
}

/// The effect of one statement on SQL-prepared statements, if any.
pub(super) fn statement_effect(sql: &str) -> Option<PrepareEffect> {
    let head = crate::protocol::skip_leading_comments(sql);
    let kw = |k: &str| crate::protocol::leads_with_keyword(head.as_bytes(), k);
    if kw("PREPARE") {
        let words = lead_words(head, 2);
        // `PREPARE TRANSACTION 'gid'` is two-phase commit.
        return match words.get(1) {
            Some((w, false)) if w == "transaction" => None,
            Some((name, _)) => Some(PrepareEffect::Prepare(name.clone())),
            None => None,
        };
    }
    if kw("DEALLOCATE") {
        let words = lead_words(head, 3);
        let name = match words.get(1) {
            Some((w, false)) if w == "prepare" => words.get(2),
            other => other,
        };
        return match name {
            Some((w, false)) if w == "all" => Some(PrepareEffect::DeallocateAll),
            Some((name, _)) => Some(PrepareEffect::Deallocate(name.clone())),
            None => None,
        };
    }
    if kw("DISCARD") {
        let words = lead_words(head, 2);
        return matches!(words.get(1), Some((w, false)) if w == "all")
            .then_some(PrepareEffect::DeallocateAll);
    }
    None
}

/// Whether `sql` (any of its statements) prepares a statement with SQL
/// `PREPARE`. A single statement is read by its leading keyword alone.
pub(super) fn prepares(sql: &str) -> bool {
    if single_statement(sql) {
        return leads_with_any(sql, &["PREPARE"])
            && matches!(statement_effect(sql), Some(PrepareEffect::Prepare(_)));
    }
    statements(sql)
        .into_iter()
        .any(|s| matches!(statement_effect(s), Some(PrepareEffect::Prepare(_))))
}

/// The SQL-prepared statements `sql` refers to: the name after every
/// `EXECUTE` and the one a `DEALLOCATE` names (folded).
pub(super) fn used_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    if !crate::protocol::contains_ci(sql, "execute")
        && !crate::protocol::contains_ci(sql, "deallocate")
    {
        return names;
    }
    let mut after_execute = false;
    let _ = crate::replay_sql::words(sql, |w| {
        if after_execute {
            names.push(fold(w.text, w.quoted));
        }
        after_execute = !w.quoted && w.text.eq_ignore_ascii_case("execute");
        true
    });
    for statement in statements(sql) {
        if let Some(PrepareEffect::Deallocate(name)) = statement_effect(statement) {
            names.push(name);
        }
    }
    names
}

/// The SQL texts an extended-protocol `batch` runs, in order: the statement
/// behind each `Execute`'s portal, bound in the batch to a statement parsed
/// in it (or held aside by the unnamed-`Parse` promotion, `held_unnamed`),
/// or registered earlier under its name (`registry`). An `Execute` whose
/// statement is not known here (the unnamed statement of an earlier cycle)
/// is skipped.
pub(super) fn executed_texts(
    batch: &[u8],
    held_unnamed: Option<&[u8]>,
    registry: &HashMap<String, bytes::Bytes>,
) -> Vec<String> {
    let mut statements: HashMap<String, String> = HashMap::new();
    if let Some(sql) = held_unnamed.and_then(ProxyServer::parse_msg_sql) {
        statements.insert(String::new(), sql.to_string());
    }
    let mut portals: HashMap<String, String> = HashMap::new();
    let mut ran = Vec::new();
    let cstr = |body: &[u8], at: usize| -> Option<(String, usize)> {
        let end = at + memchr::memchr(0, body.get(at..)?)?;
        Some((
            String::from_utf8_lossy(&body[at..end]).into_owned(),
            end + 1,
        ))
    };
    let mut at = 0;
    while let Some(head) = batch.get(at..at + 5) {
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let Some(body) = batch.get(at + 5..at + 1 + len) else {
            break;
        };
        match head[0] {
            b'P' => {
                if let Some((name, next)) = cstr(body, 0) {
                    if let Some((sql, _)) = cstr(body, next) {
                        statements.insert(name, sql);
                    }
                }
            }
            b'B' => {
                if let Some((portal, next)) = cstr(body, 0) {
                    if let Some((stmt, _)) = cstr(body, next) {
                        let sql = statements.get(&stmt).cloned().or_else(|| {
                            (!stmt.is_empty())
                                .then(|| registry.get(&stmt))
                                .flatten()
                                .and_then(|p| ProxyServer::parse_msg_sql(p))
                                .map(str::to_string)
                        });
                        match sql {
                            Some(sql) => {
                                portals.insert(portal, sql);
                            }
                            None => {
                                portals.remove(&portal);
                            }
                        }
                    }
                }
            }
            b'E' => {
                if let Some(sql) = cstr(body, 0).and_then(|(portal, _)| portals.get(&portal)) {
                    ran.push(sql.clone());
                }
            }
            _ => {}
        }
        at += 1 + len;
    }
    ran
}

/// Whether `batch` uses the unnamed statement before parsing one of its
/// own: a query the proxy runs ahead of it on the connection would destroy
/// the unnamed statement it relies on.
pub(super) fn uses_prior_unnamed(batch: &[u8]) -> bool {
    let mut at = 0;
    while let Some(head) = batch.get(at..at + 5) {
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let body = batch
            .get(at + 5..(at + 1 + len).min(batch.len()))
            .unwrap_or_default();
        match head[0] {
            b'P' if body.first() == Some(&0) => return false,
            b'B' => {
                // portal\0 statement\0: the unnamed statement is "".
                if let Some(end) = memchr::memchr(0, body) {
                    if body.get(end + 1) == Some(&0) {
                        return true;
                    }
                }
            }
            b'D' if body.get(..2) == Some(b"S\0") => return true,
            _ => {}
        }
        at += 1 + len;
    }
    false
}

/// The statements a session prepared with SQL `PREPARE` (`track`), by
/// folded name, with their `PREPARE` text.
#[derive(Debug, Default)]
pub(crate) struct SqlPrepared {
    texts: HashMap<String, String>,
    bytes: usize,
}

impl SqlPrepared {
    pub(super) fn get(&self, name: &str) -> Option<&str> {
        self.texts.get(name).map(String::as_str)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.texts.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.texts.is_empty()
    }

    /// Whether `name` with a `PREPARE` text of `text_len` bytes fits in
    /// `max_count` statements and `max_bytes` bytes (a redefinition replaces
    /// the old text).
    pub(super) fn fits(
        &self,
        name: &str,
        text_len: usize,
        max_count: usize,
        max_bytes: usize,
    ) -> bool {
        let old = self.texts.get(name).map_or(0, String::len);
        let count = self.texts.len() + usize::from(!self.texts.contains_key(name));
        count <= max_count && self.bytes - old + text_len <= max_bytes
    }

    pub(super) fn insert(&mut self, name: String, text: String) {
        let added = text.len();
        if let Some(old) = self.texts.insert(name, text) {
            self.bytes -= old.len();
        }
        self.bytes += added;
    }

    pub(super) fn remove(&mut self, name: &str) {
        if let Some(old) = self.texts.remove(name) {
            self.bytes -= old.len();
        }
    }

    pub(super) fn clear(&mut self) {
        self.texts.clear();
        self.bytes = 0;
    }
}

impl ProxyServer {
    /// The prepared-statement mode that acts on this session's requests:
    /// `None` in session pooling, which keeps prepared statements on the
    /// session's own connection.
    pub(super) fn pooled_prepare_mode(config: &ProxyConfig) -> Option<PreparedStatementMode> {
        config
            .pool_mode
            .pooled()
            .then(|| config.pool_mode.effective_prepared_statement_mode())
    }

    /// A named protocol-level `Parse` is refused (`disable`).
    pub(super) fn named_parse_refused(config: &ProxyConfig) -> bool {
        Self::pooled_prepare_mode(config) == Some(PreparedStatementMode::Disable)
    }

    /// The refusal of a statement text before it reaches a backend, if any:
    /// under `disable` one that prepares with SQL `PREPARE` (0A000); under
    /// `track` one whose `PREPARE` the session's registry has no room for
    /// (54000).
    pub(super) fn sql_prepare_refusal(
        sql: &str,
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) -> Option<(&'static str, String)> {
        let mode = Self::pooled_prepare_mode(config)?;
        if single_statement(sql) && !leads_with_any(sql, &["PREPARE"]) {
            return None;
        }
        match mode {
            PreparedStatementMode::Disable => {
                prepares(sql).then(|| (DISABLED_SQLSTATE, DISABLED_MESSAGE.to_string()))
            }
            PreparedStatementMode::Track => {
                let registry = session.sql_prepared.lock();
                for statement in statements(sql) {
                    if let Some(PrepareEffect::Prepare(name)) = statement_effect(statement) {
                        if !registry.fits(
                            &name,
                            statement.len(),
                            state.limits.max_prepared_statements,
                            state.limits.max_prepared_bytes,
                        ) {
                            return Some((
                                REGISTRY_FULL_SQLSTATE,
                                format!(
                                    "too many SQL-prepared statements for this session ([limits] \
                                     max_prepared_statements {}, max_prepared_bytes {}): \
                                     DEALLOCATE one first",
                                    state.limits.max_prepared_statements,
                                    state.limits.max_prepared_bytes
                                ),
                            ));
                        }
                    }
                }
                None
            }
            PreparedStatementMode::Named => None,
        }
    }

    /// `track`: before `texts` run on the connection `bc`, run there the
    /// `PREPARE` of every SQL-prepared statement they use that the
    /// connection lacks. Inside a transaction block each runs under a
    /// savepoint, so a `PREPARE` the backend refuses leaves the client's
    /// transaction as it was; in a failed one nothing is run (the backend
    /// answers the request as it would). A refused `PREPARE` stops here and
    /// the request is sent as it is, to fail or not as PostgreSQL decides.
    /// `Err` only when the connection itself fails (nothing of the request
    /// was sent).
    pub(super) async fn reprepare_sql(
        bc: &mut BackendConn,
        texts: &[&str],
        session: &ClientSession,
        state: &ServerState,
    ) -> Result<()> {
        if !session
            .sql_prepared_live
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }
        let needed: Vec<(String, String)> = {
            let registry = session.sql_prepared.lock();
            let mut needed: Vec<(String, String)> = Vec::new();
            for name in texts.iter().flat_map(|t| used_names(t)) {
                if bc.sql_prepared.contains(&name) || needed.iter().any(|(n, _)| *n == name) {
                    continue;
                }
                if let Some(text) = registry.get(&name) {
                    needed.push((name, text.to_string()));
                }
            }
            needed
        };
        if needed.is_empty() {
            return Ok(());
        }
        let status = session
            .last_rfq_status
            .load(std::sync::atomic::Ordering::Relaxed);
        if status == b'E' {
            return Ok(());
        }
        let (wt, rt, mf) = (
            state.limits.backend_write_timeout,
            state.limits.backend_read_timeout,
            state.limits.max_backend_frame_bytes,
        );
        const SAVEPOINT: &str = "helios_sql_prepare";
        for (name, text) in needed {
            // The proxy's own Queries destroy the unnamed statement and
            // portal, and leave session state the reset must clear.
            bc.unnamed_destroyed();
            #[cfg(feature = "pool-modes")]
            {
                bc.dirty = true;
            }
            let in_block = status == b'T';
            if in_block {
                let sql = format!("SAVEPOINT {SAVEPOINT}");
                if let Err(e) = Self::tr_run_discard(&mut bc.stream, &sql, wt, rt, mf, None).await {
                    return match e {
                        ProxyError::Protocol(_) => Ok(()),
                        e => Err(e),
                    };
                }
            }
            let prepared = Self::tr_run_discard(&mut bc.stream, &text, wt, rt, mf, None).await;
            let finish = match (&prepared, in_block) {
                (Ok(_), true) => Some(format!("RELEASE SAVEPOINT {SAVEPOINT}")),
                (Err(ProxyError::Protocol(_)), true) => Some(format!(
                    "ROLLBACK TO SAVEPOINT {SAVEPOINT}; RELEASE SAVEPOINT {SAVEPOINT}"
                )),
                _ => None,
            };
            if let Some(sql) = finish {
                if let Err(e) = Self::tr_run_discard(&mut bc.stream, &sql, wt, rt, mf, None).await {
                    return match e {
                        ProxyError::Protocol(_) => Ok(()),
                        e => Err(e),
                    };
                }
            }
            match prepared {
                Ok(_) => {
                    tracing::debug!(target: "helios::pool", statement = %name, "prepared a SQL-prepared statement again on a pooled connection");
                    bc.sql_prepared.insert(name);
                }
                Err(ProxyError::Protocol(_)) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// After a request reached `node`: record what its statements `ran`
    /// (in order) did to the session's SQL-prepared statements and to the
    /// connection's. `completed` is how many of them completed (the
    /// request's tally; `None` when it is not whole): a `PREPARE` counts
    /// only once it completed, a `DEALLOCATE` already when it may have run.
    pub(super) fn note_sql_prepares(
        conns: &mut super::ConnMap,
        node: Option<&str>,
        ran: &[&str],
        completed: Option<u32>,
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) {
        if Self::pooled_prepare_mode(config) != Some(PreparedStatementMode::Track) {
            return;
        }
        if !ran.iter().any(|sql| may_change(sql)) {
            return;
        }
        let mut bc = node.and_then(|n| conns.get_mut(n));
        let mut registry = session.sql_prepared.lock();
        for (i, statement) in ran.iter().enumerate() {
            let done = completed.is_some_and(|c| (i as u32) < c);
            match statement_effect(statement) {
                Some(PrepareEffect::Prepare(name)) if done => {
                    if registry.fits(
                        &name,
                        statement.len(),
                        state.limits.max_prepared_statements,
                        state.limits.max_prepared_bytes,
                    ) {
                        registry.insert(name.clone(), statement.trim().to_string());
                    }
                    if let Some(bc) = bc.as_deref_mut() {
                        bc.sql_prepared.insert(name);
                    }
                }
                Some(PrepareEffect::Deallocate(name)) => {
                    registry.remove(&name);
                    if let Some(bc) = bc.as_deref_mut() {
                        bc.sql_prepared.remove(&name);
                    }
                }
                Some(PrepareEffect::DeallocateAll) => {
                    registry.clear();
                    if let Some(bc) = bc.as_deref_mut() {
                        bc.sql_prepared.clear();
                    }
                }
                _ => {}
            }
        }
        session
            .sql_prepared_live
            .store(!registry.is_empty(), std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_effects_follow_postgresql_names() {
        assert_eq!(
            statement_effect("PREPARE Foo (int) AS SELECT $1"),
            Some(PrepareEffect::Prepare("foo".into()))
        );
        assert_eq!(
            statement_effect("/* c */ prepare \"Mixed\" AS SELECT 1"),
            Some(PrepareEffect::Prepare("Mixed".into()))
        );
        assert_eq!(statement_effect("PREPARE TRANSACTION 'gid'"), None);
        assert_eq!(
            statement_effect("PREPARE \"transaction\" AS SELECT 1"),
            Some(PrepareEffect::Prepare("transaction".into()))
        );
        assert_eq!(
            statement_effect("DEALLOCATE p"),
            Some(PrepareEffect::Deallocate("p".into()))
        );
        assert_eq!(
            statement_effect("deallocate prepare P"),
            Some(PrepareEffect::Deallocate("p".into()))
        );
        assert_eq!(
            statement_effect("DEALLOCATE ALL"),
            Some(PrepareEffect::DeallocateAll)
        );
        assert_eq!(
            statement_effect("DISCARD ALL"),
            Some(PrepareEffect::DeallocateAll)
        );
        assert_eq!(statement_effect("DISCARD PLANS"), None);
        assert_eq!(statement_effect("SELECT 'PREPARE x'"), None);
        assert!(prepares("SELECT 1; PREPARE p AS SELECT 2"));
        assert!(!prepares("SELECT 'prepare p as'"));
        assert!(!prepares("PREPARE TRANSACTION 'x'"));
    }

    #[test]
    fn used_names_cover_execute_explain_ctas_and_deallocate() {
        assert_eq!(used_names("EXECUTE p(1)"), ["p"]);
        assert_eq!(used_names("explain analyze execute P (2)"), ["p"]);
        assert_eq!(
            used_names("CREATE TABLE t AS EXECUTE \"Q\"; DEALLOCATE r"),
            ["Q", "r"]
        );
        assert!(used_names("SELECT 'execute p'").is_empty());
        assert!(used_names("SELECT 1").is_empty());
    }

    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn executed_texts_follow_portals_to_their_statements() {
        let parse = |name: &str, sql: &str| frame(b'P', format!("{name}\0{sql}\0\0\0").as_bytes());
        let bind = |portal: &str, stmt: &str| {
            frame(b'B', format!("{portal}\0{stmt}\0\0\0\0\0\0\0").as_bytes())
        };
        let execute = |portal: &str| frame(b'E', format!("{portal}\0\0\0\0\0").as_bytes());
        let mut registry = HashMap::new();
        registry.insert(
            "old".to_string(),
            bytes::Bytes::from(parse("old", "DEALLOCATE x")),
        );
        let batch = [
            parse("", "PREPARE p AS SELECT 1"),
            bind("", ""),
            execute(""),
            // Parsed only: never executed.
            parse("n", "PREPARE q AS SELECT 2"),
            bind("c", "old"),
            execute("c"),
            frame(b'S', b""),
        ]
        .concat();
        assert_eq!(
            executed_texts(&batch, None, &registry),
            ["PREPARE p AS SELECT 1", "DEALLOCATE x"]
        );
        let held = parse("", "EXECUTE p");
        let batch = [bind("", ""), execute(""), frame(b'S', b"")].concat();
        assert_eq!(
            executed_texts(&batch, Some(&held), &registry),
            ["EXECUTE p"]
        );
        assert!(executed_texts(&batch, None, &registry).is_empty());
        assert!(uses_prior_unnamed(&batch));
        let own = [parse("", "EXECUTE p"), bind("", ""), execute("")].concat();
        assert!(!uses_prior_unnamed(&own));
        let named = [bind("", "s1"), execute("")].concat();
        assert!(!uses_prior_unnamed(&named));
    }

    #[test]
    fn the_registry_counts_names_and_bytes() {
        let mut r = SqlPrepared::default();
        assert!(r.fits("a", 10, 1, 10));
        r.insert("a".into(), "x".repeat(10));
        assert!(!r.fits("b", 1, 1, 100), "count");
        assert!(r.fits("a", 10, 1, 10), "a redefinition replaces");
        assert!(!r.fits("a", 11, 1, 10), "bytes");
        r.insert("a".into(), "y".repeat(4));
        assert!(r.fits("b", 6, 2, 10));
        r.remove("a");
        assert_eq!(r.len(), 0);
        assert!(r.fits("b", 10, 1, 10));
        r.insert("b".into(), "z".into());
        r.clear();
        assert_eq!((r.len(), r.bytes), (0, 0));
    }
}
