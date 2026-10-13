use super::*;

impl ProxyServer {
    pub(super) fn is_write_query(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let trimmed = sql.trim();

        // Write operations
        if starts_with_ci(trimmed, "INSERT")
            || starts_with_ci(trimmed, "UPDATE")
            || starts_with_ci(trimmed, "DELETE")
            || starts_with_ci(trimmed, "CREATE")
            || starts_with_ci(trimmed, "DROP")
            || starts_with_ci(trimmed, "ALTER")
            || starts_with_ci(trimmed, "TRUNCATE")
            || starts_with_ci(trimmed, "GRANT")
            || starts_with_ci(trimmed, "REVOKE")
            || starts_with_ci(trimmed, "VACUUM")
            || starts_with_ci(trimmed, "REINDEX")
            || starts_with_ci(trimmed, "CLUSTER")
        {
            return true;
        }

        // Transaction control goes to current node
        if starts_with_ci(trimmed, "BEGIN")
            || starts_with_ci(trimmed, "START")
            || starts_with_ci(trimmed, "COMMIT")
            || starts_with_ci(trimmed, "ROLLBACK")
            || starts_with_ci(trimmed, "SAVEPOINT")
            || starts_with_ci(trimmed, "RELEASE")
        {
            return true;
        }

        // SET commands go to primary to maintain session state
        if starts_with_ci(trimmed, "SET") && !starts_with_ci(trimmed, "SET TRANSACTION READ ONLY") {
            return true;
        }

        false
    }

    /// Should this successfully-executed simple-query string trigger an edge
    /// invalidation?
    ///
    /// - Bare transaction control (BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK)
    ///   changes no rows: exempt, or every ORM transaction would full-flush
    ///   the whole fleet twice per request. COMMIT is deliberately NOT
    ///   exempt (its flush closes the invalidate-at-statement vs
    ///   commit-visibility window for in-transaction writes), nor is SET
    ///   (the flush is the interim mitigation for GUC-sensitive results).
    /// - A multi-statement string (interior `;`) may hide a trailing write
    ///   behind a read-classified lead — always invalidate (the fingerprint
    ///   extracts tables from every sub-statement; over-invalidation only).
    /// - `COPY ... FROM` loads rows but is not classified `is_write` (it
    ///   must not be re-routed); catch it here for invalidation.
    ///
    /// `multi_stmt` is `StmtFacts::has_interior_semicolon` for the same SQL —
    /// passed in rather than re-scanned (`stmt_has_interior_semicolon` is the
    /// single definition of that fact).
    #[cfg(feature = "edge-proxy")]
    pub(super) fn edge_write_needs_invalidation(
        is_write: bool,
        sql: &str,
        multi_stmt: bool,
    ) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if !multi_stmt
            && (starts_with_ci(core, "BEGIN")
                || starts_with_ci(core, "START")
                || starts_with_ci(core, "SAVEPOINT")
                || starts_with_ci(core, "RELEASE")
                || starts_with_ci(core, "ROLLBACK"))
        {
            return false;
        }
        is_write
            || multi_stmt
            || Self::is_edge_copy_write_sql(core)
            || Self::is_edge_procedural_sql(core)
            || Self::is_edge_txn_end_sql(core)
    }

    /// Does the simple-query string carry more than one statement? A `;`
    /// before the optional trailing one means a leading-keyword check cannot
    /// vouch for what follows. (A `;` inside a string literal also trips this —
    /// conservative, and only ever costs an extra invalidation/reset.)
    #[cfg(feature = "edge-proxy")]
    pub(super) fn stmt_has_interior_semicolon(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        core.contains(';')
    }

    /// `COPY ... FROM ...` (STDIN or file) loads rows. Word-boundary FROM so
    /// `COPY t TO ...` stays a read; a `COPY (SELECT ... FROM t) TO ...`
    /// false-positive only over-invalidates — safe.
    #[cfg(feature = "edge-proxy")]
    pub(super) fn is_edge_copy_write_sql(sql: &str) -> bool {
        crate::protocol::starts_with_ci(sql.trim_start(), "COPY")
            && Self::contains_word_ci(sql, "from")
    }

    /// Which of a batch's Closed statement names may have their edge
    /// invalidation metadata pruned at a Sync boundary. A name Closed and then
    /// re-Parsed in the SAME batch must keep its FRESH metadata (dropping it
    /// would silently disable invalidation for the live re-prepared statement —
    /// the Npgsql statement-replacement pattern), so it is excluded. Pruning is
    /// additionally gated on the Sync by the caller, so a Close seen at an
    /// earlier Flush keeps its metadata alive for the terminating Sync's
    /// invalidation hook. Regression guard for the G1 finding.
    #[cfg(feature = "edge-proxy")]
    pub(super) fn edge_meta_prunable<'a>(closes: &'a [String], defines: &[String]) -> Vec<&'a str> {
        closes
            .iter()
            .filter(|n| !defines.contains(n))
            .map(String::as_str)
            .collect()
    }

    /// SQL-level statements whose written table set cannot be attributed
    /// statically — `EXECUTE` (runs a prepared plan), `CALL` (a procedure),
    /// `DO` (an anonymous block, often dynamic SQL). Treated as an
    /// invalidate-everything wildcard write. Rare on the wire (no mainstream
    /// driver emits them for data changes), so the full flush is cheap in
    /// practice and over-invalidation is always safe.
    #[cfg(feature = "edge-proxy")]
    pub(super) fn is_edge_procedural_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        starts_with_ci(t, "EXECUTE") || starts_with_ci(t, "CALL") || starts_with_ci(t, "DO")
    }

    /// Transaction-ending statements: `COMMIT`/`END` and their variants
    /// (`COMMIT WORK`/`AND CHAIN`/`PREPARED`, `END TRANSACTION`). They make a
    /// transaction's in-flight writes visible, so — like the simple-path
    /// COMMIT — they trigger the conservative wildcard flush that closes the
    /// invalidate-at-statement vs commit-visibility window. `BEGIN`/`START`/
    /// `SAVEPOINT`/`RELEASE`/`ROLLBACK` are excluded (they make nothing newly
    /// visible). `END` is a COMMIT synonym here; a rare non-transaction
    /// statement that happens to start with `END` only over-invalidates.
    #[cfg(feature = "edge-proxy")]
    pub(super) fn is_edge_txn_end_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        starts_with_ci(t, "COMMIT") || starts_with_ci(t, "END")
    }

    /// Extended-protocol statement classifier for edge invalidation: does
    /// this Parse'd SQL modify table data when executed? Deliberately NOT
    /// `is_write_query` — that routing classifier counts BEGIN/COMMIT/SET as
    /// writes, and their empty table set would full-flush the fleet on every
    /// transaction commit. WITH-prefixed statements are checked for
    /// data-modifying CTE verbs by word (false positives over-invalidate
    /// only).
    #[cfg(feature = "edge-proxy")]
    pub(super) fn is_edge_dml_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        if starts_with_ci(t, "INSERT")
            || starts_with_ci(t, "UPDATE")
            || starts_with_ci(t, "DELETE")
            || starts_with_ci(t, "MERGE")
            || starts_with_ci(t, "CREATE")
            || starts_with_ci(t, "DROP")
            || starts_with_ci(t, "ALTER")
            || starts_with_ci(t, "TRUNCATE")
            || starts_with_ci(t, "GRANT")
            || starts_with_ci(t, "REVOKE")
        {
            return true;
        }
        if starts_with_ci(t, "COPY") {
            return Self::contains_word_ci(t, "from");
        }
        if starts_with_ci(t, "WITH") {
            return Self::contains_word_ci(t, "insert")
                || Self::contains_word_ci(t, "update")
                || Self::contains_word_ci(t, "delete")
                || Self::contains_word_ci(t, "merge");
        }
        false
    }

    /// Union the invalidation table set for an extended-protocol batch from
    /// the per-statement metadata memoized at Parse time. Returns `None`
    /// when the batch references no DML statement; `Some(vec![])` (the
    /// invalidate-everything wildcard) when any referenced DML has an
    /// unattributable table set.
    #[cfg(feature = "edge-proxy")]
    pub(super) fn edge_extended_batch_tables(
        refs: &[String],
        bound_unnamed: bool,
        named_meta: &HashMap<String, Option<Vec<String>>>,
        unnamed_meta: &Option<Vec<String>>,
    ) -> Option<Vec<String>> {
        let mut any_dml = false;
        let mut wipe_all = false;
        let mut union: Vec<String> = Vec::new();
        {
            let mut consider = |meta: &Option<Vec<String>>| {
                if let Some(tables) = meta {
                    any_dml = true;
                    if tables.is_empty() {
                        wipe_all = true;
                    } else {
                        for t in tables {
                            if !union.contains(t) {
                                union.push(t.clone());
                            }
                        }
                    }
                }
            };
            for name in refs {
                if let Some(meta) = named_meta.get(name) {
                    consider(meta);
                }
            }
            if bound_unnamed {
                consider(unnamed_meta);
            }
        }
        if !any_dml {
            None
        } else if wipe_all {
            Some(Vec::new())
        } else {
            Some(union)
        }
    }

    /// Version-stamp a completed write, drop matching local entries, and
    /// (home role) fan the invalidation out over SSE. An edge never
    /// broadcasts — the home versions writes, so the edge sweeps in the
    /// observed-home domain and lets the home's own event follow.
    #[cfg(feature = "edge-proxy")]
    pub(super) async fn edge_invalidate_write(
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        tables: Vec<String>,
    ) {
        if config.edge.role == crate::edge::EdgeRole::Home {
            let version = state.edge_cache.next_version();
            let dropped = state.edge_cache.invalidate(version, &tables);
            let (notified, pruned) = state
                .edge_registry
                .broadcast(crate::edge::InvalidationEvent {
                    seq: 0,
                    up_to_version: version,
                    tables,
                    committed_at: chrono::Utc::now().to_rfc3339(),
                    epoch: state.edge_cache.epoch(),
                })
                .await;
            tracing::debug!(
                target: "helios::edge",
                version,
                dropped,
                notified,
                pruned,
                "write invalidation broadcast to edges"
            );
        } else {
            // Edge-local sweep in the observed-home domain: every locally
            // cached entry is stamped at or below the observed version, so
            // this drops all entries for the touched tables. It also bumps
            // the invalidation epoch, rejecting in-flight read stores that
            // raced this write.
            let version = state.edge_cache.observed_home_version();
            let dropped = state.edge_cache.invalidate(version, &tables);
            tracing::debug!(
                target: "helios::edge",
                version,
                dropped,
                "write invalidated local edge cache (edge role — home broadcasts)"
            );
        }
    }

    /// Conservative classifier for the conditional-reset optimisation: could
    /// this forwarded simple-query SQL leave *session-level* state on the
    /// backend connection that `DISCARD ALL` would need to clear before another
    /// client reuses it (a `SET`/GUC, temp table, prepared statement, cursor
    /// WITH HOLD, `LISTEN`, advisory lock, session authorization, …)?
    ///
    /// Biased hard toward `true`. A false negative (calling a dirtying
    /// statement clean) would leak state to the next borrower — a correctness
    /// and security bug — so only statements *provably* session-neutral return
    /// `false`; everything ambiguous returns `true` (forcing the full reset,
    /// which is merely slower, never unsafe).
    ///
    /// Known, documented limitation: a `SELECT` that calls a user-defined
    /// function which internally runs `set_config(..., is_local => false)` or
    /// takes an advisory lock via an aliased path is NOT detectable from the
    /// SQL text. The direct forms (`set_config`, `pg_advisory*`, `nextval`,
    /// `setval`) ARE caught. This is why `skip_clean_reset` is opt-in and
    /// intended for autocommit/simple-protocol workloads.
    ///
    /// Also reused by the edge cache as its sticky session-eligibility
    /// gate: a session that leaves session state (GUCs, SET ROLE, temp
    /// objects) no longer matches the shared cache's key model and is
    /// permanently excluded from edge lookup/store.
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    pub(super) fn stmt_leaves_session_state(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim();
        if t.is_empty() {
            return false;
        }
        // Multiple statements in one simple-query string: a leading-keyword
        // check cannot vouch for what follows a `;`, so treat any non-trailing
        // `;` as dirtying. A `;` inside a string literal also trips this —
        // safe, merely an unnecessary reset.
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return true;
        }
        // The statement's leading keyword must be one that provably leaves no
        // session state. CREATE / SET / PREPARE / DECLARE / LISTEN / DISCARD /
        // RESET / GRANT / ALTER / LOCK / COPY / … are all absent here, so they
        // fall through to `true` (dirtying).
        let neutral_lead = starts_with_ci(core, "SELECT")
            || starts_with_ci(core, "INSERT")
            || starts_with_ci(core, "UPDATE")
            || starts_with_ci(core, "DELETE")
            || starts_with_ci(core, "WITH")
            || starts_with_ci(core, "VALUES")
            || starts_with_ci(core, "TABLE")
            || starts_with_ci(core, "SHOW")
            || starts_with_ci(core, "EXPLAIN")
            || starts_with_ci(core, "FETCH")
            || starts_with_ci(core, "BEGIN")
            || starts_with_ci(core, "START")
            || starts_with_ci(core, "COMMIT")
            || starts_with_ci(core, "END")
            || starts_with_ci(core, "ROLLBACK")
            || starts_with_ci(core, "ABORT")
            || starts_with_ci(core, "SAVEPOINT")
            || starts_with_ci(core, "RELEASE");
        if !neutral_lead {
            return true;
        }
        // A neutral-lead statement can still create session state:
        //  * `SELECT ... INTO [TEMP] t` (and the `WITH … SELECT … INTO` form)
        //    creates a table. The `INTO` keyword is matched as a whole word (so
        //    a column name like `into_total` does not trip it) and ONLY for
        //    SELECT/WITH leads — `INSERT INTO`, `UPDATE`, `DELETE` use `INTO`
        //    (or not) as ordinary syntax and leave no session state.
        //  * `set_config()` sets a GUC; `pg_advisory*` takes a session lock;
        //    `nextval`/`setval` touch the per-session sequence cache.
        if (starts_with_ci(core, "SELECT") || starts_with_ci(core, "WITH"))
            && Self::contains_word_ci(core, "into")
        {
            return true;
        }
        // Same one-pass-lowercase trick as `is_cacheable_read_sql`: rather
        // than 4 separate case-insensitive windowed scans over `core`,
        // lowercase it once and use plain `str::contains`.
        const DIRTY_TOKENS: [&str; 4] = ["set_config", "advisory", "nextval", "setval"];
        let lower = core.to_ascii_lowercase();
        DIRTY_TOKENS.iter().any(|tok| lower.contains(tok))
    }

    /// Case-insensitive whole-word (ASCII identifier-boundary) search — a match
    /// requires the token to be bounded by a non-`[A-Za-z0-9_]` char (or the
    /// string edge) on both sides, so a real SQL keyword like `INTO` is caught
    /// regardless of surrounding whitespace while an identifier substring
    /// (`into_total`) is not. Always compiled: the in-session TR statement
    /// classifier (`tr_classify`) uses it on every build.
    /// A statement a standby can run with the result the primary would give
    /// (up to replication lag): one SELECT / VALUES / TABLE / WITH that
    /// transaction replay classifies as a read — only side-effect-free
    /// built-ins and `tr_read_functions`, no `INTO`, no data-modifying CTE —
    /// without a row-locking clause (`FOR UPDATE` / `FOR SHARE` …, which needs
    /// a writable node) and without a call of a function that answers about
    /// the primary or fails during recovery ([`STANDBY_UNSAFE_FUNCTIONS`]).
    /// Everything else is never offloaded.
    pub(super) fn standby_safe_read(sql: &str, policy: &TrReadPolicy) -> bool {
        let t = sql.trim_start();
        let read_lead = Self::starts_with_word_ci(t, "SELECT")
            || Self::starts_with_word_ci(t, "VALUES")
            || Self::starts_with_word_ci(t, "TABLE")
            || Self::starts_with_word_ci(t, "WITH");
        if !read_lead || Self::tr_classify(sql, policy) != StmtKind::Read {
            return false;
        }
        if Self::contains_word_ci(t, "for")
            && (Self::contains_word_ci(t, "update") || Self::contains_word_ci(t, "share"))
        {
            return false;
        }
        let mut primary_bound = false;
        let scanned = crate::replay_sql::words(t, |w| {
            if w.call
                && STANDBY_UNSAFE_FUNCTIONS
                    .iter()
                    .any(|f| w.text.eq_ignore_ascii_case(f))
            {
                primary_bound = true;
                return false;
            }
            true
        });
        scanned.is_ok() && !primary_bound
    }

    /// Whether the session's named statement `name` is a standby-safe read,
    /// classified from the registry's canonical `Parse` and memoized. A memo
    /// entry is trusted only while it still describes the very `Parse` bytes
    /// the registry holds (a redefinition or a refused-batch undo replaces
    /// them); an unknown name is not offloaded.
    pub(super) fn named_standby_read(
        memo: &mut HashMap<String, (bytes::Bytes, bool)>,
        registry: &HashMap<String, bytes::Bytes>,
        name: &str,
        policy: &TrReadPolicy,
    ) -> bool {
        let Some(parse) = registry.get(name) else {
            return false;
        };
        if let Some((seen, safe)) = memo.get(name) {
            if seen.as_ptr() == parse.as_ptr() && seen.len() == parse.len() {
                return *safe;
            }
        }
        let safe =
            Self::parse_msg_sql(parse).is_some_and(|sql| Self::standby_safe_read(sql, policy));
        memo.insert(name.to_string(), (parse.clone(), safe));
        safe
    }

    /// A statement after which the session's reads stay on its home node: it
    /// leaves state that only the backend connection running it holds and
    /// that the proxy does not mirror onto standbys — a temporary object
    /// (`CREATE TEMP …`, `SELECT … INTO`), a cursor, a SQL `PREPARE`,
    /// `LISTEN`, `LOAD`, a GUC change through `set_config()` — or a
    /// multi-statement string, whose leading keyword vouches for nothing
    /// after the first `;`. Session `SET`/`RESET` are not here: they are
    /// tracked and mirrored.
    pub(super) fn pins_reads(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return true;
        }
        let kw = |w: &str| Self::starts_with_word_ci(core, w);
        if kw("DECLARE") || kw("LISTEN") || kw("LOAD") {
            return true;
        }
        if kw("PREPARE") && !kw("PREPARE TRANSACTION") {
            return true;
        }
        if kw("CREATE")
            && (Self::contains_word_ci(core, "temp") || Self::contains_word_ci(core, "temporary"))
        {
            return true;
        }
        if (kw("SELECT") || kw("WITH")) && Self::contains_word_ci(core, "into") {
            return true;
        }
        Self::contains_word_ci(core, "set_config")
    }

    pub(super) fn contains_word_ci(haystack: &str, word: &str) -> bool {
        let hb = haystack.as_bytes();
        let wb = word.as_bytes();
        if wb.is_empty() || hb.len() < wb.len() {
            return false;
        }
        let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        let mut i = 0;
        while i + wb.len() <= hb.len() {
            if hb[i..i + wb.len()].eq_ignore_ascii_case(wb) {
                let before_ok = i == 0 || !is_ident(hb[i - 1]);
                let after = i + wb.len();
                let after_ok = after == hb.len() || !is_ident(hb[after]);
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
        false
    }
}
