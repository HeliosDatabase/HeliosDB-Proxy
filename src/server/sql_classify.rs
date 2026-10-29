use super::*;

/// The first words of a statement known to change no rows, in a
/// multi-statement string ([`ProxyServer::edge_statement_changes_nothing`]):
/// a read (`WITH` and `EXPLAIN` once
/// [`ProxyServer::is_edge_dml_sql`] found no data-modifying verb and no
/// `ANALYZE` in them) or bare transaction control.
#[cfg(feature = "edge-proxy")]
const EDGE_QUIET_LEADS: &[&str] = &[
    "SELECT",
    "VALUES",
    "TABLE",
    "SHOW",
    "WITH",
    "EXPLAIN",
    "BEGIN",
    "START",
    "SAVEPOINT",
    "RELEASE",
    "ROLLBACK",
];

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
    /// - A string of more than one statement is read one statement at a
    ///   time, split where the caches' lexer finds a `;` outside literals,
    ///   quoted identifiers and comments (`sql_text::statements`, R6-2): it
    ///   invalidates when any statement does (a trailing write behind a
    ///   read-classified lead included; `fingerprint::write_tables` then
    ///   gives the every-entry set) and when any statement is not one known
    ///   to change no rows ([`Self::edge_statement_changes_nothing`]: a
    ///   read or bare transaction control), so a statement this classifier
    ///   does not list as a write still flushes there, as every
    ///   multi-statement string used to. A string of reads and bare
    ///   transaction control invalidates nothing (`SELECT a FROM t1; SELECT b
    ///   FROM t2` used to flush every edge entry). One the backends may split
    ///   differently (a backend-ambiguous rest) may hold anything: it
    ///   invalidates. So does one a routing hint or a plugin sent to the
    ///   primary as a write (`is_write` where the first word reads no write).
    ///   A string the lexer finds one statement in, its `;` inside a literal,
    ///   a quoted identifier or a comment (`COPY t TO STDOUT (DELIMITER
    ///   ';')`, `NOTIFY c, 'a;b'`), is read as that one statement, as it is
    ///   without the `;`: it used to flush whenever that statement was not a
    ///   read.
    /// - `COPY ... FROM` loads rows but is not classified `is_write` (it
    ///   must not be re-routed); catch it here for invalidation.
    /// - The statement is read from its first word past leading comments
    ///   (`sql_text::lead`, the lexer the caches share), so
    ///   `/* job */ UPDATE …` and `-- v2` newline `CREATE OR REPLACE VIEW …`
    ///   are writes, and a `MERGE` (`MERGE INTO … USING …`, HeliosDB-Nano's
    ///   `MERGE BRANCH src INTO dst`) is one too ([`Self::is_edge_dml_sql`]).
    ///   When the backends may end a leading comment in different places
    ///   (`Lead::Rest`), or the first word holds a non-ASCII character other
    ///   than a space (HeliosDB-Lite reads `ınsert` as `INSERT`:
    ///   `journal_capture::first_word_not_ascii`), the statement may be
    ///   anything: a write of unknown scope.
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
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if !multi_stmt {
            return Self::edge_statement_needs_invalidation(is_write, core);
        }
        let Some(statements) = crate::sql_text::statements(core) else {
            return true;
        };
        if statements.len() < 2 {
            return Self::edge_statement_needs_invalidation(
                is_write,
                statements.first().copied().unwrap_or(core),
            );
        }
        (is_write && !Self::is_write_query(sql))
            || statements.into_iter().any(|statement| {
                Self::edge_statement_needs_invalidation(false, statement)
                    || !Self::edge_statement_changes_nothing(statement)
            })
    }

    /// Whether the one statement `statement` (of a multi-statement string,
    /// past [`Self::edge_statement_needs_invalidation`]) is known to change
    /// no rows: its first word past leading comments is one of
    /// [`EDGE_QUIET_LEADS`], a read or bare transaction control. Any other
    /// statement may write (`REFRESH MATERIALIZED VIEW`, a statement a
    /// backend adds), so its string flushes every edge entry.
    #[cfg(feature = "edge-proxy")]
    fn edge_statement_changes_nothing(statement: &str) -> bool {
        crate::sql_text::statement_start(statement).is_some_and(|lead| {
            EDGE_QUIET_LEADS
                .iter()
                .any(|word| crate::sql_text::starts_with_keyword(lead, word))
        })
    }

    /// [`Self::edge_write_needs_invalidation`] for the one statement `core`
    /// (without its trailing `;`), `is_write` its routing class.
    #[cfg(feature = "edge-proxy")]
    fn edge_statement_needs_invalidation(is_write: bool, core: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let lead = match crate::sql_text::lead(core) {
            crate::sql_text::Lead::Start(start) => start,
            crate::sql_text::Lead::Rest => return true,
            crate::sql_text::Lead::Nothing => core,
        };
        if crate::journal_capture::first_word_not_ascii(lead) {
            return true;
        }
        if starts_with_ci(lead, "BEGIN")
            || starts_with_ci(lead, "START")
            || starts_with_ci(lead, "SAVEPOINT")
            || starts_with_ci(lead, "RELEASE")
            || starts_with_ci(lead, "ROLLBACK")
        {
            return false;
        }
        is_write
            || Self::is_write_query(lead)
            || Self::is_edge_dml_sql(lead)
            || Self::is_edge_copy_write_sql(lead)
            || Self::is_edge_procedural_sql(lead)
            || Self::is_edge_txn_end_sql(lead)
    }

    /// Does the simple-query string carry more than one statement? A `;`
    /// before the optional trailing one means a leading-keyword check cannot
    /// vouch for what follows. (A `;` inside a string literal also trips this —
    /// conservative, and only ever costs an extra invalidation/reset.)
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
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
    /// data-modifying CTE verbs among the words the caches' lexer reads
    /// outside literals, quoted identifiers and comments
    /// (`sql_text::may_hold_keyword`: `WITH r AS (SELECT … WHERE op =
    /// 'delete') …` is a read, and used to flush its tables on every run),
    /// upper-cased the Unicode way when a word is not ASCII, as
    /// HeliosDB-Lite matches keywords (`ınsert` is `INSERT` there); text the
    /// backends may read differently (a backend-ambiguous rest) may hold
    /// one. The verb
    /// is the first word past leading comments (`sql_text::lead`); a leading
    /// comment the backends may end in different places (`Lead::Rest`)
    /// hides it, and a first word holding a non-ASCII character other than a
    /// space (`journal_capture::first_word_not_ascii`) cannot be read as
    /// Lite reads it: either makes the statement DML of unknown scope. Every
    /// verb the journal capture counts as a write but `CALL` and `DO`
    /// ([`Self::is_edge_procedural_sql`]) is here (`REFRESH MATERIALIZED
    /// VIEW`, `COMMENT`, `SECURITY LABEL`, `IMPORT FOREIGN SCHEMA` and
    /// `REASSIGN OWNED` included, whose scope `fingerprint::write_tables`
    /// does not know), and so is an `EXPLAIN` holding `ANALYZE` (or
    /// `ANALYSE`) outside its literals and comments, which runs the
    /// statement it explains.
    #[cfg(feature = "edge-proxy")]
    pub(super) fn is_edge_dml_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = match crate::sql_text::lead(sql) {
            crate::sql_text::Lead::Start(start) => start,
            crate::sql_text::Lead::Rest => return true,
            crate::sql_text::Lead::Nothing => return false,
        };
        if crate::journal_capture::first_word_not_ascii(t) {
            return true;
        }
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
            || starts_with_ci(t, "REFRESH")
            || starts_with_ci(t, "COMMENT")
            || starts_with_ci(t, "SECURITY")
            || starts_with_ci(t, "IMPORT")
            || starts_with_ci(t, "REASSIGN")
        {
            return true;
        }
        if starts_with_ci(t, "COPY") {
            return Self::contains_word_ci(t, "from");
        }
        if starts_with_ci(t, "WITH") {
            return crate::sql_text::may_hold_keyword(t, &["INSERT", "UPDATE", "DELETE", "MERGE"]);
        }
        if starts_with_ci(t, "EXPLAIN") {
            return crate::sql_text::may_hold_keyword(t, &["ANALYZE", "ANALYSE"]);
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
        let t = crate::protocol::skip_leading_comments(sql);
        let read_lead = Self::starts_with_word_ci(t, "SELECT")
            || Self::starts_with_word_ci(t, "VALUES")
            || Self::starts_with_word_ci(t, "TABLE")
            || Self::starts_with_word_ci(t, "WITH");
        if !read_lead || Self::tr_classify(t, policy) != StmtKind::Read {
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
    /// tracked and mirrored. Leading comments (sqlcommenter tags) do not hide
    /// the statement.
    pub(super) fn pins_reads(sql: &str) -> bool {
        let t = crate::protocol::skip_leading_comments(sql).trim_end();
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
        // Token-exact checks outside literals, comments and quoted names:
        // `set_config(…)` and `… INTO TEMP[ORARY] …`.
        let is = |w: &str, k: &str| w.eq_ignore_ascii_case(k);
        let (mut set_config, mut into_temp, mut after_into) = (false, false, false);
        let scanned = crate::replay_sql::words(core, |w| {
            if w.call && is(w.text, "set_config") {
                set_config = true;
                return false;
            }
            into_temp |= !w.quoted && after_into && (is(w.text, "temp") || is(w.text, "temporary"));
            after_into = !w.quoted && is(w.text, "into");
            true
        });
        if scanned.is_err() || set_config {
            return true;
        }
        if (kw("SELECT") || kw("WITH")) && into_temp {
            return true;
        }
        // `CREATE [OR REPLACE] [GLOBAL | LOCAL] TEMP[ORARY] …`: leading
        // keywords only, where no literal can occur.
        if kw("CREATE") {
            let mut rest = core.split_whitespace().skip(1).peekable();
            if rest.peek().is_some_and(|w| is(w, "or")) {
                rest.next();
                rest.next(); // REPLACE
            }
            if rest
                .peek()
                .is_some_and(|w| is(w, "global") || is(w, "local"))
            {
                rest.next();
            }
            return rest
                .next()
                .is_some_and(|w| is(w, "temp") || is(w, "temporary"));
        }
        false
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
