use super::*;

// ---------------------------------------------------------------------------
// In-session Transaction Replay (`tr_mode`)
//
// Transparent failover of a LIVE client session when its backend connection
// fails: the forward path reports the fault (`BackendFault`), a pure decision
// table (`tr_decide`) picks an action from the configured `TrMode`, and a thin
// async orchestrator (`tr_handle_fault`) executes it — reconnecting to a
// healthy primary, restoring session state, replaying the recorded explicit
// transaction and/or re-executing the interrupted request, or returning ONE
// well-formed `ErrorResponse` instead of dropping the socket.
//
// Hard rules encoded in the table: a write whose outcome is unknown is never
// re-executed except as part of `transaction` mode's replay of an UNCOMMITTED
// transaction (the original copy died uncommitted with the old backend), and
// a COMMIT whose outcome is unknown is never retried.
// ---------------------------------------------------------------------------

/// Where a backend fault struck relative to the in-flight request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FaultPhase {
    /// Dispatch has not begun (e.g. connect failure or an idle socket loss),
    /// or a single Query frame was not completely delivered. Never use this
    /// for a failed extended-batch write: complete prefix frames may have run.
    NotDelivered,
    /// Dispatch was attempted, or the response was lost before ReadyForQuery.
    /// The backend may have processed the whole request or complete prefix
    /// frames of an extended batch; write_all failure does not establish zero delivery.
    OutcomeUnknown,
}

/// What the client has already seen from the interrupted response.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ResponseProgress {
    pub(super) bytes: u64,
    /// A CommandComplete or ErrorResponse was already published. Neither may be
    /// followed by a synthesized frame: a second ErrorResponse has no legal
    /// place, and an ErrorResponse after a CommandComplete would report failure
    /// for a statement the backend actually finished — a client that then
    /// retries an INSERT double-applies it. The socket closes instead, which is
    /// the one report a client cannot misread as "the statement did not run".
    pub(super) terminal: bool,
    /// Flush forwarding may have ended inside a frame.
    pub(super) raw: bool,
}

#[derive(Debug)]
pub(super) struct ResponseFailure {
    pub(super) error: ProxyError,
    pub(super) progress: ResponseProgress,
}

impl std::fmt::Display for ResponseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// A backend fault reported by the forward path to the session loop.
#[derive(Debug, Clone)]
pub(super) struct BackendFault {
    /// Address of the backend that failed.
    pub(super) node: String,
    pub(super) phase: FaultPhase,
    pub(super) progress: ResponseProgress,
    /// Populated by recovery after classifying the whole interrupted request.
    pub(super) kind: Option<StmtKind>,
    /// Human-readable cause (used in the client-visible error message).
    pub(super) error: String,
}

impl BackendFault {
    /// Record a fault into `slot` — unless the error is client-side (the client
    /// went away; there is nothing to recover for and the caller propagates).
    pub(super) fn set(
        slot: &mut Option<BackendFault>,
        node: &str,
        phase: FaultPhase,
        err: &ProxyError,
    ) {
        let error = err.to_string();
        if error.contains("Client") {
            return;
        }
        *slot = Some(BackendFault {
            node: node.to_string(),
            phase,
            progress: ResponseProgress::default(),
            kind: None,
            error,
        });
    }

    pub(super) fn set_response(
        slot: &mut Option<BackendFault>,
        node: &str,
        failure: &ResponseFailure,
    ) {
        Self::set(slot, node, FaultPhase::OutcomeUnknown, &failure.error);
        if let Some(fault) = slot {
            fault.progress = failure.progress;
        }
    }
}

/// Coarse classification of the statement a fault interrupted (and of every
/// statement recorded inside an explicit transaction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StmtKind {
    /// Side-effect-free read: `SELECT`/read-only `WITH`/`VALUES`/`TABLE`/`SHOW`/
    /// `COPY ... TO`/plain `EXPLAIN`.
    Read,
    /// Modifies data or schema (or may: data-modifying CTE, `COPY ... FROM`,
    /// `CALL`/`DO`/`EXECUTE`, `SELECT ... INTO`, a multi-statement string).
    Write,
    /// Makes a transaction durable: `COMMIT`/`END`/`PREPARE TRANSACTION`/
    /// `COMMIT PREPARED` (or a multi-statement string that may contain one).
    Commit,
    /// Idempotent session/transaction control with no data effect: `BEGIN`/
    /// `START`, `SAVEPOINT`, `RELEASE`, `ROLLBACK [TO]`, `ABORT`, `SET`/`RESET`,
    /// `DISCARD`, the empty query.
    Control,
    /// Anything else (`LISTEN`/`NOTIFY`, `LOCK`, cursors, `EXPLAIN ANALYZE`,
    /// `SELECT nextval(...)`, unknown verbs): conservatively treated like a
    /// write for re-execution purposes.
    Other,
}

/// What the proxy does about a backend fault (see `tr_decide`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TrAction {
    /// A response may be complete or mid-frame: close without appending bytes.
    CloseIncompleteResponse,
    /// Send `ErrorResponse(sqlstate)` + `ReadyForQuery`, then close the client.
    CloseWithError(&'static str),
    /// Re-home the session on a healthy primary, send ONE
    /// `ErrorResponse(sqlstate)` + `ReadyForQuery` for the in-flight request
    /// (aborting the client-visible transaction if inside one) and keep
    /// serving the session.
    ErrorAndContinue(&'static str),
    /// Re-home the session and transparently re-execute the in-flight request.
    Reexecute,
    /// Re-home the session, replay the recorded explicit transaction from its
    /// `BEGIN` (responses discarded), then re-execute the in-flight request
    /// inside it.
    ReplayThenReexecute,
}

/// The request a fault interrupted, borrowed so it can be re-executed.
pub(super) enum InFlight<'a> {
    Simple(&'a Message),
    Extended {
        batch: &'a [u8],
        route_sql: Option<&'a str>,
        wait_ready: bool,
        reprepare: &'a [String],
        defines: &'a [String],
        unnamed: Option<&'a (bytes::Bytes, bytes::Bytes)>,
    },
}

/// Why a transaction replay failed.
pub(super) enum ReplayFailure {
    /// The replacement backend's socket failed mid-replay.
    Backend(ProxyError),
    /// A replayed statement was rejected (or re-prepare rejected).
    Statement(String),
}

/// Flush-terminated extended-protocol frames of the cycle in progress,
/// accumulated until its `Sync` completes and the cycle is recorded as ONE
/// replay entry (a Flush yields no `ReadyForQuery`, so a replay must send the
/// whole cycle before draining once).
pub(super) struct TrExtCycle {
    pub(super) frames: BytesMut,
    pub(super) unnamed_parse: Option<bytes::Bytes>,
    pub(super) defines: Vec<String>,
    pub(super) refs: Vec<String>,
    pub(super) route_sql: Option<String>,
}

/// Per-session, loop-local state for in-session TR (no locks: only the
/// session's own task touches it).
pub(super) struct TrSession {
    /// Record statements of explicit transactions (`select`/`transaction`).
    pub(super) record_tx: bool,
    /// Track session `SET`/`RESET` for restore (`tr_mode != none`).
    pub(super) track_gucs: bool,
    /// Committed session-level `SET` statements to replay on a new backend, in
    /// order, one per DISTINCT variable (a later `SET` of the same variable
    /// replaces the earlier one), bounded by `[limits]
    /// tr_max_session_set_statements`.
    pub(super) gucs: Vec<String>,
    /// Tracking stopped at the cap — the restore would be incomplete, so a
    /// failover is refused rather than re-homing with partial state.
    pub(super) guc_cap_hit: bool,
    /// GUC operations issued inside the current explicit transaction, applied
    /// to `gucs` in order only if the transaction COMMITs. PostgreSQL rolls
    /// `SET`/`RESET`/`RESET ALL` back with the transaction, so must we.
    pub(super) pending_tx_gucs: Vec<GucOp>,
    /// Open savepoints of the current transaction: (name, length of
    /// `pending_tx_gucs` when it was taken). `ROLLBACK TO` truncates to it.
    pub(super) tx_savepoints: Vec<(String, usize)>,
    /// The session's backend socket died while idle: the next request is a
    /// not-delivered fault against this node.
    pub(super) lost_backend: Option<String>,
    /// The client-visible transaction was aborted by a failover: every
    /// statement except a transaction end is answered with 25P02 until the
    /// client issues ROLLBACK/COMMIT (which ends it as a ROLLBACK), so no
    /// statement of the dead transaction leaks into autocommit on the new
    /// backend.
    pub(super) tx_aborted: bool,
    /// `ReadyForQuery` status before the statement being recorded.
    pub(super) prev_status: u8,
    pub(super) ext_cycle: Option<TrExtCycle>,
    /// An open Flush cycle exceeded its recording budget before any RFQ.
    pub(super) ext_cycle_dropped: bool,
    /// A Flush has dispatched part of the current cycle, regardless of TR mode.
    pub(super) ext_dispatched: bool,
}

impl TrSession {
    pub(super) fn new(mode: TrMode) -> Self {
        Self {
            record_tx: matches!(mode, TrMode::Select | TrMode::Transaction),
            track_gucs: mode != TrMode::None,
            gucs: Vec::new(),
            guc_cap_hit: false,
            pending_tx_gucs: Vec::new(),
            tx_savepoints: Vec::new(),
            lost_backend: None,
            tx_aborted: false,
            prev_status: b'I',
            ext_cycle: None,
            ext_cycle_dropped: false,
            ext_dispatched: false,
        }
    }
}

impl ProxyServer {
    /// Unlike a single Query frame, a partially written extended batch may
    /// contain complete Execute/Sync frames. `write_all` and its timeout do not
    /// report a trustworthy zero-delivery guarantee (including buffered TLS).
    /// Conservatively preserve uncertainty even if the first write fails.
    pub(super) async fn tr_write_batch<W: tokio::io::AsyncWrite + Unpin>(
        stream: &mut W,
        batch: &[u8],
        write_timeout: Duration,
    ) -> std::result::Result<(), (ProxyError, FaultPhase)> {
        let message = match tokio::time::timeout(write_timeout, stream.write_all(batch)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => format!("Backend write error: {}", e),
            Err(_) => "Backend write timeout".to_string(),
        };
        Err((ProxyError::Network(message), FaultPhase::OutcomeUnknown))
    }

    /// Record the `ReadyForQuery` that closed a response: the hot-path
    /// `in_transaction` flag plus the raw status byte and whether the response
    /// carried an `ErrorResponse` (both read by the in-session TR bookkeeping).
    /// Publish the TR-06 observation of the response that just completed (or
    /// clear it when none was taken) for the recorder to attach to its entry.
    pub(super) fn note_observation(session: &ClientSession, obs: Option<&Observation>) {
        let (digest, unverifiable) = match obs {
            Some(o) => match o.finish() {
                Some(d) => (d, false),
                None => (0, true),
            },
            None => (0, false),
        };
        session
            .last_response_digest
            .store(digest, Ordering::Relaxed);
        session
            .last_response_unverifiable
            .store(unverifiable, Ordering::Relaxed);
    }

    /// A transaction whose isolation level pins a snapshot cannot be replayed:
    /// re-running it on another backend cannot reproduce that snapshot.
    /// Detects an explicit level on BEGIN/START TRANSACTION or a later SET
    /// TRANSACTION, and a session `default_transaction_isolation` above READ
    /// COMMITTED tracked in the restore set.
    pub(super) fn tr_snapshot_sensitive(sql: &str, gucs: &[String]) -> bool {
        let strict = |t: &str| {
            Self::contains_word_ci(t, "isolation")
                && (Self::contains_word_ci(t, "serializable")
                    || Self::contains_word_ci(t, "repeatable"))
        };
        let head = sql.trim_start();
        let opens = crate::protocol::starts_with_ci(head, "BEGIN")
            || crate::protocol::starts_with_ci(head, "START");
        if opens && strict(sql) {
            return true;
        }
        if crate::protocol::starts_with_ci(head, "SET")
            && Self::contains_word_ci(sql, "transaction")
            && (strict(sql) || Self::contains_word_ci(sql, "snapshot"))
        {
            return true;
        }
        if opens && !Self::contains_word_ci(sql, "isolation") {
            // Inherits the session default.
            return gucs.iter().any(|g| {
                Self::tr_guc_name(g).as_deref() == Some("default_transaction_isolation")
                    && (Self::contains_word_ci(g, "serializable")
                        || Self::contains_word_ci(g, "repeatable"))
            });
        }
        false
    }

    pub(super) fn note_ready_for_query(session: &ClientSession, status: u8, had_error: bool) {
        let st = TransactionStatus::from_byte(status);
        session.in_transaction.store(
            st != TransactionStatus::Idle,
            std::sync::atomic::Ordering::Relaxed,
        );
        session
            .last_rfq_status
            .store(st.to_byte(), std::sync::atomic::Ordering::Relaxed);
        session
            .last_response_error
            .store(had_error, std::sync::atomic::Ordering::Relaxed);
    }

    /// The pure in-session TR decision table. `in_tx` is the client-visible
    /// transaction state BEFORE the interrupted statement; `tx_has_writes` /
    /// `tx_replayable` describe the recorded transaction (irrelevant when
    /// `!in_tx`); `kind` classifies the interrupted statement.
    pub(super) fn tr_decide(
        mode: TrMode,
        phase: FaultPhase,
        in_tx: bool,
        tx_has_writes: bool,
        tx_replayable: bool,
        kind: StmtKind,
    ) -> TrAction {
        use TrAction::*;
        if mode == TrMode::None {
            return CloseWithError("57P01");
        }
        // Safe to run again even if the backend already ran it once.
        let idempotent = matches!(kind, StmtKind::Read | StmtKind::Control);
        // Can the recorded transaction be reproduced on the new backend in
        // this mode? `select` only ever replays READ-ONLY transactions (no
        // effect can be doubled); `transaction` replays any uncommitted one.
        let can_replay = tx_replayable
            && match mode {
                TrMode::Select => !tx_has_writes,
                TrMode::Transaction => true,
                TrMode::None | TrMode::Session => false,
            };
        match phase {
            FaultPhase::NotDelivered => {
                if !in_tx {
                    // Never ran, no transaction context lost: just run it.
                    return Reexecute;
                }
                // The transaction died with the old backend; the statement
                // itself never ran.
                if can_replay {
                    ReplayThenReexecute
                } else {
                    ErrorAndContinue("57P01")
                }
            }
            FaultPhase::OutcomeUnknown => {
                if mode == TrMode::Session {
                    return ErrorAndContinue("08007");
                }
                if !in_tx {
                    // Autocommit: a write may have been applied — never redo it.
                    return if idempotent {
                        Reexecute
                    } else {
                        ErrorAndContinue("08007")
                    };
                }
                // Inside an explicit transaction the old copy died UNCOMMITTED,
                // so nothing was applied — unless the in-flight statement was
                // the COMMIT itself, which may have landed.
                if kind == StmtKind::Commit {
                    return ErrorAndContinue("08007");
                }
                if can_replay {
                    ReplayThenReexecute
                } else {
                    ErrorAndContinue("08007")
                }
            }
        }
    }

    /// Never concatenate a replayed response to bytes already seen by the
    /// client. Complete row frames can be followed by one error; terminal
    /// responses and raw Flush fragments must end with a socket close. The
    /// `raw`/`terminal` tests are deliberately independent of the byte count:
    /// both already imply bytes were written, and predicating the close on that
    /// would leave the invariant resting on a coincidence of two other
    /// functions rather than on the flags themselves.
    pub(super) fn tr_response_action(
        action: TrAction,
        progress: ResponseProgress,
        prior_flush: bool,
    ) -> TrAction {
        if prior_flush || progress.raw || progress.terminal {
            return TrAction::CloseIncompleteResponse;
        }
        if progress.bytes > 0
            && matches!(action, TrAction::Reexecute | TrAction::ReplayThenReexecute)
        {
            return TrAction::ErrorAndContinue("08007");
        }
        action
    }

    /// Case-insensitive keyword prefix with an identifier boundary after it
    /// (`SET x` matches `SET`, `SETTINGS` does not).
    pub(super) fn starts_with_word_ci(s: &str, word: &str) -> bool {
        crate::protocol::starts_with_ci(s, word)
            && s.as_bytes()
                .get(word.len())
                .map(|&c| !(c.is_ascii_alphanumeric() || c == b'_'))
                .unwrap_or(true)
    }

    /// Classify one client statement for in-session TR (see `StmtKind`).
    /// Conservative by construction: anything not positively recognised as a
    /// read or as idempotent control is treated as a possible write.
    /// Whether `sql` durably commits the transaction it ends, so SETs issued
    /// inside that transaction survive. Distinct from `StmtKind::Commit`, which
    /// answers the wider replay-safety question.
    pub(super) fn tr_commits_session_state(sql: &str) -> bool {
        crate::replay_sql::boundaries(sql)
            .map(|b| b.may_commit && !b.ends_tx)
            .unwrap_or(false)
    }

    /// TR-03: can this read-shaped statement be re-executed on an unknown
    /// outcome? Only if every syntactic function call is a PostgreSQL built-in
    /// known to be side-effect-free (or operator-listed), no quoted identifier
    /// is called (its case is opaque to the policy), and the statement does not
    /// select INTO or consume a sequence. Anything else is `Other`: it may have
    /// run once already, so the proxy will not run it again.
    pub(super) fn tr_read_eligible(core: &str, policy: &TrReadPolicy) -> bool {
        if Self::contains_word_ci(core, "into") {
            return false;
        }
        let mut ok = true;
        let scanned = crate::replay_sql::words(core, |w| {
            if w.call && (w.quoted || !policy.allows_call(w.text)) {
                ok = false;
                return false;
            }
            true
        });
        scanned.is_ok() && ok
    }

    pub(super) fn tr_classify(sql: &str, policy: &TrReadPolicy) -> StmtKind {
        let Ok(boundaries) = crate::replay_sql::boundaries(sql) else {
            // The session's lexical rules or malformed input make commit
            // boundaries uncertain. Never replay a possibly durable outcome.
            return StmtKind::Commit;
        };
        if boundaries.may_commit {
            return StmtKind::Commit;
        }
        // Preserve the existing read/control eligibility restrictions. The
        // lexical guard strengthens commit detection; it must not silently
        // broaden the read subset before the volatility work in TR-03.
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.is_empty() {
            return StmtKind::Control;
        }
        if core.contains(';') {
            // Multi-statement string: opaque. If it may end a transaction it
            // must never be retried on an unknown outcome.
            return if Self::contains_word_ci(core, "commit")
                || Self::contains_word_ci(core, "end")
                || Self::contains_word_ci(core, "prepare")
            {
                StmtKind::Commit
            } else {
                StmtKind::Write
            };
        }
        let kw = |w: &str| Self::starts_with_word_ci(core, w);
        if kw("COMMIT") || kw("END") || kw("PREPARE TRANSACTION") {
            return StmtKind::Commit;
        }
        if kw("BEGIN")
            || kw("START")
            || kw("SAVEPOINT")
            || kw("RELEASE")
            || kw("ROLLBACK")
            || kw("ABORT")
            || kw("SET")
            || kw("RESET")
            || kw("DISCARD")
        {
            return StmtKind::Control;
        }
        if kw("SELECT") || kw("VALUES") || kw("TABLE") {
            return if Self::tr_read_eligible(core, policy) {
                StmtKind::Read
            } else {
                StmtKind::Other
            };
        }
        if kw("SHOW") {
            return StmtKind::Read;
        }
        if kw("WITH") {
            return if Self::contains_word_ci(core, "insert")
                || Self::contains_word_ci(core, "update")
                || Self::contains_word_ci(core, "delete")
                || Self::contains_word_ci(core, "merge")
            {
                StmtKind::Write
            } else if Self::tr_read_eligible(core, policy) {
                StmtKind::Read
            } else {
                StmtKind::Other
            };
        }
        if kw("COPY") {
            return if Self::contains_word_ci(core, "from") {
                StmtKind::Write
            } else {
                StmtKind::Read
            };
        }
        if kw("EXPLAIN") {
            return if Self::contains_word_ci(core, "analyze")
                || Self::contains_word_ci(core, "analyse")
            {
                StmtKind::Other
            } else {
                StmtKind::Read
            };
        }
        if kw("INSERT")
            || kw("UPDATE")
            || kw("DELETE")
            || kw("MERGE")
            || kw("CREATE")
            || kw("DROP")
            || kw("ALTER")
            || kw("TRUNCATE")
            || kw("GRANT")
            || kw("REVOKE")
            || kw("VACUUM")
            || kw("REINDEX")
            || kw("CLUSTER")
            || kw("CALL")
            || kw("DO")
            || kw("EXECUTE")
            || kw("REFRESH")
            || kw("IMPORT")
            || kw("COMMENT")
            || kw("SECURITY")
            || kw("ANALYZE")
        {
            return StmtKind::Write;
        }
        StmtKind::Other
    }

    /// A single-statement transaction end the aborted-transaction emulation
    /// accepts: `ROLLBACK`/`ABORT`/`COMMIT`/`END` (optionally `WORK`/
    /// `TRANSACTION`) — not `ROLLBACK TO`, not `* PREPARED`, not `AND CHAIN`.
    pub(super) fn tr_ends_transaction(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return false;
        }
        let mut words = core.split_ascii_whitespace();
        let Some(first) = words.next() else {
            return false;
        };
        let first_ok = ["ROLLBACK", "ABORT", "COMMIT", "END"]
            .iter()
            .any(|w| first.eq_ignore_ascii_case(w));
        if !first_ok {
            return false;
        }
        match words.next() {
            None => true,
            Some(w) => {
                (w.eq_ignore_ascii_case("WORK") || w.eq_ignore_ascii_case("TRANSACTION"))
                    && words.next().is_none()
            }
        }
    }

    /// Is this simple-protocol statement a session-level GUC change worth
    /// replaying onto a replacement backend? `SET LOCAL` / `SET TRANSACTION` /
    /// `SET CONSTRAINTS` are transaction-scoped and excluded.
    pub(super) fn tr_is_session_set(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return false;
        }
        if Self::starts_with_word_ci(core, "RESET") {
            return true;
        }
        if !Self::starts_with_word_ci(core, "SET") {
            return false;
        }
        let rest = core[3..].trim_start();
        !(Self::starts_with_word_ci(rest, "LOCAL")
            || Self::starts_with_word_ci(rest, "TRANSACTION")
            || Self::starts_with_word_ci(rest, "CONSTRAINTS"))
    }

    /// `RESET ALL` / `DISCARD ALL` wipe every tracked session GUC.
    pub(super) fn tr_resets_all(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        (Self::starts_with_word_ci(core, "RESET") || Self::starts_with_word_ci(core, "DISCARD"))
            && Self::contains_word_ci(core, "all")
    }

    /// SQL text of an encoded `Parse` message (5-byte header, name cstring,
    /// query cstring).
    pub(super) fn parse_msg_sql(parse_bytes: &[u8]) -> Option<&str> {
        let body = parse_bytes.get(5..)?;
        let name_end = body.iter().position(|&b| b == 0)?;
        crate::protocol::query_text(&body[name_end + 1..])
    }

    /// Classify every Execute in wire order, preserving Bind-time statement
    /// identity (replacing a Parse does not change an already bound portal).
    /// Borrow names from the bounded batch; never decode or rewrite Bind values.
    /// References outside this batch are deliberately opaque: the routing
    /// registry does not retain the acknowledged portal/statement generations.
    /// Such a reference may be a COMMIT and cannot authorize recovery.
    pub(super) fn tr_extended_kind(
        batch: &[u8],
        unnamed: Option<&[u8]>,
        max_bindings: usize,
        policy: &TrReadPolicy,
    ) -> StmtKind {
        Self::tr_extended_cycle_kind(&[batch], unnamed, max_bindings, policy)
    }

    pub(super) fn tr_extended_cycle_kind(
        batches: &[&[u8]],
        unnamed: Option<&[u8]>,
        max_bindings: usize,
        policy: &TrReadPolicy,
    ) -> StmtKind {
        let statement_kind = |sql: &str| -> StmtKind {
            let kind = ProxyServer::tr_classify(sql, policy);
            // ROLLBACK can expose subsequent Executes to autocommit. Reads and
            // writes need no second lexical pass; only possible controls do.
            if matches!(kind, StmtKind::Control | StmtKind::Other) {
                if let Ok(boundaries) = crate::replay_sql::boundaries(sql) {
                    if ProxyServer::starts_with_word_ci(boundaries.head, "ROLLBACK")
                        || ProxyServer::starts_with_word_ci(boundaries.head, "ABORT")
                    {
                        return StmtKind::Commit;
                    }
                }
            }
            kind
        };
        fn cstring<'a>(body: &mut &'a [u8]) -> Option<&'a [u8]> {
            let end = memchr::memchr(0, body)?;
            let value = &body[..end];
            *body = &body[end + 1..];
            Some(value)
        }
        let mut statements = HashMap::<&[u8], StmtKind>::new();
        let mut portals = HashMap::<&[u8], StmtKind>::new();
        // The common unnamed Parse/Bind/Execute shape needs no map allocation.
        let mut unnamed_statement = None;
        let mut unnamed_portal = None;
        if let Some(parse) = unnamed {
            let Some(sql) = Self::parse_msg_sql(parse) else {
                return StmtKind::Commit;
            };
            unnamed_statement = Some(statement_kind(sql));
        }
        let mut kind = StmtKind::Control;
        for batch in batches {
            let mut rest = *batch;
            while !rest.is_empty() {
                let Some(header) = rest.get(..5) else {
                    return StmtKind::Commit;
                };
                let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                if len < 4 {
                    return StmtKind::Commit;
                }
                let Some(frame_len) = len.checked_add(1) else {
                    return StmtKind::Commit;
                };
                let Some(mut body) = rest.get(5..frame_len) else {
                    return StmtKind::Commit;
                };
                match header[0] {
                    b'P' => {
                        let Some(name) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let Some(sql) =
                            cstring(&mut body).and_then(|b| std::str::from_utf8(b).ok())
                        else {
                            return StmtKind::Commit;
                        };
                        if name.is_empty() {
                            unnamed_statement = Some(statement_kind(sql));
                        } else {
                            if statements.len() >= max_bindings && !statements.contains_key(name) {
                                return StmtKind::Commit;
                            }
                            statements.insert(name, statement_kind(sql));
                        }
                    }
                    b'B' => {
                        let Some(portal) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let Some(name) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let bound_kind = if name.is_empty() {
                            unnamed_statement
                        } else {
                            statements.get(name).copied()
                        }
                        .unwrap_or(StmtKind::Commit);
                        if portal.is_empty() {
                            unnamed_portal = Some(bound_kind);
                        } else {
                            if portals.len() >= max_bindings && !portals.contains_key(portal) {
                                return StmtKind::Commit;
                            }
                            portals.insert(portal, bound_kind);
                        }
                    }
                    b'E' => {
                        let Some(portal) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let k = if portal.is_empty() {
                            unnamed_portal
                        } else {
                            portals.get(portal).copied()
                        }
                        .unwrap_or(StmtKind::Commit);
                        kind = match (kind, k) {
                            (StmtKind::Commit, _) | (_, StmtKind::Commit) => {
                                return StmtKind::Commit
                            }
                            (StmtKind::Write | StmtKind::Other, _)
                            | (_, StmtKind::Write | StmtKind::Other) => StmtKind::Write,
                            (StmtKind::Read, _) | (_, StmtKind::Read) => StmtKind::Read,
                            _ => StmtKind::Control,
                        };
                    }
                    b'C' => {
                        let Some((&target, mut name)) = body.split_first() else {
                            return StmtKind::Commit;
                        };
                        let Some(name) = cstring(&mut name) else {
                            return StmtKind::Commit;
                        };
                        match target {
                            b'S' if name.is_empty() => unnamed_statement = None,
                            b'P' if name.is_empty() => unnamed_portal = None,
                            b'S' => {
                                statements.remove(name);
                            }
                            b'P' => {
                                portals.remove(name);
                            }
                            _ => return StmtKind::Commit,
                        }
                    }
                    b'D' | b'H' | b'S' => {}
                    _ => return StmtKind::Commit,
                }
                rest = &rest[frame_len..];
            }
        }
        kind
    }

    /// Representative SQL for logging/bookkeeping only. Never use routing SQL
    /// as a recovery safety decision; a batch can execute several statements.
    pub(super) fn tr_extended_sql<'a>(
        route_sql: Option<&'a str>,
        refs: &[String],
        registry: &'a HashMap<String, bytes::Bytes>,
    ) -> Option<&'a str> {
        route_sql.or_else(|| {
            refs.iter()
                .find_map(|n| registry.get(n).and_then(|b| Self::parse_msg_sql(b)))
        })
    }

    /// The variable a session `SET`/`RESET` statement addresses, lowercased, so
    /// repeated `SET`s of one variable share a slot and `RESET` can find it.
    /// Special forms map to the GUC they set: `TIME ZONE` -> `timezone`,
    /// `SCHEMA` -> `search_path`, `NAMES` -> `client_encoding`, `SESSION
    /// AUTHORIZATION` -> `session_authorization`, `SEED` -> `seed`, `ROLE` ->
    /// `role`. `SET SESSION CHARACTERISTICS` has no single GUC and keys on the
    /// whole phrase.
    pub(super) fn tr_guc_name(sql: &str) -> Option<String> {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        let mut words = core.split_whitespace().peekable();
        let verb = words.next()?;
        if !(verb.eq_ignore_ascii_case("SET") || verb.eq_ignore_ascii_case("RESET")) {
            return None;
        }
        if words
            .peek()
            .is_some_and(|w| w.eq_ignore_ascii_case("SESSION"))
        {
            words.next();
            if words
                .peek()
                .is_some_and(|w| w.eq_ignore_ascii_case("AUTHORIZATION"))
            {
                return Some("session_authorization".to_string());
            }
            if words
                .peek()
                .is_some_and(|w| w.eq_ignore_ascii_case("CHARACTERISTICS"))
            {
                return Some("session characteristics".to_string());
            }
        }
        let first = words.next()?;
        let name = match first.to_ascii_lowercase().as_str() {
            "time" if words.peek().is_some_and(|w| w.eq_ignore_ascii_case("ZONE")) => {
                "timezone".to_string()
            }
            "schema" => "search_path".to_string(),
            "names" => "client_encoding".to_string(),
            "seed" => "seed".to_string(),
            "role" => "role".to_string(),
            other => other
                .split(|c: char| c == '=' || c.is_whitespace())
                .next()
                .unwrap_or(other)
                .trim_matches('"')
                .to_string(),
        };
        (!name.is_empty() && name != "all").then_some(name)
    }

    /// Apply a `SET` to the committed restore set: replace the variable's
    /// earlier statement in place, or append it honouring the cap.
    pub(super) fn tr_apply_set(tr: &mut TrSession, sql: &str, state: &Arc<ServerState>) {
        let name = Self::tr_guc_name(sql);
        if let Some(name) = name.as_deref() {
            if let Some(slot) = tr
                .gucs
                .iter()
                .position(|g| Self::tr_guc_name(g).as_deref() == Some(name))
            {
                tr.gucs[slot] = sql.to_string();
                return;
            }
        }
        Self::tr_push_guc(tr, sql, state);
    }

    /// Apply a `RESET <name>` to the committed restore set.
    pub(super) fn tr_apply_reset(tr: &mut TrSession, name: &str) {
        tr.gucs
            .retain(|g| Self::tr_guc_name(g).as_deref() != Some(name));
    }

    /// Apply one deferred transaction op at COMMIT.
    pub(super) fn tr_apply_guc_op(tr: &mut TrSession, op: GucOp, state: &Arc<ServerState>) {
        match op {
            GucOp::Set(sql) => Self::tr_apply_set(tr, &sql, state),
            GucOp::Reset(name) => Self::tr_apply_reset(tr, &name),
            GucOp::ResetAll => {
                tr.gucs.clear();
                tr.guc_cap_hit = false;
            }
        }
    }

    /// `SAVEPOINT s` / `RELEASE [SAVEPOINT] s` / `ROLLBACK TO [SAVEPOINT] s`
    /// bookkeeping for deferred GUC ops. Unquoted names fold to lowercase.
    pub(super) fn tr_note_savepoint(tr: &mut TrSession, sql: &str) {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        let mut w = core.split_whitespace();
        let Some(verb) = w.next() else { return };
        let fold = |n: &str| -> String {
            if let Some(q) = n.strip_prefix('"').and_then(|x| x.strip_suffix('"')) {
                q.replace("\"\"", "\"")
            } else {
                n.to_ascii_lowercase()
            }
        };
        if verb.eq_ignore_ascii_case("SAVEPOINT") {
            if let Some(name) = w.next() {
                tr.tx_savepoints
                    .push((fold(name), tr.pending_tx_gucs.len()));
            }
        } else if verb.eq_ignore_ascii_case("RELEASE") {
            let mut name = w.next();
            if name.is_some_and(|n| n.eq_ignore_ascii_case("SAVEPOINT")) {
                name = w.next();
            }
            if let Some(name) = name.map(fold) {
                // Release destroys the savepoint and every later one; the ops
                // made since stay part of the transaction.
                if let Some(i) = tr.tx_savepoints.iter().rposition(|(n, _)| *n == name) {
                    tr.tx_savepoints.truncate(i);
                }
            }
        } else if verb.eq_ignore_ascii_case("ROLLBACK") {
            if !w.next().is_some_and(|n| n.eq_ignore_ascii_case("TO")) {
                return;
            }
            let mut name = w.next();
            if name.is_some_and(|n| n.eq_ignore_ascii_case("SAVEPOINT")) {
                name = w.next();
            }
            if let Some(name) = name.map(fold) {
                // Undo the ops made after the savepoint; the savepoint itself
                // survives, later ones are destroyed.
                if let Some(i) = tr.tx_savepoints.iter().rposition(|(n, _)| *n == name) {
                    let len = tr.tx_savepoints[i].1;
                    tr.pending_tx_gucs.truncate(len);
                    tr.tx_savepoints.truncate(i + 1);
                }
            }
        }
    }

    /// Before re-homing: an incomplete restore set is not something to hand
    /// the client silently. Refuse the failover instead (surfaces as `08006`
    /// "session state could not be restored").
    pub(super) fn tr_restore_preflight(tr: &TrSession) -> Result<()> {
        if tr.guc_cap_hit {
            return Err(ProxyError::Protocol(
                "session SET tracking exceeded [limits] tr_max_session_set_statements; \
                 the session state cannot be restored on a replacement backend"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Append one tracked session `SET`, honouring the cap.
    pub(super) fn tr_push_guc(tr: &mut TrSession, sql: &str, state: &Arc<ServerState>) {
        if tr.guc_cap_hit {
            return;
        }
        if tr.gucs.len() >= state.limits.tr_max_session_set_statements {
            tr.guc_cap_hit = true;
            state
                .metrics
                .tr
                .session_set_cap_exceeded
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                limit = state.limits.tr_max_session_set_statements,
                "in-session TR: session SET tracking cap reached; failover restore will be incomplete"
            );
            return;
        }
        tr.gucs.push(sql.to_string());
    }

    /// Per-response in-session TR bookkeeping, run after every fully relayed
    /// simple-query or extended-protocol response. `sql` is the client's
    /// statement text (extended: the batch's resolved SQL); `entry` builds
    /// the replay record and is only invoked while inside an explicit
    /// transaction in `select`/`transaction` mode. `extended_kind` supplies
    /// the whole cycle's safety classification; None denotes simple protocol
    /// and enables GUC tracking (extended SETs are not tracked).
    ///
    /// Hot-path cost outside a transaction: a few prefix compares, no
    /// allocation, no lock.
    pub(super) async fn tr_after_response(
        tr: &mut TrSession,
        sql: Option<&str>,
        extended_kind: Option<StmtKind>,
        entry: impl FnOnce() -> StatementLog,
        entry_bytes: usize,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) {
        if !tr.record_tx && !tr.track_gucs {
            return;
        }
        // A COPY that yielded for client data has no ReadyForQuery yet; the
        // cycle is finished (and the status refreshed) by the CopyDone drain.
        if session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if tr.record_tx {
                let mut ts = session.tx_state.write().await;
                if tr.prev_status != b'I' || ts.in_transaction {
                    // A COPY inside the transaction: its data is not recorded.
                    ts.non_replayable = true;
                    ts.statements = Vec::new();
                    ts.replay_bytes = 0;
                }
            }
            return;
        }
        let status = session
            .last_rfq_status
            .load(std::sync::atomic::Ordering::Relaxed);
        let had_error = session
            .last_response_error
            .load(std::sync::atomic::Ordering::Relaxed);
        let prev_status = tr.prev_status;
        tr.prev_status = status;
        let now_in_tx = status != b'I';
        let was_in_tx = prev_status != b'I';
        let sql = sql.unwrap_or("");
        // Classify lazily: only needed inside a transaction or for a SET.
        let simple = extended_kind.is_none();
        let mut kind_cache = extended_kind;
        let kind = |kind_cache: &mut Option<StmtKind>| -> StmtKind {
            *kind_cache.get_or_insert_with(|| Self::tr_classify(sql, &state.tr_read_policy))
        };

        // --- session GUC tracking (simple protocol only) ---
        if tr.track_gucs && simple {
            let is_set_like = crate::protocol::starts_with_ci(sql.trim_start(), "SET")
                || crate::protocol::starts_with_ci(sql.trim_start(), "RESET")
                || crate::protocol::starts_with_ci(sql.trim_start(), "DISCARD");
            if is_set_like && !had_error {
                // Inside a transaction every GUC change is deferred: PostgreSQL
                // rolls SET/RESET/RESET ALL back with the transaction, so the
                // restore set must not learn them until COMMIT.
                let op = if Self::tr_resets_all(sql) {
                    Some(GucOp::ResetAll)
                } else if Self::tr_is_session_set(sql) {
                    if crate::protocol::starts_with_ci(sql.trim_start(), "RESET") {
                        Self::tr_guc_name(sql).map(GucOp::Reset)
                    } else {
                        Some(GucOp::Set(sql.to_string()))
                    }
                } else {
                    None
                };
                if let Some(op) = op {
                    if now_in_tx {
                        tr.pending_tx_gucs.push(op);
                    } else {
                        Self::tr_apply_guc_op(tr, op, state);
                    }
                }
            } else if now_in_tx && !had_error {
                let head = sql.trim_start();
                if crate::protocol::starts_with_ci(head, "SAVEPOINT")
                    || crate::protocol::starts_with_ci(head, "RELEASE")
                    || crate::protocol::starts_with_ci(head, "ROLLBACK")
                {
                    Self::tr_note_savepoint(tr, sql);
                }
            }
            if was_in_tx && !now_in_tx {
                // Transaction ended: SETs made inside it persist only if it
                // committed (a failed transaction's COMMIT is a ROLLBACK).
                // `StmtKind::Commit` is the replay-safety classification: it is
                // deliberately wide and also covers `ROLLBACK; <DML>`, where the
                // trailing DML commits in autocommit but the transaction's own
                // session state was DISCARDED. Promoting that transaction's SETs
                // would restore settings the database never kept, so the durable
                // commit is confirmed against the statement itself.
                let committed = prev_status == b'T'
                    && !had_error
                    && kind(&mut kind_cache) == StmtKind::Commit
                    && Self::tr_commits_session_state(sql);
                if committed {
                    let pending = std::mem::take(&mut tr.pending_tx_gucs);
                    for op in pending {
                        Self::tr_apply_guc_op(tr, op, state);
                    }
                } else {
                    tr.pending_tx_gucs.clear();
                }
                tr.tx_savepoints.clear();
            }
        }

        // --- explicit-transaction statement recording ---
        if !now_in_tx {
            session.tr_replay_tainted.store(false, Ordering::Relaxed);
        }
        if !tr.record_tx || (!now_in_tx && !was_in_tx) {
            return;
        }
        let mut ts = session.tx_state.write().await;
        if !now_in_tx {
            // Transaction over (COMMIT/ROLLBACK/implicit): release the record.
            *ts = TransactionState::default();
            return;
        }
        if !was_in_tx {
            // Idle -> InTx: this statement opened the transaction.
            *ts = TransactionState::default();
            ts.in_transaction = true;
            ts.tx_id = Some(Uuid::new_v4());
            ts.read_only = true;
        }
        let k = kind(&mut kind_cache);
        if !matches!(k, StmtKind::Read | StmtKind::Control) {
            ts.has_writes = true;
            ts.read_only = false;
        }
        let tainted = session
            .tr_replay_tainted
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        if status == b'E' || tainted || k == StmtKind::Commit {
            // A failed transaction can only be rolled back; a transformed
            // statement's recorded text is not what executed.
            ts.non_replayable = true;
        }
        if ts.non_replayable {
            if !ts.statements.is_empty() {
                ts.statements = Vec::new();
                ts.replay_bytes = 0;
            }
            return;
        }
        if ts.statements.len() >= state.limits.tr_max_replay_statements
            || ts.replay_bytes.saturating_add(entry_bytes) > state.limits.tr_max_replay_bytes
        {
            ts.non_replayable = true;
            ts.statements = Vec::new();
            ts.replay_bytes = 0;
            state
                .metrics
                .tr
                .replay_cap_exceeded
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                target: "helios::tr",
                "transaction exceeded [limits] tr_max_replay_statements/bytes; marked non-replayable"
            );
            return;
        }
        // TR-06: a statement executed inside the transaction must carry a
        // verifiable observation, or the transaction cannot be replayed.
        let mut logged = entry();
        if prev_status == b'T' {
            if session
                .last_response_unverifiable
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                ts.non_replayable = true;
                ts.statements = Vec::new();
                ts.replay_bytes = 0;
                tracing::debug!(
                    target: "helios::tr",
                    "response exceeded [limits] tr_max_observation_bytes; transaction marked non-replayable"
                );
                return;
            }
            let d = session
                .last_response_digest
                .swap(0, std::sync::atomic::Ordering::Relaxed);
            logged.result_checksum = (d != 0).then_some(d);
        }
        if !sql.is_empty() && Self::tr_snapshot_sensitive(sql, &tr.gucs) {
            ts.non_replayable = true;
            ts.statements = Vec::new();
            ts.replay_bytes = 0;
            return;
        }
        ts.statements.push(logged);
        ts.replay_bytes += entry_bytes;
    }

    /// Bookkeeping after a simple-query response.
    pub(super) async fn tr_after_simple(
        tr: &mut TrSession,
        msg: &Message,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) {
        let incomplete_cycle = tr.ext_cycle.take().is_some();
        if std::mem::take(&mut tr.ext_cycle_dropped) || incomplete_cycle {
            // A simple Query can follow a Flush without Sync. The current
            // entry cannot represent that earlier extended prefix, so never
            // replay a transaction from this incomplete history.
            session.tr_replay_tainted.store(true, Ordering::Relaxed);
        }
        let sql = crate::protocol::query_text(&msg.payload);
        let bytes = sql.map(|s| s.len()).unwrap_or(0);
        Self::tr_after_response(
            tr,
            sql,
            None,
            || StatementLog {
                sql: sql.unwrap_or("").to_string(),
                params: Vec::new(),
                result_checksum: None,
                executed_at: chrono::Utc::now(),
                extended: None,
            },
            bytes,
            session,
            state,
        )
        .await;
    }

    /// Bookkeeping after an extended-protocol batch. A Flush-terminated batch
    /// is accumulated into the open cycle; the Sync-terminated batch closes
    /// the cycle and records it as one replay entry.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tr_after_extended(
        tr: &mut TrSession,
        batch: &bytes::Bytes,
        unnamed: Option<&(bytes::Bytes, bytes::Bytes)>,
        route_sql: Option<&str>,
        wait_ready: bool,
        defines: &[String],
        refs: &[String],
        registry: &HashMap<String, bytes::Bytes>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) {
        if !tr.record_tx {
            return;
        }
        // A Flush may open BEGIN while the last RFQ still says Idle. Retain
        // that bounded prefix until Sync establishes the transaction state.
        let in_tx_context = tr.prev_status != b'I'
            || session
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed);
        if !in_tx_context && wait_ready {
            tr.ext_cycle = None;
            if std::mem::take(&mut tr.ext_cycle_dropped) {
                *session.tx_state.write().await = TransactionState::default();
            }
            return;
        }
        if !wait_ready {
            // A client can issue arbitrarily many Flushes without a Sync.
            // Apply the replay budget before retaining each chunk, rather
            // than only when the completed cycle becomes a history entry.
            let mut ts = session.tx_state.write().await;
            let retained = tr.ext_cycle.as_ref().map_or(0, |c| {
                c.frames
                    .len()
                    .saturating_add(c.unnamed_parse.as_ref().map_or(0, |p| p.len()))
            });
            let projected = ts
                .replay_bytes
                .saturating_add(retained)
                .saturating_add(batch.len())
                .saturating_add(unnamed.map_or(0, |(p, _)| p.len()));
            if !ts.non_replayable
                && (projected > state.limits.tr_max_replay_bytes
                    || ts.statements.len() >= state.limits.tr_max_replay_statements)
            {
                ts.non_replayable = true;
                ts.statements = Vec::new();
                ts.replay_bytes = 0;
                state
                    .metrics
                    .tr
                    .replay_cap_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            if ts.non_replayable {
                tr.ext_cycle = None;
                tr.ext_cycle_dropped = true;
                return;
            }
            drop(ts);
            let first_chunk = tr.ext_cycle.is_none();
            let cycle = tr.ext_cycle.get_or_insert_with(|| TrExtCycle {
                frames: BytesMut::new(),
                unnamed_parse: None,
                defines: Vec::new(),
                refs: Vec::new(),
                route_sql: None,
            });
            if let Some((p, _)) = unnamed {
                if first_chunk {
                    cycle.unnamed_parse = Some(p.clone());
                } else {
                    // A later optimized Parse replaces the unnamed statement
                    // at this point, after earlier Binds/Executes and Flushes.
                    cycle.frames.extend_from_slice(p);
                }
            }
            cycle.frames.extend_from_slice(batch);
            cycle.defines.extend(defines.iter().cloned());
            cycle.refs.extend(refs.iter().cloned());
            if cycle.route_sql.is_none() {
                cycle.route_sql = route_sql.map(|s| s.to_string());
            }
            return;
        }
        let cycle = tr.ext_cycle.take();
        let dropped = std::mem::take(&mut tr.ext_cycle_dropped);
        let cycle_route: Option<String> = cycle.as_ref().and_then(|c| c.route_sql.clone());
        let resolved = Self::tr_extended_sql(route_sql.or(cycle_route.as_deref()), refs, registry);
        // The first held Parse remains a prefix; every later held Parse is
        // retained at its actual position between chunks.
        let unnamed_len = cycle
            .as_ref()
            .and_then(|c| c.unnamed_parse.as_ref().map(|p| p.len()))
            .unwrap_or(0)
            .saturating_add(unnamed.map_or(0, |(p, _)| p.len()));
        let entry_bytes = batch
            .len()
            .saturating_add(cycle.as_ref().map_or(0, |c| c.frames.len()))
            .saturating_add(unnamed_len);
        Self::tr_after_response(
            tr,
            resolved,
            Some(match &cycle {
                _ if dropped => StmtKind::Commit,
                Some(c) => Self::tr_extended_cycle_kind(
                    &[
                        &c.frames,
                        unnamed.map_or(&[][..], |(p, _)| p.as_ref()),
                        batch,
                    ],
                    c.unnamed_parse.as_deref(),
                    state.limits.max_prepared_statements,
                    &state.tr_read_policy,
                ),
                None => Self::tr_extended_kind(
                    batch,
                    unnamed.map(|(p, _)| p.as_ref()),
                    state.limits.max_prepared_statements,
                    &state.tr_read_policy,
                ),
            }),
            || {
                let (frames, unnamed_parse, mut all_defines, mut all_refs) = match cycle {
                    Some(mut c) => {
                        if let Some((p, _)) = unnamed {
                            c.frames.extend_from_slice(p);
                        }
                        c.frames.extend_from_slice(batch);
                        (c.frames.freeze(), c.unnamed_parse, c.defines, c.refs)
                    }
                    None => (
                        batch.clone(),
                        unnamed.map(|(p, _)| p.clone()),
                        Vec::new(),
                        Vec::new(),
                    ),
                };
                all_defines.extend(defines.iter().cloned());
                all_refs.extend(refs.iter().cloned());
                StatementLog {
                    sql: resolved.unwrap_or("").to_string(),
                    params: Vec::new(),
                    result_checksum: None,
                    executed_at: chrono::Utc::now(),
                    extended: Some(ExtendedBatchLog {
                        frames,
                        unnamed_parse,
                        defines: all_defines,
                        refs: all_refs,
                    }),
                }
            },
            entry_bytes,
            session,
            state,
        )
        .await;
    }

    /// Finish the bookkeeping for a COPY cycle once its CopyDone/CopyFail
    /// drained to `ReadyForQuery`: refresh the status baseline and, if the
    /// COPY ran inside an explicit transaction, mark it non-replayable (its
    /// data stream is not recorded).
    pub(super) async fn tr_after_copy_drain(tr: &mut TrSession, session: &Arc<ClientSession>) {
        let status = session
            .last_rfq_status
            .load(std::sync::atomic::Ordering::Relaxed);
        tr.prev_status = status;
        if !tr.record_tx {
            return;
        }
        let mut ts = session.tx_state.write().await;
        if status == b'I' {
            *ts = TransactionState::default();
        } else {
            ts.in_transaction = true;
            ts.non_replayable = true;
            ts.has_writes = true;
            ts.read_only = false;
            ts.statements = Vec::new();
            ts.replay_bytes = 0;
        }
    }

    /// Build `CommandComplete(tag)`.
    pub(super) fn create_command_complete(tag: &str) -> Vec<u8> {
        crate::protocol::CommandComplete {
            tag: tag.to_string(),
        }
        .encode()
        .encode()
        .to_vec()
    }

    /// Write `ErrorResponse(code, message)` (+ `ReadyForQuery` when
    /// `with_ready`, status `E` inside a transaction else `I`) to the client.
    /// Returns bytes written.
    pub(super) async fn tr_send_error(
        client: &mut ClientStream,
        code: &str,
        message: &str,
        in_tx: bool,
        with_ready: bool,
    ) -> Result<u64> {
        let mut resp = Self::create_error_response(code, message);
        if with_ready {
            resp.extend_from_slice(&Self::create_ready_for_query(if in_tx {
                b'E'
            } else {
                b'I'
            }));
        }
        client
            .write_all(&resp)
            .await
            .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
        Ok(resp.len() as u64)
    }

    /// Read and discard backend frames up to and including one `ReadyForQuery`.
    /// Returns `(status byte, saw ErrorResponse)`. Every read is bounded by
    /// `read_timeout`. A COPY-in request from the backend cannot be satisfied
    /// here and is an error.
    pub(super) async fn drain_until_ready<S: AsyncReadExt + Unpin>(
        backend: &mut S,
        read_timeout: Duration,
        max_frame_bytes: usize,
        mut observe: Option<&mut Observation>,
    ) -> Result<(u8, bool)> {
        let mut buf = BytesMut::with_capacity(4096);
        let mut had_error = false;
        loop {
            let mut consumed = 0usize;
            let mut ready: Option<u8> = None;
            loop {
                let rem = &buf[consumed..];
                let Some(len) = backend_frame_len(rem, max_frame_bytes)? else {
                    break;
                };
                if rem.len() < len + 1 {
                    break;
                }
                let mtype = rem[0];
                let frame_total = len + 1;
                if let Some(obs) = observe.as_deref_mut() {
                    obs.note(&rem[..frame_total]);
                }
                consumed += frame_total;
                match mtype {
                    b'E' => had_error = true,
                    b'G' | b'W' => {
                        return Err(ProxyError::Protocol(
                            "backend requested COPY data during replay".to_string(),
                        ))
                    }
                    b'Z' => {
                        ready = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                        break;
                    }
                    _ => {}
                }
            }
            let _ = buf.split_to(consumed);
            if let Some(status) = ready {
                return Ok((status, had_error));
            }
            buf.reserve(4096);
            let n = tokio::time::timeout(read_timeout, backend.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("replay drain read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("replay drain read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "backend closed during replay".to_string(),
                ));
            }
        }
    }

    /// Run one simple-query statement on a backend socket and discard its
    /// response. `Err(Protocol)` if the backend rejected it; `Err(Network|
    /// Connection)` on a socket failure.
    pub(super) async fn tr_run_discard<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        sql: &str,
        write_timeout: Duration,
        read_timeout: Duration,
        max_frame_bytes: usize,
        expected: Option<(u64, usize)>,
    ) -> Result<u8> {
        let msg = crate::protocol::QueryMessage {
            query: sql.to_string(),
        }
        .encode()
        .encode();
        tokio::time::timeout(write_timeout, backend.write_all(&msg))
            .await
            .map_err(|_| ProxyError::Network("replay write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("replay write error: {}", e)))?;
        let mut obs = expected.map(|(_, cap)| Observation::new(cap));
        let (status, had_error) =
            Self::drain_until_ready(backend, read_timeout, max_frame_bytes, obs.as_mut()).await?;
        if had_error {
            return Err(ProxyError::Protocol(format!(
                "backend rejected replayed statement: {}",
                Self::tr_short_sql(sql)
            )));
        }
        if let (Some((want, _)), Some(obs)) = (expected, obs.as_ref()) {
            if obs.finish() != Some(want) {
                return Err(ProxyError::Protocol(format!(
                    "replayed statement returned a different result than the client observed: {}",
                    Self::tr_short_sql(sql)
                )));
            }
        }
        Ok(status)
    }

    /// Statement text abbreviated for log/error messages.
    pub(super) fn tr_short_sql(sql: &str) -> String {
        let t = sql.trim();
        if t.len() <= 80 {
            t.to_string()
        } else {
            let mut end = 80;
            while !t.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}...", &t[..end])
        }
    }

    /// Restore session-level state on a freshly dialed replacement
    /// connection: replay the tracked `SET`/`RESET` statements in order.
    /// Returns how many were replayed. `Err(Protocol)` if the backend rejected
    /// one (the session cannot be faithfully restored); `Err(Network|Connection)`
    /// on a socket failure.
    pub(super) async fn tr_restore_session_state<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        gucs: &[String],
        write_timeout: Duration,
        read_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<usize> {
        for sql in gucs {
            Self::tr_run_discard(
                backend,
                sql,
                write_timeout,
                read_timeout,
                max_frame_bytes,
                None,
            )
            .await?;
        }
        Ok(gucs.len())
    }

    /// Re-home the session: wait for a healthy primary (bounded by
    /// `write_timeout_secs`), dial it (startup params re-sent by
    /// `ensure_conn`), restore the tracked session GUCs, and make it the
    /// session's current node. A node that fails to connect or whose socket
    /// dies during restore is demoted and the wait continues until the
    /// deadline; a backend that *rejects* a restore statement aborts at once.
    pub(super) async fn tr_acquire_replacement(
        conns: &mut HashMap<String, BackendConn>,
        tr: &TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        deadline: tokio::time::Instant,
    ) -> Result<String> {
        let expired = || ProxyError::Network("recovery deadline exceeded".to_string());
        loop {
            let node = Self::select_primary_until(session, state, config, deadline).await?;
            let err = match tokio::time::timeout_at(
                deadline,
                Self::ensure_conn(conns, &node, session, config, state),
            )
            .await
            .unwrap_or_else(|_| Err(expired()))
            {
                Ok(()) => {
                    let bc = conns.get_mut(&node).expect("just ensured");
                    Self::tr_restore_preflight(tr)?;
                    match tokio::time::timeout_at(
                        deadline,
                        Self::tr_restore_session_state(
                            &mut bc.stream,
                            &tr.gucs,
                            state.limits.backend_write_timeout,
                            state.limits.backend_read_timeout,
                            state.limits.max_backend_frame_bytes,
                        ),
                    )
                    .await
                    .unwrap_or_else(|_| Err(expired()))
                    {
                        Ok(n) => {
                            #[cfg(feature = "pool-modes")]
                            if n > 0 {
                                bc.dirty = true;
                            }
                            *session.current_node.write().await = Some(node.clone());
                            state.metrics.tr.failovers.fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                target: "helios::tr",
                                node = %node,
                                restored_sets = n,
                                incomplete = tr.guc_cap_hit,
                                "in-session failover: session re-homed"
                            );
                            return Ok(node);
                        }
                        Err(e @ ProxyError::Protocol(_)) => {
                            return Err(e);
                        }
                        Err(e) => {
                            conns.remove(&node);
                            e
                        }
                    }
                }
                // A backend that challenges for a credential the proxy does
                // not hold (pass-through mode) or rejects it is healthy — do
                // not demote it, and do not wait: fail the recovery now.
                Err(e @ ProxyError::Auth(_)) => return Err(e),
                Err(e) => e,
            };
            Self::record_backend_failure(state, &node, &err.to_string());
            if tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Replay a recorded explicit transaction on `node`'s connection,
    /// discarding every response. Named statements a cycle references are
    /// re-prepared from the registry unless an earlier replayed cycle (or the
    /// connection) already holds them. Leaves the connection inside the
    /// replayed (uncommitted) transaction on success.
    pub(super) async fn tr_replay_transaction(
        conns: &mut HashMap<String, BackendConn>,
        node: &str,
        entries: &[StatementLog],
        registry: &HashMap<String, bytes::Bytes>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<(), ReplayFailure> {
        let bc = conns.get_mut(node).ok_or_else(|| {
            ReplayFailure::Backend(ProxyError::Connection("no connection".into()))
        })?;
        let wt = state.limits.backend_write_timeout;
        let rt = state.limits.backend_read_timeout;
        let mf = state.limits.max_backend_frame_bytes;
        let ob = state.limits.tr_max_observation_bytes;
        let total = entries.len();
        for (i, st) in entries.iter().enumerate() {
            let status = match &st.extended {
                None => Self::tr_run_discard(
                    &mut bc.stream,
                    &st.sql,
                    wt,
                    rt,
                    mf,
                    st.result_checksum.map(|d| (d, ob)),
                )
                .await
                .map_err(|e| match e {
                    ProxyError::Protocol(_) => ReplayFailure::Statement(format!(
                        "statement {}/{} rejected: {}",
                        i + 1,
                        total,
                        Self::tr_short_sql(&st.sql)
                    )),
                    other => ReplayFailure::Backend(other),
                })?,
                Some(ext) => {
                    for name in &ext.refs {
                        if bc.prepared.contains(name) || ext.defines.contains(name) {
                            continue;
                        }
                        let Some(parse_bytes) = registry.get(name) else {
                            continue;
                        };
                        Self::reprepare_statement(
                            &mut bc.stream,
                            parse_bytes,
                            state.limits.reprepare_timeout,
                            state.limits.max_backend_frame_bytes,
                        )
                        .await
                        .map_err(|e| match e {
                            ProxyError::Protocol(_) => ReplayFailure::Statement(format!(
                                "re-prepare of statement '{}' rejected",
                                name
                            )),
                            other => ReplayFailure::Backend(other),
                        })?;
                        bc.prepared.insert(name.clone());
                    }
                    let mut wire = Vec::with_capacity(
                        ext.frames.len() + ext.unnamed_parse.as_ref().map(|p| p.len()).unwrap_or(0),
                    );
                    if let Some(p) = &ext.unnamed_parse {
                        wire.extend_from_slice(p);
                    }
                    wire.extend_from_slice(&ext.frames);
                    tokio::time::timeout(wt, bc.stream.write_all(&wire))
                        .await
                        .map_err(|_| {
                            ReplayFailure::Backend(ProxyError::Network(
                                "replay write timeout".to_string(),
                            ))
                        })?
                        .map_err(|e| {
                            ReplayFailure::Backend(ProxyError::Network(format!(
                                "replay write error: {}",
                                e
                            )))
                        })?;
                    let mut obs = st.result_checksum.map(|_| Observation::new(ob));
                    let (status, had_error) =
                        Self::drain_until_ready(&mut bc.stream, rt, mf, obs.as_mut())
                            .await
                            .map_err(ReplayFailure::Backend)?;
                    if let (Some(want), Some(o)) = (st.result_checksum, obs.as_ref()) {
                        if o.finish() != Some(want) {
                            return Err(ReplayFailure::Statement(format!(
                                "extended batch {}/{} returned a different result than the client observed",
                                i + 1,
                                total
                            )));
                        }
                    }
                    if had_error {
                        return Err(ReplayFailure::Statement(format!(
                            "extended batch {}/{} rejected: {}",
                            i + 1,
                            total,
                            Self::tr_short_sql(&st.sql)
                        )));
                    }
                    for d in &ext.defines {
                        bc.prepared.insert(d.clone());
                    }
                    // The cycle may have (re)defined the unnamed statement.
                    bc.unnamed_sig = None;
                    status
                }
            };
            if status == b'E' {
                return Err(ReplayFailure::Statement(format!(
                    "statement {}/{} left the transaction in the failed state",
                    i + 1,
                    total
                )));
            }
        }
        Ok(())
    }

    /// Drop the session's recorded transaction (it died with the backend).
    pub(super) async fn tr_clear_tx(session: &Arc<ClientSession>) {
        *session.tx_state.write().await = TransactionState::default();
    }

    /// Routing found no healthy node within `write_timeout_secs`: tell the
    /// client (08006) instead of dropping the socket. The caller closes.
    pub(super) async fn send_no_healthy_nodes(
        client: &mut ClientStream,
        session: &Arc<ClientSession>,
        with_ready: bool,
    ) {
        let in_tx = session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed);
        let _ = Self::tr_send_error(
            client,
            "08006",
            "no healthy backend node available within write_timeout; connection closed",
            in_tx,
            with_ready,
        )
        .await;
    }

    /// Execute the in-session TR recovery for a backend fault. Returns
    /// `Ok(Some(forward_result))` when the session continues (the request was
    /// re-executed or answered with an error), `Ok(None)` when the client
    /// connection must be closed, `Err` only when the CLIENT socket failed.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tr_handle_fault(
        client: &mut ClientStream,
        conns: &mut HashMap<String, BackendConn>,
        current_node: &mut Option<String>,
        mut fault: BackendFault,
        inflight: InFlight<'_>,
        tr: &mut TrSession,
        registry: &HashMap<String, bytes::Bytes>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<Option<(Option<String>, u64)>> {
        // TR-06: ONE deadline for the whole recovery — primary wait, connect and
        // auth, session restore, replay — instead of per-phase timeouts that
        // could add up well beyond `write_timeout_secs`.
        let deadline = tokio::time::Instant::now() + config.write_timeout();
        let mode = session.tr_mode;
        let in_tx = session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed);
        let copying = session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed);
        let (kind, wait_ready) = match &inflight {
            InFlight::Simple(msg) => (
                crate::protocol::query_text(&msg.payload)
                    .map(|sql| Self::tr_classify(sql, &state.tr_read_policy))
                    .unwrap_or(StmtKind::Commit),
                true,
            ),
            InFlight::Extended {
                batch,
                wait_ready,
                unnamed,
                ..
            } => (
                Self::tr_extended_kind(
                    batch,
                    unnamed.map(|(p, _)| p.as_ref()),
                    state.limits.max_prepared_statements,
                    &state.tr_read_policy,
                ),
                *wait_ready,
            ),
        };
        fault.kind = Some(kind);
        let (tx_has_writes, tx_replayable) = {
            let ts = session.tx_state.read().await;
            (
                ts.has_writes,
                !ts.non_replayable && !ts.statements.is_empty(),
            )
        };
        let prior_flush = tr.ext_dispatched;
        if prior_flush {
            // The current chunk may be unsent, but earlier Executes in this
            // cycle may already have run (or even committed).
            fault.phase = FaultPhase::OutcomeUnknown;
        }
        let mut action = if copying {
            // The COPY data stream is unrecoverable in every mode.
            TrAction::CloseWithError("08006")
        } else {
            Self::tr_decide(mode, fault.phase, in_tx, tx_has_writes, tx_replayable, kind)
        };
        // A backend read timeout does not establish that execution stopped.
        if !Self::is_backend_fault(&fault.error)
            && matches!(action, TrAction::Reexecute | TrAction::ReplayThenReexecute)
        {
            action = TrAction::ErrorAndContinue(match fault.phase {
                FaultPhase::NotDelivered => "57P01",
                FaultPhase::OutcomeUnknown => "08007",
            });
        }
        action = Self::tr_response_action(action, fault.progress, prior_flush);
        state
            .metrics
            .bytes_sent
            .fetch_add(fault.progress.bytes, Ordering::Relaxed);
        tr.ext_cycle = None;
        tr.ext_cycle_dropped = false;
        tr.ext_dispatched = false;
        tracing::warn!(
            target: "helios::tr",
            node = %fault.node,
            error = %fault.error,
            phase = ?fault.phase,
            mode = ?mode,
            in_tx,
            kind = ?kind,
            action = ?action,
            "backend fault on a live session"
        );
        let phase_desc = match fault.phase {
            FaultPhase::NotDelivered => "before the statement was delivered",
            FaultPhase::OutcomeUnknown => "while the statement was in flight (outcome unknown)",
        };

        match action {
            TrAction::CloseIncompleteResponse => {
                Self::tr_clear_tx(session).await;
                Ok(None)
            }
            TrAction::CloseWithError(code) => {
                let message = if copying {
                    format!(
                        "backend {} failed during COPY ({}); connection closed",
                        fault.node, fault.error
                    )
                } else {
                    format!(
                        "backend {} failed {} ({}); closing connection (tr_mode = {:?})",
                        fault.node, phase_desc, fault.error, mode
                    )
                };
                let _ = Self::tr_send_error(client, code, &message, in_tx, wait_ready).await;
                Self::tr_clear_tx(session).await;
                Ok(None)
            }
            TrAction::ErrorAndContinue(code) => {
                let node =
                    match Self::tr_acquire_replacement(conns, tr, session, state, config, deadline)
                        .await
                    {
                        Ok(n) => n,
                        Err(e) => {
                            return Self::tr_fail_no_replacement(
                                client, &fault, &e, in_tx, wait_ready, session,
                            )
                            .await;
                        }
                    };
                let message = format!(
                    "backend {} failed {} ({}){}",
                    fault.node,
                    phase_desc,
                    fault.error,
                    if code == "08007" {
                        "; outcome unknown — verify the database outcome before retrying"
                    } else if in_tx {
                        "; the transaction was aborted — ROLLBACK and retry"
                    } else {
                        ""
                    }
                );
                if code == "08007" {
                    state
                        .metrics
                        .tr
                        .unknown_outcome_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                let sent = Self::tr_send_error(client, code, &message, in_tx, wait_ready).await?;
                Self::tr_enter_aborted(tr, in_tx, session).await;
                *current_node = Some(node.clone());
                Ok(Some((Some(node), sent)))
            }
            TrAction::Reexecute | TrAction::ReplayThenReexecute => {
                let node =
                    match Self::tr_acquire_replacement(conns, tr, session, state, config, deadline)
                        .await
                    {
                        Ok(n) => n,
                        Err(e) => {
                            return Self::tr_fail_no_replacement(
                                client, &fault, &e, in_tx, wait_ready, session,
                            )
                            .await;
                        }
                    };
                if action == TrAction::ReplayThenReexecute {
                    let entries = session.tx_state.read().await.statements.clone();
                    match tokio::time::timeout_at(
                        deadline,
                        Self::tr_replay_transaction(conns, &node, &entries, registry, state),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err(ReplayFailure::Backend(ProxyError::Network(
                            "recovery deadline exceeded during replay".to_string(),
                        )))
                    }) {
                        Ok(()) => {
                            state
                                .metrics
                                .tr
                                .transactions_replayed
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                target: "helios::tr",
                                node = %node,
                                statements = entries.len(),
                                "transaction replayed on new backend"
                            );
                        }
                        Err(failure) => {
                            state
                                .metrics
                                .tr
                                .replay_failures
                                .fetch_add(1, Ordering::Relaxed);
                            let detail = match failure {
                                ReplayFailure::Statement(d) => {
                                    // Leave the new backend clean.
                                    if let Some(bc) = conns.get_mut(&node) {
                                        let _ = Self::tr_run_discard(
                                            &mut bc.stream,
                                            "ROLLBACK",
                                            state.limits.backend_write_timeout,
                                            state.limits.backend_read_timeout,
                                            state.limits.max_backend_frame_bytes,
                                            None,
                                        )
                                        .await;
                                    }
                                    d
                                }
                                ReplayFailure::Backend(e) => {
                                    conns.remove(&node);
                                    Self::record_backend_failure(state, &node, &e.to_string());
                                    format!("replacement backend {} failed: {}", node, e)
                                }
                            };
                            let message =
                                format!("transaction replay failed after failover: {}", detail);
                            let sent =
                                Self::tr_send_error(client, "40001", &message, in_tx, wait_ready)
                                    .await?;
                            Self::tr_enter_aborted(tr, in_tx, session).await;
                            let cur = conns.contains_key(&node).then(|| node.clone());
                            *current_node = cur.clone();
                            return Ok(Some((cur, sent)));
                        }
                    }
                }
                state
                    .metrics
                    .tr
                    .statements_reexecuted
                    .fetch_add(1, Ordering::Relaxed);
                *current_node = Some(node.clone());
                let mut second: Option<BackendFault> = None;
                let r = match inflight {
                    InFlight::Simple(msg) => {
                        Self::forward_simple_query(
                            client,
                            msg,
                            conns,
                            Some(node.as_str()),
                            session,
                            state,
                            config,
                            &mut second,
                        )
                        .await
                    }
                    InFlight::Extended {
                        batch,
                        route_sql,
                        wait_ready,
                        reprepare,
                        defines,
                        unnamed,
                    } => {
                        Self::forward_extended_batch(
                            client,
                            batch,
                            route_sql,
                            wait_ready,
                            conns,
                            Some(node.as_str()),
                            registry,
                            reprepare,
                            defines,
                            unnamed,
                            session,
                            state,
                            config,
                            &mut second,
                        )
                        .await
                    }
                };
                match r {
                    Ok(v) => Ok(Some(v)),
                    Err(e) => {
                        let Some(f2) = second else {
                            // Client-side failure: nothing left to do.
                            return Err(e);
                        };
                        state
                            .metrics
                            .bytes_sent
                            .fetch_add(f2.progress.bytes, Ordering::Relaxed);
                        if Self::tr_response_action(
                            TrAction::ErrorAndContinue("08006"),
                            f2.progress,
                            false,
                        ) == TrAction::CloseIncompleteResponse
                        {
                            Self::tr_clear_tx(session).await;
                            return Ok(None);
                        }
                        // The replacement failed too: give this request up
                        // with one error rather than cascading recoveries.
                        tracing::warn!(
                            target: "helios::tr",
                            node = %f2.node,
                            error = %f2.error,
                            "replacement backend failed during re-execution"
                        );
                        let message = format!(
                            "backend {} failed during failover re-execution ({}); verify the database outcome before retrying",
                            f2.node, f2.error,
                        );
                        let code = if f2.phase == FaultPhase::OutcomeUnknown {
                            "08007"
                        } else {
                            "08006"
                        };
                        let sent =
                            Self::tr_send_error(client, code, &message, in_tx, wait_ready).await?;
                        Self::tr_enter_aborted(tr, in_tx, session).await;
                        *current_node = None;
                        Ok(Some((None, sent)))
                    }
                }
            }
        }
    }

    /// No healthy primary within `write_timeout_secs` (or the session state
    /// could not be restored): tell the client and close.
    pub(super) async fn tr_fail_no_replacement(
        client: &mut ClientStream,
        fault: &BackendFault,
        err: &ProxyError,
        in_tx: bool,
        wait_ready: bool,
        session: &Arc<ClientSession>,
    ) -> Result<Option<(Option<String>, u64)>> {
        let message = match err {
            ProxyError::Protocol(_) => format!(
                "backend {} failed ({}); session state could not be restored on the new primary: {}",
                fault.node, fault.error, err
            ),
            ProxyError::Auth(_) => format!(
                "backend {} failed ({}); the proxy could not authenticate to the replacement primary: {}",
                fault.node, fault.error, err
            ),
            _ => format!(
                "backend {} failed ({}); no healthy primary became available within write_timeout: {}",
                fault.node, fault.error, err
            ),
        };
        // An autocommit INSERT whose response was lost may already be durable,
        // exactly like a lost COMMIT: the client must be told to verify rather
        // than shown a bare connection failure it would reasonably retry. Only
        // reads and pure control statements can claim nothing was decided.
        let uncertain = fault.phase == FaultPhase::OutcomeUnknown
            && !matches!(fault.kind, Some(StmtKind::Read) | Some(StmtKind::Control));
        let (code, message) = if uncertain {
            (
                "08007",
                format!("{message}; verify the database outcome before retrying"),
            )
        } else {
            ("08006", message)
        };
        let _ = Self::tr_send_error(client, code, &message, in_tx, wait_ready).await;
        Self::tr_clear_tx(session).await;
        Ok(None)
    }

    /// After an error was returned for the in-flight request: the recorded
    /// transaction is gone; if the client believes it is inside one, enter
    /// the aborted-transaction emulation until it ends it.
    pub(super) async fn tr_enter_aborted(
        tr: &mut TrSession,
        in_tx: bool,
        session: &Arc<ClientSession>,
    ) {
        Self::tr_clear_tx(session).await;
        tr.pending_tx_gucs.clear();
        tr.tx_savepoints.clear();
        // The response the client just saw ended with ErrorResponse + RFQ.
        Self::note_ready_for_query(session, if in_tx { b'E' } else { b'I' }, true);
        Self::journal_discard(session, if in_tx { b'E' } else { b'I' });
        if in_tx {
            tr.tx_aborted = true;
        }
    }
}
