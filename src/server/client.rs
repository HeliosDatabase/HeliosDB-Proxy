use super::*;

use crate::tenant_slots::{QueryHold, QueryWait, TenantBudget, TenantQueryRefusal};

/// What the name of every function taking a session-level advisory lock
/// contains (`pg_advisory_lock`, `pg_try_advisory_lock_shared`, ...): the
/// cheap prefilter of [`ProxyServer::takes_session_lock`], before its
/// whole-word match.
const SESSION_LOCK_STEM: &str = "advisory_lock";

/// How the slow path of a tenant's active-query gate ended (ac858d61400b).
pub(super) enum QueryGateWait<'g> {
    /// A permit came free (`true`: after a wait).
    Admitted(tokio::sync::SemaphorePermit<'g>, bool),
    /// A single-statement simple-protocol `COMMIT PREPARED` /
    /// `ROLLBACK PREPARED`: runs without a permit.
    Exempt,
    /// No permit within `active_query_wait_ms`: 53300.
    Refused(TenantQueryRefusal),
    /// A CancelRequest named the session during the slow path (the wait, or
    /// the parking before it): 57014.
    Cancelled,
    /// The client went away during the wait: the session ends.
    ClientGone,
}

/// What the slow path of the active-query gate works with: the session's
/// client stream and read buffer (bytes that arrive during a wait are kept),
/// its backend connections (parked before a wait, probed after one) and the
/// loop state a probe that finds the backend gone updates.
pub(super) struct GateCtx<'a, 'g> {
    pub(super) gate: &'g TenantBudget,
    pub(super) client: &'a mut ClientStream,
    pub(super) buffer: &'a mut BytesMut,
    pub(super) conns: &'a mut HashMap<String, BackendConn>,
    pub(super) tr: &'a mut TrSession,
    /// The session's home node and current node.
    pub(super) nodes: [&'a mut Option<String>; 2],
    /// The node whose connection holds the session's open extended cycle,
    /// when one is open (see `park_idle_conns`).
    pub(super) cycle_node: Option<&'a str>,
    pub(super) offload_on: bool,
    pub(super) session: &'a Arc<ClientSession>,
    pub(super) state: &'a Arc<ServerState>,
    pub(super) config: &'a ProxyConfig,
}

/// The query-loop state of one session that outlives every exit from the
/// loop (`ProxyServer::client_loop`): its cached backend connections, its TR
/// state (the open extended cycle among it), the node holding that cycle,
/// and its hold on its tenant's active-query cap. Dropped in that order, so
/// the permit returns after the connections close.
pub(super) struct SessionHeld<'a> {
    pub(super) conns: HashMap<String, BackendConn>,
    pub(super) tr: TrSession,
    pub(super) cycle_node: Option<String>,
    pub(super) hold: Option<QueryHold<'a>>,
}

impl ProxyServer {
    /// PostgreSQL `ErrorResponse` bytes for a connection refused because the
    /// `[limits] max_client_connections` cap is saturated.
    ///
    /// SQLSTATE `53300` (`too_many_connections`) with PostgreSQL's own severity
    /// and wording (`FATAL`, `sorry, too many clients already`), so an
    /// off-the-shelf driver marks the connection dead and reports the same
    /// condition it would report against a PostgreSQL server at
    /// `max_connections` instead of seeing a bare TCP reset. Factored out so it
    /// can be asserted on without a socket.
    pub(super) fn over_capacity_error_bytes() -> Vec<u8> {
        Self::create_fatal_response("53300", "sorry, too many clients already")
    }

    /// Admission control for a client connection whose first startup-phase
    /// message has just been classified.
    ///
    /// `Ok(slot)` admits the connection (`None` = no cap configured, the
    /// default); `Err(())` means the cap is saturated and the caller must
    /// answer with [`Self::over_capacity_error_bytes`] and close.
    ///
    /// A `CancelRequest` NEVER consumes a slot: it always arrives as its own
    /// throwaway connection, it is answered without ever becoming a session,
    /// and refusing it would make query cancellation impossible exactly when
    /// the proxy is saturated — PostgreSQL likewise handles cancels in the
    /// postmaster without taking a `max_connections` slot.
    pub(super) async fn admit_client_slot(
        state: &Arc<ServerState>,
        first: &StartupMessage,
    ) -> std::result::Result<Option<tokio::sync::OwnedSemaphorePermit>, ()> {
        if matches!(first, StartupMessage::CancelRequest { .. }) {
            return Ok(None);
        }
        let Some(sem) = state.client_slots.as_ref() else {
            return Ok(None);
        };
        match state.limits.client_admission_wait {
            // Pre-H-05 behaviour: refuse immediately at the cap.
            None => match Arc::clone(sem).try_acquire_owned() {
                Ok(permit) => Ok(Some(permit)),
                Err(_) => {
                    state
                        .metrics
                        .connections_rejected
                        .fetch_add(1, Ordering::Relaxed);
                    Err(())
                }
            },
            // Bounded fair queue (H-05): wait up to the configured budget for
            // a permit before refusing, so a reconnect burst is absorbed
            // instead of answered with a wall of 53300s.
            Some(wait) => match Arc::clone(sem).try_acquire_owned() {
                Ok(permit) => Ok(Some(permit)),
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    state
                        .metrics
                        .admission_waited
                        .fetch_add(1, Ordering::Relaxed);
                    let waiting = std::time::Instant::now();
                    state
                        .metrics
                        .admission_queue_depth
                        .fetch_add(1, Ordering::Relaxed);
                    let acquired =
                        tokio::time::timeout(wait, Arc::clone(sem).acquire_owned()).await;
                    state
                        .metrics
                        .admission_queue_depth
                        .fetch_sub(1, Ordering::Relaxed);
                    state.latency.admission_wait.record(waiting.elapsed());
                    match acquired {
                        Ok(Ok(permit)) => Ok(Some(permit)),
                        _ => {
                            state
                                .metrics
                                .admission_timeouts
                                .fetch_add(1, Ordering::Relaxed);
                            state
                                .metrics
                                .connections_rejected
                                .fetch_add(1, Ordering::Relaxed);
                            Err(())
                        }
                    }
                }
                Err(tokio::sync::TryAcquireError::Closed) => {
                    state
                        .metrics
                        .connections_rejected
                        .fetch_add(1, Ordering::Relaxed);
                    Err(())
                }
            },
        }
    }

    /// Tell a client the proxy is at its connection cap, then close the stream.
    ///
    /// Bounded by the configured client write timeout, so a client that never
    /// reads cannot pin this task and the session slot it is being refused:
    /// worst case the frame is dropped and the connection closed anyway.
    pub(super) async fn refuse_over_capacity(stream: &mut ClientStream, write_timeout: Duration) {
        Self::refuse_with(stream, &Self::over_capacity_error_bytes(), write_timeout).await;
    }

    /// Send `msg` (a FATAL `ErrorResponse`) and close, bounded like
    /// [`Self::refuse_over_capacity`].
    pub(super) async fn refuse_with(
        stream: &mut ClientStream,
        msg: &[u8],
        write_timeout: Duration,
    ) {
        let _ = tokio::time::timeout(write_timeout, async {
            let _ = stream.write_all(msg).await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        })
        .await;
    }

    /// Per-tenant admission (H-05): take the session's slot under its
    /// tenant's client cap, read from the startup packet, before the
    /// process-wide slot, so sessions queued at their tenant's cap never hold
    /// process-wide capacity. Waits like [`Self::admit_client_slot`]
    /// (`[limits] client_admission_wait_secs`). `Ok(None)`: no cap applies
    /// (no capped tenant named, a `CancelRequest`, or no `multi-tenancy`).
    pub(super) async fn admit_tenant_slot(
        state: &Arc<ServerState>,
        first: &StartupMessage,
    ) -> std::result::Result<
        Option<tokio::sync::OwnedSemaphorePermit>,
        crate::tenant_slots::TenantRefusal,
    > {
        let StartupMessage::Startup { params, .. } = first else {
            return Ok(None);
        };
        if state.tenant_slots.is_empty() {
            return Ok(None);
        }
        let Some(tenant) = state.tenant_slots.tenant_of(params) else {
            return Ok(None);
        };
        let wait = state.limits.client_admission_wait;
        if wait.is_some() {
            state
                .metrics
                .admission_queue_depth
                .fetch_add(1, Ordering::Relaxed);
        }
        let acquired = state.tenant_slots.acquire(tenant, wait).await;
        if wait.is_some() {
            state
                .metrics
                .admission_queue_depth
                .fetch_sub(1, Ordering::Relaxed);
        }
        match acquired {
            Ok((slot, waited)) => {
                if let Some(w) = waited {
                    state.latency.admission_wait.record(w);
                }
                Ok(slot)
            }
            Err(refusal) => {
                state
                    .metrics
                    .connections_rejected
                    .fetch_add(1, Ordering::Relaxed);
                Err(refusal)
            }
        }
    }

    /// ac858d61400b: attach the request budgets of the tenant the startup
    /// packet names (its active-query cap, its replay-byte budget) to the
    /// session. Sessions of a tenant without one get nothing, so their
    /// request path never touches a budget.
    pub(super) fn attach_tenant_budget(
        state: &ServerState,
        session: &ClientSession,
        first: &StartupMessage,
    ) {
        let StartupMessage::Startup { params, .. } = first else {
            return;
        };
        if let Some(b) = state.tenant_slots.budget_of(params) {
            let _ = session
                .tenant_charge
                .set(crate::tenant_slots::TenantCharge::new(Arc::clone(b)));
        }
    }

    /// PostgreSQL `ErrorResponse` bytes for a session terminated by the
    /// `[limits] client_idle_timeout_secs` idle-session timeout.
    ///
    /// SQLSTATE `57P05` (`idle_session_timeout`) with PostgreSQL's own severity
    /// and wording (`FATAL`), so a driver marks the connection dead instead of
    /// trying to continue on it and then hitting a bare EOF.
    pub(super) fn idle_session_timeout_error_bytes() -> Vec<u8> {
        Self::create_fatal_response(
            "57P05",
            "terminating connection due to idle-session timeout",
        )
    }

    /// Tell a client its session is being reclaimed by the idle-session
    /// timeout. The caller then leaves the query loop, which closes the
    /// connection through the normal teardown path.
    ///
    /// The write is bounded by the configured client write timeout: a client
    /// that has stopped reading (a zero receive window, a vanished host —
    /// precisely the stall this timeout exists to reclaim) must not be able to
    /// pin the connection task, its session-map entry and its connection slot
    /// forever on the goodbye frame.
    pub(super) async fn terminate_idle_session(
        stream: &mut ClientStream,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        tracing::debug!(
            client = %session.client_addr,
            in_transaction = session
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed),
            timeout_secs = state
                .limits
                .client_idle_timeout
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "terminating idle client session (idle-session timeout)"
        );
        let emsg = Self::idle_session_timeout_error_bytes();
        let _ = tokio::time::timeout(state.limits.client_write_timeout, async {
            let _ = stream.write_all(&emsg).await;
            let _ = stream.flush().await;
        })
        .await;
    }

    /// PostgreSQL `ErrorResponse` bytes for a session terminated idle inside
    /// a transaction: SQLSTATE `25P03` (`idle_in_transaction_session_timeout`)
    /// with PostgreSQL's severity and wording.
    pub(super) fn idle_in_transaction_timeout_error_bytes() -> Vec<u8> {
        Self::create_fatal_response(
            "25P03",
            "terminating connection due to idle-in-transaction timeout",
        )
    }

    /// `[multi_tenancy] idle_permit_timeout_ms` (ac858d61400b): tell a client
    /// that sat idle holding one of its tenant's active-query permits (in a
    /// transaction, holding session-level advisory locks, or behind a
    /// plugin-blocked Query that left an extended cycle open) why its session
    /// ends: 25P03 in a transaction, 57P05 otherwise. Bounded like
    /// [`Self::terminate_idle_session`]; the caller leaves the query loop,
    /// which returns the permit.
    pub(super) async fn terminate_idle_permit_session(
        stream: &mut ClientStream,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        let in_tx = session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            target: "helios::tenant",
            client = %session.client_addr,
            tenant = session.tenant_budget().map_or("", |b| b.tenant()),
            in_transaction = in_tx,
            "terminating a session idle while holding its tenant's active-query permit (idle_permit_timeout_ms)"
        );
        let emsg = if in_tx {
            Self::idle_in_transaction_timeout_error_bytes()
        } else {
            Self::idle_session_timeout_error_bytes()
        };
        let _ = tokio::time::timeout(state.limits.client_write_timeout, async {
            let _ = stream.write_all(&emsg).await;
            let _ = stream.flush().await;
        })
        .await;
    }

    /// The backend connection holding the extended cycle the session's
    /// client is still in died while the session waited for that client
    /// (ac858d61400b), which may be waiting for answers no backend will give:
    /// tell it its session ends (FATAL 08006), bounded like
    /// [`Self::terminate_idle_session`]. The caller leaves the query loop,
    /// which returns the session's tenant permit.
    pub(super) async fn end_session_with_lost_cycle(
        stream: &mut ClientStream,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
        node: &str,
    ) {
        tracing::warn!(
            target: "helios::tr",
            client = %session.client_addr,
            node = %node,
            "backend connection holding the session's open extended-protocol cycle lost while its client waited; closing the session"
        );
        let emsg = Self::create_fatal_response(
            "08006",
            &format!(
                "backend {node} connection lost while an extended-protocol cycle was open on it (its outcome is unknown); connection closed"
            ),
        );
        let _ = tokio::time::timeout(state.limits.client_write_timeout, async {
            let _ = stream.write_all(&emsg).await;
            let _ = stream.flush().await;
        })
        .await;
    }

    /// Whether the backend answered the end of the session's extended COPY
    /// (`tr.ext_copy_end`): its CommandComplete, or the error that failed
    /// it, relayed by the idle watch into the request tally, which the Sync
    /// of the batch that started the COPY reset (its client then owes only
    /// its Sync, and waits for nothing).
    pub(super) fn copy_end_answered(session: &ClientSession) -> bool {
        session
            .cycle_completions
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
            || session
                .cycle_errored
                .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Return the session's active-query permit once the session is idle
    /// again (ac858d61400b): no transaction, no COPY, no extended cycle open
    /// or partly received (`cycle_open`), and no session-level advisory lock
    /// held. Called after the pool parked the connection, so the waiter the
    /// permit wakes finds it there.
    pub(super) fn tenant_query_release_if_idle(
        hold: &mut Option<QueryHold<'_>>,
        session: &ClientSession,
        cycle_open: bool,
        holds_session_locks: bool,
    ) {
        if let Some(h) = hold {
            if h.is_held()
                && !cycle_open
                && !holds_session_locks
                && !session
                    .in_transaction
                    .load(std::sync::atomic::Ordering::Relaxed)
                && !session
                    .copy_in_progress
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                h.release();
            }
        }
    }

    /// A single-statement `COMMIT PREPARED` / `ROLLBACK PREPARED`: it can
    /// only release prepared-transaction locks that permit holders may be
    /// blocked on, so it runs past its tenant's active-query cap.
    pub(super) fn tenant_gate_exempt(sql: &str) -> bool {
        let core = sql.trim_ascii_end();
        let single = memchr::memchr(b';', core.as_bytes()).is_none_or(|i| i + 1 == core.len());
        if !single {
            return false;
        }
        let Some(words) = Self::tr_lead_words(sql, 2) else {
            return false;
        };
        (Self::tr_word_is(sql, &words, 0, "COMMIT") || Self::tr_word_is(sql, &words, 0, "ROLLBACK"))
            && Self::tr_word_is(sql, &words, 1, "PREPARED")
    }

    /// Track session-level advisory locks under session pooling (they
    /// outlive ReadyForQuery 'I', so their holder keeps its active-query
    /// permit): a statement taking one ([`Self::takes_session_lock`]) sets
    /// `holds`; a call of `pg_advisory_unlock_all()`
    /// ([`Self::calls_unlock_all`]) or `DISCARD ALL` that succeeded clears
    /// it. `_xact_` locks end with their transaction and do not count. Called
    /// only for a simple query a backend answered, with the mark of the
    /// connection that answered it (`BackendConn::session_locks`):
    /// `last_response_error` describes that answer.
    pub(super) fn note_session_locks(holds: &mut bool, sql: &str, session: &ClientSession) {
        if Self::takes_session_lock(sql.as_bytes()) {
            *holds = true;
        } else if *holds
            && !session
                .last_response_error
                .load(std::sync::atomic::Ordering::Relaxed)
            && (Self::calls_unlock_all(sql)
                || Self::tr_lead_words(sql, 2).is_some_and(|w| {
                    Self::tr_word_is(sql, &w, 0, "DISCARD") && Self::tr_word_is(sql, &w, 1, "ALL")
                }))
        {
            *holds = false;
        }
    }

    /// Whether `sql` (a statement's text, or the protocol frames carrying
    /// it) may take a session-level advisory lock: it names one of the
    /// functions that take one, `pg_advisory_lock`, `pg_advisory_lock_shared`,
    /// `pg_try_advisory_lock` or `pg_try_advisory_lock_shared`, as a whole
    /// word (in any case). Anywhere in the text, string literals and comments
    /// included: a `DO` block's body, or a query string a function runs, can
    /// take one, so a name there counts (conservative). Other text that
    /// merely contains `advisory_lock` (a table `job_advisory_locks`, a column
    /// `advisory_lock_id`, a literal `'advisory_lock'`) takes none. The
    /// `_xact_` variants end with their transaction and are not matched.
    #[inline]
    pub(super) fn takes_session_lock(sql: &[u8]) -> bool {
        crate::protocol::contains_ci_bytes(sql, SESSION_LOCK_STEM)
            && Self::names_session_lock_function(sql)
    }

    /// The whole-word match behind [`Self::takes_session_lock`], once the
    /// text is known to contain [`SESSION_LOCK_STEM`]: each occurrence of the
    /// stem preceded by `pg_` or `pg_try_` and followed by nothing or
    /// `_shared`, with no identifier byte (a letter, digit, `_`, `$` or a
    /// non-ASCII byte, as PostgreSQL's identifiers have) on either side.
    #[cold]
    #[inline(never)]
    fn names_session_lock_function(sql: &[u8]) -> bool {
        const PREFIXES: [&[u8]; 2] = [b"pg_try_", b"pg_"];
        const SHARED: &[u8] = b"_shared";
        let stem = SESSION_LOCK_STEM.as_bytes();
        let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80;
        let word_ends = |at: usize| sql.get(at).is_none_or(|&c| !ident(c));
        let mut pos = 0;
        while let Some(off) = memchr::memchr2(b'a', b'A', &sql[pos..]) {
            let at = pos + off;
            pos = at + 1;
            if !sql
                .get(at..at + stem.len())
                .is_some_and(|w| w.eq_ignore_ascii_case(stem))
            {
                continue;
            }
            let starts_word = PREFIXES.iter().any(|prefix| {
                at.checked_sub(prefix.len()).is_some_and(|start| {
                    sql[start..at].eq_ignore_ascii_case(prefix)
                        && (start == 0 || !ident(sql[start - 1]))
                })
            });
            let end = at + stem.len();
            let shared = sql
                .get(end..end + SHARED.len())
                .is_some_and(|t| t.eq_ignore_ascii_case(SHARED));
            if starts_word && (word_ends(end) || (shared && word_ends(end + SHARED.len()))) {
                return true;
            }
        }
        false
    }

    /// Whether `sql` calls `pg_advisory_unlock_all()`, which ends every
    /// session-level advisory lock of its connection: the function's name as
    /// an unquoted word followed by `(`, outside string literals, quoted
    /// identifiers, dollar quotes and comments. Where that cannot be told
    /// (a backslash, which an escape string may use to hide a quote; a text
    /// the scan refuses) the locks are taken to be still held
    /// (conservative), as they are for a name inside a literal or a `DO`
    /// block.
    pub(super) fn calls_unlock_all(sql: &str) -> bool {
        if !crate::protocol::contains_ci(sql, "advisory_unlock_all") || sql.contains('\\') {
            return false;
        }
        let mut calls = false;
        let _ = crate::replay_sql::words(sql, |w| {
            calls = w.call && !w.quoted && w.text.eq_ignore_ascii_case("pg_advisory_unlock_all");
            !calls
        });
        calls
    }

    /// Whether the session holds session-level advisory locks that keep its
    /// active-query permit (`track`: a capped tenant under session pooling):
    /// one of its cached backend connections took one
    /// (`BackendConn::session_locks`). Locks a lost or replaced connection
    /// held ended with it, so they no longer count.
    pub(super) fn session_locks_held(track: bool, conns: &HashMap<String, BackendConn>) -> bool {
        track && conns.values().any(|bc| bc.session_locks)
    }

    /// Whether the session's open extended cycle keeps its active-query
    /// permit (the `cycle_open` part of [`Self::tenant_query_release_if_idle`]):
    /// a partly received one (`pending`, `held_unnamed`: `partial`), or one
    /// the backend holds open (a Flush dispatched part of it, or an extended
    /// COPY awaits its Sync). A cycle a refusal left open behind its client's
    /// ReadyForQuery (`refused_open`) keeps it only while the connection
    /// holding it (`cycle_node`, never parked while the cycle is open) is
    /// cached: one the session lost died with that connection, the backend
    /// rolled it back, and its idle client holds nothing but the fault its
    /// next request reports.
    pub(super) fn cycle_keeps_permit(
        partial: bool,
        tr: &TrSession,
        refused_open: bool,
        cycle_node: Option<&str>,
        conns: &HashMap<String, BackendConn>,
    ) -> bool {
        partial
            || ((tr.ext_dispatched || tr.ext_copy_end.is_some())
                && (!refused_open || cycle_node.is_none_or(|n| conns.contains_key(n))))
    }

    /// Whether the session's named statement `name` takes a session-level
    /// advisory lock ([`Self::takes_session_lock`]),
    /// classified from the registry's canonical `Parse` once per definition
    /// and memoized, as `named_standby_read` is: every Bind of it then costs
    /// a lookup, never a rescan of the stored SQL (which may be far larger
    /// than the Bind). A memo entry is trusted only while it still describes
    /// the very `Parse` bytes the registry holds; an unknown name takes none.
    pub(super) fn named_takes_session_lock(
        memo: &mut HashMap<String, (bytes::Bytes, bool)>,
        registry: &HashMap<String, bytes::Bytes>,
        name: &str,
    ) -> bool {
        let Some(parse) = registry.get(name) else {
            return false;
        };
        if let Some((seen, locks)) = memo.get(name) {
            if seen.as_ptr() == parse.as_ptr() && seen.len() == parse.len() {
                return *locks;
            }
        }
        let locks = Self::parse_msg_sql_bytes(parse).is_some_and(Self::takes_session_lock);
        memo.insert(name.to_string(), (parse.clone(), locks));
        locks
    }

    /// Park every idle backend connection of the session under
    /// transaction/statement pooling, so a request waiting at (or refused
    /// by) its tenant's active-query cap holds no backend connection or node
    /// slot, and on disconnect a later same-identity client can reuse them.
    /// Anything mid-transaction is left alone, and so is the connection
    /// holding the session's open extended cycle (a Flush dispatched part of
    /// it, or an extended COPY awaits its Sync: `tr.ext_dispatched`,
    /// `tr.ext_copy_end`, on `cycle_node`; any connection while that node is
    /// unknown). A reset sent into that cycle would run inside it, and commit
    /// what the client never Synced, or wait out the reset's drain timeout
    /// behind a failed chunk the backend skips to the Sync. At the session's
    /// end it is dropped with the session (closed: the backend rolls the
    /// cycle back), as a connection mid-transaction is.
    #[cfg(feature = "pool-modes")]
    async fn park_idle_conns(
        conns: &mut HashMap<String, BackendConn>,
        tr: &TrSession,
        cycle_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        if state.backend_pool.is_none() {
            return;
        }
        let cycle_open = tr.ext_dispatched || tr.ext_copy_end.is_some();
        let nodes: Vec<String> = conns.keys().cloned().collect();
        for node in nodes {
            if cycle_open && cycle_node.is_none_or(|c| c == node) {
                continue;
            }
            Self::release_to_pool_if_idle(conns, Some(node.as_str()), session, state, config).await;
        }
    }

    /// The backend connection the idle watch covers: the session's current
    /// node or, with read offload, its home node (it holds the session's
    /// state) even while the last request ran on a standby. While an
    /// extended cycle its client is still in is open (`cycle`: a `Flush`
    /// dispatched part of it, or an extended COPY's end awaits its `Sync`),
    /// the connection holding that cycle, wherever routing sent it (a
    /// `route=standby` hint, a node hint): the rest of its results reach the
    /// client only through this watch, and the client waits for them before
    /// it sends anything else. `None` mid-COPY (the backend is legitimately
    /// awaiting CopyData) or without a cached connection.
    fn watch_target<'n>(
        conns: &HashMap<String, BackendConn>,
        home_node: Option<&'n str>,
        current_node: Option<&'n str>,
        cycle: Option<&'n str>,
        offload_on: bool,
        session: &ClientSession,
    ) -> Option<&'n str> {
        if session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            None
        } else if let Some(node) = cycle.filter(|node| conns.contains_key(*node)) {
            Some(node)
        } else if offload_on {
            home_node
                .filter(|node| conns.contains_key(*node))
                .or_else(|| current_node.filter(|node| conns.contains_key(*node)))
        } else {
            current_node.filter(|node| conns.contains_key(*node))
        }
    }

    /// The watched backend connection died while the session was idle (its
    /// entry is gone from `conns`): carry the loss into the next request
    /// when it matters, and forget the node as home/current (`nodes`).
    fn note_watched_gone(
        gone: String,
        watching_home: bool,
        tr: &mut TrSession,
        session: &ClientSession,
        nodes: [&mut Option<String>; 2],
    ) {
        let [home_node, current_node] = nodes;
        if home_node.as_deref() == Some(gone.as_str()) {
            *home_node = None;
        }
        if current_node.as_deref() == Some(gone.as_str()) {
            *current_node = None;
        }
        // F3: dropping the dead cached connection is enough only with
        // `tr_mode = none` outside a transaction (the next query simply
        // redials, as before). Otherwise the loss must be carried into
        // the next request as a not-delivered backend fault against
        // this node: a transaction that was open died with the socket
        // (it must NOT silently continue in autocommit on a fresh
        // connection), and session state (SETs) has to be restored on
        // the replacement connection. `tr_handle_fault` picks this up.
        // A standby socket used only for offloaded reads is simply
        // forgotten.
        if watching_home
            && (session.tr_mode != TrMode::None
                || tr.ext_dispatched
                || session
                    .in_transaction
                    .load(std::sync::atomic::Ordering::Relaxed))
        {
            tr.lost_backend = Some(gone);
        }
    }

    /// One non-blocking read of the watched backend connection after a wait
    /// at the tenant's active-query cap: frames that arrived meanwhile are
    /// relayed as by the idle watch, and a backend that ended (EOF, its own
    /// FATAL, a read error) is removed from `conns`. Returns whether it is
    /// gone, so the request takes the not-delivered path instead of failing
    /// on a dead socket.
    async fn probe_watched_backend(
        stream: &mut ClientStream,
        conns: &mut HashMap<String, BackendConn>,
        node: &str,
        state: &Arc<ServerState>,
        watch: IdleWatch<'_>,
    ) -> Result<bool> {
        let Some(bc) = conns.get_mut(node) else {
            return Ok(false);
        };
        bc.response_buf.reserve(16 * 1024);
        let gone = match bc.stream.try_read_buf(&mut bc.response_buf) {
            Ok(0) => true,
            Ok(_) => {
                let (sent, ended) =
                    Self::forward_idle_backend_frames(stream, &mut bc.response_buf, state, watch)
                        .await?;
                state
                    .metrics
                    .bytes_sent
                    .fetch_add(sent as u64, Ordering::Relaxed);
                ended
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => false,
            Err(e) => {
                tracing::debug!(node = %node, error = %e, "backend read error after a tenant active-query wait; dropping cached connection");
                true
            }
        };
        if gone {
            conns.remove(node);
        }
        Ok(gone)
    }

    /// Wait FIFO for one of the tenant's active-query permits, racing a
    /// CancelRequest for the session's wait registered under its
    /// BackendKeyData (`waiter`: the key and the session's own wait a cancel
    /// claims) and the client going away. Client bytes that arrive meanwhile
    /// are appended to `buffer` (up to `[limits] max_pending_bytes`) and
    /// decoded afterwards. The backend watch is deliberately not raced: its
    /// frame relay writes to the client and is not cancel-safe.
    pub(super) async fn wait_tenant_query_permit<'g>(
        gate: &'g TenantBudget,
        stream: &mut ClientStream,
        buffer: &mut BytesMut,
        waiter: Option<((u32, u32), Arc<QueryWait>)>,
        state: &ServerState,
    ) -> QueryGateWait<'g> {
        let mut outcome = {
            // Every wait goes into the wait histogram when it ends: admitted,
            // refused at its timeout, cancelled or abandoned.
            let wait = gate.wait_query(&state.latency.tenant_query_wait);
            tokio::pin!(wait);
            let cancelled = async {
                match &waiter {
                    Some((_, w)) => w.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(cancelled);
            let mut reading = true;
            loop {
                buffer.reserve(16384);
                tokio::select! {
                    biased;
                    // First: a cancel that arrived before the wait (while the
                    // session parked its connections) ends it without
                    // touching the semaphore.
                    () = &mut cancelled => break QueryGateWait::Cancelled,
                    r = &mut wait => break match r {
                        Ok((p, waited)) => QueryGateWait::Admitted(p, waited.is_some()),
                        Err(r) => QueryGateWait::Refused(r),
                    },
                    r = stream.read_buf(buffer), if reading => match r {
                        Ok(0) | Err(_) => break QueryGateWait::ClientGone,
                        Ok(n) => {
                            state.metrics.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                            reading = buffer.len() <= state.limits.max_pending_bytes;
                        }
                    },
                }
            }
        };
        if let Some((k, w)) = &waiter {
            // Only this session's own entry goes; whether a cancel claimed
            // it is its own flag (another session relayed the same key keeps
            // its wait, and never cancels this one).
            if state.tenant_slots.unregister_query_wait(*k, w)
                && matches!(outcome, QueryGateWait::Admitted(..))
            {
                // A CancelRequest claimed this wait first: the permit goes
                // back and the request is cancelled, never admitted. A
                // refusal or a client gone stands (nothing left to cancel).
                outcome = QueryGateWait::Cancelled;
            }
        }
        // Counted once the outcome is final, so the counters match what the
        // client was told.
        match outcome {
            QueryGateWait::Admitted(..) => gate.note_query_admitted(),
            QueryGateWait::Cancelled | QueryGateWait::ClientGone => gate.note_wait_cancelled(),
            QueryGateWait::Refused(_) | QueryGateWait::Exempt => {}
        }
        outcome
    }

    /// The active-query gate's slow path, after the one-CAS fast path found
    /// the tenant at its cap: the 2PC exemption (simple protocol only:
    /// `simple_sql`), registering the wait for a CancelRequest, parking the
    /// session's idle connections, the FIFO wait, and after a wait one probe
    /// of the watched backend.
    pub(super) async fn gate_slow_path<'g>(
        ctx: GateCtx<'_, 'g>,
        simple_sql: Option<&str>,
    ) -> Result<QueryGateWait<'g>> {
        let GateCtx {
            gate,
            client,
            buffer,
            conns,
            tr,
            nodes: [home_node, current_node],
            cycle_node,
            offload_on,
            session,
            state,
            config,
        } = ctx;
        if simple_sql.is_some_and(Self::tenant_gate_exempt) {
            return Ok(QueryGateWait::Exempt);
        }
        // Registered before the parking: a CancelRequest at any point of the
        // slow path ends the wait, never reaching a backend connection the
        // session is parking (or has parked for another session).
        let waiter = session
            .cancel_key
            .get()
            .map(|k| (*k, state.tenant_slots.register_query_wait(*k)));
        #[cfg(feature = "pool-modes")]
        Self::park_idle_conns(conns, tr, cycle_node, session, state, config).await;
        #[cfg(not(feature = "pool-modes"))]
        let _ = (cycle_node, config);
        let outcome = Self::wait_tenant_query_permit(gate, client, buffer, waiter, state).await;
        if matches!(outcome, QueryGateWait::Admitted(_, true)) && tr.lost_backend.is_none() {
            // A request is gated only with no cycle its client is still in
            // open: such a cycle keeps the session's permit.
            let watch_node = Self::watch_target(
                conns,
                home_node.as_deref(),
                current_node.as_deref(),
                None,
                offload_on,
                session,
            )
            .map(str::to_string);
            if let Some(node) = watch_node {
                let watching_home = !offload_on || home_node.as_deref() == Some(node.as_str());
                let watch = IdleWatch {
                    session,
                    keep_session: session.tr_mode != TrMode::None || !watching_home,
                };
                if Self::probe_watched_backend(client, conns, &node, state, watch).await? {
                    Self::note_watched_gone(
                        node,
                        watching_home,
                        tr,
                        session,
                        [home_node, current_node],
                    );
                }
            }
        }
        Ok(outcome)
    }

    /// Answer a request refused at its tenant's active-query cap (53300) or
    /// cancelled while waiting (57014). The simple protocol also gets
    /// ReadyForQuery 'I' (refusals happen only idle, outside a transaction);
    /// an extended cycle gets the ErrorResponse at once and is then discarded
    /// up to its Sync.
    pub(super) async fn write_query_refusal(
        stream: &mut ClientStream,
        state: &ServerState,
        outcome: QueryGateWait<'_>,
        simple: bool,
    ) -> Result<()> {
        let mut resp = match outcome {
            QueryGateWait::Refused(r) => {
                tracing::debug!(
                    target: "helios::tenant",
                    tenant = %r.tenant,
                    limit = r.limit,
                    "tenant active-query cap reached; request refused"
                );
                // Every refusal is logged at debug; a warning at each power
                // of two bounds the volume.
                if r.nth.is_power_of_two() {
                    tracing::warn!(
                        target: "helios::tenant",
                        tenant = %r.tenant,
                        limit = r.limit,
                        refusals = r.nth,
                        "tenant active-query cap reached; requests refused so far: {}",
                        r.nth
                    );
                }
                Self::create_error_response("53300", &r.to_string())
            }
            QueryGateWait::Cancelled => {
                Self::create_error_response("57014", "canceling statement due to user request")
            }
            QueryGateWait::Admitted(..) | QueryGateWait::Exempt | QueryGateWait::ClientGone => {
                return Ok(());
            }
        };
        if simple {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
        }
        stream
            .write_all(&resp)
            .await
            .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
        state
            .metrics
            .bytes_sent
            .fetch_add(resp.len() as u64, Ordering::Relaxed);
        if simple {
            // An extended cycle is counted at its Sync.
            state
                .metrics
                .queries_processed
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// The session's unnamed statement (and with `which.portal` its unnamed
    /// portal) was destroyed by a request that reached none of `conns`: each
    /// closes what it still holds of them before a later batch could use it
    /// (see [`UnnamedStale`]).
    pub(super) fn orphan_unnamed(conns: &mut HashMap<String, BackendConn>, which: UnnamedStale) {
        for bc in conns.values_mut() {
            bc.unnamed_stale.statement |= which.statement;
            bc.unnamed_stale.portal |= which.portal;
        }
    }

    /// Enable TCP keepalive on an accepted client socket (`[limits]
    /// client_tcp_keepalive_secs`, ac858d61400b): once a client that vanished
    /// without a FIN or RST leaves the probes unanswered, the session's
    /// pending read fails and the session ends, returning what it held (its
    /// tenant active-query permit, its client slot, its backend
    /// connections). Without it such a session waits on its client until
    /// the proxy restarts. Best-effort: a socket that refuses the option is
    /// served without it.
    fn set_client_keepalive(stream: &TcpStream, addr: SocketAddr, limits: &ResolvedLimits) {
        let Some(every) = limits.client_tcp_keepalive else {
            return;
        };
        let keepalive = socket2::TcpKeepalive::new().with_time(every);
        #[cfg(any(
            target_os = "android",
            target_os = "freebsd",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "windows",
        ))]
        let keepalive = keepalive.with_interval(every);
        if let Err(e) = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive) {
            tracing::debug!(client = %addr, error = %e, "could not enable TCP keepalive on a client connection");
        }
    }

    /// Handle a client connection
    pub(super) async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        state: Arc<ServerState>,
        config: Arc<ProxyConfig>,
        _shutdown_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        tracing::debug!("New client connection from {}", addr);
        Self::set_client_keepalive(&stream, addr, &state.limits);

        // Create session
        let session_id = Uuid::new_v4();
        let session = Arc::new(ClientSession {
            id: session_id,
            client_addr: addr,
            client_ip_str: addr.ip().to_string(),
            session_id_str: session_id.to_string(),
            current_node: RwLock::new(None),
            in_transaction: std::sync::atomic::AtomicBool::new(false),
            copy_in_progress: std::sync::atomic::AtomicBool::new(false),
            last_rfq_status: std::sync::atomic::AtomicU8::new(b'I'),
            last_response_error: std::sync::atomic::AtomicBool::new(false),
            cycle_completions: std::sync::atomic::AtomicU32::new(0),
            cycle_errored: std::sync::atomic::AtomicBool::new(false),
            cycle_counted: std::sync::atomic::AtomicBool::new(true),
            last_response_digest: std::sync::atomic::AtomicU64::new(0),
            last_response_unverifiable: std::sync::atomic::AtomicBool::new(false),
            tr_replay_tainted: std::sync::atomic::AtomicBool::new(false),
            backend_credential: RwLock::new(None),
            #[cfg(feature = "query-cache")]
            tx_cache_stage: std::sync::Mutex::new(TxCacheStage::default()),
            #[cfg(feature = "query-cache")]
            cache_unseen_write: std::sync::atomic::AtomicBool::new(false),
            journal: std::sync::Mutex::new(crate::journal_capture::SessionCapture::new(
                config.journal.max_statement_bytes,
            )),
            journal_armed: std::sync::atomic::AtomicBool::new(false),
            journal_open: std::sync::atomic::AtomicBool::new(false),
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: config.effective_tr_mode(),
            #[cfg(feature = "lag-routing")]
            last_write_at: RwLock::new(None),
            #[cfg(feature = "pool-modes")]
            pool_client_id: ClientId::new(),
            #[cfg(feature = "wasm-plugins")]
            plugin_identity: RwLock::new(None),
            #[cfg(feature = "edge-proxy")]
            edge_ineligible: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "edge-proxy")]
            pending_edge_copy_tables: std::sync::Mutex::new(None),
            #[cfg(feature = "rate-limiting")]
            rate_limit_key: std::sync::OnceLock::new(),
            startup_slot: parking_lot::Mutex::new(None),
            reads_pinned: std::sync::atomic::AtomicBool::new(false),
            read_node: parking_lot::Mutex::new(None),
            last_write: parking_lot::Mutex::new(None),
            tenant_charge: std::sync::OnceLock::new(),
            cancel_key: std::sync::OnceLock::new(),
        });

        // Register the session, then attach an RAII guard that deregisters it on
        // ANY exit from here on — a normal return OR a panic unwind. Before this,
        // a panic anywhere in negotiation/startup/the query loop leaked the
        // ClientSession entry forever, inflating the active-session gauge and
        // stalling every graceful drain to its full timeout. The guard also
        // owns the rest of the per-connection teardown (metric + L1 cache) so it
        // too runs unconditionally.
        state.sessions.insert(session.id, session.clone());
        let mut _session_guard = SessionGuard {
            state: state.clone(),
            session_id: session.id,
            // Filled in below, once admission control has run: the guard owns
            // the slot so it is returned on every exit path, panics included.
            _client_slot: None,
            _tenant_slot: None,
        };

        // Negotiate client TLS (if the client sent SSLRequest). Produces a
        // ClientStream that is plaintext or TLS-wrapped; the rest of the
        // session is written against that single stream type. `pre` carries
        // a first startup/cancel message already read while peeking.
        //
        // Bound the pre-auth negotiation (first-message read + TLS handshake) in
        // time: a client that connects and then stalls must not pin this task
        // and its session-map slot indefinitely (slow-loris). ONE deadline
        // covers the handshake *and* the first startup message read below, so
        // the TLS path is bounded by the same budget as the plaintext one. The
        // query loop that follows is intentionally NOT under this deadline.
        let handshake_deadline = tokio::time::Instant::now() + state.limits.startup_timeout;
        let negotiated = match tokio::time::timeout_at(
            handshake_deadline,
            Self::negotiate_client_tls(stream, &state),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!(client = %addr, "pre-auth negotiation timed out; closing");
                Err(ProxyError::Connection(
                    "startup negotiation timeout".to_string(),
                ))
            }
        };
        let result = match negotiated {
            Ok((mut client_stream, pre)) => {
                // Resolve the first startup-phase message before anything else:
                // admission control ([limits] max_client_connections) must be
                // able to tell a real `Startup` from a `CancelRequest`, and on
                // the TLS path that message only arrives after the handshake, so
                // it is read here (inside the same pre-auth deadline) rather
                // than inside `handle_startup`. `buffer` carries any bytes read
                // past it into the query loop.
                let mut buffer = BytesMut::with_capacity(8192);
                let first = match pre {
                    Some(msg) => Ok(Some(msg)),
                    None => match tokio::time::timeout_at(
                        handshake_deadline,
                        Self::read_startup_message(&mut client_stream, &mut buffer),
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(_) => {
                            tracing::debug!(client = %addr, "startup message timed out; closing");
                            Err(ProxyError::Connection(
                                "startup negotiation timeout".to_string(),
                            ))
                        }
                    },
                };
                match first {
                    // Client closed before sending a complete startup message.
                    Ok(None) => Ok(()),
                    Ok(Some(msg)) => match Self::admit_tenant_slot(&state, &msg).await {
                        Err(refusal) => {
                            tracing::warn!(
                                client = %addr,
                                tenant = %refusal.tenant,
                                limit = refusal.limit,
                                "tenant client cap reached; refusing connection"
                            );
                            Self::refuse_with(
                                &mut client_stream,
                                &Self::create_fatal_response("53300", &refusal.to_string()),
                                state.limits.client_write_timeout,
                            )
                            .await;
                            Ok(())
                        }
                        Ok(tenant_slot) => match Self::admit_client_slot(&state, &msg).await {
                            Ok(slot) => {
                                _session_guard._tenant_slot = tenant_slot;
                                _session_guard._client_slot = slot;
                                Self::attach_tenant_budget(&state, &session, &msg);
                                Self::client_loop(
                                    &mut client_stream,
                                    Some(msg),
                                    buffer,
                                    &session,
                                    &state,
                                    &config,
                                )
                                .await
                            }
                            Err(()) => {
                                tracing::warn!(
                                    client = %addr,
                                    "client connection cap reached; refusing connection"
                                );
                                Self::refuse_over_capacity(
                                    &mut client_stream,
                                    state.limits.client_write_timeout,
                                )
                                .await;
                                Ok(())
                            }
                        },
                    },
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };

        // TR-07: a transaction still open when the session ends is rolled
        // back by the backend; drop its active journal.
        Self::journal_close(&session, &state).await;

        // Session deregistration, the connections-closed metric, and the L1
        // cache reclaim are all handled by `_session_guard`'s Drop (which runs
        // here on the normal path and on an unwind), so nothing is required here.
        result
    }

    /// Deadline for the client's next message, or `None` when this session must
    /// not be reclaimed by the idle-session timeout right now.
    ///
    /// PostgreSQL's `idle_session_timeout` only applies to a session that is
    /// genuinely waiting for a NEW command, so the deadline is armed only at a
    /// true message boundary:
    ///
    /// * not while `buffer` still holds a partially received message — a client
    ///   trickling a large message is slow, not idle, and killing it would
    ///   truncate a legitimate statement;
    /// * not while the session is inside a `COPY FROM STDIN` — a COPY producer
    ///   that pauses longer than the timeout would otherwise be killed
    ///   mid-stream and the bulk load aborted.
    ///
    /// `None` is also returned when the timeout is disabled (the default), in
    /// which case the read below is unbounded exactly as it was before the key
    /// existed.
    pub(super) fn client_idle_deadline(
        state: &ServerState,
        buffer: &BytesMut,
        session: &ClientSession,
    ) -> Option<tokio::time::Instant> {
        let idle = state.limits.client_idle_timeout?;
        if !buffer.is_empty() {
            return None;
        }
        if session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        Some(tokio::time::Instant::now() + idle)
    }

    /// Read the client's next message into `buffer`, optionally under an
    /// idle-session deadline.
    pub(super) async fn read_client_bytes(
        stream: &mut ClientStream,
        buffer: &mut BytesMut,
        idle_deadline: Option<tokio::time::Instant>,
    ) -> Result<ClientRead> {
        let read = match idle_deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline, stream.read_buf(buffer)).await {
                    Ok(r) => r,
                    Err(_) => return Ok(ClientRead::IdleTimeout),
                }
            }
            None => stream.read_buf(buffer).await,
        };
        Ok(ClientRead::Bytes(read.map_err(|e| {
            ProxyError::Network(format!("Read error: {}", e))
        })?))
    }

    /// Length of the longest prefix of `buf` made up of WHOLE backend frames.
    ///
    /// A backend message is a 1-byte tag followed by a 4-byte big-endian length
    /// that counts itself, so one frame occupies `1 + len` bytes. Returns an
    /// error for a length below the 4-byte minimum: that is a malformed frame,
    /// and treating it as "incomplete" would stall reassembly until the session
    /// buffer cap instead of surfacing the fault (H-07).
    pub(super) fn complete_frame_prefix(buf: &[u8], max_frame_bytes: usize) -> Result<usize> {
        let mut off = 0usize;
        while let Some(len) = backend_frame_len(&buf[off..], max_frame_bytes)? {
            let end = off + 1 + len;
            if end > buf.len() {
                break;
            }
            off = end;
        }
        Ok(off)
    }

    /// Offset of the first FATAL/PANIC `ErrorResponse` among the complete
    /// frames `buf[..whole]`, if any. Read while the session is idle, such a
    /// frame is the backend announcing its own end (`pg_terminate_backend`, a
    /// fast shutdown, `idle_session_timeout`).
    pub(super) fn idle_termination_offset(buf: &[u8], whole: usize) -> Option<usize> {
        let mut off = 0usize;
        while off + 5 <= whole {
            let len = u32::from_be_bytes([buf[off + 1], buf[off + 2], buf[off + 3], buf[off + 4]])
                as usize;
            let end = (off + 1 + len).min(whole);
            if buf[off] == b'E' && Self::error_ends_session(&buf[off + 5..end]) {
                return Some(off);
            }
            off = end;
        }
        None
    }

    /// Whether an `ErrorResponse` body's severity (`V`, else `S`) is FATAL or
    /// PANIC.
    fn error_ends_session(mut fields: &[u8]) -> bool {
        let mut severity: Option<&[u8]> = None;
        while let Some((&code, tail)) = fields.split_first() {
            if code == 0 {
                break;
            }
            let Some(nul) = memchr::memchr(0, tail) else {
                break;
            };
            let value = &tail[..nul];
            if code == b'V' {
                severity = Some(value);
                break;
            }
            if code == b'S' && severity.is_none() {
                severity = Some(value);
            }
            fields = &tail[nul + 1..];
        }
        severity.is_some_and(|s| s == b"FATAL" || s == b"PANIC")
    }

    /// Before a request: when the session's home is no longer the write
    /// leader (see `home_moved`), drop its connection and forget it as home
    /// (`nodes`: the home and current node). Under transaction replay, or
    /// inside a transaction, the session re-homes like one whose home was
    /// lost while idle (replay restores its settings or refuses; an open
    /// transaction is reported lost). Otherwise the next connection simply
    /// goes to the leader, as after any lost backend. A cycle a Flush left
    /// open on the old home (or an extended COPY's end) may still be running
    /// there, so its connection is cancelled before it is dropped
    /// (`abandon_conn`).
    async fn rehome_if_moved(
        conns: &mut HashMap<String, BackendConn>,
        nodes: [&mut Option<String>; 2],
        offload_on: bool,
        tr: &mut TrSession,
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) {
        let [home_node, current_node] = nodes;
        let watched = if offload_on {
            home_node.as_deref()
        } else {
            current_node.as_deref()
        };
        let Some(moved) = Self::home_moved(conns, watched, state, config) else {
            return;
        };
        if tr.ext_dispatched || tr.ext_copy_end.is_some() {
            Self::abandon_conn(conns, &moved, session, state).await;
        } else {
            conns.remove(&moved);
        }
        tracing::info!(
            target: "helios::tr",
            node = %moved,
            "session home is no longer the write leader: re-homing"
        );
        for node in [home_node, current_node] {
            if node.as_deref() == Some(moved.as_str()) {
                *node = None;
            }
        }
        if session.tr_mode != TrMode::None
            || tr.ext_dispatched
            || session
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            tr.lost_backend = Some(moved);
        }
    }

    /// A COPY drained to its ReadyForQuery (`done`: it ended with CopyDone,
    /// not CopyFail): settle its bookkeeping and, for COPY ... FROM, fire the
    /// deferred edge invalidation now that its rows are visible.
    async fn copy_drained(
        tr: &mut TrSession,
        done: bool,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        Self::tr_after_copy_drain(tr, session, state).await;
        #[cfg(feature = "edge-proxy")]
        if config.edge.enabled {
            let stashed = session
                .pending_edge_copy_tables
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            // CopyFail loaded nothing: the stash is just cleared.
            if let (Some(tables), true) = (stashed, done) {
                Self::edge_invalidate_write(state, config, tables).await;
            }
        }
        #[cfg(not(feature = "edge-proxy"))]
        let _ = (done, config);
    }

    /// [`Self::forward_buffered_backend_frames`] for the idle watch. With
    /// `keep_session` (the session's TR re-homes it: `tr_mode != none`),
    /// frames up to a backend's own FATAL/PANIC are forwarded; that frame and
    /// everything after it are dropped and `true` reports the backend gone.
    /// The proxy keeps the client session (it redials, restores the session's
    /// state and, in a transaction, replays), so the client must not be told
    /// the session ended: a driver that sees an unsolicited FATAL closes its
    /// connection. Without `keep_session` nothing is restored, and the FATAL
    /// is the client's notice that its session state is gone: it is forwarded.
    async fn forward_idle_backend_frames(
        stream: &mut ClientStream,
        buf: &mut BytesMut,
        state: &Arc<ServerState>,
        watch: IdleWatch<'_>,
    ) -> Result<(usize, bool)> {
        let IdleWatch {
            session,
            keep_session,
        } = watch;
        let whole = Self::complete_frame_prefix(buf, state.limits.max_backend_frame_bytes)?;
        let cut = if keep_session {
            Self::idle_termination_offset(buf, whole)
        } else {
            None
        };
        let Some(cut) = cut else {
            return Ok((
                Self::forward_buffered_backend_frames(stream, buf, session, state).await?,
                false,
            ));
        };
        let mut head = buf.split_to(cut);
        let sent = Self::forward_buffered_backend_frames(stream, &mut head, session, state).await?;
        buf.clear();
        Ok((sent, true))
    }

    /// Wait for the client's next message.
    ///
    /// When `watch_node` names a cached backend connection, that socket is
    /// watched at the same time so unsolicited backend traffic (LISTEN/NOTIFY
    /// notifications, NoticeResponse, ParameterStatus, the delayed tail of a
    /// `Flush` response) is relayed to the client promptly instead of sitting
    /// unread until the next query, and a backend that dies while the session is
    /// idle is noticed at once — in which case its entry is removed from `conns`
    /// (the session survives; the next query redials) and the wait ends with
    /// [`ClientRead::BackendGone`], so the caller can settle at once what died
    /// with it (a tenant permit only it kept) before waiting on the client
    /// alone.
    ///
    /// `idle_deadline` is the session's idle-session deadline; relaying async
    /// backend traffic to an idle client is not client activity, so the deadline
    /// is NOT re-armed while doing so.
    pub(super) async fn read_next_client_message(
        stream: &mut ClientStream,
        buffer: &mut BytesMut,
        conns: &mut HashMap<String, BackendConn>,
        watch_node: Option<&str>,
        idle_deadline: Option<tokio::time::Instant>,
        state: &Arc<ServerState>,
        watch: IdleWatch<'_>,
    ) -> Result<ClientRead> {
        let Some(node) = watch_node else {
            return Self::read_client_bytes(stream, buffer, idle_deadline).await;
        };
        // Built once, outside the watch loop: a `select!` evaluates even a
        // disabled branch's expression, so an inline `sleep_until` would
        // construct a timer on every relay iteration — and with no idle timeout
        // configured (the default) this future never registers one at all.
        let idle_wait = async move {
            match idle_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(idle_wait);
        loop {
            let mut backend_gone = false;
            let mut client_bytes: Option<usize> = None;
            // Every relay of this socket shares its unpublished frame tail.
            {
                let bc = conns.get_mut(node).expect("watch_node is in conns");
                let (sent, ended) =
                    Self::forward_idle_backend_frames(stream, &mut bc.response_buf, state, watch)
                        .await?;
                state
                    .metrics
                    .bytes_sent
                    .fetch_add(sent as u64, Ordering::Relaxed);
                bc.response_buf.reserve(16 * 1024);
                if ended {
                    backend_gone = true;
                } else {
                    tokio::select! {
                        r = stream.read_buf(buffer) => {
                            client_bytes = Some(r.map_err(|e| {
                                ProxyError::Network(format!("Read error: {}", e))
                            })?);
                        }
                        _ = &mut idle_wait => return Ok(ClientRead::IdleTimeout),
                        r = bc.stream.read_buf(&mut bc.response_buf) => match r {
                            Ok(0) => backend_gone = true,
                            Ok(_) => {
                                // Publish only COMPLETE frames. A truncated frame is
                                // unrecoverable: the client blocks waiting for the
                                // rest, and nothing may ever be injected after it —
                                // not an ErrorResponse, and certainly not the rows of
                                // a re-executed statement, which would be appended to
                                // whatever the partial frame turns out to be.
                                let (sent, ended) = Self::forward_idle_backend_frames(stream, &mut bc.response_buf, state, watch).await?;
                                state.metrics.bytes_sent.fetch_add(sent as u64, Ordering::Relaxed);
                                if ended {
                                    tracing::debug!(node = %node, "backend ended the session while idle (FATAL); dropping cached connection");
                                    backend_gone = true;
                                }
                            }
                            Err(e) => {
                                tracing::debug!(node = %node, error = %e, "backend read error while idle; dropping cached connection");
                                backend_gone = true;
                            }
                        },
                    }
                }
            }
            if backend_gone {
                // Drop the dead cached connection but keep the client session
                // alive — the next query redials. (Mid-transaction the next
                // forward fails and surfaces the error.) Any partially received
                // frame dies with it: it was never published, so the client
                // stream stays frame-aligned and a synthesized error or a
                // re-executed statement can still be written safely.
                conns.remove(node);
                return Ok(ClientRead::BackendGone);
            }
            if let Some(cn) = client_bytes {
                return Ok(ClientRead::Bytes(cn));
            }
            // Otherwise we relayed async backend bytes; keep watching.
        }
    }

    /// Main client processing loop with full PostgreSQL protocol handling
    /// ([`Self::serve_client`]), and what follows every exit from it, a
    /// `return`, an error or the end of the loop alike (ac858d61400b): the
    /// session's backend connections, its TR state, the node holding its
    /// open extended cycle and its tenant active-query permit live here, so
    /// a cycle still open on a backend when the session ends (its client
    /// gone, or terminated, after a `Flush` dispatched an `Execute`, or
    /// after an extended COPY's end) is cancelled there
    /// ([`Self::cancel_open_cycle`]) before the permit returns. Dropping the
    /// socket alone would leave the statement running outside the cap until
    /// it next writes.
    pub(super) async fn client_loop(
        stream: &mut ClientStream,
        pre: Option<StartupMessage>,
        buffer: BytesMut,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<()> {
        let mut held = SessionHeld {
            conns: HashMap::new(),
            tr: TrSession::new(session.tr_mode),
            cycle_node: None,
            hold: session
                .tenant_budget()
                .filter(|b| b.has_query_cap())
                .map(QueryHold::new),
        };
        let result =
            Self::serve_client(stream, pre, buffer, session, state, config, &mut held).await;
        Self::cancel_open_cycle(&mut held, session, state).await;
        result
    }

    /// The session ends with an extended cycle open on a backend connection
    /// (`tr.ext_dispatched`: a `Flush` dispatched part of it; or
    /// `tr.ext_copy_end`: an extended COPY ended and its `Sync` is owed):
    /// what that cycle runs (an `Execute` a pipelining client flushed, the
    /// end of a COPY) may still be executing there. Close that connection
    /// (`cycle_node`, or every one while it is unknown) after cancelling it
    /// (`abandon_conn`), before the session's tenant permit returns. A
    /// connection the session lost has nothing left to cancel.
    async fn cancel_open_cycle(
        held: &mut SessionHeld<'_>,
        session: &ClientSession,
        state: &ServerState,
    ) {
        if !held.tr.ext_dispatched && held.tr.ext_copy_end.is_none() {
            return;
        }
        Self::abandon_cycle_conn(&mut held.conns, held.cycle_node.as_deref(), session, state).await;
    }

    /// Close the connection holding the session's open extended cycle
    /// (`cycle_node`, or every cached one while it is unknown) after
    /// cancelling what that cycle may still run there (`abandon_conn`). A
    /// connection already gone has nothing left to cancel.
    pub(super) async fn abandon_cycle_conn(
        conns: &mut HashMap<String, BackendConn>,
        cycle_node: Option<&str>,
        session: &ClientSession,
        state: &ServerState,
    ) {
        let nodes: Vec<String> = match cycle_node {
            Some(node) => vec![node.to_string()],
            None => conns.keys().cloned().collect(),
        };
        for node in nodes {
            Self::abandon_conn(conns, &node, session, state).await;
        }
    }

    /// The session's query loop, from its startup: see [`Self::client_loop`],
    /// which owns `held`.
    async fn serve_client(
        stream: &mut ClientStream,
        pre: Option<StartupMessage>,
        mut buffer: BytesMut,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        held: &mut SessionHeld<'_>,
    ) -> Result<()> {
        let SessionHeld {
            conns,
            tr,
            cycle_node,
            hold,
        } = held;
        let codec = ProtocolCodec::new();

        // Handle startup phase. The session keeps a per-node cache of
        // authenticated backend connections (`conns`) instead of a single
        // stream: when read/write routing moves a session between primary
        // and standby it now reuses the already-authenticated connection to
        // each node rather than dropping the socket and paying a fresh TCP
        // connect + startup + SCRAM handshake on every switch (Batch C).
        // Connections are authenticated with the client's own credentials
        // (auth is pass-through). In Transaction/Statement pooling mode they
        // are returned to a shared, identity-keyed idle pool at each
        // transaction boundary (DISCARD ALL reset on release) and reused by the
        // next same-identity acquisition — see `release_to_pool_if_idle` /
        // `ensure_conn`. The first connection of a session is still
        // established through the authenticated startup path; drawing the
        // startup connection from the pool (to reduce *concurrent* backend
        // connections below the client count) additionally needs
        // proxy-terminated backend auth and is the documented next increment.
        // (`conns` lives in `client_loop`, which outlives every exit from here.)
        // Bound the startup/authentication exchange in time (the TLS path reads
        // the real startup packet here, after negotiation). A client that opens
        // the connection but never completes startup must not hold the task and
        // its session slot open indefinitely.
        // A startup handler may already have relayed or emitted a terminal
        // ErrorResponse. Keep that fact outside the timed future: cancellation
        // during its write must not append a second frame to a partial error.
        let mut startup_error_started = false;
        let startup_result = match tokio::time::timeout(
            state.limits.startup_timeout,
            Self::handle_startup(
                stream,
                &mut buffer,
                pre,
                session,
                state,
                config,
                &mut startup_error_started,
            ),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(ProxyError::Connection("startup timeout".to_string())),
        };
        let mut current_node: Option<String> = match startup_result {
            Ok((Some(stream_conn), node_addr)) => {
                let slot = session.startup_slot.lock().take();
                let mut bc = BackendConn::with_slot(stream_conn, slot);
                // The BackendKeyData relayed to the client at startup is
                // this socket's.
                bc.backend_key = session.cancel_key.get().copied();
                conns.insert(node_addr.clone(), bc);
                Some(node_addr)
            }
            Ok((None, _)) => {
                // SSL rejected or cancel request, connection should close
                return Ok(());
            }
            Err(e) => {
                tracing::error!("Startup failed: {}", e);
                if !startup_error_started {
                    let err_msg =
                        Self::create_error_response("08006", &format!("Startup failed: {}", e));
                    let _ = stream.write_all(&err_msg).await;
                }
                return Err(e);
            }
        };

        // Main query loop.
        //
        // Two wire shapes are handled. Simple-query (`Query`) messages are
        // self-contained: route, forward, and stream the response back
        // frame-by-frame until ReadyForQuery. Extended-protocol messages
        // (`Parse`/`Bind`/`Describe`/`Execute`/`Close`) carry no response of
        // their own until the client sends `Sync` (or `Flush`), so they are
        // accumulated into `pending` and forwarded as one batch at that
        // boundary — this is what stops the per-message 30s backend-read
        // timeout that made every prepared-statement driver unusable. The
        // routing decision for an extended batch is taken from the SQL in its
        // first `Parse`; a batch with no `Parse` (a re-`Bind`/`Execute` of a
        // named prepared statement) stays on the connection the statement was
        // prepared on.
        let mut pending = BytesMut::new();
        let mut pending_route_sql: Option<String> = None;
        // Prepared-statement tracking (Batch F.4). `stmt_registry` is the
        // session's canonical record of every *named* `Parse` the client has
        // issued (name -> full Parse message bytes) so the proxy can re-prepare
        // a statement on any backend connection that is missing it. `batch_*`
        // accumulate, for the in-flight extended batch, which named statements
        // it defines (Parse), references (Bind/Describe-S), and closes
        // (Close-S) — resolved at the Sync/Flush boundary.
        let mut stmt_registry: HashMap<String, bytes::Bytes> = HashMap::new();
        // Running sum of the bytes held in `stmt_registry`, kept in step with
        // it so the aggregate-size cap is O(1) per Parse (see MAX_PREPARED_BYTES).
        let mut stmt_registry_bytes: usize = 0;
        let mut batch_defines: Vec<String> = Vec::new();
        let mut batch_refs: Vec<String> = Vec::new();
        let mut batch_closes: Vec<String> = Vec::new();
        // The names the in-flight batch Closed and did not Parse again after
        // the Close: what it leaves closed. Wire order matters here, so it is
        // tracked per message, and like `batch_defines` it spans the batch's
        // Flush-terminated parts until the Sync (F2).
        let mut batch_left_closed: Vec<String> = Vec::new();
        // Registry writes (name, previous value) of Parses that arrived while
        // the aborted-transaction emulation was active: if their batch is then
        // answered 25P02 without being forwarded, the backend kept the old
        // definitions and these writes are undone (TR-08 review F1).
        let mut aborted_undo: Vec<(String, Option<bytes::Bytes>)> = Vec::new();
        #[cfg(feature = "edge-proxy")]
        let mut aborted_edge_undo: Vec<(String, Option<Option<Vec<String>>>)> = Vec::new();
        // Edge invalidation metadata for extended-protocol statements,
        // memoized at Parse time (name -> Some(tables) when the statement is
        // DML, None when read-only) so steady-state Bind/Execute cycles of a
        // prepared write cost a map lookup, not a regex pass. The unnamed
        // statement gets its own slot; `edge_batch_bound_unnamed` records
        // whether the in-flight batch Bind-referenced it. Resolved into an
        // invalidation at the Sync boundary.
        #[cfg(feature = "edge-proxy")]
        let mut edge_stmt_meta: HashMap<String, Option<Vec<String>>> = HashMap::new();
        #[cfg(feature = "edge-proxy")]
        let mut edge_unnamed_meta: Option<Vec<String>> = None;
        #[cfg(feature = "edge-proxy")]
        let mut edge_batch_bound_unnamed = false;
        // Unnamed-`Parse` promotion (Batch H). `held_unnamed` parks an unnamed
        // Parse that is the FIRST message of a batch (so the batch stays the
        // clean Parse→Bind→…→Sync shape) — it is NOT appended to `pending`; the
        // decision to forward or skip it is taken at the batch boundary once the
        // target connection is known. Holds (full Parse message, signature).
        let promote_unnamed = config.optimize_unnamed_parse;
        let mut held_unnamed: Option<(bytes::Bytes, bytes::Bytes)> = None;
        // In-session Transaction Replay state (`tr_mode`, in `tr`): tracked
        // session SETs, the lost-while-idle backend, the aborted-transaction
        // emulation and the open extended-protocol cycle. See
        // `tr_handle_fault`.
        // ac858d61400b: the session's hold on its tenant's active-query cap
        // (`hold`; `None`: no cap, no await and no lock per request), and an
        // extended cycle being discarded to its Sync after a refusal or a
        // cancel. Session-level advisory locks keep a permit under session
        // pooling: they are marked on the backend connection that took them
        // (`session_locks_held`).
        let mut cycle_refused = false;
        // A Sync or Query was answered (ReadyForQuery) without reaching the
        // backend while a cycle stays open there (`tr.ext_dispatched`,
        // `tr.ext_copy_end`): only a plugin Block refuses a request that
        // continues such a cycle. The client is idle then, so a permit holder
        // counts as idle and `idle_permit_timeout_ms` bounds it, although the
        // permit stays held until a later request settles the cycle, or the
        // connection holding it is lost (`cycle_keeps_permit`).
        let mut refused_open = false;
        // The reverse: a Flush-terminated batch was answered (an error)
        // without reaching a backend — an empty rate-limit bucket, an open
        // circuit, the aborted-transaction emulation, a failed replacement.
        // Nothing of its cycle is open there, but the client got no
        // ReadyForQuery and still owes the cycle's Sync: it is mid-cycle, not
        // an idle permit holder, until that Sync arrives. As PostgreSQL skips
        // to the Sync after an error, the rest of the cycle is discarded up
        // to it (`cycle_refused`), so nothing the client pipelined behind the
        // error runs, or commits at that Sync; the Sync arm answers the Sync.
        // The aborted-transaction emulation answers each batch itself (a
        // Query ends this there).
        let mut refused_flush = false;
        #[cfg(feature = "pool-modes")]
        let session_pooling = state.backend_pool.is_none();
        #[cfg(not(feature = "pool-modes"))]
        let session_pooling = true;
        let track_session_locks = hold.is_some() && session_pooling;
        // The extended batch being accumulated takes a session-level advisory
        // lock (a `Parse` of one, a `Bind` of a named one): the connection it
        // reaches is marked at its Flush or Sync.
        let mut batch_locks = false;
        // Which named statements take a session-level advisory lock,
        // memoized against the registry's Parse bytes
        // (`named_takes_session_lock`); kept only while tracking.
        let mut stmt_locks: HashMap<String, (bytes::Bytes, bool)> = HashMap::new();
        // Read offload (`[load_balancer] standby_reads`): on when reads may
        // leave the primary and a standby can serve them. Mirroring the
        // session's settings onto standby connections needs the SET tracker.
        let offload_on = config.load_balancer.offloads_reads()
            && (state.authoritative_topology
                || config.nodes.iter().any(|n| {
                    n.enabled && matches!(n.role, NodeRole::Standby | NodeRole::ReadReplica)
                }));
        tr.track_gucs |= offload_on;
        let ryw = Duration::from_millis(config.load_balancer.read_your_writes_ms);
        // Extended batches under read offload: every statement the in-flight
        // batch executes is a standby-safe read; a Flush part of it was
        // already forwarded (a chain stays where it started); this batch
        // Parsed the unnamed statement; and the standby-safety of named
        // statements, memoized against the registry's Parse bytes.
        let mut batch_reads = true;
        let mut batch_flushed = false;
        let mut batch_parsed_unnamed = false;
        let mut stmt_reads: HashMap<String, (bytes::Bytes, bool)> = HashMap::new();
        // The batch Binds the unnamed statement; the node holding the client's
        // unnamed statement (where its last unnamed Parse ran); the session's
        // home node (where its last non-offloaded request ran), which the idle
        // watch covers while the session sits on a standby.
        let mut batch_binds_unnamed = false;
        let mut unnamed_node: Option<String> = None;
        let mut home_node: Option<String> = current_node.clone();
        // Extended-protocol SET tracking (`ext_set`): the open cycle, and
        // what the client's unnamed statement is (it outlives the cycle that
        // Parsed it).
        let mut ext_set = ExtSetCycle::with_limit(state.limits.tr_max_cycle_statements);
        // The client's last request was extended-protocol (so a COPY it
        // started is too); such a COPY's end waiting for the client's Sync
        // is `tr.ext_copy_end`.
        let mut ext_copy = false;
        // A COPY whose backend connection was gone when the client's COPY
        // frames arrived: its CopyDone/CopyFail is answered (57000), while
        // one outside any COPY is ignored, as PostgreSQL ignores it.
        let mut copy_lost = false;
        let mut unnamed_bound = Bound::Other;
        // `cycle_node`: the node whose connection holds the open cycle
        // (`tr.ext_dispatched`, `tr.ext_copy_end`), set whenever one opens: a
        // request continuing it goes there whatever routing would pick
        // (`OpenCycle`). Read only while a cycle is open.
        // The extended batch being accumulated has an Execute (it runs
        // statements of its own: see `OpenCycle::runs`).
        let mut batch_executes = false;
        loop {
            // Read the client's next message directly into the accumulation
            // buffer (no intermediate zeroed scratch, no extra copy). `read_buf`
            // appends into `buffer`'s spare capacity and advances its length.
            //
            // While waiting, ALSO watch the session's current backend connection
            // so unsolicited backend traffic — LISTEN/NOTIFY notifications,
            // NoticeResponse, ParameterStatus, and the delayed tail of a `Flush`
            // response — is relayed to the client promptly instead of sitting
            // unread until the next query, and a backend that dies while the
            // session is idle is noticed at once. At the top of the loop the
            // backend is always quiescent (every query response is fully drained
            // before we return here), so any bytes it produces are out-of-band
            // and are relayed verbatim. A mid-COPY backend is excluded — it is
            // legitimately awaiting CopyData, which the client drives.
            buffer.reserve(16384);
            // Idle-session deadline — armed only when this session is genuinely
            // waiting for a NEW command (see `client_idle_deadline`), i.e. never
            // mid-COPY and never with a partially received message pending.
            // `None` (the default, `client_idle_timeout_secs = 0`) leaves every
            // read unbounded exactly as before.
            let client_deadline = Self::client_idle_deadline(state, &buffer, session);
            // ac858d61400b: a permit holder waiting for its client with
            // nothing in flight is idle (in a transaction, holding
            // session-level advisory locks, or behind a refusal that left a
            // cycle, or a COPY in copy-in, open on the backend): counted, and
            // terminated after `idle_permit_timeout_ms` when that deadline
            // comes first. A client that still owes a cycle's Sync is not
            // (`refused_flush`), nor one streaming a COPY.
            let permit_deadline = match hold.as_mut() {
                Some(h) => {
                    let idle = h.is_held()
                        && buffer.is_empty()
                        && pending.is_empty()
                        && held_unnamed.is_none()
                        && !refused_flush
                        && (refused_open || (!tr.ext_dispatched && tr.ext_copy_end.is_none()))
                        && (refused_open
                            || !session
                                .copy_in_progress
                                .load(std::sync::atomic::Ordering::Relaxed));
                    h.set_idle(idle);
                    if idle {
                        h.budget()
                            .idle_permit_timeout()
                            .map(|t| tokio::time::Instant::now() + t)
                    } else {
                        None
                    }
                }
                None => None,
            };
            let mut permit_first =
                permit_deadline.is_some_and(|p| client_deadline.is_none_or(|g| p <= g));
            let idle_deadline = if permit_first {
                permit_deadline
            } else {
                client_deadline
            };
            // Borrowed, not cloned (Fix S4): the watch target is the session's
            // current node (with read offload, its home node; the node holding
            // an open cycle its client is still in, while there is one), so
            // cloning the node key into a fresh `String` on every
            // client-message wait was pure hot-path overhead.
            let watch_node: Option<&str> = Self::watch_target(
                conns,
                home_node.as_deref(),
                current_node.as_deref(),
                cycle_node
                    .as_deref()
                    .filter(|_| !refused_open && (tr.ext_dispatched || tr.ext_copy_end.is_some())),
                offload_on,
                session,
            );
            // Only the home socket's loss is the session's: a standby socket
            // used for offloaded reads holds nothing the session cannot rebuild,
            // so its FATAL is not forwarded and its loss is silent.
            let watching_home = !offload_on || watch_node == home_node.as_deref();
            // ac858d61400b: the watched connection holds the cycle its
            // client is still in (a Flush dispatched part of it, or an
            // extended COPY awaits its Sync), whose answers reach the client
            // only through this watch. Its loss is settled below; its own
            // FATAL is never relayed then, as the proxy tells the client once.
            let cycle_watched = !refused_open
                && (tr.ext_dispatched || tr.ext_copy_end.is_some())
                && watch_node.is_some_and(|w| cycle_node.as_deref() == Some(w));
            let mut read = Self::read_next_client_message(
                stream,
                &mut buffer,
                conns,
                watch_node,
                idle_deadline,
                state,
                IdleWatch {
                    session,
                    keep_session: cycle_watched
                        || session.tr_mode != TrMode::None
                        || !watching_home,
                },
            )
            .await?;
            if read == ClientRead::BackendGone {
                // ac858d61400b: the lost connection held the cycle its client
                // is still in, and the client may be waiting for answers it
                // will never get: the rest of a Flush-dispatched cycle's
                // results, which only the watch relays, or the answer to its
                // extended COPY's end, which the backend never gave. The
                // proxy would wait for the client in turn, holding the
                // tenant's permit. A Flush-dispatched cycle is lost for good
                // (a fault on it at the next message closes the session:
                // `tr_response_action`), and so is the COPY's (its rows died
                // uncommitted): the session ends now, its client told why,
                // and the permit returns. A client whose COPY end was
                // answered owes only the Sync, which the fault path settles.
                if let Some(node) = watch_node.filter(|_| {
                    cycle_watched && (tr.ext_dispatched || !Self::copy_end_answered(session))
                }) {
                    Self::end_session_with_lost_cycle(stream, state, session, node).await;
                    break;
                }
                // `read_next_client_message` dropped the watched backend
                // connection, which died while the session was idle; the
                // session survives and the next query redials. Recorded here
                // rather than inside `read_next_client_message` (where O2/S4
                // moved the watch loop) because the helper is session-agnostic
                // — it takes no `&ClientSession` and owns no `TrSession` — and
                // the missing `conns` entry reconstructs the same signal
                // losslessly (see `note_watched_gone`).
                if let Some(gone) = watch_node.map(str::to_string) {
                    Self::note_watched_gone(
                        gone,
                        watching_home,
                        tr,
                        session,
                        [&mut home_node, &mut current_node],
                    );
                }
                // ac858d61400b: what kept the permit of a session idle since
                // its last request may have died with that connection (its
                // session-level advisory locks, a cycle a refusal left open
                // on it), so it returns now, not at the client's next
                // message; a transaction (which the next request may replay)
                // or a cycle the client is still in keeps it. The wait goes
                // on, on the client alone, under the deadlines already armed:
                // a returned permit takes its own along.
                Self::tenant_query_release_if_idle(
                    hold,
                    session,
                    Self::cycle_keeps_permit(
                        !pending.is_empty() || held_unnamed.is_some(),
                        tr,
                        refused_open,
                        cycle_node.as_deref(),
                        conns,
                    ),
                    Self::session_locks_held(track_session_locks, conns),
                );
                permit_first &= hold.as_ref().is_some_and(|h| h.is_held());
                let deadline = if permit_first {
                    idle_deadline
                } else {
                    client_deadline
                };
                read = Self::read_client_bytes(stream, &mut buffer, deadline).await?;
            }
            let n: usize = match read {
                ClientRead::Bytes(n) => n,
                ClientRead::IdleTimeout if permit_first => {
                    // `[multi_tenancy] idle_permit_timeout_ms`: the permit
                    // returns when the loop ends (after the teardown parking).
                    if let Some(h) = hold.as_ref() {
                        h.budget().note_idle_permit_timeout();
                    }
                    Self::terminate_idle_permit_session(stream, state, session).await;
                    break;
                }
                ClientRead::IdleTimeout => {
                    // PostgreSQL's `idle_session_timeout` behaviour: tell the
                    // client why, then close. A session idle INSIDE an open
                    // transaction is terminated too — PostgreSQL splits that
                    // case out into the separate
                    // `idle_in_transaction_session_timeout` GUC, which this
                    // proxy does not implement. Leaving the loop (rather than
                    // returning here) keeps the normal teardown path, so under
                    // transaction/statement pooling the session's still-idle
                    // backend connections are parked for reuse as usual. A
                    // client in the middle of a Flush-split cycle gets no
                    // message; one a refusal answered is idle like any other.
                    if !tr.ext_dispatched || refused_open {
                        Self::terminate_idle_session(stream, state, session).await;
                    }
                    break;
                }
                // Settled above: `read_client_bytes` never ends with it.
                ClientRead::BackendGone => unreachable!("handled above"),
            };
            if let Some(h) = hold.as_mut() {
                h.set_idle(false);
            }

            if n == 0 {
                // Client disconnected
                break;
            }

            state
                .metrics
                .bytes_received
                .fetch_add(n as u64, Ordering::Relaxed);

            // Bound a single in-flight message: refuse before the accumulation
            // buffer for one (possibly malicious) oversized frame can exhaust
            // memory. A legitimate client never needs a single >64 MiB message.
            if buffer.len() > state.limits.max_pending_bytes {
                let emsg =
                    Self::create_error_response("53400", "message exceeds per-session size limit");
                let _ = stream.write_all(&emsg).await;
                let _ = stream.write_all(&Self::create_ready_for_query(b'I')).await;
                tracing::warn!(
                    client = %session.client_addr,
                    bytes = buffer.len(),
                    "inbound message exceeds size cap; closing connection"
                );
                return Ok(());
            }

            // Process all complete messages in buffer
            while let Some(msg) = codec.decode_message(&mut buffer)? {
                if cycle_refused {
                    // PostgreSQL's extended-query error recovery: after the
                    // cycle's ErrorResponse everything up to Sync is ignored
                    // (Flush and Query included); Sync gets ReadyForQuery.
                    // The backend never saw the cycle.
                    match msg.msg_type {
                        MessageType::Terminate => return Ok(()),
                        // A Flush batch of the cycle was answered with an
                        // error before it reached any backend
                        // (`refused_flush`): the Sync arm answers this Sync
                        // the same way (`cycle_refused` tells it), settling
                        // what the cycle left recorded and returning its
                        // permit.
                        MessageType::Sync if refused_flush => {}
                        MessageType::Sync => {
                            cycle_refused = false;
                            let rfq = Self::create_ready_for_query(b'I');
                            stream.write_all(&rfq).await.map_err(|e| {
                                ProxyError::Network(format!("Client write error: {}", e))
                            })?;
                            state
                                .metrics
                                .bytes_sent
                                .fetch_add(rfq.len() as u64, Ordering::Relaxed);
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        _ => continue,
                    }
                }
                match msg.msg_type {
                    MessageType::Terminate => return Ok(()),

                    // ---- Simple query protocol ----
                    MessageType::Query => {
                        // A fresh tally for the query's response (see
                        // `ClientSession::cycle_completions`).
                        Self::tally_reset(session);
                        ext_copy = false;
                        // A COPY lost before its end is over (PostgreSQL ends
                        // copy-in at any other message).
                        copy_lost = false;
                        // The query continues a cycle the backend holds open
                        // (a Flush dispatched part of it, or an extended COPY
                        // awaits its Sync): it is part of a request admitted
                        // already, so it goes to the connection holding the
                        // cycle (`cycle_node`) and is never refused by the
                        // rate limiter (which paces it) or an open circuit,
                        // nor answered from a cache; that backend's reply
                        // settles the cycle. Until it reaches the backend the
                        // cycle stays open behind the query's ReadyForQuery
                        // (`refused_open`). So does a query that arrives in
                        // copy-in (`copy_in`): PostgreSQL ends the COPY at it,
                        // failing the COPY without running it, so it goes to
                        // the COPY's own connection (`current_node`), whose
                        // ReadyForQuery ends the session's COPY.
                        let cycle_open = tr.ext_dispatched || tr.ext_copy_end.is_some();
                        let copy_in = session
                            .copy_in_progress
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let continues_cycle = cycle_open || copy_in;
                        refused_open = continues_cycle;
                        // Whatever answers the query ends with ReadyForQuery.
                        refused_flush = false;
                        // PostgreSQL destroys the unnamed statement, and the
                        // unnamed portal, at every simple Query before it runs
                        // anything: however this one ends (refused, answered
                        // by a cache or a plugin, or run), the session has
                        // neither afterwards. The socket it reaches destroys
                        // its own (`forward_simple_query`); the others close
                        // theirs before a later batch could use them. Only
                        // while the cycle this query continues stays open (a
                        // plugin Block) are that cycle's own later batches not
                        // checked, and what they bind is unknown. A partly
                        // received cycle reaches the backend after the query,
                        // its own unnamed Parse included: what it binds is
                        // still recorded.
                        Self::orphan_unnamed(
                            conns,
                            UnnamedStale::query(
                                session
                                    .last_rfq_status
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    != b'I',
                            ),
                        );
                        if pending.is_empty() && held_unnamed.is_none() {
                            unnamed_bound = if continues_cycle {
                                Bound::Unknown
                            } else {
                                Bound::Other
                            };
                            unnamed_node = None;
                        }
                        if tr.ext_copy_end.is_some() {
                            // An extended COPY ended without the Sync that
                            // settles it (libpq and tokio-postgres always send
                            // one): its completion may precede the query's
                            // response, and its transaction is not replayable.
                            // The query settles it once it reaches the backend
                            // (below); blocked before that by a plugin, the
                            // backend still awaits the Sync.
                            session
                                .cycle_counted
                                .store(false, std::sync::atomic::Ordering::Relaxed);
                            session
                                .tr_replay_tainted
                                .store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        if tr.track_gucs && ext_set.is_open() {
                            // Output the cycle produced may still precede the
                            // query's own response.
                            session
                                .cycle_counted
                                .store(false, std::sync::atomic::Ordering::Relaxed);
                            // A Query after a Flush leaves the extended cycle
                            // without its Sync: settle what may have run of it.
                            let outcome = ext_set.interrupt(
                                session
                                    .last_rfq_status
                                    .load(std::sync::atomic::Ordering::Relaxed),
                            );
                            if Self::tr_ext_set_outcome(tr, outcome, false, session, state)
                                && offload_on
                            {
                                session
                                    .reads_pinned
                                    .store(true, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        // Anomaly detector — record every Query message before
                        // the plugin hook so a detection lands in the audit
                        // trail even if a plugin later blocks.
                        #[cfg(feature = "anomaly-detection")]
                        Self::record_anomaly_observation(&msg, state, session);

                        // Plugin pre-query hook — may rewrite the SQL, block,
                        // or return a cached response.
                        let (msg, action) = Self::apply_pre_query_hook(msg, state, session);
                        // Without wasm-plugins the hook can only forward.
                        #[cfg(not(feature = "wasm-plugins"))]
                        let PreQueryAction::Forward = action;

                        // A plugin's Block, or its cached result, answers the
                        // query before any backend.
                        #[cfg(feature = "wasm-plugins")]
                        let answered = match &action {
                            PreQueryAction::Block(reason) => {
                                tracing::info!(reason = %reason, "pre-query plugin blocked query");
                                Self::send_block_response(
                                    stream,
                                    reason,
                                    session
                                        .last_rfq_status
                                        .load(std::sync::atomic::Ordering::Relaxed),
                                    state,
                                )
                                .await?;
                                true
                            }
                            PreQueryAction::Cached(bytes) if !continues_cycle => {
                                match Self::synthesise_cached_response(
                                    bytes,
                                    session
                                        .last_rfq_status
                                        .load(std::sync::atomic::Ordering::Relaxed),
                                ) {
                                    Ok(reply) => {
                                        stream.write_all(&reply).await.map_err(|e| {
                                            ProxyError::Network(format!("Write error: {}", e))
                                        })?;
                                        state
                                            .metrics
                                            .bytes_sent
                                            .fetch_add(reply.len() as u64, Ordering::Relaxed);
                                        true
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "failed to synthesise cached response; falling back to backend");
                                        false
                                    }
                                }
                            }
                            _ => false,
                        };
                        #[cfg(feature = "wasm-plugins")]
                        if answered {
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            // The client is idle again (ReadyForQuery). A
                            // permit the session still holds returns, after
                            // the pool parked the connection, as after any
                            // answered query, unless the session is not idle
                            // (a transaction, a cycle still open on the
                            // backend). A Query after a Flush batch refused
                            // before any backend never gets here: it is
                            // discarded with the rest of that cycle.
                            #[cfg(feature = "pool-modes")]
                            if !tr.ext_dispatched && tr.ext_copy_end.is_none() {
                                Self::release_to_pool_if_idle(
                                    conns,
                                    current_node.as_deref(),
                                    session,
                                    state,
                                    config,
                                )
                                .await;
                            }
                            Self::tenant_query_release_if_idle(
                                hold,
                                session,
                                Self::cycle_keeps_permit(
                                    !pending.is_empty() || held_unnamed.is_some(),
                                    tr,
                                    refused_open,
                                    cycle_node.as_deref(),
                                    conns,
                                ),
                                Self::session_locks_held(track_session_locks, conns),
                            );
                            continue;
                        }

                        // ac858d61400b: a request that starts while the
                        // session is idle takes one of its tenant's
                        // active-query permits (refused requests are never
                        // mirrored). Inside a transaction the session already
                        // holds one.
                        if let Some(h) = hold.as_mut() {
                            if !h.is_held()
                                && !session
                                    .in_transaction
                                    .load(std::sync::atomic::Ordering::Relaxed)
                            {
                                let gate = h.budget();
                                match gate.try_acquire_query() {
                                    Some(p) => h.hold(p),
                                    None => match Self::gate_slow_path(
                                        GateCtx {
                                            gate,
                                            client: stream,
                                            buffer: &mut buffer,
                                            conns,
                                            tr,
                                            nodes: [&mut home_node, &mut current_node],
                                            cycle_node: cycle_node.as_deref(),
                                            offload_on,
                                            session,
                                            state,
                                            config,
                                        },
                                        crate::protocol::query_text(&msg.payload),
                                    )
                                    .await?
                                    {
                                        QueryGateWait::Admitted(p, _) => h.hold(p),
                                        QueryGateWait::Exempt => {}
                                        QueryGateWait::ClientGone => return Ok(()),
                                        refused => {
                                            // Nothing ran: the status stays 'I'.
                                            Self::write_query_refusal(stream, state, refused, true)
                                                .await?;
                                            continue;
                                        }
                                    },
                                }
                            }
                        }

                        // Traffic mirror: offer the (final, post-rewrite)
                        // statement to the secondary backend. Non-blocking —
                        // never delays the client path.
                        if let Some(ref mirror) = state.mirror {
                            if let Some(sql) = crate::protocol::query_text(&msg.payload) {
                                mirror.offer(sql, Self::is_write_query(sql));
                            }
                        }

                        // Aborted-transaction emulation after an in-session
                        // failover: the client's transaction died with the old
                        // backend, so until it ends the transaction every other
                        // statement is refused exactly as PostgreSQL would
                        // (25P02) — never run in autocommit on the new backend.
                        if tr.tx_aborted {
                            let sql = crate::protocol::query_text(&msg.payload).unwrap_or("");
                            let mut resp = if Self::tr_ends_transaction(sql) {
                                tr.tx_aborted = false;
                                Self::note_ready_for_query(session, b'I', false);
                                Self::journal_discard(session, b'I');
                                let mut r = Self::create_command_complete("ROLLBACK");
                                r.extend_from_slice(&Self::create_ready_for_query(b'I'));
                                r
                            } else {
                                Self::note_ready_for_query(session, b'E', true);
                                Self::journal_discard(session, b'E');
                                let mut r = Self::create_error_response(
                                    "25P02",
                                    "current transaction is aborted, commands ignored until end of transaction block (aborted by proxy failover)",
                                );
                                r.extend_from_slice(&Self::create_ready_for_query(b'E'));
                                r
                            };
                            stream.write_all(&resp).await.map_err(|e| {
                                ProxyError::Network(format!("Client write error: {}", e))
                            })?;
                            // Nothing reached a backend: no setting changed.
                            tr.skip_settings = true;
                            Self::tr_after_simple(tr, &msg, session, state).await;
                            // Whatever the lost backend had open died with it.
                            tr.ext_dispatched = false;
                            tr.ext_copy_end = None;
                            refused_open = false;
                            state
                                .metrics
                                .bytes_sent
                                .fetch_add(resp.len() as u64, Ordering::Relaxed);
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            // A synthesized ROLLBACK ended the transaction:
                            // under transaction/statement pooling the
                            // replacement connection is parked first (a 25P02
                            // leaves the session in its transaction, and it is
                            // kept), then the tenant permit returns.
                            #[cfg(feature = "pool-modes")]
                            Self::release_to_pool_if_idle(
                                conns,
                                current_node.as_deref(),
                                session,
                                state,
                                config,
                            )
                            .await;
                            Self::tenant_query_release_if_idle(
                                hold,
                                session,
                                Self::cycle_keeps_permit(
                                    !pending.is_empty() || held_unnamed.is_some(),
                                    tr,
                                    refused_open,
                                    cycle_node.as_deref(),
                                    conns,
                                ),
                                Self::session_locks_held(track_session_locks, conns),
                            );
                            resp.clear();
                            continue;
                        }

                        #[cfg(feature = "wasm-plugins")]
                        let forward_start = std::time::Instant::now();
                        let mut fault: Option<BackendFault> = None;
                        // Read offload: a statement that leaves connection-local
                        // state keeps the session's reads on its home node.
                        if offload_on
                            && !session
                                .reads_pinned
                                .load(std::sync::atomic::Ordering::Relaxed)
                            && crate::protocol::query_text(&msg.payload)
                                .is_some_and(Self::pins_reads)
                        {
                            session
                                .reads_pinned
                                .store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                        Self::rehome_if_moved(
                            conns,
                            [&mut home_node, &mut current_node],
                            offload_on,
                            tr,
                            session,
                            state,
                            config,
                        )
                        .await;
                        let fr = match tr.lost_backend.take() {
                            // The session's backend died while idle: the request
                            // was certainly not delivered anywhere.
                            Some(lost) => {
                                fault = Some(BackendFault {
                                    node: lost,
                                    phase: FaultPhase::NotDelivered,
                                    progress: ResponseProgress::default(),
                                    kind: None,
                                    error: "backend connection closed while the session was idle"
                                        .to_string(),
                                });
                                Err(ProxyError::Connection(
                                    "backend connection lost".to_string(),
                                ))
                            }
                            None => {
                                Self::forward_simple_query(
                                    ForwardCtx {
                                        client: stream,
                                        conns,
                                        current_node: current_node.as_deref(),
                                        reads: &ReadRouting {
                                            offload_on,
                                            may_offload: offload_on
                                                && !tr.guc_cap_hit
                                                && !tr.serializable_default
                                                && !session
                                                    .reads_pinned
                                                    .load(std::sync::atomic::Ordering::Relaxed),
                                            batch_reads: false,
                                            batch_whole: true,
                                            gucs: &tr.gucs,
                                            guc_epoch: tr.guc_epoch,
                                            read_your_writes: ryw,
                                        },
                                        session,
                                        state,
                                        config,
                                        fault: &mut fault,
                                        continues_cycle: cycle_node
                                            .as_deref()
                                            .filter(|_| cycle_open)
                                            .or_else(|| current_node.as_deref().filter(|_| copy_in))
                                            .map(|node| OpenCycle { node, runs: true }),
                                    },
                                    &msg,
                                )
                                .await
                            }
                        };
                        #[cfg(feature = "wasm-plugins")]
                        Self::fire_post_query_hook(
                            &msg,
                            session,
                            state,
                            &fr,
                            forward_start.elapsed(),
                        );
                        // The backend that held the session's open cycle failed
                        // (the recovery cleared `ext_dispatched`).
                        let mut faulted = false;
                        let (used_node, sent) = match fr {
                            Ok(v) => v,
                            Err(e) => match fault.take() {
                                Some(f) => {
                                    faulted = true;
                                    match Self::tr_handle_fault(
                                        TrRecovery {
                                            client: stream,
                                            conns,
                                            current_node: &mut current_node,
                                            tr,
                                            cycle_node: cycle_node.as_deref(),
                                            registry: &stmt_registry,
                                            session,
                                            state,
                                            config,
                                        },
                                        f,
                                        InFlight::Simple(&msg),
                                    )
                                    .await?
                                    {
                                        Some(v) => v,
                                        None => return Ok(()),
                                    }
                                }
                                None => {
                                    if matches!(e, ProxyError::NoHealthyNodes) {
                                        if tr.ext_dispatched {
                                            return Ok(());
                                        }
                                        Self::send_no_healthy_nodes(stream, session, true).await;
                                        return Ok(());
                                    }
                                    return Err(e);
                                }
                            },
                        };
                        if offload_on {
                            Self::note_home(
                                &mut home_node,
                                used_node.as_deref(),
                                session,
                                state,
                                config,
                            );
                        }
                        // The query ends a cycle the backend holds open (one a
                        // Flush dispatched, or an extended COPY awaiting its
                        // Sync) only when it reached that backend (a query
                        // continuing a cycle goes nowhere else: `OpenCycle`),
                        // or that backend failed. Blocked before it by a route
                        // plugin, it leaves the cycle open there: the permit
                        // stays held and the connection pinned until a later
                        // request settles it, and the session idles meanwhile
                        // (`refused_open`). After a fault the query reached a
                        // backend only when the recovery re-ran it there: one
                        // the recovery answered with an error (the session
                        // moved to the replacement all the same) ran nowhere.
                        let reached = used_node.is_some() && (!faulted || tr.fault_resent);
                        let settled = faulted || reached;
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        // The query ended the COPY it reached in copy-in (its
                        // ReadyForQuery is in): the COPY failed and loaded
                        // nothing, as at a CopyFail.
                        if copy_in
                            && reached
                            && !session
                                .copy_in_progress
                                .load(std::sync::atomic::Ordering::Relaxed)
                        {
                            Self::copy_drained(tr, false, session, state, config).await;
                        }
                        Self::tr_after_simple(tr, &msg, session, state).await;
                        if settled {
                            tr.ext_dispatched = false;
                            tr.ext_copy_end = None;
                            refused_open = false;
                        }
                        // Only a statement a backend answered can have taken
                        // or released a session-level lock, on the connection
                        // that answered it: one refused or answered before any
                        // backend (rate limit, open circuit, route-plugin
                        // Block, a cache hit, a failover's error) left them as
                        // they were, and `last_response_error` still describes
                        // an earlier reply. Locks of a connection the session
                        // lost died with it.
                        if track_session_locks && reached {
                            if let (Some(sql), Some(bc)) = (
                                crate::protocol::query_text(&msg.payload),
                                current_node.as_ref().and_then(|n| conns.get_mut(n)),
                            ) {
                                Self::note_session_locks(&mut bc.session_locks, sql, session);
                            }
                        }
                        // Transaction/Statement pooling: park the connection
                        // back to the shared pool once the session is idle
                        // (never with a cycle still open on it).
                        #[cfg(feature = "pool-modes")]
                        if !tr.ext_dispatched && tr.ext_copy_end.is_none() {
                            Self::release_to_pool_if_idle(
                                conns,
                                current_node.as_deref(),
                                session,
                                state,
                                config,
                            )
                            .await;
                        }
                        // ...then return the tenant permit, so the waiter it
                        // wakes finds the connection parked. A partly
                        // received extended cycle keeps it.
                        Self::tenant_query_release_if_idle(
                            hold,
                            session,
                            Self::cycle_keeps_permit(
                                !pending.is_empty() || held_unnamed.is_some(),
                                tr,
                                refused_open,
                                cycle_node.as_deref(),
                                conns,
                            ),
                            Self::session_locks_held(track_session_locks, conns),
                        );
                        state.metrics.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                        state
                            .metrics
                            .queries_processed
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    // ---- Extended query protocol: accumulate until Sync/Flush ----
                    MessageType::Parse
                    | MessageType::Bind
                    | MessageType::Describe
                    | MessageType::Execute
                    | MessageType::Close => {
                        if pending.is_empty() && held_unnamed.is_none() && !tr.ext_dispatched {
                            // ac858d61400b: the cycle's first message takes
                            // one of the tenant's active-query permits, before
                            // anything of the cycle is recorded, so a refusal
                            // leaves nothing to undo.
                            if let Some(h) = hold.as_mut() {
                                if !h.is_held()
                                    && !session
                                        .in_transaction
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                {
                                    let gate = h.budget();
                                    match gate.try_acquire_query() {
                                        Some(p) => h.hold(p),
                                        None => match Self::gate_slow_path(
                                            GateCtx {
                                                gate,
                                                client: stream,
                                                buffer: &mut buffer,
                                                conns,
                                                tr,
                                                nodes: [&mut home_node, &mut current_node],
                                                cycle_node: cycle_node.as_deref(),
                                                offload_on,
                                                session,
                                                state,
                                                config,
                                            },
                                            None,
                                        )
                                        .await?
                                        {
                                            QueryGateWait::Admitted(p, _) => h.hold(p),
                                            QueryGateWait::Exempt => {}
                                            QueryGateWait::ClientGone => return Ok(()),
                                            refused => {
                                                // The ErrorResponse goes out at
                                                // once; this message is the
                                                // first of the discarded cycle.
                                                Self::write_query_refusal(
                                                    stream, state, refused, false,
                                                )
                                                .await?;
                                                // PostgreSQL destroys the
                                                // unnamed statement before it
                                                // parses an unnamed Parse, so
                                                // even one that fails leaves
                                                // none.
                                                if msg.msg_type == MessageType::Parse
                                                    && Self::parse_stmt_name(&msg.payload)
                                                        .is_empty()
                                                {
                                                    Self::orphan_unnamed(
                                                        conns,
                                                        UnnamedStale::STATEMENT,
                                                    );
                                                    unnamed_bound = Bound::Other;
                                                    unnamed_node = None;
                                                }
                                                cycle_refused = true;
                                                continue;
                                            }
                                        },
                                    }
                                }
                            }
                            // A new cycle: a fresh tally for its responses.
                            Self::tally_reset(session);
                            copy_lost = false;
                            if tr.ext_copy_end.is_some() {
                                // An extended COPY's completion may still be
                                // in flight ahead of them.
                                session
                                    .cycle_counted
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        // Whether this message is appended to `pending`. An
                        // unnamed Parse held aside for promotion is the lone
                        // exception (resolved at the batch boundary).
                        let mut add_to_pending = true;
                        match msg.msg_type {
                            MessageType::Parse => {
                                // Register named statements so they can be
                                // re-prepared on a different backend later, and
                                // borrow the query (2nd cstring) for routing.
                                let name = Self::parse_stmt_name(&msg.payload);
                                let unnamed = name.is_empty();
                                if tr.track_gucs {
                                    let sql = Self::parse_payload_sql_bytes(&msg.payload);
                                    let set_like = sql.is_some_and(Self::tr_is_set_like_bytes);
                                    let replaced = if unnamed {
                                        matches!(unnamed_bound, Bound::Set(_) | Bound::Unknown)
                                    } else {
                                        stmt_registry
                                            .get(name)
                                            .and_then(|b| Self::parse_msg_sql_bytes(b))
                                            .is_some_and(Self::tr_is_set_like_bytes)
                                    };
                                    ext_set.on_parse(!unnamed, set_like, replaced);
                                    if unnamed {
                                        unnamed_bound = sql.map_or(Bound::Unknown, Self::tr_bound);
                                    }
                                }
                                if !unnamed {
                                    let name = name.to_string();
                                    let existed = stmt_registry.contains_key(&name);
                                    // Cap distinct prepared statements per session so a
                                    // client issuing unbounded named `Parse`s can't grow
                                    // `stmt_registry` without limit.
                                    if !existed
                                        && stmt_registry.len()
                                            >= state.limits.max_prepared_statements
                                    {
                                        let emsg = Self::create_error_response(
                                            "54000",
                                            "too many prepared statements for this session",
                                        );
                                        let _ = stream.write_all(&emsg).await;
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                        tracing::warn!(
                                            client = %session.client_addr,
                                            limit = state.limits.max_prepared_statements,
                                            "prepared-statement cap exceeded; closing connection"
                                        );
                                        return Ok(());
                                    }
                                    let encoded = msg.encode().freeze();
                                    // Bound the AGGREGATE bytes retained, not just the
                                    // count: a (possibly re-Parsed) statement that would
                                    // push the session over the byte cap is refused.
                                    let old_len =
                                        stmt_registry.get(&name).map(|b| b.len()).unwrap_or(0);
                                    let projected =
                                        stmt_registry_bytes.saturating_sub(old_len) + encoded.len();
                                    if projected > state.limits.max_prepared_bytes {
                                        let emsg = Self::create_error_response(
                                            "54000",
                                            "prepared-statement memory limit exceeded for this session",
                                        );
                                        let _ = stream.write_all(&emsg).await;
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                        tracing::warn!(
                                            client = %session.client_addr,
                                            limit = state.limits.max_prepared_bytes,
                                            "prepared-statement byte cap exceeded; closing connection"
                                        );
                                        return Ok(());
                                    }
                                    let previous = stmt_registry.insert(name.clone(), encoded);
                                    // A redefinition is classified at its next
                                    // Bind; the memo holds the old bytes no longer.
                                    if track_session_locks {
                                        stmt_locks.remove(&name);
                                    }
                                    if tr.tx_aborted {
                                        aborted_undo.push((name.clone(), previous));
                                    }
                                    stmt_registry_bytes = projected;
                                    Self::remember_batch_name(
                                        &mut batch_defines,
                                        &name,
                                        state.limits.max_prepared_statements,
                                    );
                                    Self::forget_batch_close(&mut batch_left_closed, &name);
                                }
                                // A session-level advisory lock keeps the
                                // tenant permit (session pooling) once the
                                // batch reaches a backend (`batch_locks`); the
                                // extended protocol never clears it.
                                if track_session_locks
                                    && !batch_locks
                                    && Self::parse_payload_sql_bytes(&msg.payload)
                                        .is_some_and(Self::takes_session_lock)
                                {
                                    batch_locks = true;
                                }
                                if pending_route_sql.is_none() {
                                    if let Some(end) = msg.payload.iter().position(|&b| b == 0) {
                                        if let Some(q) =
                                            crate::protocol::query_text(&msg.payload[end + 1..])
                                        {
                                            if !q.is_empty() {
                                                pending_route_sql = Some(q.to_string());
                                                #[cfg(feature = "anomaly-detection")]
                                                Self::record_anomaly_sql(q, state, session);
                                            }
                                        }
                                    }
                                }
                                // Edge invalidation metadata: classify every
                                // Parse'd statement ONCE (is it DML? which
                                // tables?) so Sync-time invalidation of
                                // re-executed prepared writes is a map hit.
                                // Extended-protocol statements can dirty
                                // session state too (JDBC-style SET) — the
                                // sticky edge-ineligibility flag must catch
                                // them, not just simple-protocol text.
                                #[cfg(feature = "edge-proxy")]
                                if config.edge.enabled {
                                    if let Some(end) = msg.payload.iter().position(|&b| b == 0) {
                                        if let Some(q) =
                                            crate::protocol::query_text(&msg.payload[end + 1..])
                                        {
                                            if !session
                                                .edge_ineligible
                                                .load(std::sync::atomic::Ordering::Relaxed)
                                                && Self::stmt_leaves_session_state(q)
                                            {
                                                session.edge_ineligible.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Relaxed,
                                                );
                                            }
                                            let meta = if Self::is_edge_dml_sql(q) {
                                                Some(crate::edge::fingerprint::write_tables(q))
                                            } else if Self::is_edge_procedural_sql(q)
                                                || Self::is_edge_txn_end_sql(q)
                                            {
                                                // Opaque write (EXECUTE/CALL/DO) or a
                                                // COMMIT/END that makes in-transaction
                                                // writes visible — memoize as the
                                                // empty-set wildcard so the batch's
                                                // Sync hook full-flushes.
                                                Some(Vec::new())
                                            } else {
                                                None
                                            };
                                            if unnamed {
                                                edge_unnamed_meta = meta;
                                            } else {
                                                let key =
                                                    Self::parse_stmt_name(&msg.payload).to_string();
                                                if tr.tx_aborted {
                                                    let previous =
                                                        edge_stmt_meta.insert(key.clone(), meta);
                                                    aborted_edge_undo.push((key, previous));
                                                } else {
                                                    edge_stmt_meta.insert(key, meta);
                                                }
                                            }
                                        }
                                    }
                                }
                                // Read offload: a batch is offloaded only when
                                // every statement it Parses is a standby-safe
                                // read; a statement that leaves connection-local
                                // state pins the session's reads to its home
                                // (SET/RESET/DISCARD are decided at the Sync).
                                if offload_on {
                                    let sql = msg
                                        .payload
                                        .iter()
                                        .position(|&b| b == 0)
                                        .and_then(|end| {
                                            crate::protocol::query_text(&msg.payload[end + 1..])
                                        })
                                        .unwrap_or("");
                                    if batch_reads
                                        && !Self::standby_safe_read(sql, &state.tr_read_policy)
                                    {
                                        batch_reads = false;
                                    }
                                    if Self::pins_reads(sql) {
                                        session
                                            .reads_pinned
                                            .store(true, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    if unnamed {
                                        batch_parsed_unnamed = true;
                                    }
                                }
                                // Promotion: park an unnamed Parse that opens a
                                // fresh batch. Its signature is the payload after
                                // the empty statement-name NUL (query + param
                                // types). Anything that breaks the clean shape
                                // (a second Parse, a non-empty `pending`) un-parks
                                // it back into `pending` to preserve wire order.
                                if promote_unnamed
                                    && unnamed
                                    && pending.is_empty()
                                    && held_unnamed.is_none()
                                {
                                    let sig = bytes::Bytes::copy_from_slice(&msg.payload[1..]);
                                    held_unnamed = Some((msg.encode().freeze(), sig));
                                    add_to_pending = false;
                                } else if let Some((held_msg, _)) = held_unnamed.take() {
                                    let mut combined =
                                        BytesMut::with_capacity(held_msg.len() + pending.len());
                                    combined.extend_from_slice(&held_msg);
                                    combined.extend_from_slice(&pending);
                                    pending = combined;
                                }
                            }
                            MessageType::Bind => {
                                let stmt_ref = Self::bind_stmt_ref(&msg.payload);
                                if stmt_ref.is_none() {
                                    batch_binds_unnamed = true;
                                }
                                if tr.track_gucs {
                                    let bound = match stmt_ref {
                                        Some(name) => stmt_registry
                                            .get(name)
                                            .and_then(|b| Self::parse_msg_sql_bytes(b))
                                            .map_or(Bound::Unknown, Self::tr_bound),
                                        None => unnamed_bound.clone(),
                                    };
                                    ext_set.on_bind(Self::leading_cstring(&msg.payload), bound);
                                }
                                if offload_on && batch_reads {
                                    batch_reads = match stmt_ref {
                                        Some(name) => Self::named_standby_read(
                                            &mut stmt_reads,
                                            &stmt_registry,
                                            name,
                                            &state.tr_read_policy,
                                        ),
                                        // The unnamed statement: only this
                                        // batch's own (classified) Parse.
                                        None => batch_parsed_unnamed,
                                    };
                                }
                                // A named statement that takes a session-level
                                // advisory lock takes it at every execution,
                                // not only the one it was Parsed for (a simple
                                // unlock may have cleared the mark since, or
                                // the batch may run on another connection).
                                // The unnamed statement lives only on the
                                // connection whose batch Parsed it, which that
                                // batch marked: a simple Query (the only thing
                                // that clears a mark) destroys it, and so does
                                // the loss of that connection.
                                if track_session_locks
                                    && !batch_locks
                                    && stmt_ref.is_some_and(|name| {
                                        Self::named_takes_session_lock(
                                            &mut stmt_locks,
                                            &stmt_registry,
                                            name,
                                        )
                                    })
                                {
                                    batch_locks = true;
                                }
                                // Unnamed statement: not tracked in
                                // batch_refs, but the edge invalidation must
                                // still see its execution.
                                #[cfg(feature = "edge-proxy")]
                                if stmt_ref.is_none() {
                                    edge_batch_bound_unnamed = true;
                                }
                                if let Some(name) = stmt_ref {
                                    Self::remember_batch_name(
                                        &mut batch_refs,
                                        name,
                                        state.limits.max_prepared_statements,
                                    );
                                }
                            }
                            MessageType::Describe => {
                                if tr.track_gucs {
                                    ext_set.on_other_message();
                                }
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    Self::remember_batch_name(
                                        &mut batch_refs,
                                        name,
                                        state.limits.max_prepared_statements,
                                    );
                                }
                            }
                            MessageType::Execute if tr.track_gucs => {
                                ext_set.on_execute(Self::leading_cstring(&msg.payload));
                            }
                            MessageType::Close => {
                                if tr.track_gucs {
                                    ext_set.on_other_message();
                                }
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    Self::remember_batch_name(
                                        &mut batch_closes,
                                        name,
                                        state.limits.max_prepared_statements,
                                    );
                                    Self::remember_batch_name(
                                        &mut batch_left_closed,
                                        name,
                                        state.limits.max_prepared_statements,
                                    );
                                }
                            }
                            _ => {}
                        }
                        if add_to_pending {
                            batch_executes |= msg.msg_type == MessageType::Execute;
                            msg.encode_into(&mut pending);
                        }
                    }

                    // ---- Extended batch boundary ----
                    MessageType::Sync | MessageType::Flush => {
                        // ac858d61400b: a bare Flush with no extended cycle open
                        // has nothing to flush: at an idle boundary (in a
                        // transaction or not) the backend has sent everything
                        // up to its last ReadyForQuery, in copy-in it ignores
                        // a Flush, and PostgreSQL answers nothing either way.
                        // Every session consumes it here. It runs nothing, so
                        // nothing may refuse it (a rate limit, an open circuit,
                        // a lost backend's recovery): a refused Flush would
                        // leave its client owing a Sync it never sends, and
                        // discard the requests after it. Forwarded, it would
                        // make the next request look like the continuation of
                        // a cycle nothing opened (no cache, no refusal, pinned
                        // to that node). A capped session, with or without its
                        // permit, could neither dispatch a cycle without the
                        // permit nor look mid-cycle (and so escape
                        // `idle_permit_timeout_ms`). Nor behind a cycle a
                        // refusal left open (`refused_open`): its client was
                        // answered ReadyForQuery, so to it the Flush is an idle
                        // one, and whatever that backend still holds of the
                        // cycle reaches it with the request that settles it.
                        if msg.msg_type == MessageType::Flush
                            && pending.is_empty()
                            && held_unnamed.is_none()
                            && (refused_open || (!tr.ext_dispatched && tr.ext_copy_end.is_none()))
                        {
                            continue;
                        }
                        let wait_ready = msg.msg_type == MessageType::Sync;
                        // This Sync ends a cycle discarded after one of its
                        // Flush batches was answered with an error before it
                        // reached any backend (`refused_flush`): it reaches
                        // none either, and is answered as one refused there.
                        let ends_refused = std::mem::take(&mut cycle_refused);
                        // The batch continues a cycle the backend holds open
                        // (an earlier Flush dispatched part of it, or an
                        // extended COPY awaits this Sync): part of a request
                        // admitted already, it goes to the connection holding
                        // the cycle (`cycle_node`) and is never refused by the
                        // rate limiter (which paces one that executes
                        // anything: `runs`) or an open circuit, so that
                        // backend's own reply settles the cycle and what the
                        // client is told matches what the backend did. So
                        // does a batch that arrives in copy-in (`copy_in`):
                        // PostgreSQL ends the COPY at it (see the simple
                        // Query's), on the COPY's own connection.
                        let continues_cycle = tr.ext_dispatched || tr.ext_copy_end.is_some();
                        let copy_in = session
                            .copy_in_progress
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let runs = std::mem::take(&mut batch_executes);
                        let takes_locks = std::mem::take(&mut batch_locks);
                        let status_before = session
                            .last_rfq_status
                            .load(std::sync::atomic::Ordering::Relaxed);
                        msg.encode_into(&mut pending);
                        let batch = pending.split().freeze();
                        // The batch opens with an unnamed Parse: the one the
                        // promotion held aside, or its first frame.
                        let leads_unnamed_parse = held_unnamed.is_some()
                            || (batch.first() == Some(&b'P') && batch.get(5) == Some(&0));
                        // Re-prepare any named statement this batch references
                        // but does not itself define, in case the target
                        // connection (after a switch/redial) is missing it.
                        let reprepare: Vec<String> = batch_refs
                            .iter()
                            .filter(|r| !batch_defines.contains(r))
                            .cloned()
                            .collect();
                        // Aborted-transaction emulation (see the simple-query
                        // path): only a transaction end goes through to the new
                        // backend; anything else is refused with 25P02. A bare
                        // Sync just yields the failed-transaction ReadyForQuery.
                        let mut skip_forward = false;
                        // The transaction end the emulation lets through.
                        let mut aborted_end = false;
                        if tr.tx_aborted {
                            let ends = pending_route_sql
                                .as_deref()
                                .map(Self::tr_ends_transaction)
                                .unwrap_or(false);
                            if ends {
                                tr.tx_aborted = false;
                                aborted_end = true;
                            } else {
                                skip_forward = true;
                                let bare_sync = batch.len() == 5 && batch[0] == b'S';
                                let mut resp = Vec::new();
                                if !bare_sync {
                                    resp.extend_from_slice(&Self::create_error_response(
                                        "25P02",
                                        "current transaction is aborted, commands ignored until end of transaction block (aborted by proxy failover)",
                                    ));
                                }
                                if wait_ready {
                                    resp.extend_from_slice(&Self::create_ready_for_query(b'E'));
                                    Self::note_ready_for_query(session, b'E', !bare_sync);
                                    Self::journal_discard(session, b'E');
                                }
                                if !resp.is_empty() {
                                    stream.write_all(&resp).await.map_err(|e| {
                                        ProxyError::Network(format!("Client write error: {}", e))
                                    })?;
                                    state
                                        .metrics
                                        .bytes_sent
                                        .fetch_add(resp.len() as u64, Ordering::Relaxed);
                                }
                                held_unnamed = None;
                                // Nothing was closed on the backend: forget the
                                // batch's Close requests without pruning.
                                batch_closes.clear();
                                batch_left_closed.clear();
                                // ...and nothing was Parsed: the backend kept
                                // the definitions the registry must keep too.
                                if !aborted_undo.is_empty() {
                                    // The memo never outlives a name the
                                    // undo removes (nor holds bytes it drops).
                                    if track_session_locks {
                                        for (name, _) in &aborted_undo {
                                            stmt_locks.remove(name);
                                        }
                                    }
                                    Self::undo_map_writes(
                                        &mut stmt_registry,
                                        std::mem::take(&mut aborted_undo),
                                    );
                                    stmt_registry_bytes =
                                        stmt_registry.values().map(|b| b.len()).sum();
                                }
                                #[cfg(feature = "edge-proxy")]
                                Self::undo_map_writes(
                                    &mut edge_stmt_meta,
                                    std::mem::take(&mut aborted_edge_undo),
                                );
                            }
                        }
                        aborted_undo.clear();
                        #[cfg(feature = "edge-proxy")]
                        aborted_edge_undo.clear();
                        // The backend that held the session's open cycle failed
                        // (the recovery cleared `ext_dispatched`).
                        let mut faulted = false;
                        let (used_node, sent) = if skip_forward {
                            (None, 0)
                        } else if ends_refused {
                            // Nothing of the cycle reached a backend: its
                            // status is the last one relayed.
                            let rfq = Self::create_ready_for_query(
                                session
                                    .last_rfq_status
                                    .load(std::sync::atomic::Ordering::Relaxed),
                            );
                            stream.write_all(&rfq).await.map_err(|e| {
                                ProxyError::Network(format!("Client write error: {}", e))
                            })?;
                            (None, rfq.len() as u64)
                        } else {
                            let mut fault: Option<BackendFault> = None;
                            Self::rehome_if_moved(
                                conns,
                                [&mut home_node, &mut current_node],
                                offload_on,
                                tr,
                                session,
                                state,
                                config,
                            )
                            .await;
                            let fr = match tr.lost_backend.take() {
                                Some(lost) => {
                                    fault = Some(BackendFault {
                                        node: lost,
                                        phase: FaultPhase::NotDelivered,
                                        progress: ResponseProgress::default(),
                                        kind: None,
                                        error:
                                            "backend connection closed while the session was idle"
                                                .to_string(),
                                    });
                                    Err(ProxyError::Connection(
                                        "backend connection lost".to_string(),
                                    ))
                                }
                                None => {
                                    // TR-09: classified only when holding
                                    // is configured.
                                    let hold = Self::tr_hold_bytes(session, state, || {
                                        Self::tr_extended_kind_prior(
                                            &batch,
                                            held_unnamed.as_ref().map(|(p, _)| p.as_ref()),
                                            state.limits.max_prepared_statements,
                                            &state.tr_read_policy,
                                            PriorCycles {
                                                statements: &tr.acked,
                                                portals: &tr.tx_portals,
                                                registry: Some(&stmt_registry),
                                            },
                                        )
                                    });
                                    // A batch with no Parse that executes named
                                    // statements routes by the first one's SQL,
                                    // so a prepared write never runs on a
                                    // standby the session last read from.
                                    // A batch that Binds the unnamed statement
                                    // without Parsing it runs where that
                                    // statement lives.
                                    let unnamed_home = (offload_on
                                        && batch_binds_unnamed
                                        && !batch_parsed_unnamed)
                                        .then(|| unnamed_node.clone())
                                        .flatten();
                                    let ref_route_sql = if offload_on
                                        && pending_route_sql.is_none()
                                        && !batch_parsed_unnamed
                                    {
                                        batch_refs
                                            .first()
                                            .and_then(|n| stmt_registry.get(n))
                                            .and_then(|b| Self::parse_msg_sql(b))
                                            .map(str::to_string)
                                    } else {
                                        None
                                    };
                                    let (route_sql, route_from) = match unnamed_home.as_deref() {
                                        Some(node) => (None, Some(node)),
                                        None => (
                                            pending_route_sql
                                                .as_deref()
                                                .or(ref_route_sql.as_deref()),
                                            current_node.as_deref(),
                                        ),
                                    };
                                    Self::forward_extended_batch(
                                        ForwardCtx {
                                            client: stream,
                                            conns,
                                            current_node: route_from,
                                            reads: &ReadRouting {
                                                offload_on,
                                                may_offload: offload_on
                                                    && !tr.guc_cap_hit
                                                    && !tr.serializable_default
                                                    && !session
                                                        .reads_pinned
                                                        .load(std::sync::atomic::Ordering::Relaxed),
                                                batch_reads,
                                                batch_whole: wait_ready && !batch_flushed,
                                                gucs: &tr.gucs,
                                                guc_epoch: tr.guc_epoch,
                                                read_your_writes: ryw,
                                            },
                                            session,
                                            state,
                                            config,
                                            fault: &mut fault,
                                            continues_cycle: cycle_node
                                                .as_deref()
                                                .filter(|_| continues_cycle)
                                                .or_else(|| {
                                                    current_node.as_deref().filter(|_| copy_in)
                                                })
                                                .map(|node| OpenCycle { node, runs }),
                                        },
                                        ExtBatch {
                                            batch: &batch,
                                            route_sql,
                                            wait_ready,
                                            defines: &batch_defines,
                                            unnamed: held_unnamed.as_ref(),
                                        },
                                        &reprepare,
                                        &stmt_registry,
                                        hold,
                                    )
                                    .await
                                }
                            };
                            match fr {
                                Ok(v) => v,
                                Err(e) => match fault.take() {
                                    Some(f) => {
                                        faulted = true;
                                        match Self::tr_handle_fault(
                                            TrRecovery {
                                                client: stream,
                                                conns,
                                                current_node: &mut current_node,
                                                tr,
                                                cycle_node: cycle_node.as_deref(),
                                                registry: &stmt_registry,
                                                session,
                                                state,
                                                config,
                                            },
                                            f,
                                            InFlight::Extended {
                                                ext: ExtBatch {
                                                    batch: &batch,
                                                    route_sql: pending_route_sql.as_deref(),
                                                    wait_ready,
                                                    defines: &batch_defines,
                                                    unnamed: held_unnamed.as_ref(),
                                                },
                                                reprepare: &reprepare,
                                            },
                                        )
                                        .await?
                                        {
                                            Some(v) => v,
                                            None => return Ok(()),
                                        }
                                    }
                                    None => {
                                        if matches!(e, ProxyError::NoHealthyNodes) {
                                            if tr.ext_dispatched {
                                                return Ok(());
                                            }
                                            Self::send_no_healthy_nodes(
                                                stream, session, wait_ready,
                                            )
                                            .await;
                                            return Ok(());
                                        }
                                        return Err(e);
                                    }
                                },
                            }
                        };
                        if used_node.is_none() {
                            // Refused before it reached a backend (rate limit,
                            // open circuit, failed replacement): as on the
                            // 25P02 path above, nothing was closed (F2).
                            batch_closes.clear();
                            batch_left_closed.clear();
                            // ...but PostgreSQL destroys the unnamed statement
                            // before it parses an unnamed Parse, so a batch
                            // refused at one (its first message) leaves none.
                            if leads_unnamed_parse {
                                Self::orphan_unnamed(conns, UnnamedStale::STATEMENT);
                                unnamed_bound = Bound::Other;
                                unnamed_node = None;
                            }
                            // A transaction end the emulation let through was
                            // refused with the failed-transaction status: the
                            // client is still in its failed transaction, so
                            // the emulation goes on (later statements get
                            // 25P02, never autocommit on the new backend).
                            if aborted_end
                                && session
                                    .last_rfq_status
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    == b'E'
                            {
                                tr.tx_aborted = true;
                            }
                        }
                        if offload_on {
                            Self::note_home(
                                &mut home_node,
                                used_node.as_deref(),
                                session,
                                state,
                                config,
                            );
                            if batch_parsed_unnamed {
                                if let Some(n) = used_node.as_ref() {
                                    unnamed_node = Some(n.clone());
                                }
                            }
                        }
                        let forwarded = used_node.is_some() && !skip_forward;
                        // The batch reached a backend that answers it: after a
                        // fault, only when the recovery re-sent it. One the
                        // recovery answered with an error (the session moved
                        // to the replacement all the same) reached none.
                        let delivered = forwarded && (!faulted || tr.fault_resent);
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        // A session-level advisory lock the batch takes is
                        // held by the connection it reached (one refused or
                        // answered before any backend took none, and one cut
                        // short by a fault died with that connection).
                        if takes_locks && delivered {
                            if let Some(bc) = current_node.as_ref().and_then(|n| conns.get_mut(n)) {
                                bc.session_locks = true;
                            }
                        }
                        // Extended-protocol SETs (JDBC, Npgsql and tokio-postgres
                        // send `SET` this way) and transaction ends, tracked
                        // like the simple protocol's where the cycle can be
                        // read statement by statement (see `ext_set`).
                        let status_after = session
                            .last_rfq_status
                            .load(std::sync::atomic::Ordering::Relaxed);
                        let copying = session
                            .copy_in_progress
                            .load(std::sync::atomic::Ordering::Relaxed);
                        if copying {
                            // This cycle's COPY drain is not a simple query's.
                            tr.copy_query = None;
                        }
                        // A COPY that follows is the extended protocol's.
                        ext_copy = true;
                        if tr.track_gucs {
                            let (completed, errored) = Self::tally(session);
                            let outcome = ext_set.at_boundary(ExtSetBoundary {
                                sync: wait_ready,
                                forwarded,
                                before: status_before,
                                after: status_after,
                                failed: session
                                    .last_response_error
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    || errored,
                                copying,
                                completed,
                                simple: false,
                            });
                            let idle_after_sync =
                                wait_ready && forwarded && !copying && status_after == b'I';
                            if Self::tr_ext_set_outcome(
                                tr,
                                outcome,
                                idle_after_sync,
                                session,
                                state,
                            ) && offload_on
                            {
                                session
                                    .reads_pinned
                                    .store(true, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        // The batch ended the COPY it reached in copy-in (its
                        // ReadyForQuery is in): the COPY failed and loaded
                        // nothing, as at a CopyFail.
                        if copy_in && delivered && !copying {
                            Self::copy_drained(tr, false, session, state, config).await;
                        }
                        if !skip_forward {
                            Self::tr_after_extended(
                                tr,
                                ExtBatch {
                                    batch: &batch,
                                    route_sql: pending_route_sql.as_deref(),
                                    wait_ready,
                                    defines: &batch_defines,
                                    unnamed: held_unnamed.as_ref(),
                                },
                                &batch_refs,
                                &stmt_registry,
                                session,
                                state,
                            )
                            .await;
                            // Only a batch that reached the backend moves its
                            // cycle on there: a Flush leaves it open (on the
                            // node recorded in `cycle_node`), a Sync ends it. A
                            // batch continuing an open cycle is never refused
                            // before it nor sent elsewhere (`continues_cycle`);
                            // a fault already cleared it (that backend is gone),
                            // and a batch the recovery answered without
                            // re-sending it opens none (`delivered`).
                            if delivered {
                                tr.ext_dispatched = !wait_ready;
                                if tr.ext_dispatched {
                                    cycle_node.clone_from(&current_node);
                                }
                            }
                            if wait_ready && forwarded && !copying {
                                // The status baseline the simple protocol's
                                // tracking compares against, in every TR mode.
                                tr.prev_status = status_after;
                            }
                        }
                        held_unnamed = None;
                        if wait_ready {
                            // This Sync's ReadyForQuery ends an extended COPY
                            // when it reached the backend (it continues the
                            // COPY's cycle, so no refusal stops it). One that
                            // failed (or the 25P02 emulation's lost one)
                            // awaits none.
                            if forwarded || faulted || skip_forward {
                                if let Some(done) = tr.ext_copy_end.take() {
                                    if forwarded && !copying {
                                        Self::copy_drained(tr, done, session, state, config).await;
                                    }
                                }
                            }
                            // The cycle ended: a fresh tally for the next one.
                            Self::tally_reset(session);
                        } else if faulted {
                            // A fault on a Flush batch: the connection holding
                            // the session's cycle is gone. When that cycle is
                            // an extended COPY whose end awaits its Sync, the
                            // COPY's uncommitted rows died with it, so nothing
                            // of the cycle is open on any backend: the Sync the
                            // client still owes is answered without one
                            // (`refused_flush`) and leaves the session idle,
                            // its permit returns and, under pooling, the
                            // replacement connection is parked.
                            tr.ext_copy_end = None;
                        }
                        if forwarded || faulted {
                            // The client is in a cycle the backend answers
                            // again, not idle behind a refusal.
                            refused_open = false;
                        }
                        // A Flush batch answered without reaching a backend
                        // (a fault's recovery error included) leaves its
                        // client owing the cycle's Sync; a Sync, or a batch a
                        // backend took (`tr.ext_dispatched` then), settles
                        // that.
                        refused_flush = !wait_ready && !delivered;
                        // The client was told its cycle failed there: the
                        // rest of it, up to that Sync, is discarded, as
                        // PostgreSQL skips to the Sync after an error. A later
                        // chunk never runs (nor commits at the Sync) behind
                        // the error, and the cycle gets that one error. The
                        // aborted-transaction emulation answers each batch
                        // itself.
                        if refused_flush && !skip_forward {
                            cycle_refused = true;
                        }
                        // A `Sync` is the extended-protocol transaction/statement
                        // boundary (it yields ReadyForQuery); a `Flush` is not, so
                        // only a Sync triggers a pool release, and never while
                        // the backend still holds a cycle open.
                        #[cfg(feature = "pool-modes")]
                        if wait_ready && !tr.ext_dispatched && tr.ext_copy_end.is_none() {
                            Self::release_to_pool_if_idle(
                                conns,
                                current_node.as_deref(),
                                session,
                                state,
                                config,
                            )
                            .await;
                        }
                        // ...then the tenant permit (park first, then release).
                        if wait_ready {
                            Self::tenant_query_release_if_idle(
                                hold,
                                session,
                                Self::cycle_keeps_permit(
                                    !pending.is_empty() || held_unnamed.is_some(),
                                    tr,
                                    refused_open,
                                    cycle_node.as_deref(),
                                    conns,
                                ),
                                Self::session_locks_held(track_session_locks, conns),
                            );
                        }
                        state.metrics.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                        // Extended-protocol writes drove zero edge invalidation
                        // before this hook — prepared-statement drivers
                        // (JDBC/Npgsql/asyncpg) would leave every edge stale
                        // until TTL. Union the tables of the batch's referenced
                        // DML statements (memoized at Parse) and invalidate/
                        // broadcast exactly like the simple path. Errors inside
                        // the batch only over-invalidate — safe. A COPY ... FROM
                        // batch is deferred to its CopyDone drain (rows visible
                        // then). Runs BEFORE the batch_closes drain below, so an
                        // execute-then-Close batch still sees its statement's
                        // metadata; only Sync-terminated batches fire (a pure
                        // Flush pipeline accumulates refs until its Sync).
                        #[cfg(feature = "edge-proxy")]
                        if wait_ready && config.edge.enabled {
                            if let Some(tables) = Self::edge_extended_batch_tables(
                                &batch_refs,
                                edge_batch_bound_unnamed,
                                &edge_stmt_meta,
                                &edge_unnamed_meta,
                            ) {
                                if session
                                    .copy_in_progress
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                {
                                    *session
                                        .pending_edge_copy_tables
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner()) = Some(tables);
                                } else {
                                    Self::edge_invalidate_write(state, config, tables).await;
                                }
                            }
                            edge_batch_bound_unnamed = false;
                        }
                        // Statements the batch left closed are deallocated —
                        // forget their canonical Parse so they are never
                        // re-prepared. A name Closed and then Parsed again in
                        // the batch keeps its new definition (F2).
                        for name in &batch_left_closed {
                            if let Some(removed) = stmt_registry.remove(name) {
                                stmt_registry_bytes =
                                    stmt_registry_bytes.saturating_sub(removed.len());
                            }
                            stmt_reads.remove(name);
                            if track_session_locks {
                                stmt_locks.remove(name);
                            }
                        }
                        // ...and the session's other backend connections may
                        // still hold their own copies (F2).
                        Self::note_statement_closes(
                            conns,
                            current_node.as_deref(),
                            &batch_closes,
                            &batch_left_closed,
                        );
                        #[cfg(not(feature = "edge-proxy"))]
                        batch_closes.clear();
                        // Edge builds also prune per-statement invalidation metadata
                        // — but that metadata is consumed by the Sync-time hook ABOVE
                        // (which runs first) and must survive a Close seen at an
                        // earlier Flush of the same batch, and a name Closed then
                        // re-Parsed in this batch must keep its FRESH meta. So the
                        // close queue is drained only at the Sync (re-noting its
                        // names at each boundary is idempotent), and edge meta is
                        // removed there only for names not redefined this batch.
                        // Retaining meta for a genuinely-closed name is safe: a later
                        // re-Parse overwrites it, and a stale entry only ever
                        // over-invalidates.
                        #[cfg(feature = "edge-proxy")]
                        {
                            if wait_ready {
                                for name in Self::edge_meta_prunable(&batch_closes, &batch_defines)
                                {
                                    edge_stmt_meta.remove(name);
                                }
                                batch_closes.clear();
                            }
                        }
                        if wait_ready {
                            // Sync ends the extended cycle; reset routing so the
                            // next Parse can re-route. Flush leaves it intact so
                            // the rest of the in-flight sequence stays put.
                            pending_route_sql = None;
                            batch_defines.clear();
                            batch_refs.clear();
                            batch_left_closed.clear();
                            batch_reads = true;
                            // A Sync continuing an open cycle always reaches
                            // the backend (`continues_cycle`): none is left open.
                            batch_flushed = false;
                            batch_parsed_unnamed = false;
                            batch_binds_unnamed = false;
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                        } else if !skip_forward {
                            batch_flushed = true;
                        }
                    }

                    // ---- COPY sub-protocol (client -> backend) ----
                    MessageType::CopyData | MessageType::CopyDone | MessageType::CopyFail => {
                        let is_copy_end =
                            matches!(msg.msg_type, MessageType::CopyDone | MessageType::CopyFail);
                        // Only a COPY in progress ends: PostgreSQL ignores a
                        // stray CopyDone/CopyFail outside copy-in, so it ends
                        // no extended COPY (no `tr.ext_copy_end`, which would let
                        // a bare Flush through and dispatch a cycle without its
                        // tenant permit) and drains no response.
                        let ends_copy = is_copy_end
                            && session
                                .copy_in_progress
                                .load(std::sync::atomic::Ordering::Relaxed);
                        let conn = current_node.as_ref().and_then(|n| conns.get_mut(n));
                        match conn {
                            Some(b) => {
                                if let Err(e) = b.stream.write_all(&msg.encode()).await {
                                    // The backend died mid-COPY: the data stream
                                    // cannot be recovered in any tr_mode — one
                                    // error, then close (never a bare drop).
                                    let node = current_node.clone().unwrap_or_default();
                                    let err = format!("Backend copy write error: {}", e);
                                    Self::record_backend_failure(state, &node, &err);
                                    Self::abandon_conn(conns, &node, session, state).await;
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    // The query that started the COPY never drains.
                                    tr.copy_query = None;
                                    let in_tx = session
                                        .in_transaction
                                        .load(std::sync::atomic::Ordering::Relaxed);
                                    let _ = Self::tr_send_error(
                                        stream,
                                        "08006",
                                        &format!(
                                            "backend {} failed during COPY ({}); connection closed",
                                            node, err
                                        ),
                                        in_tx,
                                        true,
                                    )
                                    .await;
                                    return Ok(());
                                }
                                if ends_copy && ext_copy {
                                    // An extended-protocol COPY: the backend
                                    // ignored the cycle's Sync during copy-in, so
                                    // the ReadyForQuery answers the client's next
                                    // Sync (libpq and tokio-postgres send one
                                    // after CopyDone). Waiting for it here would
                                    // stall until that Sync, which is not read.
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    ext_copy = false;
                                    tr.ext_copy_end =
                                        Some(matches!(msg.msg_type, MessageType::CopyDone));
                                    cycle_node.clone_from(&current_node);
                                } else if ends_copy {
                                    let node = current_node.clone().unwrap();
                                    // Cleared before the drain: a query with a
                                    // second COPY makes the relay set it again
                                    // when it yields for that one.
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    let r = Self::stream_until_ready_buffered(
                                        stream,
                                        &mut b.stream,
                                        &mut b.response_buf,
                                        session,
                                        state,
                                        0,
                                    )
                                    .await;
                                    // Drained back to ReadyForQuery, unless the
                                    // relay yielded for the next COPY: then the
                                    // session is still mid-COPY.
                                    let copying_on = r.is_ok()
                                        && session
                                            .copy_in_progress
                                            .load(std::sync::atomic::Ordering::Relaxed);
                                    if !copying_on {
                                        session
                                            .copy_in_progress
                                            .store(false, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    match r {
                                        Ok(sent) if copying_on => {
                                            state
                                                .metrics
                                                .bytes_sent
                                                .fetch_add(sent, Ordering::Relaxed);
                                        }
                                        Ok(sent) => {
                                            Self::copy_drained(
                                                tr,
                                                matches!(msg.msg_type, MessageType::CopyDone),
                                                session,
                                                state,
                                                config,
                                            )
                                            .await;
                                            state
                                                .metrics
                                                .bytes_sent
                                                .fetch_add(sent, Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            let err = e.to_string();
                                            let client_gone = err.contains("Client");
                                            let ended = e.progress.terminal || e.progress.raw;
                                            if !client_gone && !ended {
                                                Self::record_backend_failure(state, &node, &err);
                                            }
                                            Self::abandon_conn(conns, &node, session, state).await;
                                            if client_gone {
                                                return Err(e.error);
                                            }
                                            if ended {
                                                return Ok(());
                                            }
                                            // Backend fault mid-COPY drain: tell the
                                            // client (08006) and close — every tr_mode.
                                            let in_tx = session
                                                .in_transaction
                                                .load(std::sync::atomic::Ordering::Relaxed);
                                            let _ = Self::tr_send_error(
                                                stream,
                                                "08006",
                                                &format!(
                                                    "backend {} failed during COPY ({}); connection closed",
                                                    node, err
                                                ),
                                                in_tx,
                                                true,
                                            )
                                            .await;
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            None => {
                                // The client is streaming COPY frames but the
                                // backend connection is gone (dropped/redialed).
                                // Silently discarding them hangs the client
                                // forever; instead tell it the copy failed and
                                // return it to a clean idle state. Only a COPY
                                // in progress, though: a session idle under
                                // transaction/statement pooling has no
                                // connection, and a stray CopyData/CopyDone/
                                // CopyFail there (pgx sends CopyDone after a
                                // COPY that failed or was refused at once) is
                                // ignored, as PostgreSQL ignores it; an answer
                                // would put every later reply one behind.
                                if session
                                    .copy_in_progress
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                {
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    // Its CopyDone/CopyFail, now or after more
                                    // CopyData, is told the COPY failed.
                                    copy_lost = true;
                                    // The query that started the COPY never drains.
                                    tr.copy_query = None;
                                    // The COPY aborted — nothing loaded — so discard any
                                    // deferred invalidation stash, else it would later
                                    // fire against an unrelated COPY's drain.
                                    #[cfg(feature = "edge-proxy")]
                                    {
                                        *session
                                            .pending_edge_copy_tables
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner()) = None;
                                    }
                                }
                                if is_copy_end && std::mem::take(&mut copy_lost) {
                                    let emsg = Self::create_error_response(
                                        "57000",
                                        "COPY aborted: backend connection lost",
                                    );
                                    let _ = stream.write_all(&emsg).await;
                                    // An extended COPY's ReadyForQuery answers
                                    // the client's next Sync.
                                    if !std::mem::take(&mut ext_copy) {
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                    }
                                }
                            }
                        }
                        // A COPY that drained to ReadyForQuery may leave the
                        // session idle; an extended COPY keeps the permit
                        // until the Sync that settles it (`tr.ext_copy_end`).
                        if is_copy_end {
                            // Transaction/Statement pooling: park the
                            // connection once the session is idle (never
                            // mid-COPY, mid-transaction or with a cycle still
                            // open on it)...
                            #[cfg(feature = "pool-modes")]
                            if !tr.ext_dispatched && tr.ext_copy_end.is_none() {
                                Self::release_to_pool_if_idle(
                                    conns,
                                    current_node.as_deref(),
                                    session,
                                    state,
                                    config,
                                )
                                .await;
                            }
                            // ...then return the tenant permit, so the waiter
                            // it wakes finds the connection parked.
                            Self::tenant_query_release_if_idle(
                                hold,
                                session,
                                Self::cycle_keeps_permit(
                                    !pending.is_empty() || held_unnamed.is_some(),
                                    tr,
                                    refused_open,
                                    cycle_node.as_deref(),
                                    conns,
                                ),
                                Self::session_locks_held(track_session_locks, conns),
                            );
                        }
                    }

                    // ---- Anything else: forward to current backend best-effort ----
                    _ => {
                        if let Some(ref node) = current_node {
                            if let Some(b) = conns.get_mut(node) {
                                let _ = b.stream.write_all(&msg.encode()).await;
                            }
                        }
                    }
                }
            }

            // Bound un-flushed extended-protocol accumulation: a client must
            // reach a Sync/Flush boundary before this many bytes pile up in
            // `pending` (otherwise a never-syncing client grows it unbounded).
            if pending.len() > state.limits.max_pending_bytes {
                let emsg = Self::create_error_response(
                    "53400",
                    "un-flushed extended-protocol buffer exceeds per-session limit",
                );
                let _ = stream.write_all(&emsg).await;
                let _ = stream.write_all(&Self::create_ready_for_query(b'I')).await;
                tracing::warn!(
                    client = %session.client_addr,
                    pending = pending.len(),
                    "pending extended-protocol buffer cap exceeded; closing connection"
                );
                return Ok(());
            }
        }

        // On disconnect, park this session's still-idle connections so a later
        // same-identity client can reuse them (cross-client pooling). Anything
        // mid-transaction, or holding an extended cycle still open on the
        // backend (a Flush dispatched part of it, or an extended COPY awaits
        // its Sync), is left to drop (closed → backend rolls back). The
        // tenant permit (`hold`) returns after this, when the loop ends.
        #[cfg(feature = "pool-modes")]
        Self::park_idle_conns(conns, tr, cycle_node.as_deref(), session, state, config).await;

        Ok(())
    }
}
