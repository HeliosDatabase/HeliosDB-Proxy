use super::*;

impl ProxyServer {
    /// Register a simple-query string for capture; arms the relay when the
    /// statement's outcome matters (write, COPY, EXECUTE, transaction control).
    pub(super) fn journal_register_simple(session: &ClientSession, sql: &str) {
        let armed = session
            .journal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .register_simple(sql);
        if armed {
            session.journal_armed.store(true, Ordering::Relaxed);
        }
    }

    /// Register every `Parse` / `Bind` / `Execute` / `Close` of an extended
    /// batch (raw wire bytes), preceded by the held unnamed `Parse` message
    /// when promotion kept it off the wire but the backend still holds it.
    pub(super) fn journal_register_batch(
        session: &ClientSession,
        batch: &[u8],
        held_unnamed: Option<&[u8]>,
    ) {
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(msg) = held_unnamed {
            if msg.len() >= 5 && msg[0] == b'P' {
                Self::journal_note_parse(&mut cap, &msg[5..]);
            }
        }
        let mut i = 0usize;
        while i + 5 <= batch.len() {
            let mtype = batch[i];
            let len = u32::from_be_bytes([batch[i + 1], batch[i + 2], batch[i + 3], batch[i + 4]])
                as usize;
            if len < 4 || i + 1 + len > batch.len() {
                break;
            }
            let payload = &batch[i + 5..i + 1 + len];
            match mtype {
                b'P' => Self::journal_note_parse(&mut cap, payload),
                b'B' => cap.note_bind(payload),
                b'E' => {
                    let end = payload
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(payload.len());
                    let portal = std::str::from_utf8(&payload[..end]).unwrap_or("");
                    cap.note_execute(portal);
                }
                b'C' => cap.note_close(payload),
                _ => {}
            }
            i += 1 + len;
        }
        if cap.armed() {
            session.journal_armed.store(true, Ordering::Relaxed);
        }
    }

    /// Decode a `Parse` payload (name, query, param type OIDs) into the capture.
    pub(super) fn journal_note_parse(
        cap: &mut crate::journal_capture::SessionCapture,
        payload: &[u8],
    ) {
        let Some(n_end) = payload.iter().position(|&b| b == 0) else {
            return;
        };
        let name = std::str::from_utf8(&payload[..n_end]).unwrap_or("");
        let rest = &payload[n_end + 1..];
        let Some(q_end) = rest.iter().position(|&b| b == 0) else {
            return;
        };
        let Ok(query) = std::str::from_utf8(&rest[..q_end]) else {
            return;
        };
        let mut types = Vec::new();
        let t = &rest[q_end + 1..];
        if t.len() >= 2 {
            let n = u16::from_be_bytes([t[0], t[1]]) as usize;
            let mut off = 2;
            for _ in 0..n {
                if off + 4 > t.len() {
                    break;
                }
                types.push(u32::from_be_bytes([
                    t[off],
                    t[off + 1],
                    t[off + 2],
                    t[off + 3],
                ]));
                off += 4;
            }
        }
        cap.note_parse(name, query, types);
    }

    /// Note one backend frame for the capture: `CommandComplete` tags,
    /// `PortalSuspended`, `EmptyQueryResponse` and the first `ErrorResponse`.
    pub(super) fn journal_note_frame(
        frame: &[u8],
        completions: &mut Vec<crate::journal_capture::Completion>,
        first_error: &mut Option<(String, String)>,
    ) {
        use crate::journal_capture::Completion;
        match frame.first() {
            Some(b'C') => {
                let body = frame.get(5..).unwrap_or(&[]);
                let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
                completions.push(Completion::Tag(
                    String::from_utf8_lossy(&body[..end]).into_owned(),
                ));
            }
            Some(b's') => completions.push(Completion::Suspended),
            Some(b'I') => completions.push(Completion::Empty),
            Some(b'E') if first_error.is_none() => {
                let mut code = String::new();
                let mut message = String::new();
                let mut body = frame.get(5..).unwrap_or(&[]);
                while let Some((&field, rest)) = body.split_first() {
                    if field == 0 {
                        break;
                    }
                    let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
                    let value = String::from_utf8_lossy(&rest[..end]);
                    match field {
                        b'C' => code = value.into_owned(),
                        b'M' => message = value.into_owned(),
                        _ => {}
                    }
                    body = rest.get(end + 1..).unwrap_or(&[]);
                }
                *first_error = Some((code, message));
            }
            _ => {}
        }
    }

    /// Source identity of the session's current transaction for the journal.
    pub(super) async fn journal_source(
        session: &ClientSession,
    ) -> crate::transaction_journal::SourceIdentity {
        let (user, database, tenant) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned().unwrap_or_default(),
                vars.get("database").cloned().unwrap_or_default(),
                vars.get("tenant_id").cloned(),
            )
        };
        let backend = session
            .current_node
            .read()
            .await
            .clone()
            .unwrap_or_default();
        crate::transaction_journal::SourceIdentity {
            client_addr: session.client_addr.to_string(),
            user,
            database,
            backend,
            tenant,
        }
    }

    /// Apply capture operations and count them. Runs under the session's
    /// capture lock and never awaits: an open explicit transaction lives in
    /// the session (`SessionCapture::active_mut`) and only a commit touches
    /// shared state (`journal_capture::apply_ops`).
    pub(super) fn journal_apply(
        state: &ServerState,
        ops: Vec<crate::journal_capture::JournalOp>,
        source: &crate::transaction_journal::SourceIdentity,
        session_id: Uuid,
        local: &mut Option<crate::transaction_journal::TransactionJournalEntry>,
    ) {
        if ops.is_empty() {
            return;
        }
        let node_id = crate::journal_capture::node_id_for_backend(&source.backend);
        let applied = crate::journal_capture::apply_ops(
            &state.transaction_journal,
            ops,
            session_id,
            node_id,
            source,
            local,
        );
        if applied.committed > 0 {
            state
                .metrics
                .journal_committed
                .fetch_add(applied.committed, Ordering::Relaxed);
        }
        if applied.rolled_back > 0 {
            state
                .metrics
                .journal_rolled_back
                .fetch_add(applied.rolled_back, Ordering::Relaxed);
        }
        if applied.statements > 0 {
            state
                .metrics
                .journal_statements
                .fetch_add(applied.statements, Ordering::Relaxed);
        }
    }

    /// A backend response completed (`ReadyForQuery` relayed): reconcile the
    /// capture with what the backend answered and apply the journal ops.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy", test))]
    pub(super) async fn journal_observe(
        session: &ClientSession,
        state: &ServerState,
        outcome: crate::journal_capture::ResponseOutcome,
    ) {
        if let Some(cycle) = Self::journal_observe_sync(session, state, outcome) {
            Self::journal_observe_finish(session, state, cycle).await;
        }
    }

    /// Phase 1 of observing a completed cycle, synchronous and run before
    /// the `ReadyForQuery` is forwarded: reconcile the capture with what the
    /// backend answered and move the query-cache generations of the tables
    /// the cycle wrote (C-02). `None` on the idle fast path.
    pub(super) fn journal_observe_sync(
        session: &ClientSession,
        state: &ServerState,
        outcome: crate::journal_capture::ResponseOutcome,
    ) -> Option<ObservedCycle> {
        let armed = session.journal_armed.swap(false, Ordering::Relaxed);
        // Fast path: an idle autocommit response with nothing registered and
        // no transaction being captured changes nothing.
        if !armed && outcome.status == b'I' && !session.journal_open.load(Ordering::Relaxed) {
            return None;
        }
        let ops = {
            let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
            let ops = cap.observe(&outcome);
            session
                .journal_open
                .store(cap.in_transaction(), Ordering::Relaxed);
            ops
        };
        #[cfg(feature = "query-cache")]
        let purge = match state.query_cache.as_ref() {
            Some(qc) if !ops.is_empty() => {
                let work = {
                    let mut stage = session
                        .tx_cache_stage
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    Self::cache_work_from_ops(qc, &ops, &mut stage)
                };
                if work.all {
                    qc.invalidate_all();
                }
                qc.mark_written(&work.tables);
                work.tables
            }
            _ => Vec::new(),
        };
        #[cfg(not(feature = "query-cache"))]
        let _ = state;
        Some(ObservedCycle {
            ops,
            #[cfg(feature = "query-cache")]
            purge,
        })
    }

    /// Phase 2, after the client has its answer: purge the L2 entries of the
    /// written tables and apply the journal ops (skipped when TR is off and
    /// the capture ran only for the cache).
    pub(super) async fn journal_observe_finish(
        session: &ClientSession,
        state: &ServerState,
        cycle: ObservedCycle,
    ) {
        #[cfg(feature = "query-cache")]
        if !cycle.purge.is_empty() {
            if let Some(qc) = state.query_cache.as_ref() {
                qc.purge_tables(&cycle.purge).await;
            }
        }
        if cycle.ops.is_empty() || !state.live_config.load().tr_enabled {
            return;
        }
        // The source identity is the only thing that awaits; it is read
        // before the capture lock so nothing is held across an await.
        let source = Self::journal_source(session).await;
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        Self::journal_apply(state, cycle.ops, &source, session.id, cap.active_mut());
    }

    /// Whether the journal capture must run for the query cache even when TR
    /// is off: commit-aware invalidation (C-02) is driven by its ops.
    pub(super) fn cache_needs_capture(state: &ServerState) -> bool {
        #[cfg(feature = "query-cache")]
        {
            state.query_cache.is_some()
        }
        #[cfg(not(feature = "query-cache"))]
        {
            let _ = state;
            false
        }
    }

    /// Translate one cycle's capture ops into cache invalidation (C-02).
    /// Statement-time: every successful write invalidates its tables at once
    /// (a later read must not hit a pre-write entry). Commit-time: the tables
    /// an explicit transaction staged are invalidated again when the backend
    /// reports the commit, closing the window in which another session
    /// refilled an entry from pre-commit data. A rollback drops the stage.
    /// A write whose tables cannot be read from its text invalidates
    /// everything.
    #[cfg(feature = "query-cache")]
    pub(super) fn cache_work_from_ops(
        qc: &crate::cache::QueryCache,
        ops: &[crate::journal_capture::JournalOp],
        stage: &mut TxCacheStage,
    ) -> CacheWork {
        use crate::journal_capture::JournalOp;
        let mut work = CacheWork::default();
        let add = |tables: &mut Vec<String>, t: Vec<String>| {
            for table in t {
                if !tables.contains(&table) {
                    tables.push(table);
                }
            }
        };
        for op in ops {
            match op {
                // Inside an explicit transaction a write is only staged: no
                // other session can see it before the commit, and the
                // session's own reads are never served from the cache while
                // it is in a transaction. Marking it at statement time as well
                // cost every in-transaction statement a generation bump and a
                // purge before its ReadyForQuery (C-02 user-path gate).
                JournalOp::Log { entry, .. } => match qc.write_tables(&entry.statement) {
                    Some(t) => add(&mut stage.tables, t),
                    None => stage.unknown = true,
                },
                JournalOp::Incomplete { .. } => stage.unknown = true,
                JournalOp::AutoCommit {
                    entries,
                    incomplete,
                    ..
                } => {
                    if incomplete.is_some() {
                        work.all = true;
                    }
                    for e in entries {
                        match qc.write_tables(&e.statement) {
                            Some(t) => add(&mut work.tables, t),
                            None => work.all = true,
                        }
                    }
                }
                JournalOp::Commit { .. } => {
                    let staged = std::mem::take(&mut stage.tables);
                    add(&mut work.tables, staged);
                    if std::mem::take(&mut stage.unknown) {
                        work.all = true;
                    }
                }
                JournalOp::Rollback { .. } => {
                    stage.tables.clear();
                    stage.unknown = false;
                }
                JournalOp::Begin { .. }
                | JournalOp::Savepoint { .. }
                | JournalOp::RollbackTo { .. } => {}
            }
        }
        work
    }

    /// `journal_observe` for a relay that recorded only the status byte
    /// (the cacheable-read relay never carries a registered statement).
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) async fn journal_observe_status(session: &ClientSession, state: &ServerState) {
        let status = session.last_rfq_status.load(Ordering::Relaxed);
        Self::journal_observe(
            session,
            state,
            crate::journal_capture::ResponseOutcome::status_only(status),
        )
        .await;
    }

    /// The proxy synthesized the `ReadyForQuery` the client just saw: nothing
    /// registered for the cycle reached the backend.
    pub(super) fn journal_discard(session: &ClientSession, status: u8) {
        session.journal_armed.store(false, Ordering::Relaxed);
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        cap.discard(status);
        // Keep the relay's fast path off until a deferred rollback has been
        // applied by the next `observe` (it only runs under the lock).
        session.journal_open.store(
            cap.in_transaction() || cap.has_deferred(),
            Ordering::Relaxed,
        );
    }

    /// The session ended.
    pub(super) async fn journal_close(session: &ClientSession, state: &ServerState) {
        #[cfg(feature = "query-cache")]
        {
            let mut stage = session
                .tx_cache_stage
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *stage = TxCacheStage::default();
        }
        let source = Self::journal_source(session).await;
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        let ops = cap.close();
        session.journal_open.store(false, Ordering::Relaxed);
        Self::journal_apply(state, ops, &source, session.id, cap.active_mut());
    }

    /// Hand a forwarded query to the analytics engine. This only builds the
    /// `QueryExecution` and pushes it onto the engine's bounded queue — the
    /// fingerprinting, metrics, slow-query log and pattern detection all run
    /// on the single background consumer task (see
    /// `QueryAnalytics::start_consumer`), so the connection task never pays
    /// for them. A full queue drops the sample rather than stalling the relay.
    /// No-op when analytics is disabled.
    #[cfg(feature = "query-analytics")]
    pub(super) async fn record_analytics(
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
        sql: &str,
        node: &str,
        duration: Duration,
        error: Option<String>,
    ) {
        let Some(analytics) = state.analytics.as_ref() else {
            return;
        };
        let (user, database) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned().unwrap_or_default(),
                vars.get("database").cloned().unwrap_or_default(),
            )
        };
        let mut exec = crate::analytics::QueryExecution::new(sql, duration);
        exec.user = user;
        exec.database = database;
        // Both pre-rendered at session creation — see `ClientSession`.
        exec.client_ip = session.client_ip_str.clone();
        exec.node = node.to_string();
        exec.session_id = Some(session.session_id_str.clone());
        exec.error = error;
        analytics.record(exec);
    }
}
