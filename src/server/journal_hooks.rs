use super::*;

impl ProxyServer {
    /// Keep bounded response metadata across Flush and COPY boundaries.
    pub(super) fn journal_stash_response(
        session: &ClientSession,
        mut completions: Vec<crate::journal_capture::Completion>,
        error: Option<(String, String)>,
        limit: usize,
    ) -> Result<()> {
        use crate::journal_capture::{Completion, ResponseOutcome};
        if completions.is_empty() && error.is_none() {
            return Ok(());
        }
        let mut held = session
            .journal_response_prefix
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (prefix, strings) = held.get_or_insert_with(|| (ResponseOutcome::default(), 0));
        let added = Self::journal_response_strings(
            &completions,
            error.as_ref().filter(|_| prefix.error.is_none()),
        );
        let needed = prefix.completions.len().saturating_add(completions.len());
        let charge = prefix
            .completions
            .capacity()
            .max(needed)
            .saturating_mul(std::mem::size_of::<Completion>())
            .saturating_add(*strings)
            .saturating_add(added);
        if charge > limit {
            return Err(ProxyError::Protocol("journal response metadata exceeds limits.max_pending_bytes; session ended with an unverified outcome".into()));
        }
        prefix
            .completions
            .try_reserve_exact(needed.saturating_sub(prefix.completions.len()))
            .map_err(|e| {
                ProxyError::Internal(format!("journal response allocation failed: {e}"))
            })?;
        prefix.completions.append(&mut completions);
        if prefix.error.is_none() {
            prefix.error = error;
        }
        *strings = strings.saturating_add(added);
        session
            .journal_response_pending
            .store(true, Ordering::Release);
        Ok(())
    }

    /// String bytes a response's metadata retains: its completion tags and
    /// the error that would be kept.
    fn journal_response_strings(
        completions: &[crate::journal_capture::Completion],
        error: Option<&(String, String)>,
    ) -> usize {
        completions
            .iter()
            .map(|c| match c {
                crate::journal_capture::Completion::Tag(t) => t.capacity(),
                _ => 0,
            })
            .sum::<usize>()
            .saturating_add(error.map_or(0, |(c, m)| c.capacity().saturating_add(m.capacity())))
    }

    /// The `limits.max_pending_bytes` bound of [`Self::journal_stash_response`]
    /// for metadata the relay hands straight to its `ReadyForQuery`
    /// observation: a complete cycle with nothing held is charged exactly
    /// what stashing it would have charged, without the stash round trip.
    pub(super) fn journal_direct_response_fits(
        completions: &[crate::journal_capture::Completion],
        error: Option<&(String, String)>,
        limit: usize,
    ) -> Result<()> {
        let charge = completions
            .len()
            .saturating_mul(std::mem::size_of::<crate::journal_capture::Completion>())
            .saturating_add(Self::journal_response_strings(completions, error));
        if charge > limit {
            return Err(ProxyError::Protocol("journal response metadata exceeds limits.max_pending_bytes; session ended with an unverified outcome".into()));
        }
        Ok(())
    }

    fn journal_merge_response(
        session: &ClientSession,
        outcome: &mut crate::journal_capture::ResponseOutcome,
    ) {
        if !session
            .journal_response_pending
            .swap(false, Ordering::AcqRel)
        {
            return;
        }
        if let Some((mut prefix, _)) = session
            .journal_response_prefix
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            prefix.completions.append(&mut outcome.completions);
            outcome.completions = prefix.completions;
            if prefix.error.is_some() {
                outcome.error = prefix.error;
            }
        }
    }

    fn journal_clear_response(session: &ClientSession) {
        if session
            .journal_response_pending
            .swap(false, Ordering::AcqRel)
        {
            drop(
                session
                    .journal_response_prefix
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take(),
            );
        }
    }

    /// Register a simple-query string for capture; arms the relay when the
    /// statement's outcome matters (write, COPY, EXECUTE, transaction control).
    #[cfg(test)]
    pub(super) fn journal_register_simple(session: &ClientSession, sql: &str) {
        Self::journal_register_simple_cycle(session, sql, true);
    }

    pub(super) fn journal_register_simple_cycle(
        session: &ClientSession,
        sql: &str,
        starts_cycle: bool,
    ) {
        if starts_cycle {
            Self::journal_clear_response(session);
        }
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        if starts_cycle {
            cap.begin_cycle();
        }
        let armed = cap.register_simple(sql);
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
        starts_cycle: bool,
        flush: bool,
    ) {
        if starts_cycle {
            Self::journal_clear_response(session);
        }
        let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
        if starts_cycle {
            cap.begin_cycle();
        }
        let mut executed = false;
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
                    executed = true;
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
        if cap.armed() || (flush && executed) {
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
    ) -> Result<()> {
        use crate::journal_capture::Completion;
        match frame.first() {
            Some(b'C') => {
                let body = frame.get(5..).unwrap_or(&[]);
                let Some(tag) = body.strip_suffix(&[0]) else {
                    return Err(ProxyError::Protocol(
                        "invalid backend CommandComplete terminator".into(),
                    ));
                };
                if tag.contains(&0) {
                    return Err(ProxyError::Protocol(
                        "invalid backend CommandComplete trailing bytes".into(),
                    ));
                }
                let tag = std::str::from_utf8(tag).map_err(|_| {
                    ProxyError::Protocol("invalid backend CommandComplete UTF8".into())
                })?;
                completions.push(Completion::Tag(tag.to_owned()));
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
        Ok(())
    }

    /// Source identity of the session's current transaction for the journal.
    pub(super) async fn journal_source(
        session: &ClientSession,
    ) -> crate::transaction_journal::SourceIdentity {
        let (user, database, configured_tenant) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned().unwrap_or_default(),
                vars.get("database").cloned().unwrap_or_default(),
                vars.get("tenant_id").cloned(),
            )
        };
        // The startup-resolved tenant is attached once to the session's
        // shared budget. Prefer that canonical identity: database/user-based
        // tenant configurations do not necessarily populate a `tenant_id`
        // session variable, but journal accounting must still use the same
        // tenant key that admitted the session.
        let tenant = session
            .tenant_budget()
            .map(|budget| budget.tenant().to_owned())
            .or(configured_tenant);
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
        let tenant_journal_budget = source
            .tenant
            .as_deref()
            .and_then(|tenant| state.tenant_slots.budget(tenant))
            .and_then(|budget| budget.journal_budget())
            .cloned();
        let applying = std::time::Instant::now();
        let applied = crate::journal_capture::apply_ops_with_tenant_budget(
            &state.transaction_journal,
            ops,
            session_id,
            node_id,
            source,
            local,
            tenant_journal_budget.as_ref(),
        );
        state.latency.journal_apply.record(applying.elapsed());
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

    /// Retain bounded metadata and apply only an exact extended commit prefix
    /// proven before the first ErrorResponse. `proven` counts completions in
    /// this addition, not payload bytes or an advertised future offset.
    pub(super) async fn journal_stash_proven_response(
        session: &ClientSession,
        state: &ServerState,
        completions: Vec<crate::journal_capture::Completion>,
        error: Option<(String, String)>,
        proven: usize,
    ) -> Result<usize> {
        // The relay calls this once per backend read chunk while capture is
        // armed. RowDescription/DataRow-only chunks have nothing to retain;
        // avoid taking the response-prefix mutex on that common path.
        if completions.is_empty() && error.is_none() && proven == 0 {
            return Ok(0);
        }
        let (prior, prior_prepared, failed) = {
            let held = session
                .journal_response_prefix
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            held.as_ref().map_or((0, 0, false), |(prefix, _)| {
                (prefix.completions.len(), prefix.completions.iter().filter(|c| matches!(c, crate::journal_capture::Completion::Tag(t) if t.eq_ignore_ascii_case("COMMIT PREPARED"))).count(), prefix.error.is_some())
            })
        };
        let proven = proven.min(completions.len());
        Self::journal_stash_response(session, completions, error, state.limits.max_pending_bytes)?;
        if proven == 0 || failed {
            return Ok(0);
        }
        let (ops, newly_consumed) = {
            let mut held = session
                .journal_response_prefix
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let Some((outcome, strings)) = held.as_mut() else {
                return Ok(0);
            };
            let mut cap = session.journal.lock().unwrap_or_else(|e| e.into_inner());
            let prepared_before = outcome.completions.iter().filter(|c| matches!(c, crate::journal_capture::Completion::Tag(t) if t.eq_ignore_ascii_case("COMMIT PREPARED"))).count();
            let ops = cap.observe_committed_prefix(outcome, prior.saturating_add(proven));
            let prepared_after = outcome.completions.iter().filter(|c| matches!(c, crate::journal_capture::Completion::Tag(t) if t.eq_ignore_ascii_case("COMMIT PREPARED"))).count();
            let newly = prepared_before
                .saturating_sub(prepared_after)
                .saturating_sub(prior_prepared);
            session
                .journal_open
                .store(cap.in_transaction(), Ordering::Relaxed);
            // Retained Vec capacity remains within the existing R8 charge;
            // removing proven strings can only reduce the retained budget.
            *strings = outcome
                .completions
                .iter()
                .fold(0usize, |bytes, completion| {
                    bytes.saturating_add(match completion {
                        crate::journal_capture::Completion::Tag(tag) => tag.capacity(),
                        _ => 0,
                    })
                });
            if let Some((code, message)) = outcome.error.as_ref() {
                *strings = strings
                    .saturating_add(code.capacity())
                    .saturating_add(message.capacity());
            }
            if outcome.completions.is_empty() && outcome.error.is_none() {
                *held = None;
                session
                    .journal_response_pending
                    .store(false, Ordering::Release);
            }
            // The real RFQ still owns cycle admission and the armed flag.
            (ops, newly)
        };
        if let Some(cycle) = Self::journal_process_ops(session, state, ops) {
            // A failed client write cannot erase already committed history.
            Self::journal_observe_finish(session, state, cycle).await;
        }
        Ok(newly_consumed)
    }

    pub(super) fn journal_mark_raw_prepared(session: &ClientSession, applied: usize) {
        if applied == 0 {
            return;
        }
        let held = session
            .journal_response_prefix
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let retained = held.as_ref().map_or(0, |(outcome, _)| outcome.completions.iter().filter(|c| matches!(c, crate::journal_capture::Completion::Tag(t) if t.eq_ignore_ascii_case("COMMIT PREPARED"))).count());
        if retained > 0 {
            session
                .journal
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .note_raw_prepared_published(applied, retained);
        }
    }

    pub(super) fn journal_apply_raw_prepared(state: &ServerState) -> bool {
        #[cfg(feature = "query-cache")]
        let query = if let Some(g) = state.table_generations.as_ref() {
            match state.query_cache.as_ref() {
                Some(q) => q.invalidate_all(),
                None => g.invalidate_all(),
            };
            true
        } else {
            false
        };
        #[cfg(not(feature = "query-cache"))]
        let query = {
            let _ = state;
            false
        };
        query
    }

    /// Phase 1 of observing a completed cycle, synchronous and run before
    /// the `ReadyForQuery` is forwarded: reconcile the capture with what the
    /// backend answered and move the query-cache generations of the tables
    /// the cycle wrote (C-02). `None` on the idle fast path.
    pub(super) fn journal_observe_sync(
        session: &ClientSession,
        state: &ServerState,
        mut outcome: crate::journal_capture::ResponseOutcome,
    ) -> Option<ObservedCycle> {
        Self::journal_merge_response(session, &mut outcome);
        let armed = session.journal_armed.swap(false, Ordering::Relaxed);
        // A statement the capture cannot see may have written anything
        // (`cache_unseen_write`): every cached result is retired before the
        // client has the answer, and a transaction it leaves open retires
        // everything again at its commit.
        #[cfg(feature = "query-cache")]
        if session.cache_unseen_write.load(Ordering::Relaxed)
            && session.cache_unseen_write.swap(false, Ordering::Relaxed)
        {
            if let Some(generations) = state.table_generations.as_ref() {
                match state.query_cache.as_ref() {
                    Some(qc) => qc.invalidate_all_for_unseen_write(),
                    None => generations.invalidate_all(),
                }
                if outcome.status != b'I' {
                    session
                        .tx_cache_stage
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .unknown = true;
                }
            }
        }
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
        Self::journal_process_ops(session, state, ops)
    }

    /// Apply one proven observation's cache/edge effects. The exact accepted
    /// prerequisite's existing observer body is shared, not duplicated.
    fn journal_process_ops(
        session: &ClientSession,
        state: &ServerState,
        ops: Vec<crate::journal_capture::JournalOp>,
    ) -> Option<ObservedCycle> {
        #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
        let _ = session;
        // The generations are the query cache's when it runs, so its
        // heatmap and invalidation index follow the write; without it (the
        // MCP tool cache alone) only the generations move.
        #[cfg(feature = "query-cache")]
        let purge = match state.table_generations.as_ref() {
            Some(generations) if !ops.is_empty() => {
                let work = {
                    let mut stage = session
                        .tx_cache_stage
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    Self::cache_work_from_ops(generations, &ops, &mut stage)
                };
                match state.query_cache.as_ref() {
                    Some(qc) => {
                        if work.all {
                            qc.invalidate_all();
                        }
                        qc.mark_written(&work.tables);
                        work.tables
                    }
                    None => {
                        if work.all {
                            generations.invalidate_all();
                        }
                        generations.mark_written(&work.tables);
                        Vec::new()
                    }
                }
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

    /// Finish one observation: explicit committed prefixes finish before C
    /// is published; ordinary complete cycles retain their existing ordering.
    /// Purge the L2 entries of the
    /// written tables and apply the journal ops (skipped when TR is off and
    /// the capture ran only for the cache).
    pub(super) async fn journal_observe_finish(
        session: &ClientSession,
        state: &ServerState,
        cycle: ObservedCycle,
    ) {
        #[cfg(feature = "edge-proxy")]
        if cycle.ops.iter().any(|op| {
            matches!(
                op,
                crate::journal_capture::JournalOp::Commit { .. }
                    | crate::journal_capture::JournalOp::AutoCommit { .. }
                    | crate::journal_capture::JournalOp::PreparedCommitted
            )
        }) {
            let config = state.live_config.load();
            if config.edge.enabled {
                // The journal's commit boundary is the first point where a
                // transaction's writes are durable. Unknown scope flushes all.
                Self::edge_invalidate_write(state, &config, Vec::new()).await;
            }
        }
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

    /// Whether the journal capture must run for the caches even when TR is
    /// off: commit-aware invalidation (C-02) is driven by its ops. True
    /// whenever the write generations exist (a cache that reads them runs).
    pub(super) fn cache_needs_capture(state: &ServerState) -> bool {
        #[cfg(feature = "query-cache")]
        {
            state.table_generations.is_some()
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
        generations: &crate::cache::TableGenerations,
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
                JournalOp::Log { entry, .. } => match generations.write_tables(&entry.statement) {
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
                        match generations.write_tables(&e.statement) {
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
                // COMMIT PREPARED can finish a transaction prepared by a
                // different session or proxy. Its table set is unknown, so
                // invalidate every tier before forwarding ReadyForQuery.
                JournalOp::PreparedCommitted => work.all = true,
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
        Self::journal_clear_response(session);
        session.journal_armed.store(false, Ordering::Relaxed);
        #[cfg(feature = "query-cache")]
        session.cache_unseen_write.store(false, Ordering::Relaxed);
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
