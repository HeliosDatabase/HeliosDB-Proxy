use super::*;

/// How a request that is neither a write nor hint-forced to a node is routed
/// (see [`ProxyServer::choose_target_node`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadRoute {
    /// Reads are not offloaded, or the session is inside a transaction: stay
    /// on the session's node (pick a read node only when it has none or it
    /// is unhealthy).
    Stay,
    /// The primary must serve it: go to the session's home node.
    Home,
    /// A standby may serve it.
    Offload,
}

/// Read-routing inputs the client loop hands to the forward paths.
pub(super) struct ReadRouting<'a> {
    /// `[load_balancer]` offloads reads and a standby can serve them.
    pub(super) offload_on: bool,
    /// ...and this session may still read from a standby (not pinned).
    pub(super) may_offload: bool,
    /// Extended protocol: every statement of the batch is a standby-safe
    /// read. The simple path classifies its final SQL itself.
    pub(super) batch_reads: bool,
    /// Extended protocol: the batch is a whole `Sync`-terminated cycle with
    /// no `Flush` part forwarded before it; a split cycle stays home.
    pub(super) batch_whole: bool,
    /// The session's tracked `SET`s, mirrored onto a standby connection
    /// before its next read, and the epoch that names this set.
    pub(super) gucs: &'a [String],
    pub(super) guc_epoch: u64,
    /// `[load_balancer] read_your_writes_ms`.
    pub(super) read_your_writes: Duration,
}

impl ReadRouting<'_> {
    /// No read offload (internal re-execution paths).
    pub(super) const OFF: ReadRouting<'static> = ReadRouting {
        offload_on: false,
        may_offload: false,
        batch_reads: false,
        batch_whole: false,
        gucs: &[],
        guc_epoch: 0,
        read_your_writes: Duration::ZERO,
    };

    /// The route of a request that is not classified as a write and is not
    /// forced to a node. `standby_hint`: a `route=standby` hint, which also
    /// overrides read-your-writes; `standby_safe`: whether the request is a
    /// read a standby may serve. A request the proxy cannot vouch for
    /// (`CALL`, `DO`, `EXECUTE`, a function that may write, …) counts as a
    /// write for read-your-writes.
    pub(super) fn route(
        &self,
        session: &ClientSession,
        standby_hint: bool,
        standby_safe: impl FnOnce() -> bool,
    ) -> ReadRoute {
        if !self.offload_on
            || session
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return ReadRoute::Stay;
        }
        if !self.may_offload {
            return ReadRoute::Home;
        }
        let safe = standby_safe();
        if !safe {
            self.note_write(session);
        }
        let wrote_recently = !self.read_your_writes.is_zero()
            && session
                .last_write
                .lock()
                .is_some_and(|t| t.elapsed() < self.read_your_writes);
        if standby_hint || (safe && !wrote_recently) {
            ReadRoute::Offload
        } else {
            ReadRoute::Home
        }
    }

    /// Note a write (or `SET`, or transaction control) the session sent, for
    /// read-your-writes. Only while reads are offloaded.
    pub(super) fn note_write(&self, session: &ClientSession) {
        if self.offload_on && !self.read_your_writes.is_zero() {
            *session.last_write.lock() = Some(std::time::Instant::now());
        }
    }
}

impl ProxyServer {
    /// Decide which node a request should be routed to, without doing any
    /// I/O. A write goes to the write target (reusing `current_node` when it
    /// is one); a hint-forced request to its node. Any other request follows
    /// `read` (see [`ReadRoute`]); with [`ReadRoute::Stay`] it reuses a
    /// healthy `current_node` (sticky session) as before read offload
    /// existed. Returns the address — the key into the per-session
    /// connection cache — and whether it is an offloaded read on a standby.
    pub(super) async fn choose_target_node(
        is_write: bool,
        forced_target: Option<String>,
        current_node: Option<&str>,
        read: ReadRoute,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(String, bool)> {
        // After a migration cutover, every request stays on the promoted
        // target — never route back to the former primary.
        if let Some(t) = state.cutover.load_full().as_ref() {
            return Ok((t.addr.clone(), false));
        }

        // Read-your-writes: within the window after a write, a read is pinned to
        // the primary (overriding the reuse-of-a-standby path) so the client
        // observes its own writes despite replica lag.
        #[cfg(feature = "lag-routing")]
        if !is_write && forced_target.is_none() && config.lag_routing.enabled {
            let last_write = *session.last_write_at.read().await;
            if Self::ryw_pins_primary(last_write, config.lag_routing.ryw_window_ms) {
                tracing::debug!(target: "helios::routing", "read-your-writes: pinning read to primary");
                return Ok((
                    Self::select_primary_with_timeout(session, state, config).await?,
                    false,
                ));
            }
        }

        if let Some(forced) = forced_target {
            // Resolve a node *name* to its address; an address is passed
            // through unchanged. This lets `/*helios:node=pg-standby*/` (and a
            // plugin `Node("name")`) target a node by its configured name
            // rather than requiring the raw host:port.
            let resolved = config
                .nodes
                .iter()
                .find(|n| n.name.as_deref() == Some(forced.as_str()) || n.address() == forced)
                .map(|n| n.address().to_string())
                .unwrap_or(forced);
            return Ok((resolved, false));
        }

        let healthy = |addr: &str| {
            state
                .health
                .load_full()
                .get(addr)
                .map(|h| h.healthy)
                .unwrap_or(false)
        };
        if !is_write {
            match read {
                ReadRoute::Stay => {}
                ReadRoute::Offload => {
                    let sync_only =
                        config.load_balancer.standby_reads == crate::config::StandbyReads::Sync;
                    let sticky = session.read_node.lock().clone();
                    if let Some(prev) = sticky {
                        if Self::still_read_target(state, config, &prev, sync_only) {
                            *session.current_node.write().await = Some(prev.clone());
                            return Ok((prev, true));
                        }
                    }
                    if let Some(addr) = Self::pick_standby(session, state, config, sync_only).await
                    {
                        if !Self::is_write_target(state, config, &addr) {
                            *session.read_node.lock() = Some(addr.clone());
                            return Ok((addr, true));
                        }
                    }
                    *session.read_node.lock() = None;
                    // No standby qualifies right now: the primary serves it.
                    if let Some(c) = current_node {
                        if healthy(c) && Self::is_write_target(state, config, c) {
                            return Ok((c.to_string(), false));
                        }
                    }
                    if let Some(home) = Self::home_node(state, config) {
                        return Ok((home, false));
                    }
                }
                ReadRoute::Home => {
                    if let Some(c) = current_node {
                        if healthy(c) && Self::is_write_target(state, config, c) {
                            return Ok((c.to_string(), false));
                        }
                    }
                    if let Some(home) = Self::home_node(state, config) {
                        return Ok((home, false));
                    }
                    // No primary right now: behave as without offload, so a
                    // read can still be served by a standby during an outage.
                }
            }
        }

        let need_switch = if let Some(current) = current_node {
            if !healthy(current) {
                true
            } else if is_write {
                !Self::is_write_target(state, config, current)
            } else {
                false
            }
        } else {
            true
        };
        if need_switch {
            if is_write {
                Ok((
                    Self::select_primary_with_timeout(session, state, config).await?,
                    false,
                ))
            } else {
                Ok((Self::select_read_node(session, state, config).await?, false))
            }
        } else {
            Ok((current_node.unwrap().to_string(), false))
        }
    }

    /// Read offload: remember where the session's last request that was not
    /// an offloaded read ran — its home, the socket the idle watch covers.
    pub(super) fn note_home(
        home: &mut Option<String>,
        used: Option<&str>,
        session: &ClientSession,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        let Some(node) = used else {
            return;
        };
        let offloaded = session.read_node.lock().as_deref() == Some(node)
            && !Self::is_write_target(state, config, node);
        if !offloaded && home.as_deref() != Some(node) {
            *home = Some(node.to_string());
        }
    }

    /// A standby socket kept for offloaded reads is not watched while the
    /// session idles on its home. Before reusing it, drop it if the backend
    /// closed it or sent anything unsolicited (a FATAL before closing): it
    /// holds nothing the session cannot rebuild, and `ensure_conn` dials anew.
    pub(super) fn drop_if_unusable(conns: &mut HashMap<String, BackendConn>, target: &str) {
        let Some(bc) = conns.get_mut(target) else {
            return;
        };
        let mut probe = [0u8; 1];
        let idle = bc.response_buf.is_empty()
            && matches!(
                bc.stream.try_read(&mut probe),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
            );
        if !idle {
            conns.remove(target);
        }
    }

    /// `target` once took the session's settings by mirroring (a standby the
    /// session read from) and is behind them, while reads are offloaded.
    pub(super) fn mirror_stale(
        conns: &HashMap<String, BackendConn>,
        target: &str,
        reads: &ReadRouting<'_>,
    ) -> bool {
        reads.offload_on
            && conns
                .get(target)
                .is_some_and(|bc| bc.gucs_synced.is_some_and(|e| e != reads.guc_epoch))
    }

    /// A once-mirrored connection now serves as the session's home: from here
    /// on it runs the session's `SET`s itself.
    pub(super) fn mirror_now_home(conns: &mut HashMap<String, BackendConn>, target: &str) {
        if let Some(bc) = conns.get_mut(target) {
            bc.gucs_synced = None;
        }
    }

    /// An offloaded read is about to run on the standby connection `target`:
    /// bring that socket's session settings up to the session's tracked
    /// `SET`s (the primary connection ran them itself) unless it already
    /// matches `reads.guc_epoch` — `RESET ALL` first if it carries an older
    /// set, then each tracked `SET`. Only in session pooling: with
    /// transaction or statement pooling every connection is reset between
    /// transactions anyway. `false` when the socket could not be brought up
    /// to date: it is dropped, a backend that refused a `SET` pins the
    /// session's reads to its home, and the caller routes the read home.
    pub(super) async fn mirror_read_gucs(
        client: &mut ClientStream,
        conns: &mut HashMap<String, BackendConn>,
        target: &str,
        reads: &ReadRouting<'_>,
        session: &ClientSession,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> bool {
        let Some(bc) = conns.get_mut(target) else {
            return false;
        };
        if bc.gucs_synced == Some(reads.guc_epoch)
            || config.pool_mode.mode != crate::config::PoolingMode::Session
        {
            return true;
        }
        if bc.gucs_synced.is_none() && reads.gucs.is_empty() {
            // A fresh connection already has the session's startup settings.
            bc.gucs_synced = Some(reads.guc_epoch);
            return true;
        }
        let wt = state.limits.backend_write_timeout;
        let rt = state.limits.backend_read_timeout;
        let mf = state.limits.max_backend_frame_bytes;
        let reset = bc.gucs_synced.is_some();
        let synced = async {
            Self::finish_pending_backend_frames(client, bc, state).await?;
            if reset {
                Self::tr_run_discard(&mut bc.stream, "RESET ALL", wt, rt, mf, None).await?;
            }
            Self::tr_restore_session_state(&mut bc.stream, reads.gucs, wt, rt, mf).await
        }
        .await;
        match synced {
            Ok(_) => {
                bc.gucs_synced = Some(reads.guc_epoch);
                #[cfg(feature = "pool-modes")]
                if !reads.gucs.is_empty() {
                    bc.dirty = true;
                }
                true
            }
            Err(e) => {
                tracing::warn!(
                    target: "helios::routing",
                    node = %target,
                    error = %e,
                    "could not mirror the session's settings onto a standby; reading from the primary"
                );
                conns.remove(target);
                state
                    .metrics
                    .read_mirror_failures
                    .fetch_add(1, Ordering::Relaxed);
                if matches!(e, ProxyError::Network(_)) {
                    Self::record_backend_failure(state, target, &e.to_string());
                } else {
                    session
                        .reads_pinned
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                false
            }
        }
    }

    /// Ensure the per-session cache holds an authenticated backend connection
    /// to `target`, dialing + silently re-authenticating one (with the
    /// client's pass-through credentials) only if absent. The cached
    /// connection is then reused across read/write route switches.
    pub(super) async fn ensure_conn(
        conns: &mut HashMap<String, BackendConn>,
        target: &str,
        session: &Arc<ClientSession>,
        config: &ProxyConfig,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        if conns.contains_key(target) {
            return Ok(());
        }

        // Transaction/Statement pooling: lease a parked, identity-matched
        // connection before paying for a fresh TCP connect + startup + auth.
        // The parked connection was `DISCARD ALL`-reset on release, so it is
        // clean for this (same-identity) client.
        #[cfg(feature = "pool-modes")]
        if let Some(pool) = state.backend_pool.as_ref() {
            let key = Self::pool_key_for(target, session).await;
            if let Some((stream, slot)) = pool.checkout_with_slot(&key) {
                tracing::info!(
                    target: "helios::pool",
                    node = %target,
                    "reused pooled backend connection"
                );
                conns.insert(target.to_string(), BackendConn::with_slot(stream, slot));
                return Ok(());
            }
        }

        let params = session.variables.read().await.clone();
        let startup = Self::build_startup_message(&params);
        let user = params.get("user").map(String::as_str).unwrap_or("");
        let credential = session.backend_credential.read().await.clone();
        // A 53300 refusal (backend at max_connections) is retried after
        // freeing one idle pooled connection to this node, bounded by the
        // pool acquire timeout — see `connect_and_authenticate`.
        // H-05: hold a slot on the node's cap for the connection's life.
        let slot = Self::acquire_node_slot(state, target).await?;
        let capacity_deadline = tokio::time::Instant::now() + config.pool.acquire_timeout();
        let seed = session.id.as_u128() as u64;
        let mut attempt: u32 = 0;
        let backend = loop {
            let mut backend =
                tokio::time::timeout(config.pool.acquire_timeout(), TcpStream::connect(target))
                    .await
                    .map_err(|_| {
                        ProxyError::Connection(format!("Connection timeout to {}", target))
                    })?
                    .map_err(|e| {
                        ProxyError::Connection(format!("Failed to connect to {}: {}", target, e))
                    })?;
            let _ = backend.set_nodelay(true);
            backend
                .write_all(&startup)
                .await
                .map_err(|e| ProxyError::Network(format!("Backend startup error: {}", e)))?;
            match Self::complete_backend_auth(
                &mut backend,
                state.limits.max_pending_bytes,
                user,
                credential.as_deref(),
            )
            .await
            {
                Ok(_) => break backend,
                Err(ProxyError::PoolExhausted(msg)) => {
                    if !Self::backend_capacity_backoff(
                        state,
                        target,
                        attempt,
                        seed,
                        capacity_deadline,
                    )
                    .await
                    {
                        state
                            .metrics
                            .backend_capacity_refusals
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(ProxyError::PoolExhausted(msg));
                    }
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        };
        #[cfg(feature = "pool-modes")]
        if state.backend_pool.is_some() {
            tracing::debug!(target: "helios::pool", node = %target, "dialed fresh backend connection (pool miss)");
        }
        tracing::debug!(node = %target, "opened backend connection");
        conns.insert(target.to_string(), BackendConn::with_slot(backend, slot));
        Ok(())
    }

    /// Take a slot on `node`'s `max_connections` cap (H-05) before opening a
    /// connection to it: a full node first gives up one idle pooled
    /// connection, then the request waits up to `[limits]
    /// backend_slot_wait_ms`, then fails with `PoolExhausted` (53300).
    pub(super) async fn acquire_node_slot(
        state: &ServerState,
        node: &str,
    ) -> Result<crate::node_slots::Slot> {
        let (slot, waited) = state
            .node_slots
            .acquire(node, state.limits.backend_slot_wait, || {
                Self::free_idle_backend(state, node)
            })
            .await
            .map_err(|e| ProxyError::PoolExhausted(e.to_string()))?;
        if let Some(waited) = waited {
            state.latency.backend_slot_wait.record(waited);
        }
        Ok(slot)
    }

    fn free_idle_backend(state: &ServerState, node: &str) -> bool {
        #[cfg(feature = "pool-modes")]
        if let Some(pool) = state.backend_pool.as_ref() {
            return pool.evict_one_idle_for_node(node);
        }
        let _ = (state, node);
        false
    }

    /// Startup parameters that change how the backend interprets or renders
    /// values and are reset by `DISCARD ALL`/`RESET ALL` to the connection's
    /// *startup* values. Two clients of the same `(node,user,db)` but different
    /// values for any of these must NOT share a pooled connection, or the
    /// borrower silently inherits the lender's encoding/date/number formatting.
    /// This mirrors PgBouncer's `track_extra_parameters` intent.
    #[cfg(feature = "pool-modes")]
    const POOL_IDENTITY_PARAMS: &'static [&'static str] = &[
        "client_encoding",
        "DateStyle",
        "TimeZone",
        "IntervalStyle",
        "standard_conforming_strings",
        "options",
    ];

    /// Build the pool key for the current session's connection identity.
    /// Base identity is `(node, user, database)` — connections are reused only
    /// within an identity, so a borrower always matches the principal the parked
    /// connection was authenticated as. When any routing-relevant startup GUC
    /// (see `POOL_IDENTITY_PARAMS`) is set, a hash of those is folded in so the
    /// borrower also matches the lender's value-formatting settings. The common
    /// case (no custom GUCs) keeps the bare `(node,user,db)` key unchanged.
    #[cfg(feature = "pool-modes")]
    pub(super) async fn pool_key_for(target: &str, session: &Arc<ClientSession>) -> String {
        let vars = session.variables.read().await;
        let user = vars.get("user").map(|s| s.as_str()).unwrap_or("");
        // PostgreSQL defaults the database to the role name when unset.
        let database = vars.get("database").map(|s| s.as_str()).unwrap_or(user);
        let base = crate::pool::pool_key(target, user, database);

        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let mut any = false;
        for k in Self::POOL_IDENTITY_PARAMS {
            if let Some(v) = vars.get(*k) {
                any = true;
                k.hash(&mut h);
                v.hash(&mut h);
            }
        }
        if any {
            format!("{}\0{:016x}", base, h.finish())
        } else {
            base
        }
    }

    /// Reset a backend connection to a clean session state before parking it
    /// for reuse: runs the configured reset query (default `DISCARD ALL`,
    /// which deallocates prepared statements, drops temp tables, resets GUCs
    /// and advisory locks) and drains its response to `ReadyForQuery`. Returns
    /// `Err` if the connection is unhealthy OR the reset itself did not cleanly
    /// succeed — the caller then drops (closes) it instead of parking a
    /// poisoned connection.
    ///
    /// "Cleanly succeeded" means: no `ErrorResponse` frame in the reply AND the
    /// terminating `ReadyForQuery` reported idle (`'I'`, not in/failed
    /// transaction). The previous version returned `Ok` on the first
    /// `ReadyForQuery` regardless, so a failed `DISCARD ALL` (e.g. a copy-abort,
    /// or a custom `reset_query` that errors) would park a connection with its
    /// GUCs / temp tables / prepared statements intact — exactly what pooling
    /// must never do. Frames are walked by raw tag because the wire decoder is
    /// direction-agnostic (`'E'` decodes to the client-side `Execute`), so a
    /// backend `ErrorResponse` cannot be recognised via `msg_type`.
    #[cfg(feature = "pool-modes")]
    pub(super) async fn reset_backend<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        stream: &mut S,
        reset_sql: &str,
        backend_write_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<()> {
        let msg = crate::protocol::QueryMessage {
            query: reset_sql.to_string(),
        }
        .encode();
        tokio::time::timeout(backend_write_timeout, stream.write_all(&msg.encode()))
            .await
            .map_err(|_| ProxyError::Network("reset write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("reset write error: {}", e)))?;

        let mut buf = BytesMut::with_capacity(1024);
        let mut had_error = false;
        loop {
            // Walk complete frames by raw tag, tracking any ErrorResponse and
            // stopping at ReadyForQuery.
            let mut consumed = 0usize;
            let mut ready_status: Option<u8> = None;
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
                if mtype == b'E' {
                    had_error = true;
                }
                consumed += frame_total;
                if mtype == b'Z' {
                    ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                    break;
                }
            }
            let _ = buf.split_to(consumed);

            if let Some(status) = ready_status {
                if had_error || status != b'I' {
                    return Err(ProxyError::Connection(format!(
                        "reset query did not cleanly succeed (error={}, status={})",
                        had_error, status as char
                    )));
                }
                return Ok(());
            }

            buf.reserve(1024);
            let n = tokio::time::timeout(Duration::from_secs(5), stream.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("reset drain timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("reset drain read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "backend closed during reset".to_string(),
                ));
            }
        }
    }

    /// Transaction/Statement pooling release point: when the session is at an
    /// idle boundary (`ReadyForQuery` reported not-in-transaction), reset the
    /// just-used connection and park it for reuse by the next same-identity
    /// client. A no-op in Session mode or when the feature is off. Never
    /// releases mid-transaction.
    #[cfg(feature = "pool-modes")]
    pub(super) async fn release_to_pool_if_idle(
        conns: &mut HashMap<String, BackendConn>,
        node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        let Some(pool) = state.backend_pool.as_ref() else {
            return;
        };
        let Some(node) = node else {
            return;
        };
        // Only release at a clean transaction boundary — never mid-transaction
        // and never mid-COPY (the backend is awaiting CopyData; resetting +
        // parking the socket now aborts the copy and hangs the client).
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
            || session
                .copy_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let Some(mut bc) = conns.remove(node) else {
            return;
        };

        // A buffered response belongs to this socket's previous user. Never
        // park it after discarding a partial frame and lose wire alignment.
        if !bc.response_buf.is_empty() {
            return;
        }

        // Conditional reset: a connection that provably touched no session
        // state — no dirtying simple statement, no named prepared statement, no
        // unnamed prepared statement — can be parked WITHOUT the `DISCARD ALL`
        // round-trip, removing a backend RTT from the critical path for clean
        // autocommit workloads. Any of these three signals forces the full
        // reset. Extended-protocol traffic always has `prepared`/`unnamed_sig`
        // set (or `dirty`, after a Close), so it is never clean-skipped
        // (conservative by construction).
        let clean =
            !bc.dirty && bc.prepared.is_empty() && bc.stale.is_empty() && bc.unnamed_sig.is_none();
        if config.pool_mode.skip_clean_reset && clean {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin_with_slot(&key, bc.stream, bc._slot) {
                pool.note_reset_skipped();
                tracing::debug!(target: "helios::pool", node = %node, "parked clean backend connection (reset skipped)");
            }
            return;
        }

        if Self::reset_backend(
            &mut bc.stream,
            &config.pool_mode.reset_query,
            state.limits.backend_write_timeout,
            state.limits.max_backend_frame_bytes,
        )
        .await
        .is_ok()
        {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin_with_slot(&key, bc.stream, bc._slot) {
                tracing::debug!(target: "helios::pool", node = %node, "parked backend connection for reuse");
            }
        }
        // On reset failure the connection is dropped here (closed).
    }
}
