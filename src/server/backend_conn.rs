use super::*;

/// What a backend's `server_version` contains when it is HeliosDB-Nano or
/// HeliosDB-Lite (compared ASCII case-insensitively).
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
const HELIOSDB_VERSION_MARK: &[u8] = b"HeliosDB";

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

/// What a forward path (`forward_simple_query`, `forward_extended_batch`)
/// works with besides the request. Passed by value and destructured on entry,
/// so the bodies keep the locals separate parameters gave them.
pub(super) struct ForwardCtx<'a> {
    pub(super) client: &'a mut ClientStream,
    /// The session's per-node backend connections.
    pub(super) conns: &'a mut HashMap<String, BackendConn>,
    /// The node the request stays on unless routing moves it: the session's
    /// current node, the node holding the unnamed statement a Parse-less batch
    /// binds, or the replacement node on a TR re-execution.
    pub(super) current_node: Option<&'a str>,
    pub(super) reads: &'a ReadRouting<'a>,
    pub(super) session: &'a Arc<ClientSession>,
    pub(super) state: &'a Arc<ServerState>,
    pub(super) config: &'a ProxyConfig,
    /// Filled on a backend fault (failed node, delivery phase); a client-side
    /// error leaves it untouched.
    pub(super) fault: &'a mut Option<BackendFault>,
    /// The request continues a cycle a backend connection already holds open
    /// (a Flush dispatched part of it, or an extended COPY awaits its Sync):
    /// part of a request admitted already, it is neither refused (rate limit,
    /// open circuit) nor answered from a cache, nor routed anywhere but that
    /// connection, any of which would leave the cycle open behind a reply its
    /// backend never gave. `None`: a new request.
    pub(super) continues_cycle: Option<OpenCycle<'a>>,
}

/// The cycle a backend connection holds open, as the request continuing it
/// sees it (see [`ForwardCtx::continues_cycle`]).
#[derive(Debug, Clone, Copy)]
pub(super) struct OpenCycle<'a> {
    /// The node whose connection holds the cycle. The request goes there
    /// whatever routing would pick (read offload, a hint, an unhealthy or
    /// disabled node): that connection's reply, or its failure, settles the
    /// cycle. With that connection gone the request is a fault on it.
    pub(super) node: &'a str,
    /// The request runs statements of its own (an `Execute`, a `Query`): the
    /// rate limiter charges it like any request, but paces it instead of
    /// refusing it (`rate_limit_pace`). A batch that runs none (a bare
    /// `Sync` or `Flush`, a `Close`) is not charged.
    pub(super) runs: bool,
}

impl OpenCycle<'_> {
    /// The connection holding the cycle is gone (its backend died, or the
    /// session dropped it): the request continuing the cycle is a fault on
    /// that node, never sent to a fresh connection that knows nothing of the
    /// cycle. Part of the cycle may already have run there, so its outcome
    /// is unknown.
    pub(super) fn lost(
        &self,
        conns: &HashMap<String, BackendConn>,
        fault: &mut Option<BackendFault>,
    ) -> Option<ProxyError> {
        if conns.contains_key(self.node) {
            return None;
        }
        let e = ProxyError::Connection(format!(
            "the backend connection holding the open extended-protocol cycle on {} is gone",
            self.node
        ));
        BackendFault::set(fault, self.node, FaultPhase::OutcomeUnknown, &e);
        Some(e)
    }
}

/// Read offload under pass-through auth: the standbys that refused a login
/// the proxy opened without a password, per (user, database) — `pg_hba.conf`
/// decides per user and database, so a refusal says nothing about other
/// logins. Sessions without a backend credential skip a standby that refused
/// their login; a SIGHUP reload forgets every refusal.
#[derive(Default)]
pub(super) struct OffloadRefusals {
    /// Set by the first refusal, so routing touches the set (and builds a
    /// session's login) only once some standby has refused one.
    any: std::sync::atomic::AtomicBool,
    refused: dashmap::DashSet<(String, String, String)>,
}

impl OffloadRefusals {
    /// Whether any standby refused any login.
    pub(super) fn any(&self) -> bool {
        self.any.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Remember that `node` refused `login` (user, database); true the
    /// first time.
    pub(super) fn insert(&self, node: &str, login: &(String, String)) -> bool {
        let first = self
            .refused
            .insert((node.to_string(), login.0.clone(), login.1.clone()));
        self.any.store(true, std::sync::atomic::Ordering::Relaxed);
        first
    }

    /// Whether `node` refused `login`.
    pub(super) fn refused(&self, node: &str, login: &(String, String)) -> bool {
        self.any()
            && self
                .refused
                .contains(&(node.to_string(), login.0.clone(), login.1.clone()))
    }

    /// Forget every refusal (the standbys' auth may have changed).
    pub(super) fn clear(&self) {
        self.any.store(false, std::sync::atomic::Ordering::Relaxed);
        self.refused.clear();
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
                    let login = Self::refusable_login(state, session).await;
                    let sticky = session.read_node.lock().clone();
                    if let Some(prev) = sticky {
                        if Self::still_read_target(state, config, &prev, sync_only, login.as_ref())
                        {
                            *session.current_node.write().await = Some(prev.clone());
                            return Ok((prev, true));
                        }
                    }
                    if let Some(addr) =
                        Self::pick_standby(session, state, config, sync_only, login.as_ref()).await
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

    /// The (user, database) a session's backend connections log in as.
    async fn login(session: &ClientSession) -> (String, String) {
        let vars = session.variables.read().await;
        let user = vars.get("user").cloned().unwrap_or_default();
        let database = vars
            .get("database")
            .cloned()
            .unwrap_or_else(|| user.clone());
        (user, database)
    }

    /// The login a standby may have refused ([`OffloadRefusals`]): `None`
    /// while no standby has refused any, or when the session holds a backend
    /// credential (it can answer a password request).
    pub(super) async fn refusable_login(
        state: &ServerState,
        session: &ClientSession,
    ) -> Option<(String, String)> {
        if !state.offload_refusals.any() || session.backend_credential.read().await.is_some() {
            return None;
        }
        Some(Self::login(session).await)
    }

    /// An offloaded read could not open its standby connection; the caller
    /// serves it from the home instead. A standby that wants a password the
    /// proxy does not hold (pass-through auth) refuses this login for every
    /// session without a credential: remember the refusal so those sessions
    /// stop trying the node, keep this session's reads home, and do not count
    /// it as a backend fault (the node is healthy). Any other failure demotes
    /// the node as a failed connection always does.
    pub(super) async fn offload_refused(
        state: &Arc<ServerState>,
        session: &ClientSession,
        node: &str,
        err: &ProxyError,
    ) {
        state
            .metrics
            .read_mirror_failures
            .fetch_add(1, Ordering::Relaxed);
        if !matches!(err, ProxyError::Auth(_)) {
            Self::record_backend_failure(state, node, &err.to_string());
            return;
        }
        session
            .reads_pinned
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if session.backend_credential.read().await.is_some() {
            return;
        }
        let login = Self::login(session).await;
        if state.offload_refusals.insert(node, &login) {
            tracing::warn!(
                target: "helios::routing",
                node = %node,
                user = %login.0,
                database = %login.1,
                error = %err,
                "standby refused a login without a password: with pass-through auth, this user's reads of this database stay on the primary for sessions without a backend credential until a SIGHUP reload (use [auth] mode = \"scram\" with an auth_file, or let the standby trust the proxy)"
            );
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

    /// Under an authoritative topology, the session's home connection is
    /// still open but its node is no longer the write leader (a switchover
    /// left the old primary running). Carrying on would move the session to
    /// a fresh connection without its settings, or run the rest of an open
    /// transaction outside it. Returns that node: the caller treats it like a
    /// home lost while idle, so transaction replay re-homes the session
    /// (restoring its settings, or refusing when they are unknown).
    pub(super) fn home_moved(
        conns: &HashMap<String, BackendConn>,
        home: Option<&str>,
        state: &ServerState,
        config: &ProxyConfig,
    ) -> Option<String> {
        if !state.authoritative_topology {
            return None;
        }
        let home = home.filter(|h| conns.contains_key(*h))?;
        let leader = Self::home_node(state, config)?;
        (leader != home).then(|| home.to_string())
    }

    /// A once-mirrored connection now serves as the session's home: from here
    /// on it runs the session's `SET`s itself.
    pub(super) fn mirror_now_home(conns: &mut HashMap<String, BackendConn>, target: &str) {
        if let Some(bc) = conns.get_mut(target) {
            bc.gucs_synced = None;
            bc.gucs_role = false;
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
        let role = bc.gucs_role;
        let synced = async {
            Self::finish_pending_backend_frames(client, bc, session, state).await?;
            if reset {
                Self::tr_reset_mirrored(&mut bc.stream, role, wt, rt, mf).await?;
            }
            Self::tr_restore_session_state(&mut bc.stream, reads.gucs, wt, rt, mf).await
        }
        .await;
        match synced {
            Ok(_) => {
                // Sent as simple Queries (a `RESET ALL`, or settings on a
                // fresh connection: never nothing).
                bc.unnamed_destroyed();
                bc.gucs_synced = Some(reads.guc_epoch);
                bc.gucs_role = Self::tr_sets_role(reads.gucs);
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
                Self::abandon_conn(conns, target, session, state).await;
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
            if let Some((stream, slot, backend_key)) = pool.checkout_keyed(&key) {
                tracing::info!(
                    target: "helios::pool",
                    node = %target,
                    "reused pooled backend connection"
                );
                let mut bc = BackendConn::with_slot(stream, slot);
                bc.backend_key = backend_key;
                conns.insert(target.to_string(), bc);
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
        // H-05: at most `max_concurrent_reconnects_per_node` such dials to one
        // node at a time, from TCP connect through authentication.
        let dialing = Self::acquire_reconnect_slot(state, target).await?;
        let capacity_deadline = tokio::time::Instant::now() + config.pool.acquire_timeout();
        let seed = session.id.as_u128() as u64;
        let mut attempt: u32 = 0;
        let (backend, hot_standby, backend_key) = loop {
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
                Ok(frames) => {
                    Self::note_backend_startup(state, target, &frames);
                    break (
                        backend,
                        Self::reports_hot_standby(&frames),
                        Self::backend_key_data(&frames),
                    );
                }
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
        drop(dialing);
        #[cfg(feature = "pool-modes")]
        if state.backend_pool.is_some() {
            tracing::debug!(target: "helios::pool", node = %target, "dialed fresh backend connection (pool miss)");
        }
        tracing::debug!(node = %target, "opened backend connection");
        let mut bc = BackendConn::with_slot(backend, slot);
        bc.hot_standby = hot_standby;
        bc.backend_key = backend_key;
        conns.insert(target.to_string(), bc);
        Ok(())
    }

    /// The BackendKeyData (process id, secret key) in a backend's startup
    /// frames (as `complete_backend_auth` returns them), if it sent one.
    pub(super) fn backend_key_data(frames: &[u8]) -> Option<(u32, u32)> {
        let mut at = 0;
        while let Some(head) = frames.get(at..at + 5) {
            let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let body = frames.get(at + 5..at + 1 + len)?;
            if head[0] == b'K' && body.len() >= 8 {
                return Some((
                    u32::from_be_bytes([body[0], body[1], body[2], body[3]]),
                    u32::from_be_bytes([body[4], body[5], body[6], body[7]]),
                ));
            }
            at += 1 + len;
        }
        None
    }

    /// Whether the backend behind `bc` may still be running what the proxy
    /// sent it: its socket is not observed closed. A backend that ended its
    /// session (exited, or was killed) closed or reset the socket; one that
    /// is still executing, waiting on a lock, or unreachable without a reset
    /// looks open. Reads at most one byte, so only on a socket being dropped.
    pub(super) fn backend_may_be_running(bc: &BackendConn) -> bool {
        let mut probe = [0u8; 1];
        match bc.stream.try_read(&mut probe) {
            Ok(0) => false,
            Ok(_) => true,
            Err(e) => e.kind() == std::io::ErrorKind::WouldBlock,
        }
    }

    /// Drop the session's connection to `node` after a fault on it (or its
    /// client going away) while a request the proxy delivered on it may
    /// still be running there, cancelling that request first.
    ///
    /// Closing the socket alone does not stop it: PostgreSQL notices a
    /// closed client socket only when it next writes to it (or every
    /// `client_connection_check_interval`, off by default), so a statement
    /// silent past `[limits] backend_read_timeout_secs`, one waiting on a
    /// lock, or a `Flush`-dispatched `Execute` whose client vanished, runs on
    /// with its CPU, I/O and locks for its whole duration, outside the
    /// tenant active-query cap whose permit the session then returns
    /// (ac858d61400b). So the backend gets a CancelRequest with the
    /// connection's BackendKeyData ([`Self::cancel_abandoned`]); only a
    /// backend that implements CancelRequest stops the statement
    /// (HeliosDB-Nano ignores one: there it runs to completion). A backend
    /// that already closed or reset the socket, or a connection without a
    /// key, gets none. Callers record the fault first: a cancel this waits
    /// for never delays the node's demotion.
    pub(super) async fn abandon_conn(
        conns: &mut HashMap<String, BackendConn>,
        node: &str,
        session: &ClientSession,
        state: &ServerState,
    ) {
        if let Some(bc) = conns.remove(node) {
            Self::cancel_abandoned(bc, node, session, state).await;
        }
    }

    /// Drop `node`'s connection after a fault before anything of the
    /// request reached it (`FaultPhase::NotDelivered`): only a cycle the
    /// backend holds open, which the request continues (`open_cycle`: a
    /// `Flush`-dispatched `Execute`, a COPY), can be running there, and is
    /// cancelled ([`Self::abandon_conn`]); otherwise the backend is idle (or
    /// waits for the rest of a frame) and the socket is just closed.
    pub(super) async fn drop_undelivered(
        conns: &mut HashMap<String, BackendConn>,
        node: &str,
        open_cycle: bool,
        session: &ClientSession,
        state: &ServerState,
    ) {
        if open_cycle {
            Self::abandon_conn(conns, node, session, state).await;
        } else {
            conns.remove(node);
        }
    }

    /// [`Self::abandon_conn`] for a connection already taken out of the
    /// session's map: the CancelRequest, if its backend may still be running
    /// something, then the socket's close (it stays open until the request
    /// was sent, so its backend keeps the process id the key names).
    ///
    /// Awaited, bounded by `[limits] backend_cancel_timeout_ms`, only when
    /// it guards a tenant active-query permit (the session's tenant has an
    /// active-query cap): the permit then returns only after the cancel was
    /// sent. Every other session sends it from a task of its own, so its
    /// fault path (failover, re-execution) or its next message never waits
    /// for a backend host that does not answer, as before the cancel existed.
    pub(super) async fn cancel_abandoned(
        bc: BackendConn,
        node: &str,
        session: &ClientSession,
        state: &ServerState,
    ) {
        let Some((pid, key)) = bc.backend_key else {
            return;
        };
        if !Self::backend_may_be_running(&bc) {
            return;
        }
        let timeout = state.limits.backend_cancel_timeout;
        if session.tenant_query_capped() {
            Self::cancel_then_close(bc, node, (pid, key), timeout).await;
        } else {
            let node = node.to_string();
            tokio::spawn(async move {
                Self::cancel_then_close(bc, &node, (pid, key), timeout).await;
            });
        }
    }

    /// Send `bc`'s backend the CancelRequest for `(pid, key)`, then close
    /// `bc` (dropped here). The request goes to the address `bc` is open to:
    /// the key names a process of that server alone, and `node` is resolved
    /// afresh on every dial, so a name with several addresses, or one that
    /// changed since `bc` connected, can lead to another server, which
    /// ignores the key while the statement runs on. `node` is dialled only
    /// when the socket no longer reports its peer.
    async fn cancel_then_close(bc: BackendConn, node: &str, key: (u32, u32), timeout: Duration) {
        let peer = bc.stream.peer_addr().ok();
        let sent = match peer {
            Some(peer) => Self::send_cancel_request(peer, key.0, key.1, timeout).await,
            None => Self::send_cancel_request(node, key.0, key.1, timeout).await,
        };
        if sent {
            tracing::info!(
                node = %node,
                peer = ?peer,
                pid = key.0,
                "sent a CancelRequest for the request abandoned on a backend connection before closing it"
            );
        }
        drop(bc);
    }

    /// Whether a backend's startup frames (as `complete_backend_auth`
    /// returns them) carry `ParameterStatus in_hot_standby = on`.
    pub(super) fn reports_hot_standby(frames: &[u8]) -> bool {
        let mut at = 0;
        while let Some(head) = frames.get(at..at + 5) {
            let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let Some(body) = frames.get(at + 5..at + 1 + len) else {
                return false;
            };
            if head[0] == b'S' && body == b"in_hot_standby\0on\0" {
                return true;
            }
            at += 1 + len;
        }
        false
    }

    /// Note every `ParameterStatus` of a backend connection's startup frames
    /// (as `complete_backend_auth` returns them):
    /// [`Self::note_parameter_status`].
    pub(super) fn note_backend_startup(state: &ServerState, node: &str, frames: &[u8]) {
        let mut at = 0;
        while let Some(head) = frames.get(at..at + 5) {
            let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
            let Some(frame) = frames.get(at..at + 1 + len) else {
                return;
            };
            if head[0] == b'S' {
                Self::note_parameter_status(state, node, frame);
            }
            at += 1 + len;
        }
    }

    /// Note one backend `ParameterStatus` frame (tag and length included) a
    /// connection's startup sent. A `server_version` naming HeliosDB (Nano
    /// sends `16.0 (HeliosDB Nano x.y.z)`, Lite `17.0 (HeliosDB-Lite 2.0)`;
    /// matched ASCII case-insensitively) sets the process-wide sticky flag
    /// before the connection answers any read, so every cached result from
    /// such a backend is keyed accordingly: the result caches key reads by
    /// their text as written (whitespace and comments included) from then on,
    /// and a semantic tier with `[cache] semantic_match = "canonical"`
    /// (PostgreSQL semantics) neither stores nor serves, with one warning per
    /// process naming the key and the node. Entries stored before are
    /// retired.
    pub(super) fn note_parameter_status(state: &ServerState, node: &str, frame: &[u8]) {
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            let mut parts = frame.get(5..).unwrap_or_default().split(|&b| b == 0);
            let (Some(name), Some(value)) = (parts.next(), parts.next()) else {
                return;
            };
            if name != b"server_version"
                || !value
                    .windows(HELIOSDB_VERSION_MARK.len())
                    .any(|w| w.eq_ignore_ascii_case(HELIOSDB_VERSION_MARK))
            {
                return;
            }
            // The query cache's own flag first, for every such startup (an
            // idempotent swap): a second HeliosDB connection that finds the
            // server-wide flag already set, while its first setter has not
            // reached this point yet, must not answer reads with the cache
            // still keying the PostgreSQL way (collapsed L2 keys, a
            // canonical semantic tier).
            #[cfg(feature = "query-cache")]
            if let Some(qc) = state.query_cache.as_ref() {
                qc.note_heliosdb_backend();
            }
            if state
                .heliosdb_backend
                .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                return;
            }
            let version = String::from_utf8_lossy(value);
            #[cfg(feature = "query-cache")]
            if let Some(qc) = state.query_cache.as_ref() {
                let l3 = &qc.config().l3;
                if l3.enabled && l3.semantic_match == crate::config::SemanticMatch::Canonical {
                    tracing::warn!(
                        node = %node,
                        server_version = %version,
                        "cache.semantic_match = \"canonical\" assumes PostgreSQL semantics, but \
                         backend {node} reports a HeliosDB server_version ({version}), whose \
                         engines re-read the raw statement text: the semantic tier neither \
                         stores nor serves from now on (hinted reads count in \
                         semantic_ineligible); set cache.semantic_match = \"verbatim\""
                    );
                }
            }
            #[cfg(feature = "edge-proxy")]
            state.edge_cache.flush_all();
            tracing::info!(
                node = %node,
                server_version = %version,
                "backend reports a HeliosDB server_version: the result caches key reads by \
                 their text as written (whitespace and comments included) from now on"
            );
        }
        #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
        let _ = (state, node, frame);
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

    /// A permit to open a connection to `node` on a session's behalf under
    /// `[limits] max_concurrent_reconnects_per_node` (H-05): waits up to
    /// `backend_slot_wait_ms`, then fails with `PoolExhausted` (53300).
    pub(super) async fn acquire_reconnect_slot(
        state: &ServerState,
        node: &str,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>> {
        let (permit, waited) = state
            .node_slots
            .acquire_reconnect(node, state.limits.backend_slot_wait)
            .await
            .map_err(|e| ProxyError::PoolExhausted(e.to_string()))?;
        if let Some(waited) = waited {
            state.latency.backend_slot_wait.record(waited);
        }
        Ok(permit)
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
        // set (or `dirty`, after a Close or an unnamed Parse the promotion did
        // not record), so it is never clean-skipped (conservative by
        // construction). Nor is an unnamed statement or portal the session no
        // longer has (`unnamed_stale`).
        let clean = !bc.dirty
            && bc.prepared.is_empty()
            && bc.stale.is_empty()
            && bc.unnamed_sig.is_none()
            && !bc.unnamed_stale.any();
        if config.pool_mode.skip_clean_reset && clean {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin_keyed(&key, bc.stream, bc._slot, bc.backend_key) {
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
            if pool.checkin_keyed(&key, bc.stream, bc._slot, bc.backend_key) {
                tracing::debug!(target: "helios::pool", node = %node, "parked backend connection for reuse");
            }
            return;
        }
        // On reset failure the connection is dropped here (closed), the
        // reset cancelled first if it may still be running.
        Self::cancel_abandoned(bc, node, session, state).await;
    }
}
