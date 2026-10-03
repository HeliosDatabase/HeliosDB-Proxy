use super::*;

impl ProxyServer {
    /// Decide which node a request should be routed to, without doing any
    /// I/O. Reuses `current_node` when it is healthy and role-compatible
    /// (sticky session), otherwise selects a fresh primary/read node. The
    /// returned address is the key into the per-session connection cache.
    pub(super) async fn choose_target_node(
        is_write: bool,
        forced_target: Option<String>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // After a migration cutover, every request stays on the promoted
        // target — never route back to the former primary.
        if let Some(t) = state.cutover.load_full().as_ref() {
            return Ok(t.addr.clone());
        }

        // Read-your-writes: within the window after a write, a read is pinned to
        // the primary (overriding the reuse-of-a-standby path) so the client
        // observes its own writes despite replica lag.
        #[cfg(feature = "lag-routing")]
        if !is_write && forced_target.is_none() && config.lag_routing.enabled {
            let last_write = *session.last_write_at.read().await;
            if Self::ryw_pins_primary(last_write, config.lag_routing.ryw_window_ms) {
                tracing::debug!(target: "helios::routing", "read-your-writes: pinning read to primary");
                return Self::select_primary_with_timeout(session, state, config).await;
            }
        }

        let need_switch = if let Some(ref forced) = forced_target {
            let health = state.health.load_full();
            let reuse = current_node
                .map(|c| c == forced && health.get(c).map(|h| h.healthy).unwrap_or(false))
                .unwrap_or(false);
            !reuse
        } else if let Some(current) = current_node {
            let health = state.health.load_full();
            let current_healthy = health.get(current).map(|h| h.healthy).unwrap_or(false);
            if !current_healthy {
                true
            } else if is_write {
                let is_primary = config
                    .nodes
                    .iter()
                    .find(|n| n.address() == current)
                    .map(|n| n.role == NodeRole::Primary)
                    .unwrap_or(false);
                !is_primary
            } else {
                false
            }
        } else {
            true
        };

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
            Ok(resolved)
        } else if need_switch {
            if is_write {
                Self::select_primary_with_timeout(session, state, config).await
            } else {
                Self::select_read_node(session, state, config).await
            }
        } else {
            Ok(current_node.unwrap().to_string())
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
            if let Some(stream) = pool.checkout(&key) {
                tracing::info!(
                    target: "helios::pool",
                    node = %target,
                    "reused pooled backend connection"
                );
                conns.insert(target.to_string(), BackendConn::new(stream));
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
        conns.insert(target.to_string(), BackendConn::new(backend));
        Ok(())
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

        // Conditional reset: a connection that provably touched no session
        // state — no dirtying simple statement, no named prepared statement, no
        // unnamed prepared statement — can be parked WITHOUT the `DISCARD ALL`
        // round-trip, removing a backend RTT from the critical path for clean
        // autocommit workloads. Any of these three signals forces the full
        // reset. Extended-protocol traffic always has `prepared`/`unnamed_sig`
        // set, so it is never clean-skipped (conservative by construction).
        let clean = !bc.dirty && bc.prepared.is_empty() && bc.unnamed_sig.is_none();
        if config.pool_mode.skip_clean_reset && clean {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin(&key, bc.stream) {
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
            if pool.checkin(&key, bc.stream) {
                tracing::debug!(target: "helios::pool", node = %node, "parked backend connection for reuse");
            }
        }
        // On reset failure the connection is dropped here (closed).
    }
}
