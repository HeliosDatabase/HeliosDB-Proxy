use super::*;

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
                    match tokio::time::timeout(wait, Arc::clone(sem).acquire_owned()).await {
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
        let msg = Self::over_capacity_error_bytes();
        let _ = tokio::time::timeout(write_timeout, async {
            let _ = stream.write_all(&msg).await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        })
        .await;
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

    /// Handle a client connection
    pub(super) async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        state: Arc<ServerState>,
        config: Arc<ProxyConfig>,
        _shutdown_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        tracing::debug!("New client connection from {}", addr);

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
            last_response_digest: std::sync::atomic::AtomicU64::new(0),
            last_response_unverifiable: std::sync::atomic::AtomicBool::new(false),
            tr_replay_tainted: std::sync::atomic::AtomicBool::new(false),
            backend_credential: RwLock::new(None),
            #[cfg(feature = "query-cache")]
            tx_cache_stage: std::sync::Mutex::new(TxCacheStage::default()),
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
                    Ok(Some(msg)) => match Self::admit_client_slot(&state, &msg).await {
                        Ok(slot) => {
                            _session_guard._client_slot = slot;
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

    /// Wait for the client's next message.
    ///
    /// When `watch_node` names a cached backend connection, that socket is
    /// watched at the same time so unsolicited backend traffic (LISTEN/NOTIFY
    /// notifications, NoticeResponse, ParameterStatus, the delayed tail of a
    /// `Flush` response) is relayed to the client promptly instead of sitting
    /// unread until the next query, and a backend that dies while the session is
    /// idle is noticed at once — in which case its entry is removed from `conns`
    /// (the session survives; the next query redials) and the wait continues on
    /// the client alone.
    ///
    /// `idle_deadline` is the session's idle-session deadline; relaying async
    /// backend traffic to an idle client is not client activity, so the deadline
    /// is NOT re-armed while doing so.
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

    pub(super) async fn read_next_client_message(
        stream: &mut ClientStream,
        buffer: &mut BytesMut,
        conns: &mut HashMap<String, BackendConn>,
        watch_node: Option<&str>,
        abuf: &mut BytesMut,
        idle_deadline: Option<tokio::time::Instant>,
        state: &Arc<ServerState>,
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
            // `abuf` is the caller's session-scoped reassembly buffer (Fix S4:
            // reused, never re-allocated, and never zero-filled — `read_buf`
            // appends into spare capacity). It is deliberately NOT cleared here:
            // one read can end mid-frame, and only whole frames may be forwarded,
            // so any trailing partial frame stays buffered until the rest of it
            // arrives.
            abuf.reserve(16 * 1024);
            {
                let bc = conns.get_mut(node).expect("watch_node is in conns");
                tokio::select! {
                    r = stream.read_buf(buffer) => {
                        client_bytes = Some(r.map_err(|e| {
                            ProxyError::Network(format!("Read error: {}", e))
                        })?);
                    }
                    _ = &mut idle_wait => return Ok(ClientRead::IdleTimeout),
                    r = bc.stream.read_buf(&mut *abuf) => match r {
                        Ok(0) => backend_gone = true,
                        Ok(_) => {
                            // Publish only COMPLETE frames. A truncated frame is
                            // unrecoverable: the client blocks waiting for the
                            // rest, and nothing may ever be injected after it —
                            // not an ErrorResponse, and certainly not the rows of
                            // a re-executed statement, which would be appended to
                            // whatever the partial frame turns out to be.
                            let whole = Self::complete_frame_prefix(abuf, state.limits.max_backend_frame_bytes)?;
                            if whole > 0 {
                                tokio::time::timeout(state.limits.client_write_timeout, stream.write_all(&abuf[..whole]))
                                    .await
                                    .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                                    .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                                let _ = abuf.split_to(whole);
                                state.metrics.bytes_sent.fetch_add(whole as u64, Ordering::Relaxed);
                            }
                            // A single asynchronous frame must not grow the
                            // session's memory without bound while it reassembles.
                            if abuf.len() > state.limits.max_pending_bytes {
                                return Err(ProxyError::Protocol(format!(
                                    "backend asynchronous frame exceeds {} bytes",
                                    state.limits.max_pending_bytes
                                )));
                            }
                        }
                        Err(e) => {
                            tracing::debug!(node = %node, error = %e, "backend read error while idle; dropping cached connection");
                            backend_gone = true;
                        }
                    },
                }
            }
            if backend_gone {
                // Drop the dead cached connection but keep the client session
                // alive — the next query redials. (Mid-transaction the next
                // forward fails and surfaces the error.) Any partially received
                // frame dies with it: it was never published, so the client
                // stream stays frame-aligned and a synthesized error or a
                // re-executed statement can still be written safely.
                abuf.clear();
                conns.remove(node);
                return Self::read_client_bytes(stream, buffer, idle_deadline).await;
            }
            if let Some(cn) = client_bytes {
                return Ok(ClientRead::Bytes(cn));
            }
            // Otherwise we relayed async backend bytes; keep watching.
        }
    }

    /// Main client processing loop with full PostgreSQL protocol handling
    pub(super) async fn client_loop(
        stream: &mut ClientStream,
        pre: Option<StartupMessage>,
        mut buffer: BytesMut,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<()> {
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
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        // Bound the startup/authentication exchange in time (the TLS path reads
        // the real startup packet here, after negotiation). A client that opens
        // the connection but never completes startup must not hold the task and
        // its session slot open indefinitely.
        let startup_result = match tokio::time::timeout(
            state.limits.startup_timeout,
            Self::handle_startup(stream, &mut buffer, pre, session, state, config),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(ProxyError::Connection("startup timeout".to_string())),
        };
        let mut current_node: Option<String> = match startup_result {
            Ok((Some(stream_conn), node_addr)) => {
                conns.insert(node_addr.clone(), BackendConn::new(stream_conn));
                Some(node_addr)
            }
            Ok((None, _)) => {
                // SSL rejected or cancel request, connection should close
                return Ok(());
            }
            Err(e) => {
                tracing::error!("Startup failed: {}", e);
                // Send error to client
                let err_msg =
                    Self::create_error_response("08006", &format!("Startup failed: {}", e));
                let _ = stream.write_all(&err_msg).await;
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
        // Scratch buffer for the backend-watch below, created once per
        // session rather than once per client-message wait (a stack array
        // here would force the whole `handle_client` future to carry 16 KiB
        // live across every `.await` in this loop). Cleared — not
        // re-allocated — before each backend read via the same `clear()` +
        // `read_buf`-into-spare-capacity idiom `stream_flush` uses below, so
        // steady state costs no allocation and no zero-fill.
        let mut abuf = BytesMut::with_capacity(16384);
        // In-session Transaction Replay state (`tr_mode`): tracked session
        // SETs, the lost-while-idle backend, the aborted-transaction emulation
        // and the open extended-protocol cycle. See `tr_handle_fault`.
        let mut tr = TrSession::new(session.tr_mode);
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
            let idle_deadline = Self::client_idle_deadline(state, &buffer, session);
            // Borrowed, not cloned (Fix S4): the watch target is always the
            // session's current node, so cloning the node key into a fresh
            // `String` on every client-message wait was pure hot-path overhead.
            let watch_node: Option<&str> = if session
                .copy_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                None
            } else {
                current_node
                    .as_deref()
                    .filter(|node| conns.contains_key(*node))
            };
            let n: usize = match Self::read_next_client_message(
                stream,
                &mut buffer,
                &mut conns,
                watch_node,
                &mut abuf,
                idle_deadline,
                state,
            )
            .await?
            {
                ClientRead::Bytes(n) => n,
                ClientRead::IdleTimeout => {
                    // PostgreSQL's `idle_session_timeout` behaviour: tell the
                    // client why, then close. A session idle INSIDE an open
                    // transaction is terminated too — PostgreSQL splits that
                    // case out into the separate
                    // `idle_in_transaction_session_timeout` GUC, which this
                    // proxy does not implement. Leaving the loop (rather than
                    // returning here) keeps the normal teardown path, so under
                    // transaction/statement pooling the session's still-idle
                    // backend connections are parked for reuse as usual.
                    if !tr.ext_dispatched {
                        Self::terminate_idle_session(stream, state, session).await;
                    }
                    break;
                }
            };
            // `read_next_client_message` drops a watched backend connection that
            // died while the session was idle; the session survives and the next
            // query redials. `watch_node` is a reborrow of `current_node` (see
            // where it is built above), so a now-missing entry is always the
            // session's current node — no equality re-check is needed.
            let watched_backend_gone = watch_node.is_some_and(|node| !conns.contains_key(node));
            if watched_backend_gone {
                // F3: dropping the dead cached connection is enough only with
                // `tr_mode = none` outside a transaction (the next query simply
                // redials, as before). Otherwise the loss must be carried into
                // the next request as a not-delivered backend fault against
                // this node: a transaction that was open died with the socket
                // (it must NOT silently continue in autocommit on a fresh
                // connection), and session state (SETs) has to be restored on
                // the replacement connection. `tr_handle_fault` picks this up.
                //
                // Recorded here rather than inside `read_next_client_message`
                // (where O2/S4 moved the watch loop) because the helper is
                // session-agnostic — it takes no `&ClientSession` and owns no
                // `TrSession` — and `watched_backend_gone` already reconstructs
                // the same signal losslessly from `conns`.
                if session.tr_mode != TrMode::None
                    || tr.ext_dispatched
                    || session
                        .in_transaction
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    tr.lost_backend = watch_node.map(|node| node.to_string());
                }
                current_node = None;
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
                match msg.msg_type {
                    MessageType::Terminate => return Ok(()),

                    // ---- Simple query protocol ----
                    MessageType::Query => {
                        // Anomaly detector — record every Query message before
                        // the plugin hook so a detection lands in the audit
                        // trail even if a plugin later blocks.
                        #[cfg(feature = "anomaly-detection")]
                        Self::record_anomaly_observation(&msg, state, session);

                        // Plugin pre-query hook — may rewrite the SQL, block,
                        // or return a cached response.
                        let (msg, action) = Self::apply_pre_query_hook(msg, state, session);

                        if let PreQueryAction::Block(reason) = &action {
                            tracing::info!(reason = %reason, "pre-query plugin blocked query");
                            Self::send_block_response(stream, reason, state).await?;
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        #[cfg(feature = "wasm-plugins")]
                        if let PreQueryAction::Cached(bytes) = &action {
                            match Self::synthesise_cached_response(bytes) {
                                Ok(reply) => {
                                    stream.write_all(&reply).await.map_err(|e| {
                                        ProxyError::Network(format!("Write error: {}", e))
                                    })?;
                                    state
                                        .metrics
                                        .bytes_sent
                                        .fetch_add(reply.len() as u64, Ordering::Relaxed);
                                    state
                                        .metrics
                                        .queries_processed
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "failed to synthesise cached response; falling back to backend");
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
                            Self::tr_after_simple(&mut tr, &msg, session, state).await;
                            tr.ext_dispatched = false;
                            state
                                .metrics
                                .bytes_sent
                                .fetch_add(resp.len() as u64, Ordering::Relaxed);
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            resp.clear();
                            continue;
                        }

                        #[cfg(feature = "wasm-plugins")]
                        let forward_start = std::time::Instant::now();
                        let mut fault: Option<BackendFault> = None;
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
                                    stream,
                                    &msg,
                                    &mut conns,
                                    current_node.as_deref(),
                                    session,
                                    state,
                                    config,
                                    &mut fault,
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
                        let (used_node, sent) = match fr {
                            Ok(v) => v,
                            Err(e) => match fault.take() {
                                Some(f) => {
                                    match Self::tr_handle_fault(
                                        stream,
                                        &mut conns,
                                        &mut current_node,
                                        f,
                                        InFlight::Simple(&msg),
                                        &mut tr,
                                        &stmt_registry,
                                        session,
                                        state,
                                        config,
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
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        Self::tr_after_simple(&mut tr, &msg, session, state).await;
                        tr.ext_dispatched = false;
                        // Transaction/Statement pooling: park the connection
                        // back to the shared pool once the session is idle.
                        #[cfg(feature = "pool-modes")]
                        Self::release_to_pool_if_idle(
                            &mut conns,
                            current_node.as_deref(),
                            session,
                            state,
                            config,
                        )
                        .await;
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
                                    stmt_registry.insert(name.clone(), encoded);
                                    stmt_registry_bytes = projected;
                                    Self::remember_batch_name(
                                        &mut batch_defines,
                                        &name,
                                        state.limits.max_prepared_statements,
                                    );
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
                                                Some(crate::edge::fingerprint::tables_only(q))
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
                                                edge_stmt_meta.insert(
                                                    Self::parse_stmt_name(&msg.payload).to_string(),
                                                    meta,
                                                );
                                            }
                                        }
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
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    Self::remember_batch_name(
                                        &mut batch_refs,
                                        name,
                                        state.limits.max_prepared_statements,
                                    );
                                }
                            }
                            MessageType::Close => {
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    Self::remember_batch_name(
                                        &mut batch_closes,
                                        name,
                                        state.limits.max_prepared_statements,
                                    );
                                }
                            }
                            _ => {}
                        }
                        if add_to_pending {
                            msg.encode_into(&mut pending);
                        }
                    }

                    // ---- Extended batch boundary ----
                    MessageType::Sync | MessageType::Flush => {
                        let wait_ready = msg.msg_type == MessageType::Sync;
                        msg.encode_into(&mut pending);
                        let batch = pending.split().freeze();
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
                        if tr.tx_aborted {
                            let ends = pending_route_sql
                                .as_deref()
                                .map(Self::tr_ends_transaction)
                                .unwrap_or(false);
                            if ends {
                                tr.tx_aborted = false;
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
                            }
                        }
                        let (used_node, sent) = if skip_forward {
                            (None, 0)
                        } else {
                            let mut fault: Option<BackendFault> = None;
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
                                    Self::forward_extended_batch(
                                        stream,
                                        &batch,
                                        pending_route_sql.as_deref(),
                                        wait_ready,
                                        &mut conns,
                                        current_node.as_deref(),
                                        &stmt_registry,
                                        &reprepare,
                                        &batch_defines,
                                        held_unnamed.as_ref(),
                                        session,
                                        state,
                                        config,
                                        &mut fault,
                                    )
                                    .await
                                }
                            };
                            match fr {
                                Ok(v) => v,
                                Err(e) => match fault.take() {
                                    Some(f) => {
                                        match Self::tr_handle_fault(
                                            stream,
                                            &mut conns,
                                            &mut current_node,
                                            f,
                                            InFlight::Extended {
                                                batch: &batch,
                                                route_sql: pending_route_sql.as_deref(),
                                                wait_ready,
                                                reprepare: &reprepare,
                                                defines: &batch_defines,
                                                unnamed: held_unnamed.as_ref(),
                                            },
                                            &mut tr,
                                            &stmt_registry,
                                            session,
                                            state,
                                            config,
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
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        if !skip_forward {
                            Self::tr_after_extended(
                                &mut tr,
                                &batch,
                                held_unnamed.as_ref(),
                                pending_route_sql.as_deref(),
                                wait_ready,
                                &batch_defines,
                                &batch_refs,
                                &stmt_registry,
                                session,
                                state,
                            )
                            .await;
                            tr.ext_dispatched = !wait_ready;
                        }
                        held_unnamed = None;
                        // A `Sync` is the extended-protocol transaction/statement
                        // boundary (it yields ReadyForQuery); a `Flush` is not, so
                        // only a Sync triggers a pool release.
                        #[cfg(feature = "pool-modes")]
                        if wait_ready {
                            Self::release_to_pool_if_idle(
                                &mut conns,
                                current_node.as_deref(),
                                session,
                                state,
                                config,
                            )
                            .await;
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
                        // Closed statements are deallocated everywhere — forget
                        // their canonical Parse so they are never re-prepared.
                        #[cfg(not(feature = "edge-proxy"))]
                        for name in batch_closes.drain(..) {
                            if let Some(removed) = stmt_registry.remove(&name) {
                                stmt_registry_bytes =
                                    stmt_registry_bytes.saturating_sub(removed.len());
                            }
                        }
                        // Edge builds also prune per-statement invalidation metadata
                        // — but that metadata is consumed by the Sync-time hook ABOVE
                        // (which runs first) and must survive a Close seen at an
                        // earlier Flush of the same batch, and a name Closed then
                        // re-Parsed in this batch must keep its FRESH meta. So the
                        // close queue is drained only at the Sync (stmt_registry is
                        // still pruned every boundary — idempotent), and edge meta is
                        // removed there only for names not redefined this batch.
                        // Retaining meta for a genuinely-closed name is safe: a later
                        // re-Parse overwrites it, and a stale entry only ever
                        // over-invalidates.
                        #[cfg(feature = "edge-proxy")]
                        {
                            for name in &batch_closes {
                                if let Some(removed) = stmt_registry.remove(name) {
                                    stmt_registry_bytes =
                                        stmt_registry_bytes.saturating_sub(removed.len());
                                }
                            }
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
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // ---- COPY sub-protocol (client -> backend) ----
                    MessageType::CopyData | MessageType::CopyDone | MessageType::CopyFail => {
                        let is_copy_end =
                            matches!(msg.msg_type, MessageType::CopyDone | MessageType::CopyFail);
                        let conn = current_node.as_ref().and_then(|n| conns.get_mut(n));
                        match conn {
                            Some(b) => {
                                if let Err(e) = b.stream.write_all(&msg.encode()).await {
                                    // The backend died mid-COPY: the data stream
                                    // cannot be recovered in any tr_mode — one
                                    // error, then close (never a bare drop).
                                    let node = current_node.clone().unwrap_or_default();
                                    let err = format!("Backend copy write error: {}", e);
                                    conns.remove(&node);
                                    Self::record_backend_failure(state, &node, &err);
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
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
                                if is_copy_end {
                                    let node = current_node.clone().unwrap();
                                    let r = Self::stream_until_ready(
                                        stream,
                                        &mut b.stream,
                                        session,
                                        state,
                                    )
                                    .await;
                                    // Copy has drained back to ReadyForQuery — the
                                    // session is no longer mid-COPY, so pool release
                                    // is allowed again.
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    match r {
                                        Ok(sent) => {
                                            Self::tr_after_copy_drain(&mut tr, session).await;
                                            state
                                                .metrics
                                                .bytes_sent
                                                .fetch_add(sent, Ordering::Relaxed);
                                            // COPY ... FROM rows became visible at
                                            // this drain: fire the deferred edge
                                            // invalidation now. CopyFail loaded
                                            // nothing — just clear the stash.
                                            #[cfg(feature = "edge-proxy")]
                                            if config.edge.enabled {
                                                let stashed = session
                                                    .pending_edge_copy_tables
                                                    .lock()
                                                    .unwrap_or_else(|e| e.into_inner())
                                                    .take();
                                                if let Some(tables) = stashed {
                                                    if matches!(msg.msg_type, MessageType::CopyDone)
                                                    {
                                                        Self::edge_invalidate_write(
                                                            state, config, tables,
                                                        )
                                                        .await;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            conns.remove(&node);
                                            let err = e.to_string();
                                            if err.contains("Client") {
                                                return Err(e.error);
                                            }
                                            if e.progress.terminal || e.progress.raw {
                                                return Ok(());
                                            }
                                            // Backend fault mid-COPY drain: tell the
                                            // client (08006) and close — every tr_mode.
                                            Self::record_backend_failure(state, &node, &err);
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
                                // return it to a clean idle state.
                                session
                                    .copy_in_progress
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
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
                                if is_copy_end {
                                    let emsg = Self::create_error_response(
                                        "57000",
                                        "COPY aborted: backend connection lost",
                                    );
                                    let _ = stream.write_all(&emsg).await;
                                    let _ =
                                        stream.write_all(&Self::create_ready_for_query(b'I')).await;
                                }
                            }
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
        // mid-transaction is left to drop (closed → backend rolls back).
        #[cfg(feature = "pool-modes")]
        if state.backend_pool.is_some() {
            let nodes: Vec<String> = conns.keys().cloned().collect();
            for node in nodes {
                Self::release_to_pool_if_idle(
                    &mut conns,
                    Some(node.as_str()),
                    session,
                    state,
                    config,
                )
                .await;
            }
        }

        Ok(())
    }
}
