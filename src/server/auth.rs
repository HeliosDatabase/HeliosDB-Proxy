use super::*;

impl ProxyServer {
    /// Read one startup-phase message (`Startup`, `SSLRequest` or
    /// `CancelRequest`) from a client stream, appending whatever it reads into
    /// `buffer` so any bytes that follow the message are preserved for the
    /// caller. `Ok(None)` = the client closed before a complete message
    /// arrived. Callers bound this in time (pre-auth `startup_timeout`).
    pub(super) async fn read_startup_message<S: AsyncRead + Unpin>(
        stream: &mut S,
        buffer: &mut BytesMut,
    ) -> Result<Option<StartupMessage>> {
        let codec = ProtocolCodec::new();
        let mut read_buf = vec![0u8; 1024];
        loop {
            if let Some(msg) = codec.decode_startup(buffer)? {
                return Ok(Some(msg));
            }
            let n = stream
                .read(&mut read_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("Startup read error: {}", e)))?;
            if n == 0 {
                return Ok(None);
            }
            buffer.extend_from_slice(&read_buf[..n]);
        }
    }

    /// Peek the first startup-phase message and negotiate client TLS.
    ///
    /// On `SSLRequest` the proxy answers `S` and runs a rustls server
    /// handshake when a TLS acceptor is configured, otherwise `N`
    /// (plaintext). A `Startup`/`CancelRequest` arriving first (no
    /// SSLRequest) is returned in `pre` so the caller doesn't re-read it.
    pub(super) async fn negotiate_client_tls(
        mut tcp: TcpStream,
        state: &Arc<ServerState>,
    ) -> Result<(ClientStream, Option<StartupMessage>)> {
        let mut buffer = BytesMut::with_capacity(1024);
        let first = match Self::read_startup_message(&mut tcp, &mut buffer).await? {
            Some(msg) => msg,
            None => {
                return Err(ProxyError::Connection(
                    "client closed before startup".to_string(),
                ))
            }
        };

        match first {
            StartupMessage::SSLRequest => match state.tls_acceptor.as_ref() {
                Some(acceptor) => {
                    tcp.write_all(b"S")
                        .await
                        .map_err(|e| ProxyError::Network(format!("SSL accept write: {}", e)))?;
                    let tls = acceptor
                        .accept(tcp)
                        .await
                        .map_err(|e| ProxyError::Network(format!("TLS handshake failed: {}", e)))?;
                    if tls.get_ref().1.peer_certificates().is_some() {
                        tracing::debug!("client presented a certificate (mTLS)");
                    }
                    Ok((ClientStream::Tls(Box::new(tls)), None))
                }
                None => {
                    tcp.write_all(b"N")
                        .await
                        .map_err(|e| ProxyError::Network(format!("SSL reject write: {}", e)))?;
                    Ok((ClientStream::Plain(tcp), None))
                }
            },
            other => Ok((ClientStream::Plain(tcp), Some(other))),
        }
    }

    /// Handle PostgreSQL startup phase (authentication). TLS/SSLRequest is
    /// already handled upstream in `negotiate_client_tls`; `pre` carries the
    /// first startup/cancel message when it was read during negotiation.
    pub(super) async fn handle_startup(
        client_stream: &mut ClientStream,
        buffer: &mut BytesMut,
        pre: Option<StartupMessage>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // Use the message already read during TLS negotiation, or read one
        // now (the TLS case, where the real startup follows the handshake).
        let startup_msg = match pre {
            Some(msg) => Some(msg),
            None => match Self::read_startup_message(client_stream, buffer).await? {
                Some(msg) => Some(msg),
                // Client closed before sending a complete startup message.
                None => return Ok((None, String::new())),
            },
        };

        match startup_msg {
            Some(StartupMessage::SSLRequest) => {
                // SSL is negotiated upstream; a second SSLRequest here is a
                // protocol error — reject defensively.
                client_stream
                    .write_all(b"N")
                    .await
                    .map_err(|e| ProxyError::Network(format!("SSL reject error: {}", e)))?;
                Err(ProxyError::Protocol(
                    "unexpected SSLRequest after startup".to_string(),
                ))
            }
            Some(StartupMessage::CancelRequest { pid, key }) => {
                // Forward the cancel to the backend that owns this key, then
                // close (the client opened this connection only to cancel).
                Self::forward_cancel_request(state, pid, key).await;
                Ok((None, String::new()))
            }
            Some(StartupMessage::Startup { params, .. }) => {
                Self::connect_and_authenticate(client_stream, &params, session, state, config).await
            }
            None => Err(ProxyError::Protocol(
                "Incomplete startup message".to_string(),
            )),
        }
    }

    /// Evaluate pg_hba-style admission rules in order. The first rule whose
    /// user, database, and address all match decides; if none match, admit.
    pub(super) fn hba_admits(
        rules: &[HbaRule],
        ip: std::net::IpAddr,
        user: &str,
        database: &str,
    ) -> bool {
        for r in rules {
            let user_ok = r.user == "all" || r.user == user;
            let db_ok = r.database == "all" || r.database == database;
            if user_ok && db_ok && Self::hba_addr_matches(&r.address, ip) {
                return r.action == HbaAction::Allow;
            }
        }
        true
    }

    /// Match a client address against an hba `address` spec: "all", a bare
    /// IP, or a CIDR (`10.0.0.0/8`, `::1/128`).
    pub(super) fn hba_addr_matches(spec: &str, ip: std::net::IpAddr) -> bool {
        use std::net::IpAddr;
        if spec == "all" {
            return true;
        }
        if let Some((net, bits)) = spec.split_once('/') {
            let bits: u32 = match bits.parse() {
                Ok(b) => b,
                Err(_) => return false,
            };
            match (net.parse::<IpAddr>(), ip) {
                (Ok(IpAddr::V4(n)), IpAddr::V4(i)) if bits <= 32 => {
                    let mask = if bits == 0 {
                        0
                    } else {
                        u32::MAX << (32 - bits)
                    };
                    (u32::from(n) & mask) == (u32::from(i) & mask)
                }
                (Ok(IpAddr::V6(n)), IpAddr::V6(i)) if bits <= 128 => {
                    let mask = if bits == 0 {
                        0
                    } else {
                        u128::MAX << (128 - bits)
                    };
                    (u128::from(n) & mask) == (u128::from(i) & mask)
                }
                _ => false,
            }
        } else {
            spec.parse::<IpAddr>().map(|s| s == ip).unwrap_or(false)
        }
    }

    /// Run a proxy-terminated SCRAM-SHA-256 server exchange against the
    /// client, validating its password with the configured `auth_file`. On
    /// success the client is authenticated by the proxy (no AuthenticationOk
    /// is sent here — the backend's is forwarded later). On any failure
    /// returns Err; the caller emits an ErrorResponse and closes.
    pub(super) async fn proxy_scram_auth(
        client: &mut ClientStream,
        user: &str,
        state: &Arc<ServerState>,
    ) -> std::result::Result<(), String> {
        use crate::auth_scram::ScramServer;
        let auth_file = state.auth_file.as_ref().ok_or("scram not configured")?;

        // 1. AuthenticationSASL: advertise SCRAM-SHA-256.
        let mut sasl = BytesMut::new();
        sasl.put_i32(10); // SASL
        sasl.extend_from_slice(b"SCRAM-SHA-256\0");
        sasl.put_u8(0); // end of mechanism list
        Self::write_auth_frame(client, &sasl).await?;

        // 2. Read SASLInitialResponse ('p'): mechanism cstring + i32 len + data.
        let init = Self::read_password_message(client).await?;
        let mech_end = init
            .iter()
            .position(|&b| b == 0)
            .ok_or("malformed SASLInitialResponse (no mechanism)")?;
        if init.len() < mech_end + 5 {
            return Err("short SASLInitialResponse".into());
        }
        let client_first =
            std::str::from_utf8(&init[mech_end + 5..]).map_err(|_| "client-first not UTF-8")?;

        // 3. Look up the verifier (unknown user -> generic failure).
        let verifier = auth_file.get(user).ok_or("no such user")?.clone();

        // 4. server-first.
        let server_nonce = Self::random_nonce();
        let (server, server_first) = ScramServer::start(verifier, client_first, &server_nonce)?;

        // 5. AuthenticationSASLContinue.
        let mut cont = BytesMut::new();
        cont.put_i32(11);
        cont.extend_from_slice(server_first.as_bytes());
        Self::write_auth_frame(client, &cont).await?;

        // 6. Read SASLResponse ('p'): payload = client-final.
        let client_final_raw = Self::read_password_message(client).await?;
        let client_final =
            std::str::from_utf8(&client_final_raw).map_err(|_| "client-final not UTF-8")?;

        // 7. Verify -> server-final.
        let server_final = server.finish(client_final)?;

        // 8. AuthenticationSASLFinal (no AuthenticationOk — backend's follows).
        let mut fin = BytesMut::new();
        fin.put_i32(12);
        fin.extend_from_slice(server_final.as_bytes());
        Self::write_auth_frame(client, &fin).await?;
        Ok(())
    }

    /// Write an AuthenticationRequest ('R') frame with the given payload.
    pub(super) async fn write_auth_frame(
        client: &mut ClientStream,
        payload: &[u8],
    ) -> std::result::Result<(), String> {
        let mut frame = BytesMut::with_capacity(payload.len() + 5);
        frame.put_u8(b'R');
        frame.put_u32((payload.len() + 4) as u32);
        frame.extend_from_slice(payload);
        client
            .write_all(&frame)
            .await
            .map_err(|e| format!("client write: {}", e))
    }

    /// Read one Password/SASL ('p') message from the client, returning its
    /// payload. Errors on EOF or any non-'p' frame.
    pub(super) async fn read_password_message(
        client: &mut ClientStream,
    ) -> std::result::Result<BytesMut, String> {
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(1024);
        let mut read_buf = vec![0u8; 1024];
        loop {
            if let Some(msg) = codec
                .decode_message(&mut buffer)
                .map_err(|e| format!("decode: {}", e))?
            {
                if msg.msg_type == MessageType::Password {
                    return Ok(msg.payload);
                }
                return Err(format!("expected SASL response, got {:?}", msg.msg_type));
            }
            let n = client
                .read(&mut read_buf)
                .await
                .map_err(|e| format!("client read: {}", e))?;
            if n == 0 {
                return Err("client closed during SASL".into());
            }
            buffer.extend_from_slice(&read_buf[..n]);
        }
    }

    /// A fresh random SCRAM server nonce (printable, no comma).
    pub(super) fn random_nonce() -> String {
        use rand::Rng;
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::thread_rng();
        (0..24)
            .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
            .collect()
    }

    /// Connect to backend and handle authentication
    pub(super) async fn connect_and_authenticate(
        client_stream: &mut ClientStream,
        params: &HashMap<String, String>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // pg_hba-style admission: reject disallowed (user, database, client
        // address) combinations before opening any backend connection.
        let user = params.get("user").map(String::as_str).unwrap_or("");
        let database = params.get("database").map(String::as_str).unwrap_or(user);
        if !Self::hba_admits(&config.hba, session.client_addr.ip(), user, database) {
            tracing::info!(%user, %database, client = %session.client_addr, "connection rejected by hba rule");
            let err = Self::create_error_response(
                "28000",
                "connection rejected by proxy admission rules",
            );
            let _ = client_stream.write_all(&err).await;
            return Ok((None, String::new()));
        }

        // Proxy-terminated SCRAM-SHA-256: when an auth_file is configured the
        // proxy authenticates the client itself (becoming the auth boundary)
        // instead of relaying credentials to the backend. On success it falls
        // through to the normal backend connect, whose AuthenticationOk +
        // session messages are forwarded to the already-authenticated client.
        if state.auth_file.is_some() {
            if let Err(e) = Self::proxy_scram_auth(client_stream, user, state).await {
                tracing::info!(%user, error = %e, "proxy SCRAM auth failed");
                let err =
                    Self::create_error_response("28P01", &format!("authentication failed: {}", e));
                let _ = client_stream.write_all(&err).await;
                return Ok((None, String::new()));
            }
            tracing::debug!(%user, "client authenticated by proxy SCRAM");
        }

        // Plugin Authenticate hook — may deny the connection outright or
        // attach a richer identity (roles, tenant_id, claims) onto the
        // session for downstream plugins to consume. Happens before any
        // backend connection is opened so denials cost nothing on the
        // backend side.
        Self::apply_authenticate_hook(params, session, state).await?;

        // Migration cutover: when active, redirect this connection to the
        // promoted target, substituting the target's credentials/database for
        // the client's so the cutover is transparent to the application.
        let cutover = state.cutover.load_full();
        let (node_addr, effective_params) = if let Some(t) = cutover.as_ref() {
            let mut p = params.clone();
            p.insert("user".to_string(), t.user.clone());
            if let Some(ref db) = t.database {
                p.insert("database".to_string(), db.clone());
            } else {
                p.remove("database");
            }
            tracing::debug!(target = %t.addr, "routing connection to cutover target");
            (t.addr.clone(), p)
        } else {
            (
                Self::select_node(session, state, config).await?,
                params.clone(),
            )
        };

        // Connect to backend. A failure here (the node is down at the moment a
        // new client connects) demotes the node in-band too — not just failures
        // on the forward path — so a dead backend is detected on the very next
        // connection instead of waiting for the periodic health checker.
        let params = &effective_params;
        let mut backend = Self::dial_backend_startup(&node_addr, params, config, state).await?;

        // A backend at `max_connections` refuses the new connection with 53300
        // right after the startup packet — before any authentication exchange
        // and before anything reaches the client. Parked pool connections hold
        // backend slots that are idle by definition, and a pass-through client
        // cannot borrow one for its own authentication, so free one idle
        // connection to this node and redial with jittered backoff, bounded by
        // `[pool] acquire_timeout_secs`; only then refuse the client with a
        // truthful 53300. A full backend is not a failed node: nothing here
        // demotes it.
        let capacity_deadline = tokio::time::Instant::now() + config.pool.acquire_timeout();
        let seed = session.id.as_u128() as u64;
        let mut attempt: u32 = 0;
        loop {
            match Self::authenticate_backend(
                client_stream,
                &mut backend,
                session,
                state,
                user,
                &node_addr,
            )
            .await
            {
                Err(ProxyError::PoolExhausted(msg)) => {
                    if !Self::backend_capacity_backoff(
                        state,
                        &node_addr,
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
                        tracing::warn!(
                            node = %node_addr,
                            attempts = attempt + 1,
                            "backend at max_connections for the whole acquire timeout; refusing the client with 53300"
                        );
                        let err = Self::create_error_response(SQLSTATE_TOO_MANY_CONNECTIONS, &msg);
                        let _ = client_stream.write_all(&err).await;
                        return Err(ProxyError::PoolExhausted(msg));
                    }
                    attempt += 1;
                    backend = Self::dial_backend_startup(&node_addr, params, config, state).await?;
                }
                other => {
                    other?;
                    break;
                }
            }
        }

        // Store session variables
        {
            let mut vars = session.variables.write().await;
            for (k, v) in params {
                vars.insert(k.clone(), v.clone());
            }
        }

        Ok((Some(backend), node_addr))
    }

    /// Dial `node_addr` and send the startup packet for `params`. A connect
    /// failure demotes the node in-band (a dead backend is detected on the
    /// next connection, not only by the periodic health checker).
    pub(super) async fn dial_backend_startup(
        node_addr: &str,
        params: &HashMap<String, String>,
        config: &ProxyConfig,
        state: &Arc<ServerState>,
    ) -> Result<TcpStream> {
        let mut backend = match tokio::time::timeout(
            config.pool.acquire_timeout(),
            TcpStream::connect(node_addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                let msg = format!("Failed to connect to {}: {}", node_addr, e);
                Self::note_backend_failure(state, node_addr, &msg);
                return Err(ProxyError::Connection(msg));
            }
            Err(_) => {
                let msg = format!("Connection timeout to {}", node_addr);
                Self::note_backend_failure(state, node_addr, &msg);
                return Err(ProxyError::Connection(msg));
            }
        };
        let _ = backend.set_nodelay(true);
        let startup_bytes = Self::build_startup_message(params);
        backend
            .write_all(&startup_bytes)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend startup write error: {}", e)))?;
        Ok(backend)
    }

    /// Authenticate the freshly dialed startup connection: as the proxy when
    /// an auth_file is configured, otherwise by relaying the client's own
    /// exchange. Returns `PoolExhausted` — with nothing written to the client
    /// — when the backend refused the connection at `max_connections`.
    pub(super) async fn authenticate_backend(
        client_stream: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        user: &str,
        node_addr: &str,
    ) -> Result<()> {
        if let Some(af) = state.auth_file.as_ref() {
            // The proxy is the auth boundary: the client is already
            // authenticated, so the backend's challenges must NOT be relayed to
            // it. Authenticate the backend ourselves (SCRAM/MD5/cleartext with
            // the user's plaintext auth_file entry; trust needs nothing), then
            // hand the client a synthesized AuthenticationOk followed by the
            // backend's own ParameterStatus/BackendKeyData/ReadyForQuery.
            let credential = af.password(user).map(str::to_string);
            let post_auth = match Self::complete_backend_auth(
                backend,
                state.limits.max_pending_bytes,
                user,
                credential.as_deref(),
            )
            .await
            {
                Ok(frames) => frames,
                // Capacity refusal: nothing was sent to the client; the caller
                // frees idle capacity and redials (not a node failure).
                Err(e @ ProxyError::PoolExhausted(_)) => return Err(e),
                Err(e) => {
                    Self::note_backend_failure(state, node_addr, &e.to_string());
                    let err = Self::create_error_response(
                        "08006",
                        &format!("backend authentication failed: {}", e),
                    );
                    let _ = client_stream.write_all(&err).await;
                    return Err(e);
                }
            };
            *session.backend_credential.write().await = credential;
            // Register the cancel key from the BackendKeyData frame.
            let mut off = 0usize;
            while off + 5 <= post_auth.len() {
                let len = u32::from_be_bytes([
                    post_auth[off + 1],
                    post_auth[off + 2],
                    post_auth[off + 3],
                    post_auth[off + 4],
                ]) as usize;
                if len < 4 || off + 1 + len > post_auth.len() {
                    break;
                }
                if post_auth[off] == b'K' && len + 1 >= 13 {
                    let f = &post_auth[off..off + 1 + len];
                    let pid = u32::from_be_bytes([f[5], f[6], f[7], f[8]]);
                    let key = u32::from_be_bytes([f[9], f[10], f[11], f[12]]);
                    Self::register_cancel_key(state, pid, key, node_addr);
                }
                off += 1 + len;
            }
            let mut to_client = Vec::with_capacity(9 + post_auth.len());
            to_client.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]); // AuthenticationOk
            to_client.extend_from_slice(&post_auth);
            client_stream
                .write_all(&to_client)
                .await
                .map_err(|e| ProxyError::Network(format!("Client auth write error: {}", e)))?;
            Ok(())
        } else {
            // Pass-through: forward authentication messages between client and
            // backend. Registers the backend's BackendKeyData so a later
            // CancelRequest can be routed back to this node.
            Self::proxy_authentication(client_stream, backend, state, node_addr).await
        }
    }

    /// One capacity-wait step after a 53300 refusal: close one idle pooled
    /// connection to `node` and sleep a jittered backoff. Returns `false`
    /// (without sleeping) when the backoff would cross `deadline`.
    pub(super) async fn backend_capacity_backoff(
        state: &ServerState,
        node: &str,
        attempt: u32,
        seed: u64,
        deadline: tokio::time::Instant,
    ) -> bool {
        let freed_idle = Self::release_idle_capacity(state, node);
        let pause = Self::reconnect_backoff(attempt, seed);
        if tokio::time::Instant::now() + pause >= deadline {
            return false;
        }
        state
            .metrics
            .backend_capacity_waits
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(%node, attempt, freed_idle, "backend at max_connections; retrying");
        tokio::time::sleep(pause).await;
        true
    }

    /// Close one idle pooled connection to `node`, returning whether one
    /// was parked. Without the pool there is nothing to release.
    pub(super) fn release_idle_capacity(state: &ServerState, node: &str) -> bool {
        #[cfg(feature = "pool-modes")]
        if let Some(pool) = state.backend_pool.as_ref() {
            return pool.evict_one_idle_for_node(node);
        }
        #[cfg(not(feature = "pool-modes"))]
        let _ = (state, node);
        false
    }

    /// `(SQLSTATE, message)` of a complete backend `ErrorResponse` frame
    /// (tag and length included).
    pub(super) fn error_response_fields(frame: &[u8]) -> (String, String) {
        let parsed = frame
            .get(5..)
            .map(BytesMut::from)
            .and_then(|payload| ErrorResponse::parse(payload).ok());
        match parsed {
            Some(e) => (
                e.code().unwrap_or("").to_string(),
                e.message().unwrap_or("backend error").to_string(),
            ),
            None => (String::new(), "malformed backend error".to_string()),
        }
    }

    /// Build PostgreSQL startup message
    pub(super) fn build_startup_message(params: &HashMap<String, String>) -> Vec<u8> {
        let mut payload = BytesMut::new();

        // Protocol version 3.0
        payload.put_u32(196608);

        // Parameters
        for (key, value) in params {
            payload.extend_from_slice(key.as_bytes());
            payload.put_u8(0);
            payload.extend_from_slice(value.as_bytes());
            payload.put_u8(0);
        }
        payload.put_u8(0); // Terminator

        // Build complete message with length prefix
        let mut msg = BytesMut::new();
        msg.put_u32((payload.len() + 4) as u32);
        msg.extend_from_slice(&payload);

        msg.to_vec()
    }

    // The operational limits/timeouts that were compiled-in `const`s here are
    // now tunable via the `[limits]` config section (`crate::config::LimitsToml`),
    // resolved once at startup into `ServerState::limits` (`ResolvedLimits`).
    // Defaults are byte-for-byte the prior constants; see that struct.

    /// Record the backend that owns a BackendKeyData (pid, secret) pair.
    pub(super) fn register_cancel_key(
        state: &Arc<ServerState>,
        pid: u32,
        key: u32,
        node_addr: &str,
    ) {
        // FIFO-evict the oldest registrations when at capacity, rather than
        // dropping all of them. Evict a small batch so we don't churn the lock
        // on every insert once full.
        {
            let mut order = state.cancel_order.lock();
            while state.cancel_map.len() >= state.limits.max_cancel_keys {
                match order.pop_front() {
                    Some(old) => {
                        state.cancel_map.remove(&old);
                    }
                    None => {
                        // Order queue empty but map full (shouldn't happen) —
                        // fall back to a clear to stay bounded.
                        state.cancel_map.clear();
                        break;
                    }
                }
            }
            order.push_back((pid, key));
        }
        state.cancel_map.insert((pid, key), node_addr.to_string());
    }

    /// Forward a client CancelRequest to the backend that issued the
    /// matching BackendKeyData. Best-effort: unknown keys are ignored.
    pub(super) async fn forward_cancel_request(state: &Arc<ServerState>, pid: u32, key: u32) {
        let Some(addr) = state.cancel_map.get(&(pid, key)).map(|e| e.clone()) else {
            tracing::debug!(pid, "cancel request for unknown key; ignoring");
            return;
        };
        // CancelRequest: int32 len(16) + int32 code(80877102) + pid + key.
        let mut msg = BytesMut::with_capacity(16);
        msg.put_u32(16);
        msg.put_u32(80877102);
        msg.put_u32(pid);
        msg.put_u32(key);
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr)).await {
            Ok(Ok(mut conn)) => {
                let _ = conn.set_nodelay(true);
                if let Err(e) = conn.write_all(&msg).await {
                    tracing::warn!(node = %addr, error = %e, "failed to forward CancelRequest");
                }
                // PG closes the connection after handling a CancelRequest.
            }
            other => {
                tracing::warn!(node = %addr, ?other, "could not connect to forward CancelRequest")
            }
        }
    }

    /// Proxy authentication messages between client and backend
    pub(super) async fn proxy_authentication(
        client_stream: &mut ClientStream,
        backend_stream: &mut TcpStream,
        state: &Arc<ServerState>,
        node_addr: &str,
    ) -> Result<()> {
        // Bidirectional relay driven by readiness, not a fixed poll. The old
        // loop read the backend (untimed), forwarded, then polled the client
        // with a fixed 100ms window; a client that answered an auth challenge
        // more than 100ms after receiving it (WAN RTT, slow SCRAM client) missed
        // its window, and the loop then re-blocked on the untimed backend read
        // while the backend waited for the very response the proxy never read —
        // a deadlock until PostgreSQL's authentication_timeout killed it. Here
        // both directions are relayed as either side becomes readable, under one
        // overall deadline, so multi-round SCRAM completes regardless of client
        // latency.
        //
        // Backend-side frames are inspected by RAW tag (the wire decoder is
        // direction-agnostic — 'E' would decode to the client-side `Execute`,
        // not `ErrorResponse`), so a backend auth error is recognised here
        // rather than falling through to a misleading timeout.
        let mut backend_buffer = BytesMut::with_capacity(4096);
        let mut cbuf = vec![0u8; 4096];
        let mut bbuf = vec![0u8; 4096];
        let deadline = tokio::time::Instant::now() + state.limits.startup_timeout;
        // Whether any backend frame has reached the client yet. A 53300 refusal
        // arrives as the very first frame and is held back, so the caller can
        // free idle capacity and redial without the client seeing it.
        let mut relayed_any = false;

        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ProxyError::Auth(
                        "authentication timed out".to_string(),
                    ));
                }
                // Backend -> client: relay every byte, then scan complete frames
                // for the auth terminal states.
                r = backend_stream.read(&mut bbuf) => {
                    let n = r.map_err(|e| {
                        ProxyError::Network(format!("Backend auth read error: {}", e))
                    })?;
                    if n == 0 {
                        return Err(ProxyError::Connection(
                            "Backend closed during auth".to_string(),
                        ));
                    }
                    backend_buffer.extend_from_slice(&bbuf[..n]);

                    // Walk complete frames by raw tag; relay each complete
                    // frame (one write per read) and stop at a terminal one.
                    let mut relay: Vec<u8> = Vec::with_capacity(n);
                    let mut outcome: Option<Result<()>> = None;
                    loop {
                        if backend_buffer.len() < 5 {
                            break;
                        }
                        let len = u32::from_be_bytes([
                            backend_buffer[1],
                            backend_buffer[2],
                            backend_buffer[3],
                            backend_buffer[4],
                        ]) as usize;
                        if len < 4 {
                            break;
                        }
                        validate_backend_frame_len(len, state.limits.max_pending_bytes)?;
                        if backend_buffer.len() < len + 1 {
                            break;
                        }
                        let tag = backend_buffer[0];
                        let frame = backend_buffer.split_to(len + 1);
                        if tag == b'E' {
                            let (code, message) = Self::error_response_fields(&frame);
                            if !relayed_any
                                && relay.is_empty()
                                && code == SQLSTATE_TOO_MANY_CONNECTIONS
                            {
                                return Err(ProxyError::PoolExhausted(format!(
                                    "backend {} is at max_connections ({}): {}",
                                    node_addr, code, message
                                )));
                            }
                            relay.extend_from_slice(&frame);
                            // Relayed to the client below; report what the
                            // backend actually said. Only class 28 is an
                            // authentication failure.
                            outcome = Some(Err(if code.starts_with("28") {
                                ProxyError::Auth(format!("{}: {}", code, message))
                            } else {
                                ProxyError::Connection(format!(
                                    "backend refused the session ({}): {}",
                                    code, message
                                ))
                            }));
                            break;
                        }
                        relay.extend_from_slice(&frame);
                        match tag {
                            // BackendKeyData: 5-byte header + pid(4) + key(4).
                            // Remember which backend owns this cancel key.
                            b'K' if frame.len() >= 13 => {
                                let pid = u32::from_be_bytes([
                                    frame[5], frame[6], frame[7], frame[8],
                                ]);
                                let key = u32::from_be_bytes([
                                    frame[9], frame[10], frame[11], frame[12],
                                ]);
                                Self::register_cancel_key(state, pid, key, node_addr);
                            }
                            // ReadyForQuery: authentication + startup complete.
                            b'Z' => {
                                outcome = Some(Ok(()));
                                break;
                            }
                            _ => {}
                        }
                    }
                    if !relay.is_empty() {
                        client_stream
                            .write_all(&relay)
                            .await
                            .map_err(|e| ProxyError::Network(format!("Client auth write error: {}", e)))?;
                        relayed_any = true;
                    }
                    if let Some(result) = outcome {
                        return result;
                    }
                }
                // Client -> backend: relay the client's auth response(s)
                // whenever they arrive, with no artificial deadline of their own.
                r = client_stream.read(&mut cbuf) => {
                    let n = r.map_err(|e| {
                        ProxyError::Network(format!("Client auth read error: {}", e))
                    })?;
                    if n == 0 {
                        return Err(ProxyError::Connection(
                            "Client closed during auth".to_string(),
                        ));
                    }
                    backend_stream
                        .write_all(&cbuf[..n])
                        .await
                        .map_err(|e| {
                            ProxyError::Network(format!("Backend password write error: {}", e))
                        })?;
                }
            }
        }
    }

    /// Complete backend authentication by reading until ReadyForQuery
    /// This is used when switching backends - we don't forward auth to client.
    /// `max_frame_len` bounds a single declared backend frame length (reuses
    /// `state.limits.max_pending_bytes`; see [`validate_backend_frame_len`]).
    /// Drive a freshly dialed backend connection (startup already sent) to
    /// `ReadyForQuery`, answering its authentication challenges with
    /// `credential` when it has any: SCRAM-SHA-256 (the RFC-5802 client in
    /// `backend::auth`), MD5 and cleartext password. Without a credential
    /// only a non-challenging (trust) backend can be completed — a challenge
    /// then fails fast with `ProxyError::Auth` instead of timing out.
    ///
    /// Returns the post-authentication frames the backend sent (ParameterStatus,
    /// BackendKeyData, ReadyForQuery — every frame except the `R` auth
    /// requests) so a caller that is the client's auth boundary can forward
    /// them after its own synthesized `AuthenticationOk`.
    pub(super) async fn complete_backend_auth<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        max_frame_len: usize,
        user: &str,
        credential: Option<&str>,
    ) -> Result<Vec<u8>> {
        let mut buffer = BytesMut::with_capacity(4096);
        let mut forward: Vec<u8> = Vec::with_capacity(512);
        let mut scram: Option<crate::backend::auth::Scram> = None;
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(ProxyError::Auth(
                    "Backend authentication timeout".to_string(),
                ));
            }

            buffer.reserve(4096);
            let n = tokio::time::timeout(Duration::from_secs(5), backend.read_buf(&mut buffer))
                .await
                .map_err(|_| ProxyError::Auth("Read timeout during backend auth".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend auth read error: {}", e)))?;

            if n == 0 {
                return Err(ProxyError::Connection(
                    "Backend closed during auth".to_string(),
                ));
            }

            // Walk complete frames by raw tag. The wire decoder is
            // direction-agnostic ('E' decodes to the client-side `Execute`), so
            // a backend ErrorResponse must be detected by its raw tag rather
            // than by `msg_type` — the previous version matched
            // `MessageType::ErrorResponse`, which never fired, so a failed
            // backend auth surfaced as a misleading timeout.
            loop {
                if buffer.len() < 5 {
                    break;
                }
                let len = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                if len < 4 {
                    break;
                }
                validate_backend_frame_len(len, max_frame_len)?;
                if buffer.len() < len + 1 {
                    break;
                }
                let tag = buffer[0];
                let frame = buffer.split_to(len + 1);
                match tag {
                    // ReadyForQuery: authentication complete.
                    b'Z' => {
                        forward.extend_from_slice(&frame);
                        return Ok(forward);
                    }
                    // ErrorResponse: parse its message for a clear error.
                    b'E' => {
                        let payload = BytesMut::from(&frame[5..]);
                        let parsed = ErrorResponse::parse(payload).ok();
                        let code = parsed.as_ref().and_then(|e| e.code()).unwrap_or("");
                        let err = parsed
                            .as_ref()
                            .map(|e| e.message().unwrap_or("Unknown error").to_string())
                            .unwrap_or_else(|| "authentication failed".to_string());
                        if code == SQLSTATE_TOO_MANY_CONNECTIONS {
                            return Err(ProxyError::PoolExhausted(format!(
                                "backend is at max_connections ({}): {}",
                                code, err
                            )));
                        }
                        return Err(ProxyError::Auth(err));
                    }
                    // Authentication request: answer the challenge.
                    b'R' if frame.len() >= 9 => {
                        let kind = u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]);
                        let body = &frame[9..];
                        let reply: Option<Vec<u8>> = match kind {
                            0 => None, // AuthenticationOk
                            3 | 5 | 10 | 11 | 12 => {
                                let Some(password) = credential else {
                                    return Err(ProxyError::Auth(format!(
                                        "backend requires authentication (type {}) for user '{}' but the proxy holds no credential: pass-through auth cannot open a fresh backend connection — configure [auth] mode = \"scram\" with a plaintext auth_file entry, or a trust backend",
                                        kind, user
                                    )));
                                };
                                match kind {
                                    // CleartextPassword
                                    3 => {
                                        let mut p = password.as_bytes().to_vec();
                                        p.push(0);
                                        Some(p)
                                    }
                                    // MD5Password: 4-byte salt.
                                    5 => {
                                        if body.len() < 4 {
                                            return Err(ProxyError::Auth(
                                                "malformed MD5 challenge".to_string(),
                                            ));
                                        }
                                        let salt = [body[0], body[1], body[2], body[3]];
                                        Some(crate::backend::auth::md5_password_response(
                                            user, password, &salt,
                                        ))
                                    }
                                    // SASL: mechanism list (cstrings, empty terminator).
                                    10 => {
                                        let offered =
                                            body.split(|&b| b == 0).any(|m| m == b"SCRAM-SHA-256");
                                        if !offered {
                                            return Err(ProxyError::Auth(
                                                "backend offers no SCRAM-SHA-256 SASL mechanism"
                                                    .to_string(),
                                            ));
                                        }
                                        let (client, first) =
                                            crate::backend::auth::Scram::client_first(
                                                Self::random_nonce(),
                                            );
                                        scram = Some(client);
                                        Some(first.0)
                                    }
                                    // SASLContinue: server-first.
                                    11 => {
                                        let Some(client) = scram.as_mut() else {
                                            return Err(ProxyError::Auth(
                                                "SASLContinue before SASL start".to_string(),
                                            ));
                                        };
                                        Some(
                                            client
                                                .client_final(body, password)
                                                .map_err(|e| {
                                                    ProxyError::Auth(format!("SCRAM: {}", e))
                                                })?
                                                .0,
                                        )
                                    }
                                    // SASLFinal: verify the server signature.
                                    _ => {
                                        let Some(client) = scram.as_ref() else {
                                            return Err(ProxyError::Auth(
                                                "SASLFinal before SASL start".to_string(),
                                            ));
                                        };
                                        client.verify_server(body).map_err(|e| {
                                            ProxyError::Auth(format!("SCRAM: {}", e))
                                        })?;
                                        None
                                    }
                                }
                            }
                            other => {
                                return Err(ProxyError::Auth(format!(
                                    "unsupported backend authentication method {}",
                                    other
                                )));
                            }
                        };
                        if let Some(payload) = reply {
                            // PasswordMessage: 'p' + int32 len + payload.
                            let mut msg = Vec::with_capacity(payload.len() + 5);
                            msg.push(b'p');
                            msg.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
                            msg.extend_from_slice(&payload);
                            backend.write_all(&msg).await.map_err(|e| {
                                ProxyError::Network(format!("Backend auth write error: {}", e))
                            })?;
                        }
                    }
                    _ => forward.extend_from_slice(&frame),
                }
            }
        }
    }
}
