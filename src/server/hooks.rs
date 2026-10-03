use super::*;

impl ProxyServer {
    /// Run the pre-query plugin hook on a client message.
    ///
    /// When the `wasm-plugins` feature is off, or the plugin manager has no
    /// loaded plugins, this is a zero-cost passthrough that returns the
    /// message untouched with `PreQueryAction::Forward`.
    ///
    /// Only simple-query (`MessageType::Query`) messages are inspected today.
    /// Extended-protocol messages (`Parse`/`Bind`/`Execute`) are passed
    /// through unchanged — a future task wires them in.
    pub(super) fn apply_pre_query_hook(
        msg: Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) -> (Message, PreQueryAction) {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return (msg, PreQueryAction::Forward),
            };

            if msg.msg_type != MessageType::Query {
                return (msg, PreQueryAction::Forward);
            }

            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::PreQuery) {
                return (msg, PreQueryAction::Forward);
            }

            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return (msg, PreQueryAction::Forward),
            };

            let ctx = Self::build_query_context(&query_msg.query, session);

            match pm.execute_pre_query(&ctx) {
                PreQueryResult::Continue => (msg, PreQueryAction::Forward),
                PreQueryResult::Block(reason) => (msg, PreQueryAction::Block(reason)),
                PreQueryResult::Rewrite(new_sql) => {
                    let rewritten = QueryMessage { query: new_sql }.encode();
                    (rewritten, PreQueryAction::Forward)
                }
                PreQueryResult::Cached(bytes) => (msg, PreQueryAction::Cached(bytes)),
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (state, session);
            (msg, PreQueryAction::Forward)
        }
    }

    /// Feed the anomaly detector a per-query observation. Cheap —
    /// only the SQL-injection scan and the novel-fingerprint check
    /// are non-trivial, both well under a microsecond on
    /// representative queries. Returns nothing; detections land in
    /// the detector's ring buffer and are surfaced via /api/anomalies.
    #[cfg(feature = "anomaly-detection")]
    pub(super) fn record_anomaly_observation(
        msg: &Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        if msg.msg_type != MessageType::Query {
            return;
        }
        // Borrow the SQL straight out of the payload — the message is
        // forwarded verbatim, so no deep copy of the frame is needed.
        if let Some(query) = crate::protocol::query_text(&msg.payload) {
            Self::record_anomaly_sql(query, state, session);
        }
    }

    /// Feed one SQL statement to the anomaly detector. Shared by the
    /// simple-query path and the extended-protocol `Parse` path so
    /// prepared-statement traffic is observed too.
    #[cfg(feature = "anomaly-detection")]
    pub(super) fn record_anomaly_sql(
        query: &str,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        // Tenant identifier is the most-specific known per-session
        // attribute the proxy can attribute traffic to. Multi-tenancy
        // sets `tenant_id` in `variables`; otherwise we fall back to
        // the client address. session.variables is a tokio RwLock but this
        // is a sync helper — try_read avoids an await; on contention we
        // fall back to the client IP, still a valid per-source identifier.
        let tenant = match session.variables.try_read() {
            Ok(vars) => vars
                .get("tenant_id")
                .or_else(|| vars.get("user"))
                .cloned()
                .unwrap_or_else(|| session.client_addr.ip().to_string()),
            Err(_) => session.client_addr.ip().to_string(),
        };
        // Both the fingerprint and the SQL are *lent* to the detector:
        // the fingerprint from a buffer sized for this call, the SQL
        // straight from the wire frame. The detector copies only on
        // the rare paths that retain something (first-seen
        // fingerprint, emitted event excerpt). The fingerprint buffer
        // is allocated per call rather than cached in a thread-local,
        // so nothing is retained between queries — a client sending
        // one very large statement does not leave its buffer
        // permanently resident on the worker thread.
        let mut fingerprint = String::with_capacity(query.len());
        anomaly_fingerprint_into(query, &mut fingerprint);
        let obs = crate::anomaly::QueryObservation {
            tenant,
            fingerprint: std::borrow::Cow::Borrowed(fingerprint.as_str()),
            sql: std::borrow::Cow::Borrowed(query),
            timestamp: std::time::Instant::now(),
        };
        for ev in state.anomaly_detector.record_query(&obs) {
            tracing::warn!(anomaly = ?ev, "anomaly detected");
        }
    }

    /// Send the client a `Block`-outcome response: an error frame plus
    /// `ReadyForQuery` so the client's state machine returns to idle and
    /// the next query can be accepted.
    pub(super) async fn send_block_response(
        stream: &mut ClientStream,
        reason: &str,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        let err =
            Self::create_error_response("42000", &format!("Query blocked by plugin: {}", reason));
        stream
            .write_all(&err)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        let rfq = Self::create_ready_for_query(b'I');
        stream
            .write_all(&rfq)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        state
            .metrics
            .bytes_sent
            .fetch_add((err.len() + rfq.len()) as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Build a `QueryContext` for the plugin hook. Populated fields: `query`
    /// (verbatim), `is_read_only` (derived from SQL verb), and `hook_context`
    /// with the session id as `client_id`. `normalized` and `tables` are
    /// left as cheap stand-ins until the analytics normaliser is wired in
    /// (T0-d, unified context).
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn build_query_context(query: &str, session: &Arc<ClientSession>) -> QueryContext {
        let is_read_only = !Self::is_write_query(query);
        let hook_context = HookContext {
            client_id: Some(session.id.to_string()),
            ..HookContext::default()
        };
        QueryContext {
            query: query.to_string(),
            normalized: query.to_string(),
            tables: Vec::new(),
            is_read_only,
            hook_context,
        }
    }

    /// Run the Authenticate plugin hook at startup. Called from
    /// `connect_and_authenticate` before any backend connection.
    ///
    /// Behaviour by `AuthResult`:
    /// * `Defer` — no plugin opinion; proceed with the default
    ///   PostgreSQL auth flow unchanged.
    /// * `Success(identity)` — store the identity on the session so
    ///   downstream plugins (masking, residency) can gate on roles /
    ///   tenant_id / claims. PostgreSQL backend auth still runs
    ///   normally afterwards (the plugin does not replace PG auth in
    ///   this iteration; that's a follow-up).
    /// * `Denied(reason)` — surfaces as `ProxyError::Auth`, which the
    ///   caller already handles by writing an ErrorResponse to the
    ///   client and closing the connection.
    ///
    /// The `AuthRequest` populated here carries username, database,
    /// and client IP from the PostgreSQL startup parameters. Password
    /// is deliberately `None` — PG protocol sends the password in
    /// response to the backend's challenge, not at startup, so
    /// password-aware plugin auth is a separate future task.
    pub(super) async fn apply_authenticate_hook(
        _params: &HashMap<String, String>,
        _session: &Arc<ClientSession>,
        _state: &Arc<ServerState>,
    ) -> Result<()> {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match _state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return Ok(()),
            };

            let request = PluginAuthRequest {
                headers: HashMap::new(),
                username: _params.get("user").cloned(),
                password: None,
                client_ip: _session.client_addr.ip().to_string(),
                database: _params.get("database").cloned(),
            };

            match pm.execute_authenticate(&request) {
                AuthResult::Defer => Ok(()),
                AuthResult::Success(identity) => {
                    tracing::debug!(
                        user = %identity.username,
                        roles = ?identity.roles,
                        "plugin authenticated user"
                    );
                    *_session.plugin_identity.write().await = Some(identity);
                    Ok(())
                }
                AuthResult::Denied(reason) => {
                    tracing::info!(
                        reason = %reason,
                        client = %_session.client_addr,
                        user = ?_params.get("user"),
                        "plugin denied authentication"
                    );
                    Err(ProxyError::Auth(format!(
                        "authentication denied by plugin: {}",
                        reason
                    )))
                }
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            Ok(())
        }
    }

    /// Run the Route plugin hook on a message. Only simple-query messages
    /// are inspected; other message types always return `None`.
    pub(super) fn apply_route_hook(
        msg: &Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) -> RouteOverride {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return RouteOverride::None,
            };
            if msg.msg_type != MessageType::Query {
                return RouteOverride::None;
            }
            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::Route) {
                return RouteOverride::None;
            }
            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return RouteOverride::None,
            };
            let ctx = Self::build_query_context(&query_msg.query, session);
            match pm.execute_route(&ctx) {
                RouteResult::Default => RouteOverride::None,
                RouteResult::Primary => RouteOverride::Primary,
                RouteResult::Standby => RouteOverride::Standby,
                RouteResult::Node(name) => RouteOverride::Node(name),
                RouteResult::Block(reason) => RouteOverride::Block(reason),
                RouteResult::Branch(name) => {
                    tracing::warn!(
                        branch = %name,
                        "Route hook returned Branch but branch routing is not yet wired — using default"
                    );
                    RouteOverride::None
                }
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (msg, state, session);
            RouteOverride::None
        }
    }

    /// Map parsed SQL-comment hints to a `RouteOverride`. Precedence:
    /// `node=` > `route=` > `consistency=strong`. Read-tier route targets
    /// (standby/sync/semisync/async/local) all map to the read path; `any`
    /// and `vector` impose no constraint. `lag=` / `consistency=bounded`
    /// freshness enforcement arrives with the lag-routing feature.
    #[cfg(feature = "routing-hints")]
    pub(super) fn hint_to_override(hints: &crate::routing::ParsedHints) -> RouteOverride {
        use crate::routing::{ConsistencyLevel, RouteTarget};
        if let Some(node) = &hints.node {
            return RouteOverride::Node(node.clone());
        }
        if let Some(route) = hints.route {
            return match route {
                RouteTarget::Primary => RouteOverride::Primary,
                RouteTarget::Standby
                | RouteTarget::Sync
                | RouteTarget::SemiSync
                | RouteTarget::Async
                | RouteTarget::Local => RouteOverride::Standby,
                RouteTarget::Any | RouteTarget::Vector => RouteOverride::None,
            };
        }
        if hints.consistency == Some(ConsistencyLevel::Strong) {
            return RouteOverride::Primary;
        }
        RouteOverride::None
    }

    /// Resolve the effective routing for a simple `Query` when the
    /// routing-hints feature is active. Returns `(override, is_write,
    /// forward_msg)`: the write flag is recomputed on the hint-stripped SQL so
    /// a leading hint comment never masks the verb, and `forward_msg` is a
    /// rebuilt `Query` (hint removed) when stripping is on. An explicit
    /// positional hint wins over a plugin route override; a plugin `Block` is
    /// handled by the caller before this runs.
    #[cfg(feature = "routing-hints")]
    pub(super) fn resolve_simple_route(
        msg: &Message,
        plugin_override: RouteOverride,
        default_is_write: bool,
        state: &Arc<ServerState>,
    ) -> (RouteOverride, bool, Option<Message>) {
        let parser = match state.hint_parser.as_ref() {
            Some(p) => p,
            None => return (plugin_override, default_is_write, None),
        };
        let sql = match crate::protocol::query_text(&msg.payload) {
            Some(s) => s,
            None => return (plugin_override, default_is_write, None),
        };
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return (plugin_override, default_is_write, None);
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        let effective = match Self::hint_to_override(&hints) {
            RouteOverride::None => plugin_override,
            hint_override => hint_override,
        };
        let forward = if parser.strip_hints {
            Some(crate::protocol::QueryMessage { query: stripped }.encode())
        } else {
            None
        };
        (effective, is_write, forward)
    }

    /// Resolve hint-driven routing for an extended-protocol batch from the
    /// first Parse's SQL. `Some((is_write, forced_node))` when hints are
    /// present (write flag computed on the stripped SQL), else `None` so the
    /// caller uses verb-based defaults. The hint comment is left in the
    /// forwarded `Parse` (a no-op SQL comment); rewriting the batch buffer is
    /// unnecessary for correctness.
    #[cfg(feature = "routing-hints")]
    pub(super) fn extended_hint_route(
        state: &Arc<ServerState>,
        sql: &str,
    ) -> Option<(bool, Option<String>)> {
        let parser = state.hint_parser.as_ref()?;
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return None;
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        match Self::hint_to_override(&hints) {
            RouteOverride::Primary => Some((true, None)),
            RouteOverride::Standby => Some((false, None)),
            RouteOverride::Node(n) => Some((is_write, Some(n))),
            _ => Some((is_write, None)),
        }
    }

    /// Fire post-query hooks after a message has been forwarded (or failed
    /// to forward). Best-effort; errors from individual plugins are logged
    /// by the plugin manager and never surface here.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn fire_post_query_hook(
        msg: &Message,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        result: &Result<(Option<String>, u64)>,
        elapsed: Duration,
    ) {
        let pm = match state.plugin_manager.as_ref() {
            Some(pm) => pm,
            None => return,
        };
        if msg.msg_type != MessageType::Query {
            return;
        }
        // Zero plugins registered for this hook — skip the payload
        // clone, SQL parse, and context construction entirely.
        if !pm.has_hook(HookType::PostQuery) {
            return;
        }
        let query_msg = match QueryMessage::parse(msg.payload.clone()) {
            Ok(q) => q,
            Err(_) => return,
        };
        let ctx = Self::build_query_context(&query_msg.query, session);
        let outcome = match result {
            Ok((node, bytes)) => PostQueryOutcome {
                success: true,
                target_node: node.clone(),
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: *bytes,
                error: None,
            },
            Err(e) => PostQueryOutcome {
                success: false,
                target_node: None,
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: 0,
                error: Some(e.to_string()),
            },
        };
        pm.execute_post_query(&ctx, &outcome);
    }
}
