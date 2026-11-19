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
    ) -> (Message, PreQueryAction, bool) {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return (msg, PreQueryAction::Forward, false),
            };

            if msg.msg_type != MessageType::Query {
                return (msg, PreQueryAction::Forward, false);
            }

            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::PreQuery) {
                return (msg, PreQueryAction::Forward, false);
            }

            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return (msg, PreQueryAction::Forward, false),
            };

            let ctx = Self::build_query_context(&query_msg.query, session);

            match pm.execute_pre_query(&ctx) {
                PreQueryResult::Continue => (msg, PreQueryAction::Forward, false),
                PreQueryResult::Block(reason) => (msg, PreQueryAction::Block(reason), false),
                PreQueryResult::Rewrite(new_sql) => {
                    let rewritten = QueryMessage { query: new_sql }.encode();
                    (rewritten, PreQueryAction::Forward, true)
                }
                PreQueryResult::Cached(bytes) => (msg, PreQueryAction::Cached(bytes), false),
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (state, session);
            (msg, PreQueryAction::Forward, false)
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

    /// Answer a simple query from the pre-query plugin's decision: its Block
    /// (an error, then ReadyForQuery) or its cached result (never for a query
    /// that continues a cycle the backend holds open). `Ok(true)`: answered,
    /// it must not reach a backend.
    pub(super) async fn plugin_answered(
        action: &PreQueryAction,
        continues_cycle: bool,
        stream: &mut ClientStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> Result<bool> {
        let status = session
            .last_rfq_status
            .load(std::sync::atomic::Ordering::Relaxed);
        match action {
            PreQueryAction::Forward => {
                let _ = (continues_cycle, stream, state, status);
                Ok(false)
            }
            #[cfg(feature = "wasm-plugins")]
            PreQueryAction::Block(reason) => {
                tracing::info!(reason = %reason, "pre-query plugin blocked query");
                Self::send_block_response(stream, reason, status, state).await?;
                Ok(true)
            }
            #[cfg(feature = "wasm-plugins")]
            PreQueryAction::Cached(_) if continues_cycle => Ok(false),
            #[cfg(feature = "wasm-plugins")]
            PreQueryAction::Cached(bytes) => {
                match Self::synthesise_cached_response(bytes, status) {
                    Ok(reply) => {
                        stream
                            .write_all(&reply)
                            .await
                            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
                        state
                            .metrics
                            .bytes_sent
                            .fetch_add(reply.len() as u64, Ordering::Relaxed);
                        Ok(true)
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to synthesise cached response; falling back to backend");
                        Ok(false)
                    }
                }
            }
        }
    }

    /// Send the client a `Block`-outcome response: an error frame plus
    /// `ReadyForQuery` so the client's state machine is ready for its next
    /// query. `status` is the backend's real transaction status (the last
    /// one relayed): the block never reached the backend, so a transaction
    /// open there stays open, and the client must not be told 'I'.
    #[cfg(feature = "wasm-plugins")]
    pub(super) async fn send_block_response(
        stream: &mut ClientStream,
        reason: &str,
        status: u8,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        let err =
            Self::create_error_response("42000", &format!("Query blocked by plugin: {}", reason));
        stream
            .write_all(&err)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        let rfq = Self::create_ready_for_query(status);
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

    /// Build a `QueryContext` for a plugin hook call: `query` (verbatim),
    /// `normalized` (its fingerprint: literals replaced by `?`), `tables`
    /// (the relations it names, sorted, schema-qualified as written),
    /// `is_read_only` (from the SQL verb), and the session's `hook_context`
    /// ([`Self::plugin_session_context`]) with a request id of its own.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn build_query_context(query: &str, session: &Arc<ClientSession>) -> QueryContext {
        let is_read_only = !Self::is_write_query(query);
        // The session's context (identity, database, branch, attributes)
        // with a request id of this call's own.
        let mut hook_context =
            session
                .plugin_context
                .get()
                .cloned()
                .unwrap_or_else(|| HookContext {
                    client_id: Some(session.id.to_string()),
                    ..HookContext::default()
                });
        hook_context.request_id = uuid::Uuid::new_v4().to_string();
        let mut tables: Vec<String> =
            crate::sql_relations::table_names(&crate::sql_relations::RELATION_PATTERN, query)
                .map(|t| t.split_whitespace().collect::<String>())
                .collect();
        tables.sort();
        tables.dedup();
        QueryContext {
            query: query.to_string(),
            normalized: crate::sql_text::fingerprint(query).text,
            tables,
            is_read_only,
            hook_context,
        }
    }

    /// What a session's plugin hook calls carry in `hook_context`, from its
    /// startup parameters (`params`), the identity an Authenticate plugin
    /// gave it, and its tenant (`[multi_tenancy] identify_by`):
    ///
    /// - `client_id` (the session id) and `identity` (the plugin identity's
    ///   username, else the startup `user`);
    /// - `database` and `branch` (`<database>@<branch>` split);
    /// - `attributes`: `client_ip`, `user`, `application_name`, `tenant_id`
    ///   (the configured tenant, else the plugin identity's), the identity
    ///   as `identity.user_id` / `identity.username` / `identity.roles`
    ///   (comma-separated) / `identity.tenant_id`, and every `helios.*`
    ///   setting of the session: a startup parameter, or `-c helios.x=y` /
    ///   `--helios.x=y` in `options`. `helios.agent_id` and
    ///   `helios.model_id` are also given as `agent_id` and `model_id`.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn plugin_session_context(
        params: &HashMap<String, String>,
        identity: Option<&crate::plugins::Identity>,
        tenant: Option<&str>,
        client_ip: &str,
        session_id: &str,
    ) -> HookContext {
        let mut attributes = HashMap::new();
        attributes.insert("client_ip".to_string(), client_ip.to_string());
        for key in ["user", "application_name"] {
            if let Some(value) = params.get(key) {
                attributes.insert(key.to_string(), value.clone());
            }
        }
        for (key, value) in params.iter().filter(|(k, _)| k.starts_with("helios.")) {
            attributes.insert(key.clone(), value.clone());
        }
        if let Some(options) = params.get("options") {
            for (key, value) in Self::options_settings(options) {
                if key.starts_with("helios.") {
                    attributes.insert(key, value);
                }
            }
        }
        for (prefixed, plain) in [
            ("helios.agent_id", "agent_id"),
            ("helios.model_id", "model_id"),
        ] {
            if let Some(value) = attributes.get(prefixed).cloned() {
                attributes.insert(plain.to_string(), value);
            }
        }
        if let Some(id) = identity {
            attributes.insert("identity.user_id".to_string(), id.user_id.clone());
            attributes.insert("identity.username".to_string(), id.username.clone());
            attributes.insert("identity.roles".to_string(), id.roles.join(","));
            if let Some(t) = &id.tenant_id {
                attributes.insert("identity.tenant_id".to_string(), t.clone());
            }
        }
        let tenant_id = tenant
            .map(str::to_string)
            .or_else(|| identity.and_then(|id| id.tenant_id.clone()));
        if let Some(t) = tenant_id {
            attributes.insert("tenant_id".to_string(), t);
        }
        let user = params.get("user").cloned();
        let database = params.get("database").cloned().or_else(|| user.clone());
        let (database, branch) = match database.as_deref() {
            Some(db) => {
                let (base, branch) = crate::database_name::split_branch(db);
                (Some(base.to_string()), branch.map(str::to_string))
            }
            None => (None, None),
        };
        HookContext {
            request_id: String::new(),
            client_id: Some(session_id.to_string()),
            identity: identity
                .map(|id| id.username.clone())
                .filter(|name| !name.is_empty())
                .or(user),
            database,
            branch,
            attributes,
        }
    }

    /// The `name=value` settings of a startup `options` string, as
    /// PostgreSQL reads it: words separated by unescaped whitespace (a
    /// backslash escapes the next character), each `-c name=value`,
    /// `-cname=value` or `--name=value`.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn options_settings(options: &str) -> Vec<(String, String)> {
        let mut words = Vec::new();
        let mut word = String::new();
        let mut chars = options.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => {
                    if let Some(next) = chars.next() {
                        word.push(next);
                    }
                }
                c if c.is_whitespace() => {
                    if !word.is_empty() {
                        words.push(std::mem::take(&mut word));
                    }
                }
                c => word.push(c),
            }
        }
        if !word.is_empty() {
            words.push(word);
        }
        let mut settings = Vec::new();
        let mut iter = words.into_iter();
        while let Some(w) = iter.next() {
            let setting = if w == "-c" {
                iter.next()
            } else if let Some(rest) = w.strip_prefix("--") {
                Some(rest.to_string())
            } else {
                w.strip_prefix("-c").map(str::to_string)
            };
            if let Some((name, value)) = setting.as_deref().and_then(|s| s.split_once('=')) {
                settings.push((name.to_string(), value.to_string()));
            }
        }
        settings
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
            Self::route_result_override(pm.execute_route(&ctx))
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (msg, state, session);
            RouteOverride::None
        }
    }

    /// A route plugin's answer as the override the relay applies, on either
    /// protocol.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn route_result_override(result: RouteResult) -> RouteOverride {
        match result {
            RouteResult::Default => RouteOverride::None,
            RouteResult::Primary => RouteOverride::Primary,
            RouteResult::Standby => RouteOverride::Standby,
            RouteResult::Node(name) => RouteOverride::Node(name),
            RouteResult::Block(reason) => RouteOverride::Block(reason),
            // The session moves to the branch before the statement runs
            // (`switch_branch`), or the statement is refused: it never runs
            // on the database the plugin did not mean.
            RouteResult::Branch(name) => RouteOverride::Branch(name),
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
    /// The fourth element is the hint's `branch=` request, if any.
    #[cfg(feature = "routing-hints")]
    pub(super) fn resolve_simple_route(
        msg: &Message,
        plugin_override: RouteOverride,
        default_is_write: bool,
        state: &Arc<ServerState>,
    ) -> (RouteOverride, bool, Option<Message>, Option<String>) {
        let parser = match state.hint_parser.as_ref() {
            Some(p) => p,
            None => return (plugin_override, default_is_write, None, None),
        };
        let sql = match crate::protocol::query_text(&msg.payload) {
            Some(s) => s,
            None => return (plugin_override, default_is_write, None, None),
        };
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return (plugin_override, default_is_write, None, None);
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        let effective = match Self::hint_to_override(&hints) {
            RouteOverride::None => plugin_override,
            hint_override => hint_override,
        };
        let forward = if parser.strip_hints {
            Some(
                crate::protocol::QueryMessage {
                    query: stripped.into_owned(),
                }
                .encode(),
            )
        } else {
            None
        };
        (effective, is_write, forward, hints.branch)
    }

    /// Resolve hint-driven routing for an extended-protocol batch from the
    /// first Parse's SQL. `Some((is_write, forced_node, standby_hint))` when
    /// hints are present (write flag computed on the stripped SQL), else
    /// `None` so the caller uses the route plugin's override (`plugin`) or
    /// the verb-based defaults. A hint that names no route leaves the
    /// plugin's override in force, as on the simple path
    /// (`resolve_simple_route`). The hint comment is left in the forwarded
    /// `Parse` (a no-op SQL comment); rewriting the batch buffer is
    /// unnecessary for correctness.
    /// The second element is the hint's `branch=` request, if any.
    #[cfg(feature = "routing-hints")]
    pub(super) fn extended_hint_route(
        state: &Arc<ServerState>,
        sql: &str,
        plugin: &RouteOverride,
    ) -> Option<(super::ext_hooks::BatchRoute, Option<String>)> {
        let parser = state.hint_parser.as_ref()?;
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return None;
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        let route = match Self::hint_to_override(&hints) {
            RouteOverride::None => Self::route_by_override(plugin, is_write),
            hinted => Self::route_by_override(&hinted, is_write),
        };
        Some((route, hints.branch))
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
        Self::fire_post_query_sql(pm, &query_msg.query, session, result, elapsed);
    }

    /// Fire the post-query hooks for `sql`: a simple Query, or the first
    /// statement of an extended-protocol cycle its Sync ended.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn fire_post_query_sql(
        pm: &PluginManager,
        sql: &str,
        session: &Arc<ClientSession>,
        result: &Result<(Option<String>, u64)>,
        elapsed: Duration,
    ) {
        let ctx = Self::build_query_context(sql, session);
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
