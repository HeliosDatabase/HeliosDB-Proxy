use super::*;

impl ProxyServer {
    /// Forward a simple-query (`Query`) message and stream its response back
    /// to the client frame-by-frame, ending at ReadyForQuery. Picks (and, if
    /// needed, opens) the target node's connection from the per-session
    /// cache. Returns `(Some(node_used), bytes)` — `None` node means the
    /// request was short-circuited (plugin block) without touching a backend.
    ///
    /// On a backend fault the broken connection is dropped, the node is
    /// demoted, `fault` is filled with the failed node and whether the
    /// statement had already been delivered, and `Err` is returned so the
    /// caller can run the `tr_mode` recovery. A client-side error leaves
    /// `fault` untouched.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn forward_simple_query(
        client: &mut ClientStream,
        msg: &Message,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        fault: &mut Option<BackendFault>,
    ) -> Result<(Option<String>, u64)> {
        // Rate-limit gate: deny before any backend selection.
        #[cfg(feature = "rate-limiting")]
        if let Some(mut resp) = Self::rate_limit_check(session, state, config).await {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // Lazily-memoized lexical classification: every cheap fact this
        // forward path may need about the statement (write? session-dirtying?
        // cacheable read? multi-statement?) is derived AT MOST ONCE here,
        // instead of each gate re-walking the same SQL string — and only if
        // the gate that needs it is actually enabled, so the default
        // configuration keeps paying for exactly one classification (the write
        // check below, which every route decision needs). Rebuilt further down
        // only if a hint strip / rewrite / tenant transform replaces the text.
        let mut facts = StmtFacts::of_query(msg);
        let default_is_write = facts.is_write();
        let plugin_override = Self::apply_route_hook(msg, state, session);

        // Block short-circuits before any backend selection.
        if let RouteOverride::Block(reason) = plugin_override {
            let mut response = Vec::with_capacity(64 + reason.len());
            response.extend_from_slice(&Self::create_error_response(
                "42000",
                &format!("Query blocked by route plugin: {}", reason),
            ));
            response.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&response)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, response.len() as u64));
        }

        // SQL-comment routing hints (feature + `[routing_hints] enabled`)
        // refine the override, recompute the write flag on the stripped SQL,
        // and may rewrite the message to drop the hint comment.
        #[cfg(feature = "routing-hints")]
        let (route_override, default_is_write, stripped_msg) =
            Self::resolve_simple_route(msg, plugin_override, default_is_write, state);
        #[cfg(not(feature = "routing-hints"))]
        let (route_override, stripped_msg): (RouteOverride, Option<Message>) =
            (plugin_override, None);

        let (is_write, forced_target) = match route_override {
            RouteOverride::None => (default_is_write, None),
            RouteOverride::Primary => (true, None),
            RouteOverride::Standby => (false, None),
            RouteOverride::Node(name) => (default_is_write, Some(name)),
            RouteOverride::Block(_) => unreachable!("handled above"),
        };

        // Read-your-writes: stamp the session on a write so subsequent reads
        // pin to the primary for the configured window.
        #[cfg(feature = "lag-routing")]
        if is_write && config.lag_routing.enabled {
            *session.last_write_at.write().await = Some(std::time::Instant::now());
        }

        // Forward the stripped message when routing-hints rewrote it, else the
        // original (borrowed, no copy).
        let forward_msg = stripped_msg.as_ref().unwrap_or(msg);

        // Query rewriting: apply rules to the SQL; if any rule fired, forward a
        // rebuilt Query carrying the rewritten SQL (so caching + the backend
        // both see the rewritten form).
        #[cfg(feature = "query-rewriting")]
        let rewritten_msg: Option<Message> = state.rewriter.as_ref().and_then(|rw| {
            let sql = crate::protocol::query_text(&forward_msg.payload)?;
            match rw.rewrite(sql) {
                Ok(res) if res.was_rewritten() => {
                    tracing::debug!(target: "helios::rewrite", rules = ?res.rules_applied, "query rewritten");
                    Some(crate::protocol::QueryMessage { query: res.query().to_string() }.encode())
                }
                _ => None,
            }
        });
        #[cfg(feature = "query-rewriting")]
        if rewritten_msg.is_some() {
            // The recorded client text is not what executes — the enclosing
            // transaction must not be blindly replayed after a failover.
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "query-rewriting")]
        let forward_msg = rewritten_msg.as_ref().unwrap_or(forward_msg);

        // Multi-tenancy: resolve the session's tenant and inject a row-level
        // tenant filter. Done BEFORE the cache lookup so each tenant's results
        // are cached under their own (filtered) SQL — no cross-tenant leakage.
        #[cfg(feature = "multi-tenancy")]
        let tenant_msg: Option<Message> = if let Some(tm) = state.tenant_manager.as_ref() {
            match crate::protocol::query_text(&forward_msg.payload) {
                Some(sql) => {
                    let ctx = Self::tenant_request_ctx(session).await;
                    match tm.identify_tenant(&ctx) {
                        Some(tenant) => {
                            let res = tm.transform_query(sql, &tenant);
                            if res.transformed {
                                tracing::debug!(target: "helios::tenant", tenant = %tenant.0, "tenant filter injected");
                                Some(crate::protocol::QueryMessage { query: res.query }.encode())
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                }
                None => None,
            }
        } else {
            None
        };
        #[cfg(feature = "multi-tenancy")]
        if tenant_msg.is_some() {
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "multi-tenancy")]
        let forward_msg = tenant_msg.as_ref().unwrap_or(forward_msg);

        // `forward_msg` is now final. The hint strip / rewrite / tenant
        // transforms above may each have replaced the SQL, and facts only
        // describe the text they were derived from — so rebuild them (which
        // clears every memo cell) when any of them fired. An untouched query
        // (the common path) keeps the value built from the original message,
        // and rebuilding classifies nothing by itself. The block is gated on
        // the features that read facts below, so a build with none of them
        // carries no dead assignment.
        #[cfg(any(
            feature = "pool-modes",
            feature = "query-cache",
            feature = "edge-proxy"
        ))]
        {
            let sql_changed = stripped_msg.is_some();
            #[cfg(feature = "query-rewriting")]
            let sql_changed = sql_changed || rewritten_msg.is_some();
            #[cfg(feature = "multi-tenancy")]
            let sql_changed = sql_changed || tenant_msg.is_some();
            if sql_changed {
                facts = StmtFacts::of_query(forward_msg);
            }
        }

        // Edge cache: independent of the query-cache below — when both are
        // enabled the edge lookup runs first, so an edge hit returns before
        // the query-cache is consulted. On a miss the read's stamp/epoch is
        // taken BEFORE forwarding, so the store gate can reject the store if
        // an invalidation lands while the read is in flight.
        //
        // Session-state stickiness (F2): the shared cache cannot model
        // session-local execution context (SET search_path / SET ROLE / RLS
        // GUCs / temp objects). Any statement that leaves such state makes
        // the session permanently ineligible for edge lookup AND store —
        // conservative, but the alternative is cross-session wrong rows.
        #[cfg(feature = "edge-proxy")]
        if config.edge.enabled
            && !session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
            && facts.leaves_session_state()
        {
            session
                .edge_ineligible
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "edge-proxy")]
        let edge_ctx: Option<(crate::edge::CacheKey, Vec<String>, u64, u64)> = if is_write
            || !config.edge.enabled
            || session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            None
        } else {
            let sql = facts.sql();
            // Same gate as the query-cache: a plain deterministic SELECT, and
            // never mid-transaction (visibility would be wrong).
            if facts.is_cacheable_read()
                && !session
                    .in_transaction
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                // Tenant identity + result-affecting startup params
                // (TimeZone, options=-c..., extra_float_digits, ...) all
                // partition the key: identical SQL under a different
                // session environment must not share a slot.
                let (database, user, session_vars) = {
                    let vars = session.variables.read().await;
                    let database = vars
                        .get("database")
                        .cloned()
                        .unwrap_or_else(|| "default".to_string());
                    let user = vars.get("user").cloned().unwrap_or_default();
                    let mut rest: Vec<(String, String)> = vars
                        .iter()
                        .filter(|(k, _)| k.as_str() != "database" && k.as_str() != "user")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    rest.sort();
                    (database, user, rest)
                };
                let fp = crate::edge::fingerprint::analyze(sql, &database, &user, &session_vars);
                // A read with no extractable tables could never be dropped by
                // a table-targeted invalidation — never cache it (coherence
                // rule).
                if fp.tables.is_empty() {
                    None
                } else {
                    let key = crate::edge::CacheKey {
                        fingerprint: fp.fingerprint,
                        params_hash: fp.params_hash,
                        database,
                        user,
                    };
                    if let Some(entry) = state.edge_cache.get(&key) {
                        tracing::debug!(target: "helios::edge", "edge cache hit");
                        client.write_all(&entry.response_bytes).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, entry.response_bytes.len() as u64));
                    }
                    // Miss: stamp the read before it reaches a backend, in
                    // the INVALIDATION clock's domain. Home role: mint from
                    // the local counter (the same clock that versions
                    // writes). Edge role: writes are versioned by the HOME,
                    // so stamp with the last observed home version — any
                    // later home write mints a strictly greater version and
                    // always sweeps this entry; the store race is closed by
                    // the invalidation-epoch snapshot instead of the hwm.
                    let (read_version, inval_epoch) =
                        if config.edge.role == crate::edge::EdgeRole::Edge {
                            (
                                state.edge_cache.observed_home_version(),
                                state.edge_cache.invalidation_epoch(),
                            )
                        } else {
                            (state.edge_cache.next_version(), 0)
                        };
                    Some((key, fp.tables, read_version, inval_epoch))
                }
            } else {
                None
            }
        };

        // Query cache: on a cacheable read, a hit is served from cache with no
        // backend round-trip; on a miss we keep the context to store the result.
        // The lookup's hint parse + normalization are carried over to the
        // store below (`put_prepared`), so a miss+store normalizes the SQL
        // once instead of twice.
        #[cfg(feature = "query-cache")]
        let cache_ctx: Option<(crate::cache::CacheContext, crate::cache::QueryPrep)> = if is_write {
            None
        } else if let Some(qc) = state.query_cache.as_ref() {
            let sql = facts.sql();
            match Self::cacheable_read_ctx(session, facts.is_cacheable_read()).await {
                Some(ctx) => {
                    let (lookup, prep) = qc.get_with_prep(sql, &ctx).await;
                    if let crate::cache::CacheLookup::Hit { result, level } = lookup {
                        tracing::debug!(target: "helios::cache", level = %level, "cache hit");
                        client.write_all(&result.data).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, result.data.len() as u64));
                    }
                    Some((ctx, prep))
                }
                None => None,
            }
        } else {
            None
        };

        // Schema/workload routing: pin an analytical (OLAP) read to the
        // configured analytics node, unless something already forced a target.
        #[cfg(feature = "schema-routing")]
        let forced_target = match state.schema_analyzer.as_ref() {
            Some(analyzer)
                if forced_target.is_none()
                    && !is_write
                    && !config.schema_routing.analytics_node.is_empty() =>
            {
                match crate::protocol::query_text(&forward_msg.payload) {
                    Some(sql) if analyzer.analyze(sql).is_analytics() => {
                        tracing::debug!(target: "helios::schema", "OLAP query routed to analytics node");
                        Some(config.schema_routing.analytics_node.clone())
                    }
                    _ => forced_target,
                }
            }
            _ => forced_target,
        };

        // Analytics: capture the forwarded SQL + start the latency timer.
        #[cfg(feature = "query-analytics")]
        let analytics_sql =
            crate::protocol::query_text(&forward_msg.payload).map(|s| s.to_string());
        #[cfg(feature = "query-analytics")]
        let started = std::time::Instant::now();

        let target = Self::choose_target_node(
            is_write,
            forced_target,
            current_node,
            session,
            state,
            config,
        )
        .await?;
        tracing::debug!(target: "helios::routing", node = %target, is_write, "routed simple query");

        // Circuit breaker: fast-fail when the chosen node's circuit is open.
        #[cfg(feature = "circuit-breaker")]
        if let Some(mut resp) = Self::circuit_fast_fail(state, &target) {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // A connect/auth failure trips the breaker (and is propagated as today).
        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Conditional-reset bookkeeping: if this statement is not provably
        // session-neutral, mark the connection dirty so it is fully reset (not
        // clean-skipped) when parked. Still evaluated only when the
        // optimisation is enabled and the connection is not already dirty (one
        // O(len) scan at most, until the first dirtying statement) — the facts
        // memo just means the edge gate above, when it is also on, shares that
        // one scan instead of doing its own.
        #[cfg(feature = "pool-modes")]
        if config.pool_mode.skip_clean_reset && !backend.dirty && facts.leaves_session_state() {
            backend.dirty = true;
        }

        let backend_err = match tokio::time::timeout(
            state.limits.backend_write_timeout,
            backend.stream.write_all(&forward_msg.encode()),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("Backend write error: {}", e)),
            Err(_) => Some("Backend write timeout".to_string()),
        };
        if let Some(msg) = backend_err {
            let e = ProxyError::Network(msg);
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }

        // Cacheable read miss (query-cache and/or edge cache): capture the
        // response frames ONCE and store them so a later identical read is
        // served from cache without a backend hit. Both caches share the one
        // captured buffer — never capture twice.
        // TR-07: register the statement the backend is now executing so the
        // relay collects its outcome (writes, COPY and transaction control
        // only — reads register nothing and arm nothing).
        if config.tr_enabled || Self::cache_needs_capture(state) {
            if let Some(sql) = crate::protocol::query_text(&forward_msg.payload) {
                Self::journal_register_simple(session, sql);
            }
        }

        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            #[cfg(feature = "query-cache")]
            let qc_wants = cache_ctx.is_some() && state.query_cache.is_some();
            #[cfg(not(feature = "query-cache"))]
            let qc_wants = false;
            #[cfg(feature = "edge-proxy")]
            let edge_wants = edge_ctx.is_some();
            #[cfg(not(feature = "edge-proxy"))]
            let edge_wants = false;
            if qc_wants || edge_wants {
                return match Self::stream_until_ready_capture(
                    client,
                    &mut backend.stream,
                    session,
                    state.limits.relay(),
                    config.cache.max_cacheable_response_bytes,
                    &state.metrics,
                )
                .await
                {
                    Ok((sent, captured, cacheable, rows)) => {
                        #[cfg(not(feature = "query-cache"))]
                        let _ = rows; // row count only feeds the query-cache
                        #[cfg(feature = "circuit-breaker")]
                        Self::circuit_record(state, &target, true, "");
                        // One zero-copy conversion; both caches then share the
                        // refcounted buffer (no per-miss memcpy, F20).
                        let body = bytes::Bytes::from(captured);
                        // Edge store. The race-checked insert variants re-verify
                        // the gate UNDER the map lock: a concurrent invalidation
                        // that completed between the read and this store must
                        // reject it (read-after-invalidate TOCTOU, F18). Home
                        // role gates on the version hwm; edge role on the
                        // invalidation-epoch snapshot taken before forwarding.
                        #[cfg(feature = "edge-proxy")]
                        if let Some((key, tables, read_version, inval_epoch)) = edge_ctx {
                            if cacheable && !body.is_empty() {
                                let entry = crate::edge::CacheEntry {
                                    version: read_version,
                                    response_bytes: body.clone(),
                                    tables,
                                    expires_at: std::time::Instant::now()
                                        + config.edge.default_ttl(),
                                };
                                let stored = if config.edge.role == crate::edge::EdgeRole::Edge {
                                    state.edge_cache.insert_if_epoch(key, entry, inval_epoch)
                                } else {
                                    state.edge_cache.insert_if_fresh(key, entry)
                                };
                                if !stored {
                                    tracing::debug!(
                                        target: "helios::edge",
                                        "edge store skipped — invalidation raced the read"
                                    );
                                }
                            }
                        }
                        #[cfg(feature = "query-cache")]
                        if let (Some((ctx, prep)), Some(qc)) =
                            (cache_ctx.as_ref(), state.query_cache.as_ref())
                        {
                            if cacheable && !body.is_empty() {
                                let sql =
                                    crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
                                qc.put_prepared(
                                    sql,
                                    ctx,
                                    prep,
                                    body.clone(),
                                    rows,
                                    std::time::Duration::ZERO,
                                )
                                .await;
                            }
                        }
                        let _ = body;
                        // TR-07: the capture relay has no journal handle; reconcile
                        // the transaction state from the status it recorded.
                        Self::journal_observe_status(session, state).await;
                        #[cfg(feature = "query-analytics")]
                        if let Some(sql) = analytics_sql.as_deref() {
                            Self::record_analytics(
                                state,
                                session,
                                sql,
                                &target,
                                started.elapsed(),
                                None,
                            )
                            .await;
                        }
                        Ok((Some(target), sent))
                    }
                    Err(e) => {
                        conns.remove(&target);
                        Self::record_backend_failure(state, &target, &e.to_string());
                        BackendFault::set_response(fault, &target, &e);
                        Err(e.error)
                    }
                };
            }
        }

        match Self::stream_until_ready(client, &mut backend.stream, session, state).await {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                // Query-cache invalidation for this write (C-02) already ran
                // from the journal capture's ops in `stream_until_ready`, on
                // both protocols, before the ReadyForQuery reached the client.
                // Edge cache: drop local entries for the touched tables and
                // (home role only) fan the invalidation out to every
                // registered edge over SSE. Also fires for SELECT-leading
                // multi-statement strings (the trailing write would otherwise
                // invalidate nothing); bare transaction-control statements
                // (BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK) change no rows and
                // are exempted — but COMMIT and SET keep their conservative
                // full flush. A `COPY ... FROM` is deferred to its CopyDone
                // drain (rows become visible then).
                #[cfg(feature = "edge-proxy")]
                if config.edge.enabled {
                    let sql = facts.sql();
                    if Self::edge_write_needs_invalidation(
                        is_write,
                        sql,
                        facts.has_interior_semicolon(),
                    ) {
                        // `tables_only` skips the fingerprint/params-hash work
                        // (discarded here) — a bulk INSERT must not pay full-
                        // buffer regex rewrites on the forward path. An EMPTY
                        // table set invalidates everything at or below the
                        // version — an unparseable write must not leave stale
                        // entries behind (coherence rule).
                        let tables = crate::edge::fingerprint::tables_only(sql);
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
                }
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(state, session, sql, &target, started.elapsed(), None)
                        .await;
                }
                Ok((Some(target), sent))
            }
            Err(e) => {
                // Drop the broken connection so the next use redials.
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                BackendFault::set_response(fault, &target, &e);
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(
                        state,
                        session,
                        sql,
                        &target,
                        started.elapsed(),
                        Some(e.to_string()),
                    )
                    .await;
                }
                Err(e.error)
            }
        }
    }

    /// Forward an accumulated extended-protocol batch (Parse/Bind/Describe/
    /// Execute/Close terminated by Sync or Flush) and stream the response.
    /// Routing is taken from `route_sql` (the first Parse's SQL); when it is
    /// `None` (a re-Bind/Execute of a named prepared statement) the request
    /// stays on the connection the statement was prepared on — no switch.
    ///
    /// `reprepare` lists named statements this batch references but does not
    /// itself define; any that the chosen connection has not seen are
    /// re-prepared from `registry` (their original `Parse`) before the batch is
    /// sent, so a named statement survives a backend switch/redial (Batch F.4).
    /// `defines` are the named statements this batch's own `Parse`s create —
    /// recorded against the connection once it accepts the batch.
    ///
    /// `fault` is filled on a backend fault exactly like `forward_simple_query`.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn forward_extended_batch(
        client: &mut ClientStream,
        batch: &[u8],
        route_sql: Option<&str>,
        wait_ready: bool,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        registry: &HashMap<String, bytes::Bytes>,
        reprepare: &[String],
        defines: &[String],
        unnamed: Option<&(bytes::Bytes, bytes::Bytes)>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        fault: &mut Option<BackendFault>,
    ) -> Result<(Option<String>, u64)> {
        // Rate-limit gate. The terminating ReadyForQuery is only appended when
        // the batch carried a Sync (`wait_ready`); a Flush-terminated batch
        // expects an ErrorResponse with no ReadyForQuery.
        #[cfg(feature = "rate-limiting")]
        if let Some(mut resp) = Self::rate_limit_check(session, state, config).await {
            if wait_ready {
                resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            }
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // Analytics: the routable SQL (first Parse) + latency timer.
        #[cfg(feature = "query-analytics")]
        let analytics_sql = route_sql.map(|s| s.to_string());
        #[cfg(feature = "query-analytics")]
        let started = std::time::Instant::now();

        let target = match route_sql {
            Some(sql) => {
                // Routing-hints, when active, can override the verb-based
                // target (and recompute the write flag on the stripped SQL).
                #[cfg(feature = "routing-hints")]
                let (is_write, forced) = Self::extended_hint_route(state, sql)
                    .unwrap_or_else(|| (Self::is_write_query(sql), None));
                #[cfg(not(feature = "routing-hints"))]
                let (is_write, forced): (bool, Option<String>) = (Self::is_write_query(sql), None);
                #[cfg(feature = "lag-routing")]
                if is_write && config.lag_routing.enabled {
                    *session.last_write_at.write().await = Some(std::time::Instant::now());
                }
                Self::choose_target_node(is_write, forced, current_node, session, state, config)
                    .await?
            }
            // No Parse in this batch: stay on the prepared-statement /
            // portal connection. Fall back to a read node only if the
            // session has no current connection yet.
            None => match current_node {
                Some(c) => c.to_string(),
                None => Self::select_read_node(session, state, config).await?,
            },
        };

        // Circuit breaker: fast-fail when the chosen node's circuit is open.
        #[cfg(feature = "circuit-breaker")]
        if let Some(mut resp) = Self::circuit_fast_fail(state, &target) {
            if wait_ready {
                resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            }
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Transparently re-prepare any referenced named statement this socket
        // is missing. Each is sent as its original `Parse` + `Flush`; the
        // resulting `ParseComplete` is consumed here so the client never sees
        // the extra round trip. A re-prepare failure recycles the connection.
        for name in reprepare {
            if backend.prepared.contains(name) {
                continue;
            }
            let Some(parse_bytes) = registry.get(name) else {
                continue; // unknown statement — let the batch surface the error
            };
            match Self::reprepare_statement(
                &mut backend.stream,
                parse_bytes,
                state.limits.reprepare_timeout,
                state.limits.max_backend_frame_bytes,
            )
            .await
            {
                Ok(()) => {
                    backend.prepared.insert(name.clone());
                }
                Err(e) => {
                    conns.remove(&target);
                    // A socket-level failure here is a backend fault (the batch
                    // was never sent); a backend *rejecting* the re-prepare is
                    // a protocol-level problem and is propagated as before.
                    if matches!(e, ProxyError::Network(_)) {
                        Self::record_backend_failure(state, &target, &e.to_string());
                        BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
                    }
                    return Err(e);
                }
            }
        }

        // Unnamed-`Parse` promotion: if the held unnamed Parse matches what this
        // connection's unnamed statement already holds, skip forwarding it and
        // synthesize its `ParseComplete` to the client; otherwise forward it
        // first (re-establishing the connection's unnamed statement) and record
        // its signature. A fresh/redialed connection has no signature, so the
        // Parse is always (re)forwarded there — correctness is preserved.
        let mut inject_parse_complete = false;
        let mut new_unnamed_sig: Option<bytes::Bytes> = None;
        if let Some((parse_msg, sig)) = unnamed {
            if backend.unnamed_sig.as_deref() == Some(&sig[..]) {
                inject_parse_complete = true;
            } else {
                if let Err(e) = tokio::time::timeout(
                    state.limits.backend_write_timeout,
                    backend.stream.write_all(parse_msg),
                )
                .await
                .map_err(|_| ProxyError::Network("Backend write timeout".to_string()))
                .and_then(|r| {
                    r.map_err(|e| ProxyError::Network(format!("Backend write error: {}", e)))
                }) {
                    conns.remove(&target);
                    Self::record_backend_failure(state, &target, &e.to_string());
                    BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
                    return Err(e);
                }
                new_unnamed_sig = Some(sig.clone());
            }
        }

        if let Err((e, phase)) = Self::tr_write_batch(
            &mut backend.stream,
            batch,
            state.limits.backend_write_timeout,
        )
        .await
        {
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, phase, &e);
            return Err(e);
        }

        // The client expects `ParseComplete` first; the backend won't send one
        // for a skipped Parse, so emit it here before relaying the response.
        let mut injected: u64 = 0;
        if inject_parse_complete {
            if let Err(e) = client
                .write_all(&[b'1', 0, 0, 0, 4])
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))
            {
                conns.remove(&target);
                return Err(e);
            }
            injected = 5;
        }

        // TR-07: register the batch's Parse/Bind/Execute/Close (and the held
        // unnamed Parse the backend already holds) so the relay collects each
        // Execute's outcome with its bound parameter values.
        if config.tr_enabled || Self::cache_needs_capture(state) {
            Self::journal_register_batch(session, batch, unnamed.map(|(m, _)| &m[..]));
        }

        let r = if wait_ready {
            Self::stream_until_ready(client, &mut backend.stream, session, state).await
        } else {
            Self::stream_flush(client, &mut backend.stream, session, state).await
        };
        match r {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(state, session, sql, &target, started.elapsed(), None)
                        .await;
                }
                // The connection now holds these named statements.
                for name in defines {
                    backend.prepared.insert(name.clone());
                }
                // ...and the (re)forwarded unnamed statement.
                if let Some(sig) = new_unnamed_sig {
                    backend.unnamed_sig = Some(sig);
                }
                Ok((Some(target), sent + injected))
            }
            Err(mut e) => {
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                e.progress.bytes += injected;
                BackendFault::set_response(fault, &target, &e);
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(
                        state,
                        session,
                        sql,
                        &target,
                        started.elapsed(),
                        Some(e.to_string()),
                    )
                    .await;
                }
                Err(e.error)
            }
        }
    }

    /// Re-issue one named `Parse` on a backend socket out-of-band: send the
    /// original `Parse` bytes followed by a `Flush`, then read and discard the
    /// single `ParseComplete` the backend emits. The statement persists on the
    /// connection (the implicit transaction is closed later by the real
    /// batch's `Sync`). An `ErrorResponse` means the re-prepare failed.
    pub(super) async fn reprepare_statement<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        parse_bytes: &[u8],
        reprepare_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<()> {
        tokio::time::timeout(reprepare_timeout, backend.write_all(parse_bytes))
            .await
            .map_err(|_| ProxyError::Network("re-prepare write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare write error: {}", e)))?;
        // Flush: 'H' + length 4.
        tokio::time::timeout(reprepare_timeout, backend.write_all(&[b'H', 0, 0, 0, 4]))
            .await
            .map_err(|_| ProxyError::Network("re-prepare flush timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare flush error: {}", e)))?;
        let mtype = tokio::time::timeout(
            reprepare_timeout,
            Self::read_one_frame_type(backend, max_frame_bytes),
        )
        .await
        .map_err(|_| ProxyError::Network("re-prepare read timeout".to_string()))??;
        match mtype {
            b'1' => Ok(()), // ParseComplete
            b'E' => Err(ProxyError::Protocol(
                "re-prepare rejected by backend".to_string(),
            )),
            other => Err(ProxyError::Protocol(format!(
                "unexpected re-prepare reply: {}",
                other as char
            ))),
        }
    }

    /// Read exactly one backend message frame (5-byte header + body) and return
    /// its type byte, discarding the body. Used to consume the `ParseComplete`
    /// produced by an out-of-band re-prepare.
    pub(super) async fn read_one_frame_type<S: AsyncReadExt + Unpin>(
        backend: &mut S,
        max_frame_bytes: usize,
    ) -> Result<u8> {
        let mut header = [0u8; 5];
        backend
            .read_exact(&mut header)
            .await
            .map_err(|e| ProxyError::Network(format!("re-prepare read error: {}", e)))?;
        // H-07: validate before trusting the length, and never allocate the
        // advertised body size — discard it through a fixed scratch buffer.
        let len = backend_frame_len(&header, max_frame_bytes)?.expect("5 header bytes were read");
        let mut remaining = len - 4;
        let mut scratch = [0u8; 16 * 1024];
        while remaining > 0 {
            let n = remaining.min(scratch.len());
            backend
                .read_exact(&mut scratch[..n])
                .await
                .map_err(|e| ProxyError::Network(format!("re-prepare body read error: {}", e)))?;
            remaining -= n;
        }
        Ok(header[0])
    }

    /// Name a `Parse` defines: its first cstring. `""` is the unnamed
    /// statement, which is per-protocol transient and never tracked.
    pub(super) fn parse_stmt_name(payload: &[u8]) -> &str {
        let end = payload.iter().position(|&b| b == 0).unwrap_or(0);
        std::str::from_utf8(&payload[..end]).unwrap_or("")
    }

    /// Prepared-statement name a `Bind` references: the *second* cstring
    /// (portal name first, then statement name). `None` for the unnamed
    /// statement.
    pub(super) fn bind_stmt_ref(payload: &[u8]) -> Option<&str> {
        let portal_end = payload.iter().position(|&b| b == 0)?;
        let rest = &payload[portal_end + 1..];
        let stmt_end = rest.iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&rest[..stmt_end]).ok()?;
        (!name.is_empty()).then_some(name)
    }

    /// Statement name a `Describe`/`Close` targets — only when it is
    /// statement-kind (`'S'`, not portal `'P'`). `None` otherwise.
    pub(super) fn stmt_kind_name(payload: &[u8]) -> Option<&str> {
        if payload.first() != Some(&b'S') {
            return None;
        }
        let rest = &payload[1..];
        let end = rest.iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&rest[..end]).ok()?;
        (!name.is_empty()).then_some(name)
    }

    /// Stream backend response frames to the client until ReadyForQuery (end
    /// of a Sync/simple-query response). Forwards bytes verbatim, coalescing
    /// all currently-complete frames into one write and keeping only a
    /// partial-frame tail buffered, so proxy memory stays O(frame) rather
    /// than O(result). Also yields on CopyInResponse/CopyBothResponse so the
    /// client can supply COPY data. Updates `tx_state` from the RFQ status.
    /// Returns bytes streamed to the client.
    pub(super) async fn stream_until_ready(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<u64, ResponseFailure> {
        let client_write_timeout = state.limits.client_write_timeout;
        let backend_read_timeout = state.limits.backend_read_timeout;
        let response_deadline = state
            .limits
            .backend_response_timeout
            .map(|d| tokio::time::Instant::now() + d);
        // TR-06: observe only inside an explicit transaction under a recording
        // mode; everything else pays no hashing.
        let mut observation = (session.in_transaction.load(Ordering::Relaxed)
            && matches!(session.tr_mode, TrMode::Select | TrMode::Transaction))
        .then(|| Observation::new(state.limits.tr_max_observation_bytes));
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut had_error = false;
        let mut command_complete = false;
        // TR-07: per-statement completions and the first error are collected
        // only when the forward path registered something for this cycle.
        let capture_armed = session.journal_armed.load(Ordering::Relaxed);
        let mut completions: Vec<crate::journal_capture::Completion> = Vec::new();
        let mut first_error: Option<(String, String)> = None;

        let response = async {
            loop {
                // Walk complete frames in `buf`, stopping at a boundary frame.
                let mut consumed = 0usize;
                let mut ready_status: Option<u8> = None;
                let mut yield_for_copy = false;
                loop {
                    let rem = &buf[consumed..];
                    // H-07: a malformed header (len < 4, or above the budget) is
                    // an error NOW, not "wait for more bytes" — no byte count can
                    // make it valid and the accumulator must not chase it.
                    let Some(len) = backend_frame_len(rem, state.limits.max_backend_frame_bytes)?
                    else {
                        break;
                    };
                    if rem.len() < len + 1 {
                        break; // incomplete — need more bytes
                    }
                    let frame_total = len + 1;
                    let mtype = rem[0];
                    consumed += frame_total;
                    if mtype == b'E' {
                        had_error = true;
                    }
                    command_complete |= mtype == b'C';
                    if let Some(obs) = observation.as_mut() {
                        obs.note(&rem[..frame_total]);
                    }
                    if capture_armed {
                        Self::journal_note_frame(
                            &rem[..frame_total],
                            &mut completions,
                            &mut first_error,
                        );
                    }
                    if mtype == b'Z' {
                        // ReadyForQuery: payload is one status byte at rem[5].
                        ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                        break;
                    }
                    if mtype == b'G' || mtype == b'W' {
                        // CopyInResponse / CopyBothResponse: the backend now wants
                        // CopyData from the client — forward up to here and yield.
                        yield_for_copy = true;
                        break;
                    }
                }

                // C-02: with the query cache on, settle the cycle's capture
                // ops — and move the cache generations of what it wrote —
                // BEFORE the ReadyForQuery that acknowledges it reaches the
                // client, so no read that starts after the acknowledgement is
                // served a pre-commit entry. Without a cache nothing depends
                // on that ordering, so the observation stays after the write,
                // off the client's round trip (as before C-02).
                let mut outcome =
                    ready_status.map(|status| crate::journal_capture::ResponseOutcome {
                        status,
                        completions: std::mem::take(&mut completions),
                        error: first_error.take(),
                    });
                let early = if Self::cache_needs_capture(state) {
                    outcome
                        .take()
                        .and_then(|o| Self::journal_observe_sync(session, state, o))
                } else {
                    None
                };

                if consumed > 0 {
                    tokio::time::timeout(client_write_timeout, client.write_all(&buf[..consumed]))
                        .await
                        .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    sent += consumed as u64;
                    let _ = buf.split_to(consumed);
                }

                if let Some(status) = ready_status {
                    Self::note_ready_for_query(session, status, had_error);
                    Self::note_observation(session, observation.as_ref());
                    let observed = early.or_else(|| {
                        outcome
                            .take()
                            .and_then(|o| Self::journal_observe_sync(session, state, o))
                    });
                    if let Some(cycle) = observed {
                        Self::journal_observe_finish(session, state, cycle).await;
                    }
                    return Ok(sent);
                }
                if yield_for_copy {
                    // The backend now awaits CopyData from the client; the session
                    // is mid-COPY, not at a clean boundary. Mark it so pool release
                    // is suppressed until the COPY drains (cleared in the CopyDone
                    // path). Harmless in session mode (release is a no-op there).
                    session
                        .copy_in_progress
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return Ok(sent);
                }

                // Read straight into the frame accumulator — no zeroed scratch, no
                // copy. `read_buf` appends to `buf`'s spare capacity.
                buf.reserve(16384);
                let n = tokio::time::timeout(
                    read_budget(backend_read_timeout, response_deadline)?,
                    backend.read_buf(&mut buf),
                )
                .await
                .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;
                if n == 0 {
                    return Err(ProxyError::Connection(
                        "Backend closed mid-response".to_string(),
                    ));
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: had_error || command_complete,
                raw: false,
            },
        })
    }

    /// Like `stream_until_ready` but also captures the full response bytes for
    /// caching (query-cache and/or edge cache — one shared capture).
    /// Returns `(bytes_sent, captured, cacheable, row_count)`.
    /// `cacheable` is false if the response carried an `ErrorResponse`, ended in
    /// a non-idle transaction status, yielded for COPY, or contained an
    /// asynchronous backend frame — NotificationResponse ('A'), NoticeResponse
    /// ('N'), or ParameterStatus ('S'). Async frames belong to THIS session's
    /// moment in time (a LISTENing session's notification, a GUC change);
    /// replaying them to every later hitter would leak the notification
    /// payload cross-session and desynchronize hitters' parameter state. The
    /// frames are still forwarded to the live requester — only the store is
    /// suppressed.
    ///
    /// `max_capture_bytes` ([cache] `max_cacheable_response_bytes`) bounds the
    /// capture buffer: the capture is a *transient* held per concurrent
    /// session ON TOP of the bytes already streamed out, so an unbounded one
    /// let a single `SELECT * FROM big_table` pin the whole result set in the
    /// proxy. The moment appending would cross the bound the buffer is dropped
    /// (memory freed immediately), nothing further is captured, and the
    /// response is reported non-cacheable. Forwarding to the client is
    /// untouched — the client still receives every byte, byte-for-byte.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) async fn stream_until_ready_capture(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        relay: RelayLimits,
        max_capture_bytes: usize,
        metrics: &ServerMetrics,
    ) -> std::result::Result<(u64, Vec<u8>, bool, usize), ResponseFailure> {
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut captured: Vec<u8> = Vec::with_capacity(4096);
        let mut had_error = false;
        let mut command_complete = false;
        let mut saw_async = false;
        let mut row_count: usize = 0;
        // Set once the response outgrows `max_capture_bytes`; `captured` is
        // then empty and stays empty for the rest of the response.
        let mut oversize = false;
        let response_deadline = relay
            .response_timeout
            .map(|d| tokio::time::Instant::now() + d);
        let mut observation = (session.in_transaction.load(Ordering::Relaxed)
            && matches!(session.tr_mode, TrMode::Select | TrMode::Transaction))
        .then(|| Observation::new(relay.observation_bytes));

        let response = async {
            loop {
                let mut consumed = 0usize;
                let mut ready_status: Option<u8> = None;
                let mut yield_for_copy = false;
                loop {
                    let rem = &buf[consumed..];
                    let Some(len) = backend_frame_len(rem, relay.max_frame_bytes)? else {
                        break;
                    };
                    if rem.len() < len + 1 {
                        break;
                    }
                    let frame_total = len + 1;
                    let mtype = rem[0];
                    if mtype == b'E' {
                        had_error = true;
                    }
                    command_complete |= mtype == b'C';
                    if let Some(obs) = observation.as_mut() {
                        obs.note(&rem[..frame_total]);
                    }
                    // Async backend frames (backend 'S' is unambiguous here —
                    // PortalSuspended is lowercase 's').
                    if mtype == b'A' || mtype == b'N' || mtype == b'S' {
                        saw_async = true;
                    }
                    if mtype == b'C' {
                        // CommandComplete tag, e.g. "SELECT 5" — take the row count.
                        if let Some(tag) = rem.get(5..frame_total) {
                            if let Some(end) = tag.iter().position(|&b| b == 0) {
                                if let Ok(s) = std::str::from_utf8(&tag[..end]) {
                                    if let Some(n) =
                                        s.rsplit(' ').next().and_then(|x| x.parse::<usize>().ok())
                                    {
                                        row_count = n;
                                    }
                                }
                            }
                        }
                    }
                    consumed += frame_total;
                    if mtype == b'Z' {
                        ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                        break;
                    }
                    if mtype == b'G' || mtype == b'W' {
                        yield_for_copy = true;
                        break;
                    }
                }

                if consumed > 0 {
                    tokio::time::timeout(
                        relay.client_write_timeout,
                        client.write_all(&buf[..consumed]),
                    )
                    .await
                    .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                    .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    if !oversize {
                        if captured.len().saturating_add(consumed) > max_capture_bytes {
                            oversize = true;
                            // Free the transient NOW (`clear` alone keeps the
                            // allocation alive for the rest of the response).
                            captured = Vec::new();
                            metrics
                                .cache_capture_oversize
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                target: "helios::cache",
                                limit = max_capture_bytes,
                                "response exceeds cache.max_cacheable_response_bytes — \
                                 capture abandoned, response not cached"
                            );
                        } else {
                            captured.extend_from_slice(&buf[..consumed]);
                        }
                    }
                    sent += consumed as u64;
                    let _ = buf.split_to(consumed);
                }

                if let Some(status) = ready_status {
                    Self::note_ready_for_query(session, status, had_error);
                    Self::note_observation(session, observation.as_ref());
                    let cacheable = !had_error && status == b'I' && !saw_async && !oversize;
                    return Ok((sent, captured, cacheable, row_count));
                }
                if yield_for_copy {
                    session
                        .copy_in_progress
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return Ok((sent, captured, false, row_count));
                }

                // Read straight into the frame accumulator — no zeroed scratch.
                buf.reserve(16384);
                let n = tokio::time::timeout(
                    read_budget(relay.backend_read_timeout, response_deadline)?,
                    backend.read_buf(&mut buf),
                )
                .await
                .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;
                if n == 0 {
                    return Err(ProxyError::Connection(
                        "Backend closed mid-response".to_string(),
                    ));
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: had_error || command_complete,
                raw: false,
            },
        })
    }

    /// Relay whatever the backend has *already* produced in response to a
    /// `Flush` (which, unlike `Sync`, yields no ReadyForQuery), then return
    /// immediately — without waiting.
    ///
    /// Any Flush output that has not landed in the socket yet is delivered by
    /// the main loop's backend watch (which relays the current backend's
    /// out-of-band bytes while waiting for the client), so there is no fixed
    /// post-Flush stall: the previous version blocked the session loop for up to
    /// 200 ms after the last backend byte before it would read the client's next
    /// message, adding that latency to every `Parse`/`Flush`-then-`Bind` prepare
    /// cycle. Here we drain what is instantly available and hand control back;
    /// the client's next frames are read at once. The eventual `Sync` drains the
    /// final ReadyForQuery via `stream_until_ready`.
    pub(super) async fn stream_flush(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<u64, ResponseFailure> {
        let _ = session;
        // Reused across every read of this call via `try_read_buf`, which
        // writes straight into the buffer's spare capacity from the read
        // syscall — unlike `vec![0u8; 16384]`, nothing here is
        // zero-initialized before use.
        let mut read_buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let response = async {
            loop {
                read_buf.clear();
                match backend.try_read_buf(&mut read_buf) {
                    Ok(0) => {
                        return Err(ProxyError::Connection(
                            "Backend closed mid-flush".to_string(),
                        ))
                    }
                    Ok(n) => {
                        tokio::time::timeout(
                            state.limits.client_write_timeout,
                            client.write_all(&read_buf[..n]),
                        )
                        .await
                        .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                        sent += n as u64;
                    }
                    // Nothing more instantly available — the backend watch delivers
                    // any remaining Flush output as it arrives.
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(sent),
                    Err(e) => {
                        return Err(ProxyError::Network(format!("Backend read error: {}", e)))
                    }
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: false,
                raw: sent > 0,
            },
        })
    }

    /// Append `name` to a per-cycle extended-batch tracker at most once,
    /// bounded by the session's named-statement cap (O-05). A client that
    /// never sends Sync cannot grow `batch_refs`/`batch_defines`/`batch_closes`
    /// without bound, and the re-prepare filter stays linear in the cap.
    pub(super) fn remember_batch_name(list: &mut Vec<String>, name: &str, cap: usize) {
        if list.len() >= cap || list.iter().any(|n| n == name) {
            return;
        }
        list.push(name.to_string());
    }
}
