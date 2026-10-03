use super::*;

impl ProxyServer {
    /// Spawn health checker background task
    pub(super) fn spawn_health_checker(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Clamp to a 1s floor: `tokio::time::interval` panics on a zero
            // period, which would silently kill this task (it would then never
            // probe, so the proxy keeps routing to dead backends). `validate()`
            // already rejects 0 in a file config; this defends a
            // programmatically-built or reloaded config too.
            let interval_secs = state.live_config.load().health.check_interval_secs.max(1);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Read the live config each tick so a SIGHUP that
                        // adds/removes nodes is checked on the next sweep.
                        let config = state.live_config.load_full();
                        // Run the sweep in a child task so an unexpected panic in
                        // check_all_nodes surfaces as a JoinError and is logged —
                        // the health loop keeps running instead of dying silently
                        // and freezing health at its last snapshot forever.
                        let st = state.clone();
                        if let Err(e) = tokio::spawn(async move {
                            Self::check_all_nodes(&st, &config).await;
                        })
                        .await
                        {
                            tracing::error!(error = %e, "health-check sweep panicked; health loop continuing");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        })
    }

    /// Check health of all nodes.
    ///
    /// Probes run concurrently (one slow/unreachable node no longer delays
    /// detection on the others — lowers the failover-detection latency
    /// floor), then a single new health snapshot is published via ArcSwap so
    /// readers on the query path never block.
    pub(super) async fn check_all_nodes(state: &Arc<ServerState>, config: &ProxyConfig) {
        // Probe every node in parallel (owned address + timeout so each
        // probe is 'static and runs on its own task).
        let timeout = Duration::from_secs(config.health.check_timeout_secs);
        // H-04: with credentials configured, sample the primary's current WAL
        // position once per sweep so each standby's replay position becomes a
        // byte lag. Best-effort; any failure leaves lag unknown.
        let primary_lsn = if config.health.user.is_some() && config.lag_routing.enabled {
            Self::probe_primary_lsn(config, timeout).await
        } else {
            None
        };

        let mut set = tokio::task::JoinSet::new();
        for node in &config.nodes {
            let addr = node.address().to_string();
            let health_cfg = config.health.clone();
            let role = node.role;
            set.spawn(async move {
                let r = Self::probe_node(&health_cfg, &addr, role, primary_lsn, timeout).await;
                (addr, r)
            });
        }
        let mut results = Vec::with_capacity(config.nodes.len());
        while let Some(joined) = set.join_next().await {
            if let Ok(pair) = joined {
                results.push(pair);
            }
        }

        // Clone-and-modify the current snapshot, then atomically swap it in.
        // Hold the write lock so a concurrent in-band demotion landing in this
        // load→store window (or a SIGHUP reconcile) cannot clobber, or be
        // clobbered by, this full-map rebuild. All node probing above already
        // completed; no await is held under the guard.
        let _writers = state.health_write.lock();
        let mut next = (*state.health.load_full()).clone();
        for (addr, result) in results {
            if let Some(node_health) = next.get_mut(&addr) {
                let (ok, latency, lag) = match result {
                    Ok((latency, lag)) => (true, Some(latency), lag),
                    Err(e) => {
                        node_health.last_error = Some(e.to_string());
                        (false, None, None)
                    }
                };
                let (healthy, sc, fc) = Self::advance_health(
                    node_health.healthy,
                    node_health.success_count,
                    node_health.failure_count,
                    ok,
                    config.health.failure_threshold,
                    config.health.success_threshold,
                );
                let was_healthy = node_health.healthy;
                node_health.healthy = healthy;
                node_health.success_count = sc;
                node_health.failure_count = fc;
                if let Some(latency) = latency {
                    node_health.latency_ms = latency;
                    node_health.last_error = None;
                }
                if let Some(lag) = lag {
                    node_health.replication_lag_bytes = Some(lag);
                    node_health.lag_sampled_at = Some(chrono::Utc::now());
                }
                if was_healthy && !healthy {
                    tracing::warn!(
                        "Node {} marked unhealthy after {} failures",
                        addr,
                        node_health.failure_count
                    );
                } else if !was_healthy && healthy {
                    tracing::info!(
                        "Node {} recovered after {} consecutive successes",
                        addr,
                        node_health.success_count
                    );
                }
                node_health.last_check = chrono::Utc::now();
            }
        }
        state.health.store(Arc::new(next));
    }

    /// Advance the consecutive success/failure counters and decide health
    /// (H-03). A healthy node stays healthy on success; a node marked
    /// unhealthy only returns to healthy after `success_threshold` consecutive
    /// successes. Failures always count toward `failure_threshold`. Pure so the
    /// policy is unit-tested without a backend.
    pub(super) fn advance_health(
        healthy: bool,
        success_count: u32,
        failure_count: u32,
        ok: bool,
        failure_threshold: u32,
        success_threshold: u32,
    ) -> (bool, u32, u32) {
        if ok {
            let sc = success_count.saturating_add(1);
            (healthy || sc >= success_threshold.max(1), sc, 0)
        } else {
            let fc = failure_count.saturating_add(1);
            let now_healthy = if fc >= failure_threshold.max(1) {
                false
            } else {
                healthy
            };
            (now_healthy, 0, fc)
        }
    }

    /// One node probe (H-03/H-04): a credential-less protocol probe by default,
    /// or `check_query` plus a standby WAL-position lag probe when
    /// `[health] user` is configured. Returns `(latency_ms, lag_bytes)`.
    pub(super) async fn probe_node(
        health: &crate::config::HealthConfig,
        addr: &str,
        role: NodeRole,
        primary_lsn: Option<u64>,
        timeout: Duration,
    ) -> Result<(f64, Option<u64>)> {
        if health.user.is_none() {
            return Self::check_node_addr(addr, timeout)
                .await
                .map(|l| (l, None));
        }
        Self::check_node_query(health, addr, role, primary_lsn, timeout).await
    }

    /// Credentialed probe: connect, run `[health] check_query` and, for a
    /// standby/read-replica with a known primary LSN, `pg_last_wal_replay_lsn()`
    /// to compute the byte lag. The lag probe is best-effort and never fails
    /// the health check (non-PostgreSQL backends simply report unknown lag).
    pub(super) async fn check_node_query(
        health: &crate::config::HealthConfig,
        addr: &str,
        role: NodeRole,
        primary_lsn: Option<u64>,
        timeout: Duration,
    ) -> Result<(f64, Option<u64>)> {
        use crate::backend::{
            tls::default_client_config, BackendClient, BackendConfig, TextValue, TlsMode,
        };
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| ProxyError::HealthCheck(format!("bad node address {}", addr)))?;
        let port: u16 = port
            .parse()
            .map_err(|_| ProxyError::HealthCheck(format!("bad node port {}", addr)))?;
        let mk_cfg = |user: String| BackendConfig {
            host: host.to_string(),
            port,
            user,
            password: health.password.clone(),
            database: health.database.clone(),
            application_name: Some("heliosdb-proxy-health".into()),
            tls_mode: TlsMode::Prefer,
            connect_timeout: timeout,
            query_timeout: timeout,
            tls_config: default_client_config(),
        };
        let cfg = mk_cfg(
            health
                .user
                .clone()
                .unwrap_or_else(|| "postgres".to_string()),
        );
        let start = std::time::Instant::now();
        let mut client = tokio::time::timeout(timeout, BackendClient::connect(&cfg))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("Timeout connecting to {}", addr)))?
            .map_err(|e| ProxyError::HealthCheck(format!("connect {}: {}", addr, e)))?;
        tokio::time::timeout(timeout, client.simple_query(&health.check_query))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("{} check_query timed out", addr)))?
            .map_err(|e| ProxyError::HealthCheck(format!("{} check_query: {}", addr, e)))?;
        let latency = start.elapsed().as_secs_f64() * 1000.0;

        let mut lag = None;
        if matches!(role, NodeRole::Standby | NodeRole::ReadReplica) {
            if let Some(primary) = primary_lsn {
                if let Ok(res) = client
                    .simple_query("SELECT pg_last_wal_replay_lsn()::text")
                    .await
                {
                    let text = res
                        .rows
                        .first()
                        .and_then(|r| r.first())
                        .and_then(|v| match v {
                            TextValue::Text(s) => Some(s.clone()),
                            TextValue::Null => None,
                        });
                    if let Some(text) = text.as_deref().and_then(parse_pg_lsn) {
                        lag = Some(primary.saturating_sub(text));
                    }
                }
            }
        }
        client.close().await;
        Ok((latency, lag))
    }

    /// Sample the configured primary's current WAL position once per sweep.
    /// `None` on any failure (the standby probes then report unknown lag).
    pub(super) async fn probe_primary_lsn(config: &ProxyConfig, timeout: Duration) -> Option<u64> {
        use crate::backend::{
            tls::default_client_config, BackendClient, BackendConfig, TextValue, TlsMode,
        };
        let primary = config
            .nodes
            .iter()
            .find(|n| n.role == NodeRole::Primary && n.enabled)?;
        let cfg = BackendConfig {
            host: primary.host.clone(),
            port: primary.port,
            user: config
                .health
                .user
                .clone()
                .unwrap_or_else(|| "postgres".to_string()),
            password: config.health.password.clone(),
            database: config.health.database.clone(),
            application_name: Some("heliosdb-proxy-health".into()),
            tls_mode: TlsMode::Prefer,
            connect_timeout: timeout,
            query_timeout: timeout,
            tls_config: default_client_config(),
        };
        let mut client = tokio::time::timeout(timeout, BackendClient::connect(&cfg))
            .await
            .ok()?
            .ok()?;
        let out = tokio::time::timeout(
            timeout,
            client.simple_query("SELECT pg_current_wal_lsn()::text"),
        )
        .await
        .ok()?
        .ok()?;
        client.close().await;
        let text = out
            .rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| match v {
                TextValue::Text(s) => Some(s.as_str()),
                TextValue::Null => None,
            })?;
        parse_pg_lsn(text)
    }

    /// Check health of a single node with a protocol-level liveness probe.
    ///
    /// A bare TCP connect is not enough: a wedged backend (postmaster stuck,
    /// out of backend slots, mid-crash-recovery) still *accepts* the socket but
    /// never processes the wire protocol, so a connect-only probe reports it
    /// healthy. Instead we connect, send a PostgreSQL `SSLRequest`, and require
    /// the postmaster to answer (`S`/`N`) within the timeout. The SSLRequest is
    /// auth-free and not logged, so it costs the backend essentially nothing,
    /// yet it proves the server is actually servicing the protocol. Returns the
    /// round-trip latency in milliseconds.
    pub(super) async fn check_node_addr(addr: &str, timeout: Duration) -> Result<f64> {
        // length(8) + SSLRequest code 80877103 (0x04D2162F).
        const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];
        let start = std::time::Instant::now();
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("Timeout connecting to {}", addr)))?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("Failed to connect to {}: {}", addr, e))
            })?;

        let probe = async {
            stream.write_all(&SSL_REQUEST).await?;
            let mut resp = [0u8; 1];
            stream.read_exact(&mut resp).await?;
            Ok::<u8, std::io::Error>(resp[0])
        };
        // Budget whatever time is left after the connect for the handshake.
        let remaining = timeout
            .saturating_sub(start.elapsed())
            .max(Duration::from_millis(1));
        let byte = tokio::time::timeout(remaining, probe)
            .await
            .map_err(|_| {
                ProxyError::HealthCheck(format!("{} did not answer protocol probe in time", addr))
            })?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("{} protocol probe error: {}", addr, e))
            })?;
        // 'S' (TLS available) or 'N' (not) both prove the postmaster is live and
        // talking the protocol; anything else means a non-PostgreSQL listener.
        if byte != b'S' && byte != b'N' {
            return Err(ProxyError::HealthCheck(format!(
                "{} sent unexpected probe reply {:#x}",
                addr, byte
            )));
        }
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        Ok(latency)
    }
}
