use super::*;

impl ProxyServer {
    /// Select primary node with write timeout during failover
    pub(super) async fn select_primary_with_timeout(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        let deadline = tokio::time::Instant::now() + config.write_timeout();
        Self::select_primary_until(session, state, config, deadline).await
    }

    /// Build the authoritative primary tracker from `[topology]` (H-01).
    ///
    /// `static` (the default) returns a standalone tracker and
    /// `authoritative = false`, preserving the historical write-path
    /// behaviour. `postgres` builds a `PostgresTopologyProvider` over every
    /// configured node and `patroni` a `PatroniTopologyProvider` over
    /// `topology.patroni_endpoints` (both feature `postgres-topology`); either
    /// marks the tracker authoritative, so a promotion moves the write
    /// destination without any `proxy.toml` edit.
    pub(super) fn build_primary_tracker(
        config: &ProxyConfig,
    ) -> (Arc<PrimaryTracker>, bool, TopologyPoller) {
        #[cfg(feature = "postgres-topology")]
        {
            if config.topology.provider == crate::config::TopologyProviderKind::Patroni {
                // The leader Patroni names must be one of these addresses,
                // exactly as `[[nodes]]` spells them, or nothing is authorized.
                let nodes = config
                    .nodes
                    .iter()
                    .map(|n| crate::primary_tracker::PatroniNode {
                        node_id: uuid::Uuid::new_v4(),
                        address: n.address().to_string(),
                    })
                    .collect();
                let provider = Arc::new(
                    crate::primary_tracker::PatroniTopologyProvider::new(
                        config.topology.patroni_endpoints.clone(),
                        nodes,
                        Duration::from_millis(config.topology.patroni_request_timeout_ms.max(1)),
                    )
                    .with_poll_interval(Duration::from_secs(
                        config.topology.poll_interval_secs.max(1),
                    ))
                    .with_leader_lock_confirmation(config.topology.patroni_confirm_leader_lock),
                );
                tracing::info!(
                    endpoints = config.topology.patroni_endpoints.len(),
                    confirm_leader_lock = config.topology.patroni_confirm_leader_lock,
                    nodes = config.nodes.len(),
                    poll_interval_secs = config.topology.poll_interval_secs,
                    lease_timeout_secs = config.topology.lease_timeout_secs,
                    "authoritative topology provider enabled: patroni (GET /cluster polling)"
                );
                let tracker = Arc::new(
                    PrimaryTracker::with_provider(provider.clone()).with_lease_timeout(
                        Duration::from_secs(config.topology.lease_timeout_secs.max(1)),
                    ),
                );
                return (tracker, true, TopologyPoller::Patroni(provider));
            }
            if config.topology.provider == crate::config::TopologyProviderKind::Postgres {
                let nodes = config
                    .nodes
                    .iter()
                    .map(|n| crate::primary_tracker::PostgresNode {
                        node_id: uuid::Uuid::new_v4(),
                        host: n.host.clone(),
                        port: n.port,
                        user: config.topology.user.clone(),
                        password: config.topology.password.clone(),
                        database: config.topology.database.clone(),
                    })
                    .collect();
                let provider = Arc::new(
                    crate::primary_tracker::PostgresTopologyProvider::new(nodes)
                        .with_poll_interval(Duration::from_secs(
                            config.topology.poll_interval_secs.max(1),
                        )),
                );
                tracing::info!(
                    nodes = config.nodes.len(),
                    poll_interval_secs = config.topology.poll_interval_secs,
                    lease_timeout_secs = config.topology.lease_timeout_secs,
                    "authoritative topology provider enabled: postgres (pg_is_in_recovery polling)"
                );
                let tracker = Arc::new(
                    PrimaryTracker::with_provider(provider.clone()).with_lease_timeout(
                        Duration::from_secs(config.topology.lease_timeout_secs.max(1)),
                    ),
                );
                return (tracker, true, TopologyPoller::Postgres(provider));
            }
        }
        let _ = config;
        (
            Arc::new(PrimaryTracker::new_standalone()),
            false,
            TopologyPoller::Static,
        )
    }

    /// Resolve the authoritative provider's leader against the configured
    /// nodes (H-01/H-02). Returns `Some(address)` only while the tracker's
    /// authority lease is valid (the provider was reached within
    /// `topology.lease_timeout_secs`) and the leader is an **enabled**
    /// configured node; `None` means the write path must wait (fail closed)
    /// instead of falling back to roles or using stale knowledge.
    pub(super) fn authoritative_leader(
        config: &ProxyConfig,
        tracker: &PrimaryTracker,
    ) -> Option<String> {
        if !tracker.authority_valid() {
            return None;
        }
        let addr = tracker.get_primary_address()?;
        config
            .nodes
            .iter()
            .any(|n| n.enabled && n.address() == addr)
            .then_some(addr)
    }

    /// Whether `addr` is currently an eligible write target. Under an
    /// authoritative topology provider only the provider's leader qualifies
    /// (and nothing does while its authority lease is invalid); otherwise the
    /// configured `role = "primary"` decides. A node the provider has demoted
    /// stays `role = "primary"` in a static `[[nodes]]` list, so the role alone
    /// must never be trusted once a provider is in charge.
    pub(super) fn is_write_target(state: &ServerState, config: &ProxyConfig, addr: &str) -> bool {
        if state.authoritative_topology {
            return state.routing.load().write_leader(std::time::Instant::now()) == Some(addr)
                && config
                    .nodes
                    .iter()
                    .any(|n| n.enabled && n.address() == addr);
        }
        config
            .nodes
            .iter()
            .find(|n| n.address() == addr)
            .map(|n| n.role == NodeRole::Primary)
            .unwrap_or(false)
    }

    /// Full-jitter exponential backoff for the primary-wait loops (H-05):
    /// base 100 ms doubling to a 2 s cap, mixed with a per-session seed so a
    /// fleet waking after the same failover does not stampede the promoted
    /// primary in lockstep. Deterministic per `(seed, attempt)`.
    pub(super) fn reconnect_backoff(attempt: u32, seed: u64) -> Duration {
        let base_ms = 100u64;
        let cap_ms = 2_000u64;
        let window = base_ms
            .saturating_mul(1u64 << attempt.min(5))
            .clamp(1, cap_ms);
        Duration::from_millis(
            splitmix64(seed ^ (attempt as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)) % window + 1,
        )
    }

    /// `select_primary_with_timeout` against a caller-owned deadline, so one
    /// recovery deadline can be carried through every phase (TR-06).
    pub(super) async fn select_primary_until(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        deadline: tokio::time::Instant,
    ) -> Result<String> {
        let start = tokio::time::Instant::now();
        let timeout = deadline.saturating_duration_since(start);
        // H-05: per-session full-jitter backoff instead of a fixed 100 ms poll,
        // so a fleet that lost the same primary does not reconnect in lockstep.
        let backoff_seed = session.id.as_u128() as u64;
        let mut attempt: u32 = 0;

        loop {
            // H-01: when a topology provider is authoritative, only its
            // leader is eligible. If the provider has no leader (or the
            // leader is disabled) new writes wait — they must never fall
            // back to a configured role the provider has not authorised.
            if state.authoritative_topology {
                if let Some(addr) = Self::snapshot_leader(state, config) {
                    let mut current = session.current_node.write().await;
                    *current = Some(addr.clone());
                    return Ok(addr);
                }
                if tokio::time::Instant::now() >= deadline {
                    state.metrics.failovers.fetch_add(1, Ordering::Relaxed);
                    return Err(ProxyError::NoHealthyNodes);
                }
                tracing::warn!(
                    "Authoritative topology has no eligible primary; waiting... ({:.1}s elapsed)",
                    start.elapsed().as_secs_f64()
                );
                state
                    .metrics
                    .reconnect_attempts
                    .fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Self::reconnect_backoff(attempt, backoff_seed)).await;
                attempt = attempt.saturating_add(1);
                continue;
            }

            // Try to find a healthy primary. Every enabled primary is
            // considered in config order (not just the first): a config that
            // lists a promoted/secondary primary must be able to fail over to
            // it once the first is demoted in-band — `select_node_for_startup`
            // already applies the same rule for new sessions.
            let health = state.health.load_full();
            let primary = config.nodes.iter().find(|n| {
                n.role == NodeRole::Primary
                    && n.enabled
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false)
            });

            if let Some(primary_node) = primary {
                // Update session's current node
                let node_addr = primary_node.address().to_string();
                let mut current = session.current_node.write().await;
                *current = Some(node_addr.clone());
                return Ok(node_addr);
            }
            drop(health);

            // Check if the deadline passed
            if tokio::time::Instant::now() >= deadline {
                state.metrics.failovers.fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::NoHealthyNodes);
            }

            tracing::warn!(
                "Primary unavailable, waiting for failover... ({:.1}s elapsed, {:.1}s timeout)",
                start.elapsed().as_secs_f64(),
                timeout.as_secs_f64()
            );

            // Wait before retry (jittered exponential backoff, H-05).
            state
                .metrics
                .reconnect_attempts
                .fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Self::reconnect_backoff(attempt, backoff_seed)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    /// Choose one index among the eligible read nodes for `strategy` (H-03).
    ///
    /// Pure and unit-tested: callers pass parallel per-node arrays (weights,
    /// measured health-probe latency, sessions currently attached) plus the
    /// monotonic round-robin ticket. `ticket` also seeds the Random and
    /// PowerOfTwo samplers so the sequence stays deterministic in tests while
    /// behaving as a uniform stream in production.
    ///
    /// Strategies that need load accounting are only invoked after the caller
    /// gathered it, so the default RoundRobin path stays O(1).
    pub(super) fn pick_read_node(
        strategy: crate::config::Strategy,
        weights: &[u32],
        latency_ms: &[f64],
        attached: &[u64],
        ticket: u64,
    ) -> Option<usize> {
        use crate::config::Strategy;
        let len = weights.len();
        if len == 0 || latency_ms.len() != len || attached.len() != len {
            return None;
        }
        let pick_pair = |t: u64, len: usize| -> (usize, usize) {
            let i = (splitmix64(t) % len as u64) as usize;
            let mut j = (splitmix64(t ^ 0x5deece66d) % len as u64) as usize;
            if j == i {
                j = (j + 1) % len;
            }
            (i, j)
        };

        match strategy {
            Strategy::RoundRobin => Some((ticket % len as u64) as usize),
            Strategy::WeightedRoundRobin => {
                let total: u64 = weights.iter().map(|w| *w as u64).sum();
                if total == 0 {
                    return Some((ticket % len as u64) as usize);
                }
                let mut target = ticket % total;
                for (i, w) in weights.iter().enumerate() {
                    if target < *w as u64 {
                        return Some(i);
                    }
                    target -= *w as u64;
                }
                Some(len - 1)
            }
            Strategy::LeastConnections | Strategy::PowerOfTwo => {
                let score = |i: usize| (attached[i], latency_ms[i]);
                if strategy == Strategy::LeastConnections {
                    return (0..len).min_by(|a, b| {
                        score(*a)
                            .partial_cmp(&score(*b))
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then(a.cmp(b))
                    });
                }
                let (i, j) = pick_pair(ticket, len);
                let (a, b) = (score(i), score(j));
                Some(
                    if a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
                        != std::cmp::Ordering::Greater
                    {
                        i
                    } else {
                        j
                    },
                )
            }
            Strategy::LatencyBased => (0..len).min_by(|a, b| {
                latency_ms[*a]
                    .partial_cmp(&latency_ms[*b])
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.cmp(b))
            }),
            Strategy::Random => Some((splitmix64(ticket) % len as u64) as usize),
        }
    }

    /// Select node for read operations with load balancing
    pub(super) async fn select_read_node(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // If in transaction, stick to current node
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(node) = session.current_node.read().await.clone() {
                return Ok(node);
            }
        }
        if let Some(addr) = Self::pick_standby(session, state, config, false).await {
            return Ok(addr);
        }
        // Fall back to primary if no healthy standbys
        Self::select_node(session, state, config).await
    }

    /// Pick a node that may serve reads right now — a healthy, enabled
    /// standby or read replica that is read-eligible (circuit closed, lag
    /// within `[lag_routing]`) — with `[load_balancer] read_strategy`, and pin
    /// the session to it. `sync_only` keeps synchronous standbys only. `None`
    /// when no node qualifies.
    pub(super) async fn pick_standby(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        sync_only: bool,
    ) -> Option<String> {
        let members = if sync_only {
            state.primary_tracker.provider_members()
        } else {
            Vec::new()
        };
        let sync_ok = |addr: &str| !sync_only || Self::is_sync_standby(config, &members, addr);

        // H-01 step 3: under a topology provider the published snapshot names
        // the read set, so a promotion moves reads too (the demoted former
        // primary serves them, the new leader stops being treated as a
        // standby) without a proxy.toml edit.
        if state.authoritative_topology {
            let snapshot = state.routing.load();
            let mut candidates: Vec<&str> = Vec::with_capacity(snapshot.replicas.len());
            let mut weights: Vec<u32> = Vec::with_capacity(snapshot.replicas.len());
            let mut latency: Vec<f64> = Vec::with_capacity(snapshot.replicas.len());
            for r in &snapshot.replicas {
                if config
                    .nodes
                    .iter()
                    .any(|n| n.enabled && n.address() == r.address)
                    && Self::read_eligible(state, config, &r.address, r.lag_bytes)
                    && sync_ok(&r.address)
                {
                    candidates.push(&r.address);
                    weights.push(r.weight);
                    latency.push(r.latency_ms);
                }
            }
            return Self::pin_read_target(
                session,
                state,
                config,
                &candidates,
                |a| *a,
                &weights,
                &latency,
            )
            .await;
        }

        // Get healthy nodes (prefer standbys for reads)
        let health = state.health.load_full();
        let healthy_standbys: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| {
                n.enabled
                    && (n.role == NodeRole::Standby || n.role == NodeRole::ReadReplica)
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false)
                    && Self::read_eligible(
                        state,
                        config,
                        n.address(),
                        health
                            .get(n.address())
                            .and_then(|h| h.replication_lag_bytes),
                    )
                    && sync_ok(n.address())
            })
            .collect();
        let weights: Vec<u32> = healthy_standbys.iter().map(|n| n.weight).collect();
        let latency: Vec<f64> = healthy_standbys
            .iter()
            .map(|n| health.get(n.address()).map(|h| h.latency_ms).unwrap_or(0.0))
            .collect();
        Self::pin_read_target(
            session,
            state,
            config,
            &healthy_standbys,
            |n| n.address(),
            &weights,
            &latency,
        )
        .await
    }

    /// Whether `addr` may still serve a session's offloaded reads (the sticky
    /// read node): the same tests [`Self::pick_standby`] applies, for one node.
    pub(super) fn still_read_target(
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        addr: &str,
        sync_only: bool,
    ) -> bool {
        if Self::is_write_target(state, config, addr) {
            return false;
        }
        if sync_only
            && !Self::is_sync_standby(config, &state.primary_tracker.provider_members(), addr)
        {
            return false;
        }
        if state.authoritative_topology {
            let snapshot = state.routing.load();
            return snapshot.replicas.iter().any(|r| {
                r.address == addr
                    && config
                        .nodes
                        .iter()
                        .any(|n| n.enabled && n.address() == addr)
                    && Self::read_eligible(state, config, addr, r.lag_bytes)
            });
        }
        let health = state.health.load_full();
        let Some(h) = health.get(addr) else {
            return false;
        };
        h.healthy
            && config.nodes.iter().any(|n| {
                n.enabled
                    && n.address() == addr
                    && (n.role == NodeRole::Standby || n.role == NodeRole::ReadReplica)
            })
            && Self::read_eligible(state, config, addr, h.replication_lag_bytes)
    }

    /// Whether `addr` replicates synchronously: `[[nodes]] synchronous =
    /// true`, or the provider (Patroni) reports it as `sync_standby`.
    pub(super) fn is_sync_standby(
        config: &ProxyConfig,
        members: &[crate::primary_tracker::TopologyMember],
        addr: &str,
    ) -> bool {
        config
            .nodes
            .iter()
            .any(|n| n.synchronous && n.address() == addr)
            || members
                .iter()
                .any(|m| m.role == "sync_standby" && m.address == addr)
    }

    /// The session's home for statements the primary must serve, found
    /// without waiting: the provider's leader while its lease is valid, or
    /// (static topology) a healthy configured primary. `None` when there is
    /// none right now.
    pub(super) fn home_node(state: &ServerState, config: &ProxyConfig) -> Option<String> {
        if state.authoritative_topology {
            let snapshot = state.routing.load();
            return snapshot
                .write_leader(std::time::Instant::now())
                .filter(|a| config.nodes.iter().any(|n| n.enabled && n.address() == *a))
                .map(str::to_string);
        }
        let health = state.health.load_full();
        config
            .nodes
            .iter()
            .find(|n| {
                n.enabled
                    && n.role == NodeRole::Primary
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false)
            })
            .map(|n| n.address().to_string())
    }

    /// Whether a healthy read candidate may serve reads right now: its
    /// circuit is not open and it does not lag beyond `[lag_routing]`.
    fn read_eligible(
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        addr: &str,
        lag_bytes: Option<u64>,
    ) -> bool {
        // Drop a standby whose circuit is open so reads avoid it.
        #[cfg(feature = "circuit-breaker")]
        if Self::circuit_is_open(state, addr) {
            return false;
        }
        // Drop a standby lagging beyond the configured byte threshold.
        #[cfg(feature = "lag-routing")]
        if Self::lag_excludes_standby(
            lag_bytes,
            config.lag_routing.max_lag_bytes,
            config.lag_routing.require_known_lag,
        ) {
            return false;
        }
        let _ = (state, config, addr, lag_bytes);
        true
    }

    /// Pick one of the read candidates (parallel to `weights` / `latency`;
    /// `addr_of` names each) with `[load_balancer] read_strategy` and pin the
    /// session to it. `None` when there is no candidate.
    async fn pin_read_target<C>(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        candidates: &[C],
        addr_of: impl Fn(&C) -> &str,
        weights: &[u32],
        latency: &[f64],
    ) -> Option<String> {
        if candidates.is_empty() {
            return None;
        }
        let strategy = config.load_balancer.read_strategy;
        // Load accounting is only gathered for the strategies that use it,
        // so the default RoundRobin path stays O(1). "Attached" is the
        // best-effort load signal available on the data path: how many
        // live sessions currently have this node as their backend.
        let attached: Vec<u64> =
            if matches!(strategy, Strategy::LeastConnections | Strategy::PowerOfTwo) {
                candidates
                    .iter()
                    .map(|c| {
                        let addr = addr_of(c);
                        state
                            .sessions
                            .iter()
                            .filter(|s| {
                                s.current_node
                                    .try_read()
                                    .ok()
                                    .and_then(|g| g.clone())
                                    .map(|cur| cur == addr)
                                    .unwrap_or(false)
                            })
                            .count() as u64
                    })
                    .collect()
            } else {
                vec![0; candidates.len()]
            };

        let ticket = state.lb_state.rr_counter.fetch_add(1, Ordering::Relaxed);
        let index = Self::pick_read_node(strategy, weights, latency, &attached, ticket)
            .unwrap_or_else(|| (ticket as usize) % candidates.len());
        let node_addr = addr_of(&candidates[index]).to_string();

        let mut current = session.current_node.write().await;
        *current = Some(node_addr.clone());
        Some(node_addr)
    }

    /// Select a backend node for the request
    /// Select a backend node for initial connection
    /// Prefers primary but falls back to standbys for read connections
    pub(super) async fn select_node(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // If in a transaction, stick to the current node
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(node) = session.current_node.read().await.clone() {
                return Ok(node);
            }
        }

        // Get healthy nodes
        let health = state.health.load_full();
        let healthy_nodes: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| n.enabled && health.get(n.address()).map(|h| h.healthy).unwrap_or(false))
            .collect();

        if healthy_nodes.is_empty() {
            return Err(ProxyError::NoHealthyNodes);
        }

        // Under an authoritative provider the initial connection goes to the
        // provider's leader, not to whichever node is configured as primary
        // (which the provider may have demoted). With no valid leader the
        // historical fallback below applies; the write path re-checks the
        // target with `is_write_target` before any write is sent.
        if state.authoritative_topology {
            if let Some(addr) = Self::snapshot_leader(state, config) {
                if healthy_nodes.iter().any(|n| n.address() == addr) {
                    let mut current = session.current_node.write().await;
                    *current = Some(addr.clone());
                    return Ok(addr);
                }
            }
        }

        // Try to find healthy primary first
        if let Some(primary) = healthy_nodes.iter().find(|n| n.role == NodeRole::Primary) {
            let node_addr = primary.address().to_string();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to standby if primary is unavailable
        // (Initial connection will work, writes will use write timeout to wait for primary)
        if let Some(standby) = healthy_nodes.iter().find(|n| n.role == NodeRole::Standby) {
            tracing::warn!("Primary unavailable, connecting to standby for initial session");
            let node_addr = standby.address().to_string();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // No nodes available
        Err(ProxyError::NoHealthyNodes)
    }
}
