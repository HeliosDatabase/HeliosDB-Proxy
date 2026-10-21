use super::*;
use crate::routing_snapshot::{Authority, NodeFacts, RoutingSnapshot};

impl ProxyServer {
    /// Build the routing snapshot (H-01 step 3) from the live configuration,
    /// the current health map and, when a provider is authoritative, the
    /// tracker's leader.
    pub(super) fn take_routing_snapshot(
        state: &ServerState,
        now: std::time::Instant,
    ) -> RoutingSnapshot {
        let config = state.live_config.load();
        let health = state.health.load();
        let facts: Vec<NodeFacts<'_>> = config
            .nodes
            .iter()
            .map(|n| {
                let h = health.get(n.address());
                NodeFacts {
                    address: n.address(),
                    role: n.role,
                    // An operator override holds the node out as if it were
                    // not enabled; under a provider that also drops the leader.
                    enabled: n.enabled && !state.node_overrides.is_overridden(n.address()),
                    healthy: h.map(|h| h.healthy).unwrap_or(false),
                    weight: n.weight,
                    latency_ms: h.map(|h| h.latency_ms).unwrap_or(0.0),
                    lag_bytes: h.and_then(|h| h.replication_lag_bytes),
                }
            })
            .collect();
        if !state.authoritative_topology {
            return RoutingSnapshot::build(&facts, Authority::Static, now);
        }
        let tracker = &state.primary_tracker;
        let leader = if tracker.authority_valid() {
            tracker.get_primary_address()
        } else {
            None
        };
        // A provider-backed authority expires with its lease; a manual one
        // (no provider) does not.
        let lease = if tracker.has_provider() {
            tracker.lease_remaining()
        } else {
            None
        };
        let authority = Authority::Provider {
            leader: leader.as_deref(),
            epoch: tracker.get_epoch(),
            lease,
        };
        RoutingSnapshot::build(&facts, authority, now)
    }

    /// Publish a fresh routing snapshot. When the leader moved to another
    /// node, close the idle pooled connections to the old leader: a demoted
    /// server is typically restarted by its HA manager, so they are likely
    /// dead, and none of them may be handed out for a write.
    pub(super) fn publish_routing(state: &ServerState) {
        let moved = state
            .routing
            .publish_with(|| Self::take_routing_snapshot(state, std::time::Instant::now()));
        if let Some(old) = moved {
            let evicted = Self::evict_idle_backends(state, &old);
            state.routing.record_evicted(evicted);
            tracing::info!(
                old_leader = %old,
                new_leader = ?state.routing.load().leader,
                evicted,
                "routing: leader moved; closed idle pooled connections to the old leader"
            );
        }
    }

    fn evict_idle_backends(state: &ServerState, node: &str) -> usize {
        #[cfg(feature = "pool-modes")]
        if let Some(pool) = state.backend_pool.as_ref() {
            return pool.evict_idle_for_node(node);
        }
        let _ = (state, node);
        0
    }

    /// The published write leader, if it is an enabled node of the session's
    /// `config` (a session keeps the configuration it started with across a
    /// SIGHUP, the snapshot follows the live one).
    pub(super) fn snapshot_leader(state: &ServerState, config: &ProxyConfig) -> Option<String> {
        let snapshot = state.routing.load();
        let leader = snapshot.write_leader(std::time::Instant::now())?;
        config
            .nodes
            .iter()
            .any(|n| n.enabled && n.address() == leader)
            .then(|| leader.to_string())
    }

    /// Keep the routing snapshot in step with the authoritative tracker:
    /// republish on every tracker event and on the tracker's own cadence,
    /// which refreshes the leader's lease bound as often as the tracker
    /// refreshes the lease itself. `None` for a static topology, whose
    /// snapshot changes only with health and configuration.
    pub(super) fn spawn_routing_refresher(&self) -> Option<tokio::task::JoinHandle<()>> {
        if !self.state.authoritative_topology {
            return None;
        }
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut events = state.primary_tracker.subscribe();
            let mut tick = tokio::time::interval(state.primary_tracker.tracking_interval());
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    event = events.recv() => {
                        if let Err(tokio::sync::broadcast::error::RecvError::Closed) = event {
                            return;
                        }
                    }
                    _ = tick.tick() => {}
                    _ = shutdown_rx.recv() => return,
                }
                Self::publish_routing(&state);
            }
        }))
    }
}
