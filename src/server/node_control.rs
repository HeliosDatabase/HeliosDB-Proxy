//! The admin API's path into node eligibility (Sprinter b251750f1d54):
//! `POST /nodes/{addr}/disable|enable` and `POST /api/chaos` used to flip the
//! admin's display copy of node health, which the server overwrote within a
//! second, so no admin node control ever reached routing. They now write the
//! server's override map and republish health and routing synchronously.
use super::*;
use crate::node_overrides::{NodeControl, NodeOverride, NodeOverrides};

/// Handle the daemon hands the admin API at startup.
pub struct NodeControlHandle {
    state: Arc<ServerState>,
}

impl ProxyServer {
    /// A [`NodeControl`] bound to this server, for an embedding admin API.
    pub fn node_control(&self) -> Arc<dyn NodeControl> {
        Self::node_control_for(&self.state)
    }

    pub(super) fn node_control_for(state: &Arc<ServerState>) -> Arc<dyn NodeControl> {
        Arc::new(NodeControlHandle {
            state: state.clone(),
        })
    }

    /// Operator overrides win over the probes: an overridden node publishes
    /// as unhealthy whatever its probe said, so a passing probe cannot
    /// re-admit it. Every writer of `ServerState::health` calls this on the
    /// map it is about to store.
    pub(super) fn apply_node_overrides(
        overrides: &NodeOverrides,
        next: &mut HashMap<String, NodeHealth>,
    ) {
        if overrides.is_empty() {
            return;
        }
        for (addr, node_health) in next.iter_mut() {
            if overrides.is_overridden(addr) {
                node_health.healthy = false;
            }
        }
    }

    /// Apply `override_` to `addr` (or clear it with `None`), then republish
    /// health and routing under the health write lock so the change cannot
    /// race a probe sweep or an in-band demotion. Returns the node's effective
    /// health afterwards.
    ///
    /// Clearing re-admits the node at once only when its probe counters
    /// already say it is fine (no failure in progress and `success_threshold`
    /// consecutive successes, exactly what the next sweep would conclude);
    /// otherwise the node stays out until its next probe passes, so an
    /// `enable` never routes to a backend nothing has checked.
    pub(super) fn apply_override(
        state: &Arc<ServerState>,
        addr: &str,
        override_: Option<NodeOverride>,
    ) -> Result<bool> {
        let config = state.live_config.load_full();
        if !config.nodes.iter().any(|n| n.address() == addr) {
            return Err(ProxyError::Config(format!("Node not found: {}", addr)));
        }
        let _writers = state.health_write.lock();
        match &override_ {
            Some(o) => {
                state.node_overrides.set(addr, o.clone());
            }
            None => {
                state.node_overrides.clear(addr);
            }
        }
        let mut next = (*state.health.load_full()).clone();
        let mut healthy_after = false;
        if let Some(nh) = next.get_mut(addr) {
            match &override_ {
                Some(o) => {
                    if nh.healthy {
                        tracing::warn!(
                            node = %addr,
                            kind = o.kind.as_str(),
                            note = %o.note,
                            "admin override: node taken out of routing"
                        );
                    }
                    nh.healthy = false;
                }
                None => {
                    let proven = nh.failure_count == 0
                        && nh.success_count >= config.health.success_threshold.max(1);
                    if proven && !nh.healthy {
                        nh.healthy = true;
                    }
                    tracing::info!(
                        node = %addr,
                        healthy = nh.healthy,
                        "admin override cleared: node eligible again{}",
                        if nh.healthy { "" } else { " once its next probe passes" }
                    );
                }
            }
            healthy_after = nh.healthy;
        }
        Self::apply_node_overrides(&state.node_overrides, &mut next);
        state.health.store(Arc::new(next));
        Self::publish_routing(state);
        Ok(healthy_after)
    }
}

impl NodeControl for NodeControlHandle {
    fn set_override(&self, addr: &str, override_: Option<NodeOverride>) -> Result<bool> {
        ProxyServer::apply_override(&self.state, addr, override_)
    }

    fn overrides(&self) -> HashMap<String, NodeOverride> {
        self.state.node_overrides.snapshot()
    }
}
