//! `[load_balancer] latency_threshold_ms` (W02): standbys too slow to serve
//! offloaded reads.
//!
//! While the threshold is set, every health probe that succeeds folds its
//! latency into the node's smoothed latency (`latency_smoothing`, an
//! exponential moving average). A
//! node whose smoothed latency rises above the threshold is held out of
//! read selection until it falls below `latency_readmit_ratio` of it, so a
//! node near the threshold does not flap. A node never probed counts as
//! eligible; with no eligible standby the primary serves the read, as it
//! does when none is healthy. The probe pays the bookkeeping; a read pays
//! one atomic load of the published set, and only while the threshold is
//! set.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::LoadBalancerConfig;

#[derive(Debug, Default, Clone, Copy)]
struct GateNode {
    smoothed: Option<f64>,
    excluded: bool,
}

/// The gate's per-node state and the published set of held-out nodes.
#[derive(Default)]
pub(super) struct LatencyGate {
    nodes: parking_lot::Mutex<HashMap<String, GateNode>>,
    excluded: ArcSwap<HashSet<String>>,
}

impl LatencyGate {
    /// Fold the latencies of successful probes (`(node, ms)`) in, under the
    /// live `[load_balancer]`.
    pub(super) fn observe(&self, lb: &LoadBalancerConfig, samples: &[(String, f64)]) {
        if samples.is_empty() {
            return;
        }
        let mut nodes = self.nodes.lock();
        let mut changed = false;
        for (addr, ms) in samples {
            let node = nodes.entry(addr.clone()).or_default();
            node.smoothed = Some(lb.smooth_latency(node.smoothed, *ms));
            let excluded = lb.latency_excludes(node.excluded, node.smoothed);
            if excluded != node.excluded {
                changed = true;
                node.excluded = excluded;
                if excluded {
                    tracing::warn!(
                        target: "helios::routing",
                        node = %addr,
                        smoothed_ms = node.smoothed.unwrap_or_default(),
                        threshold_ms = lb.latency_threshold_ms,
                        "standby held out of read routing: smoothed probe latency above [load_balancer] latency_threshold_ms"
                    );
                } else {
                    tracing::info!(
                        target: "helios::routing",
                        node = %addr,
                        smoothed_ms = node.smoothed.unwrap_or_default(),
                        "standby serves reads again: smoothed probe latency below latency_readmit_ratio of the threshold"
                    );
                }
            }
        }
        if changed {
            let set: HashSet<String> = nodes
                .iter()
                .filter(|(_, n)| n.excluded)
                .map(|(addr, _)| addr.clone())
                .collect();
            self.excluded.store(Arc::new(set));
        }
    }

    /// Whether `addr` is held out of reads now.
    pub(super) fn excludes(&self, addr: &str) -> bool {
        self.excluded.load().contains(addr)
    }

    /// The smoothed latency of `addr`, if it was probed.
    #[cfg(test)]
    fn smoothed_ms(&self, addr: &str) -> Option<f64> {
        self.nodes.lock().get(addr).and_then(|n| n.smoothed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slow_node_goes_out_and_comes_back_below_the_readmit_ratio() {
        let gate = LatencyGate::default();
        let lb = LoadBalancerConfig {
            latency_threshold_ms: 100,
            latency_smoothing: 1.0,
            ..Default::default()
        };
        let probe = |ms: f64| vec![("s:5432".to_string(), ms)];
        assert!(!gate.excludes("s:5432"), "never probed: eligible");
        gate.observe(&lb, &probe(50.0));
        assert!(!gate.excludes("s:5432"));
        gate.observe(&lb, &probe(150.0));
        assert!(gate.excludes("s:5432"));
        gate.observe(&lb, &probe(90.0));
        assert!(gate.excludes("s:5432"), "not yet below 70 ms");
        gate.observe(&lb, &probe(60.0));
        assert!(!gate.excludes("s:5432"));
        assert_eq!(gate.smoothed_ms("s:5432"), Some(60.0));
        // Threshold off: nothing is held out.
        gate.observe(&lb, &probe(500.0));
        assert!(gate.excludes("s:5432"));
        let off = LoadBalancerConfig::default();
        gate.observe(&off, &probe(500.0));
        assert!(!gate.excludes("s:5432"));
    }

    #[test]
    fn smoothing_rides_out_a_single_spike() {
        let gate = LatencyGate::default();
        let lb = LoadBalancerConfig {
            latency_threshold_ms: 100,
            ..Default::default()
        };
        let probe = |ms: f64| vec![("s:5432".to_string(), ms)];
        gate.observe(&lb, &probe(10.0));
        gate.observe(&lb, &probe(300.0));
        assert!(!gate.excludes("s:5432"), "0.7*10 + 0.3*300 = 97");
        gate.observe(&lb, &probe(300.0));
        assert!(gate.excludes("s:5432"));
    }
}
