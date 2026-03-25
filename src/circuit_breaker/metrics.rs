//! Circuit Breaker Metrics
//!
//! Metrics collection and reporting for circuit breaker operations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;

use super::state::CircuitState;

/// Circuit breaker metrics collector
pub struct CircuitMetrics {
    /// Start time for uptime calculation
    start_time: Instant,

    /// Total requests allowed
    total_allowed: AtomicU64,

    /// Total requests rejected (circuit open)
    total_rejected: AtomicU64,

    /// Total circuit opens
    total_opens: AtomicU64,

    /// Total circuit closes
    total_closes: AtomicU64,

    /// Per-node metrics
    node_metrics: DashMap<String, NodeMetrics>,
}

impl CircuitMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_allowed: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_opens: AtomicU64::new(0),
            total_closes: AtomicU64::new(0),
            node_metrics: DashMap::new(),
        }
    }

    /// Record an allowed request
    pub fn record_allowed(&self, node_id: &str) {
        self.total_allowed.fetch_add(1, Ordering::SeqCst);
        self.get_or_create_node(node_id).record_allowed();
    }

    /// Record a rejected request
    pub fn record_rejected(&self, node_id: &str) {
        self.total_rejected.fetch_add(1, Ordering::SeqCst);
        self.get_or_create_node(node_id).record_rejected();
    }

    /// Record a circuit open event
    pub fn record_open(&self, node_id: &str) {
        self.total_opens.fetch_add(1, Ordering::SeqCst);
        self.get_or_create_node(node_id).record_open();
    }

    /// Record a circuit close event
    pub fn record_close(&self, node_id: &str) {
        self.total_closes.fetch_add(1, Ordering::SeqCst);
        self.get_or_create_node(node_id).record_close();
    }

    fn get_or_create_node(&self, node_id: &str) -> dashmap::mapref::one::RefMut<'_, String, NodeMetrics> {
        if !self.node_metrics.contains_key(node_id) {
            self.node_metrics
                .insert(node_id.to_string(), NodeMetrics::new());
        }
        self.node_metrics.get_mut(node_id).expect("just inserted")
    }

    /// Get total allowed requests
    pub fn total_allowed(&self) -> u64 {
        self.total_allowed.load(Ordering::SeqCst)
    }

    /// Get total rejected requests
    pub fn total_rejected(&self) -> u64 {
        self.total_rejected.load(Ordering::SeqCst)
    }

    /// Get total circuit opens
    pub fn total_opens(&self) -> u64 {
        self.total_opens.load(Ordering::SeqCst)
    }

    /// Get total circuit closes
    pub fn total_closes(&self) -> u64 {
        self.total_closes.load(Ordering::SeqCst)
    }

    /// Get rejection rate (0.0 - 1.0)
    pub fn rejection_rate(&self) -> f64 {
        let allowed = self.total_allowed.load(Ordering::SeqCst);
        let rejected = self.total_rejected.load(Ordering::SeqCst);
        let total = allowed + rejected;

        if total == 0 {
            0.0
        } else {
            rejected as f64 / total as f64
        }
    }

    /// Get uptime
    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Get metrics snapshot for a specific node
    pub fn get_node_metrics(&self, node_id: &str) -> Option<NodeMetricsSnapshot> {
        self.node_metrics
            .get(node_id)
            .map(|m| m.snapshot())
    }

    /// Get all node metrics
    pub fn get_all_node_metrics(&self) -> HashMap<String, NodeMetricsSnapshot> {
        self.node_metrics
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().snapshot()))
            .collect()
    }

    /// Get summary statistics
    pub fn get_stats(&self) -> MetricsStats {
        MetricsStats {
            uptime_secs: self.uptime().as_secs(),
            total_allowed: self.total_allowed(),
            total_rejected: self.total_rejected(),
            total_opens: self.total_opens(),
            total_closes: self.total_closes(),
            rejection_rate: self.rejection_rate(),
            node_count: self.node_metrics.len(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_allowed.store(0, Ordering::SeqCst);
        self.total_rejected.store(0, Ordering::SeqCst);
        self.total_opens.store(0, Ordering::SeqCst);
        self.total_closes.store(0, Ordering::SeqCst);
        self.node_metrics.clear();
    }
}

impl Default for CircuitMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-node metrics
struct NodeMetrics {
    allowed: AtomicU64,
    rejected: AtomicU64,
    opens: AtomicU64,
    closes: AtomicU64,
    last_open: parking_lot::RwLock<Option<Instant>>,
    last_close: parking_lot::RwLock<Option<Instant>>,
}

impl NodeMetrics {
    fn new() -> Self {
        Self {
            allowed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            opens: AtomicU64::new(0),
            closes: AtomicU64::new(0),
            last_open: parking_lot::RwLock::new(None),
            last_close: parking_lot::RwLock::new(None),
        }
    }

    fn record_allowed(&self) {
        self.allowed.fetch_add(1, Ordering::SeqCst);
    }

    fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::SeqCst);
    }

    fn record_open(&self) {
        self.opens.fetch_add(1, Ordering::SeqCst);
        *self.last_open.write() = Some(Instant::now());
    }

    fn record_close(&self) {
        self.closes.fetch_add(1, Ordering::SeqCst);
        *self.last_close.write() = Some(Instant::now());
    }

    fn snapshot(&self) -> NodeMetricsSnapshot {
        NodeMetricsSnapshot {
            allowed: self.allowed.load(Ordering::SeqCst),
            rejected: self.rejected.load(Ordering::SeqCst),
            opens: self.opens.load(Ordering::SeqCst),
            closes: self.closes.load(Ordering::SeqCst),
            last_open_ago: self.last_open.read().map(|t| t.elapsed()),
            last_close_ago: self.last_close.read().map(|t| t.elapsed()),
        }
    }
}

/// Snapshot of node metrics
#[derive(Debug, Clone)]
pub struct NodeMetricsSnapshot {
    pub allowed: u64,
    pub rejected: u64,
    pub opens: u64,
    pub closes: u64,
    pub last_open_ago: Option<std::time::Duration>,
    pub last_close_ago: Option<std::time::Duration>,
}

impl NodeMetricsSnapshot {
    /// Get rejection rate for this node
    pub fn rejection_rate(&self) -> f64 {
        let total = self.allowed + self.rejected;
        if total == 0 {
            0.0
        } else {
            self.rejected as f64 / total as f64
        }
    }

    /// Get total requests for this node
    pub fn total_requests(&self) -> u64 {
        self.allowed + self.rejected
    }
}

/// Summary metrics statistics
#[derive(Debug, Clone)]
pub struct MetricsStats {
    pub uptime_secs: u64,
    pub total_allowed: u64,
    pub total_rejected: u64,
    pub total_opens: u64,
    pub total_closes: u64,
    pub rejection_rate: f64,
    pub node_count: usize,
}

/// Statistics for all circuits
#[derive(Debug, Clone, Default)]
pub struct CircuitStats {
    /// Per-node statistics
    pub nodes: HashMap<String, NodeCircuitStats>,
    /// Count by state
    pub state_counts: HashMap<String, usize>,
    /// Total failure count across all nodes
    pub total_failures: u64,
    /// Total success count across all nodes
    pub total_successes: u64,
    /// Total open count across all nodes
    pub total_opens: u64,
}

impl CircuitStats {
    /// Add statistics for a node
    pub fn add_node_stats(
        &mut self,
        node_id: &str,
        state: CircuitState,
        failure_count: u32,
        open_count: u64,
        total_failures: u64,
        total_successes: u64,
    ) {
        let stats = NodeCircuitStats {
            state,
            failure_count,
            open_count,
            total_failures,
            total_successes,
        };

        self.nodes.insert(node_id.to_string(), stats);

        // Update state counts
        let state_str = state.to_string();
        *self.state_counts.entry(state_str).or_insert(0) += 1;

        // Update totals
        self.total_failures += total_failures;
        self.total_successes += total_successes;
        self.total_opens += open_count;
    }

    /// Get number of nodes in each state
    pub fn nodes_by_state(&self) -> HashMap<CircuitState, usize> {
        let mut result = HashMap::new();
        for stats in self.nodes.values() {
            *result.entry(stats.state).or_insert(0) += 1;
        }
        result
    }

    /// Get nodes in open state
    pub fn open_nodes(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, s)| s.state == CircuitState::Open)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get nodes in half-open state
    pub fn half_open_nodes(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, s)| s.state == CircuitState::HalfOpen)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get overall health percentage (0.0 - 1.0)
    pub fn health_percentage(&self) -> f64 {
        if self.nodes.is_empty() {
            return 1.0;
        }

        let closed_count = self
            .nodes
            .values()
            .filter(|s| s.state == CircuitState::Closed)
            .count();

        closed_count as f64 / self.nodes.len() as f64
    }
}

/// Statistics for a single node's circuit
#[derive(Debug, Clone)]
pub struct NodeCircuitStats {
    pub state: CircuitState,
    pub failure_count: u32,
    pub open_count: u64,
    pub total_failures: u64,
    pub total_successes: u64,
}

impl NodeCircuitStats {
    /// Get success rate for this node
    pub fn success_rate(&self) -> f64 {
        let total = self.total_failures + self.total_successes;
        if total == 0 {
            1.0
        } else {
            self.total_successes as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = CircuitMetrics::new();
        assert_eq!(metrics.total_allowed(), 0);
        assert_eq!(metrics.total_rejected(), 0);
        assert_eq!(metrics.rejection_rate(), 0.0);
    }

    #[test]
    fn test_metrics_recording() {
        let metrics = CircuitMetrics::new();

        metrics.record_allowed("node-1");
        metrics.record_allowed("node-1");
        metrics.record_rejected("node-1");

        assert_eq!(metrics.total_allowed(), 2);
        assert_eq!(metrics.total_rejected(), 1);
        assert!((metrics.rejection_rate() - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_metrics_per_node() {
        let metrics = CircuitMetrics::new();

        metrics.record_allowed("node-1");
        metrics.record_allowed("node-2");
        metrics.record_rejected("node-1");

        let node1 = metrics.get_node_metrics("node-1").unwrap();
        assert_eq!(node1.allowed, 1);
        assert_eq!(node1.rejected, 1);

        let node2 = metrics.get_node_metrics("node-2").unwrap();
        assert_eq!(node2.allowed, 1);
        assert_eq!(node2.rejected, 0);
    }

    #[test]
    fn test_metrics_stats() {
        let metrics = CircuitMetrics::new();

        metrics.record_allowed("node-1");
        metrics.record_open("node-1");

        let stats = metrics.get_stats();
        assert_eq!(stats.total_allowed, 1);
        assert_eq!(stats.total_opens, 1);
        assert_eq!(stats.node_count, 1);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = CircuitMetrics::new();

        metrics.record_allowed("node-1");
        metrics.record_rejected("node-1");

        assert_eq!(metrics.total_allowed(), 1);

        metrics.reset();

        assert_eq!(metrics.total_allowed(), 0);
        assert_eq!(metrics.total_rejected(), 0);
    }

    #[test]
    fn test_circuit_stats() {
        let mut stats = CircuitStats::default();

        stats.add_node_stats("node-1", CircuitState::Closed, 0, 5, 10, 100);
        stats.add_node_stats("node-2", CircuitState::Open, 3, 2, 5, 50);
        stats.add_node_stats("node-3", CircuitState::HalfOpen, 1, 1, 3, 30);

        assert_eq!(stats.nodes.len(), 3);
        assert_eq!(stats.open_nodes(), vec!["node-2"]);
        assert_eq!(stats.half_open_nodes(), vec!["node-3"]);
        assert!((stats.health_percentage() - 0.333).abs() < 0.01);
    }

    #[test]
    fn test_node_circuit_stats() {
        let stats = NodeCircuitStats {
            state: CircuitState::Closed,
            failure_count: 0,
            open_count: 5,
            total_failures: 10,
            total_successes: 90,
        };

        assert!((stats.success_rate() - 0.9).abs() < 0.01);
    }
}
