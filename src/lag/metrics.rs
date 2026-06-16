//! Lag Routing Metrics
//!
//! Metrics and statistics for lag-aware routing decisions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::monitor::NodeId;
use super::SyncMode;

/// Lag metrics collector
pub struct LagMetrics {
    /// Total routing decisions
    total_decisions: AtomicU64,

    /// Decisions that went to primary
    primary_decisions: AtomicU64,

    /// Decisions that went to standby
    standby_decisions: AtomicU64,

    /// Fallback to primary due to lag
    fallback_count: AtomicU64,

    /// RYW-triggered primary routes
    ryw_fallback_count: AtomicU64,

    /// No eligible nodes
    no_nodes_count: AtomicU64,

    /// Per-node statistics
    node_stats: DashMap<NodeId, NodeLagStats>,

    /// Per-sync-mode statistics
    sync_mode_stats: DashMap<SyncMode, AtomicU64>,

    /// Routing decision timing histogram (microseconds)
    decision_times_us: RwLock<Vec<u64>>,

    /// Maximum samples to keep for timing
    max_timing_samples: usize,

    /// Start time for uptime calculation
    started_at: Instant,
}

impl LagMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            total_decisions: AtomicU64::new(0),
            primary_decisions: AtomicU64::new(0),
            standby_decisions: AtomicU64::new(0),
            fallback_count: AtomicU64::new(0),
            ryw_fallback_count: AtomicU64::new(0),
            no_nodes_count: AtomicU64::new(0),
            node_stats: DashMap::new(),
            sync_mode_stats: DashMap::new(),
            decision_times_us: RwLock::new(Vec::with_capacity(1000)),
            max_timing_samples: 1000,
            started_at: Instant::now(),
        }
    }

    /// Record a routing decision to primary
    pub fn record_primary_decision(&self, elapsed: Duration, reason: &str) {
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
        self.primary_decisions.fetch_add(1, Ordering::Relaxed);

        // Check for ryw first (more specific case) since "ryw fallback" contains "fallback"
        if reason.contains("ryw") || reason.contains("RYW") {
            self.ryw_fallback_count.fetch_add(1, Ordering::Relaxed);
        } else if reason.contains("fallback") {
            self.fallback_count.fetch_add(1, Ordering::Relaxed);
        }

        self.record_timing(elapsed);
    }

    /// Record a routing decision to standby
    pub fn record_standby_decision(
        &self,
        node_id: &str,
        sync_mode: SyncMode,
        lag_ms: u64,
        elapsed: Duration,
    ) {
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
        self.standby_decisions.fetch_add(1, Ordering::Relaxed);

        // Update per-node stats
        self.node_stats
            .entry(node_id.to_string())
            .and_modify(|stats| stats.record_decision(lag_ms))
            .or_insert_with(|| {
                let mut stats = NodeLagStats::new(sync_mode);
                stats.record_decision(lag_ms);
                stats
            });

        // Update per-sync-mode stats
        self.sync_mode_stats
            .entry(sync_mode)
            .and_modify(|count| {
                count.fetch_add(1, Ordering::Relaxed);
            })
            .or_insert_with(|| AtomicU64::new(1));

        self.record_timing(elapsed);
    }

    /// Record no eligible nodes
    pub fn record_no_nodes(&self, elapsed: Duration) {
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
        self.no_nodes_count.fetch_add(1, Ordering::Relaxed);
        self.record_timing(elapsed);
    }

    /// Record decision timing
    fn record_timing(&self, elapsed: Duration) {
        let us = elapsed.as_micros() as u64;
        let mut times = self.decision_times_us.write();

        if times.len() >= self.max_timing_samples {
            // Remove oldest half when full
            times.drain(0..self.max_timing_samples / 2);
        }
        times.push(us);
    }

    /// Get current statistics snapshot
    pub fn get_stats(&self) -> LagStatsSnapshot {
        let total = self.total_decisions.load(Ordering::Relaxed);
        let primary = self.primary_decisions.load(Ordering::Relaxed);
        let standby = self.standby_decisions.load(Ordering::Relaxed);
        let fallback = self.fallback_count.load(Ordering::Relaxed);
        let ryw_fallback = self.ryw_fallback_count.load(Ordering::Relaxed);
        let no_nodes = self.no_nodes_count.load(Ordering::Relaxed);

        // Calculate timing stats
        let times = self.decision_times_us.read();
        let (avg_time_us, p50_time_us, p99_time_us) = if times.is_empty() {
            (0, 0, 0)
        } else {
            let mut sorted = times.clone();
            sorted.sort_unstable();

            let avg = sorted.iter().sum::<u64>() / sorted.len() as u64;
            let p50 = sorted[sorted.len() / 2];
            let p99_idx = (sorted.len() as f64 * 0.99) as usize;
            let p99 = sorted.get(p99_idx).copied().unwrap_or(sorted[sorted.len() - 1]);

            (avg, p50, p99)
        };

        // Collect per-node stats
        let node_stats: HashMap<_, _> = self
            .node_stats
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().snapshot()))
            .collect();

        // Collect per-sync-mode stats
        let sync_mode_counts: HashMap<_, _> = self
            .sync_mode_stats
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Relaxed)))
            .collect();

        LagStatsSnapshot {
            total_decisions: total,
            primary_decisions: primary,
            standby_decisions: standby,
            fallback_count: fallback,
            ryw_fallback_count: ryw_fallback,
            no_nodes_count: no_nodes,
            avg_decision_time_us: avg_time_us,
            p50_decision_time_us: p50_time_us,
            p99_decision_time_us: p99_time_us,
            node_stats,
            sync_mode_counts,
            uptime_secs: self.started_at.elapsed().as_secs(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_decisions.store(0, Ordering::Relaxed);
        self.primary_decisions.store(0, Ordering::Relaxed);
        self.standby_decisions.store(0, Ordering::Relaxed);
        self.fallback_count.store(0, Ordering::Relaxed);
        self.ryw_fallback_count.store(0, Ordering::Relaxed);
        self.no_nodes_count.store(0, Ordering::Relaxed);
        self.node_stats.clear();
        self.sync_mode_stats.clear();
        self.decision_times_us.write().clear();
    }
}

impl Default for LagMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LagMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LagMetrics")
            .field("total_decisions", &self.total_decisions.load(Ordering::Relaxed))
            .field("node_count", &self.node_stats.len())
            .finish()
    }
}

/// Per-node lag statistics
pub struct NodeLagStats {
    /// Sync mode of this node
    sync_mode: SyncMode,

    /// Total decisions routed to this node
    total_decisions: AtomicU64,

    /// Sum of lag values (for average calculation)
    total_lag_ms: AtomicU64,

    /// Minimum observed lag
    min_lag_ms: AtomicU64,

    /// Maximum observed lag
    max_lag_ms: AtomicU64,

    /// Recent lag samples
    recent_lags: RwLock<Vec<u64>>,
}

impl NodeLagStats {
    fn new(sync_mode: SyncMode) -> Self {
        Self {
            sync_mode,
            total_decisions: AtomicU64::new(0),
            total_lag_ms: AtomicU64::new(0),
            min_lag_ms: AtomicU64::new(u64::MAX),
            max_lag_ms: AtomicU64::new(0),
            recent_lags: RwLock::new(Vec::with_capacity(100)),
        }
    }

    fn record_decision(&mut self, lag_ms: u64) {
        self.total_decisions.fetch_add(1, Ordering::Relaxed);
        self.total_lag_ms.fetch_add(lag_ms, Ordering::Relaxed);

        // Update min
        let mut current_min = self.min_lag_ms.load(Ordering::Relaxed);
        while lag_ms < current_min {
            match self.min_lag_ms.compare_exchange_weak(
                current_min,
                lag_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max
        let mut current_max = self.max_lag_ms.load(Ordering::Relaxed);
        while lag_ms > current_max {
            match self.max_lag_ms.compare_exchange_weak(
                current_max,
                lag_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }

        // Add to recent samples
        let mut recent = self.recent_lags.write();
        if recent.len() >= 100 {
            recent.remove(0);
        }
        recent.push(lag_ms);
    }

    fn snapshot(&self) -> NodeLagStatsSnapshot {
        let total = self.total_decisions.load(Ordering::Relaxed);
        let total_lag = self.total_lag_ms.load(Ordering::Relaxed);
        let min = self.min_lag_ms.load(Ordering::Relaxed);
        let max = self.max_lag_ms.load(Ordering::Relaxed);

        let avg = total_lag.checked_div(total).unwrap_or(0);

        NodeLagStatsSnapshot {
            sync_mode: self.sync_mode,
            total_decisions: total,
            avg_lag_ms: avg,
            min_lag_ms: if min == u64::MAX { 0 } else { min },
            max_lag_ms: max,
        }
    }
}

/// Snapshot of node lag statistics
#[derive(Debug, Clone)]
pub struct NodeLagStatsSnapshot {
    /// Node's sync mode
    pub sync_mode: SyncMode,

    /// Total routing decisions to this node
    pub total_decisions: u64,

    /// Average lag when routed
    pub avg_lag_ms: u64,

    /// Minimum observed lag
    pub min_lag_ms: u64,

    /// Maximum observed lag
    pub max_lag_ms: u64,
}

/// Snapshot of overall lag routing statistics
#[derive(Debug, Clone)]
pub struct LagStatsSnapshot {
    /// Total routing decisions made
    pub total_decisions: u64,

    /// Decisions that went to primary
    pub primary_decisions: u64,

    /// Decisions that went to standby
    pub standby_decisions: u64,

    /// Fallback to primary due to lag
    pub fallback_count: u64,

    /// RYW-triggered primary routes
    pub ryw_fallback_count: u64,

    /// No eligible nodes found
    pub no_nodes_count: u64,

    /// Average decision time in microseconds
    pub avg_decision_time_us: u64,

    /// P50 decision time in microseconds
    pub p50_decision_time_us: u64,

    /// P99 decision time in microseconds
    pub p99_decision_time_us: u64,

    /// Per-node statistics
    pub node_stats: HashMap<NodeId, NodeLagStatsSnapshot>,

    /// Per-sync-mode decision counts
    pub sync_mode_counts: HashMap<SyncMode, u64>,

    /// Uptime in seconds
    pub uptime_secs: u64,
}

impl LagStatsSnapshot {
    /// Calculate standby routing percentage
    pub fn standby_percentage(&self) -> f64 {
        if self.total_decisions == 0 {
            return 0.0;
        }
        self.standby_decisions as f64 / self.total_decisions as f64 * 100.0
    }

    /// Calculate fallback percentage
    pub fn fallback_percentage(&self) -> f64 {
        if self.total_decisions == 0 {
            return 0.0;
        }
        self.fallback_count as f64 / self.total_decisions as f64 * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = LagMetrics::new();
        let stats = metrics.get_stats();

        assert_eq!(stats.total_decisions, 0);
        assert_eq!(stats.primary_decisions, 0);
        assert_eq!(stats.standby_decisions, 0);
    }

    #[test]
    fn test_record_primary_decision() {
        let metrics = LagMetrics::new();

        metrics.record_primary_decision(Duration::from_micros(50), "direct");
        metrics.record_primary_decision(Duration::from_micros(60), "fallback");
        metrics.record_primary_decision(Duration::from_micros(70), "ryw fallback");

        let stats = metrics.get_stats();
        assert_eq!(stats.total_decisions, 3);
        assert_eq!(stats.primary_decisions, 3);
        assert_eq!(stats.fallback_count, 1);
        assert_eq!(stats.ryw_fallback_count, 1);
    }

    #[test]
    fn test_record_standby_decision() {
        let metrics = LagMetrics::new();

        metrics.record_standby_decision("node-1", SyncMode::Sync, 5, Duration::from_micros(30));
        metrics.record_standby_decision("node-1", SyncMode::Sync, 10, Duration::from_micros(40));
        metrics.record_standby_decision("node-2", SyncMode::Async, 100, Duration::from_micros(50));

        let stats = metrics.get_stats();
        assert_eq!(stats.total_decisions, 3);
        assert_eq!(stats.standby_decisions, 3);
        assert_eq!(stats.node_stats.len(), 2);

        let node1_stats = stats.node_stats.get("node-1").unwrap();
        assert_eq!(node1_stats.total_decisions, 2);
        assert_eq!(node1_stats.min_lag_ms, 5);
        assert_eq!(node1_stats.max_lag_ms, 10);
    }

    #[test]
    fn test_timing_stats() {
        let metrics = LagMetrics::new();

        for i in 1..=100 {
            metrics.record_primary_decision(Duration::from_micros(i * 10), "test");
        }

        let stats = metrics.get_stats();
        assert!(stats.avg_decision_time_us > 0);
        assert!(stats.p50_decision_time_us > 0);
        assert!(stats.p99_decision_time_us >= stats.p50_decision_time_us);
    }

    #[test]
    fn test_sync_mode_counts() {
        let metrics = LagMetrics::new();

        metrics.record_standby_decision("n1", SyncMode::Sync, 5, Duration::from_micros(30));
        metrics.record_standby_decision("n2", SyncMode::Sync, 5, Duration::from_micros(30));
        metrics.record_standby_decision("n3", SyncMode::Async, 100, Duration::from_micros(50));

        let stats = metrics.get_stats();
        assert_eq!(stats.sync_mode_counts.get(&SyncMode::Sync), Some(&2));
        assert_eq!(stats.sync_mode_counts.get(&SyncMode::Async), Some(&1));
    }

    #[test]
    fn test_reset_metrics() {
        let metrics = LagMetrics::new();

        metrics.record_primary_decision(Duration::from_micros(50), "test");
        metrics.record_standby_decision("node-1", SyncMode::Async, 100, Duration::from_micros(50));

        assert!(metrics.get_stats().total_decisions > 0);

        metrics.reset();

        let stats = metrics.get_stats();
        assert_eq!(stats.total_decisions, 0);
        assert_eq!(stats.node_stats.len(), 0);
    }

    #[test]
    fn test_percentages() {
        let stats = LagStatsSnapshot {
            total_decisions: 100,
            primary_decisions: 20,
            standby_decisions: 80,
            fallback_count: 10,
            ryw_fallback_count: 5,
            no_nodes_count: 0,
            avg_decision_time_us: 50,
            p50_decision_time_us: 45,
            p99_decision_time_us: 100,
            node_stats: HashMap::new(),
            sync_mode_counts: HashMap::new(),
            uptime_secs: 3600,
        };

        assert!((stats.standby_percentage() - 80.0).abs() < 0.01);
        assert!((stats.fallback_percentage() - 10.0).abs() < 0.01);
    }
}
