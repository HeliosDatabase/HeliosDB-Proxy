//! Lag Monitor - Continuous replication lag tracking
//!
//! Monitors replication lag across all standbys in real-time,
//! providing data for lag-aware routing decisions.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::config::{LagCalculation, LagRoutingConfig};
use super::SyncMode;

/// Unique identifier for a node
pub type NodeId = String;

/// Lag trend indicating whether lag is improving, stable, or degrading
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagTrend {
    /// Lag is decreasing
    Improving,
    /// Lag is stable within tolerance
    Stable,
    /// Lag is increasing
    Degrading,
    /// Not enough samples to determine trend
    Unknown,
}

impl Default for LagTrend {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for LagTrend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LagTrend::Improving => write!(f, "improving"),
            LagTrend::Stable => write!(f, "stable"),
            LagTrend::Degrading => write!(f, "degrading"),
            LagTrend::Unknown => write!(f, "unknown"),
        }
    }
}

/// Lag information for a single node
#[derive(Debug, Clone)]
pub struct LagInfo {
    /// Current LSN (Log Sequence Number) on this node
    pub current_lsn: u64,

    /// Lag in bytes (LSN difference from primary)
    pub lag_bytes: u64,

    /// Estimated lag in time
    pub lag_time: Duration,

    /// When this info was last updated
    pub updated_at: Instant,

    /// Lag trend (improving, stable, degrading)
    pub trend: LagTrend,

    /// Node's sync mode
    pub sync_mode: SyncMode,

    /// Whether the node is considered healthy based on lag
    pub healthy: bool,
}

impl Default for LagInfo {
    fn default() -> Self {
        Self {
            current_lsn: 0,
            lag_bytes: 0,
            lag_time: Duration::ZERO,
            updated_at: Instant::now(),
            trend: LagTrend::Unknown,
            sync_mode: SyncMode::Unknown,
            healthy: true,
        }
    }
}

impl LagInfo {
    /// Check if this lag info is stale (not updated recently)
    pub fn is_stale(&self, max_age: Duration) -> bool {
        self.updated_at.elapsed() > max_age
    }

    /// Check if this node meets the freshness requirement
    pub fn meets_freshness(&self, max_lag: Duration) -> bool {
        self.healthy && self.lag_time <= max_lag
    }

    /// Check if this node has reached a specific LSN
    pub fn has_reached_lsn(&self, required_lsn: u64) -> bool {
        self.current_lsn >= required_lsn
    }
}

/// Internal tracking data for a node
#[derive(Debug)]
pub struct NodeLagData {
    /// Current lag info
    pub info: LagInfo,

    /// Recent lag samples for trend calculation
    lag_history: VecDeque<u64>,

    /// Smoothing window size
    window_size: usize,
}

impl NodeLagData {
    fn new(window_size: usize) -> Self {
        Self {
            info: LagInfo::default(),
            lag_history: VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    fn add_sample(&mut self, lag_bytes: u64) {
        if self.lag_history.len() >= self.window_size {
            self.lag_history.pop_front();
        }
        self.lag_history.push_back(lag_bytes);
    }

    fn calculate_trend(&self) -> LagTrend {
        if self.lag_history.len() < 3 {
            return LagTrend::Unknown;
        }

        let recent: Vec<_> = self.lag_history.iter().rev().take(3).collect();
        let oldest = *recent[2];
        let middle = *recent[1];
        let newest = *recent[0];

        // Calculate trend based on recent samples
        let threshold = (oldest as f64 * 0.1) as u64; // 10% threshold

        if newest + threshold < oldest && newest + threshold < middle {
            LagTrend::Improving
        } else if newest > oldest + threshold && newest > middle + threshold {
            LagTrend::Degrading
        } else {
            LagTrend::Stable
        }
    }

    fn get_smoothed_lag(&self) -> u64 {
        if self.lag_history.is_empty() {
            return self.info.lag_bytes;
        }

        // Use exponential moving average for smoothing
        let alpha = 0.3;
        let mut ema = self.lag_history[0] as f64;

        for &sample in self.lag_history.iter().skip(1) {
            ema = alpha * sample as f64 + (1.0 - alpha) * ema;
        }

        ema as u64
    }
}

/// Lag Monitor - tracks replication lag across all nodes
pub struct LagMonitor {
    /// Current lag data for each node
    node_lags: DashMap<NodeId, NodeLagData>,

    /// Current LSN on primary
    primary_lsn: AtomicU64,

    /// Primary node ID
    primary_id: RwLock<Option<NodeId>>,

    /// Configuration
    config: LagRoutingConfig,

    /// Whether monitoring is running
    running: AtomicBool,

    /// Last update time for primary LSN
    primary_updated_at: RwLock<Instant>,
}

impl LagMonitor {
    /// Create a new lag monitor
    pub fn new(config: LagRoutingConfig) -> Self {
        Self {
            node_lags: DashMap::new(),
            primary_lsn: AtomicU64::new(0),
            primary_id: RwLock::new(None),
            config,
            running: AtomicBool::new(false),
            primary_updated_at: RwLock::new(Instant::now()),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(LagRoutingConfig::default())
    }

    /// Check if monitoring is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Start monitoring (sets running flag)
    pub fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
    }

    /// Stop monitoring
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Set the primary node ID
    pub fn set_primary(&self, node_id: &str) {
        *self.primary_id.write() = Some(node_id.to_string());
    }

    /// Get the primary node ID
    pub fn get_primary(&self) -> Option<NodeId> {
        self.primary_id.read().clone()
    }

    /// Update primary LSN
    pub fn update_primary_lsn(&self, lsn: u64) {
        self.primary_lsn.store(lsn, Ordering::SeqCst);
        *self.primary_updated_at.write() = Instant::now();
    }

    /// Get current primary LSN
    pub fn get_primary_lsn(&self) -> u64 {
        self.primary_lsn.load(Ordering::SeqCst)
    }

    /// Register a standby node
    pub fn register_standby(&self, node_id: &str, sync_mode: SyncMode) {
        let mut data = NodeLagData::new(self.config.smoothing_window);
        data.info.sync_mode = sync_mode;
        self.node_lags.insert(node_id.to_string(), data);
    }

    /// Remove a node from monitoring
    pub fn remove_node(&self, node_id: &str) {
        self.node_lags.remove(node_id);
    }

    /// Update lag info for a standby node
    pub fn update_standby_lag(
        &self,
        node_id: &str,
        current_lsn: u64,
        time_lag: Option<Duration>,
    ) {
        let primary_lsn = self.primary_lsn.load(Ordering::SeqCst);
        let lag_bytes = primary_lsn.saturating_sub(current_lsn);

        // Calculate lag time using configured method
        let lag_time = self.config.lag_calculation.calculate_lag(lag_bytes, time_lag);

        // Determine if node is healthy
        let healthy = lag_time <= self.config.stale_threshold;

        self.node_lags
            .entry(node_id.to_string())
            .and_modify(|data| {
                // Add sample for trend calculation
                data.add_sample(lag_bytes);

                // Calculate trend
                let trend = if self.config.enable_smoothing {
                    data.calculate_trend()
                } else {
                    LagTrend::Unknown
                };

                // Get smoothed lag if enabled
                let effective_lag_bytes = if self.config.enable_smoothing {
                    data.get_smoothed_lag()
                } else {
                    lag_bytes
                };

                let effective_lag_time = self.config.lag_calculation.calculate_lag(
                    effective_lag_bytes,
                    time_lag,
                );

                // Update lag info
                data.info = LagInfo {
                    current_lsn,
                    lag_bytes: effective_lag_bytes,
                    lag_time: effective_lag_time,
                    updated_at: Instant::now(),
                    trend,
                    sync_mode: data.info.sync_mode,
                    healthy,
                };
            })
            .or_insert_with(|| {
                let mut data = NodeLagData::new(self.config.smoothing_window);
                data.info = LagInfo {
                    current_lsn,
                    lag_bytes,
                    lag_time,
                    updated_at: Instant::now(),
                    trend: LagTrend::Unknown,
                    sync_mode: SyncMode::Unknown,
                    healthy,
                };
                data
            });
    }

    /// Get lag info for a specific node
    pub fn get_lag(&self, node_id: &str) -> Option<LagInfo> {
        self.node_lags.get(node_id).map(|data| data.info.clone())
    }

    /// Get all current lag info
    pub fn get_all_lags(&self) -> Vec<(NodeId, LagInfo)> {
        self.node_lags
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().info.clone()))
            .collect()
    }

    /// Get nodes that meet freshness requirement
    pub fn get_fresh_nodes(&self, max_lag: Duration) -> Vec<NodeId> {
        let stale_threshold = self.config.poll_interval * 3;

        self.node_lags
            .iter()
            .filter(|entry| {
                let info = &entry.value().info;
                !info.is_stale(stale_threshold) && info.meets_freshness(max_lag)
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get nodes that have reached a specific LSN
    pub fn get_nodes_at_lsn(&self, required_lsn: u64) -> Vec<NodeId> {
        let stale_threshold = self.config.poll_interval * 3;

        self.node_lags
            .iter()
            .filter(|entry| {
                let info = &entry.value().info;
                !info.is_stale(stale_threshold) && info.has_reached_lsn(required_lsn)
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Check if a node has reached a specific LSN
    pub fn has_reached_lsn(&self, node_id: &str, required_lsn: u64) -> bool {
        self.node_lags
            .get(node_id)
            .map(|data| data.info.has_reached_lsn(required_lsn))
            .unwrap_or(false)
    }

    /// Get healthy nodes (lag below stale threshold)
    pub fn get_healthy_nodes(&self) -> Vec<NodeId> {
        self.node_lags
            .iter()
            .filter(|entry| entry.value().info.healthy)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get nodes by sync mode
    pub fn get_nodes_by_sync_mode(&self, mode: SyncMode) -> Vec<NodeId> {
        self.node_lags
            .iter()
            .filter(|entry| entry.value().info.sync_mode == mode)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get the freshest standby (lowest lag)
    pub fn get_freshest_standby(&self) -> Option<(NodeId, LagInfo)> {
        let stale_threshold = self.config.poll_interval * 3;

        self.node_lags
            .iter()
            .filter(|entry| {
                let info = &entry.value().info;
                info.healthy && !info.is_stale(stale_threshold)
            })
            .min_by_key(|entry| entry.value().info.lag_time)
            .map(|entry| (entry.key().clone(), entry.value().info.clone()))
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        self.node_lags.len()
    }

    /// Clear all lag data
    pub fn clear(&self) {
        self.node_lags.clear();
        self.primary_lsn.store(0, Ordering::SeqCst);
    }
}

impl std::fmt::Debug for LagMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LagMonitor")
            .field("primary_lsn", &self.primary_lsn.load(Ordering::Relaxed))
            .field("node_count", &self.node_lags.len())
            .field("running", &self.running.load(Ordering::Relaxed))
            .finish()
    }
}

// Thread-safe wrapper for use with Arc
impl LagMonitor {
    /// Create an Arc-wrapped instance
    pub fn arc(config: LagRoutingConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lag_info_default() {
        let info = LagInfo::default();
        assert_eq!(info.current_lsn, 0);
        assert_eq!(info.lag_bytes, 0);
        assert!(info.healthy);
    }

    #[test]
    fn test_lag_info_meets_freshness() {
        let mut info = LagInfo::default();
        info.lag_time = Duration::from_millis(100);

        assert!(info.meets_freshness(Duration::from_millis(200)));
        assert!(info.meets_freshness(Duration::from_millis(100)));
        assert!(!info.meets_freshness(Duration::from_millis(50)));
    }

    #[test]
    fn test_lag_info_has_reached_lsn() {
        let mut info = LagInfo::default();
        info.current_lsn = 1000;

        assert!(info.has_reached_lsn(500));
        assert!(info.has_reached_lsn(1000));
        assert!(!info.has_reached_lsn(1001));
    }

    #[test]
    fn test_lag_monitor_creation() {
        let monitor = LagMonitor::with_defaults();
        assert!(!monitor.is_running());
        assert_eq!(monitor.node_count(), 0);
    }

    #[test]
    fn test_lag_monitor_primary_lsn() {
        let monitor = LagMonitor::with_defaults();
        monitor.update_primary_lsn(1000);
        assert_eq!(monitor.get_primary_lsn(), 1000);
    }

    #[test]
    fn test_lag_monitor_register_standby() {
        let monitor = LagMonitor::with_defaults();
        monitor.register_standby("standby-1", SyncMode::Sync);
        monitor.register_standby("standby-2", SyncMode::Async);

        assert_eq!(monitor.node_count(), 2);
        assert_eq!(monitor.get_nodes_by_sync_mode(SyncMode::Sync).len(), 1);
        assert_eq!(monitor.get_nodes_by_sync_mode(SyncMode::Async).len(), 1);
    }

    #[test]
    fn test_lag_monitor_update_lag() {
        let monitor = LagMonitor::with_defaults();
        monitor.update_primary_lsn(1000);
        monitor.register_standby("standby-1", SyncMode::Async);
        monitor.update_standby_lag("standby-1", 990, Some(Duration::from_millis(50)));

        let lag = monitor.get_lag("standby-1").unwrap();
        assert_eq!(lag.current_lsn, 990);
        assert!(lag.lag_bytes > 0);
    }

    #[test]
    fn test_lag_monitor_fresh_nodes() {
        let monitor = LagMonitor::with_defaults();
        monitor.update_primary_lsn(1000);

        monitor.register_standby("fresh", SyncMode::Sync);
        monitor.update_standby_lag("fresh", 999, Some(Duration::from_millis(10)));

        monitor.register_standby("stale", SyncMode::Async);
        monitor.update_standby_lag("stale", 500, Some(Duration::from_secs(5)));

        let fresh = monitor.get_fresh_nodes(Duration::from_millis(100));
        assert!(fresh.contains(&"fresh".to_string()));
        assert!(!fresh.contains(&"stale".to_string()));
    }

    #[test]
    fn test_lag_monitor_lsn_check() {
        let monitor = LagMonitor::with_defaults();
        monitor.update_primary_lsn(1000);
        monitor.register_standby("standby-1", SyncMode::Async);
        monitor.update_standby_lag("standby-1", 900, None);

        assert!(monitor.has_reached_lsn("standby-1", 800));
        assert!(monitor.has_reached_lsn("standby-1", 900));
        assert!(!monitor.has_reached_lsn("standby-1", 901));
    }

    #[test]
    fn test_lag_monitor_freshest_standby() {
        let config = LagRoutingConfig::new()
            .with_lag_calculation(LagCalculation::time());
        let monitor = LagMonitor::new(config);
        monitor.update_primary_lsn(1000);

        monitor.register_standby("slow", SyncMode::Async);
        monitor.update_standby_lag("slow", 900, Some(Duration::from_millis(500)));

        monitor.register_standby("fast", SyncMode::Sync);
        monitor.update_standby_lag("fast", 999, Some(Duration::from_millis(10)));

        let (node_id, _) = monitor.get_freshest_standby().unwrap();
        assert_eq!(node_id, "fast");
    }

    #[test]
    fn test_node_lag_data_trend() {
        let mut data = NodeLagData::new(10);

        // Add improving samples (decreasing lag)
        data.add_sample(1000);
        data.add_sample(800);
        data.add_sample(600);

        assert_eq!(data.calculate_trend(), LagTrend::Improving);

        // Add degrading samples (increasing lag)
        data.add_sample(700);
        data.add_sample(900);
        data.add_sample(1100);

        assert_eq!(data.calculate_trend(), LagTrend::Degrading);
    }

    #[test]
    fn test_lag_trend_display() {
        assert_eq!(LagTrend::Improving.to_string(), "improving");
        assert_eq!(LagTrend::Stable.to_string(), "stable");
        assert_eq!(LagTrend::Degrading.to_string(), "degrading");
        assert_eq!(LagTrend::Unknown.to_string(), "unknown");
    }
}
