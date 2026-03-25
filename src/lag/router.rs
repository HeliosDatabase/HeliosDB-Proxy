//! Lag-Aware Router - Route queries based on freshness requirements
//!
//! Routes queries to standbys that meet lag/freshness constraints,
//! with support for read-your-writes consistency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::config::LagRoutingConfig;
use super::monitor::{LagInfo, LagMonitor, NodeId};
use super::ryw::ReadYourWritesTracker;
use super::SyncMode;

/// Reason for a routing decision
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LagRoutingReason {
    /// Routed to primary (always fresh)
    Primary(String),
    /// Routed to standby meeting freshness requirement
    FreshnessMatch(String),
    /// Routed to standby meeting LSN requirement (read-your-writes)
    LsnMatch(String),
    /// Routed to primary because all standbys too laggy
    FallbackToPrimary(String),
    /// Routed to primary for read-your-writes (no standby caught up)
    RywFallback(String),
    /// No eligible nodes found
    NoEligibleNodes(String),
    /// Routing disabled or bypassed
    Bypassed(String),
}

impl std::fmt::Display for LagRoutingReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LagRoutingReason::Primary(msg) => write!(f, "primary: {}", msg),
            LagRoutingReason::FreshnessMatch(msg) => write!(f, "freshness: {}", msg),
            LagRoutingReason::LsnMatch(msg) => write!(f, "lsn: {}", msg),
            LagRoutingReason::FallbackToPrimary(msg) => write!(f, "fallback: {}", msg),
            LagRoutingReason::RywFallback(msg) => write!(f, "ryw-fallback: {}", msg),
            LagRoutingReason::NoEligibleNodes(msg) => write!(f, "no-nodes: {}", msg),
            LagRoutingReason::Bypassed(msg) => write!(f, "bypassed: {}", msg),
        }
    }
}

/// Result of a lag-aware routing decision
#[derive(Debug, Clone)]
pub struct LagRoutingDecision {
    /// Target node (None = primary)
    pub target_node: Option<NodeId>,

    /// Whether to route to primary
    pub use_primary: bool,

    /// Reason for the decision
    pub reason: LagRoutingReason,

    /// Lag info for the selected node (if standby)
    pub lag_info: Option<LagInfo>,

    /// Time taken to make the decision
    pub elapsed: Duration,

    /// Max lag requirement that was applied
    pub max_lag_applied: Option<Duration>,

    /// LSN requirement that was applied (for RYW)
    pub lsn_requirement: Option<u64>,
}

impl LagRoutingDecision {
    /// Create a decision to route to primary
    pub fn primary(reason: LagRoutingReason, elapsed: Duration) -> Self {
        Self {
            target_node: None,
            use_primary: true,
            reason,
            lag_info: None,
            elapsed,
            max_lag_applied: None,
            lsn_requirement: None,
        }
    }

    /// Create a decision to route to a standby
    pub fn standby(
        node_id: NodeId,
        reason: LagRoutingReason,
        lag_info: LagInfo,
        elapsed: Duration,
    ) -> Self {
        Self {
            target_node: Some(node_id),
            use_primary: false,
            reason,
            lag_info: Some(lag_info),
            elapsed,
            max_lag_applied: None,
            lsn_requirement: None,
        }
    }

    /// Add max lag info
    pub fn with_max_lag(mut self, max_lag: Duration) -> Self {
        self.max_lag_applied = Some(max_lag);
        self
    }

    /// Add LSN requirement info
    pub fn with_lsn(mut self, lsn: u64) -> Self {
        self.lsn_requirement = Some(lsn);
        self
    }
}

/// Lag-Aware Router
///
/// Routes queries based on replication lag and freshness requirements.
pub struct LagAwareRouter {
    /// Lag monitor for real-time lag data
    lag_monitor: Arc<LagMonitor>,

    /// Read-your-writes tracker
    ryw_tracker: Arc<ReadYourWritesTracker>,

    /// Configuration
    config: LagRoutingConfig,
}

impl LagAwareRouter {
    /// Create a new lag-aware router
    pub fn new(
        lag_monitor: Arc<LagMonitor>,
        ryw_tracker: Arc<ReadYourWritesTracker>,
        config: LagRoutingConfig,
    ) -> Self {
        Self {
            lag_monitor,
            ryw_tracker,
            config,
        }
    }

    /// Create with shared components
    pub fn with_shared(
        lag_monitor: Arc<LagMonitor>,
        config: LagRoutingConfig,
    ) -> Self {
        let ryw_tracker = Arc::new(ReadYourWritesTracker::new(config.ryw_retention));
        Self::new(lag_monitor, ryw_tracker, config)
    }

    /// Get the read-your-writes tracker
    pub fn ryw_tracker(&self) -> &Arc<ReadYourWritesTracker> {
        &self.ryw_tracker
    }

    /// Route a query with freshness requirement
    ///
    /// # Arguments
    /// * `session_id` - Session identifier for RYW tracking
    /// * `max_lag` - Maximum acceptable lag (None = use default)
    /// * `prefer_sync_mode` - Preferred sync mode (optional)
    ///
    /// # Returns
    /// A routing decision indicating which node to use
    pub fn route(
        &self,
        session_id: Option<&str>,
        max_lag: Option<Duration>,
        prefer_sync_mode: Option<SyncMode>,
    ) -> LagRoutingDecision {
        let start = Instant::now();

        // Check if routing is disabled
        if !self.config.enabled {
            return LagRoutingDecision::primary(
                LagRoutingReason::Bypassed("Lag routing disabled".to_string()),
                start.elapsed(),
            );
        }

        // Determine freshness requirement
        let max_lag = max_lag.unwrap_or(self.config.default_max_lag);

        // Check read-your-writes requirement
        if self.config.read_your_writes {
            if let Some(sid) = session_id {
                if let Some(required_lsn) = self.ryw_tracker.get_required_lsn(sid) {
                    return self
                        .route_with_lsn_requirement(required_lsn, start)
                        .with_lsn(required_lsn);
                }
            }
        }

        // Route based on freshness
        self.route_by_freshness(max_lag, prefer_sync_mode, start)
    }

    /// Route requiring a specific LSN (for read-your-writes)
    fn route_with_lsn_requirement(
        &self,
        required_lsn: u64,
        start: Instant,
    ) -> LagRoutingDecision {
        // Find standbys that have replayed past required LSN
        let eligible = self.lag_monitor.get_nodes_at_lsn(required_lsn);

        if eligible.is_empty() {
            // No standby caught up yet, route to primary
            if self.config.fallback_to_primary {
                return LagRoutingDecision::primary(
                    LagRoutingReason::RywFallback(format!(
                        "No standby reached LSN {}",
                        required_lsn
                    )),
                    start.elapsed(),
                );
            }

            return LagRoutingDecision::primary(
                LagRoutingReason::NoEligibleNodes(
                    "No standby caught up for RYW".to_string(),
                ),
                start.elapsed(),
            );
        }

        // Select best node from eligible
        let (node_id, lag_info) = self.select_best_node(&eligible);

        LagRoutingDecision::standby(
            node_id,
            LagRoutingReason::LsnMatch(format!("Reached LSN {}", required_lsn)),
            lag_info,
            start.elapsed(),
        )
    }

    /// Route based on freshness/lag requirement
    fn route_by_freshness(
        &self,
        max_lag: Duration,
        prefer_sync_mode: Option<SyncMode>,
        start: Instant,
    ) -> LagRoutingDecision {
        // If max_lag is zero, only sync standbys or primary allowed
        if max_lag == Duration::ZERO {
            let sync_nodes = self.lag_monitor.get_nodes_by_sync_mode(SyncMode::Sync);
            let fresh_sync: Vec<_> = sync_nodes
                .into_iter()
                .filter(|n| {
                    self.lag_monitor
                        .get_lag(n)
                        .map(|info| info.healthy)
                        .unwrap_or(false)
                })
                .collect();

            if fresh_sync.is_empty() {
                return LagRoutingDecision::primary(
                    LagRoutingReason::Primary(
                        "Zero lag required, no sync standby available".to_string(),
                    ),
                    start.elapsed(),
                )
                .with_max_lag(max_lag);
            }

            let (node_id, lag_info) = self.select_best_node(&fresh_sync);
            return LagRoutingDecision::standby(
                node_id,
                LagRoutingReason::FreshnessMatch("Sync standby with zero lag".to_string()),
                lag_info,
                start.elapsed(),
            )
            .with_max_lag(max_lag);
        }

        // Get nodes meeting freshness requirement
        let mut eligible = self.lag_monitor.get_fresh_nodes(max_lag);

        // Filter by preferred sync mode if specified
        if let Some(mode) = prefer_sync_mode {
            let mode_nodes = self.lag_monitor.get_nodes_by_sync_mode(mode);
            let preferred: Vec<_> = eligible
                .iter()
                .filter(|n| mode_nodes.contains(n))
                .cloned()
                .collect();

            if !preferred.is_empty() {
                eligible = preferred;
            }
            // If no nodes match preferred mode, use all eligible
        }

        if eligible.is_empty() {
            // All standbys too laggy
            if self.config.fallback_to_primary {
                return LagRoutingDecision::primary(
                    LagRoutingReason::FallbackToPrimary(format!(
                        "All standbys exceed {}ms lag",
                        max_lag.as_millis()
                    )),
                    start.elapsed(),
                )
                .with_max_lag(max_lag);
            }

            // Try to get the freshest available node anyway
            if let Some((node_id, lag_info)) = self.lag_monitor.get_freshest_standby() {
                return LagRoutingDecision::standby(
                    node_id,
                    LagRoutingReason::FreshnessMatch(format!(
                        "Best available ({}ms lag, wanted {}ms)",
                        lag_info.lag_time.as_millis(),
                        max_lag.as_millis()
                    )),
                    lag_info,
                    start.elapsed(),
                )
                .with_max_lag(max_lag);
            }

            return LagRoutingDecision::primary(
                LagRoutingReason::NoEligibleNodes("No healthy standbys".to_string()),
                start.elapsed(),
            )
            .with_max_lag(max_lag);
        }

        // Select best node from eligible
        let (node_id, lag_info) = self.select_best_node(&eligible);

        LagRoutingDecision::standby(
            node_id,
            LagRoutingReason::FreshnessMatch(format!(
                "{}ms lag <= {}ms requirement",
                lag_info.lag_time.as_millis(),
                max_lag.as_millis()
            )),
            lag_info,
            start.elapsed(),
        )
        .with_max_lag(max_lag)
    }

    /// Select the best node from eligible nodes
    ///
    /// Prefers: lower lag, then sync mode weight
    fn select_best_node(&self, eligible: &[NodeId]) -> (NodeId, LagInfo) {
        let mut best: Option<(NodeId, LagInfo, f64)> = None;

        for node_id in eligible {
            if let Some(lag_info) = self.lag_monitor.get_lag(node_id) {
                let weight = self.config.get_sync_mode_weight(lag_info.sync_mode);
                let score = lag_info.lag_time.as_secs_f64() / weight;

                if best.is_none() || score < best.as_ref().unwrap().2 {
                    best = Some((node_id.clone(), lag_info, score));
                }
            }
        }

        best.map(|(id, info, _)| (id, info))
            .unwrap_or_else(|| {
                (
                    eligible[0].clone(),
                    self.lag_monitor
                        .get_lag(&eligible[0])
                        .unwrap_or_default(),
                )
            })
    }

    /// Record a write operation for read-your-writes tracking
    pub fn record_write(&self, session_id: &str, write_lsn: u64) {
        if self.config.read_your_writes {
            self.ryw_tracker.record_write(session_id, write_lsn);
        }
    }

    /// Clear RYW requirement after successful read
    pub fn clear_ryw(&self, session_id: &str) {
        self.ryw_tracker.clear(session_id);
    }

    /// Get current configuration
    pub fn config(&self) -> &LagRoutingConfig {
        &self.config
    }
}

impl std::fmt::Debug for LagAwareRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LagAwareRouter")
            .field("enabled", &self.config.enabled)
            .field("default_max_lag", &self.config.default_max_lag)
            .field("ryw_enabled", &self.config.read_your_writes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lag::config::LagCalculation;

    fn setup_router() -> LagAwareRouter {
        let config = LagRoutingConfig::new()
            .with_lag_calculation(LagCalculation::time())
            .with_default_max_lag(Duration::from_secs(1));

        let monitor = Arc::new(LagMonitor::new(config.clone()));
        monitor.update_primary_lsn(10000);

        // Add test standbys
        monitor.register_standby("sync-1", SyncMode::Sync);
        monitor.update_standby_lag("sync-1", 9999, Some(Duration::from_millis(5)));

        monitor.register_standby("async-1", SyncMode::Async);
        monitor.update_standby_lag("async-1", 9500, Some(Duration::from_millis(200)));

        monitor.register_standby("async-2", SyncMode::Async);
        monitor.update_standby_lag("async-2", 9000, Some(Duration::from_secs(2)));

        LagAwareRouter::with_shared(monitor, config)
    }

    #[test]
    fn test_route_with_default_lag() {
        let router = setup_router();
        let decision = router.route(None, None, None);

        assert!(!decision.use_primary);
        assert!(decision.target_node.is_some());
        // Should pick one of the fresh nodes (sync-1 or async-1)
        let node = decision.target_node.unwrap();
        assert!(node == "sync-1" || node == "async-1");
    }

    #[test]
    fn test_route_zero_lag() {
        let router = setup_router();
        let decision = router.route(None, Some(Duration::ZERO), None);

        // Zero lag requires sync standby
        if decision.target_node.is_some() {
            assert_eq!(decision.target_node.as_ref().unwrap(), "sync-1");
        }
    }

    #[test]
    fn test_route_tight_lag() {
        let router = setup_router();
        let decision = router.route(None, Some(Duration::from_millis(10)), None);

        // Only sync-1 has 5ms lag
        if !decision.use_primary {
            assert_eq!(decision.target_node.as_ref().unwrap(), "sync-1");
        }
    }

    #[test]
    fn test_route_prefer_sync_mode() {
        let router = setup_router();
        let decision = router.route(None, Some(Duration::from_secs(1)), Some(SyncMode::Sync));

        assert!(!decision.use_primary);
        assert_eq!(decision.target_node.as_ref().unwrap(), "sync-1");
    }

    #[test]
    fn test_route_read_your_writes() {
        let config = LagRoutingConfig::new()
            .with_lag_calculation(LagCalculation::time())
            .with_read_your_writes(true);

        let monitor = Arc::new(LagMonitor::new(config.clone()));
        monitor.update_primary_lsn(10000);

        monitor.register_standby("standby-1", SyncMode::Async);
        monitor.update_standby_lag("standby-1", 9500, Some(Duration::from_millis(100)));

        let router = LagAwareRouter::with_shared(monitor, config);

        // Record a write at LSN 9800
        router.record_write("session-1", 9800);

        // Route for session-1 - should need LSN 9800
        let decision = router.route(Some("session-1"), None, None);

        // standby-1 is at 9500, hasn't reached 9800 yet
        // Should fall back to primary
        assert!(decision.use_primary);
        match decision.reason {
            LagRoutingReason::RywFallback(_) => {}
            _ => panic!("Expected RywFallback reason"),
        }
    }

    #[test]
    fn test_route_ryw_satisfied() {
        let config = LagRoutingConfig::new()
            .with_lag_calculation(LagCalculation::time())
            .with_read_your_writes(true);

        let monitor = Arc::new(LagMonitor::new(config.clone()));
        monitor.update_primary_lsn(10000);

        monitor.register_standby("standby-1", SyncMode::Async);
        monitor.update_standby_lag("standby-1", 9800, Some(Duration::from_millis(100)));

        let router = LagAwareRouter::with_shared(monitor, config);

        // Record a write at LSN 9700
        router.record_write("session-1", 9700);

        // Route for session-1 - standby-1 at 9800 >= 9700
        let decision = router.route(Some("session-1"), None, None);

        assert!(!decision.use_primary);
        assert_eq!(decision.target_node.as_ref().unwrap(), "standby-1");
        match decision.reason {
            LagRoutingReason::LsnMatch(_) => {}
            _ => panic!("Expected LsnMatch reason"),
        }
    }

    #[test]
    fn test_routing_decision_display() {
        let reason = LagRoutingReason::FreshnessMatch("100ms lag".to_string());
        assert!(reason.to_string().contains("freshness"));

        let reason = LagRoutingReason::FallbackToPrimary("all laggy".to_string());
        assert!(reason.to_string().contains("fallback"));
    }

    #[test]
    fn test_disabled_routing() {
        let mut config = LagRoutingConfig::new();
        config.enabled = false;

        let monitor = Arc::new(LagMonitor::new(config.clone()));
        let router = LagAwareRouter::with_shared(monitor, config);

        let decision = router.route(None, None, None);
        assert!(decision.use_primary);
        match decision.reason {
            LagRoutingReason::Bypassed(_) => {}
            _ => panic!("Expected Bypassed reason"),
        }
    }

    #[test]
    fn test_select_best_node_prefers_lower_lag() {
        let config = LagRoutingConfig::new()
            .with_lag_calculation(LagCalculation::time());

        let monitor = Arc::new(LagMonitor::new(config.clone()));
        monitor.update_primary_lsn(10000);

        // Add nodes with different lags
        monitor.register_standby("slow", SyncMode::Async);
        monitor.update_standby_lag("slow", 9000, Some(Duration::from_millis(500)));

        monitor.register_standby("fast", SyncMode::Async);
        monitor.update_standby_lag("fast", 9900, Some(Duration::from_millis(50)));

        let router = LagAwareRouter::with_shared(monitor, config);
        let decision = router.route(None, Some(Duration::from_secs(1)), None);

        assert!(!decision.use_primary);
        assert_eq!(decision.target_node.as_ref().unwrap(), "fast");
    }
}
