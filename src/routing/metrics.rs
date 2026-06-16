//! Routing Metrics
//!
//! Tracks routing decisions and performance metrics.

use super::RouteTarget;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Routing metrics tracker
pub struct RoutingMetrics {
    /// Total queries routed
    total_routed: AtomicU64,
    /// Queries routed with hints
    with_hints: AtomicU64,
    /// Queries routed without hints
    without_hints: AtomicU64,
    /// Invalid hints encountered
    invalid_hints: AtomicU64,
    /// Fallback routing used
    fallback_count: AtomicU64,
    /// No nodes available
    no_nodes_count: AtomicU64,
    /// Total routing time (microseconds)
    total_routing_time_us: AtomicU64,
    /// Per-target counts
    target_counts: RwLock<HashMap<RouteTarget, u64>>,
    /// Per-hint usage counts
    hint_usage: RwLock<HashMap<String, u64>>,
    /// Recent routing decisions (for debugging)
    recent_decisions: RwLock<Vec<RoutingDecisionRecord>>,
    /// Maximum recent decisions to keep
    max_recent: usize,
}

impl RoutingMetrics {
    /// Create new metrics tracker
    pub fn new() -> Self {
        Self {
            total_routed: AtomicU64::new(0),
            with_hints: AtomicU64::new(0),
            without_hints: AtomicU64::new(0),
            invalid_hints: AtomicU64::new(0),
            fallback_count: AtomicU64::new(0),
            no_nodes_count: AtomicU64::new(0),
            total_routing_time_us: AtomicU64::new(0),
            target_counts: RwLock::new(HashMap::new()),
            hint_usage: RwLock::new(HashMap::new()),
            recent_decisions: RwLock::new(Vec::new()),
            max_recent: 100,
        }
    }

    /// Record a routing decision
    pub fn record_routing(
        &self,
        target: Option<RouteTarget>,
        had_hints: bool,
        elapsed: Duration,
    ) {
        self.total_routed.fetch_add(1, Ordering::SeqCst);

        if had_hints {
            self.with_hints.fetch_add(1, Ordering::SeqCst);
        } else {
            self.without_hints.fetch_add(1, Ordering::SeqCst);
        }

        self.total_routing_time_us
            .fetch_add(elapsed.as_micros() as u64, Ordering::SeqCst);

        // Track target usage (async - won't block)
        if let Some(t) = target {
            let _target = t;
            tokio::spawn(async move {
                // In real implementation, would update the actual counter
                // This is simplified for the skeleton
            });
        }
    }

    /// Record invalid hints
    pub fn record_invalid_hints(&self) {
        self.invalid_hints.fetch_add(1, Ordering::SeqCst);
    }

    /// Record fallback routing
    pub fn record_fallback(&self) {
        self.fallback_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Record no nodes available
    pub fn record_no_nodes(&self) {
        self.no_nodes_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Record hint usage
    pub async fn record_hint(&self, hint_name: &str) {
        let mut usage = self.hint_usage.write().await;
        *usage.entry(hint_name.to_string()).or_insert(0) += 1;
    }

    /// Record a decision for debugging
    pub async fn record_decision(&self, record: RoutingDecisionRecord) {
        let mut recent = self.recent_decisions.write().await;
        recent.push(record);

        // Keep only recent decisions
        if recent.len() > self.max_recent {
            recent.remove(0);
        }
    }

    /// Get a snapshot of current stats
    pub fn snapshot(&self) -> RoutingStats {
        let total = self.total_routed.load(Ordering::SeqCst);
        let total_time_us = self.total_routing_time_us.load(Ordering::SeqCst);

        RoutingStats {
            total_routed: total,
            with_hints: self.with_hints.load(Ordering::SeqCst),
            without_hints: self.without_hints.load(Ordering::SeqCst),
            invalid_hints: self.invalid_hints.load(Ordering::SeqCst),
            fallback_count: self.fallback_count.load(Ordering::SeqCst),
            no_nodes_count: self.no_nodes_count.load(Ordering::SeqCst),
            avg_routing_time_us: total_time_us.checked_div(total).unwrap_or(0),
        }
    }

    /// Get hint usage stats
    pub async fn hint_usage(&self) -> HintUsageStats {
        let usage = self.hint_usage.read().await;
        HintUsageStats {
            by_hint: usage.clone(),
        }
    }

    /// Get recent decisions
    pub async fn recent_decisions(&self, limit: usize) -> Vec<RoutingDecisionRecord> {
        let recent = self.recent_decisions.read().await;
        recent.iter().rev().take(limit).cloned().collect()
    }

    /// Get target distribution
    pub async fn target_distribution(&self) -> HashMap<RouteTarget, u64> {
        self.target_counts.read().await.clone()
    }

    /// Reset all metrics
    pub async fn reset(&self) {
        self.total_routed.store(0, Ordering::SeqCst);
        self.with_hints.store(0, Ordering::SeqCst);
        self.without_hints.store(0, Ordering::SeqCst);
        self.invalid_hints.store(0, Ordering::SeqCst);
        self.fallback_count.store(0, Ordering::SeqCst);
        self.no_nodes_count.store(0, Ordering::SeqCst);
        self.total_routing_time_us.store(0, Ordering::SeqCst);
        self.target_counts.write().await.clear();
        self.hint_usage.write().await.clear();
        self.recent_decisions.write().await.clear();
    }
}

impl Default for RoutingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Routing statistics snapshot
#[derive(Debug, Clone)]
pub struct RoutingStats {
    /// Total queries routed
    pub total_routed: u64,
    /// Queries with routing hints
    pub with_hints: u64,
    /// Queries without routing hints
    pub without_hints: u64,
    /// Invalid hint combinations
    pub invalid_hints: u64,
    /// Fallback routing count
    pub fallback_count: u64,
    /// No nodes available count
    pub no_nodes_count: u64,
    /// Average routing decision time (microseconds)
    pub avg_routing_time_us: u64,
}

impl RoutingStats {
    /// Get percentage of queries with hints
    pub fn hints_percentage(&self) -> f64 {
        if self.total_routed == 0 {
            0.0
        } else {
            (self.with_hints as f64 / self.total_routed as f64) * 100.0
        }
    }

    /// Get fallback percentage
    pub fn fallback_percentage(&self) -> f64 {
        if self.total_routed == 0 {
            0.0
        } else {
            (self.fallback_count as f64 / self.total_routed as f64) * 100.0
        }
    }
}

/// Hint usage statistics
#[derive(Debug, Clone)]
pub struct HintUsageStats {
    /// Count by hint name
    pub by_hint: HashMap<String, u64>,
}

impl HintUsageStats {
    /// Get most used hints
    pub fn top_hints(&self, n: usize) -> Vec<(String, u64)> {
        let mut hints: Vec<_> = self.by_hint.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        hints.sort_by_key(|b| std::cmp::Reverse(b.1));
        hints.truncate(n);
        hints
    }
}

/// Record of a routing decision (for debugging/auditing)
#[derive(Debug, Clone)]
pub struct RoutingDecisionRecord {
    /// Query hash (for privacy)
    pub query_hash: u64,
    /// Target node
    pub target_node: Option<String>,
    /// Route target hint
    pub route_target: Option<RouteTarget>,
    /// Hints used
    pub hints: Vec<String>,
    /// Decision reason
    pub reason: String,
    /// Timestamp
    pub timestamp: Instant,
    /// Routing time
    pub elapsed_us: u64,
}

impl RoutingDecisionRecord {
    /// Create a new record
    pub fn new(
        query: &str,
        target_node: Option<String>,
        route_target: Option<RouteTarget>,
        hints: Vec<String>,
        reason: String,
        elapsed: Duration,
    ) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);

        Self {
            query_hash: hasher.finish(),
            target_node,
            route_target,
            hints,
            reason,
            timestamp: Instant::now(),
            elapsed_us: elapsed.as_micros() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_record_routing() {
        let metrics = RoutingMetrics::new();

        metrics.record_routing(Some(RouteTarget::Primary), true, Duration::from_micros(100));
        metrics.record_routing(Some(RouteTarget::Standby), false, Duration::from_micros(50));
        metrics.record_routing(Some(RouteTarget::Async), true, Duration::from_micros(75));

        let stats = metrics.snapshot();
        assert_eq!(stats.total_routed, 3);
        assert_eq!(stats.with_hints, 2);
        assert_eq!(stats.without_hints, 1);
    }

    #[tokio::test]
    async fn test_record_errors() {
        let metrics = RoutingMetrics::new();

        metrics.record_invalid_hints();
        metrics.record_invalid_hints();
        metrics.record_fallback();
        metrics.record_no_nodes();

        let stats = metrics.snapshot();
        assert_eq!(stats.invalid_hints, 2);
        assert_eq!(stats.fallback_count, 1);
        assert_eq!(stats.no_nodes_count, 1);
    }

    #[tokio::test]
    async fn test_hint_usage() {
        let metrics = RoutingMetrics::new();

        metrics.record_hint("route").await;
        metrics.record_hint("route").await;
        metrics.record_hint("node").await;

        let usage = metrics.hint_usage().await;
        assert_eq!(usage.by_hint.get("route"), Some(&2));
        assert_eq!(usage.by_hint.get("node"), Some(&1));
    }

    #[tokio::test]
    async fn test_recent_decisions() {
        let metrics = RoutingMetrics::new();

        for i in 0..5 {
            metrics.record_decision(RoutingDecisionRecord::new(
                &format!("SELECT {}", i),
                Some("node".to_string()),
                Some(RouteTarget::Standby),
                vec!["route".to_string()],
                "test".to_string(),
                Duration::from_micros(100),
            )).await;
        }

        let recent = metrics.recent_decisions(3).await;
        assert_eq!(recent.len(), 3);
    }

    #[tokio::test]
    async fn test_reset() {
        let metrics = RoutingMetrics::new();

        metrics.record_routing(Some(RouteTarget::Primary), true, Duration::from_micros(100));
        metrics.record_hint("route").await;

        metrics.reset().await;

        let stats = metrics.snapshot();
        assert_eq!(stats.total_routed, 0);

        let usage = metrics.hint_usage().await;
        assert!(usage.by_hint.is_empty());
    }

    #[test]
    fn test_stats_percentages() {
        let stats = RoutingStats {
            total_routed: 100,
            with_hints: 30,
            without_hints: 70,
            invalid_hints: 2,
            fallback_count: 5,
            no_nodes_count: 1,
            avg_routing_time_us: 50,
        };

        assert!((stats.hints_percentage() - 30.0).abs() < f64::EPSILON);
        assert!((stats.fallback_percentage() - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_top_hints() {
        let mut by_hint = HashMap::new();
        by_hint.insert("route".to_string(), 100);
        by_hint.insert("node".to_string(), 50);
        by_hint.insert("lag".to_string(), 25);

        let usage = HintUsageStats { by_hint };
        let top = usage.top_hints(2);

        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "route");
        assert_eq!(top[1].0, "node");
    }
}
