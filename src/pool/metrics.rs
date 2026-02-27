//! Pool Mode Metrics
//!
//! Tracks connection pool statistics for monitoring and debugging.

use super::mode::PoolingMode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Connection pool metrics
#[derive(Debug)]
pub struct PoolModeMetrics {
    /// Total connections acquired
    pub acquires: AtomicU64,
    /// Total connections released
    pub releases: AtomicU64,
    /// Connection acquire failures
    pub acquire_failures: AtomicU64,
    /// Acquire timeouts
    pub acquire_timeouts: AtomicU64,
    /// Connections created
    pub connections_created: AtomicU64,
    /// Connections closed
    pub connections_closed: AtomicU64,
    /// Connection resets performed
    pub connection_resets: AtomicU64,
    /// Reset failures
    pub reset_failures: AtomicU64,
    /// Total transactions completed
    pub transactions_completed: AtomicU64,
    /// Total statements executed
    pub statements_executed: AtomicU64,
    /// Current active leases
    pub active_leases: AtomicU64,
    /// Peak active leases
    pub peak_active_leases: AtomicU64,
    /// Queue wait count (when pool exhausted)
    pub queue_waits: AtomicU64,
    /// Per-mode statistics
    mode_stats: Arc<parking_lot::RwLock<HashMap<PoolingMode, ModeStats>>>,
}

/// Per-mode statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModeStats {
    /// Connections using this mode
    pub active_connections: u64,
    /// Total acquires in this mode
    pub total_acquires: u64,
    /// Total releases in this mode
    pub total_releases: u64,
    /// Average lease duration (ms)
    pub avg_lease_duration_ms: f64,
    /// Average statements per lease
    pub avg_statements_per_lease: f64,
}

impl Default for PoolModeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolModeMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        let mut mode_stats = HashMap::new();
        mode_stats.insert(PoolingMode::Session, ModeStats::default());
        mode_stats.insert(PoolingMode::Transaction, ModeStats::default());
        mode_stats.insert(PoolingMode::Statement, ModeStats::default());

        Self {
            acquires: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            acquire_failures: AtomicU64::new(0),
            acquire_timeouts: AtomicU64::new(0),
            connections_created: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            connection_resets: AtomicU64::new(0),
            reset_failures: AtomicU64::new(0),
            transactions_completed: AtomicU64::new(0),
            statements_executed: AtomicU64::new(0),
            active_leases: AtomicU64::new(0),
            peak_active_leases: AtomicU64::new(0),
            queue_waits: AtomicU64::new(0),
            mode_stats: Arc::new(parking_lot::RwLock::new(mode_stats)),
        }
    }

    /// Record a connection acquire
    pub fn record_acquire(&self, mode: PoolingMode) {
        self.acquires.fetch_add(1, Ordering::Relaxed);
        let active = self.active_leases.fetch_add(1, Ordering::Relaxed) + 1;

        // Update peak
        loop {
            let peak = self.peak_active_leases.load(Ordering::Relaxed);
            if active <= peak {
                break;
            }
            if self
                .peak_active_leases
                .compare_exchange(peak, active, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        // Update mode stats
        let mut stats = self.mode_stats.write();
        if let Some(mode_stat) = stats.get_mut(&mode) {
            mode_stat.active_connections += 1;
            mode_stat.total_acquires += 1;
        }
    }

    /// Record a connection release
    pub fn record_release(&self, mode: PoolingMode, lease_duration_ms: u64, statements: u64) {
        self.releases.fetch_add(1, Ordering::Relaxed);
        self.active_leases.fetch_sub(1, Ordering::Relaxed);
        self.statements_executed
            .fetch_add(statements, Ordering::Relaxed);

        // Update mode stats with running average
        let mut stats = self.mode_stats.write();
        if let Some(mode_stat) = stats.get_mut(&mode) {
            mode_stat.active_connections = mode_stat.active_connections.saturating_sub(1);
            mode_stat.total_releases += 1;

            // Update running average for lease duration
            let n = mode_stat.total_releases as f64;
            mode_stat.avg_lease_duration_ms =
                mode_stat.avg_lease_duration_ms * ((n - 1.0) / n) + (lease_duration_ms as f64 / n);

            // Update running average for statements per lease
            mode_stat.avg_statements_per_lease =
                mode_stat.avg_statements_per_lease * ((n - 1.0) / n) + (statements as f64 / n);
        }
    }

    /// Record an acquire failure
    pub fn record_acquire_failure(&self) {
        self.acquire_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an acquire timeout
    pub fn record_acquire_timeout(&self) {
        self.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection creation
    pub fn record_connection_created(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection close
    pub fn record_connection_closed(&self) {
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection reset
    pub fn record_reset(&self, success: bool) {
        self.connection_resets.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.reset_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record transaction completion
    pub fn record_transaction_complete(&self) {
        self.transactions_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record queue wait
    pub fn record_queue_wait(&self) {
        self.queue_waits.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            acquires: self.acquires.load(Ordering::Relaxed),
            releases: self.releases.load(Ordering::Relaxed),
            acquire_failures: self.acquire_failures.load(Ordering::Relaxed),
            acquire_timeouts: self.acquire_timeouts.load(Ordering::Relaxed),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            connection_resets: self.connection_resets.load(Ordering::Relaxed),
            reset_failures: self.reset_failures.load(Ordering::Relaxed),
            transactions_completed: self.transactions_completed.load(Ordering::Relaxed),
            statements_executed: self.statements_executed.load(Ordering::Relaxed),
            active_leases: self.active_leases.load(Ordering::Relaxed),
            peak_active_leases: self.peak_active_leases.load(Ordering::Relaxed),
            queue_waits: self.queue_waits.load(Ordering::Relaxed),
            mode_stats: self.mode_stats.read().clone(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.acquires.store(0, Ordering::Relaxed);
        self.releases.store(0, Ordering::Relaxed);
        self.acquire_failures.store(0, Ordering::Relaxed);
        self.acquire_timeouts.store(0, Ordering::Relaxed);
        self.connections_created.store(0, Ordering::Relaxed);
        self.connections_closed.store(0, Ordering::Relaxed);
        self.connection_resets.store(0, Ordering::Relaxed);
        self.reset_failures.store(0, Ordering::Relaxed);
        self.transactions_completed.store(0, Ordering::Relaxed);
        self.statements_executed.store(0, Ordering::Relaxed);
        // Note: active_leases and peak are not reset
        self.queue_waits.store(0, Ordering::Relaxed);

        let mut stats = self.mode_stats.write();
        for stat in stats.values_mut() {
            *stat = ModeStats::default();
        }
    }
}

/// Serializable metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub acquires: u64,
    pub releases: u64,
    pub acquire_failures: u64,
    pub acquire_timeouts: u64,
    pub connections_created: u64,
    pub connections_closed: u64,
    pub connection_resets: u64,
    pub reset_failures: u64,
    pub transactions_completed: u64,
    pub statements_executed: u64,
    pub active_leases: u64,
    pub peak_active_leases: u64,
    pub queue_waits: u64,
    pub mode_stats: HashMap<PoolingMode, ModeStats>,
}

impl MetricsSnapshot {
    /// Calculate connection efficiency (releases / acquires)
    pub fn connection_efficiency(&self) -> f64 {
        if self.acquires == 0 {
            1.0
        } else {
            self.releases as f64 / self.acquires as f64
        }
    }

    /// Calculate reset success rate
    pub fn reset_success_rate(&self) -> f64 {
        if self.connection_resets == 0 {
            1.0
        } else {
            1.0 - (self.reset_failures as f64 / self.connection_resets as f64)
        }
    }

    /// Calculate acquire success rate
    pub fn acquire_success_rate(&self) -> f64 {
        let total_attempts = self.acquires + self.acquire_failures + self.acquire_timeouts;
        if total_attempts == 0 {
            1.0
        } else {
            self.acquires as f64 / total_attempts as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new() {
        let metrics = PoolModeMetrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.acquires, 0);
        assert_eq!(snapshot.releases, 0);
        assert_eq!(snapshot.active_leases, 0);
    }

    #[test]
    fn test_record_acquire_release() {
        let metrics = PoolModeMetrics::new();

        metrics.record_acquire(PoolingMode::Transaction);
        assert_eq!(metrics.active_leases.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.acquires.load(Ordering::Relaxed), 1);

        metrics.record_release(PoolingMode::Transaction, 100, 5);
        assert_eq!(metrics.active_leases.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.releases.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.statements_executed.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_peak_active_leases() {
        let metrics = PoolModeMetrics::new();

        metrics.record_acquire(PoolingMode::Session);
        metrics.record_acquire(PoolingMode::Session);
        metrics.record_acquire(PoolingMode::Session);
        assert_eq!(metrics.peak_active_leases.load(Ordering::Relaxed), 3);

        metrics.record_release(PoolingMode::Session, 100, 1);
        metrics.record_release(PoolingMode::Session, 100, 1);
        assert_eq!(metrics.peak_active_leases.load(Ordering::Relaxed), 3);

        metrics.record_acquire(PoolingMode::Session);
        assert_eq!(metrics.peak_active_leases.load(Ordering::Relaxed), 3);

        metrics.record_acquire(PoolingMode::Session);
        metrics.record_acquire(PoolingMode::Session);
        assert_eq!(metrics.peak_active_leases.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_mode_stats() {
        let metrics = PoolModeMetrics::new();

        metrics.record_acquire(PoolingMode::Transaction);
        metrics.record_release(PoolingMode::Transaction, 200, 10);

        metrics.record_acquire(PoolingMode::Transaction);
        metrics.record_release(PoolingMode::Transaction, 100, 5);

        let snapshot = metrics.snapshot();
        let txn_stats = snapshot.mode_stats.get(&PoolingMode::Transaction).unwrap();

        assert_eq!(txn_stats.total_acquires, 2);
        assert_eq!(txn_stats.total_releases, 2);
        assert_eq!(txn_stats.avg_statements_per_lease, 7.5); // (10 + 5) / 2
    }

    #[test]
    fn test_reset() {
        let metrics = PoolModeMetrics::new();

        metrics.record_acquire(PoolingMode::Session);
        metrics.record_connection_created();
        metrics.record_transaction_complete();

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.acquires, 0);
        assert_eq!(snapshot.connections_created, 0);
        assert_eq!(snapshot.transactions_completed, 0);
        // active_leases is NOT reset
        assert_eq!(snapshot.active_leases, 1);
    }

    #[test]
    fn test_snapshot_calculations() {
        let metrics = PoolModeMetrics::new();

        // Perfect efficiency
        for _ in 0..10 {
            metrics.record_acquire(PoolingMode::Transaction);
            metrics.record_release(PoolingMode::Transaction, 100, 1);
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connection_efficiency(), 1.0);
        assert_eq!(snapshot.acquire_success_rate(), 1.0);
    }
}
