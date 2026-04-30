//! Connection Pool Hardening
//!
//! Additional safety and reliability features for connection pooling:
//! - Transaction leak detection
//! - Connection health validation
//! - Stale lease cleanup
//! - Pool exhaustion monitoring

use super::lease::ClientId;
use super::mode::PoolingMode;
use crate::{ProxyError, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tracing::{warn, info, debug};

/// Transaction leak detector
///
/// Tracks active transactions and warns when they exceed expected lifetimes.
/// Helps identify abandoned transactions that could block connections.
#[derive(Debug)]
pub struct TransactionLeakDetector {
    /// Active transactions: client_id -> (start_time, mode)
    active_transactions: RwLock<HashMap<ClientId, TransactionInfo>>,
    /// Warning threshold for transaction duration
    warning_threshold: Duration,
    /// Critical threshold - transaction is considered leaked
    critical_threshold: Duration,
    /// Number of leak warnings issued
    warnings_issued: AtomicU64,
    /// Number of transactions forced-closed
    force_closed: AtomicU64,
}

/// Information about an active transaction
#[derive(Debug, Clone)]
struct TransactionInfo {
    /// When the transaction started
    started_at: Instant,
    /// Pooling mode of the connection
    mode: PoolingMode,
    /// First SQL statement (truncated)
    first_statement: String,
    /// Whether warning has been issued
    warning_issued: bool,
}

impl Default for TransactionLeakDetector {
    fn default() -> Self {
        Self::new(Duration::from_secs(60), Duration::from_secs(300))
    }
}

impl TransactionLeakDetector {
    /// Create a new transaction leak detector
    ///
    /// # Arguments
    /// * `warning_threshold` - Duration after which to issue a warning
    /// * `critical_threshold` - Duration after which transaction is considered leaked
    pub fn new(warning_threshold: Duration, critical_threshold: Duration) -> Self {
        Self {
            active_transactions: RwLock::new(HashMap::new()),
            warning_threshold,
            critical_threshold,
            warnings_issued: AtomicU64::new(0),
            force_closed: AtomicU64::new(0),
        }
    }

    /// Track the start of a transaction
    pub fn transaction_started(&self, client_id: ClientId, mode: PoolingMode, first_sql: &str) {
        let info = TransactionInfo {
            started_at: Instant::now(),
            mode,
            first_statement: truncate_sql(first_sql, 100),
            warning_issued: false,
        };
        self.active_transactions.write().insert(client_id, info);
    }

    /// Track the end of a transaction
    pub fn transaction_ended(&self, client_id: &ClientId) {
        self.active_transactions.write().remove(client_id);
    }

    /// Check for leaked transactions and issue warnings
    ///
    /// Returns list of client IDs that have exceeded the critical threshold
    pub fn check_for_leaks(&self) -> Vec<ClientId> {
        let now = Instant::now();
        let mut leaked = Vec::new();
        let mut txns = self.active_transactions.write();

        for (client_id, info) in txns.iter_mut() {
            let duration = now.duration_since(info.started_at);

            // Check critical threshold first
            if duration >= self.critical_threshold {
                leaked.push(*client_id);
                warn!(
                    "CRITICAL: Transaction leak detected for client {:?}, running for {:?}, mode: {:?}, sql: {}",
                    client_id, duration, info.mode, info.first_statement
                );
                self.force_closed.fetch_add(1, Ordering::Relaxed);
            }
            // Then check warning threshold
            else if duration >= self.warning_threshold && !info.warning_issued {
                warn!(
                    "Long-running transaction for client {:?}, running for {:?}, mode: {:?}, sql: {}",
                    client_id, duration, info.mode, info.first_statement
                );
                info.warning_issued = true;
                self.warnings_issued.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Remove leaked transactions from tracking (they'll be force-closed)
        for client_id in &leaked {
            txns.remove(client_id);
        }

        leaked
    }

    /// Get statistics about transaction tracking
    pub fn stats(&self) -> TransactionLeakStats {
        let txns = self.active_transactions.read();
        TransactionLeakStats {
            active_transactions: txns.len(),
            warnings_issued: self.warnings_issued.load(Ordering::Relaxed),
            force_closed: self.force_closed.load(Ordering::Relaxed),
            warning_threshold_secs: self.warning_threshold.as_secs(),
            critical_threshold_secs: self.critical_threshold.as_secs(),
        }
    }
}

/// Transaction leak statistics
#[derive(Debug, Clone)]
pub struct TransactionLeakStats {
    /// Currently active transactions being tracked
    pub active_transactions: usize,
    /// Total warnings issued
    pub warnings_issued: u64,
    /// Total transactions force-closed
    pub force_closed: u64,
    /// Warning threshold in seconds
    pub warning_threshold_secs: u64,
    /// Critical threshold in seconds
    pub critical_threshold_secs: u64,
}

/// Connection health validator
///
/// Validates that connections are healthy before returning them from the pool.
#[derive(Debug)]
pub struct ConnectionHealthValidator {
    /// Query to execute for validation
    validation_query: String,
    /// Validation timeout
    timeout: Duration,
    /// Total validations performed
    validations: AtomicU64,
    /// Validation failures
    failures: AtomicU64,
}

impl Default for ConnectionHealthValidator {
    fn default() -> Self {
        Self::new("SELECT 1", Duration::from_secs(5))
    }
}

impl ConnectionHealthValidator {
    /// Create a new health validator
    pub fn new(validation_query: impl Into<String>, timeout: Duration) -> Self {
        Self {
            validation_query: validation_query.into(),
            timeout,
            validations: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }

    /// Get the validation query
    pub fn validation_query(&self) -> &str {
        &self.validation_query
    }

    /// Get the timeout
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Record a validation attempt
    pub fn record_validation(&self, success: bool) {
        self.validations.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get validation statistics
    pub fn stats(&self) -> ValidationStats {
        ValidationStats {
            validations: self.validations.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
        }
    }

    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.validations.load(Ordering::Relaxed);
        let failures = self.failures.load(Ordering::Relaxed);
        if total == 0 {
            1.0
        } else {
            (total - failures) as f64 / total as f64
        }
    }
}

/// Validation statistics
#[derive(Debug, Clone)]
pub struct ValidationStats {
    /// Total validations
    pub validations: u64,
    /// Failed validations
    pub failures: u64,
}

/// Stale lease cleaner
///
/// Identifies and cleans up leases that have been held too long without activity.
#[derive(Debug)]
pub struct StaleLeaseCleaner {
    /// Maximum idle time before a lease is considered stale
    max_idle_time: Duration,
    /// Tracked lease activity: client_id -> last_activity
    lease_activity: RwLock<HashMap<ClientId, Instant>>,
    /// Leases cleaned up
    cleaned_count: AtomicU64,
}

impl Default for StaleLeaseCleaner {
    fn default() -> Self {
        Self::new(Duration::from_secs(1800)) // 30 minutes default
    }
}

impl StaleLeaseCleaner {
    /// Create a new stale lease cleaner
    pub fn new(max_idle_time: Duration) -> Self {
        Self {
            max_idle_time,
            lease_activity: RwLock::new(HashMap::new()),
            cleaned_count: AtomicU64::new(0),
        }
    }

    /// Record activity for a lease
    pub fn record_activity(&self, client_id: ClientId) {
        self.lease_activity.write().insert(client_id, Instant::now());
    }

    /// Remove tracking for a lease
    pub fn lease_released(&self, client_id: &ClientId) {
        self.lease_activity.write().remove(client_id);
    }

    /// Find stale leases that should be cleaned up
    pub fn find_stale_leases(&self) -> Vec<ClientId> {
        let now = Instant::now();
        let activity = self.lease_activity.read();

        activity
            .iter()
            .filter(|(_, last_activity)| now.duration_since(**last_activity) > self.max_idle_time)
            .map(|(client_id, _)| *client_id)
            .collect()
    }

    /// Clean up stale leases and return their IDs
    pub fn clean_stale(&self) -> Vec<ClientId> {
        let stale = self.find_stale_leases();
        let count = stale.len();

        if count > 0 {
            let mut activity = self.lease_activity.write();
            for client_id in &stale {
                activity.remove(client_id);
            }
            self.cleaned_count.fetch_add(count as u64, Ordering::Relaxed);

            info!(
                "Cleaned {} stale leases (idle > {:?})",
                count, self.max_idle_time
            );
        }

        stale
    }

    /// Get cleaned count
    pub fn cleaned_count(&self) -> u64 {
        self.cleaned_count.load(Ordering::Relaxed)
    }
}

/// Pool exhaustion monitor
///
/// Tracks pool exhaustion events and can trigger alerts or backpressure.
#[derive(Debug)]
pub struct PoolExhaustionMonitor {
    /// Maximum queue size before rejecting
    max_queue_size: usize,
    /// Current queue size
    current_queue: AtomicU64,
    /// Total exhaustion events
    exhaustion_events: AtomicU64,
    /// Total requests rejected due to queue full
    rejected_requests: AtomicU64,
    /// Whether to enable backpressure (reject requests when pool full)
    enable_backpressure: bool,
}

impl Default for PoolExhaustionMonitor {
    fn default() -> Self {
        Self::new(1000, true)
    }
}

impl PoolExhaustionMonitor {
    /// Create a new exhaustion monitor
    pub fn new(max_queue_size: usize, enable_backpressure: bool) -> Self {
        Self {
            max_queue_size,
            current_queue: AtomicU64::new(0),
            exhaustion_events: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
            enable_backpressure,
        }
    }

    /// Check if a request should be queued or rejected
    ///
    /// Returns Ok(()) if request can proceed, Err if should be rejected
    pub fn check_capacity(&self) -> Result<()> {
        let queue_size = self.current_queue.load(Ordering::Relaxed);

        if self.enable_backpressure && queue_size >= self.max_queue_size as u64 {
            self.rejected_requests.fetch_add(1, Ordering::Relaxed);
            return Err(ProxyError::PoolExhausted(format!(
                "Pool queue full ({} waiting), request rejected",
                queue_size
            )));
        }

        Ok(())
    }

    /// Record entering the wait queue
    pub fn enter_queue(&self) {
        let prev = self.current_queue.fetch_add(1, Ordering::Relaxed);
        if prev == 0 {
            // First waiter means pool is exhausted
            self.exhaustion_events.fetch_add(1, Ordering::Relaxed);
            debug!("Pool exhaustion event - requests now queuing");
        }
    }

    /// Record leaving the wait queue (got a connection)
    pub fn leave_queue(&self) {
        self.current_queue.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current queue size
    pub fn queue_size(&self) -> u64 {
        self.current_queue.load(Ordering::Relaxed)
    }

    /// Get exhaustion statistics
    pub fn stats(&self) -> ExhaustionStats {
        ExhaustionStats {
            current_queue: self.current_queue.load(Ordering::Relaxed),
            max_queue_size: self.max_queue_size as u64,
            exhaustion_events: self.exhaustion_events.load(Ordering::Relaxed),
            rejected_requests: self.rejected_requests.load(Ordering::Relaxed),
            backpressure_enabled: self.enable_backpressure,
        }
    }
}

/// Pool exhaustion statistics
#[derive(Debug, Clone)]
pub struct ExhaustionStats {
    /// Current requests waiting in queue
    pub current_queue: u64,
    /// Maximum queue size
    pub max_queue_size: u64,
    /// Total exhaustion events
    pub exhaustion_events: u64,
    /// Total rejected requests
    pub rejected_requests: u64,
    /// Whether backpressure is enabled
    pub backpressure_enabled: bool,
}

/// Combined hardening features
#[derive(Debug)]
pub struct PoolHardening {
    /// Transaction leak detector
    pub leak_detector: TransactionLeakDetector,
    /// Connection health validator
    pub health_validator: ConnectionHealthValidator,
    /// Stale lease cleaner
    pub stale_cleaner: StaleLeaseCleaner,
    /// Pool exhaustion monitor
    pub exhaustion_monitor: PoolExhaustionMonitor,
}

impl Default for PoolHardening {
    fn default() -> Self {
        Self {
            leak_detector: TransactionLeakDetector::default(),
            health_validator: ConnectionHealthValidator::default(),
            stale_cleaner: StaleLeaseCleaner::default(),
            exhaustion_monitor: PoolExhaustionMonitor::default(),
        }
    }
}

impl PoolHardening {
    /// Create with custom configuration
    pub fn new(
        tx_warning_threshold: Duration,
        tx_critical_threshold: Duration,
        validation_query: &str,
        validation_timeout: Duration,
        max_lease_idle: Duration,
        max_queue_size: usize,
        enable_backpressure: bool,
    ) -> Self {
        Self {
            leak_detector: TransactionLeakDetector::new(tx_warning_threshold, tx_critical_threshold),
            health_validator: ConnectionHealthValidator::new(validation_query, validation_timeout),
            stale_cleaner: StaleLeaseCleaner::new(max_lease_idle),
            exhaustion_monitor: PoolExhaustionMonitor::new(max_queue_size, enable_backpressure),
        }
    }

    /// Run periodic maintenance
    ///
    /// Returns (leaked_txns, stale_leases) that need to be cleaned up
    pub fn run_maintenance(&self) -> (Vec<ClientId>, Vec<ClientId>) {
        let leaked = self.leak_detector.check_for_leaks();
        let stale = self.stale_cleaner.clean_stale();
        (leaked, stale)
    }

    /// Get combined statistics
    pub fn stats(&self) -> HardeningStats {
        HardeningStats {
            leak_stats: self.leak_detector.stats(),
            validation_stats: self.health_validator.stats(),
            exhaustion_stats: self.exhaustion_monitor.stats(),
            stale_cleaned: self.stale_cleaner.cleaned_count(),
        }
    }
}

/// Combined hardening statistics
#[derive(Debug, Clone)]
pub struct HardeningStats {
    /// Transaction leak statistics
    pub leak_stats: TransactionLeakStats,
    /// Validation statistics
    pub validation_stats: ValidationStats,
    /// Exhaustion statistics
    pub exhaustion_stats: ExhaustionStats,
    /// Stale leases cleaned
    pub stale_cleaned: u64,
}

/// Truncate SQL for logging
fn truncate_sql(sql: &str, max_len: usize) -> String {
    if sql.len() <= max_len {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_leak_detector() {
        let detector = TransactionLeakDetector::new(
            Duration::from_millis(10),
            Duration::from_millis(50),
        );

        let client1 = ClientId::new();
        let client2 = ClientId::new();

        // Start two transactions
        detector.transaction_started(client1, PoolingMode::Transaction, "BEGIN; SELECT * FROM users");
        detector.transaction_started(client2, PoolingMode::Statement, "SELECT 1");

        // No leaks immediately
        assert!(detector.check_for_leaks().is_empty());

        // End one transaction
        detector.transaction_ended(&client2);

        // Wait for warning threshold
        std::thread::sleep(Duration::from_millis(15));
        let leaked = detector.check_for_leaks();
        assert!(leaked.is_empty()); // Warning issued but not critical yet

        // Wait for critical threshold
        std::thread::sleep(Duration::from_millis(40));
        let leaked = detector.check_for_leaks();
        assert_eq!(leaked.len(), 1);
        assert_eq!(leaked[0], client1);
    }

    #[test]
    fn test_connection_health_validator() {
        let validator = ConnectionHealthValidator::default();

        validator.record_validation(true);
        validator.record_validation(true);
        validator.record_validation(false);

        assert_eq!(validator.stats().validations, 3);
        assert_eq!(validator.stats().failures, 1);
        assert!((validator.success_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_stale_lease_cleaner() {
        let cleaner = StaleLeaseCleaner::new(Duration::from_millis(20));

        let client1 = ClientId::new();
        let client2 = ClientId::new();

        cleaner.record_activity(client1);
        cleaner.record_activity(client2);

        // No stale leases immediately
        assert!(cleaner.find_stale_leases().is_empty());

        // Wait and only update client1
        std::thread::sleep(Duration::from_millis(25));
        cleaner.record_activity(client1);

        // client2 should be stale
        let stale = cleaner.clean_stale();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0], client2);
        assert_eq!(cleaner.cleaned_count(), 1);
    }

    #[test]
    fn test_pool_exhaustion_monitor() {
        let monitor = PoolExhaustionMonitor::new(2, true);

        // First two requests OK
        assert!(monitor.check_capacity().is_ok());
        monitor.enter_queue();
        assert!(monitor.check_capacity().is_ok());
        monitor.enter_queue();

        // Third should be rejected (backpressure)
        assert!(monitor.check_capacity().is_err());
        assert_eq!(monitor.stats().rejected_requests, 1);

        // Leave queue
        monitor.leave_queue();
        assert!(monitor.check_capacity().is_ok());
    }

    #[test]
    fn test_pool_hardening_combined() {
        let hardening = PoolHardening::default();

        // Run maintenance on empty state
        let (leaked, stale) = hardening.run_maintenance();
        assert!(leaked.is_empty());
        assert!(stale.is_empty());

        // Check stats
        let stats = hardening.stats();
        assert_eq!(stats.leak_stats.active_transactions, 0);
        assert_eq!(stats.stale_cleaned, 0);
    }
}
