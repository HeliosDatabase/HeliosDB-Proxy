//! Circuit Breaker Configuration
//!
//! Configuration types for circuit breaker behavior including failure thresholds,
//! cooldown periods, and failure conditions.

use std::collections::HashSet;
use std::time::Duration;

/// Configuration for a circuit breaker instance
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures to trigger open state
    pub failure_threshold: u32,

    /// Time window for counting failures (seconds)
    pub failure_window: Duration,

    /// Time to wait before trying half-open (seconds)
    pub cooldown: Duration,

    /// Number of successful probes needed to close circuit
    pub half_open_success_threshold: u32,

    /// Maximum concurrent probe requests in half-open state
    pub half_open_max_probes: u32,

    /// Conditions that count as failures
    pub failure_conditions: FailureConditions,

    /// Enable adaptive thresholds based on historical data
    pub adaptive_enabled: bool,

    /// Sync mode-specific thresholds
    pub sync_mode_thresholds: Option<SyncModeThresholds>,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            failure_window: Duration::from_secs(30),
            cooldown: Duration::from_secs(10),
            half_open_success_threshold: 3,
            half_open_max_probes: 2,
            failure_conditions: FailureConditions::default(),
            adaptive_enabled: false,
            sync_mode_thresholds: None,
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a new builder for configuration
    pub fn builder() -> CircuitBreakerConfigBuilder {
        CircuitBreakerConfigBuilder::default()
    }

    /// Get effective failure threshold (may be adjusted for sync mode)
    pub fn effective_threshold(&self, sync_mode: Option<&str>) -> u32 {
        if let (Some(thresholds), Some(mode)) = (&self.sync_mode_thresholds, sync_mode) {
            match mode.to_lowercase().as_str() {
                "sync" | "synchronous" => thresholds.sync_threshold,
                "semisync" | "semi-sync" => thresholds.semisync_threshold,
                "async" | "asynchronous" => thresholds.async_threshold,
                _ => self.failure_threshold,
            }
        } else {
            self.failure_threshold
        }
    }

    /// Get effective cooldown (may be adjusted for sync mode)
    pub fn effective_cooldown(&self, sync_mode: Option<&str>) -> Duration {
        if let (Some(thresholds), Some(mode)) = (&self.sync_mode_thresholds, sync_mode) {
            match mode.to_lowercase().as_str() {
                "sync" | "synchronous" => thresholds.sync_cooldown,
                "semisync" | "semi-sync" => thresholds.semisync_cooldown,
                "async" | "asynchronous" => thresholds.async_cooldown,
                _ => self.cooldown,
            }
        } else {
            self.cooldown
        }
    }
}

/// Builder for circuit breaker configuration
#[derive(Debug, Default)]
pub struct CircuitBreakerConfigBuilder {
    failure_threshold: Option<u32>,
    failure_window_secs: Option<u64>,
    cooldown_secs: Option<u64>,
    half_open_success_threshold: Option<u32>,
    half_open_max_probes: Option<u32>,
    failure_conditions: Option<FailureConditions>,
    adaptive_enabled: Option<bool>,
    sync_mode_thresholds: Option<SyncModeThresholds>,
}

impl CircuitBreakerConfigBuilder {
    /// Set failure threshold (number of failures to trigger open)
    pub fn failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = Some(threshold);
        self
    }

    /// Set failure counting window in seconds
    pub fn failure_window_secs(mut self, secs: u64) -> Self {
        self.failure_window_secs = Some(secs);
        self
    }

    /// Set cooldown period in seconds
    pub fn cooldown_secs(mut self, secs: u64) -> Self {
        self.cooldown_secs = Some(secs);
        self
    }

    /// Set number of successes needed in half-open to close
    pub fn half_open_success_threshold(mut self, threshold: u32) -> Self {
        self.half_open_success_threshold = Some(threshold);
        self
    }

    /// Set maximum concurrent probes in half-open
    pub fn half_open_max_probes(mut self, max: u32) -> Self {
        self.half_open_max_probes = Some(max);
        self
    }

    /// Set failure conditions
    pub fn failure_conditions(mut self, conditions: FailureConditions) -> Self {
        self.failure_conditions = Some(conditions);
        self
    }

    /// Enable adaptive thresholds
    pub fn adaptive(mut self, enabled: bool) -> Self {
        self.adaptive_enabled = Some(enabled);
        self
    }

    /// Set sync mode-specific thresholds
    pub fn sync_mode_thresholds(mut self, thresholds: SyncModeThresholds) -> Self {
        self.sync_mode_thresholds = Some(thresholds);
        self
    }

    /// Build the configuration
    pub fn build(self) -> CircuitBreakerConfig {
        let default = CircuitBreakerConfig::default();
        CircuitBreakerConfig {
            failure_threshold: self.failure_threshold.unwrap_or(default.failure_threshold),
            failure_window: self
                .failure_window_secs
                .map(Duration::from_secs)
                .unwrap_or(default.failure_window),
            cooldown: self
                .cooldown_secs
                .map(Duration::from_secs)
                .unwrap_or(default.cooldown),
            half_open_success_threshold: self
                .half_open_success_threshold
                .unwrap_or(default.half_open_success_threshold),
            half_open_max_probes: self
                .half_open_max_probes
                .unwrap_or(default.half_open_max_probes),
            failure_conditions: self.failure_conditions.unwrap_or(default.failure_conditions),
            adaptive_enabled: self.adaptive_enabled.unwrap_or(default.adaptive_enabled),
            sync_mode_thresholds: self.sync_mode_thresholds,
        }
    }
}

/// Conditions that define what counts as a failure
#[derive(Debug, Clone)]
pub struct FailureConditions {
    /// Request timeout threshold
    pub timeout: Duration,

    /// Error codes that count as failures
    pub error_codes: HashSet<String>,

    /// Response time threshold (slow responses count as failures)
    pub slow_threshold: Option<Duration>,

    /// Ignore transient/retryable errors
    pub ignore_transient: bool,

    /// Include connection errors as failures
    pub count_connection_errors: bool,

    /// Include timeouts as failures
    pub count_timeouts: bool,
}

impl Default for FailureConditions {
    fn default() -> Self {
        let mut error_codes = HashSet::new();
        // PostgreSQL error codes for serious failures
        error_codes.insert("08001".to_string()); // SQL client unable to establish SQL connection
        error_codes.insert("08004".to_string()); // SQL server rejected establishment of SQL connection
        error_codes.insert("08006".to_string()); // Connection failure
        error_codes.insert("57P01".to_string()); // Admin shutdown
        error_codes.insert("57P02".to_string()); // Crash shutdown
        error_codes.insert("57P03".to_string()); // Cannot connect now
        error_codes.insert("XX000".to_string()); // Internal error
        error_codes.insert("XX001".to_string()); // Data corrupted
        error_codes.insert("XX002".to_string()); // Index corrupted

        Self {
            timeout: Duration::from_secs(5),
            error_codes,
            slow_threshold: Some(Duration::from_secs(2)),
            ignore_transient: true,
            count_connection_errors: true,
            count_timeouts: true,
        }
    }
}

impl FailureConditions {
    /// Create new failure conditions
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout threshold
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Add error code
    pub fn with_error_code(mut self, code: &str) -> Self {
        self.error_codes.insert(code.to_string());
        self
    }

    /// Set slow threshold
    pub fn with_slow_threshold(mut self, threshold: Duration) -> Self {
        self.slow_threshold = Some(threshold);
        self
    }

    /// Set ignore transient flag
    pub fn ignore_transient(mut self, ignore: bool) -> Self {
        self.ignore_transient = ignore;
        self
    }

    /// Check if an error code is a failure
    pub fn is_failure_code(&self, code: &str) -> bool {
        self.error_codes.contains(code)
    }

    /// Check if a response time is considered slow/failure
    pub fn is_slow(&self, response_time: Duration) -> bool {
        self.slow_threshold
            .map(|threshold| response_time > threshold)
            .unwrap_or(false)
    }

    /// Check if a timeout occurred
    pub fn is_timeout(&self, response_time: Duration) -> bool {
        response_time > self.timeout
    }
}

/// Sync mode-specific thresholds
#[derive(Debug, Clone)]
pub struct SyncModeThresholds {
    /// Threshold for synchronous standbys (most sensitive)
    pub sync_threshold: u32,
    /// Cooldown for sync standbys
    pub sync_cooldown: Duration,

    /// Threshold for semi-synchronous standbys
    pub semisync_threshold: u32,
    /// Cooldown for semisync standbys
    pub semisync_cooldown: Duration,

    /// Threshold for asynchronous standbys (least sensitive)
    pub async_threshold: u32,
    /// Cooldown for async standbys
    pub async_cooldown: Duration,
}

impl Default for SyncModeThresholds {
    fn default() -> Self {
        Self {
            sync_threshold: 3,
            sync_cooldown: Duration::from_secs(5),
            semisync_threshold: 5,
            semisync_cooldown: Duration::from_secs(10),
            async_threshold: 10,
            async_cooldown: Duration::from_secs(30),
        }
    }
}

impl SyncModeThresholds {
    /// Create new sync mode thresholds
    pub fn new() -> Self {
        Self::default()
    }

    /// Set sync mode thresholds
    pub fn with_sync(mut self, threshold: u32, cooldown_secs: u64) -> Self {
        self.sync_threshold = threshold;
        self.sync_cooldown = Duration::from_secs(cooldown_secs);
        self
    }

    /// Set semisync mode thresholds
    pub fn with_semisync(mut self, threshold: u32, cooldown_secs: u64) -> Self {
        self.semisync_threshold = threshold;
        self.semisync_cooldown = Duration::from_secs(cooldown_secs);
        self
    }

    /// Set async mode thresholds
    pub fn with_async(mut self, threshold: u32, cooldown_secs: u64) -> Self {
        self.async_threshold = threshold;
        self.async_cooldown = Duration::from_secs(cooldown_secs);
        self
    }
}

/// Per-node configuration override
#[derive(Debug, Clone)]
pub struct NodeOverride {
    /// Node identifier
    pub node_id: String,
    /// Override failure threshold
    pub failure_threshold: Option<u32>,
    /// Override cooldown period
    pub cooldown: Option<Duration>,
    /// Override half-open success threshold
    pub half_open_success_threshold: Option<u32>,
}

impl NodeOverride {
    /// Create a new node override
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            failure_threshold: None,
            cooldown: None,
            half_open_success_threshold: None,
        }
    }

    /// Set failure threshold override
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = Some(threshold);
        self
    }

    /// Set cooldown override
    pub fn with_cooldown_secs(mut self, secs: u64) -> Self {
        self.cooldown = Some(Duration::from_secs(secs));
        self
    }

    /// Set half-open success threshold override
    pub fn with_half_open_success_threshold(mut self, threshold: u32) -> Self {
        self.half_open_success_threshold = Some(threshold);
        self
    }

    /// Apply override to base config
    pub fn apply_to(&self, base: &CircuitBreakerConfig) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: self.failure_threshold.unwrap_or(base.failure_threshold),
            cooldown: self.cooldown.unwrap_or(base.cooldown),
            half_open_success_threshold: self
                .half_open_success_threshold
                .unwrap_or(base.half_open_success_threshold),
            ..base.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.cooldown, Duration::from_secs(10));
        assert_eq!(config.half_open_success_threshold, 3);
    }

    #[test]
    fn test_config_builder() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(10)
            .failure_window_secs(60)
            .cooldown_secs(20)
            .half_open_success_threshold(5)
            .adaptive(true)
            .build();

        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.failure_window, Duration::from_secs(60));
        assert_eq!(config.cooldown, Duration::from_secs(20));
        assert_eq!(config.half_open_success_threshold, 5);
        assert!(config.adaptive_enabled);
    }

    #[test]
    fn test_failure_conditions() {
        let conditions = FailureConditions::default();

        assert!(conditions.is_failure_code("08001"));
        assert!(conditions.is_failure_code("57P01"));
        assert!(!conditions.is_failure_code("42P01")); // Undefined table

        assert!(conditions.is_slow(Duration::from_secs(3)));
        assert!(!conditions.is_slow(Duration::from_secs(1)));
    }

    #[test]
    fn test_sync_mode_thresholds() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(5)
            .sync_mode_thresholds(SyncModeThresholds::default())
            .build();

        assert_eq!(config.effective_threshold(Some("sync")), 3);
        assert_eq!(config.effective_threshold(Some("semisync")), 5);
        assert_eq!(config.effective_threshold(Some("async")), 10);
        assert_eq!(config.effective_threshold(None), 5);
    }

    #[test]
    fn test_node_override() {
        let base = CircuitBreakerConfig::default();
        let override_ = NodeOverride::new("special-node")
            .with_failure_threshold(20)
            .with_cooldown_secs(60);

        let merged = override_.apply_to(&base);
        assert_eq!(merged.failure_threshold, 20);
        assert_eq!(merged.cooldown, Duration::from_secs(60));
        assert_eq!(merged.half_open_success_threshold, base.half_open_success_threshold);
    }
}
