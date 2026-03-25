//! Lag routing configuration types
//!
//! Configuration for replica lag monitoring and routing decisions.

use std::collections::HashMap;
use std::time::Duration;

use super::SyncMode;

/// Main configuration for lag-aware routing
#[derive(Debug, Clone)]
pub struct LagRoutingConfig {
    /// Enable lag-aware routing
    pub enabled: bool,

    /// Interval between lag status polls
    pub poll_interval: Duration,

    /// Method for calculating lag
    pub lag_calculation: LagCalculation,

    /// Default maximum acceptable lag for reads
    pub default_max_lag: Duration,

    /// Threshold for considering a node "fresh"
    pub fresh_threshold: Duration,

    /// Threshold for marking a node unhealthy due to lag
    pub stale_threshold: Duration,

    /// Whether to fall back to primary when all standbys are too laggy
    pub fallback_to_primary: bool,

    /// Lag threshold that triggers primary fallback
    pub fallback_threshold: Duration,

    /// Enable read-your-writes tracking
    pub read_your_writes: bool,

    /// How long to retain RYW LSN requirements
    pub ryw_retention: Duration,

    /// Per-sync-mode lag limits
    pub sync_mode_limits: HashMap<SyncMode, SyncModeLagConfig>,

    /// Enable lag trend smoothing to avoid oscillation
    pub enable_smoothing: bool,

    /// Number of samples for trend smoothing
    pub smoothing_window: usize,

    /// Minimum samples before trusting lag data
    pub min_samples: usize,
}

impl Default for LagRoutingConfig {
    fn default() -> Self {
        let mut sync_mode_limits = HashMap::new();
        sync_mode_limits.insert(
            SyncMode::Sync,
            SyncModeLagConfig {
                max_lag: Duration::from_millis(0),
                weight: 1.0,
            },
        );
        sync_mode_limits.insert(
            SyncMode::SemiSync,
            SyncModeLagConfig {
                max_lag: Duration::from_millis(500),
                weight: 0.8,
            },
        );
        sync_mode_limits.insert(
            SyncMode::Async,
            SyncModeLagConfig {
                max_lag: Duration::from_secs(10),
                weight: 0.5,
            },
        );

        Self {
            enabled: true,
            poll_interval: Duration::from_millis(100),
            lag_calculation: LagCalculation::default(),
            default_max_lag: Duration::from_secs(1),
            fresh_threshold: Duration::from_millis(100),
            stale_threshold: Duration::from_secs(10),
            fallback_to_primary: true,
            fallback_threshold: Duration::from_secs(5),
            read_your_writes: true,
            ryw_retention: Duration::from_secs(300), // 5 minutes
            sync_mode_limits,
            enable_smoothing: true,
            smoothing_window: 10,
            min_samples: 3,
        }
    }
}

impl LagRoutingConfig {
    /// Create a new config with all defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder pattern: set poll interval
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Builder pattern: set default max lag
    pub fn with_default_max_lag(mut self, lag: Duration) -> Self {
        self.default_max_lag = lag;
        self
    }

    /// Builder pattern: set fallback threshold
    pub fn with_fallback_threshold(mut self, threshold: Duration) -> Self {
        self.fallback_threshold = threshold;
        self
    }

    /// Builder pattern: enable/disable read-your-writes
    pub fn with_read_your_writes(mut self, enabled: bool) -> Self {
        self.read_your_writes = enabled;
        self
    }

    /// Builder pattern: set RYW retention
    pub fn with_ryw_retention(mut self, retention: Duration) -> Self {
        self.ryw_retention = retention;
        self
    }

    /// Builder pattern: set lag calculation method
    pub fn with_lag_calculation(mut self, method: LagCalculation) -> Self {
        self.lag_calculation = method;
        self
    }

    /// Builder pattern: enable/disable smoothing
    pub fn with_smoothing(mut self, enabled: bool, window: usize) -> Self {
        self.enable_smoothing = enabled;
        self.smoothing_window = window;
        self
    }

    /// Get the max lag allowed for a sync mode
    pub fn get_sync_mode_max_lag(&self, mode: SyncMode) -> Duration {
        self.sync_mode_limits
            .get(&mode)
            .map(|c| c.max_lag)
            .unwrap_or(self.default_max_lag)
    }

    /// Get the weight for a sync mode (for load balancing)
    pub fn get_sync_mode_weight(&self, mode: SyncMode) -> f64 {
        self.sync_mode_limits
            .get(&mode)
            .map(|c| c.weight)
            .unwrap_or(1.0)
    }
}

/// Configuration for a specific sync mode
#[derive(Debug, Clone)]
pub struct SyncModeLagConfig {
    /// Maximum acceptable lag for this sync mode
    pub max_lag: Duration,

    /// Weight for load balancing (higher = more preferred)
    pub weight: f64,
}

/// Method for calculating replication lag
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LagCalculation {
    /// WAL-based lag (LSN difference converted to time)
    Wal {
        /// Estimated WAL bytes per second for time conversion
        bytes_per_second: u64,
    },

    /// Time-based lag (last transaction timestamp)
    Time,

    /// Hybrid (use both WAL and time, take maximum)
    Hybrid {
        /// Estimated WAL bytes per second for time conversion
        bytes_per_second: u64,
    },
}

impl Default for LagCalculation {
    fn default() -> Self {
        // Default to hybrid with 50KB/s WAL rate estimate
        LagCalculation::Hybrid {
            bytes_per_second: 50_000,
        }
    }
}

impl LagCalculation {
    /// Create WAL-based calculation
    pub fn wal(bytes_per_second: u64) -> Self {
        LagCalculation::Wal { bytes_per_second }
    }

    /// Create time-based calculation
    pub fn time() -> Self {
        LagCalculation::Time
    }

    /// Create hybrid calculation
    pub fn hybrid(bytes_per_second: u64) -> Self {
        LagCalculation::Hybrid { bytes_per_second }
    }

    /// Calculate lag duration from byte lag
    pub fn calculate_lag(&self, lag_bytes: u64, time_lag: Option<Duration>) -> Duration {
        match self {
            LagCalculation::Wal { bytes_per_second } => {
                if *bytes_per_second == 0 {
                    return Duration::ZERO;
                }
                Duration::from_secs_f64(lag_bytes as f64 / *bytes_per_second as f64)
            }
            LagCalculation::Time => time_lag.unwrap_or(Duration::ZERO),
            LagCalculation::Hybrid { bytes_per_second } => {
                let wal_lag = if *bytes_per_second > 0 {
                    Duration::from_secs_f64(lag_bytes as f64 / *bytes_per_second as f64)
                } else {
                    Duration::ZERO
                };
                let time = time_lag.unwrap_or(Duration::ZERO);
                wal_lag.max(time)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LagRoutingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert_eq!(config.default_max_lag, Duration::from_secs(1));
        assert!(config.fallback_to_primary);
        assert!(config.read_your_writes);
    }

    #[test]
    fn test_builder_pattern() {
        let config = LagRoutingConfig::new()
            .with_poll_interval(Duration::from_millis(50))
            .with_default_max_lag(Duration::from_millis(500))
            .with_read_your_writes(false);

        assert_eq!(config.poll_interval, Duration::from_millis(50));
        assert_eq!(config.default_max_lag, Duration::from_millis(500));
        assert!(!config.read_your_writes);
    }

    #[test]
    fn test_sync_mode_limits() {
        let config = LagRoutingConfig::default();
        assert_eq!(
            config.get_sync_mode_max_lag(SyncMode::Sync),
            Duration::from_millis(0)
        );
        assert_eq!(
            config.get_sync_mode_max_lag(SyncMode::SemiSync),
            Duration::from_millis(500)
        );
        assert_eq!(
            config.get_sync_mode_max_lag(SyncMode::Async),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn test_lag_calculation_wal() {
        let calc = LagCalculation::wal(1000); // 1000 bytes/sec
        let lag = calc.calculate_lag(5000, None);
        assert_eq!(lag, Duration::from_secs(5));
    }

    #[test]
    fn test_lag_calculation_time() {
        let calc = LagCalculation::time();
        let lag = calc.calculate_lag(5000, Some(Duration::from_secs(3)));
        assert_eq!(lag, Duration::from_secs(3));
    }

    #[test]
    fn test_lag_calculation_hybrid() {
        let calc = LagCalculation::hybrid(1000); // 1000 bytes/sec
                                                  // WAL lag = 5s, time lag = 3s -> max = 5s
        let lag = calc.calculate_lag(5000, Some(Duration::from_secs(3)));
        assert_eq!(lag, Duration::from_secs(5));

        // WAL lag = 2s, time lag = 4s -> max = 4s
        let lag2 = calc.calculate_lag(2000, Some(Duration::from_secs(4)));
        assert_eq!(lag2, Duration::from_secs(4));
    }
}
