//! Analytics Configuration
//!
//! Configuration for query analytics, slow query log, and pattern detection.

use std::time::Duration;
use std::path::PathBuf;

/// Analytics configuration
#[derive(Debug, Clone)]
pub struct AnalyticsConfig {
    /// Enable analytics
    pub enabled: bool,

    /// Normalize queries (replace literals with placeholders)
    pub normalize_queries: bool,

    /// Track parameter values (privacy consideration)
    pub track_parameters: bool,

    /// Statistics retention duration
    pub retention: Duration,

    /// Maximum fingerprints to track
    pub max_fingerprints: usize,

    /// Slow query configuration
    pub slow_query: SlowQueryConfig,

    /// Pattern detection configuration
    pub patterns: PatternConfig,

    /// Sampling configuration
    pub sampling: SamplingConfig,

    /// Alert configuration
    pub alerts: AlertConfig,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            normalize_queries: true,
            track_parameters: false,
            retention: Duration::from_secs(7 * 24 * 3600), // 7 days
            max_fingerprints: 10000,
            slow_query: SlowQueryConfig::default(),
            patterns: PatternConfig::default(),
            sampling: SamplingConfig::default(),
            alerts: AlertConfig::default(),
        }
    }
}

/// Builder for AnalyticsConfig
#[derive(Debug, Default)]
pub struct AnalyticsConfigBuilder {
    config: AnalyticsConfig,
}

impl AnalyticsConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    pub fn normalize_queries(mut self, normalize: bool) -> Self {
        self.config.normalize_queries = normalize;
        self
    }

    pub fn track_parameters(mut self, track: bool) -> Self {
        self.config.track_parameters = track;
        self
    }

    pub fn retention_days(mut self, days: u64) -> Self {
        self.config.retention = Duration::from_secs(days * 24 * 3600);
        self
    }

    pub fn max_fingerprints(mut self, max: usize) -> Self {
        self.config.max_fingerprints = max;
        self
    }

    pub fn slow_query(mut self, config: SlowQueryConfig) -> Self {
        self.config.slow_query = config;
        self
    }

    pub fn patterns(mut self, config: PatternConfig) -> Self {
        self.config.patterns = config;
        self
    }

    pub fn sampling(mut self, config: SamplingConfig) -> Self {
        self.config.sampling = config;
        self
    }

    pub fn alerts(mut self, config: AlertConfig) -> Self {
        self.config.alerts = config;
        self
    }

    pub fn build(self) -> AnalyticsConfig {
        self.config
    }
}

impl AnalyticsConfig {
    pub fn builder() -> AnalyticsConfigBuilder {
        AnalyticsConfigBuilder::new()
    }
}

/// Slow query log configuration
#[derive(Debug, Clone)]
pub struct SlowQueryConfig {
    /// Enable slow query logging
    pub enabled: bool,

    /// Threshold duration for slow queries
    pub threshold: Duration,

    /// Log file path (None for in-memory only)
    pub log_file: Option<PathBuf>,

    /// Log parameter values
    pub log_parameters: bool,

    /// Maximum query length to log
    pub max_query_length: usize,

    /// Maximum recent entries to keep in memory
    pub max_recent_entries: usize,
}

impl Default for SlowQueryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: Duration::from_secs(1),
            log_file: None,
            log_parameters: false,
            max_query_length: 4096,
            max_recent_entries: 1000,
        }
    }
}

impl SlowQueryConfig {
    pub fn with_threshold_ms(mut self, ms: u64) -> Self {
        self.threshold = Duration::from_millis(ms);
        self
    }

    pub fn with_threshold_secs(mut self, secs: u64) -> Self {
        self.threshold = Duration::from_secs(secs);
        self
    }

    pub fn with_log_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.log_file = Some(path.into());
        self
    }

    pub fn with_max_recent(mut self, max: usize) -> Self {
        self.max_recent_entries = max;
        self
    }
}

/// Pattern detection configuration
#[derive(Debug, Clone)]
pub struct PatternConfig {
    /// Enable N+1 query detection
    pub n_plus_one_detection: bool,

    /// Threshold for N+1 detection (min repeated queries)
    pub n_plus_one_threshold: usize,

    /// Enable burst detection
    pub burst_detection: bool,

    /// Threshold for burst detection (queries per window)
    pub burst_threshold: usize,

    /// Burst detection window
    pub burst_window: Duration,

    /// Session query history size
    pub session_history_size: usize,

    /// Session timeout (cleanup inactive sessions)
    pub session_timeout: Duration,

    /// Maximum sessions to track
    pub max_sessions: usize,
}

impl Default for PatternConfig {
    fn default() -> Self {
        Self {
            n_plus_one_detection: true,
            n_plus_one_threshold: 5,
            burst_detection: true,
            burst_threshold: 50,
            burst_window: Duration::from_millis(100),
            session_history_size: 100,
            session_timeout: Duration::from_secs(300),
            max_sessions: 10000,
        }
    }
}

impl PatternConfig {
    pub fn with_n_plus_one_threshold(mut self, threshold: usize) -> Self {
        self.n_plus_one_threshold = threshold;
        self
    }

    pub fn with_burst_threshold(mut self, threshold: usize) -> Self {
        self.burst_threshold = threshold;
        self
    }

    pub fn disable_patterns(mut self) -> Self {
        self.n_plus_one_detection = false;
        self.burst_detection = false;
        self
    }
}

/// Sampling configuration
#[derive(Debug, Clone)]
pub struct SamplingConfig {
    /// Enable sampling
    pub enabled: bool,

    /// Sample rate (0.0 - 1.0)
    pub rate: f64,

    /// Always sample slow queries
    pub always_sample_slow: bool,

    /// Always sample errors
    pub always_sample_errors: bool,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rate: 1.0,
            always_sample_slow: true,
            always_sample_errors: true,
        }
    }
}

impl SamplingConfig {
    pub fn with_rate(mut self, rate: f64) -> Self {
        self.rate = rate.clamp(0.0, 1.0);
        self.enabled = rate < 1.0;
        self
    }
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Slow query alert threshold
    pub slow_query_threshold: Duration,

    /// Error rate threshold (0.0 - 1.0)
    pub error_rate_threshold: f64,

    /// N+1 detection alert
    pub alert_on_n_plus_one: bool,

    /// Burst detection alert
    pub alert_on_burst: bool,

    /// Webhook URL for alerts
    pub webhook_url: Option<String>,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            slow_query_threshold: Duration::from_secs(5),
            error_rate_threshold: 0.05,
            alert_on_n_plus_one: true,
            alert_on_burst: true,
            webhook_url: None,
        }
    }
}

impl AlertConfig {
    pub fn with_webhook(mut self, url: impl Into<String>) -> Self {
        self.webhook_url = Some(url.into());
        self
    }

    pub fn with_slow_threshold_secs(mut self, secs: u64) -> Self {
        self.slow_query_threshold = Duration::from_secs(secs);
        self
    }

    pub fn with_error_rate(mut self, rate: f64) -> Self {
        self.error_rate_threshold = rate.clamp(0.0, 1.0);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AnalyticsConfig::default();
        assert!(config.enabled);
        assert!(config.normalize_queries);
        assert!(!config.track_parameters);
        assert_eq!(config.max_fingerprints, 10000);
    }

    #[test]
    fn test_builder() {
        let config = AnalyticsConfig::builder()
            .enabled(true)
            .max_fingerprints(5000)
            .retention_days(14)
            .build();

        assert!(config.enabled);
        assert_eq!(config.max_fingerprints, 5000);
        assert_eq!(config.retention, Duration::from_secs(14 * 24 * 3600));
    }

    #[test]
    fn test_slow_query_config() {
        let config = SlowQueryConfig::default()
            .with_threshold_ms(500)
            .with_max_recent(2000);

        assert_eq!(config.threshold, Duration::from_millis(500));
        assert_eq!(config.max_recent_entries, 2000);
    }

    #[test]
    fn test_pattern_config() {
        let config = PatternConfig::default()
            .with_n_plus_one_threshold(10)
            .with_burst_threshold(100);

        assert_eq!(config.n_plus_one_threshold, 10);
        assert_eq!(config.burst_threshold, 100);
    }

    #[test]
    fn test_sampling_config() {
        let config = SamplingConfig::default().with_rate(0.1);
        assert!(config.enabled);
        assert!((config.rate - 0.1).abs() < 0.001);
    }
}
