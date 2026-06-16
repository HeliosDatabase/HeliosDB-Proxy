//! Rate Limit Configuration
//!
//! Configuration types and builders for rate limiting.

use std::collections::HashMap;
use std::time::Duration;

use super::limiter::LimiterKey;

/// Priority levels for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum PriorityLevel {
    /// Low priority - accept more throttling
    Low = 0,
    /// Normal priority (default)
    #[default]
    Normal = 1,
    /// High priority - bypass some limits
    High = 2,
    /// Critical priority - minimal throttling
    Critical = 3,
}

impl std::fmt::Display for PriorityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriorityLevel::Low => write!(f, "low"),
            PriorityLevel::Normal => write!(f, "normal"),
            PriorityLevel::High => write!(f, "high"),
            PriorityLevel::Critical => write!(f, "critical"),
        }
    }
}

impl std::str::FromStr for PriorityLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "low" => Ok(PriorityLevel::Low),
            "normal" | "default" => Ok(PriorityLevel::Normal),
            "high" => Ok(PriorityLevel::High),
            "critical" | "urgent" => Ok(PriorityLevel::Critical),
            _ => Err(format!("Unknown priority level: {}", s)),
        }
    }
}

/// Action to take when rate limit is exceeded
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ExceededAction {
    /// Return error immediately
    #[default]
    Reject,

    /// Queue and wait (up to max_wait)
    Queue { max_wait: Duration },

    /// Throttle by delaying response
    Throttle { delay: Duration },

    /// Log warning but allow
    Warn,
}

impl std::fmt::Display for ExceededAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExceededAction::Reject => write!(f, "reject"),
            ExceededAction::Queue { max_wait } => write!(f, "queue({}ms)", max_wait.as_millis()),
            ExceededAction::Throttle { delay } => write!(f, "throttle({}ms)", delay.as_millis()),
            ExceededAction::Warn => write!(f, "warn"),
        }
    }
}

impl std::str::FromStr for ExceededAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_lowercase();
        if lower == "reject" {
            Ok(ExceededAction::Reject)
        } else if lower == "warn" {
            Ok(ExceededAction::Warn)
        } else if lower.starts_with("queue") {
            // Parse queue(5s) or queue format
            let ms = parse_duration_from_str(&lower).unwrap_or(5000);
            Ok(ExceededAction::Queue {
                max_wait: Duration::from_millis(ms),
            })
        } else if lower.starts_with("throttle") {
            let ms = parse_duration_from_str(&lower).unwrap_or(100);
            Ok(ExceededAction::Throttle {
                delay: Duration::from_millis(ms),
            })
        } else {
            Err(format!("Unknown exceeded action: {}", s))
        }
    }
}

fn parse_duration_from_str(s: &str) -> Option<u64> {
    // Extract number from strings like "queue(5s)" or "throttle(100ms)"
    let start = s.find('(')?;
    let end = s.find(')')?;
    let duration_str = &s[start + 1..end];

    if let Some(s) = duration_str.strip_suffix("ms") {
        s.parse().ok()
    } else if let Some(s) = duration_str.strip_suffix('s') {
        s.parse::<u64>().ok().map(|s| s * 1000)
    } else {
        duration_str.parse().ok()
    }
}

/// Per-key limit override
#[derive(Debug, Clone)]
pub struct LimitOverride {
    /// Queries per second
    pub qps: Option<u32>,

    /// Burst capacity
    pub burst: Option<u32>,

    /// Maximum concurrent queries
    pub max_concurrent: Option<u32>,

    /// Custom action when exceeded
    pub exceeded_action: Option<ExceededAction>,

    /// Override duration (None = permanent)
    pub duration: Option<Duration>,

    /// When override was created
    pub created_at: std::time::Instant,
}

impl LimitOverride {
    /// Create a new limit override
    pub fn new() -> Self {
        Self {
            qps: None,
            burst: None,
            max_concurrent: None,
            exceeded_action: None,
            duration: None,
            created_at: std::time::Instant::now(),
        }
    }

    /// Set QPS limit
    pub fn with_qps(mut self, qps: u32) -> Self {
        self.qps = Some(qps);
        self
    }

    /// Set burst capacity
    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Set max concurrent
    pub fn with_max_concurrent(mut self, max: u32) -> Self {
        self.max_concurrent = Some(max);
        self
    }

    /// Set exceeded action
    pub fn with_action(mut self, action: ExceededAction) -> Self {
        self.exceeded_action = Some(action);
        self
    }

    /// Set duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = Some(duration);
        self
    }

    /// Check if override has expired
    pub fn is_expired(&self) -> bool {
        if let Some(duration) = self.duration {
            self.created_at.elapsed() > duration
        } else {
            false
        }
    }
}

impl Default for LimitOverride {
    fn default() -> Self {
        Self::new()
    }
}

/// Main rate limit configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Whether rate limiting is enabled
    pub enabled: bool,

    /// Default queries per second
    pub default_qps: u32,

    /// Default burst capacity
    pub default_burst: u32,

    /// Default max concurrent queries
    pub default_concurrency: u32,

    /// Action when limit exceeded
    pub exceeded_action: ExceededAction,

    /// Whether to include Retry-After header
    pub retry_after: bool,

    /// Per-key overrides
    pub overrides: HashMap<LimiterKey, LimitOverride>,

    /// Enable per-user limits
    pub user_limits_enabled: bool,

    /// Enable per-database limits
    pub database_limits_enabled: bool,

    /// Enable per-client-IP limits
    pub client_ip_limits_enabled: bool,

    /// Enable per-query-pattern limits
    pub pattern_limits_enabled: bool,

    /// Queue configuration
    pub queue_max_wait: Duration,
    pub queue_size: u32,

    /// Replication throttle threshold (lag duration)
    pub replication_throttle_threshold: Option<Duration>,

    /// Cleanup interval for expired entries
    pub cleanup_interval: Duration,

    /// Priority multipliers (higher priority = higher effective limit)
    pub priority_multipliers: HashMap<PriorityLevel, f32>,

    /// Cost estimation enabled
    pub cost_estimation_enabled: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        let mut priority_multipliers = HashMap::new();
        priority_multipliers.insert(PriorityLevel::Low, 0.5);
        priority_multipliers.insert(PriorityLevel::Normal, 1.0);
        priority_multipliers.insert(PriorityLevel::High, 2.0);
        priority_multipliers.insert(PriorityLevel::Critical, 10.0);

        Self {
            enabled: true,
            default_qps: 1000,
            default_burst: 2000,
            default_concurrency: 100,
            exceeded_action: ExceededAction::Reject,
            retry_after: true,
            overrides: HashMap::new(),
            user_limits_enabled: true,
            database_limits_enabled: true,
            client_ip_limits_enabled: true,
            pattern_limits_enabled: false,
            queue_max_wait: Duration::from_secs(5),
            queue_size: 1000,
            replication_throttle_threshold: Some(Duration::from_secs(5)),
            cleanup_interval: Duration::from_secs(60),
            priority_multipliers,
            cost_estimation_enabled: true,
        }
    }
}

impl RateLimitConfig {
    /// Create a new configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a builder for configuration
    pub fn builder() -> RateLimitConfigBuilder {
        RateLimitConfigBuilder::new()
    }

    /// Get effective QPS for a key, considering overrides
    pub fn effective_qps(&self, key: &LimiterKey, priority: PriorityLevel) -> u32 {
        let base_qps = self
            .overrides
            .get(key)
            .and_then(|o| o.qps)
            .unwrap_or(self.default_qps);

        let multiplier = self
            .priority_multipliers
            .get(&priority)
            .copied()
            .unwrap_or(1.0);

        (base_qps as f32 * multiplier) as u32
    }

    /// Get effective burst for a key
    pub fn effective_burst(&self, key: &LimiterKey, priority: PriorityLevel) -> u32 {
        let base_burst = self
            .overrides
            .get(key)
            .and_then(|o| o.burst)
            .unwrap_or(self.default_burst);

        let multiplier = self
            .priority_multipliers
            .get(&priority)
            .copied()
            .unwrap_or(1.0);

        (base_burst as f32 * multiplier) as u32
    }

    /// Get effective max concurrent for a key
    pub fn effective_concurrency(&self, key: &LimiterKey, priority: PriorityLevel) -> u32 {
        let base = self
            .overrides
            .get(key)
            .and_then(|o| o.max_concurrent)
            .unwrap_or(self.default_concurrency);

        let multiplier = self
            .priority_multipliers
            .get(&priority)
            .copied()
            .unwrap_or(1.0);

        (base as f32 * multiplier) as u32
    }

    /// Get action for a key
    pub fn action_for_key(&self, key: &LimiterKey) -> ExceededAction {
        self.overrides
            .get(key)
            .and_then(|o| o.exceeded_action.clone())
            .unwrap_or_else(|| self.exceeded_action.clone())
    }

    /// Add an override for a key
    pub fn add_override(&mut self, key: LimiterKey, override_: LimitOverride) {
        self.overrides.insert(key, override_);
    }

    /// Remove an override
    pub fn remove_override(&mut self, key: &LimiterKey) -> Option<LimitOverride> {
        self.overrides.remove(key)
    }

    /// Clean up expired overrides
    pub fn cleanup_expired(&mut self) {
        self.overrides.retain(|_, v| !v.is_expired());
    }
}

/// Builder for RateLimitConfig
pub struct RateLimitConfigBuilder {
    config: RateLimitConfig,
}

impl RateLimitConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: RateLimitConfig::default(),
        }
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    pub fn default_qps(mut self, qps: u32) -> Self {
        self.config.default_qps = qps;
        self
    }

    pub fn default_burst(mut self, burst: u32) -> Self {
        self.config.default_burst = burst;
        self
    }

    pub fn default_concurrency(mut self, concurrency: u32) -> Self {
        self.config.default_concurrency = concurrency;
        self
    }

    pub fn exceeded_action(mut self, action: ExceededAction) -> Self {
        self.config.exceeded_action = action;
        self
    }

    pub fn retry_after(mut self, enabled: bool) -> Self {
        self.config.retry_after = enabled;
        self
    }

    pub fn user_limits(mut self, enabled: bool) -> Self {
        self.config.user_limits_enabled = enabled;
        self
    }

    pub fn database_limits(mut self, enabled: bool) -> Self {
        self.config.database_limits_enabled = enabled;
        self
    }

    pub fn client_ip_limits(mut self, enabled: bool) -> Self {
        self.config.client_ip_limits_enabled = enabled;
        self
    }

    pub fn pattern_limits(mut self, enabled: bool) -> Self {
        self.config.pattern_limits_enabled = enabled;
        self
    }

    pub fn queue_max_wait(mut self, duration: Duration) -> Self {
        self.config.queue_max_wait = duration;
        self
    }

    pub fn queue_size(mut self, size: u32) -> Self {
        self.config.queue_size = size;
        self
    }

    pub fn replication_throttle_threshold(mut self, threshold: Option<Duration>) -> Self {
        self.config.replication_throttle_threshold = threshold;
        self
    }

    pub fn cost_estimation(mut self, enabled: bool) -> Self {
        self.config.cost_estimation_enabled = enabled;
        self
    }

    pub fn add_override(mut self, key: LimiterKey, override_: LimitOverride) -> Self {
        self.config.overrides.insert(key, override_);
        self
    }

    pub fn build(self) -> RateLimitConfig {
        self.config
    }
}

impl Default for RateLimitConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_level_parsing() {
        assert_eq!("low".parse::<PriorityLevel>().unwrap(), PriorityLevel::Low);
        assert_eq!(
            "normal".parse::<PriorityLevel>().unwrap(),
            PriorityLevel::Normal
        );
        assert_eq!(
            "high".parse::<PriorityLevel>().unwrap(),
            PriorityLevel::High
        );
        assert_eq!(
            "critical".parse::<PriorityLevel>().unwrap(),
            PriorityLevel::Critical
        );
        assert!("invalid".parse::<PriorityLevel>().is_err());
    }

    #[test]
    fn test_exceeded_action_parsing() {
        assert_eq!(
            "reject".parse::<ExceededAction>().unwrap(),
            ExceededAction::Reject
        );
        assert_eq!(
            "warn".parse::<ExceededAction>().unwrap(),
            ExceededAction::Warn
        );

        match "queue(5s)".parse::<ExceededAction>().unwrap() {
            ExceededAction::Queue { max_wait } => {
                assert_eq!(max_wait, Duration::from_secs(5));
            }
            _ => panic!("Expected Queue action"),
        }

        match "throttle(100ms)".parse::<ExceededAction>().unwrap() {
            ExceededAction::Throttle { delay } => {
                assert_eq!(delay, Duration::from_millis(100));
            }
            _ => panic!("Expected Throttle action"),
        }
    }

    #[test]
    fn test_limit_override_expiration() {
        let override_ = LimitOverride::new()
            .with_qps(100)
            .with_duration(Duration::from_millis(10));

        assert!(!override_.is_expired());

        std::thread::sleep(Duration::from_millis(20));
        assert!(override_.is_expired());
    }

    #[test]
    fn test_effective_qps_with_priority() {
        let config = RateLimitConfig::builder().default_qps(100).build();

        let key = LimiterKey::User("test".to_string());

        // Low priority gets 50% (0.5 multiplier)
        assert_eq!(config.effective_qps(&key, PriorityLevel::Low), 50);

        // Normal gets 100%
        assert_eq!(config.effective_qps(&key, PriorityLevel::Normal), 100);

        // High gets 200%
        assert_eq!(config.effective_qps(&key, PriorityLevel::High), 200);

        // Critical gets 1000%
        assert_eq!(config.effective_qps(&key, PriorityLevel::Critical), 1000);
    }

    #[test]
    fn test_config_builder() {
        let config = RateLimitConfig::builder()
            .enabled(true)
            .default_qps(500)
            .default_burst(1000)
            .default_concurrency(50)
            .exceeded_action(ExceededAction::Warn)
            .user_limits(false)
            .build();

        assert!(config.enabled);
        assert_eq!(config.default_qps, 500);
        assert_eq!(config.default_burst, 1000);
        assert_eq!(config.default_concurrency, 50);
        assert_eq!(config.exceeded_action, ExceededAction::Warn);
        assert!(!config.user_limits_enabled);
    }

    #[test]
    fn test_override_cleanup() {
        let mut config = RateLimitConfig::default();

        let short_lived = LimitOverride::new()
            .with_qps(100)
            .with_duration(Duration::from_millis(10));

        let permanent = LimitOverride::new().with_qps(200);

        config.add_override(LimiterKey::User("short".to_string()), short_lived);
        config.add_override(LimiterKey::User("perm".to_string()), permanent);

        assert_eq!(config.overrides.len(), 2);

        std::thread::sleep(Duration::from_millis(20));
        config.cleanup_expired();

        assert_eq!(config.overrides.len(), 1);
        assert!(config
            .overrides
            .contains_key(&LimiterKey::User("perm".to_string())));
    }
}
