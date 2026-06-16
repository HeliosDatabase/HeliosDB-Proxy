//! Rate Limiter
//!
//! Central rate limiting coordinator that combines token buckets,
//! sliding windows, and concurrency limiters.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::concurrency::ConcurrencyLimiter;
use super::config::{ExceededAction, PriorityLevel, RateLimitConfig};
use super::cost_estimator::QueryCostEstimator;
use super::metrics::RateLimitMetrics;
use super::sliding_window::{SlidingWindow, SlidingWindowExceeded};
use super::token_bucket::{TokenBucket, TokenBucketExceeded};

/// Key for identifying rate limit buckets
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum LimiterKey {
    /// Global limiter
    Global,

    /// Per-user limits
    User(String),

    /// Per-client IP limits
    ClientIp(IpAddr),

    /// Per-database limits
    Database(String),

    /// Per-tenant limits (multi-tenancy)
    Tenant(String),

    /// Per-query-pattern limits
    QueryPattern(String),

    /// Per-role limits
    Role(String),

    /// Composite key (multiple dimensions)
    Composite(Vec<LimiterKey>),
}

impl LimiterKey {
    /// Create a user key
    pub fn user(name: impl Into<String>) -> Self {
        Self::User(name.into())
    }

    /// Create a database key
    pub fn database(name: impl Into<String>) -> Self {
        Self::Database(name.into())
    }

    /// Create a tenant key
    pub fn tenant(id: impl Into<String>) -> Self {
        Self::Tenant(id.into())
    }

    /// Create a pattern key
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self::QueryPattern(pattern.into())
    }

    /// Create a composite key
    pub fn composite(keys: Vec<LimiterKey>) -> Self {
        Self::Composite(keys)
    }
}

impl std::fmt::Display for LimiterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LimiterKey::Global => write!(f, "global"),
            LimiterKey::User(u) => write!(f, "user:{}", u),
            LimiterKey::ClientIp(ip) => write!(f, "ip:{}", ip),
            LimiterKey::Database(d) => write!(f, "db:{}", d),
            LimiterKey::Tenant(t) => write!(f, "tenant:{}", t),
            LimiterKey::QueryPattern(p) => write!(f, "pattern:{}", p),
            LimiterKey::Role(r) => write!(f, "role:{}", r),
            LimiterKey::Composite(keys) => {
                let parts: Vec<_> = keys.iter().map(|k| k.to_string()).collect();
                write!(f, "composite:[{}]", parts.join(","))
            }
        }
    }
}

/// Result of rate limit check
#[derive(Debug, Clone)]
pub enum RateLimitResult {
    /// Request allowed
    Allowed,

    /// Request should be queued (returns wait time)
    Queued(Duration),

    /// Request should be throttled (returns delay)
    Throttled(Duration),

    /// Request allowed but logged a warning
    Warned(String),

    /// Request denied
    Denied(RateLimitExceeded),
}

impl RateLimitResult {
    /// Check if request is allowed (including queued, throttled, warned)
    pub fn is_allowed(&self) -> bool {
        !matches!(self, RateLimitResult::Denied(_))
    }

    /// Get wait/delay duration if applicable
    pub fn wait_duration(&self) -> Option<Duration> {
        match self {
            RateLimitResult::Queued(d) | RateLimitResult::Throttled(d) => Some(*d),
            _ => None,
        }
    }
}

/// Rate limit exceeded error
#[derive(Debug, Clone)]
pub struct RateLimitExceeded {
    /// Which key was exceeded
    pub key: LimiterKey,

    /// Type of limit exceeded
    pub limit_type: LimitType,

    /// Current rate/count
    pub current: u64,

    /// Limit value
    pub limit: u64,

    /// When to retry
    pub retry_after: Duration,

    /// Human-readable message
    pub message: String,
}

impl std::fmt::Display for RateLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} exceeded for {} ({}/{}), retry after {}ms",
            self.message,
            self.limit_type,
            self.key,
            self.current,
            self.limit,
            self.retry_after.as_millis()
        )
    }
}

impl std::error::Error for RateLimitExceeded {}

/// Type of rate limit
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitType {
    /// Token bucket (QPS)
    TokenBucket,
    /// Sliding window (per-minute, per-hour)
    SlidingWindow,
    /// Concurrency
    Concurrency,
}

impl std::fmt::Display for LimitType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LimitType::TokenBucket => write!(f, "qps"),
            LimitType::SlidingWindow => write!(f, "window"),
            LimitType::Concurrency => write!(f, "concurrency"),
        }
    }
}

/// Main rate limiter
pub struct RateLimiter {
    /// Configuration
    config: RwLock<RateLimitConfig>,

    /// Token bucket limiters (burst + sustained rate)
    token_buckets: DashMap<LimiterKey, TokenBucket>,

    /// Sliding window limiters (rolling counts)
    sliding_windows: DashMap<LimiterKey, SlidingWindow>,

    /// Concurrency limiters (active query count)
    concurrency: DashMap<LimiterKey, Arc<ConcurrencyLimiter>>,

    /// Query cost estimator
    cost_estimator: QueryCostEstimator,

    /// Metrics collector
    metrics: Arc<RateLimitMetrics>,

    /// Creation time
    created_at: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config: RwLock::new(config),
            token_buckets: DashMap::new(),
            sliding_windows: DashMap::new(),
            concurrency: DashMap::new(),
            cost_estimator: QueryCostEstimator::new(),
            metrics: Arc::new(RateLimitMetrics::new()),
            created_at: Instant::now(),
        }
    }

    /// Create with custom cost estimator
    pub fn with_cost_estimator(config: RateLimitConfig, estimator: QueryCostEstimator) -> Self {
        Self {
            config: RwLock::new(config),
            token_buckets: DashMap::new(),
            sliding_windows: DashMap::new(),
            concurrency: DashMap::new(),
            cost_estimator: estimator,
            metrics: Arc::new(RateLimitMetrics::new()),
            created_at: Instant::now(),
        }
    }

    /// Check rate limit for a key
    pub fn check(&self, key: &LimiterKey, cost: u32) -> RateLimitResult {
        self.check_with_priority(key, cost, PriorityLevel::Normal)
    }

    /// Check rate limit with priority
    pub fn check_with_priority(
        &self,
        key: &LimiterKey,
        cost: u32,
        priority: PriorityLevel,
    ) -> RateLimitResult {
        let config = self.config.read();

        if !config.enabled {
            return RateLimitResult::Allowed;
        }

        let start = Instant::now();

        // Check token bucket (QPS)
        if let Err(exceeded) = self.check_token_bucket(key, cost, priority, &config) {
            let result = self.handle_exceeded(key, exceeded, &config);
            self.metrics.record_decision(key, &result, start.elapsed());
            return result;
        }

        // Check sliding window (per-minute)
        if let Err(exceeded) = self.check_sliding_window(key, cost, &config) {
            let result = self.handle_exceeded_window(key, exceeded, &config);
            self.metrics.record_decision(key, &result, start.elapsed());
            return result;
        }

        self.metrics
            .record_decision(key, &RateLimitResult::Allowed, start.elapsed());
        RateLimitResult::Allowed
    }

    /// Check and acquire concurrency slot
    pub fn check_concurrency(
        &self,
        key: &LimiterKey,
    ) -> Result<Arc<ConcurrencyLimiter>, RateLimitExceeded> {
        let config = self.config.read();

        if !config.enabled {
            // Return a dummy limiter that allows everything
            return Ok(Arc::new(ConcurrencyLimiter::new(u32::MAX)));
        }

        let max = config.effective_concurrency(key, PriorityLevel::Normal);

        let limiter = self
            .concurrency
            .entry(key.clone())
            .or_insert_with(|| Arc::new(ConcurrencyLimiter::new(max)))
            .clone();

        // Check if would exceed
        if limiter.at_capacity() {
            return Err(RateLimitExceeded {
                key: key.clone(),
                limit_type: LimitType::Concurrency,
                current: limiter.active_count() as u64,
                limit: max as u64,
                retry_after: Duration::from_millis(100), // Estimate
                message: "Concurrency limit exceeded".to_string(),
            });
        }

        Ok(limiter)
    }

    /// Check for a query with automatic cost estimation
    pub fn check_query(&self, key: &LimiterKey, query: &str) -> RateLimitResult {
        self.check_query_with_priority(key, query, PriorityLevel::Normal)
    }

    /// Check query with priority
    pub fn check_query_with_priority(
        &self,
        key: &LimiterKey,
        query: &str,
        priority: PriorityLevel,
    ) -> RateLimitResult {
        let config = self.config.read();

        let cost = if config.cost_estimation_enabled {
            self.cost_estimator.estimate_cost_with_hint(query)
        } else {
            1
        };

        drop(config);
        self.check_with_priority(key, cost, priority)
    }

    /// Check multiple keys (returns first failure)
    pub fn check_all(&self, keys: &[LimiterKey], cost: u32) -> RateLimitResult {
        for key in keys {
            let result = self.check(key, cost);
            if !result.is_allowed() {
                return result;
            }
        }
        RateLimitResult::Allowed
    }

    /// Reset limits for a key
    pub fn reset(&self, key: &LimiterKey) {
        if let Some(bucket) = self.token_buckets.get(key) {
            bucket.reset();
        }
        if let Some(window) = self.sliding_windows.get(key) {
            window.reset();
        }
        if let Some(limiter) = self.concurrency.get(key) {
            limiter.reset_stats();
        }
        self.metrics.reset_key(key);
    }

    /// Get current stats for a key
    pub fn get_key_stats(&self, key: &LimiterKey) -> HashMap<String, u64> {
        let mut stats = HashMap::new();

        if let Some(bucket) = self.token_buckets.get(key) {
            stats.insert(
                "tokens_available".to_string(),
                bucket.current_tokens() as u64,
            );
            stats.insert("bucket_capacity".to_string(), bucket.capacity() as u64);
        }

        if let Some(window) = self.sliding_windows.get(key) {
            stats.insert("window_count".to_string(), window.current_count() as u64);
            stats.insert("window_max".to_string(), window.max_events() as u64);
        }

        if let Some(limiter) = self.concurrency.get(key) {
            stats.insert(
                "active_concurrent".to_string(),
                limiter.active_count() as u64,
            );
            stats.insert(
                "max_concurrent".to_string(),
                limiter.max_concurrent() as u64,
            );
            stats.insert("queued".to_string(), limiter.queue_length() as u64);
        }

        stats
    }

    /// Get metrics
    pub fn metrics(&self) -> Arc<RateLimitMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Update configuration
    pub fn update_config(&self, config: RateLimitConfig) {
        *self.config.write() = config;
    }

    /// Get current configuration (cloned)
    pub fn config(&self) -> RateLimitConfig {
        self.config.read().clone()
    }

    // Internal methods

    fn check_token_bucket(
        &self,
        key: &LimiterKey,
        cost: u32,
        priority: PriorityLevel,
        config: &RateLimitConfig,
    ) -> Result<(), TokenBucketExceeded> {
        let qps = config.effective_qps(key, priority);
        let burst = config.effective_burst(key, priority);

        let bucket = self
            .token_buckets
            .entry(key.clone())
            .or_insert_with(|| TokenBucket::from_qps(qps, burst));

        bucket.try_acquire(cost)
    }

    fn check_sliding_window(
        &self,
        key: &LimiterKey,
        cost: u32,
        _config: &RateLimitConfig,
    ) -> Result<(), SlidingWindowExceeded> {
        // Use a per-minute sliding window
        let window = self
            .sliding_windows
            .entry(key.clone())
            .or_insert_with(|| SlidingWindow::per_minute(60_000)); // 60k per minute default

        window.try_record_n(cost)
    }

    fn handle_exceeded(
        &self,
        key: &LimiterKey,
        exceeded: TokenBucketExceeded,
        config: &RateLimitConfig,
    ) -> RateLimitResult {
        let error = RateLimitExceeded {
            key: key.clone(),
            limit_type: LimitType::TokenBucket,
            current: exceeded.current_tokens as u64,
            limit: exceeded.requested_tokens as u64,
            retry_after: exceeded.retry_after,
            message: "QPS rate limit exceeded".to_string(),
        };

        self.apply_action(&config.action_for_key(key), error)
    }

    fn handle_exceeded_window(
        &self,
        key: &LimiterKey,
        exceeded: SlidingWindowExceeded,
        config: &RateLimitConfig,
    ) -> RateLimitResult {
        let error = RateLimitExceeded {
            key: key.clone(),
            limit_type: LimitType::SlidingWindow,
            current: exceeded.current_count as u64,
            limit: exceeded.max_count as u64,
            retry_after: exceeded.retry_after,
            message: "Window rate limit exceeded".to_string(),
        };

        self.apply_action(&config.action_for_key(key), error)
    }

    fn apply_action(&self, action: &ExceededAction, error: RateLimitExceeded) -> RateLimitResult {
        match action {
            ExceededAction::Reject => RateLimitResult::Denied(error),
            ExceededAction::Queue { max_wait } => {
                let wait = error.retry_after.min(*max_wait);
                RateLimitResult::Queued(wait)
            }
            ExceededAction::Throttle { delay } => RateLimitResult::Throttled(*delay),
            ExceededAction::Warn => {
                RateLimitResult::Warned(format!("Rate limit warning: {}", error))
            }
        }
    }

    /// Clean up expired entries
    pub fn cleanup(&self) {
        let mut config = self.config.write();
        config.cleanup_expired();
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("enabled", &self.config.read().enabled)
            .field("token_buckets", &self.token_buckets.len())
            .field("sliding_windows", &self.sliding_windows.len())
            .field("concurrency_limiters", &self.concurrency.len())
            .field("uptime", &self.uptime())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limiter_creation() {
        let config = RateLimitConfig::default();
        let limiter = RateLimiter::new(config);

        assert!(limiter.uptime().as_nanos() > 0);
    }

    #[test]
    fn test_check_allowed() {
        let config = RateLimitConfig::builder()
            .default_qps(100)
            .default_burst(200)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());
        let result = limiter.check(&key, 1);

        assert!(result.is_allowed());
    }

    #[test]
    fn test_check_exceeded() {
        let config = RateLimitConfig::builder()
            .default_qps(1)
            .default_burst(1)
            .exceeded_action(ExceededAction::Reject)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        // First request should succeed
        assert!(limiter.check(&key, 1).is_allowed());

        // Second request should fail (burst exhausted)
        let result = limiter.check(&key, 1);
        assert!(!result.is_allowed());
    }

    #[test]
    fn test_check_with_priority() {
        let config = RateLimitConfig::builder()
            .default_qps(10)
            .default_burst(10)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        // High priority gets 2x limit (20 burst)
        for _ in 0..20 {
            assert!(limiter
                .check_with_priority(&key, 1, PriorityLevel::High)
                .is_allowed());
        }
    }

    #[test]
    fn test_check_disabled() {
        let config = RateLimitConfig::builder()
            .enabled(false)
            .default_qps(1)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        // Should always allow when disabled
        for _ in 0..100 {
            assert!(limiter.check(&key, 1).is_allowed());
        }
    }

    #[test]
    fn test_check_query() {
        let config = RateLimitConfig::builder()
            .default_qps(100)
            .default_burst(200)
            .cost_estimation(true)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        // SELECT should have low cost
        let result = limiter.check_query(&key, "SELECT * FROM users WHERE id = 1");
        assert!(result.is_allowed());
    }

    #[test]
    fn test_check_all_keys() {
        let config = RateLimitConfig::builder()
            .default_qps(100)
            .default_burst(200)
            .build();
        let limiter = RateLimiter::new(config);

        let keys = vec![
            LimiterKey::User("test".to_string()),
            LimiterKey::Database("db1".to_string()),
            LimiterKey::Global,
        ];

        let result = limiter.check_all(&keys, 1);
        assert!(result.is_allowed());
    }

    #[test]
    fn test_reset() {
        let config = RateLimitConfig::builder()
            .default_qps(1)
            .default_burst(1)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        // Exhaust limit
        assert!(limiter.check(&key, 1).is_allowed());
        assert!(!limiter.check(&key, 1).is_allowed());

        // Reset
        limiter.reset(&key);

        // Should be allowed again
        assert!(limiter.check(&key, 1).is_allowed());
    }

    #[test]
    fn test_get_key_stats() {
        let config = RateLimitConfig::default();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        // Make a request to create bucket
        let _ = limiter.check(&key, 1);

        let stats = limiter.get_key_stats(&key);
        assert!(stats.contains_key("tokens_available"));
        assert!(stats.contains_key("bucket_capacity"));
    }

    #[test]
    fn test_exceeded_action_queue() {
        let config = RateLimitConfig::builder()
            .default_qps(1)
            .default_burst(1)
            .exceeded_action(ExceededAction::Queue {
                max_wait: Duration::from_secs(5),
            })
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        assert!(limiter.check(&key, 1).is_allowed());

        let result = limiter.check(&key, 1);
        match result {
            RateLimitResult::Queued(wait) => {
                assert!(wait.as_secs() <= 5);
            }
            _ => panic!("Expected Queued result"),
        }
    }

    #[test]
    fn test_exceeded_action_warn() {
        let config = RateLimitConfig::builder()
            .default_qps(1)
            .default_burst(1)
            .exceeded_action(ExceededAction::Warn)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        assert!(limiter.check(&key, 1).is_allowed());

        let result = limiter.check(&key, 1);
        match result {
            RateLimitResult::Warned(msg) => {
                assert!(msg.contains("Rate limit"));
            }
            _ => panic!("Expected Warned result"),
        }
    }

    #[test]
    fn test_limiter_key_display() {
        assert_eq!(LimiterKey::Global.to_string(), "global");
        assert_eq!(
            LimiterKey::User("alice".to_string()).to_string(),
            "user:alice"
        );
        assert_eq!(
            LimiterKey::Database("mydb".to_string()).to_string(),
            "db:mydb"
        );
    }

    #[test]
    fn test_update_config() {
        let config = RateLimitConfig::builder().default_qps(100).build();
        let limiter = RateLimiter::new(config);

        assert_eq!(limiter.config().default_qps, 100);

        let new_config = RateLimitConfig::builder().default_qps(200).build();
        limiter.update_config(new_config);

        assert_eq!(limiter.config().default_qps, 200);
    }

    #[test]
    fn test_concurrency_check() {
        let config = RateLimitConfig::builder().default_concurrency(10).build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("test".to_string());

        let result = limiter.check_concurrency(&key);
        assert!(result.is_ok());

        let conc_limiter = result.unwrap();
        assert_eq!(conc_limiter.max_concurrent(), 10);
    }

    #[test]
    fn test_rate_limit_result_methods() {
        assert!(RateLimitResult::Allowed.is_allowed());
        assert!(RateLimitResult::Queued(Duration::from_secs(1)).is_allowed());
        assert!(RateLimitResult::Throttled(Duration::from_secs(1)).is_allowed());
        assert!(RateLimitResult::Warned("test".to_string()).is_allowed());

        let error = RateLimitExceeded {
            key: LimiterKey::Global,
            limit_type: LimitType::TokenBucket,
            current: 0,
            limit: 100,
            retry_after: Duration::from_secs(1),
            message: "test".to_string(),
        };
        assert!(!RateLimitResult::Denied(error).is_allowed());

        assert_eq!(
            RateLimitResult::Queued(Duration::from_secs(5)).wait_duration(),
            Some(Duration::from_secs(5))
        );
        assert_eq!(RateLimitResult::Allowed.wait_duration(), None);
    }
}
