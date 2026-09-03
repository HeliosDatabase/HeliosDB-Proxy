//! Rate Limiter
//!
//! Central rate limiting coordinator that combines token buckets,
//! sliding windows, and concurrency limiters.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::DashMap;

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

/// A `LimiterKey` paired with its pre-rendered `Display` string.
///
/// The rate-limit gate runs on every query, and both halves of the old hot
/// path allocated: the key itself was rebuilt from the session variables per
/// query, and the metrics collector formatted `key.to_string()` per query to
/// index its per-key stats map. A session's keying dimension (global / client
/// IP / database / user) is fixed for its whole life, so the pair is resolved
/// once and reused; the string half is an `Arc<str>` so the metrics map can
/// take an owned copy on first sight of a key without re-formatting.
///
/// Equality and hashing follow the key alone — the rendered string is derived
/// state, never an independent identity.
#[derive(Debug, Clone)]
pub struct CachedLimiterKey {
    key: LimiterKey,
    display: Arc<str>,
}

impl CachedLimiterKey {
    /// Resolve a key and render its display string once.
    pub fn new(key: LimiterKey) -> Self {
        let display: Arc<str> = Arc::from(key.to_string().as_str());
        Self { key, display }
    }

    /// The underlying bucket key.
    pub fn key(&self) -> &LimiterKey {
        &self.key
    }

    /// The pre-rendered `Display` form, shareable without re-formatting.
    pub fn display(&self) -> &Arc<str> {
        &self.display
    }
}

impl From<LimiterKey> for CachedLimiterKey {
    fn from(key: LimiterKey) -> Self {
        Self::new(key)
    }
}

impl std::fmt::Display for CachedLimiterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display)
    }
}

impl PartialEq for CachedLimiterKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for CachedLimiterKey {}

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
    /// Configuration. Held in an `ArcSwap` rather than a `RwLock` so the
    /// per-query check path is a lock-free load instead of a global reader
    /// acquisition shared by every connection; `update_config`/`cleanup`
    /// publish a whole new snapshot (RCU).
    config: ArcSwap<RateLimitConfig>,

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
            config: ArcSwap::from_pointee(config),
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
            config: ArcSwap::from_pointee(config),
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
        let config = self.config.load();

        if !config.enabled {
            return RateLimitResult::Allowed;
        }

        // Callers that hold a `CachedLimiterKey` (the proxy's per-query gate)
        // reuse its rendered form instead of re-formatting here.
        let display: Arc<str> = Arc::from(key.to_string().as_str());
        self.check_resolved(key, &display, cost, priority, &config)
    }

    /// Check rate limit for a pre-resolved key (no key rebuild, no
    /// `Display` formatting). This is the per-query path.
    pub fn check_cached(&self, key: &CachedLimiterKey, cost: u32) -> RateLimitResult {
        self.check_cached_with_priority(key, cost, PriorityLevel::Normal)
    }

    /// Check rate limit for a pre-resolved key, with priority.
    pub fn check_cached_with_priority(
        &self,
        key: &CachedLimiterKey,
        cost: u32,
        priority: PriorityLevel,
    ) -> RateLimitResult {
        let config = self.config.load();

        if !config.enabled {
            return RateLimitResult::Allowed;
        }

        self.check_resolved(&key.key, &key.display, cost, priority, &config)
    }

    /// Shared body of the enabled check path. `display` is the key's rendered
    /// form, threaded through so the metrics collector never formats it.
    fn check_resolved(
        &self,
        key: &LimiterKey,
        display: &Arc<str>,
        cost: u32,
        priority: PriorityLevel,
        config: &RateLimitConfig,
    ) -> RateLimitResult {
        let start = Instant::now();

        // Check token bucket (QPS)
        if let Err(exceeded) = self.check_token_bucket(key, cost, priority, config) {
            let result = self.handle_exceeded(key, exceeded, config);
            self.metrics
                .record_decision_keyed(display, &result, start.elapsed());
            return result;
        }

        // Check sliding window (per-minute)
        if let Err(exceeded) = self.check_sliding_window(key, cost, config) {
            let result = self.handle_exceeded_window(key, exceeded, config);
            self.metrics
                .record_decision_keyed(display, &result, start.elapsed());
            return result;
        }

        self.metrics
            .record_decision_keyed(display, &RateLimitResult::Allowed, start.elapsed());
        RateLimitResult::Allowed
    }

    /// Check and acquire concurrency slot
    pub fn check_concurrency(
        &self,
        key: &LimiterKey,
    ) -> Result<Arc<ConcurrencyLimiter>, RateLimitExceeded> {
        let config = self.config.load();

        if !config.enabled {
            // Return a dummy limiter that allows everything
            return Ok(Arc::new(ConcurrencyLimiter::new(u32::MAX)));
        }

        let max = config.effective_concurrency(key, PriorityLevel::Normal);

        // Hit path first: an existing limiter is looked up by reference, so the
        // key is only cloned when a new bucket actually has to be inserted.
        let limiter = match self.concurrency.get(key) {
            Some(existing) => Arc::clone(existing.value()),
            None => self
                .concurrency
                .entry(key.clone())
                .or_insert_with(|| Arc::new(ConcurrencyLimiter::new(max)))
                .clone(),
        };

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
        let config = self.config.load();

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
        self.config.store(Arc::new(config));
    }

    /// Get current configuration (cloned)
    pub fn config(&self) -> RateLimitConfig {
        RateLimitConfig::clone(&self.config.load())
    }

    // Internal methods

    fn check_token_bucket(
        &self,
        key: &LimiterKey,
        cost: u32,
        priority: PriorityLevel,
        config: &RateLimitConfig,
    ) -> Result<(), TokenBucketExceeded> {
        // Hit path: look the bucket up by reference so the steady state never
        // clones the key (nor resolves the effective limits, which only matter
        // when a bucket is created).
        if let Some(bucket) = self.token_buckets.get(key) {
            return bucket.try_acquire(cost);
        }

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
        // Hit path: existing windows are looked up by reference (no key clone).
        if let Some(window) = self.sliding_windows.get(key) {
            return window.try_record_n(cost);
        }

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
        // RCU against the `ArcSwap` snapshot: mutate a private copy, publish it.
        let mut config = RateLimitConfig::clone(&self.config.load());
        config.cleanup_expired();
        self.config.store(Arc::new(config));
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("enabled", &self.config.load().enabled)
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

    /// A `CachedLimiterKey` must address exactly the same bucket as the plain
    /// key it was built from — `check_cached` and `check` share state and
    /// produce identical verdicts.
    #[test]
    fn test_cached_key_shares_bucket_with_plain_key() {
        let config = RateLimitConfig::builder()
            .default_qps(1)
            .default_burst(2)
            .exceeded_action(ExceededAction::Reject)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("shared".to_string());
        let cached = CachedLimiterKey::new(key.clone());

        // Two tokens of burst, consumed one through each entry point.
        assert!(limiter.check(&key, 1).is_allowed());
        assert!(limiter.check_cached(&cached, 1).is_allowed());

        // Third check must be denied through either entry point — proving the
        // cached key did not open a second, independent bucket.
        assert!(!limiter.check_cached(&cached, 1).is_allowed());
        assert!(!limiter.check(&key, 1).is_allowed());

        assert_eq!(limiter.token_buckets.len(), 1);
        assert_eq!(limiter.sliding_windows.len(), 1);
    }

    /// The cached key's rendered form is exactly `LimiterKey`'s `Display`, so
    /// the metrics map is keyed identically no matter which path recorded it.
    #[test]
    fn test_cached_key_display_matches_limiter_key() {
        for key in [
            LimiterKey::Global,
            LimiterKey::User("alice".to_string()),
            LimiterKey::Database("mydb".to_string()),
            LimiterKey::ClientIp("10.0.0.7".parse().unwrap()),
            LimiterKey::composite(vec![
                LimiterKey::User("u".to_string()),
                LimiterKey::Database("d".to_string()),
            ]),
        ] {
            let cached = CachedLimiterKey::new(key.clone());
            assert_eq!(cached.to_string(), key.to_string());
            assert_eq!(&**cached.display(), key.to_string().as_str());
            assert_eq!(cached.key(), &key);
        }
    }

    /// Metric bookkeeping must be identical between the cached and plain
    /// paths: same per-key bucket, same totals.
    #[test]
    fn test_cached_and_plain_paths_record_same_metric_key() {
        let config = RateLimitConfig::builder()
            .default_qps(100)
            .default_burst(200)
            .build();
        let limiter = RateLimiter::new(config);

        let key = LimiterKey::User("metered".to_string());
        let cached = CachedLimiterKey::new(key.clone());

        limiter.check(&key, 1);
        limiter.check_cached(&cached, 1);

        let stats = limiter.metrics().get_stats();
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.key_stats.len(), 1, "both paths share one metric key");
        assert_eq!(stats.key_stats.get("user:metered").unwrap().total, 2);
    }

    /// `check_cached` honors a live `update_config` (the ArcSwap reload path):
    /// disabling the limiter must short-circuit to Allowed immediately.
    #[test]
    fn test_update_config_visible_to_cached_path() {
        let config = RateLimitConfig::builder()
            .enabled(true)
            .default_qps(1)
            .default_burst(1)
            .exceeded_action(ExceededAction::Reject)
            .build();
        let limiter = RateLimiter::new(config);
        let cached = CachedLimiterKey::new(LimiterKey::User("reload".to_string()));

        assert!(limiter.check_cached(&cached, 1).is_allowed());
        assert!(!limiter.check_cached(&cached, 1).is_allowed());

        limiter.update_config(
            RateLimitConfig::builder()
                .enabled(false)
                .default_qps(1)
                .build(),
        );

        assert!(!limiter.config().enabled);
        for _ in 0..10 {
            assert!(limiter.check_cached(&cached, 1).is_allowed());
        }
    }

    /// `cleanup` is an RCU over the config snapshot: expired overrides must be
    /// gone from the published config afterwards, live ones retained.
    #[test]
    fn test_cleanup_publishes_pruned_config() {
        use super::super::config::LimitOverride;

        let mut config = RateLimitConfig::builder().default_qps(100).build();
        config.add_override(
            LimiterKey::User("expiring".to_string()),
            LimitOverride::new()
                .with_qps(5)
                .with_duration(Duration::from_millis(10)),
        );
        config.add_override(
            LimiterKey::User("permanent".to_string()),
            LimitOverride::new().with_qps(7),
        );

        let limiter = RateLimiter::new(config);
        assert_eq!(limiter.config().overrides.len(), 2);

        std::thread::sleep(Duration::from_millis(20));
        limiter.cleanup();

        let after = limiter.config();
        assert_eq!(after.overrides.len(), 1);
        assert!(after
            .overrides
            .contains_key(&LimiterKey::User("permanent".to_string())));
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
