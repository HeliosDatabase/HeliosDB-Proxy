//! Rate Limiting & Query Throttling Module
//!
//! Multi-dimensional rate limiting at the proxy layer to prevent:
//! - Runaway queries exhausting connection pools
//! - Query storms from buggy applications
//! - DoS attacks (intentional or accidental)
//! - Heavy users starving other tenants
//!
//! # Architecture
//!
//! ```text
//!                     +---------------------------------------------+
//!                     |              RATE LIMITER                   |
//!                     |                                             |
//!   Query ----------->|  1. Identify Limiter Keys                   |
//!                     |     - User/Role                             |
//!                     |     - Client IP                             |
//!                     |     - Database                              |
//!                     |     - Query pattern                         |
//!                     |                                             |
//!                     |  2. Check Rate Limits                       |
//!                     |     - Token Bucket (queries/sec)            |
//!                     |     - Sliding Window (queries/minute)       |
//!                     |     - Concurrency (active queries)          |
//!                     |                                             |
//!                     |  3. Decision: ALLOW / THROTTLE / DENY       |
//!                     +---------------------------------------------+
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::rate_limit::{RateLimiter, RateLimitConfig, LimiterKey};
//!
//! let config = RateLimitConfig::default();
//! let limiter = RateLimiter::new(config);
//!
//! // Check rate limit for a user
//! let key = LimiterKey::User("app_user".to_string());
//! match limiter.check(&key, 1).await {
//!     Ok(()) => println!("Query allowed"),
//!     Err(e) => println!("Rate limited: {:?}", e),
//! }
//! ```

pub mod config;
pub mod token_bucket;
pub mod sliding_window;
pub mod concurrency;
pub mod limiter;
pub mod cost_estimator;
pub mod metrics;
pub mod agent;

// Re-exports for convenience
pub use config::{
    RateLimitConfig, LimitOverride, ExceededAction, PriorityLevel,
    RateLimitConfigBuilder,
};
pub use token_bucket::TokenBucket;
pub use sliding_window::SlidingWindow;
pub use concurrency::{ConcurrencyLimiter, ConcurrencyGuard};
pub use limiter::{RateLimiter, LimiterKey, RateLimitResult, RateLimitExceeded};
pub use cost_estimator::{QueryCostEstimator, OperationType};
pub use metrics::{RateLimitMetrics, RateLimitStats, KeyStats};
pub use agent::{AgentTokenBudget, WorkflowQuota, WorkflowToken, BudgetExceeded, QuotaExceeded};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_limiter_key_equality() {
        let key1 = LimiterKey::User("test".to_string());
        let key2 = LimiterKey::User("test".to_string());
        let key3 = LimiterKey::User("other".to_string());

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_limiter_key_hash() {
        use std::collections::HashMap;
        let mut map: HashMap<LimiterKey, u32> = HashMap::new();

        map.insert(LimiterKey::User("user1".to_string()), 100);
        map.insert(LimiterKey::Database("db1".to_string()), 200);

        assert_eq!(map.get(&LimiterKey::User("user1".to_string())), Some(&100));
        assert_eq!(map.get(&LimiterKey::Database("db1".to_string())), Some(&200));
    }

    #[test]
    fn test_default_config() {
        let config = RateLimitConfig::default();
        assert!(config.enabled);
        assert!(config.default_qps > 0);
        assert!(config.default_burst > 0);
    }

    #[test]
    fn test_exceeded_action_display() {
        assert_eq!(format!("{}", ExceededAction::Reject), "reject");
        assert_eq!(format!("{}", ExceededAction::Warn), "warn");
    }

    #[test]
    fn test_priority_level_ordering() {
        assert!(PriorityLevel::Critical > PriorityLevel::High);
        assert!(PriorityLevel::High > PriorityLevel::Normal);
        assert!(PriorityLevel::Normal > PriorityLevel::Low);
    }
}
