//! Pool Mode Configuration
//!
//! Configuration structures for connection pooling modes.

use super::mode::{PoolingMode, PreparedStatementMode};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Connection pool mode configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolModeConfig {
    /// Default pooling mode for new connections
    #[serde(default)]
    pub default_mode: PoolingMode,

    /// Maximum connections in the pool per node
    #[serde(default = "default_max_pool_size")]
    pub max_pool_size: u32,

    /// Minimum idle connections to maintain per node
    #[serde(default = "default_min_idle")]
    pub min_idle: u32,

    /// Idle connection timeout (seconds)
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,

    /// Maximum connection lifetime (seconds)
    #[serde(default = "default_max_lifetime_secs")]
    pub max_lifetime_secs: u64,

    /// Timeout for acquiring a connection (seconds)
    #[serde(default = "default_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,

    /// SQL to reset connection state when returning to pool
    #[serde(default = "default_reset_query")]
    pub reset_query: String,

    /// Prepared statement handling mode
    #[serde(default)]
    pub prepared_statement_mode: PreparedStatementMode,

    /// Whether to validate connections on acquire
    #[serde(default = "default_test_on_acquire")]
    pub test_on_acquire: bool,

    /// Validation query
    #[serde(default = "default_validation_query")]
    pub validation_query: String,

    /// Connection queue timeout when pool is exhausted (seconds)
    #[serde(default = "default_queue_timeout_secs")]
    pub queue_timeout_secs: u64,

    /// Maximum queue size when pool is exhausted (0 = unlimited)
    #[serde(default)]
    pub max_queue_size: u32,
}

fn default_max_pool_size() -> u32 {
    100
}

fn default_min_idle() -> u32 {
    10
}

fn default_idle_timeout_secs() -> u64 {
    600 // 10 minutes
}

fn default_max_lifetime_secs() -> u64 {
    3600 // 1 hour
}

fn default_acquire_timeout_secs() -> u64 {
    5
}

fn default_reset_query() -> String {
    "DISCARD ALL".to_string()
}

fn default_test_on_acquire() -> bool {
    true
}

fn default_validation_query() -> String {
    "SELECT 1".to_string()
}

fn default_queue_timeout_secs() -> u64 {
    30
}

impl Default for PoolModeConfig {
    fn default() -> Self {
        Self {
            default_mode: PoolingMode::default(),
            max_pool_size: default_max_pool_size(),
            min_idle: default_min_idle(),
            idle_timeout_secs: default_idle_timeout_secs(),
            max_lifetime_secs: default_max_lifetime_secs(),
            acquire_timeout_secs: default_acquire_timeout_secs(),
            reset_query: default_reset_query(),
            prepared_statement_mode: PreparedStatementMode::default(),
            test_on_acquire: default_test_on_acquire(),
            validation_query: default_validation_query(),
            queue_timeout_secs: default_queue_timeout_secs(),
            max_queue_size: 0,
        }
    }
}

impl PoolModeConfig {
    /// Create config for session mode (safest defaults)
    pub fn session_mode() -> Self {
        Self {
            default_mode: PoolingMode::Session,
            prepared_statement_mode: PreparedStatementMode::Named,
            ..Default::default()
        }
    }

    /// Create config for transaction mode (balanced)
    pub fn transaction_mode() -> Self {
        Self {
            default_mode: PoolingMode::Transaction,
            prepared_statement_mode: PreparedStatementMode::Track,
            ..Default::default()
        }
    }

    /// Create config for statement mode (most aggressive)
    pub fn statement_mode() -> Self {
        Self {
            default_mode: PoolingMode::Statement,
            prepared_statement_mode: PreparedStatementMode::Disable,
            ..Default::default()
        }
    }

    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_timeout_secs)
    }

    /// Get max lifetime as Duration
    pub fn max_lifetime(&self) -> Duration {
        Duration::from_secs(self.max_lifetime_secs)
    }

    /// Get acquire timeout as Duration
    pub fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.acquire_timeout_secs)
    }

    /// Get queue timeout as Duration
    pub fn queue_timeout(&self) -> Duration {
        Duration::from_secs(self.queue_timeout_secs)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_pool_size == 0 {
            return Err("max_pool_size must be > 0".to_string());
        }

        if self.min_idle > self.max_pool_size {
            return Err("min_idle cannot exceed max_pool_size".to_string());
        }

        if self.acquire_timeout_secs == 0 {
            return Err("acquire_timeout_secs must be > 0".to_string());
        }

        if self.reset_query.is_empty() {
            return Err("reset_query cannot be empty".to_string());
        }

        // Statement mode cannot use named prepared statements
        if self.default_mode == PoolingMode::Statement
            && self.prepared_statement_mode == PreparedStatementMode::Named
        {
            return Err("Statement mode cannot use named prepared statements".to_string());
        }

        Ok(())
    }

    /// Apply overrides from another config (for merging)
    pub fn merge(&mut self, other: &PoolModeConfig) {
        // Only override non-default values
        // This is a simplified merge - in practice you'd want Option<T> fields
        if other.max_pool_size != default_max_pool_size() {
            self.max_pool_size = other.max_pool_size;
        }
        if other.min_idle != default_min_idle() {
            self.min_idle = other.min_idle;
        }
        if other.idle_timeout_secs != default_idle_timeout_secs() {
            self.idle_timeout_secs = other.idle_timeout_secs;
        }
        if other.reset_query != default_reset_query() {
            self.reset_query = other.reset_query.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PoolModeConfig::default();
        assert_eq!(config.default_mode, PoolingMode::Session);
        assert_eq!(config.max_pool_size, 100);
        assert_eq!(config.min_idle, 10);
        assert_eq!(config.reset_query, "DISCARD ALL");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_session_mode_config() {
        let config = PoolModeConfig::session_mode();
        assert_eq!(config.default_mode, PoolingMode::Session);
        assert_eq!(config.prepared_statement_mode, PreparedStatementMode::Named);
    }

    #[test]
    fn test_transaction_mode_config() {
        let config = PoolModeConfig::transaction_mode();
        assert_eq!(config.default_mode, PoolingMode::Transaction);
        assert_eq!(config.prepared_statement_mode, PreparedStatementMode::Track);
    }

    #[test]
    fn test_statement_mode_config() {
        let config = PoolModeConfig::statement_mode();
        assert_eq!(config.default_mode, PoolingMode::Statement);
        assert_eq!(
            config.prepared_statement_mode,
            PreparedStatementMode::Disable
        );
    }

    #[test]
    fn test_validation() {
        let mut config = PoolModeConfig::default();

        // Valid config
        assert!(config.validate().is_ok());

        // Invalid: max_pool_size = 0
        config.max_pool_size = 0;
        assert!(config.validate().is_err());
        config.max_pool_size = 100;

        // Invalid: min_idle > max_pool_size
        config.min_idle = 200;
        assert!(config.validate().is_err());
        config.min_idle = 10;

        // Invalid: statement mode with named prepared statements
        config.default_mode = PoolingMode::Statement;
        config.prepared_statement_mode = PreparedStatementMode::Named;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_durations() {
        let config = PoolModeConfig::default();
        assert_eq!(config.idle_timeout(), Duration::from_secs(600));
        assert_eq!(config.max_lifetime(), Duration::from_secs(3600));
        assert_eq!(config.acquire_timeout(), Duration::from_secs(5));
    }
}
