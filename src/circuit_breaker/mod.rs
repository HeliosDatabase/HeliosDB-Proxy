//! Circuit Breaker Pattern for HeliosProxy
//!
//! Implements the circuit breaker pattern to protect against cascading failures
//! when backend nodes become unhealthy.
//!
//! # States
//!
//! - **Closed**: Normal operation, requests pass through, failures counted
//! - **Open**: Fail-fast mode, all requests rejected immediately
//! - **Half-Open**: Recovery testing, limited probe requests allowed
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb_lite::proxy::circuit_breaker::{
//!     CircuitBreaker, CircuitBreakerConfig, CircuitState,
//! };
//!
//! // Create a circuit breaker with 5 failure threshold
//! let config = CircuitBreakerConfig::builder()
//!     .failure_threshold(5)
//!     .failure_window_secs(30)
//!     .cooldown_secs(10)
//!     .build();
//!
//! let breaker = CircuitBreaker::new("primary-node", config);
//!
//! // Wrap requests with circuit breaker
//! match breaker.allow_request() {
//!     Ok(guard) => {
//!         // Execute request
//!         match execute_query() {
//!             Ok(result) => {
//!                 guard.success();
//!                 Ok(result)
//!             }
//!             Err(e) => {
//!                 guard.failure(&e);
//!                 Err(e)
//!             }
//!         }
//!     }
//!     Err(open) => {
//!         // Circuit is open, fail fast
//!         Err(ProxyError::CircuitOpen(open))
//!     }
//! }
//! ```

pub mod adaptive;
pub mod agent;
pub mod breaker;
pub mod config;
pub mod manager;
pub mod metrics;
pub mod sliding_counter;
pub mod state;

// Re-exports for public API
pub use adaptive::{AdaptiveThreshold, RollingStats};
pub use agent::{AgentRetryStrategy, ConversationFallback, RetryDecision};
pub use breaker::{CircuitBreaker, CircuitOpen, RequestGuard};
pub use config::{
    CircuitBreakerConfig, CircuitBreakerConfigBuilder, FailureConditions, NodeOverride,
    SyncModeThresholds,
};
pub use manager::{CircuitBreakerManager, ManagerConfig};
pub use metrics::{CircuitMetrics, CircuitStats, NodeCircuitStats};
pub use sliding_counter::SlidingWindowCounter;
pub use state::{CircuitState, StateTransition, TransitionReason};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_circuit_breaker_basic() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(3)
            .failure_window_secs(60)
            .cooldown_secs(5)
            .build();

        let breaker = CircuitBreaker::new("test-node", config);
        assert_eq!(breaker.get_state(), CircuitState::Closed);

        // Simulate failures
        for _ in 0..3 {
            breaker.record_failure_direct();
        }

        assert_eq!(breaker.get_state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_state_transitions() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .cooldown_secs(0) // Immediate transition for testing
            .half_open_success_threshold(2)
            .build();

        let breaker = CircuitBreaker::new("test-node", config);

        // Closed -> Open
        breaker.record_failure_direct();
        breaker.record_failure_direct();
        assert_eq!(breaker.get_state(), CircuitState::Open);

        // Open -> Half-Open (after cooldown)
        std::thread::sleep(Duration::from_millis(50));
        let guard = breaker
            .allow_request()
            .expect("should allow request after cooldown");
        assert_eq!(breaker.get_state(), CircuitState::HalfOpen);
        guard.success(); // Mark as successful to avoid auto-failure on drop

        // Need one more success to meet threshold of 2
        let guard = breaker.allow_request().expect("should allow probe");
        guard.success();
        assert_eq!(breaker.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_failure() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(1)
            .cooldown_secs(0)
            .build();

        let breaker = CircuitBreaker::new("test-node", config);

        // Go to half-open
        breaker.record_failure_direct();
        std::thread::sleep(Duration::from_millis(50));
        let guard = breaker
            .allow_request()
            .expect("should allow after cooldown");
        assert_eq!(breaker.get_state(), CircuitState::HalfOpen);

        // Failure in half-open goes back to open
        guard.failure("test failure");
        assert_eq!(breaker.get_state(), CircuitState::Open);
    }
}
