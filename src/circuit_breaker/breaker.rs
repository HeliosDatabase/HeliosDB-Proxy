//! Circuit Breaker Implementation
//!
//! Core circuit breaker logic implementing the Closed -> Open -> Half-Open -> Closed
//! state machine for protecting against cascading failures.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::config::CircuitBreakerConfig;
use super::sliding_counter::SlidingWindowCounter;
use super::state::{CircuitBreakerListener, CircuitEvent, CircuitState, TransitionReason};

/// Error returned when circuit is open
#[derive(Debug, Clone)]
pub struct CircuitOpen {
    /// Node this circuit protects
    pub node_id: String,
    /// Time until circuit may transition to half-open
    pub retry_after: Duration,
    /// Current failure count
    pub failure_count: u32,
    /// Last error that caused circuit to open
    pub last_error: Option<String>,
}

impl CircuitOpen {
    /// Create a new CircuitOpen error
    pub fn new(node_id: impl Into<String>, retry_after: Duration) -> Self {
        Self {
            node_id: node_id.into(),
            retry_after,
            failure_count: 0,
            last_error: None,
        }
    }

    /// Add failure count
    pub fn with_failure_count(mut self, count: u32) -> Self {
        self.failure_count = count;
        self
    }

    /// Add last error
    pub fn with_last_error(mut self, error: impl Into<String>) -> Self {
        self.last_error = Some(error.into());
        self
    }

    /// Generate LLM-friendly error message
    pub fn to_llm_message(&self) -> String {
        format!(
            r#"{{"error":"circuit_breaker_open","node":"{}","retry_after_ms":{},"failure_count":{},"message":"Backend node is temporarily unavailable. Please retry after {} milliseconds."}}"#,
            self.node_id,
            self.retry_after.as_millis(),
            self.failure_count,
            self.retry_after.as_millis()
        )
    }
}

impl std::fmt::Display for CircuitOpen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Circuit open for node '{}', retry after {:?}",
            self.node_id, self.retry_after
        )
    }
}

impl std::error::Error for CircuitOpen {}

/// Guard returned from allow_request() that tracks request success/failure
pub struct RequestGuard {
    breaker: Arc<CircuitBreakerInner>,
    is_probe: bool,
    start_time: Instant,
    completed: bool,
}

impl RequestGuard {
    fn new(breaker: Arc<CircuitBreakerInner>) -> Self {
        Self {
            breaker,
            is_probe: false,
            start_time: Instant::now(),
            completed: false,
        }
    }

    fn new_probe(breaker: Arc<CircuitBreakerInner>) -> Self {
        breaker.active_probes.fetch_add(1, Ordering::SeqCst);
        Self {
            breaker,
            is_probe: true,
            start_time: Instant::now(),
            completed: false,
        }
    }

    /// Mark request as successful
    pub fn success(mut self) {
        self.completed = true;
        let duration = self.start_time.elapsed();

        if self.is_probe {
            self.breaker.active_probes.fetch_sub(1, Ordering::SeqCst);
            self.breaker.notify_probe_result(true, duration);
        }

        self.breaker.record_success_internal();
    }

    /// Mark request as failed
    pub fn failure(mut self, error: &str) {
        self.completed = true;
        let duration = self.start_time.elapsed();

        if self.is_probe {
            self.breaker.active_probes.fetch_sub(1, Ordering::SeqCst);
            self.breaker.notify_probe_result(false, duration);
        }

        self.breaker.record_failure_internal(error);
    }

    /// Get elapsed time since request started
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Check if this is a probe request (half-open state)
    pub fn is_probe(&self) -> bool {
        self.is_probe
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        // If not explicitly completed, count as failure
        if !self.completed {
            if self.is_probe {
                self.breaker.active_probes.fetch_sub(1, Ordering::SeqCst);
            }
            self.breaker.record_failure_internal("request abandoned");
        }
    }
}

/// Internal circuit breaker state
struct CircuitBreakerInner {
    /// Node this circuit protects
    node_id: String,

    /// Current state (atomic u8)
    state: AtomicU8,

    /// Failure counter (rolling window)
    failure_counter: SlidingWindowCounter,

    /// Success counter (for half-open validation)
    success_counter: AtomicU32,

    /// Time when circuit opened (nanos since start)
    opened_at: AtomicU64,

    /// Start time for relative timestamps
    start_time: Instant,

    /// Number of active probes in half-open
    active_probes: AtomicU32,

    /// Last error message
    last_error: RwLock<Option<String>>,

    /// Configuration
    config: RwLock<CircuitBreakerConfig>,

    /// Event listeners
    listeners: RwLock<Vec<Arc<dyn CircuitBreakerListener>>>,

    /// Transition history (limited size)
    history: RwLock<Vec<super::state::StateTransition>>,

    /// Total times circuit opened
    open_count: AtomicU64,

    /// Total failures recorded
    total_failures: AtomicU64,

    /// Total successes recorded
    total_successes: AtomicU64,
}

impl CircuitBreakerInner {
    fn get_state(&self) -> CircuitState {
        CircuitState::from_u8(self.state.load(Ordering::SeqCst)).unwrap_or(CircuitState::Closed)
    }

    fn now_nanos(&self) -> u64 {
        self.start_time.elapsed().as_nanos() as u64
    }

    fn should_try_half_open(&self) -> bool {
        let config = self.config.read();
        let opened_at = self.opened_at.load(Ordering::SeqCst);
        let elapsed_nanos = self.now_nanos().saturating_sub(opened_at);
        let elapsed = Duration::from_nanos(elapsed_nanos);
        elapsed >= config.cooldown
    }

    fn time_until_half_open(&self) -> Duration {
        let config = self.config.read();
        let opened_at = self.opened_at.load(Ordering::SeqCst);
        let elapsed_nanos = self.now_nanos().saturating_sub(opened_at);
        let elapsed = Duration::from_nanos(elapsed_nanos);
        config.cooldown.saturating_sub(elapsed)
    }

    fn can_probe(&self) -> bool {
        let config = self.config.read();
        self.active_probes.load(Ordering::SeqCst) < config.half_open_max_probes
    }

    fn transition_to_open(&self, reason: TransitionReason) {
        let prev = self
            .state
            .swap(CircuitState::Open as u8, Ordering::SeqCst);
        if prev != CircuitState::Open as u8 {
            self.opened_at.store(self.now_nanos(), Ordering::SeqCst);
            self.open_count.fetch_add(1, Ordering::SeqCst);

            let from = CircuitState::from_u8(prev).unwrap_or(CircuitState::Closed);
            self.record_transition(from, CircuitState::Open, reason.clone());

            self.notify_opened(reason);
        }
    }

    fn transition_to_half_open(&self) {
        let prev_state = self.get_state();
        self.state
            .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
        self.success_counter.store(0, Ordering::SeqCst);

        let config = self.config.read();
        let reason = TransitionReason::CooldownElapsed {
            cooldown: config.cooldown,
        };

        self.record_transition(prev_state, CircuitState::HalfOpen, reason);
        self.notify_half_opened();
    }

    fn transition_to_closed(&self, reason: TransitionReason) {
        let prev_state = self.get_state();
        self.state
            .store(CircuitState::Closed as u8, Ordering::SeqCst);
        self.failure_counter.reset();

        self.record_transition(prev_state, CircuitState::Closed, reason.clone());
        self.notify_closed(reason);
    }

    fn record_success_internal(&self) {
        self.total_successes.fetch_add(1, Ordering::SeqCst);

        match self.get_state() {
            CircuitState::Closed => {
                // Success in closed state - could reset failure counter
                // (optional behavior, we keep failures for window duration)
            }
            CircuitState::HalfOpen => {
                let config = self.config.read();
                let count = self.success_counter.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= config.half_open_success_threshold {
                    drop(config);
                    self.transition_to_closed(TransitionReason::ProbeSucceeded {
                        success_count: count,
                        threshold: count,
                    });
                }
            }
            CircuitState::Open => {
                // Should not happen - requests blocked in open state
            }
        }
    }

    fn record_failure_internal(&self, error: &str) {
        self.total_failures.fetch_add(1, Ordering::SeqCst);
        *self.last_error.write() = Some(error.to_string());

        match self.get_state() {
            CircuitState::Closed => {
                let count = self.failure_counter.increment();
                let config = self.config.read();
                let threshold = config.failure_threshold;

                self.notify_failure_recorded(error, count);

                if count >= threshold {
                    drop(config);
                    self.transition_to_open(TransitionReason::FailureThresholdExceeded {
                        failure_count: count,
                        threshold,
                    });
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open goes back to open
                self.transition_to_open(TransitionReason::ProbeFailed {
                    error: error.to_string(),
                });
            }
            CircuitState::Open => {
                // Already open, just update timestamp
                self.opened_at.store(self.now_nanos(), Ordering::SeqCst);
            }
        }
    }

    fn record_transition(
        &self,
        from: CircuitState,
        to: CircuitState,
        reason: TransitionReason,
    ) {
        let transition = super::state::StateTransition::new(from, to, reason);
        let mut history = self.history.write();
        history.push(transition);
        // Keep last 100 transitions
        if history.len() > 100 {
            history.remove(0);
        }
    }

    fn notify_opened(&self, reason: TransitionReason) {
        let event = CircuitEvent::Opened {
            node_id: self.node_id.clone(),
            reason,
            failure_count: self.failure_counter.count(),
        };
        self.notify_listeners(event);
    }

    fn notify_half_opened(&self) {
        let config = self.config.read();
        let event = CircuitEvent::HalfOpened {
            node_id: self.node_id.clone(),
            cooldown_elapsed: config.cooldown,
        };
        drop(config);
        self.notify_listeners(event);
    }

    fn notify_closed(&self, reason: TransitionReason) {
        // Calculate recovery time from when circuit was opened
        let opened_at = self.opened_at.load(Ordering::SeqCst);
        let recovery_nanos = self.now_nanos().saturating_sub(opened_at);

        let event = CircuitEvent::Closed {
            node_id: self.node_id.clone(),
            reason,
            recovery_time: Duration::from_nanos(recovery_nanos),
        };
        self.notify_listeners(event);
    }

    fn notify_failure_recorded(&self, error: &str, count: u32) {
        let event = CircuitEvent::FailureRecorded {
            node_id: self.node_id.clone(),
            error: error.to_string(),
            failure_count: count,
        };
        self.notify_listeners(event);
    }

    fn notify_probe_result(&self, success: bool, duration: Duration) {
        let event = CircuitEvent::ProbeResult {
            node_id: self.node_id.clone(),
            success,
            duration,
        };
        self.notify_listeners(event);
    }

    fn notify_listeners(&self, event: CircuitEvent) {
        let listeners = self.listeners.read();
        for listener in listeners.iter() {
            listener.on_event(event.clone());
        }
    }
}

/// Circuit Breaker
///
/// Protects against cascading failures by tracking errors and temporarily
/// blocking requests to unhealthy nodes.
#[derive(Clone)]
pub struct CircuitBreaker {
    inner: Arc<CircuitBreakerInner>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(node_id: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        let failure_window = config.failure_window;
        Self {
            inner: Arc::new(CircuitBreakerInner {
                node_id: node_id.into(),
                state: AtomicU8::new(CircuitState::Closed as u8),
                failure_counter: SlidingWindowCounter::new(failure_window),
                success_counter: AtomicU32::new(0),
                opened_at: AtomicU64::new(0),
                start_time: Instant::now(),
                active_probes: AtomicU32::new(0),
                last_error: RwLock::new(None),
                config: RwLock::new(config),
                listeners: RwLock::new(Vec::new()),
                history: RwLock::new(Vec::new()),
                open_count: AtomicU64::new(0),
                total_failures: AtomicU64::new(0),
                total_successes: AtomicU64::new(0),
            }),
        }
    }

    /// Get the node ID this circuit protects
    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    /// Get current circuit state
    pub fn get_state(&self) -> CircuitState {
        self.inner.get_state()
    }

    /// Try to get permission for a request
    pub fn allow_request(&self) -> Result<RequestGuard, CircuitOpen> {
        match self.inner.get_state() {
            CircuitState::Closed => Ok(RequestGuard::new(Arc::clone(&self.inner))),
            CircuitState::Open => {
                if self.inner.should_try_half_open() {
                    self.inner.transition_to_half_open();
                    Ok(RequestGuard::new_probe(Arc::clone(&self.inner)))
                } else {
                    Err(CircuitOpen::new(
                        &self.inner.node_id,
                        self.inner.time_until_half_open(),
                    )
                    .with_failure_count(self.inner.failure_counter.count())
                    .with_last_error(
                        self.inner
                            .last_error
                            .read()
                            .clone()
                            .unwrap_or_default(),
                    ))
                }
            }
            CircuitState::HalfOpen => {
                if self.inner.can_probe() {
                    Ok(RequestGuard::new_probe(Arc::clone(&self.inner)))
                } else {
                    Err(CircuitOpen::new(&self.inner.node_id, Duration::from_millis(100))
                        .with_failure_count(self.inner.failure_counter.count()))
                }
            }
        }
    }

    /// Record a success (called internally by RequestGuard)
    pub fn record_success(&self) {
        self.inner.record_success_internal();
    }

    /// Record a failure directly (bypassing guard pattern)
    pub fn record_failure_direct(&self) {
        self.inner.record_failure_internal("direct failure");
    }

    /// Record a failure with error message directly
    pub fn record_failure(&self, error: &str) {
        self.inner.record_failure_internal(error);
    }

    /// Force circuit to open state
    pub fn force_open(&self, admin: Option<&str>) {
        self.inner.transition_to_open(TransitionReason::ManualForce {
            admin: admin.map(String::from),
        });
    }

    /// Force circuit to closed state
    pub fn force_close(&self, admin: Option<&str>) {
        self.inner.transition_to_closed(TransitionReason::ManualForce {
            admin: admin.map(String::from),
        });
    }

    /// Reset circuit breaker (close and clear counters)
    pub fn reset(&self) {
        self.inner.failure_counter.reset();
        self.inner.success_counter.store(0, Ordering::SeqCst);
        self.inner.transition_to_closed(TransitionReason::Reset);
    }

    /// Update configuration
    pub fn update_config(&self, config: CircuitBreakerConfig) {
        *self.inner.config.write() = config;
    }

    /// Get current configuration
    pub fn config(&self) -> CircuitBreakerConfig {
        self.inner.config.read().clone()
    }

    /// Add event listener
    pub fn add_listener(&self, listener: Arc<dyn CircuitBreakerListener>) {
        self.inner.listeners.write().push(listener);
    }

    /// Get current failure count
    pub fn failure_count(&self) -> u32 {
        self.inner.failure_counter.count()
    }

    /// Get total times circuit has opened
    pub fn open_count(&self) -> u64 {
        self.inner.open_count.load(Ordering::SeqCst)
    }

    /// Get total failures recorded
    pub fn total_failures(&self) -> u64 {
        self.inner.total_failures.load(Ordering::SeqCst)
    }

    /// Get total successes recorded
    pub fn total_successes(&self) -> u64 {
        self.inner.total_successes.load(Ordering::SeqCst)
    }

    /// Get last error message
    pub fn last_error(&self) -> Option<String> {
        self.inner.last_error.read().clone()
    }

    /// Get time until circuit may transition to half-open (if open)
    pub fn time_until_half_open(&self) -> Option<Duration> {
        if self.get_state() == CircuitState::Open {
            Some(self.inner.time_until_half_open())
        } else {
            None
        }
    }

    /// Get transition history
    pub fn get_history(&self) -> Vec<super::state::StateTransition> {
        self.inner.history.read().clone()
    }
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("node_id", &self.inner.node_id)
            .field("state", &self.get_state())
            .field("failure_count", &self.failure_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_new() {
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new("test-node", config);

        assert_eq!(breaker.node_id(), "test-node");
        assert_eq!(breaker.get_state(), CircuitState::Closed);
        assert_eq!(breaker.failure_count(), 0);
    }

    #[test]
    fn test_circuit_breaker_allow_request_closed() {
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new("test-node", config);

        let guard = breaker.allow_request().expect("should allow");
        assert!(!guard.is_probe());
        guard.success();

        assert_eq!(breaker.total_successes(), 1);
    }

    #[test]
    fn test_circuit_breaker_open_on_failures() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(3)
            .build();
        let breaker = CircuitBreaker::new("test-node", config);

        breaker.record_failure("error 1");
        breaker.record_failure("error 2");
        assert_eq!(breaker.get_state(), CircuitState::Closed);

        breaker.record_failure("error 3");
        assert_eq!(breaker.get_state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_open_error() {
        let err = CircuitOpen::new("node-1", Duration::from_secs(10))
            .with_failure_count(5)
            .with_last_error("connection timeout");

        assert_eq!(err.node_id, "node-1");
        assert_eq!(err.failure_count, 5);
        assert_eq!(err.last_error, Some("connection timeout".to_string()));

        let msg = err.to_llm_message();
        assert!(msg.contains("circuit_breaker_open"));
        assert!(msg.contains("node-1"));
    }

    #[test]
    fn test_force_open_close() {
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new("test-node", config);

        breaker.force_open(Some("admin"));
        assert_eq!(breaker.get_state(), CircuitState::Open);

        breaker.force_close(Some("admin"));
        assert_eq!(breaker.get_state(), CircuitState::Closed);
    }

    #[test]
    fn test_reset() {
        let config = CircuitBreakerConfig::builder()
            .failure_threshold(2)
            .build();
        let breaker = CircuitBreaker::new("test-node", config);

        breaker.record_failure("error 1");
        breaker.record_failure("error 2");
        assert_eq!(breaker.get_state(), CircuitState::Open);

        breaker.reset();
        assert_eq!(breaker.get_state(), CircuitState::Closed);
        assert_eq!(breaker.failure_count(), 0);
    }
}
