//! Circuit Breaker State Machine
//!
//! Defines the circuit breaker states and transition logic.

use std::fmt;
use std::time::{Duration, Instant};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CircuitState {
    /// Normal operation, requests pass through, failures counted
    Closed = 0,
    /// Fail-fast mode, all requests rejected immediately
    Open = 1,
    /// Recovery testing, limited probe requests allowed
    HalfOpen = 2,
}

impl CircuitState {
    /// Convert from u8 representation
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(CircuitState::Closed),
            1 => Some(CircuitState::Open),
            2 => Some(CircuitState::HalfOpen),
            _ => None,
        }
    }

    /// Check if circuit is allowing requests
    pub fn allows_requests(&self) -> bool {
        match self {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => true, // Limited requests
        }
    }

    /// Check if circuit is in a failure state
    pub fn is_unhealthy(&self) -> bool {
        match self {
            CircuitState::Closed => false,
            CircuitState::Open | CircuitState::HalfOpen => true,
        }
    }
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half_open"),
        }
    }
}

impl std::str::FromStr for CircuitState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "closed" => Ok(CircuitState::Closed),
            "open" => Ok(CircuitState::Open),
            "half_open" | "halfopen" | "half-open" => Ok(CircuitState::HalfOpen),
            _ => Err(format!("Unknown circuit state: {}", s)),
        }
    }
}

/// Record of a state transition
#[derive(Debug, Clone)]
pub struct StateTransition {
    /// State before transition
    pub from: CircuitState,
    /// State after transition
    pub to: CircuitState,
    /// When the transition occurred
    pub timestamp: Instant,
    /// Reason for the transition
    pub reason: TransitionReason,
}

impl StateTransition {
    /// Create a new state transition record
    pub fn new(from: CircuitState, to: CircuitState, reason: TransitionReason) -> Self {
        Self {
            from,
            to,
            timestamp: Instant::now(),
            reason,
        }
    }

    /// Time since this transition occurred
    pub fn elapsed(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

impl fmt::Display for StateTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {} ({})", self.from, self.to, self.reason)
    }
}

/// Reason for a state transition
#[derive(Debug, Clone)]
pub enum TransitionReason {
    /// Failures exceeded threshold
    FailureThresholdExceeded { failure_count: u32, threshold: u32 },
    /// Cooldown period elapsed
    CooldownElapsed { cooldown: Duration },
    /// Probe succeeded in half-open
    ProbeSucceeded { success_count: u32, threshold: u32 },
    /// Probe failed in half-open
    ProbeFailed { error: String },
    /// Manual intervention (admin force)
    ManualForce { admin: Option<String> },
    /// Reset requested
    Reset,
    /// Adaptive threshold adjusted
    AdaptiveAdjustment {
        old_threshold: u32,
        new_threshold: u32,
    },
    /// Replication lag exceeded threshold
    ReplicationLagExceeded { lag: Duration, threshold: Duration },
}

impl fmt::Display for TransitionReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransitionReason::FailureThresholdExceeded {
                failure_count,
                threshold,
            } => write!(f, "{} failures (threshold: {})", failure_count, threshold),
            TransitionReason::CooldownElapsed { cooldown } => {
                write!(f, "cooldown elapsed ({:?})", cooldown)
            }
            TransitionReason::ProbeSucceeded {
                success_count,
                threshold,
            } => write!(
                f,
                "{} successful probes (threshold: {})",
                success_count, threshold
            ),
            TransitionReason::ProbeFailed { error } => write!(f, "probe failed: {}", error),
            TransitionReason::ManualForce { admin } => {
                if let Some(admin) = admin {
                    write!(f, "manual force by {}", admin)
                } else {
                    write!(f, "manual force")
                }
            }
            TransitionReason::Reset => write!(f, "reset"),
            TransitionReason::AdaptiveAdjustment {
                old_threshold,
                new_threshold,
            } => write!(
                f,
                "adaptive adjustment {} -> {}",
                old_threshold, new_threshold
            ),
            TransitionReason::ReplicationLagExceeded { lag, threshold } => {
                write!(f, "replication lag {:?} > {:?}", lag, threshold)
            }
        }
    }
}

/// Event types for circuit breaker listeners
#[derive(Debug, Clone)]
pub enum CircuitEvent {
    /// Circuit opened (entered fail-fast mode)
    Opened {
        node_id: String,
        reason: TransitionReason,
        failure_count: u32,
    },
    /// Circuit moved to half-open (testing recovery)
    HalfOpened {
        node_id: String,
        cooldown_elapsed: Duration,
    },
    /// Circuit closed (recovered)
    Closed {
        node_id: String,
        reason: TransitionReason,
        recovery_time: Duration,
    },
    /// Failure recorded
    FailureRecorded {
        node_id: String,
        error: String,
        failure_count: u32,
    },
    /// Probe attempt
    ProbeAttempt { node_id: String, attempt: u32 },
    /// Probe result
    ProbeResult {
        node_id: String,
        success: bool,
        duration: Duration,
    },
}

impl CircuitEvent {
    /// Get the node ID associated with this event
    pub fn node_id(&self) -> &str {
        match self {
            CircuitEvent::Opened { node_id, .. }
            | CircuitEvent::HalfOpened { node_id, .. }
            | CircuitEvent::Closed { node_id, .. }
            | CircuitEvent::FailureRecorded { node_id, .. }
            | CircuitEvent::ProbeAttempt { node_id, .. }
            | CircuitEvent::ProbeResult { node_id, .. } => node_id,
        }
    }

    /// Get the event type as a string
    pub fn event_type(&self) -> &'static str {
        match self {
            CircuitEvent::Opened { .. } => "opened",
            CircuitEvent::HalfOpened { .. } => "half_opened",
            CircuitEvent::Closed { .. } => "closed",
            CircuitEvent::FailureRecorded { .. } => "failure_recorded",
            CircuitEvent::ProbeAttempt { .. } => "probe_attempt",
            CircuitEvent::ProbeResult { .. } => "probe_result",
        }
    }
}

/// Listener trait for circuit breaker events
pub trait CircuitBreakerListener: Send + Sync {
    /// Called when a circuit event occurs
    fn on_event(&self, event: CircuitEvent);
}

/// No-op listener for testing
#[derive(Debug, Default)]
pub struct NoOpListener;

impl CircuitBreakerListener for NoOpListener {
    fn on_event(&self, _event: CircuitEvent) {}
}

/// Logging listener that logs events to tracing
#[derive(Debug, Default)]
pub struct LoggingListener;

impl CircuitBreakerListener for LoggingListener {
    fn on_event(&self, event: CircuitEvent) {
        match &event {
            CircuitEvent::Opened {
                node_id,
                reason,
                failure_count,
            } => {
                tracing::warn!(
                    node_id = %node_id,
                    reason = %reason,
                    failure_count = failure_count,
                    "Circuit breaker opened"
                );
            }
            CircuitEvent::HalfOpened {
                node_id,
                cooldown_elapsed,
            } => {
                tracing::info!(
                    node_id = %node_id,
                    cooldown_elapsed = ?cooldown_elapsed,
                    "Circuit breaker half-opened"
                );
            }
            CircuitEvent::Closed {
                node_id,
                reason,
                recovery_time,
            } => {
                tracing::info!(
                    node_id = %node_id,
                    reason = %reason,
                    recovery_time = ?recovery_time,
                    "Circuit breaker closed"
                );
            }
            CircuitEvent::FailureRecorded {
                node_id,
                error,
                failure_count,
            } => {
                tracing::debug!(
                    node_id = %node_id,
                    error = %error,
                    failure_count = failure_count,
                    "Failure recorded"
                );
            }
            CircuitEvent::ProbeAttempt { node_id, attempt } => {
                tracing::debug!(
                    node_id = %node_id,
                    attempt = attempt,
                    "Probe attempt"
                );
            }
            CircuitEvent::ProbeResult {
                node_id,
                success,
                duration,
            } => {
                tracing::debug!(
                    node_id = %node_id,
                    success = success,
                    duration = ?duration,
                    "Probe result"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_state_from_u8() {
        assert_eq!(CircuitState::from_u8(0), Some(CircuitState::Closed));
        assert_eq!(CircuitState::from_u8(1), Some(CircuitState::Open));
        assert_eq!(CircuitState::from_u8(2), Some(CircuitState::HalfOpen));
        assert_eq!(CircuitState::from_u8(3), None);
    }

    #[test]
    fn test_circuit_state_allows_requests() {
        assert!(CircuitState::Closed.allows_requests());
        assert!(!CircuitState::Open.allows_requests());
        assert!(CircuitState::HalfOpen.allows_requests());
    }

    #[test]
    fn test_circuit_state_display() {
        assert_eq!(CircuitState::Closed.to_string(), "closed");
        assert_eq!(CircuitState::Open.to_string(), "open");
        assert_eq!(CircuitState::HalfOpen.to_string(), "half_open");
    }

    #[test]
    fn test_circuit_state_parse() {
        assert_eq!(
            "closed".parse::<CircuitState>().unwrap(),
            CircuitState::Closed
        );
        assert_eq!("OPEN".parse::<CircuitState>().unwrap(), CircuitState::Open);
        assert_eq!(
            "half_open".parse::<CircuitState>().unwrap(),
            CircuitState::HalfOpen
        );
        assert_eq!(
            "half-open".parse::<CircuitState>().unwrap(),
            CircuitState::HalfOpen
        );
    }

    #[test]
    fn test_state_transition() {
        let transition = StateTransition::new(
            CircuitState::Closed,
            CircuitState::Open,
            TransitionReason::FailureThresholdExceeded {
                failure_count: 5,
                threshold: 5,
            },
        );

        assert_eq!(transition.from, CircuitState::Closed);
        assert_eq!(transition.to, CircuitState::Open);
        assert!(transition.elapsed().as_nanos() > 0);
    }

    #[test]
    fn test_transition_reason_display() {
        let reason = TransitionReason::FailureThresholdExceeded {
            failure_count: 5,
            threshold: 5,
        };
        assert_eq!(reason.to_string(), "5 failures (threshold: 5)");

        let reason = TransitionReason::CooldownElapsed {
            cooldown: Duration::from_secs(10),
        };
        assert_eq!(reason.to_string(), "cooldown elapsed (10s)");
    }

    #[test]
    fn test_circuit_event_node_id() {
        let event = CircuitEvent::Opened {
            node_id: "test-node".to_string(),
            reason: TransitionReason::Reset,
            failure_count: 0,
        };
        assert_eq!(event.node_id(), "test-node");
        assert_eq!(event.event_type(), "opened");
    }
}
