use crate::circuit_breaker::{
    CircuitBreakerConfig, CircuitBreakerManager, CircuitState, ManagerConfig,
};
use std::time::Duration;

fn mgr(threshold: u32) -> CircuitBreakerManager {
    let cfg = CircuitBreakerConfig {
        failure_threshold: threshold,
        cooldown: Duration::from_secs(10),
        ..Default::default()
    };
    CircuitBreakerManager::new(ManagerConfig::new(cfg))
}

#[test]
fn opens_after_threshold_failures() {
    let m = mgr(3);
    let b = m.get_breaker("n1");
    assert_eq!(b.get_state(), CircuitState::Closed);
    b.record_failure("boom");
    b.record_failure("boom");
    // Under threshold: still serving.
    assert_eq!(b.get_state(), CircuitState::Closed);
    // Threshold reached: tripped open.
    b.record_failure("boom");
    assert_eq!(b.get_state(), CircuitState::Open);
}

#[test]
fn healthy_node_stays_closed() {
    let m = mgr(3);
    let b = m.get_breaker("n2");
    b.record_success();
    b.record_success();
    assert_eq!(b.get_state(), CircuitState::Closed);
}
