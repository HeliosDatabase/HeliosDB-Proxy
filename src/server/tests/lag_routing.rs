use super::ProxyServer;

#[test]
fn ryw_pins_recent_write() {
    // A write "now" falls inside a 1s window -> pin to primary.
    assert!(ProxyServer::ryw_pins_primary(
        Some(std::time::Instant::now()),
        1000
    ));
}

#[test]
fn ryw_releases_old_write() {
    let old = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(10))
        .unwrap();
    assert!(!ProxyServer::ryw_pins_primary(Some(old), 1000));
}

#[test]
fn ryw_no_write_or_disabled() {
    assert!(!ProxyServer::ryw_pins_primary(None, 1000));
    // window=0 disables read-your-writes entirely.
    assert!(!ProxyServer::ryw_pins_primary(
        Some(std::time::Instant::now()),
        0
    ));
}

#[test]
fn lag_exclusion_thresholds() {
    // max=0 disables exclusion.
    assert!(!ProxyServer::lag_excludes_standby(Some(999_999), 0, false));
    // unknown lag never excludes unless the strict policy is on.
    assert!(!ProxyServer::lag_excludes_standby(None, 1000, false));
    assert!(ProxyServer::lag_excludes_standby(None, 1000, true));
    // within ceiling stays in rotation.
    assert!(!ProxyServer::lag_excludes_standby(Some(500), 1000, false));
    // beyond ceiling is dropped.
    assert!(ProxyServer::lag_excludes_standby(Some(2000), 1000, false));
}
