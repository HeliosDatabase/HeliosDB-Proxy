//! Deployment archive regressions exercised through the actual file loader.
use heliosdb_proxy::config::{NodeRole, ProxyConfig, TrMode};

fn load(text: &str) -> heliosdb_proxy::Result<ProxyConfig> {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), text).unwrap();
    ProxyConfig::from_file(file.path().to_str().unwrap())
}

#[test]
fn deployment_issue2_unsupported_ha_has_actionable_error() {
    for strict in [false, true] {
        let mut config = ProxyConfig {
            strict_config: strict,
            ..ProxyConfig::default()
        };
        config.add_node("127.0.0.1:5432", "primary").unwrap();
        let source = format!(
            "{}\n[ha]\nauto_failover = true\n",
            toml::to_string(&config).unwrap()
        );
        let error = load(&source).unwrap_err().to_string();
        assert!(error.contains("does not automatically promote"), "{error}");
        assert!(
            error.contains("[topology]") && error.contains("external HA manager"),
            "{error}"
        );
    }
}

#[test]
fn deployment_issue4_minimal_config_uses_operational_defaults() {
    let config = load("[[nodes]]\nhost = '127.0.0.1'\nrole = 'primary'\n").unwrap();
    assert_eq!(config.listen_address, "0.0.0.0:5432");
    assert_eq!(config.admin_address, "127.0.0.1:9090");
    assert!(config.admin_token.is_none());
    assert!(!config.admin_allow_insecure);
    assert!(config.tr_enabled);
    assert_eq!(config.tr_mode, TrMode::Session);
    assert_eq!(config.nodes[0].role, NodeRole::Primary);
    assert_eq!(config.nodes[0].port, 5432);
    assert_eq!(config.nodes[0].weight, 100);
    assert!(config.nodes[0].enabled);
    assert_eq!(
        config.pool.max_connections,
        ProxyConfig::default().pool.max_connections
    );
}

#[test]
fn deployment_issue4_partial_sections_preserve_explicit_values() {
    let config = load("[pool]\nmax_connections = 12\n[pool_mode]\nmode = 'transaction'\n[health]\ncheck_interval_secs = 2\n[load_balancer]\nread_write_split = false\n[[nodes]]\nhost = '127.0.0.1'\nport = 6432\nrole = 'primary'\nweight = 7\nenabled = false\n").unwrap();
    assert_eq!(config.pool.max_connections, 12);
    assert_eq!(
        config.pool.min_connections,
        ProxyConfig::default().pool.min_connections
    );
    assert_eq!(config.health.check_interval_secs, 2);
    assert!(!config.load_balancer.read_write_split);
    assert_eq!(config.nodes[0].port, 6432);
    assert_eq!(config.nodes[0].weight, 7);
    assert!(!config.nodes[0].enabled);
}

#[test]
fn deployment_issue4_defaults_do_not_bypass_validation() {
    assert!(load("")
        .unwrap_err()
        .to_string()
        .contains("No backend nodes"));
    assert!(load("[[nodes]]\nrole = 'primary'\n")
        .unwrap_err()
        .to_string()
        .contains("host"));
    assert!(load("[[nodes]]\nhost = '127.0.0.1'\n")
        .unwrap_err()
        .to_string()
        .contains("role"));
    assert!(load(
        "admin_address = '0.0.0.0:9090'\n[[nodes]]\nhost = '127.0.0.1'\nrole = 'primary'\n"
    )
    .unwrap_err()
    .to_string()
    .contains("admin_token"));
    assert!(
        load("[pool]\nmax_connections = 0\n[[nodes]]\nhost = '127.0.0.1'\nrole = 'primary'\n")
            .is_err()
    );
    assert!(load("strict_config = true\n[health]\ncheck_intervl_secs = 1\n[[nodes]]\nhost = '127.0.0.1'\nrole = 'primary'\n").unwrap_err().to_string().contains("health.check_intervl_secs"));
}
