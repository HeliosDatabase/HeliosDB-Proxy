//! Admin node overrides reach health and routing (Sprinter b251750f1d54):
//! `/nodes/{addr}/disable` and `/api/chaos force_unhealthy` hold a node out
//! of routing within the call, survive probe sweeps and SIGHUP reconciles,
//! and clear without re-admitting a node nothing has checked.
use super::*;
use crate::node_overrides::{NodeOverride, OverrideKind};

/// Two configured primaries (static topology fails over to the second) and
/// one standby.
fn three_nodes() -> ProxyConfig {
    let mut config = ProxyConfig {
        listen_address: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    config.add_node("pg-a:5432", "primary").unwrap();
    config.add_node("pg-b:5432", "primary").unwrap();
    config.add_node("pg-s:5432", "standby").unwrap();
    config
}

fn healthy(server: &ProxyServer, addr: &str) -> bool {
    server
        .state
        .health
        .load_full()
        .get(addr)
        .map(|h| h.healthy)
        .unwrap_or(false)
}

fn held(kind: OverrideKind) -> Option<NodeOverride> {
    Some(NodeOverride::now(kind, "test"))
}

fn replicas(server: &ProxyServer) -> Vec<String> {
    server
        .state
        .routing
        .load()
        .replicas
        .iter()
        .map(|r| r.address.clone())
        .collect()
}

#[tokio::test]
async fn forcing_the_primary_moves_the_home_within_the_call() {
    let config = three_nodes();
    let server = ProxyServer::new(config.clone()).unwrap();
    assert_eq!(
        ProxyServer::home_node(&server.state, &config).as_deref(),
        Some("pg-a:5432")
    );

    let healthy_after = ProxyServer::apply_override(
        &server.state,
        "pg-a:5432",
        held(OverrideKind::ForceUnhealthy),
    )
    .unwrap();
    assert!(!healthy_after);
    assert!(!healthy(&server, "pg-a:5432"));
    assert_eq!(
        ProxyServer::home_node(&server.state, &config).as_deref(),
        Some("pg-b:5432"),
        "writes move to the other healthy primary at once"
    );
    assert_eq!(
        server.state.routing.load().leader.as_deref(),
        Some("pg-b:5432"),
        "the routing snapshot was republished inside the call"
    );
    assert_eq!(
        server.state.node_overrides.get("pg-a:5432").unwrap().kind,
        OverrideKind::ForceUnhealthy
    );
}

#[tokio::test]
async fn disabling_a_standby_removes_it_from_the_read_set() {
    let config = three_nodes();
    let server = ProxyServer::new(config).unwrap();
    assert_eq!(replicas(&server), vec!["pg-s:5432".to_string()]);
    ProxyServer::apply_override(&server.state, "pg-s:5432", held(OverrideKind::Disabled)).unwrap();
    assert!(replicas(&server).is_empty());
    assert!(!healthy(&server, "pg-s:5432"));
    // The primaries are untouched.
    assert_eq!(
        server.state.routing.load().leader.as_deref(),
        Some("pg-a:5432")
    );
}

#[tokio::test]
async fn an_unknown_node_is_refused() {
    let server = ProxyServer::new(three_nodes()).unwrap();
    let err = ProxyServer::apply_override(&server.state, "nope:5432", held(OverrideKind::Disabled))
        .unwrap_err();
    assert!(err.to_string().contains("Node not found"), "{err}");
    assert!(server.state.node_overrides.is_empty());
}

/// A listener that answers the credential-less probe (SSLRequest -> `N`),
/// so every health sweep against it passes.
async fn live_postmaster() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = [0u8; 8];
                if socket.read_exact(&mut request).await.is_ok() {
                    let _ = socket.write_all(b"N").await;
                }
            });
        }
    });
    addr
}

#[tokio::test]
async fn a_passing_probe_cannot_resurrect_an_overridden_node() {
    let addr = live_postmaster().await;
    let mut config = ProxyConfig {
        listen_address: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    config.add_node(&addr, "primary").unwrap();
    config.health.check_timeout_secs = 2;
    let server = ProxyServer::new(config.clone()).unwrap();

    ProxyServer::apply_override(&server.state, &addr, held(OverrideKind::ForceUnhealthy)).unwrap();
    for sweep in 0..3 {
        ProxyServer::check_all_nodes(&server.state, &config).await;
        assert!(
            !healthy(&server, &addr),
            "sweep {sweep}: a passing probe re-admitted an overridden node"
        );
        assert!(ProxyServer::home_node(&server.state, &config).is_none());
    }
    let snapshot = server.state.health.load_full();
    let probes = snapshot.get(&addr).unwrap();
    assert_eq!(probes.failure_count, 0, "the probes themselves passed");
    assert!(
        probes.success_count >= config.health.success_threshold.max(1),
        "the counters kept recording the passing probes"
    );
    drop(snapshot);

    // Clearing re-admits the node at once: its counters already prove it.
    assert!(ProxyServer::apply_override(&server.state, &addr, None).unwrap());
    assert!(healthy(&server, &addr));
    assert_eq!(
        ProxyServer::home_node(&server.state, &config).as_deref(),
        Some(addr.as_str())
    );
    assert!(server.state.node_overrides.is_empty());
}

#[tokio::test]
async fn clearing_without_proof_waits_for_the_next_probe() {
    let config = three_nodes();
    let server = ProxyServer::new(config.clone()).unwrap();

    // Seeded healthy, never probed: an enable does not route to it blindly.
    ProxyServer::apply_override(&server.state, "pg-a:5432", held(OverrideKind::Disabled)).unwrap();
    assert!(!ProxyServer::apply_override(&server.state, "pg-a:5432", None).unwrap());
    assert!(!healthy(&server, "pg-a:5432"));
    assert!(
        server.state.node_overrides.is_empty(),
        "the override itself is gone"
    );
    assert_eq!(
        ProxyServer::home_node(&server.state, &config).as_deref(),
        Some("pg-b:5432")
    );

    // A node mid-failure is not re-admitted either, however many successes
    // preceded the failure.
    {
        let _writers = server.state.health_write.lock();
        let mut next = (*server.state.health.load_full()).clone();
        let nh = next.get_mut("pg-b:5432").unwrap();
        nh.success_count = 10;
        nh.failure_count = 1;
        server.state.health.store(Arc::new(next));
    }
    ProxyServer::apply_override(&server.state, "pg-b:5432", held(OverrideKind::Disabled)).unwrap();
    assert!(!ProxyServer::apply_override(&server.state, "pg-b:5432", None).unwrap());
    assert!(!healthy(&server, "pg-b:5432"));

    // A proven node (no failure in progress, enough successes) comes back.
    {
        let _writers = server.state.health_write.lock();
        let mut next = (*server.state.health.load_full()).clone();
        let nh = next.get_mut("pg-a:5432").unwrap();
        nh.success_count = config.health.success_threshold.max(1);
        nh.failure_count = 0;
        server.state.health.store(Arc::new(next));
    }
    ProxyServer::apply_override(&server.state, "pg-a:5432", held(OverrideKind::Disabled)).unwrap();
    assert!(ProxyServer::apply_override(&server.state, "pg-a:5432", None).unwrap());
    assert!(healthy(&server, "pg-a:5432"));
    assert_eq!(
        ProxyServer::home_node(&server.state, &config).as_deref(),
        Some("pg-a:5432")
    );
}

#[tokio::test]
async fn sighup_reconcile_keeps_overrides_and_drops_removed_nodes() {
    let config = three_nodes();
    let server = ProxyServer::new(config).unwrap();
    ProxyServer::apply_override(
        &server.state,
        "pg-a:5432",
        held(OverrideKind::ForceUnhealthy),
    )
    .unwrap();
    ProxyServer::apply_override(&server.state, "pg-s:5432", held(OverrideKind::Disabled)).unwrap();

    // The reload drops the standby and keeps both primaries.
    let mut smaller = ProxyConfig {
        listen_address: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    smaller.add_node("pg-a:5432", "primary").unwrap();
    smaller.add_node("pg-b:5432", "primary").unwrap();
    ProxyServer::reconcile_health(&server.state, &smaller);

    assert!(
        !healthy(&server, "pg-a:5432"),
        "the rebuilt map keeps the override"
    );
    assert!(server.state.node_overrides.is_overridden("pg-a:5432"));
    assert!(
        !server.state.node_overrides.is_overridden("pg-s:5432"),
        "a removed node's override is dropped with it"
    );
    assert_eq!(
        server.state.routing.load().leader.as_deref(),
        Some("pg-b:5432")
    );
}

#[test]
fn apply_node_overrides_only_touches_overridden_nodes() {
    let overrides = crate::node_overrides::NodeOverrides::new();
    let seed = |addr: &str| NodeHealth {
        address: addr.to_string(),
        healthy: true,
        last_check: chrono::Utc::now(),
        failure_count: 0,
        success_count: 3,
        last_error: None,
        latency_ms: 1.0,
        replication_lag_bytes: None,
        lag_sampled_at: None,
    };
    let mut map: HashMap<String, NodeHealth> = ["a:1", "b:1"]
        .into_iter()
        .map(|a| (a.to_string(), seed(a)))
        .collect();
    ProxyServer::apply_node_overrides(&overrides, &mut map);
    assert!(
        map["a:1"].healthy && map["b:1"].healthy,
        "no override, no change"
    );
    overrides.set("a:1", NodeOverride::now(OverrideKind::Disabled, ""));
    ProxyServer::apply_node_overrides(&overrides, &mut map);
    assert!(!map["a:1"].healthy);
    assert!(map["b:1"].healthy);
    assert_eq!(map["a:1"].success_count, 3, "counters are the probes' own");
}
