use super::*;
use crate::multi_tenancy::{
    IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId,
    TenantManager, TenantManagerBuilder, TenantQueryTransformer,
};

fn manager() -> TenantManager {
    let transformer = TenantQueryTransformer::new().register_tables(&["t"], "tid");
    let tm = TenantManagerBuilder::new()
        .config(MultiTenancyConfig {
            enabled: true,
            identification: IdentificationMethod::Header {
                header_name: "application_name".to_string(),
            },
            ..Default::default()
        })
        .query_transformer(transformer)
        .build();
    tm.register_tenant(TenantConfig::new(
        TenantId::new("acme"),
        IsolationStrategy::row("public", "tid"),
    ));
    tm
}

#[test]
fn tenant_table_gets_filter() {
    let res = manager().transform_query("select * from t", &TenantId::new("acme"));
    assert!(res.transformed, "expected a tenant filter to be injected");
    let q = res.query.to_lowercase();
    assert!(
        q.contains("tid") && q.contains("acme"),
        "filter missing: {}",
        res.query
    );
}

#[test]
fn non_tenant_table_passes_through() {
    let res = manager().transform_query("select * from other", &TenantId::new("acme"));
    assert!(!res.transformed);
}

/// H-05 per-tenant admission: a tenant at its cap is refused (53300 naming
/// it) while other tenants, unknown tenants and cancel requests pass.
#[tokio::test]
async fn tenant_admission_caps_each_tenant_separately() {
    let mut config = test_config();
    config.multi_tenancy.enabled = true;
    config.multi_tenancy.tenants = vec!["acme".into(), "tiny".into()];
    config.multi_tenancy.max_clients_per_tenant = 1;
    let server = ProxyServer::new(config).unwrap();
    let startup = |app: &str| StartupMessage::Startup {
        protocol_version: 196608,
        params: [
            ("user".to_string(), "u".to_string()),
            ("application_name".to_string(), app.to_string()),
        ]
        .into(),
    };
    let held = ProxyServer::admit_tenant_slot(&server.state, &startup("acme"))
        .await
        .unwrap();
    assert!(held.is_some());
    let refused = ProxyServer::admit_tenant_slot(&server.state, &startup("acme"))
        .await
        .unwrap_err();
    assert_eq!(refused.tenant, "acme");
    assert!(refused
        .to_string()
        .contains("too many connections for tenant"));
    assert!(
        ProxyServer::admit_tenant_slot(&server.state, &startup("tiny"))
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        ProxyServer::admit_tenant_slot(&server.state, &startup("other"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(ProxyServer::admit_tenant_slot(
        &server.state,
        &StartupMessage::CancelRequest { pid: 1, key: 2 }
    )
    .await
    .unwrap()
    .is_none());
    assert_eq!(
        server
            .state
            .metrics
            .connections_rejected
            .load(Ordering::Relaxed),
        1
    );
    // Releasing the tenant's session frees its slot.
    drop(held);
    assert!(
        ProxyServer::admit_tenant_slot(&server.state, &startup("acme"))
            .await
            .unwrap()
            .is_some()
    );
}

/// ac858d61400b: `[multi_tenancy]` is built once at startup, so a SIGHUP that
/// changes it warns and keeps the running section (the published config
/// stays truthful), while the rest of the reload applies.
#[tokio::test]
async fn a_sighup_keeps_the_running_multi_tenancy_section() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("proxy.toml");
    let boot = "[[nodes]]\nhost = \"db\"\nport = 5432\nrole = \"primary\"\n\
                [multi_tenancy]\nenabled = true\ntenants = [\"acme\"]\n\
                max_active_queries_per_tenant = 2\n";
    std::fs::write(&path, boot).unwrap();
    let config = ProxyConfig::from_file(path.to_str().unwrap()).unwrap();
    let running = config.multi_tenancy.clone();
    let server = ProxyServer::new(config)
        .unwrap()
        .with_config_path(Some(path.to_string_lossy().into_owned()));

    std::fs::write(
        &path,
        format!(
            "[pool]\nmax_connections = 77\n{boot}\
             [multi_tenancy.tenant_max_active_queries]\nacme = 9\n"
        ),
    )
    .unwrap();
    server.reload_config().await;
    let live = server.state.live_config.load_full();
    assert_eq!(live.multi_tenancy, running, "the running section is kept");
    assert_eq!(
        live.pool.max_connections, 77,
        "the rest of the reload applies"
    );
    let acme = &server.state.tenant_slots.budget_stats()[0];
    assert_eq!((acme.tenant.as_str(), acme.query_limit), ("acme", 2));
}
