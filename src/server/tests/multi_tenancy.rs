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
