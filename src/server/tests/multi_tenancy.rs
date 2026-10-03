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
