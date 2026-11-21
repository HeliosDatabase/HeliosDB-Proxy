//! Existing-API regressions for the gateway's explicit cache execution contract.
use super::*;

fn config(trusted: bool) -> McpConfig {
    let cfg = McpConfig {
        enabled: true,
        tool_cache_enabled: true,
        read_only: false,
        backend_port: 0,
        ..Default::default()
    };
    let mut value = toml::Value::try_from(cfg).unwrap();
    value.as_table_mut().unwrap().insert(
        "trust_backend_read_effects".into(),
        toml::Value::Boolean(trusted),
    );
    value.try_into().unwrap()
}

fn pool() -> crate::gateway_pool::SharedBackendPool {
    Arc::new(crate::gateway_pool::BackendClientPool::new(0))
}

fn cache(cfg: &McpConfig) -> Arc<crate::mcp_cache::McpToolCache> {
    let config = crate::config::ProxyConfig {
        mcp: cfg.clone(),
        ..Default::default()
    };
    Arc::new(crate::mcp_cache::McpToolCache::from_config(
        &config,
        Arc::new(crate::cache::generations::TableGenerations::new()),
        Arc::new(crate::sql_text::VolatileScan::new(
            std::iter::empty::<&str>(),
        )),
        None,
    ))
}

async fn fill(cache: &crate::mcp_cache::McpToolCache, principal: &Arc<str>) {
    let owner = crate::mcp_cache::Owner::new(principal.clone(), None);
    let (result, outcome) = cache
        .serve(
            &owner,
            CachedTool::Query,
            "SELECT value FROM widgets",
            false,
            || async { Ok(("prefilled contract sentinel".into(), None)) },
        )
        .await;
    assert!(result.is_ok());
    assert!(matches!(
        outcome,
        crate::mcp_cache::Outcome::Miss { stored: true, .. }
    ));
}

#[test]
fn cache_effects_contract_mcp_manual_attachment_requires_vouch() {
    for trusted in [false, true] {
        let cfg = config(trusted);
        let cache = cache(&cfg);
        let shared = McpServer::new(cfg, None, pool())
            .with_tool_cache(Some(cache))
            .into_shared();
        assert_eq!(shared.cache.is_some(), trusted);
    }
}

#[tokio::test]
async fn cache_effects_contract_mcp_run_tool_checks_vouch_after_manual_injection() {
    for trusted in [false, true] {
        let cfg = config(trusted);
        let cache = cache(&cfg);
        let principal: Arc<str> = Arc::from(MCP_DEFAULT_PRINCIPAL);
        fill(&cache, &principal).await;
        let pool = pool();
        let policy = GatewayPolicy::default();
        let gateway = Gateway {
            pool: &pool,
            policy: &policy,
            peer: None,
            tenant: None,
            principal: &principal,
            cache: Some(&cache),
            memory: None,
        };
        let (result, meta) = McpServer::run_tool(
            &cfg,
            &gateway,
            "SELECT value FROM widgets",
            false,
            CachedTool::Query,
            false,
        )
        .await;
        if trusted {
            assert_eq!(result.unwrap().0, "prefilled contract sentinel");
            assert_eq!(meta.unwrap()[crate::mcp_cache::META_KEY]["hit"], true);
        } else {
            assert!(
                result.unwrap_err().contains("backend connect"),
                "native forwarding must be attempted"
            );
            assert!(meta.is_none(), "an unvouched cache is not consulted");
        }
    }
}

#[tokio::test]
async fn cache_effects_contract_mcp_advertisement_checks_vouch_after_manual_injection() {
    for trusted in [false, true] {
        let cfg = config(trusted);
        let cache = cache(&cfg);
        let pool = pool();
        let policy = GatewayPolicy::default();
        let principal: Arc<str> = Arc::from(MCP_DEFAULT_PRINCIPAL);
        let gateway = Gateway {
            pool: &pool,
            policy: &policy,
            peer: None,
            tenant: None,
            principal: &principal,
            cache: Some(&cache),
            memory: None,
        };
        let request = json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}).to_string();
        let result = McpServer::dispatch_with(
            &request,
            &cfg,
            None,
            &gateway,
            &tokio::sync::Semaphore::new(1),
        )
        .await
        .unwrap();
        let tools = result["result"]["tools"].as_array().unwrap();
        for name in ["query", "list_tables"] {
            let tool = tools.iter().find(|t| t["name"] == name).unwrap();
            assert_eq!(
                tool["inputSchema"]["properties"].get("fresh").is_some(),
                trusted,
                "{name}"
            );
        }
    }
}

#[tokio::test]
async fn cache_effects_contract_mcp_list_tables_checks_final_call_policy() {
    let cfg = config(true);
    let cache = cache(&cfg);
    let owner = crate::mcp_cache::Owner::new(Arc::from(MCP_DEFAULT_PRINCIPAL), None);
    for sql in [
        "SELECT fixture_writer() FROM widgets",
        "SELECT abs(1) FROM widgets",
    ] {
        let (result, outcome) = cache
            .serve(&owner, CachedTool::ListTables, sql, false, || async {
                Ok(("native result".into(), None))
            })
            .await;
        assert!(
            result.is_ok(),
            "ordinary native execution remains available"
        );
        assert!(
            matches!(
                outcome,
                crate::mcp_cache::Outcome::Miss { stored: false, .. }
            ),
            "{sql}: {outcome:?}"
        );
    }
}

#[tokio::test]
async fn cache_effects_contract_mcp_known_catalog_retains_a_positive_hit() {
    let cfg = config(true);
    let cache = cache(&cfg);
    let owner = crate::mcp_cache::Owner::new(Arc::from(MCP_DEFAULT_PRINCIPAL), None);
    let (_, first) = cache
        .serve(
            &owner,
            CachedTool::ListTables,
            LIST_TABLES_SQL,
            false,
            || async { Ok(("known native catalog fixture".into(), None)) },
        )
        .await;
    assert!(matches!(
        first,
        crate::mcp_cache::Outcome::Miss { stored: true, .. }
    ));
    let (result, second) = cache
        .serve(
            &owner,
            CachedTool::ListTables,
            LIST_TABLES_SQL,
            false,
            || async { panic!("known safe catalog entry must be served from cache") },
        )
        .await;
    assert!(matches!(second, crate::mcp_cache::Outcome::Hit { .. }));
    assert_eq!(result.unwrap().0, "known native catalog fixture");
}

#[tokio::test]
async fn cache_effects_contract_mcp_query_keeps_direct_call_and_postfill_guards() {
    let cfg = config(true);
    let cache = cache(&cfg);
    let owner = crate::mcp_cache::Owner::new(Arc::from(MCP_DEFAULT_PRINCIPAL), None);
    for _ in 0..2 {
        let (result, outcome) = cache
            .serve(
                &owner,
                CachedTool::Query,
                "SELECT fixture_writer() FROM widgets",
                false,
                || async { Ok(("native function result".into(), None)) },
            )
            .await;
        assert!(result.is_ok());
        assert!(matches!(
            outcome,
            crate::mcp_cache::Outcome::Miss { stored: false, .. }
        ));
    }
    assert_eq!(cache.stats_json()["stores"], 0);
    assert_eq!(cache.stats_json()["entries"], 0);
}

#[tokio::test]
async fn cache_effects_contract_mcp_default_fresh_never_consults_or_refills() {
    let cfg = config(false);
    let cache = cache(&cfg);
    let principal: Arc<str> = Arc::from(MCP_DEFAULT_PRINCIPAL);
    fill(&cache, &principal).await;
    let before = cache.stats_json();
    let pool = pool();
    let policy = GatewayPolicy::default();
    let gateway = Gateway {
        pool: &pool,
        policy: &policy,
        peer: None,
        tenant: None,
        principal: &principal,
        cache: Some(&cache),
        memory: None,
    };
    let (result, metadata) = McpServer::run_tool(
        &cfg,
        &gateway,
        "SELECT value FROM widgets",
        false,
        CachedTool::Query,
        true,
    )
    .await;
    assert!(result.unwrap_err().contains("backend connect"));
    assert!(metadata.is_none());
    assert_eq!(
        cache.stats_json(),
        before,
        "no lookup, miss accounting or fill is allowed"
    );
}
