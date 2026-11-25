//! Contract tests for `GET /api/stats` schema_version 1: the live response
//! against tests/fixtures/api-stats-v1.schema.json, every section present in
//! this build's feature set, the lists' cap, and the bearer gate.

use super::*;
use crate::server::NodeHealth;
use serde_json::Value;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SCHEMA: &str = include_str!("../../../tests/fixtures/api-stats-v1.schema.json");
const EXAMPLE: &str = include_str!("../../../tests/fixtures/api-stats-v1.example.json");

fn schema() -> Value {
    serde_json::from_str(SCHEMA).expect("the v1 schema fixture is JSON")
}

// ---------------------------------------------------------------------------
// A JSON Schema checker for the keywords the v1 fixture uses. No JSON Schema
// crate is a dependency, and the fixture uses nothing else: a keyword the
// checker does not know fails the test instead of being skipped.
// ---------------------------------------------------------------------------

const KEYWORDS: [&str; 16] = [
    "$schema",
    "$id",
    "$defs",
    "$ref",
    "title",
    "description",
    "anyOf",
    "type",
    "const",
    "enum",
    "minimum",
    "format",
    "required",
    "properties",
    "additionalProperties",
    "items",
];

/// The ways `doc` breaks `schema`; empty when it conforms. Strict: an object
/// schema that lists `properties` refuses an instance property it does not
/// list (unless it gives `additionalProperties`), so a field the proxy adds
/// must be documented in the fixture first.
fn schema_errors(schema: &Value, doc: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    check(schema, schema, doc, "$", &mut errors);
    errors
}

fn check(root: &Value, schema: &Value, value: &Value, path: &str, errors: &mut Vec<String>) {
    let rules = schema
        .as_object()
        .unwrap_or_else(|| panic!("the schema at {path} is not an object"));
    for keyword in rules.keys() {
        assert!(
            KEYWORDS.contains(&keyword.as_str()),
            "the checker does not support `{keyword}` (at {path})"
        );
    }
    if let Some(target) = rules.get("$ref") {
        let name = target
            .as_str()
            .and_then(|r| r.strip_prefix("#/$defs/"))
            .unwrap_or_else(|| panic!("unsupported $ref {target} at {path}"));
        let definition = root["$defs"]
            .get(name)
            .unwrap_or_else(|| panic!("unknown $ref {name} at {path}"));
        check(root, definition, value, path, errors);
    }
    if let Some(alternatives) = rules.get("anyOf").and_then(Value::as_array) {
        let mut failures = Vec::new();
        let matched = alternatives.iter().any(|alternative| {
            let mut found = Vec::new();
            check(root, alternative, value, path, &mut found);
            failures.push(found.join("; "));
            found.is_empty()
        });
        if !matched {
            errors.push(format!(
                "{path}: matches no alternative [{}]",
                failures.join(" | ")
            ));
        }
    }
    if let Some(types) = rules.get("type") {
        let types: Vec<&str> = match types {
            Value::String(t) => vec![t.as_str()],
            Value::Array(ts) => ts.iter().filter_map(Value::as_str).collect(),
            other => panic!("unsupported type {other} at {path}"),
        };
        if !types.iter().any(|t| type_matches(t, value)) {
            errors.push(format!("{path}: expected {types:?}, got {value}"));
            return;
        }
    }
    if let Some(expected) = rules.get("const") {
        if value != expected {
            errors.push(format!("{path}: expected {expected}, got {value}"));
        }
    }
    if let Some(allowed) = rules.get("enum").and_then(Value::as_array) {
        if !allowed.contains(value) {
            errors.push(format!("{path}: {value} is not one of {allowed:?}"));
        }
    }
    if let (Some(min), Some(n)) = (rules.get("minimum").and_then(Value::as_f64), value.as_f64()) {
        if n < min {
            errors.push(format!("{path}: {n} is below {min}"));
        }
    }
    if let Some(format) = rules.get("format").and_then(Value::as_str) {
        assert_eq!(format, "date-time", "unsupported format at {path}");
        if let Some(text) = value.as_str() {
            if chrono::DateTime::parse_from_rfc3339(text).is_err() {
                errors.push(format!("{path}: {text:?} is not RFC 3339"));
            }
        }
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = rules.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    errors.push(format!("{path}: missing `{key}`"));
                }
            }
        }
        if let Some(properties) = rules.get("properties").and_then(Value::as_object) {
            for (key, child) in object {
                let child_path = format!("{path}.{key}");
                match (properties.get(key), rules.get("additionalProperties")) {
                    (Some(s), _) => check(root, s, child, &child_path, errors),
                    (None, Some(Value::Bool(true))) => {}
                    (None, Some(s)) if s.is_object() => check(root, s, child, &child_path, errors),
                    (None, _) => errors.push(format!("{child_path}: not in the v1 schema")),
                }
            }
        }
    }
    if let (Some(items), Some(array)) = (rules.get("items"), value.as_array()) {
        for (i, item) in array.iter().enumerate() {
            check(root, items, item, &format!("{path}[{i}]"), errors);
        }
    }
}

fn type_matches(t: &str, value: &Value) -> bool {
    match t {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        "integer" => value.is_u64() || value.is_i64(),
        "number" => value.is_number(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        other => panic!("unknown JSON Schema type {other}"),
    }
}

fn assert_conforms(doc: &Value) {
    let errors = schema_errors(&schema(), doc);
    assert!(errors.is_empty(), "not a v1 document: {errors:#?}");
}

// ---------------------------------------------------------------------------
// States and requests
// ---------------------------------------------------------------------------

async fn stats(state: &Arc<AdminState>) -> Value {
    let (status, body) = AdminServer::route_request("GET", "/api/stats", None, state)
        .await
        .unwrap();
    assert_eq!(status, 200);
    body
}

/// One request through the admin connection handler on a real loopback
/// socket, bearer gate included: the status and the body.
async fn over_http(state: Arc<AdminState>, request: &str) -> (u16, String) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.unwrap();
        AdminServer::handle_connection(stream, peer, state)
            .await
            .unwrap();
    });
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut raw))
        .await
        .expect("the admin response timed out")
        .unwrap();
    server.await.unwrap();
    let text = String::from_utf8(raw).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").expect("an HTTP response");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("a status code");
    (status, body.to_string())
}

/// A query cache with the heatmap on and one entry stored and hit.
#[cfg(feature = "query-cache")]
async fn heatmap_cache() -> Arc<crate::cache::QueryCache> {
    let cache = Arc::new(crate::cache::QueryCache::new(crate::cache::CacheConfig {
        heatmap: crate::cache::HeatmapConfig {
            enabled: true,
            ..Default::default()
        },
        ..Default::default()
    }));
    let ctx = crate::cache::CacheContext::default();
    let sql = "SELECT v FROM orders WHERE id = 1";
    cache
        .put(
            sql,
            &ctx,
            bytes::Bytes::from_static(b"rows"),
            1,
            Duration::ZERO,
        )
        .await;
    assert!(matches!(
        cache.get(sql, &ctx).await,
        crate::cache::CacheLookup::Hit { .. }
    ));
    cache
}

/// An MCP tool cache holding one entry (a stored miss).
#[cfg(feature = "query-cache")]
async fn tool_cache_with_entry() -> Arc<crate::mcp_cache::McpToolCache> {
    let cache = Arc::new(crate::mcp_cache::McpToolCache::new(
        crate::mcp_cache::ToolCacheSettings::from_config(&crate::config::McpConfig::default()),
        Arc::new(crate::cache::TableGenerations::new()),
        Arc::new(crate::sql_text::VolatileScan::new(
            std::iter::empty::<&str>(),
        )),
        None,
        std::iter::empty(),
    ));
    let owner = crate::mcp_cache::Owner::new(Arc::from("agent"), None);
    let (_, outcome) = cache
        .serve(
            &owner,
            crate::mcp::CachedTool::Query,
            "SELECT v FROM t",
            false,
            || async { Ok(("v\n1\n(1 rows)".to_string(), None)) },
        )
        .await;
    assert!(matches!(
        outcome,
        crate::mcp_cache::Outcome::Miss { stored: true, .. }
    ));
    cache
}

/// A state wired as the daemon wires it, with something in every section
/// this build compiles: two nodes (the standby capped and lagging), two
/// tenants with caps and budgets, the HTTP and MCP gateways, rate limiting.
async fn populated_state(stats_max_items: usize) -> Arc<AdminState> {
    let state = Arc::new(AdminState::new());
    let mut config = ProxyConfig::default();
    config.add_node("db-a:5432", "primary").unwrap();
    config.add_node("db-b:5432", "standby").unwrap();
    config.nodes[1].max_connections = 4;
    config.admin.stats_max_items = stats_max_items;
    let mt = &mut config.multi_tenancy;
    mt.enabled = true;
    mt.tenants = vec!["acme".into(), "globex".into()];
    mt.max_clients_per_tenant = 3;
    mt.tenant_max_active_queries.insert("acme".to_string(), 2);
    mt.tenant_max_replay_bytes
        .insert("globex".to_string(), 4096);
    config.rate_limit.enabled = true;
    config.http_gateway.enabled = true;
    config.mcp.enabled = true;

    state
        .with_node_slots(Arc::new(crate::node_slots::NodeSlots::from_config(&config)))
        .await;
    state
        .with_tenant_slots(Arc::new(crate::tenant_slots::TenantSlots::from_config(
            &config,
        )))
        .await;
    state
        .with_gateway_pools(Arc::new(crate::gateway_pool::GatewayPools::new(2)))
        .await;
    state
        .with_latency(Arc::new(crate::metrics_hist::ProxyLatency::new(&[
            1.0, 10.0,
        ])))
        .await;
    {
        let now = chrono::Utc::now();
        let mut health = state.node_health.write().await;
        for (address, lag) in [("db-a:5432", None), ("db-b:5432", Some(1024))] {
            health.insert(
                address.to_string(),
                NodeHealth {
                    address: address.to_string(),
                    healthy: true,
                    last_check: now,
                    failure_count: 0,
                    success_count: 3,
                    last_error: None,
                    latency_ms: 0.5,
                    replication_lag_bytes: lag,
                    lag_sampled_at: lag.map(|_| now),
                },
            );
        }
    }
    {
        let mut m = state.metrics.write().await;
        m.connections_accepted = 5;
        m.connections_closed = 2;
        m.queries_processed = 7;
        m.rate_limit_concurrency_refused = 4;
        m.journal_committed = 3;
        m.tr.failovers = 1;
    }
    *state.active_sessions.write().await = 3;
    #[cfg(feature = "pool-modes")]
    state
        .with_backend_pool(Arc::new(crate::pool::BackendIdlePool::new(2, 8)))
        .await;
    #[cfg(feature = "circuit-breaker")]
    {
        let breakers = Arc::new(crate::circuit_breaker::CircuitBreakerManager::with_defaults());
        breakers.force_open("db-b:5432", None);
        state.with_circuit_breaker(breakers).await;
    }
    #[cfg(feature = "query-cache")]
    {
        state.with_query_cache(heatmap_cache().await).await;
        state.with_mcp_cache(tool_cache_with_entry().await).await;
    }
    // The node snapshot `/topology` reads, as the daemon fills it.
    state.config_snapshot.write().await.nodes = config
        .nodes
        .iter()
        .map(|n| crate::admin::NodeSnapshot {
            address: n.address().to_string(),
            role: format!("{:?}", n.role),
            weight: n.weight,
            enabled: n.enabled,
        })
        .collect();
    state.set_proxy_config(config).await;
    state
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// With nothing attached every section is still present: those of a
/// compiled-out or unconfigured feature as `{"enabled": false, "reason"}`
/// and nothing else. Runs in every feature set the gates build.
#[tokio::test]
async fn every_section_is_present_on_a_bare_state() {
    let state = Arc::new(AdminState::new());
    let doc = stats(&state).await;
    assert_conforms(&doc);
    assert_eq!(doc["schema_version"], SCHEMA_VERSION);
    assert_eq!(doc["proxy_version"], crate::VERSION);
    assert!(doc["uptime_secs"].as_u64().unwrap() < 3600);
    for section in SECTIONS {
        let body = doc[section]
            .as_object()
            .unwrap_or_else(|| panic!("section {section} is missing"));
        let enabled = body["enabled"]
            .as_bool()
            .unwrap_or_else(|| panic!("{section}.enabled is not a bool"));
        if !enabled {
            assert!(body["reason"].is_string(), "{section}");
            assert_eq!(body.len(), 2, "{section} carries only enabled and reason");
        }
    }
    let on = |section: &str| doc[section]["enabled"] == true;
    let reason = |section: &str| doc[section]["reason"].as_str().unwrap_or("").to_string();
    assert!(on("nodes") && on("sessions") && on("traffic") && on("gateways"));
    assert!(on("transaction_replay"), "tr_enabled defaults to true");
    assert_eq!(on("pools"), cfg!(feature = "pool-modes"));
    assert_eq!(
        reason("cache"),
        if cfg!(feature = "query-cache") {
            "the query cache is off ([cache] enabled = false)"
        } else {
            "query-cache feature not compiled in"
        }
    );
    assert_eq!(reason("tenants"), "[multi_tenancy] enabled = false");
    assert_eq!(
        reason("rate_limit"),
        if cfg!(feature = "rate-limiting") {
            "[rate_limit] enabled = false"
        } else {
            "rate-limiting feature not compiled in"
        }
    );
    for gateway in ["http", "graphql", "mcp"] {
        assert_eq!(doc["gateways"][gateway]["enabled"], false, "{gateway}");
    }
    assert_eq!(doc["nodes"]["count"], 0);
    assert_eq!(doc["nodes"]["items"], serde_json::json!([]));
    assert_eq!(doc["nodes"]["truncated"], false);
    assert!(doc["nodes"]["write_leader"].is_null());
}

/// Every attached subsystem reaches its section, from the same snapshots
/// `/metrics` and the other endpoints read.
#[tokio::test]
async fn attached_subsystems_fill_their_sections() {
    let state = populated_state(1000).await;
    let doc = stats(&state).await;
    assert_conforms(&doc);

    let nodes = &doc["nodes"];
    assert_eq!(nodes["count"], 2);
    assert_eq!(nodes["healthy_count"], 2);
    assert_eq!(nodes["truncated"], false);
    assert_eq!(nodes["write_leader"], "db-a:5432");
    assert_eq!(
        nodes["load_balancer"],
        serde_json::to_value(ProxyConfig::default().load_balancer.read_strategy).unwrap()
    );
    assert_eq!(
        nodes["reconnects"]["limit_per_node"],
        ProxyConfig::default()
            .limits
            .max_concurrent_reconnects_per_node
    );
    let (a, b) = (&nodes["items"][0], &nodes["items"][1]);
    assert_eq!(a["address"], "db-a:5432");
    assert_eq!(a["role"], "primary");
    assert_eq!(a["write_leader"], true);
    assert!(a["lag_bytes"].is_null() && a["lag_sampled_at"].is_null());
    assert!(a["backend_connections"].is_null(), "an uncapped node");
    assert_eq!(b["role"], "standby");
    assert_eq!(b["write_leader"], false);
    assert_eq!(b["lag_bytes"], 1024);
    assert!(b["lag_sampled_at"].is_string());
    assert_eq!(b["backend_connections"]["limit"], 4);
    assert_eq!(b["backend_connections"]["in_use"], 0);
    if cfg!(feature = "circuit-breaker") {
        assert_eq!(a["circuit"], "closed");
        assert_eq!(b["circuit"], "open");
    } else {
        assert!(b["circuit"].is_null());
    }

    let sessions = &doc["sessions"];
    assert_eq!(sessions["active"], 3);
    assert_eq!(sessions["connections_open"], 3);
    assert_eq!(sessions["accepted_total"], 5);
    assert_eq!(sessions["closed_total"], 2);
    assert_eq!(doc["traffic"]["queries_total"], 7);

    let tenants = &doc["tenants"];
    assert_eq!(tenants["enabled"], true);
    assert_eq!(tenants["count"], 2);
    let (acme, globex) = (&tenants["items"][0], &tenants["items"][1]);
    assert_eq!(acme["tenant"], "acme");
    assert_eq!(acme["sessions"]["limit"], 3);
    assert_eq!(acme["queries"]["limit"], 2);
    assert!(acme["replay"].is_null());
    assert_eq!(globex["tenant"], "globex");
    assert!(globex["queries"].is_null());
    assert_eq!(globex["replay"]["byte_limit"], 4096);

    if cfg!(feature = "rate-limiting") {
        assert_eq!(doc["rate_limit"]["enabled"], true);
        assert_eq!(doc["rate_limit"]["key_by"], "user");
        assert_eq!(doc["rate_limit"]["concurrency_refused_total"], 4);
    } else {
        assert_eq!(doc["rate_limit"]["enabled"], false);
    }

    let tr = &doc["transaction_replay"];
    assert_eq!(tr["mode"], "session");
    assert_eq!(tr["failovers_total"], 1);
    assert_eq!(tr["journal"]["committed_total"], 3);
    let refusals = tr["refusals_total"].as_object().unwrap();
    assert_eq!(
        refusals.len(),
        crate::metrics_hist::ReplayRefusal::ALL.len()
    );
    assert!(refusals.values().all(|n| n == 0));

    let gateways = &doc["gateways"];
    assert_eq!(gateways["http"]["enabled"], true);
    assert_eq!(gateways["http"]["pool"]["idle"], 0);
    assert!(gateways["http"]["results_refused_total"].is_u64());
    assert_eq!(
        gateways["http"]["write_invalidations_total"].is_u64(),
        cfg!(feature = "query-cache")
    );
    assert_eq!(gateways["graphql"]["enabled"], false);
    let mcp = &gateways["mcp"];
    assert_eq!(mcp["enabled"], true);
    assert!(mcp["results_truncated_total"].is_u64());

    #[cfg(feature = "query-cache")]
    {
        let tool = &mcp["tool_cache"];
        assert_eq!(tool["enabled"], true);
        assert_eq!(tool["entries"], 1);
        assert_eq!(tool["owners"], 1);
        assert_eq!(tool["stores_total"], 1);
        assert_eq!(tool["misses_total"], 1);
        assert_eq!(tool["hits_total"], 0);
        assert!(tool["bytes"].as_u64().unwrap() > 0);

        let cache = &doc["cache"];
        assert_eq!(cache["enabled"], true);
        assert_eq!(cache["puts_total"], 1);
        let hits: u64 = ["l1", "l2", "l3"]
            .iter()
            .map(|t| cache["tiers"][t]["hits_total"].as_u64().unwrap())
            .sum();
        assert_eq!(hits, 1);
        assert_eq!(cache["heatmap"]["enabled"], true);
        assert!(cache["heatmap"]["tables_tracked"].as_u64().unwrap() >= 1);
        assert!(
            cache["heatmap"].get("tables").is_none(),
            "no per-table data"
        );
    }
    #[cfg(not(feature = "query-cache"))]
    assert_eq!(
        mcp["tool_cache"]["reason"],
        "query-cache feature not compiled in"
    );

    #[cfg(feature = "pool-modes")]
    {
        let pools = &doc["pools"];
        assert_eq!(pools["mode"], "session");
        assert_eq!(pools["idle_pool"]["idle"], 0);
        assert_eq!(
            pools["max_total_idle"],
            ProxyConfig::default().limits.max_total_idle_backend_conns
        );
    }
}

/// A list longer than `[admin] stats_max_items` is cut after sorting and
/// says so; the counts are taken before the cut.
#[tokio::test]
async fn lists_are_cut_at_stats_max_items() {
    let state = populated_state(1).await;
    let doc = stats(&state).await;
    assert_conforms(&doc);
    let nodes = &doc["nodes"];
    assert_eq!(nodes["count"], 2);
    assert_eq!(nodes["healthy_count"], 2);
    assert_eq!(nodes["truncated"], true);
    assert_eq!(nodes["items"].as_array().unwrap().len(), 1);
    assert_eq!(nodes["items"][0]["address"], "db-a:5432");
    let tenants = &doc["tenants"];
    assert_eq!(tenants["count"], 2);
    assert_eq!(tenants["truncated"], true);
    assert_eq!(tenants["items"].as_array().unwrap().len(), 1);
    assert_eq!(tenants["items"][0]["tenant"], "acme");
}

/// A node the proxy knows only from health (added after startup) is
/// listed, without the configured fields.
#[tokio::test]
async fn a_node_known_only_from_health_is_listed() {
    let state = populated_state(1000).await;
    {
        let mut health = state.node_health.write().await;
        let mut extra = health["db-a:5432"].clone();
        extra.address = "db-c:5432".to_string();
        extra.healthy = false;
        health.insert(extra.address.clone(), extra);
    }
    let doc = stats(&state).await;
    assert_conforms(&doc);
    assert_eq!(doc["nodes"]["count"], 3);
    assert_eq!(doc["nodes"]["healthy_count"], 2);
    let c = &doc["nodes"]["items"][2];
    assert_eq!(c["address"], "db-c:5432");
    assert!(c["role"].is_null() && c["enabled"].is_null() && c["weight"].is_null());
    assert_eq!(c["healthy"], false);
}

/// The live response over a real socket conforms to the v1 fixture, and the
/// route is bearer-gated like every other `/api` route.
#[tokio::test]
async fn live_response_conforms_and_needs_the_admin_token() {
    for state in [Arc::new(AdminState::new()), populated_state(1000).await] {
        state.with_auth_token(Some("s3cret".to_string())).await;
        for request in [
            "GET /api/stats HTTP/1.1\r\nHost: proxy\r\n\r\n",
            "GET /api/stats HTTP/1.1\r\nAuthorization: Bearer wrong\r\n\r\n",
        ] {
            let (status, _) = over_http(state.clone(), request).await;
            assert_eq!(status, 401, "{request:?}");
        }
        for path in ["/api/stats", "/api/stats?pretty=1"] {
            let request = format!("GET {path} HTTP/1.1\r\nAuthorization: Bearer s3cret\r\n\r\n");
            let (status, body) = over_http(state.clone(), &request).await;
            assert_eq!(status, 200, "{path}");
            let doc: Value = serde_json::from_str(&body).unwrap();
            assert_conforms(&doc);
        }
    }
}

/// The fixture lists exactly the document's top-level keys, its example
/// conforms, and the checker catches each kind of drift.
#[test]
fn the_fixture_and_its_checker_catch_drift() {
    let schema = schema();
    let required: Vec<&str> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let mut expected = vec![
        "schema_version",
        "proxy_version",
        "generated_at",
        "uptime_secs",
    ];
    expected.extend(SECTIONS);
    assert_eq!(required, expected);

    let example: Value = serde_json::from_str(EXAMPLE).expect("the example fixture is JSON");
    let errors = schema_errors(&schema, &example);
    assert!(errors.is_empty(), "{errors:#?}");

    type Mutation = fn(&mut Value);
    let drift: [(&str, Mutation); 7] = [
        ("a missing section", |d| {
            d.as_object_mut().unwrap().remove("cache");
        }),
        ("an undocumented field", |d| d["nodes"]["bogus"] = 1.into()),
        ("another schema_version", |d| d["schema_version"] = 2.into()),
        ("a disabled section with data", |d| {
            d["gateways"]["graphql"]["pool"] = Value::Null
        }),
        ("an unknown role", |d| {
            d["nodes"]["items"][0]["role"] = "leader".into()
        }),
        ("a counter as a string", |d| {
            d["traffic"]["queries_total"] = "7".into()
        }),
        ("a timestamp that is not RFC 3339", |d| {
            d["generated_at"] = "yesterday".into()
        }),
    ];
    for (what, mutate) in drift {
        let mut doc = example.clone();
        mutate(&mut doc);
        assert!(
            !schema_errors(&schema, &doc).is_empty(),
            "{what} passed the checker"
        );
    }
}
