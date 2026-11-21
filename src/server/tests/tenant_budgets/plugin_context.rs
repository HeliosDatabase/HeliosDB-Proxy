//! What a plugin hook call carries about its session (`hook_context`): the
//! first-party plugins key their decisions off these attributes (cost
//! governor: `tenant_id`; column mask: `identity.roles`; residency router:
//! `helios.region`; token budget: `agent_id` / `model_id`; audit chain:
//! `client_ip`), which the proxy used to leave empty.
use super::*;

#[cfg(feature = "wasm-plugins")]
fn startup_with(params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, v) in params {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = ((8 + body.len()) as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&196608u32.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// A plugin that blocks on its session's tenant sees it: the tenant's
/// statements are blocked, another tenant's are served. One that blocks on
/// a `helios.*` setting passed in `options` sees that too.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn plugins_see_the_sessions_tenant_and_helios_settings() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_hook_plugin(
        dir.path(),
        "tenant-gate",
        "pre_query",
        r#""tenant_id":"acme""#,
        r#"{"action": "block", "value": "acme is paused"}"#,
    );
    write_hook_plugin(
        dir.path(),
        "region-gate",
        "pre_query",
        r#""helios.region":"eu-west""#,
        r#"{"action": "block", "value": "eu-west is read-only"}"#,
    );
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;

    let mut acme = h.connect("acme").await;
    assert_refused(
        &acme.query("select 1").await,
        "42000",
        "Query blocked by plugin: acme is paused",
    );
    let mut other = h.connect("other").await;
    assert_served(&other.query("select 1").await, b'I');

    let mut eu = h
        .connect_with(startup_with(&[
            ("user", "other"),
            ("options", "-c helios.region=eu-west"),
        ]))
        .await;
    assert_refused(
        &eu.query("select 2").await,
        "42000",
        "Query blocked by plugin: eu-west is read-only",
    );
}

#[cfg(feature = "wasm-plugins")]
#[test]
fn the_session_context_carries_identity_tenant_and_settings() {
    let params: HashMap<String, String> = [
        ("user", "app"),
        ("database", "shop@feature-x"),
        ("application_name", "agent-gpt"),
        ("helios.agent_id", "shopper"),
        (
            "options",
            r"-c helios.model_id=claude --helios.region=eu -c work_mem=4MB -c helios.note=a\ b",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let identity = crate::plugins::Identity {
        user_id: "u-1".into(),
        username: "alice".into(),
        roles: vec!["analyst".into(), "pii".into()],
        tenant_id: Some("acme".into()),
        claims: HashMap::new(),
    };
    let ctx =
        ProxyServer::plugin_session_context(&params, Some(&identity), None, "10.1.2.3", "s-1");
    assert_eq!(ctx.identity.as_deref(), Some("alice"));
    assert_eq!(ctx.client_id.as_deref(), Some("s-1"));
    assert_eq!(ctx.database.as_deref(), Some("shop"));
    assert_eq!(ctx.branch.as_deref(), Some("feature-x"));
    let a = &ctx.attributes;
    for (key, value) in [
        ("client_ip", "10.1.2.3"),
        ("user", "app"),
        ("application_name", "agent-gpt"),
        ("tenant_id", "acme"),
        ("identity.user_id", "u-1"),
        ("identity.roles", "analyst,pii"),
        ("helios.agent_id", "shopper"),
        ("agent_id", "shopper"),
        ("helios.model_id", "claude"),
        ("model_id", "claude"),
        ("helios.region", "eu"),
        ("helios.note", "a b"),
    ] {
        assert_eq!(a.get(key).map(String::as_str), Some(value), "{key}");
    }
    assert!(!a.contains_key("work_mem"), "only helios.* settings");

    // A configured tenant wins over the identity's; no identity: the user.
    let plain = ProxyServer::plugin_session_context(&params, None, Some("globex"), "::1", "s-2");
    assert_eq!(plain.identity.as_deref(), Some("app"));
    assert_eq!(
        plain.attributes.get("tenant_id").map(String::as_str),
        Some("globex")
    );
    assert!(!plain.attributes.contains_key("identity.roles"));
}
