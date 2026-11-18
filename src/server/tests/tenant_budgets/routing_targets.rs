//! Forced routing targets (a `node=` hint, a route plugin's node, schema
//! routing's analytics node) resolve only to configured nodes: an address a
//! client supplies is refused (42704) and never dialed.
use super::*;

#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn a_hint_naming_an_unconfigured_address_is_refused_not_dialed() {
    // A listener nobody configured: the proxy must never connect to it.
    let stranger = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stranger_addr = stranger.local_addr().unwrap().to_string();
    let standby = FakeBackend::start(None).await;
    let standby_addr = standby.addr.to_string();
    let h = Harness::new(0, None, |c| {
        c.routing_hints.enabled = true;
        c.add_node(&standby_addr, "standby").unwrap();
        c.nodes.last_mut().unwrap().name = Some("pg-standby".into());
    })
    .await;
    let mut a = h.connect("acme").await;

    let hinted = format!("/*helios:node={stranger_addr}*/ select 1");
    assert_refused(&a.query(&hinted).await, "42704", "not a configured node");
    a.send(&[cycle(&hinted), sync()].concat()).await;
    assert_refused(&a.until_ready().await, "42704", "not a configured node");
    let dialed = tokio::time::timeout(Duration::from_millis(200), stranger.accept()).await;
    assert!(
        dialed.is_err(),
        "the proxy connected to an unconfigured address"
    );

    // A configured node, by name, is routed to.
    assert_served(&a.query("/*helios:node=pg-standby*/ select 2").await, b'I');
    assert!(
        standby.log().iter().any(|s| s.tag == b'Q'),
        "{:?}",
        standby.log()
    );
}

#[cfg(feature = "schema-routing")]
#[test]
fn an_unknown_analytics_node_is_a_startup_error() {
    let mut c = test_config();
    c.schema_routing.enabled = true;
    c.schema_routing.analytics_node = "nowhere:5432".into();
    let err = c.validate().unwrap_err().to_string();
    assert!(err.contains("analytics_node"), "{err}");
}
