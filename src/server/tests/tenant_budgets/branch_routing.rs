//! Branch routing: a `branch=` routing hint or a route plugin's `branch`
//! answer moves the session to a branch database before its statement runs,
//! and the session stays there; a move that cannot be made safely refuses
//! the statement instead of running it on the wrong database.

use super::*;

#[cfg(feature = "routing-hints")]
fn branches_on(c: &mut ProxyConfig) {
    c.routing_hints.enabled = true;
    c.branch.route_enabled = true;
}

/// The database of the newest backend connection.
#[cfg(feature = "routing-hints")]
fn last_database(h: &Harness) -> String {
    h.backend
        .startup_databases()
        .last()
        .cloned()
        .unwrap_or_default()
}

/// A hint moves the session to the branch, later statements stay there, and
/// `branch=` with no name moves it back to where it started.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn a_branch_hint_moves_the_session_and_it_stays() {
    let h = Harness::new(0, None, branches_on).await;
    let mut a = h.connect_db("acme", "app").await;
    assert_served(&a.query("select 1").await, b'I');
    assert_eq!(last_database(&h), "app");

    assert_served(&a.query("/*helios:branch=feat*/ select 2").await, b'I');
    assert_eq!(last_database(&h), "feat");
    let dials = h.backend.startup_databases().len();
    assert_served(&a.query("select 3").await, b'I');
    assert_eq!(
        h.backend.startup_databases().len(),
        dials,
        "it stays on the branch"
    );
    assert_eq!(h.state.metrics.branch_moves.load(Ordering::Relaxed), 1);

    // The extended protocol moves the same way.
    a.send(&[cycle("/*helios:branch=other*/ select 4"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(last_database(&h), "other");

    assert_served(&a.query("/*helios:branch=*/ select 5").await, b'I');
    assert_eq!(last_database(&h), "app");
}

/// On HeliosDB-Nano a branch is `<base>@<name>`.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn nano_branches_are_named_after_their_base() {
    let h = Harness::new(0, None, |c| {
        branches_on(c);
        c.branch.route_mode = crate::config::BranchRouteMode::Nano;
    })
    .await;
    let mut a = h.connect_db("acme", "app").await;
    assert_served(&a.query("/*helios:branch=feat*/ select 1").await, b'I');
    assert_eq!(last_database(&h), "app@feat");
    assert_served(&a.query("/*helios:branch=next*/ select 2").await, b'I');
    assert_eq!(last_database(&h), "app@next");
}

/// A move that cannot be made safely refuses the statement, which runs
/// nowhere: inside a transaction (25001), with session state the new
/// connection would lack (0A000), a branch routing is off for (42000), not
/// allowed (42501), an invalid name (42602), or one `[[hba]]` rejects
/// (28000).
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn an_unsafe_or_unallowed_move_is_refused() {
    let h = Harness::new(0, None, |c| {
        branches_on(c);
        c.branch.route_allow = vec!["feat".into(), "secret".into()];
        c.hba.push(crate::config::HbaRule {
            action: crate::config::HbaAction::Reject,
            user: "all".into(),
            database: "secret".into(),
            address: "all".into(),
        });
    })
    .await;
    let mut a = h.connect_db("acme", "app").await;
    let queries = || h.backend.log().iter().filter(|s| s.tag == b'Q').count();

    assert_served(&a.query("BEGIN").await, b'T');
    let before = queries();
    let refused = a.query("/*helios:branch=feat*/ select 1").await;
    assert_eq!(sqlstate(&refused[0]), "25001");
    assert_eq!(refused[1], wire(b'Z', b"T"));
    assert_eq!(queries(), before, "the refused statement ran nowhere");
    assert_served(&a.query("ROLLBACK").await, b'I');

    for (hint, code) in [
        ("branch=nope", "42501"),
        ("branch=1bad", "42602"),
        ("branch=secret", "28000"),
    ] {
        let refused = a.query(&format!("/*helios:{hint}*/ select 1")).await;
        assert_eq!(sqlstate(&refused[0]), code, "{hint}");
    }
    assert_eq!(last_database(&h), "app");

    assert_served(&a.query("SET search_path = audit").await, b'I');
    let refused = a.query("/*helios:branch=feat*/ select 1").await;
    assert_eq!(sqlstate(&refused[0]), "0A000");
    assert!(has_text(&refused[0], "session settings"), "{refused:?}");
    assert_served(&a.query("DISCARD ALL").await, b'I');
    assert_served(&a.query("/*helios:branch=feat*/ select 2").await, b'I');
    assert_eq!(last_database(&h), "feat");
}

/// With branch routing off, a branch request refuses its statement rather
/// than run it on the session's own database.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn branch_routing_off_refuses_the_statement() {
    let h = Harness::new(0, None, |c| c.routing_hints.enabled = true).await;
    let mut a = h.connect_db("acme", "app").await;
    let refused = a.query("/*helios:branch=feat*/ select 1").await;
    assert_eq!(sqlstate(&refused[0]), "42000");
    assert!(has_text(&refused[0], "route_enabled"), "{refused:?}");
}

/// A route plugin's `branch` answer moves the session too, on both
/// protocols.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_route_plugins_branch_answer_moves_the_session() {
    let plugins = hook_plugin_dir_for(
        "route",
        "'to-branch'",
        r#"{"action": "branch", "target": "feat"}"#,
    );
    let h = Harness::new(0, None, |c| {
        plugins_from(c, &plugins);
        c.branch.route_enabled = true;
    })
    .await;
    let mut a = h.connect_db("acme", "app").await;
    assert_served(&a.query("select 'stay'").await, b'I');
    let databases = || h.backend.startup_databases();
    assert_eq!(databases().last().map(String::as_str), Some("app"));
    assert_served(&a.query("select 'to-branch'").await, b'I');
    assert_eq!(databases().last().map(String::as_str), Some("feat"));

    let mut b = h.connect_db("acme", "app").await;
    b.send(&[cycle("select 'to-branch'"), sync()].concat())
        .await;
    assert_served(&b.until_ready().await, b'I');
    assert_eq!(databases().last().map(String::as_str), Some("feat"));
}
