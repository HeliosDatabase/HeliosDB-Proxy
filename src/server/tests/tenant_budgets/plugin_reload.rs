//! The plugin directory, reconciled by the daemon: hot reload applies a
//! plugin dropped in after startup and ends it when its file goes, and a
//! new version replaces the running one only once it has loaded.

use super::*;

/// Send `select 1` until `done` accepts the reply, or fail after ten
/// seconds.
#[cfg(feature = "wasm-plugins")]
async fn until_reply(c: &mut Client, what: &str, done: impl Fn(&[Vec<u8>]) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let reply = c.query("select 1").await;
        if done(&reply) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: never happened (last reply {reply:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(feature = "wasm-plugins")]
fn blocked(reply: &[Vec<u8>], reason: &str) -> bool {
    reply
        .first()
        .is_some_and(|f| f[0] == b'E' && sqlstate(f) == "42000" && has_text(f, reason))
}

#[cfg(feature = "wasm-plugins")]
fn served(reply: &[Vec<u8>]) -> bool {
    reply.iter().all(|f| f[0] != b'E') && reply.last() == Some(&wire(b'Z', b"I"))
}

/// With `hot_reload`, the watcher task loads a plugin written after startup,
/// replaces it with a new version, keeps it when a newer version is broken,
/// and unloads it when its files go — all without a restart.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn hot_reload_follows_the_plugin_directory() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let h = Harness::new(0, None, |c| {
        plugins_from(c, &dir);
        c.plugins.hot_reload = true;
        c.plugins.hot_reload_interval_ms = 100;
        c.plugins.hot_reload_debounce_ms = 0;
    })
    .await;
    let watcher = h.server.spawn_plugin_watcher().expect("hot reload is on");
    let mut a = h.connect("acme").await;
    assert_served(&a.query("select 1").await, b'I');

    let wasm = write_hook_plugin(
        dir.path(),
        "guard",
        "pre_query",
        "",
        r#"{"action": "block", "value": "first guard"}"#,
    );
    until_reply(&mut a, "the new plugin", |r| blocked(r, "first guard")).await;

    write_hook_plugin(
        dir.path(),
        "guard",
        "pre_query",
        "",
        r#"{"action": "block", "value": "second guard"}"#,
    );
    until_reply(&mut a, "the new version", |r| blocked(r, "second guard")).await;

    // A broken version: the running one stays.
    std::fs::write(&wasm, b"\x00asm\x01\x00\x00\x00broken").unwrap();
    let pm = h.state.plugin_manager.clone().expect("plugins are on");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !pm
        .files()
        .iter()
        .any(|f| matches!(f.state, crate::plugins::PluginFileState::Stale { .. }))
    {
        assert!(tokio::time::Instant::now() < deadline, "{:?}", pm.files());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(blocked(&a.query("select 1").await, "second guard"));

    std::fs::remove_file(&wasm).unwrap();
    std::fs::remove_file(wasm.with_extension("json")).unwrap();
    until_reply(&mut a, "the removal", served).await;

    h.server.shutdown();
    tokio::time::timeout(Duration::from_secs(5), watcher)
        .await
        .expect("the watcher stops at shutdown")
        .expect("the watcher task ends cleanly");
}

/// Without `hot_reload` there is no watcher: the directory changes only on
/// an explicit rescan (`POST /plugins/reload`, SIGHUP).
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn without_hot_reload_a_rescan_is_explicit() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    assert!(h.server.spawn_plugin_watcher().is_none());
    let mut a = h.connect("acme").await;
    write_hook_plugin(
        dir.path(),
        "guard",
        "pre_query",
        "",
        r#"{"action": "block", "value": "guarded"}"#,
    );
    assert_served(&a.query("select 1").await, b'I');

    let admin = Arc::new(crate::admin::AdminState::new());
    admin
        .with_plugin_manager(h.state.plugin_manager.clone().expect("plugins are on"))
        .await;
    let (status, body) =
        crate::admin::AdminServer::route_request("POST", "/plugins/reload", None, &admin)
            .await
            .expect("the endpoint answers");
    assert_eq!(status, 200);
    assert_eq!(body["loaded"], serde_json::json!(["guard"]));
    assert!(blocked(&a.query("select 1").await, "guarded"));

    let (status, files) =
        crate::admin::AdminServer::route_request("GET", "/plugins/files", None, &admin)
            .await
            .expect("the endpoint answers");
    assert_eq!(status, 200);
    assert_eq!(files[0]["state"], "loaded");
    assert_eq!(files[0]["plugin"], "guard");

    let (_, listed) = crate::admin::AdminServer::route_request("GET", "/plugins", None, &admin)
        .await
        .expect("the endpoint answers");
    assert!(listed[0]["path"].as_str().unwrap().ends_with("guard.wasm"));
    assert_eq!(listed[0]["sha256"].as_str().unwrap().len(), 64);
}
