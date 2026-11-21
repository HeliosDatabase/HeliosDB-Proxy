//! A pre-query plugin's cached result answers an extended-protocol cycle
//! that is one execution it can stand for, before any backend; other shapes
//! run on the backend. An unnamed statement the plugin answered is given to
//! the backend when a later cycle binds it and the plugin no longer answers.

use super::*;

/// The SQL of every `Parse` the backend received.
#[cfg(feature = "wasm-plugins")]
fn parsed(backend: &FakeBackend) -> Vec<String> {
    backend
        .log()
        .into_iter()
        .filter(|s| s.tag == b'P')
        .map(|s| s.text)
        .collect()
}

/// The rows the plugin answers with.
#[cfg(feature = "wasm-plugins")]
const ROWS: &str = r#"{"columns":[{"name":"v"}],"rows":[["one"],["two"]]}"#;

/// A pre-query plugin answering [`ROWS`] for every statement whose context
/// contains `needle`.
#[cfg(feature = "wasm-plugins")]
fn cached_plugin(dir: &std::path::Path, name: &str, needle: &str) -> std::path::PathBuf {
    let data: Vec<String> = ROWS.bytes().map(|b| b.to_string()).collect();
    write_hook_plugin(
        dir,
        name,
        "pre_query",
        needle,
        &format!(r#"{{"action": "cached", "data": [{}]}}"#, data.join(",")),
    )
}

#[cfg(feature = "wasm-plugins")]
fn the_rows(frames: &[Vec<u8>]) -> Vec<String> {
    frames
        .iter()
        .filter(|f| f[0] == b'D')
        .map(|f| {
            // DataRow: count, then (len, bytes) per column; one column here.
            let len = u32::from_be_bytes([f[7], f[8], f[9], f[10]]) as usize;
            String::from_utf8_lossy(&f[11..11 + len]).into_owned()
        })
        .collect()
}

/// Parse/Bind/Describe/Execute/Sync of a statement the plugin answers is
/// answered by the proxy: ParseComplete, BindComplete, the plugin's columns
/// and rows, CommandComplete, ReadyForQuery. No backend sees the cycle. A
/// later cycle binding the unnamed statement again is answered again,
/// without a ParseComplete it did not ask for.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_plugins_cached_result_answers_an_extended_cycle() {
    let dir = tempfile::tempdir().expect("a temp dir");
    cached_plugin(dir.path(), "cache", "'cached'");
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    let mut a = h.connect("acme").await;
    a.send(
        &[
            parse("", "select 'cached'"),
            bind(""),
            describe_portal(),
            execute(),
            sync(),
        ]
        .concat(),
    )
    .await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"12TDDCZ");
    assert_eq!(the_rows(&reply), ["one", "two"]);
    assert!(has_text(&reply[5], "SELECT 2"));
    assert_eq!(reply.last().unwrap(), &wire(b'Z', b"I"));
    assert!(h
        .backend
        .log()
        .iter()
        .all(|s| s.tag != b'P' && s.tag != b'B'));

    a.send(&[bind(""), describe_portal(), execute(), sync()].concat())
        .await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"2TDDCZ");
    assert!(h
        .backend
        .log()
        .iter()
        .all(|s| s.tag != b'P' && s.tag != b'B'));

    // Other statements run on the backend as before.
    a.send(&[cycle("select 'other'"), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(parsed(&h.backend), ["select 'other'"]);
}

/// A named statement prepared earlier, executed by a later cycle the plugin
/// answers at its Bind, is answered too; inside a transaction the reply
/// reports the transaction's status.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_prepared_statements_execution_is_answered_at_its_bind() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    let pm = h.state.plugin_manager.clone().expect("plugins are on");
    let mut a = h.connect("acme").await;
    a.send(&[parse("s1", "select 'cached'"), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    // The plugin arrives after the Parse: it answers the execution.
    pm.load_plugin(&cached_plugin(dir.path(), "cache", "'cached'"))
        .expect("the plugin loads");
    assert_served(&a.query("BEGIN").await, b'T');
    a.send(&[bind("s1"), describe_portal(), execute(), sync()].concat())
        .await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"2TDDCZ");
    assert_eq!(reply.last().unwrap(), &wire(b'Z', b"T"));
    let binds = || h.backend.log().iter().filter(|s| s.tag == b'B').count();
    assert_eq!(binds(), 0);
}

/// Shapes the answer cannot stand for run on the backend: no Describe of
/// the portal (the client would not learn the plugin's columns), a binary
/// result format, a parameter value the Parse-time answer never saw.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn other_cycle_shapes_run_on_the_backend() {
    let dir = tempfile::tempdir().expect("a temp dir");
    cached_plugin(dir.path(), "cache", "'cached'");
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    let mut a = h.connect("acme").await;
    let binary = wire(
        b'B',
        &[cstring(""), cstring(""), vec![0, 0, 0, 0, 0, 1, 0, 1]].concat(),
    );
    let with_param = wire(
        b'B',
        &[
            cstring(""),
            cstring(""),
            vec![0, 0, 0, 1, 0, 0, 0, 1, b'x', 0, 0],
        ]
        .concat(),
    );
    let binds = || h.backend.log().iter().filter(|s| s.tag == b'B').count();
    for (shape, bind_msg, describe) in [
        ("no describe", bind(""), false),
        ("binary result", binary, true),
        ("a parameter", with_param, true),
    ] {
        let before = binds();
        let mut msgs = vec![parse("", "select 'cached'"), bind_msg];
        if describe {
            msgs.push(describe_portal());
        }
        msgs.extend([execute(), sync()]);
        a.send(&msgs.concat()).await;
        a.until_ready().await;
        assert_eq!(binds(), before + 1, "{shape} ran on the backend");
    }
}

/// When the plugin stops answering, a cycle that binds the unnamed
/// statement it answered gives the backend that statement first (its own
/// Parse and Sync, out of band), then runs there; the client sees only its
/// cycle's reply.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_rebind_without_the_plugin_prepares_the_statement_on_the_backend() {
    let dir = tempfile::tempdir().expect("a temp dir");
    cached_plugin(dir.path(), "cache", "'cached'");
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    let pm = h.state.plugin_manager.clone().expect("plugins are on");
    let mut a = h.connect("acme").await;
    a.send(
        &[
            parse("", "select 'cached'"),
            bind(""),
            describe_portal(),
            execute(),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_eq!(tags(&a.until_ready().await), b"12TDDCZ");
    pm.unload_plugin("cache").expect("unloads");

    a.send(&[bind(""), describe_portal(), execute(), sync()].concat())
        .await;
    let reply = a.until_ready().await;
    assert_served(&reply, b'I');
    assert_ne!(
        reply[0][0], b'1',
        "no ParseComplete the client did not ask for"
    );
    assert_eq!(parsed(&h.backend), ["select 'cached'"]);
    // The backend holds it now: the next bind needs no out-of-band Parse.
    a.send(&[bind(""), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(parsed(&h.backend), ["select 'cached'"]);
}
