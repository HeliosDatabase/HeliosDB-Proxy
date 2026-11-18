//! Statement hooks on the extended protocol (`ext_hooks`): what a simple
//! Query passes (plugin hooks, rewrite rules, tenant row filter, schema
//! routing), a `Parse`/`Bind`/`Execute` cycle passes too. Every test here
//! fails when the hooks run on simple Queries only.
use super::*;

/// The SQL of every `Parse` the backend received.
fn parsed(backend: &FakeBackend) -> Vec<String> {
    backend
        .log()
        .into_iter()
        .filter(|s| s.tag == b'P')
        .map(|s| s.text)
        .collect()
}

/// Nothing of a statement reached the backend.
fn assert_backend_untouched(backend: &FakeBackend, shape: &str) {
    let seen = backend.log();
    assert!(
        seen.iter()
            .all(|s| !matches!(s.tag, b'P' | b'B' | b'E' | b'Q')),
        "{shape}: {seen:?}"
    );
}

/// Row isolation on the table `t` by the column `tid`.
fn tenant_table(c: &mut ProxyConfig) {
    c.multi_tenancy.tenant_tables = vec!["t".into()];
    c.multi_tenancy.tenant_column = "tid".into();
}

/// The tenant's row filter is injected into every `Parse` that names a
/// tenant table, unnamed or named, and the statement registered under a name
/// is the filtered one: its later executions run it, on any connection.
#[tokio::test]
async fn the_tenant_row_filter_is_injected_into_a_parse() {
    for promote in [true, false] {
        let shape = format!("promotion {promote}");
        let h = Harness::new(0, None, |c| {
            tenant_table(c);
            c.optimize_unnamed_parse = promote;
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&[cycle("select v from t where id = 1"), sync()].concat())
            .await;
        assert_served(&a.until_ready().await, b'I');
        a.send(&[parse("s1", "select v from t where id = 2"), sync()].concat())
            .await;
        assert_served(&a.until_ready().await, b'I');
        a.send(&[bind("s1"), execute(), sync()].concat()).await;
        assert_served(&a.until_ready().await, b'I');
        // A table that is not a tenant table is forwarded as it came.
        a.send(&[cycle("select 1 from plain"), sync()].concat())
            .await;
        assert_served(&a.until_ready().await, b'I');

        let seen = parsed(&h.backend);
        assert_eq!(seen.len(), 3, "{shape}: {seen:?}");
        for sql in &seen[..2] {
            assert!(
                sql.contains("tid") && sql.contains("acme"),
                "{shape}: no tenant filter in {sql:?}"
            );
        }
        assert_eq!(seen[2], "select 1 from plain", "{shape}");
    }
}

/// A session with no identified tenant may not name a tenant table
/// (`unknown_tenant = "refuse"`, the default), whichever protocol it uses:
/// the cycle is refused 42501 at the `Parse`, everything the proxy held of
/// it is dropped (the statement before it in the same batch included), the
/// rest is discarded to the Sync, and the session goes on. The refused
/// statement is not registered: a later Bind of its name finds none.
#[tokio::test]
async fn a_refused_tenant_statement_refuses_its_cycle() {
    let h = Harness::new(0, None, tenant_table).await;
    let mut n = h.connect("nobody").await;

    // One packet, as libpq sends it.
    n.send(&[cycle("select v from t"), sync()].concat()).await;
    assert_refused(&n.until_ready().await, "42501", "no identified tenant");

    // Behind another statement of the same batch: neither runs.
    n.send(
        &[
            cycle("insert into plain values (1)"),
            cycle("select v from t"),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_refused(&n.until_ready().await, "42501", "no identified tenant");

    // A prepare flushed on its own (asyncpg): the error comes at once, the
    // ReadyForQuery at the Sync, and what precedes it is discarded.
    n.send(
        &[
            parse("s1", "select v from t"),
            describe_statement("s1"),
            flush(),
        ]
        .concat(),
    )
    .await;
    assert_eq!(sqlstate(&n.frame().await), "42501");
    n.quiet().await;
    n.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_eq!(n.frame().await, wire(b'Z', b"I"));
    n.quiet().await;
    assert_backend_untouched(&h.backend, "refused cycles");

    // The name was never registered, so nothing re-prepares it.
    n.send(&[bind("s1"), execute(), sync()].concat()).await;
    let reply = n.until_ready().await;
    assert_eq!(tags(&reply), b"EZ", "{reply:?}");
    assert_eq!(sqlstate(&reply[0]), "26000");
    assert!(parsed(&h.backend).is_empty(), "{:?}", h.backend.log());

    assert_served(&n.query("select 1").await, b'I');
    n.send(&[cycle("select 1 from plain"), sync()].concat())
        .await;
    assert_served(&n.until_ready().await, b'I');
}

/// PostgreSQL destroys the unnamed statement before it parses an unnamed
/// Parse, so a cycle a hook refuses at one leaves none: a later Bind of it
/// fails 26000 and never runs the older statement the backend connection
/// still holds. With and without the unnamed-Parse promotion.
#[tokio::test]
async fn a_cycle_a_hook_refuses_at_an_unnamed_parse_leaves_no_unnamed_statement() {
    for promote in [true, false] {
        let shape = format!("promotion {promote}");
        let h = Harness::new(0, None, |c| {
            tenant_table(c);
            c.optimize_unnamed_parse = promote;
        })
        .await;
        let mut n = h.connect("nobody").await;
        n.send(&[cycle("delete from plain where id = $1"), sync()].concat())
            .await;
        assert_eq!(tags(&n.until_ready().await), b"12CZ", "{shape}");

        n.send(&[parse("", "select v from t where id = $1"), sync()].concat())
            .await;
        assert_refused(&n.until_ready().await, "42501", "no identified tenant");

        n.send(&[bind(""), execute(), sync()].concat()).await;
        let reply = n.until_ready().await;
        assert_eq!(tags(&reply), b"EZ", "{shape}: {reply:?}");
        assert_eq!(sqlstate(&reply[0]), "26000", "{shape}");
        let seen = h.backend.frames_of(n.key.0);
        assert_eq!(tags_of(&seen), b"PBESCHBES".to_vec(), "{shape}: {seen:?}");
        assert_eq!(seen[4].text, "S", "{shape}: the unnamed statement");
    }
}

/// A statement refused while part of its cycle already runs on the backend
/// (a Flush dispatched it) cannot be answered with an error alone: the
/// backend would commit that part at the client's Sync. The session ends
/// with the refusal as a FATAL, and the cycle is never completed.
#[tokio::test]
async fn a_statement_refused_inside_a_dispatched_cycle_ends_the_session() {
    let h = Harness::new(0, None, tenant_table).await;
    let mut n = h.connect("nobody").await;
    n.send(&[cycle("insert into plain values (1)"), flush()].concat())
        .await;
    assert_eq!(tags(&n.frames(3).await), b"12C");

    n.send(&[cycle("select v from t"), sync()].concat()).await;
    let fatal = n.frame().await;
    assert_eq!(sqlstate(&fatal), "42501");
    assert!(has_text(&fatal, "FATAL"), "{fatal:?}");
    assert!(has_text(&fatal, "no identified tenant"), "{fatal:?}");
    let Client { mut s, task, key } = n;
    let mut rest = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, s.read_to_end(&mut rest))
        .await
        .expect("the proxy closes the connection")
        .unwrap();
    assert!(rest.is_empty(), "nothing follows the FATAL: {rest:?}");
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("the session ends")
        .unwrap()
        .unwrap();
    let seen = h.backend.frames_of(key.0);
    assert_eq!(
        tags_of(&seen),
        b"PBEH".to_vec(),
        "the cycle got no Sync and the refused statement never arrived: {seen:?}"
    );
}

/// `[query_rewrite]` rules apply to the SQL of a `Parse`.
#[cfg(feature = "query-rewriting")]
#[tokio::test]
async fn rewrite_rules_apply_to_a_parse() {
    let h = Harness::new(0, None, |c| {
        c.query_rewrite.enabled = true;
        c.query_rewrite.rules = vec![crate::config::RewriteRuleToml {
            match_table: Some("legacy".into()),
            replace_table_with: Some("current".into()),
            ..Default::default()
        }];
    })
    .await;
    let mut a = h.connect("acme").await;
    a.send(&[cycle("select * from legacy"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    a.send(
        &[
            parse("s1", "select id from legacy"),
            bind("s1"),
            execute(),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_served(&a.until_ready().await, b'I');
    a.send(&[cycle("select * from other"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');

    let seen = parsed(&h.backend);
    assert_eq!(seen.len(), 3, "{seen:?}");
    for sql in &seen[..2] {
        assert!(
            sql.contains("current") && !sql.contains("legacy"),
            "not rewritten: {sql:?}"
        );
    }
    assert_eq!(seen[2], "select * from other");
}

/// `[schema_routing]`: an analytical read sent as an extended cycle runs on
/// the analytics node; a point read and a write stay on the primary.
#[cfg(feature = "schema-routing")]
#[tokio::test]
async fn schema_routing_routes_an_extended_olap_read() {
    let olap = FakeBackend::start(None).await;
    let olap_addr = olap.addr.to_string();
    let h = Harness::new(0, None, |c| {
        c.add_node(&olap_addr, "standby").unwrap();
        // Only schema routing may move a read off the primary here.
        c.load_balancer.read_write_split = false;
        c.schema_routing.enabled = true;
        c.schema_routing.analytics_node = olap_addr.clone();
    })
    .await;
    let mut a = h.connect("acme").await;
    for sql in [
        "select * from orders where id = 1",
        "select region, count(*) from orders group by region",
        "insert into orders values (1)",
    ] {
        a.send(&[cycle(sql), sync()].concat()).await;
        assert_served(&a.until_ready().await, b'I');
    }
    assert_eq!(
        parsed(&olap),
        ["select region, count(*) from orders group by region"]
    );
    assert_eq!(
        parsed(&h.backend),
        [
            "select * from orders where id = 1",
            "insert into orders values (1)"
        ]
    );
}

/// The `Parse` a rewrite produced keeps its statement name and its declared
/// parameter types.
#[test]
fn a_rewritten_parse_keeps_its_name_and_parameter_types() {
    let params = [0u8, 2, 0, 0, 0, 23, 0, 0, 0, 25];
    let payload = [
        &cstring("s1")[..],
        &cstring("select $1, $2")[..],
        &params[..],
    ]
    .concat();
    let msg = Message::new(MessageType::Parse, BytesMut::from(&payload[..]));
    let rewritten = ProxyServer::parse_with_sql(&msg, "select $2, $1 /* swapped */");
    assert_eq!(rewritten.msg_type, MessageType::Parse);
    assert_eq!(
        rewritten.payload.as_ref(),
        [
            &cstring("s1")[..],
            &cstring("select $2, $1 /* swapped */")[..],
            &params[..]
        ]
        .concat()
    );
    // The unnamed statement, no declared types.
    let msg = Message::new(
        MessageType::Parse,
        BytesMut::from(&[&cstring("")[..], &cstring("select 1")[..], &[0, 0][..]].concat()[..]),
    );
    assert_eq!(
        ProxyServer::parse_with_sql(&msg, "select 2")
            .payload
            .as_ref(),
        [&cstring("")[..], &cstring("select 2")[..], &[0, 0][..]].concat()
    );
}

/// A route plugin's override routes an extended batch as it routes a simple
/// Query: Primary as a write, Standby as an offloadable read unless the
/// statement writes, Node to that node.
#[cfg(feature = "wasm-plugins")]
#[test]
fn a_route_override_routes_an_extended_batch() {
    let state = ProxyServer::new(test_config()).unwrap().state.clone();
    let read = "select 1";
    let write = "insert into t values (1)";
    let route =
        |sql: &str, plugin: RouteOverride| ProxyServer::extended_route(&state, sql, &plugin).0;
    assert_eq!(route(read, RouteOverride::None), (false, None, false));
    assert_eq!(route(write, RouteOverride::None), (true, None, false));
    assert_eq!(route(read, RouteOverride::Primary), (true, None, false));
    assert_eq!(route(read, RouteOverride::Standby), (false, None, true));
    assert_eq!(route(write, RouteOverride::Standby), (false, None, false));
    assert_eq!(
        route(read, RouteOverride::Node("n2".into())),
        (false, Some("n2".into()), false)
    );
    assert_eq!(
        route(write, RouteOverride::Node("n2".into())),
        (true, Some("n2".into()), false)
    );
}

/// A pre-query plugin's Block refuses the extended cycle at its `Parse`
/// (42000, the plugin's reason): one packet, a flushed prepare, and the
/// later execution of the refused name, which was never registered. Nothing
/// reaches the backend and no tenant permit stays held.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_pre_query_block_refuses_an_extended_cycle() {
    let plugins = pre_query_plugin_dir(r#"{"action": "block", "value": "not here"}"#);
    let h = Harness::new(0, None, |c| plugins_from(c, &plugins)).await;
    let mut a = h.connect("acme").await;

    a.send(&[cycle("select 'blocked'"), sync()].concat()).await;
    assert_refused(
        &a.until_ready().await,
        "42000",
        "Query blocked by plugin: not here",
    );

    a.send(
        &[
            parse("s1", "select 'blocked too'"),
            describe_statement("s1"),
            flush(),
        ]
        .concat(),
    )
    .await;
    let err = a.frame().await;
    assert_eq!(sqlstate(&err), "42000");
    a.quiet().await;
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_eq!(a.frame().await, wire(b'Z', b"I"));
    a.quiet().await;
    assert_backend_untouched(&h.backend, "blocked cycles");
    h.until("acme", "no permit held", |s| s.queries_active == 0)
        .await;

    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"EZ", "{reply:?}");
    assert_eq!(sqlstate(&reply[0]), "26000");
    assert!(parsed(&h.backend).is_empty(), "{:?}", h.backend.log());
}

/// A pre-query plugin's Rewrite replaces the SQL of the `Parse` the backend
/// receives, unnamed or named.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_pre_query_rewrite_replaces_the_sql_of_a_parse() {
    let plugins = pre_query_plugin_dir(r#"{"action": "rewrite", "value": "select 'masked'"}"#);
    let h = Harness::new(0, None, |c| plugins_from(c, &plugins)).await;
    let mut a = h.connect("acme").await;
    a.send(&[cycle("select secret from vault"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    a.send(
        &[
            parse("s1", "select secret from vault where id = 1"),
            bind("s1"),
            execute(),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(parsed(&h.backend), ["select 'masked'", "select 'masked'"]);
}

/// A route plugin's Block refuses the cycle at its `Parse` as well.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_route_block_refuses_an_extended_cycle() {
    let plugins = hook_plugin_dir("route", r#"{"action": "block", "reason": "wrong region"}"#);
    let h = Harness::new(0, None, |c| plugins_from(c, &plugins)).await;
    let mut a = h.connect("acme").await;
    a.send(&[cycle("select 1"), sync()].concat()).await;
    assert_refused(
        &a.until_ready().await,
        "42000",
        "Query blocked by route plugin: wrong region",
    );
    assert_backend_untouched(&h.backend, "route block");
}

/// A plugin that limits executions sees each one: the Bind of a statement
/// prepared in an earlier cycle runs the pre-query hook again, named or
/// unnamed, and a Block refuses that execution. The statement was prepared
/// while the hook let it through.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_block_refuses_the_later_execution_of_a_prepared_statement() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_hook_plugin(
        dir.path(),
        "allow",
        "pre_query",
        "",
        r#"{"action": "continue"}"#,
    );
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    let mut a = h.connect("acme").await;
    a.send(&[parse("s1", "select 'named'"), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    a.send(&[parse("", "select 'unnamed'"), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    let binds = || h.backend.log().iter().filter(|s| s.tag == b'B').count();
    assert_eq!(binds(), 1);

    // The budget is spent: the plugin now blocks.
    let block = write_hook_plugin(
        dir.path(),
        "budget",
        "pre_query",
        "",
        r#"{"action": "block", "value": "budget spent"}"#,
    );
    h.state
        .plugin_manager
        .as_ref()
        .expect("plugins are on")
        .load_plugin(&block)
        .expect("the plugin loads");

    for stmt in ["s1", ""] {
        a.send(&[bind(stmt), execute(), sync()].concat()).await;
        assert_refused(
            &a.until_ready().await,
            "42000",
            "Query blocked by plugin: budget spent",
        );
    }
    assert_eq!(binds(), 1, "no blocked execution reached the backend");
}

/// The post-query hook fires once for an extended cycle its Sync ended.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn the_post_query_hook_fires_for_an_extended_cycle() {
    let plugins = hook_plugin_dir("post_query", "{}");
    let h = Harness::new(0, None, |c| plugins_from(c, &plugins)).await;
    let pm = h.state.plugin_manager.clone().expect("plugins are on");
    let calls = || pm.get_metrics().total_hook_calls;
    let mut a = h.connect("acme").await;
    assert_eq!(calls(), 0);

    a.send(&[cycle("select 1"), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(calls(), 1, "an unnamed cycle");

    // A Flush ends no cycle: the hook waits for the Sync.
    a.send(&[parse("s1", "select 2"), flush()].concat()).await;
    assert_eq!(tags(&a.frames(1).await), b"1");
    assert_eq!(calls(), 1);
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(calls(), 2, "a flushed cycle, at its Sync");

    // A cycle with no Parse reports the statement it executes.
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(calls(), 3, "a prepared statement's execution");
}

/// One rule: the table `tbl_legacy` is renamed `tbl_current`.
#[cfg(feature = "query-rewriting")]
fn legacy_to_current(c: &mut ProxyConfig) {
    c.query_rewrite.enabled = true;
    c.query_rewrite.rules = vec![crate::config::RewriteRuleToml {
        match_table: Some("tbl_legacy".into()),
        replace_table_with: Some("tbl_current".into()),
        ..Default::default()
    }];
}

/// A statement whose text is not UTF-8 (a LATIN1 client: `é` as 0xE9)
/// cannot be read by the rewrite rules, nor any other statement hook: while
/// one is active it is refused 22021 on both protocols instead of passing
/// them unread, nothing of it reaches the backend, and the session goes on.
#[cfg(feature = "query-rewriting")]
#[tokio::test]
async fn a_statement_no_hook_can_read_is_refused_while_hooks_are_active() {
    let h = Harness::new(0, None, legacy_to_current).await;
    let mut a = h.connect("acme").await;
    let latin1: &[u8] = b"select * from tbl_legacy where name = '\xe9'";

    a.send(&wire(b'Q', &[latin1, b"\0"].concat())).await;
    assert_refused(&a.until_ready().await, "22021", "not valid UTF-8");

    let unnamed = wire(b'P', &[b"\0", latin1, b"\0\0\0"].concat());
    a.send(&[unnamed, bind(""), execute(), sync()].concat())
        .await;
    assert_refused(&a.until_ready().await, "22021", "not valid UTF-8");

    let named = wire(b'P', &[b"s1\0", latin1, b"\0\0\0"].concat());
    a.send(&[named, sync()].concat()).await;
    assert_refused(&a.until_ready().await, "22021", "not valid UTF-8");
    assert_backend_untouched(&h.backend, "unreadable statements");

    // The refused name was never registered; UTF-8 text is served.
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    let reply = a.until_ready().await;
    assert_eq!(sqlstate(&reply[0]), "26000", "{reply:?}");
    assert_served(&a.query("select 1").await, b'I');
}

/// The tenant row filter is a statement hook too: a row-isolated tenant's
/// statement that is not UTF-8 is refused rather than run unfiltered.
#[cfg(feature = "multi-tenancy")]
#[tokio::test]
async fn a_row_isolated_tenants_unreadable_statement_is_refused() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let latin1: &[u8] = b"select '\xe9'";
    a.send(&wire(b'Q', &[latin1, b"\0"].concat())).await;
    assert_refused(&a.until_ready().await, "22021", "not valid UTF-8");
    let unnamed = wire(b'P', &[b"\0", latin1, b"\0\0\0"].concat());
    a.send(&[unnamed, bind(""), execute(), sync()].concat())
        .await;
    assert_refused(&a.until_ready().await, "22021", "not valid UTF-8");
    assert_backend_untouched(&h.backend, "unreadable tenant statements");
    assert_served(&a.query("select 1").await, b'I');
}

/// Without a statement hook nothing reads the text: a statement that is not
/// UTF-8 is forwarded as before, on both protocols.
#[tokio::test]
async fn a_statement_that_is_not_utf8_is_forwarded_without_hooks() {
    // The harness's tenants are row-isolated, and the row filter reads every
    // statement: without it no statement hook is active.
    let h = Harness::new(0, None, |c| c.multi_tenancy.enabled = false).await;
    let mut a = h.connect("acme").await;
    let latin1: &[u8] = b"select '\xe9'";
    a.send(&wire(b'Q', &[latin1, b"\0"].concat())).await;
    assert_served(&a.until_ready().await, b'I');
    let unnamed = wire(b'P', &[b"\0", latin1, b"\0\0\0"].concat());
    a.send(&[unnamed, bind(""), execute(), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    let seen = h.backend.log();
    assert!(
        seen.iter().any(|s| s.tag == b'Q') && seen.iter().any(|s| s.tag == b'P'),
        "{seen:?}"
    );
}

/// At a later execution of a prepared statement the plugin hooks read the
/// client's text, as they did at its `Parse`, not the text the rewrite rules
/// registered: a plugin that blocks statements naming `tbl_current` lets the
/// client's `tbl_legacy` statement run (the rules rename it), named and
/// unnamed, at every execution, and still blocks the client's own
/// `tbl_current` statement.
#[cfg(all(feature = "wasm-plugins", feature = "query-rewriting"))]
#[tokio::test]
async fn the_plugin_hooks_read_the_clients_text_at_a_later_execution() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_hook_plugin(
        dir.path(),
        "guard",
        "pre_query",
        "tbl_current",
        r#"{"action": "block", "value": "tbl_current is off limits"}"#,
    );
    let h = Harness::new(0, None, |c| {
        plugins_from(c, &dir);
        legacy_to_current(c);
    })
    .await;
    let mut a = h.connect("acme").await;
    a.send(
        &[
            parse("s1", "select id from tbl_legacy"),
            parse("", "select name from tbl_legacy"),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_eq!(tags(&a.until_ready().await), b"11Z");
    for _ in 0..2 {
        for stmt in ["s1", ""] {
            a.send(&[bind(stmt), execute(), sync()].concat()).await;
            assert_served(&a.until_ready().await, b'I');
        }
    }
    assert!(
        parsed(&h.backend)
            .iter()
            .all(|sql| sql.contains("tbl_current")),
        "{:?}",
        parsed(&h.backend)
    );
    a.send(&[cycle("select id from tbl_current"), sync()].concat())
        .await;
    assert_refused(
        &a.until_ready().await,
        "42000",
        "Query blocked by plugin: tbl_current is off limits",
    );
}

/// A pre-query plugin's rewrite is applied at the `Parse`. When, at a later
/// execution, the plugins rewrite the client's text to something else than
/// the statement was prepared with, the execution is refused 0A000 (prepare
/// it again) instead of running the earlier rewrite; prepared again, it runs
/// the new one. A plugin that no longer rewrites lets it run as prepared.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_changed_plugin_rewrite_refuses_the_execution_of_a_prepared_statement() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_hook_plugin(
        dir.path(),
        "mask-v1",
        "pre_query",
        "'v1'",
        r#"{"action": "rewrite", "value": "select 'masked one'"}"#,
    );
    let h = Harness::new(0, None, |c| plugins_from(c, &dir)).await;
    let pm = h.state.plugin_manager.clone().expect("plugins are on");
    let mut a = h.connect("acme").await;
    a.send(&[parse("s1", "select 'v1'"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');

    // The masking changes: the same text is now rewritten to another one.
    pm.unload_plugin("mask-v1").expect("unloads");
    let v2 = write_hook_plugin(
        dir.path(),
        "mask-v2",
        "pre_query",
        "'v1'",
        r#"{"action": "rewrite", "value": "select 'masked two'"}"#,
    );
    pm.load_plugin(&v2).expect("the plugin loads");
    a.send(&[bind("s1"), execute(), sync()].concat()).await;
    assert_refused(&a.until_ready().await, "0A000", "prepared again");
    let binds = || h.backend.log().iter().filter(|s| s.tag == b'B').count();
    assert_eq!(
        binds(),
        1,
        "the refused execution never reached the backend"
    );

    // Prepared again (under a new name: a client deallocates the old one
    // first), it runs the new rewrite.
    a.send(&[parse("s2", "select 'v1'"), bind("s2"), execute(), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(
        parsed(&h.backend),
        ["select 'masked one'", "select 'masked two'"]
    );

    // No longer rewritten: both run as prepared.
    pm.unload_plugin("mask-v2").expect("unloads");
    let allow = write_hook_plugin(
        dir.path(),
        "allow",
        "pre_query",
        "",
        r#"{"action": "continue"}"#,
    );
    pm.load_plugin(&allow).expect("the plugin loads");
    for stmt in ["s1", "s2"] {
        a.send(&[bind(stmt), execute(), sync()].concat()).await;
        assert_served(&a.until_ready().await, b'I');
    }
}

/// `[schema_routing]` sends a cycle to the analytics node only when every
/// statement of it is an analytical read: an aggregate followed by a write in
/// the same cycle runs on the primary, as does an aggregate batched behind a
/// point read; a cycle that executes a prepared aggregate (no `Parse`) and
/// another aggregate goes there whole.
#[cfg(feature = "schema-routing")]
#[tokio::test]
async fn schema_routing_reads_every_statement_of_a_cycle() {
    let olap = FakeBackend::start(None).await;
    let olap_addr = olap.addr.to_string();
    let h = Harness::new(0, None, |c| {
        c.add_node(&olap_addr, "standby").unwrap();
        c.load_balancer.read_write_split = false;
        c.schema_routing.enabled = true;
        c.schema_routing.analytics_node = olap_addr.clone();
    })
    .await;
    let mut a = h.connect("acme").await;
    let aggregate = "select region, count(*) from orders group by region";
    for second in [
        "insert into orders values (1)",
        "select * from orders where id = 1",
    ] {
        a.send(&[cycle(aggregate), cycle(second), sync()].concat())
            .await;
        assert_served(&a.until_ready().await, b'I');
    }
    assert!(parsed(&olap).is_empty(), "{:?}", olap.log());

    // A prepared aggregate, executed with another aggregate: all analytical.
    a.send(&[parse("agg", aggregate), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    let other = "select sum(amount) from orders group by region";
    a.send(&[bind("agg"), execute(), cycle(other), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    let on_olap = parsed(&olap);
    assert!(
        on_olap.iter().any(|sql| sql == other),
        "the analytical cycle ran on the analytics node: {on_olap:?}"
    );
}

/// The route plugin's answers for the statements of one cycle are combined:
/// a cycle runs on one connection, so a statement sent to a node and another
/// sent to the primary refuse the cycle (42000) instead of running one of
/// them where its plugin did not send it; agreeing answers route it. A cycle
/// that only executes a prepared statement is routed by the plugin's answer
/// for that execution, also without read offload.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn the_route_plugin_routes_by_every_statement_of_a_cycle() {
    let eu = FakeBackend::start(None).await;
    let eu_addr = eu.addr.to_string();
    let dir = tempfile::tempdir().expect("a temp dir");
    write_hook_plugin(
        dir.path(),
        "residency",
        "route",
        "region_eu",
        &format!(r#"{{"action": "node", "target": "{eu_addr}"}}"#),
    );
    write_hook_plugin(
        dir.path(),
        "writes-home",
        "route",
        "region_us",
        r#"{"action": "primary"}"#,
    );
    let h = Harness::new(0, None, |c| {
        plugins_from(c, &dir);
        c.add_node(&eu_addr, "standby").unwrap();
        c.load_balancer.read_write_split = false;
    })
    .await;
    let mut a = h.connect("acme").await;

    a.send(
        &[
            cycle("select * from region_eu"),
            cycle("select * from region_us"),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_refused(&a.until_ready().await, "42000", "different targets");
    assert!(parsed(&eu).is_empty(), "{:?}", eu.log());
    assert!(parsed(&h.backend).is_empty(), "{:?}", h.backend.log());

    a.send(&[cycle("select * from region_eu"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(parsed(&eu), ["select * from region_eu"]);

    // Prepared where the plugin sends it; then the session runs a cycle on
    // the primary; then the statement is executed alone: the plugin's answer
    // for that execution routes the cycle back to its node (before, a cycle
    // with no Parse stayed on the session's current node, the primary).
    a.send(&[parse("s2", "select 2 from region_eu"), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    a.send(&[cycle("select 1 from region_us"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    let binds = |b: &FakeBackend| b.log().iter().filter(|s| s.tag == b'B').count();
    let primary_binds = binds(&h.backend);
    a.send(&[bind("s2"), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(binds(&eu), 2, "{:?}", eu.log());
    assert_eq!(binds(&h.backend), primary_binds, "{:?}", h.backend.log());
}
