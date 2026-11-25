//! W07: tenant isolation and per-tenant pools through the whole client
//! handler. A session is pinned to its tenant at admission; the startup
//! packet the backend gets carries the tenant's schema (`options -c
//! search_path`), database, or branch; `lock_search_path` refuses statements
//! changing `search_path`; `[[hba]]` is checked again on the database the
//! session really gets; a branch move is refused; and a tenant with pool
//! settings of its own is pooled apart and capped by `pool_max_connections`.
use super::*;
use crate::config::{SchemaApply, TenantIsolation, TenantSpec};

/// The known tenants replaced by `specs` (`tenant_settings`), identified
/// by user, without the harness's active-query caps.
fn tenants(c: &mut ProxyConfig, specs: Vec<TenantSpec>) {
    c.multi_tenancy.tenants = Vec::new();
    c.multi_tenancy.tenant_settings = specs;
    c.multi_tenancy.max_active_queries_per_tenant = 0;
}

fn spec(id: &str, isolation: TenantIsolation) -> TenantSpec {
    TenantSpec {
        isolation,
        ..TenantSpec::from(id)
    }
}

/// A startup packet (protocol 3.0) with `params`, in order.
fn startup_with(params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (k, v) in params {
        body.extend(cstring(k));
        body.extend(cstring(v));
    }
    body.push(0);
    let mut out = ((8 + body.len()) as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&196608u32.to_be_bytes());
    out.extend(body);
    out
}

/// A session the proxy refuses during its startup: the one frame it gets
/// (an ErrorResponse) before the proxy closes the connection.
async fn refused_at_startup(h: &Harness, startup: Vec<u8>) -> Vec<u8> {
    let (proxy, mut s) = pair().await;
    let peer = proxy.peer_addr().unwrap();
    let task = tokio::spawn(ProxyServer::handle_client(
        proxy,
        peer,
        h.state.clone(),
        Arc::new(h.config.clone()),
        h.server.shutdown_tx.clone(),
    ));
    s.write_all(&startup).await.unwrap();
    let mut client = Client {
        s,
        key: (0, 0),
        task,
    };
    let frame = client.frame().await;
    client.closed().await;
    frame
}

/// The startup parameters of the newest backend connection.
fn last_startup(h: &Harness) -> HashMap<String, String> {
    h.backend
        .startup_params()
        .pop()
        .expect("a backend connection was opened")
}

/// Schema isolation with `schema_apply = "startup"` (the default): the
/// backend connection is opened with the tenant's schema as its
/// `search_path` (`options -c`, after the client's own options, so it
/// wins), a `search_path` startup parameter of the client's is dropped,
/// and statements changing `search_path` are refused (42501) on either
/// protocol without reaching the backend. Reading it is fine.
#[tokio::test]
async fn schema_isolation_sets_the_search_path_at_startup_and_locks_it() {
    let h = Harness::new(0, None, |c| {
        let mut acme = spec("acme", TenantIsolation::Schema);
        acme.schema = Some("acme_s".into());
        tenants(c, vec![acme]);
    })
    .await;
    let mut a = h
        .connect_with(startup_with(&[
            ("user", "acme"),
            ("database", "app"),
            ("options", "-c work_mem=64MB"),
            ("Search_Path", "public"),
        ]))
        .await;
    let params = last_startup(&h);
    assert_eq!(
        params.get("options").map(String::as_str),
        Some("-c work_mem=64MB -c search_path=\"acme_s\"")
    );
    assert!(
        !params.keys().any(|k| k.eq_ignore_ascii_case("search_path")),
        "{params:?}"
    );
    assert_eq!(params.get("database").map(String::as_str), Some("app"));

    for sql in [
        "SET search_path TO public",
        "set local search_path = other",
        "SET SESSION \"search_path\" TO public",
        "SET SCHEMA 'public'",
        "SELECT set_config('search_path', 'public', false)",
        "select 1; set search_path = x",
    ] {
        assert_refused(&a.query(sql).await, "42501", "lock_search_path");
    }
    a.send(&[cycle("SET search_path TO public"), sync()].concat())
        .await;
    assert_refused(&a.until_ready().await, "42501", "lock_search_path");
    assert!(
        !h.backend
            .log()
            .iter()
            .any(|s| s.text.to_ascii_lowercase().contains("search_path")),
        "{:?}",
        h.backend.log()
    );
    // Reading it, and resetting what startup set (the schema stays: it is
    // the connection's startup value), are served.
    assert_served(&a.query("SHOW search_path").await, b'I');
    assert_served(&a.query("RESET ALL").await, b'I');
}

/// `lock_search_path = false` lets the session change its `search_path`.
#[tokio::test]
async fn an_unlocked_search_path_may_be_changed() {
    let h = Harness::new(0, None, |c| {
        tenants(c, vec![spec("acme", TenantIsolation::Schema)]);
        c.multi_tenancy.lock_search_path = false;
    })
    .await;
    let mut a = h.connect("acme").await;
    assert_eq!(
        last_startup(&h).get("options").map(String::as_str),
        Some("-c search_path=\"acme\""),
        "the schema defaults to the tenant id"
    );
    assert_served(&a.query("SET search_path TO public").await, b'I');
    assert!(h.backend.saw_query("SET search_path TO public"));
}

/// `schema_apply = "set_on_acquire"`: the startup packet is left alone and
/// every backend connection the session gets runs `SET search_path` first,
/// in transaction pooling again at each lease after the pool's reset; with
/// the schema set that way, `RESET ALL` and `DISCARD ALL` (which would
/// clear it) are refused too.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn set_on_acquire_sets_the_search_path_on_every_connection_the_session_gets() {
    let h = Harness::new(0, None, |c| {
        let mut acme = spec("acme", TenantIsolation::Schema);
        acme.schema = Some("acme_s".into());
        tenants(c, vec![acme]);
        c.multi_tenancy.schema_apply = SchemaApply::SetOnAcquire;
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    })
    .await;
    let mut a = h.connect("acme").await;
    assert!(!last_startup(&h).contains_key("options"));
    let set = "SET search_path TO \"acme_s\"";
    assert_served(&a.query("select 1").await, b'I');
    assert_served(&a.query("select 2").await, b'I');
    let queries: Vec<String> = h
        .backend
        .frames_of(1)
        .into_iter()
        .filter(|s| s.tag == b'Q')
        .map(|s| s.text)
        .collect();
    assert_eq!(
        queries[..5],
        [set, "select 1", "DISCARD ALL", set, "select 2"],
        "{queries:?}"
    );
    for sql in ["RESET ALL", "DISCARD ALL", "RESET search_path"] {
        assert_refused(&a.query(sql).await, "42501", "lock_search_path");
    }
}

/// Database isolation: the backend connection is opened on the tenant's
/// database whatever database the client named; another tenant's session
/// and a session of no tenant are left alone.
#[tokio::test]
async fn database_isolation_connects_to_the_tenants_database() {
    let h = Harness::new(0, None, |c| {
        let mut acme = spec("acme", TenantIsolation::Database);
        acme.database = Some("acme_db".into());
        tenants(c, vec![acme, spec("other", TenantIsolation::Row)]);
    })
    .await;
    let mut a = h.connect_db("acme", "app").await;
    assert_eq!(
        last_startup(&h).get("database").map(String::as_str),
        Some("acme_db")
    );
    assert_served(&a.query("select 1").await, b'I');
    h.connect_db("other", "app").await;
    assert_eq!(
        last_startup(&h).get("database").map(String::as_str),
        Some("app")
    );
    h.connect_db("nobody", "app").await;
    assert_eq!(
        last_startup(&h).get("database").map(String::as_str),
        Some("app")
    );
}

/// Branch isolation: the client's database's branch, `<base>@<branch>` on
/// HeliosDB-Nano (`[branch] route_mode = "nano"`), the branch database
/// itself with `route_mode = "database"`.
#[tokio::test]
async fn branch_isolation_connects_to_the_tenants_branch() {
    for (mode, expected) in [
        (crate::config::BranchRouteMode::Nano, "app@b1"),
        (crate::config::BranchRouteMode::Database, "b1"),
    ] {
        let h = Harness::new(0, None, |c| {
            let mut acme = spec("acme", TenantIsolation::Branch);
            acme.branch = Some("b1".into());
            tenants(c, vec![acme]);
            c.branch.route_mode = mode;
        })
        .await;
        h.connect_db("acme", "app@old").await;
        assert_eq!(
            last_startup(&h).get("database").map(String::as_str),
            Some(expected),
            "{mode:?}"
        );
    }
}

/// `identify_by = "database"` with database isolation: the session is
/// pinned to the tenant its startup database names before that database is
/// rewritten, so its statements are still its tenant's (not refused as a
/// session of no tenant under `unknown_tenant = "refuse"`), while a session
/// of no tenant is refused.
#[tokio::test]
async fn a_database_tenant_stays_identified_after_its_database_is_rewritten() {
    let h = Harness::new(0, None, |c| {
        c.multi_tenancy.identify_by = "database".into();
        let mut acme = spec("acme", TenantIsolation::Database);
        acme.database = Some("acme_db".into());
        tenants(c, vec![acme]);
        c.multi_tenancy.tenant_tables = vec!["orders".into()];
        c.multi_tenancy.unknown_tenant = crate::config::UnknownTenantPolicy::Refuse;
    })
    .await;
    let mut a = h.connect_db("bob", "acme").await;
    assert_eq!(
        last_startup(&h).get("database").map(String::as_str),
        Some("acme_db")
    );
    assert_served(&a.query("SELECT * FROM orders").await, b'I');
    assert!(h.backend.saw_query("SELECT * FROM orders"));
    let mut b = h.connect_db("bob", "elsewhere").await;
    assert_refused(&b.query("SELECT * FROM orders").await, "42501", "tenant");
}

/// A row tenant is still identified when its startup parameter differs in
/// case (as the row filter's identifier reads it), and filtered by its id.
#[tokio::test]
async fn a_row_tenant_is_pinned_and_filtered() {
    let h = Harness::new(0, None, |c| {
        tenants(c, vec![spec("acme", TenantIsolation::Row)]);
        c.multi_tenancy.tenant_tables = vec!["orders".into()];
    })
    .await;
    let mut a = h.connect("ACME").await;
    assert_served(&a.query("SELECT * FROM orders").await, b'I');
    let filtered = h
        .backend
        .log()
        .into_iter()
        .find(|s| s.tag == b'Q' && s.text.contains("orders"))
        .expect("the statement reached the backend");
    assert!(filtered.text.contains("'acme'"), "{}", filtered.text);
}

/// `[[hba]]` is checked again on the database the tenant's isolation gives
/// the session: a rule rejecting it refuses the session (28000) before any
/// backend connection, although the database the client named is allowed.
#[tokio::test]
async fn hba_rules_apply_to_the_tenants_database() {
    let h = Harness::new(0, None, |c| {
        let mut acme = spec("acme", TenantIsolation::Database);
        acme.database = Some("acme_db".into());
        tenants(c, vec![acme]);
        c.hba = vec![crate::config::HbaRule {
            action: crate::config::HbaAction::Reject,
            user: "all".into(),
            database: "acme_db".into(),
            address: "all".into(),
        }];
    })
    .await;
    let refusal = refused_at_startup(&h, startup_bytes_db("acme", "app")).await;
    assert_eq!(sqlstate(&refusal), "28000");
    assert!(has_text(&refusal, "admission rules"), "{refusal:?}");
    assert!(
        h.backend.startup_params().is_empty(),
        "no backend connection"
    );
    // A session of no tenant on the database the client named is admitted.
    h.connect_db("other", "app").await;
}

/// A branch move (a `branch=` hint) is refused (0A000) for a session whose
/// tenant is isolated by schema, database or branch: it would leave its
/// isolation. A row tenant moves as before.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn an_isolated_tenant_cannot_move_to_a_branch() {
    let h = Harness::new(0, None, |c| {
        tenants(
            c,
            vec![
                spec("acme", TenantIsolation::Schema),
                spec("other", TenantIsolation::Row),
            ],
        );
        c.routing_hints.enabled = true;
        c.branch.route_enabled = true;
    })
    .await;
    let mut a = h.connect_db("acme", "app").await;
    let refused = a.query("/*helios:branch=feat*/ select 1").await;
    assert_refused(&refused, "0A000", "isolated by its own schema");
    assert!(!h.backend.saw_query("/*helios:branch=feat*/ select 1"));
    assert_eq!(h.backend.startup_params().len(), 1, "no move");
    let mut b = h.connect_db("other", "app").await;
    assert_served(&b.query("/*helios:branch=feat*/ select 1").await, b'I');
    assert_eq!(
        last_startup(&h).get("database").map(String::as_str),
        Some("feat")
    );
}

/// Per-tenant pools: a tenant with pool settings of its own parks its
/// connections under keys of its own (`\0t=<tenant>`), at most
/// `pool_max_idle` per identity, apart from other tenants; `/pools` counts
/// its dials and checkouts.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn a_tenant_with_pool_settings_is_pooled_apart() {
    let h = Harness::new(0, None, |c| {
        let mut acme = spec("acme", TenantIsolation::Row);
        acme.pool_max_idle = Some(1);
        tenants(c, vec![acme, spec("other", TenantIsolation::Row)]);
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    })
    .await;
    let addr = h.backend.addr.to_string();
    let pool = h.state.backend_pool.as_deref().unwrap();
    let mut a1 = h.connect("acme").await;
    let mut a2 = h.connect("acme").await;
    let mut o = h.connect("other").await;
    for c in [&mut a1, &mut a2, &mut o] {
        assert_served(&c.query("BEGIN").await, b'T');
    }
    for c in [&mut a1, &mut a2, &mut o] {
        assert_served(&c.query("COMMIT").await, b'I');
    }
    let acme_key = format!("{}\0t=acme", crate::pool::pool_key(&addr, "acme", "acme"));
    let other_key = crate::pool::pool_key(&addr, "other", "other");
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while pool.idle_count_for(&acme_key) + pool.idle_count_for(&other_key) < 2
        || pool.over_capacity() < 1
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "parking never settled"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(pool.idle_count_for(&acme_key), 1, "pool_max_idle = 1");
    assert_eq!(pool.idle_count_for(&other_key), 1);
    assert_eq!(pool.over_capacity(), 1);
    // The parked one is leased by the tenant's next transaction.
    assert_served(&a1.query("select 1").await, b'I');
    let stats = h.state.tenant_slots.pool_stats();
    assert_eq!(stats.len(), 1, "only acme has a pool of its own");
    assert_eq!(
        (
            stats[0].tenant.as_str(),
            stats[0].limit,
            stats[0].dials,
            stats[0].checkouts
        ),
        ("acme", None, 2, 1)
    );
}

/// `pool_max_connections` caps a tenant's backend connections (in use and
/// parked): a session that needs one more first takes the slot of one of
/// the tenant's parked connections, then waits up to `[limits]
/// backend_slot_wait_ms`, then is refused with 53300 (here at its
/// admission); other tenants are not affected.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn pool_max_connections_caps_a_tenants_backend_connections() {
    let h = Harness::new(0, None, |c| {
        let mut acme = spec("acme", TenantIsolation::Row);
        acme.pool_max_connections = Some(1);
        tenants(c, vec![acme, spec("other", TenantIsolation::Row)]);
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
        c.limits.backend_slot_wait_ms = 200;
    })
    .await;
    let pool = h.state.backend_pool.as_deref().unwrap();
    let mut a = h.connect("acme").await;
    assert_served(&a.query("select 1").await, b'I');
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while pool.idle_count() != 1 {
        assert!(tokio::time::Instant::now() < deadline, "never parked");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A second acme session takes the slot of the parked connection.
    let mut b = h.connect("acme").await;
    assert_eq!(pool.idle_count(), 0);
    assert_eq!(pool.capacity_evictions(), 1);
    assert_served(&b.query("BEGIN").await, b'T');
    // A third waits for the slot b holds, then is refused at admission.
    let refusal = refused_at_startup(&h, startup_bytes("acme")).await;
    assert_eq!(sqlstate(&refusal), "53300");
    assert!(
        has_text(&refusal, "pool_max_connections (1)"),
        "{refusal:?}"
    );
    // Other tenants are not capped by it.
    let mut o = h.connect("other").await;
    assert_served(&o.query("select 1").await, b'I');
    let stats = &h.state.tenant_slots.pool_stats()[0];
    assert_eq!(
        (stats.limit, stats.in_use, stats.waits, stats.refusals),
        (Some(1), Some(1), 1, 1)
    );
    assert_served(&b.query("COMMIT").await, b'I');
}

/// A gateway with a `tenant_header` cannot apply schema, database or
/// branch isolation: the proxy refuses to start with both.
#[test]
fn gateways_with_a_tenant_header_refuse_isolated_tenants() {
    let mut config = test_config();
    config.multi_tenancy.enabled = true;
    config.multi_tenancy.identify_by = "user".into();
    config.multi_tenancy.tenant_settings = vec![spec("acme", TenantIsolation::Database)];
    config.http_gateway.enabled = true;
    config.http_gateway.tenant_header = "x-tenant".into();
    let Err(e) = ProxyServer::new(config.clone()) else {
        panic!("started with a gateway and an isolated tenant");
    };
    assert!(
        e.to_string()
            .contains("tenants acme use schema, database or branch"),
        "{e}"
    );
    config.multi_tenancy.tenant_settings.clear();
    config.multi_tenancy.tenants = vec!["acme".into()];
    assert!(
        ProxyServer::check_gateway_tenancy(&config).is_ok(),
        "row tenants are fine"
    );
}
