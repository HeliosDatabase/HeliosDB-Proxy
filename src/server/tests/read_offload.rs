//! Read offload to standbys: `[load_balancer] standby_reads` (Sprinter
//! 6b0487365969).
use super::*;
use crate::config::StandbyReads;

/// A primary and two standbys; `pg-s2` replicates synchronously.
fn offload_config(mode: StandbyReads) -> ProxyConfig {
    let mut config = ProxyConfig::default();
    config.add_node("pg-p:5432", "primary").unwrap();
    config.add_node("pg-s1:5432", "standby").unwrap();
    config.add_node("pg-s2:5432", "standby").unwrap();
    config.nodes[2].synchronous = true;
    config.load_balancer.standby_reads = mode;
    config
}

const ON: ReadRouting<'static> = ReadRouting {
    offload_on: true,
    may_offload: true,
    batch_reads: true,
    batch_whole: true,
    gucs: &[],
    guc_epoch: 0,
    read_your_writes: Duration::ZERO,
};

async fn route(
    server: &ProxyServer,
    config: &ProxyConfig,
    session: &Arc<ClientSession>,
    is_write: bool,
    current: Option<&str>,
    read: ReadRoute,
) -> (String, bool) {
    ProxyServer::choose_target_node(
        is_write,
        None,
        current,
        read,
        session,
        &server.state,
        config,
    )
    .await
    .unwrap()
}

fn set_healthy(server: &ProxyServer, addr: &str, healthy: bool) {
    let mut health = (*server.state.health.load_full()).clone();
    health.get_mut(addr).expect("configured node").healthy = healthy;
    server.state.health.store(Arc::new(health));
}

#[test]
fn standby_safe_reads_are_single_side_effect_free_reads() {
    let policy = TrReadPolicy::default();
    for (sql, safe) in [
        ("SELECT * FROM t WHERE id = 1", true),
        ("select a, b from t order by a", true),
        ("VALUES (1), (2)", true),
        ("TABLE t", true),
        ("WITH x AS (SELECT 1 AS a) SELECT a FROM x", true),
        ("SELECT * FROM t FOR UPDATE", false),
        ("SELECT * FROM t FOR NO KEY UPDATE", false),
        ("SELECT * FROM t FOR SHARE", false),
        ("SELECT now(), version()", true),
        ("SELECT inet_server_addr()", true),
        ("SELECT pg_is_in_recovery()", false),
        ("SELECT inet_server_port(), PG_IS_IN_RECOVERY()", false),
        ("SELECT pg_current_wal_lsn()", false),
        ("SELECT txid_current()", false),
        ("SELECT nextval('s')", false),
        ("SELECT my_function(1)", false),
        ("SELECT * INTO new_t FROM t", false),
        (
            "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
            false,
        ),
        ("SELECT 1; DELETE FROM t", false),
        ("INSERT INTO t VALUES (1)", false),
        ("SHOW search_path", false),
        ("EXPLAIN SELECT 1", false),
        ("COPY t TO STDOUT", false),
        ("BEGIN", false),
        ("SET search_path TO app", false),
        ("", false),
    ] {
        assert_eq!(
            ProxyServer::standby_safe_read(sql, &policy),
            safe,
            "{sql:?}"
        );
    }
}

#[test]
fn statements_that_leave_connection_state_pin_reads() {
    for (sql, pins) in [
        ("CREATE TEMP TABLE x (a int)", true),
        ("CREATE TEMPORARY TABLE x (a int)", true),
        ("create local temp table x (a int)", true),
        ("SELECT * INTO TEMP x FROM t", true),
        ("DECLARE c CURSOR WITH HOLD FOR SELECT 1", true),
        ("LISTEN chan", true),
        ("PREPARE p AS SELECT 1", true),
        ("LOAD 'auto_explain'", true),
        ("SELECT set_config('app.x', '1', false)", true),
        ("SELECT 1; SELECT 2", true),
        ("CREATE OR REPLACE TEMP VIEW v AS SELECT 1", true),
        ("CREATE GLOBAL TEMPORARY TABLE x (a int)", true),
        ("PREPARE TRANSACTION 'g1'", false),
        (
            "WITH x AS (SELECT 1 AS a) INSERT INTO t SELECT a FROM x",
            false,
        ),
        ("SELECT * INTO new_t FROM t", false),
        ("SELECT * FROM t WHERE note = 'into temp'", false),
        ("CREATE INDEX i ON t (temp)", false),
        ("SELECT 'set_config(1)'", false),
        ("SELECT 1;", false),
        ("SET search_path TO app", false),
        ("CREATE TABLE t (a int)", false),
        ("SELECT * FROM t", false),
        ("INSERT INTO t VALUES (1)", false),
    ] {
        assert_eq!(ProxyServer::pins_reads(sql), pins, "{sql:?}");
    }
}

#[test]
fn the_read_route_follows_the_session_and_the_statement() {
    let session = make_test_session();
    assert_eq!(
        ReadRouting::OFF.route(&session, true, || true),
        ReadRoute::Stay,
        "no offload: the session's node, as before"
    );
    assert_eq!(ON.route(&session, false, || true), ReadRoute::Offload);
    assert_eq!(ON.route(&session, false, || false), ReadRoute::Home);
    assert_eq!(
        ON.route(&session, true, || false),
        ReadRoute::Offload,
        "a route=standby hint offloads what the proxy cannot classify"
    );
    let pinned = ReadRouting {
        may_offload: false,
        ..ON
    };
    assert_eq!(pinned.route(&session, true, || true), ReadRoute::Home);
    // Read-your-writes: right after the session's write its reads stay home,
    // unless a hint asks for a standby.
    let ryw = ReadRouting {
        read_your_writes: Duration::from_secs(60),
        ..ON
    };
    assert_eq!(ryw.route(&session, false, || true), ReadRoute::Offload);
    ryw.note_write(&session);
    assert_eq!(ryw.route(&session, false, || true), ReadRoute::Home);
    assert_eq!(ryw.route(&session, true, || true), ReadRoute::Offload);
    assert_eq!(
        ON.route(&session, false, || true),
        ReadRoute::Offload,
        "a zero window never holds reads back"
    );
    // A statement the proxy cannot vouch for opens the window too.
    let fresh = make_test_session();
    assert_eq!(ryw.route(&fresh, false, || false), ReadRoute::Home);
    assert_eq!(ryw.route(&fresh, false, || true), ReadRoute::Home);
    let unhinted = ReadRouting {
        read_your_writes: Duration::from_millis(1),
        ..ON
    };
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(unhinted.route(&session, false, || true), ReadRoute::Offload);

    session
        .in_transaction
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        ON.route(&session, true, || true),
        ReadRoute::Stay,
        "a transaction stays on its node"
    );
}

#[tokio::test]
async fn async_reads_leave_the_primary_and_stick_to_one_standby() {
    let config = offload_config(StandbyReads::Async);
    let server = ProxyServer::new(config.clone()).unwrap();
    let session = make_test_session();
    let primary = "pg-p:5432";

    // A write goes to the primary.
    assert_eq!(
        route(
            &server,
            &config,
            &session,
            true,
            Some(primary),
            ReadRoute::Stay
        )
        .await,
        (primary.to_string(), false)
    );
    // An offloadable read leaves it for a standby...
    let (first, offloaded) = route(
        &server,
        &config,
        &session,
        false,
        Some(primary),
        ReadRoute::Offload,
    )
    .await;
    assert!(offloaded && first != primary, "{first}");
    // ...and keeps using that standby, also after the session went home.
    for _ in 0..5 {
        assert_eq!(
            route(
                &server,
                &config,
                &session,
                false,
                Some(primary),
                ReadRoute::Offload
            )
            .await,
            (first.clone(), true)
        );
    }
    // A read the primary must serve goes home from the standby.
    assert_eq!(
        route(
            &server,
            &config,
            &session,
            false,
            Some(first.as_str()),
            ReadRoute::Home
        )
        .await,
        (primary.to_string(), false)
    );
    // Without offload the session stays on its healthy node.
    assert_eq!(
        route(
            &server,
            &config,
            &session,
            false,
            Some(first.as_str()),
            ReadRoute::Stay
        )
        .await,
        (first.clone(), false)
    );

    // The sticky standby fails: the other one takes over.
    set_healthy(&server, &first, false);
    let (second, offloaded) = route(
        &server,
        &config,
        &session,
        false,
        Some(primary),
        ReadRoute::Offload,
    )
    .await;
    assert!(
        offloaded && second != first && second != primary,
        "{second}"
    );
    // No standby left: the primary serves the read.
    set_healthy(&server, &second, false);
    assert_eq!(
        route(
            &server,
            &config,
            &session,
            false,
            Some(primary),
            ReadRoute::Offload
        )
        .await,
        (primary.to_string(), false)
    );
}

#[tokio::test]
async fn sync_reads_use_only_synchronous_standbys() {
    let mut config = offload_config(StandbyReads::Sync);
    let server = ProxyServer::new(config.clone()).unwrap();
    for _ in 0..4 {
        let session = make_test_session();
        assert_eq!(
            route(
                &server,
                &config,
                &session,
                false,
                Some("pg-p:5432"),
                ReadRoute::Offload
            )
            .await,
            ("pg-s2:5432".to_string(), true)
        );
    }

    // No synchronous standby: reads stay on the primary.
    config.nodes[2].synchronous = false;
    let server = ProxyServer::new(config.clone()).unwrap();
    let session = make_test_session();
    assert_eq!(
        route(
            &server,
            &config,
            &session,
            false,
            Some("pg-p:5432"),
            ReadRoute::Offload
        )
        .await,
        ("pg-p:5432".to_string(), false)
    );

    // Patroni's sync_standby role counts as synchronous too.
    let member = |address: &str, role: &str| crate::primary_tracker::TopologyMember {
        name: address.to_string(),
        address: address.to_string(),
        role: role.to_string(),
        state: "streaming".to_string(),
        lag_bytes: None,
    };
    let members = [
        member("pg-s1:5432", "sync_standby"),
        member("pg-s2:5432", "replica"),
    ];
    assert!(ProxyServer::is_sync_standby(
        &config,
        &members,
        "pg-s1:5432"
    ));
    assert!(!ProxyServer::is_sync_standby(
        &config,
        &members,
        "pg-s2:5432"
    ));
}

/// A fake backend for one connection that answers each simple query with
/// `CommandComplete` + `ReadyForQuery`, or an error for queries containing
/// `refuse`, and reports the queries it saw once the proxy closes it.
async fn guc_backend(listener: tokio::net::TcpListener) -> Vec<String> {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut seen = Vec::new();
    loop {
        let mut header = [0u8; 5];
        if sock.read_exact(&mut header).await.is_err() {
            return seen;
        }
        let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        assert_eq!(header[0], b'Q');
        let sql = String::from_utf8_lossy(&body[..body.len() - 1]).to_string();
        let reply = if sql.contains("refuse") {
            [
                idle_frame(b'E', b"SERROR\0C42501\0Mrefused\0\0"),
                idle_frame(b'Z', b"I"),
            ]
            .concat()
        } else {
            [idle_frame(b'C', b"SET\0"), idle_frame(b'Z', b"I")].concat()
        };
        seen.push(sql);
        if sock.write_all(&reply).await.is_err() {
            return seen;
        }
    }
}

/// The standby connection is brought up to the session's tracked `SET`s
/// once per change (`RESET ALL` first when it carries an older set); a
/// standby that refuses a `SET` is dropped and the session's reads are
/// pinned to its home.
#[tokio::test]
async fn standby_connections_mirror_the_sessions_settings() {
    let config = offload_config(StandbyReads::Async);
    let server = ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let (client_raw, _client_peer) = pair().await;
    let mut client = ClientStream::Plain(client_raw);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let fake = tokio::spawn(guc_backend(listener));
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    conns.insert(
        addr.clone(),
        BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
    );

    let gucs = vec!["SET search_path TO app".to_string()];
    let mut reads = ReadRouting {
        gucs: &gucs,
        guc_epoch: 1,
        ..ON
    };
    // A fresh connection: the tracked SETs, no RESET ALL.
    assert!(
        ProxyServer::mirror_read_gucs(
            &mut client,
            &mut conns,
            &addr,
            &reads,
            &session,
            &state,
            &config
        )
        .await
    );
    // Same epoch: nothing to send.
    assert!(
        ProxyServer::mirror_read_gucs(
            &mut client,
            &mut conns,
            &addr,
            &reads,
            &session,
            &state,
            &config
        )
        .await
    );
    // The session changed its settings: start from the defaults again.
    let changed = vec![
        "SET search_path TO other".to_string(),
        "SET work_mem = '8MB'".to_string(),
    ];
    reads.gucs = &changed;
    reads.guc_epoch = 2;
    assert!(
        ProxyServer::mirror_read_gucs(
            &mut client,
            &mut conns,
            &addr,
            &reads,
            &session,
            &state,
            &config
        )
        .await
    );
    assert_eq!(conns[&addr].gucs_synced, Some(2));

    // A SET the standby refuses: the connection is dropped, reads pinned.
    let refused = vec!["SET app.refuse = '1'".to_string()];
    reads.gucs = &refused;
    reads.guc_epoch = 3;
    assert!(
        !ProxyServer::mirror_read_gucs(
            &mut client,
            &mut conns,
            &addr,
            &reads,
            &session,
            &state,
            &config
        )
        .await
    );
    assert!(!conns.contains_key(&addr));
    assert!(session
        .reads_pinned
        .load(std::sync::atomic::Ordering::Relaxed));

    assert_eq!(
        fake.await.unwrap(),
        vec![
            "SET search_path TO app",
            "RESET ALL",
            "SET search_path TO other",
            "SET work_mem = '8MB'",
            "RESET ALL",
            "SET app.refuse = '1'",
        ]
    );
}

/// The standby-safety of a named statement follows the registry's current
/// `Parse`: a redefinition is reclassified, an unknown name never offloads.
#[test]
fn named_statements_are_classified_from_the_registry() {
    let policy = TrReadPolicy::default();
    let parse = |name: &str, sql: &str| {
        let mut body = name.as_bytes().to_vec();
        body.push(0);
        body.extend_from_slice(sql.as_bytes());
        body.extend_from_slice(&[0, 0, 0]);
        bytes::Bytes::from(idle_frame(b'P', &body))
    };
    let mut memo = HashMap::new();
    let mut registry = HashMap::new();
    registry.insert("s1".to_string(), parse("s1", "SELECT * FROM t"));
    assert!(ProxyServer::named_standby_read(
        &mut memo, &registry, "s1", &policy
    ));
    registry.insert("s1".to_string(), parse("s1", "UPDATE t SET a = 1"));
    assert!(!ProxyServer::named_standby_read(
        &mut memo, &registry, "s1", &policy
    ));
    assert!(!ProxyServer::named_standby_read(
        &mut memo, &registry, "nope", &policy
    ));
}

/// An idle standby socket is reused; one whose backend closed is dropped
/// before the read (the idle watch covers only the home socket).
#[tokio::test]
async fn a_closed_standby_socket_is_dropped_before_reuse() {
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    let (live, _live_peer) = pair().await;
    conns.insert("live:5432".to_string(), BackendConn::new(live));
    ProxyServer::drop_if_unusable(&mut conns, "live:5432");
    assert!(conns.contains_key("live:5432"));

    let (dead, dead_peer) = pair().await;
    drop(dead_peer);
    conns.insert("dead:5432".to_string(), BackendConn::new(dead));
    // The FIN reaches the reactor asynchronously.
    for _ in 0..50 {
        ProxyServer::drop_if_unusable(&mut conns, "dead:5432");
        if !conns.contains_key("dead:5432") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!conns.contains_key("dead:5432"));
}

/// The home node follows the session's non-offloaded requests only, and a
/// once-mirrored socket is re-synchronised only while reads are offloaded.
#[tokio::test]
async fn home_tracking_and_stale_mirrors() {
    let config = offload_config(StandbyReads::Async);
    let server = ProxyServer::new(config.clone()).unwrap();
    let session = make_test_session();
    let mut home = Some("pg-p:5432".to_string());
    *session.read_node.lock() = Some("pg-s1:5432".to_string());
    ProxyServer::note_home(
        &mut home,
        Some("pg-s1:5432"),
        &session,
        &server.state,
        &config,
    );
    assert_eq!(home.as_deref(), Some("pg-p:5432"), "an offloaded read");
    ProxyServer::note_home(
        &mut home,
        Some("pg-s2:5432"),
        &session,
        &server.state,
        &config,
    );
    assert_eq!(
        home.as_deref(),
        Some("pg-s2:5432"),
        "a hinted request moves home"
    );

    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    let (s, _peer) = pair().await;
    let mut bc = BackendConn::new(s);
    bc.gucs_synced = Some(1);
    conns.insert("pg-s1:5432".to_string(), bc);
    let gucs = vec!["SET search_path TO app".to_string()];
    let reads = ReadRouting {
        gucs: &gucs,
        guc_epoch: 2,
        ..ON
    };
    assert!(ProxyServer::mirror_stale(&conns, "pg-s1:5432", &reads));
    assert!(!ProxyServer::mirror_stale(
        &conns,
        "pg-s1:5432",
        &ReadRouting::OFF
    ));
    let current = ReadRouting {
        guc_epoch: 1,
        ..reads
    };
    assert!(!ProxyServer::mirror_stale(&conns, "pg-s1:5432", &current));
    ProxyServer::mirror_now_home(&mut conns, "pg-s1:5432");
    assert!(!ProxyServer::mirror_stale(&conns, "pg-s1:5432", &reads));
}

/// A session that logged in as `user` to `database`.
async fn session_as(user: &str, database: &str) -> Arc<ClientSession> {
    let session = make_test_session();
    let mut vars = session.variables.write().await;
    vars.insert("user".to_string(), user.to_string());
    vars.insert("database".to_string(), database.to_string());
    drop(vars);
    session
}

fn auth_refusal() -> ProxyError {
    ProxyError::Auth("backend requires authentication".to_string())
}

/// With pass-through auth a session holds no credential for a second
/// backend: a standby that wants a password refuses its login. The read falls
/// back home, the session stops offloading, and other credential-less
/// sessions of that login skip that standby; other logins (the standby may
/// trust them) and sessions with a credential may still use it, and the node
/// is not treated as failed.
#[tokio::test]
async fn a_standby_refusing_a_credential_less_login_is_skipped_for_it() {
    let config = offload_config(StandbyReads::Async);
    let server = ProxyServer::new(config.clone()).unwrap();
    let app = ("app".to_string(), "shop".to_string());
    let session = session_as("app", "shop").await;
    assert_eq!(
        ProxyServer::refusable_login(&server.state, &session).await,
        None,
        "nothing refused yet: routing does not build the login"
    );
    ProxyServer::offload_refused(&server.state, &session, "pg-s1:5432", &auth_refusal()).await;
    assert!(session
        .reads_pinned
        .load(std::sync::atomic::Ordering::Relaxed));
    assert!(server.state.offload_refusals.refused("pg-s1:5432", &app));
    assert!(server.state.health.load_full()["pg-s1:5432"].healthy);
    assert_eq!(
        server
            .state
            .metrics
            .read_mirror_failures
            .load(Ordering::Relaxed),
        1
    );
    for _ in 0..6 {
        let other = session_as("app", "shop").await;
        assert_eq!(
            route(
                &server,
                &config,
                &other,
                false,
                Some("pg-p:5432"),
                ReadRoute::Offload
            )
            .await,
            ("pg-s2:5432".to_string(), true)
        );
    }
    assert!(!ProxyServer::still_read_target(
        &server.state,
        &config,
        "pg-s1:5432",
        false,
        Some(&app)
    ));

    // Another login (another user, or the same user on another database) and
    // a session holding a credential still offload to pg-s1.
    let mut seen = HashSet::new();
    for (user, database) in [("report", "shop"), ("app", "audit")] {
        for _ in 0..6 {
            let other = session_as(user, database).await;
            seen.insert(
                route(
                    &server,
                    &config,
                    &other,
                    false,
                    Some("pg-p:5432"),
                    ReadRoute::Offload,
                )
                .await,
            );
        }
    }
    assert!(seen.contains(&("pg-s1:5432".to_string(), true)), "{seen:?}");
    let with_password = session_as("app", "shop").await;
    *with_password.backend_credential.write().await = Some("secret".to_string());
    assert_eq!(
        ProxyServer::refusable_login(&server.state, &with_password).await,
        None
    );
    assert!(ProxyServer::still_read_target(
        &server.state,
        &config,
        "pg-s1:5432",
        false,
        None
    ));
    // A refusal despite a credential pins that session only.
    ProxyServer::offload_refused(&server.state, &with_password, "pg-s2:5432", &auth_refusal())
        .await;
    assert!(with_password
        .reads_pinned
        .load(std::sync::atomic::Ordering::Relaxed));
    assert!(!server.state.offload_refusals.refused("pg-s2:5432", &app));

    // Any other failure does not pin the session.
    let unlucky = session_as("app", "shop").await;
    ProxyServer::offload_refused(
        &server.state,
        &unlucky,
        "pg-s2:5432",
        &ProxyError::Network("connection refused".to_string()),
    )
    .await;
    assert!(!unlucky
        .reads_pinned
        .load(std::sync::atomic::Ordering::Relaxed));
    assert!(!server.state.offload_refusals.refused("pg-s2:5432", &app));
}

/// A SIGHUP reload forgets the refusals (the standbys' auth may have changed
/// with the config); a reload that fails to parse keeps them.
#[tokio::test]
async fn a_sighup_reload_retries_refused_standby_logins() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("proxy.toml");
    let config_file = "[[nodes]]\nhost = \"pg-p\"\nport = 5432\nrole = \"primary\"\n";
    std::fs::write(&path, config_file).unwrap();
    let server = ProxyServer::new(offload_config(StandbyReads::Async))
        .unwrap()
        .with_config_path(Some(path.to_string_lossy().into_owned()));
    let app = ("app".to_string(), "shop".to_string());
    server.state.offload_refusals.insert("pg-s1:5432", &app);

    std::fs::write(&path, "[[nodes]]\nhost = ").unwrap();
    server.reload_config().await;
    assert!(server.state.offload_refusals.refused("pg-s1:5432", &app));

    std::fs::write(&path, config_file).unwrap();
    server.reload_config().await;
    assert!(!server.state.offload_refusals.any());
    assert!(!server.state.offload_refusals.refused("pg-s1:5432", &app));
}
