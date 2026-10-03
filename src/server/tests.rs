use super::*;
#[cfg(not(feature = "wasm-plugins"))]
use crate::protocol::QueryMessage;

fn test_config() -> ProxyConfig {
    let mut config = ProxyConfig {
        listen_address: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    config.add_node("127.0.0.1:5432", "primary").unwrap();
    config
}

// ---- single-pass statement facts ----
mod stmt_facts {
    use super::super::stmt_fact_classifications;
    use super::{ProxyServer, StmtFacts};

    /// Statement table spanning every classifier branch the facts
    /// memoize: reads, CTEs, all DML verbs, DDL, COPY in both
    /// directions, session-state verbs, transaction control, PREPARE /
    /// EXECUTE / LISTEN, multi-statement strings, leading and hint
    /// comments, volatile and locking reads, `SELECT ... INTO`, and case
    /// / whitespace variants.
    const CASES: [&str; 48] = [
        "SELECT 1",
        "select v from t",
        "  \t\n SELECT v FROM t  ",
        "SELECT v FROM t;",
        "SELECT v FROM t ; ",
        "SELECT v FROM t WHERE into_total > 1",
        "SELECT * INTO tmp FROM t",
        "select now()",
        "SELECT random()",
        "select nextval('s')",
        "SELECT set_config('x','y',false)",
        "SELECT pg_advisory_lock(1)",
        "SELECT v FROM t FOR UPDATE",
        "SELECT v FROM t FOR SHARE",
        "WITH c AS (SELECT 1) SELECT * FROM c",
        "WITH c AS (SELECT 1) SELECT * INTO tmp FROM c",
        "VALUES (1),(2)",
        "TABLE t",
        "SHOW search_path",
        "EXPLAIN SELECT 1",
        "FETCH ALL FROM cur",
        "INSERT INTO t VALUES (1)",
        "insert into t (a) values (1)",
        "UPDATE t SET v = 1",
        "DELETE FROM t WHERE id = 1",
        "CREATE TABLE t (id int)",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN c int",
        "TRUNCATE t",
        "GRANT SELECT ON t TO r",
        "REVOKE SELECT ON t FROM r",
        "VACUUM ANALYZE t",
        "REINDEX TABLE t",
        "CLUSTER t",
        "COPY t FROM STDIN",
        "COPY t TO STDOUT",
        "SET search_path TO tenant_b",
        "set TimeZone = 'UTC'",
        "SET TRANSACTION READ ONLY",
        "RESET ALL",
        "DISCARD ALL",
        "PREPARE p AS SELECT 1",
        "EXECUTE p(1)",
        "LISTEN chan",
        "BEGIN",
        "COMMIT",
        "ROLLBACK TO SAVEPOINT sp1;",
        "SELECT v FROM t; UPDATE t SET v = 1",
    ];

    /// Extra strings that are not plain statements: empty, whitespace,
    /// leading block/line comments and a routing-hint comment. These
    /// exercise the "leading comment masks the verb" branches.
    const ODD_CASES: [&str; 7] = [
        "",
        "   ",
        "/* leading */ SELECT 1",
        "-- leading\nSELECT 1",
        "/*helios:route=primary*/ SELECT 1",
        "/*helios:route=primary*/ UPDATE t SET v = 1",
        "BEGIN; UPDATE t SET v = 1; COMMIT",
    ];

    /// Every getter must agree with the legacy classifier it memoizes —
    /// for every statement shape. This is the contract that makes
    /// threading the facts through the forward path a pure optimisation
    /// rather than a behaviour change.
    #[test]
    fn facts_agree_with_legacy_classifiers() {
        assert!(CASES.len() + ODD_CASES.len() >= 40);
        for sql in CASES.iter().chain(ODD_CASES.iter()).copied() {
            let mut f = StmtFacts::new(sql);
            assert_eq!(
                f.is_write(),
                ProxyServer::is_write_query(sql),
                "is_write mismatch for {sql:?}"
            );
            #[cfg(feature = "edge-proxy")]
            assert_eq!(
                f.has_interior_semicolon(),
                ProxyServer::stmt_has_interior_semicolon(sql),
                "has_interior_semicolon mismatch for {sql:?}"
            );
            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            assert_eq!(
                f.leaves_session_state(),
                ProxyServer::stmt_leaves_session_state(sql),
                "leaves_session_state mismatch for {sql:?}"
            );
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            assert_eq!(
                f.is_cacheable_read(),
                ProxyServer::is_cacheable_read_sql(sql),
                "is_cacheable_read mismatch for {sql:?}"
            );
        }
    }

    /// LAZINESS CONTRACT (the reason the memo is `Option`-celled rather
    /// than computed up front): building the facts — the one thing the
    /// forward path does for EVERY simple query — classifies nothing at
    /// all. In the stock configuration `skip_clean_reset`, the query cache
    /// and the edge proxy are all off, so no gate ever asks and the
    /// statement is never scanned by any of these classifiers.
    #[test]
    fn building_facts_classifies_nothing() {
        let before = stmt_fact_classifications();

        let facts = StmtFacts::new("SELECT a, b FROM t WHERE id = 1");
        let msg = crate::protocol::QueryMessage {
            query: "INSERT INTO t VALUES (1)".to_string(),
        }
        .encode();
        let from_msg = StmtFacts::of_query(&msg);
        // Reading the borrowed text is not a classification either.
        assert_eq!(facts.sql, "SELECT a, b FROM t WHERE id = 1");
        assert_eq!(from_msg.sql, "INSERT INTO t VALUES (1)");

        assert_eq!(
            stmt_fact_classifications(),
            before,
            "constructing StmtFacts must not run any classifier"
        );
    }

    /// …and once a gate does ask, the answer is computed exactly once no
    /// matter how many gates (or how many calls) consult it.
    #[test]
    fn each_fact_is_classified_at_most_once() {
        let sql = "SELECT v FROM t";
        let mut f = StmtFacts::new(sql);

        let before = stmt_fact_classifications();
        let first = f.is_write();
        let second = f.is_write();
        let third = f.is_write();
        assert_eq!(first, second);
        assert_eq!(first, third);
        assert_eq!(
            stmt_fact_classifications() - before,
            1,
            "is_write must be classified once, then memoized"
        );

        #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
        {
            let before = stmt_fact_classifications();
            let _ = f.leaves_session_state();
            let _ = f.leaves_session_state();
            assert_eq!(stmt_fact_classifications() - before, 1);
        }
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            let before = stmt_fact_classifications();
            let _ = f.is_cacheable_read();
            let _ = f.is_cacheable_read();
            assert_eq!(stmt_fact_classifications() - before, 1);
        }
        #[cfg(feature = "edge-proxy")]
        {
            let before = stmt_fact_classifications();
            let _ = f.has_interior_semicolon();
            let _ = f.has_interior_semicolon();
            assert_eq!(stmt_fact_classifications() - before, 1);
        }
    }

    /// The forward path rebuilds the facts when a routing-hint strip, a
    /// rewrite rule or the tenant transform replaced the SQL. The rebuild
    /// must describe the NEW text — i.e. clear the memo — otherwise the
    /// cache / edge / pool gates would consult facts derived from the
    /// pre-rewrite string. The hint-strip case is discriminating: a
    /// leading `helios:` comment masks the SELECT lead, so the fact flips.
    #[test]
    fn rebuilding_on_changed_sql_clears_the_memo() {
        let hinted = "/*helios:route=primary*/ SELECT v FROM t";
        let stripped = "SELECT v FROM t";

        let mut before = StmtFacts::new(hinted);
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        assert!(
            !before.is_cacheable_read(),
            "leading comment masks the SELECT lead"
        );
        #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
        assert!(before.leaves_session_state(), "…and the neutral lead too");
        let _ = before.is_write();

        // Rebuilt exactly as `forward_simple_query` does it, on the final
        // message: a fresh value, so nothing memoized from `hinted`
        // survives.
        let msg = crate::protocol::QueryMessage {
            query: stripped.to_string(),
        }
        .encode();
        let mut after = StmtFacts::of_query(&msg);
        assert_eq!(after.sql, stripped);
        let count_before = stmt_fact_classifications();
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        assert!(
            after.is_cacheable_read(),
            "the stripped SELECT is cacheable"
        );
        #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
        assert!(!after.leaves_session_state());
        assert!(!after.is_write());
        assert!(
            stmt_fact_classifications() > count_before,
            "the rebuilt facts must re-classify, not reuse the old answers"
        );
    }

    /// The multi-statement fact is exactly the interior-`;` rule the edge
    /// invalidation gate used to re-derive inline.
    #[cfg(feature = "edge-proxy")]
    #[test]
    fn interior_semicolon_only_counts_non_trailing() {
        for (sql, want) in [
            ("SELECT 1", false),
            ("SELECT 1;", false),
            ("SELECT 1 ; ", false),
            ("SELECT 1; SELECT 2", true),
            ("BEGIN; UPDATE t SET v = 1; COMMIT", true),
            ("", false),
        ] {
            assert_eq!(
                ProxyServer::stmt_has_interior_semicolon(sql),
                want,
                "{sql:?}"
            );
            assert_eq!(
                StmtFacts::new(sql).has_interior_semicolon(),
                want,
                "{sql:?}"
            );
        }
    }

    /// `of_query` borrows the SQL carried by a `Query` message, and a
    /// payload that is not a valid query cstring falls back to the empty
    /// statement — for which every classifier answers `false`, the same
    /// fallback each individual call site used before the facts existed.
    #[test]
    fn of_query_reads_the_message_payload() {
        let msg = crate::protocol::QueryMessage {
            query: "UPDATE t SET v = 1".to_string(),
        }
        .encode();
        let mut f = StmtFacts::of_query(&msg);
        assert_eq!(f.sql, "UPDATE t SET v = 1");
        assert!(f.is_write());

        let empty = crate::protocol::Message::new(
            crate::protocol::MessageType::Query,
            bytes::BytesMut::new(),
        );
        let mut f = StmtFacts::of_query(&empty);
        assert_eq!(f.sql, "");
        assert!(!f.is_write());
        #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
        assert!(!f.leaves_session_state());
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        assert!(!f.is_cacheable_read());
        #[cfg(feature = "edge-proxy")]
        assert!(!f.has_interior_semicolon());
    }
}

/// A connected loopback `TcpStream` pair, shared by the relay tests
/// below that need a real socket (so `try_read_buf`/`WouldBlock`
/// behaves as it does in production, unlike an in-memory duplex pipe).
async fn pair() -> (TcpStream, TcpStream) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let (accepted, connected) = tokio::join!(l.accept(), TcpStream::connect(addr));
    (accepted.unwrap().0, connected.unwrap())
}

#[test]
fn test_server_creation() {
    let config = test_config();
    let server = ProxyServer::new(config);
    assert!(server.is_ok());
}

#[test]
fn is_backend_fault_excludes_client_and_slow_query_errors() {
    // Real backend faults — these must demote the node in-band.
    assert!(ProxyServer::is_backend_fault(
        "Backend read error: connection reset"
    ));
    assert!(ProxyServer::is_backend_fault(
        "Backend write error: broken pipe"
    ));
    assert!(ProxyServer::is_backend_fault("Backend write timeout"));
    assert!(ProxyServer::is_backend_fault(
        "Failed to connect to 127.0.0.1:5432: Connection refused"
    ));
    // Not backend faults — a client-side problem, or a merely slow but
    // healthy query, must NEVER take a backend out of rotation cluster-wide.
    assert!(!ProxyServer::is_backend_fault("Backend read timeout"));
    assert!(!ProxyServer::is_backend_fault("Client write timeout"));
    assert!(!ProxyServer::is_backend_fault(
        "Client write error: broken pipe"
    ));
    // A backend READ timeout is exempt, but a backend read ERROR is a fault.
    assert!(!ProxyServer::is_backend_fault("Backend read timeout"));
    assert!(ProxyServer::is_backend_fault(
        "Backend read error: timed out"
    ));
}

#[test]
fn test_hba_addr_matches() {
    use std::net::IpAddr;
    let v4 = |s: &str| s.parse::<IpAddr>().unwrap();
    // "all" matches everything
    assert!(ProxyServer::hba_addr_matches("all", v4("203.0.113.7")));
    // CIDR membership
    assert!(ProxyServer::hba_addr_matches("10.0.0.0/8", v4("10.1.2.3")));
    assert!(!ProxyServer::hba_addr_matches("10.0.0.0/8", v4("11.1.2.3")));
    assert!(ProxyServer::hba_addr_matches(
        "127.0.0.1/32",
        v4("127.0.0.1")
    ));
    assert!(!ProxyServer::hba_addr_matches(
        "127.0.0.1/32",
        v4("127.0.0.2")
    ));
    // bare IP exact match
    assert!(ProxyServer::hba_addr_matches(
        "192.168.1.1",
        v4("192.168.1.1")
    ));
    assert!(!ProxyServer::hba_addr_matches(
        "192.168.1.1",
        v4("192.168.1.2")
    ));
    // IPv6 CIDR + /0 catch-all
    assert!(ProxyServer::hba_addr_matches("::1/128", v4("::1")));
    assert!(ProxyServer::hba_addr_matches("0.0.0.0/0", v4("8.8.8.8")));
}

#[test]
fn test_hba_admits() {
    use crate::config::{HbaAction, HbaRule};
    use std::net::IpAddr;
    let ip: IpAddr = "10.0.0.5".parse().unwrap();
    // No rules -> admit all
    assert!(ProxyServer::hba_admits(&[], ip, "bench", "benchdb"));
    // Reject a specific user, allow others (default admit)
    let rules = vec![HbaRule {
        action: HbaAction::Reject,
        user: "bench".into(),
        database: "all".into(),
        address: "all".into(),
    }];
    assert!(!ProxyServer::hba_admits(&rules, ip, "bench", "benchdb"));
    assert!(ProxyServer::hba_admits(&rules, ip, "alice", "benchdb"));
    // First match wins: allow bench from 10/8, reject everything else
    let rules = vec![
        HbaRule {
            action: HbaAction::Allow,
            user: "bench".into(),
            database: "all".into(),
            address: "10.0.0.0/8".into(),
        },
        HbaRule {
            action: HbaAction::Reject,
            user: "all".into(),
            database: "all".into(),
            address: "all".into(),
        },
    ];
    assert!(ProxyServer::hba_admits(&rules, ip, "bench", "benchdb"));
    assert!(!ProxyServer::hba_admits(
        &rules,
        "192.168.0.1".parse().unwrap(),
        "bench",
        "benchdb"
    ));
    assert!(!ProxyServer::hba_admits(&rules, ip, "alice", "benchdb"));
}

#[test]
fn test_initial_metrics() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();
    let metrics = server.metrics();
    assert_eq!(metrics.connections_accepted, 0);
    assert_eq!(metrics.queries_processed, 0);
}

#[tokio::test]
async fn test_session_creation() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();

    assert!(server.state.sessions.is_empty());
}

#[tokio::test]
async fn test_node_health_initialization() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();

    let health = server.state.health.load_full();
    assert!(!health.is_empty());

    for node_health in health.values() {
        assert!(node_health.healthy);
        assert_eq!(node_health.failure_count, 0);
    }
}

/// Build a minimal `ClientSession` for plugin-hook unit tests.
fn make_test_session() -> Arc<ClientSession> {
    let id = Uuid::new_v4();
    let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Arc::new(ClientSession {
        id,
        client_addr,
        client_ip_str: client_addr.ip().to_string(),
        session_id_str: id.to_string(),
        current_node: RwLock::new(None),
        in_transaction: std::sync::atomic::AtomicBool::new(false),
        copy_in_progress: std::sync::atomic::AtomicBool::new(false),
        last_rfq_status: std::sync::atomic::AtomicU8::new(b'I'),
        last_response_error: std::sync::atomic::AtomicBool::new(false),
        last_response_digest: std::sync::atomic::AtomicU64::new(0),
        last_response_unverifiable: std::sync::atomic::AtomicBool::new(false),
        tr_replay_tainted: std::sync::atomic::AtomicBool::new(false),
        backend_credential: RwLock::new(None),
        #[cfg(feature = "query-cache")]
        tx_cache_stage: std::sync::Mutex::new(TxCacheStage::default()),
        journal: std::sync::Mutex::new(crate::journal_capture::SessionCapture::default()),
        journal_armed: std::sync::atomic::AtomicBool::new(false),
        journal_open: std::sync::atomic::AtomicBool::new(false),
        tx_state: RwLock::new(TransactionState::default()),
        variables: RwLock::new(HashMap::new()),
        created_at: chrono::Utc::now(),
        tr_mode: crate::config::TrMode::default(),
        #[cfg(feature = "lag-routing")]
        last_write_at: RwLock::new(None),
        #[cfg(feature = "pool-modes")]
        pool_client_id: crate::pool::lease::ClientId::default(),
        #[cfg(feature = "wasm-plugins")]
        plugin_identity: RwLock::new(None),
        #[cfg(feature = "edge-proxy")]
        edge_ineligible: std::sync::atomic::AtomicBool::new(false),
        #[cfg(feature = "edge-proxy")]
        pending_edge_copy_tables: std::sync::Mutex::new(None),
        #[cfg(feature = "rate-limiting")]
        rate_limit_key: std::sync::OnceLock::new(),
    })
}

/// The per-query analytics path reads pre-rendered client-IP and session-id
/// strings off the session instead of formatting an `IpAddr`/`Uuid` on every
/// query. They must match what the old per-query formatting produced.
#[test]
fn test_session_caches_client_ip_and_id_strings() {
    let session = make_test_session();
    assert_eq!(session.client_ip_str, session.client_addr.ip().to_string());
    assert_eq!(session.session_id_str, session.id.to_string());
    assert!(!session.session_id_str.is_empty());
}

/// With no plugin manager attached, `apply_route_hook` must be a
/// zero-cost `None` return so the default SQL-verb routing applies.
/// Verifies the feature-gated early-return path.
#[tokio::test]
async fn test_apply_route_hook_no_plugin_manager_returns_none() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();

    let msg = QueryMessage {
        query: "SELECT * FROM users".to_string(),
    }
    .encode();

    let decision = ProxyServer::apply_route_hook(&msg, &server.state, &session);
    assert!(matches!(decision, RouteOverride::None));
}

/// Same invariant for the pre-query hook: without a plugin manager,
/// `apply_pre_query_hook` must return the message unchanged with
/// `PreQueryAction::Forward`.
#[tokio::test]
async fn test_apply_pre_query_hook_no_plugin_manager_forwards() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();

    let original = QueryMessage {
        query: "SELECT 1".to_string(),
    }
    .encode();
    let original_bytes = original.encode().to_vec();

    let (msg_out, action) = ProxyServer::apply_pre_query_hook(original, &server.state, &session);

    assert!(matches!(action, PreQueryAction::Forward));
    // The message must survive the hook byte-for-byte when no plugins run.
    assert_eq!(msg_out.encode().to_vec(), original_bytes);
}

/// Non-Query message types (e.g., extended-protocol Parse/Execute) must
/// bypass the Route hook entirely regardless of plugin state, because
/// we haven't wired SQL extraction for those variants yet.
#[tokio::test]
async fn test_apply_route_hook_skips_non_query_messages() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();

    let sync_msg = Message::empty(MessageType::Sync);
    let decision = ProxyServer::apply_route_hook(&sync_msg, &server.state, &session);
    assert!(matches!(decision, RouteOverride::None));
}

/// By default, `[plugins].enabled = false`, so `init_plugin_manager`
/// short-circuits without touching the filesystem or wasmtime and
/// returns `None`. The proxy starts normally whether or not a plugin
/// directory exists on the host.
#[cfg(feature = "wasm-plugins")]
#[test]
fn test_init_plugin_manager_disabled_by_default_returns_none() {
    let config = test_config();
    assert!(!config.plugins.enabled);
    let pm = ProxyServer::init_plugin_manager(&config.plugins);
    assert!(pm.is_none());
}

/// Plugins enabled but pointing at a directory that doesn't exist
/// must still initialise the manager (so new plugins can be hot-
/// loaded later) and log a warning — it must NOT fail startup.
#[cfg(feature = "wasm-plugins")]
#[test]
fn test_init_plugin_manager_missing_dir_logs_warning() {
    let mut config = test_config();
    config.plugins.enabled = true;
    config.plugins.plugin_dir = "/definitely/not/a/real/path".to_string();

    // Manager is created; no panic; Some(pm) returned even with empty dir.
    let pm = ProxyServer::init_plugin_manager(&config.plugins);
    assert!(pm.is_some());
}

/// With no plugin manager attached, `apply_authenticate_hook` is a
/// zero-cost `Ok(())` that leaves session identity unset — the
/// default PG auth flow applies.
#[tokio::test]
async fn test_apply_authenticate_hook_no_plugin_manager_defers() {
    let config = test_config();
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();

    let mut params = HashMap::new();
    params.insert("user".to_string(), "alice".to_string());
    params.insert("database".to_string(), "app".to_string());

    let result = ProxyServer::apply_authenticate_hook(&params, &session, &server.state).await;
    assert!(result.is_ok());

    // No plugin → no identity stored.
    #[cfg(feature = "wasm-plugins")]
    {
        let ident = session.plugin_identity.read().await;
        assert!(ident.is_none());
    }
}

/// Cached-response synthesis round-trip: a well-formed plugin
/// payload must produce concatenated wire frames in the order
/// `T D D C Z`. We inspect the raw tag bytes directly because
/// `MessageType::from_tag` conflates server→client DataRow (`'D'`)
/// with client→server Describe (same byte) — a known quirk of the
/// shared `MessageType` enum that the real proxy side-steps by
/// knowing the direction at the call site.
#[cfg(feature = "wasm-plugins")]
#[test]
fn test_synthesise_cached_response_roundtrip() {
    let payload = br#"{
            "columns": [
                {"name": "id",    "oid": 23},
                {"name": "email", "oid": 25}
            ],
            "rows": [
                ["1", "alice@example.com"],
                ["2", null]
            ]
        }"#;
    let reply = ProxyServer::synthesise_cached_response(payload).expect("synthesis");

    // Walk the concatenation frame-by-frame via length prefixes.
    // Each PG message: tag(1) + length(4, big-endian, includes self) + payload.
    let mut tags = Vec::new();
    let mut i = 0;
    while i < reply.len() {
        let tag = reply[i];
        let len =
            u32::from_be_bytes([reply[i + 1], reply[i + 2], reply[i + 3], reply[i + 4]]) as usize;
        tags.push(tag);
        i += 1 + len;
    }
    assert_eq!(i, reply.len(), "no trailing bytes");
    assert_eq!(tags, vec![b'T', b'D', b'D', b'C', b'Z'], "wire frame order");

    // Spot-check the final ReadyForQuery payload is 'I' (idle).
    assert_eq!(*reply.last().unwrap(), b'I');
}

/// Row width mismatch between columns and row data is rejected so
/// the plugin author can't produce ambiguous wire frames.
#[cfg(feature = "wasm-plugins")]
#[test]
fn test_synthesise_cached_response_rejects_row_width_mismatch() {
    let payload = br#"{
            "columns": [{"name": "id", "oid": 23}, {"name": "name", "oid": 25}],
            "rows": [["1", "alice", "extra"]]
        }"#;
    let result = ProxyServer::synthesise_cached_response(payload);
    assert!(matches!(result, Err(ProxyError::Protocol(_))));
}

/// Empty payload (no columns) is rejected — a RowDescription with
/// zero columns is technically valid PG but useless and likely a
/// plugin bug.
#[cfg(feature = "wasm-plugins")]
#[test]
fn test_synthesise_cached_response_rejects_empty_columns() {
    let payload = br#"{ "columns": [], "rows": [] }"#;
    let result = ProxyServer::synthesise_cached_response(payload);
    assert!(matches!(result, Err(ProxyError::Protocol(_))));
}

/// Malformed JSON must return a Protocol error, not panic. The
/// caller treats this as "fall back to backend."
#[cfg(feature = "wasm-plugins")]
#[test]
fn test_synthesise_cached_response_rejects_bad_json() {
    let payload = b"not json at all";
    let result = ProxyServer::synthesise_cached_response(payload);
    assert!(matches!(result, Err(ProxyError::Protocol(_))));
}

/// Denied by plugin surfaces as `ProxyError::Auth` so the existing
/// error-response path in `handle_client` writes an ErrorResponse
/// and closes the connection. Here we prove the error variant
/// when the plugin manager is present but denies. We build a
/// PluginManager with no plugins loaded — so it defers — and
/// verify the Ok path. (Denial path requires an actual
/// auth-plugin `.wasm`; covered by the plugin unit tests in
/// `plugins::tests`.)
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn test_apply_authenticate_hook_with_manager_no_plugins_defers() {
    use crate::plugins::{PluginManager, PluginRuntimeConfig};

    let config = test_config();
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();

    // Synthesise a state with a real PluginManager but zero
    // registered plugins — every hook must defer.
    let pm = Arc::new(PluginManager::new(PluginRuntimeConfig::default()).unwrap());
    #[cfg(feature = "edge-proxy")]
    let edge_defaults = crate::edge::EdgeConfig::default();
    let augmented_state = Arc::new(ServerState {
        limits: ResolvedLimits::default(),
        client_slots: None,
        sessions: DashMap::new(),
        health: ArcSwap::from_pointee(HashMap::new()),
        health_write: parking_lot::Mutex::new(()),
        live_config: ArcSwap::from_pointee(ProxyConfig::default()),
        metrics: ServerMetrics::default(),
        cancel_map: Arc::new(DashMap::new()),
        cancel_order: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
        tls_acceptor: None,
        auth_file: None,
        mirror: None,
        cutover: Arc::new(ArcSwap::from_pointee(None)),
        primary_tracker: Arc::new(PrimaryTracker::new_standalone()),
        authoritative_topology: false,
        topology_poller: TopologyPoller::Static,
        lb_state: LoadBalancerState {
            rr_counter: AtomicU64::new(0),
        },
        #[cfg(feature = "routing-hints")]
        hint_parser: None,
        #[cfg(feature = "rate-limiting")]
        rate_limiter: None,
        #[cfg(feature = "circuit-breaker")]
        circuit_breaker: None,
        #[cfg(feature = "query-analytics")]
        analytics: None,
        #[cfg(feature = "query-cache")]
        query_cache: None,
        #[cfg(feature = "query-rewriting")]
        rewriter: None,
        #[cfg(feature = "multi-tenancy")]
        tenant_manager: None,
        #[cfg(feature = "schema-routing")]
        schema_analyzer: None,
        #[cfg(feature = "pool-modes")]
        pool_manager: None,
        #[cfg(feature = "pool-modes")]
        backend_pool: None,
        plugin_manager: Some(pm),
        transaction_journal: Arc::new(crate::transaction_journal::TransactionJournal::new()),
        tr_read_policy: Arc::new(TrReadPolicy::default()),
        #[cfg(feature = "anomaly-detection")]
        anomaly_detector: Arc::new(crate::anomaly::AnomalyDetector::new(
            ProxyConfig::default().anomaly.to_anomaly_config(),
        )),
        #[cfg(feature = "edge-proxy")]
        edge_cache: Arc::new(crate::edge::EdgeCache::new(
            edge_defaults.max_entries.max(1),
        )),
        #[cfg(feature = "edge-proxy")]
        edge_registry: Arc::new(crate::edge::EdgeRegistry::new(
            edge_defaults.max_edges,
            std::time::Duration::from_secs(edge_defaults.liveness_window_secs),
        )),
    });

    let mut params = HashMap::new();
    params.insert("user".to_string(), "alice".to_string());

    let result = ProxyServer::apply_authenticate_hook(&params, &session, &augmented_state).await;
    assert!(result.is_ok());
    let ident = session.plugin_identity.read().await;
    assert!(ident.is_none());
    // Unused bindings for the sync-state build path.
    let _ = server;
}

// ---- Batch F.4: prepared-statement tracking across backend switches ----

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

#[test]
fn parse_stmt_name_extracts_named_and_unnamed() {
    // Parse payload = stmt-name cstring + query cstring + int16 nparams.
    let mut named = cstr("ps1");
    named.extend_from_slice(&cstr("SELECT 1"));
    named.extend_from_slice(&[0, 0]);
    assert_eq!(ProxyServer::parse_stmt_name(&named), "ps1");

    let mut unnamed = cstr("");
    unnamed.extend_from_slice(&cstr("SELECT 1"));
    unnamed.extend_from_slice(&[0, 0]);
    assert_eq!(ProxyServer::parse_stmt_name(&unnamed), "");
}

#[test]
fn bind_stmt_ref_reads_second_cstring() {
    // Bind payload = portal cstring + statement cstring + ...
    let mut named = cstr("portal_a");
    named.extend_from_slice(&cstr("ps1"));
    named.extend_from_slice(&[0, 0]); // 0 param-format codes, 0 params
    assert_eq!(ProxyServer::bind_stmt_ref(&named), Some("ps1"));

    // Unnamed statement (empty second cstring) is not tracked.
    let mut unnamed = cstr("");
    unnamed.extend_from_slice(&cstr(""));
    assert_eq!(ProxyServer::bind_stmt_ref(&unnamed), None);
}

#[test]
fn stmt_kind_name_only_matches_statement_kind() {
    // Describe/Close 'S' (statement) carries a trackable name.
    let mut stmt = vec![b'S'];
    stmt.extend_from_slice(&cstr("ps1"));
    assert_eq!(ProxyServer::stmt_kind_name(&stmt), Some("ps1"));

    // 'P' (portal) is not a statement reference.
    let mut portal = vec![b'P'];
    portal.extend_from_slice(&cstr("portal_a"));
    assert_eq!(ProxyServer::stmt_kind_name(&portal), None);

    // Statement-kind but unnamed -> nothing to track.
    let mut empty = vec![b'S'];
    empty.extend_from_slice(&cstr(""));
    assert_eq!(ProxyServer::stmt_kind_name(&empty), None);
}

#[tokio::test]
async fn read_one_frame_type_consumes_full_frame() {
    // ParseComplete '1' with empty body, followed by a second frame to
    // prove only the first frame is consumed.
    let (mut a, mut b) = tokio::io::duplex(64);
    // frame 1: '1' + len(4) + no body; frame 2: 'Z' + len(5) + 'I'.
    let bytes = [b'1', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'];
    b.write_all(&bytes).await.unwrap();
    let t = ProxyServer::read_one_frame_type(&mut a, usize::MAX)
        .await
        .unwrap();
    assert_eq!(t, b'1');
    // The next frame's type byte is still readable -> we stopped cleanly.
    let t2 = ProxyServer::read_one_frame_type(&mut a, usize::MAX)
        .await
        .unwrap();
    assert_eq!(t2, b'Z');
}

#[tokio::test]
async fn reprepare_statement_accepts_parse_complete_and_rejects_error() {
    // Backend answers ParseComplete -> Ok.
    let (mut client, mut backend) = tokio::io::duplex(64);
    backend.write_all(&[b'1', 0, 0, 0, 4]).await.unwrap();
    let parse = {
        let mut p = vec![b'P', 0, 0, 0, 0];
        p.extend_from_slice(&cstr("ps1"));
        p.extend_from_slice(&cstr("SELECT 1"));
        p.extend_from_slice(&[0, 0]);
        p
    };
    assert!(ProxyServer::reprepare_statement(
        &mut client,
        &parse,
        Duration::from_secs(15),
        usize::MAX
    )
    .await
    .is_ok());

    // Backend answers ErrorResponse -> Err.
    let (mut client2, mut backend2) = tokio::io::duplex(64);
    backend2.write_all(&[b'E', 0, 0, 0, 4]).await.unwrap();
    assert!(ProxyServer::reprepare_statement(
        &mut client2,
        &parse,
        Duration::from_secs(15),
        usize::MAX
    )
    .await
    .is_err());
}

// ---- routing-hints: SQL-comment hint → RouteOverride mapping ----

#[cfg(feature = "routing-hints")]
mod routing_hints {
    use super::*;
    use crate::routing::HintParser;

    fn over(sql: &str) -> RouteOverride {
        let hints = HintParser::new().parse(sql);
        ProxyServer::hint_to_override(&hints)
    }

    #[test]
    fn route_primary_maps_to_primary() {
        assert!(matches!(
            over("/*helios:route=primary*/ SELECT 1"),
            RouteOverride::Primary
        ));
    }

    #[test]
    fn read_tier_targets_map_to_standby() {
        for t in ["standby", "sync", "semisync", "async", "local"] {
            assert!(
                matches!(
                    over(&format!("/*helios:route={t}*/ SELECT 1")),
                    RouteOverride::Standby
                ),
                "route={t} should map to Standby"
            );
        }
    }

    #[test]
    fn any_and_vector_impose_no_constraint() {
        assert!(matches!(
            over("/*helios:route=any*/ SELECT 1"),
            RouteOverride::None
        ));
        assert!(matches!(
            over("/*helios:route=vector*/ SELECT 1"),
            RouteOverride::None
        ));
    }

    #[test]
    fn node_hint_maps_to_node_and_wins_over_route() {
        // node= beats route= (precedence).
        match over("/*helios:node=pg-standby,route=primary*/ SELECT 1") {
            RouteOverride::Node(n) => assert_eq!(n, "pg-standby"),
            other => panic!("expected Node, got {other:?}"),
        }
    }

    #[test]
    fn consistency_strong_forces_primary() {
        assert!(matches!(
            over("/*helios:consistency=strong*/ SELECT 1"),
            RouteOverride::Primary
        ));
    }

    #[test]
    fn no_hint_yields_none() {
        assert!(matches!(over("SELECT 1"), RouteOverride::None));
    }

    // The core correctness fix: a leading hint comment must NOT hide the
    // verb from write-detection. Raw classification misfires; classifying
    // on the stripped SQL is correct.
    #[test]
    fn write_verb_classified_after_strip() {
        let parser = HintParser::new();
        let raw = "/*helios:route=primary*/ INSERT INTO t VALUES (1)";
        // Raw (unstripped) wrongly looks like a read because it starts
        // with the comment.
        assert!(!ProxyServer::is_write_query(raw));
        // Stripped is correctly a write.
        assert!(ProxyServer::is_write_query(&parser.strip(raw)));
    }

    #[test]
    fn strip_removes_hint_comment() {
        let parser = HintParser::new();
        assert_eq!(
            parser.strip("/*helios:route=standby*/ SELECT 42"),
            "SELECT 42"
        );
    }
}

// ---- rate-limiting: the burst-then-deny contract the gate relies on ----

#[cfg(feature = "rate-limiting")]
mod rate_limiting {
    use crate::rate_limit::{LimiterKey, RateLimitConfig, RateLimitResult, RateLimiter};

    #[test]
    fn burst_allows_then_denies() {
        // Mirror the wiring's config conversion: tiny bucket, reject on
        // exceed (the engine default).
        let cfg = RateLimitConfig {
            enabled: true,
            default_qps: 1,
            default_burst: 2,
            ..Default::default()
        };
        let limiter = RateLimiter::new(cfg);
        let key = LimiterKey::User("u".to_string());

        // The first `burst` checks are admitted.
        assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));
        assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));

        // Rapid over-burst checks must produce at least one hard denial.
        let mut denied = false;
        for _ in 0..5 {
            if matches!(limiter.check(&key, 1), RateLimitResult::Denied(_)) {
                denied = true;
            }
        }
        assert!(denied, "over-burst checks must yield a Denied verdict");
    }

    /// The per-session bucket key is resolved once and then reused: two
    /// gate invocations must hand back the *same* memoized value, not a
    /// freshly built one (the whole point of the cache — no key alloc, no
    /// `variables` read lock, no metrics `format!` per query).
    #[tokio::test]
    async fn session_key_is_memoized_after_startup_params() {
        use crate::config::RateLimitKeyBy;

        let mut cfg = super::test_config();
        cfg.rate_limit.key_by = RateLimitKeyBy::User;

        let session = super::make_test_session();
        {
            let mut vars = session.variables.write().await;
            vars.insert("user".into(), "alice".into());
        }

        let first = super::ProxyServer::rate_limit_key(&session, &cfg).await;
        assert_eq!(first.as_ref().to_string(), "user:alice");
        drop(first);

        assert!(
            session.rate_limit_key.get().is_some(),
            "a resolvable key must be cached on the session"
        );

        // The second call must borrow the memoized value rather than
        // rebuild one.
        let second = super::ProxyServer::rate_limit_key(&session, &cfg).await;
        assert!(
            matches!(second, std::borrow::Cow::Borrowed(_)),
            "key was rebuilt instead of reused"
        );
        assert_eq!(second.as_ref().to_string(), "user:alice");
    }

    /// Before the startup parameters land the key must NOT be memoized —
    /// otherwise a placeholder (`user:`) would be frozen for the whole
    /// session. The pre-startup verdict is byte-identical to the old
    /// recompute-every-time behavior.
    #[tokio::test]
    async fn key_is_not_memoized_before_startup_params() {
        use crate::config::RateLimitKeyBy;

        let mut cfg = super::test_config();
        cfg.rate_limit.key_by = RateLimitKeyBy::Database;

        let session = super::make_test_session();

        let early = super::ProxyServer::rate_limit_key(&session, &cfg).await;
        assert_eq!(early.as_ref().to_string(), "db:");
        assert!(
            matches!(early, std::borrow::Cow::Owned(_)),
            "a placeholder key must not be served from the cache"
        );
        drop(early);
        assert!(
            session.rate_limit_key.get().is_none(),
            "a placeholder key must never be cached"
        );

        {
            let mut vars = session.variables.write().await;
            vars.insert("database".into(), "shop".into());
        }

        let later = super::ProxyServer::rate_limit_key(&session, &cfg).await;
        assert_eq!(later.as_ref().to_string(), "db:shop");
        drop(later);
        assert!(session.rate_limit_key.get().is_some());
    }

    /// Keying dimensions that do not read session variables are cached on
    /// the very first call, and render exactly as before.
    #[tokio::test]
    async fn variable_free_keys_are_cached_immediately() {
        use crate::config::RateLimitKeyBy;

        for (key_by, expected) in [
            (RateLimitKeyBy::Global, "global"),
            (RateLimitKeyBy::ClientIp, "ip:127.0.0.1"),
        ] {
            let mut cfg = super::test_config();
            cfg.rate_limit.key_by = key_by;

            let session = super::make_test_session();
            let key = super::ProxyServer::rate_limit_key(&session, &cfg).await;
            assert_eq!(key.as_ref().to_string(), expected);
            drop(key);
            assert!(session.rate_limit_key.get().is_some());
        }
    }

    #[test]
    fn distinct_keys_have_independent_buckets() {
        let cfg = RateLimitConfig {
            enabled: true,
            default_qps: 1,
            default_burst: 1,
            ..Default::default()
        };
        let limiter = RateLimiter::new(cfg);
        // Each user gets its own bucket: both first checks are admitted.
        assert!(matches!(
            limiter.check(&LimiterKey::User("a".to_string()), 1),
            RateLimitResult::Allowed
        ));
        assert!(matches!(
            limiter.check(&LimiterKey::User("b".to_string()), 1),
            RateLimitResult::Allowed
        ));
    }
}

// ---- circuit-breaker: open-after-threshold contract the gate relies on ----

#[cfg(feature = "circuit-breaker")]
mod circuit_breaker {
    use crate::circuit_breaker::{
        CircuitBreakerConfig, CircuitBreakerManager, CircuitState, ManagerConfig,
    };
    use std::time::Duration;

    fn mgr(threshold: u32) -> CircuitBreakerManager {
        let cfg = CircuitBreakerConfig {
            failure_threshold: threshold,
            cooldown: Duration::from_secs(10),
            ..Default::default()
        };
        CircuitBreakerManager::new(ManagerConfig::new(cfg))
    }

    #[test]
    fn opens_after_threshold_failures() {
        let m = mgr(3);
        let b = m.get_breaker("n1");
        assert_eq!(b.get_state(), CircuitState::Closed);
        b.record_failure("boom");
        b.record_failure("boom");
        // Under threshold: still serving.
        assert_eq!(b.get_state(), CircuitState::Closed);
        // Threshold reached: tripped open.
        b.record_failure("boom");
        assert_eq!(b.get_state(), CircuitState::Open);
    }

    #[test]
    fn healthy_node_stays_closed() {
        let m = mgr(3);
        let b = m.get_breaker("n2");
        b.record_success();
        b.record_success();
        assert_eq!(b.get_state(), CircuitState::Closed);
    }
}

// ---- query-analytics: record + literal-collapsing normalizer ----

#[cfg(feature = "query-analytics")]
mod query_analytics {
    use crate::analytics::{AnalyticsConfig, OrderBy, QueryAnalytics, QueryExecution};
    use std::time::Duration;

    #[test]
    fn records_and_collapses_literals() {
        let a = QueryAnalytics::new(AnalyticsConfig::default());
        for n in [1, 2, 3] {
            a.record(QueryExecution::new(
                format!("select {n}"),
                Duration::from_millis(1),
            ));
        }
        let top = a.top_queries(OrderBy::Calls, 10);
        assert!(!top.is_empty(), "no fingerprints recorded");
        // The three literal variants collapse to one fingerprint (3 calls).
        assert!(
            top.iter().any(|s| s.calls >= 3),
            "literals did not collapse: {:?}",
            top.iter()
                .map(|s| (s.normalized.clone(), s.calls))
                .collect::<Vec<_>>()
        );
    }
}

// ---- lag-routing: read-your-writes window + lag-exclusion decisions ----

#[cfg(feature = "lag-routing")]
mod lag_routing {
    use super::ProxyServer;

    #[test]
    fn ryw_pins_recent_write() {
        // A write "now" falls inside a 1s window -> pin to primary.
        assert!(ProxyServer::ryw_pins_primary(
            Some(std::time::Instant::now()),
            1000
        ));
    }

    #[test]
    fn ryw_releases_old_write() {
        let old = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(10))
            .unwrap();
        assert!(!ProxyServer::ryw_pins_primary(Some(old), 1000));
    }

    #[test]
    fn ryw_no_write_or_disabled() {
        assert!(!ProxyServer::ryw_pins_primary(None, 1000));
        // window=0 disables read-your-writes entirely.
        assert!(!ProxyServer::ryw_pins_primary(
            Some(std::time::Instant::now()),
            0
        ));
    }

    #[test]
    fn lag_exclusion_thresholds() {
        // max=0 disables exclusion.
        assert!(!ProxyServer::lag_excludes_standby(Some(999_999), 0, false));
        // unknown lag never excludes unless the strict policy is on.
        assert!(!ProxyServer::lag_excludes_standby(None, 1000, false));
        assert!(ProxyServer::lag_excludes_standby(None, 1000, true));
        // within ceiling stays in rotation.
        assert!(!ProxyServer::lag_excludes_standby(Some(500), 1000, false));
        // beyond ceiling is dropped.
        assert!(ProxyServer::lag_excludes_standby(Some(2000), 1000, false));
    }
}

/// `provider = "patroni"` builds an authoritative, provider-backed
/// tracker with a Patroni poll task (H-01).
#[cfg(feature = "postgres-topology")]
#[test]
fn patroni_provider_makes_the_tracker_authoritative() {
    let mut config = ProxyConfig::default();
    config.topology.provider = crate::config::TopologyProviderKind::Patroni;
    config.topology.patroni_endpoints = vec!["http://127.0.0.1:8008".into()];
    let (tracker, authoritative, poller) = ProxyServer::build_primary_tracker(&config);
    assert!(authoritative);
    assert!(tracker.has_provider());
    assert!(matches!(poller, TopologyPoller::Patroni(_)));

    let (tracker, authoritative, poller) =
        ProxyServer::build_primary_tracker(&ProxyConfig::default());
    assert!(!authoritative);
    assert!(!tracker.has_provider());
    assert!(matches!(poller, TopologyPoller::Static));
}

// ---- query-cache: which read SQL is safe to cache ----

#[cfg(feature = "query-cache")]
mod query_cache {
    use super::ProxyServer;

    #[test]
    fn plain_selects_are_cacheable() {
        assert!(ProxyServer::is_cacheable_read_sql("select v from t"));
        assert!(ProxyServer::is_cacheable_read_sql(
            "  SELECT a, b FROM users WHERE id = 5"
        ));
    }

    #[test]
    fn writes_and_non_selects_are_not_cacheable() {
        assert!(!ProxyServer::is_cacheable_read_sql(
            "insert into t values (1)"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql("update t set v = 1"));
        assert!(!ProxyServer::is_cacheable_read_sql("show search_path"));
    }

    #[test]
    fn locking_and_volatile_selects_are_not_cacheable() {
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * from t for update"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql("select now()"));
        assert!(!ProxyServer::is_cacheable_read_sql("select random()"));
        assert!(!ProxyServer::is_cacheable_read_sql("select nextval('s')"));
        // set_config mutates GUCs + emits ParameterStatus — replaying
        // from cache would suppress the side effect.
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select set_config('timezone', 'UTC', false) from t"
        ));
    }

    #[test]
    fn multi_statement_strings_are_not_cacheable() {
        // Replaying `SELECT ...; UPDATE ...` would fabricate the
        // UPDATE's CommandComplete while executing nothing.
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select v from t; update t set v = 1"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql("select 1; select 2"));
        // A single trailing semicolon stays cacheable.
        assert!(ProxyServer::is_cacheable_read_sql("select v from t;"));
        assert!(ProxyServer::is_cacheable_read_sql("SELECT v FROM t ; "));
    }

    #[test]
    fn literal_semicolon_or_into_over_rejects_by_design() {
        // G7 (accepted, safe-direction): the multi-statement and SELECT INTO
        // guards scan the RAW text, so a ';' or the word "into" inside a
        // string literal disqualifies an otherwise-cacheable SELECT. This
        // over-rejection is DELIBERATE and hit-rate-only — the raw scan is
        // the sole defense against multi-statement replay fabrication (a
        // literal-stripping pre-pass would misjudge `'x\'; UPDATE ...'` under
        // standard_conforming_strings and reopen that hole), and a real
        // SELECT INTO is protocol-indistinguishable from a plain SELECT.
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select v from t where url = 'a;b=c'"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select v from t where body like '%go into space%'"
        ));
        // A real SELECT INTO (it creates a table) must never be cached.
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * into snapshot from t"
        ));
    }

    #[test]
    fn select_into_is_not_cacheable() {
        // SELECT ... INTO creates a table (CREATE TABLE AS synonym);
        // a cache replay would silently skip the DDL.
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * into report_tmp from src"
        ));
        // Word-boundary: newline/tab-delimited INTO is caught too.
        assert!(!ProxyServer::is_cacheable_read_sql(
            "SELECT *\nINTO report_tmp\nFROM src"
        ));
        // ...but an identifier merely containing "into" is not.
        assert!(ProxyServer::is_cacheable_read_sql(
            "select into_total from t"
        ));
    }

    /// Regression for the single-lowercase-pass rewrite of the FOR
    /// UPDATE/FOR SHARE + VOLATILE-token checks: every needle must still
    /// be matched case-insensitively regardless of how the caller casts
    /// the keyword, exactly as the old per-needle `contains_ci` scan did.
    #[test]
    fn locking_and_volatile_checks_stay_case_insensitive() {
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * from t FOR UPDATE"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * from t For Update"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * from t for share"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select * from t FOR SHARE"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql("SELECT NOW()"));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select CURRENT_TIMESTAMP"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "select GEN_RANDOM_UUID()"
        ));
        assert!(!ProxyServer::is_cacheable_read_sql(
            "SELECT Set_Config('timezone', 'UTC', false)"
        ));
    }

    /// Non-ASCII bytes in the SQL text must not panic the lowercasing
    /// buffer (`to_ascii_lowercase` only touches ASCII bytes, so UTF-8
    /// validity is preserved) and must not be case-folded — same
    /// byte-for-byte semantics as the old `contains_ci`.
    #[test]
    fn non_ascii_sql_is_handled_safely() {
        assert!(ProxyServer::is_cacheable_read_sql(
            "select name from café where city = 'Zürich'"
        ));
        // A non-ASCII volatile-token lookalike must not be flagged —
        // "NÓW(" is not "now(" under byte-for-byte comparison.
        assert!(ProxyServer::is_cacheable_read_sql("select NÓW() from t"));
    }
}

// ---- edge-proxy: write-invalidation classifiers ----

#[cfg(feature = "edge-proxy")]
mod edge_proxy {
    use super::ProxyServer;
    use std::collections::HashMap;

    /// `edge_write_needs_invalidation` with the multi-statement fact
    /// derived from `sql` — exactly what the forward path passes from
    /// `StmtFacts::has_interior_semicolon`.
    fn ewni(is_write: bool, sql: &str) -> bool {
        ProxyServer::edge_write_needs_invalidation(
            is_write,
            sql,
            ProxyServer::stmt_has_interior_semicolon(sql),
        )
    }

    #[test]
    fn bare_txn_control_is_exempt_from_invalidation() {
        // F10: BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK change no rows —
        // an ORM's txn-per-request must not full-flush the fleet twice
        // per request.
        for sql in [
            "BEGIN",
            "begin;",
            "START TRANSACTION",
            "SAVEPOINT sp1",
            "RELEASE SAVEPOINT sp1",
            "ROLLBACK",
            "ROLLBACK TO SAVEPOINT sp1;",
        ] {
            assert!(!ewni(true, sql), "{sql:?} must be exempt");
        }
        // COMMIT keeps its conservative flush (closes the in-txn
        // write visibility window), and SET stays (GUC mitigation).
        assert!(ewni(true, "COMMIT"));
        assert!(ewni(true, "SET search_path TO tenant_b"));
    }

    #[test]
    fn compound_txn_strings_still_invalidate() {
        // A multi-statement string may hide a write behind a
        // txn-control or SELECT lead — never exempt it.
        assert!(ewni(true, "BEGIN; UPDATE t SET v = 1; COMMIT"));
        // SELECT-leading batch with a trailing write classifies
        // is_write=false, but the interior `;` forces invalidation.
        assert!(ewni(false, "SELECT v FROM t; UPDATE t SET v = 1"));
        // A plain single SELECT does not invalidate.
        assert!(!ewni(false, "SELECT v FROM t"));
    }

    #[test]
    fn copy_from_counts_as_write() {
        assert!(ewni(false, "COPY t FROM STDIN"));
        assert!(ProxyServer::is_edge_copy_write_sql("copy t from stdin"));
        // COPY TO exports rows — a read.
        assert!(!ewni(false, "COPY t TO STDOUT"));
    }

    #[test]
    fn procedural_and_txn_end_trigger_invalidation() {
        // G3: SQL-level EXECUTE/CALL/DO are opaque writes — the simple path
        // must invalidate and the classifier flags them (the extended path
        // memoizes them as the empty-set wildcard via the same predicate).
        for sql in [
            "EXECUTE ins(1)",
            "execute ins(1)",
            "CALL do_write()",
            "DO $$ BEGIN PERFORM 1; END $$",
        ] {
            assert!(
                ProxyServer::is_edge_procedural_sql(sql),
                "{sql:?} is procedural"
            );
            assert!(
                ewni(false, sql),
                "{sql:?} must invalidate on the simple path"
            );
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "SELECT 1 FROM t",
            "UPDATE t SET v=1",
        ] {
            assert!(
                !ProxyServer::is_edge_procedural_sql(sql),
                "{sql:?} is not procedural"
            );
        }

        // G2: COMMIT and its END synonym both trigger the wildcard flush
        // (closing the commit-visibility window); the openers do not.
        for sql in [
            "COMMIT",
            "commit work",
            "COMMIT PREPARED 'x'",
            "END",
            "END TRANSACTION",
            "end;",
        ] {
            assert!(ProxyServer::is_edge_txn_end_sql(sql), "{sql:?} ends a txn");
            assert!(
                ewni(false, sql),
                "{sql:?} must invalidate (commit-visibility window)"
            );
        }
        for sql in ["BEGIN", "START TRANSACTION", "SAVEPOINT s1", "ROLLBACK"] {
            assert!(
                !ProxyServer::is_edge_txn_end_sql(sql),
                "{sql:?} is not a txn-end"
            );
            assert!(
                !ewni(false, sql),
                "{sql:?} must stay exempt on the simple path"
            );
        }
    }

    #[test]
    fn edge_meta_prunable_protects_reparsed_names() {
        // G1: at a Sync, a Closed name NOT re-Parsed this batch is prunable;
        // a name Closed then re-Parsed (Npgsql statement replacement) must be
        // RETAINED so its fresh DML metadata survives to invalidate.
        let closes = vec!["S1".to_string(), "S2".to_string()];
        let none: Vec<String> = vec![];
        assert_eq!(
            ProxyServer::edge_meta_prunable(&closes, &none),
            vec!["S1", "S2"]
        );
        // S1 re-Parsed in the same batch → excluded from pruning.
        assert_eq!(
            ProxyServer::edge_meta_prunable(&closes, &["S1".to_string()]),
            vec!["S2"]
        );
        // Every closed name re-Parsed → nothing pruned (all meta kept fresh).
        assert!(ProxyServer::edge_meta_prunable(&closes, &closes).is_empty());
    }

    #[test]
    fn edge_dml_classifier_covers_dml_not_txn_control() {
        for sql in [
            "INSERT INTO t VALUES (1)",
            "update t set v = 1",
            "DELETE FROM t WHERE id = 1",
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
            "TRUNCATE t",
            "ALTER TABLE t ADD COLUMN c int",
            "COPY t FROM STDIN",
            "WITH del AS (DELETE FROM t RETURNING id) SELECT * FROM del",
        ] {
            assert!(ProxyServer::is_edge_dml_sql(sql), "{sql:?} is DML");
        }
        // Txn control / reads / plain CTE reads: NOT invalidation
        // triggers (BEGIN/COMMIT here would full-flush per txn).
        for sql in [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SET search_path TO x",
            "SELECT v FROM t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "COPY t TO STDOUT",
        ] {
            assert!(!ProxyServer::is_edge_dml_sql(sql), "{sql:?} is not DML");
        }
    }

    #[test]
    fn extended_batch_tables_union_and_wildcard() {
        let mut named: HashMap<String, Option<Vec<String>>> = HashMap::new();
        named.insert("ins".into(), Some(vec!["orders".into()]));
        named.insert("upd".into(), Some(vec!["users".into()]));
        named.insert("sel".into(), None);
        named.insert("weird".into(), Some(vec![])); // unattributable DML
        let unnamed: Option<Vec<String>> = Some(vec!["events".into()]);

        // Read-only batch: no invalidation.
        assert_eq!(
            ProxyServer::edge_extended_batch_tables(&["sel".to_string()], false, &named, &unnamed),
            None
        );
        // DML batch: union of referenced statements' tables.
        let t = ProxyServer::edge_extended_batch_tables(
            &["ins".to_string(), "upd".to_string(), "ins".to_string()],
            false,
            &named,
            &unnamed,
        )
        .expect("dml");
        assert_eq!(t, vec!["orders".to_string(), "users".to_string()]);
        // Unnamed execution folds in.
        let t = ProxyServer::edge_extended_batch_tables(&[], true, &named, &unnamed)
            .expect("unnamed dml");
        assert_eq!(t, vec!["events".to_string()]);
        // Any unattributable DML → wildcard (invalidate everything).
        let t = ProxyServer::edge_extended_batch_tables(
            &["ins".to_string(), "weird".to_string()],
            false,
            &named,
            &unnamed,
        )
        .expect("dml");
        assert!(t.is_empty(), "wildcard invalidation");
        // Unknown names (never Parse'd — backend errors anyway) are
        // treated as non-DML.
        assert_eq!(
            ProxyServer::edge_extended_batch_tables(&["ghost".to_string()], false, &named, &None),
            None
        );
    }
}

/// F12: async backend frames (NotificationResponse 'A', NoticeResponse
/// 'N', ParameterStatus 'S') captured inside a response window must
/// suppress the store (`cacheable = false`) while still being forwarded
/// byte-for-byte — a cached LISTEN/NOTIFY payload would replay to every
/// later hitter cross-session.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
#[tokio::test]
async fn capture_excludes_async_frames_from_cacheable() {
    use crate::client_tls::ClientStream;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt as _;

    fn frame(mtype: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![mtype];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    let clean: Vec<u8> = [
        frame(b'T', b"rowdesc"),
        frame(b'D', b"row"),
        frame(b'C', b"SELECT 1\0"),
        frame(b'Z', b"I"),
    ]
    .concat();
    let with_notify: Vec<u8> = [
        frame(b'T', b"rowdesc"),
        frame(b'A', b"\x00\x00\x30\x39chan\0payload\0"),
        frame(b'D', b"row"),
        frame(b'C', b"SELECT 1\0"),
        frame(b'Z', b"I"),
    ]
    .concat();
    let with_param_status: Vec<u8> = [
        frame(b'D', b"row"),
        frame(b'C', b"SELECT 1\0"),
        frame(b'S', b"TimeZone\0UTC\0"),
        frame(b'Z', b"I"),
    ]
    .concat();

    for (bytes, want_cacheable) in [
        (clean, true),
        (with_notify, false),
        (with_param_status, false),
    ] {
        let (mut backend, mut backend_peer) = pair().await;
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let session = make_test_session();

        backend_peer.write_all(&bytes).await.unwrap();
        backend_peer.flush().await.unwrap();

        let metrics = ServerMetrics::default();
        let (sent, captured, cacheable, _rows) = ProxyServer::stream_until_ready_capture(
            &mut client,
            &mut backend,
            &session,
            RelayLimits {
                client_write_timeout: Duration::from_secs(60),
                backend_read_timeout: Duration::from_secs(30),
                max_frame_bytes: usize::MAX,
                response_timeout: None,
                observation_bytes: usize::MAX,
            },
            usize::MAX,
            &metrics,
        )
        .await
        .expect("capture ok");
        assert_eq!(cacheable, want_cacheable, "cacheable flag");
        assert_eq!(sent as usize, bytes.len());
        assert_eq!(captured, bytes, "capture is byte-exact");
        assert_eq!(
            metrics
                .cache_capture_oversize
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "no cap was hit"
        );

        // Every frame — async ones included — was forwarded to the
        // live client.
        let mut got = vec![0u8; bytes.len()];
        client_peer.read_exact(&mut got).await.unwrap();
        assert_eq!(got, bytes, "forwarding must not be filtered");
    }
}

/// O1: the capture buffer is a per-session transient held ON TOP of the
/// bytes already streamed to the client, so it must be bounded. A response
/// larger than `[cache] max_cacheable_response_bytes` must (a) reach the
/// client byte-for-byte, (b) come back non-cacheable with an EMPTY capture
/// (the allocation freed, not merely ignored), and (c) bump
/// `cache_capture_oversize`. A response under the cap is unchanged.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
#[tokio::test]
async fn capture_stops_and_frees_buffer_past_byte_cap() {
    use crate::client_tls::ClientStream;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt as _;

    fn frame(mtype: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![mtype];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    // ~128 KiB of DataRows — far beyond the 1 KiB cap under test, and
    // beyond a socket buffer, so writer/reader must run concurrently.
    fn big_response(rows: usize) -> Vec<u8> {
        let row = vec![b'x'; 1024];
        let mut v = frame(b'T', b"rowdesc");
        for _ in 0..rows {
            v.extend_from_slice(&frame(b'D', &row));
        }
        v.extend_from_slice(&frame(b'C', format!("SELECT {}\0", rows).as_bytes()));
        v.extend_from_slice(&frame(b'Z', b"I"));
        v
    }

    const CAP: usize = 1024;

    for (bytes, want_cacheable, want_oversize) in [
        // Comfortably under the cap → today's behaviour, byte-for-byte.
        (big_response(0), true, 0u64),
        // Over the cap → forwarded in full, but never cached.
        (big_response(128), false, 1u64),
    ] {
        assert!(
            (bytes.len() > CAP) == (want_oversize == 1),
            "test fixture must straddle the cap"
        );

        let (mut backend, mut backend_peer) = pair().await;
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let session = make_test_session();
        let metrics = ServerMetrics::default();

        // Both peers must run concurrently with the relay: the response
        // exceeds the socket buffers in both directions.
        let to_write = bytes.clone();
        let writer = tokio::spawn(async move {
            backend_peer.write_all(&to_write).await.unwrap();
            backend_peer.flush().await.unwrap();
            backend_peer
        });
        let want_len = bytes.len();
        let reader = tokio::spawn(async move {
            let mut got = vec![0u8; want_len];
            client_peer.read_exact(&mut got).await.unwrap();
            got
        });

        let (sent, captured, cacheable, rows) = ProxyServer::stream_until_ready_capture(
            &mut client,
            &mut backend,
            &session,
            RelayLimits {
                client_write_timeout: Duration::from_secs(60),
                backend_read_timeout: Duration::from_secs(30),
                max_frame_bytes: usize::MAX,
                response_timeout: None,
                observation_bytes: usize::MAX,
            },
            CAP,
            &metrics,
        )
        .await
        .expect("capture ok");

        let _ = writer.await.unwrap();
        let got = reader.await.unwrap();

        // (a) The client stream is untouched by the cap.
        assert_eq!(got, bytes, "client bytes must be byte-exact");
        assert_eq!(sent as usize, bytes.len(), "sent count");
        // Row count is parsed from CommandComplete either way.
        assert_eq!(rows, if want_oversize == 1 { 128 } else { 0 });
        // (b) Cacheability + capture buffer.
        assert_eq!(cacheable, want_cacheable, "cacheable flag");
        if want_oversize == 1 {
            assert!(captured.is_empty(), "oversize capture must be dropped");
            assert_eq!(captured.capacity(), 0, "allocation must be freed, not kept");
        } else {
            assert_eq!(captured, bytes, "under-cap capture is byte-exact");
        }
        // (c) Operator-visible counter.
        assert_eq!(
            metrics.cache_capture_oversize.load(AtomicOrdering::Relaxed),
            want_oversize,
            "cache_capture_oversize"
        );
    }
}

/// `stream_flush` (Fix S4) relays whatever the backend has already
/// produced byte-for-byte, then returns as soon as the socket goes
/// `WouldBlock` — it must never block waiting for more. The read buffer
/// is a reused `BytesMut` (no per-call `vec![0u8; 16384]` zeroing); this
/// sends more than one 16 KiB buffer's worth of data so the loop must
/// `clear()` and refill the same buffer across multiple `try_read_buf`
/// calls without dropping or duplicating bytes.
#[tokio::test]
async fn stream_flush_relays_available_bytes_then_returns_without_blocking() {
    use crate::client_tls::ClientStream;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt as _;

    let (mut backend, mut backend_peer) = pair().await;
    let (client_raw, mut client_peer) = pair().await;
    let mut client = ClientStream::Plain(client_raw);
    let session = make_test_session();
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();

    // Larger than one 16 KiB read, so a correct implementation must
    // loop `try_read_buf` (clearing and refilling the same buffer)
    // rather than stopping after the first chunk.
    let bytes: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

    // Both sides of the relay run concurrently with the `stream_flush`
    // calls below, so the test does not depend on 40 KB fitting in the
    // kernel socket buffers: on a host with small `tcp_wmem`/`tcp_rmem`
    // an inline `write_all` (or an inline read only after the flush)
    // would deadlock rather than fail.
    let feed_bytes = bytes.clone();
    // Return the peer so it stays OPEN until the relay has drained
    // everything: dropping it here would deliver EOF right after the
    // payload, which `stream_flush` correctly reports as a closed backend.
    let feed = tokio::spawn(async move {
        backend_peer.write_all(&feed_bytes).await.unwrap();
        backend_peer.flush().await.unwrap();
        backend_peer
    });
    let bytes_len = bytes.len();
    let drain = tokio::spawn(async move {
        let mut got = vec![0u8; bytes_len];
        client_peer.read_exact(&mut got).await.unwrap();
        got
    });

    backend.readable().await.unwrap();
    // `stream_flush` is non-blocking (`try_read_buf`), so a single call
    // may see only part of the payload if the kernel hasn't finished
    // delivering it yet on a loaded host: loop calls (each `clear()`ing
    // and refilling the same reused buffer, per the doc comment above)
    // until every byte is relayed, bounded by an overall timeout so a
    // real regression fails fast instead of hanging.
    let mut sent: u64 = 0;
    tokio::time::timeout(Duration::from_secs(5), async {
        while (sent as usize) < bytes.len() {
            let n = ProxyServer::stream_flush(&mut client, &mut backend, &session, &state)
                .await
                .expect("flush ok");
            sent += n;
            if n == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    })
    .await
    .expect("all bytes must be relayed within the timeout");
    assert_eq!(sent as usize, bytes.len(), "all available bytes relayed");

    let got = tokio::time::timeout(Duration::from_secs(5), drain)
        .await
        .expect("client_peer must receive all relayed bytes within the timeout")
        .unwrap();
    assert_eq!(got, bytes, "forwarded bytes must be byte-exact");
    tokio::time::timeout(Duration::from_secs(5), feed)
        .await
        .expect("backend writer must finish within the timeout")
        .unwrap();

    // Nothing left to read: a second call must return immediately with
    // 0 rather than block — that is the whole point of Flush semantics
    // (no ReadyForQuery to wait for, unlike `stream_until_ready`).
    let sent2 = tokio::time::timeout(
        Duration::from_secs(2),
        ProxyServer::stream_flush(&mut client, &mut backend, &session, &state),
    )
    .await
    .expect("stream_flush must not block once the backend has nothing more to say")
    .expect("flush ok");
    assert_eq!(sent2, 0, "no more data means nothing sent");
}

// ---- query-rewriting: the rules-engine rewrite contract ----

#[cfg(feature = "query-rewriting")]
mod query_rewriting {
    use crate::rewriter::{
        QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation,
    };

    fn rw_with_table_replace() -> QueryRewriter {
        let rw = QueryRewriter::new(RewriterConfig {
            enabled: true,
            ..Default::default()
        });
        rw.add_rule(
            RewriteRule::build("t")
                .pattern(QueryPattern::Table("a".to_string()))
                .transform(Transformation::ReplaceTable {
                    from: "a".to_string(),
                    to: "b".to_string(),
                })
                .build(),
        );
        rw
    }

    #[test]
    fn matching_query_is_rewritten() {
        let res = rw_with_table_replace().rewrite("select * from a").unwrap();
        assert!(res.was_rewritten(), "rule did not fire");
        assert!(res.query().contains('b'), "rewritten: {}", res.query());
        assert!(
            !res.query().contains("from a"),
            "still references a: {}",
            res.query()
        );
    }

    #[test]
    fn unmatched_query_is_unchanged() {
        let res = rw_with_table_replace()
            .rewrite("select * from other")
            .unwrap();
        assert!(!res.was_rewritten());
        assert_eq!(res.query(), "select * from other");
    }
}

// ---- multi-tenancy: row-filter injection per tenant ----

#[cfg(feature = "multi-tenancy")]
mod multi_tenancy {
    use crate::multi_tenancy::{
        IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId,
        TenantManager, TenantManagerBuilder, TenantQueryTransformer,
    };

    fn manager() -> TenantManager {
        let transformer = TenantQueryTransformer::new().register_tables(&["t"], "tid");
        let tm = TenantManagerBuilder::new()
            .config(MultiTenancyConfig {
                enabled: true,
                identification: IdentificationMethod::Header {
                    header_name: "application_name".to_string(),
                },
                ..Default::default()
            })
            .query_transformer(transformer)
            .build();
        tm.register_tenant(TenantConfig::new(
            TenantId::new("acme"),
            IsolationStrategy::row("public", "tid"),
        ));
        tm
    }

    #[test]
    fn tenant_table_gets_filter() {
        let res = manager().transform_query("select * from t", &TenantId::new("acme"));
        assert!(res.transformed, "expected a tenant filter to be injected");
        let q = res.query.to_lowercase();
        assert!(
            q.contains("tid") && q.contains("acme"),
            "filter missing: {}",
            res.query
        );
    }

    #[test]
    fn non_tenant_table_passes_through() {
        let res = manager().transform_query("select * from other", &TenantId::new("acme"));
        assert!(!res.transformed);
    }
}

// ---- TR: the journal records statements the replay engine reads ----

mod ha_tr {
    use crate::transaction_journal::TransactionJournal;
    use crate::NodeId;

    #[tokio::test]
    async fn journal_records_and_windows_a_statement() {
        let j = TransactionJournal::new();
        let from = chrono::Utc::now() - chrono::Duration::seconds(60);
        let tx = uuid::Uuid::new_v4();
        j.begin_transaction(tx, uuid::Uuid::new_v4(), NodeId::new(), 0)
            .await
            .unwrap();
        j.log_statement(
            tx,
            "insert into t values (1)".to_string(),
            Vec::new(),
            None,
            None,
            0,
        )
        .await
        .unwrap();
        let to = chrono::Utc::now() + chrono::Duration::seconds(60);
        // TR-07: the window is committed history only.
        assert!(j.entries_in_window(from, to).await.is_empty());
        j.commit_transaction(tx).await.unwrap();
        let entries = j.entries_in_window(from, to).await;
        assert_eq!(entries.len(), 1, "journaled statement should be in window");
        assert!(entries[0].1.statement.contains("insert"));
    }

    /// The write path registers each statement with the session capture
    /// and the relay's observation commits it: an autocommit write becomes
    /// one committed transaction with its outcome and source identity
    /// (TR-07). Reads register nothing.
    #[tokio::test]
    async fn write_path_capture_commits_one_transaction_per_autocommit_write() {
        use super::{make_test_session, test_config};
        use crate::journal_capture::{Completion, ResponseOutcome};
        use crate::server::ProxyServer;
        use std::sync::atomic::Ordering;

        let server = ProxyServer::new(test_config()).unwrap();
        let session = make_test_session();
        let from = chrono::Utc::now() - chrono::Duration::seconds(60);

        for (sql, tag) in [
            ("insert into t values (1)", "INSERT 0 1"),
            ("update t set a = 2", "UPDATE 3"),
        ] {
            ProxyServer::journal_register_simple(&session, sql);
            assert!(session.journal_armed.load(Ordering::Relaxed));
            ProxyServer::journal_observe(
                &session,
                &server.state,
                ResponseOutcome {
                    status: b'I',
                    completions: vec![Completion::Tag(tag.into())],
                    error: None,
                },
            )
            .await;
            assert!(!session.journal_armed.load(Ordering::Relaxed));
        }
        ProxyServer::journal_register_simple(&session, "select 1");
        assert!(!session.journal_armed.load(Ordering::Relaxed));

        let to = chrono::Utc::now() + chrono::Duration::seconds(60);
        let entries = server
            .state
            .transaction_journal
            .entries_in_window(from, to)
            .await;
        assert_eq!(entries.len(), 2, "one journal entry per write");
        assert_ne!(
            entries[0].0, entries[1].0,
            "each write is its own transaction"
        );
        assert_eq!(
            server.state.transaction_journal.active_count().await,
            0,
            "autocommit writes are committed, never left active"
        );
        let committed = server.state.transaction_journal.committed_after(0).await;
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].commit_seq, Some(1));
        assert_eq!(committed[1].commit_seq, Some(2));
        assert_eq!(committed[1].entries[0].rows_affected, Some(3));
        assert_eq!(
            committed[0].source.client_addr,
            session.client_addr.to_string()
        );
        assert_eq!(
            server
                .state
                .metrics
                .journal_committed
                .load(Ordering::Relaxed),
            2
        );
    }

    /// A write the backend rejected is never journaled, and a rolled-back
    /// explicit transaction leaves no committed history (TR-07).
    #[tokio::test]
    async fn write_path_capture_excludes_failed_and_rolled_back_work() {
        use super::{make_test_session, test_config};
        use crate::journal_capture::{Completion, ResponseOutcome};
        use crate::server::ProxyServer;
        use std::sync::atomic::Ordering;

        let server = ProxyServer::new(test_config()).unwrap();
        let session = make_test_session();
        let cycle = |sql: &str, status: u8, tags: &[&str], err: bool| {
            let session = session.clone();
            let sql = sql.to_string();
            let tags: Vec<String> = tags.iter().map(|t| t.to_string()).collect();
            let state = server.state.clone();
            async move {
                ProxyServer::journal_register_simple(&session, &sql);
                ProxyServer::journal_observe(
                    &session,
                    &state,
                    ResponseOutcome {
                        status,
                        completions: tags.into_iter().map(Completion::Tag).collect(),
                        error: err.then(|| ("23505".to_string(), "dup".to_string())),
                    },
                )
                .await;
            }
        };
        cycle("insert into t values (1)", b'I', &[], true).await;
        cycle("BEGIN", b'T', &["BEGIN"], false).await;
        assert!(session.journal_open.load(Ordering::Relaxed));
        cycle("insert into t values (2)", b'T', &["INSERT 0 1"], false).await;
        assert_eq!(server.state.transaction_journal.active_count().await, 1);
        cycle("ROLLBACK", b'I', &["ROLLBACK"], false).await;
        assert!(!session.journal_open.load(Ordering::Relaxed));
        assert_eq!(server.state.transaction_journal.active_count().await, 0);
        assert!(server
            .state
            .transaction_journal
            .committed_after(0)
            .await
            .is_empty());
        assert_eq!(
            server
                .state
                .metrics
                .journal_rolled_back
                .load(Ordering::Relaxed),
            1
        );
        // A session that ends mid-transaction drops its active journal.
        cycle("BEGIN", b'T', &["BEGIN"], false).await;
        cycle("insert into t values (3)", b'T', &["INSERT 0 1"], false).await;
        ProxyServer::journal_close(&session, &server.state).await;
        assert_eq!(server.state.transaction_journal.active_count().await, 0);
    }
}

// ---- schema-routing: OLAP vs OLTP workload classification ----

#[cfg(feature = "schema-routing")]
mod schema_routing {
    use crate::schema_routing::{QueryAnalyzer, SchemaRegistry};
    use std::sync::Arc;

    fn analyzer() -> QueryAnalyzer {
        QueryAnalyzer::new(Arc::new(SchemaRegistry::new()))
    }

    #[test]
    fn aggregation_group_by_is_analytics() {
        let a = analyzer();
        assert!(a
            .analyze("select count(*) from orders group by region")
            .is_analytics());
    }

    #[test]
    fn simple_point_query_is_not_analytics() {
        let a = analyzer();
        assert!(!a
            .analyze("select * from orders where id = 1")
            .is_analytics());
    }
}

/// The session RAII guard must deregister the session and bump the
/// connections-closed metric when dropped normally.
#[tokio::test]
async fn session_guard_deregisters_on_drop() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    state.sessions.insert(session.id, session.clone());
    assert_eq!(state.sessions.len(), 1);
    let before = state.metrics.connections_closed.load(Ordering::Relaxed);
    {
        let _g = SessionGuard {
            state: state.clone(),
            session_id: session.id,
            _client_slot: None,
        };
    }
    assert!(state.sessions.is_empty(), "guard must deregister on drop");
    assert_eq!(
        state.metrics.connections_closed.load(Ordering::Relaxed),
        before + 1
    );
}

/// The critical property: the guard must deregister even when the connection
/// task unwinds via a panic (the leak this replaces).
#[tokio::test]
async fn session_guard_deregisters_on_panic() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    state.sessions.insert(session.id, session.clone());
    let sid = session.id;
    let st = state.clone();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _g = SessionGuard {
            state: st,
            session_id: sid,
            _client_slot: None,
        };
        panic!("simulated connection-task panic");
    }));
    assert!(r.is_err(), "closure must have panicked");
    assert!(
        state.sessions.is_empty(),
        "guard must deregister on a panic unwind"
    );
}

// ---- [limits] client-connection cap + idle-session timeout ----

/// With no `[limits] max_client_connections` (the default 0) the permit
/// pool is absent entirely, so the accept path is byte-for-byte the
/// historical unbounded one.
#[test]
fn client_slots_absent_when_cap_is_zero() {
    let mut config = test_config();
    config.limits.max_client_connections = 0;
    let server = ProxyServer::new(config).unwrap();
    assert!(server.state.client_slots.is_none());
    assert_eq!(server.state.limits.max_client_connections, 0);
}

/// A configured cap sizes the permit pool exactly, an exhausted pool is the
/// accept loop's refuse-and-close path, and the permit parked in the
/// `SessionGuard` is returned when the session ends.
#[test]
fn client_slot_cap_exhausts_and_guard_returns_the_permit() {
    let mut config = test_config();
    config.limits.max_client_connections = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let sem = state
        .client_slots
        .clone()
        .expect("a non-zero cap must size the permit pool");
    assert_eq!(sem.available_permits(), 1);

    // First connection takes the only slot (exactly what the accept loop does).
    let permit = Arc::clone(&sem)
        .try_acquire_owned()
        .expect("first connection gets the slot");
    // Second connection finds none -> the accept loop refuses it.
    assert!(
        Arc::clone(&sem).try_acquire_owned().is_err(),
        "cap of 1 must refuse the second concurrent connection"
    );

    // The permit lives in the session guard, so it is returned on drop.
    let session = make_test_session();
    state.sessions.insert(session.id, session.clone());
    {
        let _g = SessionGuard {
            state: state.clone(),
            session_id: session.id,
            _client_slot: Some(permit),
        };
        assert_eq!(sem.available_permits(), 0, "slot held for the live session");
    }
    assert_eq!(
        sem.available_permits(),
        1,
        "the slot must be released when the session ends"
    );
}

/// The slot must also come back when the connection task unwinds — the
/// reason the permit is owned by the RAII guard rather than released at the
/// end of `handle_client`.
#[test]
fn client_slot_returned_on_panic_unwind() {
    let mut config = test_config();
    config.limits.max_client_connections = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let sem = state.client_slots.clone().expect("cap configured");
    let permit = Arc::clone(&sem).try_acquire_owned().unwrap();
    let session = make_test_session();
    state.sessions.insert(session.id, session.clone());
    let sid = session.id;
    let st = state.clone();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _g = SessionGuard {
            state: st,
            session_id: sid,
            _client_slot: Some(permit),
        };
        panic!("simulated connection-task panic");
    }));
    assert!(r.is_err(), "closure must have panicked");
    assert_eq!(
        sem.available_permits(),
        1,
        "the slot must be released on a panic unwind"
    );
}

/// The refusal frame a capped-out proxy sends must be a real PostgreSQL
/// ErrorResponse carrying SQLSTATE 53300 (too_many_connections) at FATAL
/// severity (which is what marks the connection dead for pgx/npgsql/JDBC),
/// so a driver reports the condition instead of a bare connection reset.
#[test]
fn over_capacity_error_encodes_sqlstate_53300() {
    let bytes = ProxyServer::over_capacity_error_bytes();
    assert_eq!(bytes[0], b'E', "must be an ErrorResponse frame");
    assert!(
        bytes.windows(7).any(|w| w == b"C53300\0"),
        "SQLSTATE field must be 53300"
    );
    assert!(
        bytes.windows(7).any(|w| w == b"SFATAL\0"),
        "severity must be FATAL, as PostgreSQL sends for 53300"
    );
    assert!(String::from_utf8_lossy(&bytes).contains("sorry, too many clients already"));
}

/// End-to-end over a real socket: the refusal is written and the connection
/// is then closed (the client sees the error, then EOF).
#[tokio::test]
async fn refuse_over_capacity_writes_error_then_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        ProxyServer::refuse_over_capacity(&mut ClientStream::Plain(sock), Duration::from_secs(5))
            .await;
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut buf = Vec::new();
    // Returns only at EOF, which proves the proxy closed the socket.
    client.read_to_end(&mut buf).await.unwrap();
    srv.await.unwrap();
    assert_eq!(buf[0], b'E');
    assert!(buf.windows(7).any(|w| w == b"C53300\0"));
    assert!(String::from_utf8_lossy(&buf).contains("sorry, too many clients already"));
}

/// `client_idle_timeout_secs = 0` (the default) must leave the query loop's
/// client read unbounded exactly as before — no deadline is armed.
#[test]
fn idle_timeout_disabled_at_zero() {
    let mut config = test_config();
    config.limits.client_idle_timeout_secs = 0;
    let server = ProxyServer::new(config).unwrap();
    assert!(
        server.state.limits.client_idle_timeout.is_none(),
        "0 must disable the idle-session timeout"
    );
    // And the default config resolves the same way.
    assert!(ResolvedLimits::default().client_idle_timeout.is_none());
}

/// A non-zero `client_idle_timeout_secs` resolves to the deadline the query
/// loop arms once per idle wait.
#[test]
fn idle_timeout_armed_when_configured() {
    let mut config = test_config();
    config.limits.client_idle_timeout_secs = 45;
    let server = ProxyServer::new(config).unwrap();
    assert_eq!(
        server.state.limits.client_idle_timeout,
        Some(Duration::from_secs(45))
    );
}

/// The frame sent to a session killed by the idle timeout must carry
/// SQLSTATE 57P05 (idle_session_timeout) with PostgreSQL's wording.
#[test]
fn idle_session_timeout_error_encodes_sqlstate_57p05() {
    let bytes = ProxyServer::idle_session_timeout_error_bytes();
    assert_eq!(bytes[0], b'E', "must be an ErrorResponse frame");
    assert!(
        bytes.windows(7).any(|w| w == b"C57P05\0"),
        "SQLSTATE field must be 57P05"
    );
    assert!(
        bytes.windows(7).any(|w| w == b"SFATAL\0"),
        "severity must be FATAL, as PostgreSQL sends for 57P05"
    );
    assert!(String::from_utf8_lossy(&bytes)
        .contains("terminating connection due to idle-session timeout"));
}

// ---- wiring: admission control, the idle deadline, and the read path ----

/// Startup-message bytes for a plain (non-TLS) client: len + protocol
/// version 3.0 + a `user` parameter.
fn startup_bytes(user: &str) -> Vec<u8> {
    let mut params = Vec::new();
    params.extend_from_slice(b"user\0");
    params.extend_from_slice(user.as_bytes());
    params.extend_from_slice(b"\0\0");
    let mut out = Vec::new();
    out.extend_from_slice(&((8 + params.len()) as u32).to_be_bytes());
    out.extend_from_slice(&196608u32.to_be_bytes()); // 3.0
    out.extend_from_slice(&params);
    out
}

/// CancelRequest bytes: len(16) + code 80877102 + pid + key.
fn cancel_bytes(pid: u32, key: u32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&16u32.to_be_bytes());
    out.extend_from_slice(&80877102u32.to_be_bytes());
    out.extend_from_slice(&pid.to_be_bytes());
    out.extend_from_slice(&key.to_be_bytes());
    out
}

fn startup_msg() -> StartupMessage {
    StartupMessage::Startup {
        protocol_version: 196608,
        params: HashMap::new(),
    }
}

/// H-05: full-jitter backoff is bounded by the 2 s cap and differs across
/// sessions for the same attempt (no lockstep reconnect wave).
#[test]
fn reconnect_backoff_is_capped_and_seed_sensitive() {
    for attempt in 0..12u32 {
        let d = ProxyServer::reconnect_backoff(attempt, 0);
        assert!(d >= Duration::from_millis(1), "attempt {attempt}: {d:?}");
        assert!(d <= Duration::from_secs(2), "attempt {attempt}: {d:?}");
    }
    assert_ne!(
        ProxyServer::reconnect_backoff(3, 1),
        ProxyServer::reconnect_backoff(3, 2),
        "different sessions must desynchronise"
    );
}

/// Admission control: a real Startup takes a slot; when none is free it is
/// refused and `connections_rejected` counts it.
#[tokio::test]
async fn admission_takes_a_slot_and_counts_a_refusal() {
    let mut config = test_config();
    config.limits.max_client_connections = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();

    let slot = ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .expect("the first connection is admitted");
    assert!(slot.is_some(), "a configured cap must hand out a slot");
    assert_eq!(
        state.metrics.connections_rejected.load(Ordering::Relaxed),
        0
    );

    // Cap saturated: the next Startup is refused and counted.
    assert!(ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .is_err());
    assert_eq!(
        state.metrics.connections_rejected.load(Ordering::Relaxed),
        1,
        "a refusal must increment connections_rejected"
    );

    // Releasing the slot re-admits.
    drop(slot);
    assert!(ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .is_ok());
}

/// P-03/H-05: with a bounded admission wait, a saturated cap makes the next
/// connection wait (counted) and admit when a slot frees — or time out.
#[tokio::test]
async fn bounded_admission_waits_and_admits_or_times_out() {
    let mut config = test_config();
    config.limits.max_client_connections = 1;
    config.limits.client_admission_wait_secs = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();

    let held = ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .unwrap()
        .unwrap();
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(held);
    });
    let permit = ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .unwrap();
    assert!(
        permit.is_some(),
        "waiter must be admitted when a slot frees"
    );
    drop(permit);
    releaser.await.unwrap();
    assert_eq!(state.metrics.admission_waited.load(Ordering::Relaxed), 1);
    assert_eq!(state.metrics.admission_timeouts.load(Ordering::Relaxed), 0);

    // Hold the slot past the 1 s budget: the waiter times out and is counted.
    let _held2 = ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .unwrap()
        .unwrap();
    assert!(ProxyServer::admit_client_slot(&state, &startup_msg())
        .await
        .is_err());
    assert_eq!(state.metrics.admission_waited.load(Ordering::Relaxed), 2);
    assert_eq!(state.metrics.admission_timeouts.load(Ordering::Relaxed), 1);
    assert_eq!(
        state.metrics.connections_rejected.load(Ordering::Relaxed),
        1
    );
}

/// A CancelRequest must be admitted even with the cap saturated — it is a
/// throwaway connection that never becomes a session, and refusing it would
/// make query cancellation impossible exactly when it is needed. It must
/// also never consume a slot, nor count as a rejection.
#[tokio::test]
async fn cancel_request_is_admitted_while_the_cap_is_saturated() {
    let mut config = test_config();
    config.limits.max_client_connections = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let sem = state.client_slots.clone().expect("cap configured");
    let _held = Arc::clone(&sem).try_acquire_owned().unwrap();
    assert_eq!(sem.available_permits(), 0, "cap is saturated");

    let slot =
        ProxyServer::admit_client_slot(&state, &StartupMessage::CancelRequest { pid: 1, key: 2 })
            .await
            .expect("a cancel request must never be refused");
    assert!(slot.is_none(), "a cancel request must not consume a slot");
    assert_eq!(
        state.metrics.connections_rejected.load(Ordering::Relaxed),
        0,
        "a cancel request is not a rejection"
    );
}

/// Over the wire through `handle_client`: with the cap saturated a client
/// that sends a real Startup gets the 53300 FATAL frame then EOF, and the
/// rejection is counted — while a client that sends a CancelRequest is
/// served (no error frame) and never touches the cap.
#[tokio::test]
async fn handle_client_refuses_startup_but_serves_cancel_when_saturated() {
    let mut config = test_config();
    config.limits.max_client_connections = 1;
    let server = ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
    let sem = state.client_slots.clone().expect("cap configured");
    let _held = Arc::clone(&sem).try_acquire_owned().unwrap();
    let (shutdown_tx, _rx) = broadcast::channel(1);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // --- a real Startup while saturated: refused with 53300 ---
    let mut client = TcpStream::connect(addr).await.unwrap();
    let (sock, peer) = listener.accept().await.unwrap();
    let h = tokio::spawn(ProxyServer::handle_client(
        sock,
        peer,
        state.clone(),
        Arc::new(config.clone()),
        shutdown_tx.clone(),
    ));
    client.write_all(&startup_bytes("alice")).await.unwrap();
    let mut buf = Vec::new();
    client.read_to_end(&mut buf).await.unwrap();
    h.await.unwrap().unwrap();
    assert_eq!(buf[0], b'E', "refused client must get an ErrorResponse");
    assert!(
        buf.windows(7).any(|w| w == b"C53300\0"),
        "refusal must carry SQLSTATE 53300"
    );
    assert_eq!(
        state.metrics.connections_rejected.load(Ordering::Relaxed),
        1
    );
    assert!(state.sessions.is_empty(), "no session may be left behind");

    // --- a CancelRequest while still saturated: served, not refused ---
    let mut client = TcpStream::connect(addr).await.unwrap();
    let (sock, peer) = listener.accept().await.unwrap();
    let h = tokio::spawn(ProxyServer::handle_client(
        sock,
        peer,
        state.clone(),
        Arc::new(config.clone()),
        shutdown_tx.clone(),
    ));
    client.write_all(&cancel_bytes(42, 43)).await.unwrap();
    let mut buf = Vec::new();
    client.read_to_end(&mut buf).await.unwrap();
    h.await.unwrap().unwrap();
    assert!(
        buf.is_empty(),
        "a cancel request must not be answered with an error frame, got {:?}",
        String::from_utf8_lossy(&buf)
    );
    assert_eq!(
        state.metrics.connections_rejected.load(Ordering::Relaxed),
        1,
        "a cancel request must not count as a rejection"
    );
    assert_eq!(sem.available_permits(), 0, "the cap is still saturated");
}

/// The idle deadline is armed ONLY when the session is genuinely waiting
/// for a new command: never mid-COPY (a paused COPY FROM STDIN producer
/// must not be killed and the bulk load aborted) and never with a partially
/// received message in the buffer (a client trickling a large statement is
/// slow, not idle).
#[test]
fn idle_deadline_armed_only_at_a_message_boundary() {
    let mut config = test_config();
    config.limits.client_idle_timeout_secs = 30;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let session = make_test_session();

    let empty = BytesMut::new();
    assert!(
        ProxyServer::client_idle_deadline(&state, &empty, &session).is_some(),
        "an idle session at a message boundary is armed"
    );

    // Half a message received: not idle, still arriving.
    let mut partial = BytesMut::new();
    partial.extend_from_slice(b"Q\0\0\0");
    assert!(
        ProxyServer::client_idle_deadline(&state, &partial, &session).is_none(),
        "a partially received message must not arm the idle timeout"
    );

    // COPY FROM STDIN in progress: the client may legitimately pause.
    session
        .copy_in_progress
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(
        ProxyServer::client_idle_deadline(&state, &empty, &session).is_none(),
        "a COPY FROM STDIN must not be killed by the idle timeout"
    );
    session
        .copy_in_progress
        .store(false, std::sync::atomic::Ordering::Relaxed);

    // Disabled (the default) arms nothing at all.
    let server = ProxyServer::new(test_config()).unwrap();
    assert!(
        ProxyServer::client_idle_deadline(&server.state, &empty, &session).is_none(),
        "client_idle_timeout_secs = 0 must never arm a deadline"
    );
}

/// The idle deadline actually fires on the plain client read, and does not
/// fire when the client speaks in time.
#[tokio::test]
async fn read_client_bytes_times_out_when_idle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).await.unwrap();
    let (sock, _) = listener.accept().await.unwrap();
    let mut stream = ClientStream::Plain(sock);
    let mut buffer = BytesMut::with_capacity(64);

    let deadline = tokio::time::Instant::now() + Duration::from_millis(60);
    let outcome = ProxyServer::read_client_bytes(&mut stream, &mut buffer, Some(deadline))
        .await
        .unwrap();
    assert_eq!(outcome, ClientRead::IdleTimeout);
    assert!(buffer.is_empty());

    // A client that speaks before the deadline is not timed out.
    client.write_all(b"hello").await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let outcome = ProxyServer::read_client_bytes(&mut stream, &mut buffer, Some(deadline))
        .await
        .unwrap();
    assert_eq!(outcome, ClientRead::Bytes(5));
    assert_eq!(&buffer[..], b"hello");
}

/// …and it fires the same way while the session's cached backend connection
/// is being watched for unsolicited traffic (the `select!` arm), which is
/// the state an idle pooled session actually sits in.
#[tokio::test]
async fn read_next_client_message_times_out_while_watching_the_backend() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let _client = TcpStream::connect(addr).await.unwrap();
    let (sock, _) = listener.accept().await.unwrap();
    let mut stream = ClientStream::Plain(sock);

    // A quiet "backend" socket for the session to watch.
    let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let baddr = blistener.local_addr().unwrap();
    let backend = TcpStream::connect(baddr).await.unwrap();
    let _backend_peer = blistener.accept().await.unwrap();
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    conns.insert("node-a".to_string(), BackendConn::new(backend));

    let server = ProxyServer::new(test_config()).unwrap();
    let mut buffer = BytesMut::with_capacity(64);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(60);
    let outcome = ProxyServer::read_next_client_message(
        &mut stream,
        &mut buffer,
        &mut conns,
        Some("node-a"),
        &mut BytesMut::with_capacity(16384),
        Some(deadline),
        &server.state,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ClientRead::IdleTimeout);
    assert!(
        conns.contains_key("node-a"),
        "a live backend must not be dropped by the idle timeout"
    );
}

/// Backend frames are only forwarded to the client once they are COMPLETE.
/// A truncated asynchronous frame (a large `NotificationResponse` split
/// across reads, say) must stay buffered: a client that receives half a
/// frame blocks forever waiting for the rest, and recovery can no longer
/// inject anything after it — not an ErrorResponse, and not the rows of a
/// re-executed statement, which would be appended inside the partial frame.
#[tokio::test]
async fn watch_relay_withholds_partial_backend_frames() {
    use tokio::io::AsyncReadExt as _;
    use tokio::io::AsyncWriteExt as _;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client_peer = TcpStream::connect(addr).await.unwrap();
    let (sock, _) = listener.accept().await.unwrap();
    let mut stream = ClientStream::Plain(sock);

    let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let baddr = blistener.local_addr().unwrap();
    let backend = TcpStream::connect(baddr).await.unwrap();
    let (mut backend_peer, _) = blistener.accept().await.unwrap();
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    conns.insert("node-a".to_string(), BackendConn::new(backend));

    let server = ProxyServer::new(test_config()).unwrap();
    let mut buffer = BytesMut::with_capacity(64);
    // One `NotificationResponse`, delivered in two pieces.
    let payload = b"\x00\x00\x27\x0fchan\0hello\0".to_vec();
    let mut whole = vec![b'A'];
    whole.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    whole.extend_from_slice(&payload);
    let split = whole.len() - 3;
    backend_peer.write_all(&whole[..split]).await.unwrap();
    backend_peer.flush().await.unwrap();

    // The relay sees the head of the frame and must publish nothing.
    let mut abuf = BytesMut::with_capacity(16384);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(120);
    let outcome = ProxyServer::read_next_client_message(
        &mut stream,
        &mut buffer,
        &mut conns,
        Some("node-a"),
        &mut abuf,
        Some(deadline),
        &server.state,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ClientRead::IdleTimeout);
    assert_eq!(
        abuf.len(),
        split,
        "the partial frame must be retained for reassembly"
    );
    let mut peek = [0u8; 64];
    let seen = tokio::time::timeout(Duration::from_millis(50), client_peer.read(&mut peek)).await;
    assert!(
        seen.is_err(),
        "no bytes of an incomplete frame may reach the client"
    );

    // The remainder completes the frame: now the whole thing is published.
    backend_peer.write_all(&whole[split..]).await.unwrap();
    backend_peer.flush().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(120);
    let outcome = ProxyServer::read_next_client_message(
        &mut stream,
        &mut buffer,
        &mut conns,
        Some("node-a"),
        &mut abuf,
        Some(deadline),
        &server.state,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ClientRead::IdleTimeout);
    assert!(abuf.is_empty(), "a fully forwarded frame leaves no tail");
    let mut got = vec![0u8; whole.len()];
    tokio::time::timeout(Duration::from_millis(200), client_peer.read_exact(&mut got))
        .await
        .expect("the completed frame must be delivered")
        .unwrap();
    assert_eq!(got, whole, "the frame must arrive byte-exact");
}

/// The TR-03 allowlists are binary-searched, so they must stay sorted and
/// lowercase, and no side-effecting name may creep in.
#[test]
fn tr_pure_builtins_are_sorted_and_exclude_side_effects() {
    for list in [TR_PURE_BUILTINS, TR_CALL_KEYWORDS] {
        assert!(list.windows(2).all(|w| w[0] < w[1]), "sorted, unique");
        assert!(list.iter().all(|n| *n == n.to_ascii_lowercase()));
    }
    for bad in [
        "nextval",
        "setval",
        "currval",
        "lastval",
        "pg_notify",
        "set_config",
        "pg_advisory_lock",
        "pg_try_advisory_lock",
        "txid_current",
        "setseed",
        "lo_import",
        "pg_terminate_backend",
        "dblink",
        "pg_reload_conf",
    ] {
        assert!(
            TR_PURE_BUILTINS.binary_search(&bad).is_err(),
            "{bad} must not be pure"
        );
    }
}

/// H-07: every relay validates a backend frame header the moment it is
/// readable. A length below the 4-byte minimum or above the configured
/// budget is an error immediately — never "wait for more bytes", and never
/// an accumulator growing toward the advertised size.
#[test]
fn backend_frame_len_rejects_malformed_and_oversize_headers() {
    // Incomplete header: undecided, not an error.
    assert!(backend_frame_len(&[b'D', 0, 0], 1024).unwrap().is_none());
    assert!(backend_frame_len(&[], 1024).unwrap().is_none());
    // Smallest legal frame (ReadyForQuery, len 5) passes a budget of 5.
    assert_eq!(
        backend_frame_len(&[b'Z', 0, 0, 0, 5, b'I'], 5).unwrap(),
        Some(5)
    );
    // Below the self-counting minimum: malformed, fail closed now.
    for len in [0u32, 1, 2, 3] {
        let mut h = vec![b'D'];
        h.extend_from_slice(&len.to_be_bytes());
        assert!(backend_frame_len(&h, 1024).is_err(), "len {len}");
    }
    // Above the budget: refused before any accumulation.
    let mut big = vec![b'D'];
    big.extend_from_slice(&1025u32.to_be_bytes());
    assert!(backend_frame_len(&big, 1024).is_err());
    assert_eq!(backend_frame_len(&big, 1025).unwrap(), Some(1025));
    // usize::MAX budget must not overflow the `len + 1` frame arithmetic.
    let mut max = vec![b'D'];
    max.extend_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        backend_frame_len(&max, usize::MAX).unwrap(),
        Some(u32::MAX as usize)
    );
}

/// A streaming relay hits a malformed header and fails immediately with a
/// protocol error, instead of waiting out the read timeout for bytes that
/// can never complete the frame.
#[tokio::test]
async fn stream_until_ready_fails_fast_on_malformed_backend_frame() {
    use tokio::io::AsyncWriteExt as _;
    let (mut client_side, _client_peer) = tokio::io::duplex(4096);
    let _ = &mut client_side;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client_sock = TcpStream::connect(addr).await.unwrap();
    let (sock, _) = listener.accept().await.unwrap();
    let mut client = ClientStream::Plain(sock);
    let _keep = client_sock;

    let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let baddr = blistener.local_addr().unwrap();
    let mut backend = TcpStream::connect(baddr).await.unwrap();
    let (mut backend_peer, _) = blistener.accept().await.unwrap();

    let mut config = test_config();
    config.limits.backend_read_timeout_secs = 30; // would be the old stall
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();

    // Tag 'D' with a declared length of 2: can never be a valid frame.
    backend_peer.write_all(&[b'D', 0, 0, 0, 2]).await.unwrap();
    backend_peer.flush().await.unwrap();
    let started = std::time::Instant::now();
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        ProxyServer::stream_until_ready(&mut client, &mut backend, &session, &server.state),
    )
    .await
    .expect("must not wait out the 30 s read timeout");
    assert!(
        matches!(r, Err(ref f) if matches!(f.error, ProxyError::Protocol(_))),
        "malformed frame must be a protocol error: {r:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// The out-of-band re-prepare reader refuses an oversize declared body
/// without allocating it: an advertised 4 GiB frame used to be a
/// `vec![0u8; len]` sized by the backend.
#[tokio::test]
async fn read_one_frame_type_refuses_oversize_without_allocating() {
    use tokio::io::AsyncWriteExt as _;
    let (mut a, mut b) = tokio::io::duplex(64);
    let mut hdr = vec![b'1'];
    hdr.extend_from_slice(&u32::MAX.to_be_bytes());
    b.write_all(&hdr).await.unwrap();
    let r = ProxyServer::read_one_frame_type(&mut a, 1024 * 1024).await;
    assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");
    // Within budget, a body larger than the scratch buffer is discarded
    // in chunks and the type byte comes back.
    let (mut a2, mut b2) = tokio::io::duplex(64 * 1024);
    let body = vec![7u8; 40_000];
    let mut frame = vec![b'1'];
    frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
    tokio::spawn(async move { b2.write_all(&frame).await.unwrap() });
    assert_eq!(
        ProxyServer::read_one_frame_type(&mut a2, 1024 * 1024)
            .await
            .unwrap(),
        b'1'
    );
    assert_eq!(
        ProxyServer::read_one_frame_type(&mut a2, 1024 * 1024)
            .await
            .unwrap(),
        b'Z'
    );
}

/// The frame scanner returns only whole frames and rejects a length below
/// the 4-byte minimum instead of stalling reassembly on it.
#[test]
fn complete_frame_prefix_stops_at_frame_boundaries() {
    fn f(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }
    let a = f(b'A', b"one");
    let b = f(b'N', b"two");
    let both = [a.clone(), b.clone()].concat();
    assert_eq!(
        ProxyServer::complete_frame_prefix(&both, usize::MAX).unwrap(),
        both.len()
    );
    assert_eq!(
        ProxyServer::complete_frame_prefix(&a, usize::MAX).unwrap(),
        a.len()
    );
    // A trailing partial frame is excluded, whole ones ahead of it are not.
    let partial = [both.clone(), a[..3].to_vec()].concat();
    assert_eq!(
        ProxyServer::complete_frame_prefix(&partial, usize::MAX).unwrap(),
        both.len()
    );
    // Fewer than the 5 header bytes: nothing is complete.
    assert_eq!(
        ProxyServer::complete_frame_prefix(&a[..4], usize::MAX).unwrap(),
        0
    );
    assert_eq!(
        ProxyServer::complete_frame_prefix(&[], usize::MAX).unwrap(),
        0
    );
    // Length below the self-counting minimum is malformed, not incomplete.
    assert!(ProxyServer::complete_frame_prefix(&[b'A', 0, 0, 0, 3], usize::MAX).is_err());
    // A huge declared length is merely incomplete; the caller's buffer cap
    // is what bounds it.
    assert_eq!(
        ProxyServer::complete_frame_prefix(&[b'A', 0xff, 0xff, 0xff, 0xff, 1, 2], usize::MAX)
            .unwrap(),
        0
    );
}

/// The conditional-reset classifier must call every session-state-creating
/// statement DIRTY (so it is reset before reuse) and only provably neutral
/// statements CLEAN. A false "clean" would leak state across clients, so the
/// dirty cases here are the security-critical half of the test.
#[cfg(feature = "pool-modes")]
#[test]
fn stmt_classifier_is_conservative() {
    let clean = ProxyServer::stmt_leaves_session_state;
    // ---- Provably clean (reset may be skipped) ----
    assert!(!clean(
        "SELECT abalance FROM pgbench_accounts WHERE aid = 12345"
    ));
    assert!(!clean("SELECT 1"));
    assert!(!clean("SELECT 1;")); // single trailing ';'
    assert!(!clean("  select now()  ")); // read of a volatile fn: no session state
    assert!(!clean("INSERT INTO t VALUES (1)")); // INTO is INSERT syntax, not SELECT INTO
    assert!(!clean("UPDATE t SET c = 1 WHERE id = 2")); // "SET" is UPDATE syntax, not a GUC
    assert!(!clean("DELETE FROM t WHERE id = 3"));
    assert!(!clean("WITH x AS (SELECT 1) SELECT * FROM x"));
    assert!(!clean("SELECT into_total FROM ledger")); // column named into_total, not INTO kw
    assert!(!clean("BEGIN"));
    assert!(!clean("COMMIT"));
    assert!(!clean("SELECT current_setting('work_mem')")); // reading a GUC is fine

    // ---- Must be DIRTY (reset required) ----
    assert!(clean("SET work_mem = '1GB'"), "SET GUC");
    assert!(clean("set search_path to public"), "lowercase SET");
    assert!(clean("CREATE TEMP TABLE t(x int)"), "temp table");
    assert!(clean("CREATE TEMPORARY TABLE t(x int)"), "temp table");
    assert!(clean("SELECT * INTO TEMP t FROM src"), "SELECT INTO temp");
    assert!(clean("select a into t from s"), "SELECT INTO lowercase");
    assert!(clean("PREPARE p AS SELECT 1"), "prepared statement");
    assert!(clean("DEALLOCATE p"), "deallocate");
    assert!(
        clean("DECLARE c CURSOR WITH HOLD FOR SELECT 1"),
        "held cursor"
    );
    assert!(clean("LISTEN my_channel"), "listen");
    assert!(clean("SELECT pg_advisory_lock(42)"), "advisory lock");
    assert!(clean("SELECT pg_try_advisory_lock(1)"), "try advisory lock");
    assert!(
        clean("SELECT set_config('work_mem','1GB',false)"),
        "set_config fn"
    );
    assert!(clean("SELECT nextval('s')"), "sequence cache");
    assert!(clean("SET ROLE admin"), "set role");
    assert!(clean("SET SESSION AUTHORIZATION bob"), "session auth");
    assert!(clean("DISCARD ALL"), "explicit discard");
    assert!(clean("RESET ALL"), "reset");
    // Multi-statement: a neutral lead cannot vouch for what follows a ';'.
    assert!(clean("SELECT 1; SET work_mem='1GB'"), "hidden SET after ;");
    assert!(
        clean("SELECT 1; CREATE TEMP TABLE t(x int)"),
        "hidden temp after ;"
    );
    // ';' inside a literal → conservatively dirty (safe over-reset).
    assert!(clean("SELECT 'a;b'"), "semicolon in literal");
    // Non-neutral leads.
    assert!(clean("COPY t FROM STDIN"), "copy");
    assert!(clean("GRANT SELECT ON t TO bob"), "grant");
    assert!(clean("ALTER TABLE t ADD COLUMN c int"), "ddl");
}

/// Regression for the single-lowercase-pass rewrite of the
/// `DIRTY_TOKENS` scan: `set_config`/`advisory`/`nextval`/`setval` must
/// still be matched case-insensitively no matter how the caller casts
/// them, exactly as the old per-token `contains_ci` scan did. Also
/// checks the lowercasing buffer doesn't panic or fold non-ASCII bytes.
#[cfg(feature = "pool-modes")]
#[test]
fn stmt_classifier_dirty_tokens_stay_case_insensitive() {
    // `dirty(sql) == true` means `stmt_leaves_session_state` reports the
    // statement as session-state-creating (reset required before reuse).
    let dirty = ProxyServer::stmt_leaves_session_state;
    assert!(dirty("SELECT PG_ADVISORY_LOCK(1)"), "uppercase advisory");
    assert!(dirty("select Pg_Advisory_Unlock(1)"), "mixed-case advisory");
    assert!(dirty("SELECT NEXTVAL('s')"), "uppercase nextval");
    assert!(dirty("SELECT SETVAL('s', 1)"), "uppercase setval");
    assert!(
        dirty("SELECT Set_Config('work_mem','1GB',false)"),
        "mixed-case set_config"
    );
    // Non-ASCII bytes must not panic the lowercasing buffer, and must
    // not be folded into a false match.
    assert!(!dirty("SELECT name FROM café"), "plain non-ASCII select");
}

/// `reset_backend` must only report success when the reset query cleanly
/// completed — no ErrorResponse and an idle ReadyForQuery. A poisoned reset
/// (error, or a non-idle transaction status) must return `Err` so the caller
/// drops the connection instead of parking it dirty (Group 2, 2.0.b).
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn reset_backend_rejects_error_and_nonidle() {
    use tokio::io::AsyncWriteExt as _;
    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        v.extend_from_slice(body);
        v
    }
    let rfq = |st: u8| frame(b'Z', &[st]);
    let cc = frame(b'C', b"DISCARD ALL\0");
    let err = frame(b'E', b"SERROR\0C25P02\0Mreset failed\0\0");

    // Clean: CommandComplete + ReadyForQuery('I') -> Ok.
    let (mut client, mut server) = tokio::io::duplex(4096);
    let mut resp = cc.clone();
    resp.extend_from_slice(&rfq(b'I'));
    server.write_all(&resp).await.unwrap();
    assert!(
        ProxyServer::reset_backend(
            &mut client,
            "DISCARD ALL",
            Duration::from_secs(30),
            usize::MAX
        )
        .await
        .is_ok(),
        "clean reset must succeed"
    );

    // ErrorResponse before RFQ -> Err (connection is poisoned).
    let (mut client, mut server) = tokio::io::duplex(4096);
    let mut resp = err.clone();
    resp.extend_from_slice(&rfq(b'I'));
    server.write_all(&resp).await.unwrap();
    assert!(
        ProxyServer::reset_backend(
            &mut client,
            "DISCARD ALL",
            Duration::from_secs(30),
            usize::MAX
        )
        .await
        .is_err(),
        "reset that errored must be rejected"
    );

    // Non-idle status ('T') -> Err (still in a transaction).
    let (mut client, mut server) = tokio::io::duplex(4096);
    let mut resp = cc.clone();
    resp.extend_from_slice(&rfq(b'T'));
    server.write_all(&resp).await.unwrap();
    assert!(
        ProxyServer::reset_backend(
            &mut client,
            "DISCARD ALL",
            Duration::from_secs(30),
            usize::MAX
        )
        .await
        .is_err(),
        "reset leaving a non-idle txn must be rejected"
    );
}

/// The pool identity key stays the bare `(node,user,db)` triple when no
/// routing-relevant startup GUC is set (backward-compatible with existing
/// pooling), but diverges when a client sets a different `client_encoding` /
/// `DateStyle` / etc., so such clients never share a connection (Group 2,
/// 2.0.c).
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn pool_key_folds_startup_params() {
    let base = make_test_session();
    {
        let mut v = base.variables.write().await;
        v.insert("user".into(), "u".into());
        v.insert("database".into(), "d".into());
    }
    let k_plain = ProxyServer::pool_key_for("n:5432", &base).await;
    assert_eq!(k_plain, crate::pool::pool_key("n:5432", "u", "d"));

    // Same identity but a distinct client_encoding must produce a
    // different key (no cross-encoding sharing).
    let utf8 = make_test_session();
    let latin1 = make_test_session();
    for (s, enc) in [(&utf8, "UTF8"), (&latin1, "LATIN1")] {
        let mut v = s.variables.write().await;
        v.insert("user".into(), "u".into());
        v.insert("database".into(), "d".into());
        v.insert("client_encoding".into(), enc.into());
    }
    let k_utf8 = ProxyServer::pool_key_for("n:5432", &utf8).await;
    let k_latin1 = ProxyServer::pool_key_for("n:5432", &latin1).await;
    assert_ne!(k_utf8, k_latin1, "different client_encoding must not share");
    assert_ne!(k_utf8, k_plain, "GUC-bearing key must differ from bare key");
}

/// A declared backend frame length within the cap is accepted — this
/// must keep passing for every legitimate frame the auth-phase scanners
/// see today (S5-backend-auth-frame-cap).
#[test]
fn test_validate_backend_frame_len_within_cap_ok() {
    assert!(validate_backend_frame_len(4, 1024).is_ok());
    assert!(validate_backend_frame_len(1024, 1024).is_ok());
}

/// A hostile/compromised backend declaring a length far past the
/// configured cap (e.g. len=0xFFFFFFFF, which would otherwise grow the
/// scanner's accumulation buffer toward 4 GiB) must be rejected instead
/// of silently accepted. This is the regression case for
/// S5-backend-auth-frame-cap: on the old code (no comparison against any
/// cap at all) this assertion fails because there was no length check to
/// call.
#[test]
fn test_validate_backend_frame_len_exceeds_cap_rejected() {
    let err = validate_backend_frame_len(0xFFFF_FFFF, 1024)
        .expect_err("oversized backend frame length must be rejected");
    assert!(matches!(err, ProxyError::Protocol(_)));
    let msg = err.to_string();
    assert!(
        msg.contains("4294967295"),
        "message should name the offending length: {msg}"
    );
    assert!(
        msg.contains("1024"),
        "message should name the configured max: {msg}"
    );
}

/// Boundary: exactly at the cap is still allowed (only strictly-greater
/// is rejected), matching the `len > max_message_size` boundary
/// `ProtocolCodec` already uses on the decoded path.
#[test]
fn test_validate_backend_frame_len_boundary() {
    assert!(validate_backend_frame_len(1024, 1024).is_ok());
    assert!(validate_backend_frame_len(1025, 1024).is_err());
}

/// Verbatim copy of the pre-optimisation `anomaly_fingerprint`,
/// which built a fresh `String` per call. The reusable-buffer
/// rewrite must produce byte-identical fingerprints.
#[cfg(feature = "anomaly-detection")]
fn legacy_anomaly_fingerprint(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut in_single = false;
    let mut prev_space = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            in_single = !in_single;
            if in_single {
                out.push('?');
                while let Some(&n) = chars.peek() {
                    chars.next();
                    if n == '\'' {
                        in_single = false;
                        break;
                    }
                }
                prev_space = false;
                continue;
            }
        }
        if c.is_ascii_digit() {
            if !out.ends_with('?') {
                out.push('?');
            }
            while matches!(chars.peek(), Some(c) if c.is_ascii_digit() || *c == '.') {
                chars.next();
            }
            prev_space = false;
            continue;
        }
        if c.is_ascii_whitespace() {
            if !prev_space && !out.is_empty() {
                out.push(' ');
                prev_space = true;
            }
            continue;
        }
        out.push(c.to_ascii_lowercase());
        prev_space = false;
    }
    out.trim_end().to_string()
}

#[cfg(feature = "anomaly-detection")]
const FINGERPRINT_CORPUS: &[&str] = &[
    "",
    "   ",
    "SELECT 1",
    "SELECT * FROM users WHERE id = 1",
    "SELECT * FROM users WHERE id = 99",
    "select   *\n from\tusers  where name = 'bob'   ",
    "INSERT INTO t VALUES (1, 2.5, 'a''b', NULL)",
    "SELECT '' FROM t",
    "SELECT 'unterminated FROM t",
    "UPDATE t SET x = 3.14159 WHERE y = 'Ünïcode'",
    "SELECT * FROM «таблица» WHERE имя = 'ЗНАЧЕНИЕ'",
    "SELECT * FROM t WHERE n = 1 OR 1=1 -- 💥",
    "SELECT 1;",
    "\n\n\t",
];

#[cfg(feature = "anomaly-detection")]
#[test]
fn anomaly_fingerprint_matches_legacy_implementation() {
    for sql in FINGERPRINT_CORPUS {
        assert_eq!(
            anomaly_fingerprint(sql),
            legacy_anomaly_fingerprint(sql),
            "fingerprint diverged for {:?}",
            sql
        );
    }
}

#[cfg(feature = "anomaly-detection")]
#[test]
fn anomaly_fingerprint_into_reuses_buffer_without_residue() {
    let mut buf = String::new();
    // A reused buffer must yield exactly what a fresh one does,
    // in any order — no leftovers from the previous statement.
    for sql in FINGERPRINT_CORPUS {
        anomaly_fingerprint_into(sql, &mut buf);
        assert_eq!(buf, legacy_anomaly_fingerprint(sql), "for {:?}", sql);
    }
    anomaly_fingerprint_into("SELECT a_very_long_identifier FROM some_table", &mut buf);
    let grown = buf.capacity();
    anomaly_fingerprint_into("SELECT 1", &mut buf);
    assert_eq!(buf, "select ?");
    assert_eq!(
        buf.capacity(),
        grown,
        "capacity should be reused, not reset"
    );
}

/// The fingerprint normalises literals, so queries differing only
/// in their literal values collapse to one shape — the property
/// the novel-query detector depends on.
#[cfg(feature = "anomaly-detection")]
#[test]
fn anomaly_fingerprint_collapses_literals() {
    let mut buf = String::new();
    anomaly_fingerprint_into("SELECT * FROM users WHERE id = 1", &mut buf);
    let a = buf.clone();
    anomaly_fingerprint_into("select * from USERS where id = 99", &mut buf);
    assert_eq!(a, buf);
    assert_eq!(a, "select * from users where id = ?");
}

// ---- In-session Transaction Replay (tr_mode) ----

mod tr_in_session {
    use super::*;
    use crate::config::TrMode;
    use crate::protocol::QueryMessage;

    const MODES: [TrMode; 4] = [
        TrMode::None,
        TrMode::Session,
        TrMode::Select,
        TrMode::Transaction,
    ];
    const PHASES: [FaultPhase; 2] = [FaultPhase::NotDelivered, FaultPhase::OutcomeUnknown];
    const KINDS: [StmtKind; 5] = [
        StmtKind::Read,
        StmtKind::Write,
        StmtKind::Commit,
        StmtKind::Control,
        StmtKind::Other,
    ];

    /// Every cell of the decision table must satisfy the hard rules.
    #[test]
    fn tr_decide_exhaustive_invariants() {
        use TrAction::*;
        let mut cells = 0;
        for mode in MODES {
            for phase in PHASES {
                for in_tx in [false, true] {
                    for has_writes in [false, true] {
                        for replayable in [false, true] {
                            for kind in KINDS {
                                cells += 1;
                                let a = ProxyServer::tr_decide(
                                    mode, phase, in_tx, has_writes, replayable, kind,
                                );
                                let ctx = format!(
                                        "{mode:?}/{phase:?}/in_tx={in_tx}/writes={has_writes}/replayable={replayable}/{kind:?} -> {a:?}"
                                    );
                                // none: always one error, then close.
                                if mode == TrMode::None {
                                    assert_eq!(a, CloseWithError("57P01"), "{ctx}");
                                    continue;
                                }
                                assert!(!matches!(a, CloseWithError(_)), "{ctx}");
                                // A statement that never ran, outside a
                                // transaction, is always just re-run.
                                if phase == FaultPhase::NotDelivered && !in_tx {
                                    assert_eq!(a, Reexecute, "{ctx}");
                                }
                                // Never double-apply: an autocommit write/
                                // opaque statement with unknown outcome is
                                // never re-executed.
                                if phase == FaultPhase::OutcomeUnknown
                                    && !in_tx
                                    && matches!(kind, StmtKind::Write | StmtKind::Other)
                                {
                                    assert_eq!(a, ErrorAndContinue("08007"), "{ctx}");
                                }
                                // A COMMIT with unknown outcome is never retried.
                                if phase == FaultPhase::OutcomeUnknown
                                    && in_tx
                                    && kind == StmtKind::Commit
                                {
                                    assert_eq!(a, ErrorAndContinue("08007"), "{ctx}");
                                }
                                // Replay only ever happens inside a
                                // transaction that is recorded & replayable.
                                if a == ReplayThenReexecute {
                                    assert!(in_tx && replayable, "{ctx}");
                                    assert_ne!(mode, TrMode::Session, "{ctx}");
                                    if mode == TrMode::Select {
                                        assert!(!has_writes, "{ctx}");
                                    }
                                }
                                // Inside a transaction the ONLY transparent
                                // outcome is a replay (the tx died with the
                                // old backend; a bare re-execution would run
                                // in autocommit).
                                if in_tx {
                                    assert_ne!(a, Reexecute, "{ctx}");
                                }
                                // session mode never replays anything.
                                if mode == TrMode::Session {
                                    assert!(
                                        matches!(a, ErrorAndContinue(_))
                                            || (phase == FaultPhase::NotDelivered && !in_tx),
                                        "{ctx}"
                                    );
                                }
                                // SQLSTATE follows the phase.
                                if let ErrorAndContinue(code) = a {
                                    match phase {
                                        FaultPhase::NotDelivered => {
                                            assert_eq!(code, "57P01", "{ctx}")
                                        }
                                        FaultPhase::OutcomeUnknown => {
                                            assert_eq!(code, "08007", "{ctx}")
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        assert_eq!(cells, 4 * 2 * 2 * 2 * 2 * 5);
    }

    /// Spot rows straight from the specification table.
    #[test]
    fn tr_decide_spec_rows() {
        use FaultPhase::*;
        use StmtKind::*;
        use TrAction::*;
        let d = ProxyServer::tr_decide;
        // none
        assert_eq!(
            d(TrMode::None, OutcomeUnknown, true, true, true, Read),
            CloseWithError("57P01")
        );
        // session: not-delivered outside tx -> re-execute (any kind)
        assert_eq!(
            d(TrMode::Session, NotDelivered, false, false, false, Write),
            Reexecute
        );
        assert_eq!(
            d(TrMode::Session, NotDelivered, false, false, false, Commit),
            Reexecute
        );
        // session: not-delivered inside tx -> 57P01; unknown -> 08007
        assert_eq!(
            d(TrMode::Session, NotDelivered, true, true, true, Write),
            ErrorAndContinue("57P01")
        );
        assert_eq!(
            d(TrMode::Session, OutcomeUnknown, false, false, false, Read),
            ErrorAndContinue("08007")
        );
        assert_eq!(
            d(TrMode::Session, OutcomeUnknown, true, false, true, Read),
            ErrorAndContinue("08007")
        );
        // select: unknown-outcome read outside tx -> re-execute; write -> 08007
        assert_eq!(
            d(TrMode::Select, OutcomeUnknown, false, false, false, Read),
            Reexecute
        );
        assert_eq!(
            d(TrMode::Select, OutcomeUnknown, false, false, false, Control),
            Reexecute
        );
        assert_eq!(
            d(TrMode::Select, OutcomeUnknown, false, false, false, Write),
            ErrorAndContinue("08007")
        );
        // select: read-only replayable tx -> replay; tx with writes -> error
        assert_eq!(
            d(TrMode::Select, OutcomeUnknown, true, false, true, Read),
            ReplayThenReexecute
        );
        assert_eq!(
            d(TrMode::Select, NotDelivered, true, false, true, Write),
            ReplayThenReexecute
        );
        assert_eq!(
            d(TrMode::Select, OutcomeUnknown, true, true, true, Read),
            ErrorAndContinue("08007")
        );
        assert_eq!(
            d(TrMode::Select, NotDelivered, true, true, true, Read),
            ErrorAndContinue("57P01")
        );
        assert_eq!(
            d(TrMode::Select, OutcomeUnknown, true, false, false, Read),
            ErrorAndContinue("08007")
        );
        // transaction: replay uncommitted tx then re-execute (writes included)
        assert_eq!(
            d(TrMode::Transaction, NotDelivered, true, true, true, Write),
            ReplayThenReexecute
        );
        assert_eq!(
            d(TrMode::Transaction, OutcomeUnknown, true, true, true, Write),
            ReplayThenReexecute
        );
        assert_eq!(
            d(TrMode::Transaction, NotDelivered, true, true, true, Commit),
            ReplayThenReexecute
        );
        // transaction: COMMIT with unknown outcome -> 08007, never retried
        assert_eq!(
            d(
                TrMode::Transaction,
                OutcomeUnknown,
                true,
                true,
                true,
                Commit
            ),
            ErrorAndContinue("08007")
        );
        // transaction: autocommit write unknown -> 08007
        assert_eq!(
            d(
                TrMode::Transaction,
                OutcomeUnknown,
                false,
                false,
                false,
                Write
            ),
            ErrorAndContinue("08007")
        );
        // transaction: non-replayable (over cap) degrades to session behaviour
        assert_eq!(
            d(TrMode::Transaction, NotDelivered, true, true, false, Write),
            ErrorAndContinue("57P01")
        );
        assert_eq!(
            d(TrMode::Transaction, OutcomeUnknown, true, true, false, Read),
            ErrorAndContinue("08007")
        );
    }

    #[test]
    fn tr_classify_table() {
        use StmtKind::*;
        let c = |sql: &str| ProxyServer::tr_classify(sql, &TrReadPolicy::default());
        assert_eq!(c("SELECT 1"), Read);
        assert_eq!(c("  select * from t where x = 'a;b' "), Write);
        assert_eq!(c("SELECT count(*) FROM t;"), Read);
        assert_eq!(c("SELECT CASE WHEN x THEN 1 END FROM t"), Read);
        assert_eq!(c("SELECT * INTO t2 FROM t"), Other);
        assert_eq!(c("SELECT nextval('s')"), Other);
        assert_eq!(c("SELECT pg_sleep(0.5), 42"), Read);
        // TR-03: a read is re-executable only if every call is a known
        // side-effect-free built-in. Anything else may already have run.
        assert_eq!(c("SELECT audit_side_effect()"), Other);
        assert_eq!(c("SELECT my_schema.my_udf(1)"), Other);
        assert_eq!(c("SELECT lower(name), count(*) FROM t"), Read);
        assert_eq!(c("SELECT pg_catalog.upper('x')"), Read);
        assert_eq!(c("SELECT \"MyFn\"(1)"), Other);
        assert_eq!(c("SELECT currval('s')"), Other);
        assert_eq!(c("SELECT pg_notify('c', 'p')"), Other);
        assert_eq!(c("SELECT set_config('a', 'b', false)"), Other);
        assert_eq!(c("SELECT pg_advisory_lock(1)"), Other);
        assert_eq!(c("SELECT random(), now(), gen_random_uuid()"), Read);
        assert_eq!(c("SELECT 'f(x)' AS s, $$g()$$ FROM t"), Read); // calls only in literals
        assert_eq!(
            c("WITH x AS (SELECT audit_side_effect()) SELECT * FROM x"),
            Other
        );
        assert_eq!(
            c("SELECT * FROM t WHERE id IN (1, 2) AND EXISTS (SELECT 1)"),
            Read
        );
        assert_eq!(c("SELECT CAST(x AS int), COALESCE(a, b) FROM t"), Read);
        // Operator-listed UDFs become eligible.
        let policy = TrReadPolicy::from_config(&["Audit_Side_Effect".to_string()]);
        assert_eq!(
            ProxyServer::tr_classify("SELECT audit_side_effect()", &policy),
            Read
        );
        assert_eq!(
            ProxyServer::tr_classify("SELECT other_udf()", &policy),
            Other
        );
        assert_eq!(c("SHOW application_name"), Read);
        assert_eq!(c("VALUES (1)"), Read);
        assert_eq!(c("TABLE t"), Read);
        assert_eq!(c("WITH x AS (SELECT 1) SELECT * FROM x"), Read);
        assert_eq!(
            c("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"),
            Write
        );
        assert_eq!(c("with u as (update t set v=1) select 1"), Write);
        assert_eq!(c("EXPLAIN SELECT 1"), Read);
        assert_eq!(c("EXPLAIN ANALYZE DELETE FROM t"), Other);
        assert_eq!(c("COPY t TO STDOUT"), Read);
        assert_eq!(c("COPY t FROM STDIN"), Write);
        assert_eq!(c("INSERT INTO t VALUES (1)"), Write);
        assert_eq!(c("update t set v = 2"), Write);
        assert_eq!(c("DELETE FROM t"), Write);
        assert_eq!(
            c("MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE"),
            Write
        );
        assert_eq!(c("CREATE TABLE x(i int)"), Write);
        assert_eq!(c("CALL p()"), Write);
        assert_eq!(c("DO $$ BEGIN END $$"), Write);
        assert_eq!(c("EXECUTE p(1)"), Write);
        assert_eq!(c("COMMIT"), Commit);
        assert_eq!(c("commit;"), Commit);
        assert_eq!(c("END"), Commit);
        assert_eq!(c("END TRANSACTION"), Commit);
        assert_eq!(c("COMMIT PREPARED 'x'"), Commit);
        assert_eq!(c("PREPARE TRANSACTION 'x'"), Commit);
        assert_eq!(c("PREPARE p AS SELECT 1"), Other); // server-side prepare, not a commit
        assert_eq!(c("INSERT INTO t VALUES (1); COMMIT"), Commit); // multi-stmt that may commit
        assert_eq!(
            c("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"),
            Write
        );
        assert_eq!(c("BEGIN"), Control);
        assert_eq!(c("BEGIN; INSERT INTO t VALUES (1)"), Write);
        assert_eq!(c("START TRANSACTION"), Control);
        assert_eq!(c("SAVEPOINT s1"), Control);
        assert_eq!(c("RELEASE SAVEPOINT s1"), Control);
        assert_eq!(c("ROLLBACK"), Control);
        assert_eq!(c("ROLLBACK TO SAVEPOINT s1"), Control);
        assert_eq!(c("ABORT"), Control);
        assert_eq!(c("SET application_name = 'x'"), Control);
        assert_eq!(c("RESET ALL"), Control);
        assert_eq!(c("DISCARD ALL"), Control);
        assert_eq!(c(""), Control);
        assert_eq!(c("   ;  "), Control);
        assert_eq!(c("SETTINGS"), Other);
        assert_eq!(c("LISTEN ch"), Other);
        assert_eq!(c("NOTIFY ch"), Other);
        assert_eq!(c("LOCK TABLE t"), Other);
        assert_eq!(c("FETCH 10 FROM c"), Other);
    }

    #[test]
    fn tr_ends_transaction_recognises_single_statement_ends_only() {
        let f = ProxyServer::tr_ends_transaction;
        for s in [
            "ROLLBACK",
            "rollback;",
            "ABORT",
            "COMMIT",
            "END",
            "COMMIT WORK",
            "END TRANSACTION",
            " Rollback Work ; ",
        ] {
            assert!(f(s), "{s}");
        }
        for s in [
            "ROLLBACK TO SAVEPOINT a",
            "ROLLBACK PREPARED 'x'",
            "COMMIT PREPARED 'x'",
            "COMMIT AND CHAIN",
            "ROLLBACK; SELECT 1",
            "SELECT 1",
            "",
            "ENDING",
        ] {
            assert!(!f(s), "{s}");
        }
    }

    #[test]
    fn tr_session_set_tracking_filters() {
        let set = ProxyServer::tr_is_session_set;
        assert!(set("SET application_name = 'tr-f3'"));
        assert!(set("set search_path to a, b;"));
        assert!(set("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"));
        assert!(set("SET ROLE readonly"));
        assert!(set("RESET application_name"));
        assert!(!set("SET LOCAL statement_timeout = 1"));
        assert!(!set("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
        assert!(!set("SET CONSTRAINTS ALL DEFERRED"));
        assert!(!set("SET a = 1; SET b = 2"));
        assert!(!set("SELECT set_config('a','b',false)"));
        assert!(!set("SETTINGS"));
        let all = ProxyServer::tr_resets_all;
        assert!(all("RESET ALL"));
        assert!(all("discard all;"));
        assert!(!all("RESET application_name"));
        assert!(!all("DISCARD PLANS"));
    }

    #[test]
    fn starts_with_word_ci_requires_boundary() {
        assert!(ProxyServer::starts_with_word_ci("SET x", "SET"));
        assert!(ProxyServer::starts_with_word_ci("set", "SET"));
        assert!(ProxyServer::starts_with_word_ci("END;", "END"));
        assert!(!ProxyServer::starts_with_word_ci("SETTINGS", "SET"));
        assert!(!ProxyServer::starts_with_word_ci("SE", "SET"));
    }

    #[test]
    fn parse_msg_sql_extracts_query_from_encoded_parse() {
        let mut p = vec![b'P', 0, 0, 0, 0];
        p.extend_from_slice(&cstr("ps1"));
        p.extend_from_slice(&cstr("SELECT 42"));
        p.extend_from_slice(&[0, 0]);
        assert_eq!(ProxyServer::parse_msg_sql(&p), Some("SELECT 42"));
        assert_eq!(ProxyServer::parse_msg_sql(&p[..3]), None);
        let mut reg: HashMap<String, bytes::Bytes> = HashMap::new();
        reg.insert("ps1".to_string(), bytes::Bytes::from(p));
        let refs = vec!["ps1".to_string()];
        assert_eq!(
            ProxyServer::tr_extended_sql(None, &refs, &reg),
            Some("SELECT 42")
        );
        assert_eq!(
            ProxyServer::tr_extended_sql(Some("SELECT 1"), &refs, &reg),
            Some("SELECT 1")
        );
        assert_eq!(
            ProxyServer::tr_extended_sql(None, &["nope".to_string()], &reg),
            None
        );
    }

    fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut f = vec![tag];
        f.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        f.extend_from_slice(body);
        f
    }

    #[test]
    fn tr_commit_classification_guards_every_query_statement() {
        for sql in [
            "/* audit */ COMMIT",
            "-- ignored\nEND WORK",
            "PREPARE/* nested /* x */ */TRANSACTION 'tx'",
            "SELECT ';COMMIT'; COMMIT PREPARED 'tx'",
            "COMMIT AND CHAIN",
            "ROLLBACK; INSERT INTO t VALUES (1)",
            "SELECT $x$COMMIT$x$;END",
        ] {
            let kind = ProxyServer::tr_classify(sql, &TrReadPolicy::default());
            assert_eq!(kind, StmtKind::Commit, "{sql}");
            assert_eq!(
                ProxyServer::tr_decide(
                    TrMode::Transaction,
                    FaultPhase::OutcomeUnknown,
                    true,
                    true,
                    true,
                    kind
                ),
                TrAction::ErrorAndContinue("08007")
            );
        }
    }

    #[test]
    fn tr_visible_response_progress_never_reexecutes() {
        for action in [
            TrAction::Reexecute,
            TrAction::ReplayThenReexecute,
            TrAction::ErrorAndContinue("08007"),
            TrAction::CloseWithError("57P01"),
        ] {
            for raw in [false, true] {
                for terminal in [false, true] {
                    let progress = ResponseProgress {
                        bytes: 7,
                        raw,
                        terminal,
                    };
                    let guarded = ProxyServer::tr_response_action(action, progress, false);
                    assert!(!matches!(
                        guarded,
                        TrAction::Reexecute | TrAction::ReplayThenReexecute
                    ));
                    if raw || terminal {
                        assert_eq!(guarded, TrAction::CloseIncompleteResponse);
                    }
                    // Independent of the byte count: an unfinished frame or a
                    // published terminal frame forbids injection even if this
                    // call recorded no bytes of its own.
                    let zero = ResponseProgress {
                        bytes: 0,
                        raw,
                        terminal,
                    };
                    if raw || terminal {
                        assert_eq!(
                            ProxyServer::tr_response_action(action, zero, false),
                            TrAction::CloseIncompleteResponse
                        );
                    }
                }
            }
            // Rows published without a terminal frame: re-execution is
            // refused (it would append a second result), but the session can
            // still be told what happened.
            let published_rows = ResponseProgress {
                bytes: 7,
                raw: false,
                terminal: false,
            };
            let guarded = ProxyServer::tr_response_action(action, published_rows, false);
            assert_ne!(guarded, TrAction::CloseIncompleteResponse);
            assert!(!matches!(
                guarded,
                TrAction::Reexecute | TrAction::ReplayThenReexecute
            ));
            // Backend-watch output may be a raw prefix from an earlier
            // Flush, even if this call has not sent any response bytes.
            assert_eq!(
                ProxyServer::tr_response_action(action, ResponseProgress::default(), true),
                TrAction::CloseIncompleteResponse
            );
            assert_eq!(
                ProxyServer::tr_response_action(action, ResponseProgress::default(), false),
                action
            );
        }
    }

    #[tokio::test]
    async fn tr_stream_fault_retains_visible_rows_and_terminal_frames() {
        for (bytes, terminal) in [
            (Vec::new(), false),
            (
                [frame(b'T', b"description"), frame(b'D', b"row")].concat(),
                false,
            ),
            (
                [
                    frame(b'T', b"description"),
                    frame(b'D', b"row"),
                    frame(b'C', b"SELECT 1\0"),
                ]
                .concat(),
                true,
            ),
            (frame(b'E', b"SERROR\0CXX000\0Mfailed\0\0"), true),
        ] {
            for capture in [false, true] {
                #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
                if capture {
                    continue;
                }
                let (mut backend, mut peer) = pair().await;
                let (client, mut recipient) = pair().await;
                let mut client = ClientStream::Plain(client);
                let session = make_test_session();
                let server = ProxyServer::new(test_config()).unwrap();
                let sent = bytes.clone();
                let feed = tokio::spawn(async move {
                    peer.write_all(&sent).await.unwrap();
                });
                let receive = tokio::spawn(async move {
                    let mut received = Vec::new();
                    recipient.read_to_end(&mut received).await.unwrap();
                    received
                });
                let failure = if capture {
                    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
                    {
                        ProxyServer::stream_until_ready_capture(
                            &mut client,
                            &mut backend,
                            &session,
                            RelayLimits {
                                client_write_timeout: Duration::from_secs(1),
                                backend_read_timeout: Duration::from_secs(1),
                                max_frame_bytes: usize::MAX,
                                response_timeout: None,
                                observation_bytes: usize::MAX,
                            },
                            16,
                            &server.state.metrics,
                        )
                        .await
                        .unwrap_err()
                    }
                    #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
                    {
                        unreachable!()
                    }
                } else {
                    ProxyServer::stream_until_ready(
                        &mut client,
                        &mut backend,
                        &session,
                        &server.state,
                    )
                    .await
                    .unwrap_err()
                };
                drop(client);
                feed.await.unwrap();
                assert_eq!(receive.await.unwrap(), bytes);
                assert_eq!(failure.progress.bytes, bytes.len() as u64);
                assert_eq!(failure.progress.terminal, terminal);
                assert!(!failure.progress.raw);
                let mut fault = None;
                BackendFault::set_response(&mut fault, "backend", &failure);
                assert_eq!(fault.unwrap().progress.bytes, bytes.len() as u64);
            }
        }
    }

    #[test]
    fn tr_extended_execute_identity_and_commit_boundaries() {
        fn parse(name: &str, sql: &str) -> Vec<u8> {
            frame(b'P', &[cstr(name), cstr(sql), vec![0, 0]].concat())
        }
        fn bind(portal: &str, name: &str) -> Vec<u8> {
            // Binary int32 parameter and binary results. Safety inspection
            // must leave these opaque bytes intact, including embedded NULs.
            frame(
                b'B',
                &[
                    cstr(portal),
                    cstr(name),
                    vec![0, 1, 0, 1, 0, 1, 0, 0, 0, 4, 0, 0, 0, 7, 0, 1, 0, 1],
                ]
                .concat(),
            )
        }
        fn execute(portal: &str) -> Vec<u8> {
            frame(b'E', &[cstr(portal), vec![0; 4]].concat())
        }
        let classify = |frames: Vec<Vec<u8>>| {
            ProxyServer::tr_extended_kind(&frames.concat(), None, 256, &TrReadPolicy::default())
        };
        for end in ["COMMIT", "END", "PREPARE TRANSACTION 'x'", "ROLLBACK"] {
            assert_eq!(
                classify(vec![
                    parse("a", "SELECT 1"),
                    bind("a", "a"),
                    execute("a"),
                    parse("b", end),
                    bind("b", "b"),
                    execute("b"),
                    frame(b'S', &[])
                ]),
                StmtKind::Commit,
                "{end}"
            );
        }
        // A portal owns the definition at Bind time, even after the unnamed
        // statement is replaced. Looking up the last Parse would miss COMMIT.
        assert_eq!(
            classify(vec![
                parse("", "COMMIT"),
                bind("saved", ""),
                parse("", "SELECT 1"),
                execute("saved")
            ]),
            StmtKind::Commit
        );
        assert_eq!(
            classify(vec![
                parse("", "SELECT 1"),
                bind("saved", ""),
                parse("", "COMMIT"),
                execute("saved")
            ]),
            StmtKind::Read
        );
        // The unexecuted Parse isn't sufficient evidence of the executed SQL.
        assert_eq!(
            classify(vec![parse("read", "SELECT 1"), execute("older")]),
            StmtKind::Commit
        );
        assert_eq!(
            classify(vec![bind("p", "older"), execute("p")]),
            StmtKind::Commit
        );
        assert_eq!(
            classify(vec![parse("s", "SELECT $1"), bind("p", "s"), execute("p")]),
            StmtKind::Read
        );
        assert_eq!(
            classify(vec![
                parse("s", "SELECT 1"),
                bind("p", "s"),
                frame(b'C', b"Pp\0"),
                execute("p")
            ]),
            StmtKind::Commit
        );
        let held = parse("", "COMMIT");
        let batch = [bind("", ""), execute(""), frame(b'S', &[])].concat();
        assert_eq!(
            ProxyServer::tr_extended_kind(&batch, Some(&held), 256, &TrReadPolicy::default()),
            StmtKind::Commit
        );
        let unnamed_read = [parse("", "SELECT 1"), bind("", ""), execute("")].concat();
        assert_eq!(
            ProxyServer::tr_extended_kind(&unnamed_read, None, 0, &TrReadPolicy::default()),
            StmtKind::Read
        );
        let named_read = [parse("s", "SELECT 1"), bind("p", "s"), execute("p")].concat();
        assert_eq!(
            ProxyServer::tr_extended_kind(&named_read, None, 0, &TrReadPolicy::default()),
            StmtKind::Commit
        );
        assert_eq!(
            ProxyServer::tr_extended_kind(&named_read, None, 1, &TrReadPolicy::default()),
            StmtKind::Read
        );
        let too_many_portals = [
            parse("s", "SELECT 1"),
            bind("p", "s"),
            bind("q", "s"),
            execute("q"),
        ]
        .concat();
        assert_eq!(
            ProxyServer::tr_extended_kind(&too_many_portals, None, 1, &TrReadPolicy::default()),
            StmtKind::Commit
        );
        // Every truncated header/body is opaque; a complete prefix that
        // already Executes COMMIT must remain so, even before final Sync.
        let wire = [held, batch].concat();
        let commit_end = wire.len() - 5;
        for offset in commit_end..=wire.len() {
            assert_eq!(
                ProxyServer::tr_extended_kind(&wire[..offset], None, 256, &TrReadPolicy::default()),
                StmtKind::Commit
            );
        }
        for bytes in [
            vec![b'E'],
            vec![b'E', 0, 0, 0, 3],
            vec![b'P', 255, 255, 255, 255],
        ] {
            assert_eq!(
                ProxyServer::tr_extended_kind(&bytes, None, 256, &TrReadPolicy::default()),
                StmtKind::Commit
            );
        }
    }

    /// A transport that accepts exactly `limit` bytes, then errors, stalls,
    /// or returns zero. This reproduces write_all losing its partial count.
    struct PrefixWriter {
        received: Vec<u8>,
        limit: usize,
        failure: u8,
    }

    impl tokio::io::AsyncWrite for PrefixWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            use std::task::Poll;
            let count = bytes.len().min(self.limit - self.received.len());
            if count == 0 {
                return match self.failure {
                    b'T' => Poll::Pending,
                    b'Z' => Poll::Ready(Ok(0)),
                    _ => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
                };
            }
            self.received.extend_from_slice(&bytes[..count]);
            Poll::Ready(Ok(count))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn tr_partial_extended_writes_never_authorize_commit_reexecution() {
        let mut batch = Vec::new();
        for sql in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"] {
            batch.extend(frame(b'P', &[cstr(""), cstr(sql), vec![0, 0]].concat()));
            batch.extend(frame(b'B', &[0; 8]));
            batch.extend(frame(b'E', &[0; 5]));
        }
        batch.extend(frame(b'S', &[]));
        let kind = ProxyServer::tr_extended_kind(&batch, None, 16, &TrReadPolicy::default());
        assert_eq!(kind, StmtKind::Commit);
        // Every possible byte offset includes each frontend frame boundary
        // and a complete COMMIT Execute followed by an incomplete final Sync.
        for limit in 0..batch.len() {
            let mut writer = PrefixWriter {
                received: Vec::new(),
                limit,
                failure: b'E',
            };
            let (_, phase) =
                ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_secs(1))
                    .await
                    .unwrap_err();
            assert_eq!(writer.received, batch[..limit]);
            for in_tx in [false, true] {
                assert_eq!(
                    ProxyServer::tr_decide(TrMode::Transaction, phase, in_tx, true, true, kind),
                    TrAction::ErrorAndContinue("08007"),
                    "offset={limit}, in_tx={in_tx}"
                );
            }
        }
        for failure in *b"TZ" {
            let mut writer = PrefixWriter {
                received: Vec::new(),
                limit: batch.len() - 5,
                failure,
            };
            let (_, phase) =
                ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_millis(5))
                    .await
                    .unwrap_err();
            assert_eq!(phase, FaultPhase::OutcomeUnknown);
            assert_eq!(writer.received, batch[..batch.len() - 5]);
        }
        let mut writer = PrefixWriter {
            received: Vec::new(),
            limit: batch.len(),
            failure: b'E',
        };
        assert!(
            ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_secs(1))
                .await
                .is_ok()
        );
        assert_eq!(writer.received, batch);
        // A real pre-dispatch connection failure remains safe to retry.
        assert_eq!(
            ProxyServer::tr_decide(
                TrMode::Transaction,
                FaultPhase::NotDelivered,
                false,
                false,
                false,
                kind
            ),
            TrAction::Reexecute
        );
    }

    /// One response per call (the replay/restore paths only ever have ONE
    /// response outstanding — statement, drain, statement, drain), with
    /// the status byte and error flag reported.
    #[tokio::test]
    async fn drain_until_ready_reports_status_and_errors() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        let mut wire = frame(b'C', &cstr("INSERT 0 1"));
        wire.extend_from_slice(&frame(b'Z', b"T"));
        b.write_all(&wire).await.unwrap();
        let (status, err) =
            ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
                .await
                .unwrap();
        assert_eq!((status, err), (b'T', false));
        // Error frames are reported, not fatal.
        let mut wire = frame(b'E', &[b'S', 0, b'C', 0, 0]);
        wire.extend_from_slice(&frame(b'Z', b"E"));
        b.write_all(&wire).await.unwrap();
        let (status, err) =
            ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
                .await
                .unwrap();
        assert_eq!((status, err), (b'E', true));
        // A COPY-in request cannot be satisfied during a replay.
        b.write_all(&frame(b'G', &[0, 0, 0])).await.unwrap();
        assert!(
            ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
                .await
                .is_err()
        );
        // EOF is an error.
        drop(b);
        assert!(
            ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn drain_until_ready_times_out_on_silent_backend() {
        let (mut a, _b) = tokio::io::duplex(64);
        let r = ProxyServer::drain_until_ready(&mut a, Duration::from_millis(50), usize::MAX, None)
            .await;
        assert!(matches!(r, Err(ProxyError::Network(_))));
    }

    #[tokio::test]
    async fn tr_run_discard_and_restore_session_state() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        // Backend: answers the first statement OK, rejects the second —
        // one response per received Query, like a real server.
        let backend = tokio::spawn(async move {
            let mut seen = Vec::new();
            for i in 0..2 {
                let mut hdr = [0u8; 5];
                b.read_exact(&mut hdr).await.unwrap();
                let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
                let mut body = vec![0u8; len - 4];
                b.read_exact(&mut body).await.unwrap();
                seen.push((
                    hdr[0],
                    crate::protocol::query_text(&body).unwrap().to_string(),
                ));
                let mut out = if i == 0 {
                    frame(b'C', &cstr("SET"))
                } else {
                    frame(b'E', &[b'C', 0, 0])
                };
                out.extend_from_slice(&frame(b'Z', b"I"));
                b.write_all(&out).await.unwrap();
            }
            seen
        });
        let gucs = vec!["SET a = 1".to_string(), "SET b = 2".to_string()];
        let r = ProxyServer::tr_restore_session_state(
            &mut a,
            &gucs,
            Duration::from_secs(5),
            Duration::from_secs(5),
            usize::MAX,
        )
        .await;
        assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");
        // Both Query frames reached the backend, in order.
        let seen = backend.await.unwrap();
        assert_eq!(
            seen,
            vec![
                (b'Q', "SET a = 1".to_string()),
                (b'Q', "SET b = 2".to_string())
            ]
        );
        // Empty list restores nothing and succeeds.
        let (mut a2, _b2) = tokio::io::duplex(64);
        assert_eq!(
            ProxyServer::tr_restore_session_state(
                &mut a2,
                &[],
                Duration::from_secs(1),
                Duration::from_secs(1),
                usize::MAX,
            )
            .await
            .unwrap(),
            0
        );
    }

    /// Fake PG backend: answers every simple Query with `CommandComplete` +
    /// `ReadyForQuery('T')`; a query containing "boom" gets ErrorResponse +
    /// RFQ('E'). Received query texts are pushed to `seen`.
    async fn fake_backend(
        listener: tokio::net::TcpListener,
        seen: Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = BytesMut::with_capacity(4096);
        loop {
            buf.reserve(4096);
            match sock.read_buf(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            while buf.len() >= 5 {
                let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                if buf.len() < len + 1 {
                    break;
                }
                let f = buf.split_to(len + 1);
                if f[0] == b'Q' {
                    let q = crate::protocol::query_text(&f[5..])
                        .unwrap_or("")
                        .to_string();
                    let fail = q.contains("boom");
                    seen.lock().unwrap().push(q);
                    let mut out = if fail {
                        frame(b'E', &[b'C', b'4', b'2', b'0', b'0', b'0', 0, 0])
                    } else {
                        frame(b'C', &cstr("OK"))
                    };
                    out.extend_from_slice(&frame(b'Z', if fail { b"E" } else { b"T" }));
                    sock.write_all(&out).await.unwrap();
                }
            }
        }
    }

    fn simple_log(sql: &str) -> StatementLog {
        StatementLog {
            sql: sql.to_string(),
            params: Vec::new(),
            result_checksum: None,
            executed_at: chrono::Utc::now(),
            extended: None,
        }
    }

    #[tokio::test]
    async fn tr_replay_transaction_replays_in_order_and_reports_rejections() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        tokio::spawn(fake_backend(listener, seen.clone()));
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        conns.insert(
            addr.clone(),
            BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
        );
        let registry: HashMap<String, bytes::Bytes> = HashMap::new();
        let entries = vec![
            simple_log("BEGIN"),
            simple_log("INSERT INTO t VALUES (1)"),
            simple_log("SAVEPOINT a"),
        ];
        let r = ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state)
            .await;
        assert!(r.is_ok());
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["BEGIN", "INSERT INTO t VALUES (1)", "SAVEPOINT a"]
        );
        // A rejected statement stops the replay with a Statement failure.
        let entries = vec![
            simple_log("BEGIN"),
            simple_log("INSERT boom"),
            simple_log("SELECT 1"),
        ];
        let r = ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state)
            .await;
        match r {
            Err(ReplayFailure::Statement(d)) => assert!(d.contains("2/3"), "{d}"),
            _ => panic!("expected statement failure"),
        }
        assert_eq!(
            seen.lock().unwrap().len(),
            5,
            "replay stopped at the failure"
        );
        // A missing connection is a Backend failure.
        let r = ProxyServer::tr_replay_transaction(
            &mut conns,
            "127.0.0.1:1",
            &entries,
            &registry,
            &state,
        )
        .await;
        assert!(matches!(r, Err(ReplayFailure::Backend(_))));
    }

    fn qmsg(sql: &str) -> Message {
        QueryMessage {
            query: sql.to_string(),
        }
        .encode()
    }

    /// Drive the recorder through a transaction: BEGIN/INSERT/SET are
    /// recorded (with write tracking), the statement cap marks the
    /// transaction non-replayable (+ metric), COMMIT releases the record
    /// and promotes transaction-scoped SETs, and the SET cap stops tracking.
    /// A transaction that ends in ROLLBACK discards its session state, so
    /// SETs made inside it must never be restored on the replacement backend.
    /// `ROLLBACK; <DML>` is the trap: for replay safety it classifies as a
    /// possible commit (the trailing statement commits in autocommit), and
    /// reusing that classification for the GUC decision would restore
    /// settings the database threw away.
    #[tokio::test]
    async fn tr_recorder_does_not_promote_gucs_of_a_rolled_back_transaction() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);

        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET work_mem = '512MB'"), &session, &state)
            .await;
        assert_eq!(
            tr.pending_tx_gucs,
            vec![GucOp::Set("SET work_mem = '512MB'".to_string())]
        );

        // Ends the transaction as a ROLLBACK, while the trailing statement
        // commits on its own — `StmtKind::Commit`, but nothing of the
        // transaction's own session state survived.
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(
            &mut tr,
            &qmsg("ROLLBACK; INSERT INTO t VALUES (1)"),
            &session,
            &state,
        )
        .await;
        assert!(
            tr.gucs.is_empty(),
            "a rolled-back transaction's SETs must not be restored: {:?}",
            tr.gucs
        );
        assert!(tr.pending_tx_gucs.is_empty(), "pending set must be dropped");

        // The same shape ending in a real COMMIT still promotes.
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET work_mem = '64MB'"), &session, &state)
            .await;
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
        assert_eq!(tr.gucs, vec!["SET work_mem = '64MB'"]);
    }

    /// TR-04: the three harness scenarios that 1.6.1 failed. GUC changes
    /// inside a transaction are deferred and follow PostgreSQL's own
    /// rollback semantics, including savepoints; variables are keyed by
    /// name so a repeated SET does not consume another cap slot.
    #[tokio::test]
    async fn tr_recorder_session_gucs_are_transactional_and_savepoint_scoped() {
        let mut config = test_config();
        config.limits.tr_max_session_set_statements = 1;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Session);
        async fn step(
            tr: &mut TrSession,
            session: &Arc<ClientSession>,
            state: &Arc<ServerState>,
            status: u8,
            sql: &str,
        ) {
            ProxyServer::note_ready_for_query(session, status, false);
            ProxyServer::tr_after_simple(tr, &qmsg(sql), session, state).await;
        }

        // guc_cap: a repeated SET of one variable replaces, cap 1 is enough
        // and the LATEST value is what gets restored.
        step(
            &mut tr,
            &session,
            &state,
            b'I',
            "SET application_name = 'first'",
        )
        .await;
        step(
            &mut tr,
            &session,
            &state,
            b'I',
            "SET application_name = 'latest'",
        )
        .await;
        assert_eq!(tr.gucs, vec!["SET application_name = 'latest'"]);
        assert!(!tr.guc_cap_hit);
        // A second DISTINCT variable exceeds the cap; the failover is then
        // refused rather than restoring a partial set.
        step(&mut tr, &session, &state, b'I', "SET work_mem = '64MB'").await;
        assert!(tr.guc_cap_hit);
        assert!(ProxyServer::tr_restore_preflight(&tr).is_err());
        step(&mut tr, &session, &state, b'I', "RESET ALL").await;
        assert!(tr.gucs.is_empty() && !tr.guc_cap_hit);
        assert!(ProxyServer::tr_restore_preflight(&tr).is_ok());

        // guc_savepoint: a SET after a savepoint that is rolled back to is
        // undone; the base value committed before the transaction survives.
        step(
            &mut tr,
            &session,
            &state,
            b'I',
            "SET application_name = 'base'",
        )
        .await;
        step(&mut tr, &session, &state, b'T', "BEGIN").await;
        step(&mut tr, &session, &state, b'T', "SAVEPOINT s").await;
        step(
            &mut tr,
            &session,
            &state,
            b'T',
            "SET application_name = 'undone'",
        )
        .await;
        step(&mut tr, &session, &state, b'T', "ROLLBACK TO s").await;
        step(&mut tr, &session, &state, b'I', "COMMIT").await;
        assert_eq!(tr.gucs, vec!["SET application_name = 'base'"]);
        assert!(tr.pending_tx_gucs.is_empty() && tr.tx_savepoints.is_empty());

        // RELEASE keeps the SET made after the savepoint.
        step(&mut tr, &session, &state, b'T', "BEGIN").await;
        step(&mut tr, &session, &state, b'T', "SAVEPOINT \"S2\"").await;
        step(
            &mut tr,
            &session,
            &state,
            b'T',
            "SET application_name = 'kept'",
        )
        .await;
        step(&mut tr, &session, &state, b'T', "RELEASE SAVEPOINT \"S2\"").await;
        step(&mut tr, &session, &state, b'I', "COMMIT").await;
        assert_eq!(tr.gucs, vec!["SET application_name = 'kept'"]);

        // guc_reset_rollback: RESET ALL inside a rolled-back transaction
        // is undone — the committed value is still restored.
        step(&mut tr, &session, &state, b'T', "BEGIN").await;
        step(&mut tr, &session, &state, b'T', "RESET ALL").await;
        assert_eq!(tr.pending_tx_gucs, vec![GucOp::ResetAll]);
        assert_eq!(tr.gucs, vec!["SET application_name = 'kept'"], "deferred");
        step(&mut tr, &session, &state, b'I', "ROLLBACK").await;
        assert_eq!(tr.gucs, vec!["SET application_name = 'kept'"]);
        // ...and applied when the transaction commits.
        step(&mut tr, &session, &state, b'T', "BEGIN").await;
        step(&mut tr, &session, &state, b'T', "RESET application_name").await;
        step(&mut tr, &session, &state, b'I', "COMMIT").await;
        assert!(tr.gucs.is_empty());
    }

    #[test]
    fn tr_guc_name_identifies_the_variable() {
        let n = |sql: &str| ProxyServer::tr_guc_name(sql);
        assert_eq!(
            n("SET application_name = 'x'").as_deref(),
            Some("application_name")
        );
        assert_eq!(
            n("set Application_Name to 'x';").as_deref(),
            Some("application_name")
        );
        assert_eq!(
            n("SET SESSION work_mem = '1MB'").as_deref(),
            Some("work_mem")
        );
        assert_eq!(n("SET TIME ZONE 'UTC'").as_deref(), Some("timezone"));
        assert_eq!(n("SET timezone TO 'UTC'").as_deref(), Some("timezone"));
        assert_eq!(n("SET SCHEMA 'public'").as_deref(), Some("search_path"));
        assert_eq!(n("SET NAMES 'UTF8'").as_deref(), Some("client_encoding"));
        assert_eq!(n("SET ROLE readonly").as_deref(), Some("role"));
        assert_eq!(
            n("SET SESSION AUTHORIZATION bob").as_deref(),
            Some("session_authorization")
        );
        assert_eq!(
            n("RESET application_name").as_deref(),
            Some("application_name")
        );
        assert_eq!(n("RESET ROLE").as_deref(), Some("role"));
        assert_eq!(n("SET \"Quoted.Var\" = 1").as_deref(), Some("quoted.var"));
        assert_eq!(n("RESET ALL"), None);
        assert_eq!(n("SELECT 1"), None);
    }

    /// TR-06: the observation digest is order-sensitive over the frames the
    /// client sees, ignores asynchronous frames, and reports overflow
    /// instead of a digest once the byte budget is exceeded.
    #[test]
    fn observation_digest_is_ordered_bounded_and_ignores_async_frames() {
        let t = frame(b'T', b"desc");
        let d1 = frame(b'D', b"row1");
        let d2 = frame(b'D', b"row2");
        let c = frame(b'C', b"SELECT 2\0");
        let digest = |frames: &[&[u8]], cap: usize| {
            let mut o = Observation::new(cap);
            for f in frames {
                o.note(f);
            }
            o.finish()
        };
        let a = digest(&[&t, &d1, &d2, &c], usize::MAX).unwrap();
        assert_eq!(
            a,
            digest(&[&t, &d1, &d2, &c], usize::MAX).unwrap(),
            "deterministic"
        );
        assert_ne!(
            a,
            digest(&[&t, &d2, &d1, &c], usize::MAX).unwrap(),
            "order matters"
        );
        assert_ne!(
            a,
            digest(&[&t, &d1, &c], usize::MAX).unwrap(),
            "row count matters"
        );
        // Notices, parameter status and notifications are not observed.
        let n = frame(b'N', b"SNOTICE\0\0");
        let s_ = frame(b'S', b"application_name\0x\0");
        let a_ = frame(b'A', b"\0\0\0\x01chan\0\0");
        assert_eq!(
            a,
            digest(&[&n, &t, &s_, &d1, &a_, &d2, &c], usize::MAX).unwrap()
        );
        // Budget: the total of observed frame bytes must fit.
        let total = t.len() + d1.len() + d2.len() + c.len();
        assert!(digest(&[&t, &d1, &d2, &c], total).is_some());
        assert!(
            digest(&[&t, &d1, &d2, &c], total - 1).is_none(),
            "over budget"
        );
        assert_ne!(a, 0, "zero is reserved for none");
    }

    /// A statement executed inside a recorded transaction carries the
    /// response digest the relay published; a response over the budget
    /// makes the transaction non-replayable; the opening BEGIN carries none.
    #[tokio::test]
    async fn tr_recorder_attaches_observations_and_refuses_unverifiable() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);

        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        // The relay observed the SELECT's response.
        let mut o = Observation::new(usize::MAX);
        o.note(&frame(b'T', b"d"));
        o.note(&frame(b'D', b"r"));
        o.note(&frame(b'C', b"SELECT 1\0"));
        let d = o.finish().unwrap();
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::note_observation(&session, Some(&o));
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
        {
            let ts = session.tx_state.read().await;
            assert_eq!(ts.statements.len(), 2);
            assert_eq!(
                ts.statements[0].result_checksum, None,
                "BEGIN: nothing to verify"
            );
            assert_eq!(ts.statements[1].result_checksum, Some(d));
            assert!(!ts.non_replayable);
        }
        // Over-budget response: unverifiable, so the transaction is dropped.
        let mut big = Observation::new(4);
        big.note(&frame(b'D', b"too large"));
        assert!(big.finish().is_none());
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::note_observation(&session, Some(&big));
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 2"), &session, &state).await;
        let ts = session.tx_state.read().await;
        assert!(ts.non_replayable && ts.statements.is_empty());
    }

    /// Snapshot-pinning isolation levels are never replayed: explicitly on
    /// BEGIN/START, via SET TRANSACTION, or inherited from a tracked
    /// `default_transaction_isolation`.
    #[test]
    fn tr_snapshot_sensitive_transactions_are_detected() {
        let f = |sql: &str, gucs: &[&str]| {
            let g: Vec<String> = gucs.iter().map(|x| x.to_string()).collect();
            ProxyServer::tr_snapshot_sensitive(sql, &g)
        };
        assert!(f("BEGIN ISOLATION LEVEL SERIALIZABLE", &[]));
        assert!(f("begin isolation level repeatable read", &[]));
        assert!(f(
            "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
            &[]
        ));
        assert!(f("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[]));
        assert!(f("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'", &[]));
        assert!(f(
            "BEGIN",
            &["SET default_transaction_isolation = 'serializable'"]
        ));
        assert!(!f(
            "BEGIN",
            &["SET default_transaction_isolation = 'read committed'"]
        ));
        assert!(!f("BEGIN", &["SET application_name = 'x'"]));
        assert!(!f("BEGIN ISOLATION LEVEL READ COMMITTED", &[]));
        assert!(!f(
            "SELECT 1",
            &["SET default_transaction_isolation = 'serializable'"]
        ));
        assert!(!f("SET application_name = 'serializable'", &[]));
    }

    /// Replay verifies each statement against the digest the client saw:
    /// a divergent result is a protocol error (surfaced as 40001 + ROLLBACK
    /// by the caller); a matching one passes.
    #[tokio::test]
    async fn tr_run_discard_verifies_the_observed_result() {
        use tokio::io::AsyncReadExt as _;
        use tokio::io::AsyncWriteExt as _;
        async fn backend(mut b: tokio::io::DuplexStream, frames: Vec<u8>) {
            let mut q = vec![0u8; 5];
            b.read_exact(&mut q).await.unwrap();
            let len = u32::from_be_bytes([q[1], q[2], q[3], q[4]]) as usize;
            let mut rest = vec![0u8; len - 4];
            b.read_exact(&mut rest).await.unwrap();
            b.write_all(&frames).await.unwrap();
        }
        let good = [
            frame(b'T', b"d"),
            frame(b'D', b"r"),
            frame(b'C', b"SELECT 1\0"),
        ]
        .concat();
        let mut want = Observation::new(usize::MAX);
        want.note(&frame(b'T', b"d"));
        want.note(&frame(b'D', b"r"));
        want.note(&frame(b'C', b"SELECT 1\0"));
        let want = want.finish().unwrap();
        let rfq = frame(b'Z', b"T");

        let (mut a, b) = tokio::io::duplex(4096);
        tokio::spawn(backend(b, [good.clone(), rfq.clone()].concat()));
        let r = ProxyServer::tr_run_discard(
            &mut a,
            "SELECT 1",
            Duration::from_secs(1),
            Duration::from_secs(1),
            usize::MAX,
            Some((want, usize::MAX)),
        )
        .await;
        assert_eq!(r.unwrap(), b'T', "matching observation replays");

        let diverged = [
            frame(b'T', b"d"),
            frame(b'D', b"OTHER"),
            frame(b'C', b"SELECT 1\0"),
        ]
        .concat();
        let (mut a2, b2) = tokio::io::duplex(4096);
        tokio::spawn(backend(b2, [diverged, rfq.clone()].concat()));
        let r = ProxyServer::tr_run_discard(
            &mut a2,
            "SELECT 1",
            Duration::from_secs(1),
            Duration::from_secs(1),
            usize::MAX,
            Some((want, usize::MAX)),
        )
        .await;
        assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");

        // No expectation recorded (e.g. the BEGIN): nothing is verified.
        let (mut a3, b3) = tokio::io::duplex(4096);
        tokio::spawn(backend(b3, [good, rfq].concat()));
        assert!(ProxyServer::tr_run_discard(
            &mut a3,
            "SELECT 1",
            Duration::from_secs(1),
            Duration::from_secs(1),
            usize::MAX,
            None,
        )
        .await
        .is_ok());
    }

    /// The whole-response deadline shrinks the per-read budget and refuses
    /// once passed; disabled it leaves the per-read timeout alone.
    #[test]
    fn read_budget_honours_the_response_deadline() {
        let per = Duration::from_secs(30);
        assert_eq!(read_budget(per, None).unwrap(), per);
        let soon = tokio::time::Instant::now() + Duration::from_millis(200);
        assert!(read_budget(per, Some(soon)).unwrap() <= Duration::from_millis(200));
        let past = tokio::time::Instant::now() - Duration::from_millis(1);
        assert!(matches!(
            read_budget(per, Some(past)),
            Err(ProxyError::Network(_))
        ));
    }

    /// A recovery deadline already in the past fails the primary wait
    /// immediately instead of polling for `write_timeout_secs`.
    #[tokio::test]
    async fn select_primary_until_respects_a_passed_deadline() {
        let mut config = test_config();
        config.write_timeout_secs = 60;
        // With every primary disabled only the deadline can end the wait.
        for n in &mut config.nodes {
            n.enabled = false;
        }
        let server = ProxyServer::new(config.clone()).unwrap();
        let session = make_test_session();
        let started = std::time::Instant::now();
        let r = ProxyServer::select_primary_until(
            &session,
            &server.state,
            &config,
            tokio::time::Instant::now(),
        )
        .await;
        assert!(matches!(r, Err(ProxyError::NoHealthyNodes)), "{r:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn advance_health_applies_success_threshold_on_recovery() {
        // Healthy nodes stay healthy on the first success.
        assert_eq!(
            ProxyServer::advance_health(true, 0, 0, true, 3, 2),
            (true, 1, 0)
        );
        // An unhealthy node needs `success_threshold` consecutive successes.
        assert_eq!(
            ProxyServer::advance_health(false, 0, 3, true, 3, 2),
            (false, 1, 0)
        );
        assert_eq!(
            ProxyServer::advance_health(false, 1, 0, true, 3, 2),
            (true, 2, 0)
        );
        // Failures count toward `failure_threshold` and reset successes.
        assert_eq!(
            ProxyServer::advance_health(true, 5, 0, false, 3, 2),
            (true, 0, 1)
        );
        assert_eq!(
            ProxyServer::advance_health(true, 5, 2, false, 3, 2),
            (false, 0, 3)
        );
    }

    #[cfg(feature = "lag-routing")]
    #[test]
    fn lag_policy_treats_unknown_as_fresh_unless_strict() {
        // Known lag over the threshold is excluded (when a threshold is set).
        assert!(ProxyServer::lag_excludes_standby(Some(100), 10, false));
        assert!(!ProxyServer::lag_excludes_standby(Some(5), 10, false));
        assert!(!ProxyServer::lag_excludes_standby(Some(100), 0, false));
        // Unknown lag: allowed by default, excluded under the strict policy.
        assert!(!ProxyServer::lag_excludes_standby(None, 10, false));
        assert!(ProxyServer::lag_excludes_standby(None, 10, true));
    }

    #[test]
    fn parse_pg_lsn_roundtrip() {
        assert_eq!(parse_pg_lsn("0/0"), Some(0));
        assert_eq!(parse_pg_lsn("0/16B3748"), Some(0x016B_3748));
        assert_eq!(parse_pg_lsn("1/0"), Some(1 << 32));
        assert_eq!(
            parse_pg_lsn("16/B374D848"),
            Some((0x16 << 32) | 0xB374_D848)
        );
        assert_eq!(parse_pg_lsn("not-an-lsn"), None);
        assert_eq!(parse_pg_lsn(""), None);
    }

    #[test]
    fn pick_read_node_honors_each_strategy() {
        use crate::config::Strategy;

        // Round-robin cycles deterministically.
        let rr = |t: u64| {
            ProxyServer::pick_read_node(Strategy::RoundRobin, &[1, 1], &[0.0, 0.0], &[0, 0], t)
                .unwrap()
        };
        assert_eq!((rr(0), rr(1), rr(2), rr(3)), (0, 1, 0, 1));

        // Weighted round-robin: 1:3 over 400 tickets.
        let mut counts = [0usize; 2];
        for t in 0..400 {
            let i = ProxyServer::pick_read_node(
                Strategy::WeightedRoundRobin,
                &[1, 3],
                &[0.0, 0.0],
                &[0, 0],
                t,
            )
            .unwrap();
            counts[i] += 1;
        }
        assert!(
            counts[1] > counts[0] * 2,
            "weighted distribution wrong: {counts:?}"
        );

        // Least-connections and latency pick the better node.
        assert_eq!(
            ProxyServer::pick_read_node(
                Strategy::LeastConnections,
                &[1, 1],
                &[1.0, 9.0],
                &[5, 1],
                0
            ),
            Some(1)
        );
        assert_eq!(
            ProxyServer::pick_read_node(Strategy::LatencyBased, &[1, 1], &[50.0, 5.0], &[0, 0], 0),
            Some(1)
        );

        // Random stays in range and samples both nodes.
        let mut seen = [false; 2];
        for t in 0..64 {
            let i = ProxyServer::pick_read_node(Strategy::Random, &[1, 1], &[0.0, 0.0], &[0, 0], t)
                .unwrap();
            assert!(i < 2);
            seen[i] = true;
        }
        assert!(seen[0] && seen[1], "random must sample both nodes");

        // Power-of-two-choices favours the unloaded node.
        let mut p2c = [0usize; 2];
        for t in 0..200 {
            let i = ProxyServer::pick_read_node(
                Strategy::PowerOfTwo,
                &[1, 1],
                &[1.0, 1.0],
                &[0, 20],
                t,
            )
            .unwrap();
            p2c[i] += 1;
        }
        assert!(
            p2c[0] > p2c[1],
            "p2c should prefer the unloaded node: {p2c:?}"
        );
    }

    /// V-01: every advertised capability that the config enables must be
    /// actually wired on the running state — not merely present as a struct
    /// field. Deleting a construction hook (or gating it out) fails this test.
    #[test]
    fn enabled_capabilities_are_actually_wired() {
        let mut config = test_config();
        // Unconditional mutation keeps `mut` meaningful in minimal builds.
        config.tr_enabled = true;

        #[cfg(feature = "query-cache")]
        {
            config.cache.enabled = true;
        }
        #[cfg(feature = "routing-hints")]
        {
            config.routing_hints.enabled = true;
        }
        #[cfg(feature = "rate-limiting")]
        {
            config.rate_limit.enabled = true;
        }
        #[cfg(feature = "circuit-breaker")]
        {
            config.circuit_breaker.enabled = true;
        }
        #[cfg(feature = "query-analytics")]
        {
            config.analytics.enabled = true;
        }

        let server = ProxyServer::new(config).unwrap();
        let state = &server.state;
        // Feature-independent: a fresh server has no sessions. Keeps the
        // binding used in a no-default-features build too.
        assert!(state.sessions.is_empty(), "fresh server has sessions");

        #[cfg(feature = "pool-modes")]
        assert!(
            state.pool_manager.is_some(),
            "pool-modes compiled but not wired"
        );

        #[cfg(feature = "query-cache")]
        assert!(
            state.query_cache.is_some(),
            "query-cache enabled but not wired"
        );
        #[cfg(feature = "routing-hints")]
        assert!(
            state.hint_parser.is_some(),
            "routing-hints enabled but not wired"
        );
        #[cfg(feature = "rate-limiting")]
        assert!(
            state.rate_limiter.is_some(),
            "rate-limiting enabled but not wired"
        );
        #[cfg(feature = "circuit-breaker")]
        assert!(
            state.circuit_breaker.is_some(),
            "circuit-breaker enabled but not wired"
        );
        #[cfg(feature = "query-analytics")]
        assert!(
            state.analytics.is_some(),
            "query-analytics enabled but not wired"
        );
    }

    #[test]
    fn authoritative_leader_requires_an_enabled_configured_node() {
        let mut config = ProxyConfig::default();
        config.add_node("primary-a:5432", "primary").unwrap();
        config.add_node("primary-b:5432", "primary").unwrap();

        let tracker = PrimaryTracker::new_standalone();
        // No provider answer yet: wait, never fall back to roles.
        assert_eq!(ProxyServer::authoritative_leader(&config, &tracker), None);

        tracker.set_primary(uuid::Uuid::new_v4(), "primary-b:5432".to_string());
        assert_eq!(
            ProxyServer::authoritative_leader(&config, &tracker).as_deref(),
            Some("primary-b:5432")
        );

        // A provider answer that is not a configured node is ignored.
        tracker.set_primary(uuid::Uuid::new_v4(), "unknown:5432".to_string());
        assert_eq!(ProxyServer::authoritative_leader(&config, &tracker), None);

        // A disabled configured node is not eligible either.
        tracker.set_primary(uuid::Uuid::new_v4(), "primary-a:5432".to_string());
        config.nodes[0].enabled = false;
        assert_eq!(ProxyServer::authoritative_leader(&config, &tracker), None);
    }

    #[test]
    fn remember_batch_name_dedups_and_caps() {
        let mut list: Vec<String> = Vec::new();
        ProxyServer::remember_batch_name(&mut list, "s1", 3);
        ProxyServer::remember_batch_name(&mut list, "s1", 3);
        ProxyServer::remember_batch_name(&mut list, "s2", 3);
        ProxyServer::remember_batch_name(&mut list, "s3", 3);
        ProxyServer::remember_batch_name(&mut list, "s4", 3);
        assert_eq!(
            list,
            vec!["s1".to_string(), "s2".to_string(), "s3".to_string()]
        );
    }

    #[test]
    fn severity_response_frames_fields_once() {
        let bytes = ProxyServer::create_severity_response("ERROR", "57P01", "backend down");
        assert_eq!(bytes[0], b'E');
        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        assert_eq!(len + 1, bytes.len(), "frame length covers payload + itself");
        let payload = &bytes[5..];
        for field in [
            b"SERROR\0".as_slice(),
            b"VERROR\0".as_slice(),
            b"C57P01\0".as_slice(),
            b"Mbackend down\0".as_slice(),
        ] {
            assert!(
                payload.windows(field.len()).any(|w| w == field),
                "missing field {:?}",
                field
            );
        }
        assert_eq!(*payload.last().unwrap(), 0, "payload terminator");
    }

    #[tokio::test]
    async fn tr_recorder_tracks_transaction_gucs_and_caps() {
        let mut config = test_config();
        config.limits.tr_max_replay_statements = 3;
        config.limits.tr_max_session_set_statements = 2;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);

        // Autocommit SET -> tracked immediately. SET LOCAL -> ignored.
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(
            &mut tr,
            &qmsg("SET application_name = 'x'"),
            &session,
            &state,
        )
        .await;
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET LOCAL a = 1"), &session, &state).await;
        assert_eq!(tr.gucs, vec!["SET application_name = 'x'"]);
        // A rejected SET is not tracked.
        ProxyServer::note_ready_for_query(&session, b'I', true);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET bogus = 1"), &session, &state).await;
        assert_eq!(tr.gucs.len(), 1);
        assert!(session.tx_state.read().await.statements.is_empty());

        // BEGIN opens the record; INSERT marks writes.
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        {
            let ts = session.tx_state.read().await;
            assert!(ts.in_transaction && ts.tx_id.is_some());
            assert_eq!(ts.statements.len(), 1);
            assert!(!ts.has_writes && ts.read_only && !ts.non_replayable);
        }
        ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT INTO t VALUES (1)"), &session, &state)
            .await;
        // SET inside the transaction is pending until COMMIT.
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET b = 2"), &session, &state).await;
        assert_eq!(
            tr.pending_tx_gucs,
            vec![GucOp::Set("SET b = 2".to_string())]
        );
        {
            let ts = session.tx_state.read().await;
            assert_eq!(ts.statements.len(), 3);
            assert!(ts.has_writes && !ts.read_only);
            assert_eq!(
                ts.replay_bytes,
                "BEGIN".len() + "INSERT INTO t VALUES (1)".len() + "SET b = 2".len()
            );
        }
        // Fourth statement exceeds tr_max_replay_statements = 3.
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
        {
            let ts = session.tx_state.read().await;
            assert!(ts.non_replayable);
            assert!(ts.statements.is_empty(), "record released at the cap");
            assert!(ts.in_transaction, "still inside the transaction");
        }
        assert_eq!(
            state.metrics.tr.replay_cap_exceeded.load(Ordering::Relaxed),
            1
        );
        // COMMIT -> record released, pending SET promoted (cap 2 reached).
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
        assert!(!session.tx_state.read().await.in_transaction);
        assert_eq!(tr.gucs, vec!["SET application_name = 'x'", "SET b = 2"]);
        assert!(tr.pending_tx_gucs.is_empty());
        // Third SET hits tr_max_session_set_statements = 2 -> cap metric.
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET c = 3"), &session, &state).await;
        assert!(tr.guc_cap_hit);
        assert_eq!(tr.gucs.len(), 2);
        assert_eq!(
            state
                .metrics
                .tr
                .session_set_cap_exceeded
                .load(Ordering::Relaxed),
            1
        );
        // RESET ALL wipes tracking and lifts the cap.
        ProxyServer::tr_after_simple(&mut tr, &qmsg("RESET ALL"), &session, &state).await;
        assert!(tr.gucs.is_empty() && !tr.guc_cap_hit);

        // A rolled-back transaction drops its pending SETs, and a failed
        // transaction ('E') is never replayable.
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET d = 4"), &session, &state).await;
        ProxyServer::note_ready_for_query(&session, b'E', true);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT boom"), &session, &state).await;
        assert!(session.tx_state.read().await.non_replayable);
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("ROLLBACK"), &session, &state).await;
        assert!(tr.gucs.is_empty() && tr.pending_tx_gucs.is_empty());
    }

    /// `session` mode records no transaction statements (no lock, no
    /// allocation on the in-transaction path) but still tracks SETs;
    /// `none` tracks nothing.
    #[tokio::test]
    async fn tr_recorder_mode_gating() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Session);
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT INTO t VALUES (1)"), &session, &state)
            .await;
        assert!(session.tx_state.read().await.statements.is_empty());
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SET a = 1"), &session, &state).await;
        assert_eq!(tr.gucs, vec!["SET a = 1"]);

        let mut none = TrSession::new(TrMode::None);
        ProxyServer::tr_after_simple(&mut none, &qmsg("SET a = 1"), &session, &state).await;
        assert!(none.gucs.is_empty());
    }

    /// A tenant/rewrite transform taints the transaction: recorded text is
    /// not what executed, so it must not be replayed.
    #[tokio::test]
    async fn tr_recorder_taint_marks_non_replayable() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        session
            .tr_replay_tainted
            .store(true, std::sync::atomic::Ordering::Relaxed);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
        let ts = session.tx_state.read().await;
        assert!(ts.non_replayable && ts.statements.is_empty());
        assert!(!session
            .tr_replay_tainted
            .load(std::sync::atomic::Ordering::Relaxed));
    }

    /// Extended-protocol cycles: Flush-terminated batches accumulate and the
    /// Sync closes them into one replay entry carrying the raw frames.
    #[tokio::test]
    async fn tr_recorder_extended_cycle_accumulates_until_sync() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        let registry: HashMap<String, bytes::Bytes> = HashMap::new();
        // Already inside a transaction (BEGIN recorded via simple protocol).
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
        let flush_batch = bytes::Bytes::from(
            [
                frame(b'B', &[b"\0\0".to_vec(), vec![0; 6]].concat()),
                frame(b'E', &[0; 5]),
                frame(b'H', &[]),
            ]
            .concat(),
        );
        let sync_batch = bytes::Bytes::from(frame(b'S', &[]));
        let unnamed = (
            bytes::Bytes::from(frame(
                b'P',
                &[cstr(""), cstr("INSERT INTO t VALUES ($1)"), vec![0; 2]].concat(),
            )),
            bytes::Bytes::from_static(b"sig"),
        );
        ProxyServer::tr_after_extended(
            &mut tr,
            &flush_batch,
            Some(&unnamed),
            Some("INSERT INTO t VALUES ($1)"),
            false,
            &[],
            &[],
            &registry,
            &session,
            &state,
        )
        .await;
        assert!(tr.ext_cycle.is_some());
        assert_eq!(session.tx_state.read().await.statements.len(), 1);
        ProxyServer::tr_after_extended(
            &mut tr,
            &sync_batch,
            None,
            None,
            true,
            &["s1".to_string()],
            &["s1".to_string()],
            &registry,
            &session,
            &state,
        )
        .await;
        assert!(tr.ext_cycle.is_none());
        let ts = session.tx_state.read().await;
        assert_eq!(ts.statements.len(), 2);
        assert!(ts.has_writes);
        let ext = ts.statements[1].extended.as_ref().expect("extended entry");
        assert_eq!(
            ext.frames,
            [flush_batch.as_ref(), sync_batch.as_ref()].concat()
        );
        assert_eq!(ext.unnamed_parse.as_ref(), Some(&unnamed.0));
        assert_eq!(ext.defines, vec!["s1"]);
        assert_eq!(ts.statements[1].sql, "INSERT INTO t VALUES ($1)");
        assert_eq!(
            ts.replay_bytes,
            "BEGIN".len() + flush_batch.len() + sync_batch.len() + unnamed.0.len()
        );
    }

    #[tokio::test]
    async fn tr_recorder_does_not_retain_a_committed_transaction_prefix() {
        let server = ProxyServer::new(test_config()).unwrap();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        ProxyServer::note_ready_for_query(&session, b'T', false);
        for sql in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT; BEGIN"] {
            ProxyServer::tr_after_simple(&mut tr, &qmsg(sql), &session, &server.state).await;
        }
        let ts = session.tx_state.read().await;
        assert!(ts.non_replayable);
        assert!(ts.statements.is_empty());
        assert_eq!(ts.replay_bytes, 0);
    }

    #[tokio::test]
    async fn tr_recorder_preserves_later_held_parse_positions() {
        for first_held in [false, true] {
            let server = ProxyServer::new(test_config()).unwrap();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            let registry = HashMap::new();
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
            let mut expected = Vec::new();
            for (index, sql) in ["SELECT 1", "SELECT 2", "SELECT 3"].iter().enumerate() {
                let parse =
                    bytes::Bytes::from(frame(b'P', &[cstr(""), cstr(sql), vec![0; 2]].concat()));
                let mut frames = Vec::new();
                let held = if first_held || index > 0 {
                    Some((parse.clone(), bytes::Bytes::new()))
                } else {
                    frames.extend_from_slice(&parse);
                    None
                };
                frames.extend_from_slice(&frame(b'B', &[0; 8]));
                frames.extend_from_slice(&frame(b'E', &[0; 5]));
                frames.extend_from_slice(&frame(if index == 2 { b'S' } else { b'H' }, &[]));
                if held.is_some() {
                    expected.extend_from_slice(&parse);
                }
                expected.extend_from_slice(&frames);
                ProxyServer::tr_after_extended(
                    &mut tr,
                    &bytes::Bytes::from(frames),
                    held.as_ref(),
                    Some(sql),
                    index == 2,
                    &[],
                    &[],
                    &registry,
                    &session,
                    &server.state,
                )
                .await;
            }
            let ts = session.tx_state.read().await;
            assert!(!ts.non_replayable);
            assert_eq!(ts.statements.len(), 2);
            let recorded = ts.statements[1].extended.as_ref().unwrap();
            assert_eq!(
                [
                    recorded.unnamed_parse.as_deref().unwrap_or(&[]),
                    &recorded.frames
                ]
                .concat(),
                expected,
            );
            assert_eq!(ts.replay_bytes, "BEGIN".len() + expected.len());
        }
    }

    #[tokio::test]
    async fn tr_recorder_bounds_open_flush_cycle_before_sync() {
        for statement_cap in [1, 256] {
            let mut config = test_config();
            config.limits.tr_max_replay_bytes = 64;
            config.limits.tr_max_replay_statements = statement_cap;
            let server = ProxyServer::new(config).unwrap();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
            let batch = bytes::Bytes::from(frame(b'H', &[]));
            for _ in 0..32 {
                ProxyServer::tr_after_extended(
                    &mut tr,
                    &batch,
                    None,
                    None,
                    false,
                    &[],
                    &[],
                    &HashMap::new(),
                    &session,
                    &server.state,
                )
                .await;
                let retained = tr.ext_cycle.as_ref().map_or(0, |c| c.frames.len());
                assert!(retained + session.tx_state.read().await.replay_bytes <= 64);
            }
            assert!(tr.ext_cycle.is_none());
            let ts = session.tx_state.read().await;
            assert!(ts.non_replayable && ts.statements.is_empty());
            assert_eq!(ts.replay_bytes, 0);
            assert_eq!(
                server
                    .state
                    .metrics
                    .tr
                    .replay_cap_exceeded
                    .load(Ordering::Relaxed),
                1
            );
        }
    }

    #[tokio::test]
    async fn tr_recorder_retains_begin_before_first_sync_or_refuses_incomplete_history() {
        for (cap, simple_end) in [(4096, false), (1, false), (4096, true)] {
            let mut config = test_config();
            config.limits.tr_max_replay_bytes = cap;
            let server = ProxyServer::new(config).unwrap();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            let parse =
                bytes::Bytes::from(frame(b'P', &[cstr(""), cstr("BEGIN"), vec![0; 2]].concat()));
            let held = (parse.clone(), bytes::Bytes::new());
            let flush = bytes::Bytes::from(
                [frame(b'B', &[0; 8]), frame(b'E', &[0; 5]), frame(b'H', &[])].concat(),
            );
            // No RFQ has exposed BEGIN yet: both the client-visible and
            // recorder status still say Idle when the Flush is retained.
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_extended(
                &mut tr,
                &flush,
                Some(&held),
                Some("BEGIN"),
                false,
                &[],
                &[],
                &HashMap::new(),
                &session,
                &server.state,
            )
            .await;
            ProxyServer::note_ready_for_query(&session, b'T', false);
            let sync = bytes::Bytes::from(frame(b'S', &[]));
            if simple_end {
                ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &server.state)
                    .await;
            } else {
                ProxyServer::tr_after_extended(
                    &mut tr,
                    &sync,
                    None,
                    None,
                    true,
                    &[],
                    &[],
                    &HashMap::new(),
                    &session,
                    &server.state,
                )
                .await;
            }
            let ts = session.tx_state.read().await;
            if cap == 1 || simple_end {
                assert!(ts.non_replayable && ts.statements.is_empty());
            } else {
                assert!(!ts.non_replayable);
                assert_eq!(ts.statements.len(), 1);
                let entry = ts.statements[0].extended.as_ref().unwrap();
                assert_eq!(
                    [entry.unnamed_parse.as_deref().unwrap_or(&[]), &entry.frames].concat(),
                    [parse.as_ref(), flush.as_ref(), sync.as_ref()].concat(),
                );
            }
            assert!(tr.ext_cycle.is_none());
            assert!(!tr.ext_cycle_dropped);
        }
    }

    #[tokio::test]
    async fn tr_recorder_idle_flush_cap_does_not_taint_next_transaction() {
        let mut config = test_config();
        config.limits.tr_max_replay_bytes = 64;
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        ProxyServer::note_ready_for_query(&session, b'I', false);
        for _ in 0..16 {
            ProxyServer::tr_after_extended(
                &mut tr,
                &bytes::Bytes::from(frame(b'H', &[])),
                None,
                None,
                false,
                &[],
                &[],
                &HashMap::new(),
                &session,
                &server.state,
            )
            .await;
        }
        assert!(tr.ext_cycle_dropped);
        ProxyServer::tr_after_extended(
            &mut tr,
            &bytes::Bytes::from(frame(b'S', &[])),
            None,
            None,
            true,
            &[],
            &[],
            &HashMap::new(),
            &session,
            &server.state,
        )
        .await;
        assert!(!session.tx_state.read().await.non_replayable);
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
        let ts = session.tx_state.read().await;
        assert!(!ts.non_replayable);
        assert_eq!(ts.statements.len(), 1);
    }

    #[test]
    fn backend_fault_set_ignores_client_errors() {
        let mut slot = None;
        BackendFault::set(
            &mut slot,
            "n",
            FaultPhase::OutcomeUnknown,
            &ProxyError::Network("Client write error: x".into()),
        );
        assert!(slot.is_none());
        BackendFault::set(
            &mut slot,
            "n",
            FaultPhase::OutcomeUnknown,
            &ProxyError::Network("Backend read error: reset".into()),
        );
        let f = slot.unwrap();
        assert_eq!(
            (f.node.as_str(), f.phase),
            ("n", FaultPhase::OutcomeUnknown)
        );
    }

    #[test]
    fn tr_metrics_snapshot_roundtrip() {
        let m = TrMetrics::default();
        m.failovers.fetch_add(2, Ordering::Relaxed);
        m.unknown_outcome_errors.fetch_add(3, Ordering::Relaxed);
        let s = m.snapshot();
        assert_eq!(s.failovers, 2);
        assert_eq!(s.unknown_outcome_errors, 3);
        assert_eq!(s.transactions_replayed, 0);
    }

    // ---- backend authentication on a fresh (redial/failover) connection ----

    /// Read one complete frame (tag + body) from a stream.
    async fn read_frame<S: AsyncReadExt + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
        let mut hdr = [0u8; 5];
        s.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body).await.unwrap();
        (hdr[0], body)
    }

    fn auth_frame(kind: u32, body: &[u8]) -> Vec<u8> {
        let mut b = kind.to_be_bytes().to_vec();
        b.extend_from_slice(body);
        frame(b'R', &b)
    }

    /// A SCRAM-SHA-256 backend (driven by the tested `ScramServer`)
    /// accepts the proxy's client exchange; the post-auth frames are
    /// returned for the caller to forward.
    #[tokio::test]
    async fn complete_backend_auth_completes_scram_with_credential() {
        use crate::auth_scram::{ScramServer, ScramVerifier};
        let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
        let password = "benchpass";
        let verifier = ScramVerifier::from_password(password, b"saltsaltsaltsalt".to_vec(), 4096);
        let backend = tokio::spawn(async move {
            // AuthenticationSASL: mechanism list.
            backend_side
                .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
                .await
                .unwrap();
            let (tag, body) = read_frame(&mut backend_side).await;
            assert_eq!(tag, b'p');
            let mech_end = body.iter().position(|&b| b == 0).unwrap() + 1;
            let client_first = std::str::from_utf8(&body[mech_end + 4..]).unwrap();
            let (server, server_first) =
                ScramServer::start(verifier, client_first, "serverNONCE").unwrap();
            backend_side
                .write_all(&auth_frame(11, server_first.as_bytes()))
                .await
                .unwrap();
            let (tag, body) = read_frame(&mut backend_side).await;
            assert_eq!(tag, b'p');
            let server_final = server.finish(std::str::from_utf8(&body).unwrap()).unwrap();
            let mut out = auth_frame(12, server_final.as_bytes());
            out.extend_from_slice(&auth_frame(0, b""));
            out.extend_from_slice(&frame(b'S', b"server_version\0"));
            out.extend_from_slice(&frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
            out.extend_from_slice(&frame(b'Z', b"I"));
            backend_side.write_all(&out).await.unwrap();
            backend_side
        });
        let forwarded =
            ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", Some(password))
                .await
                .unwrap();
        let _ = backend.await.unwrap();
        // Only non-auth frames are handed back: ParameterStatus,
        // BackendKeyData, ReadyForQuery.
        assert_eq!(forwarded[0], b'S');
        assert!(forwarded.ends_with(&frame(b'Z', b"I")));
        assert!(!forwarded.contains(&b'R') || forwarded[0] != b'R');
        let tags: Vec<u8> = {
            let mut v = Vec::new();
            let mut off = 0;
            while off < forwarded.len() {
                v.push(forwarded[off]);
                let len = u32::from_be_bytes([
                    forwarded[off + 1],
                    forwarded[off + 2],
                    forwarded[off + 3],
                    forwarded[off + 4],
                ]) as usize;
                off += 1 + len;
            }
            v
        };
        assert_eq!(tags, vec![b'S', b'K', b'Z']);
    }

    /// A wrong password is rejected by the SCRAM server -> ErrorResponse
    /// -> `ProxyError::Auth`.
    #[tokio::test]
    async fn complete_backend_auth_reports_scram_rejection() {
        use crate::auth_scram::{ScramServer, ScramVerifier};
        let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
        let verifier = ScramVerifier::from_password("right", b"saltsaltsaltsalt".to_vec(), 4096);
        tokio::spawn(async move {
            backend_side
                .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
                .await
                .unwrap();
            let (_, body) = read_frame(&mut backend_side).await;
            let mech_end = body.iter().position(|&b| b == 0).unwrap() + 1;
            let client_first = std::str::from_utf8(&body[mech_end + 4..]).unwrap();
            let (server, server_first) =
                ScramServer::start(verifier, client_first, "serverNONCE").unwrap();
            backend_side
                .write_all(&auth_frame(11, server_first.as_bytes()))
                .await
                .unwrap();
            let (_, body) = read_frame(&mut backend_side).await;
            assert!(server.finish(std::str::from_utf8(&body).unwrap()).is_err());
            let mut err = vec![b'S'];
            err.extend_from_slice(b"FATAL\0C28P01\0Mpassword authentication failed\0\0");
            backend_side.write_all(&frame(b'E', &err)).await.unwrap();
        });
        let r =
            ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", Some("wrong"))
                .await;
        match r {
            Err(ProxyError::Auth(m)) => {
                assert!(m.contains("password authentication failed"), "{m}")
            }
            other => panic!("expected Auth error, got {other:?}"),
        }
    }

    fn fatal(code: &str, message: &str) -> Vec<u8> {
        let mut body = vec![b'S'];
        body.extend_from_slice(format!("FATAL\0C{code}\0M{message}\0\0").as_bytes());
        frame(b'E', &body)
    }

    /// A backend at `max_connections` answers the startup packet with
    /// 53300: proxy-side backend auth reports capacity, not an auth failure.
    #[tokio::test]
    async fn complete_backend_auth_reports_capacity_refusal() {
        let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
        tokio::spawn(async move {
            backend_side
                .write_all(&fatal("53300", "sorry, too many clients already"))
                .await
                .unwrap();
        });
        match ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", Some("pw"))
            .await
        {
            Err(ProxyError::PoolExhausted(m)) => {
                assert!(m.contains("53300") && m.contains("too many clients"), "{m}")
            }
            other => panic!("expected PoolExhausted, got {other:?}"),
        }
    }

    /// Pass-through relay: a 53300 first frame is held back from the client
    /// (so the caller can free idle capacity and redial) and reported as
    /// capacity.
    #[tokio::test]
    async fn passthrough_auth_holds_back_a_capacity_refusal() {
        let server = ProxyServer::new(test_config()).unwrap();
        let (mut backend, mut backend_peer) = pair().await;
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        backend_peer
            .write_all(&fatal("53300", "sorry, too many clients already"))
            .await
            .unwrap();
        let r = ProxyServer::proxy_authentication(
            &mut client,
            &mut backend,
            &server.state,
            "127.0.0.1:5432",
        )
        .await;
        match r {
            Err(ProxyError::PoolExhausted(m)) => {
                assert!(m.contains("max_connections") && m.contains("53300"), "{m}")
            }
            other => panic!("expected PoolExhausted, got {other:?}"),
        }
        drop(client);
        let mut got = Vec::new();
        client_peer.read_to_end(&mut got).await.unwrap();
        assert!(
            got.is_empty(),
            "the refusal must not reach the client: {got:?}"
        );
    }

    /// Any other backend error is relayed unchanged and reported with its
    /// SQLSTATE: only class 28 is an authentication failure.
    #[tokio::test]
    async fn passthrough_auth_relays_errors_and_reports_their_sqlstate() {
        for (code, is_auth) in [("28P01", true), ("3D000", false)] {
            let server = ProxyServer::new(test_config()).unwrap();
            let (mut backend, mut backend_peer) = pair().await;
            let (client_raw, mut client_peer) = pair().await;
            let mut client = ClientStream::Plain(client_raw);
            let err = fatal(code, "refused");
            backend_peer.write_all(&err).await.unwrap();
            let r = ProxyServer::proxy_authentication(
                &mut client,
                &mut backend,
                &server.state,
                "127.0.0.1:5432",
            )
            .await;
            match (r, is_auth) {
                (Err(ProxyError::Auth(m)), true) => assert!(m.starts_with(code), "{m}"),
                (Err(ProxyError::Connection(m)), false) => assert!(m.contains(code), "{m}"),
                (other, _) => panic!("{code}: unexpected {other:?}"),
            }
            drop(client);
            let mut got = Vec::new();
            client_peer.read_to_end(&mut got).await.unwrap();
            assert_eq!(got, err, "{code}: the error reaches the client unchanged");
        }
    }

    /// A successful startup is relayed byte for byte and the cancel key
    /// is registered.
    #[tokio::test]
    async fn passthrough_auth_relays_a_successful_startup_unchanged() {
        let server = ProxyServer::new(test_config()).unwrap();
        let (mut backend, mut backend_peer) = pair().await;
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let mut bytes = auth_frame(0, b"");
        bytes.extend(frame(b'S', b"server_version\x0018.4\0"));
        bytes.extend(frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
        bytes.extend(frame(b'Z', b"I"));
        backend_peer.write_all(&bytes).await.unwrap();
        ProxyServer::proxy_authentication(
            &mut client,
            &mut backend,
            &server.state,
            "127.0.0.1:5432",
        )
        .await
        .expect("startup completes");
        drop(client);
        let mut got = Vec::new();
        client_peer.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, bytes);
    }

    #[test]
    fn error_response_fields_reads_code_and_message() {
        let (code, msg) = ProxyServer::error_response_fields(&fatal("53300", "too many clients"));
        assert_eq!((code.as_str(), msg.as_str()), ("53300", "too many clients"));
        let (code, msg) = ProxyServer::error_response_fields(&[b'E', 0, 0, 0, 4]);
        assert_eq!(code, "");
        assert!(!msg.is_empty());
    }

    #[cfg(feature = "query-cache")]
    mod c02_cache_work {
        use super::*;
        use crate::cache::{CacheConfig, QueryCache};
        use crate::journal_capture::JournalOp;
        use crate::transaction_journal::{NewEntry, StatementOutcome, WireProtocol};

        fn entry(sql: &str) -> NewEntry {
            NewEntry {
                statement: sql.to_string(),
                parameters: Vec::new(),
                param_types: Vec::new(),
                result_checksum: None,
                rows_affected: Some(1),
                duration_ms: 0,
                outcome: StatementOutcome::Succeeded {
                    tag: "UPDATE 1".into(),
                },
                protocol: WireProtocol::Extended,
            }
        }

        fn run(ops: Vec<JournalOp>, stage: &mut TxCacheStage) -> CacheWork {
            let qc = QueryCache::new(CacheConfig::default());
            ProxyServer::cache_work_from_ops(&qc, &ops, stage)
        }

        #[test]
        fn autocommit_write_invalidates_its_tables_at_once() {
            let mut stage = TxCacheStage::default();
            let w = run(
                vec![JournalOp::AutoCommit {
                    entries: vec![entry("UPDATE accounts SET v = $1")],
                    tag: "UPDATE 1".into(),
                    incomplete: None,
                }],
                &mut stage,
            );
            assert_eq!(w.tables, vec!["accounts".to_string()]);
            assert!(!w.all);
            assert!(stage.tables.is_empty());
        }

        #[test]
        fn explicit_transaction_invalidates_at_commit_only() {
            let tx = uuid::Uuid::new_v4();
            let mut stage = TxCacheStage::default();
            let w = run(
                vec![
                    JournalOp::Begin { tx_id: tx },
                    JournalOp::Log {
                        tx_id: tx,
                        entry: entry("UPDATE accounts SET v = 3"),
                    },
                ],
                &mut stage,
            );
            assert!(w.tables.is_empty() && !w.all, "nothing at statement time");
            assert_eq!(stage.tables, vec!["accounts".to_string()], "staged");
            let w = run(
                vec![JournalOp::Commit {
                    tx_id: tx,
                    tag: "COMMIT".into(),
                }],
                &mut stage,
            );
            assert_eq!(w.tables, vec!["accounts".to_string()], "commit time");
            assert!(stage.tables.is_empty());
        }

        #[test]
        fn rollback_drops_the_stage() {
            let tx = uuid::Uuid::new_v4();
            let mut stage = TxCacheStage::default();
            run(
                vec![JournalOp::Log {
                    tx_id: tx,
                    entry: entry("DELETE FROM accounts"),
                }],
                &mut stage,
            );
            let w = run(vec![JournalOp::Rollback { tx_id: tx }], &mut stage);
            assert!(w.tables.is_empty() && !w.all);
            assert!(stage.tables.is_empty() && !stage.unknown);
        }

        #[test]
        fn unknown_scope_invalidates_everything_at_commit() {
            let tx = uuid::Uuid::new_v4();
            for (ops, why) in [
                (
                    vec![JournalOp::Log {
                        tx_id: tx,
                        entry: entry("ALTER TABLE accounts ADD COLUMN c int"),
                    }],
                    "DDL",
                ),
                (
                    vec![
                        JournalOp::Log {
                            tx_id: tx,
                            entry: entry("COPY accounts FROM STDIN"),
                        },
                        JournalOp::Incomplete {
                            tx_id: tx,
                            reason: "COPY FROM".into(),
                        },
                    ],
                    "COPY",
                ),
                (
                    vec![JournalOp::Log {
                        tx_id: tx,
                        entry: entry("EXECUTE upd(1)"),
                    }],
                    "EXECUTE",
                ),
            ] {
                let mut stage = TxCacheStage::default();
                let w = run(ops, &mut stage);
                assert!(!w.all && w.tables.is_empty(), "{why}: staged only");
                assert!(stage.unknown, "{why}: staged as unknown");
                let w = run(
                    vec![JournalOp::Commit {
                        tx_id: tx,
                        tag: "COMMIT".into(),
                    }],
                    &mut stage,
                );
                assert!(w.all, "{why}: commit time");
            }
            let mut stage = TxCacheStage::default();
            let w = run(
                vec![JournalOp::AutoCommit {
                    entries: vec![entry("TRUNCATE accounts")],
                    tag: "TRUNCATE TABLE".into(),
                    incomplete: None,
                }],
                &mut stage,
            );
            assert!(w.all, "autocommit TRUNCATE");
        }
    }

    /// Without a credential (pass-through mode) a challenge fails fast with
    /// a clear error instead of a timeout; a trust backend still completes.
    #[tokio::test]
    async fn complete_backend_auth_without_credential() {
        let (mut proxy_side, mut backend_side) = tokio::io::duplex(4096);
        backend_side
            .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
            .await
            .unwrap();
        let r = ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", None).await;
        match r {
            Err(ProxyError::Auth(m)) => assert!(m.contains("holds no credential"), "{m}"),
            other => panic!("expected Auth error, got {other:?}"),
        }
        // Trust backend: AuthenticationOk straight away.
        let (mut p2, mut b2) = tokio::io::duplex(4096);
        let mut out = auth_frame(0, b"");
        out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
        out.extend_from_slice(&frame(b'Z', b"I"));
        b2.write_all(&out).await.unwrap();
        let fwd = ProxyServer::complete_backend_auth(&mut p2, 1 << 20, "bench", None)
            .await
            .unwrap();
        assert_eq!(fwd[0], b'K');
        assert!(fwd.ends_with(&frame(b'Z', b"I")));
    }

    /// Cleartext and MD5 challenges are answered from the credential.
    #[tokio::test]
    async fn complete_backend_auth_answers_cleartext_and_md5() {
        // Cleartext.
        let (mut p, mut b) = tokio::io::duplex(4096);
        let backend = tokio::spawn(async move {
            b.write_all(&auth_frame(3, b"")).await.unwrap();
            let (tag, body) = read_frame(&mut b).await;
            assert_eq!((tag, body.as_slice()), (b'p', &b"pw\0"[..]));
            let mut out = auth_frame(0, b"");
            out.extend_from_slice(&frame(b'Z', b"I"));
            b.write_all(&out).await.unwrap();
        });
        ProxyServer::complete_backend_auth(&mut p, 1 << 20, "u", Some("pw"))
            .await
            .unwrap();
        backend.await.unwrap();
        // MD5.
        let (mut p, mut b) = tokio::io::duplex(4096);
        let backend = tokio::spawn(async move {
            b.write_all(&auth_frame(5, &[1, 2, 3, 4])).await.unwrap();
            let (tag, body) = read_frame(&mut b).await;
            assert_eq!(tag, b'p');
            assert_eq!(
                body,
                crate::backend::auth::md5_password_response("u", "pw", &[1, 2, 3, 4])
            );
            let mut out = auth_frame(0, b"");
            out.extend_from_slice(&frame(b'Z', b"I"));
            b.write_all(&out).await.unwrap();
        });
        ProxyServer::complete_backend_auth(&mut p, 1 << 20, "u", Some("pw"))
            .await
            .unwrap();
        backend.await.unwrap();
    }
}
