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
mod stmt_facts;

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
mod routing_hints;

// ---- rate-limiting: the burst-then-deny contract the gate relies on ----

#[cfg(feature = "rate-limiting")]
mod rate_limiting;

// ---- circuit-breaker: open-after-threshold contract the gate relies on ----

#[cfg(feature = "circuit-breaker")]
mod circuit_breaker;

// ---- query-analytics: record + literal-collapsing normalizer ----

#[cfg(feature = "query-analytics")]
mod query_analytics;

// ---- lag-routing: read-your-writes window + lag-exclusion decisions ----

#[cfg(feature = "lag-routing")]
mod lag_routing;

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
mod query_cache;

// ---- edge-proxy: write-invalidation classifiers ----

#[cfg(feature = "edge-proxy")]
mod edge_proxy;

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
mod query_rewriting;

// ---- multi-tenancy: row-filter injection per tenant ----

#[cfg(feature = "multi-tenancy")]
mod multi_tenancy;

// ---- TR: the journal records statements the replay engine reads ----

mod ha_tr;

// ---- schema-routing: OLAP vs OLTP workload classification ----

#[cfg(feature = "schema-routing")]
mod schema_routing;

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

mod tr_in_session;
