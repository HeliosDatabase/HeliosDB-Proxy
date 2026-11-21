//! Contract tests use existing configuration/wire APIs so the pre-policy source
//! can fail at cache behavior rather than at a missing new Rust field.
use super::*;

fn set_contract(config: &mut ProxyConfig, section: &str, trusted: bool) {
    let mut value = toml::Value::try_from(config.clone()).unwrap();
    value[section].as_table_mut().unwrap().insert(
        "trust_backend_read_effects".into(),
        toml::Value::Boolean(trusted),
    );
    *config = value.try_into().unwrap();
}

fn set_tr_contract(config: &mut ProxyConfig, trusted: bool) {
    let mut value = toml::Value::try_from(config.clone()).unwrap();
    value.as_table_mut().unwrap().insert(
        "tr_trust_backend_effects".into(),
        toml::Value::Boolean(trusted),
    );
    *config = value.try_into().unwrap();
}

fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn scalar(value: &str) -> Vec<u8> {
    let mut column = 1u16.to_be_bytes().to_vec();
    column.extend_from_slice(b"value\0");
    column.extend_from_slice(&0u32.to_be_bytes());
    column.extend_from_slice(&0u16.to_be_bytes());
    column.extend_from_slice(&23u32.to_be_bytes());
    column.extend_from_slice(&4i16.to_be_bytes());
    column.extend_from_slice(&(-1i32).to_be_bytes());
    column.extend_from_slice(&0u16.to_be_bytes());
    let mut row = 1u16.to_be_bytes().to_vec();
    row.extend_from_slice(&(value.len() as i32).to_be_bytes());
    row.extend_from_slice(value.as_bytes());
    [
        wire(b'T', &column),
        wire(b'D', &row),
        wire(b'C', b"SELECT 1\0"),
        wire(b'Z', b"I"),
    ]
    .concat()
}

// Ready for the pending exact scope dependency; it is not a scope-test bypass.
fn settings() -> Vec<u8> {
    let mut columns = 3u16.to_be_bytes().to_vec();
    for name in ["name", "setting", "description"] {
        columns.extend_from_slice(name.as_bytes());
        columns.push(0);
        columns.extend_from_slice(&0u32.to_be_bytes());
        columns.extend_from_slice(&0u16.to_be_bytes());
        columns.extend_from_slice(&25u32.to_be_bytes());
        columns.extend_from_slice(&(-1i16).to_be_bytes());
        columns.extend_from_slice(&(-1i32).to_be_bytes());
        columns.extend_from_slice(&0u16.to_be_bytes());
    }
    let mut result = wire(b'T', &columns);
    for (name, value) in [
        ("search_path", "public"),
        ("row_security", "on"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("server_version", "18.4"),
    ] {
        let mut row = 3u16.to_be_bytes().to_vec();
        for part in [name, value, ""] {
            row.extend_from_slice(&(part.len() as i32).to_be_bytes());
            row.extend_from_slice(part.as_bytes());
        }
        result.extend(wire(b'D', &row));
    }
    result.extend(wire(b'C', b"SHOW\0"));
    result.extend(wire(b'Z', b"I"));
    result
}

struct Harness {
    server: ProxyServer,
    config: ProxyConfig,
    addr: String,
    session: Arc<ClientSession>,
    conns: HashMap<String, BackendConn>,
    client: ClientStream,
    peer: TcpStream,
}

impl Harness {
    async fn start(
        query_cache: bool,
        edge: bool,
        trusted: bool,
        tr_trusted: bool,
    ) -> (Self, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let backend = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            loop {
                let mut head = [0u8; 5];
                if socket.read_exact(&mut head).await.is_err() {
                    break;
                }
                assert_eq!(head[0], b'Q');
                let len = u32::from_be_bytes(head[1..].try_into().unwrap()) as usize;
                assert!((5..=1_048_576).contains(&len));
                let mut body = vec![0; len - 4];
                socket.read_exact(&mut body).await.unwrap();
                assert_eq!(body.pop(), Some(0));
                let sql = String::from_utf8(body).unwrap();
                let reply = if sql == "SHOW ALL" {
                    settings()
                } else {
                    let reply = scalar(match sql.as_str() {
                        "SELECT 42" => "42",
                        "SELECT 41" => "41",
                        _ => "7",
                    });
                    seen.push(sql);
                    reply
                };
                socket.write_all(&reply).await.unwrap();
            }
            seen
        });
        let mut config = test_config();
        config.nodes.clear();
        config.add_node(&addr, "primary").unwrap();
        config.cache.enabled = query_cache;
        config.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        #[cfg(feature = "edge-proxy")]
        {
            config.edge.enabled = edge;
        }
        #[cfg(not(feature = "edge-proxy"))]
        assert!(!edge);
        set_contract(&mut config, "cache", trusted);
        set_tr_contract(&mut config, tr_trusted);
        let server = ProxyServer::new(config.clone()).unwrap();
        let connection = BackendConn::new(TcpStream::connect(&addr).await.unwrap());
        let (client, peer) = pair().await;
        (
            Self {
                server,
                config,
                addr: addr.clone(),
                session: make_test_session(),
                conns: HashMap::from([(addr, connection)]),
                client: ClientStream::Plain(client),
                peer,
            },
            backend,
        )
    }

    async fn query(&mut self, sql: &str) -> Option<String> {
        let mut fault = None;
        let (node, sent) = ProxyServer::forward_simple_query(
            ForwardCtx {
                client: &mut self.client,
                conns: &mut self.conns,
                current_node: Some(&self.addr),
                reads: &ReadRouting::OFF,
                session: &self.session,
                state: &self.server.state,
                config: &self.config,
                fault: &mut fault,
                continues_cycle: None,
            },
            &QueryMessage { query: sql.into() }.encode(),
        )
        .await
        .unwrap();
        let mut response = vec![0; sent as usize];
        self.peer.read_exact(&mut response).await.unwrap();
        assert_eq!(
            response,
            scalar(match sql {
                "SELECT 42" => "42",
                "SELECT 41" => "41",
                _ => "7",
            })
        );
        node
    }
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn cache_effects_contract_default_relation_forwards_twice() {
    let (mut h, backend) = Harness::start(true, false, false, false).await;
    for _ in 0..2 {
        assert!(
            h.query("SELECT value FROM widgets").await.is_some(),
            "unvouched relation must execute"
        );
    }
    assert_eq!(
        h.server.state.query_cache.as_ref().unwrap().stats().puts,
        0,
        "an unvouched read must not publish a cache fill"
    );
    drop(h);
    assert_eq!(backend.await.unwrap().len(), 2);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn cache_effects_contract_default_refresh_cannot_store_relation() {
    let (mut h, backend) = Harness::start(true, false, false, false).await;
    assert!(h
        .query("SELECT value FROM widgets /*helios:cache=force*/")
        .await
        .is_some());
    assert!(
        h.query("SELECT value FROM widgets").await.is_some(),
        "refresh hint cannot vouch for a later hit"
    );
    assert_eq!(
        h.server.state.query_cache.as_ref().unwrap().stats().puts,
        0,
        "an unvouched read must not publish a cache fill"
    );
    drop(h);
    assert_eq!(backend.await.unwrap().len(), 2);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
// Compatibility control only: this seed ignores the future TR-effects field.
// The cumulative accepted strict-TR tree must run the real policy matrix too.
async fn cache_effects_contract_cache_policy_ignores_legacy_tr_config_hint() {
    for tr in [false, true] {
        for trusted in [false, true] {
            let (mut h, backend) = Harness::start(true, false, trusted, tr).await;
            assert!(h.query("SELECT value FROM widgets").await.is_some());
            assert_eq!(
                h.query("SELECT value FROM widgets").await.is_none(),
                trusted,
                "cache={trusted} TR={tr}"
            );
            drop(h);
            assert_eq!(backend.await.unwrap().len(), if trusted { 1 } else { 2 });
        }
    }
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn cache_effects_contract_default_literal_keeps_a_real_hit() {
    let (mut h, backend) = Harness::start(true, false, false, false).await;
    assert!(h.query("SELECT 42").await.is_some());
    assert!(h.query("SELECT 42").await.is_none());
    drop(h);
    assert_eq!(backend.await.unwrap(), ["SELECT 42"]);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn cache_effects_contract_trusted_mode_keeps_direct_call_guards() {
    let (mut h, backend) = Harness::start(true, false, true, true).await;
    for sql in [
        "SELECT abs(1)",
        "SELECT abs(1)",
        "SELECT value FROM widgets",
    ] {
        assert!(
            h.query(sql).await.is_some(),
            "call/context refusal remains: {sql}"
        );
    }
    drop(h);
    assert_eq!(backend.await.unwrap().len(), 3);
}

#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn cache_effects_contract_edge_only_obeys_cache_contract() {
    for trusted in [false, true] {
        let (mut h, backend) = Harness::start(false, true, trusted, false).await;
        assert!(h.query("SELECT value FROM widgets").await.is_some());
        assert_eq!(
            h.query("SELECT value FROM widgets").await.is_none(),
            trusted
        );
        let edge_stats = h.server.state.edge_cache.stats();
        assert_eq!(edge_stats.inserts, u64::from(trusted));
        assert_eq!(edge_stats.current_entries, usize::from(trusted));
        drop(h);
        assert_eq!(backend.await.unwrap().len(), if trusted { 1 } else { 2 });
    }
}

#[cfg(feature = "query-cache")]
#[test]
fn cache_effects_contract_mcp_factory_is_independent_of_pg_cache_trust() {
    for pg_trust in [false, true] {
        for mcp_trust in [false, true] {
            let mut config = test_config();
            config.cache.enabled = true;
            config.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
            config.mcp.enabled = true;
            config.mcp.tool_cache_enabled = true;
            set_contract(&mut config, "cache", pg_trust);
            set_contract(&mut config, "mcp", mcp_trust);
            let server = ProxyServer::new(config).unwrap();
            assert_eq!(
                server.state.mcp_tool_cache.is_some(),
                mcp_trust,
                "PG cache trust={pg_trust} MCP trust={mcp_trust}"
            );
        }
    }
}

/// A pre-probe error is client-visible publication too. Backpressure must use
/// the existing client-write deadline, and a lost backend must be abandoned
/// even when publication fails. No business SQL or retryable fault is allowed.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn cache_effects_contract_scope_rejection_publication_is_bounded() {
    for lost in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let backend = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = [0u8; 5];
            socket.read_exact(&mut head).await.unwrap();
            assert_eq!(head[0], b'Q');
            let len = u32::from_be_bytes(head[1..].try_into().unwrap()) as usize;
            let mut query = vec![0; len - 4];
            socket.read_exact(&mut query).await.unwrap();
            assert_eq!(query, b"SHOW ALL\0");
            let mut body = b"SERROR\0C42501\0M".to_vec();
            body.resize(1024 * 1024, b'x');
            body.extend_from_slice(b"\0\0");
            let mut response = wire(b'E', &body);
            if !lost {
                response.extend(wire(b'Z', b"I"));
            }
            socket.write_all(&response).await.unwrap();
            if lost {
                return;
            }
            let mut next = [0u8; 1];
            assert_eq!(
                socket.read(&mut next).await.unwrap(),
                0,
                "business SQL must never run"
            );
        });
        let mut config = test_config();
        config.nodes.clear();
        config.add_node(&addr, "primary").unwrap();
        config.cache.enabled = true;
        config.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        config.cache.max_backend_context_bytes = 4 * 1024 * 1024;
        config.limits.client_write_timeout_secs = 1;
        set_contract(&mut config, "cache", true);
        let server = ProxyServer::new(config.clone()).unwrap();
        let connection = BackendConn::new(TcpStream::connect(&addr).await.unwrap());
        let mut conns = HashMap::from([(addr.clone(), connection)]);
        let (client, peer) = pair().await;
        socket2::SockRef::from(&client)
            .set_send_buffer_size(4096)
            .unwrap();
        socket2::SockRef::from(&peer)
            .set_recv_buffer_size(4096)
            .unwrap();
        let mut client = ClientStream::Plain(client);
        let session = make_test_session();
        let mut fault = None;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            ProxyServer::forward_simple_query(
                ForwardCtx {
                    client: &mut client,
                    conns: &mut conns,
                    current_node: Some(&addr),
                    reads: &ReadRouting::OFF,
                    session: &session,
                    state: &server.state,
                    config: &config,
                    fault: &mut fault,
                    continues_cycle: None,
                },
                &QueryMessage {
                    query: "SELECT value FROM widgets".into(),
                }
                .encode(),
            ),
        )
        .await;
        let error = result
            .expect("scope rejection must honor client_write_timeout")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .to_ascii_lowercase()
                .contains("write timeout"),
            "{error}"
        );
        assert!(
            fault.is_none(),
            "a client publication failure must never retry the request"
        );
        if lost {
            assert!(
                !conns.contains_key(&addr),
                "a lost backend must retire even on client-write failure"
            );
        }
        drop(conns);
        drop(client);
        drop(peer);
        backend.await.unwrap();
    }
}

#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn cache_effects_contract_default_edge_literals_keep_distinct_real_hits() {
    let (mut h, backend) = Harness::start(false, true, false, false).await;
    assert!(h.query("SELECT 41").await.is_some());
    assert!(h.query("SELECT 42").await.is_some());
    assert!(h.query("SELECT 41").await.is_none());
    assert!(h.query("SELECT 42").await.is_none());
    drop(h);
    assert_eq!(backend.await.unwrap(), ["SELECT 41", "SELECT 42"]);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn cache_effects_contract_reload_keeps_both_running_contracts() {
    for initial in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy.toml");
        let mut config = test_config();
        config.cache.enabled = true;
        config.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        config.mcp.enabled = true;
        config.mcp.tool_cache_enabled = true;
        config.cache.trust_backend_read_effects = initial;
        config.mcp.trust_backend_read_effects = initial;
        std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
        let server = ProxyServer::new(config.clone())
            .unwrap()
            .with_config_path(Some(path.to_string_lossy().into_owned()));
        config.cache.trust_backend_read_effects = !initial;
        config.mcp.trust_backend_read_effects = !initial;
        config.cache.max_cacheable_response_bytes += 1;
        std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
        server.reload_config().await;
        let live = server.state.live_config.load_full();
        assert_eq!(live.cache.trust_backend_read_effects, initial);
        assert_eq!(live.mcp.trust_backend_read_effects, initial);
        assert_eq!(
            live.cache.max_cacheable_response_bytes,
            config.cache.max_cacheable_response_bytes
        );
        assert_eq!(server.state.mcp_tool_cache.is_some(), initial);
    }
}

#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn cache_effects_contract_trusted_edge_keeps_direct_call_and_postfill_guards() {
    let (mut h, backend) = Harness::start(false, true, true, true).await;
    for sql in [
        "SELECT abs(1)",
        "SELECT abs(1)",
        "SELECT value FROM widgets",
    ] {
        assert!(h.query(sql).await.is_some());
    }
    assert_eq!(h.server.state.edge_cache.stats().inserts, 0);
    assert_eq!(h.server.state.edge_cache.stats().current_entries, 0);
    drop(h);
    assert_eq!(backend.await.unwrap().len(), 3);
}
