use super::*;

/// A `[cache]` section with every semantic key off its default and per-table
/// settings.
const SEMANTIC_CACHE: &str = r#"
enabled = true
ttl_secs = 120
semantic_enabled = true
semantic_match = "canonical"
semantic_similarity_threshold = 0.95
semantic_max_entries = 42
semantic_max_variants = 4
semantic_max_bytes = 1048576
semantic_ttl_secs = 60
semantic_embedding_endpoint = "http://127.0.0.1:9/"
semantic_embedding_model = "mini"
semantic_embedding_dim = 8
semantic_embedding_timeout_ms = 100
semantic_embedding_concurrency = 1
semantic_embedding_retry_secs = 2
exclude_tables = ["Secret"]

[[tables]]
name = "Orders"
ttl_secs = 600

[[tables]]
name = "payroll"
exclude = true
"#;

/// The daemon builds its cache from every `[cache]` key: the semantic tier,
/// per-table TTLs (lower-cased names) and the L1 TTL that admits the longest.
#[test]
fn daemon_cache_config_carries_semantic_keys_and_table_ttls() {
    let mut config = test_config();
    config.cache = toml::from_str(SEMANTIC_CACHE).unwrap();
    config.validate().unwrap();
    let server = ProxyServer::new(config).unwrap();
    let qc = server.state.query_cache.as_ref().expect("cache enabled");
    let cc = qc.config();
    let l3 = &cc.l3;
    assert!(l3.enabled);
    assert_eq!(l3.semantic_match, crate::config::SemanticMatch::Canonical);
    assert!((l3.similarity_threshold - 0.95).abs() < 1e-6);
    assert_eq!(l3.max_entries, 42);
    assert_eq!(l3.max_variants, 4);
    assert_eq!(l3.max_bytes, 1_048_576);
    assert_eq!(l3.ttl, Duration::from_secs(60));
    assert_eq!(l3.embedding_endpoint, "http://127.0.0.1:9");
    assert_eq!(l3.embedding_model, "mini");
    assert_eq!(l3.embedding_dim, 8);
    assert_eq!(l3.embedding_timeout, Duration::from_millis(100));
    assert_eq!(l3.embedding_concurrency, 1);
    assert_eq!(l3.embedding_retry_after, Duration::from_secs(2));
    assert_eq!(cc.default_ttl, Duration::from_secs(120));
    assert_eq!(cc.l1.ttl, Duration::from_secs(600));
    assert_eq!(cc.table_configs.len(), 1);
    assert_eq!(cc.table_configs["orders"].ttl, Duration::from_secs(600));
    assert!(cc.excluded_tables.is_empty());
    // Exclusions are the relay's, from both lists.
    let exclusions = server.state.table_exclusions.as_ref().expect("built");
    assert_eq!(exclusions.names(), ["payroll", "secret"]);
}

/// The startup probe runs only when the semantic tier may contact its
/// endpoint (threshold below 1.0), and reports the endpoint's state.
#[tokio::test]
async fn semantic_probe_runs_only_when_the_endpoint_is_used() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let mut config = test_config();
    config.cache.enabled = true;
    config.cache.semantic_enabled = true;
    config.cache.semantic_embedding_endpoint = endpoint;
    let exact_only = ProxyServer::new(config.clone()).unwrap();
    assert!(exact_only.spawn_semantic_probe().is_none(), "threshold 1.0");

    config.cache.semantic_match = crate::config::SemanticMatch::Canonical;
    config.cache.semantic_similarity_threshold = 0.9;
    let server = ProxyServer::new(config).unwrap();
    server
        .spawn_semantic_probe()
        .expect("probe spawned")
        .await
        .unwrap();
    let health = server
        .state
        .query_cache
        .as_ref()
        .unwrap()
        .semantic_health()
        .unwrap();
    assert_eq!(health.endpoint, crate::cache::EndpointState::Down);
    assert_eq!(health.embed_requests, 1);
}

/// Without a result cache running, exclusions are not even built.
#[test]
fn table_exclusions_need_a_running_cache() {
    let mut config = test_config();
    config.cache.exclude_tables = vec!["secret".to_string()];
    let server = ProxyServer::new(config.clone()).unwrap();
    assert!(server.state.table_exclusions.is_none());
    config.cache.enabled = true;
    config.cache.exclude_tables.clear();
    let server = ProxyServer::new(config).unwrap();
    assert!(server.state.table_exclusions.is_none());
}

fn wire_frame(mtype: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![mtype];
    v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    v.extend_from_slice(body);
    v
}

/// What the fake backend answers every query with.
fn backend_reply() -> Vec<u8> {
    [wire_frame(b'C', b"SELECT 0\0"), wire_frame(b'Z', b"I")].concat()
}

/// What the cache was prefilled with (distinct from the backend's answer).
fn cached_reply() -> Vec<u8> {
    [wire_frame(b'C', b"SELECT 42\0"), wire_frame(b'Z', b"I")].concat()
}

/// The text of the next simple query on `sock`.
async fn read_query(sock: &mut TcpStream) -> String {
    let mut head = [0u8; 5];
    sock.read_exact(&mut head).await.unwrap();
    assert_eq!(head[0], b'Q');
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize - 4;
    let mut body = vec![0u8; len];
    sock.read_exact(&mut body).await.unwrap();
    String::from_utf8_lossy(&body[..len - 1]).into_owned()
}

/// A backend that answers `n` simple queries with [`backend_reply`] and
/// returns their texts.
async fn fake_backend(listener: TcpListener, n: usize) -> Vec<String> {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut seen = Vec::new();
    for _ in 0..n {
        seen.push(read_query(&mut sock).await);
        sock.write_all(&backend_reply()).await.unwrap();
    }
    seen
}

/// A relay whose only node is a fake backend on `listener`, the cache on,
/// `tune` applied, and the cache prefilled with [`cached_reply`] for
/// `prefill` (shared L2 entries, as another connection stored them).
struct Relay {
    server: ProxyServer,
    config: ProxyConfig,
    addr: String,
    session: Arc<ClientSession>,
    conns: HashMap<String, BackendConn>,
    client: crate::client_tls::ClientStream,
    peer: TcpStream,
}

impl Relay {
    async fn start(
        tune: impl FnOnce(&mut ProxyConfig),
        prefill: &[&str],
        queries: usize,
    ) -> (Self, tokio::task::JoinHandle<Vec<String>>) {
        Self::start_with(tune, prefill, move |listener| {
            tokio::spawn(fake_backend(listener, queries))
        })
        .await
    }

    /// [`Self::start`] with the backend `serve` spawns on the listener.
    async fn start_with(
        tune: impl FnOnce(&mut ProxyConfig),
        prefill: &[&str],
        serve: impl FnOnce(TcpListener) -> tokio::task::JoinHandle<Vec<String>>,
    ) -> (Self, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let backend = serve(listener);
        let mut config = ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        config.add_node(&addr, "primary").unwrap();
        config.cache.enabled = true;
        tune(&mut config);
        let server = ProxyServer::new(config.clone()).unwrap();
        let shared = crate::cache::CacheContext {
            database: "default".to_string(),
            ..Default::default()
        };
        for sql in prefill {
            let qc = server.state.query_cache.as_ref().expect("cache on");
            qc.put(
                sql,
                &shared,
                bytes::Bytes::from(cached_reply()),
                1,
                Duration::ZERO,
            )
            .await;
        }
        let mut conns = HashMap::new();
        conns.insert(
            addr.clone(),
            BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
        );
        let (client_raw, peer) = pair().await;
        let relay = Self {
            server,
            config,
            addr,
            session: make_test_session(),
            conns,
            client: crate::client_tls::ClientStream::Plain(client_raw),
            peer,
        };
        (relay, backend)
    }

    /// Forward `sql`; the node that served it (`None`: the cache) and the
    /// bytes the client received.
    async fn query(&mut self, sql: &str) -> (Option<String>, Vec<u8>) {
        let msg = QueryMessage {
            query: sql.to_string(),
        }
        .encode();
        let mut fault = None;
        let (node, sent) = ProxyServer::forward_simple_query(
            ForwardCtx {
                client: &mut self.client,
                conns: &mut self.conns,
                current_node: Some(self.addr.as_str()),
                reads: &ReadRouting::OFF,
                session: &self.session,
                state: &self.server.state,
                config: &self.config,
                fault: &mut fault,
                continues_cycle: None,
            },
            &msg,
        )
        .await
        .unwrap();
        let mut got = vec![0u8; sent as usize];
        self.peer.read_exact(&mut got).await.unwrap();
        (node, got)
    }

    fn cache(&self) -> &crate::cache::QueryCache {
        self.server.state.query_cache.as_deref().unwrap()
    }

    /// `heliosdb_proxy_cache_excluded_total`.
    fn excluded(&self) -> u64 {
        self.server
            .state
            .metrics
            .cache_excluded
            .load(Ordering::Relaxed)
    }

    /// `heliosdb_proxy_cache_catalog_skipped_total`.
    fn catalog_skipped(&self) -> u64 {
        self.server
            .state
            .metrics
            .cache_catalog_skipped
            .load(Ordering::Relaxed)
    }

    /// `heliosdb_proxy_cache_time_travel_skipped_total`.
    fn time_travel_skipped(&self) -> u64 {
        self.server
            .state
            .metrics
            .cache_time_travel_skipped
            .load(Ordering::Relaxed)
    }

    /// `heliosdb_proxy_cache_ambiguous_text_skipped_total`.
    fn ambiguous_skipped(&self) -> u64 {
        self.server
            .state
            .metrics
            .cache_ambiguous_skipped
            .load(Ordering::Relaxed)
    }

    /// Store [`cached_reply`] for `sql` in this session's L1 (and L2), as
    /// an earlier read of the session would have.
    async fn prefill_l1(&self, sql: &str) {
        let own = crate::cache::CacheContext {
            database: "default".to_string(),
            connection_id: Some(self.session.id.as_u64_pair().0),
            ..Default::default()
        };
        self.cache()
            .put(
                sql,
                &own,
                bytes::Bytes::from(cached_reply()),
                1,
                Duration::ZERO,
            )
            .await;
    }

    /// Run `sql` and check that the backend answered it.
    async fn answered_by_backend(&mut self, sql: &str) {
        let (node, got) = self.query(sql).await;
        assert_eq!(node.as_deref(), Some(self.addr.as_str()), "{sql}");
        assert_eq!(got, backend_reply(), "{sql}");
    }
}

/// Reads naming a system catalog relation (R5-2): session-dependent on
/// PostgreSQL (`pg_settings` reflects `SET`), changed by DDL no write through
/// the proxy announces. Each used to be stored and served from L1 and L2
/// (another session's `work_mem`, a frozen `pg_stat_activity` count).
const CATALOG_READS: [&str; 5] = [
    "SELECT setting FROM pg_settings WHERE name = 'work_mem'",
    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'active'",
    "SELECT table_name FROM Information_Schema.tables",
    "SELECT relname FROM \"pg_class\"",
    "SELECT oid FROM t WHERE r = 'pg_class'::regclass",
];

/// Reads whose comment holds a time-travel clause with any spacing (R5-3):
/// the keys leave the comment out (so each shared the key of
/// `SELECT value FROM widgets`), while HeliosDB-Nano and HeliosDB-Lite read
/// it.
const TIME_TRAVEL_READS: [&str; 5] = [
    "SELECT value FROM widgets /* as  of now */",
    "SELECT value FROM widgets /* As\tOf now */",
    "SELECT value FROM widgets -- as of now",
    "SELECT value FROM widgets /* versions\n between 1 and 2 */",
    "SELECT value FROM widgets /*helios:cache_ttl=60*/ /* AS OF x */",
];

/// Backend-ambiguous reads (R6-1): PostgreSQL and HeliosDB-Nano/Lite, or the
/// two `standard_conforming_strings` settings, may read each as a different
/// statement (`WHERE id = 2` is a comment for one of them). The last names a
/// table with a quoted literal, which HeliosDB-Nano and HeliosDB-Lite read
/// as `widgets` and the table extraction as `public`: it was stored under
/// `public`, and a write of `widgets` left it served.
const AMBIGUOUS_READS: [&str; 6] = [
    "SELECT value FROM widgets WHERE path = 'C:\\' OR id = 2",
    "SELECT value FROM widgets /* a /* b */ WHERE id = 2 -- */",
    "SELECT value FROM widgets -- note\rWHERE id = 2",
    "SELECT value FROM widgets WHERE s = $$it's$$",
    "SELECT value FROM widgets WHERE s = 'open",
    "SELECT value FROM public.'widgets' WHERE id = 2",
];

/// No L1 or L2 entry serves a backend-ambiguous read and none is stored;
/// each reaches the backend every time and is counted.
#[tokio::test]
async fn ambiguous_reads_skip_l1_and_l2() {
    let (mut relay, backend) =
        Relay::start(|_| {}, &AMBIGUOUS_READS, AMBIGUOUS_READS.len() * 2).await;
    for sql in AMBIGUOUS_READS {
        relay.prefill_l1(sql).await;
    }
    relay.prefill_l1("SELECT value FROM widgets").await;
    let puts = relay.cache().stats().puts;
    for sql in AMBIGUOUS_READS {
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(sql).await;
    }
    assert_eq!(relay.cache().stats().puts, puts, "nothing stored");
    assert_eq!(relay.ambiguous_skipped(), 2 * AMBIGUOUS_READS.len() as u64);
    let (node, got) = relay.query("SELECT value FROM widgets").await;
    assert_eq!((node, got), (None, cached_reply()), "control: L1 hit");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2 * AMBIGUOUS_READS.len());
}

/// No L1 or L2 entry serves a catalog read and none is stored; the read
/// reaches the backend every time and is counted. Another read of the same
/// session is still served from cache.
#[tokio::test]
async fn catalog_reads_skip_l1_and_l2() {
    let (mut relay, backend) = Relay::start(|_| {}, &CATALOG_READS, CATALOG_READS.len() * 2).await;
    for sql in CATALOG_READS {
        relay.prefill_l1(sql).await;
    }
    relay.prefill_l1("SELECT value FROM widgets").await;
    let puts = relay.cache().stats().puts;
    for sql in CATALOG_READS {
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(sql).await;
    }
    assert_eq!(relay.cache().stats().puts, puts, "nothing stored");
    assert_eq!(relay.catalog_skipped(), 2 * CATALOG_READS.len() as u64);
    let (node, got) = relay.query("SELECT value FROM widgets").await;
    assert_eq!((node, got), (None, cached_reply()), "control: L1 hit");
    assert_eq!(relay.time_travel_skipped(), 0);
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2 * CATALOG_READS.len());
}

/// No L1 or L2 entry serves a read whose comment holds a time-travel clause
/// and none is stored, whatever the whitespace between the words.
#[tokio::test]
async fn time_travel_comments_skip_l1_and_l2() {
    let (mut relay, backend) = Relay::start(
        |_| {},
        &["SELECT value FROM widgets"],
        TIME_TRAVEL_READS.len() * 2,
    )
    .await;
    for sql in TIME_TRAVEL_READS {
        relay.prefill_l1(sql).await;
    }
    let puts = relay.cache().stats().puts;
    for sql in TIME_TRAVEL_READS {
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(sql).await;
    }
    assert_eq!(relay.cache().stats().puts, puts, "nothing stored");
    assert_eq!(
        relay.time_travel_skipped(),
        2 * TIME_TRAVEL_READS.len() as u64
    );
    // Control: the comment-free read (same L2 key) and an ordinary comment.
    for sql in [
        "SELECT value FROM widgets",
        "SELECT value FROM widgets /* has an offset */",
    ] {
        let (node, got) = relay.query(sql).await;
        assert_eq!((node, got), (None, cached_reply()), "{sql}");
    }
    assert_eq!(relay.catalog_skipped(), 0);
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2 * TIME_TRAVEL_READS.len());
}

/// The semantic tier is not even consulted for a catalog, time-travel or
/// backend-ambiguous read: its ineligible counter stays put (it used to
/// grow, the read having reached L3), and an entry stored for the
/// comment-free statement is not served to a time-travel spelling of it.
#[tokio::test]
async fn catalog_and_time_travel_reads_skip_the_semantic_tier() {
    let hinted = "SELECT value FROM widgets WHERE id = 1 /*helios:cache=semantic*/";
    let refused = [
        "SELECT setting FROM pg_settings WHERE name = 'x' /*helios:cache=semantic*/",
        "SELECT value FROM widgets WHERE id = 1 /*helios:cache=semantic*/ /* as  of now */",
        "SELECT value FROM widgets WHERE id = 1 /*helios:cache=semantic*/ /* as of now */",
        "SELECT value FROM widgets WHERE id = 1 /*helios:cache=semantic*/ AND p = 'C:\\'",
        "SELECT value FROM public.'widgets' WHERE id = 1 /*helios:cache=semantic*/",
    ];
    let (mut relay, backend) = Relay::start(
        |c| {
            c.cache.semantic_enabled = true;
            c.cache.semantic_match = crate::config::SemanticMatch::Canonical;
        },
        &[hinted],
        refused.len(),
    )
    .await;
    let l3 = relay.cache().semantic_cache().unwrap().clone();
    assert_eq!(l3.len(), 1);
    let ineligible = l3.health().ineligible;
    for sql in refused {
        relay.answered_by_backend(sql).await;
    }
    assert_eq!(l3.health().ineligible, ineligible, "L3 not consulted");
    assert_eq!(l3.len(), 1, "nothing stored");
    assert_eq!(
        (
            relay.catalog_skipped(),
            relay.time_travel_skipped(),
            relay.ambiguous_skipped()
        ),
        (1, 2, 2)
    );
    let (node, got) = relay
        .query("select value from widgets as w where id = 1 /*helios:cache=semantic*/")
        .await;
    assert_eq!((node, got), (None, cached_reply()), "control: L3 hit");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), refused.len());
}

/// The edge cache neither serves nor stores a catalog, time-travel or
/// backend-ambiguous read: each reaches the backend both times, while an
/// ordinary read is stored and then served.
#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn catalog_and_time_travel_reads_skip_the_edge_cache() {
    let refused: Vec<&str> = CATALOG_READS
        .iter()
        .chain(TIME_TRAVEL_READS.iter())
        .chain(AMBIGUOUS_READS.iter())
        .copied()
        .collect();
    let (mut relay, backend) = Relay::start(
        |c| {
            c.cache.enabled = false;
            c.edge.enabled = true;
        },
        &[],
        refused.len() * 2 + 1,
    )
    .await;
    for sql in &refused {
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(sql).await;
    }
    assert_eq!(relay.server.state.edge_cache.stats().current_entries, 0);
    assert_eq!(
        relay.catalog_skipped() + relay.time_travel_skipped() + relay.ambiguous_skipped(),
        2 * refused.len() as u64
    );
    relay.answered_by_backend("SELECT value FROM widgets").await;
    let (node, got) = relay.query("SELECT value FROM widgets").await;
    assert_eq!((node, got), (None, backend_reply()), "control: edge hit");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), refused.len() * 2 + 1);
}

/// `[routing_hints]` strips every `/*helios:...*/` comment from the
/// forwarded text, so the cache used to never see a leading `cache=skip`
/// and served the read from cache anyway. The client's own text now counts.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn forward_simple_query_honours_a_stripped_skip_hint() {
    let (mut relay, backend) = Relay::start(
        |c| c.routing_hints.enabled = true,
        &["SELECT value FROM widgets"],
        2,
    )
    .await;
    // Control: a stripped hint that says nothing about the cache.
    let (node, got) = relay
        .query("/*helios:route=any*/ SELECT value FROM widgets")
        .await;
    assert_eq!((node, got), (None, cached_reply()), "served from cache");

    let puts = relay.cache().stats().puts;
    let (node, got) = relay
        .query("/*helios:cache=skip,route=any*/ SELECT value FROM widgets")
        .await;
    assert_eq!(node.as_deref(), Some(relay.addr.as_str()));
    assert_eq!(
        got,
        backend_reply(),
        "a skipped read is not served from cache"
    );
    assert_eq!(relay.cache().stats().puts, puts, "nor stored");

    // An unhinted miss through the backend is stored, as before.
    let (node, _) = relay.query("SELECT value FROM gizmos").await;
    assert!(node.is_some());
    assert_eq!(relay.cache().stats().puts, puts + 1);
    drop(relay);
    assert_eq!(
        backend.await.unwrap(),
        ["SELECT value FROM widgets", "SELECT value FROM gizmos"],
        "the hint comment was stripped from the forwarded text"
    );
}

/// `cache=bypass` and `cache=none` are skips to the routing parser, which
/// strips them from the forwarded text; the cache used to ignore both words
/// when it read the client's text back, and served the cached row.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn forward_simple_query_honours_stripped_skip_synonyms() {
    let (mut relay, backend) = Relay::start(
        |c| c.routing_hints.enabled = true,
        &["SELECT value FROM widgets"],
        2,
    )
    .await;
    let puts = relay.cache().stats().puts;
    for hint in ["/*helios:cache=bypass*/", "/*helios:cache=none*/"] {
        let (node, got) = relay
            .query(&format!("{hint} SELECT value FROM widgets"))
            .await;
        assert_eq!(node.as_deref(), Some(relay.addr.as_str()), "{hint}");
        assert_eq!(got, backend_reply(), "{hint}: not served from cache");
    }
    assert_eq!(relay.cache().stats().puts, puts, "nor stored");
    drop(relay);
    assert_eq!(
        backend.await.unwrap(),
        ["SELECT value FROM widgets", "SELECT value FROM widgets"],
        "the hint comments were stripped from the forwarded text"
    );
}

/// An excluded read is neither looked up nor captured or stored, even with
/// an entry for it already cached; other tables are unaffected.
#[tokio::test]
async fn excluded_read_is_neither_served_nor_captured() {
    let (mut relay, backend) = Relay::start(
        |c| c.cache.exclude_tables = vec!["Widgets".to_string()],
        &["SELECT value FROM widgets", "SELECT value FROM gadgets"],
        2,
    )
    .await;
    let before = relay.cache().stats();
    let (node, got) = relay.query("SELECT value FROM public.widgets").await;
    assert_eq!(node.as_deref(), Some(relay.addr.as_str()));
    assert_eq!(got, backend_reply());
    let (node, got) = relay.query("SELECT value FROM \"widgets\" w").await;
    assert!(node.is_some());
    assert_eq!(got, backend_reply());
    let after = relay.cache().stats();
    assert_eq!(after.puts, before.puts, "not stored");
    assert_eq!(after.total_misses, before.total_misses, "not looked up");
    assert_eq!(relay.excluded(), 2);

    let (node, got) = relay.query("SELECT value FROM gadgets").await;
    assert_eq!((node, got), (None, cached_reply()));
    assert_eq!(relay.excluded(), 2);
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

/// The exclusion scan, and `heliosdb_proxy_cache_excluded_total`, cover only
/// reads a result cache would look up: an in-transaction read used to be
/// scanned and counted as excluded although neither cache serves it.
#[tokio::test]
async fn exclusions_count_only_reads_a_cache_would_look_up() {
    let (mut relay, backend) = Relay::start(
        |c| c.cache.exclude_tables = vec!["widgets".to_string()],
        &[],
        2,
    )
    .await;
    relay.session.in_transaction.store(true, Ordering::Relaxed);
    let (node, _) = relay.query("SELECT value FROM widgets").await;
    assert!(node.is_some());
    assert_eq!(relay.excluded(), 0, "an in-transaction read is not cached");
    relay.session.in_transaction.store(false, Ordering::Relaxed);
    let (node, _) = relay.query("SELECT value FROM widgets").await;
    assert!(node.is_some());
    assert_eq!(relay.excluded(), 1);
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

/// The forward path's half of the facts' laziness contract
/// (`building_facts_classifies_nothing`): with neither result cache running,
/// a read is classified once, by the write check every route needs. The
/// refusal gate used to run the cacheable-read classifier (a lower-cased
/// copy of the statement and a dozen substring scans) on every read first.
#[tokio::test]
async fn a_read_is_classified_once_without_a_result_cache() {
    let (mut relay, backend) = Relay::start(|c| c.cache.enabled = false, &[], 2).await;
    assert!(relay.server.state.query_cache.is_none());
    assert!(!relay.config.edge.enabled);
    for sql in [
        "SELECT value FROM widgets WHERE id = 1",
        "SELECT setting FROM pg_settings",
    ] {
        let before = super::super::stmt_fact_classifications();
        let (node, _) = relay.query(sql).await;
        assert!(node.is_some(), "{sql}");
        assert_eq!(
            super::super::stmt_fact_classifications() - before,
            1,
            "{sql}: is_write only"
        );
    }
    assert_eq!(relay.catalog_skipped(), 0, "no cache, nothing refused");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

/// R6-1: a `;` in a backend-ambiguous rest may end a statement for a
/// backend where the journal capture reads none: `SELECT E'\'' ; UPDATE b …`
/// runs the UPDATE with `standard_conforming_strings` on, HeliosDB-Nano ends
/// `/* a /* b */` at its first `*/`, PostgreSQL ends a `--` comment at a
/// lone `\r`, HeliosDB-Lite ends a statement at a `;` inside a comment and
/// reads a quote inside one as the start of a literal, and HeliosDB-Nano
/// knows no dollar tag holding a non-ASCII byte. Read or write, on any
/// backend, its `ReadyForQuery` retires every cached result, counted in
/// `unseen_write_invalidations`. Nothing else sets the signal: a `;` inside
/// a plain literal or a quoted identifier ends no statement, a final `;` has
/// none after it, a comment holding a quote with no `;` after it or whose
/// quotes all close is no ambiguity, and neither is a `$`, a backslash or a
/// comment marker alone.
#[tokio::test]
async fn a_semicolon_in_an_ambiguous_rest_retires_the_cache() {
    const READ: &str = "SELECT v FROM b WHERE id = 1";
    const HIDDEN: [&str; 12] = [
        "SELECT E'\\'' ; UPDATE b SET v = 2",
        "SELECT 1 /* a /* b */ ; DELETE FROM b */",
        "SELECT 1 -- note\r; DELETE FROM b",
        "UPDATE a SET p = 'C:\\' ; UPDATE b SET v = 2",
        "SELECT 1 -- ; UPDATE b SET v = 2",
        "UPDATE a SET v = 1 -- old: x; DELETE FROM b",
        "SELECT 1 -- it's\n, 'a; DELETE FROM b; --'",
        "SELECT 1 /* say \"hi */, 'a\"; DELETE FROM b; --'",
        "SELECT 1 /* ' \" ' \" */, 'x\"; DELETE FROM b; --'",
        "SET app.tag = $\u{e9}$ ; TRUNCATE b ; SELECT 1 FROM t ; SET app.tag = $\u{e9}$",
        "SELECT $\u{e9}$ $$ $\u{e9}$; DELETE FROM b",
        "SELECT $\u{e9}$;$\u{e9}$; UPDATE b SET v = 2",
    ];
    const PLAIN: [&str; 12] = [
        "SELECT 1; SELECT 2",
        "SELECT 'a;b' FROM t",
        "SELECT 'a;b' FROM t /*app*/",
        "SELECT 'a;b', '$' FROM t",
        "SELECT 'a;b', \"x$y\" FROM t -- tagged",
        "SELECT 'C:\\tmp;x' FROM t",
        "SELECT 1; -- done",
        "SELECT * FROM products WHERE name ILIKE '%a;b%' /*controller='search'*/",
        "SELECT * FROM t /*controller='users'*/ WHERE note LIKE '%;%'",
        "SELECT 'C:\\' ;",
        "SELECT 1 -- done;",
        "SELECT 1 /* it's */ ;",
    ];
    for heliosdb in [false, true] {
        for (sql, hidden) in HIDDEN
            .iter()
            .map(|s| (s, true))
            .chain(PLAIN.iter().map(|s| (s, false)))
        {
            let (mut relay, backend) = Relay::start(|_| {}, &[READ], 1 + usize::from(hidden)).await;
            relay
                .server
                .state
                .heliosdb_backend
                .store(heliosdb, Ordering::Release);
            relay.answered_by_backend(sql).await;
            assert!(
                !relay.session.cache_unseen_write.load(Ordering::Relaxed),
                "{sql:?}: consumed at its ReadyForQuery"
            );
            assert_eq!(
                relay.cache().unseen_write_invalidations(),
                u64::from(hidden),
                "{sql:?}"
            );
            let (node, got) = relay.query(READ).await;
            if hidden {
                assert_eq!(node.as_deref(), Some(relay.addr.as_str()), "{sql:?}");
            } else {
                assert_eq!((node, got), (None, cached_reply()), "{sql:?}: still served");
            }
            drop(relay);
            assert_eq!(
                backend.await.unwrap().len(),
                1 + usize::from(hidden),
                "{sql:?}"
            );
        }
    }
}

/// A `$` inside a name, or a tag starting with a digit, is where the
/// backends part: PostgreSQL reads `a$b$` as a name (and runs the UPDATE
/// after its `;`, which the journal capture, splitting as PostgreSQL does,
/// journals), while HeliosDB-Nano and HeliosDB-Lite open a dollar quote there
/// (`SELECT 1 AS a$$ -- $$; DELETE FROM b` runs the DELETE on Nano, and the
/// capture sees one read). Backend-ambiguous text with a `;`: every cached
/// result is retired at its `ReadyForQuery`. The Nano cases used to retire
/// nothing.
#[tokio::test]
async fn a_dollar_quote_inside_a_name_retires_the_cache() {
    const READ: &str = "SELECT v FROM b WHERE id = 1";
    for sql in [
        "SELECT 1 AS a$b$ ; UPDATE b SET v = 2",
        "SELECT 1 AS a$$ ; UPDATE b SET v = 2; SELECT 1 AS c$$",
        "SELECT 1 AS a$$ -- $$; DELETE FROM b",
        "SELECT 1 AS a$b$ -- $b$; UPDATE b SET v = 2",
        "SELECT $1$ -- $1$; DELETE FROM b",
        "SELECT $1$ ' $1$; DELETE FROM b; --'",
    ] {
        let (mut relay, backend) = Relay::start(|_| {}, &[READ], 2).await;
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(READ).await;
        assert_eq!(relay.cache().unseen_write_invalidations(), 1, "{sql:?}");
        drop(relay);
        assert_eq!(backend.await.unwrap(), [sql, READ]);
    }
}

/// R6-3 in the journal capture: a statement whose first word lies behind a
/// leading comment PostgreSQL and HeliosDB-Nano end in different places is
/// a write of unknown scope, so its `ReadyForQuery` retires every cached
/// result. The capture used to read PostgreSQL's first word (`SELECT`, or
/// none after an unterminated nested comment) and register nothing, while
/// HeliosDB-Nano ran the write; no `;` follows the ambiguous text, so no
/// unseen-write signal is set either (R6-1).
#[tokio::test]
async fn a_leading_comment_the_backends_end_apart_retires_the_cache() {
    const READ: &str = "SELECT v FROM b WHERE id = 1";
    for sql in [
        "/* a /* b */ UPDATE b SET v = 2 WHERE id = 1",
        "-- c\rSELECT 1\nUPDATE b SET v = 2 WHERE id = 1",
        "/* /* */ UPDATE b SET v = 2 WHERE id = 1 -- */ SELECT 1",
        "/* purge /tmp/* entries */ DELETE FROM b WHERE expired",
        "-- x\rSELECT ' -- \nDELETE FROM b -- '",
        "SELECT 1; /* a /* b */ DELETE FROM b",
        "SELECT 1; /* x /* y */ DELETE FROM b -- */ SELECT 2",
    ] {
        let (mut relay, backend) = Relay::start(|_| {}, &[READ], 2).await;
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(READ).await;
        assert_eq!(relay.cache().unseen_write_invalidations(), 0, "{sql:?}");
        drop(relay);
        assert_eq!(backend.await.unwrap(), [sql, READ]);
    }
}

/// A write naming a quoted identifier (what Django and Rails send:
/// `INSERT INTO "app_book" …`) used to move only the generation of a table
/// `into`, which the table pattern took from its own optional `INTO`: the
/// cached read of `app_book` was served after the INSERT. Its scope is
/// unknown now, so everything is retired.
#[tokio::test]
async fn a_write_to_a_quoted_name_retires_the_cache() {
    const READ: &str = "SELECT * FROM app_book WHERE id = 1";
    for sql in [
        "INSERT INTO \"app_book\" (\"title\") VALUES ('x') RETURNING \"app_book\".\"id\"",
        "UPDATE public.\"app_book\" SET title = 'y' WHERE id = 1",
    ] {
        let (mut relay, backend) = Relay::start(|_| {}, &[READ], 2).await;
        relay.answered_by_backend(sql).await;
        relay.answered_by_backend(READ).await;
        drop(relay);
        assert_eq!(backend.await.unwrap(), [sql, READ]);
    }
}

/// A write that quotes only a column or a conflict target (ORM text with
/// mixed-case columns), or carries a query-tag comment whose quotes all close
/// next to a `;` inside a literal (sqlcommenter), retires its own table and
/// sets no unseen-write signal: every backend, HeliosDB-Lite's comment-blind
/// splitter included, reads one statement. Each used to retire every cached
/// result, the tagged one twice (an unknown scope and an unseen write).
#[tokio::test]
async fn a_quoted_column_or_a_query_tag_keeps_the_write_targeted() {
    const OTHER: &str = "SELECT v FROM b WHERE id = 1";
    const OWN: &str = "SELECT v FROM t WHERE id = 1";
    for sql in [
        "UPDATE t SET \"lastLogin\" = now() WHERE id = 1",
        "INSERT INTO t (\"orderId\") VALUES (1) ON CONFLICT (\"orderId\") DO NOTHING",
        "/*application='app',action='save'*/ UPDATE t SET note = 'a;b' WHERE id = 1",
    ] {
        let (mut relay, backend) = Relay::start(|_| {}, &[OTHER, OWN], 2).await;
        relay.answered_by_backend(sql).await;
        assert_eq!(relay.cache().unseen_write_invalidations(), 0, "{sql:?}");
        let (node, got) = relay.query(OTHER).await;
        assert_eq!(
            (node, got),
            (None, cached_reply()),
            "{sql:?}: another table's read is still served"
        );
        relay.answered_by_backend(OWN).await;
        drop(relay);
        assert_eq!(backend.await.unwrap(), [sql, OWN]);
    }
}

/// `[edge]` invalidates on a simple-protocol write whatever its first word
/// is behind: a `MERGE` (PostgreSQL's `MERGE INTO … USING …` and
/// HeliosDB-Nano's `MERGE BRANCH`), a write after a leading comment, and a
/// statement after a leading comment the backends end in different places.
/// None used to reach the edge invalidation (its write check read the raw
/// first word and knew no `MERGE`). DDL and a write to a quoted name used to
/// invalidate the wrong tables (a view's base table, a table `into`).
#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn simple_protocol_writes_the_edge_check_used_to_miss_flush_it() {
    const KV: &str = "SELECT v FROM kv WHERE id = 1";
    const VIEW: &str = "SELECT v FROM kv_v WHERE id = 1";
    for (read, write) in [
        (KV, "MERGE BRANCH fix INTO main"),
        (
            KV,
            "MERGE INTO kv USING src ON kv.id = src.id WHEN MATCHED THEN UPDATE SET v = src.v",
        ),
        (KV, "/* job */ UPDATE kv SET v = 2 WHERE id = 1"),
        (KV, "/* a /* b */ UPDATE kv SET v = 2 WHERE id = 1"),
        (KV, "INSERT INTO \"kv\" VALUES (1)"),
        (VIEW, "CREATE OR REPLACE VIEW kv_v AS SELECT * FROM kv"),
        (
            VIEW,
            "-- v2\nCREATE OR REPLACE VIEW kv_v AS SELECT * FROM kv",
        ),
    ] {
        let (mut relay, backend) = Relay::start(
            |c| {
                c.cache.enabled = false;
                c.edge.enabled = true;
            },
            &[],
            2,
        )
        .await;
        relay.answered_by_backend(read).await;
        let (node, got) = relay.query(read).await;
        assert_eq!((node, got), (None, backend_reply()), "{write:?}: edge hit");
        relay.answered_by_backend(write).await;
        assert_eq!(
            relay.server.state.edge_cache.stats().current_entries,
            0,
            "{write:?}: the read's entry is invalidated"
        );
        drop(relay);
        assert_eq!(backend.await.unwrap(), [read, write]);
    }
}

/// Inside a transaction the unseen write is retired again at the commit
/// (the stage turns unknown); a synthesized `ReadyForQuery` drops the flag.
#[tokio::test]
async fn an_unseen_write_in_a_transaction_retires_everything_at_its_commit() {
    const READ: &str = "SELECT v FROM b WHERE id = 1";
    let (relay, backend) = Relay::start(|_| {}, &[READ], 0).await;
    let shared = crate::cache::CacheContext {
        database: "default".to_string(),
        ..Default::default()
    };
    relay
        .session
        .cache_unseen_write
        .store(true, Ordering::Relaxed);
    let _ = ProxyServer::journal_observe_sync(
        &relay.session,
        &relay.server.state,
        crate::journal_capture::ResponseOutcome::status_only(b'T'),
    );
    assert!(!relay.session.cache_unseen_write.load(Ordering::Relaxed));
    assert!(relay.session.tx_cache_stage.lock().unwrap().unknown);
    assert!(matches!(
        relay.cache().get(READ, &shared).await,
        crate::cache::CacheLookup::Miss
    ));
    relay
        .session
        .cache_unseen_write
        .store(true, Ordering::Relaxed);
    ProxyServer::journal_discard(&relay.session, b'I');
    assert!(!relay.session.cache_unseen_write.load(Ordering::Relaxed));
    drop(relay);
    assert!(backend.await.unwrap().is_empty());
}

/// With only `[edge]` running, a session the edge cache no longer serves
/// (session state, F2) is not scanned or counted either.
#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn edge_only_exclusions_skip_sessions_the_edge_cache_does_not_serve() {
    let (mut relay, backend) = Relay::start(
        |c| {
            c.cache.enabled = false;
            c.edge.enabled = true;
            c.cache.exclude_tables = vec!["widgets".to_string()];
        },
        &[],
        2,
    )
    .await;
    assert!(relay.server.state.query_cache.is_none());
    relay.session.edge_ineligible.store(true, Ordering::Relaxed);
    relay.query("SELECT value FROM widgets").await;
    assert_eq!(
        relay.excluded(),
        0,
        "the edge cache does not serve the session"
    );
    relay
        .session
        .edge_ineligible
        .store(false, Ordering::Relaxed);
    relay.query("SELECT value FROM widgets").await;
    assert_eq!(relay.excluded(), 1);
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

/// A shorter `[[cache.tables]]` TTL caps the edge entry of a read naming
/// the table. Edge entries always lived `[edge] default_ttl_secs`, and the
/// edge lookup runs first, so a table written outside the proxy was served
/// stale for up to that long although its TTL promised less.
#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn a_shorter_table_ttl_caps_the_edge_entry() {
    let (mut relay, backend) = Relay::start(
        |c| {
            c.cache.enabled = false;
            c.edge.enabled = true;
            c.edge.default_ttl_secs = 60;
            c.cache.tables = vec![
                crate::config::CacheTableToml {
                    name: "Prices".to_string(),
                    ttl_secs: Some(5),
                    exclude: false,
                },
                crate::config::CacheTableToml {
                    name: "slow".to_string(),
                    ttl_secs: Some(600),
                    exclude: false,
                },
            ];
        },
        &[],
        5,
    )
    .await;
    let expires_in = |relay: &Relay, sql: &str| {
        let fp = crate::edge::fingerprint::analyze(sql, "default", "", &[]);
        let key = crate::edge::CacheKey {
            fingerprint: fp.fingerprint,
            params_hash: fp.params_hash,
            database: "default".to_string(),
            user: String::new(),
        };
        let entry = relay.server.state.edge_cache.get(&key).expect("stored");
        entry
            .expires_at
            .saturating_duration_since(std::time::Instant::now())
    };
    for (sql, ttl) in [
        ("SELECT * FROM prices WHERE id = 1", 5),
        // The extraction finds the name behind a schema and a comment.
        (
            "SELECT * FROM gadgets g JOIN public./**/PRICES p ON true",
            5,
        ),
        // Fail-closed: the scan finds a quoted name, which the extraction
        // misses.
        (
            "SELECT * FROM gadgets g JOIN public.\"prices\" p ON true",
            5,
        ),
        // Another table keeps the edge default; a longer table TTL never
        // extends it.
        ("SELECT * FROM gadgets WHERE id = 1", 60),
        ("SELECT * FROM slow WHERE id = 1", 60),
    ] {
        let (node, _) = relay.query(sql).await;
        assert!(node.is_some(), "{sql}");
        let left = expires_in(&relay, sql);
        assert!(
            left <= Duration::from_secs(ttl) && left > Duration::from_secs(ttl - 2),
            "{sql}: {left:?}"
        );
    }
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 5);
}

/// `[cache]` is built once at startup: a SIGHUP keeps the running section
/// (so `live_config` and `/config` stay truthful), except
/// `max_cacheable_response_bytes`, which every read takes from the live
/// config.
#[tokio::test]
async fn a_sighup_reload_keeps_the_running_cache_section() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("proxy.toml");
    let nodes = "[[nodes]]\nhost = \"pg-p\"\nport = 5432\nrole = \"primary\"\n";
    std::fs::write(
        &path,
        format!("[cache]\nenabled = true\nmax_cacheable_response_bytes = 4096\n{nodes}"),
    )
    .unwrap();
    let path = path.to_string_lossy().into_owned();
    let config = ProxyConfig::from_file(&path).unwrap();
    let running = config.cache.clone();
    let server = ProxyServer::new(config)
        .unwrap()
        .with_config_path(Some(path.clone()));

    std::fs::write(
        &path,
        format!(
            "[cache]\nenabled = true\nmax_cacheable_response_bytes = 8192\n\
             semantic_enabled = true\nexclude_tables = [\"secret\"]\nttl_secs = 9\n{nodes}"
        ),
    )
    .unwrap();
    server.reload_config().await;
    let live = server.state.live_config.load_full();
    let mut expected = running.clone();
    expected.max_cacheable_response_bytes = 8192;
    assert_eq!(live.cache, expected);
    assert!(!live.cache.semantic_enabled && live.cache.exclude_tables.is_empty());

    // Only max_cacheable_response_bytes changed: applied, no section kept.
    std::fs::write(
        &path,
        format!("[cache]\nenabled = true\nmax_cacheable_response_bytes = 1024\n{nodes}"),
    )
    .unwrap();
    server.reload_config().await;
    let live = server.state.live_config.load_full();
    assert_eq!(live.cache.max_cacheable_response_bytes, 1024);
    assert_eq!(live.cache.ttl_secs, running.ttl_secs);
}

/// The capture ceiling comes from the live config: a SIGHUP that lowers
/// `max_cacheable_response_bytes` binds connections opened before it (their
/// snapshot still says 4096), and so does raising it again. They used to
/// keep their snapshot's ceiling while the `[edge]` one moved at once.
#[tokio::test]
async fn the_capture_ceiling_follows_the_live_config() {
    let (mut relay, backend) =
        Relay::start(|c| c.cache.max_cacheable_response_bytes = 4096, &[], 2).await;
    let reload = |relay: &Relay, bytes: usize| {
        let state = &relay.server.state;
        let mut next = (*state.live_config.load_full()).clone();
        next.cache.max_cacheable_response_bytes = bytes;
        state.live_config.store(Arc::new(next));
    };
    reload(&relay, backend_reply().len() - 1);
    assert_eq!(relay.config.cache.max_cacheable_response_bytes, 4096);
    let puts = relay.cache().stats().puts;
    let (node, _) = relay.query("SELECT value FROM widgets").await;
    assert!(node.is_some());
    assert_eq!(relay.cache().stats().puts, puts, "over the live ceiling");
    assert_eq!(
        relay
            .server
            .state
            .metrics
            .cache_capture_oversize
            .load(Ordering::Relaxed),
        1
    );
    reload(&relay, 4096);
    let (node, _) = relay.query("SELECT value FROM gadgets").await;
    assert!(node.is_some());
    assert_eq!(relay.cache().stats().puts, puts + 1, "under it again");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

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

/// A backend whose startup `server_version` names HeliosDB sets the sticky
/// flag (R5-1), once; PostgreSQL's, or another parameter naming HeliosDB,
/// does not.
#[test]
fn a_heliosdb_server_version_sets_the_sticky_flag() {
    let mut config = test_config();
    config.cache.enabled = true;
    let server = ProxyServer::new(config).unwrap();
    let state = &server.state;
    let qc = state.query_cache.as_ref().unwrap();
    let flag = || state.heliosdb_backend.load(Ordering::Acquire);
    let pg = [
        wire_frame(b'S', b"application_name\0HeliosDB Nano\0"),
        wire_frame(b'S', b"server_version\x0018.4\0"),
        wire_frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]),
        wire_frame(b'Z', b"I"),
    ]
    .concat();
    ProxyServer::note_backend_startup(state, "pg:5432", &pg);
    assert!(!flag() && !qc.heliosdb_backend());
    // A truncated frame list is read as far as it goes.
    ProxyServer::note_backend_startup(state, "pg:5432", &pg[..pg.len() - 3]);
    assert!(!flag());
    let nano = [
        wire_frame(b'S', b"server_version\x0016.0 (HeliosDB Nano 4.41.0)\0"),
        wire_frame(b'Z', b"I"),
    ]
    .concat();
    ProxyServer::note_backend_startup(state, "nano:5432", &nano);
    assert!(flag() && qc.heliosdb_backend());
    // Sticky: another backend, or another HeliosDB one, changes nothing.
    ProxyServer::note_backend_startup(state, "pg:5432", &pg);
    ProxyServer::note_parameter_status(
        state,
        "lite:5432",
        &wire_frame(b'S', b"server_version\x0017.0 (HeliosDB-Lite 2.0)\0"),
    );
    assert!(flag() && qc.heliosdb_backend());
}

/// A HeliosDB startup sets the query cache's flag even when the
/// server-wide one is already set (another connection's startup swapped it
/// and has not reached the cache yet): the second connection then never
/// answers a read with the cache keying the PostgreSQL way.
#[test]
fn a_heliosdb_startup_sets_the_cache_flag_when_the_server_flag_is_set() {
    let mut config = test_config();
    config.cache.enabled = true;
    let server = ProxyServer::new(config).unwrap();
    let state = &server.state;
    let qc = state.query_cache.as_ref().unwrap();
    // The first connection's swap, caught before its cache call.
    state.heliosdb_backend.store(true, Ordering::Release);
    assert!(!qc.heliosdb_backend());
    ProxyServer::note_parameter_status(
        state,
        "nano-2:5432",
        &wire_frame(b'S', b"server_version\x0016.0 (HeliosDB Nano 4.41.0)\0"),
    );
    assert!(qc.heliosdb_backend(), "set by the second connection");
}

/// With `semantic_match = "canonical"`, a paraphrase is a canonical L3 hit
/// until a backend reports HeliosDB; from then on the tier neither serves
/// nor stores (the read counts as ineligible and reaches the backend).
#[tokio::test]
async fn a_heliosdb_backend_turns_the_canonical_tier_off_in_the_relay() {
    let hinted = "SELECT value FROM widgets WHERE id = 1 /*helios:cache=semantic*/";
    let paraphrase = "select value from widgets as w where id = 1 /*helios:cache=semantic*/";
    let (mut relay, backend) = Relay::start(
        |c| {
            c.cache.semantic_enabled = true;
            c.cache.semantic_match = crate::config::SemanticMatch::Canonical;
        },
        &[hinted],
        2,
    )
    .await;
    let (node, got) = relay.query(paraphrase).await;
    assert_eq!((node, got), (None, cached_reply()), "PostgreSQL: L3 hit");
    let nano = wire_frame(b'S', b"server_version\x0016.0 (HeliosDB Nano 4.41.0)\0");
    ProxyServer::note_parameter_status(&relay.server.state, "nano:5432", &nano);
    let l3 = relay.cache().semantic_cache().unwrap().clone();
    let ineligible = l3.health().ineligible;
    relay.answered_by_backend(paraphrase).await;
    assert_eq!(l3.health().ineligible, ineligible + 1);
    relay
        .answered_by_backend("SELECT value FROM widgets WHERE id = 2 /*helios:cache=semantic*/")
        .await;
    assert_eq!(l3.len(), 1, "nothing stored after the flag");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

/// The session's backend reports HeliosDB while a read is in flight (its
/// first connection, dialed for that read): the edge key was made the
/// PostgreSQL way before forwarding, so the response is not stored under it.
/// The next read stores under the verbatim key and is then served.
#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn an_edge_key_made_before_the_heliosdb_flag_is_not_stored() {
    let state_slot: Arc<std::sync::OnceLock<Arc<ServerState>>> = Arc::default();
    let slot = state_slot.clone();
    let (mut relay, backend) = Relay::start_with(
        |c| {
            c.cache.enabled = false;
            c.edge.enabled = true;
        },
        &[],
        move |listener| {
            tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut seen = Vec::new();
                for i in 0..2 {
                    seen.push(read_query(&mut sock).await);
                    if i == 0 {
                        let nano =
                            wire_frame(b'S', b"server_version\x0016.0 (HeliosDB Nano 4.41.0)\0");
                        ProxyServer::note_parameter_status(
                            slot.get().expect("state"),
                            "nano:5432",
                            &nano,
                        );
                    }
                    sock.write_all(&backend_reply()).await.unwrap();
                }
                seen
            })
        },
    )
    .await;
    assert!(state_slot.set(relay.server.state.clone()).is_ok());
    let sql = "SELECT value FROM widgets WHERE id = 1";
    relay.answered_by_backend(sql).await;
    assert_eq!(
        relay.server.state.edge_cache.stats().current_entries,
        0,
        "keyed before the flag: not stored"
    );
    relay.answered_by_backend(sql).await;
    let (node, _) = relay.query(sql).await;
    assert_eq!(node, None, "keyed after it: stored and served");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 2);
}

/// Once a backend reported HeliosDB, the edge key keeps comments and
/// whitespace, and the entries keyed before are dropped: a read with a
/// trailing comment is not served the comment-free read's entry
/// (HeliosDB-Nano's point-lookup fast path declines a statement holding a
/// comment, and its planner reads another table for `Post`).
#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn a_heliosdb_backend_keys_the_edge_cache_as_written() {
    let plain = "SELECT * FROM Post WHERE id = 1";
    let commented = "SELECT * FROM Post WHERE id = 1 /* orm */";
    let (mut relay, backend) = Relay::start(
        |c| {
            c.cache.enabled = false;
            c.edge.enabled = true;
        },
        &[],
        4,
    )
    .await;
    relay.answered_by_backend(plain).await;
    let (node, _) = relay.query(commented).await;
    assert_eq!(node, None, "PostgreSQL: one key");
    let nano = wire_frame(b'S', b"server_version\x0016.0 (HeliosDB Nano 4.41.0)\0");
    ProxyServer::note_parameter_status(&relay.server.state, "nano:5432", &nano);
    assert_eq!(
        relay.server.state.edge_cache.stats().current_entries,
        0,
        "dropped"
    );
    relay.answered_by_backend(plain).await;
    relay.answered_by_backend(commented).await;
    relay
        .answered_by_backend("SELECT * FROM Post WHERE id = 1  ")
        .await;
    let (node, _) = relay.query(plain).await;
    assert_eq!(node, None, "the same text is still served");
    drop(relay);
    assert_eq!(backend.await.unwrap().len(), 4);
}
