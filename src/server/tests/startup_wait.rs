//! Client startup waits for an eligible healthy node (`[limits]
//! startup_node_wait_ms`), and an immediate probe of a newly resolved leader.
use super::*;
use tokio::task::JoinHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn wire_frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// AuthenticationOk, a server_version ParameterStatus, BackendKeyData and
/// ReadyForQuery: what a trusting backend sends a passed-through client.
fn auth_ok_frames() -> Vec<u8> {
    let mut out = wire_frame(b'R', &0u32.to_be_bytes());
    out.extend(wire_frame(b'S', b"server_version\x0018.4\0"));
    out.extend(wire_frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
    out.extend(wire_frame(b'Z', b"I"));
    out
}

fn config_for(backend: SocketAddr, wait_ms: u64) -> ProxyConfig {
    let mut config = test_config();
    config.nodes.clear();
    config.add_node(&backend.to_string(), "primary").unwrap();
    config.limits.startup_node_wait_ms = wait_ms;
    config
}

/// Mark every configured node healthy or not in the live health snapshot.
fn set_health(state: &ServerState, healthy: bool) {
    let mut map = (*state.health.load_full()).clone();
    for node in map.values_mut() {
        node.healthy = healthy;
    }
    state.health.store(Arc::new(map));
}

/// A client connected to the proxy with the node's health set before the
/// startup is read, so the first selection sees it.
async fn start(
    config: ProxyConfig,
    healthy: bool,
) -> (TcpStream, JoinHandle<Result<()>>, Arc<ServerState>) {
    let server = ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
    set_health(&state, healthy);
    let (proxy, mut client) = pair().await;
    let peer = proxy.peer_addr().unwrap();
    let task = tokio::spawn(ProxyServer::handle_client(
        proxy,
        peer,
        state.clone(),
        Arc::new(config),
        server.shutdown_tx.clone(),
    ));
    client.write_all(&startup_bytes("bench")).await.unwrap();
    (client, task, state)
}

async fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut header = [0; 5];
        stream.read_exact(&mut header).await.unwrap();
        let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        assert!((4..=4096).contains(&length));
        let mut result = header.to_vec();
        result.resize(length + 1, 0);
        stream.read_exact(&mut result[5..]).await.unwrap();
        result
    })
    .await
    .expect("expected frame did not arrive")
}

async fn through_eof(stream: &mut TcpStream) -> Vec<u8> {
    let mut result = Vec::new();
    tokio::time::timeout(
        TEST_TIMEOUT,
        (&mut *stream).take(65_536).read_to_end(&mut result),
    )
    .await
    .expect("refused startup must be followed by EOF")
    .unwrap();
    result
}

async fn finish<T>(mut task: JoinHandle<T>) -> T {
    match tokio::time::timeout(TEST_TIMEOUT, &mut task).await {
        Ok(result) => result.expect("task panicked"),
        Err(_) => {
            task.abort();
            let _ = task.await;
            panic!("task did not terminate within its bound");
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// A backend that accepts one startup, answers it as trusting, then holds the
/// connection until the proxy closes it.
fn trusting_backend(listener: TcpListener) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let length = stream.read_u32().await.unwrap() as usize;
        let mut body = vec![0; length - 4];
        stream.read_exact(&mut body).await.unwrap();
        stream.write_all(&auth_ok_frames()).await.unwrap();
        let mut rest = Vec::new();
        let _ = tokio::time::timeout(TEST_TIMEOUT, stream.read_to_end(&mut rest)).await;
    })
}

/// A node unhealthy at startup that turns healthy inside the bound is served.
#[tokio::test]
async fn startup_waits_for_a_node_that_turns_healthy_and_serves_the_client() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let backend = trusting_backend(listener);
    let (mut client, handler, state) = start(config_for(address, 5_000), false).await;
    let recovery = {
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            set_health(&state, true);
        })
    };
    for _ in 0..4 {
        read_frame(&mut client).await;
    }
    client.write_all(&wire_frame(b'X', b"")).await.unwrap();
    assert!(through_eof(&mut client).await.is_empty());
    finish(handler).await.unwrap();
    recovery.await.unwrap();
    finish(backend).await;
    assert_eq!(state.metrics.startup_waits.load(Ordering::Relaxed), 1);
    assert_eq!(
        state.metrics.startup_wait_timeouts.load(Ordering::Relaxed),
        0
    );
    assert!(state.sessions.is_empty(), "served session leaked");
}

/// A node that never becomes healthy refuses the client with 08006 after the
/// bound, not before it, and the waiter is counted as a timeout.
#[tokio::test]
async fn startup_refuses_with_08006_after_the_bound_when_no_node_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let started = tokio::time::Instant::now();
    let (mut client, handler, state) = start(config_for(address, 200), false).await;
    let received = through_eof(&mut client).await;
    let elapsed = started.elapsed();
    assert!(finish(handler).await.is_err());
    assert_eq!(received.first(), Some(&b'E'), "{received:?}");
    assert!(contains(&received, b"C08006\0"), "{received:?}");
    assert!(
        contains(&received, b"No healthy nodes available"),
        "{received:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(200),
        "refused before the bound: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(5), "waited far too long");
    assert_eq!(state.metrics.startup_waits.load(Ordering::Relaxed), 1);
    assert_eq!(
        state.metrics.startup_wait_timeouts.load(Ordering::Relaxed),
        1
    );
    assert!(
        state.sessions.is_empty(),
        "refused startup leaked a session"
    );
}

/// `startup_node_wait_ms = 0` refuses at once, as before the wait existed,
/// and does not count a wait.
#[tokio::test]
async fn zero_startup_wait_refuses_at_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (mut client, handler, state) = start(config_for(address, 0), false).await;
    let received = through_eof(&mut client).await;
    assert!(finish(handler).await.is_err());
    assert!(contains(&received, b"C08006\0"), "{received:?}");
    assert_eq!(state.metrics.startup_waits.load(Ordering::Relaxed), 0);
    assert_eq!(
        state.metrics.startup_wait_timeouts.load(Ordering::Relaxed),
        0
    );
}

/// The `[limits]` key maps into the resolved limits the startup path reads.
#[test]
fn startup_node_wait_maps_into_the_resolved_limits() {
    let mut l = crate::config::LimitsToml::default();
    assert_eq!(
        crate::server::limits::ResolvedLimits::from_toml(&l).startup_node_wait,
        Duration::from_millis(5_000)
    );
    l.startup_node_wait_ms = 250;
    assert_eq!(
        crate::server::limits::ResolvedLimits::from_toml(&l).startup_node_wait,
        Duration::from_millis(250)
    );
    l.startup_node_wait_ms = 0;
    assert!(crate::server::limits::ResolvedLimits::from_toml(&l)
        .startup_node_wait
        .is_zero());
}

/// A probe of a newly resolved leader counts one success toward recovery
/// right away, and the node is re-marked healthy once `success_threshold`
/// probes have succeeded. An address that is not a configured node is ignored.
#[tokio::test]
async fn immediate_leader_probe_counts_toward_recovery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = [0u8; 8];
                if stream.read_exact(&mut request).await.is_ok() {
                    let _ = stream.write_all(b"N").await;
                }
            });
        }
    });
    let mut config = config_for(address, 0);
    config.health.success_threshold = 2;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    set_health(&state, false);
    let addr = address.to_string();
    let snapshot = |state: &ServerState| {
        let map = state.health.load_full();
        map.get(&addr).map(|h| (h.healthy, h.success_count))
    };

    ProxyServer::probe_node_now(&state, &addr).await;
    assert_eq!(snapshot(&state), Some((false, 1)));
    ProxyServer::probe_node_now(&state, &addr).await;
    assert_eq!(snapshot(&state), Some((true, 2)));

    let before = state.health.load_full().len();
    ProxyServer::probe_node_now(&state, "127.0.0.1:1").await;
    assert_eq!(state.health.load_full().len(), before);
    node.abort();
}
