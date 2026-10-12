//! Exercise terminal startup errors through the complete client handler, not
//! only the inner authentication relay that already forwards errors correctly.
use super::*;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn wire_frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_request(code: u32, body: &[u8]) -> Vec<u8> {
    let mut payload = code.to_be_bytes().to_vec();
    payload.extend_from_slice(body);
    wire_frame(b'R', &payload)
}

fn sasl_first_response() -> Vec<u8> {
    let first = b"n,,n=,r=fixture_client_nonce";
    let mut body = b"SCRAM-SHA-256\0".to_vec();
    body.extend_from_slice(&(first.len() as i32).to_be_bytes());
    body.extend_from_slice(first);
    wire_frame(b'p', &body)
}

const SCRAM_CONTINUE: &[u8] = b"r=fixture_client_nonce_server,s=MDEyMzQ1Njc4OWFiY2RlZg==,i=4096";
const SCRAM_FINAL: &[u8] =
    b"c=biws,r=fixture_client_nonce_server,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

fn fatal_frame(code: &str) -> Vec<u8> {
    wire_frame(
        b'E',
        format!("SFATAL\0VFATAL\0C{code}\0Mfixture startup refusal\0Dpreserve detail\0\0")
            .as_bytes(),
    )
}

fn config_for(backend: SocketAddr) -> ProxyConfig {
    let mut config = test_config();
    config.nodes.clear();
    config.add_node(&backend.to_string(), "primary").unwrap();
    config.pool.acquire_timeout_secs = 1;
    config.limits.startup_timeout_secs = 3;
    config
}

async fn frontend(config: ProxyConfig) -> (TcpStream, JoinHandle<Result<()>>, Arc<ServerState>) {
    let server = ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
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

async fn finish<T>(mut task: JoinHandle<T>) -> T {
    match tokio::time::timeout(TEST_TIMEOUT, &mut task).await {
        Ok(result) => result.expect("fixture task panicked"),
        Err(_) => {
            task.abort();
            let _ = task.await;
            panic!("fixture task did not terminate within its bound");
        }
    }
}

async fn read_startup(stream: &mut TcpStream) {
    let length = stream.read_u32().await.unwrap() as usize;
    assert!((8..=4096).contains(&length));
    let mut body = vec![0; length - 4];
    stream.read_exact(&mut body).await.unwrap();
    assert_eq!(&body[..4], &196608u32.to_be_bytes());
    assert!(body.windows(11).any(|s| s == b"user\0bench\0"));
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
    .expect("terminal startup response must be followed by EOF")
    .unwrap();
    assert!(
        result.len() < 65_536,
        "response exceeded fixture byte bound"
    );
    result
}

fn assert_single_error(bytes: &[u8], code: &str) {
    assert!(
        bytes.len() >= 6,
        "missing terminal ErrorResponse: {bytes:?}"
    );
    assert_eq!(bytes[0], b'E');
    let length = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
    assert_eq!(bytes.len(), length + 1, "extra frame after terminal error");
    assert_eq!(
        bytes[5..]
            .split(|b| *b == 0)
            .filter_map(|field| field.strip_prefix(b"C"))
            .collect::<Vec<_>>(),
        vec![code.as_bytes()],
        "terminal SQLSTATE changed"
    );
}

async fn fragmented_write(stream: &mut TcpStream, bytes: &[u8]) {
    // Split both the length header and the payload at different boundaries.
    // Separate writes force the relay to support incomplete read buffers;
    // TCP may still coalesce them, so this is not a packetization guarantee.
    for piece in bytes.chunks(3) {
        stream.write_all(piece).await.unwrap();
        tokio::task::yield_now().await;
    }
}

async fn password_rejection(code: &'static str, auth_ok_before_error: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let expected = fatal_frame(code);
    let outgoing = expected.clone();
    let backend = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_startup(&mut stream).await;
        stream.write_all(&auth_request(3, b"")).await.unwrap();
        assert_eq!(
            read_frame(&mut stream).await,
            wire_frame(b'p', b"fixture\0")
        );
        if auth_ok_before_error {
            stream.write_all(&auth_request(0, b"")).await.unwrap();
        }
        fragmented_write(&mut stream, &outgoing).await;
    });
    let (mut client, handler, state) = frontend(config_for(address)).await;
    assert_eq!(read_frame(&mut client).await, auth_request(3, b""));
    client
        .write_all(&wire_frame(b'p', b"fixture\0"))
        .await
        .unwrap();
    if auth_ok_before_error {
        assert_eq!(read_frame(&mut client).await, auth_request(0, b""));
    }
    let received = through_eof(&mut client).await;
    assert!(finish(handler).await.is_err());
    finish(backend).await;
    assert!(
        state.sessions.is_empty(),
        "startup failure leaked a session"
    );
    assert_eq!(
        received, expected,
        "backend terminal frame must be verbatim and final"
    );
}

#[tokio::test]
async fn startup_password_rejection_is_one_fragmented_terminal_error() {
    password_rejection("28P01", false).await;
}

#[tokio::test]
async fn startup_database_rejection_after_auth_ok_is_one_terminal_error() {
    password_rejection("3D000", true).await;
}

#[tokio::test]
async fn startup_scram_rejection_after_continue_is_one_terminal_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let expected = fatal_frame("28P01");
    let outgoing = expected.clone();
    let backend = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_startup(&mut stream).await;
        stream
            .write_all(&auth_request(10, b"SCRAM-SHA-256\0\0"))
            .await
            .unwrap();
        assert_eq!(read_frame(&mut stream).await, sasl_first_response());
        fragmented_write(&mut stream, &auth_request(11, SCRAM_CONTINUE)).await;
        assert_eq!(read_frame(&mut stream).await, wire_frame(b'p', SCRAM_FINAL));
        fragmented_write(&mut stream, &outgoing).await;
    });
    let (mut client, handler, state) = frontend(config_for(address)).await;
    assert_eq!(
        read_frame(&mut client).await,
        auth_request(10, b"SCRAM-SHA-256\0\0")
    );
    client.write_all(&sasl_first_response()).await.unwrap();
    assert_eq!(
        read_frame(&mut client).await,
        auth_request(11, SCRAM_CONTINUE)
    );
    client
        .write_all(&wire_frame(b'p', SCRAM_FINAL))
        .await
        .unwrap();
    let received = through_eof(&mut client).await;
    assert!(finish(handler).await.is_err());
    finish(backend).await;
    assert!(state.sessions.is_empty());
    assert_eq!(received, expected);
}

#[tokio::test]
async fn startup_backend_connect_failure_still_sends_one_synthetic_error() {
    // Port zero cannot be a listening backend; unlike dropping an ephemeral
    // listener, this cannot race another process rebinding that port.
    let (mut client, handler, state) = frontend(config_for("127.0.0.1:0".parse().unwrap())).await;
    let received = through_eof(&mut client).await;
    assert!(finish(handler).await.is_err());
    assert!(state.sessions.is_empty());
    assert_single_error(&received, "08006");
}

#[tokio::test]
async fn startup_capacity_retries_are_hidden_then_emit_one_53300() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, mut stopped) = oneshot::channel::<()>();
    let backend = tokio::spawn(async move {
        let mut attempts = 0;
        loop {
            tokio::select! {
                _ = &mut stopped => return attempts,
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.unwrap();
                    read_startup(&mut stream).await;
                    stream.write_all(&fatal_frame("53300")).await.unwrap();
                    attempts += 1;
                }
            }
        }
    });
    let (mut client, handler, state) = frontend(config_for(address)).await;
    let received = through_eof(&mut client).await;
    assert!(finish(handler).await.is_err());
    stop.send(()).unwrap();
    let attempts = finish(backend).await;
    assert!(attempts >= 2, "capacity retry path was not exercised");
    assert!(state.metrics.backend_capacity_waits.load(Ordering::Relaxed) > 0);
    assert_eq!(
        state
            .metrics
            .backend_capacity_refusals
            .load(Ordering::Relaxed),
        1
    );
    assert!(state.sessions.is_empty());
    assert_single_error(&received, "53300");
}

#[tokio::test]
async fn startup_capacity_retry_can_succeed_without_publishing_refusal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut success = auth_request(0, b"");
    success.extend(wire_frame(b'S', b"server_version\x0018.4\0"));
    success.extend(wire_frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
    success.extend(wire_frame(b'Z', b"I"));
    let outgoing = success.clone();
    let backend = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_startup(&mut first).await;
        first.write_all(&fatal_frame("53300")).await.unwrap();
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        read_startup(&mut second).await;
        second.write_all(&outgoing).await.unwrap();
        let _ = through_eof(&mut second).await;
    });
    let (mut client, handler, state) = frontend(config_for(address)).await;
    let mut received = Vec::new();
    for _ in 0..4 {
        received.extend(read_frame(&mut client).await);
    }
    client.write_all(&wire_frame(b'X', b"")).await.unwrap();
    assert!(through_eof(&mut client).await.is_empty());
    finish(handler).await.unwrap();
    finish(backend).await;
    assert!(state.metrics.backend_capacity_waits.load(Ordering::Relaxed) > 0);
    assert_eq!(
        state
            .metrics
            .backend_capacity_refusals
            .load(Ordering::Relaxed),
        0
    );
    assert!(state.sessions.is_empty());
    assert_eq!(
        received, success,
        "intermediate capacity error escaped to client"
    );
}

#[tokio::test]
async fn startup_hba_rejection_remains_one_terminal_error() {
    let mut config = config_for("127.0.0.1:0".parse().unwrap());
    config.hba.push(crate::config::HbaRule {
        action: crate::config::HbaAction::Reject,
        user: "bench".to_string(),
        database: "all".to_string(),
        address: "all".to_string(),
    });
    let (mut client, handler, state) = frontend(config).await;
    let received = through_eof(&mut client).await;
    finish(handler).await.unwrap();
    assert!(state.sessions.is_empty());
    assert_single_error(&received, "28000");
}
