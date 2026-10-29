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

/// A circuit fast-fail never reached the backend, so a transaction open there
/// stays open: inside it the refusal's ReadyForQuery reports the backend's
/// real status ('T'), never 'I'. In autocommit it is still 'I'.
#[tokio::test]
async fn a_circuit_fast_fail_inside_a_transaction_reports_status_t() {
    use std::sync::atomic::Ordering;

    let mut config = super::test_config();
    config.circuit_breaker.enabled = true;
    config.circuit_breaker.failure_threshold = 1;
    let server = super::ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
    let node = config.nodes[0].address().to_string();
    let breaker = state.circuit_breaker.as_ref().unwrap().get_breaker(&node);
    breaker.record_failure("boom");
    assert_eq!(breaker.get_state(), CircuitState::Open);
    let session = super::make_test_session();
    for status in *b"TI" {
        session.last_rfq_status.store(status, Ordering::Relaxed);
        session
            .in_transaction
            .store(status != b'I', Ordering::Relaxed);
        let reply =
            super::refused_simple_reply(&state, &config, &session, "INSERT INTO t VALUES (1)")
                .await;
        assert_eq!(reply[0], b'E', "the fast-fail is an ErrorResponse");
        assert!(reply.windows(7).any(|w| w == b"C08006\0"));
        assert_eq!(super::trailing_rfq_status(&reply), status);
    }
}

/// One PG-wire frame.
fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// A trust backend whose first connection closes right after answering
/// `BEGIN` (the session's transaction dies with it). Every connection logs
/// the simple queries and Parse texts it receives.
async fn dying_backend(log: std::sync::Arc<std::sync::Mutex<Vec<(u32, String)>>>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let mut pid = 0u32;
        while let Ok((mut sock, _)) = listener.accept().await {
            pid += 1;
            let log = std::sync::Arc::clone(&log);
            tokio::spawn(async move {
                loop {
                    let Ok(len) = sock.read_u32().await else {
                        return;
                    };
                    let mut body = vec![0u8; len as usize - 4];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    match u32::from_be_bytes(body[..4].try_into().unwrap()) {
                        80877103 => {
                            if sock.write_all(b"N").await.is_err() {
                                return;
                            }
                        }
                        80877102 => return,
                        _ => break,
                    }
                }
                let mut key = pid.to_be_bytes().to_vec();
                key.extend_from_slice(&(pid + 1000).to_be_bytes());
                let hello = [wire(b'R', &[0; 4]), wire(b'K', &key), wire(b'Z', b"I")].concat();
                if sock.write_all(&hello).await.is_err() {
                    return;
                }
                let mut status = b'I';
                loop {
                    let mut header = [0u8; 5];
                    if sock.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
                    let mut body = vec![0u8; len - 4];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let text = |s: &[u8]| String::from_utf8_lossy(s).into_owned();
                    let reply = match header[0] {
                        b'Q' => {
                            let sql = text(&body[..body.len() - 1]);
                            log.lock().unwrap().push((pid, sql.clone()));
                            let (tag, next) = match sql.to_ascii_uppercase().as_str() {
                                "BEGIN" => ("BEGIN\0", b'T'),
                                "COMMIT" => ("COMMIT\0", b'I'),
                                "ROLLBACK" => ("ROLLBACK\0", b'I'),
                                _ => ("SELECT 1\0", status),
                            };
                            status = next;
                            [wire(b'C', tag.as_bytes()), wire(b'Z', &[status])].concat()
                        }
                        b'P' => {
                            let sql = body.split(|b| *b == 0).nth(1).map(text);
                            log.lock().unwrap().push((pid, sql.unwrap_or_default()));
                            wire(b'1', &[])
                        }
                        b'B' => wire(b'2', &[]),
                        b'E' => wire(b'C', b"SELECT 1\0"),
                        b'S' => wire(b'Z', &[status]),
                        _ => return,
                    };
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    if pid == 1 && status == b'T' {
                        return;
                    }
                }
            });
        }
    });
    addr
}

/// The frames up to and including the next ReadyForQuery, as (tag, SQLSTATE
/// of an ErrorResponse, status of a ReadyForQuery).
async fn until_ready(s: &mut tokio::net::TcpStream) -> Vec<(u8, String)> {
    use tokio::io::AsyncReadExt;
    let mut out = Vec::new();
    loop {
        let mut header = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut header))
            .await
            .expect("a reply")
            .unwrap();
        let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body).await.unwrap();
        let detail = match header[0] {
            b'E' => body
                .split(|b| *b == 0)
                .find_map(|f| f.strip_prefix(b"C"))
                .map(|c| String::from_utf8_lossy(c).into_owned())
                .unwrap_or_default(),
            b'Z' => (body[0] as char).to_string(),
            _ => String::new(),
        };
        let done = header[0] == b'Z';
        out.push((header[0], detail));
        if done {
            return out;
        }
    }
}

/// A transaction end that the aborted-transaction emulation (after a failover
/// that could not continue the transaction) lets through, but an open circuit
/// refuses, is answered with the failed-transaction status 'E': the client is
/// still in its failed transaction, so the emulation must go on and refuse a
/// later statement with 25P02 rather than autocommit it on the new backend.
#[tokio::test]
async fn a_refused_transaction_end_keeps_the_aborted_transaction_emulation() {
    use tokio::io::AsyncWriteExt;

    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let backend = dying_backend(std::sync::Arc::clone(&log)).await;
    let mut config = super::test_config();
    config.nodes.clear();
    config.add_node(&backend, "primary").unwrap();
    config.circuit_breaker.enabled = true;
    config.limits.startup_timeout_secs = 3;
    let server = super::ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
    let (proxy, mut c) = super::pair().await;
    let peer = proxy.peer_addr().unwrap();
    let task = tokio::spawn(super::ProxyServer::handle_client(
        proxy,
        peer,
        state.clone(),
        std::sync::Arc::new(config.clone()),
        server.shutdown_tx.clone(),
    ));
    c.write_all(&super::startup_bytes("alice")).await.unwrap();
    until_ready(&mut c).await;
    let query = |sql: &str| wire(b'Q', format!("{sql}\0").as_bytes());
    let status = |frames: &[(u8, String)]| frames.last().unwrap().1.clone();

    c.write_all(&query("BEGIN")).await.unwrap();
    assert_eq!(status(&until_ready(&mut c).await), "T");
    // The backend took the transaction with it: the next statement fails and
    // the session enters the aborted-transaction emulation.
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.write_all(&query("SELECT 'lost'")).await.unwrap();
    let frames = until_ready(&mut c).await;
    assert_eq!(frames[0].0, b'E', "{frames:?}");
    assert_eq!(status(&frames), "E");

    // An extended ROLLBACK the open circuit refuses: 08006, still 'E'.
    let breaker = state
        .circuit_breaker
        .as_ref()
        .unwrap()
        .get_breaker(&backend);
    breaker.force_open(None);
    let rollback = [
        wire(b'P', b"\0ROLLBACK\0\0\0"),
        wire(b'B', &[0, 0, 0, 0, 0, 0, 0, 0]),
        wire(b'E', &[0, 0, 0, 0, 0]),
        wire(b'S', &[]),
    ]
    .concat();
    c.write_all(&rollback).await.unwrap();
    assert_eq!(
        until_ready(&mut c).await,
        vec![(b'E', "08006".to_string()), (b'Z', "E".to_string())]
    );
    breaker.force_close(None);

    // The client was told it is still in its failed transaction: a statement
    // is refused as PostgreSQL would, never run in autocommit.
    c.write_all(&query("SELECT 'after'")).await.unwrap();
    assert_eq!(
        until_ready(&mut c).await,
        vec![(b'E', "25P02".to_string()), (b'Z', "E".to_string())]
    );
    // Its transaction end still goes through.
    c.write_all(&query("ROLLBACK")).await.unwrap();
    assert_eq!(status(&until_ready(&mut c).await), "I");
    c.write_all(&query("SELECT 'next'")).await.unwrap();
    assert_eq!(status(&until_ready(&mut c).await), "I");
    let seen: Vec<String> = log.lock().unwrap().iter().map(|(_, s)| s.clone()).collect();
    assert!(!seen.iter().any(|s| s == "SELECT 'after'"), "{seen:?}");
    assert!(!seen.iter().any(|s| s == "ROLLBACK"), "{seen:?}");
    assert!(seen.iter().any(|s| s == "SELECT 'next'"), "{seen:?}");
    drop(c);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the session ends with its client")
        .unwrap()
        .unwrap();
}
