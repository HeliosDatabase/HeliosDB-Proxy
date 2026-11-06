//! Statements the proxy refuses locally (rate limit) inside an open
//! transaction never reach a backend, so neither the transaction replay log
//! nor a failover replay may contain them (fix(tr), d7350c9b1c93). A
//! Sync that continues a cycle a Flush already dispatched is never refused.

use crate::config::TrMode;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Every connection's (pid, text) log: the simple Query texts, the Parse
/// texts, and a `SYNC` marker per Sync it receives.
type Log = Arc<Mutex<Vec<(u32, String)>>>;

/// One PG-wire frame.
fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

fn query(sql: &str) -> Vec<u8> {
    wire(b'Q', &cstr(sql))
}

/// A Parse / Bind / Execute / Sync cycle (`Flush` replaces the Sync when
/// `sync` is false) running `sql` on the unnamed statement and portal.
fn extended_cycle(sql: &str, sync: bool) -> Vec<u8> {
    let mut p = vec![0u8];
    p.extend_from_slice(&cstr(sql));
    p.extend_from_slice(&[0, 0]);
    let bind = [0u8, 0, 0, 0, 0, 0, 0, 0];
    let execute = [0u8, 0, 0, 0, 0];
    [
        wire(b'P', &p),
        wire(b'B', &bind),
        wire(b'E', &execute),
        wire(if sync { b'S' } else { b'H' }, &[]),
    ]
    .concat()
}

/// The frames up to and including the next ReadyForQuery as (tag, detail):
/// the SQLSTATE of an ErrorResponse, the status of a ReadyForQuery.
async fn until_ready(s: &mut TcpStream) -> Vec<(u8, String)> {
    let mut out = Vec::new();
    loop {
        let mut header = [0u8; 5];
        match tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut header)).await {
            Ok(read) => {
                read.unwrap();
            }
            Err(_) => panic!("no reply; frames so far: {out:?}"),
        }
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

/// Serve one backend connection: a trust login, then simple and extended
/// messages. Connection 1 dies `LOSE_AFTER` after it answers `INSERT A`, so
/// the transaction it holds is lost while the client is idle and the proxy
/// fails over (replays the transaction) on the next request.
const LOSE_AFTER: Duration = Duration::from_millis(300);

async fn serve(mut sock: TcpStream, pid: u32, log: Log) {
    loop {
        let Ok(len) = sock.read_u32().await else {
            return;
        };
        let mut body = vec![0u8; len as usize - 4];
        if sock.read_exact(&mut body).await.is_err() {
            return;
        }
        eprintln!(
            "DBG pid {pid} startup-phase code {:?}",
            &body[..4.min(body.len())]
        );
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
        eprintln!(
            "DBG pid {pid} msg {} len {len} {:?}",
            header[0] as char,
            String::from_utf8_lossy(&body[..body.len().min(60)])
        );
        let mut close_after = false;
        let reply = match header[0] {
            b'Q' => {
                let sql = String::from_utf8_lossy(&body[..body.len() - 1]).into_owned();
                log.lock().unwrap().push((pid, sql.clone()));
                let (tag, next) = match sql.as_str() {
                    "BEGIN" => ("BEGIN", b'T'),
                    "COMMIT" => ("COMMIT", b'I'),
                    "ROLLBACK" => ("ROLLBACK", b'I'),
                    _ => ("SELECT 1", status),
                };
                status = next;
                close_after = pid == 1 && sql == "INSERT A";
                [wire(b'C', &cstr(tag)), wire(b'Z', &[status])].concat()
            }
            b'P' => {
                let sql = body.split(|b| *b == 0).nth(1).unwrap_or_default();
                log.lock()
                    .unwrap()
                    .push((pid, String::from_utf8_lossy(sql).into_owned()));
                wire(b'1', &[])
            }
            b'B' => wire(b'2', &[]),
            b'E' => wire(b'C', &cstr("SELECT 1")),
            b'H' => Vec::new(),
            b'S' => {
                log.lock().unwrap().push((pid, "SYNC".to_string()));
                wire(b'Z', &[status])
            }
            _ => return,
        };
        if !reply.is_empty() && sock.write_all(&reply).await.is_err() {
            return;
        }
        if close_after {
            tokio::time::sleep(LOSE_AFTER).await;
            return;
        }
    }
}

async fn lossy_backend(log: Log) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let mut pid = 0u32;
        while let Ok((sock, _)) = listener.accept().await {
            pid += 1;
            tokio::spawn(serve(sock, pid, Arc::clone(&log)));
        }
    });
    addr
}

/// A proxy session (user `alice`) with its client socket, already started.
async fn start(config: crate::config::ProxyConfig) -> TcpStream {
    let server = super::ProxyServer::new(config.clone()).unwrap();
    let state = server.state.clone();
    let (proxy, mut c) = super::pair().await;
    let peer = proxy.peer_addr().unwrap();
    tokio::spawn(super::ProxyServer::handle_client(
        proxy,
        peer,
        state,
        std::sync::Arc::new(config),
        server.shutdown_tx.clone(),
    ));
    c.write_all(&super::startup_bytes("alice")).await.unwrap();
    until_ready(&mut c).await;
    c
}

/// A transaction-replay proxy over one lossy backend whose bucket admits two
/// statements and then refuses until it refills (1 per second).
async fn limited_tr_config(log: &Log) -> crate::config::ProxyConfig {
    let backend = lossy_backend(Arc::clone(log)).await;
    let mut config = super::test_config();
    config.nodes.clear();
    config.add_node(&backend, "primary").unwrap();
    config.tr_enabled = true;
    config.tr_mode = TrMode::Transaction;
    config.rate_limit.enabled = true;
    config.rate_limit.default_qps = 1;
    config.rate_limit.default_burst = 2;
    config
}

/// Inside BEGIN, a statement the rate limiter refuses reaches no backend. The
/// transaction then loses its backend, and the failover replays what the
/// client was told succeeded (BEGIN, INSERT A) and never the refused one.
async fn refused_statement_is_not_replayed(extended: bool) {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let config = limited_tr_config(&log).await;
    let mut c = start(config).await;

    c.write_all(&query("BEGIN")).await.unwrap();
    assert_eq!(until_ready(&mut c).await.last().unwrap().1, "T");
    c.write_all(&query("INSERT A")).await.unwrap();
    assert_eq!(until_ready(&mut c).await.last().unwrap().1, "T");
    // The backend dies `LOSE_AFTER` from now, while the client is idle.

    // The bucket is empty: refused without a backend, inside the transaction.
    let refused = if extended {
        extended_cycle("INSERT B", true)
    } else {
        query("INSERT B")
    };
    c.write_all(&refused).await.unwrap();
    assert_eq!(
        until_ready(&mut c).await,
        vec![(b'E', "53400".to_string()), (b'Z', "T".to_string())]
    );

    // The lost backend is replaced (the transaction replayed) and one token
    // refills; the next statement runs on the replacement.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    c.write_all(&query("SELECT 'after'")).await.unwrap();
    let frames = until_ready(&mut c).await;
    assert!(!frames.iter().any(|(t, _)| *t == b'E'), "{frames:?}");
    assert_eq!(frames.last().unwrap().1, "T", "{frames:?}");

    let seen = log.lock().unwrap().clone();
    let replacement: Vec<&str> = seen
        .iter()
        .filter(|(pid, _)| *pid == 2)
        .map(|(_, s)| s.as_str())
        .collect();
    assert!(
        replacement.contains(&"BEGIN") && replacement.contains(&"INSERT A"),
        "the transaction was replayed: {seen:?}"
    );
    assert!(
        !seen.iter().any(|(_, s)| s == "INSERT B"),
        "a statement the proxy refused was recorded and replayed: {seen:?}"
    );
}

#[tokio::test]
async fn a_rate_limited_statement_in_a_transaction_is_not_replayed() {
    refused_statement_is_not_replayed(false).await;
}

#[tokio::test]
async fn a_rate_limited_extended_cycle_in_a_transaction_is_not_replayed() {
    refused_statement_is_not_replayed(true).await;
}

/// A Sync that continues a cycle a Flush dispatched is never refused by the
/// limiter (it is paced): the backend receives it, the client gets exactly
/// one ReadyForQuery and no error, and the next cycle works.
#[tokio::test]
async fn a_sync_closing_a_flushed_cycle_reaches_the_backend_when_limited() {
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let mut config = limited_tr_config(&log).await;
    config.rate_limit.default_burst = 1;
    let mut c = start(config).await;

    // The Flush spends the only token; the Sync that closes the cycle must
    // still reach the backend (paced, not refused).
    c.write_all(&extended_cycle("SELECT 1", false))
        .await
        .unwrap();
    c.write_all(&wire(b'S', &[])).await.unwrap();
    let frames = until_ready(&mut c).await;
    assert_eq!(
        frames.iter().filter(|(t, _)| *t == b'Z').count(),
        1,
        "{frames:?}"
    );
    assert!(!frames.iter().any(|(t, _)| *t == b'E'), "{frames:?}");
    assert_eq!(frames.last().unwrap().1, "I", "{frames:?}");
    assert!(
        log.lock().unwrap().iter().any(|(_, s)| s == "SYNC"),
        "the Sync reached the backend"
    );

    tokio::time::sleep(Duration::from_millis(1100)).await;
    c.write_all(&query("SELECT 'next'")).await.unwrap();
    assert_eq!(until_ready(&mut c).await.last().unwrap().1, "I");
}
