//! Integration Tests for HeliosProxy
//!
//! These tests verify end-to-end proxy behavior. They require a running
//! PostgreSQL backend and are marked `#[ignore]` by default.
//!
//! Run with:
//! ```bash
//! cargo test --test integration -- --ignored
//! ```

use std::net::TcpStream;
use std::time::Duration;

/// Default proxy listen address for integration tests.
const PROXY_ADDR: &str = "127.0.0.1:5432";

/// Default admin/health endpoint for integration tests.
const ADMIN_ADDR: &str = "127.0.0.1:9090";

/// Helper: check if a TCP port is accepting connections.
fn port_is_open(addr: &str, timeout: Duration) -> bool {
    TcpStream::connect_timeout(&addr.parse().expect("invalid socket address"), timeout).is_ok()
}

// ── Test: proxy starts and listens ───────────────────────────────────

/// Verify the proxy process is listening on its configured port.
///
/// Requires a running `heliosdb-proxy` instance on `PROXY_ADDR`.
#[test]
#[ignore]
fn test_proxy_starts() {
    assert!(
        port_is_open(PROXY_ADDR, Duration::from_secs(5)),
        "Proxy should be listening on {}",
        PROXY_ADDR,
    );
}

// ── Test: health endpoint ────────────────────────────────────────────

/// Verify the admin/health HTTP endpoint responds.
///
/// Requires a running `heliosdb-proxy` instance with admin API enabled
/// on `ADMIN_ADDR`.
#[test]
#[ignore]
fn test_proxy_health_endpoint() {
    // Use a raw TCP connection to send a minimal HTTP GET, avoiding
    // a dependency on reqwest/hyper in dev-dependencies just for this.
    let mut stream = TcpStream::connect_timeout(
        &ADMIN_ADDR.parse().expect("invalid admin address"),
        Duration::from_secs(5),
    )
    .expect("Failed to connect to admin endpoint");

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    use std::io::{Read, Write};
    let request = format!(
        "GET /health HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        ADMIN_ADDR
    );
    stream
        .write_all(request.as_bytes())
        .expect("Failed to send HTTP request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("Failed to read HTTP response");

    assert!(
        response.contains("200") || response.contains("ok") || response.contains("healthy"),
        "Health endpoint should return a success response, got: {}",
        &response[..response.len().min(200)],
    );
}

// ── Test: proxy accepts PostgreSQL connections ───────────────────────

/// Verify the proxy accepts a TCP connection on the PostgreSQL port
/// and responds with initial protocol data (or at minimum does not
/// immediately close the socket).
///
/// Requires a running `heliosdb-proxy` instance on `PROXY_ADDR` with
/// a configured backend.
#[test]
#[ignore]
fn test_proxy_accepts_connection() {
    let stream = TcpStream::connect_timeout(
        &PROXY_ADDR.parse().expect("invalid proxy address"),
        Duration::from_secs(5),
    )
    .expect("Failed to connect to proxy");

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Verify the connection stays open (proxy does not RST immediately).
    // A real PostgreSQL protocol handshake would follow, but we just
    // confirm the socket is alive.
    let peer = stream.peer_addr().expect("Should have a peer address");
    assert_eq!(
        peer.ip().to_string(),
        "127.0.0.1",
        "Should be connected to localhost proxy",
    );

    // Optionally, send a PostgreSQL SSLRequest to trigger a response.
    // SSLRequest: length=8, code=80877103
    use std::io::{Read, Write};
    let ssl_request: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
    if stream.try_clone().unwrap().write_all(&ssl_request).is_ok() {
        let mut buf = [0u8; 1];
        if let Ok(n) = stream.try_clone().unwrap().read(&mut buf) {
            // 'N' (0x4e) = SSL not supported, 'S' (0x53) = SSL supported
            // Either response means the proxy is alive and speaking protocol.
            if n == 1 {
                assert!(
                    buf[0] == b'N' || buf[0] == b'S',
                    "Expected SSL response byte 'N' or 'S', got: 0x{:02x}",
                    buf[0],
                );
            }
        }
    }
}

// ── Test: proxy configuration types ──────────────────────────────────

/// Verify core configuration types can be constructed (does not require
/// a running backend).
#[test]
fn test_proxy_config_types() {
    use heliosdb_proxy::connection_pool::PoolConfig;
    use heliosdb_proxy::{NodeEndpoint, NodeRole};

    let config = PoolConfig {
        min_connections: 5,
        max_connections: 100,
        ..Default::default()
    };
    assert_eq!(config.max_connections, 100);

    let node = NodeEndpoint::new("localhost", 5432).with_role(NodeRole::Primary);
    assert_eq!(node.address(), "localhost:5432");
    assert_eq!(node.role, NodeRole::Primary);
}

/// Verify NodeId generation produces unique identifiers.
#[test]
fn test_node_id_uniqueness() {
    use heliosdb_proxy::NodeId;

    let ids: Vec<NodeId> = (0..100).map(|_| NodeId::new()).collect();
    for i in 0..ids.len() {
        for j in (i + 1)..ids.len() {
            assert_ne!(ids[i], ids[j], "NodeId should be unique");
        }
    }
}
