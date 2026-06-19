//! Test fixture: spawns a ProxyServer in-process and waits for it to accept connections.
//!
//! `start_proxy()` — single-node proxy; returns `None` when `HELIOS_TEST_PG_HOST` unset.
//! `start_proxy_ha()` — two-node (primary + standby) proxy; also needs `HELIOS_TEST_STANDBY_HOST`.

use heliosdb_proxy::config::{
    LoadBalancerConfig, NodeConfig, NodeRole, PoolConfig, ProxyConfig, Strategy,
};
use heliosdb_proxy::server::ProxyServer;
use std::net::TcpStream;
use std::time::{Duration, Instant};
use tokio::task::AbortHandle;

/// Which backend database engine is being proxied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum BackendKind {
    Postgres,
    Nano,
}

/// Connection coordinates for the backend (read from env vars).
#[derive(Debug, Clone)]
pub struct BackendInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    #[allow(dead_code)]
    pub kind: BackendKind,
}

/// Connection coordinates for the standby backend (read from HELIOS_TEST_STANDBY_* env vars).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StandbyInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

/// A running proxy instance scoped to one test.
///
/// Drop it to abort the background task.
pub struct ProxyFixture {
    pub proxy_port: u16,
    pub admin_port: u16,
    pub backend: BackendInfo,
    abort: AbortHandle,
}

impl Drop for ProxyFixture {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// A running two-node (primary + standby) proxy instance.
#[allow(dead_code)]
pub struct HaFixture {
    pub proxy_port: u16,
    pub admin_port: u16,
    pub primary: BackendInfo,
    pub standby: StandbyInfo,
    abort: AbortHandle,
}

impl Drop for HaFixture {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

/// Pick a free local TCP port by binding port 0, recording the assigned port,
/// then closing the listener so the caller can bind it next.
fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind port 0");
    listener.local_addr().expect("local_addr").port()
}

/// Read standby coordinates from HELIOS_TEST_STANDBY_* env vars.
fn read_standby_info() -> Option<StandbyInfo> {
    let host = std::env::var("HELIOS_TEST_STANDBY_HOST").ok()?;
    let port: u16 = std::env::var("HELIOS_TEST_STANDBY_PORT")
        .unwrap_or_else(|_| "5434".into())
        .parse()
        .expect("HELIOS_TEST_STANDBY_PORT must be a u16");
    let user = std::env::var("HELIOS_TEST_STANDBY_USER")
        .or_else(|_| std::env::var("HELIOS_TEST_PG_USER"))
        .unwrap_or_else(|_| "postgres".into());
    let password = std::env::var("HELIOS_TEST_STANDBY_PASSWORD")
        .or_else(|_| std::env::var("HELIOS_TEST_PG_PASSWORD"))
        .unwrap_or_default();
    let dbname = std::env::var("HELIOS_TEST_STANDBY_DB")
        .or_else(|_| std::env::var("HELIOS_TEST_PG_DB"))
        .unwrap_or_else(|_| "postgres".into());
    Some(StandbyInfo {
        host,
        port,
        user,
        password,
        dbname,
    })
}

/// Read backend info from environment variables.
fn read_backend_info() -> Option<BackendInfo> {
    let host = std::env::var("HELIOS_TEST_PG_HOST").ok()?;
    let port: u16 = std::env::var("HELIOS_TEST_PG_PORT")
        .unwrap_or_else(|_| "5432".into())
        .parse()
        .expect("HELIOS_TEST_PG_PORT must be a u16");
    let user = std::env::var("HELIOS_TEST_PG_USER").unwrap_or_else(|_| "postgres".into());
    let password = std::env::var("HELIOS_TEST_PG_PASSWORD").unwrap_or_default();
    let dbname = std::env::var("HELIOS_TEST_PG_DB").unwrap_or_else(|_| "postgres".into());
    let kind = match std::env::var("HELIOS_TEST_BACKEND")
        .unwrap_or_else(|_| "postgres".into())
        .as_str()
    {
        "nano" => BackendKind::Nano,
        _ => BackendKind::Postgres,
    };
    Some(BackendInfo {
        host,
        port,
        user,
        password,
        dbname,
        kind,
    })
}

/// Wait until `addr` accepts a TCP connection, up to `timeout`.
fn wait_for_tcp(addr: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let parsed: std::net::SocketAddr = addr.parse().expect("parse socket addr");
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&parsed, Duration::from_millis(100)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Spawn a proxy and wait for it to become ready.
///
/// Returns `None` if `HELIOS_TEST_PG_HOST` is not set.
pub async fn start_proxy() -> Option<ProxyFixture> {
    let backend = read_backend_info()?;

    let proxy_port = pick_free_port();
    let admin_port = pick_free_port();

    let config = ProxyConfig {
        listen_address: format!("127.0.0.1:{}", proxy_port),
        admin_address: format!("127.0.0.1:{}", admin_port),
        tr_enabled: false,
        nodes: vec![NodeConfig {
            host: backend.host.clone(),
            port: backend.port,
            http_port: 8080,
            role: NodeRole::Primary,
            weight: 100,
            enabled: true,
            name: Some("test-primary".to_string()),
        }],
        pool: PoolConfig {
            min_connections: 1,
            max_connections: 5,
            ..Default::default()
        },
        load_balancer: LoadBalancerConfig {
            read_strategy: Strategy::RoundRobin,
            read_write_split: false,
            latency_threshold_ms: 500,
        },
        // Disable optional subsystems that need real configuration
        ..Default::default()
    };

    let server = match ProxyServer::new(config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[fixture] ProxyServer::new failed: {e}");
            return None;
        }
    };

    let jh = tokio::task::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("[fixture] proxy run error: {e}");
        }
    });
    let abort = jh.abort_handle();

    // Wait up to 5 s for the proxy port to accept connections.
    let proxy_addr = format!("127.0.0.1:{}", proxy_port);
    if !wait_for_tcp(&proxy_addr, Duration::from_secs(5)) {
        eprintln!("[fixture] proxy did not start in time on {proxy_addr}");
        abort.abort();
        return None;
    }

    Some(ProxyFixture {
        proxy_port,
        admin_port,
        backend,
        abort,
    })
}

/// Spawn a two-node (primary + standby) proxy and wait for it to become ready.
///
/// Returns `None` if either `HELIOS_TEST_PG_HOST` or `HELIOS_TEST_STANDBY_HOST` is unset.
pub async fn start_proxy_ha() -> Option<HaFixture> {
    let primary = read_backend_info()?;
    let standby = read_standby_info()?;

    let proxy_port = pick_free_port();
    let admin_port = pick_free_port();

    let config = ProxyConfig {
        listen_address: format!("127.0.0.1:{}", proxy_port),
        admin_address: format!("127.0.0.1:{}", admin_port),
        tr_enabled: false,
        nodes: vec![
            NodeConfig {
                host: primary.host.clone(),
                port: primary.port,
                http_port: 8080,
                role: NodeRole::Primary,
                weight: 100,
                enabled: true,
                name: Some("test-primary".to_string()),
            },
            NodeConfig {
                host: standby.host.clone(),
                port: standby.port,
                http_port: 8081,
                role: NodeRole::Standby,
                weight: 100,
                enabled: true,
                name: Some("test-standby".to_string()),
            },
        ],
        pool: PoolConfig {
            min_connections: 1,
            max_connections: 5,
            ..Default::default()
        },
        load_balancer: LoadBalancerConfig {
            read_strategy: Strategy::RoundRobin,
            read_write_split: true,
            latency_threshold_ms: 500,
        },
        ..Default::default()
    };

    let server = match ProxyServer::new(config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[fixture-ha] ProxyServer::new failed: {e}");
            return None;
        }
    };

    let jh = tokio::task::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("[fixture-ha] proxy run error: {e}");
        }
    });
    let abort = jh.abort_handle();

    let proxy_addr = format!("127.0.0.1:{}", proxy_port);
    if !wait_for_tcp(&proxy_addr, Duration::from_secs(5)) {
        eprintln!("[fixture-ha] HA proxy did not start in time on {proxy_addr}");
        abort.abort();
        return None;
    }

    Some(HaFixture {
        proxy_port,
        admin_port,
        primary,
        standby,
        abort,
    })
}
