//! A scripted PostgreSQL backend on a loopback port for the pool benchmarks:
//! it accepts the startup handshake (trust authentication) and answers every
//! simple query with `CommandComplete` + `ReadyForQuery`. The pools under
//! benchmark open real connections to it, so acquire/validate/release (and a
//! transaction-mode reset) do the work they do against a database, minus the
//! database.

use heliosdb_proxy::backend::{tls::default_client_config, BackendConfig, TlsMode};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub struct FakePg {
    pub addr: SocketAddr,
}

impl FakePg {
    /// Start listening on a free loopback port; connections are served on
    /// the calling runtime until it shuts down.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let _ = sock.set_nodelay(true);
                tokio::spawn(async move {
                    let _ = serve(sock).await;
                });
            }
        });
        Self { addr }
    }

    /// A backend template for this listener.
    pub fn template(&self) -> BackendConfig {
        BackendConfig {
            host: self.addr.ip().to_string(),
            port: self.addr.port(),
            user: "bench".into(),
            password: None,
            database: None,
            application_name: Some("helios-pool-bench".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(2),
            query_timeout: Duration::from_secs(2),
            tls_config: default_client_config(),
        }
    }
}

async fn serve(mut sock: TcpStream) -> std::io::Result<()> {
    let len = sock.read_u32().await? as usize;
    let mut startup = vec![0u8; len.saturating_sub(4)];
    sock.read_exact(&mut startup).await?;
    // AuthenticationOk, ReadyForQuery (idle).
    sock.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0, b'Z', 0, 0, 0, 5, b'I'])
        .await?;
    loop {
        let tag = sock.read_u8().await?;
        let len = sock.read_u32().await? as usize;
        let mut body = vec![0u8; len.saturating_sub(4)];
        sock.read_exact(&mut body).await?;
        match tag {
            // CommandComplete "OK", ReadyForQuery (idle).
            b'Q' => {
                sock.write_all(&[b'C', 0, 0, 0, 7, b'O', b'K', 0, b'Z', 0, 0, 0, 5, b'I'])
                    .await?
            }
            b'X' => return Ok(()),
            _ => {}
        }
    }
}
