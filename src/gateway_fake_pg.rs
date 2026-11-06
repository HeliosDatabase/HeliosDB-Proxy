//! Test-only fake PostgreSQL backend for the gateway result-cap tests.
//!
//! It completes the startup handshake (AuthenticationOk, ReadyForQuery) and
//! answers every simple `Query` with `rows` one-column DataRows whose cells
//! are `cell_len` bytes, then CommandComplete and ReadyForQuery. The row
//! count is read per query, so a test changes it between requests while the
//! gateway reuses one pooled connection.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// A running fake backend. Dropping it stops the listener and its connections.
pub(crate) struct FakePg {
    pub(crate) port: u16,
    /// Rows the next query returns.
    pub(crate) rows: Arc<AtomicUsize>,
    /// Client connections accepted so far (one means the connection was reused).
    pub(crate) connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Drop for FakePg {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Start a fake backend on a free loopback port.
pub(crate) async fn spawn(rows: usize, cell_len: usize) -> FakePg {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rows = Arc::new(AtomicUsize::new(rows));
    let cell_len = Arc::new(AtomicUsize::new(cell_len));
    let connections = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(accept_loop(
        listener,
        rows.clone(),
        cell_len,
        connections.clone(),
    ));
    FakePg {
        port,
        rows,
        connections,
        task,
    }
}

async fn accept_loop(
    listener: TcpListener,
    rows: Arc<AtomicUsize>,
    cell_len: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        connections.fetch_add(1, Ordering::SeqCst);
        let (rows, cell_len) = (rows.clone(), cell_len.clone());
        tokio::spawn(async move {
            let _ = serve_connection(stream, &rows, &cell_len).await;
        });
    }
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

async fn serve_connection(
    mut stream: TcpStream,
    rows: &AtomicUsize,
    cell_len: &AtomicUsize,
) -> std::io::Result<()> {
    // StartupMessage: length (including itself), then the body, no tag.
    let len = stream.read_u32().await? as usize;
    let mut startup = vec![0u8; len.saturating_sub(4)];
    stream.read_exact(&mut startup).await?;
    stream.write_all(&frame(b'R', &0u32.to_be_bytes())).await?;
    stream.write_all(&frame(b'Z', b"I")).await?;
    loop {
        let tag = match stream.read_u8().await {
            Ok(t) => t,
            Err(_) => return Ok(()),
        };
        let len = stream.read_u32().await? as usize;
        let mut body = vec![0u8; len.saturating_sub(4)];
        stream.read_exact(&mut body).await?;
        match tag {
            b'X' => return Ok(()),
            b'Q' => {
                let n = rows.load(Ordering::SeqCst);
                let cell = vec![b'x'; cell_len.load(Ordering::SeqCst)];
                let mut out = Vec::new();
                // RowDescription: one text column named `v`.
                let mut desc = Vec::new();
                desc.extend_from_slice(&1u16.to_be_bytes());
                desc.extend_from_slice(b"v\0");
                desc.extend_from_slice(&0u32.to_be_bytes());
                desc.extend_from_slice(&0u16.to_be_bytes());
                desc.extend_from_slice(&25u32.to_be_bytes());
                desc.extend_from_slice(&(-1i16).to_be_bytes());
                desc.extend_from_slice(&(-1i32).to_be_bytes());
                desc.extend_from_slice(&0u16.to_be_bytes());
                out.extend_from_slice(&frame(b'T', &desc));
                for _ in 0..n {
                    let mut row = Vec::new();
                    row.extend_from_slice(&1u16.to_be_bytes());
                    row.extend_from_slice(&(cell.len() as u32).to_be_bytes());
                    row.extend_from_slice(&cell);
                    out.extend_from_slice(&frame(b'D', &row));
                }
                out.extend_from_slice(&frame(b'C', format!("SELECT {n}\0").as_bytes()));
                out.extend_from_slice(&frame(b'Z', b"I"));
                stream.write_all(&out).await?;
            }
            _ => {}
        }
    }
}
