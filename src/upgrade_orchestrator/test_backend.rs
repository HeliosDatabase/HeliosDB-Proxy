//! Test-only scripted PostgreSQL backend.
//!
//! Completes the startup handshake (AuthenticationOk, ReadyForQuery) and
//! answers each simple `Query` with the [`Reply`] its responder picks for
//! the query text: one text column of rows, a bare command tag, or an
//! ErrorResponse. Every query text is recorded in order, across all
//! connections, so a test can assert exactly what was sent.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};

/// What the fake answers to one query.
pub(crate) enum Reply {
    /// One text column named `v`; `None` cells are NULL.
    Rows(Vec<Option<String>>),
    /// No rows, just this command tag.
    Command(&'static str),
    /// An ErrorResponse with this message.
    Error(String),
}

type Responder = dyn Fn(&str) -> Reply + Send + Sync;

/// A running fake backend. Dropping it stops the listener and its
/// connections.
pub(crate) struct ScriptedPg {
    pub(crate) port: u16,
    /// Every query text received, in order.
    pub(crate) queries: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl Drop for ScriptedPg {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScriptedPg {
    /// Start a fake backend on a free loopback port.
    pub(crate) async fn spawn(respond: impl Fn(&str) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let queries = Arc::new(Mutex::new(Vec::new()));
        let respond: Arc<Responder> = Arc::new(respond);
        let recorded = queries.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (respond, recorded) = (respond.clone(), recorded.clone());
                tokio::spawn(async move {
                    let _ = serve(stream, &*respond, &recorded).await;
                });
            }
        });
        Self {
            port,
            queries,
            task,
        }
    }

    /// `127.0.0.1:<port>`.
    pub(crate) fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// A plaintext, password-less config pointing at this backend.
    pub(crate) fn config(&self) -> BackendConfig {
        BackendConfig {
            host: "127.0.0.1".into(),
            port: self.port,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: None,
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_secs(5),
            tls_config: default_client_config(),
        }
    }

    /// The queries received so far.
    pub(crate) fn seen(&self) -> Vec<String> {
        self.queries.lock().unwrap().clone()
    }
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn reply_bytes(reply: Reply) -> Vec<u8> {
    let mut out = Vec::new();
    match reply {
        Reply::Rows(rows) => {
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
            let n = rows.len();
            for cell in rows {
                let mut row = 1u16.to_be_bytes().to_vec();
                match cell {
                    Some(v) => {
                        row.extend_from_slice(&(v.len() as i32).to_be_bytes());
                        row.extend_from_slice(v.as_bytes());
                    }
                    None => row.extend_from_slice(&(-1i32).to_be_bytes()),
                }
                out.extend_from_slice(&frame(b'D', &row));
            }
            out.extend_from_slice(&frame(b'C', format!("SELECT {n}\0").as_bytes()));
            out.extend_from_slice(&frame(b'Z', b"I"));
        }
        Reply::Command(tag) => {
            out.extend_from_slice(&frame(b'C', format!("{tag}\0").as_bytes()));
            out.extend_from_slice(&frame(b'Z', b"I"));
        }
        Reply::Error(message) => {
            let mut body = Vec::new();
            body.extend_from_slice(b"SERROR\0CXX000\0M");
            body.extend_from_slice(message.as_bytes());
            body.extend_from_slice(b"\0\0");
            out.extend_from_slice(&frame(b'E', &body));
            out.extend_from_slice(&frame(b'Z', b"I"));
        }
    }
    out
}

async fn serve(
    mut stream: TcpStream,
    respond: &Responder,
    recorded: &Mutex<Vec<String>>,
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
                let text =
                    String::from_utf8_lossy(body.strip_suffix(b"\0").unwrap_or(&body)).into_owned();
                let reply = respond(&text);
                recorded.lock().unwrap().push(text);
                stream.write_all(&reply_bytes(reply)).await?;
            }
            _ => {}
        }
    }
}
