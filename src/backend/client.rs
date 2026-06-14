//! Backend PostgreSQL client.
//!
//! Originates simple-query SQL against a backend node. Frames every
//! message through the existing `crate::protocol` types so we keep a
//! single wire-protocol implementation in the crate.
//!
//! The code path here is distinct from `ProxyServer::route_and_forward`,
//! which *forwards* client frames. That path remains zero-copy;
//! this path is for **internal TR management** queries (health check,
//! `pg_is_in_recovery()`, `pg_promote()`, WAL-position probes, failover
//! replay, session-state restoration).

use super::auth::{md5_password_response, Scram};
use super::error::{BackendError, BackendResult};
use super::stream::Stream;
use super::tls::{negotiate, TlsMode};
use super::types::{encode_literal, ParamValue, TextValue};
use crate::protocol::{Message, MessageType, ProtocolCodec};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Backend connection parameters.
#[derive(Debug, Clone)]
pub struct BackendConfig {
    /// Hostname (also used for TLS SNI).
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// PostgreSQL user.
    pub user: String,
    /// PostgreSQL password. If `None`, only `AuthenticationOk` (no
    /// password required) is accepted.
    pub password: Option<String>,
    /// Target database. If `None`, connects to the user's default.
    pub database: Option<String>,
    /// Application name reported via the `application_name` startup
    /// parameter. Defaults to `heliosdb-proxy` when `None`.
    pub application_name: Option<String>,
    /// TLS policy.
    pub tls_mode: TlsMode,
    /// Connect-timeout ceiling (covers DNS + TCP + TLS + startup).
    pub connect_timeout: Duration,
    /// Per-query ceiling (round-trip from Query send to ReadyForQuery).
    pub query_timeout: Duration,
    /// Shared rustls `ClientConfig` — build once via
    /// `super::tls::default_client_config` and reuse across connections.
    pub tls_config: Arc<rustls::ClientConfig>,
}

impl BackendConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// An established, authenticated client connection to a backend.
pub struct BackendClient {
    stream: Stream,
    /// Parameter values the server sent during startup (client_encoding,
    /// server_version, TimeZone, …). Useful for diagnostics.
    pub server_parameters: std::collections::HashMap<String, String>,
    /// BackendKeyData cached for potential cancel requests.
    pub backend_pid: Option<u32>,
    pub backend_secret: Option<u32>,
}

impl BackendClient {
    /// Connect, TLS-negotiate, authenticate, and drain server-initialisation
    /// frames through `ReadyForQuery`. On success the client is idle and
    /// ready to run SQL.
    pub async fn connect(cfg: &BackendConfig) -> BackendResult<Self> {
        tokio::time::timeout(cfg.connect_timeout, Self::connect_inner(cfg))
            .await
            .map_err(|_| BackendError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("connect to {} exceeded {:?}", cfg.address(), cfg.connect_timeout),
            )))?
    }

    async fn connect_inner(cfg: &BackendConfig) -> BackendResult<Self> {
        let tcp = TcpStream::connect(cfg.address()).await?;
        let mut stream =
            negotiate(tcp, cfg.tls_mode, cfg.tls_config.clone(), &cfg.host).await?;

        // Send StartupMessage (protocol 3.0).
        let startup = build_startup(cfg);
        stream.write_all(&startup).await?;

        let mut server_parameters = std::collections::HashMap::new();
        let mut backend_pid = None;
        let mut backend_secret = None;
        let mut buffer = BytesMut::with_capacity(4096);
        let codec = ProtocolCodec::new();
        let mut scram_state: Option<Scram> = None;

        loop {
            let msg = read_one(&mut stream, &mut buffer, &codec).await?;
            match msg.msg_type {
                MessageType::AuthRequest => {
                    handle_auth(
                        &mut stream,
                        &msg,
                        cfg,
                        &mut scram_state,
                    )
                    .await?;
                }
                MessageType::ParameterStatus => {
                    if let Some((k, v)) = parse_parameter_status(&msg.payload) {
                        server_parameters.insert(k, v);
                    }
                }
                MessageType::BackendKeyData => {
                    if msg.payload.len() >= 8 {
                        backend_pid = Some(u32::from_be_bytes(
                            msg.payload[0..4].try_into().unwrap(),
                        ));
                        backend_secret = Some(u32::from_be_bytes(
                            msg.payload[4..8].try_into().unwrap(),
                        ));
                    }
                }
                MessageType::ReadyForQuery => {
                    return Ok(Self {
                        stream,
                        server_parameters,
                        backend_pid,
                        backend_secret,
                    });
                }
                MessageType::ErrorResponse => {
                    return Err(BackendError::BackendError(error_message(&msg.payload)));
                }
                MessageType::NoticeResponse => {
                    // Ignore warnings during startup.
                }
                other => {
                    return Err(BackendError::Protocol(format!(
                        "unexpected message during startup: {:?}",
                        other
                    )));
                }
            }
        }
    }

    /// Run a simple-query (SQL text) and collect every resulting row
    /// into a `QueryResult`. For statements that don't return rows
    /// (DDL, `SET`, etc.) `rows` will be empty and `command_tag`
    /// carries the completion string.
    pub async fn simple_query(&mut self, sql: &str) -> BackendResult<QueryResult> {
        self.run_query(sql).await
    }

    /// Like `simple_query` but substitutes `$1`, `$2`, … with
    /// text-format literals before sending. We stick to simple-query
    /// rather than the extended protocol to keep the surface narrow.
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: &[ParamValue],
    ) -> BackendResult<QueryResult> {
        let substituted = interpolate_params(sql, params)?;
        self.run_query(&substituted).await
    }

    /// Shorthand for a scalar lookup: runs `sql`, expects 1 column, 1 row.
    pub async fn query_scalar(&mut self, sql: &str) -> BackendResult<TextValue> {
        let res = self.simple_query(sql).await?;
        if res.rows.len() != 1 {
            return Err(BackendError::Protocol(format!(
                "expected 1 row, got {}",
                res.rows.len()
            )));
        }
        if res.columns.len() != 1 {
            return Err(BackendError::Protocol(format!(
                "expected 1 column, got {}",
                res.columns.len()
            )));
        }
        Ok(res.rows.into_iter().next().unwrap().into_iter().next().unwrap())
    }

    /// Run a statement with no result set (DDL, SET, DO). Returns the
    /// command tag (e.g. `"SET"`, `"CREATE TABLE"`).
    pub async fn execute(&mut self, sql: &str) -> BackendResult<String> {
        let res = self.simple_query(sql).await?;
        Ok(res.command_tag)
    }

    /// Bulk-load rows via `COPY <table> [(cols)] FROM STDIN` (text format).
    /// `data` is the pre-encoded COPY text payload (rows already tab-delimited,
    /// escaped, and newline-terminated; no trailing `\.`). Returns the row count
    /// from the `COPY n` command tag.
    ///
    /// On ANY failure the connection is drained back to `ReadyForQuery`, so a
    /// caller can fall back to per-row INSERTs cleanly — `COPY` is atomic, so a
    /// failed load leaves zero rows behind (no double-insert risk).
    pub async fn copy_in(&mut self, copy_sql: &str, data: &[u8]) -> BackendResult<u64> {
        // Bulk loads can run long; give COPY a generous ceiling vs the 30s
        // management-query default.
        let t = Duration::from_secs(600);
        tokio::time::timeout(t, Self::copy_in_inner(&mut self.stream, copy_sql, data))
            .await
            .map_err(|_| {
                BackendError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("COPY exceeded {:?}", t),
                ))
            })?
    }

    async fn copy_in_inner(stream: &mut Stream, copy_sql: &str, data: &[u8]) -> BackendResult<u64> {
        // 1. Send the COPY ... FROM STDIN command as a simple Query.
        let mut payload = BytesMut::with_capacity(copy_sql.len() + 1);
        payload.extend_from_slice(copy_sql.as_bytes());
        payload.put_u8(0);
        stream
            .write_all(&Message::new(MessageType::Query, payload).encode())
            .await?;

        let mut buffer = BytesMut::with_capacity(8192);
        let codec = ProtocolCodec::new();

        // 2. Expect CopyInResponse. Its tag 'G' isn't in the shared decoder, so
        //    it surfaces as Unknown(b'G'). An ErrorResponse here means the
        //    backend rejected COPY (e.g. unsupported) — drain to RFQ and surface
        //    a recoverable error so the caller falls back to INSERTs.
        loop {
            let msg = read_one(stream, &mut buffer, &codec).await?;
            match msg.msg_type {
                MessageType::Unknown(b'G') => break,
                MessageType::ErrorResponse => {
                    let e = error_message(&msg.payload);
                    Self::drain_to_ready(stream, &mut buffer, &codec).await?;
                    return Err(BackendError::BackendError(e));
                }
                MessageType::ReadyForQuery => {
                    return Err(BackendError::Protocol(
                        "COPY: ReadyForQuery before CopyInResponse".into(),
                    ));
                }
                _ => {} // NoticeResponse / ParameterStatus / etc — keep waiting
            }
        }

        // 3. Stream the payload as CopyData frames, then CopyDone.
        const CHUNK: usize = 64 * 1024;
        let mut off = 0;
        while off < data.len() {
            let end = (off + CHUNK).min(data.len());
            let mut p = BytesMut::with_capacity(end - off);
            p.extend_from_slice(&data[off..end]);
            stream
                .write_all(&Message::new(MessageType::CopyData, p).encode())
                .await?;
            off = end;
        }
        stream
            .write_all(&Message::new(MessageType::CopyDone, BytesMut::new()).encode())
            .await?;

        // 4. Read to ReadyForQuery: CommandComplete "COPY n" or ErrorResponse.
        let mut tag = String::new();
        let mut last_error = None;
        loop {
            let msg = read_one(stream, &mut buffer, &codec).await?;
            match msg.msg_type {
                MessageType::CommandComplete | MessageType::Close => {
                    tag = parse_cstring(&msg.payload);
                }
                MessageType::ErrorResponse => {
                    last_error = Some(error_message(&msg.payload));
                }
                MessageType::ReadyForQuery => {
                    if let Some(e) = last_error {
                        return Err(BackendError::BackendError(e));
                    }
                    // "COPY n" -> n
                    let n = tag
                        .rsplit(' ')
                        .next()
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    return Ok(n);
                }
                _ => {}
            }
        }
    }

    async fn drain_to_ready(
        stream: &mut Stream,
        buffer: &mut BytesMut,
        codec: &ProtocolCodec,
    ) -> BackendResult<()> {
        loop {
            if read_one(stream, buffer, codec).await?.msg_type == MessageType::ReadyForQuery {
                return Ok(());
            }
        }
    }

    async fn run_query(&mut self, sql: &str) -> BackendResult<QueryResult> {
        let t = self.stream_query_timeout();
        tokio::time::timeout(t, Self::run_query_inner(&mut self.stream, sql))
            .await
            .map_err(|_| BackendError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("query exceeded {:?}: {}", t, truncate(sql, 64)),
            )))?
    }

    fn stream_query_timeout(&self) -> Duration {
        // 30 seconds is a sane default for management queries; callers
        // that need longer can wrap their own timeout around this call.
        Duration::from_secs(30)
    }

    async fn run_query_inner(stream: &mut Stream, sql: &str) -> BackendResult<QueryResult> {
        // Build and send a Query message (tag 'Q', payload = SQL + \0).
        let mut payload = BytesMut::with_capacity(sql.len() + 1);
        payload.extend_from_slice(sql.as_bytes());
        payload.put_u8(0);
        let frame = Message::new(MessageType::Query, payload).encode();
        stream.write_all(&frame).await?;

        let mut buffer = BytesMut::with_capacity(8192);
        let codec = ProtocolCodec::new();
        let mut columns: Vec<ColumnMeta> = Vec::new();
        let mut rows: Vec<Vec<TextValue>> = Vec::new();
        let mut command_tag = String::new();
        let mut last_error: Option<String> = None;

        loop {
            let msg = read_one(stream, &mut buffer, &codec).await?;
            match msg.msg_type {
                // Both 'T' (RowDescription) and 'C' (CommandComplete)
                // may appear; from_tag conflates T with… nothing. It
                // DOES conflate D with Describe, but on a server→client
                // frame the D tag here is always DataRow in practice,
                // because we only arrive at run_query_inner after the
                // startup handshake and the server never sends Describe.
                MessageType::RowDescription => {
                    columns = parse_row_description(&msg.payload);
                }
                MessageType::DataRow => {
                    let row = parse_data_row(&msg.payload, columns.len())?;
                    rows.push(row);
                }
                // PG tag 'C' = CommandComplete (server → client). The
                // shared MessageType enum also has Close (client → server)
                // under the same tag; again, the direction fixes the
                // ambiguity at runtime.
                MessageType::CommandComplete | MessageType::Close => {
                    command_tag = parse_cstring(&msg.payload);
                }
                MessageType::EmptyQueryResponse => {
                    command_tag = String::new();
                }
                MessageType::ErrorResponse => {
                    last_error = Some(error_message(&msg.payload));
                }
                MessageType::NoticeResponse => {
                    tracing::debug!(notice = %error_message(&msg.payload), "backend notice");
                }
                MessageType::ReadyForQuery => {
                    if let Some(e) = last_error {
                        return Err(BackendError::BackendError(e));
                    }
                    return Ok(QueryResult {
                        columns,
                        rows,
                        command_tag,
                    });
                }
                MessageType::ParameterStatus => {
                    // Server may push parameter changes (e.g. after SET).
                    // Ignore here; callers that care can call
                    // simple_query("SHOW <param>") afterwards.
                }
                _other => {
                    // Unknown message kind — keep draining until
                    // ReadyForQuery. A well-behaved PG server won't
                    // send anything strange outside of the above set.
                }
            }
        }
    }

    /// Close the connection gracefully (send Terminate, close socket).
    pub async fn close(mut self) {
        let term = Message::new(MessageType::Terminate, BytesMut::new()).encode();
        let _ = self.stream.write_all(&term).await;
        let _ = self.stream.shutdown().await;
    }

    /// Report whether the underlying connection is over TLS.
    pub fn is_tls(&self) -> bool {
        self.stream.is_tls()
    }
}

// ---------------------------------------------------------------------
// Result shape
// ---------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub type_oid: u32,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<ColumnMeta>,
    pub rows: Vec<Vec<TextValue>>,
    pub command_tag: String,
}

impl QueryResult {
    /// Return the numeric suffix of a CommandComplete tag, if any.
    /// For example, `"INSERT 0 5"` → `Some(5)`.
    pub fn rows_affected(&self) -> Option<u64> {
        self.command_tag
            .split_whitespace()
            .last()
            .and_then(|s| s.parse().ok())
    }
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn build_startup(cfg: &BackendConfig) -> Vec<u8> {
    let mut payload = BytesMut::with_capacity(128);
    // Protocol version 3.0.
    payload.put_u32(196608);
    put_cstring(&mut payload, "user");
    put_cstring(&mut payload, &cfg.user);
    if let Some(db) = &cfg.database {
        put_cstring(&mut payload, "database");
        put_cstring(&mut payload, db);
    }
    put_cstring(&mut payload, "application_name");
    put_cstring(
        &mut payload,
        cfg.application_name
            .as_deref()
            .unwrap_or("heliosdb-proxy"),
    );
    put_cstring(&mut payload, "client_encoding");
    put_cstring(&mut payload, "UTF8");
    payload.put_u8(0); // terminator

    let mut framed = BytesMut::with_capacity(payload.len() + 4);
    framed.put_u32((payload.len() + 4) as u32);
    framed.extend_from_slice(&payload);
    framed.to_vec()
}

fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.put_u8(0);
}

fn parse_cstring(payload: &[u8]) -> String {
    let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
    String::from_utf8_lossy(&payload[..end]).into_owned()
}

fn parse_parameter_status(payload: &[u8]) -> Option<(String, String)> {
    let end1 = payload.iter().position(|&b| b == 0)?;
    let key = String::from_utf8_lossy(&payload[..end1]).into_owned();
    let rest = &payload[end1 + 1..];
    let end2 = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
    let value = String::from_utf8_lossy(&rest[..end2]).into_owned();
    Some((key, value))
}

fn parse_row_description(payload: &[u8]) -> Vec<ColumnMeta> {
    let mut p = BytesMut::from(payload);
    if p.remaining() < 2 {
        return Vec::new();
    }
    let n = p.get_u16() as usize;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        // cstring name
        let end = match p.as_ref().iter().position(|&b| b == 0) {
            Some(i) => i,
            None => break,
        };
        let name = String::from_utf8_lossy(&p.as_ref()[..end]).into_owned();
        p.advance(end + 1);
        if p.remaining() < 18 {
            break;
        }
        let _table_oid = p.get_u32();
        let _column_number = p.get_u16();
        let type_oid = p.get_u32();
        let _type_len = p.get_i16();
        let _type_mod = p.get_i32();
        let _format_code = p.get_u16();
        cols.push(ColumnMeta { name, type_oid });
    }
    cols
}

fn parse_data_row(payload: &[u8], column_count: usize) -> BackendResult<Vec<TextValue>> {
    let mut p = BytesMut::from(payload);
    if p.remaining() < 2 {
        return Err(BackendError::Protocol("truncated DataRow".into()));
    }
    let n = p.get_u16() as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if p.remaining() < 4 {
            return Err(BackendError::Protocol("truncated DataRow field".into()));
        }
        let len = p.get_i32();
        if len == -1 {
            out.push(TextValue::Null);
        } else {
            let len = len as usize;
            if p.remaining() < len {
                return Err(BackendError::Protocol(
                    "truncated DataRow value".into(),
                ));
            }
            let bytes = p.split_to(len);
            out.push(TextValue::Text(
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }
    }
    let _ = column_count;
    Ok(out)
}

fn error_message(payload: &[u8]) -> String {
    // ErrorResponse fields: { code:u8; cstring }*, terminated by code=0.
    // The M (message) field is mandatory.
    let mut i = 0;
    let mut msg_field = None;
    while i < payload.len() {
        let code = payload[i];
        if code == 0 {
            break;
        }
        i += 1;
        let end = match payload[i..].iter().position(|&b| b == 0) {
            Some(e) => i + e,
            None => payload.len(),
        };
        let value = String::from_utf8_lossy(&payload[i..end]).into_owned();
        if code == b'M' {
            msg_field = Some(value);
        }
        i = end + 1;
    }
    msg_field.unwrap_or_else(|| "<no message>".to_string())
}

async fn read_one(
    stream: &mut Stream,
    buffer: &mut BytesMut,
    codec: &ProtocolCodec,
) -> BackendResult<Message> {
    loop {
        if let Some(mut msg) = codec
            .decode_message(buffer)
            .map_err(|e| BackendError::Protocol(e.to_string()))?
        {
            // The shared tag decoder is direction-agnostic and resolves the
            // tags that collide between client and server frames to their
            // CLIENT-side meaning. `read_one` only ever reads SERVER frames,
            // so remap those collisions to their server semantics:
            //   'S' Sync->ParameterStatus, 'D' Describe->DataRow,
            //   'E' Execute->ErrorResponse, 'C' Close->CommandComplete.
            msg.msg_type = match msg.msg_type {
                MessageType::Sync => MessageType::ParameterStatus,
                MessageType::Describe => MessageType::DataRow,
                MessageType::Execute => MessageType::ErrorResponse,
                MessageType::Close => MessageType::CommandComplete,
                other => other,
            };
            return Ok(msg);
        }
        let mut tmp = vec![0u8; 4096];
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Err(BackendError::Closed);
        }
        buffer.extend_from_slice(&tmp[..n]);
    }
}

async fn handle_auth(
    stream: &mut Stream,
    msg: &Message,
    cfg: &BackendConfig,
    scram_state: &mut Option<Scram>,
) -> BackendResult<()> {
    if msg.payload.len() < 4 {
        return Err(BackendError::Protocol(
            "AuthRequest payload < 4 bytes".into(),
        ));
    }
    let code =
        u32::from_be_bytes([msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3]]);
    match code {
        0 => Ok(()), // AuthenticationOk
        5 => {
            // AuthenticationMD5Password + 4-byte salt.
            if msg.payload.len() < 8 {
                return Err(BackendError::Protocol("AuthenticationMD5 truncated".into()));
            }
            let salt: [u8; 4] = [
                msg.payload[4],
                msg.payload[5],
                msg.payload[6],
                msg.payload[7],
            ];
            let password = cfg.password.as_deref().ok_or_else(|| {
                BackendError::Auth("server requested MD5 but no password configured".into())
            })?;
            let payload = md5_password_response(&cfg.user, password, &salt);
            write_password_message(stream, &payload).await
        }
        3 => {
            // AuthenticationCleartextPassword — plain password with null terminator.
            let password = cfg.password.as_deref().ok_or_else(|| {
                BackendError::Auth("server requested password but none configured".into())
            })?;
            let mut payload = Vec::with_capacity(password.len() + 1);
            payload.extend_from_slice(password.as_bytes());
            payload.push(0);
            write_password_message(stream, &payload).await
        }
        10 => {
            // AuthenticationSASL: list of mechanism cstrings, then 0.
            let mechs = parse_sasl_mechanisms(&msg.payload[4..]);
            if !mechs.iter().any(|m| m == "SCRAM-SHA-256") {
                return Err(BackendError::Auth(format!(
                    "no supported SASL mechanism; server offered {:?}",
                    mechs
                )));
            }
            let nonce = generate_nonce();
            let (scram, first) = Scram::client_first(nonce);
            *scram_state = Some(scram);
            write_password_message(stream, &first.0).await
        }
        11 => {
            // AuthenticationSASLContinue: server-first bytes.
            let scram = scram_state.as_mut().ok_or_else(|| {
                BackendError::Auth("SASLContinue before SASL start".into())
            })?;
            let password = cfg.password.as_deref().ok_or_else(|| {
                BackendError::Auth("SCRAM requires a password".into())
            })?;
            let out = scram.client_final(&msg.payload[4..], password)?;
            write_password_message(stream, &out.0).await
        }
        12 => {
            // AuthenticationSASLFinal: v=<signature>
            let scram = scram_state.as_ref().ok_or_else(|| {
                BackendError::Auth("SASLFinal before SASL start".into())
            })?;
            scram.verify_server(&msg.payload[4..])
        }
        other => Err(BackendError::Auth(format!(
            "unsupported authentication request code: {}",
            other
        ))),
    }
}

async fn write_password_message(
    stream: &mut Stream,
    payload: &[u8],
) -> BackendResult<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 5);
    buf.put_u8(b'p');
    buf.put_u32((payload.len() + 4) as u32);
    buf.extend_from_slice(payload);
    stream.write_all(&buf).await?;
    Ok(())
}

fn parse_sasl_mechanisms(payload: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < payload.len() {
        let end = match payload[i..].iter().position(|&b| b == 0) {
            Some(e) => i + e,
            None => payload.len(),
        };
        if end == i {
            break; // list terminator
        }
        out.push(String::from_utf8_lossy(&payload[i..end]).into_owned());
        i = end + 1;
    }
    out
}

fn generate_nonce() -> String {
    use base64::Engine as _;
    use rand::RngCore;
    let mut bytes = [0u8; 18];
    rand::thread_rng().fill_bytes(&mut bytes);
    // URL-safe base64 without padding keeps it ASCII.
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn interpolate_params(sql: &str, params: &[ParamValue]) -> BackendResult<String> {
    // Walk the SQL, replacing $N tokens outside of string literals with
    // the encoded parameter. This is a deliberately simple interpolator
    // for internal management queries; it does NOT try to be a full
    // PG parser.
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    let mut quote_char = 0u8;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            out.push(b as char);
            if b == quote_char {
                // PG doubles the quote to escape; peek ahead.
                if i + 1 < bytes.len() && bytes[i + 1] == quote_char {
                    out.push(quote_char as char);
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            in_string = true;
            quote_char = b;
            out.push(b as char);
            i += 1;
            continue;
        }
        if b == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            let idx: usize = std::str::from_utf8(&bytes[i + 1..j])
                .unwrap()
                .parse()
                .map_err(|_| {
                    BackendError::Protocol(format!(
                        "invalid parameter reference at byte {}",
                        i
                    ))
                })?;
            if idx == 0 || idx > params.len() {
                return Err(BackendError::Protocol(format!(
                    "parameter ${} out of range (have {})",
                    idx,
                    params.len()
                )));
            }
            out.push_str(&encode_literal(&params[idx - 1]));
            i = j;
            continue;
        }
        out.push(b as char);
        i += 1;
    }
    Ok(out)
}

fn truncate(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::types::ParamValue;

    #[test]
    fn test_build_startup_has_user_and_protocol_version() {
        let cfg = BackendConfig {
            host: "localhost".into(),
            port: 5432,
            user: "alice".into(),
            password: None,
            database: Some("app".into()),
            application_name: None,
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_secs(5),
            tls_config: crate::backend::tls::default_client_config(),
        };
        let bytes = build_startup(&cfg);
        // First 4 bytes: length, next 4: protocol version 196608 (3 << 16).
        assert_eq!(
            u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            196608
        );
        assert!(bytes
            .windows(5)
            .any(|w| w == b"user\0"));
        assert!(bytes
            .windows(10)
            .any(|w| w == b"database\0a"));
    }

    #[test]
    fn test_interpolate_params_basic() {
        let params = vec![
            ParamValue::Int(42),
            ParamValue::Text("alice".into()),
        ];
        let sql = "SELECT * FROM t WHERE id = $1 AND name = $2";
        let out = interpolate_params(sql, &params).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE id = 42 AND name = 'alice'");
    }

    #[test]
    fn test_interpolate_params_escapes_quotes() {
        let params = vec![ParamValue::Text("o'brien".into())];
        let out =
            interpolate_params("SELECT * FROM t WHERE name = $1", &params).unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE name = 'o''brien'");
    }

    #[test]
    fn test_interpolate_params_leaves_dollar_in_string_alone() {
        let params = vec![ParamValue::Int(1)];
        let sql = "SELECT '$1' AS lit, $1 AS val";
        let out = interpolate_params(sql, &params).unwrap();
        assert_eq!(out, "SELECT '$1' AS lit, 1 AS val");
    }

    #[test]
    fn test_interpolate_params_out_of_range() {
        let params = vec![ParamValue::Int(1)];
        let err = interpolate_params("SELECT $2", &params).unwrap_err();
        assert!(matches!(err, BackendError::Protocol(_)));
    }

    #[test]
    fn test_parse_row_description_shape() {
        // numFields=1, name="x"\0, tableOid=0, col#=0, typeOid=23, len=4, mod=-1, fmt=0
        let mut p = BytesMut::new();
        p.put_u16(1);
        p.extend_from_slice(b"x");
        p.put_u8(0);
        p.put_u32(0); // table oid
        p.put_u16(0); // col #
        p.put_u32(23); // int4
        p.put_i16(4);
        p.put_i32(-1);
        p.put_u16(0);
        let cols = parse_row_description(&p);
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "x");
        assert_eq!(cols[0].type_oid, 23);
    }

    #[test]
    fn test_parse_data_row_with_null() {
        // numFields=2, len=1/'a', len=-1 (NULL)
        let mut p = BytesMut::new();
        p.put_u16(2);
        p.put_i32(1);
        p.extend_from_slice(b"a");
        p.put_i32(-1);
        let row = parse_data_row(&p, 2).unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], TextValue::Text("a".into()));
        assert_eq!(row[1], TextValue::Null);
    }

    #[test]
    fn test_error_message_extracts_m_field() {
        let mut p = Vec::new();
        p.push(b'S');
        p.extend_from_slice(b"ERROR\0");
        p.push(b'C');
        p.extend_from_slice(b"28P01\0");
        p.push(b'M');
        p.extend_from_slice(b"password authentication failed\0");
        p.push(0);
        assert_eq!(error_message(&p), "password authentication failed");
    }

    #[test]
    fn test_parse_parameter_status() {
        let mut p = Vec::new();
        p.extend_from_slice(b"client_encoding\0");
        p.extend_from_slice(b"UTF8\0");
        let (k, v) = parse_parameter_status(&p).unwrap();
        assert_eq!(k, "client_encoding");
        assert_eq!(v, "UTF8");
    }

    #[test]
    fn test_parse_sasl_mechanisms() {
        let mut p = Vec::new();
        p.extend_from_slice(b"SCRAM-SHA-256\0");
        p.extend_from_slice(b"SCRAM-SHA-256-PLUS\0");
        p.push(0);
        let m = parse_sasl_mechanisms(&p);
        assert_eq!(m.len(), 2);
        assert_eq!(m[0], "SCRAM-SHA-256");
        assert_eq!(m[1], "SCRAM-SHA-256-PLUS");
    }

    #[test]
    fn test_generate_nonce_is_url_safe() {
        let n = generate_nonce();
        assert!(n.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert!(n.len() >= 18);
    }

    #[test]
    fn test_query_result_rows_affected() {
        let r = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "INSERT 0 5".into(),
        };
        assert_eq!(r.rows_affected(), Some(5));
        let r = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "SET".into(),
        };
        assert_eq!(r.rows_affected(), None);
    }
}
