//! ac858d61400b: per-tenant active-query budgets through the whole client
//! handler (`handle_client`), against an in-process trust backend that logs
//! every frame it receives.
use super::*;
use crate::tenant_slots::TenantBudgetStats;
use std::sync::Mutex as StdMutex;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// A startup packet naming `user` and `database` (protocol 3.0).
fn startup_bytes_db(user: &str, database: &str) -> Vec<u8> {
    let mut params = Vec::new();
    for (k, v) in [("user", user), ("database", database)] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0);
    let mut out = Vec::new();
    out.extend_from_slice(&((8 + params.len()) as u32).to_be_bytes());
    out.extend_from_slice(&196608u32.to_be_bytes());
    out.extend_from_slice(&params);
    out
}

fn wire(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn cstring(s: &str) -> Vec<u8> {
    let mut out = s.as_bytes().to_vec();
    out.push(0);
    out
}

fn text_row_description(names: &[&str]) -> Vec<u8> {
    let mut body = (names.len() as u16).to_be_bytes().to_vec();
    for name in names {
        body.extend(cstring(name));
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&25u32.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
    }
    wire(b'T', &body)
}

fn text_data_row(values: &[&str]) -> Vec<u8> {
    let mut body = (values.len() as u16).to_be_bytes().to_vec();
    for value in values {
        body.extend_from_slice(&(value.len() as u32).to_be_bytes());
        body.extend_from_slice(value.as_bytes());
    }
    wire(b'D', &body)
}

/// A real, bounded settings exchange, consumed by the production scope parser.
/// No cache-scope override is installed on the connection.
fn show_all_reply(status: u8) -> Vec<u8> {
    let mut reply = text_row_description(&["name", "setting", "description"]);
    for (name, value) in [
        ("search_path", "\"$user\", public"),
        ("row_security", "on"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("server_version", "18.4"),
    ] {
        reply.extend(text_data_row(&[name, value, "tenant fixture setting"]));
    }
    reply.extend(wire(b'C', b"SHOW\0"));
    reply.extend(wire(b'Z', &[status]));
    reply
}

fn query(sql: &str) -> Vec<u8> {
    wire(b'Q', &cstring(sql))
}

fn parse(name: &str, sql: &str) -> Vec<u8> {
    wire(b'P', &[cstring(name), cstring(sql), vec![0, 0]].concat())
}

fn bind(stmt: &str) -> Vec<u8> {
    wire(b'B', &[cstring(""), cstring(stmt), vec![0; 6]].concat())
}

fn execute() -> Vec<u8> {
    wire(b'E', &[cstring(""), vec![0; 4]].concat())
}

fn sync() -> Vec<u8> {
    wire(b'S', &[])
}

fn flush() -> Vec<u8> {
    wire(b'H', &[])
}

/// Describe of the statement `name`.
fn describe_statement(name: &str) -> Vec<u8> {
    wire(b'D', &[&b"S"[..], &cstring(name)].concat())
}

/// The SQLSTATE (`C` field) of an `ErrorResponse` frame.
fn sqlstate(frame: &[u8]) -> String {
    assert_eq!(frame[0], b'E', "not an ErrorResponse: {frame:?}");
    frame[5..]
        .split(|b| *b == 0)
        .find_map(|f| f.strip_prefix(b"C"))
        .map(|c| String::from_utf8_lossy(c).into_owned())
        .unwrap_or_default()
}

fn has_text(frame: &[u8], text: &str) -> bool {
    frame.windows(text.len()).any(|w| w == text.as_bytes())
}

/// Tag of the log entry noting the moment the fake backend answered a
/// `DISCARD ALL` (the pool's reset).
const RESET_ANSWERED: u8 = b'~';

/// One frame the fake backend received (or, with tag [`RESET_ANSWERED`], the
/// moment it answered a `DISCARD ALL`; with tag `!`, a CancelRequest
/// connection, its text `cancel <pid> <secret>` naming the key it carried;
/// the fake closes that connection once it noted it, as PostgreSQL does,
/// unless `BackendCtl::stall_cancels`).
#[derive(Clone, Debug)]
struct Seen {
    conn: u32,
    tag: u8,
    text: String,
    at: std::time::Instant,
}

struct BackendCtl {
    log: StdMutex<Vec<Seen>>,
    /// Permits that let a held query or Execute (its SQL contains `slow`)
    /// answer.
    slow: Semaphore,
    /// Withhold a settings proof until the test explicitly releases it.
    hold_scope_probes: std::sync::atomic::AtomicBool,
    scope_probes: Semaphore,
    /// Per connection: closes it from the backend side.
    kills: StdMutex<HashMap<u32, Arc<Notify>>>,
    /// Delay before answering a `DISCARD ALL` (the pool's reset).
    discard_delay: Option<Duration>,
    /// The BackendKeyData every connection reports, instead of its own
    /// (pid = connection number, key = pid + 1000): a backend issuing
    /// colliding keys.
    shared_key: Option<(u32, u32)>,
    /// A CancelRequest connection is noted, then never closed: the cancel
    /// of a backend host that does not answer lasts its whole `[limits]
    /// backend_cancel_timeout_ms`.
    stall_cancels: std::sync::atomic::AtomicBool,
    /// The `database` parameter of each startup packet, in order.
    startup_databases: StdMutex<Vec<String>>,
    /// Every parameter of each startup packet, in order.
    startup_params: StdMutex<Vec<HashMap<String, String>>>,
    /// A killed connection is reset (RST) instead of closed (FIN).
    reset_on_kill: std::sync::atomic::AtomicBool,
}

/// What a fake backend connection is doing besides answering messages.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BackendMode {
    Ready,
    /// COPY FROM STDIN started by a simple `Query`: CopyDone gets
    /// CommandComplete + ReadyForQuery.
    CopySimple,
    /// COPY FROM STDIN started by an `Execute`: Sync and Flush are ignored
    /// (as PostgreSQL does), CopyDone gets CommandComplete alone (the
    /// client's next Sync gets the ReadyForQuery).
    CopyExtended,
    /// After an extended-protocol error (a CopyFail, a failed Execute):
    /// everything up to Sync is discarded.
    SkipToSync,
}

impl BackendCtl {
    fn note(&self, conn: u32, tag: u8, text: &str) {
        self.log.lock().unwrap().push(Seen {
            conn,
            tag,
            text: text.to_string(),
            at: std::time::Instant::now(),
        });
    }
}

/// The status a statement leaves: BEGIN opens a transaction block, its end
/// closes it.
fn status_after(sql: &str, status: u8) -> u8 {
    // Statement by statement, as PostgreSQL runs a multi-statement query.
    sql.split(';').fold(status, |status, statement| {
        let upper = statement.trim().to_ascii_uppercase();
        if upper.starts_with("BEGIN") {
            b'T'
        } else if upper.contains("PREPARED") {
            status
        } else if upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("END")
        {
            b'I'
        } else {
            status
        }
    })
}

/// A trust backend: AuthenticationOk + BackendKeyData (pid = connection
/// number, key = pid + 1000) + ReadyForQuery, then simple queries get
/// `CommandComplete` + `ReadyForQuery` (tracking BEGIN/COMMIT), a
/// `COPY ... FROM STDIN` gets `CopyInResponse` and is answered at its
/// `CopyDone` (one of `missing_table` fails at once, 42P01), and the
/// extended protocol gets 1/2/C/Z (nothing for Flush),
/// with the same transaction tracking and an extended-protocol COPY. As in
/// PostgreSQL, a simple Query destroys the unnamed statement and portal, and
/// a portal ends with its transaction. A Bind of a statement the connection
/// does not hold (never Parsed, Closed, or destroyed) fails (26000), as does
/// an Execute with no portal (34000), and the backend then skips everything
/// up to the Sync. An
/// Execute of a statement containing `violates` fails (23505), and the
/// backend then skips everything up to the Sync. A query, or an Execute,
/// whose SQL contains `slow` is answered only once `release_slow` allows it;
/// one whose SQL contains `crash` closes its connection without an answer
/// (the backend dies while it is in flight). An Execute whose SQL contains
/// `hang`, or the end of such an extended COPY, is never answered: the
/// connection waits for its `kill` (and, with `terminate` in that SQL too,
/// sends FATAL 57P01 before it closes). In copy-in, Sync and Flush are ignored
/// and any other message but CopyData/CopyDone/CopyFail fails the COPY
/// (08P01) without running, as PostgreSQL does: then a COPY a Query started
/// answers ReadyForQuery, one an Execute started skips to the Sync.
struct FakeBackend {
    addr: SocketAddr,
    ctl: Arc<BackendCtl>,
}

impl FakeBackend {
    async fn start(discard_delay: Option<Duration>) -> Self {
        Self::launch(discard_delay, None).await
    }

    /// The `database` each connection asked for at startup, in order.
    fn startup_databases(&self) -> Vec<String> {
        self.ctl.startup_databases.lock().unwrap().clone()
    }

    /// The parameters of each connection's startup packet, in order.
    fn startup_params(&self) -> Vec<HashMap<String, String>> {
        self.ctl.startup_params.lock().unwrap().clone()
    }

    /// Every connection reports the BackendKeyData `key`.
    async fn start_shared_key(key: (u32, u32)) -> Self {
        Self::launch(None, Some(key)).await
    }

    async fn launch(discard_delay: Option<Duration>, shared_key: Option<(u32, u32)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ctl = Arc::new(BackendCtl {
            log: StdMutex::new(Vec::new()),
            slow: Semaphore::new(0),
            hold_scope_probes: std::sync::atomic::AtomicBool::new(false),
            scope_probes: Semaphore::new(0),
            kills: StdMutex::new(HashMap::new()),
            discard_delay,
            shared_key,
            stall_cancels: std::sync::atomic::AtomicBool::new(false),
            startup_databases: StdMutex::new(Vec::new()),
            startup_params: StdMutex::new(Vec::new()),
            reset_on_kill: std::sync::atomic::AtomicBool::new(false),
        });
        let accept_ctl = Arc::clone(&ctl);
        tokio::spawn(async move {
            let mut pid = 0u32;
            while let Ok((sock, _)) = listener.accept().await {
                pid += 1;
                tokio::spawn(Self::serve(sock, pid, Arc::clone(&accept_ctl)));
            }
        });
        Self { addr, ctl }
    }

    async fn serve(mut sock: TcpStream, pid: u32, ctl: Arc<BackendCtl>) {
        // The startup packet (an SSLRequest is declined first).
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
                80877102 => {
                    // The BackendKeyData it names: "cancel <pid> <secret>".
                    let named =
                        |at: usize| u32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    ctl.note(pid, b'!', &format!("cancel {} {}", named(4), named(8)));
                    if ctl.stall_cancels.load(std::sync::atomic::Ordering::Relaxed) {
                        std::future::pending::<()>().await;
                    }
                    return;
                }
                _ => {
                    // key\0value\0 ... \0 after the protocol version.
                    let fields: Vec<&[u8]> = body[4..].split(|b| *b == 0).collect();
                    let database = fields
                        .chunks(2)
                        .find(|kv| kv[0] == b"database")
                        .and_then(|kv| kv.get(1))
                        .map(|v| String::from_utf8_lossy(v).into_owned())
                        .unwrap_or_default();
                    ctl.startup_databases.lock().unwrap().push(database);
                    let params = fields
                        .chunks(2)
                        .filter(|kv| kv.len() == 2 && !kv[0].is_empty())
                        .map(|kv| {
                            (
                                String::from_utf8_lossy(kv[0]).into_owned(),
                                String::from_utf8_lossy(kv[1]).into_owned(),
                            )
                        })
                        .collect();
                    ctl.startup_params.lock().unwrap().push(params);
                    break;
                }
            }
        }
        let kill = Arc::new(Notify::new());
        ctl.kills.lock().unwrap().insert(pid, Arc::clone(&kill));
        let (key_pid, secret) = ctl.shared_key.unwrap_or((pid, pid + 1000));
        let mut key = key_pid.to_be_bytes().to_vec();
        key.extend_from_slice(&secret.to_be_bytes());
        let hello = [
            wire(b'R', &[0, 0, 0, 0]),
            wire(b'K', &key),
            wire(b'Z', b"I"),
        ]
        .concat();
        if sock.write_all(&hello).await.is_err() {
            return;
        }
        let mut status = b'I';
        let mut mode = BackendMode::Ready;
        // Extended protocol: the statements Parsed (name -> SQL) and the SQL
        // of the unnamed portal, which the last Bind created, until its
        // transaction ends.
        let mut statements: HashMap<String, String> = HashMap::new();
        let mut portal: Option<String> = None;
        // A COPY completion may stay in PostgreSQL's output buffer until a
        // Flush or Sync. Exercise that path separately from eager replies.
        let mut copy_completion = Vec::new();
        loop {
            let mut header = [0u8; 5];
            tokio::select! {
                r = sock.read_exact(&mut header) => if r.is_err() { return },
                _ = kill.notified() => {
                    if ctl.reset_on_kill.load(Ordering::Relaxed) {
                        let _ = socket2::SockRef::from(&sock).set_linger(Some(Duration::ZERO));
                    }
                    return;
                }
            }
            let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
            let mut body = vec![0u8; len - 4];
            if sock.read_exact(&mut body).await.is_err() {
                return;
            }
            let tag = header[0];
            let cstrings = |n: usize| -> String {
                body.split(|b| *b == 0)
                    .nth(n)
                    .map(|q| String::from_utf8_lossy(q).into_owned())
                    .unwrap_or_default()
            };
            let text = match tag {
                b'Q' => String::from_utf8_lossy(&body[..body.len() - 1]).into_owned(),
                b'P' => cstrings(1),
                // The kind and the name: `S` for the unnamed statement.
                b'C' => cstrings(0),
                // The function OID.
                b'F' => body
                    .first_chunk::<4>()
                    .map(|oid| u32::from_be_bytes(*oid).to_string())
                    .unwrap_or_default(),
                _ => String::new(),
            };
            ctl.note(pid, tag, &text);
            match (mode, tag) {
                (_, b'X') => return,
                (BackendMode::Ready, b'H' | b'S') if !copy_completion.is_empty() => {
                    if tag == b'S' {
                        copy_completion.extend(wire(b'Z', &[status]));
                        portal = None;
                    }
                    if sock.write_all(&copy_completion).await.is_err() {
                        return;
                    }
                    copy_completion.clear();
                    continue;
                }
                // Everything up to the Sync is discarded after an error.
                (BackendMode::SkipToSync, b'S') => mode = BackendMode::Ready,
                (BackendMode::SkipToSync, _) => continue,
                // Copy-in: data is consumed; Sync and Flush are ignored
                // (libpq sends a Sync right after an extended COPY's Execute).
                (BackendMode::CopySimple | BackendMode::CopyExtended, b'd' | b'S' | b'H') => {
                    continue
                }
                (BackendMode::CopySimple, b'c') => {
                    mode = BackendMode::Ready;
                    let reply = [wire(b'C', &cstring("COPY 1")), wire(b'Z', &[status])].concat();
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    continue;
                }
                (BackendMode::CopyExtended, b'c') => {
                    if portal.as_deref().is_some_and(|p| p.contains("hang")) {
                        kill.notified().await;
                        return;
                    }
                    if portal.as_deref().is_some_and(|p| p.contains("slow_end")) {
                        ctl.slow.acquire().await.unwrap().forget();
                    }
                    mode = BackendMode::Ready;
                    if portal.as_deref().is_some_and(|p| p.contains("flush_ack")) {
                        copy_completion = wire(b'C', &cstring("COPY 1"));
                        continue;
                    }
                    if sock
                        .write_all(&wire(b'C', &cstring("COPY 1")))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                (BackendMode::CopySimple | BackendMode::CopyExtended, b'f') => {
                    let extended = mode == BackendMode::CopyExtended;
                    let mut reply = wire(b'E', b"SERROR\0C57014\0Mcopy failed\0\0");
                    if extended {
                        // The client's Sync gets the ReadyForQuery.
                        mode = BackendMode::SkipToSync;
                    } else {
                        mode = BackendMode::Ready;
                        reply.extend(wire(b'Z', &[status]));
                    }
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    continue;
                }
                // Any other message ends copy-in: the COPY fails (08P01)
                // and the message is consumed, never run.
                (BackendMode::CopySimple | BackendMode::CopyExtended, _) => {
                    let mut reply = wire(
                        b'E',
                        format!(
                            "SERROR\0C08P01\0Munexpected message type 0x{tag:02X} during COPY from stdin\0\0"
                        )
                        .as_bytes(),
                    );
                    if mode == BackendMode::CopyExtended {
                        mode = BackendMode::SkipToSync;
                    } else {
                        mode = BackendMode::Ready;
                        if status == b'T' {
                            status = b'E';
                        }
                        reply.extend(wire(b'Z', &[status]));
                    }
                    if sock.write_all(&reply).await.is_err() {
                        return;
                    }
                    continue;
                }
                _ => {}
            }
            if tag == b'Q' {
                // PostgreSQL destroys the unnamed statement and portal at
                // every simple Query, before it runs anything.
                statements.remove("");
                portal = None;
            }
            let bound = portal.clone().unwrap_or_default();
            let reply = match tag {
                b'Q' if text.contains("crash") => return,
                b'E' if bound.contains("crash") => return,
                b'E' if bound.contains("hang") && !bound.contains("FROM STDIN") => {
                    kill.notified().await;
                    if bound.contains("terminate") {
                        // `pg_terminate_backend`: its FATAL, then the close.
                        let _ = sock
                            .write_all(&wire(
                                b'E',
                                b"SFATAL\0VFATAL\0C57P01\0Mterminating connection due to administrator command\0\0",
                            ))
                            .await;
                    }
                    return;
                }
                // A COPY of a table that does not exist fails at once: the
                // backend never enters copy-in.
                b'Q' if text.contains("missing_table") => [
                    wire(
                        b'E',
                        b"SERROR\0C42P01\0Mrelation \"missing_table\" does not exist\0\0",
                    ),
                    wire(b'Z', &[status]),
                ]
                .concat(),
                // CopyInResponse (text, no columns): the client's CopyData
                // follow, and its CopyDone is answered.
                b'Q' if text.to_ascii_uppercase().contains("FROM STDIN") => {
                    mode = BackendMode::CopySimple;
                    wire(b'G', &[0, 0, 0])
                }
                b'Q' if text == "SHOW ALL" => {
                    if ctl.hold_scope_probes.load(Ordering::Relaxed) {
                        ctl.scope_probes.acquire().await.unwrap().forget();
                    }
                    show_all_reply(status)
                }
                b'Q' if text == "SELECT v FROM scope_tenant_fixture" => [
                    text_row_description(&["v"]),
                    text_data_row(&["1"]),
                    wire(b'C', b"SELECT 1\0"),
                    wire(b'Z', &[status]),
                ]
                .concat(),
                b'Q' => {
                    if text.contains("slow") {
                        ctl.slow.acquire().await.unwrap().forget();
                    }
                    let upper = text.trim().to_ascii_uppercase();
                    status = status_after(&text, status);
                    let tag = if upper.starts_with("BEGIN") {
                        "BEGIN".to_string()
                    } else if upper.contains("PREPARED") {
                        upper.clone()
                    } else if upper.starts_with("COMMIT")
                        || upper.starts_with("ROLLBACK")
                        || upper.starts_with("END")
                    {
                        "COMMIT".to_string()
                    } else if upper.starts_with("DISCARD ALL") {
                        if let Some(d) = ctl.discard_delay {
                            tokio::time::sleep(d).await;
                        }
                        ctl.note(pid, RESET_ANSWERED, "DISCARD ALL answered");
                        status = b'I';
                        "DISCARD ALL".to_string()
                    } else {
                        "SELECT 1".to_string()
                    };
                    [wire(b'C', &cstring(&tag)), wire(b'Z', &[status])].concat()
                }
                b'P' => {
                    statements.insert(cstrings(0), text);
                    wire(b'1', &[])
                }
                b'B' => match statements.get(&cstrings(1)) {
                    Some(sql) => {
                        portal = Some(sql.clone());
                        wire(b'2', &[])
                    }
                    // PostgreSQL then skips everything up to the Sync.
                    None => {
                        mode = BackendMode::SkipToSync;
                        wire(
                            b'E',
                            b"SERROR\0C26000\0Mprepared statement does not exist\0\0",
                        )
                    }
                },
                b'E' if portal.is_none() => {
                    mode = BackendMode::SkipToSync;
                    wire(b'E', b"SERROR\0C34000\0Mportal does not exist\0\0")
                }
                b'E' if bound.to_ascii_uppercase().contains("FROM STDIN") => {
                    mode = BackendMode::CopyExtended;
                    wire(b'G', &[0, 0, 0])
                }
                b'E' if bound.contains("violates") => {
                    mode = BackendMode::SkipToSync;
                    wire(
                        b'E',
                        b"SERROR\0C23505\0Mduplicate key value violates unique constraint\0\0",
                    )
                }
                b'E' => {
                    if bound.contains("slow") {
                        ctl.slow.acquire().await.unwrap().forget();
                    }
                    status = status_after(&bound, status);
                    wire(b'C', &cstring("SELECT 1"))
                }
                b'C' => {
                    if let Some(name) = text.strip_prefix('S') {
                        statements.remove(name);
                    } else if text == "P" {
                        portal = None;
                    }
                    wire(b'3', &[])
                }
                b'D' => wire(b'n', &[]),
                // FunctionCallResponse (a four-byte result), ReadyForQuery.
                b'F' => [wire(b'V', &[0, 0, 0, 4, 0, 0, 0, 1]), wire(b'Z', &[status])].concat(),
                b'S' => {
                    // The implicit transaction ends here, and its portals.
                    if status == b'I' {
                        portal = None;
                    }
                    wire(b'Z', &[status])
                }
                _ => Vec::new(),
            };
            if !reply.is_empty() && sock.write_all(&reply).await.is_err() {
                return;
            }
        }
    }

    fn release_slow(&self) {
        self.ctl.slow.add_permits(1);
    }

    fn kill(&self, pid: u32) {
        let kill = self.ctl.kills.lock().unwrap().get(&pid).cloned();
        kill.expect("a live backend connection").notify_one();
    }

    fn log(&self) -> Vec<Seen> {
        self.ctl.log.lock().unwrap().clone()
    }

    /// Frames the backend connection `pid` received.
    fn frames_of(&self, pid: u32) -> Vec<Seen> {
        self.log().into_iter().filter(|s| s.conn == pid).collect()
    }

    fn saw_query(&self, sql: &str) -> bool {
        self.log().iter().any(|s| s.tag == b'Q' && s.text == sql)
    }
}

/// A client session through `handle_client`, past its startup.
struct Client {
    s: TcpStream,
    /// The BackendKeyData the proxy relayed: (pid, secret).
    key: (u32, u32),
    task: JoinHandle<Result<()>>,
}

impl Client {
    async fn send(&mut self, bytes: &[u8]) {
        self.s.write_all(bytes).await.unwrap();
    }

    async fn frame(&mut self) -> Vec<u8> {
        tokio::time::timeout(TEST_TIMEOUT, async {
            let mut header = [0u8; 5];
            self.s.read_exact(&mut header).await.unwrap();
            let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
            let mut out = header.to_vec();
            out.resize(len + 1, 0);
            self.s.read_exact(&mut out[5..]).await.unwrap();
            out
        })
        .await
        .expect("expected frame did not arrive")
    }

    /// Frames up to and including the next ReadyForQuery.
    async fn until_ready(&mut self) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        loop {
            let f = self.frame().await;
            let last = f[0] == b'Z';
            frames.push(f);
            if last {
                return frames;
            }
        }
    }

    /// The next `n` frames.
    async fn frames(&mut self, n: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.frame().await);
        }
        out
    }

    async fn query(&mut self, sql: &str) -> Vec<Vec<u8>> {
        self.send(&query(sql)).await;
        self.until_ready().await
    }

    /// Nothing more arrives for a short while.
    async fn quiet(&mut self) {
        let mut b = [0u8; 1];
        let r = tokio::time::timeout(Duration::from_millis(150), self.s.read(&mut b)).await;
        assert!(r.is_err(), "unexpected bytes from the proxy: {r:?}");
    }

    /// The ReadyForQuery that follows a refusal, then the end of the stream.
    async fn closed_after_ready(&mut self) {
        let ready = self.frame().await;
        assert_eq!(ready[0], b'Z', "{ready:?}");
        self.closed().await;
    }

    /// The proxy closes the connection (end of stream) with nothing more.
    async fn closed(&mut self) {
        let mut b = [0u8; 1];
        let n = tokio::time::timeout(TEST_TIMEOUT, self.s.read(&mut b))
            .await
            .expect("the proxy closes the connection")
            .unwrap_or(0);
        assert_eq!(n, 0, "unexpected bytes from the proxy before it closed");
    }
}

/// The frames are exactly ErrorResponse `code` (whose text contains `text`)
/// then ReadyForQuery 'I'.
fn assert_refused(frames: &[Vec<u8>], code: &str, text: &str) {
    assert_eq!(frames.len(), 2, "{frames:?}");
    assert_eq!(sqlstate(&frames[0]), code);
    assert!(has_text(&frames[0], text), "{:?}", frames[0]);
    assert_eq!(frames[1], wire(b'Z', b"I"));
}

fn assert_served(frames: &[Vec<u8>], status: u8) {
    assert!(
        frames.iter().all(|f| f[0] != b'E'),
        "unexpected error: {frames:?}"
    );
    assert_eq!(frames.last().unwrap(), &wire(b'Z', &[status]));
}

const REFUSED: &str = "too many active queries for tenant \"acme\" (limit 1)";

/// A complete CopyInResponse already in the socket accumulator must arm
/// the timer before the backend/client select begins waiting for more bytes.
#[tokio::test]
async fn copy_stall_deadline_arms_for_an_already_buffered_response() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let session = make_test_session();
    let (socket, _backend_peer) = pair().await;
    let mut backend = BackendConn::new(socket);
    backend
        .response_buf
        .extend_from_slice(&wire(b'G', &[0, 0, 0]));
    let mut conns = ConnMap::from_iter([("buffered".into(), backend)]);
    let (socket, _client_peer) = pair().await;
    let mut client = crate::client_tls::ClientStream::Plain(socket);
    let mut buffer = BytesMut::new();
    let read = tokio::time::timeout(
        Duration::from_millis(200),
        ProxyServer::read_next_client_message(
            &mut client,
            &mut buffer,
            &mut conns,
            Some("buffered"),
            None,
            &h.state,
            IdleWatch {
                session: &session,
                keep_session: false,
                copy_end_wait: None,
                drain: false,
            },
        ),
    )
    .await
    .expect("buffered COPY must re-arm before waiting")
    .unwrap();
    assert_eq!(read, ClientRead::CopyStateChanged);
    assert!(session.copy_in_progress.load(Ordering::Relaxed));
}

/// An ErrorResponse ends the input obligation before RFQ arrives. Keep the
/// backend pinned, but do not label the wait for its RFQ as stalled input.
#[tokio::test]
async fn copy_stall_deadline_disarms_on_a_backend_error_before_ready() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let session = make_test_session();
    session.copy_in_progress.store(true, Ordering::Relaxed);
    let (socket, _backend_peer) = pair().await;
    let mut backend = BackendConn::new(socket);
    backend
        .response_buf
        .extend_from_slice(&wire(b'E', b"SERROR\0C57014\0Mcancelled\0\0"));
    let mut conns = ConnMap::from_iter([("buffered".into(), backend)]);
    let (socket, _client_peer) = pair().await;
    let mut client = crate::client_tls::ClientStream::Plain(socket);
    let mut buffer = BytesMut::new();
    let read = tokio::time::timeout(
        Duration::from_millis(200),
        ProxyServer::read_next_client_message(
            &mut client,
            &mut buffer,
            &mut conns,
            Some("buffered"),
            None,
            &h.state,
            IdleWatch {
                session: &session,
                keep_session: false,
                copy_end_wait: None,
                drain: false,
            },
        ),
    )
    .await
    .expect("COPY error must disarm before waiting")
    .unwrap();
    assert_eq!(read, ClientRead::CopyStateChanged);
    assert!(
        session.copy_in_progress.load(Ordering::Relaxed),
        "RFQ still owed"
    );
    assert!(ProxyServer::client_idle_deadline(&h.state, &buffer, &session).is_none());
    conns
        .get_mut("buffered")
        .unwrap()
        .response_buf
        .extend_from_slice(&wire(b'Z', b"I"));
    let read = tokio::time::timeout(
        Duration::from_millis(200),
        ProxyServer::read_next_client_message(
            &mut client,
            &mut buffer,
            &mut conns,
            Some("buffered"),
            None,
            &h.state,
            IdleWatch {
                session: &session,
                keep_session: false,
                copy_end_wait: None,
                drain: false,
            },
        ),
    )
    .await
    .expect("COPY RFQ must settle before waiting")
    .unwrap();
    assert_eq!(read, ClientRead::CopyStateChanged);
    assert!(!session.copy_in_progress.load(Ordering::Relaxed));
}

/// A watched COPY backend dying is a backend failure, never client inactivity.
#[tokio::test]
async fn copy_stall_deadline_reports_backend_loss_without_waiting_for_input() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&query("COPY data FROM STDIN")).await;
    assert_eq!(a.frame().await[0], b'G');
    h.backend.kill(a.key.0);
    let error = a.frame().await;
    assert_eq!(sqlstate(&error), "08006");
    assert!(has_text(&error, "FATAL"));
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, a.s.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    h.until("acme", "lost COPY returned capacity", |s| {
        s.queries_active == 0
    })
    .await;
    assert_served(&b.query("select 1").await, b'I');
}

/// A backend cancellation can finish COPY without another client byte.
/// The buffered/Flush relay must observe that boundary before arming a timer.
#[tokio::test]
async fn copy_stall_deadline_disarms_when_backend_ends_copy() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let session = make_test_session();
    let (stream, mut peer) = pair().await;
    let mut stream = crate::client_tls::ClientStream::Plain(stream);
    let start = wire(b'G', &[0, 0, 0]);
    let mut frames = BytesMut::from(start.as_slice());
    ProxyServer::forward_buffered_backend_frames(&mut stream, &mut frames, &session, &h.state)
        .await
        .unwrap();
    assert!(session.copy_in_progress.load(Ordering::Relaxed));
    assert!(ProxyServer::client_idle_deadline(&h.state, &BytesMut::new(), &session).is_some());
    let end = [
        wire(b'E', b"SERROR\0C57014\0Mcancelled\0\0"),
        wire(b'Z', b"I"),
    ]
    .concat();
    frames.extend_from_slice(&end);
    ProxyServer::forward_buffered_backend_frames(&mut stream, &mut frames, &session, &h.state)
        .await
        .unwrap();
    assert!(!session.copy_in_progress.load(Ordering::Relaxed));
    assert!(session.last_response_error.load(Ordering::Relaxed));
    assert!(ProxyServer::client_idle_deadline(&h.state, &BytesMut::new(), &session).is_none());
    let expected = [start, end].concat();
    let mut received = vec![0; expected.len()];
    peer.read_exact(&mut received).await.unwrap();
    assert_eq!(received, expected);
}

/// A stalled COPY, including a partial CopyData frame, must roll back and
/// release its tenant's capacity without requiring any further client input.
#[tokio::test]
async fn copy_stall_deadline_reclaims_simple_extended_and_partial_input() {
    for extended in [false, true] {
        for partial in [false, true] {
            let h = Harness::new(0, None, |config| {
                config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            if extended {
                a.send(
                    &[
                        parse("c", "COPY data FROM STDIN"),
                        bind("c"),
                        execute(),
                        flush(),
                    ]
                    .concat(),
                )
                .await;
                assert_eq!(a.frame().await[0], b'1');
                assert_eq!(a.frame().await[0], b'2');
            } else {
                a.send(&query("COPY data FROM STDIN")).await;
            }
            assert_eq!(a.frame().await[0], b'G');
            assert_refused(&b.query("select 'blocked'").await, "53300", REFUSED);
            if partial {
                // A CopyData header announcing 12 payload bytes, only one sent.
                a.send(&[b'd', 0, 0, 0, 16, b'x']).await;
            }
            let fatal = a.frame().await;
            assert_eq!(sqlstate(&fatal), "57014");
            assert!(has_text(&fatal, "FATAL") && has_text(&fatal, "COPY input timeout"));
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(TEST_TIMEOUT, a.s.read(&mut byte))
                    .await
                    .expect("stalled COPY session must close")
                    .unwrap(),
                0
            );
            h.until("acme", "stalled COPY returned its permit", |stats| {
                stats.queries_active == 0
            })
            .await;
            assert_eq!(h.backend_cancels(), 1, "cancelled before capacity returned");
            assert_eq!(
                h.state
                    .metrics
                    .policy
                    .copy_input_timeouts
                    .load(Ordering::Relaxed),
                1
            );
            assert!(h
                .backend
                .frames_of(a.key.0)
                .iter()
                .all(|frame| frame.tag != b'd'));
            assert_served(&b.query("select 'after timeout'").await, b'I');
        }
    }
}

/// The COPY inactivity clock follows client progress rather than total load
/// duration, and a completed COPY leaves the connection usable.
#[tokio::test]
async fn copy_stall_deadline_resets_on_client_progress() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let mut a = h.connect("acme").await;
    a.send(&query("COPY data FROM STDIN")).await;
    assert_eq!(a.frame().await[0], b'G');
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        a.send(&wire(b'd', b"one row\n")).await;
    }
    a.send(&wire(b'c', &[])).await;
    assert_served(&a.until_ready().await, b'I');
    assert_served(&a.query("select 'still usable'").await, b'I');
}

/// A FunctionCall frame: `oid`, no argument format codes, no arguments, a
/// binary result.
fn function_call(oid: u32) -> Vec<u8> {
    let mut body = oid.to_be_bytes().to_vec();
    body.extend_from_slice(&[0, 0, 0, 0, 0, 1]);
    wire(b'F', &body)
}

/// `lo_open` and `lowrite` (large-object functions the default mode
/// forwards), and an OID that is neither a large-object function nor, in the
/// fake backend, anything else.
const LO_OPEN: u32 = 952;
const LOWRITE: u32 = 955;
const SOME_FUNCTION: u32 = 42;

fn saw_function_call(h: &Harness, oid: u32) -> bool {
    h.backend
        .log()
        .iter()
        .any(|s| s.tag == b'F' && s.text == oid.to_string())
}

/// `function_call` decides by OID what reaches the backend. A refused call
/// is answered `0A000` and ReadyForQuery, and the session goes on.
#[tokio::test]
async fn the_function_call_mode_decides_what_reaches_the_backend() {
    use crate::config::FunctionCallMode::{Any, LargeObjects, Refuse};
    for (mode, oid, forwarded) in [
        (LargeObjects, LO_OPEN, true),
        (LargeObjects, LOWRITE, true),
        (LargeObjects, SOME_FUNCTION, false),
        (Refuse, LO_OPEN, false),
        (Any, SOME_FUNCTION, true),
    ] {
        let h = Harness::new(0, None, |c| c.function_call = mode).await;
        let mut a = h.connect("acme").await;
        a.send(&function_call(oid)).await;
        let frames = a.until_ready().await;
        if forwarded {
            assert_eq!(tags(&frames), b"VZ", "{mode:?} {oid}");
            assert_served(&frames, b'I');
        } else {
            assert_refused(&frames, "0A000", "FunctionCall");
        }
        assert_eq!(saw_function_call(&h, oid), forwarded, "{mode:?} {oid}");
        // Refused inside a transaction, the status stays the backend's.
        assert_served(&a.query("BEGIN").await, b'T');
        a.send(&function_call(oid)).await;
        let frames = a.until_ready().await;
        assert_eq!(frames.last().unwrap(), &wire(b'Z', b"T"), "{mode:?} {oid}");
        assert_eq!(frames[0][0] == b'E', !forwarded, "{mode:?} {oid}");
        assert_served(&a.query("COMMIT").await, b'I');
        assert_eq!(
            h.backend
                .log()
                .iter()
                .filter(|s| s.tag == b'F' && s.text == oid.to_string())
                .count(),
            if forwarded { 2 } else { 0 },
            "{mode:?} {oid}"
        );
    }
}

/// A forwarded FunctionCall is a request under the tenant's active-query
/// cap: refused at the cap without reaching the backend, holding the permit
/// while it runs, and returning it with ReadyForQuery `I`.
#[tokio::test]
async fn a_function_call_takes_and_returns_the_tenant_permit() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&function_call(LO_OPEN)).await;
    assert_refused(&b.until_ready().await, "53300", REFUSED);
    assert!(!saw_function_call(&h, LO_OPEN), "never forwarded");
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;

    b.send(&function_call(LO_OPEN)).await;
    assert_eq!(tags(&b.until_ready().await), b"VZ");
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    // A transaction's calls run under the permit it holds.
    assert_served(&b.query("BEGIN").await, b'T');
    b.send(&function_call(LOWRITE)).await;
    assert_served(&b.until_ready().await, b'T');
    assert_refused(&a.query("select 'a'").await, "53300", REFUSED);
    assert_served(&b.query("COMMIT").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&a.query("select 'a'").await, b'I');
}

/// A large-object call takes no session lock: the session returns its permit
/// when it idles. A function called by OID under `function_call = "any"`
/// may have taken one, so the session keeps permit and connection until it
/// unlocks everything.
#[tokio::test]
async fn only_an_arbitrary_function_call_pins_the_permit() {
    let h = Harness::new(0, None, |c| {
        c.function_call = crate::config::FunctionCallMode::Any;
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&function_call(SOME_FUNCTION)).await;
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "A idle holding the permit", |s| {
        s.queries_active == 1 && s.queries_idle == 1
    })
    .await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(&a.query("SELECT pg_advisory_unlock_all()").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
}

/// A function called by OID under `function_call = "any"` may have written
/// any table: every cached result is retired before its ReadyForQuery
/// reaches the client. A large-object call retires nothing.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn an_arbitrary_function_call_retires_every_cached_result() {
    use crate::config::FunctionCallMode::{Any, LargeObjects};
    for (mode, oid, retired) in [(LargeObjects, LOWRITE, 0), (Any, SOME_FUNCTION, 1)] {
        let h = Harness::new(0, None, |c| {
            c.function_call = mode;
            c.cache.enabled = true;
            c.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&function_call(oid)).await;
        assert_eq!(tags(&a.until_ready().await), b"VZ");
        let cache = h.state.query_cache.as_ref().expect("the query cache runs");
        assert_eq!(cache.unseen_write_invalidations(), retired, "{mode:?}");
    }
}

/// The journal has no text for a FunctionCall: a transaction that ran one
/// is not replayed on a replacement backend. Without the call it is.
#[tokio::test]
async fn a_transaction_with_a_function_call_is_never_replayed() {
    for with_call in [false, true] {
        let h = Harness::new(0, None, |c| {
            c.tr_mode = crate::config::TrMode::Transaction;
        })
        .await;
        let mut a = h.connect("acme").await;
        assert_served(&a.query("BEGIN").await, b'T');
        assert_served(&a.query("insert into t values (1)").await, b'T');
        if with_call {
            a.send(&function_call(LOWRITE)).await;
            assert_served(&a.until_ready().await, b'T');
        }
        h.backend.kill(a.key.0);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let frames = a.query("select 'a'").await;
        let replayed =
            h.backend.log().iter().any(|s| {
                s.conn != a.key.0 && s.tag == b'Q' && s.text == "insert into t values (1)"
            });
        assert_eq!(replayed, !with_call, "{frames:?}");
        if with_call {
            assert_eq!(sqlstate(&frames[0]), "57P01");
            assert_eq!(frames.last().unwrap(), &wire(b'Z', b"E"));
            assert!(!h.backend.saw_query("select 'a'"));
        } else {
            assert_served(&frames, b'T');
        }
        assert_served(&a.query("ROLLBACK").await, b'I');
    }
}

/// A FunctionCall in copy-in ends the COPY at its own connection, like any
/// other message, and the session goes on.
#[tokio::test]
async fn a_function_call_in_copy_in_ends_the_copy() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    a.send(&query("COPY data FROM STDIN")).await;
    assert_eq!(a.frame().await[0], b'G');
    a.send(&function_call(LO_OPEN)).await;
    let frames = a.until_ready().await;
    assert_eq!(sqlstate(&frames[0]), "08P01");
    assert_eq!(frames.last().unwrap(), &wire(b'Z', b"I"));
    assert_served(&a.query("select 'still usable'").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// A Bind of `stmt` into the unnamed portal with one text parameter `value`
/// and one result column in `result_format` (0 text, 1 binary).
#[cfg(feature = "query-cache")]
fn bind_value(stmt: &str, value: &str, result_format: u16) -> Vec<u8> {
    let mut body = [cstring(""), cstring(stmt)].concat();
    body.extend_from_slice(&[0, 0, 0, 1]);
    body.extend_from_slice(&(value.len() as u32).to_be_bytes());
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(&[0, 1]);
    body.extend_from_slice(&result_format.to_be_bytes());
    wire(b'B', &body)
}

/// Describe of the unnamed portal.
#[cfg(feature = "query-cache")]
fn describe_portal() -> Vec<u8> {
    wire(b'D', b"P\0")
}

#[cfg(feature = "query-cache")]
const KV_READ: &str = "SELECT v FROM kv WHERE id = $1";

/// A harness whose query cache answers reads the operator vouched for.
#[cfg(feature = "query-cache")]
async fn cache_harness(tweak: impl FnOnce(&mut ProxyConfig)) -> Harness {
    Harness::new(0, None, |c| {
        c.cache.enabled = true;
        c.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        c.cache.trust_backend_read_effects = true;
        tweak(c);
    })
    .await
}

/// How many `tag` frames the backend received.
#[cfg(feature = "query-cache")]
fn backend_saw(h: &Harness, tag: u8) -> usize {
    h.backend.log().iter().filter(|s| s.tag == tag).count()
}

/// An unnamed Parse/Bind/Execute/Sync read is stored at its first run and
/// answered from the cache after it, byte for byte, without an Execute on
/// the backend. Another parameter value, another result format and a
/// described portal are other entries.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn an_unnamed_extended_read_is_answered_from_the_cache() {
    let h = cache_harness(|_| {}).await;
    let mut a = h.connect("acme").await;
    let read = |value: &str, format: u16, describe: bool| {
        let mut batch = [parse("", KV_READ), bind_value("", value, format)].concat();
        if describe {
            batch.extend(describe_portal());
        }
        batch.extend(execute());
        batch.extend(sync());
        batch
    };
    a.send(&read("1", 0, false)).await;
    let first = a.until_ready().await;
    assert_eq!(tags(&first), b"12CZ");
    assert_eq!(backend_saw(&h, b'E'), 1);
    for _ in 0..3 {
        a.send(&read("1", 0, false)).await;
        assert_eq!(
            a.until_ready().await,
            first,
            "the stored response, as it was"
        );
    }
    assert_eq!(backend_saw(&h, b'E'), 1, "three hits ran nothing");
    // What the response depends on separates the entries.
    let mut executed = 1;
    for (value, format, describe, shape) in [
        ("2", 0, false, &b"12CZ"[..]),
        ("1", 1, false, b"12CZ"),
        ("1", 0, true, b"12nCZ"),
    ] {
        for hit in [false, true] {
            a.send(&read(value, format, describe)).await;
            assert_eq!(
                tags(&a.until_ready().await),
                shape,
                "{value} {format} {describe}"
            );
            if !hit {
                executed += 1;
            }
            assert_eq!(
                backend_saw(&h, b'E'),
                executed,
                "{value} {format} {describe} hit={hit}"
            );
        }
    }
    // The first entry is still there.
    a.send(&read("1", 0, false)).await;
    assert_eq!(a.until_ready().await, first);
    assert_eq!(backend_saw(&h, b'E'), executed);
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// A prepared (named) statement's Bind/Execute/Sync is cached the same way,
/// and another session's identical statement under another name shares the
/// entry.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn a_named_extended_read_is_answered_from_the_cache() {
    let h = cache_harness(|_| {}).await;
    let mut a = h.connect("acme").await;
    a.send(&[parse("s1", KV_READ), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"1Z");
    let run = |name: &str, value: &str| [bind_value(name, value, 0), execute(), sync()].concat();
    a.send(&run("s1", "1")).await;
    let first = a.until_ready().await;
    assert_eq!(tags(&first), b"2CZ");
    assert_eq!(backend_saw(&h, b'E'), 1);
    a.send(&run("s1", "1")).await;
    assert_eq!(a.until_ready().await, first);
    assert_eq!(backend_saw(&h, b'E'), 1);
    a.send(&run("s1", "2")).await;
    assert_eq!(tags(&a.until_ready().await), b"2CZ");
    assert_eq!(backend_saw(&h, b'E'), 2);
    drop(a);
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    let mut b = h.connect("acme").await;
    b.send(&[parse("other_name", KV_READ), sync()].concat())
        .await;
    assert_eq!(tags(&b.until_ready().await), b"1Z");
    b.send(&run("other_name", "1")).await;
    assert_eq!(b.until_ready().await, first);
    assert_eq!(backend_saw(&h, b'E'), 2, "the other session's entry");
}

/// A cached response is kept in an allocation of its own size, not in the
/// capture buffer it was read into.
#[cfg(feature = "query-cache")]
#[test]
fn a_captured_body_does_not_keep_its_capture_buffer() {
    for len in [0usize, 60, 2047, 2048, 4000, 4096] {
        let mut captured = Vec::with_capacity(4096);
        captured.resize(len, 7u8);
        let body = ProxyServer::captured_body(captured.clone());
        assert_eq!(&body[..], &captured[..], "{len}");
        // Alone in its allocation, `Bytes` hands it back as a vector of the
        // capacity it holds.
        let held = Vec::from(body).capacity();
        assert_eq!(held, len, "{len}");
    }
}

/// Under the default `extended_protocol = "hint"` only a statement whose text
/// carries `helios:cache=on` is looked up, unnamed or prepared; a skip beside
/// it wins, and `"off"` ignores the hint.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn an_extended_read_is_cached_under_hint_mode_only_when_its_text_asks() {
    use crate::config::ExtendedProtocolCache::{Hint, Off};
    const ASKS: &str = "SELECT v FROM kv WHERE id = $1 /*helios:cache=on*/";
    const ASKS_AND_SKIPS: &str = "SELECT v FROM kv WHERE id = $1 /*helios:cache=on,cache=skip*/";
    let unnamed = |sql: &str| [parse("", sql), bind_value("", "1", 0), execute(), sync()].concat();
    assert_eq!(
        ProxyConfig::default().cache.extended_protocol,
        Hint,
        "the default"
    );
    let h = cache_harness(|c| c.cache.extended_protocol = Hint).await;
    let mut a = h.connect("acme").await;
    let mut executed = 0;
    // (statement, whether its second run is a hit)
    for (sql, cached) in [(KV_READ, false), (ASKS, true), (ASKS_AND_SKIPS, false)] {
        for run in 0..3 {
            a.send(&unnamed(sql)).await;
            assert_eq!(tags(&a.until_ready().await), b"12CZ", "{sql}");
            if run == 0 || !cached {
                executed += 1;
            }
            assert_eq!(backend_saw(&h, b'E'), executed, "{sql} run {run}");
        }
    }
    // A prepared statement carries the hint for each of its executions.
    for (name, sql, cached) in [("plain", KV_READ, false), ("asks", ASKS, true)] {
        a.send(&[parse(name, sql), sync()].concat()).await;
        assert_eq!(tags(&a.until_ready().await), b"1Z");
        // Another value than the unnamed runs stored.
        let run = [bind_value(name, "2", 0), execute(), sync()].concat();
        for n in 0..3 {
            a.send(&run).await;
            assert_eq!(tags(&a.until_ready().await), b"2CZ", "{name}");
            if n == 0 || !cached {
                executed += 1;
            }
            assert_eq!(backend_saw(&h, b'E'), executed, "{name} run {n}");
        }
    }

    let h = cache_harness(|c| c.cache.extended_protocol = Off).await;
    let mut a = h.connect("acme").await;
    for expected in [1, 2, 3] {
        a.send(&unnamed(ASKS)).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ");
        assert_eq!(backend_saw(&h, b'E'), expected, "\"off\" ignores the hint");
    }
}

/// A write to the table retires the entries that read it, a transaction's
/// reads are never cached, and `extended_protocol = false` leaves every
/// batch to the backend.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn extended_cache_entries_follow_writes_transactions_and_the_switch() {
    let read = || {
        [
            parse("", KV_READ),
            bind_value("", "1", 0),
            execute(),
            sync(),
        ]
        .concat()
    };
    let h = cache_harness(|_| {}).await;
    let mut a = h.connect("acme").await;
    for expected in [1, 1] {
        a.send(&read()).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ");
        assert_eq!(backend_saw(&h, b'E'), expected);
    }
    // Another session writes the table (a session that writes is no longer
    // served from the cache itself).
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    let mut writer = h.connect("acme").await;
    assert_served(
        &writer.query("UPDATE kv SET v = 2 WHERE id = 1").await,
        b'I',
    );
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    for expected in [2, 2] {
        a.send(&read()).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ");
        assert_eq!(backend_saw(&h, b'E'), expected, "refilled after the write");
    }
    // The writer's own reads run on the backend from then on.
    for expected in [3, 4] {
        writer.send(&read()).await;
        assert_eq!(tags(&writer.until_ready().await), b"12CZ");
        assert_eq!(backend_saw(&h, b'E'), expected, "a session that wrote");
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
    }
    // Inside a transaction every read runs.
    assert_served(&a.query("BEGIN").await, b'T');
    for expected in [5, 6] {
        a.send(&read()).await;
        let frames = a.until_ready().await;
        assert_eq!(frames.last().unwrap(), &wire(b'Z', b"T"));
        assert_eq!(backend_saw(&h, b'E'), expected);
    }
    assert_served(&a.query("COMMIT").await, b'I');

    let h =
        cache_harness(|c| c.cache.extended_protocol = crate::config::ExtendedProtocolCache::Off)
            .await;
    let mut a = h.connect("acme").await;
    for expected in [1, 2, 3] {
        a.send(&read()).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ");
        assert_eq!(backend_saw(&h, b'E'), expected);
    }
}

/// A hit runs nothing, but the client was told its unnamed Parse
/// succeeded: a later Bind of the unnamed statement without a Parse runs
/// that statement, on a backend that was given it.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn the_unnamed_statement_survives_a_cache_hit() {
    for immutable in [false, true] {
        let h = cache_harness(|c| c.cache.backend_context_immutable = immutable).await;
        let mut a = h.connect("acme").await;
        let read = |sql: &str| [parse("", sql), bind_value("", "1", 0), execute(), sync()].concat();
        let other = "SELECT w FROM kv WHERE id = $1";
        // Both statements are cached; the backend's unnamed one is `other`.
        for sql in [KV_READ, other] {
            a.send(&read(sql)).await;
            assert_eq!(tags(&a.until_ready().await), b"12CZ");
        }
        let executed = backend_saw(&h, b'E');
        let probes = h
            .backend
            .log()
            .iter()
            .filter(|s| s.text == "SHOW ALL")
            .count();
        // A hit on the first: the client's unnamed statement is KV_READ.
        a.send(&read(KV_READ)).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ");
        assert_eq!(backend_saw(&h, b'E'), executed, "a hit");
        if immutable {
            assert_eq!(
                h.backend
                    .log()
                    .iter()
                    .filter(|s| s.text == "SHOW ALL")
                    .count(),
                probes,
                "the proof of an immutable backend context is reused"
            );
        }
        // Bound again without a Parse, it runs as KV_READ on the backend.
        a.send(&[bind_value("", "9", 0), execute(), sync()].concat())
            .await;
        assert_eq!(
            tags(&a.until_ready().await),
            b"2CZ",
            "immutable={immutable}"
        );
        assert_eq!(backend_saw(&h, b'E'), executed + 1);
        let last_unnamed_parse = h
            .backend
            .frames_of(a.key.0)
            .iter()
            .rev()
            .find(|s| s.tag == b'P')
            .map(|s| s.text.clone());
        assert_eq!(
            last_unnamed_parse.as_deref(),
            Some(KV_READ),
            "immutable={immutable}"
        );
    }
}

/// An unnamed Parse that does not open its batch replaces the backend's
/// unnamed statement all the same: an identical Parse of the one before it
/// must reach the backend again, not be answered by the promotion.
#[tokio::test]
async fn an_unnamed_parse_inside_a_batch_ends_the_promotion_of_the_one_before() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let unnamed_parses = |h: &Harness, sql: &str| {
        h.backend
            .log()
            .iter()
            .filter(|s| s.tag == b'P' && s.text == sql)
            .count()
    };
    a.send(&[cycle("select 'x'"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ");
    // The promotion answers an identical Parse.
    a.send(&[cycle("select 'x'"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ");
    assert_eq!(unnamed_parses(&h, "select 'x'"), 1);
    // A named Parse first, then another unnamed statement.
    a.send(&[parse("s9", "select 9"), cycle("select 'y'"), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"112CZ");
    a.send(&[cycle("select 'x'"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ");
    assert_eq!(
        unnamed_parses(&h, "select 'x'"),
        2,
        "the backend held 'y': the Parse of 'x' reached it again"
    );
}

/// Unsupported execution must not evade SQL policy, admission, invalidation
/// or replay bookkeeping through the client's fallback message arm.
#[tokio::test]
async fn unsupported_frontend_messages_never_reach_the_backend() {
    for cap in [0, 1] {
        let h = Harness::new(0, None, |config| {
            config.multi_tenancy.max_active_queries_per_tenant = cap;
        })
        .await;
        for (tag, body, code) in [
            // A FunctionCall cut short after its function OID.
            (b'F', vec![0, 0, 0, 42], "08P01"),
            (b'Y', vec![], "08P01"),
            (b'R', vec![0, 0, 0, 0], "08P01"),
        ] {
            let mut client = h.connect("acme").await;
            client.send(&wire(tag, &body)).await;
            let error = client.frame().await;
            assert_eq!(sqlstate(&error), code, "tag {tag}, cap {cap}");
            assert!(has_text(&error, "FATAL"));
            let mut byte = [0];
            let read = tokio::time::timeout(TEST_TIMEOUT, client.s.read(&mut byte))
                .await
                .expect("the refused session must close")
                .unwrap();
            assert_eq!(read, 0, "no ReadyForQuery after a terminal refusal");
            assert!(
                h.backend
                    .frames_of(client.key.0)
                    .iter()
                    .all(|f| f.tag != tag),
                "unsupported tag {tag} reached the backend"
            );
            // Refusing one connection must not consume another's tenant permit.
            let mut control = h.connect("acme").await;
            assert_served(&control.query("select 1").await, b'I');
        }
    }
}

/// Flush completions belong to the eventual Sync's journal observation,
/// including read positions before a write and a previous read-only cycle.
#[tokio::test]
async fn journal_keeps_flushed_write_completions_and_cycle_positions() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    for sql in ["select 1", "insert into t values (1)"] {
        a.send(&[cycle(sql), flush()].concat()).await;
        assert_eq!(tags(&a.frames(3).await), b"12C");
    }
    assert_eq!(
        h.state.metrics.journal_committed.load(Ordering::Relaxed),
        0,
        "no commit before Sync"
    );
    a.send(&sync()).await;
    assert_eq!(tags(&a.until_ready().await), b"Z");
    h.until("acme", "first journaled cycle released its permit", |s| {
        s.queries_active == 0
    })
    .await;
    assert_eq!(
        h.state.metrics.journal_committed.load(Ordering::Relaxed),
        1,
        "flushed write completion was lost"
    );

    a.send(&[cycle("select 1"), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    a.send(&[cycle("insert into t values (2)"), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "second journaled cycle released its permit", |s| {
        s.queries_active == 0
    })
    .await;
    assert_eq!(
        h.state.metrics.journal_committed.load(Ordering::Relaxed),
        2,
        "read-only cycle polluted execute positions"
    );
}

struct Harness {
    server: ProxyServer,
    state: Arc<ServerState>,
    config: ProxyConfig,
    backend: FakeBackend,
}

impl Harness {
    /// Tenants "acme" and "other", identified by `user`, each with an
    /// active-query cap of 1 and `wait_ms` to wait for it.
    async fn new(
        wait_ms: u64,
        discard_delay: Option<Duration>,
        tweak: impl FnOnce(&mut ProxyConfig),
    ) -> Self {
        Self::on(FakeBackend::start(discard_delay).await, wait_ms, tweak)
    }

    /// [`Self::new`] in front of `backend`.
    fn on(backend: FakeBackend, wait_ms: u64, tweak: impl FnOnce(&mut ProxyConfig)) -> Self {
        let mut config = test_config();
        config.nodes.clear();
        config
            .add_node(&backend.addr.to_string(), "primary")
            .unwrap();
        config.pool.acquire_timeout_secs = 1;
        config.limits.startup_timeout_secs = 3;
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.identify_by = "user".into();
        config.multi_tenancy.tenants = vec!["acme".into(), "other".into()];
        config.multi_tenancy.max_active_queries_per_tenant = 1;
        config.multi_tenancy.active_query_wait_ms = wait_ms;
        tweak(&mut config);
        let server = ProxyServer::new(config.clone()).unwrap();
        let state = server.state.clone();
        Self {
            server,
            state,
            config,
            backend,
        }
    }

    async fn connect(&self, user: &str) -> Client {
        self.connect_with(startup_bytes(user)).await
    }

    /// A session whose startup packet names `user` and `database`.
    async fn connect_db(&self, user: &str, database: &str) -> Client {
        self.connect_with(startup_bytes_db(user, database)).await
    }

    /// A client that has sent `startup` and read nothing yet.
    async fn connect_raw(&self, startup: Vec<u8>) -> Client {
        let (proxy, mut s) = pair().await;
        let peer = proxy.peer_addr().unwrap();
        let task = tokio::spawn(ProxyServer::handle_client(
            proxy,
            peer,
            self.state.clone(),
            Arc::new(self.config.clone()),
            self.server.shutdown_tx.clone(),
        ));
        s.write_all(&startup).await.unwrap();
        Client {
            s,
            key: (0, 0),
            task,
        }
    }

    async fn connect_with(&self, startup: Vec<u8>) -> Client {
        let mut client = self.connect_raw(startup).await;
        for f in client.until_ready().await {
            match f[0] {
                b'K' => {
                    client.key = (
                        u32::from_be_bytes(f[5..9].try_into().unwrap()),
                        u32::from_be_bytes(f[9..13].try_into().unwrap()),
                    );
                }
                b'R' | b'S' | b'Z' => {}
                other => panic!("unexpected startup frame {}", other as char),
            }
        }
        assert_ne!(client.key, (0, 0), "the BackendKeyData was relayed");
        client
    }

    fn stats(&self, tenant: &str) -> TenantBudgetStats {
        self.state
            .tenant_slots
            .budget_stats()
            .into_iter()
            .find(|s| s.tenant == tenant)
            .expect("the tenant has a budget")
    }

    /// Wait until `cond` holds for the tenant's stats.
    async fn until(&self, tenant: &str, what: &str, cond: impl Fn(&TenantBudgetStats) -> bool) {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        loop {
            let s = self.stats(tenant);
            if cond(&s) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}: {s:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The tags of the messages backend connection `pid` received before it
    /// was first reset for the pool (all of them without a reset).
    fn tags_before_reset(&self, pid: u32) -> Vec<u8> {
        self.backend
            .frames_of(pid)
            .iter()
            .take_while(|s| !(s.tag == b'Q' && s.text == "DISCARD ALL"))
            .map(|s| s.tag)
            .collect()
    }

    /// Wait until the backend has seen `sql`.
    async fn until_backend_saw(&self, sql: &str) {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        while !self.backend.saw_query(sql) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the backend never saw {sql:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[cfg(feature = "query-cache")]
    async fn until_scope_probes(&self, count: usize) -> Vec<Seen> {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        loop {
            let probes: Vec<_> = self
                .backend
                .log()
                .into_iter()
                .filter(|s| s.tag == b'Q' && s.text == "SHOW ALL")
                .collect();
            if probes.len() >= count {
                return probes;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "only {} scope probes arrived; expected {count}",
                probes.len()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Wait until backend connection `pid` has received a message `tag`.
    async fn until_conn_got(&self, pid: u32, tag: u8) {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        while !self.backend.frames_of(pid).iter().any(|s| s.tag == tag) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "backend connection {pid} never received {:?}",
                tag as char
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// CancelRequests the backend received.
    fn backend_cancels(&self) -> usize {
        self.backend.log().iter().filter(|s| s.tag == b'!').count()
    }

    /// Wait until the backend has received `n` CancelRequests.
    async fn until_backend_cancels(&self, n: usize) {
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        while self.backend_cancels() < n {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the backend never received cancel {n}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Start a held query on `client` (it holds the tenant's permit until
    /// `release_slow`).
    async fn hold_permit(&self, client: &mut Client, sql: &str) {
        client.send(&query(sql)).await;
        self.until_backend_saw(sql).await;
    }

    /// Send a CancelRequest for `key` on a connection of its own, as a
    /// client does; it is never answered.
    async fn cancel(&self, key: (u32, u32)) {
        let (proxy, mut s) = pair().await;
        let peer = proxy.peer_addr().unwrap();
        let task = tokio::spawn(ProxyServer::handle_client(
            proxy,
            peer,
            self.state.clone(),
            Arc::new(self.config.clone()),
            self.server.shutdown_tx.clone(),
        ));
        s.write_all(&cancel_bytes(key.0, key.1)).await.unwrap();
        let mut rest = Vec::new();
        s.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty(), "a cancel is never answered");
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn a_query_over_the_tenant_cap_is_refused_53300_and_the_session_continues() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    let mut o = h.connect("other").await;
    h.hold_permit(&mut a, "select 'slow a'").await;

    let refused = b.query("select 'b'").await;
    assert_refused(&refused, "53300", REFUSED);
    assert!(!h.backend.saw_query("select 'b'"), "never forwarded");
    // Another tenant is served meanwhile.
    assert_served(&o.query("select 'o'").await, b'I');

    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    // The refused session stays usable.
    assert_served(&b.query("select 'b'").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    let s = h.stats("acme");
    assert_eq!((s.query_refusals, s.query_waits), (1, 0));
}

#[tokio::test]
async fn a_transaction_holds_its_tenant_permit_until_it_ends() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    h.until("acme", "an idle holder", |s| {
        s.queries_idle == 1 && s.queries_active == 1
    })
    .await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    // The transaction's own statements never wait.
    assert_served(&a.query("select 1").await, b'T');
    assert_served(&a.query("COMMIT").await, b'I');
    h.until("acme", "the permit returned", |s| {
        s.queries_idle == 0 && s.queries_active == 0
    })
    .await;
    assert_served(&b.query("select 'b'").await, b'I');
    assert_eq!(h.stats("acme").query_waits, 0);
}

#[tokio::test]
async fn a_refused_extended_cycle_is_discarded_up_to_its_sync() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;

    // The cycle's first message is refused; PostgreSQL flushes the error.
    b.send(&[parse("s1", "select 1"), flush()].concat()).await;
    let err = b.frame().await;
    assert_eq!(sqlstate(&err), "53300");
    assert!(has_text(&err, REFUSED));
    b.quiet().await;
    // Everything up to Sync is ignored (a Query included); Sync gets 'I'.
    b.send(&[bind("s1"), execute(), query("select 2"), sync()].concat())
        .await;
    assert_eq!(b.frame().await, wire(b'Z', b"I"));
    b.quiet().await;
    assert!(
        h.backend.frames_of(b.key.0).is_empty(),
        "the backend never saw the cycle: {:?}",
        h.backend.frames_of(b.key.0)
    );

    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    b.send(&[parse("s1", "select 1"), bind("s1"), execute(), sync()].concat())
        .await;
    let frames = b.until_ready().await;
    assert_eq!(
        frames.iter().map(|f| f[0]).collect::<Vec<_>>(),
        b"12CZ".to_vec()
    );
}

/// PostgreSQL destroys the unnamed statement before it parses an unnamed
/// Parse, so a cycle refused at the cap (53300) or cancelled while waiting
/// (57014) at one leaves no unnamed statement: a later cycle that Binds it
/// without a Parse of its own (a pipelined prepare-only cycle, then its
/// execution) fails 26000. It never runs the older statement the backend
/// connection still holds with the new parameters: the proxy closes that one
/// first. With and without the unnamed-Parse promotion.
#[tokio::test]
async fn a_cycle_refused_at_an_unnamed_parse_leaves_no_unnamed_statement() {
    for promote in [true, false] {
        for cancel in [false, true] {
            let shape = format!("promotion {promote}, cancel {cancel}");
            let h = Harness::new(if cancel { 5000 } else { 0 }, None, |c| {
                c.optimize_unnamed_parse = promote;
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            // A's backend connection holds an unnamed DELETE.
            a.send(&[cycle("delete from t where id = $1"), sync()].concat())
                .await;
            assert_eq!(tags(&a.until_ready().await), b"12CZ", "{shape}");
            h.until("acme", "A's permit returned", |s| s.queries_active == 0)
                .await;
            h.hold_permit(&mut b, "select 'slow b'").await;

            a.send(
                &[
                    parse("", "select * from t where id = $1"),
                    describe_statement(""),
                    sync(),
                ]
                .concat(),
            )
            .await;
            if cancel {
                h.until("acme", "A waiting", |s| s.queries_waiting == 1)
                    .await;
                h.cancel(a.key).await;
                assert_refused(
                    &a.until_ready().await,
                    "57014",
                    "canceling statement due to user request",
                );
            } else {
                assert_refused(&a.until_ready().await, "53300", REFUSED);
            }
            h.backend.release_slow();
            assert_served(&b.until_ready().await, b'I');
            h.until("acme", "the permit returned", |s| s.queries_active == 0)
                .await;

            a.send(&[bind(""), execute(), sync()].concat()).await;
            let reply = a.until_ready().await;
            assert_eq!(tags(&reply), b"EZ", "{shape}: {reply:?}");
            assert_eq!(sqlstate(&reply[0]), "26000", "{shape}");
            assert_eq!(reply[1], wire(b'Z', b"I"), "{shape}");
            // The refused cycle never reached the backend; the Bind found the
            // older statement closed.
            let seen = h.backend.frames_of(a.key.0);
            assert_eq!(tags_of(&seen), b"PBESCHBES".to_vec(), "{shape}: {seen:?}");
            assert_eq!(seen[4].text, "S", "{shape}: the unnamed statement");
        }
    }
}

/// A simple Query refused at the cap destroys the unnamed statement as one
/// PostgreSQL runs does: a Bind of it without a Parse fails 26000 rather than
/// run the statement an earlier cycle left on the backend connection. A
/// cycle whose first message Parses it again needs no Close.
#[tokio::test]
async fn a_query_refused_at_the_cap_leaves_no_unnamed_statement() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&[cycle("delete from t where id = $1"), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ");

    refuse_a_query_at_the_cap(&h, &mut a, &mut b).await;
    a.send(&[bind(""), execute(), sync()].concat()).await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"EZ", "{reply:?}");
    assert_eq!(sqlstate(&reply[0]), "26000");

    refuse_a_query_at_the_cap(&h, &mut a, &mut b).await;
    a.send(&[cycle("select 2"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ");
    // ...and the statement it Parsed is the one a later Bind runs.
    a.send(&[bind(""), execute(), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"2CZ");
    assert_eq!(
        tags_of(&h.backend.frames_of(a.key.0)),
        b"PBESCHBESPBESBES".to_vec()
    );
    assert!(!h.backend.saw_query("select 'a'"));
}

/// `b` holds the tenant's only permit while `a`'s Query is refused (53300),
/// then returns it.
async fn refuse_a_query_at_the_cap(h: &Harness, a: &mut Client, b: &mut Client) {
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    h.hold_permit(b, "select 'slow b'").await;
    assert_refused(&a.query("select 'a'").await, "53300", REFUSED);
    h.backend.release_slow();
    assert_served(&b.until_ready().await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// A simple Query the backend runs destroys its unnamed statement, so an
/// unnamed Parse identical to the one before it reaches the backend again:
/// the unnamed-Parse promotion never skips it (the Bind would fail 26000
/// where PostgreSQL runs it). One right after it, with no Query between, is
/// still skipped.
#[tokio::test]
async fn an_identical_unnamed_parse_after_a_query_reaches_the_backend() {
    for pooled in poolings() {
        let h = Harness::new(0, None, |c| transaction_pooling(c, pooled)).await;
        let mut a = h.connect("acme").await;
        let read = [cycle("select v from t where id = $1"), sync()].concat();
        a.send(&read).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ", "pooled {pooled}");
        assert_served(&a.query("select 1").await, b'I');
        a.send(&read).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ", "pooled {pooled}");
        a.send(&read).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ", "pooled {pooled}");
        if !pooled {
            assert_eq!(
                tags_of(&h.backend.frames_of(a.key.0)),
                b"PBESQPBESBES".to_vec()
            );
        }
    }
}

/// Requests refused before any backend outside the tenant cap destroy the
/// unnamed statement too: an extended cycle an open circuit refuses at its
/// unnamed Parse, and a simple Query it refuses. A later Bind of the unnamed
/// statement without a Parse of its own fails 26000 rather than run the one
/// an earlier cycle left on the backend connection.
#[cfg(feature = "circuit-breaker")]
#[tokio::test]
async fn requests_an_open_circuit_refuses_leave_no_unnamed_statement() {
    for by_query in [false, true] {
        let h = Harness::new(0, None, |c| c.circuit_breaker.enabled = true).await;
        let node = h.backend.addr.to_string();
        let breakers = h.state.circuit_breaker.clone().expect("enabled");
        let mut a = h.connect("acme").await;
        a.send(&[cycle("delete from t where id = $1"), sync()].concat())
            .await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ");
        breakers.force_open(&node, None);
        let refused = if by_query {
            a.query("select 'refused'").await
        } else {
            a.send(
                &[
                    parse("", "select * from t where id = $1"),
                    describe_statement(""),
                    sync(),
                ]
                .concat(),
            )
            .await;
            a.until_ready().await
        };
        assert_eq!(tags(&refused), b"EZ", "query {by_query}: {refused:?}");
        breakers.force_close(&node, None);
        a.send(&[bind(""), execute(), sync()].concat()).await;
        let reply = a.until_ready().await;
        assert_eq!(tags(&reply), b"EZ", "query {by_query}: {reply:?}");
        assert_eq!(sqlstate(&reply[0]), "26000", "query {by_query}");
        assert_eq!(
            tags_of(&h.backend.frames_of(a.key.0)),
            b"PBESCHBES".to_vec(),
            "query {by_query}"
        );
    }
}

/// Inside a transaction block the unnamed portal outlives its cycle, and a
/// simple Query destroys it too: after one an open circuit refuses there, an
/// Execute of the unnamed portal without a Bind of its own fails 34000
/// rather than run the portal an earlier cycle bound.
#[cfg(feature = "circuit-breaker")]
#[tokio::test]
async fn a_query_refused_in_a_transaction_leaves_no_unnamed_portal() {
    let h = Harness::new(0, None, |c| c.circuit_breaker.enabled = true).await;
    let node = h.backend.addr.to_string();
    let breakers = h.state.circuit_breaker.clone().expect("enabled");
    let mut a = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    a.send(&[parse("", "insert into t values (1)"), bind(""), sync()].concat())
        .await;
    assert_eq!(tags(&a.until_ready().await), b"12Z");
    breakers.force_open(&node, None);
    let refused = a.query("select 'refused'").await;
    assert_eq!(tags(&refused), b"EZ", "{refused:?}");
    assert_eq!(refused[1], wire(b'Z', b"T"));
    breakers.force_close(&node, None);
    a.send(&[execute(), sync()].concat()).await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"EZ", "{reply:?}");
    assert_eq!(sqlstate(&reply[0]), "34000");
    assert_served(&a.query("ROLLBACK").await, b'I');
    let seen = h.backend.frames_of(a.key.0);
    assert_eq!(tags_of(&seen), b"QPBSCCHESQ".to_vec(), "{seen:?}");
    assert_eq!((seen[4].text.as_str(), seen[5].text.as_str()), ("S", "P"));
}

/// Under transaction pooling with `skip_clean_reset`, a connection that holds
/// an unnamed statement is reset before it is parked, with or without the
/// unnamed-Parse promotion that records it: parked as clean, the next session
/// leasing it could Bind that statement without a Parse of its own and run
/// it with its own parameters.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn a_connection_holding_an_unnamed_statement_is_never_parked_unreset() {
    for promote in [false, true] {
        let h = Harness::new(0, None, |c| {
            transaction_pooling(c, true);
            c.pool_mode.skip_clean_reset = true;
            c.optimize_unnamed_parse = promote;
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&[cycle("delete from t where id = $1"), sync()].concat())
            .await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ", "promotion {promote}");
        let ran_on = h
            .backend
            .log()
            .iter()
            .find(|s| s.tag == b'P')
            .map(|s| s.conn)
            .expect("the cycle reached a backend");
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        while !h
            .backend
            .frames_of(ran_on)
            .iter()
            .any(|s| s.tag == RESET_ANSWERED)
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "promotion {promote}: parked unreset: {:?}",
                h.backend.frames_of(ran_on)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // The connection the pool hands out next holds no unnamed statement.
        a.send(&[bind(""), execute(), sync()].concat()).await;
        let reply = a.until_ready().await;
        assert_eq!(tags(&reply), b"EZ", "promotion {promote}: {reply:?}");
        assert_eq!(sqlstate(&reply[0]), "26000", "promotion {promote}");
    }
}

#[tokio::test]
async fn a_partly_received_cycle_keeps_its_permit_across_a_query() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    // B's Parse opens a cycle (and takes the permit) that is not yet sent.
    b.send(&parse("s1", "select 1")).await;
    h.until("acme", "B's permit", |s| s.queries_active == 1)
        .await;
    // A Query in the middle of it is served; the cycle stays open.
    assert_served(&b.query("select 'q'").await, b'I');
    assert_refused(&a.query("select 'a'").await, "53300", REFUSED);
    assert_eq!(h.stats("acme").queries_active, 1);
    b.send(&[bind("s1"), execute(), sync()].concat()).await;
    let frames = b.until_ready().await;
    assert_eq!(
        frames.iter().map(|f| f[0]).collect::<Vec<_>>(),
        b"12CZ".to_vec()
    );
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&a.query("select 'a'").await, b'I');
}

#[tokio::test]
async fn a_bare_flush_at_idle_does_not_bypass_the_cap() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    let mut free = h.connect("nobody").await;
    h.hold_permit(&mut a, "select 'slow a'").await;

    // The Flush is consumed; the Query after it is gated as usual.
    b.send(&[flush(), query("BEGIN")].concat()).await;
    assert_refused(&b.until_ready().await, "53300", REFUSED);
    // Flush then a cycle: refused at its Parse, discarded to its Sync.
    b.send(&[flush(), parse("", "select 1"), bind(""), execute(), sync()].concat())
        .await;
    assert_refused(&b.until_ready().await, "53300", REFUSED);
    b.quiet().await;
    assert!(
        h.backend.frames_of(b.key.0).is_empty(),
        "nothing of B reached its backend: {:?}",
        h.backend.frames_of(b.key.0)
    );

    // A session without a budget consumes it too: it never reaches the
    // backend, and the query after it is served as a request of its own.
    free.send(&flush()).await;
    assert_served(&free.query("select 'free'").await, b'I');
    assert_eq!(
        tags_of(&h.backend.frames_of(free.key.0)),
        b"Q".to_vec(),
        "{:?}",
        h.backend.frames_of(free.key.0)
    );
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
}

/// A CopyDone or CopyFail outside a COPY is ignored, as PostgreSQL ignores
/// it: after an extended-protocol request it is not taken for the end of an
/// extended COPY, so the bare Flush after it is still consumed at idle and
/// the next cycle is gated (refused 53300 at the cap) instead of dispatched
/// without a permit; after a simple query the session does not wait for a
/// ReadyForQuery that never comes.
#[tokio::test]
async fn a_stray_copy_end_at_idle_is_ignored_and_opens_no_cycle() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    // A's last request is extended-protocol (a COPY after it would be too).
    a.send(&[cycle("select 1"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ");
    h.until("acme", "A's permit returned", |s| s.queries_active == 0)
        .await;
    h.hold_permit(&mut b, "select 'slow b'").await;

    for stray in [wire(b'c', &[]), wire(b'f', &cstring("stray"))] {
        a.send(&[stray, flush()].concat()).await;
        a.quiet().await;
        a.send(&[cycle("BEGIN"), sync()].concat()).await;
        assert_refused(&a.until_ready().await, "53300", REFUSED);
    }
    assert!(
        !h.backend
            .frames_of(a.key.0)
            .iter()
            .any(|s| s.tag == b'P' && s.text == "BEGIN"),
        "the cycle never reached the backend: {:?}",
        h.backend.frames_of(a.key.0)
    );
    h.backend.release_slow();
    assert_served(&b.until_ready().await, b'I');
    h.until("acme", "B's permit returned", |s| s.queries_active == 0)
        .await;

    // After a simple query a stray CopyDone is ignored too.
    assert_served(&a.query("select 'simple'").await, b'I');
    a.send(&wire(b'c', &[])).await;
    assert_served(&a.query("select 'after'").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_eq!(
        h.tags_before_reset(a.key.0),
        b"PBEScfQcQ".to_vec(),
        "{:?}",
        h.backend.frames_of(a.key.0)
    );
}

/// pgx's CopyFrom streams CopyData right after its `COPY ... FROM STDIN`
/// Query, without waiting for CopyInResponse, and sends CopyDone (or
/// CopyFail) even after an ErrorResponse. When the COPY fails at once (a
/// missing table) or is refused at the tenant's cap, those frames arrive
/// outside any COPY and are ignored, as PostgreSQL ignores them, also by a
/// session holding no backend connection (idle under transaction pooling,
/// or parked by the refusal's wait): the next query gets its own reply, not
/// an unsolicited 57000 + ReadyForQuery that would put every later reply on
/// the connection one query behind.
#[tokio::test]
async fn copy_frames_after_a_failed_or_refused_copy_are_ignored() {
    for pooled in poolings() {
        let shape = format!("transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| transaction_pooling(c, pooled)).await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;

        // The backend fails the COPY at once.
        a.send(
            &[
                query("COPY missing_table FROM STDIN"),
                wire(b'd', b"1\n"),
                wire(b'd', b"2\n"),
                wire(b'c', &[]),
            ]
            .concat(),
        )
        .await;
        let failed = a.until_ready().await;
        assert_eq!(tags(&failed), b"EZ", "{shape}: {failed:?}");
        assert_eq!(sqlstate(&failed[0]), "42P01", "{shape}");
        let next = a.query("select 'after the failed copy'").await;
        assert_eq!(tags(&next), b"CZ", "{shape}: {next:?}");
        assert_served(&next, b'I');
        a.quiet().await;
        h.until("acme", "A's permit returned", |s| s.queries_active == 0)
            .await;

        // The tenant gate refuses the COPY (its CopyFail and CopyDone alike).
        h.hold_permit(&mut b, "select 'slow b'").await;
        for end in [wire(b'f', &cstring("refused")), wire(b'c', &[])] {
            a.send(&[query("COPY t FROM STDIN"), wire(b'd', b"1\n"), end].concat())
                .await;
            assert_refused(&a.until_ready().await, "53300", REFUSED);
            a.quiet().await;
        }
        h.backend.release_slow();
        assert_served(&b.until_ready().await, b'I');
        h.until("acme", "B's permit returned", |s| s.queries_active == 0)
            .await;
        let next = a.query("select 'after the refused copy'").await;
        assert_eq!(tags(&next), b"CZ", "{shape}: {next:?}");
        a.quiet().await;
        assert!(
            !h.backend.saw_query("COPY t FROM STDIN"),
            "{shape}: the refused COPY never reached a backend"
        );
    }
}

#[tokio::test]
async fn a_queued_request_is_admitted_when_a_permit_frees() {
    let h = Harness::new(2000, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    assert!(!h.backend.saw_query("select 'b'"));
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    assert_served(&b.until_ready().await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    let s = h.stats("acme");
    assert_eq!(
        (s.query_waits, s.query_refusals, s.queries_waiting),
        (1, 0, 0)
    );
    assert_eq!(h.state.latency.tenant_query_wait.snapshot().count(), 1);
}

#[tokio::test]
async fn a_cancel_request_ends_a_permit_wait_with_57014() {
    let h = Harness::new(5000, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;

    // Simple protocol: 57014 + ReadyForQuery.
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    h.cancel(b.key).await;
    assert_refused(
        &b.until_ready().await,
        "57014",
        "canceling statement due to user request",
    );
    // Extended protocol: 57014 at once, the cycle discarded to its Sync.
    b.send(&[parse("", "select 'e'"), bind(""), execute(), sync()].concat())
        .await;
    h.until("acme", "B waiting again", |s| s.queries_waiting == 1)
        .await;
    h.cancel(b.key).await;
    assert_refused(
        &b.until_ready().await,
        "57014",
        "canceling statement due to user request",
    );
    b.quiet().await;

    let s = h.stats("acme");
    assert_eq!((s.query_wait_cancels, s.queries_waiting), (2, 0));
    // Both cancelled waits are recorded, like every wait.
    assert_eq!(
        h.state.latency.tenant_query_wait.snapshot().count(),
        s.query_waits
    );
    assert_eq!(s.query_waits, 2);
    assert!(!h.backend.saw_query("select 'b'"));
    assert!(
        h.backend.frames_of(b.key.0).is_empty(),
        "B's backend saw nothing"
    );
    assert!(
        !h.backend.log().iter().any(|s| s.tag == b'!'),
        "no cancel reached the backend"
    );
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
}

#[tokio::test]
async fn a_client_that_disconnects_while_waiting_is_never_forwarded() {
    let h = Harness::new(5000, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    let Client { s, task, .. } = b;
    drop(s);
    // B's session ends while A still holds the permit.
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("the waiting session ends with its client")
        .unwrap()
        .unwrap();
    let s = h.stats("acme");
    assert_eq!(
        (s.query_wait_cancels, s.queries_waiting, s.queries_active),
        (1, 0, 1)
    );
    // The abandoned wait is recorded, like every wait.
    assert_eq!(h.state.latency.tenant_query_wait.snapshot().count(), 1);
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    assert!(!h.backend.saw_query("select 'b'"));
}

/// A request that waits its whole `active_query_wait_ms` and is refused
/// (53300) is recorded in the wait histogram like an admitted one: the
/// histogram's count matches `query_waits`, and its sum shows the waits.
#[tokio::test]
async fn a_wait_refused_at_its_end_is_recorded() {
    let h = Harness::new(50, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    let s = h.stats("acme");
    assert_eq!((s.query_waits, s.query_refusals), (1, 1));
    let waits = h.state.latency.tenant_query_wait.snapshot();
    assert_eq!(waits.count(), 1);
    assert!(waits.sum_us >= 50_000, "{waits:?}");
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    assert!(!h.backend.saw_query("select 'b'"));
}

#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn a_waiting_session_parks_its_backend_connection() {
    let transaction_pooling = |c: &mut ProxyConfig| {
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    };
    for wait_ms in [2000, 0] {
        let h = Harness::new(wait_ms, None, transaction_pooling).await;
        let pool = h.state.backend_pool.clone().expect("transaction pooling");
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        // A holds the permit in a transaction (its connection stays pinned).
        assert_served(&a.query("BEGIN").await, b'T');
        b.send(&query("select 'b'")).await;
        if wait_ms > 0 {
            h.until("acme", "B waiting", |s| s.queries_waiting == 1)
                .await;
        } else {
            assert_refused(&b.until_ready().await, "53300", REFUSED);
        }
        // B parked its idle startup connection before waiting (or being
        // refused): it holds no backend connection.
        assert!(h
            .backend
            .frames_of(b.key.0)
            .iter()
            .any(|s| s.tag == b'Q' && s.text == "DISCARD ALL"));
        assert_eq!(pool.idle_count(), 1);
        assert_served(&a.query("COMMIT").await, b'I');
        if wait_ms > 0 {
            assert_served(&b.until_ready().await, b'I');
        } else {
            h.until("acme", "the permit returned", |s| s.queries_active == 0)
                .await;
            assert_served(&b.query("select 'b'").await, b'I');
        }
    }
}

#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn the_permit_is_released_after_the_connection_is_parked() {
    let h = Harness::new(5000, Some(Duration::from_millis(200)), |c| {
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    assert_served(&b.until_ready().await, b'I');
    // A's reset may still be in flight if the order were wrong: wait for it.
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while !h
        .backend
        .frames_of(a.key.0)
        .iter()
        .any(|s| s.tag == RESET_ANSWERED)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "A's connection was never reset for the pool"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let log = h.backend.log();
    let a_parked = log
        .iter()
        .filter(|s| s.conn == a.key.0 && s.tag == RESET_ANSWERED)
        .map(|s| s.at)
        .max()
        .expect("A's connection was reset for the pool");
    let b_sent = log
        .iter()
        .find(|s| s.tag == b'Q' && s.text == "select 'b'")
        .map(|s| s.at)
        .expect("B's query reached a backend");
    assert!(
        b_sent >= a_parked,
        "B's query was sent {:?} before A's connection was parked",
        a_parked - b_sent
    );
}

/// The extended protocol's release, the Sync that leaves the session idle,
/// parks the connection before it returns the tenant permit too (under
/// transaction pooling, with a slow reset): the waiter that permit wakes
/// takes that connection from the pool (the one parked last, not the one it
/// parked itself before waiting), once its reset was answered.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn an_extended_sync_parks_the_connection_before_returning_the_permit() {
    let h = Harness::new(5000, Some(Duration::from_millis(200)), |c| {
        transaction_pooling(c, true);
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&[cycle("select 'slow a'"), sync()].concat()).await;
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while !h.backend.frames_of(a.key.0).iter().any(|s| s.tag == b'E') {
        assert!(
            tokio::time::Instant::now() < deadline,
            "A's Execute never reached its backend"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    h.backend.release_slow();
    assert_eq!(tags(&a.until_ready().await), b"12CZ");
    assert_served(&b.until_ready().await, b'I');
    // The log is in arrival order: A's Sync, the reset that parked A's
    // connection, then B's query on that connection.
    let log = h.backend.log();
    let at = |what: &dyn Fn(&Seen) -> bool| log.iter().position(what);
    let b_query =
        at(&|s| s.tag == b'Q' && s.text == "select 'b'").expect("B's query reached a backend");
    assert_eq!(
        log[b_query].conn, a.key.0,
        "B took A's connection, parked at its Sync: {log:?}"
    );
    let a_sync = at(&|s| s.conn == a.key.0 && s.tag == b'S').expect("A's Sync");
    let a_parked = log
        .iter()
        .skip(a_sync)
        .position(|s| s.conn == a.key.0 && s.tag == RESET_ANSWERED)
        .map(|i| a_sync + i)
        .expect("A's connection was reset for the pool");
    assert!(a_parked < b_query, "{log:?}");
}

#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn a_cancel_while_the_session_parks_its_connections_ends_the_wait() {
    // Transaction pooling with a slow reset: B's slow path is still parking
    // its startup connection when the cancel arrives.
    let h = Harness::new(5000, Some(Duration::from_millis(300)), |c| {
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&query("select 'b'")).await;
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while !h
        .backend
        .frames_of(b.key.0)
        .iter()
        .any(|s| s.tag == b'Q' && s.text == "DISCARD ALL")
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "B never parked its connection"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        !h.backend
            .frames_of(b.key.0)
            .iter()
            .any(|s| s.tag == RESET_ANSWERED),
        "the reset is still in flight"
    );
    h.cancel(b.key).await;
    assert_refused(
        &b.until_ready().await,
        "57014",
        "canceling statement due to user request",
    );
    // The cancel ended the request: it reached no backend (neither the
    // connection being reset nor one parked for another session), and B
    // never queued at the cap.
    assert!(
        !h.backend.log().iter().any(|s| s.tag == b'!'),
        "no cancel reached the backend"
    );
    let s = h.stats("acme");
    assert_eq!(
        (s.query_wait_cancels, s.query_waits, s.queries_waiting),
        (1, 0, 0)
    );
    assert!(
        h.backend
            .frames_of(b.key.0)
            .iter()
            .any(|s| s.tag == RESET_ANSWERED),
        "B's connection was reset and parked"
    );
    assert!(!h.backend.saw_query("select 'b'"));
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
}

#[tokio::test]
async fn an_advisory_lock_holder_keeps_its_permit() {
    // Session pooling: PostgreSQL keeps the lock after ReadyForQuery 'I'.
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
    h.until("acme", "A idle holding the permit", |s| {
        s.queries_active == 1 && s.queries_idle == 1
    })
    .await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    // A single unlock is not tracked: A keeps the permit (conservative).
    assert_served(&a.query("SELECT pg_advisory_unlock(1)").await, b'I');
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(&a.query("SELECT pg_advisory_unlock_all()").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');

    // Transaction pooling resets the connection at every park: a lock
    // statement does not pin the permit.
    #[cfg(feature = "pool-modes")]
    {
        let h = Harness::new(0, None, |c| {
            c.pool_mode.mode = crate::config::PoolingMode::Transaction;
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
}

#[tokio::test]
async fn a_prepared_advisory_lock_rerun_without_parse_keeps_the_permit() {
    // Session pooling: a driver Parses a named lock statement once and
    // re-runs it with Bind/Execute/Sync alone.
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    let lock = [
        parse("lk", "SELECT pg_advisory_lock($1)"),
        bind("lk"),
        execute(),
        sync(),
    ]
    .concat();
    a.send(&lock).await;
    assert_served(&a.until_ready().await, b'I');
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(&a.query("SELECT pg_advisory_unlock_all()").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');

    // The cached statement takes the lock again: A keeps its permit.
    a.send(&[bind("lk"), execute(), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "A idle holding the permit", |s| {
        s.queries_active == 1 && s.queries_idle == 1
    })
    .await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(&a.query("SELECT pg_advisory_unlock_all()").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
}

/// Only a statement naming one of the functions that take a session-level
/// advisory lock pins its session's permit (session pooling). Text that
/// merely contains `advisory_lock` (a table `job_advisory_locks`, a column,
/// a literal, a comment), over the simple protocol or the extended one
/// (where nothing ever clears the mark), leaves the session idle and the
/// permit free for the tenant's next request. A `DO` block whose body names
/// one takes the lock (conservative), and a literal naming
/// `pg_advisory_unlock_all()` releases nothing; its call does.
#[tokio::test]
async fn only_a_statement_naming_a_lock_function_pins_the_permit() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    let benign = [
        "SELECT * FROM job_advisory_locks",
        "SELECT id FROM jobs WHERE kind = 'advisory_lock'",
        "SELECT 1 /* takes no advisory_lock */",
        "SELECT advisory_lock_id FROM my_pg_advisory_locks",
    ];
    for sql in benign {
        assert_served(&a.query(sql).await, b'I');
        h.until("acme", sql, |s| s.queries_active == 0).await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
    // A named statement, Parsed once and re-run with Bind alone.
    for (i, sql) in benign.iter().enumerate() {
        let name = format!("s{i}");
        a.send(&[parse(&name, sql), bind(&name), execute(), sync()].concat())
            .await;
        assert_served(&a.until_ready().await, b'I');
        a.send(&[bind(&name), execute(), sync()].concat()).await;
        assert_served(&a.until_ready().await, b'I');
        h.until("acme", sql, |s| s.queries_active == 0).await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
    // A DO block is refused under row isolation (dynamic SQL), so the lock is
    // taken by a plain statement.
    assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
    h.until("acme", "A idle holding the permit", |s| {
        s.queries_active == 1 && s.queries_idle == 1
    })
    .await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(&a.query("SELECT 'pg_advisory_unlock_all()'").await, b'I');
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(
        &a.query("SELECT pg_catalog.pg_advisory_unlock_all()").await,
        b'I',
    );
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
}

/// A `pg_advisory_unlock_all()` or `DISCARD ALL` answered before it reached
/// a backend (an empty rate-limit bucket, an open circuit) released no lock:
/// the holder keeps its permit, and the next session of the tenant is still
/// refused, never left blocked on the lock in the backend. Only an unlock the
/// backend answers returns the permit.
#[cfg(any(feature = "rate-limiting", feature = "circuit-breaker"))]
#[tokio::test]
async fn an_unlock_refused_before_the_backend_keeps_the_lock_holders_permit() {
    for unlock in ["SELECT pg_advisory_unlock_all()", "DISCARD ALL"] {
        #[cfg(feature = "rate-limiting")]
        {
            let h = Harness::new(0, None, |c| {
                c.rate_limit.enabled = true;
                c.rate_limit.default_qps = 1;
                c.rate_limit.default_burst = 1;
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            // The lock takes the bucket's only token: the unlock is refused.
            assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
            let refused = a.query(unlock).await;
            assert_eq!(tags(&refused), b"EZ", "{unlock}");
            assert_eq!(sqlstate(&refused[0]), "53400", "{unlock}");
            assert!(!h.backend.saw_query(unlock), "{unlock}");
            h.until("acme", "A idle holding the permit", |s| {
                s.queries_active == 1 && s.queries_idle == 1
            })
            .await;
            assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
        }
        #[cfg(feature = "circuit-breaker")]
        {
            let h = Harness::new(0, None, |c| c.circuit_breaker.enabled = true).await;
            let node = h.backend.addr.to_string();
            let breakers = h.state.circuit_breaker.clone().expect("enabled");
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
            breakers.force_open(&node, None);
            let refused = a.query(unlock).await;
            assert_eq!(tags(&refused), b"EZ", "{unlock}");
            assert_eq!(sqlstate(&refused[0]), "08006", "{unlock}");
            assert!(!h.backend.saw_query(unlock), "{unlock}");
            h.until("acme", "A idle holding the permit", |s| {
                s.queries_active == 1 && s.queries_idle == 1
            })
            .await;
            assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
            // The backend answers the next unlock: the permit returns.
            breakers.force_close(&node, None);
            assert_served(&a.query(unlock).await, b'I');
            h.until("acme", "the permit returned", |s| s.queries_active == 0)
                .await;
            assert_served(&b.query("select 'b'").await, b'I');
        }
    }
}

/// A lock statement whose backend died while it was in flight, under
/// `tr_mode = session`: its outcome is unknown, so the failover answers it
/// 08007 on a replacement without running it there, and whatever lock it
/// took died with the lost connection. The session holds no lock: idle, it
/// returns its permit, and the tenant's next session is served. The same
/// over the extended protocol.
#[tokio::test]
async fn a_lock_statement_the_failover_answered_does_not_pin_the_permit() {
    for extended in [false, true] {
        let h = Harness::new(0, None, |c| c.tr_mode = crate::config::TrMode::Session).await;
        let healer = keep_healthy(&h);
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        let lock = "SELECT pg_advisory_lock(7) /* crash */";
        let failed = if extended {
            a.send(&[cycle(lock), sync()].concat()).await;
            a.until_ready().await
        } else {
            a.query(lock).await
        };
        // Whatever the backend answered before it died (the extended
        // cycle's ParseComplete and BindComplete, as far as they arrived)
        // was relayed first.
        let t = tags(&failed);
        assert!(t.len() >= 2, "extended {extended}: {failed:?}");
        let (answered, end) = t.split_at(t.len() - 2);
        assert_eq!(end, b"EZ", "extended {extended}: {failed:?}");
        assert!(
            answered
                .iter()
                .all(|t| extended && matches!(t, b'1' | b'2')),
            "extended {extended}: {failed:?}"
        );
        let error = &failed[failed.len() - 2];
        assert_eq!(sqlstate(error), "08007", "extended {extended}: {error:?}");
        assert_eq!(
            failed.last().unwrap(),
            &wire(b'Z', b"I"),
            "extended {extended}"
        );
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        assert_served(&b.query("select 'b'").await, b'I');
        // A goes on, on the replacement connection.
        assert_served(&a.query("select 'a'").await, b'I');
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
        healer.abort();
    }
}

/// Stand in for the health checker: the in-band failure of a statement in
/// flight demotes the only node, and the failover waits for a healthy
/// primary. Marks the node healthy again whenever it is not, until aborted.
fn keep_healthy(h: &Harness) -> JoinHandle<()> {
    let state = h.state.clone();
    let node = h.backend.addr.to_string();
    tokio::spawn(async move {
        loop {
            if !state
                .health
                .load_full()
                .get(&node)
                .is_some_and(|n| n.healthy)
            {
                set_healthy(&state, &node, true);
                ProxyServer::publish_routing(&state);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
}

/// A lock holder whose backend connection died while it idled lost its locks
/// with that connection: the idle watch sees the loss and the permit returns
/// at once, while its client stays silent, not at its next request. Another
/// session of the tenant is served meanwhile, and `idle_permit_timeout_ms`
/// never ends the holder, which holds nothing. Its next request is told the
/// locks are gone and its session ends (`session_bound`): 08006 naming them
/// under `tr_mode = session`, 57P01 under `none`; it is not quietly run on a
/// replacement as if it still held them.
#[tokio::test]
async fn a_lock_holder_whose_backend_died_idle_returns_its_permit_at_once() {
    for mode in [crate::config::TrMode::Session, crate::config::TrMode::None] {
        let h = Harness::new(0, None, |c| {
            c.tr_mode = mode;
            c.multi_tenancy.idle_permit_timeout_ms = 300;
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
        h.until("acme", "A idle holding the permit", |s| {
            s.queries_active == 1 && s.queries_idle == 1
        })
        .await;
        h.backend.kill(a.key.0);
        // The idle watch sees the connection close: A, silent, returns it.
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        assert_served(&b.query("select 'b'").await, b'I');
        // Past the idle-permit timeout, A is still there, never told 57P05.
        tokio::time::sleep(Duration::from_millis(400)).await;
        a.quiet().await;
        assert_eq!(h.stats("acme").idle_permit_timeouts, 0, "{mode:?}");
        a.send(&wire(b'Q', b"select 'a'\0")).await;
        let refusal = a.frame().await;
        let (code, text) = match mode {
            crate::config::TrMode::None => ("57P01", "closing connection"),
            _ => ("08006", "session-level advisory locks"),
        };
        assert_eq!(sqlstate(&refusal), code, "{mode:?}: {refusal:?}");
        assert!(has_text(&refusal, text), "{mode:?}: {refusal:?}");
        assert!(
            !h.backend.saw_query("select 'a'"),
            "{mode:?}: not run on a replacement"
        );
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
}

/// A transaction replay re-runs the transaction's lock statement on the
/// replacement, and PostgreSQL keeps a session-level lock after the
/// transaction ends: the replacement holds it, so the session, idle after
/// its COMMIT, keeps its permit until it unlocks there.
#[tokio::test]
async fn a_lock_a_replayed_transaction_took_again_keeps_the_permit() {
    let h = Harness::new(0, None, |c| {
        c.tr_mode = crate::config::TrMode::Transaction;
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'T');
    h.backend.kill(a.key.0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_served(&a.query("select 'a'").await, b'T');
    let replacement = h
        .backend
        .log()
        .iter()
        .find(|s| s.tag == b'Q' && s.text == "select 'a'")
        .map(|s| s.conn)
        .expect("re-run on a replacement");
    assert_ne!(replacement, a.key.0);
    assert!(
        h.backend
            .frames_of(replacement)
            .iter()
            .any(|s| s.tag == b'Q' && s.text == "SELECT pg_advisory_lock(1)"),
        "the replay took the lock again: {:?}",
        h.backend.frames_of(replacement)
    );
    assert_served(&a.query("COMMIT").await, b'I');
    h.until("acme", "A idle holding the permit", |s| {
        s.queries_active == 1 && s.queries_idle == 1
    })
    .await;
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    assert_served(&a.query("SELECT pg_advisory_unlock_all()").await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
}

#[tokio::test]
async fn commit_prepared_is_exempt_at_the_cap() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    assert_served(&b.query("COMMIT PREPARED 'x'").await, b'I');
    assert!(h.backend.saw_query("COMMIT PREPARED 'x'"));
    assert_served(&b.query("ROLLBACK PREPARED 'y';").await, b'I');
    assert_refused(&b.query("SELECT 1").await, "53300", REFUSED);
    // Only a single statement is exempt.
    assert_refused(
        &b.query("COMMIT PREPARED 'x'; SELECT 1").await,
        "53300",
        REFUSED,
    );
    assert_eq!(h.stats("acme").queries_active, 1);
    assert_served(&a.query("COMMIT").await, b'I');
}

#[tokio::test]
async fn a_backend_lost_during_the_wait_is_retried_as_not_delivered() {
    // Session pooling, tr_mode session (the default).
    let h = Harness::new(5000, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    // B's backend connection dies while B waits at the gate: nothing
    // watches it then.
    h.backend.kill(b.key.0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    // The post-wait probe finds it gone: the request was not delivered and
    // is re-executed on a fresh connection, never answered 08007.
    let frames = b.until_ready().await;
    assert_served(&frames, b'I');
    let ran_on: Vec<u32> = h
        .backend
        .log()
        .iter()
        .filter(|s| s.tag == b'Q' && s.text == "select 'b'")
        .map(|s| s.conn)
        .collect();
    assert_eq!(ran_on.len(), 1, "{ran_on:?}");
    assert_ne!(ran_on[0], b.key.0, "it ran on a replacement connection");
}

#[tokio::test]
async fn an_idle_transaction_past_idle_permit_timeout_is_terminated_25p03() {
    let h = Harness::new(0, None, |c| {
        c.multi_tenancy.idle_permit_timeout_ms = 200;
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    let begun = tokio::time::Instant::now();
    assert_served(&a.query("BEGIN").await, b'T');
    let fatal = a.frame().await;
    assert!(begun.elapsed() >= Duration::from_millis(200));
    assert_eq!(sqlstate(&fatal), "25P03");
    assert!(has_text(&fatal, "SFATAL"));
    assert!(has_text(
        &fatal,
        "terminating connection due to idle-in-transaction timeout"
    ));
    let mut rest = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
        .await
        .expect("the connection closes")
        .unwrap();
    assert!(rest.is_empty());
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0
    })
    .await;
    assert_eq!(h.stats("acme").idle_permit_timeouts, 1);
    assert_served(&b.query("select 'b'").await, b'I');
    // A session that holds no permit is never terminated by it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_served(&b.query("select 'b2'").await, b'I');
    assert_eq!(h.stats("acme").idle_permit_timeouts, 1);
}

#[tokio::test]
async fn an_idle_advisory_lock_holder_past_idle_permit_timeout_is_terminated_57p05() {
    // Session pooling: the lock holder keeps its permit outside a
    // transaction, so the timeout ends it as an idle session.
    let h = Harness::new(0, None, |c| {
        c.multi_tenancy.idle_permit_timeout_ms = 200;
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    let locked = tokio::time::Instant::now();
    assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
    let fatal = a.frame().await;
    assert!(locked.elapsed() >= Duration::from_millis(200));
    assert_eq!(sqlstate(&fatal), "57P05");
    assert!(has_text(&fatal, "SFATAL"));
    assert!(has_text(
        &fatal,
        "terminating connection due to idle-session timeout"
    ));
    let mut rest = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
        .await
        .expect("the connection closes")
        .unwrap();
    assert!(rest.is_empty());
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0
    })
    .await;
    assert_eq!(h.stats("acme").idle_permit_timeouts, 1);
    assert_served(&b.query("select 'b'").await, b'I');
}

/// With `[limits] client_idle_timeout_secs` armed as well, the earlier of the
/// two deadlines ends a permit holder idle in a transaction: the permit's
/// with FATAL 25P03 (counted in `idle_permit_timeouts`), the client's with
/// 57P05 (`terminate_idle_session`, not counted). The permit returns either
/// way.
#[tokio::test]
async fn the_earlier_of_the_idle_and_idle_permit_deadlines_ends_the_session() {
    for (client_idle_secs, permit_ms, code, counted) in
        [(60, 200, "25P03", 1), (1, 4000, "57P05", 0)]
    {
        let shape = format!(
            "client_idle_timeout_secs {client_idle_secs}, idle_permit_timeout_ms {permit_ms}"
        );
        let h = Harness::new(0, None, |c| {
            c.limits.client_idle_timeout_secs = client_idle_secs;
            c.multi_tenancy.idle_permit_timeout_ms = permit_ms;
        })
        .await;
        let mut a = h.connect("acme").await;
        let begun = tokio::time::Instant::now();
        assert_served(&a.query("BEGIN").await, b'T');
        let fatal = a.frame().await;
        let first = Duration::from_millis(permit_ms).min(Duration::from_secs(client_idle_secs));
        assert!(begun.elapsed() >= first, "{shape}");
        assert_eq!(sqlstate(&fatal), code, "{shape}: {fatal:?}");
        assert!(has_text(&fatal, "SFATAL"), "{shape}");
        let mut rest = Vec::new();
        tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
            .await
            .expect("the connection closes")
            .unwrap();
        assert!(rest.is_empty(), "{shape}");
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        assert_eq!(h.stats("acme").idle_permit_timeouts, counted, "{shape}");
    }
}

#[tokio::test]
async fn a_copy_holds_its_permit_until_it_ends() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&query("COPY t FROM STDIN")).await;
    assert_eq!(a.frame().await[0], b'G', "CopyInResponse");
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
        .await;
    let frames = a.until_ready().await;
    assert_eq!(
        frames.iter().map(|f| f[0]).collect::<Vec<_>>(),
        b"CZ".to_vec()
    );
    assert_served(&frames, b'I');
    // The COPY's end returned the permit, not A's next request.
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
}

#[tokio::test]
async fn the_aborted_transaction_emulation_returns_the_permit_at_its_rollback() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    // A's backend dies with its transaction: the next statement fails and A
    // enters the aborted-transaction emulation (tr_mode session), still
    // holding its permit.
    h.backend.kill(a.key.0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let failed = a.query("select 'a'").await;
    assert_eq!(failed[0][0], b'E', "{failed:?}");
    assert_eq!(failed.last().unwrap(), &wire(b'Z', b"E"));
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    let aborted = a.query("select 'a2'").await;
    assert_eq!(sqlstate(&aborted[0]), "25P02");
    assert_eq!(aborted.last().unwrap(), &wire(b'Z', b"E"));
    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
    // The synthesized ROLLBACK ends the transaction and returns the permit.
    let frames = a.query("ROLLBACK").await;
    assert_eq!(
        frames.iter().map(|f| f[0]).collect::<Vec<_>>(),
        b"CZ".to_vec()
    );
    assert_served(&frames, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
    assert!(!h.backend.saw_query("ROLLBACK"), "answered by the proxy");
}

/// A Flush batch the failover answers with an error instead of re-running
/// it (its transaction died with a backend lost while the session was idle,
/// tr_mode session: 57P01, no ReadyForQuery) reached no backend, so it opens
/// no cycle on the replacement. Its client owes the Sync (not an idle
/// holder), which the aborted-transaction emulation answers 'E'; idle then
/// in its failed transaction, the session is an idle holder that
/// `idle_permit_timeout_ms` ends (25P03), returning the permit. A bare Flush
/// meanwhile is consumed (no 25P02 for it).
#[tokio::test]
async fn a_flush_batch_answered_by_a_failover_error_opens_no_cycle() {
    let h = Harness::new(0, None, |c| {
        c.tr_mode = crate::config::TrMode::Session;
        c.multi_tenancy.idle_permit_timeout_ms = 300;
    })
    .await;
    let mut a = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    h.backend.kill(a.key.0);
    // The idle watch sees the connection close.
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.send(&[cycle("select 1"), flush()].concat()).await;
    let failed = a.frame().await;
    assert_eq!(sqlstate(&failed), "57P01", "{failed:?}");
    // No ReadyForQuery for the Flush: the client owes its Sync.
    a.quiet().await;
    let s = h.stats("acme");
    assert_eq!((s.queries_active, s.queries_idle), (1, 0));

    a.send(&sync()).await;
    assert_eq!(a.until_ready().await, vec![wire(b'Z', b"E")]);
    h.until("acme", "an idle holder", |s| {
        s.queries_idle == 1 && s.queries_active == 1
    })
    .await;
    a.send(&flush()).await;
    let fatal = a.frame().await;
    assert_eq!(sqlstate(&fatal), "25P03", "{fatal:?}");
    assert!(has_text(&fatal, "SFATAL"));
    let mut rest = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
        .await
        .expect("the connection closes")
        .unwrap();
    assert!(rest.is_empty(), "{rest:?}");
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 1
    })
    .await;
    // Nothing of the batch reached a backend.
    assert!(
        !h.backend
            .log()
            .iter()
            .any(|s| matches!(s.tag, b'P' | b'B' | b'E' | b'H')),
        "{:?}",
        h.backend.log()
    );
}

#[tokio::test]
async fn a_session_that_ends_mid_transaction_returns_its_budgets() {
    let h = Harness::new(0, None, |c| {
        c.tr_mode = crate::config::TrMode::Transaction;
        c.multi_tenancy.max_replay_bytes_per_tenant = 1 << 20;
    })
    .await;
    let mut a = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    let big = format!("SELECT '{}'", "x".repeat(1024));
    assert_served(&a.query(&big).await, b'T');
    h.until("acme", "A's budgets in use", |s| {
        s.replay_bytes > 1024 && s.queries_active == 1
    })
    .await;
    let Client { s, task, .. } = a;
    drop(s);
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("the session ends with its client")
        .unwrap()
        .unwrap();
    let s = h.stats("acme");
    assert_eq!(
        (s.replay_bytes, s.queries_active, s.queries_idle),
        (0, 0, 0)
    );
}

/// The extended-protocol messages `Parse`/`Bind`/`Execute` of `sql` on the
/// unnamed statement and portal.
fn cycle(sql: &str) -> Vec<u8> {
    [parse("", sql), bind(""), execute()].concat()
}

/// The message tags of `frames`.
fn tags(frames: &[Vec<u8>]) -> Vec<u8> {
    frames.iter().map(|f| f[0]).collect()
}

/// The tags of the messages a fake backend received.
fn tags_of(seen: &[Seen]) -> Vec<u8> {
    seen.iter().map(|s| s.tag).collect()
}

/// Session pooling, then (in builds with pooling) transaction pooling.
fn poolings() -> Vec<bool> {
    if cfg!(feature = "pool-modes") {
        vec![false, true]
    } else {
        vec![false]
    }
}

/// Transaction pooling when `on`.
fn transaction_pooling(c: &mut ProxyConfig, on: bool) {
    if on {
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    }
}

/// One shape of a cycle a Flush dispatched part of, continued by a Sync or a
/// Query (`by_query`) while `refuse` stands (an open circuit, an empty
/// rate-limit bucket). The continuation is part of the request admitted at
/// the cycle's first batch: it is never refused, so the backend's own reply
/// settles the cycle, whether the flushed chunk failed (the backend skipped
/// to the Sync, which answers ReadyForQuery) or succeeded (the Sync commits
/// it, and the client is told so). The permit returns then and, under
/// transaction pooling, the connection is parked only after it. Returns A.
#[cfg(any(feature = "circuit-breaker", feature = "rate-limiting"))]
async fn continue_a_flushed_cycle(
    h: &Harness,
    shape: &str,
    chunk_fails: bool,
    by_query: bool,
    refuse: impl Fn(),
) -> Client {
    let mut a = h.connect("acme").await;
    let sql = if chunk_fails {
        "insert violates a unique key"
    } else {
        "insert into t values (1)"
    };
    a.send(&[cycle(sql), flush()].concat()).await;
    let flushed = a.frames(3).await;
    if chunk_fails {
        assert_eq!(tags(&flushed), b"12E", "{shape}");
        assert_eq!(sqlstate(&flushed[2]), "23505", "{shape}");
    } else {
        assert_eq!(tags(&flushed), b"12C", "{shape}");
    }
    h.until("acme", "A's permit", |s| s.queries_active == 1)
        .await;

    refuse();
    let reply = if by_query {
        a.query("select 'continues'").await
    } else {
        a.send(&sync()).await;
        a.until_ready().await
    };
    // The backend's own reply, no refusal.
    let expected: &[u8] = if by_query { b"CZ" } else { b"Z" };
    assert_eq!(tags(&reply), expected, "{shape}");
    assert_served(&reply, b'I');
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0
    })
    .await;
    let expected: &[u8] = if by_query { b"PBEHQ" } else { b"PBEHS" };
    assert_eq!(
        h.tags_before_reset(a.key.0),
        expected.to_vec(),
        "{shape}: {:?}",
        h.backend.frames_of(a.key.0)
    );
    a
}

/// The continuation shapes, `(transaction pooling, chunk fails, by query)`:
/// a Sync after a failed or a succeeded chunk, and a Query after a succeeded
/// one (PostgreSQL discards a Query while it skips to the Sync, so a client
/// sends the Sync there).
#[cfg(any(feature = "circuit-breaker", feature = "rate-limiting"))]
fn continuation_shapes() -> Vec<(bool, bool, bool)> {
    poolings()
        .into_iter()
        .flat_map(|pooled| {
            [(false, false), (true, false), (false, true)]
                .map(|(chunk_fails, by_query)| (pooled, chunk_fails, by_query))
        })
        .collect()
}

/// An open circuit never fast-fails a Sync or a Query that continues a cycle
/// a Flush dispatched part of (see `continue_a_flushed_cycle`); the next
/// request is a new one, and is fast-failed as before.
#[cfg(feature = "circuit-breaker")]
#[tokio::test]
async fn an_open_circuit_never_refuses_a_message_continuing_a_flushed_cycle() {
    for (pooled, chunk_fails, by_query) in continuation_shapes() {
        let shape = format!("fails {chunk_fails}, query {by_query}, transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| {
            c.circuit_breaker.enabled = true;
            transaction_pooling(c, pooled);
        })
        .await;
        let node = h.backend.addr.to_string();
        let breakers = h.state.circuit_breaker.clone().expect("enabled");
        let open = || breakers.force_open(&node, None);
        let mut a = continue_a_flushed_cycle(&h, &shape, chunk_fails, by_query, &open).await;
        // A new request is refused (nothing open on the backend now).
        open();
        let refused = a.query("select 'new'").await;
        assert_eq!(tags(&refused), b"EZ", "{shape}");
        assert_eq!(sqlstate(&refused[0]), "08006", "{shape}");
        assert!(!h.backend.saw_query("select 'new'"), "{shape}");
        // The backend skipped nothing of the next cycle.
        breakers.force_close(&node, None);
        a.send(&[cycle("select 2"), sync()].concat()).await;
        assert_eq!(tags(&a.until_ready().await), b"12CZ", "{shape}");
    }
}

/// The rate limiter never refuses a Sync or a Query that continues a cycle a
/// Flush dispatched part of: the cycle's first batch took the bucket's only
/// token (see `continue_a_flushed_cycle`). The Query, which runs a statement,
/// waits for the next token instead; the bare Sync is not charged.
#[cfg(feature = "rate-limiting")]
#[tokio::test]
async fn the_rate_limiter_never_refuses_a_message_continuing_a_flushed_cycle() {
    for (pooled, chunk_fails, by_query) in continuation_shapes() {
        let shape = format!("fails {chunk_fails}, query {by_query}, transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| {
            c.rate_limit.enabled = true;
            c.rate_limit.default_qps = 1;
            c.rate_limit.default_burst = 1;
            transaction_pooling(c, pooled);
        })
        .await;
        continue_a_flushed_cycle(&h, &shape, chunk_fails, by_query, || {}).await;
    }
}

/// A Query that continues a cycle a Flush dispatched part of is never
/// answered from the query cache, which would leave the cycle open behind a
/// reply the backend never gave: it reaches the backend, which settles it.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn a_query_continuing_a_flushed_cycle_is_not_answered_from_the_cache() {
    let h = Harness::new(0, None, |c| {
        c.cache.enabled = true;
        c.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        c.cache.trust_backend_read_effects = true;
    })
    .await;
    let mut a = h.connect("acme").await;
    let sql = "select v from t";
    let forwarded = || {
        h.backend
            .log()
            .iter()
            .filter(|s| s.tag == b'Q' && s.text == sql)
            .count()
    };
    assert_served(&a.query(sql).await, b'I');
    assert_served(&a.query(sql).await, b'I');
    assert_eq!(forwarded(), 1, "the second was a cache hit");
    // A read chunk: nothing it does invalidates the cached entry.
    a.send(&[cycle("select 1"), flush()].concat()).await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    assert_served(&a.query(sql).await, b'I');
    assert_eq!(forwarded(), 2, "the continuing query reached the backend");
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_eq!(
        h.tags_before_reset(a.key.0)
            .into_iter()
            .filter(|t| *t != b'Q')
            .collect::<Vec<_>>(),
        b"PBEH".to_vec()
    );
}

/// Scope verification is real backend work even for a cache hit. It holds
/// the tenant permit through the proof and pool cleanup, before the next
/// warm reader may start its own proof.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn delayed_cache_scope_probe_holds_tenant_admission_through_pool_cleanup() {
    const SQL: &str = "SELECT v FROM scope_tenant_fixture";
    for pooled in poolings() {
        let h = Harness::new(5000, Some(Duration::from_millis(50)), |c| {
            c.cache.enabled = true;
            c.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
            c.cache.trust_backend_read_effects = true;
            c.pool_mode.skip_clean_reset = false;
            // Every read proves: the probes below are counted.
            c.cache.proof_ttl_ms = 0;
            transaction_pooling(c, pooled);
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        let first = b.query(SQL).await;
        assert_eq!(tags(&first), b"TDCZ", "proof frames must remain internal");
        assert_eq!(b.query(SQL).await, first);
        h.until("acme", "warm reader idle after cleanup", |s| {
            s.queries_active == 0
        })
        .await;
        let cache = h.state.query_cache.as_ref().expect("enabled");
        let hits_before = cache.stats().total_hits;
        assert!(hits_before > 0, "a positive cache hit is required");
        let business_queries = || {
            h.backend
                .log()
                .iter()
                .filter(|s| s.tag == b'Q' && s.text == SQL)
                .count()
        };
        assert_eq!(business_queries(), 1, "the warm lookup must hit");
        let before = h.until_scope_probes(1).await.len();
        h.backend
            .ctl
            .hold_scope_probes
            .store(true, Ordering::Relaxed);
        a.send(&query(SQL)).await;
        let first_probe = h.until_scope_probes(before + 1).await[before].clone();
        assert_eq!(
            h.stats("acme").queries_active,
            1,
            "the delayed proof owns the permit"
        );
        b.send(&query(SQL)).await;
        h.until("acme", "second warm reader queued", |s| {
            s.queries_waiting == 1
        })
        .await;
        assert_eq!(
            h.until_scope_probes(before + 1).await.len(),
            before + 1,
            "a queued cache hit must not probe before admission"
        );
        assert_eq!(business_queries(), 1);

        h.backend.ctl.scope_probes.add_permits(1);
        assert_eq!(a.until_ready().await, first);
        let second_probe = h.until_scope_probes(before + 2).await[before + 1].clone();
        let budget = h.stats("acme");
        assert_eq!((budget.queries_active, budget.queries_waiting), (1, 0));
        assert_eq!(
            business_queries(),
            1,
            "both delayed readers must be cache hits"
        );
        if pooled {
            let reset = h
                .backend
                .log()
                .into_iter()
                .find(|s| {
                    s.conn == first_probe.conn && s.tag == RESET_ANSWERED && s.at > first_probe.at
                })
                .expect("the first hit's physical connection was reset");
            assert!(
                reset.at < second_probe.at,
                "pool cleanup precedes waiter admission"
            );
        }
        h.backend
            .ctl
            .hold_scope_probes
            .store(false, Ordering::Relaxed);
        h.backend.ctl.scope_probes.add_permits(1);
        assert_eq!(b.until_ready().await, first);
        h.until("acme", "all proof owners released after cleanup", |s| {
            s.queries_active == 0 && s.queries_waiting == 0
        })
        .await;
        assert_eq!(business_queries(), 1);
        assert_eq!(cache.stats().total_hits, hits_before + 2);
        assert_eq!(h.until_scope_probes(before + 2).await.len(), before + 2);
        #[cfg(feature = "pool-modes")]
        if pooled {
            assert!(
                h.state.backend_pool.as_ref().unwrap().idle_count() > 0,
                "the admitted waiter's hit returned its lease before its permit"
            );
        }
    }
}

/// The cache's refusal checks run only outside an open cycle: a query
/// continuing a flushed cycle is not classified, so naming a catalog relation
/// there counts no catalog skip, while the same read at idle counts one.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn a_query_continuing_a_flushed_cycle_counts_no_cache_skip() {
    let h = Harness::new(0, None, |c| {
        c.cache.enabled = true;
        c.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        c.cache.trust_backend_read_effects = true;
    })
    .await;
    let mut a = h.connect("acme").await;
    let sql = "select relname from pg_class";
    let skipped = || {
        h.state
            .metrics
            .cache_catalog_skipped
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    assert_served(&a.query(sql).await, b'I');
    assert_eq!(skipped(), 1, "a catalog read at idle is counted");
    a.send(&[cycle("select 1"), flush()].concat()).await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    assert_served(&a.query(sql).await, b'I');
    assert_eq!(skipped(), 1, "the continuing query was not classified");
}

/// A session that ends with an extended cycle still open on its backend
/// connection, its client gone after a Flush dispatched part of the cycle
/// (whose chunk succeeded, or failed and left the backend skipping to the
/// Sync) or after an extended COPY's end with its Sync still owed: under
/// transaction pooling that connection is closed with the session (the
/// backend rolls the cycle back), never reset into the cycle (which would
/// commit what the client never Synced, or wait out the reset's drain
/// timeout) nor parked. A session that ends idle still parks its connection.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn a_session_ending_with_a_cycle_open_closes_that_connection_unparked() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Open {
        Flushed,
        FlushedFailed,
        CopyEnded,
        Idle,
    }
    for open in [
        Open::Flushed,
        Open::FlushedFailed,
        Open::CopyEnded,
        Open::Idle,
    ] {
        let shape = format!("{open:?}");
        let h = Harness::new(0, None, |c| transaction_pooling(c, true)).await;
        let pool = h.state.backend_pool.clone().expect("transaction pooling");
        let mut a = h.connect("acme").await;
        let conn = a.key.0;
        let expected: &[u8] = match open {
            Open::Flushed => {
                a.send(&[cycle("insert into t values (1)"), flush()].concat())
                    .await;
                assert_eq!(tags(&a.frames(3).await), b"12C", "{shape}");
                b"PBEH"
            }
            Open::FlushedFailed => {
                a.send(&[cycle("insert violates a unique key"), flush()].concat())
                    .await;
                let flushed = a.frames(3).await;
                assert_eq!(tags(&flushed), b"12E", "{shape}");
                assert_eq!(sqlstate(&flushed[2]), "23505", "{shape}");
                b"PBEH"
            }
            Open::CopyEnded => {
                a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
                assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
                a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
                    .await;
                assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
                b"PBESdc"
            }
            // The control: nothing open, the startup connection is parked.
            Open::Idle => b"",
        };
        h.until("acme", "A's permit", |s| {
            s.queries_active == usize::from(open != Open::Idle)
        })
        .await;
        let Client { s, task, .. } = a;
        drop(s);
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("the session ends with its client")
            .unwrap()
            .unwrap();
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
        if open == Open::Idle {
            assert_eq!(pool.idle_count(), 1, "{shape}: parked at the session's end");
        } else {
            assert_eq!(
                tags_of(&h.backend.frames_of(conn)),
                expected.to_vec(),
                "{shape}: nothing reached the cycle's connection after it"
            );
            assert_eq!(pool.idle_count(), 0, "{shape}: not parked");
        }
    }
}

/// Write the plugin `name` (compiled from WAT) into `dir`. Its `hook` answers
/// `result` (the JSON that hook returns) for every call whose input, the
/// statement's JSON context, contains `needle`, and nothing (the hook's
/// default outcome) for any other; an empty `needle` matches every call. The
/// hooks read statements of both protocols, so a test that needs one
/// statement through and another answered tells them apart by their SQL.
/// Returns the path of the `.wasm`.
#[cfg(feature = "wasm-plugins")]
fn write_hook_plugin(
    dir: &std::path::Path,
    name: &str,
    hook: &str,
    needle: &str,
    result: &str,
) -> std::path::PathBuf {
    let escaped = |text: &str| -> String { text.bytes().map(|b| format!("\\{b:02x}")).collect() };
    let wat = format!(
        r#"(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 4096))
  (func (export "dealloc") (param i32) (param i32))
  (func $contains (param $ptr i32) (param $len i32) (result i32)
    (local $i i32) (local $j i32)
    (block $absent
      (loop $next
        (br_if $absent
          (i32.gt_u (i32.add (local.get $i) (i32.const {needle_len})) (local.get $len)))
        (local.set $j (i32.const 0))
        (block $differs
          (loop $same
            (if (i32.eq (local.get $j) (i32.const {needle_len}))
              (then (return (i32.const 1))))
            (br_if $differs
              (i32.ne
                (i32.load8_u
                  (i32.add (local.get $ptr) (i32.add (local.get $i) (local.get $j))))
                (i32.load8_u (i32.add (i32.const 2048) (local.get $j)))))
            (local.set $j (i32.add (local.get $j) (i32.const 1)))
            (br $same)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next)))
    (i32.const 0))
  (func (export "{hook}") (param i32) (param i32) (result i64)
    (if (result i64) (call $contains (local.get 0) (local.get 1))
      (then (i64.or (i64.shl (i64.const 1024) (i64.const 32)) (i64.const {result_len})))
      (else (i64.const 0))))
  (data (i32.const 1024) "{result}")
  (data (i32.const 2048) "{needle}"))"#,
        needle_len = needle.len(),
        result_len = result.len(),
        result = escaped(result),
        needle = escaped(needle),
    );
    assert!(
        result.len() <= 1024 && needle.len() <= 1024,
        "fixture too long"
    );
    let wasm = dir.join(format!("{name}.wasm"));
    std::fs::write(&wasm, wat::parse_str(&wat).expect("wat parses")).unwrap();
    std::fs::write(
        dir.join(format!("{name}.json")),
        format!(r#"{{"name": "{name}", "version": "1.0.0", "hooks": ["{hook}"]}}"#),
    )
    .unwrap();
    wasm
}

/// A directory for `[plugins] plugin_dir` holding one plugin whose `hook`
/// answers `result` for the statements whose context contains `needle`
/// ([`write_hook_plugin`]).
#[cfg(feature = "wasm-plugins")]
fn hook_plugin_dir_for(hook: &str, needle: &str, result: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_hook_plugin(dir.path(), "answer", hook, needle, result);
    dir
}

/// [`hook_plugin_dir_for`] answering every statement.
#[cfg(feature = "wasm-plugins")]
fn hook_plugin_dir(hook: &str, result: &str) -> tempfile::TempDir {
    hook_plugin_dir_for(hook, "", result)
}

/// [`hook_plugin_dir`] for the pre-query hook.
#[cfg(feature = "wasm-plugins")]
fn pre_query_plugin_dir(result: &str) -> tempfile::TempDir {
    hook_plugin_dir("pre_query", result)
}

/// Plugins from `dir` on.
#[cfg(feature = "wasm-plugins")]
fn plugins_from(c: &mut ProxyConfig, dir: &tempfile::TempDir) {
    c.plugins.enabled = true;
    c.plugins.plugin_dir = dir.path().to_string_lossy().into_owned();
}

/// A Query that continues a cycle a Flush dispatched part of, blocked by a
/// plugin, never reaches the backend: the cycle stays open there and the
/// session keeps its permit. Its client was answered ReadyForQuery and is
/// idle, so the session counts as an idle holder and
/// `idle_permit_timeout_ms` ends it (57P05), returning the permit; a bare
/// Flush sent meanwhile does not shield it. The session ends with the cycle
/// still open: under transaction pooling its connection is closed (the
/// backend rolls the cycle back), never reset into the cycle nor parked.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_blocked_query_after_a_flush_is_an_idle_holder_bounded_by_the_timeout() {
    let plugins = hook_plugin_dir_for(
        "pre_query",
        "blocked",
        r#"{"action": "block", "value": "not here"}"#,
    );
    for pooled in poolings() {
        let shape = format!("transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| {
            c.multi_tenancy.idle_permit_timeout_ms = 300;
            plugins_from(c, &plugins);
            transaction_pooling(c, pooled);
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&[cycle("insert into t values (1)"), flush()].concat())
            .await;
        assert_eq!(tags(&a.frames(3).await), b"12C", "{shape}");
        let conn = h
            .backend
            .log()
            .iter()
            .find(|s| s.tag == b'P')
            .expect("the cycle reached the backend")
            .conn;
        h.until("acme", "A's permit", |s| s.queries_active == 1)
            .await;
        // Mid-cycle (the client owes its Sync), the session is not idle.
        assert_eq!(h.stats("acme").queries_idle, 0, "{shape}");

        let blocked = a.query("select 'blocked'").await;
        assert_eq!(tags(&blocked), b"EZ", "{shape}");
        assert_eq!(sqlstate(&blocked[0]), "42000", "{shape}");
        h.until("acme", "an idle holder", |s| {
            s.queries_idle == 1 && s.queries_active == 1
        })
        .await;
        // A bare Flush is an idle one to the client: consumed, it neither
        // reaches the backend nor takes the session out of the idle count.
        a.send(&flush()).await;
        // The idle-permit timeout ends the session.
        assert_eq!(sqlstate(&a.frame().await), "57P05", "{shape}");
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 1
        })
        .await;
        assert!(!h.backend.saw_query("select 'blocked'"), "{shape}");
        let flushes = h.backend.log().iter().filter(|s| s.tag == b'H').count();
        assert_eq!(flushes, 1, "{shape}: {:?}", h.backend.log());
        let Client { task, .. } = a;
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("the session ends at its idle-permit timeout")
            .unwrap()
            .unwrap();
        assert_eq!(
            tags_of(&h.backend.frames_of(conn)),
            b"PBEH".to_vec(),
            "{shape}: nothing reached the cycle's connection after it"
        );
        #[cfg(feature = "pool-modes")]
        if pooled {
            let pool = h.state.backend_pool.clone().expect("transaction pooling");
            assert_eq!(pool.idle_count(), 0, "{shape}: not parked");
        }
    }
}

/// The cycle a plugin-blocked Query left open on the backend (its session
/// an idle holder) dies with that connection: the idle watch sees the loss
/// and the permit returns at once, while the client stays silent, not at its
/// next message; another session of the tenant is served meanwhile and
/// `idle_permit_timeout_ms` never ends the session, which holds nothing.
/// A later Query the plugin blocks again is answered before the tenant gate
/// and leaves no permit held either. Neither Query ever reaches a backend.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_blocked_cycle_whose_connection_died_idle_returns_its_permit_at_once() {
    let plugins = hook_plugin_dir_for(
        "pre_query",
        "blocked",
        r#"{"action": "block", "value": "not here"}"#,
    );
    for pooled in poolings() {
        let shape = format!("transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| {
            c.multi_tenancy.idle_permit_timeout_ms = 300;
            plugins_from(c, &plugins);
            transaction_pooling(c, pooled);
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        a.send(&[cycle("insert into t values (1)"), flush()].concat())
            .await;
        assert_eq!(tags(&a.frames(3).await), b"12C", "{shape}");
        let conn = h
            .backend
            .log()
            .iter()
            .find(|s| s.tag == b'P')
            .expect("the cycle reached the backend")
            .conn;
        let blocked = a.query("select 'blocked'").await;
        assert_eq!(tags(&blocked), b"EZ", "{shape}");
        h.until("acme", "an idle holder", |s| {
            s.queries_idle == 1 && s.queries_active == 1
        })
        .await;

        h.backend.kill(conn);
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        // B, whose statement the plugin lets through, is served.
        b.send(&[cycle("select 'b'"), sync()].concat()).await;
        let served = b.until_ready().await;
        assert_eq!(tags(&served), b"12CZ", "{shape}");
        assert_served(&served, b'I');
        // Blocked again (before the tenant gate), A holds no permit after it.
        let blocked = a.query("select 'blocked'").await;
        assert_eq!(tags(&blocked), b"EZ", "{shape}");
        assert_eq!(sqlstate(&blocked[0]), "42000", "{shape}");
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        b.send(&[cycle("select 'b'"), sync()].concat()).await;
        let served = b.until_ready().await;
        assert_eq!(tags(&served), b"12CZ", "{shape}");
        assert_served(&served, b'I');
        tokio::time::sleep(Duration::from_millis(400)).await;
        a.quiet().await;
        assert_eq!(h.stats("acme").idle_permit_timeouts, 0, "{shape}");
        assert!(!h.backend.saw_query("select 'blocked'"), "{shape}");
    }
}

/// A Query in place of the Sync an extended COPY's end awaits, blocked by a
/// route plugin (past the tenant gate), leaves that cycle open on the
/// backend and its session an idle holder. With `tr_mode = none` the
/// connection holding the cycle, lost while the session idles, is only
/// forgotten (no fault is carried into the next request): the permit
/// returns at once all the same, and a later Query the plugin blocks again
/// takes a permit and returns it at its end, the dead cycle keeping none.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_route_blocked_query_continuing_a_lost_cycle_returns_its_permit() {
    let plugins = hook_plugin_dir_for(
        "route",
        "blocked",
        r#"{"action": "block", "reason": "not here"}"#,
    );
    let h = Harness::new(0, None, |c| {
        c.tr_mode = crate::config::TrMode::None;
        plugins_from(c, &plugins);
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
    assert_eq!(tags(&a.frames(3).await), b"12G");
    a.send(&wire(b'd', b"1\n")).await;
    a.send(&wire(b'c', &[])).await;
    assert_eq!(tags(&a.frames(1).await), b"C");
    let blocked = a.query("select 'blocked'").await;
    assert_eq!(tags(&blocked), b"EZ");
    assert!(has_text(&blocked[0], "route plugin"), "{:?}", blocked[0]);
    h.until("acme", "an idle holder", |s| {
        s.queries_idle == 1 && s.queries_active == 1
    })
    .await;

    h.backend.kill(a.key.0);
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0
    })
    .await;
    // B, whose statement the route hook lets through, is served.
    b.send(&[cycle("select 'b'"), sync()].concat()).await;
    let served = b.until_ready().await;
    assert_eq!(tags(&served), b"12CZ");
    assert_served(&served, b'I');
    // Blocked again past the gate: A's permit returns at the Query's end.
    let blocked = a.query("select 'blocked'").await;
    assert_eq!(tags(&blocked), b"EZ");
    assert!(has_text(&blocked[0], "route plugin"), "{:?}", blocked[0]);
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0
    })
    .await;
    b.send(&[cycle("select 'b'"), sync()].concat()).await;
    let served = b.until_ready().await;
    assert_eq!(tags(&served), b"12CZ");
    assert_served(&served, b'I');
    assert!(!h.backend.saw_query("select 'blocked'"));
}

/// A Query that continues a cycle a Flush dispatched part of is not
/// answered by a pre-query plugin's cached result: it reaches the backend,
/// which settles the cycle. Outside such a cycle the plugin's answer is
/// served as before.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_query_continuing_a_flushed_cycle_skips_a_plugin_cached_answer() {
    let rows = br#"{"columns": [{"name": "v", "oid": 23}], "rows": [["1"]]}"#;
    let result = serde_json::json!({"action": "cached", "data": rows.to_vec()}).to_string();
    let plugins = pre_query_plugin_dir(&result);
    let h = Harness::new(0, None, |c| plugins_from(c, &plugins)).await;
    let mut a = h.connect("acme").await;
    let sql = "select v from t";
    let cached = a.query(sql).await;
    assert_eq!(tags(&cached), b"TDCZ");
    assert!(!h.backend.saw_query(sql), "the plugin answered it");
    a.send(&[cycle("insert into t values (1)"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    let reply = a.query(sql).await;
    assert_eq!(tags(&reply), b"CZ", "the backend's reply");
    assert_served(&reply, b'I');
    assert!(h.backend.saw_query(sql));
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// A Query that continues a cycle a Flush dispatched part of, blocked by a
/// route plugin (on the forward path, past the gates and caches that no
/// longer refuse such a query): it never reaches the backend, so the cycle
/// stays open there. The session keeps its permit and, under transaction
/// pooling, its connection, neither reset nor parked, and counts as an idle
/// holder; the next Sync reaches that same connection and settles the cycle,
/// and only then does the permit return.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_route_blocked_query_after_a_flush_keeps_the_cycle_and_its_permit() {
    let plugins = hook_plugin_dir_for(
        "route",
        "select 1",
        r#"{"action": "block", "reason": "not here"}"#,
    );
    for pooled in poolings() {
        let shape = format!("transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| {
            plugins_from(c, &plugins);
            transaction_pooling(c, pooled);
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&[cycle("insert into t values (1)"), flush()].concat())
            .await;
        assert_eq!(tags(&a.frames(3).await), b"12C", "{shape}");
        let conn = h
            .backend
            .log()
            .iter()
            .find(|s| s.tag == b'P')
            .expect("the cycle reached the backend")
            .conn;

        let blocked = a.query("select 1").await;
        assert_eq!(tags(&blocked), b"EZ", "{shape}");
        assert_eq!(sqlstate(&blocked[0]), "42000", "{shape}");
        assert!(has_text(&blocked[0], "route plugin"), "{shape}");
        assert_eq!(blocked[1], wire(b'Z', b"I"), "{shape}");
        h.until("acme", "an idle holder", |s| {
            s.queries_idle == 1 && s.queries_active == 1
        })
        .await;
        // Nothing reached the backend: neither the query nor a pool reset.
        assert!(
            !h.backend.log().iter().any(|s| s.tag == b'Q'),
            "{shape}: {:?}",
            h.backend.log()
        );

        a.send(&sync()).await;
        assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")], "{shape}");
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        assert_eq!(
            h.tags_before_reset(conn),
            b"PBEHS".to_vec(),
            "{shape}: {:?}",
            h.backend.log()
        );
    }
}

/// Open a transaction and have a plugin `Block` a Query in it (`text` names
/// which; the plugin answers only statements that contain `insert`). Nothing
/// reached the backend, whose transaction stays open: the Block's
/// ReadyForQuery reports 'T', never 'I', and the transaction keeps its
/// permit until its end returns it.
#[cfg(feature = "wasm-plugins")]
async fn a_plugin_block_in_a_transaction_reports_status_t(plugins: &tempfile::TempDir, text: &str) {
    let h = Harness::new(0, None, |c| plugins_from(c, plugins)).await;
    let mut a = h.connect("acme").await;
    a.send(&[cycle("BEGIN"), sync()].concat()).await;
    let begun = a.until_ready().await;
    assert_eq!(tags(&begun), b"12CZ", "{begun:?}");
    assert_eq!(begun[3], wire(b'Z', b"T"));
    let blocked = a.query("insert into t values (1)").await;
    assert_eq!(tags(&blocked), b"EZ", "{blocked:?}");
    assert_eq!(sqlstate(&blocked[0]), "42000");
    assert!(has_text(&blocked[0], text), "{:?}", blocked[0]);
    assert_eq!(
        blocked[1],
        wire(b'Z', b"T"),
        "the transaction is still open"
    );
    assert_eq!(h.stats("acme").queries_active, 1, "its permit is held");
    assert!(!h.backend.saw_query("insert into t values (1)"));
    a.send(&[cycle("COMMIT"), sync()].concat()).await;
    assert_eq!(a.until_ready().await.last().unwrap(), &wire(b'Z', b"I"));
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// [`a_plugin_block_in_a_transaction_reports_status_t`] for a route plugin.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_route_plugin_block_in_a_transaction_reports_status_t() {
    let plugins = hook_plugin_dir_for(
        "route",
        "insert",
        r#"{"action": "block", "reason": "not here"}"#,
    );
    a_plugin_block_in_a_transaction_reports_status_t(&plugins, "Query blocked by route plugin")
        .await;
}

/// [`a_plugin_block_in_a_transaction_reports_status_t`] for a pre-query
/// plugin.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_pre_query_plugin_block_in_a_transaction_reports_status_t() {
    let plugins = hook_plugin_dir_for(
        "pre_query",
        "insert",
        r#"{"action": "block", "value": "not here"}"#,
    );
    a_plugin_block_in_a_transaction_reports_status_t(&plugins, "Query blocked by plugin").await;
}

/// A Flush batch refused before any backend (an open circuit) leaves its
/// permit held while the client owes the cycle's Sync. A Query sent then is
/// discarded with the rest of the cycle, as PostgreSQL discards it while it
/// skips to the Sync: neither a pre-query plugin (Block, or a cached result)
/// nor a backend answers it. The Sync is answered, and returns the permit:
/// its client is idle again, outside any transaction or cycle, so
/// `idle_permit_timeout_ms` never ends the session. The plugin answers the
/// next Query, as before.
#[cfg(all(feature = "wasm-plugins", feature = "circuit-breaker"))]
#[tokio::test]
async fn a_query_after_a_refused_flush_is_discarded_and_its_sync_returns_the_permit() {
    let rows = br#"{"columns": [{"name": "v", "oid": 23}], "rows": [["1"]]}"#;
    let cached = serde_json::json!({"action": "cached", "data": rows.to_vec()}).to_string();
    let block = r#"{"action": "block", "value": "not here"}"#.to_string();
    for (result, expected) in [(block, &b"EZ"[..]), (cached, &b"TDCZ"[..])] {
        // The plugin answers the two Queries below, not the cycle before them.
        let plugins = hook_plugin_dir_for("pre_query", "select '", &result);
        for pooled in poolings() {
            let shape = format!("{result}, transaction pooling {pooled}");
            let h = Harness::new(0, None, |c| {
                c.circuit_breaker.enabled = true;
                c.multi_tenancy.idle_permit_timeout_ms = 200;
                plugins_from(c, &plugins);
                transaction_pooling(c, pooled);
            })
            .await;
            let node = h.backend.addr.to_string();
            let breakers = h.state.circuit_breaker.clone().expect("enabled");
            let mut a = h.connect("acme").await;
            breakers.force_open(&node, None);
            a.send(&[cycle("select 1"), flush()].concat()).await;
            assert_eq!(sqlstate(&a.frame().await), "08006", "{shape}");
            h.until("acme", "A's permit", |s| s.queries_active == 1)
                .await;

            a.send(&query("select 'discarded'")).await;
            a.quiet().await;
            assert_eq!(h.stats("acme").queries_active, 1, "{shape}");
            a.send(&sync()).await;
            assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")], "{shape}");
            h.until("acme", "the permit returned", |s| {
                s.queries_active == 0 && s.queries_idle == 0
            })
            .await;
            // Well past the idle-permit timeout: the session is not ended.
            tokio::time::sleep(Duration::from_millis(400)).await;
            a.quiet().await;
            assert_eq!(h.stats("acme").idle_permit_timeouts, 0, "{shape}");
            // Nothing of the cycle reached a backend.
            assert!(
                !h.backend
                    .log()
                    .iter()
                    .any(|s| matches!(s.tag, b'P' | b'B' | b'E' | b'H' | b'S')),
                "{shape}: {:?}",
                h.backend.log()
            );
            let reply = a.query("select 'answered'").await;
            assert_eq!(tags(&reply), expected, "{shape}");
            assert_eq!(reply.last().unwrap(), &wire(b'Z', b"I"), "{shape}");
            assert!(!h.backend.saw_query("select 'discarded'"), "{shape}");
            assert!(!h.backend.saw_query("select 'answered'"), "{shape}");
        }
    }
}

/// An extended statement the rate limiter refuses at its Sync inside a
/// transaction never reached the backend, whose transaction stays open: the
/// refusal's ReadyForQuery reports 'T', never 'I', and the transaction keeps
/// its permit.
#[cfg(feature = "rate-limiting")]
#[tokio::test]
async fn an_extended_statement_rate_limited_in_a_transaction_reports_status_t() {
    let h = Harness::new(0, None, |c| {
        c.rate_limit.enabled = true;
        c.rate_limit.default_qps = 1;
        c.rate_limit.default_burst = 1;
    })
    .await;
    let mut a = h.connect("acme").await;
    // BEGIN takes the bucket's only token.
    assert_served(&a.query("BEGIN").await, b'T');
    a.send(&[cycle("insert into t values (1)"), sync()].concat())
        .await;
    let refused = a.until_ready().await;
    assert_eq!(tags(&refused), b"EZ", "{refused:?}");
    assert_eq!(sqlstate(&refused[0]), "53400");
    assert_eq!(
        refused[1],
        wire(b'Z', b"T"),
        "the transaction is still open"
    );
    assert_eq!(h.stats("acme").queries_active, 1, "its permit is held");
    assert!(
        !h.backend.log().iter().any(|s| s.tag == b'P'),
        "{:?}",
        h.backend.log()
    );
}

/// Every chunk of a pipeline chained with `Flush`es (Bind/Execute/Flush
/// after Bind/Execute/Flush, no Sync) continues the one open cycle: the rate
/// limiter never refuses one (that would leave the cycle open behind an
/// error), but each chunk that executes anything takes its token, and with
/// the bucket empty waits for it. The chain runs at the bucket's rate, not on
/// the cycle's first token; the Sync that ends it executes nothing and is not
/// charged.
#[cfg(feature = "rate-limiting")]
#[tokio::test]
async fn a_flush_chained_pipeline_is_paced_by_the_rate_limiter_never_refused() {
    const QPS: u32 = 10;
    const CHUNKS: u32 = 4;
    let h = Harness::new(0, None, |c| {
        c.rate_limit.enabled = true;
        c.rate_limit.default_qps = QPS;
        c.rate_limit.default_burst = 1;
    })
    .await;
    let limiter = h.state.rate_limiter.clone().expect("enabled");
    let mut a = h.connect("acme").await;
    // The cycle's first batch takes the bucket's only token.
    a.send(&[cycle("insert into t values (1)"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    let started = std::time::Instant::now();
    for _ in 0..CHUNKS {
        a.send(&[bind(""), execute(), flush()].concat()).await;
        assert_eq!(tags(&a.frames(2).await), b"2C", "served, never refused");
    }
    // Each chunk found the bucket empty and waited for its token to refill.
    let paced = Duration::from_millis(u64::from(CHUNKS) * 1000 / u64::from(QPS));
    assert!(
        started.elapsed() >= paced,
        "{CHUNKS} chunks ran in {:?}, faster than {QPS} per second",
        started.elapsed()
    );
    // The first batch and every chunk took a token.
    assert_eq!(limiter.metrics().allowed(), u64::from(CHUNKS) + 1);
    let decisions = limiter.metrics().total_requests();
    a.send(&sync()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_eq!(
        limiter.metrics().total_requests(),
        decisions,
        "a bare Sync runs nothing and is not charged"
    );
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_eq!(
        h.tags_before_reset(a.key.0),
        b"PBEHBEHBEHBEHBEHS".to_vec(),
        "{:?}",
        h.backend.frames_of(a.key.0)
    );
}

/// An open circuit never fast-fails a chunk of a `Flush`-chained pipeline
/// either (it continues the cycle its node holds open); the next request is
/// a new one and is fast-failed.
#[cfg(feature = "circuit-breaker")]
#[tokio::test]
async fn an_open_circuit_never_fast_fails_a_flush_chained_chunk() {
    let h = Harness::new(0, None, |c| c.circuit_breaker.enabled = true).await;
    let node = h.backend.addr.to_string();
    let breakers = h.state.circuit_breaker.clone().expect("enabled");
    let mut a = h.connect("acme").await;
    a.send(&[cycle("insert into t values (1)"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    breakers.force_open(&node, None);
    a.send(&[bind(""), execute(), flush()].concat()).await;
    assert_eq!(tags(&a.frames(2).await), b"2C");
    a.send(&[bind(""), execute(), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"2CZ");
    let refused = a.query("select 'new'").await;
    assert_eq!(tags(&refused), b"EZ");
    assert_eq!(sqlstate(&refused[0]), "08006");
    breakers.force_close(&node, None);
    assert_eq!(
        h.tags_before_reset(a.key.0),
        b"PBEHBEHBES".to_vec(),
        "{:?}",
        h.backend.frames_of(a.key.0)
    );
}

/// Mark `addr` healthy or not, as the health checker would.
fn set_healthy(state: &ServerState, addr: &str, healthy: bool) {
    let mut health = (*state.health.load_full()).clone();
    health.get_mut(addr).expect("configured node").healthy = healthy;
    state.health.store(Arc::new(health));
}

/// Read offload: a `Query` that continues a write cycle a `Flush` dispatched
/// to the primary is a read a standby could serve, but it goes to the
/// primary connection holding the cycle, which it settles; only then does
/// the tenant permit return. Sent to the standby, it would leave the write
/// (and its row locks) open on the primary while the permit went to another
/// session.
#[tokio::test]
async fn a_query_continuing_a_flushed_write_stays_on_its_node_under_read_offload() {
    let standby = FakeBackend::start(None).await;
    let standby_addr = standby.addr.to_string();
    let h = Harness::on(FakeBackend::start(None).await, 0, |c| {
        c.add_node(&standby_addr, "standby").unwrap();
        c.load_balancer.standby_reads = crate::config::StandbyReads::Async;
        c.load_balancer.read_your_writes_ms = 0;
    });
    let mut a = h.connect("acme").await;
    // A read leaves the primary.
    assert_served(&a.query("select 'offloaded'").await, b'I');
    assert!(standby.saw_query("select 'offloaded'"));

    a.send(&[cycle("update t set v = v + 1 where id = 1"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    h.until("acme", "A's permit", |s| s.queries_active == 1)
        .await;
    let reply = a.query("select 'continues'").await;
    assert_eq!(tags(&reply), b"CZ");
    assert_served(&reply, b'I');
    assert!(
        !standby.saw_query("select 'continues'"),
        "the continuation left the cycle's node"
    );
    let on_primary: Vec<u8> = h
        .backend
        .log()
        .iter()
        .filter(|s| s.tag != RESET_ANSWERED)
        .map(|s| s.tag)
        .collect();
    assert!(
        on_primary.ends_with(b"PBEHQ"),
        "the cycle and the query that settles it on one connection: {:?}",
        h.backend.log()
    );
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// A `Sync` that continues a cycle a `Flush` dispatched goes to the
/// connection holding the cycle even when that node has been marked
/// unhealthy meanwhile (its connection is alive): the backend settles the
/// cycle and the permit returns, instead of the Sync waiting for another
/// primary and the session ending with the cycle open.
#[tokio::test]
async fn a_sync_continuing_a_flushed_cycle_reaches_its_node_marked_unhealthy() {
    let h = Harness::new(0, None, |_| {}).await;
    let node = h.backend.addr.to_string();
    let mut a = h.connect("acme").await;
    a.send(&[cycle("update t set v = v + 1 where id = 1"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    set_healthy(&h.state, &node, false);
    a.send(&sync()).await;
    assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")]);
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_eq!(h.tags_before_reset(a.key.0), b"PBEHS".to_vec());
}

/// The connection holding an extended COPY's cycle (its end sent, its Sync
/// still owed) died while the session was idle: the Sync is a fault on that
/// node (with `tr_mode = none`, one error and the session closes), never sent
/// to a fresh connection whose bare ReadyForQuery would tell the client the
/// COPY completed. The permit returns with the session.
#[tokio::test]
async fn a_continuation_whose_cycle_connection_died_is_a_fault_on_it() {
    let h = Harness::new(0, None, |c| c.tr_mode = crate::config::TrMode::None).await;
    let mut a = h.connect("acme").await;
    a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
    assert_eq!(tags(&a.frames(3).await), b"12G");
    a.send(&wire(b'd', b"1\n")).await;
    a.send(&wire(b'c', &[])).await;
    assert_eq!(tags(&a.frames(1).await), b"C");
    let connections = h.backend.log().iter().map(|s| s.conn).max();
    h.backend.kill(a.key.0);
    // The idle watch sees the connection close.
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.send(&sync()).await;
    let reply = a.until_ready().await;
    assert_eq!(tags(&reply), b"EZ");
    assert_eq!(sqlstate(&reply[0]), "57P01");
    let mut rest = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
        .await
        .expect("the session closes")
        .unwrap();
    assert!(rest.is_empty(), "{rest:?}");
    assert_eq!(
        h.backend.log().iter().map(|s| s.conn).max(),
        connections,
        "no fresh backend connection was opened for the Sync"
    );
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
}

/// An extended-protocol COPY (libpq: Parse/Bind/Execute/Sync, CopyData,
/// CopyDone or CopyFail, then a Sync the backend answers) holds its tenant
/// permit until the Sync after its end; the permit returns after the pool
/// parked the connection. Also: a Query in place of that Sync settles it,
/// and an open circuit does not refuse that Sync (it continues the COPY's
/// cycle).
#[tokio::test]
async fn an_extended_copy_holds_its_permit_until_the_sync_after_its_end() {
    #[derive(Clone, Copy, Debug)]
    enum End {
        DoneSync,
        FailSync,
        DoneQuery,
        #[cfg(feature = "circuit-breaker")]
        DoneSyncOpenCircuit,
    }
    #[cfg(feature = "circuit-breaker")]
    let ends = [
        End::DoneSync,
        End::FailSync,
        End::DoneQuery,
        End::DoneSyncOpenCircuit,
    ];
    #[cfg(not(feature = "circuit-breaker"))]
    let ends = [End::DoneSync, End::FailSync, End::DoneQuery];
    for end in ends {
        for pooled in poolings() {
            let shape = format!("{end:?}, transaction pooling {pooled}");
            let h = Harness::new(0, None, |c| {
                #[cfg(feature = "circuit-breaker")]
                {
                    c.circuit_breaker.enabled = matches!(end, End::DoneSyncOpenCircuit);
                }
                transaction_pooling(c, pooled);
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            // The backend ignores the cycle's Sync during copy-in.
            a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
            assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
            assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
            a.send(&wire(b'd', b"1\n")).await;
            let expected_before_reset: &[u8] = match end {
                End::DoneSync => {
                    a.send(&wire(b'c', &[])).await;
                    // The COPY's completion is relayed at once; its
                    // ReadyForQuery answers the Sync still to come.
                    assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
                    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
                    a.send(&sync()).await;
                    assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")], "{shape}");
                    b"PBESdcS"
                }
                End::FailSync => {
                    a.send(&wire(b'f', &cstring("client gave up"))).await;
                    assert_eq!(sqlstate(&a.frame().await), "57014", "{shape}");
                    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
                    a.send(&sync()).await;
                    assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")], "{shape}");
                    b"PBESdfS"
                }
                End::DoneQuery => {
                    a.send(&wire(b'c', &[])).await;
                    assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
                    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
                    // A Query in place of the Sync: the backend answers it,
                    // which settles the COPY too.
                    assert_served(&a.query("select 'after'").await, b'I');
                    b"PBESdcQ"
                }
                #[cfg(feature = "circuit-breaker")]
                End::DoneSyncOpenCircuit => {
                    a.send(&wire(b'c', &[])).await;
                    assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
                    assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
                    // The Sync continues the COPY's cycle: the open circuit
                    // lets it through to the backend, which answers it.
                    let node = h.backend.addr.to_string();
                    let breakers = h.state.circuit_breaker.clone().expect("enabled");
                    breakers.force_open(&node, None);
                    a.send(&sync()).await;
                    let reply = a.until_ready().await;
                    breakers.force_close(&node, None);
                    assert_eq!(reply, vec![wire(b'Z', b"I")], "{shape}");
                    b"PBESdcS"
                }
            };
            h.until("acme", "the permit returned", |s| s.queries_active == 0)
                .await;
            assert_served(&b.query("select 'b'").await, b'I');
            assert_eq!(
                h.tags_before_reset(a.key.0),
                expected_before_reset.to_vec(),
                "{shape}: {:?}",
                h.backend.frames_of(a.key.0)
            );
        }
    }
}

/// Sessions relayed the same BackendKeyData (a backend issuing colliding
/// keys): a CancelRequest naming the key cannot tell which session sent it.
/// With one session waiting at its tenant's cap and another running a
/// statement, the cancel (here the running session's own) ends no wait and
/// goes on to the backend; with two waiting, it ends neither. Each admission
/// ends only its own wait.
#[tokio::test]
async fn waits_sharing_a_backend_key_never_cancel_each_other() {
    let shared = (77, 1077);
    let h = Harness::on(FakeBackend::start_shared_key(shared).await, 5000, |_| {});
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_eq!((a.key, b.key), (shared, shared));
    h.hold_permit(&mut a, "select 'slow a'").await;
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;

    // B's is the only wait under the key, but A, running, holds it too: the
    // cancel A's client sends for its own statement leaves B waiting and
    // reaches the backend.
    h.cancel(shared).await;
    h.until_backend_cancels(1).await;
    let s = h.stats("acme");
    assert_eq!((s.query_wait_cancels, s.queries_waiting), (0, 1));

    // Two waits under the key: ambiguous as well.
    let mut c = h.connect("acme").await;
    assert_eq!(c.key, shared);
    c.send(&query("select 'c'")).await;
    h.until("acme", "B and C waiting", |s| s.queries_waiting == 2)
        .await;
    h.cancel(shared).await;
    h.until_backend_cancels(2).await;
    let s = h.stats("acme");
    assert_eq!((s.query_wait_cancels, s.queries_waiting), (0, 2));

    // B then C are admitted in turn, each ending only its own wait.
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    assert_served(&b.until_ready().await, b'I');
    assert_served(&c.until_ready().await, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    let s = h.stats("acme");
    assert_eq!(
        (
            s.query_wait_cancels,
            s.query_waits,
            s.query_refusals,
            s.queries_waiting
        ),
        (0, 2, 0, 0)
    );
    assert!(h.backend.saw_query("select 'b'") && h.backend.saw_query("select 'c'"));
}

/// A key relayed to several sessions names one again once the others have
/// ended: a cancel then ends the remaining session's wait (57014) without
/// reaching the backend. (The tenant's permit is held here outside any
/// session, so that no running session shares the key.)
#[tokio::test]
async fn a_shared_key_names_the_last_session_holding_it() {
    let shared = (78, 1078);
    let h = Harness::on(FakeBackend::start_shared_key(shared).await, 5000, |_| {});
    let held = h
        .state
        .tenant_slots
        .budget("acme")
        .expect("acme has a budget")
        .try_acquire_query()
        .expect("a free permit");
    let other = h.connect("other").await;
    let mut b = h.connect("acme").await;
    b.send(&query("select 'b'")).await;
    h.until("acme", "B waiting", |s| s.queries_waiting == 1)
        .await;
    // A session of another tenant holds the key too: ambiguous, so the
    // cancel goes on to the backend and B keeps waiting.
    h.cancel(shared).await;
    h.until_backend_cancels(1).await;
    assert_eq!(h.stats("acme").queries_waiting, 1);
    // That session ends: the key names B alone.
    let Client { s, task, .. } = other;
    drop(s);
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("the session ends with its client")
        .unwrap()
        .unwrap();
    h.cancel(shared).await;
    assert_refused(
        &b.until_ready().await,
        "57014",
        "canceling statement due to user request",
    );
    let s = h.stats("acme");
    assert_eq!((s.query_wait_cancels, s.queries_waiting), (1, 0));
    assert_eq!(
        h.backend_cancels(),
        1,
        "only the ambiguous cancel reached the backend"
    );
    drop(held);
    assert_served(&b.query("select 'b'").await, b'I');
}

/// Re-check of the round-2 replay-budget fix (tr.rs, Flush chunks): with
/// another session holding most of the tenant's replay budget, an
/// autocommit Flush chunk bigger than the room left holds no tenant bytes,
/// so it is neither dropped nor counted as a refusal; a Flush chunk that
/// opens a transaction is charged at its Sync, strictly, and refused there.
#[tokio::test]
async fn an_autocommit_flush_chunk_is_not_held_to_the_tenant_replay_budget() {
    let h = Harness::new(0, None, |c| {
        c.tr_mode = crate::config::TrMode::Transaction;
        // Replay budget only: both sessions run transactions at once.
        c.multi_tenancy.max_active_queries_per_tenant = 0;
        c.multi_tenancy.max_replay_bytes_per_tenant = 2048;
    })
    .await;
    let mut x = h.connect("acme").await;
    let mut y = h.connect("acme").await;
    assert_served(&x.query("BEGIN").await, b'T');
    let big = format!("SELECT '{}'", "x".repeat(1500));
    assert_served(&x.query(&big).await, b'T');
    h.until("acme", "X's replay bytes", |s| s.replay_bytes > 1500)
        .await;
    let held = h.stats("acme").replay_bytes;
    let chunk_sql = format!("SELECT '{}'", "y".repeat(800));
    let chunk = [cycle(&chunk_sql), flush()].concat();
    assert!(held + chunk.len() > 2048, "the chunk exceeds the room left");

    // Y, in autocommit: the chunk, then its Sync.
    y.send(&chunk).await;
    assert_eq!(tags(&y.frames(3).await), b"12C");
    y.send(&sync()).await;
    assert_served(&y.until_ready().await, b'I');
    let s = h.stats("acme");
    assert_eq!(
        (s.replay_bytes, s.replay_refusals),
        (held, 0),
        "an autocommit chunk is neither charged nor refused"
    );

    // A chunk that opens a transaction: charged when its Sync records the
    // cycle, and over the room left, refused there.
    y.send(&[cycle("BEGIN"), flush()].concat()).await;
    assert_eq!(tags(&y.frames(3).await), b"12C");
    y.send(&[cycle(&chunk_sql), sync()].concat()).await;
    assert_served(&y.until_ready().await, b'T');
    let s = h.stats("acme");
    assert_eq!((s.replay_bytes, s.replay_refusals), (held, 1));
    assert_served(&y.query("COMMIT").await, b'I');
    assert_served(&x.query("COMMIT").await, b'I');
    h.until("acme", "the budget returned", |s| s.replay_bytes == 0)
        .await;
}

/// A bare Flush from a permit holder with no extended cycle open (idle in a
/// transaction, or holding session-level advisory locks) has nothing to
/// flush: it is consumed, never forwarded, so the holder stays idle and
/// `idle_permit_timeout_ms` still ends it (25P03 in the transaction, 57P05
/// otherwise), returning the permit. Forwarded, it marked a cycle open and
/// the session kept its permit for as long as its client stayed connected.
#[tokio::test]
async fn a_bare_flush_does_not_shield_an_idle_permit_holder_from_the_timeout() {
    for (opener, status, code) in [
        ("BEGIN", b'T', "25P03"),
        ("SELECT pg_advisory_lock(1)", b'I', "57P05"),
    ] {
        let h = Harness::new(0, None, |c| {
            c.multi_tenancy.idle_permit_timeout_ms = 200;
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        assert_served(&a.query(opener).await, status);
        let flushed = tokio::time::Instant::now();
        a.send(&flush()).await;
        let fatal = a.frame().await;
        assert!(flushed.elapsed() >= Duration::from_millis(200), "{opener}");
        assert_eq!(sqlstate(&fatal), code, "{opener}");
        assert!(has_text(&fatal, "SFATAL"), "{opener}");
        let mut rest = Vec::new();
        tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
            .await
            .expect("the connection closes")
            .unwrap();
        assert!(rest.is_empty(), "{opener}: {rest:?}");
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 1
        })
        .await;
        assert!(
            !h.backend.frames_of(a.key.0).iter().any(|s| s.tag == b'H'),
            "{opener}: the Flush reached the backend: {:?}",
            h.backend.frames_of(a.key.0)
        );
        assert_served(&b.query("select 'b'").await, b'I');
    }
}

/// A Flush sent during copy-in, which PostgreSQL ignores, is consumed for a
/// capped session too: the simple-protocol COPY still ends at its CopyDone
/// (it is not taken for an extended COPY waiting for a Sync) and returns its
/// permit there.
#[tokio::test]
async fn a_flush_during_copy_in_is_consumed() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&query("COPY t FROM STDIN")).await;
    assert_eq!(a.frame().await[0], b'G', "CopyInResponse");
    a.send(&[wire(b'd', b"1\n"), flush(), wire(b'c', &[])].concat())
        .await;
    let frames = a.until_ready().await;
    assert_eq!(tags(&frames), b"CZ");
    assert_served(&frames, b'I');
    h.until("acme", "the permit returned", |s| s.queries_active == 0)
        .await;
    assert_served(&b.query("select 'b'").await, b'I');
    assert_eq!(
        h.tags_before_reset(a.key.0),
        b"Qdc".to_vec(),
        "{:?}",
        h.backend.frames_of(a.key.0)
    );
}

/// PostgreSQL ends copy-in at any message other than CopyData, CopyDone and
/// CopyFail (and the Flush and Sync it ignores there): it fails the COPY
/// (08P01) without running that message, then answers ReadyForQuery (a COPY
/// a Query started) or skips to the Sync that does (one an Execute started).
/// That ReadyForQuery ends the session's COPY: its tenant permit returns
/// (under transaction pooling after the connection was parked), and the
/// session is an idle one again, never taken for one still mid-COPY, so
/// `idle_permit_timeout_ms` ends a transaction it then leaves idle.
#[tokio::test]
async fn a_request_that_ends_copy_in_returns_the_permit() {
    for extended in [false, true] {
        for pooled in poolings() {
            let shape = format!("extended {extended}, transaction pooling {pooled}");
            let h = Harness::new(0, None, |c| {
                c.multi_tenancy.idle_permit_timeout_ms = 300;
                transaction_pooling(c, pooled);
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            let conn = a.key.0;
            if extended {
                a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
                assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
            } else {
                a.send(&query("COPY t FROM STDIN")).await;
                assert_eq!(tags(&a.frames(1).await), b"G", "{shape}");
            }
            a.send(&wire(b'd', b"1\n")).await;
            h.until("acme", "A's permit", |s| s.queries_active == 1)
                .await;
            // Mid-COPY, the session is not idle.
            assert_eq!(h.stats("acme").queries_idle, 0, "{shape}");

            // The client abandons the COPY with another request.
            let ended = if extended {
                a.send(&[cycle("select 'instead'"), sync()].concat()).await;
                a.until_ready().await
            } else {
                a.query("select 'instead'").await
            };
            assert_eq!(tags(&ended), b"EZ", "{shape}: {ended:?}");
            assert_eq!(sqlstate(&ended[0]), "08P01", "{shape}");
            assert_eq!(ended[1], wire(b'Z', b"I"), "{shape}");
            h.until("acme", "the permit returned", |s| {
                s.queries_active == 0 && s.queries_idle == 0
            })
            .await;
            assert_served(&b.query("select 'b'").await, b'I');
            h.until("acme", "B's permit returned", |s| s.queries_active == 0)
                .await;
            let expected: &[u8] = if extended { b"PBESdPBES" } else { b"QdQ" };
            assert_eq!(
                h.tags_before_reset(conn),
                expected.to_vec(),
                "{shape}: the COPY's own connection: {:?}",
                h.backend.frames_of(conn)
            );

            // An idle session again: the idle-permit timeout ends the
            // transaction it leaves idle.
            assert_served(&a.query("BEGIN").await, b'T');
            let fatal = a.frame().await;
            assert_eq!(sqlstate(&fatal), "25P03", "{shape}: {fatal:?}");
            h.until("acme", "the permit returned", |s| {
                s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 1
            })
            .await;
        }
    }
}

/// Read offload: a read that arrives in copy-in (a simple Query, or an
/// extended cycle) is not one a standby may serve. PostgreSQL ends the COPY
/// at it (08P01), so it goes to the COPY's own connection, whose
/// ReadyForQuery ends the session's COPY there, and only then does the
/// permit return. Served by the standby, it would leave the COPY open on the
/// primary behind a ReadyForQuery that ended none.
#[tokio::test]
async fn a_read_in_copy_in_goes_to_the_copys_connection_under_read_offload() {
    for extended in [false, true] {
        let shape = format!("extended {extended}");
        let standby = FakeBackend::start(None).await;
        let standby_addr = standby.addr.to_string();
        let h = Harness::on(FakeBackend::start(None).await, 0, |c| {
            c.add_node(&standby_addr, "standby").unwrap();
            c.load_balancer.standby_reads = crate::config::StandbyReads::Async;
            c.load_balancer.read_your_writes_ms = 0;
        });
        let mut a = h.connect("acme").await;
        // A read leaves the primary.
        assert_served(&a.query("select 'offloaded'").await, b'I');
        assert!(standby.saw_query("select 'offloaded'"), "{shape}");

        let read = "select 'in copy'";
        let ended = if extended {
            a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
            assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
            a.send(&[cycle(read), sync()].concat()).await;
            a.until_ready().await
        } else {
            a.send(&query("COPY t FROM STDIN")).await;
            assert_eq!(tags(&a.frames(1).await), b"G", "{shape}");
            assert!(
                h.backend.saw_query("COPY t FROM STDIN"),
                "{shape}: on the primary"
            );
            a.query(read).await
        };
        assert_eq!(tags(&ended), b"EZ", "{shape}: {ended:?}");
        assert_eq!(sqlstate(&ended[0]), "08P01", "{shape}");
        let reached = |b: &FakeBackend| {
            b.log()
                .iter()
                .any(|s| matches!(s.tag, b'Q' | b'P') && s.text == read)
        };
        assert!(
            !reached(&standby),
            "{shape}: the read left the COPY's connection"
        );
        assert!(reached(&h.backend), "{shape}");
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
        // The session goes on, its reads offloaded again.
        assert_served(&a.query("select 'after'").await, b'I');
        assert!(standby.saw_query("select 'after'"), "{shape}");
    }
}

/// A Query a plugin blocks in copy-in (the pre-query or the route hook)
/// never reaches the backend, which still holds the COPY: the session keeps
/// its permit, but its client was answered ReadyForQuery and is idle, so it
/// counts as an idle holder and `idle_permit_timeout_ms` ends it (57P05),
/// returning the permit.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_query_blocked_in_copy_in_is_an_idle_holder_bounded_by_the_timeout() {
    for (hook, result) in [
        ("pre_query", r#"{"action": "block", "value": "not here"}"#),
        ("route", r#"{"action": "block", "reason": "not here"}"#),
    ] {
        let plugins = hook_plugin_dir_for(hook, "blocked", result);
        let h = Harness::new(0, None, |c| {
            c.multi_tenancy.idle_permit_timeout_ms = 300;
            plugins_from(c, &plugins);
        })
        .await;
        let mut a = h.connect("acme").await;
        // An extended COPY, which the plugin lets through.
        a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
        assert_eq!(tags(&a.frames(3).await), b"12G", "{hook}");
        a.send(&wire(b'd', b"1\n")).await;
        h.until("acme", "A's permit", |s| s.queries_active == 1)
            .await;

        let blocked = a.query("select 'blocked'").await;
        assert_eq!(tags(&blocked), b"EZ", "{hook}: {blocked:?}");
        h.until("acme", "an idle holder", |s| {
            s.queries_idle == 1 && s.queries_active == 1
        })
        .await;
        assert_eq!(sqlstate(&a.frame().await), "57P05", "{hook}");
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 1
        })
        .await;
        assert!(!h.backend.saw_query("select 'blocked'"), "{hook}");
    }
}

/// The connection holding the extended cycle its client is still in died
/// while the client waited for answers only it could give: the rest of a
/// Flush-dispatched cycle's results (its chunk answered or not yet), which
/// only the idle watch relays, or the answer to an extended COPY's end.
/// Nothing will answer the client, and the cycle cannot be rebuilt
/// elsewhere, so the session ends at once with one FATAL, 08006, in every
/// `tr_mode` (the backend's own FATAL is never relayed) and its tenant
/// permit returns: another session of the tenant is served. (A client whose
/// COPY end was answered owes only the Sync: see
/// `a_continuation_of_a_lost_extended_copy_is_answered_08007`.)
#[tokio::test]
async fn a_cycle_lost_while_its_client_waits_for_it_ends_the_session() {
    #[derive(Clone, Copy, Debug)]
    enum Open {
        FlushUnanswered,
        /// The backend sends its own FATAL before it closes.
        FlushTerminated,
        FlushAnswered,
        CopyEndUnanswered,
    }
    for mode in [
        crate::config::TrMode::None,
        crate::config::TrMode::Session,
        crate::config::TrMode::Transaction,
    ] {
        for open in [
            Open::FlushUnanswered,
            Open::FlushTerminated,
            Open::FlushAnswered,
            Open::CopyEndUnanswered,
        ] {
            let shape = format!("{mode:?}, {open:?}");
            let h = Harness::new(0, None, |c| c.tr_mode = mode).await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            let conn = a.key.0;
            match open {
                Open::FlushUnanswered | Open::FlushTerminated => {
                    let sql = if matches!(open, Open::FlushTerminated) {
                        "select 'hang, terminate'"
                    } else {
                        "select 'hang'"
                    };
                    a.send(&[cycle(sql), flush()].concat()).await;
                    assert_eq!(tags(&a.frames(2).await), b"12", "{shape}");
                    h.until_conn_got(conn, b'E').await;
                }
                Open::FlushAnswered => {
                    a.send(&[cycle("select 1"), flush()].concat()).await;
                    assert_eq!(tags(&a.frames(3).await), b"12C", "{shape}");
                }
                Open::CopyEndUnanswered => {
                    a.send(&[cycle("COPY hang FROM STDIN"), sync()].concat())
                        .await;
                    assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
                    a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
                        .await;
                    h.until_conn_got(conn, b'c').await;
                }
            }
            h.until("acme", "A's permit", |s| s.queries_active == 1)
                .await;
            assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
            // The session idles, watching that connection.
            tokio::time::sleep(Duration::from_millis(50)).await;
            h.backend.kill(conn);
            let fatal = a.frame().await;
            assert_eq!(sqlstate(&fatal), "08006", "{shape}: {fatal:?}");
            assert!(has_text(&fatal, "SFATAL"), "{shape}");
            assert!(
                has_text(&fatal, "extended-protocol cycle"),
                "{shape}: {fatal:?}"
            );
            let mut rest = Vec::new();
            tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
                .await
                .expect("the session closes")
                .unwrap();
            assert!(rest.is_empty(), "{shape}: {rest:?}");
            h.until("acme", "the permit returned", |s| {
                s.queries_active == 0 && s.queries_idle == 0
            })
            .await;
            assert_served(&b.query("select 'b'").await, b'I');
        }
    }
}

/// The connection holding an extended COPY's cycle (its end sent, its Sync
/// still owed) died while the session was idle, under a `tr_mode` that
/// recovers: the COPY's rows died uncommitted with it, so neither the Sync
/// nor a Query in its place (which would have committed them) is re-run
/// alone on a replacement connection, whose bare ReadyForQuery would tell
/// the client its COPY completed. It is answered 08007 (outcome unknown) and
/// the session goes on; the permit returns.
#[tokio::test]
async fn a_continuation_of_a_lost_extended_copy_is_answered_08007() {
    for mode in [
        crate::config::TrMode::Session,
        crate::config::TrMode::Select,
        crate::config::TrMode::Transaction,
    ] {
        for by_query in [false, true] {
            let shape = format!("{mode:?}, query {by_query}");
            let h = Harness::new(0, None, |c| c.tr_mode = mode).await;
            let mut a = h.connect("acme").await;
            a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
            assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
            a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
                .await;
            assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
            h.backend.kill(a.key.0);
            // The idle watch sees the connection close.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let reply = if by_query {
                a.query("select 'after'").await
            } else {
                a.send(&sync()).await;
                a.until_ready().await
            };
            assert_eq!(tags(&reply), b"EZ", "{shape}: {reply:?}");
            assert_eq!(sqlstate(&reply[0]), "08007", "{shape}");
            assert_eq!(reply[1], wire(b'Z', b"I"), "{shape}");
            assert!(!h.backend.saw_query("select 'after'"), "{shape}");
            h.until("acme", "the permit returned", |s| s.queries_active == 0)
                .await;
            // The session goes on, on a replacement connection.
            assert_served(&a.query("select 'next'").await, b'I');
        }
    }
}

/// A Flush continuing an extended COPY (its end sent, its Sync still owed)
/// whose connection died while the session was idle, under a `tr_mode` that
/// recovers: the recovery answers it 08007 (outcome unknown), with no
/// ReadyForQuery, on a replacement connection, and nothing of the COPY's
/// cycle is open on any backend any more. The client still owes the cycle's
/// Sync (the session is mid-cycle, not idle, until then); that Sync gets
/// ReadyForQuery 'I' and leaves the session idle: the permit returns (and is
/// not counted idle), under transaction pooling after the replacement
/// connection was parked, and the next query is a request of its own, served
/// there (never the continuation of the dead cycle).
#[tokio::test]
async fn a_flush_continuing_a_lost_extended_copy_leaves_its_sync_to_settle_the_session() {
    for mode in [
        crate::config::TrMode::Session,
        crate::config::TrMode::Select,
        crate::config::TrMode::Transaction,
    ] {
        for pooled in poolings() {
            let shape = format!("{mode:?}, transaction pooling {pooled}");
            let h = Harness::new(0, None, |c| {
                c.tr_mode = mode;
                transaction_pooling(c, pooled);
            })
            .await;
            let mut a = h.connect("acme").await;
            a.send(&[cycle("COPY t FROM STDIN"), sync()].concat()).await;
            assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
            a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
                .await;
            assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
            h.backend.kill(a.key.0);
            // The idle watch sees the connection close.
            tokio::time::sleep(Duration::from_millis(100)).await;
            a.send(&flush()).await;
            let refused = a.frame().await;
            assert_eq!(sqlstate(&refused), "08007", "{shape}: {refused:?}");
            a.quiet().await;
            let s = h.stats("acme");
            assert_eq!((s.queries_active, s.queries_idle), (1, 0), "{shape}");
            a.send(&sync()).await;
            assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")], "{shape}");
            h.until("acme", "the permit returned", |s| {
                s.queries_active == 0 && s.queries_idle == 0
            })
            .await;
            #[cfg(feature = "pool-modes")]
            if pooled {
                let pool = h.state.backend_pool.clone().expect("transaction pooling");
                assert_eq!(pool.idle_count(), 1, "{shape}: the replacement was parked");
            }
            assert_served(&a.query("select 'next'").await, b'I');
            let ran_on: Vec<u32> = h
                .backend
                .log()
                .iter()
                .filter(|s| s.tag == b'Q' && s.text == "select 'next'")
                .map(|s| s.conn)
                .collect();
            assert_eq!(ran_on.len(), 1, "{shape}: {ran_on:?}");
            assert_ne!(ran_on[0], a.key.0, "{shape}: it ran on the replacement");
            if pooled {
                // Parked at the Sync: reset before the next query took it.
                let replacement = h.backend.frames_of(ran_on[0]);
                let reset = replacement
                    .iter()
                    .position(|s| s.tag == RESET_ANSWERED)
                    .expect("the replacement was reset for the pool");
                let next = replacement
                    .iter()
                    .position(|s| s.tag == b'Q' && s.text == "select 'next'")
                    .unwrap();
                assert!(reset < next, "{shape}: {replacement:?}");
            }
            h.until("acme", "the permit returned", |s| s.queries_active == 0)
                .await;
        }
    }
}

/// `select` mode never replays a transaction with writes, so such a
/// transaction is not charged to its tenant's replay budget, and a Flush
/// chunk that writes is not refused by it either, although its Sync has not
/// recorded the write yet: with another session holding most of the budget,
/// the write chunk is neither refused nor counted, while a read-only
/// transaction's chunk over the room left still is.
#[tokio::test]
async fn a_select_mode_flush_chunk_that_writes_is_not_held_to_the_tenant_replay_budget() {
    let h = Harness::new(0, None, |c| {
        c.tr_mode = crate::config::TrMode::Select;
        // Replay budget only: both sessions run transactions at once.
        c.multi_tenancy.max_active_queries_per_tenant = 0;
        c.multi_tenancy.max_replay_bytes_per_tenant = 2048;
    })
    .await;
    let mut x = h.connect("acme").await;
    let mut y = h.connect("acme").await;
    // X holds most of the budget in a read-only transaction.
    assert_served(&x.query("BEGIN").await, b'T');
    let big = format!("SELECT '{}'", "x".repeat(1500));
    assert_served(&x.query(&big).await, b'T');
    h.until("acme", "X's replay bytes", |s| s.replay_bytes > 1500)
        .await;
    let held = h.stats("acme").replay_bytes;

    // Y's write chunk, over the room left, inside Y's transaction.
    assert_served(&y.query("BEGIN").await, b'T');
    let write = format!("INSERT INTO t VALUES ('{}')", "y".repeat(800));
    let chunk = [cycle(&write), flush()].concat();
    assert!(held + chunk.len() > 2048, "the chunk exceeds the room left");
    y.send(&chunk).await;
    assert_eq!(tags(&y.frames(3).await), b"12C");
    y.send(&sync()).await;
    assert_served(&y.until_ready().await, b'T');
    let s = h.stats("acme");
    assert_eq!(
        (s.replay_bytes, s.replay_refusals),
        (held, 0),
        "a write chunk is neither charged nor refused"
    );
    assert_served(&y.query("COMMIT").await, b'I');

    // A read-only transaction's chunk over the room left is refused at its
    // Flush.
    assert_served(&y.query("BEGIN").await, b'T');
    let read = format!("SELECT '{}'", "z".repeat(800));
    y.send(&[cycle(&read), flush()].concat()).await;
    assert_eq!(tags(&y.frames(3).await), b"12C");
    y.send(&sync()).await;
    assert_served(&y.until_ready().await, b'T');
    let s = h.stats("acme");
    assert_eq!((s.replay_bytes, s.replay_refusals), (held, 1));
    assert_served(&y.query("COMMIT").await, b'I');
    assert_served(&x.query("COMMIT").await, b'I');
    h.until("acme", "the budget returned", |s| s.replay_bytes == 0)
        .await;
}

/// Under transaction pooling the end of a COPY, and the synthesized ROLLBACK
/// that ends a transaction a failover aborted, park the session's backend
/// connection before they return its tenant permit, like every other
/// release: the waiter that permit wakes takes that connection from the pool
/// (the one parked last), not the one it parked itself before waiting.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn a_copy_end_and_a_synthesized_rollback_park_before_returning_the_permit() {
    let ran_on = |h: &Harness| -> Vec<u32> {
        h.backend
            .log()
            .iter()
            .filter(|s| s.tag == b'Q' && s.text == "select 'b'")
            .map(|s| s.conn)
            .collect()
    };
    // The end of a COPY.
    {
        let h = Harness::new(5000, None, |c| transaction_pooling(c, true)).await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        a.send(&query("COPY t FROM STDIN")).await;
        assert_eq!(a.frame().await[0], b'G', "CopyInResponse");
        b.send(&query("select 'b'")).await;
        h.until("acme", "B waiting", |s| s.queries_waiting == 1)
            .await;
        a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
            .await;
        assert_served(&a.until_ready().await, b'I');
        assert_served(&b.until_ready().await, b'I');
        assert_eq!(
            ran_on(&h),
            vec![a.key.0],
            "B took A's connection, parked at the COPY's end: {:?}",
            h.backend.log()
        );
    }
    // The synthesized ROLLBACK of a transaction a failover aborted.
    {
        let h = Harness::new(5000, None, |c| {
            c.tr_mode = crate::config::TrMode::Session;
            transaction_pooling(c, true);
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        assert_served(&a.query("BEGIN").await, b'T');
        h.backend.kill(a.key.0);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // A's next statement fails over to a replacement connection, and the
        // session emulates the transaction the failover aborted.
        let failed = a.query("select 'a'").await;
        assert_eq!(failed.last().unwrap(), &wire(b'Z', b"E"), "{failed:?}");
        b.send(&query("select 'b'")).await;
        h.until("acme", "B waiting", |s| s.queries_waiting == 1)
            .await;
        assert_eq!(tags(&a.query("ROLLBACK").await), b"CZ");
        assert_served(&b.until_ready().await, b'I');
        let ran = ran_on(&h);
        assert!(
            ran.len() == 1 && ran[0] != a.key.0 && ran[0] != b.key.0,
            "B took A's replacement connection, parked at the ROLLBACK: {ran:?} {:?}",
            h.backend.log()
        );
    }
}

/// A Flush-terminated batch of a new cycle refused before it reached a
/// backend (an open circuit, an empty rate-limit bucket) leaves its client
/// owing the cycle's Sync: the session is mid-cycle, not an idle permit
/// holder, so `idle_permit_timeout_ms` does not end it; its Sync returns the
/// permit.
#[cfg(any(feature = "circuit-breaker", feature = "rate-limiting"))]
#[tokio::test]
async fn a_client_owing_its_sync_after_a_refused_flush_is_not_an_idle_holder() {
    /// Well past the idle-permit timeout, A still holds its permit, is not
    /// counted idle and was not terminated.
    async fn owes_the_sync(h: &Harness, a: &mut Client, shape: &str) {
        h.until("acme", "A's permit", |s| s.queries_active == 1)
            .await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        a.quiet().await;
        let s = h.stats("acme");
        assert_eq!(
            (s.queries_active, s.queries_idle, s.idle_permit_timeouts),
            (1, 0, 0),
            "{shape}"
        );
    }
    #[cfg(feature = "circuit-breaker")]
    {
        let h = Harness::new(0, None, |c| {
            c.circuit_breaker.enabled = true;
            c.multi_tenancy.idle_permit_timeout_ms = 200;
        })
        .await;
        let node = h.backend.addr.to_string();
        let breakers = h.state.circuit_breaker.clone().expect("enabled");
        let mut a = h.connect("acme").await;
        breakers.force_open(&node, None);
        a.send(&[cycle("select 1"), flush()].concat()).await;
        assert_eq!(sqlstate(&a.frame().await), "08006");
        owes_the_sync(&h, &mut a, "open circuit").await;
        breakers.force_close(&node, None);
        a.send(&sync()).await;
        assert_served(&a.until_ready().await, b'I');
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 0
        })
        .await;
    }
    #[cfg(feature = "rate-limiting")]
    {
        let h = Harness::new(0, None, |c| {
            c.rate_limit.enabled = true;
            c.rate_limit.default_qps = 2;
            c.rate_limit.default_burst = 1;
            c.multi_tenancy.idle_permit_timeout_ms = 200;
        })
        .await;
        let mut a = h.connect("acme").await;
        // The bucket's only token.
        assert_served(&a.query("select 0").await, b'I');
        a.send(&[cycle("select 1"), flush()].concat()).await;
        assert_eq!(sqlstate(&a.frame().await), "53400");
        owes_the_sync(&h, &mut a, "empty bucket").await;
        // The Sync settles the refused cycle (answered without a backend).
        a.send(&sync()).await;
        assert_served(&a.until_ready().await, b'I');
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0 && s.idle_permit_timeouts == 0
        })
        .await;
    }
}

/// A Flush batch of a new cycle (Parse/Bind/Execute/Flush of a named write)
/// refused before any backend with `code` (`arm` sets the refusal up),
/// inside a transaction or not. The client was told the cycle failed, so
/// the rest of it is discarded up to its Sync, as PostgreSQL skips to the
/// Sync after an error: neither the chunk nor the Query the client
/// pipelined behind it runs, even once nothing refuses them (`lift` ran),
/// so nothing commits behind the error and the cycle gets one error. The
/// Sync gets ReadyForQuery with the backend's status, and returns the
/// permit when that leaves the session idle; the next cycle runs whole.
#[cfg(any(feature = "circuit-breaker", feature = "rate-limiting"))]
async fn a_refused_flush_batch_discards_its_cycle<Fut>(
    h: &Harness,
    shape: &str,
    in_tx: bool,
    code: &str,
    arm: impl Fn(),
    lift: impl Fn() -> Fut,
) where
    Fut: std::future::Future<Output = ()>,
{
    let status = if in_tx { b'T' } else { b'I' };
    let mut a = h.connect("acme").await;
    assert_served(
        &a.query(if in_tx { "BEGIN" } else { "select 0" }).await,
        status,
    );
    arm();
    a.send(
        &[
            parse("s1", "insert into t values (1)"),
            bind("s1"),
            execute(),
            flush(),
        ]
        .concat(),
    )
    .await;
    let refused = a.frame().await;
    assert_eq!(sqlstate(&refused), code, "{shape}: {refused:?}");
    lift().await;
    a.send(&[bind("s1"), execute(), flush(), query("select 'discarded'")].concat())
        .await;
    a.quiet().await;
    assert_eq!(h.stats("acme").queries_active, 1, "{shape}");
    a.send(&sync()).await;
    assert_eq!(
        a.until_ready().await,
        vec![wire(b'Z', &[status])],
        "{shape}"
    );
    if in_tx {
        // The transaction still holds the permit; its end returns it.
        assert_eq!(h.stats("acme").queries_active, 1, "{shape}");
        lift().await;
        assert_served(&a.query("COMMIT").await, b'I');
    }
    h.until("acme", "the permit returned", |s| {
        s.queries_active == 0 && s.queries_idle == 0
    })
    .await;
    assert!(
        !h.backend
            .log()
            .iter()
            .any(|s| matches!(s.tag, b'P' | b'B' | b'E' | b'H' | b'S')),
        "{shape}: nothing of the cycle reached a backend: {:?}",
        h.backend.log()
    );
    assert!(!h.backend.saw_query("select 'discarded'"), "{shape}");
    lift().await;
    a.send(&[cycle("select 2"), sync()].concat()).await;
    assert_eq!(tags(&a.until_ready().await), b"12CZ", "{shape}");
}

/// [`a_refused_flush_batch_discards_its_cycle`] behind an open circuit.
#[cfg(feature = "circuit-breaker")]
#[tokio::test]
async fn a_cycle_fast_failed_at_a_flush_batch_is_discarded_up_to_its_sync() {
    for pooled in poolings() {
        for in_tx in [false, true] {
            let shape = format!("transaction {in_tx}, transaction pooling {pooled}");
            let h = Harness::new(0, None, |c| {
                c.circuit_breaker.enabled = true;
                transaction_pooling(c, pooled);
            })
            .await;
            let node = h.backend.addr.to_string();
            let breakers = h.state.circuit_breaker.clone().expect("enabled");
            a_refused_flush_batch_discards_its_cycle(
                &h,
                &shape,
                in_tx,
                "08006",
                || breakers.force_open(&node, None),
                || {
                    breakers.force_close(&node, None);
                    std::future::ready(())
                },
            )
            .await;
        }
    }
}

/// [`a_refused_flush_batch_discards_its_cycle`] against an empty rate-limit
/// bucket, which refills before the client's next chunk: a chunk forwarded
/// then would run, and the Sync commit it, behind the error.
#[cfg(feature = "rate-limiting")]
#[tokio::test]
async fn a_cycle_rate_limited_at_a_flush_batch_is_discarded_up_to_its_sync() {
    const QPS: u32 = 4;
    // Long enough for the bucket's one token to come back.
    let refill = Duration::from_millis(u64::from(1000 / QPS) * 2);
    for pooled in poolings() {
        for in_tx in [false, true] {
            let shape = format!("transaction {in_tx}, transaction pooling {pooled}");
            let h = Harness::new(0, None, |c| {
                c.rate_limit.enabled = true;
                c.rate_limit.default_qps = QPS;
                c.rate_limit.default_burst = 1;
                transaction_pooling(c, pooled);
            })
            .await;
            a_refused_flush_batch_discards_its_cycle(
                &h,
                &shape,
                in_tx,
                "53400",
                || {},
                || tokio::time::sleep(refill),
            )
            .await;
        }
    }
}

/// A session without an active-query cap (here no `[multi_tenancy]` at all)
/// consumes a bare Flush at idle like a capped one: it runs nothing, so an
/// empty rate-limit bucket or an open circuit never refuses it. Refused, it
/// would leave its client owing a Sync and discard the simple Query sent
/// after it (a client that never sends a Sync would hang); forwarded, it
/// would make that Query the continuation of a cycle nothing opened (paced
/// instead of refused, never fast-failed, never answered from a cache). The
/// Query gets its own answer.
#[cfg(any(feature = "circuit-breaker", feature = "rate-limiting"))]
#[tokio::test]
async fn an_uncapped_sessions_bare_flush_is_consumed_and_never_refused() {
    async fn flush_then_query(h: &Harness, a: &mut Client, code: &str, shape: &str) {
        a.send(&[flush(), query("select 1")].concat()).await;
        let reply = a.until_ready().await;
        assert_eq!(tags(&reply), b"EZ", "{shape}: {reply:?}");
        assert_eq!(
            sqlstate(&reply[0]),
            code,
            "{shape}: the Query's own refusal"
        );
        assert_eq!(reply[1], wire(b'Z', b"I"), "{shape}");
        a.quiet().await;
        assert!(
            !h.backend.frames_of(a.key.0).iter().any(|s| s.tag == b'H'),
            "{shape}: the Flush never reached the backend: {:?}",
            h.backend.frames_of(a.key.0)
        );
    }
    let uncapped = |c: &mut ProxyConfig| c.multi_tenancy = Default::default();
    #[cfg(feature = "rate-limiting")]
    {
        const QPS: u32 = 4;
        let h = Harness::new(0, None, |c| {
            uncapped(c);
            c.rate_limit.enabled = true;
            c.rate_limit.default_qps = QPS;
            c.rate_limit.default_burst = 1;
        })
        .await;
        let mut a = h.connect("acme").await;
        assert!(h.state.tenant_slots.budget("acme").is_none(), "uncapped");
        // The bucket's only token.
        assert_served(&a.query("select 0").await, b'I');
        flush_then_query(&h, &mut a, "53400", "empty bucket").await;
        tokio::time::sleep(Duration::from_millis(u64::from(1000 / QPS) * 2)).await;
        assert_served(&a.query("select 2").await, b'I');
    }
    #[cfg(feature = "circuit-breaker")]
    {
        let h = Harness::new(0, None, |c| {
            uncapped(c);
            c.circuit_breaker.enabled = true;
        })
        .await;
        let node = h.backend.addr.to_string();
        let breakers = h.state.circuit_breaker.clone().expect("enabled");
        let mut a = h.connect("acme").await;
        breakers.force_open(&node, None);
        flush_then_query(&h, &mut a, "08006", "open circuit").await;
        breakers.force_close(&node, None);
        assert_served(&a.query("select 2").await, b'I');
    }
}

/// A CancelRequest that claimed a wait just as a released permit admitted it
/// wins: the request is cancelled (57014) and the permit goes back. It counts
/// as one cancel only, never as an admission; its wait is recorded in the
/// wait histogram like every wait.
#[tokio::test]
async fn a_cancel_racing_an_admission_counts_only_as_a_cancel() {
    let mut config = test_config();
    config.multi_tenancy.enabled = true;
    config.multi_tenancy.tenants = vec!["acme".into()];
    config.multi_tenancy.max_active_queries_per_tenant = 1;
    config.multi_tenancy.active_query_wait_ms = 5000;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let gate = state.tenant_slots.budget("acme").unwrap();
    let held = gate.try_acquire_query().unwrap();
    let (proxy, _client) = pair().await;
    let mut stream = ClientStream::Plain(proxy);
    let mut buffer = BytesMut::new();
    // The cancel claims the registered wait; its wake-up has not reached the
    // waiter (it was taken already) when the permit comes free.
    let key = (7, 1007);
    // One live session holds the key: the cancel names it.
    state.tenant_slots.session_key_relayed(key);
    let wait = state.tenant_slots.register_query_wait(key);
    assert!(state.tenant_slots.cancel_query_wait(key.0, key.1));
    tokio::time::timeout(TEST_TIMEOUT, wait.cancelled())
        .await
        .expect("the stored wake-up");
    let waiter = Some((key, wait));
    let release = async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        drop(held);
    };
    let (outcome, ()) = tokio::join!(
        ProxyServer::wait_tenant_query_permit(gate, &mut stream, &mut buffer, waiter, &state),
        release
    );
    assert!(matches!(
        outcome,
        crate::server::client::QueryGateWait::Cancelled
    ));
    let s = acme_stats(&state);
    assert_eq!(
        (
            s.queries_admitted,
            s.query_waits,
            s.query_wait_cancels,
            s.queries_active
        ),
        (1, 1, 1, 0),
        "only the first holder was admitted"
    );
    assert_eq!(state.latency.tenant_query_wait.snapshot().count(), 1);
    state.tenant_slots.session_key_ended(key);
}

/// The stats of tenant "acme".
fn acme_stats(state: &ServerState) -> TenantBudgetStats {
    state
        .tenant_slots
        .budget_stats()
        .into_iter()
        .find(|s| s.tenant == "acme")
        .expect("acme has a budget")
}

#[test]
fn release_helper_waits_for_an_idle_session() {
    let mut config = test_config();
    config.multi_tenancy.enabled = true;
    config.multi_tenancy.tenants = vec!["acme".into()];
    config.multi_tenancy.max_active_queries_per_tenant = 1;
    let slots = crate::tenant_slots::TenantSlots::from_config(&config);
    let budget = slots.budget("acme").unwrap();
    let session = make_test_session();
    let mut hold = Some(crate::tenant_slots::QueryHold::new(budget));
    hold.as_mut()
        .unwrap()
        .hold(budget.try_acquire_query().unwrap());
    let held = |hold: &Option<crate::tenant_slots::QueryHold<'_>>| hold.as_ref().unwrap().is_held();

    session.in_transaction.store(true, Ordering::Relaxed);
    ProxyServer::tenant_query_release_if_idle(&mut hold, &session, false, false);
    assert!(held(&hold), "in a transaction");
    session.in_transaction.store(false, Ordering::Relaxed);
    session.copy_in_progress.store(true, Ordering::Relaxed);
    ProxyServer::tenant_query_release_if_idle(&mut hold, &session, false, false);
    assert!(held(&hold), "mid-COPY");
    session.copy_in_progress.store(false, Ordering::Relaxed);
    ProxyServer::tenant_query_release_if_idle(&mut hold, &session, true, false);
    assert!(held(&hold), "a cycle open");
    ProxyServer::tenant_query_release_if_idle(&mut hold, &session, false, true);
    assert!(held(&hold), "holding session-level advisory locks");
    ProxyServer::tenant_query_release_if_idle(&mut hold, &session, false, false);
    assert!(!held(&hold), "idle: released");
    assert!(budget.try_acquire_query().is_some());
    // No gate: nothing to do.
    ProxyServer::tenant_query_release_if_idle(&mut None, &session, false, false);
}

#[test]
fn only_single_statement_2pc_ends_are_exempt() {
    for sql in [
        "COMMIT PREPARED 'x'",
        "rollback prepared 'y';",
        "/* c */ COMMIT PREPARED 'x'  ",
    ] {
        assert!(ProxyServer::tenant_gate_exempt(sql), "{sql}");
    }
    for sql in [
        "COMMIT",
        "PREPARE TRANSACTION 'x'",
        "COMMIT PREPARED 'x'; SELECT 1",
        "SELECT 'COMMIT PREPARED'",
        "\"COMMIT\" PREPARED 'x'",
    ] {
        assert!(!ProxyServer::tenant_gate_exempt(sql), "{sql}");
    }
}

#[test]
fn session_locks_are_tracked_by_their_statements() {
    let session = make_test_session();
    let mut holds = false;
    for (sql, expect) in [
        ("SELECT 1", false),
        ("SELECT pg_advisory_xact_lock(1)", false),
        ("SELECT pg_try_advisory_lock(1)", true),
        ("SELECT pg_advisory_unlock(1)", true),
        ("SELECT PG_ADVISORY_UNLOCK_ALL()", false),
        ("select pg_advisory_lock_shared(2)", true),
        ("DISCARD ALL", false),
    ] {
        ProxyServer::note_session_locks(&mut holds, sql, &session);
        assert_eq!(holds, expect, "{sql}");
    }
    // Text that only contains `advisory_lock` takes none, and naming the
    // unlock in a literal releases nothing.
    for (sql, expect) in [
        ("SELECT * FROM job_advisory_locks", false),
        ("SELECT pg_advisory_lock(4)", true),
        ("SELECT 'pg_advisory_unlock_all()'", true),
        ("SELECT pg_advisory_unlock_all()", false),
    ] {
        ProxyServer::note_session_locks(&mut holds, sql, &session);
        assert_eq!(holds, expect, "{sql}");
    }
    // A failed unlock leaves the lock held.
    ProxyServer::note_session_locks(&mut holds, "SELECT pg_advisory_lock(3)", &session);
    session.last_response_error.store(true, Ordering::Relaxed);
    ProxyServer::note_session_locks(&mut holds, "SELECT pg_advisory_unlock_all()", &session);
    assert!(holds);
}

/// A statement takes a session-level advisory lock when it names one of
/// the four functions that take one as a whole word, anywhere in its text
/// (a literal or a `DO` body may run it); other text containing
/// `advisory_lock` takes none. Only a real call of `pg_advisory_unlock_all()`
/// clears the mark: its name in a literal, a comment, a quoted identifier,
/// or a statement the scan cannot read (a backslash, an unterminated
/// literal) does not.
#[test]
fn session_locks_are_taken_by_statements_naming_the_lock_functions() {
    for sql in [
        "SELECT pg_advisory_lock(1)",
        "select PG_TRY_ADVISORY_LOCK_SHARED(1, 2)",
        "SELECT pg_advisory_lock_shared(1)",
        "SELECT pg_try_advisory_lock(1)",
        "SELECT pg_catalog.pg_advisory_lock(1)",
        "SELECT \"pg_advisory_lock\"(1)",
        "SELECT pg_advisory_lock/* why */(1)",
        "SELECT pg_advisory_lock (1)",
        "DO $$BEGIN PERFORM pg_advisory_lock(1); END$$",
        "SELECT query_to_xml('select pg_try_advisory_lock(1)', true, true, '')",
        "SELECT x FROM t;SELECT pg_advisory_lock(1)",
        "SELECT pg_advisory_lock",
    ] {
        assert!(ProxyServer::takes_session_lock(sql.as_bytes()), "{sql}");
    }
    for sql in [
        "SELECT 1",
        "SELECT * FROM job_advisory_locks",
        "SELECT id FROM jobs WHERE kind = 'advisory_lock'",
        "SELECT 1 -- advisory_lock",
        "SELECT advisory_lock_id FROM t",
        "SELECT my_pg_advisory_lock(1)",
        "SELECT xpg_try_advisory_lock(1)",
        "SELECT pg_advisory_locks FROM t",
        "SELECT pg_advisory_lock_sharedx(1)",
        "SELECT pg_advisory_lock$1 FROM t",
        "SELECT pg_advisory_lock\u{e9}(1)",
        "SELECT pg_advisory_xact_lock(1)",
        "SELECT pg_try_advisory_xact_lock_shared(1)",
        "SELECT pg_advisory_unlock(1)",
        "SELECT pg_advisory_unlock_all()",
        "SELECT pg_advisory_",
    ] {
        assert!(!ProxyServer::takes_session_lock(sql.as_bytes()), "{sql}");
    }
    for sql in [
        "SELECT pg_advisory_unlock_all()",
        "select PG_ADVISORY_UNLOCK_ALL ()",
        "SELECT pg_catalog.pg_advisory_unlock_all()",
        "/* done */ SELECT pg_advisory_unlock_all()",
        "SELECT pg_advisory_unlock_all/**/()",
    ] {
        assert!(ProxyServer::calls_unlock_all(sql), "{sql}");
    }
    for sql in [
        "SELECT 'pg_advisory_unlock_all()'",
        "SELECT 1 -- pg_advisory_unlock_all()",
        "SELECT 1 /* pg_advisory_unlock_all() */",
        "SELECT \"pg_advisory_unlock_all\"()",
        "SELECT pg_advisory_unlock_all",
        "SELECT my_pg_advisory_unlock_all()",
        "DO $$BEGIN PERFORM pg_advisory_unlock_all(); END$$",
        "SELECT E'\\' pg_advisory_unlock_all() --'",
        "SELECT 'x pg_advisory_unlock_all()",
        "SELECT pg_advisory_unlock(1)",
    ] {
        assert!(!ProxyServer::calls_unlock_all(sql), "{sql}");
    }
}

/// What a transaction replay sends is scanned for a session-level advisory
/// lock: each simple statement, a cycle's frames and held-aside unnamed
/// `Parse`, the `Parse`s it redefines, and those of the statements it
/// references, from the registry.
#[test]
fn a_replay_that_takes_a_session_lock_is_recognised() {
    let simple = |sql: &str| StatementLog {
        sql: sql.to_string(),
        params: Vec::new(),
        result_checksum: None,
        executed_at: chrono::Utc::now(),
        extended: None,
    };
    let cycle_of = |frames: Vec<u8>, ext: ExtendedBatchLog| StatementLog {
        extended: Some(ExtendedBatchLog {
            frames: bytes::Bytes::from(frames),
            ..ext
        }),
        ..simple("SELECT 1")
    };
    let none = ExtendedBatchLog {
        frames: bytes::Bytes::new(),
        unnamed_parse: None,
        defines: Vec::new(),
        refs: Vec::new(),
        outside: Vec::new(),
    };
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    let takes = |entries: &[StatementLog], registry: &HashMap<String, bytes::Bytes>| {
        ProxyServer::tr_replay_takes_session_lock(entries, registry)
    };
    assert!(!takes(&[], &registry));
    assert!(!takes(
        &[simple("BEGIN"), simple("SELECT pg_advisory_xact_lock(1)")],
        &registry
    ));
    assert!(takes(
        &[simple("BEGIN"), simple("select PG_ADVISORY_LOCK(1)")],
        &registry
    ));
    let plain = [parse("", "SELECT 1"), bind(""), execute(), sync()].concat();
    assert!(!takes(&[cycle_of(plain.clone(), none.clone())], &registry));
    let locking = [parse("", "SELECT pg_try_advisory_lock(1)"), sync()].concat();
    assert!(takes(&[cycle_of(locking, none.clone())], &registry));
    let held = ExtendedBatchLog {
        unnamed_parse: Some(bytes::Bytes::from(parse("", "SELECT pg_advisory_lock(1)"))),
        ..none.clone()
    };
    assert!(takes(&[cycle_of(plain.clone(), held)], &registry));
    let redefined = ExtendedBatchLog {
        outside: vec![(
            "lk".to_string(),
            bytes::Bytes::from(parse("lk", "SELECT pg_advisory_lock($1)")),
        )],
        ..none.clone()
    };
    assert!(takes(&[cycle_of(plain.clone(), redefined)], &registry));
    let referenced = ExtendedBatchLog {
        refs: vec!["lk".to_string()],
        ..none.clone()
    };
    let rerun = [bind("lk"), execute(), sync()].concat();
    assert!(!takes(
        &[cycle_of(rerun.clone(), referenced.clone())],
        &registry
    ));
    registry.insert(
        "lk".to_string(),
        bytes::Bytes::from(parse("lk", "SELECT pg_advisory_lock($1)")),
    );
    assert!(takes(&[cycle_of(rerun, referenced)], &registry));
}

/// Whether a named statement takes a session-level advisory lock is
/// classified once per definition, memoized against the registry's Parse
/// bytes: every Bind of it is a lookup, never a rescan of the stored SQL
/// (which may be far larger than the Bind), while a redefinition is
/// classified afresh.
#[test]
fn a_named_statements_session_lock_is_classified_once_per_definition() {
    let parse_of = |sql: &str| bytes::Bytes::from(parse("s1", sql));
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    let mut memo = HashMap::new();
    assert!(
        !ProxyServer::named_takes_session_lock(&mut memo, &registry, "s1"),
        "an unknown statement takes none"
    );
    registry.insert("s1".into(), parse_of("SELECT pg_advisory_lock(1)"));
    assert!(ProxyServer::named_takes_session_lock(
        &mut memo, &registry, "s1"
    ));
    // The memo answers for these very bytes without a rescan: an answer
    // planted for them is what comes back.
    memo.get_mut("s1").expect("memoized").1 = false;
    assert!(!ProxyServer::named_takes_session_lock(
        &mut memo, &registry, "s1"
    ));
    // A re-Parse (new bytes, even with the same text) is classified again.
    registry.insert("s1".into(), parse_of("SELECT pg_advisory_lock(1)"));
    assert!(ProxyServer::named_takes_session_lock(
        &mut memo, &registry, "s1"
    ));
    registry.insert("s1".into(), parse_of("SELECT pg_advisory_xact_lock(1)"));
    assert!(!ProxyServer::named_takes_session_lock(
        &mut memo, &registry, "s1"
    ));
}

/// What a named statement does to the session-bound state, and whether it
/// releases the session's advisory locks, is read from its text once per
/// definition (memoized against the registry's Parse bytes): every Bind is
/// a lookup, and a redefinition is read afresh.
#[test]
fn a_named_statements_text_facts_are_read_once_per_definition() {
    let parse_of = |sql: &str| bytes::Bytes::from(parse("s1", sql));
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    let mut memo = HashMap::new();
    assert!(
        ProxyServer::named_text_facts(&mut memo, &registry, "s1").is_none(),
        "an unknown statement says nothing"
    );
    assert!(memo.is_empty());
    let listen = "LISTEN jobs";
    registry.insert("s1".into(), parse_of(listen));
    let facts = ProxyServer::named_text_facts(&mut memo, &registry, "s1").expect("known");
    assert_eq!(facts.effect, super::session_bound::text_effect(listen));
    assert_ne!(facts.effect, super::session_bound::Effect::default());
    assert!(!facts.unlocks);
    // The memo answers for these very bytes without a rescan: an answer
    // planted for them is what comes back.
    memo.get_mut("s1").expect("memoized").1 = None;
    assert!(ProxyServer::named_text_facts(&mut memo, &registry, "s1").is_none());
    // A re-Parse (new bytes, even with the same text) is read again.
    registry.insert("s1".into(), parse_of(listen));
    assert_eq!(
        ProxyServer::named_text_facts(&mut memo, &registry, "s1"),
        Some(facts)
    );
    let unlock = "SELECT pg_advisory_unlock_all()";
    registry.insert("s1".into(), parse_of(unlock));
    let facts = ProxyServer::named_text_facts(&mut memo, &registry, "s1").expect("known");
    assert!(facts.unlocks);
    assert_eq!(facts.effect, super::session_bound::text_effect(unlock));
    // Text that is not UTF-8 says nothing (the backend refuses it).
    let mut invalid = parse("s1", "SELECT 'x'");
    let at = invalid.iter().position(|&b| b == b'x').expect("literal");
    invalid[at] = 0xff;
    registry.insert("s1".into(), bytes::Bytes::from(invalid));
    assert!(ProxyServer::named_text_facts(&mut memo, &registry, "s1").is_none());
}

/// The frames the proxy sends `client` up to a ReadyForQuery or the end of
/// the connection, whichever comes first.
async fn frames_until_ready_or_closed(client: &mut Client) -> Vec<Vec<u8>> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut frames = Vec::new();
        loop {
            let mut header = [0u8; 5];
            if client.s.read_exact(&mut header).await.is_err() {
                return frames;
            }
            let len = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
            let mut frame = header.to_vec();
            frame.resize(len + 1, 0);
            if client.s.read_exact(&mut frame[5..]).await.is_err() {
                return frames;
            }
            let ready = frame[0] == b'Z';
            frames.push(frame);
            if ready {
                return frames;
            }
        }
    })
    .await
    .expect("the proxy answered or closed the connection")
}

/// Wait until the fake backend received a message (a simple Query, or a
/// Parse) whose SQL is `sql`; returns the backend connection that did.
async fn until_conn_ran(h: &Harness, sql: &str) -> u32 {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    loop {
        if let Some(seen) = h
            .backend
            .log()
            .iter()
            .find(|s| matches!(s.tag, b'Q' | b'P') && s.text == sql)
        {
            return seen.conn;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the backend never ran {sql:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Exactly one CancelRequest reached the fake backend, naming the
/// BackendKeyData of backend connection `pid` (the one running what the
/// proxy abandoned), and it did before the tenant's next request (`next`, a
/// simple query that waited for the permit) did.
fn assert_cancelled_before(h: &Harness, pid: u32, next: &str, shape: &str) {
    let log = h.backend.log();
    let cancels: Vec<&Seen> = log.iter().filter(|s| s.tag == b'!').collect();
    assert_eq!(cancels.len(), 1, "{shape}: {cancels:?}");
    assert_eq!(
        cancels[0].text,
        format!("cancel {pid} {}", pid + 1000),
        "{shape}: the cancel names the connection running the statement"
    );
    let admitted = log
        .iter()
        .find(|s| s.tag == b'Q' && s.text == next)
        .expect("the next request reached the backend");
    assert!(
        cancels[0].at < admitted.at,
        "{shape}: the cancel reached the backend before the next request"
    );
}

/// A statement that sends nothing for longer than `[limits]
/// backend_read_timeout_secs` (or outlives `backend_response_timeout_secs`)
/// is abandoned by the relay while its backend may still be running it: its
/// connection is dropped and the client told the outcome is unknown, and the
/// session's permit returns. PostgreSQL notices the closed socket only when
/// it next writes, so the statement would run on outside the tenant's cap
/// while the permit admits the next one. The backend gets a CancelRequest
/// with that connection's own BackendKeyData first, before the next request
/// of the tenant is admitted: on the startup connection, one leased from the
/// pool (which keeps the key while the connection is parked), one dialed
/// afresh after the startup one was lost, and on the extended protocol.
#[tokio::test]
async fn a_statement_abandoned_on_a_live_backend_is_cancelled_before_its_permit_returns() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Shape {
        Startup,
        Pooled,
        Redialed,
        Extended,
        ResponseDeadline,
    }
    let mut shapes = vec![
        Shape::Startup,
        Shape::Redialed,
        Shape::Extended,
        Shape::ResponseDeadline,
    ];
    if cfg!(feature = "pool-modes") {
        shapes.push(Shape::Pooled);
    }
    for shape in shapes {
        let name = format!("{shape:?}");
        let h = Harness::new(5000, None, |c| {
            if shape == Shape::ResponseDeadline {
                c.limits.backend_response_timeout_secs = 1;
            } else {
                c.limits.backend_read_timeout_secs = 1;
            }
            if shape == Shape::Redialed {
                c.tr_mode = crate::config::TrMode::None;
            }
            transaction_pooling(c, shape == Shape::Pooled);
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        match shape {
            Shape::Pooled => {
                // The startup connection goes to the pool (before the
                // permit returns), and the statement below leases it back.
                assert_served(&a.query("select 'a'").await, b'I');
                h.until("acme", "A's permit returned", |s| s.queries_active == 0)
                    .await;
                #[cfg(feature = "pool-modes")]
                assert_eq!(
                    h.state.backend_pool.as_ref().expect("pooling").idle_count(),
                    1,
                    "{name}"
                );
            }
            Shape::Redialed => {
                // B holds the tenant's only permit (an idle lock holder), so
                // A's statement waits at the cap; A's startup connection
                // dies meanwhile. Admitted once B unlocks, A finds it gone
                // and, holding nothing that connection had, runs the
                // statement on a fresh dial. (A session that held a lock or
                // a transaction there would be ended instead: `session_bound`.)
                assert_served(&b.query("SELECT pg_advisory_lock(1)").await, b'I');
                h.until("acme", "B idle holding the permit", |s| s.queries_idle == 1)
                    .await;
                a.send(&query(SLOW)).await;
                h.until("acme", "A waiting", |s| s.queries_waiting == 1)
                    .await;
                h.backend.kill(a.key.0);
                assert_served(&b.query("SELECT pg_advisory_unlock_all()").await, b'I');
            }
            _ => {}
        }
        const SLOW: &str = "select 'slow: never answered'";
        let slow = SLOW;
        match shape {
            Shape::Extended => a.send(&[cycle(slow), sync()].concat()).await,
            Shape::Redialed => {}
            _ => a.send(&query(slow)).await,
        }
        let ran_on = until_conn_ran(&h, slow).await;
        match shape {
            Shape::Pooled => assert_eq!(ran_on, a.key.0, "{name}: leased from the pool"),
            Shape::Redialed => assert_ne!(ran_on, a.key.0, "{name}: a fresh connection"),
            _ => assert_eq!(ran_on, a.key.0, "{name}"),
        }
        b.send(&query("select 'b'")).await;
        h.until("acme", "B waiting", |s| s.queries_waiting == 1)
            .await;

        // The relay gives up on the silent statement: A is told it failed
        // (outcome unknown, or the session closes under `tr_mode = none`).
        let told = frames_until_ready_or_closed(&mut a).await;
        assert!(
            told.iter().any(|f| f[0] == b'E') && told.iter().all(|f| f[0] != b'C'),
            "{name}: {told:?}"
        );
        // B is admitted only once the backend was told to cancel.
        assert_served(&b.until_ready().await, b'I');
        assert_cancelled_before(&h, ran_on, "select 'b'", &name);
    }
}

/// A session that ends while what an extended cycle dispatched may still
/// run on its backend (a pipelining client's `Execute` sent with a `Flush`,
/// or an extended COPY's end awaiting its `Sync`) returns its tenant permit
/// at its end; closing the connection alone would leave that statement
/// running until it next writes, outside the cap. However the session ends
/// (its client gone, a `Terminate`, `[limits] client_idle_timeout_secs`),
/// that backend gets a CancelRequest with the connection's BackendKeyData
/// before the permit returns and the tenant's next request is admitted.
#[tokio::test]
async fn a_session_ending_with_its_dispatched_cycle_running_cancels_it_before_its_permit_returns() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum End {
        ClientGone,
        Terminate,
        IdleTimeout,
        CopyEndClientGone,
    }
    for pooled in poolings() {
        for end in [
            End::ClientGone,
            End::Terminate,
            End::IdleTimeout,
            End::CopyEndClientGone,
        ] {
            let shape = format!("{end:?}, transaction pooling {pooled}");
            let h = Harness::new(5000, None, |c| {
                if end == End::IdleTimeout {
                    c.limits.client_idle_timeout_secs = 1;
                }
                transaction_pooling(c, pooled);
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            let conn = a.key.0;
            if end == End::CopyEndClientGone {
                // The backend never answers this COPY's end.
                a.send(&[cycle("COPY t FROM STDIN hang"), sync()].concat())
                    .await;
                assert_eq!(tags(&a.frames(3).await), b"12G", "{shape}");
                a.send(&[wire(b'd', b"1\n"), wire(b'c', &[])].concat())
                    .await;
                h.until_conn_got(conn, b'c').await;
            } else {
                // The backend runs the flushed Execute until released.
                a.send(&[cycle("select 'slow: flushed'"), flush()].concat())
                    .await;
                assert_eq!(tags(&a.frames(2).await), b"12", "{shape}");
                h.until_conn_got(conn, b'E').await;
            }
            h.until("acme", "A's permit", |s| s.queries_active == 1)
                .await;
            b.send(&query("select 'b'")).await;
            h.until("acme", "B waiting", |s| s.queries_waiting == 1)
                .await;

            let Client { s, task, .. } = a;
            let mut kept = Some(s);
            match end {
                End::Terminate => kept
                    .as_mut()
                    .expect("A's client")
                    .write_all(&wire(b'X', &[]))
                    .await
                    .unwrap(),
                End::ClientGone | End::CopyEndClientGone => drop(kept.take()),
                End::IdleTimeout => {}
            }
            tokio::time::timeout(TEST_TIMEOUT, task)
                .await
                .expect("A's session ends")
                .unwrap()
                .unwrap();
            assert_served(&b.until_ready().await, b'I');
            assert_cancelled_before(&h, conn, "select 'b'", &shape);
            drop(kept);
        }
    }
}

/// Only a session whose tenant has an active-query cap waits for the
/// CancelRequest of a statement it abandons, so that its permit returns only
/// after the cancel was sent. Every other session (no `[multi_tenancy]`, or
/// a tenant with only a replay budget) sends it from a task of its own: a
/// backend host that never answers it does not hold up the session's fault
/// path for `[limits] backend_cancel_timeout_ms`. The cancel still reaches
/// the backend, naming the connection that ran the statement.
#[tokio::test]
async fn only_a_capped_session_waits_for_the_cancel_of_what_it_abandons() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Tenancy {
        Off,
        ReplayBudgetOnly,
        Capped,
    }
    const READ_TIMEOUT_SECS: u64 = 1;
    const CANCEL_TIMEOUT_MS: u64 = 2000;
    let both = Duration::from_secs(READ_TIMEOUT_SECS) + Duration::from_millis(CANCEL_TIMEOUT_MS);
    for tenancy in [Tenancy::Off, Tenancy::ReplayBudgetOnly, Tenancy::Capped] {
        let name = format!("{tenancy:?}");
        let h = Harness::new(5000, None, |c| {
            c.limits.backend_read_timeout_secs = READ_TIMEOUT_SECS;
            c.limits.backend_cancel_timeout_ms = CANCEL_TIMEOUT_MS;
            match tenancy {
                Tenancy::Off => c.multi_tenancy.enabled = false,
                Tenancy::ReplayBudgetOnly => {
                    c.multi_tenancy.max_active_queries_per_tenant = 0;
                    c.multi_tenancy.max_replay_bytes_per_tenant = 1 << 20;
                }
                Tenancy::Capped => {}
            }
        })
        .await;
        // The backend takes a CancelRequest but never closes its connection.
        h.backend
            .ctl
            .stall_cancels
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let mut a = h.connect("acme").await;
        if tenancy == Tenancy::ReplayBudgetOnly {
            let s = h.stats("acme");
            assert_eq!((s.query_limit, s.replay_byte_limit), (0, 1 << 20));
        }
        let slow = "select 'slow: never answered'";
        let started = std::time::Instant::now();
        a.send(&query(slow)).await;
        let ran_on = until_conn_ran(&h, slow).await;
        let told = frames_until_ready_or_closed(&mut a).await;
        let took = started.elapsed();
        assert!(told.iter().any(|f| f[0] == b'E'), "{name}: {told:?}");
        h.until_backend_cancels(1).await;
        let cancels: Vec<String> = h
            .backend
            .log()
            .into_iter()
            .filter(|s| s.tag == b'!')
            .map(|s| s.text)
            .collect();
        assert_eq!(
            cancels,
            vec![format!("cancel {ran_on} {}", ran_on + 1000)],
            "{name}"
        );
        if tenancy == Tenancy::Capped {
            assert!(
                took >= both,
                "{name}: the capped session waited for the cancel: {took:?}"
            );
        } else {
            assert!(
                took < both,
                "{name}: the fault path waited for the cancel: {took:?}"
            );
        }
    }
}

/// The CancelRequest for a statement abandoned on a backend connection goes
/// to the server that connection is open to, not to whatever its node's
/// name resolves to now (a name with several addresses, or one changed since
/// the connection opened): the key it carries names a process of that
/// server alone, and another one ignores it while the statement runs on.
/// Here the node's name leads to `elsewhere`, while the connection runs on
/// `running`.
#[tokio::test]
async fn an_abandoned_statements_cancel_goes_to_the_server_its_connection_is_open_to() {
    let running = FakeBackend::start(None).await;
    let elsewhere = FakeBackend::start(None).await;
    let server = ProxyServer::new(test_config()).unwrap();
    let mut bc = BackendConn::new(TcpStream::connect(running.addr).await.unwrap());
    bc.backend_key = Some((7, 1007));
    let node = elsewhere.addr.to_string();
    ProxyServer::cancel_abandoned(bc, &node, &make_test_session(), &server.state).await;
    let cancels = |b: &FakeBackend| -> Vec<String> {
        b.log()
            .into_iter()
            .filter(|s| s.tag == b'!')
            .map(|s| s.text)
            .collect()
    };
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while cancels(&running).is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the server running the statement never got its cancel; elsewhere got {:?}",
            cancels(&elsewhere)
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(cancels(&running), vec!["cancel 7 1007".to_string()]);
    assert!(cancels(&elsewhere).is_empty(), "{:?}", cancels(&elsewhere));
}

/// The same for a client's own CancelRequest: it goes to the server whose
/// connection issued the key it names, not to whatever that node's name
/// resolves to now (several addresses, a name a failover or switchover moved
/// since the session connected), which ignores the key while the statement
/// runs on, holding its session's permit. Here the node's name leads to the
/// harness backend (`elsewhere`), while the session's connection runs on
/// `running`; both startup paths register the key (the relayed
/// authentication, and the proxy's own with an `auth_file`).
#[tokio::test]
async fn a_client_cancel_goes_to_the_server_its_key_came_from() {
    let dir = tempfile::tempdir().unwrap();
    let users = dir.path().join("users.txt");
    std::fs::write(&users, "acme:pw\n").unwrap();
    let cancels = |b: &FakeBackend| -> Vec<String> {
        b.log()
            .into_iter()
            .filter(|s| s.tag == b'!')
            .map(|s| s.text)
            .collect()
    };
    for proxy_auth in [false, true] {
        let running = FakeBackend::start(None).await;
        let h = Harness::on(FakeBackend::start(None).await, 0, |c| {
            if proxy_auth {
                c.auth.mode = crate::config::AuthMode::Scram;
                c.auth.auth_file = Some(users.to_string_lossy().into_owned());
            }
        });
        assert_eq!(h.state.auth_file.is_some(), proxy_auth);
        let node = h.backend.addr.to_string();
        let mut backend = TcpStream::connect(running.addr).await.unwrap();
        backend.write_all(&startup_bytes("acme")).await.unwrap();
        let (client_raw, _client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let session = make_test_session();
        ProxyServer::authenticate_backend(
            &mut client,
            &mut backend,
            &session,
            &h.state,
            "acme",
            &node,
            &mut false,
        )
        .await
        .expect("startup completes");
        let key = *session.cancel_key.get().expect("the key was registered");

        h.cancel(key).await;
        assert_eq!(
            cancels(&running),
            vec![format!("cancel {} {}", key.0, key.1)],
            "proxy auth {proxy_auth}: the server running the session got its cancel; elsewhere got {:?}",
            cancels(&h.backend)
        );
        assert!(
            cancels(&h.backend).is_empty(),
            "proxy auth {proxy_auth}: {:?}",
            cancels(&h.backend)
        );
    }
}

/// Read offload with a `route=standby` hint: a pipelining client's
/// `Flush`-dispatched read that the hint sent to a standby (the session's
/// home, which the idle watch covers otherwise, stays on the primary) gets
/// the rest of its results while the client waits for them before sending
/// anything else, as the idle watch covers the connection holding the open
/// cycle. Unwatched, they sat in the standby's socket while the proxy waited
/// for the client, and the session held its tenant permit until the client
/// gave up. When that connection is lost meanwhile, the session ends with
/// 08006 as on the home node, and the permit returns.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn a_flushed_cycle_a_hint_sent_to_a_standby_is_relayed_while_its_client_waits() {
    for lost in [false, true] {
        let shape = format!("lost {lost}");
        let standby = FakeBackend::start(None).await;
        let standby_addr = standby.addr.to_string();
        let h = Harness::on(FakeBackend::start(None).await, 0, |c| {
            c.add_node(&standby_addr, "standby").unwrap();
            c.load_balancer.standby_reads = crate::config::StandbyReads::Async;
            c.load_balancer.read_your_writes_ms = 0;
            c.routing_hints.enabled = true;
        });
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        let sql = if lost {
            "/*helios:route=standby*/ select 'hang: on the standby'"
        } else {
            "/*helios:route=standby*/ select 'slow: on the standby'"
        };
        a.send(&[cycle(sql), flush()].concat()).await;
        assert_eq!(tags(&a.frames(2).await), b"12", "{shape}");
        // The standby runs the Execute; the primary never saw the cycle.
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        let conn = loop {
            if let Some(seen) = standby.log().iter().find(|s| s.tag == b'E') {
                break seen.conn;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{shape}: the standby never received the Execute"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(
            h.backend.log().iter().all(|s| s.tag != b'P'),
            "{shape}: {:?}",
            h.backend.log()
        );
        h.until("acme", "A's permit", |s| s.queries_active == 1)
            .await;
        assert_refused(&b.query("select 'b'").await, "53300", REFUSED);
        // The session idles, waiting for its client.
        tokio::time::sleep(Duration::from_millis(50)).await;
        if lost {
            standby.kill(conn);
            let fatal = a.frame().await;
            assert_eq!(sqlstate(&fatal), "08006", "{shape}: {fatal:?}");
            assert!(
                has_text(&fatal, "extended-protocol cycle"),
                "{shape}: {fatal:?}"
            );
            let mut rest = Vec::new();
            tokio::time::timeout(TEST_TIMEOUT, a.s.read_to_end(&mut rest))
                .await
                .expect("the session closes")
                .unwrap();
            assert!(rest.is_empty(), "{shape}: {rest:?}");
        } else {
            standby.release_slow();
            // The Execute's result, before the client sends anything else.
            assert_eq!(tags(&a.frames(1).await), b"C", "{shape}");
            a.send(&sync()).await;
            assert_eq!(a.until_ready().await, vec![wire(b'Z', b"I")], "{shape}");
            assert_eq!(
                standby
                    .frames_of(conn)
                    .iter()
                    .map(|s| s.tag)
                    .filter(|t| matches!(t, b'P' | b'B' | b'E' | b'H' | b'S'))
                    .collect::<Vec<u8>>(),
                b"PBEHS".to_vec(),
                "{shape}: the cycle and its Sync on one standby connection"
            );
        }
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
}

/// [`Harness::on`] under an authoritative topology: a `[topology]` provider
/// names the write leader ([`lead`]), at first the configured primary.
#[cfg(feature = "routing-hints")]
fn authoritative_harness(
    backend: FakeBackend,
    wait_ms: u64,
    tweak: impl FnOnce(&mut ProxyConfig),
) -> (Harness, Arc<PrimaryTracker>) {
    let Harness {
        mut server,
        state,
        config,
        backend,
    } = Harness::on(backend, wait_ms, tweak);
    drop(state);
    let tracker = Arc::new(PrimaryTracker::new_standalone());
    let shared = Arc::get_mut(&mut server.state).expect("fresh server state is unshared");
    shared.authoritative_topology = true;
    shared.primary_tracker = tracker.clone();
    shared.routing = Arc::new(crate::routing_snapshot::RoutingView::new(true));
    let h = Harness {
        state: server.state.clone(),
        server,
        config,
        backend,
    };
    lead(&h, &tracker, &h.backend.addr.to_string());
    (h, tracker)
}

/// What the routing refresher does on a provider event: `addr` becomes the
/// write leader and the routing snapshot is republished.
#[cfg(feature = "routing-hints")]
fn lead(h: &Harness, tracker: &PrimaryTracker, addr: &str) {
    tracker.set_primary(uuid::Uuid::new_v4(), addr.to_string());
    ProxyServer::publish_routing(&h.state);
}

/// Read offload with a `route=standby` hint: a pipelining client's
/// `Flush`-dispatched read runs on a standby while the session's home stays
/// on the primary. A fault of that home can end the session at the cycle's
/// next message, before it reaches the standby: the home lost the write
/// leadership meanwhile (an authoritative topology moved it), or, after a
/// plugin's Block left the session an idle holder behind a ReadyForQuery,
/// the home's connection died idle. The read may still run on the standby,
/// so the recovery cancels it there (a CancelRequest with that connection's
/// BackendKeyData) before the session's tenant permit returns and the
/// tenant's next request is admitted. The recovery used to forget the cycle
/// first, and the session's end then closed the standby's connection
/// unannounced, its statement running on outside the cap.
#[cfg(feature = "routing-hints")]
#[tokio::test]
async fn a_home_fault_ending_a_session_cancels_its_cycle_on_a_standby() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Loss {
        HomeMoved,
        HomeDiedIdle,
    }
    let mut losses = vec![Loss::HomeMoved];
    if cfg!(feature = "wasm-plugins") {
        losses.push(Loss::HomeDiedIdle);
    }
    for loss in losses {
        let shape = format!("{loss:?}");
        #[cfg(feature = "wasm-plugins")]
        let plugins = hook_plugin_dir_for(
            "pre_query",
            "blocked",
            r#"{"action": "block", "value": "not here"}"#,
        );
        let standby = FakeBackend::start(None).await;
        let standby_addr = standby.addr.to_string();
        let tweak = |c: &mut ProxyConfig| {
            c.add_node(&standby_addr, "standby").unwrap();
            c.load_balancer.standby_reads = crate::config::StandbyReads::Async;
            c.load_balancer.read_your_writes_ms = 0;
            c.routing_hints.enabled = true;
            #[cfg(feature = "wasm-plugins")]
            if loss == Loss::HomeDiedIdle {
                plugins_from(c, &plugins);
            }
        };
        let primary = FakeBackend::start(None).await;
        let (h, tracker) = if loss == Loss::HomeMoved {
            let (h, tracker) = authoritative_harness(primary, 5000, tweak);
            (h, Some(tracker))
        } else {
            (Harness::on(primary, 5000, tweak), None)
        };
        let mut a = h.connect("acme").await;
        a.send(
            &[
                cycle("/*helios:route=standby*/ select 'slow: on the standby'"),
                flush(),
            ]
            .concat(),
        )
        .await;
        assert_eq!(tags(&a.frames(2).await), b"12", "{shape}");
        let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
        let conn = loop {
            if let Some(seen) = standby.log().iter().find(|s| s.tag == b'E') {
                break seen.conn;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{shape}: the standby never received the Execute"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        h.until("acme", "A's permit", |s| s.queries_active == 1)
            .await;
        let mut b = match tracker.as_ref() {
            Some(tracker) => {
                // A switchover: the standby running the read is promoted.
                lead(&h, tracker, &standby_addr);
                h.connect("acme").await
            }
            None => {
                let b = h.connect("acme").await;
                // A Query continuing the cycle, blocked by the plugin: the
                // cycle stays open on the standby, the client idles behind
                // its ReadyForQuery, and the idle watch covers the home.
                let blocked = a.query("select 'blocked'").await;
                assert_eq!(tags(&blocked), b"EZ", "{shape}");
                h.until("acme", "an idle holder", |s| {
                    s.queries_idle == 1 && s.queries_active == 1
                })
                .await;
                h.backend.kill(a.key.0);
                // The watch sees the loss; the cycle's connection is still
                // cached, so the permit stays held.
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(h.stats("acme").queries_active, 1, "{shape}");
                b
            }
        };
        // B (a statement the plugin lets through) waits for A's permit.
        b.send(&[cycle("select 'b'"), sync()].concat()).await;
        h.until("acme", "B waiting", |s| s.queries_waiting == 1)
            .await;

        // A's Sync is a fault on its home: the session ends.
        a.send(&sync()).await;
        let told = frames_until_ready_or_closed(&mut a).await;
        assert!(
            told.iter().all(|f| !matches!(f[0], b'C' | b'Z')),
            "{shape}: {told:?}"
        );
        let Client { task, .. } = a;
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("A's session ends")
            .unwrap()
            .unwrap();
        let served = b.until_ready().await;
        assert_eq!(tags(&served), b"12CZ", "{shape}");
        assert_served(&served, b'I');

        let on_standby = standby.log();
        let cancels: Vec<&Seen> = on_standby.iter().filter(|s| s.tag == b'!').collect();
        assert_eq!(cancels.len(), 1, "{shape}: {on_standby:?}");
        assert_eq!(
            cancels[0].text,
            format!("cancel {conn} {}", conn + 1000),
            "{shape}: the cancel names the standby connection running the read"
        );
        let on_primary = h.backend.log();
        let admitted = on_primary
            .iter()
            .chain(on_standby.iter())
            .find(|s| s.tag == b'P' && s.text == "select 'b'")
            .expect("B's cycle reached a backend");
        assert!(
            cancels[0].at < admitted.at,
            "{shape}: the cancel reached the standby before B was admitted"
        );
        assert!(
            !on_standby.iter().any(|s| s.conn == conn && s.tag == b'S'),
            "{shape}: A's Sync never reached the standby"
        );
        h.until("acme", "the permit returned", |s| {
            s.queries_active == 0 && s.queries_idle == 0
        })
        .await;
    }
}

/// The BackendKeyData a backend's startup frames carry is read wherever it
/// sits among them; a truncated frame, a short `K` or none at all gives no
/// key (and no cancel for that connection).
#[test]
fn the_backend_key_is_read_from_the_startup_frames() {
    let key = [0, 0, 0, 7, 0, 0, 3, 239];
    assert_eq!(
        ProxyServer::backend_key_data(
            &[
                wire(b'S', b"DateStyle\0ISO, MDY\0"),
                wire(b'K', &key),
                wire(b'Z', b"I"),
            ]
            .concat()
        ),
        Some((7, 1007))
    );
    assert_eq!(
        ProxyServer::backend_key_data(&[wire(b'S', b"a\0b\0"), wire(b'Z', b"I")].concat()),
        None
    );
    assert_eq!(ProxyServer::backend_key_data(&wire(b'K', &key[..4])), None);
    assert_eq!(ProxyServer::backend_key_data(&wire(b'K', &key)[..9]), None);
    assert_eq!(ProxyServer::backend_key_data(&[b'K', 0, 0, 0, 2]), None);
    assert_eq!(ProxyServer::backend_key_data(&[]), None);
}

/// `[rate_limit] max_concurrent`: the in-flight cap per rate-limit bucket,
/// through the whole client handler against the same fake backend. The
/// bucket is the user (`key_by` defaults to `user`).
#[cfg(feature = "rate-limiting")]
mod max_concurrent {
    use super::*;
    use std::sync::atomic::Ordering;

    const REFUSED_ALICE: &str = "too many concurrent queries for user:alice (max_concurrent 1)";

    struct Rate {
        server: ProxyServer,
        state: Arc<ServerState>,
        config: ProxyConfig,
        backend: FakeBackend,
    }

    impl Rate {
        async fn new(tweak: impl FnOnce(&mut ProxyConfig)) -> Self {
            let backend = FakeBackend::start(None).await;
            let mut config = test_config();
            config.nodes.clear();
            config
                .add_node(&backend.addr.to_string(), "primary")
                .unwrap();
            config.pool.acquire_timeout_secs = 1;
            config.limits.startup_timeout_secs = 3;
            config.rate_limit.enabled = true;
            config.rate_limit.max_concurrent = 1;
            tweak(&mut config);
            let server = ProxyServer::new(config.clone()).unwrap();
            let state = server.state.clone();
            Self {
                server,
                state,
                config,
                backend,
            }
        }

        async fn connect(&self, user: &str) -> Client {
            let (proxy, mut s) = pair().await;
            let peer = proxy.peer_addr().unwrap();
            let task = tokio::spawn(ProxyServer::handle_client(
                proxy,
                peer,
                self.state.clone(),
                Arc::new(self.config.clone()),
                self.server.shutdown_tx.clone(),
            ));
            s.write_all(&startup_bytes(user)).await.unwrap();
            let mut client = Client {
                s,
                key: (0, 0),
                task,
            };
            for f in client.until_ready().await {
                if f[0] == b'K' {
                    client.key = (
                        u32::from_be_bytes(f[5..9].try_into().unwrap()),
                        u32::from_be_bytes(f[9..13].try_into().unwrap()),
                    );
                }
            }
            client
        }

        /// The bucket's in-flight count for `user` right now.
        fn in_flight(&self, user: &str) -> u32 {
            let limiter = self.state.rate_limiter.as_ref().expect("enabled");
            limiter.in_flight(&crate::rate_limit::CachedLimiterKey::new(
                crate::rate_limit::LimiterKey::User(user.to_string()),
            ))
        }

        /// Wait until `user`'s bucket has no request in flight.
        async fn until_idle(&self, user: &str) {
            let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
            while self.in_flight(user) != 0 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{user}'s bucket never went idle"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        /// Send `sql` on `client` and wait until the backend has it (it is
        /// held in flight until `release_slow`).
        async fn hold(&self, client: &mut Client, sql: &str) {
            client.send(&query(sql)).await;
            let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
            while !self.backend.saw_query(sql) {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "never forwarded {sql:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        fn refusals(&self) -> u64 {
            self.state
                .metrics
                .rate_limit_concurrency_refused
                .load(Ordering::Relaxed)
        }
    }

    /// The frames are exactly ErrorResponse 53400 (naming the bucket) then
    /// ReadyForQuery `status`, with the statement never forwarded.
    fn assert_bucket_refused(frames: &[Vec<u8>], status: u8) {
        assert_eq!(frames.len(), 2, "{frames:?}");
        assert_eq!(sqlstate(&frames[0]), "53400", "{frames:?}");
        assert!(has_text(&frames[0], REFUSED_ALICE), "{:?}", frames[0]);
        assert_eq!(frames[1], wire(b'Z', &[status]));
    }

    #[tokio::test]
    async fn a_second_query_in_the_bucket_is_refused_while_the_first_is_in_flight() {
        let r = Rate::new(|_| {}).await;
        let mut a = r.connect("alice").await;
        let mut b = r.connect("alice").await;
        r.hold(&mut a, "select 'slow a'").await;
        assert_eq!(r.in_flight("alice"), 1);

        assert_bucket_refused(&b.query("select 'b'").await, b'I');
        assert!(
            !r.backend.saw_query("select 'b'"),
            "a refusal never reaches the backend"
        );
        assert_eq!(r.refusals(), 1);

        r.backend.release_slow();
        assert_served(&a.until_ready().await, b'I');
        r.until_idle("alice").await;
        assert_served(&b.query("select 'b'").await, b'I');
        assert_eq!(r.refusals(), 1, "the second attempt was admitted");
        r.until_idle("alice").await;
    }

    #[tokio::test]
    async fn another_bucket_is_not_refused() {
        let r = Rate::new(|_| {}).await;
        let mut a = r.connect("alice").await;
        let mut bob = r.connect("bob").await;
        r.hold(&mut a, "select 'slow a'").await;

        assert_served(&bob.query("select 'bob'").await, b'I');
        assert_eq!(r.refusals(), 0);

        r.backend.release_slow();
        assert_served(&a.until_ready().await, b'I');
        r.until_idle("alice").await;
    }

    #[tokio::test]
    async fn a_zero_cap_is_no_cap() {
        let r = Rate::new(|c| c.rate_limit.max_concurrent = 0).await;
        let mut a = r.connect("alice").await;
        let mut b = r.connect("alice").await;
        r.hold(&mut a, "select 'slow a'").await;

        assert_served(&b.query("select 'b'").await, b'I');
        assert_eq!(r.in_flight("alice"), 0, "nothing is counted without a cap");
        assert_eq!(r.refusals(), 0);

        r.backend.release_slow();
        assert_served(&a.until_ready().await, b'I');
    }

    async fn mixed_rate_completion_cycle(prior: bool) {
        for wait_ready in [true, false] {
            let r = Rate::new(|c| c.optimize_unnamed_parse = false).await;
            let mut a = r.connect("alice").await;
            let mut b = r.connect("alice").await;
            if prior {
                b.send(&[parse("work", "UPDATE rate_ledger SET v=v+1"), sync()].concat())
                    .await;
                assert_eq!(tags(&b.until_ready().await), b"1Z");
                r.until_idle("alice").await;
            }
            r.hold(&mut a, "select 'slow holder'").await;
            assert_eq!(r.in_flight("alice"), 1);
            let before = r.backend.log().len();
            let mut batch = parse("end", "COMMIT");
            if !prior {
                batch.extend(parse("work", "UPDATE rate_ledger SET v=v+1"));
            }
            batch.extend(bind("work"));
            batch.extend(execute());
            batch.extend(if wait_ready { sync() } else { flush() });
            b.send(&batch).await;
            if wait_ready {
                assert_bucket_refused(&b.until_ready().await, b'I');
            } else {
                let error = b.frame().await;
                assert_eq!(sqlstate(&error), "53400", "prior={prior}: {error:?}");
                assert!(has_text(&error, REFUSED_ALICE));
                b.send(&sync()).await;
                assert_eq!(b.until_ready().await, vec![wire(b'Z', b"I")]);
            }
            assert_eq!(r.in_flight("alice"), 1, "mixed work took no extra slot");
            assert!(
                r.backend.log()[before..]
                    .iter()
                    .all(|s| !matches!(s.tag, b'P' | b'B' | b'E')),
                "mixed batch reached the backend: {:?}",
                r.backend.log()
            );
            r.backend.release_slow();
            assert_served(&a.until_ready().await, b'I');
            r.until_idle("alice").await;
            assert_served(&b.query("select 'healthy'").await, b'I');
            assert_eq!(r.refusals(), 1);
        }
    }

    #[tokio::test]
    async fn rate_completion_mixed_fresh_work_is_gated() {
        mixed_rate_completion_cycle(false).await;
    }

    #[tokio::test]
    async fn rate_completion_prior_work_is_gated() {
        mixed_rate_completion_cycle(true).await;
    }

    #[tokio::test]
    async fn rate_completion_standalone_unnamed_commit_still_progresses() {
        for promoted in [false, true] {
            let r = Rate::new(|c| c.optimize_unnamed_parse = promoted).await;
            let mut a = r.connect("alice").await;
            let mut b = r.connect("alice").await;
            assert_served(&b.query("BEGIN").await, b'T');
            r.hold(&mut a, "select 'slow holder'").await;
            b.send(&[cycle("COMMIT"), sync()].concat()).await;
            let reply = b.until_ready().await;
            assert_served(&reply, b'I');
            assert_eq!(tags(&reply), b"12CZ");
            assert_eq!(r.in_flight("alice"), 1);
            assert_eq!(r.refusals(), 0);
            r.backend.release_slow();
            assert_served(&a.until_ready().await, b'I');
            r.until_idle("alice").await;
        }
    }

    #[tokio::test]
    async fn commit_is_exempt_from_the_bucket_cap_inside_a_transaction() {
        let r = Rate::new(|_| {}).await;
        let mut a = r.connect("alice").await;
        let mut b = r.connect("alice").await;
        assert_served(&b.query("BEGIN").await, b'T');
        r.hold(&mut a, "select 'slow a'").await;

        assert_bucket_refused(&b.query("select 'b'").await, b'T');
        assert_served(&b.query("COMMIT").await, b'I');
        assert!(
            r.backend.saw_query("COMMIT"),
            "the exempt COMMIT reached the backend"
        );
        assert_eq!(r.refusals(), 1, "only the select was refused");

        r.backend.release_slow();
        assert_served(&a.until_ready().await, b'I');
        r.until_idle("alice").await;
    }

    #[tokio::test]
    async fn an_error_response_releases_the_slot() {
        let r = Rate::new(|_| {}).await;
        let mut a = r.connect("alice").await;
        let mut b = r.connect("alice").await;
        let frames = a.query("select * from missing_table").await;
        assert_eq!(sqlstate(&frames[0]), "42P01", "{frames:?}");
        r.until_idle("alice").await;

        assert_served(&b.query("select 'b'").await, b'I');
        assert_eq!(r.refusals(), 0);
    }

    #[tokio::test]
    async fn a_disconnect_while_in_flight_releases_the_slot() {
        let r = Rate::new(|_| {}).await;
        let mut a = r.connect("alice").await;
        let mut b = r.connect("alice").await;
        r.hold(&mut a, "select 'slow a'").await;
        assert_eq!(r.in_flight("alice"), 1);

        drop(a);
        r.backend.release_slow();
        r.until_idle("alice").await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
}

/// A statement the row filter cannot cover is answered with SQLSTATE 42501
/// and ReadyForQuery, and the backend never receives it; the session stays
/// usable for the next statement.
#[tokio::test]
async fn a_refused_tenant_statement_never_reaches_the_backend() {
    let h = Harness::new(0, None, |c| {
        c.multi_tenancy.tenant_tables = vec!["orders".into()];
    })
    .await;
    let mut acme = h.connect("acme").await;
    let refused = "WITH o AS (SELECT * FROM orders) SELECT * FROM o";
    let frames = acme.query(refused).await;
    assert_refused(&frames, "42501", "row-level tenant isolation");
    assert!(
        !h.backend.saw_query(refused),
        "the refused statement reached the backend"
    );
    let frames = acme.query("SELECT 1").await;
    assert_served(&frames, b'I');
}

/// Tenants are identified by the startup database (`identify_by =
/// "database"`), with `orders` as the tenant table.
fn by_database(c: &mut ProxyConfig) {
    c.multi_tenancy.identify_by = "database".into();
    c.multi_tenancy.tenant_tables = vec!["orders".into()];
}

/// `[multi_tenancy] unknown_tenant = "refuse"` (the default): a session whose
/// database names no tenant cannot run a statement on a tenant table, since
/// it could not be row-filtered; the statement is refused with 42501, never
/// forwarded, and the session stays usable.
#[tokio::test]
async fn an_unidentified_session_is_refused_on_tenant_tables() {
    let h = Harness::on(FakeBackend::start(None).await, 0, by_database);
    let mut u = h.connect_db("u", "nobody").await;
    let refused = u.query("select * from orders").await;
    assert_refused(&refused, "42501", "no identified tenant");
    assert!(
        !h.backend.saw_query("select * from orders"),
        "never forwarded"
    );
    assert_served(&u.query("select 1").await, b'I');
}

/// `unknown_tenant = "passthrough"` keeps the unfiltered forward.
#[tokio::test]
async fn an_unidentified_session_is_forwarded_under_passthrough() {
    let h = Harness::on(FakeBackend::start(None).await, 0, |c| {
        by_database(c);
        c.multi_tenancy.unknown_tenant = crate::config::UnknownTenantPolicy::Passthrough;
    });
    let mut u = h.connect_db("u", "nobody").await;
    assert_served(&u.query("select * from orders").await, b'I');
    assert!(h.backend.saw_query("select * from orders"));
}

/// `acme@feat` is tenant `acme`: its tenant table statements get acme's
/// row filter, and its requests count against acme's active-query cap.
#[tokio::test]
async fn a_branch_database_gets_its_base_tenants_filter_and_caps() {
    let h = Harness::on(FakeBackend::start(None).await, 0, by_database);
    let mut a = h.connect_db("u", "acme@feat").await;
    assert_served(&a.query("select * from orders").await, b'I');
    let filtered = h.backend.log().into_iter().any(|s| {
        s.tag == b'Q' && s.text.to_lowercase().contains("where") && s.text.contains("acme")
    });
    assert!(filtered, "acme's row filter reached the backend");

    h.hold_permit(&mut a, "select 'slow a'").await;
    h.until("acme", "the branch session holds acme's permit", |s| {
        s.queries_active == 1
    })
    .await;
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
}

/// COPY's extended transaction still owns its tenant permit until Sync.
/// Completing/failing the input must not turn the configured COPY timer off.
#[tokio::test]
async fn copy_stall_deadline_requires_final_sync_after_done_or_fail() {
    for done in [true, false] {
        let h = Harness::new(0, None, |config| {
            config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        a.send(&[cycle("COPY data FROM STDIN"), flush()].concat())
            .await;
        assert_eq!(tags(&a.frames(3).await), b"12G");
        a.send(&wire(
            if done { b'c' } else { b'f' },
            if done { b"" } else { b"cancelled\0" },
        ))
        .await;
        let answer = a.frame().await;
        assert_eq!(answer[0], if done { b'C' } else { b'E' });
        assert_refused(&b.query("select 'still pinned'").await, "53300", REFUSED);
        let fatal = a.frame().await;
        assert_eq!(
            sqlstate(&fatal),
            "57014",
            "COPY final Sync must be bounded: {done}"
        );
        assert!(has_text(&fatal, "SFATAL"));
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, a.s.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        h.until("acme", "withheld COPY Sync returned capacity", |s| {
            s.queries_active == 0
        })
        .await;
        assert_served(&b.query("select 'capacity returned'").await, b'I');
    }
}

#[tokio::test]
async fn copy_stall_deadline_final_sync_cancels_timer_and_settles_outcome() {
    for done in [true, false] {
        let h = Harness::new(0, None, |config| {
            config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&[cycle("COPY data FROM STDIN"), flush()].concat())
            .await;
        assert_eq!(tags(&a.frames(3).await), b"12G");
        a.send(&wire(
            if done { b'c' } else { b'f' },
            if done { b"" } else { b"cancelled\0" },
        ))
        .await;
        assert_eq!(a.frame().await[0], if done { b'C' } else { b'E' });
        a.send(&sync()).await;
        assert_served(&a.until_ready().await, b'I');
        h.until("acme", "COPY Sync returned capacity", |s| {
            s.queries_active == 0
        })
        .await;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_served(&a.query("select 'after COPY Sync'").await, b'I');
    }
}

/// PostgreSQL may buffer CommandComplete until Flush. The proxy needs that
/// acknowledgement to start its final-Sync timer after backend work finishes.
#[tokio::test]
async fn copy_stall_deadline_flushes_extended_copy_completion() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    a.send(&[cycle("COPY flush_ack FROM STDIN"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12G");
    a.send(&wire(b'c', b"")).await;
    assert_eq!(a.frame().await[0], b'C');
    assert_refused(&b.query("select 'still pinned'").await, "53300", REFUSED);
    let fatal = a.frame().await;
    assert_eq!(sqlstate(&fatal), "57014");
    assert!(has_text(&fatal, "SFATAL"));
    h.until("acme", "buffered COPY completion returned capacity", |s| {
        s.queries_active == 0
    })
    .await;
    assert_served(&b.query("select 'capacity returned'").await, b'I');
}

/// A preceding statement's CommandComplete in this Flush cycle is not the
/// acknowledgement of COPY completion. Backend COPY work may exceed the
/// configured client input timeout before the final-Sync wait starts.
#[tokio::test]
async fn copy_stall_deadline_ignores_prior_cycle_completion_during_backend_work() {
    let h = Harness::new(0, None, |config| {
        config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
    })
    .await;
    let mut a = h.connect("acme").await;
    a.send(&[cycle("SELECT 42"), flush()].concat()).await;
    assert_eq!(tags(&a.frames(3).await), b"12C");
    a.send(&[cycle("COPY slow_end FROM STDIN"), flush()].concat())
        .await;
    assert_eq!(tags(&a.frames(3).await), b"12G");
    a.send(&wire(b'c', b"")).await;
    h.until_conn_got(a.key.0, b'c').await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    h.backend.release_slow();
    assert_eq!(
        a.frame().await[0],
        b'C',
        "backend work is not stalled client input"
    );
    a.send(&sync()).await;
    assert_served(&a.until_ready().await, b'I');
    h.until("acme", "mixed COPY cycle returned capacity", |s| {
        s.queries_active == 0
    })
    .await;
    assert_served(&a.query("select 'after COPY'").await, b'I');
}

/// The COPY completion retained before Sync must be committed once, then
/// cleared so a subsequent same-session Flush cycle keeps its own prefix.
#[tokio::test]
async fn journal_after_extended_copy_keeps_flushed_committed_prefix() {
    for done in [true, false] {
        let h = Harness::new(0, None, |config| {
            config.limits = toml::from_str("copy_idle_timeout_secs = 1").unwrap();
        })
        .await;
        let mut a = h.connect("acme").await;
        a.send(&[cycle("SELECT 42"), flush()].concat()).await;
        assert_eq!(tags(&a.frames(3).await), b"12C");
        a.send(&[cycle("COPY data FROM STDIN"), flush()].concat())
            .await;
        assert_eq!(tags(&a.frames(3).await), b"12G");
        a.send(&wire(
            if done { b'c' } else { b'f' },
            if done { b"" } else { b"cancelled\0" },
        ))
        .await;
        assert_eq!(a.frame().await[0], if done { b'C' } else { b'E' });
        a.send(&sync()).await;
        assert_eq!(tags(&a.until_ready().await), b"Z");
        h.until("acme", "COPY journal observation completed", |s| {
            s.queries_active == 0
        })
        .await;
        let copy_commits = u64::from(done);
        assert_eq!(
            h.state.metrics.journal_committed.load(Ordering::Relaxed),
            copy_commits
        );
        for sql in ["SELECT 42", "INSERT INTO t VALUES (1)"] {
            a.send(&[cycle(sql), flush()].concat()).await;
            assert_eq!(tags(&a.frames(3).await), b"12C");
        }
        assert_eq!(
            h.state.metrics.journal_committed.load(Ordering::Relaxed),
            copy_commits,
            "Flush did not commit"
        );
        a.send(&sync()).await;
        assert_eq!(tags(&a.until_ready().await), b"Z");
        h.until(
            "acme",
            "post-COPY write journal observation completed",
            |s| s.queries_active == 0,
        )
        .await;
        assert_eq!(
            h.state.metrics.journal_committed.load(Ordering::Relaxed),
            copy_commits + 1,
            "post-COPY flushed write completion was lost: done={done}"
        );
    }
}

/// Completion-only extended cycles release prepared locks without waiting for
/// a permit held by a transaction that may itself be waiting for those locks.
#[tokio::test]
async fn extended_2pc_complete_fresh_cycle_is_exempt_at_the_cap() {
    for pooled in poolings() {
        for promote in [false, true] {
            let h = Harness::new(0, None, |c| {
                transaction_pooling(c, pooled);
                c.optimize_unnamed_parse = promote;
            })
            .await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            assert_served(&a.query("BEGIN").await, b'T');
            for sql in ["COMMIT PREPARED 'x'", "ROLLBACK PREPARED 'y';"] {
                b.send(&[parse("", sql), bind(""), execute(), sync()].concat())
                    .await;
                let reply = b.until_ready().await;
                assert_eq!(
                    tags(&reply),
                    b"12CZ",
                    "{sql}, pooled={pooled}, promote={promote}"
                );
                assert!(h
                    .backend
                    .log()
                    .iter()
                    .any(|s| s.tag == b'P' && s.text == sql));
                assert_eq!(
                    h.stats("acme").queries_active,
                    1,
                    "completion borrowed no permit"
                );
            }
            // The exemption ends at its Sync even if the next cycle was
            // already pipelined in the same TCP write.
            b.send(
                &[
                    parse("", "ROLLBACK PREPARED 'pipeline'"),
                    bind(""),
                    execute(),
                    sync(),
                    parse("", "SELECT 1"),
                    bind(""),
                    execute(),
                    sync(),
                ]
                .concat(),
            )
            .await;
            assert_eq!(tags(&b.until_ready().await), b"12CZ");
            assert_refused(&b.until_ready().await, "53300", REFUSED);
            assert_refused(&b.query("SELECT 1").await, "53300", REFUSED);
            assert_served(&a.query("COMMIT").await, b'I');
            // RFQ publication precedes reset/checkin and permit release.
            // Wait for the actual ownership boundary before testing reuse.
            h.until("acme", "A commit cleanup returned its permit", |s| {
                s.queries_active == 0 && s.queries_waiting == 0
            })
            .await;
            let admitted = b.query("SELECT 1").await;
            assert_eq!(
                tags(&admitted),
                b"CZ",
                "after cleanup: pooled={pooled}, promote={promote}"
            );
            assert_served(&admitted, b'I');
        }
    }
}

#[tokio::test]
async fn extended_2pc_fragmented_cycle_is_exempt_at_the_cap() {
    for bytewise in [false, true] {
        for promote in [false, true] {
            let h = Harness::new(0, None, |c| c.optimize_unnamed_parse = promote).await;
            let mut a = h.connect("acme").await;
            let mut b = h.connect("acme").await;
            assert_served(&a.query("BEGIN").await, b'T');
            let frames = [
                parse("", "ROLLBACK PREPARED 'split'"),
                bind(""),
                execute(),
                sync(),
            ];
            if bytewise {
                for byte in frames.concat() {
                    b.send(&[byte]).await;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            } else {
                for frame in frames {
                    b.send(&frame).await;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            assert_eq!(
                tags(&b.until_ready().await),
                b"12CZ",
                "bytewise={bytewise}, promote={promote}"
            );
            assert_eq!(h.stats("acme").queries_active, 1);
            assert_served(&a.query("COMMIT").await, b'I');
        }
    }
}

/// An exemption must prove the entire fresh cycle, including its bound
/// statement and single execution, before any Parse reaches a backend.
#[tokio::test]
async fn extended_2pc_mixed_partial_and_prior_identity_stay_gated() {
    let sql = "COMMIT PREPARED 'x'";
    let shapes = [
        (
            "named definition",
            vec![parse("named", sql), bind("named"), execute(), sync()],
        ),
        (
            "different statement",
            vec![parse("", sql), bind("prior"), execute(), sync()],
        ),
        (
            "Parse parameter OID",
            vec![
                wire(
                    b'P',
                    &[cstring(""), cstring(sql), vec![0, 1, 0, 0, 0, 23]].concat(),
                ),
                bind(""),
                execute(),
                sync(),
            ],
        ),
        (
            "Bind parameter formats",
            vec![
                parse("", sql),
                wire(b'B', &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0]),
                execute(),
                sync(),
            ],
        ),
        (
            "Execute row limit",
            vec![
                parse("", sql),
                bind(""),
                wire(b'E', &[0, 0, 0, 0, 1]),
                sync(),
            ],
        ),
        ("prior portal", vec![execute(), sync()]),
        (
            "multiple executions",
            vec![parse("", sql), bind(""), execute(), execute(), sync()],
        ),
        (
            "redefinition",
            vec![
                parse("", "SELECT 1"),
                parse("", sql),
                bind(""),
                execute(),
                sync(),
            ],
        ),
        (
            "extra work",
            vec![
                parse("", sql),
                bind(""),
                execute(),
                parse("", "SELECT 1"),
                bind(""),
                execute(),
                sync(),
            ],
        ),
        (
            "Flush boundary",
            vec![parse("", sql), bind(""), execute(), flush(), sync()],
        ),
    ];
    for (name, messages) in shapes {
        let h = Harness::new(0, None, |_| {}).await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        // A previous named definition and portal must not supply identity
        // that makes a fresh completion-shaped request safe by accident.
        b.send(&[parse("prior", "SELECT 1"), bind("prior"), execute(), sync()].concat())
            .await;
        assert_served(&b.until_ready().await, b'I');
        assert_served(&a.query("BEGIN").await, b'T');
        let before = h.backend.log().len();
        b.send(&messages.concat()).await;
        assert_refused(&b.until_ready().await, "53300", REFUSED);
        assert_eq!(
            h.backend.log().len(),
            before,
            "{name}: no work reached backend"
        );
        assert_served(&a.query("COMMIT").await, b'I');
        assert_served(&b.query("SELECT 1").await, b'I');
    }
    for bind_arrived in [false, true] {
        let h = Harness::new(0, None, |_| {}).await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        assert_served(&a.query("BEGIN").await, b'T');
        let mut first = parse("", sql);
        if bind_arrived {
            first.extend(bind(""));
        }
        // Ending the Parse/Bind cycle without executing proves no
        // completion. A later Bind/Execute cannot reuse that exemption.
        first.extend(sync());
        b.send(&first).await;
        assert_refused(&b.until_ready().await, "53300", REFUSED);
        let rest = if bind_arrived {
            vec![execute(), sync()]
        } else {
            vec![bind(""), execute(), sync()]
        };
        b.send(&rest.concat()).await;
        assert_refused(&b.until_ready().await, "53300", REFUSED);
        assert!(!h
            .backend
            .log()
            .iter()
            .any(|s| s.tag == b'P' && s.text == sql));
        assert_served(&a.query("COMMIT").await, b'I');
    }
}

/// The statement hooks run before admission reads the cycle, so a completion
/// is proven on the statement as it will run: with rewrite rules, a tenant
/// row filter or plugins on, `COMMIT PREPARED` still runs past the cap (it
/// used to be refused there), and a statement a plugin rewrites into
/// something else is not exempt.
#[tokio::test]
async fn extended_2pc_completion_is_exempt_with_statement_hooks_on() {
    for mode in ["rewrite", "tenant tables", "plugins"] {
        let h = Harness::new(0, None, |c| match mode {
            "rewrite" => c.query_rewrite.enabled = true,
            "tenant tables" => c.multi_tenancy.tenant_tables = vec!["widgets".into()],
            "plugins" => c.plugins.enabled = true,
            _ => unreachable!(),
        })
        .await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        assert_served(&a.query("BEGIN").await, b'T');
        b.send(
            &[
                parse("", "COMMIT PREPARED 'x'"),
                bind(""),
                execute(),
                sync(),
            ]
            .concat(),
        )
        .await;
        assert_eq!(tags(&b.until_ready().await), b"12CZ", "{mode}");
        assert_eq!(
            h.stats("acme").queries_active,
            1,
            "{mode}: no permit borrowed"
        );
        assert_served(&a.query("COMMIT").await, b'I');
    }
}

/// A plugin that rewrites a `COMMIT PREPARED` into another statement leaves
/// nothing to exempt: the rewritten statement waits for a permit like any.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn extended_2pc_rewritten_into_other_work_stays_gated() {
    let plugins = hook_plugin_dir_for(
        "pre_query",
        "PREPARED 'swap'",
        r#"{"action": "rewrite", "value": "SELECT 'other work'"}"#,
    );
    let h = Harness::new(0, None, |c| plugins_from(c, &plugins)).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    let before = h.backend.log().len();
    b.send(
        &[
            parse("", "COMMIT PREPARED 'swap'"),
            bind(""),
            execute(),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_refused(&b.until_ready().await, "53300", REFUSED);
    assert_eq!(h.backend.log().len(), before);
    assert_served(&a.query("COMMIT").await, b'I');
}

/// The other shapes drivers send a completion in run past the cap too: a
/// `Describe` of the portal between `Bind` and `Execute` (pgjdbc), result
/// format codes on the `Bind`, and a named statement prepared earlier and
/// executed with `Bind`/`Execute`/`Sync` alone (a driver's server-side
/// prepared statement). Each is answered and borrows no permit.
#[tokio::test]
async fn extended_2pc_named_and_described_cycles_are_exempt_at_the_cap() {
    let describe_portal = || wire(b'D', b"P\0");
    let text_results = |stmt: &str| {
        wire(
            b'B',
            &[cstring(""), cstring(stmt), vec![0, 0, 0, 0, 0, 1, 0, 0]].concat(),
        )
    };
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    let mut b = h.connect("acme").await;
    // Prepared while a permit is free (the driver's first use).
    b.send(&[parse("fin", "ROLLBACK PREPARED 'named'"), sync()].concat())
        .await;
    assert_eq!(tags(&b.until_ready().await), b"1Z");
    assert_served(&a.query("BEGIN").await, b'T');
    type Cycle<'a> = (&'a str, Vec<Vec<u8>>, &'a [u8]);
    let cycles: [Cycle; 4] = [
        (
            "described",
            vec![
                parse("", "COMMIT PREPARED 'd'"),
                bind(""),
                describe_portal(),
                execute(),
                sync(),
            ],
            b"12nCZ",
        ),
        (
            "result formats",
            vec![
                parse("", "COMMIT PREPARED 'f'"),
                text_results(""),
                execute(),
                sync(),
            ],
            b"12CZ",
        ),
        ("named", vec![bind("fin"), execute(), sync()], b"2CZ"),
        (
            "named, described",
            vec![bind("fin"), describe_portal(), execute(), sync()],
            b"2nCZ",
        ),
    ];
    for (shape, frames, expected) in cycles {
        b.send(&frames.concat()).await;
        assert_eq!(tags(&b.until_ready().await), expected, "{shape}");
        assert_eq!(
            h.stats("acme").queries_active,
            1,
            "{shape}: no permit borrowed"
        );
    }
    // A named statement that is not a completion is gated as before.
    b.send(&[parse("other", "SELECT 1"), sync()].concat()).await;
    assert_refused(&b.until_ready().await, "53300", REFUSED);
    assert_served(&a.query("COMMIT").await, b'I');
}

/// Session pooling: a session-level advisory lock keeps its holder's permit,
/// and an unlock of all of them sent over the extended protocol returns it,
/// as the simple protocol's does: `pg_advisory_unlock_all()` and `DISCARD
/// ALL` in a cycle, and a prepared unlock executed with `Bind` alone. A cycle
/// whose unlock fails keeps the permit.
#[tokio::test]
async fn an_extended_unlock_returns_the_permit() {
    for unlock in [
        vec![cycle("SELECT pg_advisory_unlock_all()"), sync()],
        vec![cycle("DISCARD ALL"), sync()],
        vec![bind("unlock"), execute(), sync()],
    ] {
        let h = Harness::new(0, None, |_| {}).await;
        let mut a = h.connect("acme").await;
        let mut b = h.connect("acme").await;
        a.send(&[parse("unlock", "SELECT pg_advisory_unlock_all()"), sync()].concat())
            .await;
        assert_eq!(tags(&a.until_ready().await), b"1Z");
        assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
        h.until("acme", "A idle holding the permit", |s| {
            s.queries_active == 1 && s.queries_idle == 1
        })
        .await;
        assert_refused(&b.query("select 'b'").await, "53300", REFUSED);

        // An unlock the backend fails releases nothing.
        a.send(
            &[
                cycle("SELECT pg_advisory_unlock_all() /* violates */"),
                sync(),
            ]
            .concat(),
        )
        .await;
        let failed = a.until_ready().await;
        assert_eq!(sqlstate(&failed[failed.len() - 2]), "23505", "{failed:?}");
        h.until("acme", "A still holding the permit", |s| {
            s.queries_active == 1
        })
        .await;
        assert_refused(&b.query("select 'b'").await, "53300", REFUSED);

        a.send(&unlock.concat()).await;
        assert_served(&a.until_ready().await, b'I');
        h.until("acme", "the permit returned", |s| s.queries_active == 0)
            .await;
        assert_served(&b.query("select 'b'").await, b'I');
    }
}

mod simple_query_payload_regressions {
    use super::*;

    async fn rejected(h: &Harness, client: &mut Client, body: &[u8]) {
        let before = h.backend.log().len();
        client.send(&wire(b'Q', body)).await;
        let error = client.frame().await;
        assert_eq!(
            error[0], b'E',
            "malformed Query must not return a cached/native prefix response"
        );
        assert_eq!(sqlstate(&error), "08P01", "{error:?}");
        assert!(
            has_text(&error, "FATAL"),
            "structural failure must close rather than fabricate transaction state"
        );
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, client.s.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(
            h.backend.log()[before..].iter().all(|f| f.tag != b'Q'),
            "malformed Query must be rejected before any backend/probe dispatch"
        );
    }

    #[tokio::test]
    async fn simple_query_payload_handler_rejects_before_backend_dispatch() {
        let h = Harness::new(0, None, |_| {}).await;
        for body in [
            b"SELECT 42\0junk".as_slice(),
            b"SELECT 42",
            b"",
            b"SELECT 42\0\0",
        ] {
            let mut client = h.connect("acme").await;
            rejected(&h, &mut client, body).await;
        }
    }

    #[cfg(feature = "query-cache")]
    #[tokio::test]
    async fn simple_query_payload_warm_cache_cannot_mask_trailing_bytes() {
        let h = Harness::new(0, None, |c| {
            c.cache.enabled = true;
            c.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        })
        .await;
        let mut client = h.connect("acme").await;
        assert_served(&client.query("SELECT 42").await, b'I');
        let hits = || {
            let s = h.state.query_cache.as_ref().unwrap().stats();
            s.l1.hits + s.l2.hits + s.l3.hits
        };
        let before = hits();
        assert_served(&client.query("SELECT 42").await, b'I');
        assert!(hits() > before, "positive actual cache hit required");
        let before = hits();
        rejected(&h, &mut client, b"SELECT 42\0junk").await;
        assert_eq!(
            hits(),
            before,
            "malformed request cannot consult a warmed prefix cache key"
        );
    }

    #[tokio::test]
    async fn simple_query_payload_valid_empty_and_following_query_stay_live() {
        // No statement hook: the row filter of the harness's tenants refuses
        // text it cannot read (22021; `extended_hooks`).
        let h = Harness::new(0, None, |c| c.multi_tenancy.enabled = false).await;
        let mut client = h.connect("acme").await;
        assert_served(&client.query("").await, b'I');
        assert_served(&client.query("SELECT 42").await, b'I');
        // Structural validation cannot impose UTF-8 on a client's encoding.
        // This canned backend accepts these raw bytes; the Rust String parser
        // intentionally retains its separate UTF-8 contract.
        client.send(&wire(b'Q', b"SELECT '\xff'\0")).await;
        assert_served(&client.until_ready().await, b'I');
        assert!(h.backend.saw_query("SELECT '\u{fffd}'"));
    }

    #[cfg(feature = "query-rewriting")]
    #[tokio::test]
    async fn simple_query_payload_rewrite_cannot_clean_a_malformed_prefix() {
        let h = Harness::new(0, None, |c| {
            c.query_rewrite = toml::from_str(
                "enabled = true\n[[rules]]\nmatch_regex = '^SELECT 42$'\nadd_limit = 1\n",
            )
            .unwrap();
        })
        .await;
        let mut client = h.connect("acme").await;
        assert_served(&client.query("SELECT 42").await, b'I');
        assert!(
            h.backend
                .log()
                .iter()
                .any(|f| f.tag == b'Q' && f.text.to_ascii_lowercase().contains("limit 1")),
            "positive valid rewrite must actually run"
        );
        rejected(&h, &mut client, b"SELECT 42\0junk").await;
    }

    #[tokio::test]
    async fn simple_query_payload_direct_forward_cannot_bypass_validation() {
        let (backend_socket, mut peer) = pair().await;
        let target = backend_socket.peer_addr().unwrap().to_string();
        let backend = tokio::spawn(async move {
            let mut head = [0u8; 5];
            if peer.read_exact(&mut head).await.is_err() {
                return None;
            }
            assert_eq!(head[0], b'Q');
            let len = u32::from_be_bytes(head[1..].try_into().unwrap()) as usize;
            assert!((4..1024).contains(&len));
            let mut body = vec![0; len - 4];
            peer.read_exact(&mut body).await.unwrap();
            peer.write_all(&[wire(b'C', b"SELECT 1\0"), wire(b'Z', b"I")].concat())
                .await
                .unwrap();
            Some(body)
        });
        let mut config = test_config();
        config.nodes.clear();
        config.add_node(&target, "primary").unwrap();
        let server = ProxyServer::new(config.clone()).unwrap();
        let session = make_test_session();
        let mut conns = ConnMap::from_iter([(target.clone(), BackendConn::new(backend_socket))]);
        let (client, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client);
        let mut fault = None;
        let result = ProxyServer::forward_simple_query(
            ForwardCtx {
                client: &mut client,
                conns: &mut conns,
                current_node: Some(&target),
                reads: &ReadRouting::OFF,
                session: &session,
                state: &server.state,
                config: &config,
                fault: &mut fault,
                continues_cycle: None,
            },
            &crate::protocol::Message::new(
                crate::protocol::MessageType::Query,
                BytesMut::from(b"SELECT 42\0junk".as_slice()),
            ),
        )
        .await;
        assert!(
            matches!(result, Err(ProxyError::Protocol(_))),
            "malformed direct forwarding must fail before native dispatch: {result:?}"
        );
        assert!(fault.is_none(), "structural refusal cannot trigger replay");
        let mut response = Vec::new();
        tokio::time::timeout(TEST_TIMEOUT, client_peer.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            response,
            ProxyServer::create_fatal_response("08P01", "invalid simple Query payload")
        );
        drop(conns);
        assert!(
            backend.await.unwrap().is_none(),
            "no malformed Query was dispatched"
        );
    }
}

#[tokio::test]
async fn journal_source_uses_startup_tenant_budget_identity() {
    let mut config = test_config();
    config.multi_tenancy.enabled = true;
    config.multi_tenancy.identify_by = "database".into();
    config.multi_tenancy.tenants = vec!["benchdb".into()];
    config.multi_tenancy.max_journal_bytes_per_tenant = 4096;
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();
    assert!(session.variables.read().await.get("tenant_id").is_none());

    let startup = crate::protocol::StartupMessage::Startup {
        protocol_version: 196608,
        params: HashMap::from([
            ("user".into(), "bench".into()),
            ("database".into(), "benchdb".into()),
        ]),
    };
    ProxyServer::attach_tenant_budget(&server.state, &session, &startup);

    let source = ProxyServer::journal_source(&session).await;
    assert_eq!(source.tenant.as_deref(), Some("benchdb"));
    drop(source);
}

#[tokio::test]
async fn journal_source_is_resolved_once_and_follows_the_current_node() {
    let session = make_test_session();
    {
        let mut vars = session.variables.write().await;
        vars.insert("user".into(), "alice".into());
        vars.insert("database".into(), "shop".into());
    }
    *session.current_node.write().await = Some("a:5432".into());
    let first = ProxyServer::journal_source(&session).await.clone();
    assert_eq!(first.user, "alice");
    assert_eq!(first.database, "shop");
    assert_eq!(first.backend, "a:5432");
    assert_eq!(first.client_addr, session.client_addr.to_string());
    assert_eq!(first.tenant, None);

    // The backend follows the session's node on every call.
    *session.current_node.write().await = Some("b:5432".into());
    assert_eq!(
        ProxyServer::journal_source(&session).await.backend,
        "b:5432"
    );
    *session.current_node.write().await = None;
    assert_eq!(ProxyServer::journal_source(&session).await.backend, "");

    // A resolved identity does not wait for the session variables.
    let held = session.variables.write().await;
    let source = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ProxyServer::journal_source(&session),
    )
    .await
    .expect("resolved identity does not read the variables")
    .clone();
    drop(held);
    assert_eq!(source.user, "alice");

    // A change of the startup variables is picked up once it is announced.
    session
        .variables
        .write()
        .await
        .insert("user".into(), "bob".into());
    assert_eq!(ProxyServer::journal_source(&session).await.user, "alice");
    session.invalidate_journal_identity();
    let source = ProxyServer::journal_source(&session).await.clone();
    assert_eq!(source.user, "bob");
    assert_eq!(source.database, "shop");
}

mod branch_routing;
mod extended_hooks;
#[cfg(feature = "wasm-plugins")]
mod plugin_cached;
#[cfg(feature = "wasm-plugins")]
mod plugin_context;
#[cfg(feature = "wasm-plugins")]
mod plugin_reload;
#[cfg(feature = "pool-modes")]
mod pool_keys;
mod routing_targets;
mod session_bound;
mod shutdown_drain;
mod statement_pooling;
mod tenant_isolation;
