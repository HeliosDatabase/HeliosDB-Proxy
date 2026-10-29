//! ac858d61400b: per-tenant active-query budgets through the whole client
//! handler (`handle_client`), against an in-process trust backend that logs
//! every frame it receives.
use super::*;
use crate::tenant_slots::TenantBudgetStats;
use std::sync::Mutex as StdMutex;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinHandle;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

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
    let upper = sql.trim().to_ascii_uppercase();
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
            kills: StdMutex::new(HashMap::new()),
            discard_delay,
            shared_key,
            stall_cancels: std::sync::atomic::AtomicBool::new(false),
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
                _ => break,
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
        loop {
            let mut header = [0u8; 5];
            tokio::select! {
                r = sock.read_exact(&mut header) => if r.is_err() { return },
                _ = kill.notified() => return,
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
                _ => String::new(),
            };
            ctl.note(pid, tag, &text);
            match (mode, tag) {
                (_, b'X') => return,
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
                    mode = BackendMode::Ready;
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
    assert_served(
        &a.query("DO $$BEGIN PERFORM pg_advisory_lock(1); END$$")
            .await,
        b'I',
    );
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
/// never ends the holder, which holds nothing. Its next request runs on a
/// replacement (re-homed by `tr_mode = session`, simply redialed under
/// `none`).
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
        assert_served(&a.query("select 'a'").await, b'I');
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
    let h = Harness::new(0, None, |c| c.cache.enabled = true).await;
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

/// A directory for `[plugins] plugin_dir` holding one plugin (compiled from
/// WAT) whose `hook` answers every simple Query with `result`, the JSON that
/// hook returns.
#[cfg(feature = "wasm-plugins")]
fn hook_plugin_dir(hook: &str, result: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("a temp dir");
    let payload: String = result.bytes().map(|b| format!("\\{b:02x}")).collect();
    let wat = format!(
        r#"(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) (i32.const 4096))
  (func (export "dealloc") (param i32) (param i32))
  (func (export "{hook}") (param i32) (param i32) (result i64)
    (i64.or (i64.shl (i64.const 1024) (i64.const 32)) (i64.const {len})))
  (data (i32.const 1024) "{payload}"))"#,
        len = result.len()
    );
    std::fs::write(
        dir.path().join("answer.wasm"),
        wat::parse_str(&wat).expect("wat parses"),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("answer.json"),
        format!(r#"{{"name": "answer", "version": "1.0.0", "hooks": ["{hook}"]}}"#),
    )
    .unwrap();
    dir
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
    let plugins = pre_query_plugin_dir(r#"{"action": "block", "value": "not here"}"#);
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
    let plugins = pre_query_plugin_dir(r#"{"action": "block", "value": "not here"}"#);
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
        // B, extended (plugins see simple Queries only), is served.
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
    let plugins = hook_plugin_dir("route", r#"{"action": "block", "reason": "not here"}"#);
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
    // B, extended (the route hook sees simple Queries only), is served.
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
    let plugins = hook_plugin_dir("route", r#"{"action": "block", "reason": "not here"}"#);
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

/// Open a transaction (an extended BEGIN: plugins see simple Queries only)
/// and have a plugin `Block` a Query in it (`text` names which). Nothing
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
    let plugins = hook_plugin_dir("route", r#"{"action": "block", "reason": "not here"}"#);
    a_plugin_block_in_a_transaction_reports_status_t(&plugins, "Query blocked by route plugin")
        .await;
}

/// [`a_plugin_block_in_a_transaction_reports_status_t`] for a pre-query
/// plugin.
#[cfg(feature = "wasm-plugins")]
#[tokio::test]
async fn a_pre_query_plugin_block_in_a_transaction_reports_status_t() {
    let plugins = pre_query_plugin_dir(r#"{"action": "block", "value": "not here"}"#);
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
        let plugins = pre_query_plugin_dir(&result);
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
        let plugins = hook_plugin_dir(hook, result);
        let h = Harness::new(0, None, |c| {
            c.multi_tenancy.idle_permit_timeout_ms = 300;
            plugins_from(c, &plugins);
        })
        .await;
        let mut a = h.connect("acme").await;
        // An extended COPY: the hooks see simple Queries only.
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
    ] {
        assert!(ProxyServer::calls_unlock_all(sql), "{sql}");
    }
    for sql in [
        "SELECT 'pg_advisory_unlock_all()'",
        "SELECT 1 -- pg_advisory_unlock_all()",
        "SELECT 1 /* pg_advisory_unlock_all() */",
        "SELECT \"pg_advisory_unlock_all\"()",
        "SELECT pg_advisory_unlock_all",
        "SELECT pg_advisory_unlock_all/**/()",
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
                // A lock holder keeps its permit while idle; its connection
                // dies, the permit returns (so the loss was seen), and the
                // statement below runs on a fresh dial.
                assert_served(&a.query("SELECT pg_advisory_lock(1)").await, b'I');
                h.until("acme", "A idle holding the permit", |s| s.queries_idle == 1)
                    .await;
                h.backend.kill(a.key.0);
                h.until("acme", "the permit returned", |s| s.queries_active == 0)
                    .await;
            }
            _ => {}
        }
        let slow = "select 'slow: never answered'";
        if shape == Shape::Extended {
            a.send(&[cycle(slow), sync()].concat()).await;
        } else {
            a.send(&query(slow)).await;
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
        let plugins = pre_query_plugin_dir(r#"{"action": "block", "value": "not here"}"#);
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
        // B (extended: the plugin sees simple Queries only) waits for A's
        // permit.
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
