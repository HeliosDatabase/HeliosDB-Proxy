//! The MCP agent memory tools through the gateway (Sprinter 20bcc218385f),
//! against a fake backend that runs the memory statements on an in-memory
//! table with a settable clock. It answers only the statements
//! [`MemorySql`] holds (anything else is an error), records each with its
//! bound parameters, and applies their `WHERE` clauses as written: the owner
//! columns `$1` and `$2`, the expiry, the key prefix and tags.

use super::*;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicI64;

use serde_json::{json, Value};

use crate::config::{McpConfig, McpPrincipal};
use crate::gateway_policy::GatewayPolicy;
use crate::mcp::{McpServer, McpShared};
use crate::mcp_memory::{MemoryOp, MemorySql, RefuseReason};

const A: &str = "agent-a";
const A_TOKEN: &str = "token-a";
const B: &str = "agent-b";
const B_TOKEN: &str = "token-b";

/// One statement the fake ran: its text and its bound parameters (`bound`
/// = extended protocol; a simple query has none).
#[derive(Debug, Clone)]
struct Seen {
    sql: String,
    params: Vec<Option<String>>,
    bound: bool,
}

#[derive(Debug, Clone)]
struct Row {
    value: String,
    tags: Vec<String>,
    created: i64,
    updated: i64,
    expires: i64,
}

/// `(owner_principal, owner_tenant, key)` → row.
type Rows = BTreeMap<(String, String, String), Row>;

#[derive(Default)]
struct Table {
    exists: bool,
    rows: Rows,
}

/// The fake's answer to one statement: columns, rows, command tag.
type Answer = (Vec<&'static str>, Vec<Vec<Option<String>>>, String);

/// A fake memory backend on a loopback port. Dropping it stops it.
struct FakeMemoryPg {
    port: u16,
    table: Arc<parking_lot::Mutex<Table>>,
    /// The fake `now()`, seconds.
    clock: Arc<AtomicI64>,
    seen: Arc<parking_lot::Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeMemoryPg {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeMemoryPg {
    /// A fake for the statements of `cfg`'s memory table; `exists`: whether
    /// the table is already there.
    async fn start(cfg: &McpConfig, exists: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let table = Arc::new(parking_lot::Mutex::new(Table {
            exists,
            rows: Rows::new(),
        }));
        let clock = Arc::new(AtomicI64::new(1_000));
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let sql = Arc::new(MemorySql::new(&cfg.memory_table));
        let (t, c, s) = (table.clone(), clock.clone(), seen.clone());
        let task = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                let state = State {
                    sql: sql.clone(),
                    table: t.clone(),
                    clock: c.clone(),
                    seen: s.clone(),
                };
                tokio::spawn(async move {
                    let _ = state.serve(sock).await;
                });
            }
        });
        Self {
            port,
            table,
            clock,
            seen,
            task,
        }
    }

    fn advance(&self, secs: i64) {
        self.clock.fetch_add(secs, Ordering::SeqCst);
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().clone()
    }

    fn rows(&self) -> Rows {
        self.table.lock().rows.clone()
    }
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn row_description(names: &[&str]) -> Vec<u8> {
    let mut b = (names.len() as u16).to_be_bytes().to_vec();
    for name in names {
        b.extend_from_slice(name.as_bytes());
        b.push(0);
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&25u32.to_be_bytes());
        b.extend_from_slice(&(-1i16).to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes());
    }
    frame(b'T', &b)
}

fn data_row(cells: &[Option<String>]) -> Vec<u8> {
    let mut b = (cells.len() as u16).to_be_bytes().to_vec();
    for c in cells {
        match c {
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(s) => {
                b.extend_from_slice(&(s.len() as u32).to_be_bytes());
                b.extend_from_slice(s.as_bytes());
            }
        }
    }
    frame(b'D', &b)
}

fn error_frame(message: &str) -> Vec<u8> {
    frame(b'E', format!("SERROR\0C42P01\0M{message}\0\0").as_bytes())
}

/// A NUL-terminated string at `*pos` of `body`.
fn cstring(body: &[u8], pos: &mut usize) -> String {
    let end = body[*pos..].iter().position(|&b| b == 0).unwrap() + *pos;
    let s = String::from_utf8(body[*pos..end].to_vec()).unwrap();
    *pos = end + 1;
    s
}

fn u16_at(body: &[u8], pos: &mut usize) -> usize {
    let v = u16::from_be_bytes([body[*pos], body[*pos + 1]]) as usize;
    *pos += 2;
    v
}

fn stamp(secs: i64) -> Option<String> {
    Some(format!("t{secs}"))
}

#[derive(Clone)]
struct State {
    sql: Arc<MemorySql>,
    table: Arc<parking_lot::Mutex<Table>>,
    clock: Arc<AtomicI64>,
    seen: Arc<parking_lot::Mutex<Vec<Seen>>>,
}

impl State {
    async fn serve(&self, mut sock: TcpStream) -> std::io::Result<()> {
        let len = sock.read_u32().await? as usize;
        let mut startup = vec![0u8; len.saturating_sub(4)];
        sock.read_exact(&mut startup).await?;
        sock.write_all(&[frame(b'R', &0u32.to_be_bytes()), frame(b'Z', b"I")].concat())
            .await?;
        let (mut sql, mut params) = (String::new(), Vec::new());
        let mut out = Vec::new();
        loop {
            let tag = match sock.read_u8().await {
                Ok(t) => t,
                Err(_) => return Ok(()),
            };
            let len = sock.read_u32().await? as usize;
            let mut body = vec![0u8; len.saturating_sub(4)];
            sock.read_exact(&mut body).await?;
            let mut pos = 0;
            match tag {
                b'X' => return Ok(()),
                b'Q' => {
                    let text = cstring(&body, &mut pos);
                    self.seen.lock().push(Seen {
                        sql: text.clone(),
                        params: Vec::new(),
                        bound: false,
                    });
                    let mut reply = self.answer_frames(self.simple(&text));
                    reply.extend(frame(b'Z', b"I"));
                    sock.write_all(&reply).await?;
                }
                b'P' => {
                    let _name = cstring(&body, &mut pos);
                    sql = cstring(&body, &mut pos);
                    assert_eq!(u16_at(&body, &mut pos), 0, "no declared parameter types");
                    out.extend(frame(b'1', b""));
                }
                b'B' => {
                    let _portal = cstring(&body, &mut pos);
                    let _stmt = cstring(&body, &mut pos);
                    assert_eq!(u16_at(&body, &mut pos), 0, "text parameters");
                    let n = u16_at(&body, &mut pos);
                    params = (0..n)
                        .map(|_| {
                            let len = i32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
                            pos += 4;
                            (len >= 0).then(|| {
                                let v = String::from_utf8(body[pos..pos + len as usize].to_vec())
                                    .unwrap();
                                pos += len as usize;
                                v
                            })
                        })
                        .collect();
                    out.extend(frame(b'2', b""));
                }
                b'E' => {
                    self.seen.lock().push(Seen {
                        sql: sql.clone(),
                        params: params.clone(),
                        bound: true,
                    });
                    let answer = self.bound(&sql, &params);
                    out.extend(self.answer_frames(answer));
                }
                b'S' => {
                    out.extend(frame(b'Z', b"I"));
                    sock.write_all(&out).await?;
                    out.clear();
                }
                _ => {}
            }
        }
    }

    fn answer_frames(&self, answer: std::result::Result<Answer, String>) -> Vec<u8> {
        match answer {
            Err(message) => error_frame(&message),
            Ok((columns, rows, tag)) => {
                let mut out = if columns.is_empty() {
                    frame(b'n', b"")
                } else {
                    row_description(&columns)
                };
                for row in &rows {
                    out.extend(data_row(row));
                }
                out.extend(frame(b'C', format!("{tag}\0").as_bytes()));
                out
            }
        }
    }

    fn simple(&self, text: &str) -> std::result::Result<Answer, String> {
        let mut table = self.table.lock();
        if text == self.sql.create {
            table.exists = true;
            return Ok((Vec::new(), Vec::new(), "CREATE TABLE".into()));
        }
        if text == self.sql.probe {
            if !table.exists {
                return Err("relation \"heliosdb_mcp_memory\" does not exist".into());
            }
            return Ok((vec!["owner_principal"], Vec::new(), "SELECT 0".into()));
        }
        Err(format!("unexpected simple query: {text}"))
    }

    fn bound(&self, text: &str, params: &[Option<String>]) -> std::result::Result<Answer, String> {
        let now = self.clock.load(Ordering::SeqCst);
        let mut table = self.table.lock();
        if !table.exists {
            return Err("relation \"heliosdb_mcp_memory\" does not exist".into());
        }
        let p = |i: usize| params.get(i).cloned().flatten().unwrap_or_default();
        let (principal, tenant) = (p(0), p(1));
        let mine = |k: &(String, String, String)| k.0 == principal && k.1 == tenant;
        let s = &self.sql;
        let rows = &mut table.rows;
        if text == s.sweep {
            let before = rows.len();
            rows.retain(|k, r| !(mine(k) && r.expires <= now));
            return Ok((
                Vec::new(),
                Vec::new(),
                format!("DELETE {}", before - rows.len()),
            ));
        }
        if text == s.count {
            let key = p(2);
            let others = rows
                .iter()
                .filter(|(k, r)| mine(k) && k.2 != key && r.expires > now)
                .count();
            return Ok((
                vec!["count"],
                vec![vec![Some(others.to_string())]],
                "SELECT 1".into(),
            ));
        }
        if text == s.put {
            let key = p(2);
            let ttl: i64 = p(5).parse().unwrap();
            let columns = vec!["created_at", "updated_at", "expires_at"];
            let tags: Vec<String> = serde_json::from_str(&p(4)).unwrap();
            let entry = rows
                .entry((principal.clone(), tenant.clone(), key))
                .or_insert(Row {
                    value: String::new(),
                    tags: Vec::new(),
                    created: now,
                    updated: now,
                    expires: now,
                });
            entry.value = p(3);
            entry.tags = tags;
            entry.updated = now;
            entry.expires = now + ttl;
            let row = vec![
                stamp(entry.created),
                stamp(entry.updated),
                stamp(entry.expires),
            ];
            return Ok((columns, vec![row], "INSERT 0 1".into()));
        }
        if text == s.get {
            let key = (principal.clone(), tenant.clone(), p(2));
            let found = rows.get(&key).filter(|r| r.expires > now).map(|r| {
                vec![
                    Some(r.value.clone()),
                    Some(serde_json::to_string(&r.tags).unwrap()),
                    stamp(r.created),
                    stamp(r.updated),
                    stamp(r.expires),
                ]
            });
            let columns = vec!["value", "tags", "created_at", "updated_at", "expires_at"];
            return Ok((columns, found.into_iter().collect(), "SELECT".into()));
        }
        if text == s.list || text == s.search {
            let values = text == s.search;
            let cursor = p(2);
            let prefix_chars: usize = p(3).parse().unwrap();
            let prefix = p(4);
            let want: Vec<String> = serde_json::from_str(&p(5)).unwrap();
            let limit: usize = p(6).parse().unwrap();
            let out: Vec<Vec<Option<String>>> = rows
                .iter()
                .filter(|(k, r)| {
                    mine(k)
                        && r.expires > now
                        && k.2 > cursor
                        && k.2.chars().take(prefix_chars).collect::<String>() == prefix
                        && want.iter().all(|t| r.tags.contains(t))
                })
                .take(limit)
                .map(|(k, r)| {
                    let mut row = vec![Some(k.2.clone())];
                    if values {
                        row.push(Some(r.value.clone()));
                    }
                    row.push(Some(serde_json::to_string(&r.tags).unwrap()));
                    row.extend([stamp(r.created), stamp(r.updated), stamp(r.expires)]);
                    row
                })
                .collect();
            let mut columns = vec!["key"];
            if values {
                columns.push("value");
            }
            columns.extend(["tags", "created_at", "updated_at", "expires_at"]);
            let tag = format!("SELECT {}", out.len());
            return Ok((columns, out, tag));
        }
        if text == s.delete {
            let removed = rows.remove(&(principal.clone(), tenant.clone(), p(2)));
            return Ok((
                Vec::new(),
                Vec::new(),
                format!("DELETE {}", u8::from(removed.is_some())),
            ));
        }
        Err(format!("unexpected statement: {text}"))
    }
}

fn principal(id: &str, token: &str) -> McpPrincipal {
    McpPrincipal {
        id: id.into(),
        token: token.into(),
    }
}

/// A read-write gateway with the memory tools on, principals A and B, on
/// the fake at `port`; `tune` adjusts it.
fn memory_cfg(port: u16, tune: impl FnOnce(&mut McpConfig)) -> McpConfig {
    let mut cfg = McpConfig {
        enabled: true,
        backend_port: port,
        read_only: false,
        memory_enabled: true,
        principals: vec![principal(A, A_TOKEN), principal(B, B_TOKEN)],
        ..McpConfig::default()
    };
    tune(&mut cfg);
    cfg
}

fn gateway(
    cfg: McpConfig,
    policy: GatewayPolicy,
    contract: Option<crate::agent_contract::AgentContract>,
) -> Arc<McpShared> {
    let pool: crate::gateway_pool::SharedBackendPool =
        Arc::new(crate::gateway_pool::BackendClientPool::new(4));
    McpServer::new(cfg, contract, pool)
        .with_policy(Arc::new(policy))
        .into_shared()
}

/// A fake with the table, and a default gateway on it.
async fn setup(tune: impl FnOnce(&mut McpConfig)) -> (FakeMemoryPg, Arc<McpShared>) {
    let probe = memory_cfg(0, |_| {});
    let fake = FakeMemoryPg::start(&probe, true).await;
    let shared = gateway(memory_cfg(fake.port, tune), GatewayPolicy::default(), None);
    (fake, shared)
}

/// One JSON-RPC request (`token` as the bearer, `headers` extra lines).
async fn rpc(shared: &Arc<McpShared>, token: Option<&str>, headers: &str, body: Value) -> Value {
    let body = body.to_string();
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST / HTTP/1.1\r\n{auth}{headers}Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = TcpStream::connect(addr).await.unwrap();
    let (server, peer) = listener.accept().await.unwrap();
    let task = tokio::spawn(McpServer::handle_connection(server, peer, shared.clone()));
    client.write_all(request.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    client.read_to_end(&mut buf).await.unwrap();
    task.await.unwrap().unwrap();
    let resp = String::from_utf8(buf).unwrap();
    let (_, json) = resp.split_once("\r\n\r\n").expect("an HTTP body");
    serde_json::from_str(json).unwrap_or_else(|e| panic!("{e}: {resp}"))
}

async fn call_as(
    shared: &Arc<McpShared>,
    token: Option<&str>,
    headers: &str,
    tool: &str,
    arguments: Value,
) -> Value {
    rpc(
        shared,
        token,
        headers,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": tool, "arguments": arguments }
        }),
    )
    .await
}

/// A tool call as principal A.
async fn call(shared: &Arc<McpShared>, tool: &str, arguments: Value) -> Value {
    call_as(shared, Some(A_TOKEN), "", tool, arguments).await
}

fn is_error(resp: &Value) -> bool {
    resp["result"]["isError"].as_bool().unwrap_or(true)
}

fn text(resp: &Value) -> &str {
    resp["result"]["content"][0]["text"].as_str().unwrap_or("")
}

/// The result object of a successful memory call; its text is the same JSON.
fn ok(resp: &Value) -> &Value {
    assert!(!is_error(resp), "{resp}");
    let structured = &resp["result"]["structuredContent"];
    let from_text: Value = serde_json::from_str(text(resp)).unwrap();
    assert_eq!(&from_text, structured, "text and structuredContent agree");
    structured
}

async fn tool_names(shared: &Arc<McpShared>) -> (Value, Vec<String>) {
    let resp = rpc(
        shared,
        Some(A_TOKEN),
        "",
        json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }),
    )
    .await;
    let tools = resp["result"]["tools"].clone();
    let names = tools
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    (tools, names)
}

/// With `memory_enabled = false` the catalog is exactly the three SQL tools
/// (the memory keys set or not), and a memory tool name is unknown; with it
/// on, the five memory tools follow the three, which are unchanged.
#[tokio::test]
async fn the_memory_tools_are_listed_only_when_enabled() {
    let fake = FakeMemoryPg::start(&memory_cfg(0, |_| {}), true).await;
    let plain = gateway(
        McpConfig {
            enabled: true,
            backend_port: fake.port,
            read_only: false,
            principals: vec![principal(A, A_TOKEN)],
            ..McpConfig::default()
        },
        GatewayPolicy::default(),
        None,
    );
    let tuned_off = gateway(
        memory_cfg(fake.port, |c| {
            c.memory_enabled = false;
            c.memory_table = "other_memory".into();
            c.memory_max_entries_per_owner = 7;
        }),
        GatewayPolicy::default(),
        None,
    );
    let (plain_tools, plain_names) = tool_names(&plain).await;
    assert_eq!(plain_names, ["query", "list_tables", "explain"]);
    assert_eq!(tool_names(&tuned_off).await.0, plain_tools);
    let resp = call(&tuned_off, "memory_get", json!({ "key": "k" })).await;
    assert!(is_error(&resp));
    assert_eq!(text(&resp), "unknown tool: memory_get");

    let on = gateway(
        memory_cfg(fake.port, |_| {}),
        GatewayPolicy::default(),
        None,
    );
    let (on_tools, on_names) = tool_names(&on).await;
    let memory: Vec<&str> = MemoryOp::ALL.iter().map(|op| op.tool()).collect();
    assert_eq!(on_names[..3], plain_names[..]);
    assert_eq!(on_names[3..], memory[..]);
    assert_eq!(
        on_tools.as_array().unwrap()[..3],
        plain_tools.as_array().unwrap()[..]
    );
    assert!(fake.seen().is_empty(), "listing ran nothing on the backend");
}

/// Every memory statement is one of the fixed texts, sent bound, with the
/// authenticated principal as `$1` and the tenant (`''` without tenancy) as
/// `$2`; owner-looking arguments change nothing, and no argument text is
/// ever part of a statement.
#[tokio::test]
async fn every_statement_is_bound_and_carries_the_proxy_set_owner() {
    let (fake, mcp) = setup(|_| {}).await;
    let spoof = json!({ "owner_principal": B, "principal": B, "owner_tenant": "globex",
                        "tenant": "globex" });
    let with = |extra: Value| {
        let mut args = spoof.clone();
        for (k, v) in extra.as_object().unwrap() {
            args[k] = v.clone();
        }
        args
    };
    let secret = "s3cr3t-value'); DROP TABLE x; --";
    ok(&call(
        &mcp,
        "memory_put",
        with(json!({ "key": "k'1", "value": secret, "tags": ["t"] })),
    )
    .await);
    assert_eq!(
        ok(&call(&mcp, "memory_get", with(json!({ "key": "k'1" }))).await)["value"],
        secret
    );
    ok(&call(&mcp, "memory_list", with(json!({ "prefix": "k" }))).await);
    ok(&call(&mcp, "memory_search", with(json!({ "tag": "t" }))).await);
    ok(&call(&mcp, "memory_delete", with(json!({ "key": "k'1" }))).await);

    let seen = fake.seen();
    let sql = MemorySql::new(&memory_cfg(0, |_| {}).memory_table);
    let fixed = sql.all();
    let bound: Vec<&Seen> = seen.iter().filter(|s| s.bound).collect();
    // sweep + count + put, get, list, search, delete.
    assert_eq!(bound.len(), 7, "{seen:#?}");
    for s in &seen {
        assert!(
            fixed.contains(&s.sql.as_str()),
            "not a memory statement: {}",
            s.sql
        );
        for arg in ["k'1", "s3cr3t", "globex", B] {
            assert!(!s.sql.contains(arg), "{arg} in {}", s.sql);
        }
    }
    for s in bound {
        assert_eq!(s.params[0].as_deref(), Some(A), "{s:?}");
        assert_eq!(s.params[1].as_deref(), Some(""), "{s:?}");
    }
    assert!(fake.rows().is_empty());
}

/// Principal B can neither read, list, search nor delete A's key, and its
/// own key of the same name is a separate entry.
#[tokio::test]
async fn one_principal_cannot_read_or_delete_another_principals_key() {
    let (fake, mcp) = setup(|_| {}).await;
    ok(&call(
        &mcp,
        "memory_put",
        json!({ "key": "plan", "value": { "step": 1 } }),
    )
    .await);
    let as_b = |tool: &'static str, args: Value| {
        let mcp = mcp.clone();
        async move { call_as(&mcp, Some(B_TOKEN), "", tool, args).await }
    };
    assert_eq!(
        ok(&as_b("memory_get", json!({ "key": "plan" })).await)["found"],
        false
    );
    assert_eq!(
        ok(&as_b("memory_list", json!({})).await)["entries"],
        json!([])
    );
    assert_eq!(
        ok(&as_b("memory_search", json!({ "prefix": "p" })).await)["entries"],
        json!([])
    );
    assert_eq!(
        ok(&as_b("memory_delete", json!({ "key": "plan" })).await)["deleted"],
        false
    );
    ok(&as_b("memory_put", json!({ "key": "plan", "value": "b's own" })).await);

    let a = ok(&call(&mcp, "memory_get", json!({ "key": "plan" })).await).clone();
    assert_eq!(
        (a["found"].clone(), a["value"].clone()),
        (json!(true), json!({ "step": 1 }))
    );
    let b = ok(&as_b("memory_get", json!({ "key": "plan" })).await).clone();
    assert_eq!(b["value"], "b's own");
    let owners: Vec<String> = fake.rows().keys().map(|k| k.0.clone()).collect();
    assert_eq!(owners, [A, B]);
}

/// The same principal under two tenants holds two separate memories: the
/// tenant comes from the request's tenant header, bound as `$2`.
#[cfg(feature = "multi-tenancy")]
#[tokio::test]
async fn tenants_are_isolated() {
    let fake = FakeMemoryPg::start(&memory_cfg(0, |_| {}), true).await;
    let mcp = gateway(
        memory_cfg(fake.port, |c| c.tenant_header = "X-Helios-Tenant".into()),
        crate::gateway_policy::capped_tenancy_policy("X-Helios-Tenant"),
        None,
    );
    let acme = "X-Helios-Tenant: acme\r\n";
    let globex = "X-Helios-Tenant: globex\r\n";
    ok(&call_as(
        &mcp,
        Some(A_TOKEN),
        acme,
        "memory_put",
        json!({ "key": "k", "value": 1 }),
    )
    .await);
    let other = call_as(
        &mcp,
        Some(A_TOKEN),
        globex,
        "memory_get",
        json!({ "key": "k" }),
    )
    .await;
    assert_eq!(ok(&other)["found"], false);
    let other = call_as(&mcp, Some(A_TOKEN), globex, "memory_list", json!({})).await;
    assert_eq!(ok(&other)["entries"], json!([]));
    let same = call_as(
        &mcp,
        Some(A_TOKEN),
        acme,
        "memory_get",
        json!({ "key": "k" }),
    )
    .await;
    assert_eq!(ok(&same)["value"], 1);
    for s in fake.seen().iter().filter(|s| s.bound) {
        assert!(
            matches!(s.params[1].as_deref(), Some("acme") | Some("globex")),
            "{s:?}"
        );
    }
    let keys: Vec<_> = fake.rows().into_keys().collect();
    assert_eq!(keys, [(A.to_string(), "acme".to_string(), "k".to_string())]);
    // A request without a known tenant is refused before any statement.
    let before = fake.seen().len();
    let refused = call_as(&mcp, Some(A_TOKEN), "", "memory_get", json!({ "key": "k" })).await;
    assert!(refused.to_string().contains("tenant"), "{refused}");
    assert_eq!(fake.seen().len(), before);
}

/// An expired entry is not returned by any read, and the owner's next put
/// deletes it; rewriting an expired key starts a new entry.
#[tokio::test]
async fn expired_entries_are_filtered_on_read_and_swept_by_the_next_put() {
    let (fake, mcp) = setup(|_| {}).await;
    let put = ok(&call(
        &mcp,
        "memory_put",
        json!({ "key": "short", "value": 1, "ttl_secs": 10 }),
    )
    .await)
    .clone();
    assert_eq!(put["expires_at"], "t1010");
    ok(&call(&mcp, "memory_put", json!({ "key": "long", "value": 2 })).await);
    fake.advance(11);
    assert_eq!(
        ok(&call(&mcp, "memory_get", json!({ "key": "short" })).await)["found"],
        false
    );
    let listed = ok(&call(&mcp, "memory_list", json!({})).await).clone();
    assert_eq!(listed["entries"].as_array().unwrap().len(), 1);
    assert_eq!(listed["entries"][0]["key"], "long");
    assert_eq!(fake.rows().len(), 2, "filtered, not yet deleted");
    ok(&call(&mcp, "memory_put", json!({ "key": "next", "value": 3 })).await);
    let keys: Vec<String> = fake.rows().keys().map(|k| k.2.clone()).collect();
    assert_eq!(keys, ["long", "next"], "the expired row was swept");
    let sql = MemorySql::new(&memory_cfg(0, |_| {}).memory_table);
    let sweeps: Vec<Seen> = fake
        .seen()
        .into_iter()
        .filter(|s| s.sql == sql.sweep)
        .collect();
    assert_eq!(sweeps.len(), 3, "one per put");
    assert!(sweeps.iter().all(|s| s.params.len() == 2), "{sweeps:?}");
    // An expired key rewritten before any sweep: the put's own sweep drops
    // the old row, so the entry starts again.
    ok(&call(
        &mcp,
        "memory_put",
        json!({ "key": "brief", "value": 5, "ttl_secs": 1 }),
    )
    .await);
    fake.advance(2);
    let again = ok(&call(&mcp, "memory_put", json!({ "key": "brief", "value": 6 })).await).clone();
    assert_eq!(again["created_at"], "t1013");
    assert_eq!(again["created_at"], again["updated_at"]);
}

/// A put of a new key past `memory_max_entries_per_owner` is refused and
/// counted; replacing a held key still works, and the quota is per owner.
#[tokio::test]
async fn a_new_key_past_the_quota_is_refused() {
    let (fake, mcp) = setup(|c| c.memory_max_entries_per_owner = 2).await;
    ok(&call(&mcp, "memory_put", json!({ "key": "k1", "value": 1 })).await);
    ok(&call(&mcp, "memory_put", json!({ "key": "k2", "value": 2 })).await);
    let hits = crate::mcp_memory::entries_quota_hits_total();
    let refused = crate::mcp_memory::refused_total(RefuseReason::Quota);
    let over = call(&mcp, "memory_put", json!({ "key": "k3", "value": 3 })).await;
    assert!(is_error(&over), "{over}");
    assert!(
        text(&over).contains("memory_max_entries_per_owner"),
        "{over}"
    );
    assert!(crate::mcp_memory::entries_quota_hits_total() > hits);
    assert!(crate::mcp_memory::refused_total(RefuseReason::Quota) > refused);
    ok(&call(&mcp, "memory_put", json!({ "key": "k1", "value": 10 })).await);
    ok(&call_as(
        &mcp,
        Some(B_TOKEN),
        "",
        "memory_put",
        json!({ "key": "k3", "value": 3 }),
    )
    .await);
    assert_eq!(fake.rows().len(), 3);
    // Once one is deleted, a new key fits again.
    ok(&call(&mcp, "memory_delete", json!({ "key": "k2" })).await);
    ok(&call(&mcp, "memory_put", json!({ "key": "k3", "value": 3 })).await);
}

/// On a read-only gateway, or under a read-only contract, `memory_put` and
/// `memory_delete` are refused before any statement; reads still work.
#[tokio::test]
async fn writes_are_refused_when_the_gateway_is_read_only() {
    let (fake, read_only) = setup(|c| c.read_only = true).await;
    let before = crate::mcp_memory::refused_total(RefuseReason::ReadOnly);
    for (tool, args) in [
        ("memory_put", json!({ "key": "k", "value": 1 })),
        ("memory_delete", json!({ "key": "k" })),
    ] {
        let resp = call(&read_only, tool, args).await;
        assert!(
            is_error(&resp) && text(&resp).contains("read-only"),
            "{resp}"
        );
    }
    assert!(fake.seen().is_empty(), "{:?}", fake.seen());
    assert!(crate::mcp_memory::refused_total(RefuseReason::ReadOnly) >= before + 2);
    assert_eq!(
        ok(&call(&read_only, "memory_get", json!({ "key": "k" })).await)["found"],
        false
    );

    let contract = crate::agent_contract::AgentContract {
        id: "reader".into(),
        read_only: true,
        allowed_verbs: None,
        allowed_tables: None,
        denied_tables: vec![],
        require_predicate_on: vec![],
        require_limit: false,
        max_rows: None,
    };
    let under_contract = gateway(
        memory_cfg(fake.port, |_| {}),
        GatewayPolicy::default(),
        Some(contract),
    );
    let resp = call(
        &under_contract,
        "memory_put",
        json!({ "key": "k", "value": 1 }),
    )
    .await;
    assert!(
        is_error(&resp) && text(&resp).contains("read-only"),
        "{resp}"
    );
    assert!(fake.rows().is_empty());
}

/// The shared principal `default` (no token configured, or the shared
/// `auth_token`) is refused before any statement.
#[tokio::test]
async fn the_shared_default_principal_is_refused() {
    let fake = FakeMemoryPg::start(&memory_cfg(0, |_| {}), true).await;
    let open = gateway(
        memory_cfg(fake.port, |c| c.principals.clear()),
        GatewayPolicy::default(),
        None,
    );
    let resp = call_as(&open, None, "", "memory_get", json!({ "key": "k" })).await;
    assert!(
        is_error(&resp) && text(&resp).contains("\"default\""),
        "{resp}"
    );
    let shared_token = gateway(
        memory_cfg(fake.port, |c| c.auth_token = Some("shared".into())),
        GatewayPolicy::default(),
        None,
    );
    let resp = call_as(&shared_token, Some("shared"), "", "memory_list", json!({})).await;
    assert!(
        is_error(&resp) && text(&resp).contains("\"default\""),
        "{resp}"
    );
    assert!(fake.seen().is_empty());
}

/// A refused argument reaches no statement and is counted by reason.
#[tokio::test]
async fn invalid_arguments_reach_no_statement() {
    let (fake, mcp) = setup(|c| c.memory_max_key_bytes = 4).await;
    let too_long = crate::mcp_memory::refused_total(RefuseReason::KeyTooLarge);
    let resp = call(&mcp, "memory_get", json!({ "key": "12345" })).await;
    assert!(
        is_error(&resp) && text(&resp).contains("more than the 4 allowed"),
        "{resp}"
    );
    assert!(crate::mcp_memory::refused_total(RefuseReason::KeyTooLarge) > too_long);
    let resp = call(&mcp, "memory_put", json!({ "key": "k" })).await;
    assert!(
        is_error(&resp) && text(&resp).contains("missing 'value'"),
        "{resp}"
    );
    assert!(fake.seen().is_empty());
}

/// Without the table and with `memory_auto_create = false`, the startup
/// check fails naming the DDL and a call is refused with it; with
/// `memory_auto_create = true` the first call creates the table, once.
#[tokio::test]
async fn a_missing_table_names_the_ddl_or_is_created_at_first_use() {
    let cfg = memory_cfg(0, |_| {});
    let fake = FakeMemoryPg::start(&cfg, false).await;
    let cfg = memory_cfg(fake.port, |_| {});
    let memory = crate::mcp_memory::McpMemory::new(&cfg);
    let pool: crate::gateway_pool::SharedBackendPool =
        Arc::new(crate::gateway_pool::BackendClientPool::new(0));
    let err = memory.startup_check(&cfg, &pool).await.unwrap_err();
    assert!(err.contains("does not exist"), "{err}");
    assert!(err.contains(memory.create_sql()), "{err}");
    let mcp = gateway(cfg, GatewayPolicy::default(), None);
    let before = crate::mcp_memory::refused_total(RefuseReason::NotReady);
    let resp = call(&mcp, "memory_get", json!({ "key": "k" })).await;
    assert!(is_error(&resp), "{resp}");
    assert!(
        text(&resp).contains("CREATE TABLE IF NOT EXISTS heliosdb_mcp_memory"),
        "{resp}"
    );
    assert!(crate::mcp_memory::refused_total(RefuseReason::NotReady) > before);

    let auto = gateway(
        memory_cfg(fake.port, |c| c.memory_auto_create = true),
        GatewayPolicy::default(),
        None,
    );
    ok(&call(&auto, "memory_put", json!({ "key": "k", "value": 1 })).await);
    ok(&call(&auto, "memory_get", json!({ "key": "k" })).await);
    let creates = fake
        .seen()
        .iter()
        .filter(|s| s.sql == memory.create_sql())
        .count();
    assert_eq!(creates, 1, "created once, at the first call");
    // The table now exists: the startup check passes.
    let cfg = memory_cfg(fake.port, |_| {});
    memory.startup_check(&cfg, &pool).await.unwrap();
    // A backend that cannot be reached is only warned about.
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    memory
        .startup_check(&memory_cfg(port, |_| {}), &pool)
        .await
        .unwrap();
}

/// Pages follow the key order with `cursor`; `prefix` is compared as text
/// (`_` and `%` are not wildcards); `memory_search` returns the values.
#[tokio::test]
async fn listing_pages_by_cursor_and_filters_by_prefix_and_tag() {
    let (_fake, mcp) = setup(|_| {}).await;
    for (key, tags) in [
        ("a_1", json!(["x"])),
        ("a_2", json!(["x", "y"])),
        ("ab", json!([])),
        ("b", json!(["y"])),
    ] {
        ok(&call(
            &mcp,
            "memory_put",
            json!({ "key": key, "value": { "k": key }, "tags": tags }),
        )
        .await);
    }
    let page = ok(&call(&mcp, "memory_list", json!({ "prefix": "a_", "limit": 1 })).await).clone();
    assert_eq!(page["entries"][0]["key"], "a_1");
    assert_eq!(page["entries"][0]["tags"], json!(["x"]));
    assert!(
        page["entries"][0].get("value").is_none(),
        "list carries no values"
    );
    assert_eq!(page["next_cursor"], "a_1");
    let next = ok(&call(
        &mcp,
        "memory_list",
        json!({ "prefix": "a_", "limit": 1,
                                                     "cursor": "a_1" }),
    )
    .await)
    .clone();
    assert_eq!(next["entries"][0]["key"], "a_2");
    assert_eq!(next["next_cursor"], Value::Null);
    let found = ok(&call(&mcp, "memory_search", json!({ "tag": "y" })).await).clone();
    let keys: Vec<&str> = found["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["a_2", "b"]);
    assert_eq!(found["entries"][1]["value"], json!({ "k": "b" }));
    let all = ok(&call(&mcp, "memory_list", json!({})).await).clone();
    assert_eq!(all["entries"].as_array().unwrap().len(), 4);
    let literal = ok(&call(&mcp, "memory_list", json!({ "prefix": "a%" })).await).clone();
    assert_eq!(literal["entries"], json!([]), "% is no wildcard");
}

/// `query` and `explain` refuse a statement that names the memory table
/// (before the backend), only while the memory tools are on.
#[tokio::test]
async fn the_sql_tools_refuse_statements_naming_the_memory_table() {
    let (fake, mcp) = setup(|_| {}).await;
    for (tool, sql) in [
        ("query", "SELECT * FROM heliosdb_mcp_memory"),
        ("query", "select value from public.\"HELIOSDB_MCP_MEMORY\""),
        ("explain", "SELECT * FROM HeliosDB_MCP_Memory"),
    ] {
        let resp = call(&mcp, tool, json!({ "sql": sql })).await;
        assert!(
            is_error(&resp) && text(&resp).contains("memory_* tools"),
            "{resp}"
        );
    }
    assert!(fake.seen().is_empty());
}

/// The writes move the shared C-02 generations of the memory table only (a
/// put's sweep and upsert, a delete); reads move nothing.
#[cfg(feature = "query-cache")]
#[tokio::test]
async fn memory_writes_move_the_write_generations_of_their_table() {
    let fake = FakeMemoryPg::start(&memory_cfg(0, |_| {}), true).await;
    let generations = Arc::new(crate::cache::TableGenerations::new());
    let policy = GatewayPolicy::default()
        .with_cache_writes(crate::cache::GatewayWrites::new(generations.clone(), None));
    let mcp = gateway(memory_cfg(fake.port, |_| {}), policy, None);
    let table = ["heliosdb_mcp_memory".to_string()];
    let at = |g: &crate::cache::TableGenerations| (g.generation_of(&table), g.global_generation());
    let start = at(&generations);
    ok(&call(&mcp, "memory_put", json!({ "key": "k", "value": 1 })).await);
    let after_put = at(&generations);
    assert_eq!(
        after_put,
        (start.0 + 2, start.1),
        "sweep + upsert, this table only"
    );
    ok(&call(&mcp, "memory_get", json!({ "key": "k" })).await);
    ok(&call(&mcp, "memory_list", json!({})).await);
    assert_eq!(at(&generations), after_put, "reads move nothing");
    ok(&call(&mcp, "memory_delete", json!({ "key": "k" })).await);
    assert_eq!(at(&generations), (after_put.0 + 1, start.1));
}

/// Completed calls are counted by tool.
#[tokio::test]
async fn completed_calls_are_counted_by_tool() {
    let (_fake, mcp) = setup(|_| {}).await;
    let before: Vec<u64> = MemoryOp::ALL
        .iter()
        .map(|op| crate::mcp_memory::ops_total(*op))
        .collect();
    ok(&call(&mcp, "memory_put", json!({ "key": "k", "value": 1 })).await);
    ok(&call(&mcp, "memory_get", json!({ "key": "k" })).await);
    ok(&call(&mcp, "memory_list", json!({})).await);
    ok(&call(&mcp, "memory_search", json!({})).await);
    ok(&call(&mcp, "memory_delete", json!({ "key": "k" })).await);
    for (op, was) in MemoryOp::ALL.iter().zip(before) {
        assert!(crate::mcp_memory::ops_total(*op) > was, "{op:?}");
    }
}
