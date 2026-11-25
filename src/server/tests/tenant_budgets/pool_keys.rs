//! W02: the `[pool]` and `[pool_mode]` keys through the whole client handler,
//! in transaction pooling against the fake backend: the per-node connection
//! cap and the acquire deadline, the hand-over of a released connection to a
//! session waiting for its slot, the liveness probe on lease, the max
//! lifetime, the floors the idle reaper keeps (`min_idle`,
//! `min_connections`), and the prepared statement modes.
use super::*;

/// Transaction pooling, without the harness's tenants (their active-query
/// caps would serialize the sessions these tests run side by side).
fn pooled(c: &mut ProxyConfig) {
    c.multi_tenancy.enabled = false;
    c.pool_mode.mode = crate::config::PoolingMode::Transaction;
}

fn pool(h: &Harness) -> &crate::pool::BackendIdlePool {
    h.state
        .backend_pool
        .as_deref()
        .expect("transaction pooling has a pool")
}

/// The node took no fault: still healthy, no failure counted.
fn no_node_fault(h: &Harness, addr: &str) -> bool {
    h.state
        .health
        .load()
        .get(addr)
        .is_some_and(|n| n.healthy && n.failure_count == 0)
}

/// Backend connections opened so far (startup packets the backend got).
fn dials(h: &Harness) -> usize {
    h.backend.startup_params().len()
}

/// Wait until `cond` holds.
async fn eventually(what: &str, cond: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Wait until exactly `n` connections are parked.
async fn until_idle(h: &Harness, n: usize) {
    eventually(&format!("{n} parked connections"), || {
        pool(h).idle_count() == n
    })
    .await;
}

/// The backend closes its connection `pid` (parked), and the proxy's end
/// has seen it.
async fn kill_parked(h: &Harness, pid: u32) {
    h.backend.kill(pid);
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// The node's `/pools` counts.
fn node_stats(h: &Harness) -> crate::node_slots::NodeConnStats {
    let addr = h.backend.addr.to_string();
    h.state
        .node_slots
        .conn_stats()
        .into_iter()
        .find(|s| s.node == addr)
        .expect("the configured node")
}

/// `[pool] max_connections` caps the backend connections to a node (in use
/// and parked). A request that cannot get one within `[pool_mode]
/// acquire_timeout_secs` is refused with 53300 long before `[limits]
/// backend_slot_wait_ms`, the refusal is counted, nothing reaches the
/// backend, the node takes no fault, and the session goes on.
#[tokio::test]
async fn the_node_cap_and_the_acquire_deadline_refuse_with_53300() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool.max_connections = 1;
        c.pool_mode.acquire_timeout_secs = 1;
        c.limits.backend_slot_wait_ms = 30_000;
    })
    .await;
    let addr = h.backend.addr.to_string();
    let mut a = h.connect("alice").await;
    assert_served(&a.query("select 1").await, b'I');
    until_idle(&h, 1).await;
    // Bob's startup takes the only slot from alice's parked connection.
    let mut b = h.connect("bob").await;
    assert_eq!(pool(&h).capacity_evictions(), 1);
    assert_served(&b.query("BEGIN").await, b'T');

    let started = std::time::Instant::now();
    let refused = a.query("select 2").await;
    let waited = started.elapsed();
    assert_refused(&refused, "53300", "acquire_timeout_secs");
    assert!(
        waited >= Duration::from_millis(900) && waited < Duration::from_secs(5),
        "{waited:?}"
    );
    assert_eq!(pool(&h).acquire_timeouts(), 1);
    assert!(!h.backend.saw_query("select 2"));
    assert!(no_node_fault(&h, &addr));
    let node = node_stats(&h);
    assert_eq!(
        (node.limit, node.open, node.waiting, node.refusals),
        (Some(1), 1, 0, 0),
        "the abandoned wait is no waiter and no node-cap refusal"
    );

    // Once bob's transaction ends alice gets the slot.
    assert_served(&b.query("COMMIT").await, b'I');
    assert_served(&a.query("select 3").await, b'I');
    assert_eq!(node_stats(&h).open, 1, "never over the cap");
}

/// Without `[pool_mode] acquire_timeout_secs` (0, the default) the same
/// request waits for the node's slot up to `[limits] backend_slot_wait_ms`
/// and is then refused by the node cap, as before W02.
#[tokio::test]
async fn without_the_acquire_deadline_the_slot_wait_bounds_the_request() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool.max_connections = 1;
        c.limits.backend_slot_wait_ms = 300;
    })
    .await;
    let mut a = h.connect("alice").await;
    assert_served(&a.query("select 1").await, b'I');
    until_idle(&h, 1).await;
    let mut b = h.connect("bob").await;
    assert_served(&b.query("BEGIN").await, b'T');
    assert_refused(&a.query("select 2").await, "53300", "max_connections (1)");
    assert_eq!(pool(&h).acquire_timeouts(), 0);
    assert_eq!(node_stats(&h).refusals, 1);
}

/// A connection released while another session waits for its node's slot
/// is closed instead of parked: the waiter gets the slot at once instead
/// of waiting out `[limits] backend_slot_wait_ms` behind an idle
/// connection it cannot use.
#[tokio::test]
async fn a_released_connection_goes_to_a_session_waiting_for_its_slot() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool.max_connections = 1;
        c.limits.backend_slot_wait_ms = 30_000;
    })
    .await;
    let mut a = h.connect("alice").await;
    assert_served(&a.query("BEGIN").await, b'T');
    let started = std::time::Instant::now();
    let (b, committed) = tokio::join!(h.connect("bob"), async {
        // Bob's startup waits for the slot alice's transaction holds.
        eventually("bob waiting for the node", || node_stats(&h).waiting == 1).await;
        a.query("COMMIT").await
    });
    assert_served(&committed, b'I');
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(pool(&h).handed_over(), 1);
    assert_eq!(pool(&h).idle_count(), 0, "closed, not parked");
    assert_eq!(node_stats(&h).open, 1);
    drop(b);
}

/// `[pool] test_on_acquire`: by default a parked connection the backend
/// reset is found by the liveness probe at checkout and replaced. With
/// `false` it is leased without the probe; its first write fails, and the
/// request is sent once more on a fresh connection, with no node fault.
#[tokio::test]
async fn test_on_acquire_probes_or_redials_a_connection_reset_while_parked() {
    for probe in [true, false] {
        let h = Harness::new(0, None, |c| {
            pooled(c);
            c.pool.test_on_acquire = probe;
        })
        .await;
        h.backend.ctl.reset_on_kill.store(true, Ordering::Relaxed);
        let addr = h.backend.addr.to_string();
        let mut a = h.connect("alice").await;
        assert_served(&a.query("select 1").await, b'I');
        until_idle(&h, 1).await;
        // The backend resets the parked connection (backend pid 1).
        kill_parked(&h, 1).await;
        assert_served(&a.query("select 2").await, b'I');
        assert!(
            h.backend
                .frames_of(2)
                .iter()
                .any(|s| s.tag == b'Q' && s.text == "select 2"),
            "probe = {probe}: {:?}",
            h.backend.log()
        );
        assert_eq!(
            pool(&h).stale_evicted(),
            u64::from(probe),
            "probe = {probe}"
        );
        assert_eq!(
            pool(&h).stale_lease_redials(),
            u64::from(!probe),
            "probe = {probe}"
        );
        assert!(no_node_fault(&h, &addr), "probe = {probe}");
        assert_eq!(dials(&h), 2, "probe = {probe}");
    }
}

/// `max_lifetime_secs` (the smaller non-zero of `[pool]` and `[pool_mode]`):
/// a connection in use is never closed under its transaction; past its
/// lifetime it is closed when released instead of parked, closed at
/// checkout, and closed by the reaper.
#[tokio::test]
async fn connections_past_their_max_lifetime_are_replaced_between_transactions() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool.max_lifetime_secs = 1;
        c.pool_mode.max_lifetime_secs = 3600;
    })
    .await;
    let life = Duration::from_millis(1100);
    let mut a = h.connect("alice").await;
    assert_served(&a.query("BEGIN").await, b'T');
    tokio::time::sleep(life).await;
    assert_served(&a.query("select 1").await, b'T');
    assert_served(&a.query("COMMIT").await, b'I');
    assert_eq!(dials(&h), 1, "never closed under its transaction");
    // Released past its lifetime: closed, not parked.
    eventually("the release to close it", || {
        pool(&h).lifetime_expired() == 1
    })
    .await;
    assert_eq!(pool(&h).idle_count(), 0);
    assert!(h
        .backend
        .frames_of(1)
        .iter()
        .all(|s| s.text != "DISCARD ALL"));

    // Parked, then too old when leased: closed at checkout.
    assert_served(&a.query("select 2").await, b'I');
    assert_eq!(dials(&h), 2);
    until_idle(&h, 1).await;
    tokio::time::sleep(life).await;
    assert_served(&a.query("select 3").await, b'I');
    assert_eq!(dials(&h), 3);
    assert_eq!(pool(&h).lifetime_expired(), 2);

    // Parked, then too old: the reaper closes it.
    until_idle(&h, 1).await;
    tokio::time::sleep(life).await;
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(pool(&h).lifetime_expired(), 3);
    assert_eq!(pool(&h).idle_count(), 0);
    assert_eq!(pool(&h).reaped(), 0, "a lifetime close is not an idle reap");
}

/// `[pool_mode] min_idle`: the reaper keeps that many connections parked
/// for each identity a session used within the idle TTL, opening the
/// missing ones ahead of use with the session's startup parameters (a
/// trust backend needs no credential), and the idle TTL spares them.
#[tokio::test]
async fn min_idle_keeps_connections_parked_for_each_identity() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool_mode.min_idle = 2;
        c.pool.idle_timeout_secs = 1;
    })
    .await;
    let mut a = h.connect("alice").await;
    assert_served(&a.query("select 1").await, b'I');
    until_idle(&h, 1).await;
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(pool(&h).idle_count(), 2);
    assert_eq!(pool(&h).topped_up(), 1);
    let params = h.backend.startup_params();
    assert_eq!(params.len(), 2);
    assert_eq!(params[1], params[0], "opened as the session's identity");
    // Again: the floor is met, nothing more is opened.
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!((pool(&h).idle_count(), dials(&h)), (2, 2));

    // Past the idle TTL the floor stays parked.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(pool(&h).idle_count(), 2);
    assert_eq!(pool(&h).reaped(), 0);
    // The session's next transactions lease them without a dial.
    assert_served(&a.query("select 2").await, b'I');
    assert_served(&a.query("select 3").await, b'I');
    assert_eq!(dials(&h), 2);
}

/// `[pool] min_connections`: the reaper keeps that many connections open
/// to each enabled node, opening them ahead of use with the identity the
/// node served most recently, and the idle TTL closes connections only
/// down to that floor.
#[tokio::test]
async fn min_connections_keeps_each_node_at_its_floor() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool.min_connections = 2;
        c.pool.idle_timeout_secs = 1;
    })
    .await;
    let addr = h.backend.addr.to_string();
    let mut a = h.connect("alice").await;
    assert_served(&a.query("select 1").await, b'I');
    until_idle(&h, 1).await;
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(h.state.node_slots.open(&addr), 2);
    assert_eq!(pool(&h).topped_up(), 1);
    assert_eq!(
        h.backend.startup_params()[1]
            .get("user")
            .map(String::as_str),
        Some("alice")
    );

    // Two more sessions: four open, all parked.
    for user in ["bob", "carol"] {
        let mut c = h.connect(user).await;
        assert_served(&c.query("select 1").await, b'I');
    }
    until_idle(&h, 4).await;
    assert_eq!(h.state.node_slots.open(&addr), 4);
    // Past the idle TTL the reaper closes down to the floor, not below.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(h.state.node_slots.open(&addr), 2);
    assert_eq!(pool(&h).idle_count(), 2);
    assert_eq!(pool(&h).reaped(), 2);
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(h.state.node_slots.open(&addr), 2, "the floor stays");
}

/// An identity the backend asks a password for, which the proxy does not
/// hold (pass-through auth), is never opened ahead of use: one attempt,
/// counted as a failure, then no more; the warning is logged once and the
/// node takes no fault.
#[tokio::test]
async fn an_identity_the_proxy_holds_no_credential_for_is_not_opened_ahead() {
    // A backend that asks every connection for a cleartext password.
    let locked = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let locked_addr = locked.local_addr().unwrap().to_string();
    let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&asked);
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = locked.accept().await {
            counter.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                let len = sock.read_u32().await.unwrap_or(8) as usize;
                let mut body = vec![0u8; len.saturating_sub(4)];
                let _ = sock.read_exact(&mut body).await;
                let _ = sock.write_all(&wire(b'R', &3u32.to_be_bytes())).await;
                let mut rest = Vec::new();
                let _ = sock.read_to_end(&mut rest).await;
            });
        }
    });
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool_mode.min_idle = 1;
        c.add_node(&locked_addr, "standby").unwrap();
    })
    .await;
    let key = crate::pool::pool_key(&locked_addr, "alice", "alice");
    let startup = ProxyServer::build_startup_message(&HashMap::from([(
        "user".to_string(),
        "alice".to_string(),
    )]));
    pool(&h).note_use(&key, std::time::Instant::now(), || crate::pool::DialSpec {
        node: locked_addr.clone(),
        startup: startup.into(),
        user: "alice".into(),
        tenant_pool: None,
        max_idle: 4,
        last_used: std::time::Instant::now(),
        no_credential: false,
    });
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(asked.load(Ordering::Relaxed), 1);
    assert_eq!((pool(&h).topped_up(), pool(&h).topup_failures()), (0, 1));
    assert!(pool(&h).specs().is_empty(), "not offered again");
    assert!(
        !pool(&h).first_no_credential_warning(),
        "the warning was logged once already"
    );
    assert!(no_node_fault(&h, &locked_addr));
    ProxyServer::pool_maintenance(&h.state).await;
    assert_eq!(asked.load(Ordering::Relaxed), 1, "never dialed again");
}

/// `prepared_statement_mode = "disable"` (transaction pooling): a named
/// `Parse` is refused with 0A000 and the rest of its cycle is discarded up
/// to the Sync; a SQL `PREPARE`, alone or among other statements, is
/// refused the same way; unnamed statements are served. Nothing refused
/// reaches the backend.
#[tokio::test]
async fn disable_refuses_named_and_sql_prepared_statements() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.pool_mode.prepared_statement_mode = crate::config::PreparedStatementMode::Disable;
    })
    .await;
    let mut a = h.connect("alice").await;
    a.send(&[parse("s1", "select 41"), bind("s1"), execute(), sync()].concat())
        .await;
    assert_refused(&a.until_ready().await, "0A000", "prepared_statement_mode");
    for sql in ["PREPARE p AS SELECT 1", "select 1; prepare q as select 2"] {
        assert_refused(&a.query(sql).await, "0A000", "prepared_statement_mode");
    }
    a.send(
        &[
            parse("", "PREPARE r AS SELECT 3"),
            bind(""),
            execute(),
            sync(),
        ]
        .concat(),
    )
    .await;
    assert_refused(&a.until_ready().await, "0A000", "prepared_statement_mode");
    assert!(
        h.backend
            .log()
            .iter()
            .all(|s| !s.text.to_ascii_lowercase().contains("prepare") && s.text != "select 41"),
        "{:?}",
        h.backend.log()
    );
    a.send(&[cycle("select 2"), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');
    assert_served(&a.query("select 3").await, b'I');
}

/// `prepared_statement_mode = "track"` (the default in transaction
/// pooling): a statement prepared with SQL `PREPARE` is prepared again on
/// whatever connection a later `EXECUTE` (simple or extended) runs on,
/// right before it; `DEALLOCATE` forgets it. The registry is bounded by
/// `[limits] max_prepared_statements` (54000 past it).
#[tokio::test]
async fn track_prepares_sql_statements_again_where_they_are_executed() {
    let h = Harness::new(0, None, |c| {
        pooled(c);
        c.limits.max_prepared_statements = 1;
    })
    .await;
    let mut a = h.connect("alice").await;
    assert_served(&a.query("PREPARE p AS SELECT 1").await, b'I');
    assert_refused(
        &a.query("PREPARE q AS SELECT 2").await,
        "54000",
        "max_prepared_statements",
    );
    let mut pid = 1;
    for (request, tag) in [
        (query("EXECUTE p"), b'Q'),
        (
            [parse("", "EXECUTE p"), bind(""), execute(), sync()].concat(),
            b'P',
        ),
    ] {
        // The connection holding `p` is gone: the next one lacks it.
        until_idle(&h, 1).await;
        kill_parked(&h, pid).await;
        pid += 1;
        a.send(&request).await;
        assert_served(&a.until_ready().await, b'I');
        let seen: Vec<(u8, String)> = h
            .backend
            .frames_of(pid)
            .into_iter()
            .filter(|s| matches!(s.tag, b'Q' | b'P'))
            .map(|s| (s.tag, s.text))
            .collect();
        assert_eq!(
            seen[..2],
            [
                (b'Q', "PREPARE p AS SELECT 1".to_string()),
                (tag, "EXECUTE p".to_string())
            ],
            "{seen:?}"
        );
    }

    // DEALLOCATE forgets it: nothing is prepared again afterwards.
    assert_served(&a.query("DEALLOCATE p").await, b'I');
    until_idle(&h, 1).await;
    kill_parked(&h, pid).await;
    pid += 1;
    assert_served(&a.query("EXECUTE p").await, b'I');
    assert!(
        h.backend
            .frames_of(pid)
            .iter()
            .all(|s| !s.text.starts_with("PREPARE")),
        "{:?}",
        h.backend.frames_of(pid)
    );
    // With `p` gone there is room for `q`.
    assert_served(&a.query("PREPARE q AS SELECT 2").await, b'I');
}

/// Session pooling keeps prepared statements on the session's own
/// connection: neither a named `Parse` nor a SQL `PREPARE` is refused or
/// rewritten, whatever `prepared_statement_mode` the transaction modes
/// would use.
#[tokio::test]
async fn session_pooling_leaves_prepared_statements_alone() {
    let h = Harness::new(0, None, |c| {
        c.multi_tenancy.enabled = false;
        c.pool_mode.prepared_statement_mode = crate::config::PreparedStatementMode::Disable;
    })
    .await;
    let mut a = h.connect("alice").await;
    a.send(&[parse("s1", "select 1"), bind("s1"), execute(), sync()].concat())
        .await;
    assert_served(&a.until_ready().await, b'I');
    assert_served(&a.query("PREPARE p AS SELECT 1").await, b'I');
    assert!(h.backend.saw_query("PREPARE p AS SELECT 1"));
}
