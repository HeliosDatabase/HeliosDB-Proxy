//! The shutdown drain (SIGTERM / SIGINT, `ProxyServer::begin_drain`): a
//! session ends with `57P01` as soon as nothing of it is in flight, never in
//! the middle of a transaction, a query, an extended cycle or a partly
//! received request.
use super::*;

/// The next frame is the drain's FATAL `57P01`; the connection then closes
/// and the session's task ends cleanly, deregistered.
async fn assert_drained(mut c: Client, shape: &str) {
    let fatal = c.frame().await;
    assert_eq!(sqlstate(&fatal), "57P01", "{shape}");
    assert!(has_text(&fatal, "FATAL"), "{shape}: {fatal:?}");
    assert!(has_text(&fatal, "shutting down"), "{shape}: {fatal:?}");
    let Client { mut s, task, .. } = c;
    let mut rest = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, s.read_to_end(&mut rest))
        .await
        .expect("the proxy closes the connection")
        .unwrap();
    assert!(rest.is_empty(), "{shape}: nothing follows the FATAL");
    tokio::time::timeout(TEST_TIMEOUT, task)
        .await
        .expect("the session ends")
        .unwrap()
        .unwrap();
}

fn drained(h: &Harness) -> u64 {
    h.state
        .metrics
        .policy
        .sessions_drained
        .load(Ordering::Relaxed)
}

/// An idle session is ended at once, whether it still watches its backend
/// connection (session pooling) or parked it (transaction pooling); one that
/// connects after the drain began is answered the same FATAL in place of its
/// startup, without being authenticated or reaching a backend.
#[tokio::test]
async fn a_drain_ends_idle_sessions_at_once() {
    for pooled in poolings() {
        let shape = format!("transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| transaction_pooling(c, pooled)).await;
        let mut used = h.connect("acme").await;
        assert_served(&used.query("select 1").await, b'I');
        let fresh = h.connect("other").await;
        assert_eq!(h.state.sessions.len(), 2, "{shape}");

        ProxyServer::begin_drain(&h.state);
        assert_drained(used, &shape).await;
        assert_drained(fresh, &shape).await;
        assert_eq!(drained(&h), 2, "{shape}");
        assert!(h.state.sessions.is_empty(), "{shape}");

        let startups = h.backend.startup_databases().len();
        let late = h.connect_raw(startup_bytes("acme")).await;
        assert_drained(late, &shape).await;
        assert_eq!(drained(&h), 3, "{shape}");
        assert_eq!(
            h.backend.startup_databases().len(),
            startups,
            "{shape}: the late session dialed no backend"
        );
    }
}

/// A session inside a transaction keeps running it: its statements are
/// served, and the session ends once the transaction is over.
#[tokio::test]
async fn a_drain_waits_for_an_open_transaction() {
    for pooled in poolings() {
        let shape = format!("transaction pooling {pooled}");
        let h = Harness::new(0, None, |c| transaction_pooling(c, pooled)).await;
        let mut a = h.connect("acme").await;
        assert_served(&a.query("BEGIN").await, b'T');

        ProxyServer::begin_drain(&h.state);
        a.quiet().await;
        assert_served(&a.query("insert into t values (1)").await, b'T');
        a.quiet().await;
        assert_eq!(drained(&h), 0, "{shape}");
        assert_served(&a.query("COMMIT").await, b'I');
        assert_drained(a, &shape).await;
        assert!(h.backend.saw_query("COMMIT"), "{shape}");
        assert_eq!(drained(&h), 1, "{shape}");
    }
}

/// A query in flight is answered in full before its session ends.
#[tokio::test]
async fn a_drain_lets_a_running_query_finish() {
    let h = Harness::new(0, None, |_| {}).await;
    let mut a = h.connect("acme").await;
    h.hold_permit(&mut a, "select 'slow a'").await;

    ProxyServer::begin_drain(&h.state);
    a.quiet().await;
    h.backend.release_slow();
    assert_served(&a.until_ready().await, b'I');
    assert_drained(a, "running query").await;
}

/// An extended cycle a Flush dispatched part of, and one only partly
/// received, are both served to their Sync.
#[tokio::test]
async fn a_drain_waits_for_an_open_extended_cycle() {
    let h = Harness::new(0, None, |_| {}).await;

    let mut flushed = h.connect("acme").await;
    flushed
        .send(&[cycle("insert into t values (1)"), flush()].concat())
        .await;
    assert_eq!(tags(&flushed.frames(3).await), b"12C");
    let mut partial = h.connect("other").await;
    partial.send(&parse("", "select 2")).await;
    // The proxy has read the Parse once the session is no longer idle.
    h.until("other", "the cycle's permit", |s| s.queries_active == 1)
        .await;

    ProxyServer::begin_drain(&h.state);
    flushed.quiet().await;
    partial.quiet().await;
    assert_eq!(drained(&h), 0);

    flushed.send(&sync()).await;
    assert_eq!(flushed.frame().await, wire(b'Z', b"I"));
    assert_drained(flushed, "flushed cycle").await;

    partial.send(&[bind(""), execute(), sync()].concat()).await;
    assert_served(&partial.until_ready().await, b'I');
    assert_drained(partial, "partly received cycle").await;
    assert_eq!(drained(&h), 2);
}

/// Wait for the drain as the task serving `session` does.
async fn wait_for_drain(state: Arc<ServerState>, session: Arc<ClientSession>) {
    DrainSignal {
        state: &state,
        session: &session,
    }
    .await;
}

/// The signal itself: pending until the drain begins, woken by it, and
/// ready at once afterwards.
#[tokio::test]
async fn the_drain_signal_wakes_a_waiting_session() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    state.sessions.insert(session.id, session.clone());

    let waiter = tokio::spawn(wait_for_drain(state.clone(), session.clone()));
    // Registered: the session's waker is in its slot.
    let deadline = tokio::time::Instant::now() + TEST_TIMEOUT;
    while session.drain_waker.lock().is_none() {
        assert!(tokio::time::Instant::now() < deadline, "never registered");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!waiter.is_finished());

    ProxyServer::begin_drain(&state);
    tokio::time::timeout(TEST_TIMEOUT, waiter)
        .await
        .expect("the drain wakes the waiter")
        .unwrap();
    // Once begun it never blocks, and beginning it again changes nothing.
    ProxyServer::begin_drain(&state);
    tokio::time::timeout(
        TEST_TIMEOUT,
        DrainSignal {
            state: &state,
            session: &session,
        },
    )
    .await
    .expect("ready at once");
}
