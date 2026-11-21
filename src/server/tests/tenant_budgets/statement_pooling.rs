//! Statement pooling (`[pool_mode] mode = "statement"`): the backend
//! connection returns to the pool after every statement, so a transaction
//! block may not stay open across statements. A lone `BEGIN` / `START
//! TRANSACTION` is refused on either protocol and the session goes on; a
//! transaction a single request opens and ends is served; a request that
//! leaves one open anyway ends its session, so its connection is closed
//! (the backend rolls the block back) instead of being lent to another client.
use super::*;

fn statement_pooling(c: &mut ProxyConfig) {
    c.pool_mode.mode = crate::config::PoolingMode::Statement;
}

#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn statement_pooling_refuses_a_transaction_block() {
    let h = Harness::new(0, None, statement_pooling).await;
    let mut a = h.connect("acme").await;
    for begin in [
        "BEGIN",
        "begin isolation level serializable;",
        "START TRANSACTION",
    ] {
        assert_refused(&a.query(begin).await, "0A000", "statement pooling");
    }
    for frames in [
        vec![cycle("BEGIN"), sync()],
        vec![parse("tx", "START TRANSACTION READ ONLY"), sync()],
    ] {
        a.send(&frames.concat()).await;
        assert_refused(&a.until_ready().await, "0A000", "statement pooling");
    }
    let opened = h.backend.log().iter().any(|s| {
        let upper = s.text.trim_start().to_ascii_uppercase();
        upper.starts_with("BEGIN") || upper.starts_with("START")
    });
    assert!(!opened, "nothing opened a block: {:?}", h.backend.log());

    // Autocommit statements are served, and a block one request opens and
    // ends is served whole.
    assert_served(&a.query("select 1").await, b'I');
    assert_served(&a.query("BEGIN; select 1; COMMIT").await, b'I');
    a.send(&[cycle("select 2"), sync()].concat()).await;
    assert_served(&a.until_ready().await, b'I');

    // A request that leaves a block open: its reply, then the session ends.
    let reply = a.query("select 1; BEGIN").await;
    assert_eq!(reply.last(), Some(&wire(b'Z', b"T")), "{reply:?}");
    let fatal = a.frame().await;
    assert_eq!(sqlstate(&fatal), "0A000", "{fatal:?}");
    assert!(has_text(&fatal, "FATAL"), "{fatal:?}");
    a.closed().await;
}

/// Transaction pooling keeps its transactions: the same `BEGIN` is served.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn transaction_pooling_still_serves_transaction_blocks() {
    let h = Harness::new(0, None, |c| {
        c.pool_mode.mode = crate::config::PoolingMode::Transaction;
    })
    .await;
    let mut a = h.connect("acme").await;
    assert_served(&a.query("BEGIN").await, b'T');
    assert_served(&a.query("select 1").await, b'T');
    assert_served(&a.query("COMMIT").await, b'I');
}
