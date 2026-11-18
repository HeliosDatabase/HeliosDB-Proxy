//! A failover never quietly leaves behind state only the failed backend had
//! (`session_bound`): a session that holds temporary objects, a cursor `WITH
//! HOLD`, statements prepared with SQL `PREPARE`, `LISTEN` registrations or
//! session-level advisory locks is told so (08006, naming them) and closed
//! instead of being re-homed without them. What a transaction replay recreates
//! and what the session cleared again do not block; under `tr_mode = none`
//! even settings close the session rather than being lost on a redial.
use super::*;

async fn after_backend_death(h: &Harness, a: &mut Client) -> Vec<u8> {
    h.backend.kill(a.key.0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    a.send(&wire(b'Q', b"select 'after'\0")).await;
    a.frame().await
}

#[tokio::test]
async fn a_failover_ends_a_session_holding_backend_only_state() {
    for (setup, words) in [
        ("CREATE TEMP TABLE scratch (a int)", "temporary tables"),
        ("DECLARE c CURSOR WITH HOLD FOR SELECT 1", "WITH HOLD"),
        ("PREPARE p AS SELECT 1", "SQL PREPARE"),
        ("LISTEN jobs", "LISTEN"),
        ("SELECT pg_advisory_lock(1)", "advisory locks"),
    ] {
        for extended in [false, true] {
            let shape = format!("{setup} (extended: {extended})");
            let h = Harness::new(0, None, |c| c.tr_mode = crate::config::TrMode::Session).await;
            let mut a = h.connect("acme").await;
            if extended {
                a.send(&[cycle(setup), sync()].concat()).await;
                assert_served(&a.until_ready().await, b'I');
            } else {
                assert_served(&a.query(setup).await, b'I');
            }
            let refusal = after_backend_death(&h, &mut a).await;
            assert_eq!(sqlstate(&refusal), "08006", "{shape}: {refusal:?}");
            assert!(has_text(&refusal, words), "{shape}: {refusal:?}");
            assert!(!h.backend.saw_query("select 'after'"), "{shape}");
            a.closed_after_ready().await;
        }
    }
}

/// Cleared state, tracked settings and a transaction the replay recreates do
/// not block a failover: the session is re-homed and its settings restored.
#[tokio::test]
async fn cleared_and_restorable_state_does_not_block_a_failover() {
    let h = Harness::new(0, None, |c| c.tr_mode = crate::config::TrMode::Session).await;
    let mut a = h.connect("acme").await;
    assert_served(&a.query("CREATE TEMP TABLE scratch (a int)").await, b'I');
    assert_served(&a.query("LISTEN jobs").await, b'I');
    assert_served(&a.query("DISCARD ALL").await, b'I');
    assert_served(&a.query("SET application_name = 'kept'").await, b'I');
    let old = a.key.0;
    h.backend.kill(old);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_served(&a.query("select 'after'").await, b'I');
    let replacement = h
        .backend
        .log()
        .iter()
        .find(|s| s.tag == b'Q' && s.text == "select 'after'")
        .map(|s| s.conn)
        .expect("re-run on a replacement");
    assert_ne!(replacement, old);
    assert!(
        h.backend
            .frames_of(replacement)
            .iter()
            .any(|s| s.tag == b'Q' && s.text == "SET application_name = 'kept'"),
        "settings restored: {:?}",
        h.backend.frames_of(replacement)
    );
}

/// Under `tr_mode = none` a session whose backend died idle is simply
/// redialed only when it held nothing there: one with settings (or any other
/// backend-only state) reports the loss and closes, as that mode promises.
#[tokio::test]
async fn tr_mode_none_closes_a_session_with_backend_state_instead_of_redialing() {
    let h = Harness::new(0, None, |c| c.tr_mode = crate::config::TrMode::None).await;
    let mut a = h.connect("acme").await;
    let mut plain = h.connect("acme").await;
    assert_served(&a.query("SET search_path = app").await, b'I');
    assert_served(&plain.query("select 1").await, b'I');
    h.backend.kill(plain.key.0);
    let refusal = after_backend_death(&h, &mut a).await;
    assert_eq!(sqlstate(&refusal), "57P01", "{refusal:?}");
    assert!(!h.backend.saw_query("select 'after'"));
    assert_served(&plain.query("select 'plain'").await, b'I');
}
