//! Existing relay APIs: a proven explicit commit must settle before Sync.
mod paths;

use super::*;
use crate::journal_capture::{Completion, ResponseOutcome};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut wire = vec![tag];
    wire.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    wire.extend_from_slice(payload);
    wire
}

fn done(tag: &str) -> Vec<u8> {
    frame(b'C', &[tag.as_bytes(), b"\0"].concat())
}

fn execute(name: &str, sql: &str) -> Vec<u8> {
    execute_limit(name, sql, 0)
}

fn execute_limit(name: &str, sql: &str, max_rows: u32) -> Vec<u8> {
    let mut parse = name.as_bytes().to_vec();
    parse.push(0);
    parse.extend_from_slice(sql.as_bytes());
    parse.extend_from_slice(&[0, 0, 0]);
    let mut bind = name.as_bytes().to_vec();
    bind.push(0);
    bind.extend_from_slice(name.as_bytes());
    bind.extend_from_slice(&[0; 7]);
    let mut exec = name.as_bytes().to_vec();
    exec.push(0);
    exec.extend_from_slice(&max_rows.to_be_bytes());
    [frame(b'P', &parse), frame(b'B', &bind), frame(b'E', &exec)].concat()
}

fn register(session: &ClientSession, statements: &[(&str, &str)]) {
    let wire = statements
        .iter()
        .flat_map(|(name, sql)| execute(name, sql))
        .collect::<Vec<_>>();
    ProxyServer::journal_register_batch(session, &wire, None, true, true);
}

fn server() -> ProxyServer {
    let mut config = test_config();
    config.tr_enabled = true;
    #[cfg(feature = "query-cache")]
    {
        config.cache.enabled = true;
        config.cache.extended_protocol = crate::config::ExtendedProtocolCache::On;
        config.cache.trust_backend_read_effects = true;
    }
    ProxyServer::new(config).unwrap()
}

async fn settled(server: &ProxyServer, session: &ClientSession, sql: &str, tag: &str, status: u8) {
    ProxyServer::journal_register_simple_cycle(session, sql, true);
    if let Some(cycle) = ProxyServer::journal_observe_sync(
        session,
        &server.state,
        ResponseOutcome {
            status,
            completions: vec![Completion::Tag(tag.into())],
            error: None,
        },
    ) {
        ProxyServer::journal_observe_finish(session, &server.state, cycle).await;
    }
}

async fn open_write(server: &ProxyServer, session: &ClientSession, opaque: bool) {
    settled(server, session, "BEGIN", "BEGIN", b'T').await;
    let (sql, tag) = if opaque {
        ("SELECT mutate_ledger()", "SELECT 1")
    } else {
        ("UPDATE ledger SET value=1", "UPDATE 1")
    };
    settled(server, session, sql, tag, b'T').await;
    assert_eq!(committed(server), 0);
}

fn committed(server: &ProxyServer) -> u64 {
    server
        .state
        .metrics
        .journal_committed
        .load(Ordering::Relaxed)
}

async fn relay(server: &ProxyServer, session: &Arc<ClientSession>, wire: &[u8]) {
    let (socket, mut peer) = pair().await;
    let mut client = ClientStream::Plain(socket);
    let mut buffer = BytesMut::from(wire);
    let sent = ProxyServer::forward_buffered_backend_frames(
        &mut client,
        &mut buffer,
        session,
        &server.state,
    )
    .await
    .unwrap();
    assert_eq!(sent, wire.len());
    assert!(buffer.is_empty());
    let mut got = vec![0; wire.len()];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, wire);
}

#[cfg(feature = "query-cache")]
async fn fill(server: &ProxyServer) {
    server
        .state
        .query_cache
        .as_ref()
        .unwrap()
        .put(
            "SELECT value FROM ledger",
            &crate::cache::CacheContext::default(),
            bytes::Bytes::from_static(b"old"),
            1,
            Duration::from_millis(1),
        )
        .await;
    assert!(hit(server).await, "positive real QueryCache lookup");
}

#[cfg(feature = "query-cache")]
async fn hit(server: &ProxyServer) -> bool {
    matches!(
        server
            .state
            .query_cache
            .as_ref()
            .unwrap()
            .get(
                "SELECT value FROM ledger",
                &crate::cache::CacheContext::default(),
            )
            .await,
        crate::cache::CacheLookup::Hit { .. }
    )
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_journal_and_cache_retire_before_sync() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    fill(&server).await;
    register(&session, &[("end", "COMMIT")]);
    relay(&server, &session, &done("COMMIT")).await;
    assert_eq!(committed(&server), 1, "durable commit cannot wait for Sync");
    assert!(!hit(&server).await, "pre-commit refill must be retired");
    fill(&server).await;
    relay(&server, &session, &frame(b'Z', b"I")).await;
    assert_eq!(committed(&server), 1, "Sync must not publish history twice");
    assert!(hit(&server).await, "Sync must not reapply consumed commit");
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_opaque_stage_retires_before_sync() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, true).await;
    fill(&server).await;
    register(&session, &[("end", "COMMIT")]);
    relay(&server, &session, &done("COMMIT")).await;
    assert!(
        !hit(&server).await,
        "opaque effects still have unknown scope"
    );
    assert_eq!(committed(&server), 1);
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 1);
    assert!(
        history[0].incomplete_reason.is_some(),
        "opaque history cannot become replayable"
    );
}

#[tokio::test]
async fn commit_flush_journal_finishes_before_client_publication() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    register(&session, &[("end", "COMMIT")]);
    let (socket, mut peer) = pair().await;
    let mut client = ClientStream::Plain(socket);
    // The source-identity lock gives an exact journal barrier: a correct
    // relay cannot publish C while applying its commit is still waiting here.
    // The identity is resolved once per session, so make this commit resolve
    // it again.
    session.invalidate_journal_identity();
    let barrier = session.variables.write().await;
    let task_session = session.clone();
    let state = server.state.clone();
    let task = tokio::spawn(async move {
        let mut buffer = BytesMut::from(done("COMMIT").as_slice());
        ProxyServer::forward_buffered_backend_frames(
            &mut client,
            &mut buffer,
            &task_session,
            &state,
        )
        .await
    });
    let mut byte = [0];
    let early = tokio::time::timeout(Duration::from_millis(100), peer.read(&mut byte)).await;
    drop(barrier);
    assert!(
        early.is_err(),
        "client saw committed C before journal application"
    );
    task.await.unwrap().unwrap();
    assert_eq!(committed(&server), 1);
    let mut got = vec![0; done("COMMIT").len()];
    tokio::time::timeout(Duration::from_secs(2), peer.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, done("COMMIT"));
}

#[tokio::test]
async fn commit_flush_client_write_failure_keeps_committed_history() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    register(&session, &[("end", "COMMIT")]);
    let (socket, _peer) = pair().await;
    let mut client = ClientStream::Plain(socket);
    client.shutdown().await.unwrap();
    let mut buffer = BytesMut::from(done("COMMIT").as_slice());
    let error = ProxyServer::forward_buffered_backend_frames(
        &mut client,
        &mut buffer,
        &session,
        &server.state,
    )
    .await
    .expect_err("the local write half is shut down");
    assert!(error.to_string().contains("Client write"), "{error}");
    assert_eq!(
        committed(&server),
        1,
        "failed publication cannot erase durable commit"
    );
    ProxyServer::journal_close(&session, &server.state).await;
    assert_eq!(committed(&server), 1);
}

#[tokio::test]
async fn commit_flush_later_error_or_close_does_not_undo_commit() {
    for close in [false, true] {
        let server = server();
        let session = make_test_session();
        open_write(&server, &session, false).await;
        register(&session, &[("end", "COMMIT"), ("later", "SELECT 1 / 0")]);
        relay(&server, &session, &done("COMMIT")).await;
        assert_eq!(committed(&server), 1);
        if close {
            ProxyServer::journal_close(&session, &server.state).await;
        } else {
            relay(
                &server,
                &session,
                &[
                    frame(b'E', b"SERROR\0C22012\0Mdivision by zero\0\0"),
                    frame(b'Z', b"I"),
                ]
                .concat(),
            )
            .await;
        }
        let history = server.state.transaction_journal.committed_after(0).await;
        assert_eq!(history.len(), 1, "close={close}");
        assert_eq!(committed(&server), 1, "close={close}");
    }
}

#[tokio::test]
async fn commit_flush_read_prefix_and_future_execute_survive_rebase() {
    let server = server();
    let session = make_test_session();
    register(
        &session,
        &[
            ("read", "SELECT value FROM ledger"),
            ("first", "INSERT INTO ledger VALUES(1)"),
            ("end", "COMMIT"),
            ("future", "INSERT INTO ledger VALUES(2)"),
        ],
    );
    relay(
        &server,
        &session,
        &[done("SELECT 1"), done("INSERT 0 1"), done("COMMIT")].concat(),
    )
    .await;
    assert_eq!(committed(&server), 1);
    relay(
        &server,
        &session,
        &[done("INSERT 0 1"), frame(b'Z', b"I")].concat(),
    )
    .await;
    assert_eq!(committed(&server), 2);
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].entries.len(), 1);
    assert_eq!(history[1].entries.len(), 1);
    assert_eq!(
        history[0].entries[0].statement,
        "INSERT INTO ledger VALUES(1)"
    );
    assert_eq!(
        history[1].entries[0].statement,
        "INSERT INTO ledger VALUES(2)"
    );
}

#[tokio::test]
async fn commit_flush_multiple_boundaries_keep_distinct_history() {
    let server = server();
    let session = make_test_session();
    register(
        &session,
        &[
            ("a", "INSERT INTO ledger VALUES(1)"),
            ("end_a", "COMMIT"),
            ("b", "INSERT INTO ledger VALUES(2)"),
            ("end_b", "COMMIT"),
            ("c", "INSERT INTO ledger VALUES(3)"),
        ],
    );
    relay(
        &server,
        &session,
        &[
            done("INSERT 0 1"),
            done("COMMIT"),
            done("INSERT 0 1"),
            done("COMMIT"),
        ]
        .concat(),
    )
    .await;
    assert_eq!(
        committed(&server),
        2,
        "distinct durable boundaries at one Flush"
    );
    relay(
        &server,
        &session,
        &[done("INSERT 0 1"), frame(b'Z', b"I")].concat(),
    )
    .await;
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 3);
    for (i, entry) in history.iter().enumerate() {
        assert_eq!(entry.entries.len(), 1);
        assert_eq!(
            entry.entries[0].statement,
            format!("INSERT INTO ledger VALUES({})", i + 1)
        );
    }
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_prepared_completion_is_not_applied_twice() {
    let server = server();
    let session = make_test_session();
    fill(&server).await;
    register(&session, &[("end", "COMMIT PREPARED 'remote'")]);
    relay(&server, &session, &done("COMMIT PREPARED")).await;
    assert!(!hit(&server).await);
    assert_eq!(
        committed(&server),
        0,
        "remote prepared history is not local replay history"
    );
    fill(&server).await;
    relay(&server, &session, &frame(b'Z', b"I")).await;
    assert!(
        hit(&server).await,
        "Sync must not reapply prepared invalidation"
    );
    assert_eq!(committed(&server), 0);
}

#[cfg(feature = "edge-proxy")]
#[tokio::test]
async fn commit_flush_edge_event_is_recorded_before_sync() {
    for prepared in [false, true] {
        let mut config = test_config();
        config.tr_enabled = false;
        config.edge.enabled = true;
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();
        if !prepared {
            open_write(&server, &session, true).await;
        }
        let mut events = server
            .state
            .edge_registry
            .register("reader", "fixture", "", "now")
            .unwrap();
        let version = server.state.edge_cache.next_version();
        assert!(server.state.edge_cache.insert_if_fresh(
            crate::edge::CacheKey::new("ledger", "p"),
            crate::edge::CacheEntry {
                version,
                response_bytes: bytes::Bytes::from_static(b"old"),
                tables: vec!["ledger".into()],
                expires_at: std::time::Instant::now() + Duration::from_secs(60),
            },
        ));
        let (sql, tag) = if prepared {
            ("COMMIT PREPARED 'remote'", "COMMIT PREPARED")
        } else {
            ("COMMIT", "COMMIT")
        };
        register(&session, &[("end", sql)]);
        relay(&server, &session, &done(tag)).await;
        assert_eq!(server.state.edge_cache.stats().current_entries, 0);
        let event = events
            .try_recv()
            .expect("home event must survive missing Sync");
        assert_eq!(event.epoch, server.state.edge_cache.epoch());
        assert!(
            event.up_to_version > version,
            "event covers the actual seeded entry"
        );
        relay(&server, &session, &frame(b'Z', b"I")).await;
        assert!(
            events.try_recv().is_err(),
            "Sync emitted duplicate commit event"
        );
    }
}

#[tokio::test]
async fn commit_flush_dml_alone_does_not_infer_autocommit() {
    let server = server();
    let session = make_test_session();
    register(&session, &[("write", "INSERT INTO ledger VALUES(1)")]);
    relay(&server, &session, &done("INSERT 0 1")).await;
    assert_eq!(
        committed(&server),
        0,
        "DML C is not the implicit commit boundary"
    );
    relay(&server, &session, &frame(b'Z', b"I")).await;
    assert_eq!(committed(&server), 1);
}

#[tokio::test]
async fn commit_flush_rollback_and_failed_commit_are_not_commits() {
    for sql in ["ROLLBACK", "COMMIT"] {
        let server = server();
        let session = make_test_session();
        open_write(&server, &session, false).await;
        register(&session, &[("end", sql)]);
        relay(&server, &session, &done("ROLLBACK")).await;
        assert_eq!(committed(&server), 0);
        relay(&server, &session, &frame(b'Z', b"I")).await;
        assert_eq!(committed(&server), 0);
    }
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_partial_tag_waits_for_the_complete_frame() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    fill(&server).await;
    register(&session, &[("end", "COMMIT")]);
    let (socket, peer) = pair().await;
    let mut client = ClientStream::Plain(socket);
    let complete = done("COMMIT");
    let mut buffer = BytesMut::from(&complete[..complete.len() - 1]);
    assert_eq!(
        ProxyServer::forward_buffered_backend_frames(
            &mut client,
            &mut buffer,
            &session,
            &server.state
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(committed(&server), 0);
    assert!(hit(&server).await);
    assert!(
        matches!(peer.try_read(&mut [0]), Err(error) if error.kind()==std::io::ErrorKind::WouldBlock)
    );
    buffer.extend_from_slice(&complete[complete.len() - 1..]);
    ProxyServer::forward_buffered_backend_frames(&mut client, &mut buffer, &session, &server.state)
        .await
        .unwrap();
    assert_eq!(committed(&server), 1);
    assert!(!hit(&server).await);
}

#[tokio::test]
async fn commit_flush_rollback_prefix_is_not_committed_history() {
    let server = server();
    let session = make_test_session();
    register(
        &session,
        &[
            ("rolled_back", "INSERT INTO ledger VALUES(1)"),
            ("rollback", "ROLLBACK"),
            ("committed", "INSERT INTO ledger VALUES(2)"),
            ("commit", "COMMIT"),
        ],
    );
    relay(
        &server,
        &session,
        &[
            done("INSERT 0 1"),
            done("ROLLBACK"),
            done("INSERT 0 1"),
            done("COMMIT"),
        ]
        .concat(),
    )
    .await;
    assert_eq!(committed(&server), 1);
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].entries.len(),
        1,
        "rolled-back prefix must not be replayed"
    );
    assert_eq!(
        history[0].entries[0].statement,
        "INSERT INTO ledger VALUES(2)"
    );
    relay(&server, &session, &frame(b'Z', b"I")).await;
    assert_eq!(committed(&server), 1);
}

#[tokio::test]
async fn commit_flush_suspended_prefix_makes_history_incomplete() {
    let server = server();
    let session = make_test_session();
    let wire = [
        execute_limit(
            "partial",
            "INSERT INTO ledger VALUES(1),(2) RETURNING value",
            1,
        ),
        execute("begin", "BEGIN"),
        execute("end", "COMMIT"),
    ]
    .concat();
    ProxyServer::journal_register_batch(&session, &wire, None, true, true);
    relay(
        &server,
        &session,
        &[frame(b's', b""), done("BEGIN"), done("COMMIT")].concat(),
    )
    .await;
    assert_eq!(committed(&server), 1);
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 1);
    assert!(
        history[0].incomplete_reason.is_some(),
        "a suspended write's full SQL is unproven"
    );
}

#[tokio::test]
async fn commit_flush_malformed_tag_is_not_an_early_commit_proof() {
    for tag in [
        b"COMMIT".as_slice(),
        b"COMMIT\0junk\0",
        b"COMMIT PREPARED\0junk\0",
        b"COMMIT\xff\0",
    ] {
        for captured in [false, true] {
            let server = server();
            let session = make_test_session();
            if captured {
                open_write(&server, &session, false).await;
                register(&session, &[("end", "COMMIT")]);
            }
            #[cfg(feature = "query-cache")]
            fill(&server).await;
            let (socket, _peer) = pair().await;
            let mut client = ClientStream::Plain(socket);
            let wire = frame(b'C', tag);
            let mut buffer = BytesMut::from(wire.as_slice());
            match ProxyServer::forward_buffered_backend_frames(
                &mut client,
                &mut buffer,
                &session,
                &server.state,
            )
            .await
            {
                Ok(sent) => assert_eq!(sent, wire.len()),
                Err(ProxyError::Protocol(message)) => assert!(message.contains("CommandComplete")),
                Err(error) => panic!("unrelated relay error: {error}"),
            }
            assert_eq!(committed(&server), 0, "malformed tag={tag:?}");
            #[cfg(feature = "query-cache")]
            assert!(
                hit(&server).await,
                "neither captured nor raw fallback may credit malformed C"
            );
            ProxyServer::journal_close(&session, &server.state).await;
            assert_eq!(committed(&server), 0);
        }
    }
}

#[tokio::test]
async fn commit_flush_future_bound_portal_survives_commit_observation() {
    let server = server();
    let session = make_test_session();
    let future_execute = frame(b'E', b"future\0\0\0\0\0");
    let mut future_bind = execute("future", "INSERT INTO ledger VALUES(2)");
    future_bind.truncate(future_bind.len() - future_execute.len());
    let wire = [
        execute("first", "INSERT INTO ledger VALUES(1)"),
        execute("end", "COMMIT"),
        future_bind,
    ]
    .concat();
    ProxyServer::journal_register_batch(&session, &wire, None, true, true);
    relay(
        &server,
        &session,
        &[done("INSERT 0 1"), done("COMMIT")].concat(),
    )
    .await;
    assert_eq!(committed(&server), 1);
    ProxyServer::journal_register_batch(&session, &future_execute, None, false, true);
    relay(
        &server,
        &session,
        &[done("INSERT 0 1"), frame(b'Z', b"I")].concat(),
    )
    .await;
    assert_eq!(
        committed(&server),
        2,
        "future Bind metadata was lost at prior commit"
    );
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 2);
    assert_eq!(history[1].entries.len(), 1);
    assert_eq!(
        history[1].entries[0].statement,
        "INSERT INTO ledger VALUES(2)"
    );
}
