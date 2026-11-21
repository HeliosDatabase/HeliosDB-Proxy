use super::*;

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_error_before_tag_cannot_establish_prefix_proof() {
    for captured in [false, true] {
        for split in [false, true] {
            let server = server();
            let session = make_test_session();
            if captured {
                open_write(&server, &session, false).await;
                register(&session, &[("end", "COMMIT")]);
            }
            fill(&server).await;
            let (socket, _peer) = pair().await;
            let mut client = ClientStream::Plain(socket);
            let tag = if captured {
                "COMMIT"
            } else {
                "COMMIT PREPARED"
            };
            let error = frame(b'E', b"SERROR\0CXX000\0Mearlier failure\0\0");
            let wire = if split {
                let mut earlier = BytesMut::from(error.as_slice());
                ProxyServer::forward_buffered_backend_frames(
                    &mut client,
                    &mut earlier,
                    &session,
                    &server.state,
                )
                .await
                .unwrap();
                done(tag)
            } else {
                [error, done(tag)].concat()
            };
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
            assert_eq!(committed(&server), 0);
            assert!(
                hit(&server).await,
                "a completion after the first error has no proven prefix"
            );
            ProxyServer::journal_close(&session, &server.state).await;
            assert_eq!(committed(&server), 0);
        }
    }
}

#[tokio::test]
async fn commit_flush_normal_stream_settles_before_later_ready() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    register(&session, &[("end", "COMMIT")]);
    let (mut backend, mut source) = pair().await;
    let (socket, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(socket);
    let state = server.state.clone();
    let task_session = session.clone();
    let task = tokio::spawn(async move {
        ProxyServer::stream_until_ready_buffered(
            &mut client,
            &mut backend,
            &mut BytesMut::new(),
            &task_session,
            &state,
            0,
        )
        .await
    });
    source.write_all(&done("COMMIT")).await.unwrap();
    let mut got = vec![0; done("COMMIT").len()];
    tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, done("COMMIT"));
    let before_ready = committed(&server);
    source.write_all(&frame(b'Z', b"I")).await.unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(
        before_ready, 1,
        "normal stream published C before the committed history"
    );
    assert_eq!(committed(&server), 1);
}

#[tokio::test]
async fn commit_flush_copy_yield_preserves_the_future_copy_metadata() {
    let server = server();
    let session = make_test_session();
    register(
        &session,
        &[
            ("first", "INSERT INTO ledger VALUES(1)"),
            ("end", "COMMIT"),
            ("copy", "COPY ledger FROM STDIN"),
        ],
    );
    let (mut backend, mut source) = pair().await;
    let (socket, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(socket);
    let mut buffer = BytesMut::new();
    let prefix = [
        done("INSERT 0 1"),
        done("COMMIT"),
        frame(b'G', &[0, 0, 1, 0, 0]),
    ]
    .concat();
    source.write_all(&prefix).await.unwrap();
    let sent = tokio::time::timeout(
        Duration::from_secs(2),
        ProxyServer::stream_until_ready_buffered(
            &mut client,
            &mut backend,
            &mut buffer,
            &session,
            &server.state,
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(sent, prefix.len() as u64);
    let mut got = vec![0; prefix.len()];
    recipient.read_exact(&mut got).await.unwrap();
    assert_eq!(got, prefix);
    let before_copy = committed(&server);
    let tail = [frame(b'c', b""), done("COPY 1"), frame(b'Z', b"I")].concat();
    source.write_all(&tail).await.unwrap();
    ProxyServer::stream_until_ready_buffered(
        &mut client,
        &mut backend,
        &mut buffer,
        &session,
        &server.state,
        0,
    )
    .await
    .unwrap();
    assert_eq!(
        before_copy, 1,
        "committed prefix was waiting for COPY's final Sync"
    );
    assert_eq!(committed(&server), 2);
    let history = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].entries.len(), 1);
    assert_eq!(history[1].entries.len(), 1);
    assert_eq!(history[1].entries[0].statement, "COPY ledger FROM STDIN");
    assert!(history[1].incomplete_reason.is_some());
}

#[tokio::test]
async fn commit_flush_implicit_incomplete_reason_survives_begin() {
    for streamed in [false, true] {
        for oversize in [false, true] {
            let server = server();
            let session = make_test_session();
            if oversize {
                *session.journal.lock().unwrap() = crate::journal_capture::SessionCapture::new(8);
            }
            register(
                &session,
                &[
                    ("opaque", "SELECT mutate_ledger()"),
                    ("begin", "BEGIN"),
                    ("end", "COMMIT"),
                ],
            );
            if streamed {
                relay(
                    &server,
                    &session,
                    &[
                        done("SELECT 1"),
                        done("BEGIN"),
                        done("COMMIT"),
                        frame(b'Z', b"I"),
                    ]
                    .concat(),
                )
                .await;
            } else {
                let cycle = ProxyServer::journal_observe_sync(
                    &session,
                    &server.state,
                    ResponseOutcome {
                        status: b'I',
                        completions: ["SELECT 1", "BEGIN", "COMMIT"]
                            .iter()
                            .map(|tag| Completion::Tag((*tag).into()))
                            .collect(),
                        error: None,
                    },
                )
                .unwrap();
                ProxyServer::journal_observe_finish(&session, &server.state, cycle).await;
            }
            assert_eq!(committed(&server), 1);
            let history = server.state.transaction_journal.committed_after(0).await;
            assert_eq!(history.len(), 1);
            assert!(
                history[0].incomplete_reason.is_some(),
                "streamed={streamed} oversize={oversize}"
            );
        }
    }
}

#[tokio::test]
async fn commit_flush_single_simple_commit_survives_publication_failure() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    ProxyServer::journal_register_simple_cycle(&session, "COMMIT", true);
    let (mut backend, mut source) = pair().await;
    let (socket, _recipient) = pair().await;
    let mut client = ClientStream::Plain(socket);
    client.shutdown().await.unwrap();
    source.write_all(&done("COMMIT")).await.unwrap();
    let failure = tokio::time::timeout(
        Duration::from_secs(2),
        ProxyServer::stream_until_ready_buffered(
            &mut client,
            &mut backend,
            &mut BytesMut::new(),
            &session,
            &server.state,
            0,
        ),
    )
    .await
    .unwrap()
    .expect_err("closed client write half");
    assert!(failure.error.to_string().contains("Client write"));
    assert_eq!(
        committed(&server),
        1,
        "one simple COMMIT's C already proves durability"
    );
    ProxyServer::journal_close(&session, &server.state).await;
    assert_eq!(
        server
            .state
            .transaction_journal
            .committed_after(0)
            .await
            .len(),
        1
    );
}

#[cfg(feature = "query-cache")]
fn generation(server: &ProxyServer) -> u64 {
    server
        .state
        .table_generations
        .as_ref()
        .unwrap()
        .global_generation()
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_declined_capture_raw_prepared_has_one_owner() {
    for count in [2, 1] {
        let server = server();
        let session = make_test_session();
        ProxyServer::journal_register_simple_cycle(&session, "COMMIT PREPARED 'remote'", true);
        fill(&server).await;
        let before = generation(&server);
        let wire = done("COMMIT PREPARED").repeat(count);
        relay(&server, &session, &wire).await;
        assert_eq!(generation(&server), before + count as u64);
        assert!(!hit(&server).await);
        fill(&server).await;
        let after_raw = generation(&server);
        relay(&server, &session, &frame(b'Z', b"I")).await;
        assert_eq!(
            generation(&server),
            after_raw,
            "RFQ duplicated a raw-applied completion"
        );
        assert!(hit(&server).await);
        assert_eq!(committed(&server), 0);
    }
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_fresh_prepared_suffix_is_still_applied() {
    let server = server();
    let session = make_test_session();
    ProxyServer::journal_register_simple_cycle(&session, "COMMIT PREPARED 'first'", true);
    relay(&server, &session, &done("COMMIT PREPARED")).await;
    fill(&server).await;
    let before_suffix = generation(&server);
    // The raw observer covers authoritative prepared completions even when
    // the local preparing/statement metadata is unavailable. A newer suffix
    // must not be skipped merely because an older raw completion was applied.
    relay(
        &server,
        &session,
        &[done("COMMIT PREPARED"), frame(b'Z', b"I")].concat(),
    )
    .await;
    assert_eq!(
        generation(&server),
        before_suffix + 1,
        "exactly the fresh suffix is applied"
    );
    assert!(!hit(&server).await);
    fill(&server).await;
    let next_cycle = generation(&server);
    ProxyServer::journal_register_simple_cycle(&session, "COMMIT PREPARED 'third'", true);
    relay(&server, &session, &done("COMMIT PREPARED")).await;
    assert_eq!(
        generation(&server),
        next_cycle + 1,
        "old raw ownership leaked into a fresh cycle"
    );
    fill(&server).await;
    relay(&server, &session, &frame(b'Z', b"I")).await;
    assert!(hit(&server).await);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_uncaptured_multiple_prepared_completions_control() {
    let server = server();
    let session = make_test_session();
    fill(&server).await;
    let before = generation(&server);
    relay(&server, &session, &done("COMMIT PREPARED").repeat(2)).await;
    assert_eq!(generation(&server), before + 2);
    assert!(!hit(&server).await);
    fill(&server).await;
    let settled = generation(&server);
    relay(&server, &session, &frame(b'Z', b"I")).await;
    assert_eq!(generation(&server), settled);
    assert!(hit(&server).await);
}

#[tokio::test]
async fn commit_flush_later_malformed_tag_does_not_erase_proven_commit() {
    let server = server();
    let session = make_test_session();
    open_write(&server, &session, false).await;
    register(
        &session,
        &[
            ("end", "COMMIT"),
            ("future", "INSERT INTO ledger VALUES(2)"),
        ],
    );
    let wire = [done("COMMIT"), frame(b'C', b"COMMIT\0junk\0")].concat();
    let (socket, _peer) = pair().await;
    let mut client = ClientStream::Plain(socket);
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
    assert_eq!(
        committed(&server),
        1,
        "invalid later metadata cannot undo proven COMMIT"
    );
    ProxyServer::journal_close(&session, &server.state).await;
    assert_eq!(committed(&server), 1);
}

#[tokio::test]
async fn commit_flush_valid_prefix_survives_a_later_malformed_header() {
    for streamed in [false, true] {
        for length in [3u32, u32::MAX] {
            let server = server();
            let session = make_test_session();
            open_write(&server, &session, false).await;
            register(&session, &[("end", "COMMIT")]);
            let mut wire = done("COMMIT");
            wire.push(b'C');
            wire.extend_from_slice(&length.to_be_bytes());
            let (socket, _peer) = pair().await;
            let mut client = ClientStream::Plain(socket);
            let failure = if streamed {
                let (mut backend, _source) = pair().await;
                tokio::time::timeout(
                    Duration::from_secs(2),
                    ProxyServer::stream_until_ready_buffered(
                        &mut client,
                        &mut backend,
                        &mut BytesMut::from(wire.as_slice()),
                        &session,
                        &server.state,
                        0,
                    ),
                )
                .await
                .unwrap()
                .expect_err("invalid backend header")
                .error
            } else {
                ProxyServer::forward_buffered_backend_frames(
                    &mut client,
                    &mut BytesMut::from(wire.as_slice()),
                    &session,
                    &server.state,
                )
                .await
                .expect_err("invalid backend header")
            };
            assert!(matches!(failure, ProxyError::Protocol(_)));
            assert_eq!(
                committed(&server),
                1,
                "streamed={streamed} length={length}: later header cannot undo completed COMMIT"
            );
            ProxyServer::journal_close(&session, &server.state).await;
            assert_eq!(committed(&server), 1);
        }
    }
}
