use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag];
    bytes.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[cfg(feature = "query-cache")]
fn cached_server() -> ProxyServer {
    let mut config = test_config();
    config.cache.enabled = true;
    config.tr_enabled = false;
    ProxyServer::new(config).unwrap()
}

#[cfg(feature = "query-cache")]
async fn fill(server: &ProxyServer) {
    server
        .state
        .query_cache
        .as_ref()
        .unwrap()
        .put(
            "SELECT value FROM widgets",
            &crate::cache::CacheContext::default(),
            bytes::Bytes::from_static(b"old"),
            1,
            Duration::from_millis(1),
        )
        .await;
    assert!(hit(server).await);
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
                "SELECT value FROM widgets",
                &crate::cache::CacheContext::default(),
            )
            .await,
        crate::cache::CacheLookup::Hit { .. }
    )
}

async fn flush_prefix(
    client: &mut ClientStream,
    backend: &mut BackendConn,
    session: &Arc<ClientSession>,
    state: &Arc<ServerState>,
    expected: usize,
) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.response_buf.len() < expected {
            backend.stream.readable().await.unwrap();
            assert_eq!(
                ProxyServer::stream_flush(client, backend, session, state)
                    .await
                    .unwrap(),
                0
            );
        }
    })
    .await
    .expect("partial Flush must return promptly");
    assert_eq!(backend.response_buf.len(), expected);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_flush_withholds_every_partial_header_and_payload() {
    let server = cached_server();
    let session = make_test_session();
    let commit = frame(b'C', b"COMMIT PREPARED\0");
    for split in 1..commit.len() {
        fill(&server).await;
        let (backend, mut source) = pair().await;
        let mut backend = BackendConn::new(backend);
        let (client, mut recipient) = pair().await;
        let mut client = ClientStream::Plain(client);
        source.write_all(&commit[..split]).await.unwrap();
        flush_prefix(&mut client, &mut backend, &session, &server.state, split).await;
        assert!(hit(&server).await, "incomplete tag must not invalidate");
        let mut probe = [0; 1];
        assert!(
            matches!(recipient.try_read(&mut probe), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
        );

        source.write_all(&commit[split..]).await.unwrap();
        let mut sent = 0;
        tokio::time::timeout(Duration::from_secs(2), async {
            while sent < commit.len() as u64 {
                backend.stream.readable().await.unwrap();
                sent +=
                    ProxyServer::stream_flush(&mut client, &mut backend, &session, &server.state)
                        .await
                        .unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(sent, commit.len() as u64);
        assert!(
            !hit(&server).await,
            "successful completion invalidates before Sync, TR off"
        );
        assert!(backend.response_buf.is_empty());
        let mut got = vec![0; commit.len()];
        tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, commit);
    }
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn commit_fragment_survives_flush_watch_and_sync_handoffs() {
    let server = cached_server();
    fill(&server).await;
    let session = make_test_session();
    let (backend, mut source) = pair().await;
    let mut conns = HashMap::from([("a".to_string(), BackendConn::new(backend))]);
    let (client, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(client);
    let commit = frame(b'C', b"COMMIT PREPARED\0");
    let ready = frame(b'Z', b"I");
    source.write_all(&commit[..3]).await.unwrap();
    flush_prefix(
        &mut client,
        conns.get_mut("a").unwrap(),
        &session,
        &server.state,
        3,
    )
    .await;
    let mut second = commit[3..].to_vec();
    second.extend_from_slice(&ready[..3]);
    source.write_all(&second).await.unwrap();
    let mut client_input = BytesMut::new();
    let result = ProxyServer::read_next_client_message(
        &mut client,
        &mut client_input,
        &mut conns,
        Some("a"),
        Some(tokio::time::Instant::now() + Duration::from_millis(100)),
        &server.state,
    )
    .await
    .unwrap();
    assert_eq!(result, ClientRead::IdleTimeout);
    assert_eq!(&conns["a"].response_buf[..], &ready[..3]);
    assert!(
        !hit(&server).await,
        "watch must observe C before forwarding, without waiting for RFQ"
    );
    let mut got = vec![0; commit.len()];
    tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, commit);

    source.write_all(&ready[3..]).await.unwrap();
    let bc = conns.get_mut("a").unwrap();
    let sent = tokio::time::timeout(
        Duration::from_secs(2),
        ProxyServer::stream_until_ready_buffered(
            &mut client,
            &mut bc.stream,
            &mut bc.response_buf,
            &session,
            &server.state,
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(sent, ready.len() as u64);
    assert!(bc.response_buf.is_empty());
    let mut got = vec![0; ready.len()];
    tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, ready);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn sync_finishes_partial_commit_before_later_error() {
    let server = cached_server();
    fill(&server).await;
    let session = make_test_session();
    let (backend, mut source) = pair().await;
    let mut backend = BackendConn::new(backend);
    let (client, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(client);
    let commit = frame(b'C', b"COMMIT PREPARED\0");
    source.write_all(&commit[..commit.len() - 1]).await.unwrap();
    flush_prefix(
        &mut client,
        &mut backend,
        &session,
        &server.state,
        commit.len() - 1,
    )
    .await;
    let error = frame(b'E', b"SERROR\0C22012\0Mdivision by zero\0\0");
    let ready = frame(b'Z', b"I");
    let suffix = [&commit[commit.len() - 1..], &error, &ready].concat();
    source.write_all(&suffix).await.unwrap();
    let expected = [&commit[..], &error, &ready].concat();
    let sent = tokio::time::timeout(
        Duration::from_secs(2),
        ProxyServer::stream_until_ready_buffered(
            &mut client,
            &mut backend.stream,
            &mut backend.response_buf,
            &session,
            &server.state,
            0,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(sent, expected.len() as u64);
    assert!(!hit(&server).await);
    let mut got = vec![0; expected.len()];
    tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, expected);
}

#[cfg(feature = "query-cache")]
#[tokio::test]
async fn prepared_commit_observer_ignores_other_tags_and_embedded_text() {
    let server = cached_server();
    fill(&server).await;
    for (tag, payload) in [
        (b'C', b"PREPARE TRANSACTION\0".as_slice()),
        (b'C', b"ROLLBACK PREPARED\0".as_slice()),
        (b'C', b"COMMIT\0".as_slice()),
        (b'C', b"COMMIT PREPARED extra\0".as_slice()),
        (b'D', b"COMMIT PREPARED\0".as_slice()),
        (b'N', b"COMMIT PREPARED\0".as_slice()),
        (
            b'E',
            b"SERROR\0C42704\0Mmissing prepared transaction\0\0".as_slice(),
        ),
    ] {
        let bytes = frame(tag, payload);
        assert_eq!(
            ProxyServer::complete_frame_prefix(&bytes, 1024).unwrap(),
            bytes.len()
        );
        ProxyServer::observe_prepared_commit_frames(&bytes, &server.state);
        assert!(
            hit(&server).await,
            "only exact successful backend tag invalidates"
        );
    }
}

#[tokio::test]
async fn backend_frame_tail_is_isolated_by_socket_and_dropped_on_loss() {
    let server = ProxyServer::new(test_config()).unwrap();
    let session = make_test_session();
    let (a, mut source_a) = pair().await;
    let (b, mut source_b) = pair().await;
    let (client, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(client);
    let mut conns = HashMap::from([
        ("a".to_string(), BackendConn::new(a)),
        ("b".to_string(), BackendConn::new(b)),
    ]);
    let a_frame = frame(b'N', b"SNOTICE\0Mfrom a\0\0");
    let b_frame = frame(b'N', b"SNOTICE\0Mfrom b\0\0");
    source_a.write_all(&a_frame[..7]).await.unwrap();
    flush_prefix(
        &mut client,
        conns.get_mut("a").unwrap(),
        &session,
        &server.state,
        7,
    )
    .await;
    source_b.write_all(&b_frame).await.unwrap();
    let mut input = BytesMut::new();
    assert_eq!(
        ProxyServer::read_next_client_message(
            &mut client,
            &mut input,
            &mut conns,
            Some("b"),
            Some(tokio::time::Instant::now() + Duration::from_millis(100)),
            &server.state,
        )
        .await
        .unwrap(),
        ClientRead::IdleTimeout
    );
    assert_eq!(&conns["a"].response_buf[..], &a_frame[..7]);
    assert!(conns["b"].response_buf.is_empty());
    let mut got = vec![0; b_frame.len()];
    tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, b_frame);
    drop(source_a);
    assert_eq!(
        ProxyServer::read_next_client_message(
            &mut client,
            &mut input,
            &mut conns,
            Some("a"),
            Some(tokio::time::Instant::now() + Duration::from_millis(100)),
            &server.state,
        )
        .await
        .unwrap(),
        ClientRead::IdleTimeout
    );
    assert!(
        !conns.contains_key("a"),
        "dead socket drops its unpublished tail"
    );
    let mut probe = [0; 1];
    assert!(
        matches!(recipient.try_read(&mut probe), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[tokio::test]
async fn flush_rejects_malformed_or_oversized_header_without_forwarding() {
    let server = ProxyServer::new(test_config()).unwrap();
    let session = make_test_session();
    for len in [3, u32::MAX] {
        let (backend, _source) = pair().await;
        let mut backend = BackendConn::new(backend);
        backend.response_buf.extend_from_slice(b"C");
        backend.response_buf.extend_from_slice(&len.to_be_bytes());
        let (client, recipient) = pair().await;
        let mut client = ClientStream::Plain(client);
        let error = ProxyServer::stream_flush(&mut client, &mut backend, &session, &server.state)
            .await
            .unwrap_err();
        assert!(matches!(error.error, ProxyError::Protocol(_)));
        let mut probe = [0; 1];
        assert!(
            matches!(recipient.try_read(&mut probe), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
        );
    }
}

#[tokio::test]
async fn pending_frame_drain_preserves_internal_reply_boundary() {
    let server = ProxyServer::new(test_config()).unwrap();
    let (backend, mut source) = pair().await;
    let mut backend = BackendConn::new(backend);
    let (client, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(client);
    let notice = frame(b'N', b"SNOTICE\0Mpending\0\0");
    backend.response_buf.extend_from_slice(&notice[..3]);
    source.write_all(&notice[3..]).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        ProxyServer::finish_pending_backend_frames(&mut client, &mut backend, &server.state),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(backend.response_buf.is_empty());
    let mut got = vec![0; notice.len()];
    tokio::time::timeout(Duration::from_secs(2), recipient.read_exact(&mut got))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, notice);
    source.write_all(&frame(b'1', b"")).await.unwrap();
    assert_eq!(
        ProxyServer::read_one_frame_type(&mut backend.stream, 1024)
            .await
            .unwrap(),
        b'1'
    );
}

#[tokio::test]
async fn replay_refuses_pending_frame_before_sending_statements() {
    let server = ProxyServer::new(test_config()).unwrap();
    let (stream, source) = pair().await;
    let mut backend = BackendConn::new(stream);
    backend.response_buf.extend_from_slice(&[b'N', 0, 0]);
    let mut conns = HashMap::from([("a".to_string(), backend)]);
    let entries = vec![StatementLog {
        sql: "BEGIN".into(),
        params: Vec::new(),
        result_checksum: None,
        executed_at: chrono::Utc::now(),
        extended: None,
    }];
    let result = ProxyServer::tr_replay_transaction(
        &mut conns,
        "a",
        &entries,
        &HashMap::new(),
        &server.state,
    )
    .await;
    assert!(matches!(
        result,
        Err(ReplayFailure::Backend(ProxyError::Protocol(_)))
    ));
    let mut probe = [0; 1];
    assert!(
        matches!(source.try_read(&mut probe), Err(e) if e.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[tokio::test]
async fn recovery_redials_cached_socket_with_unpublished_tail() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let mut config = ProxyConfig::default();
    config.add_node(&address, "primary").unwrap();
    let server = ProxyServer::new(config.clone()).unwrap();
    let session = make_test_session();
    let (old, mut old_peer) = pair().await;
    let mut old = BackendConn::new(old);
    old.response_buf.extend_from_slice(&[b'N', 0, 0]);
    let mut conns = HashMap::from([(address.clone(), old)]);
    let serving = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let length = socket.read_u32().await.unwrap();
        assert!((8..4096).contains(&length));
        let mut startup = vec![0; length as usize - 4];
        socket.read_exact(&mut startup).await.unwrap();
        socket
            .write_all(&[frame(b'R', &[0; 4]), frame(b'Z', b"I")].concat())
            .await
            .unwrap();
        socket
    });
    let selected = ProxyServer::tr_acquire_replacement(
        &mut conns,
        &TrSession::new(TrMode::Session),
        &session,
        &server.state,
        &config,
        tokio::time::Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert_eq!(selected, address);
    assert!(conns[&address].response_buf.is_empty());
    let _new_peer = tokio::time::timeout(Duration::from_secs(2), serving)
        .await
        .unwrap()
        .unwrap();
    let mut probe = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), old_peer.read(&mut probe))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}
