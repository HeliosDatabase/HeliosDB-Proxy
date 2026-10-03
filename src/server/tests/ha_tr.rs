use crate::transaction_journal::TransactionJournal;
use crate::NodeId;

#[tokio::test]
async fn journal_records_and_windows_a_statement() {
    let j = TransactionJournal::new();
    let from = chrono::Utc::now() - chrono::Duration::seconds(60);
    let tx = uuid::Uuid::new_v4();
    j.begin_transaction(tx, uuid::Uuid::new_v4(), NodeId::new(), 0)
        .await
        .unwrap();
    j.log_statement(
        tx,
        "insert into t values (1)".to_string(),
        Vec::new(),
        None,
        None,
        0,
    )
    .await
    .unwrap();
    let to = chrono::Utc::now() + chrono::Duration::seconds(60);
    // TR-07: the window is committed history only.
    assert!(j.entries_in_window(from, to).await.is_empty());
    j.commit_transaction(tx).await.unwrap();
    let entries = j.entries_in_window(from, to).await;
    assert_eq!(entries.len(), 1, "journaled statement should be in window");
    assert!(entries[0].1.statement.contains("insert"));
}

/// The write path registers each statement with the session capture
/// and the relay's observation commits it: an autocommit write becomes
/// one committed transaction with its outcome and source identity
/// (TR-07). Reads register nothing.
#[tokio::test]
async fn write_path_capture_commits_one_transaction_per_autocommit_write() {
    use super::{make_test_session, test_config};
    use crate::journal_capture::{Completion, ResponseOutcome};
    use crate::server::ProxyServer;
    use std::sync::atomic::Ordering;

    let server = ProxyServer::new(test_config()).unwrap();
    let session = make_test_session();
    let from = chrono::Utc::now() - chrono::Duration::seconds(60);

    for (sql, tag) in [
        ("insert into t values (1)", "INSERT 0 1"),
        ("update t set a = 2", "UPDATE 3"),
    ] {
        ProxyServer::journal_register_simple(&session, sql);
        assert!(session.journal_armed.load(Ordering::Relaxed));
        ProxyServer::journal_observe(
            &session,
            &server.state,
            ResponseOutcome {
                status: b'I',
                completions: vec![Completion::Tag(tag.into())],
                error: None,
            },
        )
        .await;
        assert!(!session.journal_armed.load(Ordering::Relaxed));
    }
    ProxyServer::journal_register_simple(&session, "select 1");
    assert!(!session.journal_armed.load(Ordering::Relaxed));

    let to = chrono::Utc::now() + chrono::Duration::seconds(60);
    let entries = server
        .state
        .transaction_journal
        .entries_in_window(from, to)
        .await;
    assert_eq!(entries.len(), 2, "one journal entry per write");
    assert_ne!(
        entries[0].0, entries[1].0,
        "each write is its own transaction"
    );
    assert_eq!(
        server.state.transaction_journal.active_count().await,
        0,
        "autocommit writes are committed, never left active"
    );
    let committed = server.state.transaction_journal.committed_after(0).await;
    assert_eq!(committed.len(), 2);
    assert_eq!(committed[0].commit_seq, Some(1));
    assert_eq!(committed[1].commit_seq, Some(2));
    assert_eq!(committed[1].entries[0].rows_affected, Some(3));
    assert_eq!(
        committed[0].source.client_addr,
        session.client_addr.to_string()
    );
    assert_eq!(
        server
            .state
            .metrics
            .journal_committed
            .load(Ordering::Relaxed),
        2
    );
}

/// A write the backend rejected is never journaled, and a rolled-back
/// explicit transaction leaves no committed history (TR-07).
#[tokio::test]
async fn write_path_capture_excludes_failed_and_rolled_back_work() {
    use super::{make_test_session, test_config};
    use crate::journal_capture::{Completion, ResponseOutcome};
    use crate::server::ProxyServer;
    use std::sync::atomic::Ordering;

    let server = ProxyServer::new(test_config()).unwrap();
    let session = make_test_session();
    let cycle = |sql: &str, status: u8, tags: &[&str], err: bool| {
        let session = session.clone();
        let sql = sql.to_string();
        let tags: Vec<String> = tags.iter().map(|t| t.to_string()).collect();
        let state = server.state.clone();
        async move {
            ProxyServer::journal_register_simple(&session, &sql);
            ProxyServer::journal_observe(
                &session,
                &state,
                ResponseOutcome {
                    status,
                    completions: tags.into_iter().map(Completion::Tag).collect(),
                    error: err.then(|| ("23505".to_string(), "dup".to_string())),
                },
            )
            .await;
        }
    };
    cycle("insert into t values (1)", b'I', &[], true).await;
    cycle("BEGIN", b'T', &["BEGIN"], false).await;
    assert!(session.journal_open.load(Ordering::Relaxed));
    cycle("insert into t values (2)", b'T', &["INSERT 0 1"], false).await;
    assert_eq!(server.state.transaction_journal.active_count().await, 1);
    cycle("ROLLBACK", b'I', &["ROLLBACK"], false).await;
    assert!(!session.journal_open.load(Ordering::Relaxed));
    assert_eq!(server.state.transaction_journal.active_count().await, 0);
    assert!(server
        .state
        .transaction_journal
        .committed_after(0)
        .await
        .is_empty());
    assert_eq!(
        server
            .state
            .metrics
            .journal_rolled_back
            .load(Ordering::Relaxed),
        1
    );
    // A session that ends mid-transaction drops its active journal.
    cycle("BEGIN", b'T', &["BEGIN"], false).await;
    cycle("insert into t values (3)", b'T', &["INSERT 0 1"], false).await;
    ProxyServer::journal_close(&session, &server.state).await;
    assert_eq!(server.state.transaction_journal.active_count().await, 0);
}
