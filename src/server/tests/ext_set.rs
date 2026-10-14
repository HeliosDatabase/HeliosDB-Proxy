//! Extended-protocol SET tracking: the per-cycle decision (Sprinter
//! d3a4c04c28be). The client loop feeds every Parse, Bind, Execute and
//! boundary of a cycle; only a lone SET is tracked, anything doubtful pins.
use super::*;

const SET: &str = "SET search_path TO t2";

fn sync(forwarded: bool, idle_before: bool, idle_after: bool, failed: bool) -> ExtSetBoundary {
    ExtSetBoundary {
        sync: true,
        forwarded,
        idle_before,
        idle_after,
        failed,
    }
}

fn ok_sync() -> ExtSetBoundary {
    sync(true, true, true, false)
}

fn flush(forwarded: bool) -> ExtSetBoundary {
    ExtSetBoundary {
        sync: false,
        forwarded,
        idle_before: true,
        idle_after: false,
        failed: false,
    }
}

/// Parse(unnamed SET) Bind Execute — the JDBC / Npgsql / tokio-postgres shape.
fn lone_set(c: &mut ExtSetCycle) {
    c.on_parse(false, true, false);
    c.on_bind(b"", Some(Some(SET.to_string())));
    c.on_execute(b"");
}

#[test]
fn a_lone_set_in_autocommit_is_tracked() {
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(
        c.at_boundary(ok_sync()),
        ExtSetDecision::Track(SET.to_string())
    );
    // The cycle ended: the next one starts clean.
    c.on_parse(false, false, false);
    c.on_bind(b"", Some(None));
    c.on_execute(b"");
    assert_eq!(c.at_boundary(ok_sync()), ExtSetDecision::Nothing);
}

#[test]
fn a_named_set_bound_again_later_is_tracked() {
    let mut c = ExtSetCycle::default();
    c.on_parse(true, true, false);
    assert_eq!(c.at_boundary(ok_sync()), ExtSetDecision::Nothing);
    c.on_bind(b"p", Some(Some(SET.to_string())));
    c.on_execute(b"p");
    assert_eq!(
        c.at_boundary(ok_sync()),
        ExtSetDecision::Track(SET.to_string())
    );
}

#[test]
fn a_failed_lone_set_changed_nothing() {
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(
        c.at_boundary(sync(true, true, true, true)),
        ExtSetDecision::Nothing
    );
}

#[test]
fn anything_but_a_lone_set_pins() {
    type Case = (&'static str, fn(&mut ExtSetCycle), ExtSetBoundary);
    let cases: [Case; 7] = [
        (
            "inside a transaction",
            lone_set,
            sync(true, false, false, false),
        ),
        (
            "opens a transaction",
            lone_set,
            sync(true, true, false, false),
        ),
        (
            "SET then another statement",
            |c| {
                lone_set(c);
                c.on_bind(b"", Some(None));
                c.on_execute(b"");
            },
            ok_sync(),
        ),
        (
            "another statement then SET",
            |c| {
                c.on_bind(b"", Some(None));
                c.on_execute(b"");
                c.on_parse(false, true, false);
                c.on_bind(b"", Some(Some(SET.to_string())));
                c.on_execute(b"");
            },
            ok_sync(),
        ),
        (
            "Execute of another portal",
            |c| {
                c.on_bind(b"a", Some(Some(SET.to_string())));
                c.on_execute(b"holdcur");
            },
            ok_sync(),
        ),
        (
            "SET bound twice",
            |c| {
                c.on_bind(b"", Some(Some(SET.to_string())));
                c.on_execute(b"");
                c.on_bind(b"", Some(Some(SET.to_string())));
                c.on_execute(b"");
            },
            ok_sync(),
        ),
        (
            "a statement the proxy has no record of",
            |c| {
                c.on_bind(b"", None);
                c.on_execute(b"");
            },
            ok_sync(),
        ),
    ];
    for (what, feed, boundary) in cases {
        let mut c = ExtSetCycle::default();
        feed(&mut c);
        assert_eq!(c.at_boundary(boundary), ExtSetDecision::Pin, "{what}");
    }
}

#[test]
fn a_flush_split_pins_at_the_flush() {
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true)), ExtSetDecision::Pin);
    // ...and stays pinned at the Sync, refused or not.
    assert_eq!(
        c.at_boundary(sync(false, true, true, false)),
        ExtSetDecision::Pin
    );
    // A Flush before the SET: the cycle is no longer lone either.
    let mut c = ExtSetCycle::default();
    c.on_bind(b"", Some(None));
    c.on_execute(b"");
    assert_eq!(c.at_boundary(flush(true)), ExtSetDecision::Nothing);
    lone_set(&mut c);
    assert_eq!(c.at_boundary(ok_sync()), ExtSetDecision::Pin);
}

#[test]
fn a_stale_statement_record_pins() {
    // A named SET-like Parse in a failed cycle (e.g. 42P05: the backend
    // keeps the old definition while the proxy recorded the new one).
    let mut c = ExtSetCycle::default();
    c.on_parse(true, true, false);
    assert_eq!(
        c.at_boundary(sync(true, true, true, true)),
        ExtSetDecision::Pin
    );
    // A named Parse replacing a SET-like statement, never forwarded.
    let mut c = ExtSetCycle::default();
    c.on_parse(true, false, true);
    assert_eq!(
        c.at_boundary(sync(false, true, true, false)),
        ExtSetDecision::Pin
    );
    // An unnamed SET-like Parse never forwarded (rate limit, open circuit).
    let mut c = ExtSetCycle::default();
    c.on_parse(false, true, false);
    assert_eq!(c.at_boundary(flush(false)), ExtSetDecision::Pin);
    // A failed unnamed Parse leaves no unnamed statement: nothing to pin.
    let mut c = ExtSetCycle::default();
    c.on_parse(false, true, false);
    assert_eq!(
        c.at_boundary(sync(true, true, true, true)),
        ExtSetDecision::Nothing
    );
    // A cycle that never reached a backend and touched no SET: nothing.
    let mut c = ExtSetCycle::default();
    c.on_bind(b"", Some(None));
    c.on_execute(b"");
    assert_eq!(
        c.at_boundary(sync(false, true, true, false)),
        ExtSetDecision::Nothing
    );
}

#[test]
fn set_like_statements_are_recognised_from_raw_bytes() {
    for (sql, set_like) in [
        (&b"SET search_path TO a"[..], true),
        (b"/* jdbc */ set application_name = 'x'", true),
        (b"-- c\rRESET ALL", true),
        (b"DISCARD ALL", true),
        (b"SELECT 1", false),
        (b"SETTINGS", false),
        (b"/* SET */ SELECT 1", false),
        (b"\xff\xfe SET a = 1", false),
    ] {
        assert_eq!(
            ProxyServer::tr_is_set_like_bytes(sql),
            set_like,
            "{:?}",
            String::from_utf8_lossy(sql)
        );
    }
    let parse = |name: &str, sql: &str| {
        let mut payload = name.as_bytes().to_vec();
        payload.push(0);
        payload.extend_from_slice(sql.as_bytes());
        payload.extend_from_slice(&[0, 0, 0]);
        payload
    };
    assert_eq!(
        ProxyServer::parse_payload_sql_bytes(&parse("s1", "SET a = 1")),
        Some(&b"SET a = 1"[..])
    );
    assert_eq!(
        ProxyServer::parse_payload_sql_bytes(&parse("", "SELECT 1")),
        Some(&b"SELECT 1"[..])
    );
    assert_eq!(ProxyServer::leading_cstring(b"portal\0\0\0\0\0"), b"portal");
    assert_eq!(ProxyServer::leading_cstring(b"\0rest"), b"");
}
