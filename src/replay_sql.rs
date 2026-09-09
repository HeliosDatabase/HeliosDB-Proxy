//! Allocation-free SQL boundary inspection for live replay. This is a lexical
//! guard, not a SQL validator or a side-effect/volatility classifier.
//!
//! Like the MCP splitter, it recognizes PostgreSQL nested comments and quoted
//! tokens. A backslash inside a single-quoted literal is session-dependent: it
//! escapes the next byte only when `standard_conforming_strings` is off (or the
//! literal carries an `E` prefix), and the proxy does not track that GUC. Rather
//! than refuse every such literal — which would strip replay eligibility from
//! ordinary regexes, Windows paths and JSON escapes — the scan runs under BOTH
//! interpretations and keeps a statement only when they AGREE that no commit
//! boundary exists. The two readings diverge exactly when a backslash precedes a
//! quote, which is the semicolon-hiding trick this guard exists to catch.

pub(crate) struct Boundaries<'a> {
    pub head: &'a str,
    pub may_commit: bool,
    /// A ROLLBACK or ABORT begins one of the statements. `may_commit` is a
    /// deliberately wide net (a ROLLBACK followed by DML commits that DML in
    /// autocommit), so callers that need "did this durably commit the
    /// transaction's own work" must exclude this case.
    pub ends_tx: bool,
}

fn ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

fn ident_cont(b: u8) -> bool {
    ident_start(b) || b.is_ascii_digit() || b == b'$'
}

/// Inspect the SQL under both backslash interpretations and combine them
/// conservatively: any reading that finds a commit boundary wins, and a reading
/// that cannot lex the input at all refuses. The second pass runs only when the
/// first met a backslash inside a single-quoted literal, so the common case
/// stays a single scan.
pub(crate) fn boundaries(sql: &str) -> Result<Boundaries<'_>, ()> {
    let plain = scan(sql, false)?;
    if !plain.saw_quoted_backslash {
        return Ok(Boundaries {
            head: plain.head,
            may_commit: plain.may_commit,
            ends_tx: plain.ends_tx,
        });
    }
    // Ambiguous: the same bytes are also a valid escaped-literal program.
    let escaped = scan(sql, true)?;
    Ok(Boundaries {
        head: plain.head,
        may_commit: plain.may_commit || escaped.may_commit,
        ends_tx: plain.ends_tx || escaped.ends_tx,
    })
}

struct Scan<'a> {
    head: &'a str,
    may_commit: bool,
    ends_tx: bool,
    saw_quoted_backslash: bool,
}

/// One lexical pass. `backslash_escapes` selects the session reading: when true
/// a backslash consumes the next byte inside a single-quoted literal.
fn scan(sql: &str, backslash_escapes: bool) -> Result<Scan<'_>, ()> {
    let b = sql.as_bytes();
    let mut saw_quoted_backslash = false;
    let mut i = 0;
    let mut head = None;
    let mut statements = 0usize;
    let mut at_start = true;
    let mut may_commit = false;
    let mut ends_tx = false;
    let mut first = "";
    let mut second = false;
    while i < b.len() {
        let start = i;
        match b[i] {
            b if b.is_ascii_whitespace() => {
                i += 1;
                continue;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                i += 2;
                while i < b.len() && !matches!(b[i], b'\n' | b'\r') {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1usize;
                while depth != 0 {
                    match b.get(i..i.saturating_add(2)) {
                        Some(b"/*") => {
                            depth += 1;
                            i += 2;
                        }
                        Some(b"*/") => {
                            depth -= 1;
                            i += 2;
                        }
                        Some(_) => i += 1,
                        None => return Err(()),
                    }
                }
                continue;
            }
            b';' => {
                at_start = true;
                i += 1;
                continue;
            }
            0 => return Err(()),
            _ => {}
        }
        if at_start {
            head.get_or_insert(start);
            statements += 1;
            first = "";
            second = false;
        }
        let mut word = "";
        if ident_start(b[i]) {
            i += 1;
            while i < b.len() && ident_cont(b[i]) {
                i += 1;
            }
            word = &sql[start..i];
        } else if matches!(b[i], b'\'' | b'"') {
            let quote = b[i];
            i += 1;
            loop {
                match b.get(i) {
                    None | Some(0) => return Err(()),
                    // Session-dependent: consume the escaped byte under the
                    // escaping reading, treat it as ordinary otherwise. Either
                    // way the caller re-scans and takes the union.
                    Some(b'\\') if quote == b'\'' => {
                        saw_quoted_backslash = true;
                        i += if backslash_escapes { 2 } else { 1 };
                    }
                    Some(c) if *c == quote => {
                        i += 1;
                        if b.get(i) == Some(&quote) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    Some(_) => i += 1,
                }
            }
        } else if b[i] == b'$' {
            let mut end = i + 1;
            if b.get(end).is_some_and(|c| ident_start(*c)) {
                end += 1;
                while b
                    .get(end)
                    .is_some_and(|c| ident_start(*c) || c.is_ascii_digit())
                {
                    end += 1;
                }
            }
            if b.get(end) == Some(&b'$') {
                let tag = &b[i..=end];
                let tail = &b[end + 1..];
                let close = memchr::memmem::find(tail, tag).ok_or(())?;
                i = end + 1 + close + tag.len();
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
        if at_start {
            first = word;
            if word.eq_ignore_ascii_case("COMMIT") || word.eq_ignore_ascii_case("END") {
                may_commit = true;
            }
            if word.eq_ignore_ascii_case("ROLLBACK") || word.eq_ignore_ascii_case("ABORT") {
                ends_tx = true;
            }
        } else if !second {
            if first.eq_ignore_ascii_case("PREPARE") && word.eq_ignore_ascii_case("TRANSACTION") {
                may_commit = true;
            }
            second = true;
        }
        at_start = false;
    }
    Ok(Scan {
        head: &sql[head.unwrap_or(sql.len())..],
        // ROLLBACK followed by DML can commit that DML in autocommit. It is
        // insufficient to classify the whole string as an uncommitted write.
        may_commit: may_commit || (ends_tx && statements > 1),
        ends_tx,
        saw_quoted_backslash,
    })
}

/// A word token outside quotes and comments, with whether the next
/// non-whitespace byte is `(` — i.e. whether it syntactically looks like a
/// function call. Quoted identifiers are reported with `quoted = true` so a
/// policy can refuse them rather than guess at case folding.
pub(crate) struct Word<'a> {
    pub text: &'a str,
    pub call: bool,
    pub quoted: bool,
}

/// Walk every word of `sql` outside string literals, quoted identifiers,
/// dollar quotes and comments, in order, under the non-escaping reading (the
/// caller has already established via [`boundaries`] that both readings agree
/// on statement structure, so word order is the same). Stops early when `f`
/// returns `false`. Allocation-free. Errors mirror [`boundaries`]: unterminated
/// tokens refuse.
pub(crate) fn words(sql: &str, mut f: impl FnMut(Word<'_>) -> bool) -> Result<(), ()> {
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let start = i;
        match b[i] {
            c if c.is_ascii_whitespace() => {
                i += 1;
                continue;
            }
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < b.len() && !matches!(b[i], b'\n' | b'\r') {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                let mut depth = 1usize;
                while depth != 0 {
                    match b.get(i..i.saturating_add(2)) {
                        Some(b"/*") => {
                            depth += 1;
                            i += 2;
                        }
                        Some(b"*/") => {
                            depth -= 1;
                            i += 2;
                        }
                        Some(_) => i += 1,
                        None => return Err(()),
                    }
                }
                continue;
            }
            0 => return Err(()),
            _ => {}
        }
        if ident_start(b[i]) {
            i += 1;
            while i < b.len() && ident_cont(b[i]) {
                i += 1;
            }
            let text = &sql[start..i];
            let mut j = i;
            while j < b.len() && b[j].is_ascii_whitespace() {
                j += 1;
            }
            let call = b.get(j) == Some(&b'(');
            if !f(Word {
                text,
                call,
                quoted: false,
            }) {
                return Ok(());
            }
        } else if b[i] == b'"' {
            // Quoted identifier: report it (opaque to case folding), skip it.
            i += 1;
            loop {
                match b.get(i) {
                    None | Some(0) => return Err(()),
                    Some(b'"') => {
                        i += 1;
                        if b.get(i) == Some(&b'"') {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    Some(_) => i += 1,
                }
            }
            let mut j = i;
            while j < b.len() && b[j].is_ascii_whitespace() {
                j += 1;
            }
            let call = b.get(j) == Some(&b'(');
            if !f(Word {
                text: &sql[start..i],
                call,
                quoted: true,
            }) {
                return Ok(());
            }
        } else if b[i] == b'\'' {
            i += 1;
            loop {
                match b.get(i) {
                    None | Some(0) => return Err(()),
                    Some(b'\'') => {
                        i += 1;
                        if b.get(i) == Some(&b'\'') {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    Some(_) => i += 1,
                }
            }
        } else if b[i] == b'$' {
            let mut end = i + 1;
            if b.get(end).is_some_and(|c| ident_start(*c)) {
                end += 1;
                while b
                    .get(end)
                    .is_some_and(|c| ident_start(*c) || c.is_ascii_digit())
                {
                    end += 1;
                }
            }
            if b.get(end) == Some(&b'$') {
                let tag = &b[i..=end];
                let tail = &b[end + 1..];
                let close = memchr::memmem::find(tail, tag).ok_or(())?;
                i = end + 1 + close + tag.len();
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_transaction_boundaries_outside_quotes() {
        for sql in [
            "/* outer /* nested */ end */ COMMIT",
            "-- comment\rEND WORK",
            "PREPARE/* comment */\nTRANSACTION 'x'",
            "SELECT 1; -- x\nCOMMIT PREPARED 'x'",
            "SELECT ';commit'; END",
            "SELECT $$;END$$; COMMIT",
            "SELECT $é$;END$é$; COMMIT",
            "SELECT 1 AS é$$; COMMIT",
            "ROLLBACK; INSERT INTO t VALUES (1)",
            "COMMIT AND CHAIN",
            "COMMIT; BEGIN",
            // Ambiguous backslash: the non-escaping reading exposes a COMMIT
            // that the escaping reading hides inside the literal. The union
            // must refuse, whichever reading the session actually uses.
            "SELECT 'a\\'; COMMIT; --'",
            "SELECT 'a\\'; END; --'",
        ] {
            assert!(boundaries(sql).unwrap().may_commit, "{sql}");
        }
        for sql in [
            "SELECT ';commit'",
            "SELECT $$;END$$",
            "SELECT $tag$;COMMIT$tag$",
            "SELECT 'it''s;COMMIT'",
            "SELECT \"a\"\";COMMIT\"",
            "SELECT CASE WHEN true THEN 1 END",
            "SELECT 1 /* COMMIT */",
            "PREPARE p AS SELECT 1",
            "BEGIN; INSERT INTO t VALUES (1)",
            // Ordinary backslashes: both readings agree there is no commit
            // boundary, so these keep their replay eligibility. Refusing them
            // outright stripped `select`/`transaction` mode from regexes,
            // Windows paths and JSON escapes.
            "SELECT id FROM t WHERE name ~ '^\\d+$'",
            "INSERT INTO t VALUES ('C:\\tmp\\x')",
            "UPDATE t SET j = '{\"a\": \"b\\nc\"}' WHERE id = 1",
            "SELECT 'back\\\\slash'",
        ] {
            let info = boundaries(sql).unwrap();
            assert!(!info.may_commit, "{sql}");
        }
        // `may_commit` is wide enough to cover autocommit exposure, but a
        // rolled-back transaction must be distinguishable: its session state
        // was discarded even though trailing DML committed.
        for sql in ["ROLLBACK", "ROLLBACK; INSERT INTO t VALUES (1)", "ABORT"] {
            assert!(boundaries(sql).unwrap().ends_tx, "{sql}");
        }
        for sql in [
            "COMMIT",
            "COMMIT; BEGIN",
            "END WORK",
            "INSERT INTO t VALUES (1)",
        ] {
            assert!(!boundaries(sql).unwrap().ends_tx, "{sql}");
        }
        assert_eq!(boundaries("; /* empty */ ;").unwrap().head, "");
        assert!(!boundaries("SELECT ';'; -- done").unwrap().may_commit);
    }

    #[test]
    fn refuses_unterminated_or_session_dependent_quoting() {
        for sql in [
            "SELECT '",
            "SELECT \"",
            "SELECT $$",
            "/*",
            "SELECT E'a\\'b'",
            "SELECT '\0'",
        ] {
            assert!(boundaries(sql).is_err(), "{sql:?}");
        }
        let nested = format!("{}{} COMMIT", "/*".repeat(4096), "*/".repeat(4096));
        assert!(boundaries(&nested).unwrap().may_commit);
    }

    #[test]
    fn words_reports_calls_outside_quotes_and_comments() {
        let mut seen = Vec::new();
        words(
            "SELECT count(*), lower (name), 'f(x)' /* g( */ -- h(\n, \"Quoted\"(1), t.col FROM t",
            |w| {
                seen.push((w.text.to_string(), w.call, w.quoted));
                true
            },
        )
        .unwrap();
        assert_eq!(
            seen,
            vec![
                ("SELECT".to_string(), false, false),
                ("count".to_string(), true, false),
                ("lower".to_string(), true, false),
                ("name".to_string(), false, false),
                ("\"Quoted\"".to_string(), true, true),
                ("t".to_string(), false, false),
                ("col".to_string(), false, false),
                ("FROM".to_string(), false, false),
                ("t".to_string(), false, false),
            ]
        );
        // Dollar-quoted bodies and escaped quotes hide nothing that looks
        // like a call.
        let mut calls = Vec::new();
        words("SELECT $$nextval('s')$$, 'it''s(', $q$ f( $q$", |w| {
            if w.call {
                calls.push(w.text.to_string());
            }
            true
        })
        .unwrap();
        assert!(calls.is_empty(), "{calls:?}");
        // Early stop.
        let mut n = 0;
        words("a b c d", |_| {
            n += 1;
            n < 2
        })
        .unwrap();
        assert_eq!(n, 2);
        assert!(words("SELECT 'unterminated", |_| true).is_err());
    }
}
