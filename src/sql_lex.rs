//! The quote-aware SQL lexer core, shared by every feature that reads SQL
//! text (`sql_text` holds the cache scanners built on it).
//!
//! [`Pieces`] reads the text once into [`Piece`]s. The routing hint parser
//! ([`comments`]), the multi-tenancy identifier scan ([`code_identifiers`],
//! [`lex_units`], [`truncated_identifier`]) and the result caches' statement
//! and fingerprint passes all use it. Each item carries the feature set that
//! uses it, so a single-feature build compiles only the lexer it reads. The
//! rules are documented on `sql_text`.

use crate::replay_sql::{ident_cont, ident_start};
use memchr::memmem::Finder;
use once_cell::sync::Lazy;
#[cfg(any(
    feature = "multi-tenancy",
    feature = "query-cache",
    feature = "edge-proxy"
))]
use std::borrow::Cow;
use std::fmt;

/// One lexical piece of SQL text.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Piece<'a> {
    /// A run of whitespace.
    Space(&'a str),
    /// A `--` or `/* */` comment.
    Comment(&'a str),
    /// A word, an operator or punctuation character, `..`, a `$n`
    /// parameter, or a number that runs into an identifier: case-insensitive
    /// text.
    Text(&'a str),
    /// A quoted identifier `"…"`, quotes included.
    Ident(&'a str),
    /// A string literal `'…'`, quotes included.
    Str(&'a str),
    /// A dollar-quoted literal with its delimiters, and the length of one
    /// delimiter (`$tag$`).
    Dollar(&'a str, usize),
    /// A numeric literal.
    Number(&'a str),
    /// The rest of the text, from a point where the backends or the
    /// server's `standard_conforming_strings` setting may read it
    /// differently, or from an unterminated token: backend-ambiguous text,
    /// always the last piece.
    Rest(&'a str),
}

/// Written by hand, not derived: a derived `Debug` is ignored by the dead-code
/// analysis, so a feature build that reads no variant field (routing hints
/// read only comments) would warn about every field it never reads.
impl fmt::Debug for Piece<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Piece::Space(t) => f.debug_tuple("Space").field(&t).finish(),
            Piece::Comment(t) => f.debug_tuple("Comment").field(&t).finish(),
            Piece::Text(t) => f.debug_tuple("Text").field(&t).finish(),
            Piece::Ident(t) => f.debug_tuple("Ident").field(&t).finish(),
            Piece::Str(t) => f.debug_tuple("Str").field(&t).finish(),
            Piece::Dollar(t, n) => f.debug_tuple("Dollar").field(&t).field(&n).finish(),
            Piece::Number(t) => f.debug_tuple("Number").field(&t).finish(),
            Piece::Rest(t) => f.debug_tuple("Rest").field(&t).finish(),
        }
    }
}

/// The pieces of `sql` in order, each with its byte offset.
pub(crate) struct Pieces<'a> {
    sql: &'a str,
    i: usize,
    /// The offset of the last `;` in `sql` without its trailing whitespace
    /// and one final `;` (and the whitespace before it), `Some(None)` when
    /// there is none: found once, when a comment first needs it
    /// ([`Self::comment`]), so each comment then costs only its own length.
    last_semicolon: Option<Option<usize>>,
}

impl<'a> Pieces<'a> {
    pub(crate) fn new(sql: &'a str) -> Self {
        Self {
            sql,
            i: 0,
            last_semicolon: None,
        }
    }

    /// The comment from `start` to `end` as a [`Piece::Comment`], or the rest
    /// of the text when HeliosDB-Lite may end a statement inside it or after
    /// it where the other backends do not. Lite's simple-query splitter knows
    /// no comments: it ends a statement at a `;` inside one, and a quote one
    /// leaves open moves its later boundaries (`SELECT 1 -- it's` newline
    /// `, 'a; DELETE FROM b; --'` runs the DELETE on Lite)
    /// ([`lite_reads_into`]). Such a comment is ambiguous only when a `;`
    /// other than the text's final one lies at or after its start: Lite
    /// splits nowhere else, so `… /* it's */` at the end is still a comment.
    pub(crate) fn comment(&mut self, start: usize, end: usize) -> (Piece<'a>, usize) {
        let sql = self.sql;
        if lite_reads_into(&sql.as_bytes()[start..end]) {
            let last = *self.last_semicolon.get_or_insert_with(|| {
                let t = sql.trim_end();
                memchr::memrchr(
                    b';',
                    t.strip_suffix(';').map_or(t, str::trim_end).as_bytes(),
                )
            });
            if last.is_some_and(|at| at >= start) {
                return (Piece::Rest(&sql[start..]), sql.len());
            }
        }
        (Piece::Comment(&sql[start..end]), end)
    }
}

/// Whether HeliosDB-Lite's simple-query splitter, which reads the comment
/// `body` as SQL, may end a statement inside it or leave it inside a
/// literal: a `;` or a `$` in it (its dollar quotes are not followed here),
/// or a quote it leaves open (a `'` or `"` opens a literal that only the
/// same quote closes: `it's`, `' " ' "`). The splitter enters a comment
/// outside any literal, as the lexer does, so a comment whose quotes all
/// close (`/*controller='users',action='index'*/`) leaves it there too, and
/// it splits the text after it where PostgreSQL does. One pass over the
/// body's quotes.
pub(crate) fn lite_reads_into(body: &[u8]) -> bool {
    if memchr::memchr2(b';', b'$', body).is_some() {
        return true;
    }
    let mut open = None;
    for at in memchr::memchr2_iter(b'\'', b'"', body) {
        let quote = body[at];
        match open {
            None => open = Some(quote),
            Some(q) if q == quote => open = None,
            Some(_) => {}
        }
    }
    open.is_some()
}

/// Whether `c` is whitespace: PostgreSQL's (16 and later) `space`, which is
/// also what HeliosDB-Nano's tokenizer skips (`\v` included, which
/// `u8::is_ascii_whitespace` is not).
pub(crate) fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C)
}

/// Whether a dollar-quoted literal's `body` holds a byte HeliosDB-Nano's
/// comment stripper, which knows no dollar quotes, reads as SQL: the start of
/// a comment, or a quote (which flips its idea of what is quoted).
pub(crate) fn nano_reads_into(body: &[u8]) -> bool {
    memchr::memchr2(b'\'', b'"', body).is_some() || holds_comment_start(body)
}

/// The searchers for the comment markers, each built once.
/// `memchr::memmem::find` builds its searcher for every call, which costs
/// more than the search of a statement-sized text: in a sampled profile
/// building them was 84 ns of a 2.6 us L2 cache hit and, for the hint
/// marker's alone, 34 ns of a 480 ns L1 hit (gpc001ca, 2026-11-16).
static LINE_COMMENT_START: Lazy<Finder<'static>> = Lazy::new(|| Finder::new(b"--"));
static BLOCK_COMMENT_START: Lazy<Finder<'static>> = Lazy::new(|| Finder::new(b"/*"));
static BLOCK_COMMENT_END: Lazy<Finder<'static>> = Lazy::new(|| Finder::new(b"*/"));

/// Whether `b` holds `--` or `/*` anywhere.
fn holds_comment_start(b: &[u8]) -> bool {
    LINE_COMMENT_START.find(b).is_some() || BLOCK_COMMENT_START.find(b).is_some()
}

impl<'a> Iterator for Pieces<'a> {
    type Item = (usize, Piece<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let sql = self.sql;
        let b = sql.as_bytes();
        let start = self.i;
        let c = *b.get(start)?;
        let rest = (Piece::Rest(&sql[start..]), b.len());
        let (piece, end) = match c {
            c if is_space(c) => {
                let end = run(b, start + 1, is_space);
                (Piece::Space(&sql[start..end]), end)
            }
            b'-' if b.get(start + 1) == Some(&b'-') => {
                let end = run(b, start + 2, |c| c != b'\n' && c != b'\r');
                // A `\r` alone ends the comment for PostgreSQL only.
                if b.get(end) == Some(&b'\r') && b.get(end + 1) != Some(&b'\n') {
                    rest
                } else {
                    self.comment(start, end)
                }
            }
            b'/' if b.get(start + 1) == Some(&b'*') => match block_comment_end(b, start) {
                Some(end) => self.comment(start, end),
                None => rest,
            },
            b'\'' => match string_end(b, start) {
                Some(end) => (Piece::Str(&sql[start..end]), end),
                None => rest,
            },
            b'"' => match quoted_ident_end(b, start) {
                Some(end) => (Piece::Ident(&sql[start..end]), end),
                None => rest,
            },
            b'$' => match dollar_tag_len(b, start) {
                // HeliosDB-Nano's statement splitter knows only ASCII tags:
                // `$é$ ; TRUNCATE kv ; $é$` is three statements there.
                Some(tag) if b[start..start + tag].is_ascii() => {
                    match memchr::memmem::find(&b[start + tag..], &b[start..start + tag]) {
                        Some(at) if !nano_reads_into(&b[start + tag..start + tag + at]) => {
                            let end = start + tag + at + tag;
                            (Piece::Dollar(&sql[start..end], tag), end)
                        }
                        _ => rest,
                    }
                }
                Some(_) => rest,
                // A tag PostgreSQL does not take for one, HeliosDB-Nano's and
                // HeliosDB-Lite's splitters do (`$1$`, a tag starting with a
                // digit).
                None if opens_dollar_quote_elsewhere(b, start) => rest,
                // `$n` (a parameter reference) or a lone `$`.
                None => {
                    let end = run(b, start + 1, |c| c.is_ascii_digit());
                    (Piece::Text(&sql[start..end]), end)
                }
            },
            // `..` is one token (`1..5` is `1`, `..`, `5`), so the second
            // dot never starts a number.
            b'.' if b.get(start + 1) == Some(&b'.') => {
                (Piece::Text(&sql[start..start + 2]), start + 2)
            }
            b'.' if b.get(start + 1).is_some_and(u8::is_ascii_digit) => number(sql, start),
            c if c.is_ascii_digit() => number(sql, start),
            c if ident_start(c) => word(sql, start, run(b, start + 1, ident_cont)),
            // Any other ASCII byte (bytes >= 0x80 start a word above).
            _ => (Piece::Text(&sql[start..start + 1]), start + 1),
        };
        self.i = end;
        Some((start, piece))
    }
}

/// The end of the run of bytes from `i` that satisfy `keep`.
pub(crate) fn run(b: &[u8], mut i: usize, keep: impl Fn(u8) -> bool) -> usize {
    while i < b.len() && keep(b[i]) {
        i += 1;
    }
    i
}

/// The end of the block comment at `start`: its first `*/`. `None` when it
/// is not closed or a `/*` comes before that `*/` (PostgreSQL nests block
/// comments, HeliosDB-Nano ends one at its first `*/`).
pub(crate) fn block_comment_end(b: &[u8], start: usize) -> Option<usize> {
    let body = &b[start + 2..];
    let close = BLOCK_COMMENT_END.find(body)?;
    BLOCK_COMMENT_START
        .find(&body[..=close])
        .is_none()
        .then_some(start + 2 + close + 2)
}

/// The end of the `'…'` literal at `start`, or `None` when it is not closed
/// or a backslash precedes one of its quotes (`standard_conforming_strings`
/// on or off, which the proxy does not track, end it in different places).
pub(crate) fn string_end(b: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    loop {
        match *b.get(i)? {
            b'\\' if b.get(i + 1) == Some(&b'\'') => return None,
            b'\'' if b.get(i + 1) == Some(&b'\'') => i += 2,
            b'\'' => return Some(i + 1),
            _ => i += 1,
        }
    }
}

/// The end of the `"…"` identifier at `start`, or `None` when it is not
/// closed.
pub(crate) fn quoted_ident_end(b: &[u8], start: usize) -> Option<usize> {
    let mut i = start + 1;
    loop {
        match *b.get(i)? {
            b'"' if b.get(i + 1) == Some(&b'"') => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
}

/// The length of the `$tag$` delimiter at `start` (`$$` included), or `None`
/// when the `$` does not open a dollar quote.
pub(crate) fn dollar_tag_len(b: &[u8], start: usize) -> Option<usize> {
    let mut end = start + 1;
    if b.get(end).is_some_and(|&c| ident_start(c)) {
        end = run(b, end + 1, |c| ident_start(c) || c.is_ascii_digit());
    }
    (b.get(end) == Some(&b'$')).then_some(end + 1 - start)
}

/// Whether HeliosDB-Nano's or HeliosDB-Lite's simple-query splitter may open
/// a dollar quote at the `$` at `at`: both do at any `$` outside quotes and
/// comments that tag characters (ASCII letters and digits and `_` for Nano,
/// Unicode ones for Lite; here every byte that continues a name but `$`) and
/// another `$` follow, a `$` inside a name and a tag starting with a digit
/// included, where PostgreSQL reads a name or a parameter.
pub(crate) fn opens_dollar_quote_elsewhere(b: &[u8], at: usize) -> bool {
    b.get(run(b, at + 1, |c| c != b'$' && ident_cont(c))) == Some(&b'$')
}

/// The word from `start` to `end` (a run of [`ident_cont`] bytes) as a
/// [`Piece::Text`], or the rest of the text when it holds a `$` that
/// HeliosDB-Nano and HeliosDB-Lite open a dollar quote at: two `$` in one
/// name (`a$$`, `a$b$`), which PostgreSQL reads as part of it
/// (`SELECT 1 AS a$$ -- $$; DELETE FROM b` runs the DELETE on Nano).
pub(crate) fn word(sql: &str, start: usize, end: usize) -> (Piece<'_>, usize) {
    let w = &sql.as_bytes()[start..end];
    if memchr::memchr(b'$', w).is_some_and(|first| memchr::memchr(b'$', &w[first + 1..]).is_some())
    {
        (Piece::Rest(&sql[start..]), sql.len())
    } else {
        (Piece::Text(&sql[start..end]), end)
    }
}

/// The number at `start` (a digit, or a `.` before one) as a
/// [`Piece::Number`]. One running into an identifier (`1x`, `0x1F`, `1.e`)
/// is not a literal of its own: a [`Piece::Text`] up to the end of that
/// identifier ([`word`]).
pub(crate) fn number(sql: &str, start: usize) -> (Piece<'_>, usize) {
    let b = sql.as_bytes();
    let end = number_end(b, start);
    if b.get(end).is_some_and(|&c| ident_cont(c)) {
        word(sql, start, run(b, end, ident_cont))
    } else {
        (Piece::Number(&sql[start..end]), end)
    }
}

/// The end of the numeric literal at `start`, as PostgreSQL's lexer reads
/// one: a `{decinteger}`, then a `.` with an optional fraction, or a `.`
/// and a fraction alone (`.5`), then an optional exponent. `1.` and `1.e3`
/// are one literal each (`1. e3` is `1.` labelled `e3`); a `.` followed by
/// another (`1..`) is not part of the number.
pub(crate) fn number_end(b: &[u8], start: usize) -> usize {
    let mut i = decinteger_end(b, start);
    if b.get(i) == Some(&b'.') && b.get(i + 1) != Some(&b'.') {
        i = decinteger_end(b, i + 1);
    }
    if matches!(b.get(i), Some(b'e' | b'E')) {
        let digits = i + 1 + usize::from(matches!(b.get(i + 1), Some(b'+' | b'-')));
        let end = decinteger_end(b, digits);
        if end > digits {
            i = end;
        }
    }
    i
}

/// The end of the `{decinteger}` at `i`: digits, with a single `_` between
/// two of them (`1_000`). `i` itself when no digit starts there.
pub(crate) fn decinteger_end(b: &[u8], mut i: usize) -> usize {
    while b.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        if b.get(i) == Some(&b'_') && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
    }
    i
}

/// `sql` with every comment (outside quotes) replaced by one space, and
/// everything else, literals and a backend-ambiguous rest included,
/// unchanged: the text the table extraction reads (a comment can neither
/// hide a table name nor glue one to its keyword). Borrowed when there is no
/// comment, without a scan when the text contains neither `--` nor `/*`.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) fn strip_comments(sql: &str) -> Cow<'_, str> {
    let mut stripped = Stripped::default();
    if has_comment_marker(sql.as_bytes()) {
        for (start, piece) in Pieces::new(sql) {
            if let Piece::Comment(comment) = piece {
                stripped.comment(sql, start, comment.len());
            }
        }
    }
    stripped.finish(sql)
}

/// [`strip_comments`], built one comment at a time.
#[derive(Default)]
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) struct Stripped {
    out: Option<String>,
    copied: usize,
}

#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
impl Stripped {
    /// Replace the comment of `len` bytes at `start` with one space.
    pub(crate) fn comment(&mut self, sql: &str, start: usize, len: usize) {
        let out = self
            .out
            .get_or_insert_with(|| String::with_capacity(sql.len()));
        out.push_str(&sql[self.copied..start]);
        out.push(' ');
        self.copied = start + len;
    }

    pub(crate) fn finish(self, sql: &str) -> Cow<'_, str> {
        match self.out {
            None => Cow::Borrowed(sql),
            Some(mut out) => {
                out.push_str(&sql[self.copied..]);
                Cow::Owned(out)
            }
        }
    }
}

/// Whether `b` holds the start of a comment, `--` or `/*`, anywhere (in a
/// literal too): without one the lexer finds no comment.
#[cfg(any(
    feature = "query-cache",
    feature = "edge-proxy",
    feature = "wasm-plugins"
))]
pub(crate) fn has_comment_marker(b: &[u8]) -> bool {
    holds_comment_start(b)
}

/// `sql` from its first token that is neither whitespace nor a comment, or
/// `None` when nothing else is left or that token is a [`Piece::Rest`]: the
/// backends may disagree on where its leading comments end (a block comment
/// holding `/*`, which PostgreSQL nests and HeliosDB-Nano ends at its first
/// `*/`; a `--` comment a lone `\r` ends for PostgreSQL only) or one is
/// unterminated. What a statement is (DDL, a write, a read) is decided on
/// its first word, which is then unknown. A Unicode space (U+00A0, U+3000,
/// …) is skipped as whitespace too: HeliosDB-Nano and HeliosDB-Lite read it
/// so and run the statement after it (`\u{a0}CREATE OR REPLACE VIEW …`),
/// while PostgreSQL reads it as part of an identifier and fails the
/// statement.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) fn statement_start(sql: &str) -> Option<&str> {
    match lead(sql) {
        Lead::Start(start) => Some(start),
        Lead::Rest | Lead::Nothing => None,
    }
}

/// What comes first in a statement past its leading whitespace and
/// comments ([`lead`]).
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) enum Lead<'a> {
    /// The statement from its first word.
    Start(&'a str),
    /// A backend-ambiguous rest.
    Rest,
    /// Nothing but whitespace and comments.
    Nothing,
}

/// What `sql` starts with past its leading whitespace and comments, in one
/// pass of the lexer up to its first word: [`statement_start`] and
/// [`leading_rest`] at once.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) fn lead(sql: &str) -> Lead<'_> {
    for (start, piece) in Pieces::new(sql) {
        match piece {
            Piece::Space(_) | Piece::Comment(_) => {}
            Piece::Rest(_) => return Lead::Rest,
            Piece::Text(text) => {
                let word = text.trim_start_matches(char::is_whitespace);
                if !word.is_empty() {
                    return Lead::Start(&sql[start + text.len() - word.len()..]);
                }
            }
            _ => return Lead::Start(&sql[start..]),
        }
    }
    Lead::Nothing
}

/// What an `EXPLAIN` prefix makes of the text it starts ([`explain`]).
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Explain<'a> {
    /// The text does not start with EXPLAIN: it is a statement of its own.
    NoPrefix,
    /// `EXPLAIN` without ANALYZE: it executes nothing.
    Plan,
    /// `EXPLAIN ... ANALYZE`: it executes the statement from this point on.
    Analyze(&'a str),
    /// The options or the explained statement cannot be read (an unterminated
    /// option list, a backend-ambiguous rest, nothing after ANALYZE).
    Unknown,
}

/// Read the `EXPLAIN` prefix of `sql`, quote- and comment-aware through the
/// lexer. Forms: `EXPLAIN [ANALYZE|ANALYSE] [VERBOSE] stmt` and
/// `EXPLAIN ( option [, ...] ) stmt`. In the option list `ANALYZE` turns
/// analysis on, unless a following `false`, `off` or `0` turns it off.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) fn explain(sql: &str) -> Explain<'_> {
    let mut pieces = Pieces::new(sql)
        .filter(|(_, p)| !matches!(p, Piece::Space(_) | Piece::Comment(_)))
        .peekable();
    match pieces.next() {
        Some((_, Piece::Text(w))) if w.eq_ignore_ascii_case("EXPLAIN") => {}
        _ => return Explain::NoPrefix,
    }
    let mut analyze = false;
    if matches!(pieces.peek(), Some((_, Piece::Text("(")))) {
        pieces.next();
        loop {
            match pieces.next() {
                None | Some((_, Piece::Rest(_))) => return Explain::Unknown,
                Some((_, Piece::Text(")"))) => break,
                Some((_, Piece::Text(w)))
                    if w.eq_ignore_ascii_case("ANALYZE") || w.eq_ignore_ascii_case("ANALYSE") =>
                {
                    let on = match pieces.peek() {
                        Some((_, Piece::Text(v)))
                            if v.eq_ignore_ascii_case("false") || v.eq_ignore_ascii_case("off") =>
                        {
                            pieces.next();
                            false
                        }
                        Some((_, Piece::Text(v)))
                            if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on") =>
                        {
                            pieces.next();
                            true
                        }
                        Some((_, Piece::Number(n))) => {
                            let on = *n != "0";
                            pieces.next();
                            on
                        }
                        _ => true,
                    };
                    analyze |= on;
                }
                Some(_) => {}
            }
        }
    } else {
        while let Some((_, Piece::Text(w))) = pieces.peek() {
            if w.eq_ignore_ascii_case("ANALYZE") || w.eq_ignore_ascii_case("ANALYSE") {
                analyze = true;
            } else if !w.eq_ignore_ascii_case("VERBOSE") {
                break;
            }
            pieces.next();
        }
    }
    match pieces.peek() {
        Some((_, Piece::Rest(_))) => Explain::Unknown,
        Some(&(at, _)) if analyze => Explain::Analyze(&sql[at..]),
        Some(_) => Explain::Plan,
        None if analyze => Explain::Unknown,
        None => Explain::Plan,
    }
}

/// The statement `sql` runs once its `EXPLAIN ... ANALYZE` prefixes are
/// peeled: `sql` itself unless it is an analysed EXPLAIN. `None` when that
/// statement cannot be read (fail closed: the caller treats it as unknown).
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
pub(crate) fn explain_innermost(sql: &str) -> Option<&str> {
    let mut text = sql;
    loop {
        match explain(text) {
            Explain::NoPrefix | Explain::Plan => return Some(text),
            Explain::Analyze(inner) => text = inner,
            Explain::Unknown => return None,
        }
    }
}

/// The kind of one lexical unit of statement text ([`lex_units`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(feature = "multi-tenancy")]
pub(crate) enum LexKind {
    /// An unquoted word (keyword or identifier), as written.
    Word,
    /// A quoted identifier `"…"`, quotes included.
    Ident,
    /// Punctuation: `(`, `)`, `,`, `;` or `.`.
    Punct,
    /// An operator run, `::`, `..` or a `$n` parameter.
    Op,
    /// A string, dollar-quoted or numeric literal.
    Literal,
    /// A `--` or `/* */` comment.
    Comment,
}

/// One lexical unit of statement text, by byte range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(feature = "multi-tenancy")]
pub(crate) struct LexUnit {
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) kind: LexKind,
}

/// The lexical units of `sql` in order, whitespace dropped, read by the same
/// lexer as the result caches. `None` when the lexer reaches a
/// backend-ambiguous rest ([`Piece::Rest`]) or meets a non-ASCII byte outside
/// a literal, quoted identifier or comment (HeliosDB-Lite matches keywords
/// by their Unicode upper-case form, so no word is certain): a caller must
/// then fail closed.
#[cfg(feature = "multi-tenancy")]
pub(crate) fn lex_units(sql: &str) -> Option<Vec<LexUnit>> {
    let mut out = Vec::new();
    for (start, piece) in Pieces::new(sql) {
        let (len, kind) = match piece {
            Piece::Space(_) => continue,
            Piece::Rest(_) => return None,
            Piece::Comment(t) => (t.len(), LexKind::Comment),
            Piece::Ident(t) => (t.len(), LexKind::Ident),
            Piece::Str(t) | Piece::Dollar(t, _) | Piece::Number(t) => (t.len(), LexKind::Literal),
            Piece::Text(t) => {
                let c = t.as_bytes()[0];
                let kind = if !t.is_ascii() {
                    return None;
                } else if ident_start(c) {
                    LexKind::Word
                } else if matches!(t, "(" | ")" | "," | ";" | ".") {
                    LexKind::Punct
                } else if c.is_ascii_digit() || (c == b'.' && t.len() > 1) {
                    LexKind::Literal
                } else {
                    LexKind::Op
                };
                (t.len(), kind)
            }
        };
        out.push(LexUnit {
            start,
            end: start + len,
            kind,
        });
    }
    Some(out)
}

/// Every comment outside quotes that starts at or before byte `last`, in
/// order, with its byte offset and its text including the delimiters
/// (`/*…*/`, or `--` to the end of the line). Reading stops at the
/// backend-ambiguous rest ([`Piece::Rest`]): no comment after it is yielded.
/// The lexer reads nothing past the first piece that starts after `last`.
/// Used by the routing hint parser, which passes the offset of the last
/// `helios:` in the text: no comment after it holds a hint.
#[cfg(any(feature = "routing-hints", test))]
pub(crate) fn comments(sql: &str, last: usize) -> impl Iterator<Item = (usize, &str)> {
    Pieces::new(sql)
        .take_while(move |&(at, _)| at <= last)
        .filter_map(|(at, piece)| match piece {
            Piece::Comment(comment) => Some((at, comment)),
            _ => None,
        })
}

/// Whether `sql` holds no `'`, `"` or `$`: no string literal (`'…'`, `E'…'`,
/// `U&'…'`), quoted identifier (`"…"`, `U&"…"`), dollar quote or `$n`
/// parameter can start in it, so [`quote_free_comments`] reads its comments
/// as [`comments`] does. One vectorised byte scan.
#[cfg(any(feature = "routing-hints", test))]
pub(crate) fn quote_free(sql: &str) -> bool {
    memchr::memchr3(b'\'', b'"', b'$', sql.as_bytes()).is_none()
}

/// [`comments`] for text that is [`quote_free`], without the lexer: it
/// jumps from one `-` or `/` to the next. Without a quote or a `$`, no piece
/// but a comment holds a `--` or a `/*` (a word, a number or a punctuation
/// byte cannot; the `-` of an exponent, `1e-5`, is followed by a digit), the
/// lexer reaches a backend-ambiguous rest only at a comment, and each
/// comment starts at the first `--` or `/*` after the previous one ends. The
/// comment rules are the lexer's: a `--` comment ends at the first CR or LF,
/// and one a lone CR ends is a rest; a block comment ends at its first `*/`,
/// and one without it or holding a `/*` before it is a rest; HeliosDB-Lite's
/// statement splitter makes a comment a rest the same way
/// ([`Pieces::comment`]). For text with a quote or a `$` this is not the
/// lexer's answer: callers check [`quote_free`] first.
#[cfg(any(feature = "routing-hints", test))]
pub(crate) fn quote_free_comments(sql: &str, last: usize) -> QuoteFreeComments<'_> {
    QuoteFreeComments {
        pieces: Pieces::new(sql),
        at: 0,
        end: last.saturating_add(1).min(sql.len()),
    }
}

/// The iterator of [`quote_free_comments`].
#[cfg(any(feature = "routing-hints", test))]
pub(crate) struct QuoteFreeComments<'a> {
    /// The text, and the last `;` the HeliosDB-Lite rule of its comments
    /// reads, found once when first needed ([`Pieces::comment`]).
    pieces: Pieces<'a>,
    /// Where the search for the next comment starts.
    at: usize,
    /// One past the last offset a yielded comment may start at.
    end: usize,
}

#[cfg(any(feature = "routing-hints", test))]
impl<'a> Iterator for QuoteFreeComments<'a> {
    type Item = (usize, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        let sql = self.pieces.sql;
        let b = sql.as_bytes();
        loop {
            let start = self.at + memchr::memchr2(b'-', b'/', b.get(self.at..self.end)?)?;
            let end = match (b[start], b.get(start + 1)) {
                (b'-', Some(&b'-')) => {
                    let end = run(b, start + 2, |c| c != b'\n' && c != b'\r');
                    // A `\r` alone ends the comment for PostgreSQL only.
                    (b.get(end) != Some(&b'\r') || b.get(end + 1) == Some(&b'\n')).then_some(end)
                }
                (b'/', Some(&b'*')) => short_block_comment_end(b, start),
                _ => {
                    self.at = start + 1;
                    continue;
                }
            };
            // Nothing is read past a rest: the search range is left empty.
            self.at = self.end;
            return match self.pieces.comment(start, end?) {
                (Piece::Comment(comment), end) => {
                    self.at = end.min(self.end);
                    Some((start, comment))
                }
                _ => None,
            };
        }
    }
}

/// [`block_comment_end`] with byte searches instead of a substring search
/// built for each call: the first `*` followed by `/`, then a `/` followed
/// by `*` before it (the `*` may be the closing one's). The same answer for
/// every text and start; cheaper for the short comments a hint is.
#[cfg(any(feature = "routing-hints", test))]
fn short_block_comment_end(b: &[u8], start: usize) -> Option<usize> {
    let body = &b[start + 2..];
    let mut at = 0;
    let close = loop {
        let star = at + memchr::memchr(b'*', &body[at..])?;
        if body.get(star + 1) == Some(&b'/') {
            break star;
        }
        at = star + 1;
    };
    let mut at = 0;
    while let Some(slash) = memchr::memchr(b'/', &body[at..close]) {
        if body[at + slash + 1] == b'*' {
            return None;
        }
        at += slash + 1;
    }
    Some(start + 2 + close + 2)
}

/// The longest identifier PostgreSQL keeps (`NAMEDATALEN - 1` bytes): it
/// truncates a longer one to this length, clipped back to a character
/// boundary.
#[cfg(any(
    feature = "multi-tenancy",
    feature = "query-cache",
    feature = "edge-proxy"
))]
pub(crate) const MAX_IDENTIFIER_BYTES: usize = 63;

/// `name` as PostgreSQL stores it: cut to [`MAX_IDENTIFIER_BYTES`], clipped
/// back to a character boundary. The table extraction keys a table this
/// way, so a read that spells a longer name and a write (or a decoded
/// change, which names the catalog's truncated relation) name one table.
#[cfg(any(
    feature = "multi-tenancy",
    feature = "query-cache",
    feature = "edge-proxy"
))]
pub(crate) fn truncated_identifier(name: &str) -> &str {
    if name.len() <= MAX_IDENTIFIER_BYTES {
        return name;
    }
    let mut end = MAX_IDENTIFIER_BYTES;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}

/// The identifiers of `sql`'s code, in text order: each unquoted word of the
/// text pieces, ASCII-lower-cased, and each quoted identifier unquoted (with
/// `""` unescaped, lower-cased too). String literals, dollar-quoted bodies,
/// comments and numbers yield nothing; a qualified name yields each of its
/// parts. The rest of the text after a piece the lexer cannot read
/// ([`Piece::Rest`], an unterminated token) is code, so it yields its words:
/// fail-closed.
#[cfg(any(
    feature = "multi-tenancy",
    all(test, any(feature = "query-cache", feature = "edge-proxy"))
))]
pub(crate) fn code_identifiers(sql: &str) -> impl Iterator<Item = Cow<'_, str>> + '_ {
    Pieces::new(sql).flat_map(|(_, piece)| -> Vec<Cow<'_, str>> {
        match piece {
            Piece::Text(t) | Piece::Rest(t) => text_words(t).collect(),
            Piece::Ident(t) => {
                let inner = &t[1..t.len() - 1];
                let unescaped = if inner.contains("\"\"") {
                    Cow::Owned(inner.replace("\"\"", "\""))
                } else {
                    Cow::Borrowed(inner)
                };
                vec![lower(unescaped)]
            }
            Piece::Space(_)
            | Piece::Comment(_)
            | Piece::Str(_)
            | Piece::Dollar(..)
            | Piece::Number(_) => Vec::new(),
        }
    })
}

/// The words of a text piece: runs of letters, digits, `_` and `$`.
#[cfg(any(
    feature = "multi-tenancy",
    all(test, any(feature = "query-cache", feature = "edge-proxy"))
))]
pub(crate) fn text_words(text: &str) -> impl Iterator<Item = Cow<'_, str>> {
    text.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .filter(|w| !w.is_empty())
        .map(|w| lower(Cow::Borrowed(w)))
}

/// `text` ASCII-lower-cased, borrowed when it has no upper-case letter.
#[cfg(any(
    feature = "multi-tenancy",
    all(test, any(feature = "query-cache", feature = "edge-proxy"))
))]
pub(crate) fn lower(text: Cow<'_, str>) -> Cow<'_, str> {
    if text.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(text.to_ascii_lowercase())
    } else {
        text
    }
}

/// Test-only: `count` generated statements from `seed` (xorshift) for the
/// comment readers' equivalence checks: words, numbers (an exponent's `-`
/// too), punctuation, every whitespace byte (a lone CR included), `--`, `/*`,
/// `*/`, nested, unterminated and empty comments, `;` inside and outside
/// them, routing hints (`/*helios:…*/`, in a line comment too) and stray
/// `helios:` text. With `quotes`, about half also hold literals, quoted
/// identifiers, dollar quotes and `$n` (a hint inside some of them).
#[cfg(test)]
pub(crate) fn generated_comment_texts(mut seed: u64, count: usize, quotes: bool) -> Vec<String> {
    const PLAIN: &[&str] = &[
        "SELECT",
        "select",
        "x",
        "t1",
        "a_b",
        "FROM",
        "1",
        "42",
        "1e-5",
        "1e--5",
        "1.5",
        ".5",
        "e",
        "é",
        "名",
        "\u{a0}",
        ";",
        ",",
        "(",
        ")",
        "=",
        "+",
        "<>",
        "::",
        "..",
        ".",
        "-",
        "/",
        "*",
        "--",
        "/*",
        "*/",
        "/**/",
        "/*/",
        "-- c",
        "/* c */",
        "/* a; b */",
        "/*;*/",
        "/* it */",
        "/* / * */",
        "helios:",
        "/*helios:route=primary*/",
        "/* helios:cache=skip */",
        "/*helios:route=standby,consistency=eventual*/",
        "/*helios:a=1 * b*/",
        "/*helios:*/",
        "/*helios:route=primary",
        "-- /*helios:route=standby*/",
        "--/*helios:cache=skip*/",
        "/* /*helios:route=primary*/",
        "/*helios:route=sync*//*helios:lag=5s*/",
        "/*\u{a0}helios:route=primary*/",
        "/*\nhelios:route=standby\n*/",
        "/*helios:a=1**/",
        "/**helios:route=primary*/",
        "--helios:route=primary",
    ];
    const SPACES: &[&str] = &[" ", "\n", "\r", "\r\n", "\t", "\u{b}", "\u{c}", "  "];
    const QUOTED: &[&str] = &[
        "'",
        "\"",
        "$",
        "$$",
        "$1",
        "$a$",
        "a$$",
        "'x'",
        "'it''s'",
        "E'a\\'b'",
        "U&'d'",
        "U&\"q\"",
        "\"id\"",
        "\"a\"\"b\"",
        "$$ x $$",
        "$t$ /* $t$",
        "'/*helios:route=standby*/'",
        "$$/*helios:route=standby*/$$",
        "\"/*helios:cache=skip*/\"",
        "/* it's */",
        "/* $ */",
    ];
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    (0..count)
        .map(|_| {
            let quoted = quotes && next() % 2 == 0;
            let mut sql = String::new();
            for _ in 0..next() % 16 {
                let token = if quoted && next() % 5 == 0 {
                    QUOTED[(next() % QUOTED.len() as u64) as usize]
                } else {
                    PLAIN[(next() % PLAIN.len() as u64) as usize]
                };
                sql.push_str(token);
                if next() % 3 != 0 {
                    sql.push_str(SPACES[(next() % SPACES.len() as u64) as usize]);
                }
            }
            if next() % 4 == 0 {
                sql.push(';');
            }
            sql
        })
        .collect()
}

#[cfg(test)]
mod comment_reader_tests {
    use super::*;

    /// Every comment of `sql`, read by the lexer with no limit.
    fn all_comments(sql: &str) -> Vec<(usize, &str)> {
        Pieces::new(sql)
            .filter_map(|(at, piece)| match piece {
                Piece::Comment(comment) => Some((at, comment)),
                _ => None,
            })
            .collect()
    }

    /// On quote-free text the scanner yields what the lexer yields, for
    /// every limit; the lexer's limit drops only the comments after it.
    #[test]
    fn quote_free_comments_match_the_lexer() {
        let mut found = 0;
        for sql in generated_comment_texts(0x51A7_E5C0_FFEE_1234, 20_000, false) {
            assert!(quote_free(&sql), "{sql:?}");
            let all = all_comments(&sql);
            let limits = [0, sql.len() / 3, sql.len() / 2, sql.len(), usize::MAX];
            for last in limits.into_iter().chain(all.iter().map(|&(at, _)| at)) {
                let lexed: Vec<_> = comments(&sql, last).collect();
                let want: Vec<_> = all.iter().copied().filter(|&(at, _)| at <= last).collect();
                assert_eq!(lexed, want, "lexer limit {last} in {sql:?}");
                let scanned: Vec<_> = quote_free_comments(&sql, last).collect();
                assert_eq!(scanned, lexed, "limit {last} in {sql:?}");
            }
            found += all.len();
        }
        assert!(
            found > 2_000,
            "only {found} comments read: the corpus must hold many"
        );
    }

    /// `short_block_comment_end` answers as `block_comment_end` at every
    /// `/*` of generated byte strings over `/`, `*` and filler bytes (nested,
    /// unterminated, `/*/`, `**/` and `/**/` included).
    #[test]
    fn short_block_comment_end_matches_block_comment_end() {
        const BYTES: &[u8] = b"/*/*a \n";
        let mut seed = 0x0B10_C0DE_7E57_0001_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut checked = 0;
        for _ in 0..20_000 {
            let len = (next() % 24) as usize;
            let text: Vec<u8> = (0..len)
                .map(|_| BYTES[(next() % BYTES.len() as u64) as usize])
                .collect();
            for start in 0..text.len().saturating_sub(1) {
                if &text[start..start + 2] == b"/*" {
                    assert_eq!(
                        short_block_comment_end(&text, start),
                        block_comment_end(&text, start),
                        "{:?} at {start}",
                        String::from_utf8_lossy(&text)
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 5_000, "{checked} openers checked");
        for (text, want) in [
            ("/**/", Some(4)),
            ("/*/", None),
            ("/* a /* b */", None),
            ("/* a */ /* b */", Some(7)),
            ("/* a **/", Some(8)),
            ("/* a /*/", None),
            ("/*", None),
        ] {
            assert_eq!(
                short_block_comment_end(text.as_bytes(), 0),
                want,
                "{text:?}"
            );
            assert_eq!(block_comment_end(text.as_bytes(), 0), want, "{text:?}");
        }
    }

    /// The searchers built once find `--` and `/*` exactly where a window
    /// scan does, over generated byte strings of `-`, `/`, `*`, quotes and
    /// filler (empty, one byte and a marker split across the end included).
    #[test]
    fn comment_start_searchers_match_a_window_scan() {
        const BYTES: &[u8] = b"-/*'\"a \n";
        let mut seed = 0x0C0D_E5EA_7C4E_0001_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let (mut with, mut without) = (0, 0);
        for _ in 0..20_000 {
            let len = (next() % 12) as usize;
            let text: Vec<u8> = (0..len)
                .map(|_| BYTES[(next() % BYTES.len() as u64) as usize])
                .collect();
            let want = text.windows(2).any(|pair| pair == b"--" || pair == b"/*");
            assert_eq!(holds_comment_start(&text), want, "{text:?}");
            let quoted = text.iter().any(|b| matches!(b, b'\'' | b'"'));
            assert_eq!(nano_reads_into(&text), want || quoted, "{text:?}");
            if want {
                with += 1;
            } else {
                without += 1;
            }
        }
        assert!(
            with > 2_000 && without > 2_000,
            "{with} with, {without} without"
        );
        for text in ["", "-", "/", "a-", "a/", "- -", "/ *", "*/"] {
            assert!(!holds_comment_start(text.as_bytes()), "{text:?}");
        }
    }

    #[test]
    fn quote_free_names_the_bytes_that_start_quoted_text() {
        for sql in ["SELECT 1 /* x */", "-- c\nSELECT a - b / c", ""] {
            assert!(quote_free(sql), "{sql:?}");
        }
        for sql in [
            "SELECT 'a'",
            "SELECT \"a\"",
            "SELECT $1",
            "SELECT $$a$$",
            "U&'x'",
        ] {
            assert!(!quote_free(sql), "{sql:?}");
        }
    }

    #[test]
    fn quote_free_comments_stop_at_a_rest() {
        // A lone CR ends a `--` comment for PostgreSQL only; a nested and an
        // unterminated block comment; a `;` HeliosDB-Lite splits inside.
        for sql in [
            "SELECT 1 -- a\r/*helios:route=primary*/",
            "/* a /*helios:route=primary*/ SELECT 1",
            "SELECT 1 /* open",
            "/* a; */ SELECT 1; SELECT 2 /* b */",
        ] {
            assert_eq!(
                quote_free_comments(sql, usize::MAX).collect::<Vec<_>>(),
                all_comments(sql),
                "{sql:?}"
            );
        }
        assert_eq!(
            quote_free_comments("SELECT 1 -- a\r\n/* b */", usize::MAX).collect::<Vec<_>>(),
            [(9, "-- a"), (15, "/* b */")]
        );
    }
}
