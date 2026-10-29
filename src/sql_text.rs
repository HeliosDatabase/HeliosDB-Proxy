//! Quote-aware SQL text scanning for the result caches (C-07).
//!
//! The query cache's normalizer (`src/cache/normalizer.rs`) and the edge
//! cache's fingerprint (`src/edge/fingerprint.rs`) both key a read by its
//! text with comments removed and literals folded to `?`. Doing that with
//! regular expressions run one after another merged statements that return
//! different rows: a `--` or `/*` inside a literal truncated the key, a
//! removed comment glued the tokens around it together, strings were folded
//! before numbers (so literal order was lost), and dollar-quoted literals and
//! quoted identifiers were upper-cased. This module reads the text once, the
//! way PostgreSQL does with `standard_conforming_strings = on`:
//!
//! - `--` (to the end of the line) and nested `/* */` are comments only
//!   outside `'…'`, `"…"` and `$tag$…$tag$`;
//! - `'…'` (with `''`), `$tag$…$tag$` and numbers are literals;
//! - `"…"` (with `""`) is a quoted identifier, whose case and spacing matter;
//! - every other word keeps its spelling in a fingerprint: HeliosDB-Nano
//!   labels an output column as written, so `SELECT Name …` and
//!   `SELECT name …` never share a key.
//!
//! Some text is backend-ambiguous: a backslash right before a quote inside
//! `'…'` (`standard_conforming_strings` on or off, which the proxy does not
//! track, end the literal in different places), and text HeliosDB-Nano or
//! HeliosDB-Lite (supported backends) read differently from PostgreSQL.
//! Nano's statement splitter and its parser's comment stripper end a block
//! comment at its first `*/` (no nesting) and a `--` comment at `\n` only
//! (not at `\r`), the stripper knows no dollar quotes, and the splitter opens
//! a dollar quote at any `$` that ASCII tag characters and another `$`
//! follow (inside a name too: `a$$`, `$1$`) and at no tag holding another
//! byte (`$é$`). Lite's splitter opens one alike and knows no comments at
//! all. So a `/*` inside a block comment, a `--` comment a lone `\r` ends, a
//! dollar-quoted literal holding `--`, `/*`, `'` or `"`, a `$` where only
//! some of them open a dollar quote, and a comment holding `;` or `$` or
//! leaving a quote open, with a `;` (not the final one) at or after its
//! start, stop the lexer, as does an unterminated literal, identifier or
//! comment: the rest of the text is one [`Piece::Rest`], never read any
//! other way. No result cache tier stores or serves a read that holds one
//! ([`uncacheable_text`]), a write that holds one invalidates everything
//! ([`single_statement`]), the journal capture takes a statement whose
//! first word lies behind one for a write of unknown scope
//! ([`leading_rest`]), and one holding a `;` ([`ambiguous_rest`]) may end a
//! statement the journal capture never sees. Each pass is linear in the
//! text.
//!
//! Whitespace is PostgreSQL's (16 and later) and HeliosDB-Nano's: space,
//! `\t`, `\n`, `\r`, `\f` and `\v`; only `\n` and `\r` are newlines.
//!
//! [`TableExclusions`] is the relay's fail-closed check for `[cache]
//! exclude_tables`: it looks at the raw text, comments and literals included.

use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use crate::replay_sql::{ident_cont, ident_start};

/// Kind byte of a `'…'` string literal in [`Fingerprint::kinds`].
pub(crate) const KIND_STRING: u8 = b's';
/// Kind byte of a `$tag$…$tag$` literal in [`Fingerprint::kinds`].
pub(crate) const KIND_DOLLAR: u8 = b'd';
/// Kind byte of a numeric literal in [`Fingerprint::kinds`].
pub(crate) const KIND_NUMBER: u8 = b'n';

/// One lexical piece of SQL text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Piece<'a> {
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

/// The pieces of `sql` in order, each with its byte offset.
struct Pieces<'a> {
    sql: &'a str,
    i: usize,
    /// The offset of the last `;` in `sql` without its trailing whitespace
    /// and one final `;` (and the whitespace before it), `Some(None)` when
    /// there is none: found once, when a comment first needs it
    /// ([`Self::comment`]), so each comment then costs only its own length.
    last_semicolon: Option<Option<usize>>,
}

impl<'a> Pieces<'a> {
    fn new(sql: &'a str) -> Self {
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
    fn comment(&mut self, start: usize, end: usize) -> (Piece<'a>, usize) {
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
fn lite_reads_into(body: &[u8]) -> bool {
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
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C)
}

/// Whether a dollar-quoted literal's `body` holds a byte HeliosDB-Nano's
/// comment stripper, which knows no dollar quotes, reads as SQL: the start of
/// a comment, or a quote (which flips its idea of what is quoted).
fn nano_reads_into(body: &[u8]) -> bool {
    memchr::memchr2(b'\'', b'"', body).is_some()
        || memchr::memmem::find(body, b"--").is_some()
        || memchr::memmem::find(body, b"/*").is_some()
}

/// Whether HeliosDB-Nano's time-travel rewrite may fire on the words `before`
/// and `after` when whitespace or comments separate them. Before parsing,
/// HeliosDB-Nano strips comments (a block comment becomes one space),
/// upper-cases the whole text and, when it holds exactly `" AS OF"`,
/// `"AS OF "` or `"VERSIONS BETWEEN"` (one space each), cuts an `AS OF …` or
/// `VERSIONS BETWEEN …` clause out of the statement: `SELECT v AS of FROM t`
/// loses its `FROM t`, `SELECT v AS  of FROM t` (two spaces) does not. So
/// whether it fires depends on the whitespace and comments around words
/// that end in `as` and start with `of` (`AS office`), or end in `versions`
/// and start with `between` (`conversions BETWEEN 1 AND 5`), compared the
/// way it upper-cases them.
pub(crate) fn nano_time_travel_words(before: &str, after: &str) -> bool {
    // No character but `o`/`O` (`b`/`B`) upper-cases to a string that starts
    // with `O` (`B`): a cheap exact filter.
    match after.as_bytes().first() {
        Some(b'o' | b'O') => upper_ends_with(before, "AS") && upper_starts_with(after, "OF"),
        Some(b'b' | b'B') => {
            upper_ends_with(before, "VERSIONS") && upper_starts_with(after, "BETWEEN")
        }
        _ => false,
    }
}

/// Whether one of HeliosDB-Nano's always-on text rewrites that match an exact
/// spacing may fire on the words `before` and `after` when whitespace or
/// comments separate them (its comment stripper turns a block comment into
/// one space and a `--` comment into a newline). Besides the time-travel
/// rewrite ([`nano_time_travel_words`]) these are its `SECURITY DEFINER` /
/// `SECURITY INVOKER` removal, which cuts the first of each (exactly one
/// space, any case, also where the words run on: `FROM xsecurity definer`
/// reads the table `x`, `FROM xsecurity  definer` the table `xsecurity`
/// aliased `definer`), and its `AS IDENTITY` rewrite, which drops a
/// parenthesized list after exactly `AS IDENTITY` (`FROM kv AS identity (a,
/// b)` loses its column aliases, `FROM kv AS  identity (a, b)` keeps them).
/// Both search the whole upper-cased text (string literals included), so
/// the words are compared the way it upper-cases them (`ı` is `I`). These
/// are not all of its spacing-exact rewrites (its empty-projection rewrite
/// matches exactly `select from ` with single spaces, and its point-lookup
/// fast path declines any statement holding a comment), and HeliosDB-Lite
/// has its own: on a HeliosDB backend the L2 and edge keys therefore keep
/// every run of whitespace and every comment ([`fingerprint_verbatim`]); this
/// check only narrows the collapsed fingerprint used for other backends.
pub(crate) fn nano_rewrite_words(before: &str, after: &str) -> bool {
    // No character but `d`/`D` upper-cases to a string that starts with `D`,
    // and none but `i`/`I` and `ı` (U+0131, UTF-8 0xC4 0xB1) with `I`: a cheap
    // exact filter.
    match after.as_bytes().first() {
        Some(b'd' | b'D') => {
            upper_ends_with(before, "SECURITY") && upper_starts_with(after, "DEFINER")
        }
        Some(b'i' | b'I' | 0xC4) => {
            (upper_ends_with(before, "SECURITY") && upper_starts_with(after, "INVOKER"))
                || (upper_ends_with(before, "AS") && upper_starts_with(after, "IDENTITY"))
        }
        _ => nano_time_travel_words(before, after),
    }
}

/// Whether `word` upper-cased (as Rust and HeliosDB-Nano do it, Unicode
/// included) starts with the upper-case ASCII `prefix`.
fn upper_starts_with(word: &str, prefix: &str) -> bool {
    if word.is_ascii() {
        word.len() >= prefix.len()
            && word.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
    } else {
        word.to_uppercase().starts_with(prefix)
    }
}

/// Whether `word` upper-cased ends with the upper-case ASCII `suffix`.
fn upper_ends_with(word: &str, suffix: &str) -> bool {
    if word.is_ascii() {
        word.len() >= suffix.len()
            && word.as_bytes()[word.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
    } else {
        word.to_uppercase().ends_with(suffix)
    }
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
fn run(b: &[u8], mut i: usize, keep: impl Fn(u8) -> bool) -> usize {
    while i < b.len() && keep(b[i]) {
        i += 1;
    }
    i
}

/// The end of the block comment at `start`: its first `*/`. `None` when it
/// is not closed or a `/*` comes before that `*/` (PostgreSQL nests block
/// comments, HeliosDB-Nano ends one at its first `*/`).
fn block_comment_end(b: &[u8], start: usize) -> Option<usize> {
    let body = &b[start + 2..];
    let close = memchr::memmem::find(body, b"*/")?;
    memchr::memmem::find(&body[..=close], b"/*")
        .is_none()
        .then_some(start + 2 + close + 2)
}

/// The end of the `'…'` literal at `start`, or `None` when it is not closed
/// or a backslash precedes one of its quotes (`standard_conforming_strings`
/// on or off, which the proxy does not track, end it in different places).
fn string_end(b: &[u8], start: usize) -> Option<usize> {
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
fn quoted_ident_end(b: &[u8], start: usize) -> Option<usize> {
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
fn dollar_tag_len(b: &[u8], start: usize) -> Option<usize> {
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
fn opens_dollar_quote_elsewhere(b: &[u8], at: usize) -> bool {
    b.get(run(b, at + 1, |c| c != b'$' && ident_cont(c))) == Some(&b'$')
}

/// The word from `start` to `end` (a run of [`ident_cont`] bytes) as a
/// [`Piece::Text`], or the rest of the text when it holds a `$` that
/// HeliosDB-Nano and HeliosDB-Lite open a dollar quote at: two `$` in one
/// name (`a$$`, `a$b$`), which PostgreSQL reads as part of it
/// (`SELECT 1 AS a$$ -- $$; DELETE FROM b` runs the DELETE on Nano).
fn word(sql: &str, start: usize, end: usize) -> (Piece<'_>, usize) {
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
fn number(sql: &str, start: usize) -> (Piece<'_>, usize) {
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
fn number_end(b: &[u8], start: usize) -> usize {
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
fn decinteger_end(b: &[u8], mut i: usize) -> usize {
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
struct Stripped {
    out: Option<String>,
    copied: usize,
}

impl Stripped {
    /// Replace the comment of `len` bytes at `start` with one space.
    fn comment(&mut self, sql: &str, start: usize, len: usize) {
        let out = self
            .out
            .get_or_insert_with(|| String::with_capacity(sql.len()));
        out.push_str(&sql[self.copied..start]);
        out.push(' ');
        self.copied = start + len;
    }

    fn finish(self, sql: &str) -> Cow<'_, str> {
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
fn has_comment_marker(b: &[u8]) -> bool {
    memchr::memmem::find(b, b"--").is_some() || memchr::memmem::find(b, b"/*").is_some()
}

/// Whether the lexer may reach a [`Piece::Rest`] in `b`: only a quote, a
/// dollar quote or a comment can start one.
fn may_hold_rest(b: &[u8]) -> bool {
    memchr::memchr3(b'\'', b'"', b'$', b).is_some() || has_comment_marker(b)
}

/// The backend-ambiguous rest of `sql` ([`Piece::Rest`]), when the lexer
/// reaches one: from there PostgreSQL and HeliosDB-Nano/Lite, or the two
/// `standard_conforming_strings` settings, may read different statements.
/// The relay takes one holding a `;` for a statement the journal capture
/// may not see (`cache_unseen_write`).
#[cfg(any(feature = "query-cache", test))]
pub(crate) fn ambiguous_rest(sql: &str) -> Option<&str> {
    if !may_hold_rest(sql.as_bytes()) {
        return None;
    }
    Pieces::new(sql).find_map(|(_, piece)| match piece {
        Piece::Rest(rest) => Some(rest),
        _ => None,
    })
}

/// The statement `sql` as the write invalidation reads it, in one pass of
/// the lexer: `sql` from its first word past whitespace and comments
/// ([`statement_start`]), and its comment-free text ([`strip_comments`]).
/// `None`, which makes the write's scope unknown (invalidate everything),
/// when the lexer reaches a backend-ambiguous rest anywhere ([`Piece::Rest`],
/// a leading comment the backends end in different places included), when
/// the text holds more than one statement (anything but whitespace and
/// comments after a `;` outside literals and comments: `UPDATE a …;
/// TRUNCATE b` used to move only `a`), when it holds no statement, when,
/// where the table extraction reads a table name ([`TableSpot`]), it names
/// something that extraction cannot read whole: a quoted identifier
/// (`UPDATE s."t" …` was read as a write of `s`, `UPDATE "t" … FROM u` as
/// one of `u`) or a name with a `$` or a non-ASCII character
/// ([`unreadable_name`]: `UPDATE t$x` was read as one of `t`), when, where
/// it may read a table the write changes ([`TableSpot::names_written_table`]:
/// after a table keyword other than `FROM`, or after any `FROM` once a
/// `DELETE` came before it), it names a quoted literal (HeliosDB-Nano and
/// HeliosDB-Lite, through sqlparser, take `'kv'` there for the table `kv`:
/// `UPDATE public.'kv' …` was read as a write of `public`, `DELETE x FROM
/// 'kv' …`, which deletes from `kv` there, as one of the tables after it),
/// when it holds a Unicode-escaped identifier (`U&"t"`) anywhere, and when a
/// word anywhere may be a keyword HeliosDB-Lite reads where the proxy reads
/// a name ([`may_read_as_keyword`]: `ınsert ınto kv` in a CTE). A quoted
/// identifier, a literal or a non-ASCII name anywhere else (a column, a
/// conflict target, an alias: `UPDATE t SET "lastLogin" = now()`,
/// `UPDATE customers SET 名前 = 'x'`), and a literal where a table the write
/// only reads would be (the `FROM` of an `UPDATE`, a subquery's,
/// `COPY t FROM '/file'`, `substring(s FROM 'x')`, in a statement without a
/// `DELETE`), names no written table and leaves the targets read; so does
/// anything after the `FROM` of `IS [NOT] DISTINCT FROM` ([`Role::Predicate`]).
/// Statement boundaries come only from the lexer: a `;` inside a literal
/// ends nothing, and no literal's text is ever read as SQL.
pub(crate) fn single_statement(sql: &str) -> Option<(&str, Cow<'_, str>)> {
    let b = sql.as_bytes();
    let mut stripped = Stripped::default();
    let mut first = None;
    let mut ended = false;
    let mut spot = TableSpot::Elsewhere;
    let mut deletes = DeleteBefore::default();
    for (start, piece) in Pieces::new(sql) {
        match piece {
            Piece::Space(_) => {}
            Piece::Comment(comment) => stripped.comment(sql, start, comment.len()),
            Piece::Rest(_) => return None,
            _ if ended => return None,
            Piece::Text(";") => ended = true,
            Piece::Text(text) => {
                spot = spot.after_write_text(text)?;
                first.get_or_insert(start);
                continue;
            }
            Piece::Ident(_) if spot.names_table() => return None,
            Piece::Str(_) | Piece::Dollar(..)
                if spot.names_written_table(|| deletes.found(sql, start)) =>
            {
                return None
            }
            Piece::Ident(_) if start.checked_sub(1).is_some_and(|i| b[i] == b'&') => return None,
            _ => {
                first.get_or_insert(start);
            }
        }
        spot = spot.after(piece);
    }
    Some((statement_start(&sql[first?..])?, stripped.finish(sql)))
}

/// The words the table extraction (the query cache's and the edge cache's
/// `TABLE_PATTERN`) reads a table name after, `ONLY` included.
const TABLE_KEYWORDS: &[&str] = &[
    "FROM", "JOIN", "INTO", "UPDATE", "TABLE", "COPY", "INSERT", "ONLY",
];

/// Where a statement's pieces stand for the table extraction, which reads a
/// name after one of [`TABLE_KEYWORDS`] (across whitespace, comments and
/// opening parentheses) and the parts a `.` joins to it. A quoted
/// identifier or a name with a `$` or a non-ASCII character there is a
/// table it cannot read ([`single_statement`]); so is a quoted literal,
/// which HeliosDB-Nano and HeliosDB-Lite read as a table there: a read with
/// one there is not cached ([`uncacheable_text`]), and a write with one
/// where it may name a table the write changes
/// ([`Self::names_written_table`]) has an unknown scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableSpot {
    /// No table name is read here.
    Elsewhere,
    /// After `IS` or `IS NOT`.
    Is,
    /// After `IS [NOT] DISTINCT`: a `FROM` next is the predicate's
    /// ([`Role::Predicate`]).
    IsDistinct,
    /// After a table keyword, a `(` after one, or a number running into a
    /// word ([`number_runs_into_word`]): a name is read next.
    Name(Role),
    /// After a name read there: a `.` continues it.
    AfterName(Role),
    /// After that `.`: the next part of the name is read.
    Part(Role),
}

/// What a table name read at a [`TableSpot`] is to a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// A table the write may change: after `UPDATE`, `INTO`, `TABLE`,
    /// `COPY`, `INSERT`, `ONLY` or `JOIN` (an `UPDATE`'s table may hold
    /// joins in sqlparser), or after a number running into a word, whose
    /// rest may be any of them ([`number_runs_into_word`]).
    Target,
    /// A table after a `FROM`: one a write only reads (`UPDATE t … FROM u`,
    /// a subquery) or loads from (`COPY t FROM '/file'`), or an argument of a
    /// function's `FROM` (`substring(s FROM 'x')`), unless a `DELETE` comes
    /// before it. sqlparser's `DELETE [<names>] FROM <table>` deletes from
    /// that table whatever names come between (HeliosDB-Nano and
    /// HeliosDB-Lite run `DELETE x FROM 'kv'` as a delete from `kv`), so
    /// after a `DELETE` any `FROM` may name a table the write changes
    /// ([`TableSpot::names_written_table`]).
    Source,
    /// After the `FROM` of `IS [NOT] DISTINCT FROM`, which starts an
    /// expression where `IS` is the operator, and never names a table a
    /// write changes: in sqlparser's grammar no `DELETE`'s `FROM` follows
    /// `IS DISTINCT`. sqlparser also takes such an `is` for a column and
    /// `DISTINCT` for its alias (HeliosDB-Nano and HeliosDB-Lite read
    /// `SELECT kv.is DISTINCT FROM 'kv'` as a read of `kv`), so a read still
    /// takes a table name here ([`uncacheable_text`]).
    Predicate,
}

impl TableSpot {
    /// Whether the table extraction, or sqlparser, may read a table name
    /// here: a read with a quoted literal here is not cached
    /// ([`uncacheable_text`]).
    fn reads_name(self) -> bool {
        matches!(self, Self::Name(_) | Self::Part(_))
    }

    /// Whether a write may name a table here: a name is read here, and not
    /// after the `FROM` of `IS [NOT] DISTINCT FROM` ([`Role::Predicate`]).
    fn names_table(self) -> bool {
        matches!(self, Self::Name(role) | Self::Part(role) if role != Role::Predicate)
    }

    /// Whether a write may name a table it changes here: after a
    /// [`Role::Target`] keyword, or after a `FROM` ([`Role::Source`]) when
    /// `delete_before` finds a `DELETE` before it (asked only then).
    fn names_written_table(self, delete_before: impl FnOnce() -> bool) -> bool {
        match self {
            Self::Name(Role::Target) | Self::Part(Role::Target) => true,
            Self::Name(Role::Source) | Self::Part(Role::Source) => delete_before(),
            _ => false,
        }
    }

    /// The spot after `piece`. Whitespace and comments keep it; a Unicode
    /// space inside a [`Piece::Text`] separates its words, as it does for
    /// HeliosDB-Nano, HeliosDB-Lite and the extraction's pattern.
    fn after(self, piece: Piece<'_>) -> Self {
        match piece {
            Piece::Space(_) | Piece::Comment(_) => self,
            Piece::Text(text) if text.is_ascii() => self.after_word(text),
            Piece::Text(text) => text
                .split(char::is_whitespace)
                .filter(|word| !word.is_empty())
                .fold(self, Self::after_word),
            _ => Self::Elsewhere,
        }
    }

    /// The spot after the [`Piece::Text`] `text` of a write, as
    /// [`Self::after`] reads it, or `None` when one of its words is a name
    /// the table extraction cannot read whole where a write may name a table
    /// ([`unreadable_name`], [`Self::names_table`]), or, wherever it stands,
    /// a word HeliosDB-Lite may read as a keyword ([`may_read_as_keyword`]).
    /// Each word of a text holding a Unicode space is checked against its
    /// own spot.
    fn after_write_text(self, text: &str) -> Option<Self> {
        if text.is_ascii() {
            return (!(self.names_table() && unreadable_name(text))).then(|| self.after_word(text));
        }
        let mut spot = self;
        for word in text
            .split(char::is_whitespace)
            .filter(|word| !word.is_empty())
        {
            if (spot.names_table() && unreadable_name(word)) || may_read_as_keyword(word) {
                return None;
            }
            spot = spot.after_word(word);
        }
        Some(spot)
    }

    /// The spot after the word, operator or punctuation `word`. Only a word
    /// is compared with the keywords: the punctuation of a long `VALUES`
    /// list costs a byte test or two.
    fn after_word(self, word: &str) -> Self {
        if !word.bytes().next().is_some_and(ident_start) {
            return match (self, word) {
                (Self::Name(role), "(") => Self::Name(role),
                (Self::AfterName(role), ".") => Self::Part(role),
                _ if number_runs_into_word(word) => Self::Name(Role::Target),
                _ => Self::Elsewhere,
            };
        }
        let is = |keyword: &str| word.eq_ignore_ascii_case(keyword);
        if is("FROM") {
            return Self::Name(if self == Self::IsDistinct {
                Role::Predicate
            } else {
                Role::Source
            });
        }
        if TABLE_KEYWORDS.iter().any(|k| is(k)) {
            return Self::Name(Role::Target);
        }
        match self {
            Self::Name(role) | Self::Part(role) => Self::AfterName(role),
            _ if is("IS") => Self::Is,
            Self::Is if is("NOT") => Self::Is,
            Self::Is if is("DISTINCT") => Self::IsDistinct,
            _ => Self::Elsewhere,
        }
    }
}

/// Whether the [`Piece::Text`] word `word` is a number running into an
/// identifier (`1from`, `.5join`, `0x1F`: [`number`]). sqlparser takes no
/// identifier starting with a digit: it reads a number there and the rest
/// as a word of its own (HeliosDB-Nano and HeliosDB-Lite read
/// `SELECT 1from 'kv'` as `SELECT 1 FROM 'kv'`, a read of `kv`), which may
/// be any table keyword, so a table name of any role may be read after it
/// ([`Role::Target`]).
fn number_runs_into_word(word: &str) -> bool {
    let b = word.as_bytes();
    b.first().is_some_and(u8::is_ascii_digit)
        || (b.first() == Some(&b'.') && b.get(1).is_some_and(u8::is_ascii_digit))
}

/// Whether a statement holds the word `DELETE` (ASCII case-insensitive, a
/// run of identifier characters in a [`Piece::Text`]: a Unicode space ends
/// one) before a point [`single_statement`] asks about, searched only when
/// it asks and from where the last search stopped: each byte at most once.
/// No literal's, identifier's or comment's text counts. A non-ASCII
/// spelling HeliosDB-Lite reads as `DELETE` already makes the statement's
/// scope unknown ([`may_read_as_keyword`]).
#[derive(Default)]
struct DeleteBefore {
    searched: usize,
    found: bool,
}

impl DeleteBefore {
    /// Whether `sql` holds `DELETE` before byte `at`, the start of a piece
    /// at or after the last one asked about.
    fn found(&mut self, sql: &str, at: usize) -> bool {
        if !self.found {
            self.found = Pieces::new(&sql[self.searched..at]).any(|(_, piece)| match piece {
                Piece::Text(text) => text
                    .split(|c: char| !continues_identifier_everywhere(c))
                    .any(|word| word.eq_ignore_ascii_case("DELETE")),
                _ => false,
            });
            self.searched = at;
        }
        self.found
    }
}

/// Whether the word `text` (a [`Piece::Text`]) may be, or hold, a name the
/// table extraction cannot read whole: its pattern reads ASCII letters,
/// digits and `_`, and whitespace, so a name with a `$` or another non-ASCII
/// character would lose its end (`t$x` read as `t`) or not be read at all
/// (`INSERT INTO été …` was read as a write of a table `into`). A `$n`
/// parameter is no name. Checked only where the extraction reads a table
/// ([`TableSpot`]): a non-ASCII column (`UPDATE kunden SET größe = 3`)
/// leaves the target read.
fn unreadable_name(text: &str) -> bool {
    !text.starts_with('$')
        && (memchr::memchr(b'$', text.as_bytes()).is_some()
            || (!text.is_ascii() && text.chars().any(|c| !c.is_ascii() && !c.is_whitespace())))
}

/// Whether the word `word` (no whitespace in it) holds a run of identifier
/// characters ([`continues_identifier_everywhere`], sqlparser's word
/// characters) that is not ASCII but upper-cases, the Unicode way, to one of
/// sqlparser's keywords ([`SQLPARSER_KEYWORDS`]): HeliosDB-Lite (sqlparser)
/// takes a word for the keyword its upper case (`str::to_uppercase`) is,
/// and reads `ınsert` (a dotless `ı`) as `INSERT`, `ſet` as `SET` and
/// `ﬁlter` (a ligature) as `FILTER`, where PostgreSQL and the table
/// extraction read a name (`WITH d AS (ınsert ınto kv …) SELECT * FROM d`
/// writes `kv` there). A run whose upper case is no keyword is a name on
/// every backend: one with a character that stays non-ASCII (`größe` is
/// `GRÖSSE`, `名前`) and one that upper-cases to ASCII text no keyword
/// spells (`straße` is `STRASSE`, `adı` is `ADI`). A run that starts with a
/// digit is not one word for sqlparser, which starts none with a digit: it
/// reads a number there and the rest as a word of its own (`1ınsert` is `1`
/// and `INSERT`), so such a run counts whenever its upper case is ASCII
/// text, whatever the number's end.
fn may_read_as_keyword(word: &str) -> bool {
    !word.is_ascii()
        && word
            .split(|c: char| !continues_identifier_everywhere(c))
            .filter(|run| !run.is_ascii())
            .any(|run| {
                let upper = run.to_uppercase();
                upper.is_ascii()
                    && (run.starts_with(|c: char| c.is_ascii_digit())
                        || SQLPARSER_KEYWORDS.binary_search(&upper.as_str()).is_ok())
            })
}

/// sqlparser 0.53.0's `ALL_KEYWORDS` (the version HeliosDB-Nano and
/// HeliosDB-Lite pin), in its byte order: its tokenizer takes a word for a
/// keyword when the word's upper case (`str::to_uppercase`) is one of these
/// ([`may_read_as_keyword`]). Re-read it from the pinned version when the
/// backends move to another.
#[rustfmt::skip]
const SQLPARSER_KEYWORDS: &[&str] = &[
    "ABORT", "ABS", "ABSENT", "ABSOLUTE", "ACCESS", "ACCOUNT", "ACTION", "ADD", "ADMIN", "AFTER",
    "AGAINST", "AGGREGATION", "ALIAS", "ALL", "ALLOCATE", "ALTER", "ALWAYS", "ANALYZE", "AND",
    "ANTI", "ANY", "APPLICATION", "APPLY", "ARCHIVE", "ARE", "ARRAY", "ARRAY_MAX_CARDINALITY",
    "AS", "ASC", "ASENSITIVE", "ASOF", "ASSERT", "ASYMMETRIC", "AT", "ATOMIC", "ATTACH",
    "AUTHORIZATION", "AUTO", "AUTOINCREMENT", "AUTO_INCREMENT", "AVG", "AVRO", "BACKWARD",
    "BASE64", "BEFORE", "BEGIN", "BEGIN_FRAME", "BEGIN_PARTITION", "BETWEEN", "BIGDECIMAL",
    "BIGINT", "BIGNUMERIC", "BINARY", "BINDING", "BIT", "BLOB", "BLOOMFILTER", "BOOL", "BOOLEAN",
    "BOTH", "BROWSE", "BTREE", "BUCKETS", "BY", "BYPASSRLS", "BYTEA", "BYTES", "CACHE", "CALL",
    "CALLED", "CARDINALITY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CEIL", "CEILING",
    "CENTURY", "CHAIN", "CHANGE", "CHANGE_TRACKING", "CHANNEL", "CHAR", "CHARACTER", "CHARACTERS",
    "CHARACTER_LENGTH", "CHARSET", "CHAR_LENGTH", "CHECK", "CLEAR", "CLOB", "CLONE", "CLOSE",
    "CLUSTER", "CLUSTERED", "CLUSTERING", "COALESCE", "COLLATE", "COLLATION", "COLLECT",
    "COLLECTION", "COLUMN", "COLUMNS", "COLUMNSTORE", "COMMENT", "COMMIT", "COMMITTED",
    "COMPRESSION", "COMPUTE", "CONCURRENTLY", "CONDITION", "CONFLICT", "CONNECT", "CONNECTION",
    "CONSTRAINT", "CONTAINS", "CONTINUE", "CONVERT", "COPY", "COPY_OPTIONS", "CORR",
    "CORRESPONDING", "COUNT", "COVAR_POP", "COVAR_SAMP", "CREATE", "CREATEDB", "CREATEROLE",
    "CREDENTIALS", "CROSS", "CSV", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_CATALOG",
    "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH", "CURRENT_ROLE",
    "CURRENT_ROW", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE", "DATA", "DATABASE",
    "DATABASES", "DATA_RETENTION_TIME_IN_DAYS", "DATE", "DATE32", "DATETIME", "DATETIME64", "DAY",
    "DAYOFWEEK", "DAYOFYEAR", "DEALLOCATE", "DEC", "DECADE", "DECIMAL", "DECLARE", "DEDUPLICATE",
    "DEFAULT", "DEFAULT_DDL_COLLATION", "DEFERRABLE", "DEFERRED", "DEFINE", "DEFINED", "DELAYED",
    "DELETE", "DELIMITED", "DELIMITER", "DELTA", "DENSE_RANK", "DEREF", "DESC", "DESCRIBE",
    "DETACH", "DETAIL", "DETERMINISTIC", "DIRECTORY", "DISABLE", "DISCARD", "DISCONNECT",
    "DISTINCT", "DISTRIBUTE", "DIV", "DO", "DOUBLE", "DOW", "DOY", "DROP", "DRY", "DUPLICATE",
    "DYNAMIC", "EACH", "ELEMENT", "ELEMENTS", "ELSE", "EMPTY", "ENABLE", "ENABLE_SCHEMA_EVOLUTION",
    "ENCODING", "ENCRYPTION", "END", "END-EXEC", "ENDPOINT", "END_FRAME", "END_PARTITION",
    "ENFORCED", "ENGINE", "ENUM", "ENUM16", "ENUM8", "EPHEMERAL", "EPOCH", "EQUALS", "ERROR",
    "ESCAPE", "ESCAPED", "EVENT", "EVERY", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXCLUSIVE", "EXEC",
    "EXECUTE", "EXISTS", "EXP", "EXPANSION", "EXPLAIN", "EXPLICIT", "EXPORT", "EXTENDED",
    "EXTENSION", "EXTERNAL", "EXTRACT", "FAIL", "FALSE", "FETCH", "FIELDS", "FILE", "FILES",
    "FILE_FORMAT", "FILL", "FILTER", "FINAL", "FIRST", "FIRST_VALUE", "FIXEDSTRING", "FLOAT",
    "FLOAT32", "FLOAT4", "FLOAT64", "FLOAT8", "FLOOR", "FLUSH", "FN", "FOLLOWING", "FOR", "FORCE",
    "FORCE_NOT_NULL", "FORCE_NULL", "FORCE_QUOTE", "FOREIGN", "FORMAT", "FORMATTED", "FORWARD",
    "FRAME_ROW", "FREE", "FREEZE", "FROM", "FSCK", "FULL", "FULLTEXT", "FUNCTION", "FUNCTIONS",
    "FUSION", "GENERAL", "GENERATE", "GENERATED", "GEOGRAPHY", "GET", "GLOBAL", "GRANT", "GRANTED",
    "GRANTS", "GRAPHVIZ", "GROUP", "GROUPING", "GROUPS", "HASH", "HAVING", "HEADER", "HEAP",
    "HIGH_PRIORITY", "HISTORY", "HIVEVAR", "HOLD", "HOSTS", "HOUR", "HOURS", "ID", "IDENTITY",
    "IF", "IGNORE", "ILIKE", "IMMEDIATE", "IMMUTABLE", "IN", "INCLUDE", "INCLUDE_NULL_VALUES",
    "INCREMENT", "INDEX", "INDICATOR", "INHERIT", "INITIALLY", "INNER", "INOUT", "INPATH", "INPUT",
    "INPUTFORMAT", "INSENSITIVE", "INSERT", "INSTALL", "INSTEAD", "INT", "INT128", "INT16", "INT2",
    "INT256", "INT32", "INT4", "INT64", "INT8", "INTEGER", "INTERPOLATE", "INTERSECT",
    "INTERSECTION", "INTERVAL", "INTO", "IS", "ISODOW", "ISOLATION", "ISOWEEK", "ISOYEAR", "ITEMS",
    "JAR", "JOIN", "JSON", "JSONB", "JSONFILE", "JSON_TABLE", "JULIAN", "KEY", "KEYS", "KILL",
    "LAG", "LANGUAGE", "LARGE", "LAST", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT",
    "LEVEL", "LIKE", "LIKE_REGEX", "LIMIT", "LINES", "LISTEN", "LN", "LOAD", "LOCAL", "LOCALTIME",
    "LOCALTIMESTAMP", "LOCATION", "LOCK", "LOCKED", "LOGIN", "LOGS", "LONGBLOB", "LONGTEXT",
    "LOWCARDINALITY", "LOWER", "LOW_PRIORITY", "MACRO", "MANAGEDLOCATION", "MAP", "MASKING",
    "MATCH", "MATCHED", "MATCHES", "MATCH_CONDITION", "MATCH_RECOGNIZE", "MATERIALIZE",
    "MATERIALIZED", "MAX", "MAXVALUE", "MAX_DATA_EXTENSION_TIME_IN_DAYS", "MEASURES", "MEDIUMBLOB",
    "MEDIUMINT", "MEDIUMTEXT", "MEMBER", "MERGE", "METADATA", "METHOD", "MICROSECOND",
    "MICROSECONDS", "MILLENIUM", "MILLENNIUM", "MILLISECOND", "MILLISECONDS", "MIN", "MINUTE",
    "MINVALUE", "MOD", "MODE", "MODIFIES", "MODIFY", "MODULE", "MONTH", "MSCK", "MULTISET",
    "MUTATION", "NAME", "NANOSECOND", "NANOSECONDS", "NATIONAL", "NATURAL", "NCHAR", "NCLOB",
    "NESTED", "NEW", "NEXT", "NO", "NOBYPASSRLS", "NOCREATEDB", "NOCREATEROLE", "NOINHERIT",
    "NOLOGIN", "NONE", "NOORDER", "NOREPLICATION", "NORMALIZE", "NOSCAN", "NOSUPERUSER", "NOT",
    "NOTHING", "NOTIFY", "NOWAIT", "NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE", "NULL", "NULLABLE",
    "NULLIF", "NULLS", "NUMERIC", "NVARCHAR", "OBJECT", "OCCURRENCES_REGEX", "OCTETS",
    "OCTET_LENGTH", "OF", "OFFSET", "OLD", "OMIT", "ON", "ONE", "ONLY", "OPEN", "OPENJSON",
    "OPERATOR", "OPTIMIZE", "OPTIMIZER_COSTS", "OPTION", "OPTIONS", "OR", "ORC", "ORDER",
    "ORDINALITY", "OUT", "OUTER", "OUTPUTFORMAT", "OVER", "OVERFLOW", "OVERLAPS", "OVERLAY",
    "OVERWRITE", "OWNED", "OWNER", "PARALLEL", "PARAMETER", "PARQUET", "PART", "PARTITION",
    "PARTITIONED", "PARTITIONS", "PASSWORD", "PAST", "PATH", "PATTERN", "PER", "PERCENT",
    "PERCENTILE_CONT", "PERCENTILE_DISC", "PERCENT_RANK", "PERIOD", "PERMISSIVE", "PERSISTENT",
    "PIVOT", "PLACING", "PLAN", "PLANS", "POLICY", "PORTION", "POSITION", "POSITION_REGEX",
    "POWER", "PRAGMA", "PRECEDES", "PRECEDING", "PRECISION", "PREPARE", "PRESERVE", "PREWHERE",
    "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PROGRAM", "PROJECTION", "PURGE", "QUALIFY",
    "QUARTER", "QUERY", "QUOTE", "RANGE", "RANK", "RAW", "RCFILE", "READ", "READS", "READ_ONLY",
    "REAL", "RECLUSTER", "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGCLASS", "REGEXP",
    "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX",
    "REGR_SXY", "REGR_SYY", "RELATIVE", "RELAY", "RELEASE", "REMOTE", "RENAME", "REORG", "REPAIR",
    "REPEATABLE", "REPLACE", "REPLICA", "REPLICATION", "RESET", "RESPECT", "RESTART", "RESTRICT",
    "RESTRICTED", "RESTRICTIVE", "RESULT", "RESULTSET", "RESUME", "RETAIN", "RETURN", "RETURNING",
    "RETURNS", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES", "ROLLBACK", "ROLLUP", "ROOT", "ROW",
    "ROWID", "ROWS", "ROW_NUMBER", "RULE", "RUN", "SAFE", "SAFE_CAST", "SAVEPOINT", "SCHEMA",
    "SCHEMAS", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SECONDARY", "SECRET", "SECURITY", "SELECT",
    "SEMI", "SENSITIVE", "SEPARATOR", "SEQUENCE", "SEQUENCEFILE", "SEQUENCES", "SERDE",
    "SERDEPROPERTIES", "SERIALIZABLE", "SESSION", "SESSION_USER", "SET", "SETS", "SETTINGS",
    "SHARE", "SHOW", "SIMILAR", "SKIP", "SLOW", "SMALLINT", "SNAPSHOT", "SOME", "SORT", "SORTED",
    "SOURCE", "SPATIAL", "SPECIFIC", "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE",
    "SQLWARNING", "SQRT", "STABLE", "STAGE", "START", "STARTS", "STATEMENT", "STATIC",
    "STATISTICS", "STATUS", "STDDEV_POP", "STDDEV_SAMP", "STDIN", "STDOUT", "STEP",
    "STORAGE_INTEGRATION", "STORED", "STRICT", "STRING", "STRUCT", "SUBMULTISET", "SUBSTRING",
    "SUBSTRING_REGEX", "SUCCEEDS", "SUM", "SUPER", "SUPERUSER", "SUSPEND", "SWAP", "SYMMETRIC",
    "SYNC", "SYSTEM", "SYSTEM_TIME", "SYSTEM_USER", "TABLE", "TABLES", "TABLESAMPLE", "TAG",
    "TARGET", "TBLPROPERTIES", "TEMP", "TEMPORARY", "TERMINATED", "TERSE", "TEXT", "TEXTFILE",
    "THEN", "TIES", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "TIMETZ", "TIMEZONE", "TIMEZONE_ABBR",
    "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TIMEZONE_REGION", "TINYBLOB", "TINYINT", "TINYTEXT", "TO",
    "TOP", "TOTALS", "TRAILING", "TRANSACTION", "TRANSIENT", "TRANSLATE", "TRANSLATE_REGEX",
    "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRIM_ARRAY", "TRUE", "TRUNCATE", "TRY_CAST",
    "TRY_CONVERT", "TUPLE", "TYPE", "UESCAPE", "UINT128", "UINT16", "UINT256", "UINT32", "UINT64",
    "UINT8", "UNBOUNDED", "UNCACHE", "UNCOMMITTED", "UNFREEZE", "UNION", "UNIQUE", "UNKNOWN",
    "UNLISTEN", "UNLOAD", "UNLOCK", "UNLOGGED", "UNMATCHED", "UNNEST", "UNPIVOT", "UNSAFE",
    "UNSIGNED", "UNTIL", "UPDATE", "UPPER", "URL", "USAGE", "USE", "USER", "USER_RESOURCES",
    "USING", "UUID", "VACUUM", "VALID", "VALIDATION_MODE", "VALUE", "VALUES", "VALUE_OF",
    "VARBINARY", "VARCHAR", "VARIABLES", "VARYING", "VAR_POP", "VAR_SAMP", "VERBOSE", "VERSION",
    "VERSIONING", "VIEW", "VIEWS", "VIRTUAL", "VOLATILE", "WAREHOUSE", "WEEK", "WHEN", "WHENEVER",
    "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WITHOUT_ARRAY_WRAPPER",
    "WORK", "WRITE", "XML", "XOR", "YEAR", "ZONE", "ZORDER",
];

/// Why no result cache tier (L1, L2, L3, `[edge]`) stores or serves a read,
/// from its comments and quoting ([`uncacheable_text`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UncacheableText {
    /// A comment holds a time-travel clause: `AS OF` or `VERSIONS BETWEEN`
    /// with any run of whitespace between the words, ASCII
    /// case-insensitive (a `helios:` hint is checked too; a real one holds
    /// neither). The keys leave comments out, while HeliosDB-Nano and
    /// HeliosDB-Lite read a time-travel clause in a comment (Lite cuts it
    /// out of the raw text across the end of the comment).
    TimeTravelComment,
    /// Backend-ambiguous text, where the lexer stops: a backslash before a
    /// quote in a `'…'` literal, a nested `/*`, a `--` comment a lone `\r`
    /// ends, a dollar quote holding a quote or a comment marker, a `$` that
    /// HeliosDB-Nano or HeliosDB-Lite open a dollar quote at and PostgreSQL
    /// does not (`a$$`, `$1$`) or the reverse (`$é$`), a comment holding `;`
    /// or `$` or leaving a quote open before a later `;`, or an unterminated
    /// token; or a quoted literal where the table extraction or sqlparser
    /// may read a table name ([`TableSpot`]: `FROM public.'kv'`,
    /// `FROM 'kv'`, after any `FROM`, that of `IS DISTINCT FROM` included,
    /// and after a number running into a word, `SELECT 1from 'kv'`), which
    /// HeliosDB-Nano and HeliosDB-Lite (sqlparser) read as the table `kv`,
    /// PostgreSQL rejects, and the extraction cannot read.
    /// PostgreSQL and HeliosDB-Nano/Lite may read it as different statements.
    AmbiguousText,
}

/// Why the read `sql` is uncacheable for its comments and quoting, in one
/// pass of the lexer (none without a quote, a dollar quote or a comment
/// marker), or `None`.
pub fn uncacheable_text(sql: &str) -> Option<UncacheableText> {
    if !may_hold_rest(sql.as_bytes()) {
        return None;
    }
    let mut spot = TableSpot::Elsewhere;
    for (_, piece) in Pieces::new(sql) {
        match piece {
            Piece::Comment(text) if holds_time_travel_words(text) => {
                return Some(UncacheableText::TimeTravelComment)
            }
            Piece::Rest(_) => return Some(UncacheableText::AmbiguousText),
            Piece::Str(_) | Piece::Dollar(..) if spot.reads_name() => {
                return Some(UncacheableText::AmbiguousText)
            }
            _ => {}
        }
        spot = spot.after(piece);
    }
    None
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
pub(crate) fn statement_start(sql: &str) -> Option<&str> {
    match lead(sql) {
        Lead::Start(start) => Some(start),
        Lead::Rest | Lead::Nothing => None,
    }
}

/// Whether the lexer reaches a backend-ambiguous rest ([`Piece::Rest`])
/// before the first word of `sql` (past its leading whitespace and
/// comments): what the statement is (a read, a write, DDL, transaction
/// control) is then unknown, since the backends may end the leading comment
/// in different places (`/* purge /tmp/* entries */ DELETE FROM sessions`
/// runs the DELETE on HeliosDB-Nano and fails on PostgreSQL; `-- x` and a
/// lone `\r` before `SELECT 1` newline `UPDATE kv …` runs the UPDATE on
/// Nano). The journal capture takes such a statement for a write it cannot
/// reproduce, and `[edge]` for a write of unknown scope.
pub(crate) fn leading_rest(sql: &str) -> bool {
    matches!(lead(sql), Lead::Rest)
}

/// What comes first in a statement past its leading whitespace and
/// comments ([`lead`]).
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

/// The statements of the simple-query string `sql`, split at each `;` the
/// lexer finds outside literals, quoted identifiers, dollar quotes and
/// comments (R6-2: a statement boundary comes from nowhere else), each from
/// its first token past whitespace and comments up to its `;`; one holding
/// nothing else is left out. `None` when the lexer reaches a
/// backend-ambiguous rest ([`Piece::Rest`]): where the backends end the
/// statements is unknown.
#[cfg(any(feature = "edge-proxy", test))]
pub(crate) fn statements(sql: &str) -> Option<Vec<&str>> {
    let mut statements = Vec::new();
    let mut first = None;
    for (start, piece) in Pieces::new(sql) {
        match piece {
            Piece::Space(_) | Piece::Comment(_) => {}
            Piece::Rest(_) => return None,
            Piece::Text(";") => statements.extend(first.take().map(|at| &sql[at..start])),
            _ => {
                first.get_or_insert(start);
            }
        }
    }
    statements.extend(first.map(|at| &sql[at..]));
    Some(statements)
}

/// Whether `sql` holds one of the upper-case ASCII keywords `words` as a
/// word outside its literals, quoted identifiers, dollar quotes and comments
/// (a run of identifier characters in a [`Piece::Text`]: a Unicode space, or
/// another character no backend continues a name with, ends one), compared
/// ASCII case-insensitively, or upper-cased the Unicode way when it is not
/// ASCII, as HeliosDB-Lite matches keywords (`ınsert`, with a dotless `ı`,
/// is `INSERT` there); or whether the lexer reaches a backend-ambiguous
/// rest ([`Piece::Rest`]), where what is a word is unknown. No literal's,
/// identifier's or comment's text is read as SQL: `WITH r AS (SELECT … WHERE
/// op = 'delete') …` holds no `DELETE`.
#[cfg(any(feature = "edge-proxy", test))]
pub(crate) fn may_hold_keyword(sql: &str, words: &[&str]) -> bool {
    Pieces::new(sql).any(|(_, piece)| match piece {
        Piece::Rest(_) => true,
        Piece::Text(text) if text.is_ascii() => {
            words.iter().any(|word| text.eq_ignore_ascii_case(word))
        }
        Piece::Text(text) => text
            .split(|c: char| !continues_identifier_everywhere(c))
            .any(|run| {
                words
                    .iter()
                    .any(|word| run.chars().flat_map(char::to_uppercase).eq(word.chars()))
            }),
        _ => false,
    })
}

/// Whether `text` starts with the keyword `word` (ASCII case-insensitive),
/// not followed by a character that continues an identifier on every
/// backend ([`continues_identifier_everywhere`]): after a Unicode space the
/// keyword ends for HeliosDB-Nano and HeliosDB-Lite (`MERGE\u{a0}BRANCH`).
pub(crate) fn starts_with_keyword(text: &str, word: &str) -> bool {
    let b = text.as_bytes();
    b.len() >= word.len()
        && b[..word.len()].eq_ignore_ascii_case(word.as_bytes())
        && !char_after(b, word.len()).is_some_and(continues_identifier_everywhere)
}

/// Whether the statement `sql` (from its first word: [`statement_start`])
/// is a `MERGE` that is not PostgreSQL's `MERGE INTO target USING …`:
/// HeliosDB-Nano's `MERGE [DATABASE] BRANCH src INTO dst` changes every
/// table of the branch it merges into, and the word after its `INTO` is a
/// branch, never a table.
pub(crate) fn merge_without_into(sql: &str) -> bool {
    const MERGE: &str = "MERGE";
    starts_with_keyword(sql, MERGE)
        && !statement_start(&sql[MERGE.len()..])
            .is_some_and(|rest| starts_with_keyword(rest, "INTO"))
}

/// `text` past its leading whitespace and comments ([`statement_start`])
/// and the keyword `word`, when it starts with that keyword.
fn after_keyword<'a>(text: &'a str, word: &str) -> Option<&'a str> {
    let start = statement_start(text)?;
    starts_with_keyword(start, word).then(|| &start[word.len()..])
}

/// The conflict words sqlparser's PostgreSQL dialect accepts after
/// `INSERT OR`: HeliosDB-Nano and HeliosDB-Lite parse with it.
const INSERT_CONFLICT_WORDS: &[&str] = &["REPLACE", "ROLLBACK", "ABORT", "FAIL", "IGNORE"];

/// Whether the statement `sql` (from its first word: [`statement_start`])
/// is an `INSERT` whose target does not follow an `INTO`. PostgreSQL
/// requires `INTO`; HeliosDB-Nano and HeliosDB-Lite (sqlparser's
/// PostgreSQL dialect) take it as optional, after an optional `OR REPLACE`
/// (`ROLLBACK`, `ABORT`, `FAIL`, `IGNORE`) or `REPLACE` and `OVERWRITE`, and
/// write the table named next: `INSERT orders SELECT … FROM staging` writes
/// `orders`, which the table extraction (a table after `INTO`) did not
/// find. Its scope is unknown, as is the scope of an `INSERT` whose next
/// word follows a comment the backends end in different places.
pub(crate) fn insert_without_into(sql: &str) -> bool {
    let Some(mut rest) = after_keyword(sql, "INSERT") else {
        return false;
    };
    if let Some(conflict) = after_keyword(rest, "OR") {
        match INSERT_CONFLICT_WORDS
            .iter()
            .find_map(|word| after_keyword(conflict, word))
        {
            Some(after) => rest = after,
            None => return true,
        }
    } else if let Some(after) = after_keyword(rest, "REPLACE") {
        rest = after;
    }
    if let Some(after) = after_keyword(rest, "OVERWRITE") {
        rest = after;
    }
    after_keyword(rest, "INTO").is_none()
}

/// The character that ends right before byte `at` of `text`: `None` at its
/// start, or when the bytes there are not a whole UTF-8 character.
fn char_before(text: &[u8], at: usize) -> Option<char> {
    let last = *text.get(at.checked_sub(1)?)?;
    if last.is_ascii() {
        return Some(char::from(last));
    }
    let start = (at.saturating_sub(4)..at)
        .rev()
        .find(|&i| text[i] & 0xC0 != 0x80)?;
    std::str::from_utf8(&text[start..at]).ok()?.chars().next()
}

/// The character that starts at byte `at` of `text`: `None` at its end, or
/// when no whole UTF-8 character starts there.
fn char_after(text: &[u8], at: usize) -> Option<char> {
    let first = *text.get(at)?;
    if first.is_ascii() {
        return Some(char::from(first));
    }
    let bytes = &text[at..(at + 4).min(text.len())];
    let valid = match std::str::from_utf8(bytes) {
        Ok(valid) => valid,
        Err(e) => std::str::from_utf8(&bytes[..e.valid_up_to()]).ok()?,
    };
    valid.chars().next()
}

/// Whether every backend the caches serve reads `c` as a character of an
/// unquoted identifier: an ASCII letter or digit, `_`, `$`, or a non-ASCII
/// letter. PostgreSQL reads any non-ASCII character as one, HeliosDB-Nano
/// and HeliosDB-Lite (sqlparser's PostgreSQL dialect) only an alphabetic
/// one, and a Unicode space (U+00A0, U+3000, …) as whitespace: next to any
/// other character a name may be a word of its own there.
fn continues_identifier_everywhere(c: char) -> bool {
    c.is_alphabetic() || c.is_ascii_digit() || c == '_' || c == '$'
}

/// The table names `pattern` captures in `text` (a statement's
/// [`strip_comments`] text): the query cache's and the edge cache's table
/// extraction. `pattern` matches a keyword (`FROM`, `JOIN`, …) as
/// group 1 and captures the table after it as group 2.
///
/// A keyword right after a letter (a non-ASCII one included) or `_` is the
/// end of an identifier on every backend (`valid_from`, `last_update`,
/// `data_table`, `ñfrom`), never a keyword: skipped. After any other
/// character it counts: a non-ASCII one that is not a letter (a Unicode
/// space, U+00A0 or U+3000, pasted before `FROM`) is part of the identifier
/// for PostgreSQL but whitespace for HeliosDB-Nano and HeliosDB-Lite, which
/// read the table after it. After a skipped one the search goes on from
/// inside it, so a keyword of several words (`INSERT INTO`) cannot hide the
/// one it holds. Every other match counts, and the search goes on
/// right after its keyword, not after the table it captured, since that
/// "table" may be the real keyword: `SELECT t.from FROM prices`,
/// `SELECT 1 AS from FROM prices`, `SELECT x$from FROM prices`. A keyword
/// after a digit or `$` is kept: it may follow a number (`1from`, which a
/// PostgreSQL before 15 reads as `1` and `FROM`) or a dollar-quoted literal
/// (`$$a$$FROM t`). A name only such a misreading finds widens invalidation,
/// which is safe; a table a keyword names is never lost.
///
/// `pattern` may let parentheses come before the table (`FROM (orders JOIN
/// items ON …)`, a parenthesized join): a word there that starts a subquery
/// or a `VALUES` list ([`SUBQUERY_WORDS`]) is skipped, never a table, so a
/// read like `FROM (SELECT f(1)) x` keeps an empty table set (which the edge
/// cache never stores) instead of a table `select`.
pub(crate) fn table_names<'a>(
    pattern: &'a regex::Regex,
    text: &'a str,
) -> impl Iterator<Item = &'a str> + 'a {
    table_matches(pattern, text).map(|(_, table)| table)
}

/// [`table_names`], each with the keyword (group 1) that named it: the
/// write invalidation's reading ([`skipped_target`]).
pub(crate) fn table_matches<'a>(
    pattern: &'a regex::Regex,
    text: &'a str,
) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
    let mut at = 0;
    std::iter::from_fn(move || loop {
        let caps = pattern.captures_at(text, at)?;
        let keyword = caps.get(1)?;
        let ends_identifier = char_before(text.as_bytes(), keyword.start())
            .is_some_and(|c| c.is_alphabetic() || c == '_');
        if ends_identifier {
            // Not a keyword: search on from inside it, so a keyword of
            // several words (`INSERT INTO`) cannot hide the one it holds
            // (`last_insert INTO t`). It starts with an ASCII letter.
            at = keyword.start() + 1;
            continue;
        }
        at = keyword.end();
        if let Some(table) = caps.get(2) {
            let parenthesized = text[keyword.end()..table.start()].contains('(');
            let subquery = parenthesized
                && SUBQUERY_WORDS
                    .iter()
                    .any(|w| table.as_str().eq_ignore_ascii_case(w));
            if !subquery {
                return Some((keyword.as_str(), table.as_str()));
            }
        }
    })
}

/// The conflict words sqlparser's PostgreSQL dialect reads after `UPDATE`
/// (`UPDATE OR IGNORE t`, `UPDATE REPLACE t`): the first word of its
/// conflict clause.
const UPDATE_CONFLICT_WORDS: &[&str] = &["OR", "REPLACE"];

/// The optional words of the write table pattern's `INSERT` keyword, and
/// `ONLY` (the query cache's and the edge cache's `TABLE_PATTERN`): never a
/// table there, unless the pattern stepped back over one.
const PATTERN_WORDS: &[&str] = &["INTO", "TABLE", "OVERWRITE", "OR", "REPLACE", "ONLY"];

/// Whether a [`table_matches`] match takes for the table a word that
/// sqlparser's PostgreSQL dialect, which HeliosDB-Nano and HeliosDB-Lite
/// parse with, reads before the target: an `UPDATE`'s conflict clause
/// (`UPDATE OR IGNORE kv`, `UPDATE REPLACE kv`, which those engines run as
/// a plain `UPDATE kv`) or Hive's `LOCAL` after an `INSERT`'s `INTO`
/// (`INSERT INTO LOCAL kv`, a plain insert into `kv` there). Such a write
/// changes the table named next, which the extraction did not read: its
/// scope is unknown (a table really named `replace` or `local`, which
/// PostgreSQL allows, then costs a whole-cache invalidation). PostgreSQL
/// rejects the clauses themselves. So is a match that took one of the
/// pattern's own words for the table ([`PATTERN_WORDS`]): it stepped back
/// over an optional word because the name after it could not be read
/// (`INSERT INTO "t" …` and `INSERT INTO été …` matched `INSERT` and the
/// table `into`).
pub(crate) fn skipped_target(keyword: &str, table: &str) -> bool {
    let k = keyword.as_bytes();
    if PATTERN_WORDS
        .iter()
        .any(|word| table.eq_ignore_ascii_case(word))
    {
        return true;
    }
    if k.len() >= 4 && k[k.len() - 4..].eq_ignore_ascii_case(b"INTO") {
        return table.eq_ignore_ascii_case("LOCAL");
    }
    keyword.eq_ignore_ascii_case("UPDATE")
        && UPDATE_CONFLICT_WORDS
            .iter()
            .any(|word| table.eq_ignore_ascii_case(word))
}

/// Words that start a parenthesized subquery or `VALUES` list after `FROM (`
/// or `JOIN (` (all reserved or, for `VALUES`, unusable there as a table
/// name): [`table_names`] never takes one for a table.
const SUBQUERY_WORDS: &[&str] = &["select", "values", "with", "table"];

/// A statement's cache fingerprint: its text with comments removed,
/// whitespace collapsed and literals replaced by `?`, every other word kept
/// as written, plus the literals that were replaced, in statement order.
/// Words are not case-folded: HeliosDB-Nano labels an output column as
/// written (`SELECT Name …` is labelled `Name`, `SELECT name …` `name`), so
/// two spellings of one statement can return different RowDescriptions; and
/// the proxy does not know the session's `client_encoding`, in which (SJIS,
/// BIG5, GBK, UHC, JOHAB, GB18030) a letter byte can belong to a multi-byte
/// character. Case variants therefore never share a key (only a hit-rate
/// cost). Two statements share a fingerprint text and literal list only if
/// PostgreSQL reads them the same way; two string literals that PostgreSQL
/// reads as one are kept apart from the same two on one line (see
/// [`STRING_CONTINUATION`]). Where HeliosDB-Nano reads a statement in a way
/// that depends on its whitespace or comments, they are kept: from a comment
/// or dollar quote it ends elsewhere ([`Piece::Rest`]), and throughout a
/// statement one of its spacing-exact rewrites (time travel, `SECURITY
/// DEFINER`, `AS IDENTITY`) may fire on ([`fingerprint`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Fingerprint {
    /// The normalized text.
    pub(crate) text: String,
    /// Each replaced literal's raw value: a string's text between its quotes,
    /// a dollar quote's body, or a number's text.
    pub(crate) params: Vec<String>,
    /// Each replaced literal's kind ([`KIND_STRING`], [`KIND_DOLLAR`] or
    /// [`KIND_NUMBER`]): `'1'` and `1` are different values.
    pub(crate) kinds: Vec<u8>,
    /// Byte offset in `text` of each literal's `?`, so a literal and an
    /// operator spelled `?` (jsonb) cannot trade places within one key.
    pub(crate) marks: Vec<u32>,
}

impl Fingerprint {
    /// Replace one literal with `?`.
    fn mask(&mut self, kind: u8, value: &str) {
        self.marks
            .push(u32::try_from(self.text.len()).unwrap_or(u32::MAX));
        self.text.push('?');
        self.params.push(value.to_string());
        self.kinds.push(kind);
    }

    /// Feed the literals (kind, value and position, in order) to `hasher`.
    pub(crate) fn hash_literals<H: Hasher>(&self, hasher: &mut H) {
        for ((kind, param), mark) in self.kinds.iter().zip(&self.params).zip(&self.marks) {
            kind.hash(hasher);
            param.hash(hasher);
            mark.hash(hasher);
        }
    }
}

/// What [`fingerprint`] writes, in place of the one space any other run of
/// whitespace and comments becomes, between two `'…'` literals that
/// PostgreSQL reads as one: a run of whitespace and `--` comments with a
/// newline in it continues a string (`'ab'` newline `'c'` is `'abc'`), while
/// the same two literals on one line, or with a block comment between them,
/// are a syntax error. Without it the valid and the rejected statement shared
/// an L2 and edge key, and the rejected one was answered with the other's
/// rows. Nothing else writes this byte right after a masked literal: other
/// whitespace collapses to a space, and every piece starts with a byte that
/// is not whitespace (a fingerprint kept verbatim, see [`fingerprint`], is
/// one whose words make every statement sharing it keep it verbatim too).
const STRING_CONTINUATION: char = '\n';

/// What HeliosDB-Nano's and HeliosDB-Lite's spacing-exact text rewrites
/// search a statement for, upper-cased: time travel (`AS OF`, `VERSIONS
/// BETWEEN`), the `SECURITY DEFINER` / `SECURITY INVOKER` removal and the
/// `AS IDENTITY` list removal ([`nano_rewrite_words`]). They read string
/// literals as text, and the time-travel ones comments too: Nano's planner
/// searches the raw statement for `AS OF` / `VERSIONS BETWEEN` (and the
/// clause after it, up to a keyword spelled with one space), and
/// HeliosDB-Lite cuts such a clause out of the raw text before parsing,
/// across the end of a comment (`-- as of now` newline `WHERE id = 1` loses
/// its `WHERE`). Each holds a space.
pub(crate) const NANO_REWRITE_TEXTS: &[&str] = &[
    "AS OF",
    "VERSIONS BETWEEN",
    "SECURITY DEFINER",
    "SECURITY INVOKER",
    "AS IDENTITY",
];

/// Whether `text` (a literal, quoted identifier, comment or verbatim rest),
/// upper-cased the way HeliosDB-Nano and HeliosDB-Lite do it (Unicode
/// `to_uppercase`, so `aſ of` is `AS OF`), holds one of
/// [`NANO_REWRITE_TEXTS`]. No character upper-cases to a space, so a text
/// without one never does (no scan, no allocation).
pub(crate) fn holds_nano_rewrite_text(text: &str) -> bool {
    if memchr::memchr(b' ', text.as_bytes()).is_none() {
        return false;
    }
    if text.is_ascii() {
        NANO_REWRITE_TEXTS
            .iter()
            .any(|w| crate::protocol::contains_ci(text, w))
    } else {
        let upper = text.to_uppercase();
        NANO_REWRITE_TEXTS.iter().any(|w| upper.contains(w))
    }
}

/// Whether `sql` (comments and literals included, any case) holds `pg_` or
/// `information_schema`. HeliosDB-Nano answers a `SELECT` naming one of the
/// catalogs it emulates (`pg_settings`, `pg_tables`, `pg_class`, …,
/// `information_schema.*`) from a router that matches the raw text, spacing
/// included: `WHERE name = 'x'` filters `pg_settings` and `WHERE name='x'`
/// or `WHERE name IN  ('x')` keeps every row, `tablename = 'x'` answers
/// `pg_tables` with five columns and `tablename='x'` with eight. Every
/// catalog name and psql signature it matches holds one of the two, its
/// psql signatures in literals too. One pass over the `_` bytes.
pub(crate) fn names_nano_catalog(sql: &[u8]) -> bool {
    memchr::memchr_iter(b'_', sql).any(|i| {
        (i >= 2 && sql[i - 2..i].eq_ignore_ascii_case(b"pg"))
            || (i >= 11
                && sql[i - 11..i].eq_ignore_ascii_case(b"information")
                && sql
                    .get(i + 1..i + 7)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"schema")))
    })
}

/// Whether a character continues a word for [`names_system_catalog`] on
/// every backend: [`continues_identifier_everywhere`] without `$`, so the
/// body of `$$pg_class$$` counts. A non-ASCII character that is not a
/// letter (a Unicode space) ends the word before it for HeliosDB-Nano and
/// HeliosDB-Lite.
fn catalog_word_char(c: char) -> bool {
    c != '$' && continues_identifier_everywhere(c)
}

/// Whether `sql` may name a system catalog relation, which no result cache
/// tier (L1, L2, L3, `[edge]`) stores or serves: a word starting with `pg_`
/// (the schema `pg_catalog`, an unqualified `pg_settings`,
/// `pg_stat_activity`, `pg_class`, …) or with `information_schema`, ASCII
/// case-insensitive, anywhere in the text: comments, literals
/// (`'pg_class'::regclass`) and quoted identifiers included (fail-closed; a
/// function such as `pg_backend_pid()` or a column named `pg_x` only costs
/// the hit). A word starts after anything but a letter, a digit or `_`: a
/// Unicode space too, which HeliosDB-Nano and HeliosDB-Lite read as
/// whitespace (`FROM\u{a0}pg_settings`). Any Unicode-escaped quoted
/// identifier (`U&"…"`) counts as well: it can spell a catalog name without
/// its bytes (`U&"pg\005fsettings"`, `U&"\0070g_settings"`). Such a read
/// depends on the session (`pg_settings` reflects `SET`), changes with DDL
/// that no write through the proxy announces, and HeliosDB-Nano answers
/// several of them from a router that matches the raw text. One pass over
/// the `&"` pairs and one over the `_` bytes.
pub fn names_system_catalog(sql: &[u8]) -> bool {
    let word_start = |at: usize| !char_before(sql, at).is_some_and(catalog_word_char);
    unicode_escaped_identifier(sql)
        || memchr::memchr_iter(b'_', sql).any(|i| {
            (i >= 2 && sql[i - 2..i].eq_ignore_ascii_case(b"pg") && word_start(i - 2))
                || (i >= 11
                    && sql[i - 11..i].eq_ignore_ascii_case(b"information")
                    && sql
                        .get(i + 1..i + 7)
                        .is_some_and(|s| s.eq_ignore_ascii_case(b"schema"))
                    && word_start(i - 11))
        })
}

/// Whether `text` holds `first`, a run of one or more whitespace bytes
/// ([`is_space`]), then `second` (both lower-case ASCII words), compared
/// ASCII case-insensitively as plain substrings (`has offset` holds `as`
/// and `of`).
fn holds_spaced_words(text: &[u8], first: &[u8], second: &[u8]) -> bool {
    let mut at = 0;
    while let Some(found) = memchr::memchr2(first[0], first[0].to_ascii_uppercase(), &text[at..]) {
        let start = at + found;
        at = start + 1;
        let gap = start + first.len();
        if !text
            .get(start..gap)
            .is_some_and(|w| w.eq_ignore_ascii_case(first))
        {
            continue;
        }
        let next = run(text, gap, is_space);
        if next > gap
            && text
                .get(next..next + second.len())
                .is_some_and(|w| w.eq_ignore_ascii_case(second))
        {
            return true;
        }
    }
    false
}

/// Whether `text` holds `AS OF` or `VERSIONS BETWEEN` with any run of
/// whitespace between the words, ASCII case-insensitive; a non-ASCII text
/// is also compared upper-cased the way HeliosDB-Nano and HeliosDB-Lite do it
/// (`aſ of` is `AS OF`).
fn holds_time_travel_words(text: &str) -> bool {
    let check = |t: &[u8]| {
        holds_spaced_words(t, b"as", b"of") || holds_spaced_words(t, b"versions", b"between")
    };
    check(text.as_bytes()) || (!text.is_ascii() && check(text.to_uppercase().as_bytes()))
}

/// The [`Fingerprint`] of `sql` with every run of whitespace and every
/// comment kept as written, and a dollar quote's delimiters too (only
/// literal values are folded, with their kind and position): two statements
/// share it only when they differ in literal values alone, so their keys
/// (which hash the literals) are equal only for the same text. The L2 and
/// edge keys on a HeliosDB backend, whose engines
/// read comments and spacing before they parse: HeliosDB-Lite's `SIMILAR TO`
/// and `COLLATE` rewrites track quotes without knowing comments (a quote in
/// one flips what they take for a literal) and match one exact spacing,
/// HeliosDB-Nano's point-lookup fast path declines any statement holding a
/// comment and looks its table up as written while its planner folds the
/// name, and its empty-projection rewrite matches `select from ` exactly.
pub(crate) fn fingerprint_verbatim(sql: &str) -> Fingerprint {
    fingerprint_with(sql, true).unwrap_or_default()
}

/// The [`Fingerprint`] of `sql`. Its whitespace and comments are kept
/// verbatim instead when one of HeliosDB-Nano's spacing-exact rewrites may
/// fire on two of its words ([`nano_rewrite_words`]: time travel, `SECURITY
/// DEFINER` / `INVOKER`, `AS IDENTITY`), or on a literal, quoted identifier
/// or comment ([`holds_nano_rewrite_text`]), and when it names a catalog
/// HeliosDB-Nano answers by matching the raw text ([`names_nano_catalog`]),
/// since what the backend reads depends on them. On a HeliosDB backend the
/// caches use [`fingerprint_verbatim`] for every statement.
pub(crate) fn fingerprint(sql: &str) -> Fingerprint {
    fingerprint_with(sql, false).unwrap_or_else(|| fingerprint_verbatim(sql))
}

/// [`fingerprint`] with every run of whitespace and comments collapsed
/// (`None` when what HeliosDB-Nano reads may depend on them: two words
/// separated by one may set off one of its spacing-exact rewrites, a
/// literal, quoted identifier or comment holds one of their texts, or the
/// statement names a catalog it emulates), or with them all kept
/// `verbatim`: then two statements share a fingerprint only if they differ
/// in literal values alone.
fn fingerprint_with(sql: &str, verbatim: bool) -> Option<Fingerprint> {
    if !verbatim && names_nano_catalog(sql.as_bytes()) {
        return None;
    }
    let mut fp = Fingerprint {
        text: String::with_capacity(sql.len()),
        ..Fingerprint::default()
    };
    // Whether whitespace or comments separate this piece from the last one,
    // and, when the last one was a `'…'` literal, whether that run has a
    // newline and a block comment (the string-continuation rule).
    let mut space = false;
    let mut after_string = false;
    let mut newline = false;
    let mut block_comment = false;
    // The last piece, when it was a word (or other text).
    let mut last_text: Option<&str> = None;
    for (_, piece) in Pieces::new(sql) {
        match piece {
            Piece::Space(run) | Piece::Comment(run) if verbatim => {
                fp.text.push_str(run);
                continue;
            }
            Piece::Space(run) => {
                space = true;
                newline |= after_string && run.bytes().any(|c| matches!(c, b'\n' | b'\r'));
                continue;
            }
            Piece::Comment(comment) => {
                if holds_nano_rewrite_text(comment) {
                    return None;
                }
                space = true;
                block_comment |= !comment.starts_with("--");
                continue;
            }
            Piece::Str(text) | Piece::Dollar(text, _) | Piece::Ident(text) | Piece::Rest(text)
                if !verbatim && holds_nano_rewrite_text(text) =>
            {
                return None;
            }
            _ => {}
        }
        let separated = std::mem::take(&mut space);
        if let (Piece::Text(text), Some(before)) = (piece, last_text) {
            if separated && nano_rewrite_words(before, text) {
                return None;
            }
        }
        if separated && !fp.text.is_empty() {
            let opens_string = match piece {
                Piece::Str(_) => true,
                Piece::Rest(rest) => rest.starts_with('\''),
                _ => false,
            };
            fp.text.push(
                if after_string && newline && !block_comment && opens_string {
                    STRING_CONTINUATION
                } else {
                    ' '
                },
            );
        }
        after_string = matches!(piece, Piece::Str(_));
        newline = false;
        block_comment = false;
        last_text = match piece {
            Piece::Text(text) => Some(text),
            _ => None,
        };
        match piece {
            Piece::Str(text) => fp.mask(KIND_STRING, &text[1..text.len() - 1]),
            // Verbatim, the delimiters stay: HeliosDB-Nano and HeliosDB-Lite
            // tokenize with sqlparser, which loses a tagged quote's end when
            // its body ends in `$` and a prefix of the tag (`$ab$x$a$ab$`
            // fails there, `$$x$a$$` reads `x$a`).
            Piece::Dollar(text, tag) if verbatim => {
                fp.text.push_str(&text[..tag]);
                fp.mask(KIND_DOLLAR, &text[tag..text.len() - tag]);
                fp.text.push_str(&text[text.len() - tag..]);
            }
            Piece::Dollar(text, tag) => fp.mask(KIND_DOLLAR, &text[tag..text.len() - tag]),
            Piece::Number(text) => fp.mask(KIND_NUMBER, text),
            Piece::Text(text) | Piece::Ident(text) | Piece::Rest(text) => fp.text.push_str(text),
            Piece::Space(_) | Piece::Comment(_) => {}
        }
    }
    Some(fp)
}

/// The body of every `helios:` comment outside quotes, in order: the text
/// after `helios:` up to the end of a block comment, or of the line for a
/// `--` comment. Only a comment whose text starts with `helios:` (after
/// whitespace) counts, so a hint inside a string literal or a quoted
/// identifier is never read. Nothing is read from a backend-ambiguous rest
/// ([`Piece::Rest`]): no result cache tier stores or serves such a read.
#[cfg(feature = "query-cache")]
pub(crate) fn helios_hint_bodies(sql: &str) -> impl Iterator<Item = &str> {
    Pieces::new(sql).filter_map(|(_, piece)| match piece {
        Piece::Comment(comment) => comment
            .strip_prefix("--")
            .unwrap_or_else(|| &comment[2..comment.len() - 2])
            .trim_start()
            .strip_prefix("helios:"),
        _ => None,
    })
}

/// What a canonical-form token is.
#[cfg(feature = "query-cache")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenKind {
    /// An unquoted word (keyword or identifier), ASCII-lower-cased.
    Word,
    /// A quoted identifier, verbatim with its quotes.
    Ident,
    /// A `'…'` string literal, verbatim with its quotes.
    Str,
    /// A dollar-quoted literal, verbatim.
    Dollar,
    /// A number, verbatim.
    Number,
    /// A `$n` parameter reference.
    Param,
    /// An operator (a run of operator characters; `!=` is spelled `<>`).
    Op,
    /// Punctuation: `( ) [ ] , ; . :`, `::` and `..`, or any other single
    /// byte.
    Punct,
}

/// One token of a statement, as the semantic tier compares them.
#[cfg(feature = "query-cache")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) text: String,
    /// A string or quoted identifier glued to a preceding identifier byte or
    /// `&` (`E'…'`, `B'…'`, `U&'…'`, `U&"…"`): its meaning is not its text.
    pub(crate) prefixed: bool,
    /// An unquoted word as written, when that is not `text` (it has
    /// upper-case letters). PostgreSQL folds it, but a backend may not:
    /// HeliosDB-Nano labels an output column with its alias or column name
    /// as written.
    pub(crate) written: Option<String>,
}

#[cfg(feature = "query-cache")]
impl Token {
    /// Whether this is the unquoted word `w` (lower-case).
    pub(crate) fn is_word(&self, w: &str) -> bool {
        self.kind == TokenKind::Word && self.text == w
    }

    /// Whether this is the punctuation or operator `p`.
    pub(crate) fn is_punct(&self, p: &str) -> bool {
        matches!(self.kind, TokenKind::Punct | TokenKind::Op) && self.text == p
    }
}

/// The tokens of `sql` with comments and whitespace dropped (they only
/// separate tokens), unquoted words ASCII-lower-cased (their spelling kept
/// aside) and everything else verbatim. Each comment is dropped only when
/// `inert` (given the whole comment, delimiters included) says it may be: a
/// backend may read a comment's text (HeliosDB-Nano's planner and
/// HeliosDB-Lite's rewrites search the raw statement). `Err` when a token is
/// unterminated, a backslash precedes a quote inside a `'…'` literal (the two
/// `standard_conforming_strings` readings disagree), or `inert` refuses a
/// comment.
#[cfg(feature = "query-cache")]
pub(crate) fn tokens(sql: &str, mut inert: impl FnMut(&str) -> bool) -> Result<Vec<Token>, ()> {
    const OPERATOR: &[u8] = b"+-*/<>=~!@#%^&|`?";
    let b = sql.as_bytes();
    let mut out: Vec<Token> = Vec::new();
    // End offset of the previous non-separator piece.
    let mut prev_end = usize::MAX;
    for (start, piece) in Pieces::new(sql) {
        let adjacent = prev_end == start;
        let glued = start
            .checked_sub(1)
            .map(|i| b[i])
            .is_some_and(|c| ident_cont(c) || c == b'&');
        let (kind, text, prefixed) = match piece {
            Piece::Space(_) => continue,
            Piece::Comment(comment) if inert(comment) => continue,
            Piece::Comment(_) => return Err(()),
            Piece::Rest(_) => return Err(()),
            Piece::Ident(t) => (TokenKind::Ident, t.to_string(), glued),
            Piece::Str(t) => (TokenKind::Str, t.to_string(), glued),
            Piece::Dollar(t, _) => (TokenKind::Dollar, t.to_string(), false),
            Piece::Number(t) => (TokenKind::Number, t.to_string(), false),
            Piece::Text(t) => {
                let c = t.as_bytes()[0];
                if ident_start(c) {
                    (TokenKind::Word, t.to_ascii_lowercase(), false)
                } else if c.is_ascii_digit()
                    || (c == b'.' && t.as_bytes().get(1).is_some_and(u8::is_ascii_digit))
                {
                    // A number running into an identifier (`1x`, `.5x`).
                    (TokenKind::Number, t.to_string(), false)
                } else if c == b'$' && t.len() > 1 {
                    (TokenKind::Param, t.to_string(), false)
                } else if OPERATOR.contains(&c) {
                    match out.last_mut() {
                        Some(last) if adjacent && last.kind == TokenKind::Op => {
                            last.text.push_str(t);
                            prev_end = start + t.len();
                            continue;
                        }
                        _ => (TokenKind::Op, t.to_string(), false),
                    }
                } else if c == b':' {
                    match out.last_mut() {
                        Some(last)
                            if adjacent && last.kind == TokenKind::Punct && last.text == ":" =>
                        {
                            last.text.push(':');
                            prev_end = start + 1;
                            continue;
                        }
                        _ => (TokenKind::Punct, t.to_string(), false),
                    }
                } else {
                    (TokenKind::Punct, t.to_string(), false)
                }
            }
        };
        prev_end = start + piece_len(piece);
        let written = match piece {
            Piece::Text(t) if kind == TokenKind::Word && t != text => Some(t.to_string()),
            _ => None,
        };
        out.push(Token {
            kind,
            text,
            prefixed,
            written,
        });
    }
    for token in &mut out {
        if token.kind == TokenKind::Op && token.text == "!=" {
            token.text = "<>".to_string();
        }
    }
    Ok(out)
}

/// The byte length of a piece.
#[cfg(feature = "query-cache")]
fn piece_len(piece: Piece<'_>) -> usize {
    match piece {
        Piece::Space(t)
        | Piece::Comment(t)
        | Piece::Text(t)
        | Piece::Ident(t)
        | Piece::Str(t)
        | Piece::Dollar(t, _)
        | Piece::Number(t)
        | Piece::Rest(t) => t.len(),
    }
}

/// Tables whose reads neither result cache may serve or store
/// (`[cache] exclude_tables` and `[[cache.tables]] exclude = true`).
///
/// [`TableExclusions::matches`] is deliberately fail-closed: it searches the
/// raw statement text, comments and literals included, for each name as a
/// whole identifier (case-insensitively). That catches a schema-qualified,
/// quoted or comma-joined use and a name hidden behind a comment, at the cost
/// of also excluding a read that only mentions the name as a column, an alias,
/// a literal or in a comment. A Unicode-escaped quoted identifier (`U&"…"`)
/// can spell a name without its bytes, so any read with one is excluded too.
/// A name of 60 bytes or more also matches as the start of a longer
/// identifier, which PostgreSQL truncates to 63 bytes (clipping a multi-byte
/// character). Views and functions over an excluded table are not detected:
/// they must be listed too.
#[derive(Debug, Clone)]
pub struct TableExclusions {
    scan: IdentifierScan,
}

impl TableExclusions {
    /// Exclusions for `names` (ASCII-lower-cased and de-duplicated).
    pub fn new<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut lower: Vec<String> = names
            .into_iter()
            .map(|n| n.as_ref().to_ascii_lowercase())
            .collect();
        lower.sort();
        lower.dedup();
        Self {
            scan: IdentifierScan::new(lower),
        }
    }

    /// Whether `sql` mentions one of the names as a whole identifier: the
    /// characters around the match do not continue an identifier on every
    /// backend (`[A-Za-z0-9_$]` or a non-ASCII letter: a Unicode space next
    /// to the name, which HeliosDB-Nano and HeliosDB-Lite read as
    /// whitespace, leaves it whole); for a name of 60 bytes or more only the
    /// character before it counts (PostgreSQL truncates a longer identifier
    /// to 63 bytes). Any `U&"` (a Unicode-escaped quoted identifier, which can
    /// name a table without its bytes: `U&"s\0065crets"`) matches as well.
    pub fn matches(&self, sql: &str) -> bool {
        if self.scan.names().is_empty() {
            return false;
        }
        let b = sql.as_bytes();
        unicode_escaped_identifier(b) || self.scan.found(b).next().is_some()
    }

    /// The excluded names, lower-cased and sorted.
    pub fn names(&self) -> &[String] {
        self.scan.names()
    }

    /// Number of excluded names.
    pub fn len(&self) -> usize {
        self.scan.names().len()
    }

    /// Whether no name is excluded.
    pub fn is_empty(&self) -> bool {
        self.scan.names().is_empty()
    }
}

/// The longest identifier PostgreSQL keeps (`NAMEDATALEN - 1` bytes): it
/// truncates a longer one to this length, clipped back to a character
/// boundary.
pub(crate) const MAX_IDENTIFIER_BYTES: usize = 63;

/// The shortest identifier PostgreSQL's truncation can produce. It cuts an
/// identifier longer than [`MAX_IDENTIFIER_BYTES`] to that length, clipped
/// back to a character boundary, which drops up to 3 more bytes of a
/// multi-byte character: a truncated identifier is 60 to 63 bytes long. A
/// name at least this long may therefore be what a longer identifier in the
/// text means, so [`IdentifierScan`] matches it without a boundary after it.
pub(crate) const TRUNCATED_IDENTIFIER_MIN_BYTES: usize = 60;

/// `name` as PostgreSQL stores it: cut to [`MAX_IDENTIFIER_BYTES`], clipped
/// back to a character boundary. The table extraction keys a table this
/// way, so a read that spells a longer name and a write (or a decoded
/// change, which names the catalog's truncated relation) name one table.
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

/// A fail-closed search of raw statement text for a list of names, each as
/// a whole identifier (case-insensitively): comments and literals are
/// searched too, so a schema-qualified, quoted, comma-joined or
/// comment-hidden use is found, at the cost of also finding a column, alias,
/// literal or comment of the same name. Shared by [`TableExclusions`] and
/// the per-table TTLs ([`ShorterTtls`]). One pass over the text, whatever
/// the number of names (an Aho-Corasick automaton over all of them).
#[derive(Debug, Clone)]
pub(crate) struct IdentifierScan {
    names: Vec<String>,
    /// Every name, matched ASCII-case-insensitively and overlapping (so a
    /// name inside a longer one, `pay` in `payroll`, cannot hide the longer
    /// one's match). `None` without names, or when it could not be built
    /// (only for pattern sets far beyond any table list): every name then
    /// counts as found (fail-closed).
    automaton: Option<aho_corasick::AhoCorasick>,
}

impl IdentifierScan {
    /// A scan for `names` (ASCII-lower-cased; empty names dropped), in the
    /// given order: [`Self::found`] yields indices into it.
    pub(crate) fn new<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let names: Vec<String> = names
            .into_iter()
            .map(|n| n.as_ref().to_ascii_lowercase())
            .filter(|n| !n.is_empty())
            .collect();
        let automaton = if names.is_empty() {
            None
        } else {
            match aho_corasick::AhoCorasick::builder()
                .ascii_case_insensitive(true)
                .build(&names)
            {
                Ok(automaton) => Some(automaton),
                Err(error) => {
                    tracing::warn!(
                        names = names.len(),
                        %error,
                        "cache table-name scan could not be built: every read counts as \
                         naming every configured table (fail-closed)"
                    );
                    None
                }
            }
        };
        Self { names, automaton }
    }

    /// The names, in order.
    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    /// Indices of the names that `sql` (the statement text, any case)
    /// mentions as a whole identifier, in text order and once per mention:
    /// the character before the match does not continue an identifier on
    /// every backend ([`continues_identifier_everywhere`]: `[A-Za-z0-9_$]` or
    /// a non-ASCII letter), nor does the character after it, unless the name
    /// is at least [`TRUNCATED_IDENTIFIER_MIN_BYTES`] long (then a longer
    /// identifier may be truncated to it). A non-ASCII character that is not
    /// a letter (a Unicode space, U+00A0 or U+3000) is part of the identifier
    /// for PostgreSQL but whitespace for HeliosDB-Nano and HeliosDB-Lite, so
    /// the name next to it is found (fail-closed). Without a usable automaton
    /// every index, once.
    pub(crate) fn found<'a>(&'a self, sql: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        let matches = self
            .automaton
            .as_ref()
            .and_then(|a| a.try_find_overlapping_iter(sql).ok());
        let all = matches.is_none().then_some(0..self.names.len());
        let whole = move |m: &aho_corasick::Match| {
            !char_before(sql, m.start()).is_some_and(continues_identifier_everywhere)
                && (m.len() >= TRUNCATED_IDENTIFIER_MIN_BYTES
                    || !char_after(sql, m.end()).is_some_and(continues_identifier_everywhere))
        };
        all.into_iter().flatten().chain(
            matches
                .into_iter()
                .flatten()
                .filter(whole)
                .map(|m| m.pattern().as_usize()),
        )
    }
}

/// Per-table TTLs shorter than a cache's own TTL, applied fail-closed from a
/// statement's text: a configured name anywhere in it, as a whole identifier
/// ([`IdentifierScan`]: comments and literals included, a name of 60 bytes or
/// more also as the start of a longer identifier, which PostgreSQL
/// truncates), gives the read that table's TTL. A Unicode-escaped quoted
/// identifier (`U&"…"`) can spell any name, so it gives the shortest. The
/// query cache applies it to a read whose relations were not all
/// recognised; the edge cache to every read, as its only table-TTL check (a
/// table the statement names is a whole identifier in its text, or a longer
/// one PostgreSQL truncates, so the scan finds it).
#[derive(Debug, Clone)]
pub(crate) struct ShorterTtls {
    scan: IdentifierScan,
    /// Each scanned name's TTL, in the scan's order.
    ttls: Vec<std::time::Duration>,
}

impl ShorterTtls {
    /// The `(name, TTL)` entries with a TTL shorter than `below` (empty
    /// names dropped); `None` when there is none.
    pub(crate) fn new<I, S>(entries: I, below: std::time::Duration) -> Option<Self>
    where
        I: IntoIterator<Item = (S, std::time::Duration)>,
        S: AsRef<str>,
    {
        let (names, ttls): (Vec<S>, Vec<std::time::Duration>) = entries
            .into_iter()
            .filter(|(name, ttl)| !name.as_ref().is_empty() && *ttl < below)
            .unzip();
        (!names.is_empty()).then(|| Self {
            scan: IdentifierScan::new(names),
            ttls,
        })
    }

    /// The shortest TTL of a configured name `sql` mentions, if any.
    pub(crate) fn min_in(&self, sql: &str) -> Option<std::time::Duration> {
        let sql = sql.as_bytes();
        if unicode_escaped_identifier(sql) {
            return self.ttls.iter().copied().min();
        }
        self.scan.found(sql).map(|i| self.ttls[i]).min()
    }
}

/// Whether `sql` (any case) holds a Unicode-escaped quoted identifier
/// (`U&"…"`), which can spell any name without its bytes
/// (`U&"s\0065crets"`): a fail-closed name scan must treat it as a match.
pub(crate) fn unicode_escaped_identifier(sql: &[u8]) -> bool {
    memchr::memmem::find_iter(sql, b"&\"")
        .any(|at| at > 0 && sql[at - 1].eq_ignore_ascii_case(&b'u'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    /// The cache key the normalizer derives: text, then the literals.
    fn key(sql: &str) -> u64 {
        let fp = fingerprint(sql);
        let mut h = DefaultHasher::new();
        fp.text.hash(&mut h);
        fp.hash_literals(&mut h);
        h.finish()
    }

    #[test]
    fn comment_markers_inside_literals_do_not_truncate() {
        let fp = fingerprint("SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 7");
        assert_eq!(
            fp.text,
            "SELECT * FROM posts WHERE title ILIKE ? AND owner_id = ?"
        );
        assert_eq!(fp.params, ["%a -- b%", "7"]);
        let fp = fingerprint("SELECT '/* x */', $q$x$q$, \"col--x\" FROM t");
        assert_eq!(fp.text, "SELECT ?, ?, \"col--x\" FROM t");
        assert_eq!(fp.params, ["/* x */", "x"]);
        assert_eq!(fp.kinds, [KIND_STRING, KIND_DOLLAR]);
        // A dollar quote holding `--` is not truncated either: HeliosDB-Nano's
        // comment stripper (which knows no dollar quotes) reads a comment
        // there, so it and the rest are kept as written.
        let fp = fingerprint("SELECT '/* x */', $$--$$, \"col--x\" FROM t");
        assert_eq!(fp.text, "SELECT ?, $$--$$, \"col--x\" FROM t");
        assert_eq!(fp.params, ["/* x */"]);
        // The pre-C-07 normalizer truncated both at the `--` and gave them
        // the same key (and the same L2 slot).
        let seven = "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 7";
        let eight = "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 8";
        assert_eq!(fingerprint(seven).text, fingerprint(eight).text);
        assert_ne!(fingerprint(seven).params, fingerprint(eight).params);
        assert_ne!(key(seven), key(eight));
    }

    /// Every word keeps its spelling. HeliosDB-Nano labels an output column
    /// as written, so `SELECT Name …` and `SELECT name …` return different
    /// RowDescriptions: upper-casing every word gave them one L2 and edge key
    /// and served one client the other's column names. With
    /// `client_encoding` SJIS, `C2 83 41` (U+0083 `A` read as UTF-8) and
    /// `C2 83 61` are two different identifiers, and both valid UTF-8.
    #[test]
    fn words_keep_their_spelling() {
        assert_eq!(
            fingerprint("SELECT Name FROM users WHERE id = 1").text,
            "SELECT Name FROM users WHERE id = ?"
        );
        for (a, b) in [
            (
                "SELECT Name FROM users WHERE id = 1",
                "SELECT name FROM users WHERE id = 1",
            ),
            (
                "SELECT name AS Total FROM users WHERE id = 1",
                "SELECT name AS total FROM users WHERE id = 1",
            ),
            ("SELECT COUNT(*) FROM t", "SELECT count(*) FROM t"),
            ("SELECT naive_A FROM t", "select NAIVE_a from T"),
        ] {
            assert_ne!(key(a), key(b), "{a} vs {b}");
        }
        let upper = "SELECT id FROM t WHERE \u{83}A = 1";
        let lower = "SELECT id FROM t WHERE \u{83}a = 1";
        assert_eq!(
            fingerprint(upper).text,
            "SELECT id FROM t WHERE \u{83}A = ?"
        );
        assert_ne!(key(upper), key(lower));
        assert_ne!(
            key("SELECT na\u{ef}ve_A FROM t"),
            key("SELECT na\u{ef}ve_a FROM t")
        );
    }

    #[test]
    fn comments_become_one_space() {
        assert_eq!(
            fingerprint("SELECT v FROM/**/secrets").text,
            "SELECT v FROM secrets"
        );
        assert_eq!(
            strip_comments("SELECT v FROM/**/secrets"),
            "SELECT v FROM secrets"
        );
        assert_ne!(key("SELECT a/**/b FROM t"), key("SELECT ab FROM t"));
        assert_eq!(
            fingerprint("SELECT 1 /* a */ FROM t -- end").text,
            "SELECT ? FROM t"
        );
        assert_eq!(
            strip_comments("SELECT 1 /* a */ FROM t /**/ -- x\r\n"),
            "SELECT 1   FROM t    \r\n"
        );
        // A nested comment is where PostgreSQL and HeliosDB-Nano part: the
        // rest is kept verbatim (see `nano_comment_readings_keep_the_rest_verbatim`).
        assert_eq!(
            fingerprint("SELECT 1 /* a /* nested */ still */ FROM t -- end").text,
            "SELECT ? /* a /* nested */ still */ FROM t -- end"
        );
        assert!(matches!(
            strip_comments("SELECT v FROM t WHERE x = 1"),
            Cow::Borrowed(_)
        ));
        // Markers only inside literals: nothing to strip, nothing copied.
        assert!(matches!(
            strip_comments("SELECT v FROM t WHERE x = '--'"),
            Cow::Borrowed(_)
        ));
        // Whitespace runs and comments collapse to one space.
        assert_eq!(
            fingerprint("select  *\n  from   users\twhere id=1").text,
            fingerprint("select * from users where id=1").text
        );
    }

    #[test]
    fn literals_keep_statement_order_and_kind() {
        assert_ne!(
            key("SELECT v FROM t WHERE a = '1' AND b = 2"),
            key("SELECT v FROM t WHERE a = 2 AND b = '1'")
        );
        assert_ne!(key("SELECT '1' FROM t"), key("SELECT 1 FROM t"));
        let fp = fingerprint("SELECT * FROM users WHERE id = 123 AND name = 'John'");
        assert_eq!(fp.params, ["123", "John"]);
        assert_eq!(fp.kinds, [KIND_NUMBER, KIND_STRING]);
        assert_eq!(
            fingerprint("SELECT * FROM t WHERE p < 99.99 AND r > 4.5 AND v = 1e10").params,
            ["99.99", "4.5", "1e10"]
        );
        // A jsonb `?` operator and a literal cannot trade places.
        assert_ne!(
            key("SELECT 1 FROM t WHERE x ? 'k'"),
            key("SELECT 1 FROM t WHERE x 'k' ?")
        );
    }

    /// PostgreSQL reads two `'…'` literals separated by whitespace (and `--`
    /// comments) with a newline in it as one (`'ab'` newline `'c'` is
    /// `'abc'`), and rejects the same two on one line or with a block comment
    /// between them. Both used to be `name = ? ?` with the params `ab`, `c`:
    /// the rejected statement was answered from the valid one's L2 or edge
    /// entry instead of with its ErrorResponse.
    #[test]
    fn continued_strings_never_share_a_key_with_rejected_ones() {
        let continued = [
            "SELECT id FROM t WHERE name = 'ab'\n'c'",
            "SELECT id FROM t WHERE name = 'ab'  \r\n\t 'c'",
            "SELECT id FROM t WHERE name = 'ab' -- more\n'c'",
            "SELECT id FROM t WHERE name = 'ab'\n-- more\n  'c'",
            "SELECT id FROM t WHERE name = E'ab'\n'c'",
            // PostgreSQL 16+ (and HeliosDB-Nano) read `\v` as whitespace,
            // never as a newline.
            "SELECT id FROM t WHERE name = 'ab'\n\x0b'c'",
            "SELECT id FROM t WHERE name = 'ab'\x0b\n'c'",
        ];
        let rejected = [
            "SELECT id FROM t WHERE name = 'ab' 'c'",
            "SELECT id FROM t WHERE name = 'ab'/**/'c'",
            "SELECT id FROM t WHERE name = 'ab'/**/\n'c'",
            "SELECT id FROM t WHERE name = 'ab'\n/* x */\n'c'",
            "SELECT id FROM t WHERE name = E'ab' 'c'",
            "SELECT id FROM t WHERE name = 'ab' \x0b'c'",
            "SELECT id FROM t WHERE name = 'ab'\x0b 'c'",
        ];
        for (a, b) in continued.iter().zip(&rejected) {
            assert_ne!(fingerprint(a).text, fingerprint(b).text, "{a:?} vs {b:?}");
            assert_ne!(key(a), key(b), "{a:?} vs {b:?}");
        }
        // Every way of continuing reads alike, and so does every rejected
        // form; the literals keep their own values.
        for sql in &continued[1..4] {
            assert_eq!(key(sql), key(continued[0]), "{sql:?}");
        }
        for sql in &rejected[1..4] {
            assert_eq!(key(sql), key(rejected[0]), "{sql:?}");
        }
        // A `\v` used to be a word of its own, which hid the newline next to
        // it: the `\v` pairs shared `name = ? \x0b?`.
        for sql in &continued[5..] {
            assert_eq!(key(sql), key(continued[0]), "{sql:?}");
        }
        for sql in &rejected[5..] {
            assert_eq!(key(sql), key(rejected[0]), "{sql:?}");
        }
        assert_eq!(fingerprint(continued[0]).params, ["ab", "c"]);
        // Only two literals: a newline before a number or a dollar quote is
        // one space.
        assert_eq!(
            fingerprint("SELECT 'a'\n1, 'b'\n$$c$$, 1\n'd'").text,
            "SELECT ? ?, ? ?, ? ?"
        );
        // The verbatim rest after an ambiguous literal is kept apart too.
        assert_ne!(
            key("SELECT 'ab'\n'c\\' FROM t"),
            key("SELECT 'ab' 'c\\' FROM t")
        );
    }

    #[test]
    fn dollar_literals_and_quoted_identifiers_keep_case() {
        assert_ne!(
            key("SELECT v FROM t WHERE n = $$alice$$"),
            key("SELECT v FROM t WHERE n = $$ALICE$$")
        );
        assert_eq!(fingerprint("SELECT $q$a$q$").params, ["a"]);
        assert_ne!(
            key("SELECT v FROM \"Orders\""),
            key("SELECT v FROM \"ORDERS\"")
        );
        assert_ne!(
            key("SELECT \"my  col\" FROM t"),
            key("SELECT \"my col\" FROM t")
        );
        assert_eq!(
            fingerprint("select \"MiXed\" from t").text,
            "select \"MiXed\" from t"
        );
        assert_eq!(fingerprint("select é from café").text, "select é from café");
    }

    #[test]
    fn ambiguous_backslash_keeps_the_rest_verbatim() {
        let x = fingerprint(r"SELECT v FROM t WHERE p = 'C:\' -- x");
        let y = fingerprint(r"SELECT v FROM t WHERE p = 'C:\' -- y");
        assert_ne!(x, y);
        assert_eq!(x.text, r"SELECT v FROM t WHERE p = 'C:\' -- x");
        assert!(x.params.is_empty());
        // Literals before the divergence are still folded.
        let fp = fingerprint(r"SELECT 'a', 1 FROM t WHERE p = 'x\'' AND q = 2");
        assert_eq!(fp.text, r"SELECT ?, ? FROM t WHERE p = 'x\'' AND q = 2");
        assert_eq!(fp.params, ["a", "1"]);
        // strip_comments keeps the comment after the divergence.
        assert_eq!(
            strip_comments(r"SELECT 1 /* c */ FROM t WHERE p = 'C:\' -- x"),
            r"SELECT 1   FROM t WHERE p = 'C:\' -- x"
        );
        // A backslash elsewhere in a literal is ordinary text.
        assert_eq!(fingerprint(r"SELECT 'a\b' FROM t").params, [r"a\b"]);
        // Unterminated tokens are kept verbatim too.
        assert_eq!(
            fingerprint("SELECT 'open FROM t").text,
            "SELECT 'open FROM t"
        );
        assert_eq!(fingerprint("SELECT 1 /* open").text, "SELECT ? /* open");
        assert_eq!(fingerprint("SELECT $a$ open").text, "SELECT $a$ open");
    }

    /// A number is one literal where PostgreSQL's lexer reads one, so a
    /// space inside what it reads as one token changes the key: `1.e3` is
    /// 1000, `1. e3` is `1.` labelled `e3`.
    #[test]
    fn numbers_lex_as_postgresql_reads_them() {
        for number in [
            "1",
            "1.",
            "1.5",
            ".5",
            "1.e3",
            "1.E+3",
            ".5e-2",
            "1e10",
            "1_000",
            "1_000.5_1",
            "1_000.e1_0",
        ] {
            let fp = fingerprint(&format!("SELECT {number} FROM t"));
            assert_eq!(fp.text, "SELECT ? FROM t", "{number}");
            assert_eq!(fp.params, [number]);
        }
        let label = fingerprint("SELECT 1. e3 FROM t");
        assert_eq!(label.text, "SELECT ? e3 FROM t");
        assert_eq!(label.params, ["1."]);
        assert_ne!(key("SELECT 1.e3 FROM t"), key("SELECT 1. e3 FROM t"));
        assert_ne!(key("SELECT 1.e3 FROM t"), key("SELECT 1./**/e3 FROM t"));
        // `1..5` is `1`, `..`, `5`; a lone `.` before digits starts a number.
        let fp = fingerprint("SELECT 1..5, 1...5, t.5");
        assert_eq!(fp.text, "SELECT ?..?, ?..?, t?");
        assert_eq!(fp.params, ["1", "5", "1", ".5", ".5"]);
    }

    #[test]
    fn numbers_follow_identifier_boundaries() {
        assert_eq!(
            fingerprint("SELECT t1.c2 FROM t1").text,
            "SELECT t1.c2 FROM t1"
        );
        assert_eq!(
            fingerprint("SELECT 0x1F, 1_000_, 1x").text,
            "SELECT 0x1F, 1_000_, 1x"
        );
        assert_eq!(fingerprint("SELECT $1, -2.5").text, "SELECT $1, -?");
        // A number running into an identifier is text up to its end.
        assert_eq!(
            fingerprint("SELECT 1.e, 1.5x, .5e+, 1e+3x").text,
            "SELECT 1.e, 1.5x, .5e+, 1e+3x"
        );
    }

    #[cfg(feature = "query-cache")]
    #[test]
    fn hint_bodies_come_from_comments_outside_quotes() {
        let bodies: Vec<&str> = helios_hint_bodies(
            "/* helios:ttl=60 */ SELECT 'x /*helios:cache=skip*/' AS \"--helios:no\" \
             /*helios:route=standby,cache=semantic*/ -- helios:cache=skip, ttl=5m\nFROM t",
        )
        .collect();
        assert_eq!(
            bodies,
            [
                "ttl=60 ",
                "route=standby,cache=semantic",
                "cache=skip, ttl=5m"
            ]
        );
        assert_eq!(helios_hint_bodies("SELECT 1 /* other */").count(), 0);
        // Nothing is read from a backend-ambiguous rest (no tier caches such
        // a read); a hint before it still is.
        let bodies: Vec<&str> = helios_hint_bodies(
            r"SELECT /*helios:ttl=5*/ path FROM files WHERE path = 'C:\' /*helios:cache=skip*/",
        )
        .collect();
        assert_eq!(bodies, ["ttl=5"]);
        // An unterminated token is not read twice.
        assert_eq!(
            helios_hint_bodies("SELECT 1 /*helios:cache=skip").count(),
            0
        );
    }

    #[cfg(feature = "query-cache")]
    #[test]
    fn tokens_fold_words_and_keep_literals() {
        let toks = tokens(
            "SELECT T.\"Val\", 'A''b', 1.5e3 FROM kv t WHERE x != $1 AND y::text <-1 -- c",
            |_| true,
        )
        .unwrap();
        let text: Vec<&str> = toks.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(
            text,
            [
                "select", "t", ".", "\"Val\"", ",", "'A''b'", ",", "1.5e3", "from", "kv", "t",
                "where", "x", "<>", "$1", "and", "y", "::", "text", "<-", "1"
            ]
        );
        assert_eq!(toks[14].kind, TokenKind::Param);
        // A word keeps its spelling aside, only when the fold changed it.
        let written: Vec<Option<&str>> = toks.iter().map(|t| t.written.as_deref()).collect();
        assert_eq!(
            written[..4],
            [Some("SELECT"), Some("T"), None, None],
            "the quoted \"Val\" is verbatim already"
        );
        assert_eq!((toks[9].text.as_str(), written[9]), ("kv", None));
        let glued = tokens("SELECT E'x', U&'y'", |_| true).unwrap();
        assert_eq!(glued.iter().filter(|t| t.prefixed).count(), 2);
        assert!(!tokens("SELECT 'x'", |_| true).unwrap()[1].prefixed);
        assert!(tokens(r"SELECT 'a\'' FROM t", |_| true).is_err());
        assert!(tokens("SELECT 1 /* open", |_| true).is_err());
        assert!(tokens("SELECT 'open", |_| true).is_err());
        // Comments and dollar quotes HeliosDB-Nano ends elsewhere.
        for sql in [
            "SELECT 1 /* a /* b */ c */ FROM t",
            "SELECT 1 --\rFROM t",
            "SELECT $$a--b$$ FROM t",
        ] {
            assert!(tokens(sql, |_| true).is_err(), "{sql:?}");
        }
        assert!(tokens("SELECT 1 /* a */ -- b\r\nFROM t", |_| true).is_ok());
        // A comment is dropped only when `inert` says so.
        assert!(tokens("SELECT 1 /* x */ FROM t", |_| false).is_err());
        let mut seen = Vec::new();
        let toks = tokens("SELECT 1 /* x */ FROM t -- y", |c| {
            seen.push(c.to_string());
            true
        })
        .unwrap();
        assert_eq!(seen, ["/* x */", "-- y"]);
        assert_eq!(toks.len(), 4);
    }

    /// Whitespace and comments are dropped, so a split PostgreSQL does not
    /// make inside a number would let `1.e3` (1000) and `1. e3` (`1.`
    /// labelled `e3`) share their tokens.
    /// The hint and token reads use the same lexer, linear in the text
    /// however many comments hold a quote
    /// ([`quoted_comments_are_read_in_linear_time`]).
    #[cfg(feature = "query-cache")]
    #[test]
    fn hint_and_token_reads_of_quoted_comments_are_linear() {
        for sql in quoted_comment_texts() {
            holds_quickly("tokens", || tokens(&sql, |_| true).is_ok());
            holds_quickly("helios_hint_bodies", || {
                helios_hint_bodies(&sql).next().is_none()
            });
        }
    }

    #[cfg(feature = "query-cache")]
    #[test]
    fn tokens_split_numbers_where_postgresql_does() {
        let texts = |sql: &str| -> Vec<String> {
            tokens(sql, |_| true)
                .unwrap()
                .into_iter()
                .skip(1)
                .map(|t| t.text)
                .collect()
        };
        assert_eq!(texts("SELECT 1.e3"), ["1.e3"]);
        assert_eq!(texts("SELECT 1. e3"), ["1.", "e3"]);
        assert_eq!(texts("SELECT 1./**/e3"), ["1.", "e3"]);
        assert_eq!(texts("SELECT 1_000.e3"), ["1_000.e3"]);
        assert_eq!(texts("SELECT 1_000. e3"), ["1_000.", "e3"]);
        assert_eq!(texts("SELECT .5"), [".5"]);
        assert_eq!(texts("SELECT . 5"), [".", "5"]);
        assert_eq!(texts("SELECT 1..5"), ["1", "..", "5"]);
        assert_eq!(texts("SELECT 1. .5"), ["1.", ".5"]);
        assert_eq!(texts("SELECT 1 . .5"), ["1", ".", ".5"]);
        assert_eq!(texts("SELECT t.c, t.5"), ["t", ".", "c", ",", "t", ".5"]);
        // A number running into an identifier is one number token.
        let junk = tokens("SELECT 1.x, .5x", |_| true).unwrap();
        let junk: Vec<_> = junk.iter().map(|t| (t.kind, t.text.as_str())).collect();
        assert_eq!(
            junk[1..],
            [
                (TokenKind::Number, "1.x"),
                (TokenKind::Punct, ","),
                (TokenKind::Number, ".5x")
            ]
        );
        assert_eq!(
            tokens("SELECT 1..5", |_| true).unwrap()[2].kind,
            TokenKind::Punct
        );
    }

    #[test]
    fn table_exclusions_match_whole_identifiers_fail_closed() {
        let x = TableExclusions::new(["Secrets", "secrets", "audit"]);
        assert_eq!(x.len(), 2);
        assert_eq!(x.names(), ["audit", "secrets"]);
        for sql in [
            "SELECT * FROM secrets",
            "SELECT * FROM public.secrets",
            "SELECT * FROM \"Secrets\"",
            "SELECT * FROM a, secrets",
            "SELECT * FROM/**/secrets",
            "SELECT * FROM a WHERE x = '--' AND y IN (SELECT 1 FROM secrets)",
            "SELECT * FROM SECRETS",
            // Documented over-exclusion: a literal or a comment.
            "SELECT * FROM a WHERE note = 'secrets'",
            "SELECT * FROM a /* secrets */",
            // A Unicode-escaped identifier can spell the name without its
            // bytes: any `U&"` excludes the read.
            r#"SELECT * FROM U&"s\0065crets" WHERE id = 1"#,
            r#"SELECT * FROM u&"secr\+000065ts""#,
            r#"SELECT * FROM U&"\0073ecrets""#,
            r#"SELECT * FROM U&"a" UESCAPE '!'"#,
        ] {
            assert!(x.matches(sql), "{sql}");
        }
        for sql in [
            "SELECT * FROM secrets_v2",
            "SELECT * FROM my_secrets",
            "SELECT * FROM secrets$x",
            "SELECT * FROM secretsé",
            "SELECT * FROM a",
            // `&` and `"` apart, or a plain quoted identifier.
            r#"SELECT a & "b" FROM a"#,
            r#"SELECT "u" FROM a"#,
        ] {
            assert!(!x.matches(sql), "{sql}");
        }
        let none = TableExclusions::new(Vec::<String>::new());
        assert!(none.is_empty() && !none.matches("SELECT * FROM secrets"));
        assert!(!none.matches(r#"SELECT * FROM U&"s\0065crets""#));
    }

    /// HeliosDB-Nano and HeliosDB-Lite (sqlparser's PostgreSQL dialect) read
    /// a Unicode space (U+00A0, U+3000) as whitespace and only a letter as an
    /// identifier character: a name next to any other non-ASCII character is
    /// a word of its own there. Exclusions, shorter TTLs and the catalog
    /// refusal used to count every byte of a multi-byte character as an
    /// identifier byte, so `FROM\u{a0}secrets` was cached on those backends.
    #[test]
    fn names_next_to_a_unicode_space_are_whole_identifiers() {
        let x = TableExclusions::new(["secrets"]);
        let ttls = ShorterTtls::new(
            [("prices", std::time::Duration::from_secs(5))],
            std::time::Duration::from_secs(300),
        )
        .expect("one shorter TTL");
        for space in ['\u{a0}', '\u{3000}', '\u{2028}'] {
            for sql in [
                format!("SELECT v FROM{space}secrets WHERE id = 1"),
                format!("SELECT v FROM secrets{space}WHERE id = 1"),
                format!("SELECT v FROM a,{space}secrets"),
                format!("SELECT v FROM{space}secrets{space}"),
            ] {
                assert!(x.matches(&sql), "{sql:?}");
                let priced = sql.replace("secrets", "prices");
                assert_eq!(
                    ttls.min_in(&priced),
                    Some(std::time::Duration::from_secs(5)),
                    "{priced:?}"
                );
            }
            for sql in [
                format!("SELECT * FROM{space}pg_settings"),
                format!("SELECT v FROM t WHERE x ={space}information_schema.x"),
            ] {
                assert!(names_system_catalog(sql.as_bytes()), "{sql:?}");
            }
        }
        // A letter still continues the identifier on every backend.
        for sql in [
            "SELECT * FROM secrets\u{e9}",
            "SELECT * FROM \u{e9}secrets",
            "SELECT * FROM \u{4e2d}secrets",
        ] {
            assert!(!x.matches(sql), "{sql}");
        }
        assert!(!names_system_catalog("SELECT \u{e9}pg_x FROM t".as_bytes()));
        assert_eq!(char_before("a\u{3000}".as_bytes(), 4), Some('\u{3000}'));
        assert_eq!(char_after("\u{3000}a".as_bytes(), 0), Some('\u{3000}'));
        assert_eq!(char_before(b"a", 0), None);
        assert_eq!(char_after(b"a", 1), None);
        // Not a whole character: no character (a boundary, fail-closed).
        assert_eq!(char_before(&[b'a', 0xA0], 2), None);
        assert_eq!(char_after(&[0xE3, 0x80], 0), None);
    }

    /// PostgreSQL truncates an identifier to 63 bytes, clipped back to a
    /// character boundary: a longer identifier in the text names the table
    /// whose 60- to 63-byte name it starts with. Such a name matches without
    /// a boundary after it (fail-closed); a shorter one keeps both
    /// boundaries.
    #[test]
    fn long_names_match_the_identifiers_postgresql_truncates_to_them() {
        let ascii63 = "t".repeat(63);
        let x = TableExclusions::new([ascii63.as_str()]);
        assert!(x.matches(&format!("SELECT * FROM {ascii63}")));
        assert!(x.matches(&format!("SELECT * FROM {ascii63}_more_bytes")));
        assert!(x.matches(&format!("SELECT * FROM {}", ascii63.to_uppercase())));
        assert!(
            !x.matches(&format!("SELECT * FROM x{ascii63}")),
            "leading boundary"
        );
        // 61 ASCII bytes and a 3-byte character: PostgreSQL clips the
        // character and keeps 61 bytes.
        let ascii61 = "s".repeat(61);
        let clipped = TableExclusions::new([ascii61.as_str()]);
        assert!(clipped.matches(&format!("SELECT * FROM {ascii61}\u{20ac}x")));
        let ascii60 = "s".repeat(60);
        let shortest = TableExclusions::new([ascii60.as_str()]);
        assert!(shortest.matches(&format!("SELECT * FROM {ascii60}\u{1f600}")));
        // Below 60 bytes no truncation can produce the name: a letter after
        // it continues the identifier on every backend. A character that is
        // no letter (an emoji) ends the name for HeliosDB-Nano and
        // HeliosDB-Lite, so it matches (fail-closed).
        let ascii59 = "s".repeat(59);
        let short = TableExclusions::new([ascii59.as_str()]);
        assert!(!short.matches(&format!("SELECT * FROM {ascii59}\u{4e2d}")));
        assert!(!short.matches(&format!("SELECT * FROM {ascii59}s")));
        assert!(short.matches(&format!("SELECT * FROM {ascii59}")));
        assert!(short.matches(&format!("SELECT * FROM {ascii59}\u{1f600}")));
        assert_eq!(TRUNCATED_IDENTIFIER_MIN_BYTES, 63 - 3);
    }

    #[test]
    fn identifier_scan_reports_each_name_found() {
        let scan = IdentifierScan::new(["Orders", "fx", "prices"]);
        assert_eq!(scan.names(), ["orders", "fx", "prices"]);
        let sql = "select * from ORDERS o join fx_rates r on true where o.note = 'Prices'";
        let found: Vec<usize> = scan.found(sql.as_bytes()).collect();
        assert_eq!(found, [0, 2]);
        // Once per mention, in text order.
        let found: Vec<usize> = scan.found(b"select prices from orders, prices").collect();
        assert_eq!(found, [2, 0, 2]);
        assert!(unicode_escaped_identifier(b"select * from u&\"x\""));
        assert!(unicode_escaped_identifier(b"SELECT * FROM U&\"x\""));
        assert!(!unicode_escaped_identifier(b"select a & \"b\" from t"));
        assert_eq!(IdentifierScan::new([""]).found(b"select 1").count(), 0);
    }

    /// A name inside a longer one, or overlapping it, cannot hide the
    /// other's match: the scan reports every overlapping mention.
    #[test]
    fn identifier_scan_sees_overlapping_names() {
        let scan = IdentifierScan::new(["pay", "payroll", "roll", "payroll_2024"]);
        let found = |sql: &str| {
            let mut found: Vec<usize> = scan.found(sql.as_bytes()).collect();
            found.sort_unstable();
            found.dedup();
            found
        };
        assert_eq!(found("SELECT * FROM payroll"), [1]);
        assert_eq!(found("SELECT * FROM PAYROLL_2024"), [3]);
        assert_eq!(found("SELECT pay FROM payroll_2024"), [0, 3]);
        assert_eq!(found("SELECT * FROM payrolls"), Vec::<usize>::new());
        let x = TableExclusions::new(["pay", "payroll"]);
        assert!(x.matches("SELECT * FROM payroll"));
        assert!(!x.matches("SELECT * FROM payrolls"));
    }

    /// One pass whatever the number of names: a daily-partitioned table over
    /// three years, every partition listed.
    #[test]
    fn many_names_are_scanned_together() {
        let names: Vec<String> = (0..1100).map(|d| format!("events_p{d:04}")).collect();
        let x = TableExclusions::new(&names);
        assert_eq!(x.len(), 1100);
        assert!(x.matches("SELECT * FROM events_p0999 WHERE id = 1"));
        assert!(x.matches("SELECT * FROM public.EVENTS_P1099"));
        assert!(!x.matches("SELECT * FROM events_p1100"));
        assert!(!x.matches("SELECT * FROM events_p00001"));
        assert!(!x.matches("SELECT * FROM events"));
        let scan = IdentifierScan::new(&names);
        let found: Vec<usize> = scan
            .found(b"select * from events_p0007 join events_p0042 on true")
            .collect();
        assert_eq!(found, [7, 42]);
    }

    /// HeliosDB-Nano ends a block comment at its first `*/` and a `--`
    /// comment at `\n` only, and its comment stripper knows no dollar
    /// quotes. Where that differs from PostgreSQL the rest is kept verbatim:
    /// these used to share a key with the plain statement (id 1) while Nano
    /// ran another (id 2, every row, an error).
    #[test]
    fn nano_comment_readings_keep_the_rest_verbatim() {
        let plain = "SELECT val FROM kv WHERE id = 1";
        for (sql, rest) in [
            // Nano: `… WHERE id = 2`, the rest a line comment.
            (
                "SELECT val FROM kv /* /* */ WHERE id = 2 -- */ WHERE id = 1",
                "/* /* */ WHERE id = 2 -- */ WHERE id = 1",
            ),
            // `/*/` opens a nested comment for PostgreSQL, closes for Nano.
            (
                "SELECT val FROM kv /* /*/ WHERE id = 1",
                "/* /*/ WHERE id = 1",
            ),
            // Nano: the comment runs to the end, every row.
            ("SELECT val FROM kv --\rWHERE id = 1", "--\rWHERE id = 1"),
            (
                "SELECT val FROM kv -- c\r WHERE id = 1",
                "-- c\r WHERE id = 1",
            ),
        ] {
            let fp = fingerprint(sql);
            assert_eq!(fp.text, format!("SELECT val FROM kv {rest}"), "{sql:?}");
            assert!(fp.params.is_empty(), "{sql:?}");
            assert_ne!(key(sql), key(plain), "{sql:?}");
        }
        // Both read these alike: a comment.
        for sql in [
            "SELECT val FROM kv /* a */ WHERE id = 1",
            "SELECT val FROM kv /**/ WHERE id = 1",
            "SELECT val FROM kv /* a * / */ WHERE id = 1",
            "SELECT val FROM kv -- c\r\nWHERE id = 1",
            "SELECT val FROM kv -- c\nWHERE id = 1",
            "SELECT val FROM kv WHERE id = 1 -- c",
        ] {
            assert_eq!(key(sql), key(plain), "{sql:?}");
        }
        // Nano: `SELECT $$a` and a line comment (an error) vs `$$a\n$$`
        // (a value): the stripper reads the `--` inside the dollar quote.
        let newline = "SELECT $$a--$$\n$$ AS x FROM t";
        let space = "SELECT $$a--$$ $$ AS x FROM t";
        assert_ne!(key(newline), key(space));
        assert_eq!(fingerprint(newline).text, newline);
        for body in ["a/*b", "it's", "say \"x\""] {
            let sql = format!("SELECT $q${body}$q$ AS x FROM t WHERE id = 1");
            assert_eq!(fingerprint(&sql).text, sql, "{body}");
        }
        // A dollar quote without those is still a literal.
        let fp = fingerprint("SELECT $q$a-b*c/d$q$ AS x FROM t");
        assert_eq!(
            (fp.text.as_str(), fp.kinds.as_slice()),
            ("SELECT ? AS x FROM t", &[KIND_DOLLAR][..])
        );
    }

    /// R6-3: a statement's first word comes from the lexer pass that also
    /// strips its comments, past its leading comments. A leading comment the
    /// backends end in different places is a backend-ambiguous rest, which
    /// leaves no statement (R6-1): PostgreSQL ends a `--` comment at a lone
    /// `\r` (`-- v2` `\r` `CREATE OR REPLACE VIEW …` runs the CREATE there,
    /// Nano reads one comment) and nests block comments. R6-2: anything but
    /// whitespace and comments after a `;` outside literals and comments is
    /// another statement; a `;` inside a literal, a quoted identifier, a
    /// dollar quote or a comment ends nothing.
    #[test]
    fn single_statement_reads_one_unambiguous_statement() {
        for (sql, want) in [
            ("UPDATE t SET v = 1", Some("UPDATE t SET v = 1")),
            ("  /* a */ -- b\n CREATE VIEW v", Some("CREATE VIEW v")),
            ("-- b\r\nDROP TABLE t", Some("DROP TABLE t")),
            ("/* a * / */GRANT x", Some("GRANT x")),
            (
                "UPDATE t SET v = 1 ;  -- done\n",
                Some("UPDATE t SET v = 1 ;  -- done\n"),
            ),
            ("UPDATE t SET s = 'a;b'", Some("UPDATE t SET s = 'a;b'")),
            ("UPDATE t SET s = $$c;d$$", Some("UPDATE t SET s = $$c;d$$")),
            // A quoted identifier where no table name is read.
            (
                "UPDATE t SET \"a;b\" = $$c;d$$",
                Some("UPDATE t SET \"a;b\" = $$c;d$$"),
            ),
            // HeliosDB-Lite ends a statement at a `;` inside a comment.
            ("UPDATE t SET v = 1 -- ; DELETE FROM u", None),
            (
                "UPDATE t SET v = 1 -- done;",
                Some("UPDATE t SET v = 1 -- done;"),
            ),
            ("-- v2\rCREATE OR REPLACE VIEW v AS SELECT * FROM t", None),
            ("/* x */ -- y\rREVOKE SELECT ON kv FROM r", None),
            (
                "/* /* */ CREATE VIEW v AS SELECT 1 */ UPDATE t SET v = 1",
                None,
            ),
            ("/* open", None),
            ("UPDATE t SET p = 'C:\\' WHERE id = 1", None),
            ("UPDATE t SET p = $$it's$$", None),
            ("UPDATE a SET x = 1; UPDATE b SET v = 2", None),
            ("UPDATE a SET note = 'x;y'; DROP TABLE b", None),
            ("UPDATE a SET x = 1;;", None),
            (";UPDATE t SET v = 1", None),
            (";", None),
            ("-- only a comment", None),
            ("", None),
            // HeliosDB-Nano and HeliosDB-Lite read a Unicode space as
            // whitespace and run the statement after it.
            (
                "\u{a0}CREATE OR REPLACE VIEW v AS SELECT * FROM t",
                Some("CREATE OR REPLACE VIEW v AS SELECT * FROM t"),
            ),
            (
                "\u{3000}/* c */\u{a0}\u{a0}TRUNCATE kv",
                Some("TRUNCATE kv"),
            ),
            ("\u{a0}", None),
            // A name the table extraction cannot read whole where it reads
            // a table: a quoted identifier, a `$` or a non-ASCII character
            // other than a space.
            ("UPDATE t SET v = 1 FROM \u{e9}x", None),
            ("UPDATE \"t\" SET v = 1", None),
            ("UPDATE s.\"t\" SET v = 1", None),
            ("UPDATE t SET v = 1 FROM \"u\"", None),
            ("UPDATE t$x SET v = 1", None),
            ("INSERT INTO caf\u{e9} VALUES (1)", None),
            ("INSERT INTO t\u{20ac}x VALUES (1)", None),
            // Not in a literal, a `$n` parameter or a Unicode space.
            (
                "UPDATE\u{a0}t SET v = 'caf\u{e9}', w = $1",
                Some("UPDATE\u{a0}t SET v = 'caf\u{e9}', w = $1"),
            ),
        ] {
            assert_eq!(single_statement(sql).map(|(t, _)| t), want, "{sql:?}");
        }
        // The tables are matched on the comment-free text, borrowed when
        // there is no comment.
        let (_, text) = single_statement("UPDATE app./**/kv SET v = 2 -- c").unwrap();
        assert_eq!(text, "UPDATE app. kv SET v = 2  ");
        let (_, text) = single_statement("UPDATE t SET s = '--'").unwrap();
        assert!(matches!(text, Cow::Borrowed(_)));
    }

    /// A quoted identifier only hides a table where the table extraction
    /// reads one: after a table keyword (or `ONLY`, or a `(` there) and after
    /// the `.` of a name there; and a Unicode-escaped one (`U&"t"`) anywhere,
    /// since the extraction reads its `U` as a name. A quoted column, conflict
    /// target or alias (ordinary ORM text) used to make every such write one
    /// of unknown scope, and both caches invalidated everything.
    #[test]
    fn quoted_identifiers_hide_only_table_names() {
        for sql in [
            "UPDATE accounts SET \"lastLogin\" = now() WHERE id = $1",
            "INSERT INTO orders (\"orderId\", \"createdAt\") VALUES ($1, $2) \
             ON CONFLICT (\"orderId\") DO NOTHING",
            "UPDATE t AS \"x\" SET v = 1 WHERE \"x\".id = 2 RETURNING \"v\"",
            "DELETE FROM t \"x\" USING u, \"w\" WHERE \"x\".id = u.id",
            "INSERT INTO s.t(\"a\") SELECT \"b\" FROM u",
        ] {
            assert!(single_statement(sql).is_some(), "{sql:?}");
        }
        for sql in [
            "INSERT INTO \"t\" VALUES (1)",
            "INSERT INTO s . /* c */ \"t\" VALUES (1)",
            "UPDATE s.t.\"u\" SET v = 1",
            "UPDATE\"t\" SET v = 1",
            "UPDATE ONLY \"t\" SET v = 1",
            "DELETE FROM ONLY ( \"t\" )",
            "UPDATE t SET v = 1 FROM (\"u\" JOIN w ON true)",
            "WITH d AS (DELETE FROM \"t\" RETURNING id) SELECT id FROM d",
            "DELETE FROM kv WHERE id IN (SELECT id FROM \"Gone\")",
            "COPY \"t\" FROM STDIN",
            "TRUNCATE TABLE \"t\"",
            "UPDATE\u{a0}\"t\" SET v = 1 FROM u",
            "UPDATE U&\"t\" SET v = 1 FROM u",
            "UPDATE t SET v = 1 FROM s.u&\"w\"",
        ] {
            assert_eq!(single_statement(sql), None, "{sql:?}");
        }
    }

    /// A name with a `$` or a non-ASCII character hides a table only where
    /// the table extraction reads one; a non-ASCII column (PostgreSQL,
    /// HeliosDB-Nano and HeliosDB-Lite all accept `名前`, `größe`, `prénom`
    /// unquoted) used to make every write of such a schema one of unknown
    /// scope, so both caches invalidated everything on each. A word whose
    /// upper case (the Unicode way) is one of sqlparser's keywords, which
    /// HeliosDB-Lite then reads (`ınsert`, a dotless `ı`; `ſet`; `ﬁlter`, a
    /// ligature), is unknown wherever it stands; one whose upper case is
    /// ASCII text no keyword spells (`straße` is `STRASSE`, `adı` is `ADI`)
    /// is a name there too, and used to be unknown as well. A run starting
    /// with a digit is a number and a word for sqlparser (`1ınsert` is `1`
    /// and `INSERT`): unknown whenever its upper case is ASCII.
    #[test]
    fn non_ascii_names_hide_only_table_names() {
        for sql in [
            "UPDATE customers SET \u{540d}\u{524d} = 'x' WHERE id = 1",
            "UPDATE kunden SET gr\u{f6}\u{df}e = 3 WHERE id = 7",
            "INSERT INTO produits (pr\u{e9}nom) VALUES ('a')",
            "UPDATE t AS caf\u{e9} SET v = 1 WHERE caf\u{e9}.id = 2",
            "UPDATE t SET a$b = 1 RETURNING a$b",
            "UPDATE t\u{a0}SET v = 1 WHERE \u{e9}x = 2",
            "UPDATE kunden SET stra\u{df}e = 'Hauptstr. 1' WHERE id = 7",
            "UPDATE users SET ad\u{131} = 'x' WHERE id = 1",
            "UPDATE users SET kullan\u{131}c\u{131} = 'x' WHERE id = 1",
            "UPDATE t SET \u{fb01}eld = 1",
        ] {
            assert!(single_statement(sql).is_some(), "{sql:?}");
        }
        for sql in [
            "UPDATE caf\u{e9} SET v = 1",
            "UPDATE s.caf\u{e9} SET v = 1",
            "UPDATE t SET v = 1 FROM u, x WHERE id IN (SELECT id FROM \u{e9}t\u{e9})",
            "INSERT INTO t\u{20ac} VALUES (1)",
            "UPDATE t SET v = 1 FROM\u{a0}caf\u{e9}",
            "WITH d AS (\u{131}nsert \u{131}nto kv VALUES (1) RETURNING *) SELECT * FROM d",
            "UPDATE t SET v = 1 WHERE id IN (SELECT id FROM kv \u{17f}et)",
            "UPDATE t SET \u{fb01}lter = 1",
            "UPDATE t SET v = 1 WHERE x = \u{131}nto\u{20ac}kv",
            "UPDATE t SET v = 1 WHERE \u{131}d = 2",
            "UPDATE t SET v = 1 WHERE x = 1\u{131}nsert",
        ] {
            assert_eq!(single_statement(sql), None, "{sql:?}");
        }
        for word in [
            "\u{131}nsert",
            "\u{17f}et",
            "\u{fb01}lter",
            "x.\u{131}nto",
            "1\u{131}nsert",
            "2\u{17f}et",
            "1.5\u{131}nto",
            "1e5\u{131}nto",
        ] {
            assert!(may_read_as_keyword(word), "{word:?}");
        }
        for word in [
            "gr\u{f6}\u{df}e",
            "\u{540d}\u{524d}",
            "insert",
            "stra\u{df}e",
            "ad\u{131}",
            "\u{fb01}eld",
            "2\u{e8}me",
            "x1\u{131}d",
        ] {
            assert!(!may_read_as_keyword(word), "{word:?}");
        }
    }

    /// The vendored keyword list is sqlparser's: sorted for its binary
    /// search, as the tokenizer searches it.
    #[test]
    fn sqlparser_keywords_are_sorted() {
        assert!(SQLPARSER_KEYWORDS.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(SQLPARSER_KEYWORDS.len(), 799);
        for keyword in ["INSERT", "INTO", "SET", "FILTER", "ID", "END-EXEC"] {
            assert!(
                SQLPARSER_KEYWORDS.binary_search(&keyword).is_ok(),
                "{keyword}"
            );
        }
    }

    /// A quoted literal where the table extraction reads a table name:
    /// HeliosDB-Nano and HeliosDB-Lite (sqlparser) read `'kv'` there as the
    /// table `kv`, PostgreSQL rejects it, and the extraction read the
    /// qualifier or the next table instead (`UPDATE public.'kv' …` was a
    /// write of `public`, `DELETE FROM 'kv' … FROM t2` one of `t2`). A write
    /// holding one where it names a table the write may change has an
    /// unknown scope, and a read holding one is not cached. A literal
    /// anywhere else is a value, and so is one where a write's `FROM` names a
    /// table it only reads or loads from (`COPY kv FROM '/tmp/x.csv'`,
    /// `substring(s FROM 'x')`): that write's targets are read as before.
    /// After a `DELETE`, any `FROM` may name the table it deletes from:
    /// sqlparser's `DELETE x FROM 'kv'` deletes from `kv` (HeliosDB-Nano and
    /// HeliosDB-Lite ignore the names between), and was read as a write of
    /// the tables after it. A read takes a table name after the `FROM` of
    /// `IS DISTINCT FROM` too (sqlparser reads `SELECT kv.is DISTINCT FROM
    /// 'kv'` as a read of `kv`, aliased `distinct`), and after a number
    /// running into a word (`SELECT 1from 'kv'` is `SELECT 1 FROM 'kv'`):
    /// such reads were cached with no table, served after a write of `kv`.
    #[test]
    fn quoted_literals_hide_table_names() {
        for sql in [
            "UPDATE public.'kv' SET v = 'new' WHERE id = 1",
            "UPDATE 'kv' SET v = 2 WHERE id IN (SELECT id FROM t2)",
            "INSERT INTO s.'t' VALUES (1)",
            "DELETE FROM 'kv' WHERE id IN (SELECT id FROM t2)",
            "DELETE /* c */ FROM ONLY ( $$kv$$ )",
            "DELETE\u{a0}FROM 'kv'",
            "WITH d AS (DELETE FROM public.'kv' RETURNING *) SELECT * FROM d",
            "MERGE INTO 'kv' USING s ON kv.id = s.id WHEN MATCHED THEN UPDATE SET v = 1",
            "COPY 'kv' FROM STDIN",
            "UPDATE t JOIN 'kv' ON true SET v = 1",
            "DELETE x FROM 'kv' WHERE id IN (SELECT id FROM u)",
            "DELETE \"x\" FROM 'kv' WHERE id IN (SELECT id FROM u)",
            "DELETE a, b FROM 'kv' WHERE id IN (SELECT id FROM t2)",
            "DELETE update FROM $$kv$$",
            "DELETE kv FROM public.'kv' JOIN t2 ON true",
            "WITH d AS (DELETE x FROM 'kv' RETURNING *) SELECT * FROM d",
            // After a `DELETE`, a function's `FROM` is not told from it.
            "DELETE FROM t WHERE s = substring(w FROM 'x')",
            "UPDATE t SET v = 1into 'kv'",
        ] {
            assert_eq!(single_statement(sql), None, "{sql:?}");
        }
        for sql in [
            "SELECT v FROM public.'kv' WHERE id = 1",
            "SELECT v FROM 'kv'",
            "SELECT v FROM t JOIN ( 'u' ) ON true",
            "SELECT v FROM t WHERE id IN (SELECT id FROM s . /* c */ 'u')",
            // `DISTINCT` without `IS` keeps the table name after `FROM`.
            "SELECT DISTINCT FROM 'kv'",
            "SELECT v FROM t WHERE a IS DISTINCT FROM (SELECT v FROM 'kv')",
            // A function's `FROM` is not told from a table's: not cached.
            "SELECT v FROM t WHERE s = substring(w FROM 'x')",
            // `is` may be a column there, `DISTINCT` its alias.
            "SELECT kv.is DISTINCT FROM 'kv'",
            "SELECT is DISTINCT FROM public.'kv'",
            "SELECT * FROM jobs WHERE status IS DISTINCT FROM 'done'",
            "SELECT * FROM t WHERE a IS NOT\u{a0}DISTINCT FROM $$x$$ AND b = 'c'",
            // A number running into a keyword.
            "SELECT 1from 'kv'",
            "SELECT v FROM t WHERE id IN (SELECT 1from 'kv')",
            "SELECT .5from public.'kv'",
            "SELECT 1e5from ( 'kv' )",
            "SELECT 1\u{a0}from 'kv'",
        ] {
            assert_eq!(
                uncacheable_text(sql),
                Some(UncacheableText::AmbiguousText),
                "{sql:?}"
            );
        }
        for sql in [
            "UPDATE t SET v = 'kv' WHERE id = 1",
            "INSERT INTO t VALUES ('from', $$into$$)",
            "UPDATE t SET v = 1 FROM (VALUES ('a', 1)) AS x(a, b) WHERE t.id = x.b",
            "INSERT INTO t (a) SELECT 'x' FROM u",
            "UPDATE t SET v = 1 FROM ($$u$$)",
            "UPDATE jobs SET status = 'done' WHERE id = 5 AND status IS DISTINCT FROM 'done'",
            "UPDATE t SET v = 1 WHERE a IS NOT /* c */ DISTINCT FROM $$x$$",
            "UPDATE t SET v = 1 WHERE a IS DISTINCT FROM \"b\"",
            "UPDATE t SET y = EXTRACT(year FROM '2024-01-01'::date)",
            "UPDATE t SET s = substring(s FROM 'x') WHERE id IN (SELECT id FROM u)",
            "COPY kv FROM '/tmp/x.csv'",
            "COPY kv (a, b) FROM PROGRAM 'cat x'",
            "DELETE FROM jobs WHERE status IS DISTINCT FROM 'done'",
            "DELETE FROM t WHERE s = 'from' AND id IN (SELECT id FROM u)",
            "UPDATE t SET v = 'delete' FROM ($$u$$)",
        ] {
            assert!(single_statement(sql).is_some(), "{sql:?}");
        }
        for sql in [
            "SELECT 'kv' FROM t",
            "SELECT v FROM t WHERE s = 'from' AND w = 'into'",
            "SELECT v FROM (SELECT 'x' AS v) s",
            "SELECT v FROM t AS x WHERE x.s = 'a'",
            "SELECT 0x1F FROM t WHERE s = 'a'",
            "SELECT 1from t WHERE s = 'a'",
        ] {
            assert_eq!(uncacheable_text(sql), None, "{sql:?}");
        }
    }

    /// R6-2: the statements of a string end only at a `;` the lexer finds
    /// outside literals, quoted identifiers and comments; a backend-ambiguous
    /// rest leaves them unknown.
    #[test]
    fn statements_end_where_the_lexer_finds_a_semicolon() {
        for (sql, want) in [
            (
                "SELECT a FROM t1; SELECT b FROM t2",
                vec!["SELECT a FROM t1", "SELECT b FROM t2"],
            ),
            (
                "SELECT 'a;b', \"c;d\", $$e;f$$ FROM t",
                vec!["SELECT 'a;b', \"c;d\", $$e;f$$ FROM t"],
            ),
            (
                "/* x */ SELECT 1 ;; -- c\n UPDATE t SET v = 1;",
                vec!["SELECT 1 ", "UPDATE t SET v = 1"],
            ),
            ("; -- only a comment", vec![]),
        ] {
            assert_eq!(statements(sql), Some(want), "{sql:?}");
        }
        for sql in [
            r"SELECT E'\'' ; UPDATE b SET v = 2",
            "SELECT 1 -- ; \n; UPDATE b SET v = 2",
            "SELECT 1 AS a$$ -- $$; DELETE FROM b",
        ] {
            assert_eq!(statements(sql), None, "{sql:?}");
        }
    }

    /// A keyword is a word outside literals, quoted identifiers and comments
    /// (upper-cased the Unicode way when it is not ASCII, as HeliosDB-Lite
    /// matches keywords); a backend-ambiguous rest may hold one.
    #[test]
    fn keywords_are_words_outside_literals_and_comments() {
        let verbs = ["INSERT", "UPDATE", "DELETE", "MERGE"];
        for sql in [
            "WITH d AS (DELETE FROM t RETURNING id) SELECT * FROM d",
            "with d as (update t set v = 1 returning id) select * from d",
            "WITH d AS (\u{a0}DELETE FROM t RETURNING id) SELECT 1",
            "WITH c AS (SELECT 1) \u{131}nsert \u{131}nto kv SELECT * FROM c",
            r"WITH c AS (SELECT 'C:\' ) SELECT 1",
        ] {
            assert!(may_hold_keyword(sql, &verbs), "{sql:?}");
        }
        for sql in [
            "WITH r AS (SELECT id, op FROM audit_log WHERE op = 'delete') SELECT count(*) FROM r",
            "WITH r AS (SELECT 1) -- update nightly\nSELECT * FROM r",
            "WITH r AS (SELECT \"update\" FROM t) SELECT * FROM r",
            "WITH r AS (SELECT $$merge$$ AS m) /* insert */ SELECT * FROM r",
            "WITH r AS (SELECT deleted, x$update FROM t) SELECT * FROM r",
        ] {
            assert!(!may_hold_keyword(sql, &verbs), "{sql:?}");
        }
    }

    /// Texts of many comments holding a quote, leading and trailing, that
    /// every backend reads as one statement: the lexer's worst case for
    /// HeliosDB-Lite's comment rule ([`Pieces::comment`]).
    fn quoted_comment_texts() -> Vec<String> {
        let n = 100_000;
        let block = "/*'*/".repeat(n);
        let line = "-- '\n".repeat(n);
        vec![
            format!("{block}SELECT 1{block}"),
            format!("{line}SELECT 1 FROM t\n{line};"),
            format!("{block}UPDATE t SET v = 1{block}"),
            format!("{block}UPDATE t SET v = 1 /* it's */"),
        ]
    }

    /// `check` holds, within the bound the linear-time tests use.
    fn holds_quickly(what: &str, check: impl FnOnce() -> bool) {
        let started = std::time::Instant::now();
        assert!(check(), "{what}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "{what}: {:?}",
            started.elapsed()
        );
    }

    /// The comment rule finds the last `;` once per text, so a comment
    /// holding a quote costs only its own length. It used to scan from each
    /// such comment to the end of the text: 100 000 `/*'*/` before
    /// `SELECT 1` (500 KB) read about 4·10¹⁰ bytes per pass of the lexer, on
    /// every statement the journal capture classifies.
    #[test]
    fn quoted_comments_are_read_in_linear_time() {
        for sql in quoted_comment_texts() {
            holds_quickly("lead", || matches!(lead(&sql), Lead::Start(_)));
            holds_quickly("leading_rest", || !leading_rest(&sql));
            holds_quickly("uncacheable_text", || uncacheable_text(&sql).is_none());
            holds_quickly("ambiguous_rest", || ambiguous_rest(&sql).is_none());
            holds_quickly("strip_comments", || {
                matches!(strip_comments(&sql), Cow::Owned(_))
            });
            holds_quickly("single_statement", || single_statement(&sql).is_some());
            holds_quickly("fingerprint", || !fingerprint(&sql).text.is_empty());
            holds_quickly("fingerprint_verbatim", || {
                !fingerprint_verbatim(&sql).text.is_empty()
            });
            holds_quickly("classify", || {
                !matches!(
                    crate::journal_capture::classify(&sql).0,
                    crate::journal_capture::StmtKind::Opaque
                )
            });
        }
    }

    /// R6-1: the lexer stops at backend-ambiguous text, and only there; a
    /// `$`, a backslash or a comment marker alone is no ambiguity.
    #[test]
    fn ambiguous_rests_are_where_the_readings_part() {
        for (sql, rest) in [
            (
                r"SELECT 1 WHERE p = 'C:\' ; DELETE FROM b",
                r"'C:\' ; DELETE FROM b",
            ),
            (
                r"SELECT E'\'' ; UPDATE b SET v = 2",
                r"'\'' ; UPDATE b SET v = 2",
            ),
            (
                "SELECT 1 /* a /* b */ ; DELETE FROM b */",
                "/* a /* b */ ; DELETE FROM b */",
            ),
            (
                "SELECT 1 -- note\r; DELETE FROM b",
                "-- note\r; DELETE FROM b",
            ),
            ("SELECT $$it's$$; DELETE FROM b", "$$it's$$; DELETE FROM b"),
            ("SELECT $q$a--b$q$", "$q$a--b$q$"),
            ("SELECT 'open; DELETE FROM b", "'open; DELETE FROM b"),
            ("SELECT \"open", "\"open"),
            ("SELECT 1 /* open", "/* open"),
            // HeliosDB-Nano and HeliosDB-Lite open a dollar quote at the `$`
            // of a name; PostgreSQL runs the UPDATE.
            (
                "SELECT 1 AS a$b$ ; UPDATE b SET v = 2",
                "a$b$ ; UPDATE b SET v = 2",
            ),
        ] {
            assert_eq!(ambiguous_rest(sql), Some(rest), "{sql:?}");
            assert_eq!(
                uncacheable_text(sql),
                Some(UncacheableText::AmbiguousText),
                "{sql:?}"
            );
        }
        for sql in [
            "SELECT 'a;b' FROM t",
            "SELECT 'a;b', '$' FROM t /*app*/",
            r"SELECT 'C:\tmp;x' FROM t",
            "SELECT $$a;b$$, $1 FROM t -- c\r\n",
            "SELECT \"x$y\", 'it''s' FROM t /* a * / */",
            "SELECT 1 AS a$b ; UPDATE b SET v = 2",
            "SELECT v FROM t",
        ] {
            assert_eq!(ambiguous_rest(sql), None, "{sql:?}");
            assert_eq!(uncacheable_text(sql), None, "{sql:?}");
        }
    }

    /// HeliosDB-Nano's and HeliosDB-Lite's simple-query splitters open a
    /// dollar quote at any `$` that tag characters and another `$` follow, a
    /// `$` inside a name and a tag starting with a digit included, where
    /// PostgreSQL reads a name or a parameter; Nano opens none at a tag
    /// holding a non-ASCII byte, where PostgreSQL does. Each used to be read
    /// as one statement, while Nano ran the write after the `;`.
    #[test]
    fn nano_and_lite_dollar_readings_are_ambiguous() {
        for (sql, rest) in [
            (
                "SELECT 1 AS a$$ -- $$; DELETE FROM b",
                "a$$ -- $$; DELETE FROM b",
            ),
            (
                "SELECT 1 AS a$b$ -- $b$; UPDATE b SET v = 2",
                "a$b$ -- $b$; UPDATE b SET v = 2",
            ),
            (
                "SELECT 1x$$ -- $$; DELETE FROM b",
                "1x$$ -- $$; DELETE FROM b",
            ),
            (
                "SELECT $1$ -- $1$; DELETE FROM b",
                "$1$ -- $1$; DELETE FROM b",
            ),
            (
                "SELECT $1$ ' $1$; DELETE FROM b; --'",
                "$1$ ' $1$; DELETE FROM b; --'",
            ),
            (
                "SET app.tag = $\u{e9}$ ; TRUNCATE kv ; SELECT 1 FROM t ; SET app.tag = $\u{e9}$",
                "$\u{e9}$ ; TRUNCATE kv ; SELECT 1 FROM t ; SET app.tag = $\u{e9}$",
            ),
        ] {
            assert_eq!(ambiguous_rest(sql), Some(rest), "{sql:?}");
            assert_eq!(
                uncacheable_text(sql),
                Some(UncacheableText::AmbiguousText),
                "{sql:?}"
            );
            assert_eq!(single_statement(sql), None, "{sql:?}");
        }
        // Every backend reads these alike.
        for sql in [
            "SELECT t$x FROM a$b WHERE c = $1",
            "SELECT $q$a;b$q$, $$c$$, $_1$d$_1$ FROM t",
            "SELECT \"a$$b\", 'c$$d' FROM t",
            "SELECT $1, $2 FROM t",
            "SELECT 1 $ 2",
        ] {
            assert_eq!(ambiguous_rest(sql), None, "{sql:?}");
            assert_eq!(uncacheable_text(sql), None, "{sql:?}");
        }
    }

    /// HeliosDB-Lite's simple-query splitter knows no comments: it ends a
    /// statement at a `;` inside one, and a quote one leaves open (or a `$`,
    /// whose dollar quotes are not followed) opens a literal that moves its
    /// later boundaries. Such a comment used to be an ordinary one, so only
    /// the first statement's tables (or none) were invalidated. It is
    /// ambiguous only before a `;` that is not the final one, and a comment
    /// whose quotes all close never is: it used to be, so a sqlcommenter tag
    /// next to `'a;b'` retired every cached result.
    #[test]
    fn lite_comment_readings_are_ambiguous() {
        for (sql, rest) in [
            (
                "UPDATE a SET v = 1 -- old: x; DELETE FROM b",
                "-- old: x; DELETE FROM b",
            ),
            ("SELECT v FROM a -- ; DELETE FROM b", "-- ; DELETE FROM b"),
            (
                "SELECT v FROM a /* ; */ ; DELETE FROM b",
                "/* ; */ ; DELETE FROM b",
            ),
            (
                "SELECT 1 -- it's\n, 'a; DELETE FROM b; --'",
                "-- it's\n, 'a; DELETE FROM b; --'",
            ),
            (
                "SELECT 1 /* say \"hi */, 'a\"; DELETE FROM b; --'",
                "/* say \"hi */, 'a\"; DELETE FROM b; --'",
            ),
            (
                "-- $q$\nSELECT $q$; DELETE FROM b; $q$",
                "-- $q$\nSELECT $q$; DELETE FROM b; $q$",
            ),
            // Two quotes of each kind, and Lite ends the comment inside a
            // `"`: the `"` of the literal closes it, and the `;` splits.
            (
                "SELECT 1 /* ' \" ' \" */, 'x\"; DELETE FROM b; --'",
                "/* ' \" ' \" */, 'x\"; DELETE FROM b; --'",
            ),
            (
                "SELECT 1 -- it's\n; DELETE FROM b",
                "-- it's\n; DELETE FROM b",
            ),
            (
                "/* it's */ UPDATE t SET v = 'a; DELETE FROM b; --'",
                "/* it's */ UPDATE t SET v = 'a; DELETE FROM b; --'",
            ),
        ] {
            assert_eq!(ambiguous_rest(sql), Some(rest), "{sql:?}");
            assert_eq!(single_statement(sql), None, "{sql:?}");
        }
        // No `;` after the comment but a final one: Lite splits nowhere else.
        for sql in [
            "UPDATE t SET v = 1 -- done;",
            "UPDATE t SET v = 1 /* it's */ ;  ",
            "SELECT * FROM products WHERE name ILIKE '%a;b%' /*controller='search'*/",
            "SELECT 1; -- it's done",
            "INSERT INTO t VALUES (1) /*traceparent='00-ab'*/;",
            "SELECT 1 -- note\nFROM t",
        ] {
            assert_eq!(ambiguous_rest(sql), None, "{sql:?}");
            assert_eq!(uncacheable_text(sql), None, "{sql:?}");
        }
        // Quotes that all close (sqlcommenter's query tags) leave Lite's
        // splitter outside any literal, where PostgreSQL is after the
        // comment: a `;` inside a later literal ends nothing on either.
        for sql in [
            "SELECT * FROM t /*controller='users'*/ WHERE note LIKE '%;%'",
            "/*controller='posts',action='index'*/ SELECT * FROM posts \
             WHERE body = 'Tom &amp; Jerry'",
            "/*application='app'*/ UPDATE t SET note = 'a;b' WHERE id = 1",
            "SELECT v FROM t /* say \"hi\" it''s */ WHERE s = 'a;b'",
            "SELECT v FROM t -- \"x'y\"\nWHERE s = 'a;b'",
        ] {
            assert_eq!(ambiguous_rest(sql), None, "{sql:?}");
            assert_eq!(uncacheable_text(sql), None, "{sql:?}");
            assert!(!leading_rest(sql), "{sql:?}");
            assert!(single_statement(sql).is_some(), "{sql:?}");
        }
    }

    /// R6-3 for the journal capture and `[edge]`: a statement whose first
    /// word lies behind a leading comment the backends end in different
    /// places is not read by that word.
    #[test]
    fn leading_rests_hide_the_first_word() {
        for sql in [
            "/* a /* b */ UPDATE kv SET v = 2 WHERE id = 1",
            "-- c\rSELECT 1\nUPDATE kv SET v = 2 WHERE id = 1",
            "/* purge /tmp/* entries */ DELETE FROM sessions WHERE expired",
            "  /* ok */ \u{a0}/* /* */ DELETE FROM kv -- */ SELECT 1",
            "-- x\rSELECT ' -- \nDELETE FROM kv -- '",
            "/* it's */ UPDATE t SET v = 1; DELETE FROM b",
            "'open",
        ] {
            assert!(leading_rest(sql), "{sql:?}");
            assert_eq!(statement_start(sql), None, "{sql:?}");
        }
        for sql in [
            "/* a */ UPDATE kv SET v = 2",
            "-- c\r\nSELECT 1",
            "SELECT 1 /* /* */",
            "UPDATE t SET p = 'C:\\'",
            "-- only a comment",
            "",
        ] {
            assert!(!leading_rest(sql), "{sql:?}");
        }
    }

    /// PostgreSQL's MERGE is always `MERGE INTO`; HeliosDB-Nano's `MERGE
    /// [DATABASE] BRANCH src INTO dst` merges a whole branch.
    #[test]
    fn branch_merges_are_merges_without_into() {
        for sql in [
            "MERGE BRANCH fix INTO main",
            "merge database branch fix into main",
            "MERGE/**/BRANCH fix INTO main",
            "MERGE -- x\rINTO t USING s ON true WHEN MATCHED THEN DELETE",
            "MERGE",
            // A Unicode space ends the keyword for HeliosDB-Nano.
            "MERGE\u{a0}BRANCH fix INTO main",
            "MERGE\u{3000}BRANCH fix\u{3000}INTO main",
        ] {
            assert!(merge_without_into(sql), "{sql:?}");
        }
        for sql in [
            "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "merge /* c */ into t USING s ON true WHEN MATCHED THEN DELETE",
            "MERGED_ROWS",
            "MERGE\u{e9} BRANCH fix INTO main",
            "MERGE\u{a0}INTO t USING s ON true WHEN MATCHED THEN DELETE",
            "UPDATE t SET v = 1",
        ] {
            assert!(!merge_without_into(sql), "{sql:?}");
        }
    }

    /// HeliosDB-Nano and HeliosDB-Lite take `INTO` as optional in an
    /// `INSERT` and write the table named next; PostgreSQL requires it.
    #[test]
    fn inserts_without_into_are_found() {
        for sql in [
            "INSERT orders SELECT id, v FROM staging",
            "insert orders (id, v) VALUES ((SELECT max(id) FROM s), 1)",
            "INSERT OR REPLACE orders VALUES (1)",
            "INSERT OR IGNORE /* c */ orders VALUES (1)",
            "INSERT REPLACE orders VALUES (1)",
            "INSERT OVERWRITE TABLE orders SELECT * FROM s",
            "INSERT OR UPSERT INTO orders VALUES (1)",
            "INSERT -- x\rINTO orders VALUES (1)",
            "INSERT \"orders\" VALUES (1)",
            "INSERT\u{a0}orders VALUES (1)",
            "INSERT",
        ] {
            assert!(insert_without_into(sql), "{sql:?}");
        }
        for sql in [
            "INSERT INTO orders VALUES (1)",
            "insert /* c */ into orders SELECT * FROM s",
            "INSERT OR REPLACE INTO orders VALUES (1)",
            "INSERT OR ABORT OVERWRITE INTO orders VALUES (1)",
            "INSERT REPLACE INTO orders VALUES (1)",
            "INSERT\u{a0}INTO orders VALUES (1)",
            "INSERTED_ROWS",
            "UPDATE orders SET v = 1",
        ] {
            assert!(!insert_without_into(sql), "{sql:?}");
        }
    }

    /// HeliosDB-Nano's planner and HeliosDB-Lite read `AS OF` / `VERSIONS
    /// BETWEEN` in the raw statement, comments and literals included, and
    /// cut the clause up to a keyword spelled with one space: a comment or
    /// literal holding one keeps the statement's whitespace and comments.
    #[test]
    fn rewrite_texts_in_literals_and_comments_keep_the_whitespace() {
        for (a, b) in [
            (
                "SELECT val FROM kv WHERE id = 1 /* was offline */",
                "SELECT val FROM kv WHERE id = 1",
            ),
            (
                "SELECT val FROM kv WHERE id = 1 -- AS OF TIMESTAMP '2026-10-01'\n",
                "SELECT val FROM kv WHERE id = 1",
            ),
            (
                "SELECT val FROM kv -- as of now\nWHERE id = 1",
                "SELECT val FROM kv WHERE id = 1",
            ),
            (
                "SELECT val FROM kv WHERE note = 'as of now' GROUP BY val",
                "SELECT val FROM kv WHERE note = 'as of now' GROUP  BY val",
            ),
            (
                "SELECT val FROM kv WHERE note = $$x versions between y$$",
                "SELECT val FROM  kv WHERE note = $$x versions between y$$",
            ),
            ("SELECT \"as of\" FROM kv", "SELECT \"as of\"  FROM kv"),
            (
                "SELECT val FROM kv WHERE note = 'a\u{17f} of'",
                "SELECT val FROM  kv WHERE note = 'a\u{17f} of'",
            ),
        ] {
            assert_ne!(key(a), key(b), "{a:?} vs {b:?}");
        }
        assert_eq!(
            fingerprint("SELECT val  FROM kv /* was offline */").text,
            "SELECT val  FROM kv /* was offline */"
        );
        // Other comments and literals still collapse.
        assert_eq!(
            key("SELECT val FROM kv WHERE note = 'as  of' /* see notes */"),
            key("SELECT val  FROM kv WHERE note = 'as  of'")
        );
        assert!(!holds_nano_rewrite_text("x Security  Invoker y"));
        assert!(holds_nano_rewrite_text("x Security Invoker y"));
        assert!(!holds_nano_rewrite_text("asof"));
    }

    /// HeliosDB-Nano answers a SELECT naming a catalog it emulates
    /// (`pg_settings`, `pg_tables`, `information_schema.*`, …) from a router
    /// that matches the raw text, spacing and comment lengths included:
    /// `name IN ('x')` filters `pg_settings`, `name IN  ('x')` keeps every
    /// row. Such a statement keeps its whitespace and comments.
    #[test]
    fn nano_catalog_reads_keep_the_whitespace() {
        for (a, b) in [
            (
                "SELECT name, setting FROM pg_settings WHERE name IN ('work_mem')",
                "SELECT name, setting FROM pg_settings WHERE name IN  ('work_mem')",
            ),
            (
                "SELECT count(*) FROM pg_catalog.pg_tables GROUP BY schemaname",
                "SELECT count(*) FROM pg_catalog.pg_tables GROUP  BY schemaname",
            ),
            (
                "SELECT * FROM information_schema.tables WHERE table_name = 'kv'",
                "SELECT * FROM INFORMATION_SCHEMA.tables /**/ WHERE table_name = 'kv'",
            ),
            (
                "SELECT v FROM kv WHERE note = 'pg_class'",
                "SELECT v FROM kv  WHERE note = 'pg_class'",
            ),
        ] {
            assert_ne!(key(a), key(b), "{a:?} vs {b:?}");
        }
        for sql in [
            "SELECT * FROM PG_SETTINGS",
            "SELECT * FROM app_pg_settings",
            "SELECT 1 FROM t -- information_schema notes",
        ] {
            assert!(names_nano_catalog(sql.as_bytes()), "{sql}");
        }
        for sql in [
            "SELECT * FROM pgbench_accounts WHERE aid = 1",
            "SELECT information FROM schema_x",
            "SELECT pg FROM _t",
        ] {
            assert!(!names_nano_catalog(sql.as_bytes()), "{sql}");
        }
        // Other statements still collapse whitespace runs.
        assert_eq!(
            key("SELECT v FROM kv WHERE id IN  (1)"),
            key("SELECT v FROM kv WHERE id IN (1)")
        );
    }

    /// HeliosDB-Nano cuts an `AS OF` / `VERSIONS BETWEEN` clause out of a
    /// statement whose upper-cased text holds exactly `" AS OF"`, `"AS OF "`
    /// or `"VERSIONS BETWEEN"`: with whitespace collapsed, `AS of` (which
    /// loses its `FROM t` there) and `AS  of` (which does not) shared a key.
    /// Such a statement keeps its whitespace and comments.
    #[test]
    fn nano_time_travel_words_keep_the_whitespace() {
        for (a, b) in [
            (
                "SELECT val AS of FROM t WHERE id = 1",
                "SELECT val AS  of FROM t WHERE id = 1",
            ),
            (
                "SELECT val AS of FROM t WHERE id = 1",
                "SELECT val AS/**/of FROM t WHERE id = 1",
            ),
            (
                "SELECT val AS office FROM t WHERE id = 1",
                "SELECT val AS\noffice FROM t WHERE id = 1",
            ),
            (
                "SELECT val FROM t WHERE conversions BETWEEN 1 AND 5",
                "SELECT val FROM t WHERE conversions  BETWEEN 1 AND 5",
            ),
            (
                "SELECT val FROM t x AS of WHERE id = 1",
                "SELECT val FROM t x AS of  WHERE id = 1",
            ),
        ] {
            assert_ne!(key(a), key(b), "{a:?} vs {b:?}");
        }
        // Literals are still folded, and a statement without such words
        // still collapses its whitespace.
        let fp = fingerprint("SELECT val AS  of FROM t WHERE id = 1");
        assert_eq!(fp.text, "SELECT val AS  of FROM t WHERE id = ?");
        assert_eq!(fp.params, ["1"]);
        assert_eq!(
            key("SELECT val AS  v FROM t WHERE id = 1"),
            key("SELECT val AS v FROM t WHERE id = 1")
        );
        for (before, after, fires) in [
            ("as", "of", true),
            ("AS", "OFFSET", true),
            ("alias", "Office", true),
            ("1as", "of", true),
            ("versions", "between", true),
            ("xVersions", "BETWEENx", true),
            // Upper-cased as Nano does: `ſ` is `S`, `ﬀ` is `FF`.
            ("a\u{17f}", "of", true),
            ("as", "o\u{fb00}ice", true),
            ("as", "o", false),
            ("a", "of", false),
            ("as", "if", false),
            ("version", "between", false),
            ("versions", "betwee", false),
        ] {
            assert_eq!(
                nano_time_travel_words(before, after),
                fires,
                "{before} {after}"
            );
        }
    }

    /// HeliosDB-Nano also removes the first `SECURITY DEFINER` and the first
    /// `SECURITY INVOKER` (exactly one space, any case, where the words run on
    /// too) and drops the parenthesized list after exactly `AS IDENTITY`,
    /// after turning a block comment into one space: `FROM xsecurity definer`
    /// reads the table `x` there, `FROM xsecurity\tdefiner` the table
    /// `xsecurity`. With whitespace collapsed the two shared one L2 and edge
    /// key. Such a statement keeps its whitespace and comments.
    #[test]
    fn nano_security_and_identity_words_keep_the_whitespace() {
        for (a, b) in [
            (
                "SELECT val FROM xsecurity definer WHERE id = 1",
                "SELECT val FROM xsecurity\tdefiner WHERE id = 1",
            ),
            (
                "SELECT val FROM xsecurity definer WHERE id = 1",
                "SELECT val FROM xsecurity  definer WHERE id = 1",
            ),
            (
                "SELECT val FROM xsecurity definer WHERE id = 1",
                "SELECT val FROM xsecurity\ndefiner WHERE id = 1",
            ),
            (
                "SELECT val FROM xsecurity/**/definer WHERE id = 1",
                "SELECT val FROM xsecurity\tdefiner WHERE id = 1",
            ),
            (
                "SELECT val FROM xSecurity INVOKER WHERE id = 1",
                "SELECT val FROM xSecurity  INVOKER WHERE id = 1",
            ),
            (
                "SELECT xsecurity definerx FROM kv",
                "SELECT xsecurity  definerx FROM kv",
            ),
            (
                "SELECT * FROM kv AS identity (a, b)",
                "SELECT * FROM kv AS  identity (a, b)",
            ),
            (
                "SELECT * FROM kv x$as identity (a, b)",
                "SELECT * FROM kv x$as\tidentity (a, b)",
            ),
        ] {
            assert_ne!(key(a), key(b), "{a:?} vs {b:?}");
        }
        // Literals are still folded, and a statement without such words
        // still collapses its whitespace.
        let fp = fingerprint("SELECT val FROM xsecurity  definer WHERE id = 1");
        assert_eq!(fp.text, "SELECT val FROM xsecurity  definer WHERE id = ?");
        assert_eq!(fp.params, ["1"]);
        assert_eq!(
            key("SELECT val FROM security  x WHERE id = 1"),
            key("SELECT val FROM security x WHERE id = 1")
        );
        assert_eq!(
            key("SELECT * FROM kv AS  ident (a, b)"),
            key("SELECT * FROM kv AS ident (a, b)")
        );
        for (before, after, fires) in [
            ("security", "definer", true),
            ("xSECURITY", "Definerx", true),
            ("security", "invoker", true),
            ("as", "identity", true),
            ("x$as", "IDENTITYx", true),
            // Upper-cased as Nano does: `ſ` is `S`, `ı` is `I`.
            ("x\u{17f}ecurity", "invoker", true),
            ("security", "\u{131}nvoker", true),
            ("a\u{17f}", "identity", true),
            ("securit", "definer", false),
            ("security", "define", false),
            ("security", "invoke", false),
            ("as", "identit", false),
            ("a", "identity", false),
            ("security", "identity", false),
            ("as", "invoker", false),
            // The time-travel words count too.
            ("as", "of", true),
            ("conversions", "between", true),
            ("as", "if", false),
        ] {
            assert_eq!(nano_rewrite_words(before, after), fires, "{before} {after}");
        }
    }

    /// R5-2: a word starting with `pg_` or `information_schema` anywhere
    /// (literals, quoted identifiers and comments too); not inside a word.
    #[test]
    fn system_catalog_names_are_found_anywhere() {
        for sql in [
            "SELECT setting FROM pg_settings WHERE name = 'work_mem'",
            "SELECT count(*) FROM PG_CATALOG.pg_stat_activity",
            "SELECT * FROM Information_Schema.tables",
            "SELECT * FROM \"pg_class\"",
            "SELECT 'pg_class'::regclass",
            "SELECT $$pg_class$$::regclass",
            "SELECT v FROM t -- see pg_stat",
            "SELECT pg_backend_pid()",
            "SELECT v FROM s.pg_x",
            // A Unicode-escaped identifier can spell a catalog name without
            // its bytes: any `U&"` counts.
            r#"SELECT setting FROM U&"\0070g_settings" WHERE name = 'work_mem'"#,
            r#"SELECT setting FROM u&"pg\005fsettings""#,
            r#"SELECT * FROM U&"information\005fschema".tables"#,
            r#"SELECT * FROM U&"!0070g_class" UESCAPE '!'"#,
        ] {
            assert!(names_system_catalog(sql.as_bytes()), "{sql}");
        }
        for sql in [
            "SELECT v FROM t WHERE f = 'img.jpg_1'",
            "SELECT mypg_x FROM t",
            "SELECT v FROM my_information_schema_copy",
            "SELECT v FROM pgx",
            "SELECT v FROM t",
            r#"SELECT a & "b" FROM t"#,
            "",
        ] {
            assert!(!names_system_catalog(sql.as_bytes()), "{sql}");
        }
    }

    /// R5-3: `AS OF` / `VERSIONS BETWEEN` in a comment, with any run of
    /// whitespace between the words and in any ASCII case; never outside a
    /// comment, nor inside a literal.
    #[test]
    fn time_travel_comments_are_found_with_any_spacing() {
        for sql in [
            "SELECT v FROM t /* as of now */",
            "SELECT v FROM t /* as  of now */",
            "SELECT v FROM t /* AS\tOF now */",
            "SELECT v FROM t -- As\x0bOf\nWHERE id = 1",
            "SELECT v FROM t /* versions \r\n between 1 and 2 */",
            "SELECT v FROM t /* was offline */",
            "SELECT v FROM t /*helios:cache=semantic*/ /* as of x */",
            // Upper-cased the way the engines do it: `ſ` is `S`.
            "SELECT v FROM t /* a\u{17f} of x */",
        ] {
            assert_eq!(
                uncacheable_text(sql),
                Some(UncacheableText::TimeTravelComment),
                "{sql:?}"
            );
        }
        for sql in [
            "SELECT v AS of FROM t",
            "SELECT v FROM t WHERE x = '/* as of now */'",
            "SELECT v FROM t /* as-of */",
            "SELECT v FROM t /* asof */",
            "SELECT v FROM t /* versions: between */",
            "SELECT v FROM t /*helios:cache=semantic*/",
            "SELECT v FROM t",
        ] {
            assert_eq!(uncacheable_text(sql), None, "{sql:?}");
        }
    }

    /// The verbatim fingerprint keeps every run of whitespace and every
    /// comment, and folds only the literals.
    #[test]
    fn verbatim_fingerprint_keeps_whitespace_and_comments() {
        let fp = fingerprint_verbatim("SELECT  v FROM t /* c */ WHERE id = 1 AND s = 'x'");
        assert_eq!(fp.text, "SELECT  v FROM t /* c */ WHERE id = ? AND s = ?");
        assert_eq!(fp.params, ["1", "x"]);
        assert_eq!(
            fingerprint("SELECT  v FROM t /* c */ WHERE id = 1 AND s = 'x'").text,
            "SELECT v FROM t WHERE id = ? AND s = ?"
        );
    }

    /// The verbatim fingerprint keeps a dollar quote's tag: sqlparser
    /// (HeliosDB-Nano, HeliosDB-Lite) fails `$ab$x$a$ab$` (its tag scanner
    /// loses the end after `$a`) and reads `$$x$a$$` as `x$a`, so the two
    /// never share a key there. The body stays the literal; PostgreSQL reads
    /// both alike, and the collapsed fingerprint folds the tag away.
    #[test]
    fn verbatim_fingerprint_keeps_the_dollar_tag() {
        let untagged = fingerprint_verbatim("SELECT id FROM kv WHERE s = $$x$a$$");
        let tagged = fingerprint_verbatim("SELECT id FROM kv WHERE s = $ab$x$a$ab$");
        assert_eq!(untagged.text, "SELECT id FROM kv WHERE s = $$?$$");
        assert_eq!(tagged.text, "SELECT id FROM kv WHERE s = $ab$?$ab$");
        assert_eq!(untagged.params, ["x$a"]);
        assert_eq!(tagged.params, ["x$a"]);
        assert_ne!(untagged.text, tagged.text);
        // Another body under one tag still folds into the same text.
        assert_eq!(
            fingerprint_verbatim("SELECT id FROM kv WHERE s = $ab$y$ab$").text,
            tagged.text
        );
        assert_eq!(
            fingerprint("SELECT id FROM kv WHERE s = $$x$a$$"),
            fingerprint("SELECT id FROM kv WHERE s = $ab$x$a$ab$")
        );
    }
}
