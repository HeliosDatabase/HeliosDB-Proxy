//! The relations a statement names, read with a keyword pattern: the table
//! extraction the query cache, the edge cache and the plugin hooks'
//! statement context share. Built with any of those features; the rest of
//! the SQL text scanning ([`crate::sql_text`]) only with the caches.

/// The relations a statement names after `FROM`, `JOIN`, `INTO`, `UPDATE`,
/// `TABLE`, `COPY` or an `INSERT` form (schema-qualified names whole), for
/// [`table_names`]: what the plugin hooks' statement context lists.
#[cfg(feature = "wasm-plugins")]
pub(crate) static RELATION_PATTERN: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| {
        regex::Regex::new(r"(?i)(FROM|JOIN|INTO|UPDATE|TABLE|COPY|INSERT(?:\s+OR\s+[a-zA-Z_]+|\s+REPLACE)?(?:\s+OVERWRITE)?(?:\s+INTO)?(?:\s+TABLE)?)[\s(]+(?:ONLY(?:\s+|\s*\(\s*))?([a-zA-Z_][a-zA-Z0-9_]*(?:\s*\.\s*[a-zA-Z_][a-zA-Z0-9_]*){0,2})")
            .expect("the relation pattern compiles")
    });

/// The table names `pattern` captures in `text` (a statement's
/// `strip_comments` text): the query cache's and the edge cache's table
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
/// write invalidation's reading (`sql_text::skipped_target`).
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

/// Words that start a parenthesized subquery or `VALUES` list after `FROM (`
/// or `JOIN (` (all reserved or, for `VALUES`, unusable there as a table
/// name): [`table_names`] never takes one for a table.
const SUBQUERY_WORDS: &[&str] = &["select", "values", "with", "table"];

/// The character that ends right before byte `at` of `text`: `None` at its
/// start, or when the bytes there are not a whole UTF-8 character.
pub(crate) fn char_before(text: &[u8], at: usize) -> Option<char> {
    let last = *text.get(at.checked_sub(1)?)?;
    if last.is_ascii() {
        return Some(char::from(last));
    }
    let start = (at.saturating_sub(4)..at)
        .rev()
        .find(|&i| text[i] & 0xC0 != 0x80)?;
    std::str::from_utf8(&text[start..at]).ok()?.chars().next()
}
