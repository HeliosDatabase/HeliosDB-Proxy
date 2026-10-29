//! Query Normalizer
//!
//! Normalizes SQL queries for cache key generation by:
//! - Replacing literal values with placeholders, keeping them (with their
//!   kind and position, in statement order) in the hash
//! - Removing comments and collapsing whitespace, outside quotes only
//! - Extracting table names
//! - Computing stable hashes
//!
//! The text is read by the quote-aware scanner in `crate::sql_text` (C-07),
//! shared with the edge cache's fingerprint: a `--` or `/*` inside a literal
//! is text, a comment separates the tokens around it, and dollar-quoted
//! literals and quoted identifiers keep their case.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::sql_text;

/// Normalized query representation
#[derive(Debug, Clone)]
pub struct NormalizedQuery {
    /// Normalized query fingerprint (literals replaced with ?)
    pub fingerprint: String,

    /// Hash of the fingerprint and, when the normalizer preserves parameter
    /// order (the default), of every literal's kind, value and position
    pub hash: u64,

    /// Tables referenced in the query
    pub tables: Vec<String>,

    /// Extracted literal values (a string's text between its quotes, a
    /// dollar quote's body, a number's digits), in statement order
    pub parameters: Vec<String>,
}

impl NormalizedQuery {
    /// Get the fingerprint for display
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Get the hash value
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Get referenced tables
    pub fn tables(&self) -> &[String] {
        &self.tables
    }
}

/// Query normalizer for cache key generation
#[derive(Debug, Clone)]
pub struct QueryNormalizer {
    /// Whether to preserve parameter order
    preserve_order: bool,
}

/// A table after `FROM`/`JOIN`/`INTO`/`UPDATE`/`TABLE`, or after `INSERT`
/// with its optional `OR <word>` / `REPLACE`, `OVERWRITE`, `INTO` and
/// `TABLE` (HeliosDB-Nano and HeliosDB-Lite take `INTO` as optional:
/// `UPDATE a …; INSERT orders SELECT …` writes `orders` there). The
/// keyword is group 1, the table group 2, past any run of whitespace and `(` (a
/// parenthesized join, `FROM (orders JOIN items ON …)`, whose first table
/// used to be lost) and an optional `ONLY` (`UPDATE ONLY kv`,
/// `DELETE FROM ONLY (kv)`, `MERGE INTO ONLY kv`): `ONLY` is a reserved word,
/// never the table. Read through [`sql_text::table_names`], which skips a
/// keyword that ends an identifier (`SELECT valid_from FROM prices`), a
/// subquery's `SELECT` after a `(`, and never lets one match swallow the
/// next keyword. A name has up to three dotted parts, the table last
/// (`benchdb.public.kv`, which PostgreSQL accepts for the current database,
/// used to be read as the table `public`), and a dot may have whitespace on
/// either side (`app . kv`, and `app./**/kv`, whose comment the table text
/// turns into a space), a Unicode space too (`app\u{a0}.kv`, which
/// HeliosDB-Nano and HeliosDB-Lite read as whitespace): PostgreSQL reads
/// the dot as a token of its own, so that is still the table `kv`, never
/// `app`. Keep in sync with the edge cache's twin (`src/edge/fingerprint.rs`).
static TABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(FROM|JOIN|INTO|UPDATE|TABLE|INSERT(?:\s+OR\s+[a-zA-Z_]+|\s+REPLACE)?(?:\s+OVERWRITE)?(?:\s+INTO)?(?:\s+TABLE)?)[\s(]+(?:ONLY(?:\s+|\s*\(\s*))?([a-zA-Z_][a-zA-Z0-9_]*(?:\s*\.\s*[a-zA-Z_][a-zA-Z0-9_]*){0,2})").unwrap()
});

impl QueryNormalizer {
    /// Create a new query normalizer
    pub fn new() -> Self {
        Self {
            preserve_order: true,
        }
    }

    /// Create a normalizer that doesn't preserve parameter order
    pub fn unordered() -> Self {
        Self {
            preserve_order: false,
        }
    }

    /// Normalize a SQL query
    pub fn normalize(&self, sql: &str) -> NormalizedQuery {
        self.normalize_with(sql, false)
    }

    /// [`Self::normalize`], with the fingerprint's whitespace and comments
    /// kept as written when `verbatim` (the query cache's choice once a
    /// backend reported a HeliosDB `server_version`: those engines read both
    /// before parsing, so two texts share a fingerprint only when they differ
    /// in literal values alone).
    pub fn normalize_with(&self, sql: &str, verbatim: bool) -> NormalizedQuery {
        // Tables come from the comment-free text (a comment can neither hide
        // a table name nor glue one to its keyword).
        let tables = self.statement_tables(sql);
        let fp = if verbatim {
            sql_text::fingerprint_verbatim(sql)
        } else {
            sql_text::fingerprint(sql)
        };

        let mut hasher = DefaultHasher::new();
        fp.text.hash(&mut hasher);
        if self.preserve_order {
            // The literals, in statement order, with their kinds: `'1'` and
            // `1`, or `a = '1' AND b = 2` and `a = 2 AND b = '1'`, differ.
            fp.hash_literals(&mut hasher);
        }
        let hash = hasher.finish();

        NormalizedQuery {
            fingerprint: fp.text,
            hash,
            tables,
            parameters: fp.params,
        }
    }

    /// `sql` with every comment (outside quotes) replaced by one space.
    /// Borrowed (no scan, no allocation) when it contains neither `/*` nor
    /// `--`, and borrowed when those only occur inside literals.
    pub fn strip_comments<'a>(&self, sql: &'a str) -> std::borrow::Cow<'a, str> {
        sql_text::strip_comments(sql)
    }

    /// The tables the statement `sql` names, read from its comment-free
    /// text ([`sql_text::strip_comments`]), first seen first, each once.
    pub(crate) fn statement_tables(&self, sql: &str) -> Vec<String> {
        self.extract_tables(&sql_text::strip_comments(sql))
    }

    /// The tables a write may change, read from its comment-free `text`
    /// ([`sql_text::single_statement`]). `None` (unknown) when a keyword's
    /// table is a word HeliosDB-Nano and HeliosDB-Lite read before the
    /// target ([`sql_text::skipped_target`]: `UPDATE OR IGNORE kv`,
    /// `INSERT INTO LOCAL kv`).
    pub(crate) fn write_statement_tables(&self, text: &str) -> Option<Vec<String>> {
        let mut tables = Vec::new();
        for (keyword, table) in sql_text::table_matches(&TABLE_PATTERN, text) {
            if sql_text::skipped_target(keyword, table) {
                return None;
            }
            Self::add_table(&mut tables, table);
        }
        Some(tables)
    }

    /// Extract table names from a SQL query
    pub(crate) fn extract_tables(&self, sql: &str) -> Vec<String> {
        let mut tables = Vec::new();
        self.add_tables(&mut tables, sql);
        tables
    }

    /// Add the table names `sql` names to `tables`, each once.
    fn add_tables(&self, tables: &mut Vec<String>, sql: &str) {
        for table in sql_text::table_names(&TABLE_PATTERN, sql) {
            Self::add_table(tables, table);
        }
    }

    /// Add `table` (as the pattern captured it) to `tables`, once.
    fn add_table(tables: &mut Vec<String>, table: &str) {
        // Remove schema prefix if present (and the whitespace after its
        // dot), and cut the name to the 63 bytes PostgreSQL keeps of it
        // (the catalog's name, which writes and decoded changes use).
        let table_name = table.rsplit('.').next().unwrap_or(table).trim();
        let table_name = sql_text::truncated_identifier(table_name).to_lowercase();
        if !tables.contains(&table_name) {
            tables.push(table_name);
        }
    }

    /// Normalize for comparison only (no parameter extraction)
    pub fn fingerprint(&self, sql: &str) -> String {
        sql_text::fingerprint(sql).text
    }

    /// Check if two queries are equivalent (same fingerprint)
    pub fn are_equivalent(&self, sql1: &str, sql2: &str) -> bool {
        self.fingerprint(sql1) == self.fingerprint(sql2)
    }
}

impl Default for QueryNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Quick fingerprint generation without full normalization
pub fn quick_fingerprint(sql: &str) -> u64 {
    let normalized = QueryNormalizer::new().fingerprint(sql);
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish()
}

/// Extract tables from SQL without full normalization
pub fn extract_tables(sql: &str) -> Vec<String> {
    let normalizer = QueryNormalizer::new();
    normalizer.extract_tables(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_simple_query() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM users WHERE id = 123";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM users WHERE id = ?");
        assert_eq!(normalized.parameters, vec!["123"]);
        assert_eq!(normalized.tables, vec!["users"]);
    }

    #[test]
    fn test_normalize_string_literals() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM users WHERE name = 'John Doe'";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM users WHERE name = ?");
        assert_eq!(normalized.parameters, vec!["John Doe"]);
    }

    #[test]
    fn test_normalize_multiple_parameters() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM users WHERE age > 18 AND status = 'active' AND score < 100";
        let normalized = normalizer.normalize(query);

        assert_eq!(
            normalized.fingerprint,
            "SELECT * FROM users WHERE age > ? AND status = ? AND score < ?"
        );
        assert_eq!(normalized.parameters.len(), 3);
    }

    #[test]
    fn test_extract_tables_join() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id";
        let normalized = normalizer.normalize(query);

        assert!(normalized.tables.contains(&"users".to_string()));
        assert!(normalized.tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_normalize_removes_comments() {
        let normalizer = QueryNormalizer::new();
        let query = "/* helios:cache_ttl=60 */ SELECT * FROM users -- inline comment\nWHERE id = 1";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn test_normalize_whitespace() {
        let normalizer = QueryNormalizer::new();
        let query1 = "SELECT  *  FROM   users   WHERE   id=1";
        let query2 = "SELECT * FROM users WHERE id=1";

        assert_eq!(
            normalizer.fingerprint(query1),
            normalizer.fingerprint(query2)
        );
    }

    #[test]
    fn test_equivalent_queries() {
        let normalizer = QueryNormalizer::new();

        // Same query with different literal values
        let query1 = "SELECT * FROM users WHERE id = 123";
        let query2 = "SELECT * FROM users WHERE id = 456";

        assert!(normalizer.are_equivalent(query1, query2));

        // Different query structure
        let query3 = "SELECT * FROM users WHERE name = 'test'";
        assert!(!normalizer.are_equivalent(query1, query3));
    }

    #[test]
    fn test_hash_consistency() {
        let normalizer = QueryNormalizer::new();

        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 1";

        let norm1 = normalizer.normalize(query1);
        let norm2 = normalizer.normalize(query2);

        assert_eq!(norm1.hash, norm2.hash);
    }

    #[test]
    fn test_hash_different_params() {
        let normalizer = QueryNormalizer::new();

        // With preserve_order=true, different params should have different hashes
        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 2";

        let norm1 = normalizer.normalize(query1);
        let norm2 = normalizer.normalize(query2);

        assert_ne!(norm1.hash, norm2.hash);
    }

    #[test]
    fn test_unordered_normalizer() {
        let normalizer = QueryNormalizer::unordered();

        // With preserve_order=false, different params should have same hash
        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 2";

        let norm1 = normalizer.normalize(query1);
        let norm2 = normalizer.normalize(query2);

        // Fingerprints are the same
        assert_eq!(norm1.fingerprint, norm2.fingerprint);
    }

    #[test]
    fn test_extract_tables_various() {
        let normalizer = QueryNormalizer::new();

        let queries = vec![
            ("INSERT INTO users VALUES (1)", vec!["users"]),
            ("UPDATE products SET price = 10", vec!["products"]),
            ("DELETE FROM orders WHERE id = 1", vec!["orders"]),
            ("SELECT * FROM schema.table", vec!["table"]),
            ("SELECT * FROM db.schema.table", vec!["table"]),
            ("TABLE users", vec!["users"]),
        ];

        for (sql, expected_tables) in queries {
            let normalized = normalizer.normalize(sql);
            for table in expected_tables {
                assert!(
                    normalized.tables.contains(&table.to_string()),
                    "Query '{}' should contain table '{}'",
                    sql,
                    table
                );
            }
        }
    }

    /// A keyword at the end of a select-list column (`valid_from`,
    /// `last_update`, `data_table`) used to match, capture the real `FROM` as
    /// the table and step over the relation: the read's tables were `[from]`,
    /// which no write to its table moves (L1, L2), and L3 refused it.
    #[test]
    fn a_keyword_ending_an_identifier_is_not_one() {
        let normalizer = QueryNormalizer::new();
        for (sql, want) in [
            (
                "SELECT id, valid_from FROM prices WHERE id = 1",
                vec!["prices"],
            ),
            ("SELECT valid_from FROM prices", vec!["prices"]),
            ("SELECT last_update FROM accounts", vec!["accounts"]),
            ("SELECT x, data_table FROM t", vec!["t"]),
            ("SELECT yjoin FROM t", vec!["t"]),
            ("SELECT zinto FROM t", vec!["t"]),
            ("SELECT valid_from FROM a JOIN b ON true", vec!["a", "b"]),
            ("SELECT \u{f1}from FROM t", vec!["t"]),
        ] {
            assert_eq!(normalizer.normalize(sql).tables, want, "{sql}");
        }
        assert_eq!(
            extract_tables("UPDATE prices SET valid_from = now() WHERE id = 1"),
            ["prices"]
        );
        // A keyword after any other byte still counts, and its match no
        // longer swallows the real keyword right after it.
        for sql in [
            "SELECT t.from FROM prices",
            "SELECT 1 AS from FROM prices",
            "SELECT x$from FROM prices",
            "SELECT 1from prices",
            "SELECT $$a$$FROM prices",
            "SELECT t.update FROM prices",
        ] {
            let tables = normalizer.normalize(sql).tables;
            assert!(tables.contains(&"prices".to_string()), "{sql}: {tables:?}");
        }
    }

    /// HeliosDB-Nano and HeliosDB-Lite (sqlparser's PostgreSQL dialect) read
    /// a Unicode space as whitespace: `v\u{a0}FROM kv` reads `kv` there. The
    /// keyword after one used to be skipped as the end of an identifier, so
    /// the read was stored with no table and `UPDATE kv` never retired it;
    /// and `INSERT\u{a0}INTO kv SELECT … FROM src` invalidated `src` only.
    #[test]
    fn a_keyword_after_a_unicode_space_counts() {
        let normalizer = QueryNormalizer::new();
        for space in ['\u{a0}', '\u{3000}'] {
            for (sql, want) in [
                (format!("SELECT v{space}FROM kv WHERE id = 1"), vec!["kv"]),
                (
                    format!("SELECT v FROM kv{space}JOIN other ON true"),
                    vec!["kv", "other"],
                ),
                (
                    format!("SELECT v FROM kv WHERE id IN (SELECT id{space}FROM other)"),
                    vec!["kv", "other"],
                ),
                (format!("SELECT v FROM{space}kv"), vec!["kv"]),
                (format!("SELECT v FROM app{space}.{space}kv"), vec!["kv"]),
            ] {
                assert_eq!(normalizer.normalize(&sql).tables, want, "{sql:?}");
            }
            let insert = format!("INSERT{space}INTO kv SELECT * FROM src");
            assert_eq!(
                normalizer.write_statement_tables(&insert).unwrap(),
                ["kv", "src"],
                "{insert:?}"
            );
        }
    }

    /// HeliosDB-Nano and HeliosDB-Lite take `INTO` as optional in an
    /// `INSERT`: its target is read wherever the `INSERT` is (a later
    /// statement of the string, a data-modifying CTE), and `INSERT INTO`
    /// still names only its table.
    #[test]
    fn an_insert_names_its_target_with_or_without_into() {
        let normalizer = QueryNormalizer::new();
        for (sql, want) in [
            (
                "UPDATE a SET x = 1; INSERT orders SELECT * FROM staging",
                vec!["a", "orders", "staging"],
            ),
            (
                "WITH n AS (INSERT orders VALUES (1) RETURNING *) SELECT * FROM n",
                vec!["orders", "n"],
            ),
            (
                "UPDATE a SET x = 1; INSERT OR REPLACE orders VALUES (1)",
                vec!["a", "orders"],
            ),
            (
                "INSERT OVERWRITE TABLE orders SELECT * FROM s",
                vec!["orders", "s"],
            ),
            ("INSERT INTO orders VALUES (1)", vec!["orders"]),
            (
                "INSERT OR REPLACE INTO orders SELECT * FROM s",
                vec!["orders", "s"],
            ),
            ("insert orders values (1)", vec!["orders"]),
            // A skipped keyword does not hide the one it holds.
            ("SELECT last_insert INTO t FROM s", vec!["t", "s"]),
        ] {
            assert_eq!(
                normalizer.write_statement_tables(sql).unwrap(),
                want,
                "{sql}"
            );
        }
    }

    /// sqlparser's PostgreSQL dialect (HeliosDB-Nano, HeliosDB-Lite) reads
    /// an `UPDATE`'s conflict clause and Hive's `LOCAL` after an `INSERT`'s
    /// `INTO` before the target, and those engines write the table named
    /// next: the extraction took `or`, `replace` or `local` for the table.
    /// The scope is unknown; a real table after those keywords still names
    /// itself.
    #[test]
    fn a_word_sqlparser_reads_before_the_target_makes_the_scope_unknown() {
        let normalizer = QueryNormalizer::new();
        for sql in [
            "UPDATE OR IGNORE kv SET v = 2 WHERE id = 1",
            "update or replace kv SET v = 2",
            "UPDATE REPLACE kv SET v = 2",
            "UPDATE /* c */ OR ROLLBACK kv SET v = 2",
            "UPDATE\u{a0}OR FAIL kv SET v = 2",
            "INSERT INTO LOCAL kv VALUES (1)",
            "INSERT OR IGNORE INTO local kv VALUES (1)",
            "INSERT OVERWRITE INTO LOCAL TABLE kv SELECT * FROM s",
            "UPDATE a SET x = 1; UPDATE OR ABORT kv SET v = 2",
            "WITH u AS (UPDATE OR IGNORE kv SET v = 2 RETURNING *) SELECT * FROM u",
            // The pattern stepped back over its own optional word, the name
            // after it being one it cannot read: it took `into` (`only`) for
            // the table.
            "INSERT INTO \"t\" VALUES (1)",
            "INSERT OR REPLACE INTO \u{e9}t\u{e9} VALUES (1)",
            "DELETE FROM ONLY \"kv\" WHERE id = 1",
        ] {
            let text = sql_text::strip_comments(sql);
            assert_eq!(normalizer.write_statement_tables(&text), None, "{sql:?}");
        }
        for (sql, want) in [
            ("UPDATE kv SET v = 2", vec!["kv"]),
            ("UPDATE ONLY kv SET v = 2", vec!["kv"]),
            ("UPDATE orders SET v = 2", vec!["orders"]),
            ("INSERT INTO TABLE local VALUES (1)", vec!["local"]),
            ("INSERT INTO local.kv VALUES (1)", vec!["kv"]),
            ("INSERT INTO kv SELECT * FROM local", vec!["kv", "local"]),
            ("INSERT OR REPLACE INTO kv VALUES (1)", vec!["kv"]),
        ] {
            assert_eq!(
                normalizer.write_statement_tables(sql).unwrap(),
                want,
                "{sql:?}"
            );
        }
    }

    #[test]
    fn test_decimal_numbers() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM products WHERE price < 99.99 AND rating > 4.5";
        let normalized = normalizer.normalize(query);

        assert!(normalized.parameters.contains(&"99.99".to_string()));
        assert!(normalized.parameters.contains(&"4.5".to_string()));
    }

    #[test]
    fn test_scientific_notation() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM data WHERE value = 1e10";
        let normalized = normalizer.normalize(query);

        assert!(normalized.fingerprint.contains("value = ?"));
    }

    /// The verbatim L2 key (a HeliosDB backend) keeps a dollar quote's tag:
    /// sqlparser fails `$ab$x$a$ab$` and reads `$$x$a$$` as `x$a`. The
    /// collapsed key (PostgreSQL, which reads both alike) folds it away.
    #[test]
    fn the_verbatim_key_keeps_the_dollar_tag() {
        let normalizer = QueryNormalizer::new();
        let untagged = "SELECT id FROM kv WHERE s = $$x$a$$";
        let tagged = "SELECT id FROM kv WHERE s = $ab$x$a$ab$";
        assert_ne!(
            normalizer.normalize_with(untagged, true).hash,
            normalizer.normalize_with(tagged, true).hash
        );
        assert_eq!(
            normalizer.normalize_with(untagged, true).parameters,
            ["x$a"]
        );
        assert_eq!(
            normalizer.normalize(untagged).hash,
            normalizer.normalize(tagged).hash
        );
    }

    #[test]
    fn test_quick_fingerprint() {
        let hash1 = quick_fingerprint("SELECT * FROM users WHERE id = 1");
        let hash2 = quick_fingerprint("SELECT * FROM users WHERE id = 2");

        // Quick fingerprint ignores parameter values
        assert_eq!(hash1, hash2);
    }

    /// C-07: statements that differ only inside a literal holding `--`, in
    /// literal order or kind, around a comment, inside a dollar quote or in a
    /// quoted identifier's case used to share an L2 key (and slot). So did
    /// two spellings of an output column's label, which HeliosDB-Nano
    /// reports as written: the second client got the first one's column
    /// names.
    #[test]
    fn literal_and_comment_forms_never_share_a_key() {
        let n = QueryNormalizer::new();
        for (a, b) in [
            (
                "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 7",
                "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 8",
            ),
            (
                "SELECT v FROM t WHERE a = '1' AND b = 2",
                "SELECT v FROM t WHERE a = 2 AND b = '1'",
            ),
            ("SELECT '1' FROM t", "SELECT 1 FROM t"),
            ("SELECT a/**/b FROM t", "SELECT ab FROM t"),
            ("SELECT $$alice$$ FROM t", "SELECT $$ALICE$$ FROM t"),
            ("SELECT v FROM \"Orders\"", "SELECT v FROM \"ORDERS\""),
            (
                "SELECT Name FROM users WHERE id = 1",
                "SELECT name FROM users WHERE id = 1",
            ),
            (
                "SELECT name AS Total FROM users WHERE id = 1",
                "SELECT name AS total FROM users WHERE id = 1",
            ),
            // `\v` is whitespace: the first is `'ab'`, the second an error.
            (
                "SELECT id FROM t WHERE s = 'a'\x0b\n'b'",
                "SELECT id FROM t WHERE s = 'a'\x0b 'b'",
            ),
            (
                "SELECT id FROM t WHERE s = 'a'\n\x0b'b'",
                "SELECT id FROM t WHERE s = 'a' \x0b'b'",
            ),
            // HeliosDB-Nano runs `… WHERE id = 2`, every row, and a statement
            // without its `FROM t`.
            (
                "SELECT val FROM kv /* /* */ WHERE id = 2 -- */ WHERE id = 1",
                "SELECT val FROM kv WHERE id = 1",
            ),
            (
                "SELECT val FROM kv --\rWHERE id = 1",
                "SELECT val FROM kv WHERE id = 1",
            ),
            (
                "SELECT val AS of FROM kv WHERE id = 1",
                "SELECT val AS  of FROM kv WHERE id = 1",
            ),
        ] {
            assert_ne!(n.normalize(a).hash, n.normalize(b).hash, "{a:?} vs {b:?}");
        }
        // Tables hidden behind a comment are found.
        assert_eq!(n.normalize("SELECT v FROM/**/secrets").tables, ["secrets"]);
    }

    /// `ONLY` (inheritance) is skipped, never read as the table: `UPDATE
    /// ONLY kv` used to move the generation of a table `only`, and kv's
    /// cached reads stayed served.
    #[test]
    fn only_is_never_the_table() {
        let n = QueryNormalizer::new();
        for (sql, want) in [
            ("UPDATE ONLY kv SET val = 'new' WHERE id = 1", vec!["kv"]),
            ("update only public.kv set val = 1", vec!["kv"]),
            ("DELETE FROM ONLY kv WHERE id = 1", vec!["kv"]),
            ("DELETE FROM ONLY (kv) WHERE id = 1", vec!["kv"]),
            ("DELETE FROM ONLY(kv)", vec!["kv"]),
            (
                "MERGE INTO ONLY kv USING src ON kv.id = src.id WHEN MATCHED THEN DELETE",
                vec!["kv"],
            ),
            ("SELECT * FROM ONLY kv", vec!["kv"]),
            ("TABLE ONLY kv", vec!["kv"]),
            ("SELECT * FROM a JOIN ONLY\n  b ON true", vec!["a", "b"]),
            // A table whose name starts with `only` is that table.
            ("SELECT * FROM only_rows", vec!["only_rows"]),
            ("UPDATE onlyx SET v = 1", vec!["onlyx"]),
        ] {
            assert_eq!(n.normalize(sql).tables, want, "{sql}");
        }
    }

    /// A schema qualifier's dot next to a comment or whitespace is still a
    /// qualifier: the table text turns `app./**/kv` into `app. kv`, which the
    /// pattern used to read as the table `app`, so a write spelled that way
    /// never retired `app.kv`'s reads (and such a read was never retired by
    /// `UPDATE app.kv`).
    #[test]
    fn a_spaced_qualifier_dot_still_names_the_table() {
        let n = QueryNormalizer::new();
        for sql in [
            "UPDATE app./**/kv SET v = 2 WHERE id = 1",
            "SELECT v FROM app/**/.kv WHERE id = 1",
            "SELECT v FROM app /* a */ . /* b */ kv WHERE id = 1",
            "INSERT INTO app . kv VALUES (1)",
            "DELETE FROM app.\n  kv WHERE id = 1",
            "SELECT v FROM app -- schema\n.kv",
            "UPDATE ONLY app . kv SET v = 1",
            "SELECT v FROM app.kv WHERE id = 1",
        ] {
            assert_eq!(n.normalize(sql).tables, ["kv"], "{sql}");
        }
        assert_eq!(
            n.normalize("SELECT * FROM a . b JOIN c/**/./**/d ON true")
                .tables,
            ["b", "d"]
        );
        // No dot: the word after the table is not a name.
        assert_eq!(
            n.normalize("SELECT v FROM kv x WHERE id = 1").tables,
            ["kv"]
        );
        assert_eq!(n.normalize("SELECT v FROM kv WHERE x = .5").tables, ["kv"]);
    }

    /// A three-part name (`catalog.schema.table`, which PostgreSQL accepts
    /// for the current database) used to be read as the table `schema`, so
    /// a write spelled `kv` and a read spelled `benchdb.public.kv` never met.
    #[test]
    fn three_part_names_name_the_table() {
        let n = QueryNormalizer::new();
        for sql in [
            "SELECT v FROM benchdb.public.kv WHERE id = 1",
            "UPDATE benchdb.public.kv SET v = 2 WHERE id = 1",
            "UPDATE kv SET v = 2 WHERE id = 1",
            "DELETE FROM benchdb . public . kv WHERE id = 1",
            "INSERT INTO benchdb.public./**/kv VALUES (1)",
            "SELECT v FROM ONLY benchdb.public.kv",
            "SELECT v FROM (benchdb.public.kv JOIN kv x ON true)",
        ] {
            assert_eq!(n.normalize(sql).tables, ["kv"], "{sql}");
        }
    }

    /// A parenthesized join (`FROM (a JOIN b ON …)`) used to lose its first
    /// table: a write to it never retired the cached join.
    #[test]
    fn parenthesized_joins_keep_their_first_table() {
        let n = QueryNormalizer::new();
        for (sql, want) in [
            (
                "SELECT * FROM (orders JOIN items ON orders.id = items.order_id) WHERE orders.id = 1",
                vec!["orders", "items"],
            ),
            ("SELECT * FROM(orders JOIN items ON true)", vec!["orders", "items"]),
            (
                "SELECT * FROM ((a JOIN b ON true) JOIN c ON true)",
                vec!["a", "b", "c"],
            ),
            ("SELECT * FROM a JOIN ( b JOIN c ON true) ON true", vec!["a", "b", "c"]),
            ("SELECT * FROM (ONLY a JOIN b ON true)", vec!["a", "b"]),
            ("SELECT * FROM (public.a JOIN b ON true)", vec!["a", "b"]),
            // A subquery's or VALUES list's first word is not a table.
            ("SELECT * FROM (SELECT * FROM t) x", vec!["t"]),
            ("SELECT * FROM (VALUES (1)) v JOIN t ON true", vec!["t"]),
            ("SELECT * FROM (SELECT f(1)) x", vec![]),
            ("SELECT * FROM ((WITH c AS (SELECT 1) TABLE c)) x", vec!["c"]),
            ("SELECT * FROM (select_log JOIN b ON true)", vec!["select_log", "b"]),
            // Without a parenthesis the word is read as before.
            ("SELECT * FROM values", vec!["values"]),
        ] {
            assert_eq!(n.normalize(sql).tables, want, "{sql}");
        }
        assert_eq!(
            extract_tables("UPDATE orders SET status = 'x' WHERE id = 1"),
            ["orders"]
        );
    }

    /// A table name is keyed by the 63 bytes PostgreSQL keeps of it: a read
    /// that spells a longer name used to keep its whole spelling, so a write
    /// naming the catalog's (truncated) relation, or a decoded change, moved
    /// another key and the read stayed served until its TTL ended.
    #[test]
    fn table_names_are_truncated_like_postgresql() {
        let n = QueryNormalizer::new();
        let long = format!("t{}", "x".repeat(69));
        let stored = &long[..63];
        assert_eq!(long.len(), 70);
        let read = n.normalize(&format!("SELECT v FROM {long} WHERE id = 1"));
        let write = n.normalize(&format!("UPDATE {stored} SET v = 'new' WHERE id = 1"));
        assert_eq!(read.tables, [stored]);
        assert_eq!(write.tables, read.tables);
        // Another spelling past byte 63, in another case and schema, is the
        // same table.
        let other = format!("{}xyz", stored.to_uppercase());
        let write = n.normalize(&format!("UPDATE s.{other} SET v = 'new' WHERE id = 1"));
        assert_eq!(write.tables, [stored]);
        // A name of 63 bytes is kept whole; a longer read and its write in
        // one statement collapse to one table.
        assert_eq!(n.normalize(&format!("TABLE {stored}")).tables, [stored]);
        assert_eq!(
            n.normalize(&format!("SELECT * FROM {long} JOIN {stored} ON true"))
                .tables,
            [stored]
        );
    }

    /// The normalizer and its edge-cache twin read text the same way.
    #[cfg(feature = "edge-proxy")]
    #[test]
    fn cache_and_edge_fingerprints_agree() {
        let n = QueryNormalizer::new();
        for sql in [
            "SELECT * FROM users WHERE id = 123",
            "/* helios:route=primary */ SELECT * FROM users -- c\nWHERE id = 1",
            "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 7",
            "select $$x$$, \"Q\", 1e5, 'it''s' from t /* a /* b */ */ where c = 2",
            r"SELECT v FROM t WHERE p = 'C:\' -- x",
            "SELECT * FROM f WHERE p = 'C:\\' AND o IN (SELECT id FROM -- c\n owners)",
            "SELECT u.*, o.* FROM public.users u JOIN orders o ON u.id = o.user_id",
            "DELETE FROM ONLY (kv) WHERE id = 1",
            "SELECT id FROM t WHERE \u{83}A = 1",
            "SELECT Name FROM users WHERE id = 1",
            &format!("SELECT v FROM s.T{} WHERE id = 1", "x".repeat(69)),
            "SELECT v FROM benchdb.public.kv WHERE id = 1",
            "SELECT * FROM (orders JOIN items ON true) WHERE orders.id = 1",
            "SELECT * FROM (SELECT f(1)) x",
            "SELECT val FROM kv /* /* */ WHERE id = 2 -- */ WHERE id = 1",
            "SELECT val FROM kv --\rWHERE id = 1",
            "SELECT $$a--$$\n$$ AS x FROM t",
            "SELECT id FROM t WHERE s = 'a'\x0b\n'b'",
            "SELECT val AS  of FROM kv WHERE id = 1",
        ] {
            let edge = crate::edge::fingerprint::analyze(sql, "db", "u", &[]);
            let cache = n.normalize(sql);
            assert_eq!(cache.fingerprint, edge.fingerprint, "{sql}");
            assert_eq!(cache.tables, edge.tables, "{sql}");
        }
    }
}
