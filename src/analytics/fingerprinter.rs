//! Query Fingerprinting
//!
//! Normalize queries and generate fingerprints for grouping similar queries.

use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use regex::Regex;

/// Default bound on the memoized fingerprint cache. Mirrors the
/// `[analytics] fingerprint_cache_size` default in `proxy.toml`.
pub const DEFAULT_FINGERPRINT_CACHE_SIZE: usize = 10_000;

/// Default per-statement byte cap for memoization. Mirrors the
/// `[analytics] fingerprint_cache_max_sql_bytes` default in `proxy.toml`.
///
/// A count-only bound is not a memory bound: the memo key is the raw SQL, so
/// without a per-entry cap a client issuing large distinct statements (a bulk
/// `INSERT` with inline `VALUES`, say) could pin `fingerprint_cache_size`
/// whole statements of arbitrary size. Statements longer than this are still
/// fingerprinted, just never memoized, which caps memo retention at
/// `fingerprint_cache_size * fingerprint_cache_max_sql_bytes` bytes of SQL
/// (~40 MiB with both defaults).
pub const DEFAULT_FINGERPRINT_CACHE_MAX_SQL_BYTES: usize = 4096;

/// Query fingerprinter
#[derive(Debug)]
pub struct QueryFingerprinter {
    /// Regex for string literals
    string_literal_re: Regex,
    /// Regex for numeric literals
    numeric_literal_re: Regex,
    /// Regex for IN lists
    in_list_re: Regex,
    /// Regex for whitespace
    whitespace_re: Regex,
    /// Regex for UUID
    uuid_re: Regex,
    /// Regex for hex values
    hex_re: Regex,
    /// Memo of raw SQL -> fingerprint. Fingerprinting costs six regex passes
    /// plus a whole-statement case conversion; real workloads replay a small
    /// set of statement shapes, so memoizing turns the repeat case into one
    /// hash + map lookup and skips ALL regex work. Bounded by
    /// `cache_capacity`: when it fills, the map is cleared wholesale (a cheap
    /// stand-in for LRU that keeps memory flat without per-hit bookkeeping).
    cache: DashMap<Box<str>, Arc<QueryFingerprint>>,
    /// Bound on the NUMBER of `cache` entries. `0` disables memoization.
    cache_capacity: usize,
    /// Bound on the SIZE of a memoized statement, in bytes. A statement longer
    /// than this is fingerprinted but not memoized, so the memo's total
    /// retention is bounded in bytes and not only in entries.
    cache_max_sql_bytes: usize,
    /// Occupancy of `cache`, tracked here because `DashMap::len()` read-locks
    /// every shard — far too expensive to call on each memo miss. Concurrent
    /// misses can overshoot `cache_capacity` slightly between the check and
    /// the insert (bounded by the number of racing inserters, corrected by the
    /// next clear); the bound is a memory guard, not an exact quota.
    cache_len: AtomicUsize,
}

impl QueryFingerprinter {
    /// Create a new fingerprinter with the default memo-cache bound.
    pub fn new() -> Self {
        Self::with_cache_size(DEFAULT_FINGERPRINT_CACHE_SIZE)
    }

    /// Create a fingerprinter with an explicit memo-entry bound and the default
    /// per-statement byte cap. `0` disables memoization, so every call
    /// recomputes (the pre-cache behaviour).
    pub fn with_cache_size(cache_capacity: usize) -> Self {
        Self::with_cache_limits(cache_capacity, DEFAULT_FINGERPRINT_CACHE_MAX_SQL_BYTES)
    }

    /// Create a fingerprinter with both memo bounds: at most `cache_capacity`
    /// entries, and only for statements of at most `cache_max_sql_bytes`
    /// bytes. Wired to `[analytics] fingerprint_cache_size` /
    /// `fingerprint_cache_max_sql_bytes`.
    pub fn with_cache_limits(cache_capacity: usize, cache_max_sql_bytes: usize) -> Self {
        Self {
            string_literal_re: Regex::new(r"'[^']*'").expect("Invalid regex"),
            numeric_literal_re: Regex::new(r"\b\d+(\.\d+)?\b").expect("Invalid regex"),
            in_list_re: Regex::new(r"(?i)IN\s*\([^)]+\)").expect("Invalid regex"),
            whitespace_re: Regex::new(r"\s+").expect("Invalid regex"),
            uuid_re: Regex::new(
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            )
            .expect("Invalid regex"),
            hex_re: Regex::new(r"0x[0-9a-fA-F]+").expect("Invalid regex"),
            cache: DashMap::new(),
            cache_capacity,
            cache_max_sql_bytes,
            cache_len: AtomicUsize::new(0),
        }
    }

    /// Generate fingerprint from query (memoized).
    ///
    /// Clones out of the memoized `Arc`; callers on a hot path should prefer
    /// [`Self::fingerprint_cached`] (or [`Self::fingerprint_cached_lower`],
    /// which also shares the caller's folded copy) and keep the `Arc`.
    pub fn fingerprint(&self, query: &str) -> QueryFingerprint {
        (*self.fingerprint_cached(query)).clone()
    }

    /// Memoizing fingerprint returning the shared entry, so repeat statements
    /// pay neither the regex passes nor a clone of the normalized text.
    /// Allocates the ASCII-folded copy itself; the analytics ingest path holds
    /// one already and calls [`Self::fingerprint_cached_lower`] instead.
    pub fn fingerprint_cached(&self, query: &str) -> Arc<QueryFingerprint> {
        let lower = ascii_lower(query);
        self.fingerprint_cached_lower(query, &lower)
    }

    /// [`Self::fingerprint_cached`] given the caller's ASCII-folded copy of the
    /// statement (see [`ascii_lower`]), so the whole ingest path pays exactly
    /// one case conversion per query.
    ///
    /// `lower` MUST be `ascii_lower(query)`: it is used for keyword offsets
    /// that are then applied to `query`, which only holds because ASCII
    /// folding is length-preserving.
    pub fn fingerprint_cached_lower(&self, query: &str, lower: &str) -> Arc<QueryFingerprint> {
        debug_assert_eq!(
            query.len(),
            lower.len(),
            "`lower` must be the ASCII-folded copy of `query`"
        );
        // Skip the memo entirely for disabled caches and for oversized
        // statements: the key is the raw SQL, so memoizing a huge one-off
        // statement would retain it for the life of the cache generation.
        if self.cache_capacity == 0 || query.len() > self.cache_max_sql_bytes {
            return Arc::new(self.compute_fingerprint_lower(query, lower));
        }
        if let Some(hit) = self.cache.get(query) {
            return hit.clone();
        }
        let fingerprint = Arc::new(self.compute_fingerprint_lower(query, lower));
        // Bound the memo. Clearing on overflow is deliberate: it keeps the hot
        // path free of LRU bookkeeping, and a workload with more than
        // `cache_capacity` live shapes gains little from partial eviction.
        if self.cache_len.load(Ordering::Relaxed) >= self.cache_capacity {
            self.cache.clear();
            self.cache_len.store(0, Ordering::Relaxed);
        }
        if self
            .cache
            .insert(query.into(), fingerprint.clone())
            .is_none()
        {
            self.cache_len.fetch_add(1, Ordering::Relaxed);
        }
        fingerprint
    }

    /// Number of memoized fingerprints currently held.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Compute a fingerprint, bypassing the memo.
    fn compute_fingerprint(&self, query: &str) -> QueryFingerprint {
        let lower = ascii_lower(query);
        self.compute_fingerprint_lower(query, &lower)
    }

    /// Compute a fingerprint from the statement and its ASCII-folded copy.
    ///
    /// ONE case conversion is shared between table extraction, operation
    /// detection and (on the ingest path) intent classification; each of those
    /// previously allocated a whole-statement copy of its own.
    fn compute_fingerprint_lower(&self, query: &str, lower: &str) -> QueryFingerprint {
        let normalized = self.normalize(query);
        let hash = self.compute_hash(&normalized);

        QueryFingerprint {
            hash,
            normalized,
            tables: self.extract_tables_lower(query, lower),
            operation: Self::detect_operation_lower(lower.trim()),
            original_length: query.len(),
        }
    }

    /// Apply one regex replacement, reusing the input buffer when the pattern
    /// does not match. `Regex::replace_all` already returns a `Cow`; the old
    /// `.to_string()` threw that away and copied the whole statement on every
    /// pass, so a six-pass normalize allocated six times even for a query
    /// containing no literals at all.
    fn replace_all_owned(re: &Regex, s: String, replacement: &str) -> String {
        let replaced = match re.replace_all(&s, replacement) {
            Cow::Borrowed(_) => None,
            Cow::Owned(out) => Some(out),
        };
        replaced.unwrap_or(s)
    }

    /// Normalize query (remove literals, standardize whitespace)
    pub fn normalize(&self, query: &str) -> String {
        let mut normalized = query.to_string();

        // Replace UUIDs first
        normalized = Self::replace_all_owned(&self.uuid_re, normalized, "?");

        // Replace hex values
        normalized = Self::replace_all_owned(&self.hex_re, normalized, "?");

        // Replace string literals with ?
        normalized = Self::replace_all_owned(&self.string_literal_re, normalized, "?");

        // Replace numeric literals with ?
        normalized = Self::replace_all_owned(&self.numeric_literal_re, normalized, "?");

        // Replace IN lists with (?)
        normalized = Self::replace_all_owned(&self.in_list_re, normalized, "IN (?)");

        // Normalize whitespace
        normalized = Self::replace_all_owned(&self.whitespace_re, normalized, " ");

        normalized.trim().to_lowercase()
    }

    /// Compute hash of normalized query
    fn compute_hash(&self, normalized: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
    }

    /// Extract table names from a query, given its ASCII-folded copy.
    ///
    /// `lower` is only used for case-insensitive keyword search; every
    /// extracted identifier still comes from the original `query`, so keyword
    /// offsets found in `lower` are applied to `query`. That mapping is only
    /// sound because [`ascii_lower`] is length-preserving — a Unicode
    /// `to_lowercase()` is NOT (U+0130 folds to two chars / three bytes), and
    /// an offset from such a copy can exceed `query.len()` or land mid-char.
    /// Every slice below additionally goes through `str::get`, so a future
    /// caller that passes a mismatched copy gets no table rather than a panic
    /// on the shared analytics consumer task.
    fn extract_tables_lower(&self, query: &str, lower: &str) -> Vec<String> {
        let mut tables = HashSet::new();
        let push_after = |offset: usize, tables: &mut HashSet<String>| {
            if let Some(rest) = query.get(offset..) {
                if let Some(table) = self.extract_first_identifier(rest) {
                    tables.insert(table);
                }
            }
        };

        // FROM clause
        if let Some(from_pos) = lower.find("from") {
            push_after(from_pos + 4, &mut tables);
        }

        // JOIN clauses
        for keyword in [
            "join",
            "inner join",
            "left join",
            "right join",
            "outer join",
        ] {
            let mut search_pos = 0;
            while let Some(pos) = lower[search_pos..].find(keyword) {
                let absolute_pos = search_pos + pos + keyword.len();
                push_after(absolute_pos, &mut tables);
                search_pos = absolute_pos;
            }
        }

        // INSERT INTO
        if let Some(pos) = lower.find("insert into") {
            push_after(pos + 11, &mut tables);
        }

        // UPDATE
        if let Some(pos) = lower.find("update") {
            push_after(pos + 6, &mut tables);
        }

        // DELETE FROM
        if let Some(pos) = lower.find("delete from") {
            push_after(pos + 11, &mut tables);
        }

        tables.into_iter().collect()
    }

    /// Extract first identifier from string
    fn extract_first_identifier(&self, s: &str) -> Option<String> {
        let trimmed = s.trim();
        let mut chars = trimmed.chars().peekable();

        // Skip leading whitespace
        while chars.peek().map(|c| c.is_whitespace()).unwrap_or(false) {
            chars.next();
        }

        // Collect identifier characters
        let mut ident = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_alphanumeric() || c == '_' || c == '.' || c == '"' {
                ident.push(c);
                chars.next();
            } else {
                break;
            }
        }

        if ident.is_empty() {
            None
        } else {
            // Remove quotes and return lowercase
            let cleaned = ident.replace('"', "").to_lowercase();
            Some(cleaned)
        }
    }

    /// Detect operation type from an already-lowercased, trimmed statement.
    #[allow(clippy::if_same_then_else)]
    fn detect_operation_lower(trimmed: &str) -> OperationType {
        if trimmed.starts_with("select") {
            OperationType::Select
        } else if trimmed.starts_with("insert") {
            OperationType::Insert
        } else if trimmed.starts_with("update") {
            OperationType::Update
        } else if trimmed.starts_with("delete") {
            OperationType::Delete
        } else if trimmed.starts_with("create") {
            OperationType::Ddl
        } else if trimmed.starts_with("alter") {
            OperationType::Ddl
        } else if trimmed.starts_with("drop") {
            OperationType::Ddl
        } else if trimmed.starts_with("begin") || trimmed.starts_with("start transaction") {
            OperationType::Transaction
        } else if trimmed.starts_with("commit") || trimmed.starts_with("rollback") {
            OperationType::Transaction
        } else if trimmed.starts_with("set") {
            OperationType::Utility
        } else if trimmed.starts_with("explain") {
            OperationType::Utility
        } else if trimmed.starts_with("analyze") {
            OperationType::Utility
        } else {
            OperationType::Other
        }
    }

    /// Raw-SQL convenience wrapper around [`Self::detect_operation_lower`].
    /// The ingest path always holds a lowercase copy already, so this exists
    /// only for direct/test callers.
    #[cfg(test)]
    fn detect_operation(&self, query: &str) -> OperationType {
        Self::detect_operation_lower(ascii_lower(query.trim()).as_str())
    }
}

/// ASCII-fold a statement: the ONE case conversion the analytics ingest path
/// takes, shared by fingerprinting and intent classification.
///
/// Deliberately ASCII-only rather than [`str::to_lowercase`]. Folding is used
/// two ways here — to find SQL keywords, which are ASCII, and to map the
/// offsets of those keywords back onto the original statement. Unicode folding
/// breaks the second use: `'\u{130}'.to_lowercase()` yields two chars / three
/// bytes for a two-byte input, so offsets taken from the folded copy no longer
/// address the original and slicing it panics. ASCII folding only rewrites
/// bytes `A-Z` in place, so byte offsets are identical in both strings and any
/// keyword match is guaranteed to sit on a char boundary of the original.
pub fn ascii_lower(query: &str) -> String {
    query.to_ascii_lowercase()
}

impl Default for QueryFingerprinter {
    fn default() -> Self {
        Self::new()
    }
}

/// Query fingerprint
#[derive(Debug, Clone)]
pub struct QueryFingerprint {
    /// 64-bit hash of normalized query
    pub hash: u64,

    /// Normalized query text
    pub normalized: String,

    /// Tables involved
    pub tables: Vec<String>,

    /// Operation type
    pub operation: OperationType,

    /// Original query length
    pub original_length: usize,
}

impl QueryFingerprint {
    /// Get a short identifier for this fingerprint
    pub fn short_id(&self) -> String {
        format!("{:016x}", self.hash)
    }
}

/// Operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Transaction,
    Utility,
    Other,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Select => write!(f, "SELECT"),
            OperationType::Insert => write!(f, "INSERT"),
            OperationType::Update => write!(f, "UPDATE"),
            OperationType::Delete => write!(f, "DELETE"),
            OperationType::Ddl => write!(f, "DDL"),
            OperationType::Transaction => write!(f, "TRANSACTION"),
            OperationType::Utility => write!(f, "UTILITY"),
            OperationType::Other => write!(f, "OTHER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprinter_new() {
        let fp = QueryFingerprinter::new();
        assert!(fp.string_literal_re.is_match("'hello'"));
    }

    #[test]
    fn test_normalize_string_literals() {
        let fp = QueryFingerprinter::new();

        let normalized = fp.normalize("SELECT * FROM users WHERE name = 'Alice'");
        assert_eq!(normalized, "select * from users where name = ?");
    }

    #[test]
    fn test_normalize_numeric_literals() {
        let fp = QueryFingerprinter::new();

        let normalized = fp.normalize("SELECT * FROM users WHERE id = 123 AND age > 25");
        assert_eq!(normalized, "select * from users where id = ? and age > ?");
    }

    #[test]
    fn test_normalize_in_list() {
        let fp = QueryFingerprinter::new();

        let normalized = fp.normalize("SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)");
        assert_eq!(normalized, "select * from users where id in (?)");
    }

    #[test]
    fn test_normalize_uuid() {
        let fp = QueryFingerprinter::new();

        let normalized =
            fp.normalize("SELECT * FROM users WHERE id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'");
        assert!(normalized.contains("?"));
    }

    #[test]
    fn test_same_fingerprint_different_values() {
        let fp = QueryFingerprinter::new();

        let fp1 = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = fp.fingerprint("SELECT * FROM users WHERE id = 2");

        assert_eq!(fp1.hash, fp2.hash);
        assert_eq!(fp1.normalized, fp2.normalized);
    }

    #[test]
    fn test_different_fingerprint_different_queries() {
        let fp = QueryFingerprinter::new();

        let fp1 = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = fp.fingerprint("SELECT * FROM orders WHERE id = 1");

        assert_ne!(fp1.hash, fp2.hash);
    }

    #[test]
    fn test_extract_tables() {
        let fp = QueryFingerprinter::new();

        let result = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        assert!(result.tables.contains(&"users".to_string()));

        let result = fp.fingerprint("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(result.tables.contains(&"users".to_string()));
        assert!(result.tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_detect_operation() {
        let fp = QueryFingerprinter::new();

        assert_eq!(
            fp.detect_operation("SELECT * FROM users"),
            OperationType::Select
        );
        assert_eq!(
            fp.detect_operation("INSERT INTO users VALUES (1)"),
            OperationType::Insert
        );
        assert_eq!(
            fp.detect_operation("UPDATE users SET name = 'Bob'"),
            OperationType::Update
        );
        assert_eq!(
            fp.detect_operation("DELETE FROM users WHERE id = 1"),
            OperationType::Delete
        );
        assert_eq!(
            fp.detect_operation("CREATE TABLE foo (id INT)"),
            OperationType::Ddl
        );
        assert_eq!(fp.detect_operation("BEGIN"), OperationType::Transaction);
    }

    /// A memo hit must be byte-for-byte the same fingerprint as a memo miss.
    #[test]
    fn test_fingerprint_memo_hit_matches_miss() {
        let sql = "SELECT a, b FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 42";

        let uncached = QueryFingerprinter::with_cache_size(0);
        let cached = QueryFingerprinter::new();

        let expected = uncached.fingerprint(sql);
        let first = cached.fingerprint(sql);
        let second = cached.fingerprint(sql); // served from the memo

        assert_eq!(cached.cache_len(), 1, "second call must be a memo hit");
        for got in [&first, &second] {
            assert_eq!(got.hash, expected.hash);
            assert_eq!(got.normalized, expected.normalized);
            assert_eq!(got.operation, expected.operation);
            assert_eq!(got.original_length, expected.original_length);
            let mut a = got.tables.clone();
            let mut b = expected.tables.clone();
            a.sort();
            b.sort();
            assert_eq!(a, b);
        }
    }

    /// Regression: statements whose Unicode lowercase is LONGER than the
    /// original must not panic. `'\u{130}'.to_lowercase()` is two chars /
    /// three bytes for a two-byte input, so a keyword offset taken from a
    /// `to_lowercase()` copy can point past the end of the original statement
    /// (or into the middle of a char). `SELECT \u{130} from` is 14 bytes but
    /// 15 lowercased, and `find("from") + 4 == 15` — slicing the original
    /// there panicked with "byte index 15 is out of range".
    #[test]
    fn test_fingerprint_length_changing_unicode_does_not_panic() {
        let fp = QueryFingerprinter::new();

        for sql in [
            "SELECT \u{130} from",
            "SELECT \u{130} FROM users",
            "select \u{130}\u{130}\u{130} from orders where name = '\u{130}'",
            "INSERT INTO \u{130}tbl VALUES (1)",
            "UPDATE \u{130} SET a = 1",
            "DELETE FROM \u{130} WHERE id = 1",
            // Other length-changing folds: German sharp s and the ligature fi.
            "SELECT * FROM \u{1E9E} JOIN \u{FB01} ON 1 = 1",
        ] {
            let out = fp.fingerprint(sql);
            assert_eq!(out.original_length, sql.len());
        }

        // The ASCII keyword scan still finds the real table names around them.
        let out = fp.fingerprint("SELECT \u{130} FROM users");
        assert!(
            out.tables.contains(&"users".to_string()),
            "tables: {:?}",
            out.tables
        );
    }

    /// ASCII folding is what makes the offset mapping sound: it must never
    /// change the byte length, unlike `str::to_lowercase`.
    #[test]
    fn test_ascii_lower_preserves_byte_length() {
        for sql in [
            "SELECT \u{130} FROM T",
            "\u{1E9E}\u{130}\u{FB01}",
            "SELECT * FROM users",
        ] {
            assert_eq!(ascii_lower(sql).len(), sql.len());
        }
        assert_eq!(ascii_lower("SeLeCt 1"), "select 1");
        // Non-ASCII is left alone (by design — SQL keywords are ASCII).
        assert_eq!(ascii_lower("\u{130}"), "\u{130}");
    }

    /// A statement larger than the per-entry byte cap is still fingerprinted
    /// but must never enter the memo: the key is the raw SQL, so caching it
    /// would retain the whole statement for the life of the cache generation.
    #[test]
    fn test_fingerprint_memo_skips_oversized_statements() {
        let fp = QueryFingerprinter::with_cache_limits(1000, 64);

        let small = "SELECT * FROM users WHERE id = 1";
        fp.fingerprint(small);
        assert_eq!(fp.cache_len(), 1);

        let big = format!("SELECT * FROM users WHERE id IN ({})", "1,".repeat(200));
        assert!(big.len() > 64);
        let a = fp.fingerprint(&big);
        let b = fp.fingerprint(&big);
        assert_eq!(
            fp.cache_len(),
            1,
            "oversized statement must not be memoized"
        );
        // Bypassing the memo must not change the answer.
        assert_eq!(a.hash, b.hash);
        assert_eq!(
            a.hash,
            QueryFingerprinter::with_cache_size(0)
                .fingerprint(&big)
                .hash
        );
    }

    /// The memo must stay bounded by its configured capacity.
    #[test]
    fn test_fingerprint_cache_is_bounded() {
        let fp = QueryFingerprinter::with_cache_size(4);
        for i in 0..64 {
            fp.fingerprint(&format!("SELECT c{i} FROM t{i}"));
        }
        assert!(
            fp.cache_len() <= 4,
            "memo grew past its bound: {}",
            fp.cache_len()
        );
    }

    /// `with_cache_size(0)` disables memoization entirely.
    #[test]
    fn test_fingerprint_cache_size_zero_disables_memo() {
        let fp = QueryFingerprinter::with_cache_size(0);
        fp.fingerprint("SELECT 1");
        fp.fingerprint("SELECT 1");
        assert_eq!(fp.cache_len(), 0);
    }

    /// Distinct statements must not collide in the memo (the key is the raw
    /// SQL, not a lossy digest of it).
    #[test]
    fn test_fingerprint_memo_keys_on_raw_sql() {
        let fp = QueryFingerprinter::new();
        let a = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let b = fp.fingerprint("SELECT * FROM orders WHERE id = 1");
        assert_ne!(a.hash, b.hash);
        assert_eq!(fp.cache_len(), 2);
    }

    /// Normalization must be unchanged for statements that match no literal
    /// pattern — the `Cow` fast path returns the buffer untouched.
    #[test]
    fn test_normalize_without_literals_unchanged() {
        let fp = QueryFingerprinter::new();
        assert_eq!(fp.normalize("SELECT  a FROM  t"), "select a from t");
    }

    /// Mixed-case SQL must fingerprint identically to its uppercase form —
    /// the scan now runs against a lowercase copy of the statement.
    #[test]
    fn test_fingerprint_is_case_insensitive_for_keywords() {
        let fp = QueryFingerprinter::with_cache_size(0);
        let upper = fp.fingerprint("SELECT * FROM Users WHERE id = 1");
        let mixed = fp.fingerprint("select * From Users where id = 1");
        assert_eq!(upper.operation, mixed.operation);
        assert_eq!(upper.tables, mixed.tables);
        assert!(upper.tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(OperationType::Select.to_string(), "SELECT");
        assert_eq!(OperationType::Insert.to_string(), "INSERT");
    }
}
