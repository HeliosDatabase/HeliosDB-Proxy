//! Cache Configuration
//!
//! Configuration structures for the multi-tier query cache.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// Main cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Enable/disable cache globally
    pub enabled: bool,

    /// L1 hot cache configuration
    pub l1: L1Config,

    /// L2 warm cache configuration
    pub l2: L2Config,

    /// L3 semantic cache configuration
    pub l3: L3Config,

    /// Cache invalidation configuration
    pub invalidation: InvalidationConfig,

    /// Default TTL for cached results
    pub default_ttl: Duration,

    /// Maximum result size to cache (bytes)
    pub max_result_size: usize,

    /// Table-specific configurations. `QueryCache` reads only their `ttl`
    /// (keys are lower-case, unqualified table names).
    pub table_configs: HashMap<String, TableCacheConfig>,

    /// Excluded tables. Not read by `QueryCache`: the daemon enforces
    /// `[cache] exclude_tables` in the relay, ahead of both result caches.
    pub excluded_tables: Vec<String>,

    /// Refresh-ahead: once a shared (L2) entry's age reaches this fraction of
    /// its TTL, one reader refills it (single flight) while every other is
    /// served the entry. `0.0` (default) is off; otherwise in (0, 1). Needs
    /// L2.
    pub refresh_ahead_ratio: f64,

    /// How long a refresh-ahead claim holds before the next reader that finds
    /// the entry due takes it over. Default 5 s; non-zero when refresh-ahead
    /// is on.
    pub refresh_ahead_claim_timeout: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            l1: L1Config::default(),
            l2: L2Config::default(),
            l3: L3Config::default(),
            invalidation: InvalidationConfig::default(),
            default_ttl: Duration::from_secs(300), // 5 minutes
            max_result_size: 10 * 1024 * 1024,     // 10 MB
            table_configs: HashMap::new(),
            excluded_tables: Vec::new(),
            refresh_ahead_ratio: 0.0,
            refresh_ahead_claim_timeout: Duration::from_secs(5),
        }
    }
}

impl CacheConfig {
    /// Create a minimal configuration (L1 only)
    pub fn minimal() -> Self {
        Self {
            enabled: true,
            l1: L1Config::default(),
            l2: L2Config {
                enabled: false,
                ..Default::default()
            },
            l3: L3Config {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Create a configuration optimized for high-throughput reads
    pub fn high_throughput() -> Self {
        Self {
            enabled: true,
            l1: L1Config {
                size: 2000,
                ttl: Duration::from_secs(60),
                ..Default::default()
            },
            l2: L2Config {
                max_bytes: 1024 * 1024 * 1024,
                ttl: Duration::from_secs(600),
                ..Default::default()
            },
            l3: L3Config {
                enabled: false,
                ..Default::default()
            },
            default_ttl: Duration::from_secs(300),
            ..Default::default()
        }
    }

    /// Create a configuration optimized for AI/RAG workloads
    pub fn ai_workload() -> Self {
        Self {
            enabled: true,
            l1: L1Config::default(),
            l2: L2Config::default(),
            l3: L3Config {
                enabled: true,
                similarity_threshold: 0.90,
                max_entries: 10000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.l1.size == 0 && self.l1.enabled {
            return Err("L1 cache size cannot be 0 when enabled".to_string());
        }

        if self.l1.max_bytes == 0 && self.l1.enabled {
            return Err("L1 cache max_bytes cannot be 0 when enabled".to_string());
        }

        if self.l2.max_bytes == 0 && self.l2.enabled {
            return Err("L2 cache max_bytes cannot be 0 when enabled".to_string());
        }

        if self.l2.compression == crate::config::CacheCompression::Zstd
            && !crate::config::CACHE_ZSTD_LEVELS.contains(&self.l2.zstd_level)
        {
            return Err(format!(
                "L2 zstd_level must be in {}..={}",
                crate::config::CACHE_ZSTD_LEVELS.start(),
                crate::config::CACHE_ZSTD_LEVELS.end()
            ));
        }

        let r = self.refresh_ahead_ratio;
        if !(r == 0.0 || (r > 0.0 && r < 1.0)) {
            return Err(format!(
                "refresh_ahead_ratio must be 0 (off) or in (0, 1), got {r}"
            ));
        }
        if r > 0.0 && self.refresh_ahead_claim_timeout.is_zero() {
            return Err(
                "refresh_ahead_claim_timeout cannot be 0 when refresh-ahead is on".to_string(),
            );
        }

        let t = self.l3.similarity_threshold;
        if !(t > 0.0 && t <= 1.0) {
            return Err("L3 similarity threshold must be > 0.0 and <= 1.0".to_string());
        }
        if t < 1.0 && self.l3.semantic_match == crate::config::SemanticMatch::Verbatim {
            return Err(
                "L3 similarity threshold below 1.0 needs semantic_match = Canonical".to_string(),
            );
        }

        // The grammar parse it bounds runs whether or not the tier is enabled
        // (a per-table TTL longer than the default) and takes about a hundred
        // times the statement.
        if self.l3.max_statement_bytes > crate::config::SEMANTIC_MAX_STATEMENT_BYTES_CEILING {
            return Err(format!(
                "L3 max_statement_bytes must be at most {}",
                crate::config::SEMANTIC_MAX_STATEMENT_BYTES_CEILING
            ));
        }

        if self.l3.enabled {
            let l3 = &self.l3;
            for (name, zero) in [
                ("max_entries", l3.max_entries == 0),
                ("max_variants", l3.max_variants == 0),
                ("embedding_dim", l3.embedding_dim == 0),
                ("ttl", l3.ttl.is_zero()),
                ("embedding_timeout", l3.embedding_timeout.is_zero()),
                ("embedding_retry_after", l3.embedding_retry_after.is_zero()),
                ("max_statement_bytes", l3.max_statement_bytes == 0),
            ] {
                if zero {
                    return Err(format!("L3 {name} cannot be 0 when enabled"));
                }
            }
            if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&l3.embedding_concurrency) {
                return Err(format!(
                    "L3 embedding_concurrency must be in 1..={}",
                    tokio::sync::Semaphore::MAX_PERMITS
                ));
            }
        }

        Ok(())
    }
}

/// L1 hot cache configuration (per-connection)
#[derive(Debug, Clone)]
pub struct L1Config {
    /// Enable L1 cache
    pub enabled: bool,

    /// Maximum entries per connection
    pub size: usize,

    /// Aggregate byte budget for retained payloads (C-01). `0` means
    /// unbounded (entry count still applies). Default 32 MiB.
    pub max_bytes: usize,

    /// Time-to-live for cached entries
    pub ttl: Duration,
}

impl Default for L1Config {
    fn default() -> Self {
        Self {
            enabled: true,
            size: 500,
            max_bytes: 32 * 1024 * 1024,
            ttl: Duration::from_secs(30),
        }
    }
}

/// L2 warm cache configuration (shared)
#[derive(Debug, Clone)]
pub struct L2Config {
    /// Enable L2 cache
    pub enabled: bool,

    /// Aggregate byte budget for retained entries, counted at their stored
    /// size (a compressed payload counts its compressed bytes) plus per-entry
    /// overhead (`[cache] l2_max_bytes`). Default 256 MiB. Replaces the former
    /// `size_mb`.
    pub max_bytes: usize,

    /// How a payload of at least `compression_min_bytes` is stored
    /// (`[cache] l2_compression`): raw (the default), LZ4 or zstd. A hit
    /// decompresses before the result is served or promoted to L1; a payload
    /// compression does not shrink is stored raw.
    pub compression: crate::config::CacheCompression,

    /// Smallest payload `compression` applies to, in bytes. Default 4096.
    pub compression_min_bytes: usize,

    /// zstd level of `compression = Zstd` (1..=19). Default 3.
    pub zstd_level: i32,

    /// Time-to-live for cached entries
    pub ttl: Duration,

    /// Enable query normalization
    pub normalize_queries: bool,

    /// Storage backend
    pub storage: StorageBackend,

    /// Memory-mapped file path (for mmap backend)
    pub mmap_path: Option<PathBuf>,

    /// Entries the `cache-reclaim` thread removes per step when an eviction
    /// pass sheds write-retired entries (0 = all in one step).
    pub shed_batch: usize,

    /// Pause between those steps, so a shed of a whole retired working set
    /// (10^5-10^6 entries) does not contend with concurrent readers for its
    /// whole duration at full rate.
    pub shed_pause: Duration,
}

impl Default for L2Config {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bytes: 256 * 1024 * 1024,
            compression: crate::config::CacheCompression::None,
            compression_min_bytes: 4096,
            zstd_level: 3,
            ttl: Duration::from_secs(300),
            normalize_queries: true,
            storage: StorageBackend::Memory,
            mmap_path: None,
            shed_batch: 1024,
            shed_pause: Duration::from_millis(2),
        }
    }
}

/// Storage backend for L2 cache
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StorageBackend {
    /// In-process memory (lost on restart)
    #[default]
    Memory,
    /// File-backed spill storage (index is process-local; no restart recovery)
    Mmap,
}

/// L3 semantic cache configuration
#[derive(Debug, Clone)]
pub struct L3Config {
    /// Enable L3 semantic cache
    pub enabled: bool,

    /// How a statement matches an entry: its canonical form (the library
    /// default, which the approximate part needs) or its exact text
    /// (`[cache] semantic_match`, which defaults to `Verbatim` in the
    /// daemon). The canonical form assumes PostgreSQL semantics: once
    /// `QueryCache::note_heliosdb_backend` ran, the tier neither stores nor
    /// serves it.
    pub semantic_match: crate::config::SemanticMatch,

    /// Cosine similarity threshold for cache hits
    pub similarity_threshold: f32,

    /// Maximum entries in semantic cache (all its shards together), and
    /// maximum remembered column-name embeddings. Also sets the shard count:
    /// `max_entries / 32` rounded down to a power of two, 1 to 64.
    pub max_entries: usize,

    /// Maximum entries for one statement shape that differ only in their
    /// bare select-list columns (a full shape replaces its oldest). Bounds
    /// the work of one lookup. Default 16.
    pub max_variants: usize,

    /// Aggregate byte budget for retained entries plus remembered
    /// embeddings (C-01). Embeddings take at most a quarter of it and yield
    /// to entries, which they never evict. `0` means unbounded (entry count
    /// still applies, to both). Default 64 MiB.
    pub max_bytes: usize,

    /// Time-to-live for semantic cache entries
    pub ttl: Duration,

    /// Ollama endpoint for embeddings
    pub embedding_endpoint: String,

    /// Embedding model name
    pub embedding_model: String,

    /// Embedding dimension
    pub embedding_dim: usize,

    /// Total time limit of one embedding call (was a fixed 30 s). Calls run
    /// in the background; a lookup never waits on one. Default 5 s.
    pub embedding_timeout: Duration,

    /// Embedding calls in flight at most (was a fixed 10). A call that finds
    /// none free is skipped, never queued. Default 4.
    pub embedding_concurrency: usize,

    /// After an endpoint failure, no embedding call is made for this long;
    /// then one call probes the endpoint. Default 30 s.
    pub embedding_retry_after: Duration,

    /// Longest statement the tier's grammar is parsed for, in bytes. A
    /// longer one is not tokenized (the tokens and the parse take about a
    /// hundred times the text): it is outside the grammar, so it never uses
    /// the tier, and its relations count as incomplete for a per-table TTL
    /// longer than the default. The query cache applies it whether or not
    /// the tier is enabled. At most
    /// [`crate::config::SEMANTIC_MAX_STATEMENT_BYTES_CEILING`] (1 MiB).
    /// Default 16 KiB.
    pub max_statement_bytes: usize,
}

impl Default for L3Config {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default
            semantic_match: crate::config::SemanticMatch::Canonical,
            similarity_threshold: 0.92,
            max_entries: 5000,
            max_variants: 16,
            max_bytes: 64 * 1024 * 1024,
            ttl: Duration::from_secs(3600),
            embedding_endpoint: "http://localhost:11434".to_string(),
            embedding_model: "all-minilm".to_string(),
            embedding_dim: 384, // all-MiniLM-L6-v2 dimension
            embedding_timeout: Duration::from_secs(5),
            embedding_concurrency: 4,
            embedding_retry_after: Duration::from_secs(30),
            max_statement_bytes: 16 * 1024,
        }
    }
}

/// Cache invalidation configuration
#[derive(Debug, Clone)]
pub struct InvalidationConfig {
    /// Invalidation mode
    pub mode: super::InvalidationMode,

    /// Subscribe to WAL for automatic invalidation
    pub wal_subscribe: bool,

    /// Fallback TTL when WAL is unavailable
    pub ttl_fallback: Duration,
}

impl Default for InvalidationConfig {
    fn default() -> Self {
        Self {
            mode: super::InvalidationMode::Wal,
            wal_subscribe: true,
            ttl_fallback: Duration::from_secs(60),
        }
    }
}

/// Table-specific cache configuration
#[derive(Debug, Clone)]
pub struct TableCacheConfig {
    /// Time-to-live for this table
    pub ttl: Duration,

    /// Exclude this table from caching. Not read by `QueryCache`: the daemon
    /// enforces `[cache] exclude_tables` in the relay.
    pub exclude: bool,

    /// Columns to exclude from cached results. Not read by `QueryCache`;
    /// column-level exclusion is not offered by the daemon.
    pub exclude_columns: Vec<String>,
}

impl Default for TableCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            exclude: false,
            exclude_columns: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CacheConfig::default();
        assert!(config.enabled);
        assert!(config.l1.enabled);
        assert!(config.l2.enabled);
        assert!(!config.l3.enabled); // Disabled by default
        assert_eq!(config.l1.size, 500);
        assert_eq!(config.l1.max_bytes, 32 * 1024 * 1024);
        assert_eq!(config.l2.max_bytes, 256 * 1024 * 1024);
        assert_eq!(config.l2.compression, crate::config::CacheCompression::None);
    }

    #[test]
    fn test_minimal_config() {
        let config = CacheConfig::minimal();
        assert!(config.l1.enabled);
        assert!(!config.l2.enabled);
        assert!(!config.l3.enabled);
    }

    #[test]
    fn test_high_throughput_config() {
        let config = CacheConfig::high_throughput();
        assert_eq!(config.l1.size, 2000);
        assert_eq!(config.l2.max_bytes, 1024 * 1024 * 1024);
        assert!(!config.l3.enabled);
    }

    #[test]
    fn test_ai_workload_config() {
        let config = CacheConfig::ai_workload();
        assert!(config.l3.enabled);
        assert_eq!(config.l3.similarity_threshold, 0.90);
    }

    #[test]
    fn test_validation() {
        let config = CacheConfig::default();
        assert!(config.validate().is_ok());

        let mut invalid = CacheConfig::default();
        invalid.l1.size = 0;
        assert!(invalid.validate().is_err());

        let mut invalid2 = CacheConfig::default();
        invalid2.l3.similarity_threshold = 1.5;
        assert!(invalid2.validate().is_err());

        let mut no_l2_budget = CacheConfig::default();
        no_l2_budget.l2.max_bytes = 0;
        assert!(no_l2_budget.validate().unwrap_err().contains("max_bytes"));
        no_l2_budget.l2.enabled = false;
        assert!(
            no_l2_budget.validate().is_ok(),
            "only checked while enabled"
        );

        // The zstd level matters only to the zstd codec.
        let mut zstd = CacheConfig::default();
        zstd.l2.zstd_level = 0;
        assert!(zstd.validate().is_ok(), "unused without zstd");
        zstd.l2.compression = crate::config::CacheCompression::Zstd;
        assert!(zstd.validate().unwrap_err().contains("zstd_level"));
        for level in [1, 3, 19] {
            zstd.l2.zstd_level = level;
            assert!(zstd.validate().is_ok(), "level {level}");
        }
        zstd.l2.zstd_level = 20;
        assert!(zstd.validate().is_err());
    }

    #[test]
    fn validation_rejects_bad_l3_values() {
        for t in [f32::NAN, 0.0, 1.5, -0.1] {
            let mut c = CacheConfig::default();
            c.l3.similarity_threshold = t;
            assert!(c.validate().is_err(), "threshold {t}");
        }
        let mut one = CacheConfig::default();
        one.l3.similarity_threshold = 1.0;
        assert!(one.validate().is_ok());
        let enabled = || {
            let mut c = CacheConfig::default();
            c.l3.enabled = true;
            c
        };
        assert!(enabled().validate().is_ok());
        let breakers: [fn(&mut L3Config); 9] = [
            |l3| l3.max_entries = 0,
            |l3| l3.max_variants = 0,
            |l3| l3.embedding_dim = 0,
            |l3| l3.ttl = Duration::ZERO,
            |l3| l3.embedding_timeout = Duration::ZERO,
            |l3| l3.embedding_retry_after = Duration::ZERO,
            |l3| l3.max_statement_bytes = 0,
            |l3| l3.embedding_concurrency = 0,
            |l3| l3.embedding_concurrency = tokio::sync::Semaphore::MAX_PERMITS + 1,
        ];
        for (i, brk) in breakers.iter().enumerate() {
            let mut c = enabled();
            brk(&mut c.l3);
            assert!(c.validate().is_err(), "breaker {i}");
            // Off, the tier's sizes are not checked.
            c.l3.enabled = false;
            assert!(c.validate().is_ok(), "breaker {i} while disabled");
        }
        // The statement bound has a ceiling, on or off: the parse it bounds
        // runs for per-table TTLs too.
        for on in [true, false] {
            let mut c = enabled();
            c.l3.enabled = on;
            c.l3.max_statement_bytes = crate::config::SEMANTIC_MAX_STATEMENT_BYTES_CEILING;
            assert!(c.validate().is_ok(), "at the ceiling, enabled={on}");
            c.l3.max_statement_bytes += 1;
            let err = c.validate().unwrap_err();
            assert!(err.contains("max_statement_bytes"), "{err}");
        }
        assert!(CacheConfig::ai_workload().validate().is_ok());
        // Approximate substitution works on the canonical form only.
        let mut verbatim = CacheConfig::default();
        verbatim.l3.semantic_match = crate::config::SemanticMatch::Verbatim;
        assert!(verbatim.validate().is_err(), "0.92 with verbatim");
        verbatim.l3.similarity_threshold = 1.0;
        assert!(verbatim.validate().is_ok());
    }

    #[test]
    fn test_storage_backend() {
        assert_eq!(StorageBackend::default(), StorageBackend::Memory);
    }

    #[test]
    fn validation_bounds_refresh_ahead() {
        let d = CacheConfig::default();
        assert_eq!(d.refresh_ahead_ratio, 0.0, "off by default");
        assert_eq!(d.refresh_ahead_claim_timeout, Duration::from_secs(5));
        for r in [0.0, 0.01, 0.5, 0.99] {
            let c = CacheConfig {
                refresh_ahead_ratio: r,
                ..Default::default()
            };
            assert!(c.validate().is_ok(), "ratio {r}");
        }
        for r in [1.0, 1.5, -0.1, f64::NAN, f64::INFINITY] {
            let c = CacheConfig {
                refresh_ahead_ratio: r,
                ..Default::default()
            };
            let err = c.validate().unwrap_err();
            assert!(err.contains("refresh_ahead_ratio"), "{r}: {err}");
        }
        let mut c = CacheConfig {
            refresh_ahead_claim_timeout: Duration::ZERO,
            ..Default::default()
        };
        assert!(c.validate().is_ok(), "the timeout is not read while off");
        c.refresh_ahead_ratio = 0.8;
        let err = c.validate().unwrap_err();
        assert!(err.contains("refresh_ahead_claim_timeout"), "{err}");
    }
}
