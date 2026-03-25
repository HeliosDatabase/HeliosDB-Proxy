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

    /// Table-specific configurations
    pub table_configs: HashMap<String, TableCacheConfig>,

    /// Excluded tables (never cache)
    pub excluded_tables: Vec<String>,
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
        }
    }
}

impl CacheConfig {
    /// Create a minimal configuration (L1 only)
    pub fn minimal() -> Self {
        Self {
            enabled: true,
            l1: L1Config::default(),
            l2: L2Config { enabled: false, ..Default::default() },
            l3: L3Config { enabled: false, ..Default::default() },
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
                size_mb: 1024,
                ttl: Duration::from_secs(600),
                ..Default::default()
            },
            l3: L3Config { enabled: false, ..Default::default() },
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

        if self.l2.size_mb == 0 && self.l2.enabled {
            return Err("L2 cache size cannot be 0 when enabled".to_string());
        }

        if self.l3.similarity_threshold < 0.0 || self.l3.similarity_threshold > 1.0 {
            return Err("L3 similarity threshold must be between 0.0 and 1.0".to_string());
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

    /// Time-to-live for cached entries
    pub ttl: Duration,
}

impl Default for L1Config {
    fn default() -> Self {
        Self {
            enabled: true,
            size: 500,
            ttl: Duration::from_secs(30),
        }
    }
}

/// L2 warm cache configuration (shared)
#[derive(Debug, Clone)]
pub struct L2Config {
    /// Enable L2 cache
    pub enabled: bool,

    /// Maximum cache size in MB
    pub size_mb: usize,

    /// Time-to-live for cached entries
    pub ttl: Duration,

    /// Enable query normalization
    pub normalize_queries: bool,

    /// Storage backend
    pub storage: StorageBackend,

    /// Memory-mapped file path (for mmap backend)
    pub mmap_path: Option<PathBuf>,
}

impl Default for L2Config {
    fn default() -> Self {
        Self {
            enabled: true,
            size_mb: 256,
            ttl: Duration::from_secs(300),
            normalize_queries: true,
            storage: StorageBackend::Memory,
            mmap_path: None,
        }
    }
}

/// Storage backend for L2 cache
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    /// In-process memory (lost on restart)
    Memory,
    /// Memory-mapped file (survives restarts)
    Mmap,
}

impl Default for StorageBackend {
    fn default() -> Self {
        Self::Memory
    }
}

/// L3 semantic cache configuration
#[derive(Debug, Clone)]
pub struct L3Config {
    /// Enable L3 semantic cache
    pub enabled: bool,

    /// Cosine similarity threshold for cache hits
    pub similarity_threshold: f32,

    /// Maximum entries in semantic cache
    pub max_entries: usize,

    /// Time-to-live for semantic cache entries
    pub ttl: Duration,

    /// Ollama endpoint for embeddings
    pub embedding_endpoint: String,

    /// Embedding model name
    pub embedding_model: String,

    /// Embedding dimension
    pub embedding_dim: usize,
}

impl Default for L3Config {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default (requires Ollama)
            similarity_threshold: 0.92,
            max_entries: 5000,
            ttl: Duration::from_secs(3600),
            embedding_endpoint: "http://localhost:11434".to_string(),
            embedding_model: "all-minilm".to_string(),
            embedding_dim: 384, // all-MiniLM-L6-v2 dimension
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

    /// Exclude this table from caching
    pub exclude: bool,

    /// Columns to exclude from cached results
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
        assert_eq!(config.l2.size_mb, 256);
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
        assert_eq!(config.l2.size_mb, 1024);
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
    }

    #[test]
    fn test_storage_backend() {
        assert_eq!(StorageBackend::default(), StorageBackend::Memory);
    }
}
