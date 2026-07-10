//! DataLoader
//!
//! Batching and caching for N+1 query prevention.

use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

/// DataLoader configuration
#[derive(Debug, Clone)]
pub struct DataLoaderConfig {
    /// Batch window duration
    pub batch_window: Duration,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Enable caching
    pub cache_enabled: bool,
    /// Cache TTL
    pub cache_ttl: Duration,
    /// Enable deduplication
    pub dedupe: bool,
}

impl Default for DataLoaderConfig {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_millis(10),
            max_batch_size: 100,
            cache_enabled: true,
            cache_ttl: Duration::from_secs(60),
            dedupe: true,
        }
    }
}

impl DataLoaderConfig {
    /// Create a new configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set batch window
    pub fn batch_window(mut self, duration: Duration) -> Self {
        self.batch_window = duration;
        self
    }

    /// Set max batch size
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Enable/disable caching
    pub fn cache(mut self, enabled: bool) -> Self {
        self.cache_enabled = enabled;
        self
    }

    /// Set cache TTL
    pub fn cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }
}

/// Batch result from loader function
#[derive(Debug, Clone)]
pub struct BatchResult<K, V> {
    /// Results mapped by key
    pub results: HashMap<K, V>,
    /// Keys that returned no results
    pub missing: Vec<K>,
}

impl<K: Eq + Hash, V> BatchResult<K, V> {
    /// Create a new batch result
    pub fn new(results: HashMap<K, V>) -> Self {
        Self {
            results,
            missing: Vec::new(),
        }
    }

    /// Create an empty result
    pub fn empty() -> Self {
        Self {
            results: HashMap::new(),
            missing: Vec::new(),
        }
    }

    /// Add missing keys
    pub fn with_missing(mut self, missing: Vec<K>) -> Self {
        self.missing = missing;
        self
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<&V> {
        self.results.get(key)
    }

    /// Check if a key is missing
    pub fn is_missing(&self, key: &K) -> bool
    where
        K: PartialEq,
    {
        self.missing.contains(key)
    }
}

/// Cache entry with TTL
#[derive(Debug, Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at: Instant,
}

impl<V> CacheEntry<V> {
    fn new(value: V, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

/// DataLoader for batching and caching
///
/// Prevents N+1 queries by batching multiple individual loads
/// into a single batch load.
#[derive(Debug)]
pub struct DataLoader<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Configuration
    config: DataLoaderConfig,
    /// Cache
    cache: std::sync::Mutex<HashMap<K, CacheEntry<V>>>,
    /// Pending requests
    pending: std::sync::Mutex<Vec<K>>,
    /// Statistics
    stats: std::sync::Mutex<DataLoaderStats>,
}

/// DataLoader statistics
#[derive(Debug, Clone, Default)]
pub struct DataLoaderStats {
    /// Total loads requested
    pub total_loads: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Batch loads executed
    pub batch_loads: u64,
    /// Average batch size
    pub avg_batch_size: f64,
}

impl DataLoaderStats {
    /// Get cache hit rate
    pub fn hit_rate(&self) -> f64 {
        if self.total_loads == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.total_loads as f64
        }
    }
}

impl<K, V> DataLoader<K, V>
where
    K: Eq + Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new DataLoader
    pub fn new(config: DataLoaderConfig) -> Self {
        Self {
            config,
            cache: std::sync::Mutex::new(HashMap::new()),
            pending: std::sync::Mutex::new(Vec::new()),
            stats: std::sync::Mutex::new(DataLoaderStats::default()),
        }
    }

    /// Load a single value
    pub fn load(&self, key: K) -> Option<V> {
        self.update_stats(|s| s.total_loads += 1);

        // Check cache first
        if self.config.cache_enabled {
            if let Some(value) = self.get_cached(&key) {
                self.update_stats(|s| s.cache_hits += 1);
                return Some(value);
            }
            self.update_stats(|s| s.cache_misses += 1);
        }

        // Add to pending
        self.pending.lock().unwrap().push(key);

        None
    }

    /// Load multiple values
    pub fn load_many(&self, keys: Vec<K>) -> HashMap<K, Option<V>> {
        let mut results = HashMap::new();

        for key in keys {
            results.insert(key.clone(), self.load(key));
        }

        results
    }

    /// Prime the cache with a value
    pub fn prime(&self, key: K, value: V) {
        if self.config.cache_enabled {
            let entry = CacheEntry::new(value, self.config.cache_ttl);
            self.cache.lock().unwrap().insert(key, entry);
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Clear a single key from the cache
    pub fn clear_key(&self, key: &K) {
        self.cache.lock().unwrap().remove(key);
    }

    /// Execute pending batch
    pub fn execute_batch<F>(&self, mut loader: F) -> BatchResult<K, V>
    where
        F: FnMut(Vec<K>) -> HashMap<K, V>,
    {
        // Take pending keys
        let keys: Vec<K> = {
            let mut pending = self.pending.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        if keys.is_empty() {
            return BatchResult::empty();
        }

        // Deduplicate if enabled
        let unique_keys: Vec<K> = if self.config.dedupe {
            let mut seen = std::collections::HashSet::new();
            keys.into_iter()
                .filter(|k| seen.insert(k.clone()))
                .collect()
        } else {
            keys
        };

        // Split into batches if needed
        let _batch_count = unique_keys.len().div_ceil(self.config.max_batch_size);

        let mut all_results = HashMap::new();

        for batch in unique_keys.chunks(self.config.max_batch_size) {
            let batch_keys: Vec<K> = batch.to_vec();
            let batch_size = batch_keys.len();

            // Execute loader
            let results = loader(batch_keys);

            self.update_stats(|s| {
                s.batch_loads += 1;
                let total_batches = s.batch_loads as f64;
                s.avg_batch_size = ((s.avg_batch_size * (total_batches - 1.0)) + batch_size as f64)
                    / total_batches;
            });

            // Cache results
            if self.config.cache_enabled {
                let mut cache = self.cache.lock().unwrap();
                for (k, v) in &results {
                    cache.insert(k.clone(), CacheEntry::new(v.clone(), self.config.cache_ttl));
                }
            }

            all_results.extend(results);
        }

        BatchResult::new(all_results)
    }

    /// Get cached value if valid
    fn get_cached(&self, key: &K) -> Option<V> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(entry) = cache.get(key) {
            if !entry.is_expired() {
                return Some(entry.value.clone());
            } else {
                cache.remove(key);
            }
        }

        None
    }

    /// Update statistics
    fn update_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut DataLoaderStats),
    {
        let mut stats = self.stats.lock().unwrap();
        f(&mut stats);
    }

    /// Get statistics
    pub fn stats(&self) -> DataLoaderStats {
        self.stats.lock().unwrap().clone()
    }

    /// Get configuration
    pub fn config(&self) -> &DataLoaderConfig {
        &self.config
    }

    /// Clean expired cache entries
    pub fn clean_expired(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.retain(|_, entry| !entry.is_expired());
    }
}

impl<K, V> Clone for DataLoader<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            cache: std::sync::Mutex::new(self.cache.lock().unwrap().clone()),
            pending: std::sync::Mutex::new(self.pending.lock().unwrap().clone()),
            stats: std::sync::Mutex::new(self.stats.lock().unwrap().clone()),
        }
    }
}

/// DataLoader factory for creating typed loaders
#[derive(Debug)]
pub struct DataLoaderFactory {
    /// Default configuration
    default_config: DataLoaderConfig,
}

impl DataLoaderFactory {
    /// Create a new factory
    pub fn new(config: DataLoaderConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Create a DataLoader with default config
    pub fn create<K, V>(&self) -> DataLoader<K, V>
    where
        K: Eq + Hash + Clone + Send + Sync,
        V: Clone + Send + Sync,
    {
        DataLoader::new(self.default_config.clone())
    }

    /// Create a DataLoader with custom config
    pub fn create_with_config<K, V>(&self, config: DataLoaderConfig) -> DataLoader<K, V>
    where
        K: Eq + Hash + Clone + Send + Sync,
        V: Clone + Send + Sync,
    {
        DataLoader::new(config)
    }
}

impl Default for DataLoaderFactory {
    fn default() -> Self {
        Self::new(DataLoaderConfig::default())
    }
}

/// Type alias for ID-based loaders
pub type IdLoader<V> = DataLoader<String, V>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataloader_config() {
        let config = DataLoaderConfig::new()
            .batch_window(Duration::from_millis(20))
            .max_batch_size(50)
            .cache(true)
            .cache_ttl(Duration::from_secs(120));

        assert_eq!(config.batch_window, Duration::from_millis(20));
        assert_eq!(config.max_batch_size, 50);
        assert!(config.cache_enabled);
        assert_eq!(config.cache_ttl, Duration::from_secs(120));
    }

    #[test]
    fn test_dataloader_prime_and_load() {
        let loader: DataLoader<String, String> = DataLoader::new(DataLoaderConfig::default());

        loader.prime("key1".to_string(), "value1".to_string());

        let result = loader.load("key1".to_string());
        assert_eq!(result, Some("value1".to_string()));

        let stats = loader.stats();
        assert_eq!(stats.cache_hits, 1);
    }

    #[test]
    fn test_dataloader_batch_execution() {
        let loader: DataLoader<String, String> = DataLoader::new(DataLoaderConfig::default());

        // Add pending keys
        loader.load("key1".to_string());
        loader.load("key2".to_string());
        loader.load("key3".to_string());

        // Execute batch
        let result = loader.execute_batch(|keys| {
            keys.into_iter()
                .map(|k| (k.clone(), format!("value_{}", k)))
                .collect()
        });

        assert_eq!(result.results.len(), 3);
        assert_eq!(
            result.get(&"key1".to_string()),
            Some(&"value_key1".to_string())
        );

        let stats = loader.stats();
        assert_eq!(stats.batch_loads, 1);
    }

    #[test]
    fn test_dataloader_deduplication() {
        let loader: DataLoader<String, i32> =
            DataLoader::new(DataLoaderConfig::default().max_batch_size(100));

        // Add duplicate keys
        loader.load("key1".to_string());
        loader.load("key1".to_string());
        loader.load("key2".to_string());
        loader.load("key1".to_string());

        let mut batch_keys_count = 0;
        let result = loader.execute_batch(|keys| {
            batch_keys_count = keys.len();
            keys.into_iter().map(|k| (k, 1)).collect()
        });

        // Should only have 2 unique keys
        assert_eq!(batch_keys_count, 2);
        assert_eq!(result.results.len(), 2);
    }

    #[test]
    fn test_dataloader_batch_splitting() {
        let loader: DataLoader<i32, i32> =
            DataLoader::new(DataLoaderConfig::default().max_batch_size(2));

        // Add 5 keys
        for i in 0..5 {
            loader.load(i);
        }

        let result = loader.execute_batch(|keys| keys.into_iter().map(|k| (k, k * 10)).collect());

        assert_eq!(result.results.len(), 5);

        let stats = loader.stats();
        assert_eq!(stats.batch_loads, 3); // 5 keys / 2 per batch = 3 batches
    }

    #[test]
    fn test_dataloader_clear() {
        let loader: DataLoader<String, String> = DataLoader::new(DataLoaderConfig::default());

        loader.prime("key1".to_string(), "value1".to_string());
        loader.prime("key2".to_string(), "value2".to_string());

        assert!(loader.load("key1".to_string()).is_some());

        loader.clear();

        // After clear, should be cache miss
        assert!(loader.load("key1".to_string()).is_none());
    }

    #[test]
    fn test_dataloader_clear_key() {
        let loader: DataLoader<String, String> = DataLoader::new(DataLoaderConfig::default());

        loader.prime("key1".to_string(), "value1".to_string());
        loader.prime("key2".to_string(), "value2".to_string());

        loader.clear_key(&"key1".to_string());

        assert!(loader.load("key1".to_string()).is_none());
        assert!(loader.load("key2".to_string()).is_some());
    }

    #[test]
    fn test_dataloader_stats() {
        let loader: DataLoader<String, String> = DataLoader::new(DataLoaderConfig::default());

        loader.prime("cached".to_string(), "value".to_string());

        // Cache hit
        loader.load("cached".to_string());
        // Cache miss
        loader.load("not_cached".to_string());

        let stats = loader.stats();
        assert_eq!(stats.total_loads, 2);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_dataloader_cache_disabled() {
        let loader: DataLoader<String, String> =
            DataLoader::new(DataLoaderConfig::default().cache(false));

        loader.prime("key1".to_string(), "value1".to_string());

        // With cache disabled, prime doesn't work
        let result = loader.load("key1".to_string());
        assert!(result.is_none());
    }

    #[test]
    fn test_batch_result() {
        let mut results = HashMap::new();
        results.insert("a".to_string(), 1);
        results.insert("b".to_string(), 2);

        let batch = BatchResult::new(results).with_missing(vec!["c".to_string()]);

        assert_eq!(batch.get(&"a".to_string()), Some(&1));
        assert_eq!(batch.get(&"c".to_string()), None);
        assert!(batch.is_missing(&"c".to_string()));
        assert!(!batch.is_missing(&"a".to_string()));
    }

    #[test]
    fn test_dataloader_factory() {
        let factory = DataLoaderFactory::new(DataLoaderConfig::default().max_batch_size(50));

        let loader: DataLoader<String, i32> = factory.create();
        assert_eq!(loader.config().max_batch_size, 50);

        let custom_loader: DataLoader<String, i32> =
            factory.create_with_config(DataLoaderConfig::default().max_batch_size(100));
        assert_eq!(custom_loader.config().max_batch_size, 100);
    }

    #[test]
    fn test_dataloader_load_many() {
        let loader: DataLoader<String, String> = DataLoader::new(DataLoaderConfig::default());

        loader.prime("key1".to_string(), "value1".to_string());

        let results = loader.load_many(vec!["key1".to_string(), "key2".to_string()]);

        assert_eq!(results.get("key1"), Some(&Some("value1".to_string())));
        assert_eq!(results.get("key2"), Some(&None));
    }
}
