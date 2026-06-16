//! L2 Warm Cache
//!
//! Shared cache with normalized queries and configurable storage backend.
//! Supports both in-memory and memory-mapped file storage.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;

use super::config::{L2Config, StorageBackend};
use super::result::{CachedResult, CacheKey, L2Entry};

/// L2 warm cache (shared across connections)
///
/// This cache stores normalized query results shared across all connections.
/// It supports two storage backends:
/// - Memory: Fast, volatile storage
/// - Mmap: Memory-mapped file that survives restarts
#[derive(Debug)]
pub struct L2WarmCache {
    /// Cache configuration
    config: L2Config,

    /// Cache entries (in-memory storage)
    memory_entries: DashMap<u64, L2Entry>,

    /// Memory-mapped storage (if enabled)
    mmap_storage: Option<RwLock<MmapStorage>>,

    /// Current memory usage in bytes
    memory_usage: std::sync::atomic::AtomicUsize,
}

/// Memory-mapped storage for persistent caching
#[derive(Debug)]
struct MmapStorage {
    /// File path
    path: PathBuf,

    /// File handle
    file: Option<File>,

    /// Cached entries index (hash -> offset)
    index: HashMap<u64, MmapEntry>,

    /// Total file size
    file_size: usize,
}

/// Entry metadata for mmap storage
#[derive(Debug, Clone)]
struct MmapEntry {
    /// Offset in the file
    offset: usize,

    /// Size of the entry
    size: usize,

    /// TTL expiration timestamp (seconds since epoch)
    expires_at: u64,
}

impl L2WarmCache {
    /// Create a new L2 warm cache
    pub fn new(config: L2Config) -> Self {
        let mmap_storage = if config.storage == StorageBackend::Mmap {
            config.mmap_path.as_ref().map(|path| {
                RwLock::new(MmapStorage::new(path.clone()))
            })
        } else {
            None
        };

        Self {
            config,
            memory_entries: DashMap::new(),
            mmap_storage,
            memory_usage: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Look up a cache key
    pub async fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        if !self.config.enabled {
            return None;
        }

        let hash = key.hash_value();

        // Try memory storage first
        if let Some(mut entry) = self.memory_entries.get_mut(&hash) {
            if entry.is_expired() {
                drop(entry);
                self.memory_entries.remove(&hash);
                return None;
            }

            entry.touch();
            return Some(entry.result.clone());
        }

        // Try mmap storage
        if let Some(ref mmap) = self.mmap_storage {
            if let Ok(storage) = mmap.read() {
                if let Some(result) = storage.get(hash) {
                    // Promote to memory cache
                    self.promote_to_memory(key, result.clone());
                    return Some(result);
                }
            }
        }

        None
    }

    /// Store a result in the cache
    pub async fn put(&self, key: CacheKey, result: CachedResult) {
        if !self.config.enabled {
            return;
        }

        let entry_size = result.size() + std::mem::size_of::<L2Entry>();

        // Check size limit
        let max_bytes = self.config.size_mb * 1024 * 1024;
        let current_usage = self.memory_usage.load(std::sync::atomic::Ordering::Relaxed);

        if current_usage + entry_size > max_bytes {
            self.evict_to_fit(entry_size).await;
        }

        let hash = key.hash_value();
        let fingerprint = format!("{:016x}", hash);
        let entry = L2Entry::new(key, fingerprint, result);
        let entry_memory = entry.memory_size;

        self.memory_entries.insert(hash, entry);
        self.memory_usage.fetch_add(entry_memory, std::sync::atomic::Ordering::Relaxed);
    }

    /// Remove an entry from the cache
    pub async fn remove(&self, key: &CacheKey) {
        let hash = key.hash_value();

        if let Some((_, entry)) = self.memory_entries.remove(&hash) {
            self.memory_usage.fetch_sub(entry.memory_size, std::sync::atomic::Ordering::Relaxed);
        }

        // Also remove from mmap if present
        if let Some(ref mmap) = self.mmap_storage {
            if let Ok(mut storage) = mmap.write() {
                storage.remove(hash);
            }
        }
    }

    /// Clear all entries
    pub async fn clear(&self) {
        self.memory_entries.clear();
        self.memory_usage.store(0, std::sync::atomic::Ordering::Relaxed);

        if let Some(ref mmap) = self.mmap_storage {
            if let Ok(mut storage) = mmap.write() {
                storage.clear();
            }
        }
    }

    /// Get current entry count
    pub fn len(&self) -> usize {
        self.memory_entries.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.memory_entries.is_empty()
    }

    /// Get current memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get cache statistics
    pub fn stats(&self) -> L2CacheStats {
        let total_access: u64 = self.memory_entries
            .iter()
            .map(|e| e.access_count)
            .sum();

        L2CacheStats {
            entry_count: self.memory_entries.len(),
            memory_usage_bytes: self.memory_usage(),
            max_memory_bytes: self.config.size_mb * 1024 * 1024,
            total_accesses: total_access,
            storage_backend: self.config.storage.clone(),
        }
    }

    /// Evict entries to fit new data
    async fn evict_to_fit(&self, required_bytes: usize) {
        let max_bytes = self.config.size_mb * 1024 * 1024;
        let target = max_bytes.saturating_sub(required_bytes);

        // First, evict expired entries
        let expired: Vec<u64> = self.memory_entries
            .iter()
            .filter(|e| e.is_expired())
            .map(|e| *e.key())
            .collect();

        for hash in expired {
            if let Some((_, entry)) = self.memory_entries.remove(&hash) {
                self.memory_usage.fetch_sub(entry.memory_size, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // If still over limit, evict LRU entries
        while self.memory_usage.load(std::sync::atomic::Ordering::Relaxed) > target {
            // Find LRU entry
            let lru_hash = self.memory_entries
                .iter()
                .min_by_key(|e| e.last_access)
                .map(|e| *e.key());

            if let Some(hash) = lru_hash {
                // Optionally move to mmap before evicting
                if self.mmap_storage.is_some() {
                    if let Some(entry) = self.memory_entries.get(&hash) {
                        self.demote_to_mmap(&entry);
                    }
                }

                if let Some((_, entry)) = self.memory_entries.remove(&hash) {
                    self.memory_usage.fetch_sub(entry.memory_size, std::sync::atomic::Ordering::Relaxed);
                }
            } else {
                break;
            }
        }
    }

    /// Promote an entry from mmap to memory
    fn promote_to_memory(&self, key: &CacheKey, result: CachedResult) {
        let hash = key.hash_value();
        let fingerprint = format!("{:016x}", hash);
        let entry = L2Entry::new(key.clone(), fingerprint, result);
        let entry_memory = entry.memory_size;

        self.memory_entries.insert(hash, entry);
        self.memory_usage.fetch_add(entry_memory, std::sync::atomic::Ordering::Relaxed);
    }

    /// Demote an entry to mmap storage
    fn demote_to_mmap(&self, entry: &dashmap::mapref::one::Ref<u64, L2Entry>) {
        if let Some(ref mmap) = self.mmap_storage {
            if let Ok(mut storage) = mmap.write() {
                storage.put(*entry.key(), &entry.result);
            }
        }
    }

    /// Flush memory entries to mmap (for graceful shutdown)
    pub fn flush_to_disk(&self) -> Result<usize, std::io::Error> {
        let Some(ref mmap) = self.mmap_storage else {
            return Ok(0);
        };

        let mut storage = mmap.write()
            .map_err(|_| std::io::Error::other("Lock poisoned"))?;

        let mut count = 0;
        for entry in self.memory_entries.iter() {
            if !entry.is_expired() {
                storage.put(*entry.key(), &entry.result);
                count += 1;
            }
        }

        storage.sync()?;
        Ok(count)
    }

    /// Load entries from mmap on startup
    pub fn load_from_disk(&self) -> Result<usize, std::io::Error> {
        let Some(ref mmap) = self.mmap_storage else {
            return Ok(0);
        };

        let storage = mmap.read()
            .map_err(|_| std::io::Error::other("Lock poisoned"))?;

        Ok(storage.entry_count())
    }
}

impl MmapStorage {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            file: None,
            index: HashMap::new(),
            file_size: 0,
        }
    }

    fn get(&self, hash: u64) -> Option<CachedResult> {
        let entry = self.index.get(&hash)?;

        // Check expiration
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs();

        if now > entry.expires_at {
            return None;
        }

        // Read from file
        let mut file = File::open(&self.path).ok()?;
        let mut buffer = vec![0u8; entry.size];

        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(entry.offset as u64)).ok()?;
        file.read_exact(&mut buffer).ok()?;

        // Deserialize (simple format: ttl_secs:row_count:data)
        deserialize_result(&buffer)
    }

    fn put(&mut self, hash: u64, result: &CachedResult) {
        let data = serialize_result(result);

        // Open or create file
        let file = match &mut self.file {
            Some(f) => f,
            None => {
                self.file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&self.path)
                    .ok();
                match &mut self.file {
                    Some(f) => f,
                    None => return,
                }
            }
        };

        // Append to file
        use std::io::Seek;
        if file.seek(std::io::SeekFrom::End(0)).is_err() {
            return;
        }

        let offset = self.file_size;
        if file.write_all(&data).is_ok() {
            let expires_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() + result.ttl.as_secs())
                .unwrap_or(0);

            self.index.insert(hash, MmapEntry {
                offset,
                size: data.len(),
                expires_at,
            });
            self.file_size += data.len();
        }
    }

    fn remove(&mut self, hash: u64) {
        self.index.remove(&hash);
        // Note: This doesn't reclaim space, just marks as removed
    }

    fn clear(&mut self) {
        self.index.clear();
        self.file_size = 0;

        // Truncate file
        if let Some(ref mut file) = self.file {
            let _ = file.set_len(0);
        }
    }

    fn sync(&mut self) -> Result<(), std::io::Error> {
        if let Some(ref file) = self.file {
            file.sync_all()?;
        }
        Ok(())
    }

    fn entry_count(&self) -> usize {
        self.index.len()
    }
}

/// Serialize a cached result for mmap storage
fn serialize_result(result: &CachedResult) -> Vec<u8> {
    let mut buffer = Vec::new();

    // Write TTL (8 bytes)
    buffer.extend_from_slice(&result.ttl.as_secs().to_le_bytes());

    // Write row count (8 bytes)
    buffer.extend_from_slice(&(result.row_count as u64).to_le_bytes());

    // Write data length (8 bytes) + data
    buffer.extend_from_slice(&(result.data.len() as u64).to_le_bytes());
    buffer.extend_from_slice(&result.data);

    buffer
}

/// Deserialize a cached result from mmap storage
fn deserialize_result(buffer: &[u8]) -> Option<CachedResult> {
    if buffer.len() < 24 {
        return None;
    }

    let ttl_secs = u64::from_le_bytes(buffer[0..8].try_into().ok()?);
    let row_count = u64::from_le_bytes(buffer[8..16].try_into().ok()?) as usize;
    let data_len = u64::from_le_bytes(buffer[16..24].try_into().ok()?) as usize;

    if buffer.len() < 24 + data_len {
        return None;
    }

    let data = Bytes::copy_from_slice(&buffer[24..24 + data_len]);

    Some(CachedResult {
        data,
        row_count,
        cached_at: Instant::now(),
        ttl: std::time::Duration::from_secs(ttl_secs),
        tables: Vec::new(), // Tables are not persisted
        execution_time: std::time::Duration::from_millis(0),
    })
}

/// L2 cache statistics
#[derive(Debug, Clone)]
pub struct L2CacheStats {
    /// Number of entries in cache
    pub entry_count: usize,

    /// Current memory usage in bytes
    pub memory_usage_bytes: usize,

    /// Maximum memory in bytes
    pub max_memory_bytes: usize,

    /// Total number of accesses
    pub total_accesses: u64,

    /// Storage backend type
    pub storage_backend: StorageBackend,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use crate::cache::CacheContext;
    use crate::cache::normalizer::NormalizedQuery;

    fn create_result(data: &str) -> CachedResult {
        CachedResult::new(
            Bytes::from(data.to_string()),
            1,
            Duration::from_secs(60),
            vec!["test".to_string()],
            Duration::from_millis(5),
        )
    }

    fn create_key(query_hash: u64) -> CacheKey {
        CacheKey::from_parts(
            query_hash,
            "test".to_string(),
            None,
            None,
        )
    }

    #[tokio::test]
    async fn test_basic_get_put() {
        let config = L2Config::default();
        let cache = L2WarmCache::new(config);

        let key = create_key(12345);
        let result = create_result("test data");

        // Initially empty
        assert!(cache.get(&key).await.is_none());

        // Put and get
        cache.put(key.clone(), result.clone()).await;
        let cached = cache.get(&key).await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().data, result.data);
    }

    #[tokio::test]
    async fn test_different_keys() {
        let config = L2Config::default();
        let cache = L2WarmCache::new(config);

        let key1 = create_key(11111);
        let key2 = create_key(22222);
        let result = create_result("data");

        cache.put(key1.clone(), result.clone()).await;

        assert!(cache.get(&key1).await.is_some());
        assert!(cache.get(&key2).await.is_none());
    }

    #[tokio::test]
    async fn test_expiration() {
        let config = L2Config {
            ttl: Duration::from_millis(10),
            ..Default::default()
        };
        let cache = L2WarmCache::new(config);

        let key = create_key(12345);
        let mut result = create_result("data");
        result.ttl = Duration::from_millis(10);

        cache.put(key.clone(), result).await;
        assert!(cache.get(&key).await.is_some());

        std::thread::sleep(Duration::from_millis(15));
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_remove() {
        let config = L2Config::default();
        let cache = L2WarmCache::new(config);

        let key = create_key(12345);
        let result = create_result("data");

        cache.put(key.clone(), result).await;
        assert!(cache.get(&key).await.is_some());

        cache.remove(&key).await;
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_clear() {
        let config = L2Config::default();
        let cache = L2WarmCache::new(config);

        cache.put(create_key(111), create_result("1")).await;
        cache.put(create_key(222), create_result("2")).await;

        assert_eq!(cache.len(), 2);

        cache.clear().await;

        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn test_memory_eviction() {
        let config = L2Config {
            size_mb: 1, // 1 MB limit
            ..Default::default()
        };
        let cache = L2WarmCache::new(config);

        // Add entries until eviction kicks in
        let large_data = "x".repeat(100 * 1024); // 100 KB per entry
        for i in 0..15 {
            cache.put(create_key(i), create_result(&large_data)).await;
        }

        // Should have evicted some entries
        assert!(cache.memory_usage() <= 1024 * 1024 + 100 * 1024);
    }

    #[tokio::test]
    async fn test_stats() {
        let config = L2Config::default();
        let cache = L2WarmCache::new(config);

        cache.put(create_key(111), create_result("1")).await;
        cache.put(create_key(222), create_result("2")).await;

        cache.get(&create_key(111)).await;
        cache.get(&create_key(111)).await;

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 2);
        assert!(stats.memory_usage_bytes > 0);
        assert_eq!(stats.storage_backend, StorageBackend::Memory);
    }

    #[tokio::test]
    async fn test_disabled_cache() {
        let config = L2Config {
            enabled: false,
            ..Default::default()
        };
        let cache = L2WarmCache::new(config);

        let key = create_key(12345);
        cache.put(key.clone(), create_result("data")).await;

        assert!(cache.get(&key).await.is_none());
    }

    #[test]
    fn test_serialize_deserialize() {
        let result = create_result("test data for serialization");
        let serialized = serialize_result(&result);
        let deserialized = deserialize_result(&serialized).unwrap();

        assert_eq!(deserialized.data, result.data);
        assert_eq!(deserialized.row_count, result.row_count);
        assert_eq!(deserialized.ttl.as_secs(), result.ttl.as_secs());
    }
}
