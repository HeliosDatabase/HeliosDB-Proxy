//! L2 Warm Cache - compressed in-memory cache
//!
//! Entries are serialized, compressed and held in process memory. Nothing is
//! written to disk: the tier trades CPU (compression) for a larger capacity
//! than the uncompressed L1, and its contents do not survive a restart.
//!
//! Features:
//! - Compressed storage using LZ4 or Zstd
//! - Bloom filter for fast negative lookups
//! - TTL-based expiration
//! - Size-bounded, oldest-entry-first eviction

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use dashmap::DashMap;

use super::{CacheEntry, CompressionType, TierStats};
use crate::distribcache::{CacheError, CacheResult, QueryFingerprint};

/// Bloom filter for fast negative lookups
struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: usize,
}

impl BloomFilter {
    fn new(capacity: usize) -> Self {
        // Calculate optimal size and hash count
        let bits_per_item = 10; // ~1% false positive rate
        let num_bits = capacity * bits_per_item;
        let num_words = num_bits.div_ceil(64);

        Self {
            bits: vec![0; num_words],
            num_hashes: 7, // Optimal for ~1% FPR
        }
    }

    fn insert(&mut self, data: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.hash(data, i);
            let idx = hash as usize % (self.bits.len() * 64);
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1 << bit;
        }
    }

    fn may_contain(&self, data: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(data, i);
            let idx = hash as usize % (self.bits.len() * 64);
            let word = idx / 64;
            let bit = idx % 64;
            if (self.bits[word] & (1 << bit)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash(&self, data: &[u8], seed: usize) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        data.hash(&mut hasher);
        hasher.finish()
    }

    fn clear(&mut self) {
        self.bits.fill(0);
    }
}

/// L2 Warm Cache - compressed, in process memory
pub struct WarmCache {
    /// Index (key -> metadata)
    index: DashMap<u64, EntryMetadata>,

    /// Compressed, serialized entries, held in process memory
    data: DashMap<u64, Vec<u8>>,

    /// Bloom filter for fast negative lookups
    bloom: RwLock<BloomFilter>,

    /// Table to key index for invalidation
    table_index: DashMap<String, HashSet<u64>>,

    /// Compression type
    compression: CompressionType,

    /// Current size in bytes
    current_size: AtomicU64,

    /// Maximum size in bytes
    max_size: u64,

    /// Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    compressed_size: AtomicU64,
    uncompressed_size: AtomicU64,
}

/// Entry metadata stored in index
#[derive(Debug, Clone)]
struct EntryMetadata {
    /// Size of compressed data
    compressed_size: usize,
    /// Creation timestamp
    created_at: u64,
    /// TTL in seconds
    ttl_secs: u64,
    /// Tables for invalidation
    tables: Vec<String>,
}

impl WarmCache {
    /// Create a new warm cache holding at most `max_size` compressed bytes
    /// in memory.
    pub fn new(max_size: u64, compression: CompressionType) -> Self {
        Self {
            index: DashMap::new(),
            data: DashMap::new(),
            bloom: RwLock::new(BloomFilter::new(100_000)),
            table_index: DashMap::new(),
            compression,
            current_size: AtomicU64::new(0),
            max_size,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            compressed_size: AtomicU64::new(0),
            uncompressed_size: AtomicU64::new(0),
        }
    }

    /// Get an entry from the cache
    pub fn get(&self, fingerprint: &QueryFingerprint) -> Option<CacheEntry> {
        let key = self.fingerprint_to_hash(fingerprint);
        let key_bytes = key.to_le_bytes();

        // Fast path: bloom filter check
        {
            let bloom = self.bloom.read().ok()?;
            if !bloom.may_contain(&key_bytes) {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }

        // Check index (a bloom-filter false positive lands here)
        let Some(metadata) = self.index.get(&key) else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };

        // Check TTL
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if now > metadata.created_at + metadata.ttl_secs {
            drop(metadata);
            self.remove_entry(key);
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        drop(metadata);

        // Decompress and deserialize. Stored bytes that do not decode are
        // dropped and reported as a miss, never returned.
        let decoded = self.data.get(&key).and_then(|compressed| {
            self.decompress(&compressed)
                .and_then(|bytes| postcard::from_bytes::<CacheEntry>(&bytes).ok())
        });
        match decoded {
            Some(entry) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry)
            }
            None => {
                tracing::warn!(key, "L2 entry could not be decoded; dropping it");
                self.remove_entry(key);
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert an entry into the cache.
    ///
    /// Fails with [`CacheError::Serialization`] / [`CacheError::Compression`]
    /// when the entry cannot be encoded, and [`CacheError::TooLarge`] when its
    /// compressed size exceeds the tier's capacity; nothing is stored then.
    pub fn insert(&self, fingerprint: QueryFingerprint, entry: CacheEntry) -> CacheResult<()> {
        let key = self.fingerprint_to_hash(&fingerprint);

        // Serialize
        let serialized =
            postcard::to_stdvec(&entry).map_err(|e| CacheError::Serialization(e.to_string()))?;

        let uncompressed_size = serialized.len();

        // Compress
        let compressed = self.compress(&serialized).ok_or_else(|| {
            CacheError::Compression(format!("{:?} compression failed", self.compression))
        })?;

        let compressed_size = compressed.len();
        if compressed_size as u64 > self.max_size {
            return Err(CacheError::TooLarge(
                compressed_size,
                self.max_size.min(usize::MAX as u64) as usize,
            ));
        }

        // Evict if needed
        while self.current_size.load(Ordering::Relaxed) + compressed_size as u64 > self.max_size {
            if !self.evict_oldest() {
                break;
            }
        }

        // Remove old entry if exists
        self.remove_entry(key);

        // Create metadata
        let metadata = EntryMetadata {
            compressed_size,
            created_at: entry.created_at,
            ttl_secs: entry.ttl_secs,
            tables: entry.tables.clone(),
        };

        // Index by tables
        for table in &entry.tables {
            self.table_index
                .entry(table.clone())
                .or_default()
                .insert(key);
        }

        // Insert into bloom filter
        {
            if let Ok(mut bloom) = self.bloom.write() {
                bloom.insert(&key.to_le_bytes());
            }
        }

        // Store
        self.index.insert(key, metadata);
        self.data.insert(key, compressed);
        self.current_size
            .fetch_add(compressed_size as u64, Ordering::Relaxed);
        self.compressed_size
            .fetch_add(compressed_size as u64, Ordering::Relaxed);
        self.uncompressed_size
            .fetch_add(uncompressed_size as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Invalidate entries for a table
    pub fn invalidate_by_table(&self, table: &str) {
        if let Some((_, keys)) = self.table_index.remove(table) {
            for key in keys {
                self.remove_entry(key);
            }
        }
    }

    /// Invalidate a specific entry
    pub fn invalidate(&self, fingerprint: &QueryFingerprint) {
        let key = self.fingerprint_to_hash(fingerprint);
        self.remove_entry(key);
    }

    /// Remove an entry
    fn remove_entry(&self, key: u64) {
        if let Some((_, metadata)) = self.index.remove(&key) {
            self.data.remove(&key);
            self.current_size
                .fetch_sub(metadata.compressed_size as u64, Ordering::Relaxed);

            // Clean up table index
            for table in &metadata.tables {
                if let Some(mut keys) = self.table_index.get_mut(table) {
                    keys.remove(&key);
                }
            }
        }
    }

    /// Evict oldest entry
    fn evict_oldest(&self) -> bool {
        let mut oldest_key = None;
        let mut oldest_time = u64::MAX;

        for entry in self.index.iter() {
            if entry.created_at < oldest_time {
                oldest_time = entry.created_at;
                oldest_key = Some(*entry.key());
            }
        }

        if let Some(key) = oldest_key {
            self.remove_entry(key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Compress data
    fn compress(&self, data: &[u8]) -> Option<Vec<u8>> {
        match self.compression {
            CompressionType::None => {
                let mut output = Vec::with_capacity(data.len() + 1);
                output.push(0x00); // No compression marker
                output.extend_from_slice(data);
                Some(output)
            }
            CompressionType::Lz4 => {
                // Real LZ4 block compression. compress_prepend_size writes the
                // uncompressed length as a little-endian u32 prefix so the
                // decoder can size its output buffer exactly.
                let compressed = lz4_flex::block::compress_prepend_size(data);
                let mut output = Vec::with_capacity(compressed.len() + 1);
                output.push(0x01); // LZ4 marker
                output.extend_from_slice(&compressed);
                Some(output)
            }
            CompressionType::Zstd => {
                // Real zstd compression
                let compressed = zstd::stream::encode_all(data, 3).ok()?;
                let mut output = Vec::with_capacity(compressed.len() + 1);
                output.push(0x02); // Zstd marker
                output.extend_from_slice(&compressed);
                Some(output)
            }
        }
    }

    /// Decompress data
    fn decompress(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.is_empty() {
            return None;
        }

        let marker = data[0];
        let payload = &data[1..];

        match marker {
            0x00 => Some(payload.to_vec()), // Uncompressed
            0x01 => {
                // Real LZ4 block decompression — reads the u32 size prefix
                // written by compress_prepend_size.
                lz4_flex::block::decompress_size_prepended(payload).ok()
            }
            0x02 => {
                // Real zstd decompression
                zstd::stream::decode_all(payload).ok()
            }
            _ => None, // Unknown marker: not data this tier wrote
        }
    }

    /// Convert fingerprint to hash key
    fn fingerprint_to_hash(&self, fingerprint: &QueryFingerprint) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        fingerprint.template.hash(&mut hasher);
        if let Some(param) = fingerprint.param_hash {
            param.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Get cache statistics
    pub fn stats(&self) -> TierStats {
        let compressed = self.compressed_size.load(Ordering::Relaxed);
        let uncompressed = self.uncompressed_size.load(Ordering::Relaxed);

        TierStats {
            size_bytes: self.current_size.load(Ordering::Relaxed),
            max_size_bytes: self.max_size,
            entry_count: self.index.len() as u64,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            compression_ratio: if compressed > 0 {
                Some(uncompressed as f64 / compressed as f64)
            } else {
                None
            },
            peer_count: None,
            healthy_peers: None,
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.index.clear();
        self.data.clear();
        self.table_index.clear();
        if let Ok(mut bloom) = self.bloom.write() {
            bloom.clear();
        }
        self.current_size.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_warm_cache_insert_get() {
        let cache = WarmCache::new(1024 * 1024 * 1024, CompressionType::Lz4);

        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        let entry = CacheEntry::new(vec![1, 2, 3], vec!["users".to_string()], 1)
            .with_ttl(Duration::from_secs(300));

        cache.insert(fp.clone(), entry).unwrap();

        let result = cache.get(&fp);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 2, 3]);
    }

    // Proves LZ4 is real now: it round-trips AND a compressible payload comes
    // out strictly smaller than it went in. The previous impl just copied the
    // bytes behind a marker, so this shrink assertion would have failed.
    #[test]
    fn test_lz4_compression_is_real() {
        let cache = WarmCache::new(1024 * 1024, CompressionType::Lz4);

        // Highly compressible: 4 KiB of a repeating pattern.
        let original = b"helios-distribcache-".repeat(200);
        let compressed = cache.compress(&original).expect("compress");
        // marker byte + LZ4 block; must be meaningfully smaller than the input.
        assert!(
            compressed.len() < original.len(),
            "LZ4 did not shrink data: {} -> {}",
            original.len(),
            compressed.len()
        );

        let restored = cache.decompress(&compressed).expect("decompress");
        assert_eq!(restored, original, "LZ4 round-trip mismatch");

        // Sanity: Zstd path round-trips too.
        let zcache = WarmCache::new(1024 * 1024, CompressionType::Zstd);
        let zc = zcache.compress(&original).expect("zstd compress");
        assert!(zc.len() < original.len());
        assert_eq!(zcache.decompress(&zc).expect("zstd decompress"), original);
    }

    #[test]
    fn test_warm_cache_bloom_filter() {
        let cache = WarmCache::new(1024 * 1024, CompressionType::None);

        let fp1 = QueryFingerprint::from_query("SELECT * FROM users");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");

        cache
            .insert(
                fp1.clone(),
                CacheEntry::new(vec![1], vec![], 1).with_ttl(Duration::from_secs(300)),
            )
            .unwrap();

        // fp1 should hit bloom filter
        assert!(cache.get(&fp1).is_some());

        // fp2 should miss bloom filter (fast path)
        assert!(cache.get(&fp2).is_none());
    }

    #[test]
    fn test_warm_cache_invalidate_by_table() {
        let cache = WarmCache::new(1024 * 1024, CompressionType::None);

        let fp1 = QueryFingerprint::from_query("SELECT * FROM users");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");

        cache
            .insert(
                fp1.clone(),
                CacheEntry::new(vec![1], vec!["users".to_string()], 1)
                    .with_ttl(Duration::from_secs(300)),
            )
            .unwrap();
        cache
            .insert(
                fp2.clone(),
                CacheEntry::new(vec![2], vec!["orders".to_string()], 1)
                    .with_ttl(Duration::from_secs(300)),
            )
            .unwrap();

        cache.invalidate_by_table("users");

        assert!(cache.get(&fp1).is_none());
        assert!(cache.get(&fp2).is_some());
    }

    #[test]
    fn test_warm_cache_stats() {
        let cache = WarmCache::new(1024 * 1024, CompressionType::Lz4);

        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        cache
            .insert(
                fp.clone(),
                CacheEntry::new(vec![1], vec![], 1).with_ttl(Duration::from_secs(300)),
            )
            .unwrap();

        cache.get(&fp); // Hit
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");
        cache.get(&fp2); // Miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert!(stats.compression_ratio.is_some());
    }

    /// Eviction is counted in the tier stats (it used to report 0 always).
    #[test]
    fn test_warm_cache_counts_evictions() {
        // Room for roughly one compressed entry only.
        let cache = WarmCache::new(40, CompressionType::None);
        for i in 0..3 {
            let fp = QueryFingerprint::from_query(&format!("SELECT * FROM t{}", i));
            cache
                .insert(
                    fp,
                    CacheEntry::new(vec![i; 20], vec![], 1).with_ttl(Duration::from_secs(300)),
                )
                .unwrap();
        }
        let stats = cache.stats();
        assert!(stats.evictions >= 2, "evictions: {}", stats.evictions);
        assert!(stats.size_bytes <= 40);
    }

    /// An entry larger than the tier's capacity is rejected, not stored past
    /// the limit.
    #[test]
    fn test_warm_cache_rejects_entry_larger_than_capacity() {
        let cache = WarmCache::new(16, CompressionType::None);
        let fp = QueryFingerprint::from_query("SELECT * FROM big");
        let err = cache
            .insert(fp.clone(), CacheEntry::new(vec![7; 100], vec![], 1))
            .unwrap_err();
        assert!(matches!(err, CacheError::TooLarge(_, 16)), "got {err:?}");
        assert_eq!(cache.stats().entry_count, 0);
        assert_eq!(cache.stats().size_bytes, 0);
    }

    /// Bytes with an unknown compression marker are not returned as data.
    #[test]
    fn test_unknown_compression_marker_is_rejected() {
        let cache = WarmCache::new(1024, CompressionType::None);
        assert!(cache.decompress(&[0x7f, 1, 2, 3]).is_none());
    }
}
