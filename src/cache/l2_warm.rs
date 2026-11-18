//! L2 Warm Cache
//!
//! Shared cache with normalized queries and configurable storage backend.
//! Supports both in-memory and memory-mapped file storage. Payloads of at
//! least `compression_min_bytes` may be stored compressed
//! (`[cache] l2_compression`, [`super::codec`]); a hit decompresses them.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;

use super::codec::{self, L2Codec, L2Payload};
use super::config::{L2Config, StorageBackend};
use super::result::{CacheKey, CachedResult, L2Entry};
use crate::config::CacheCompression;

/// L2 warm cache (shared across connections)
///
/// This cache stores normalized query results shared across all connections.
/// It supports two storage backends:
/// - Memory: Fast, volatile storage
/// - Mmap: File-backed spill storage with a process-local index. Entries
///   are not recovered after restart.
///
/// With `compression` set, an entry keeps a payload of at least
/// `compression_min_bytes` compressed when that shrinks it, and the byte
/// budget counts the compressed size. [`Self::get`] returns the raw payload;
/// an entry whose blob does not decode is dropped and reads as a miss.
#[derive(Debug)]
pub struct L2WarmCache {
    /// Cache configuration
    config: L2Config,

    /// The payload codec (`compression`, `compression_min_bytes`,
    /// `zstd_level` of `config`).
    codec: L2Codec,

    /// Cache entries (in-memory storage)
    memory_entries: DashMap<u64, L2Entry>,

    /// Memory-mapped storage (if enabled)
    mmap_storage: Option<RwLock<MmapStorage>>,

    /// Current memory usage in bytes
    memory_usage: AtomicUsize,

    /// Bytes compression saves on the entries in memory: their raw minus
    /// their stored payload sizes.
    compression_saved: AtomicUsize,

    /// Set while an eviction pass runs. When L2 fills up, every concurrent
    /// put would otherwise scan and shed the same entries — a herd that
    /// stalled all readers for seconds. A put that finds a pass in progress
    /// inserts without evicting: the size bound is soft for that long.
    evicting: AtomicBool,
}

/// File-backed spill storage. The index, including monotonic entry timing,
/// lives only in this process; the first write creates/truncates the file.
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

    /// Original cache insertion time, preserved through every demotion.
    cached_at: Instant,

    /// Original full-precision TTL; neither a disk read nor promotion renews it.
    ttl: Duration,
}

impl L2WarmCache {
    /// Create a new L2 warm cache
    pub fn new(config: L2Config) -> Self {
        let mmap_storage = if config.storage == StorageBackend::Mmap {
            config
                .mmap_path
                .as_ref()
                .map(|path| RwLock::new(MmapStorage::new(path.clone())))
        } else {
            None
        };

        Self {
            codec: L2Codec {
                compression: config.compression,
                min_bytes: config.compression_min_bytes,
                zstd_level: config.zstd_level,
            },
            config,
            memory_entries: DashMap::new(),
            mmap_storage,
            memory_usage: AtomicUsize::new(0),
            compression_saved: AtomicUsize::new(0),
            evicting: AtomicBool::new(false),
        }
    }

    /// The form this cache stores `data` in: compressed when the codec
    /// applies and shrinks it, else `data` itself. CPU-bound when it
    /// compresses (zstd of a large payload takes milliseconds): see
    /// [`Self::compresses_off_worker`].
    pub fn encode_payload(&self, data: &Bytes) -> L2Payload {
        self.codec.encode(data)
    }

    /// Whether storing a payload of `len` bytes runs zstd, which the query
    /// cache does on the blocking thread pool rather than the async worker
    /// (LZ4 is fast enough to run inline).
    pub fn compresses_off_worker(&self, len: usize) -> bool {
        self.config.enabled
            && self.codec.compression == CacheCompression::Zstd
            && self.codec.applies(len)
    }

    /// Look up a cache key
    pub async fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        self.get_now(key, &mut false)
    }

    /// [`Self::get`], also setting `expired` when the key's in-memory entry
    /// was found past its TTL (and removed). An expired mmap entry reads as
    /// absent.
    pub async fn get_noting_expiry(
        &self,
        key: &CacheKey,
        expired: &mut bool,
    ) -> Option<CachedResult> {
        self.get_now(key, expired)
    }

    /// [`Self::get_noting_expiry`] as a plain call. A lookup never waits (the
    /// memory map is sharded, the spill file is read under a blocking lock),
    /// so the query cache calls this directly: its own lookup then has no
    /// suspension point, and its future no state saved across one, which
    /// every caller would copy.
    pub(crate) fn get_now(&self, key: &CacheKey, expired: &mut bool) -> Option<CachedResult> {
        if !self.config.enabled {
            return None;
        }

        let hash = key.hash_value();

        // Try memory storage first. A raw entry is served under the shard's
        // lock; a compressed one is copied out (a reference count) and
        // decoded after the lock is released.
        if let Some(mut entry) = self.memory_entries.get_mut(&hash) {
            if entry.is_expired() {
                *expired = true;
                drop(entry);
                if let Some((_, old)) = self.memory_entries.remove(&hash) {
                    self.release(&old);
                }
                return None;
            }
            entry.touch();
            let result = entry.result.clone();
            let Some(raw_len) = entry.raw_len else {
                return Some(result);
            };
            drop(entry);
            return self.decoded(hash, result, raw_len);
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

    /// `result` of a compressed entry with its payload decoded. A blob that
    /// does not decode is never served: the entry is dropped (unless a store
    /// replaced it meanwhile) and the read misses.
    fn decoded(&self, hash: u64, mut result: CachedResult, raw_len: usize) -> Option<CachedResult> {
        if let Some(raw) = codec::decode(&result.data, raw_len) {
            result.data = Bytes::from(raw);
            return Some(result);
        }
        let blob = result.data;
        if let Some((_, old)) = self.memory_entries.remove_if(&hash, |_, e| {
            e.result.data.as_ptr() == blob.as_ptr() && e.result.data.len() == blob.len()
        }) {
            self.release(&old);
        }
        tracing::warn!(
            target: "helios::cache",
            stored_bytes = blob.len(),
            raw_bytes = raw_len,
            "an L2 entry did not decompress; dropped it (the read misses)"
        );
        None
    }

    /// Store a result in the cache
    pub async fn put(&self, key: CacheKey, result: CachedResult) {
        self.put_evicting(key, result, |_| false, Err).await;
    }

    /// [`Self::put`] with the caller's notion of a stale entry. When the
    /// cache must make room, one eviction pass at a time runs: it collects
    /// every expired entry and every entry `is_stale` retires (the query
    /// cache passes its write-generation check), so one scan frees all that
    /// writes retired instead of evicting live entries one LRU scan at a
    /// time. `hand_off` may take those hashes to shed them off the caller's
    /// path; it must then call [`Self::shed`] and [`Self::end_eviction`].
    /// If it hands them back (`Err`), they are shed here. With nothing stale
    /// or expired, the pass evicts least-recently-used entries inline.
    /// Returns the number of entries evicted here (not those handed off).
    /// The payload is compressed inline: [`Self::put_payload_evicting`]
    /// takes one compressed elsewhere.
    pub async fn put_evicting(
        &self,
        key: CacheKey,
        result: CachedResult,
        is_stale: impl Fn(&CachedResult) -> bool,
        hand_off: impl FnOnce(Vec<u64>) -> Result<(), Vec<u64>>,
    ) -> usize {
        if !self.config.enabled {
            return 0;
        }
        let payload = self.encode_payload(&result.data);
        self.put_payload_now(key, result, payload, is_stale, hand_off)
    }

    /// [`Self::put_evicting`] with the payload already in its stored form:
    /// `payload` must be [`Self::encode_payload`] of `result.data` (the query
    /// cache makes it on the blocking pool for zstd). The entry is charged
    /// at the stored size.
    pub async fn put_payload_evicting(
        &self,
        key: CacheKey,
        result: CachedResult,
        payload: L2Payload,
        is_stale: impl Fn(&CachedResult) -> bool,
        hand_off: impl FnOnce(Vec<u64>) -> Result<(), Vec<u64>>,
    ) -> usize {
        self.put_payload_now(key, result, payload, is_stale, hand_off)
    }

    /// [`Self::put_payload_evicting`] as a plain call: a store never waits
    /// (an eviction pass runs inline or is handed off), so the query cache's
    /// store calls it directly, as it calls [`Self::get_now`].
    pub(crate) fn put_payload_now(
        &self,
        key: CacheKey,
        mut result: CachedResult,
        payload: L2Payload,
        is_stale: impl Fn(&CachedResult) -> bool,
        hand_off: impl FnOnce(Vec<u64>) -> Result<(), Vec<u64>>,
    ) -> usize {
        if !self.config.enabled {
            return 0;
        }
        let (stored, raw_len) = payload.into_parts();
        debug_assert_eq!(
            raw_len.unwrap_or(stored.len()),
            result.data.len(),
            "the payload of another result"
        );
        result.data = stored;

        let entry_size = result.size() + std::mem::size_of::<L2Entry>();
        let hash = key.hash_value();
        let mut evicted = 0;

        // Check size limit. Replacing an existing entry for the same key
        // (e.g. refilling one a write made stale) only grows the cache by
        // the difference.
        let max_bytes = self.config.max_bytes;
        let current_usage = self.memory_usage.load(Ordering::Relaxed);
        let replacing = self
            .memory_entries
            .get(&hash)
            .map(|e| e.memory_size)
            .unwrap_or(0);

        if current_usage.saturating_sub(replacing) + entry_size > max_bytes
            && self
                .evicting
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            let retired: Vec<u64> = self
                .memory_entries
                .iter()
                .filter(|e| e.is_expired() || is_stale(&e.result))
                .map(|e| *e.key())
                .collect();
            if retired.is_empty() {
                evicted += self.evict_to_fit(entry_size);
                self.end_eviction();
            } else if let Err(retired) = hand_off(retired) {
                evicted += self.shed(&retired);
                self.end_eviction();
            }
        }

        let fingerprint = format!("{:016x}", hash);
        let entry = match raw_len {
            Some(raw_len) => L2Entry::compressed(key, fingerprint, result, raw_len),
            None => L2Entry::new(key, fingerprint, result),
        };
        self.insert(hash, entry);
        evicted
    }

    /// Insert `entry` under `hash`. Replacing an entry for the same key (a
    /// refill after a write made it stale, a promotion) takes the old one's
    /// bytes out of the account, or the usage only grows and forces
    /// evictions of live entries.
    fn insert(&self, hash: u64, entry: L2Entry) {
        let (memory, saved) = (entry.memory_size, entry.saved_bytes());
        if let Some(old) = self.memory_entries.insert(hash, entry) {
            self.release(&old);
        }
        self.memory_usage.fetch_add(memory, Ordering::Relaxed);
        if saved > 0 {
            self.compression_saved.fetch_add(saved, Ordering::Relaxed);
        }
    }

    /// Take an entry that left the map out of the byte accounts.
    fn release(&self, entry: &L2Entry) {
        self.memory_usage
            .fetch_sub(entry.memory_size, Ordering::Relaxed);
        let saved = entry.saved_bytes();
        if saved > 0 {
            self.compression_saved.fetch_sub(saved, Ordering::Relaxed);
        }
    }

    /// Drop the entries with these key hashes (an eviction pass's retired
    /// set, see [`Self::put_evicting`]); returns how many were still there.
    pub fn shed(&self, hashes: &[u64]) -> usize {
        let mut removed = 0;
        for hash in hashes {
            if let Some((_, old)) = self.memory_entries.remove(hash) {
                self.release(&old);
                removed += 1;
            }
        }
        removed
    }

    /// [`Self::shed`] in steps of `shed_batch` entries with `shed_pause`
    /// between them, for the `cache-reclaim` thread (blocking: never call it
    /// on an async worker). Each removal is short; the pacing spreads a bulk
    /// shed out so concurrent readers are not contended at full rate for its
    /// whole duration. Returns how many entries it removed.
    pub fn shed_paced(&self, hashes: &[u64]) -> usize {
        let batch = self.config.shed_batch;
        if batch == 0 || self.config.shed_pause.is_zero() || hashes.len() <= batch {
            return self.shed(hashes);
        }
        let mut removed = 0;
        let mut chunks = hashes.chunks(batch).peekable();
        while let Some(chunk) = chunks.next() {
            removed += self.shed(chunk);
            if chunks.peek().is_some() {
                std::thread::sleep(self.config.shed_pause);
            }
        }
        removed
    }

    /// End the eviction pass [`Self::put_evicting`] handed off.
    pub fn end_eviction(&self) {
        self.evicting.store(false, Ordering::Release);
    }

    /// Remove an entry from the cache
    pub async fn remove(&self, key: &CacheKey) {
        self.remove_now(key)
    }

    /// [`Self::remove`] as a plain call (it never waits), for the query
    /// cache's lookup, as [`Self::get_now`].
    pub(crate) fn remove_now(&self, key: &CacheKey) {
        let hash = key.hash_value();

        if let Some((_, entry)) = self.memory_entries.remove(&hash) {
            self.release(&entry);
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
        self.memory_usage.store(0, Ordering::Relaxed);
        self.compression_saved.store(0, Ordering::Relaxed);

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
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Bytes compression saves on the entries in memory: the raw minus the
    /// stored size of their payloads (`0` without compression).
    pub fn compression_saved_bytes(&self) -> usize {
        self.compression_saved.load(Ordering::Relaxed)
    }

    /// Get cache statistics
    pub fn stats(&self) -> L2CacheStats {
        let total_access: u64 = self.memory_entries.iter().map(|e| e.access_count).sum();

        L2CacheStats {
            entry_count: self.memory_entries.len(),
            memory_usage_bytes: self.memory_usage(),
            max_memory_bytes: self.config.max_bytes,
            total_accesses: total_access,
            storage_backend: self.config.storage.clone(),
        }
    }

    /// Evict least-recently-used entries until `required_bytes` more fit,
    /// and `evict_headroom_pct` of `max_bytes` more besides; returns how many
    /// it evicted. One pass scans the map once: it takes every entry's last
    /// access and size, selects the oldest entries whose sizes cover what
    /// must go (expired ones first), and removes those not read since the
    /// scan. Its work is bounded by the map's size, never by how far behind
    /// the byte account is, and the headroom makes room for many puts per
    /// pass. (It used to scan the whole map again for every entry it
    /// evicted, while concurrent puts kept inserting: with L2 full under
    /// load, one put could run for minutes without yielding.)
    fn evict_to_fit(&self, required_bytes: usize) -> usize {
        let max_bytes = self.config.max_bytes;
        let target = max_bytes.saturating_sub(required_bytes);
        let usage = self.memory_usage.load(Ordering::Relaxed);
        if usage <= target {
            return 0;
        }
        let headroom = max_bytes / 100 * usize::from(self.config.evict_headroom_pct);
        let need = usage - target.saturating_sub(headroom);

        // (expired, last access, key hash, size) of every entry; expired
        // entries sort first, then the least recently used.
        let mut candidates: Vec<(bool, Instant, u64, usize)> = self
            .memory_entries
            .iter()
            .map(|e| (!e.is_expired(), e.last_access, *e.key(), e.memory_size))
            .collect();
        let chosen = Self::oldest_covering(&mut candidates, need);

        let mut evicted = 0;
        for &(live, seen, hash, _) in &candidates[..chosen] {
            // An entry read since the scan is no longer least recently used.
            let Some((_, entry)) = self
                .memory_entries
                .remove_if(&hash, |_, e| !live || e.last_access <= seen)
            else {
                continue;
            };
            if live && self.mmap_storage.is_some() {
                self.demote_to_mmap(hash, &entry);
            }
            self.release(&entry);
            evicted += 1;
        }
        evicted
    }

    /// Order the first entries of `candidates` oldest first (expired ones
    /// before live ones) until their sizes reach `need` bytes, and return
    /// how many that is (all of them when the whole map does not reach it).
    /// Expected linear time: each round selects about as many more entries as
    /// the bytes still missing take at the average entry size.
    fn oldest_covering(candidates: &mut [(bool, Instant, u64, usize)], need: usize) -> usize {
        let total: usize = candidates.iter().map(|c| c.3).sum();
        if total <= need {
            return candidates.len();
        }
        let average = (total / candidates.len()).max(1);
        let (mut chosen, mut covered) = (0, 0usize);
        while covered < need && chosen < candidates.len() {
            let rest = &mut candidates[chosen..];
            let more = ((need - covered) / average + 1).min(rest.len());
            if more < rest.len() {
                rest.select_nth_unstable_by_key(more - 1, |c| (c.0, c.1));
            }
            rest[..more].sort_unstable_by_key(|c| (c.0, c.1));
            for c in &rest[..more] {
                if covered >= need {
                    break;
                }
                covered += c.3;
                chosen += 1;
            }
        }
        chosen
    }

    /// Promote an entry from mmap to memory (the file keeps raw payloads:
    /// it is compressed again here when the codec applies).
    fn promote_to_memory(&self, key: &CacheKey, mut result: CachedResult) {
        let hash = key.hash_value();
        let fingerprint = format!("{:016x}", hash);
        let (stored, raw_len) = self.encode_payload(&result.data).into_parts();
        result.data = stored;
        let entry = match raw_len {
            Some(raw_len) => L2Entry::compressed(key.clone(), fingerprint, result, raw_len),
            None => L2Entry::new(key.clone(), fingerprint, result),
        };
        self.insert(hash, entry);
    }

    /// `entry`'s result with its raw payload (decoded when it is stored
    /// compressed; `None` when that blob does not decode).
    fn raw_result(entry: &L2Entry) -> Option<Cow<'_, CachedResult>> {
        match entry.raw_len {
            None => Some(Cow::Borrowed(&entry.result)),
            Some(raw_len) => {
                let raw = codec::decode(&entry.result.data, raw_len)?;
                Some(Cow::Owned(CachedResult {
                    data: Bytes::from(raw),
                    ..entry.result.clone()
                }))
            }
        }
    }

    /// Demote an entry to mmap storage, with its raw payload.
    fn demote_to_mmap(&self, hash: u64, entry: &L2Entry) {
        if let Some(ref mmap) = self.mmap_storage {
            if let Some(result) = Self::raw_result(entry) {
                if let Ok(mut storage) = mmap.write() {
                    storage.put(hash, &result);
                }
            }
        }
    }

    /// Flush memory entries to mmap (for graceful shutdown)
    pub fn flush_to_disk(&self) -> Result<usize, std::io::Error> {
        let Some(ref mmap) = self.mmap_storage else {
            return Ok(0);
        };

        let mut storage = mmap
            .write()
            .map_err(|_| std::io::Error::other("Lock poisoned"))?;

        let mut count = 0;
        for entry in self.memory_entries.iter() {
            if !entry.is_expired() {
                if let Some(result) = Self::raw_result(&entry) {
                    storage.put(*entry.key(), &result);
                    count += 1;
                }
            }
        }

        storage.sync()?;
        Ok(count)
    }

    /// Report entries in the current process's file index.
    ///
    /// This does not recover an index or cached entries from a previous process.
    pub fn load_from_disk(&self) -> Result<usize, std::io::Error> {
        let Some(ref mmap) = self.mmap_storage else {
            return Ok(0);
        };

        let storage = mmap
            .read()
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

        // Preserve the original monotonic lifetime, including time spent in
        // memory before demotion. Wall-clock adjustments cannot renew it.
        if mmap_expired_at(entry.cached_at, entry.ttl, Instant::now()) {
            return None;
        }

        // Read from file
        let mut file = File::open(&self.path).ok()?;
        let mut buffer = vec![0u8; entry.size];

        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(entry.offset as u64))
            .ok()?;
        file.read_exact(&mut buffer).ok()?;

        // Decoding checks expiry again: disk I/O may cross the deadline.
        deserialize_result(&buffer, entry.cached_at, entry.ttl)
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
            self.index.insert(
                hash,
                MmapEntry {
                    offset,
                    size: data.len(),
                    cached_at: result.cached_at,
                    ttl: result.ttl,
                },
            );
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

    // Keep the existing seconds field in the blob. The process-local index
    // carries the authoritative original age and full-precision TTL.
    buffer.extend_from_slice(&result.ttl.as_secs().to_le_bytes());

    // Write row count (8 bytes)
    buffer.extend_from_slice(&(result.row_count as u64).to_le_bytes());

    // Write data length (8 bytes) + data
    buffer.extend_from_slice(&(result.data.len() as u64).to_le_bytes());
    buffer.extend_from_slice(&result.data);

    // C-02: the generation the result was fetched under and the tables it
    // read, so a hit (and an L1 promotion) can be checked against later
    // writes. Generation (8) | table count (4) | per table: len (4) + bytes.
    buffer.extend_from_slice(&result.generation.to_le_bytes());
    buffer.extend_from_slice(&(result.tables.len() as u32).to_le_bytes());
    for table in &result.tables {
        buffer.extend_from_slice(&(table.len() as u32).to_le_bytes());
        buffer.extend_from_slice(table.as_bytes());
    }

    buffer
}

/// Deserialize with the original timing from the process-local file index.
fn deserialize_result(buffer: &[u8], cached_at: Instant, ttl: Duration) -> Option<CachedResult> {
    if buffer.len() < 24 {
        return None;
    }

    let ttl_secs = u64::from_le_bytes(buffer[0..8].try_into().ok()?);
    if ttl_secs != ttl.as_secs() {
        return None;
    }
    let row_count = u64::from_le_bytes(buffer[8..16].try_into().ok()?) as usize;
    let data_len = u64::from_le_bytes(buffer[16..24].try_into().ok()?) as usize;

    if buffer.len() < 24 + data_len {
        return None;
    }

    let data = Bytes::copy_from_slice(&buffer[24..24 + data_len]);

    // Generation + tables (C-02). A blob without them cannot be validated
    // against later writes, so it is not served.
    let mut rest = &buffer[24 + data_len..];
    let mut take = |n: usize| -> Option<&[u8]> {
        if rest.len() < n {
            return None;
        }
        let (head, tail) = rest.split_at(n);
        rest = tail;
        Some(head)
    };
    let generation = u64::from_le_bytes(take(8)?.try_into().ok()?);
    let count = u32::from_le_bytes(take(4)?.try_into().ok()?) as usize;
    let mut tables = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let len = u32::from_le_bytes(take(4)?.try_into().ok()?) as usize;
        tables.push(String::from_utf8(take(len)?.to_vec()).ok()?);
    }

    if mmap_expired_at(cached_at, ttl, Instant::now()) {
        return None;
    }

    Some(CachedResult {
        data,
        row_count,
        cached_at,
        ttl,
        tables,
        execution_time: std::time::Duration::from_millis(0),
        generation,
    })
}

/// An exhausted lifetime (including zero TTL) or invalid future insertion time
/// is a miss. Comparing elapsed time avoids overflow for very large TTLs.
fn mmap_expired_at(cached_at: Instant, ttl: Duration, now: Instant) -> bool {
    match now.checked_duration_since(cached_at) {
        Some(age) => age >= ttl,
        None => true,
    }
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
        CacheKey::from_parts(query_hash, "test".to_string(), None, None)
    }

    #[test]
    fn mmap_ttl_read_keeps_original_age_and_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = MmapStorage::new(dir.path().join("cache"));
        let mut result = create_result("aged rows");
        result.cached_at = Instant::now() - Duration::from_secs(45);
        let deadline = result.cached_at + result.ttl;
        storage.put(1, &result);

        for _ in 0..3 {
            let loaded = storage.get(1).unwrap();
            assert_eq!(loaded.cached_at, result.cached_at);
            assert_eq!(loaded.cached_at + loaded.ttl, deadline);
            // Repeated demotion must not renew the entry's lifetime either.
            storage.put(1, &loaded);
        }
    }

    #[test]
    fn mmap_ttl_read_preserves_subsecond_precision() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = MmapStorage::new(dir.path().join("cache"));
        let mut result = create_result("fractional ttl");
        result.ttl = Duration::new(60, 123_456_789);
        storage.put(1, &result);
        assert_eq!(storage.get(1).unwrap().ttl, result.ttl);
    }

    #[test]
    fn mmap_ttl_expired_result_is_not_revived_on_demotion() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = MmapStorage::new(dir.path().join("cache"));
        let mut result = create_result("expired rows");
        result.cached_at = Instant::now() - Duration::from_secs(120);
        assert!(result.is_expired());
        storage.put(1, &result);
        assert!(storage.get(1).is_none());
    }

    #[tokio::test]
    async fn mmap_ttl_promotion_keeps_age_and_l1_cap() {
        use crate::cache::{config::L1Config, l1_hot::L1HotCache};

        let dir = tempfile::tempdir().unwrap();
        let cache = L2WarmCache::new(L2Config {
            storage: StorageBackend::Mmap,
            mmap_path: Some(dir.path().join("cache")),
            ..L2Config::default()
        });
        let key = create_key(1);
        let mut result = create_result("aged rows");
        result.cached_at = Instant::now() - Duration::from_secs(45);
        cache.put(key.clone(), result.clone()).await;
        assert_eq!(cache.flush_to_disk().unwrap(), 1);
        cache.shed(&[key.hash_value()]);
        assert!(cache.is_empty());

        let promoted = cache.get(&key).await.unwrap();
        let l1 = L1HotCache::new(L1Config {
            ttl: Duration::from_secs(30),
            ..L1Config::default()
        });
        l1.put("SELECT 1".to_string(), promoted);
        assert!(l1.get("SELECT 1").is_none(), "L1 must cap the original age");
        let in_memory = cache.get(&key).await.unwrap();
        assert_eq!(in_memory.cached_at, result.cached_at);
        assert_eq!(in_memory.ttl, result.ttl);
    }

    #[tokio::test]
    async fn mmap_ttl_memory_storage_control_keeps_age() {
        let cache = L2WarmCache::new(L2Config::default());
        let key = create_key(1);
        let mut result = create_result("memory control");
        result.cached_at = Instant::now() - Duration::from_secs(45);
        cache.put(key.clone(), result.clone()).await;
        assert_eq!(cache.get(&key).await.unwrap().cached_at, result.cached_at);
    }

    #[test]
    fn mmap_ttl_exact_expiry_boundary_and_future_time() {
        let start = Instant::now();
        let ttl = Duration::from_nanos(950);
        assert!(!mmap_expired_at(
            start,
            ttl,
            start + ttl - Duration::from_nanos(1)
        ));
        assert!(mmap_expired_at(start, ttl, start + ttl));
        assert!(mmap_expired_at(
            start,
            ttl,
            start + ttl + Duration::from_nanos(1)
        ));
        assert!(mmap_expired_at(start, Duration::ZERO, start));
        assert!(mmap_expired_at(start, ttl, start - Duration::from_nanos(1)));
        assert!(!mmap_expired_at(start, Duration::MAX, start + ttl));
    }

    #[test]
    fn mmap_ttl_decode_rechecks_expiry_after_io() {
        let result = create_result("valid payload");
        let bytes = serialize_result(&result);
        // The same valid payload is usable before its deadline, but a read
        // completing after that deadline cannot revive it during decoding.
        assert!(deserialize_result(&bytes, result.cached_at, result.ttl).is_some());
        assert!(deserialize_result(
            &bytes,
            Instant::now() - Duration::from_secs(120),
            result.ttl,
        )
        .is_none());
        assert!(deserialize_result(
            &bytes,
            Instant::now() + Duration::from_secs(120),
            result.ttl,
        )
        .is_none());
    }

    #[test]
    fn mmap_ttl_decode_rejects_mismatched_index_timing() {
        let result = create_result("valid payload");
        let mut bytes = serialize_result(&result);
        bytes[..8].copy_from_slice(&(result.ttl.as_secs() + 1).to_le_bytes());
        assert!(deserialize_result(&bytes, result.cached_at, result.ttl).is_none());
    }

    #[test]
    fn mmap_ttl_less_than_one_second_is_not_truncated_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = MmapStorage::new(dir.path().join("cache"));
        let mut result = create_result("subsecond rows");
        result.ttl = Duration::from_millis(950);
        storage.put(1, &result);
        let loaded = storage.get(1).unwrap();
        assert_eq!(loaded.ttl, result.ttl);
        assert_eq!(loaded.cached_at, result.cached_at);
        assert!(!loaded.is_expired());
    }

    #[test]
    fn mmap_ttl_zero_future_and_huge_lifetimes() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = MmapStorage::new(dir.path().join("cache"));
        let mut result = create_result("rows");
        result.ttl = Duration::ZERO;
        storage.put(1, &result);
        assert!(storage.get(1).is_none());

        result.ttl = Duration::MAX;
        storage.put(1, &result);
        assert_eq!(storage.get(1).unwrap().ttl, Duration::MAX);

        result.cached_at = Instant::now() + Duration::from_secs(60);
        storage.put(1, &result);
        assert!(storage.get(1).is_none());
    }

    #[tokio::test]
    async fn mmap_ttl_expired_memory_then_disk_remain_misses() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2WarmCache::new(L2Config {
            storage: StorageBackend::Mmap,
            mmap_path: Some(dir.path().join("cache")),
            ..L2Config::default()
        });
        let key = create_key(1);
        let mut result = create_result("expired rows");
        result.cached_at = Instant::now() - Duration::from_secs(120);
        // Model an entry whose memory and disk copies have both aged out.
        cache
            .mmap_storage
            .as_ref()
            .unwrap()
            .write()
            .unwrap()
            .put(key.hash_value(), &result);
        cache.put(key.clone(), result).await;
        assert!(cache.get(&key).await.is_none()); // Removes expired memory.
        assert!(cache.is_empty());
        assert!(cache.get(&key).await.is_none()); // Disk cannot resurrect it.
        assert!(cache.is_empty());
    }

    /// `get_noting_expiry` tells an entry found past its TTL (removed) from
    /// an absent or a live one; `get` serves exactly what it serves.
    #[tokio::test]
    async fn get_noting_expiry_flags_only_an_expired_entry() {
        let cache = L2WarmCache::new(L2Config::default());
        let (live, old) = (create_key(1), create_key(2));
        cache.put(live.clone(), create_result("live")).await;
        let mut aged = create_result("aged");
        aged.cached_at = Instant::now() - Duration::from_secs(120);
        cache.put(old.clone(), aged).await;

        let mut expired = false;
        assert!(cache.get_noting_expiry(&live, &mut expired).await.is_some());
        assert!(!expired);
        assert!(cache
            .get_noting_expiry(&create_key(3), &mut expired)
            .await
            .is_none());
        assert!(!expired, "absent is not expired");
        assert!(cache.get_noting_expiry(&old, &mut expired).await.is_none());
        assert!(expired);
        let mut again = false;
        assert!(cache.get_noting_expiry(&old, &mut again).await.is_none());
        assert!(!again, "the expired entry was removed by the lookup");
        assert!(cache.get(&live).await.is_some());
    }

    /// A compressed entry whose blob does not decode is dropped and misses,
    /// but it was not past its TTL: no expiry (the heatmap's `expired`), and
    /// none on a decoded compressed hit either.
    #[tokio::test]
    async fn a_corrupt_compressed_entry_is_no_expiry() {
        let cache = codec_cache(CacheCompression::Lz4, 64 * 1024 * 1024);
        let (good, bad) = (create_key(7), create_key(8));
        let rows = rows_result(64 * 1024);
        cache.put(good.clone(), rows.clone()).await;
        cache.put(bad.clone(), rows.clone()).await;
        {
            let mut entry = cache.memory_entries.get_mut(&bad.hash_value()).unwrap();
            assert!(entry.raw_len.is_some(), "stored compressed");
            let mut blob = entry.result.data.to_vec();
            blob[0] = 0xff;
            entry.result.data = Bytes::from(blob);
        }
        let mut expired = false;
        let hit = cache.get_noting_expiry(&good, &mut expired).await;
        assert_eq!(hit.map(|r| r.data), Some(rows.data), "decoded");
        assert!(!expired);
        assert!(cache.get_noting_expiry(&bad, &mut expired).await.is_none());
        assert!(!expired, "a corrupt entry is not an expired one");
        assert_eq!(cache.len(), 1, "the corrupt entry was dropped");
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

    /// A 1 MB cache holding nine ~100 KB entries: data of `n` bytes.
    fn sized(n: usize) -> CachedResult {
        create_result(&"x".repeat(n))
    }

    /// When a put has to make room, every entry the caller calls stale goes
    /// in that one pass (C-02: a write retires a table's entries), and live
    /// entries stay; replacing an entry for the same key at capacity evicts
    /// nothing.
    #[tokio::test]
    async fn eviction_sheds_stale_entries_first_and_a_replace_evicts_nothing() {
        let cache = L2WarmCache::new(L2Config {
            max_bytes: 1024 * 1024,
            ..L2Config::default()
        });
        for i in 0..9u64 {
            let mut r = sized(100 * 1024);
            r.generation = i; // entries 0..5 will be "stale"
            cache.put(create_key(i), r).await;
        }
        assert_eq!(cache.stats().entry_count, 9);

        // Same key, same size, at capacity: replaced in place.
        let mut again = sized(100 * 1024);
        again.generation = 8;
        cache.put(create_key(8), again).await;
        assert_eq!(cache.stats().entry_count, 9, "a replace evicts nothing");

        // A new key needs room: the five stale entries all go, not one LRU
        // (shed here, the hand-off refusing them), and count as evicted.
        let evicted = cache
            .put_evicting(
                create_key(100),
                sized(300 * 1024),
                |r| r.generation < 5,
                Err,
            )
            .await;
        assert_eq!(evicted, 5);
        for i in 0..5 {
            assert!(cache.get(&create_key(i)).await.is_none(), "stale {i} shed");
        }
        for i in 5..9 {
            assert!(cache.get(&create_key(i)).await.is_some(), "live {i} kept");
        }
        assert!(cache.get(&create_key(100)).await.is_some());
        assert_eq!(cache.stats().entry_count, 5);
    }

    /// While an eviction pass is running (here: handed off and not yet
    /// ended), a put over the bound inserts without starting another scan;
    /// the handed-off hashes are exactly the retired entries.
    /// The reclaim thread's paced shed removes every retired entry, in
    /// steps of `shed_batch` with `shed_pause` between them.
    #[test]
    fn a_paced_shed_removes_everything_in_steps() {
        let cache = L2WarmCache::new(L2Config {
            max_bytes: 64 * 1024 * 1024,
            shed_batch: 3,
            shed_pause: Duration::from_millis(5),
            ..Default::default()
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for i in 0..10u64 {
            rt.block_on(cache.put(create_key(i), sized(64)));
        }
        let hashes: Vec<u64> = (0..10u64).map(|i| create_key(i).hash_value()).collect();
        let started = Instant::now();
        cache.shed_paced(&hashes);
        // 10 entries in steps of 3: 4 steps, 3 pauses.
        assert!(started.elapsed() >= Duration::from_millis(15));
        assert!(cache.is_empty());
        assert_eq!(cache.memory_usage(), 0);
        // Unpaced (batch 0): one step, no pause.
        let unpaced = L2WarmCache::new(L2Config {
            shed_batch: 0,
            ..Default::default()
        });
        rt.block_on(unpaced.put(create_key(1), sized(64)));
        unpaced.shed_paced(&[create_key(1).hash_value()]);
        assert!(unpaced.is_empty());
    }

    #[tokio::test]
    async fn one_eviction_pass_at_a_time() {
        let cache = L2WarmCache::new(L2Config {
            max_bytes: 1024 * 1024,
            ..L2Config::default()
        });
        for i in 0..9u64 {
            let mut r = sized(100 * 1024);
            r.generation = i;
            cache.put(create_key(i), r).await;
        }
        let mut handed = Vec::new();
        cache
            .put_evicting(
                create_key(100),
                sized(300 * 1024),
                |r| r.generation < 3,
                |h| {
                    handed = h;
                    Ok(())
                },
            )
            .await;
        handed.sort_unstable();
        let mut want: Vec<u64> = (0..3).map(|i| create_key(i).hash_value()).collect();
        want.sort_unstable();
        assert_eq!(handed, want, "the retired set went to the hand-off");
        assert_eq!(
            cache.stats().entry_count,
            10,
            "inserted over the soft bound"
        );

        // Pass still open: another put over the bound neither scans nor
        // hands anything off.
        cache
            .put_evicting(
                create_key(101),
                sized(300 * 1024),
                |_| true,
                |_| panic!("a second pass started while one was running"),
            )
            .await;
        assert_eq!(cache.stats().entry_count, 11);

        cache.shed(&handed);
        cache.end_eviction();
        assert_eq!(cache.stats().entry_count, 8);
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
            max_bytes: 1024 * 1024, // 1 MB limit
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
    fn blob_keeps_tables_and_generation() {
        let mut result = create_result("rows");
        result.tables = vec!["accounts".to_string(), "users".to_string()];
        result.generation = 42;
        let back =
            deserialize_result(&serialize_result(&result), result.cached_at, result.ttl).unwrap();
        assert_eq!(back.tables, result.tables);
        assert_eq!(back.generation, 42);
        assert_eq!(back.data, result.data);
        // A blob cut before its generation/tables trailer is not served.
        let mut short = serialize_result(&result);
        short.truncate(short.len() - 3);
        assert!(deserialize_result(&short, result.cached_at, result.ttl).is_none());
    }

    #[test]
    fn test_serialize_deserialize() {
        let result = create_result("test data for serialization");
        let serialized = serialize_result(&result);
        let deserialized = deserialize_result(&serialized, result.cached_at, result.ttl).unwrap();

        assert_eq!(deserialized.data, result.data);
        assert_eq!(deserialized.row_count, result.row_count);
        assert_eq!(deserialized.ttl.as_secs(), result.ttl.as_secs());
    }

    fn codec_cache(compression: CacheCompression, max_bytes: usize) -> L2WarmCache {
        L2WarmCache::new(L2Config {
            max_bytes,
            compression,
            ..L2Config::default()
        })
    }

    /// A result whose payload is `bytes` of DataRow frames.
    fn rows_result(bytes: usize) -> CachedResult {
        CachedResult::new(
            codec::data_rows(bytes),
            1,
            Duration::from_secs(60),
            vec!["test".to_string()],
            Duration::ZERO,
        )
    }

    /// The budget counts stored bytes: compressed, a 1 MiB L2 keeps all
    /// twenty 100 KiB DataRow results, raw at most ten. Every hit returns the
    /// bytes stored, and the saved-bytes gauge is the raw minus the stored
    /// total.
    #[tokio::test]
    async fn compression_fits_more_entries_in_the_budget() {
        const ENTRIES: u64 = 20;
        let result = rows_result(100 * 1024);
        let raw = codec_cache(CacheCompression::None, 1024 * 1024);
        for i in 0..ENTRIES {
            raw.put(create_key(i), result.clone()).await;
        }
        assert!(raw.len() <= 10, "raw: {} entries", raw.len());
        assert_eq!(raw.compression_saved_bytes(), 0);
        for compression in [CacheCompression::Lz4, CacheCompression::Zstd] {
            let cache = codec_cache(compression, 1024 * 1024);
            for i in 0..ENTRIES {
                cache.put(create_key(i), result.clone()).await;
            }
            assert_eq!(cache.len() as u64, ENTRIES, "{compression:?}");
            assert!(cache.memory_usage() <= 1024 * 1024, "{compression:?}");
            let stored: usize = cache.memory_entries.iter().map(|e| e.result.size()).sum();
            assert_eq!(
                cache.compression_saved_bytes(),
                ENTRIES as usize * result.data.len() - stored,
                "{compression:?}"
            );
            for i in 0..ENTRIES {
                let hit = cache.get(&create_key(i)).await.unwrap();
                assert_eq!(hit.data, result.data, "{compression:?} entry {i}");
                assert_eq!(hit.row_count, result.row_count);
                assert_eq!(hit.tables, result.tables);
            }
        }
    }

    /// Below `compression_min_bytes`, or without a codec, the payload is
    /// stored as given (the same buffer, nothing saved).
    #[tokio::test]
    async fn small_payloads_and_the_default_stay_raw() {
        for (compression, bytes) in [
            (CacheCompression::None, 64 * 1024),
            (CacheCompression::Lz4, 1024),
            (CacheCompression::Zstd, 4095),
        ] {
            let cache = codec_cache(compression, 64 * 1024 * 1024);
            let result = rows_result(bytes);
            let result = CachedResult {
                data: result.data.slice(..bytes),
                ..result
            };
            cache.put(create_key(1), result.clone()).await;
            let entry = cache
                .memory_entries
                .get(&create_key(1).hash_value())
                .unwrap();
            assert_eq!(entry.raw_len, None, "{compression:?}");
            assert_eq!(entry.result.data.as_ptr(), result.data.as_ptr());
            drop(entry);
            assert_eq!(cache.compression_saved_bytes(), 0);
            assert_eq!(cache.get(&create_key(1)).await.unwrap().data, result.data);
        }
    }

    /// Only zstd above the threshold leaves the async worker.
    #[test]
    fn only_zstd_compresses_off_the_worker() {
        let zstd = codec_cache(CacheCompression::Zstd, 1024 * 1024);
        assert!(zstd.compresses_off_worker(4096));
        assert!(!zstd.compresses_off_worker(4095));
        assert!(!codec_cache(CacheCompression::Lz4, 1024 * 1024).compresses_off_worker(1 << 20));
        assert!(!codec_cache(CacheCompression::None, 1024 * 1024).compresses_off_worker(1 << 20));
        let off = L2WarmCache::new(L2Config {
            enabled: false,
            compression: CacheCompression::Zstd,
            ..L2Config::default()
        });
        assert!(!off.compresses_off_worker(1 << 20));
    }

    /// A compressed entry whose blob does not decode (an unknown marker, a
    /// damaged frame) is never served: the read misses and the entry leaves
    /// the cache and both byte accounts.
    #[tokio::test]
    async fn a_corrupt_compressed_entry_is_a_miss_and_is_dropped() {
        let damages: [fn(&mut [u8]); 2] = [|blob| blob[0] = 0xff, |blob| blob[1..].fill(0xff)];
        for compression in [CacheCompression::Lz4, CacheCompression::Zstd] {
            for (i, damage) in damages.iter().enumerate() {
                let cache = codec_cache(compression, 64 * 1024 * 1024);
                let key = create_key(7);
                cache.put(key.clone(), rows_result(64 * 1024)).await;
                cache.put(create_key(8), rows_result(64 * 1024)).await;
                let both = cache.compression_saved_bytes();
                assert!(both > 0);
                {
                    let mut entry = cache.memory_entries.get_mut(&key.hash_value()).unwrap();
                    assert!(entry.raw_len.is_some());
                    let mut blob = entry.result.data.to_vec();
                    damage(&mut blob);
                    entry.result.data = Bytes::from(blob);
                }
                assert!(
                    cache.get(&key).await.is_none(),
                    "{compression:?} damage {i}"
                );
                assert_eq!(cache.len(), 1, "dropped");
                assert_eq!(cache.compression_saved_bytes(), both / 2);
                cache.shed(&[create_key(8).hash_value()]);
                assert_eq!(cache.memory_usage(), 0);
                assert_eq!(cache.compression_saved_bytes(), 0);
            }
        }
    }

    /// The saved-bytes account follows every way an entry leaves: replaced,
    /// removed, shed, evicted, cleared.
    #[tokio::test]
    async fn saved_bytes_follow_every_removal() {
        let cache = codec_cache(CacheCompression::Lz4, 64 * 1024 * 1024);
        let result = rows_result(32 * 1024);
        for i in 0..4 {
            cache.put(create_key(i), result.clone()).await;
        }
        let one = cache.compression_saved_bytes() / 4;
        assert!(one > 0);
        assert_eq!(cache.compression_saved_bytes(), 4 * one);
        cache.put(create_key(0), result.clone()).await;
        assert_eq!(cache.compression_saved_bytes(), 4 * one, "replaced");
        cache.remove(&create_key(0)).await;
        assert_eq!(cache.compression_saved_bytes(), 3 * one, "removed");
        assert_eq!(cache.shed(&[create_key(1).hash_value()]), 1);
        assert_eq!(cache.compression_saved_bytes(), 2 * one, "shed");
        cache.clear().await;
        assert_eq!(cache.compression_saved_bytes(), 0, "cleared");
        assert_eq!(cache.memory_usage(), 0);

        // Evicted to make room: a budget of about three entries.
        let small = codec_cache(CacheCompression::Lz4, 1);
        small.put(create_key(1), result.clone()).await;
        let budget = small.memory_usage() * 3;
        let small = codec_cache(CacheCompression::Lz4, budget);
        let mut evicted = 0;
        for i in 0..10 {
            evicted += small
                .put_evicting(create_key(i), result.clone(), |_| false, Err)
                .await;
        }
        assert_eq!(evicted, 10 - small.len());
        assert!(evicted > 0);
        assert_eq!(small.compression_saved_bytes(), small.len() * one);
    }

    /// Stores that skipped eviction while a pass ran (the anti-herd rule)
    /// can leave L2 far over its budget. One pass then brings it under the
    /// budget less the headroom, in a single scan, least recently used
    /// first: entries read since keep their place.
    #[tokio::test]
    async fn one_eviction_pass_clears_a_backlog_with_headroom_keeping_recent_reads() {
        let cache = L2WarmCache::new(L2Config {
            max_bytes: 1024 * 1024,
            evict_headroom_pct: 20,
            ..L2Config::default()
        });
        // About twice the budget, inserted without evicting.
        for i in 0..200u64 {
            let key = create_key(i);
            cache.insert(
                key.hash_value(),
                L2Entry::new(key, format!("{i:016x}"), sized(10 * 1024)),
            );
        }
        assert!(cache.memory_usage() > 2 * 1024 * 1024 - 64 * 1024);
        tokio::time::sleep(Duration::from_millis(2)).await;
        for i in 0..20u64 {
            assert!(cache.get(&create_key(i)).await.is_some());
        }
        let need = 10 * 1024 + std::mem::size_of::<L2Entry>();
        let evicted = cache.evict_to_fit(need);
        let low = 1024 * 1024 - need - 1024 * 1024 / 100 * 20;
        assert!(
            cache.memory_usage() <= low,
            "{} > {low}",
            cache.memory_usage()
        );
        assert_eq!(evicted, 200 - cache.len());
        for i in 0..20u64 {
            assert!(
                cache.get(&create_key(i)).await.is_some(),
                "entry {i} was read after the others were stored"
            );
        }
        // Under the budget now: the next pass evicts nothing.
        assert_eq!(cache.evict_to_fit(need), 0);
    }

    /// Expired entries go before any live one, however recently used.
    #[tokio::test]
    async fn an_eviction_pass_takes_expired_entries_first() {
        let cache = L2WarmCache::new(L2Config {
            max_bytes: 1024 * 1024,
            evict_headroom_pct: 0,
            ..L2Config::default()
        });
        for i in 0..60u64 {
            let key = create_key(i);
            let mut result = sized(10 * 1024);
            if i >= 50 {
                result.ttl = Duration::from_millis(1);
            }
            cache.insert(key.hash_value(), L2Entry::new(key, format!("{i}"), result));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Room for one more entry, which the ten expired ones cover.
        let usage = cache.memory_usage();
        let need = 1024 * 1024 + 10 * 1024 - usage;
        let evicted = cache.evict_to_fit(need);
        assert!((1..=10).contains(&evicted), "{evicted} evicted");
        for i in 0..50u64 {
            assert!(cache
                .memory_entries
                .contains_key(&create_key(i).hash_value()));
        }
    }

    /// `oldest_covering` picks exactly the shortest oldest-first prefix (an
    /// expired entry before any live one) whose sizes reach `need`.
    #[test]
    fn oldest_covering_matches_a_full_sort() {
        let base = Instant::now();
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for round in 0..200 {
            let n = 1 + (next() % 500) as usize;
            let mut candidates: Vec<(bool, Instant, u64, usize)> = (0..n as u64)
                .map(|hash| {
                    (
                        next() % 8 != 0,
                        // Distinct in practice: the expected count assumes no ties.
                        base + Duration::from_nanos(next() % 1_000_000_000_000),
                        hash,
                        1 + (next() % 4096) as usize,
                    )
                })
                .collect();
            let total: usize = candidates.iter().map(|c| c.3).sum();
            let need = if round % 10 == 0 {
                total + 1
            } else {
                1 + (next() as usize % total)
            };
            let mut sorted = candidates.clone();
            sorted.sort_by_key(|c| (c.0, c.1, c.2));
            let mut expected = 0;
            let mut covered = 0;
            while covered < need && expected < sorted.len() {
                covered += sorted[expected].3;
                expected += 1;
            }
            let chosen = L2WarmCache::oldest_covering(&mut candidates, need);
            assert_eq!(chosen, expected, "round {round}");
            // The same entries up to ties in (expired, last access).
            let cut = sorted[expected - 1];
            let mut picked: Vec<_> = candidates[..chosen].iter().map(|c| (c.0, c.1)).collect();
            picked.sort();
            let mut want: Vec<_> = sorted[..expected].iter().map(|c| (c.0, c.1)).collect();
            want.sort();
            assert_eq!(picked, want, "round {round}");
            assert!(candidates[..chosen]
                .iter()
                .all(|c| (c.0, c.1) <= (cut.0, cut.1)));
        }
    }

    /// `put_evicting` reports the entries it evicted to make room: expired
    /// and least recently used.
    #[tokio::test]
    async fn put_evicting_counts_its_evictions() {
        let cache = L2WarmCache::new(L2Config {
            max_bytes: 1024 * 1024,
            ..L2Config::default()
        });
        let mut evicted = 0;
        for i in 0..15u64 {
            evicted += cache
                .put_evicting(create_key(i), sized(100 * 1024), |_| false, Err)
                .await;
        }
        assert!(evicted > 0);
        assert_eq!(evicted, 15 - cache.len());
        // A replacement is no eviction.
        let before = cache.len();
        assert_eq!(
            cache
                .put_evicting(create_key(14), sized(100 * 1024), |_| false, Err)
                .await,
            0
        );
        assert_eq!(cache.len(), before);
    }

    /// A compressed entry demoted to the spill file is written raw and comes
    /// back byte-identical, compressed again in memory.
    #[tokio::test]
    async fn a_compressed_entry_spills_raw_and_comes_back_identical() {
        let dir = tempfile::tempdir().unwrap();
        let cache = L2WarmCache::new(L2Config {
            storage: StorageBackend::Mmap,
            mmap_path: Some(dir.path().join("cache")),
            compression: CacheCompression::Zstd,
            ..L2Config::default()
        });
        let key = create_key(1);
        let result = rows_result(64 * 1024);
        cache.put(key.clone(), result.clone()).await;
        assert!(cache.compression_saved_bytes() > 0);
        assert_eq!(cache.flush_to_disk().unwrap(), 1);
        cache.shed(&[key.hash_value()]);
        assert_eq!(cache.compression_saved_bytes(), 0);
        let back = cache.get(&key).await.unwrap();
        assert_eq!(back.data, result.data, "from the file");
        assert!(cache.compression_saved_bytes() > 0, "promoted compressed");
        assert_eq!(cache.get(&key).await.unwrap().data, result.data);
    }
}
