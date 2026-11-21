//! WAL-based cache invalidator
//!
//! Subscribes to WAL stream for real-time cache coherency.
//! Invalidates cached entries when underlying data changes.
//!
//! The invalidator fails closed: [`WalInvalidator::start`] returns an error
//! when the endpoint cannot be reached or does not acknowledge the
//! subscription, and a stream that breaks (I/O error, malformed or
//! unrecognised frame) stops the invalidator — [`WalInvalidator::is_running`]
//! turns `false` — instead of silently skipping changes it could not read.
//!
//! # Protocol
//!
//! The WAL streaming protocol uses TCP with the following message format:
//! ```text
//! [1 byte: message type][4 bytes: payload length][payload]
//! ```
//!
//! Message types:
//! - 0x01: WAL entry
//! - 0x02: Heartbeat
//! - 0x03: Subscription request
//! - 0x04: Subscription ack

use dashmap::DashMap;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{CacheError, CacheResult, DistribCacheConfig, QueryFingerprint};

/// WAL protocol message types
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
enum WalMessageType {
    Entry = 0x01,
    Heartbeat = 0x02,
    Subscribe = 0x03,
    SubscribeAck = 0x04,
    Error = 0xFF,
}

/// WAL operation types
#[derive(Debug, Clone)]
pub enum WalOperation {
    /// Insert/Update operation
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete operation
    Delete { key: Vec<u8> },
    /// Counter update
    UpdateCounter { table_name: String, counter: u64 },
    /// Schema change
    SchemaChange { table_name: String },
    /// Transaction commit
    Commit { txn_id: u64 },
}

/// WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    /// Log sequence number
    pub lsn: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Operation
    pub operation: WalOperation,
}

/// WAL stream subscriber with TCP connection
pub struct WalStreamer {
    /// TCP connection to WAL server
    connection: Option<TcpStream>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Last heartbeat received
    last_heartbeat: Instant,
}

impl WalStreamer {
    fn new() -> Self {
        Self {
            connection: None,
            running: Arc::new(AtomicBool::new(false)),
            last_heartbeat: Instant::now(),
        }
    }

    /// Connect to the WAL streaming endpoint
    async fn connect(endpoint: &str) -> CacheResult<Self> {
        let mut streamer = Self::new();

        // Attempt TCP connection
        match TcpStream::connect_timeout(
            &endpoint
                .parse()
                .map_err(|_| CacheError::ConnectionError("Invalid endpoint address".to_string()))?,
            Duration::from_secs(5),
        ) {
            Ok(stream) => {
                // Set TCP options
                stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
                stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
                stream.set_nodelay(true).ok();

                streamer.connection = Some(stream);
                streamer.last_heartbeat = Instant::now();
                Ok(streamer)
            }
            Err(e) => Err(CacheError::ConnectionError(format!(
                "failed to connect to WAL endpoint {}: {}",
                endpoint, e
            ))),
        }
    }

    /// Send subscription request and start receiving WAL entries
    async fn subscribe(&mut self, start_lsn: Option<u64>) -> CacheResult<WalSubscription> {
        let stream = self.connection.as_mut().ok_or_else(|| {
            CacheError::ConnectionError("WAL streamer is not connected".to_string())
        })?;
        {
            // Build subscription request
            let lsn = start_lsn.unwrap_or(0);
            let mut request = vec![WalMessageType::Subscribe as u8];
            request.extend_from_slice(&(8u32).to_be_bytes()); // payload length
            request.extend_from_slice(&lsn.to_be_bytes()); // start LSN

            // Send subscription request
            if let Err(e) = stream.write_all(&request) {
                tracing::error!("Failed to send subscription request: {}", e);
                return Err(CacheError::ConnectionError(format!(
                    "Subscription failed: {}",
                    e
                )));
            }

            // Wait for the subscription ack; anything else is a failure.
            let mut header = [0u8; 5];
            stream.read_exact(&mut header).map_err(|e| {
                CacheError::ConnectionError(format!("no WAL subscription ack received: {}", e))
            })?;
            let payload_len =
                u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
            if header[0] == WalMessageType::Error as u8 {
                return Err(CacheError::ConnectionError(
                    "WAL subscription rejected by server".to_string(),
                ));
            }
            if header[0] != WalMessageType::SubscribeAck as u8 {
                return Err(CacheError::ConnectionError(format!(
                    "unexpected WAL message type 0x{:02x} instead of a subscription ack",
                    header[0]
                )));
            }
            if payload_len > 0 {
                let mut payload = vec![0u8; payload_len.min(MAX_WAL_PAYLOAD)];
                stream.read_exact(&mut payload).map_err(|e| {
                    CacheError::ConnectionError(format!("truncated WAL subscription ack: {}", e))
                })?;
            }
            tracing::info!("WAL subscription acknowledged");
        }

        self.running.store(true, Ordering::SeqCst);
        Ok(WalSubscription {
            running: self.running.clone(),
            connection: self.connection.take(),
            current_lsn: 0,
            buffer: Vec::with_capacity(64 * 1024),
        })
    }
}

/// WAL subscription for receiving streaming WAL entries
pub struct WalSubscription {
    running: Arc<AtomicBool>,
    connection: Option<TcpStream>,
    current_lsn: u64,
    buffer: Vec<u8>,
}

/// Largest WAL frame payload accepted (10 MiB).
const MAX_WAL_PAYLOAD: usize = 10 * 1024 * 1024;

impl WalSubscription {
    /// Mark the stream unusable. Every later `next` returns `None` and
    /// [`Self::is_active`] reports `false`.
    fn fail(&mut self, reason: &str) -> Option<WalEntry> {
        tracing::error!(
            lsn = self.current_lsn,
            "WAL stream broken, cache invalidation stopped: {}",
            reason
        );
        self.running.store(false, Ordering::SeqCst);
        self.connection = None;
        None
    }

    /// Receive the next WAL entry.
    ///
    /// Returns `None` when no entry arrived within the read timeout (the
    /// subscription stays active) or when the stream broke — I/O error,
    /// oversized, malformed or unrecognised frame — in which case the
    /// subscription becomes inactive: a frame that cannot be read may carry
    /// a change, so it is never skipped silently.
    pub async fn next(&mut self) -> Option<WalEntry> {
        loop {
            if !self.running.load(Ordering::SeqCst) {
                return None;
            }

            let stream = match self.connection.as_mut() {
                Some(s) => s,
                None => {
                    // No connection - sleep and return None
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    return None;
                }
            };

            // Read message header: [type: 1 byte][length: 4 bytes]
            let mut header = [0u8; 5];
            match stream.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data available, yield and retry
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    // No frame within the read timeout; still subscribed.
                    return None;
                }
                Err(e) => return self.fail(&format!("read error: {}", e)),
            }

            let msg_type = header[0];
            let payload_len =
                u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

            if payload_len > MAX_WAL_PAYLOAD {
                return self.fail(&format!("frame payload of {} bytes", payload_len));
            }

            // Handle heartbeat messages
            if msg_type == WalMessageType::Heartbeat as u8 {
                // Heartbeat has no payload or small status payload
                if payload_len > 0 {
                    let mut payload = vec![0u8; payload_len];
                    if let Err(e) = stream.read_exact(&mut payload) {
                        return self.fail(&format!("truncated heartbeat: {}", e));
                    }
                }
                continue; // Skip heartbeats
            }

            // Only WAL entries may follow a subscription
            if msg_type != WalMessageType::Entry as u8 {
                return self.fail(&format!("unexpected message type 0x{:02x}", msg_type));
            }

            self.buffer.resize(payload_len, 0);
            if let Err(e) = stream.read_exact(&mut self.buffer) {
                return self.fail(&format!("truncated WAL entry: {}", e));
            }

            // Parse WAL entry from payload
            // Format: [lsn: 8 bytes][timestamp: 8 bytes][op_type: 1 byte][data...]
            if self.buffer.len() < 17 {
                return self.fail("WAL entry shorter than its 17-byte header");
            }

            let lsn = u64::from_be_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
                self.buffer[4],
                self.buffer[5],
                self.buffer[6],
                self.buffer[7],
            ]);
            let timestamp = u64::from_be_bytes([
                self.buffer[8],
                self.buffer[9],
                self.buffer[10],
                self.buffer[11],
                self.buffer[12],
                self.buffer[13],
                self.buffer[14],
                self.buffer[15],
            ]);
            let op_type = self.buffer[16];
            let data = self.buffer[17..].to_vec();
            let data = data.as_slice();

            let operation = match op_type {
                0x01 => {
                    // Put operation: [key_len: 4][key][value]
                    if data.len() < 4 {
                        return self.fail("truncated WAL entry key length");
                    }
                    let key_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                    if data.len() < 4 + key_len {
                        return self.fail("truncated WAL entry key");
                    }
                    let key = data[4..4 + key_len].to_vec();
                    let value = data[4 + key_len..].to_vec();
                    WalOperation::Put { key, value }
                }
                0x02 => {
                    // Delete operation: [key_len: 4][key]
                    if data.len() < 4 {
                        return self.fail("truncated WAL entry key length");
                    }
                    let key_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                    if data.len() < 4 + key_len {
                        return self.fail("truncated WAL entry key");
                    }
                    let key = data[4..4 + key_len].to_vec();
                    WalOperation::Delete { key }
                }
                0x03 => {
                    // Counter update: [table_name_len: 2][table_name][counter: 8]
                    if data.len() < 10 {
                        return self.fail("truncated counter update");
                    }
                    let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                    if data.len() < 2 + name_len + 8 {
                        return self.fail("truncated counter update");
                    }
                    let table_name = String::from_utf8_lossy(&data[2..2 + name_len]).to_string();
                    let counter_offset = 2 + name_len;
                    let counter = u64::from_be_bytes([
                        data[counter_offset],
                        data[counter_offset + 1],
                        data[counter_offset + 2],
                        data[counter_offset + 3],
                        data[counter_offset + 4],
                        data[counter_offset + 5],
                        data[counter_offset + 6],
                        data[counter_offset + 7],
                    ]);
                    WalOperation::UpdateCounter {
                        table_name,
                        counter,
                    }
                }
                0x04 => {
                    // Schema change: [table_name_len: 2][table_name]
                    if data.len() < 2 {
                        return self.fail("truncated schema change");
                    }
                    let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                    if data.len() < 2 + name_len {
                        return self.fail("truncated schema change");
                    }
                    let table_name = String::from_utf8_lossy(&data[2..2 + name_len]).to_string();
                    WalOperation::SchemaChange { table_name }
                }
                0x05 => {
                    // Commit: [txn_id: 8]
                    if data.len() < 8 {
                        return self.fail("truncated commit");
                    }
                    let txn_id = u64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]);
                    WalOperation::Commit { txn_id }
                }
                other => {
                    return self.fail(&format!("unknown WAL operation type 0x{:02x}", other));
                }
            };

            self.current_lsn = lsn;
            return Some(WalEntry {
                lsn,
                timestamp,
                operation,
            });
        }
    }

    /// Get current LSN position
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Check if subscription is active
    pub fn is_active(&self) -> bool {
        self.running.load(Ordering::SeqCst) && self.connection.is_some()
    }
}

/// Invalidation target
#[derive(Debug, Clone)]
pub struct InvalidationTarget {
    /// Table name
    pub table: String,
    /// Optional row key for fine-grained invalidation
    pub row_key: Option<Vec<u8>>,
    /// Whether to invalidate all entries for this table
    pub invalidate_all: bool,
}

/// Invalidation callback
pub type InvalidationCallback = Arc<dyn Fn(&InvalidationTarget) + Send + Sync>;

/// WAL-based cache invalidator
pub struct WalInvalidator {
    /// Active WAL subscription
    subscription: tokio::sync::RwLock<Option<WalSubscription>>,

    /// Table to fingerprint index
    table_index: DashMap<String, HashSet<QueryFingerprint>>,

    /// Invalidation callbacks (synchronous; never held across an await)
    callbacks: Arc<parking_lot::RwLock<Vec<InvalidationCallback>>>,

    /// Running flag
    running: Arc<AtomicBool>,

    /// Last processed LSN
    last_lsn: AtomicU64,

    /// Statistics
    stats: InvalidatorStats,
}

/// Invalidator statistics
#[derive(Debug, Default)]
struct InvalidatorStats {
    wal_entries_processed: AtomicU64,
    tables_invalidated: AtomicU64,
    entries_invalidated: AtomicU64,
    invalidation_lag_ms: AtomicU64,
}

impl WalInvalidator {
    /// Create a new invalidator
    pub fn new(_config: DistribCacheConfig) -> Self {
        Self {
            subscription: tokio::sync::RwLock::new(None),
            table_index: DashMap::new(),
            callbacks: Arc::new(parking_lot::RwLock::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            last_lsn: AtomicU64::new(0),
            stats: InvalidatorStats::default(),
        }
    }

    /// Connect to the WAL endpoint and subscribe from the last processed
    /// LSN.
    ///
    /// Fails — and leaves the invalidator stopped — when the endpoint cannot
    /// be reached or does not acknowledge the subscription. Entries are
    /// processed by [`Self::start_processing`] / [`Self::process_loop`].
    pub async fn start(&self, wal_endpoint: &str) -> CacheResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        let mut streamer = WalStreamer::connect(wal_endpoint).await?;

        // Start subscription from last known LSN (for recovery)
        let start_lsn = self.last_lsn.load(Ordering::Relaxed);
        let start_lsn = if start_lsn > 0 { Some(start_lsn) } else { None };

        let sub = streamer.subscribe(start_lsn).await?;
        *self.subscription.write().await = Some(sub);
        self.running.store(true, Ordering::SeqCst);
        tracing::info!("WAL invalidator started, connected to {}", wal_endpoint);
        Ok(())
    }

    /// Run [`Self::process_loop`] in the background until [`Self::stop`] or
    /// until the stream breaks. The WAL socket is read with blocking I/O, so
    /// the loop runs on a thread of the runtime's blocking pool rather than
    /// occupying an async worker. Must be called within a Tokio runtime.
    pub fn start_processing(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = Arc::clone(self);
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            tracing::info!("WAL processing loop started");
            handle.block_on(this.process_loop());
            tracing::info!("WAL processing loop stopped");
        })
    }

    /// Process WAL entries until [`Self::stop`] or until the stream breaks.
    /// A broken (or missing) subscription stops the invalidator:
    /// [`Self::is_running`] turns `false`.
    pub async fn process_loop(&self) {
        while self.running.load(Ordering::SeqCst) {
            let entry = {
                let mut sub_guard = self.subscription.write().await;
                match sub_guard.as_mut() {
                    Some(sub) if sub.is_active() => sub.next().await,
                    _ => {
                        if self.running.swap(false, Ordering::SeqCst) {
                            tracing::error!(
                                "WAL subscription is not active; cache invalidation stopped"
                            );
                        }
                        break;
                    }
                }
            };

            match entry {
                Some(wal_entry) => {
                    let start = Instant::now();
                    self.process_wal_entry(wal_entry.clone()).await;
                    self.last_lsn.store(wal_entry.lsn, Ordering::Relaxed);

                    // Track invalidation lag
                    let lag = start.elapsed().as_millis() as u64;
                    self.stats.invalidation_lag_ms.store(lag, Ordering::Relaxed);
                }
                None => {
                    // No entry available, brief sleep
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Stop the invalidator
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);

        // Close subscription
        let mut sub = self.subscription.write().await;
        *sub = None;

        tracing::info!("WAL invalidator stopped");
    }

    /// Check if running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Register a cache fingerprint for a table
    pub fn register(&self, table: &str, fingerprint: QueryFingerprint) {
        self.table_index
            .entry(table.to_string())
            .or_default()
            .insert(fingerprint);
    }

    /// Unregister a fingerprint
    pub fn unregister(&self, table: &str, fingerprint: &QueryFingerprint) {
        if let Some(mut set) = self.table_index.get_mut(table) {
            set.remove(fingerprint);
        }
    }

    /// Add invalidation callback
    pub async fn add_callback(&self, callback: InvalidationCallback) {
        self.add_callback_sync(callback);
    }

    /// Add invalidation callback from synchronous code.
    pub fn add_callback_sync(&self, callback: InvalidationCallback) {
        self.callbacks.write().push(callback);
    }

    /// Process a WAL entry
    async fn process_wal_entry(&self, entry: WalEntry) {
        self.stats
            .wal_entries_processed
            .fetch_add(1, Ordering::Relaxed);

        let (table, row_key) = match &entry.operation {
            WalOperation::Put { key, .. } => (self.extract_table(key), Some(key.clone())),
            WalOperation::Delete { key } => (self.extract_table(key), Some(key.clone())),
            WalOperation::UpdateCounter { table_name, .. } => (Some(table_name.clone()), None),
            WalOperation::SchemaChange { table_name } => {
                // Schema changes invalidate all entries for the table
                self.invalidate_table(table_name, true).await;
                return;
            }
            WalOperation::Commit { .. } => return,
        };

        if let Some(table) = table {
            // Use fine-grained invalidation if row key available
            if let Some(key) = row_key {
                self.invalidate_row(&table, &key).await;
            } else {
                self.invalidate_table(&table, false).await;
            }
        }
    }

    /// Invalidate entries for a table
    async fn invalidate_table(&self, table: &str, all_entries: bool) {
        self.stats
            .tables_invalidated
            .fetch_add(1, Ordering::Relaxed);

        let target = InvalidationTarget {
            table: table.to_string(),
            row_key: None,
            invalidate_all: all_entries,
        };

        // Notify callbacks
        let callbacks = self.callbacks.read().clone();
        for callback in callbacks.iter() {
            callback(&target);
        }

        // Track invalidated entries
        if let Some(entries) = self.table_index.get(table) {
            self.stats
                .entries_invalidated
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
        }
    }

    /// Fine-grained row invalidation
    async fn invalidate_row(&self, table: &str, row_key: &[u8]) {
        let target = InvalidationTarget {
            table: table.to_string(),
            row_key: Some(row_key.to_vec()),
            invalidate_all: false,
        };

        let callbacks = self.callbacks.read().clone();
        for callback in callbacks.iter() {
            callback(&target);
        }
    }

    /// Manually invalidate a table (public API)
    pub async fn invalidate_table_manual(&self, table: &str, all_entries: bool) {
        self.invalidate_table(table, all_entries).await;
    }

    /// Extract table name from key
    fn extract_table(&self, key: &[u8]) -> Option<String> {
        // Key format assumed: "table:primary_key"
        let key_str = String::from_utf8_lossy(key);
        key_str.split(':').next().map(|s| s.to_string())
    }

    /// Get invalidator statistics
    pub fn stats(&self) -> InvalidatorStatsSnapshot {
        InvalidatorStatsSnapshot {
            wal_entries_processed: self.stats.wal_entries_processed.load(Ordering::Relaxed),
            tables_invalidated: self.stats.tables_invalidated.load(Ordering::Relaxed),
            entries_invalidated: self.stats.entries_invalidated.load(Ordering::Relaxed),
            invalidation_lag_ms: self.stats.invalidation_lag_ms.load(Ordering::Relaxed),
            registered_tables: self.table_index.len(),
            is_running: self.running.load(Ordering::Relaxed),
        }
    }
}

/// Invalidator statistics snapshot
#[derive(Debug, Clone)]
pub struct InvalidatorStatsSnapshot {
    pub wal_entries_processed: u64,
    pub tables_invalidated: u64,
    pub entries_invalidated: u64,
    pub invalidation_lag_ms: u64,
    pub registered_tables: usize,
    pub is_running: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_fingerprint() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        invalidator.register("users", fp.clone());

        assert!(invalidator.table_index.contains_key("users"));
        let entries = invalidator.table_index.get("users").unwrap();
        assert!(entries.contains(&fp));
    }

    #[test]
    fn test_unregister_fingerprint() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        invalidator.register("users", fp.clone());
        invalidator.unregister("users", &fp);

        let entries = invalidator.table_index.get("users").unwrap();
        assert!(!entries.contains(&fp));
    }

    #[tokio::test]
    async fn test_callback_invocation() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();

        invalidator
            .add_callback(Arc::new(move |target| {
                if target.table == "users" {
                    called_clone.store(true, Ordering::SeqCst);
                }
            }))
            .await;

        invalidator.invalidate_table_manual("users", false).await;

        assert!(called.load(Ordering::SeqCst));
    }

    #[test]
    fn test_extract_table() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        let key = b"users:123";
        let table = invalidator.extract_table(key);
        assert_eq!(table, Some("users".to_string()));
    }

    #[tokio::test]
    async fn test_process_wal_entry() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        invalidator.register("users", fp);

        let entry = WalEntry {
            lsn: 1,
            timestamp: 0,
            operation: WalOperation::Put {
                key: b"users:123".to_vec(),
                value: b"data".to_vec(),
            },
        };

        invalidator.process_wal_entry(entry).await;

        let stats = invalidator.stats();
        assert_eq!(stats.wal_entries_processed, 1);
    }

    /// Fail closed: an unreachable endpoint is a start error, and the
    /// invalidator does not report itself running.
    #[tokio::test]
    async fn test_start_unreachable_endpoint_fails() {
        let invalidator = WalInvalidator::new(DistribCacheConfig::default());
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = dead.local_addr().unwrap();
        drop(dead);

        let err = invalidator.start(&addr.to_string()).await.unwrap_err();
        assert!(matches!(err, CacheError::ConnectionError(_)), "got {err:?}");
        assert!(!invalidator.is_running());
    }

    /// Fake WAL server: acks the subscription (or answers `first` instead),
    /// then writes `frames` and keeps the socket open until the test ends.
    fn spawn_wal_server(first: Vec<u8>, frames: Vec<Vec<u8>>) -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut req = [0u8; 13];
            sock.read_exact(&mut req).unwrap();
            sock.write_all(&first).unwrap();
            for frame in frames {
                sock.write_all(&frame).unwrap();
            }
            std::thread::sleep(Duration::from_secs(2));
        });
        addr
    }

    fn ack() -> Vec<u8> {
        vec![WalMessageType::SubscribeAck as u8, 0, 0, 0, 0]
    }

    fn entry_frame(lsn: u64, op: u8, data: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&0u64.to_be_bytes());
        payload.push(op);
        payload.extend_from_slice(data);
        let mut frame = vec![WalMessageType::Entry as u8];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// A server that answers the subscription with anything but an ack is a
    /// start error.
    #[tokio::test]
    async fn test_start_without_subscription_ack_fails() {
        let addr = spawn_wal_server(vec![WalMessageType::Error as u8, 0, 0, 0, 0], vec![]);
        let invalidator = WalInvalidator::new(DistribCacheConfig::default());
        let err = invalidator.start(&addr.to_string()).await.unwrap_err();
        assert!(matches!(err, CacheError::ConnectionError(_)), "got {err:?}");
        assert!(!invalidator.is_running());
    }

    /// End to end: a WAL entry streamed after the ack reaches the
    /// invalidation callbacks through the processing task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_processing_task_applies_streamed_entries() {
        let mut key = Vec::new();
        key.extend_from_slice(&(9u32).to_be_bytes());
        key.extend_from_slice(b"users:123");
        let addr = spawn_wal_server(ack(), vec![entry_frame(7, 0x02, &key)]);

        let invalidator = Arc::new(WalInvalidator::new(DistribCacheConfig::default()));
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        invalidator.add_callback_sync(Arc::new(move |target| {
            seen_cb.lock().push(target.table.clone());
        }));
        invalidator.start(&addr.to_string()).await.unwrap();
        let task = invalidator.start_processing();

        for _ in 0..200 {
            if !seen.lock().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(seen.lock().as_slice(), ["users".to_string()]);
        assert_eq!(invalidator.stats().wal_entries_processed, 1);

        invalidator.stop().await;
        task.abort();
    }

    /// A malformed frame stops the invalidator instead of being skipped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_malformed_entry_stops_invalidator() {
        let addr = spawn_wal_server(ack(), vec![entry_frame(1, 0x7e, &[])]);
        let invalidator = Arc::new(WalInvalidator::new(DistribCacheConfig::default()));
        invalidator.start(&addr.to_string()).await.unwrap();
        assert!(invalidator.is_running());

        let task = invalidator.start_processing();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("processing loop must stop on a malformed frame")
            .unwrap();
        assert!(!invalidator.is_running());
        assert_eq!(invalidator.stats().wal_entries_processed, 0);
    }

    #[test]
    fn test_wal_entry_parsing() {
        // Test WalOperation variants
        let put = WalOperation::Put {
            key: b"users:1".to_vec(),
            value: b"data".to_vec(),
        };
        assert!(matches!(put, WalOperation::Put { .. }));

        let delete = WalOperation::Delete {
            key: b"users:1".to_vec(),
        };
        assert!(matches!(delete, WalOperation::Delete { .. }));

        let counter = WalOperation::UpdateCounter {
            table_name: "users".to_string(),
            counter: 100,
        };
        assert!(matches!(counter, WalOperation::UpdateCounter { .. }));

        let schema = WalOperation::SchemaChange {
            table_name: "users".to_string(),
        };
        assert!(matches!(schema, WalOperation::SchemaChange { .. }));

        let commit = WalOperation::Commit { txn_id: 12345 };
        assert!(matches!(commit, WalOperation::Commit { .. }));
    }

    #[tokio::test]
    async fn test_invalidation_stats() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        // Register some fingerprints (different query templates to avoid normalization collision)
        let fp1 = QueryFingerprint::from_query("SELECT * FROM users WHERE id = ?");
        let fp2 = QueryFingerprint::from_query("SELECT name FROM users WHERE email = ?");
        invalidator.register("users", fp1);
        invalidator.register("users", fp2);

        // Invalidate table
        invalidator.invalidate_table_manual("users", false).await;

        let stats = invalidator.stats();
        assert_eq!(stats.tables_invalidated, 1);
        assert_eq!(stats.entries_invalidated, 2);
    }
}
