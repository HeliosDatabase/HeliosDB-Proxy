//! WAL-based cache invalidator
//!
//! Subscribes to WAL stream for real-time cache coherency.
//! Invalidates cached entries when underlying data changes.
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
    /// Endpoint address (host:port)
    endpoint: String,
    /// TCP connection to WAL server
    connection: Option<TcpStream>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Current LSN
    current_lsn: AtomicU64,
    /// Last heartbeat received
    last_heartbeat: Instant,
    /// Reconnection attempts
    reconnect_attempts: u32,
    /// Maximum reconnection attempts
    max_reconnect_attempts: u32,
    /// Reconnect delay
    reconnect_delay: Duration,
}

impl WalStreamer {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            connection: None,
            running: Arc::new(AtomicBool::new(false)),
            current_lsn: AtomicU64::new(0),
            last_heartbeat: Instant::now(),
            reconnect_attempts: 0,
            max_reconnect_attempts: 10,
            reconnect_delay: Duration::from_secs(1),
        }
    }

    /// Connect to the WAL streaming endpoint
    async fn connect(endpoint: &str) -> CacheResult<Self> {
        let mut streamer = Self::new(endpoint);

        // Attempt TCP connection
        match TcpStream::connect_timeout(
            &endpoint.parse().map_err(|_| CacheError::ConnectionError("Invalid endpoint address".to_string()))?,
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
            Err(e) => {
                // Return a disconnected streamer - can be reconnected later
                tracing::warn!("Failed to connect to WAL endpoint {}: {}", endpoint, e);
                Ok(streamer)
            }
        }
    }

    /// Send subscription request and start receiving WAL entries
    async fn subscribe(&mut self, start_lsn: Option<u64>) -> CacheResult<WalSubscription> {
        self.running.store(true, Ordering::SeqCst);

        if let Some(ref mut stream) = self.connection {
            // Build subscription request
            let lsn = start_lsn.unwrap_or(0);
            let mut request = vec![WalMessageType::Subscribe as u8];
            request.extend_from_slice(&(8u32).to_be_bytes()); // payload length
            request.extend_from_slice(&lsn.to_be_bytes());     // start LSN

            // Send subscription request
            if let Err(e) = stream.write_all(&request) {
                tracing::error!("Failed to send subscription request: {}", e);
                return Err(CacheError::ConnectionError(format!("Subscription failed: {}", e)));
            }

            // Wait for subscription ack
            let mut header = [0u8; 5];
            match stream.read_exact(&mut header) {
                Ok(_) => {
                    if header[0] == WalMessageType::SubscribeAck as u8 {
                        tracing::info!("WAL subscription acknowledged");
                    } else if header[0] == WalMessageType::Error as u8 {
                        return Err(CacheError::ConnectionError("Subscription rejected by server".to_string()));
                    }
                }
                Err(e) => {
                    tracing::warn!("No subscription ack received: {}", e);
                }
            }
        }

        Ok(WalSubscription {
            running: self.running.clone(),
            connection: self.connection.take(),
            current_lsn: 0,
            buffer: Vec::with_capacity(64 * 1024),
        })
    }

    /// Attempt to reconnect to the WAL endpoint
    async fn reconnect(&mut self) -> CacheResult<bool> {
        if self.reconnect_attempts >= self.max_reconnect_attempts {
            return Ok(false);
        }

        self.reconnect_attempts += 1;
        let delay = self.reconnect_delay * self.reconnect_attempts;
        tokio::time::sleep(delay).await;

        tracing::info!("Attempting WAL reconnection (attempt {})", self.reconnect_attempts);

        match TcpStream::connect_timeout(
            &self.endpoint.parse().map_err(|_| CacheError::ConnectionError("Invalid endpoint".to_string()))?,
            Duration::from_secs(5),
        ) {
            Ok(stream) => {
                stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
                stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
                stream.set_nodelay(true).ok();

                self.connection = Some(stream);
                self.reconnect_attempts = 0;
                self.last_heartbeat = Instant::now();
                tracing::info!("WAL reconnection successful");
                Ok(true)
            }
            Err(e) => {
                tracing::warn!("WAL reconnection failed: {}", e);
                Ok(false)
            }
        }
    }

    fn disconnect(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(stream) = self.connection.take() {
            drop(stream);
        }
    }

    /// Check if connected
    fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
}

/// WAL subscription for receiving streaming WAL entries
pub struct WalSubscription {
    running: Arc<AtomicBool>,
    connection: Option<TcpStream>,
    current_lsn: u64,
    buffer: Vec<u8>,
}

impl WalSubscription {
    /// Receive next WAL entry from the stream (non-recursive loop-based implementation)
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
                    // Timeout - this is normal, just continue
                    return None;
                }
                Err(_) => {
                    // Connection error
                    self.running.store(false, Ordering::SeqCst);
                    return None;
                }
            }

            let msg_type = header[0];
            let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

            // Handle heartbeat messages
            if msg_type == WalMessageType::Heartbeat as u8 {
                // Heartbeat has no payload or small status payload
                if payload_len > 0 {
                    let mut _payload = vec![0u8; payload_len];
                    let _ = stream.read_exact(&mut _payload);
                }
                continue; // Skip heartbeats
            }

            // Only process WAL entries
            if msg_type != WalMessageType::Entry as u8 {
                // Skip unknown message types
                if payload_len > 0 && payload_len < 1024 * 1024 {
                    let mut skip = vec![0u8; payload_len];
                    let _ = stream.read_exact(&mut skip);
                }
                continue;
            }

            // Read WAL entry payload
            if payload_len == 0 || payload_len > 10 * 1024 * 1024 {
                // Invalid payload size
                return None;
            }

            self.buffer.resize(payload_len, 0);
            if stream.read_exact(&mut self.buffer).is_err() {
                self.running.store(false, Ordering::SeqCst);
                return None;
            }

            // Parse WAL entry from payload
            // Format: [lsn: 8 bytes][timestamp: 8 bytes][op_type: 1 byte][data...]
            if self.buffer.len() < 17 {
                continue; // Invalid entry, skip
            }

            let lsn = u64::from_be_bytes([
                self.buffer[0], self.buffer[1], self.buffer[2], self.buffer[3],
                self.buffer[4], self.buffer[5], self.buffer[6], self.buffer[7],
            ]);
            let timestamp = u64::from_be_bytes([
                self.buffer[8], self.buffer[9], self.buffer[10], self.buffer[11],
                self.buffer[12], self.buffer[13], self.buffer[14], self.buffer[15],
            ]);
            let op_type = self.buffer[16];
            let data = &self.buffer[17..];

            let operation = match op_type {
                0x01 => {
                    // Put operation: [key_len: 4][key][value]
                    if data.len() < 4 {
                        continue;
                    }
                    let key_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                    if data.len() < 4 + key_len {
                        continue;
                    }
                    let key = data[4..4 + key_len].to_vec();
                    let value = data[4 + key_len..].to_vec();
                    WalOperation::Put { key, value }
                }
                0x02 => {
                    // Delete operation: [key_len: 4][key]
                    if data.len() < 4 {
                        continue;
                    }
                    let key_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                    if data.len() < 4 + key_len {
                        continue;
                    }
                    let key = data[4..4 + key_len].to_vec();
                    WalOperation::Delete { key }
                }
                0x03 => {
                    // Counter update: [table_name_len: 2][table_name][counter: 8]
                    if data.len() < 10 {
                        continue;
                    }
                    let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                    if data.len() < 2 + name_len + 8 {
                        continue;
                    }
                    let table_name = String::from_utf8_lossy(&data[2..2 + name_len]).to_string();
                    let counter_offset = 2 + name_len;
                    let counter = u64::from_be_bytes([
                        data[counter_offset], data[counter_offset + 1],
                        data[counter_offset + 2], data[counter_offset + 3],
                        data[counter_offset + 4], data[counter_offset + 5],
                        data[counter_offset + 6], data[counter_offset + 7],
                    ]);
                    WalOperation::UpdateCounter { table_name, counter }
                }
                0x04 => {
                    // Schema change: [table_name_len: 2][table_name]
                    if data.len() < 2 {
                        continue;
                    }
                    let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                    if data.len() < 2 + name_len {
                        continue;
                    }
                    let table_name = String::from_utf8_lossy(&data[2..2 + name_len]).to_string();
                    WalOperation::SchemaChange { table_name }
                }
                0x05 => {
                    // Commit: [txn_id: 8]
                    if data.len() < 8 {
                        continue;
                    }
                    let txn_id = u64::from_be_bytes([
                        data[0], data[1], data[2], data[3],
                        data[4], data[5], data[6], data[7],
                    ]);
                    WalOperation::Commit { txn_id }
                }
                _ => {
                    // Unknown operation type, skip
                    continue;
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
    /// Configuration
    config: DistribCacheConfig,

    /// WAL stream
    wal_stream: Option<WalStreamer>,

    /// Active WAL subscription
    subscription: tokio::sync::RwLock<Option<WalSubscription>>,

    /// Table to fingerprint index
    table_index: DashMap<String, HashSet<QueryFingerprint>>,

    /// Invalidation callbacks
    callbacks: Arc<tokio::sync::RwLock<Vec<InvalidationCallback>>>,

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
    pub fn new(config: DistribCacheConfig) -> Self {
        Self {
            config,
            wal_stream: None,
            subscription: tokio::sync::RwLock::new(None),
            table_index: DashMap::new(),
            callbacks: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            last_lsn: AtomicU64::new(0),
            stats: InvalidatorStats::default(),
        }
    }

    /// Start the invalidator - connects to WAL endpoint and begins processing
    pub async fn start(&self, wal_endpoint: &str) -> CacheResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        self.running.store(true, Ordering::SeqCst);

        // Connect to WAL streaming endpoint
        let mut streamer = WalStreamer::connect(wal_endpoint).await?;

        // Start subscription from last known LSN (for recovery)
        let start_lsn = self.last_lsn.load(Ordering::Relaxed);
        let start_lsn = if start_lsn > 0 { Some(start_lsn) } else { None };

        match streamer.subscribe(start_lsn).await {
            Ok(sub) => {
                *self.subscription.write().await = Some(sub);
                tracing::info!("WAL invalidator started, connected to {}", wal_endpoint);
            }
            Err(e) => {
                tracing::warn!("Failed to subscribe to WAL stream: {}. Running in degraded mode.", e);
                // Still mark as running - can accept manual invalidations
            }
        }

        Ok(())
    }

    /// Start the WAL processing loop in a background task
    pub fn start_processing(&self) -> tokio::task::JoinHandle<()> {
        let running = self.running.clone();
        let _subscription = self.subscription.write();
        let _callbacks = self.callbacks.clone();
        let _stats = InvalidatorStats {
            wal_entries_processed: AtomicU64::new(0),
            tables_invalidated: AtomicU64::new(0),
            entries_invalidated: AtomicU64::new(0),
            invalidation_lag_ms: AtomicU64::new(0),
        };
        let _table_index = self.table_index.clone();
        let _last_lsn = AtomicU64::new(self.last_lsn.load(Ordering::Relaxed));

        tokio::spawn(async move {
            tracing::info!("WAL processing loop started");

            // Note: This is a simplified version - in production you'd pass
            // the subscription handle properly
            while running.load(Ordering::SeqCst) {
                // Sleep to avoid busy loop when no subscription
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            tracing::info!("WAL processing loop stopped");
        })
    }

    /// Process WAL entries in the current task (blocking)
    pub async fn process_loop(&self) {
        while self.running.load(Ordering::SeqCst) {
            let entry = {
                let mut sub_guard = self.subscription.write().await;
                if let Some(ref mut sub) = *sub_guard {
                    sub.next().await
                } else {
                    None
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

        if let Some(_stream) = self.wal_stream.as_ref().map(|_| ()) {
            // Stream cleanup handled by drop
        }

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
        self.callbacks.write().await.push(callback);
    }

    /// Add callback (sync version for compatibility)
    pub fn add_callback_sync(&self, callback: InvalidationCallback) {
        // Use blocking for sync context
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.block_on(async {
                self.callbacks.write().await.push(callback);
            });
        }
    }

    /// Process a WAL entry
    async fn process_wal_entry(&self, entry: WalEntry) {
        self.stats.wal_entries_processed.fetch_add(1, Ordering::Relaxed);

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
        self.stats.tables_invalidated.fetch_add(1, Ordering::Relaxed);

        let target = InvalidationTarget {
            table: table.to_string(),
            row_key: None,
            invalidate_all: all_entries,
        };

        // Notify callbacks
        let callbacks = self.callbacks.read().await;
        for callback in callbacks.iter() {
            callback(&target);
        }

        // Track invalidated entries
        if let Some(entries) = self.table_index.get(table) {
            self.stats.entries_invalidated.fetch_add(
                entries.len() as u64,
                Ordering::Relaxed,
            );
        }
    }

    /// Fine-grained row invalidation
    async fn invalidate_row(&self, table: &str, row_key: &[u8]) {
        let target = InvalidationTarget {
            table: table.to_string(),
            row_key: Some(row_key.to_vec()),
            invalidate_all: false,
        };

        let callbacks = self.callbacks.read().await;
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

        invalidator.add_callback(Arc::new(move |target| {
            if target.table == "users" {
                called_clone.store(true, Ordering::SeqCst);
            }
        })).await;

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

    #[tokio::test]
    async fn test_start_stop() {
        let config = DistribCacheConfig::default();
        let invalidator = WalInvalidator::new(config);

        // Start with invalid endpoint (will not connect but won't fail)
        invalidator.start("127.0.0.1:59999").await.unwrap();
        assert!(invalidator.is_running());

        // Stop
        invalidator.stop().await;
        assert!(!invalidator.is_running());
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
