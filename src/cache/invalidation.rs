//! Cache Invalidation
//!
//! Manages cache invalidation through multiple strategies:
//! - WAL-based: Subscribe to WAL events for real-time invalidation
//! - TTL-based: Time-based expiration fallback
//! - Manual: Explicit invalidation via API

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::broadcast;

use super::config::InvalidationConfig;
use super::result::CacheKey;

/// Cache invalidation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InvalidationMode {
    /// WAL-based invalidation (real-time)
    #[default]
    Wal,

    /// TTL-based invalidation only
    TtlOnly,

    /// Manual invalidation only
    ManualOnly,

    /// Combined WAL + TTL fallback
    WalWithTtlFallback,
}

/// Cache invalidation manager
///
/// Tracks table -> cache key mappings and handles invalidation events.
#[derive(Debug)]
pub struct InvalidationManager {
    /// Configuration
    config: InvalidationConfig,

    /// Table -> cache keys mapping
    table_keys: DashMap<String, HashSet<CacheKey>>,

    /// Cache key -> tables mapping (reverse index)
    key_tables: DashMap<CacheKey, HashSet<String>>,

    /// Last invalidation time per table
    last_invalidation: DashMap<String, Instant>,

    /// Invalidation event sender
    event_tx: broadcast::Sender<InvalidationEvent>,

    /// WAL subscription status
    wal_connected: std::sync::atomic::AtomicBool,

    /// Pending invalidations (batched)
    pending_invalidations: RwLock<HashSet<String>>,

    /// Batch timer
    last_batch_flush: RwLock<Instant>,
}

/// Invalidation event
#[derive(Debug, Clone)]
pub enum InvalidationEvent {
    /// Invalidate specific tables
    Tables(Vec<String>),

    /// Invalidate specific cache keys
    Keys(Vec<CacheKey>),

    /// Invalidate all caches
    All,

    /// WAL event received
    WalEvent {
        table: String,
        operation: WalOperation,
        lsn: u64,
    },
}

/// WAL operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalOperation {
    Insert,
    Update,
    Delete,
    Truncate,
}

impl InvalidationManager {
    /// Create a new invalidation manager
    pub fn new(config: InvalidationConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1024);

        Self {
            config,
            table_keys: DashMap::new(),
            key_tables: DashMap::new(),
            last_invalidation: DashMap::new(),
            event_tx,
            wal_connected: std::sync::atomic::AtomicBool::new(false),
            pending_invalidations: RwLock::new(HashSet::new()),
            last_batch_flush: RwLock::new(Instant::now()),
        }
    }

    /// Register a cache key for table-based invalidation
    pub fn register(&self, key: &CacheKey, table: &str) {
        // Add to table -> keys mapping
        self.table_keys
            .entry(table.to_string())
            .or_insert_with(HashSet::new)
            .insert(key.clone());

        // Add to key -> tables mapping
        self.key_tables
            .entry(key.clone())
            .or_insert_with(HashSet::new)
            .insert(table.to_string());
    }

    /// Unregister a cache key
    pub fn unregister(&self, key: &CacheKey) {
        if let Some((_, tables)) = self.key_tables.remove(key) {
            for table in tables {
                if let Some(mut keys) = self.table_keys.get_mut(&table) {
                    keys.remove(key);
                }
            }
        }
    }

    /// Get all cache keys associated with a table
    pub fn get_keys_for_table(&self, table: &str) -> Vec<CacheKey> {
        self.table_keys
            .get(table)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all tables associated with a cache key
    pub fn get_tables_for_key(&self, key: &CacheKey) -> Vec<String> {
        self.key_tables
            .get(key)
            .map(|tables| tables.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Invalidate all cache entries for a table
    pub fn invalidate_table(&self, table: &str) {
        // Record invalidation time
        self.last_invalidation.insert(table.to_string(), Instant::now());

        // Send event
        let _ = self.event_tx.send(InvalidationEvent::Tables(vec![table.to_string()]));

        // Clear table -> keys mapping
        if let Some((_, keys)) = self.table_keys.remove(table) {
            for key in keys {
                if let Some(mut tables) = self.key_tables.get_mut(&key) {
                    tables.remove(table);
                }
            }
        }
    }

    /// Invalidate multiple tables
    pub fn invalidate_tables(&self, tables: &[String]) {
        for table in tables {
            self.invalidate_table(table);
        }
    }

    /// Queue a table for batched invalidation
    pub fn queue_invalidation(&self, table: &str) {
        if let Ok(mut pending) = self.pending_invalidations.write() {
            pending.insert(table.to_string());
        }

        // Check if we should flush the batch
        self.maybe_flush_batch();
    }

    /// Flush pending invalidations
    pub fn flush_pending(&self) {
        let tables: Vec<String> = {
            let mut pending = match self.pending_invalidations.write() {
                Ok(p) => p,
                Err(_) => return,
            };

            let tables: Vec<String> = pending.drain().collect();
            tables
        };

        if !tables.is_empty() {
            self.invalidate_tables(&tables);
        }

        if let Ok(mut last) = self.last_batch_flush.write() {
            *last = Instant::now();
        }
    }

    /// Check if batch should be flushed
    fn maybe_flush_batch(&self) {
        let should_flush = {
            let last = match self.last_batch_flush.read() {
                Ok(l) => *l,
                Err(_) => return,
            };

            let pending_count = self.pending_invalidations
                .read()
                .map(|p| p.len())
                .unwrap_or(0);

            // Flush if batch is large or time threshold exceeded
            pending_count >= 100 || last.elapsed() > Duration::from_millis(50)
        };

        if should_flush {
            self.flush_pending();
        }
    }

    /// Handle a WAL event
    pub fn on_wal_event(&self, table: &str, operation: WalOperation, lsn: u64) {
        // Send WAL event
        let _ = self.event_tx.send(InvalidationEvent::WalEvent {
            table: table.to_string(),
            operation,
            lsn,
        });

        // Queue for batched invalidation
        self.queue_invalidation(table);
    }

    /// Subscribe to invalidation events
    pub fn subscribe(&self) -> broadcast::Receiver<InvalidationEvent> {
        self.event_tx.subscribe()
    }

    /// Check if WAL is connected
    pub fn is_wal_connected(&self) -> bool {
        self.wal_connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set WAL connection status
    pub fn set_wal_connected(&self, connected: bool) {
        self.wal_connected.store(connected, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get invalidation mode
    pub fn mode(&self) -> InvalidationMode {
        self.config.mode
    }

    /// Get last invalidation time for a table
    pub fn last_invalidation_time(&self, table: &str) -> Option<Instant> {
        self.last_invalidation.get(table).map(|t| *t)
    }

    /// Get statistics
    pub fn stats(&self) -> InvalidationStats {
        let total_keys: usize = self.table_keys
            .iter()
            .map(|e| e.value().len())
            .sum();

        InvalidationStats {
            tracked_tables: self.table_keys.len(),
            tracked_keys: total_keys,
            pending_invalidations: self.pending_invalidations
                .read()
                .map(|p| p.len())
                .unwrap_or(0),
            wal_connected: self.is_wal_connected(),
            mode: self.config.mode,
        }
    }

    /// Clear all tracking data
    pub fn clear(&self) {
        self.table_keys.clear();
        self.key_tables.clear();
        self.last_invalidation.clear();

        if let Ok(mut pending) = self.pending_invalidations.write() {
            pending.clear();
        }
    }
}

/// Invalidation statistics
#[derive(Debug, Clone)]
pub struct InvalidationStats {
    /// Number of tracked tables
    pub tracked_tables: usize,

    /// Number of tracked cache keys
    pub tracked_keys: usize,

    /// Number of pending invalidations
    pub pending_invalidations: usize,

    /// Whether WAL is connected
    pub wal_connected: bool,

    /// Current invalidation mode
    pub mode: InvalidationMode,
}

/// WAL event parser
pub struct WalEventParser;

impl WalEventParser {
    /// Parse a WAL message into an invalidation event
    pub fn parse(message: &[u8]) -> Option<(String, WalOperation, u64)> {
        // Simple format: "OP:TABLE:LSN"
        let text = std::str::from_utf8(message).ok()?;
        let parts: Vec<&str> = text.split(':').collect();

        if parts.len() < 3 {
            return None;
        }

        let operation = match parts[0].to_uppercase().as_str() {
            "I" | "INSERT" => WalOperation::Insert,
            "U" | "UPDATE" => WalOperation::Update,
            "D" | "DELETE" => WalOperation::Delete,
            "T" | "TRUNCATE" => WalOperation::Truncate,
            _ => return None,
        };

        let table = parts[1].to_string();
        let lsn = parts[2].parse().ok()?;

        Some((table, operation, lsn))
    }

    /// Extract affected tables from SQL
    pub fn extract_affected_tables(sql: &str) -> Vec<String> {
        let sql_upper = sql.to_uppercase();
        let mut tables = Vec::new();

        // Simple extraction (more sophisticated parsing would use sqlparser)
        let patterns = [
            (r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)", 1),
            (r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)", 1),
            (r"DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)", 1),
            (r"TRUNCATE\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)", 1),
        ];

        for (pattern, group) in patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                for cap in re.captures_iter(&sql_upper) {
                    if let Some(m) = cap.get(group) {
                        tables.push(m.as_str().to_lowercase());
                    }
                }
            }
        }

        tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_key(hash: u64) -> CacheKey {
        CacheKey::from_parts(hash, "test".to_string(), None, None)
    }

    #[test]
    fn test_register_and_lookup() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        let key1 = create_key(111);
        let key2 = create_key(222);

        manager.register(&key1, "users");
        manager.register(&key2, "users");
        manager.register(&key1, "sessions");

        // Check table -> keys
        let user_keys = manager.get_keys_for_table("users");
        assert_eq!(user_keys.len(), 2);
        assert!(user_keys.contains(&key1));
        assert!(user_keys.contains(&key2));

        // Check key -> tables
        let key1_tables = manager.get_tables_for_key(&key1);
        assert_eq!(key1_tables.len(), 2);
        assert!(key1_tables.contains(&"users".to_string()));
        assert!(key1_tables.contains(&"sessions".to_string()));
    }

    #[test]
    fn test_unregister() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        let key = create_key(111);
        manager.register(&key, "users");
        manager.register(&key, "sessions");

        manager.unregister(&key);

        assert!(manager.get_keys_for_table("users").is_empty());
        assert!(manager.get_keys_for_table("sessions").is_empty());
        assert!(manager.get_tables_for_key(&key).is_empty());
    }

    #[test]
    fn test_invalidate_table() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        let key1 = create_key(111);
        let key2 = create_key(222);

        manager.register(&key1, "users");
        manager.register(&key2, "orders");

        manager.invalidate_table("users");

        assert!(manager.get_keys_for_table("users").is_empty());
        assert!(!manager.get_keys_for_table("orders").is_empty());
        assert!(manager.last_invalidation_time("users").is_some());
    }

    #[test]
    fn test_queue_and_flush() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        let key = create_key(111);
        manager.register(&key, "users");

        manager.queue_invalidation("users");

        {
            let pending = manager.pending_invalidations.read().unwrap();
            assert!(pending.contains("users"));
        }

        manager.flush_pending();

        {
            let pending = manager.pending_invalidations.read().unwrap();
            assert!(pending.is_empty());
        }

        assert!(manager.get_keys_for_table("users").is_empty());
    }

    #[test]
    fn test_wal_event() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        let key = create_key(111);
        manager.register(&key, "users");

        let mut receiver = manager.subscribe();

        manager.on_wal_event("users", WalOperation::Update, 12345);

        // Flush to process the event
        manager.flush_pending();

        // Should have received the WAL event
        let event = receiver.try_recv();
        assert!(event.is_ok());
    }

    #[test]
    fn test_stats() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        let key1 = create_key(111);
        let key2 = create_key(222);

        manager.register(&key1, "users");
        manager.register(&key2, "orders");

        let stats = manager.stats();
        assert_eq!(stats.tracked_tables, 2);
        assert_eq!(stats.tracked_keys, 2);
    }

    #[test]
    fn test_wal_event_parser() {
        // Test message parsing
        let (table, op, lsn) = WalEventParser::parse(b"INSERT:users:12345").unwrap();
        assert_eq!(table, "users");
        assert_eq!(op, WalOperation::Insert);
        assert_eq!(lsn, 12345);

        let (table, op, _) = WalEventParser::parse(b"U:orders:67890").unwrap();
        assert_eq!(table, "orders");
        assert_eq!(op, WalOperation::Update);
    }

    #[test]
    fn test_extract_affected_tables() {
        let tests = vec![
            ("INSERT INTO users VALUES (1)", vec!["users"]),
            ("UPDATE orders SET status = 'done'", vec!["orders"]),
            ("DELETE FROM sessions WHERE expired", vec!["sessions"]),
            ("TRUNCATE TABLE logs", vec!["logs"]),
            ("TRUNCATE products", vec!["products"]),
        ];

        for (sql, expected) in tests {
            let tables = WalEventParser::extract_affected_tables(sql);
            assert_eq!(tables, expected, "Failed for SQL: {}", sql);
        }
    }

    #[test]
    fn test_clear() {
        let config = InvalidationConfig::default();
        let manager = InvalidationManager::new(config);

        manager.register(&create_key(111), "users");
        manager.queue_invalidation("users");

        manager.clear();

        assert_eq!(manager.stats().tracked_tables, 0);
        assert_eq!(manager.stats().tracked_keys, 0);
        assert_eq!(manager.stats().pending_invalidations, 0);
    }
}
