//! Slow Query Log
//!
//! Track and persist slow queries for analysis.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;

use super::config::SlowQueryConfig;
use super::fingerprinter::QueryFingerprint;
use super::statistics::QueryExecution;

/// Slow query log entry
#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    /// Timestamp (nanos since epoch)
    pub timestamp_nanos: u64,

    /// Query duration
    pub duration: Duration,

    /// Query text (possibly truncated)
    pub query: String,

    /// Normalized/fingerprinted query
    pub fingerprint: String,

    /// Fingerprint hash
    pub fingerprint_hash: u64,

    /// User who executed the query
    pub user: String,

    /// Database name
    pub database: String,

    /// Client IP
    pub client_ip: String,

    /// Node that executed the query
    pub node: String,

    /// Rows returned/affected
    pub rows: usize,

    /// Error message (if query failed)
    pub error: Option<String>,

    /// Session ID
    pub session_id: Option<String>,

    /// Workflow ID (for agent tracing)
    pub workflow_id: Option<String>,
}

impl SlowQueryEntry {
    /// Create from execution record and fingerprint
    pub fn from_execution(
        execution: &QueryExecution,
        fingerprint: &QueryFingerprint,
        max_query_length: usize,
    ) -> Self {
        let query = if execution.query.len() > max_query_length {
            format!("{}...", &execution.query[..max_query_length])
        } else {
            execution.query.clone()
        };

        Self {
            timestamp_nanos: now_nanos(),
            duration: execution.duration,
            query,
            fingerprint: fingerprint.normalized.clone(),
            fingerprint_hash: fingerprint.hash,
            user: execution.user.clone(),
            database: execution.database.clone(),
            client_ip: execution.client_ip.clone(),
            node: execution.node.clone(),
            rows: execution.rows,
            error: execution.error.clone(),
            session_id: execution.session_id.clone(),
            workflow_id: execution.workflow_id.clone(),
        }
    }

    /// Format as log line
    pub fn format_log_line(&self) -> String {
        let timestamp = format_timestamp(self.timestamp_nanos);
        let duration_ms = self.duration.as_secs_f64() * 1000.0;
        let status = if self.error.is_some() { "ERROR" } else { "OK" };

        format!(
            "{} user={} db={} client={} node={} duration={:.3}ms rows={} status={} query={}",
            timestamp,
            self.user,
            self.database,
            self.client_ip,
            self.node,
            duration_ms,
            self.rows,
            status,
            self.query.replace('\n', " ")
        )
    }

    /// Parse from log line
    pub fn parse_log_line(line: &str) -> Option<Self> {
        // Basic parser for log lines
        // Format: TIMESTAMP user=X db=X client=X node=X duration=Xms rows=X status=X query=X

        let parts: Vec<&str> = line.splitn(9, ' ').collect();
        if parts.len() < 9 {
            return None;
        }

        let timestamp = parts[0];
        let timestamp_nanos = parse_timestamp(timestamp)?;

        let mut user = String::new();
        let mut db = String::new();
        let mut client = String::new();
        let mut node = String::new();
        let mut duration_ms = 0.0f64;
        let mut rows = 0usize;
        let mut status = "OK";
        let mut query = String::new();

        for part in &parts[1..] {
            if let Some(val) = part.strip_prefix("user=") {
                user = val.to_string();
            } else if let Some(val) = part.strip_prefix("db=") {
                db = val.to_string();
            } else if let Some(val) = part.strip_prefix("client=") {
                client = val.to_string();
            } else if let Some(val) = part.strip_prefix("node=") {
                node = val.to_string();
            } else if let Some(val) = part.strip_prefix("duration=") {
                if let Some(ms_str) = val.strip_suffix("ms") {
                    duration_ms = ms_str.parse().unwrap_or(0.0);
                }
            } else if let Some(val) = part.strip_prefix("rows=") {
                rows = val.parse().unwrap_or(0);
            } else if let Some(val) = part.strip_prefix("status=") {
                status = val;
            } else if let Some(val) = part.strip_prefix("query=") {
                query = val.to_string();
            }
        }

        let error = if status == "ERROR" {
            Some("Query failed".to_string())
        } else {
            None
        };

        Some(Self {
            timestamp_nanos,
            duration: Duration::from_secs_f64(duration_ms / 1000.0),
            query,
            fingerprint: String::new(),
            fingerprint_hash: 0,
            user,
            database: db,
            client_ip: client,
            node,
            rows,
            error,
            session_id: None,
            workflow_id: None,
        })
    }
}

/// Slow query log
pub struct SlowQueryLog {
    /// Configuration
    config: SlowQueryConfig,

    /// Recent entries (in-memory)
    recent: RwLock<VecDeque<SlowQueryEntry>>,

    /// Log file writer
    file_writer: RwLock<Option<File>>,

    /// Total logged count
    logged_count: AtomicU64,
}

impl SlowQueryLog {
    /// Create new slow query log
    pub fn new(config: SlowQueryConfig) -> Self {
        let file_writer = if let Some(ref path) = config.log_file {
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                Ok(file) => Some(file),
                Err(e) => {
                    eprintln!("Failed to open slow query log file {:?}: {}", path, e);
                    None
                }
            }
        } else {
            None
        };

        Self {
            config,
            recent: RwLock::new(VecDeque::new()),
            file_writer: RwLock::new(file_writer),
            logged_count: AtomicU64::new(0),
        }
    }

    /// Log query if it exceeds threshold
    pub fn log_if_slow(&self, execution: &QueryExecution, fingerprint: &QueryFingerprint) {
        if !self.config.enabled {
            return;
        }

        if execution.duration < self.config.threshold {
            return;
        }

        let entry = SlowQueryEntry::from_execution(
            execution,
            fingerprint,
            self.config.max_query_length,
        );

        self.log_entry(entry);
    }

    /// Log an entry directly
    pub fn log_entry(&self, entry: SlowQueryEntry) {
        self.logged_count.fetch_add(1, Ordering::Relaxed);

        // Add to recent entries
        {
            let mut recent = self.recent.write();
            recent.push_back(entry.clone());

            // Trim if exceeding max
            while recent.len() > self.config.max_recent_entries {
                recent.pop_front();
            }
        }

        // Write to file if configured
        if let Some(ref mut file) = *self.file_writer.write() {
            let line = entry.format_log_line();
            if let Err(e) = writeln!(file, "{}", line) {
                eprintln!("Failed to write slow query log: {}", e);
            }
        }
    }

    /// Get recent slow queries
    pub fn recent(&self, limit: usize) -> Vec<SlowQueryEntry> {
        let recent = self.recent.read();
        recent
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get all recent entries
    pub fn all_recent(&self) -> Vec<SlowQueryEntry> {
        self.recent.read().iter().cloned().collect()
    }

    /// Get count of logged slow queries
    pub fn count(&self) -> u64 {
        self.logged_count.load(Ordering::Relaxed)
    }

    /// Get threshold
    pub fn threshold(&self) -> Duration {
        self.config.threshold
    }

    /// Clear recent entries
    pub fn clear(&self) {
        self.recent.write().clear();
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Reader for slow query log files
pub struct SlowQueryReader {
    /// Path to log file
    path: PathBuf,
}

impl SlowQueryReader {
    /// Create reader for log file
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Read all entries from file
    pub fn read_all(&self) -> std::io::Result<Vec<SlowQueryEntry>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if let Some(entry) = SlowQueryEntry::parse_log_line(&line) {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    /// Read entries within time range
    pub fn read_range(
        &self,
        start_nanos: u64,
        end_nanos: u64,
    ) -> std::io::Result<Vec<SlowQueryEntry>> {
        let all = self.read_all()?;
        Ok(all
            .into_iter()
            .filter(|e| e.timestamp_nanos >= start_nanos && e.timestamp_nanos <= end_nanos)
            .collect())
    }

    /// Read entries slower than threshold
    pub fn read_slower_than(&self, threshold: Duration) -> std::io::Result<Vec<SlowQueryEntry>> {
        let all = self.read_all()?;
        Ok(all
            .into_iter()
            .filter(|e| e.duration > threshold)
            .collect())
    }

    /// Read last N entries
    pub fn read_last(&self, n: usize) -> std::io::Result<Vec<SlowQueryEntry>> {
        let all = self.read_all()?;
        Ok(all.into_iter().rev().take(n).collect())
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn format_timestamp(nanos: u64) -> String {
    // Simple ISO 8601-like format
    let secs = nanos / 1_000_000_000;
    let ms = (nanos % 1_000_000_000) / 1_000_000;

    // Use chrono if available, otherwise basic format
    format!("{}:{:03}", secs, ms)
}

fn parse_timestamp(s: &str) -> Option<u64> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() >= 2 {
        let secs: u64 = parts[0].parse().ok()?;
        let ms: u64 = parts[1].parse().ok()?;
        Some(secs * 1_000_000_000 + ms * 1_000_000)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slow_query_entry_format() {
        let entry = SlowQueryEntry {
            timestamp_nanos: 1704067200_000_000_000,
            duration: Duration::from_millis(1500),
            query: "SELECT * FROM users WHERE id = 1".to_string(),
            fingerprint: "select * from users where id = ?".to_string(),
            fingerprint_hash: 12345,
            user: "alice".to_string(),
            database: "mydb".to_string(),
            client_ip: "192.168.1.100".to_string(),
            node: "primary".to_string(),
            rows: 1,
            error: None,
            session_id: None,
            workflow_id: None,
        };

        let line = entry.format_log_line();
        assert!(line.contains("user=alice"));
        assert!(line.contains("db=mydb"));
        assert!(line.contains("duration=1500.000ms"));
        assert!(line.contains("status=OK"));
    }

    #[test]
    fn test_slow_query_log_enabled() {
        let config = SlowQueryConfig {
            enabled: true,
            threshold: Duration::from_millis(100),
            log_file: None,
            log_parameters: false,
            max_query_length: 1000,
            max_recent_entries: 10,
        };

        let log = SlowQueryLog::new(config);
        assert!(log.is_enabled());
        assert_eq!(log.threshold(), Duration::from_millis(100));
    }

    #[test]
    fn test_slow_query_log_threshold() {
        let config = SlowQueryConfig {
            enabled: true,
            threshold: Duration::from_millis(100),
            log_file: None,
            log_parameters: false,
            max_query_length: 1000,
            max_recent_entries: 10,
        };

        let log = SlowQueryLog::new(config);

        // Fast query - should not be logged
        let fast_exec = QueryExecution::new("SELECT 1", Duration::from_millis(50));
        let fingerprint = super::super::fingerprinter::QueryFingerprinter::new()
            .fingerprint("SELECT 1");
        log.log_if_slow(&fast_exec, &fingerprint);
        assert_eq!(log.count(), 0);

        // Slow query - should be logged
        let slow_exec = QueryExecution::new("SELECT * FROM users", Duration::from_millis(150));
        let fingerprint = super::super::fingerprinter::QueryFingerprinter::new()
            .fingerprint("SELECT * FROM users");
        log.log_if_slow(&slow_exec, &fingerprint);
        assert_eq!(log.count(), 1);
    }

    #[test]
    fn test_slow_query_log_recent() {
        let config = SlowQueryConfig {
            enabled: true,
            threshold: Duration::from_millis(100),
            log_file: None,
            log_parameters: false,
            max_query_length: 1000,
            max_recent_entries: 5,
        };

        let log = SlowQueryLog::new(config);
        let fp = super::super::fingerprinter::QueryFingerprinter::new();

        // Log 10 slow queries
        for i in 0..10 {
            let exec = QueryExecution::new(
                format!("SELECT * FROM table_{}", i),
                Duration::from_millis(150),
            );
            let fingerprint = fp.fingerprint(&exec.query);
            log.log_if_slow(&exec, &fingerprint);
        }

        // Should only keep last 5
        let recent = log.recent(10);
        assert_eq!(recent.len(), 5);
        assert!(recent[0].query.contains("table_9")); // Most recent first
    }

    #[test]
    fn test_slow_query_entry_parse() {
        let line = "1704067200:000 user=alice db=mydb client=127.0.0.1 node=primary duration=1500.000ms rows=1 status=OK query=SELECT 1";
        let entry = SlowQueryEntry::parse_log_line(line);

        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.user, "alice");
        assert_eq!(entry.database, "mydb");
        assert_eq!(entry.rows, 1);
    }

    #[test]
    fn test_query_truncation() {
        let config = SlowQueryConfig {
            enabled: true,
            threshold: Duration::from_millis(100),
            log_file: None,
            log_parameters: false,
            max_query_length: 20,
            max_recent_entries: 10,
        };

        let log = SlowQueryLog::new(config);
        let fp = super::super::fingerprinter::QueryFingerprinter::new();

        let long_query = "SELECT * FROM users WHERE name = 'this is a very long query'";
        let exec = QueryExecution::new(long_query, Duration::from_millis(150));
        let fingerprint = fp.fingerprint(long_query);
        log.log_if_slow(&exec, &fingerprint);

        let recent = log.recent(1);
        assert_eq!(recent.len(), 1);
        assert!(recent[0].query.len() <= 23); // 20 + "..."
        assert!(recent[0].query.ends_with("..."));
    }
}
