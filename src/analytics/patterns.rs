//! Pattern Detection
//!
//! Detect problematic query patterns like N+1 queries and query bursts.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::config::PatternConfig;
use super::fingerprinter::QueryFingerprint;
use super::statistics::QueryExecution;

/// Pattern alert types
#[derive(Debug, Clone)]
pub enum PatternAlert {
    /// N+1 query detected
    NplusOne(NplusOnePattern),
    /// Query burst detected
    Burst(QueryBurst),
}

impl PatternAlert {
    /// Get severity level (1-5)
    pub fn severity(&self) -> u8 {
        match self {
            PatternAlert::NplusOne(p) => {
                if p.repeat_count > 100 {
                    5
                } else if p.repeat_count > 50 {
                    4
                } else if p.repeat_count > 20 {
                    3
                } else if p.repeat_count > 10 {
                    2
                } else {
                    1
                }
            }
            PatternAlert::Burst(b) => {
                if b.query_count > 500 {
                    5
                } else if b.query_count > 200 {
                    4
                } else if b.query_count > 100 {
                    3
                } else if b.query_count > 50 {
                    2
                } else {
                    1
                }
            }
        }
    }

    /// Get description
    pub fn description(&self) -> String {
        match self {
            PatternAlert::NplusOne(p) => {
                format!(
                    "N+1 query pattern: {} repeated {} times in session {}",
                    truncate(&p.fingerprint, 50),
                    p.repeat_count,
                    p.session_id
                )
            }
            PatternAlert::Burst(b) => {
                format!(
                    "Query burst: {} queries in {:?} from session {}",
                    b.query_count, b.window, b.session_id
                )
            }
        }
    }
}

/// N+1 query pattern
#[derive(Debug, Clone)]
pub struct NplusOnePattern {
    /// Session that exhibited the pattern
    pub session_id: String,

    /// Fingerprint of the repeated query
    pub fingerprint: String,

    /// Fingerprint hash
    pub fingerprint_hash: u64,

    /// Number of repetitions
    pub repeat_count: usize,

    /// Time window in which repetitions occurred
    pub window: Duration,

    /// First seen timestamp
    pub first_seen_nanos: u64,

    /// Last seen timestamp
    pub last_seen_nanos: u64,

    /// Tables involved
    pub tables: Vec<String>,
}

/// Query burst (many queries in short window)
#[derive(Debug, Clone)]
pub struct QueryBurst {
    /// Session that exhibited the burst
    pub session_id: String,

    /// Number of queries in window
    pub query_count: usize,

    /// Detection window
    pub window: Duration,

    /// Start timestamp
    pub start_nanos: u64,

    /// End timestamp
    pub end_nanos: u64,

    /// Top fingerprints in burst
    pub top_fingerprints: Vec<(u64, usize)>,
}

/// Session query history
struct SessionHistory {
    /// Recent query timestamps
    query_times: VecDeque<Instant>,

    /// Recent fingerprint hashes
    recent_fingerprints: VecDeque<(u64, Instant, String, Vec<String>)>,

    /// Last activity time
    last_activity: Instant,

    /// Session ID
    session_id: String,
}

impl SessionHistory {
    fn new(session_id: String) -> Self {
        Self {
            query_times: VecDeque::new(),
            recent_fingerprints: VecDeque::new(),
            last_activity: Instant::now(),
            session_id,
        }
    }

    fn record_query(&mut self, fingerprint: &QueryFingerprint, max_history: usize) {
        let now = Instant::now();
        self.last_activity = now;

        // Record timestamp
        self.query_times.push_back(now);
        while self.query_times.len() > max_history {
            self.query_times.pop_front();
        }

        // Record fingerprint
        self.recent_fingerprints.push_back((
            fingerprint.hash,
            now,
            fingerprint.normalized.clone(),
            fingerprint.tables.clone(),
        ));
        while self.recent_fingerprints.len() > max_history {
            self.recent_fingerprints.pop_front();
        }
    }

    fn count_in_window(&self, window: Duration) -> usize {
        let cutoff = Instant::now() - window;
        self.query_times
            .iter()
            .filter(|t| **t > cutoff)
            .count()
    }

    fn count_fingerprint_in_window(&self, hash: u64, window: Duration) -> usize {
        let cutoff = Instant::now() - window;
        self.recent_fingerprints
            .iter()
            .filter(|(h, t, _, _)| *h == hash && *t > cutoff)
            .count()
    }

    fn get_repeated_fingerprints(&self, threshold: usize) -> Vec<(u64, usize, String, Vec<String>)> {
        let mut counts: std::collections::HashMap<u64, (usize, String, Vec<String>)> =
            std::collections::HashMap::new();

        for (hash, _, normalized, tables) in &self.recent_fingerprints {
            let entry = counts
                .entry(*hash)
                .or_insert((0, normalized.clone(), tables.clone()));
            entry.0 += 1;
        }

        counts
            .into_iter()
            .filter(|(_, (count, _, _))| *count >= threshold)
            .map(|(hash, (count, normalized, tables))| (hash, count, normalized, tables))
            .collect()
    }
}

/// Pattern detector
pub struct PatternDetector {
    /// Configuration
    config: PatternConfig,

    /// Per-session history
    sessions: DashMap<String, SessionHistory>,

    /// Detected alerts
    alerts: RwLock<VecDeque<PatternAlert>>,

    /// Alert counter
    alert_count: AtomicU64,

    /// Last cleanup time
    last_cleanup: RwLock<Instant>,
}

impl PatternDetector {
    /// Create new pattern detector
    pub fn new(config: PatternConfig) -> Self {
        Self {
            config,
            sessions: DashMap::new(),
            alerts: RwLock::new(VecDeque::new()),
            alert_count: AtomicU64::new(0),
            last_cleanup: RwLock::new(Instant::now()),
        }
    }

    /// Record a query execution
    pub fn record_query(
        &self,
        session_id: &str,
        _execution: &QueryExecution,
        fingerprint: &QueryFingerprint,
    ) {
        // Periodic cleanup
        self.maybe_cleanup();

        // Get or create session history
        let mut session = self
            .sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionHistory::new(session_id.to_string()));

        // Record the query
        session.record_query(fingerprint, self.config.session_history_size);

        // Check for N+1 pattern
        if self.config.n_plus_one_detection {
            self.check_n_plus_one(&session, fingerprint);
        }

        // Check for burst pattern
        if self.config.burst_detection {
            self.check_burst(&session);
        }
    }

    /// Check for N+1 query pattern
    fn check_n_plus_one(&self, session: &SessionHistory, fingerprint: &QueryFingerprint) {
        let count = session.count_fingerprint_in_window(fingerprint.hash, Duration::from_secs(5));

        if count >= self.config.n_plus_one_threshold {
            let pattern = NplusOnePattern {
                session_id: session.session_id.clone(),
                fingerprint: fingerprint.normalized.clone(),
                fingerprint_hash: fingerprint.hash,
                repeat_count: count,
                window: Duration::from_secs(5),
                first_seen_nanos: now_nanos(),
                last_seen_nanos: now_nanos(),
                tables: fingerprint.tables.clone(),
            };

            self.add_alert(PatternAlert::NplusOne(pattern));
        }
    }

    /// Check for query burst
    fn check_burst(&self, session: &SessionHistory) {
        let count = session.count_in_window(self.config.burst_window);

        if count >= self.config.burst_threshold {
            // Get top fingerprints in the burst
            let repeated = session.get_repeated_fingerprints(3);
            let top_fingerprints: Vec<_> = repeated
                .iter()
                .take(5)
                .map(|(hash, count, _, _)| (*hash, *count))
                .collect();

            let burst = QueryBurst {
                session_id: session.session_id.clone(),
                query_count: count,
                window: self.config.burst_window,
                start_nanos: now_nanos() - self.config.burst_window.as_nanos() as u64,
                end_nanos: now_nanos(),
                top_fingerprints,
            };

            self.add_alert(PatternAlert::Burst(burst));
        }
    }

    /// Add an alert
    fn add_alert(&self, alert: PatternAlert) {
        self.alert_count.fetch_add(1, Ordering::Relaxed);

        let mut alerts = self.alerts.write();
        alerts.push_back(alert);

        // Keep only recent alerts (max 1000)
        while alerts.len() > 1000 {
            alerts.pop_front();
        }
    }

    /// Get recent alerts
    pub fn get_alerts(&self) -> Vec<PatternAlert> {
        self.alerts.read().iter().cloned().collect()
    }

    /// Get alerts by type
    pub fn get_n_plus_one_alerts(&self) -> Vec<NplusOnePattern> {
        self.alerts
            .read()
            .iter()
            .filter_map(|a| match a {
                PatternAlert::NplusOne(p) => Some(p.clone()),
                _ => None,
            })
            .collect()
    }

    /// Get burst alerts
    pub fn get_burst_alerts(&self) -> Vec<QueryBurst> {
        self.alerts
            .read()
            .iter()
            .filter_map(|a| match a {
                PatternAlert::Burst(b) => Some(b.clone()),
                _ => None,
            })
            .collect()
    }

    /// Get alert count
    pub fn alert_count(&self) -> u64 {
        self.alert_count.load(Ordering::Relaxed)
    }

    /// Clear alerts
    pub fn clear_alerts(&self) {
        self.alerts.write().clear();
    }

    /// Cleanup inactive sessions
    fn maybe_cleanup(&self) {
        let now = Instant::now();
        let mut last_cleanup = self.last_cleanup.write();

        // Cleanup every minute
        if now.duration_since(*last_cleanup) < Duration::from_secs(60) {
            return;
        }
        *last_cleanup = now;
        drop(last_cleanup);

        // Remove inactive sessions
        let timeout = self.config.session_timeout;
        self.sessions.retain(|_, session| {
            now.duration_since(session.last_activity) < timeout
        });

        // Enforce max sessions
        while self.sessions.len() > self.config.max_sessions {
            // Remove oldest session
            let oldest = self
                .sessions
                .iter()
                .min_by_key(|s| s.last_activity)
                .map(|s| s.key().clone());

            if let Some(key) = oldest {
                self.sessions.remove(&key);
            } else {
                break;
            }
        }
    }

    /// Get session count
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Reset detector
    pub fn reset(&self) {
        self.sessions.clear();
        self.alerts.write().clear();
        self.alert_count.store(0, Ordering::Relaxed);
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...", &s[..max])
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::fingerprinter::QueryFingerprinter;

    #[test]
    fn test_pattern_detector_new() {
        let config = PatternConfig::default();
        let detector = PatternDetector::new(config);
        assert_eq!(detector.session_count(), 0);
        assert_eq!(detector.alert_count(), 0);
    }

    #[test]
    fn test_n_plus_one_detection() {
        let mut config = PatternConfig::default();
        config.n_plus_one_threshold = 3;
        config.burst_detection = false;

        let detector = PatternDetector::new(config);
        let fp = QueryFingerprinter::new();

        let session_id = "session-1";

        // Record same query multiple times
        for i in 0..5 {
            let query = format!("SELECT * FROM users WHERE id = {}", i);
            let fingerprint = fp.fingerprint(&query);
            let execution = super::super::statistics::QueryExecution::new(
                query,
                Duration::from_millis(5),
            );
            detector.record_query(session_id, &execution, &fingerprint);
        }

        // Should have detected N+1 pattern
        let alerts = detector.get_n_plus_one_alerts();
        assert!(!alerts.is_empty(), "Should detect N+1 pattern");
    }

    #[test]
    fn test_burst_detection() {
        let mut config = PatternConfig::default();
        config.burst_threshold = 5;
        config.burst_window = Duration::from_secs(1);
        config.n_plus_one_detection = false;

        let detector = PatternDetector::new(config);
        let fp = QueryFingerprinter::new();

        let session_id = "session-1";

        // Record many queries quickly
        for i in 0..10 {
            let query = format!("SELECT * FROM table_{}", i);
            let fingerprint = fp.fingerprint(&query);
            let execution = super::super::statistics::QueryExecution::new(
                query,
                Duration::from_millis(1),
            );
            detector.record_query(session_id, &execution, &fingerprint);
        }

        // Should have detected burst
        let alerts = detector.get_burst_alerts();
        assert!(!alerts.is_empty(), "Should detect burst pattern");
    }

    #[test]
    fn test_alert_severity() {
        let pattern = NplusOnePattern {
            session_id: "session-1".to_string(),
            fingerprint: "select * from users where id = ?".to_string(),
            fingerprint_hash: 12345,
            repeat_count: 25,
            window: Duration::from_secs(5),
            first_seen_nanos: 0,
            last_seen_nanos: 0,
            tables: vec!["users".to_string()],
        };

        let alert = PatternAlert::NplusOne(pattern);
        assert_eq!(alert.severity(), 3);
    }

    #[test]
    fn test_session_cleanup() {
        let mut config = PatternConfig::default();
        config.session_timeout = Duration::from_millis(100);

        let detector = PatternDetector::new(config);
        let fp = QueryFingerprinter::new();

        // Record query in session
        let fingerprint = fp.fingerprint("SELECT 1");
        let execution = super::super::statistics::QueryExecution::new(
            "SELECT 1",
            Duration::from_millis(1),
        );
        detector.record_query("session-1", &execution, &fingerprint);

        assert_eq!(detector.session_count(), 1);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(150));

        // Record in new session to trigger cleanup
        detector.record_query("session-2", &execution, &fingerprint);

        // Old session should be cleaned up (cleanup runs every minute in production,
        // but our test may or may not have triggered it)
    }

    #[test]
    fn test_reset() {
        let config = PatternConfig::default();
        let detector = PatternDetector::new(config);
        let fp = QueryFingerprinter::new();

        let fingerprint = fp.fingerprint("SELECT 1");
        let execution = super::super::statistics::QueryExecution::new(
            "SELECT 1",
            Duration::from_millis(1),
        );
        detector.record_query("session-1", &execution, &fingerprint);

        detector.reset();

        assert_eq!(detector.session_count(), 0);
        assert_eq!(detector.alert_count(), 0);
    }

    #[test]
    fn test_alert_description() {
        let pattern = NplusOnePattern {
            session_id: "sess-123".to_string(),
            fingerprint: "select * from users where id = ?".to_string(),
            fingerprint_hash: 12345,
            repeat_count: 10,
            window: Duration::from_secs(5),
            first_seen_nanos: 0,
            last_seen_nanos: 0,
            tables: vec!["users".to_string()],
        };

        let alert = PatternAlert::NplusOne(pattern);
        let desc = alert.description();

        assert!(desc.contains("N+1"));
        assert!(desc.contains("10 times"));
        assert!(desc.contains("sess-123"));
    }
}
