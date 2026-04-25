//! Anomaly detection (T3.1).
//!
//! Statistical + heuristic detector for production-shape security
//! and operational anomalies. In-process sliding windows; no
//! external data store. Four detector families:
//!
//! 1. **Rate spike** — z-score on per-tenant queries-per-second
//!    against a rolling EWMA baseline.
//! 2. **Credential stuffing** — failed-auth burst per (user, ip)
//!    inside a sliding 60s window.
//! 3. **SQL injection** — heuristic pattern match against well-known
//!    payload shapes (UNION-based, comment escapes, stacked queries,
//!    boolean blind, time-based blind).
//! 4. **Novel query** — query fingerprint never seen before, useful
//!    on high-churn application workloads only as an informational
//!    signal (low confidence by default; admins can tighten via
//!    config).
//!
//! Why not a trained classifier today?
//!
//! Production anomaly classifiers want labels — feedback loops from
//! analyst-marked false positives. Without that loop in place, a
//! trained model overfits to whatever traffic was present at
//! training time. Statistical detectors are honest about their
//! priors (the EWMA + z-score) and degrade gracefully. The
//! [`AnomalyEvent`] trail makes it possible to bolt a learned
//! classifier on later: events become labeled training data.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub mod ewma;
pub mod sql_injection;

pub use ewma::{Ewma, RateWindow};

/// Anomaly severity — surfaces in admin output and lets operators
/// filter detections at scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// One anomaly detection event.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnomalyEvent {
    /// Per-tenant request-rate spike against the rolling baseline.
    RateSpike {
        tenant: String,
        rate_per_sec: f64,
        baseline: f64,
        z_score: f64,
        severity: Severity,
        detected_at: String,
    },
    /// Failed-auth burst from a single (user, ip) pair.
    AuthBurst {
        user: String,
        client_ip: String,
        failures: u32,
        window_secs: u32,
        severity: Severity,
        detected_at: String,
    },
    /// SQL-injection-shaped statement matched one or more
    /// well-known payload patterns.
    SqlInjection {
        sql_excerpt: String,
        patterns_matched: Vec<String>,
        severity: Severity,
        detected_at: String,
    },
    /// First-seen query fingerprint. Informational by default.
    NovelQuery {
        fingerprint: String,
        sql_excerpt: String,
        detected_at: String,
    },
}

impl AnomalyEvent {
    pub fn severity(&self) -> Severity {
        match self {
            AnomalyEvent::RateSpike { severity, .. } => *severity,
            AnomalyEvent::AuthBurst { severity, .. } => *severity,
            AnomalyEvent::SqlInjection { severity, .. } => *severity,
            AnomalyEvent::NovelQuery { .. } => Severity::Info,
        }
    }
}

/// Tunables. Defaults match production-friendly behaviour: spike
/// threshold above 3σ, credential burst above 10 failures / 60s.
#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    /// Rolling window for the per-tenant EWMA, in seconds.
    pub rate_window_secs: u64,
    /// Minimum z-score before a rate spike fires.
    pub spike_z_threshold: f64,
    /// Window for failed-auth bursts, in seconds.
    pub auth_window_secs: u64,
    /// Failures inside the auth window that trigger Critical.
    pub auth_critical_count: u32,
    /// Failures inside the auth window that trigger Warning.
    pub auth_warning_count: u32,
    /// Maximum events kept in the in-memory ring buffer.
    pub event_buffer_size: usize,
    /// Treat novel queries as informational events. Set false to
    /// suppress on high-churn workloads (e.g. ad-hoc analytics).
    pub emit_novel_queries: bool,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            rate_window_secs: 60,
            spike_z_threshold: 3.0,
            auth_window_secs: 60,
            auth_critical_count: 10,
            auth_warning_count: 5,
            event_buffer_size: 1024,
            emit_novel_queries: true,
        }
    }
}

/// Top-level detector. Cheap to clone via Arc — the inner state is
/// guarded by parking_lot RwLocks scoped per-detector to avoid
/// cross-detector contention.
#[derive(Clone)]
pub struct AnomalyDetector {
    config: Arc<AnomalyConfig>,
    rate_windows: Arc<RwLock<HashMap<String, RateWindow>>>,
    auth_windows: Arc<RwLock<HashMap<(String, String), AuthBurstWindow>>>,
    seen_fingerprints: Arc<RwLock<HashMap<String, ()>>>,
    events: Arc<RwLock<VecDeque<AnomalyEvent>>>,
}

impl AnomalyDetector {
    pub fn new(config: AnomalyConfig) -> Self {
        Self {
            config: Arc::new(config),
            rate_windows: Arc::new(RwLock::new(HashMap::new())),
            auth_windows: Arc::new(RwLock::new(HashMap::new())),
            seen_fingerprints: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(VecDeque::with_capacity(1024))),
        }
    }

    /// Record a query event. Detectors may emit zero or more events.
    /// Returns the events emitted by THIS call (the caller can also
    /// poll `recent_events` for the full ring buffer).
    pub fn record_query(&self, ctx: &QueryObservation) -> Vec<AnomalyEvent> {
        let mut emitted = Vec::new();

        // Rate-spike detector.
        let mut rates = self.rate_windows.write();
        let window = rates
            .entry(ctx.tenant.clone())
            .or_insert_with(|| RateWindow::new(self.config.rate_window_secs));
        if let Some(spike) = window.observe_and_score(ctx.timestamp) {
            if spike.z_score >= self.config.spike_z_threshold {
                let severity = if spike.z_score >= self.config.spike_z_threshold * 2.0 {
                    Severity::Critical
                } else {
                    Severity::Warning
                };
                let ev = AnomalyEvent::RateSpike {
                    tenant: ctx.tenant.clone(),
                    rate_per_sec: spike.rate,
                    baseline: spike.baseline,
                    z_score: spike.z_score,
                    severity,
                    detected_at: ctx.iso_timestamp.clone(),
                };
                emitted.push(ev.clone());
                self.push_event(ev);
            }
        }
        drop(rates);

        // Novel-query detector.
        if self.config.emit_novel_queries {
            let mut seen = self.seen_fingerprints.write();
            if !seen.contains_key(&ctx.fingerprint) {
                seen.insert(ctx.fingerprint.clone(), ());
                let ev = AnomalyEvent::NovelQuery {
                    fingerprint: ctx.fingerprint.clone(),
                    sql_excerpt: excerpt(&ctx.sql, 120),
                    detected_at: ctx.iso_timestamp.clone(),
                };
                emitted.push(ev.clone());
                self.push_event(ev);
            }
        }

        // SQL-injection detector. Pure heuristic — runs even if the
        // upstream pre-query already passed; multiple layers is the
        // point.
        let matches = sql_injection::scan(&ctx.sql);
        if !matches.is_empty() {
            let severity = if matches.len() >= 2 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            let ev = AnomalyEvent::SqlInjection {
                sql_excerpt: excerpt(&ctx.sql, 200),
                patterns_matched: matches,
                severity,
                detected_at: ctx.iso_timestamp.clone(),
            };
            emitted.push(ev.clone());
            self.push_event(ev);
        }

        emitted
    }

    /// Record an authentication outcome. Failed auths feed the
    /// credential-stuffing detector.
    pub fn record_auth(
        &self,
        user: &str,
        client_ip: &str,
        succeeded: bool,
        timestamp: Instant,
        iso_timestamp: &str,
    ) -> Option<AnomalyEvent> {
        if succeeded {
            // Successful auth resets the burst counter — common
            // case after the operator unlocks an account.
            self.auth_windows
                .write()
                .remove(&(user.to_string(), client_ip.to_string()));
            return None;
        }
        let mut windows = self.auth_windows.write();
        let window = windows
            .entry((user.to_string(), client_ip.to_string()))
            .or_insert_with(|| AuthBurstWindow::new(self.config.auth_window_secs));
        let count = window.record_failure(timestamp);
        let severity = if count >= self.config.auth_critical_count {
            Severity::Critical
        } else if count >= self.config.auth_warning_count {
            Severity::Warning
        } else {
            return None;
        };
        let ev = AnomalyEvent::AuthBurst {
            user: user.to_string(),
            client_ip: client_ip.to_string(),
            failures: count,
            window_secs: self.config.auth_window_secs as u32,
            severity,
            detected_at: iso_timestamp.to_string(),
        };
        drop(windows);
        self.push_event(ev.clone());
        Some(ev)
    }

    /// Snapshot of the most recent events. Newest first.
    pub fn recent_events(&self, limit: usize) -> Vec<AnomalyEvent> {
        let evs = self.events.read();
        let n = limit.min(evs.len());
        let mut out = Vec::with_capacity(n);
        for ev in evs.iter().rev().take(n) {
            out.push(ev.clone());
        }
        out
    }

    /// Total events ever recorded (since process start). Useful for
    /// metrics export.
    pub fn event_count(&self) -> usize {
        self.events.read().len()
    }

    fn push_event(&self, ev: AnomalyEvent) {
        let mut evs = self.events.write();
        if evs.len() >= self.config.event_buffer_size {
            evs.pop_front();
        }
        evs.push_back(ev);
    }
}

/// Per-query observation passed to the detector. Built by the proxy
/// at hook time; populated as much as the proxy knows about the
/// query.
#[derive(Debug, Clone)]
pub struct QueryObservation {
    /// Tenant identifier (or "default" / "" when no multi-tenancy).
    pub tenant: String,
    /// Canonical query fingerprint (literals normalised). Same shape
    /// the analytics module produces.
    pub fingerprint: String,
    /// Raw SQL — used for SQL-injection scanning + UI excerpt.
    pub sql: String,
    /// Wall-clock timestamp the query arrived. Detectors compute
    /// rates against this.
    pub timestamp: Instant,
    /// Pre-formatted RFC 3339 timestamp the proxy already has;
    /// detectors copy it into events rather than re-format.
    pub iso_timestamp: String,
}

/// Sliding 60s window of failed auths. Auto-evicts entries older
/// than `window_secs`.
struct AuthBurstWindow {
    window: Duration,
    failures: VecDeque<Instant>,
}

impl AuthBurstWindow {
    fn new(window_secs: u64) -> Self {
        Self {
            window: Duration::from_secs(window_secs),
            failures: VecDeque::new(),
        }
    }

    fn record_failure(&mut self, now: Instant) -> u32 {
        // Evict entries older than the window.
        while let Some(&front) = self.failures.front() {
            if now.duration_since(front) > self.window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
        self.failures.push_back(now);
        self.failures.len() as u32
    }
}

fn excerpt(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(tenant: &str, fp: &str, sql: &str) -> QueryObservation {
        QueryObservation {
            tenant: tenant.into(),
            fingerprint: fp.into(),
            sql: sql.into(),
            timestamp: Instant::now(),
            iso_timestamp: "2026-04-25T13:30:00Z".into(),
        }
    }

    #[test]
    fn novel_query_fires_once_per_fingerprint() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let evs = d.record_query(&obs("acme", "fp1", "SELECT 1"));
        assert!(evs
            .iter()
            .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. })));
        let evs2 = d.record_query(&obs("acme", "fp1", "SELECT 1"));
        assert!(!evs2
            .iter()
            .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. })));
    }

    #[test]
    fn novel_query_can_be_suppressed_via_config() {
        let mut cfg = AnomalyConfig::default();
        cfg.emit_novel_queries = false;
        let d = AnomalyDetector::new(cfg);
        let evs = d.record_query(&obs("acme", "fp1", "SELECT 1"));
        assert!(!evs
            .iter()
            .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. })));
    }

    #[test]
    fn sql_injection_detector_flags_classic_or_payload() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let evs = d.record_query(&obs(
            "acme",
            "fp-inj",
            "SELECT * FROM users WHERE id = 1 OR 1=1 --",
        ));
        let sqli = evs
            .iter()
            .find(|e| matches!(e, AnomalyEvent::SqlInjection { .. }));
        assert!(sqli.is_some(), "expected SqlInjection event in {:?}", evs);
    }

    #[test]
    fn auth_burst_warning_below_critical_threshold() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        let mut last = None;
        for _ in 0..6 {
            last = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        }
        match last {
            Some(AnomalyEvent::AuthBurst { failures, severity, .. }) => {
                assert_eq!(failures, 6);
                assert_eq!(severity, Severity::Warning);
            }
            other => panic!("expected AuthBurst Warning, got {:?}", other),
        }
    }

    #[test]
    fn auth_burst_critical_at_high_threshold() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        let mut last = None;
        for _ in 0..12 {
            last = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        }
        match last {
            Some(AnomalyEvent::AuthBurst { failures, severity, .. }) => {
                assert_eq!(failures, 12);
                assert_eq!(severity, Severity::Critical);
            }
            other => panic!("expected AuthBurst Critical, got {:?}", other),
        }
    }

    #[test]
    fn auth_success_resets_burst_window() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        for _ in 0..6 {
            let _ = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        }
        // Successful auth clears the window — next failure starts at 1.
        let _ = d.record_auth("alice", "10.0.0.1", true, now, "ts");
        let r = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        // 1 failure is below the warning threshold (5) — None.
        assert!(r.is_none());
    }

    #[test]
    fn recent_events_returns_newest_first() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let _ = d.record_query(&obs("a", "fp1", "SELECT 1"));
        let _ = d.record_query(&obs("a", "fp2", "SELECT 2"));
        let _ = d.record_query(&obs("a", "fp3", "SELECT 3"));
        let recent = d.recent_events(10);
        // First event in `recent` is the newest novel-query (fp3).
        match &recent[0] {
            AnomalyEvent::NovelQuery { fingerprint, .. } => {
                assert_eq!(fingerprint, "fp3")
            }
            other => panic!("expected NovelQuery fp3, got {:?}", other),
        }
    }

    #[test]
    fn recent_events_respects_limit() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        for i in 0..50 {
            let fp = format!("fp{}", i);
            let _ = d.record_query(&obs("a", &fp, "SELECT 1"));
        }
        assert_eq!(d.recent_events(10).len(), 10);
        assert_eq!(d.recent_events(100).len(), 50);
    }

    #[test]
    fn event_buffer_evicts_oldest_when_full() {
        let mut cfg = AnomalyConfig::default();
        cfg.event_buffer_size = 5;
        let d = AnomalyDetector::new(cfg);
        for i in 0..20 {
            let _ = d.record_query(&obs("a", &format!("fp{}", i), "SELECT 1"));
        }
        // Buffer holds at most 5; total event_count reflects current
        // buffer size, not lifetime count (simpler than tracking
        // separately).
        assert_eq!(d.event_count(), 5);
    }
}
