//! Read-Your-Writes (RYW) Consistency Tracker
//!
//! Tracks write LSNs per session to ensure subsequent reads
//! can see the session's own writes.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;

/// Session ID type
pub type SessionId = String;

/// Session RYW data
#[derive(Debug)]
pub struct RywSession {
    /// Last write LSN for this session
    pub last_write_lsn: AtomicU64,

    /// When the LSN was recorded
    pub written_at: Instant,

    /// Number of writes tracked
    pub write_count: AtomicU64,
}

impl RywSession {
    fn new(lsn: u64) -> Self {
        Self {
            last_write_lsn: AtomicU64::new(lsn),
            written_at: Instant::now(),
            write_count: AtomicU64::new(1),
        }
    }

    fn update(&self, lsn: u64) {
        // Only update if new LSN is higher
        let current = self.last_write_lsn.load(Ordering::Relaxed);
        if lsn > current {
            self.last_write_lsn.store(lsn, Ordering::Relaxed);
        }
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    fn is_expired(&self, retention: Duration) -> bool {
        self.written_at.elapsed() > retention
    }

    fn get_lsn(&self) -> u64 {
        self.last_write_lsn.load(Ordering::Relaxed)
    }
}

/// Read-Your-Writes Tracker
///
/// Tracks the last write LSN per session to ensure reads
/// are routed to replicas that have replayed past that point.
pub struct ReadYourWritesTracker {
    /// Session -> RYW data
    sessions: DashMap<SessionId, RywSession>,

    /// How long to retain LSN requirements
    retention: Duration,

    /// Last cleanup time
    last_cleanup: parking_lot::Mutex<Instant>,

    /// Cleanup interval
    cleanup_interval: Duration,
}

impl ReadYourWritesTracker {
    /// Create a new RYW tracker
    pub fn new(retention: Duration) -> Self {
        Self {
            sessions: DashMap::new(),
            retention,
            last_cleanup: parking_lot::Mutex::new(Instant::now()),
            cleanup_interval: Duration::from_secs(60),
        }
    }

    /// Create with default retention (5 minutes)
    pub fn with_defaults() -> Self {
        Self::new(Duration::from_secs(300))
    }

    /// Record that a session wrote at this LSN
    pub fn record_write(&self, session_id: &str, lsn: u64) {
        self.maybe_cleanup();

        self.sessions
            .entry(session_id.to_string())
            .and_modify(|session| session.update(lsn))
            .or_insert_with(|| RywSession::new(lsn));
    }

    /// Get the required LSN for read-your-writes
    ///
    /// Returns None if no writes recorded or requirement expired
    pub fn get_required_lsn(&self, session_id: &str) -> Option<u64> {
        self.sessions.get(session_id).and_then(|session| {
            if session.is_expired(self.retention) {
                None
            } else {
                Some(session.get_lsn())
            }
        })
    }

    /// Check if a session has a pending RYW requirement
    pub fn has_requirement(&self, session_id: &str) -> bool {
        self.get_required_lsn(session_id).is_some()
    }

    /// Clear LSN requirement (after successful read that satisfied it)
    pub fn clear(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    /// Clear all requirements for a session
    pub fn clear_session(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    /// Get number of tracked sessions
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get session info
    pub fn get_session_info(&self, session_id: &str) -> Option<(u64, Duration, u64)> {
        self.sessions.get(session_id).map(|session| {
            (
                session.get_lsn(),
                session.written_at.elapsed(),
                session.write_count.load(Ordering::Relaxed),
            )
        })
    }

    /// Perform cleanup of expired sessions (if due)
    fn maybe_cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.lock();
        if last_cleanup.elapsed() < self.cleanup_interval {
            return;
        }

        // Remove expired sessions
        self.sessions
            .retain(|_, session| !session.is_expired(self.retention));

        *last_cleanup = Instant::now();
    }

    /// Force cleanup now
    pub fn cleanup(&self) {
        self.sessions
            .retain(|_, session| !session.is_expired(self.retention));
        *self.last_cleanup.lock() = Instant::now();
    }

    /// Clear all tracked sessions
    pub fn clear_all(&self) {
        self.sessions.clear();
    }
}

impl std::fmt::Debug for ReadYourWritesTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadYourWritesTracker")
            .field("session_count", &self.sessions.len())
            .field("retention", &self.retention)
            .finish()
    }
}

/// Workflow Consistency Tracker
///
/// For multi-step workflows that need coordinated consistency
/// across multiple operations.
#[derive(Debug)]
pub struct WorkflowConsistency {
    /// Workflow ID
    workflow_id: String,

    /// LSN at workflow start
    start_lsn: u64,

    /// Current consistency point (highest write LSN)
    consistency_point: AtomicU64,

    /// When the workflow started
    started_at: Instant,

    /// Step counter
    step_count: AtomicU64,
}

impl WorkflowConsistency {
    /// Begin a new workflow
    pub fn begin(workflow_id: &str, current_lsn: u64) -> Self {
        Self {
            workflow_id: workflow_id.to_string(),
            start_lsn: current_lsn,
            consistency_point: AtomicU64::new(current_lsn),
            started_at: Instant::now(),
            step_count: AtomicU64::new(0),
        }
    }

    /// Record a write in this workflow
    pub fn record_write(&self, write_lsn: u64) {
        // Update consistency point to max of current and new LSN
        let current = self.consistency_point.load(Ordering::Relaxed);
        if write_lsn > current {
            self.consistency_point.store(write_lsn, Ordering::Relaxed);
        }
        self.step_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the required LSN for consistent reads in this workflow
    pub fn get_read_lsn_requirement(&self) -> u64 {
        self.consistency_point.load(Ordering::Relaxed)
    }

    /// Get workflow ID
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Get start LSN
    pub fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    /// Get workflow duration
    pub fn duration(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Get step count
    pub fn step_count(&self) -> u64 {
        self.step_count.load(Ordering::Relaxed)
    }

    /// Check if workflow has advanced (writes occurred)
    pub fn has_writes(&self) -> bool {
        self.consistency_point.load(Ordering::Relaxed) > self.start_lsn
    }
}

/// Multi-workflow tracker
pub struct WorkflowTracker {
    workflows: DashMap<String, WorkflowConsistency>,
    max_age: Duration,
}

impl WorkflowTracker {
    /// Create a new workflow tracker
    pub fn new(max_age: Duration) -> Self {
        Self {
            workflows: DashMap::new(),
            max_age,
        }
    }

    /// Begin a new workflow
    pub fn begin_workflow(&self, workflow_id: &str, current_lsn: u64) {
        self.workflows.insert(
            workflow_id.to_string(),
            WorkflowConsistency::begin(workflow_id, current_lsn),
        );
    }

    /// Record a write in a workflow
    pub fn record_write(&self, workflow_id: &str, write_lsn: u64) {
        if let Some(workflow) = self.workflows.get(workflow_id) {
            workflow.record_write(write_lsn);
        }
    }

    /// Get read LSN requirement for a workflow
    pub fn get_read_requirement(&self, workflow_id: &str) -> Option<u64> {
        self.workflows
            .get(workflow_id)
            .map(|w| w.get_read_lsn_requirement())
    }

    /// End a workflow
    pub fn end_workflow(&self, workflow_id: &str) {
        self.workflows.remove(workflow_id);
    }

    /// Cleanup expired workflows
    pub fn cleanup(&self) {
        self.workflows
            .retain(|_, workflow| workflow.duration() < self.max_age);
    }

    /// Get active workflow count
    pub fn workflow_count(&self) -> usize {
        self.workflows.len()
    }
}

impl Default for WorkflowTracker {
    fn default() -> Self {
        Self::new(Duration::from_secs(3600)) // 1 hour default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ryw_tracker_basic() {
        let tracker = ReadYourWritesTracker::with_defaults();

        // Record a write
        tracker.record_write("session-1", 1000);

        // Should have requirement
        assert!(tracker.has_requirement("session-1"));
        assert_eq!(tracker.get_required_lsn("session-1"), Some(1000));
    }

    #[test]
    fn test_ryw_tracker_updates() {
        let tracker = ReadYourWritesTracker::with_defaults();

        tracker.record_write("session-1", 1000);
        tracker.record_write("session-1", 1500);

        // Should use higher LSN
        assert_eq!(tracker.get_required_lsn("session-1"), Some(1500));

        // Lower LSN shouldn't update
        tracker.record_write("session-1", 1200);
        assert_eq!(tracker.get_required_lsn("session-1"), Some(1500));
    }

    #[test]
    fn test_ryw_tracker_clear() {
        let tracker = ReadYourWritesTracker::with_defaults();

        tracker.record_write("session-1", 1000);
        assert!(tracker.has_requirement("session-1"));

        tracker.clear("session-1");
        assert!(!tracker.has_requirement("session-1"));
        assert_eq!(tracker.get_required_lsn("session-1"), None);
    }

    #[test]
    fn test_ryw_tracker_multiple_sessions() {
        let tracker = ReadYourWritesTracker::with_defaults();

        tracker.record_write("session-1", 1000);
        tracker.record_write("session-2", 2000);
        tracker.record_write("session-3", 500);

        assert_eq!(tracker.session_count(), 3);
        assert_eq!(tracker.get_required_lsn("session-1"), Some(1000));
        assert_eq!(tracker.get_required_lsn("session-2"), Some(2000));
        assert_eq!(tracker.get_required_lsn("session-3"), Some(500));
    }

    #[test]
    fn test_ryw_session_expiry() {
        // Use very short retention for test
        let tracker = ReadYourWritesTracker::new(Duration::from_millis(50));

        tracker.record_write("session-1", 1000);
        assert!(tracker.has_requirement("session-1"));

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(100));

        // Should be expired
        assert!(!tracker.has_requirement("session-1"));
    }

    #[test]
    fn test_workflow_consistency_basic() {
        let workflow = WorkflowConsistency::begin("wf-1", 1000);

        assert_eq!(workflow.workflow_id(), "wf-1");
        assert_eq!(workflow.start_lsn(), 1000);
        assert_eq!(workflow.get_read_lsn_requirement(), 1000);
        assert!(!workflow.has_writes());
    }

    #[test]
    fn test_workflow_consistency_writes() {
        let workflow = WorkflowConsistency::begin("wf-1", 1000);

        workflow.record_write(1500);
        assert!(workflow.has_writes());
        assert_eq!(workflow.get_read_lsn_requirement(), 1500);

        workflow.record_write(2000);
        assert_eq!(workflow.get_read_lsn_requirement(), 2000);

        // Lower write shouldn't update
        workflow.record_write(1800);
        assert_eq!(workflow.get_read_lsn_requirement(), 2000);
    }

    #[test]
    fn test_workflow_tracker() {
        let tracker = WorkflowTracker::new(Duration::from_secs(60));

        tracker.begin_workflow("wf-1", 1000);
        tracker.begin_workflow("wf-2", 2000);

        assert_eq!(tracker.workflow_count(), 2);

        tracker.record_write("wf-1", 1500);
        assert_eq!(tracker.get_read_requirement("wf-1"), Some(1500));

        tracker.end_workflow("wf-1");
        assert_eq!(tracker.workflow_count(), 1);
        assert_eq!(tracker.get_read_requirement("wf-1"), None);
    }

    #[test]
    fn test_ryw_session_info() {
        let tracker = ReadYourWritesTracker::with_defaults();

        tracker.record_write("session-1", 1000);
        tracker.record_write("session-1", 1500);
        tracker.record_write("session-1", 2000);

        let (lsn, age, count) = tracker.get_session_info("session-1").unwrap();
        assert_eq!(lsn, 2000);
        assert!(age < Duration::from_secs(1));
        assert_eq!(count, 3);
    }
}
