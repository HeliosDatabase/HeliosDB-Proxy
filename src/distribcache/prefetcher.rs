//! Predictive prefetcher for intelligent cache warming
//!
//! Uses query sequence patterns and temporal patterns to predict
//! and pre-warm cache with likely future queries.

use chrono::{Datelike, Timelike};
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use super::{DistribCacheConfig, QueryFingerprint, SessionId};

/// Prefetch request
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    /// Query fingerprint to prefetch
    pub fingerprint: QueryFingerprint,
    /// Priority (0-100)
    pub priority: u32,
}

/// Prefetch queue
pub struct PrefetchQueue {
    /// Queue of pending requests
    queue: std::sync::Mutex<VecDeque<PrefetchRequest>>,
    /// Notifier for new items
    notify: tokio::sync::Notify,
}

impl PrefetchQueue {
    fn new() -> Self {
        Self {
            queue: std::sync::Mutex::new(VecDeque::new()),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub fn enqueue(&self, request: PrefetchRequest) {
        let mut queue = self.queue.lock().unwrap();

        // Insert by priority (higher priority first)
        let pos = queue.iter()
            .position(|r| r.priority < request.priority)
            .unwrap_or(queue.len());

        queue.insert(pos, request);
        self.notify.notify_one();
    }

    pub async fn dequeue(&self) -> Option<PrefetchRequest> {
        loop {
            {
                let mut queue = self.queue.lock().unwrap();
                if let Some(request) = queue.pop_front() {
                    return Some(request);
                }
            }
            self.notify.notified().await;
        }
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }
}

/// Temporal pattern storage
pub struct TemporalPatternStore {
    /// Patterns by hour of day (0-23)
    hourly_patterns: [DashMap<QueryFingerprint, u64>; 24],
    /// Patterns by day of week (0-6)
    daily_patterns: [DashMap<QueryFingerprint, u64>; 7],
}

impl TemporalPatternStore {
    fn new() -> Self {
        Self {
            hourly_patterns: std::array::from_fn(|_| DashMap::new()),
            daily_patterns: std::array::from_fn(|_| DashMap::new()),
        }
    }

    fn record(&self, fingerprint: &QueryFingerprint, hour: usize, weekday: usize) {
        if hour < 24 {
            self.hourly_patterns[hour]
                .entry(fingerprint.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }
        if weekday < 7 {
            self.daily_patterns[weekday]
                .entry(fingerprint.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
        }
    }

    fn predict_for_hour(&self, hour: usize) -> Vec<QueryFingerprint> {
        if hour >= 24 {
            return Vec::new();
        }

        let patterns = &self.hourly_patterns[hour];
        let mut predictions: Vec<_> = patterns.iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect();

        predictions.sort_by_key(|b| std::cmp::Reverse(b.1));
        predictions.into_iter()
            .take(10)
            .map(|(fp, _)| fp)
            .collect()
    }
}

/// Predictive prefetcher
pub struct PredictivePrefetcher {
    /// Configuration
    config: DistribCacheConfig,

    /// Query sequence patterns (prev -> next queries)
    patterns: DashMap<QueryFingerprint, Vec<QueryFingerprint>>,

    /// Session-based sequences
    session_sequences: DashMap<SessionId, VecDeque<QueryFingerprint>>,

    /// Temporal patterns
    temporal_patterns: TemporalPatternStore,

    /// Prefetch queue
    prefetch_queue: Arc<PrefetchQueue>,

    /// Running flag
    running: AtomicBool,

    /// Statistics
    predictions_made: AtomicU64,
    prefetch_hits: AtomicU64,
    prefetch_misses: AtomicU64,
}

impl PredictivePrefetcher {
    /// Create a new prefetcher
    pub fn new(config: DistribCacheConfig) -> Self {
        Self {
            config,
            patterns: DashMap::new(),
            session_sequences: DashMap::new(),
            temporal_patterns: TemporalPatternStore::new(),
            prefetch_queue: Arc::new(PrefetchQueue::new()),
            running: AtomicBool::new(false),
            predictions_made: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            prefetch_misses: AtomicU64::new(0),
        }
    }

    /// Record a query for pattern learning
    pub fn record(&self, session: &SessionId, fingerprint: QueryFingerprint) {
        // Get or create session sequence
        let mut seq = self.session_sequences
            .entry(session.clone())
            .or_insert_with(|| VecDeque::with_capacity(100));

        // Learn pattern from sequence
        if !seq.is_empty() {
            if let Some(prev) = seq.back() {
                self.patterns
                    .entry(prev.clone())
                    .or_default()
                    .push(fingerprint.clone());
            }
        }

        // Add to sequence
        seq.push_back(fingerprint.clone());

        // Maintain size limit
        while seq.len() > 100 {
            seq.pop_front();
        }

        // Record temporal pattern
        let now = chrono::Utc::now();
        self.temporal_patterns.record(
            &fingerprint,
            now.hour() as usize,
            now.weekday().num_days_from_monday() as usize,
        );
    }

    /// Predict and enqueue prefetch requests
    pub fn predict_and_prefetch(&self, current: &QueryFingerprint, _session: &SessionId) {
        if !self.config.prefetch_enabled {
            return;
        }

        // 1. Pattern-based prediction
        if let Some(next_queries) = self.patterns.get(current) {
            let predictions = self.get_top_predictions(next_queries.value());

            for (fingerprint, confidence) in predictions {
                if confidence > self.config.prefetch_confidence_threshold {
                    self.prefetch_queue.enqueue(PrefetchRequest {
                        fingerprint,
                        priority: (confidence * 100.0) as u32,
                    });
                    self.predictions_made.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // 2. Temporal prediction
        let hour = chrono::Utc::now().hour() as usize;
        let temporal_predictions = self.temporal_patterns.predict_for_hour(hour);

        for fingerprint in temporal_predictions.into_iter().take(self.config.prefetch_lookahead as usize) {
            self.prefetch_queue.enqueue(PrefetchRequest {
                fingerprint,
                priority: 50, // Medium priority for temporal
            });
        }
    }

    /// Get top predictions with confidence scores
    fn get_top_predictions(&self, next_queries: &[QueryFingerprint]) -> Vec<(QueryFingerprint, f32)> {
        // Count occurrences
        let mut counts: HashMap<&QueryFingerprint, u32> = HashMap::new();
        for fp in next_queries {
            *counts.entry(fp).or_default() += 1;
        }

        let total = next_queries.len() as f32;

        // Calculate confidence and sort
        let mut predictions: Vec<_> = counts.into_iter()
            .map(|(fp, count)| (fp.clone(), count as f32 / total))
            .collect();

        predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        predictions.into_iter()
            .take(self.config.prefetch_lookahead as usize)
            .collect()
    }

    /// Start the prefetch background worker
    pub async fn start(&self) {
        self.running.store(true, Ordering::SeqCst);

        // In production, this would spawn a background task
        // that processes the prefetch queue
    }

    /// Stop the prefetcher
    pub async fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Record prefetch hit
    pub fn record_hit(&self) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record prefetch miss
    pub fn record_miss(&self) {
        self.prefetch_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get prefetch statistics
    pub fn stats(&self) -> PrefetchStats {
        let hits = self.prefetch_hits.load(Ordering::Relaxed);
        let misses = self.prefetch_misses.load(Ordering::Relaxed);

        PrefetchStats {
            predictions_made: self.predictions_made.load(Ordering::Relaxed),
            queue_size: self.prefetch_queue.len(),
            hit_rate: if hits + misses > 0 {
                hits as f64 / (hits + misses) as f64
            } else {
                0.0
            },
            patterns_learned: self.patterns.len(),
            sessions_tracked: self.session_sequences.len(),
        }
    }

    /// Clean up old sessions
    pub fn cleanup_old_sessions(&self, _max_age: std::time::Duration) {
        // In production, track timestamps and clean up
        // For now, just limit total sessions
        if self.session_sequences.len() > 10000 {
            // Remove random entries to stay under limit
            let to_remove: Vec<_> = self.session_sequences.iter()
                .take(1000)
                .map(|e| e.key().clone())
                .collect();

            for key in to_remove {
                self.session_sequences.remove(&key);
            }
        }
    }
}

/// Prefetch statistics
#[derive(Debug, Clone)]
pub struct PrefetchStats {
    /// Total predictions made
    pub predictions_made: u64,
    /// Current queue size
    pub queue_size: usize,
    /// Prefetch hit rate
    pub hit_rate: f64,
    /// Number of patterns learned
    pub patterns_learned: usize,
    /// Number of sessions tracked
    pub sessions_tracked: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_queue() {
        let queue = PrefetchQueue::new();

        let fp1 = QueryFingerprint::from_query("SELECT 1");
        let fp2 = QueryFingerprint::from_query("SELECT 2");
        let fp3 = QueryFingerprint::from_query("SELECT 3");

        // Add with different priorities
        queue.enqueue(PrefetchRequest { fingerprint: fp1.clone(), priority: 50 });
        queue.enqueue(PrefetchRequest { fingerprint: fp2.clone(), priority: 100 });
        queue.enqueue(PrefetchRequest { fingerprint: fp3.clone(), priority: 25 });

        assert_eq!(queue.len(), 3);
    }

    #[test]
    fn test_pattern_learning() {
        let config = DistribCacheConfig::default();
        let prefetcher = PredictivePrefetcher::new(config);
        let session = SessionId::new("test");

        let fp1 = QueryFingerprint::from_query("SELECT * FROM users");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");
        let fp3 = QueryFingerprint::from_query("SELECT * FROM items");

        // Simulate sequence: fp1 -> fp2 -> fp3
        prefetcher.record(&session, fp1.clone());
        prefetcher.record(&session, fp2.clone());
        prefetcher.record(&session, fp3.clone());

        // Pattern fp1 -> fp2 should be learned
        assert!(prefetcher.patterns.contains_key(&fp1));
        let next = prefetcher.patterns.get(&fp1).unwrap();
        assert!(next.contains(&fp2));
    }

    #[test]
    fn test_prediction() {
        let config = DistribCacheConfig::builder()
            .prefetch_enabled(true)
            .prefetch_confidence_threshold(0.0) // Accept all predictions for test
            .build();
        let prefetcher = PredictivePrefetcher::new(config);
        let session = SessionId::new("test");

        // Train pattern: query1 -> query2 (repeated)
        let fp1 = QueryFingerprint::from_query("SELECT * FROM users WHERE id = ?");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders WHERE user_id = ?");

        for _ in 0..10 {
            prefetcher.record(&session, fp1.clone());
            prefetcher.record(&session, fp2.clone());
        }

        // Now predict after fp1
        prefetcher.predict_and_prefetch(&fp1, &session);

        // Should have enqueued prefetch for fp2
        assert!(!prefetcher.prefetch_queue.is_empty());
    }

    #[test]
    fn test_temporal_patterns() {
        let store = TemporalPatternStore::new();
        let fp = QueryFingerprint::from_query("SELECT * FROM reports");

        // Record at hour 9 multiple times
        for _ in 0..10 {
            store.record(&fp, 9, 1);
        }

        // Predict for hour 9 should include our query
        let predictions = store.predict_for_hour(9);
        assert!(predictions.contains(&fp));
    }

    #[test]
    fn test_stats() {
        let config = DistribCacheConfig::default();
        let prefetcher = PredictivePrefetcher::new(config);

        prefetcher.record_hit();
        prefetcher.record_hit();
        prefetcher.record_miss();

        let stats = prefetcher.stats();
        assert!((stats.hit_rate - 0.666).abs() < 0.01);
    }
}
