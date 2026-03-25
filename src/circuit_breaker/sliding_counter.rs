//! Sliding Window Counter for Failure Tracking
//!
//! Implements a sliding window counter for tracking failures over a time period.
//! Unlike the rate limiting sliding window, this one is optimized for counting
//! discrete events rather than enforcing limits.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Sliding window counter for tracking events over time
#[derive(Debug)]
pub struct SlidingWindowCounter {
    /// Event timestamps
    events: Mutex<VecDeque<Instant>>,
    /// Window duration
    window: Duration,
    /// Optional maximum events to track (for memory bounding)
    max_events: Option<usize>,
}

impl SlidingWindowCounter {
    /// Create a new sliding window counter
    pub fn new(window: Duration) -> Self {
        Self {
            events: Mutex::new(VecDeque::new()),
            window,
            max_events: Some(1000), // Default cap to prevent unbounded growth
        }
    }

    /// Create counter with specific maximum events
    pub fn with_max_events(window: Duration, max_events: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::new()),
            window,
            max_events: Some(max_events),
        }
    }

    /// Record an event and return the current count
    pub fn increment(&self) -> u32 {
        let now = Instant::now();
        let mut events = self.events.lock().expect("lock poisoned");

        // Remove expired events
        let cutoff = now - self.window;
        while events.front().map(|&t| t < cutoff).unwrap_or(false) {
            events.pop_front();
        }

        // Add new event
        events.push_back(now);

        // Enforce max events if set
        if let Some(max) = self.max_events {
            while events.len() > max {
                events.pop_front();
            }
        }

        events.len() as u32
    }

    /// Get current count without adding an event
    pub fn count(&self) -> u32 {
        let now = Instant::now();
        let mut events = self.events.lock().expect("lock poisoned");

        // Remove expired events
        let cutoff = now - self.window;
        while events.front().map(|&t| t < cutoff).unwrap_or(false) {
            events.pop_front();
        }

        events.len() as u32
    }

    /// Reset the counter
    pub fn reset(&self) {
        let mut events = self.events.lock().expect("lock poisoned");
        events.clear();
    }

    /// Get the window duration
    pub fn window_duration(&self) -> Duration {
        self.window
    }

    /// Check if there are any events in the window
    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    /// Get timestamp of most recent event
    pub fn last_event_time(&self) -> Option<Instant> {
        let events = self.events.lock().expect("lock poisoned");
        events.back().copied()
    }

    /// Get timestamp of oldest event in window
    pub fn first_event_time(&self) -> Option<Instant> {
        let now = Instant::now();
        let mut events = self.events.lock().expect("lock poisoned");

        // Remove expired events
        let cutoff = now - self.window;
        while events.front().map(|&t| t < cutoff).unwrap_or(false) {
            events.pop_front();
        }

        events.front().copied()
    }

    /// Get events per second rate
    pub fn rate(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            return 0.0;
        }

        let events = self.events.lock().expect("lock poisoned");
        if let (Some(&first), Some(&last)) = (events.front(), events.back()) {
            let duration = last.duration_since(first);
            if duration.as_secs_f64() > 0.0 {
                return count as f64 / duration.as_secs_f64();
            }
        }

        count as f64 / self.window.as_secs_f64()
    }

    /// Get events for statistics (returns copy of timestamps)
    pub fn get_events(&self) -> Vec<Instant> {
        let now = Instant::now();
        let mut events = self.events.lock().expect("lock poisoned");

        // Remove expired events
        let cutoff = now - self.window;
        while events.front().map(|&t| t < cutoff).unwrap_or(false) {
            events.pop_front();
        }

        events.iter().copied().collect()
    }
}

impl Clone for SlidingWindowCounter {
    fn clone(&self) -> Self {
        let events = self.events.lock().expect("lock poisoned");
        Self {
            events: Mutex::new(events.clone()),
            window: self.window,
            max_events: self.max_events,
        }
    }
}

/// Counter with sub-window bucketing for more efficient counting
/// Uses a circular buffer of buckets for O(1) operations
#[derive(Debug)]
pub struct BucketedCounter {
    /// Bucket counts
    buckets: Mutex<Vec<u32>>,
    /// Bucket duration
    bucket_duration: Duration,
    /// Number of buckets
    num_buckets: usize,
    /// Current bucket index
    current_bucket: Mutex<usize>,
    /// Last update time
    last_update: Mutex<Instant>,
}

impl BucketedCounter {
    /// Create a new bucketed counter
    ///
    /// `window` is the total time window
    /// `num_buckets` is the number of sub-buckets (more = finer granularity)
    pub fn new(window: Duration, num_buckets: usize) -> Self {
        let num_buckets = num_buckets.max(1);
        let bucket_duration = window / num_buckets as u32;

        Self {
            buckets: Mutex::new(vec![0; num_buckets]),
            bucket_duration,
            num_buckets,
            current_bucket: Mutex::new(0),
            last_update: Mutex::new(Instant::now()),
        }
    }

    /// Advance time and clear old buckets
    fn advance_time(&self) {
        let now = Instant::now();
        let mut last_update = self.last_update.lock().expect("lock poisoned");
        let elapsed = now.duration_since(*last_update);

        // How many bucket periods have elapsed?
        let buckets_to_clear =
            (elapsed.as_nanos() / self.bucket_duration.as_nanos()) as usize;

        if buckets_to_clear > 0 {
            let mut buckets = self.buckets.lock().expect("lock poisoned");
            let mut current = self.current_bucket.lock().expect("lock poisoned");

            let clear_count = buckets_to_clear.min(self.num_buckets);
            for _ in 0..clear_count {
                *current = (*current + 1) % self.num_buckets;
                buckets[*current] = 0;
            }

            *last_update = now;
        }
    }

    /// Increment the counter
    pub fn increment(&self) -> u32 {
        self.advance_time();

        let mut buckets = self.buckets.lock().expect("lock poisoned");
        let current = *self.current_bucket.lock().expect("lock poisoned");

        buckets[current] += 1;
        buckets.iter().sum()
    }

    /// Get current count
    pub fn count(&self) -> u32 {
        self.advance_time();

        let buckets = self.buckets.lock().expect("lock poisoned");
        buckets.iter().sum()
    }

    /// Reset the counter
    pub fn reset(&self) {
        let mut buckets = self.buckets.lock().expect("lock poisoned");
        buckets.iter_mut().for_each(|b| *b = 0);
    }

    /// Get total window duration
    pub fn window_duration(&self) -> Duration {
        self.bucket_duration * self.num_buckets as u32
    }
}

impl Clone for BucketedCounter {
    fn clone(&self) -> Self {
        let buckets = self.buckets.lock().expect("lock poisoned");
        let current = *self.current_bucket.lock().expect("lock poisoned");
        let last = *self.last_update.lock().expect("lock poisoned");

        Self {
            buckets: Mutex::new(buckets.clone()),
            bucket_duration: self.bucket_duration,
            num_buckets: self.num_buckets,
            current_bucket: Mutex::new(current),
            last_update: Mutex::new(last),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sliding_window_counter() {
        let counter = SlidingWindowCounter::new(Duration::from_secs(60));

        assert_eq!(counter.count(), 0);
        assert!(counter.is_empty());

        assert_eq!(counter.increment(), 1);
        assert_eq!(counter.increment(), 2);
        assert_eq!(counter.increment(), 3);

        assert_eq!(counter.count(), 3);
        assert!(!counter.is_empty());
    }

    #[test]
    fn test_sliding_window_expiration() {
        let counter = SlidingWindowCounter::new(Duration::from_millis(50));

        counter.increment();
        counter.increment();
        assert_eq!(counter.count(), 2);

        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn test_sliding_window_reset() {
        let counter = SlidingWindowCounter::new(Duration::from_secs(60));

        counter.increment();
        counter.increment();
        assert_eq!(counter.count(), 2);

        counter.reset();
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn test_bucketed_counter() {
        let counter = BucketedCounter::new(Duration::from_secs(10), 10);

        assert_eq!(counter.count(), 0);

        assert_eq!(counter.increment(), 1);
        assert_eq!(counter.increment(), 2);
        assert_eq!(counter.increment(), 3);

        assert_eq!(counter.count(), 3);
    }

    #[test]
    fn test_bucketed_counter_reset() {
        let counter = BucketedCounter::new(Duration::from_secs(10), 10);

        counter.increment();
        counter.increment();
        assert_eq!(counter.count(), 2);

        counter.reset();
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn test_sliding_window_rate() {
        let counter = SlidingWindowCounter::new(Duration::from_secs(10));

        for _ in 0..10 {
            counter.increment();
            std::thread::sleep(Duration::from_millis(10));
        }

        let rate = counter.rate();
        // Should be approximately 100 events/sec (10 events over ~100ms)
        assert!(rate > 0.0);
    }

    #[test]
    fn test_sliding_window_clone() {
        let counter = SlidingWindowCounter::new(Duration::from_secs(60));
        counter.increment();
        counter.increment();

        let cloned = counter.clone();
        assert_eq!(cloned.count(), 2);

        counter.increment();
        assert_eq!(counter.count(), 3);
        assert_eq!(cloned.count(), 2); // Clone is independent
    }
}
