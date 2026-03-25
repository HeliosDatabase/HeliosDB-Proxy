//! Sliding Window Rate Limiter
//!
//! Implements a sliding window algorithm for rate limiting over
//! rolling time periods (e.g., queries per minute).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Sliding window rate limiter
///
/// Tracks events over a rolling time window, allowing precise
/// rate limiting over time periods like "100 queries per minute".
#[derive(Debug)]
pub struct SlidingWindow {
    /// Window duration
    window_size: Duration,

    /// Maximum events allowed in window
    max_events: u32,

    /// Event timestamps (relative to epoch)
    events: Mutex<VecDeque<u64>>,

    /// Epoch for time calculations
    epoch: Instant,

    /// Total events processed (for metrics)
    total_events: AtomicU64,

    /// Events rejected (for metrics)
    rejected_events: AtomicU64,
}

impl SlidingWindow {
    /// Create a new sliding window
    ///
    /// # Arguments
    /// * `window_size` - Duration of the sliding window
    /// * `max_events` - Maximum events allowed within the window
    pub fn new(window_size: Duration, max_events: u32) -> Self {
        Self {
            window_size,
            max_events,
            events: Mutex::new(VecDeque::with_capacity(max_events as usize)),
            epoch: Instant::now(),
            total_events: AtomicU64::new(0),
            rejected_events: AtomicU64::new(0),
        }
    }

    /// Create a sliding window for events per second
    pub fn per_second(max_events: u32) -> Self {
        Self::new(Duration::from_secs(1), max_events)
    }

    /// Create a sliding window for events per minute
    pub fn per_minute(max_events: u32) -> Self {
        Self::new(Duration::from_secs(60), max_events)
    }

    /// Create a sliding window for events per hour
    pub fn per_hour(max_events: u32) -> Self {
        Self::new(Duration::from_secs(3600), max_events)
    }

    /// Try to record an event
    ///
    /// Returns Ok(()) if event was recorded, Err with wait time if limit exceeded.
    pub fn try_record(&self) -> Result<(), SlidingWindowExceeded> {
        self.try_record_n(1)
    }

    /// Try to record multiple events
    pub fn try_record_n(&self, count: u32) -> Result<(), SlidingWindowExceeded> {
        let now = self.epoch.elapsed().as_nanos() as u64;
        let window_nanos = self.window_size.as_nanos() as u64;
        let cutoff = now.saturating_sub(window_nanos);

        let mut events = self.events.lock();

        // Remove expired events
        while let Some(&front) = events.front() {
            if front < cutoff {
                events.pop_front();
            } else {
                break;
            }
        }

        // Check if we have room
        let current_count = events.len() as u32;
        if current_count + count > self.max_events {
            self.rejected_events.fetch_add(count as u64, Ordering::Relaxed);

            let wait_time = if let Some(&oldest) = events.front() {
                let expires_at = oldest + window_nanos;
                if expires_at > now {
                    Duration::from_nanos(expires_at - now)
                } else {
                    Duration::ZERO
                }
            } else {
                Duration::ZERO
            };

            return Err(SlidingWindowExceeded {
                retry_after: wait_time,
                current_count,
                max_count: self.max_events,
                window_size: self.window_size,
            });
        }

        // Record events
        for _ in 0..count {
            events.push_back(now);
        }

        self.total_events.fetch_add(count as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Record an event, blocking until allowed (with timeout)
    pub fn record_blocking(&self, timeout: Duration) -> Result<(), SlidingWindowExceeded> {
        let deadline = Instant::now() + timeout;

        loop {
            match self.try_record() {
                Ok(()) => return Ok(()),
                Err(exceeded) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(exceeded);
                    }

                    let wait = exceeded.retry_after.min(deadline - now);
                    std::thread::sleep(wait);
                }
            }
        }
    }

    /// Get current event count in window
    pub fn current_count(&self) -> u32 {
        let now = self.epoch.elapsed().as_nanos() as u64;
        let cutoff = now.saturating_sub(self.window_size.as_nanos() as u64);

        let events = self.events.lock();
        events.iter().filter(|&&t| t >= cutoff).count() as u32
    }

    /// Get remaining capacity
    pub fn remaining_capacity(&self) -> u32 {
        self.max_events.saturating_sub(self.current_count())
    }

    /// Get window size
    pub fn window_size(&self) -> Duration {
        self.window_size
    }

    /// Get max events
    pub fn max_events(&self) -> u32 {
        self.max_events
    }

    /// Get utilization ratio (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        self.current_count() as f64 / self.max_events as f64
    }

    /// Get total events processed
    pub fn total_events(&self) -> u64 {
        self.total_events.load(Ordering::Relaxed)
    }

    /// Get total events rejected
    pub fn rejected_events(&self) -> u64 {
        self.rejected_events.load(Ordering::Relaxed)
    }

    /// Get rejection rate (0.0 - 1.0)
    pub fn rejection_rate(&self) -> f64 {
        let total = self.total_events();
        let rejected = self.rejected_events();
        let attempted = total + rejected;

        if attempted == 0 {
            0.0
        } else {
            rejected as f64 / attempted as f64
        }
    }

    /// Reset the sliding window
    pub fn reset(&self) {
        self.events.lock().clear();
        self.total_events.store(0, Ordering::Relaxed);
        self.rejected_events.store(0, Ordering::Relaxed);
    }

    /// Get event rate (events per second)
    pub fn current_rate(&self) -> f64 {
        let count = self.current_count();
        count as f64 / self.window_size.as_secs_f64()
    }

    /// Estimate time until an event can be recorded
    pub fn time_until_available(&self) -> Duration {
        if self.remaining_capacity() > 0 {
            return Duration::ZERO;
        }

        let now = self.epoch.elapsed().as_nanos() as u64;
        let window_nanos = self.window_size.as_nanos() as u64;

        let events = self.events.lock();
        if let Some(&oldest) = events.front() {
            let expires_at = oldest + window_nanos;
            if expires_at > now {
                return Duration::from_nanos(expires_at - now);
            }
        }

        Duration::ZERO
    }

    /// Update max events (for dynamic limits)
    pub fn set_max_events(&mut self, max_events: u32) {
        self.max_events = max_events;
    }

    /// Update window size (for dynamic limits)
    pub fn set_window_size(&mut self, window_size: Duration) {
        self.window_size = window_size;
    }
}

impl Clone for SlidingWindow {
    fn clone(&self) -> Self {
        Self {
            window_size: self.window_size,
            max_events: self.max_events,
            events: Mutex::new(self.events.lock().clone()),
            epoch: self.epoch,
            total_events: AtomicU64::new(self.total_events.load(Ordering::Relaxed)),
            rejected_events: AtomicU64::new(self.rejected_events.load(Ordering::Relaxed)),
        }
    }
}

/// Error returned when sliding window limit is exceeded
#[derive(Debug, Clone)]
pub struct SlidingWindowExceeded {
    /// Time until an event slot opens up
    pub retry_after: Duration,

    /// Current event count in window
    pub current_count: u32,

    /// Maximum events allowed
    pub max_count: u32,

    /// Window size
    pub window_size: Duration,
}

impl std::fmt::Display for SlidingWindowExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sliding window exceeded: {}/{} events in {:?}, retry after {}ms",
            self.current_count,
            self.max_count,
            self.window_size,
            self.retry_after.as_millis()
        )
    }
}

impl std::error::Error for SlidingWindowExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_creation() {
        let window = SlidingWindow::new(Duration::from_secs(60), 100);
        assert_eq!(window.window_size(), Duration::from_secs(60));
        assert_eq!(window.max_events(), 100);
        assert_eq!(window.current_count(), 0);
    }

    #[test]
    fn test_per_second() {
        let window = SlidingWindow::per_second(10);
        assert_eq!(window.window_size(), Duration::from_secs(1));
        assert_eq!(window.max_events(), 10);
    }

    #[test]
    fn test_per_minute() {
        let window = SlidingWindow::per_minute(100);
        assert_eq!(window.window_size(), Duration::from_secs(60));
        assert_eq!(window.max_events(), 100);
    }

    #[test]
    fn test_record_success() {
        let window = SlidingWindow::new(Duration::from_secs(60), 10);

        for i in 0..10 {
            assert!(window.try_record().is_ok(), "Failed on event {}", i);
        }

        assert_eq!(window.current_count(), 10);
    }

    #[test]
    fn test_record_exceeded() {
        let window = SlidingWindow::new(Duration::from_secs(60), 5);

        for _ in 0..5 {
            assert!(window.try_record().is_ok());
        }

        let result = window.try_record();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.current_count, 5);
        assert_eq!(err.max_count, 5);
    }

    #[test]
    fn test_record_n() {
        let window = SlidingWindow::new(Duration::from_secs(60), 10);

        assert!(window.try_record_n(5).is_ok());
        assert_eq!(window.current_count(), 5);

        assert!(window.try_record_n(5).is_ok());
        assert_eq!(window.current_count(), 10);

        // Should fail - would exceed
        assert!(window.try_record_n(1).is_err());
    }

    #[test]
    fn test_event_expiration() {
        let window = SlidingWindow::new(Duration::from_millis(50), 5);

        // Fill window
        for _ in 0..5 {
            assert!(window.try_record().is_ok());
        }
        assert_eq!(window.current_count(), 5);

        // Should be full
        assert!(window.try_record().is_err());

        // Wait for events to expire
        std::thread::sleep(Duration::from_millis(60));

        // Should be able to record again
        assert!(window.try_record().is_ok());
        // Count should be 1 (only the new event, old ones expired)
        assert!(window.current_count() <= 2); // Allow some timing variance
    }

    #[test]
    fn test_remaining_capacity() {
        let window = SlidingWindow::new(Duration::from_secs(60), 10);

        assert_eq!(window.remaining_capacity(), 10);

        assert!(window.try_record_n(3).is_ok());
        assert_eq!(window.remaining_capacity(), 7);

        assert!(window.try_record_n(7).is_ok());
        assert_eq!(window.remaining_capacity(), 0);
    }

    #[test]
    fn test_utilization() {
        let window = SlidingWindow::new(Duration::from_secs(60), 10);

        assert!((window.utilization() - 0.0).abs() < 0.01);

        assert!(window.try_record_n(5).is_ok());
        assert!((window.utilization() - 0.5).abs() < 0.01);

        assert!(window.try_record_n(5).is_ok());
        assert!((window.utilization() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_total_and_rejected() {
        let window = SlidingWindow::new(Duration::from_secs(60), 3);

        assert!(window.try_record().is_ok());
        assert!(window.try_record().is_ok());
        assert!(window.try_record().is_ok());
        assert!(window.try_record().is_err());
        assert!(window.try_record().is_err());

        assert_eq!(window.total_events(), 3);
        assert_eq!(window.rejected_events(), 2);
    }

    #[test]
    fn test_rejection_rate() {
        let window = SlidingWindow::new(Duration::from_secs(60), 2);

        assert!(window.try_record().is_ok()); // 1 success
        assert!(window.try_record().is_ok()); // 2 success
        assert!(window.try_record().is_err()); // 1 failure
        assert!(window.try_record().is_err()); // 2 failures

        // 2 rejected out of 4 attempts = 50%
        assert!((window.rejection_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_reset() {
        let window = SlidingWindow::new(Duration::from_secs(60), 10);

        assert!(window.try_record_n(5).is_ok());
        assert_eq!(window.current_count(), 5);

        window.reset();

        assert_eq!(window.current_count(), 0);
        assert_eq!(window.total_events(), 0);
        assert_eq!(window.rejected_events(), 0);
    }

    #[test]
    fn test_current_rate() {
        let window = SlidingWindow::new(Duration::from_secs(10), 100);

        assert!(window.try_record_n(50).is_ok());

        // 50 events in a 10 second window = 5 events/sec
        let rate = window.current_rate();
        assert!((rate - 5.0).abs() < 0.1);
    }

    #[test]
    fn test_time_until_available() {
        let window = SlidingWindow::new(Duration::from_millis(100), 1);

        // Empty window should be immediately available
        assert_eq!(window.time_until_available(), Duration::ZERO);

        // Fill window
        assert!(window.try_record().is_ok());

        // Should need to wait for expiration
        let wait = window.time_until_available();
        assert!(wait.as_millis() > 0);
        assert!(wait.as_millis() <= 100);
    }

    #[test]
    fn test_clone() {
        let window1 = SlidingWindow::new(Duration::from_secs(60), 10);
        assert!(window1.try_record_n(5).is_ok());

        let window2 = window1.clone();
        assert_eq!(window2.current_count(), 5);
        assert_eq!(window2.max_events(), 10);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let window = Arc::new(SlidingWindow::new(Duration::from_secs(60), 100));
        let mut handles = vec![];

        // Spawn 10 threads, each trying to record 20 events
        for _ in 0..10 {
            let window = Arc::clone(&window);
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    let _ = window.try_record();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have exactly 100 events (limited by max)
        assert_eq!(window.current_count(), 100);
        // Should have 100 rejected (200 attempts - 100 success)
        assert_eq!(window.rejected_events(), 100);
    }

    #[test]
    fn test_record_blocking() {
        let window = SlidingWindow::new(Duration::from_millis(20), 1);

        // Record first event
        assert!(window.try_record().is_ok());

        // Should succeed after waiting
        let result = window.record_blocking(Duration::from_millis(50));
        assert!(result.is_ok());
    }

    #[test]
    fn test_record_blocking_timeout() {
        let window = SlidingWindow::new(Duration::from_secs(60), 1);

        // Fill window
        assert!(window.try_record().is_ok());

        // Should timeout
        let result = window.record_blocking(Duration::from_millis(10));
        assert!(result.is_err());
    }
}
