//! Concurrency Limiter
//!
//! Limits the number of concurrent operations, with optional queuing
//! for requests when the limit is reached.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// Concurrency limiter
///
/// Limits the number of concurrent operations. Operations acquire a guard
/// when starting and release it when complete.
#[derive(Debug)]
pub struct ConcurrencyLimiter {
    /// Maximum concurrent operations
    max_concurrent: AtomicU32,

    /// Currently active operations
    active: AtomicU32,

    /// Waiting queue
    waiters: Mutex<VecDeque<oneshot::Sender<()>>>,

    /// Total operations processed
    total_processed: AtomicU64,

    /// Total operations rejected/timed out
    total_rejected: AtomicU64,

    /// Total wait time (nanoseconds, for average calculation)
    total_wait_time_ns: AtomicU64,

    /// Maximum queue size (0 = unlimited)
    max_queue_size: u32,
}

impl ConcurrencyLimiter {
    /// Create a new concurrency limiter
    pub fn new(max_concurrent: u32) -> Self {
        Self {
            max_concurrent: AtomicU32::new(max_concurrent),
            active: AtomicU32::new(0),
            waiters: Mutex::new(VecDeque::new()),
            total_processed: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_wait_time_ns: AtomicU64::new(0),
            max_queue_size: 0,
        }
    }

    /// Create with a queue size limit
    pub fn with_queue_size(max_concurrent: u32, max_queue_size: u32) -> Self {
        Self {
            max_concurrent: AtomicU32::new(max_concurrent),
            active: AtomicU32::new(0),
            waiters: Mutex::new(VecDeque::with_capacity(max_queue_size as usize)),
            total_processed: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_wait_time_ns: AtomicU64::new(0),
            max_queue_size,
        }
    }

    /// Try to acquire a concurrency slot immediately
    pub fn try_acquire(&self) -> Option<ConcurrencyGuard<'_>> {
        let max = self.max_concurrent.load(Ordering::Acquire);

        loop {
            let current = self.active.load(Ordering::Acquire);
            if current >= max {
                return None;
            }

            if self
                .active
                .compare_exchange_weak(current, current + 1, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                self.total_processed.fetch_add(1, Ordering::Relaxed);
                return Some(ConcurrencyGuard {
                    limiter: self,
                    acquired_at: Instant::now(),
                });
            }
        }
    }

    /// Acquire a slot, waiting if necessary
    pub async fn acquire(&self) -> ConcurrencyGuard<'_> {
        let start = Instant::now();

        // Try immediate acquisition
        if let Some(guard) = self.try_acquire() {
            return guard;
        }

        // Wait for slot
        let (tx, rx) = oneshot::channel();

        {
            let mut waiters = self.waiters.lock();
            waiters.push_back(tx);
        }

        // Wait for notification
        let _ = rx.await;

        // Record wait time
        let wait_ns = start.elapsed().as_nanos() as u64;
        self.total_wait_time_ns.fetch_add(wait_ns, Ordering::Relaxed);

        // Acquire slot (should succeed now)
        self.active.fetch_add(1, Ordering::Release);
        self.total_processed.fetch_add(1, Ordering::Relaxed);

        ConcurrencyGuard {
            limiter: self,
            acquired_at: Instant::now(),
        }
    }

    /// Acquire with timeout
    pub async fn acquire_timeout(&self, timeout: Duration) -> Result<ConcurrencyGuard<'_>, ConcurrencyExceeded> {
        let start = Instant::now();

        // Try immediate acquisition
        if let Some(guard) = self.try_acquire() {
            return Ok(guard);
        }

        // Check queue limit
        if self.max_queue_size > 0 {
            let waiters = self.waiters.lock();
            if waiters.len() >= self.max_queue_size as usize {
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(ConcurrencyExceeded {
                    current: self.active.load(Ordering::Relaxed),
                    max: self.max_concurrent.load(Ordering::Relaxed),
                    queue_length: waiters.len() as u32,
                    wait_time: None,
                });
            }
        }

        // Set up wait
        let (tx, rx) = oneshot::channel();

        {
            let mut waiters = self.waiters.lock();
            waiters.push_back(tx);
        }

        // Wait with timeout
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(())) => {
                let wait_ns = start.elapsed().as_nanos() as u64;
                self.total_wait_time_ns.fetch_add(wait_ns, Ordering::Relaxed);

                self.active.fetch_add(1, Ordering::Release);
                self.total_processed.fetch_add(1, Ordering::Relaxed);

                Ok(ConcurrencyGuard {
                    limiter: self,
                    acquired_at: Instant::now(),
                })
            }
            _ => {
                self.total_rejected.fetch_add(1, Ordering::Relaxed);
                Err(ConcurrencyExceeded {
                    current: self.active.load(Ordering::Relaxed),
                    max: self.max_concurrent.load(Ordering::Relaxed),
                    queue_length: self.waiters.lock().len() as u32,
                    wait_time: Some(start.elapsed()),
                })
            }
        }
    }

    /// Release a slot and wake up waiters
    fn release(&self) {
        self.active.fetch_sub(1, Ordering::Release);

        // Wake up next waiter
        let maybe_waiter = self.waiters.lock().pop_front();
        if let Some(waiter) = maybe_waiter {
            let _ = waiter.send(());
        }
    }

    /// Get current active count
    pub fn active_count(&self) -> u32 {
        self.active.load(Ordering::Relaxed)
    }

    /// Get max concurrent
    pub fn max_concurrent(&self) -> u32 {
        self.max_concurrent.load(Ordering::Relaxed)
    }

    /// Get available slots
    pub fn available(&self) -> u32 {
        let max = self.max_concurrent.load(Ordering::Relaxed);
        let active = self.active.load(Ordering::Relaxed);
        max.saturating_sub(active)
    }

    /// Get queue length
    pub fn queue_length(&self) -> u32 {
        self.waiters.lock().len() as u32
    }

    /// Get utilization ratio (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        let max = self.max_concurrent.load(Ordering::Relaxed);
        let active = self.active.load(Ordering::Relaxed);

        if max == 0 {
            0.0
        } else {
            active as f64 / max as f64
        }
    }

    /// Get total processed
    pub fn total_processed(&self) -> u64 {
        self.total_processed.load(Ordering::Relaxed)
    }

    /// Get total rejected
    pub fn total_rejected(&self) -> u64 {
        self.total_rejected.load(Ordering::Relaxed)
    }

    /// Get average wait time
    pub fn average_wait_time(&self) -> Duration {
        let total = self.total_processed.load(Ordering::Relaxed);
        let wait_ns = self.total_wait_time_ns.load(Ordering::Relaxed);

        if total == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(wait_ns / total)
        }
    }

    /// Update max concurrent (for dynamic limits)
    pub fn set_max_concurrent(&self, max: u32) {
        self.max_concurrent.store(max, Ordering::Release);
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        self.total_processed.store(0, Ordering::Relaxed);
        self.total_rejected.store(0, Ordering::Relaxed);
        self.total_wait_time_ns.store(0, Ordering::Relaxed);
    }

    /// Check if at capacity
    pub fn at_capacity(&self) -> bool {
        self.active.load(Ordering::Relaxed) >= self.max_concurrent.load(Ordering::Relaxed)
    }
}

impl Clone for ConcurrencyLimiter {
    fn clone(&self) -> Self {
        Self {
            max_concurrent: AtomicU32::new(self.max_concurrent.load(Ordering::Relaxed)),
            active: AtomicU32::new(0), // New clone starts with 0 active
            waiters: Mutex::new(VecDeque::new()),
            total_processed: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            total_wait_time_ns: AtomicU64::new(0),
            max_queue_size: self.max_queue_size,
        }
    }
}

/// Guard returned when a concurrency slot is acquired
///
/// The slot is released when the guard is dropped.
pub struct ConcurrencyGuard<'a> {
    limiter: &'a ConcurrencyLimiter,
    acquired_at: Instant,
}

impl<'a> ConcurrencyGuard<'a> {
    /// Get how long this guard has been held
    pub fn held_duration(&self) -> Duration {
        self.acquired_at.elapsed()
    }
}

impl<'a> Drop for ConcurrencyGuard<'a> {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

impl<'a> std::fmt::Debug for ConcurrencyGuard<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencyGuard")
            .field("held_duration", &self.held_duration())
            .finish()
    }
}

/// Error returned when concurrency limit is exceeded
#[derive(Debug, Clone)]
pub struct ConcurrencyExceeded {
    /// Current active operations
    pub current: u32,

    /// Maximum allowed
    pub max: u32,

    /// Current queue length
    pub queue_length: u32,

    /// Time spent waiting (if timed out)
    pub wait_time: Option<Duration>,
}

impl std::fmt::Display for ConcurrencyExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Concurrency limit exceeded: {}/{} active, {} queued",
            self.current, self.max, self.queue_length
        )?;
        if let Some(wait) = self.wait_time {
            write!(f, ", waited {}ms", wait.as_millis())?;
        }
        Ok(())
    }
}

impl std::error::Error for ConcurrencyExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limiter_creation() {
        let limiter = ConcurrencyLimiter::new(10);
        assert_eq!(limiter.max_concurrent(), 10);
        assert_eq!(limiter.active_count(), 0);
        assert_eq!(limiter.available(), 10);
    }

    #[test]
    fn test_try_acquire_success() {
        let limiter = ConcurrencyLimiter::new(3);

        let g1 = limiter.try_acquire();
        assert!(g1.is_some());
        assert_eq!(limiter.active_count(), 1);

        let g2 = limiter.try_acquire();
        assert!(g2.is_some());
        assert_eq!(limiter.active_count(), 2);

        let g3 = limiter.try_acquire();
        assert!(g3.is_some());
        assert_eq!(limiter.active_count(), 3);
    }

    #[test]
    fn test_try_acquire_failure() {
        let limiter = ConcurrencyLimiter::new(2);

        let _g1 = limiter.try_acquire();
        let _g2 = limiter.try_acquire();

        // Should fail - at capacity
        let g3 = limiter.try_acquire();
        assert!(g3.is_none());
    }

    #[test]
    fn test_guard_release_on_drop() {
        let limiter = ConcurrencyLimiter::new(2);

        {
            let _g1 = limiter.try_acquire();
            let _g2 = limiter.try_acquire();
            assert_eq!(limiter.active_count(), 2);
        }

        // Guards dropped
        assert_eq!(limiter.active_count(), 0);
        assert_eq!(limiter.available(), 2);
    }

    #[tokio::test]
    async fn test_acquire_waits() {
        let limiter = ConcurrencyLimiter::new(1);

        let g1 = limiter.try_acquire().unwrap();
        assert_eq!(limiter.active_count(), 1);

        // Start acquire that will need to wait
        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move {
            limiter_clone.acquire().await;
        });

        // Give time for waiter to queue
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Release first guard
        drop(g1);

        // Wait for second acquire to complete
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_acquire_timeout_success() {
        let limiter = ConcurrencyLimiter::new(1);

        let g1 = limiter.try_acquire().unwrap();

        // Start timed acquire in background
        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move {
            let result = limiter_clone.acquire_timeout(Duration::from_millis(100)).await;
            result.is_ok()
        });

        // Release after short delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(g1);

        let ok = handle.await.unwrap();
        assert!(ok);
    }

    #[tokio::test]
    async fn test_acquire_timeout_failure() {
        let limiter = ConcurrencyLimiter::new(1);

        let _g1 = limiter.try_acquire().unwrap();

        // Should timeout
        let result = limiter.acquire_timeout(Duration::from_millis(10)).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.current, 1);
        assert_eq!(err.max, 1);
        assert!(err.wait_time.is_some());
    }

    #[test]
    fn test_utilization() {
        let limiter = ConcurrencyLimiter::new(10);

        assert!((limiter.utilization() - 0.0).abs() < 0.01);

        let _g1 = limiter.try_acquire();
        let _g2 = limiter.try_acquire();
        let _g3 = limiter.try_acquire();

        assert!((limiter.utilization() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_statistics() {
        let limiter = ConcurrencyLimiter::new(2);

        // Process some requests
        {
            let _g1 = limiter.try_acquire();
            let _g2 = limiter.try_acquire();
            let _ = limiter.try_acquire(); // Should fail (won't be counted as rejected in try_acquire)
        }

        assert_eq!(limiter.total_processed(), 2);
    }

    #[test]
    fn test_set_max_concurrent() {
        let limiter = ConcurrencyLimiter::new(5);

        assert_eq!(limiter.max_concurrent(), 5);

        limiter.set_max_concurrent(10);
        assert_eq!(limiter.max_concurrent(), 10);
    }

    #[test]
    fn test_at_capacity() {
        let limiter = ConcurrencyLimiter::new(2);

        assert!(!limiter.at_capacity());

        let _g1 = limiter.try_acquire();
        assert!(!limiter.at_capacity());

        let _g2 = limiter.try_acquire();
        assert!(limiter.at_capacity());
    }

    #[test]
    fn test_guard_held_duration() {
        let limiter = ConcurrencyLimiter::new(1);

        let guard = limiter.try_acquire().unwrap();
        std::thread::sleep(Duration::from_millis(10));

        let held = guard.held_duration();
        assert!(held.as_millis() >= 10);
    }

    #[test]
    fn test_queue_size_limit() {
        let limiter = ConcurrencyLimiter::with_queue_size(1, 2);

        assert_eq!(limiter.max_queue_size, 2);
    }

    #[test]
    fn test_clone() {
        let limiter1 = ConcurrencyLimiter::new(10);
        let _g = limiter1.try_acquire();

        let limiter2 = limiter1.clone();

        // Clone should have same max but 0 active
        assert_eq!(limiter2.max_concurrent(), 10);
        assert_eq!(limiter2.active_count(), 0);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let limiter = Arc::new(ConcurrencyLimiter::new(5));
        let mut handles = vec![];

        // Spawn 10 threads, each acquiring and releasing
        for _ in 0..10 {
            let limiter = Arc::clone(&limiter);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    if let Some(guard) = limiter.try_acquire() {
                        thread::sleep(Duration::from_micros(100));
                        drop(guard);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All guards should be released
        assert_eq!(limiter.active_count(), 0);
    }
}
