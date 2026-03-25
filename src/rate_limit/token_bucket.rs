//! Token Bucket Rate Limiter
//!
//! Implements the token bucket algorithm for rate limiting.
//! Allows burst traffic while enforcing sustained rate limits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Token bucket rate limiter
///
/// The token bucket allows for burst traffic up to the bucket capacity,
/// while enforcing a sustained rate over time.
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens (burst capacity)
    capacity: u32,

    /// Current token count (stored as fixed-point: tokens * 1000)
    tokens: AtomicU64,

    /// Refill rate (tokens per second)
    refill_rate: f64,

    /// Last refill timestamp (nanoseconds since epoch)
    last_refill: AtomicU64,

    /// Epoch for time calculations
    epoch: Instant,

    /// Lock for atomic operations across multiple fields
    refill_lock: Mutex<()>,
}

impl TokenBucket {
    /// Create a new token bucket
    ///
    /// # Arguments
    /// * `capacity` - Maximum tokens (burst capacity)
    /// * `refill_rate` - Tokens added per second
    pub fn new(capacity: u32, refill_rate: f64) -> Self {
        let epoch = Instant::now();
        Self {
            capacity,
            tokens: AtomicU64::new((capacity as u64) * 1000), // Start full
            refill_rate,
            last_refill: AtomicU64::new(0),
            epoch,
            refill_lock: Mutex::new(()),
        }
    }

    /// Create a token bucket from QPS configuration
    pub fn from_qps(qps: u32, burst: u32) -> Self {
        Self::new(burst, qps as f64)
    }

    /// Try to acquire tokens
    ///
    /// Returns Ok(()) if tokens were acquired, Err with retry info if not.
    pub fn try_acquire(&self, tokens: u32) -> Result<(), TokenBucketExceeded> {
        self.refill();

        let tokens_needed = (tokens as u64) * 1000;
        let mut current = self.tokens.load(Ordering::Acquire);

        loop {
            if current >= tokens_needed {
                match self.tokens.compare_exchange_weak(
                    current,
                    current - tokens_needed,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Ok(()),
                    Err(updated) => current = updated,
                }
            } else {
                return Err(TokenBucketExceeded {
                    retry_after: self.time_until_available(tokens),
                    current_tokens: (current / 1000) as u32,
                    requested_tokens: tokens,
                });
            }
        }
    }

    /// Acquire tokens, blocking until available (with timeout)
    pub fn acquire_blocking(&self, tokens: u32, timeout: Duration) -> Result<(), TokenBucketExceeded> {
        let deadline = Instant::now() + timeout;

        loop {
            match self.try_acquire(tokens) {
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

    /// Return tokens to the bucket (e.g., if operation was cancelled)
    pub fn return_tokens(&self, tokens: u32) {
        let tokens_to_add = (tokens as u64) * 1000;
        let max = (self.capacity as u64) * 1000;

        let mut current = self.tokens.load(Ordering::Acquire);
        loop {
            let new_value = (current + tokens_to_add).min(max);
            match self.tokens.compare_exchange_weak(
                current,
                new_value,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(updated) => current = updated,
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let _lock = self.refill_lock.lock();

        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        let last = self.last_refill.load(Ordering::Acquire);

        if now_nanos <= last {
            return;
        }

        let elapsed_secs = (now_nanos - last) as f64 / 1_000_000_000.0;
        let new_tokens = (elapsed_secs * self.refill_rate * 1000.0) as u64;

        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Acquire);
            let max = (self.capacity as u64) * 1000;
            let updated = (current + new_tokens).min(max);

            self.tokens.store(updated, Ordering::Release);
            self.last_refill.store(now_nanos, Ordering::Release);
        }
    }

    /// Calculate time until requested tokens are available
    fn time_until_available(&self, tokens: u32) -> Duration {
        let current = self.tokens.load(Ordering::Relaxed) / 1000;
        let needed = (tokens as u64).saturating_sub(current);

        if needed == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(needed as f64 / self.refill_rate)
        }
    }

    /// Get current token count
    pub fn current_tokens(&self) -> u32 {
        self.refill();
        (self.tokens.load(Ordering::Relaxed) / 1000) as u32
    }

    /// Get capacity
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Get refill rate (tokens per second)
    pub fn refill_rate(&self) -> f64 {
        self.refill_rate
    }

    /// Check if bucket is empty
    pub fn is_empty(&self) -> bool {
        self.current_tokens() == 0
    }

    /// Check if bucket is full
    pub fn is_full(&self) -> bool {
        self.current_tokens() >= self.capacity
    }

    /// Get fill percentage (0.0 - 1.0)
    pub fn fill_ratio(&self) -> f64 {
        self.current_tokens() as f64 / self.capacity as f64
    }

    /// Reset bucket to full capacity
    pub fn reset(&self) {
        self.tokens.store((self.capacity as u64) * 1000, Ordering::Release);
        self.last_refill.store(self.epoch.elapsed().as_nanos() as u64, Ordering::Release);
    }

    /// Update capacity (for dynamic limits)
    pub fn set_capacity(&mut self, capacity: u32) {
        self.capacity = capacity;
        // Cap current tokens to new capacity
        let current = self.tokens.load(Ordering::Acquire);
        let max = (capacity as u64) * 1000;
        if current > max {
            self.tokens.store(max, Ordering::Release);
        }
    }

    /// Update refill rate (for dynamic limits)
    pub fn set_refill_rate(&mut self, rate: f64) {
        self.refill_rate = rate;
    }
}

impl Clone for TokenBucket {
    fn clone(&self) -> Self {
        Self {
            capacity: self.capacity,
            tokens: AtomicU64::new(self.tokens.load(Ordering::Relaxed)),
            refill_rate: self.refill_rate,
            last_refill: AtomicU64::new(self.last_refill.load(Ordering::Relaxed)),
            epoch: self.epoch,
            refill_lock: Mutex::new(()),
        }
    }
}

/// Error returned when token bucket is exceeded
#[derive(Debug, Clone)]
pub struct TokenBucketExceeded {
    /// Time until requested tokens are available
    pub retry_after: Duration,

    /// Current tokens in bucket
    pub current_tokens: u32,

    /// Tokens that were requested
    pub requested_tokens: u32,
}

impl std::fmt::Display for TokenBucketExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Token bucket exceeded: {} available, {} requested, retry after {}ms",
            self.current_tokens,
            self.requested_tokens,
            self.retry_after.as_millis()
        )
    }
}

impl std::error::Error for TokenBucketExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_creation() {
        let bucket = TokenBucket::new(100, 10.0);
        assert_eq!(bucket.capacity(), 100);
        assert_eq!(bucket.current_tokens(), 100);
        assert!(bucket.is_full());
    }

    #[test]
    fn test_from_qps() {
        let bucket = TokenBucket::from_qps(100, 200);
        assert_eq!(bucket.capacity(), 200);
        assert_eq!(bucket.refill_rate(), 100.0);
    }

    #[test]
    fn test_acquire_success() {
        let bucket = TokenBucket::new(100, 10.0);

        assert!(bucket.try_acquire(50).is_ok());
        assert_eq!(bucket.current_tokens(), 50);

        assert!(bucket.try_acquire(50).is_ok());
        assert_eq!(bucket.current_tokens(), 0);
    }

    #[test]
    fn test_acquire_failure() {
        let bucket = TokenBucket::new(10, 1.0);

        // Acquire all tokens
        assert!(bucket.try_acquire(10).is_ok());

        // Should fail now
        let result = bucket.try_acquire(1);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.current_tokens, 0);
        assert_eq!(err.requested_tokens, 1);
    }

    #[test]
    fn test_refill() {
        let bucket = TokenBucket::new(100, 100.0); // 100 tokens/sec

        // Drain bucket
        assert!(bucket.try_acquire(100).is_ok());
        assert_eq!(bucket.current_tokens(), 0);

        // Wait for refill
        std::thread::sleep(Duration::from_millis(50));

        // Should have some tokens now (approximately 5)
        let tokens = bucket.current_tokens();
        assert!(tokens > 0);
        assert!(tokens <= 10); // Allow some variance
    }

    #[test]
    fn test_return_tokens() {
        let bucket = TokenBucket::new(100, 10.0);

        assert!(bucket.try_acquire(50).is_ok());
        assert_eq!(bucket.current_tokens(), 50);

        bucket.return_tokens(30);
        assert_eq!(bucket.current_tokens(), 80);

        // Returning more than capacity should cap at capacity
        bucket.return_tokens(50);
        assert_eq!(bucket.current_tokens(), 100);
    }

    #[test]
    fn test_reset() {
        let bucket = TokenBucket::new(100, 10.0);

        assert!(bucket.try_acquire(100).is_ok());
        assert!(bucket.is_empty());

        bucket.reset();
        assert!(bucket.is_full());
    }

    #[test]
    fn test_fill_ratio() {
        let bucket = TokenBucket::new(100, 10.0);

        assert!((bucket.fill_ratio() - 1.0).abs() < 0.01);

        assert!(bucket.try_acquire(50).is_ok());
        assert!((bucket.fill_ratio() - 0.5).abs() < 0.01);

        assert!(bucket.try_acquire(50).is_ok());
        assert!((bucket.fill_ratio() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_time_until_available() {
        let bucket = TokenBucket::new(100, 10.0); // 10 tokens/sec

        // Drain bucket
        assert!(bucket.try_acquire(100).is_ok());

        // Try to get 10 tokens - should need ~1 second
        let result = bucket.try_acquire(10);
        assert!(result.is_err());

        let err = result.unwrap_err();
        // Should be approximately 1 second (within 100ms tolerance)
        assert!(err.retry_after.as_millis() >= 900);
        assert!(err.retry_after.as_millis() <= 1100);
    }

    #[test]
    fn test_acquire_blocking() {
        let bucket = TokenBucket::new(10, 100.0); // 100 tokens/sec

        // Drain bucket
        assert!(bucket.try_acquire(10).is_ok());

        // Should succeed within timeout
        let result = bucket.acquire_blocking(5, Duration::from_millis(100));
        assert!(result.is_ok());
    }

    #[test]
    fn test_acquire_blocking_timeout() {
        let bucket = TokenBucket::new(10, 1.0); // 1 token/sec

        // Drain bucket
        assert!(bucket.try_acquire(10).is_ok());

        // Should timeout (need 10 seconds, only wait 10ms)
        let result = bucket.acquire_blocking(10, Duration::from_millis(10));
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let bucket = Arc::new(TokenBucket::new(1000, 1000.0));
        let mut handles = vec![];

        // Spawn 10 threads, each trying to acquire 50 tokens
        for _ in 0..10 {
            let bucket = Arc::clone(&bucket);
            handles.push(thread::spawn(move || {
                for _ in 0..10 {
                    let _ = bucket.try_acquire(5);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Tokens should be reduced (exact value depends on timing)
        assert!(bucket.current_tokens() < 1000);
    }

    #[test]
    fn test_clone() {
        let bucket1 = TokenBucket::new(100, 10.0);
        assert!(bucket1.try_acquire(50).is_ok());

        let bucket2 = bucket1.clone();
        assert_eq!(bucket2.capacity(), 100);
        assert_eq!(bucket2.current_tokens(), 50);
    }
}
