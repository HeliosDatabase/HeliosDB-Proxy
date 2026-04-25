//! EWMA + sliding-bucket rate window. Used by the rate-spike
//! detector to compute "queries per second" over a rolling window
//! and a baseline EWMA against which spikes are scored.
//!
//! Bucket layout: 1-second buckets in a fixed ring. `observe` bumps
//! the current bucket, advances the ring as time passes (zero-fill
//! gaps), and recomputes the per-second mean + std-dev when asked
//! for a score.
//!
//! Memory: 8 bytes per bucket × `window_secs`. At the default 60s
//! window that's 480 bytes per tenant. Cheap.

use std::time::{Duration, Instant};

/// Exponentially-weighted moving average. Online, O(1) per update.
#[derive(Debug, Clone)]
pub struct Ewma {
    /// Smoothing factor in (0, 1]. Closer to 1 = more reactive.
    alpha: f64,
    /// Current estimate. None until the first observation.
    value: Option<f64>,
}

impl Ewma {
    pub fn new(alpha: f64) -> Self {
        assert!(alpha > 0.0 && alpha <= 1.0, "alpha must be in (0, 1]");
        Self { alpha, value: None }
    }

    pub fn observe(&mut self, x: f64) {
        self.value = Some(match self.value {
            None => x,
            Some(prev) => self.alpha * x + (1.0 - self.alpha) * prev,
        });
    }

    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

/// Sliding-bucket rate counter. Tracks per-second event counts in a
/// fixed ring of length `window_secs`. Provides `observe_and_score`
/// which:
///   1. Increments the current second's bucket.
///   2. Computes the mean + std-dev across the ring.
///   3. Returns z-score = (current_rate - mean) / std-dev when the
///      window is fully populated AND std-dev > 0.
#[derive(Debug, Clone)]
pub struct RateWindow {
    window_secs: u64,
    /// Ring of per-second counts. Length == window_secs.
    buckets: Vec<u64>,
    /// Index into `buckets` for the most recent second.
    head: usize,
    /// Wall-clock anchor for the current head bucket. Advancing
    /// time rotates the ring and zero-fills skipped seconds.
    anchor: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpikeScore {
    pub rate: f64,
    pub baseline: f64,
    pub z_score: f64,
}

impl RateWindow {
    pub fn new(window_secs: u64) -> Self {
        assert!(window_secs >= 2, "window must hold at least 2 buckets");
        Self {
            window_secs,
            buckets: vec![0u64; window_secs as usize],
            head: 0,
            anchor: None,
        }
    }

    /// Bump the current bucket by one event and return a SpikeScore
    /// when the window has accumulated enough data to be meaningful
    /// (full window AND non-zero std-dev).
    pub fn observe_and_score(&mut self, now: Instant) -> Option<SpikeScore> {
        self.advance_to(now);
        self.buckets[self.head] = self.buckets[self.head].saturating_add(1);

        // Need at least window_secs * 0.5 buckets populated AND
        // non-zero variance in the prior buckets to score.
        let n = self.buckets.len();
        let prior: Vec<u64> = (0..n)
            .filter(|&i| i != self.head)
            .map(|i| self.buckets[i])
            .collect();
        let prior_sum: u64 = prior.iter().sum();
        let populated = prior.iter().filter(|&&v| v > 0).count();
        if populated < (n / 2) {
            return None;
        }
        let mean = prior_sum as f64 / prior.len() as f64;
        let var = prior
            .iter()
            .map(|&v| {
                let d = v as f64 - mean;
                d * d
            })
            .sum::<f64>()
            / prior.len() as f64;
        let std = var.sqrt();
        if std <= 0.0 {
            return None;
        }
        let rate = self.buckets[self.head] as f64;
        let z = (rate - mean) / std;
        Some(SpikeScore {
            rate,
            baseline: mean,
            z_score: z,
        })
    }

    /// Advance the head pointer to track wall-clock seconds. Skipped
    /// seconds get zero-filled buckets.
    fn advance_to(&mut self, now: Instant) {
        let anchor = match self.anchor {
            None => {
                self.anchor = Some(now);
                return;
            }
            Some(a) => a,
        };
        let elapsed = now.duration_since(anchor);
        let secs_advanced = elapsed.as_secs();
        if secs_advanced == 0 {
            return;
        }
        // Cap advancement at the ring size — anything beyond means
        // we wrap and effectively reset the window.
        let cap = self.window_secs.min(secs_advanced);
        for _ in 0..cap {
            self.head = (self.head + 1) % self.buckets.len();
            self.buckets[self.head] = 0;
        }
        self.anchor = Some(anchor + Duration::from_secs(secs_advanced));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ewma_seeds_with_first_observation() {
        let mut e = Ewma::new(0.5);
        assert_eq!(e.value(), None);
        e.observe(10.0);
        assert_eq!(e.value(), Some(10.0));
    }

    #[test]
    fn ewma_smooths_subsequent_observations() {
        let mut e = Ewma::new(0.5);
        e.observe(10.0);
        e.observe(20.0);
        // 0.5 * 20 + 0.5 * 10 = 15
        assert!((e.value().unwrap() - 15.0).abs() < 1e-9);
        e.observe(30.0);
        // 0.5 * 30 + 0.5 * 15 = 22.5
        assert!((e.value().unwrap() - 22.5).abs() < 1e-9);
    }

    #[test]
    fn rate_window_returns_none_before_window_fills() {
        let mut w = RateWindow::new(10);
        let r = w.observe_and_score(Instant::now());
        assert!(r.is_none());
    }

    #[test]
    fn rate_window_scores_a_clean_spike() {
        let mut w = RateWindow::new(10);
        let mut t = Instant::now();
        // Seed 9 prior seconds with 1 event each, then a spike of
        // 100 in the 10th. Mean of prior = 1, std-dev = 0 → no
        // score (uniform prior). To get a non-zero std-dev, vary
        // the prior slightly.
        for i in 0..9 {
            t += Duration::from_secs(1);
            for _ in 0..(1 + (i % 2) as u32) {
                let _ = w.observe_and_score(t);
            }
        }
        // Spike: 50 events in the next second.
        t += Duration::from_secs(1);
        let mut last = None;
        for _ in 0..50 {
            last = w.observe_and_score(t);
        }
        let score = last.expect("should have a score after window fills");
        assert!(
            score.z_score > 5.0,
            "expected a large z-score, got {:?}",
            score
        );
    }

    #[test]
    fn rate_window_zero_fills_idle_gaps() {
        let mut w = RateWindow::new(5);
        let t = Instant::now();
        // Events at t and then a 10-second idle gap (longer than
        // window): the prior buckets should all be 0 + the head
        // counts the new event.
        for _ in 0..3 {
            let _ = w.observe_and_score(t);
        }
        let later = t + Duration::from_secs(10);
        let _ = w.observe_and_score(later);
        // After the 10s gap, the prior buckets are all zeros (we
        // advanced more than window_secs). Score returns None
        // because std-dev is 0 across the (all-zero) prior.
        let r = w.observe_and_score(later);
        assert!(r.is_none(), "all-zero prior should produce no score");
    }

    #[test]
    fn rate_window_panics_on_too_small_window() {
        let res = std::panic::catch_unwind(|| RateWindow::new(1));
        assert!(res.is_err());
    }

    #[test]
    fn ewma_panics_on_invalid_alpha() {
        let res = std::panic::catch_unwind(|| Ewma::new(0.0));
        assert!(res.is_err());
        let res = std::panic::catch_unwind(|| Ewma::new(1.5));
        assert!(res.is_err());
    }
}
