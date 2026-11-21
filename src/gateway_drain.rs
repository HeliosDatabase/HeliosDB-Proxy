//! Shutdown drain for the HTTP SQL, MCP and GraphQL gateways.
//!
//! A shutdown or binary handoff closes the PG-wire listener and ends each
//! session at its next point with nothing in flight. The gateways take part
//! in the same drain through one [`GatewayDrain`]: [`GatewayDrain::close`]
//! makes every gateway's accept loop return (dropping its listener, so new
//! requests are refused at the socket), and every connection a gateway
//! accepted holds a [`RequestGuard`] until its handler returns, so the server
//! can wait for [`GatewayDrain::in_flight`] to reach zero under the same
//! deadline as the sessions.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Inner {
    closing: tokio::sync::watch::Sender<bool>,
    in_flight: AtomicUsize,
}

/// Shared by the server and its gateways; cloning shares the same state.
#[derive(Clone)]
pub struct GatewayDrain {
    inner: Arc<Inner>,
}

impl Default for GatewayDrain {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GatewayDrain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayDrain")
            .field("closing", &self.is_closing())
            .field("in_flight", &self.in_flight())
            .finish()
    }
}

/// One accepted gateway connection; the drain waits until it is dropped.
pub struct RequestGuard {
    inner: Arc<Inner>,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.inner.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl GatewayDrain {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                closing: tokio::sync::watch::channel(false).0,
                in_flight: AtomicUsize::new(0),
            }),
        }
    }

    /// Stop accepting: every gateway accept loop waiting in
    /// [`GatewayDrain::closed`] returns. Idempotent.
    pub fn close(&self) {
        self.inner.closing.send_replace(true);
    }

    /// Whether [`GatewayDrain::close`] was called.
    pub fn is_closing(&self) -> bool {
        *self.inner.closing.borrow()
    }

    /// Resolves once [`GatewayDrain::close`] has been called (at once when it
    /// already was).
    pub async fn closed(&self) {
        let mut rx = self.inner.closing.subscribe();
        // The sender lives in `self`, so the channel cannot close while this
        // future is polled; an error would only mean it did, and then there
        // is nothing left to wait for.
        let _ = rx.wait_for(|closing| *closing).await;
    }

    /// Count one accepted connection until the returned guard is dropped.
    pub fn track(&self) -> RequestGuard {
        self.inner.in_flight.fetch_add(1, Ordering::AcqRel);
        RequestGuard {
            inner: self.inner.clone(),
        }
    }

    /// Accepted gateway connections whose handler has not returned.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn close_wakes_waiters_and_guards_count_in_flight_work() {
        let drain = GatewayDrain::new();
        assert!(!drain.is_closing());
        let waiter = {
            let drain = drain.clone();
            tokio::spawn(async move { drain.closed().await })
        };
        let first = drain.track();
        let second = drain.clone().track();
        assert_eq!(drain.in_flight(), 2);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished(), "nothing closed the drain yet");

        drain.close();
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("close wakes the waiter")
            .unwrap();
        assert!(drain.is_closing());
        // A waiter that arrives after the close returns at once.
        tokio::time::timeout(Duration::from_secs(2), drain.closed())
            .await
            .expect("already closed");

        drop(first);
        assert_eq!(drain.in_flight(), 1);
        drop(second);
        assert_eq!(drain.in_flight(), 0);
        drain.close();
        assert!(drain.is_closing(), "close is idempotent");
    }
}
