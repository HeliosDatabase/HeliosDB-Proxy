//! Edge-role invalidation client (control plane).
//!
//! When this proxy runs with `role = "edge"`, `EdgeClient::spawn`
//! holds a long-lived SSE subscription against the home proxy's
//! *admin* listener (`GET /api/edge/subscribe`, same bearer gate as
//! every other admin route). Each `event: invalidate` frame the home
//! pushes is applied to the local `EdgeCache`: matching entries are
//! dropped and the local version clock is advanced past the home's
//! (`observe_home_version`), so reads cached after the event can
//! never be mistaken for pre-invalidation state.
//!
//! Delivery is best-effort by design. A dropped connection is
//! re-established with capped exponential backoff (the subscribe
//! endpoint re-registers the edge on every connect), and any events
//! missed while disconnected are covered by the cache TTL — the
//! bounded-staleness contract from the module doc. The data plane
//! (cache misses and writes) does NOT flow through here: it uses the
//! ordinary PG-wire forwarding path with the home listed as this
//! proxy's backend node.

use std::sync::Arc;
use std::time::Duration;

use super::cache::EdgeCache;
use super::registry::InvalidationEvent;
use super::EdgeConfig;

/// TCP/TLS connect budget. The request itself gets NO overall timeout
/// — the SSE stream must live forever.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// First reconnect delay; doubles per consecutive failure.
const BACKOFF_INITIAL: Duration = Duration::from_millis(500);

/// Reconnect delay ceiling.
const BACKOFF_CAP: Duration = Duration::from_secs(10);

/// The home writes a `: keepalive` comment every 15s. Three missed
/// beats with no other traffic means the connection is presumed dead
/// (half-open TCP after a home crash / NAT drop would otherwise leave
/// the edge deaf forever) — tear down and reconnect.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// One SSE line is a single JSON invalidation event — tiny. A partial
/// line larger than this means the stream is garbage; drop the
/// buffered prefix instead of ballooning memory. The tail of the
/// oversized line later parses as junk fields and is skipped, so the
/// parser resynchronises on the next real frame.
const MAX_BUFFERED_LINE: usize = 256 * 1024;

/// Handle-less namespace for the edge-side subscribe loop.
pub struct EdgeClient;

impl EdgeClient {
    /// Spawn the background subscribe/reconnect loop. Runs for the
    /// life of the process (abort the returned handle to stop it).
    ///
    /// The server wires this up only when `edge.enabled` and
    /// `role == Edge`; the loop itself just trusts its config.
    pub fn spawn(cfg: EdgeConfig, cache: Arc<EdgeCache>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            run_subscribe_loop(cfg, cache).await;
        })
    }
}

/// Reconnect-forever driver: connect, pump events until the stream
/// dies, back off, repeat. Backoff resets on every successful connect.
async fn run_subscribe_loop(cfg: EdgeConfig, cache: Arc<EdgeCache>) {
    let edge_id = resolve_edge_id(&cfg.edge_id);
    let subscribe_url = format!("{}/api/edge/subscribe", cfg.home_url.trim_end_matches('/'));

    // One client for the life of the task. Connect timeout only — an
    // overall request timeout would kill the healthy long-lived GET.
    // For an https home_url, refuse any downgrade (a redirect to
    // plain http would otherwise re-send the admin bearer cleartext);
    // validate() already refuses a plain-http home_url when an
    // auth_token is configured (unless explicitly opted out).
    let mut builder = reqwest::Client::builder().connect_timeout(CONNECT_TIMEOUT);
    // URL schemes are case-insensitive (RFC 3986); lower before matching so an
    // `HTTPS://` home still gets downgrade protection.
    if cfg
        .home_url
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("https://")
    {
        builder = builder.https_only(true);
    }
    let client = match builder.build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                error = %e,
                "edge client: failed to build HTTP client — invalidation subscription disabled"
            );
            return;
        }
    };

    let mut backoff = BACKOFF_INITIAL;
    loop {
        match open_stream(&client, &subscribe_url, &cfg, &edge_id).await {
            Ok(resp) => {
                tracing::info!(
                    edge_id = %edge_id,
                    region = %cfg.region,
                    url = %subscribe_url,
                    "edge client: subscribed to home invalidation stream"
                );
                // Successful connect resets the backoff ladder.
                backoff = BACKOFF_INITIAL;
                match pump_stream(resp, &cache).await {
                    // Clean EOF: home closed us (shutdown, an eviction,
                    // or a same-id re-register replaced this stream) —
                    // reconnecting re-registers. Healthy idle edges are
                    // NOT GC-churned: successful heartbeat writes
                    // refresh registry liveness on the home.
                    Ok(()) => tracing::info!(
                        edge_id = %edge_id,
                        "edge client: home closed the invalidation stream — reconnecting"
                    ),
                    Err(e) => tracing::warn!(
                        edge_id = %edge_id,
                        error = %e,
                        "edge client: invalidation stream failed — reconnecting"
                    ),
                }
            }
            Err(e) => {
                tracing::warn!(
                    edge_id = %edge_id,
                    error = %e,
                    retry_in_ms = backoff.as_millis() as u64,
                    "edge client: subscribe attempt failed"
                );
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff);
    }
}

/// One subscribe attempt: send the GET, require a 2xx. Returns the
/// still-streaming response on success.
async fn open_stream(
    client: &reqwest::Client,
    url: &str,
    cfg: &EdgeConfig,
    edge_id: &str,
) -> Result<reqwest::Response, String> {
    // `query` url-encodes the values. `base_url` is the callback slot
    // the registry records for future ack-checks — empty today (the
    // edge has no HTTP listener of its own to advertise).
    let mut req = client.get(url).query(&[
        ("edge_id", edge_id),
        ("region", cfg.region.as_str()),
        ("base_url", ""),
    ]);
    if !cfg.auth_token.is_empty() {
        req = req.bearer_auth(&cfg.auth_token);
    }
    let resp = req.send().await.map_err(|e| format!("request: {}", e))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("home returned HTTP {}", status));
    }
    Ok(resp)
}

/// Read the SSE body chunk-by-chunk until EOF (`Ok`), a read error, or
/// an idle timeout (`Err`), applying every completed invalidation
/// frame to the cache as it arrives.
///
/// `bytes_stream()` needs reqwest's `stream` feature, which this crate
/// doesn't enable — `Response::chunk()` in a loop is the equivalent.
async fn pump_stream(mut resp: reqwest::Response, cache: &EdgeCache) -> Result<(), String> {
    let mut parser = SseParser::default();
    loop {
        let chunk = match tokio::time::timeout(IDLE_TIMEOUT, resp.chunk()).await {
            Err(_) => {
                return Err(format!(
                    "no data or heartbeat for {}s — connection presumed dead",
                    IDLE_TIMEOUT.as_secs()
                ))
            }
            Ok(Err(e)) => return Err(format!("read: {}", e)),
            Ok(Ok(None)) => return Ok(()), // clean EOF
            Ok(Ok(Some(c))) => c,
        };
        for payload in parser.feed(&chunk) {
            apply_invalidation(&payload, cache);
        }
    }
}

/// Parse one `data:` payload as an `InvalidationEvent` and apply it.
/// Order is load-bearing:
///
/// 1. `on_home_epoch` — a changed home epoch (home restart) flushes
///    everything and resets the observed-home clock, since stamps from
///    the previous epoch are incomparable with the new counter.
/// 2. `invalidate` — drops matching entries and bumps the invalidation
///    epoch BEFORE the map lock, rejecting in-flight stores.
/// 3. `observe_home_version` — advances the observed-home clock LAST,
///    so an entry stamped `H` is only insertable after `invalidate(H)`
///    already ran and can never be swept by its own event.
///
/// Unparseable frames are skipped (warn), never fatal.
fn apply_invalidation(payload: &str, cache: &EdgeCache) {
    match serde_json::from_str::<InvalidationEvent>(payload) {
        Ok(ev) => {
            let flushed = cache.on_home_epoch(ev.epoch);
            if flushed > 0 {
                tracing::info!(
                    epoch = ev.epoch,
                    flushed,
                    "edge client: home epoch changed (home restart) — flushed local cache"
                );
            }
            let dropped = cache.invalidate(ev.up_to_version, &ev.tables);
            cache.observe_home_version(ev.up_to_version);
            tracing::info!(
                up_to_version = ev.up_to_version,
                tables = ?ev.tables,
                dropped,
                committed_at = %ev.committed_at,
                "edge client: applied invalidation from home"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                payload = %payload,
                "edge client: skipping unparseable invalidation frame"
            );
        }
    }
}

/// Stable id this edge registers under: the configured `edge_id`, or
/// `edge-<pid>` when unset (the documented `EdgeConfig` fallback).
fn resolve_edge_id(configured: &str) -> String {
    if configured.is_empty() {
        format!("edge-{}", std::process::id())
    } else {
        configured.to_string()
    }
}

fn next_backoff(cur: Duration) -> Duration {
    (cur * 2).min(BACKOFF_CAP)
}

/// Incremental server-sent-events parser.
///
/// Feed raw body bytes as they arrive; the `data:` payloads of every
/// event completed by that chunk come back in order. Handles frames
/// split anywhere across chunk boundaries, multiple frames per chunk,
/// `\r\n` line endings, comment/heartbeat lines (leading `:`), and
/// ignores non-data fields (`event:`, `id:`, `retry:`) — the home only
/// ever emits `event: invalidate`, so the event name carries nothing.
#[derive(Debug, Default)]
struct SseParser {
    /// Unconsumed bytes — at most one partial line between feeds.
    buf: Vec<u8>,
    /// `data:` lines of the event currently being accumulated.
    data_lines: Vec<String>,
}

impl SseParser {
    /// Append a chunk and return the payloads of all events it
    /// completed.
    fn feed(&mut self, chunk: &[u8]) -> Vec<String> {
        self.buf.extend_from_slice(chunk);
        let mut completed = Vec::new();
        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            // Take the line out of the buffer, without its terminator.
            let mut line: Vec<u8> = self.buf.drain(..=nl).collect();
            line.pop(); // the '\n'
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.process_line(&line, &mut completed);
        }
        if self.buf.len() > MAX_BUFFERED_LINE {
            tracing::warn!(
                buffered = self.buf.len(),
                "edge client: oversized SSE line — discarding buffered bytes"
            );
            self.buf.clear();
        }
        completed
    }

    fn process_line(&mut self, line: &[u8], completed: &mut Vec<String>) {
        if line.is_empty() {
            // Blank line = event boundary: dispatch what accumulated.
            if !self.data_lines.is_empty() {
                completed.push(self.data_lines.join("\n"));
                self.data_lines.clear();
            }
            return;
        }
        if line.starts_with(b":") {
            return; // comment — the home's keepalive heartbeat
        }
        if let Some(rest) = line.strip_prefix(b"data:") {
            // The SSE spec strips exactly one space after the colon.
            let rest = rest.strip_prefix(b" ").unwrap_or(rest);
            self.data_lines
                .push(String::from_utf8_lossy(rest).into_owned());
        }
        // Every other field (`event:`, `id:`, `retry:`) is ignored.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::cache::{CacheEntry, CacheKey};
    use std::time::Instant;

    // ---- SSE parser ----

    #[test]
    fn parser_extracts_single_event() {
        let mut p = SseParser::default();
        let got = p.feed(b"event: invalidate\ndata: {\"up_to_version\":3}\n\n");
        assert_eq!(got, vec!["{\"up_to_version\":3}".to_string()]);
    }

    #[test]
    fn parser_handles_frames_split_across_chunks() {
        let mut p = SseParser::default();
        assert!(p.feed(b"event: inval").is_empty());
        assert!(p.feed(b"idate\nda").is_empty());
        assert!(p.feed(b"ta: {\"a\":1}").is_empty());
        // The terminating blank line arrives in two pieces too.
        assert!(p.feed(b"\n").is_empty());
        let got = p.feed(b"\n");
        assert_eq!(got, vec!["{\"a\":1}".to_string()]);
    }

    #[test]
    fn parser_ignores_heartbeat_comments() {
        let mut p = SseParser::default();
        assert!(p.feed(b": keepalive\n\n").is_empty());
        // Heartbeats interleaved with a real event don't disturb it.
        let got = p.feed(b": keepalive\n\ndata: X\n\n: keepalive\n\n");
        assert_eq!(got, vec!["X".to_string()]);
    }

    #[test]
    fn parser_yields_multiple_events_from_one_chunk() {
        let mut p = SseParser::default();
        let got =
            p.feed(b"event: invalidate\ndata: A\n\nevent: invalidate\ndata: B\n\ndata: C\n\n");
        assert_eq!(got, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    }

    #[test]
    fn parser_tolerates_crlf_and_unspaced_data() {
        let mut p = SseParser::default();
        let got = p.feed(b"data:no-space\r\n\r\n");
        assert_eq!(got, vec!["no-space".to_string()]);
    }

    #[test]
    fn parser_joins_multi_line_data_per_sse_spec() {
        let mut p = SseParser::default();
        let got = p.feed(b"data: line1\ndata: line2\n\n");
        assert_eq!(got, vec!["line1\nline2".to_string()]);
    }

    #[test]
    fn parser_discards_oversized_garbage_line() {
        let mut p = SseParser::default();
        let junk = vec![b'x'; MAX_BUFFERED_LINE + 1];
        assert!(p.feed(&junk).is_empty());
        assert!(p.buf.is_empty(), "oversized partial line dropped");
        // Resynchronises on the next complete frame: the tail of the
        // giant line reads as one junk field line and is ignored.
        let got = p.feed(b"junk-tail\ndata: ok\n\n");
        assert_eq!(got, vec!["ok".to_string()]);
    }

    // ---- event application ----

    #[test]
    fn apply_invalidation_drops_entries_and_advances_clock() {
        let cache = EdgeCache::new(10);
        cache.insert(
            CacheKey::new("fp", "p"),
            CacheEntry {
                version: 1,
                response_bytes: bytes::Bytes::from_static(b"row"),
                tables: vec!["users".into()],
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );
        // Legacy frame without an epoch field parses (serde default 0)
        // and never triggers epoch handling.
        apply_invalidation(
            r#"{"up_to_version":5,"tables":["users"],"committed_at":"ts"}"#,
            &cache,
        );
        assert!(cache.get(&CacheKey::new("fp", "p")).is_none());
        // hwm gate: reads stamped at or below 5 are no longer cacheable…
        assert!(!cache.should_cache(5));
        // …the observed-home clock tracks the event…
        assert_eq!(cache.observed_home_version(), 5);
        // …and the local clock jumped past the home's.
        assert!(cache.next_version() > 5);
        assert_eq!(cache.stats().invalidations_received, 1);
    }

    #[test]
    fn apply_invalidation_epoch_change_flushes_cache() {
        // F9: a restarted home resets its version clock. Entries
        // stamped under the previous epoch (arbitrarily high) must not
        // survive just because the new counter is small.
        let cache = EdgeCache::new(10);
        apply_invalidation(
            r#"{"up_to_version":1000000,"tables":[],"committed_at":"ts","epoch":11}"#,
            &cache,
        );
        assert_eq!(cache.observed_home_version(), 1_000_000);
        cache.insert(
            CacheKey::new("fp", "p"),
            CacheEntry {
                version: cache.observed_home_version(),
                response_bytes: bytes::Bytes::from_static(b"row"),
                tables: vec!["users".into()],
                expires_at: Instant::now() + Duration::from_secs(60),
            },
        );
        // New epoch, tiny post-restart version: the old-scheme sweep
        // (2 <= 1000000) would drop nothing — the epoch flush must.
        apply_invalidation(
            r#"{"up_to_version":2,"tables":["users"],"committed_at":"ts","epoch":22}"#,
            &cache,
        );
        assert!(cache.get(&CacheKey::new("fp", "p")).is_none());
        // Clock re-synced to the new home domain.
        assert_eq!(cache.observed_home_version(), 2);
    }

    #[test]
    fn apply_invalidation_skips_garbage_payload() {
        let cache = EdgeCache::new(10);
        apply_invalidation("not json", &cache); // must not panic
        assert_eq!(cache.stats().invalidations_received, 0);
    }

    // ---- helpers ----

    #[test]
    fn edge_id_falls_back_to_pid() {
        assert_eq!(resolve_edge_id("edge-eu-1"), "edge-eu-1");
        assert_eq!(resolve_edge_id(""), format!("edge-{}", std::process::id()));
    }

    #[test]
    fn backoff_doubles_to_cap_only() {
        let mut b = BACKOFF_INITIAL;
        let mut seen = Vec::new();
        for _ in 0..8 {
            seen.push(b.as_millis());
            b = next_backoff(b);
        }
        assert_eq!(seen, vec![500, 1000, 2000, 4000, 8000, 10000, 10000, 10000]);
    }
}
