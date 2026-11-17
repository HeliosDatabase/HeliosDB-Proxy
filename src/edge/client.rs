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
    // Highest `boot:seq` stream position applied so far (C-03). Sent back on
    // reconnect so the home can replay only the missed tail.
    let mut cursor: Option<(u64, u64)> = None;
    loop {
        match open_stream(&client, &subscribe_url, &cfg, &edge_id, cursor).await {
            Ok(resp) => {
                tracing::info!(
                    edge_id = %edge_id,
                    region = %cfg.region,
                    url = %subscribe_url,
                    "edge client: subscribed to home invalidation stream"
                );
                // C-03: with no cursor there is nothing to resume — start
                // cold. With a cursor the home's `: resume` marker decides:
                // `warm` keeps the cache, `gap` flushes it (see pump_stream).
                if cursor.is_none() {
                    let dropped = cache.flush_all();
                    if dropped > 0 {
                        tracing::warn!(
                            edge_id = %edge_id,
                            dropped,
                            "edge client: cold start — flushed any pre-existing cache entries"
                        );
                    }
                }
                // Successful connect resets the backoff ladder.
                backoff = BACKOFF_INITIAL;
                match pump_stream(resp, &cache, &mut cursor).await {
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
    cursor: Option<(u64, u64)>,
) -> Result<reqwest::Response, String> {
    // `query` url-encodes the values. `base_url` is the callback slot
    // the registry records for future ack-checks — empty today (the
    // edge has no HTTP listener of its own to advertise).
    let mut params: Vec<(&str, String)> = vec![
        ("edge_id", edge_id.to_string()),
        ("region", cfg.region.clone()),
        ("base_url", String::new()),
    ];
    if let Some((boot, seq)) = cursor {
        // C-03 warm-resume cursor: only meaningful to homes that emitted
        // `id:` offsets; pre-C-03 homes ignore the unknown query key.
        params.push(("last_event_id", format!("{boot}:{seq}")));
    }
    let mut req = client.get(url).query(&params);
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
async fn pump_stream(
    mut resp: reqwest::Response,
    cache: &EdgeCache,
    cursor: &mut Option<(u64, u64)>,
) -> Result<(), String> {
    let mut parser = SseParser::default();
    let mut stream_boot = None;
    loop {
        let chunk = match tokio::time::timeout(IDLE_TIMEOUT, resp.chunk()).await {
            Err(_) => {
                return Err(format!(
                    "no data or heartbeat for {}s — connection presumed dead",
                    IDLE_TIMEOUT.as_secs()
                ))
            }
            Ok(Err(e)) => return Err(format!("read: {e}")),
            Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some(chunk))) => chunk,
        };
        let payloads = parser.feed(&chunk);
        // Preserve the exact order within a feed: a later marker cannot
        // authorize an earlier event, even when both share a network chunk.
        for delivered in parser.take_deliveries() {
            match delivered {
                SseDelivery::Resume(marker) => {
                    let rest = marker.strip_prefix("resume ").unwrap_or("");
                    let Some(boot) = field(rest, "boot=") else {
                        cache.flush_all();
                        *cursor = None;
                        return Err("invalid edge resume boot".into());
                    };
                    let Some(upto) = field(rest, "upto=") else {
                        cache.flush_all();
                        *cursor = None;
                        return Err("invalid edge resume position".into());
                    };
                    if rest.starts_with("warm ") {
                        if !cursor
                            .is_some_and(|(prior_boot, seq)| prior_boot == boot && seq <= upto)
                        {
                            cache.flush_all();
                            *cursor = None;
                            return Err("warm edge resume has no matching applied cursor".into());
                        }
                        // `upto` describes the queued replay tail. The only
                        // checkpoint remains the last event actually applied.
                        tracing::info!(
                            boot,
                            upto,
                            "edge client: warm resume — awaiting replay tail"
                        );
                    } else if rest.starts_with("gap ") {
                        let dropped = cache.flush_all();
                        // Zero is also the checkpoint of an empty stream on
                        // old homes. Do not acknowledge a possible first seq0.
                        *cursor = (upto > 0).then_some((boot, upto));
                        tracing::warn!(
                            boot,
                            upto,
                            dropped,
                            "edge client: invalidation gap — cache flushed"
                        );
                    } else {
                        cache.flush_all();
                        *cursor = None;
                        return Err("invalid edge resume mode".into());
                    }
                    stream_boot = Some(boot);
                }
                SseDelivery::Event { index, id } => {
                    let sequenced = id.filter(|seq| *seq > 0);
                    let boot = if sequenced.is_some() {
                        let Some(boot) = stream_boot else {
                            cache.flush_all();
                            *cursor = None;
                            return Err("sequenced edge event precedes its boot marker".into());
                        };
                        Some(boot)
                    } else {
                        None
                    };
                    let applied = match sequenced {
                        Some(seq) => apply_invalidation_checked(&payloads[index], cache, Some(seq)),
                        None => apply_invalidation(&payloads[index], cache),
                    };
                    if !applied {
                        cache.flush_all();
                        return Err("edge invalidation was not successfully applied".into());
                    }
                    if let (Some(boot), Some(seq)) = (boot, sequenced) {
                        let previous = cursor.filter(|(b, _)| *b == boot).map(|(_, seq)| seq);
                        // A dropped live event, or the first positive id from
                        // an older home, cannot silently preserve stale scope.
                        if previous.is_none_or(|old| seq > old.saturating_add(1)) {
                            cache.flush_all();
                        }
                        *cursor = Some((boot, previous.map_or(seq, |old| old.max(seq))));
                    }
                }
                SseDelivery::LostBoundary => {
                    cache.flush_all();
                    *cursor = None;
                    return Err("oversized edge frame lost its boundary".into());
                }
            }
        }
        // Clear the compatibility marker projection; ordered delivery above
        // owns checkpoint decisions, while the accessor remains available.
        if let Some(marker) = parser.take_resume() {
            tracing::trace!(marker, last_completed = ?parser.last_id(), "edge stream chunk applied");
        }
    }
}

/// Extract `name<digits>` from a resume marker (space-delimited fields).
fn field(s: &str, name: &str) -> Option<u64> {
    s.split_whitespace()
        .find_map(|f| f.strip_prefix(name))
        .and_then(|v| v.parse().ok())
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
/// Invalid payloads return false; the stream owner flushes and reconnects without acknowledging them.
fn apply_invalidation(payload: &str, cache: &EdgeCache) -> bool {
    apply_invalidation_checked(payload, cache, None)
}

fn apply_invalidation_checked(payload: &str, cache: &EdgeCache, expected_seq: Option<u64>) -> bool {
    match serde_json::from_str::<InvalidationEvent>(payload) {
        Ok(ev) => {
            if expected_seq.is_some_and(|seq| ev.seq != seq) {
                tracing::warn!(expected_seq = ?expected_seq, actual_seq = ev.seq, "edge event id does not name its payload");
                return false;
            }
            let flushed = cache.on_home_epoch(ev.epoch);
            if flushed > 0 {
                tracing::info!(
                    epoch = ev.epoch,
                    flushed,
                    "edge client: home epoch changed — flushed local cache"
                );
            }
            let dropped = cache.invalidate(ev.up_to_version, &ev.tables);
            cache.observe_home_version(ev.up_to_version);
            tracing::info!(up_to_version = ev.up_to_version, tables = ?ev.tables,
                dropped, committed_at = %ev.committed_at, "edge client: applied invalidation from home");
            true
        }
        Err(error) => {
            tracing::warn!(error = %error, "edge client: unparseable invalidation frame");
            false
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

#[derive(Debug)]
enum SseDelivery {
    Resume(String),
    Event { index: usize, id: Option<u64> },
    LostBoundary,
}

/// Incremental SSE payload parser with ordered delivery metadata. Partial id
/// lines do not become completed checkpoints; payload/marker accessors retain
/// their existing signatures.
#[derive(Debug, Default)]
struct SseParser {
    buf: Vec<u8>,
    /// Raw bytes of completed lines in the current event. Pending buf bytes
    /// are checked with these before any conversion or accumulation.
    event_bytes: usize,
    discard_event: bool,
    data_lines: Vec<String>,
    /// Highest id belonging to a complete event, never a partial id line.
    last_id: Option<u64>,
    pending_id: Option<u64>,
    resume: Option<String>,
    /// Metadata for this feed, in stream order; payload indexes name feed's
    /// existing Vec<String> result without cloning event bodies.
    deliveries: Vec<SseDelivery>,
}

impl SseParser {
    fn feed(&mut self, chunk: &[u8]) -> Vec<String> {
        self.deliveries.clear();
        let mut completed = Vec::new();
        let mut remaining = chunk;
        while !remaining.is_empty() {
            let newline = remaining.iter().position(|&byte| byte == b'\n');
            let length = newline.map_or(remaining.len(), |offset| offset + 1);
            let (part, tail) = remaining.split_at(length);
            let ends_line = newline.is_some();
            if self.discard_event {
                self.discard_part(part, ends_line);
                remaining = tail;
                continue;
            }
            let line_bytes = self.buf.len().saturating_add(part.len());
            let event_bytes = self.event_bytes.saturating_add(line_bytes);
            if line_bytes > MAX_BUFFERED_LINE || event_bytes > MAX_BUFFERED_LINE {
                // Check raw sizes before copying the new part or converting
                // a data/id line. The same bound caps a multiline event.
                tracing::warn!(line_bytes, event_bytes, "edge client: oversized SSE event");
                self.data_lines.clear();
                self.pending_id = None;
                self.event_bytes = 0;
                self.deliveries.push(SseDelivery::LostBoundary);
                self.discard_event = true;
                // At most a CRLF prefix is needed to find a true empty line
                // while rejecting the rest of this event, even across chunks.
                self.buf.truncate(2);
                self.discard_part(part, ends_line);
                remaining = tail;
                continue;
            }
            self.buf.extend_from_slice(part);
            if ends_line {
                let mut line = std::mem::take(&mut self.buf);
                self.event_bytes = event_bytes;
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                let boundary = line.is_empty();
                self.process_line(&line, &mut completed);
                if boundary {
                    self.event_bytes = 0;
                }
                line.clear();
                self.buf = line;
            }
            remaining = tail;
        }
        completed
    }

    fn discard_part(&mut self, part: &[u8], ends_line: bool) {
        let content = if ends_line {
            &part[..part.len() - 1]
        } else {
            part
        };
        let prefix_len = content.len().min(2usize.saturating_sub(self.buf.len()));
        self.buf.extend_from_slice(&content[..prefix_len]);
        if ends_line {
            let boundary = self.buf.is_empty() || self.buf.as_slice() == b"\r";
            self.buf.clear();
            if boundary {
                self.discard_event = false;
                self.event_bytes = 0;
            }
        }
    }

    fn last_id(&self) -> Option<u64> {
        self.last_id
    }
    fn take_resume(&mut self) -> Option<String> {
        self.resume.take()
    }
    fn take_deliveries(&mut self) -> Vec<SseDelivery> {
        std::mem::take(&mut self.deliveries)
    }

    fn process_line(&mut self, line: &[u8], completed: &mut Vec<String>) {
        if line.is_empty() {
            let id = self.pending_id.take();
            if !self.data_lines.is_empty() {
                let index = completed.len();
                completed.push(self.data_lines.join("\n"));
                self.data_lines.clear();
                if let Some(id) = id {
                    self.last_id = Some(self.last_id.map_or(id, |old| old.max(id)));
                }
                self.deliveries.push(SseDelivery::Event { index, id });
            }
            return;
        }
        if line.starts_with(b":") {
            let text = String::from_utf8_lossy(&line[1..]).trim().to_string();
            if text.starts_with("resume ") {
                self.resume = Some(text.clone());
                self.deliveries.push(SseDelivery::Resume(text));
            }
            return;
        }
        if let Some(rest) = line.strip_prefix(b"id:") {
            let rest = rest.strip_prefix(b" ").unwrap_or(rest);
            self.pending_id = std::str::from_utf8(rest)
                .ok()
                .and_then(|value| value.parse().ok());
            return;
        }
        if let Some(rest) = line.strip_prefix(b"data:") {
            let rest = rest.strip_prefix(b" ").unwrap_or(rest);
            self.data_lines
                .push(String::from_utf8_lossy(rest).into_owned());
        }
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
        assert!(
            p.buf.len() <= 2 && p.discard_event,
            "only a bounded CRLF prefix remains"
        );
        // Resynchronises on the next complete frame: the tail of the
        // giant line reads as one junk field line and is ignored.
        let got = p.feed(b"junk-tail\n\ndata: ok\n\n");
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

#[cfg(test)]
mod c03_cursor_tests {
    use super::*;

    #[test]
    fn parses_resume_marker_and_stream_ids() {
        let mut p = SseParser::default();
        let frames = p.feed(
            b": resume warm boot=7 upto=41 replayed=2\n\
              : keepalive\n\n\
              event: invalidate\nid: 42\ndata: {\"v\":1}\n\n",
        );
        assert_eq!(
            p.take_resume().as_deref(),
            Some("resume warm boot=7 upto=41 replayed=2")
        );
        assert_eq!(p.last_id(), Some(42));
        assert_eq!(frames, vec!["{\"v\":1}".to_string()]);
        assert!(p.take_resume().is_none(), "marker taken once");

        let gap = p.feed(b": resume gap boot=8 upto=0\n\n");
        assert!(gap.is_empty());
        assert_eq!(p.take_resume().as_deref(), Some("resume gap boot=8 upto=0"));
    }
}

#[cfg(test)]
mod c03_checkpoint_regressions {
    use super::*;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A finite HTTP body controls Response::chunk boundaries exactly, without
    /// TCP packing assumptions, sleeps, network services or production hooks.
    struct Chunks(VecDeque<bytes::Bytes>);
    impl http_body::Body for Chunks {
        type Data = bytes::Bytes;
        type Error = Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(
                self.0
                    .pop_front()
                    .map(|body| Ok(http_body::Frame::data(body))),
            )
        }
    }

    fn response(chunks: Vec<String>) -> reqwest::Response {
        let chunks = Chunks(chunks.into_iter().map(bytes::Bytes::from).collect());
        http::Response::new(reqwest::Body::wrap(chunks)).into()
    }

    fn event(seq: u64, version: u64, id: bool) -> String {
        let payload = serde_json::json!({"seq":seq,"up_to_version":version,
            "tables":["ledger"],"committed_at":"fixture","epoch":0});
        let id = if id {
            format!("id: {seq}\n")
        } else {
            String::new()
        };
        format!("event: invalidate\n{id}data: {payload}\n\n")
    }

    fn hello() -> String {
        "event: invalidate\ndata: {\"seq\":0,\"up_to_version\":0,\"tables\":[],\"committed_at\":\"fixture\",\"epoch\":0}\n\n".into()
    }

    fn warm_cache() -> EdgeCache {
        let cache = EdgeCache::new(4);
        cache.insert(
            crate::edge::CacheKey::new("ledger", "p"),
            crate::edge::CacheEntry {
                version: 1,
                response_bytes: bytes::Bytes::from_static(b"old"),
                tables: vec!["ledger".into()],
                expires_at: std::time::Instant::now() + Duration::from_secs(60),
            },
        );
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_some());
        cache
    }

    #[test]
    fn c03_checkpoint_id_waits_for_complete_event_boundary() {
        let mut parser = SseParser::default();
        assert!(parser.feed(b"event: invalidate\nid: 2\n").is_empty());
        assert_eq!(parser.last_id(), None, "an id line acknowledges no event");
        assert!(parser.feed(b"data: {\"seq\":2}\n").is_empty());
        assert_eq!(
            parser.last_id(),
            None,
            "data without a blank terminator is incomplete"
        );
        assert_eq!(parser.feed(b"\n"), vec!["{\"seq\":2}".to_string()]);
        assert_eq!(parser.last_id(), Some(2));
    }

    #[tokio::test]
    async fn c03_checkpoint_warm_marker_does_not_ack_queued_tail() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let chunks = vec![
            format!(": resume warm boot=7 upto=3 replayed=2\n\n{}", hello()),
            event(2, 2, true),
        ];
        pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(
            cursor,
            Some((7, 2)),
            "event3 was queued but never delivered/applied"
        );
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    #[tokio::test]
    async fn c03_checkpoint_warm_marker_without_replay_keeps_prior_cursor() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let chunks = vec![
            ": resume warm boot=7 upto=9 replayed=8\n\n".into(),
            ": keepalive\n\n".into(),
        ];
        pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(
            cursor,
            Some((7, 1)),
            "advertised queued tail is not an acknowledgement"
        );
    }

    #[tokio::test]
    async fn c03_checkpoint_partial_event_at_eof_does_not_advance_cursor() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            "event: invalidate\nid: 2\n".into(),
            "data: {\"seq\":2".into(),
        ];
        pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor, Some((7, 1)), "partial event2 was never applied");
    }

    #[tokio::test]
    async fn c03_checkpoint_coalesced_gap_marker_precedes_first_event() {
        let cache = warm_cache();
        let mut cursor = None;
        let body = format!(": resume gap boot=7 upto=0\n\n{}", event(1, 2, true));
        pump_stream(response(vec![body]), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(
            cursor,
            Some((7, 1)),
            "feed and process the boot marker before the same chunk's event"
        );
    }

    #[tokio::test]
    async fn c03_checkpoint_malformed_event_never_acknowledges_its_id() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            "event: invalidate\nid: 2\ndata: not-json\n\n".into(),
        ];
        assert!(pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .is_err());
        assert_eq!(cursor, Some((7, 1)));
        assert!(
            cache
                .get(&crate::edge::CacheKey::new("ledger", "p"))
                .is_none(),
            "unknown event scope fails closed"
        );
    }

    #[tokio::test]
    async fn c03_checkpoint_id_must_name_the_applied_event() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let bad = event(2, 2, true).replace("id: 2", "id: 9");
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            bad,
        ];
        assert!(pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .is_err());
        assert_eq!(cursor, Some((7, 1)));
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    #[tokio::test]
    async fn c03_checkpoint_later_marker_cannot_anchor_an_earlier_event() {
        let cache = warm_cache();
        let mut cursor = None;
        let body = format!("{}: resume gap boot=7 upto=0\n\n", event(1, 2, true));
        assert!(pump_stream(response(vec![body]), &cache, &mut cursor)
            .await
            .is_err());
        assert_eq!(cursor, None);
    }

    #[tokio::test]
    async fn c03_checkpoint_oversized_partial_frame_forces_cold_resume() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            "event: invalidate\nid: 2\n".into(),
            "data: ".to_string() + &"x".repeat(MAX_BUFFERED_LINE + 1),
        ];
        assert!(pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .is_err());
        assert_eq!(cursor, None);
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    fn multiline_event(seq: u64, padding_lines: usize) -> String {
        let mut body = format!("event: invalidate\nid: {seq}\ndata: {{\"seq\":{seq},\n");
        for n in 0..padding_lines {
            body.push_str(&format!("data: \"padding{n}\":\"{}\",\n", "x".repeat(1024)));
        }
        body.push_str("data: \"up_to_version\":2,\n");
        body.push_str("data: \"tables\":[\"ledger\"],\n");
        body.push_str("data: \"committed_at\":\"fixture\",\n");
        body.push_str("data: \"epoch\":0}\n\n");
        body
    }

    #[tokio::test]
    async fn c03_checkpoint_complete_oversized_line_is_not_acknowledged() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let payload = serde_json::json!({"seq":2,"up_to_version":2,"tables":["ledger"],
            "committed_at":"x".repeat(MAX_BUFFERED_LINE+1),"epoch":0});
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            format!("event: invalidate\nid: 2\ndata: {payload}\n\n"),
        ];
        assert!(pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .is_err());
        assert_eq!(cursor, None);
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    #[tokio::test]
    async fn c03_checkpoint_cumulative_multiline_budget_is_enforced() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let multiline = multiline_event(2, MAX_BUFFERED_LINE / 1024 + 1);
        assert!(multiline
            .lines()
            .all(|line| line.len() + 1 < MAX_BUFFERED_LINE));
        assert!(multiline.len() > MAX_BUFFERED_LINE);
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            multiline,
        ];
        assert!(pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .is_err());
        assert_eq!(cursor, None);
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    #[tokio::test]
    async fn c03_checkpoint_valid_multiline_event_is_applied_and_acknowledged() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let chunks = vec![
            format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello()),
            multiline_event(2, 1),
        ];
        pump_stream(response(chunks), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor, Some((7, 2)));
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    #[tokio::test]
    async fn c03_checkpoint_legacy_unsequenced_delivery_remains_supported() {
        let cache = warm_cache();
        let mut cursor = None;
        pump_stream(response(vec![event(0, 2, false)]), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor, None);
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_none());
    }

    #[tokio::test]
    async fn c03_checkpoint_caught_up_warm_stream_preserves_cache() {
        let cache = warm_cache();
        let mut cursor = Some((7, 1));
        let body = format!(": resume warm boot=7 upto=1 replayed=0\n\n{}", hello());
        pump_stream(response(vec![body]), &cache, &mut cursor)
            .await
            .unwrap();
        assert_eq!(cursor, Some((7, 1)));
        assert!(cache
            .get(&crate::edge::CacheKey::new("ledger", "p"))
            .is_some());
    }
}
