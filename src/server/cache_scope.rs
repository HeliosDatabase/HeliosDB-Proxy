//! Proof of the effective backend settings used by a result-cache lookup.
//!
//! Startup parameters alone omit role/database defaults. The actual target
//! socket must answer this bounded exchange before a cached result can be used.
//!
//! The proof (`SHOW ALL`) is taken on the connection that would run the read
//! and kept there ([`CacheScopeMemo`]) for `[cache] proof_ttl_ms`: later reads
//! reuse it while nothing but proven cacheable reads reached the connection
//! (any other statement, a `FunctionCall` or a relayed ParameterStatus
//! retires it). Its reply is read into one buffer, as much per read as the
//! socket has, and parsed in place: the rows are borrowed from that buffer,
//! never copied.
use super::session::CacheScopeMemo;
use super::*;
use sha2::{Digest, Sha256};
use std::ops::Range;
use std::sync::atomic::AtomicUsize;

/// An internal probe error is not permission to execute the business query.
pub(super) enum BackendCacheScope {
    Verified([u8; 32]),
    Unsupported,
    Rejected(Vec<u8>),
    RejectedLost(Vec<u8>),
}

fn scope_error_code(mut fields: &[u8]) -> Option<&[u8]> {
    while let Some((&tag, body)) = fields.split_first() {
        if tag == 0 {
            return None;
        }
        let end = body.iter().position(|&b| b == 0)?;
        if tag == b'C' {
            let code = &body[..end];
            return (code.len() == 5
                && code
                    .iter()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()))
            .then_some(code);
        }
        fields = &body[end + 1..];
    }
    None
}

/// Size of the last complete proof reply read in this process: the next
/// proof reads into one allocation of about that size. A hint only.
static PROOF_REPLY_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Smallest buffer a proof reads into.
const MIN_PROOF_BUFFER: usize = 8 * 1024;

/// The settings the proof requires: standard PostgreSQL settings a partial
/// `SHOW ALL` implementation lacks. A reply without all of them executes
/// natively.
const REQUIRED_SETTINGS: [&str; 7] = [
    "search_path",
    "row_security",
    "DateStyle",
    "TimeZone",
    "client_encoding",
    "standard_conforming_strings",
    "server_version",
];

/// A `SHOW ALL` reply as it is read: its rows are ranges of the buffer the
/// frames were read into (`frames` below), the name and the setting of each.
#[derive(Default)]
struct ScopeReply {
    columns: bool,
    complete: bool,
    refused: bool,
    rows: Vec<(Range<usize>, Range<usize>)>,
}

impl ScopeReply {
    fn columns(body: &[u8]) -> bool {
        if body.get(..2) != Some(&3u16.to_be_bytes()) {
            return false;
        }
        let mut rest = &body[2..];
        for expected in [b"name".as_slice(), b"setting", b"description"] {
            let Some(end) = rest.iter().position(|&b| b == 0) else {
                return false;
            };
            if &rest[..end] != expected {
                return false;
            }
            rest = &rest[end + 1..];
            let Some(meta) = rest.get(..18) else {
                return false;
            };
            // SHOW ALL has three text columns, all in text format.
            if meta[6..10] != 25u32.to_be_bytes() || meta[16..18] != [0, 0] {
                return false;
            }
            rest = &rest[18..];
        }
        rest.is_empty()
    }

    /// A `DataRow` body's name and setting, as ranges of `body`: three
    /// non-NULL UTF-8 fields without NUL bytes, nothing after them, and a
    /// non-empty name of ASCII letters, digits, `_` and `.`.
    fn row(body: &[u8]) -> Option<(Range<usize>, Range<usize>)> {
        if body.get(..2)? != 3u16.to_be_bytes() {
            return None;
        }
        let mut at = 2usize;
        let mut fields: [Range<usize>; 3] = [0..0, 0..0, 0..0];
        for field in &mut fields {
            let len = i32::from_be_bytes(body.get(at..at + 4)?.try_into().ok()?);
            at += 4;
            let len = usize::try_from(len).ok()?; // NULL is not a valid proof.
            let value = body.get(at..at + len)?;
            if std::str::from_utf8(value).is_err() || value.contains(&0) {
                return None;
            }
            *field = at..at + len;
            at += len;
        }
        let name = &body[fields[0].clone()];
        if at != body.len()
            || name.is_empty()
            || !name
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.'))
        {
            return None;
        }
        let [name, setting, _] = fields;
        Some((name, setting))
    }

    /// Read one frame of the reply: `tag`, and its body at `body` in
    /// `frames`.
    fn frame(&mut self, frames: &[u8], tag: u8, body: Range<usize>) {
        let bytes = &frames[body.clone()];
        match tag {
            b'T' if !self.columns && !self.complete && self.rows.is_empty() => {
                self.columns = Self::columns(bytes);
                self.refused |= !self.columns;
            }
            b'D' if self.columns && !self.complete => match Self::row(bytes) {
                Some((name, setting)) => {
                    let at = body.start;
                    self.rows.push((
                        name.start + at..name.end + at,
                        setting.start + at..setting.end + at,
                    ));
                }
                None => self.refused = true,
            },
            b'C' if self.columns && !self.complete && bytes == b"SHOW\0" => {
                self.complete = true;
            }
            _ => self.refused = true,
        }
    }

    /// The namespace the reply proves, its rows read from `frames`: the
    /// settings in byte order of their names (PostgreSQL sends them in
    /// case-insensitive order), each name once.
    fn namespace(&self, frames: &[u8], startup: &[u8; 32], node: &str) -> Option<[u8; 32]> {
        if self.refused || !self.complete {
            return None;
        }
        let text = |range: &Range<usize>| std::str::from_utf8(&frames[range.clone()]).ok();
        let mut rows: Vec<(&str, &str)> = Vec::with_capacity(self.rows.len());
        for (name, setting) in &self.rows {
            rows.push((text(name)?, text(setting)?));
        }
        // Nearly sorted already: the stable sort runs in about linear time.
        rows.sort_by(|a, b| a.0.cmp(b.0));
        if rows.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return None;
        }
        if REQUIRED_SETTINGS
            .iter()
            .any(|key| rows.binary_search_by(|row| row.0.cmp(key)).is_err())
        {
            return None;
        }
        let mut hash = Sha256::new();
        hash.update(b"helios-query-cache-effective-settings-v1");
        hash.update(startup);
        for part in std::iter::once(node).chain(rows.iter().flat_map(|(k, v)| [*k, *v])) {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        Some(hash.finalize().into())
    }
}

impl ProxyServer {
    /// The effective-settings namespace of the backend connection a cached
    /// read would run on. PostgreSQL can change effective settings after
    /// SIGHUP on an existing socket without a complete ParameterStatus
    /// signal, so a proof is reused only for `[cache] proof_ttl_ms` (and
    /// only while nothing that may change settings reached the connection),
    /// or for good under `backend_context_immutable`.
    pub(super) async fn cache_backend_namespace(
        client: &mut ClientStream,
        backend: &mut BackendConn,
        session: &ClientSession,
        state: &ServerState,
        node: &str,
    ) -> Result<BackendCacheScope> {
        #[cfg(test)]
        if let Some(namespace) = backend.cache_scope_test_override {
            return Ok(BackendCacheScope::Verified(namespace));
        }
        if backend.cache_scope_unsupported {
            return Ok(BackendCacheScope::Unsupported);
        }
        let (immutable, ttl) = {
            let live = state.live_config.load();
            (
                live.cache.backend_context_immutable,
                Duration::from_millis(live.cache.proof_ttl_ms),
            )
        };
        if immutable {
            if let Some(proof) = backend.cache_scope_verified {
                return Ok(BackendCacheScope::Verified(proof.namespace));
            }
        } else if let Some(memo) = backend.cache_scope_memo {
            if backend.response_buf.is_empty()
                && memo.at.elapsed() < ttl
                && memo.parameter_changes == session.parameter_changes.load(Ordering::Relaxed)
            {
                state
                    .metrics
                    .cache_scope_proofs_reused
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(BackendCacheScope::Verified(memo.namespace));
            }
            backend.cache_scope_memo = None;
        }
        Self::finish_pending_backend_frames(client, backend, session, state).await?;
        state
            .metrics
            .cache_scope_proofs
            .fetch_add(1, Ordering::Relaxed);
        let parameter_changes = session.parameter_changes.load(Ordering::Relaxed);
        let startup = *Self::cache_startup_namespace(session).await;
        let max_bytes = state
            .limits
            .max_pending_bytes
            .min(state.live_config.load().cache.max_backend_context_bytes);
        let max_frame = state.limits.max_backend_frame_bytes.min(max_bytes);
        let mut rejected: Option<Vec<u8>> = None;
        let exchange = async {
            let wire = crate::protocol::QueryMessage {
                query: "SHOW ALL".to_string(),
            }
            .encode()
            .encode();
            backend
                .stream
                .write_all(&wire)
                .await
                .map_err(|e| ProxyError::Network(format!("cache scope write error: {e}")))?;
            // SHOW ALL changes no setting: what was known about them stays.
            backend.unnamed_destroyed_keeping_settings();
            let hint = PROOF_REPLY_BYTES.load(Ordering::Relaxed);
            let mut buf = BytesMut::with_capacity(hint.max(MIN_PROOF_BUFFER).min(max_bytes.max(5)));
            let mut reply = ScopeReply::default();
            let mut pos = 0usize;
            loop {
                // The next whole frame at `pos`, reading as much as the
                // socket has each time.
                let len = loop {
                    let header = backend_frame_len(&buf[pos..], max_frame)?;
                    if let Some(len) = header {
                        let end = pos.checked_add(len + 1).ok_or_else(|| {
                            ProxyError::Protocol("cache scope response size overflow".to_string())
                        })?;
                        if end > max_bytes {
                            return Err(ProxyError::Protocol(
                                "cache scope response exceeds byte limit".to_string(),
                            ));
                        }
                        if buf.len() >= end {
                            break len;
                        }
                    }
                    // Bytes still missing for the header or the frame.
                    let wanted = header.map_or(pos + 5, |len| pos + len + 1) - buf.len();
                    if buf.capacity() - buf.len() < wanted {
                        buf.reserve(wanted.max(buf.len()));
                    }
                    let n =
                        backend.stream.read_buf(&mut buf).await.map_err(|e| {
                            ProxyError::Network(format!("cache scope read error: {e}"))
                        })?;
                    if n == 0 {
                        return Err(ProxyError::Network(
                            "cache scope read error: the backend closed the connection".to_string(),
                        ));
                    }
                };
                let frame = pos..pos + len + 1;
                let body = pos + 5..frame.end;
                pos = frame.end;
                match buf[frame.start] {
                    b'Z' => {
                        if buf[body] != *b"I" {
                            return Err(ProxyError::Protocol(
                                "cache scope probe did not end idle".to_string(),
                            ));
                        }
                        PROOF_REPLY_BYTES.store(pos, Ordering::Relaxed);
                        // Whatever followed the reply (an asynchronous
                        // message) belongs to the client: it is relayed
                        // like any other unpublished backend output.
                        backend.response_buf.extend_from_slice(&buf[pos..]);
                        if let Some(mut error) = rejected.take() {
                            backend.cache_scope_verified = None;
                            error.extend_from_slice(&buf[frame]);
                            return Ok(BackendCacheScope::Rejected(error));
                        }
                        let namespace = reply.namespace(&buf, &startup, node);
                        // An unsupported protocol response has been fully
                        // drained. Repeated probes cannot make it supported.
                        backend.cache_scope_unsupported = namespace.is_none();
                        backend.cache_scope_verified =
                            namespace.map(|namespace| crate::pool::SettingsProof {
                                namespace,
                                startup,
                                proven_at: std::time::Instant::now(),
                            });
                        return Ok(match namespace {
                            Some(scope) => BackendCacheScope::Verified(scope),
                            None => BackendCacheScope::Unsupported,
                        });
                    }
                    b'E' => {
                        let code = scope_error_code(&buf[body.clone()]).ok_or_else(|| {
                            ProxyError::Protocol("malformed cache scope ErrorResponse".to_string())
                        })?;
                        // Only unsupported SHOW ALL syntax/feature/object
                        // errors permit native fallback. In particular, a
                        // cancellation (57014), resource limit or permission
                        // error terminates this request without retrying it.
                        if !matches!(code, b"0A000" | b"42601" | b"42704" | b"42883" | b"42P01")
                            && rejected.is_none()
                        {
                            rejected = Some(buf[frame].to_vec());
                        }
                        reply.frame(&buf, b'E', body);
                    }
                    tag @ (b'A' | b'N' | b'S') => {
                        // Async frames still belong to this session. Do not
                        // swallow notifications while hiding the probe rows.
                        if tag == b'S' {
                            session.parameter_changes.fetch_add(1, Ordering::Relaxed);
                        }
                        client.write_all(&buf[frame.clone()]).await.map_err(|e| {
                            ProxyError::Network(format!("cache scope client write error: {e}"))
                        })?;
                        state
                            .metrics
                            .bytes_sent
                            .fetch_add(frame.len() as u64, Ordering::Relaxed);
                    }
                    tag => reply.frame(&buf, tag, body),
                }
            }
        };
        let result = tokio::time::timeout(state.limits.reprepare_timeout, exchange)
            .await
            .map_err(|_| ProxyError::Network("cache scope probe timeout".to_string()))
            .and_then(std::convert::identity);
        if let Ok(BackendCacheScope::Verified(namespace)) = &result {
            // Kept for the reads that follow within the TTL, unless a
            // ParameterStatus arrived meanwhile (the reply may predate it).
            if !immutable
                && !ttl.is_zero()
                && session.parameter_changes.load(Ordering::Relaxed) == parameter_changes
            {
                backend.cache_scope_memo = Some(CacheScopeMemo {
                    namespace: *namespace,
                    startup,
                    at: std::time::Instant::now(),
                    parameter_changes,
                });
            }
        }
        match (result, rejected) {
            // Once an error rejected this request, a later wire loss must not
            // turn it into a retryable business query. The caller publishes
            // that error, abandons the socket and closes the client.
            (Err(_), Some(error)) => Ok(BackendCacheScope::RejectedLost(error)),
            (result, _) => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn columns() -> Vec<u8> {
        let mut out = 3u16.to_be_bytes().to_vec();
        for name in ["name", "setting", "description"] {
            out.extend_from_slice(name.as_bytes());
            out.push(0);
            out.extend_from_slice(&0u32.to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes());
            out.extend_from_slice(&25u32.to_be_bytes());
            out.extend_from_slice(&(-1i16).to_be_bytes());
            out.extend_from_slice(&(-1i32).to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes());
        }
        out
    }

    fn row(name: &str, setting: &str, description: &str) -> Vec<u8> {
        let mut out = 3u16.to_be_bytes().to_vec();
        for part in [name, setting, description] {
            out.extend_from_slice(&(part.len() as i32).to_be_bytes());
            out.extend_from_slice(part.as_bytes());
        }
        out
    }

    /// A reply being read: its frames so far and the parser over them.
    struct Reading {
        frames: Vec<u8>,
        reply: ScopeReply,
    }

    impl Reading {
        fn new() -> Self {
            Self {
                frames: Vec::new(),
                reply: ScopeReply::default(),
            }
        }

        fn frame(&mut self, tag: u8, body: &[u8]) -> &mut Self {
            let start = self.frames.len() + 5;
            self.frames.push(tag);
            self.frames
                .extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            self.frames.extend_from_slice(body);
            let end = self.frames.len();
            self.reply.frame(&self.frames, tag, start..end);
            self
        }

        fn namespace(&self, startup: &[u8; 32], node: &str) -> Option<[u8; 32]> {
            self.reply.namespace(&self.frames, startup, node)
        }
    }

    const SETTINGS: [(&str, &str); 7] = [
        ("search_path", "first"),
        ("row_security", "on"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
        ("client_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("server_version", "18.4"),
    ];

    fn reading(entries: &[(&str, &str)], description: &str) -> Reading {
        let mut r = Reading::new();
        r.frame(b'T', &columns());
        for (name, setting) in entries {
            r.frame(b'D', &row(name, setting, description));
        }
        r.frame(b'C', b"SHOW\0");
        r
    }

    fn reply(path: &str, reverse: bool) -> Reading {
        let mut entries = SETTINGS.to_vec();
        entries[0].1 = path;
        if reverse {
            entries.reverse();
        }
        reading(&entries, if reverse { "changed" } else { "" })
    }

    /// The namespace as 2.2 computed it (settings in a `BTreeMap`): the
    /// buffered reader proves the same namespaces.
    fn v1_namespace(entries: &[(&str, &str)], startup: &[u8; 32], node: &str) -> [u8; 32] {
        let settings: BTreeMap<&str, &str> = entries.iter().copied().collect();
        let mut hash = Sha256::new();
        hash.update(b"helios-query-cache-effective-settings-v1");
        hash.update(startup);
        for part in std::iter::once(node).chain(settings.iter().flat_map(|(k, v)| [*k, *v])) {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        hash.finalize().into()
    }

    #[test]
    fn effective_scope_canonicalizes_settings_but_partitions_actual_context() {
        let a = reply("first", false).namespace(&[1; 32], "node-a");
        assert!(a.is_some());
        assert_eq!(a, reply("first", true).namespace(&[1; 32], "node-a"));
        assert_ne!(a, reply("second", false).namespace(&[1; 32], "node-a"));
        assert_ne!(a, reply("first", false).namespace(&[2; 32], "node-a"));
        assert_ne!(a, reply("first", false).namespace(&[1; 32], "node-b"));
    }

    /// PostgreSQL's order (case-insensitive: `DateStyle` among the `d`s),
    /// byte order and any other order prove one namespace, the one 2.2
    /// proved.
    #[test]
    fn the_namespace_is_unchanged_from_the_map_based_proof() {
        let mut entries = SETTINGS.to_vec();
        entries.push(("application_name", "app"));
        entries.push(("work_mem", "4MB"));
        let mut pg_order = entries.clone();
        pg_order.sort_by_key(|(name, _)| name.to_ascii_lowercase());
        let expected = v1_namespace(&entries, &[7; 32], "pg:5432");
        for order in [entries.clone(), pg_order] {
            assert_eq!(
                reading(&order, "d").namespace(&[7; 32], "pg:5432"),
                Some(expected)
            );
        }
    }

    #[test]
    fn effective_scope_refuses_duplicate_malformed_and_partial_replies() {
        // A name twice, adjacent or not.
        for at in [1, 6] {
            let mut entries = SETTINGS.to_vec();
            entries.insert(at, ("search_path", "second"));
            assert!(reading(&entries, "").namespace(&[0; 32], "n").is_none());
        }
        for (tag, body) in [
            (b'T', columns()),          // a second description
            (b'C', b"SHOW\0".to_vec()), // a second completion
            (b'E', b"error".to_vec()),
            (b'D', row("x", "v", "")), // a row after completion
        ] {
            let mut bad = reply("first", false);
            bad.frame(tag, &body);
            assert!(bad.namespace(&[0; 32], "n").is_none());
        }
        let partial: Vec<_> = SETTINGS
            .iter()
            .copied()
            .filter(|(name, _)| *name != "row_security")
            .collect();
        assert!(reading(&partial, "").namespace(&[0; 32], "n").is_none());
        let mut bad = Reading::new();
        bad.frame(b'T', &columns());
        bad.frame(b'D', b"\0\x03\xff\xff\xff\xff");
        bad.frame(b'C', b"SHOW\0");
        assert!(bad.namespace(&[0; 32], "n").is_none());
        // Incomplete: no CommandComplete.
        let mut open = Reading::new();
        open.frame(b'T', &columns());
        for (name, setting) in SETTINGS {
            open.frame(b'D', &row(name, setting, ""));
        }
        assert!(open.namespace(&[0; 32], "n").is_none());
    }

    #[test]
    fn a_row_must_be_three_text_fields_with_a_plain_name() {
        assert!(ScopeReply::row(&row("work_mem", "4MB", "d")).is_some());
        for body in [
            row("", "v", "d"),
            row("bad name", "v", "d"),
            row("n", "nul\0", "d"),
            [row("n", "v", "d"), vec![0]].concat(),
            b"\0\x02\0\0\0\x01n\0\0\0\x01v".to_vec(),
        ] {
            assert!(ScopeReply::row(&body).is_none(), "{body:?}");
        }
    }
}
