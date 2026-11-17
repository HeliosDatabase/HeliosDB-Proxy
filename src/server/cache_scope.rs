//! Proof of the effective backend settings used by a result-cache lookup.
//!
//! Startup parameters alone omit role/database defaults. The actual target
//! socket must answer this bounded exchange before a cached result can be used.
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

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

#[derive(Default)]
struct ScopeReply {
    columns: bool,
    complete: bool,
    refused: bool,
    settings: BTreeMap<String, String>,
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

    fn row(body: &[u8]) -> Option<(String, String)> {
        if body.get(..2)? != 3u16.to_be_bytes() {
            return None;
        }
        let mut rest = &body[2..];
        let mut fields = Vec::with_capacity(3);
        for _ in 0..3 {
            let len = i32::from_be_bytes(rest.get(..4)?.try_into().ok()?);
            rest = &rest[4..];
            let len = usize::try_from(len).ok()?; // NULL is not a valid proof.
            let value = std::str::from_utf8(rest.get(..len)?).ok()?;
            if value.as_bytes().contains(&0) {
                return None;
            }
            fields.push(value);
            rest = &rest[len..];
        }
        if !rest.is_empty()
            || fields[0].is_empty()
            || !fields[0]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.'))
        {
            return None;
        }
        Some((fields[0].to_string(), fields[1].to_string()))
    }

    fn frame(&mut self, tag: u8, body: &[u8]) {
        match tag {
            b'T' if !self.columns && !self.complete && self.settings.is_empty() => {
                self.columns = Self::columns(body);
                self.refused |= !self.columns;
            }
            b'D' if self.columns && !self.complete => {
                if let Some((key, value)) = Self::row(body) {
                    self.refused |= self.settings.insert(key, value).is_some();
                } else {
                    self.refused = true;
                }
            }
            b'C' if self.columns && !self.complete && body == b"SHOW\0" => {
                self.complete = true;
            }
            _ => self.refused = true,
        }
    }

    fn namespace(&self, startup: &[u8; 32], node: &str) -> Option<[u8; 32]> {
        if self.refused || !self.complete {
            return None;
        }
        // These are standard PostgreSQL settings required by the supported
        // proof. A partial SHOW ALL implementation must execute natively.
        for key in [
            "search_path",
            "row_security",
            "DateStyle",
            "TimeZone",
            "client_encoding",
            "standard_conforming_strings",
            "server_version",
        ] {
            if !self.settings.contains_key(key) {
                return None;
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"helios-query-cache-effective-settings-v1");
        hash.update(startup);
        for part in std::iter::once(node).chain(
            self.settings
                .iter()
                .flat_map(|(key, value)| [key.as_str(), value.as_str()]),
        ) {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        Some(hash.finalize().into())
    }
}

impl ProxyServer {
    /// Probe each candidate read: PostgreSQL can change effective settings
    /// after SIGHUP on an existing socket without a complete ParameterStatus
    /// generation signal. A startup-only or once-per-socket proof is stale.
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
        if state.live_config.load().cache.backend_context_immutable {
            if let Some(namespace) = backend.cache_scope_verified {
                return Ok(BackendCacheScope::Verified(namespace));
            }
        }
        Self::finish_pending_backend_frames(client, backend, session, state).await?;
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
            backend.unnamed_destroyed();
            let mut reply = ScopeReply::default();
            let mut total = 0usize;
            // Every backend frame consumes at least five budgeted bytes;
            // row allocation is likewise bounded by the total wire budget.
            for _ in 0..max_bytes / 5 {
                let mut head = [0u8; 5];
                backend
                    .stream
                    .read_exact(&mut head)
                    .await
                    .map_err(|e| ProxyError::Network(format!("cache scope read error: {e}")))?;
                let len = backend_frame_len(&head, max_frame)?.ok_or_else(|| {
                    ProxyError::Protocol("incomplete cache scope frame header".to_string())
                })?;
                total = total.checked_add(len + 1).ok_or_else(|| {
                    ProxyError::Protocol("cache scope response size overflow".to_string())
                })?;
                if total > max_bytes {
                    return Err(ProxyError::Protocol(
                        "cache scope response exceeds byte limit".to_string(),
                    ));
                }
                let mut body = vec![0; len - 4];
                backend
                    .stream
                    .read_exact(&mut body)
                    .await
                    .map_err(|e| ProxyError::Network(format!("cache scope read error: {e}")))?;
                match head[0] {
                    b'Z' => {
                        if body != b"I" {
                            return Err(ProxyError::Protocol(
                                "cache scope probe did not end idle".to_string(),
                            ));
                        }
                        if let Some(mut error) = rejected.take() {
                            backend.cache_scope_verified = None;
                            error.extend_from_slice(&head);
                            error.extend_from_slice(&body);
                            return Ok(BackendCacheScope::Rejected(error));
                        }
                        let namespace = reply.namespace(&startup, node);
                        // An unsupported protocol response has been fully
                        // drained. Repeated probes cannot make it supported.
                        backend.cache_scope_unsupported = namespace.is_none();
                        backend.cache_scope_verified = namespace;
                        return Ok(match namespace {
                            Some(scope) => BackendCacheScope::Verified(scope),
                            None => BackendCacheScope::Unsupported,
                        });
                    }
                    b'E' => {
                        let code = scope_error_code(&body).ok_or_else(|| {
                            ProxyError::Protocol("malformed cache scope ErrorResponse".to_string())
                        })?;
                        // Only unsupported SHOW ALL syntax/feature/object
                        // errors permit native fallback. In particular, a
                        // cancellation (57014), resource limit or permission
                        // error terminates this request without retrying it.
                        if !matches!(code, b"0A000" | b"42601" | b"42704" | b"42883" | b"42P01")
                            && rejected.is_none()
                        {
                            let mut error = head.to_vec();
                            error.extend_from_slice(&body);
                            rejected = Some(error);
                        }
                        reply.frame(b'E', &body);
                    }
                    b'A' | b'N' | b'S' => {
                        // Async frames still belong to this session. Do not
                        // swallow notifications while hiding the probe rows.
                        client.write_all(&head).await.map_err(|e| {
                            ProxyError::Network(format!("cache scope client write error: {e}"))
                        })?;
                        client.write_all(&body).await.map_err(|e| {
                            ProxyError::Network(format!("cache scope client write error: {e}"))
                        })?;
                        state
                            .metrics
                            .bytes_sent
                            .fetch_add((len + 1) as u64, Ordering::Relaxed);
                    }
                    tag => reply.frame(tag, &body),
                }
            }
            Err(ProxyError::Protocol(
                "cache scope response exceeds frame limit".to_string(),
            ))
        };
        let result = tokio::time::timeout(state.limits.reprepare_timeout, exchange)
            .await
            .map_err(|_| ProxyError::Network("cache scope probe timeout".to_string()))
            .and_then(std::convert::identity);
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

    fn reply(path: &str, reverse: bool) -> ScopeReply {
        let mut reply = ScopeReply::default();
        reply.frame(b'T', &columns());
        let mut entries = vec![
            ("search_path", path),
            ("row_security", "on"),
            ("DateStyle", "ISO, MDY"),
            ("TimeZone", "UTC"),
            ("client_encoding", "UTF8"),
            ("standard_conforming_strings", "on"),
            ("server_version", "18.4"),
        ];
        if reverse {
            entries.reverse();
        }
        for (name, setting) in entries {
            reply.frame(
                b'D',
                &row(name, setting, if reverse { "changed" } else { "" }),
            );
        }
        reply.frame(b'C', b"SHOW\0");
        reply
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

    #[test]
    fn effective_scope_refuses_duplicate_malformed_and_partial_replies() {
        let mut duplicate = reply("first", false);
        duplicate.complete = false;
        duplicate.frame(b'D', &row("search_path", "second", ""));
        duplicate.frame(b'C', b"SHOW\0");
        assert!(duplicate.namespace(&[0; 32], "n").is_none());
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
        let mut partial = reply("first", false);
        partial.settings.remove("row_security");
        assert!(partial.namespace(&[0; 32], "n").is_none());
        let mut bad = ScopeReply::default();
        bad.frame(b'T', &columns());
        bad.frame(b'D', b"\0\x03\xff\xff\xff\xff");
        bad.frame(b'C', b"SHOW\0");
        assert!(bad.namespace(&[0; 32], "n").is_none());
    }
}
