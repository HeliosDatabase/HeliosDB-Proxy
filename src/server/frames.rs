use super::*;

/// Validate a backend-declared frame length against the configured cap
/// before it is used to size a read/accumulation buffer.
///
/// The backend auth-phase scanners (`proxy_authentication`,
/// `complete_backend_auth`) hand-parse the raw wire header — `len` is the
/// 4-byte big-endian length field straight off a byte a hostile or
/// compromised backend controls. Without this check a declared
/// `len = 0xFFFFFFFF` makes the scanner wait for ~4 GiB to accumulate in
/// `backend_buffer`/`buffer` before ever reaching `ProtocolCodec`, which
/// enforces its own `max_message_size` only on the already-decoded path.
/// `max` is `state.limits.max_pending_bytes` — the same configured cap the
/// client-facing buffers in this file already use to bound frame/pending
/// accumulation (see the `max_pending_bytes` checks above in the data
/// path), reused here rather than inventing a second size limit.
/// Read one backend frame header out of `rem` and validate it (H-07).
///
/// `Ok(None)` when fewer than the 5 header bytes are present. `Err` when the
/// declared length is below the 4-byte protocol minimum (no further bytes can
/// make it a valid frame, so waiting is a hang) or above `max` (the accumulator
/// would otherwise grow toward whatever the backend advertises). `Ok(Some(len))`
/// is the declared length; the frame occupies `len + 1` bytes, and because
/// `len <= max <= usize::MAX - 1` is enforced here that addition cannot overflow.
pub(super) fn backend_frame_len(rem: &[u8], max: usize) -> Result<Option<usize>> {
    if rem.len() < 5 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([rem[1], rem[2], rem[3], rem[4]]) as usize;
    if len < 4 {
        return Err(ProxyError::Protocol(format!(
            "backend frame '{}' declares length {} below the 4-byte minimum",
            rem[0] as char, len
        )));
    }
    validate_backend_frame_len(len, max.min(usize::MAX - 1))?;
    Ok(Some(len))
}

/// Per-read timeout under an optional whole-response deadline (H-07 slow drip):
/// the smaller of the per-read timeout and what is left of the deadline, or an
/// error once the deadline has passed.
pub(super) fn read_budget(
    per_read: Duration,
    response_deadline: Option<tokio::time::Instant>,
) -> Result<Duration> {
    match response_deadline {
        None => Ok(per_read),
        Some(d) => {
            let left = d.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Err(ProxyError::Network(
                    "Backend response timeout: whole-response deadline exceeded".to_string(),
                ));
            }
            Ok(per_read.min(left))
        }
    }
}

/// The budgets one streaming relay needs, resolved from `[limits]`. Only the
/// cache-capture relay takes them as a bundle (clippy's argument cap); the plain
/// relays read `state.limits` directly, so this is gated with that relay.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
#[derive(Debug, Clone, Copy)]
pub(super) struct RelayLimits {
    pub(super) client_write_timeout: Duration,
    pub(super) backend_read_timeout: Duration,
    /// H-07 backend frame budget (`[limits] max_backend_frame_bytes`).
    pub(super) max_frame_bytes: usize,
    /// Whole-response deadline; `None` = off.
    pub(super) response_timeout: Option<Duration>,
    /// TR-06 observation byte budget.
    pub(super) observation_bytes: usize,
}

pub(super) fn validate_backend_frame_len(len: usize, max: usize) -> Result<()> {
    if len > max {
        return Err(ProxyError::Protocol(format!(
            "backend frame length {} exceeds max {}",
            len, max
        )));
    }
    Ok(())
}
