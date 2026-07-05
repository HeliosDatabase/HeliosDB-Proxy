//! Shared request-parsing bounds for the HTTP-facing listeners (HTTP `/sql`,
//! MCP, and GraphQL gateways). Without these an unauthenticated client can pin
//! a handler task forever (slowloris — no read deadline), grow memory with an
//! endless header stream, or force a multi-gigabyte `vec![0u8; Content-Length]`
//! allocation. The admin server already enforces equivalent caps; this module
//! brings the gateways to the same posture.

use crate::{ProxyError, Result};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};
use tokio::time::{timeout_at, Duration, Instant};

/// Overall deadline for reading one request (request line + headers + body).
pub(crate) const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(15);
/// Max number of header lines accepted.
pub(crate) const MAX_HTTP_HEADERS: usize = 100;
/// Max total bytes of the header section.
pub(crate) const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
/// Max request body accepted — bounds the `vec![0u8; len]` allocation.
pub(crate) const MAX_HTTP_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Constant-time equality over two strings' bytes (reveals nothing beyond the
/// length via timing). Used for Bearer-token checks so a `==` short-circuit
/// can't be used as an oracle.
pub(crate) fn constant_time_eq_str(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A parsed HTTP request line + header lines, read under bounds.
pub(crate) struct RequestHead {
    pub method: String,
    pub path: String,
    pub content_length: usize,
    /// Trimmed header lines (excluding the request line), for the caller to
    /// scan for listener-specific headers (Authorization, Neon-Array-Mode, …).
    pub headers: Vec<String>,
}

impl RequestHead {
    /// The trimmed value of the first header whose name matches `name`
    /// (case-insensitive), or `None`.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }
}

/// Read the request line + headers under `deadline`, bounding header count and
/// total header bytes. Returns `Err` on timeout, oversized headers, or an early
/// close — the caller drops the connection.
pub(crate) async fn read_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    deadline: Instant,
) -> Result<RequestHead> {
    let mut line = String::new();
    let mut method = String::new();
    let mut path = String::new();
    let mut content_length = 0usize;
    let mut headers = Vec::new();
    let mut total = 0usize;
    let mut first = true;
    loop {
        line.clear();
        let n = timeout_at(deadline, reader.read_line(&mut line))
            .await
            .map_err(|_| ProxyError::Network("request read timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("request read: {}", e)))?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        total += n;
        if headers.len() >= MAX_HTTP_HEADERS || total > MAX_HTTP_HEADER_BYTES {
            return Err(ProxyError::Network(
                "request header section too large".to_string(),
            ));
        }
        if first {
            let mut parts = line.split_whitespace();
            method = parts.next().unwrap_or("").to_string();
            path = parts.next().unwrap_or("").to_string();
            first = false;
            continue;
        }
        let trimmed = line.trim();
        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("content-length:") {
            content_length = trimmed
                .split(':')
                .nth(1)
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
        }
        headers.push(trimmed.to_string());
    }
    Ok(RequestHead {
        method,
        path,
        content_length,
        headers,
    })
}

/// Read exactly `len` bytes under `deadline`. The caller MUST have already
/// rejected `len > MAX_HTTP_BODY_BYTES` (with a 413) before calling this, so the
/// allocation here is bounded.
pub(crate) async fn read_body<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    len: usize,
    deadline: Instant,
) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    if len > 0 {
        timeout_at(deadline, reader.read_exact(&mut buf))
            .await
            .map_err(|_| ProxyError::Network("request body read timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("request body read: {}", e)))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq_str("Bearer abc", "Bearer abc"));
        assert!(!constant_time_eq_str("Bearer abc", "Bearer abd"));
        assert!(!constant_time_eq_str("Bearer abc", "Bearer ab"));
        assert!(!constant_time_eq_str("", "x"));
        assert!(constant_time_eq_str("", ""));
    }

    #[tokio::test]
    async fn read_head_bounds_headers() {
        // A header section over the line cap is rejected.
        let mut big = String::from("POST /sql HTTP/1.1\r\n");
        for i in 0..(MAX_HTTP_HEADERS + 5) {
            big.push_str(&format!("X-Pad-{}: y\r\n", i));
        }
        big.push_str("\r\n");
        let mut r = tokio::io::BufReader::new(big.as_bytes());
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(read_head(&mut r, deadline).await.is_err());
    }

    #[tokio::test]
    async fn read_head_parses_and_exposes_headers() {
        let req = "POST /sql HTTP/1.1\r\nContent-Length: 7\r\nAuthorization: Bearer tok\r\n\r\n";
        let mut r = tokio::io::BufReader::new(req.as_bytes());
        let deadline = Instant::now() + Duration::from_secs(5);
        let head = read_head(&mut r, deadline).await.unwrap();
        assert_eq!(head.method, "POST");
        assert_eq!(head.path, "/sql");
        assert_eq!(head.content_length, 7);
        assert_eq!(head.header("authorization"), Some("Bearer tok"));
        assert_eq!(head.header("x-missing"), None);
    }
}
