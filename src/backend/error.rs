//! Backend client error types.

use thiserror::Error;

/// Errors produced while acting as a PostgreSQL client.
#[derive(Debug, Error)]
pub enum BackendError {
    /// I/O on the underlying TCP or TLS stream failed.
    #[error("backend I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The backend closed the connection before the expected reply
    /// arrived (e.g. TCP RST, idle timeout, server shutdown).
    #[error("backend closed connection unexpectedly")]
    Closed,

    /// A protocol-level violation: unexpected message type, malformed
    /// payload, or truncated frame.
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// The backend returned an `ErrorResponse` (tag `E`). The string is
    /// the `M` (Message) field, which is always present.
    #[error("backend error: {0}")]
    BackendError(String),

    /// Authentication did not complete successfully — wrong password,
    /// unsupported SASL mechanism, or SCRAM server verifier mismatch.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// TLS handshake failed. String carries the rustls diagnostic.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Unsupported type OID in a result column — the proxy only knows
    /// how to decode a fixed set of common OIDs.
    #[error("unsupported type OID: {0}")]
    UnsupportedType(u32),

    /// Value parsing (text-format) failed — e.g. malformed int, bad
    /// boolean string, invalid timestamp.
    #[error("value parse error: column {column}: {reason}")]
    ParseValue { column: String, reason: String },
}

pub type BackendResult<T> = std::result::Result<T, BackendError>;
