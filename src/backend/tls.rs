//! TLS handshake for backend PostgreSQL connections.
//!
//! Flow:
//! 1. Send `SSLRequest` (8 bytes: length=8, code=80877103) on plain TCP.
//! 2. Read one byte: `S` = server accepts TLS, `N` = server refuses.
//! 3. On `S`, run a rustls client handshake on top of the same TCP
//!    socket and continue with the normal PG startup message over the
//!    TLS stream.
//! 4. On `N`, fail (if TLS was required) or fall back to plain.

use super::error::{BackendError, BackendResult};
use super::stream::Stream;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::ServerName;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// TLS connection policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// Never attempt TLS — plain TCP only.
    Disable,
    /// Try TLS first; if the server refuses, fall back to plain. Matches
    /// `libpq sslmode=prefer`.
    Prefer,
    /// Require TLS. Error out if the server refuses.
    Require,
}

/// Build a rustls `ClientConfig` that verifies peer certs against the
/// Mozilla root set shipped in `webpki-roots`. Keeping the builder here
/// lets callers reuse it without reconstructing on every connect.
pub fn default_client_config() -> Arc<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Arc::new(config)
}

/// Perform the PG SSLRequest dance and (if accepted) upgrade the TCP
/// stream to TLS.
///
/// `sni` is the server name used for certificate verification; it must
/// match the server certificate's CN/SAN. Typically the hostname from
/// the cluster config.
pub async fn negotiate(
    mut tcp: TcpStream,
    mode: TlsMode,
    config: Arc<ClientConfig>,
    sni: &str,
) -> BackendResult<Stream> {
    if mode == TlsMode::Disable {
        return Ok(Stream::Plain(tcp));
    }

    // SSLRequest frame: [length=8][code=80877103]
    let ssl_request: [u8; 8] = [
        0x00, 0x00, 0x00, 0x08, // length = 8
        0x04, 0xd2, 0x16, 0x2f, // 80877103
    ];
    tcp.write_all(&ssl_request).await?;

    let mut reply = [0u8; 1];
    tcp.read_exact(&mut reply).await?;

    match reply[0] {
        b'S' => {
            let dns = ServerName::try_from(sni.to_string())
                .map_err(|_| BackendError::Tls(format!("invalid SNI hostname: {:?}", sni)))?;
            let connector = TlsConnector::from(config);
            let tls = connector
                .connect(dns, tcp)
                .await
                .map_err(|e| BackendError::Tls(e.to_string()))?;
            Ok(Stream::Tls(tls))
        }
        b'N' => {
            if mode == TlsMode::Require {
                Err(BackendError::Tls(
                    "server refused TLS and tls_mode=require".to_string(),
                ))
            } else {
                Ok(Stream::Plain(tcp))
            }
        }
        other => Err(BackendError::Tls(format!(
            "unexpected reply to SSLRequest: 0x{:02x}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client_config_builds() {
        let _ = default_client_config();
    }

    #[test]
    fn test_tls_mode_variants() {
        assert_ne!(TlsMode::Disable, TlsMode::Prefer);
        assert_ne!(TlsMode::Prefer, TlsMode::Require);
    }
}
