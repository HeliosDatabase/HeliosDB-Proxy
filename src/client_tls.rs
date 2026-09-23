//! Client-facing TLS termination.
//!
//! The proxy can terminate TLS from PostgreSQL clients: it answers the
//! `SSLRequest` with `S`, runs a rustls **server** handshake over the TCP
//! socket, and then speaks the wire protocol over the encrypted stream.
//! Optionally it requires and verifies a client certificate (mTLS).
//!
//! Backend connections stay plain `TcpStream` (or use the separate backend
//! TLS in `backend::tls`); this module is only about the client side.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::config::TlsConfig;

/// A client connection that may or may not be TLS-wrapped. Implements
/// `AsyncRead`/`AsyncWrite` by delegating to the active variant, so the
/// whole session loop can be written against one stream type regardless of
/// whether the client negotiated TLS.
pub enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
    /// The peer certificate subject (DER-encoded leaf), if the client
    /// presented one during an mTLS handshake. Used for identity mapping.
    pub fn peer_cert_present(&self) -> bool {
        match self {
            ClientStream::Plain(_) => false,
            ClientStream::Tls(s) => s
                .get_ref()
                .1
                .peer_certificates()
                .map(|c| !c.is_empty())
                .unwrap_or(false),
        }
    }
}

impl AsyncRead for ClientStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_flush(cx),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            ClientStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Build a `TlsAcceptor` from the proxy's `[tls]` config: load the server
/// certificate chain + private key (PEM), and — when `require_client_cert`
/// is set — a client-certificate verifier rooted at `ca_path` (mTLS).
pub fn build_tls_acceptor(tls: &TlsConfig) -> Result<TlsAcceptor, String> {
    use rustls::pki_types::pem::{self, PemObject};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let cert_chain: Vec<CertificateDer<'static>> = {
        let data = std::fs::read(&tls.cert_path)
            .map_err(|e| format!("reading cert {}: {}", tls.cert_path, e))?;
        CertificateDer::pem_slice_iter(&data)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parsing cert {}: {}", tls.cert_path, e))?
    };
    if cert_chain.is_empty() {
        return Err(format!("no certificates found in {}", tls.cert_path));
    }

    let key: PrivateKeyDer<'static> = {
        let data = std::fs::read(&tls.key_path)
            .map_err(|e| format!("reading key {}: {}", tls.key_path, e))?;
        PrivateKeyDer::from_pem_slice(&data).map_err(|e| match e {
            pem::Error::NoItemsFound => format!("no private key found in {}", tls.key_path),
            e => format!("parsing key {}: {}", tls.key_path, e),
        })?
    };

    let builder = rustls::ServerConfig::builder();

    let config = if tls.require_client_cert {
        let ca_path = tls
            .ca_path
            .as_ref()
            .ok_or_else(|| "require_client_cert is set but ca_path is missing".to_string())?;
        let ca_data =
            std::fs::read(ca_path).map_err(|e| format!("reading ca {}: {}", ca_path, e))?;
        let mut roots = rustls::RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(&ca_data) {
            let ca = ca.map_err(|e| format!("parsing ca {}: {}", ca_path, e))?;
            roots
                .add(ca)
                .map_err(|e| format!("adding ca cert: {}", e))?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("building client verifier: {}", e))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, key)
            .map_err(|e| format!("server config (mTLS): {}", e))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| format!("server config: {}", e))?
    };

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        format!("{}/tests/fixtures/tls/{}", env!("CARGO_MANIFEST_DIR"), name)
    }

    fn tls(cert: &str, key: &str) -> TlsConfig {
        TlsConfig {
            enabled: true,
            cert_path: fixture(cert),
            key_path: fixture(key),
            ca_path: None,
            require_client_cert: false,
        }
    }

    fn err(cfg: &TlsConfig) -> String {
        match build_tls_acceptor(cfg) {
            Ok(_) => panic!("expected an error for {cfg:?}"),
            Err(e) => e,
        }
    }

    #[test]
    fn accepts_every_supported_private_key_encoding() {
        // PKCS#8 ("PRIVATE KEY"), SEC1 ("EC PRIVATE KEY"), PKCS#1 ("RSA PRIVATE KEY").
        build_tls_acceptor(&tls("server-ec.pem", "server-ec.pkcs8.pem")).expect("PKCS#8 EC");
        build_tls_acceptor(&tls("server-ec.pem", "server-ec.sec1.pem")).expect("SEC1 EC");
        build_tls_acceptor(&tls("server-rsa.pem", "server-rsa.pkcs1.pem")).expect("PKCS#1 RSA");
    }

    #[test]
    fn missing_files_name_the_path() {
        let e = err(&tls("does-not-exist.pem", "server-ec.pkcs8.pem"));
        assert!(
            e.starts_with("reading cert") && e.contains("does-not-exist.pem"),
            "{e}"
        );
        let e = err(&tls("server-ec.pem", "does-not-exist.pem"));
        assert!(
            e.starts_with("reading key") && e.contains("does-not-exist.pem"),
            "{e}"
        );
    }

    #[test]
    fn files_without_the_expected_pem_items_are_rejected() {
        // A key file is not a certificate chain, and a certificate is not a key.
        let e = err(&tls("server-ec.pkcs8.pem", "server-ec.pkcs8.pem"));
        assert!(e.starts_with("no certificates found"), "{e}");
        let e = err(&tls("server-ec.pem", "server-ec.pem"));
        assert!(e.starts_with("no private key found"), "{e}");
        // Non-PEM content yields no items rather than a parse panic.
        let e = err(&tls("README.md", "server-ec.pkcs8.pem"));
        assert!(e.starts_with("no certificates found"), "{e}");
    }

    #[test]
    fn a_key_that_does_not_match_the_certificate_is_rejected() {
        let e = err(&tls("server-rsa.pem", "server-ec.pkcs8.pem"));
        assert!(e.starts_with("server config"), "{e}");
    }

    #[test]
    fn client_certificate_verification_needs_a_ca() {
        let mut cfg = tls("server-ec.pem", "server-ec.pkcs8.pem");
        cfg.require_client_cert = true;
        let e = err(&cfg);
        assert!(e.contains("ca_path is missing"), "{e}");

        cfg.ca_path = Some(fixture("ca.pem"));
        build_tls_acceptor(&cfg).expect("mTLS with a CA bundle");

        cfg.ca_path = Some(fixture("server-ec.pkcs8.pem"));
        let e = err(&cfg);
        assert!(e.starts_with("building client verifier"), "{e}");
    }
}
