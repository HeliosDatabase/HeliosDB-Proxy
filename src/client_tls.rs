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
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let cert_chain: Vec<CertificateDer<'static>> = {
        let data = std::fs::read(&tls.cert_path)
            .map_err(|e| format!("reading cert {}: {}", tls.cert_path, e))?;
        rustls_pemfile::certs(&mut &data[..])
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parsing cert {}: {}", tls.cert_path, e))?
    };
    if cert_chain.is_empty() {
        return Err(format!("no certificates found in {}", tls.cert_path));
    }

    let key: PrivateKeyDer<'static> = {
        let data = std::fs::read(&tls.key_path)
            .map_err(|e| format!("reading key {}: {}", tls.key_path, e))?;
        rustls_pemfile::private_key(&mut &data[..])
            .map_err(|e| format!("parsing key {}: {}", tls.key_path, e))?
            .ok_or_else(|| format!("no private key found in {}", tls.key_path))?
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
        for ca in rustls_pemfile::certs(&mut &ca_data[..]) {
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
