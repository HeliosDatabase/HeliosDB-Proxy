//! A pluggable I/O stream — plain TCP or TLS-wrapped TCP.
//!
//! Used by the backend client so the rest of the module code (auth,
//! query, etc.) stays ignorant of whether TLS is on. Implements
//! `AsyncRead`/`AsyncWrite` by delegating to the inner variant.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// A backend connection stream.
#[allow(clippy::large_enum_variant)]
pub enum Stream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Stream {
    /// Expose the peer address if available (best-effort — TLS hides it).
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Stream::Plain(s) => s.peer_addr(),
            Stream::Tls(s) => s.get_ref().0.peer_addr(),
        }
    }

    /// Return `true` if this connection is encrypted.
    pub fn is_tls(&self) -> bool {
        matches!(self, Stream::Tls(_))
    }

    /// Best-effort, non-blocking liveness probe of the underlying TCP
    /// socket. Peeks (never consumes) at most one byte:
    ///
    /// * `Pending` / `WouldBlock` — socket is quiet: **alive**.
    /// * `Ok(0)` — the peer sent FIN: **dead**.
    /// * `Ok(n > 0)` — unread bytes are already buffered. For an *idle*
    ///   pooled connection this means either an asynchronous server frame
    ///   or protocol desync; handing such a socket out would corrupt the
    ///   next query, so it is reported **dead** (the caller recycles it).
    /// * `Err(_)` — socket in an error state: **dead**.
    ///
    /// Never blocks and never awaits, so it is cheap enough to run on
    /// every checkout.
    pub fn is_probably_alive(&self) -> bool {
        let tcp: &TcpStream = match self {
            Stream::Plain(s) => s,
            Stream::Tls(s) => s.get_ref().0,
        };
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut scratch = [0u8; 1];
        let mut buf = ReadBuf::new(&mut scratch);
        // `Pending` means nothing readable and no FIN — the socket is quiet
        // and usable. Every `Ready` outcome makes it unusable: `Ok(0)` is
        // FIN, `Ok(n > 0)` is unread/desynced bytes, `Err(_)` is a broken
        // socket.
        tcp.poll_peek(&mut cx, &mut buf).is_pending()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Stream::Tls(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Stream::Plain(s) => s.is_write_vectored(),
            Stream::Tls(s) => s.is_write_vectored(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A quiet, connected socket is alive; once the peer closes it the
    /// probe must report dead. This is the primitive the connection pool
    /// uses to avoid handing out a socket the backend closed while idle.
    #[tokio::test]
    async fn test_is_probably_alive_detects_peer_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        let (server, _) = listener.accept().await.expect("accept");

        let stream = Stream::Plain(client);
        assert!(
            stream.is_probably_alive(),
            "quiet connected socket must read as alive"
        );

        drop(server);
        // Give the FIN time to arrive on the loopback interface.
        for _ in 0..50 {
            if !stream.is_probably_alive() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            !stream.is_probably_alive(),
            "socket closed by the peer must read as dead"
        );
    }
}
