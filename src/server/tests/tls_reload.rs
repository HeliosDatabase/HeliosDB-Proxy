//! `[tls]` follows a SIGHUP reload (967e1db11c29, certificate rotation): new
//! connections get the rebuilt acceptor, a file that fails to load keeps the
//! running one, and the section can be switched on or off without a restart.
use super::*;
use crate::config::TlsConfig;

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/tls/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn tls(cert: &str, key: &str) -> Option<TlsConfig> {
    Some(TlsConfig {
        enabled: true,
        cert_path: fixture(cert),
        key_path: fixture(key),
        ca_path: None,
        require_client_cert: false,
    })
}

fn config_with(tls: Option<TlsConfig>) -> ProxyConfig {
    let mut config = ProxyConfig {
        listen_address: "127.0.0.1:0".to_string(),
        ..Default::default()
    };
    config.add_node("127.0.0.1:5432", "primary").unwrap();
    config.tls = tls;
    config
}

fn acceptor(server: &ProxyServer) -> Option<Arc<tokio_rustls::TlsAcceptor>> {
    server.state.tls_acceptor.load_full()
}

#[test]
fn rotation_swaps_the_acceptor_and_a_bad_file_keeps_it() {
    let server =
        ProxyServer::new(config_with(tls("server-rsa.pem", "server-rsa.pkcs1.pem"))).unwrap();
    let first = acceptor(&server).expect("TLS enabled at startup");

    // Rotation: a different certificate and key -> a new acceptor.
    let rotated = config_with(tls("server-ec.pem", "server-ec.pkcs8.pem"));
    assert!(ProxyServer::reload_client_tls(&server.state, &rotated));
    let second = acceptor(&server).expect("still enabled");
    assert!(!Arc::ptr_eq(&first, &second), "the acceptor was rebuilt");

    // A key that fails to load keeps the running acceptor.
    let broken = config_with(tls("server-ec.pem", "missing.pem"));
    assert!(!ProxyServer::reload_client_tls(&server.state, &broken));
    let kept = acceptor(&server).expect("still enabled after a bad reload");
    assert!(
        Arc::ptr_eq(&second, &kept),
        "a bad rotation never drops TLS"
    );

    // Switching the section off takes effect for new connections.
    assert!(ProxyServer::reload_client_tls(
        &server.state,
        &config_with(None)
    ));
    assert!(acceptor(&server).is_none());

    // And back on.
    let again = config_with(tls("server-rsa.pem", "server-rsa.pkcs1.pem"));
    assert!(ProxyServer::reload_client_tls(&server.state, &again));
    assert!(acceptor(&server).is_some());
}

#[test]
fn a_server_without_tls_stays_plaintext_across_a_reload() {
    let server = ProxyServer::new(config_with(None)).unwrap();
    assert!(acceptor(&server).is_none());
    assert!(!ProxyServer::reload_client_tls(
        &server.state,
        &config_with(None)
    ));
    assert!(acceptor(&server).is_none());
    let mut disabled = config_with(tls("server-rsa.pem", "server-rsa.pkcs1.pem"));
    disabled.tls.as_mut().unwrap().enabled = false;
    assert!(!ProxyServer::reload_client_tls(&server.state, &disabled));
    assert!(acceptor(&server).is_none(), "enabled = false is off");
}
