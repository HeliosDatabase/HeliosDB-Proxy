//! PostgreSQL client-side authentication helpers.
//!
//! Covers the two mechanisms we need today:
//! - **MD5** (AuthenticationMD5Password, request code 5). Legacy but
//!   still widely deployed. Payload is
//!   `"md5" + hex(md5(hex(md5(password + username)) + salt))`.
//! - **SCRAM-SHA-256** (AuthenticationSASL, mechanism
//!   `SCRAM-SHA-256`, request code 10). The current PG default.
//!
//! Both implementations verify the server's end of the handshake where
//! the protocol allows it — MD5 has no server-side verifier, SCRAM does
//! (the server-final message includes `v=<server-signature>`).

use super::error::{BackendError, BackendResult};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------
// MD5 authentication
// ---------------------------------------------------------------------

/// Compute the response payload for `AuthenticationMD5Password`.
///
/// Returns the complete `PasswordMessage` payload (the null-terminated
/// string the client sends back), excluding the tag and length prefix
/// — the caller frames it.
pub fn md5_password_response(user: &str, password: &str, salt: &[u8; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(35 + 1);
    let inner = md5_hex(format!("{}{}", password, user).as_bytes());
    let mut salted = Vec::with_capacity(inner.len() + 4);
    salted.extend_from_slice(inner.as_bytes());
    salted.extend_from_slice(salt);
    out.extend_from_slice(b"md5");
    out.extend_from_slice(md5_hex(&salted).as_bytes());
    out.push(0);
    out
}

fn md5_hex(bytes: &[u8]) -> String {
    let digest = md5::Md5::digest(bytes);
    let mut s = String::with_capacity(digest.len() * 2);
    for b in digest {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

// ---------------------------------------------------------------------
// SCRAM-SHA-256
// ---------------------------------------------------------------------

/// SCRAM client state machine. Create with `Scram::client_first`, feed
/// the server-first into `client_final`, and feed the server-final into
/// `verify_server`.
pub struct Scram {
    /// Cached for HMAC in server-signature check.
    client_first_bare: String,
    /// Saved for AuthMessage construction.
    nonce: String,
    /// Computed after `client_final`; used to verify server signature.
    server_key: [u8; 32],
    /// Full AuthMessage computed after client_final.
    auth_message: String,
    /// Whether `client_final` ran (guards `verify_server`).
    finalised: bool,
}

/// Result of one SCRAM step: the opaque bytes to send to the server.
#[derive(Debug)]
pub struct ScramMessage(pub Vec<u8>);

impl Scram {
    /// Build the SASL initial response for `SCRAM-SHA-256`.
    ///
    /// The returned bytes are the payload of a `PasswordMessage`
    /// (tag `p`). `nonce` must be a unique random string per session —
    /// the caller provides it so the function is testable.
    pub fn client_first(nonce: impl Into<String>) -> (Self, ScramMessage) {
        let nonce = nonce.into();
        // gs2-header is "n,," (no channel binding, no authzid).
        // client-first-bare is n=<user>,r=<nonce>. PG ignores <user>
        // and takes the name from the StartupMessage; we send empty.
        let client_first_bare = format!("n=,r={}", nonce);
        let client_first = format!("n,,{}", client_first_bare);

        // SASL format: mechanism + NUL + 4-byte BE length + bytes.
        let mech = b"SCRAM-SHA-256\0";
        let mut out = Vec::with_capacity(mech.len() + 4 + client_first.len());
        out.extend_from_slice(mech);
        out.extend_from_slice(&(client_first.len() as u32).to_be_bytes());
        out.extend_from_slice(client_first.as_bytes());

        (
            Self {
                client_first_bare,
                nonce,
                server_key: [0u8; 32],
                auth_message: String::new(),
                finalised: false,
            },
            ScramMessage(out),
        )
    }

    /// Consume the server-first message and produce the client-final.
    ///
    /// `server_first` is the raw bytes the server sent (the payload of
    /// an `AuthenticationSASLContinue` frame, minus the 4-byte type
    /// code which the caller strips).
    pub fn client_final(
        &mut self,
        server_first: &[u8],
        password: &str,
    ) -> BackendResult<ScramMessage> {
        let server_first_str = std::str::from_utf8(server_first)
            .map_err(|e| BackendError::Auth(format!("server-first is not UTF-8: {}", e)))?;

        // Parse r=<combined-nonce>,s=<salt-base64>,i=<iteration-count>
        let mut server_nonce = None;
        let mut salt_b64 = None;
        let mut iterations: Option<u32> = None;
        for field in server_first_str.split(',') {
            if let Some(rest) = field.strip_prefix("r=") {
                server_nonce = Some(rest);
            } else if let Some(rest) = field.strip_prefix("s=") {
                salt_b64 = Some(rest);
            } else if let Some(rest) = field.strip_prefix("i=") {
                iterations = rest.parse().ok();
            }
        }
        let server_nonce =
            server_nonce.ok_or_else(|| BackendError::Auth("missing r= in server-first".into()))?;
        let salt_b64 =
            salt_b64.ok_or_else(|| BackendError::Auth("missing s= in server-first".into()))?;
        let iterations = iterations
            .ok_or_else(|| BackendError::Auth("missing/invalid i= in server-first".into()))?;

        // The server must echo the client nonce as a prefix.
        if !server_nonce.starts_with(&self.nonce) {
            return Err(BackendError::Auth(
                "server nonce does not extend client nonce".into(),
            ));
        }
        if iterations < 1 {
            return Err(BackendError::Auth("iteration count must be >= 1".into()));
        }

        let salt = BASE64
            .decode(salt_b64)
            .map_err(|e| BackendError::Auth(format!("bad salt base64: {}", e)))?;

        // Derive keys per RFC 5802.
        let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        self.server_key = hmac_sha256(&salted_password, b"Server Key");

        // channel-binding: "c=" + base64("n,,")
        let channel_binding = BASE64.encode(b"n,,");

        let client_final_without_proof = format!("c={},r={}", channel_binding, server_nonce);
        self.auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first_str, client_final_without_proof
        );

        let client_signature = hmac_sha256(&stored_key, self.auth_message.as_bytes());
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64.encode(client_proof)
        );

        self.finalised = true;
        Ok(ScramMessage(client_final.into_bytes()))
    }

    /// Verify the server-final message's `v=<server-signature>` tag.
    /// Returns `Ok(())` only if the signature matches what we expect
    /// from the derived `server_key`.
    pub fn verify_server(&self, server_final: &[u8]) -> BackendResult<()> {
        if !self.finalised {
            return Err(BackendError::Auth(
                "verify_server called before client_final".into(),
            ));
        }
        let s = std::str::from_utf8(server_final)
            .map_err(|e| BackendError::Auth(format!("server-final is not UTF-8: {}", e)))?;
        // Server may send `e=<error>` on failure.
        if let Some(err) = s.strip_prefix("e=") {
            return Err(BackendError::Auth(format!("server reported: {}", err)));
        }
        let sig_b64 = s
            .strip_prefix("v=")
            .ok_or_else(|| BackendError::Auth("missing v= in server-final".into()))?
            .split(',')
            .next()
            .unwrap_or("");
        let received = BASE64
            .decode(sig_b64)
            .map_err(|e| BackendError::Auth(format!("bad v= base64: {}", e)))?;
        let expected = hmac_sha256(&self.server_key, self.auth_message.as_bytes());
        if received == expected {
            Ok(())
        } else {
            Err(BackendError::Auth("server signature mismatch".into()))
        }
    }
}

// ---- crypto primitives --------------------------------------------------

pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let tag = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&tag);
    out
}

pub(crate) fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iters: u32) -> [u8; 32] {
    // Single-block PBKDF2 (dkLen == hLen == 32) — exactly what SCRAM
    // requires.
    let mut mac = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
    mac.update(salt);
    mac.update(&1u32.to_be_bytes());
    let mut u: [u8; 32] = mac.finalize().into_bytes().into();
    let mut out = u;
    for _ in 1..iters {
        let mut mac = HmacSha256::new_from_slice(password).expect("HMAC accepts any key length");
        mac.update(&u);
        u = mac.finalize().into_bytes().into();
        for i in 0..32 {
            out[i] ^= u[i];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known-answer MD5 auth per PostgreSQL docs:
    /// `concat('md5', md5(md5(password || username) || salt))`.
    #[test]
    fn test_md5_password_response_known_answer() {
        // username = "alice", password = "secret", salt = [0x01,0x02,0x03,0x04]
        let got = md5_password_response("alice", "secret", &[0x01, 0x02, 0x03, 0x04]);
        // Last byte is the cstring terminator.
        assert_eq!(got.last().copied(), Some(0u8));
        let body = std::str::from_utf8(&got[..got.len() - 1]).unwrap();
        assert!(body.starts_with("md5"));
        assert_eq!(body.len(), 3 + 32); // "md5" + 32 hex chars
                                        // Re-derive and compare.
        let inner = md5_hex(b"secretalice");
        let mut combined = inner.into_bytes();
        combined.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        let outer = md5_hex(&combined);
        assert_eq!(&body[3..], outer);
    }

    /// PBKDF2-HMAC-SHA-256 known-answer from RFC 7914 / RFC 5802 test
    /// vectors. (P="password", S="salt", c=1, dkLen=32.)
    #[test]
    fn test_pbkdf2_hmac_sha256_rfc_vector() {
        let got = pbkdf2_hmac_sha256(b"password", b"salt", 1);
        let expected = [
            0x12, 0x0f, 0xb6, 0xcf, 0xfc, 0xf8, 0xb3, 0x2c, 0x43, 0xe7, 0x22, 0x52, 0x56, 0xc4,
            0xf8, 0x37, 0xa8, 0x65, 0x48, 0xc9, 0x2c, 0xcc, 0x35, 0x48, 0x08, 0x05, 0x98, 0x7c,
            0xb7, 0x0b, 0xe1, 0x7b,
        ];
        assert_eq!(got, expected);
    }

    /// Higher iteration count — smoke test that the loop accumulates
    /// correctly. Taken from the same RFC set (c=4096).
    #[test]
    fn test_pbkdf2_hmac_sha256_high_iters() {
        let got = pbkdf2_hmac_sha256(b"password", b"salt", 4096);
        let expected = [
            0xc5, 0xe4, 0x78, 0xd5, 0x92, 0x88, 0xc8, 0x41, 0xaa, 0x53, 0x0d, 0xb6, 0x84, 0x5c,
            0x4c, 0x8d, 0x96, 0x28, 0x93, 0xa0, 0x01, 0xce, 0x4e, 0x11, 0xa4, 0x96, 0x38, 0x73,
            0xaa, 0x98, 0x13, 0x4a,
        ];
        assert_eq!(got, expected);
    }

    /// Full SCRAM-SHA-256 round-trip against a synthetic server that
    /// follows RFC 5802 mechanics with PG-compatible message shape.
    /// This is the end-to-end property test: client_first -> server
    /// crafts server_first -> client_final -> server verifies +
    /// replies server_final -> client verify_server succeeds.
    #[test]
    fn test_scram_roundtrip_against_synthetic_server() {
        // Client nonce.
        let (mut scram, first) = Scram::client_first("fyko+d2lbbFgONRv9qkxdawL");
        // Parse the mechanism header out of client_first:
        // "SCRAM-SHA-256\0<u32 len><bytes>"
        let msg = &first.0;
        let mech_end = msg.iter().position(|&b| b == 0).unwrap();
        assert_eq!(&msg[..mech_end], b"SCRAM-SHA-256");
        let len = u32::from_be_bytes(msg[mech_end + 1..mech_end + 5].try_into().unwrap()) as usize;
        let cfirst = &msg[mech_end + 5..mech_end + 5 + len];
        let cfirst_str = std::str::from_utf8(cfirst).unwrap();
        assert!(cfirst_str.starts_with("n,,n=,r=fyko+d2lbbFgONRv9qkxdawL"));

        // ---- synthetic server ----
        let server_nonce_suffix = "3rfcNHYJY1ZVvWVs7j";
        let combined_nonce = format!("fyko+d2lbbFgONRv9qkxdawL{}", server_nonce_suffix);
        let salt: [u8; 16] = [
            0x41, 0x25, 0xc2, 0x47, 0xe4, 0x3a, 0xb1, 0xe9, 0x3c, 0x6d, 0xff, 0x76, 0xd1, 0x22,
            0x3a, 0x10,
        ];
        let iterations = 4096u32;
        let salt_b64 = BASE64.encode(salt);
        let server_first = format!("r={},s={},i={}", combined_nonce, salt_b64, iterations);

        let password = "pencil";
        let client_final = scram
            .client_final(server_first.as_bytes(), password)
            .expect("client_final");
        let cfinal_str = std::str::from_utf8(&client_final.0).unwrap();

        // Expected pieces present.
        assert!(cfinal_str.starts_with("c=biws,r=")); // base64("n,,") = "biws"
        assert!(cfinal_str.contains(&format!("r={}", combined_nonce)));
        assert!(cfinal_str.contains(",p="));

        // Server-side: derive the same server_key, build AuthMessage from
        // the pieces we know, then sign.
        let salted = pbkdf2_hmac_sha256(password.as_bytes(), &salt, iterations);
        let server_key = hmac_sha256(&salted, b"Server Key");
        let (cfinal_no_proof, _proof) = {
            let idx = cfinal_str.rfind(",p=").unwrap();
            (&cfinal_str[..idx], &cfinal_str[idx + 3..])
        };
        let auth_message = format!(
            "n=,r=fyko+d2lbbFgONRv9qkxdawL,{},{}",
            server_first, cfinal_no_proof
        );
        let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
        let server_final = format!("v={}", BASE64.encode(server_sig));

        // Client verifies.
        scram
            .verify_server(server_final.as_bytes())
            .expect("verify_server");
    }

    #[test]
    fn test_scram_rejects_nonce_mismatch() {
        let (mut scram, _) = Scram::client_first("client-nonce");
        let server_first = "r=OTHER-nonce,s=QUJD,i=4096";
        let err = scram
            .client_final(server_first.as_bytes(), "pw")
            .unwrap_err();
        assert!(matches!(err, BackendError::Auth(_)));
    }

    #[test]
    fn test_scram_rejects_bad_server_signature() {
        let (mut scram, _) = Scram::client_first("abc");
        // Set up with valid server-first so client_final succeeds.
        let server_first = "r=abc-extension,s=QUJD,i=4096";
        let _ = scram.client_final(server_first.as_bytes(), "pw").unwrap();
        // Then fake a server-final with a wrong signature.
        let bad_sig = BASE64.encode([0u8; 32]);
        let server_final = format!("v={}", bad_sig);
        assert!(scram.verify_server(server_final.as_bytes()).is_err());
    }

    #[test]
    fn test_scram_rejects_server_error() {
        let (mut scram, _) = Scram::client_first("abc");
        let server_first = "r=abc-extension,s=QUJD,i=4096";
        let _ = scram.client_final(server_first.as_bytes(), "pw").unwrap();
        assert!(scram.verify_server(b"e=invalid-proof").is_err());
    }
}
