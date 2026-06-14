//! Proxy-side SCRAM-SHA-256 **server** authentication.
//!
//! When `[auth] mode = "scram"` is configured, the proxy terminates the
//! client's SCRAM-SHA-256 exchange itself (it becomes the auth boundary)
//! against verifiers loaded from an `auth_file`, instead of relaying the
//! client's credentials straight through to the backend. This is the
//! foundation for cross-client connection pooling (the backend connection
//! is then established independently of the client's auth).
//!
//! The crypto mirrors the (RFC-5802-tested) client state machine in
//! `backend::auth`, reusing its primitives. The state machine here is the
//! server inverse: send server-first (salt/iterations/nonce), receive
//! client-final, verify the `ClientProof`, return server-final.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use crate::backend::auth::{hmac_sha256, pbkdf2_hmac_sha256, sha256};

/// A SCRAM-SHA-256 verifier for one user: everything needed to validate a
/// `ClientProof` without knowing the plaintext password.
#[derive(Debug, Clone)]
pub struct ScramVerifier {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramVerifier {
    /// Derive a verifier from a plaintext password (salt + iterations
    /// chosen here). Used for `auth_file` entries that store a plaintext
    /// secret rather than a pre-computed `SCRAM-SHA-256$...` verifier.
    pub fn from_password(password: &str, salt: Vec<u8>, iterations: u32) -> Self {
        let salted = pbkdf2_hmac_sha256(password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted, b"Server Key");
        Self { salt, iterations, stored_key, server_key }
    }

    /// Parse a PostgreSQL-format verifier string:
    /// `SCRAM-SHA-256$<iter>:<salt_b64>$<StoredKey_b64>:<ServerKey_b64>`
    /// (this is exactly `pg_authid.rolpassword` for SCRAM users).
    pub fn parse(s: &str) -> Option<Self> {
        let rest = s.strip_prefix("SCRAM-SHA-256$")?;
        let (params, keys) = rest.split_once('$')?;
        let (iter_str, salt_b64) = params.split_once(':')?;
        let (stored_b64, server_b64) = keys.split_once(':')?;
        let iterations: u32 = iter_str.parse().ok()?;
        let salt = BASE64.decode(salt_b64.trim()).ok()?;
        let stored = BASE64.decode(stored_b64.trim()).ok()?;
        let server = BASE64.decode(server_b64.trim()).ok()?;
        if stored.len() != 32 || server.len() != 32 {
            return None;
        }
        let mut stored_key = [0u8; 32];
        stored_key.copy_from_slice(&stored);
        let mut server_key = [0u8; 32];
        server_key.copy_from_slice(&server);
        Some(Self { salt, iterations, stored_key, server_key })
    }
}

/// Map of username -> verifier, loaded from an `auth_file`.
///
/// File format, one entry per line (`#` comments and blank lines ignored):
/// `username:secret` where `secret` is either a plaintext password or a
/// `SCRAM-SHA-256$...` verifier string. Quoted pgbouncer-style values are
/// accepted (surrounding double quotes are stripped).
#[derive(Debug, Clone, Default)]
pub struct AuthFile {
    users: HashMap<String, ScramVerifier>,
}

impl AuthFile {
    pub fn load(path: &str) -> Result<Self, String> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| format!("reading auth_file {}: {}", path, e))?;
        Self::parse_str(&data, path)
    }

    pub fn parse_str(data: &str, path: &str) -> Result<Self, String> {
        let mut users = HashMap::new();
        for (lineno, raw) in data.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (user, secret) = line
                .split_once(':')
                .ok_or_else(|| format!("{}:{}: expected `user:secret`", path, lineno + 1))?;
            let user = unquote(user.trim());
            let secret = unquote(secret.trim());
            let verifier = if secret.starts_with("SCRAM-SHA-256$") {
                ScramVerifier::parse(&secret).ok_or_else(|| {
                    format!("{}:{}: malformed SCRAM verifier", path, lineno + 1)
                })?
            } else {
                // Plaintext: derive a verifier with a fixed salt derived
                // from the username (stable across restarts so the same
                // client password always validates) and 4096 iterations.
                let salt = sha256(user.as_bytes())[..16].to_vec();
                ScramVerifier::from_password(&secret, salt, 4096)
            };
            users.insert(user, verifier);
        }
        Ok(Self { users })
    }

    pub fn get(&self, user: &str) -> Option<&ScramVerifier> {
        self.users.get(user)
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    if t.len() >= 2 && t.starts_with('"') && t.ends_with('"') {
        t[1..t.len() - 1].to_string()
    } else {
        t.to_string()
    }
}

/// Server-side SCRAM-SHA-256 state machine. One per client handshake.
pub struct ScramServer {
    verifier: ScramVerifier,
    combined_nonce: String,
    client_first_bare: String,
    server_first: String,
}

impl ScramServer {
    /// Begin the exchange from the client's first message (the
    /// SASLInitialResponse payload, e.g. `n,,n=,r=<clientnonce>`).
    /// `server_nonce` must be a fresh random token. Returns the
    /// `server-first` message to send back (AuthenticationSASLContinue).
    pub fn start(
        verifier: ScramVerifier,
        client_first: &str,
        server_nonce: &str,
    ) -> Result<(Self, String), String> {
        // Strip the gs2 header ("n,," / "y,," / "p=...,,"): the bare part
        // is everything after the second comma.
        let mut parts = client_first.splitn(3, ',');
        let _gs2_cbind = parts.next();
        let _gs2_authzid = parts.next();
        let bare = parts
            .next()
            .ok_or_else(|| "malformed client-first (no bare part)".to_string())?;

        let client_nonce = bare
            .split(',')
            .find_map(|f| f.strip_prefix("r="))
            .ok_or_else(|| "client-first missing r=".to_string())?;
        if client_nonce.is_empty() {
            return Err("empty client nonce".to_string());
        }

        let combined_nonce = format!("{}{}", client_nonce, server_nonce);
        let salt_b64 = BASE64.encode(&verifier.salt);
        let server_first = format!("r={},s={},i={}", combined_nonce, salt_b64, verifier.iterations);

        Ok((
            Self {
                verifier,
                combined_nonce,
                client_first_bare: bare.to_string(),
                server_first: server_first.clone(),
            },
            server_first,
        ))
    }

    /// Verify the client-final message (`c=<cb>,r=<nonce>,p=<proof>`).
    /// On success returns the `server-final` message (`v=<sig>`) to send
    /// in AuthenticationSASLFinal.
    pub fn finish(&self, client_final: &str) -> Result<String, String> {
        // Split off the trailing ",p=<proof>".
        let proof_pos = client_final
            .rfind(",p=")
            .ok_or_else(|| "client-final missing p=".to_string())?;
        let without_proof = &client_final[..proof_pos];
        let proof_b64 = &client_final[proof_pos + 3..];

        // Nonce echoed back must equal ours.
        let echoed_nonce = without_proof
            .split(',')
            .find_map(|f| f.strip_prefix("r="))
            .ok_or_else(|| "client-final missing r=".to_string())?;
        if echoed_nonce != self.combined_nonce {
            return Err("nonce mismatch".to_string());
        }

        let proof = BASE64
            .decode(proof_b64.trim())
            .map_err(|e| format!("bad proof base64: {}", e))?;
        if proof.len() != 32 {
            return Err("proof wrong length".to_string());
        }

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        // ClientKey       = ClientProof XOR ClientSignature
        // verify H(ClientKey) == StoredKey
        let client_signature = hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes());
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = proof[i] ^ client_signature[i];
        }
        let derived_stored = sha256(&client_key);
        if !constant_time_eq(&derived_stored, &self.verifier.stored_key) {
            return Err("authentication failed (proof mismatch)".to_string());
        }

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_signature = hmac_sha256(&self.verifier.server_key, auth_message.as_bytes());
        Ok(format!("v={}", BASE64.encode(server_signature)))
    }
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::auth::Scram;

    #[test]
    fn parse_pg_verifier_roundtrips_from_password() {
        // Build a verifier from a password, format it PG-style, reparse.
        let v = ScramVerifier::from_password("s3cret", b"0123456789abcdef".to_vec(), 4096);
        let s = format!(
            "SCRAM-SHA-256${}:{}${}:{}",
            v.iterations,
            BASE64.encode(&v.salt),
            BASE64.encode(v.stored_key),
            BASE64.encode(v.server_key),
        );
        let p = ScramVerifier::parse(&s).expect("parses");
        assert_eq!(p.iterations, v.iterations);
        assert_eq!(p.salt, v.salt);
        assert_eq!(p.stored_key, v.stored_key);
        assert_eq!(p.server_key, v.server_key);
    }

    #[test]
    fn full_scram_handshake_client_vs_server() {
        // Drive the tested client (backend::auth::Scram) against our server.
        let password = "correct horse battery staple";
        let verifier = ScramVerifier::from_password(password, b"saltsaltsaltsalt".to_vec(), 4096);

        let (mut client, init) = Scram::client_first("clientNONCE123");
        // init = SASLInitialResponse: mechanism cstring + int32 len + data.
        // Recover the client-first payload (after the mechanism + length).
        let data = &init.0;
        let mech_end = data.iter().position(|&b| b == 0).unwrap() + 1;
        let client_first = std::str::from_utf8(&data[mech_end + 4..]).unwrap();

        let (server, server_first) =
            ScramServer::start(verifier.clone(), client_first, "serverNONCE456").unwrap();

        let client_final = client.client_final(server_first.as_bytes(), password).unwrap();
        let server_final = server.finish(std::str::from_utf8(&client_final.0).unwrap()).unwrap();

        // The client verifies the server signature -> mutual auth complete.
        client.verify_server(server_final.as_bytes()).unwrap();
    }

    #[test]
    fn wrong_password_is_rejected() {
        let verifier = ScramVerifier::from_password("rightpw", b"saltsaltsaltsalt".to_vec(), 4096);
        let (mut client, init) = Scram::client_first("nonceAAA");
        let data = &init.0;
        let mech_end = data.iter().position(|&b| b == 0).unwrap() + 1;
        let client_first = std::str::from_utf8(&data[mech_end + 4..]).unwrap();
        let (server, server_first) =
            ScramServer::start(verifier, client_first, "nonceBBB").unwrap();
        // Client uses the WRONG password.
        let client_final = client.client_final(server_first.as_bytes(), "wrongpw").unwrap();
        let res = server.finish(std::str::from_utf8(&client_final.0).unwrap());
        assert!(res.is_err(), "wrong password must be rejected");
    }

    #[test]
    fn auth_file_parses_plaintext_and_verifier() {
        let v = ScramVerifier::from_password("pw", b"0123456789abcdef".to_vec(), 4096);
        let verifier_line = format!(
            "carol:SCRAM-SHA-256${}:{}${}:{}",
            v.iterations,
            BASE64.encode(&v.salt),
            BASE64.encode(v.stored_key),
            BASE64.encode(v.server_key),
        );
        let body = format!("# comment\nalice:secret\n\nbob:\"quoted\"\n{}\n", verifier_line);
        let af = AuthFile::parse_str(&body, "test").unwrap();
        assert!(af.get("alice").is_some());
        assert!(af.get("bob").is_some());
        assert!(af.get("carol").is_some());
        assert!(af.get("dave").is_none());
    }
}
