//! Plugin registry resolution + one-command install (Batch H, item 78).
//!
//! The plugin runtime already has the whole trust pipeline — Ed25519 trust
//! roots, SHA-256 manifests, a hot-reloading plugin directory. The only missing
//! verb was *distribution*: getting a signed artefact from a catalog onto disk
//! where hot-reload picks it up. This module is that verb.
//!
//! A registry is a JSON index listing `{name, version, artifact, sha256,
//! signature?}`. `install` resolves an entry, fetches the artefact, checks its
//! SHA-256 against the index, optionally verifies an Ed25519 signature against a
//! trust root (reusing [`SignatureVerifier`]), and drops `<name>.wasm` (plus a
//! `<name>.sig` sidecar when signed) into the destination plugins directory.
//!
//! This offline slice resolves **local / `file://` artefacts** — exactly what a
//! private registry on a shared filesystem or an air-gapped mirror needs, and
//! what is testable without a network. Resolving `https://` artefacts (a public
//! registry over GitHub Releases) is a thin follow-on at the fetch step.

use std::path::{Path, PathBuf};

/// A registry index file: a flat list of installable plugin artefacts.
#[derive(Debug, serde::Deserialize)]
pub struct RegistryIndex {
    #[serde(default)]
    pub schema_version: String,
    pub plugins: Vec<RegistryEntry>,
}

/// One installable artefact in a [`RegistryIndex`].
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegistryEntry {
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub description: String,
    /// Artefact location: a path relative to the index file, an absolute path,
    /// or a `file://` URL. (`https://` is rejected in this offline slice.)
    pub artifact: String,
    /// Lowercase hex SHA-256 of the artefact bytes.
    pub sha256: String,
    /// Base64 of the raw 64-byte Ed25519 signature over the artefact bytes.
    /// Required when installing with a trust root.
    #[serde(default)]
    pub signature: Option<String>,
}

/// What an [`install`] produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallReport {
    pub name: String,
    pub version: String,
    pub wasm_path: PathBuf,
    pub sig_path: Option<PathBuf>,
    pub sha256: String,
    /// Trust-root key label that verified the signature, when verified.
    pub signed_by: Option<String>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in digest.iter() {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Verify an Ed25519 signature (base64 of the raw 64-byte signature) over
/// `bytes` against any `*.pub` key in `trust_root` (each a base64 of a raw
/// 32-byte Ed25519 public key — the same trust-root format the plugin loader
/// uses). Returns the matching key's label. Self-contained so `install` needs
/// no WASM runtime / `wasm-plugins` feature.
fn verify_against_trust_root(bytes: &[u8], sig_b64: &str, trust_root: &Path) -> Result<String, String> {
    use base64::Engine as _;
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    let b64 = base64::engine::general_purpose::STANDARD;
    let sig_bytes = b64
        .decode(sig_b64.trim())
        .map_err(|e| format!("decode signature: {e}"))?;
    let sig_arr: [u8; 64] = sig_bytes
        .as_slice()
        .try_into()
        .map_err(|_| format!("signature must be 64 bytes, got {}", sig_bytes.len()))?;
    let sig = Signature::from_bytes(&sig_arr);

    let mut any = false;
    let entries = std::fs::read_dir(trust_root)
        .map_err(|e| format!("read trust root {}: {e}", trust_root.display()))?;
    for ent in entries.flatten() {
        let p = ent.path();
        if p.extension().and_then(|e| e.to_str()) != Some("pub") {
            continue;
        }
        let raw = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
        let kb = b64
            .decode(raw.trim())
            .map_err(|e| format!("decode {}: {e}", p.display()))?;
        let karr: [u8; 32] = kb
            .as_slice()
            .try_into()
            .map_err(|_| format!("{} must hold a 32-byte pubkey", p.display()))?;
        let vk =
            VerifyingKey::from_bytes(&karr).map_err(|e| format!("{} invalid pubkey: {e}", p.display()))?;
        any = true;
        if vk.verify(bytes, &sig).is_ok() {
            let label = p.file_stem().and_then(|s| s.to_str()).unwrap_or("?").to_string();
            return Ok(label);
        }
    }
    if !any {
        return Err(format!("trust root {} has no *.pub keys", trust_root.display()));
    }
    Err("signature does not match any trusted key".to_string())
}

/// Parse a registry index from disk.
pub fn load_index(index_path: &Path) -> Result<RegistryIndex, String> {
    let raw = std::fs::read_to_string(index_path)
        .map_err(|e| format!("read registry index {}: {}", index_path.display(), e))?;
    serde_json::from_str(&raw).map_err(|e| format!("parse registry index: {}", e))
}

/// Resolve an entry's `artifact` field to a local path. Local/relative/`file://`
/// only — `https://` is an explicit error in the offline slice.
fn resolve_artifact_path(index_path: &Path, artifact: &str) -> Result<PathBuf, String> {
    if artifact.starts_with("http://") || artifact.starts_with("https://") {
        return Err(format!(
            "artefact {artifact} is remote; this build installs only local/file:// artefacts"
        ));
    }
    let raw = artifact.strip_prefix("file://").unwrap_or(artifact);
    let p = Path::new(raw);
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        // Relative paths resolve against the index file's directory.
        let base = index_path.parent().unwrap_or_else(|| Path::new("."));
        Ok(base.join(p))
    }
}

/// Find an entry by name (optionally pinned to an exact version).
pub fn find_entry<'a>(
    index: &'a RegistryIndex,
    name: &str,
    version: Option<&str>,
) -> Result<&'a RegistryEntry, String> {
    let mut matches = index.plugins.iter().filter(|e| e.name == name);
    match version {
        Some(v) => matches
            .find(|e| e.version == v)
            .ok_or_else(|| format!("no plugin '{name}' at version '{v}' in registry")),
        None => matches.next().ok_or_else(|| format!("no plugin '{name}' in registry")),
    }
}

/// Install a plugin from the registry into `dest_dir`.
///
/// Verifies the SHA-256 against the index; when `trust_root` is given the entry
/// MUST carry a signature and it is verified against the trust root. On success
/// writes `<name>.wasm` (+ `<name>.sig` when signed) into `dest_dir`.
pub fn install(
    index_path: &Path,
    name: &str,
    version: Option<&str>,
    dest_dir: &Path,
    trust_root: Option<&Path>,
) -> Result<InstallReport, String> {
    let index = load_index(index_path)?;
    let entry = find_entry(&index, name, version)?.clone();

    let artifact_path = resolve_artifact_path(index_path, &entry.artifact)?;
    let bytes = std::fs::read(&artifact_path)
        .map_err(|e| format!("read artefact {}: {}", artifact_path.display(), e))?;

    // Integrity: the bytes must match the SHA-256 the index advertises.
    let actual = sha256_hex(&bytes);
    if !actual.eq_ignore_ascii_case(&entry.sha256) {
        return Err(format!(
            "sha256 mismatch for '{name}': index={} actual={actual}",
            entry.sha256
        ));
    }

    // Authenticity: when a trust root is configured, require + verify a
    // signature against it.
    let mut signed_by = None;
    if let Some(root) = trust_root {
        let sig = entry.signature.as_deref().ok_or_else(|| {
            format!("'{name}' has no signature but a trust root was supplied")
        })?;
        let label = verify_against_trust_root(&bytes, sig, root)
            .map_err(|e| format!("signature verification failed for '{name}': {e}"))?;
        signed_by = Some(label);
    }

    std::fs::create_dir_all(dest_dir)
        .map_err(|e| format!("create dest dir {}: {}", dest_dir.display(), e))?;
    let wasm_path = dest_dir.join(format!("{name}.wasm"));
    std::fs::write(&wasm_path, &bytes)
        .map_err(|e| format!("write {}: {}", wasm_path.display(), e))?;

    let sig_path = if let Some(sig) = entry.signature.as_deref() {
        let p = dest_dir.join(format!("{name}.sig"));
        std::fs::write(&p, sig).map_err(|e| format!("write {}: {}", p.display(), e))?;
        Some(p)
    } else {
        None
    };

    Ok(InstallReport {
        name: entry.name,
        version: entry.version,
        wasm_path,
        sig_path,
        sha256: actual,
        signed_by,
    })
}

/// What a [`verify`] produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    pub sha256: String,
    /// Trust-root key label that verified the signature, when a trust root was
    /// supplied (otherwise `None` — only the digest was computed).
    pub signed_by: Option<String>,
}

/// Verify a local plugin artefact already on disk (a pre-deploy / audit check,
/// distinct from `install`): compute its SHA-256 and, when a trust root is
/// given, check its Ed25519 signature. The signature is read from `sig_path`,
/// or a `<name>.sig` sidecar next to the artefact (the convention the loader
/// uses: `path.with_extension("sig")`).
pub fn verify(
    wasm_path: &Path,
    trust_root: Option<&Path>,
    sig_path: Option<&Path>,
) -> Result<VerifyReport, String> {
    let bytes = std::fs::read(wasm_path)
        .map_err(|e| format!("read {}: {}", wasm_path.display(), e))?;
    let sha256 = sha256_hex(&bytes);

    let mut signed_by = None;
    if let Some(root) = trust_root {
        let sig_file = match sig_path {
            Some(p) => p.to_path_buf(),
            None => wasm_path.with_extension("sig"),
        };
        let sig = std::fs::read_to_string(&sig_file)
            .map_err(|e| format!("read signature {}: {}", sig_file.display(), e))?;
        let label = verify_against_trust_root(&bytes, sig.trim(), root)
            .map_err(|e| format!("signature verification failed: {e}"))?;
        signed_by = Some(label);
    }
    Ok(VerifyReport { sha256, signed_by })
}

/// Scaffold a new plugin source skeleton under `dir/<name>/`.
///
/// Writes a `plugin.yaml` manifest, a minimal Rust `src/lib.rs` hook stub, and a
/// README so `helios-plugin new <name>` gives a buildable starting point.
pub fn scaffold(name: &str, dir: &Path) -> Result<PathBuf, String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!("invalid plugin name '{name}' (use [A-Za-z0-9_-])"));
    }
    let root = dir.join(name);
    if root.exists() {
        return Err(format!("{} already exists", root.display()));
    }
    std::fs::create_dir_all(root.join("src"))
        .map_err(|e| format!("create {}: {}", root.display(), e))?;

    let manifest = format!(
        "name: {name}\nversion: 0.1.0\ndescription: A HeliosProxy plugin\nlicense: Apache-2.0\nhooks:\n  - pre_query\npermissions: []\n"
    );
    std::fs::write(root.join("plugin.yaml"), manifest)
        .map_err(|e| format!("write plugin.yaml: {e}"))?;

    let lib_rs = "// Minimal HeliosProxy WASM plugin stub.\n// Build to wasm32-unknown-unknown, then `helios-plugin` pack + sign.\n//\n// Export the hooks named in plugin.yaml; the host calls pre_query(ptr,len)\n// before forwarding a query. Return 0 to allow, non-zero to block.\n#[no_mangle]\npub extern \"C\" fn pre_query(_ptr: i32, _len: i32) -> i32 {\n    0 // allow\n}\n";
    std::fs::write(root.join("src/lib.rs"), lib_rs)
        .map_err(|e| format!("write src/lib.rs: {e}"))?;

    let readme = format!(
        "# {name}\n\nA HeliosProxy WASM plugin.\n\n## Build\n\n```\ncargo build --release --target wasm32-unknown-unknown\n```\n\nThen pack + sign the resulting `.wasm` and add it to your registry index so\n`helios-plugin install {name}` can deploy it.\n"
    );
    std::fs::write(root.join("README.md"), readme)
        .map_err(|e| format!("write README.md: {e}"))?;

    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};

    const WASM: &[u8] = b"\x00asm\x01\x00\x00\x00pretend-real-plugin-wasm";

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// Write an artefact + a registry index referencing it (relative path).
    fn make_registry(dir: &Path, signature: Option<&str>) -> PathBuf {
        std::fs::write(dir.join("colmask.wasm"), WASM).unwrap();
        let sig_field = signature
            .map(|s| format!(",\n      \"signature\": \"{s}\""))
            .unwrap_or_default();
        let index = format!(
            "{{\n  \"schema_version\": \"1\",\n  \"plugins\": [\n    {{\n      \"name\": \"colmask\",\n      \"version\": \"0.1.0\",\n      \"artifact\": \"colmask.wasm\",\n      \"sha256\": \"{}\"{sig_field}\n    }}\n  ]\n}}",
            sha256_hex(WASM)
        );
        let index_path = dir.join("index.json");
        std::fs::write(&index_path, index).unwrap();
        index_path
    }

    #[test]
    fn install_unsigned_lands_wasm() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let index = make_registry(src.path(), None);

        let r = install(&index, "colmask", None, dst.path(), None).unwrap();
        assert_eq!(r.name, "colmask");
        assert!(r.wasm_path.exists());
        assert!(r.sig_path.is_none());
        assert!(r.signed_by.is_none());
        assert_eq!(std::fs::read(&r.wasm_path).unwrap(), WASM);
    }

    #[test]
    fn install_rejects_sha256_mismatch() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        // Index claims the right hash, then we corrupt the artefact on disk.
        let index = make_registry(src.path(), None);
        std::fs::write(src.path().join("colmask.wasm"), b"tampered").unwrap();

        let err = install(&index, "colmask", None, dst.path(), None).unwrap_err();
        assert!(err.contains("sha256 mismatch"), "{err}");
    }

    #[test]
    fn install_verifies_signature_against_trust_root() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();

        // Trusted publisher signs the artefact.
        let key = SigningKey::from_bytes(&[7u8; 32]);
        std::fs::write(trust.path().join("official.pub"), b64(&key.verifying_key().to_bytes()))
            .unwrap();
        let sig = b64(&key.sign(WASM).to_bytes());
        let index = make_registry(src.path(), Some(&sig));

        let r = install(&index, "colmask", None, dst.path(), Some(trust.path())).unwrap();
        assert_eq!(r.signed_by.as_deref(), Some("official"));
        assert!(r.sig_path.as_ref().unwrap().exists());
    }

    #[test]
    fn install_rejects_untrusted_signature() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();

        // Trust root holds the official key; an attacker signs with another.
        let official = SigningKey::from_bytes(&[7u8; 32]);
        std::fs::write(trust.path().join("official.pub"), b64(&official.verifying_key().to_bytes()))
            .unwrap();
        let attacker = SigningKey::from_bytes(&[0xABu8; 32]);
        let sig = b64(&attacker.sign(WASM).to_bytes());
        let index = make_registry(src.path(), Some(&sig));

        let err = install(&index, "colmask", None, dst.path(), Some(trust.path())).unwrap_err();
        assert!(err.contains("signature verification failed"), "{err}");
    }

    #[test]
    fn install_requires_signature_when_trust_root_set() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7u8; 32]);
        std::fs::write(trust.path().join("official.pub"), b64(&key.verifying_key().to_bytes()))
            .unwrap();
        let index = make_registry(src.path(), None); // unsigned entry

        let err = install(&index, "colmask", None, dst.path(), Some(trust.path())).unwrap_err();
        assert!(err.contains("no signature"), "{err}");
    }

    #[test]
    fn rejects_remote_artifact_offline() {
        let p = resolve_artifact_path(Path::new("/tmp/index.json"), "https://example/colmask.wasm");
        assert!(p.unwrap_err().contains("remote"));
    }

    #[test]
    fn verify_digest_only_without_trust_root() {
        let dir = tempfile::tempdir().unwrap();
        let wasm = dir.path().join("p.wasm");
        std::fs::write(&wasm, WASM).unwrap();
        let r = verify(&wasm, None, None).unwrap();
        assert_eq!(r.sha256, sha256_hex(WASM));
        assert!(r.signed_by.is_none());
    }

    #[test]
    fn verify_signature_via_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7u8; 32]);
        std::fs::write(trust.path().join("official.pub"), b64(&key.verifying_key().to_bytes()))
            .unwrap();
        let wasm = dir.path().join("p.wasm");
        std::fs::write(&wasm, WASM).unwrap();
        // `p.wasm` -> `p.sig` sidecar (with_extension), matching the loader.
        std::fs::write(dir.path().join("p.sig"), b64(&key.sign(WASM).to_bytes())).unwrap();

        let r = verify(&wasm, Some(trust.path()), None).unwrap();
        assert_eq!(r.signed_by.as_deref(), Some("official"));
    }

    #[test]
    fn verify_rejects_tampered_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7u8; 32]);
        std::fs::write(trust.path().join("official.pub"), b64(&key.verifying_key().to_bytes()))
            .unwrap();
        let wasm = dir.path().join("p.wasm");
        // Sign the real bytes, but write tampered bytes to disk.
        std::fs::write(dir.path().join("p.sig"), b64(&key.sign(WASM).to_bytes())).unwrap();
        std::fs::write(&wasm, b"tampered-wasm").unwrap();

        let err = verify(&wasm, Some(trust.path()), None).unwrap_err();
        assert!(err.contains("signature verification failed"), "{err}");
    }

    #[test]
    fn scaffold_creates_skeleton() {
        let dir = tempfile::tempdir().unwrap();
        let root = scaffold("my-plugin", dir.path()).unwrap();
        assert!(root.join("plugin.yaml").exists());
        assert!(root.join("src/lib.rs").exists());
        assert!(root.join("README.md").exists());
        // Reject invalid names + double-create.
        assert!(scaffold("bad name", dir.path()).is_err());
        assert!(scaffold("my-plugin", dir.path()).is_err());
    }
}
