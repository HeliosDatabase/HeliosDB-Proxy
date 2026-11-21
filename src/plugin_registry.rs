//! Plugin registry resolution + one-command install (Batch H, item 78).
//!
//! The plugin runtime already has the whole trust pipeline — Ed25519 trust
//! roots, SHA-256 manifests, a hot-reloading plugin directory. The only missing
//! verb was *distribution*: getting a signed artefact from a catalog onto disk
//! where hot-reload picks it up. This module is that verb.
//!
//! A registry is a JSON index listing `{name, version, artifact, sha256,
//! signature?}`. `install` resolves an entry, fetches the artefact, checks its
//! SHA-256 against the index, optionally verifies an Ed25519 signature over it
//! against a trust root ([`crate::plugin_artefact::TrustRoot`]), and drops it
//! into the destination plugins directory: a packed artefact (gzip) as
//! `<name>.tar.gz`, a bare module as `<name>.wasm` (plus a `<name>.sig`
//! sidecar when the index signs it).
//!
//! Artefacts are resolved from **local / `file://` paths** (a private registry
//! on a shared filesystem or an air-gapped mirror) or fetched over **`http://`**
//! (a mirror, or a localhost TLS-terminating proxy). Because the index is a
//! local trusted file, its `sha256` makes a plain-HTTP fetch integrity-safe — so
//! no TLS stack is pulled in. `https://` artefacts return a clear error pointing
//! at those options (a direct TLS client is the remaining follow-on).

use std::path::{Path, PathBuf};

use crate::plugin_artefact::{self, TrustRoot};

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
    /// The installed plugin file: `<name>.tar.gz` for a packed artefact,
    /// `<name>.wasm` for a bare module.
    pub wasm_path: PathBuf,
    /// The `<name>.sig` sidecar written for a signed bare module.
    pub sig_path: Option<PathBuf>,
    pub sha256: String,
    /// Trust-root key label that verified the signature, when verified.
    pub signed_by: Option<String>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    plugin_artefact::sha256_hex(bytes)
}

/// Verify an Ed25519 signature (base64 of the raw 64-byte signature) over
/// `bytes` against a trust root (a directory of `*.pub` keys, a key file,
/// or an inline hex key — the format the plugin loader reads). Returns the
/// matching key's label. Needs no WASM runtime / `wasm-plugins` feature.
fn verify_against_trust_root(
    bytes: &[u8],
    sig_b64: &str,
    trust_root: &Path,
) -> Result<String, String> {
    let trust = TrustRoot::load(&trust_root.to_string_lossy()).map_err(|e| e.to_string())?;
    trust
        .verify(bytes, sig_b64)
        .map(str::to_string)
        .map_err(|e| e.to_string())
}

/// Remove `path` if it exists.
fn remove_if_present(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("remove {}: {e}", path.display())),
    }
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

/// Minimal blocking HTTP/1.1 GET for `http://host[:port]/path`, returning the
/// response body. Dependency-free (std::net only) and intentionally tiny — the
/// artefact's integrity comes from the index `sha256`, not the transport, so
/// this needs no TLS and no redirect/keep-alive handling. `Connection: close`
/// lets us read to EOF instead of parsing Content-Length / chunked encoding.
fn http_get(url: &str) -> Result<Vec<u8>, String> {
    use std::io::{Read, Write};
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("not an http:// url: {url}"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let connect_addr = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };
    let mut stream = std::net::TcpStream::connect(&connect_addr)
        .map_err(|e| format!("connect {connect_addr}: {e}"))?;
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(30)));
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: helios-plugin\r\n\
         Accept: */*\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("send request: {e}"))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("read response from {connect_addr}: {e}"))?;

    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response (no header terminator)".to_string())?;
    let status_line_end = buf.iter().position(|&b| b == b'\r').unwrap_or(0);
    let status_line = std::str::from_utf8(&buf[..status_line_end]).unwrap_or("");
    let code = status_line.split_whitespace().nth(1).unwrap_or("");
    if code != "200" {
        return Err(format!("HTTP {} fetching {url}", status_line.trim()));
    }
    Ok(buf[sep + 4..].to_vec())
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
        None => matches
            .next()
            .ok_or_else(|| format!("no plugin '{name}' in registry")),
    }
}

/// Install a plugin from the registry into `dest_dir`.
///
/// Verifies the SHA-256 against the index; when `trust_root` is given the entry
/// MUST carry a signature and it is verified against the trust root. A packed
/// artefact (gzip) must unpack cleanly and declare the entry's name, and with a
/// trust root its own `plugin.sig` must verify too (the proxy requires it). On
/// success writes `<name>.tar.gz`, or `<name>.wasm` (+ `<name>.sig` when the
/// index signs it), through a temporary file and a rename, the signature
/// before the module, so a watching proxy never sees a half-written plugin.
/// The other format's files for the same name are removed, so the directory
/// holds one version of the plugin.
pub fn install(
    index_path: &Path,
    name: &str,
    version: Option<&str>,
    dest_dir: &Path,
    trust_root: Option<&Path>,
) -> Result<InstallReport, String> {
    let index = load_index(index_path)?;
    let entry = find_entry(&index, name, version)?.clone();

    // Fetch the artefact bytes. Local / file:// are read from disk; http:// is
    // fetched over the network. Because the index is a *local, trusted* file
    // (a `--registry` path), the `sha256` it pins makes a plain-HTTP fetch
    // integrity-safe — a tampered download fails the hash check below.
    let bytes = if entry.artifact.starts_with("http://") {
        http_get(&entry.artifact)?
    } else if entry.artifact.starts_with("https://") {
        return Err(format!(
            "https:// artefact fetch is not supported in this build ({}); use an \
             http:// URL (e.g. a localhost TLS-terminating mirror) or a file:// / local \
             artefact — the index sha256 verifies the bytes regardless of transport",
            entry.artifact
        ));
    } else {
        let artifact_path = resolve_artifact_path(index_path, &entry.artifact)?;
        std::fs::read(&artifact_path)
            .map_err(|e| format!("read artefact {}: {}", artifact_path.display(), e))?
    };

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
        let sig = entry
            .signature
            .as_deref()
            .ok_or_else(|| format!("'{name}' has no signature but a trust root was supplied"))?;
        let label = verify_against_trust_root(&bytes, sig, root)
            .map_err(|e| format!("signature verification failed for '{name}': {e}"))?;
        signed_by = Some(label);
    }

    plugin_artefact::validate_name(name).map_err(|e| e.to_string())?;
    std::fs::create_dir_all(dest_dir)
        .map_err(|e| format!("create dest dir {}: {}", dest_dir.display(), e))?;
    let write = |path: &Path, bytes: &[u8]| {
        plugin_artefact::write_atomic(path, bytes, 0o644).map_err(|e| e.to_string())
    };
    let (wasm_path, sig_path) = if plugin_artefact::is_gzip(&bytes) {
        let artefact = plugin_artefact::unpack(&bytes, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
            .map_err(|e| format!("'{name}' is not a valid plugin artefact: {e}"))?;
        if artefact.manifest.name != name {
            return Err(format!(
                "'{name}' artefact declares the plugin name '{}'",
                artefact.manifest.name
            ));
        }
        if let Some(root) = trust_root {
            let trust = TrustRoot::load(&root.to_string_lossy()).map_err(|e| e.to_string())?;
            plugin_artefact::verify_artefact(&artefact, &trust, false)
                .map_err(|e| format!("'{name}' artefact: {e}"))?;
        }
        let path = dest_dir.join(format!("{name}.tar.gz"));
        write(&path, &bytes)?;
        for ext in ["wasm", "sig", "yaml", "json"] {
            remove_if_present(&dest_dir.join(format!("{name}.{ext}")))?;
        }
        (path, None)
    } else if plugin_artefact::is_wasm(&bytes) {
        let path = dest_dir.join(format!("{name}.wasm"));
        let sig_path = match entry.signature.as_deref() {
            Some(sig) => {
                let p = dest_dir.join(format!("{name}.sig"));
                write(&p, sig.trim().as_bytes())?;
                Some(p)
            }
            None => {
                // A stale sidecar would no longer match the new module.
                remove_if_present(&dest_dir.join(format!("{name}.sig")))?;
                None
            }
        };
        write(&path, &bytes)?;
        remove_if_present(&dest_dir.join(format!("{name}.tar.gz")))?;
        (path, sig_path)
    } else {
        return Err(format!(
            "'{name}' artefact is neither a WebAssembly module nor a .tar.gz plugin artefact"
        ));
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
    /// SHA-256 of the file (the module, or the whole `.tar.gz`).
    pub sha256: String,
    /// Trust-root key label that verified the signature over the module,
    /// when a trust root was supplied (otherwise `None` — only the digest
    /// and, for an artefact, its integrity were checked).
    pub signed_by: Option<String>,
    /// For an artefact with a `manifest.sig`: the key that signed its
    /// manifest (with a trust root).
    pub manifest_signed_by: Option<String>,
}

/// Verify a local plugin file already on disk (a pre-deploy / audit check,
/// distinct from `install`).
///
/// - A `.tar.gz` artefact must unpack cleanly (entries, manifest, module
///   SHA-256); with a trust root, its `plugin.sig` must verify, and so must
///   its `manifest.sig` when present (and, with `require_manifest_signature`,
///   it must be present).
/// - A bare module: its SHA-256 and, with a trust root, its signature, read
///   from `sig_path` or the `<name>.sig` sidecar (the loader's convention).
pub fn verify(
    path: &Path,
    trust_root: Option<&Path>,
    sig_path: Option<&Path>,
) -> Result<VerifyReport, String> {
    verify_with(path, trust_root, sig_path, false)
}

/// [`verify`], optionally requiring an artefact's signed manifest.
pub fn verify_with(
    path: &Path,
    trust_root: Option<&Path>,
    sig_path: Option<&Path>,
    require_manifest_signature: bool,
) -> Result<VerifyReport, String> {
    let bytes = plugin_artefact::read_bounded(path, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
        .map_err(|e| e.to_string())?;
    let sha256 = sha256_hex(&bytes);
    let trust = trust_root
        .map(|root| TrustRoot::load(&root.to_string_lossy()).map_err(|e| e.to_string()))
        .transpose()?;

    if plugin_artefact::is_gzip(&bytes) {
        let artefact = plugin_artefact::unpack(&bytes, plugin_artefact::DEFAULT_MAX_ARTEFACT_BYTES)
            .map_err(|e| e.to_string())?;
        let (signed_by, manifest_signed_by) = match &trust {
            Some(trust) => {
                let t =
                    plugin_artefact::verify_artefact(&artefact, trust, require_manifest_signature)
                        .map_err(|e| format!("signature verification failed: {e}"))?;
                (Some(t.wasm_signed_by), t.manifest_signed_by)
            }
            None => (None, None),
        };
        return Ok(VerifyReport {
            sha256,
            signed_by,
            manifest_signed_by,
        });
    }

    let mut signed_by = None;
    if let Some(trust) = &trust {
        let sig_file = match sig_path {
            Some(p) => p.to_path_buf(),
            None => path.with_extension("sig"),
        };
        let sig = std::fs::read_to_string(&sig_file)
            .map_err(|e| format!("read signature {}: {}", sig_file.display(), e))?;
        let label = trust
            .verify(&bytes, sig.trim())
            .map_err(|e| format!("signature verification failed: {e}"))?;
        signed_by = Some(label.to_string());
    }
    Ok(VerifyReport {
        sha256,
        signed_by,
        manifest_signed_by: None,
    })
}

/// Scaffold a new plugin crate under `dir/<name>/`: a `Cargo.toml` for a
/// `wasm32-unknown-unknown` cdylib, a `src/lib.rs` implementing the plugin ABI
/// (`alloc`, `dealloc` and a `pre_query` hook answering `continue`), the
/// `plugin.yaml` manifest `helios-plugin pack --manifest` reads, and a README
/// with the build, pack and load steps. It builds as generated and depends on
/// nothing but `std`.
pub fn scaffold(name: &str, dir: &Path) -> Result<PathBuf, String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(format!("invalid plugin name '{name}' (use [A-Za-z0-9_-])"));
    }
    let root = dir.join(name);
    if root.exists() {
        return Err(format!("{} already exists", root.display()));
    }
    std::fs::create_dir_all(root.join("src"))
        .map_err(|e| format!("create {}: {}", root.display(), e))?;
    let crate_name = name.replace('-', "_");
    let write = |file: &str, text: String| {
        std::fs::write(root.join(file), text).map_err(|e| format!("write {file}: {e}"))
    };

    write(
        "Cargo.toml",
        format!(
            r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"
publish = false

[lib]
crate-type = ["cdylib"]

[profile.release]
opt-level = "s"
lto = true
codegen-units = 1
panic = "abort"
"#
        ),
    )?;

    write(
        "plugin.yaml",
        format!(
            "name: {name}\nversion: 0.1.0\ndescription: A HeliosProxy plugin\nlicense: Apache-2.0\nhooks:\n  - pre_query\n"
        ),
    )?;

    write("src/lib.rs", SCAFFOLD_LIB_RS.to_string())?;

    write(
        "README.md",
        format!(
            r#"# {name}

A HeliosProxy WASM plugin.

## Build

```
rustup target add wasm32-unknown-unknown
cargo build --release --target wasm32-unknown-unknown
```

## Pack and sign

```
helios-plugin keygen --out-dir keys --name publisher      # once
helios-plugin pack --manifest plugin.yaml \
  --wasm target/wasm32-unknown-unknown/release/{crate_name}.wasm \
  --sign-with keys/publisher.key --output {name}.tar.gz
helios-plugin inspect {name}.tar.gz
```

## Load

Copy `{name}.tar.gz` into the proxy's `[plugins] plugin_dir` (with
`trust_root` pointing at `keys/publisher.pub` or a directory holding it), then
`POST /plugins/reload` on the admin API, send SIGHUP, or let `hot_reload` pick
it up. `GET /plugins` lists it; `GET /plugins/files` shows why a file did not
load.

## The hook

The proxy calls `pre_query` with the statement's context as JSON (`query`,
`normalized`, `tables`, `is_read_only`, `hook_context`) and reads the JSON
answer: `{{"kind": "continue"}}`, `{{"kind": "block", "reason": "..."}}`,
`{{"kind": "rewrite", "sql": "..."}}`. Add `route`, `post_query` or
`authenticate` the same way and list them under `hooks` in `plugin.yaml`.
"#
        ),
    )?;

    Ok(root)
}

/// `src/lib.rs` of a scaffolded plugin.
const SCAFFOLD_LIB_RS: &str = r##"//! A HeliosProxy WASM plugin.
//!
//! The plugin ABI: the host calls `alloc(len)` for a buffer, writes the
//! hook's JSON input there, calls the hook with `(ptr, len)`, and frees the
//! buffer with `dealloc(ptr, len)`. A hook that answers returns
//! `(ptr << 32) | len` of a buffer it allocated with `alloc`; the host reads
//! the JSON there and frees it with `dealloc`.

use std::alloc::Layout;

fn layout(size: i32) -> Option<Layout> {
    if size <= 0 {
        return None;
    }
    Layout::from_size_align(size as usize, 8).ok()
}

/// Allocate `size` bytes for the host. Returns 0 for an empty buffer.
#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    match layout(size) {
        // SAFETY: the layout has a non-zero size.
        Some(layout) => unsafe { std::alloc::alloc(layout) as i32 },
        None => 0,
    }
}

/// Free a buffer `alloc` returned, with the size it was allocated with.
#[no_mangle]
pub extern "C" fn dealloc(ptr: i32, size: i32) {
    if let (Some(layout), true) = (layout(size), ptr != 0) {
        // SAFETY: `ptr` came from `alloc` with this size.
        unsafe { std::alloc::dealloc(ptr as *mut u8, layout) }
    }
}

/// Hand `bytes` to the host as a hook's answer.
fn respond(bytes: &[u8]) -> i64 {
    let ptr = alloc(bytes.len() as i32);
    if ptr == 0 {
        return 0;
    }
    // SAFETY: `alloc` returned `bytes.len()` writable bytes at `ptr`.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len()) };
    ((ptr as u32 as i64) << 32) | (bytes.len() as i64)
}

/// The hook's JSON input.
fn input<'a>(ptr: i32, len: i32) -> &'a [u8] {
    if ptr == 0 || len <= 0 {
        return &[];
    }
    // SAFETY: the host wrote `len` bytes at `ptr` and keeps them alive for
    // the call.
    unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) }
}

/// Called before each statement runs. Parse the JSON input (for example
/// with serde_json) to decide; this one lets every statement through.
#[no_mangle]
pub extern "C" fn pre_query(ptr: i32, len: i32) -> i64 {
    let _context = input(ptr, len);
    respond(br#"{"kind":"continue"}"#)
}
"##;

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
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&key.verifying_key().to_bytes()),
        )
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
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&official.verifying_key().to_bytes()),
        )
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
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&key.verifying_key().to_bytes()),
        )
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
    fn install_rejects_https_artifact() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let index = src.path().join("idx.json");
        std::fs::write(
            &index,
            r#"{"plugins":[{"name":"x","artifact":"https://example/x.wasm","sha256":"00"}]}"#,
        )
        .unwrap();
        let err = install(&index, "x", None, dst.path(), None).unwrap_err();
        assert!(err.contains("https://"), "{err}");
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
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&key.verifying_key().to_bytes()),
        )
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
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&key.verifying_key().to_bytes()),
        )
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
        for file in ["Cargo.toml", "plugin.yaml", "src/lib.rs", "README.md"] {
            assert!(root.join(file).exists(), "{file}");
        }
        let lib = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
        for export in [
            "fn alloc(size: i32) -> i32",
            "fn dealloc(ptr: i32, size: i32)",
            "fn pre_query(ptr: i32, len: i32) -> i64",
        ] {
            assert!(lib.contains(export), "{export}");
        }
        let cargo = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
        assert!(cargo.contains("crate-type = [\"cdylib\"]"));
        // The manifest it writes is the one `pack --manifest` reads.
        let manifest = crate::plugin_artefact::parse_yaml_manifest(
            &std::fs::read_to_string(root.join("plugin.yaml")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.name, "my-plugin");
        assert_eq!(manifest.hooks, ["pre_query"]);
        assert!(manifest.unknown_keys.is_empty());
        // Reject invalid names + double-create.
        assert!(scaffold("bad name", dir.path()).is_err());
        assert!(scaffold("my-plugin", dir.path()).is_err());
    }

    /// A packed artefact installs as `<name>.tar.gz`, replacing a bare
    /// module of the same name, and must declare the entry's name.
    #[test]
    fn install_lands_an_artefact_as_tar_gz() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let input = crate::plugin_artefact::PackInput {
            name: "colmask".into(),
            version: "0.2.0".into(),
            description: String::new(),
            license: String::new(),
            hooks: vec!["pre_query".into()],
            wasm: WASM.to_vec(),
            wasm_signature: None,
        };
        let (_, bytes) = crate::plugin_artefact::pack(&input, Some(&key)).unwrap();
        std::fs::write(src.path().join("colmask.tar.gz"), &bytes).unwrap();
        let index = |name: &str| {
            let index = format!(
                r#"{{"plugins":[{{"name":"{name}","artifact":"colmask.tar.gz","sha256":"{}","signature":"{}"}}]}}"#,
                sha256_hex(&bytes),
                b64(&key.sign(&bytes).to_bytes())
            );
            let path = src.path().join(format!("{name}.json"));
            std::fs::write(&path, index).unwrap();
            path
        };
        let trust = tempfile::tempdir().unwrap();
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&key.verifying_key().to_bytes()),
        )
        .unwrap();
        std::fs::write(dst.path().join("colmask.wasm"), WASM).unwrap();
        std::fs::write(dst.path().join("colmask.sig"), "old").unwrap();

        let r = install(
            &index("colmask"),
            "colmask",
            None,
            dst.path(),
            Some(trust.path()),
        )
        .unwrap();
        assert_eq!(r.wasm_path, dst.path().join("colmask.tar.gz"));
        assert_eq!(std::fs::read(&r.wasm_path).unwrap(), bytes);
        assert!(r.sig_path.is_none());
        assert!(
            !dst.path().join("colmask.wasm").exists(),
            "the bare module is replaced"
        );
        assert!(!dst.path().join("colmask.sig").exists());

        let err = install(&index("other"), "other", None, dst.path(), None).unwrap_err();
        assert!(err.contains("declares the plugin name 'colmask'"), "{err}");
    }

    #[test]
    fn verify_checks_an_artefacts_signatures() {
        let dir = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7u8; 32]);
        std::fs::write(
            trust.path().join("official.pub"),
            b64(&key.verifying_key().to_bytes()),
        )
        .unwrap();
        let input = crate::plugin_artefact::PackInput {
            name: "p".into(),
            version: "1.0.0".into(),
            description: String::new(),
            license: String::new(),
            hooks: vec!["route".into()],
            wasm: WASM.to_vec(),
            wasm_signature: None,
        };
        let signed = dir.path().join("signed.tar.gz");
        std::fs::write(
            &signed,
            crate::plugin_artefact::pack(&input, Some(&key)).unwrap().1,
        )
        .unwrap();
        let r = verify_with(&signed, Some(trust.path()), None, true).unwrap();
        assert_eq!(r.signed_by.as_deref(), Some("official"));
        assert_eq!(r.manifest_signed_by.as_deref(), Some("official"));

        let unsigned = dir.path().join("unsigned.tar.gz");
        std::fs::write(
            &unsigned,
            crate::plugin_artefact::pack(&input, None).unwrap().1,
        )
        .unwrap();
        assert!(verify(&unsigned, None, None).is_ok(), "integrity only");
        assert!(verify(&unsigned, Some(trust.path()), None).is_err());
    }
}
