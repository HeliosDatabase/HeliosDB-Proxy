//! Plugin artefacts: the `.tar.gz` distribution format, its manifest, Ed25519
//! keys and trust roots.
//!
//! One implementation shared by the proxy's plugin loader, the `helios-plugin`
//! CLI (`pack`, `inspect`, `sign`, `verify`, `keygen`) and the registry
//! installer, so a file one of them accepts is a file the others accept. It
//! is compiled in every build (no feature gate): the CLI and the installer
//! need no WASM runtime.
//!
//! # Artefact layout
//!
//! A gzip-compressed tar archive with these regular-file entries at its root,
//! each at most once, and nothing else:
//!
//! | entry | required | content |
//! |---|---|---|
//! | `manifest.json` | yes | [`ArtefactManifest`] |
//! | `plugin.wasm` | yes | the WebAssembly module; its SHA-256 is the manifest's `wasm_sha256` |
//! | `plugin.sig` | no | base64 Ed25519 signature over the `plugin.wasm` bytes |
//! | `manifest.sig` | no (schema 1.1) | base64 Ed25519 signature over the exact `manifest.json` bytes |
//!
//! `plugin.sig` proves who built the code. `manifest.sig` also binds the
//! code to its name, version and hooks: the manifest pins `wasm_sha256`, so
//! a signed wasm cannot be repacked under another plugin's name (and so
//! reach that plugin's KV namespace) without invalidating it.
//!
//! # Keys
//!
//! A signing key is an Ed25519 seed given as a PKCS#8 PEM file (what
//! `openssl genpkey -algorithm Ed25519` writes), base64 of the 32-byte seed,
//! or 64 hex digits. A public key is a SubjectPublicKeyInfo PEM file (what
//! `openssl pkey -pubout` writes), base64 of the 32 raw bytes, or 64 hex
//! digits. A trust root is a directory of `*.pub` files, one key each, a
//! single key file, or one key given inline as 64 hex digits.

use std::collections::HashSet;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use ed25519_dalek::{Signer, Verifier};

/// Artefact schema version this build writes. Readers accept every `1.x`;
/// minor versions only add optional fields and entries.
pub const SCHEMA_VERSION: &str = "1.1";

/// The manifest entry.
pub const ENTRY_MANIFEST: &str = "manifest.json";
/// The WebAssembly module entry.
pub const ENTRY_WASM: &str = "plugin.wasm";
/// The signature over `plugin.wasm`.
pub const ENTRY_SIG: &str = "plugin.sig";
/// The signature over `manifest.json` (schema 1.1).
pub const ENTRY_MANIFEST_SIG: &str = "manifest.sig";

/// Default bound on an artefact's size: the file on disk and the sum of its
/// decompressed entries (`[plugins] max_artefact_bytes`).
pub const DEFAULT_MAX_ARTEFACT_BYTES: u64 = 64 * 1024 * 1024;

/// Longest plugin name accepted. The name is the plugin's identity and its
/// KV namespace.
pub const MAX_NAME_LEN: usize = 128;

/// Longest version string accepted.
pub const MAX_VERSION_LEN: usize = 64;

/// The DER prefix of an RFC 8410 Ed25519 private key in PKCS#8 v1 form; the
/// 32-byte seed follows it.
const PKCS8_ED25519_V1_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// The DER prefix of an RFC 8410 Ed25519 private key in PKCS#8 v2 form
/// (OneAsymmetricKey, which also carries the public key); the 32-byte seed
/// follows it, then `a1 23 03 21 00` and the public key.
const PKCS8_ED25519_V2_PREFIX: [u8; 16] = [
    0x30, 0x53, 0x02, 0x01, 0x01, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// The trailer between the seed and the public key in the PKCS#8 v2 form.
const PKCS8_ED25519_V2_PUBLIC: [u8; 5] = [0xa1, 0x23, 0x03, 0x21, 0x00];

/// The DER length of the PKCS#8 v2 form: prefix, seed, trailer, public key.
const PKCS8_ED25519_V2_LEN: usize = 16 + 32 + 5 + 32;

/// The DER prefix of an Ed25519 SubjectPublicKeyInfo; the 32-byte key
/// follows it.
const SPKI_ED25519_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// Why an artefact, key or trust root was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArtefactError {
    /// Reading a file failed.
    Io(String),
    /// The archive or one of its entries is malformed or not allowed.
    Format(String),
    /// `manifest.json` is malformed or carries an invalid field.
    Manifest(String),
    /// A key or trust root is malformed.
    Key(String),
    /// A signature is malformed, missing where required, or matches no
    /// trusted key.
    Signature(String),
}

impl std::fmt::Display for ArtefactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtefactError::Io(m) => write!(f, "I/O error: {m}"),
            ArtefactError::Format(m) => write!(f, "invalid artefact: {m}"),
            ArtefactError::Manifest(m) => write!(f, "invalid manifest: {m}"),
            ArtefactError::Key(m) => write!(f, "invalid key: {m}"),
            ArtefactError::Signature(m) => write!(f, "signature check failed: {m}"),
        }
    }
}

impl std::error::Error for ArtefactError {}

/// `manifest.json` of an artefact.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArtefactManifest {
    /// `major.minor`; readers accept every `1.x`.
    pub schema_version: String,
    /// Plugin name: `[A-Za-z0-9_-]`, at most [`MAX_NAME_LEN`] characters.
    pub name: String,
    /// Plugin version: `[A-Za-z0-9.+-]`, at most [`MAX_VERSION_LEN`]
    /// characters (semver recommended).
    pub version: String,
    /// One-line description.
    #[serde(default)]
    pub description: String,
    /// SPDX license identifier.
    #[serde(default)]
    pub license: String,
    /// Hook export names the module implements (`pre_query`, `route`, ...).
    pub hooks: Vec<String>,
    /// Lower-case hex SHA-256 of `plugin.wasm`.
    pub wasm_sha256: String,
    /// Lower-case hex SHA-256 of `plugin.sig`, when the artefact has one.
    #[serde(default)]
    pub signature_sha256: Option<String>,
    /// `"ed25519"` when the artefact is signed.
    #[serde(default)]
    pub signature_algorithm: Option<String>,
    /// RFC 3339 time the artefact was packed.
    #[serde(default)]
    pub packed_at: String,
}

impl ArtefactManifest {
    /// Whether this build reads the manifest's schema (same major version).
    pub fn schema_compatible(&self) -> bool {
        self.schema_version.split('.').next() == SCHEMA_VERSION.split('.').next()
    }

    /// Check every field a loader relies on: schema, name, version, a hook
    /// list without blanks or repeats, and a well-formed `wasm_sha256`.
    pub fn validate(&self) -> Result<(), ArtefactError> {
        if !self.schema_compatible() {
            return Err(ArtefactError::Manifest(format!(
                "unsupported schema version {} (this build reads {}.x)",
                self.schema_version,
                SCHEMA_VERSION.split('.').next().unwrap_or("1")
            )));
        }
        validate_name(&self.name)?;
        validate_version(&self.version)?;
        let mut seen = HashSet::new();
        for hook in &self.hooks {
            if hook.trim().is_empty() {
                return Err(ArtefactError::Manifest("empty hook name".into()));
            }
            if !seen.insert(hook.as_str()) {
                return Err(ArtefactError::Manifest(format!("hook {hook} listed twice")));
            }
        }
        if !is_sha256_hex(&self.wasm_sha256) {
            return Err(ArtefactError::Manifest(
                "wasm_sha256 is not 64 lower-case hex digits".into(),
            ));
        }
        Ok(())
    }
}

/// Check a plugin name: non-empty, at most [`MAX_NAME_LEN`] characters of
/// `[A-Za-z0-9_-]`. The name is the plugin's identity and KV namespace.
pub fn validate_name(name: &str) -> Result<(), ArtefactError> {
    if name.is_empty() {
        return Err(ArtefactError::Manifest("plugin name is empty".into()));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(ArtefactError::Manifest(format!(
            "plugin name longer than {MAX_NAME_LEN} characters"
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(ArtefactError::Manifest(format!(
            "plugin name {name:?} may only contain A-Z, a-z, 0-9, '-' and '_'"
        )));
    }
    Ok(())
}

/// Check a plugin version: non-empty, at most [`MAX_VERSION_LEN`]
/// characters of `[A-Za-z0-9.+-]` (so semver pre-release and build tags
/// pass).
pub fn validate_version(version: &str) -> Result<(), ArtefactError> {
    if version.is_empty() {
        return Err(ArtefactError::Manifest("plugin version is empty".into()));
    }
    if version.len() > MAX_VERSION_LEN {
        return Err(ArtefactError::Manifest(format!(
            "plugin version longer than {MAX_VERSION_LEN} characters"
        )));
    }
    if !version
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'))
    {
        return Err(ArtefactError::Manifest(format!(
            "plugin version {version:?} may only contain A-Z, a-z, 0-9, '.', '-' and '+'"
        )));
    }
    Ok(())
}

fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Lower-case hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex_lower(&Sha256::digest(bytes))
}

fn hex_lower(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let nibble = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks(2)
        .map(|pair| Some(nibble(pair[0])? << 4 | nibble(pair[1])?))
        .collect()
}

/// Whether `bytes` start with the WebAssembly magic number and version.
pub fn is_wasm(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && &bytes[0..4] == b"\x00asm"
}

/// Whether `bytes` start with the gzip magic number.
pub fn is_gzip(bytes: &[u8]) -> bool {
    bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b
}

/// The `(id, body)` of every section of a WebAssembly module, in order.
/// Walks the section headers only; a truncated or malformed header is an
/// error.
pub fn wasm_sections(wasm: &[u8]) -> Result<Vec<(u8, &[u8])>, ArtefactError> {
    let malformed = |what: &str| ArtefactError::Format(format!("malformed module: {what}"));
    if !is_wasm(wasm) {
        return Err(malformed("bad magic number"));
    }
    let mut out = Vec::new();
    let mut pos = 8;
    while pos < wasm.len() {
        let id = wasm[pos];
        pos += 1;
        let (size, used) = leb128_u32(&wasm[pos..]).ok_or_else(|| malformed("section size"))?;
        pos += used;
        let end = pos
            .checked_add(size as usize)
            .filter(|&end| end <= wasm.len())
            .ok_or_else(|| malformed("section runs past the end"))?;
        out.push((id, &wasm[pos..end]));
        pos = end;
    }
    Ok(out)
}

/// The payload of the custom section named `name` in a WebAssembly module,
/// if it has one.
pub fn wasm_custom_section<'a>(
    wasm: &'a [u8],
    name: &str,
) -> Result<Option<&'a [u8]>, ArtefactError> {
    let malformed = |what: &str| ArtefactError::Format(format!("malformed module: {what}"));
    for (id, body) in wasm_sections(wasm)? {
        if id != 0 {
            continue;
        }
        let (len, used) = leb128_u32(body).ok_or_else(|| malformed("custom section name"))?;
        let name_end = used
            .checked_add(len as usize)
            .filter(|&e| e <= body.len())
            .ok_or_else(|| malformed("custom section name runs past its section"))?;
        if &body[used..name_end] == name.as_bytes() {
            return Ok(Some(&body[name_end..]));
        }
    }
    Ok(None)
}

/// The names of the functions a WebAssembly module exports, in order.
pub fn wasm_exported_functions(wasm: &[u8]) -> Result<Vec<String>, ArtefactError> {
    let malformed = |what: &str| ArtefactError::Format(format!("malformed module: {what}"));
    let mut names = Vec::new();
    for (id, body) in wasm_sections(wasm)? {
        if id != 7 {
            continue;
        }
        let (count, mut pos) = leb128_u32(body).ok_or_else(|| malformed("export count"))?;
        for _ in 0..count {
            let (len, used) = body
                .get(pos..)
                .and_then(leb128_u32)
                .ok_or_else(|| malformed("export name length"))?;
            pos += used;
            let end = pos
                .checked_add(len as usize)
                .filter(|&e| e <= body.len())
                .ok_or_else(|| malformed("export name runs past its section"))?;
            let name = std::str::from_utf8(&body[pos..end])
                .map_err(|_| malformed("export name is not UTF-8"))?
                .to_string();
            pos = end;
            let kind = *body.get(pos).ok_or_else(|| malformed("export kind"))?;
            pos += 1;
            let (_, used) = body
                .get(pos..)
                .and_then(leb128_u32)
                .ok_or_else(|| malformed("export index"))?;
            pos += used;
            if kind == 0 {
                names.push(name);
            }
        }
    }
    Ok(names)
}

/// An unsigned LEB128 `u32` at the start of `bytes`: `(value, bytes used)`.
fn leb128_u32(bytes: &[u8]) -> Option<(u32, usize)> {
    let mut value: u32 = 0;
    for (i, &b) in bytes.iter().take(5).enumerate() {
        let chunk = u32::from(b & 0x7f);
        if i == 4 && chunk > 0x0f {
            return None;
        }
        value |= chunk << (7 * i);
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

/// The two plugin file kinds a plugin directory holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtefactKind {
    /// A bare `.wasm` module with optional `.sig` / `.yaml` / `.json`
    /// sidecars.
    Wasm,
    /// A packed `.tar.gz` artefact.
    TarGz,
}

impl ArtefactKind {
    /// The kind of plugin file `path` names: a name ending `.wasm` or
    /// `.tar.gz`. Anything else (sidecars, keys, editor backups) is not a
    /// plugin.
    pub fn of(path: &Path) -> Option<Self> {
        let name = path.file_name()?.to_str()?;
        // Hidden files are editor swap files, partial downloads and the
        // temporaries `write_atomic` renames into place: never plugins.
        if name.starts_with('.') {
            return None;
        }
        if name.ends_with(".tar.gz") && name.len() > ".tar.gz".len() {
            Some(ArtefactKind::TarGz)
        } else if name.ends_with(".wasm") && name.len() > ".wasm".len() {
            Some(ArtefactKind::Wasm)
        } else {
            None
        }
    }

    /// The files besides `path` whose content belongs to this plugin: the
    /// `.sig`, `.yaml` and `.json` sidecars of a bare module (whether or not
    /// they exist); none for an artefact, which carries its own.
    pub fn sidecars(self, path: &Path) -> Vec<PathBuf> {
        match self {
            ArtefactKind::Wasm => ["sig", "yaml", "json"]
                .iter()
                .map(|ext| path.with_extension(ext))
                .collect(),
            ArtefactKind::TarGz => Vec::new(),
        }
    }
}

/// An unpacked artefact.
#[derive(Debug, Clone)]
pub struct Artefact {
    /// The parsed and validated manifest.
    pub manifest: ArtefactManifest,
    /// The exact `manifest.json` bytes (what `manifest.sig` signs).
    pub manifest_json: Vec<u8>,
    /// The module; its SHA-256 matched the manifest.
    pub wasm: Vec<u8>,
    /// `plugin.sig`, when present.
    pub wasm_signature: Option<String>,
    /// `manifest.sig`, when present.
    pub manifest_signature: Option<String>,
}

/// Read and unpack the artefact at `path`, refusing a file or content over
/// `max_bytes`.
pub fn read(path: &Path, max_bytes: u64) -> Result<Artefact, ArtefactError> {
    let raw = read_bounded(path, max_bytes)?;
    unpack(&raw, max_bytes)
}

/// Read a whole file, refusing one over `max_bytes`.
pub fn read_bounded(path: &Path, max_bytes: u64) -> Result<Vec<u8>, ArtefactError> {
    let file = std::fs::File::open(path)
        .map_err(|e| ArtefactError::Io(format!("open {}: {e}", path.display())))?;
    let len = file
        .metadata()
        .map_err(|e| ArtefactError::Io(format!("stat {}: {e}", path.display())))?
        .len();
    if len > max_bytes {
        return Err(ArtefactError::Format(format!(
            "{} is {len} bytes, over the {max_bytes}-byte limit",
            path.display()
        )));
    }
    let mut raw = Vec::with_capacity(len as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut raw)
        .map_err(|e| ArtefactError::Io(format!("read {}: {e}", path.display())))?;
    if raw.len() as u64 > max_bytes {
        return Err(ArtefactError::Format(format!(
            "{} grew over the {max_bytes}-byte limit while being read",
            path.display()
        )));
    }
    Ok(raw)
}

/// Unpack an artefact from its `.tar.gz` bytes.
///
/// Refuses: anything but the four known entries, an entry twice, an entry
/// that is not a regular file, decompressed content over `max_bytes`, a
/// missing manifest or module, an invalid manifest, a module whose SHA-256
/// differs from the manifest's, a module without the WebAssembly magic
/// number, a signature that is not UTF-8, and a `plugin.sig` whose SHA-256
/// differs from the manifest's `signature_sha256`.
pub fn unpack(bytes: &[u8], max_bytes: u64) -> Result<Artefact, ArtefactError> {
    if !is_gzip(bytes) {
        return Err(ArtefactError::Format("not a gzip file".into()));
    }
    let gz = flate2::read::GzDecoder::new(Cursor::new(bytes));
    // Bound the decompressed stream itself, so a small archive cannot
    // inflate into gigabytes before any entry size is looked at.
    let mut archive = tar::Archive::new(gz.take(max_bytes.saturating_add(64 * 1024)));
    let mut manifest_json: Option<Vec<u8>> = None;
    let mut wasm: Option<Vec<u8>> = None;
    let mut wasm_sig: Option<Vec<u8>> = None;
    let mut manifest_sig: Option<Vec<u8>> = None;
    let mut total: u64 = 0;
    let entries = archive
        .entries()
        .map_err(|e| ArtefactError::Format(format!("tar: {e}")))?;
    for entry in entries {
        let mut entry = entry.map_err(|e| ArtefactError::Format(format!("tar entry: {e}")))?;
        let name = entry
            .path()
            .map_err(|e| ArtefactError::Format(format!("tar entry path: {e}")))?
            .to_string_lossy()
            .into_owned();
        if !entry.header().entry_type().is_file() {
            return Err(ArtefactError::Format(format!(
                "entry {name} is not a regular file"
            )));
        }
        let slot = match name.as_str() {
            ENTRY_MANIFEST => &mut manifest_json,
            ENTRY_WASM => &mut wasm,
            ENTRY_SIG => &mut wasm_sig,
            ENTRY_MANIFEST_SIG => &mut manifest_sig,
            other => {
                return Err(ArtefactError::Format(format!(
                    "unexpected entry {other} (allowed: {ENTRY_MANIFEST}, {ENTRY_WASM}, \
                     {ENTRY_SIG}, {ENTRY_MANIFEST_SIG})"
                )))
            }
        };
        if slot.is_some() {
            return Err(ArtefactError::Format(format!("entry {name} appears twice")));
        }
        let size = entry.header().size().unwrap_or(u64::MAX);
        total = total.saturating_add(size);
        if total > max_bytes {
            return Err(ArtefactError::Format(format!(
                "content over the {max_bytes}-byte limit"
            )));
        }
        let mut buf = Vec::with_capacity(size as usize);
        entry
            .read_to_end(&mut buf)
            .map_err(|e| ArtefactError::Format(format!("read entry {name}: {e}")))?;
        *slot = Some(buf);
    }
    let manifest_json =
        manifest_json.ok_or_else(|| ArtefactError::Format(format!("missing {ENTRY_MANIFEST}")))?;
    let wasm = wasm.ok_or_else(|| ArtefactError::Format(format!("missing {ENTRY_WASM}")))?;
    let manifest: ArtefactManifest = serde_json::from_slice(&manifest_json)
        .map_err(|e| ArtefactError::Manifest(format!("{ENTRY_MANIFEST}: {e}")))?;
    manifest.validate()?;
    let actual = sha256_hex(&wasm);
    if actual != manifest.wasm_sha256 {
        return Err(ArtefactError::Format(format!(
            "{ENTRY_WASM} SHA-256 is {actual}, the manifest claims {}",
            manifest.wasm_sha256
        )));
    }
    if !is_wasm(&wasm) {
        return Err(ArtefactError::Format(format!(
            "{ENTRY_WASM} is not a WebAssembly module"
        )));
    }
    if let (Some(sig), Some(claimed)) = (&wasm_sig, &manifest.signature_sha256) {
        let actual = sha256_hex(sig);
        if &actual != claimed {
            return Err(ArtefactError::Format(format!(
                "{ENTRY_SIG} SHA-256 is {actual}, the manifest claims {claimed}"
            )));
        }
    }
    let utf8 = |entry: &str, raw: Vec<u8>| {
        String::from_utf8(raw)
            .map(|s| s.trim().to_string())
            .map_err(|_| ArtefactError::Signature(format!("{entry} is not UTF-8 base64")))
    };
    Ok(Artefact {
        manifest,
        manifest_json,
        wasm,
        wasm_signature: wasm_sig.map(|s| utf8(ENTRY_SIG, s)).transpose()?,
        manifest_signature: manifest_sig
            .map(|s| utf8(ENTRY_MANIFEST_SIG, s))
            .transpose()?,
    })
}

/// What [`pack`] builds an artefact from.
#[derive(Debug, Clone)]
pub struct PackInput {
    /// Plugin name.
    pub name: String,
    /// Plugin version.
    pub version: String,
    /// One-line description.
    pub description: String,
    /// SPDX license identifier.
    pub license: String,
    /// Hook export names.
    pub hooks: Vec<String>,
    /// The module.
    pub wasm: Vec<u8>,
    /// A ready-made `plugin.sig` (base64 Ed25519 over `wasm`), when the
    /// module was signed elsewhere. Ignored when `signing_key` is set.
    pub wasm_signature: Option<String>,
}

/// Build an artefact. With `signing_key` the artefact carries a `plugin.sig`
/// and a `manifest.sig` made with it; without one it carries the supplied
/// `wasm_signature` (if any) and no `manifest.sig`. Returns the manifest and
/// the `.tar.gz` bytes.
pub fn pack(
    input: &PackInput,
    signing_key: Option<&ed25519_dalek::SigningKey>,
) -> Result<(ArtefactManifest, Vec<u8>), ArtefactError> {
    if !is_wasm(&input.wasm) {
        return Err(ArtefactError::Format(
            "the module is not WebAssembly (bad magic number)".into(),
        ));
    }
    let wasm_signature = match signing_key {
        Some(key) => Some(sign(key, &input.wasm)),
        None => input.wasm_signature.clone(),
    };
    let manifest = ArtefactManifest {
        schema_version: SCHEMA_VERSION.to_string(),
        name: input.name.clone(),
        version: input.version.clone(),
        description: input.description.clone(),
        license: input.license.clone(),
        hooks: input.hooks.clone(),
        wasm_sha256: sha256_hex(&input.wasm),
        signature_sha256: wasm_signature.as_deref().map(|s| sha256_hex(s.as_bytes())),
        signature_algorithm: wasm_signature.as_ref().map(|_| "ed25519".to_string()),
        packed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    };
    manifest.validate()?;
    let manifest_json = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| ArtefactError::Manifest(format!("serialise: {e}")))?;
    let manifest_signature = signing_key.map(|key| sign(key, &manifest_json));
    let bytes = build_archive(
        &manifest_json,
        &input.wasm,
        wasm_signature.as_deref(),
        manifest_signature.as_deref(),
    )?;
    Ok((manifest, bytes))
}

/// Re-sign an existing artefact with `key`: a new `plugin.sig`, a manifest
/// updated to schema [`SCHEMA_VERSION`] with the new signature's digest, and
/// a `manifest.sig` over it. Returns the new manifest and `.tar.gz` bytes.
pub fn resign(
    artefact: &Artefact,
    key: &ed25519_dalek::SigningKey,
) -> Result<(ArtefactManifest, Vec<u8>), ArtefactError> {
    let input = PackInput {
        name: artefact.manifest.name.clone(),
        version: artefact.manifest.version.clone(),
        description: artefact.manifest.description.clone(),
        license: artefact.manifest.license.clone(),
        hooks: artefact.manifest.hooks.clone(),
        wasm: artefact.wasm.clone(),
        wasm_signature: None,
    };
    pack(&input, Some(key))
}

fn build_archive(
    manifest_json: &[u8],
    wasm: &[u8],
    wasm_signature: Option<&str>,
    manifest_signature: Option<&str>,
) -> Result<Vec<u8>, ArtefactError> {
    let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);
    // Manifest first, so a streaming reader can check the schema before
    // reading the module.
    let mut entries: Vec<(&str, &[u8])> = vec![(ENTRY_MANIFEST, manifest_json), (ENTRY_WASM, wasm)];
    if let Some(sig) = wasm_signature {
        entries.push((ENTRY_SIG, sig.as_bytes()));
    }
    if let Some(sig) = manifest_signature {
        entries.push((ENTRY_MANIFEST_SIG, sig.as_bytes()));
    }
    for (name, bytes) in entries {
        let mut header = tar::Header::new_gnu();
        header
            .set_path(name)
            .map_err(|e| ArtefactError::Format(format!("entry {name}: {e}")))?;
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_cksum();
        tar.append(&header, bytes)
            .map_err(|e| ArtefactError::Format(format!("append {name}: {e}")))?;
    }
    let gz = tar
        .into_inner()
        .map_err(|e| ArtefactError::Format(format!("finish tar: {e}")))?;
    gz.finish()
        .map_err(|e| ArtefactError::Format(format!("finish gzip: {e}")))
}

/// Write `bytes` to `path` through a temporary file in the same directory
/// and a rename, so a watcher never sees a half-written file. `mode` sets
/// the Unix permission bits of the new file.
pub fn write_atomic(path: &Path, bytes: &[u8], mode: u32) -> Result<(), ArtefactError> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| ArtefactError::Io(format!("{} has no file name", path.display())))?;
    let tmp = dir.join(format!(".{file_name}.{}.tmp", std::process::id()));
    let result = (|| {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(mode);
        }
        #[cfg(not(unix))]
        let _ = mode;
        let mut file = options
            .open(&tmp)
            .map_err(|e| ArtefactError::Io(format!("create {}: {e}", tmp.display())))?;
        file.write_all(bytes)
            .and_then(|()| file.sync_all())
            .map_err(|e| ArtefactError::Io(format!("write {}: {e}", tmp.display())))?;
        std::fs::rename(&tmp, path).map_err(|e| {
            ArtefactError::Io(format!(
                "rename {} to {}: {e}",
                tmp.display(),
                path.display()
            ))
        })
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// base64 of the Ed25519 signature of `bytes` by `key`.
pub fn sign(key: &ed25519_dalek::SigningKey, bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(key.sign(bytes).to_bytes())
}

/// Generate a signing key from the operating system's random source.
pub fn generate_signing_key() -> Result<ed25519_dalek::SigningKey, ArtefactError> {
    use rand::RngCore;
    let mut seed = [0u8; 32];
    rand::rngs::OsRng
        .try_fill_bytes(&mut seed)
        .map_err(|e| ArtefactError::Key(format!("the OS random source failed: {e}")))?;
    Ok(ed25519_dalek::SigningKey::from_bytes(&seed))
}

/// A signing key as PKCS#8 PEM (`openssl genpkey -algorithm Ed25519`
/// format).
pub fn signing_key_pem(key: &ed25519_dalek::SigningKey) -> String {
    let mut der = PKCS8_ED25519_V1_PREFIX.to_vec();
    der.extend_from_slice(&key.to_bytes());
    pem_encode("PRIVATE KEY", &der)
}

/// A public key as SubjectPublicKeyInfo PEM (`openssl pkey -pubout`
/// format).
pub fn public_key_pem(key: &ed25519_dalek::VerifyingKey) -> String {
    let mut der = SPKI_ED25519_PREFIX.to_vec();
    der.extend_from_slice(key.as_bytes());
    pem_encode("PUBLIC KEY", &der)
}

/// A public key as base64 of its 32 raw bytes (the `*.pub` trust-root
/// format).
pub fn public_key_base64(key: &ed25519_dalek::VerifyingKey) -> String {
    base64::engine::general_purpose::STANDARD.encode(key.as_bytes())
}

fn pem_encode(label: &str, der: &[u8]) -> String {
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// The DER body of the single PEM block labelled `label` in `text`.
fn pem_decode(text: &str, label: &str) -> Result<Vec<u8>, ArtefactError> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let start = text
        .find(&begin)
        .ok_or_else(|| ArtefactError::Key(format!("no {begin} block")))?;
    let body_start = start + begin.len();
    let stop = text[body_start..]
        .find(&end)
        .ok_or_else(|| ArtefactError::Key(format!("no {end} line")))?;
    let body: String = text[body_start..body_start + stop]
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| ArtefactError::Key(format!("PEM body is not base64: {e}")))
}

/// Parse a signing key: PKCS#8 PEM (v1, or v2 whose embedded public key
/// must match), base64 of the 32-byte seed, or 64 hex digits.
pub fn parse_signing_key(text: &str) -> Result<ed25519_dalek::SigningKey, ArtefactError> {
    let text = text.trim();
    if text.contains("-----BEGIN") {
        if text.contains("ENCRYPTED PRIVATE KEY") {
            return Err(ArtefactError::Key(
                "encrypted PEM keys are not supported; decrypt it with `openssl pkey`".into(),
            ));
        }
        let der = pem_decode(text, "PRIVATE KEY")?;
        let v1 = der.len() == 48 && der[..16] == PKCS8_ED25519_V1_PREFIX;
        // v2: 16-byte prefix, 32-byte seed, 5-byte trailer, 32-byte public
        // key (SEQUENCE of 0x53 content bytes plus its 2-byte header).
        let v2 = der.len() == PKCS8_ED25519_V2_LEN
            && der[..16] == PKCS8_ED25519_V2_PREFIX
            && der[48..53] == PKCS8_ED25519_V2_PUBLIC;
        if !(v1 || v2) {
            return Err(ArtefactError::Key(
                "the PEM private key is not an Ed25519 PKCS#8 key".into(),
            ));
        }
        let seed = &der[16..48];
        let mut arr = [0u8; 32];
        arr.copy_from_slice(seed);
        let key = ed25519_dalek::SigningKey::from_bytes(&arr);
        if v2 && der[53..] != key.verifying_key().as_bytes()[..] {
            return Err(ArtefactError::Key(
                "the PEM key's public key does not match its private key".into(),
            ));
        }
        return Ok(key);
    }
    let seed = decode_32(text, "signing key")?;
    Ok(ed25519_dalek::SigningKey::from_bytes(&seed))
}

/// Parse a public key: SubjectPublicKeyInfo PEM, base64 of the 32 raw
/// bytes, or 64 hex digits.
pub fn parse_public_key(text: &str) -> Result<ed25519_dalek::VerifyingKey, ArtefactError> {
    let text = text.trim();
    let raw = if text.contains("-----BEGIN") {
        let der = pem_decode(text, "PUBLIC KEY")?;
        if der.len() != 44 || der[..12] != SPKI_ED25519_PREFIX {
            return Err(ArtefactError::Key(
                "the PEM public key is not an Ed25519 key".into(),
            ));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&der[12..]);
        arr
    } else {
        decode_32(text, "public key")?
    };
    ed25519_dalek::VerifyingKey::from_bytes(&raw)
        .map_err(|e| ArtefactError::Key(format!("not a valid Ed25519 public key: {e}")))
}

/// 32 bytes from 64 hex digits or base64.
fn decode_32(text: &str, what: &str) -> Result<[u8; 32], ArtefactError> {
    let bytes = if text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit()) {
        hex_decode(text).unwrap_or_default()
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(|e| ArtefactError::Key(format!("{what} is neither hex nor base64: {e}")))?
    };
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
        ArtefactError::Key(format!("{what} must hold 32 bytes, found {}", bytes.len()))
    })
}

/// A set of trusted Ed25519 publisher keys.
#[derive(Debug, Clone, Default)]
pub struct TrustRoot {
    /// `(label, key)`; the label names the key in messages (the `*.pub`
    /// file stem, the key file's stem, or `inline`).
    keys: Vec<(String, ed25519_dalek::VerifyingKey)>,
}

impl TrustRoot {
    /// Load a trust root from `spec`:
    /// - a directory: every `*.pub` file in it, one key each;
    /// - a file: the one key it holds;
    /// - when no such path exists, 64 hex digits: that key inline.
    ///
    /// Any malformed key refuses the whole trust root, and so does one with
    /// no key at all.
    pub fn load(spec: &str) -> Result<Self, ArtefactError> {
        let path = Path::new(spec);
        let keys = if path.is_dir() {
            let mut files: Vec<PathBuf> = std::fs::read_dir(path)
                .map_err(|e| ArtefactError::Io(format!("trust root {spec}: {e}")))?
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("pub"))
                .collect();
            files.sort();
            let mut keys = Vec::with_capacity(files.len());
            for file in files {
                keys.push(Self::key_file(&file)?);
            }
            keys
        } else if path.is_file() {
            vec![Self::key_file(path)?]
        } else if spec.len() == 64 && spec.bytes().all(|b| b.is_ascii_hexdigit()) {
            vec![("inline".to_string(), parse_public_key(spec)?)]
        } else {
            return Err(ArtefactError::Io(format!(
                "trust root {spec} is neither a directory, a key file, nor 64 hex digits"
            )));
        };
        if keys.is_empty() {
            return Err(ArtefactError::Key(format!(
                "trust root {spec} holds no keys (*.pub files)"
            )));
        }
        Ok(Self { keys })
    }

    /// A trust root of the given keys (tests, embedding).
    pub fn from_keys(keys: Vec<(String, ed25519_dalek::VerifyingKey)>) -> Self {
        Self { keys }
    }

    fn key_file(path: &Path) -> Result<(String, ed25519_dalek::VerifyingKey), ArtefactError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ArtefactError::Io(format!("read {}: {e}", path.display())))?;
        let key = parse_public_key(&text)
            .map_err(|e| ArtefactError::Key(format!("{}: {e}", path.display())))?;
        let label = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("(unnamed)")
            .to_string();
        Ok((label, key))
    }

    /// Number of trusted keys.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Verify a base64 Ed25519 signature over `bytes`; returns the label of
    /// the trusted key that made it.
    pub fn verify(&self, bytes: &[u8], signature_b64: &str) -> Result<&str, ArtefactError> {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(signature_b64.trim())
            .map_err(|e| ArtefactError::Signature(format!("not base64: {e}")))?;
        let raw = <[u8; 64]>::try_from(raw.as_slice()).map_err(|_| {
            ArtefactError::Signature(format!("a signature is 64 bytes, found {}", raw.len()))
        })?;
        let signature = ed25519_dalek::Signature::from_bytes(&raw);
        self.keys
            .iter()
            .find(|(_, key)| key.verify(bytes, &signature).is_ok())
            .map(|(label, _)| label.as_str())
            .ok_or_else(|| ArtefactError::Signature("the signature matches no trusted key".into()))
    }
}

/// What a [`verify_artefact`] check established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtefactTrust {
    /// The key that signed `plugin.wasm`.
    pub wasm_signed_by: String,
    /// The key that signed `manifest.json`, when the artefact has a
    /// `manifest.sig`.
    pub manifest_signed_by: Option<String>,
}

/// Check an artefact's signatures against `trust`: `plugin.sig` must exist
/// and verify; a present `manifest.sig` must verify too, and with
/// `require_manifest_signature` it must exist.
pub fn verify_artefact(
    artefact: &Artefact,
    trust: &TrustRoot,
    require_manifest_signature: bool,
) -> Result<ArtefactTrust, ArtefactError> {
    let wasm_sig = artefact
        .wasm_signature
        .as_deref()
        .ok_or_else(|| ArtefactError::Signature(format!("the artefact has no {ENTRY_SIG}")))?;
    let wasm_signed_by = trust
        .verify(&artefact.wasm, wasm_sig)
        .map_err(|e| ArtefactError::Signature(format!("{ENTRY_SIG}: {e}")))?
        .to_string();
    let manifest_signed_by = match artefact.manifest_signature.as_deref() {
        Some(sig) => Some(
            trust
                .verify(&artefact.manifest_json, sig)
                .map_err(|e| ArtefactError::Signature(format!("{ENTRY_MANIFEST_SIG}: {e}")))?
                .to_string(),
        ),
        None if require_manifest_signature => {
            return Err(ArtefactError::Signature(format!(
                "the artefact has no {ENTRY_MANIFEST_SIG}, which this proxy requires \
                 (re-sign it with `helios-plugin sign`)"
            )))
        }
        None => None,
    };
    Ok(ArtefactTrust {
        wasm_signed_by,
        manifest_signed_by,
    })
}

/// The manifest fields of a bare module's `.yaml` / `.json` sidecar (or of
/// the JSON in its `helios_manifest` custom section).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SidecarManifest {
    /// `name`.
    pub name: String,
    /// `version` (empty when absent).
    pub version: String,
    /// `description`.
    pub description: String,
    /// `author`.
    pub author: String,
    /// `license`.
    pub license: String,
    /// `hooks`.
    pub hooks: Vec<String>,
    /// `permissions`.
    pub permissions: Vec<String>,
    /// `resources.min_memory` as written (`"1MB"`, a byte count...).
    pub min_memory: Option<String>,
    /// `resources.max_memory` as written.
    pub max_memory: Option<String>,
    /// Top-level keys this format does not define; the loader logs them,
    /// so a misspelt key is visible rather than silently dropped.
    pub unknown_keys: Vec<String>,
}

/// Parse the YAML subset sidecar manifests use:
///
/// - top-level `key: value` scalars (quotes optional, ` #` comments);
/// - `hooks:` and `permissions:` as indented `- item` lines or an inline
///   `[a, b]` list;
/// - a `resources:` block holding indented `min_memory:` / `max_memory:`.
///
/// Other top-level keys (and anything nested under them) are collected in
/// [`SidecarManifest::unknown_keys`]. A list item outside a list, or a
/// nested key under `hooks:` / `permissions:`, is an error.
pub fn parse_yaml_manifest(text: &str) -> Result<SidecarManifest, ArtefactError> {
    let mut m = SidecarManifest::default();
    let mut block = String::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.split(" #").next().unwrap_or(raw).trim_end();
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let at = |what: &str| ArtefactError::Manifest(format!("line {}: {what}", lineno + 1));
        let indented = line.starts_with(' ') || line.starts_with('\t');
        if let Some(item) = trimmed
            .strip_prefix("- ")
            .or_else(|| (trimmed == "-").then_some(""))
        {
            let item = unquote(item);
            match block.as_str() {
                "hooks" => m.hooks.push(item),
                "permissions" => m.permissions.push(item),
                "" => return Err(at("a list item outside any list")),
                _ => {} // part of an unknown block, already reported
            }
            continue;
        }
        if indented {
            match block.as_str() {
                "resources" => {
                    let (key, value) = trimmed
                        .split_once(':')
                        .ok_or_else(|| at("expected `key: value` under resources"))?;
                    match key.trim() {
                        "min_memory" => m.min_memory = Some(unquote(value)),
                        "max_memory" => m.max_memory = Some(unquote(value)),
                        other => m.unknown_keys.push(format!("resources.{other}")),
                    }
                }
                "hooks" | "permissions" => {
                    return Err(at(&format!("{block} must be a list of `- name` items")))
                }
                "" => return Err(at("an indented line outside any block")),
                _ => {}
            }
            continue;
        }
        let (key, value) = trimmed
            .split_once(':')
            .ok_or_else(|| at("expected `key: value`"))?;
        let key = key.trim();
        let value = value.trim();
        block.clear();
        match key {
            "name" => m.name = unquote(value),
            "version" => m.version = unquote(value),
            "description" => m.description = unquote(value),
            "author" => m.author = unquote(value),
            "license" => m.license = unquote(value),
            "hooks" | "permissions" | "resources" if value.is_empty() => block.push_str(key),
            "hooks" | "permissions" => {
                let inner = value
                    .strip_prefix('[')
                    .and_then(|v| v.strip_suffix(']'))
                    .ok_or_else(|| at(&format!("{key} must be a list")))?;
                let items = inner.split(',').map(unquote).filter(|s| !s.is_empty());
                if key == "hooks" {
                    m.hooks.extend(items);
                } else {
                    m.permissions.extend(items);
                }
            }
            "resources" => return Err(at("resources must be a block")),
            other => {
                m.unknown_keys.push(other.to_string());
                if value.is_empty() {
                    // A block of an unknown key: skip its nested lines.
                    block.push_str(other);
                }
            }
        }
    }
    Ok(m)
}

/// Parse a JSON sidecar manifest: the same fields as the YAML form, with
/// memory under a `resources` object. Wrong types are errors; unknown
/// top-level keys are collected in [`SidecarManifest::unknown_keys`].
pub fn parse_json_manifest(text: &str) -> Result<SidecarManifest, ArtefactError> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| ArtefactError::Manifest(format!("JSON: {e}")))?;
    let obj = value
        .as_object()
        .ok_or_else(|| ArtefactError::Manifest("the manifest is not a JSON object".into()))?;
    let string = |key: &str| -> Result<String, ArtefactError> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(String::new()),
            Some(serde_json::Value::String(s)) => Ok(s.clone()),
            Some(_) => Err(ArtefactError::Manifest(format!("{key} must be a string"))),
        }
    };
    let list = |key: &str| -> Result<Vec<String>, ArtefactError> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(Vec::new()),
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| ArtefactError::Manifest(format!("{key} must hold strings")))
                })
                .collect(),
            Some(_) => Err(ArtefactError::Manifest(format!("{key} must be a list"))),
        }
    };
    let mut m = SidecarManifest {
        name: string("name")?,
        version: string("version")?,
        description: string("description")?,
        author: string("author")?,
        license: string("license")?,
        hooks: list("hooks")?,
        permissions: list("permissions")?,
        ..SidecarManifest::default()
    };
    if let Some(resources) = obj.get("resources") {
        let resources = resources
            .as_object()
            .ok_or_else(|| ArtefactError::Manifest("resources must be an object".into()))?;
        for (key, value) in resources {
            let size = value
                .as_str()
                .map(str::to_string)
                .or_else(|| value.as_u64().map(|n| n.to_string()))
                .ok_or_else(|| {
                    ArtefactError::Manifest(format!("resources.{key} must be a size"))
                })?;
            match key.as_str() {
                "min_memory" => m.min_memory = Some(size),
                "max_memory" => m.max_memory = Some(size),
                other => m.unknown_keys.push(format!("resources.{other}")),
            }
        }
    }
    const KNOWN: [&str; 8] = [
        "name",
        "version",
        "description",
        "author",
        "license",
        "hooks",
        "permissions",
        "resources",
    ];
    m.unknown_keys
        .extend(obj.keys().filter(|k| !KNOWN.contains(&k.as_str())).cloned());
    Ok(m)
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    for q in ['"', '\''] {
        if s.len() >= 2 && s.starts_with(q) && s.ends_with(q) {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

/// Parse a memory size: a byte count, or a number with a `KB`, `MB` or `GB`
/// suffix (binary multiples, case-insensitive).
pub fn parse_memory_size(s: &str) -> Result<usize, ArtefactError> {
    let t = s.trim().to_ascii_uppercase();
    let (digits, mult) = if let Some(n) = t.strip_suffix("GB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = t.strip_suffix("MB") {
        (n, 1024 * 1024)
    } else if let Some(n) = t.strip_suffix("KB") {
        (n, 1024)
    } else {
        (t.as_str(), 1)
    };
    digits
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|n| n.checked_mul(mult))
        .ok_or_else(|| ArtefactError::Manifest(format!("invalid memory size {s:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const WASM: &[u8] = b"\x00asm\x01\x00\x00\x00";

    fn key(n: u8) -> ed25519_dalek::SigningKey {
        ed25519_dalek::SigningKey::from_bytes(&[n; 32])
    }

    fn input() -> PackInput {
        PackInput {
            name: "colmask".into(),
            version: "1.2.0-rc.1".into(),
            description: "mask columns".into(),
            license: "Apache-2.0".into(),
            hooks: vec!["pre_query".into()],
            wasm: WASM.to_vec(),
            wasm_signature: None,
        }
    }

    fn trust(keys: &[&ed25519_dalek::SigningKey]) -> TrustRoot {
        TrustRoot::from_keys(
            keys.iter()
                .enumerate()
                .map(|(i, k)| (format!("k{i}"), k.verifying_key()))
                .collect(),
        )
    }

    /// Raw tar.gz bytes with exactly the given entries.
    fn archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut tar = tar::Builder::new(gz);
        for (name, bytes) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_path(name).unwrap();
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append(&h, *bytes).unwrap();
        }
        tar.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn a_signed_pack_unpacks_and_verifies_both_signatures() {
        let k = key(7);
        let (manifest, bytes) = pack(&input(), Some(&k)).unwrap();
        assert_eq!(manifest.schema_version, SCHEMA_VERSION);
        let art = unpack(&bytes, DEFAULT_MAX_ARTEFACT_BYTES).unwrap();
        assert_eq!(art.manifest, manifest);
        assert_eq!(art.wasm, WASM);
        let t = verify_artefact(&art, &trust(&[&k]), true).unwrap();
        assert_eq!(t.wasm_signed_by, "k0");
        assert_eq!(t.manifest_signed_by.as_deref(), Some("k0"));
        // Another publisher's trust root refuses it.
        assert!(verify_artefact(&art, &trust(&[&key(9)]), false).is_err());
    }

    #[test]
    fn an_unsigned_pack_loads_only_without_a_trust_requirement() {
        let (_, bytes) = pack(&input(), None).unwrap();
        let art = unpack(&bytes, DEFAULT_MAX_ARTEFACT_BYTES).unwrap();
        assert!(art.wasm_signature.is_none() && art.manifest_signature.is_none());
        let err = verify_artefact(&art, &trust(&[&key(7)]), false).unwrap_err();
        assert!(err.to_string().contains("no plugin.sig"), "{err}");
    }

    #[test]
    fn a_repacked_manifest_breaks_its_signature() {
        // A signed module repacked under another plugin's name keeps a
        // valid plugin.sig, but its manifest.sig no longer verifies.
        let k = key(7);
        let (_, bytes) = pack(&input(), Some(&k)).unwrap();
        let art = unpack(&bytes, DEFAULT_MAX_ARTEFACT_BYTES).unwrap();
        let mut forged = art.manifest.clone();
        forged.name = "other-plugin".into();
        let forged_json = serde_json::to_vec_pretty(&forged).unwrap();
        let repacked = archive(&[
            (ENTRY_MANIFEST, forged_json.as_slice()),
            (ENTRY_WASM, art.wasm.as_slice()),
            (ENTRY_SIG, art.wasm_signature.as_deref().unwrap().as_bytes()),
            (
                ENTRY_MANIFEST_SIG,
                art.manifest_signature.as_deref().unwrap().as_bytes(),
            ),
        ]);
        let art = unpack(&repacked, DEFAULT_MAX_ARTEFACT_BYTES).unwrap();
        let err = verify_artefact(&art, &trust(&[&k]), false).unwrap_err();
        assert!(err.to_string().contains("manifest.sig"), "{err}");
        // Without its manifest.sig the forgery passes only when the
        // manifest signature is not required.
        let stripped = archive(&[
            (ENTRY_MANIFEST, forged_json.as_slice()),
            (ENTRY_WASM, art.wasm.as_slice()),
            (ENTRY_SIG, art.wasm_signature.as_deref().unwrap().as_bytes()),
        ]);
        let art = unpack(&stripped, DEFAULT_MAX_ARTEFACT_BYTES).unwrap();
        assert!(verify_artefact(&art, &trust(&[&k]), false).is_ok());
        assert!(verify_artefact(&art, &trust(&[&k]), true).is_err());
    }

    #[test]
    fn unexpected_duplicate_and_missing_entries_are_refused() {
        let (manifest, _) = pack(&input(), None).unwrap();
        let json = serde_json::to_vec(&manifest).unwrap();
        let json: &[u8] = &json;
        let other_wasm: &[u8] = b"\x00asm\x01\x00\x00\x01";
        let readme: &[u8] = b"x";
        type Case<'a> = (Vec<(&'a str, &'a [u8])>, &'a str);
        let cases: Vec<Case> = vec![
            (
                vec![
                    (ENTRY_MANIFEST, json),
                    (ENTRY_WASM, WASM),
                    ("README", readme),
                ],
                "unexpected entry README",
            ),
            (
                vec![
                    (ENTRY_MANIFEST, json),
                    (ENTRY_WASM, WASM),
                    (ENTRY_WASM, WASM),
                ],
                "appears twice",
            ),
            (vec![(ENTRY_MANIFEST, json)], "missing plugin.wasm"),
            (vec![(ENTRY_WASM, WASM)], "missing manifest.json"),
            (
                vec![(ENTRY_MANIFEST, json), (ENTRY_WASM, other_wasm)],
                "SHA-256",
            ),
        ];
        for (entries, needle) in cases {
            let err = unpack(&archive(&entries), DEFAULT_MAX_ARTEFACT_BYTES).unwrap_err();
            assert!(err.to_string().contains(needle), "{needle}: {err}");
        }
        assert!(unpack(b"not gzip", 1024).is_err());
    }

    #[test]
    fn content_over_the_limit_is_refused() {
        let mut big = input();
        big.wasm.extend_from_slice(&[0u8; 4096]);
        let (_, bytes) = pack(&big, None).unwrap();
        assert!(unpack(&bytes, DEFAULT_MAX_ARTEFACT_BYTES).is_ok());
        let err = unpack(&bytes, 2048).unwrap_err();
        assert!(err.to_string().contains("limit"), "{err}");
    }

    #[test]
    fn manifest_fields_are_validated() {
        let mut bad = input();
        bad.name = "../escape".into();
        assert!(pack(&bad, None).is_err());
        let mut bad = input();
        bad.version = "1.0 beta".into();
        assert!(pack(&bad, None).is_err());
        let mut bad = input();
        bad.hooks = vec!["pre_query".into(), "pre_query".into()];
        assert!(pack(&bad, None).is_err());
        let mut bad = input();
        bad.wasm = b"not wasm".to_vec();
        assert!(pack(&bad, None).is_err());
        assert!(validate_version("2.0.0+build.7").is_ok());
    }

    #[test]
    fn keys_round_trip_through_every_accepted_form() {
        let k = key(3);
        let pem = signing_key_pem(&k);
        assert_eq!(parse_signing_key(&pem).unwrap().to_bytes(), k.to_bytes());
        let b64 = base64::engine::general_purpose::STANDARD.encode(k.to_bytes());
        assert_eq!(parse_signing_key(&b64).unwrap().to_bytes(), k.to_bytes());
        let hex = hex_lower(&k.to_bytes());
        assert_eq!(parse_signing_key(&hex).unwrap().to_bytes(), k.to_bytes());

        let vk = k.verifying_key();
        for text in [
            public_key_pem(&vk),
            public_key_base64(&vk),
            hex_lower(vk.as_bytes()),
        ] {
            assert_eq!(parse_public_key(&text).unwrap(), vk, "{text}");
        }
        assert!(parse_public_key("AAAA").is_err());
        assert!(parse_signing_key("-----BEGIN ENCRYPTED PRIVATE KEY-----").is_err());
    }

    #[test]
    fn a_pkcs8_v2_key_must_carry_its_own_public_key() {
        let k = key(5);
        let mut der = PKCS8_ED25519_V2_PREFIX.to_vec();
        der.extend_from_slice(&k.to_bytes());
        der.extend_from_slice(&PKCS8_ED25519_V2_PUBLIC);
        der.extend_from_slice(k.verifying_key().as_bytes());
        let good = pem_encode("PRIVATE KEY", &der);
        assert_eq!(parse_signing_key(&good).unwrap().to_bytes(), k.to_bytes());
        let n = der.len();
        der[n - 1] ^= 1;
        let bad = pem_encode("PRIVATE KEY", &der);
        assert!(parse_signing_key(&bad).is_err());
    }

    #[test]
    fn trust_roots_load_from_a_directory_a_file_or_inline_hex() {
        let dir = tempfile::tempdir().unwrap();
        let a = key(1).verifying_key();
        let b = key(2).verifying_key();
        std::fs::write(dir.path().join("a.pub"), public_key_base64(&a)).unwrap();
        std::fs::write(dir.path().join("b.pub"), public_key_pem(&b)).unwrap();
        std::fs::write(dir.path().join("notes.txt"), "ignored").unwrap();
        let root = TrustRoot::load(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(root.key_count(), 2);
        let sig = sign(&key(2), b"payload");
        assert_eq!(root.verify(b"payload", &sig).unwrap(), "b");

        let file = dir.path().join("b.pub");
        let single = TrustRoot::load(file.to_str().unwrap()).unwrap();
        assert_eq!(single.verify(b"payload", &sig).unwrap(), "b");

        let inline = TrustRoot::load(&hex_lower(b.as_bytes())).unwrap();
        assert_eq!(inline.verify(b"payload", &sig).unwrap(), "inline");

        let empty = tempfile::tempdir().unwrap();
        assert!(TrustRoot::load(empty.path().to_str().unwrap()).is_err());
        std::fs::write(empty.path().join("bad.pub"), "nope").unwrap();
        assert!(TrustRoot::load(empty.path().to_str().unwrap()).is_err());
        assert!(TrustRoot::load("/nonexistent/trust/root").is_err());
    }

    #[test]
    fn artefact_kinds_follow_the_file_name() {
        assert_eq!(
            ArtefactKind::of(Path::new("/p/a.wasm")),
            Some(ArtefactKind::Wasm)
        );
        assert_eq!(
            ArtefactKind::of(Path::new("/p/a.tar.gz")),
            Some(ArtefactKind::TarGz)
        );
        for other in ["/p/a.gz", "/p/a.sig", "/p/a.yaml", "/p/.wasm", "/p/a.wasm~"] {
            assert_eq!(ArtefactKind::of(Path::new(other)), None, "{other}");
        }
        let sidecars = ArtefactKind::Wasm.sidecars(Path::new("/p/a.wasm"));
        assert_eq!(
            sidecars,
            [
                PathBuf::from("/p/a.sig"),
                PathBuf::from("/p/a.yaml"),
                PathBuf::from("/p/a.json")
            ]
        );
    }

    #[test]
    fn sidecar_manifests_parse_and_report_unknown_keys() {
        let yaml = "name: colmask\nversion: \"0.2.0\"\nhooks:\n  - pre_query\n  - post_query\n\
                    permissions: [kv_read]\nresources:\n  max_memory: 16MB # cap\n\
                    config:\n  mask: email\n";
        let m = parse_yaml_manifest(yaml).unwrap();
        assert_eq!(m.name, "colmask");
        assert_eq!(m.version, "0.2.0");
        assert_eq!(m.hooks, ["pre_query", "post_query"]);
        assert_eq!(m.permissions, ["kv_read"]);
        assert_eq!(
            parse_memory_size(m.max_memory.as_deref().unwrap()).unwrap(),
            16 << 20
        );
        assert_eq!(m.unknown_keys, ["config"]);
        // A misspelt list key is reported, and its items are not hooks.
        let typo = parse_yaml_manifest("name: x\nhoks:\n  - pre_query\n").unwrap();
        assert!(typo.hooks.is_empty());
        assert_eq!(typo.unknown_keys, ["hoks"]);
        assert!(parse_yaml_manifest("- stray\n").is_err());
        assert!(parse_yaml_manifest("hooks:\n  pre_query: yes\n").is_err());

        let json = r#"{"name": "colmask", "version": "1.0.0", "hooks": ["route"],
                       "resources": {"min_memory": "1MB"}, "hookz": []}"#;
        let m = parse_json_manifest(json).unwrap();
        assert_eq!(m.hooks, ["route"]);
        assert_eq!(m.min_memory.as_deref(), Some("1MB"));
        assert_eq!(m.unknown_keys, ["hookz"]);
        assert!(parse_json_manifest(r#"{"name": "x", "hooks": "pre_query"}"#).is_err());
        assert!(parse_json_manifest("[]").is_err());
        assert!(parse_memory_size("12 parsecs").is_err());
    }

    #[test]
    fn write_atomic_replaces_the_file_and_leaves_no_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.tar.gz");
        write_atomic(&path, b"one", 0o644).unwrap();
        write_atomic(&path, b"two", 0o644).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
    }
}
