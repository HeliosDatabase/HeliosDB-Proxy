//! Plugin Loader
//!
//! Loads WASM plugins from files and parses their manifests.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::sandbox::Permission;
use super::HookType;

/// Error types for plugin loading
#[derive(Debug, Clone)]
pub enum PluginLoadError {
    /// File not found
    FileNotFound(String),

    /// Invalid file format
    InvalidFormat(String),

    /// Manifest parsing error
    ManifestError(String),

    /// IO error
    IoError(String),

    /// Validation error
    ValidationError(String),

    /// Signature verification failed (Ed25519 over the .wasm bytes
    /// did not match any trusted public key, or the signature blob
    /// itself was malformed).
    SignatureInvalid(String),
}

impl std::fmt::Display for PluginLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginLoadError::FileNotFound(path) => write!(f, "File not found: {}", path),
            PluginLoadError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            PluginLoadError::ManifestError(msg) => write!(f, "Manifest error: {}", msg),
            PluginLoadError::IoError(msg) => write!(f, "IO error: {}", msg),
            PluginLoadError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            PluginLoadError::SignatureInvalid(msg) => {
                write!(f, "Signature verification failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for PluginLoadError {}

impl From<std::io::Error> for PluginLoadError {
    fn from(err: std::io::Error) -> Self {
        PluginLoadError::IoError(err.to_string())
    }
}

impl From<PluginLoadError> for super::runtime::PluginError {
    fn from(err: PluginLoadError) -> Self {
        super::runtime::PluginError::LoadError(err.to_string())
    }
}

/// Plugin manifest (from plugin.yaml or embedded in WASM)
#[derive(Debug, Clone)]
pub struct PluginManifest {
    /// Plugin name
    pub name: String,

    /// Version
    pub version: String,

    /// Description
    pub description: String,

    /// Author
    pub author: String,

    /// License
    pub license: String,

    /// Supported hooks
    pub hooks: Vec<HookType>,

    /// Required permissions
    pub permissions: Vec<Permission>,

    /// Minimum memory requirement
    pub min_memory: usize,

    /// Maximum memory requirement
    pub max_memory: usize,

    /// Configuration schema
    pub config_schema: HashMap<String, ConfigField>,

    /// Plugin file path
    pub path: PathBuf,
}

impl Default for PluginManifest {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: "0.0.0".to_string(),
            description: String::new(),
            author: String::new(),
            license: String::new(),
            hooks: Vec::new(),
            permissions: Vec::new(),
            min_memory: 1024 * 1024,      // 1MB
            max_memory: 64 * 1024 * 1024, // 64MB
            config_schema: HashMap::new(),
            path: PathBuf::new(),
        }
    }
}

/// Configuration field schema
#[derive(Debug, Clone)]
pub struct ConfigField {
    /// Field type
    pub field_type: ConfigFieldType,

    /// Whether field is required
    pub required: bool,

    /// Default value
    pub default: Option<String>,

    /// Description
    pub description: String,
}

/// Configuration field types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFieldType {
    String,
    Integer,
    Float,
    Boolean,
    Array,
    Object,
}

/// Plugin loader
pub struct PluginLoader {
    /// Search paths for plugins
    search_paths: Vec<PathBuf>,

    /// Allowed extensions
    allowed_extensions: Vec<String>,

    /// Optional Ed25519 trust root. When `Some`, every loaded .wasm
    /// must have a matching `.sig` sidecar verifiable against one of
    /// these keys. When `None`, signatures are not checked (preserves
    /// the dev-loop ergonomic of dropping unsigned `.wasm` files in
    /// the plugin dir).
    signature_verifier: Option<SignatureVerifier>,
}

/// Ed25519 signature verifier for plugin .wasm files.
///
/// Trust root format: a directory of `*.pub` files, each containing
/// a base64-encoded 32-byte Ed25519 public key (one per trusted
/// publisher). The .sig file format is base64 of the raw 64-byte
/// Ed25519 signature over the .wasm bytes.
///
/// Wire shape is intentionally plain text + base64 — no PEM, no
/// X.509, no JSON envelope — so operators can sign with `openssl
/// pkeyutl -sign` or `signify` without bringing a CA story along.
#[derive(Debug, Default)]
pub struct SignatureVerifier {
    /// (label, public_key) pairs. Label is the .pub filename (no
    /// extension) and shows up in error messages so operators can
    /// trace which key matched.
    keys: Vec<(String, ed25519_dalek::VerifyingKey)>,
}

impl SignatureVerifier {
    /// Build a verifier from a directory of `*.pub` files. Each file
    /// must contain exactly one base64-encoded 32-byte Ed25519
    /// public key. Whitespace at the start / end is tolerated.
    pub fn from_trust_root(dir: &Path) -> Result<Self, PluginLoadError> {
        use base64::Engine as _;

        let mut keys = Vec::new();
        let entries = fs::read_dir(dir).map_err(|e| {
            PluginLoadError::IoError(format!("trust-root {}: {}", dir.display(), e))
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| PluginLoadError::IoError(e.to_string()))?;
            let p = entry.path();
            if p.extension().and_then(|s| s.to_str()) != Some("pub") {
                continue;
            }
            let raw = fs::read_to_string(&p).map_err(|e| {
                PluginLoadError::IoError(format!("read {}: {}", p.display(), e))
            })?;
            let raw = raw.trim();
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(raw)
                .map_err(|e| {
                    PluginLoadError::SignatureInvalid(format!(
                        "{} not valid base64: {}",
                        p.display(),
                        e
                    ))
                })?;
            if bytes.len() != 32 {
                return Err(PluginLoadError::SignatureInvalid(format!(
                    "{} should be 32 bytes (raw Ed25519 pubkey), got {}",
                    p.display(),
                    bytes.len()
                )));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            let key = ed25519_dalek::VerifyingKey::from_bytes(&arr).map_err(|e| {
                PluginLoadError::SignatureInvalid(format!(
                    "{} not a valid Ed25519 pubkey: {}",
                    p.display(),
                    e
                ))
            })?;
            let label = p
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("(unknown)")
                .to_string();
            keys.push((label, key));
        }
        Ok(Self { keys })
    }

    /// Verify a signature blob (base64-encoded Ed25519 signature)
    /// against the .wasm bytes. Returns Ok with the matching label
    /// on success.
    pub fn verify(&self, wasm: &[u8], sig_b64: &str) -> Result<&str, PluginLoadError> {
        use base64::Engine as _;
        use ed25519_dalek::Verifier;

        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(sig_b64.trim())
            .map_err(|e| {
                PluginLoadError::SignatureInvalid(format!("base64 decode: {}", e))
            })?;
        if sig_bytes.len() != 64 {
            return Err(PluginLoadError::SignatureInvalid(format!(
                "signature should be 64 bytes, got {}",
                sig_bytes.len()
            )));
        }
        let mut arr = [0u8; 64];
        arr.copy_from_slice(&sig_bytes);
        let sig = ed25519_dalek::Signature::from_bytes(&arr);

        for (label, key) in &self.keys {
            if key.verify(wasm, &sig).is_ok() {
                return Ok(label.as_str());
            }
        }
        Err(PluginLoadError::SignatureInvalid(
            "signature did not match any trusted key".to_string(),
        ))
    }

    /// Number of trusted keys. Useful for diagnostics — a verifier
    /// with zero keys rejects every signature.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }
}

impl PluginLoader {
    /// Create a new plugin loader
    pub fn new() -> Self {
        Self {
            search_paths: Vec::new(),
            allowed_extensions: vec!["wasm".to_string()],
            signature_verifier: None,
        }
    }

    /// Attach a trust-root verifier. Once set, every load() call
    /// requires a matching .sig sidecar; loads without one fail.
    pub fn with_signature_verifier(mut self, verifier: SignatureVerifier) -> Self {
        self.signature_verifier = Some(verifier);
        self
    }

    /// Add a search path
    pub fn add_search_path(&mut self, path: PathBuf) {
        self.search_paths.push(path);
    }

    /// Load a plugin from a file path
    pub fn load(&self, path: &Path) -> Result<(PluginManifest, Vec<u8>), PluginLoadError> {
        // Check file exists
        if !path.exists() {
            return Err(PluginLoadError::FileNotFound(path.display().to_string()));
        }

        // Check extension
        let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !self.allowed_extensions.contains(&extension.to_string()) {
            return Err(PluginLoadError::InvalidFormat(format!(
                "Invalid extension: {}. Allowed: {:?}",
                extension, self.allowed_extensions
            )));
        }

        // Read WASM bytes
        let wasm_bytes = fs::read(path)?;

        // Validate WASM magic number
        if wasm_bytes.len() < 8 || &wasm_bytes[0..4] != b"\x00asm" {
            return Err(PluginLoadError::InvalidFormat(
                "Invalid WASM file (bad magic number)".to_string(),
            ));
        }

        // Signature check (when a trust root is configured). The .sig
        // sidecar is required — no signature, no load.
        if let Some(ref verifier) = self.signature_verifier {
            let sig_path = path.with_extension("sig");
            if !sig_path.exists() {
                return Err(PluginLoadError::SignatureInvalid(format!(
                    "{} requires a sidecar .sig file (trust root active)",
                    path.display()
                )));
            }
            let sig_b64 = fs::read_to_string(&sig_path).map_err(|e| {
                PluginLoadError::IoError(format!("read {}: {}", sig_path.display(), e))
            })?;
            let label = verifier.verify(&wasm_bytes, &sig_b64)?;
            tracing::info!(
                plugin = %path.display(),
                signed_by = %label,
                "plugin signature verified"
            );
        }

        // Try to load manifest from sidecar file
        let manifest = self.load_manifest(path, &wasm_bytes)?;

        Ok((manifest, wasm_bytes))
    }

    /// Load plugin manifest
    fn load_manifest(&self, wasm_path: &Path, wasm_bytes: &[u8]) -> Result<PluginManifest, PluginLoadError> {
        // Try sidecar YAML manifest
        let yaml_path = wasm_path.with_extension("yaml");
        if yaml_path.exists() {
            return self.parse_yaml_manifest(&yaml_path, wasm_path);
        }

        // Try sidecar JSON manifest
        let json_path = wasm_path.with_extension("json");
        if json_path.exists() {
            return self.parse_json_manifest(&json_path, wasm_path);
        }

        // Try embedded manifest (custom section in WASM)
        if let Some(manifest) = self.extract_embedded_manifest(wasm_bytes, wasm_path)? {
            return Ok(manifest);
        }

        // Generate minimal manifest from filename
        Ok(self.generate_minimal_manifest(wasm_path))
    }

    /// Parse YAML manifest
    fn parse_yaml_manifest(&self, yaml_path: &Path, wasm_path: &Path) -> Result<PluginManifest, PluginLoadError> {
        let content = fs::read_to_string(yaml_path)?;

        // Simple YAML parsing (in production, would use serde_yaml)
        let mut manifest = PluginManifest::default();
        manifest.path = wasm_path.to_path_buf();

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once(':') {
                let key = key.trim();
                let value = value.trim().trim_matches('"').trim_matches('\'');

                match key {
                    "name" => manifest.name = value.to_string(),
                    "version" => manifest.version = value.to_string(),
                    "description" => manifest.description = value.to_string(),
                    "author" => manifest.author = value.to_string(),
                    "license" => manifest.license = value.to_string(),
                    _ => {}
                }
            }
        }

        // Parse hooks section
        if let Some(hooks_start) = content.find("hooks:") {
            let hooks_section = &content[hooks_start..];
            for line in hooks_section.lines().skip(1) {
                let line = line.trim();
                if line.is_empty() || !line.starts_with('-') {
                    if !line.starts_with(' ') && !line.is_empty() {
                        break;
                    }
                    continue;
                }
                let hook_name = line.trim_start_matches('-').trim();
                if let Some(hook) = HookType::from_str(hook_name) {
                    manifest.hooks.push(hook);
                }
            }
        }

        // Parse permissions section
        if let Some(perms_start) = content.find("permissions:") {
            let perms_section = &content[perms_start..];
            for line in perms_section.lines().skip(1) {
                let line = line.trim();
                if line.is_empty() || !line.starts_with('-') {
                    if !line.starts_with(' ') && !line.is_empty() {
                        break;
                    }
                    continue;
                }
                let perm_name = line.trim_start_matches('-').trim();
                if let Some(perm) = Permission::from_str(perm_name) {
                    manifest.permissions.push(perm);
                }
            }
        }

        // Validate manifest
        self.validate_manifest(&manifest)?;

        Ok(manifest)
    }

    /// Parse JSON manifest
    fn parse_json_manifest(&self, json_path: &Path, wasm_path: &Path) -> Result<PluginManifest, PluginLoadError> {
        let content = fs::read_to_string(json_path)?;

        // Parse JSON
        let json: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| PluginLoadError::ManifestError(e.to_string()))?;

        let mut manifest = PluginManifest::default();
        manifest.path = wasm_path.to_path_buf();

        if let Some(name) = json.get("name").and_then(|v| v.as_str()) {
            manifest.name = name.to_string();
        }
        if let Some(version) = json.get("version").and_then(|v| v.as_str()) {
            manifest.version = version.to_string();
        }
        if let Some(description) = json.get("description").and_then(|v| v.as_str()) {
            manifest.description = description.to_string();
        }
        if let Some(author) = json.get("author").and_then(|v| v.as_str()) {
            manifest.author = author.to_string();
        }
        if let Some(license) = json.get("license").and_then(|v| v.as_str()) {
            manifest.license = license.to_string();
        }

        // Parse hooks
        if let Some(hooks) = json.get("hooks").and_then(|v| v.as_array()) {
            for hook in hooks {
                if let Some(hook_name) = hook.as_str() {
                    if let Some(hook_type) = HookType::from_str(hook_name) {
                        manifest.hooks.push(hook_type);
                    }
                }
            }
        }

        // Parse permissions
        if let Some(perms) = json.get("permissions").and_then(|v| v.as_array()) {
            for perm in perms {
                if let Some(perm_name) = perm.as_str() {
                    if let Some(permission) = Permission::from_str(perm_name) {
                        manifest.permissions.push(permission);
                    }
                }
            }
        }

        // Parse memory requirements
        if let Some(resources) = json.get("resources") {
            if let Some(min_mem) = resources.get("min_memory").and_then(|v| v.as_str()) {
                manifest.min_memory = parse_memory_size(min_mem);
            }
            if let Some(max_mem) = resources.get("max_memory").and_then(|v| v.as_str()) {
                manifest.max_memory = parse_memory_size(max_mem);
            }
        }

        self.validate_manifest(&manifest)?;
        Ok(manifest)
    }

    /// Extract embedded manifest from WASM custom section
    fn extract_embedded_manifest(
        &self,
        _wasm_bytes: &[u8],
        wasm_path: &Path,
    ) -> Result<Option<PluginManifest>, PluginLoadError> {
        // In a real implementation, would parse WASM custom sections
        // looking for a "helios_manifest" section containing JSON

        // For now, return None (no embedded manifest found)
        let _ = wasm_path;
        Ok(None)
    }

    /// Generate minimal manifest from filename
    fn generate_minimal_manifest(&self, wasm_path: &Path) -> PluginManifest {
        let name = wasm_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        PluginManifest {
            name,
            version: "0.0.0".to_string(),
            description: "Auto-generated manifest".to_string(),
            author: "Unknown".to_string(),
            license: "Unknown".to_string(),
            hooks: Vec::new(), // No hooks without manifest
            permissions: Vec::new(),
            min_memory: 1024 * 1024,
            max_memory: 64 * 1024 * 1024,
            config_schema: HashMap::new(),
            path: wasm_path.to_path_buf(),
        }
    }

    /// Validate manifest
    fn validate_manifest(&self, manifest: &PluginManifest) -> Result<(), PluginLoadError> {
        if manifest.name.is_empty() {
            return Err(PluginLoadError::ValidationError(
                "Plugin name is required".to_string(),
            ));
        }

        if manifest.name.len() > 128 {
            return Err(PluginLoadError::ValidationError(
                "Plugin name too long (max 128 chars)".to_string(),
            ));
        }

        // Validate name format (alphanumeric + hyphens)
        if !manifest.name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
            return Err(PluginLoadError::ValidationError(
                "Plugin name must be alphanumeric (hyphens and underscores allowed)".to_string(),
            ));
        }

        // Validate version format (semver-like)
        if !manifest.version.chars().all(|c| c.is_numeric() || c == '.') {
            return Err(PluginLoadError::ValidationError(
                "Invalid version format (expected semver)".to_string(),
            ));
        }

        // Validate memory requirements
        if manifest.min_memory > manifest.max_memory {
            return Err(PluginLoadError::ValidationError(
                "min_memory cannot exceed max_memory".to_string(),
            ));
        }

        if manifest.max_memory > 256 * 1024 * 1024 {
            return Err(PluginLoadError::ValidationError(
                "max_memory cannot exceed 256MB".to_string(),
            ));
        }

        Ok(())
    }

    /// Discover plugins in search paths
    pub fn discover(&self) -> Result<Vec<PathBuf>, PluginLoadError> {
        let mut plugins = Vec::new();

        for search_path in &self.search_paths {
            if !search_path.exists() || !search_path.is_dir() {
                continue;
            }

            for entry in fs::read_dir(search_path)? {
                let entry = entry?;
                let path = entry.path();

                if !path.is_file() {
                    continue;
                }

                let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
                if self.allowed_extensions.contains(&extension.to_string()) {
                    plugins.push(path);
                }
            }
        }

        Ok(plugins)
    }
}

impl Default for PluginLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse memory size string (e.g., "64MB", "1024KB")
fn parse_memory_size(s: &str) -> usize {
    let s = s.trim().to_uppercase();

    if let Some(mb) = s.strip_suffix("MB") {
        mb.trim().parse::<usize>().unwrap_or(0) * 1024 * 1024
    } else if let Some(kb) = s.strip_suffix("KB") {
        kb.trim().parse::<usize>().unwrap_or(0) * 1024
    } else if let Some(gb) = s.strip_suffix("GB") {
        gb.trim().parse::<usize>().unwrap_or(0) * 1024 * 1024 * 1024
    } else {
        s.parse::<usize>().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_load_error_display() {
        let err = PluginLoadError::FileNotFound("/test.wasm".to_string());
        assert!(err.to_string().contains("File not found"));

        let err = PluginLoadError::ManifestError("invalid".to_string());
        assert!(err.to_string().contains("Manifest error"));
    }

    #[test]
    fn test_plugin_manifest_default() {
        let manifest = PluginManifest::default();
        assert!(manifest.name.is_empty());
        assert_eq!(manifest.version, "0.0.0");
        assert!(manifest.hooks.is_empty());
    }

    #[test]
    fn test_plugin_loader_new() {
        let loader = PluginLoader::new();
        assert!(loader.search_paths.is_empty());
        assert!(loader.allowed_extensions.contains(&"wasm".to_string()));
    }

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("64MB"), 64 * 1024 * 1024);
        assert_eq!(parse_memory_size("1024KB"), 1024 * 1024);
        assert_eq!(parse_memory_size("1GB"), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1048576"), 1048576);
    }

    #[test]
    fn test_manifest_validation_empty_name() {
        let loader = PluginLoader::new();
        let manifest = PluginManifest::default();

        let result = loader.validate_manifest(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name is required"));
    }

    #[test]
    fn test_manifest_validation_invalid_memory() {
        let loader = PluginLoader::new();
        let mut manifest = PluginManifest::default();
        manifest.name = "test-plugin".to_string();
        manifest.min_memory = 100 * 1024 * 1024;
        manifest.max_memory = 50 * 1024 * 1024;

        let result = loader.validate_manifest(&manifest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_memory"));
    }

    #[test]
    fn test_manifest_validation_success() {
        let loader = PluginLoader::new();
        let mut manifest = PluginManifest::default();
        manifest.name = "test-plugin".to_string();

        let result = loader.validate_manifest(&manifest);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_minimal_manifest() {
        let loader = PluginLoader::new();
        let path = PathBuf::from("/plugins/my-plugin.wasm");
        let manifest = loader.generate_minimal_manifest(&path);

        assert_eq!(manifest.name, "my-plugin");
        assert_eq!(manifest.version, "0.0.0");
    }

    #[test]
    fn test_config_field_type() {
        assert_eq!(ConfigFieldType::String, ConfigFieldType::String);
        assert_ne!(ConfigFieldType::String, ConfigFieldType::Integer);
    }

    // -----------------------------------------------------------------
    // SignatureVerifier tests
    //
    // We generate an Ed25519 keypair at runtime, write the public key
    // into a temp trust-root dir, sign a fake .wasm, and check that
    // the loader accepts the signed bytes and rejects tampered ones.
    // -----------------------------------------------------------------

    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};

    /// Helper: write a single .pub file with `key`'s public component
    /// into `dir/<label>.pub`. Returns `dir`.
    fn write_pub_key(dir: &Path, label: &str, key: &SigningKey) {
        let pub_bytes = key.verifying_key().to_bytes();
        let b64 = base64::engine::general_purpose::STANDARD.encode(pub_bytes);
        std::fs::write(dir.join(format!("{label}.pub")), b64).unwrap();
    }

    fn make_signing_key() -> SigningKey {
        // Deterministic seed → reproducible tests.
        let seed = [7u8; 32];
        SigningKey::from_bytes(&seed)
    }

    #[test]
    fn test_signature_verifier_accepts_matching_signature() {
        let dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(dir.path(), "official", &key);

        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();
        assert_eq!(verifier.key_count(), 1);

        let wasm = b"\x00asm\x01\x00\x00\x00pretend-real-wasm";
        let sig = key.sign(wasm);
        let sig_b64 =
            base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let label = verifier.verify(wasm, &sig_b64).unwrap();
        assert_eq!(label, "official");
    }

    #[test]
    fn test_signature_verifier_rejects_tampered_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(dir.path(), "official", &key);
        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();

        let wasm = b"\x00asm\x01\x00\x00\x00pretend-real-wasm";
        let sig = key.sign(wasm);
        let sig_b64 =
            base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let tampered = b"\x00asm\x01\x00\x00\x00pretend-real-wasn"; // 'm' → 'n'
        let err = verifier.verify(tampered, &sig_b64).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }

    #[test]
    fn test_signature_verifier_rejects_unknown_signer() {
        let dir = tempfile::tempdir().unwrap();
        let trusted = make_signing_key();
        write_pub_key(dir.path(), "official", &trusted);
        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();

        // Sign with a completely different key.
        let attacker = SigningKey::from_bytes(&[0xAB; 32]);
        let wasm = b"\x00asm\x01\x00\x00\x00pretend-real-wasm";
        let sig = attacker.sign(wasm);
        let sig_b64 =
            base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let err = verifier.verify(wasm, &sig_b64).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }

    #[test]
    fn test_signature_verifier_rejects_wrong_length_pubkey() {
        let dir = tempfile::tempdir().unwrap();
        // 31 bytes — invalid Ed25519 length.
        std::fs::write(
            dir.path().join("bad.pub"),
            base64::engine::general_purpose::STANDARD.encode([0u8; 31]),
        )
        .unwrap();
        let err = SignatureVerifier::from_trust_root(dir.path()).unwrap_err();
        assert!(matches!(err, PluginLoadError::SignatureInvalid(_)));
    }

    #[test]
    fn test_signature_verifier_supports_multiple_keys() {
        let dir = tempfile::tempdir().unwrap();
        let k1 = SigningKey::from_bytes(&[1u8; 32]);
        let k2 = SigningKey::from_bytes(&[2u8; 32]);
        write_pub_key(dir.path(), "publisher-a", &k1);
        write_pub_key(dir.path(), "publisher-b", &k2);

        let verifier = SignatureVerifier::from_trust_root(dir.path()).unwrap();
        assert_eq!(verifier.key_count(), 2);

        let wasm = b"\x00asm\x01\x00\x00\x00abc";
        let sig = k2.sign(wasm); // signed by the SECOND publisher
        let sig_b64 =
            base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());

        let label = verifier.verify(wasm, &sig_b64).unwrap();
        assert_eq!(label, "publisher-b");
    }

    #[test]
    fn test_loader_with_verifier_rejects_unsigned_plugin() {
        let dir = tempfile::tempdir().unwrap();
        let wasm_path = dir.path().join("plugin.wasm");
        std::fs::write(&wasm_path, b"\x00asm\x01\x00\x00\x00body").unwrap();

        let trust_dir = tempfile::tempdir().unwrap();
        let key = make_signing_key();
        write_pub_key(trust_dir.path(), "official", &key);

        let loader = PluginLoader::new()
            .with_signature_verifier(SignatureVerifier::from_trust_root(trust_dir.path()).unwrap());
        let err = loader.load(&wasm_path).unwrap_err();
        assert!(
            matches!(err, PluginLoadError::SignatureInvalid(_)),
            "expected SignatureInvalid for missing .sig, got {:?}",
            err
        );
    }
}
